#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "configraft.pb.h"

namespace configraft {

using namespace v1;  // 协议类型（KV/RaftCmd/Code 等）定义在 configraft.v1 包

// 一次写指令 apply 完成后的结果（code 语义对齐 configraft.v1.Code）
struct ApplyResult {
    int32_t code = Code::OK;
    std::string message;
    KV kv;                    // 受影响 key 的最新 KV
    std::vector<KV> kvs;      // 批量操作结果
    std::string leader_id;    // NOT_LEADER 时的重定向目标
    // Watch 事件（M4）：本次写成功产生的变更事件，由调用方（on_apply / LocalNode::Apply）
    // 交给 WatchHub 广播。仅在写成功（code==OK）时填充；与 iter.done() 无关——
    // 复制到 Follower 的日志同样要广播，保证 Watch 可落在任意节点。
    std::vector<WatchEvent> events;
};

struct GetResult {
    int32_t code = Code::OK;
    std::string message;
    KV kv;
    std::vector<KV> kvs;
    std::string leader_id;  // NOT_LEADER 时的重定向目标
};

struct ConfigResult {
    int32_t code = Code::OK;
    std::string message;
    KV kv;                    // 最新/指定版本 KV
    std::vector<KV> history;  // 历史版本
};

// Watch 长轮询结果（M4）。current_revision 是客户端续传锚点：
// 有事件时为最后一条事件的 revision，无事件时为当前集群 revision。客户端下一轮
// from_revision 取此值即可保证不丢、不重。
struct WatchResult {
    int32_t code = Code::OK;          // OK / COMPACTED / INTERNAL(overflow)
    std::string message;
    int64_t current_revision = 0;
    std::vector<WatchEvent> events;   // 升序（按 revision）
};

// 成员变更（AddPeer/RemovePeer）结果（M6）。code 语义对齐 configraft.v1.Code：
// OK / NOT_LEADER（请求落到 Follower，message 含 leader 地址）/ INTERNAL。
struct ConfChangeResult {
    int32_t code = Code::OK;
    std::string message;
};

// 节点抽象。
//   - 单机模式（M1）：LocalNode 同步执行；
//   - 集群模式（M2+）：RaftNode 经 braft 提交复制日志，同步等待 apply 完成。
// 服务层只面向本接口编程，使两种模式无缝切换。
class ConfigNode {
public:
    virtual ~ConfigNode() = default;

    // ---- 写路径：同步等待 apply 完成 ----
    virtual void Apply(const RaftCmd& cmd, ApplyResult* out) = 0;

    // ---- 读路径 ----
    virtual void Get(const std::string& key, bool serializable, GetResult* out) = 0;
    virtual void GetConfig(const std::string& key, int64_t version, ConfigResult* out) = 0;

    // ---- Watch 长轮询（M4） ----
    // 阻塞式长轮询：重放 (from_revision, now] 历史事件并等待新事件，直到有事件 /
    // 超时 / server 取消 / overflow。不要求必须落在 Leader（每节点本地事件流随副本
    // apply 推进，以 revision 对齐）。server_deadline_us<=0 表示无服务器侧截止。
    // canceled 非空时在等待循环中轮询（客户端断开/服务器关闭连接时提前退出）。
    virtual void Watch(const std::string& key, int64_t from_revision, int64_t timeout_ms,
                       int64_t server_deadline_us, const std::function<bool()>* canceled,
                       WatchResult* out) = 0;

    // ---- 维护 ----
    // 回收过期 MVCC 历史版本（每 key 保留最近 keep_versions 个）。返回删除条数。
    virtual int Compaction(int keep_versions) = 0;

    // ---- 成员变更（M6） ----
    // 在线加入/移除一个 Raft 成员（形如 "127.0.0.1:8004"）。仅 Leader 可执行，
    // 非 Leader 返回 NOT_LEADER + leader 地址；单机模式（LocalNode）不支持。
    virtual void AddPeer(const std::string& peer, ConfChangeResult* out) = 0;
    virtual void RemovePeer(const std::string& peer, ConfChangeResult* out) = 0;

    // ---- 元信息（Admin / 监控） ----
    virtual bool IsLeader() const = 0;
    virtual std::string LeaderId() const = 0;
    virtual std::string Role() const = 0;
    virtual int64_t Term() const = 0;
    virtual int64_t CommitIndex() const = 0;
    virtual int64_t AppliedIndex() const = 0;
    virtual std::vector<std::string> Peers() const = 0;
    virtual int64_t CurrentRevision() const = 0;
};

}  // namespace configraft
