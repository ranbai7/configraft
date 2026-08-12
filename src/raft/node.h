#pragma once
#include <cstdint>
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
