#pragma once
#include <memory>
#include <mutex>

#include "raft/node.h"
#include "store/store.h"

namespace configraft {

class WatchHub;  // watch/watch_hub.h

// 单机模式的 ConfigNode：直接调用 Store 同步执行，无 Raft。
// 集群接入 braft 后由 RaftNode 替代，服务层透明。
// hub_（非拥有）用于 apply 后广播 Watch 事件与实现长轮询。
class LocalNode : public ConfigNode {
public:
    LocalNode(std::unique_ptr<Store> store, WatchHub* hub);

    void Apply(const RaftCmd& cmd, ApplyResult* out) override;
    void Get(const std::string& key, bool serializable, GetResult* out) override;
    void GetConfig(const std::string& key, int64_t version, ConfigResult* out) override;
    void Watch(const std::string& key, int64_t from_revision, int64_t timeout_ms,
               int64_t server_deadline_us, const std::function<bool()>* canceled,
               WatchResult* out) override;

    int Compaction(int keep_versions) override;

    // 单机模式无 Raft 成员，成员变更不支持
    void AddPeer(const std::string& peer, ConfChangeResult* out) override;
    void RemovePeer(const std::string& peer, ConfChangeResult* out) override;

    bool IsLeader() const override { return true; }
    std::string LeaderId() const override { return ""; }
    std::string Role() const override { return "leader"; }
    int64_t Term() const override { return 0; }
    int64_t CommitIndex() const override { return 0; }
    int64_t AppliedIndex() const override { return 0; }
    std::vector<std::string> Peers() const override { return {}; }
    int64_t CurrentRevision() const override;

private:
    // 把 RaftCmd 分发到 Store 各写方法（单机直接执行，集群走 on_apply 同一逻辑）
    void ApplyPut(const RaftCmd::PutCmd& cmd, ApplyResult* out);
    void ApplyDelete(const RaftCmd::DeleteCmd& cmd, ApplyResult* out);
    void ApplyBatch(const RaftCmd::BatchPutCmd& cmd, ApplyResult* out);

    std::unique_ptr<Store> store_;
    WatchHub* hub_;  // 不拥有

    // 写路径串行化：集群模式由 braft on_apply 天然串行；单机模式 brpc 多 worker
    // 并发调用 Apply，须在此串行，否则 Store 的 revision 读-改-写会拿到重复值
    // （审查发现 H2——并发 Put/CAS 下全局 revision 重复、Watch 续传错乱）。
    std::mutex mu_;
};

}  // namespace configraft
