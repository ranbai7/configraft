#pragma once
#include <memory>

#include "raft/node.h"
#include "store/store.h"

namespace configraft {

// 单机模式的 ConfigNode：直接调用 Store 同步执行，无 Raft。
// 集群接入 braft 后由 RaftNode 替代，服务层透明。
class LocalNode : public ConfigNode {
public:
    explicit LocalNode(std::unique_ptr<Store> store);

    void Apply(const RaftCmd& cmd, ApplyResult* out) override;
    void Get(const std::string& key, bool serializable, GetResult* out) override;
    void GetConfig(const std::string& key, int64_t version, ConfigResult* out) override;

    int Compaction(int keep_versions) override;

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
};

}  // namespace configraft
