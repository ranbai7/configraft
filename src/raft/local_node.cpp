#include "raft/local_node.h"

#include <utility>

#include "store/store_ops.h"

namespace configraft {

LocalNode::LocalNode(std::unique_ptr<Store> store) : store_(std::move(store)) {}

void LocalNode::Apply(const RaftCmd& cmd, ApplyResult* out) {
    ApplyCmdToStore(store_.get(), cmd, out);
}

void LocalNode::Get(const std::string& key, bool serializable, GetResult* out) {
    // 单机模式下 serializable 与线性一致等价
    KV kv;
    int32_t code = 0;
    if (!store_->Get(key, &kv, &code)) {
        out->code = code;
        out->message = "key not found";
        return;
    }
    out->code = Code::OK;
    out->kv = std::move(kv);
}

void LocalNode::GetConfig(const std::string& key, int64_t version, ConfigResult* out) {
    // M3 实现配置多版本查询
    (void)key;
    (void)version;
    out->code = Code::INTERNAL;
    out->message = "GetConfig not implemented yet";
}

int64_t LocalNode::CurrentRevision() const { return store_->CurrentRevision(); }

}  // namespace configraft
