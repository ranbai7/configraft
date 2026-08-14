#include "raft/local_node.h"

#include <utility>

#include "store/store_ops.h"
#include "watch/watch_hub.h"

namespace configraft {

LocalNode::LocalNode(std::unique_ptr<Store> store, WatchHub* hub)
    : store_(std::move(store)), hub_(hub) {}

void LocalNode::Apply(const RaftCmd& cmd, ApplyResult* out) {
    ApplyCmdToStore(store_.get(), cmd, out);
    // 单机模式同样广播 Watch 事件（LocalNode 也要支持长轮询）
    if (hub_ && !out->events.empty()) {
        hub_->Broadcast(out->events);
    }
}

void LocalNode::Watch(const std::string& key, int64_t from_revision, int64_t timeout_ms,
                      int64_t server_deadline_us, const std::function<bool()>* canceled,
                      WatchResult* out) {
    if (hub_) {
        hub_->Watch(key, from_revision, timeout_ms, server_deadline_us, canceled,
                    store_.get(), out);
    } else {
        out->code = Code::INTERNAL;
        out->message = "watch hub unavailable";
    }
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
    KV kv;
    int32_t code = 0;
    if (!store_->GetConfig(key, version, &kv, &code)) {
        out->code = code;
        out->message = (code == Code::VERSION_NOT_FOUND) ? "version not found"
                                                         : "key not found";
        return;
    }
    out->code = Code::OK;
    out->kv = std::move(kv);
    store_->GetHistory(key, &out->history);
}

int64_t LocalNode::CurrentRevision() const { return store_->CurrentRevision(); }

int LocalNode::Compaction(int keep_versions) {
    return store_->Compaction(keep_versions);
}

void LocalNode::AddPeer(const std::string& peer, ConfChangeResult* out) {
    out->code = Code::INTERNAL;
    out->message = "single-node mode does not support membership change";
}

void LocalNode::RemovePeer(const std::string& peer, ConfChangeResult* out) {
    out->code = Code::INTERNAL;
    out->message = "single-node mode does not support membership change";
}

}  // namespace configraft
