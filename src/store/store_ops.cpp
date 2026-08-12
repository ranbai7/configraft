#include "store/store_ops.h"

#include <utility>

namespace configraft {

namespace {

// 把写入后的 KV 转成 Watch 事件（type 由 tombstone 决定）。
// KV 的 revision 由 Store 写路径填充（每次写全局 +1），是事件的有序锚点。
WatchEvent MakeEvent(const KV& kv) {
    WatchEvent ev;
    ev.set_key(kv.key());
    ev.set_value(kv.value());
    ev.set_version(kv.version());
    ev.set_revision(kv.revision());
    ev.set_type(kv.deleted() ? "DELETE" : "PUT");
    return ev;
}

void ApplyPut(Store* store, const RaftCmd::PutCmd& cmd, ApplyResult* out) {
    KV kv;
    const int64_t rev = store->Put(cmd.key(), cmd.value(), &kv);
    if (rev < 0) {
        out->code = Code::INTERNAL;
        out->message = "store write failed";
        return;
    }
    out->code = Code::OK;
    out->kv = std::move(kv);
    out->events.push_back(MakeEvent(out->kv));
}

void ApplyDelete(Store* store, const RaftCmd::DeleteCmd& cmd, ApplyResult* out) {
    KV kv;
    const int64_t rev = store->Delete(cmd.key(), &kv);
    if (rev < 0) {
        out->code = Code::INTERNAL;
        out->message = "store write failed";
        return;
    }
    out->code = Code::OK;
    out->kv = std::move(kv);
    out->events.push_back(MakeEvent(out->kv));
}

void ApplyPublish(Store* store, const RaftCmd::PublishCmd& cmd, ApplyResult* out) {
    KV kv;
    const int64_t rev = store->Publish(cmd.key(), cmd.value(), &kv);
    if (rev < 0) {
        out->code = Code::INTERNAL;
        out->message = "store publish failed";
        return;
    }
    out->code = Code::OK;
    out->kv = std::move(kv);
    out->events.push_back(MakeEvent(out->kv));
}

void ApplyRollback(Store* store, const RaftCmd::RollbackCmd& cmd, ApplyResult* out) {
    bool ok = false;
    KV kv;
    const int64_t rev = store->Rollback(cmd.key(), cmd.target_version(), &kv, &ok);
    if (!ok) {
        out->code = Code::VERSION_NOT_FOUND;
        out->message = "target version not found";
        return;
    }
    if (rev < 0) {
        out->code = Code::INTERNAL;
        out->message = "store rollback failed";
        return;
    }
    out->code = Code::OK;
    out->kv = std::move(kv);
    out->events.push_back(MakeEvent(out->kv));
}

void ApplyBatch(Store* store, const RaftCmd::BatchPutCmd& cmd, ApplyResult* out) {
    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(cmd.puts_size());
    for (const auto& p : cmd.puts()) {
        kvs.emplace_back(p.key(), p.value());
    }
    std::vector<KV> new_kvs;
    const int64_t rev = store->BatchPut(kvs, &new_kvs);
    if (rev < 0) {
        out->code = Code::INTERNAL;
        out->message = "store batch write failed";
        return;
    }
    out->code = Code::OK;
    out->kvs = std::move(new_kvs);
    out->events.reserve(new_kvs.size());
    for (const auto& kv : out->kvs) {
        out->events.push_back(MakeEvent(kv));
    }
}

}  // namespace

void ApplyCmdToStore(Store* store, const RaftCmd& cmd, ApplyResult* out) {
    switch (cmd.cmd_case()) {
        case RaftCmd::CmdCase::kPut:
            ApplyPut(store, cmd.put(), out);
            break;
        case RaftCmd::CmdCase::kDelete:
            ApplyDelete(store, cmd.delete_(), out);
            break;
        case RaftCmd::CmdCase::kBatchPut:
            ApplyBatch(store, cmd.batch_put(), out);
            break;
        case RaftCmd::CmdCase::kCas:
            // M5 实现 CAS（比较-写入，串行 apply 天然原子）
            out->code = Code::INTERNAL;
            out->message = "CAS not implemented yet";
            break;
        case RaftCmd::CmdCase::kPublish:
            ApplyPublish(store, cmd.publish(), out);
            break;
        case RaftCmd::CmdCase::kRollback:
            ApplyRollback(store, cmd.rollback(), out);
            break;
        default:
            out->code = Code::INTERNAL;
            out->message = "unknown raft cmd";
            break;
    }
}

}  // namespace configraft
