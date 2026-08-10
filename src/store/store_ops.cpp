#include "store/store_ops.h"

#include <utility>

namespace configraft {

namespace {

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
            // M3 实现配置发布
            out->code = Code::INTERNAL;
            out->message = "Publish not implemented yet";
            break;
        case RaftCmd::CmdCase::kRollback:
            // M3 实现配置回滚
            out->code = Code::INTERNAL;
            out->message = "Rollback not implemented yet";
            break;
        default:
            out->code = Code::INTERNAL;
            out->message = "unknown raft cmd";
            break;
    }
}

}  // namespace configraft
