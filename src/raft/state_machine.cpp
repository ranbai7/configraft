#include "raft/state_machine.h"

#include <memory>

#include "brpc/closure_guard.h"
#include "braft/protobuf_file.h"
#include "bthread/bthread.h"
#include "butil/iobuf.h"
#include "common/log.h"
#include "common/storekey.h"
#include "store/store_ops.h"

namespace configraft {

void ConfigraftStateMachine::on_apply(braft::Iterator& iter) {
    // 一批已提交的日志，必须通过 iter 逐个处理
    for (; iter.valid(); iter.next()) {
        // 保证 iter.done()（若存在）在本批结束后异步 Run，避免阻塞状态机
        braft::AsyncClosureGuard closure_guard(iter.done());

        // 解析复制日志中的 RaftCmd
        RaftCmd cmd;
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        if (!cmd.ParseFromZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "fail to parse raft cmd at log_index=" << iter.index();
            if (iter.done()) {
                iter.done()->status().set_error(EINVAL, "bad raft cmd");
            }
            continue;
        }

        // 串行应用到存储（所有节点执行同一逻辑）
        ApplyResult result;
        ApplyCmdToStore(store_, cmd, &result);

        // 本节点提交的请求：填充结果并唤醒等待者
        if (iter.done()) {
            ApplyClosure* closure = dynamic_cast<ApplyClosure*>(iter.done());
            if (closure) {
                closure->Finish(result);
            }
        }
    }
}

void ConfigraftStateMachine::on_leader_start(int64_t term) {
    leader_term_.store(term, butil::memory_order_release);
    LOG(INFO) << "become leader at term " << term;
}

void ConfigraftStateMachine::on_leader_stop(const butil::Status& status) {
    leader_term_.store(-1, butil::memory_order_release);
    LOG(INFO) << "step down as leader: " << status;
}

void ConfigraftStateMachine::on_error(const ::braft::Error& e) {
    LOG(ERROR) << "met raft error: " << e;
}

void ConfigraftStateMachine::on_configuration_committed(
    const ::braft::Configuration& conf) {
    LOG(INFO) << "configuration committed: " << conf;
}

void ConfigraftStateMachine::on_stop_following(
    const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "stop following " << ctx;
}

void ConfigraftStateMachine::on_start_following(
    const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "start following " << ctx;
}

// ---------------- 快照保存/加载 ----------------

void* ConfigraftStateMachine::save_snapshot(void* arg) {
    SnapshotArg* sa = static_cast<SnapshotArg*>(arg);
    std::unique_ptr<SnapshotArg> arg_guard(sa);
    brpc::ClosureGuard done_guard(sa->done);

    const std::string path = sa->writer->get_path() + "/data";

    // 用 LevelDB 快照导出一致状态（遍历期间新 apply 不影响导出的数据）
    leveldb::ReadOptions ro;
    ro.snapshot = sa->store->db()->GetSnapshot();
    SnapshotData data;
    data.set_revision(sa->store->CurrentRevision());

    std::unique_ptr<leveldb::Iterator> it(sa->store->db()->NewIterator(ro));
    for (it->Seek(storekey::MainKey("")); it->Valid() && it->key().starts_with("k/");
         it->Next()) {
        KV kv;
        if (kv.ParseFromString(it->value().ToString())) {
            data.add_kvs()->CopyFrom(kv);
        }
    }
    it.reset();
    sa->store->db()->ReleaseSnapshot(ro.snapshot);

    // 序列化快照文件
    braft::ProtoBufFile pb_file(path);
    if (pb_file.save(&data, true) != 0) {
        sa->done->status().set_error(EIO, "fail to save snapshot file");
        return nullptr;
    }
    if (sa->writer->add_file("data") != 0) {
        sa->done->status().set_error(EIO, "fail to add snapshot file");
        return nullptr;
    }
    LOG(INFO) << "snapshot saved to " << path;
    return nullptr;
}

void ConfigraftStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                                              braft::Closure* done) {
    // 快照导出可能较慢，放到独立 bthread 避免阻塞状态机
    SnapshotArg* arg = new SnapshotArg{store_, writer, done};
    bthread_t tid;
    bthread_start_urgent(&tid, nullptr, save_snapshot, arg);
}

int ConfigraftStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
    if (reader->get_file_meta("data", nullptr) != 0) {
        LOG(ERROR) << "fail to find snapshot file `data'";
        return -1;
    }
    braft::ProtoBufFile pb_file(reader->get_path() + "/data");
    SnapshotData data;
    if (pb_file.load(&data) != 0) {
        LOG(ERROR) << "fail to load snapshot from " << reader->get_path();
        return -1;
    }
    std::string err;
    if (!store_->LoadSnapshot(data, &err)) {
        LOG(ERROR) << "fail to load snapshot into store: " << err;
        return -1;
    }
    LOG(INFO) << "snapshot loaded, revision=" << data.revision()
              << " kvs=" << data.kvs_size();
    return 0;
}

}  // namespace configraft
