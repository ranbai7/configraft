#pragma once
#include <memory>

#include "braft/raft.h"
#include "braft/snapshot.h"
#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "butil/atomicops.h"
#include "configraft.pb.h"
#include "raft/node.h"
#include "store/store.h"

namespace configraft {

class WatchHub;  // watch/watch_hub.h

// 写指令 apply 完成的同步等待句柄。
//
// 流程：RaftNode::Apply 序列化 RaftCmd 并作为 braft::Task 提交，等待端
// 阻塞在 Wait()；状态机 on_apply 执行完后调用 Finish() 填充结果，
// AsyncClosureGuard 随后异步触发 Run() 唤醒等待端。
// 若任务提交即失败（如 leader 变更、apply 异常），braft 直接调用 Run()，
// 此时 status() 携带错误。
class ApplyClosure : public braft::Closure {
public:
    // 等待状态独立于本对象（shared_ptr 管理）：Run() 在 braft 线程中 `delete this`，
    // 等待侧须在 this 释放后仍能安全等待。bthread::cv 的 wait 挂起 bthread 并释放
    // pthread worker——高并发写下 std::cv 会占满 worker 导致节点假死（压测复现）。
    struct WaitState {
        bthread::Mutex mu;
        bthread::ConditionVariable cv;
        bool done = false;
    };

    explicit ApplyClosure(ApplyResult* out, std::shared_ptr<WaitState> st)
        : out_(out), st_(std::move(st)) {}

    // 同步等待 apply 完成。必须传独立 WaitState（Apply 侧局部持有），**不能访问
    // 本对象成员**：braft 可能在 node_->apply() 返回前就调用 Run()（任务快速完成
    // 或提交即失败），`delete this` 后访问 st_ 是 UAF（fsync=false 高吞吐压测复现
    // 为 bthread_mutex_lock segfault）。wait 挂起 bthread 释放 pthread worker。
    static void WaitFor(WaitState& st) {
        std::unique_lock<bthread::Mutex> lock(st.mu);
        st.cv.wait(lock, [&] { return st.done; });
    }

    // on_apply 中调用：填充串行 apply 的结果（在 Run 之前）。
    void Finish(const ApplyResult& result) { *out_ = result; }

    void Run() override {
        // 自动释放本对象（new 出来的）；st_ 由 shared_ptr 管理，独立存活
        std::unique_ptr<ApplyClosure> self(this);
        if (!status().ok() && out_->code == Code::OK) {
            out_->code = Code::INTERNAL;
            out_->message = status().error_str();
        }
        {
            std::unique_lock<bthread::Mutex> lock(st_->mu);
            st_->done = true;
        }
        st_->cv.notify_one();
    }

private:
    ApplyResult* out_;
    std::shared_ptr<WaitState> st_;
};

// braft 复制状态机：持有 Store，on_apply 串行应用复制日志。
// 这是"状态只在 on_apply 修改"的核心——所有节点经同一串行路径变更状态。
// hub_（非拥有）用于在 on_apply 后广播 Watch 事件；Leader 与 Follower 都要广播，
// 保证 Watch 可落在任意节点（每节点事件流随本地 apply 推进）。
class ConfigraftStateMachine : public braft::StateMachine {
public:
    ConfigraftStateMachine(Store* store, WatchHub* hub) : store_(store), hub_(hub) {}

    // @braft::StateMachine
    void on_apply(braft::Iterator& iter) override;
    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override;
    int on_snapshot_load(braft::SnapshotReader* reader) override;
    void on_leader_start(int64_t term) override;
    void on_leader_stop(const butil::Status& status) override;
    void on_shutdown() override {}
    void on_error(const ::braft::Error& e) override;
    void on_configuration_committed(const ::braft::Configuration& conf) override;
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) override;
    void on_start_following(const ::braft::LeaderChangeContext& ctx) override;

    bool is_leader() const {
        return leader_term_.load(butil::memory_order_acquire) > 0;
    }
    int64_t leader_term() const {
        return leader_term_.load(butil::memory_order_relaxed);
    }
    Store* store() { return store_; }

private:
    struct SnapshotArg {
        Store* store;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };
    static void* save_snapshot(void* arg);

    Store* store_;  // 不拥有
    WatchHub* hub_;  // 不拥有；用于 on_apply 后广播 Watch 事件
    butil::atomic<int64_t> leader_term_{-1};
};

}  // namespace configraft
