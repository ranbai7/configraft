#pragma once
#include <condition_variable>
#include <memory>
#include <mutex>

#include "braft/raft.h"
#include "braft/snapshot.h"
#include "butil/atomicops.h"
#include "configraft.pb.h"
#include "raft/node.h"
#include "store/store.h"

namespace configraft {

// 写指令 apply 完成的同步等待句柄。
//
// 流程：RaftNode::Apply 序列化 RaftCmd 并作为 braft::Task 提交，等待端
// 阻塞在 Wait()；状态机 on_apply 执行完后调用 Finish() 填充结果，
// AsyncClosureGuard 随后异步触发 Run() 唤醒等待端。
// 若任务提交即失败（如 leader 变更、apply 异常），braft 直接调用 Run()，
// 此时 status() 携带错误。
class ApplyClosure : public braft::Closure {
public:
    explicit ApplyClosure(ApplyResult* out) : out_(out), done_(false) {}

    // on_apply 中调用：填充串行 apply 的结果（在 Run 之前）。
    void Finish(const ApplyResult& result) { *out_ = result; }

    void Run() override {
        // 自动释放本对象（new 出来的）
        std::unique_ptr<ApplyClosure> self(this);
        if (!status().ok() && out_->code == Code::OK) {
            out_->code = Code::INTERNAL;
            out_->message = status().error_str();
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            done_ = true;
        }
        cv_.notify_one();
    }

    void Wait() {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return done_; });
    }

private:
    ApplyResult* out_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool done_;
};

// braft 复制状态机：持有 Store，on_apply 串行应用复制日志。
// 这是"状态只在 on_apply 修改"的核心——所有节点经同一串行路径变更状态。
class ConfigraftStateMachine : public braft::StateMachine {
public:
    explicit ConfigraftStateMachine(Store* store) : store_(store) {}

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
    butil::atomic<int64_t> leader_term_{-1};
};

}  // namespace configraft
