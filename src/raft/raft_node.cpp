#include "raft/raft_node.h"

#include <memory>
#include <string>
#include <utility>

#include "braft/raft.h"
#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "butil/endpoint.h"
#include "butil/iobuf.h"
#include "butil/time.h"
#include "common/log.h"
#include "watch/watch_hub.h"

namespace configraft {

namespace {
// 线性一致读等待上限（毫秒）。lease 从 NOT_READY 到 VALID 需一次心跳往返，
// 一个 election_timeout 足够；applied 追平 commit 同理。
constexpr int64_t kLeaseWaitMs = 1000;
constexpr int64_t kCatchupWaitMs = 1000;

// 成员变更等待上限。add_peer 内部等新节点追平数据（braft catchup_timeout 默认
// 较大），remove_peer 等配置变更日志多数派提交。超时后返回，但底层变更仍可能
// 继续，须提示用户稍后查询确认。
constexpr int kConfChangeTimeoutSec = 60;

// braft add_peer/remove_peer 的完成句柄（异步，在 bthread 中回调）。
// 结果缓冲用 shared_ptr 而非裸指针：若上层同步等待超时先返回，closure 仍存活，
// 将来 Run() 写入 shared_ptr 安全，不会悬垂访问栈上变量。
struct ConfChangeOutcome {
    // 用 bthread 同步原语：等待发生在 bthread 中，std::cv 会占住 pthread worker
    //（高并发写场景下 worker 耗尽假死，见 ApplyClosure 同款注释）。
    bthread::Mutex mu;
    bthread::ConditionVariable cv;
    bool done = false;
    butil::Status status;
};

class ConfChangeClosure : public braft::Closure {
public:
    explicit ConfChangeClosure(std::shared_ptr<ConfChangeOutcome> out)
        : out_(std::move(out)) {}

    void Run() override {
        // 自动释放本对象（new 出来的）
        std::unique_ptr<ConfChangeClosure> self(this);
        {
            std::unique_lock<bthread::Mutex> lock(out_->mu);
            out_->status = status();
            out_->done = true;
        }
        out_->cv.notify_all();
    }

private:
    std::shared_ptr<ConfChangeOutcome> out_;
};
}  // namespace

RaftNode::RaftNode(WatchHub* hub) : hub_(hub) {}

RaftNode::~RaftNode() {
    if (node_) {
        node_->shutdown(nullptr);
        node_->join();
    }
}

bool RaftNode::Init(std::unique_ptr<Store> store, const std::string& group,
                    const std::string& listen_ip, int port,
                    const std::string& peers, const std::string& data_root,
                    int election_timeout_ms, std::string* err) {
    group_ = group;
    store_ = std::move(store);

    // 1. 创建状态机（持有 store + WatchHub，on_apply 广播事件）
    fsm_ = std::make_unique<ConfigraftStateMachine>(store_.get(), hub_);

    // 3. 构造监听地址（与 brpc server 同端口）
    butil::EndPoint addr;
    if (butil::str2endpoint(listen_ip.c_str(), port, &addr) != 0) {
        if (err) {
            *err = "invalid listen address " + listen_ip + ":" + std::to_string(port);
        }
        return false;
    }

    // 4. 配置并启动 braft Node
    braft::NodeOptions options;
    if (options.initial_conf.parse_from(peers.c_str()) != 0) {
        if (err) {
            *err = "fail to parse peers `" + peers + "'";
        }
        return false;
    }
    options.election_timeout_ms = election_timeout_ms;
    options.fsm = fsm_.get();
    options.node_owns_fsm = false;
    options.snapshot_interval_s = 30;
    const std::string prefix = "local://" + data_root + "/raft";
    options.log_uri = prefix + "/log";
    options.raft_meta_uri = prefix + "/raft_meta";
    options.snapshot_uri = prefix + "/snapshot";

    node_ = std::make_unique<braft::Node>(group, braft::PeerId(addr));
    if (node_->init(options) != 0) {
        if (err) {
            *err = "fail to init raft node " + group;
        }
        return false;
    }
    LOG(INFO) << "raft node `" << group << "' started at " << listen_ip << ":" << port;
    return true;
}

bool RaftNode::AddServiceToServer(brpc::Server* server, int port) {
    // 把 raft 的 RPC（选举/复制/快照/成员变更）注册到共享 brpc server
    if (braft::add_service(server, port) != 0) {
        LOG(ERROR) << "fail to add raft service";
        return false;
    }
    return true;
}

// ---------------- 写路径：提交复制日志并同步等待 apply ----------------

void RaftNode::Apply(const RaftCmd& cmd, ApplyResult* out) {
    // 仅 Leader 接受写请求；非 Leader 返回重定向信息
    if (!IsLeader()) {
        out->code = Code::NOT_LEADER;
        out->leader_id = LeaderId();
        return;
    }

    // 序列化指令为日志数据
    butil::IOBuf log;
    butil::IOBufAsZeroCopyOutputStream wrapper(&log);
    cmd.SerializeToZeroCopyStream(&wrapper);

    // WaitState 独立于 closure（shared_ptr）：closure 在 Run() 中 delete 自身，
    // 等待状态须存活到 WaitFor 返回之后（避免 UAF，见 ApplyClosure 注释）。
    auto wait_state = std::make_shared<ApplyClosure::WaitState>();
    auto* closure = new ApplyClosure(out, wait_state);
    braft::Task task;
    task.data = &log;
    task.done = closure;
    const int64_t term = fsm_->leader_term();
    if (term > 0) {
        // 防 ABA：若 apply 前任期已变（本节点不再是该任期 leader），任务作废
        task.expected_term = term;
    }
    node_->apply(task);

    // 阻塞当前 bthread 直到 on_apply 完成（或任务失败）。
    // 用独立 wait_state 等待而非 closure->Wait()：braft 可能在 apply() 返回前
    // 就调用 Run() 并 delete closure（任务快速完成/失败，fsync=false 高吞吐复现），
    // 访问 closure 成员是 UAF。
    ApplyClosure::WaitFor(*wait_state);
}

// ---------------- 读路径 ----------------

void RaftNode::Get(const std::string& key, bool serializable, GetResult* out) {
    if (!serializable) {
        // 线性一致读（M5，lease-based）：braft v1.1.2 无 read_index API，用内置 leader lease。
        //   LEASE_VALID ⟹ 本任期首条配置日志（=no-op）已提交 ⟹ commit 覆盖先前任期已提交写
        //   ⟹ (Leader Completeness) 本地状态机含所有已提交写；再等 applied≥commit 补齐异步 apply。
        if (!WaitLeaderLease(kLeaseWaitMs)) {
            if (IsLeader()) {
                // 是 leader 但 lease 未就绪（NOT_READY 超时 / 被禁用）：不能填 LeaderId()
                // 自我重定向（死循环），报 INTERNAL 让客户端重试。
                out->code = Code::INTERNAL;
                out->message = "leader not ready for linearizable read, retry";
            } else {
                out->code = Code::NOT_LEADER;
                out->leader_id = LeaderId();
            }
            return;
        }
        if (!WaitAppliedCatchUp(kCatchupWaitMs)) {
            out->code = Code::INTERNAL;
            out->message = "apply catch-up timeout";
            return;
        }
    }
    // serializable=true：降级读，允许任意节点本地读（最终一致，可容忍过期）。
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

bool RaftNode::WaitLeaderLease(int64_t timeout_ms) {
    if (!node_) {
        return false;
    }
    const int64_t deadline_us = butil::gettimeofday_us() + timeout_ms * 1000LL;
    while (true) {
        braft::LeaderLeaseStatus st;
        node_->get_leader_lease_status(&st);
        switch (st.state) {
            case braft::LEASE_VALID:
                return true;
            case braft::LEASE_EXPIRED:   // 已让位/失去多数派，非 leader
            case braft::LEASE_DISABLED:  // flag 未开启（部署配置错误）
                return false;
            default:  // NOT_READY：刚当选，配置条目/心跳未确认 → 内部分自旋
                if (butil::gettimeofday_us() >= deadline_us) {
                    return false;
                }
                bthread_usleep(5000);  // 挂起当前 bthread，不占 pthread worker
        }
    }
}

bool RaftNode::WaitAppliedCatchUp(int64_t timeout_ms) {
    if (!node_) {
        return false;
    }
    const int64_t deadline_us = butil::gettimeofday_us() + timeout_ms * 1000LL;
    while (true) {
        // 单次 get_status 采样同时取 applied/commit，避免跨快照误判
        //（读到较新的 applied、较旧的 commit 会误以为已追上）。
        braft::NodeStatus status;
        node_->get_status(&status);
        if (status.known_applied_index >= status.committed_index) {
            return true;
        }
        if (butil::gettimeofday_us() >= deadline_us) {
            return false;
        }
        bthread_usleep(100);
    }
}

void RaftNode::GetConfig(const std::string& key, int64_t version, ConfigResult* out) {
    // 配置读取走本地状态机（写路径已保证各节点一致；M5 起对线性一致读引入 ReadIndex）
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

void RaftNode::Watch(const std::string& key, int64_t from_revision, int64_t timeout_ms,
                     int64_t server_deadline_us, const std::function<bool()>* canceled,
                     WatchResult* out) {
    // Watch 不强制落在 Leader：每节点本地事件流随副本 apply 推进，以 revision 对齐。
    // 历史重放从本节点 MVCC v/ 前缀读取（各副本一致），实时事件由本地 on_apply 广播。
    // 注意：Follower 的 current_revision 可能滞后 Leader，客户端以其续传锚点自行推进。
    hub_->Watch(key, from_revision, timeout_ms, server_deadline_us, canceled,
                store_.get(), out);
}

// ---------------- 元信息 ----------------

bool RaftNode::IsLeader() const { return node_ && node_->is_leader(); }

std::string RaftNode::LeaderId() const {
    if (!node_) {
        return "";
    }
    const braft::PeerId leader = node_->leader_id();
    return leader.is_empty() ? "" : leader.to_string();
}

std::string RaftNode::Role() const {
    if (!node_) {
        return "uninitialized";
    }
    braft::NodeStatus status;
    node_->get_status(&status);
    switch (status.state) {
        case braft::STATE_LEADER:
            return "leader";
        case braft::STATE_CANDIDATE:
            return "candidate";
        case braft::STATE_TRANSFERRING:
            return "transferring";
        default:
            return "follower";
    }
}

int64_t RaftNode::Term() const {
    if (!node_) {
        return 0;
    }
    braft::NodeStatus status;
    node_->get_status(&status);
    return status.term;
}

int64_t RaftNode::CommitIndex() const {
    if (!node_) {
        return 0;
    }
    braft::NodeStatus status;
    node_->get_status(&status);
    return status.committed_index;
}

int64_t RaftNode::AppliedIndex() const {
    if (!node_) {
        return 0;
    }
    braft::NodeStatus status;
    node_->get_status(&status);
    return status.known_applied_index;
}

std::vector<std::string> RaftNode::Peers() const {
    std::vector<std::string> result;
    if (!node_) {
        return result;
    }
    // braft list_peers 仅 Leader 返回成员（Follower 返回 EPERM），
    // health 在 Follower 上 peers 为空可接受（可向 Leader 查）。
    std::vector<braft::PeerId> peers;
    if (node_->list_peers(&peers).ok()) {
        for (const auto& p : peers) {
            result.push_back(p.to_string());
        }
    }
    return result;
}

// ---- 成员变更（M6） ----

void RaftNode::AddPeer(const std::string& peer, ConfChangeResult* out) {
    if (!node_) {
        out->code = Code::INTERNAL;
        out->message = "raft node not initialized";
        return;
    }
    if (!IsLeader()) {
        out->code = Code::NOT_LEADER;
        out->message = "not leader, leader=" + LeaderId();
        return;
    }
    braft::PeerId peer_id;
    if (peer_id.parse(peer.c_str()) != 0) {
        out->code = Code::INTERNAL;
        out->message = "invalid peer address `" + peer + "'";
        return;
    }

    // 同步提交配置变更：braft 在 bthread 中回调 done，这里阻塞当前 bthread 等待。
    // add_peer 成功路径：leader 先让新 peer 追平（快照/日志复制），再提交配置日志。
    auto outcome = std::make_shared<ConfChangeOutcome>();
    node_->add_peer(peer_id, new ConfChangeClosure(outcome));

    // bthread cv 无带谓词的 wait_for，用 100ms 粒度轮询实现超时
    std::unique_lock<bthread::Mutex> lock(outcome->mu);
    const int64_t deadline_us =
        butil::gettimeofday_us() + kConfChangeTimeoutSec * 1000000LL;
    while (!outcome->done && butil::gettimeofday_us() < deadline_us) {
        outcome->cv.wait_for(lock, 100000);
    }
    if (!outcome->done) {
        // 超时：底层变更可能仍在进行（新节点追数据很慢 / 网络不通），不能撤销。
        out->code = Code::INTERNAL;
        out->message = "add peer timed out after " +
                       std::to_string(kConfChangeTimeoutSec) +
                       "s (change may still be in progress), check health later";
        return;
    }
    if (!outcome->status.ok()) {
        out->code = Code::INTERNAL;
        out->message = outcome->status.error_str();
        return;
    }
    out->code = Code::OK;
    out->message = "peer " + peer + " added";
}

void RaftNode::RemovePeer(const std::string& peer, ConfChangeResult* out) {
    if (!node_) {
        out->code = Code::INTERNAL;
        out->message = "raft node not initialized";
        return;
    }
    if (!IsLeader()) {
        out->code = Code::NOT_LEADER;
        out->message = "not leader, leader=" + LeaderId();
        return;
    }
    braft::PeerId peer_id;
    if (peer_id.parse(peer.c_str()) != 0) {
        out->code = Code::INTERNAL;
        out->message = "invalid peer address `" + peer + "'";
        return;
    }

    auto outcome = std::make_shared<ConfChangeOutcome>();
    node_->remove_peer(peer_id, new ConfChangeClosure(outcome));

    std::unique_lock<bthread::Mutex> lock(outcome->mu);
    const int64_t deadline_us =
        butil::gettimeofday_us() + kConfChangeTimeoutSec * 1000000LL;
    while (!outcome->done && butil::gettimeofday_us() < deadline_us) {
        outcome->cv.wait_for(lock, 100000);
    }
    if (!outcome->done) {
        out->code = Code::INTERNAL;
        out->message = "remove peer timed out after " +
                       std::to_string(kConfChangeTimeoutSec) +
                       "s (change may still be in progress), check health later";
        return;
    }
    if (!outcome->status.ok()) {
        out->code = Code::INTERNAL;
        out->message = outcome->status.error_str();
        return;
    }
    // 若移除的是本 Leader 自身，braft 会在配置提交后让位（ELEADERREMOVED）。
    out->code = Code::OK;
    out->message = "peer " + peer + " removed";
}

int64_t RaftNode::CurrentRevision() const { return store_->CurrentRevision(); }

int RaftNode::Compaction(int keep_versions) {
    return store_->Compaction(keep_versions);
}

}  // namespace configraft
