#include "raft/raft_node.h"

#include <utility>

#include "braft/raft.h"
#include "butil/endpoint.h"
#include "butil/iobuf.h"
#include "common/log.h"
#include "watch/watch_hub.h"

namespace configraft {

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

    auto* closure = new ApplyClosure(out);
    braft::Task task;
    task.data = &log;
    task.done = closure;
    const int64_t term = fsm_->leader_term();
    if (term > 0) {
        // 防 ABA：若 apply 前任期已变（本节点不再是该任期 leader），任务作废
        task.expected_term = term;
    }
    node_->apply(task);

    // 阻塞当前 bthread 直到 on_apply 完成（或任务失败）
    closure->Wait();
}

// ---------------- 读路径 ----------------

void RaftNode::Get(const std::string& key, bool serializable, GetResult* out) {
    if (!serializable && !IsLeader()) {
        // 线性一致读要求 Leader（M5 起以 ReadIndex 保证，目前先直接 Leader 读）
        out->code = Code::NOT_LEADER;
        out->leader_id = LeaderId();
        return;
    }
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
    std::vector<braft::PeerId> peers;
    if (node_->list_peers(&peers).ok()) {
        for (const auto& p : peers) {
            result.push_back(p.to_string());
        }
    }
    return result;
}

int64_t RaftNode::CurrentRevision() const { return store_->CurrentRevision(); }

int RaftNode::Compaction(int keep_versions) {
    return store_->Compaction(keep_versions);
}

}  // namespace configraft
