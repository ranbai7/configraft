#pragma once
#include <memory>
#include <string>
#include <vector>

#include "brpc/server.h"
#include "raft/node.h"
#include "raft/state_machine.h"
#include "store/store.h"

namespace configraft {

class WatchHub;  // watch/watch_hub.h

// 集群模式的 ConfigNode：写路径经 braft 复制日志同步提交，读路径走本地状态机。
// 与 LocalNode 共用 ApplyCmdToStore（保证状态变更逻辑一致）。
// hub_（非拥有）注入状态机用于 on_apply 广播 Watch 事件，并供本节点实现长轮询。
class RaftNode : public ConfigNode {
public:
    explicit RaftNode(WatchHub* hub);
    ~RaftNode() override;

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // 初始化：持有 Store + 创建状态机 + 启动 braft Node。
    // store 由调用方（server）统一打开并移交，避免 LevelDB LOCK 重复占用。
    //   group      Raft 复制组名
    //   listen_ip  本节点监听地址（与 brpc server 端口一致）
    //   port       本节点端口（也是 braft PeerId 端口）
    //   peers      初始成员，如 "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
    //   data_root  节点数据根目录（LevelDB 于其/data，Raft 元数据于其/raft）
    bool Init(std::unique_ptr<Store> store, const std::string& group,
              const std::string& listen_ip, int port, const std::string& peers,
              const std::string& data_root, int election_timeout_ms,
              std::string* err);

    // 把 braft 的 RPC 服务注册到 brpc Server（共享端口，须在 server.Start 前调用）
    bool AddServiceToServer(brpc::Server* server, int port);

    // @ConfigNode
    void Apply(const RaftCmd& cmd, ApplyResult* out) override;
    void Get(const std::string& key, bool serializable, GetResult* out) override;

    // ---- 线性一致读辅助（M5） ----
    // 等待 leader lease 变为 VALID（数据追平且无脑裂双主）。NOT_READY 时内部分自旋至
    // deadline；EXPIRED/DISABLED 立即返回 false。超时返回 false。
    bool WaitLeaderLease(int64_t timeout_ms);
    // 等待本地状态机 applied >= committed（单次 get_status 采样，避免跨快照误判），
    // 超时返回 false。
    bool WaitAppliedCatchUp(int64_t timeout_ms);
    void GetConfig(const std::string& key, int64_t version, ConfigResult* out) override;
    void Watch(const std::string& key, int64_t from_revision, int64_t timeout_ms,
               int64_t server_deadline_us, const std::function<bool()>* canceled,
               WatchResult* out) override;
    int Compaction(int keep_versions) override;
    bool IsLeader() const override;
    std::string LeaderId() const override;
    std::string Role() const override;
    int64_t Term() const override;
    int64_t CommitIndex() const override;
    int64_t AppliedIndex() const override;
    std::vector<std::string> Peers() const override;
    int64_t CurrentRevision() const override;

    // ---- 成员变更（M6） ----
    // 同步封装 braft 的 add_peer/remove_peer（异步 Closure → 条件变量等待）。
    // 仅 Leader 可执行；add_peer 会等待新节点数据追平（快照/日志）后才提交配置。
    void AddPeer(const std::string& peer, ConfChangeResult* out) override;
    void RemovePeer(const std::string& peer, ConfChangeResult* out) override;

private:
    std::unique_ptr<Store> store_;
    std::unique_ptr<ConfigraftStateMachine> fsm_;
    std::unique_ptr<braft::Node> node_;
    std::string group_;
    WatchHub* hub_;  // 不拥有
};

}  // namespace configraft
