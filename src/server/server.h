#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "brpc/server.h"
#include "raft/node.h"
#include "server/config_service_impl.h"
#include "server/kv_service_impl.h"
#include "server/watch_service_impl.h"
#include "watch/watch_hub.h"

namespace configraft {

// 启动参数（由 server_main.cpp 从 gflags 解析）
struct ServerOptions {
    int port = 8000;
    std::string listen_ip = "127.0.0.1";
    std::string data_dir = "data";
    // 集群模式（M2 起）
    std::string node_name;  // 为空 → 单机模式（LocalNode）
    std::string peers;      // 例如 "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
    int election_timeout_ms = 1000;
};

// 服务进程组装：Store + ConfigNode(Local/Raft) + 各 protobuf service + brpc Server。
// brpc 单端口同时服务 gRPC、HTTP(REST)、Raft 内部通信及内置监控面板。
class ConfigraftServer {
public:
    ConfigraftServer() = default;
    ~ConfigraftServer();

    // 初始化节点与各 service，并绑定端口（不阻塞）。
    bool Init(const ServerOptions& opts, std::string* err);

    // 阻塞运行，直至进程退出信号。
    void RunUntilAskedToQuit();

    const ServerOptions& options() const { return opts_; }

private:
    // 后台 Compaction 循环：定时回收过期 MVCC 历史版本
    void StartCompactionLoop();

    ServerOptions opts_;
    // watch_hub_ 先声明（析构逆序后销毁）：node_（Local/RaftNode）与状态机都持有
    // hub 的非拥有指针，必须保证 hub 存活到它们全部析构之后。
    std::unique_ptr<WatchHub> watch_hub_;
    std::unique_ptr<ConfigNode> node_;
    std::unique_ptr<KVServiceImpl> kv_svc_;
    std::unique_ptr<ConfigServiceImpl> config_svc_;
    std::unique_ptr<WatchServiceImpl> watch_svc_;
    brpc::Server server_;
    std::thread compaction_thread_;
    std::atomic<bool> stop_compaction_{false};
};

}  // namespace configraft
