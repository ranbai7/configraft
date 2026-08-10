#pragma once
#include <memory>
#include <string>

#include "brpc/server.h"
#include "raft/node.h"
#include "server/kv_service_impl.h"

namespace configraft {

// 启动参数（由 server_main.cpp 从 gflags 解析）
struct ServerOptions {
    int port = 8000;
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
    ~ConfigraftServer() = default;

    // 初始化节点与各 service，并绑定端口（不阻塞）。
    bool Init(const ServerOptions& opts, std::string* err);

    // 阻塞运行，直至进程退出信号。
    void RunUntilAskedToQuit();

    const ServerOptions& options() const { return opts_; }

private:
    ServerOptions opts_;
    std::unique_ptr<ConfigNode> node_;
    std::unique_ptr<KVServiceImpl> kv_svc_;
    brpc::Server server_;
};

}  // namespace configraft
