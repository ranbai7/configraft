#include "server/server.h"

#include <chrono>
#include <memory>
#include <utility>

#include "common/log.h"
#include "raft/local_node.h"
#include "raft/raft_node.h"
#include "store/store.h"

namespace configraft {

namespace {

// RESTful 路径映射：PATH => METHOD（* 匹配部分在 cntl->http_request().unresolved_path()）。
// brpc 的 restful 映射不支持 HTTP 方法前缀，同路径不同方法的语义在 Rest 方法内
// 按 method + unresolved_path 分发（GET/POST/DELETE、/cas、/batch）。
// 未出现在映射中的 gRPC 方法仍通过 /configraft.v1.KVService/Put 等访问。
constexpr char kKVServiceRestMappings[] =
    "/v1/kv/* => Rest,";
constexpr char kConfigServiceRestMappings[] =
    "/v1/config/* => Rest,";

// MVCC 版本回收策略：每 key 保留的最近版本数
constexpr int kKeepVersions = 10;
constexpr int kCompactionIntervalSeconds = 60;

}  // namespace

bool ConfigraftServer::Init(const ServerOptions& opts, std::string* err) {
    opts_ = opts;
    const bool cluster = !opts_.node_name.empty();
    const std::string store_dir = opts_.data_dir + "/data";

    // 1. 打开存储（LevelDB 目录：data_dir/data；集群下 Raft 元数据于 data_dir/raft）
    auto store = std::make_unique<Store>();
    std::string store_err;
    if (!store->Open(store_dir, &store_err)) {
        if (err) {
            *err = "open store " + store_dir + " failed: " + store_err;
        }
        return false;
    }
    LOG(INFO) << "store opened at " << store_dir;

    // 2. 创建节点
    //    - 集群模式（RaftNode）：Raft RPC 需在 server.Start 前注册（共享端口）
    //    - 单机模式（LocalNode）：同步执行
    if (cluster) {
        auto raft_node = std::make_unique<RaftNode>();
        if (!raft_node->AddServiceToServer(&server_, opts_.port)) {
            if (err) {
                *err = "fail to add raft service";
            }
            return false;
        }
        node_ = std::move(raft_node);
    } else {
        node_ = std::make_unique<LocalNode>(std::move(store));
    }

    // 3. 创建服务并注册到 brpc Server（单端口：gRPC + HTTP + Raft 内部通信 + 监控面板）
    kv_svc_ = std::make_unique<KVServiceImpl>(node_.get());
    if (server_.AddService(kv_svc_.get(), brpc::SERVER_DOESNT_OWN_SERVICE,
                           kKVServiceRestMappings) != 0) {
        if (err) {
            *err = "fail to add KVService";
        }
        return false;
    }
    config_svc_ = std::make_unique<ConfigServiceImpl>(node_.get());
    if (server_.AddService(config_svc_.get(), brpc::SERVER_DOESNT_OWN_SERVICE,
                           kConfigServiceRestMappings) != 0) {
        if (err) {
            *err = "fail to add ConfigService";
        }
        return false;
    }

    brpc::ServerOptions brpc_opts;
    brpc_opts.num_threads = 4;
    if (server_.Start(opts_.port, &brpc_opts) != 0) {
        if (err) {
            *err = "fail to start brpc server on port " + std::to_string(opts_.port);
        }
        return false;
    }

    // 4. 集群模式：server 就绪后再启动 Raft 节点（避免成为 Leader 时服务不可达）
    if (cluster) {
        auto* raft_node = static_cast<RaftNode*>(node_.get());
        if (!raft_node->Init(std::move(store), "configraft", opts_.listen_ip,
                             opts_.port, opts_.peers, opts_.data_dir,
                             opts_.election_timeout_ms, err)) {
            return false;
        }
    }

    LOG(INFO) << "Configraft server listening on " << opts_.listen_ip << ":"
              << opts_.port << " (mode=" << (cluster ? "cluster" : "single")
              << ", gRPC / HTTP / raft / status)";

    // 5. 启动后台 Compaction 循环（回收过期 MVCC 历史版本）
    StartCompactionLoop();
    return true;
}

ConfigraftServer::~ConfigraftServer() {
    stop_compaction_ = true;
    if (compaction_thread_.joinable()) {
        compaction_thread_.join();
    }
}

void ConfigraftServer::RunUntilAskedToQuit() { server_.RunUntilAskedToQuit(); }

void ConfigraftServer::StartCompactionLoop() {
    compaction_thread_ = std::thread([this] {
        while (!stop_compaction_.load()) {
            std::this_thread::sleep_for(
                std::chrono::seconds(kCompactionIntervalSeconds));
            const int removed = node_->Compaction(kKeepVersions);
            if (removed > 0) {
                LOG(INFO) << "compaction removed " << removed << " stale versions";
            }
        }
    });
}

}  // namespace configraft
