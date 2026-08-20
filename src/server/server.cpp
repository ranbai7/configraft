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
constexpr char kWatchServiceRestMappings[] =
    "/v1/watch/* => Rest,";
// Admin 直接 restful 映射到具体方法（HTTP JSON body 自动解析进 request，
// 由 brpc allow_http_body_to_pb 支持）：
//   GET  /healthz     → GetHealth（用 /healthz 而非 /health：brpc 1.17 内置
//                       HealthService 已占用 /health，短名 health 会与其冲突）
//   POST /addpeer     → AddPeer（{"peer":"127.0.0.1:8004"}）
//   POST /removepeer  → RemovePeer（{"peer":"127.0.0.1:8001"}）
constexpr char kAdminServiceRestMappings[] =
    "/healthz => GetHealth,"
    "/addpeer => AddPeer,"
    "/removepeer => RemovePeer,";
// Dashboard 静态资源：/dashboard 与 /dashboard/* 都映射到 Rest 方法
// （* 匹配部分在 unresolved_path，空路径返回 index.html）。
constexpr char kDashboardRestMappings[] =
    "/dashboard => Rest,"
    "/dashboard/* => Rest,";

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

    // 2. 创建事件分发中枢（Watch 长轮询用），再创建节点
    //    - 集群模式（RaftNode）：Raft RPC 需在 server.Start 前注册（共享端口）
    //    - 单机模式（LocalNode）：同步执行
    watch_hub_ = std::make_unique<WatchHub>();
    if (cluster) {
        auto raft_node = std::make_unique<RaftNode>(watch_hub_.get());
        if (!raft_node->AddServiceToServer(&server_, opts_.port)) {
            if (err) {
                *err = "fail to add raft service";
            }
            return false;
        }
        node_ = std::move(raft_node);
    } else {
        node_ = std::make_unique<LocalNode>(std::move(store), watch_hub_.get());
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
    watch_svc_ = std::make_unique<WatchServiceImpl>(node_.get());
    if (server_.AddService(watch_svc_.get(), brpc::SERVER_DOESNT_OWN_SERVICE,
                           kWatchServiceRestMappings) != 0) {
        if (err) {
            *err = "fail to add WatchService";
        }
        return false;
    }
    admin_svc_ = std::make_unique<AdminServiceImpl>(node_.get());
    if (server_.AddService(admin_svc_.get(), brpc::SERVER_DOESNT_OWN_SERVICE,
                           kAdminServiceRestMappings) != 0) {
        if (err) {
            *err = "fail to add AdminService";
        }
        return false;
    }
    // Web 管理界面：托管 web/ 目录静态文件（/dashboard）。需在 Start 前注册。
    dash_svc_ = std::make_unique<DashboardServiceImpl>(opts_.web_dir);
    if (server_.AddService(dash_svc_.get(), brpc::SERVER_DOESNT_OWN_SERVICE,
                           kDashboardRestMappings) != 0) {
        if (err) {
            *err = "fail to add DashboardService";
        }
        return false;
    }

    brpc::ServerOptions brpc_opts;
    brpc_opts.num_threads = 4;
    // 关闭 brpc 内置服务（/status /vars /flags /connections /rpcz 等）：共享端口
    // 面向内网/Dashboard，内置面板会泄露内部指标与运行参数（审查发现 M7）。
    brpc_opts.has_builtin_services = false;
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
    {
        std::lock_guard<std::mutex> lk(compaction_mu_);
        stop_compaction_ = true;
    }
    compaction_cv_.notify_all();
    if (compaction_thread_.joinable()) {
        compaction_thread_.join();
    }
}

void ConfigraftServer::RunUntilAskedToQuit() { server_.RunUntilAskedToQuit(); }

void ConfigraftServer::StartCompactionLoop() {
    compaction_thread_ = std::thread([this] {
        std::unique_lock<std::mutex> lk(compaction_mu_);
        while (!stop_compaction_.load()) {
            // wait_for 而非 sleep_for：析构置 stop 并 notify 后立即唤醒 join，
            // 服务关停不等满 kCompactionIntervalSeconds（优雅关停）。
            compaction_cv_.wait_for(
                lk, std::chrono::seconds(kCompactionIntervalSeconds));
            if (stop_compaction_.load()) {
                break;
            }
            const int removed = node_->Compaction(kKeepVersions);
            if (removed > 0) {
                LOG(INFO) << "compaction removed " << removed << " stale versions";
            }
        }
    });
}

}  // namespace configraft
