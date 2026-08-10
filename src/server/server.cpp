#include "server/server.h"

#include <memory>
#include <utility>

#include "common/log.h"
#include "raft/local_node.h"
#include "store/store.h"

namespace configraft {

namespace {

// RESTful 路径映射：PATH => METHOD（* 匹配部分在 cntl->http_request().unresolved_path()）。
// brpc 的 restful 映射不支持 HTTP 方法前缀，同路径不同方法的语义在 Rest 方法内
// 按 method + unresolved_path 分发（GET/POST/DELETE、/cas、/batch）。
// 未出现在映射中的 gRPC 方法仍通过 /configraft.v1.KVService/Put 等访问。
constexpr char kKVServiceRestMappings[] =
    "/v1/kv/* => Rest,";

}  // namespace

bool ConfigraftServer::Init(const ServerOptions& opts, std::string* err) {
    opts_ = opts;

    // 1. 打开存储
    auto store = std::make_unique<Store>();
    std::string store_err;
    if (!store->Open(opts_.data_dir, &store_err)) {
        if (err) {
            *err = "open store " + opts_.data_dir + " failed: " + store_err;
        }
        return false;
    }
    LOG(INFO) << "store opened at " << opts_.data_dir;

    // 2. 创建节点（集群模式 M2 起接入 braft；目前单机同步执行）
    if (!opts_.node_name.empty()) {
        if (err) {
            *err = "cluster mode not implemented yet (M2)";
        }
        return false;
    }
    node_ = std::make_unique<LocalNode>(std::move(store));

    // 3. 创建服务
    kv_svc_ = std::make_unique<KVServiceImpl>(node_.get());

    // 4. 注册到 brpc Server（单端口：gRPC + HTTP + Raft 内部通信 + 监控面板）
    if (server_.AddService(kv_svc_.get(), brpc::SERVER_DOESNT_OWN_SERVICE,
                           kKVServiceRestMappings) != 0) {
        if (err) {
            *err = "fail to add KVService";
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
    LOG(INFO) << "Configraft server listening on 0.0.0.0:" << opts_.port
              << " (gRPC / HTTP / raft / status)";
    return true;
}

void ConfigraftServer::RunUntilAskedToQuit() { server_.RunUntilAskedToQuit(); }

}  // namespace configraft
