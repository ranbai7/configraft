#pragma once
#include <cstdlib>
#include <string>

#include "brpc/controller.h"
#include "brpc/http_method.h"
#include "configraft.pb.h"
#include "raft/node.h"

namespace configraft {

using namespace v1;  // 协议类型（KVResponse/Code 等）定义在 configraft.v1 包

namespace rest {

// 统一解析请求 key：
//   - RESTful 路径（/v1/kv/{key}）中 * 匹配部分位于 unresolved_path，作为 key；
//   - gRPC / JSON body 中 key 由 request 字段携带。
inline std::string ResolveKey(google::protobuf::RpcController* cntl_base,
                              const std::string& req_key) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    const std::string& path = cntl->http_request().unresolved_path();
    if (!path.empty()) {
        return path;
    }
    return req_key;
}

inline brpc::Controller* Ctl(google::protobuf::RpcController* cntl_base) {
    return static_cast<brpc::Controller*>(cntl_base);
}

// 判断 Origin 是否来自"与请求目标同主机"的页面（Dashboard 页面从
// http://<host>:<port>/dashboard 打开、fetch 到同一 host 的另一节点端口，
// Origin 的 host 与目标 Host 一致）。第三方站点的 Origin host 不同 → 拒绝。
// 用于替代通配 `Access-Control-Allow-Origin: *`：服务无鉴权，通配 CORS 会让任意
// 恶意网页跨站读写配置/成员（审查发现 H3）。
inline bool IsAllowedOrigin(const std::string& origin, const std::string& request_host) {
    if (origin.empty() || request_host.empty()) {
        return false;  // 无 Origin（如 curl 等非浏览器）无需 ACAO；Host 缺失保守拒绝
    }
    // origin 形如 "http://host[:port]" / "https://host[:port]"
    const size_t scheme_end = origin.find("://");
    if (scheme_end == std::string::npos) {
        return false;
    }
    std::string origin_host = origin.substr(scheme_end + 3);
    // 去掉端口（IPv6 形如 [::1]:port，rfind 后主机部分含括号，仍可比对）
    const size_t origin_port = origin_host.rfind(':');
    if (origin_port != std::string::npos) {
        origin_host = origin_host.substr(0, origin_port);
    }
    std::string target_host = request_host;
    const size_t target_port = target_host.rfind(':');
    if (target_port != std::string::npos) {
        target_host = target_host.substr(0, target_port);
    }
    return origin_host == target_host;
}

// CORS：Dashboard 页面从 800X 打开、fetch 到 leader 800Y 属于跨端口请求，
// 必须在每个 HTTP Rest 方法开头调用。返回 true 表示 OPTIONS 预检已处理（可 return）。
// 放行策略：仅当 Origin 与请求目标同主机时回显该 Origin（而非通配 *），
// 既保证 Dashboard 跨端口 fetch 可用，又阻断第三方站点的跨站读写。
inline bool HandleCorsPreflight(brpc::Controller* cntl) {
    // brpc GetHeader 返回 const std::string*（可能为 nullptr）
    const std::string* origin = cntl->http_request().GetHeader("Origin");
    const std::string* host = cntl->http_request().GetHeader("Host");
    if (origin != nullptr && host != nullptr && IsAllowedOrigin(*origin, *host)) {
        cntl->http_response().SetHeader("Access-Control-Allow-Origin", *origin);
        cntl->http_response().SetHeader("Vary", "Origin");
    }
    cntl->http_response().SetHeader("Access-Control-Allow-Methods",
                                    "GET,POST,DELETE,OPTIONS");
    cntl->http_response().SetHeader("Access-Control-Allow-Headers",
                                    "Content-Type");
    if (cntl->http_request().method() == brpc::HTTP_METHOD_OPTIONS) {
        cntl->http_response().set_status_code(200);
        return true;
    }
    return false;
}

// 校验请求 key：拒绝空串（审查发现 M5——空 key 会污染 k/ v/ cfg/ 命名空间并
// 与 REST 保留后缀语义冲突）。key 含 "/"（层级 key）是配置中心的合法用法，不拒绝。
inline bool IsValidKey(const std::string& key) { return !key.empty(); }

// 读取 URL query-string 参数（Watch 的 from_revision 等），缺省返回 dflt。
inline int64_t QueryInt64(google::protobuf::RpcController* cntl_base, const char* name,
                          int64_t dflt) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    const std::string* v = cntl->http_request().uri().GetQuery(name);
    if (v == nullptr || v->empty()) {
        return dflt;
    }
    return std::strtoll(v->c_str(), nullptr, 10);
}

// 把 ApplyResult 填到 KVResponse；非 leader 时给出重定向提示。
inline void FillResponse(const ApplyResult& r, KVResponse* resp) {
    resp->set_code(r.code);
    resp->set_message(r.message);
    if (r.code == Code::OK) {
        if (r.kv.ByteSizeLong() > 0) {
            resp->mutable_kv()->CopyFrom(r.kv);
        }
        for (const auto& kv : r.kvs) {
            resp->add_kvs()->CopyFrom(kv);
        }
    } else if (r.code == Code::NOT_LEADER && !r.leader_id.empty()) {
        resp->set_message("not leader, leader=" + r.leader_id);
    }
}

}  // namespace rest
}  // namespace configraft
