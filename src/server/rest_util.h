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

// CORS：Dashboard 页面从 800X 打开、fetch 到 leader 800Y 属于跨端口请求，
// 必须在每个 HTTP Rest 方法开头调用。返回 true 表示 OPTIONS 预检已处理（可 return）。
inline bool HandleCorsPreflight(brpc::Controller* cntl) {
    cntl->http_response().SetHeader("Access-Control-Allow-Origin", "*");
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
