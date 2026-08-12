#include "server/watch_service_impl.h"

#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/http_method.h"
#include "server/rest_util.h"

namespace configraft {

// ---------------- 核心逻辑（gRPC 与 REST 共用） ----------------

void WatchServiceImpl::DoWatch(const std::string& key, int64_t from_revision,
                               int64_t timeout_ms, int64_t server_deadline_us,
                               const std::function<bool()>& canceled,
                               WatchResponse* resp) {
    WatchResult result;
    node_->Watch(key, from_revision, timeout_ms, server_deadline_us, &canceled,
                 &result);
    resp->set_code(result.code);
    resp->set_message(result.message);
    resp->set_current_revision(result.current_revision);
    for (const auto& e : result.events) {
        resp->add_events()->CopyFrom(e);
    }
}

// ---------------- gRPC 方法 ----------------

void WatchServiceImpl::Watch(google::protobuf::RpcController* cntl_base,
                             const WatchRequest* request, WatchResponse* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = rest::Ctl(cntl_base);
    std::function<bool()> canceled = [cntl] { return cntl->IsCanceled(); };
    DoWatch(request->key(), request->from_revision(), request->timeout_ms(),
            cntl->deadline_us(), canceled, response);
}

// ---------------- RESTful HTTP 统一入口 ----------------
// 路由规则（brpc restful 映射 "/v1/watch/* => Rest"，* 在 unresolved_path）：
//   GET /v1/watch/{key}?from_revision=N&timeout_ms=MS → Watch
//     {key} 省略（/v1/watch）表示监听全部 key；
//     from_revision 缺省 0（只看新事件）；timeout_ms 缺省 30000。
void WatchServiceImpl::Rest(google::protobuf::RpcController* cntl_base,
                            const WatchHttpRequest* request, WatchResponse* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = rest::Ctl(cntl_base);
    if (cntl->http_request().method() != brpc::HTTP_METHOD_GET) {
        response->set_code(Code::INTERNAL);
        response->set_message("unsupported http method, use GET");
        return;
    }
    const std::string& key = cntl->http_request().unresolved_path();
    const int64_t from_revision = rest::QueryInt64(cntl_base, "from_revision", 0);
    const int64_t timeout_ms = rest::QueryInt64(cntl_base, "timeout_ms", 30000);
    std::function<bool()> canceled = [cntl] { return cntl->IsCanceled(); };
    DoWatch(key, from_revision, timeout_ms, cntl->deadline_us(), canceled, response);
}

}  // namespace configraft
