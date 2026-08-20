#include "server/kv_service_impl.h"

#include "brpc/controller.h"
#include "brpc/http_method.h"
#include "json2pb/json_to_pb.h"
#include "server/rest_util.h"

namespace configraft {

// ---------------- 核心逻辑（gRPC 与 REST 共用） ----------------

void KVServiceImpl::DoPut(const std::string& key, const std::string& value,
                          KVResponse* resp) {
    if (!rest::IsValidKey(key)) {
        resp->set_code(Code::INVALID_ARGUMENT);
        resp->set_message("empty key");
        return;
    }
    RaftCmd cmd;
    cmd.mutable_put()->set_key(key);
    cmd.mutable_put()->set_value(value);
    ApplyResult result;
    node_->Apply(cmd, &result);
    rest::FillResponse(result, resp);
}

void KVServiceImpl::DoGet(const std::string& key, bool serializable, KVResponse* resp) {
    GetResult result;
    node_->Get(key, serializable, &result);
    resp->set_code(result.code);
    resp->set_message(result.message);
    if (result.code == Code::NOT_LEADER && !result.leader_id.empty()) {
        resp->set_message("not leader, leader=" + result.leader_id);
    }
    if (result.code == Code::OK) {
        resp->mutable_kv()->CopyFrom(result.kv);
    }
}

void KVServiceImpl::DoDelete(const std::string& key, KVResponse* resp) {
    if (!rest::IsValidKey(key)) {
        resp->set_code(Code::INVALID_ARGUMENT);
        resp->set_message("empty key");
        return;
    }
    RaftCmd cmd;
    cmd.mutable_delete_()->set_key(key);
    ApplyResult result;
    node_->Apply(cmd, &result);
    rest::FillResponse(result, resp);
}

void KVServiceImpl::DoBatchPut(const BatchPutRequest& request, KVResponse* resp) {
    RaftCmd cmd;
    auto* batch = cmd.mutable_batch_put();
    for (const auto& e : request.entries()) {
        if (!rest::IsValidKey(e.key())) {
            resp->set_code(Code::INVALID_ARGUMENT);
            resp->set_message("batch contains empty key");
            return;
        }
        auto* put = batch->add_puts();
        put->set_key(e.key());
        put->set_value(e.value());
    }
    ApplyResult result;
    node_->Apply(cmd, &result);
    rest::FillResponse(result, resp);
}

void KVServiceImpl::DoCAS(const std::string& key, const std::string& expect,
                          const std::string& value, KVResponse* resp) {
    if (!rest::IsValidKey(key)) {
        resp->set_code(Code::INVALID_ARGUMENT);
        resp->set_message("empty key");
        return;
    }
    RaftCmd cmd;
    auto* cas = cmd.mutable_cas();
    cas->set_key(key);
    cas->set_expect(expect);
    cas->set_value(value);
    ApplyResult result;
    node_->Apply(cmd, &result);
    rest::FillResponse(result, resp);
}

// ---------------- gRPC 方法 ----------------

void KVServiceImpl::Put(google::protobuf::RpcController* cntl_base,
                        const PutRequest* request, KVResponse* response,
                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string key = rest::ResolveKey(cntl_base, request->key());
    DoPut(key, request->value(), response);
}

void KVServiceImpl::Get(google::protobuf::RpcController* cntl_base,
                        const GetRequest* request, KVResponse* response,
                        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string key = rest::ResolveKey(cntl_base, request->key());
    DoGet(key, request->serializable(), response);
}

void KVServiceImpl::Delete(google::protobuf::RpcController* cntl_base,
                           const DeleteRequest* request, KVResponse* response,
                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string key = rest::ResolveKey(cntl_base, request->key());
    DoDelete(key, response);
}

void KVServiceImpl::BatchPut(google::protobuf::RpcController* cntl_base,
                             const BatchPutRequest* request, KVResponse* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    DoBatchPut(*request, response);
}

void KVServiceImpl::CompareAndSwap(google::protobuf::RpcController* cntl_base,
                                   const CompareAndSwapRequest* request,
                                   KVResponse* response,
                                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string key = rest::ResolveKey(cntl_base, request->key());
    DoCAS(key, request->expect(), request->value(), response);
}

// ---------------- RESTful HTTP 统一入口 ----------------
// 路由规则（brpc restful 映射 "/v1/kv/* => Rest"，* 在 unresolved_path）：
//   GET    /v1/kv/{key}          → Get
//   DELETE /v1/kv/{key}          → Delete
//   POST   /v1/kv/{key}          → Put（value 在 JSON body）
//   POST   /v1/kv/{key}/cas      → CAS（expect/value 在 JSON body）
//   POST   /v1/kv/batch          → BatchPut（entries 在 JSON body）
void KVServiceImpl::Rest(google::protobuf::RpcController* cntl_base,
                         const KVHttpRequest* request, KVResponse* response,
                         google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = rest::Ctl(cntl_base);
    if (rest::HandleCorsPreflight(cntl)) {
        return;  // OPTIONS 预检已处理
    }
    const std::string& path = cntl->http_request().unresolved_path();
    const brpc::HttpMethod method = cntl->http_request().method();

    if (method == brpc::HTTP_METHOD_GET) {
        const std::string* s =
            cntl->http_request().uri().GetQuery("serializable");
        const bool serializable = (s != nullptr && *s == "true");
        DoGet(path, serializable, response);
        return;
    }
    if (method == brpc::HTTP_METHOD_DELETE) {
        DoDelete(path, response);
        return;
    }
    if (method != brpc::HTTP_METHOD_POST) {
        response->set_code(Code::INTERNAL);
        response->set_message("unsupported http method");
        return;
    }

    // POST：解析 body（JSON → protobuf）并分发
    std::string body;
    cntl->request_attachment().copy_to(&body);
    const bool is_cas =
        path.size() >= 4 && path.compare(path.size() - 4, 4, "/cas") == 0;

    if (is_cas) {
        const std::string key = path.substr(0, path.size() - 4);
        configraft::v1::CompareAndSwapRequest req;
        std::string jerr;
        json2pb::JsonToProtoMessage(body, &req, json2pb::Json2PbOptions(), &jerr);
        DoCAS(key, req.expect(), req.value(), response);
    } else if (path == "batch") {
        configraft::v1::BatchPutRequest req;
        std::string jerr;
        json2pb::JsonToProtoMessage(body, &req, json2pb::Json2PbOptions(), &jerr);
        DoBatchPut(req, response);
    } else {
        configraft::v1::PutRequest req;
        std::string jerr;
        json2pb::JsonToProtoMessage(body, &req, json2pb::Json2PbOptions(), &jerr);
        DoPut(path, req.value(), response);
    }
}

}  // namespace configraft
