#include "server/config_service_impl.h"

#include "brpc/controller.h"
#include "brpc/http_method.h"
#include "json2pb/json_to_pb.h"
#include "server/rest_util.h"

namespace configraft {

// ---------------- 核心逻辑（gRPC 与 REST 共用） ----------------

void ConfigServiceImpl::DoPublish(const std::string& key, const std::string& value,
                                  ConfigResponse* resp) {
    if (!rest::IsValidKey(key)) {
        resp->set_code(Code::INVALID_ARGUMENT);
        resp->set_message("empty key");
        return;
    }
    RaftCmd cmd;
    cmd.mutable_publish()->set_key(key);
    cmd.mutable_publish()->set_value(value);
    ApplyResult result;
    node_->Apply(cmd, &result);
    resp->set_code(result.code);
    resp->set_message(result.message);
    if (result.code == Code::OK) {
        resp->mutable_kv()->CopyFrom(result.kv);
    } else if (result.code == Code::NOT_LEADER && !result.leader_id.empty()) {
        resp->set_message("not leader, leader=" + result.leader_id);
    }
}

void ConfigServiceImpl::DoGetConfig(const std::string& key, int64_t version,
                                    ConfigResponse* resp) {
    ConfigResult result;
    node_->GetConfig(key, version, &result);
    resp->set_code(result.code);
    resp->set_message(result.message);
    if (result.code == Code::OK) {
        resp->mutable_kv()->CopyFrom(result.kv);
        for (const auto& h : result.history) {
            resp->add_history()->CopyFrom(h);
        }
    }
}

void ConfigServiceImpl::DoRollback(const std::string& key, int64_t target_version,
                                   ConfigResponse* resp) {
    if (!rest::IsValidKey(key)) {
        resp->set_code(Code::INVALID_ARGUMENT);
        resp->set_message("empty key");
        return;
    }
    RaftCmd cmd;
    cmd.mutable_rollback()->set_key(key);
    cmd.mutable_rollback()->set_target_version(target_version);
    ApplyResult result;
    node_->Apply(cmd, &result);
    resp->set_code(result.code);
    resp->set_message(result.message);
    if (result.code == Code::OK) {
        resp->mutable_kv()->CopyFrom(result.kv);
    } else if (result.code == Code::NOT_LEADER && !result.leader_id.empty()) {
        resp->set_message("not leader, leader=" + result.leader_id);
    }
}

// ---------------- gRPC 方法 ----------------

void ConfigServiceImpl::Publish(google::protobuf::RpcController* cntl_base,
                                const PublishRequest* request, ConfigResponse* response,
                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string key = rest::ResolveKey(cntl_base, request->key());
    DoPublish(key, request->value(), response);
}

void ConfigServiceImpl::GetConfig(google::protobuf::RpcController* cntl_base,
                                  const GetConfigRequest* request,
                                  ConfigResponse* response,
                                  google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string key = rest::ResolveKey(cntl_base, request->key());
    DoGetConfig(key, request->version(), response);
}

void ConfigServiceImpl::Rollback(google::protobuf::RpcController* cntl_base,
                                 const RollbackRequest* request,
                                 ConfigResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string key = rest::ResolveKey(cntl_base, request->key());
    DoRollback(key, request->target_version(), response);
}

// ---------------- RESTful HTTP 统一入口 ----------------
// 路由规则（brpc restful 映射 "/v1/config/* => Rest"）：
//   GET  /v1/config/{key}?version=N        → GetConfig（N=0 最新）
//   POST /v1/config/{key}                  → Publish（value 在 JSON body）
//   POST /v1/config/{key}/rollback         → Rollback（target_version 在 JSON body）
void ConfigServiceImpl::Rest(google::protobuf::RpcController* cntl_base,
                             const ConfigHttpRequest* request, ConfigResponse* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = rest::Ctl(cntl_base);
    if (rest::HandleCorsPreflight(cntl)) {
        return;  // OPTIONS 预检已处理
    }
    const std::string& path = cntl->http_request().unresolved_path();
    const brpc::HttpMethod method = cntl->http_request().method();

    if (method == brpc::HTTP_METHOD_GET) {
        const int64_t version = rest::QueryInt64(cntl_base, "version", 0);
        DoGetConfig(path, version, response);
        return;
    }
    if (method != brpc::HTTP_METHOD_POST) {
        response->set_code(Code::INTERNAL);
        response->set_message("unsupported http method");
        return;
    }

    std::string body;
    cntl->request_attachment().copy_to(&body);
    const bool is_rollback =
        path.size() >= 9 && path.compare(path.size() - 9, 9, "/rollback") == 0;

    if (is_rollback) {
        const std::string key = path.substr(0, path.size() - 9);
        configraft::v1::RollbackRequest req;
        std::string jerr;
        json2pb::JsonToProtoMessage(body, &req, json2pb::Json2PbOptions(), &jerr);
        DoRollback(key, req.target_version(), response);
    } else {
        configraft::v1::PublishRequest req;
        std::string jerr;
        json2pb::JsonToProtoMessage(body, &req, json2pb::Json2PbOptions(), &jerr);
        DoPublish(path, req.value(), response);
    }
}

}  // namespace configraft
