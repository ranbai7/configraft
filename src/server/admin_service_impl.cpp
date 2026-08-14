#include "server/admin_service_impl.h"

#include "brpc/closure_guard.h"
#include "common/log.h"

namespace configraft {

void AdminServiceImpl::GetHealth(google::protobuf::RpcController* cntl_base,
                                 const GetHealthRequest* request,
                                 GetHealthResponse* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // 进程 + 服务存活即可返回 200（HTTP 层由 brpc 保证）；响应携带集群状态，
    // 调用方据此判断是否就绪（role=leader 且 applied≥commit 时完全就绪）。
    response->set_code(Code::OK);
    response->set_message("ok");
    response->set_role(node_->Role());
    response->set_term(node_->Term());
    response->set_commit_index(node_->CommitIndex());
    response->set_applied_index(node_->AppliedIndex());
    response->set_leader_id(node_->LeaderId());
    for (const auto& p : node_->Peers()) {
        response->add_peers(p);
    }
}

void AdminServiceImpl::AddPeer(google::protobuf::RpcController* cntl_base,
                               const AddPeerRequest* request,
                               AdminResponse* response,
                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    ConfChangeResult result;
    node_->AddPeer(request->peer(), &result);
    response->set_code(result.code);
    response->set_message(result.message);
}

void AdminServiceImpl::RemovePeer(google::protobuf::RpcController* cntl_base,
                                  const RemovePeerRequest* request,
                                  AdminResponse* response,
                                  google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    ConfChangeResult result;
    node_->RemovePeer(request->peer(), &result);
    response->set_code(result.code);
    response->set_message(result.message);
}

}  // namespace configraft
