#pragma once

#include "configraft.pb.h"
#include "raft/node.h"

namespace configraft {

using namespace v1;  // 协议类型（AdminService/GetHealthRequest 等）定义在 configraft.v1 包

// AdminService 实现（M6）：
//   - GetHealth  → 健康检查：进程存活 + 集群状态（role/term/commit/applied/leader/peers）
//   - AddPeer / RemovePeer → 在线成员变更（仅 Leader 可执行）
// gRPC 通过 /configraft.v1.AdminService/{Method}，REST 由 brpc restful 映射到具体方法
// （/health、/addpeer、/removepeer，HTTP JSON body 自动解析进 request）。
class AdminServiceImpl : public AdminService {
public:
    explicit AdminServiceImpl(ConfigNode* node) : node_(node) {}

    void GetHealth(google::protobuf::RpcController* cntl_base,
                   const GetHealthRequest* request, GetHealthResponse* response,
                   google::protobuf::Closure* done) override;
    void AddPeer(google::protobuf::RpcController* cntl_base,
                 const AddPeerRequest* request, AdminResponse* response,
                 google::protobuf::Closure* done) override;
    void RemovePeer(google::protobuf::RpcController* cntl_base,
                    const RemovePeerRequest* request, AdminResponse* response,
                    google::protobuf::Closure* done) override;

private:
    ConfigNode* node_;
};

}  // namespace configraft
