#pragma once

#include "configraft.pb.h"
#include "raft/node.h"

namespace configraft {

using namespace v1;  // 协议类型（KVService/PutRequest 等）定义在 configraft.v1 包

// KVService 实现：
//   - Put/Get/Delete/BatchPut/CAS  → gRPC（/configraft.v1.KVService/Put 等）
//   - Rest                          → RESTful HTTP（/v1/kv/*，内部按 method+path 分发）
class KVServiceImpl : public KVService {
public:
    explicit KVServiceImpl(ConfigNode* node) : node_(node) {}

    void Put(google::protobuf::RpcController* cntl_base, const PutRequest* request,
             KVResponse* response, google::protobuf::Closure* done) override;
    void Get(google::protobuf::RpcController* cntl_base, const GetRequest* request,
             KVResponse* response, google::protobuf::Closure* done) override;
    void Delete(google::protobuf::RpcController* cntl_base, const DeleteRequest* request,
                KVResponse* response, google::protobuf::Closure* done) override;
    void BatchPut(google::protobuf::RpcController* cntl_base, const BatchPutRequest* request,
                  KVResponse* response, google::protobuf::Closure* done) override;
    void CompareAndSwap(google::protobuf::RpcController* cntl_base,
                        const CompareAndSwapRequest* request, KVResponse* response,
                        google::protobuf::Closure* done) override;
    void Rest(google::protobuf::RpcController* cntl_base, const KVHttpRequest* request,
              KVResponse* response, google::protobuf::Closure* done) override;

private:
    // 核心逻辑（gRPC 方法与 Rest 共用）
    void DoPut(const std::string& key, const std::string& value, KVResponse* resp);
    void DoGet(const std::string& key, bool serializable, KVResponse* resp);
    void DoDelete(const std::string& key, KVResponse* resp);
    void DoBatchPut(const BatchPutRequest& request, KVResponse* resp);
    void DoCAS(const std::string& key, const std::string& expect,
               const std::string& value, KVResponse* resp);

    ConfigNode* node_;
};

}  // namespace configraft
