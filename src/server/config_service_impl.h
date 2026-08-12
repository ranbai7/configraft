#pragma once

#include "configraft.pb.h"
#include "raft/node.h"

namespace configraft {

using namespace v1;  // 协议类型（ConfigService/PublishRequest 等）定义在 configraft.v1 包

// ConfigService 实现：配置多版本发布 / 历史查询 / 一键回滚。
//   - Publish/GetConfig/Rollback → gRPC
//   - Rest                        → RESTful HTTP（/v1/config/*）
class ConfigServiceImpl : public ConfigService {
public:
    explicit ConfigServiceImpl(ConfigNode* node) : node_(node) {}

    void Publish(google::protobuf::RpcController* cntl_base, const PublishRequest* request,
                 ConfigResponse* response, google::protobuf::Closure* done) override;
    void GetConfig(google::protobuf::RpcController* cntl_base,
                   const GetConfigRequest* request, ConfigResponse* response,
                   google::protobuf::Closure* done) override;
    void Rollback(google::protobuf::RpcController* cntl_base,
                  const RollbackRequest* request, ConfigResponse* response,
                  google::protobuf::Closure* done) override;
    void Rest(google::protobuf::RpcController* cntl_base, const ConfigHttpRequest* request,
              ConfigResponse* response, google::protobuf::Closure* done) override;

private:
    void DoPublish(const std::string& key, const std::string& value, ConfigResponse* resp);
    void DoGetConfig(const std::string& key, int64_t version, ConfigResponse* resp);
    void DoRollback(const std::string& key, int64_t target_version, ConfigResponse* resp);

    ConfigNode* node_;
};

}  // namespace configraft
