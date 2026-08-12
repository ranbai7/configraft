#pragma once

#include <functional>

#include "configraft.pb.h"
#include "raft/node.h"

namespace configraft {

using namespace v1;  // 协议类型（WatchService/WatchRequest 等）定义在 configraft.v1 包

// WatchService 实现：基于 HTTP 长轮询的实时变更推送。
//   - Watch → gRPC
//   - Rest  → RESTful HTTP（/v1/watch/*）
// 核心逻辑委托给 ConfigNode::Watch：先经 WatchHub 重放 (from_revision, now] 历史，
// 再注册为 Waiter 阻塞等待 on_apply 广播的新事件 / 超时 / 连接取消。
// 长轮询期间 RPC handler 的 bthread 被挂起（不占 pthread worker）。
class WatchServiceImpl : public WatchService {
public:
    explicit WatchServiceImpl(ConfigNode* node) : node_(node) {}

    void Watch(google::protobuf::RpcController* cntl_base, const WatchRequest* request,
               WatchResponse* response, google::protobuf::Closure* done) override;
    void Rest(google::protobuf::RpcController* cntl_base, const WatchHttpRequest* request,
              WatchResponse* response, google::protobuf::Closure* done) override;

private:
    void DoWatch(const std::string& key, int64_t from_revision, int64_t timeout_ms,
                 int64_t server_deadline_us, const std::function<bool()>& canceled,
                 WatchResponse* resp);

    ConfigNode* node_;
};

}  // namespace configraft
