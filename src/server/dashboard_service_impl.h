#pragma once

#include <string>

#include "configraft.pb.h"

namespace configraft {

using namespace v1;  // DashboardService/DashboardHttpRequest 定义在 configraft.v1 包

// DashboardService 实现：托管 web/ 目录下的静态文件（Web 管理界面）。
// 仅经 RESTful 映射 "/dashboard/* => Rest" 提供 HTTP 资源，无 gRPC 业务语义。
class DashboardServiceImpl : public DashboardService {
public:
    explicit DashboardServiceImpl(std::string web_dir) : web_dir_(std::move(web_dir)) {}

    void Rest(google::protobuf::RpcController* cntl_base,
              const DashboardHttpRequest* request, DashboardHttpRequest* response,
              google::protobuf::Closure* done) override;

private:
    std::string web_dir_;  // 静态资源根目录（相对启动 cwd）
};

}  // namespace configraft
