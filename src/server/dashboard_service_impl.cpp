#include "server/dashboard_service_impl.h"

#include <fstream>
#include <string>

#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/http_method.h"
#include "server/rest_util.h"

namespace configraft {

namespace {

// 按扩展名推断 Content-Type。
std::string ContentType(const std::string& path) {
    const size_t dot = path.find_last_of('.');
    const std::string ext =
        dot == std::string::npos ? "" : path.substr(dot + 1);
    if (ext == "html" || ext.empty()) {
        return "text/html; charset=utf-8";
    }
    if (ext == "css") {
        return "text/css; charset=utf-8";
    }
    if (ext == "js") {
        return "application/javascript; charset=utf-8";
    }
    if (ext == "json") {
        return "application/json";
    }
    if (ext == "svg") {
        return "image/svg+xml";
    }
    if (ext == "png") {
        return "image/png";
    }
    if (ext == "ico") {
        return "image/x-icon";
    }
    return "application/octet-stream";
}

// 读入整个文件，成功返回 true。
bool ReadFile(const std::string& path, std::string* out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return false;
    }
    in.seekg(0, std::ios::end);
    std::streamsize size = in.tellg();
    if (size < 0) {
        return false;
    }
    out->resize(static_cast<size_t>(size));
    in.seekg(0, std::ios::beg);
    in.read(&(*out)[0], size);
    return in.good() || in.eof();
}

}  // namespace

// ---------------- RESTful HTTP 静态资源 ----------------
// 路由（brpc restful 映射 "/dashboard => Rest,/dashboard/* => Rest"）：
//   GET /dashboard          → index.html
//   GET /dashboard/{file}   → web/{file}（按扩展名设置 Content-Type）
void DashboardServiceImpl::Rest(google::protobuf::RpcController* cntl_base,
                                const DashboardHttpRequest* request,
                                DashboardHttpRequest* response,
                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = rest::Ctl(cntl_base);
    if (cntl->http_request().method() != brpc::HTTP_METHOD_GET) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
        return;
    }
    std::string path = cntl->http_request().unresolved_path();
    if (path.empty()) {
        path = "index.html";
    }
    // 路径安全：拒绝绝对路径与上层目录穿越。
    if (path[0] == '/' || path.find("..") != std::string::npos) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
        return;
    }
    const std::string file = web_dir_ + "/" + path;
    std::string contents;
    if (!ReadFile(file, &contents)) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
        return;
    }
    cntl->http_response().set_content_type(ContentType(path));
    cntl->response_attachment().append(contents);
}

}  // namespace configraft
