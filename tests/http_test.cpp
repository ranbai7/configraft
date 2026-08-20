// HTTP/RESTful 集成测试（M8 安全审查补充）：拉起单机 ConfigraftServer，经 brpc HTTP
// 客户端打真实 /v1/* 路由，验证路由分发、CORS、参数校验、错误码、内置服务关闭
// 与 gRPC 路径共存。此前路由/接线层（restful 映射、json2pb body、CORS）零测试，
// 审查抓到的 CORS 通配、key 编码等 bug 恰发生在这层。
#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <memory>
#include <string>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/http_method.h"
#include "json2pb/json_to_pb.h"
#include "server/server.h"

using namespace configraft;
using namespace configraft::v1;

namespace {

const int kPort = 8123;

class HttpFixture : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all(kDataDir);
        std::string err;
        ServerOptions opts;
        opts.port = kPort;
        opts.data_dir = kDataDir;
        // node_name 为空 → 单机 LocalNode 模式（gRPC + HTTP 双协议同端口）
        server_ = std::make_unique<ConfigraftServer>();
        ASSERT_TRUE(server_->Init(opts, &err)) << err;

        brpc::ChannelOptions co;
        co.protocol = brpc::PROTOCOL_HTTP;
        co.timeout_ms = 5000;
        ASSERT_EQ(0, channel_.Init("127.0.0.1:8123", &co));
    }

    void TearDown() override {
        server_.reset();  // 析构：停 brpc + Compaction 线程（cv 通知立即 join）
        std::filesystem::remove_all(kDataDir);
    }

    // 发 HTTP 请求。返回 HTTP 状态码；body 与指定响应头可选输出。
    int Do(const std::string& method, const std::string& path,
           const std::string& body = "", std::string* out = nullptr,
           std::map<std::string, std::string>* headers = nullptr,
           const std::string& origin = "") {
        brpc::Controller cntl;
        cntl.http_request().uri() = path;
        if (method == "GET") {
            cntl.http_request().set_method(brpc::HTTP_METHOD_GET);
        } else if (method == "POST") {
            cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
            cntl.http_request().SetHeader("Content-Type", "application/json");
        } else if (method == "DELETE") {
            cntl.http_request().set_method(brpc::HTTP_METHOD_DELETE);
        } else if (method == "OPTIONS") {
            cntl.http_request().set_method(brpc::HTTP_METHOD_OPTIONS);
        }
        if (!body.empty()) {
            cntl.request_attachment().append(body);
        }
        if (!origin.empty()) {
            cntl.http_request().SetHeader("Origin", origin);
        }
        // HTTP 匿名调用：CallMethod 为 5 参签名（method/request/response 均空）
        channel_.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
        if (out) {
            *out = cntl.response_attachment().to_string();
        }
        if (headers) {
            const std::string* acao =
                cntl.http_response().GetHeader("Access-Control-Allow-Origin");
            if (acao) {
                (*headers)["access-control-allow-origin"] = *acao;
            }
        }
        return cntl.http_response().status_code();
    }

    // 解析 KVResponse/ConfigResponse 的 code 字段
    static int ParseCode(const std::string& json) {
        KVResponse resp;
        json2pb::JsonToProtoMessage(json, &resp, json2pb::Json2PbOptions(), nullptr);
        return resp.code();
    }

    static constexpr const char* kDataDir = "/tmp/cfg_http_test";

    std::unique_ptr<ConfigraftServer> server_;
    brpc::Channel channel_;
};

// ---------------- 健康检查 ----------------

TEST_F(HttpFixture, Healthz) {
    std::string body;
    const int sc = Do("GET", "/healthz", "", &body);
    EXPECT_EQ(200, sc);
    EXPECT_TRUE(body.find("\"role\":\"leader\"") != std::string::npos);
}

// ---------------- 基础 KV ----------------

TEST_F(HttpFixture, PutGetDelete) {
    std::string body;
    // 写
    EXPECT_EQ(200, Do("POST", "/v1/kv/foo", "{\"value\":\"v1\"}", &body));
    EXPECT_EQ(Code::OK, ParseCode(body));
    // 读
    EXPECT_EQ(200, Do("GET", "/v1/kv/foo", "", &body));
    EXPECT_TRUE(body.find("\"value\":\"v1\"") != std::string::npos);
    // 不存在
    EXPECT_EQ(200, Do("GET", "/v1/kv/absent", "", &body));
    EXPECT_EQ(Code::KEY_NOT_FOUND, ParseCode(body));
    // 删
    EXPECT_EQ(200, Do("DELETE", "/v1/kv/foo", "", &body));
    EXPECT_EQ(Code::OK, ParseCode(body));
    // tombstone 视为不存在
    EXPECT_EQ(200, Do("GET", "/v1/kv/foo", "", &body));
    EXPECT_EQ(Code::KEY_NOT_FOUND, ParseCode(body));
}

TEST_F(HttpFixture, EmptyKeyRejected) {
    std::string body;
    // POST /v1/kv/（空 key）→ INVALID_ARGUMENT（M5 修复回归）
    EXPECT_EQ(200, Do("POST", "/v1/kv/", "{\"value\":\"x\"}", &body));
    EXPECT_EQ(Code::INVALID_ARGUMENT, ParseCode(body));
}

TEST_F(HttpFixture, BatchPut) {
    std::string body;
    EXPECT_EQ(200, Do("POST", "/v1/kv/batch",
                      "{\"entries\":[{\"key\":\"a\",\"value\":\"1\"},"
                      "{\"key\":\"b\",\"value\":\"2\"}]}",
                      &body));
    EXPECT_EQ(Code::OK, ParseCode(body));
    EXPECT_TRUE(body.find("\"key\":\"a\"") != std::string::npos);
    EXPECT_TRUE(body.find("\"key\":\"b\"") != std::string::npos);
}

TEST_F(HttpFixture, CompareAndSwap) {
    std::string body;
    Do("POST", "/v1/kv/foo", "{\"value\":\"old\"}", &body);
    // expect 匹配 → OK，值更新
    EXPECT_EQ(200, Do("POST", "/v1/kv/foo/cas",
                      "{\"expect\":\"old\",\"value\":\"new\"}", &body));
    EXPECT_EQ(Code::OK, ParseCode(body));
    EXPECT_TRUE(body.find("\"value\":\"new\"") != std::string::npos);
    // expect 不匹配 → CAS_FAILED
    EXPECT_EQ(200, Do("POST", "/v1/kv/foo/cas",
                      "{\"expect\":\"wrong\",\"value\":\"x\"}", &body));
    EXPECT_EQ(Code::CAS_FAILED, ParseCode(body));
}

// ---------------- 配置管理 ----------------

TEST_F(HttpFixture, ConfigPublishGetRollback) {
    std::string body;
    Do("POST", "/v1/config/app", "{\"value\":\"v1\"}", &body);
    Do("POST", "/v1/config/app", "{\"value\":\"v2\"}", &body);
    Do("POST", "/v1/config/app", "{\"value\":\"v3\"}", &body);

    // 最新版本
    EXPECT_EQ(200, Do("GET", "/v1/config/app?version=0", "", &body));
    EXPECT_TRUE(body.find("\"value\":\"v3\"") != std::string::npos);
    // 指定版本
    EXPECT_EQ(200, Do("GET", "/v1/config/app?version=1", "", &body));
    EXPECT_TRUE(body.find("\"value\":\"v1\"") != std::string::npos);
    // 版本不存在
    EXPECT_EQ(200, Do("GET", "/v1/config/app?version=99", "", &body));
    EXPECT_EQ(Code::VERSION_NOT_FOUND, ParseCode(body));

    // 回滚到 v1：写回旧值，产生新版本
    EXPECT_EQ(200, Do("POST", "/v1/config/app/rollback", "{\"target_version\":1}",
                      &body));
    EXPECT_EQ(Code::OK, ParseCode(body));
    Do("GET", "/v1/config/app?version=0", "", &body);
    EXPECT_TRUE(body.find("\"value\":\"v1\"") != std::string::npos);
}

// 层级 key 历史不串（M3 前缀碰撞修复回归：cfg/app/ 前缀不应匹配 cfg/app/db/）
TEST_F(HttpFixture, HierarchicalKeyHistoryIsolated) {
    std::string body;
    Do("POST", "/v1/config/app", "{\"value\":\"v1\"}", &body);
    Do("POST", "/v1/config/app/db", "{\"value\":\"v2\"}", &body);
    Do("GET", "/v1/config/app?version=0", "", &body);
    EXPECT_TRUE(body.find("\"key\":\"app\"") != std::string::npos);
    EXPECT_TRUE(body.find("\"key\":\"app/db\"") == std::string::npos);
}

// ---------------- Watch ----------------

TEST_F(HttpFixture, WatchReturnsRevision) {
    std::string body;
    Do("POST", "/v1/kv/wk", "{\"value\":\"v\"}", &body);
    // from_revision=0 不重放历史：立即返回当前 revision
    EXPECT_EQ(200, Do("GET", "/v1/watch?from_revision=0&timeout_ms=500", "", &body));
    EXPECT_TRUE(body.find("current_revision") != std::string::npos);
    // 单 key 形式
    EXPECT_EQ(200, Do("GET", "/v1/watch/wk?from_revision=0&timeout_ms=500", "",
                      &body));
    EXPECT_TRUE(body.find("current_revision") != std::string::npos);
}

// ---------------- CORS（H3 收紧回归） ----------------

TEST_F(HttpFixture, CorsSameHostAllowed) {
    std::map<std::string, std::string> headers;
    // Origin 与目标同主机（127.0.0.1:其他端口）→ 回显该 Origin
    EXPECT_EQ(200, Do("OPTIONS", "/v1/kv/foo", "", nullptr, &headers,
                      "http://127.0.0.1:9999"));
    EXPECT_EQ("http://127.0.0.1:9999", headers["access-control-allow-origin"]);
}

TEST_F(HttpFixture, CorsCrossSiteBlocked) {
    std::map<std::string, std::string> headers;
    // 第三方站点 Origin（host 不同）→ 响应不含 ACAO，浏览器跨站读取被阻断
    EXPECT_EQ(200, Do("OPTIONS", "/v1/kv/foo", "", nullptr, &headers,
                      "http://evil.example.com"));
    EXPECT_TRUE(headers.find("access-control-allow-origin") == headers.end());
}

// ---------------- 运维与加固 ----------------

TEST_F(HttpFixture, BuiltinServicesDisabled) {
    std::string body;
    // has_builtin_services=false → /status /vars 404（M7 修复回归）
    EXPECT_EQ(404, Do("GET", "/status", "", &body));
    EXPECT_EQ(404, Do("GET", "/vars", "", &body));
}

TEST_F(HttpFixture, UnsupportedHttpMethod) {
    std::string body;
    // PUT 方法未支持 → INTERNAL + 提示
    brpc::Controller cntl;
    cntl.http_request().uri() = "/v1/kv/foo";
    cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
    channel_.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    body = cntl.response_attachment().to_string();
    EXPECT_EQ(Code::INTERNAL, ParseCode(body));
}

// ---------------- gRPC 路径与 HTTP 共存 ----------------

TEST_F(HttpFixture, GrpcPutGet) {
    brpc::Channel grpc_ch;
    brpc::ChannelOptions co;
    co.protocol = brpc::PROTOCOL_H2;  // gRPC = HTTP/2（brpc 1.17 协议枚举 PROTOCOL_H2）
    co.timeout_ms = 5000;
    ASSERT_EQ(0, grpc_ch.Init("127.0.0.1:8123", &co));
    KVService_Stub stub(&grpc_ch);

    PutRequest put_req;
    put_req.set_key("grpc-key");
    put_req.set_value("gv");
    KVResponse put_resp;
    brpc::Controller put_cntl;
    stub.Put(&put_cntl, &put_req, &put_resp, nullptr);
    EXPECT_EQ(Code::OK, put_resp.code());

    GetRequest get_req;
    get_req.set_key("grpc-key");
    KVResponse get_resp;
    brpc::Controller get_cntl;
    stub.Get(&get_cntl, &get_req, &get_resp, nullptr);
    EXPECT_EQ(Code::OK, get_resp.code());
    EXPECT_EQ("gv", get_resp.kv().value());
}

}  // namespace
