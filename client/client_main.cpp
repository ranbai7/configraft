// Configraft 示例客户端（gRPC）
//
// 用法：
//   configraft_client --server=127.0.0.1:8000 --method=Put --key=foo --value=bar
//   configraft_client --server=127.0.0.1:8000 --method=Get --key=foo
//   configraft_client --server=127.0.0.1:8000 --method=Delete --key=foo
#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "brpc/channel.h"
#include "common/log.h"
#include "configraft.pb.h"

DEFINE_string(server, "127.0.0.1:8000", "server address");
DEFINE_string(method, "Get",
              "Put / Get / Delete / BatchPut / CAS / GetHealth / AddPeer / RemovePeer");
DEFINE_string(key, "", "key");
DEFINE_string(value, "", "value");
DEFINE_string(expect, "", "expected value for CAS");
DEFINE_string(peer, "", "peer address for AddPeer/RemovePeer");
DEFINE_int32(timeout_ms, 3000, "rpc timeout in ms");

using configraft::v1::KVService_Stub;
using configraft::v1::AdminService_Stub;

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "h2";  // HTTP/2 承载 gRPC（brpc 协议名）
    options.timeout_ms = FLAGS_timeout_ms;
    if (channel.Init(FLAGS_server.c_str(), &options) != 0) {
        LOG(ERROR) << "fail to init channel to " << FLAGS_server;
        return 1;
    }

    KVService_Stub stub(&channel);
    brpc::Controller cntl;

    const std::string method = FLAGS_method;
    if (method == "Put") {
        configraft::v1::PutRequest req;
        req.set_key(FLAGS_key);
        req.set_value(FLAGS_value);
        configraft::v1::KVResponse resp;
        stub.Put(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "Put failed: " << cntl.ErrorText();
            return 1;
        }
        std::cout << "Put ok, code=" << resp.code() << " version=" << resp.kv().version()
                  << " revision=" << resp.kv().revision() << std::endl;
    } else if (method == "Get") {
        configraft::v1::GetRequest req;
        req.set_key(FLAGS_key);
        configraft::v1::KVResponse resp;
        stub.Get(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "Get failed: " << cntl.ErrorText();
            return 1;
        }
        if (resp.code() != configraft::v1::Code::OK) {
            std::cout << "Get: code=" << resp.code() << " message=" << resp.message()
                      << std::endl;
        } else {
            std::cout << "Get: value=" << resp.kv().value()
                      << " version=" << resp.kv().version()
                      << " revision=" << resp.kv().revision() << std::endl;
        }
    } else if (method == "Delete") {
        configraft::v1::DeleteRequest req;
        req.set_key(FLAGS_key);
        configraft::v1::KVResponse resp;
        stub.Delete(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "Delete failed: " << cntl.ErrorText();
            return 1;
        }
        std::cout << "Delete: code=" << resp.code() << std::endl;
    } else if (method == "CAS") {
        configraft::v1::CompareAndSwapRequest req;
        req.set_key(FLAGS_key);
        req.set_expect(FLAGS_expect);
        req.set_value(FLAGS_value);
        configraft::v1::KVResponse resp;
        stub.CompareAndSwap(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "CAS failed: " << cntl.ErrorText();
            return 1;
        }
        std::cout << "CAS: code=" << resp.code() << " message=" << resp.message()
                  << std::endl;
    } else if (method == "GetHealth") {
        AdminService_Stub stub(&channel);
        configraft::v1::GetHealthRequest req;
        configraft::v1::GetHealthResponse resp;
        brpc::Controller cntl2;
        stub.GetHealth(&cntl2, &req, &resp, nullptr);
        if (cntl2.Failed()) {
            LOG(ERROR) << "GetHealth failed: " << cntl2.ErrorText();
            return 1;
        }
        std::cout << "role=" << resp.role() << " term=" << resp.term()
                  << " commit=" << resp.commit_index()
                  << " applied=" << resp.applied_index()
                  << " leader=" << resp.leader_id() << " peers=[";
        for (int i = 0; i < resp.peers_size(); ++i) {
            if (i) {
                std::cout << ", ";
            }
            std::cout << resp.peers(i);
        }
        std::cout << "]" << std::endl;
    } else if (method == "AddPeer" || method == "RemovePeer") {
        if (FLAGS_peer.empty()) {
            LOG(ERROR) << method << " requires --peer=127.0.0.1:8004";
            return 1;
        }
        AdminService_Stub stub(&channel);
        configraft::v1::AdminResponse resp;
        brpc::Controller cntl2;
        if (method == "AddPeer") {
            configraft::v1::AddPeerRequest req;
            req.set_peer(FLAGS_peer);
            stub.AddPeer(&cntl2, &req, &resp, nullptr);
        } else {
            configraft::v1::RemovePeerRequest req;
            req.set_peer(FLAGS_peer);
            stub.RemovePeer(&cntl2, &req, &resp, nullptr);
        }
        if (cntl2.Failed()) {
            LOG(ERROR) << method << " failed: " << cntl2.ErrorText();
            return 1;
        }
        std::cout << method << ": code=" << resp.code() << " message=" << resp.message()
                  << std::endl;
    } else {
        LOG(ERROR) << "unknown method: " << method;
        return 1;
    }
    return 0;
}
