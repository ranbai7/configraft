// M5 CAS 原子性：走真实 Raft 路径（提交 → on_apply → 串行 apply）验证"并发 CAS 恰一个成功"。
//
// 用单节点 braft 组（majority=1，自选主）而非 LocalNode：LocalNode::Apply 直接同步调
// ApplyCmdToStore，多线程并发调用会违反 Store 单写者约定（数据竞争）；单节点组仍走完
// "braft 提交复制日志 → on_apply 串行 apply → 比较-写入"全链路，是原子性最直接的证明。
#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "brpc/server.h"
#include "raft/raft_node.h"
#include "store/store.h"

using namespace configraft;
using namespace configraft::v1;

namespace {

// 动态取一个空闲端口（bind 0 后关闭，测试用，存在理论竞态可接受）
int GetFreePort() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    socklen_t len = sizeof(addr);
    if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        close(fd);
        return 0;
    }
    const int port = ntohs(addr.sin_port);
    close(fd);
    return port;
}

class RaftCasTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/cfg_raft_cas_test_XXXXXX";
        char* d = mkdtemp(tmpl);
        ASSERT_NE(d, nullptr);
        dir_ = d;
        port_ = GetFreePort();
        ASSERT_GT(port_, 0);

        std::string err;
        auto store = std::make_unique<Store>();
        ASSERT_TRUE(store->Open(dir_ + "/data", &err)) << err;

        node_ = std::make_unique<RaftNode>(/*hub=*/nullptr);
        ASSERT_TRUE(node_->AddServiceToServer(&server_, port_)) << err;
        const std::string peers = "127.0.0.1:" + std::to_string(port_);
        ASSERT_TRUE(node_->Init(std::move(store), "cas_test", "127.0.0.1", port_,
                                peers, dir_, /*election_timeout_ms=*/500, &err))
            << err;

        brpc::ServerOptions opts;
        opts.num_threads = 4;
        ASSERT_EQ(server_.Start(port_, &opts), 0);

        // 等单节点组选出 leader（自己）
        for (int i = 0; i < 100 && !node_->IsLeader(); ++i) {
            bthread_usleep(50 * 1000);
        }
        ASSERT_TRUE(node_->IsLeader());
    }

    void TearDown() override {
        // 先停 brpc server（等待在途 RPC），再析构 RaftNode（shutdown braft node）
        server_.Stop(0);
        server_.Join();
        node_.reset();
        const std::string cmd = "rm -rf " + dir_;
        std::system(cmd.c_str());
    }

    std::string dir_;
    int port_ = 0;
    brpc::Server server_;
    std::unique_ptr<RaftNode> node_;
};

TEST_F(RaftCasTest, ConcurrentCasExactlyOneSucceeds) {
    // seed：k = old
    {
        RaftCmd seed;
        seed.mutable_put()->set_key("k");
        seed.mutable_put()->set_value("old");
        ApplyResult r;
        node_->Apply(seed, &r);
        ASSERT_EQ(r.code, Code::OK);
    }

    // 16 个线程并发 CAS（同 key、同 expect），经 Raft 复制日志串行 apply
    const int kThreads = 16;
    std::vector<std::thread> threads;
    std::vector<ApplyResult> results(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([this, i, &results] {
            RaftCmd cmd;
            auto* cas = cmd.mutable_cas();
            cas->set_key("k");
            cas->set_expect("old");
            cas->set_value("new" + std::to_string(i));
            node_->Apply(cmd, &results[i]);
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    // 恰一个成功，其余 CAS_FAILED（串行 apply 的原子性）
    int ok = 0;
    for (const auto& r : results) {
        if (r.code == Code::OK) {
            ++ok;
        } else {
            EXPECT_EQ(r.code, Code::CAS_FAILED);
        }
    }
    EXPECT_EQ(ok, 1);

    // 终值 = 胜者值（serializable 本地读，无需 lease）
    GetResult gr;
    node_->Get("k", /*serializable=*/true, &gr);
    ASSERT_EQ(gr.code, Code::OK);
    EXPECT_NE(gr.kv.value(), "old");
}

// CAS 失败不消耗 revision：16 个 CAS 仅 1 成功 → 全局 revision 只推进 2（seed + 胜者）
TEST_F(RaftCasTest, CasFailureDoesNotAdvanceRevision) {
    RaftCmd seed;
    seed.mutable_put()->set_key("k");
    seed.mutable_put()->set_value("old");
    ApplyResult r;
    node_->Apply(seed, &r);
    ASSERT_EQ(r.code, Code::OK);
    const int64_t rev_after_seed = r.kv.revision();

    const int kThreads = 8;
    std::vector<std::thread> threads;
    std::vector<ApplyResult> results(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([this, i, &results] {
            RaftCmd cmd;
            auto* cas = cmd.mutable_cas();
            cas->set_key("k");
            cas->set_expect("old");
            cas->set_value("v" + std::to_string(i));
            node_->Apply(cmd, &results[i]);
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    GetResult gr;
    node_->Get("k", true, &gr);
    ASSERT_EQ(gr.code, Code::OK);
    EXPECT_EQ(gr.kv.revision(), rev_after_seed + 1);  // 仅胜者那次 CAS 消耗了 revision
}

}  // namespace
