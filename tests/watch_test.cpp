// M4 Watch：Store 历史重放 + WatchHub 长轮询 单元测试
#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "raft/node.h"
#include "store/store.h"
#include "watch/watch_hub.h"

using namespace configraft;
using namespace configraft::v1;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/cfg_watch_test_XXXXXX";
    char* d = mkdtemp(tmpl);
    return d ? std::string(d) : std::string();
}

// ---------------- Store 层：ReplayEvents / CompactRev ----------------

class StoreFixture : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = MakeTempDir();
        ASSERT_FALSE(dir_.empty());
        std::string err;
        ASSERT_TRUE(store_.Open(dir_, &err)) << err;
    }
    void TearDown() override {
        store_.Close();
        std::string cmd = "rm -rf " + dir_;
        std::system(cmd.c_str());
    }

    Store store_;
    std::string dir_;
};

TEST_F(StoreFixture, ReplayAllInOrderWithDelete) {
    KV kv;
    store_.Put("a", "1", &kv);  // rev 1 PUT
    store_.Put("b", "2", &kv);  // rev 2 PUT
    store_.Delete("a", &kv);    // rev 3 DELETE

    int32_t code = -1;
    std::vector<WatchEvent> evs;
    ASSERT_TRUE(
        store_.ReplayEvents(0, store_.CurrentRevision(), "", 100, &evs, &code));
    ASSERT_EQ(evs.size(), 3u);
    EXPECT_EQ(evs[0].revision(), 1);
    EXPECT_EQ(evs[0].key(), "a");
    EXPECT_EQ(evs[0].type(), "PUT");
    EXPECT_EQ(evs[1].key(), "b");
    EXPECT_EQ(evs[2].revision(), 3);
    EXPECT_EQ(evs[2].type(), "DELETE");
    EXPECT_EQ(code, Code::OK);
}

TEST_F(StoreFixture, ReplayKeyFilter) {
    store_.Put("a", "1", nullptr);  // rev 1
    store_.Put("b", "2", nullptr);  // rev 2
    store_.Put("a", "3", nullptr);  // rev 3

    int32_t code = -1;
    std::vector<WatchEvent> evs;
    ASSERT_TRUE(store_.ReplayEvents(0, store_.CurrentRevision(), "a", 100, &evs,
                                    &code));
    ASSERT_EQ(evs.size(), 2u);
    EXPECT_EQ(evs[0].key(), "a");
    EXPECT_EQ(evs[0].revision(), 1);
    EXPECT_EQ(evs[1].revision(), 3);
}

TEST_F(StoreFixture, ReplayOpenInterval) {
    store_.Put("a", "1", nullptr);  // rev 1
    store_.Put("a", "2", nullptr);  // rev 2
    store_.Put("a", "3", nullptr);  // rev 3

    int32_t code = -1;
    std::vector<WatchEvent> evs;
    // 开区间 (1, 3] → rev 2、3（不含 rev 1 自身）
    ASSERT_TRUE(store_.ReplayEvents(1, 3, "", 100, &evs, &code));
    ASSERT_EQ(evs.size(), 2u);
    EXPECT_EQ(evs[0].revision(), 2);
    EXPECT_EQ(evs[1].revision(), 3);
}

TEST_F(StoreFixture, ReplayEmptyInterval) {
    store_.Put("a", "1", nullptr);
    int32_t code = -1;
    std::vector<WatchEvent> evs;
    const int64_t cur = store_.CurrentRevision();
    ASSERT_TRUE(store_.ReplayEvents(cur, cur, "", 100, &evs, &code));
    EXPECT_TRUE(evs.empty());
}

TEST_F(StoreFixture, ReplayMaxEventsCapped) {
    for (int i = 1; i <= 10; ++i) {
        store_.Put("k", std::to_string(i), nullptr);
    }
    int32_t code = -1;
    std::vector<WatchEvent> evs;
    ASSERT_TRUE(store_.ReplayEvents(0, store_.CurrentRevision(), "", 3, &evs,
                                    &code));
    ASSERT_EQ(evs.size(), 3u);  // 超出部分留给客户端续传
    EXPECT_EQ(evs[0].revision(), 1);
    EXPECT_EQ(evs[2].revision(), 3);
}

TEST_F(StoreFixture, CompactionSetsCompactRev) {
    for (int i = 1; i <= 15; ++i) {
        store_.Put("k", std::to_string(i), nullptr);
    }
    const int64_t cur = store_.CurrentRevision();  // 15
    ASSERT_GT(store_.Compaction(5), 0);            // 删最旧 10 版 → compact_rev=10

    EXPECT_EQ(store_.CompactRev(), 10);
    // from < compact_rev → 历史不完整，报 COMPACTED
    int32_t code = -1;
    std::vector<WatchEvent> evs;
    ASSERT_FALSE(store_.ReplayEvents(0, cur, "", 100, &evs, &code));
    EXPECT_EQ(code, Code::COMPACTED);
    // from >= compact_rev → (from, cur] 内无洞，可完整重放
    code = -1;
    ASSERT_TRUE(store_.ReplayEvents(10, cur, "", 100, &evs, &code));
    EXPECT_FALSE(evs.empty());
    EXPECT_EQ(evs[0].revision(), 11);
}

TEST_F(StoreFixture, LoadSnapshotResetsCompactRev) {
    SnapshotData data;
    data.set_revision(7);
    KV kv;
    kv.set_key("a");
    kv.set_value("1");
    kv.set_version(1);
    kv.set_revision(7);
    *data.add_kvs() = kv;

    std::string err;
    ASSERT_TRUE(store_.LoadSnapshot(data, &err));
    EXPECT_EQ(store_.CompactRev(), 7);
    // 快照不保留历史：from < 7 → COMPACTED
    int32_t code = -1;
    std::vector<WatchEvent> evs;
    ASSERT_FALSE(store_.ReplayEvents(0, 7, "", 100, &evs, &code));
    EXPECT_EQ(code, Code::COMPACTED);
}

// ---------------- WatchHub 层：长轮询等待 / 广播 / 背压 / 超时 ----------------

// watcher 运行参数（在 bthread 中调用 hub->Watch）
struct WatchArg {
    WatchHub* hub;
    Store* store;
    std::string key;
    int64_t from;
    int64_t timeout_ms;
    WatchResult* result;
};

void* RunWatch(void* arg) {
    WatchArg* a = static_cast<WatchArg*>(arg);
    a->hub->Watch(a->key, a->from, a->timeout_ms, -1, nullptr, a->store, a->result);
    return nullptr;
}

WatchEvent MakePutEvent(const std::string& key, const std::string& value,
                        int64_t revision) {
    WatchEvent e;
    e.set_key(key);
    e.set_value(value);
    e.set_version(1);
    e.set_revision(revision);
    e.set_type("PUT");
    return e;
}

class WatchHubTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = MakeTempDir();
        ASSERT_FALSE(dir_.empty());
        std::string err;
        ASSERT_TRUE(store_.Open(dir_, &err)) << err;
    }
    void TearDown() override {
        store_.Close();
        std::string cmd = "rm -rf " + dir_;
        std::system(cmd.c_str());
    }

    Store store_;
    WatchHub hub_;
    std::string dir_;
};

TEST_F(WatchHubTest, ReplayHistoricalEvents) {
    store_.Put("k", "1", nullptr);  // rev 1
    store_.Put("k", "2", nullptr);  // rev 2
    store_.Put("k", "3", nullptr);  // rev 3

    WatchResult result;
    WatchArg arg{&hub_, &store_, "k", 1, 1000, &result};
    bthread_t tid;
    ASSERT_EQ(bthread_start_background(&tid, nullptr, RunWatch, &arg), 0);
    bthread_join(tid, nullptr);

    // (1, 3] → rev 2、3，立即返回（无需等广播）
    ASSERT_EQ(result.code, Code::OK);
    ASSERT_EQ(result.events.size(), 2u);
    EXPECT_EQ(result.events[0].revision(), 2);
    EXPECT_EQ(result.events[1].revision(), 3);
    EXPECT_EQ(result.current_revision, 3);  // 锚点 = 最后事件 rev
}

TEST_F(WatchHubTest, BroadcastWakesWaiter) {
    WatchResult result;
    WatchArg arg{&hub_, &store_, "foo", 0, 5000, &result};
    bthread_t tid;
    ASSERT_EQ(bthread_start_background(&tid, nullptr, RunWatch, &arg), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 等注册

    std::vector<WatchEvent> evs = {MakePutEvent("foo", "v1", 10)};
    hub_.Broadcast(evs);
    bthread_join(tid, nullptr);

    ASSERT_EQ(result.code, Code::OK);
    ASSERT_EQ(result.events.size(), 1u);
    EXPECT_EQ(result.events[0].key(), "foo");
    EXPECT_EQ(result.events[0].value(), "v1");
    EXPECT_EQ(result.current_revision, 10);
}

TEST_F(WatchHubTest, BroadcastFiltersByKey) {
    WatchResult result;
    WatchArg arg{&hub_, &store_, "foo", 0, 150, &result};  // 150ms 超时
    bthread_t tid;
    ASSERT_EQ(bthread_start_background(&tid, nullptr, RunWatch, &arg), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // 广播不匹配的 key → 不被接收，最终超时返回空
    hub_.Broadcast({MakePutEvent("bar", "v1", 1)});
    bthread_join(tid, nullptr);

    EXPECT_EQ(result.code, Code::OK);
    EXPECT_TRUE(result.events.empty());
}

TEST_F(WatchHubTest, FromZeroWaitsOnlyNewEvents) {
    store_.Put("foo", "old", nullptr);  // 注册前的历史（rev 1），from=0 不应返回

    WatchResult result;
    WatchArg arg{&hub_, &store_, "foo", 0, 3000, &result};
    bthread_t tid;
    ASSERT_EQ(bthread_start_background(&tid, nullptr, RunWatch, &arg), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 模拟 on_apply 广播一条新事件
    hub_.Broadcast({MakePutEvent("foo", "new", 5)});
    bthread_join(tid, nullptr);

    ASSERT_EQ(result.code, Code::OK);
    ASSERT_EQ(result.events.size(), 1u);
    EXPECT_EQ(result.events[0].revision(), 5);  // 只收到新事件
}

TEST_F(WatchHubTest, OverflowBackpressure) {
    store_.Put("k", "0", nullptr);  // current_revision = 1（作为重连锚点）
    WatchHub small_hub(2);          // 缓冲上限 2
    WatchResult result;
    WatchArg arg{&small_hub, &store_, "k", 0, 2000, &result};
    bthread_t tid;
    ASSERT_EQ(bthread_start_background(&tid, nullptr, RunWatch, &arg), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 广播 3 条匹配事件，超过上限 → 背压断开
    std::vector<WatchEvent> evs;
    for (int i = 1; i <= 3; ++i) {
        evs.push_back(MakePutEvent("k", std::to_string(i), i));
    }
    small_hub.Broadcast(evs);
    bthread_join(tid, nullptr);

    EXPECT_EQ(result.code, Code::INTERNAL);  // overflow
    EXPECT_TRUE(result.events.empty());
    EXPECT_GT(result.current_revision, 0);   // 客户端以该 rev 重连重放
}

TEST_F(WatchHubTest, TimeoutReturnsEmptyOk) {
    WatchResult result;
    WatchArg arg{&hub_, &store_, "k", 0, 150, &result};
    bthread_t tid;
    ASSERT_EQ(bthread_start_background(&tid, nullptr, RunWatch, &arg), 0);
    bthread_join(tid, nullptr);

    EXPECT_EQ(result.code, Code::OK);
    EXPECT_TRUE(result.events.empty());
    EXPECT_GE(result.current_revision, 0);  // 当前 revision，客户端立即续传
}

TEST_F(WatchHubTest, BroadcastToMultipleWaiters) {
    WatchResult r1, r2;
    WatchArg a1{&hub_, &store_, "k", 0, 2000, &r1};
    WatchArg a2{&hub_, &store_, "k", 0, 2000, &r2};
    bthread_t t1, t2;
    ASSERT_EQ(bthread_start_background(&t1, nullptr, RunWatch, &a1), 0);
    ASSERT_EQ(bthread_start_background(&t2, nullptr, RunWatch, &a2), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    hub_.Broadcast({MakePutEvent("k", "v", 1)});
    bthread_join(t1, nullptr);
    bthread_join(t2, nullptr);

    ASSERT_EQ(r1.events.size(), 1u);
    ASSERT_EQ(r2.events.size(), 1u);
    EXPECT_EQ(r1.events[0].revision(), 1);
    EXPECT_EQ(r2.events[0].revision(), 1);
}

TEST_F(WatchHubTest, CompactedFromRevision) {
    for (int i = 1; i <= 15; ++i) {
        store_.Put("k", std::to_string(i), nullptr);
    }
    store_.Compaction(5);  // compact_rev = 10

    WatchResult result;
    WatchArg arg{&hub_, &store_, "k", 1, 1000, &result};  // from=1 < 10
    bthread_t tid;
    ASSERT_EQ(bthread_start_background(&tid, nullptr, RunWatch, &arg), 0);
    bthread_join(tid, nullptr);

    EXPECT_EQ(result.code, Code::COMPACTED);
    EXPECT_TRUE(result.events.empty());
}

}  // namespace
