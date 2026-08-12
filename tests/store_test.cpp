#include <cstdlib>
#include <cstring>
#include <string>

#include <gtest/gtest.h>

#include "store/store.h"

using namespace configraft;

namespace {

class StoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/cfg_store_test_XXXXXX";
        char* d = mkdtemp(tmpl);
        ASSERT_NE(d, nullptr);
        dir_ = d;
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

}  // namespace

TEST_F(StoreTest, PutAndGet) {
    KV kv;
    const int64_t rev = store_.Put("a", "1", &kv);
    ASSERT_GT(rev, 0);
    EXPECT_EQ(kv.key(), "a");
    EXPECT_EQ(kv.value(), "1");
    EXPECT_EQ(kv.version(), 1);
    EXPECT_EQ(kv.revision(), rev);

    KV got;
    int32_t code = -1;
    ASSERT_TRUE(store_.Get("a", &got, &code));
    EXPECT_EQ(got.value(), "1");
    EXPECT_EQ(code, Code::OK);
}

TEST_F(StoreTest, PutOverwriteIncrementsVersion) {
    KV kv;
    store_.Put("a", "1", &kv);
    store_.Put("a", "2", &kv);
    EXPECT_EQ(kv.version(), 2);
    EXPECT_EQ(kv.value(), "2");
}

TEST_F(StoreTest, DeleteMakesKeyNotFound) {
    store_.Put("a", "1", nullptr);
    store_.Delete("a", nullptr);
    KV got;
    int32_t code = -1;
    ASSERT_FALSE(store_.Get("a", &got, &code));
    EXPECT_EQ(code, Code::KEY_NOT_FOUND);
}

TEST_F(StoreTest, RevisionMonotonicAcrossKeys) {
    KV kv;
    store_.Put("a", "1", &kv);
    const int64_t r1 = kv.revision();
    store_.Put("b", "2", &kv);
    EXPECT_GT(kv.revision(), r1);
}

TEST_F(StoreTest, PersistenceAcrossReopen) {
    store_.Put("persist", "hello", nullptr);
    store_.Close();

    Store reopened;
    std::string err;
    ASSERT_TRUE(reopened.Open(dir_, &err)) << err;
    KV got;
    int32_t code = -1;
    ASSERT_TRUE(reopened.Get("persist", &got, &code));
    EXPECT_EQ(got.value(), "hello");
    reopened.Close();
}

TEST_F(StoreTest, BatchPut) {
    std::vector<std::pair<std::string, std::string>> kvs = {{"a", "1"}, {"b", "2"}, {"c", "3"}};
    std::vector<KV> out;
    ASSERT_GE(store_.BatchPut(kvs, &out), 0);
    EXPECT_EQ(out.size(), 3u);

    KV got;
    int32_t code = -1;
    ASSERT_TRUE(store_.Get("c", &got, &code));
    EXPECT_EQ(got.value(), "3");
    EXPECT_EQ(got.version(), 1);
}

// ---------------- M3：MVCC 版本模型 ----------------

TEST_F(StoreTest, PublishAndGetConfigVersion) {
    KV kv;
    store_.Publish("cfg", "1000", &kv);
    store_.Publish("cfg", "2000", &kv);
    store_.Publish("cfg", "3000", &kv);
    EXPECT_EQ(kv.version(), 3);

    KV v1;
    int32_t code = 0;
    ASSERT_TRUE(store_.GetConfig("cfg", 1, &v1, &code));
    EXPECT_EQ(v1.value(), "1000");
    EXPECT_EQ(code, Code::OK);

    // 历史应含 3 个版本
    std::vector<KV> history;
    store_.GetHistory("cfg", &history);
    EXPECT_EQ(history.size(), 3u);

    // 不存在的版本
    code = 0;
    ASSERT_FALSE(store_.GetConfig("cfg", 99, &v1, &code));
    EXPECT_EQ(code, Code::VERSION_NOT_FOUND);
}

TEST_F(StoreTest, RollbackCreatesNewVersion) {
    store_.Publish("cfg", "1000", nullptr);
    store_.Publish("cfg", "2000", nullptr);

    KV kv;
    bool ok = false;
    const int64_t rev = store_.Rollback("cfg", 1, &kv, &ok);
    ASSERT_TRUE(ok);
    EXPECT_GT(rev, 0);
    EXPECT_EQ(kv.value(), "1000");  // 回滚到 v1 的值
    EXPECT_EQ(kv.version(), 3);     // 回滚产生新版本 v3（不删历史）

    std::vector<KV> history;
    store_.GetHistory("cfg", &history);
    EXPECT_EQ(history.size(), 3u);

    // 回滚到不存在的版本
    ok = true;
    store_.Rollback("cfg", 99, &kv, &ok);
    EXPECT_FALSE(ok);
}

TEST_F(StoreTest, CompactionKeepsLatestVersions) {
    for (int i = 1; i <= 15; ++i) {
        store_.Publish("cfg", std::to_string(i), nullptr);
    }

    // 当前值 v15
    KV cur;
    int32_t code = 0;
    ASSERT_TRUE(store_.GetConfig("cfg", 0, &cur, &code));
    EXPECT_EQ(cur.value(), "15");

    // Compaction 保留最近 5 个版本
    const int removed = store_.Compaction(5);
    EXPECT_GT(removed, 0);

    // 最新值不受影响
    ASSERT_TRUE(store_.GetConfig("cfg", 0, &cur, &code));
    EXPECT_EQ(cur.value(), "15");

    // 老版本 v1 已被回收
    KV old;
    code = 0;
    ASSERT_FALSE(store_.GetConfig("cfg", 1, &old, &code));
    EXPECT_EQ(code, Code::VERSION_NOT_FOUND);

    // 保留的 v11 仍可读
    ASSERT_TRUE(store_.GetConfig("cfg", 11, &old, &code));
    EXPECT_EQ(old.value(), "11");

    // 历史只含最近 5 个
    std::vector<KV> history;
    store_.GetHistory("cfg", &history);
    EXPECT_EQ(history.size(), 5u);
}
