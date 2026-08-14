#include <cstdlib>
#include <cstring>
#include <string>

#include <gtest/gtest.h>

#include "store/store.h"
#include "store/store_ops.h"

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

// ---------------- M5：CAS 原子更新 ----------------

TEST_F(StoreTest, CasSuccessUpdatesValue) {
    store_.Put("k", "old", nullptr);
    const int64_t before = store_.CurrentRevision();

    KV out;
    int32_t code = -1;
    const int64_t rev = store_.CompareAndSwap("k", "old", "new", &out, &code);
    ASSERT_GT(rev, 0);
    EXPECT_EQ(code, Code::OK);
    EXPECT_EQ(out.value(), "new");
    EXPECT_EQ(out.version(), 2);
    EXPECT_EQ(out.revision(), rev);
    EXPECT_EQ(rev, before + 1);  // 成功消耗一个新 revision
}

TEST_F(StoreTest, CasExpectMismatchNoRevisionConsumed) {
    store_.Put("k", "old", nullptr);
    const int64_t before = store_.CurrentRevision();

    KV out;
    int32_t code = -1;
    const int64_t rev = store_.CompareAndSwap("k", "wrong", "new", &out, &code);
    EXPECT_LT(rev, 0);
    EXPECT_EQ(code, Code::CAS_FAILED);
    EXPECT_EQ(store_.CurrentRevision(), before);  // 失败不消耗 revision

    // 原值未被改动
    KV cur;
    int32_t gcode = 0;
    ASSERT_TRUE(store_.Get("k", &cur, &gcode));
    EXPECT_EQ(cur.value(), "old");
}

TEST_F(StoreTest, CasAbsentWithEmptyExpect) {
    KV out;
    int32_t code = -1;
    const int64_t rev = store_.CompareAndSwap("k", "", "v1", &out, &code);
    ASSERT_GT(rev, 0);
    EXPECT_EQ(code, Code::OK);
    EXPECT_EQ(out.version(), 1);
}

TEST_F(StoreTest, CasAbsentWithNonEmptyExpect) {
    KV out;
    int32_t code = -1;
    EXPECT_LT(store_.CompareAndSwap("k", "x", "v1", &out, &code), 0);
    EXPECT_EQ(code, Code::CAS_FAILED);
}

TEST_F(StoreTest, CasRebuildAfterDelete) {
    store_.Put("k", "v1", nullptr);
    store_.Delete("k", nullptr);
    // tombstone 视为不存在：expect="" 可重建，version 延续 tombstone 计数
    KV out;
    int32_t code = -1;
    const int64_t rev = store_.CompareAndSwap("k", "", "v2", &out, &code);
    ASSERT_GT(rev, 0);
    EXPECT_EQ(code, Code::OK);
    EXPECT_EQ(out.value(), "v2");
    EXPECT_EQ(out.version(), 3);  // Put(v1) + Delete + CAS = 第 3 次修改
}

TEST_F(StoreTest, CasEmptyValueIsNotAbsent) {
    // 存在且值为空串：expect="" 表示"期望不存在"，故不匹配（歧义文档化：
    // CAS 无法用空 expect 表达"期望值为空串"，更新空串值请用 Put）
    store_.Put("k", "", nullptr);
    KV out;
    int32_t code = -1;
    EXPECT_LT(store_.CompareAndSwap("k", "", "v1", &out, &code), 0);
    EXPECT_EQ(code, Code::CAS_FAILED);

    // 再次调用值仍为空串（首次失败未改动），依旧不匹配
    code = -1;
    EXPECT_LT(store_.CompareAndSwap("k", "", "v1", &out, &code), 0);
    EXPECT_EQ(code, Code::CAS_FAILED);

    // 用 Put 可更新空串值（CAS 语义上的已知歧义）
    store_.Put("k", "v1", nullptr);
    KV cur;
    int32_t gcode = 0;
    ASSERT_TRUE(store_.Get("k", &cur, &gcode));
    EXPECT_EQ(cur.value(), "v1");
}

// CAS 走 ApplyCmdToStore（on_apply 同一路径）：成功产生 PUT 事件，失败无事件
TEST_F(StoreTest, ApplyCmdCasEmitsEventOnSuccess) {
    store_.Put("k", "old", nullptr);  // seed

    RaftCmd cmd;
    auto* cas = cmd.mutable_cas();
    cas->set_key("k");
    cas->set_expect("old");
    cas->set_value("new");

    ApplyResult result;
    ApplyCmdToStore(&store_, cmd, &result);
    ASSERT_EQ(result.code, Code::OK);
    ASSERT_EQ(result.events.size(), 1u);
    EXPECT_EQ(result.events[0].type(), "PUT");
    EXPECT_EQ(result.events[0].key(), "k");
    EXPECT_EQ(result.events[0].value(), "new");
}

TEST_F(StoreTest, ApplyCmdCasFailedNoEvent) {
    RaftCmd cmd;
    auto* cas = cmd.mutable_cas();
    cas->set_key("k");        // 不存在
    cas->set_expect("old");   // 期望存在但实际不存在 → 不匹配
    cas->set_value("new");

    ApplyResult result;
    ApplyCmdToStore(&store_, cmd, &result);
    EXPECT_EQ(result.code, Code::CAS_FAILED);
    EXPECT_TRUE(result.events.empty());
}
