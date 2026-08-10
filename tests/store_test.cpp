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
