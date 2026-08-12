#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "configraft.pb.h"
#include "leveldb/db.h"

namespace configraft {

using namespace v1;  // 协议类型（KV/Code 等）定义在 configraft.v1 包

// 存储层：LevelDB 封装 + MVCC 语义（参考 etcd v3）。
//
// 一致性约定：写路径仅在状态机 on_apply 中串行调用（单 Leader + 串行 apply），
// 因此 Store 内部无需加锁；读路径是并发的，只读 LevelDB（LevelDB 单线程写，
// 但读线程安全）。
//
// LevelDB key 布局见 common/storekey.h。
class Store {
public:
    Store() = default;
    ~Store();

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    // 打开/创建 LevelDB（data_dir 下）。失败返回 false 并填充 err。
    bool Open(const std::string& data_dir, std::string* err);
    void Close();

    // ---------------- 写路径（串行） ----------------
    // 每个写操作：分配新全局 revision → 写主索引 + 历史版本 → 递增 meta/revision。
    // 成功返回新 revision；out_kv 填充写入后的最新 KV（含 version/revision）。
    // 失败返回 -1。
    int64_t Put(const std::string& key, const std::string& value, KV* out_kv);
    int64_t Delete(const std::string& key, KV* out_kv);          // 写 tombstone
    int64_t BatchPut(const std::vector<std::pair<std::string, std::string>>& kvs,
                     std::vector<KV>* out_kvs);

    // ---------------- 读路径（并发） ----------------
    // 读当前值。key 不存在或已被删除时返回 false，code 置 KEY_NOT_FOUND。
    bool Get(const std::string& key, KV* out, int32_t* code) const;

    // ---------------- 元信息 ----------------
    // 当前已分配的最新全局 revision（仅读，从 LevelDB 读取）。
    int64_t CurrentRevision() const;

    // ---------------- 快照 ----------------
    // 用快照数据重建整个 DB（braft on_snapshot_load 使用）：关闭现有 DB、
    // 重建目录、批量写入快照的主索引与 revision。data 为空则清空。
    bool LoadSnapshot(const SnapshotData& data, std::string* err);

    leveldb::DB* db() const { return db_.get(); }

private:
    // 读取主索引 KV（不存在返回 false）
    bool ReadMain(const std::string& key, KV* out) const;
    // 分配下一个 revision 并写回 meta/revision（仅写路径串行调用）
    int64_t NextRevision();
    // 序列化 KV 到字符串
    static void SerializeKV(const KV& kv, std::string* out);

    std::unique_ptr<leveldb::DB> db_;
    std::string data_dir_;  // LevelDB 目录（快照重建时使用）
};

}  // namespace configraft
