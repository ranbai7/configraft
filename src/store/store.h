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

    // ---------------- 配置语义（M3） ----------------
    // 发布配置：同 Put 但额外写 cfg/{key}/{version} 索引（供按版本查询）。
    int64_t Publish(const std::string& key, const std::string& value, KV* out_kv);
    // 回滚到指定版本：读目标版本的值，作为新版本写回（不删历史，etcd 语义）。
    // 目标版本不存在时 ok=false。
    int64_t Rollback(const std::string& key, int64_t target_version, KV* out_kv, bool* ok);
    // 读指定版本配置。version<=0 表示最新（主索引）；>0 读 cfg 索引。
    // 不存在时返回 false，code 置 VERSION_NOT_FOUND。
    bool GetConfig(const std::string& key, int64_t version, KV* out, int32_t* code) const;
    // 返回该 key 的全部历史版本（按 version 升序，供 ConfigResponse.history）。
    void GetHistory(const std::string& key, std::vector<KV>* out) const;
    // 回收过期历史版本：每 key 仅保留最近 keep_versions 个版本，
    // 删除更早的 v/ 记录与对应 cfg/ 记录（tombstone 一并回收）。
    // 返回删除的记录条数。
    int Compaction(int keep_versions);

    // ---------------- 读路径（并发） ----------------
    // 读当前值。key 不存在或已被删除时返回 false，code 置 KEY_NOT_FOUND。
    bool Get(const std::string& key, KV* out, int32_t* code) const;

    // ---------------- Watch（M4） ----------------
    // 重放 (from_revision, current_revision] 开区间内 key 匹配（空=全部）的变更事件，
    // 按 revision 升序，最多 max_events 条（超出部分留给客户端以最后事件 revision 续传）。
    // 事件数据直接复用 MVCC 历史版本（v/ 前缀），无需额外持久化。
    // 若 from_revision 之前的历史已被 Compaction 回收，返回 false 且 *code=COMPACTED。
    bool ReplayEvents(int64_t from_revision, int64_t current_revision,
                      const std::string& key, int max_events,
                      std::vector<WatchEvent>* out, int32_t* code) const;
    // Compaction 已回收的最大 revision（meta/compact_rev，缺失返回 0）。
    // Watch 判定：from_revision < CompactRev() 时历史不完整，须返回 COMPACTED。
    int64_t CompactRev() const;

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
