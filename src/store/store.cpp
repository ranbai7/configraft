#include "store/store.h"

#include <filesystem>
#include <map>
#include <utility>

#include "common/storekey.h"
#include "leveldb/write_batch.h"

namespace configraft {

Store::~Store() { Close(); }

bool Store::Open(const std::string& data_dir, std::string* err) {
    std::filesystem::create_directories(data_dir);
    leveldb::Options opts;
    opts.create_if_missing = true;
    leveldb::DB* db = nullptr;
    const leveldb::Status s = leveldb::DB::Open(opts, data_dir, &db);
    if (!s.ok()) {
        if (err) {
            *err = s.ToString();
        }
        return false;
    }
    db_.reset(db);
    data_dir_ = data_dir;
    return true;
}

void Store::Close() {
    if (db_) {
        db_.reset();
    }
}

void Store::SerializeKV(const KV& kv, std::string* out) {
    kv.SerializeToString(out);
}

bool Store::ReadMain(const std::string& key, KV* out) const {
    std::string raw;
    const leveldb::Status s =
        db_->Get(leveldb::ReadOptions(), storekey::MainKey(key), &raw);
    if (s.IsNotFound()) {
        return false;
    }
    if (!s.ok()) {
        return false;
    }
    return out->ParseFromString(raw);
}

int64_t Store::NextRevision() {
    std::string raw;
    const leveldb::Status s = db_->Get(
        leveldb::ReadOptions(), storekey::RevisionMetaKey(), &raw);
    uint64_t rev = 0;
    if (s.ok()) {
        rev = storekey::DecodeUint64(raw);
    }
    rev += 1;
    std::string encoded;
    storekey::EncodeUint64(rev, &encoded);
    const leveldb::Status ws =
        db_->Put(leveldb::WriteOptions(), storekey::RevisionMetaKey(), encoded);
    if (!ws.ok()) {
        return -1;
    }
    return static_cast<int64_t>(rev);
}

int64_t Store::Put(const std::string& key, const std::string& value, KV* out_kv) {
    KV old_kv;
    bool existed = ReadMain(key, &old_kv);

    const int64_t rev = NextRevision();
    if (rev < 0) {
        return -1;
    }

    KV new_kv;
    new_kv.set_key(key);
    new_kv.set_value(value);
    new_kv.set_version(existed ? old_kv.version() + 1 : 1);
    new_kv.set_revision(rev);
    new_kv.set_deleted(false);

    std::string serialized;
    SerializeKV(new_kv, &serialized);

    leveldb::WriteBatch batch;
    batch.Put(storekey::MainKey(key), serialized);
    batch.Put(storekey::VersionKey(rev, key), serialized);
    const leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
        return -1;
    }
    if (out_kv) {
        *out_kv = std::move(new_kv);
    }
    return rev;
}

int64_t Store::Delete(const std::string& key, KV* out_kv) {
    const int64_t rev = NextRevision();
    if (rev < 0) {
        return -1;
    }

    KV kv;
    kv.set_key(key);
    kv.set_deleted(true);
    kv.set_revision(rev);

    // 若存在，继承 version 语义：删除也计数（便于理解修改历史）
    KV old_kv;
    if (ReadMain(key, &old_kv)) {
        kv.set_version(old_kv.version() + 1);
    } else {
        kv.set_version(1);
    }

    std::string serialized;
    SerializeKV(kv, &serialized);

    leveldb::WriteBatch batch;
    batch.Put(storekey::MainKey(key), serialized);
    batch.Put(storekey::VersionKey(rev, key), serialized);
    const leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
        return -1;
    }
    if (out_kv) {
        *out_kv = std::move(kv);
    }
    return rev;
}

int64_t Store::BatchPut(
    const std::vector<std::pair<std::string, std::string>>& kvs,
    std::vector<KV>* out_kvs) {
    if (kvs.empty()) {
        return 0;
    }
    leveldb::WriteBatch batch;
    std::vector<KV> new_kvs;
    new_kvs.reserve(kvs.size());

    for (const auto& [key, value] : kvs) {
        KV old_kv;
        const bool existed = ReadMain(key, &old_kv);

        const int64_t rev = NextRevision();
        if (rev < 0) {
            return -1;
        }

        KV new_kv;
        new_kv.set_key(key);
        new_kv.set_value(value);
        new_kv.set_version(existed ? old_kv.version() + 1 : 1);
        new_kv.set_revision(rev);
        new_kv.set_deleted(false);

        std::string serialized;
        SerializeKV(new_kv, &serialized);
        batch.Put(storekey::MainKey(key), serialized);
        batch.Put(storekey::VersionKey(rev, key), serialized);
        new_kvs.push_back(std::move(new_kv));
    }

    const leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
        return -1;
    }
    if (out_kvs) {
        *out_kvs = std::move(new_kvs);
    }
    return 0;  // 批量成功，revision 由各条记录携带
}

int64_t Store::Publish(const std::string& key, const std::string& value, KV* out_kv) {
    KV old_kv;
    const bool existed = ReadMain(key, &old_kv);

    const int64_t rev = NextRevision();
    if (rev < 0) {
        return -1;
    }

    KV new_kv;
    new_kv.set_key(key);
    new_kv.set_value(value);
    new_kv.set_version(existed ? old_kv.version() + 1 : 1);
    new_kv.set_revision(rev);
    new_kv.set_deleted(false);

    std::string serialized;
    SerializeKV(new_kv, &serialized);

    leveldb::WriteBatch batch;
    batch.Put(storekey::MainKey(key), serialized);
    batch.Put(storekey::VersionKey(rev, key), serialized);
    batch.Put(storekey::ConfigKey(key, new_kv.version()), serialized);
    const leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
        return -1;
    }
    if (out_kv) {
        *out_kv = std::move(new_kv);
    }
    return rev;
}

int64_t Store::Rollback(const std::string& key, int64_t target_version, KV* out_kv,
                        bool* ok) {
    if (ok) {
        *ok = false;
    }
    KV target;
    int32_t code = 0;
    if (!GetConfig(key, target_version, &target, &code)) {
        return -1;
    }
    if (ok) {
        *ok = true;
    }
    // 回滚不删历史：把目标版本的值作为新版本发布（对齐 etcd 语义）
    return Publish(key, target.value(), out_kv);
}

bool Store::GetConfig(const std::string& key, int64_t version, KV* out,
                      int32_t* code) const {
    if (version <= 0) {
        return Get(key, out, code);
    }
    std::string raw;
    const leveldb::Status s =
        db_->Get(leveldb::ReadOptions(), storekey::ConfigKey(key, version), &raw);
    if (s.IsNotFound()) {
        if (code) {
            *code = Code::VERSION_NOT_FOUND;
        }
        return false;
    }
    if (!s.ok()) {
        if (code) {
            *code = Code::INTERNAL;
        }
        return false;
    }
    if (!out->ParseFromString(raw)) {
        if (code) {
            *code = Code::INTERNAL;
        }
        return false;
    }
    if (out->deleted()) {
        if (code) {
            *code = Code::VERSION_NOT_FOUND;
        }
        return false;
    }
    if (code) {
        *code = Code::OK;
    }
    return true;
}

void Store::GetHistory(const std::string& key, std::vector<KV>* out) const {
    const std::string prefix = "cfg/" + key + "/";
    leveldb::ReadOptions ro;
    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(ro));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        KV kv;
        if (kv.ParseFromString(it->value().ToString()) && !kv.deleted()) {
            out->push_back(std::move(kv));
        }
    }
}

int Store::Compaction(int keep_versions) {
    if (keep_versions < 1) {
        keep_versions = 1;
    }
    // 遍历 v/ 前缀，按 key 收集所有 (revision, version)，revision 升序
    std::map<std::string, std::vector<std::pair<int64_t, int64_t>>> per_key;
    leveldb::ReadOptions ro;
    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(ro));
    const std::string v_prefix = "v/";
    for (it->Seek(v_prefix); it->Valid() && it->key().starts_with(v_prefix);
         it->Next()) {
        // key 格式: v/{rev:16hex}/{key}
        const leveldb::Slice k = it->key();
        if (k.size() < 18) {
            continue;
        }
        const std::string rev_hex = k.ToString().substr(2, 16);
        const std::string key = k.ToString().substr(2 + 16 + 1);
        KV kv;
        if (!kv.ParseFromString(it->value().ToString())) {
            continue;
        }
        per_key[key].emplace_back(storekey::DecodeOrd(rev_hex), kv.version());
    }

    // 对每个 key，删除最旧的 excess 个版本对应的 v/ 与 cfg/ 记录；
    // 同时记录被删 v/ 记录的最大 revision（Watch 断点续传的回收判定锚点）
    leveldb::WriteBatch batch;
    int removed = 0;
    int64_t max_deleted_rev = 0;
    for (const auto& [key, records] : per_key) {
        const int excess = static_cast<int>(records.size()) - keep_versions;
        if (excess <= 0) {
            continue;
        }
        for (int i = 0; i < excess; ++i) {
            const auto& [rev, ver] = records[i];
            batch.Delete(storekey::VersionKey(rev, key));
            batch.Delete(storekey::ConfigKey(key, ver));
            if (rev > max_deleted_rev) {
                max_deleted_rev = rev;
            }
            ++removed;
        }
    }
    if (removed > 0) {
        // compact_rev = 历史累计被删的最大 revision。读改写仅在 Compaction 线程串行执行，
        // 与所有 Delete 放同一 WriteBatch 原子提交：读者要么看到"记录已删 + compact_rev 已更新"，
        // 要么都看不到，杜绝 Watch 误放行的中间态。
        const int64_t new_compact_rev =
            std::max(CompactRev(), max_deleted_rev);
        std::string raw;
        storekey::EncodeUint64(static_cast<uint64_t>(new_compact_rev), &raw);
        batch.Put(storekey::CompactRevKey(), raw);
        db_->Write(leveldb::WriteOptions(), &batch);
    }
    return removed;
}

bool Store::Get(const std::string& key, KV* out, int32_t* code) const {
    KV kv;
    if (!ReadMain(key, &kv)) {
        if (code) {
            *code = Code::KEY_NOT_FOUND;
        }
        return false;
    }
    if (kv.deleted()) {
        if (code) {
            *code = Code::KEY_NOT_FOUND;
        }
        return false;
    }
    if (out) {
        *out = std::move(kv);
    }
    if (code) {
        *code = Code::OK;
    }
    return true;
}

int64_t Store::CurrentRevision() const {
    std::string raw;
    const leveldb::Status s = db_->Get(
        leveldb::ReadOptions(), storekey::RevisionMetaKey(), &raw);
    if (!s.ok()) {
        return 0;
    }
    return static_cast<int64_t>(storekey::DecodeUint64(raw));
}

// ---------------- Watch（M4） ----------------

int64_t Store::CompactRev() const {
    std::string raw;
    const leveldb::Status s = db_->Get(
        leveldb::ReadOptions(), storekey::CompactRevKey(), &raw);
    if (!s.ok()) {
        return 0;
    }
    return static_cast<int64_t>(storekey::DecodeUint64(raw));
}

bool Store::ReplayEvents(int64_t from_revision, int64_t current_revision,
                         const std::string& key, int max_events,
                         std::vector<WatchEvent>* out, int32_t* code) const {
    if (code) {
        *code = Code::OK;
    }
    if (out) {
        out->clear();
    }
    if (max_events <= 0) {
        return true;
    }
    if (from_revision < 0) {
        from_revision = 0;
    }

    // 顺序不变量：先建立一致迭代器快照（钉住 LevelDB 版本集），再读 compact_rev。
    // 与 Compaction 的原子写入（删除 + compact_rev 同一 WriteBatch）配合：
    //   - Compaction 先提交：迭代器看不到被删记录，但 Get 已见新 compact_rev → 报 COMPACTED；
    //   - Compaction 后提交：迭代器快照仍含记录，Get 可能见新 compact_rev → 保守误报 COMPACTED。
    // 二者都杜绝"读到不完整重放却未报 COMPACTED"。
    leveldb::ReadOptions ro;
    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(ro));
    const int64_t compact_rev = CompactRev();

    // compact_rev = 被删 v/ 记录的最大 revision，故 (from, current] 内无洞 ⟺ from >= compact_rev。
    if (from_revision < compact_rev) {
        if (code) {
            *code = Code::COMPACTED;
        }
        return false;
    }
    if (from_revision >= current_revision) {
        return true;  // 开区间为空，无事件可重放
    }

    // Seek 到 (from_revision+1) 的第一个 v/ 记录（v/ 按 revision 字典序 == 数值序升序）
    for (it->Seek(storekey::VersionKey(from_revision + 1, ""));
         it->Valid() && it->key().starts_with("v/"); it->Next()) {
        const leveldb::Slice k = it->key();
        if (k.size() < 18) {  // "v/" + 16 hex + "/" 之后的 key 至少占 1 位
            continue;
        }
        const int64_t rev = storekey::DecodeOrd(k.ToString().substr(2, 16));
        if (rev > current_revision) {
            break;
        }
        KV kv;
        if (!kv.ParseFromString(it->value().ToString())) {
            continue;
        }
        if (!key.empty() && kv.key() != key) {
            continue;
        }
        WatchEvent ev;
        ev.set_revision(rev);
        ev.set_key(kv.key());
        ev.set_value(kv.value());
        ev.set_version(kv.version());
        ev.set_type(kv.deleted() ? "DELETE" : "PUT");
        if (out) {
            out->push_back(std::move(ev));
        }
        if (out && static_cast<int>(out->size()) >= max_events) {
            break;
        }
    }
    return true;
}

bool Store::LoadSnapshot(const SnapshotData& data, std::string* err) {
    // 1. 关闭现有 DB
    db_.reset();
    // 2. 删除并重建目录（LevelDB 不允许直接在旧目录上重开）
    std::error_code ec;
    std::filesystem::remove_all(data_dir_, ec);
    // 3. 重新打开
    if (!Open(data_dir_, err)) {
        return false;
    }
    // 4. 批量写入快照的主索引与历史版本
    leveldb::WriteBatch batch;
    for (const auto& kv : data.kvs()) {
        std::string serialized;
        kv.SerializeToString(&serialized);
        batch.Put(storekey::MainKey(kv.key()), serialized);
        batch.Put(storekey::VersionKey(kv.revision(), kv.key()), serialized);
    }
    if (data.revision() > 0) {
        std::string raw;
        storekey::EncodeUint64(static_cast<uint64_t>(data.revision()), &raw);
        batch.Put(storekey::RevisionMetaKey(), raw);
        // 快照只保留主索引（历史由后续日志重建），故 compact_rev 重置为快照 revision：
        // 任何 from_revision < snapshot.revision() 的续传都是不完整的（历史已丢弃），须报 COMPACTED。
        std::string compact_raw;
        storekey::EncodeUint64(static_cast<uint64_t>(data.revision()), &compact_raw);
        batch.Put(storekey::CompactRevKey(), compact_raw);
    }
    const leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
        if (err) {
            *err = "write snapshot failed: " + s.ToString();
        }
        return false;
    }
    return true;
}

}  // namespace configraft
