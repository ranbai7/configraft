#include "store/store.h"

#include <filesystem>
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
