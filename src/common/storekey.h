#pragma once
#include <cstdint>
#include <string>

namespace configraft {
namespace storekey {

// LevelDB key 布局（前缀即命名空间，排序可控）：
//   meta/revision          → 全局单调 revision（8 字节大端）
//   k/{key}                → 主索引：当前 KV（序列化的 configraft.v1.KV）
//   v/{rev:16hex}/{key}    → 历史版本记录（MVCC 主键，按 revision 排序）
//   cfg/{key}/{ver:16hex}  → 配置多版本索引（供 GetConfig(key, version)）

std::string RevisionMetaKey();                                  // "meta/revision"
std::string MainKey(const std::string& key);                    // "k/" + key
std::string VersionKey(int64_t revision, const std::string& key);  // "v/<hex>/" + key
std::string ConfigKey(const std::string& key, int64_t version);    // "cfg/" + key + "/<hex>"

// 固定宽度十六进制编码：保证字符串字典序 == 数值序（LevelDB 按字节序排序）
std::string EncodeOrd(int64_t v);      // 16 位小写 hex
int64_t DecodeOrd(const std::string& hex);

// 8 字节大端编解码（用于 meta/revision 等原始计数器）
void EncodeUint64(uint64_t v, std::string* out);
uint64_t DecodeUint64(const std::string& in);

}  // namespace storekey
}  // namespace configraft
