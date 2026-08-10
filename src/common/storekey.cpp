#include "common/storekey.h"

#include <cstdio>
#include <cstring>
#include <sstream>

namespace configraft {
namespace storekey {

namespace {
constexpr char kRevisionMetaKey[] = "meta/revision";
constexpr char kMainPrefix[] = "k/";
constexpr char kVersionPrefix[] = "v/";
constexpr char kConfigPrefix[] = "cfg/";
}  // namespace

std::string RevisionMetaKey() { return kRevisionMetaKey; }

std::string MainKey(const std::string& key) { return kMainPrefix + key; }

std::string VersionKey(int64_t revision, const std::string& key) {
    return kVersionPrefix + EncodeOrd(revision) + "/" + key;
}

std::string ConfigKey(const std::string& key, int64_t version) {
    return kConfigPrefix + key + "/" + EncodeOrd(version);
}

std::string EncodeOrd(int64_t v) {
    char buf[17];
    std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(v));
    return std::string(buf, 16);
}

int64_t DecodeOrd(const std::string& hex) {
    return static_cast<int64_t>(std::stoll(hex, nullptr, 16));
}

void EncodeUint64(uint64_t v, std::string* out) {
    out->resize(8);
    for (int i = 7; i >= 0; --i) {
        (*out)[i] = static_cast<char>(v & 0xff);
        v >>= 8;
    }
}

uint64_t DecodeUint64(const std::string& in) {
    uint64_t v = 0;
    if (in.size() < 8) {
        return 0;
    }
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<uint8_t>(in[i]);
    }
    return v;
}

}  // namespace storekey
}  // namespace configraft
