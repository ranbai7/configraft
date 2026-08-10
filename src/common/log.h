#pragma once
// 轻量日志宏（不依赖 glog/brpc 日志）：LOG(INFO)/LOG(ERROR)/LOG(WARNING)。
// 输出到 stderr，带时间戳与级别前缀。线程安全（单次 fprintf 原子性足够）。
//
// 用法：LOG(INFO) << "message " << 42;
// 说明：本项目 brpc 以 WITH_GLOG=OFF 构建，未安装 glog 头文件，故自备。

#include <cstdio>
#include <ctime>
#include <sstream>
#include <string>

namespace configraft {
namespace log_internal {

inline std::string CurrentTimeStr() {
    char buf[32];
    const std::time_t now = std::time(nullptr);
    std::tm tm{};
    localtime_r(&now, &tm);
    std::strftime(buf, sizeof(buf), "%H:%M:%S", &tm);
    return buf;
}

struct Logger {
    std::string level;
    std::ostringstream ss;
    ~Logger() {
        std::fprintf(stderr, "[%s %s] %s\n", CurrentTimeStr().c_str(),
                     level.c_str(), ss.str().c_str());
    }
};

}  // namespace log_internal
}  // namespace configraft

#define LOG(severity) ::configraft::log_internal::Logger{#severity}.ss
