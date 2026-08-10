#pragma once
#include <string>
#include <utility>

namespace configraft {

// 简单状态封装：code 语义对齐 configraft.v1.Code
struct Status {
    int32_t code = 0;
    std::string message;

    bool ok() const { return code == 0; }

    static Status OK() { return Status{}; }
    static Status Error(int32_t c, std::string msg) {
        Status s;
        s.code = c;
        s.message = std::move(msg);
        return s;
    }
};

}  // namespace configraft
