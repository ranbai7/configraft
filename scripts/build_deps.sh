#!/usr/bin/env bash
# Configraft 第三方依赖构建脚本（幂等，可重复执行）
#
# 从源码构建 protobuf / brpc / braft / googletest，统一安装到 third_party/install，
# 使项目完全自包含，规避系统 protobuf 头(3.12.4)/库(23.0.4)版本不匹配的问题。
#
# 用法:  scripts/build_deps.sh [JOBS]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
THIRD_PARTY="$REPO_ROOT/third_party"
SRC_DIR="$THIRD_PARTY/src"
PREFIX="$THIRD_PARTY/install"

JOBS="${1:-$(nproc)}"
# protobuf 单文件编译吃内存，内存紧张的机器用小并行
PB_JOBS="${PB_JOBS:-2}"

# ---- 版本锁定（brpc/braft/protobuf 兼容组合） ----
PROTOBUF_VER="v3.21.12"
BRPC_VER="1.17.0"
BRAFT_VER="v1.1.2"
GTEST_VER="release-1.11.0"

PROTOBUF_URL="https://github.com/protocolbuffers/protobuf/archive/refs/tags/${PROTOBUF_VER}.tar.gz"
BRPC_URL="https://github.com/apache/brpc/archive/refs/tags/${BRPC_VER}.tar.gz"
BRAFT_URL="https://github.com/baidu/braft/archive/refs/tags/${BRAFT_VER}.tar.gz"
GTEST_URL="https://github.com/google/googletest/archive/refs/tags/${GTEST_VER}.tar.gz"

mkdir -p "$SRC_DIR" "$PREFIX"
cd "$SRC_DIR"

# fetch <name> <url> <target-dir>：下载 tarball 并解压到 target-dir。
# 主源 github.com 失败时自动回退到 codeload.github.com（tarball 专用域，
# 对 GitHub 临时限流更稳健）。
fetch() {
    local name="$1" url="$2" target="$3"
    if [ -d "$target" ]; then
        echo ">> [$name] 源码已存在，跳过下载"
        return 0
    fi
    # https://github.com/<org>/<repo>/archive/refs/tags/<tag>.tar.gz
    #   → https://codeload.github.com/<org>/<repo>/tar.gz/refs/tags/<tag>
    local codeload_url
    codeload_url="$(printf '%s' "$url" \
        | sed 's#github.com/#codeload.github.com/#' \
        | sed 's#/archive/#/tar.gz/#' \
        | sed 's#\.tar\.gz$##')"
    echo ">> [$name] 下载 $url"
    if ! curl -fSL --retry 3 --connect-timeout 30 -o "$name.tar.gz" "$url"; then
        echo ">> [$name] 主源失败，回退 codeload: $codeload_url"
        curl -fSL --retry 3 --connect-timeout 30 -o "$name.tar.gz" "$codeload_url"
    fi
    tar xzf "$name.tar.gz"
    rm -f "$name.tar.gz"
    mv "$name"-* "$target"   # 解压目录形如 name-<tag>
    echo ">> [$name] 源码就绪: $target"
}

# build <name> <src-dir> <jobs> [cmake-extra-args...]
build() {
    local name="$1" src="$2" jobs="$3"; shift 3
    echo ">> [$name] cmake 配置"
    cmake -S "$src" -B "$src/build" \
        -DCMAKE_INSTALL_PREFIX="$PREFIX" \
        -DCMAKE_PREFIX_PATH="$PREFIX" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        "$@"
    echo ">> [$name] 编译 (-j$jobs)"
    cmake --build "$src/build" -j "$jobs"
    echo ">> [$name] 安装"
    cmake --install "$src/build"
    echo ">> [$name] 完成 ✓"
}

echo "=============================================="
echo " 第三方依赖: protobuf=$PROTOBUF_VER brpc=$BRPC_VER braft=$BRAFT_VER gtest=$GTEST_VER"
echo " 安装前缀: $PREFIX   并行: JOBS=$JOBS PB_JOBS=$PB_JOBS"
echo "=============================================="

# 1. protobuf（3.21.x：soname 23，与系统库 ABI 兼容的稳定版）
# 注意：必须启用 zlib（protobuf_WITH_ZLIB=ON），否则 brpc 的 GzipOutputStream
# 符号缺失会导致链接失败。
fetch protobuf "$PROTOBUF_URL" protobuf
build protobuf protobuf "$PB_JOBS" \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_SHARED_LIBS=OFF \
    -Dprotobuf_WITH_ZLIB=ON

PROTOBUF_CMAKE_DIR="$PREFIX/lib/cmake/protobuf"

# 2. brpc（统一网络框架：Raft 内部 + gRPC + HTTP + 监控面板）
# 跳过 brpc 自带的 tools（rpc_press 等）与单测 gtest 下载，加快构建。
fetch brpc "$BRPC_URL" brpc
build brpc brpc "$JOBS" \
    -DProtobuf_DIR="$PROTOBUF_CMAKE_DIR" \
    -DWITH_DEBUG_SYMBOLS=OFF \
    -DWITH_GLOG=OFF \
    -DWITH_MESALINK=OFF \
    -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_BRPC_TOOLS=OFF \
    -DDOWNLOAD_GTEST=OFF

# 3. braft（工业级 C++ Raft）
fetch braft "$BRAFT_URL" braft
# 打补丁：braft 多处 CMakeLists 定义了多余的 -D__const__=，在新版 glibc(>=2.35)
# 下会破坏 sys/cdefs.h 的 __glibc_has_attribute(__const__)（宏参数为空报错）。
# 源码并未使用 __const__，安全移除。幂等：重复执行无副作用。
for f in CMakeLists.txt tools/CMakeLists.txt example/counter/CMakeLists.txt \
         example/block/CMakeLists.txt example/atomic/CMakeLists.txt test/CMakeLists.txt; do
    sed -i 's/ -D__const__=//g' "$SRC_DIR/braft/$f" 2>/dev/null || true
done
# 打补丁：braft v1.1.2 与新版 brpc(>=1.10) 的 bvar API 兼容修复——util.cpp
# 在 namespace bvar::detail 内部误写 detail::Sample（应为 Sample）。
# 老版 brpc 存在 detail::detail 嵌套命名空间故可编译，新版已移除。
# 注意：只能对 detail 命名空间块内替换；块外（bvar::CounterRecorder::qps）
# 的 detail::Sample 是正确的，保留。
sed -i '/namespace detail {/,/}  \/\/ namespace detail/s/detail::Sample<Stat>/Sample<Stat>/g' \
    "$SRC_DIR/braft/src/braft/util.cpp" 2>/dev/null || true
build braft braft "$JOBS" \
    -DProtobuf_DIR="$PROTOBUF_CMAKE_DIR" \
    -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_EXAMPLES=OFF \
    -DBUILD_TESTS=OFF

# 4. googletest（单元测试）
fetch googletest "$GTEST_URL" googletest
build googletest googletest "$JOBS"

echo ""
echo ">>> 全部依赖构建完成，安装到: $PREFIX"
ls "$PREFIX/lib" | grep -E "protobuf|brpc|braft|gtest" | head -20
