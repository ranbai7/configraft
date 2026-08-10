cmake_minimum_required(VERSION 3.16)
project(configraft VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ------------------------------------------------------------------
# 第三方依赖：全部来自 scripts/build_deps.sh 构建的 third_party/install
# （protobuf/brpc/braft/gtest 自构建；leveldb/gflags/ssl 复用系统）
# ------------------------------------------------------------------
set(DEPS_PREFIX "${CMAKE_SOURCE_DIR}/third_party/install")
set(CMAKE_PREFIX_PATH "${DEPS_PREFIX}")

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

include_directories(SYSTEM "${DEPS_PREFIX}/include")
# 第三方库目录（braft/brpc 自构建；leveldb/gflags/ssl 复用系统，系统路径自动覆盖）
link_directories("${DEPS_PREFIX}/lib")

# 链接顺序：上层库在前，被依赖库在后（GCC 静态链接要求）
set(DEPS_LIB
    braft          # 工业级 C++ Raft
    brpc           # 统一网络框架（Raft 内部 + gRPC + HTTP + 监控）
    ${Protobuf_LIBRARIES}   # protobuf
    leveldb        # 存储引擎（系统）
    gflags         # 命令行参数（系统）
    ssl crypto
    z
    pthread dl rt
)

# ------------------------------------------------------------------
# 生成 proto 代码（build/configraft.pb.{h,cc}）
# protobuf_generate_cpp 默认输出到 CMAKE_CURRENT_BINARY_DIR（即 build/）
# ------------------------------------------------------------------
set(PROTO_SRCS "${CMAKE_BINARY_DIR}/configraft.pb.cc")
set(PROTO_HDRS "${CMAKE_BINARY_DIR}/configraft.pb.h")
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
    "${CMAKE_SOURCE_DIR}/proto/configraft.proto"
)

# ------------------------------------------------------------------
# 核心库：所有业务模块打成静态库，供 server / client / tests 复用
# ------------------------------------------------------------------
file(GLOB_RECURSE CORE_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_SOURCE_DIR}/src/common/*.cpp"
    "${CMAKE_SOURCE_DIR}/src/store/*.cpp"
    "${CMAKE_SOURCE_DIR}/src/raft/*.cpp"
    "${CMAKE_SOURCE_DIR}/src/server/*.cpp"
)

add_library(configraft_core STATIC ${CORE_SOURCES} ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(configraft_core PUBLIC
    "${CMAKE_SOURCE_DIR}"
    "${CMAKE_SOURCE_DIR}/src"   # 支持 #include "server/xxx.h" / "store/xxx.h"
    "${CMAKE_BINARY_DIR}"       # configraft.pb.h 生成位置
    "${DEPS_PREFIX}/include"
)
target_link_libraries(configraft_core PUBLIC ${DEPS_LIB})

# ------------------------------------------------------------------
# 可执行文件
# ------------------------------------------------------------------
add_executable(configraft_server src/main/server_main.cpp)
target_link_libraries(configraft_server PRIVATE configraft_core)

add_executable(configraft_client client/client_main.cpp)
target_link_libraries(configraft_client PRIVATE configraft_core)

# ------------------------------------------------------------------
# 测试
# ------------------------------------------------------------------
enable_testing()
add_subdirectory(tests)
