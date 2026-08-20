find_package(GTest REQUIRED)

add_executable(store_test store_test.cpp)
target_link_libraries(store_test PRIVATE configraft_core GTest::gtest_main)
add_test(NAME store_test COMMAND store_test)

add_executable(watch_test watch_test.cpp)
target_link_libraries(watch_test PRIVATE configraft_core GTest::gtest_main)
add_test(NAME watch_test COMMAND watch_test)

# M5：单节点 braft 组并发 CAS 原子性（走真实 Raft apply 路径）
add_executable(raft_cas_test raft_cas_test.cpp)
target_link_libraries(raft_cas_test PRIVATE configraft_core GTest::gtest_main)
add_test(NAME raft_cas_test COMMAND raft_cas_test)

# HTTP/RESTful 集成测试（M8 审查补充）：拉起单机 server，经 brpc HTTP/gRPC 客户端
# 验证路由分发、CORS 收紧、参数校验、错误码与内置服务关闭（接线层零测试补全）
add_executable(http_test http_test.cpp)
target_link_libraries(http_test PRIVATE configraft_core GTest::gtest_main)
add_test(NAME http_test COMMAND http_test)
