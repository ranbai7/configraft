find_package(GTest REQUIRED)

add_executable(store_test store_test.cpp)
target_link_libraries(store_test PRIVATE configraft_core GTest::gtest_main)
add_test(NAME store_test COMMAND store_test)

add_executable(watch_test watch_test.cpp)
target_link_libraries(watch_test PRIVATE configraft_core GTest::gtest_main)
add_test(NAME watch_test COMMAND watch_test)
