cmake_minimum_required(VERSION 2.8)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

project (ForestDBench)
include_directories("/usr/local/include")
link_directories("/usr/local/lib")
add_executable(fdb_bench
               fdb_bench.cc
               timing.cc)

if ((NOT WIN32) AND (NOT APPLE))
    target_link_libraries(fdb_bench forestdb -lrt)
else ((NOT WIN32) AND (NOT APPLE))
    target_link_libraries(fdb_bench forestdb)
endif ((NOT WIN32) AND (NOT APPLE))

# add test target
add_test(fdb_bench fdb_bench)
ADD_CUSTOM_TARGET(benchmark
    COMMAND ctest
)
