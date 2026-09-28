cmake_minimum_required(VERSION 3.10.0)
project(metrics-pipeline VERSION 0.1.0 LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(THREADS_PREFER_PTHREAD_FLAG true)

find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)

set(SOURCES 
    src/main.cpp
    src/tcp_server.cpp
    src/http_request.cpp
)

add_executable(metrics-pipeline ${SOURCES})
target_include_directories(metrics-pipeline PRIVATE include)
target_compile_options(metrics-pipeline PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(metrics-pipeline PRIVATE SQLite::SQLite3 Threads::Threads)