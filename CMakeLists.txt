cmake_minimum_required(VERSION 3.10.0)
project(metrics-pipeline VERSION 0.1.0 LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(THREADS_PREFER_PTHREAD_FLAG true)

find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)
include(FetchContent)

FetchContent_Declare(
  simdjson
  GIT_REPOSITORY https://github.com/simdjson/simdjson.git
  GIT_TAG  tags/v5.0.2
  GIT_SHALLOW TRUE)

FetchContent_MakeAvailable(simdjson)

set(METRICS_OPTIONS_FILE "${CMAKE_CURRENT_SOURCE_DIR}/config/options.json")

set(SOURCES 
    src/main.cpp
    src/tcp_server.cpp
    src/thread_pool.cpp
)

add_executable(metrics-pipeline ${SOURCES})
target_include_directories(metrics-pipeline PRIVATE include)
target_compile_definitions(metrics-pipeline PRIVATE
    METRICS_OPTIONS_FILE="${METRICS_OPTIONS_FILE}"
)
target_compile_options(metrics-pipeline PRIVATE -Wall -Wextra -Wpedantic)
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_options(metrics-pipeline PRIVATE -std=c++2c)
endif()
target_link_libraries(metrics-pipeline PRIVATE SQLite::SQLite3 Threads::Threads simdjson)