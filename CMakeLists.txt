cmake_minimum_required(VERSION 3.20.0)
project(metrics-pipeline VERSION 0.1.0 LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(THREADS_PREFER_PTHREAD_FLAG true)

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
    src/server.cpp
    src/thread_pool.cpp
)

add_executable(metrics-pipeline ${SOURCES})
target_include_directories(metrics-pipeline PRIVATE include)
target_compile_definitions(metrics-pipeline PRIVATE
    METRICS_OPTIONS_FILE="${METRICS_OPTIONS_FILE}"
)
target_compile_options(metrics-pipeline PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(metrics-pipeline PRIVATE simdjson)

add_executable(metrics-loadgen tests/simulator.cpp)
target_include_directories(metrics-loadgen PRIVATE include)
target_compile_options(metrics-loadgen PRIVATE -Wall -Wextra -Wpedantic)

include(CTest)
if(BUILD_TESTING)
    add_executable(message-framing-tests tests/message_framing.cpp)
    target_include_directories(message-framing-tests PRIVATE include)
    target_compile_options(message-framing-tests PRIVATE -Wall -Wextra -Wpedantic)
    add_test(NAME message-framing-tests COMMAND message-framing-tests)
endif()
