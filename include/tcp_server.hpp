#pragma once

#include <cstdint>
#include <string>
#include <string_view>

enum class QueueImplementation {
    mutex,
    spsc
};

struct Options {
    uint64_t expect = 1000000;
    int workers = 1;
    int io_cpu = -1;
    int worker_cpu_base = -1;
    uint16_t port = 9000;
    QueueImplementation queue = QueueImplementation::mutex;
};

Options loadOptions(std::string_view path);
void runTcpServer(const Options& options);