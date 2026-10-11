#include "tcp_server.hpp"

#include <exception>
#include <iostream>

int main() {
    try {
        const Options options = loadOptions(METRICS_OPTIONS_FILE);
        runTcpServer(options);
    } catch (const std::exception& error) {
        std::cerr << "Startup failed: " << error.what() << '\n';
        return 1;
    }
}
