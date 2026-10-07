#include "tcp_server.hpp"
#include "message.hpp"
#include "common.hpp"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>
#include <string_view>

struct Options {
    uint64_t expect = 1000000;
    int workers = 1;
    int io_cpu = -1;
    int worker_cpu_base = -1;
    uint16_t port = 9000;
    std::string queue = "mutex";
};

static Options parse_args(int argc, char* argv[]) { // will switch out to using file based program parameters
    Options opts;
    for(int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--expect") {
            opts.expect = std::stoull(argv[++i]);
        } else if (arg == "--workers") {
            opts.workers = std::stoi(argv[++i]);
        } else if (arg == "--io-cpu") {
            opts.io_cpu = std::stoi(argv[++i]);
        } else if (arg == "--worker-cpu-base") {
            opts.worker_cpu_base = std::stoi(argv[++i]);
        } else if (arg == "--port") {
            opts.port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--queue") {
            opts.queue = argv[++i];
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
        }
    }
    return opts;
}

static inline void handle_event(const Message& m, uint64_t& sink) {
    for(uint8_t i = 0; i < sizeof(m.payload); ++i) {
        sink += m.payload[i];
    }
}

static std::pair<int, int> make_listener(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) die("socket");

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        die("bind");
    }

    if (listen(fd, 15) < 0) {
        die("listen");
    }

    int epollfd = epoll_create1(0); 
    if(epollfd < 0) {
        die("epoll instance");
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    if(epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        die("epoll_ctl");
    }

    return {epollfd, fd};
}

void runTcpServer() {
    std::pair<int, int> listener = make_listener(8080);
    int epollfd = listener.first;
    int listenfd = listener.second;

    epoll_event events[128];
    while(true) {
        int nfds = epoll_wait(epollfd, events, 128, -1);
        for(int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;
            if(events[i].data.fd == listenfd) {
                // accept connections and
            }
        }
        
    }
}