#include "server.hpp"
#include "message.hpp"
#include "message_framing.hpp"
#include "common.hpp"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>
#include <string_view>
#include <limits>
#include <stdexcept>
#include "simdjson.h"

Options loadOptions(std::string_view path) {
    simdjson::dom::parser parser;
    simdjson::dom::element document;
    auto error = parser.load(path).get(document);
    if (error) {
        throw std::runtime_error(
            "cannot parse options file '" + std::string(path) + "': " +
            simdjson::error_message(error)
        );
    }

    simdjson::dom::object object;
    error = document.get_object().get(object);
    if (error) {
        throw std::runtime_error(
            "options file must contain a JSON object: " +
            std::string(simdjson::error_message(error))
        );
    }

    Options options;
    for (const auto field : object) {
        const std::string_view key = field.key;
        if (key == "expect") {
            uint64_t value;
            error = field.value.get_uint64().get(value);
            if (error) {
                throw std::runtime_error("option 'expect' must be an unsigned integer");
            }
            options.expect = value;
        } else if (key == "workers") {
            int64_t value;
            error = field.value.get_int64().get(value);
            if (error || value < 1 || value > std::numeric_limits<int>::max()) {
                throw std::runtime_error("option 'workers' must be a positive integer");
            }
            options.workers = static_cast<int>(value);
        } else if (key == "io_cpu" || key == "worker_cpu_base") {
            int64_t value;
            error = field.value.get_int64().get(value);
            if (error || value < std::numeric_limits<int>::min() ||
                value > std::numeric_limits<int>::max()) {
                throw std::runtime_error(
                    "option '" + std::string(key) + "' must be an integer"
                );
            }
            if (key == "io_cpu") {
                options.io_cpu = static_cast<int>(value);
            } else {
                options.worker_cpu_base = static_cast<int>(value);
            }
        } else if (key == "port") {
            uint64_t value;
            error = field.value.get_uint64().get(value);
            if (error || value == 0 || value > std::numeric_limits<uint16_t>::max()) {
                throw std::runtime_error("option 'port' must be between 1 and 65535");
            }
            options.port = static_cast<uint16_t>(value);
        } else if (key == "queue") {
            std::string_view value;
            error = field.value.get_string().get(value);
            if (error) {
                throw std::runtime_error("option 'queue' must be a string");
            }
            if (value == "mutex") {
                options.queue = QueueImplementation::mutex;
            } else if (value == "spsc") {
                options.queue = QueueImplementation::spsc;
            } else {
                throw std::runtime_error(
                    "option 'queue' must be either 'mutex' or 'spsc'"
                );
            }
        } else {
            throw std::runtime_error("unknown option '" + std::string(key) + "'");
        }
    }

    return options;
}

static inline void handle_event(const Envelope& m, uint64_t& sink) {
    sink += 0; 
}

[[nodiscard]] static std::pair<int, int> make_listener(uint16_t port) {
    int fd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) { die("socket"); }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    int enable = 1;
    int rcvbuf = 4 *1024 * 1024;

    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(enable));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        die("bind");
    }

    int epollfd = epoll_create1(0); 
    if(epollfd < 0) {
        die("epoll instance");
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = fd;
    if(epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        die("epoll_ctl");
    }

    return {epollfd, fd};
}


namespace {

struct MutexQueueSelection {};
struct SpscQueueSelection {};

template <typename QueueSelection>
void runTcpServerWithQueue(const Options& options) {
    constexpr int MaxEvents = 128;
    std::pair<int, int> listener = make_listener(options.port);
    int epollfd = listener.first;

    epoll_event events[MaxEvents];
    uint64_t sink = 0;

    while(true) {
        int nfds = epoll_wait(epollfd, events, MaxEvents, -1);
        if(nfds < 0) {
            if(errno == EINTR) continue;
            die("epoll_wait");
        }
        for(int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            uint8_t received[4096];
            while(true) {
                ssize_t nread = recvfrom(fd, received, sizeof(received), MSG_DONTWAIT, nullptr, nullptr);
                if(nread > 0) {
                    const bool processed = message_framing::processDatagram(
                        received,
                        static_cast<std::size_t>(nread),
                        [&sink](const Envelope& envelope) {
                            handle_event(envelope, sink);
                        }
                    );
                    if (!processed) {
                        std::cerr << "Ignoring malformed UDP datagram of "
                                  << nread << " bytes\n";
                    }
                    continue;
                }

                if(nread == 0) {
                    std::cerr << "Ignoring empty UDP datagram\n";
                    continue;
                }
                if(errno == EINTR) continue;
                if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                die("recvfrom issue");
            }
        }
    }
}

}

void runTcpServer(const Options& options) {
    switch (options.queue) {
        case QueueImplementation::mutex:
            runTcpServerWithQueue<MutexQueueSelection>(options);
            return;
        case QueueImplementation::spsc:
            runTcpServerWithQueue<SpscQueueSelection>(options);
            return;
    }

    throw std::runtime_error("unsupported queue implementation");
}