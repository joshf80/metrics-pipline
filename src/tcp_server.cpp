#include "tcp_server.hpp"
#include "message.hpp"
#include "common.hpp"

#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <unistd.h>
#include <string_view>
#include <unordered_map>
#include <vector>
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


namespace {

struct MutexQueueSelection {};
struct SpscQueueSelection {};

template <typename QueueSelection>
void runTcpServerWithQueue(const Options& options) {
    std::pair<int, int> listener = make_listener(options.port);
    int epollfd = listener.first;
    int listenfd = listener.second;

    epoll_event events[128];
    std::unordered_map<int, std::vector<uint8_t>> client_buffers;
    uint64_t sink = 0;

    while(true) {
        int nfds = epoll_wait(epollfd, events, 128, -1);
        if(nfds < 0) {
            if(errno == EINTR) continue;
            die("epoll_wait");
        }

        for(int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;
            if(fd == listenfd) {
                while(true) {
                    sockaddr_in clientAddr{};
                    socklen_t addrlen = sizeof(clientAddr);
                    int clientfd = accept4(
                        listenfd,
                        reinterpret_cast<sockaddr*>(&clientAddr),
                        &addrlen,
                        SOCK_NONBLOCK | SOCK_CLOEXEC
                    );

                    if(clientfd < 0) {
                        if(errno == EINTR) continue;
                        if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                        die("accept4");
                    }

                    epoll_event clientEvent{};
                    clientEvent.events = EPOLLIN | EPOLLRDHUP;
                    clientEvent.data.fd = clientfd;
                    if(epoll_ctl(epollfd, EPOLL_CTL_ADD, clientfd, &clientEvent) < 0) {
                        close(clientfd);
                        die("epoll_ctl client");
                    }
                    client_buffers.emplace(clientfd, std::vector<uint8_t>{});
                }
                continue;
            }

            auto clientIt = client_buffers.find(fd);
            if(clientIt == client_buffers.end()) continue;

            bool closeClient = false;
            uint8_t received[4096];
            while(true) {
                ssize_t nread = recv(fd, received, sizeof(received), 0);
                if(nread > 0) {
                    auto& buffer = clientIt->second;
                    buffer.insert(buffer.end(), received, received + nread);

                    size_t consumed = 0;
                    while(buffer.size() - consumed >= sizeof(Message)) {
                        Message message{};
                        std::memcpy(&message, buffer.data() + consumed, sizeof(message));
                        handle_event(message, sink);
                        consumed += sizeof(message);
                    }
                    if(consumed > 0) {
                        buffer.erase(buffer.begin(), buffer.begin() + consumed);
                    }
                    continue;
                }

                if(nread == 0) {
                    closeClient = true;
                    break;
                }
                if(errno == EINTR) continue;
                if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                perror("recv");
                closeClient = true;
                break;
            }

            if(events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                closeClient = true;
            }
            if(closeClient) {
                if(epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, nullptr) < 0 &&
                   errno != ENOENT && errno != EBADF) {
                    perror("epoll_ctl remove client");
                }
                close(fd);
                client_buffers.erase(clientIt);
            }
        }
    }
}

} // namespace

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