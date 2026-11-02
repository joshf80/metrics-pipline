#include "message.hpp"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <netdb.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
constexpr std::size_t write_budget = 256 * 1024;
constexpr uint64_t max_timer_expirations_per_wakeup = 10'000;

struct Options {
    std::string host = "127.0.0.1";
    std::string port = "9000";
    uint64_t clients = 100;
    uint64_t duration_seconds = 10;
    uint64_t rate_per_second = 0;
};

struct Client {
    int fd = -1;
    bool connecting = true;
    bool connected = false;
    bool pending = false;
    std::size_t offset = 0;
    uint32_t next_id = 0;
    uint64_t random_state = 1;
    uint32_t epoll_events = 0;
    Message message{};
};

struct Statistics {
    uint64_t connect_failures = 0;
    uint64_t connection_errors = 0;
    uint64_t completed_frames = 0;
    uint64_t sent_bytes = 0;
    uint64_t skipped_rate_slots = 0;
};

[[noreturn]] void usageError(const std::string& message) {
    throw std::invalid_argument(message + "\nUse --help for usage.");
}

uint64_t parseUnsigned(std::string_view text, std::string_view option) {
    uint64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        usageError("Invalid value for " + std::string(option) + ": " + std::string(text));
    }
    return value;
}

Options parseOptions(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view argument(argv[i]);
        if (argument == "--help" || argument == "-h") {
            std::cout
                << "Usage: metrics-loadgen [options]\n"
                << "  --host HOST       server hostname or IP (default 127.0.0.1)\n"
                << "  --port PORT       server port (default 9000)\n"
                << "  --clients COUNT   concurrent TCP connections (default 100)\n"
                << "  --duration SEC    test duration in seconds (default 10)\n"
                << "  --rate MSG/SEC    total offered messages per second; omit for max throughput\n";
            std::exit(0);
        }

        if (i + 1 >= argc) {
            usageError("Missing value after " + std::string(argument));
        }
        const std::string_view value(argv[++i]);
        if (argument == "--host") {
            options.host = value;
        } else if (argument == "--port") {
            const uint64_t port = parseUnsigned(value, argument);
            if (port == 0 || port > 65535) usageError("--port must be between 1 and 65535");
            options.port = value;
        } else if (argument == "--clients") {
            options.clients = parseUnsigned(value, argument);
        } else if (argument == "--duration") {
            options.duration_seconds = parseUnsigned(value, argument);
        } else if (argument == "--rate") {
            options.rate_per_second = parseUnsigned(value, argument);
        } else {
            usageError("Unknown option: " + std::string(argument));
        }
    }

    if (options.host.empty()) usageError("--host must not be empty");
    if (options.clients == 0 || options.clients > 1'000'000) {
        usageError("--clients must be between 1 and 1000000");
    }
    if (options.duration_seconds == 0 || options.duration_seconds > 86400) {
        usageError("--duration must be between 1 and 86400 seconds");
    }
    if (options.rate_per_second > 1'000'000'000) {
        usageError("--rate must not exceed 1000000000 messages per second");
    }
    return options;
}

uint64_t nextRandom(uint64_t& state) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}

class LoadGenerator {
public:
    explicit LoadGenerator(const Options& options) : options_(options) {
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0) throw std::system_error(errno, std::generic_category(), "epoll_create1");

        if (options_.rate_per_second != 0) {
            timer_fd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
            if (timer_fd_ < 0) {
                const int error = errno;
                close(epoll_fd_);
                epoll_fd_ = -1;
                throw std::system_error(error, std::generic_category(), "timerfd_create");
            }
        }
    }

    ~LoadGenerator() {
        for (const Client& client : clients_) {
            if (client.fd >= 0) close(client.fd);
        }
        if (timer_fd_ >= 0) close(timer_fd_);
        if (epoll_fd_ >= 0) close(epoll_fd_);
    }

    int run() {
        connectClients();
        if (timer_fd_ >= 0) configureRateTimer();
        const auto started = Clock::now();
        const auto deadline = started + std::chrono::seconds(options_.duration_seconds);

        while (Clock::now() < deadline) {
            if (active_connections_ == 0 && pending_connections_ == 0) break;

            epoll_event events[256];
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - Clock::now()
            ).count();
            const int timeout_ms = static_cast<int>(std::clamp<int64_t>(remaining, 0, 1000));
            const int count = epoll_wait(epoll_fd_, events, 256, timeout_ms);
            if (count < 0) {
                if (errno == EINTR) continue;
                throw std::system_error(errno, std::generic_category(), "epoll_wait");
            }

            for (int i = 0; i < count; ++i) {
                if (Clock::now() >= deadline) break;
                const int fd = events[i].data.fd;
                if (fd == timer_fd_) {
                    handleRateTimer();
                    continue;
                }

                const auto found = client_by_fd_.find(fd);
                if (found == client_by_fd_.end()) continue;
                Client& client = clients_[found->second];
                const uint32_t flags = events[i].events;

                if (client.connecting && (flags & (EPOLLOUT | EPOLLERR | EPOLLHUP))) {
                    finishConnect(client);
                }
                if (!client.connected) continue;

                if (flags & EPOLLOUT) {
                    if (!flushClient(client)) {
                        disconnect(client, true);
                        continue;
                    }
                }
                if (flags & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    disconnect(client, true);
                }
            }
        }

        const auto ended = Clock::now();
        printStatistics(started, ended);
        return statistics_.connect_failures == 0 && statistics_.connection_errors == 0 ? 0 : 1;
    }

private:
    const Options& options_;
    int epoll_fd_ = -1;
    int timer_fd_ = -1;
    std::vector<Client> clients_;
    std::unordered_map<int, std::size_t> client_by_fd_;
    Statistics statistics_;
    uint64_t active_connections_ = 0;
    uint64_t pending_connections_ = 0;
    std::size_t next_rate_client_ = 0;

    void configureRateTimer() {
        const uint64_t interval_ns = std::max<uint64_t>(1, 1'000'000'000 / options_.rate_per_second);
        itimerspec timer{};
        timer.it_value.tv_sec = static_cast<time_t>(interval_ns / 1'000'000'000);
        timer.it_value.tv_nsec = static_cast<long>(interval_ns % 1'000'000'000);
        timer.it_interval = timer.it_value;
        if (timerfd_settime(timer_fd_, 0, &timer, nullptr) < 0) {
            throw std::system_error(errno, std::generic_category(), "timerfd_settime");
        }
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = timer_fd_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &event) < 0) {
            throw std::system_error(errno, std::generic_category(), "epoll_ctl timer");
        }
    }

    void connectClients() {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* addresses = nullptr;
        const int resolve_error = getaddrinfo(options_.host.c_str(), options_.port.c_str(),
                                              &hints, &addresses);
        if (resolve_error != 0) {
            throw std::runtime_error("getaddrinfo: " + std::string(gai_strerror(resolve_error)));
        }

        clients_.reserve(static_cast<std::size_t>(options_.clients));
        for (uint64_t i = 0; i < options_.clients; ++i) {
            int fd = -1;
            int connect_error = 0;
            for (addrinfo* address = addresses; address != nullptr; address = address->ai_next) {
                fd = socket(address->ai_family, address->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                            address->ai_protocol);
                if (fd < 0) {
                    connect_error = errno;
                    continue;
                }
                if (connect(fd, address->ai_addr, address->ai_addrlen) == 0) {
                    connect_error = 0;
                    break;
                }
                if (errno == EINPROGRESS) {
                    connect_error = EINPROGRESS;
                    break;
                }
                connect_error = errno;
                close(fd);
                fd = -1;
            }

            if (fd < 0) {
                ++statistics_.connect_failures;
                continue;
            }

            Client client;
            client.fd = fd;
            client.random_state = 0x9e3779b97f4a7c15ULL ^ (i + 1);
            if (client.random_state == 0) client.random_state = 1;
            client.connecting = connect_error == EINPROGRESS;
            client.connected = connect_error == 0;
            client.epoll_events = EPOLLRDHUP;
            if (client.connecting || options_.rate_per_second == 0) {
                client.epoll_events |= EPOLLOUT;
            }

            epoll_event event{};
            event.events = client.epoll_events;
            event.data.fd = fd;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
                const int error = errno;
                close(fd);
                freeaddrinfo(addresses);
                throw std::system_error(error, std::generic_category(), "epoll_ctl client");
            }

            const std::size_t index = clients_.size();
            clients_.push_back(client);
            client_by_fd_.emplace(fd, index);
            if (client.connecting) {
                ++pending_connections_;
            } else {
                ++active_connections_;
                if (options_.rate_per_second == 0) beginMessage(clients_.back());
            }
        }

        freeaddrinfo(addresses);
        std::cout << "Requested " << options_.clients << " clients; "
                  << active_connections_ << " connected, " << pending_connections_
                  << " connecting, " << statistics_.connect_failures << " failed immediately.\n";
    }

    void finishConnect(Client& client) {
        int error = 0;
        socklen_t length = sizeof(error);
        if (getsockopt(client.fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0) error = errno;
        client.connecting = false;
        --pending_connections_;
        if (error != 0) {
            ++statistics_.connect_failures;
            disconnect(client, false);
            return;
        }

        client.connected = true;
        ++active_connections_;
        if (options_.rate_per_second == 0) beginMessage(client);
        else updateInterest(client, EPOLLRDHUP);
    }

    void beginMessage(Client& client) {
        client.message = Message{};
        client.message.time = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now().time_since_epoch()
            ).count()
        );
        client.message.type = 1;
        client.message.id = client.next_id++;
        for (std::size_t offset = 0; offset < sizeof(client.message.payload); offset += sizeof(uint64_t)) {
            const uint64_t random_value = nextRandom(client.random_state);
            std::memcpy(client.message.payload + offset, &random_value, sizeof(random_value));
        }
        client.message.flags = static_cast<uint8_t>(client.message.id & 0xff);
        client.offset = 0;
        client.pending = true;
    }

    bool flushClient(Client& client) {
        std::size_t written_this_event = 0;
        while (client.pending && written_this_event < write_budget) {
            const auto* bytes = reinterpret_cast<const uint8_t*>(&client.message);
            const ssize_t sent = send(client.fd, bytes + client.offset,
                                      sizeof(client.message) - client.offset, MSG_NOSIGNAL);
            if (sent > 0) {
                const auto count = static_cast<std::size_t>(sent);
                client.offset += count;
                written_this_event += count;
                statistics_.sent_bytes += count;
                if (client.offset == sizeof(client.message)) {
                    ++statistics_.completed_frames;
                    client.pending = false;
                    client.offset = 0;
                    if (options_.rate_per_second == 0) beginMessage(client);
                }
                continue;
            }
            if (sent < 0 && errno == EINTR) continue;
            if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
            if (sent == 0) errno = EPIPE;
            return false;
        }

        if (options_.rate_per_second != 0 && !client.pending) {
            updateInterest(client, EPOLLRDHUP);
        }
        return true;
    }

    void handleRateTimer() {
        uint64_t expirations = 0;
        const ssize_t bytes = read(timer_fd_, &expirations, sizeof(expirations));
        if (bytes < 0 && (errno == EAGAIN || errno == EINTR)) return;
        if (bytes != sizeof(expirations)) {
            throw std::system_error(bytes < 0 ? errno : EIO,
                                    std::generic_category(), "read rate timer");
        }

        const uint64_t process_count = std::min(expirations, max_timer_expirations_per_wakeup);
        statistics_.skipped_rate_slots += expirations - process_count;
        for (uint64_t i = 0; i < process_count; ++i) {
            if (clients_.empty()) {
                ++statistics_.skipped_rate_slots;
                continue;
            }

            Client& client = clients_[next_rate_client_];
            next_rate_client_ = (next_rate_client_ + 1) % clients_.size();
            if (!client.connected || client.pending) {
                ++statistics_.skipped_rate_slots;
                continue;
            }

            beginMessage(client);
            updateInterest(client, EPOLLRDHUP | EPOLLOUT);
            if (!flushClient(client)) disconnect(client, true);
        }
    }

    void updateInterest(Client& client, uint32_t events) {
        if (client.epoll_events == events) return;
        epoll_event event{};
        event.events = events;
        event.data.fd = client.fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client.fd, &event) < 0) {
            throw std::system_error(errno, std::generic_category(), "epoll_ctl modify client");
        }
        client.epoll_events = events;
    }

    void disconnect(Client& client, bool count_error) {
        if (client.fd < 0) return;
        if (count_error) ++statistics_.connection_errors;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client.fd, nullptr) < 0 &&
            errno != ENOENT && errno != EBADF) {
            throw std::system_error(errno, std::generic_category(), "epoll_ctl remove client");
        }
        client_by_fd_.erase(client.fd);
        close(client.fd);
        client.fd = -1;
        if (client.connecting) {
            client.connecting = false;
            --pending_connections_;
        }
        if (client.connected) {
            client.connected = false;
            --active_connections_;
        }
    }

    void printStatistics(Clock::time_point started, Clock::time_point ended) const {
        const double seconds = std::chrono::duration<double>(ended - started).count();
        const double frames_per_second = seconds > 0 ? statistics_.completed_frames / seconds : 0;
        const double mib_per_second = seconds > 0
            ? statistics_.sent_bytes / (1024.0 * 1024.0 * seconds)
            : 0;

        std::cout << "\nLoad test finished after " << seconds << " seconds\n"
                  << "  Frames fully sent: " << statistics_.completed_frames << '\n'
                  << "  Bytes accepted by local sockets: " << statistics_.sent_bytes << '\n'
                  << "  Throughput: " << frames_per_second << " frames/s, "
                  << mib_per_second << " MiB/s\n"
                  << "  Active connections at end: " << active_connections_ << '\n'
                  << "  Connection failures: " << statistics_.connect_failures << '\n'
                  << "  Connections closed/errors: " << statistics_.connection_errors << '\n';
        if (options_.rate_per_second != 0) {
            std::cout << "  Rate-limit slots skipped (busy/disconnected clients or timer delay): "
                      << statistics_.skipped_rate_slots << '\n';
        }
    }
};

} // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parseOptions(argc, argv);
        LoadGenerator generator(options);
        return generator.run();
    } catch (const std::exception& error) {
        std::cerr << "Load generator failed: " << error.what() << '\n';
        return 2;
    }
}
