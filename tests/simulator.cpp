#include "message.hpp"

#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <netdb.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
constexpr std::size_t datagram_budget = 256 * 1024 / sizeof(Message);
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
    bool active = false;
    bool pending = false;
    uint32_t next_id = 0;
    uint64_t random_state = 1;
    uint32_t epoll_events = 0;
    Message message{};
};

struct Statistics {
    uint64_t client_failures = 0;
    uint64_t send_errors = 0;
    uint64_t sent_datagrams = 0;
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
                << "  --clients COUNT   concurrent UDP client sockets (default 100)\n"
                << "  --duration SEC    test duration in seconds (default 10)\n"
                << "  --rate MSG/SEC    total offered datagrams per second; omit for max throughput\n";
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
        usageError("--rate must not exceed 1000000000 datagrams per second");
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
        if (epoll_fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), "epoll_create1");
        }

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
        createClients();
        if (timer_fd_ >= 0) configureRateTimer();

        const auto started = Clock::now();
        const auto deadline = started + std::chrono::seconds(options_.duration_seconds);
        while (Clock::now() < deadline && active_clients_ != 0) {
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
                if (events[i].events & EPOLLERR) {
                    handleSocketError(client);
                    continue;
                }

                if (events[i].events & EPOLLOUT) {
                    if (options_.rate_per_second == 0) {
                        sendBurst(client);
                    } else if (client.pending) {
                        sendPending(client);
                    }
                }
            }
        }

        const auto ended = Clock::now();
        printStatistics(started, ended);
        return statistics_.client_failures == 0 && statistics_.send_errors == 0 ? 0 : 1;
    }

private:
    const Options& options_;
    int epoll_fd_ = -1;
    int timer_fd_ = -1;
    std::vector<Client> clients_;
    std::unordered_map<int, std::size_t> client_by_fd_;
    Statistics statistics_;
    uint64_t active_clients_ = 0;
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

    void createClients() {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM;
        addrinfo* addresses = nullptr;
        const int resolve_error = getaddrinfo(options_.host.c_str(), options_.port.c_str(),
                                              &hints, &addresses);
        if (resolve_error != 0) {
            throw std::runtime_error("getaddrinfo: " + std::string(gai_strerror(resolve_error)));
        }

        clients_.reserve(static_cast<std::size_t>(options_.clients));
        for (uint64_t i = 0; i < options_.clients; ++i) {
            int fd = -1;
            for (addrinfo* address = addresses; address != nullptr; address = address->ai_next) {
                fd = socket(address->ai_family,
                            address->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                            address->ai_protocol);
                if (fd < 0) {
                    continue;
                }
                if (connect(fd, address->ai_addr, address->ai_addrlen) == 0) {
                    break;
                }
                close(fd);
                fd = -1;
            }

            if (fd < 0) {
                ++statistics_.client_failures;
                continue;
            }

            Client client;
            client.fd = fd;
            client.active = true;
            client.random_state = 0x9e3779b97f4a7c15ULL ^ (i + 1);
            if (client.random_state == 0) client.random_state = 1;
            client.epoll_events = options_.rate_per_second == 0 ? EPOLLOUT : 0;

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
            ++active_clients_;
        }

        freeaddrinfo(addresses);
        std::cout << "Requested " << options_.clients << " UDP clients; "
                  << active_clients_ << " ready, " << statistics_.client_failures
                  << " failed to initialize.\n";
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
        client.pending = true;
    }

    bool sendPending(Client& client) {
        while (true) {
            const ssize_t sent = send(client.fd, &client.message, sizeof(client.message), MSG_NOSIGNAL);
            if (sent == static_cast<ssize_t>(sizeof(client.message))) {
                ++statistics_.sent_datagrams;
                statistics_.sent_bytes += static_cast<uint64_t>(sent);
                client.pending = false;
                if (options_.rate_per_second != 0) updateInterest(client, 0);
                return true;
            }
            if (sent < 0 && errno == EINTR) continue;
            if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (options_.rate_per_second != 0) updateInterest(client, EPOLLOUT);
                return true;
            }
            if (sent >= 0) errno = EIO;
            failClient(client, errno);
            return false;
        }
    }

    void sendBurst(Client& client) {
        if (client.pending && !sendPending(client)) return;
        if (client.pending) return;
        for (std::size_t i = 0; i < datagram_budget && client.active; ++i) {
            beginMessage(client);
            if (!sendPending(client)) return;
            if (client.pending) return;
        }
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
            if (!client.active || client.pending) {
                ++statistics_.skipped_rate_slots;
                continue;
            }

            beginMessage(client);
            if (!sendPending(client)) continue;
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

    void handleSocketError(Client& client) {
        int error = 0;
        socklen_t length = sizeof(error);
        if (getsockopt(client.fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0) error = errno;
        failClient(client, error == 0 ? EIO : error);
    }

    void failClient(Client& client, int error) {
        if (!client.active) return;
        ++statistics_.send_errors;
        std::cerr << "UDP client socket failed: " << std::generic_category().message(error) << '\n';
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client.fd, nullptr) < 0 &&
            errno != ENOENT && errno != EBADF) {
            throw std::system_error(errno, std::generic_category(), "epoll_ctl remove client");
        }
        client_by_fd_.erase(client.fd);
        close(client.fd);
        client.fd = -1;
        client.active = false;
        client.pending = false;
        --active_clients_;
    }

    void printStatistics(Clock::time_point started, Clock::time_point ended) const {
        const double seconds = std::chrono::duration<double>(ended - started).count();
        const double datagrams_per_second =
            seconds > 0 ? statistics_.sent_datagrams / seconds : 0;
        const double mib_per_second = seconds > 0
            ? statistics_.sent_bytes / (1024.0 * 1024.0 * seconds)
            : 0;

        std::cout << "\nUDP load test finished after " << seconds << " seconds\n"
                  << "  Datagrams sent: " << statistics_.sent_datagrams << '\n'
                  << "  Bytes accepted by local sockets: " << statistics_.sent_bytes << '\n'
                  << "  Throughput: " << datagrams_per_second << " datagrams/s, "
                  << mib_per_second << " MiB/s\n"
                  << "  Active UDP clients at end: " << active_clients_ << '\n'
                  << "  Client setup failures: " << statistics_.client_failures << '\n'
                  << "  UDP send errors: " << statistics_.send_errors << '\n';
        if (options_.rate_per_second != 0) {
            std::cout << "  Rate-limit slots skipped (busy/failed clients or timer delay): "
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
