#pragma once

#include <cstdint>
#include <type_traits>
#include <new>

struct alignas(64) Message {
    uint64_t seq;
    uint64_t send_ts_ns;
    uint64_t recv_ts_ns;
    uint32_t type;
    uint32_t flags;
    uint8_t payload[32];
};

static_assert(sizeof(Message) == 64, "Message size must be 64 bytes");
static_assert(std::is_trivially_copyable_v<Message>, "Message must be trivially copyable");
