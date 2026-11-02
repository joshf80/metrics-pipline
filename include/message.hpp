#pragma once

#include <cstdint>
#include <type_traits>
#include <new>

struct Message {
    uint64_t time;
    uint32_t type;
    uint32_t id;
    uint8_t payload[32];
    uint8_t flags;
};