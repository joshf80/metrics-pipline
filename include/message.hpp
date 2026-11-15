#pragma once

#include <cstdint>
#include <type_traits>

struct Envelope {
    uint64_t sequence;
    uint64_t sourceTs;
    uint32_t entityId;
    uint16_t flags;
    uint64_t payload;
};

static_assert(std::is_trivially_copyable_v<Envelope>, "Envelope must be trivially copyable");