#pragma once

#include <cstdint>
#include <type_traits>

enum class MessageType : uint8_t {
    metric = 1,
    event = 2,
    log = 3
};

struct Envelope {
    uint8_t version;
    uint64_t sequence;
    MessageType type;
    uint32_t entityId;
    uint64_t sourceTs;
};