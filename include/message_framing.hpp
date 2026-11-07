#pragma once

#include "message.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace message_framing {

inline constexpr std::size_t frame_size = sizeof(Message);

template <typename Handler>
bool processDatagram(const uint8_t* data, std::size_t size, Handler&& handle) {
    if (data == nullptr || size != frame_size) {
        return false;
    }

    Envelope envelope{};
    std::memcpy(&envelope, data, frame_size);
    handle(envelope);
    return true;
}

}
