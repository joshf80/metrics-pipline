#pragma once

#include "message.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <cstring>

namespace message_framing {

inline constexpr std::size_t frame_size = sizeof(Envelope);

template <typename Handler>
bool processDatagram(const std::span<std::byte> view, std::size_t size, Handler&& handle) {
    if (view == nullptr || size != frame_size) {
        return false;
    }

    Envelope envelope{};
    std::memcpy(&envelope, view, frame_size);
    handle(envelope);
    return true;
}

}