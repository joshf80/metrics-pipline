#pragma once

#include "message.hpp"

#include <cstddef>
#include <cstring>
#include <span>

namespace message_framing {

inline constexpr std::size_t frame_size = sizeof(Envelope);

template <typename Handler>
bool processDatagram(std::span<const std::byte> view, Handler&& handle) {
    if (view.data() == nullptr || view.size() != frame_size) {
        return false;
    }

    Envelope envelope{};
    std::memcpy(&envelope, view.data(), frame_size);
    handle(envelope);
    return true;
}

}