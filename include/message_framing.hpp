#pragma once

#include "message.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace message_framing {

inline constexpr std::size_t frame_size = sizeof(Message);

class Buffer {
public:
    template <typename Handler>
    void append(const uint8_t* data, std::size_t size, Handler&& handle) {
        if (size > 0) {
            bytes_.insert(bytes_.end(), data, data + size);
        }

        std::size_t consumed = 0;
        while (bytes_.size() - consumed >= frame_size) {
            Message message{};
            std::memcpy(&message, bytes_.data() + consumed, frame_size);
            handle(message);
            consumed += frame_size;
        }

        if (consumed > 0) {
            bytes_.erase(bytes_.begin(), bytes_.begin() + consumed);
        }
    }

private:
    std::vector<uint8_t> bytes_;
};

}
