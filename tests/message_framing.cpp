#include "message_framing.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

namespace {

bool check(bool condition, const char* message) {
    if (!condition) {
        std::cerr << message << '\n';
        return false;
    }
    return true;
}

std::array<uint8_t, message_framing::frame_size> bytesFor(const Message& message) {
    std::array<uint8_t, message_framing::frame_size> bytes{};
    std::memcpy(bytes.data(), &message, bytes.size());
    return bytes;
}

} // namespace

int main() {
    Message first{};
    first.id = 11;
    Message second{};
    second.id = 22;

    const auto firstBytes = bytesFor(first);
    const auto secondBytes = bytesFor(second);
    message_framing::Buffer buffer;
    std::vector<uint32_t> processed;
    const auto record = [&processed](const Message& message) {
        processed.push_back(message.id);
    };

    buffer.append(firstBytes.data(), 3, record);
    if (!check(processed.empty(), "fragment must not be processed before complete")) {
        return 1;
    }

    std::array<uint8_t, message_framing::frame_size - 3 + message_framing::frame_size> combined{};
    std::memcpy(combined.data(), firstBytes.data() + 3, firstBytes.size() - 3);
    std::memcpy(
        combined.data() + firstBytes.size() - 3,
        secondBytes.data(),
        secondBytes.size()
    );
    buffer.append(combined.data(), combined.size(), record);
    if (!check(processed == std::vector<uint32_t>{11, 22},
               "fragmented and coalesced frames must be processed in order")) {
        return 1;
    }

    message_framing::Buffer firstClient;
    message_framing::Buffer secondClient;
    std::vector<uint32_t> perClient;
    const auto recordPerClient = [&perClient](const Message& message) {
        perClient.push_back(message.id);
    };
    firstClient.append(firstBytes.data(), firstBytes.size() / 2, recordPerClient);
    secondClient.append(secondBytes.data(), secondBytes.size(), recordPerClient);
    firstClient.append(
        firstBytes.data() + firstBytes.size() / 2,
        firstBytes.size() - firstBytes.size() / 2,
        recordPerClient
    );
    if (!check(perClient == std::vector<uint32_t>{22, 11},
               "partial frames must remain isolated to their client")) {
        return 1;
    }

    {
        message_framing::Buffer disconnectedClient;
        disconnectedClient.append(firstBytes.data(), firstBytes.size() - 1, record);
    }
    if (!check(processed == std::vector<uint32_t>{11, 22},
               "incomplete frame must not be processed when its buffer is discarded")) {
        return 1;
    }

    return 0;
}
