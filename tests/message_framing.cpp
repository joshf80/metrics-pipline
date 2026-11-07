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

std::array<uint8_t, message_framing::frame_size> datagramFor(const Message& message) {
    std::array<uint8_t, message_framing::frame_size> datagram{};
    std::memcpy(datagram.data(), &message, datagram.size());
    return datagram;
}

} // namespace

int main() {
    Message first{};
    first.id = 11;
    Message second{};
    second.id = 22;

    const auto firstDatagram = datagramFor(first);
    const auto secondDatagram = datagramFor(second);
    std::vector<uint32_t> processed;
    const auto record = [&processed](const Message& message) {
        processed.push_back(message.id);
    };

    if (!check(message_framing::processDatagram(firstDatagram.data(), firstDatagram.size(), record),
               "a complete UDP datagram must be accepted")) {
        return 1;
    }
    if (!check(message_framing::processDatagram(secondDatagram.data(), secondDatagram.size(), record),
               "a second complete UDP datagram must be accepted")) {
        return 1;
    }
    if (!check(processed == std::vector<uint32_t>{11, 22},
               "separate UDP datagrams must be processed as separate messages in order")) {
        return 1;
    }

    std::vector<uint32_t> perClient;
    const auto recordPerClient = [&perClient](const Message& message) {
        perClient.push_back(message.id);
    };

    if (!check(message_framing::processDatagram(firstDatagram.data(), firstDatagram.size(),
                                                recordPerClient) &&
                   message_framing::processDatagram(secondDatagram.data(), secondDatagram.size(),
                                                    recordPerClient),
               "messages from independent UDP clients must be accepted")) {
        return 1;
    }
    if (!check(perClient == std::vector<uint32_t>{11, 22},
               "messages from independent UDP clients must remain independent")) {
        return 1;
    }

    if (!check(!message_framing::processDatagram(firstDatagram.data(), firstDatagram.size() - 1,
                                                 record),
               "a truncated UDP datagram must be rejected")) {
        return 1;
    }
    std::array<uint8_t, message_framing::frame_size * 2> combinedDatagram{};
    std::memcpy(combinedDatagram.data(), firstDatagram.data(), firstDatagram.size());
    std::memcpy(combinedDatagram.data() + firstDatagram.size(),
                secondDatagram.data(), secondDatagram.size());
    if (!check(!message_framing::processDatagram(combinedDatagram.data(), combinedDatagram.size(),
                                                 record),
               "a datagram containing multiple messages must be rejected")) {
        return 1;
    }
    if (!check(processed == std::vector<uint32_t>{11, 22},
               "invalid UDP datagrams must not be processed")) {
        return 1;
    }

    return 0;
}
