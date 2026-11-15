#include "message_framing.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <span>
#include <vector>

namespace {

bool check(bool condition, const char* message) {
    if (!condition) {
        std::cerr << message << '\n';
        return false;
    }
    return true;
}

std::array<std::byte, message_framing::frame_size> datagramFor(const Envelope& message) {
    std::array<std::byte, message_framing::frame_size> datagram{};
    std::memcpy(datagram.data(), &message, datagram.size());
    return datagram;
}

}

int main() {
    Envelope first{};
    first.sequence = 11;
    Envelope second{};
    second.sequence = 22;

    const auto firstDatagram = datagramFor(first);
    const auto secondDatagram = datagramFor(second);
    const std::span<const std::byte> firstView{firstDatagram};
    const std::span<const std::byte> secondView{secondDatagram};
    std::vector<uint32_t> processed;
    const auto record = [&processed](const Envelope& message) {
        processed.push_back(message.sequence);
    };

    if (!check(message_framing::processDatagram(firstView, record), "a complete UDP datagram must be accepted")) {
        return 1;
    }
    if (!check(message_framing::processDatagram(secondView, record), "a second complete UDP datagram must be accepted")) {
        return 1;
    }
    if (!check(processed == std::vector<uint32_t>{11, 22}, "separate UDP datagrams must be processed as separate messages in order")) {
        return 1;
    }

    std::vector<uint32_t> perClient;
    const auto recordPerClient = [&perClient](const Envelope& message) {
        perClient.push_back(message.sequence);
    };

    if (!check(message_framing::processDatagram(firstView, recordPerClient) &&  message_framing::processDatagram(secondView, recordPerClient), "messages from independent UDP clients must be accepted")) {
        return 1;
    }

    if (!check(perClient == std::vector<uint32_t>{11, 22}, "messages from independent UDP clients must remain independent")) {
        return 1;
    }

    if (!check(!message_framing::processDatagram(firstView.first(firstView.size() - 1), record), "a truncated UDP datagram must be rejected")) {
        return 1;
    }

    std::array<std::byte, message_framing::frame_size * 2> combinedDatagram{};
    std::memcpy(combinedDatagram.data(), firstDatagram.data(), firstDatagram.size());
    std::memcpy(combinedDatagram.data() + firstDatagram.size(), secondDatagram.data(), secondDatagram.size());

    if (!check(!message_framing::processDatagram(std::span<const std::byte>{combinedDatagram}, record), "a datagram containing multiple messages must be rejected")) {
        return 1;
    }

    if (!check(processed == std::vector<uint32_t>{11, 22},
               "invalid UDP datagrams must not be processed")) {
        return 1;
    }

    return 0;
}
