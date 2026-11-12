#pragma once

#include <cstdint>
#include <variant>

struct AddMessage { 
    uint64_t orderRef;
    uint32_t price;
    uint32_t qty;
    char side;
};

struct DeleteMessage {
    uint64_t orderRef;
};

struct ExecMessage {
    uint64_t orderRef;
    uint64_t qty;
};

struct Envelope {
    uint64_t sequence;
    uint64_t sourceTsNs;
    uint64_t recieveTsNs;
    uint32_t entityId;
    uint16_t flags;
    std::variant<AddMessage, DeleteMessage, ExecMessage> payload;
};