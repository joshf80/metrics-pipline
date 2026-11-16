# Event Pipeline

A C++23 project exploring UDP event ingestion. The current server receives
datagrams and validates their framing; event processing and storage are not
implemented yet.

## Message format

[`include/message.hpp`](include/message.hpp) defines the fixed-size `Envelope`:

| Field | Type | Meaning |
| --- | --- | --- |
| `version` | `uint8_t` | Message format version |
| `sequence` | `uint64_t` | Sequence number |
| `type` | `MessageType` | Category: `metric`, `event`, or `log` |
| `entityId` | `uint32_t` | Identifier for the affected entity  |
| `sourceTs` | `uint64_t` | Source timestamp in nanoseconds |

[`include/message_framing.hpp`](include/message_framing.hpp) defines the current
datagram contract: a datagram is accepted only when its size exactly matches
`sizeof(Envelope)`. Accepted bytes are copied into an `Envelope` and passed to
the handler. Truncated, oversized, or empty datagrams are rejected.


## Current implementation

- The server creates a nonblocking UDP socket, registers it with `epoll`, and
  drains available datagrams.
- Valid datagrams are decoded as one `Envelope`
- The handler currently performs no event processing or storage.

## Build and tests

The Clang CMake preset writes build files to `out/build/clang`:

```sh
cmake --preset clang
cmake --build out/build/clang
ctest --test-dir out/build/clang --output-on-failure
```
