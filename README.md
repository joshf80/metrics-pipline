# Event Pipeline

A C++ 23 project exploring how events can be, transported over UDP, validated, and eventually stored.

## Intended event flow

```text
Event source
  → construct an Envelope
  → encode the Envelope into a defined wire format
  → send one UDP datagram
  → receive and validate the datagram
  → decode the Envelope
  → dispatch the event
  → persist or otherwise store it
```


## Envelope model

[`include/message.hpp`](include/message.hpp) defines an `Envelope` containing:

| Field | Meaning |
| --- | --- |
| `sequence` | Sequence number for ordering and gap detection |
| `sourceTsNs` | Event-source timestamp in nanoseconds |
| `recieveTsNs` | Intended receive timestamp in nanoseconds |
| `entityId` | Identifier for the affected entity |
| `flags` | Event metadata flags |
| `payload` | A tagged `std::variant` holding one event |

The current payload alternatives are:

| Event | Fields |
| --- | --- |
| `AddOrder` | Order reference, price, quantity, side |
| `DeleteOrder` | Order reference |
| `ExecOrder` | Order reference, executed quantity |

Pretty similar to financial data pipelines, used for the sake of data ingestion simulation - will eventually broaden scope based on different message formats.


## Current implementation status

- The server creates a nonblocking  socket, registers it with `epoll`, and
  drains available datagrams.
- The handler currently performs no event processing or storage.


## Build and tests

The Clang CMake preset writes build files to `out/build/clang`:

```sh
cmake --preset clang
cmake --build out/build/clang
ctest --test-dir out/build/clang --output-on-failure