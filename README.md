# Real Time Metrics Pipeline

A C++ TCP server for ingesting fixed-size metric messages.

## Architecture

```text
Clients → TCP → epoll-based server → message framing → message handler
```

## Message format

Messages use the `Message` struct defined in `include/message.hpp`.

| Field | Size (bytes) |
|---|---:|
| `time` | 8 |
| `type` | 4 |
| `id` | 4 |
| `payload` | 32 |
| `flags` | 1 |

## Load generator

`metrics-loadgen` opens concurrent nonblocking TCP connections and sends valid
raw `Message` frames to the server. It uses `epoll` rather than one thread per
connection, and reports completed frames, bytes accepted by local sockets,
throughput, and connection failures. The server does not acknowledge messages,
so these figures measure traffic sent, not confirmed server processing.

Start the server in one terminal:

```sh
cmake --build build-clang --target metrics-pipeline metrics-loadgen
./build-clang/metrics-pipeline
```

In another terminal, run a maximum-throughput test:

```sh
./build-clang/metrics-loadgen --host 127.0.0.1 --port 9000 \
  --clients 1000 --duration 60
```

On systems with a low open-file limit, raise the limit in both the server and
load-generator terminals before starting them (for example, `ulimit -n 10000`).
The limit must accommodate the listening/epoll descriptors and every client
socket. Linux also clamps the listen backlog to its `net.core.somaxconn` limit.

To offer a rate-limited total traffic rate instead, add `--rate`:

```sh
./build-clang/metrics-loadgen --clients 1000 --duration 60 --rate 50000
```

Options are `--host`, `--port`, `--clients`, `--duration`, and `--rate`;
`--help` prints defaults and usage. Omitting `--rate` runs at maximum
throughput.]

Run tests from the build directory with:

```sh
ctest --test-dir build-clang --output-on-failure
```
