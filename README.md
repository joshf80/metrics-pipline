 ## Real Time Metrics Pipeline

A latency sensitive ingestion pipeline built in C++20. It receives events over TCP, normalizes them into a common schema, hands them to worker threads through bounded queues, with end to end measurements.


## Architecture
 
```
client --TCP--> IO thread --push--> queue[i] --pop--> worker[i]
                (epoll, framing,                      (process, record latency)
                 recv timestamp)
```

### Wire format
 
Each message is exactly 64 bytes (one cache line):
 
| Field | Size | Notes |
|---|---:|---|
| seq | 8 | Sender's sequence number |
| send_ts_ns | 8 | Stamped by the sender just before `send()` |
| recv_ts_ns | 8 | Filled in by the server after framing (sender writes 0) |
| type | 4 | |
| flags | 4 | |
| payload | 32 | |

### Configuration

The server parses `config/options.json` once at startup, before calling
`runTcpServer()`. Invalid or unreadable configuration stops startup with an
error; JSON parsing is not performed in the server event loop.