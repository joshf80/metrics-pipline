 ## Real Time Metrics Pipeline

A low-latency time series ingestion pipeline in C++20. It receives events over TCP, normalizes them into a common schema, hands them to worker threads through bounded queues, and measures its own latency while doing so.


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
 
Known simplifications: host byte order, no magic number or checksum, and the in-memory struct is the wire format. Fine for two processes on one machine, not for a real protocol.