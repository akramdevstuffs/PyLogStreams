# Devlog

## [2025-10-27]

- Found message dropping and fake offset commit
- After a client disconnect, his coroutine is keep running and sending messages
- Decided to switch back from asyncio to selectors
  - managing acks and client state is hard asyncio
  - Latency is high in asyncio
  - Number of concurrent connections is very low. Start to struggle at 50 concurrent connection and throughput drops.
  - Even after implementing heartbeat system for checking client state and handling coroutines better to fix dead client issue.
  - Concurrency still remains problem. Too much coroutines running now.

## [2025-10-28]

- Decided to move away from asyncio
- By implementing selectors for main event loop
- Multiplexing for main event for network I/O + worker thread for disk I/O
- Connected with non-blocking queue

## [2025-11-01]

- implementation of selectors is put on hold right now
- After uvloop and posix_advice or os hinting concurrency got a bit better
- Currently handles 200+ client if running it on my machine
- With throughput of 20k to 30k for 1KB message in my machine
- Throughput 63k send and receiving or 126k msgs/s of 1KB msg in total in macbook m2
- Implemented heartbeat system for detecting disconnected clients.
- Implemented custom offset setting by consumer. Consumer can update their offset by doing
      - SET [TOPIC] [OFFSET]
      - This can help prevent the previous problem of offset getting updated after client is disconnected because of coroutine not terminating