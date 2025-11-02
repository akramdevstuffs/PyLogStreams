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

### Status
- Implementation of `selectors` is currently on hold.
- After enabling `uvloop` and applying `posix_advice` (OS hinting), concurrency improved.
- The system now handles over **200+ clients** on my machine.

### Performance
- Achieves **20k–30k msgs/s** throughput for **1 KB messages** on my machine.
- Achieved **~63k send + receive ops (≈126k msgs/s)** for **1 KB messages** on a MacBook M2.

### Reliability
- Implemented a **heartbeat system** to detect disconnected clients.
- Added a **message checksum** mechanism to maintain integrity.  
  - PUB message frame format: `[4B length][message][4B checksum]`.

### Protocol
- Added support for **manual consumer offset setting**:
  - Command: `SET [TOPIC] [OFFSET]`
  - This prevents the issue of offsets being updated after a client disconnects due to coroutine leaks.
- Standardized all command names to **3 bytes**:
  - `PUB`, `SUB`, `SET`, `CID` (was `ID`), `PNG` (was `PING`), `POG` (was `PONG`).

### Bug Report — Client Offset Issue
- **Symptom:** Server crashes when reloading saved client offsets.
- **Steps to reproduce:**
  1. Produce and consume 50k messages multiple times.
  2. Restart the server (loads saved offsets).
  3. Produce and consume another 50k messages → crash occurs.
- **Observation:** During tests, the offset was always reset to `0` before consuming to ensure consuming starts from the oldest message.
- **Possible cause:** Incorrect offset restoration or race condition after reload.

### Next Steps
- Investigate and fix the offset reload crash.
- Resume `selectors` implementation after stability is confirmed.

## [2025-11-02]

* Added **message integrity checksum**, which currently verifies only the message body (not the command).
* When checksum is enabled, the producer now sends messages as:
  `[4 bytes message length][message][4 bytes hash]`
* On the broker side:

  * The checksum is verified and stored with the message on disk.
  * When a consumer reads it, the checksum is verified again before delivery.
* Using `zlib.crc32` for hashing (implemented in `src/PyLogStreams/utility.py` as `verify_checksum`).
* Updated `append_message` in `src/PyLogStreams/log_manager.py` to accept an optional checksum input.
  It verifies the checksum and returns a result code:

  * `0` → success
  * `1` → invalid message
  * `2` → corrupted data
  * `3` → invalid hash or hash length ≠ 4 bytes
* **Next step:** implement an ACK system so the broker can send result codes to clients, allowing retry or error handling on failure.

---

* Added **Client implementation** in `client/client.py`.
  Provides a `Client` class for interacting with the broker.

  * Constructor: `Client(host, port)` — connects to the broker on init
  * `register()` — register a new client and get client ID
  * `login(id)` — log in existing client
  * `subscribe(topic)` — subscribe to topic
  * `produce(topic, message)` — publish a message
  * `consume()` — block until a message arrives and return it
  * `reset_offset_oldest(topic)` — reset topic offset to 0 on server
  * `reset_offset_latest(topic)` - reset topic offset to lastest on server
* The client runs background **writer** and **ping** threads for concurrent and safe message handling.

---

* Added **`SET TOPIC offset -1`** support in `broker.py`, allowing clients to reset a topic’s offset to the **latest message**.

---

* Implemented **Locust-based stress testing** in `tests/locustfile.py`.

  * Supports sustained load testing up to **12 MB/s**.
  * Measures **average (50th percentile)** and **95th percentile** latencies.
  * Automatically adjusts the number of producer and consumer users.
  * Generates detailed **throughput and latency graphs** for performance analysis.