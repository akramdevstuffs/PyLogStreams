# PyLogStreams 🌀

A distributed, Redis-style **pub/sub system** with **Kafka-inspired persistent logs** — built for chat apps and real-time systems.

---

## 🌟 Highlights

* 🔁 **Pub/Sub API** — Simple producer/consumer model
* 💾 **Persistent logs** — Kafka-like segment files per topic
* ⚡ **Memory-mapped I/O** — High-throughput read/write
* 🧹 **Background compaction & file cleanup**
* 🧠 **Cluster-ready design** — Planned replication & partition assignment
* 🧩 **Modular storage engine** — Swap local disk for distributed backend
* 🗣️ Ideal for chat apps, analytics, and real-time event streaming

---

## 🚀 Planned Features

* [x] Persistent local log with append-only segments
* [x] Multiprocessing producer/consumer model
* [x] Background log cleaner and file remover
* [ ] Distributed cluster coordination
* [ ] Replication and leader election
* [ ] Stream rebalancing
* [ ] REST and gRPC APIs

---

## ⚙️ Architecture Overview

Each topic maintains an append-only commit log segmented across files.
Consumers read sequentially and track offsets.
Future versions will use **Raft or custom gossip-based coordination** for cluster management.

---

## 🧪 How to Use

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/akramdevstuffs/PyLogStreams
cd PyLogStreams
```

### 2️⃣ Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows
```

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Run the Broker

By default, the broker runs on **localhost:1234**.

```bash
python src/PyLogStreams/broker.py
```

You can change the `HOST` and `PORT` values in
`src/PyLogStreams/broker.py` to customize the server address.

---

### 5️⃣ Use the Client

You can either implement your own client logic or use the built-in implementation:

```
client/client.py
```

Example:

```python
from client.client import Client

client = Client("localhost", 1234)

# Register and receive unique client ID
cid = client.register()
print("Registered with ID:", cid)

# Produce a message
client.produce("news", "Breaking: PyLogStreams is live!")

# Subscribe to a topic
client.subscribe("news")

# Consume messages
msg = client.consume()
print("Received:", msg)
```

---

### 6️⃣ Benchmarking

To benchmark or stress-test:

```bash
python tests/test_minikafka.py
```

You can tweak producer count, message sizes, and other parameters for custom testing.

---

## 🧰 Default Configuration

| Setting                  | Default     |
| ------------------------ | ----------- |
| Host                     | `localhost` |
| Port                     | `1234`      |
| Heartbeat Interval       | `30s`       |
| Outgoing Buffer Capacity | `1000`      |
| Checksum                 | Enabled     |

---