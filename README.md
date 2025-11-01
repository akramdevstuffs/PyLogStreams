# PyLogStreams 🌀

A distributed, Redis-style pub/sub system with Kafka-inspired persistent logs — built for chat apps and real-time systems.

## 🌟 Highlights

- 🔁 **Pub/Sub API** — Simple producer/consumer model
- 💾 **Persistent logs** — Kafka-like segment files per topic
- ⚡ **Memory-mapped I/O** — High-throughput read/write
- 🧹 **Background compaction & file cleanup**
- 🧠 **Cluster-ready design** — Planned replication & partition assignment
- 🧩 **Modular storage engine** — Swap local disk for distributed backend
- 🗣️ Ideal for chat apps, analytics, and real-time event streaming

---

## 🚀 Planned Features

- [x] Persistent local log with append-only segments
- [x] Multiprocessing producer/consumer model
- [x] Background log cleaner and file remover
- [ ] Distributed cluster coordination
- [ ] Replication and leader election
- [ ] Stream rebalancing
- [ ] REST and gRPC APIs

---

## ⚙️ Architecture Overview

Each topic maintains an append-only commit log segmented across files.  
Consumers read sequentially and track offsets.  
Future versions will use **Raft or custom gossip-based coordination** for cluster management.
