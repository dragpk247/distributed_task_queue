# Distributed Task Queue

A high-performance, asynchronous distributed task queue engine built in Rust, powered by Tokio and compatible with the Redis Serialization Protocol (RESP).

---

## ⚡ Overview

`distributed_task_queue` is designed as a lightweight, low-latency task broker and coordination engine. It implements an asynchronous TCP event loop that accepts client connections, parses streaming protocol frames, and provides thread-safe in-memory queue primitives.

### Key Highlights

- **Asynchronous Network I/O**: Non-blocking TCP server built on [Tokio](https://tokio.rs/), spawning lightweight tasks per connected client.
- **Concurrent In-Memory Queues**: Thread-safe task state backed by `parking_lot::RwLock` for low-overhead read/write locking across worker threads.
- **RESP Protocol Interface**: Wire-level compatibility with the Redis Serialization Protocol (listening by default on port `6379`), enabling native integration with existing Redis clients and CLIs.
- **Efficient Buffer Management**: Chunked streaming and zero-copy slicing powered by [`bytes::BytesMut`](https://docs.rs/bytes).
- **Structured Telemetry**: Diagnostic tracing and event logging via [`tracing`](https://docs.rs/tracing) and [`tracing-subscriber`](https://docs.rs/tracing-subscriber).

---

## 🏗️ Architecture

```text
       Client (redis-cli / worker)
                  │
                  ▼ [TCP :6379]
        ┌───────────────────┐
        │ Tokio TCP Listener│
        └─────────┬─────────┘
                  │ spawns per connection
                  ▼
        ┌───────────────────┐
        │   handle_client   │ ──► Reads into BytesMut buffer
        └─────────┬─────────┘
                  │ CRLF frame evaluation
                  ▼
       ┌─────────────────────┐
       │ Arc<RwLock<Engine>> │
       │ ┌─────────────────┐ │
       │ │ Queue: "tasks"  │ │ ──► VecDeque<Vec<u8>>
       │ │ Queue: "events" │ │
       │ └─────────────────┘ │
       └─────────────────────┘
```

---

## 📦 Dependencies

- **[tokio](https://crates.io/crates/tokio)**: Asynchronous runtime with full networking primitives.
- **[bytes](https://crates.io/crates/bytes)**: Utilities for zero-copy byte buffers.
- **[parking_lot](https://crates.io/crates/parking_lot)**: Fast, compact synchronization primitives.
- **[crossbeam-channel](https://crates.io/crates/crossbeam-channel)**: Multi-producer multi-consumer channels.
- **[clap](https://crates.io/crates/clap)**: Command-line argument parsing.
- **[tracing](https://crates.io/crates/tracing)** / **[tracing-subscriber](https://crates.io/crates/tracing-subscriber)**: Production-grade observability.

---

## 🚀 Getting Started

### Prerequisites

Ensure you have a recent version of the Rust toolchain installed:

```bash
rustc --version # 1.80+ recommended
cargo --version
```

### Running the Server

Clone the repository and launch the server:

```bash
git clone https://github.com/dragpk247/distributed_task_queue.git
cd distributed_task_queue

# Run in debug mode with info tracing enabled
RUST_LOG=info cargo run

# Or build optimized release binary
cargo build --release
./target/release/distributed_task_queue
```

### Testing the Connection

Once running on `127.0.0.1:6379`, you can verify the connection using `nc` or `redis-cli`:

```bash
# Using netcat
echo -e "PING\r\n" | nc 127.0.0.1 6379
# Response: +OK

# Using redis-cli
redis-cli -p 6379
```

---

## 🗺️ Roadmap

- [ ] Full RESP2 / RESP3 binary frame parser and serializer.
- [ ] Task acknowledgment (`ACK`), dead-letter queues (`DLQ`), and visibility timeouts.
- [ ] Worker heartbeat registry and health monitoring.
- [ ] Priority scheduling and delayed execution queues.
- [ ] Write-Ahead Logging (WAL) and snapshot persistence to disk.
- [ ] Multi-node clustering and raft consensus for distributed fault tolerance.

---

## 📄 License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
