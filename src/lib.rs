//! # Distributed Task Queue Crate Root
//!
//! Exposes the primary modules for the Redis-compatible distributed task queue:
//! - [`protocol`]: Binary streaming parser and frame serializers for the Redis Serialization Protocol (RESP).
//! - [`engine`]: Concurrent in-memory queue data structures, lease management, dead-letter queue (DLQ) routing, and async listeners.
//! - [`aof`]: Append-Only-File persistence layer with startup replay and online atomic compaction (`BGREWRITEAOF`).
//! - [`server`]: Asynchronous Tokio TCP server managing client connections, replication streaming, and worker leasing.

pub mod aof;
pub mod engine;
pub mod protocol;
pub mod server;
