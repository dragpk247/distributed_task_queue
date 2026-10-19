# 🗺️ Distributed Task Queue — Project Tracks & Roadmap

This document outlines the current state and forward execution tracks for `distributed_task_queue`.

---

## 📊 Current State
- **Language & Runtime**: Rust (Tokio async event loop)
- **Status**: Stable & Clean on `main`
- **Test Suite**: 41/41 passing (29 unit tests in `src/lib.rs` + 12 integration tests in `tests/integration_tests.rs`)

---

## 🎯 Active Execution Tracks

### Track B.1: Benchmarking & Performance Profiling
- [ ] **Throughput & Latency Benchmarks**:
  - Implement Criterion or Tokio-based microbenchmarks (`benches/throughput.rs`).
  - Measure ops/sec for `LPUSH`, `RPOP`, `BRPOP`, and `RPOPLEASE` under high concurrent worker contention.
  - Profile lock contention on `parking_lot::RwLock` under high worker counts.
- [ ] **Flamegraph Generation**:
  - Run `cargo flamegraph` during stress bursts to identify potential serialization or channel bottlenecks.

### Track B.2: Clustering & High Availability
- [ ] **Replication (`SYNC`) Chaos Tests**:
  - Add test harness simulating replica disconnection, network latency, and reconnection replay.
  - Verify state consistency across primary and secondary nodes during heavy mutation streams.
- [ ] **Failover & Re-election**:
  - Explore automated primary failover mechanisms.

### Track B.3: Operational Tooling & Observability
- [ ] **Prometheus Metrics Exporter**:
  - Expose task queue metrics (queue depth, processing latency, DLQ count, lease expirations) via `/metrics` or Redis `INFO` response.
- [ ] **Management CLI**:
  - Lightweight inspection tool to inspect dead-letter tasks, replay failed jobs, and query cluster health.
