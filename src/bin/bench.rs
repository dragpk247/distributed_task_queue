//! # Benchmark Suite for Distributed Task Queue
//!
//! Measures throughput and latency percentiles (P50, P90, P99, P99.9) across
//! various task queue workloads using `hdrhistogram::Histogram<u64>` (in microseconds).
//!
//! Supports both network benchmarking over Tokio TCP loopback and direct in-memory
//! execution against [`QueueEngine`].

use bytes::{Buf, BytesMut};
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Barrier;

use distributed_task_queue::engine::QueueEngine;
use distributed_task_queue::protocol::{parse_command, resp_array, resp_bulk_string};
use distributed_task_queue::server::Server;

/// Workload target options
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TargetWorkload {
    All,
    Lpush,
    Rpop,
    LeaseAck,
    LpushDelay,
    Priority,
}

#[derive(Parser, Debug)]
#[command(
    name = "bench",
    about = "High-performance benchmark & stress-testing suite for Distributed Task Queue"
)]
struct BenchArgs {
    /// Remote or local server address (e.g. 127.0.0.1:6379). If omitted, an ephemeral in-process server is spawned.
    #[arg(short, long)]
    address: Option<String>,

    /// Number of concurrent client tasks / workers
    #[arg(short, long, default_value_t = 50)]
    concurrency: usize,

    /// Total requests per workload benchmark
    #[arg(short, long, default_value_t = 20000)]
    requests: usize,

    /// Payload size in bytes for pushed tasks
    #[arg(short, long, default_value_t = 64)]
    payload_size: usize,

    /// Target workload to execute (all, lpush, rpop, lease-ack, lpush-delay, priority)
    #[arg(short, long, value_enum, default_value_t = TargetWorkload::All)]
    target: TargetWorkload,

    /// Benchmark QueueEngine directly in-memory in addition to TCP loopback
    #[arg(long, default_value_t = false)]
    in_memory: bool,
}

/// Aggregated benchmark metrics
#[derive(Debug, Clone)]
struct BenchmarkMetrics {
    workload_name: String,
    total_ops: usize,
    elapsed: Duration,
    histogram: Histogram<u64>,
}

impl BenchmarkMetrics {
    fn new(workload_name: &str) -> Self {
        Self {
            workload_name: workload_name.to_string(),
            total_ops: 0,
            elapsed: Duration::ZERO,
            // Track latencies from 1 microsecond up to 60 seconds with 3 significant digits
            histogram: Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
                .expect("Failed to create Histogram"),
        }
    }

    fn print_report(&self) {
        let throughput = if self.elapsed.as_secs_f64() > 0.0 {
            self.total_ops as f64 / self.elapsed.as_secs_f64()
        } else {
            0.0
        };

        let p50_us = self.histogram.value_at_quantile(0.50);
        let p90_us = self.histogram.value_at_quantile(0.90);
        let p99_us = self.histogram.value_at_quantile(0.99);
        let p999_us = self.histogram.value_at_quantile(0.999);
        let min_us = self.histogram.min();
        let max_us = self.histogram.max();
        let mean_us = self.histogram.mean();

        println!();
        println!(
            "================================================================================"
        );
        println!(" Workload: {}", self.workload_name);
        println!(
            "--------------------------------------------------------------------------------"
        );
        println!(
            " Total Operations : {:>12} ops",
            format_number(self.total_ops)
        );
        println!(" Elapsed Duration : {:>12.4?}", self.elapsed);
        println!(
            " Throughput       : {:>12} ops/sec",
            format!("{:.2}", throughput)
        );
        println!(
            "--------------------------------------------------------------------------------"
        );
        println!(" Latency Distribution (micros / millis):");
        println!(
            "   Min    : {:>10.2} µs ({:.3} ms)",
            min_us as f64,
            min_us as f64 / 1000.0
        );
        println!(
            "   Mean   : {:>10.2} µs ({:.3} ms)",
            mean_us,
            mean_us / 1000.0
        );
        println!(
            "   P50    : {:>10.2} µs ({:.3} ms)",
            p50_us as f64,
            p50_us as f64 / 1000.0
        );
        println!(
            "   P90    : {:>10.2} µs ({:.3} ms)",
            p90_us as f64,
            p90_us as f64 / 1000.0
        );
        println!(
            "   P99    : {:>10.2} µs ({:.3} ms)",
            p99_us as f64,
            p99_us as f64 / 1000.0
        );
        println!(
            "   P99.9  : {:>10.2} µs ({:.3} ms)",
            p999_us as f64,
            p999_us as f64 / 1000.0
        );
        println!(
            "   Max    : {:>10.2} µs ({:.3} ms)",
            max_us as f64,
            max_us as f64 / 1000.0
        );
        println!(
            "================================================================================"
        );
    }
}

fn format_number(val: usize) -> String {
    let s = val.to_string();
    let mut out = String::with_capacity(s.len() + 4);
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();
    for (i, &c) in chars.iter().enumerate() {
        out.push(c);
        let rem = len - i - 1;
        if rem > 0 && rem.is_multiple_of(3) {
            out.push(',');
        }
    }
    out
}

/// Helper client for sending a RESP request and reading a RESP response over TCP
struct SimpleRespClient {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl SimpleRespClient {
    async fn connect(addr: &str) -> tokio::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(8192),
        })
    }

    async fn send_command(&mut self, cmd_bytes: &[u8]) -> tokio::io::Result<()> {
        self.stream.write_all(cmd_bytes).await
    }

    async fn read_response(&mut self) -> tokio::io::Result<Option<Vec<u8>>> {
        loop {
            if let Some(pos) = self.read_buf.windows(2).position(|w| w == b"\r\n") {
                let first_byte = self.read_buf[0];
                match first_byte {
                    b'+' | b'-' | b':' => {
                        let line_end = pos + 2;
                        let line = self.read_buf[..line_end].to_vec();
                        self.read_buf.advance(line_end);
                        return Ok(Some(line));
                    }
                    b'$' => {
                        let len_str = std::str::from_utf8(&self.read_buf[1..pos]).map_err(|e| {
                            tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e)
                        })?;
                        let len: isize = len_str.parse().map_err(|e| {
                            tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e)
                        })?;
                        if len < 0 {
                            let line_end = pos + 2;
                            let line = self.read_buf[..line_end].to_vec();
                            self.read_buf.advance(line_end);
                            return Ok(Some(line));
                        }
                        let ulen = len as usize;
                        let total_expected = pos + 2 + ulen + 2;
                        if self.read_buf.len() >= total_expected {
                            let data = self.read_buf[..total_expected].to_vec();
                            self.read_buf.advance(total_expected);
                            return Ok(Some(data));
                        }
                    }
                    b'*' => {
                        let mut copy_buf = self.read_buf.clone();
                        match parse_command(&mut copy_buf) {
                            Ok(Some((_, raw))) => {
                                let consumed = raw.len();
                                self.read_buf.advance(consumed);
                                return Ok(Some(raw));
                            }
                            Ok(None) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    _ => {
                        let line_end = pos + 2;
                        let line = self.read_buf[..line_end].to_vec();
                        self.read_buf.advance(line_end);
                        return Ok(Some(line));
                    }
                }
            }

            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(tokio::io::Error::new(
                    tokio::io::ErrorKind::UnexpectedEof,
                    "Connection closed by server",
                ));
            }
            self.read_buf.extend_from_slice(&chunk[..n]);
        }
    }
}

// -----------------------------------------------------------------------------
// TCP Benchmark Implementations
// -----------------------------------------------------------------------------

async fn bench_tcp_lpush(
    addr: &str,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let mut metrics =
        BenchmarkMetrics::new(&format!("TCP LPUSH (Enqueue {}B payload)", payload.len()));
    let queue_name = "bench_lpush_q";

    let lpush_frame = resp_array(&[
        resp_bulk_string(b"LPUSH"),
        resp_bulk_string(queue_name.as_bytes()),
        resp_bulk_string(payload),
    ]);

    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let addr = addr.to_string();
        let frame = lpush_frame.clone();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut client = SimpleRespClient::connect(&addr)
                .await
                .expect("Failed to connect client");
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();

            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let start = Instant::now();
                client.send_command(&frame).await.unwrap();
                let _ = client.read_response().await.unwrap();
                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

async fn bench_tcp_rpop(
    addr: &str,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let queue_name = "bench_rpop_q";

    // Pre-populate queue with total_requests items
    {
        let mut pre_client = SimpleRespClient::connect(addr).await.unwrap();
        let lpush_frame = resp_array(&[
            resp_bulk_string(b"LPUSH"),
            resp_bulk_string(queue_name.as_bytes()),
            resp_bulk_string(payload),
        ]);
        let chunk_size = 1000;
        let mut sent = 0;
        while sent < total_requests {
            let batch = (total_requests - sent).min(chunk_size);
            let mut pipeline = Vec::with_capacity(lpush_frame.len() * batch);
            for _ in 0..batch {
                pipeline.extend_from_slice(&lpush_frame);
            }
            pre_client.send_command(&pipeline).await.unwrap();
            for _ in 0..batch {
                let _ = pre_client.read_response().await.unwrap();
            }
            sent += batch;
        }
    }

    let mut metrics =
        BenchmarkMetrics::new(&format!("TCP RPOP (Dequeue {}B payload)", payload.len()));
    let rpop_frame = resp_array(&[
        resp_bulk_string(b"RPOP"),
        resp_bulk_string(queue_name.as_bytes()),
    ]);

    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let addr = addr.to_string();
        let frame = rpop_frame.clone();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut client = SimpleRespClient::connect(&addr)
                .await
                .expect("Failed to connect client");
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();

            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let start = Instant::now();
                client.send_command(&frame).await.unwrap();
                let _ = client.read_response().await.unwrap();
                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

async fn bench_tcp_lease_ack(
    addr: &str,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let queue_name = "bench_lease_ack_q";

    // Pre-populate queue
    {
        let mut pre_client = SimpleRespClient::connect(addr).await.unwrap();
        let lpush_frame = resp_array(&[
            resp_bulk_string(b"LPUSH"),
            resp_bulk_string(queue_name.as_bytes()),
            resp_bulk_string(payload),
        ]);
        let chunk_size = 1000;
        let mut sent = 0;
        while sent < total_requests {
            let batch = (total_requests - sent).min(chunk_size);
            let mut pipeline = Vec::with_capacity(lpush_frame.len() * batch);
            for _ in 0..batch {
                pipeline.extend_from_slice(&lpush_frame);
            }
            pre_client.send_command(&pipeline).await.unwrap();
            for _ in 0..batch {
                let _ = pre_client.read_response().await.unwrap();
            }
            sent += batch;
        }
    }

    let mut metrics = BenchmarkMetrics::new("TCP Lease & Ack (RPOPLEASE + TASKACK)");
    let rpoplease_frame = resp_array(&[
        resp_bulk_string(b"RPOPLEASE"),
        resp_bulk_string(queue_name.as_bytes()),
        resp_bulk_string(b"60"),
    ]);

    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let addr = addr.to_string();
        let lease_frame = rpoplease_frame.clone();
        let q_name = queue_name.to_string();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut client = SimpleRespClient::connect(&addr)
                .await
                .expect("Failed to connect client");
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();

            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let start = Instant::now();
                // 1. RPOPLEASE
                client.send_command(&lease_frame).await.unwrap();
                let resp = client.read_response().await.unwrap();

                // Extract task_id from resp array [*2\r\n$<len>\r\n<task_id>\r\n...]
                if let Some(raw) = resp {
                    if let Some(task_id) = extract_task_id_from_array(&raw) {
                        // 2. TASKACK
                        let ack_frame = resp_array(&[
                            resp_bulk_string(b"TASKACK"),
                            resp_bulk_string(q_name.as_bytes()),
                            resp_bulk_string(task_id.as_bytes()),
                        ]);
                        client.send_command(&ack_frame).await.unwrap();
                        let _ = client.read_response().await.unwrap();
                    }
                }

                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

fn extract_task_id_from_array(raw: &[u8]) -> Option<String> {
    // Array format: *2\r\n$<len>\r\n<id>\r\n$<len>\r\n<payload>\r\n
    if !raw.starts_with(b"*2\r\n") {
        return None;
    }
    let rest = &raw[4..];
    if !rest.starts_with(b"$") {
        return None;
    }
    let crlf1 = rest.windows(2).position(|w| w == b"\r\n")?;
    let len_str = std::str::from_utf8(&rest[1..crlf1]).ok()?;
    let len: usize = len_str.parse().ok()?;
    let id_start = crlf1 + 2;
    let id_end = id_start + len;
    if rest.len() >= id_end {
        let id = std::str::from_utf8(&rest[id_start..id_end]).ok()?;
        return Some(id.to_string());
    }
    None
}

async fn bench_tcp_priority(
    addr: &str,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let mut metrics = BenchmarkMetrics::new("TCP Priority Enqueue & Pop (LPUSH_PRIORITY + RPOP)");
    let queue_name = "bench_priority_q";

    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for worker_idx in 0..concurrency {
        let addr = addr.to_string();
        let q_name = queue_name.to_string();
        let payload = payload.to_vec();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut client = SimpleRespClient::connect(&addr)
                .await
                .expect("Failed to connect client");
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();

            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let prio = ((worker_idx % 10) as u8).to_string();
                let lpush_frame = resp_array(&[
                    resp_bulk_string(b"LPUSH_PRIORITY"),
                    resp_bulk_string(q_name.as_bytes()),
                    resp_bulk_string(prio.as_bytes()),
                    resp_bulk_string(&payload),
                ]);
                let rpop_frame = resp_array(&[
                    resp_bulk_string(b"RPOP"),
                    resp_bulk_string(q_name.as_bytes()),
                ]);

                let start = Instant::now();
                // Push with priority
                client.send_command(&lpush_frame).await.unwrap();
                let _ = client.read_response().await.unwrap();
                // Pop
                client.send_command(&rpop_frame).await.unwrap();
                let _ = client.read_response().await.unwrap();

                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

async fn bench_tcp_lpush_delay(
    addr: &str,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let mut metrics = BenchmarkMetrics::new("TCP Delayed Enqueue (LPUSH_DELAY)");
    let queue_name = "bench_delay_q";

    let delay_frame = resp_array(&[
        resp_bulk_string(b"LPUSH_DELAY"),
        resp_bulk_string(queue_name.as_bytes()),
        resp_bulk_string(b"30.0"),
        resp_bulk_string(payload),
    ]);

    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let addr = addr.to_string();
        let frame = delay_frame.clone();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut client = SimpleRespClient::connect(&addr)
                .await
                .expect("Failed to connect client");
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();

            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let start = Instant::now();
                client.send_command(&frame).await.unwrap();
                let _ = client.read_response().await.unwrap();
                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

// -----------------------------------------------------------------------------
// In-Memory Engine Direct Benchmarks (--in-memory)
// -----------------------------------------------------------------------------

async fn bench_in_memory_lpush(
    engine: Arc<QueueEngine>,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let mut metrics = BenchmarkMetrics::new(&format!(
        "In-Memory Engine LPUSH ({}B payload)",
        payload.len()
    ));
    let queue_name = "inmem_lpush_q";

    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let eng = Arc::clone(&engine);
        let q_name = queue_name.to_string();
        let pay = payload.to_vec();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let start = Instant::now();
                eng.lpush(&q_name, pay.clone());
                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

async fn bench_in_memory_rpop(
    engine: Arc<QueueEngine>,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let queue_name = "inmem_rpop_q";

    for _ in 0..total_requests {
        engine.lpush(queue_name, payload.to_vec());
    }

    let mut metrics = BenchmarkMetrics::new(&format!(
        "In-Memory Engine RPOP ({}B payload)",
        payload.len()
    ));
    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let eng = Arc::clone(&engine);
        let q_name = queue_name.to_string();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let start = Instant::now();
                let _ = eng.rpop(&q_name);
                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

async fn bench_in_memory_lease_ack(
    engine: Arc<QueueEngine>,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let queue_name = "inmem_lease_ack_q";

    for _ in 0..total_requests {
        engine.lpush(queue_name, payload.to_vec());
    }

    let mut metrics = BenchmarkMetrics::new("In-Memory Engine Lease & Ack (RPOPLEASE + TASKACK)");
    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let eng = Arc::clone(&engine);
        let q_name = queue_name.to_string();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
            b.wait().await;

            loop {
                let current_idx = counter.fetch_add(1, Ordering::Relaxed);
                if current_idx >= total_requests {
                    break;
                }
                let task_id = format!("bench-task-{}", current_idx);
                let start = Instant::now();
                if eng
                    .rpop_with_lease(&q_name, task_id.clone(), Duration::from_secs(60))
                    .is_some()
                {
                    eng.task_ack(&q_name, &task_id);
                }
                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

async fn bench_in_memory_priority(
    engine: Arc<QueueEngine>,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let mut metrics =
        BenchmarkMetrics::new("In-Memory Engine Priority Enqueue & Pop (LPUSH_PRIORITY + RPOP)");
    let queue_name = "inmem_priority_q";

    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for worker_idx in 0..concurrency {
        let eng = Arc::clone(&engine);
        let q_name = queue_name.to_string();
        let pay = payload.to_vec();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
            let prio = (worker_idx % 10) as u8;
            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let start = Instant::now();
                eng.lpush_priority(&q_name, prio, pay.clone());
                let _ = eng.rpop(&q_name);
                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

async fn bench_in_memory_lpush_delay(
    engine: Arc<QueueEngine>,
    concurrency: usize,
    total_requests: usize,
    payload: &[u8],
) -> BenchmarkMetrics {
    let mut metrics = BenchmarkMetrics::new("In-Memory Engine Delayed Enqueue (LPUSH_DELAY)");
    let queue_name = "inmem_delay_q";

    let req_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let eng = Arc::clone(&engine);
        let q_name = queue_name.to_string();
        let pay = payload.to_vec();
        let counter = Arc::clone(&req_counter);
        let b = Arc::clone(&barrier);

        tasks.push(tokio::spawn(async move {
            let mut local_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
            b.wait().await;

            while counter.fetch_add(1, Ordering::Relaxed) < total_requests {
                let start = Instant::now();
                eng.lpush_delayed(&q_name, Duration::from_secs(30), pay.clone());
                let elapsed_us = start.elapsed().as_micros().max(1) as u64;
                let _ = local_hist.record(elapsed_us);
            }

            local_hist
        }));
    }

    barrier.wait().await;
    let overall_start = Instant::now();

    for t in tasks {
        let h = t.await.unwrap();
        metrics.histogram.add(&h).unwrap();
    }

    metrics.elapsed = overall_start.elapsed();
    metrics.total_ops = total_requests;
    metrics
}

// -----------------------------------------------------------------------------
// Ephemeral Server & Main Entry Point
// -----------------------------------------------------------------------------

async fn start_ephemeral_server() -> (String, tokio::sync::oneshot::Sender<()>, std::path::PathBuf)
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind ephemeral TCP listener");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let addr = format!("127.0.0.1:{}", port);
    let aof_path = std::env::temp_dir().join(format!("bench_server_{}.aof", port));
    let _ = std::fs::remove_file(&aof_path);

    let server = Server::new(&addr, aof_path.to_str().unwrap()).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let _ = server
            .run_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    // Wait for server to bind
    for _ in 0..50 {
        if TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    (addr, shutdown_tx, aof_path)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = BenchArgs::parse();

    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║       DISTRIBUTED TASK QUEUE BENCHMARK & STRESS TESTING SUITE                ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!(" Configuration:");
    println!("   Concurrency  : {}", args.concurrency);
    println!("   Requests/test: {}", format_number(args.requests));
    println!("   Payload Size : {} bytes", args.payload_size);
    println!("   Target       : {:?}", args.target);
    println!("   In-Memory    : {}", args.in_memory);

    let payload = vec![b'x'; args.payload_size];

    // Determine target server
    let (server_addr, _shutdown_tx, aof_path_opt) = match args.address {
        Some(ref custom_addr) => {
            println!("   Target Server: {} (external)", custom_addr);
            (custom_addr.clone(), None, None)
        }
        None => {
            println!("   Target Server: Starting ephemeral in-process TCP server...");
            let (addr, tx, aof_path) = start_ephemeral_server().await;
            println!("   Target Server: Bound to {}", addr);
            (addr, Some(tx), Some(aof_path))
        }
    };

    println!(
        "\n>>> Running TCP Wire Protocol Benchmarks (Client Concurrency: {})...",
        args.concurrency
    );

    // Execute selected workloads
    let run_all = args.target == TargetWorkload::All;

    if run_all || args.target == TargetWorkload::Lpush {
        let report = bench_tcp_lpush(&server_addr, args.concurrency, args.requests, &payload).await;
        report.print_report();
    }

    if run_all || args.target == TargetWorkload::Rpop {
        let report = bench_tcp_rpop(&server_addr, args.concurrency, args.requests, &payload).await;
        report.print_report();
    }

    if run_all || args.target == TargetWorkload::LeaseAck {
        let report =
            bench_tcp_lease_ack(&server_addr, args.concurrency, args.requests, &payload).await;
        report.print_report();
    }

    if run_all || args.target == TargetWorkload::Priority {
        let report =
            bench_tcp_priority(&server_addr, args.concurrency, args.requests, &payload).await;
        report.print_report();
    }

    if run_all || args.target == TargetWorkload::LpushDelay {
        let report =
            bench_tcp_lpush_delay(&server_addr, args.concurrency, args.requests, &payload).await;
        report.print_report();
    }

    // Direct in-memory engine benchmarks if requested
    if args.in_memory {
        println!(
            "\n>>> Running Direct In-Memory QueueEngine Benchmarks (Lock Concurrency: {})...",
            args.concurrency
        );
        let engine = Arc::new(QueueEngine::new());

        if run_all || args.target == TargetWorkload::Lpush {
            let report = bench_in_memory_lpush(
                Arc::clone(&engine),
                args.concurrency,
                args.requests,
                &payload,
            )
            .await;
            report.print_report();
        }

        if run_all || args.target == TargetWorkload::Rpop {
            let report = bench_in_memory_rpop(
                Arc::clone(&engine),
                args.concurrency,
                args.requests,
                &payload,
            )
            .await;
            report.print_report();
        }

        if run_all || args.target == TargetWorkload::LeaseAck {
            let report = bench_in_memory_lease_ack(
                Arc::clone(&engine),
                args.concurrency,
                args.requests,
                &payload,
            )
            .await;
            report.print_report();
        }

        if run_all || args.target == TargetWorkload::Priority {
            let report = bench_in_memory_priority(
                Arc::clone(&engine),
                args.concurrency,
                args.requests,
                &payload,
            )
            .await;
            report.print_report();
        }

        if run_all || args.target == TargetWorkload::LpushDelay {
            let report = bench_in_memory_lpush_delay(
                Arc::clone(&engine),
                args.concurrency,
                args.requests,
                &payload,
            )
            .await;
            report.print_report();
        }
    }

    // Clean up temporary AOF if ephemeral server was used
    if let Some(aof_path) = aof_path_opt {
        let _ = std::fs::remove_file(aof_path);
    }

    println!("\nBenchmark suite completed successfully!");
    Ok(())
}
