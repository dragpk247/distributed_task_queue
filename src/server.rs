//! # Server Module: Async TCP Server & Connection Lifecycle
//!
//! Handles incoming TCP connections from client producers, workers, and replicas:
//! - Spawns a Tokio task per connected client socket.
//! - Executes commands against the [`QueueEngine`].
//! - Dispatches mutations to the [`AofManager`] persistence ledger.
//! - Broadcasts mutations live to connected replicas over [`broadcast::Sender`].
//! - Runs an autonomous background timer thread to reclaim expired visibility task leases.

use bytes::{Buf, BytesMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use crate::aof::AofManager;
use crate::engine::QueueEngine;
use crate::protocol::{
    parse_command, resp_array, resp_bulk_string, resp_error, resp_integer, resp_null,
    resp_simple_string, Command,
};

/// Shared state available across all connection handler tasks.
pub struct ServerContext {
    /// In-memory queue storage and concurrency primitives.
    pub engine: Arc<QueueEngine>,
    /// Append-only file persistence manager.
    pub aof: Arc<AofManager>,
    /// Broadcast channel streaming raw mutations to any connected replicas (`SYNC`).
    pub replica_stream: broadcast::Sender<Vec<u8>>,
    /// Monotonically increasing atomic counter for generating distinct Task IDs (`task-1`, `task-2`, ...).
    pub task_counter: AtomicU64,
    /// Optional password required to authenticate client connections.
    pub requirepass: Option<String>,
}

/// The main distributed task queue TCP server.
pub struct Server {
    addr: String,
    context: Arc<ServerContext>,
}

impl Server {
    /// Initializes the server without authentication, replays the existing AOF persistence ledger,
    /// and configures background channels.
    pub fn new(addr: &str, aof_path: &str) -> std::io::Result<Self> {
        Self::with_requirepass(addr, aof_path, None)
    }

    /// Initializes the server with optional authentication requirement (`requirepass`).
    pub fn with_requirepass(
        addr: &str,
        aof_path: &str,
        requirepass: Option<String>,
    ) -> std::io::Result<Self> {
        let aof = Arc::new(AofManager::open(aof_path)?);
        let restored_queues = aof.replay()?;

        let engine = Arc::new(QueueEngine::new());
        {
            let mut inner = engine.inner.write();
            inner.queues = restored_queues;
        }

        let (replica_tx, _) = broadcast::channel(4096);

        let context = Arc::new(ServerContext {
            engine,
            aof,
            replica_stream: replica_tx,
            task_counter: AtomicU64::new(1),
            requirepass,
        });

        Ok(Self {
            addr: addr.to_string(),
            context,
        })
    }

    /// Access the underlying queue engine (useful for integration testing).
    pub fn get_engine(&self) -> Arc<QueueEngine> {
        Arc::clone(&self.context.engine)
    }

    /// Explicitly flushes the Append-Only File persistence ledger to disk.
    pub fn flush_aof(&self) -> std::io::Result<()> {
        self.context.aof.flush()
    }

    /// Starts the TCP listener event loop, background lease reaper, and delayed task polling loop.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        tracing::info!("Server listening on {}", self.addr);

        // Background worker: Periodically reclaims expired visibility timeout leases every 1 second
        let engine_clone = Arc::clone(&self.context.engine);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let reaped = engine_clone.reap_expired_leases();
                if reaped > 0 {
                    tracing::info!("Reaped and re-queued/DLQ'd {} expired tasks", reaped);
                }
            }
        });

        // Background worker: Polls and promotes ready delayed tasks every 50ms
        let engine_delayed = Arc::clone(&self.context.engine);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            loop {
                interval.tick().await;
                engine_delayed.pop_ready_delayed_tasks();
            }
        });

        // Main connection acceptance loop with graceful shutdown signal listener
        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    let (socket, client_addr) = accept_res?;
                    tracing::debug!("New connection from: {}", client_addr);

                    let ctx = Arc::clone(&self.context);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, ctx).await {
                            tracing::debug!("Connection closed with error: {:?}", e);
                        }
                    });
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Server received shutdown signal (Ctrl+C). Flushing AOF...");
                    let _ = self.flush_aof();
                    break;
                }
            }
        }

        let _ = self.flush_aof();
        Ok(())
    }
}

/// Asynchronous stream processor for an individual client connection.
pub async fn handle_connection(
    mut socket: TcpStream,
    ctx: Arc<ServerContext>,
) -> tokio::io::Result<()> {
    let mut buffer = BytesMut::with_capacity(4096);
    let mut authenticated = ctx.requirepass.is_none();

    loop {
        let mut chunk = [0u8; 1024];
        let bytes_read = socket.read(&mut chunk).await?;
        if bytes_read == 0 {
            return Ok(()); // Client disconnected cleanly
        }

        buffer.extend_from_slice(&chunk[..bytes_read]);

        // Process all complete frames currently in the buffer (supports pipelining)
        while !buffer.is_empty() {
            match parse_command(&mut buffer) {
                Ok(Some((command, raw_frame))) => {
                    // ---------------------------------------------------------
                    // 1. Authentication Check & Session Validation
                    // ---------------------------------------------------------
                    if let Command::Auth { password } = &command {
                        if ctx.requirepass.as_deref() == Some(password.as_str()) {
                            // Password matches configured --requirepass
                            authenticated = true;
                            socket.write_all(&resp_simple_string("OK")).await?;
                        } else if ctx.requirepass.is_none() {
                            // Client attempted AUTH on a server without password configured
                            socket
                                .write_all(&resp_error(
                                    "ERR Client sent AUTH, but no password is set",
                                ))
                                .await?;
                        } else {
                            // Invalid password provided
                            socket
                                .write_all(&resp_error(
                                    "WRONGPASS invalid username-password pair or token",
                                ))
                                .await?;
                        }
                        continue;
                    }

                    // Block all commands except PING if connection has not authenticated
                    if !authenticated && !matches!(command, Command::Ping) {
                        socket
                            .write_all(&resp_error("NOAUTH Authentication required."))
                            .await?;
                        continue;
                    }

                    // ---------------------------------------------------------
                    // 2. Command Execution & State Dispatch
                    // ---------------------------------------------------------
                    match command {
                        // Health check probe (accessible unauthenticated)
                        Command::Ping => {
                            socket.write_all(&resp_simple_string("PONG")).await?;
                        }

                        // Push item to head of queue
                        Command::Lpush { queue, payload } => {
                            let len = ctx.engine.lpush(&queue, payload);
                            let _ = ctx.aof.append(&raw_frame);
                            let _ = ctx.replica_stream.send(raw_frame);
                            socket.write_all(&resp_integer(len as i64)).await?;
                        }

                        // Pop item from tail of queue
                        Command::Rpop { queue } => {
                            let maybe_item = ctx.engine.rpop(&queue);
                            if let Some(item) = maybe_item {
                                let _ = ctx.aof.append(&raw_frame);
                                let _ = ctx.replica_stream.send(raw_frame);
                                socket.write_all(&resp_bulk_string(&item)).await?;
                            } else {
                                socket.write_all(&resp_null()).await?;
                            }
                        }

                        // Atomic transfer between queues
                        Command::Rpoplpush {
                            source,
                            destination,
                        } => {
                            let maybe_item = ctx.engine.rpoplpush(&source, &destination);
                            if let Some(item) = maybe_item {
                                let _ = ctx.aof.append(&raw_frame);
                                let _ = ctx.replica_stream.send(raw_frame);
                                socket.write_all(&resp_bulk_string(&item)).await?;
                            } else {
                                socket.write_all(&resp_null()).await?;
                            }
                        }

                        // Non-busy blocking pop across multiple queues
                        Command::Brpop {
                            queues,
                            timeout_secs,
                        } => {
                            let timeout = Duration::from_secs_f64(timeout_secs);
                            let maybe_item = ctx.engine.brpop(&queues, timeout).await;
                            if let Some((matched_queue, item)) = maybe_item {
                                let rpop_frame = format!(
                                    "*2\r\n$4\r\nRPOP\r\n${}\r\n{}\r\n",
                                    matched_queue.len(),
                                    matched_queue
                                )
                                .into_bytes();
                                let _ = ctx.aof.append(&rpop_frame);
                                let _ = ctx.replica_stream.send(rpop_frame);

                                let resp = resp_array(&[
                                    resp_bulk_string(matched_queue.as_bytes()),
                                    resp_bulk_string(&item),
                                ]);
                                socket.write_all(&resp).await?;
                            } else {
                                socket.write_all(&resp_null()).await?;
                            }
                        }

                        // Blocking transfer
                        Command::Brpoplpush {
                            source,
                            destination,
                            timeout_secs,
                        } => {
                            let timeout = Duration::from_secs_f64(timeout_secs);
                            let rx = ctx
                                .engine
                                .brpop(std::slice::from_ref(&source), timeout)
                                .await;
                            if let Some((_, item)) = rx {
                                ctx.engine.lpush(&destination, item.clone());
                                let rpoplpush_frame = format!(
                                    "*3\r\n$9\r\nRPOPLPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                    source.len(),
                                    source,
                                    destination.len(),
                                    destination
                                )
                                .into_bytes();
                                let _ = ctx.aof.append(&rpoplpush_frame);
                                let _ = ctx.replica_stream.send(rpoplpush_frame);
                                socket.write_all(&resp_bulk_string(&item)).await?;
                            } else {
                                socket.write_all(&resp_null()).await?;
                            }
                        }

                        // Pop with visibility timeout lease: returns [task_id, payload]
                        Command::RpopLease {
                            queue,
                            visibility_secs,
                        } => {
                            let task_num = ctx.task_counter.fetch_add(1, Ordering::Relaxed);
                            let task_id = format!("task-{}", task_num);
                            let vis_duration = Duration::from_secs_f64(visibility_secs);

                            if let Some(payload) =
                                ctx.engine
                                    .rpop_with_lease(&queue, task_id.clone(), vis_duration)
                            {
                                let rpop_frame = format!(
                                    "*2\r\n$4\r\nRPOP\r\n${}\r\n{}\r\n",
                                    queue.len(),
                                    queue
                                )
                                .into_bytes();
                                let _ = ctx.aof.append(&rpop_frame);
                                let _ = ctx.replica_stream.send(rpop_frame);

                                let resp = resp_array(&[
                                    resp_bulk_string(task_id.as_bytes()),
                                    resp_bulk_string(&payload),
                                ]);
                                socket.write_all(&resp).await?;
                            } else {
                                socket.write_all(&resp_null()).await?;
                            }
                        }

                        // Blocking pop with lease: returns [task_id, payload]
                        Command::BrpopLease {
                            queue,
                            timeout_secs,
                            visibility_secs,
                        } => {
                            let task_num = ctx.task_counter.fetch_add(1, Ordering::Relaxed);
                            let task_id = format!("task-{}", task_num);
                            let timeout = Duration::from_secs_f64(timeout_secs);
                            let vis_duration = Duration::from_secs_f64(visibility_secs);

                            if let Some(payload) = ctx
                                .engine
                                .brpop_lease(&queue, timeout, vis_duration, task_id.clone())
                                .await
                            {
                                let rpop_frame = format!(
                                    "*2\r\n$4\r\nRPOP\r\n${}\r\n{}\r\n",
                                    queue.len(),
                                    queue
                                )
                                .into_bytes();
                                let _ = ctx.aof.append(&rpop_frame);
                                let _ = ctx.replica_stream.send(rpop_frame);

                                let resp = resp_array(&[
                                    resp_bulk_string(task_id.as_bytes()),
                                    resp_bulk_string(&payload),
                                ]);
                                socket.write_all(&resp).await?;
                            } else {
                                socket.write_all(&resp_null()).await?;
                            }
                        }

                        // Renew task lease window (heartbeat)
                        Command::TaskTouch {
                            queue,
                            task_id,
                            extend_secs,
                        } => {
                            let extend_by = Duration::from_secs_f64(extend_secs);
                            let success = ctx.engine.task_touch(&queue, &task_id, extend_by);
                            if success {
                                socket.write_all(&resp_simple_string("OK")).await?;
                            } else {
                                socket
                                    .write_all(&resp_error("Task ID not found in-flight"))
                                    .await?;
                            }
                        }

                        // Settle task successfully
                        Command::TaskAck { queue, task_id } => {
                            let success = ctx.engine.task_ack(&queue, &task_id);
                            if success {
                                socket.write_all(&resp_simple_string("OK")).await?;
                            } else {
                                socket
                                    .write_all(&resp_error("Task ID not found in-flight"))
                                    .await?;
                            }
                        }

                        // Mark task as failed
                        Command::TaskNack { queue, task_id } => {
                            let success = ctx.engine.task_nack(&queue, &task_id);
                            if success {
                                socket.write_all(&resp_simple_string("OK")).await?;
                            } else {
                                socket
                                    .write_all(&resp_error("Task ID not found in-flight"))
                                    .await?;
                            }
                        }

                        // Compact persistence ledger
                        Command::BgRewriteAof => {
                            let current_state = {
                                let inner = ctx.engine.inner.read();
                                inner.queues.clone()
                            };
                            match ctx.aof.compact(&current_state) {
                                Ok(count) => {
                                    socket
                                        .write_all(&resp_simple_string(&format!(
                                            "Background append only file rewriting started with {} items",
                                            count
                                        )))
                                        .await?;
                                }
                                Err(e) => {
                                    socket
                                        .write_all(&resp_error(&format!(
                                            "AOF rewrite failed: {}",
                                            e
                                        )))
                                        .await?;
                                }
                            }
                        }

                        // Replica node real-time mutation stream subscriber
                        Command::Sync => {
                            let mut rx = ctx.replica_stream.subscribe();
                            socket.write_all(&resp_simple_string("SYNC OK")).await?;
                            loop {
                                match rx.recv().await {
                                    Ok(frame) => {
                                        if let Err(e) = socket.write_all(&frame).await {
                                            tracing::warn!("Replica disconnected: {:?}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                                        tracing::warn!("Replica lagged by {} messages", missed);
                                    }
                                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                                }
                            }
                        }

                        Command::ReplConf => {
                            socket.write_all(&resp_simple_string("OK")).await?;
                        }

                        // Schedule an item with a future execution delay (min-heap priority queue)
                        Command::LpushDelay {
                            queue,
                            delay_secs,
                            payload,
                        } => {
                            // Convert floating point seconds into std::time::Duration (clamp negative to 0)
                            let duration = Duration::from_secs_f64(delay_secs.max(0.0));
                            // Enqueue task into Engine's min-heap and receive unique assigned task ID
                            let task_id =
                                ctx.engine.lpush_delayed(&queue, duration, payload.clone());
                            // Persist delayed task schedule to AOF log and mirror to connected replicas
                            let _ = ctx.aof.append(&raw_frame);
                            let _ = ctx.replica_stream.send(raw_frame);
                            // Return the assigned scheduled task ID as a RESP Integer (:id\r\n)
                            socket.write_all(&resp_integer(task_id as i64)).await?;
                        }

                        Command::Auth { .. } => {
                            // Handled before match
                            socket.write_all(&resp_simple_string("OK")).await?;
                        }

                        // Diagnostic information & metrics probe
                        Command::Info => {
                            let stats = ctx.engine.get_stats();
                            let mut info_text = String::from("# QueueEngine\r\n");
                            info_text.push_str(&format!(
                                "delayed_tasks:{}\r\n",
                                stats.delayed_tasks_count
                            ));

                            info_text.push_str("# Queues\r\n");
                            for (q, len) in &stats.queue_lengths {
                                info_text.push_str(&format!(
                                    "queue_{}:{};in_flight:{};dlq:{}\r\n",
                                    q,
                                    len,
                                    stats.in_flight_counts.get(q).unwrap_or(&0),
                                    stats.dlq_counts.get(q).unwrap_or(&0)
                                ));
                            }

                            socket
                                .write_all(&resp_bulk_string(info_text.as_bytes()))
                                .await?;
                        }

                        Command::Unknown => {
                            socket
                                .write_all(&resp_error("unknown command or syntax error"))
                                .await?;
                        }
                    }
                }
                Ok(None) => break, // Partial frame received; loop to read more socket data
                Err(_) => {
                    socket.write_all(&resp_error("protocol error")).await?;
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Protocol parse error",
                    ));
                }
            }
        }
    }
}

/// Runs replica follower synchronization loop (alias for [`start_replica_follower`]).
pub async fn run_replica_sync(primary_addr: String, engine: Arc<QueueEngine>) {
    start_replica_follower(primary_addr, engine, None).await;
}

/// Connects to a primary server node and replicates mutation commands (`SYNC`).
///
/// Connects to `primary_addr`, transmits `AUTH` (if `master_auth` is configured),
/// transmits `*1\r\n$4\r\nSYNC\r\n`, and consumes incoming RESP mutation frames
/// (`LPUSH`, `RPOP`, `RPOPLPUSH`, `LPUSHDELAY`), applying each directly to the local
/// [`QueueEngine`]. If the connection fails or drops, automatically reconnects starting
/// after 2 seconds with exponential backoff.
pub async fn start_replica_follower(
    primary_addr: String,
    engine: Arc<QueueEngine>,
    master_auth: Option<String>,
) {
    let mut retry_delay = Duration::from_secs(2);
    let max_retry_delay = Duration::from_secs(32);

    loop {
        tracing::info!(
            "Connecting to primary replication master at {}",
            primary_addr
        );
        match TcpStream::connect(&primary_addr).await {
            Ok(mut stream) => {
                // If the primary master requires authentication, send AUTH first
                if let Some(ref pass) = master_auth {
                    let auth_frame = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass);
                    if let Err(e) = stream.write_all(auth_frame.as_bytes()).await {
                        tracing::warn!("Failed to send AUTH to primary: {:?}", e);
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                        continue;
                    }
                }

                tracing::info!("Connected to primary {}. Sending SYNC...", primary_addr);
                if let Err(e) = stream.write_all(b"*1\r\n$4\r\nSYNC\r\n").await {
                    tracing::warn!("Failed to send SYNC command to primary: {:?}", e);
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                    continue;
                }

                // Reset backoff upon successful handshake
                retry_delay = Duration::from_secs(2);
                let mut buffer = BytesMut::with_capacity(4096);
                let mut chunk = [0u8; 1024];

                loop {
                    match stream.read(&mut chunk).await {
                        Ok(0) => {
                            tracing::warn!("Primary closed replication connection");
                            break;
                        }
                        Ok(n) => {
                            buffer.extend_from_slice(&chunk[..n]);

                            // Discard status lines such as "+SYNC OK\r\n"
                            while buffer.starts_with(b"+") || buffer.starts_with(b"-") {
                                if let Some(pos) = buffer.windows(2).position(|w| w == b"\r\n") {
                                    buffer.advance(pos + 2);
                                } else {
                                    break;
                                }
                            }

                            while !buffer.is_empty() {
                                match parse_command(&mut buffer) {
                                    Ok(Some((cmd, _))) => match cmd {
                                        Command::Lpush { queue, payload } => {
                                            engine.lpush(&queue, payload);
                                        }
                                        Command::Rpop { queue } => {
                                            engine.rpop(&queue);
                                        }
                                        Command::Rpoplpush {
                                            source,
                                            destination,
                                        } => {
                                            engine.rpoplpush(&source, &destination);
                                        }
                                        Command::LpushDelay {
                                            queue,
                                            delay_secs,
                                            payload,
                                        } => {
                                            let duration =
                                                Duration::from_secs_f64(delay_secs.max(0.0));
                                            engine.lpush_delayed(&queue, duration, payload);
                                        }
                                        _ => {
                                            tracing::debug!(
                                                "Ignored non-mutation command in replica follower: {:?}",
                                                cmd
                                            );
                                        }
                                    },
                                    Ok(None) => break,
                                    Err(e) => {
                                        tracing::warn!(
                                            "Protocol parse error in replication stream: {:?}",
                                            e
                                        );
                                        buffer.clear();
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Replication read error from primary: {:?}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to connect to primary {}: {:?}", primary_addr, e);
            }
        }

        tracing::info!(
            "Replication connection lost. Reconnecting in {:?}...",
            retry_delay
        );
        tokio::time::sleep(retry_delay).await;
        retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
    }
}
