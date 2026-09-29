use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use bytes::BytesMut;

use crate::aof::AofManager;
use crate::engine::QueueEngine;
use crate::protocol::{
    parse_command, resp_array, resp_bulk_string, resp_error, resp_integer, resp_null,
    resp_simple_string, Command,
};

pub struct ServerContext {
    pub engine: Arc<QueueEngine>,
    pub aof: Arc<AofManager>,
    /// Broadcast channel streaming raw mutations to any connected replicas
    pub replica_stream: broadcast::Sender<Vec<u8>>,
}

pub struct Server {
    addr: String,
    context: Arc<ServerContext>,
}

impl Server {
    pub fn new(addr: &str, aof_path: &str) -> std::io::Result<Self> {
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
        });

        Ok(Self {
            addr: addr.to_string(),
            context,
        })
    }

    pub fn get_engine(&self) -> Arc<QueueEngine> {
        Arc::clone(&self.context.engine)
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        tracing::info!("Server listening on {}", self.addr);

        // Spawn background reaper for expired visibility timeouts
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

        loop {
            let (socket, client_addr) = listener.accept().await?;
            tracing::debug!("New connection from: {}", client_addr);

            let ctx = Arc::clone(&self.context);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, ctx).await {
                    tracing::debug!("Connection closed with error: {:?}", e);
                }
            });
        }
    }
}

pub async fn handle_connection(
    mut socket: TcpStream,
    ctx: Arc<ServerContext>,
) -> tokio::io::Result<()> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        let mut chunk = [0u8; 1024];
        let bytes_read = socket.read(&mut chunk).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        buffer.extend_from_slice(&chunk[..bytes_read]);

        while !buffer.is_empty() {
            match parse_command(&mut buffer) {
                Ok(Some((command, raw_frame))) => {
                    match command {
                        Command::Ping => {
                            socket.write_all(&resp_simple_string("PONG")).await?;
                        }
                        Command::Lpush { queue, payload } => {
                            let len = ctx.engine.lpush(&queue, payload);
                            let _ = ctx.aof.append(&raw_frame);
                            let _ = ctx.replica_stream.send(raw_frame);
                            socket.write_all(&resp_integer(len as i64)).await?;
                        }
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
                        Command::Rpoplpush { source, destination } => {
                            let maybe_item = ctx.engine.rpoplpush(&source, &destination);
                            if let Some(item) = maybe_item {
                                let _ = ctx.aof.append(&raw_frame);
                                let _ = ctx.replica_stream.send(raw_frame);
                                socket.write_all(&resp_bulk_string(&item)).await?;
                            } else {
                                socket.write_all(&resp_null()).await?;
                            }
                        }
                        Command::Brpop {
                            queues,
                            timeout_secs,
                        } => {
                            let timeout = Duration::from_secs_f64(timeout_secs);
                            let maybe_item = ctx.engine.brpop(&queues, timeout).await;
                            if let Some((matched_queue, item)) = maybe_item {
                                // Record pop into AOF
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
                        Command::Brpoplpush {
                            source,
                            destination,
                            timeout_secs,
                        } => {
                            let timeout = Duration::from_secs_f64(timeout_secs);
                            let rx = ctx.engine.brpop(std::slice::from_ref(&source), timeout).await;
                            if let Some((_, item)) = rx {
                                // Put to destination
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
                                        .write_all(&resp_error(&format!("AOF rewrite failed: {}", e)))
                                        .await?;
                                }
                            }
                        }
                        Command::Sync => {
                            // Replica stream handler
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
                        Command::Unknown => {
                            socket
                                .write_all(&resp_error("unknown command or syntax error"))
                                .await?;
                        }
                    }
                }
                Ok(None) => break,
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
