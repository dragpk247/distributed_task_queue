#![deny(clippy::all)]

pub mod protocol;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::fs::{OpenOptions, File};
use std::io::{Write, Read, BufReader};
use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use protocol::{parse_command, Command};

const AOF_PATH: &str = "queue_persistence.aof";

struct EngineState {
    queues: HashMap<String, VecDeque<Vec<u8>>>,
    aof_file: File,
}

impl EngineState {
    /// Append a mutation transaction directly onto the physical disk block storage media
    fn log_to_aof(&mut self, command_bytes: &[u8]) {
        if let Err(e) = self.aof_file.write_all(command_bytes) {
            tracing::error!("CRITICAL: Failed writing metadata serialization transaction to storage track: {:?}", e);
        }
        let _ = self.aof_file.flush();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    // 1. Initialize or load the disk ledger log
    let aof_file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(AOF_PATH)?;

    let mut initial_queues = HashMap::new();
    replay_aof_log(&mut initial_queues)?;

    let state = Arc::new(RwLock::new(EngineState {
        queues: initial_queues,
        aof_file,
    }));

    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("RESP Task Queue Server active [PERSISTENCE LAYER LOADED] listening on: {}", addr);

    loop {
        let (socket, client_addr) = listener.accept().await?;
        tracing::info!("New connection established from client segment: {}", client_addr);
        
        let local_state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, local_state).await {
                tracing::error!("Connection runtime exception dropped: {:?}", e);
            }
        });
    }
}

/// Parse and replay the physical text file at engine launch to reconstruct server state
fn replay_aof_log(queues: &mut HashMap<String, VecDeque<Vec<u8>>>) -> std::io::Result<()> {
    let file = OpenOptions::new().read(true).open(AOF_PATH);
    let Ok(f) = file else { return Ok(()); }; // If no file exists, state is clean
    
    let mut reader = BufReader::new(f);
    let mut file_bytes = Vec::new();
    reader.read_to_end(&mut file_bytes)?;
    
    let mut buffer = bytes::BytesMut::from(&file_bytes[..]);
    let mut recovered_count = 0;

    while !buffer.is_empty() {
        match parse_command(&mut buffer) {
            Ok(Some(Command::Lpush { queue, payload })) => {
                queues.entry(queue).or_default().push_back(payload);
                recovered_count += 1;
            }
            Ok(Some(Command::Rpoplpush { source, destination })) => {
                if let Some(payload) = queues.get_mut(&source).and_then(|q| q.pop_front()) {
                    queues.entry(destination).or_default().push_back(payload);
                    recovered_count += 1;
                }
            }
            Ok(Some(_)) => {} // Ignores non-mutating actions (like PING)
            Ok(None) => break, // EOF reached inside buffer array stream
            Err(_) => {
                tracing::error!("Corrupted entry detected inside the persistence log stream. Recovery cut short.");
                break;
            }
        }
    }
    
    if recovered_count > 0 {
        tracing::info!("Successfully replayed persistence layer ledger. Recovered {} actions.", recovered_count);
    }
    Ok(())
}

async fn handle_client(mut socket: TcpStream, state: Arc<RwLock<EngineState>>) -> tokio::io::Result<()> {
    let mut buffer = bytes::BytesMut::with_capacity(4096);

    loop {
        let mut chunk = [0u8; 1024];
        let bytes_read = socket.read(&mut chunk).await?;
        
        if bytes_read == 0 {
            return Ok(());
        }

        buffer.extend_from_slice(&chunk[..bytes_read]);

        while !buffer.is_empty() {
            // Keep track of the buffer size to compute exactly how many bytes the current command occupied
            let pre_parse_len = buffer.len();
            
            match parse_command(&mut buffer) {
                Ok(Some(command)) => {
                    let post_parse_len = buffer.len();
                    let command_size = pre_parse_len - post_parse_len;

                    match command {
                        Command::Ping => {
                            socket.write_all(b"+PONG\r\n").await?;
                        }
                        Command::Lpush { queue, payload } => {
                            // Recover raw command bytes from the read stream history to log them to disk
                            let raw_bytes = &buffer.as_ref()[..command_size]; 
                            {
                                let mut lock = state.write();
                                lock.queues.entry(queue.clone()).or_default().push_back(payload);
                                lock.log_to_aof(raw_bytes);
                            }
                            socket.write_all(b"+OK\r\n").await?;
                        }
                        Command::Rpop { queue } => {
                            let maybe_payload = {
                                let mut lock = state.write();
                                lock.queues.get_mut(&queue).and_then(|q| q.pop_front())
                            };

                            if let Some(payload) = maybe_payload {
                                let response = format!("${}\r\n", payload.len());
                                socket.write_all(response.as_bytes()).await?;
                                socket.write_all(&payload).await?;
                                socket.write_all(b"\r\n").await?;
                                continue;
                            }
                            socket.write_all(b"$-1\r\n").await?; 
                        }
                        Command::Rpoplpush { source, destination } => {
                            let raw_bytes = &buffer.as_ref()[..command_size];
                            let maybe_payload = {
                                let mut lock = state.write();
                                if let Some(payload) = lock.queues.get_mut(&source).and_then(|q| q.pop_front()) {
                                    lock.queues.entry(destination.clone()).or_default().push_back(payload.clone());
                                    lock.log_to_aof(raw_bytes);
                                    Some(payload)
                                } else {
                                    None
                                }
                            };

                            if let Some(payload) = maybe_payload {
                                let response = format!("${}\r\n", payload.len());
                                socket.write_all(response.as_bytes()).await?;
                                socket.write_all(&payload).await?;
                                socket.write_all(b"\r\n").await?;
                            } else {
                                socket.write_all(b"$-1\r\n").await?;
                            }
                        }
                        Command::Unknown => {
                            socket.write_all(b"-ERR unknown command or parameter mismatched configuration\r\n").await?;
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    socket.write_all(b"-ERR protocol layout structural error\r\n").await?;
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, err));
                }
            }
        }
    }
}
