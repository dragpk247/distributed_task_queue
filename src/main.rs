#![deny(clippy::all)]

pub mod protocol;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use protocol::{parse_command, Command};

struct EngineState {
    queues: HashMap<String, VecDeque<Vec<u8>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("RESP Task Queue Server active and listening on: {}", addr);

    let state = Arc::new(RwLock::new(EngineState {
        queues: HashMap::new(),
    }));

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

async fn handle_client(mut socket: TcpStream, state: Arc<RwLock<EngineState>>) -> tokio::io::Result<()> {
    let mut buffer = bytes::BytesMut::with_capacity(4096);

    loop {
        let mut chunk = [0u8; 1024];
        let bytes_read = socket.read(&mut chunk).await?;
        
        if bytes_read == 0 {
            tracing::info!("Client disconnected cleanly.");
            return Ok(());
        }

        buffer.extend_from_slice(&chunk[..bytes_read]);

        // Process loop to clear all complete pipelined command frames out of the buffer matrix
        while !buffer.is_empty() {
            match parse_command(&mut buffer) {
                Ok(Some(command)) => {
                    match command {
                        Command::Ping => {
                            socket.write_all(b"+PONG\r\n").await?;
                        }
                        Command::Lpush { queue, payload } => {
                            // Isolate the lock scope so it drops before socket write operations await
                            {
                                let mut lock = state.write();
                                lock.queues.entry(queue.clone())
                                    .or_insert_with(VecDeque::new)
                                    .push_back(payload);
                            } // Lock guard safely dropped here
                            
                            tracing::info!("Task successfully queued into lane: '{}'", queue);
                            socket.write_all(b"+OK\r\n").await?;
                        }
                        Command::Rpop { queue } => {
                            // Safely pop the payload, dropping the lock immediately
                            let maybe_payload = {
                                let mut lock = state.write();
                                lock.queues.get_mut(&queue)
                                    .and_then(|q| q.pop_front())
                            }; // Lock guard safely dropped here

                            if let Some(payload) = maybe_payload {
                                let response = format!("${}\r\n", payload.len());
                                socket.write_all(response.as_bytes()).await?;
                                socket.write_all(&payload).await?;
                                socket.write_all(b"\r\n").await?;
                                continue;
                            }
                            socket.write_all(b"$-1\r\n").await?; 
                        }
                        Command::Unknown => {
                            socket.write_all(b"-ERR unknown or unsupported command parameters\r\n").await?;
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(err) => {
                    tracing::error!("Parsing structural violation encountered: Protocol mismatch {:?}", err);
                    socket.write_all(b"-ERR protocol structural error layout violation\r\n").await?;
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, err));
                }
            }
        }
    }
}
