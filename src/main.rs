//! # Main Entry Point: Distributed Task Queue Server Binary
//!
//! Handles command-line argument parsing, logging initialization, primary/replica setup,
//! and graceful termination handling via POSIX / Ctrl+C signals.

use clap::Parser;
use distributed_task_queue::server::{self, Server};

/// CLI Configuration Options parsed from environment and flags.
#[derive(Parser, Debug)]
#[command(author, version, about = "High-performance distributed task queue server", long_about = None)]
struct Args {
    /// Network interface and TCP port to bind the server listener to
    #[arg(short, long, default_value = "127.0.0.1:6379")]
    bind: String,

    /// File path to Append-Only-File (AOF) mutation log for durable storage
    #[arg(short, long, default_value = "queue_persistence.aof")]
    aof: String,

    /// Address of primary master node to replicate mutations from (`<host:port>`)
    #[arg(long)]
    replicaof: Option<String>,

    /// Optional password authentication for authenticating with the primary master node
    #[arg(long)]
    masterauth: Option<String>,

    /// Optional password authentication requirement (`AUTH <password>`)
    #[arg(long)]
    requirepass: Option<String>,

    /// Network interface and TCP port to bind the HTTP metrics and dashboard server to (`<host:port>`)
    #[arg(long)]
    http_bind: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize structured diagnostic tracing logger
    tracing_subscriber::fmt::init();

    // 2. Parse command-line flags
    let args = Args::parse();
    tracing::info!("Starting Distributed Task Queue engine on {}", args.bind);

    // 3. Instantiate the queue server with optional password security
    let server = Server::with_requirepass(&args.bind, &args.aof, args.requirepass)?;

    // 4. If --replicaof is specified, spawn the follower replication background worker
    if let Some(primary_addr) = args.replicaof {
        let engine = server.get_engine();
        let master_auth = args.masterauth;
        tracing::info!(
            "Starting replica follower connecting to primary {}",
            primary_addr
        );
        tokio::spawn(async move {
            server::start_replica_follower(primary_addr, engine, master_auth).await;
        });
    }

    // 5. If --http-bind is specified, spawn the HTTP server for Prometheus metrics & Dashboard
    if let Some(http_addr) = args.http_bind {
        let engine = server.get_engine();
        tokio::spawn(async move {
            distributed_task_queue::http_server::start_http_server(http_addr, engine).await;
        });
    }

    // 6. Run the TCP server loop with signal interruption guarding
    tokio::select! {
        res = server.run() => {
            if let Err(e) = res {
                tracing::error!("Server encountered an error: {:?}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C interrupt, shutting down gracefully...");
        }
    }

    // 7. Ensure the append-only persistence log is cleanly flushed to disk before exit
    server.flush_aof()?;
    tracing::info!("Persistence AOF ledger flushed. Goodbye!");

    Ok(())
}
