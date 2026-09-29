use clap::Parser;
use distributed_task_queue::server::{self, Server};

#[derive(Parser, Debug)]
#[command(author, version, about = "High-performance distributed task queue server", long_about = None)]
struct Args {
    /// Port / bind address
    #[arg(short, long, default_value = "127.0.0.1:6379")]
    bind: String,

    /// Path to Append-Only-File (AOF) persistence storage
    #[arg(short, long, default_value = "queue_persistence.aof")]
    aof: String,

    /// Address of primary node to replicate from (<host:port>)
    #[arg(long)]
    replicaof: Option<String>,

    /// Require clients to issue AUTH <password> before executing commands
    #[arg(long)]
    requirepass: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    tracing::info!("Starting Distributed Task Queue engine on {}", args.bind);

    let server = Server::with_requirepass(&args.bind, &args.aof, args.requirepass)?;

    // If --replicaof is specified, spawn follower synchronization loop
    if let Some(primary_addr) = args.replicaof {
        let engine = server.get_engine();
        tracing::info!("Starting replica follower connecting to primary {}", primary_addr);
        tokio::spawn(async move {
            server::start_replica_follower(primary_addr, engine).await;
        });
    }

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

    // Ensure persistence log is cleanly flushed to disk before process exit
    server.flush_aof()?;
    tracing::info!("Persistence AOF ledger flushed. Goodbye!");

    Ok(())
}
