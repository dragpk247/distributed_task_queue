use clap::Parser;
use distributed_task_queue::server::Server;

#[derive(Parser, Debug)]
#[command(author, version, about = "High-performance distributed task queue server", long_about = None)]
struct Args {
    /// Port / bind address
    #[arg(short, long, default_value = "127.0.0.1:6379")]
    bind: String,

    /// Path to Append-Only-File (AOF) persistence storage
    #[arg(short, long, default_value = "queue_persistence.aof")]
    aof: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    tracing::info!("Starting Distributed Task Queue engine on {}", args.bind);

    let server = Server::new(&args.bind, &args.aof)?;
    server.run().await?;

    Ok(())
}
