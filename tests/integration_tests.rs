use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use distributed_task_queue::server::Server;

async fn start_test_server(port: u16) -> (String, std::path::PathBuf) {
    let addr = format!("127.0.0.1:{}", port);
    let aof_path = std::env::temp_dir().join(format!("test_server_{}.aof", port));
    let _ = std::fs::remove_file(&aof_path);

    let server = Server::new(&addr, aof_path.to_str().unwrap()).unwrap();
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Wait briefly for socket binding
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, aof_path)
}

#[tokio::test]
async fn test_full_pipeline_tcp_integration() {
    let (addr, aof_path) = start_test_server(16379).await;

    // Connect client 1 (Worker listening with BRPOP)
    let mut worker = TcpStream::connect(&addr).await.unwrap();

    let worker_handle = tokio::spawn(async move {
        // Issue BRPOP queue1 2 (2 seconds timeout)
        let brpop_cmd = b"*3\r\n$5\r\nBRPOP\r\n$6\r\nqueue1\r\n$1\r\n2\r\n";
        worker.write_all(brpop_cmd).await.unwrap();

        let mut buf = [0u8; 512];
        let n = worker.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    });

    // Give worker time to block
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect client 2 (Producer pushing item)
    let mut producer = TcpStream::connect(&addr).await.unwrap();
    let lpush_cmd = b"*3\r\n$5\r\nLPUSH\r\n$6\r\nqueue1\r\n$12\r\njob_payload1\r\n";
    producer.write_all(lpush_cmd).await.unwrap();

    let mut resp = [0u8; 64];
    let n = producer.read(&mut resp).await.unwrap();
    assert_eq!(&resp[..n], b":1\r\n"); // Returned queue length 1

    let worker_resp = worker_handle.await.unwrap();
    // RESP array response contains queue name and payload
    assert!(worker_resp.contains("queue1"));
    assert!(worker_resp.contains("job_payload1"));

    let _ = std::fs::remove_file(&aof_path);
}

#[tokio::test]
async fn test_live_replication_streaming() {
    let (addr, aof_path) = start_test_server(16380).await;

    // 1. Connect a replica client and issue SYNC
    let mut replica = TcpStream::connect(&addr).await.unwrap();
    replica.write_all(b"*1\r\n$4\r\nSYNC\r\n").await.unwrap();

    let mut sync_ack = [0u8; 64];
    let n = replica.read(&mut sync_ack).await.unwrap();
    assert_eq!(&sync_ack[..n], b"+SYNC OK\r\n");

    // 2. Connect producer and push
    let mut producer = TcpStream::connect(&addr).await.unwrap();
    let lpush_cmd = b"*3\r\n$5\r\nLPUSH\r\n$6\r\nevents\r\n$7\r\nevent_A\r\n";
    producer.write_all(lpush_cmd).await.unwrap();

    let mut prod_ack = [0u8; 64];
    let _ = producer.read(&mut prod_ack).await.unwrap();

    // 3. Verify replica immediately receives the raw mutation frame
    let mut replicated_frame = [0u8; 256];
    let n = replica.read(&mut replicated_frame).await.unwrap();
    assert_eq!(&replicated_frame[..n], lpush_cmd);

    let _ = std::fs::remove_file(&aof_path);
}

#[tokio::test]
async fn test_aof_compaction_command() {
    let (addr, aof_path) = start_test_server(16381).await;

    let mut client = TcpStream::connect(&addr).await.unwrap();
    // Push two tasks
    client.write_all(b"*3\r\n$5\r\nLPUSH\r\n$4\r\ntodo\r\n$5\r\ntask1\r\n").await.unwrap();
    let mut buf = [0u8; 64];
    let _ = client.read(&mut buf).await.unwrap();

    client.write_all(b"*3\r\n$5\r\nLPUSH\r\n$4\r\ntodo\r\n$5\r\ntask2\r\n").await.unwrap();
    let _ = client.read(&mut buf).await.unwrap();

    // Trigger BGREWRITEAOF
    client.write_all(b"*1\r\n$12\r\nBGREWRITEAOF\r\n").await.unwrap();
    let n = client.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.starts_with("+Background append only file rewriting"));

    let _ = std::fs::remove_file(&aof_path);
}
