use distributed_task_queue::server::Server;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
    client
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$4\r\ntodo\r\n$5\r\ntask1\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    let _ = client.read(&mut buf).await.unwrap();

    client
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$4\r\ntodo\r\n$5\r\ntask2\r\n")
        .await
        .unwrap();
    let _ = client.read(&mut buf).await.unwrap();

    // Trigger BGREWRITEAOF
    client
        .write_all(b"*1\r\n$12\r\nBGREWRITEAOF\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.starts_with("+Background append only file rewriting"));

    let _ = std::fs::remove_file(&aof_path);
}

#[tokio::test]
async fn test_lease_heartbeat_and_ack_tcp() {
    let (addr, aof_path) = start_test_server(16382).await;

    let mut client = TcpStream::connect(&addr).await.unwrap();

    // 1. Push a task
    client
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$8\r\npayments\r\n$11\r\ninvoice_101\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 128];
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b":1\r\n");

    // 2. Lease the task via RPOPLEASE payments 10 (10s visibility)
    client
        .write_all(b"*3\r\n$9\r\nRPOPLEASE\r\n$8\r\npayments\r\n$2\r\n10\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);
    assert!(resp.contains("task-1"));
    assert!(resp.contains("invoice_101"));

    // 3. Heartbeat / renew lease via TASKTOUCH payments task-1 30
    client
        .write_all(b"*4\r\n$9\r\nTASKTOUCH\r\n$8\r\npayments\r\n$6\r\ntask-1\r\n$2\r\n30\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");

    // 4. Acknowledge task completion via TASKACK payments task-1
    client
        .write_all(b"*3\r\n$7\r\nTASKACK\r\n$8\r\npayments\r\n$6\r\ntask-1\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");

    // 5. Subsequent ACK should fail with error since task is no longer in-flight
    client
        .write_all(b"*3\r\n$7\r\nTASKACK\r\n$8\r\npayments\r\n$6\r\ntask-1\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains("-ERR Task ID not found in-flight"));

    let _ = std::fs::remove_file(&aof_path);
}

#[tokio::test]
async fn test_auth_session_handling() {
    let addr = "127.0.0.1:16383";
    let aof_path = std::env::temp_dir().join("test_server_16383.aof");
    let _ = std::fs::remove_file(&aof_path);

    let server = Server::with_requirepass(
        addr,
        aof_path.to_str().unwrap(),
        Some("supersecret".to_string()),
    )
    .unwrap();
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 256];

    // 1. PING should be permitted without authentication
    client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+PONG\r\n");

    // 2. Other commands (LPUSH) should be rejected with NOAUTH
    client
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$4\r\ndata\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"-NOAUTH Authentication required.\r\n");

    // 3. Incorrect password AUTH should fail with WRONGPASS
    client
        .write_all(b"*2\r\n$4\r\nAUTH\r\n$8\r\nwrongpwd\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b"-WRONGPASS invalid username-password pair or token\r\n"
    );

    // 4. Correct password AUTH succeeds with OK
    client
        .write_all(b"*2\r\n$4\r\nAUTH\r\n$11\r\nsupersecret\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");

    // 5. Subsequent commands now succeed
    client
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$5\r\nqueue\r\n$4\r\ndata\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b":1\r\n");

    // 6. Connect to server without password requirement and send AUTH
    let (noauth_addr, noauth_aof) = start_test_server(16384).await;
    let mut noauth_client = TcpStream::connect(&noauth_addr).await.unwrap();
    noauth_client
        .write_all(b"*2\r\n$4\r\nAUTH\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();
    let n = noauth_client.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b"-ERR Client sent AUTH, but no password is set\r\n"
    );

    let _ = std::fs::remove_file(&aof_path);
    let _ = std::fs::remove_file(&noauth_aof);
}

#[tokio::test]
async fn test_delayed_task_polling_integration() {
    let (addr, aof_path) = start_test_server(16385).await;
    let mut client = TcpStream::connect(&addr).await.unwrap();
    let mut buf = [0u8; 256];

    // Push task with 150ms delay
    client
        .write_all(
            b"*4\r\n$10\r\nLPUSHDELAY\r\n$6\r\nfuture\r\n$4\r\n0.15\r\n$11\r\nhello_delay\r\n",
        )
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b":1\r\n"); // Returned task ID 1

    // Immediately pop -> should be empty (None / $-1\r\n)
    client
        .write_all(b"*2\r\n$4\r\nRPOP\r\n$6\r\nfuture\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"$-1\r\n");

    // Sleep 200ms to allow the 50ms polling loop to promote the task
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Pop again -> should now return the promoted payload
    client
        .write_all(b"*2\r\n$4\r\nRPOP\r\n$6\r\nfuture\r\n")
        .await
        .unwrap();
    let n = client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"$11\r\nhello_delay\r\n");

    let _ = std::fs::remove_file(&aof_path);
}

#[tokio::test]
async fn test_replica_follower_sync_mode() {
    let (primary_addr, primary_aof) = start_test_server(16386).await;

    // Start a replica server on another port
    let replica_addr = "127.0.0.1:16387";
    let replica_aof = std::env::temp_dir().join("test_server_16387.aof");
    let _ = std::fs::remove_file(&replica_aof);

    let replica_server = Server::new(replica_addr, replica_aof.to_str().unwrap()).unwrap();
    let replica_engine = replica_server.get_engine();

    // Spawn replica follower loop connecting to primary
    let primary_addr_clone = primary_addr.clone();
    let engine_clone = replica_engine.clone();
    let follower_handle = tokio::spawn(async move {
        distributed_task_queue::server::start_replica_follower(
            primary_addr_clone,
            engine_clone,
            None,
        )
        .await;
    });

    // Spawn replica server
    tokio::spawn(async move {
        let _ = replica_server.run().await;
    });

    // Allow connection and handshake to establish
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Send LPUSH to primary
    let mut primary_client = TcpStream::connect(&primary_addr).await.unwrap();
    primary_client
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$11\r\nreplicatedq\r\n$9\r\nsync_item\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 128];
    let n = primary_client.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b":1\r\n");

    // Wait briefly for replication frame to be received and processed by replica
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify replica engine received the replicated item
    let item = replica_engine.rpop("replicatedq");
    assert_eq!(item, Some(b"sync_item".to_vec()));

    follower_handle.abort();
    let _ = std::fs::remove_file(&primary_aof);
    let _ = std::fs::remove_file(&replica_aof);
}

#[tokio::test]
async fn test_info_command() {
    let (addr, aof_path) = start_test_server(16388).await;
    let mut client = TcpStream::connect(&addr).await.unwrap();

    // Push an item and a delayed task
    client
        .write_all(b"*3\r\n$5\r\nLPUSH\r\n$7\r\nmyqueue\r\n$5\r\nitem1\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 512];
    let _ = client.read(&mut buf).await.unwrap();

    // Query INFO
    client.write_all(b"*1\r\n$4\r\nINFO\r\n").await.unwrap();
    let n = client.read(&mut buf).await.unwrap();
    let resp = String::from_utf8_lossy(&buf[..n]);

    assert!(resp.contains("# QueueEngine"));
    assert!(resp.contains("myqueue"));

    let _ = std::fs::remove_file(&aof_path);
}

#[tokio::test]
async fn test_http_metrics_and_dashboard_integration() {
    use distributed_task_queue::engine::QueueEngine;
    use distributed_task_queue::http_server::start_http_server;
    use std::sync::Arc;

    let engine = Arc::new(QueueEngine::new());
    engine.lpush("web_queue", b"task_alpha".to_vec());
    engine.lpush("web_queue", b"task_beta".to_vec());
    let _ = engine.rpop_with_lease("web_queue", "lease-1".to_string(), Duration::from_secs(30));

    // Force an item into DLQ
    engine.lpush("failing_q", b"poison_pill".to_vec());
    for i in 1..=3 {
        let tid = format!("t-fail-{}", i);
        let _ = engine.rpop_with_lease("failing_q", tid.clone(), Duration::from_secs(30));
        assert!(engine.task_nack("failing_q", &tid));
    }

    let http_port = 19091;
    let http_addr = format!("127.0.0.1:{}", http_port);
    let engine_clone = Arc::clone(&engine);
    tokio::spawn(async move {
        start_http_server(http_addr, engine_clone).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 1. Test /metrics endpoint
    {
        let mut client = TcpStream::connect(format!("127.0.0.1:{}", http_port))
            .await
            .unwrap();
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        client.read_to_end(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp);

        assert!(resp_str.starts_with("HTTP/1.1 200 OK"));
        assert!(resp_str.contains("dtq_queue_size{queue=\"web_queue\"} 1"));
        assert!(resp_str.contains("dtq_in_flight_tasks{queue=\"web_queue\"} 1"));
        assert!(resp_str.contains("dtq_dead_letter_queue_size{queue=\"failing_q\"} 1"));
    }

    // 2. Test / or /dashboard endpoint
    {
        let mut client = TcpStream::connect(format!("127.0.0.1:{}", http_port))
            .await
            .unwrap();
        client
            .write_all(b"GET /dashboard HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        client.read_to_end(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp);

        assert!(resp_str.starts_with("HTTP/1.1 200 OK"));
        assert!(resp_str.contains("Distributed Task Queue Dashboard"));
        assert!(resp_str.contains("Queue Administration"));
    }

    // 3. Test /api/stats endpoint
    {
        let mut client = TcpStream::connect(format!("127.0.0.1:{}", http_port))
            .await
            .unwrap();
        client
            .write_all(b"GET /api/stats HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        client.read_to_end(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp);

        assert!(resp_str.starts_with("HTTP/1.1 200 OK"));
        assert!(resp_str.contains("\"total_dlq\":1"));
        assert!(resp_str.contains("\"total_in_flight\":1"));
        assert!(resp_str.contains("\"name\":\"web_queue\""));
        assert!(resp_str.contains("\"name\":\"failing_q\""));
    }

    // 4. Test /api/dlq/requeue endpoint
    {
        let mut client = TcpStream::connect(format!("127.0.0.1:{}", http_port))
            .await
            .unwrap();
        client
            .write_all(b"POST /api/dlq/requeue?queue=failing_q HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        client.read_to_end(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp);

        assert!(resp_str.starts_with("HTTP/1.1 200 OK"));
        assert!(resp_str.contains(r#""requeued":1"#));

        // Verify DLQ is now empty and queue has ready item
        let stats = engine.get_stats();
        assert_eq!(stats.dlq_counts.get("failing_q"), None);
        assert_eq!(stats.queue_lengths.get("failing_q"), Some(&1));
    }
}
