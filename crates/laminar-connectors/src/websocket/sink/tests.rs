use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use tokio::io::AsyncWriteExt;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn test_config() -> WebSocketSinkConfig {
    WebSocketSinkConfig::Server {
        bind_address: "127.0.0.1:0".into(),
        max_connections: 100,
        ping_interval: std::time::Duration::from_secs(30),
        ping_timeout: std::time::Duration::from_secs(10),
    }
}

async fn reserved_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["one"])),
        ],
    )
    .unwrap()
}

#[test]
fn test_new() {
    let sink =
        WebSocketSinkServer::new(test_schema(), test_config(), WebSocketSinkMetrics::local());
    assert_eq!(sink.state, ConnectorState::Created);
    assert_eq!(sink.connected_clients(), 0);
}

#[test]
fn test_schema_returned() {
    let schema = test_schema();
    let sink =
        WebSocketSinkServer::new(schema.clone(), test_config(), WebSocketSinkMetrics::local());
    assert_eq!(sink.schema(), schema);
}

#[tokio::test]
async fn sink_drop_seals_client_admission_before_terminal_tasks_exit() {
    let mut sink =
        WebSocketSinkServer::new(test_schema(), test_config(), WebSocketSinkMetrics::local());
    let tracker = sink.terminal_task_tracker().unwrap();
    let task_owner = sink.task_owner.as_ref().unwrap();
    let task_admission = task_owner.admission();
    let acceptor_guard = task_owner.track().unwrap();
    let client_guard = task_admission.track().unwrap();
    let (acceptor_started_tx, acceptor_started_rx) = tokio::sync::oneshot::channel();
    let (acceptor_done_tx, acceptor_done_rx) = tokio::sync::oneshot::channel();
    let (admission_sealed_tx, admission_sealed_rx) = tokio::sync::oneshot::channel();
    let (acceptor_release_tx, acceptor_release_rx) = std::sync::mpsc::channel();
    sink.acceptor_handle = Some(tokio::task::spawn_blocking(move || {
        let _ = acceptor_started_tx.send(());
        let _ = acceptor_release_rx.recv();
        let _ = admission_sealed_tx.send(task_admission.track().is_none());
        drop(acceptor_guard);
        let _ = acceptor_done_tx.send(());
    }));
    let (client_started_tx, client_started_rx) = tokio::sync::oneshot::channel();
    let (client_release_tx, client_release_rx) = std::sync::mpsc::channel();
    let client = tokio::task::spawn_blocking(move || {
        let _ = client_started_tx.send(());
        let _ = client_release_rx.recv();
        drop(client_guard);
    });
    acceptor_started_rx.await.unwrap();
    client_started_rx.await.unwrap();

    drop(sink);

    assert!(!tracker.is_terminated());
    acceptor_release_tx.send(()).unwrap();
    assert!(admission_sealed_rx.await.unwrap());
    acceptor_done_rx.await.unwrap();
    assert!(!tracker.is_terminated());
    client_release_tx.send(()).unwrap();
    client.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), tracker.wait_terminated())
        .await
        .expect("server task guards did not resolve after acceptor and client exit");
}

#[tokio::test(start_paused = true)]
async fn close_timeout_retires_the_server_generation() {
    let mut sink =
        WebSocketSinkServer::new(test_schema(), test_config(), WebSocketSinkMetrics::local());
    let tracker = sink.terminal_task_tracker().unwrap();
    let guard = sink.task_owner.as_ref().unwrap().track().unwrap();
    sink.acceptor_handle = Some(tokio::spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    }));
    sink.state = ConnectorState::Running;

    let error = sink.close().await.unwrap_err();

    assert!(error.to_string().contains("close deadline"), "{error}");
    assert_eq!(sink.state, ConnectorState::Failed);
    assert!(sink.acceptor_handle.is_none());
    assert!(matches!(
        sink.open(&ConnectorConfig::new("websocket")).await,
        Err(ConnectorError::InvalidState { .. })
    ));
    drop(sink);
    tokio::time::timeout(Duration::from_secs(1), tracker.wait_terminated())
        .await
        .expect("aborted WebSocket server generation did not terminate");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_acceptor_completion_is_a_close_timeout() {
    let mut handle = tokio::spawn(async {
        std::thread::sleep(Duration::from_millis(25));
    });

    let outcome = wait_acceptor_until(
        &mut handle,
        tokio::time::Instant::now() + Duration::from_millis(5),
    )
    .await;

    assert!(matches!(outcome, AcceptorWait::TimedOut));
}

#[tokio::test]
async fn malformed_initial_control_message_never_subscribes() {
    let address = reserved_address().await;
    let mut config = test_config();
    if let WebSocketSinkConfig::Server { bind_address, .. } = &mut config {
        *bind_address = address.to_string();
    }
    let mut sink = WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();

    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
        .await
        .unwrap();
    client
        .send(tungstenite::Message::Text(
            r#"{"action":"subscribe","filter":"id > 1"}"#.into(),
        ))
        .await
        .unwrap();

    let response = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("server must reject malformed control promptly");
    if let Some(Ok(tungstenite::Message::Text(text))) = response {
        let value: serde_json::Value = serde_json::from_str(text.as_ref()).unwrap();
        assert_ne!(value["type"], "subscribed");
    }
    sink.close().await.unwrap();
}

#[tokio::test]
async fn close_cancels_an_incomplete_handshake() {
    let address = reserved_address().await;
    let mut config = test_config();
    if let WebSocketSinkConfig::Server { bind_address, .. } = &mut config {
        *bind_address = address.to_string();
    }
    let mut sink = WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();
    let mut stalled_client = tokio::net::TcpStream::connect(address).await.unwrap();
    stalled_client
        .write_all(b"GET / HTTP/1.1\r\n")
        .await
        .unwrap();
    tokio::task::yield_now().await;

    tokio::time::timeout(Duration::from_secs(1), sink.close())
        .await
        .expect("close must cancel pending handshakes")
        .unwrap();

    assert_eq!(sink.connected_clients(), 0);
}

#[test]
fn test_contract() {
    let sink =
        WebSocketSinkServer::new(test_schema(), test_config(), WebSocketSinkMetrics::local());
    let contract = sink.contract(&ConnectorConfig::new("websocket")).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
    assert_eq!(contract.topology, SinkTopology::NodeLocalEgress);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(10));
}

#[tokio::test]
async fn second_open_is_rejected_without_replacing_the_acceptor() {
    let address = reserved_address().await;
    let mut config = test_config();
    if let WebSocketSinkConfig::Server { bind_address, .. } = &mut config {
        *bind_address = address.to_string();
    }
    let mut sink = WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();

    let error = sink
        .open(&ConnectorConfig::new("websocket"))
        .await
        .unwrap_err();

    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    assert_eq!(sink.state, ConnectorState::Running);
    sink.close().await.unwrap();
}

#[tokio::test]
async fn drop_without_close_releases_the_listener() {
    let address = reserved_address().await;
    let mut config = test_config();
    if let WebSocketSinkConfig::Server { bind_address, .. } = &mut config {
        *bind_address = address.to_string();
    }
    let mut sink = WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();

    drop(sink);

    let rebound = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match TcpListener::bind(address).await {
                Ok(listener) => break listener,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("dropping the sink must release its listener");
    drop(rebound);
}

#[tokio::test(flavor = "current_thread")]
async fn consecutive_writes_cooperate_with_the_socket_task() {
    let address = reserved_address().await;
    let mut config = test_config();
    if let WebSocketSinkConfig::Server { bind_address, .. } = &mut config {
        *bind_address = address.to_string();
    }
    let mut sink = WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
        .await
        .unwrap();
    client
        .send(tungstenite::Message::Text(
            r#"{"action":"subscribe"}"#.into(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        client.next().await,
        Some(Ok(tungstenite::Message::Text(_)))
    ));
    let received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let reader_progress = Arc::clone(&received);
    let reader = tokio::spawn(async move {
        for expected_sequence in 1..=300 {
            let message = tokio::time::timeout(Duration::from_secs(2), client.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let tungstenite::Message::Text(text) = message else {
                panic!("expected data text frame");
            };
            let value: serde_json::Value = serde_json::from_str(text.as_ref()).unwrap();
            assert_eq!(value["sequence"], expected_sequence);
            reader_progress.store(expected_sequence, std::sync::atomic::Ordering::Release);
        }
    });

    for sequence in 1..=300 {
        sink.write_batch(&test_batch()).await.unwrap();
        if sequence % 16 == 0 {
            tokio::time::timeout(Duration::from_secs(2), async {
                while received.load(std::sync::atomic::Ordering::Acquire) < sequence {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("fast WebSocket client did not keep pace with the bounded ring");
        }
    }

    reader.await.unwrap();
    sink.close().await.unwrap();
}
