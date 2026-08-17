use super::super::source_config::ReconnectConfig;
use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn test_config() -> WebSocketSinkConfig {
    WebSocketSinkConfig::Client {
        url: "ws://localhost:9090".into(),
    }
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

fn two_row_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["one", "two"])),
        ],
    )
    .unwrap()
}

#[test]
fn test_new() {
    let sink =
        WebSocketSinkClient::new(test_schema(), test_config(), WebSocketSinkMetrics::local());
    assert_eq!(sink.state, ConnectorState::Created);
    assert!(sink.ws_sink.is_none());
}

#[test]
fn test_schema_returned() {
    let schema = test_schema();
    let sink =
        WebSocketSinkClient::new(schema.clone(), test_config(), WebSocketSinkMetrics::local());
    assert_eq!(sink.schema(), schema);
}

#[tokio::test]
async fn terminal_tracker_waits_for_client_reader_exit_after_sink_drop() {
    let mut sink =
        WebSocketSinkClient::new(test_schema(), test_config(), WebSocketSinkMetrics::local());
    let tracker = sink.terminal_task_tracker().unwrap();
    let task_guard = sink.task_owner.track().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    sink.reader_handle = Some(tokio::task::spawn_blocking(move || {
        let _task_guard = task_guard;
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    }));
    started_rx.await.unwrap();

    drop(sink);

    assert!(!tracker.is_terminated());
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), tracker.wait_terminated())
        .await
        .expect("client reader guard did not resolve after task exit");
}

#[tokio::test]
async fn failed_reconnect_is_retried_on_a_later_call() {
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);

    let reconnect = ReconnectConfig {
        enabled: true,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(2),
        max_retries: Some(4),
    };
    let url = format!("ws://{address}");
    let config = WebSocketSinkConfig::Client { url: url.clone() };
    let mut sink = WebSocketSinkClient::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.conn_mgr = Some(ConnectionManager::new(vec![url], reconnect));
    sink.next_reconnect_at = Some(tokio::time::Instant::now());

    assert!(!sink.reconnect_if_due().await.unwrap());
    assert!(!sink.reconnect_exhausted);

    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tokio_tungstenite::accept_async(stream).await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(3)).await;
    assert!(sink.reconnect_if_due().await.unwrap());
    assert_eq!(sink.conn_mgr.as_ref().unwrap().attempt(), 1);
    drop(server.await.unwrap());
    sink.close().await.unwrap();
}

#[tokio::test]
async fn background_reader_drives_peer_ping_pong() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        websocket
            .send(tungstenite::Message::Ping(bytes::Bytes::from_static(
                b"probe",
            )))
            .await
            .unwrap();
        loop {
            if let tungstenite::Message::Pong(payload) = websocket.next().await.unwrap().unwrap() {
                return payload;
            }
        }
    });

    let config = WebSocketSinkConfig::Client {
        url: format!("ws://{address}"),
    };
    let mut sink = WebSocketSinkClient::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();

    let pong = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("client must service control frames while idle")
        .unwrap();
    assert_eq!(pong.as_ref(), b"probe");
    sink.close().await.unwrap();
}

#[tokio::test]
async fn batch_write_delivers_one_frame_per_row() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut rows = Vec::new();
        while rows.len() < 2 {
            if let tungstenite::Message::Text(text) = websocket.next().await.unwrap().unwrap() {
                rows.push(serde_json::from_str::<serde_json::Value>(text.as_ref()).unwrap());
            }
        }
        rows
    });
    let config = WebSocketSinkConfig::Client {
        url: format!("ws://{address}"),
    };
    let mut sink = WebSocketSinkClient::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();

    let result = sink.write_batch(&two_row_batch()).await.unwrap();
    let rows = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.records_written, 2);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[1]["value"], "two");
    sink.close().await.unwrap();
}

#[tokio::test]
async fn immediate_peer_close_balances_the_connection_gauge() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        websocket.close(None).await.unwrap();
    });
    let metrics = WebSocketSinkMetrics::local();
    let mut sink = WebSocketSinkClient::new(
        test_schema(),
        WebSocketSinkConfig::Client {
            url: format!("ws://{address}"),
        },
        metrics.clone(),
    );

    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();
    server.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while metrics.connected_clients.get() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(metrics.client_disconnects.get(), 1);
    sink.close().await.unwrap();
}

#[tokio::test]
async fn dropping_without_close_aborts_the_reader_and_balances_metrics() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _ = websocket.next().await;
    });
    let metrics = WebSocketSinkMetrics::local();
    let mut sink = WebSocketSinkClient::new(
        test_schema(),
        WebSocketSinkConfig::Client {
            url: format!("ws://{address}"),
        },
        metrics.clone(),
    );
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();
    assert_eq!(metrics.connected_clients.get(), 1);

    drop(sink);

    tokio::time::timeout(Duration::from_secs(2), async {
        while metrics.connected_clients.get() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metrics.client_disconnects.get(), 1);
}

#[tokio::test]
async fn empty_flush_does_not_reset_reconnect_backoff() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _ = websocket.next().await;
    });
    let mut sink = WebSocketSinkClient::new(
        test_schema(),
        WebSocketSinkConfig::Client {
            url: format!("ws://{address}"),
        },
        WebSocketSinkMetrics::local(),
    );
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();
    assert!(sink.conn_mgr.as_mut().unwrap().next_backoff().is_some());
    assert_eq!(sink.conn_mgr.as_ref().unwrap().attempt(), 1);

    sink.flush().await.unwrap();

    assert_eq!(sink.conn_mgr.as_ref().unwrap().attempt(), 1);
    sink.close().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn second_open_is_rejected_without_replacing_the_reader() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _ = websocket.next().await;
    });
    let mut sink = WebSocketSinkClient::new(
        test_schema(),
        WebSocketSinkConfig::Client {
            url: format!("ws://{address}"),
        },
        WebSocketSinkMetrics::local(),
    );
    sink.open(&ConnectorConfig::new("websocket")).await.unwrap();

    let error = sink
        .open(&ConnectorConfig::new("websocket"))
        .await
        .unwrap_err();

    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    sink.close().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn disconnected_sink_rejects_writes_and_flushes() {
    let reconnect = ReconnectConfig {
        enabled: false,
        ..ReconnectConfig::default()
    };
    let url = "ws://127.0.0.1:9".to_string();
    let config = WebSocketSinkConfig::Client { url: url.clone() };
    let mut sink = WebSocketSinkClient::new(test_schema(), config, WebSocketSinkMetrics::local());
    sink.conn_mgr = Some(ConnectionManager::new(vec![url], reconnect));
    sink.state = ConnectorState::Running;

    let write_error = sink.write_batch(&test_batch()).await.unwrap_err();
    let flush_error = sink.flush().await.unwrap_err();

    assert!(matches!(write_error, ConnectorError::WriteError(_)));
    assert!(matches!(flush_error, ConnectorError::WriteError(_)));
}

#[test]
fn test_contract() {
    let sink =
        WebSocketSinkClient::new(test_schema(), test_config(), WebSocketSinkMetrics::local());
    let contract = sink.contract(&ConnectorConfig::new("websocket")).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
    assert_eq!(contract.topology, SinkTopology::Singleton);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(10));
}
