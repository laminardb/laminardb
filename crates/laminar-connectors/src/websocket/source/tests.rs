use super::super::source_config::MessageFormat;
use super::*;
use crate::connector::DeliveryGuarantee;
use arrow_array::{Array, BinaryArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn test_config() -> WebSocketSourceConfig {
    WebSocketSourceConfig {
        urls: vec!["ws://localhost:9090".into()],
        subscribe_message: None,
        reconnect: ReconnectConfig::default(),
        format: MessageFormat::Json,
        on_backpressure: WsBackpressure::Block,
        max_message_size: 64 * 1024 * 1024,
    }
}

#[test]
fn test_new_defaults() {
    let source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );
    assert_eq!(source.state, ConnectorState::Created);
}

#[test]
fn test_schema_returned() {
    let schema = test_schema();
    let source = WebSocketSource::new(
        schema.clone(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );
    assert_eq!(source.schema(), schema);
}

#[test]
fn test_checkpoint_empty() {
    let source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );
    let cp = source.checkpoint();
    assert!(cp.is_empty());
}

#[tokio::test]
async fn terminal_tracker_waits_for_reader_exit_after_source_drop() {
    let mut source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );
    let tracker = source.terminal_task_tracker().unwrap();
    let task_guard = source.task_owner.track().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    source.reader_handle = Some(tokio::task::spawn_blocking(move || {
        let _task_guard = task_guard;
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    }));
    started_rx.await.unwrap();

    drop(source);

    assert!(!tracker.is_terminated());
    release_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), tracker.wait_terminated())
        .await
        .expect("source reader guard did not resolve after task exit");
}

#[test]
fn client_source_requires_singleton_placement() {
    let source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );
    let contract = source.contract(&ConnectorConfig::new("websocket")).unwrap();
    assert_eq!(contract.consistency, SourceConsistency::Ephemeral);
    assert_eq!(contract.topology, SourceTopology::Singleton);
}

#[tokio::test]
async fn drop_newest_counts_the_drop_without_a_spurious_wakeup() {
    let (tx, _rx) = mpsc::bounded_async::<BufferedMessage>(1);
    let byte_budget = Arc::new(Semaphore::new(2));
    let permit = Arc::clone(&byte_budget).acquire_owned().await.unwrap();
    assert!(tx
        .try_send(BufferedMessage {
            payload: Bytes::from_static(&[1]),
            _permit: permit,
        })
        .is_ok());
    let notify = Notify::new();
    let metrics = WebSocketSourceMetrics::local();
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    send_with_backpressure(
        &tx,
        Bytes::from_static(&[2]),
        &WsBackpressure::DropNewest,
        &notify,
        &metrics,
        &byte_budget,
        &mut shutdown_rx,
    )
    .await
    .unwrap();

    assert_eq!(metrics.backpressure_drops.get(), 1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn terminal_reader_error_is_reported_outside_the_bounded_channel() {
    let (tx, rx) = mpsc::bounded_async::<BufferedMessage>(1);
    drop(tx);
    let mut source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );
    source.rx = Some(rx);
    source.state = ConnectorState::Running;
    publish_terminal(
        &source.terminal_error,
        &source.data_ready,
        "reconnect budget exhausted".into(),
    );

    let error = source.poll_batch(1).await.unwrap_err().to_string();

    assert!(error.contains("reconnect budget exhausted"), "{error}");
    assert_eq!(source.state, ConnectorState::Failed);
}

#[tokio::test]
async fn ack_then_close_sessions_exhaust_reconnect_budget() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            websocket
                .send(tungstenite::Message::Text(
                    r#"{"type":"subscribed"}"#.into(),
                ))
                .await
                .unwrap();
            websocket
                .send(tungstenite::Message::Close(None))
                .await
                .unwrap();
        }
        3
    });

    let mut config = test_config();
    config.urls = vec![format!("ws://{address}")];
    config.reconnect = ReconnectConfig {
        enabled: true,
        initial_delay: std::time::Duration::from_millis(1),
        max_delay: std::time::Duration::from_millis(1),
        max_retries: Some(2),
    };
    let mut source = WebSocketSource::new(test_schema(), config, WebSocketSourceMetrics::local());
    source
        .start(
            SourceStart::new(
                ConnectorConfig::new("websocket"),
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let error = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match source.poll_batch(1).await {
                Err(error) => break error,
                Ok(None) => source.data_ready.notified().await,
                Ok(Some(_)) => {}
            }
        }
    })
    .await
    .expect("source did not exhaust its reconnect budget")
    .to_string();

    assert!(error.contains("no more retries"), "{error}");
    assert_eq!(source.metrics.reconnect_count.get(), 2);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), server)
            .await
            .expect("flapping server did not finish")
            .unwrap(),
        3
    );
    source.close().await.unwrap();
}

#[tokio::test]
async fn runtime_config_always_rebuilds_the_parser() {
    let binary_schema = Schema::new(vec![Field::new("payload", DataType::Binary, false)]);
    let mut config = ConnectorConfig::new("websocket");
    config.set("url", "ws://127.0.0.1:9");
    config.set("format", "binary");
    config.set("reconnect.enabled", "false");
    config.set(
        "_arrow_schema",
        crate::config::encode_arrow_schema_ipc(&binary_schema),
    );

    let mut source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );
    source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let batch = source.parser.parse_batch(&[b"payload"]).unwrap();
    let payload = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(payload.value(0), b"payload");
    source.close().await.unwrap();
}

#[tokio::test]
async fn second_start_is_rejected_without_replacing_the_reader() {
    let mut source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );
    let request = || {
        SourceStart::new(
            ConnectorConfig::new("websocket"),
            SourcePosition::Initial,
            DeliveryGuarantee::BestEffort,
        )
        .unwrap()
    };
    source.start(request()).await.unwrap();

    let error = source.start(request()).await.unwrap_err();

    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    assert_eq!(source.state, ConnectorState::Running);
    source.close().await.unwrap();
}

#[tokio::test]
async fn drop_without_close_terminates_the_reader_socket() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
        accepted_tx.send(()).unwrap();
        let _ = websocket.next().await;
    });
    let mut config = test_config();
    config.urls = vec![format!("ws://{address}")];
    let mut source = WebSocketSource::new(test_schema(), config, WebSocketSourceMetrics::local());
    source
        .start(
            SourceStart::new(
                ConnectorConfig::new("websocket"),
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    accepted_rx.await.unwrap();

    drop(source);

    tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn corrupt_runtime_schema_fails_before_reader_start() {
    let mut config = ConnectorConfig::new("websocket");
    config.set("url", "ws://127.0.0.1:9");
    config.set("_arrow_schema", "not-arrow-ipc");
    let mut source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );

    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("_arrow_schema"), "{error}");
    assert!(source.reader_handle.is_none());
    assert_eq!(source.state, ConnectorState::Created);
}

#[tokio::test]
async fn missing_runtime_schema_fails_before_reader_start() {
    let mut config = ConnectorConfig::new("websocket");
    config.set("url", "ws://127.0.0.1:9");
    let mut source = WebSocketSource::new(
        Arc::new(Schema::empty()),
        WebSocketSourceConfig::default(),
        WebSocketSourceMetrics::local(),
    );

    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("declared Arrow schema"), "{error}");
    assert!(source.reader_handle.is_none());
}

#[tokio::test]
async fn binary_schema_mismatch_fails_before_reader_start() {
    let schema = Schema::new(vec![Field::new("payload", DataType::Utf8, false)]);
    let mut config = ConnectorConfig::new("websocket");
    config.set("url", "ws://127.0.0.1:9");
    config.set("format", "binary");
    config.set(
        "_arrow_schema",
        crate::config::encode_arrow_schema_ipc(&schema),
    );
    let mut source = WebSocketSource::new(
        test_schema(),
        test_config(),
        WebSocketSourceMetrics::local(),
    );

    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, ConnectorError::SchemaMismatch(_)));
    assert!(source.reader_handle.is_none());
}

#[tokio::test]
async fn typed_source_limits_are_validated_before_reader_start() {
    let mut typed_config = test_config();
    typed_config.max_message_size = INGRESS_BUFFER_BYTES + 1;
    let mut source =
        WebSocketSource::new(test_schema(), typed_config, WebSocketSourceMetrics::local());

    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("websocket"),
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(source.reader_handle.is_none());
}
