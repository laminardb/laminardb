//! WebSocket sink connector — server mode.
//!
//! [`WebSocketSinkServer`] hosts a WebSocket endpoint that connected clients
//! subscribe to for streaming query results. A shared bounded broadcast ring
//! disconnects lagging clients instead of allocating per-client payload copies.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;

use super::fanout::FanoutManager;
use super::protocol::{ClientMessage, ServerMessage};
use super::serializer::BatchSerializer;
use super::sink_config::WebSocketSinkConfig;
use super::sink_metrics::WebSocketSinkMetrics;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONTROL_MESSAGE_BYTES: usize = 16 * 1024;

/// WebSocket sink connector in server mode.
///
/// Hosts a WebSocket server. Connected clients subscribe and receive
/// streaming query results via the fan-out manager.
pub struct WebSocketSinkServer {
    /// Configuration.
    config: WebSocketSinkConfig,
    /// Input Arrow schema.
    schema: SchemaRef,
    /// Serializer for `RecordBatch` → JSON.
    serializer: BatchSerializer,
    /// Fan-out manager for per-client message distribution.
    fanout: Arc<FanoutManager>,
    /// Connector state.
    state: ConnectorState,
    /// Metrics.
    metrics: Arc<WebSocketSinkMetrics>,
    /// Shutdown signal sender.
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Acceptor task handle.
    acceptor_handle: Option<tokio::task::JoinHandle<()>>,
}

impl WebSocketSinkServer {
    /// Creates a new WebSocket sink server connector.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        config: WebSocketSinkConfig,
        metrics: WebSocketSinkMetrics,
    ) -> Self {
        let serializer = BatchSerializer::new(schema.clone());
        Self {
            config,
            schema,
            serializer,
            fanout: Arc::new(FanoutManager::new()),
            state: ConnectorState::Created,
            metrics: Arc::new(metrics),
            shutdown_tx: None,
            acceptor_handle: None,
        }
    }

    /// Returns the number of connected clients.
    #[must_use]
    pub fn connected_clients(&self) -> usize {
        self.fanout.client_count()
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl SinkConnector for WebSocketSinkServer {
    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let cfg = if config.properties().is_empty() {
            self.config.clone()
        } else {
            WebSocketSinkConfig::from_config(config)?
        };
        cfg.validate()?;
        if !matches!(cfg, WebSocketSinkConfig::Server { .. }) {
            return Err(ConnectorError::ConfigurationError(
                "WebSocketSinkServer requires mode = 'server'".into(),
            ));
        }
        Ok(SinkContract::new(
            SinkConsistency::Ephemeral,
            SinkTopology::NodeLocalEgress,
            SinkInputMode::AppendOnly,
        ))
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if !matches!(self.state, ConnectorState::Created | ConnectorState::Closed) {
            return Err(ConnectorError::InvalidState {
                expected: "Created or Closed".into(),
                actual: self.state.to_string(),
            });
        }

        let effective_config = if config.properties().is_empty() {
            self.config.clone()
        } else {
            WebSocketSinkConfig::from_config(config)?
        };
        effective_config.validate()?;

        let (bind_address, max_connections, ping_interval, ping_timeout) = match &effective_config {
            WebSocketSinkConfig::Server {
                bind_address,
                max_connections,
                ping_interval,
                ping_timeout,
                ..
            } => (
                bind_address.clone(),
                *max_connections,
                *ping_interval,
                *ping_timeout,
            ),
            WebSocketSinkConfig::Client { .. } => {
                return Err(ConnectorError::ConfigurationError(
                        "WebSocketSinkServer is for server mode; use WebSocketSinkClient for client mode".into(),
                    ));
            }
        };
        self.state = ConnectorState::Initializing;

        info!(
            bind = %bind_address,
            max_connections,
            "opening WebSocket sink server"
        );

        let listener = match TcpListener::bind(&bind_address).await {
            Ok(listener) => listener,
            Err(error) => {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::ConnectionFailed(format!(
                    "failed to bind {bind_address}: {error}"
                )));
            }
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.config = effective_config;
        self.fanout = Arc::new(FanoutManager::new());
        let fanout = Arc::clone(&self.fanout);
        let metrics = Arc::clone(&self.metrics);
        let connection_slots = Arc::new(tokio::sync::Semaphore::new(max_connections));

        let handle = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            let mut clients = tokio::task::JoinSet::new();

            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                let Ok(permit) = Arc::clone(&connection_slots).try_acquire_owned() else {
                                    warn!(addr = %addr, "rejecting: max_connections exceeded");
                                    drop(stream);
                                    continue;
                                };

                                let _ = stream.set_nodelay(true);
                                let fanout = Arc::clone(&fanout);
                                let metrics = metrics.clone();
                                let mut client_shutdown = shutdown_rx.clone();
                                let client_ping_interval = ping_interval;
                                let client_ping_timeout = ping_timeout;

                                clients.spawn(async move {
                                    let _permit = permit;
                                    // Clients only send small subscription and heartbeat controls.
                                    let mut ws_config = tungstenite::protocol::WebSocketConfig::default();
                                    ws_config.max_message_size = Some(MAX_CONTROL_MESSAGE_BYTES);
                                    ws_config.max_frame_size = Some(MAX_CONTROL_MESSAGE_BYTES);
                                    let handshake = tokio::time::timeout(
                                        HANDSHAKE_TIMEOUT,
                                        tokio_tungstenite::accept_async_with_config(stream, Some(ws_config)),
                                    );
                                    let ws_stream = match tokio::select! {
                                        result = handshake => result,
                                        _ = client_shutdown.changed() => return,
                                    } {
                                        Ok(Ok(ws)) => ws,
                                        Ok(Err(error)) => {
                                            warn!(addr = %addr, error = %error, "handshake failed");
                                            return;
                                        }
                                        Err(_) => {
                                            warn!(addr = %addr, "handshake timed out");
                                            return;
                                        }
                                    };

                                    let (mut write, mut read) = ws_stream.split();

                                    // Require an explicit subscription before allocating fanout state.
                                    let subscription = tokio::select! {
                                        result = tokio::time::timeout(HANDSHAKE_TIMEOUT, read.next()) => result,
                                        _ = client_shutdown.changed() => return,
                                    };
                                    match subscription {
                                        Ok(Some(Ok(tungstenite::Message::Text(text)))) => {
                                            match serde_json::from_str::<ClientMessage>(text.as_ref()) {
                                                Ok(ClientMessage::Subscribe {}) => {}
                                                Ok(ClientMessage::Unsubscribe { .. }) => {
                                                    warn!(addr = %addr, "unsubscribe received before subscription, rejecting");
                                                    return;
                                                }
                                                Err(error) => {
                                                    warn!(addr = %addr, error = %error, "invalid subscription control message, rejecting");
                                                    return;
                                                }
                                            }
                                        }
                                        Ok(Some(Err(e))) => {
                                            warn!(addr = %addr, error = %e, "client read error during subscribe, rejecting");
                                            return;
                                        }
                                        Ok(Some(Ok(tungstenite::Message::Close(_))) | None) => return,
                                        Ok(Some(Ok(_))) => {
                                            warn!(addr = %addr, "non-text subscription control message, rejecting");
                                            return;
                                        }
                                        Err(_) => {
                                            warn!(addr = %addr, "subscription control timed out, rejecting");
                                            return;
                                        }
                                    }

                                    let (client_id, mut rx) = fanout.subscribe();
                                    let sub_id = format!("sub_{client_id}");

                                    // Send subscription confirmation.
                                    let confirm = ServerMessage::Subscribed {
                                        subscription_id: sub_id.clone(),
                                    };
                                    let Ok(json) = serde_json::to_string(&confirm) else {
                                        return;
                                    };
                                    let confirmation = tokio::select! {
                                        result = tokio::time::timeout(
                                            CLIENT_WRITE_TIMEOUT,
                                            write.send(tungstenite::Message::Text(json.into())),
                                        ) => result,
                                        _ = client_shutdown.changed() => return,
                                    };
                                    if !matches!(
                                        confirmation,
                                        Ok(Ok(()))
                                    )
                                    {
                                        return;
                                    }
                                    let _connection_metrics = metrics.connection_guard();

                                    // Fan-out loop with ping/pong heartbeats.
                                    let mut ping_ticker = tokio::time::interval(client_ping_interval);
                                    ping_ticker.tick().await; // consume initial immediate tick
                                    let mut awaiting_pong = false;
                                    let pong_deadline = tokio::time::sleep(std::time::Duration::from_secs(86_400));
                                    tokio::pin!(pong_deadline);

                                    loop {
                                        tokio::select! {
                                            data = rx.recv() => {
                                                match data {
                                                    Ok(data) => {
                                                        let data_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                                                        let delivered = tokio::select! {
                                                            result = tokio::time::timeout(
                                                                CLIENT_WRITE_TIMEOUT,
                                                                write.send(tungstenite::Message::Text(data)),
                                                            ) => matches!(result, Ok(Ok(()))),
                                                            _ = client_shutdown.changed() => break,
                                                        };
                                                        if !delivered {
                                                            metrics.record_delivery_failure(1);
                                                            break;
                                                        }
                                                        metrics.record_send(data_len);
                                                    }
                                                    Err(tokio::sync::broadcast::error::RecvError::Lagged(frames)) => {
                                                        metrics.record_drops(frames);
                                                        warn!(addr = %addr, frames, "lagging WebSocket client disconnected");
                                                        break;
                                                    }
                                                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                                                }
                                            }
                                            msg = read.next() => {
                                                match msg {
                                                    Some(Ok(tungstenite::Message::Close(_))) | None => break,
                                                    Some(Ok(tungstenite::Message::Pong(_))) => {
                                                        awaiting_pong = false;
                                                    }
                                                    Some(Ok(tungstenite::Message::Text(text))) => {
                                                        match serde_json::from_str::<ClientMessage>(text.as_ref()) {
                                                            Ok(ClientMessage::Unsubscribe { subscription_id })
                                                                if subscription_id == sub_id => break,
                                                            Ok(_) | Err(_) => {
                                                                warn!(addr = %addr, "invalid active subscription control message, disconnecting");
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    Some(Ok(tungstenite::Message::Ping(data))) => {
                                                        if !matches!(
                                                            tokio::time::timeout(
                                                                CLIENT_WRITE_TIMEOUT,
                                                                write.send(tungstenite::Message::Pong(data)),
                                                            ).await,
                                                            Ok(Ok(()))
                                                        ) {
                                                            break;
                                                        }
                                                    }
                                                    Some(Err(e)) => {
                                                        warn!(addr = %addr, error = %e, "client read error, disconnecting");
                                                        break;
                                                    }
                                                    Some(Ok(_)) => {
                                                        warn!(addr = %addr, "unexpected active subscription frame, disconnecting");
                                                        break;
                                                    }
                                                }
                                            }
                                            _ = ping_ticker.tick() => {
                                                if !awaiting_pong {
                                                    if !matches!(
                                                        tokio::time::timeout(
                                                            CLIENT_WRITE_TIMEOUT,
                                                            write.send(tungstenite::Message::Ping(bytes::Bytes::new())),
                                                        ).await,
                                                        Ok(Ok(()))
                                                    ) {
                                                        break;
                                                    }
                                                    awaiting_pong = true;
                                                    pong_deadline.as_mut().reset(tokio::time::Instant::now() + client_ping_timeout);
                                                }
                                            }
                                            () = &mut pong_deadline, if awaiting_pong => {
                                                debug!(addr = %addr, "ping timeout — disconnecting");
                                                metrics.record_ping_timeout();
                                                break;
                                            }
                                            _ = client_shutdown.changed() => break,
                                        }
                                    }
                                    debug!(addr = %addr, "sink client disconnected");
                                });
                            }
                            Err(e) => {
                                warn!(error = %e, "accept error; retrying after 100 ms");
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                    Some(result) = clients.join_next(), if !clients.is_empty() => {
                        if let Err(error) = result {
                            debug!(error = %error, "WebSocket client task stopped");
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("sink server acceptor shutting down");
                        break;
                    }
                }
            }
            let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            while !clients.is_empty() {
                tokio::select! {
                    _ = clients.join_next() => {}
                    () = tokio::time::sleep_until(drain_deadline) => break,
                }
            }
            clients.abort_all();
            while clients.join_next().await.is_some() {}
        });

        self.shutdown_tx = Some(shutdown_tx);
        self.acceptor_handle = Some(handle);
        self.state = ConnectorState::Running;

        info!(bind = %bind_address, "WebSocket sink server started");
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }
        if self
            .acceptor_handle
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::WriteError(
                "WebSocket server acceptor stopped unexpectedly".into(),
            ));
        }

        if self.fanout.client_count() == 0 {
            // No live consumer requires serialization.
            return Ok(WriteResult::new(0, 0));
        }

        let rows = self.serializer.serialize_rows(batch)?;
        let result = self.fanout.publish_rows(&rows)?;
        tokio::task::yield_now().await;

        debug!(
            records = batch.num_rows(),
            frames = result.frames,
            receiver_enqueues = result.receiver_enqueues,
            payload_bytes = result.payload_bytes,
            transport_bytes = result.transport_bytes,
            sequence = result.sequence,
            "broadcast batch to WebSocket clients"
        );

        Ok(WriteResult::new(batch.num_rows(), result.transport_bytes))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        // In-memory fanout — timeout is effectively unreachable.
        Duration::from_secs(10)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing WebSocket sink server");

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        if let Some(mut handle) = self.acceptor_handle.take() {
            if tokio::time::timeout(Duration::from_secs(5), &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        self.state = ConnectorState::Closed;
        info!("WebSocket sink server closed");
        Ok(())
    }
}

impl Drop for WebSocketSinkServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown_tx.take() {
            let _ = shutdown.send(true);
        }
        if let Some(handle) = self.acceptor_handle.take() {
            handle.abort();
        }
    }
}

impl std::fmt::Debug for WebSocketSinkServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSinkServer")
            .field("state", &self.state)
            .field("connected_clients", &self.connected_clients())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
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
    async fn malformed_initial_control_message_never_subscribes() {
        let address = reserved_address().await;
        let mut config = test_config();
        if let WebSocketSinkConfig::Server { bind_address, .. } = &mut config {
            *bind_address = address.to_string();
        }
        let mut sink =
            WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
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
        let mut sink =
            WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
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
        let mut sink =
            WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
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
        let mut sink =
            WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
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
        let mut sink =
            WebSocketSinkServer::new(test_schema(), config, WebSocketSinkMetrics::local());
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
            }
        });

        for _ in 0..300 {
            sink.write_batch(&test_batch()).await.unwrap();
        }

        reader.await.unwrap();
        sink.close().await.unwrap();
    }
}
