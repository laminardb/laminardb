//! WebSocket sink connector — server mode.
//!
//! [`WebSocketSinkServer`] hosts a WebSocket endpoint that connected clients
//! subscribe to for streaming query results. Implements per-client isolation
//! via the [`FanoutManager`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::fanout::FanoutManager;
use super::protocol::{ClientMessage, ServerMessage};
use super::serializer::BatchSerializer;
use super::sink_config::{SinkMode, SlowClientPolicy, WebSocketSinkConfig};
use super::sink_metrics::WebSocketSinkMetrics;

/// WebSocket sink connector in server mode.
///
/// Hosts a WebSocket server. Connected clients subscribe and receive
/// streaming query results via the fan-out manager.
pub struct WebSocketSinkServer {
    /// Configuration.
    config: WebSocketSinkConfig,
    /// Input Arrow schema.
    schema: SchemaRef,
    /// Serializer for `RecordBatch` → JSON/Binary.
    serializer: BatchSerializer,
    /// Fan-out manager for per-client message distribution.
    fanout: Arc<FanoutManager>,
    /// Connector state.
    state: ConnectorState,
    /// Metrics.
    metrics: Arc<WebSocketSinkMetrics>,
    /// Current epoch.
    current_epoch: u64,
    /// Shutdown signal sender.
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Acceptor task handle.
    acceptor_handle: Option<tokio::task::JoinHandle<()>>,
    /// Global sequence counter (shared with fanout).
    sequence: Arc<AtomicU64>,
}

impl WebSocketSinkServer {
    /// Creates a new WebSocket sink server connector.
    #[must_use]
    pub fn new(schema: SchemaRef, config: WebSocketSinkConfig) -> Self {
        let serializer = BatchSerializer::new(config.format.clone());

        let (buffer_capacity, policy, replay_size) = match &config.mode {
            SinkMode::Server {
                per_client_buffer,
                slow_client_policy,
                replay_buffer_size,
                ..
            } => {
                // Convert bytes to approximate message count (assume ~256 bytes/msg).
                let msg_capacity = (*per_client_buffer / 256).max(1);
                (
                    msg_capacity,
                    slow_client_policy.clone(),
                    *replay_buffer_size,
                )
            }
            SinkMode::Client { .. } => (1024, SlowClientPolicy::DropOldest, None),
        };

        let fanout = Arc::new(FanoutManager::new(policy, buffer_capacity, replay_size));

        Self {
            config,
            schema,
            serializer,
            fanout,
            state: ConnectorState::Created,
            metrics: Arc::new(WebSocketSinkMetrics::new()),
            current_epoch: 0,
            shutdown_tx: None,
            acceptor_handle: None,
            sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns the number of connected clients.
    #[must_use]
    pub fn connected_clients(&self) -> usize {
        self.fanout.client_count()
    }

    /// Returns a reference to the fan-out manager.
    #[must_use]
    pub fn fanout(&self) -> &Arc<FanoutManager> {
        &self.fanout
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl SinkConnector for WebSocketSinkServer {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // If config has properties, re-parse (supports runtime config via SQL WITH).
        if !config.properties().is_empty() {
            self.config = WebSocketSinkConfig::from_config(config)?;
        }

        let (bind_address, max_connections, _path, ping_interval, ping_timeout) = match &self
            .config
            .mode
        {
            SinkMode::Server {
                bind_address,
                max_connections,
                path,
                ping_interval,
                ping_timeout,
                ..
            } => (
                bind_address.clone(),
                *max_connections,
                path.clone(),
                *ping_interval,
                *ping_timeout,
            ),
            SinkMode::Client { .. } => {
                return Err(ConnectorError::ConfigurationError(
                        "WebSocketSinkServer is for server mode; use WebSocketSinkClient for client mode".into(),
                    ));
            }
        };

        info!(
            bind = %bind_address,
            max_connections,
            format = ?self.config.format,
            "opening WebSocket sink server"
        );

        let listener = TcpListener::bind(&bind_address).await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!("failed to bind {bind_address}: {e}"))
        })?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let fanout = Arc::clone(&self.fanout);
        let metrics = Arc::clone(&self.metrics);
        let _ = (ping_interval, ping_timeout); // reserved for heartbeat

        let handle = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;

            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                if fanout.client_count() >= max_connections {
                                    warn!(addr = %addr, "rejecting: max_connections exceeded");
                                    drop(stream);
                                    continue;
                                }

                                let _ = stream.set_nodelay(true);
                                let fanout = Arc::clone(&fanout);
                                let metrics = metrics.clone();
                                let mut client_shutdown = shutdown_rx.clone();

                                tokio::spawn(async move {
                                    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                                        Ok(ws) => ws,
                                        Err(e) => {
                                            warn!(addr = %addr, error = %e, "handshake failed");
                                            return;
                                        }
                                    };

                                    let (mut write, mut read) = ws_stream.split();

                                    // Wait for a subscribe message or auto-subscribe.
                                    let sub_id = format!("sub_{}", addr.port());
                                    let filter = None;

                                    // Check for initial subscribe message (with timeout).
                                    let (filter, last_seq) = match tokio::time::timeout(
                                        std::time::Duration::from_secs(5),
                                        read.next(),
                                    )
                                    .await
                                    {
                                        Ok(Some(Ok(tungstenite::Message::Text(text)))) => {
                                            match serde_json::from_str::<ClientMessage>(text.as_ref()) {
                                                Ok(ClientMessage::Subscribe {
                                                    filter,
                                                    last_sequence,
                                                    ..
                                                }) => (filter, last_sequence),
                                                _ => (None, None),
                                            }
                                        }
                                        _ => (filter, None),
                                    };

                                    // Register the client.
                                    let (client_id, mut rx) =
                                        fanout.add_client(sub_id.clone(), filter, None);

                                    metrics.record_connect();

                                    // Send subscription confirmation.
                                    let confirm = ServerMessage::Subscribed {
                                        subscription_id: sub_id.clone(),
                                    };
                                    if let Ok(json) = serde_json::to_string(&confirm) {
                                        let _ = write
                                            .send(tungstenite::Message::Text(json.into()))
                                            .await;
                                    }

                                    // Replay if requested.
                                    if let Some(seq) = last_seq {
                                        let replay_msgs = fanout.replay_from(seq);
                                        metrics.record_replay();
                                        for (_seq, data) in replay_msgs {
                                            if write
                                                .send(tungstenite::Message::Text(
                                                    String::from_utf8_lossy(&data).into_owned().into(),
                                                ))
                                                .await
                                                .is_err()
                                            {
                                                break;
                                            }
                                        }
                                    }

                                    // Fan-out loop: forward messages to this client.
                                    loop {
                                        tokio::select! {
                                            Some(data) = rx.recv() => {
                                                let text = String::from_utf8_lossy(&data).into_owned();
                                                if write.send(tungstenite::Message::Text(text.into())).await.is_err() {
                                                    break;
                                                }
                                                metrics.record_send(data.len() as u64);
                                            }
                                            msg = read.next() => {
                                                match msg {
                                                    Some(Ok(tungstenite::Message::Close(_))) | None => break,
                                                    Some(Ok(tungstenite::Message::Text(text))) => {
                                                        // Handle unsubscribe.
                                                        if let Ok(ClientMessage::Unsubscribe { .. }) =
                                                            serde_json::from_str::<ClientMessage>(text.as_ref())
                                                        {
                                                            break;
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                            }
                                            _ = client_shutdown.changed() => break,
                                        }
                                    }

                                    fanout.remove_client(client_id);
                                    metrics.record_disconnect();
                                    debug!(addr = %addr, "sink client disconnected");
                                });
                            }
                            Err(e) => {
                                warn!(error = %e, "accept error");
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("sink server acceptor shutting down");
                        break;
                    }
                }
            }
        });

        self.shutdown_tx = Some(shutdown_tx);
        self.acceptor_handle = Some(handle);
        self.state = ConnectorState::Running;

        info!(bind = %bind_address, "WebSocket sink server started");
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        if self.fanout.client_count() == 0 {
            // No clients connected — discard.
            return Ok(WriteResult::new(0, 0));
        }

        // Serialize the batch to JSON.
        let json = self.serializer.serialize_to_json(batch)?;
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;

        let msg = ServerMessage::Data {
            subscription_id: String::new(), // broadcast to all
            data: json,
            sequence: seq,
            watermark: None,
        };

        let serialized = serde_json::to_string(&msg)
            .map_err(|e| ConnectorError::Serde(crate::error::SerdeError::Json(e.to_string())))?;

        let bytes_len = serialized.len() as u64;
        let data = Bytes::from(serialized);

        let result = self.fanout.broadcast(data);

        self.metrics.record_send(bytes_len);
        if result.dropped > 0 {
            for _ in 0..result.dropped {
                self.metrics.record_drop();
            }
        }

        debug!(
            records = batch.num_rows(),
            sent = result.sent,
            dropped = result.dropped,
            sequence = result.sequence,
            "broadcast batch to WebSocket clients"
        );

        Ok(WriteResult::new(batch.num_rows(), bytes_len))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => HealthStatus::Healthy,
            ConnectorState::Created | ConnectorState::Initializing => HealthStatus::Unknown,
            ConnectorState::Paused => HealthStatus::Degraded("connector paused".into()),
            ConnectorState::Recovering => HealthStatus::Degraded("recovering".into()),
            ConnectorState::Closed => HealthStatus::Unhealthy("closed".into()),
            ConnectorState::Failed => HealthStatus::Unhealthy("failed".into()),
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        SinkConnectorCapabilities::default()
        // WebSocket sink: no exactly-once, no upsert, no changelog
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.current_epoch = epoch;
        Ok(())
    }

    async fn commit_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing WebSocket sink server");

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        if let Some(handle) = self.acceptor_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }

        self.state = ConnectorState::Closed;
        info!("WebSocket sink server closed");
        Ok(())
    }
}

impl std::fmt::Debug for WebSocketSinkServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSinkServer")
            .field("state", &self.state)
            .field("connected_clients", &self.connected_clients())
            .field("format", &self.config.format)
            .field("current_epoch", &self.current_epoch)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::sink_config::SinkFormat;
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn test_config() -> WebSocketSinkConfig {
        WebSocketSinkConfig {
            mode: SinkMode::Server {
                bind_address: "127.0.0.1:0".into(),
                path: None,
                max_connections: 100,
                per_client_buffer: 262_144,
                slow_client_policy: SlowClientPolicy::DropOldest,
                ping_interval: std::time::Duration::from_secs(30),
                ping_timeout: std::time::Duration::from_secs(10),
                enable_subscription_filter: false,
                replay_buffer_size: None,
            },
            format: SinkFormat::Json,
            auth: None,
        }
    }

    #[test]
    fn test_new() {
        let sink = WebSocketSinkServer::new(test_schema(), test_config());
        assert_eq!(sink.state(), ConnectorState::Created);
        assert_eq!(sink.connected_clients(), 0);
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let sink = WebSocketSinkServer::new(schema.clone(), test_config());
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn test_capabilities() {
        let sink = WebSocketSinkServer::new(test_schema(), test_config());
        let caps = sink.capabilities();
        assert!(!caps.exactly_once);
        assert!(!caps.upsert);
    }

    #[test]
    fn test_health_created() {
        let sink = WebSocketSinkServer::new(test_schema(), test_config());
        assert_eq!(sink.health_check(), HealthStatus::Unknown);
    }

    #[test]
    fn test_metrics_initial() {
        let sink = WebSocketSinkServer::new(test_schema(), test_config());
        let m = sink.metrics();
        assert_eq!(m.records_total, 0);
    }
}
