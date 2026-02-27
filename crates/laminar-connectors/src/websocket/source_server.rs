//! WebSocket source connector — server mode.
//!
//! [`WebSocketSourceServer`] listens on a TCP port and accepts incoming
//! WebSocket connections from clients pushing data (e.g., `IoT` sensors,
//! browser events).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, info, warn};

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::checkpoint::WebSocketSourceCheckpoint;
use super::metrics::WebSocketSourceMetrics;
use super::parser::MessageParser;
use super::source_config::{SourceMode, WebSocketSourceConfig};

/// WebSocket source connector in server mode.
///
/// Binds a TCP listener and accepts incoming WebSocket connections.
/// All clients' messages feed into the same bounded channel for
/// `poll_batch()` consumption.
pub struct WebSocketSourceServer {
    /// Parsed configuration.
    config: WebSocketSourceConfig,
    /// Output Arrow schema.
    schema: SchemaRef,
    /// Message parser.
    parser: MessageParser,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Metrics.
    metrics: WebSocketSourceMetrics,
    /// Checkpoint state.
    checkpoint_state: WebSocketSourceCheckpoint,
    /// Bounded channel receiver for messages from client handler tasks.
    rx: Option<mpsc::Receiver<Vec<u8>>>,
    /// Shutdown signal sender.
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Handle to the spawned acceptor task.
    acceptor_handle: Option<tokio::task::JoinHandle<()>>,
    /// Message buffer between polls.
    message_buffer: Vec<Vec<u8>>,
    /// Connected client count (shared with acceptor task).
    connected_clients: Arc<AtomicU64>,
    /// Maximum records per batch.
    max_batch_size: usize,
    /// Notification handle signalled when data arrives from client handler tasks.
    data_ready: Arc<Notify>,
}

impl WebSocketSourceServer {
    /// Creates a new WebSocket source connector in server mode.
    #[must_use]
    pub fn new(schema: SchemaRef, config: WebSocketSourceConfig) -> Self {
        let parser = MessageParser::new(schema.clone(), config.format.clone());

        Self {
            config,
            schema,
            parser,
            state: ConnectorState::Created,
            metrics: WebSocketSourceMetrics::new(),
            checkpoint_state: WebSocketSourceCheckpoint::default(),
            rx: None,
            shutdown_tx: None,
            acceptor_handle: None,
            message_buffer: Vec::new(),
            connected_clients: Arc::new(AtomicU64::new(0)),
            max_batch_size: 1000,
            data_ready: Arc::new(Notify::new()),
        }
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns the number of currently connected clients.
    #[must_use]
    pub fn connected_clients(&self) -> u64 {
        self.connected_clients.load(Ordering::Relaxed)
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl SourceConnector for WebSocketSourceServer {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // If config has properties, re-parse (supports runtime config via SQL WITH).
        if !config.properties().is_empty() {
            self.config = WebSocketSourceConfig::from_config(config)?;
        }

        let (bind_address, max_connections, _path) = match &self.config.mode {
            SourceMode::Server {
                bind_address,
                max_connections,
                path,
            } => (bind_address.clone(), *max_connections, path.clone()),
            SourceMode::Client { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "WebSocketSourceServer is for server mode; use WebSocketSource for client mode"
                        .into(),
                ));
            }
        };

        info!(
            bind = %bind_address,
            max_connections,
            format = ?self.config.format,
            "opening WebSocket source connector (server mode)"
        );

        let listener = TcpListener::bind(&bind_address).await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!("failed to bind {bind_address}: {e}"))
        })?;

        let channel_capacity = 10_000;
        let (tx, rx) = mpsc::channel(channel_capacity);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let connected = Arc::clone(&self.connected_clients);
        let max_msg_size = self.config.max_message_size;
        let data_ready = Arc::clone(&self.data_ready);

        let handle = tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;

            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                let current = connected.load(Ordering::Relaxed);
                                if current >= max_connections as u64 {
                                    warn!(
                                        current_connections = current,
                                        max = max_connections,
                                        addr = %addr,
                                        "rejecting connection: max_connections exceeded"
                                    );
                                    drop(stream);
                                    continue;
                                }

                                // Set TCP_NODELAY for low latency.
                                let _ = stream.set_nodelay(true);

                                let tx = tx.clone();
                                let connected = Arc::clone(&connected);
                                let mut client_shutdown = shutdown_rx.clone();
                                let data_ready = Arc::clone(&data_ready);

                                connected.fetch_add(1, Ordering::Relaxed);
                                debug!(addr = %addr, "accepted WebSocket client");

                                tokio::spawn(async move {
                                    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
                                        Ok(ws) => ws,
                                        Err(e) => {
                                            warn!(addr = %addr, error = %e, "WebSocket handshake failed");
                                            connected.fetch_sub(1, Ordering::Relaxed);
                                            return;
                                        }
                                    };

                                    let (_write, mut read) = ws_stream.split();

                                    loop {
                                        tokio::select! {
                                            msg = read.next() => {
                                                match msg {
                                                    Some(Ok(tungstenite::Message::Text(text))) => {
                                                        let payload = text.as_bytes().to_vec();
                                                        if payload.len() <= max_msg_size {
                                                            if tx.send(payload).await.is_err() {
                                                                break;
                                                            }
                                                            data_ready.notify_one();
                                                        }
                                                    }
                                                    Some(Ok(tungstenite::Message::Binary(data))) => {
                                                        let payload = data.to_vec();
                                                        if payload.len() <= max_msg_size {
                                                            if tx.send(payload).await.is_err() {
                                                                break;
                                                            }
                                                            data_ready.notify_one();
                                                        }
                                                    }
                                                    Some(Ok(tungstenite::Message::Close(_))) | None => break,
                                                    Some(Ok(_)) => {} // Ping/Pong handled by tungstenite
                                                    Some(Err(e)) => {
                                                        debug!(addr = %addr, error = %e, "client read error");
                                                        break;
                                                    }
                                                }
                                            }
                                            _ = client_shutdown.changed() => break,
                                        }
                                    }

                                    connected.fetch_sub(1, Ordering::Relaxed);
                                    debug!(addr = %addr, "client disconnected");
                                });
                            }
                            Err(e) => {
                                warn!(error = %e, "accept error");
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("acceptor shutting down");
                        break;
                    }
                }
            }
        });

        self.rx = Some(rx);
        self.shutdown_tx = Some(shutdown_tx);
        self.acceptor_handle = Some(handle);
        self.state = ConnectorState::Running;

        info!(bind = %bind_address, "WebSocket source server started");
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        let rx = self
            .rx
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "channel initialized".into(),
                actual: "channel is None".into(),
            })?;

        let limit = max_records.min(self.max_batch_size);

        // Non-blocking drain: pull all available messages from the channel.
        // The pipeline coordinator handles wake-up timing via data_ready_notify().
        while self.message_buffer.len() < limit {
            match rx.try_recv() {
                Ok(payload) => {
                    self.metrics.record_message(payload.len() as u64);
                    self.message_buffer.push(payload);
                }
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break
                }
            }
        }

        self.metrics
            .set_connected_clients(self.connected_clients.load(Ordering::Relaxed));

        if self.message_buffer.is_empty() {
            return Ok(None);
        }

        let refs: Vec<&[u8]> = self.message_buffer.iter().map(Vec::as_slice).collect();
        let batch = self.parser.parse_batch(&refs).inspect_err(|_e| {
            self.metrics.record_parse_error();
        })?;

        let num_rows = batch.num_rows();
        self.message_buffer.clear();

        self.checkpoint_state.watermark = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        debug!(
            records = num_rows,
            clients = self.connected_clients(),
            "polled batch from WebSocket server source"
        );
        Ok(Some(SourceBatch::new(batch)))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.checkpoint_state.to_source_checkpoint(0)
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        info!(
            epoch = checkpoint.epoch(),
            "restoring WebSocket source server from checkpoint (best-effort)"
        );
        self.checkpoint_state = WebSocketSourceCheckpoint::from_source_checkpoint(checkpoint);
        warn!("WebSocket source server restored; data gap expected (non-replayable)");
        Ok(())
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

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing WebSocket source server");

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        if let Some(handle) = self.acceptor_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }

        self.rx = None;
        self.state = ConnectorState::Closed;
        info!("WebSocket source server closed");
        Ok(())
    }
}

impl std::fmt::Debug for WebSocketSourceServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSourceServer")
            .field("state", &self.state)
            .field("connected_clients", &self.connected_clients())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::source_config::MessageFormat;
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn test_config() -> WebSocketSourceConfig {
        WebSocketSourceConfig {
            mode: SourceMode::Server {
                bind_address: "127.0.0.1:0".into(),
                max_connections: 100,
                path: None,
            },
            format: MessageFormat::Json,
            on_backpressure: super::super::backpressure::BackpressureStrategy::Block,
            event_time_field: None,
            event_time_format: None,
            max_message_size: 64 * 1024 * 1024,
            auth: None,
        }
    }

    #[test]
    fn test_new() {
        let server = WebSocketSourceServer::new(test_schema(), test_config());
        assert_eq!(server.state(), ConnectorState::Created);
        assert_eq!(server.connected_clients(), 0);
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let server = WebSocketSourceServer::new(schema.clone(), test_config());
        assert_eq!(server.schema(), schema);
    }

    #[test]
    fn test_health_created() {
        let server = WebSocketSourceServer::new(test_schema(), test_config());
        assert_eq!(server.health_check(), HealthStatus::Unknown);
    }
}
