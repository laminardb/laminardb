//! WebSocket source connector — client mode.
//!
//! [`WebSocketSource`] connects to an external WebSocket server (e.g., exchange
//! market data feeds) and produces Arrow `RecordBatch` data via the
//! [`SourceConnector`] trait.
//!
//! # Delivery Guarantees
//!
//! WebSocket is non-replayable. This connector provides **at-most-once** or
//! **best-effort** delivery. On recovery, data gaps should be expected.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, Notify};
use tracing::{debug, info, warn};

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::backpressure::BackpressureStrategy;
use super::checkpoint::WebSocketSourceCheckpoint;
use super::connection::ConnectionManager;
use super::metrics::WebSocketSourceMetrics;
use super::parser::MessageParser;
use super::source_config::{ReconnectConfig, SourceMode, WebSocketSourceConfig};

/// Internal channel message from the WS reader task to the connector.
enum WsMessage {
    /// A raw WebSocket message payload.
    Data(Vec<u8>),
    /// The connection was lost.
    Disconnected(String),
}

/// WebSocket source connector in client mode.
///
/// Connects to one or more external WebSocket server URLs and consumes
/// messages, converting them to Arrow `RecordBatch` data.
///
/// All WebSocket I/O runs in a spawned Tokio task (Ring 2). Parsed data
/// is delivered to `poll_batch()` via a bounded channel.
pub struct WebSocketSource {
    /// Parsed configuration.
    config: WebSocketSourceConfig,
    /// Output Arrow schema.
    schema: SchemaRef,
    /// Message parser (JSON/CSV/Binary → Arrow).
    parser: MessageParser,
    /// Connection manager for reconnection and failover.
    conn_mgr: Option<ConnectionManager>,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Metrics.
    metrics: WebSocketSourceMetrics,
    /// Checkpoint state.
    checkpoint_state: WebSocketSourceCheckpoint,
    /// Bounded channel receiver for messages from the WS reader task.
    rx: Option<mpsc::Receiver<WsMessage>>,
    /// Shutdown signal sender.
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Handle to the spawned reader task.
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    /// Buffer of raw messages accumulated between `poll_batch()` calls.
    message_buffer: Vec<Vec<u8>>,
    /// Maximum records per batch.
    max_batch_size: usize,
    /// Notification handle signalled when data arrives from the reader task.
    data_ready: Arc<Notify>,
}

impl WebSocketSource {
    /// Creates a new WebSocket source connector in client mode.
    ///
    /// # Arguments
    ///
    /// * `schema` - Arrow schema for output batches.
    /// * `config` - WebSocket source configuration.
    #[must_use]
    pub fn new(schema: SchemaRef, config: WebSocketSourceConfig) -> Self {
        let parser = MessageParser::new(schema.clone(), config.format.clone());

        Self {
            config,
            schema,
            parser,
            conn_mgr: None,
            state: ConnectorState::Created,
            metrics: WebSocketSourceMetrics::new(),
            checkpoint_state: WebSocketSourceCheckpoint::default(),
            rx: None,
            shutdown_tx: None,
            reader_handle: None,
            message_buffer: Vec::new(),
            max_batch_size: 1000,
            data_ready: Arc::new(Notify::new()),
        }
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Spawns the WebSocket reader task that connects to the server and
    /// feeds messages through the bounded channel.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::unused_self
    )]
    fn spawn_reader(
        &self,
        urls: Vec<String>,
        subscribe_message: Option<String>,
        reconnect: ReconnectConfig,
        max_message_size: usize,
        on_backpressure: BackpressureStrategy,
        tx: mpsc::Sender<WsMessage>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
        data_ready: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut conn_mgr = ConnectionManager::new(urls, reconnect);

            'outer: loop {
                // Check shutdown.
                if *shutdown_rx.borrow() {
                    break;
                }

                let url = conn_mgr.current_url().to_string();
                info!(url = %url, "connecting to WebSocket server");

                // Attempt connection.
                let ws_stream = match tokio_tungstenite::connect_async(&url).await {
                    Ok((stream, _response)) => {
                        conn_mgr.reset();
                        info!(url = %url, "WebSocket connection established");
                        stream
                    }
                    Err(e) => {
                        warn!(url = %url, error = %e, "WebSocket connection failed");
                        if let Some(delay) = conn_mgr.next_backoff() {
                            tokio::select! {
                                () = tokio::time::sleep(delay) => continue,
                                _ = shutdown_rx.changed() => break,
                            }
                        } else {
                            let _ = tx
                                .send(WsMessage::Disconnected(format!(
                                    "connection failed, no more retries: {e}"
                                )))
                                .await;
                            break;
                        }
                    }
                };

                let (mut write, mut read) = ws_stream.split();

                // Send subscription message if configured.
                if let Some(ref msg) = subscribe_message {
                    if let Err(e) = write
                        .send(tungstenite::Message::Text(msg.clone().into()))
                        .await
                    {
                        warn!(error = %e, "failed to send subscription message");
                        if let Some(delay) = conn_mgr.next_backoff() {
                            tokio::select! {
                                () = tokio::time::sleep(delay) => continue,
                                _ = shutdown_rx.changed() => break,
                            }
                        }
                        continue;
                    }
                    debug!("subscription message sent");
                }

                // Read loop.
                loop {
                    tokio::select! {
                        msg = read.next() => {
                            match msg {
                                Some(Ok(tungstenite::Message::Text(text))) => {
                                    let payload = text.as_bytes().to_vec();
                                    if payload.len() > max_message_size {
                                        warn!(size = payload.len(), max = max_message_size, "message exceeds max size, dropping");
                                        continue;
                                    }
                                    if send_with_backpressure(&tx, WsMessage::Data(payload), &on_backpressure, &data_ready).await.is_err() {
                                        break 'outer;
                                    }
                                }
                                Some(Ok(tungstenite::Message::Binary(data))) => {
                                    let payload = data.to_vec();
                                    if payload.len() > max_message_size {
                                        warn!(size = payload.len(), max = max_message_size, "message exceeds max size, dropping");
                                        continue;
                                    }
                                    if send_with_backpressure(&tx, WsMessage::Data(payload), &on_backpressure, &data_ready).await.is_err() {
                                        break 'outer;
                                    }
                                }
                                Some(Ok(tungstenite::Message::Ping(data))) => {
                                    let _ = write.send(tungstenite::Message::Pong(data)).await;
                                }
                                Some(Ok(tungstenite::Message::Close(_))) => {
                                    info!(url = %url, "server sent Close frame");
                                    break;
                                }
                                Some(Ok(_)) => {} // Pong, Frame — ignore
                                Some(Err(e)) => {
                                    warn!(url = %url, error = %e, "WebSocket read error");
                                    break;
                                }
                                None => {
                                    info!(url = %url, "WebSocket stream ended");
                                    break;
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            debug!("shutdown signal received in reader");
                            let _ = write.send(tungstenite::Message::Close(None)).await;
                            break 'outer;
                        }
                    }
                }

                // Disconnected — attempt reconnect.
                let _ = tx
                    .send(WsMessage::Disconnected(format!("disconnected from {url}")))
                    .await;

                if let Some(delay) = conn_mgr.next_backoff() {
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {},
                        _ = shutdown_rx.changed() => break,
                    }
                } else {
                    break;
                }
            }
        })
    }
}

/// Sends a message through the channel, applying the backpressure strategy
/// if the channel is full. Signals `data_ready` on successful send so the
/// pipeline coordinator wakes immediately.
///
/// Returns `Err(())` if the channel is closed (shutdown).
async fn send_with_backpressure(
    tx: &mpsc::Sender<WsMessage>,
    msg: WsMessage,
    strategy: &BackpressureStrategy,
    data_ready: &Notify,
) -> Result<(), ()> {
    let result = match strategy {
        BackpressureStrategy::Block => tx.send(msg).await.map_err(|_| ()),
        BackpressureStrategy::DropNewest => match tx.try_send(msg) {
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(()),
        },
        BackpressureStrategy::DropOldest
        | BackpressureStrategy::Buffer { .. }
        | BackpressureStrategy::Sample { .. } => {
            // For simplicity, DropOldest/Buffer/Sample all use try_send + drop.
            match tx.try_send(msg) {
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(()),
            }
        }
    };
    if result.is_ok() {
        data_ready.notify_one();
    }
    result
}

#[async_trait]
impl SourceConnector for WebSocketSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // If config has properties, re-parse (supports runtime config via SQL WITH).
        if !config.properties().is_empty() {
            self.config = WebSocketSourceConfig::from_config(config)?;
        }

        // Override schema from SQL DDL if provided.
        if let Some(schema) = config.arrow_schema() {
            info!(
                fields = schema.fields().len(),
                "using SQL-defined schema for deserialization"
            );
            self.schema = schema;
            self.parser = MessageParser::new(self.schema.clone(), self.config.format.clone());
        }

        let mode = &self.config.mode;
        let (urls, subscribe_message, reconnect, ping_interval, ping_timeout) = match mode {
            SourceMode::Client {
                urls,
                subscribe_message,
                reconnect,
                ping_interval,
                ping_timeout,
            } => (
                urls.clone(),
                subscribe_message.clone(),
                reconnect.clone(),
                *ping_interval,
                *ping_timeout,
            ),
            SourceMode::Server { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "WebSocketSource is for client mode; use WebSocketSourceServer for server mode"
                        .into(),
                ));
            }
        };

        if urls.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "at least one WebSocket URL is required".into(),
            ));
        }

        let _ = (ping_interval, ping_timeout); // reserved for future heartbeat use

        info!(
            urls = ?urls,
            format = ?self.config.format,
            backpressure = ?self.config.on_backpressure,
            "opening WebSocket source connector (client mode)"
        );

        // Create bounded channel between reader task and poll_batch().
        let channel_capacity = 10_000;
        let (tx, rx) = mpsc::channel(channel_capacity);

        // Create shutdown signal.
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Spawn the reader task.
        let handle = self.spawn_reader(
            urls.clone(),
            subscribe_message,
            reconnect.clone(),
            self.config.max_message_size,
            self.config.on_backpressure.clone(),
            tx,
            shutdown_rx,
            Arc::clone(&self.data_ready),
        );

        self.conn_mgr = Some(ConnectionManager::new(urls, reconnect));
        self.rx = Some(rx);
        self.shutdown_tx = Some(shutdown_tx);
        self.reader_handle = Some(handle);
        self.state = ConnectorState::Running;

        info!("WebSocket source connector opened successfully");
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
                Ok(WsMessage::Data(payload)) => {
                    self.metrics.record_message(payload.len() as u64);
                    self.message_buffer.push(payload);
                }
                Ok(WsMessage::Disconnected(reason)) => {
                    self.metrics.record_reconnect();
                    warn!(reason = %reason, "WebSocket disconnected");
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // Channel closed — reader task ended.
                    self.state = ConnectorState::Failed;
                    return Err(ConnectorError::ReadError(
                        "WebSocket reader task terminated".into(),
                    ));
                }
            }
        }

        if self.message_buffer.is_empty() {
            return Ok(None);
        }

        // Parse the accumulated messages into a RecordBatch.
        let refs: Vec<&[u8]> = self.message_buffer.iter().map(Vec::as_slice).collect();
        let batch = self.parser.parse_batch(&refs).inspect_err(|_e| {
            self.metrics.record_parse_error();
        })?;

        let num_rows = batch.num_rows();
        self.message_buffer.clear();

        // Update checkpoint state.
        self.checkpoint_state.watermark = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        debug!(records = num_rows, "polled batch from WebSocket");
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
            "restoring WebSocket source from checkpoint (best-effort)"
        );
        self.checkpoint_state = WebSocketSourceCheckpoint::from_source_checkpoint(checkpoint);

        // WebSocket is non-replayable — log the gap.
        warn!(
            last_sequence = ?self.checkpoint_state.last_sequence,
            last_event_time = ?self.checkpoint_state.last_event_time,
            "WebSocket source restored; data gap expected (non-replayable transport)"
        );

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
        info!("closing WebSocket source connector");

        // Signal shutdown.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        // Wait for the reader task to finish.
        if let Some(handle) = self.reader_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }

        self.rx = None;
        self.state = ConnectorState::Closed;
        info!("WebSocket source connector closed");
        Ok(())
    }
}

impl std::fmt::Debug for WebSocketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSource")
            .field("state", &self.state)
            .field("mode", &"client")
            .field("format", &self.config.format)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::source_config::MessageFormat;
    use super::*;
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
            mode: SourceMode::Client {
                urls: vec!["ws://localhost:9090".into()],
                subscribe_message: None,
                reconnect: ReconnectConfig::default(),
                ping_interval: std::time::Duration::from_secs(30),
                ping_timeout: std::time::Duration::from_secs(10),
            },
            format: MessageFormat::Json,
            on_backpressure: BackpressureStrategy::Block,
            event_time_field: None,
            event_time_format: None,
            max_message_size: 64 * 1024 * 1024,
            auth: None,
        }
    }

    #[test]
    fn test_new_defaults() {
        let source = WebSocketSource::new(test_schema(), test_config());
        assert_eq!(source.state(), ConnectorState::Created);
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let source = WebSocketSource::new(schema.clone(), test_config());
        assert_eq!(source.schema(), schema);
    }

    #[test]
    fn test_checkpoint_empty() {
        let source = WebSocketSource::new(test_schema(), test_config());
        let cp = source.checkpoint();
        assert!(!cp.is_empty()); // has websocket_state key
    }

    #[test]
    fn test_health_check_created() {
        let source = WebSocketSource::new(test_schema(), test_config());
        assert_eq!(source.health_check(), HealthStatus::Unknown);
    }

    #[test]
    fn test_metrics_initial() {
        let source = WebSocketSource::new(test_schema(), test_config());
        let m = source.metrics();
        assert_eq!(m.records_total, 0);
        assert_eq!(m.bytes_total, 0);
    }
}
