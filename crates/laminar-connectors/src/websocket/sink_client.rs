//! WebSocket sink connector — client mode.
//!
//! [`WebSocketSinkClient`] pushes streaming query output to an external
//! WebSocket server by connecting as a client.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tracing::{debug, info, warn};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskGuard, ConnectorTaskOwner, ConnectorTaskTracker, SinkConnector, SinkConsistency,
    SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;

use super::connection::{redact_url, ConnectionManager};
use super::serializer::BatchSerializer;
use super::sink_config::WebSocketSinkConfig;
use super::sink_metrics::{ConnectionGuard, WebSocketSinkMetrics};

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONTROL_MESSAGE_BYTES: usize = 16 * 1024;
const MAX_WRITE_BUFFER_BYTES: usize = 64 * 1024 * 1024;

fn websocket_config() -> tungstenite::protocol::WebSocketConfig {
    let mut config = tungstenite::protocol::WebSocketConfig::default();
    config.max_message_size = Some(MAX_CONTROL_MESSAGE_BYTES);
    config.max_frame_size = Some(MAX_CONTROL_MESSAGE_BYTES);
    config.max_write_buffer_size = MAX_WRITE_BUFFER_BYTES;
    config
}

/// Type alias for the split WebSocket sink half.
type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tungstenite::Message,
>;
type WsRead = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

fn spawn_control_reader(
    mut read: WsRead,
    alive: Arc<AtomicBool>,
    connection_guard: ConnectionGuard,
    task_guard: ConnectorTaskGuard,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _task_guard = task_guard;
        let _connection_guard = connection_guard;
        while let Some(message) = read.next().await {
            match message {
                Ok(tungstenite::Message::Close(_)) => {
                    alive.store(false, Ordering::Release);
                    // Keep polling to drive the automatic close response.
                }
                Ok(tungstenite::Message::Text(_) | tungstenite::Message::Binary(_)) => {
                    debug!("WebSocket sink peer sent unexpected application data");
                    break;
                }
                Ok(_) => {
                    // Polling drives tungstenite's automatic ping/pong handling.
                }
                Err(error) => {
                    debug!(error = %error, "WebSocket sink peer reader stopped");
                    break;
                }
            }
        }
        alive.store(false, Ordering::Release);
    })
}

/// WebSocket sink connector in client mode.
///
/// Connects to an external WebSocket server and pushes serialized
/// `RecordBatch` data as JSON text messages.
pub struct WebSocketSinkClient {
    /// Configuration.
    config: WebSocketSinkConfig,
    /// Input Arrow schema.
    schema: SchemaRef,
    /// Serializer for `RecordBatch` → messages.
    serializer: BatchSerializer,
    /// Connection manager for reconnection.
    conn_mgr: Option<ConnectionManager>,
    /// WebSocket sink (write half).
    ws_sink: Option<WsSink>,
    /// Background task that drives peer control frames and close detection.
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    /// Liveness signal shared with the background reader.
    connection_alive: Arc<AtomicBool>,
    /// Connector state.
    state: ConnectorState,
    /// Metrics.
    metrics: WebSocketSinkMetrics,
    /// Earliest time at which another connection attempt may run.
    next_reconnect_at: Option<tokio::time::Instant>,
    /// Whether the configured retry budget is exhausted or disabled.
    reconnect_exhausted: bool,
    /// Admission authority for reader tasks owned by this connector generation.
    task_owner: ConnectorTaskOwner,
    /// Terminal observer retained by the database before the connector opens.
    task_tracker: ConnectorTaskTracker,
}

impl WebSocketSinkClient {
    /// Creates a new WebSocket sink client connector.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        config: WebSocketSinkConfig,
        metrics: WebSocketSinkMetrics,
    ) -> Self {
        let serializer = BatchSerializer::new(schema.clone());
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();

        Self {
            config,
            schema,
            serializer,
            conn_mgr: None,
            ws_sink: None,
            reader_handle: None,
            connection_alive: Arc::new(AtomicBool::new(false)),
            state: ConnectorState::Created,
            metrics,
            next_reconnect_at: None,
            reconnect_exhausted: false,
            task_owner,
            task_tracker,
        }
    }

    fn schedule_reconnect(&mut self) {
        let delay = self
            .conn_mgr
            .as_mut()
            .and_then(ConnectionManager::next_backoff);
        self.next_reconnect_at = delay.map(|delay| tokio::time::Instant::now() + delay);
        self.reconnect_exhausted = delay.is_none();
    }

    fn install_connection(
        &mut self,
        stream: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<(), ConnectorError> {
        self.mark_disconnected();
        let task_guard = self.task_owner.track().ok_or_else(|| {
            ConnectorError::Internal(
                "WebSocket sink task generation is no longer accepting readers".into(),
            )
        })?;
        let (sink, read) = stream.split();
        let alive = Arc::new(AtomicBool::new(true));
        let connection_guard = self.metrics.connection_guard();
        self.reader_handle = Some(spawn_control_reader(
            read,
            Arc::clone(&alive),
            connection_guard,
            task_guard,
        ));
        self.connection_alive = alive;
        self.ws_sink = Some(sink);
        Ok(())
    }

    fn mark_disconnected(&mut self) {
        self.connection_alive.store(false, Ordering::Release);
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
        }
        self.ws_sink = None;
    }

    fn observe_reader_liveness(&mut self) {
        if self.ws_sink.is_some() && !self.connection_alive.load(Ordering::Acquire) {
            if let Some(handle) = self.reader_handle.take() {
                handle.abort();
            }
            self.ws_sink = None;
            self.schedule_reconnect();
        }
    }

    /// Attempts one due reconnect without blocking the sink on backoff sleeps.
    async fn reconnect_if_due(&mut self) -> Result<bool, ConnectorError> {
        self.observe_reader_liveness();
        if self.ws_sink.is_some() {
            return Ok(true);
        }
        if self.reconnect_exhausted {
            return Ok(false);
        }
        if self.next_reconnect_at.is_none() {
            self.schedule_reconnect();
        }
        let Some(deadline) = self.next_reconnect_at else {
            return Ok(false);
        };
        if tokio::time::Instant::now() < deadline {
            return Ok(false);
        }

        let conn_mgr = self
            .conn_mgr
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "connection manager initialized".into(),
                actual: "None".into(),
            })?;

        let url = conn_mgr.current_url().to_string();
        let safe_url = redact_url(&url);
        info!(url = %safe_url, "attempting WebSocket reconnection");

        let connection = tokio::time::timeout(
            IO_TIMEOUT,
            tokio_tungstenite::connect_async_with_config(&url, Some(websocket_config()), true),
        )
        .await;
        match connection {
            Err(_) => {
                warn!(url = %safe_url, "WebSocket reconnection attempt timed out");
                self.schedule_reconnect();
                Ok(false)
            }
            Ok(Ok((stream, _))) => {
                self.next_reconnect_at = None;
                self.reconnect_exhausted = false;
                self.install_connection(stream)?;
                info!(url = %safe_url, "WebSocket reconnected");

                Ok(true)
            }
            Ok(Err(e)) => {
                warn!(url = %safe_url, error = %e, "WebSocket reconnection attempt failed");
                self.schedule_reconnect();
                Ok(false)
            }
        }
    }
}

#[async_trait]
impl SinkConnector for WebSocketSinkClient {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let cfg = if config.properties().is_empty() {
            self.config.clone()
        } else {
            WebSocketSinkConfig::from_config(config)?
        };
        cfg.validate()?;
        if !matches!(cfg, WebSocketSinkConfig::Client { .. }) {
            return Err(ConnectorError::ConfigurationError(
                "WebSocketSinkClient requires mode = 'client'".into(),
            ));
        }
        Ok(SinkContract::new(
            SinkConsistency::Ephemeral,
            SinkTopology::Singleton,
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

        let url = match &effective_config {
            WebSocketSinkConfig::Client { url } => url.clone(),
            WebSocketSinkConfig::Server { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "WebSocketSinkClient is for client mode; use WebSocketSinkServer for server mode".into(),
                ));
            }
        };
        let reconnect = super::source_config::ReconnectConfig::default();
        self.state = ConnectorState::Initializing;

        let safe_url = redact_url(&url);
        info!(url = %safe_url, "opening WebSocket sink client");

        let connection = tokio::time::timeout(
            IO_TIMEOUT,
            tokio_tungstenite::connect_async_with_config(&url, Some(websocket_config()), true),
        )
        .await;
        let stream = match connection {
            Ok(Ok((stream, _response))) => stream,
            Ok(Err(error)) => {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::ConnectionFailed(format!(
                    "failed to connect to {safe_url}: {error}"
                )));
            }
            Err(_) => {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::ConnectionFailed(format!(
                    "connection to {safe_url} timed out"
                )));
            }
        };

        self.install_connection(stream)?;
        self.config = effective_config;
        self.conn_mgr = Some(ConnectionManager::new(vec![url.clone()], reconnect));
        self.next_reconnect_at = None;
        self.reconnect_exhausted = false;
        self.state = ConnectorState::Running;

        info!(url = %safe_url, "WebSocket sink client connected");
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

        let rows = self.serializer.serialize_rows(batch)?;
        if rows.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }
        let mut bytes_queued: u64 = 0;
        let mut records_queued: usize = 0;

        self.observe_reader_liveness();
        if self.ws_sink.is_none() && !self.reconnect_if_due().await? {
            return Err(ConnectorError::WriteError(
                "WebSocket sink is disconnected and cannot accept the batch".into(),
            ));
        }

        let deadline = tokio::time::Instant::now() + IO_TIMEOUT;
        for row in rows {
            let row_len = row.len();
            let text = tungstenite::Utf8Bytes::try_from(row).map_err(|error| {
                ConnectorError::Serde(crate::error::SerdeError::Json(format!(
                    "JSON encoder produced invalid UTF-8: {error}"
                )))
            })?;
            if let Some(ref mut sink) = self.ws_sink {
                match tokio::time::timeout_at(deadline, sink.feed(tungstenite::Message::Text(text)))
                    .await
                {
                    Ok(Ok(())) => {
                        bytes_queued += row_len as u64;
                        records_queued += 1;
                    }
                    Ok(Err(error)) => {
                        self.metrics.record_delivery_failure(1);
                        self.mark_disconnected();
                        self.schedule_reconnect();
                        return Err(ConnectorError::WriteError(format!(
                            "WebSocket send failed after {records_queued} rows: {error}"
                        )));
                    }
                    Err(_) => {
                        self.metrics.record_delivery_failure(1);
                        self.mark_disconnected();
                        self.schedule_reconnect();
                        return Err(ConnectorError::WriteError(format!(
                            "WebSocket batch deadline elapsed after {records_queued} rows"
                        )));
                    }
                }
            } else {
                return Err(ConnectorError::WriteError(
                    "WebSocket sink disconnected while writing the batch".into(),
                ));
            }
        }

        let flush_result = if let Some(ref mut sink) = self.ws_sink {
            tokio::time::timeout_at(deadline, sink.flush()).await
        } else {
            return Err(ConnectorError::WriteError(
                "WebSocket sink disconnected before flushing the batch".into(),
            ));
        };
        match flush_result {
            Ok(Ok(())) => {
                if records_queued > 0 {
                    if let Some(conn_mgr) = self.conn_mgr.as_mut() {
                        conn_mgr.reset();
                    }
                }
            }
            Ok(Err(error)) => {
                self.metrics
                    .record_delivery_failure(u64::try_from(records_queued).unwrap_or(u64::MAX));
                self.mark_disconnected();
                self.schedule_reconnect();
                return Err(ConnectorError::WriteError(format!(
                    "WebSocket batch flush failed after {records_queued} rows: {error}"
                )));
            }
            Err(_) => {
                self.metrics
                    .record_delivery_failure(u64::try_from(records_queued).unwrap_or(u64::MAX));
                self.mark_disconnected();
                self.schedule_reconnect();
                return Err(ConnectorError::WriteError(format!(
                    "WebSocket batch flush deadline elapsed after {records_queued} rows"
                )));
            }
        }
        self.metrics
            .record_sends(records_queued as u64, bytes_queued);

        debug!(
            records = records_queued,
            bytes = bytes_queued,
            "wrote batch to WebSocket"
        );

        Ok(WriteResult::new(records_queued, bytes_queued))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.observe_reader_liveness();
        if self.ws_sink.is_none() && !self.reconnect_if_due().await? {
            return Err(ConnectorError::WriteError(
                "cannot flush a disconnected WebSocket sink".into(),
            ));
        }
        // Flush the WebSocket.
        if let Some(ref mut sink) = self.ws_sink {
            match tokio::time::timeout(IO_TIMEOUT, sink.flush()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.mark_disconnected();
                    self.schedule_reconnect();
                    return Err(ConnectorError::WriteError(format!("flush failed: {error}")));
                }
                Err(_) => {
                    self.mark_disconnected();
                    self.schedule_reconnect();
                    return Err(ConnectorError::WriteError(
                        "WebSocket flush timed out".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing WebSocket sink client");

        if let Some(ref mut sink) = self.ws_sink {
            let _ = tokio::time::timeout(IO_TIMEOUT, sink.send(tungstenite::Message::Close(None)))
                .await;
        }
        self.ws_sink = None;
        if let Some(handle) = self.reader_handle.as_mut() {
            if tokio::time::timeout(Duration::from_secs(1), &mut *handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
                debug!("timed out waiting for WebSocket close handshake");
            }
        }
        self.reader_handle = None;
        self.connection_alive.store(false, Ordering::Release);
        self.next_reconnect_at = None;
        self.state = ConnectorState::Closed;
        info!("WebSocket sink client closed");
        Ok(())
    }
}

impl Drop for WebSocketSinkClient {
    fn drop(&mut self) {
        self.connection_alive.store(false, Ordering::Release);
        self.ws_sink = None;
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
        }
    }
}

impl std::fmt::Debug for WebSocketSinkClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSinkClient")
            .field("state", &self.state)
            .field("connected", &self.ws_sink.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
