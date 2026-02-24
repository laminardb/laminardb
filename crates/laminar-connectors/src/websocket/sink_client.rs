//! WebSocket sink connector — client mode.
//!
//! [`WebSocketSinkClient`] pushes streaming query output to an external
//! WebSocket server by connecting as a client.

use std::collections::VecDeque;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tracing::{debug, info, warn};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::connection::ConnectionManager;
use super::serializer::BatchSerializer;
use super::sink_config::{SinkMode, WebSocketSinkConfig};
use super::sink_metrics::WebSocketSinkMetrics;

/// Type alias for the split WebSocket sink half.
type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tungstenite::Message,
>;

/// WebSocket sink connector in client mode.
///
/// Connects to an external WebSocket server and pushes serialized
/// `RecordBatch` data as text or binary messages.
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
    /// Connector state.
    state: ConnectorState,
    /// Metrics.
    metrics: WebSocketSinkMetrics,
    /// Current epoch.
    current_epoch: u64,
    /// Buffer for messages while disconnected.
    disconnect_buffer: VecDeque<String>,
    /// Max buffer size in bytes when disconnected.
    max_buffer_bytes: usize,
    /// Current buffered bytes.
    buffered_bytes: usize,
}

impl WebSocketSinkClient {
    /// Creates a new WebSocket sink client connector.
    #[must_use]
    pub fn new(schema: SchemaRef, config: WebSocketSinkConfig) -> Self {
        let serializer = BatchSerializer::new(config.format.clone());

        let max_buffer_bytes = match &config.mode {
            SinkMode::Client {
                buffer_on_disconnect,
                ..
            } => buffer_on_disconnect.unwrap_or(0),
            SinkMode::Server { .. } => 0,
        };

        Self {
            config,
            schema,
            serializer,
            conn_mgr: None,
            ws_sink: None,
            state: ConnectorState::Created,
            metrics: WebSocketSinkMetrics::new(),
            current_epoch: 0,
            disconnect_buffer: VecDeque::new(),
            max_buffer_bytes,
            buffered_bytes: 0,
        }
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Attempts to reconnect and flush the disconnect buffer.
    async fn try_reconnect(&mut self) -> Result<(), ConnectorError> {
        let conn_mgr = self
            .conn_mgr
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "connection manager initialized".into(),
                actual: "None".into(),
            })?;

        let url = conn_mgr.current_url().to_string();
        info!(url = %url, "attempting WebSocket reconnection");

        match tokio_tungstenite::connect_async(&url).await {
            Ok((stream, _)) => {
                conn_mgr.reset();
                let (sink, _read) = stream.split();
                self.ws_sink = Some(sink);
                self.metrics.record_connect();
                info!(url = %url, "WebSocket reconnected");

                // Flush disconnect buffer.
                self.flush_disconnect_buffer().await?;
                Ok(())
            }
            Err(e) => {
                self.metrics.record_disconnect();
                Err(ConnectorError::ConnectionFailed(format!(
                    "reconnection to {url} failed: {e}"
                )))
            }
        }
    }

    /// Flushes buffered messages that accumulated during disconnection.
    async fn flush_disconnect_buffer(&mut self) -> Result<(), ConnectorError> {
        if self.disconnect_buffer.is_empty() {
            return Ok(());
        }

        let sink = self
            .ws_sink
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "ws_sink initialized".into(),
                actual: "None".into(),
            })?;

        let count = self.disconnect_buffer.len();
        debug!(buffered_messages = count, "flushing disconnect buffer");

        while let Some(msg) = self.disconnect_buffer.pop_front() {
            self.buffered_bytes -= msg.len();
            if let Err(e) = sink.send(tungstenite::Message::Text(msg.into())).await {
                warn!(error = %e, "failed to flush buffered message");
                return Err(ConnectorError::WriteError(format!(
                    "buffer flush failed: {e}"
                )));
            }
        }

        Ok(())
    }

    /// Buffers a message for later delivery (when disconnected).
    fn buffer_message(&mut self, msg: String) {
        if self.max_buffer_bytes == 0 {
            return; // buffering disabled
        }

        let msg_len = msg.len();

        // Evict oldest if buffer would exceed limit.
        while self.buffered_bytes + msg_len > self.max_buffer_bytes {
            if let Some(old) = self.disconnect_buffer.pop_front() {
                self.buffered_bytes -= old.len();
            } else {
                break;
            }
        }

        if msg_len <= self.max_buffer_bytes {
            self.buffered_bytes += msg_len;
            self.disconnect_buffer.push_back(msg);
        }
    }
}

#[async_trait]
impl SinkConnector for WebSocketSinkClient {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // If config has properties, re-parse (supports runtime config via SQL WITH).
        if !config.properties().is_empty() {
            self.config = WebSocketSinkConfig::from_config(config)?;
        }

        let (url, reconnect) = match &self.config.mode {
            SinkMode::Client { url, reconnect, .. } => (url.clone(), reconnect.clone()),
            SinkMode::Server { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "WebSocketSinkClient is for client mode; use WebSocketSinkServer for server mode".into(),
                ));
            }
        };

        info!(url = %url, "opening WebSocket sink client");

        let (stream, _response) = tokio_tungstenite::connect_async(&url).await.map_err(|e| {
            ConnectorError::ConnectionFailed(format!("failed to connect to {url}: {e}"))
        })?;

        let (sink, _read) = stream.split();
        self.ws_sink = Some(sink);
        self.conn_mgr = Some(ConnectionManager::new(vec![url.clone()], reconnect));
        self.state = ConnectorState::Running;
        self.metrics.record_connect();

        info!(url = %url, "WebSocket sink client connected");
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
        let mut bytes_written: u64 = 0;

        for row in &rows {
            if let Some(ref mut sink) = self.ws_sink {
                match sink
                    .send(tungstenite::Message::Text(row.clone().into()))
                    .await
                {
                    Ok(()) => {
                        bytes_written += row.len() as u64;
                        self.metrics.record_send(row.len() as u64);
                    }
                    Err(e) => {
                        warn!(error = %e, "send failed, buffering and attempting reconnect");
                        self.ws_sink = None;
                        self.buffer_message(row.clone());

                        // Try to reconnect.
                        if self.try_reconnect().await.is_err() {
                            // Buffer remaining rows.
                            for remaining in rows.iter().skip(1) {
                                self.buffer_message(remaining.clone());
                            }
                            return Ok(WriteResult::new(0, 0));
                        }
                    }
                }
            } else {
                self.buffer_message(row.clone());
            }
        }

        debug!(
            records = batch.num_rows(),
            bytes = bytes_written,
            "wrote batch to WebSocket"
        );

        Ok(WriteResult::new(batch.num_rows(), bytes_written))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => {
                if self.ws_sink.is_some() {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Degraded("disconnected, buffering".into())
                }
            }
            ConnectorState::Created | ConnectorState::Initializing => HealthStatus::Unknown,
            ConnectorState::Paused => HealthStatus::Degraded("paused".into()),
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
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.current_epoch = epoch;
        Ok(())
    }

    async fn commit_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        // Flush the WebSocket.
        if let Some(ref mut sink) = self.ws_sink {
            sink.flush()
                .await
                .map_err(|e| ConnectorError::WriteError(format!("flush failed: {e}")))?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing WebSocket sink client");

        if let Some(ref mut sink) = self.ws_sink {
            let _ = sink.send(tungstenite::Message::Close(None)).await;
        }

        self.ws_sink = None;
        self.state = ConnectorState::Closed;
        info!("WebSocket sink client closed");
        Ok(())
    }
}

impl std::fmt::Debug for WebSocketSinkClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketSinkClient")
            .field("state", &self.state)
            .field("connected", &self.ws_sink.is_some())
            .field("buffered_messages", &self.disconnect_buffer.len())
            .field("current_epoch", &self.current_epoch)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::source_config::ReconnectConfig;
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn test_config() -> WebSocketSinkConfig {
        WebSocketSinkConfig {
            mode: SinkMode::Client {
                url: "ws://localhost:9090".into(),
                reconnect: ReconnectConfig::default(),
                buffer_on_disconnect: Some(1_048_576), // 1MB
                batch_interval: None,
                batch_max_size: None,
            },
            format: super::super::sink_config::SinkFormat::Json,
            auth: None,
        }
    }

    #[test]
    fn test_new() {
        let sink = WebSocketSinkClient::new(test_schema(), test_config());
        assert_eq!(sink.state(), ConnectorState::Created);
        assert!(sink.ws_sink.is_none());
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let sink = WebSocketSinkClient::new(schema.clone(), test_config());
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn test_buffer_message() {
        let mut sink = WebSocketSinkClient::new(test_schema(), test_config());
        sink.buffer_message("hello".into());
        sink.buffer_message("world".into());
        assert_eq!(sink.disconnect_buffer.len(), 2);
        assert_eq!(sink.buffered_bytes, 10);
    }

    #[test]
    fn test_buffer_eviction() {
        let config = WebSocketSinkConfig {
            mode: SinkMode::Client {
                url: "ws://localhost:9090".into(),
                reconnect: ReconnectConfig::default(),
                buffer_on_disconnect: Some(10), // 10 bytes max
                batch_interval: None,
                batch_max_size: None,
            },
            format: super::super::sink_config::SinkFormat::Json,
            auth: None,
        };
        let mut sink = WebSocketSinkClient::new(test_schema(), config);

        sink.buffer_message("12345".into()); // 5 bytes
        sink.buffer_message("67890".into()); // 5 bytes, total 10
        sink.buffer_message("abcde".into()); // evicts "12345"

        assert_eq!(sink.disconnect_buffer.len(), 2);
        assert_eq!(sink.disconnect_buffer[0], "67890");
        assert_eq!(sink.disconnect_buffer[1], "abcde");
    }

    #[test]
    fn test_buffer_disabled() {
        let config = WebSocketSinkConfig {
            mode: SinkMode::Client {
                url: "ws://localhost:9090".into(),
                reconnect: ReconnectConfig::default(),
                buffer_on_disconnect: None, // disabled
                batch_interval: None,
                batch_max_size: None,
            },
            format: super::super::sink_config::SinkFormat::Json,
            auth: None,
        };
        let mut sink = WebSocketSinkClient::new(test_schema(), config);
        sink.buffer_message("hello".into());
        assert!(sink.disconnect_buffer.is_empty());
    }

    #[test]
    fn test_capabilities() {
        let sink = WebSocketSinkClient::new(test_schema(), test_config());
        let caps = sink.capabilities();
        assert!(!caps.exactly_once);
    }

    #[test]
    fn test_health_created() {
        let sink = WebSocketSinkClient::new(test_schema(), test_config());
        assert_eq!(sink.health_check(), HealthStatus::Unknown);
    }
}
