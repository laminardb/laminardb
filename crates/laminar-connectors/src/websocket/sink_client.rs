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
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
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
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
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
    ) {
        self.mark_disconnected();
        let (sink, read) = stream.split();
        let alive = Arc::new(AtomicBool::new(true));
        let connection_guard = self.metrics.connection_guard();
        self.reader_handle = Some(spawn_control_reader(
            read,
            Arc::clone(&alive),
            connection_guard,
        ));
        self.connection_alive = alive;
        self.ws_sink = Some(sink);
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
                self.install_connection(stream);
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

        self.install_connection(stream);
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
        if let Some(mut handle) = self.reader_handle.take() {
            if tokio::time::timeout(Duration::from_secs(1), &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
                debug!("timed out waiting for WebSocket close handshake");
            }
        }
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
mod tests {
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
        let mut sink =
            WebSocketSinkClient::new(test_schema(), config, WebSocketSinkMetrics::local());
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
                match websocket.next().await.unwrap().unwrap() {
                    tungstenite::Message::Pong(payload) => return payload,
                    _ => continue,
                }
            }
        });

        let config = WebSocketSinkConfig::Client {
            url: format!("ws://{address}"),
        };
        let mut sink =
            WebSocketSinkClient::new(test_schema(), config, WebSocketSinkMetrics::local());
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
        let mut sink =
            WebSocketSinkClient::new(test_schema(), config, WebSocketSinkMetrics::local());
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
        let mut sink =
            WebSocketSinkClient::new(test_schema(), config, WebSocketSinkMetrics::local());
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
}
