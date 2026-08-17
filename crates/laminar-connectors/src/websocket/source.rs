//! WebSocket source connector — client mode.
//!
//! [`WebSocketSource`] connects to an external WebSocket server (e.g., exchange
//! market data feeds) and produces Arrow `RecordBatch` data via the
//! [`SourceConnector`] trait.
//!
//! # Delivery Guarantees
//!
//! WebSocket is non-replayable. Delivery is best-effort; reconnects can create
//! gaps or duplicates.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use crossfire::{mpsc, AsyncRx, MAsyncTx, TryRecvError, TrySendError};
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskGuard, ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceConnector,
    SourceConsistency, SourceContract, SourceInputMode, SourceTopology,
};
use crate::connector::{SourcePosition, SourceStart};
use crate::error::ConnectorError;

use crate::schema::json::decoder::JsonDecoderConfig;

use super::backpressure::WsBackpressure;
use super::connection::{redact_url, ConnectionManager};
use super::metrics::WebSocketSourceMetrics;
use super::parser::MessageParser;
use super::source_config::{
    MessageFormat, ReconnectConfig, WebSocketSourceConfig, INGRESS_BUFFER_BYTES,
};

const IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const RECONNECT_STABILITY_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);

struct BufferedMessage {
    payload: Bytes,
    _permit: OwnedSemaphorePermit,
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
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Metrics.
    metrics: WebSocketSourceMetrics,
    /// Bounded channel receiver for messages from the WS reader task.
    rx: Option<AsyncRx<mpsc::Array<BufferedMessage>>>,
    /// Shutdown signal sender.
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Handle to the spawned reader task.
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    /// Byte-accounted messages accumulated between `poll_batch()` calls.
    message_buffer: Vec<BufferedMessage>,
    /// Maximum records per batch.
    max_batch_size: usize,
    /// Notification handle signalled when data arrives from the reader task.
    data_ready: Arc<Notify>,
    /// Terminal reader failure published outside the bounded data channel.
    terminal_error: Arc<Mutex<Option<String>>>,
    /// Admission authority for tasks owned by this connector generation.
    task_owner: ConnectorTaskOwner,
    /// Terminal observer retained by the database before the connector starts.
    task_tracker: ConnectorTaskTracker,
}

impl WebSocketSource {
    /// Creates a new WebSocket source connector in client mode.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        config: WebSocketSourceConfig,
        metrics: WebSocketSourceMetrics,
    ) -> Self {
        let parser = MessageParser::new(
            schema.clone(),
            config.format.clone(),
            JsonDecoderConfig::default(),
        );
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();

        Self {
            config,
            schema,
            parser,
            state: ConnectorState::Created,
            metrics,
            rx: None,
            shutdown_tx: None,
            reader_handle: None,
            message_buffer: Vec::new(),
            max_batch_size: 1000,
            data_ready: Arc::new(Notify::new()),
            terminal_error: Arc::new(Mutex::new(None)),
            task_owner,
            task_tracker,
        }
    }

    fn track_reader_task(&self) -> Result<ConnectorTaskGuard, ConnectorError> {
        self.task_owner.track().ok_or_else(|| {
            ConnectorError::Internal(
                "WebSocket source task generation is no longer accepting readers".into(),
            )
        })
    }

    /// Spawns the WebSocket reader task that connects to the server and
    /// feeds messages through the bounded channel.
    fn spawn_reader(
        urls: Vec<String>,
        subscribe_message: Option<String>,
        reconnect: ReconnectConfig,
        max_message_size: usize,
        on_backpressure: WsBackpressure,
        tx: MAsyncTx<mpsc::Array<BufferedMessage>>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
        data_ready: Arc<Notify>,
        metrics: WebSocketSourceMetrics,
        byte_budget: Arc<Semaphore>,
        terminal_error: Arc<Mutex<Option<String>>>,
        task_guard: ConnectorTaskGuard,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let _task_guard = task_guard;
            let mut conn_mgr = ConnectionManager::new(urls, reconnect);

            'outer: loop {
                // Check shutdown.
                if *shutdown_rx.borrow() {
                    break;
                }

                let url = conn_mgr.current_url().to_string();
                let safe_url = redact_url(&url);
                if conn_mgr.attempt() > 0 {
                    metrics.record_reconnect();
                }
                info!(url = %safe_url, "connecting to WebSocket server");

                // Attempt connection with frame-level size cap.
                let mut ws_config = tungstenite::protocol::WebSocketConfig::default();
                ws_config.max_message_size = Some(max_message_size);
                ws_config.max_frame_size = Some(max_message_size);
                let connection = tokio::select! {
                    result = tokio::time::timeout(
                        IO_TIMEOUT,
                        tokio_tungstenite::connect_async_with_config(
                            &url,
                            Some(ws_config),
                            true, // disable Nagle for low latency
                        ),
                    ) => result
                        .map_err(|_| format!("connection attempt timed out after {IO_TIMEOUT:?}"))
                        .and_then(|result| result.map_err(|error| error.to_string())),
                    _ = shutdown_rx.changed() => break,
                };
                let ws_stream = match connection {
                    Ok((stream, _response)) => {
                        info!(url = %safe_url, "WebSocket connection established");
                        stream
                    }
                    Err(e) => {
                        warn!(url = %safe_url, error = %e, "WebSocket connection failed");
                        if let Some(delay) = conn_mgr.next_backoff() {
                            tokio::select! {
                                () = tokio::time::sleep(delay) => continue,
                                _ = shutdown_rx.changed() => break,
                            }
                        } else {
                            publish_terminal(
                                &terminal_error,
                                &data_ready,
                                format!("connection failed, no more retries: {e}"),
                            );
                            break;
                        }
                    }
                };

                let (mut write, mut read) = ws_stream.split();

                // Send subscription message if configured.
                if let Some(ref msg) = subscribe_message {
                    let subscription = tokio::select! {
                        result = tokio::time::timeout(
                            IO_TIMEOUT,
                            write.send(tungstenite::Message::Text(msg.clone().into())),
                        ) => result
                            .map_err(|_| format!("subscription write timed out after {IO_TIMEOUT:?}"))
                            .and_then(|result| result.map_err(|error| error.to_string())),
                        _ = shutdown_rx.changed() => break 'outer,
                    };
                    if let Err(e) = subscription {
                        warn!(error = %e, "failed to send subscription message");
                        if let Some(delay) = conn_mgr.next_backoff() {
                            tokio::select! {
                                () = tokio::time::sleep(delay) => continue,
                                _ = shutdown_rx.changed() => break 'outer,
                            }
                        } else {
                            publish_terminal(
                                &terminal_error,
                                &data_ready,
                                format!("subscription failed, no more retries: {e}"),
                            );
                            break 'outer;
                        }
                    }
                    debug!("subscription message sent");
                }
                let session_started = tokio::time::Instant::now();

                // Read loop.
                loop {
                    tokio::select! {
                        msg = read.next() => {
                            match msg {
                                Some(Ok(tungstenite::Message::Text(text))) => {
                                    let payload = Bytes::from(text);
                                    if payload.len() > max_message_size {
                                        warn!(size = payload.len(), max = max_message_size, "message exceeds max size, dropping");
                                        continue;
                                    }
                                    metrics.record_message(payload.len());
                                    if send_with_backpressure(
                                        &tx,
                                        payload,
                                        &on_backpressure,
                                        &data_ready,
                                        &metrics,
                                        &byte_budget,
                                        &mut shutdown_rx,
                                    ).await.is_err() {
                                        break 'outer;
                                    }
                                }
                                Some(Ok(tungstenite::Message::Binary(data))) => {
                                    let payload = data;
                                    if payload.len() > max_message_size {
                                        warn!(size = payload.len(), max = max_message_size, "message exceeds max size, dropping");
                                        continue;
                                    }
                                    metrics.record_message(payload.len());
                                    if send_with_backpressure(
                                        &tx,
                                        payload,
                                        &on_backpressure,
                                        &data_ready,
                                        &metrics,
                                        &byte_budget,
                                        &mut shutdown_rx,
                                    ).await.is_err() {
                                        break 'outer;
                                    }
                                }
                                Some(Ok(tungstenite::Message::Ping(data))) => {
                                    if !matches!(tokio::time::timeout(
                                        IO_TIMEOUT,
                                        write.send(tungstenite::Message::Pong(data)),
                                    ).await, Ok(Ok(()))) {
                                        warn!(url = %safe_url, "failed to send WebSocket pong");
                                        break;
                                    }
                                }
                                Some(Ok(tungstenite::Message::Close(_))) => {
                                    info!(url = %safe_url, "server sent Close frame");
                                    let _ = tokio::time::timeout(IO_TIMEOUT, write.flush()).await;
                                    break;
                                }
                                Some(Ok(_)) => {} // Pong, Frame — ignore
                                Some(Err(e)) => {
                                    warn!(url = %safe_url, error = %e, "WebSocket read error");
                                    break;
                                }
                                None => {
                                    info!(url = %safe_url, "WebSocket stream ended");
                                    break;
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            debug!("shutdown signal received in reader");
                            let _ = tokio::time::timeout(
                                IO_TIMEOUT,
                                write.send(tungstenite::Message::Close(None)),
                            ).await;
                            break 'outer;
                        }
                    }
                }

                warn!(url = %safe_url, "WebSocket source disconnected");
                if session_started.elapsed() >= RECONNECT_STABILITY_WINDOW {
                    conn_mgr.reset();
                }

                if let Some(delay) = conn_mgr.next_backoff() {
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {},
                        _ = shutdown_rx.changed() => break,
                    }
                } else {
                    publish_terminal(
                        &terminal_error,
                        &data_ready,
                        format!("disconnected from {safe_url}, no more retries"),
                    );
                    break;
                }
            }
            data_ready.notify_one();
        })
    }
}

/// Sends a message through the channel, applying the backpressure strategy
/// if the channel is full. Signals `data_ready` on successful send so the
/// pipeline coordinator wakes immediately.
///
/// Returns `Err(())` if the channel is closed (shutdown).
async fn send_with_backpressure(
    tx: &MAsyncTx<mpsc::Array<BufferedMessage>>,
    payload: Bytes,
    strategy: &WsBackpressure,
    data_ready: &Notify,
    metrics: &WebSocketSourceMetrics,
    byte_budget: &Arc<Semaphore>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), ()> {
    let permits = u32::try_from(payload.len().max(1)).map_err(|_| ())?;
    let enqueued = match strategy {
        WsBackpressure::Block => {
            let permit = tokio::select! {
                result = Arc::clone(byte_budget).acquire_many_owned(permits) => {
                    result.map_err(|_| ())?
                }
                _ = shutdown_rx.changed() => return Err(()),
            };
            let message = BufferedMessage {
                payload,
                _permit: permit,
            };
            tokio::select! {
                result = tx.send(message) => result.map(|()| true).map_err(|_| ()),
                _ = shutdown_rx.changed() => Err(()),
            }
        }
        WsBackpressure::DropNewest => {
            let permit = match Arc::clone(byte_budget).try_acquire_many_owned(permits) {
                Ok(permit) => permit,
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    metrics.record_backpressure_drop();
                    return Ok(());
                }
                Err(tokio::sync::TryAcquireError::Closed) => return Err(()),
            };
            let message = BufferedMessage {
                payload,
                _permit: permit,
            };
            match tx.try_send(message) {
                Ok(()) => Ok(true),
                Err(TrySendError::Full(_)) => {
                    metrics.record_backpressure_drop();
                    Ok(false)
                }
                Err(TrySendError::Disconnected(_)) => Err(()),
            }
        }
    }?;
    if enqueued {
        data_ready.notify_one();
    }
    Ok(())
}

fn publish_terminal(error: &Mutex<Option<String>>, data_ready: &Notify, reason: String) {
    let mut terminal = error.lock();
    if terminal.is_none() {
        *terminal = Some(reason);
    }
    drop(terminal);
    data_ready.notify_one();
}

#[async_trait]
impl SourceConnector for WebSocketSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        if !matches!(self.state, ConnectorState::Created | ConnectorState::Closed) {
            return Err(ConnectorError::InvalidState {
                expected: "Created or Closed".into(),
                actual: self.state.to_string(),
            });
        }
        if let SourcePosition::Resume { attempt, .. } = position {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket client is an ephemeral source and cannot resume checkpoint attempt {attempt:?}"
            )));
        }
        let config = &config;
        let effective_config = if config.properties().is_empty() {
            self.config.clone()
        } else {
            WebSocketSourceConfig::from_config(config)?
        };
        effective_config.validate()?;

        // Override schema from SQL DDL if provided and fail closed on a corrupt encoding.
        let decoded_schema = config.arrow_schema();
        if config.get("_arrow_schema").is_some() && decoded_schema.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "invalid WebSocket source _arrow_schema encoding".into(),
            ));
        }
        let effective_schema = if let Some(schema) = decoded_schema {
            info!(
                fields = schema.fields().len(),
                "using SQL-defined schema for deserialization"
            );
            schema
        } else {
            self.schema.clone()
        };
        if effective_schema.fields().is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "WebSocket source requires a declared Arrow schema".into(),
            ));
        }
        MessageParser::validate_format_schema(&effective_schema, &effective_config.format)?;
        let decoder_config = if matches!(&effective_config.format, MessageFormat::Json) {
            JsonDecoderConfig::from_connector_config(config, &effective_schema)?
        } else {
            JsonDecoderConfig::default()
        };
        let parser = MessageParser::new(
            effective_schema.clone(),
            effective_config.format.clone(),
            decoder_config,
        );

        let urls = effective_config.urls.clone();
        let subscribe_message = effective_config.subscribe_message.clone();
        let reconnect = effective_config.reconnect.clone();

        if urls.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "at least one WebSocket URL is required".into(),
            ));
        }

        info!(
            url_count = urls.len(),
            format = ?effective_config.format,
            backpressure = ?effective_config.on_backpressure,
            "opening WebSocket source connector (client mode)"
        );
        self.state = ConnectorState::Initializing;
        self.config = effective_config;
        self.schema = effective_schema;
        self.parser = parser;
        self.message_buffer.clear();
        *self.terminal_error.lock() = None;

        // Create bounded channel between reader task and poll_batch().
        let channel_capacity = 10_000;
        let (tx, rx) = mpsc::bounded_async::<BufferedMessage>(channel_capacity);

        // Create shutdown signal.
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let byte_budget = Arc::new(Semaphore::new(INGRESS_BUFFER_BYTES));
        let task_guard = self.track_reader_task()?;

        // Spawn the reader task.
        let handle = Self::spawn_reader(
            urls,
            subscribe_message,
            reconnect,
            self.config.max_message_size,
            self.config.on_backpressure.clone(),
            tx,
            shutdown_rx,
            Arc::clone(&self.data_ready),
            self.metrics.clone(),
            byte_budget,
            Arc::clone(&self.terminal_error),
            task_guard,
        );

        self.rx = Some(rx);
        self.shutdown_tx = Some(shutdown_tx);
        self.reader_handle = Some(handle);
        self.state = ConnectorState::Running;

        info!("WebSocket source connector opened successfully");
        Ok(())
    }

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
        let mut reader_ended = false;
        while self.message_buffer.len() < limit {
            match rx.try_recv() {
                Ok(payload) => {
                    self.message_buffer.push(payload);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    reader_ended = true;
                    break;
                }
            }
        }

        if self.message_buffer.is_empty() {
            if let Some(reason) = self.terminal_error.lock().take() {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::ReadError(reason));
            }
            if reader_ended {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::ReadError(
                    "WebSocket reader task terminated".into(),
                ));
            }
            return Ok(None);
        }

        // Parse the accumulated messages into a RecordBatch.
        let refs: Vec<&[u8]> = self
            .message_buffer
            .iter()
            .map(|message| message.payload.as_ref())
            .collect();
        let batch = match self.parser.parse_batch_bounded(&refs, limit) {
            Ok(batch) => batch,
            Err(error) => {
                self.metrics.record_parse_error();
                self.message_buffer.clear();
                return Err(error);
            }
        };

        let num_rows = batch.num_rows();
        self.message_buffer.clear();

        debug!(records = num_rows, "polled batch from WebSocket");
        Ok(Some(SourceBatch::new(batch)))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        if !config.properties().is_empty() {
            WebSocketSourceConfig::from_config(config)?;
        }
        Ok(SourceContract::new(
            SourceConsistency::Ephemeral,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        ))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing WebSocket source connector");

        // Signal shutdown.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        // Wait for the reader task to finish.
        if let Some(handle) = self.reader_handle.as_mut() {
            if tokio::time::timeout(std::time::Duration::from_secs(5), &mut *handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        self.reader_handle = None;

        self.rx = None;
        self.message_buffer.clear();
        *self.terminal_error.lock() = None;
        self.state = ConnectorState::Closed;
        info!("WebSocket source connector closed");
        Ok(())
    }
}

impl Drop for WebSocketSource {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown_tx.take() {
            let _ = shutdown.send(true);
        }
        if let Some(handle) = self.reader_handle.take() {
            handle.abort();
        }
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
mod tests;
