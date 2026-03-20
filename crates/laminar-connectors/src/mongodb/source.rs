//! `MongoDB` CDC source connector implementation.
//!
//! Implements [`SourceConnector`] for streaming change events from `MongoDB`
//! change streams into `LaminarDB` as Arrow `RecordBatch`es.
//!
//! # Architecture
//!
//! - **Ring 0**: No CDC code — just SPSC channel pop (~5ns)
//! - **Ring 1**: Change stream consumption, event parsing, Arrow conversion
//! - **Ring 2**: Connection management, collection validation, health checks
//!
//! # Cancellation Safety
//!
//! The `poll_batch` method is cancellation-safe: dropping the future
//! mid-execution does not lose events. Buffered events remain in the
//! internal `VecDeque` and will be returned on the next poll.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, StringBuilder, UInt32Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::change_event::{MongoDbChangeEvent, OperationType};
use super::config::MongoDbSourceConfig;
use super::metrics::MongoSourceMetrics;
use super::resume_token::{InMemoryResumeTokenStore, ResumeToken, ResumeTokenStore};

/// Returns the Arrow schema for `MongoDB` CDC envelope records.
///
/// | Column              | Type   | Nullable | Description                        |
/// |---------------------|--------|----------|------------------------------------|
/// | `_namespace`        | Utf8   | no       | `database.collection`              |
/// | `_op`               | Utf8   | no       | Operation code (I/U/R/D/DROP/...)  |
/// | `_document_key`     | Utf8   | no       | Document key JSON                  |
/// | `_cluster_time_s`   | UInt32 | no       | Cluster time seconds               |
/// | `_cluster_time_i`   | UInt32 | no       | Cluster time increment             |
/// | `_wall_time_ms`     | Int64  | no       | Wall clock timestamp (Unix ms)     |
/// | `_full_document`    | Utf8   | yes      | Full document JSON                 |
/// | `_update_desc`      | Utf8   | yes      | Update description JSON            |
/// | `_resume_token`     | Utf8   | no       | Opaque resume token JSON           |
#[must_use]
pub fn mongodb_cdc_envelope_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_namespace", DataType::Utf8, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_document_key", DataType::Utf8, false),
        Field::new("_cluster_time_s", DataType::UInt32, false),
        Field::new("_cluster_time_i", DataType::UInt32, false),
        Field::new("_wall_time_ms", DataType::Int64, false),
        Field::new("_full_document", DataType::Utf8, true),
        Field::new("_update_desc", DataType::Utf8, true),
        Field::new("_resume_token", DataType::Utf8, false),
    ]))
}

/// `MongoDB` CDC source connector.
///
/// Streams change events from a `MongoDB` change stream using the
/// `SourceConnector` trait. Events are buffered internally and
/// converted to Arrow `RecordBatch`es on `poll_batch`.
///
/// # Resume Token Handling
///
/// The connector tracks resume tokens from individual change event `_id`
/// fields. Tokens are persisted via a pluggable [`ResumeTokenStore`].
///
/// # Sharded Cluster Note
///
/// On sharded clusters, `mongos` opens per-shard cursors and merges
/// results transparently. Ensure `max_pool_size` is at least as large
/// as the expected number of concurrent change streams to avoid
/// connection starvation.
pub struct MongoDbCdcSource {
    /// Connector configuration.
    config: MongoDbSourceConfig,

    /// Current lifecycle state.
    state: ConnectorState,

    /// Output schema (CDC envelope).
    schema: SchemaRef,

    /// Lock-free metrics.
    metrics: Arc<MongoSourceMetrics>,

    /// Buffered change events awaiting `poll_batch`.
    event_buffer: VecDeque<MongoDbChangeEvent>,

    /// Last persisted resume token.
    last_resume_token: Option<ResumeToken>,

    /// Resume token store.
    resume_token_store: Box<dyn ResumeTokenStore>,

    /// Notification handle signalled when data arrives from the stream.
    data_ready: Arc<Notify>,

    /// Whether an invalidate event has been received.
    invalidated: bool,

    /// Background change stream reader task handle (feature-gated).
    #[cfg(feature = "mongodb-cdc")]
    reader_handle: Option<tokio::task::JoinHandle<()>>,

    /// Channel receiver for change events from the background task.
    #[cfg(feature = "mongodb-cdc")]
    event_rx: Option<tokio::sync::mpsc::Receiver<ChangeStreamPayload>>,

    /// Shutdown signal for the background reader task.
    #[cfg(feature = "mongodb-cdc")]
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,
}

/// Payload from the background change stream reader task.
#[allow(dead_code)]
enum ChangeStreamPayload {
    /// A change event.
    Event(Box<MongoDbChangeEvent>),
    /// Fatal error from the reader task.
    Error(String),
}

impl MongoDbCdcSource {
    /// Creates a new `MongoDB` CDC source with the given configuration.
    #[must_use]
    pub fn new(config: MongoDbSourceConfig) -> Self {
        Self {
            config,
            state: ConnectorState::Created,
            schema: mongodb_cdc_envelope_schema(),
            metrics: Arc::new(MongoSourceMetrics::new()),
            event_buffer: VecDeque::new(),
            last_resume_token: None,
            resume_token_store: Box::new(InMemoryResumeTokenStore::new()),
            data_ready: Arc::new(Notify::new()),
            invalidated: false,
            #[cfg(feature = "mongodb-cdc")]
            reader_handle: None,
            #[cfg(feature = "mongodb-cdc")]
            event_rx: None,
            #[cfg(feature = "mongodb-cdc")]
            reader_shutdown: None,
        }
    }

    /// Creates a new source from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the configuration is invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mongo_config = MongoDbSourceConfig::from_config(config)?;
        Ok(Self::new(mongo_config))
    }

    /// Sets a custom resume token store.
    #[must_use]
    pub fn with_resume_token_store(mut self, store: Box<dyn ResumeTokenStore>) -> Self {
        self.resume_token_store = store;
        self
    }

    /// Returns a reference to the source configuration.
    #[must_use]
    pub fn config(&self) -> &MongoDbSourceConfig {
        &self.config
    }

    /// Returns the last persisted resume token.
    #[must_use]
    pub fn last_resume_token(&self) -> Option<&ResumeToken> {
        self.last_resume_token.as_ref()
    }

    /// Returns the number of buffered events.
    #[must_use]
    pub fn buffered_events(&self) -> usize {
        self.event_buffer.len()
    }

    /// Returns `true` if the stream has been invalidated.
    #[must_use]
    pub fn is_invalidated(&self) -> bool {
        self.invalidated
    }

    /// Enqueues a change event for processing (used by tests and the
    /// background reader task).
    pub fn enqueue_event(&mut self, event: MongoDbChangeEvent) {
        if event.operation_type == OperationType::Invalidate {
            self.invalidated = true;
        }
        self.metrics.record_event(event.operation_type.as_str());
        self.event_buffer.push_back(event);
    }

    /// Drains up to `max_records` events from the buffer and converts
    /// them to an Arrow `RecordBatch`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if Arrow batch construction fails.
    pub fn drain_to_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.event_buffer.is_empty() {
            return Ok(None);
        }

        let count = max_records.min(self.event_buffer.len());
        let events: Vec<MongoDbChangeEvent> = self.event_buffer.drain(..count).collect();

        let batch = events_to_record_batch(&events, &self.schema)?;
        self.metrics.record_batch();

        // Track resume token from the last event in this batch.
        if let Some(last) = events.last() {
            self.last_resume_token = Some(ResumeToken::new(last.resume_token.clone()));
        }

        Ok(Some(SourceBatch::new(batch)))
    }
}

/// Converts a batch of [`MongoDbChangeEvent`]s to an Arrow `RecordBatch`.
fn events_to_record_batch(
    events: &[MongoDbChangeEvent],
    schema: &SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    let len = events.len();

    let mut ns_builder = StringBuilder::with_capacity(len, len * 32);
    let mut op_builder = StringBuilder::with_capacity(len, len * 4);
    let mut dk_builder = StringBuilder::with_capacity(len, len * 64);
    let mut cts_builder = UInt32Builder::with_capacity(len);
    let mut ct_inc_builder = UInt32Builder::with_capacity(len);
    let mut wt_builder = Int64Builder::with_capacity(len);
    let mut fd_builder = StringBuilder::with_capacity(len, len * 128);
    let mut ud_builder = StringBuilder::with_capacity(len, len * 64);
    let mut rt_builder = StringBuilder::with_capacity(len, len * 64);

    for event in events {
        ns_builder.append_value(event.namespace.full_name());
        op_builder.append_value(event.operation_type.as_str());
        dk_builder.append_value(&event.document_key);
        cts_builder.append_value(event.cluster_time_secs);
        ct_inc_builder.append_value(event.cluster_time_inc);
        wt_builder.append_value(event.wall_time_ms);

        match &event.full_document {
            Some(doc) => fd_builder.append_value(doc),
            None => fd_builder.append_null(),
        }

        match &event.update_description {
            Some(desc) => {
                let json = serde_json::to_string(desc)
                    .map_err(|e| ConnectorError::Internal(format!("serialize update_desc: {e}")))?;
                ud_builder.append_value(&json);
            }
            None => ud_builder.append_null(),
        }

        rt_builder.append_value(&event.resume_token);
    }

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(ns_builder.finish()),
            Arc::new(op_builder.finish()),
            Arc::new(dk_builder.finish()),
            Arc::new(cts_builder.finish()),
            Arc::new(ct_inc_builder.finish()),
            Arc::new(wt_builder.finish()),
            Arc::new(fd_builder.finish()),
            Arc::new(ud_builder.finish()),
            Arc::new(rt_builder.finish()),
        ],
    )
    .map_err(|e| ConnectorError::Internal(format!("arrow batch: {e}")))
}

#[async_trait]
impl SourceConnector for MongoDbCdcSource {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.config.validate()?;

        // Load any persisted resume token.
        let persisted_token = self
            .resume_token_store
            .load()
            .await
            .map_err(|e| ConnectorError::Internal(format!("load resume token: {e}")))?;

        if let Some(token) = persisted_token {
            tracing::info!(resume_token = %token, "resuming from persisted token");
            self.last_resume_token = Some(token);
        }

        // Feature-gated: start the background change stream reader.
        #[cfg(feature = "mongodb-cdc")]
        {
            self.start_change_stream_reader().await?;
        }

        self.state = ConnectorState::Running;
        tracing::info!(
            database = %self.config.database,
            collection = %self.config.collection,
            full_document_mode = ?self.config.full_document_mode,
            "MongoDB CDC source opened"
        );

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        #[cfg(feature = "mongodb-cdc")]
        {
            self.drain_channel()?;
        }

        self.drain_to_batch(max_records)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(0);
        if let Some(ref token) = self.last_resume_token {
            cp.set_offset("resume_token", token.as_str());
        }
        cp.set_metadata("connector", "mongodb-cdc");
        cp.set_metadata("database", &self.config.database);
        cp.set_metadata("collection", &self.config.collection);
        cp
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        if let Some(token_str) = checkpoint.get_offset("resume_token") {
            let token = ResumeToken::new(token_str.to_string());
            tracing::info!(resume_token = %token, "restoring from checkpoint");
            self.last_resume_token = Some(token.clone());
            self.resume_token_store
                .save(&token)
                .await
                .map_err(|e| ConnectorError::CheckpointError(format!("save token: {e}")))?;
        }
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => {
                if self.invalidated {
                    HealthStatus::Degraded("change stream invalidated".to_string())
                } else {
                    HealthStatus::Healthy
                }
            }
            ConnectorState::Created => HealthStatus::Unknown,
            ConnectorState::Closed | ConnectorState::Failed => {
                HealthStatus::Unhealthy("closed".to_string())
            }
            _ => HealthStatus::Degraded(format!("state: {}", self.state)),
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Persist the last resume token before shutting down.
        if let Some(ref token) = self.last_resume_token {
            if let Err(e) = self.resume_token_store.save(token).await {
                tracing::warn!(error = %e, "failed to persist resume token on close");
            }
        }

        // Shut down the background reader task.
        #[cfg(feature = "mongodb-cdc")]
        {
            if let Some(tx) = self.reader_shutdown.take() {
                let _ = tx.send(true);
            }
            if let Some(handle) = self.reader_handle.take() {
                let _ = handle.await;
            }
        }

        self.state = ConnectorState::Closed;
        tracing::info!("MongoDB CDC source closed");
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn supports_replay(&self) -> bool {
        // MongoDB change streams support resume from a token.
        true
    }
}

// ── Feature-gated I/O (real MongoDB driver) ──

#[cfg(feature = "mongodb-cdc")]
impl MongoDbCdcSource {
    /// Starts the background change stream reader task.
    async fn start_change_stream_reader(&mut self) -> Result<(), ConnectorError> {
        use mongodb::options::ClientOptions;

        let client_options = ClientOptions::parse(&self.config.connection_uri)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("parse URI: {e}")))?;

        // Warn if pool size may be too small for sharded clusters.
        if let Some(pool) = client_options.max_pool_size {
            if pool <= 1 {
                tracing::warn!(
                    max_pool_size = pool,
                    "max_pool_size is very small — on sharded clusters, \
                     mongos opens per-shard cursors and may exhaust the pool"
                );
            }
        }

        let client = mongodb::Client::with_options(client_options)
            .map_err(|e| ConnectorError::ConnectionFailed(format!("create client: {e}")))?;

        let db = client.database(&self.config.database);

        // Pre-flight: detect time series collections.
        if self.config.collection != "*" {
            preflight_timeseries_guard(&db, &self.config.database, &self.config.collection).await?;
        }

        let (tx, rx) = tokio::sync::mpsc::channel(self.config.max_buffered_events);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let config = self.config.clone();
        let resume_token = self.last_resume_token.clone();
        let data_ready = Arc::clone(&self.data_ready);
        let metrics = Arc::clone(&self.metrics);

        let handle = tokio::spawn(async move {
            if let Err(e) = run_change_stream_reader(
                db,
                config,
                resume_token,
                tx,
                shutdown_rx,
                data_ready,
                metrics,
            )
            .await
            {
                tracing::error!(error = %e, "change stream reader task failed");
            }
        });

        self.reader_handle = Some(handle);
        self.event_rx = Some(rx);
        self.reader_shutdown = Some(shutdown_tx);

        Ok(())
    }

    /// Drains events from the background reader channel into the buffer.
    fn drain_channel(&mut self) -> Result<(), ConnectorError> {
        // Collect payloads first to avoid double borrow.
        let mut payloads = Vec::new();
        if let Some(rx) = &mut self.event_rx {
            while let Ok(payload) = rx.try_recv() {
                payloads.push(payload);
            }
        }

        for payload in payloads {
            match payload {
                ChangeStreamPayload::Event(event) => {
                    self.enqueue_event(*event);
                }
                ChangeStreamPayload::Error(msg) => {
                    self.metrics.record_error();
                    return Err(ConnectorError::ReadError(msg));
                }
            }
        }
        Ok(())
    }
}

/// Pre-flight check: reject time series collections.
#[cfg(feature = "mongodb-cdc")]
async fn preflight_timeseries_guard(
    db: &mongodb::Database,
    database: &str,
    collection: &str,
) -> Result<(), ConnectorError> {
    use futures_util::StreamExt;
    use mongodb::bson::doc;

    let filter = doc! { "name": collection };
    let mut cursor = db
        .list_collections()
        .filter(filter)
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("list collections: {e}")))?;
    if let Some(result) = cursor.next().await {
        let spec = result
            .map_err(|e| ConnectorError::ConnectionFailed(format!("read collection spec: {e}")))?;
        // Check if collection_type indicates time series.
        if spec.collection_type == mongodb::results::CollectionType::Timeseries {
            return Err(ConnectorError::ConfigurationError(format!(
                "time series collection {database}.{collection} does not support change streams"
            )));
        }
    }

    Ok(())
}

/// Maximum consecutive failures before the reader gives up.
#[cfg(feature = "mongodb-cdc")]
const MAX_FAILURES: u32 = 10;

/// Background task that reads from the `MongoDB` change stream and sends
/// events to the source via a channel.
///
/// Uses a `'reconnect` / `'recv` double-loop pattern (mirroring the
/// Postgres CDC source) with exponential backoff capped at 30 seconds.
#[cfg(feature = "mongodb-cdc")]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_change_stream_reader(
    db: mongodb::Database,
    config: MongoDbSourceConfig,
    resume_token: Option<ResumeToken>,
    tx: tokio::sync::mpsc::Sender<ChangeStreamPayload>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    data_ready: Arc<Notify>,
    metrics: Arc<MongoSourceMetrics>,
) -> Result<(), ConnectorError> {
    use futures_util::StreamExt;
    use mongodb::options::ChangeStreamOptions;

    let full_document = match config.full_document_mode {
        super::config::FullDocumentMode::Delta => None,
        super::config::FullDocumentMode::UpdateLookup => {
            Some(mongodb::options::FullDocumentType::UpdateLookup)
        }
        super::config::FullDocumentMode::RequirePostImage => {
            Some(mongodb::options::FullDocumentType::Required)
        }
        super::config::FullDocumentMode::WhenAvailable => {
            Some(mongodb::options::FullDocumentType::WhenAvailable)
        }
    };

    // Initialize the last resume token for reconnection.
    let mut last_token: Option<mongodb::change_stream::event::ResumeToken> =
        if let Some(ref token) = resume_token {
            serde_json::from_str::<mongodb::change_stream::event::ResumeToken>(token.as_str())
                .map(Some)
                .map_err(|e| ConnectorError::ReadError(format!("parse resume token: {e}")))?
        } else {
            None
        };

    // Build a static pipeline (without $changeStreamSplitLargeEvent — the
    // mongodb v3 driver drops the splitEvent field during deserialization,
    // so fragment detection is not possible; see config validation).
    let pipeline: Vec<mongodb::bson::Document> = config
        .pipeline
        .iter()
        .filter_map(|v| mongodb::bson::to_document(v).ok())
        .collect();

    let mut current_db = db;
    let mut consecutive_failures: u32 = 0;

    'reconnect: loop {
        // Build options for this connection attempt.
        let mut options = ChangeStreamOptions::default();
        options.full_document = full_document.clone();
        options.max_await_time = config
            .max_await_time_ms
            .map(std::time::Duration::from_millis);
        options.batch_size = config.batch_size;
        options.resume_after = last_token.clone();

        // Only set start_at_operation_time on fresh starts (no resume token).
        if last_token.is_none() {
            if let Some((secs, inc)) = config.start_at_operation_time {
                options.start_at_operation_time = Some(mongodb::bson::Timestamp {
                    time: secs,
                    increment: inc,
                });
            }
        }

        // Open the change stream cursor.
        let cursor_result = if config.collection == "*" {
            current_db
                .watch()
                .pipeline(pipeline.clone())
                .with_options(options)
                .await
        } else {
            current_db
                .collection::<mongodb::bson::Document>(&config.collection)
                .watch()
                .pipeline(pipeline.clone())
                .with_options(options)
                .await
        };

        let mut cursor = match cursor_result {
            Ok(c) => c,
            Err(e) => {
                consecutive_failures += 1;
                if consecutive_failures >= MAX_FAILURES {
                    let msg =
                        format!("change stream open failed after {MAX_FAILURES} attempts: {e}");
                    tracing::error!(%msg);
                    let _ = tx.send(ChangeStreamPayload::Error(msg)).await;
                    break 'reconnect;
                }
                let backoff_secs = (1u64 << consecutive_failures).min(30);
                tracing::warn!(
                    attempt = consecutive_failures,
                    backoff_secs,
                    error = %e,
                    "failed to open change stream, retrying"
                );
                metrics.record_reconnect();
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break 'reconnect;
                        }
                    }
                    () = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                }
                continue 'reconnect;
            }
        };

        tracing::info!(
            database = %config.database,
            collection = %config.collection,
            resumed = last_token.is_some(),
            "change stream reader started"
        );

        'recv: loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("change stream reader shutting down");
                        break 'reconnect;
                    }
                }
                next = cursor.next() => {
                    match next {
                        Some(Ok(cs_event)) => {
                            consecutive_failures = 0;

                            // Track resume token for reconnection.
                            last_token = Some(cs_event.id.clone());

                            let change_event = parse_change_stream_event(&cs_event)?;

                            if tx
                                .send(ChangeStreamPayload::Event(Box::new(change_event)))
                                .await
                                .is_err()
                            {
                                tracing::warn!("source channel closed, stopping reader");
                                break 'reconnect;
                            }
                            data_ready.notify_one();
                        }
                        Some(Err(e)) => {
                            tracing::error!(error = %e, "change stream error");
                            break 'recv;
                        }
                        None => {
                            tracing::info!("change stream cursor exhausted");
                            break 'recv;
                        }
                    }
                }
            }
        }

        // Exited recv loop due to error or cursor exhaustion — attempt reconnect.
        consecutive_failures += 1;
        if consecutive_failures >= MAX_FAILURES {
            let msg = format!("change stream failed after {MAX_FAILURES} consecutive failures");
            tracing::error!(%msg);
            let _ = tx.send(ChangeStreamPayload::Error(msg)).await;
            break 'reconnect;
        }

        let backoff_secs = (1u64 << consecutive_failures).min(30);
        tracing::warn!(
            resume_token = ?last_token,
            attempt = consecutive_failures,
            backoff_secs,
            "reconnecting change stream"
        );
        metrics.record_reconnect();

        // Interruptible backoff.
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break 'reconnect;
                }
            }
            () = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
        }

        // Re-create client and database for reconnection.
        match mongodb::options::ClientOptions::parse(&config.connection_uri).await {
            Ok(new_opts) => match mongodb::Client::with_options(new_opts) {
                Ok(new_client) => {
                    current_db = new_client.database(&config.database);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to create client on reconnect");
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, "failed to parse URI on reconnect");
            }
        }
    }

    Ok(())
}

/// Parses a `ChangeStreamEvent<Document>` into a [`MongoDbChangeEvent`].
#[cfg(feature = "mongodb-cdc")]
fn parse_change_stream_event(
    event: &mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
) -> Result<MongoDbChangeEvent, ConnectorError> {
    use super::change_event::{Namespace, UpdateDescription};
    use mongodb::change_stream::event::OperationType as MongoOpType;

    let operation_type = match event.operation_type {
        MongoOpType::Insert => OperationType::Insert,
        MongoOpType::Update => OperationType::Update,
        MongoOpType::Replace => OperationType::Replace,
        MongoOpType::Delete => OperationType::Delete,
        MongoOpType::Drop => OperationType::Drop,
        MongoOpType::Rename => OperationType::Rename,
        MongoOpType::Invalidate => OperationType::Invalidate,
        ref other => {
            return Err(ConnectorError::ReadError(format!(
                "unknown operation type: {other:?}"
            )));
        }
    };

    let namespace = event.ns.as_ref().map_or_else(
        || Namespace {
            db: String::new(),
            coll: String::new(),
        },
        |ns| Namespace {
            db: ns.db.clone(),
            coll: ns.coll.clone().unwrap_or_default(),
        },
    );

    let document_key = event
        .document_key
        .as_ref()
        .and_then(|d| serde_json::to_string(d).ok())
        .unwrap_or_default();

    let full_document = event
        .full_document
        .as_ref()
        .and_then(|d| serde_json::to_string(d).ok());

    let update_description = event.update_description.as_ref().map(|ud| {
        let updated_fields = ud
            .updated_fields
            .iter()
            .filter_map(|(k, v)| serde_json::to_value(v).ok().map(|jv| (k.clone(), jv)))
            .collect();

        let removed_fields = ud.removed_fields.clone();

        #[allow(clippy::cast_sign_loss)]
        let truncated_arrays = ud
            .truncated_arrays
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|t| super::change_event::TruncatedArray {
                field: t.field.clone(),
                new_size: t.new_size as u32,
            })
            .collect();

        UpdateDescription {
            updated_fields,
            removed_fields,
            truncated_arrays,
        }
    });

    let (cluster_time_secs, cluster_time_inc) = event
        .cluster_time
        .map_or((0, 0), |ts| (ts.time, ts.increment));

    let wall_time_ms = event
        .wall_time
        .map_or(0, mongodb::bson::DateTime::timestamp_millis);

    // Serialize the ResumeToken via serde (it implements Serialize).
    let resume_token = serde_json::to_string(&event.id).unwrap_or_default();

    Ok(MongoDbChangeEvent {
        operation_type,
        namespace,
        document_key,
        full_document,
        update_description,
        cluster_time_secs,
        cluster_time_inc,
        resume_token,
        wall_time_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::super::change_event::Namespace;
    use super::*;

    fn sample_event(op: OperationType) -> MongoDbChangeEvent {
        MongoDbChangeEvent {
            operation_type: op,
            namespace: Namespace {
                db: "testdb".to_string(),
                coll: "users".to_string(),
            },
            document_key: r#"{"_id": "1"}"#.to_string(),
            full_document: Some(r#"{"_id": "1", "name": "Alice"}"#.to_string()),
            update_description: None,
            cluster_time_secs: 1_700_000_000,
            cluster_time_inc: 1,
            resume_token: r#"{"_data": "token1"}"#.to_string(),
            wall_time_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn test_schema() {
        let schema = mongodb_cdc_envelope_schema();
        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(0).name(), "_namespace");
        assert_eq!(schema.field(6).name(), "_full_document");
        assert!(schema.field(6).is_nullable());
    }

    #[test]
    fn test_new_source() {
        let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
        let source = MongoDbCdcSource::new(config);
        assert_eq!(source.buffered_events(), 0);
        assert!(!source.is_invalidated());
        assert!(source.last_resume_token().is_none());
    }

    #[test]
    fn test_enqueue_event() {
        let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
        let mut source = MongoDbCdcSource::new(config);

        source.enqueue_event(sample_event(OperationType::Insert));
        assert_eq!(source.buffered_events(), 1);
        assert!(!source.is_invalidated());
    }

    #[test]
    fn test_enqueue_invalidate() {
        let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
        let mut source = MongoDbCdcSource::new(config);

        let mut event = sample_event(OperationType::Invalidate);
        event.full_document = None;
        source.enqueue_event(event);
        assert!(source.is_invalidated());
    }

    #[test]
    fn test_events_to_record_batch() {
        let schema = mongodb_cdc_envelope_schema();
        let events = vec![
            sample_event(OperationType::Insert),
            sample_event(OperationType::Delete),
        ];

        let batch = events_to_record_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 9);
    }

    #[test]
    fn test_events_to_record_batch_empty() {
        let schema = mongodb_cdc_envelope_schema();
        let batch = events_to_record_batch(&[], &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 9);
    }

    #[test]
    fn test_drain_to_batch() {
        let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
        let mut source = MongoDbCdcSource::new(config);

        // Empty buffer returns None.
        assert!(source.drain_to_batch(10).unwrap().is_none());

        // Add events and drain.
        for _ in 0..5 {
            source.enqueue_event(sample_event(OperationType::Insert));
        }
        let batch = source.drain_to_batch(3).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(source.buffered_events(), 2);

        // Drain remaining.
        let batch = source.drain_to_batch(10).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(source.buffered_events(), 0);
    }

    #[test]
    fn test_checkpoint() {
        let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "testdb", "users");
        let mut source = MongoDbCdcSource::new(config);

        // Without resume token.
        let cp = source.checkpoint();
        assert!(cp.get_offset("resume_token").is_none());
        assert_eq!(cp.get_metadata("connector"), Some("mongodb-cdc"));

        // With resume token.
        source.last_resume_token = Some(ResumeToken::new("tok123".to_string()));
        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("resume_token"), Some("tok123"));
    }

    #[tokio::test]
    async fn test_restore_checkpoint() {
        let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
        let mut source = MongoDbCdcSource::new(config);

        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset("resume_token", "restored_token");

        source.restore(&cp).await.unwrap();
        assert_eq!(
            source.last_resume_token().unwrap().as_str(),
            "restored_token"
        );
    }

    #[test]
    fn test_health_check() {
        let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
        let mut source = MongoDbCdcSource::new(config);

        assert_eq!(source.health_check(), HealthStatus::Unknown);

        source.state = ConnectorState::Running;
        assert_eq!(source.health_check(), HealthStatus::Healthy);

        source.invalidated = true;
        assert!(matches!(source.health_check(), HealthStatus::Degraded(_)));

        source.state = ConnectorState::Closed;
        assert!(matches!(source.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_drain_tracks_resume_token() {
        let config = MongoDbSourceConfig::new("mongodb://localhost:27017", "db", "coll");
        let mut source = MongoDbCdcSource::new(config);

        let mut event = sample_event(OperationType::Insert);
        event.resume_token = r#"{"_data": "final_token"}"#.to_string();
        source.enqueue_event(event);

        source.drain_to_batch(10).unwrap();
        assert_eq!(
            source.last_resume_token().unwrap().as_str(),
            r#"{"_data": "final_token"}"#
        );
    }
}
