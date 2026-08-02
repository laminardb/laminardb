//! `MongoDB` CDC source connector implementation.
//!
//! Implements [`SourceConnector`] for streaming change events from `MongoDB`
//! change streams into `LaminarDB` as Arrow `RecordBatch`es.
//!
//! # Cancellation Safety
//!
//! Connector lifecycle futures never directly poll the `MongoDB` driver. Driver
//! I/O lives in an owned reader task; cancellation aborts that task so no
//! connection or cursor outlives its connector.

use std::collections::{BTreeMap, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

use arrow_array::builder::{StringBuilder, UInt32Builder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
#[cfg(feature = "mongodb-cdc")]
use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceConnector, SourceContract,
    SourcePosition, SourceStart,
};
use crate::error::ConnectorError;

use super::change_event::{MongoDbChangeEvent, OperationType};
use super::config::MongoDbSourceConfig;
use super::metrics::MongoDbCdcMetrics;

const MAX_RESUME_TOKEN_BYTES: usize = 64 * 1024;
const MONGODB_CHECKPOINT_CONNECTOR: &str = "mongodb-cdc";
const MONGODB_CHECKPOINT_VERSION: &str = "4";
const STREAM_IDENTITY_METADATA: &str = "stream_identity_sha256";
const COLLECTION_UUID_METADATA: &str = "collection_uuid";
const DEPLOYMENT_IDENTITY_METADATA: &str = "deployment_identity";
const RESUME_TOKEN_OFFSET: &str = "resume_token";
const START_AFTER_TOKEN_OFFSET: &str = "start_after_token";
#[cfg(feature = "mongodb-cdc")]
const MAX_MONGODB_WIRE_EVENT_BYTES: usize = 16 * 1024 * 1024;
#[cfg(feature = "mongodb-cdc")]
const CURSOR_MAX_AWAIT_TIME: std::time::Duration = std::time::Duration::from_secs(1);
#[cfg(feature = "mongodb-cdc")]
const READER_STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
enum MongoCheckpointPosition {
    ResumeAfter(String),
    StartAfter(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MongoDeploymentIdentity {
    ReplicaSet(String),
    ShardedCluster(String),
}

impl MongoDeploymentIdentity {
    fn encode(&self) -> String {
        match self {
            Self::ReplicaSet(id) => format!("replica-set:{id}"),
            Self::ShardedCluster(id) => format!("sharded-cluster:{id}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedMongoCheckpoint {
    position: MongoCheckpointPosition,
    collection_uuid: Uuid,
    deployment_identity: MongoDeploymentIdentity,
}

fn mongodb_stream_identity(config: &MongoDbSourceConfig) -> String {
    let mut digest = Sha256::new();
    digest.update(b"laminardb-mongodb-change-stream-v4\0");
    let full_document_mode = match config.full_document_mode {
        super::config::FullDocumentMode::Delta => 0_u8,
        super::config::FullDocumentMode::RequirePostImage => 1,
    };
    digest.update([full_document_mode]);
    digest.update([1]); // showExpandedEvents is always enabled.
    let pipeline = super::config::canonical_pipeline_json(&config.pipeline);
    digest.update(
        u64::try_from(pipeline.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    digest.update(pipeline.as_bytes());
    format!("{:x}", digest.finalize())
}

fn canonical_resume_token(token: &str) -> Result<String, ConnectorError> {
    if token.is_empty() || token.len() > MAX_RESUME_TOKEN_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC resume token size must be 1..={MAX_RESUME_TOKEN_BYTES} bytes"
        )));
    }
    let value: serde_json::Value = serde_json::from_str(token).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "MongoDB CDC resume token is not valid JSON: {error}"
        ))
    })?;
    let serde_json::Value::Object(document) = &value else {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC resume token must be a JSON document".into(),
        ));
    };
    if document.is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC resume token document must not be empty".into(),
        ));
    }
    let canonical = serde_json::to_string(&value).map_err(|error| {
        ConnectorError::Internal(format!("serialize MongoDB CDC resume token: {error}"))
    })?;
    if canonical != token {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC resume token is not in canonical JSON form".into(),
        ));
    }
    Ok(canonical)
}

fn parse_collection_uuid(encoded: &str) -> Result<Uuid, ConnectorError> {
    let uuid = Uuid::parse_str(encoded).map_err(|error| {
        ConnectorError::ConfigurationError(format!("invalid MongoDB CDC collection UUID: {error}"))
    })?;
    if uuid.hyphenated().to_string() != encoded {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC collection UUID is not in canonical lowercase hyphenated form".into(),
        ));
    }
    Ok(uuid)
}

fn parse_deployment_identity(encoded: &str) -> Result<MongoDeploymentIdentity, ConnectorError> {
    let (kind, id) = encoded.split_once(':').ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "MongoDB CDC deployment identity must include its deployment type".into(),
        )
    })?;
    if id.contains(':') {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC deployment identity has too many fields".into(),
        ));
    }
    let object_id = mongodb::bson::oid::ObjectId::parse_str(id).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid MongoDB CDC deployment ObjectId: {error}"
        ))
    })?;
    if object_id.to_hex() != id {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC deployment ObjectId is not canonical lowercase hex".into(),
        ));
    }
    match kind {
        "replica-set" => Ok(MongoDeploymentIdentity::ReplicaSet(id.to_string())),
        "sharded-cluster" => Ok(MongoDeploymentIdentity::ShardedCluster(id.to_string())),
        _ => Err(ConnectorError::ConfigurationError(format!(
            "unknown MongoDB CDC deployment identity type '{kind}'"
        ))),
    }
}

fn parse_mongodb_checkpoint(
    checkpoint: &SourceCheckpoint,
    config: &MongoDbSourceConfig,
) -> Result<ParsedMongoCheckpoint, ConnectorError> {
    let expected_stream_identity = mongodb_stream_identity(config);
    if checkpoint.get_metadata("connector") != Some(MONGODB_CHECKPOINT_CONNECTOR)
        || checkpoint.get_metadata("version") != Some(MONGODB_CHECKPOINT_VERSION)
        || checkpoint.get_metadata("database") != Some(config.database.as_str())
        || checkpoint.get_metadata("collection") != Some(config.collection.as_str())
        || checkpoint.get_metadata(STREAM_IDENTITY_METADATA)
            != Some(expected_stream_identity.as_str())
    {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint identity or format does not match the configured source".into(),
        ));
    }
    let collection_uuid = checkpoint
        .get_metadata(COLLECTION_UUID_METADATA)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "MongoDB CDC checkpoint is missing its collection UUID".into(),
            )
        })
        .and_then(parse_collection_uuid)?;
    let deployment_identity = checkpoint
        .get_metadata(DEPLOYMENT_IDENTITY_METADATA)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "MongoDB CDC checkpoint is missing its deployment identity".into(),
            )
        })
        .and_then(parse_deployment_identity)?;
    if checkpoint.metadata().len() != 7 {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint contains unknown metadata fields".into(),
        ));
    }
    if checkpoint.offsets().len() != 1 {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint must contain exactly one resume token".into(),
        ));
    }
    let position = if let Some(token) = checkpoint.get_offset(RESUME_TOKEN_OFFSET) {
        canonical_resume_token(token).map(MongoCheckpointPosition::ResumeAfter)?
    } else if let Some(token) = checkpoint.get_offset(START_AFTER_TOKEN_OFFSET) {
        canonical_resume_token(token).map(MongoCheckpointPosition::StartAfter)?
    } else {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC checkpoint contains an unknown position key".into(),
        ));
    };
    Ok(ParsedMongoCheckpoint {
        position,
        collection_uuid,
        deployment_identity,
    })
}

enum BufferedMongoPayload {
    Event(Box<MongoDbChangeEvent>),
    HighWatermark {
        token: String,
        requires_start_after: bool,
    },
}

struct BufferedMongoEvent {
    payload: BufferedMongoPayload,
    _byte_permit: OwnedSemaphorePermit,
}

impl BufferedMongoEvent {
    fn new(event: MongoDbChangeEvent, byte_permit: OwnedSemaphorePermit) -> Self {
        Self {
            payload: BufferedMongoPayload::Event(Box::new(event)),
            _byte_permit: byte_permit,
        }
    }

    fn high_watermark(
        token: String,
        requires_start_after: bool,
        byte_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            payload: BufferedMongoPayload::HighWatermark {
                token,
                requires_start_after,
            },
            _byte_permit: byte_permit,
        }
    }

    fn event(&self) -> Option<&MongoDbChangeEvent> {
        match &self.payload {
            BufferedMongoPayload::Event(event) => Some(event),
            BufferedMongoPayload::HighWatermark { .. } => None,
        }
    }
}

fn checked_size_add(total: &mut usize, value: usize) -> Result<(), ConnectorError> {
    *total = total.checked_add(value).ok_or_else(|| {
        ConnectorError::ConfigurationError("MongoDB CDC event size overflow".into())
    })?;
    Ok(())
}

fn json_retained_bytes(value: &serde_json::Value) -> Result<usize, ConnectorError> {
    let mut total = size_of::<serde_json::Value>();
    match value {
        serde_json::Value::String(value) => checked_size_add(&mut total, value.capacity())?,
        serde_json::Value::Array(values) => {
            checked_size_add(
                &mut total,
                values
                    .capacity()
                    .checked_mul(size_of::<serde_json::Value>())
                    .ok_or_else(|| {
                        ConnectorError::ConfigurationError(
                            "MongoDB CDC JSON array size overflow".into(),
                        )
                    })?,
            )?;
            for value in values {
                checked_size_add(&mut total, json_retained_bytes(value)?)?;
            }
        }
        serde_json::Value::Object(values) => {
            // serde_json::Map does not expose its allocation capacity. Charging each live
            // entry plus its recursively owned values is stable across map backends.
            for (key, value) in values {
                checked_size_add(&mut total, size_of::<String>())?;
                checked_size_add(&mut total, key.capacity())?;
                checked_size_add(&mut total, json_retained_bytes(value)?)?;
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
    Ok(total)
}

fn mongo_event_retained_bytes(event: &MongoDbChangeEvent) -> Result<usize, ConnectorError> {
    let mut total = size_of::<BufferedMongoEvent>();
    if let OperationType::Other(value) = &event.operation_type {
        checked_size_add(&mut total, value.capacity())?;
    }
    checked_size_add(&mut total, event.namespace.db.capacity())?;
    checked_size_add(&mut total, event.namespace.coll.capacity())?;
    checked_size_add(&mut total, event.document_key.capacity())?;
    checked_size_add(
        &mut total,
        event.full_document.as_ref().map_or(0, String::capacity),
    )?;
    checked_size_add(&mut total, event.resume_token.capacity())?;

    if let Some(update) = &event.update_description {
        checked_size_add(
            &mut total,
            update
                .updated_fields
                .capacity()
                .checked_mul(size_of::<(String, serde_json::Value)>())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC update field size overflow".into(),
                    )
                })?,
        )?;
        for (key, value) in &update.updated_fields {
            checked_size_add(&mut total, key.capacity())?;
            checked_size_add(&mut total, json_retained_bytes(value)?)?;
        }
        checked_size_add(
            &mut total,
            update
                .removed_fields
                .capacity()
                .checked_mul(size_of::<String>())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC removed field size overflow".into(),
                    )
                })?,
        )?;
        for field in &update.removed_fields {
            checked_size_add(&mut total, field.capacity())?;
        }
        checked_size_add(
            &mut total,
            update
                .truncated_arrays
                .capacity()
                .checked_mul(size_of::<super::change_event::TruncatedArray>())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC truncated array size overflow".into(),
                    )
                })?,
        )?;
        for array in &update.truncated_arrays {
            checked_size_add(&mut total, array.field.capacity())?;
        }
        checked_size_add(
            &mut total,
            update
                .disambiguated_paths
                .capacity()
                .checked_mul(size_of::<(String, serde_json::Value)>())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC disambiguated path size overflow".into(),
                    )
                })?,
        )?;
        for (key, value) in &update.disambiguated_paths {
            checked_size_add(&mut total, key.capacity())?;
            checked_size_add(&mut total, json_retained_bytes(value)?)?;
        }
    }
    Ok(total.max(1))
}

fn mongo_high_watermark_retained_bytes(token_capacity: usize) -> Result<usize, ConnectorError> {
    let mut total = size_of::<BufferedMongoEvent>();
    checked_size_add(&mut total, token_capacity)?;
    Ok(total)
}

/// Returns the Arrow schema for `MongoDB` CDC envelope records.
///
/// | Column              | Type   | Nullable | Description                        |
/// |---------------------|--------|----------|------------------------------------|
/// | `_namespace`        | Utf8   | no       | `database.collection`              |
/// | `_op`               | Utf8   | no       | Operation code (I/U/R/D/DROP/...)  |
/// | `_document_key`     | Utf8   | no       | Document key JSON                  |
/// | `_cluster_time_s`   | UInt32 | no       | Cluster time seconds               |
/// | `_cluster_time_i`   | UInt32 | no       | Cluster time increment             |
/// | `_wall_time_ms`     | Timestamp(ms) | no | Wall clock timestamp             |
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
        Field::new(
            "_wall_time_ms",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            false,
        ),
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
    metrics: Arc<MongoDbCdcMetrics>,

    /// Buffered change events awaiting `poll_batch`.
    event_buffer: VecDeque<BufferedMongoEvent>,

    /// Latest ordered event or post-batch token consumed by `poll_batch`.
    /// The reader's newer cursor token is deliberately not shared with this field.
    checkpoint_resume_token: Option<String>,

    /// Invalidation tokens must be restored with `startAfter`, never `resumeAfter`.
    checkpoint_requires_start_after: bool,

    /// Physical identity of the fixed collection admitted by `listCollections`.
    collection_uuid: Option<Uuid>,

    /// Immutable server-issued identity of the replica set or sharded cluster.
    deployment_identity: Option<MongoDeploymentIdentity>,

    /// Shared ownership limits span the reader channel and poll buffer.
    byte_budget: Arc<Semaphore>,

    /// Notification handle signalled when data arrives from the stream.
    data_ready: Arc<Notify>,

    /// Background change stream reader task handle (feature-gated).
    #[cfg(feature = "mongodb-cdc")]
    reader_handle: Option<tokio::task::JoinHandle<()>>,

    /// Channel receiver for change events from the background task.
    #[cfg(feature = "mongodb-cdc")]
    event_rx: Option<ChangeStreamRx>,

    /// Shutdown signal for the background reader task.
    #[cfg(feature = "mongodb-cdc")]
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,

    /// Terminal reader failure, independent of the bounded event queue.
    #[cfg(feature = "mongodb-cdc")]
    reader_error: Option<tokio::sync::watch::Receiver<Option<MongoReaderFailure>>>,

    /// Admission authority and terminal observer for this connector generation.
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

impl Drop for MongoDbCdcSource {
    fn drop(&mut self) {
        #[cfg(feature = "mongodb-cdc")]
        if let Some(shutdown) = self.reader_shutdown.take() {
            shutdown.send_replace(true);
        }
        #[cfg(feature = "mongodb-cdc")]
        if let Some(handle) = self.reader_handle.take() {
            reap_mongo_reader(handle, &self.task_owner);
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
fn reap_mongo_reader(handle: tokio::task::JoinHandle<()>, task_owner: &ConnectorTaskOwner) {
    let Some(reaper_guard) = task_owner.track() else {
        tracing::warn!("MongoDB CDC task generation was sealed before reader reaping");
        return;
    };
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        // The reader owns a separate task guard. Runtime destruction drops the
        // reader future and resolves that proof without a timer or join guess.
        drop(reaper_guard);
        return;
    };
    drop(runtime.spawn(async move {
        let _reaper_guard = reaper_guard;
        if let Err(error) = handle.await {
            tracing::debug!(%error, "MongoDB CDC retired reader task reaped");
        }
    }));
}

/// Cloneable async sender for the change stream reader → `poll_batch` queue.
#[cfg(feature = "mongodb-cdc")]
type ChangeStreamTx = crossfire::MAsyncTx<crossfire::mpsc::Array<BufferedMongoEvent>>;
/// Single-consumer async receiver for the change stream reader → `poll_batch` queue.
#[cfg(feature = "mongodb-cdc")]
type ChangeStreamRx = crossfire::AsyncRx<crossfire::mpsc::Array<BufferedMongoEvent>>;

#[cfg(feature = "mongodb-cdc")]
#[derive(Debug)]
struct MongoReaderReady {
    initial_resume_token: Option<String>,
    collection_uuid: Uuid,
    deployment_identity: MongoDeploymentIdentity,
}

#[cfg(feature = "mongodb-cdc")]
#[derive(Clone, Debug)]
enum MongoReaderFailure {
    Configuration(String),
    Connection(String),
    Read(String),
}

#[cfg(feature = "mongodb-cdc")]
impl MongoReaderFailure {
    fn from_connector(error: &ConnectorError) -> Self {
        match error {
            ConnectorError::ConfigurationError(message) => Self::Configuration(message.clone()),
            ConnectorError::ConnectionFailed(message) => Self::Connection(message.clone()),
            ConnectorError::ReadError(message) => Self::Read(message.clone()),
            error if error.is_transient() => Self::Read(error.to_string()),
            error => Self::Configuration(error.to_string()),
        }
    }

    fn into_connector(self) -> ConnectorError {
        match self {
            Self::Configuration(message) => ConnectorError::ConfigurationError(message),
            Self::Connection(message) => ConnectorError::ConnectionFailed(message),
            Self::Read(message) => ConnectorError::ReadError(message),
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MongoCollectionObservation {
    collection_uuid: Uuid,
    post_images_enabled: bool,
}

#[cfg(feature = "mongodb-cdc")]
#[derive(Clone, Debug, PartialEq, Eq)]
struct MongoAdmissionObservation {
    deployment_identity: MongoDeploymentIdentity,
    collection: MongoCollectionObservation,
}

#[cfg(feature = "mongodb-cdc")]
struct MongoReaderAdmissionGuard {
    shutdown: Option<tokio::sync::watch::Sender<bool>>,
}

#[cfg(feature = "mongodb-cdc")]
impl MongoReaderAdmissionGuard {
    fn new(shutdown: tokio::sync::watch::Sender<bool>) -> Self {
        Self {
            shutdown: Some(shutdown),
        }
    }

    fn disarm(&mut self) {
        self.shutdown = None;
    }
}

#[cfg(feature = "mongodb-cdc")]
impl Drop for MongoReaderAdmissionGuard {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.as_ref() {
            shutdown.send_replace(true);
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
#[derive(Clone, Debug, PartialEq)]
enum MongoResumePosition {
    ResumeAfter(mongodb::change_stream::event::ResumeToken),
    StartAfter(mongodb::change_stream::event::ResumeToken),
}

impl MongoDbCdcSource {
    /// Creates a new `MongoDB` CDC source with the given configuration.
    #[must_use]
    pub fn new(config: MongoDbSourceConfig, registry: Option<&prometheus::Registry>) -> Self {
        let byte_budget = Arc::new(Semaphore::new(config.max_buffered_bytes));
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            byte_budget,
            config,
            state: ConnectorState::Created,
            schema: mongodb_cdc_envelope_schema(),
            metrics: Arc::new(MongoDbCdcMetrics::new(registry)),
            event_buffer: VecDeque::new(),
            checkpoint_resume_token: None,
            checkpoint_requires_start_after: false,
            collection_uuid: None,
            deployment_identity: None,
            data_ready: Arc::new(Notify::new()),
            #[cfg(feature = "mongodb-cdc")]
            reader_handle: None,
            #[cfg(feature = "mongodb-cdc")]
            event_rx: None,
            #[cfg(feature = "mongodb-cdc")]
            reader_shutdown: None,
            #[cfg(feature = "mongodb-cdc")]
            reader_error: None,
            task_owner,
            task_tracker,
        }
    }

    #[cfg(test)]
    fn buffered_events(&self) -> usize {
        self.event_buffer
            .iter()
            .filter(|item| item.event().is_some())
            .count()
    }

    /// Enqueues a change event for focused source tests without bypassing production bounds.
    #[cfg(test)]
    fn enqueue_event(&mut self, event: MongoDbChangeEvent) -> Result<(), ConnectorError> {
        let retained_bytes = mongo_event_retained_bytes(&event)?;
        let byte_permits = u32::try_from(retained_bytes).map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "MongoDB CDC event exceeds the hard byte bound: event={retained_bytes}, limit={}",
                self.config.max_buffered_bytes
            ))
        })?;
        if retained_bytes > self.config.max_buffered_bytes {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC event exceeds the hard byte bound: event={retained_bytes}, limit={}",
                self.config.max_buffered_bytes
            )));
        }
        let byte_permit = Arc::clone(&self.byte_budget)
            .try_acquire_many_owned(byte_permits)
            .map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "MongoDB CDC buffered bytes reached the hard bound: limit={}",
                    self.config.max_buffered_bytes
                ))
            })?;
        self.metrics.record_event(event.operation_type.as_str());
        self.event_buffer
            .push_back(BufferedMongoEvent::new(event, byte_permit));
        Ok(())
    }

    #[cfg(test)]
    fn enqueue_high_watermark(&mut self, token: &str) -> Result<(), ConnectorError> {
        let token = canonical_resume_token(token)?;
        let retained_bytes = mongo_high_watermark_retained_bytes(token.capacity())?;
        if retained_bytes > self.config.max_buffered_bytes {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC high watermark exceeds the hard byte bound: item={retained_bytes}, \
                 limit={}",
                self.config.max_buffered_bytes
            )));
        }
        let permits = u32::try_from(retained_bytes).map_err(|_| {
            ConnectorError::ConfigurationError("MongoDB CDC high watermark is too large".into())
        })?;
        let byte_permit = Arc::clone(&self.byte_budget)
            .try_acquire_many_owned(permits)
            .map_err(|_| {
                ConnectorError::ConfigurationError(
                    "MongoDB CDC high watermark exceeded the byte budget".into(),
                )
            })?;
        self.event_buffer
            .push_back(BufferedMongoEvent::high_watermark(
                token,
                false,
                byte_permit,
            ));
        Ok(())
    }

    /// Drains up to `max_records` events from the buffer and converts
    /// them to an Arrow `RecordBatch`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if Arrow batch construction fails.
    fn drain_to_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if max_records == 0 || self.event_buffer.is_empty() {
            return Ok(None);
        }

        let count = max_records.min(self.event_buffer.len());
        // An invalidate token changes the legal resume option. End the batch exactly there even
        // when the background reader has already reopened with startAfter and queued later data.
        let count = self
            .event_buffer
            .iter()
            .take(count)
            .position(|item| {
                item.event()
                    .is_some_and(|event| event.operation_type == OperationType::Invalidate)
            })
            .map_or(count, |index| index + 1);
        let items: Vec<BufferedMongoEvent> = self.event_buffer.drain(..count).collect();
        let events: Vec<&MongoDbChangeEvent> =
            items.iter().filter_map(BufferedMongoEvent::event).collect();
        let (position_token, requires_start_after) = match items.last() {
            Some(item) => {
                let (token, start_after) = match &item.payload {
                    BufferedMongoPayload::Event(event) => (
                        event.resume_token.as_str(),
                        event.operation_type == OperationType::Invalidate,
                    ),
                    BufferedMongoPayload::HighWatermark {
                        token,
                        requires_start_after,
                    } => (token.as_str(), *requires_start_after),
                };
                match canonical_resume_token(token) {
                    Ok(token) => (token, start_after),
                    Err(error) => {
                        drop(events);
                        for item in items.into_iter().rev() {
                            self.event_buffer.push_front(item);
                        }
                        return Err(error);
                    }
                }
            }
            None => return Ok(None),
        };

        if events.is_empty() {
            self.checkpoint_resume_token = Some(position_token);
            self.checkpoint_requires_start_after = requires_start_after;
            return Ok(None);
        }

        let batch = match events_to_record_batch_refs(&events, &self.schema) {
            Ok(batch) => batch,
            Err(error) => {
                drop(events);
                for item in items.into_iter().rev() {
                    self.event_buffer.push_front(item);
                }
                return Err(error);
            }
        };
        self.metrics.record_batch();
        self.checkpoint_resume_token = Some(position_token);
        self.checkpoint_requires_start_after = requires_start_after;

        Ok(Some(SourceBatch::new(batch)))
    }
}

/// Converts a batch of [`MongoDbChangeEvent`]s to an Arrow `RecordBatch`.
#[cfg(test)]
fn events_to_record_batch(
    events: &[MongoDbChangeEvent],
    schema: &SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    let events: Vec<&MongoDbChangeEvent> = events.iter().collect();
    events_to_record_batch_refs(&events, schema)
}

fn events_to_record_batch_refs(
    events: &[&MongoDbChangeEvent],
    schema: &SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    let len = events.len();

    let mut ns_builder = StringBuilder::with_capacity(len, len * 32);
    let mut op_builder = StringBuilder::with_capacity(len, len * 4);
    let mut dk_builder = StringBuilder::with_capacity(len, len * 64);
    let mut cts_builder = UInt32Builder::with_capacity(len);
    let mut ct_inc_builder = UInt32Builder::with_capacity(len);
    let mut wt_builder = arrow_array::builder::TimestampMillisecondBuilder::with_capacity(len);
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
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
        let mut parsed = if config.properties().is_empty() {
            self.config.clone()
        } else {
            MongoDbSourceConfig::from_config(config)?
        };
        parsed.normalize_pipeline()?;
        parsed.validate()?;
        let pipeline = super::config::canonical_pipeline_json(&parsed.pipeline);

        Ok(Some(BTreeMap::from([
            ("collection".into(), parsed.collection),
            ("database".into(), parsed.database),
            (
                "full.document.mode".into(),
                parsed.full_document_mode.to_string(),
            ),
            ("pipeline".into(), pipeline),
            ("wire.protocol".into(), "change-stream-expanded-v1".into()),
        ])))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        let (config, position, _) = request.into_parts();
        let parsed_config = if config.properties().is_empty() {
            let mut config = self.config.clone();
            config.normalize_pipeline()?;
            config.validate()?;
            config
        } else {
            MongoDbSourceConfig::from_config(&config)?
        };
        let (
            checkpoint_resume_token,
            checkpoint_requires_start_after,
            initial_resume_position,
            expected_collection_uuid,
            expected_deployment_identity,
        ) = match position {
            SourcePosition::Initial => (None, false, None, None, None),
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => {
                let ParsedMongoCheckpoint {
                    position,
                    collection_uuid,
                    deployment_identity,
                } = parse_mongodb_checkpoint(&checkpoint, &parsed_config).map_err(|error| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid MongoDB CDC checkpoint {attempt:?}: {error}"
                    ))
                })?;
                match position {
                    MongoCheckpointPosition::ResumeAfter(token) => {
                        let driver_token = serde_json::from_str(&token).map_err(|error| {
                            ConnectorError::ConfigurationError(format!(
                                "invalid MongoDB CDC resume token in checkpoint {attempt:?}: \
                                 {error}"
                            ))
                        })?;
                        (
                            Some(token),
                            false,
                            Some(MongoResumePosition::ResumeAfter(driver_token)),
                            Some(collection_uuid),
                            Some(deployment_identity),
                        )
                    }
                    MongoCheckpointPosition::StartAfter(token) => {
                        let driver_token = serde_json::from_str(&token).map_err(|error| {
                            ConnectorError::ConfigurationError(format!(
                                "invalid MongoDB CDC start-after token in checkpoint {attempt:?}: \
                                 {error}"
                            ))
                        })?;
                        (
                            Some(token),
                            true,
                            Some(MongoResumePosition::StartAfter(driver_token)),
                            Some(collection_uuid),
                            Some(deployment_identity),
                        )
                    }
                }
            }
        };

        self.start_change_stream_reader(
            parsed_config,
            checkpoint_resume_token,
            checkpoint_requires_start_after,
            initial_resume_position,
            expected_collection_uuid,
            expected_deployment_identity,
        )
        .await?;

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
        self.drain_channel(max_records.saturating_sub(self.event_buffer.len()));
        if let Some(batch) = self.drain_to_batch(max_records)? {
            return Ok(Some(batch));
        }
        self.check_reader_error()?;
        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        let Some(collection_uuid) = self.collection_uuid else {
            // A configured namespace is not a physical replay identity until admission has read
            // the server-assigned collection UUID.
            return checkpoint;
        };
        let Some(deployment_identity) = self.deployment_identity.as_ref() else {
            return checkpoint;
        };
        if let Some(token) = self.checkpoint_resume_token.as_ref() {
            checkpoint.set_offset(
                if self.checkpoint_requires_start_after {
                    START_AFTER_TOKEN_OFFSET
                } else {
                    RESUME_TOKEN_OFFSET
                },
                token,
            );
        } else {
            // Before a fresh source has opened, it has no lossless replay position.
            return checkpoint;
        }
        checkpoint.set_metadata("connector", MONGODB_CHECKPOINT_CONNECTOR);
        checkpoint.set_metadata("version", MONGODB_CHECKPOINT_VERSION);
        checkpoint.set_metadata("database", &self.config.database);
        checkpoint.set_metadata("collection", &self.config.collection);
        checkpoint.set_metadata(
            COLLECTION_UUID_METADATA,
            collection_uuid.hyphenated().to_string(),
        );
        checkpoint.set_metadata(DEPLOYMENT_IDENTITY_METADATA, deployment_identity.encode());
        checkpoint.set_metadata(
            STREAM_IDENTITY_METADATA,
            mongodb_stream_identity(&self.config),
        );
        checkpoint
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "mongodb-cdc")]
        let mut reader_join_error = None;
        #[cfg(feature = "mongodb-cdc")]
        {
            if let Some(tx) = self.reader_shutdown.as_ref() {
                tx.send_replace(true);
            }
            let mut detach_reader = false;
            if let Some(handle) = self.reader_handle.as_mut() {
                match tokio::time::timeout(READER_SHUTDOWN_TIMEOUT, &mut *handle).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if error.is_cancelled() => {}
                    Ok(Err(error)) => reader_join_error = Some(error.to_string()),
                    Err(_) => {
                        tracing::warn!(
                            "MongoDB CDC reader exceeded its close deadline; its tracked reaper retains shutdown ownership"
                        );
                        detach_reader = true;
                    }
                }
            }
            if detach_reader {
                let handle = self
                    .reader_handle
                    .take()
                    .expect("reader handle was present while awaiting it");
                reap_mongo_reader(handle, &self.task_owner);
            } else {
                self.reader_handle = None;
            }
            self.reader_shutdown = None;
            self.event_rx = None;
            self.reader_error = None;
        }

        self.event_buffer.clear();
        self.state = ConnectorState::Closed;
        tracing::info!("MongoDB CDC source closed");
        #[cfg(feature = "mongodb-cdc")]
        if let Some(error) = reader_join_error {
            return Err(ConnectorError::ReadError(format!(
                "MongoDB CDC reader task failed during close: {error}"
            )));
        }
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        if config.properties().is_empty() {
            self.config.validate()?;
        } else {
            MongoDbSourceConfig::from_config(config)?;
        }
        Err(ConnectorError::ConfigurationError(
            "MongoDB CDC emits a raw JSON change envelope; canonical primary-keyed row/delete records are required"
                .into(),
        ))
    }
}

// ── Feature-gated I/O (real MongoDB driver) ──

#[cfg(feature = "mongodb-cdc")]
fn clamp_source_startup_timeout(configured: Option<std::time::Duration>) -> std::time::Duration {
    configured
        .filter(|timeout| !timeout.is_zero())
        .map_or(READER_STARTUP_TIMEOUT, |timeout| {
            timeout.min(READER_STARTUP_TIMEOUT)
        })
}

#[cfg(feature = "mongodb-cdc")]
async fn source_client_options(
    connection_uri: &str,
) -> Result<mongodb::options::ClientOptions, ConnectorError> {
    let mut options = mongodb::options::ClientOptions::parse(connection_uri)
        .await
        .map_err(|error| ConnectorError::ConfigurationError(format!("parse URI: {error}")))?;
    super::sink::harden_mongodb_tls(&mut options)?;
    options.connect_timeout = Some(clamp_source_startup_timeout(options.connect_timeout));
    options.server_selection_timeout = Some(clamp_source_startup_timeout(
        options.server_selection_timeout,
    ));

    if let Some(pool) = options.max_pool_size {
        if pool <= 1 {
            tracing::warn!(
                max_pool_size = pool,
                "max_pool_size is very small; mongos may exhaust per-shard cursors"
            );
        }
    }
    Ok(options)
}

#[cfg(feature = "mongodb-cdc")]
async fn source_database(
    connection_uri: &str,
    database: &str,
) -> Result<mongodb::Database, ConnectorError> {
    let options = source_client_options(connection_uri).await?;
    let client = mongodb::Client::with_options(options)
        .map_err(|error| ConnectorError::ConfigurationError(format!("create client: {error}")))?;
    Ok(client.database(database))
}

#[cfg(feature = "mongodb-cdc")]
async fn await_mongo_reader_ready(
    ready_rx: tokio::sync::oneshot::Receiver<Result<MongoReaderReady, MongoReaderFailure>>,
    shutdown_tx: &tokio::sync::watch::Sender<bool>,
    handle: &mut tokio::task::JoinHandle<()>,
) -> Result<MongoReaderReady, ConnectorError> {
    let (error, include_join_error) =
        match tokio::time::timeout(READER_STARTUP_TIMEOUT, ready_rx).await {
            Ok(Ok(Ok(ready))) => return Ok(ready),
            Ok(Ok(Err(error))) => (error.into_connector(), false),
            Ok(Err(_)) => (
                ConnectorError::ReadError(
                    "MongoDB CDC reader exited before opening the change stream".into(),
                ),
                true,
            ),
            Err(_) => (
                ConnectorError::ReadError(format!(
                    "MongoDB CDC did not open a change stream within the {READER_STARTUP_TIMEOUT:?} startup deadline"
                )),
                false,
            ),
        };

    shutdown_tx.send_replace(true);
    let Ok(join_result) = tokio::time::timeout(READER_SHUTDOWN_TIMEOUT, &mut *handle).await else {
        tracing::warn!(
            "MongoDB CDC admission reader exceeded its shutdown deadline; the retired generation remains tracked until it exits"
        );
        return Err(error);
    };
    let error = if include_join_error {
        match join_result {
            Err(join_error) => ConnectorError::ReadError(format!("{error}: {join_error}")),
            _ => error,
        }
    } else {
        error
    };
    Err(error)
}

#[cfg(feature = "mongodb-cdc")]
impl MongoDbCdcSource {
    /// Starts the background change stream reader task.
    async fn start_change_stream_reader(
        &mut self,
        config: MongoDbSourceConfig,
        checkpoint_resume_token: Option<String>,
        checkpoint_requires_start_after: bool,
        initial_resume_position: Option<MongoResumePosition>,
        expected_collection_uuid: Option<Uuid>,
        expected_deployment_identity: Option<MongoDeploymentIdentity>,
    ) -> Result<(), ConnectorError> {
        if self.reader_handle.is_some() {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: "reader already started".into(),
            });
        }
        if !self.event_buffer.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB CDC cannot start with pre-buffered test events".into(),
            ));
        }
        let max_buffered_bytes = config.max_buffered_bytes;
        let byte_budget = Arc::new(Semaphore::new(max_buffered_bytes));

        let channel_capacity = config.reader_channel_capacity();
        let (tx, rx) = crossfire::mpsc::bounded_async::<BufferedMongoEvent>(channel_capacity);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (error_tx, error_rx) = tokio::sync::watch::channel(None);
        let (ready_tx, ready_rx) =
            tokio::sync::oneshot::channel::<Result<MongoReaderReady, MongoReaderFailure>>();
        let reader_config = config.clone();
        let data_ready = Arc::clone(&self.data_ready);
        let terminal_ready = Arc::clone(&self.data_ready);
        let metrics = Arc::clone(&self.metrics);
        let task_byte_budget = Arc::clone(&byte_budget);

        let reader_guard = self.task_owner.track().ok_or_else(|| {
            ConnectorError::Internal("MongoDB CDC connector generation is already retired".into())
        })?;

        let mut handle = tokio::spawn(async move {
            let _reader_guard = reader_guard;
            let result = async {
                let db =
                    match source_database(&reader_config.connection_uri, &reader_config.database)
                        .await
                    {
                        Ok(database) => database,
                        Err(error) => {
                            let admission_error = MongoReaderFailure::from_connector(&error);
                            let _ = ready_tx.send(Err(admission_error));
                            return Err(error);
                        }
                    };
                run_change_stream_reader(
                    db,
                    reader_config,
                    tx,
                    shutdown_rx,
                    data_ready,
                    metrics,
                    task_byte_budget,
                    max_buffered_bytes,
                    initial_resume_position,
                    expected_collection_uuid,
                    expected_deployment_identity,
                    ready_tx,
                )
                .await
            }
            .await;
            if let Err(e) = result {
                tracing::error!(error = %e, "change stream reader task failed");
                error_tx.send_replace(Some(MongoReaderFailure::from_connector(&e)));
                terminal_ready.notify_one();
            }
        });
        let mut admission_guard = MongoReaderAdmissionGuard::new(shutdown_tx.clone());

        let ready = await_mongo_reader_ready(ready_rx, &shutdown_tx, &mut handle).await?;

        admission_guard.disarm();
        self.config = config;
        self.checkpoint_resume_token = ready.initial_resume_token.or(checkpoint_resume_token);
        self.checkpoint_requires_start_after = checkpoint_requires_start_after;
        self.collection_uuid = Some(ready.collection_uuid);
        self.deployment_identity = Some(ready.deployment_identity);
        self.byte_budget = byte_budget;
        self.reader_handle = Some(handle);
        self.event_rx = Some(rx);
        self.reader_shutdown = Some(shutdown_tx);
        self.reader_error = Some(error_rx);
        Ok(())
    }

    /// Drains events from the background reader channel into the buffer.
    fn drain_channel(&mut self, max_events: usize) {
        let max_events = max_events.min(self.config.reader_channel_capacity());
        for _ in 0..max_events {
            let item = {
                let Some(rx) = self.event_rx.as_mut() else {
                    break;
                };
                let Ok(item) = rx.try_recv() else {
                    break;
                };
                item
            };
            if let Some(event) = item.event() {
                self.metrics.record_event(event.operation_type.as_str());
            }
            self.event_buffer.push_back(item);
        }
    }

    fn check_reader_error(&mut self) -> Result<(), ConnectorError> {
        let error = self
            .reader_error
            .as_mut()
            .and_then(|receiver| receiver.borrow_and_update().clone());
        if let Some(error) = error {
            self.metrics.record_error();
            return Err(error.into_connector());
        }
        Ok(())
    }
}

#[cfg(feature = "mongodb-cdc")]
fn mongodb_identity_command_is_permanent(code: i32, code_name: &str) -> bool {
    matches!(
        code,
        13 | 18 | 20 | 26 | 59 | 72 | 76 | 115 | 123 | 323 | 8000
    ) || matches!(
        code_name,
        "Unauthorized"
            | "AuthenticationFailed"
            | "IllegalOperation"
            | "NamespaceNotFound"
            | "CommandNotFound"
            | "InvalidOptions"
            | "NoReplicationEnabled"
            | "CommandNotSupported"
            | "NotAReplicaSet"
            | "APIStrictError"
            | "AtlasError"
    )
}

#[cfg(feature = "mongodb-cdc")]
fn mongodb_identity_probe_is_permanent(error: &mongodb::error::Error) -> bool {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Authentication { .. } => true,
        mongodb::error::ErrorKind::Command(command) => {
            mongodb_identity_command_is_permanent(command.code, &command.code_name)
        }
        _ => false,
    }
}

#[cfg(feature = "mongodb-cdc")]
async fn observe_mongodb_deployment(
    db: &mongodb::Database,
) -> Result<MongoDeploymentIdentity, ConnectorError> {
    let hello = db
        .run_command(mongodb::bson::doc! { "hello": 1 })
        .await
        .map_err(|error| {
            if mongodb_identity_probe_is_permanent(&error) {
                return ConnectorError::ConfigurationError(format!(
                    "MongoDB CDC cannot inspect deployment topology; verify credentials and \
                     deployment command support: {error}"
                ));
            }
            ConnectorError::ConnectionFailed(format!(
                "inspect MongoDB deployment topology with hello: {error}"
            ))
        })?;

    if hello.get_str("msg").ok() == Some("isdbgrid") {
        let version = db
            .client()
            .database("config")
            .collection::<mongodb::bson::Document>("version")
            .find_one(mongodb::bson::doc! { "_id": 1 })
            .projection(mongodb::bson::doc! { "clusterId": 1 })
            .await
            .map_err(|error| {
                if mongodb_identity_probe_is_permanent(&error) {
                    ConnectorError::ConfigurationError(format!(
                        "MongoDB CDC requires read access to config.version {{_id: 1}}.clusterId \
                         to bind checkpoints to the sharded cluster identity: {error}"
                    ))
                } else {
                    ConnectorError::ConnectionFailed(format!(
                        "read MongoDB sharded cluster identity from config.version: {error}"
                    ))
                }
            })?
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "MongoDB config.version {_id: 1} is missing; cannot bind CDC checkpoints to \
                     this sharded cluster"
                        .into(),
                )
            })?;
        let cluster_id = version.get_object_id("clusterId").map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "MongoDB config.version.clusterId is missing or not an ObjectId: {error}"
            ))
        })?;
        return Ok(MongoDeploymentIdentity::ShardedCluster(cluster_id.to_hex()));
    }

    if hello.get_str("setName").is_ok() {
        let response = db
            .client()
            .database("admin")
            .run_command(mongodb::bson::doc! { "replSetGetConfig": 1 })
            .await
            .map_err(|error| {
                if mongodb_identity_probe_is_permanent(&error) {
                    ConnectorError::ConfigurationError(format!(
                        "MongoDB CDC requires replSetGetConfig access to bind checkpoints to the \
                         replica-set identity; Atlas M0 and Flex tiers do not support this \
                         command: {error}"
                    ))
                } else {
                    ConnectorError::ConnectionFailed(format!(
                        "read MongoDB replica-set identity with replSetGetConfig: {error}"
                    ))
                }
            })?;
        let replica_set_id = response
            .get_document("config")
            .and_then(|config| config.get_document("settings"))
            .and_then(|settings| settings.get_object_id("replicaSetId"))
            .map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "MongoDB replSetGetConfig omitted settings.replicaSetId: {error}"
                ))
            })?;
        return Ok(MongoDeploymentIdentity::ReplicaSet(replica_set_id.to_hex()));
    }

    Err(ConnectorError::ConfigurationError(
        "MongoDB CDC requires a replica set or sharded cluster; hello reported neither topology"
            .into(),
    ))
}

#[cfg(feature = "mongodb-cdc")]
async fn observe_mongodb_admission(
    db: &mongodb::Database,
    database: &str,
    collection: &str,
) -> Result<MongoAdmissionObservation, ConnectorError> {
    let (deployment_identity, collection) = tokio::try_join!(
        observe_mongodb_deployment(db),
        observe_mongodb_collection(db, database, collection),
    )?;
    Ok(MongoAdmissionObservation {
        deployment_identity,
        collection,
    })
}

/// Read the immutable identity and post-image capability for one fixed collection.
#[cfg(feature = "mongodb-cdc")]
async fn observe_mongodb_collection(
    db: &mongodb::Database,
    database: &str,
    collection: &str,
) -> Result<MongoCollectionObservation, ConnectorError> {
    let mut cursor = db
        .list_collections()
        .filter(mongodb::bson::doc! { "name": collection })
        .batch_size(1)
        .await
        .map_err(|error| {
            if mongodb_identity_probe_is_permanent(&error) {
                ConnectorError::ConfigurationError(format!(
                    "MongoDB CDC requires database-scoped listCollections access to bind \
                     {database}.{collection} to its collection UUID: {error}"
                ))
            } else {
                ConnectorError::ConnectionFailed(format!(
                    "inspect MongoDB collection {database}.{collection}: {error}"
                ))
            }
        })?;
    let spec = cursor
        .try_next()
        .await
        .map_err(|error| {
            ConnectorError::ConnectionFailed(format!(
                "read MongoDB collection identity for {database}.{collection}: {error}"
            ))
        })?
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "MongoDB CDC collection {database}.{collection} does not exist; create the fixed \
                 collection before starting the source"
            ))
        })?;

    match spec.collection_type {
        mongodb::results::CollectionType::Collection => {}
        mongodb::results::CollectionType::Timeseries => {
            return Err(ConnectorError::ConfigurationError(format!(
                "time series collection {database}.{collection} does not support change streams"
            )));
        }
        mongodb::results::CollectionType::View => {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC source {database}.{collection} must be a collection, not a view"
            )));
        }
        _ => {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC source {database}.{collection} has an unsupported collection type"
            )));
        }
    }

    let post_images_enabled = spec
        .options
        .change_stream_pre_and_post_images
        .as_ref()
        .is_some_and(|options| options.enabled);
    let binary = spec.info.uuid.ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "MongoDB collection {database}.{collection} did not expose an immutable UUID"
        ))
    })?;
    if binary.subtype != mongodb::bson::spec::BinarySubtype::Uuid || binary.bytes.len() != 16 {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB collection {database}.{collection} returned a non-standard collection UUID"
        )));
    }
    let collection_uuid = Uuid::from_slice(&binary.bytes).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "invalid UUID for MongoDB collection {database}.{collection}: {error}"
        ))
    })?;
    Ok(MongoCollectionObservation {
        collection_uuid,
        post_images_enabled,
    })
}

/// Maximum consecutive failures before the reader gives up.
#[cfg(feature = "mongodb-cdc")]
const MAX_FAILURES: u32 = 10;

#[cfg(feature = "mongodb-cdc")]
const READER_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg(feature = "mongodb-cdc")]
enum ChangeStreamRead {
    Stop,
    Reconnect,
}

#[cfg(feature = "mongodb-cdc")]
fn reader_stopping(shutdown_rx: &tokio::sync::watch::Receiver<bool>) -> bool {
    *shutdown_rx.borrow() || shutdown_rx.has_changed().is_err()
}

#[cfg(feature = "mongodb-cdc")]
fn change_stream_options(
    config: &MongoDbSourceConfig,
    position: Option<&MongoResumePosition>,
) -> mongodb::options::ChangeStreamOptions {
    let mut options = mongodb::options::ChangeStreamOptions::default();
    options.full_document = match config.full_document_mode {
        super::config::FullDocumentMode::Delta => None,
        super::config::FullDocumentMode::RequirePostImage => {
            Some(mongodb::options::FullDocumentType::Required)
        }
    };
    options.max_await_time = Some(CURSOR_MAX_AWAIT_TIME);
    options.batch_size = Some(config.cursor_batch_size());
    options.show_expanded_events = Some(true);
    match position {
        Some(MongoResumePosition::ResumeAfter(token)) => options.resume_after = Some(token.clone()),
        Some(MongoResumePosition::StartAfter(token)) => options.start_after = Some(token.clone()),
        None => {}
    }
    options
}

#[cfg(feature = "mongodb-cdc")]
fn bootstrap_change_stream_options(
    config: &MongoDbSourceConfig,
) -> mongodb::options::ChangeStreamOptions {
    let mut options = change_stream_options(config, None);
    // MongoDB guarantees an empty firstBatch for batchSize=0, so its PBRT is an exact opening
    // cut and cannot skip concurrently buffered events.
    options.batch_size = Some(0);
    options
}

#[cfg(feature = "mongodb-cdc")]
async fn forward_change_stream(
    cursor: &mut mongodb::change_stream::ChangeStream<
        mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
    >,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    resume_position: &mut Option<MongoResumePosition>,
    tx: &ChangeStreamTx,
    data_ready: &Notify,
    consecutive_failures: &mut u32,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    metrics: &MongoDbCdcMetrics,
) -> Result<ChangeStreamRead, ConnectorError> {
    loop {
        if reader_stopping(shutdown_rx) {
            tracing::info!("change stream reader shutting down");
            return Ok(ChangeStreamRead::Stop);
        }

        // Poll getMore to completion during normal operation; maxAwaitTime keeps cooperative
        // shutdown prompt. The connector aborts and joins the owned task at its hard deadline.
        let next = cursor.next_if_any().await;
        if reader_stopping(shutdown_rx) {
            tracing::info!("change stream reader shutting down after completed getMore");
            return Ok(ChangeStreamRead::Stop);
        }

        match next {
            Ok(Some(event)) => {
                *consecutive_failures = 0;
                let event_token = event.id.clone();
                let wire_bytes = mongodb::bson::to_vec(&event)
                    .map_err(|error| {
                        ConnectorError::ReadError(format!(
                            "serialize change stream event for byte accounting: {error}"
                        ))
                    })?
                    .len();
                if wire_bytes > MAX_MONGODB_WIRE_EVENT_BYTES {
                    return Err(ConnectorError::ReadError(format!(
                        "MongoDB CDC wire event exceeds the supported unsplit BSON bound: \
                                 event={wire_bytes}, limit={MAX_MONGODB_WIRE_EVENT_BYTES}"
                    )));
                }
                metrics.record_bytes(u64::try_from(wire_bytes).unwrap_or(u64::MAX));
                let change_event = parse_change_stream_event(&event)?;
                let invalidated = change_event.operation_type == OperationType::Invalidate;
                let Some(change_event) = acquire_mongo_event_ownership(
                    change_event,
                    byte_budget,
                    max_buffered_bytes,
                    shutdown_rx,
                )
                .await?
                else {
                    return Ok(ChangeStreamRead::Stop);
                };
                if !send_event_or_shutdown(tx, change_event, shutdown_rx).await {
                    return Ok(ChangeStreamRead::Stop);
                }
                *resume_position = Some(if invalidated {
                    MongoResumePosition::StartAfter(event_token)
                } else {
                    MongoResumePosition::ResumeAfter(cursor.resume_token().unwrap_or(event_token))
                });
                data_ready.notify_one();
                if invalidated {
                    return Ok(ChangeStreamRead::Reconnect);
                }
            }
            Ok(None) => {
                let cursor_alive = cursor.is_alive();
                if !matches!(
                    resume_position.as_ref(),
                    Some(MongoResumePosition::StartAfter(_))
                ) {
                    if let Some(token) = cursor.resume_token() {
                        let requires_start_after = !cursor_alive;
                        let changed = match resume_position.as_ref() {
                            Some(MongoResumePosition::ResumeAfter(current)) => {
                                requires_start_after || current != &token
                            }
                            Some(MongoResumePosition::StartAfter(_)) | None => true,
                        };
                        if changed {
                            let encoded = serde_json::to_string(&token).map_err(|error| {
                                ConnectorError::ReadError(format!(
                                    "serialize MongoDB post-batch resume token: {error}"
                                ))
                            })?;
                            let encoded = canonical_resume_token(&encoded).map_err(|error| {
                                ConnectorError::ReadError(format!(
                                    "invalid MongoDB post-batch resume token: {error}"
                                ))
                            })?;
                            let Some(marker) = acquire_mongo_high_watermark_ownership(
                                encoded,
                                requires_start_after,
                                byte_budget,
                                max_buffered_bytes,
                                shutdown_rx,
                            )
                            .await?
                            else {
                                return Ok(ChangeStreamRead::Stop);
                            };
                            if !send_event_or_shutdown(tx, marker, shutdown_rx).await {
                                return Ok(ChangeStreamRead::Stop);
                            }
                            data_ready.notify_one();
                        }
                        *resume_position = Some(if requires_start_after {
                            MongoResumePosition::StartAfter(token)
                        } else {
                            MongoResumePosition::ResumeAfter(token)
                        });
                    }
                }
                if !cursor_alive {
                    tracing::info!("change stream cursor exhausted");
                    return Ok(ChangeStreamRead::Reconnect);
                }
                *consecutive_failures = 0;
            }
            Err(error) => {
                tracing::error!(%error, "change stream error");
                return Ok(ChangeStreamRead::Reconnect);
            }
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
async fn send_event_or_shutdown(
    tx: &ChangeStreamTx,
    event: BufferedMongoEvent,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    if reader_stopping(shutdown_rx) {
        return false;
    }

    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => false,
        result = tx.send(event) => {
            if result.is_err() {
                tracing::warn!("source channel closed, stopping reader");
            }
            result.is_ok()
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
async fn acquire_mongo_event_ownership(
    event: MongoDbChangeEvent,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<BufferedMongoEvent>, ConnectorError> {
    let retained_bytes = mongo_event_retained_bytes(&event)?;
    let Some(byte_permit) =
        acquire_mongo_byte_permit(retained_bytes, byte_budget, max_buffered_bytes, shutdown_rx)
            .await?
    else {
        return Ok(None);
    };

    Ok(Some(BufferedMongoEvent::new(event, byte_permit)))
}

#[cfg(feature = "mongodb-cdc")]
async fn acquire_mongo_high_watermark_ownership(
    token: String,
    requires_start_after: bool,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<BufferedMongoEvent>, ConnectorError> {
    let retained_bytes = mongo_high_watermark_retained_bytes(token.capacity())?;
    let Some(byte_permit) =
        acquire_mongo_byte_permit(retained_bytes, byte_budget, max_buffered_bytes, shutdown_rx)
            .await?
    else {
        return Ok(None);
    };
    Ok(Some(BufferedMongoEvent::high_watermark(
        token,
        requires_start_after,
        byte_permit,
    )))
}

#[cfg(feature = "mongodb-cdc")]
async fn acquire_mongo_byte_permit(
    retained_bytes: usize,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<OwnedSemaphorePermit>, ConnectorError> {
    if reader_stopping(shutdown_rx) {
        return Ok(None);
    }
    if retained_bytes > max_buffered_bytes {
        return Err(ConnectorError::ReadError(format!(
            "MongoDB CDC decoded item exceeds the hard byte bound: item={retained_bytes}, \
             limit={max_buffered_bytes}"
        )));
    }
    let permits = u32::try_from(retained_bytes).map_err(|_| {
        ConnectorError::ReadError(format!(
            "MongoDB CDC decoded item exceeds the hard byte bound: item={retained_bytes}, \
             limit={max_buffered_bytes}"
        ))
    })?;
    let byte_permit = tokio::select! {
        biased;
        _ = shutdown_rx.changed() => return Ok(None),
        permit = Arc::clone(byte_budget).acquire_many_owned(permits) => permit.map_err(|_| {
            ConnectorError::ReadError("MongoDB CDC byte budget closed".into())
        })?,
    };
    Ok(Some(byte_permit))
}

#[cfg(feature = "mongodb-cdc")]
async fn retry_interrupted(
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    delay: std::time::Duration,
) -> bool {
    tokio::select! {
        changed = shutdown_rx.changed() => changed.is_err() || *shutdown_rx.borrow(),
        () = tokio::time::sleep(delay) => false,
    }
}

#[cfg(feature = "mongodb-cdc")]
fn parse_change_stream_pipeline(
    pipeline: &[serde_json::Value],
) -> Result<Vec<mongodb::bson::Document>, ConnectorError> {
    pipeline
        .iter()
        .enumerate()
        .map(|(index, value)| {
            mongodb::bson::to_document(value).map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "pipeline stage {index} cannot be represented as BSON: {error}"
                ))
            })
        })
        .collect()
}

#[cfg(feature = "mongodb-cdc")]
fn verify_mongodb_collection_uuid(
    expected: Uuid,
    observed: Uuid,
    database: &str,
    collection: &str,
) -> Result<(), ConnectorError> {
    if expected == observed {
        return Ok(());
    }
    Err(ConnectorError::ConfigurationError(format!(
        "MongoDB CDC collection identity changed for {database}.{collection}: \
         checkpoint/bound UUID={expected}, observed UUID={observed}"
    )))
}

#[cfg(feature = "mongodb-cdc")]
fn verify_mongodb_collection(
    config: &MongoDbSourceConfig,
    expected_uuid: Uuid,
    observation: &MongoCollectionObservation,
) -> Result<(), ConnectorError> {
    verify_mongodb_collection_uuid(
        expected_uuid,
        observation.collection_uuid,
        &config.database,
        &config.collection,
    )?;
    if config.full_document_mode == super::config::FullDocumentMode::RequirePostImage
        && !observation.post_images_enabled
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC full.document.mode=required needs changeStreamPreAndPostImages enabled \
             on {}.{} before the source starts",
            config.database, config.collection
        )));
    }
    Ok(())
}

#[cfg(feature = "mongodb-cdc")]
fn verify_mongodb_deployment_identity(
    expected: &MongoDeploymentIdentity,
    observed: &MongoDeploymentIdentity,
) -> Result<(), ConnectorError> {
    if expected == observed {
        return Ok(());
    }
    Err(ConnectorError::ConfigurationError(format!(
        "MongoDB CDC deployment identity changed: checkpoint/bound identity={}, observed \
         identity={}",
        expected.encode(),
        observed.encode()
    )))
}

#[cfg(feature = "mongodb-cdc")]
fn verify_mongodb_admission(
    config: &MongoDbSourceConfig,
    expected_deployment: &MongoDeploymentIdentity,
    expected_uuid: Uuid,
    observation: &MongoAdmissionObservation,
) -> Result<(), ConnectorError> {
    verify_mongodb_deployment_identity(expected_deployment, &observation.deployment_identity)?;
    verify_mongodb_collection(config, expected_uuid, &observation.collection)
}

#[cfg(feature = "mongodb-cdc")]
fn fresh_stream_anchor(
    cursor: &mongodb::change_stream::ChangeStream<
        mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
    >,
) -> Result<(mongodb::change_stream::event::ResumeToken, String), ConnectorError> {
    // The bootstrap aggregate uses batchSize=0, so MongoDB returns an empty firstBatch and its
    // exact postBatchResumeToken. Refuse an inclusive timestamp fallback: it can replay the final
    // write that preceded admission.
    let token = cursor.resume_token().ok_or_else(|| {
        ConnectorError::ReadError(
            "fresh MongoDB change stream omitted its initial postBatchResumeToken".into(),
        )
    })?;
    let encoded = serde_json::to_string(&token).map_err(|error| {
        ConnectorError::ReadError(format!(
            "serialize initial MongoDB post-batch resume token: {error}"
        ))
    })?;
    let encoded = canonical_resume_token(&encoded).map_err(|error| {
        ConnectorError::ReadError(format!(
            "invalid initial MongoDB post-batch resume token: {error}"
        ))
    })?;
    Ok((token, encoded))
}

#[cfg(feature = "mongodb-cdc")]
fn report_mongo_reader_admission_error(
    ready_tx: &mut Option<
        tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    >,
    error: &ConnectorError,
) {
    if let Some(ready_tx) = ready_tx.take() {
        let _ = ready_tx.send(Err(MongoReaderFailure::from_connector(error)));
    }
}

/// Background task that reads from the `MongoDB` change stream and sends
/// events to the source via a channel.
///
/// Uses a `'reconnect` / `'recv` double-loop pattern (mirroring the
/// Postgres CDC source) with exponential backoff capped at 30 seconds.
#[cfg(feature = "mongodb-cdc")]
async fn run_change_stream_reader(
    db: mongodb::Database,
    config: MongoDbSourceConfig,
    tx: ChangeStreamTx,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    data_ready: Arc<Notify>,
    metrics: Arc<MongoDbCdcMetrics>,
    byte_budget: Arc<Semaphore>,
    max_buffered_bytes: usize,
    initial_resume_position: Option<MongoResumePosition>,
    expected_collection_uuid: Option<Uuid>,
    expected_deployment_identity: Option<MongoDeploymentIdentity>,
    ready_tx: tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
) -> Result<(), ConnectorError> {
    let client = db.client().clone();
    let result = run_change_stream_reader_loop(
        db,
        config,
        tx,
        shutdown_rx,
        data_ready,
        metrics,
        byte_budget,
        max_buffered_bytes,
        initial_resume_position,
        expected_collection_uuid,
        expected_deployment_identity,
        ready_tx,
    )
    .await;

    // The loop owns every database, collection, and cursor handle. Once it
    // returns, shutdown can drain the driver's own async cleanup tasks.
    client.shutdown().await;
    result
}

#[cfg(feature = "mongodb-cdc")]
async fn run_change_stream_reader_loop(
    db: mongodb::Database,
    config: MongoDbSourceConfig,
    tx: ChangeStreamTx,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    data_ready: Arc<Notify>,
    metrics: Arc<MongoDbCdcMetrics>,
    byte_budget: Arc<Semaphore>,
    max_buffered_bytes: usize,
    initial_resume_position: Option<MongoResumePosition>,
    expected_collection_uuid: Option<Uuid>,
    expected_deployment_identity: Option<MongoDeploymentIdentity>,
    ready_tx: tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
) -> Result<(), ConnectorError> {
    let mut resume_position = initial_resume_position;
    let fresh_start = resume_position.is_none();
    let mut initial_resume_token = None;
    let mut ready_tx = Some(ready_tx);
    let pipeline = match parse_change_stream_pipeline(&config.pipeline) {
        Ok(pipeline) => pipeline,
        Err(error) => {
            report_mongo_reader_admission_error(&mut ready_tx, &error);
            return Err(error);
        }
    };

    let current_db = db;
    let mut consecutive_failures: u32 = 0;
    let initial_observation = loop {
        match observe_mongodb_admission(&current_db, &config.database, &config.collection).await {
            Ok(observation) => break observation,
            Err(error) if !error.is_transient() => {
                report_mongo_reader_admission_error(&mut ready_tx, &error);
                return Err(error);
            }
            Err(error) => {
                consecutive_failures += 1;
                if consecutive_failures >= MAX_FAILURES {
                    report_mongo_reader_admission_error(&mut ready_tx, &error);
                    return Err(error);
                }
                let backoff = crate::retry::Backoff::broker_reconnect().delay(consecutive_failures);
                tracing::warn!(
                    attempt = consecutive_failures,
                    ?backoff,
                    error = %error,
                    "failed to inspect MongoDB deployment or collection identity, retrying"
                );
                metrics.record_reconnect();
                if retry_interrupted(&mut shutdown_rx, backoff).await {
                    return Ok(());
                }
            }
        }
    };
    consecutive_failures = 0;

    let collection_uuid =
        expected_collection_uuid.unwrap_or(initial_observation.collection.collection_uuid);
    let deployment_identity = expected_deployment_identity
        .unwrap_or_else(|| initial_observation.deployment_identity.clone());
    if let Err(error) = verify_mongodb_admission(
        &config,
        &deployment_identity,
        collection_uuid,
        &initial_observation,
    ) {
        report_mongo_reader_admission_error(&mut ready_tx, &error);
        return Err(error);
    }

    let mut verify_before_open = false;

    'reconnect: loop {
        if verify_before_open {
            match observe_mongodb_admission(&current_db, &config.database, &config.collection).await
            {
                Ok(observation) => {
                    if let Err(error) = verify_mongodb_admission(
                        &config,
                        &deployment_identity,
                        collection_uuid,
                        &observation,
                    ) {
                        report_mongo_reader_admission_error(&mut ready_tx, &error);
                        return Err(error);
                    }
                    verify_before_open = false;
                }
                Err(error) if !error.is_transient() => {
                    report_mongo_reader_admission_error(&mut ready_tx, &error);
                    return Err(error);
                }
                Err(error) => {
                    consecutive_failures += 1;
                    if consecutive_failures >= MAX_FAILURES {
                        report_mongo_reader_admission_error(&mut ready_tx, &error);
                        return Err(error);
                    }
                    let backoff =
                        crate::retry::Backoff::broker_reconnect().delay(consecutive_failures);
                    tracing::warn!(
                        attempt = consecutive_failures,
                        ?backoff,
                        error = %error,
                        "failed to verify MongoDB deployment or collection identity before reconnect"
                    );
                    metrics.record_reconnect();
                    if retry_interrupted(&mut shutdown_rx, backoff).await {
                        break 'reconnect;
                    }
                    continue 'reconnect;
                }
            }
        }

        let bootstrap = fresh_start && ready_tx.is_some() && resume_position.is_none();
        let options = if bootstrap {
            bootstrap_change_stream_options(&config)
        } else {
            change_stream_options(&config, resume_position.as_ref())
        };

        // Open the change stream cursor.
        let cursor_result = current_db
            .collection::<mongodb::bson::Document>(&config.collection)
            .watch()
            .pipeline(pipeline.clone())
            .with_options(options)
            .await;

        let mut cursor = match cursor_result {
            Ok(c) => c,
            Err(e) => {
                consecutive_failures += 1;
                if consecutive_failures >= MAX_FAILURES {
                    let msg =
                        format!("change stream open failed after {MAX_FAILURES} attempts: {e}");
                    tracing::error!(%msg);
                    let error = ConnectorError::ReadError(msg);
                    report_mongo_reader_admission_error(&mut ready_tx, &error);
                    return Err(error);
                }
                let backoff = crate::retry::Backoff::broker_reconnect().delay(consecutive_failures);
                tracing::warn!(
                    attempt = consecutive_failures,
                    ?backoff,
                    error = %e,
                    "failed to open change stream, retrying"
                );
                metrics.record_reconnect();
                if retry_interrupted(&mut shutdown_rx, backoff).await {
                    break 'reconnect;
                }
                verify_before_open = true;
                continue 'reconnect;
            }
        };

        match observe_mongodb_admission(&current_db, &config.database, &config.collection).await {
            Ok(observation) => {
                if let Err(error) = verify_mongodb_admission(
                    &config,
                    &deployment_identity,
                    collection_uuid,
                    &observation,
                ) {
                    report_mongo_reader_admission_error(&mut ready_tx, &error);
                    return Err(error);
                }
            }
            Err(error) if !error.is_transient() => {
                report_mongo_reader_admission_error(&mut ready_tx, &error);
                return Err(error);
            }
            Err(error) => {
                consecutive_failures += 1;
                if consecutive_failures >= MAX_FAILURES {
                    report_mongo_reader_admission_error(&mut ready_tx, &error);
                    return Err(error);
                }
                let backoff = crate::retry::Backoff::broker_reconnect().delay(consecutive_failures);
                tracing::warn!(
                    attempt = consecutive_failures,
                    ?backoff,
                    error = %error,
                    "failed to verify MongoDB deployment or collection identity after opening change stream"
                );
                metrics.record_reconnect();
                if retry_interrupted(&mut shutdown_rx, backoff).await {
                    break 'reconnect;
                }
                verify_before_open = true;
                continue 'reconnect;
            }
        }
        consecutive_failures = 0;

        if bootstrap {
            match fresh_stream_anchor(&cursor) {
                Ok((token, encoded)) => {
                    resume_position = Some(MongoResumePosition::ResumeAfter(token));
                    initial_resume_token = Some(encoded);
                }
                Err(error) => {
                    report_mongo_reader_admission_error(&mut ready_tx, &error);
                    return Err(error);
                }
            }
            drop(cursor);
            continue 'reconnect;
        }

        if let Some(ready_tx) = ready_tx.take() {
            let _ = ready_tx.send(Ok(MongoReaderReady {
                initial_resume_token: initial_resume_token.take(),
                collection_uuid,
                deployment_identity: deployment_identity.clone(),
            }));
        }

        tracing::info!(
            database = %config.database,
            collection = %config.collection,
            resumed = resume_position.is_some(),
            "change stream reader started"
        );

        if matches!(
            forward_change_stream(
                &mut cursor,
                &mut shutdown_rx,
                &mut resume_position,
                &tx,
                &data_ready,
                &mut consecutive_failures,
                &byte_budget,
                max_buffered_bytes,
                &metrics,
            )
            .await?,
            ChangeStreamRead::Stop
        ) {
            break 'reconnect;
        }

        // Exited recv loop due to error or cursor exhaustion — attempt reconnect.
        consecutive_failures += 1;
        if consecutive_failures >= MAX_FAILURES {
            let msg = format!("change stream failed after {MAX_FAILURES} consecutive failures");
            tracing::error!(%msg);
            return Err(ConnectorError::ReadError(msg));
        }

        let backoff = crate::retry::Backoff::broker_reconnect().delay(consecutive_failures);
        tracing::warn!(
            resume_position = ?resume_position,
            attempt = consecutive_failures,
            ?backoff,
            "reconnecting change stream"
        );
        metrics.record_reconnect();

        if retry_interrupted(&mut shutdown_rx, backoff).await {
            break 'reconnect;
        }

        // The MongoDB client owns topology monitoring and reconnects its pool.
        // Reusing it avoids spawning untracked driver generations on each retry.
        verify_before_open = true;
    }

    if let Some(ready_tx) = ready_tx.take() {
        let _ = ready_tx.send(Err(MongoReaderFailure::Read(
            "change stream reader was shut down before the cursor opened".into(),
        )));
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

    let operation_type = match &event.operation_type {
        MongoOpType::Insert => OperationType::Insert,
        MongoOpType::Update => OperationType::Update,
        MongoOpType::Replace => OperationType::Replace,
        MongoOpType::Delete => OperationType::Delete,
        MongoOpType::Drop => OperationType::Drop,
        MongoOpType::Rename => OperationType::Rename,
        MongoOpType::Invalidate => OperationType::Invalidate,
        MongoOpType::DropDatabase => OperationType::DropDatabase,
        MongoOpType::Other(value) => OperationType::Other(value.clone()),
        other => {
            return Err(ConnectorError::ReadError(format!(
                "unsupported MongoDB operation type: {other:?}"
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

    let document_key = event.document_key.as_ref().map_or_else(
        || Ok(String::new()),
        |document| {
            serde_json::to_string(document)
                .map_err(|error| ConnectorError::ReadError(format!("document key: {error}")))
        },
    )?;

    let full_document = event
        .full_document
        .as_ref()
        .map(|document| {
            serde_json::to_string(document)
                .map_err(|error| ConnectorError::ReadError(format!("full document: {error}")))
        })
        .transpose()?;

    let update_description = event
        .update_description
        .as_ref()
        .map(|ud| -> Result<UpdateDescription, ConnectorError> {
            let updated_fields = ud
                .updated_fields
                .iter()
                .map(|(key, value)| {
                    serde_json::to_value(value)
                        .map(|value| (key.clone(), value))
                        .map_err(|error| {
                            ConnectorError::ReadError(format!("updated field '{key}': {error}"))
                        })
                })
                .collect::<Result<_, _>>()?;

            let removed_fields = ud.removed_fields.clone();

            let truncated_arrays = ud
                .truncated_arrays
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|t| {
                    let new_size = u32::try_from(t.new_size).map_err(|_| {
                        ConnectorError::ReadError(format!(
                            "truncated array '{}' has negative newSize {}",
                            t.field, t.new_size
                        ))
                    })?;
                    Ok(super::change_event::TruncatedArray {
                        field: t.field.clone(),
                        new_size,
                    })
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?;

            let disambiguated_paths = ud
                .disambiguated_paths
                .as_ref()
                .map(|paths| {
                    paths
                        .iter()
                        .map(|(key, value)| {
                            serde_json::to_value(value)
                                .map(|value| (key.clone(), value))
                                .map_err(|error| {
                                    ConnectorError::ReadError(format!(
                                        "disambiguated path '{key}': {error}"
                                    ))
                                })
                        })
                        .collect::<Result<_, _>>()
                })
                .transpose()?
                .unwrap_or_default();

            Ok(UpdateDescription {
                updated_fields,
                removed_fields,
                truncated_arrays,
                disambiguated_paths,
            })
        })
        .transpose()?;

    let (cluster_time_secs, cluster_time_inc) = event
        .cluster_time
        .map_or((0, 0), |ts| (ts.time, ts.increment));

    let wall_time_ms = event
        .wall_time
        .map_or(0, mongodb::bson::DateTime::timestamp_millis);

    // Serialize the ResumeToken via serde (it implements Serialize).
    let resume_token = serde_json::to_string(&event.id)
        .map_err(|error| ConnectorError::ReadError(format!("resume token: {error}")))?;

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
mod tests;
