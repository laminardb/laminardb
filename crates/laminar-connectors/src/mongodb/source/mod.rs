//! `MongoDB` CDC source connector implementation.
//!
//! Implements [`crate::connector::SourceConnector`] for streaming change events from `MongoDB`
//! change streams into `LaminarDB` as Arrow `RecordBatch`es.
//!
//! # Cancellation Safety
//!
//! Connector lifecycle futures never directly poll the `MongoDB` driver. Driver
//! I/O lives in an owned reader task; cancellation aborts that task so no
//! connection or cursor outlives its connector.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

use crate::config::ConnectorState;
use crate::connector::{ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch};
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

mod admission;
mod buffering;
mod checkpoint;
mod decoding;
mod lifecycle;
mod reader;

use admission::observe_mongodb_admission;
#[cfg(test)]
use admission::{
    await_mongo_reader_ready, mongodb_identity_command_is_permanent, source_client_options,
};
#[cfg(test)]
use buffering::events_to_record_batch;
use buffering::{
    events_to_record_batch_refs, mongo_event_retained_bytes, mongo_high_watermark_retained_bytes,
    BufferedMongoEvent, BufferedMongoPayload,
};
#[cfg(test)]
use checkpoint::parse_deployment_identity;
use checkpoint::{
    canonical_resume_token, mongodb_stream_identity, parse_mongodb_checkpoint,
    MongoCheckpointPosition, MongoDeploymentIdentity, ParsedMongoCheckpoint,
};
use decoding::parse_change_stream_event;
#[cfg(test)]
use reader::{
    acquire_mongo_event_ownership, bootstrap_change_stream_options, change_stream_options,
    send_event_or_shutdown, verify_mongodb_collection, verify_mongodb_collection_uuid,
    verify_mongodb_deployment_identity,
};
use reader::{run_change_stream_reader, READER_SHUTDOWN_TIMEOUT};

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

#[cfg(test)]
mod tests;
