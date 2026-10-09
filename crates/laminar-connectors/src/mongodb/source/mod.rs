//! `MongoDB` CDC source connector implementation.
//!
//! Implements [`crate::connector::SourceConnector`] for streaming change events from `MongoDB`
//! change streams into `LaminarDB` as Arrow `RecordBatch`es, in one of two shapes:
//!
//! - history: every event (and every snapshot copy) becomes an immutable, versioned record;
//! - document: full post-images become keyed puts and deletes become key-only tombstones.
//!
//! # Cancellation Safety
//!
//! Connector lifecycle futures never directly poll the `MongoDB` driver. Driver
//! I/O lives in an owned reader task; cancellation aborts that task so no
//! connection or cursor outlives its connector.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{BinaryArray, UInt32Array};
use arrow_schema::SchemaRef;
use mongodb::bson::{RawDocumentBuf, Timestamp};
use tokio::sync::{Notify, Semaphore};
use uuid::Uuid;

use crate::config::ConnectorState;
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceMutation, SourceRowPositions,
};
use crate::error::ConnectorError;

use super::change_event::ChangeOperation;
use super::config::{MongoDbSourceConfig, SourceOutputMode};
use super::metrics::MongoDbCdcMetrics;

const MAX_RESUME_TOKEN_BYTES: usize = 64 * 1024;
const MONGODB_CHECKPOINT_CONNECTOR: &str = "mongodb-cdc";
const MONGODB_CHECKPOINT_VERSION: &str = "5";
const STREAM_IDENTITY_METADATA: &str = "stream_identity_sha256";
const COLLECTION_UUID_METADATA: &str = "collection_uuid";
const DEPLOYMENT_IDENTITY_METADATA: &str = "deployment_identity";
const MAX_MONGODB_WIRE_EVENT_BYTES: usize = 16 * 1024 * 1024;
const CURSOR_MAX_AWAIT_TIME: std::time::Duration = std::time::Duration::from_secs(1);
const READER_STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

mod admission;
mod buffering;
mod checkpoint;
mod decoding;
mod document;
mod history;
mod lifecycle;
mod reader;
mod schema_resolution;

pub use history::{mongodb_history_schema, MONGODB_HISTORY_VERSION, SNAPSHOT_OPERATION};

use admission::observe_mongodb_admission;
use buffering::{BufferedMongoEvent, BufferedMongoPayload};
use checkpoint::{
    mongodb_stream_identity, parse_mongodb_checkpoint, EmittedPosition, MongoCheckpointPosition,
    MongoDeploymentIdentity, ParsedMongoCheckpoint, SnapshotCut, StreamPosition,
};
use decoding::decode_change;
use document::{DocumentProjection, DocumentRow};
use history::{history_batch, id_key, HistoryIdentity};
use reader::{run_change_stream_reader, MongoResumePosition, ReaderStart, READER_SHUTDOWN_TIMEOUT};

/// `MongoDB` CDC source connector.
///
/// Events are read by an owned background task into a byte-bounded queue and converted to
/// Arrow on `poll_batch`, outside the engine's compute thread.
pub struct MongoDbCdcSource {
    config: MongoDbSourceConfig,
    state: ConnectorState,
    schema: SchemaRef,
    metrics: Arc<MongoDbCdcMetrics>,
    event_buffer: VecDeque<BufferedMongoEvent>,

    /// Position and row count of everything returned by `poll_batch`. The reader's newer
    /// cursor position is deliberately not shared with this field.
    emitted: Option<EmittedPosition>,

    /// Physical identity of the fixed collection admitted by `listCollections`.
    collection_uuid: Option<Uuid>,

    /// Immutable server-issued identity of the replica set or sharded cluster.
    deployment_identity: Option<MongoDeploymentIdentity>,

    /// Document-mode projection, validated before the reader starts.
    projection: Option<DocumentProjection>,

    /// Opens the snapshot copy once a checkpoint carrying the fresh snapshot cut is durable.
    snapshot_committed: Option<(Timestamp, tokio::sync::watch::Sender<bool>)>,

    /// Shared ownership limits span the reader channel and poll buffer.
    byte_budget: Arc<Semaphore>,

    /// Notification handle signalled when data arrives from the stream.
    data_ready: Arc<Notify>,

    reader_handle: Option<tokio::task::JoinHandle<()>>,
    event_rx: Option<ChangeStreamRx>,
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,

    /// Terminal reader failure, independent of the bounded event queue.
    reader_error: Option<tokio::sync::watch::Receiver<Option<MongoReaderFailure>>>,

    /// Admission authority and terminal observer for this connector generation.
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

impl Drop for MongoDbCdcSource {
    fn drop(&mut self) {
        if let Some(shutdown) = self.reader_shutdown.take() {
            shutdown.send_replace(true);
        }
        if let Some(handle) = self.reader_handle.take() {
            reap_mongo_reader(handle, &self.task_owner);
        }
    }
}

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
type ChangeStreamTx = crossfire::MAsyncTx<crossfire::mpsc::Array<BufferedMongoEvent>>;
/// Single-consumer async receiver for the change stream reader → `poll_batch` queue.
type ChangeStreamRx = crossfire::AsyncRx<crossfire::mpsc::Array<BufferedMongoEvent>>;

#[derive(Debug)]
struct MongoReaderReady {
    /// Fresh starts only: the boundary established before the first record.
    initial_position: Option<MongoCheckpointPosition>,
    collection_uuid: Uuid,
    deployment_identity: MongoDeploymentIdentity,
}

#[derive(Clone, Debug)]
enum MongoReaderFailure {
    Configuration(String),
    Connection(String),
    Read(String),
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MongoCollectionObservation {
    collection_uuid: Uuid,
    post_images_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MongoAdmissionObservation {
    deployment_identity: MongoDeploymentIdentity,
    collection: MongoCollectionObservation,
}

struct MongoReaderAdmissionGuard {
    shutdown: Option<tokio::sync::watch::Sender<bool>>,
}

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

impl Drop for MongoReaderAdmissionGuard {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.as_ref() {
            shutdown.send_replace(true);
        }
    }
}

/// Records of one batch, resolved against the emitted position they advance.
struct PendingBatch {
    items: Vec<BufferedMongoEvent>,
    next: EmittedPosition,
    first_sequence: u64,
    /// Snapshot time for snapshot rows in this batch.
    snapshot_at: Option<Timestamp>,
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
            schema: mongodb_history_schema(),
            metrics: Arc::new(MongoDbCdcMetrics::new(registry)),
            event_buffer: VecDeque::new(),
            emitted: None,
            collection_uuid: None,
            deployment_identity: None,
            projection: None,
            snapshot_committed: None,
            data_ready: Arc::new(Notify::new()),
            reader_handle: None,
            event_rx: None,
            reader_shutdown: None,
            reader_error: None,
            task_owner,
            task_tracker,
        }
    }

    /// Advance `position` past one buffered item without mutating the source.
    fn advance(
        &self,
        position: &mut EmittedPosition,
        item: &BufferedMongoEvent,
    ) -> Result<Option<Timestamp>, ConnectorError> {
        let snapshot_at = match &position.position {
            MongoCheckpointPosition::Snapshot(cut) => Some(cut.at),
            MongoCheckpointPosition::Stream(_) => None,
        };
        match &item.payload {
            BufferedMongoPayload::Change(record) => {
                if self.produces_row(record.operation)? {
                    position.next_sequence = next_sequence(position.next_sequence)?;
                }
                position.position = MongoCheckpointPosition::Stream(
                    if record.operation == ChangeOperation::Invalidate {
                        StreamPosition::StartAfter(record.token.clone())
                    } else {
                        StreamPosition::ResumeAfter(record.token.clone())
                    },
                );
            }
            BufferedMongoPayload::Snapshot(record) => {
                let at = snapshot_at.ok_or_else(|| {
                    ConnectorError::Internal("snapshot row outside a snapshot scan".into())
                })?;
                position.next_sequence = next_sequence(position.next_sequence)?;
                position.position = MongoCheckpointPosition::Snapshot(SnapshotCut {
                    at,
                    after_key: Some(record.key.clone()),
                });
            }
            BufferedMongoPayload::HighWatermark {
                token,
                requires_start_after,
            } => {
                position.position = MongoCheckpointPosition::Stream(if *requires_start_after {
                    StreamPosition::StartAfter(token.clone())
                } else {
                    StreamPosition::ResumeAfter(token.clone())
                });
            }
            BufferedMongoPayload::SnapshotComplete => {
                let at = snapshot_at.ok_or_else(|| {
                    ConnectorError::Internal("snapshot completion outside a snapshot scan".into())
                })?;
                position.position = MongoCheckpointPosition::Stream(StreamPosition::StartAt(at));
            }
        }
        Ok(snapshot_at)
    }

    /// Whether a change event becomes an output row in this output mode.
    fn produces_row(&self, operation: ChangeOperation) -> Result<bool, ConnectorError> {
        match (self.config.output_mode, operation) {
            (SourceOutputMode::History, _)
            | (
                SourceOutputMode::Document,
                ChangeOperation::Insert
                | ChangeOperation::Update
                | ChangeOperation::Replace
                | ChangeOperation::Delete,
            ) => Ok(true),
            (SourceOutputMode::Document, ChangeOperation::Metadata) => Ok(false),
            (
                SourceOutputMode::Document,
                ChangeOperation::Invalidate
                | ChangeOperation::Drop
                | ChangeOperation::Rename
                | ChangeOperation::DropDatabase
                | ChangeOperation::Unknown,
            ) => Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC document replication of {}.{} stopped at a {operation:?} event; \
                 the mirror is not changed. Recreating, renaming, or dropping the source \
                 collection requires rebuilding the targets from a new snapshot",
                self.config.database, self.config.collection
            ))),
        }
    }

    /// Select the next batch: at most `max_records` items, ending at an invalidation.
    fn plan_batch(&mut self, max_records: usize) -> Result<Option<PendingBatch>, ConnectorError> {
        let Some(start) = self.emitted.clone() else {
            return Err(ConnectorError::InvalidState {
                expected: "admitted position".into(),
                actual: "no emitted position".into(),
            });
        };
        let mut next = start.clone();
        let mut snapshot_at = None;
        let mut count = 0;
        for item in self.event_buffer.iter().take(max_records) {
            let at = self.advance(&mut next, item)?;
            snapshot_at = snapshot_at.or(at);
            count += 1;
            // An invalidate token changes the legal resume option. End the batch exactly there
            // even when the reader has already reopened with startAfter and queued later data.
            if item.is_invalidate() {
                break;
            }
        }
        if count == 0 {
            return Ok(None);
        }
        Ok(Some(PendingBatch {
            items: self.event_buffer.drain(..count).collect(),
            next,
            first_sequence: start.next_sequence,
            snapshot_at,
        }))
    }

    /// Drains up to `max_records` items and converts them to one source batch.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if a record cannot be represented; the queue is left intact.
    fn drain_to_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if max_records == 0 || self.event_buffer.is_empty() {
            return Ok(None);
        }
        let Some(pending) = self.plan_batch(max_records)? else {
            return Ok(None);
        };
        let result = if pending.items.iter().any(BufferedMongoEvent::is_record) {
            self.build_batch(&pending).map(Some)
        } else {
            Ok(None)
        };
        match result {
            Ok(batch) => {
                for item in &pending.items {
                    if let BufferedMongoPayload::Change(record) = &item.payload {
                        self.metrics.record_event(record.operation);
                        if self.config.output_mode == SourceOutputMode::Document
                            && record.operation == ChangeOperation::Metadata
                        {
                            self.metrics.metadata_events_skipped.inc();
                        }
                    }
                }
                if batch.is_some() {
                    self.metrics.record_batch();
                }
                self.metrics
                    .snapshot_in_progress
                    .set(i64::from(pending.next.in_snapshot()));
                self.emitted = Some(pending.next);
                Ok(batch)
            }
            Err(error) => {
                for item in pending.items.into_iter().rev() {
                    self.event_buffer.push_front(item);
                }
                Err(error)
            }
        }
    }

    fn build_batch(&self, pending: &PendingBatch) -> Result<SourceBatch, ConnectorError> {
        let (Some(collection_uuid), Some(deployment)) =
            (self.collection_uuid, self.deployment_identity.as_ref())
        else {
            return Err(ConnectorError::InvalidState {
                expected: "admitted collection identity".into(),
                actual: "unbound source".into(),
            });
        };
        match &self.projection {
            None => {
                let deployment = deployment.encode();
                let identity = HistoryIdentity {
                    deployment: &deployment,
                    collection_uuid,
                    database: &self.config.database,
                    collection: &self.config.collection,
                };
                let records = pending
                    .items
                    .iter()
                    .filter(|item| item.is_record())
                    .map(|item| &item.payload)
                    .collect::<Vec<_>>();
                let batch = history_batch(
                    records.into_iter(),
                    &self.schema,
                    &identity,
                    pending.snapshot_at,
                )?;
                Ok(SourceBatch::new(batch))
            }
            Some(projection) => {
                self.document_batch(projection, pending, collection_uuid.as_bytes())
            }
        }
    }

    fn document_batch(
        &self,
        projection: &DocumentProjection,
        pending: &PendingBatch,
        partition: &[u8],
    ) -> Result<SourceBatch, ConnectorError> {
        let snapshot_keys = pending
            .items
            .iter()
            .filter_map(|item| match &item.payload {
                BufferedMongoPayload::Snapshot(record) => Some(record),
                _ => None,
            })
            .map(|record| {
                record
                    .raw
                    .get("_id")
                    .ok()
                    .flatten()
                    .map(id_key)
                    .ok_or_else(|| {
                        ConnectorError::SchemaMismatch("snapshot document has no _id".into())
                    })
            })
            .collect::<Result<Vec<RawDocumentBuf>, _>>()?;
        let mut snapshot_keys = snapshot_keys.iter();
        let mut rows = Vec::with_capacity(pending.items.len());
        for item in &pending.items {
            match &item.payload {
                BufferedMongoPayload::Change(record) => {
                    if let Some(row) = self.change_row(record)? {
                        rows.push(row);
                    }
                }
                BufferedMongoPayload::Snapshot(record) => rows.push(DocumentRow::Put {
                    key: snapshot_keys.next().ok_or_else(|| {
                        ConnectorError::Internal("snapshot key bookkeeping".into())
                    })?,
                    document: &record.raw,
                }),
                BufferedMongoPayload::HighWatermark { .. }
                | BufferedMongoPayload::SnapshotComplete => {}
            }
        }
        let (records, mutations) = projection.build(&rows)?;
        let positions = row_positions(partition, pending.first_sequence, rows.len())?;
        let batch = SourceBatch::positioned(records, positions)?;
        if mutations.contains(&SourceMutation::Tombstone) {
            batch.with_mutations(mutations)
        } else {
            Ok(batch)
        }
    }

    fn change_row<'a>(
        &self,
        record: &'a buffering::ChangeRecord,
    ) -> Result<Option<DocumentRow<'a>>, ConnectorError> {
        if !self.produces_row(record.operation)? {
            return Ok(None);
        }
        let change = decode_change(&record.raw)?;
        let key = change.document_key.ok_or_else(|| {
            ConnectorError::SchemaMismatch(format!(
                "MongoDB {} event omitted its document key",
                change.operation
            ))
        })?;
        if record.operation == ChangeOperation::Delete {
            return Ok(Some(DocumentRow::Tombstone { key }));
        }
        let document = change.full_document.ok_or_else(|| {
            ConnectorError::SchemaMismatch(format!(
                "MongoDB {} event for {}.{} has no full document; document replication never \
                 treats a missing image as a delete",
                change.operation, self.config.database, self.config.collection
            ))
        })?;
        Ok(Some(DocumentRow::Put { key, document }))
    }
}

fn next_sequence(sequence: u64) -> Result<u64, ConnectorError> {
    sequence
        .checked_add(1)
        .ok_or_else(|| ConnectorError::Internal("MongoDB CDC row sequence exhausted u64".into()))
}

/// Ordered positions: one partition per collection incarnation, the emitted-row sequence as the
/// order key. The sequence is restored from the checkpoint, so replay reproduces it exactly.
fn row_positions(
    partition: &[u8],
    first_sequence: u64,
    rows: usize,
) -> Result<SourceRowPositions, ConnectorError> {
    let mut order_keys = Vec::with_capacity(rows);
    let mut sequence = first_sequence;
    for _ in 0..rows {
        order_keys.push(sequence.to_be_bytes());
        sequence = next_sequence(sequence)?;
    }
    SourceRowPositions::try_new(
        BinaryArray::from_iter_values(std::iter::repeat_n(partition, rows)),
        BinaryArray::from_iter_values(order_keys.iter()),
        UInt32Array::from(vec![0; rows]),
    )
}

#[cfg(test)]
mod tests;
