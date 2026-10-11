//! `PostgreSQL` CDC source connector implementation.
//!
//! Streams the changes of one primary-keyed table from `PostgreSQL` logical replication
//! (`pgoutput`) as typed Arrow rows of the declared source schema. A fresh source claims a new
//! slot name and creates the slot once a checkpoint commits the claim; it then either copies the
//! table in chunks while streaming from the slot's consistent point (`snapshot.mode=initial`) or
//! starts at that point (`snapshot.mode=never`). Later starts resume from the engine checkpoint.

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ConnectorState;
use crate::connector::{ConnectorTaskOwner, ConnectorTaskTracker, SourceMutation};
use crate::error::ConnectorError;

use super::config::PostgresCdcConfig;
use super::lsn::Lsn;
use super::metrics::PostgresCdcMetrics;
use super::postgres_io::PostgresCheckpointBinding;
use super::schema::RelationInfo;
use super::typed_rows::{RowBuilder, RowLayout};

mod checkpoint;
mod claim;
mod decoding;
mod drain;
mod lifecycle;
mod reader;
mod snapshot;
mod startup;

use claim::{ClaimTask, SlotClaim};
use reader::{OwnedWalPayload, WalPayload, WalPayloadRx, WalTerminalError};
use snapshot::{ChunkedSnapshot, CopyPlan};

const PGWIRE_IN_FLIGHT_EVENTS: usize = 1;
const RAW_WAL_QUEUE_CAPACITY: usize = 4_096;

/// Where the source currently reads from.
enum Phase {
    /// Not started, or closed.
    Idle,
    /// Intake held until a checkpoint commits the claim and its slot exists.
    Claiming(ClaimTask),
    /// `snapshot.mode=never`: the slot exists, and the reader starts at its consistent point once
    /// a committed `streaming` cursor names it (`released`). A restart from the claim may stream
    /// from a later point on another slot, so nothing may be emitted under it.
    AwaitingStream { released: bool },
    /// Copying the table in chunks while streaming, once a cursor naming the slot committed.
    Snapshot(Box<ChunkedSnapshot>),
    /// Streaming committed transactions from the slot.
    Streaming,
}

/// One emitted row: which image it reads and how it is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowImage {
    Put,
    Tombstone,
    Weighted(i64),
}

impl RowImage {
    /// Whether only key columns are emitted, the `__weight`, and the upsert mutation.
    fn parts(self) -> (bool, Option<i64>, Option<SourceMutation>) {
        match self {
            Self::Put => (false, None, Some(SourceMutation::Put)),
            Self::Tombstone => (true, None, Some(SourceMutation::Tombstone)),
            Self::Weighted(weight) => (false, Some(weight), None),
        }
    }
}

/// Declared-column texts of one row; `None` is SQL `NULL`.
type RowValues = Box<[Option<Box<[u8]>>]>;

/// A row decoded during the copy, kept as texts until the drain knows where its key lies.
struct StagedRow {
    image: RowImage,
    values: RowValues,
    /// Planned retained bytes, charged to the decoded stage.
    bytes: usize,
}

/// The staged rows of the open transaction.
#[derive(Default)]
struct StagedRows {
    rows: Vec<StagedRow>,
    bytes: usize,
}

/// The transaction being decoded; its rows become visible only at COMMIT.
struct OpenTransaction {
    final_lsn: Lsn,
    xid: u32,
    /// Rows of a transaction decoded during the copy.
    staged: Option<StagedRows>,
}

/// A decoded transaction, resumable only after every one of its rows has been emitted.
struct CommittedTransaction {
    end_lsn: Lsn,
    xid: u32,
    /// `None` when the transaction changed no captured rows, or was decoded during the copy.
    records: Option<RecordBatch>,
    /// Row mutations in upsert mode; empty in changelog mode.
    mutations: Box<[SourceMutation]>,
    /// Rows of a transaction decoded during the copy, not built until drained.
    staged: Option<Box<[StagedRow]>>,
    retained_bytes: usize,
}

impl CommittedTransaction {
    fn rows(&self) -> usize {
        self.records.as_ref().map_or(0, RecordBatch::num_rows)
            + self.staged.as_ref().map_or(0, |staged| staged.len())
    }
}

/// `PostgreSQL` CDC source connector.
///
/// In `output.mode=upsert` every row is a [`SourceMutation::Put`] of the complete current row or
/// a key-only [`SourceMutation::Tombstone`]; a primary-key change is a tombstone of the old key
/// followed by a put of the new row. In `output.mode=changelog` rows carry a trailing `__weight`:
/// `+1` for inserted and updated-to images, `-1` for deleted and updated-from images.
pub struct PostgresCdcSource {
    config: PostgresCdcConfig,
    state: ConnectorState,
    /// The error that failed the source, repeated by every later poll.
    failure: Option<String>,
    /// Declared output schema; empty until started.
    schema: SchemaRef,
    metrics: Arc<PostgresCdcMetrics>,
    phase: Phase,

    /// Declared-schema binding fixed at startup.
    layout: Option<RowLayout>,
    /// The bound relation every `pgoutput` Relation message must match exactly.
    relation: Option<RelationInfo>,
    relation_announced: bool,

    open_transaction: Option<OpenTransaction>,
    open_rows: Option<RowBuilder>,
    open_mutations: Vec<SourceMutation>,
    committed: VecDeque<CommittedTransaction>,
    /// Bytes retained by `committed` batches.
    committed_bytes: usize,

    /// Last position whose durable commit was handed to the replication worker.
    confirmed_flush_lsn: Lsn,
    /// Latest position received from the server.
    write_lsn: Lsn,
    /// The resumable cursor: the end LSN of the last transaction drained into a batch, or a later
    /// keepalive position received while no transaction was open or undrained.
    polled_lsn: Lsn,

    /// Exact database and publication identity bound to checkpoints.
    checkpoint_binding: Option<PostgresCheckpointBinding>,
    /// The claim whose slot this source creates or reads.
    claim: Option<SlotClaim>,
    /// Random per `start()`, in this incarnation's `application_name`.
    incarnation: String,
    /// The owned slot's consistent point, once the slot exists.
    consistent_point: Option<Lsn>,
    /// SQL of the initial copy, read at startup when the source copies.
    copy_plan: Option<Arc<CopyPlan>>,

    #[cfg(test)]
    pending_messages: VecDeque<Vec<u8>>,

    data_ready: Arc<Notify>,
    wal_rx: Option<WalPayloadRx>,
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,
    /// Durable progress reported by the replication worker on every status update,
    /// independently of the reader task and of a full event queue.
    applied_lsn: Option<pgwire_replication::AppliedLsnHandle>,
    /// Payloads taken from the reader queue but not yet decoded, in WAL order.
    pending_payloads: VecDeque<OwnedWalPayload>,
    wal_byte_budget: Option<Arc<Semaphore>>,
    /// Fatal reader error delivered out of band so a full WAL queue cannot hide it.
    wal_terminal_error: Option<WalTerminalError>,
    /// Earliest instant of the next live publication/table revalidation.
    next_contract_check: Option<tokio::time::Instant>,

    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

impl Drop for PostgresCdcSource {
    fn drop(&mut self) {
        if let Some(shutdown) = self.reader_shutdown.take() {
            shutdown.send_replace(true);
        }
        if let Some(handle) = self.reader_handle.take() {
            reap_postgres_reader(handle, &self.task_owner);
        }
    }
}

fn reap_postgres_reader(handle: tokio::task::JoinHandle<()>, task_owner: &ConnectorTaskOwner) {
    let Some(reaper_guard) = task_owner.track() else {
        tracing::warn!("PostgreSQL CDC task generation was sealed before reader reaping");
        return;
    };
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        // The reader's own guard remains authoritative. Dropping its runtime
        // destroys the future and therefore resolves the generation tracker.
        drop(reaper_guard);
        return;
    };
    drop(runtime.spawn(async move {
        let _reaper_guard = reaper_guard;
        if let Err(error) = handle.await {
            tracing::debug!(%error, "PostgreSQL CDC retired reader task reaped");
        }
    }));
}

impl PostgresCdcSource {
    /// Creates a new `PostgreSQL` CDC source with the given configuration.
    #[must_use]
    pub fn new(config: PostgresCdcConfig, registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            config,
            state: ConnectorState::Created,
            failure: None,
            schema: Arc::new(Schema::empty()),
            metrics: Arc::new(PostgresCdcMetrics::new(registry)),
            phase: Phase::Idle,
            layout: None,
            relation: None,
            relation_announced: false,
            open_transaction: None,
            open_rows: None,
            open_mutations: Vec::new(),
            committed: VecDeque::new(),
            committed_bytes: 0,
            confirmed_flush_lsn: Lsn::ZERO,
            write_lsn: Lsn::ZERO,
            polled_lsn: Lsn::ZERO,
            checkpoint_binding: None,
            claim: None,
            incarnation: String::new(),
            consistent_point: None,
            copy_plan: None,
            #[cfg(test)]
            pending_messages: VecDeque::new(),
            data_ready: Arc::new(Notify::new()),
            wal_rx: None,
            reader_handle: None,
            reader_shutdown: None,
            applied_lsn: None,
            pending_payloads: VecDeque::new(),
            wal_byte_budget: None,
            wal_terminal_error: None,
            next_contract_check: None,
            task_owner,
            task_tracker,
        }
    }

    /// Returns a reference to the CDC configuration.
    #[must_use]
    pub fn config(&self) -> &PostgresCdcConfig {
        &self.config
    }

    /// Returns the last durably committed position handed to the replication worker.
    #[must_use]
    pub fn confirmed_flush_lsn(&self) -> Lsn {
        self.confirmed_flush_lsn
    }

    /// Returns the latest position received from the server.
    #[must_use]
    pub fn write_lsn(&self) -> Lsn {
        self.write_lsn
    }

    /// Returns the current replication lag in bytes.
    #[must_use]
    pub fn replication_lag_bytes(&self) -> u64 {
        self.write_lsn.diff(self.confirmed_flush_lsn)
    }

    /// Rows decoded but not yet emitted, across the open and committed transactions.
    #[must_use]
    pub fn buffered_rows(&self) -> usize {
        let committed: usize = self.committed.iter().map(CommittedTransaction::rows).sum();
        let staged = self
            .open_transaction
            .as_ref()
            .and_then(|transaction| transaction.staged.as_ref())
            .map_or(0, |staged| staged.rows.len());
        committed + staged + self.open_rows.as_ref().map_or(0, RowBuilder::len)
    }

    fn fail(&mut self, error: ConnectorError) -> ConnectorError {
        self.state = ConnectorState::Failed;
        self.failure.get_or_insert_with(|| error.to_string());
        error
    }
}

#[cfg(test)]
mod tests;
