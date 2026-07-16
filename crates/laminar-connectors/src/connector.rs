//! Connector traits — async `SourceConnector` / `SinkConnector`.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

/// Delivery guarantee level for the pipeline.
///
/// Configures the expected end-to-end delivery semantics. The pipeline
/// validates at startup that all sources and sinks meet the requirements
/// for the chosen guarantee level.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    /// Best effort: no replay contract. Intended for bare-metal/embedded
    /// low-latency pipelines that explicitly accept loss on failure.
    #[default]
    BestEffort,
    /// At-least-once: records may be replayed on recovery. Requires
    /// checkpointing and replayable sources.
    AtLeastOnce,
    /// Exactly-once: no duplicates or losses. Requires all sources to
    /// support replay, all sinks to support exactly-once, and checkpoint
    /// to be enabled.
    ExactlyOnce,
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryGuarantee::BestEffort => write!(f, "best-effort"),
            DeliveryGuarantee::AtLeastOnce => write!(f, "at-least-once"),
            DeliveryGuarantee::ExactlyOnce => write!(f, "exactly-once"),
        }
    }
}

impl FromStr for DeliveryGuarantee {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "best_effort" | "besteffort" | "none" => Ok(Self::BestEffort),
            "at_least_once" | "atleastonce" => Ok(Self::AtLeastOnce),
            "exactly_once" | "exactlyonce" => Ok(Self::ExactlyOnce),
            other => Err(format!("unknown delivery guarantee: '{other}'")),
        }
    }
}

/// Recovery semantics provided by a source.
///
/// This is deliberately a small, ordered set of operational contracts rather
/// than a collection of independent capability flags. A source must advertise
/// the strongest contract its implementation can actually uphold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceConsistency {
    /// Events cannot be reconstructed after the runtime has accepted them.
    #[default]
    Ephemeral,
    /// A persisted source position can be used to reproduce accepted events.
    Replayable,
    /// Replay is supported, and upstream progress/resources advance only when
    /// the corresponding `LaminarDB` checkpoint is durably committed.
    CommitCoupled,
}

impl SourceConsistency {
    /// Whether a persisted source position can be replayed after recovery.
    #[must_use]
    pub const fn supports_replay(self) -> bool {
        !matches!(self, Self::Ephemeral)
    }

    /// Whether checkpoint commits are required for safe upstream progress.
    #[must_use]
    pub const fn requires_checkpointing(self) -> bool {
        matches!(self, Self::CommitCoupled)
    }
}

/// How a source may be placed across runtime nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceTopology {
    /// Exactly one runtime instance owns the source.
    #[default]
    Singleton,
    /// Input partitions can be assigned independently across runtime nodes.
    Splittable,
    /// Each runtime node receives a distinct, node-local input stream.
    NodeLocalIngress,
}

/// Complete source admission contract for a concrete connector configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SourceContract {
    /// Recovery and external-progress semantics.
    pub consistency: SourceConsistency,
    /// Valid runtime placement model.
    pub topology: SourceTopology,
    exact_delivery_certified: bool,
}

impl SourceContract {
    /// Construct a source contract from its recovery and placement dimensions.
    /// Exactly-once certification defaults to fail-closed.
    #[must_use]
    pub const fn new(consistency: SourceConsistency, topology: SourceTopology) -> Self {
        Self {
            consistency,
            topology,
            exact_delivery_certified: false,
        }
    }

    /// Mark a built-in connector whose exact-delivery suite is an engine release gate.
    #[must_use]
    pub(crate) const fn with_exact_delivery_certification(mut self) -> Self {
        self.exact_delivery_certified = true;
        self
    }

    /// Whether this source is certified for exactly-once delivery.
    #[doc(hidden)]
    #[must_use]
    pub const fn is_exact_delivery_certified(self) -> bool {
        self.exact_delivery_certified
    }

    /// Whether a persisted source position can be replayed after recovery.
    #[must_use]
    pub const fn supports_replay(self) -> bool {
        self.consistency.supports_replay()
    }

    /// Whether checkpoint commits are required for safe upstream progress.
    #[must_use]
    pub const fn requires_checkpointing(self) -> bool {
        self.consistency.requires_checkpointing()
    }
}

/// Durability protocol provided by a sink.
///
/// This describes externally observable behaviour, not an implementation
/// detail such as whether the client library buffers or retries writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SinkConsistency {
    /// An accepted write can be lost when the connector or peer fails.
    #[default]
    Ephemeral,
    /// Successful writes are durably acknowledged, but replay may duplicate
    /// them because visibility is not coupled to a `LaminarDB` checkpoint.
    DurableAtLeastOnce,
    /// Output can be staged and made visible by the checkpoint commit
    /// protocol. This is necessary, but not sufficient, for exactly-once
    /// certification: namespaces and recovery cursors must also be fenced.
    CheckpointCommittable,
}

/// How a sink may be placed across runtime nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SinkTopology {
    /// Only one fenced runtime writer may target the configured destination.
    #[default]
    Singleton,
    /// Independent runtime writers can safely target the destination.
    MultiWriter,
    /// Each runtime node owns a distinct local egress endpoint or audience.
    NodeLocalEgress,
}

/// The strongest input update model a configured sink understands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SinkInputMode {
    /// Inserts only; retractions or deletes would be lost.
    #[default]
    AppendOnly,
    /// Rows are reconciled by a configured key, but the connector does not
    /// consume a native full-changelog envelope.
    KeyedUpsert,
    /// Inserts, updates, and deletes/retractions are represented faithfully.
    FullChangelog,
}

impl SinkInputMode {
    /// Whether this mode can faithfully consume a full Z-set changelog,
    /// including deletes/retractions.
    #[must_use]
    pub const fn accepts_full_changelog(self) -> bool {
        matches!(self, Self::FullChangelog)
    }
}

/// Complete sink admission contract for a concrete connector configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SinkContract {
    /// Durability and checkpoint-commit semantics.
    pub consistency: SinkConsistency,
    /// Valid runtime placement model.
    pub topology: SinkTopology,
    /// Strongest supported input update model.
    pub input_mode: SinkInputMode,
}

impl SinkContract {
    /// Construct a sink contract from its three explicit dimensions.
    #[must_use]
    pub const fn new(
        consistency: SinkConsistency,
        topology: SinkTopology,
        input_mode: SinkInputMode,
    ) -> Self {
        Self {
            consistency,
            topology,
            input_mode,
        }
    }

    /// Whether this contract participates in checkpoint-owned external commit.
    #[must_use]
    pub const fn is_checkpoint_committable(self) -> bool {
        matches!(self.consistency, SinkConsistency::CheckpointCommittable)
    }

    /// Whether this contract faithfully consumes inserts, updates, and retractions.
    #[must_use]
    pub const fn accepts_full_changelog(self) -> bool {
        self.input_mode.accepts_full_changelog()
    }
}

/// A batch of records read from a source connector.
#[derive(Debug, Clone)]
pub struct SourceBatch {
    /// Arrow batch carrying the records.
    pub records: RecordBatch,
    /// The partition this batch came from, if the source is partitioned.
    pub partition: Option<PartitionInfo>,
}

impl SourceBatch {
    /// Construct without partition metadata.
    #[must_use]
    pub fn new(records: RecordBatch) -> Self {
        Self {
            records,
            partition: None,
        }
    }

    /// Construct with partition metadata attached.
    #[must_use]
    pub fn with_partition(records: RecordBatch, partition: PartitionInfo) -> Self {
        Self {
            records,
            partition: Some(partition),
        }
    }

    /// Record count in the batch.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.records.num_rows()
    }
}

/// Source partition identity + current offset (Kafka partition number,
/// CDC slot name, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionInfo {
    /// Partition id — free-form string (Kafka partition number as string,
    /// CDC slot name, file path, …).
    pub id: String,
    /// Current offset — interpretation is connector-specific (Kafka offset
    /// as string, CDC LSN, etc.).
    pub offset: String,
}

impl PartitionInfo {
    /// Construct from id/offset strings or anything that converts.
    #[must_use]
    pub fn new(id: impl Into<String>, offset: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            offset: offset.into(),
        }
    }
}

impl fmt::Display for PartitionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.offset)
    }
}

/// Summary of a successful `write_batch` call.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Records accepted by the sink.
    pub records_written: usize,
    /// Bytes written to the underlying transport (may be estimated).
    pub bytes_written: u64,
}

impl WriteResult {
    /// Construct with raw counts.
    #[must_use]
    pub fn new(records_written: usize, bytes_written: u64) -> Self {
        Self {
            records_written,
            bytes_written,
        }
    }
}

/// How the runtime may enforce a deadline around a started connector operation.
///
/// This is an internal connector/driver capability, not a deployment option.
/// Drivers that can become inconsistent when their futures are dropped must
/// finish the exact started future before the actor processes later work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorCancellationPolicy {
    /// Dropping an in-flight future leaves the connector valid for recovery or reuse.
    CancelSafe,
    /// Once polled, the future must be polled to completion even after its deadline.
    CompleteStarted,
}

/// Atomic startup position for a source connector.
///
/// A resume request carries both the durable checkpoint attempt and the
/// connector checkpoint captured by that attempt. Connectors must install the
/// position before `start` returns and before `poll_batch` can emit records.
#[derive(Debug, Clone)]
pub enum SourcePosition {
    /// Start from the connector's configured deterministic initial position.
    Initial,
    /// Resume from an exact durable engine checkpoint.
    Resume {
        /// Checkpoint attempt that owns the connector state.
        attempt: laminar_core::state::CheckpointAttempt,
        /// Connector cursor captured by `attempt`.
        checkpoint: SourceCheckpoint,
    },
}

/// Complete source startup request.
///
/// Startup is intentionally a single operation so a connector cannot become
/// externally active between opening resources and restoring its position.
#[derive(Debug, Clone)]
pub struct SourceStart {
    /// Fully resolved connector configuration.
    pub config: ConnectorConfig,
    /// Initial or exact recovery position.
    pub position: SourcePosition,
    /// Pipeline-wide delivery guarantee used for fail-closed cursor policy.
    pub delivery: DeliveryGuarantee,
}

/// Exact cluster transition and local vnode subset a partitioned source must drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDrainRequest {
    /// Compact predecessor/target/leader identity.
    pub round: laminar_core::checkpoint::AssignmentDrainId,
    /// Strictly ascending local vnodes whose input ownership is being revoked.
    pub revoking_vnodes: Arc<[u32]>,
}

impl SourceDrainRequest {
    /// Construct a canonical source drain request.
    ///
    /// # Errors
    /// Returns an error when the round or vnode set is not canonical.
    pub fn new(
        round: laminar_core::checkpoint::AssignmentDrainId,
        revoking_vnodes: Arc<[u32]>,
    ) -> Result<Self, ConnectorError> {
        if !round.is_canonical() {
            return Err(ConnectorError::ConfigurationError(
                "source drain round is not canonical".into(),
            ));
        }
        laminar_core::checkpoint::source_drain_vnode_digest(&revoking_vnodes)
            .map_err(ConnectorError::ConfigurationError)?;
        Ok(Self {
            round,
            revoking_vnodes,
        })
    }
}

/// Connector-owned proof of the concrete external-input cut behind a source receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectorDrainCut {
    /// Number of concrete external inputs paused by the connector.
    pub revoked_input_count: u32,
    /// Canonical digest of those connector-specific input identities.
    pub revoked_input_digest: [u8; 32],
    /// Canonical digest of their next-to-read positions at the cut.
    pub cut_cursor_digest: [u8; 32],
}

impl ConnectorDrainCut {
    /// Canonical empty cut used when no external input maps to the revoking vnode set.
    #[must_use]
    pub fn empty() -> Self {
        let empty_inputs = Sha256::digest(b"laminardb-source-drain-inputs-v1\0\0\0\0\0\0\0\0");
        let empty_cursor = Sha256::digest(b"laminardb-source-drain-cursor-v1\0\0\0\0\0\0\0\0");
        Self {
            revoked_input_count: 0,
            revoked_input_digest: empty_inputs.into(),
            cut_cursor_digest: empty_cursor.into(),
        }
    }

    /// Whether the cut has non-empty canonical digests.
    #[must_use]
    pub fn is_canonical(self) -> bool {
        self.revoked_input_digest != [0; 32] && self.cut_cursor_digest != [0; 32]
    }
}

/// Result of starting a non-blocking connector drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDrainStart {
    /// The connector reader is pausing inputs and will later publish a FIFO cut.
    Pending,
    /// No asynchronous reader work is required for this request.
    Ready(ConnectorDrainCut),
}

/// Terminal resolution of one exact source drain round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDrainOutcome {
    /// The target assignment committed; revoked inputs must be unassigned before release.
    Commit,
    /// The transition aborted; revoked inputs must rewind to the cut before release.
    Abort,
}

/// Exact round resolution delivered to a partitioned source connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceDrainResolution {
    /// Round being resolved.
    pub round: laminar_core::checkpoint::AssignmentDrainId,
    /// Durable transition outcome.
    pub outcome: SourceDrainOutcome,
}

/// Trait for source connectors that read data from external systems.
///
/// Source connectors operate in Ring 1 and push data into Ring 0 via
/// the streaming `Source<ArrowRecord>::push_arrow()` API.
///
/// # Lifecycle
///
/// 1. `start()` — atomically install the configured or recovered cursor and initialize the reader
/// 2. `poll_batch()` — read batches in a loop
/// 3. `checkpoint()` — capture the current connector cursor
/// 4. `close()` — clean shutdown
#[async_trait]
pub trait SourceConnector: Send {
    /// Deadline behavior required by the underlying client implementation.
    ///
    /// Completion is the conservative default: a new connector must not be
    /// cancellation-enabled until every lifecycle future has been audited.
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CompleteStarted
    }

    /// Opens the source and establishes its initial or resumed position as one
    /// indivisible lifecycle transition.
    ///
    /// Implementations must not emit records or expose an externally active
    /// consumer before the requested position has been applied successfully.
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError>;

    /// `Ok(None)` = no data currently available; runtime retries after a delay.
    /// `max_records` is the normal batching target. A source may exceed it only
    /// when one upstream atomic replay unit cannot be split without making its
    /// checkpoint cursor invalid. Such sources must enforce independent hard
    /// record and byte limits and fail before retained data can grow unbounded.
    ///
    /// The runtime cancels this future only when the connector explicitly
    /// declares [`ConnectorCancellationPolicy::CancelSafe`]. Such
    /// implementations must not advance external or checkpoint-visible
    /// position across an `.await` unless dropping the future at that point
    /// preserves replay of every not-yet-returned record. Stage work privately,
    /// then advance the cursor and return without another cancellation point.
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError>;

    /// Resolve the source schema from the `WITH (...)` properties before
    /// DDL reaches the planner. Implementations that hit the network
    /// (e.g. Kafka fetching an Avro schema from a Schema Registry) must
    /// bound their I/O with a timeout. Return `Err(ConnectorError::…)` on
    /// failure so the runtime can surface the cause to DDL — do not log
    /// and swallow.
    async fn discover_schema(
        &mut self,
        _properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Arrow schema of records this source produces.
    fn schema(&self) -> SchemaRef;

    /// Returned checkpoint must contain enough info to resume from the
    /// current position after a restart.
    fn checkpoint(&self) -> SourceCheckpoint;

    /// Atomically attempt to capture a source cursor. `Ok(None)` means a
    /// transient control-plane publication is not reconciled yet; the runtime
    /// retains the barrier and retries without advancing it.
    ///
    /// # Errors
    /// Returns a connector error when the source cannot produce a valid cursor for the current
    /// ownership/configuration state.
    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        Ok(Some(self.checkpoint()))
    }

    /// Whether this source's data-plane cursor is reconciled with the current
    /// control-plane ownership publication. The runtime does not poll records
    /// or admit checkpoint barriers while this is false.
    ///
    /// Non-partitioned and local sources are always ready. Cluster-aware
    /// sources override this with a lock-free version fence.
    ///
    /// # Errors
    /// Returns a connector error when control-plane reconciliation detects invalid or lost
    /// ownership state that cannot be retried safely.
    fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
        Ok(true)
    }

    /// Start or advance connector-side control-plane work needed to become
    /// [`checkpoint_ready`](Self::checkpoint_ready). Called even while data
    /// polling and barriers are fenced.
    fn drive_control_plane(&mut self) {}

    /// Start an exact partition drain without waiting for the reader FIFO to empty.
    ///
    /// Implementations return [`SourceDrainStart::Pending`] and later expose the cut through
    /// [`Self::take_drain_cut`]. The default accepts only an empty vnode set.
    fn begin_drain(
        &mut self,
        request: &SourceDrainRequest,
    ) -> Result<SourceDrainStart, ConnectorError> {
        if request.revoking_vnodes.is_empty() {
            Ok(SourceDrainStart::Ready(ConnectorDrainCut::empty()))
        } else {
            Err(ConnectorError::ConfigurationError(
                "partitioned source does not implement vnode drain".into(),
            ))
        }
    }

    /// Take the exact external-input cut after its FIFO boundary has been consumed.
    ///
    /// Returns `None` while the reader is still pausing or while pre-boundary payloads remain.
    fn take_drain_cut(
        &mut self,
        _round: laminar_core::checkpoint::AssignmentDrainId,
    ) -> Result<Option<ConnectorDrainCut>, ConnectorError> {
        Ok(None)
    }

    /// Resolve an exact drain after target commit or abort.
    ///
    /// An abort must rewind any client-delivered but engine-unaccepted records before resuming;
    /// a commit must unassign revoked inputs before clearing its post-cut filter.
    async fn finish_drain(
        &mut self,
        _resolution: SourceDrainResolution,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Install the cluster vnode assignment for a source that advertises
    /// [`SourceTopology::Splittable`]. The source identity is the stable,
    /// canonical catalog object name and must be part of any external split
    /// mapping ABI.
    ///
    /// Embedded, single-node, singleton, and node-local sources are not sent
    /// this hook. The default fails closed so an extension cannot advertise
    /// splittable placement while every cluster node reads the full input.
    ///
    /// # Errors
    /// Returns a configuration error unless the connector implements exact
    /// vnode-scoped input ownership.
    fn set_vnode_assignment(
        &mut self,
        _source_identity: &str,
        _registry: Arc<laminar_core::state::VnodeRegistry>,
        _self_id: laminar_core::state::NodeId,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "source advertises splittable placement but does not implement vnode assignment".into(),
        ))
    }

    /// Close the connection and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Returns a [`Notify`] handle that is signalled when new data is available.
    ///
    /// When `Some`, the pipeline coordinator awaits the notification instead of
    /// polling on a timer, eliminating idle CPU usage. Sources that receive data
    /// asynchronously (WebSocket, CDC replication streams, Kafka) should return
    /// `Some` and call `notify.notify_one()` when data arrives.
    ///
    /// The default implementation returns `None`, which causes the pipeline to
    /// fall back to timer-based polling (suitable for batch/file sources).
    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        None
    }

    /// Declare recovery and placement semantics for this exact configuration.
    ///
    /// The fail-closed default is an ephemeral singleton. Durable or
    /// distributed semantics must be opted into by the connector explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error when the concrete configuration cannot provide a valid
    /// recovery or placement contract. The default implementation never fails.
    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::default())
    }

    /// Return connector-owned semantic options for durable recovery identity.
    ///
    /// The hook must be deterministic, configuration-only, and free of external
    /// I/O. `Some` replaces the raw property map in the pipeline identity;
    /// `None` asks the runtime to use its conservative sanitized-property
    /// fallback. A connector may omit operational endpoints or credentials only
    /// when its checkpoint independently binds the exact external object.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the semantic identity cannot be
    /// derived from the supplied source configuration.
    fn recovery_identity_options(
        &self,
        _config: &ConnectorConfig,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>, ConnectorError> {
        Ok(None)
    }

    /// Acknowledge that `epoch` has been durably committed.
    ///
    /// Called after the manifest and exact engine commit decision are durable. Coordinated
    /// external sink publication may complete asynchronously afterward. The `checkpoint` is the exact
    /// per-source `SourceCheckpoint` that was persisted into the manifest
    /// for this epoch — sources can rely on it to advance external offset
    /// state (broker group offsets, lookup-DB cursors, ack tokens) using
    /// values that match what's durable.
    ///
    /// May be called with an empty `checkpoint` for timer-driven commits
    /// where no per-source state was captured; implementations should
    /// treat that as a no-op for any externally-visible advancement.
    ///
    /// Idempotent — a retry after cancellation is legal.
    ///
    /// # Errors
    ///
    /// The epoch is already durable and cannot be rolled back. During normal processing an error
    /// faults replay-capable pipelines so recovery retries the advisory upstream commit; during
    /// shutdown it is logged and the durable `LaminarDB` checkpoint remains authoritative.
    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        _checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Trait for sink connectors that write data to external systems.
///
/// Sink connectors operate in Ring 1, receiving data from Ring 0 and
/// writing to external systems. Implementations whose contract is
/// [`SinkConsistency::CheckpointCommittable`] prepare checkpoint-owned
/// committables with `begin_epoch`/`pre_commit`, expose a
/// [`CoordinatedCommitter`] for the single external commit, and implement
/// `rollback_epoch`; the runtime drives them via the checkpoint coordinator.
///
/// All sinks follow `open()` → `write_batch()`/`flush()` → `close()`.
/// Checkpoint-committable sinks additionally loop over `begin_epoch()`, staged
/// writes, `pre_commit()`, and coordinated commit (or `rollback_epoch()` on a
/// proven pre-decision failure).
#[async_trait]
pub trait SinkConnector: Send {
    /// Deadline behavior required by the underlying client implementation.
    ///
    /// Completion is the conservative default: a new connector must not be
    /// cancellation-enabled until every lifecycle future has been audited.
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CompleteStarted
    }

    /// Declare durability, placement, and input semantics for this exact
    /// configuration without opening files, sockets, clients, or transactions.
    ///
    /// The fail-closed default is an ephemeral append-only singleton. Durable
    /// or distributed behaviour must be opted into explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error when the concrete configuration cannot provide a valid
    /// durability, placement, or input contract. The default implementation never fails.
    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(SinkContract::default())
    }

    /// Open the connection and prepare to accept writes.
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;

    /// Implementations using [`ConnectorCancellationPolicy::CancelSafe`] must
    /// remain valid when this future is dropped at a deadline. For
    /// [`ConnectorCancellationPolicy::CompleteStarted`], the runtime keeps
    /// polling the exact future to completion before processing later work.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError>;

    /// Expected Arrow schema of input batches.
    fn schema(&self) -> SchemaRef;

    /// Begin checkpoint-owned staging. Called only for an admitted
    /// checkpoint-committable contract; weaker sinks use the no-op default.
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Flush + prepare, but do not finalize externally. The runtime persists
    /// the checkpoint decision before a designated committer finalizes the
    /// collected descriptors; on failure it calls `rollback_epoch`.
    ///
    /// Returns an opaque commit descriptor for checkpoint-committable sinks (the
    /// committables the designated committer will aggregate), else `None`.
    /// Default delegates to `flush()` and returns `None`.
    ///
    /// # Errors
    /// Returns `ConfigurationError` if the sink exposes a coordinated committer
    /// yet relies on this default — it would seal epochs with no external commit.
    async fn pre_commit(&mut self, _epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        if self.as_coordinated_committer().is_some() {
            return Err(ConnectorError::ConfigurationError(
                "sink exposes a coordinated committer but does not override pre_commit".into(),
            ));
        }
        self.flush().await?;
        Ok(None)
    }

    /// Must be idempotent. The runtime calls this on every
    /// checkpoint-committable sink after proving a pre-decision failure,
    /// including sinks that never completed `pre_commit`.
    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Default per-call `write_batch` I/O timeout. Users can override this via
    /// the `sink.write.timeout.ms` connector property.
    fn suggested_write_timeout(&self) -> std::time::Duration;

    /// Maximum residence time for a non-empty sink buffer before the runtime
    /// invokes [`flush`](Self::flush). Checkpoint-committable sinks ignore the
    /// periodic timer and flush only through their checkpoint protocol.
    fn flush_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }

    /// Must be internally bounded — the sink task's periodic timer
    /// calls this on every tick. Thorough drains belong in `pre_commit`
    /// / coordinated commit / `close`, not here.
    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Close the sink and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Leader-side committer for a checkpoint-committable contract; `None`
    /// for every weaker contract.
    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
        None
    }
}

/// Fixed control-plane bound for one connector's coordinated-commit payload.
///
/// Connectors must keep prepared metadata at or below this limit before
/// returning it to the checkpoint runtime. Bulk records belong in the sink's
/// data plane, referenced by the bounded payload.
pub const MAX_COORDINATED_COMMIT_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Fixed aggregate control-plane bound for one designated commit call.
pub const MAX_COORDINATED_COMMIT_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Fixed participant-marker bound for one designated commit call.
pub const MAX_COORDINATED_COMMIT_BATCH_ENTRIES: usize = 4_096;

/// Stable external commit namespace for one deployment incarnation of a logical pipeline sink.
///
/// The configured target (Delta table or Iceberg table) already scopes the
/// external metadata. The create-once deployment id prevents checkpoint-store resets or two
/// identically configured deployments from sharing a cursor. Pipeline identity plus sink id then
/// binds that deployment to one recovery-compatible logical writer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedCommitNamespace {
    /// Canonical logical-pipeline identity used by checkpoint recovery.
    pub pipeline_identity: laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity,
    /// Create-once UUID stored with checkpoint decisions and shared by every cluster member.
    pub deployment_id: String,
    /// Stable sink registration id within the pipeline.
    pub sink_id: String,
}

impl CoordinatedCommitNamespace {
    /// Construct and validate a namespace before any external metadata lookup.
    ///
    /// # Errors
    /// Returns a configuration error for a malformed pipeline digest or empty
    /// sink id.
    pub fn try_new(
        pipeline_identity: laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity,
        deployment_id: impl Into<String>,
        sink_id: impl Into<String>,
    ) -> Result<Self, ConnectorError> {
        let deployment_id = deployment_id.into();
        let sink_id = sink_id.into();
        if pipeline_identity.sha256.len() != 64
            || !pipeline_identity
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit requires a canonical lowercase SHA-256 pipeline identity"
                    .into(),
            ));
        }
        if sink_id.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit sink id cannot be empty".into(),
            ));
        }
        let parsed_deployment = uuid::Uuid::parse_str(&deployment_id).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "coordinated commit deployment id is not a UUID: {error}"
            ))
        })?;
        if parsed_deployment.is_nil() || parsed_deployment.to_string() != deployment_id {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit deployment id must be a canonical non-nil UUID".into(),
            ));
        }
        Ok(Self {
            pipeline_identity,
            deployment_id,
            sink_id,
        })
    }

    /// Bounded, filesystem/catalog-safe key for external transaction metadata.
    #[must_use]
    pub fn external_key(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.pipeline_identity.canonical_version.to_be_bytes());
        digest.update(self.pipeline_identity.sha256.as_bytes());
        digest.update([0]);
        digest.update(self.deployment_id.as_bytes());
        digest.update([0]);
        digest.update(self.sink_id.as_bytes());
        let digest = digest.finalize();
        format!("ldb-c3-{digest:x}")
    }
}

/// Exact external commit position and the authority that published it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedCommitCursor {
    /// Highest globally unique checkpoint id atomically reflected by the sink.
    pub checkpoint_id: u64,
    /// Monotonic authority token that fenced earlier designated committers.
    pub fencing_token: u64,
}

/// One participant's validated prepared marker for one exact attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedCommitPayload {
    /// Exact checkpoint attempt that admitted this marker.
    pub attempt: laminar_core::state::CheckpointAttempt,
    /// Stable runtime participant id (`0` in local modes).
    pub participant_id: u64,
    /// Connector-specific committable, or `None` for an explicitly empty cut.
    pub payload: Option<Vec<u8>>,
}

/// Exact batch submitted to a designated external-sink committer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedCommitBatch {
    /// External cursor namespace.
    pub namespace: CoordinatedCommitNamespace,
    /// Exact external cursor that must precede this batch. The zero cursor names
    /// an empty target. A different authority at the predecessor checkpoint is
    /// a conflicting history and must fail closed.
    pub expected_predecessor: CoordinatedCommitCursor,
    /// Non-zero authority token that the external commit must persist atomically.
    pub fencing_token: u64,
    /// Highest exact attempt atomically covered by this commit.
    pub target: laminar_core::state::CheckpointAttempt,
    /// Every sealed participant marker through `target`, including empty ones.
    pub entries: Vec<CoordinatedCommitPayload>,
}

/// Runtime-owned deadline for one designated external publication.
///
/// The deadline is created before the command enters the sink actor, so a
/// connector sees the actual budget left after queueing rather than a second,
/// connector-local timeout window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatedCommitContext {
    deadline: tokio::time::Instant,
}

impl CoordinatedCommitContext {
    /// Create a context from the sink actor's absolute end-to-end deadline.
    #[must_use]
    pub const fn new(deadline: tokio::time::Instant) -> Self {
        Self { deadline }
    }

    /// Absolute monotonic publication deadline.
    #[must_use]
    pub const fn deadline(self) -> tokio::time::Instant {
        self.deadline
    }

    /// Budget still available at the point the connector starts publication.
    #[must_use]
    pub fn remaining(self) -> std::time::Duration {
        self.deadline
            .saturating_duration_since(tokio::time::Instant::now())
    }
}

impl CoordinatedCommitBatch {
    /// Collision-resistant identity for one exact ordered publication cut.
    /// Every variable-length field is length framed so distinct batches cannot
    /// share an input byte stream before hashing.
    #[must_use]
    pub fn exact_fingerprint(&self) -> [u8; 32] {
        fn update_length(hasher: &mut Sha256, length: usize) {
            let source = length.to_be_bytes();
            let mut encoded = [0_u8; 16];
            let start = encoded.len() - source.len();
            encoded[start..].copy_from_slice(&source);
            hasher.update(encoded);
        }

        fn update_framed(hasher: &mut Sha256, bytes: &[u8]) {
            update_length(hasher, bytes.len());
            hasher.update(bytes);
        }

        let mut hasher = Sha256::new();
        update_framed(&mut hasher, b"laminardb/coordinated-commit-batch/v1");
        update_framed(&mut hasher, self.namespace.external_key().as_bytes());
        hasher.update(self.expected_predecessor.checkpoint_id.to_be_bytes());
        hasher.update(self.expected_predecessor.fencing_token.to_be_bytes());
        hasher.update(self.fencing_token.to_be_bytes());
        hasher.update(self.target.epoch.to_be_bytes());
        hasher.update(self.target.checkpoint_id.to_be_bytes());
        update_length(&mut hasher, self.entries.len());
        for entry in &self.entries {
            hasher.update(entry.attempt.epoch.to_be_bytes());
            hasher.update(entry.attempt.checkpoint_id.to_be_bytes());
            hasher.update(entry.participant_id.to_be_bytes());
            match &entry.payload {
                Some(payload) => {
                    hasher.update([1]);
                    update_framed(&mut hasher, payload);
                }
                None => hasher.update([0]),
            }
        }
        hasher.finalize().into()
    }

    /// Validate canonical attempt/participant order and all fixed control-plane bounds.
    /// This check is independent of external state and must run before connector I/O.
    ///
    /// # Errors
    /// Returns a diagnostic when the batch is malformed or exceeds a fixed bound.
    pub fn validate_shape(&self) -> Result<(), String> {
        use laminar_core::state::CheckpointAttemptRelation;

        if self.expected_predecessor.checkpoint_id >= self.target.checkpoint_id {
            return Err(format!(
                "invalid coordinated batch predecessor {} for target {}",
                self.expected_predecessor.checkpoint_id, self.target.checkpoint_id
            ));
        }
        if (self.expected_predecessor.checkpoint_id == 0)
            != (self.expected_predecessor.fencing_token == 0)
        {
            return Err(
                "coordinated batch predecessor must be either an exact non-zero cursor or the zero cursor"
                    .into(),
            );
        }
        if self.fencing_token == 0 {
            return Err("coordinated batch fencing token must be non-zero".into());
        }
        if self.entries.is_empty() || self.entries.len() > MAX_COORDINATED_COMMIT_BATCH_ENTRIES {
            return Err(format!(
                "coordinated batch entry count must be in 1..={MAX_COORDINATED_COMMIT_BATCH_ENTRIES}"
            ));
        }

        let mut total_payload_bytes = 0usize;
        let mut previous: Option<&CoordinatedCommitPayload> = None;
        for entry in &self.entries {
            if entry.attempt.checkpoint_id <= self.expected_predecessor.checkpoint_id
                || entry.attempt.checkpoint_id > self.target.checkpoint_id
            {
                return Err(
                    "coordinated batch entries do not cover the predecessor-to-target interval"
                        .into(),
                );
            }
            if let Some(payload) = &entry.payload {
                if payload.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES {
                    return Err(format!(
                        "coordinated participant payload exceeds the fixed {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} byte limit"
                    ));
                }
                total_payload_bytes = total_payload_bytes
                    .checked_add(payload.len())
                    .ok_or_else(|| "coordinated batch payload byte count overflow".to_owned())?;
                if total_payload_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
                    return Err(format!(
                        "coordinated batch payloads exceed the fixed {MAX_COORDINATED_COMMIT_BATCH_BYTES} byte limit"
                    ));
                }
            }

            if let Some(previous) = previous {
                match entry.attempt.relation_to(previous.attempt) {
                    CheckpointAttemptRelation::Exact
                        if entry.participant_id > previous.participant_id => {}
                    CheckpointAttemptRelation::Newer => {}
                    CheckpointAttemptRelation::Exact => {
                        return Err(
                            "coordinated batch contains a duplicate or out-of-order attempt/participant key"
                                .into(),
                        );
                    }
                    CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                        return Err(
                            "coordinated batch attempts are not in coherent epoch/checkpoint order"
                                .into(),
                        );
                    }
                }
            }
            previous = Some(entry);
        }
        if previous.map(|entry| entry.attempt) != Some(self.target) {
            return Err("coordinated batch target is not its final exact attempt".into());
        }
        Ok(())
    }

    /// Validate a cursor freshly read from the external target against this
    /// exact batch. Advancing overlap is safe only at an attempt named by the
    /// batch; rollback or an unproven gap would skip output.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when the batch is malformed, the observed cursor
    /// proves rollback, or an overlap cannot be tied to an exact batch entry.
    pub fn validate_observed_cursor(
        &self,
        observed: Option<CoordinatedCommitCursor>,
    ) -> Result<(), String> {
        self.validate_shape()?;
        let Some(observed) = observed else {
            return if self.expected_predecessor.checkpoint_id == 0 {
                Ok(())
            } else {
                Err(format!(
                    "external cursor is absent below expected predecessor {}",
                    self.expected_predecessor.checkpoint_id
                ))
            };
        };
        if observed.fencing_token == 0 {
            return Err("external cursor contains a zero fencing token".into());
        }
        if observed.fencing_token > self.fencing_token {
            return Err(format!(
                "external fencing token {} is newer than designated committer token {}",
                observed.fencing_token, self.fencing_token
            ));
        }
        if observed.checkpoint_id >= self.target.checkpoint_id
            && observed.fencing_token != self.fencing_token
        {
            return Err(format!(
                "external cursor at or above target {} has fencing token {}, expected {}",
                self.target.checkpoint_id, observed.fencing_token, self.fencing_token
            ));
        }
        if observed.checkpoint_id < self.expected_predecessor.checkpoint_id {
            return Err(format!(
                "external cursor rolled back from expected predecessor {} to {}",
                self.expected_predecessor.checkpoint_id, observed.checkpoint_id
            ));
        }
        if observed.checkpoint_id == self.expected_predecessor.checkpoint_id
            && observed != self.expected_predecessor
        {
            return Err(format!(
                "external cursor checkpoint {} has fencing token {}, expected predecessor token {}",
                observed.checkpoint_id,
                observed.fencing_token,
                self.expected_predecessor.fencing_token
            ));
        }
        if observed.checkpoint_id > self.expected_predecessor.checkpoint_id
            && observed.fencing_token < self.expected_predecessor.fencing_token
        {
            return Err(format!(
                "external cursor advanced past predecessor {} while fencing token regressed from {} to {}",
                self.expected_predecessor.checkpoint_id,
                self.expected_predecessor.fencing_token,
                observed.fencing_token
            ));
        }
        if observed.checkpoint_id < self.target.checkpoint_id
            && observed.checkpoint_id != self.expected_predecessor.checkpoint_id
            && !self
                .entries
                .iter()
                .any(|entry| entry.attempt.checkpoint_id == observed.checkpoint_id)
        {
            return Err(format!(
                "external cursor {} is not an exact attempt in batch {}..={}",
                observed.checkpoint_id,
                self.expected_predecessor.checkpoint_id,
                self.target.checkpoint_id
            ));
        }
        Ok(())
    }
}

/// Leader-side commit for checkpoint-committable sinks.
///
/// The designated committer aggregates every writer's `pre_commit` descriptor
/// for an epoch into one external commit. Must be idempotent: re-running with
/// the same inputs after a leader failover is a no-op once the target already
/// reflects the epoch.
#[async_trait]
pub trait CoordinatedCommitter: Send + Sync {
    /// Atomically commit the validated participant markers and advance the
    /// namespaced external cursor to the batch's exact target. Empty markers
    /// still advance the cursor.
    async fn commit_aggregated(
        &self,
        batch: CoordinatedCommitBatch,
        context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError>;

    /// Highest checkpoint and fencing authority committed in `namespace`.
    /// A metadata read error must be returned, never converted to an absent
    /// cursor, because that could duplicate a previously committed batch.
    async fn committed_cursor(
        &self,
        namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError>;
}

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_batch(n: usize) -> RecordBatch {
        #[allow(clippy::cast_possible_wrap)]
        let ids: Vec<i64> = (0..n as i64).collect();
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
    }

    #[test]
    fn test_source_batch() {
        let batch = SourceBatch::new(test_batch(10));
        assert_eq!(batch.num_rows(), 10);
        assert!(batch.partition.is_none());
    }

    #[test]
    fn test_source_batch_with_partition() {
        let partition = PartitionInfo::new("0", "1234");
        let batch = SourceBatch::with_partition(test_batch(5), partition);
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.partition.as_ref().unwrap().id, "0");
        assert_eq!(batch.partition.as_ref().unwrap().offset, "1234");
    }

    #[test]
    fn test_partition_info_display() {
        let p = PartitionInfo::new("3", "42");
        assert_eq!(p.to_string(), "3@42");
    }

    #[test]
    fn test_write_result() {
        let result = WriteResult::new(100, 5000);
        assert_eq!(result.records_written, 100);
        assert_eq!(result.bytes_written, 5000);
    }

    #[test]
    fn source_drain_request_requires_sorted_unique_vnodes() {
        let round = laminar_core::checkpoint::AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        assert!(SourceDrainRequest::new(round, Arc::from([1_u32, 4, 9])).is_ok());
        assert!(SourceDrainRequest::new(round, Arc::from([4_u32, 1])).is_err());
        assert!(SourceDrainRequest::new(round, Arc::from([1_u32, 1])).is_err());
        assert!(ConnectorDrainCut::empty().is_canonical());
    }

    #[test]
    fn source_contract_defaults_fail_closed() {
        let contract = SourceContract::default();
        assert_eq!(contract.consistency, SourceConsistency::Ephemeral);
        assert_eq!(contract.topology, SourceTopology::Singleton);
        assert!(!contract.supports_replay());
        assert!(!contract.requires_checkpointing());
        assert!(!contract.is_exact_delivery_certified());
    }

    #[test]
    fn commit_coupled_sources_are_replayable_and_require_checkpoints() {
        let contract = SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::NodeLocalIngress,
        );
        assert!(contract.supports_replay());
        assert!(contract.requires_checkpointing());
    }

    #[test]
    fn sink_contract_defaults_fail_closed() {
        let contract = SinkContract::default();
        assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
        assert_eq!(contract.topology, SinkTopology::Singleton);
        assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
        assert!(!contract.input_mode.accepts_full_changelog());
    }

    #[test]
    fn coordinated_namespace_is_bounded_stable_and_sink_scoped() {
        use laminar_core::storage::checkpoint_manifest::PipelineIdentity;
        const DEPLOYMENT: &str = "018f0000-0000-7000-8000-000000000001";

        let first =
            CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "orders")
                .unwrap();
        let same =
            CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "orders")
                .unwrap();
        let other =
            CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "audit")
                .unwrap();
        let other_deployment = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000002",
            "orders",
        )
        .unwrap();

        assert_eq!(first.external_key(), same.external_key());
        assert_ne!(first.external_key(), other.external_key());
        assert_ne!(first.external_key(), other_deployment.external_key());
        assert_eq!(first.external_key().len(), "ldb-c3-".len() + 64);
        assert!(first
            .external_key()
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'));
    }

    #[test]
    fn coordinated_namespace_rejects_ambiguous_identity() {
        const DEPLOYMENT: &str = "018f0000-0000-7000-8000-000000000001";

        use laminar_core::storage::checkpoint_manifest::{
            PipelineIdentity, PIPELINE_IDENTITY_VERSION,
        };
        let malformed = PipelineIdentity {
            canonical_version: PIPELINE_IDENTITY_VERSION,
            sha256: "NOT-A-DIGEST".into(),
        };
        assert!(CoordinatedCommitNamespace::try_new(malformed, DEPLOYMENT, "orders").is_err());
        assert!(
            CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "").is_err()
        );
        assert!(CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "not-a-uuid",
            "orders"
        )
        .is_err());
    }

    #[test]
    fn coordinated_batch_fingerprint_covers_the_exact_ordered_cut() {
        use laminar_core::state::CheckpointAttempt;
        use laminar_core::storage::checkpoint_manifest::PipelineIdentity;

        let namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap();
        let attempt = CheckpointAttempt::new(8, 108);
        let batch = CoordinatedCommitBatch {
            namespace,
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 107,
                fencing_token: 3,
            },
            fencing_token: 4,
            target: attempt,
            entries: vec![CoordinatedCommitPayload {
                attempt,
                participant_id: 7,
                payload: None,
            }],
        };
        let expected = batch.exact_fingerprint();
        assert_eq!(expected, batch.clone().exact_fingerprint());

        let mut variants = Vec::new();
        let mut variant = batch.clone();
        variant.namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "audit",
        )
        .unwrap();
        variants.push(variant);
        let mut variant = batch.clone();
        variant.expected_predecessor.checkpoint_id -= 1;
        variants.push(variant);
        let mut variant = batch.clone();
        variant.fencing_token += 1;
        variants.push(variant);
        let mut variant = batch.clone();
        variant.target.epoch += 1;
        variants.push(variant);
        let mut variant = batch.clone();
        variant.entries[0].attempt.checkpoint_id += 1;
        variants.push(variant);
        let mut variant = batch.clone();
        variant.entries[0].participant_id += 1;
        variants.push(variant);
        let mut variant = batch;
        variant.entries[0].payload = Some(Vec::new());
        variants.push(variant);

        assert!(variants
            .into_iter()
            .all(|variant| variant.exact_fingerprint() != expected));
    }

    #[test]
    fn coordinated_batch_rejects_cursor_rollback_and_unproven_overlap() {
        use laminar_core::state::CheckpointAttempt;
        use laminar_core::storage::checkpoint_manifest::PipelineIdentity;

        let first = CheckpointAttempt::new(8, 108);
        let target = CheckpointAttempt::new(10, 110);
        let batch = CoordinatedCommitBatch {
            namespace: CoordinatedCommitNamespace::try_new(
                PipelineIdentity::empty(),
                "018f0000-0000-7000-8000-000000000001",
                "orders",
            )
            .unwrap(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 107,
                fencing_token: 3,
            },
            fencing_token: 4,
            target,
            entries: vec![
                CoordinatedCommitPayload {
                    attempt: first,
                    participant_id: 0,
                    payload: None,
                },
                CoordinatedCommitPayload {
                    attempt: target,
                    participant_id: 0,
                    payload: None,
                },
            ],
        };

        let cursor = |checkpoint_id, fencing_token| {
            Some(CoordinatedCommitCursor {
                checkpoint_id,
                fencing_token,
            })
        };
        assert!(batch.validate_observed_cursor(cursor(106, 3)).is_err());
        assert!(batch.validate_observed_cursor(cursor(109, 3)).is_err());
        assert!(batch.validate_observed_cursor(cursor(107, 2)).is_err());
        assert!(batch.validate_observed_cursor(cursor(107, 3)).is_ok());
        assert!(batch.validate_observed_cursor(cursor(108, 3)).is_ok());
        assert!(batch.validate_observed_cursor(cursor(110, 4)).is_ok());
        assert!(batch.validate_observed_cursor(cursor(110, 3)).is_err());
        assert!(batch.validate_observed_cursor(cursor(108, 5)).is_err());
    }

    #[test]
    fn coordinated_batch_requires_unique_canonical_attempt_participants() {
        use laminar_core::state::CheckpointAttempt;
        use laminar_core::storage::checkpoint_manifest::PipelineIdentity;

        let namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap();
        let target = CheckpointAttempt::new(2, 102);
        let batch = |entries| CoordinatedCommitBatch {
            namespace: namespace.clone(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 100,
                fencing_token: 1,
            },
            fencing_token: 2,
            target,
            entries,
        };
        let payload = |attempt, participant_id| CoordinatedCommitPayload {
            attempt,
            participant_id,
            payload: None,
        };

        let duplicate = batch(vec![payload(target, 0), payload(target, 0)]);
        assert!(duplicate
            .validate_shape()
            .unwrap_err()
            .contains("duplicate"));

        let out_of_order = batch(vec![payload(target, 1), payload(target, 0)]);
        assert!(out_of_order
            .validate_shape()
            .unwrap_err()
            .contains("out-of-order"));

        let conflicting = batch(vec![
            payload(CheckpointAttempt::new(3, 101), 0),
            payload(target, 0),
        ]);
        assert!(conflicting
            .validate_shape()
            .unwrap_err()
            .contains("coherent"));
    }

    #[test]
    fn coordinated_batch_entry_limit_accepts_max_and_rejects_max_plus_one() {
        use laminar_core::state::CheckpointAttempt;
        use laminar_core::storage::checkpoint_manifest::PipelineIdentity;

        let namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap();
        let target = CheckpointAttempt::new(1, 101);
        let make_batch = |count: usize| CoordinatedCommitBatch {
            namespace: namespace.clone(),
            expected_predecessor: CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            },
            fencing_token: 1,
            target,
            entries: (0..count)
                .map(|participant_id| CoordinatedCommitPayload {
                    attempt: target,
                    participant_id: participant_id as u64,
                    payload: None,
                })
                .collect(),
        };

        assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES - 1)
            .validate_shape()
            .is_ok());
        assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES)
            .validate_shape()
            .is_ok());
        assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES + 1)
            .validate_shape()
            .is_err());
    }

    struct DefaultPreCommitSink {
        coordinated: bool,
    }

    #[async_trait]
    impl SinkConnector for DefaultPreCommitSink {
        async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(0, 0))
        }
        fn schema(&self) -> SchemaRef {
            test_schema()
        }
        fn suggested_write_timeout(&self) -> std::time::Duration {
            std::time::Duration::from_secs(5)
        }
        fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
            self.coordinated
                .then_some(self as &dyn CoordinatedCommitter)
        }
        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[async_trait]
    impl CoordinatedCommitter for DefaultPreCommitSink {
        async fn commit_aggregated(
            &self,
            _batch: CoordinatedCommitBatch,
            _context: CoordinatedCommitContext,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn committed_cursor(
            &self,
            _namespace: &CoordinatedCommitNamespace,
        ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn default_pre_commit_rejects_coordinated_sink() {
        let mut sink = DefaultPreCommitSink { coordinated: true };
        assert!(matches!(
            sink.pre_commit(1).await,
            Err(ConnectorError::ConfigurationError(_))
        ));
    }

    #[tokio::test]
    async fn default_pre_commit_ok_for_non_coordinated_sink() {
        let mut sink = DefaultPreCommitSink { coordinated: false };
        assert!(matches!(sink.pre_commit(1).await, Ok(None)));
    }
}
