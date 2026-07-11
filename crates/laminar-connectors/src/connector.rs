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
}

impl SourceContract {
    /// Construct a source contract from its two explicit dimensions.
    #[must_use]
    pub const fn new(consistency: SourceConsistency, topology: SourceTopology) -> Self {
        Self {
            consistency,
            topology,
        }
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

/// SSL connection mode for `PostgreSQL`-compatible connectors.
///
/// Shared by the `PostgreSQL` sink and `PostgreSQL` CDC source. Variant names
/// follow the `libpq` `sslmode` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PostgresSslMode {
    /// No SSL.
    Disable,
    /// Try SSL, fall back to unencrypted.
    #[default]
    Prefer,
    /// Require SSL.
    Require,
    /// Require SSL and verify CA certificate.
    VerifyCa,
    /// Require SSL, verify certificate and hostname.
    VerifyFull,
}

impl std::fmt::Display for PostgresSslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disable => write!(f, "disable"),
            Self::Prefer => write!(f, "prefer"),
            Self::Require => write!(f, "require"),
            Self::VerifyCa => write!(f, "verify-ca"),
            Self::VerifyFull => write!(f, "verify-full"),
        }
    }
}

impl FromStr for PostgresSslMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "disable" | "off" => Ok(Self::Disable),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify_ca" | "verifyca" => Ok(Self::VerifyCa),
            "verify_full" | "verifyfull" => Ok(Self::VerifyFull),
            other => Err(format!("unknown SSL mode: '{other}'")),
        }
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
    /// Opens the source and establishes its initial or resumed position as one
    /// indivisible lifecycle transition.
    ///
    /// Implementations must not emit records or expose an externally active
    /// consumer before the requested position has been applied successfully.
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError>;

    /// `Ok(None)` = no data currently available; runtime retries after a delay.
    /// A returned batch must contain at most `max_records` rows.
    ///
    /// The runtime may cancel this future to service shutdown or control-plane
    /// work (including an epoch-commit notification in runtimes that multiplex
    /// it with polling). Implementations must not advance external or
    /// checkpoint-visible position across an `.await` unless dropping the
    /// future at that point preserves replay of every not-yet-returned record.
    /// Stage work privately, then advance the cursor and return without another
    /// cancellation point.
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

    /// Install the cluster vnode assignment so a partitioned source can bind
    /// its input partitions to vnodes (`partition % vnode_count`) and consume
    /// only those it owns, re-binding when the assignment rotates. Called by
    /// the cluster startup wiring before [`start`](Self::start).
    ///
    /// Default: no-op — single-node deployments and sources without a natural
    /// partitioning ignore it. Only the Kafka source overrides it today.
    fn set_vnode_assignment(
        &mut self,
        _registry: Arc<laminar_core::state::VnodeRegistry>,
        _self_id: laminar_core::state::NodeId,
    ) {
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

    /// Returns this connector as a [`SchemaProvider`](crate::schema::SchemaProvider), if supported.
    fn as_schema_provider(&self) -> Option<&dyn crate::schema::SchemaProvider> {
        None
    }

    /// Returns this connector as a [`SchemaRegistryAware`](crate::schema::SchemaRegistryAware), if supported.
    fn as_schema_registry_aware(&self) -> Option<&dyn crate::schema::SchemaRegistryAware> {
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

    /// Returns a shared flag that the source sets to `true` when it
    /// requests an immediate checkpoint.
    ///
    /// This is used by sources that detect external state changes requiring
    /// a checkpoint before proceeding — for example, Kafka consumer group
    /// rebalance (partition revocation). The pipeline coordinator polls
    /// this flag each cycle and clears it after triggering the checkpoint.
    ///
    /// The default returns `None` (no source-initiated checkpoints).
    fn checkpoint_requested(&self) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        None
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

    /// Must be cancellation-safe: the runtime wraps this in
    /// `tokio::time::timeout`. Don't split a `&mut self` mutation
    /// across an `.await`. In-flight transactional state may remain open
    /// after cancellation; for checkpoint-committable sinks the caller will
    /// `rollback_epoch` it after proving no durable commit decision exists.
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

    /// Must be internally bounded — the sink task's periodic timer
    /// calls this on every tick. Thorough drains belong in `pre_commit`
    /// / coordinated commit / `close`, not here.
    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// Close the sink and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;

    /// Return a [`SchemaRegistryAware`](crate::schema::SchemaRegistryAware)
    /// view, if the sink speaks a schema registry protocol.
    fn as_schema_registry_aware(&self) -> Option<&dyn crate::schema::SchemaRegistryAware> {
        None
    }

    /// Leader-side committer for a checkpoint-committable contract; `None`
    /// for every weaker contract.
    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
        None
    }
}

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
        format!("ldb-c2-{digest:x}")
    }
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
    /// Exact external cursor that must precede this batch. A freshly observed
    /// cursor below this value proves target rollback and must fail closed.
    pub expected_predecessor: u64,
    /// Highest exact attempt atomically covered by this commit.
    pub target: laminar_core::state::CheckpointAttempt,
    /// Every sealed participant marker through `target`, including empty ones.
    pub entries: Vec<CoordinatedCommitPayload>,
}

impl CoordinatedCommitBatch {
    /// Validate a cursor freshly read from the external target against this
    /// exact batch. Advancing overlap is safe only at an attempt named by the
    /// batch; rollback or an unproven gap would skip output.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when the batch is malformed, the observed cursor
    /// proves rollback, or an overlap cannot be tied to an exact batch entry.
    pub fn validate_observed_cursor(&self, observed: u64) -> Result<(), String> {
        if self.expected_predecessor >= self.target.checkpoint_id {
            return Err(format!(
                "invalid coordinated batch predecessor {} for target {}",
                self.expected_predecessor, self.target.checkpoint_id
            ));
        }
        if self.entries.is_empty()
            || !self
                .entries
                .iter()
                .any(|entry| entry.attempt == self.target)
            || self.entries.iter().any(|entry| {
                entry.attempt.checkpoint_id <= self.expected_predecessor
                    || entry.attempt.checkpoint_id > self.target.checkpoint_id
            })
        {
            return Err(
                "coordinated batch entries do not cover the predecessor-to-target interval".into(),
            );
        }
        if observed < self.expected_predecessor {
            return Err(format!(
                "external cursor rolled back from expected predecessor {} to {observed}",
                self.expected_predecessor
            ));
        }
        if observed < self.target.checkpoint_id
            && observed != self.expected_predecessor
            && !self
                .entries
                .iter()
                .any(|entry| entry.attempt.checkpoint_id == observed)
        {
            return Err(format!(
                "external cursor {observed} is not an exact attempt in batch {}..={}",
                self.expected_predecessor, self.target.checkpoint_id
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
    async fn commit_aggregated(&self, batch: CoordinatedCommitBatch) -> Result<(), ConnectorError>;

    /// Highest globally unique checkpoint id already committed in `namespace`.
    /// A metadata read error must be returned, never converted to an absent
    /// cursor, because that could duplicate a previously committed batch.
    async fn committed_checkpoint_id(
        &self,
        namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<u64>, ConnectorError>;
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
    fn source_contract_defaults_fail_closed() {
        let contract = SourceContract::default();
        assert_eq!(contract.consistency, SourceConsistency::Ephemeral);
        assert_eq!(contract.topology, SourceTopology::Singleton);
        assert!(!contract.supports_replay());
        assert!(!contract.requires_checkpointing());
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
        assert_eq!(first.external_key().len(), "ldb-c2-".len() + 64);
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
            expected_predecessor: 107,
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

        assert!(batch.validate_observed_cursor(106).is_err());
        assert!(batch.validate_observed_cursor(109).is_err());
        assert!(batch.validate_observed_cursor(107).is_ok());
        assert!(batch.validate_observed_cursor(108).is_ok());
        assert!(batch.validate_observed_cursor(110).is_ok());
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
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn committed_checkpoint_id(
            &self,
            _namespace: &CoordinatedCommitNamespace,
        ) -> Result<Option<u64>, ConnectorError> {
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
