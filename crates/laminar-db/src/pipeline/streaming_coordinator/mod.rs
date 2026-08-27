//! Single-task pipeline coordinator on the dedicated `laminar-compute` thread.
//!
//! ```text
//! Source task (main runtime) ──MAsyncTx──► StreamingCoordinator
//!                                               │  execute_cycle / write_to_sinks
//!                                               ▼  Sinks
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use crossfire::{mpsc, AsyncRx, MAsyncTx};
use laminar_connectors::checkpoint::SourceCheckpoint;
#[cfg(test)]
use laminar_connectors::checkpoint::SourceCheckpointDelta;
use laminar_connectors::connector::{
    schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
    strip_source_row_positions, ConnectorCancellationPolicy, ConnectorTaskTracker,
    DeliveryGuarantee, SourceBatch, SourceBatchCursor, SourceConnector, SourceConsistency,
    SourceContract, SourceInputMode, SourcePosition, SourceRowPositionCapability, SourceStart,
    SOURCE_MUTATION_COLUMN,
};
#[cfg(feature = "cluster")]
use laminar_connectors::connector::{
    SourceDrainOutcome, SourceDrainRequest, SourceDrainResolution,
};
use laminar_connectors::error::ConnectorError;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::{
    AssignmentDrainId, AssignmentDrainTransition, CheckpointParticipant,
};
use laminar_core::checkpoint::{CheckpointAttempt, CheckpointAttemptRelation};
use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;
use rustc_hash::{FxHashMap, FxHashSet};

use super::callback::{
    BarrierOutcome, CheckpointAssignmentAdmission, CheckpointCompletion, CheckpointControlOutcome,
    CheckpointControlWake, CycleError, CycleOutcome, PipelineCallback, SourceBarrierControl,
    SourceBarrierSignal, SourceRegistration,
};
#[cfg(test)]
use super::config::CheckpointSchedule;
use super::config::PipelineConfig;
use crate::catalog::{schema_has_reserved_mutation_columns, validate_source_batch};
use crate::connector_task_fence::{ConnectorTaskFenceRegistration, OwnedConnectorTaskFences};
use crate::error::DbError;

type SourceMsgRx = AsyncRx<mpsc::Array<SourceMsg>>;
type SourceMsgTx = MAsyncTx<mpsc::Array<SourceMsg>>;
type ControlMsgRx = AsyncRx<mpsc::Array<super::ControlMsg>>;
type ForceCheckpointRequest = crate::db::ForceCheckpointRequest;

#[derive(Clone, Copy, Debug)]
struct CyclePublicationDurations {
    output_store_ns: u64,
    sink_enqueue_ns: u64,
}

mod barrier_handling;
mod checkpoint_admission;
mod checkpoint_lifecycle;
mod construction;
mod cycle;
mod drain_ownership;
mod execution;
mod intake_backpressure;
mod shutdown;
mod source_actor;
mod source_drain;
mod source_lifecycle;
mod source_runtime;

use source_drain::{
    acknowledge_latest_source_commit, take_assignment_bound_batch_cursor, try_source_checkpoint,
};
#[cfg(feature = "cluster")]
use source_drain::{
    apply_latest_source_drain_command_fenced, publish_source_drain_ready_fenced,
    resolve_pending_source_drain_fenced, source_drain_flushing, source_drain_held,
};
#[cfg(feature = "cluster")]
use source_lifecycle::{
    check_source_sync_fence, source_operation_authority_error, wait_source_drain_hold,
};
use source_lifecycle::{
    poll_source_once, run_source_operation, source_operation_deadline_error, start_source_once,
    wait_source_barrier_release, wait_source_idle, SourceConnectorLifecycle,
    SourceOperationOutcome, SourcePollOutcome, SourceStartFailure, SourceStartOutcome,
};
use source_runtime::{
    send_source_msg, spawn_source_actor, wait_coordinator_delay, SourceFault, SourceMsg,
    SourceTaskExitGuard, StreamingCoordinatorGeneration,
};
#[cfg(feature = "cluster")]
use source_runtime::{
    source_process_authority_is_live, validate_source_drain_receipts, ActiveSourceDrain,
    PendingSourceDrainResolution, SourceDrainCommand, SourceDrainCommandPolicy,
    SourceDrainLeaseControl, SourceDrainReceipt, SourceDrainTaskStatus, SourceProcessAuthority,
};

#[cfg(all(test, feature = "cluster"))]
pub(crate) use drain_ownership::install_replacement_source_drain_task_for_test;
#[cfg(feature = "cluster")]
pub(crate) use drain_ownership::{
    owned_source_drain_resolved, prepare_owned_source_drain, resolve_owned_source_drain,
};
pub use source_runtime::StreamingCoordinatorRuntime;
pub(crate) use source_runtime::{OwnedSourceTasks, SourceTaskLease};

struct SourceHandle {
    /// Whether this source's checkpoint is a durable recovery cursor. Ephemeral source
    /// checkpoints may align a barrier but must never enter a manifest or cluster handoff.
    recovery_cursor: bool,
    task: SourceTaskLease,
    /// One-shot startup fence. Source I/O cannot begin until the compute loop has installed its
    /// control plane and published the runtime-ready boundary.
    startup_activation: Option<crossfire::oneshot::TxOneshot<()>>,
    barrier_injector: CheckpointBarrierInjector,
    /// Retained exact release/stop command for a source held after barrier emission.
    barrier_release_tx: tokio::sync::watch::Sender<Option<SourceBarrierSignal>>,
    /// Notifies the source of a committed `(epoch, checkpoint)` so it can ack upstream.
    /// The checkpoint is what was written to the manifest (may lag). Empty only when the epoch
    /// captured no state for this source; an empty one is a no-op for upstream advancement.
    epoch_committed_tx: tokio::sync::watch::Sender<Option<(u64, SourceCheckpoint)>>,
}

pub(crate) struct TrackedSourceRegistration {
    source: SourceRegistration,
    contract: SourceContract,
    expected_schema: arrow_schema::SchemaRef,
    positioned_schema: arrow_schema::SchemaRef,
    mutation_schema: arrow_schema::SchemaRef,
    primary_key: Vec<String>,
    primary_key_indices: Vec<usize>,
    schema_admitted: bool,
    admitted_non_append_mode: Option<SourceInputMode>,
    task_fence: ConnectorTaskFenceRegistration,
}

pub(crate) const MUTATION_SOURCE_NOT_ADMITTED: &str =
    "[LDB-5039] mutation sources require an exclusively admitted stateful operator route";

pub(crate) fn admit_append_only_source(
    contract: SourceContract,
    has_reserved_mutation_columns: bool,
) -> Result<(), &'static str> {
    if contract.input_mode == SourceInputMode::AppendOnly && !has_reserved_mutation_columns {
        Ok(())
    } else {
        Err(MUTATION_SOURCE_NOT_ADMITTED)
    }
}

impl TrackedSourceRegistration {
    fn metadata_schemas(
        source_name: &str,
        contract: SourceContract,
        expected_schema: &arrow_schema::SchemaRef,
    ) -> Result<(arrow_schema::SchemaRef, arrow_schema::SchemaRef), DbError> {
        let map_error = |error| {
            DbError::Config(format!(
                "source '{source_name}' has an invalid source-metadata schema: {error}"
            ))
        };
        let positioned = schema_with_source_row_positions(expected_schema).map_err(map_error)?;
        let mutations =
            schema_with_source_mutations_and_row_positions(expected_schema).map_err(map_error)?;
        if contract.row_positions == SourceRowPositionCapability::OrderedDeterministic {
            Ok((positioned, mutations))
        } else {
            Ok((Arc::clone(expected_schema), Arc::clone(expected_schema)))
        }
    }

    fn resolve_contract(source: &SourceRegistration) -> Result<SourceContract, DbError> {
        let contract = source.connector.contract(&source.config).map_err(|error| {
            DbError::Config(format!(
                "source '{}' (type '{}') has an invalid contract: {error}",
                source.name,
                source.config.connector_type()
            ))
        })?;
        Ok(contract)
    }

    pub(crate) fn capture(
        source: SourceRegistration,
        owned: &OwnedConnectorTaskFences,
    ) -> Result<Self, DbError> {
        let contract = Self::resolve_contract(&source)?;
        let expected_schema = source.connector.schema();
        let (positioned_schema, mutation_schema) =
            Self::metadata_schemas(&source.name, contract, &expected_schema)?;
        let task_fence = ConnectorTaskFenceRegistration::capture_registered(
            Arc::<str>::from(format!("source:{}", source.name)),
            source.connector.terminal_task_tracker(),
            owned,
        );
        Ok(Self {
            source,
            contract,
            expected_schema,
            positioned_schema,
            mutation_schema,
            primary_key: Vec::new(),
            primary_key_indices: Vec::new(),
            schema_admitted: false,
            admitted_non_append_mode: None,
            task_fence,
        })
    }

    pub(crate) fn from_captured(
        source: SourceRegistration,
        task_fence: ConnectorTaskFenceRegistration,
    ) -> Result<Self, DbError> {
        let contract = Self::resolve_contract(&source)?;
        let expected_schema = source.connector.schema();
        let (positioned_schema, mutation_schema) =
            Self::metadata_schemas(&source.name, contract, &expected_schema)?;
        Ok(Self {
            source,
            contract,
            expected_schema,
            positioned_schema,
            mutation_schema,
            primary_key: Vec::new(),
            primary_key_indices: Vec::new(),
            schema_admitted: false,
            admitted_non_append_mode: None,
            task_fence,
        })
    }

    pub(crate) fn with_admitted_schema(
        mut self,
        expected_schema: arrow_schema::SchemaRef,
        primary_key: Vec<String>,
    ) -> Result<Self, DbError> {
        let primary_key_indices = primary_key
            .iter()
            .map(|column| {
                expected_schema.index_of(column).map_err(|_| {
                    DbError::Config(format!(
                        "source '{}' primary-key column '{column}' is absent from its admitted schema",
                        self.name
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.expected_schema = expected_schema;
        (self.positioned_schema, self.mutation_schema) =
            Self::metadata_schemas(&self.name, self.contract, &self.expected_schema)?;
        self.primary_key = primary_key;
        self.primary_key_indices = primary_key_indices;
        self.schema_admitted = true;
        Ok(self)
    }

    pub(crate) const fn contract(&self) -> SourceContract {
        self.contract
    }

    pub(crate) fn with_temporal_right_mutations(mut self) -> Self {
        debug_assert_eq!(self.contract.input_mode, SourceInputMode::KeyedUpsert);
        self.admitted_non_append_mode = Some(SourceInputMode::KeyedUpsert);
        self
    }

    pub(crate) fn with_ordered_interval_input_mode(
        mut self,
        mode: SourceInputMode,
    ) -> Result<Self, DbError> {
        if mode == SourceInputMode::AppendOnly || self.contract.input_mode != mode {
            return Err(DbError::Config(format!(
                "source '{}' lost its admitted bounded-interval input mode",
                self.name
            )));
        }
        if self
            .admitted_non_append_mode
            .is_some_and(|admitted| admitted != mode)
        {
            return Err(DbError::Config(format!(
                "source '{}' has conflicting stateful mutation routes",
                self.name
            )));
        }
        self.admitted_non_append_mode = Some(mode);
        Ok(self)
    }

    fn has_reserved_mutation_columns(&self) -> bool {
        schema_has_reserved_mutation_columns(self.expected_schema.as_ref())
    }
}

fn prepare_encoded_source_batch(
    source_name: &str,
    expected_schema: &arrow_schema::SchemaRef,
    positioned_schema: &arrow_schema::SchemaRef,
    mutation_schema: &arrow_schema::SchemaRef,
    primary_key: &[String],
    primary_key_indices: &[usize],
    capability: SourceRowPositionCapability,
    batch: SourceBatch,
) -> Result<RecordBatch, laminar_core::streaming::StreamingError> {
    validate_source_batch(
        source_name,
        expected_schema,
        primary_key,
        primary_key_indices,
        &batch.records,
    )?;
    batch
        .into_records_with_metadata(capability, positioned_schema, mutation_schema)
        .map_err(|error| {
            laminar_core::streaming::StreamingError::InvalidConfig(format!(
                "source '{source_name}' emitted invalid source metadata: {error}"
            ))
        })
}

impl std::ops::Deref for TrackedSourceRegistration {
    type Target = SourceRegistration;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

impl std::ops::DerefMut for TrackedSourceRegistration {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.source
    }
}

struct PreparedSourceGeneration {
    registration: TrackedSourceRegistration,
}

impl SourceHandle {
    fn barrier_control(&self) -> SourceBarrierControl {
        SourceBarrierControl::new(
            self.barrier_injector.clone(),
            self.barrier_release_tx.clone(),
        )
    }
}

/// Why [`StreamingCoordinator::run`] returned.
#[derive(Debug)]
pub enum ExitReason {
    /// Coordinator shutdown was explicitly signaled — a clean stop.
    Shutdown,
    /// Deterministic runtime error that recovery cannot repair. The lifecycle publishes a
    /// terminal faulted state but must not restart this deployment automatically.
    Halt(String),
    /// Fatal runtime error; the lifecycle restarts or coordinates recovery as configured.
    Fault(String),
}

struct PendingWatermarkBatch {
    source_name: Arc<str>,
    batch: RecordBatch,
    admission_floor: i64,
    input_channels: Option<Arc<[Vec<u8>]>>,
}

/// Single-task pipeline coordinator — no core threads.
pub struct StreamingCoordinator {
    config: PipelineConfig,
    rx: SourceMsgRx,
    source_fault_rx: tokio::sync::mpsc::UnboundedReceiver<SourceFault>,
    source_handles: Vec<SourceHandle>,
    source_names: Vec<Arc<str>>,
    source_input_modes: Vec<Option<SourceInputMode>>,
    shutdown: Arc<tokio::sync::Notify>,
    terminal_shutdown: tokio_util::sync::CancellationToken,
    pending_barrier: PendingBarrier,
    last_checkpoint: Instant,
    checkpoint_retry_not_before: Option<Instant>,
    checkpoint_retry_backoff: Duration,
    source_batches_buf: FxHashMap<Arc<str>, Vec<RecordBatch>>,
    /// At most one FIFO message removed just as the external intake gate closes. Exact source
    /// barrier holds make post-barrier data impossible; this slot exists only for that gate race.
    parked_source_msg: Option<SourceMsg>,
    pending_watermark_batches: Vec<PendingWatermarkBatch>,
    /// Sources that delivered a barrier this drain cycle. A later batch from one of these sources
    /// violates the source hold protocol and faults the pipeline.
    barrier_seen: FxHashSet<usize>,
    /// Per-source offset advanced from `pending_offsets` after successful cycle publication.
    committed_offsets: Vec<Option<SourceCheckpoint>>,
    /// Cursors staged by `process_msg`; a replay-preserving deferral retains them until the graph
    /// consumes its buffered work, while a fault discards them.
    pending_offsets: Vec<Option<SourceBatchCursor>>,
    /// The previous cycle retained graph work. Its retry is scheduled ahead of source intake so a
    /// newer connector cursor cannot overtake the buffered mutation.
    replay_pending: bool,
    control_rx: ControlMsgRx,
    checkpoint_complete_rx:
        Option<crossfire::AsyncRx<crossfire::mpsc::Array<CheckpointCompletion>>>,
    /// Public checkpoint requests waiting for the next newly-admitted exact attempt.
    force_ckpt_rx: Option<crate::db::ForceCheckpointRx>,
    manual_waiting: Vec<ForceCheckpointRequest>,
    /// A committed intermediate cut retained replay, so these waiters still require HANDOFF.
    manual_handoff_required: bool,
    /// Requests attached at admission. Later requests remain in `manual_waiting`.
    manual_active: Option<ManualCheckpointAttempt>,
    /// Epochs between admission and durable (tails still running); shared with callback.
    checkpoint_in_flight: Arc<AtomicU64>,
    /// Last durable completion published to sources/subscribers in this runtime. This is a
    /// defense-in-depth monotonic fence in addition to serialized tail admission.
    last_published_checkpoint: Option<CheckpointAttempt>,
    #[cfg(feature = "cluster")]
    process_authority: Option<Arc<SourceProcessAuthority>>,
    public_generation: Option<StreamingCoordinatorGeneration>,
}

// These flags describe independent protocol state and cannot be combined without obscuring the
// coordinator's transition invariants.
#[allow(clippy::struct_excessive_bools)]
struct CoordinatorRunState {
    batch_window: Duration,
    checkpoint_control_wake: Option<CheckpointControlWake>,
    shuffle_work_wake: Option<Arc<tokio::sync::Notify>>,
    checkpoint_control_poll_at: tokio::time::Instant,
    checkpoint_control_pending: bool,
    /// An observed strong intake fence restarts only the periodic checkpoint cadence when it
    /// reopens. Manual HANDOFF requests remain immediately eligible.
    intake_was_paused: bool,
    barriers: Vec<(usize, CheckpointBarrier, SourceCheckpoint)>,
    fault: Option<String>,
    halted: bool,
    halt_reason: Option<String>,
    source_channel_expected: bool,
}

impl CoordinatorRunState {
    fn halt(&mut self, reason: impl Into<String>) {
        self.halted = true;
        self.halt_reason.get_or_insert_with(|| reason.into());
        self.fault = None;
    }
}

#[derive(Default)]
struct CoordinatorWake {
    message: Option<SourceMsg>,
    retrying_replay: bool,
    checkpoint_control_due: bool,
    gates: CoordinatorGates,
}

#[derive(Default)]
struct CoordinatorGates {
    intake_paused: bool,
    external_commit_backpressured: bool,
}

enum CoordinatorWaitAction {
    Cycle,
    Continue,
    Stop,
}

struct CoordinatorWait {
    action: CoordinatorWaitAction,
    wake: CoordinatorWake,
}

impl CoordinatorWait {
    fn cycle(wake: CoordinatorWake) -> Self {
        Self {
            action: CoordinatorWaitAction::Cycle,
            wake,
        }
    }

    fn continue_loop() -> Self {
        Self {
            action: CoordinatorWaitAction::Continue,
            wake: CoordinatorWake::default(),
        }
    }

    fn stop() -> Self {
        Self {
            action: CoordinatorWaitAction::Stop,
            wake: CoordinatorWake::default(),
        }
    }
}

impl Drop for StreamingCoordinator {
    fn drop(&mut self) {
        // `run` owns the coordinator by value, so cancellation or unwind would otherwise discard
        // the only per-source shutdown controls. The DB-owned leases remain registered until the
        // actor-exit wrapper and exact connector tracker both prove terminal completion.
        for handle in &self.source_handles {
            handle.task.request_shutdown();
        }
        // Release public construction ownership only after every source has observed shutdown.
        drop(self.public_generation.take());
    }
}

/// Public checkpoint callers attached to one exact attempt at admission time.
struct ManualCheckpointAttempt {
    attempt: CheckpointAttempt,
    flags: u64,
    requests: Vec<ForceCheckpointRequest>,
}

struct CheckpointAdmission {
    manual: bool,
    flags: u64,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    /// Exact assignment/drain serialization claim returned by the callback. Clustered sourced
    /// attempts retain it through Prepare and source-barrier installation; source-less attempts
    /// release it after Prepare so mutable capture may acquire the graph's fair rotation fence.
    assignment_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    deadline: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CheckpointCleanupOwner {
    /// Embedded, single-node, or cluster-leader attempt originator.
    Originator,
    /// Cluster follower that reserved an attempt announced by the originator.
    Follower,
}

struct AlignedCheckpointContext {
    cleanup_owner: CheckpointCleanupOwner,
    attempt: CheckpointAttempt,
    started_at: Instant,
    flags: u64,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
}

/// Tracks in-flight checkpoint barrier alignment.
struct PendingBarrier {
    attempt: Option<CheckpointAttempt>,
    sources_total: usize,
    sources_aligned: FxHashSet<usize>,
    source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    started_at: Instant,
    deadline: Option<tokio::time::Instant>,
    active: bool,
    flags: u64,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    cleanup_owner: CheckpointCleanupOwner,
}

impl PendingBarrier {
    fn new() -> Self {
        Self {
            attempt: None,
            sources_total: 0,
            sources_aligned: FxHashSet::default(),
            source_checkpoints: FxHashMap::default(),
            started_at: Instant::now(),
            deadline: None,
            active: false,
            flags: laminar_core::checkpoint::flags::NONE,
            assignment_fence: None,
            cleanup_owner: CheckpointCleanupOwner::Originator,
        }
    }

    #[cfg(test)]
    fn reset(&mut self, attempt: CheckpointAttempt, sources_total: usize) {
        self.reset_with_assignment(
            attempt,
            sources_total,
            laminar_core::checkpoint::flags::NONE,
            None,
            None,
        );
    }

    fn reset_follower(&mut self, attempt: CheckpointAttempt, sources_total: usize, flags: u64) {
        self.reset_inner(
            attempt,
            sources_total,
            flags,
            None,
            CheckpointCleanupOwner::Follower,
        );
    }

    fn reset_with_assignment(
        &mut self,
        attempt: CheckpointAttempt,
        sources_total: usize,
        flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        deadline: Option<tokio::time::Instant>,
    ) {
        self.reset_inner(
            attempt,
            sources_total,
            flags,
            assignment_fence,
            CheckpointCleanupOwner::Originator,
        );
        self.deadline = deadline;
    }

    fn attempt_deadline(&self, checkpoint_timeout: Duration) -> tokio::time::Instant {
        self.deadline
            .unwrap_or_else(|| tokio::time::Instant::from_std(self.started_at) + checkpoint_timeout)
    }

    fn reset_inner(
        &mut self,
        attempt: CheckpointAttempt,
        sources_total: usize,
        flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        cleanup_owner: CheckpointCleanupOwner,
    ) {
        self.attempt = Some(attempt);
        self.sources_total = sources_total;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.started_at = Instant::now();
        self.deadline = None;
        self.active = true;
        self.flags = flags;
        self.assignment_fence = assignment_fence;
        self.cleanup_owner = cleanup_owner;
    }

    /// Clear alignment state and return the exact active attempt and its cleanup owner.
    fn take_active_attempt(&mut self) -> Option<(CheckpointAttempt, CheckpointCleanupOwner)> {
        if !self.active {
            return None;
        }
        let cleanup_owner = self.cleanup_owner;
        self.active = false;
        self.sources_total = 0;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.flags = laminar_core::checkpoint::flags::NONE;
        self.assignment_fence = None;
        self.deadline = None;
        self.cleanup_owner = CheckpointCleanupOwner::Originator;
        self.attempt.take().map(|attempt| (attempt, cleanup_owner))
    }

    fn clear(&mut self) {
        self.active = false;
        self.attempt = None;
        self.sources_total = 0;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.flags = laminar_core::checkpoint::flags::NONE;
        self.assignment_fence = None;
        self.deadline = None;
        self.cleanup_owner = CheckpointCleanupOwner::Originator;
    }
}

/// Fallback timeout for idle wake.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_millis(100);

/// Internal topology-retry floor and cap. Assignment admission remains the authoritative gate.
const CHECKPOINT_RETRY_BASE: Duration = Duration::from_millis(100);
const CHECKPOINT_RETRY_MAX: Duration = Duration::from_secs(5);

/// Cap on a source task's post-shutdown flush so a hot source can't stall shutdown.
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(2);

/// Cap on awaiting a source task at shutdown before retiring its connector generation.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(8);

/// Shutdown-only poll cadence. It closes the atomic/channel race when a tail drops its in-flight
/// guard without producing another wakeup (for example a cluster follower tail with no
/// completion).
const SHUTDOWN_COMPLETION_TICK: Duration = Duration::from_millis(10);

#[cfg(test)]
mod tests;
