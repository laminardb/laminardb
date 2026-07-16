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
use crossfire::{mpsc, AsyncRx};
use futures::FutureExt;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::SourceBatch;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart,
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
use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
use laminar_core::state::{CheckpointAttempt, CheckpointAttemptRelation};
use rustc_hash::{FxHashMap, FxHashSet};

use super::callback::{
    BarrierOutcome, CheckpointCompletion, CheckpointControlOutcome, CycleError, CycleOutcome,
    PipelineCallback, SourceBarrierControl, SourceBarrierSignal, SourceRegistration,
};
use super::config::PipelineConfig;
use crate::error::DbError;

type SourceMsgRx = AsyncRx<mpsc::Array<SourceMsg>>;
type ControlMsgRx = AsyncRx<mpsc::Array<super::ControlMsg>>;
type ForceCheckpointReply = crate::db::ForceCheckpointReply;

/// Message from a source task to the coordinator; carries the [`SourceCheckpoint`]
/// captured at production time so no offset is checkpointed for unprocessed data.
enum SourceMsg {
    Batch {
        source_idx: usize,
        batch: RecordBatch,
        /// Committed to `committed_offsets` only after a successful `execute_cycle`.
        checkpoint: SourceCheckpoint,
    },
    Barrier {
        source_idx: usize,
        barrier: CheckpointBarrier,
        checkpoint: SourceCheckpoint,
    },
}

/// Fatal source-task control event. Faults use a dedicated unbounded side channel so they cannot
/// sit behind a full data queue or be hidden while external-commit backpressure pauses data.
struct SourceFault {
    source: Arc<str>,
    error: String,
}

/// Reports every source-task exit that was not initiated by coordinator shutdown, including a
/// panic while polling connector code. `UnboundedSender::send` is synchronous and allocation-only,
/// so this remains reliable from `Drop` during unwinding without blocking the data path.
struct SourceTaskExitGuard {
    source: Arc<str>,
    expected_shutdown: Arc<AtomicBool>,
    fault_tx: tokio::sync::mpsc::UnboundedSender<SourceFault>,
}

impl Drop for SourceTaskExitGuard {
    fn drop(&mut self) {
        if !self.expected_shutdown.load(Ordering::Acquire) {
            let _ = self.fault_tx.send(SourceFault {
                source: Arc::clone(&self.source),
                error: "source task exited without coordinator shutdown".into(),
            });
        }
    }
}

#[derive(Clone)]
enum SourceTaskOutcome {
    Success,
    Cancelled,
    Failed(Arc<str>),
}

struct SourceTaskState {
    name: Arc<str>,
    cancellation_policy: ConnectorCancellationPolicy,
    shutdown: Arc<tokio::sync::Notify>,
    expected_shutdown: Arc<AtomicBool>,
    abort: tokio::task::AbortHandle,
    finished: AtomicBool,
    outcome: parking_lot::Mutex<Option<SourceTaskOutcome>>,
    notify: tokio::sync::Notify,
    #[cfg(feature = "cluster")]
    drain: parking_lot::Mutex<Option<SourceDrainLeaseControl>>,
}

#[cfg(feature = "cluster")]
#[derive(Clone)]
enum SourceDrainCommand {
    Begin {
        request: SourceDrainRequest,
        participant: CheckpointParticipant,
        deadline: tokio::time::Instant,
    },
    Resolve {
        resolution: SourceDrainResolution,
        deadline: tokio::time::Instant,
    },
}

#[cfg(feature = "cluster")]
#[derive(Clone, Debug)]
enum SourceDrainTaskStatus {
    Idle,
    Pausing(AssignmentDrainId),
    Ready(SourceDrainReceipt),
    Resolved {
        round: AssignmentDrainId,
        outcome: SourceDrainOutcome,
    },
}

#[cfg(feature = "cluster")]
#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceDrainReceipt {
    round: AssignmentDrainId,
    participant: CheckpointParticipant,
    source_task_incarnation: uuid::Uuid,
}

#[cfg(feature = "cluster")]
impl SourceDrainReceipt {
    fn is_canonical(&self) -> bool {
        self.round.is_canonical()
            && self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && !self.source_task_incarnation.is_nil()
    }
}

#[cfg(feature = "cluster")]
fn validate_source_drain_receipts(
    round: AssignmentDrainId,
    participant: CheckpointParticipant,
    receipts: &[SourceDrainReceipt],
) -> Result<(), String> {
    if receipts.iter().any(|receipt| {
        !receipt.is_canonical() || receipt.round != round || receipt.participant != participant
    }) {
        return Err("source drain contains a stale or non-canonical task receipt".into());
    }
    let mut task_incarnations: Vec<uuid::Uuid> = receipts
        .iter()
        .map(|receipt| receipt.source_task_incarnation)
        .collect();
    task_incarnations.sort_unstable();
    if task_incarnations.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("source drain contains duplicate task receipts".into());
    }
    Ok(())
}

#[cfg(feature = "cluster")]
#[derive(Clone)]
struct SourceDrainLeaseControl {
    task_incarnation: uuid::Uuid,
    command_tx: tokio::sync::watch::Sender<Option<SourceDrainCommand>>,
    status_tx: tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    wake: Arc<tokio::sync::Notify>,
}

#[cfg(feature = "cluster")]
struct ActiveSourceDrain {
    request: SourceDrainRequest,
    participant: CheckpointParticipant,
    provider_drain: bool,
    prepare_deadline: tokio::time::Instant,
    ready: bool,
    pending_resolution: Option<PendingSourceDrainResolution>,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Copy)]
struct PendingSourceDrainResolution {
    resolution: SourceDrainResolution,
    deadline: tokio::time::Instant,
}

/// DB-owned proof that one source generation has reached terminal completion.
///
/// The stable control runtime owns the actual [`tokio::task::JoinHandle`] in a supervisor task.
/// Coordinator cancellation therefore drops only this cloneable lease, never the sole task owner.
#[derive(Clone)]
pub(crate) struct SourceTaskLease {
    state: Arc<SourceTaskState>,
}

pub(crate) type OwnedSourceTasks = Arc<parking_lot::Mutex<Vec<SourceTaskLease>>>;

impl SourceTaskLease {
    pub(crate) fn supervise(
        name: Arc<str>,
        cancellation_policy: ConnectorCancellationPolicy,
        shutdown: Arc<tokio::sync::Notify>,
        expected_shutdown: Arc<AtomicBool>,
        join: tokio::task::JoinHandle<()>,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let state = Arc::new(SourceTaskState {
            name,
            cancellation_policy,
            shutdown,
            expected_shutdown,
            abort: join.abort_handle(),
            finished: AtomicBool::new(false),
            outcome: parking_lot::Mutex::new(None),
            notify: tokio::sync::Notify::new(),
            #[cfg(feature = "cluster")]
            drain: parking_lot::Mutex::new(None),
        });
        let supervisor_state = Arc::clone(&state);
        let supervisor = runtime.spawn(async move {
            let outcome = match join.await {
                Ok(()) => SourceTaskOutcome::Success,
                Err(error) if error.is_cancelled() => SourceTaskOutcome::Cancelled,
                Err(error) => SourceTaskOutcome::Failed(Arc::from(error.to_string())),
            };
            *supervisor_state.outcome.lock() = Some(outcome);
            supervisor_state.finished.store(true, Ordering::Release);
            supervisor_state.notify.notify_waiters();
        });
        // Tokio owns the scheduled supervisor; detaching its handle prevents caller cancellation
        // from ever dropping the sole source JoinHandle held inside the supervisor future.
        drop(supervisor);
        Self { state }
    }

    pub(crate) fn name(&self) -> &str {
        &self.state.name
    }

    #[cfg(feature = "cluster")]
    fn install_drain_control(&self, control: SourceDrainLeaseControl) {
        *self.state.drain.lock() = Some(control);
    }

    #[cfg(feature = "cluster")]
    fn drain_control(&self) -> Option<SourceDrainLeaseControl> {
        self.state.drain.lock().clone()
    }

    pub(crate) fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        self.state.cancellation_policy
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.state.finished.load(Ordering::Acquire)
    }

    pub(crate) fn request_shutdown(&self) {
        self.mark_expected_shutdown();
        self.notify_shutdown();
    }

    fn mark_expected_shutdown(&self) {
        self.state.expected_shutdown.store(true, Ordering::Release);
    }

    fn notify_shutdown(&self) {
        self.state.shutdown.notify_one();
    }

    pub(crate) fn abort_if_cancel_safe(&self) {
        if self.cancellation_policy() == ConnectorCancellationPolicy::CancelSafe {
            self.state.abort.abort();
        }
    }

    pub(crate) async fn wait_until(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.state.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_finished() {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified.as_mut())
                .await
                .is_err()
            {
                return self.is_finished();
            }
        }
    }

    async fn wait(&self) {
        loop {
            let notified = self.state.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_finished() {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn log_terminal_outcome(&self) {
        match self.state.outcome.lock().clone() {
            Some(SourceTaskOutcome::Failed(error)) => {
                tracing::warn!(source = %self.state.name, %error, "source task panicked");
            }
            Some(SourceTaskOutcome::Success | SourceTaskOutcome::Cancelled) | None => {}
        }
    }
}

#[cfg(feature = "cluster")]
async fn await_source_drain_receipt(
    task: &SourceTaskLease,
    control: &SourceDrainLeaseControl,
    round: AssignmentDrainId,
    deadline: tokio::time::Instant,
) -> Result<SourceDrainReceipt, String> {
    let mut status_rx = control.status_tx.subscribe();
    loop {
        match status_rx.borrow_and_update().clone() {
            SourceDrainTaskStatus::Ready(receipt) if receipt.round == round => {
                return Ok(receipt);
            }
            SourceDrainTaskStatus::Ready(receipt) => {
                return Err(format!(
                    "source '{}' retained stale drain receipt {:?} while waiting for {round:?}",
                    task.name(),
                    receipt.round
                ));
            }
            SourceDrainTaskStatus::Pausing(active) if active != round => {
                return Err(format!(
                    "source '{}' is pausing conflicting drain {active:?}",
                    task.name()
                ));
            }
            SourceDrainTaskStatus::Resolved { round: active, .. } if active == round => {
                return Err(format!(
                    "source '{}' resolved drain {round:?} before publishing a receipt",
                    task.name()
                ));
            }
            SourceDrainTaskStatus::Idle
            | SourceDrainTaskStatus::Pausing(_)
            | SourceDrainTaskStatus::Resolved { .. } => {}
        }
        if task.is_finished() {
            return Err(format!(
                "source '{}' exited while preparing drain {round:?}",
                task.name()
            ));
        }
        let task_finished = task.state.notify.notified();
        tokio::pin!(task_finished);
        task_finished.as_mut().enable();
        if task.is_finished() {
            continue;
        }
        let wait = async {
            tokio::select! {
                changed = status_rx.changed() => changed.map_err(|_| "source drain status channel closed"),
                () = task_finished.as_mut() => Ok(()),
            }
        };
        match tokio::time::timeout_at(deadline, wait).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                return Err(format!(
                    "source '{}' did not reach drain {round:?} before its deadline",
                    task.name()
                ));
            }
        }
    }
}

#[cfg(feature = "cluster")]
pub(crate) async fn prepare_owned_source_drain(
    tasks: &OwnedSourceTasks,
    transition: &AssignmentDrainTransition,
    participant: CheckpointParticipant,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    if !transition.is_canonical()
        || transition
            .predecessor
            .participant_incarnation(participant.node_id)
            != Some(participant.boot_incarnation)
    {
        return Err("local source drain participant is absent from predecessor roster".into());
    }
    let request = SourceDrainRequest::new(transition.id()).map_err(|error| error.to_string())?;
    let snapshot: Vec<(SourceTaskLease, SourceDrainLeaseControl)> = tasks
        .lock()
        .iter()
        .map(|task| {
            task.drain_control()
                .map(|control| (task.clone(), control))
                .ok_or_else(|| format!("source '{}' has no cluster drain control", task.name()))
        })
        .collect::<Result<_, _>>()?;
    for (task, control) in &snapshot {
        if task.is_finished() {
            return Err(format!(
                "source '{}' is not live at drain admission",
                task.name()
            ));
        }
        control
            .command_tx
            .send(Some(SourceDrainCommand::Begin {
                request: request.clone(),
                participant,
                deadline,
            }))
            .map_err(|_| format!("source '{}' drain command channel closed", task.name()))?;
        control.wake.notify_one();
    }
    let mut receipts = Vec::with_capacity(snapshot.len());
    for (task, control) in &snapshot {
        let receipt = await_source_drain_receipt(task, control, request.round, deadline).await?;
        if receipt.source_task_incarnation != control.task_incarnation {
            return Err(format!(
                "source '{}' returned a receipt from a replaced task generation",
                task.name()
            ));
        }
        receipts.push(receipt);
    }
    validate_source_drain_receipts(request.round, participant, &receipts)?;

    // Revalidate the exact active source generation immediately before acknowledging the cut.
    let mut expected: Vec<uuid::Uuid> = snapshot
        .iter()
        .map(|(_, control)| control.task_incarnation)
        .collect();
    let mut current: Vec<uuid::Uuid> = tasks
        .lock()
        .iter()
        .map(|task| {
            task.drain_control()
                .map(|control| control.task_incarnation)
                .ok_or_else(|| format!("source '{}' lost cluster drain control", task.name()))
        })
        .collect::<Result<_, _>>()?;
    expected.sort_unstable();
    current.sort_unstable();
    if current != expected {
        return Err("source task generation changed while preparing the drain".into());
    }
    Ok(())
}

#[cfg(feature = "cluster")]
pub(crate) fn owned_source_drain_resolved(
    tasks: &OwnedSourceTasks,
    resolution: SourceDrainResolution,
) -> Result<bool, String> {
    for task in tasks.lock().iter() {
        let control = task
            .drain_control()
            .ok_or_else(|| format!("source '{}' has no cluster drain control", task.name()))?;
        if task.is_finished() {
            if resolution.outcome == SourceDrainOutcome::Abort {
                continue;
            }
            return Ok(false);
        }
        if !matches!(
            control.status_tx.borrow().clone(),
            SourceDrainTaskStatus::Resolved { round, outcome }
                if round == resolution.round && outcome == resolution.outcome
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(feature = "cluster")]
pub(crate) async fn resolve_owned_source_drain(
    tasks: &OwnedSourceTasks,
    resolution: SourceDrainResolution,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let snapshot: Vec<(SourceTaskLease, SourceDrainLeaseControl)> = tasks
        .lock()
        .iter()
        .map(|task| {
            task.drain_control()
                .map(|control| (task.clone(), control))
                .ok_or_else(|| format!("source '{}' has no cluster drain control", task.name()))
        })
        .collect::<Result<_, _>>()?;
    for (task, control) in &snapshot {
        if task.is_finished() {
            if resolution.outcome == SourceDrainOutcome::Abort {
                continue;
            }
            return Err(format!(
                "source '{}' exited before committing drain {:?}",
                task.name(),
                resolution.round
            ));
        }
        let sent = control
            .command_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution,
                deadline,
            }))
            .is_ok();
        if !sent {
            if resolution.outcome == SourceDrainOutcome::Abort {
                continue;
            }
            return Err(format!(
                "source '{}' drain command channel closed",
                task.name()
            ));
        }
        control.wake.notify_one();
    }
    for (task, control) in &snapshot {
        if task.is_finished() && resolution.outcome == SourceDrainOutcome::Abort {
            continue;
        }
        let mut status_rx = control.status_tx.subscribe();
        loop {
            match status_rx.borrow_and_update().clone() {
                SourceDrainTaskStatus::Resolved { round, outcome }
                    if round == resolution.round && outcome == resolution.outcome =>
                {
                    break;
                }
                SourceDrainTaskStatus::Resolved { round, .. } if round != resolution.round => {
                    return Err(format!(
                        "source '{}' resolved stale drain {round:?}",
                        task.name()
                    ));
                }
                _ => {}
            }
            if task.is_finished() {
                if resolution.outcome == SourceDrainOutcome::Abort {
                    break;
                }
                return Err(format!(
                    "source '{}' exited while resolving drain {:?}",
                    task.name(),
                    resolution.round
                ));
            }
            let task_finished = task.state.notify.notified();
            tokio::pin!(task_finished);
            task_finished.as_mut().enable();
            if task.is_finished() {
                continue;
            }
            let wait = async {
                tokio::select! {
                    changed = status_rx.changed() => changed.map_err(|_| "source drain status channel closed"),
                    () = task_finished.as_mut() => Ok(()),
                }
            };
            match tokio::time::timeout_at(deadline, wait).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(format!("source '{}': {error}", task.name()));
                }
                Err(_) => {
                    return Err(format!(
                        "source '{}' did not resolve drain {:?} before its deadline",
                        task.name(),
                        resolution.round
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Handle to a running source I/O task.
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
    /// Fatal runtime error; the lifecycle restarts or coordinates recovery as configured.
    Fault(String),
}

/// Single-task pipeline coordinator — no core threads.
pub struct StreamingCoordinator {
    config: PipelineConfig,
    rx: SourceMsgRx,
    source_fault_rx: tokio::sync::mpsc::UnboundedReceiver<SourceFault>,
    source_handles: Vec<SourceHandle>,
    source_names: Vec<Arc<str>>,
    shutdown: Arc<tokio::sync::Notify>,
    pending_barrier: PendingBarrier,
    last_checkpoint: Instant,
    source_batches_buf: FxHashMap<Arc<str>, Vec<RecordBatch>>,
    /// At most one FIFO message removed just as the external intake gate closes. Exact source
    /// barrier holds make post-barrier data impossible; this slot exists only for that gate race.
    parked_source_msg: Option<SourceMsg>,
    pending_watermark_batches: Vec<(Arc<str>, RecordBatch)>,
    /// Sources that delivered a barrier this drain cycle. A later batch from one of these sources
    /// violates the source hold protocol and faults the pipeline.
    barrier_seen: FxHashSet<usize>,
    /// Per-source offset merged from `pending_offsets` after a successful `execute_cycle`.
    committed_offsets: Vec<Option<SourceCheckpoint>>,
    /// Offsets staged by `process_msg`; a replay-preserving deferral retains them until the graph
    /// consumes its buffered work, while a fault discards them.
    pending_offsets: Vec<Option<SourceCheckpoint>>,
    /// The previous cycle retained graph work. Its retry is scheduled ahead of source intake so a
    /// newer connector cursor cannot overtake the buffered mutation.
    replay_pending: bool,
    control_rx: ControlMsgRx,
    checkpoint_complete_rx:
        Option<crossfire::AsyncRx<crossfire::mpsc::Array<CheckpointCompletion>>>,
    /// Public checkpoint requests waiting for the next newly-admitted exact attempt.
    force_ckpt_rx: Option<crate::db::ForceCheckpointRx>,
    manual_waiting: Vec<ForceCheckpointReply>,
    /// Requests attached at admission. Later requests remain in `manual_waiting`.
    manual_active: Option<ManualCheckpointAttempt>,
    /// Epochs between admission and durable (tails still running); shared with callback.
    checkpoint_in_flight: Arc<AtomicU64>,
    /// Last durable completion published to sources/subscribers in this runtime. This is a
    /// defense-in-depth monotonic fence in addition to serialized tail admission.
    last_published_checkpoint: Option<CheckpointAttempt>,
    /// Captured-state bytes held by in-flight epochs; shared with callback.
    staged_bytes: Arc<AtomicU64>,
    max_staged_bytes: u64,
    /// Shared exact external-commit bound, checked before ID reservation/barrier injection.
    coordinated_commit_admission: Option<crate::checkpoint_coordinator::CoordinatedCommitAdmission>,
}

/// Public checkpoint callers attached to one exact attempt at admission time.
struct ManualCheckpointAttempt {
    attempt: CheckpointAttempt,
    replies: Vec<ForceCheckpointReply>,
}

struct CheckpointAdmission {
    manual: bool,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
}

/// Tracks in-flight checkpoint barrier alignment.
struct PendingBarrier {
    attempt: Option<CheckpointAttempt>,
    sources_total: usize,
    sources_aligned: FxHashSet<usize>,
    source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    started_at: Instant,
    active: bool,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
}

impl PendingBarrier {
    fn new() -> Self {
        Self {
            attempt: None,
            sources_total: 0,
            sources_aligned: FxHashSet::default(),
            source_checkpoints: FxHashMap::default(),
            started_at: Instant::now(),
            active: false,
            assignment_fence: None,
        }
    }

    fn reset(&mut self, attempt: CheckpointAttempt, sources_total: usize) {
        self.reset_with_assignment(attempt, sources_total, None);
    }

    fn reset_with_assignment(
        &mut self,
        attempt: CheckpointAttempt,
        sources_total: usize,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) {
        self.attempt = Some(attempt);
        self.sources_total = sources_total;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.started_at = Instant::now();
        self.active = true;
        self.assignment_fence = assignment_fence;
    }

    /// Clear alignment state and return the exact active attempt, if one existed.
    fn take_active_attempt(&mut self) -> Option<CheckpointAttempt> {
        if !self.active {
            return None;
        }
        self.active = false;
        self.sources_total = 0;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.assignment_fence = None;
        self.attempt.take()
    }

    fn clear(&mut self) {
        self.active = false;
        self.attempt = None;
        self.sources_total = 0;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.assignment_fence = None;
    }
}

/// Fallback timeout for idle wake.
const IDLE_TIMEOUT: Duration = Duration::from_millis(100);

/// Cap on a source task's post-shutdown flush so a hot source can't stall shutdown.
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(2);

/// Cap on awaiting a source task at shutdown before aborting it. This covers the drain budget plus
/// cancellation-unsafe drivers' bounded close path, so the outer task does not pre-empt the
/// connector's own hard-stop/error boundary.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(8);

/// Shutdown-only poll cadence. It closes the atomic/channel race when a tail drops its in-flight
/// guard without producing another wakeup (for example an aborted cluster follower tail).
const SHUTDOWN_COMPLETION_TICK: Duration = Duration::from_millis(10);

/// Grace period for already-captured asynchronous checkpoint tails. On expiry their tracked
/// tasks are cancelled before sources or sinks are torn down; exact attempt namespaces leave any
/// ambiguous remote write safe for recovery.
const SHUTDOWN_CHECKPOINT_TAIL_TIMEOUT: Duration = Duration::from_secs(8);

/// Graceful stop gives already-sealed checkpoints one bounded opportunity to
/// reach coordinated external sinks. Timeout leaves durable markers for replay.
const COORDINATED_COMMIT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// Throttled WARN while barrier admission is paused at the staged-state cap; this
/// runs every coordinator tick, so an unthrottled warn would spam under a backlog.
fn warn_staged_cap_throttled(staged_bytes: u64, cap: u64) {
    static THROTTLE: crate::log_throttle::LogThrottle =
        crate::log_throttle::LogThrottle::every(Duration::from_secs(10));
    if THROTTLE.allow() {
        tracing::warn!(
            staged_bytes,
            cap,
            "checkpoint admission paused: staged-state cap reached"
        );
    }
}

fn warn_external_commit_cap_throttled(known: bool, pending: u64, cap: u64) {
    static THROTTLE: crate::log_throttle::LogThrottle =
        crate::log_throttle::LogThrottle::every(Duration::from_secs(10));
    if THROTTLE.allow() {
        tracing::warn!(
            lag_known = known,
            pending_external_checkpoints = pending,
            cap,
            "checkpoint admission paused at coordinated external-commit bound"
        );
    }
}

fn try_source_checkpoint(
    connector: &dyn SourceConnector,
    assignment_scoped: bool,
) -> Result<Option<SourceCheckpoint>, ConnectorError> {
    let checkpoint = connector.try_checkpoint()?;
    let Some(captured) = checkpoint.as_ref() else {
        return Ok(None);
    };
    match (assignment_scoped, captured.assignment_version()) {
        (true, None) => Err(ConnectorError::Internal(
            "cluster-assigned source checkpoint is missing its assignment version".into(),
        )),
        (false, Some(version)) => Err(ConnectorError::Internal(format!(
            "local source checkpoint unexpectedly carries cluster assignment version {version}"
        ))),
        _ => Ok(checkpoint),
    }
}

/// Apply the newest durable commit notification while no source poll borrows
/// the connector. Non-best-effort pipelines fault if upstream acknowledgement
/// fails because silently continuing can exhaust broker retention/headroom.
async fn acknowledge_latest_source_commit(
    connector: &mut dyn SourceConnector,
    epoch_committed_rx: &mut tokio::sync::watch::Receiver<Option<(u64, SourceCheckpoint)>>,
    delivery_guarantee: DeliveryGuarantee,
    src_name: &str,
    fault_tx: &tokio::sync::mpsc::UnboundedSender<SourceFault>,
) -> bool {
    let Some((epoch, checkpoint)) = epoch_committed_rx.borrow_and_update().clone() else {
        return true;
    };
    if let Err(error) = connector.notify_epoch_committed(epoch, &checkpoint).await {
        if delivery_guarantee == DeliveryGuarantee::BestEffort {
            tracing::warn!(
                source = src_name,
                %error,
                epoch,
                "notify_epoch_committed failed",
            );
            return true;
        }
        let _ = fault_tx.send(SourceFault {
            source: Arc::from(src_name),
            error: format!("commit notification failed at epoch {epoch}: {error}"),
        });
        return false;
    }
    true
}

#[cfg(feature = "cluster")]
async fn apply_latest_source_drain_command(
    connector: &mut dyn SourceConnector,
    command_rx: &mut tokio::sync::watch::Receiver<Option<SourceDrainCommand>>,
    status_tx: &tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    active: &mut Option<ActiveSourceDrain>,
    provider_drain: bool,
) -> Result<(), ConnectorError> {
    match command_rx.has_changed() {
        Ok(false) => return Ok(()),
        Err(_) => {
            return Err(ConnectorError::Internal(
                "source drain command channel closed".into(),
            ));
        }
        Ok(true) => {}
    }
    let Some(command) = command_rx.borrow_and_update().clone() else {
        return Ok(());
    };
    match command {
        SourceDrainCommand::Begin {
            request,
            participant,
            deadline,
        } => {
            if let Some(current) = active.as_ref() {
                if current.request != request || current.participant != participant {
                    return Err(ConnectorError::InvalidState {
                        expected: format!("active source drain {:?}", current.request.round),
                        actual: format!("conflicting source drain {:?}", request.round),
                    });
                }
                // A retry may carry a fresh caller wait budget, but it must not extend the
                // provider operation that already started. Retain the original deadline.
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ConnectorError::Internal(format!(
                    "source drain {:?} expired before provider preparation began",
                    request.round
                )));
            }
            if provider_drain {
                connector.begin_drain(&request, deadline)?;
            }
            let _ = status_tx.send_replace(SourceDrainTaskStatus::Pausing(request.round));
            *active = Some(ActiveSourceDrain {
                request,
                participant,
                provider_drain,
                prepare_deadline: deadline,
                ready: false,
                pending_resolution: None,
            });
        }
        SourceDrainCommand::Resolve {
            resolution,
            deadline,
        } => {
            let Some(current) = active.as_ref() else {
                match status_tx.borrow().clone() {
                    SourceDrainTaskStatus::Resolved { round, outcome }
                        if round == resolution.round && outcome == resolution.outcome =>
                    {
                        return Ok(());
                    }
                    SourceDrainTaskStatus::Resolved { round, outcome } => {
                        return Err(ConnectorError::InvalidState {
                            expected: format!("resolved source drain {round:?} as {outcome:?}"),
                            actual: format!("conflicting resolution {resolution:?}"),
                        });
                    }
                    SourceDrainTaskStatus::Pausing(round)
                    | SourceDrainTaskStatus::Ready(SourceDrainReceipt { round, .. }) => {
                        return Err(ConnectorError::InvalidState {
                            expected: format!("active source drain {round:?}"),
                            actual: "source drain task state was lost".into(),
                        });
                    }
                    SourceDrainTaskStatus::Idle => {}
                }
                // Prepare broadcasts Begin to every source before awaiting receipts. If one
                // source fails quickly, cleanup may overwrite an unobserved Begin in another
                // task's retained command slot. No connector work can have started while
                // `active` is empty, so abort is a safe terminal no-op. A replacement task may
                // also observe a durable commit after its predecessor published the receipt and
                // exited. It has no provider cut to finish; accept that commit only after its
                // target assignment and recovery cursor are reconciled.
                if resolution.outcome == SourceDrainOutcome::Abort {
                    let _ = status_tx.send_replace(SourceDrainTaskStatus::Resolved {
                        round: resolution.round,
                        outcome: resolution.outcome,
                    });
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(ConnectorError::Internal(format!(
                        "source drain resolution {:?} expired before replacement reconciliation",
                        resolution.round
                    )));
                }
                if provider_drain {
                    if !connector.checkpoint_ready()? {
                        command_rx.mark_changed();
                        return Ok(());
                    }
                    let Some(checkpoint) = try_source_checkpoint(connector, true)? else {
                        command_rx.mark_changed();
                        return Ok(());
                    };
                    let expected = std::num::NonZeroU64::new(resolution.round.target_version)
                        .ok_or_else(|| {
                            ConnectorError::Internal(
                                "replacement drain target has zero assignment version".into(),
                            )
                        })?;
                    if checkpoint.assignment_version() != Some(expected) {
                        return Err(ConnectorError::InvalidState {
                            expected: format!(
                                "replacement source assignment {}",
                                resolution.round.target_version
                            ),
                            actual: checkpoint.assignment_version().map_or_else(
                                || "unbound source checkpoint".into(),
                                |version| format!("source assignment {version}"),
                            ),
                        });
                    }
                }
                let _ = status_tx.send_replace(SourceDrainTaskStatus::Resolved {
                    round: resolution.round,
                    outcome: resolution.outcome,
                });
                return Ok(());
            };
            if current.request.round != resolution.round {
                return Err(ConnectorError::InvalidState {
                    expected: format!("active source drain {:?}", current.request.round),
                    actual: format!("resolution for {:?}", resolution.round),
                });
            }
            let resolution_deadline = match current.pending_resolution {
                Some(pending) if pending.resolution != resolution => {
                    return Err(ConnectorError::InvalidState {
                        expected: format!("pending resolution for {:?}", current.request.round),
                        actual: format!("conflicting resolution {resolution:?}"),
                    });
                }
                Some(pending) => pending.deadline,
                None => deadline,
            };
            if tokio::time::Instant::now() >= resolution_deadline {
                return Err(ConnectorError::Internal(format!(
                    "source drain resolution {:?} expired before provider resolution",
                    resolution.round
                )));
            }
            if !current.ready {
                if resolution.outcome != SourceDrainOutcome::Abort {
                    return Err(ConnectorError::InvalidState {
                        expected: "receipt-backed source drain cut before commit".into(),
                        actual: format!("unready source drain {:?}", resolution.round),
                    });
                }
                let current = active.as_mut().expect("checked above");
                // Rewinding before the FIFO boundary is consumed would duplicate payloads that
                // are already queued ahead of it. Keep flushing, then resolve from the certified
                // cut published by `poll_drain_ready`.
                if current.pending_resolution.is_none() {
                    current.pending_resolution = Some(PendingSourceDrainResolution {
                        resolution,
                        deadline: resolution_deadline,
                    });
                }
                return Ok(());
            }
            if current.provider_drain {
                connector
                    .finish_drain(resolution, resolution_deadline)
                    .await?;
            }
            *active = None;
            let _ = status_tx.send_replace(SourceDrainTaskStatus::Resolved {
                round: resolution.round,
                outcome: resolution.outcome,
            });
        }
    }
    Ok(())
}

#[cfg(feature = "cluster")]
async fn resolve_pending_source_drain(
    connector: &mut dyn SourceConnector,
    status_tx: &tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    active: &mut Option<ActiveSourceDrain>,
) -> Result<(), ConnectorError> {
    let Some(pending) = active.as_ref().and_then(|current| {
        current
            .ready
            .then_some(current.pending_resolution)
            .flatten()
    }) else {
        return Ok(());
    };
    if tokio::time::Instant::now() >= pending.deadline {
        return Err(ConnectorError::Internal(format!(
            "source drain resolution {:?} exceeded its deadline while awaiting the FIFO cut",
            pending.resolution.round
        )));
    }
    if active
        .as_ref()
        .is_some_and(|current| current.provider_drain)
    {
        connector
            .finish_drain(pending.resolution, pending.deadline)
            .await?;
    }
    *active = None;
    let _ = status_tx.send_replace(SourceDrainTaskStatus::Resolved {
        round: pending.resolution.round,
        outcome: pending.resolution.outcome,
    });
    Ok(())
}

#[cfg(feature = "cluster")]
fn publish_source_drain_ready(
    connector: &mut dyn SourceConnector,
    control: &SourceDrainLeaseControl,
    active: &mut Option<ActiveSourceDrain>,
) -> Result<(), ConnectorError> {
    let Some(current) = active.as_mut() else {
        return Ok(());
    };
    if current.ready {
        return Ok(());
    }
    if tokio::time::Instant::now() >= current.prepare_deadline {
        return Err(ConnectorError::Internal(format!(
            "source drain {:?} exceeded its preparation deadline",
            current.request.round
        )));
    }
    let ready = if current.provider_drain {
        connector.poll_drain_ready(current.request.round)?
    } else {
        true
    };
    if !ready {
        return Ok(());
    }
    let receipt = SourceDrainReceipt {
        round: current.request.round,
        participant: current.participant,
        source_task_incarnation: control.task_incarnation,
    };
    if !receipt.is_canonical() {
        return Err(ConnectorError::Internal(
            "source task produced a non-canonical drain receipt".into(),
        ));
    }
    current.ready = true;
    let _ = control
        .status_tx
        .send_replace(SourceDrainTaskStatus::Ready(receipt));
    Ok(())
}

#[cfg(feature = "cluster")]
fn source_drain_flushing(active: &Option<ActiveSourceDrain>) -> bool {
    active.as_ref().is_some_and(|drain| !drain.ready)
}

#[cfg(feature = "cluster")]
fn source_drain_held(active: &Option<ActiveSourceDrain>) -> bool {
    active.as_ref().is_some_and(|drain| drain.ready)
}

/// Backoff between completed polls while still servicing durable commit
/// notifications immediately. This never races a live `poll_batch` future.
async fn wait_source_idle(
    connector: &mut dyn SourceConnector,
    epoch_committed_rx: &mut tokio::sync::watch::Receiver<Option<(u64, SourceCheckpoint)>>,
    delivery_guarantee: DeliveryGuarantee,
    src_name: &str,
    fault_tx: &tokio::sync::mpsc::UnboundedSender<SourceFault>,
    shutdown: &tokio::sync::Notify,
    control_wake: Option<&tokio::sync::Notify>,
    poll_interval: Duration,
) -> bool {
    let data_ready = connector.data_ready_notify();
    tokio::select! {
        biased;
        () = shutdown.notified() => false,
        changed = epoch_committed_rx.changed() => match changed {
            Ok(()) => acknowledge_latest_source_commit(
                connector,
                epoch_committed_rx,
                delivery_guarantee,
                src_name,
                fault_tx,
            ).await,
            Err(_) => false,
        },
        () = async move {
            match data_ready {
                Some(notify) => notify.notified().await,
                None => std::future::pending().await,
            }
        } => true,
        () = async move {
            match control_wake {
                Some(notify) => notify.notified().await,
                None => std::future::pending().await,
            }
        } => true,
        () = tokio::time::sleep(poll_interval) => true,
    }
}

#[cfg(feature = "cluster")]
async fn wait_source_drain_hold(
    connector: &mut dyn SourceConnector,
    epoch_committed_rx: &mut tokio::sync::watch::Receiver<Option<(u64, SourceCheckpoint)>>,
    delivery_guarantee: DeliveryGuarantee,
    src_name: &str,
    fault_tx: &tokio::sync::mpsc::UnboundedSender<SourceFault>,
    shutdown: &tokio::sync::Notify,
    control_wake: &tokio::sync::Notify,
    poll_interval: Duration,
) -> bool {
    tokio::select! {
        biased;
        () = shutdown.notified() => false,
        changed = epoch_committed_rx.changed() => match changed {
            Ok(()) => acknowledge_latest_source_commit(
                connector,
                epoch_committed_rx,
                delivery_guarantee,
                src_name,
                fault_tx,
            ).await,
            Err(_) => false,
        },
        () = control_wake.notified() => true,
        () = tokio::time::sleep(poll_interval) => true,
    }
}

fn source_barrier_release_covers(released: CheckpointAttempt, held: CheckpointAttempt) -> bool {
    matches!(
        released.relation_to(held),
        CheckpointAttemptRelation::Exact | CheckpointAttemptRelation::Newer
    )
}

/// Hold a source at an emitted barrier until the coordinator releases that exact attempt.
///
/// The retained watch value closes the release-before-wait race. While held, the source keeps its
/// connector control plane and durable upstream acknowledgements live, but never polls data.
async fn wait_source_barrier_release(
    connector: &mut dyn SourceConnector,
    epoch_committed_rx: &mut tokio::sync::watch::Receiver<Option<(u64, SourceCheckpoint)>>,
    barrier_release_rx: &mut tokio::sync::watch::Receiver<Option<SourceBarrierSignal>>,
    delivery_guarantee: DeliveryGuarantee,
    src_name: &str,
    fault_tx: &tokio::sync::mpsc::UnboundedSender<SourceFault>,
    shutdown: &tokio::sync::Notify,
    poll_interval: Duration,
    barrier: CheckpointBarrier,
) -> bool {
    let attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
    loop {
        let signal = *barrier_release_rx.borrow_and_update();
        match signal {
            Some(SourceBarrierSignal::Release(released))
                if source_barrier_release_covers(released, attempt) =>
            {
                return true;
            }
            Some(SourceBarrierSignal::Stop) => return false,
            _ => {}
        }

        connector.drive_control_plane();
        tokio::select! {
            biased;
            () = shutdown.notified() => return false,
            changed = barrier_release_rx.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            changed = epoch_committed_rx.changed() => match changed {
                Ok(()) => {
                    if !acknowledge_latest_source_commit(
                        connector,
                        epoch_committed_rx,
                        delivery_guarantee,
                        src_name,
                        fault_tx,
                    ).await {
                        return false;
                    }
                }
                Err(_) => return false,
            },
            () = tokio::time::sleep(poll_interval) => {}
        }
    }
}

impl StreamingCoordinator {
    async fn close_startup_source(source: &mut SourceRegistration, cleanup_timeout: Duration) {
        let cleanup_deadline = tokio::time::Instant::now() + cleanup_timeout;
        let cancellation_policy = source.connector.cancellation_policy();
        let mut close = std::pin::pin!(source.connector.close());
        match tokio::time::timeout_at(cleanup_deadline, close.as_mut()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    source = %source.name,
                    %error,
                    "source close failed while rolling back pipeline startup"
                );
            }
            Err(_) => {
                tracing::warn!(
                    source = %source.name,
                    "source close exceeded its pipeline-startup cleanup deadline"
                );
                if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
                    if let Err(error) = close.await {
                        tracing::warn!(
                            source = %source.name,
                            %error,
                            "cancellation-unsafe source close failed after the cleanup deadline"
                        );
                    }
                }
            }
        }
    }

    async fn close_prepared_sources(
        sources: &mut Vec<SourceRegistration>,
        cleanup_timeout: Duration,
    ) {
        while let Some(mut source) = sources.pop() {
            Self::close_startup_source(&mut source, cleanup_timeout).await;
        }
    }

    /// Reap a source task without allowing a replacement generation to overlap a
    /// cancellation-unsafe operation.
    async fn reap_source_task(task: SourceTaskLease) {
        if !task.is_finished() {
            if task.cancellation_policy() == ConnectorCancellationPolicy::CompleteStarted {
                tracing::warn!(
                    source = %task.name(),
                    "source task exceeded shutdown budget; replacement remains fenced until it terminates"
                );
                task.wait().await;
            } else {
                tracing::warn!(
                    source = %task.name(),
                    "source task did not exit within shutdown budget; requesting cancellation"
                );
                task.abort_if_cancel_safe();
                // The DB-owned lease remains registered until the stable supervisor observes
                // actual termination. Coordinator shutdown stays bounded without admitting an
                // overlapping replacement generation.
                return;
            }
        }
        task.log_terminal_outcome();
    }

    /// Notify every source of a committed epoch so they can ack to their broker.
    fn broadcast_epoch_committed(
        &self,
        epoch: u64,
        per_source: &FxHashMap<String, SourceCheckpoint>,
    ) {
        for handle in &self.source_handles {
            let cp = per_source
                .get(handle.task.name())
                .cloned()
                .unwrap_or_else(SourceCheckpoint::new);
            let _ = handle.epoch_committed_tx.send(Some((epoch, cp)));
        }
    }

    fn release_source_barrier_attempt(&self, attempt: CheckpointAttempt) {
        for handle in &self.source_handles {
            handle.barrier_control().release_exact(attempt);
        }
    }

    fn release_source_barrier_for(&self, source_idx: usize, attempt: CheckpointAttempt) {
        if let Some(handle) = self.source_handles.get(source_idx) {
            handle.barrier_control().release_exact(attempt);
        }
    }

    fn stop_source_barrier_holds(&self) {
        for handle in &self.source_handles {
            handle.barrier_control().stop_hold();
        }
    }

    fn cancel_local_source_barriers(&self, barrier: CheckpointBarrier) {
        for handle in &self.source_handles {
            handle.barrier_control().cancel_exact(barrier);
        }
    }

    /// Build the coordinator, atomically start each source connector, and spawn source tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if delivery guarantee constraints are violated or a source fails to start
    /// at its requested initial/recovered position.
    pub async fn new(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self, DbError> {
        if let Some(source) = sources.iter().find(|source| source.assignment_scoped) {
            return Err(DbError::Config(format!(
                "assignment-scoped source '{}' requires the database-owned cluster runtime",
                source.name
            )));
        }
        Self::new_with_source_registry(
            sources,
            config,
            shutdown,
            control_rx,
            source_gate,
            Arc::new(parking_lot::Mutex::new(Vec::new())),
            false,
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn new_with_source_registry(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
        owned_source_tasks: OwnedSourceTasks,
        _cluster_source_drain: bool,
    ) -> Result<Self, DbError> {
        if config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            for src in &sources {
                if !src.contract.is_exact_delivery_certified() {
                    return Err(DbError::Config(format!(
                        "[{}] exactly-once source '{}' is not production-certified",
                        laminar_core::error_codes::EXACTLY_ONCE_SOURCE_UNCERTIFIED,
                        src.name
                    )));
                }
            }
        }
        if matches!(
            config.delivery_guarantee,
            DeliveryGuarantee::AtLeastOnce | DeliveryGuarantee::ExactlyOnce
        ) {
            for src in &sources {
                if !src.contract.supports_replay() {
                    return Err(DbError::Config(format!(
                        "[LDB-5031] {} requires source '{}' to support replay",
                        config.delivery_guarantee, src.name
                    )));
                }
            }
            if config.checkpoint_interval.is_none() {
                return Err(DbError::Config(format!(
                    "[LDB-5032] {} requires checkpointing to be enabled",
                    config.delivery_guarantee
                )));
            }
        }

        // A source that releases externally retained data only on durable commit needs
        // checkpointing; otherwise that data can grow without bound. Reject the combination up
        // front.
        if config.checkpoint_interval.is_none() {
            for src in &sources {
                if src.contract.requires_checkpointing() {
                    return Err(DbError::Config(format!(
                        "[LDB-5034] source '{}' requires checkpointing to be enabled: externally \
                         retained data is only released at a durable checkpoint",
                        src.name
                    )));
                }
            }
        }

        if config.channel_capacity == 0 {
            return Err(DbError::Config(
                "[LDB-0010] channel_capacity must be > 0".into(),
            ));
        }

        let source_count = sources.len();
        let mut prepared_sources = Vec::with_capacity(source_count);
        let mut committed_offsets = Vec::with_capacity(source_count);
        let source_start_timeout = config.checkpoint_timeout;
        let source_start_deadline = tokio::time::Instant::now() + source_start_timeout;

        // Do not spawn a polling task until every source has atomically installed its startup
        // position. Otherwise a later startup failure detaches the earlier tasks and they keep
        // polling without an owner capable of shutting them down.
        for mut src in sources {
            let src_name = src.name.clone();
            let start_position = src.position.clone();
            if tokio::time::Instant::now() >= source_start_deadline {
                // `timeout_at` polls its inner future once even when already expired. Starting a
                // source can acquire a lease/slot, so never construct or poll it after the shared
                // stage budget has been consumed by earlier sources.
                Self::close_prepared_sources(
                    &mut prepared_sources,
                    PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                )
                .await;
                let error = format!(
                    "shared {source_start_timeout:?} source-start stage deadline exhausted before start began"
                );
                return match start_position {
                    SourcePosition::Initial => Err(DbError::Config(format!(
                        "source '{src_name}' start was not attempted: {error}"
                    ))),
                    SourcePosition::Resume { attempt, .. } => Err(DbError::Checkpoint(format!(
                        "[LDB-6003] source '{src_name}' start was not attempted while resuming exact checkpoint epoch={} id={}: {error}",
                        attempt.epoch, attempt.checkpoint_id
                    ))),
                };
            }
            // Seed with the durable resume position so a pre-data shutdown still checkpoints it.
            // Capture it before moving the complete request into `start`; no connector lifecycle
            // operation is allowed between configuration and cursor installation.
            let committed_offset = match &src.position {
                SourcePosition::Initial => None,
                SourcePosition::Resume { checkpoint, .. } => Some(checkpoint.clone()),
            };
            let start = SourceStart {
                config: src.config.clone(),
                position: start_position.clone(),
                delivery: config.delivery_guarantee,
            };
            let cancellation_policy = src.connector.cancellation_policy();
            let start_error = {
                let mut start = std::pin::pin!(src.connector.start(start));
                match tokio::time::timeout_at(source_start_deadline, start.as_mut()).await {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error.to_string()),
                    Err(_) => {
                        if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
                            match start.as_mut().await {
                                Ok(()) => tracing::warn!(
                                    source = %src_name,
                                    "cancellation-unsafe source start completed after its deadline"
                                ),
                                Err(error) => tracing::warn!(
                                    source = %src_name,
                                    %error,
                                    "cancellation-unsafe source start failed after its deadline"
                                ),
                            }
                        }
                        Some(format!(
                            "exceeded the shared {source_start_timeout:?} source-start stage deadline"
                        ))
                    }
                }
            };
            if let Some(error) = start_error {
                // Include the current connector: a cancelled/failed start may already have acquired
                // external resources. Every started source receives a terminal cleanup attempt.
                prepared_sources.push(src);
                Self::close_prepared_sources(
                    &mut prepared_sources,
                    PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                )
                .await;
                return match start_position {
                    SourcePosition::Initial => Err(DbError::Config(format!(
                        "source '{src_name}' start failed at initial position: {error}"
                    ))),
                    SourcePosition::Resume { attempt, .. } => Err(DbError::Checkpoint(format!(
                        "[LDB-6003] source '{src_name}' start failed while resuming exact \
                             checkpoint epoch={} id={}: {error}",
                        attempt.epoch, attempt.checkpoint_id
                    ))),
                };
            }

            committed_offsets.push(committed_offset);
            prepared_sources.push(src);
        }

        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(config.channel_capacity);
        let (source_fault_tx, source_fault_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut source_handles = Vec::with_capacity(source_count);
        let mut source_names = Vec::with_capacity(source_count);
        let source_runtime = tokio::runtime::Handle::current();

        for (idx, src) in prepared_sources.into_iter().enumerate() {
            let task_shutdown = Arc::new(tokio::sync::Notify::new());
            let task_shutdown_clone = Arc::clone(&task_shutdown);
            let task_tx = tx.clone();
            let task_fault_tx = source_fault_tx.clone();
            let task_gate = Arc::clone(&source_gate);
            let max_poll = config.max_poll_records;
            let poll_interval = config.fallback_poll_interval;
            let delivery_guarantee = config.delivery_guarantee;
            let src_name = src.name.clone();
            let recovery_cursor = src.contract.supports_replay();
            let assignment_scoped = src.assignment_scoped;
            let cancellation_policy = src.connector.cancellation_policy();
            let mut connector = src.connector;

            #[cfg(feature = "cluster")]
            let drain_control = _cluster_source_drain.then(|| {
                let (command_tx, _) =
                    tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
                let (status_tx, _) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
                SourceDrainLeaseControl {
                    task_incarnation: uuid::Uuid::new_v4(),
                    command_tx,
                    status_tx,
                    wake: Arc::new(tokio::sync::Notify::new()),
                }
            });
            #[cfg(feature = "cluster")]
            let task_drain_control = drain_control.clone();
            #[cfg(feature = "cluster")]
            let mut task_drain_command_rx = task_drain_control
                .as_ref()
                .map(|control| control.command_tx.subscribe());
            #[cfg(feature = "cluster")]
            let task_control_wake = task_drain_control
                .as_ref()
                .map(|control| Arc::clone(&control.wake));
            #[cfg(not(feature = "cluster"))]
            let task_control_wake: Option<Arc<tokio::sync::Notify>> = None;

            let barrier_injector = CheckpointBarrierInjector::new();
            let barrier_handle = barrier_injector.handle();
            let (barrier_release_tx, mut barrier_release_rx) =
                tokio::sync::watch::channel::<Option<SourceBarrierSignal>>(None);

            let (epoch_committed_tx, mut epoch_committed_rx) =
                tokio::sync::watch::channel::<Option<(u64, SourceCheckpoint)>>(None);
            let (startup_activation_tx, startup_activation_rx) =
                crossfire::oneshot::oneshot::<()>();
            let expected_shutdown = Arc::new(AtomicBool::new(false));
            let task_expected_shutdown = Arc::clone(&expected_shutdown);

            let join = source_runtime.spawn(async move {
                let _exit_guard = SourceTaskExitGuard {
                    source: Arc::from(src_name.as_str()),
                    expected_shutdown: task_expected_shutdown,
                    fault_tx: task_fault_tx.clone(),
                };
                // Starting connectors and starting their I/O are separate phases. Keeping every
                // task behind this one-shot fence prevents a fast poll/commit failure from being
                // queued before the dedicated compute loop can publish its readiness boundary.
                // Cancellation before activation closes the connector without polling it.
                let activated = tokio::select! {
                    biased;
                    () = task_shutdown_clone.notified() => false,
                    activation = startup_activation_rx => activation.is_ok(),
                };

                if !activated {
                    if let Err(e) = connector.close().await {
                        tracing::warn!(source = %src_name, error = %e, "source close error");
                    }
                    return;
                }

                let mut pending_barrier = None;
                let mut pending_batch: Option<SourceBatch> = None;
                #[cfg(feature = "cluster")]
                let mut active_source_drain: Option<ActiveSourceDrain> = None;

                // Ack a fresh commit before polling more — keeps
                // max_ack_pending headroom for the broker.
                loop {
                    #[cfg(feature = "cluster")]
                    if let (Some(control), Some(command_rx)) =
                        (task_drain_control.as_ref(), task_drain_command_rx.as_mut())
                    {
                        if let Err(error) = apply_latest_source_drain_command(
                            connector.as_mut(),
                            command_rx,
                            &control.status_tx,
                            &mut active_source_drain,
                            assignment_scoped,
                        )
                        .await
                        {
                            let _ = task_fault_tx.send(SourceFault {
                                source: Arc::from(src_name.as_str()),
                                error: error.to_string(),
                            });
                            break;
                        }
                        // At loop entry every earlier source-channel send has completed. A cut
                        // observed in the preceding poll can therefore become externally Ready.
                        if pending_batch.is_none()
                            && publish_source_drain_ready(
                                connector.as_mut(),
                                control,
                                &mut active_source_drain,
                            )
                            .is_err_and(|error| {
                                let _ = task_fault_tx.send(SourceFault {
                                    source: Arc::from(src_name.as_str()),
                                    error: error.to_string(),
                                });
                                true
                            })
                        {
                            break;
                        }
                        if let Err(error) = resolve_pending_source_drain(
                            connector.as_mut(),
                            &control.status_tx,
                            &mut active_source_drain,
                        )
                        .await
                        {
                            let _ = task_fault_tx.send(SourceFault {
                                source: Arc::from(src_name.as_str()),
                                error: error.to_string(),
                            });
                            break;
                        }
                    }

                    match epoch_committed_rx.has_changed() {
                        Ok(true) => {
                            if !acknowledge_latest_source_commit(
                                connector.as_mut(),
                                &mut epoch_committed_rx,
                                delivery_guarantee,
                                &src_name,
                                &task_fault_tx,
                            )
                            .await
                            {
                                break;
                            }
                            continue;
                        }
                        Ok(false) => {}
                        Err(_) => break,
                    }

                    connector.drive_control_plane();

                    // A cluster-aware source may observe a new ownership publication before its
                    // external consumer has rebound and validated the version-bound handoff
                    // cursor. Do not poll data or consume a barrier during that window.
                    let checkpoint_ready = match connector.checkpoint_ready() {
                        Ok(ready) => ready,
                        Err(error) => {
                            tracing::error!(
                                source = %src_name,
                                %error,
                                "source control-plane reconciliation failed"
                            );
                            let _ = task_fault_tx.send(SourceFault {
                                source: Arc::from(src_name.as_str()),
                                error: error.to_string(),
                            });
                            break;
                        }
                    };
                    if !checkpoint_ready {
                        if !wait_source_idle(
                            connector.as_mut(),
                            &mut epoch_committed_rx,
                            delivery_guarantee,
                            &src_name,
                            &task_fault_tx,
                            &task_shutdown_clone,
                            task_control_wake.as_deref(),
                            poll_interval,
                        )
                        .await
                        {
                            break;
                        }
                        continue;
                    }

                    // A source assignment can publish after a batch was drained but before its
                    // cursor is captured. Keep the already-polled batch ahead of later data and
                    // retry its cursor once reconciliation completes; faulting or dropping here
                    // would turn a normal rotation into either downtime or data loss.
                    if let Some(batch) = pending_batch.take() {
                        match try_source_checkpoint(connector.as_ref(), assignment_scoped) {
                            Ok(Some(checkpoint)) => {
                                let msg = SourceMsg::Batch {
                                    source_idx: idx,
                                    batch: batch.records,
                                    checkpoint,
                                };
                                if task_tx.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            Ok(None) => {
                                pending_batch = Some(batch);
                                if !wait_source_idle(
                                    connector.as_mut(),
                                    &mut epoch_committed_rx,
                                    delivery_guarantee,
                                    &src_name,
                                    &task_fault_tx,
                                    &task_shutdown_clone,
                                    task_control_wake.as_deref(),
                                    poll_interval,
                                )
                                .await
                                {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = task_fault_tx.send(SourceFault {
                                    source: Arc::from(src_name.as_str()),
                                    error: error.to_string(),
                                });
                                break;
                            }
                        }
                        continue;
                    }

                    let drain_flushing = {
                        #[cfg(feature = "cluster")]
                        {
                            source_drain_flushing(&active_source_drain)
                        }
                        #[cfg(not(feature = "cluster"))]
                        {
                            false
                        }
                    };
                    let drain_held = {
                        #[cfg(feature = "cluster")]
                        {
                            source_drain_held(&active_source_drain)
                        }
                        #[cfg(not(feature = "cluster"))]
                        {
                            false
                        }
                    };

                    // Once claimed, a barrier stays ahead of all later data from this source.
                    // A transient publication race retries the same barrier instead of dropping
                    // it or polling another batch across the cut.
                    if !drain_flushing {
                        let barrier = pending_barrier.take().or_else(|| {
                            if drain_held {
                                barrier_handle.poll()
                            } else {
                                None
                            }
                        });
                        if let Some(barrier) = barrier {
                            match try_source_checkpoint(connector.as_ref(), assignment_scoped) {
                                Ok(Some(checkpoint)) => {
                                    let msg = SourceMsg::Barrier {
                                        source_idx: idx,
                                        barrier,
                                        checkpoint,
                                    };
                                    if task_tx.send(msg).await.is_err() {
                                        break;
                                    }
                                    if !wait_source_barrier_release(
                                        connector.as_mut(),
                                        &mut epoch_committed_rx,
                                        &mut barrier_release_rx,
                                        delivery_guarantee,
                                        &src_name,
                                        &task_fault_tx,
                                        &task_shutdown_clone,
                                        poll_interval,
                                        barrier,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                }
                                Ok(None) => {
                                    pending_barrier = Some(barrier);
                                    if !wait_source_idle(
                                        connector.as_mut(),
                                        &mut epoch_committed_rx,
                                        delivery_guarantee,
                                        &src_name,
                                        &task_fault_tx,
                                        &task_shutdown_clone,
                                        task_control_wake.as_deref(),
                                        poll_interval,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                }
                                Err(error) => {
                                    let _ = task_fault_tx.send(SourceFault {
                                        source: Arc::from(src_name.as_str()),
                                        error: error.to_string(),
                                    });
                                    break;
                                }
                            }
                            continue;
                        }
                    }

                    #[cfg(feature = "cluster")]
                    if drain_held {
                        let control = task_drain_control
                            .as_ref()
                            .expect("active source drain has task control");
                        if !wait_source_drain_hold(
                            connector.as_mut(),
                            &mut epoch_committed_rx,
                            delivery_guarantee,
                            &src_name,
                            &task_fault_tx,
                            &task_shutdown_clone,
                            &control.wake,
                            poll_interval,
                        )
                        .await
                        {
                            break;
                        }
                        continue;
                    }

                    // Source-intake gate: held closed during a coordinated round until the
                    // restore quorum, so a rewound source doesn't re-shuffle its replay into a
                    // peer whose receiver hasn't rebound (the frames would be dropped). The
                    // compute loop keeps draining the shuffle receiver on idle cycles meanwhile.
                    if task_gate.load(std::sync::atomic::Ordering::Acquire) && !drain_flushing {
                        // Preserve a claimed barrier ahead of later data while the strong startup
                        // or recovery fence is closed. The coordinator will not fold it until
                        // authority reopens. A drain predecessor keeps this strong gate open and
                        // uses the held-drain path above for its pre-rotation barrier.
                        if let Some(barrier) =
                            pending_barrier.take().or_else(|| barrier_handle.poll())
                        {
                            match try_source_checkpoint(connector.as_ref(), assignment_scoped) {
                                Ok(Some(checkpoint)) => {
                                    let msg = SourceMsg::Barrier {
                                        source_idx: idx,
                                        barrier,
                                        checkpoint,
                                    };
                                    if task_tx.send(msg).await.is_err() {
                                        break;
                                    }
                                    if !wait_source_barrier_release(
                                        connector.as_mut(),
                                        &mut epoch_committed_rx,
                                        &mut barrier_release_rx,
                                        delivery_guarantee,
                                        &src_name,
                                        &task_fault_tx,
                                        &task_shutdown_clone,
                                        poll_interval,
                                        barrier,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                    continue;
                                }
                                Ok(None) => pending_barrier = Some(barrier),
                                Err(error) => {
                                    let _ = task_fault_tx.send(SourceFault {
                                        source: Arc::from(src_name.as_str()),
                                        error: error.to_string(),
                                    });
                                    break;
                                }
                            }
                        }
                        if !wait_source_idle(
                            connector.as_mut(),
                            &mut epoch_committed_rx,
                            delivery_guarantee,
                            &src_name,
                            &task_fault_tx,
                            &task_shutdown_clone,
                            task_control_wake.as_deref(),
                            poll_interval,
                        )
                        .await
                        {
                            break;
                        }
                        continue;
                    }
                    let cancellation_policy = connector.cancellation_policy();
                    let poll_result =
                        if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
                            let mut poll = std::pin::pin!(connector.poll_batch(max_poll));
                            tokio::select! {
                                biased;
                                () = task_shutdown_clone.notified() => {
                                    match poll.await {
                                        Ok(Some(batch)) => pending_batch = Some(batch),
                                        Ok(None) => {}
                                        Err(error) => tracing::warn!(
                                            source = %src_name,
                                            %error,
                                            "source poll failed while completing for shutdown"
                                        ),
                                    }
                                    break;
                                }
                                result = poll.as_mut() => result,
                            }
                        } else {
                            tokio::select! {
                                biased;
                                () = task_shutdown_clone.notified() => break,
                                result = connector.poll_batch(max_poll) => result,
                            }
                        };

                    match poll_result {
                        Ok(Some(batch)) => {
                            let checkpoint = match try_source_checkpoint(
                                connector.as_ref(),
                                assignment_scoped,
                            ) {
                                Ok(Some(checkpoint)) => checkpoint,
                                Ok(None) => {
                                    pending_batch = Some(batch);
                                    continue;
                                }
                                Err(error) => {
                                    let _ = task_fault_tx.send(SourceFault {
                                        source: Arc::from(src_name.as_str()),
                                        error: error.to_string(),
                                    });
                                    break;
                                }
                            };
                            let msg = SourceMsg::Batch {
                                source_idx: idx,
                                batch: batch.records,
                                checkpoint,
                            };
                            if task_tx.send(msg).await.is_err() {
                                break; // Coordinator dropped
                            }
                        }
                        Ok(None) => {
                            #[cfg(feature = "cluster")]
                            let drain_became_ready =
                                if let Some(control) = task_drain_control.as_ref() {
                                    let was_flushing = source_drain_flushing(&active_source_drain);
                                    if let Err(error) = publish_source_drain_ready(
                                        connector.as_mut(),
                                        control,
                                        &mut active_source_drain,
                                    ) {
                                        let _ = task_fault_tx.send(SourceFault {
                                            source: Arc::from(src_name.as_str()),
                                            error: error.to_string(),
                                        });
                                        break;
                                    }
                                    was_flushing && !source_drain_flushing(&active_source_drain)
                                } else {
                                    false
                                };
                            #[cfg(not(feature = "cluster"))]
                            let drain_became_ready = false;
                            if drain_became_ready {
                                continue;
                            }
                            if !wait_source_idle(
                                connector.as_mut(),
                                &mut epoch_committed_rx,
                                delivery_guarantee,
                                &src_name,
                                &task_fault_tx,
                                &task_shutdown_clone,
                                task_control_wake.as_deref(),
                                poll_interval,
                            )
                            .await
                            {
                                break;
                            }
                        }
                        Err(e) if !e.is_transient() => {
                            tracing::error!(source = %src_name, error = %e, "terminal poll error");
                            // Delivery semantics may permit dropping individual records, never a
                            // configured producer. Surface terminal source loss in every mode so
                            // the lifecycle cannot remain Running with incomplete input.
                            let _ = task_fault_tx.send(SourceFault {
                                source: Arc::from(src_name.as_str()),
                                error: e.to_string(),
                            });
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(source = %src_name, error = %e, "poll error (retrying)");
                            if !wait_source_idle(
                                connector.as_mut(),
                                &mut epoch_committed_rx,
                                delivery_guarantee,
                                &src_name,
                                &task_fault_tx,
                                &task_shutdown_clone,
                                task_control_wake.as_deref(),
                                poll_interval,
                            )
                            .await
                            {
                                break;
                            }
                        }
                    }

                    let drain_flushing = {
                        #[cfg(feature = "cluster")]
                        {
                            source_drain_flushing(&active_source_drain)
                        }
                        #[cfg(not(feature = "cluster"))]
                        {
                            false
                        }
                    };
                    if !drain_flushing {
                        if let Some(barrier) =
                            pending_barrier.take().or_else(|| barrier_handle.poll())
                        {
                            match try_source_checkpoint(connector.as_ref(), assignment_scoped) {
                                Ok(Some(checkpoint)) => {
                                    let msg = SourceMsg::Barrier {
                                        source_idx: idx,
                                        barrier,
                                        checkpoint,
                                    };
                                    if task_tx.send(msg).await.is_err() {
                                        break;
                                    }
                                    if !wait_source_barrier_release(
                                        connector.as_mut(),
                                        &mut epoch_committed_rx,
                                        &mut barrier_release_rx,
                                        delivery_guarantee,
                                        &src_name,
                                        &task_fault_tx,
                                        &task_shutdown_clone,
                                        poll_interval,
                                        barrier,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                }
                                Ok(None) => pending_barrier = Some(barrier),
                                Err(error) => {
                                    let _ = task_fault_tx.send(SourceFault {
                                        source: Arc::from(src_name.as_str()),
                                        error: error.to_string(),
                                    });
                                    break;
                                }
                            }
                        }
                    }
                }

                #[cfg(feature = "cluster")]
                let may_flush_on_shutdown = active_source_drain.is_none();
                #[cfg(not(feature = "cluster"))]
                let may_flush_on_shutdown = true;

                if may_flush_on_shutdown {
                    // Bounded best-effort flush before close(): the `while` deadline bounds an
                    // always-ready poll (timeout() polls the future first). Unflushed rows resume
                    // from the committed offset.
                    if let Some(batch) = pending_batch.take() {
                        if let Ok(Some(checkpoint)) =
                            try_source_checkpoint(connector.as_ref(), assignment_scoped)
                        {
                            let _ = task_tx.try_send(SourceMsg::Batch {
                                source_idx: idx,
                                batch: batch.records,
                                checkpoint,
                            });
                        }
                    }
                    let deadline = Instant::now() + SHUTDOWN_DRAIN_BUDGET;
                    while Instant::now() < deadline {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        let poll_result = {
                            let cancellation_policy = connector.cancellation_policy();
                            let mut poll = std::pin::pin!(connector.poll_batch(max_poll));
                            match tokio::time::timeout(remaining, poll.as_mut()).await {
                                Ok(result) => Some(result),
                                Err(_)
                                    if cancellation_policy
                                        == ConnectorCancellationPolicy::CompleteStarted =>
                                {
                                    Some(poll.await)
                                }
                                Err(_) => None,
                            }
                        };
                        match poll_result {
                            Some(Ok(Some(batch))) => {
                                let Ok(Some(checkpoint)) =
                                    try_source_checkpoint(connector.as_ref(), assignment_scoped)
                                else {
                                    break;
                                };
                                let msg = SourceMsg::Batch {
                                    source_idx: idx,
                                    batch: batch.records,
                                    checkpoint,
                                };
                                if task_tx.try_send(msg).is_err() {
                                    break;
                                }
                                if Instant::now() >= deadline {
                                    break;
                                }
                            }
                            _ => break,
                        }
                    }
                }

                // Drain EpochCommitted broadcasts before close so a durable tail settled
                // during shutdown is acknowledged to the broker.
                while let Ok(()) = epoch_committed_rx.changed().await {
                    let snapshot = epoch_committed_rx.borrow_and_update().clone();
                    if let Some((e, cp)) = snapshot {
                        if let Err(err) = connector.notify_epoch_committed(e, &cp).await {
                            tracing::warn!(
                                source = %src_name,
                                error = %err,
                                epoch = e,
                                "notify_epoch_committed failed",
                            );
                        }
                    }
                }

                if let Err(e) = connector.close().await {
                    tracing::warn!(source = %src_name, error = %e, "source close error");
                }
            });

            let arc_name: Arc<str> = Arc::from(src.name.as_str());
            let task = SourceTaskLease::supervise(
                Arc::clone(&arc_name),
                cancellation_policy,
                Arc::clone(&task_shutdown),
                Arc::clone(&expected_shutdown),
                join,
                &source_runtime,
            );
            #[cfg(feature = "cluster")]
            if let Some(control) = drain_control {
                task.install_drain_control(control);
            }
            owned_source_tasks.lock().push(task.clone());
            source_handles.push(SourceHandle {
                recovery_cursor,
                task,
                startup_activation: Some(startup_activation_tx),
                barrier_injector,
                barrier_release_tx,
                epoch_committed_tx,
            });
            source_names.push(arc_name);
        }

        Ok(Self {
            config,
            rx,
            source_fault_rx,
            source_handles,
            source_names,
            shutdown,
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            source_batches_buf: FxHashMap::default(),
            parked_source_msg: None,
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            pending_offsets: vec![None; committed_offsets.len()],
            replay_pending: false,
            committed_offsets,
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        })
    }

    /// Wire in the callback's admission counters so the coordinator gates new barriers.
    pub(crate) fn with_checkpoint_admission(
        mut self,
        in_flight: Arc<AtomicU64>,
        staged_bytes: Arc<AtomicU64>,
        max_staged_bytes: u64,
    ) -> Self {
        self.checkpoint_in_flight = in_flight;
        self.staged_bytes = staged_bytes;
        self.max_staged_bytes = max_staged_bytes;
        self
    }

    pub(crate) fn with_coordinated_commit_admission(
        mut self,
        admission: Option<crate::checkpoint_coordinator::CoordinatedCommitAdmission>,
    ) -> Self {
        self.coordinated_commit_admission = admission;
        self
    }

    pub(crate) fn with_checkpoint_complete_rx(
        mut self,
        rx: crossfire::AsyncRx<crossfire::mpsc::Array<CheckpointCompletion>>,
    ) -> Self {
        self.checkpoint_complete_rx = Some(rx);
        self
    }

    pub(crate) fn with_force_checkpoint_rx(mut self, rx: crate::db::ForceCheckpointRx) -> Self {
        self.force_ckpt_rx = Some(rx);
        self
    }

    fn drain_manual_requests(&mut self) {
        let Some(rx) = self.force_ckpt_rx.as_ref() else {
            return;
        };
        while let Ok(reply) = rx.try_recv() {
            self.manual_waiting.push(reply);
        }
    }

    fn activate_manual_attempt(&mut self, attempt: CheckpointAttempt) {
        if self.manual_waiting.is_empty() {
            return;
        }
        debug_assert!(self.manual_active.is_none());
        self.manual_active = Some(ManualCheckpointAttempt {
            attempt,
            replies: std::mem::take(&mut self.manual_waiting),
        });
    }

    fn fail_waiting_manual(&mut self, error: impl Into<String>) {
        let error = error.into();
        for reply in self.manual_waiting.drain(..) {
            reply.send(Err(DbError::Checkpoint(error.clone())));
        }
    }

    fn finish_manual_success(
        &mut self,
        attempt: CheckpointAttempt,
        result: &crate::checkpoint_coordinator::CheckpointResult,
    ) {
        let Some(active) = self.manual_active.take() else {
            return;
        };
        if active.attempt != attempt {
            self.manual_active = Some(active);
            return;
        }
        let completed = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
        if completed != attempt || !result.success {
            let reason = format!(
                "manual checkpoint terminal result mismatch: admitted epoch={} id={}, \
                 completed epoch={} id={} success={}",
                attempt.epoch,
                attempt.checkpoint_id,
                completed.epoch,
                completed.checkpoint_id,
                result.success,
            );
            for reply in active.replies {
                reply.send(Err(DbError::Checkpoint(reason.clone())));
            }
            return;
        }
        for reply in active.replies {
            reply.send(Ok(result.clone()));
        }
    }

    fn fail_manual_attempt(&mut self, attempt: CheckpointAttempt, error: impl Into<String>) {
        let Some(active) = self.manual_active.take() else {
            return;
        };
        if active.attempt != attempt {
            self.manual_active = Some(active);
            return;
        }
        let error = error.into();
        for reply in active.replies {
            reply.send(Err(DbError::Checkpoint(error.clone())));
        }
    }

    fn fail_all_manual(&mut self, error: &str) {
        self.fail_waiting_manual(error);
        if let Some(active) = self.manual_active.take() {
            for reply in active.replies {
                reply.send(Err(DbError::Checkpoint(error.to_owned())));
            }
        }
    }

    async fn cleanup_checkpoint_attempt(
        callback: &mut impl PipelineCallback,
        attempt: CheckpointAttempt,
        reason: &str,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        if callback.is_leader() {
            callback
                .abandon_checkpoint_attempt(attempt, reason, assignment_fence)
                .await
        } else {
            callback
                .cancel_source_barrier_attempt(attempt, reason)
                .await
        }
    }

    async fn cancel_pending_barrier_for_stop(
        &mut self,
        callback: &mut impl PipelineCallback,
        reason: &str,
        release_sources: bool,
    ) -> Result<(), String> {
        let was_active = self.pending_barrier.active;
        let assignment_fence = self.pending_barrier.assignment_fence.clone();
        let attempt = self.pending_barrier.take_active_attempt();
        self.barrier_seen.clear();

        match attempt {
            Some(attempt) => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason,
                    "abandoning checkpoint interrupted before source alignment"
                );
                if callback.is_leader() {
                    self.cancel_local_source_barriers(CheckpointBarrier::new(
                        attempt.checkpoint_id,
                        attempt.epoch,
                    ));
                }
                Self::cleanup_checkpoint_attempt(callback, attempt, reason, assignment_fence)
                    .await?;
                if release_sources {
                    self.release_source_barrier_attempt(attempt);
                }
                callback.record_checkpoint_failure(attempt.checkpoint_id, reason);
                self.fail_manual_attempt(
                    attempt,
                    format!("manual checkpoint was interrupted: {reason}"),
                );
            }
            None if was_active => {
                callback.record_checkpoint_admission_failure(
                    "active source barrier had no exact reserved attempt during shutdown",
                );
                return Err(
                    "active source barrier had no exact reserved attempt during shutdown".into(),
                );
            }
            None => {}
        }
        Ok(())
    }

    /// Settle every captured durable tail before source or sink lifecycle teardown.
    ///
    /// The counter is claimed synchronously before a tail is spawned. A tail sends its terminal
    /// completion before dropping the claim, so waiting for zero and then draining the channel
    /// preserves exact source acknowledgements, public barriers, and manual replies. The tick
    /// handles tails that legitimately terminate without a completion (cluster followers) and
    /// avoids relying on a channel event after the atomic reaches zero.
    async fn settle_checkpoint_tails(
        &mut self,
        callback: &mut impl PipelineCallback,
    ) -> Option<String> {
        let mut continuation_fault = None;
        let deadline = Instant::now() + SHUTDOWN_CHECKPOINT_TAIL_TIMEOUT;
        let mut tails_aborted = false;
        loop {
            self.drain_manual_requests();
            self.fail_waiting_manual("pipeline is stopping; no new checkpoint can be admitted");

            while let Some(completion) = self
                .checkpoint_complete_rx
                .as_ref()
                .and_then(|rx| rx.try_recv().ok())
            {
                if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                    continuation_fault.get_or_insert(error);
                }
            }

            if self.checkpoint_in_flight.load(Ordering::Acquire) == 0 {
                break;
            }

            if Instant::now() >= deadline {
                let pending = self.checkpoint_in_flight.load(Ordering::Acquire);
                let reason = format!(
                    "checkpoint durable-tail shutdown drain timed out after \
                     {SHUTDOWN_CHECKPOINT_TAIL_TIMEOUT:?} with {pending} attempt(s) still in \
                     flight; cancelling tails for recovery"
                );
                continuation_fault.get_or_insert(reason);
                if let Err(error) = callback.settle_checkpoint_tail_tasks(true).await {
                    continuation_fault.get_or_insert(error);
                }
                tails_aborted = true;
                break;
            }

            let completion = if let Some(rx) = self.checkpoint_complete_rx.as_mut() {
                let tick = SHUTDOWN_COMPLETION_TICK
                    .min(deadline.saturating_duration_since(Instant::now()));
                match tokio::time::timeout(tick, rx.recv()).await {
                    Ok(Ok(completion)) => Some(completion),
                    Ok(Err(_)) => {
                        tokio::time::sleep(SHUTDOWN_COMPLETION_TICK).await;
                        None
                    }
                    Err(_) => None,
                }
            } else {
                tokio::time::sleep(SHUTDOWN_COMPLETION_TICK).await;
                None
            };
            if let Some(completion) = completion {
                if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                    continuation_fault.get_or_insert(error);
                }
            }
        }

        if !tails_aborted {
            if let Err(error) = callback.settle_checkpoint_tail_tasks(false).await {
                continuation_fault.get_or_insert(error);
            }
        }

        // A sender enqueues its completion before dropping the in-flight guard. Once the counter
        // reaches zero, drain the enqueue that may have raced with our last atomic load.
        while let Some(completion) = self
            .checkpoint_complete_rx
            .as_ref()
            .and_then(|rx| rx.try_recv().ok())
        {
            if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                continuation_fault.get_or_insert(error);
            }
        }
        self.drain_manual_requests();
        self.fail_all_manual("pipeline stopped before the checkpoint reached a terminal result");
        continuation_fault
    }

    fn handle_checkpoint_completion(
        &mut self,
        completion: CheckpointCompletion,
        callback: &mut impl PipelineCallback,
    ) -> Option<String> {
        let attempt = completion.attempt();
        match completion {
            CheckpointCompletion::Committed {
                result,
                source_checkpoints,
                ..
            } => {
                let completed = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
                if !result.success || completed != attempt {
                    let reason = format!(
                        "checkpoint terminal identity mismatch: admitted epoch={} id={}, \
                         completed epoch={} id={} success={}",
                        attempt.epoch,
                        attempt.checkpoint_id,
                        completed.epoch,
                        completed.checkpoint_id,
                        result.success,
                    );
                    callback.abort_subscription_cut(attempt);
                    self.fail_manual_attempt(attempt, &reason);
                    callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                } else {
                    if self.last_published_checkpoint.is_some_and(|last| {
                        last.relation_to(attempt) != CheckpointAttemptRelation::Older
                    }) {
                        let last = self.last_published_checkpoint.unwrap();
                        let reason = format!(
                            "checkpoint completion is not strictly newer: last published epoch={} id={}, \
                             received epoch={} id={}",
                            last.epoch,
                            last.checkpoint_id,
                            attempt.epoch,
                            attempt.checkpoint_id,
                        );
                        callback.abort_subscription_cut(attempt);
                        self.fail_manual_attempt(attempt, &reason);
                        callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                        return Some(reason);
                    }
                    let continuation_error = result.continuation_error().map(str::to_owned);
                    // Ordering is semantic: N reached its durable point, so source and public
                    // acknowledgements for N must be published even when N+1 cannot be opened.
                    self.last_published_checkpoint = Some(attempt);
                    let publication_error = callback.publish_barrier(attempt).err();
                    self.broadcast_epoch_committed(attempt.epoch, &source_checkpoints);
                    self.finish_manual_success(attempt, &result);
                    return publication_error.or(continuation_error);
                }
            }
            CheckpointCompletion::Failed { error, .. } => {
                callback.abort_subscription_cut(attempt);
                self.fail_manual_attempt(attempt, &error);
                callback.record_checkpoint_failure(attempt.checkpoint_id, &error);
            }
        }
        None
    }

    /// Run the coordinator loop until shutdown or a fatal cycle fault.
    ///
    /// Cycle priority: (1) shutdown, (2) drain + SQL, (3) barrier alignment,
    /// (4) checkpointing, (5) control, (6) barrier timeout.
    pub async fn run<C: PipelineCallback>(self, callback: C) -> ExitReason {
        self.run_inner(callback, None).await
    }

    /// Run the coordinator and report when its control loop is ready.
    ///
    /// Pipeline startup and coordinated recovery use this stronger boundary: constructing the
    /// compute runtime is not enough, because recovery must not acknowledge a node before barrier
    /// injection and manual-checkpoint control are installed on the live loop.
    pub(crate) async fn run_with_ready<C: PipelineCallback>(
        self,
        callback: C,
        ready: crossfire::oneshot::TxOneshot<Result<(), String>>,
    ) -> ExitReason {
        self.run_inner(callback, Some(ready)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn run_inner<C: PipelineCallback>(
        mut self,
        mut callback: C,
        ready: Option<crossfire::oneshot::TxOneshot<Result<(), String>>>,
    ) -> ExitReason {
        /// Maximum messages to drain per cycle before yielding for background work.
        const MAX_DRAIN_PER_CYCLE: usize = 10_000;

        let injectors = self
            .source_handles
            .iter()
            .map(SourceHandle::barrier_control)
            .collect();
        callback.set_barrier_injectors(injectors);
        if let Some(ready) = ready {
            ready.send(Ok(()));
        }
        // Readiness is the linearization point: source tasks are released only after it is
        // published, so none can enqueue a batch, barrier, or fault on the pre-ready side.
        for handle in &mut self.source_handles {
            if let Some(activation) = handle.startup_activation.take() {
                activation.send(());
            }
        }

        let batch_window = self.config.batch_window;
        let coordinated_commit_progress = self
            .coordinated_commit_admission
            .as_ref()
            .map(crate::checkpoint_coordinator::CoordinatedCommitAdmission::progress_notify);
        let mut barriers_buf: Vec<(usize, CheckpointBarrier, SourceCheckpoint)> = Vec::new();
        // Set by a fatal replay-guaranteed error; gates the open-epoch shutdown drain.
        let mut fault: Option<String> = None;
        // A pipeline without connectors is valid for source-less checkpoints. `new()`
        // intentionally has no channel sender in that case, so selecting the disconnected
        // receiver would make the live control loop exit immediately.
        let source_channel_expected = !self.source_names.is_empty();

        loop {
            if let Err(error) = callback.prepare_source_intake() {
                fault = Some(format!(
                    "source recovery handoff could not be installed before intake: {error}"
                ));
                break;
            }
            let intake_paused = callback.intake_paused();
            // At the coordinated external hard bound, stop consuming source data entirely.
            // The bounded source channel then propagates backpressure to connector polling. A
            // one-batch-per-idle-tick trickle would still let the open epoch grow without bound
            // while an external catalog is unavailable.
            let external_commit_paused = self
                .coordinated_commit_admission
                .as_ref()
                .is_some_and(|admission| !admission.can_admit());
            let replay_ready = self.replay_pending && !external_commit_paused && !intake_paused;
            let parked_ready = !self.replay_pending
                && !external_commit_paused
                && !intake_paused
                && self.parked_source_msg.is_some();
            // Wait for data, shutdown, or idle timeout.
            let mut retrying_replay = false;
            let msg = tokio::select! {
                biased;
                () = self.shutdown.notified() => break,
                Some(source_fault) = self.source_fault_rx.recv() => {
                    fault = Some(format!(
                        "source '{}' fault: {}",
                        source_fault.source, source_fault.error
                    ));
                    break;
                }
                // A background persist finished (in-flight guard ensures epoch order).
                Some(completion) = async {
                    if let Some(ref mut rx) = self.checkpoint_complete_rx {
                        rx.recv().await.ok()
                    } else {
                        futures::future::pending::<Option<CheckpointCompletion>>().await
                    }
                } => {
                    if let Some(error) = self.handle_checkpoint_completion(completion, &mut callback) {
                        fault = Some(error);
                        break;
                    }
                    continue;
                }
                Some(reply) = async {
                    if let Some(ref mut rx) = self.force_ckpt_rx {
                        rx.recv().await.ok()
                    } else {
                        futures::future::pending::<Option<ForceCheckpointReply>>().await
                    }
                } => {
                    self.manual_waiting.push(reply);
                    None
                },
                () = std::future::ready(()), if replay_ready => {
                    retrying_replay = true;
                    None
                },
                () = async {
                    if let Some(notify) = coordinated_commit_progress.as_ref() {
                        notify.notified().await;
                    } else {
                        futures::future::pending::<()>().await;
                    }
                } => None,
                () = std::future::ready(()), if parked_ready => self.parked_source_msg.take(),
                msg = self.rx.recv(), if source_channel_expected && !external_commit_paused && !intake_paused => {
                    if let Ok(message) = msg {
                        if !batch_window.is_zero() {
                            tokio::time::sleep(batch_window).await;
                        }
                        Some(message)
                    } else {
                        // Source-task shutdown is driven from below only after this loop has
                        // observed the lifecycle shutdown signal. Reaching channel exhaustion
                        // here means every configured producer disappeared unexpectedly.
                        fault = Some("all configured source tasks exited unexpectedly".into());
                        break;
                    }
                }
                () = tokio::time::sleep(IDLE_TIMEOUT) => None,
            };

            // A progress wake is edge-triggered; recompute the gate on the next loop before
            // touching deferred/open-epoch data. Completion branches above already `continue`.
            if external_commit_paused && msg.is_none() {
                continue;
            }
            // Recheck after the await: recovery may have closed the gate after this loop removed
            // a message from the source FIFO. Keep that message ahead of later FIFO entries so a
            // transient close/reopen cannot silently lose it. A fenced shutdown still discards
            // all open-epoch data below, where recovery owns the rewind.
            if intake_paused || callback.intake_paused() {
                if let Some(message) = msg {
                    if self.parked_source_msg.is_some() {
                        fault = Some(
                            "source intake gate race exceeded its single parked-message slot"
                                .into(),
                        );
                        break;
                    }
                    self.parked_source_msg = Some(message);
                }
                continue;
            }

            self.source_batches_buf.clear();
            self.reset_barrier_seen_for_cycle();
            if !retrying_replay && !self.replay_pending {
                self.discard_pending_offsets();
            }
            barriers_buf.clear();
            let mut cycle_events: u64 = 0;
            let cycle_start = Instant::now();

            let had_data = msg.is_some();
            if let Some(first_msg) = msg {
                if let Err(error) = self.process_msg(
                    first_msg,
                    &mut callback,
                    &mut barriers_buf,
                    &mut cycle_events,
                ) {
                    fault = Some(error);
                }
            }
            if fault.is_some() {
                self.discard_pending_offsets();
                break;
            }

            // Coalesce additional buffered messages; stop at count, time budget, or backpressure.
            let mut drain_count = 0;
            let drain_budget_ns = self.config.drain_budget_ns;
            // `is_backpressured()` bumps a counter, so call it only on active wakeups rather than
            // idle timeouts.
            let backpressured = had_data && callback.is_backpressured();
            if backpressured {
                tracing::debug!("operator graph backpressured — skipping drain");
            }
            #[allow(clippy::cast_possible_truncation)]
            while !backpressured
                && drain_count < MAX_DRAIN_PER_CYCLE
                && (cycle_start.elapsed().as_nanos() as u64) < drain_budget_ns
            {
                match self.rx.try_recv() {
                    Ok(msg) => {
                        if let Err(error) = self.process_msg(
                            msg,
                            &mut callback,
                            &mut barriers_buf,
                            &mut cycle_events,
                        ) {
                            fault = Some(error);
                            break;
                        }
                        drain_count += 1;
                    }
                    Err(_) => break,
                }
            }
            if let Ok(source_fault) = self.source_fault_rx.try_recv() {
                fault = Some(format!(
                    "source '{}' fault: {}",
                    source_fault.source, source_fault.error
                ));
            }
            if fault.is_some() {
                self.discard_pending_offsets();
                break;
            }

            for (name, batch) in self.pending_watermark_batches.drain(..) {
                callback.extract_watermark(&name, &batch);
            }

            if !self.replay_pending {
                callback.tick_idle_watermark();
            }

            // Run on idle wakeups too when operators have deferred input; otherwise
            // deferred data stalls once the source goes quiet.
            if !self.source_batches_buf.is_empty()
                || self.replay_pending
                || callback.has_deferred_input()
            {
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(out) => {
                        // Exactly-once / coordinated recovery rewinds the whole pipeline, so
                        // don't partial-commit siblings — recover instead.
                        if out.any_failed && callback.fault_on_cycle_error() {
                            self.discard_pending_offsets();
                            tracing::error!(
                                "[LDB-3021] failure domain faulted; faulting for recovery"
                            );
                            fault = Some("isolated domain fault (exactly-once)".to_string());
                            break;
                        }
                        if let Err(error) = self.publish_cycle_outputs(&mut callback, &out).await {
                            let reason = error.to_string();
                            tracing::error!(
                                error = %reason,
                                "cycle output publication failed; faulting for recovery"
                            );
                            fault = Some(reason);
                            break;
                        }
                        if out.any_failed {
                            callback.note_cycle_error();
                            tracing::warn!(
                                "[LDB-3020] failure domain dropped (best-effort: continuing)"
                            );
                        }
                    }
                    Err(e) => {
                        self.discard_pending_offsets();
                        match e {
                            CycleError::Recovery(msg) => {
                                tracing::error!(
                                    error = %msg,
                                    "shared pipeline infrastructure failed; faulting for recovery"
                                );
                                fault = Some(msg);
                                break;
                            }
                            // Shutdown already signaled; restarting would just re-trip it.
                            CycleError::Halt(msg) => {
                                tracing::warn!(reason = %msg, "[LDB-3022] cycle halted");
                            }
                            // Continuing would drop the drained rows (EO gap), so fault for
                            // recovery under exactly-once or coordinated recovery.
                            CycleError::Fatal(msg) if callback.fault_on_cycle_error() => {
                                tracing::error!(
                                    error = %msg,
                                    "[LDB-3021] fatal SQL cycle error; faulting for recovery"
                                );
                                fault = Some(msg);
                                break;
                            }
                            // Best effort: drop the bad cycle and continue.
                            CycleError::Fatal(msg) => {
                                callback.note_cycle_error();
                                tracing::warn!(
                                    error = %msg,
                                    "[LDB-3020] SQL cycle error (best-effort: continuing)"
                                );
                            }
                        }
                    }
                }
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                callback.record_cycle(cycle_events, 0, elapsed_ns);

                if elapsed_ns >= self.config.cycle_budget_ns {
                    tracing::debug!(
                        elapsed_ms = elapsed_ns / 1_000_000,
                        budget_ms = self.config.cycle_budget_ns / 1_000_000,
                        "cycle budget exceeded — proceeding to background work"
                    );
                }
            }

            let bg_start = Instant::now();
            let bg_budget = self.config.background_budget_ns;

            // A barrier cannot capture while graph work from the pre-barrier FIFO prefix is still
            // retained. Abandon the exact attempt so the buffered mutation can retry before a new
            // barrier is admitted.
            if self.replay_pending && self.pending_barrier.active {
                if let Err(error) = self
                    .cancel_pending_barrier_for_stop(
                        &mut callback,
                        "operator input remained deferred before source barrier alignment",
                        true,
                    )
                    .await
                {
                    fault = Some(error);
                    break;
                }
            }

            // Barriers are cheap (O(num_sources) lookups) and must not be skipped.
            for (source_idx, barrier, cp) in &barriers_buf {
                if let Err(error) = self
                    .handle_barrier(*source_idx, barrier, cp, &mut callback)
                    .await
                {
                    fault = Some(error);
                    break;
                }
            }
            if fault.is_some() {
                break;
            }

            // A stop notification may arrive while SQL or barrier work is running. Observe its
            // stored permit before admission so this cycle cannot originate a fresh attempt on
            // its way out.
            if self.shutdown.notified().now_or_never().is_some() {
                break;
            }

            // Never reserve or inject another attempt after an already-observed terminal fault.
            if let Some(reason) = callback.take_pipeline_fault() {
                self.discard_pending_offsets();
                tracing::error!(
                    reason = %reason,
                    "[LDB-3024] pipeline consistency fault; stopping for recovery"
                );
                fault = Some(reason);
                break;
            }

            #[allow(clippy::cast_possible_truncation)]
            if !self.replay_pending && (bg_start.elapsed().as_nanos() as u64) < bg_budget {
                self.maybe_checkpoint(&mut callback).await;
            }

            // DDL after checkpoint so newly added queries don't appear in the same snapshot.
            while let Ok(msg) = self.control_rx.try_recv() {
                callback.apply_control(msg);
            }

            if self.pending_barrier.active
                && self.pending_barrier.started_at.elapsed() > self.config.checkpoint_timeout
            {
                if let Err(error) = self
                    .cancel_pending_barrier_for_stop(
                        &mut callback,
                        "source barrier alignment timeout",
                        true,
                    )
                    .await
                {
                    fault = Some(error);
                    break;
                }
            }
        }

        // Every exit below is coordinator-owned. Mark it before tail settlement so source-task
        // guards cannot turn an intentional teardown into a second runtime fault.
        for handle in &self.source_handles {
            handle.task.mark_expected_shutdown();
        }

        // Stop is an admission fence. Cancel alignment before waiting for captured tails: an
        // unaligned attempt has no tail and therefore cannot make the in-flight counter progress.
        self.drain_manual_requests();
        self.fail_waiting_manual("pipeline is stopping; no new checkpoint can be admitted");
        let interrupted_reason = if fault.is_some() {
            "pipeline fault interrupted source barrier alignment"
        } else {
            "pipeline shutdown interrupted source barrier alignment"
        };
        if let Err(error) = self
            .cancel_pending_barrier_for_stop(&mut callback, interrupted_reason, false)
            .await
        {
            fault.get_or_insert(error);
        }

        // Captured tails own durable state and may still need to publish source acknowledgements.
        // Settling them while sources and sinks remain open prevents close from racing commit.
        if let Some(error) = self.settle_checkpoint_tails(&mut callback).await {
            fault.get_or_insert(error);
        }
        if let Some(reason) = callback.take_pipeline_fault() {
            fault.get_or_insert(reason);
        }

        self.stop_source_barrier_holds();
        for handle in &self.source_handles {
            handle.task.notify_shutdown();
        }

        let intake_fenced = callback.intake_paused();
        self.source_batches_buf.clear();
        self.pending_watermark_batches.clear();
        self.barrier_seen.clear();
        if intake_fenced || !self.replay_pending {
            self.discard_pending_offsets();
        }
        let mut drain_events = 0_u64;

        // A message parked by the intake-gate race is open-epoch data.
        if let Some(msg) = self.parked_source_msg.take() {
            if fault.is_none() && !intake_fenced {
                if let Some(reason) =
                    self.process_shutdown_msg(msg, &mut callback, &mut drain_events)
                {
                    fault = Some(reason);
                    self.source_batches_buf.clear();
                    self.pending_watermark_batches.clear();
                    self.discard_pending_offsets();
                }
            }
        }

        // Closing the watch senders releases source tasks after they consume any exact commit
        // broadcast settled above. Keep draining their data channel while they finish so a task
        // blocked on a full channel cannot deadlock shutdown.
        let mut stopping_sources = Vec::with_capacity(self.source_handles.len());
        for handle in std::mem::take(&mut self.source_handles) {
            let SourceHandle {
                task,
                epoch_committed_tx,
                ..
            } = handle;
            drop(epoch_committed_tx);
            stopping_sources.push(task);
        }

        let source_deadline = tokio::time::Instant::now() + SHUTDOWN_JOIN_TIMEOUT;
        while stopping_sources.iter().any(|task| !task.is_finished())
            && tokio::time::Instant::now() < source_deadline
        {
            while let Ok(msg) = self.rx.try_recv() {
                if fault.is_none() && !intake_fenced {
                    if let Some(reason) =
                        self.process_shutdown_msg(msg, &mut callback, &mut drain_events)
                    {
                        fault = Some(reason);
                        self.source_batches_buf.clear();
                        self.pending_watermark_batches.clear();
                        self.discard_pending_offsets();
                    }
                }
            }

            if stopping_sources.iter().all(SourceTaskLease::is_finished) {
                break;
            }

            let tick = SHUTDOWN_COMPLETION_TICK
                .min(source_deadline.saturating_duration_since(tokio::time::Instant::now()));
            match tokio::time::timeout(tick, self.rx.recv()).await {
                Ok(Ok(msg)) if fault.is_none() && !intake_fenced => {
                    if let Some(reason) =
                        self.process_shutdown_msg(msg, &mut callback, &mut drain_events)
                    {
                        fault = Some(reason);
                        self.source_batches_buf.clear();
                        self.pending_watermark_batches.clear();
                        self.discard_pending_offsets();
                    }
                }
                Ok(Ok(_) | Err(_)) | Err(_) => {}
            }
        }

        futures::future::join_all(stopping_sources.into_iter().map(Self::reap_source_task)).await;

        // Capture messages enqueued immediately before the last source task exited.
        while let Ok(msg) = self.rx.try_recv() {
            if fault.is_none() && !intake_fenced {
                if let Some(reason) =
                    self.process_shutdown_msg(msg, &mut callback, &mut drain_events)
                {
                    fault = Some(reason);
                    self.source_batches_buf.clear();
                    self.pending_watermark_batches.clear();
                    self.discard_pending_offsets();
                }
            }
        }

        if fault.is_none() && !intake_fenced {
            for (name, batch) in self.pending_watermark_batches.drain(..) {
                callback.extract_watermark(&name, &batch);
            }
            callback.tick_idle_watermark();
            if !self.source_batches_buf.is_empty()
                || self.replay_pending
                || callback.has_deferred_input()
            {
                let cycle_start = Instant::now();
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(out) if out.any_failed && callback.fault_on_cycle_error() => {
                        self.discard_pending_offsets();
                        fault = Some(
                            "isolated domain fault during shutdown drain under replay guarantee"
                                .to_string(),
                        );
                    }
                    Ok(out) => match self.publish_cycle_outputs(&mut callback, &out).await {
                        Ok(()) => {
                            if out.any_failed {
                                callback.note_cycle_error();
                            }
                        }
                        Err(error) => {
                            fault = Some(format!(
                                "cycle output publication failed during shutdown drain: {error}"
                            ));
                        }
                    },
                    Err(CycleError::Halt(reason)) => {
                        self.discard_pending_offsets();
                        tracing::warn!(%reason, "[LDB-3022] cycle halted during shutdown drain");
                    }
                    Err(CycleError::Recovery(reason)) => {
                        self.discard_pending_offsets();
                        fault = Some(format!(
                            "shared pipeline infrastructure failed during shutdown drain: {reason}"
                        ));
                    }
                    Err(CycleError::Fatal(reason)) if callback.fault_on_cycle_error() => {
                        self.discard_pending_offsets();
                        fault = Some(format!(
                            "fatal SQL cycle error during shutdown drain: {reason}"
                        ));
                    }
                    Err(CycleError::Fatal(reason)) => {
                        self.discard_pending_offsets();
                        callback.note_cycle_error();
                        tracing::warn!(%reason, "[LDB-3020] SQL cycle error during shutdown drain");
                    }
                }
                #[allow(clippy::cast_possible_truncation)]
                callback.record_cycle(drain_events, 0, cycle_start.elapsed().as_nanos() as u64);
            }
        }

        // Captured tails are settled and no more checkpoints can be admitted. Keep sink actors
        // open while the designated committer publishes every already-sealed exact cut. Open-
        // epoch rows above are intentionally excluded and replay after restart.
        if fault.is_none() {
            if let Err(error) = self.drain_coordinated_commits().await {
                if callback.fault_on_cycle_error() {
                    fault = Some(error);
                } else {
                    callback.note_cycle_error();
                    tracing::warn!(%error, "coordinated commit drain failed during shutdown");
                }
            }
        }

        // No final snapshot is synthesized: open-epoch rows deliberately replay from the last
        // committed cut. Sink close must confirm queued writes and abort any uncommitted
        // transactional epoch. A replay guarantee turns every close failure into a recovery fault;
        // best-effort reports it but may still stop normally.
        if let Err(close_error) = callback.close_sinks().await {
            if callback.fault_on_cycle_error() {
                match fault.as_mut() {
                    Some(existing) => {
                        existing.push_str("; sink shutdown also failed: ");
                        existing.push_str(&close_error);
                    }
                    None => fault = Some(format!("sink shutdown failed: {close_error}")),
                }
            } else {
                callback.note_cycle_error();
                tracing::warn!(
                    error = %close_error,
                    "sink shutdown failed under best-effort delivery"
                );
            }
        }

        let exit = fault.map_or(ExitReason::Shutdown, ExitReason::Fault);
        let reason = match &exit {
            ExitReason::Shutdown => {
                "pipeline stopped; discard subscription rows after the last committed progress frontier"
            }
            ExitReason::Fault(error) => error,
        };
        callback.invalidate_subscriptions(reason);
        exit
    }

    async fn drain_coordinated_commits(&mut self) -> Result<(), String> {
        let Some(admission) = self.coordinated_commit_admission.as_ref() else {
            return Ok(());
        };
        let deadline = Instant::now() + COORDINATED_COMMIT_SHUTDOWN_TIMEOUT;
        loop {
            let (known, pending, _) = admission.state();
            if known && pending == 0 {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "coordinated external commit drain timed out after \
                     {COORDINATED_COMMIT_SHUTDOWN_TIMEOUT:?} (lag_known={known}, \
                     pending={pending}); durable markers remain for recovery"
                ));
            }
            let progress = admission.progress_notify();
            let notified = progress.notified();
            tokio::pin!(notified);
            admission.wake_committer();
            if tokio::time::timeout(remaining, &mut notified)
                .await
                .is_err()
            {
                let (known, pending, _) = admission.state();
                return Err(format!(
                    "coordinated external commit drain timed out after \
                     {COORDINATED_COMMIT_SHUTDOWN_TIMEOUT:?} (lag_known={known}, \
                     pending={pending}); durable markers remain for recovery"
                ));
            }
        }
    }

    fn stage_batch(
        &mut self,
        source_idx: usize,
        batch: RecordBatch,
        checkpoint: SourceCheckpoint,
        callback: &mut impl PipelineCallback,
        cycle_events: &mut u64,
    ) {
        if source_idx < self.pending_offsets.len() {
            self.pending_offsets[source_idx] = Some(checkpoint);
        }

        if let Some(name) = self.source_names.get(source_idx) {
            #[allow(clippy::cast_possible_truncation)]
            {
                *cycle_events += batch.num_rows() as u64;
            }
            // Filter against the pre-drain watermark. Extraction is deferred until after all
            // batches are filtered so one batch cannot make the next batch appear late.
            if let Some(filtered) = callback.filter_late_rows(name, &batch) {
                self.source_batches_buf
                    .entry(Arc::clone(name))
                    .or_default()
                    .push(filtered);
            }
            self.pending_watermark_batches
                .push((Arc::clone(name), batch));
        }
    }

    /// Process one source message under the exact source-barrier ordering invariant.
    fn process_msg(
        &mut self,
        msg: SourceMsg,
        callback: &mut impl PipelineCallback,
        barriers: &mut Vec<(usize, CheckpointBarrier, SourceCheckpoint)>,
        cycle_events: &mut u64,
    ) -> Result<(), String> {
        match msg {
            SourceMsg::Batch {
                source_idx,
                batch,
                checkpoint,
            } => {
                if self.barrier_seen.contains(&source_idx) {
                    return Err(format!(
                        "source {} emitted data after its checkpoint barrier without an exact release",
                        self.source_names
                            .get(source_idx)
                            .map_or("<unknown>", AsRef::as_ref)
                    ));
                }
                self.stage_batch(source_idx, batch, checkpoint, callback, cycle_events);
            }
            SourceMsg::Barrier {
                source_idx,
                barrier,
                checkpoint,
            } => {
                let attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
                if !self.pending_barrier.active || self.pending_barrier.attempt != Some(attempt) {
                    tracing::debug!(
                        source_idx,
                        checkpoint_id = barrier.checkpoint_id,
                        epoch = barrier.epoch,
                        "ignoring stale or cancelled source barrier"
                    );
                    self.release_source_barrier_for(source_idx, attempt);
                    return Ok(());
                }
                tracing::debug!(
                    source_idx,
                    checkpoint_id = barrier.checkpoint_id,
                    "coordinator received source barrier"
                );
                self.barrier_seen.insert(source_idx);
                barriers.push((source_idx, barrier, checkpoint));
            }
        }
        Ok(())
    }

    /// Process a message after checkpoint admission has closed.
    ///
    /// No shutdown checkpoint exists, so every remaining batch belongs to an uncommitted open
    /// epoch. Barriers are control records for attempts that have already been cancelled and are
    /// ignored; they must never affect later open-epoch data.
    fn process_shutdown_msg(
        &mut self,
        msg: SourceMsg,
        callback: &mut impl PipelineCallback,
        cycle_events: &mut u64,
    ) -> Option<String> {
        match msg {
            SourceMsg::Batch {
                source_idx,
                batch,
                checkpoint,
            } => {
                self.stage_batch(source_idx, batch, checkpoint, callback, cycle_events);
                None
            }
            SourceMsg::Barrier {
                source_idx,
                barrier,
                ..
            } => {
                tracing::debug!(
                    source_idx,
                    checkpoint_id = barrier.checkpoint_id,
                    epoch = barrier.epoch,
                    "ignoring checkpoint barrier during shutdown drain"
                );
                None
            }
        }
    }

    /// Per-source committed offsets keyed by source name, reflecting the last successful cycle.
    /// Follower control uses this stable cut rather than advancing without source positions.
    fn current_source_offsets(&self) -> FxHashMap<String, SourceCheckpoint> {
        self.committed_offsets
            .iter()
            .enumerate()
            .filter_map(|(idx, cp)| {
                if !self
                    .source_handles
                    .get(idx)
                    .is_none_or(|handle| handle.recovery_cursor)
                {
                    return None;
                }
                cp.as_ref().and_then(|c| {
                    self.source_names
                        .get(idx)
                        .map(|name| (name.to_string(), c.clone()))
                })
            })
            .collect()
    }

    /// Merge staged offsets into `committed_offsets` after a successful cycle.
    fn commit_pending_offsets(&mut self) {
        for (i, pending) in self.pending_offsets.iter_mut().enumerate() {
            if let Some(cp) = pending.take() {
                self.committed_offsets[i] = Some(cp);
            }
        }
    }

    /// Publish all externally visible cycle output before advancing source cursors or writing
    /// sinks. Publication failure is a shared-runtime consistency fault, so every mode recovers.
    async fn publish_cycle_outputs(
        &mut self,
        callback: &mut impl PipelineCallback,
        outcome: &CycleOutcome,
    ) -> Result<(), CycleError> {
        if let Err(error) = callback.update_mv_stores(&outcome.results) {
            self.discard_pending_offsets();
            return Err(CycleError::Recovery(format!(
                "materialized-view publication failed: {error}"
            )));
        }
        if let Err(error) = callback.push_to_streams(&outcome.results) {
            self.discard_pending_offsets();
            return Err(CycleError::Recovery(format!(
                "stream publication failed: {error}"
            )));
        }

        // Healthy sources advance, replay-preserved sources retain their cursor, and faulted
        // best-effort domains deliberately drop this cycle only after publication succeeds.
        self.settle_pending_offsets(&outcome.failed_sources, &outcome.deferred_sources);
        self.replay_pending = outcome.any_deferred;
        callback.write_to_sinks(&outcome.results).await;
        Ok(())
    }

    /// Settle one graph cycle without allowing a cursor to overtake graph-retained input.
    /// Failure takes precedence if a source appears in both sets: best-effort isolation drops the
    /// failed cycle, whereas a pure deferral must retain the exact staged cursor for its retry.
    fn settle_pending_offsets(
        &mut self,
        failed: &FxHashSet<Arc<str>>,
        deferred: &FxHashSet<Arc<str>>,
    ) {
        if failed.is_empty() {
            if deferred.is_empty() {
                self.commit_pending_offsets();
                return;
            }
        }
        for (i, pending) in self.pending_offsets.iter_mut().enumerate() {
            let in_failed_domain = self
                .source_names
                .get(i)
                .is_some_and(|name| failed.contains(name));
            if in_failed_domain {
                *pending = None;
            } else if self
                .source_names
                .get(i)
                .is_some_and(|name| deferred.contains(name))
            {
                // Retain the cursor alongside the graph's buffered input.
            } else if let Some(cp) = pending.take() {
                self.committed_offsets[i] = Some(cp);
            }
        }
    }

    /// Discard staged offsets when `execute_cycle` fails.
    fn discard_pending_offsets(&mut self) {
        for slot in &mut self.pending_offsets {
            *slot = None;
        }
    }

    /// Reset per-cycle barrier tracking at cycle start. While a multi-source barrier is still
    /// aligning, retain sources already held at the cut so any protocol violation fails closed.
    fn reset_barrier_seen_for_cycle(&mut self) {
        self.barrier_seen.clear();
        if self.pending_barrier.active {
            self.barrier_seen
                .extend(self.pending_barrier.sources_aligned.iter().copied());
        }
    }

    fn capture_replayable_barrier_cursor(
        &mut self,
        source_idx: usize,
        checkpoint: &SourceCheckpoint,
    ) {
        if self
            .source_handles
            .get(source_idx)
            .is_some_and(|handle| !handle.recovery_cursor)
        {
            return;
        }
        if let Some(name) = self.source_names.get(source_idx) {
            self.pending_barrier
                .source_checkpoints
                .insert(name.to_string(), checkpoint.clone());
        }
    }

    async fn handle_aligned_checkpoint_outcome(
        &mut self,
        callback: &mut impl PipelineCallback,
        outcome: BarrierOutcome,
        attempt: CheckpointAttempt,
        attempt_started: Instant,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        source_checkpoints: &FxHashMap<String, SourceCheckpoint>,
    ) -> Result<(), String> {
        match outcome {
            BarrierOutcome::Committed(epoch) => {
                if epoch == attempt.epoch {
                    let publication_error = callback.publish_barrier(attempt).err();
                    self.broadcast_epoch_committed(epoch, source_checkpoints);
                    self.finish_manual_success(
                        attempt,
                        &crate::checkpoint_coordinator::CheckpointResult {
                            success: true,
                            checkpoint_id: attempt.checkpoint_id,
                            epoch: attempt.epoch,
                            duration: attempt_started.elapsed(),
                            error: None,
                            failure_disposition: None,
                        },
                    );
                    if let Some(reason) = publication_error {
                        return Err(reason);
                    }
                } else {
                    callback.abort_subscription_cut(attempt);
                    let reason = format!(
                        "checkpoint callback committed epoch {epoch} for reserved epoch {}",
                        attempt.epoch
                    );
                    Self::cleanup_checkpoint_attempt(
                        callback,
                        attempt,
                        &reason,
                        assignment_fence.clone(),
                    )
                    .await?;
                    callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                    self.fail_manual_attempt(attempt, reason);
                }
            }
            BarrierOutcome::Async => {
                self.last_checkpoint = Instant::now();
            }
            BarrierOutcome::Skipped(reason) => {
                callback.abort_subscription_cut(attempt);
                tracing::debug!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason = %reason,
                    "barrier checkpoint skipped"
                );
                Self::cleanup_checkpoint_attempt(
                    callback,
                    attempt,
                    &reason.to_string(),
                    assignment_fence.clone(),
                )
                .await?;
                self.fail_manual_attempt(attempt, format!("manual checkpoint skipped: {reason}"));
            }
            BarrierOutcome::Failed => {
                callback.abort_subscription_cut(attempt);
                Self::cleanup_checkpoint_attempt(
                    callback,
                    attempt,
                    "barrier-aligned checkpoint failed before durable tail",
                    assignment_fence,
                )
                .await?;
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    "barrier-aligned checkpoint failed",
                );
                self.fail_manual_attempt(
                    attempt,
                    "manual barrier-aligned checkpoint failed before the durable tail",
                );
                tracing::warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "barrier checkpoint failed"
                );
            }
        }
        Ok(())
    }

    /// Handle a barrier from a source.
    async fn handle_barrier(
        &mut self,
        source_idx: usize,
        barrier: &CheckpointBarrier,
        barrier_checkpoint: &SourceCheckpoint,
        callback: &mut impl PipelineCallback,
    ) -> Result<(), String> {
        let barrier_attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
        if !self.pending_barrier.active || self.pending_barrier.attempt != Some(barrier_attempt) {
            self.release_source_barrier_for(source_idx, barrier_attempt);
            return Ok(());
        }

        self.capture_replayable_barrier_cursor(source_idx, barrier_checkpoint);

        self.pending_barrier.sources_aligned.insert(source_idx);

        if self.pending_barrier.sources_aligned.len() >= self.pending_barrier.sources_total {
            let checkpoints = std::mem::take(&mut self.pending_barrier.source_checkpoints);
            // Clone for fan-out so each source gets the exact checkpoint that was persisted.
            let fan_out = checkpoints.clone();
            let attempt = barrier_attempt;
            let attempt_started = self.pending_barrier.started_at;
            let assignment_fence = self.pending_barrier.assignment_fence.clone();
            self.pending_barrier.clear();
            let attempt_deadline =
                tokio::time::Instant::from_std(attempt_started) + self.config.checkpoint_timeout;
            if let Err(error) = callback
                .drain_checkpoint_edges_until(attempt_deadline)
                .await
            {
                let error = error.to_string();
                self.handle_aligned_checkpoint_outcome(
                    callback,
                    BarrierOutcome::Failed,
                    attempt,
                    attempt_started,
                    assignment_fence,
                    &fan_out,
                )
                .await
                .map_err(|cleanup| format!("{error}; checkpoint cleanup failed: {cleanup}"))?;
                return Err(error);
            }
            if let Err(error) = callback.reserve_subscription_cut(attempt) {
                self.handle_aligned_checkpoint_outcome(
                    callback,
                    BarrierOutcome::Failed,
                    attempt,
                    attempt_started,
                    assignment_fence,
                    &fan_out,
                )
                .await
                .map_err(|cleanup| format!("{error}; checkpoint cleanup failed: {cleanup}"))?;
                return Err(error);
            }
            let outcome = callback
                .checkpoint_with_barrier(
                    checkpoints,
                    attempt,
                    attempt_started,
                    assignment_fence.clone(),
                )
                .await;
            self.handle_aligned_checkpoint_outcome(
                callback,
                outcome,
                attempt,
                attempt_started,
                assignment_fence,
                &fan_out,
            )
            .await?;
            if let Some(error) = callback.take_pipeline_fault() {
                return Err(error);
            }
            // Capture or exact cleanup has completed. A cleanup failure or sticky replay fault
            // returns above and deliberately leaves the sources held for coordinated recovery.
            self.release_source_barrier_attempt(attempt);
            self.last_checkpoint = Instant::now();
        }
        Ok(())
    }

    fn checkpoint_capacity_available(&self) -> bool {
        if self.pending_barrier.active || self.checkpoint_in_flight.load(Ordering::Acquire) >= 1 {
            return false;
        }
        let staged_bytes = self.staged_bytes.load(Ordering::Acquire);
        if staged_bytes >= self.max_staged_bytes {
            warn_staged_cap_throttled(staged_bytes, self.max_staged_bytes);
            return false;
        }
        true
    }

    async fn checkpoint_admission(
        &mut self,
        callback: &mut impl PipelineCallback,
    ) -> Option<CheckpointAdmission> {
        // Requests arriving after a manual attempt was admitted belong to a later cut. Never let
        // an intervening periodic attempt consume them or attach them to the active attempt.
        if !self.manual_waiting.is_empty() && self.manual_active.is_some() {
            return None;
        }
        let manual = !self.manual_waiting.is_empty();
        let leader = callback.is_leader();
        if manual && !leader {
            self.fail_waiting_manual("only the cluster leader may admit a manual checkpoint");
            return None;
        }

        let interval = leader
            && self
                .config
                .checkpoint_interval
                .is_some_and(|value| self.last_checkpoint.elapsed() >= value);
        if !manual && !interval {
            return None;
        }

        // Every trigger observes the same recovery and assignment fence. Periodic work remains
        // due through `last_checkpoint`; a caller waiting on a manual request gets a prompt
        // rejection without burning an exact attempt ID.
        if callback.is_recovering() {
            if manual {
                self.fail_waiting_manual(
                    "manual checkpoint rejected while coordinated recovery is in progress",
                );
            }
            return None;
        }
        let assignment_fence = match callback.checkpoint_assignment_for_admission().await {
            Ok(fence) => fence,
            Err(reason) => {
                if manual {
                    self.fail_waiting_manual(format!(
                        "[LDB-6056] manual checkpoint rejected: {reason}"
                    ));
                }
                return None;
            }
        };
        if !self.checkpoint_capacity_available() {
            return None;
        }
        if let Some(admission) = &self.coordinated_commit_admission {
            if !admission.can_admit() {
                let (known, pending, cap) = admission.state();
                warn_external_commit_cap_throttled(known, pending, cap);
                return None;
            }
        }
        Some(CheckpointAdmission {
            manual,
            assignment_fence,
        })
    }

    async fn reserve_prepared_checkpoint_attempt(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt_started: Instant,
    ) -> Result<CheckpointAttempt, String> {
        let attempt = callback.reserve_checkpoint_attempt(attempt_started).await?;
        if let Err(error) = callback
            .publish_checkpoint_prepare(
                attempt,
                attempt_started,
                admission.assignment_fence.clone(),
            )
            .await
        {
            if let Err(cleanup_error) = callback
                .abandon_checkpoint_attempt(attempt, &error, admission.assignment_fence.clone())
                .await
            {
                return Err(format!(
                    "{error}; reserved checkpoint cleanup failed: {cleanup_error}"
                ));
            }
            return Err(error);
        }
        Ok(attempt)
    }

    async fn handle_source_less_checkpoint_outcome(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt: CheckpointAttempt,
        outcome: BarrierOutcome,
    ) -> Result<(), String> {
        match outcome {
            BarrierOutcome::Committed(epoch) if epoch == attempt.epoch => {
                let publication_error = callback.publish_barrier(attempt).err();
                self.broadcast_epoch_committed(epoch, &FxHashMap::default());
                self.finish_manual_success(
                    attempt,
                    &crate::checkpoint_coordinator::CheckpointResult {
                        success: true,
                        checkpoint_id: attempt.checkpoint_id,
                        epoch: attempt.epoch,
                        duration: Duration::ZERO,
                        error: None,
                        failure_disposition: None,
                    },
                );
                if let Some(reason) = publication_error {
                    return Err(reason);
                }
            }
            BarrierOutcome::Committed(epoch) => {
                callback.abort_subscription_cut(attempt);
                let reason = format!(
                    "checkpoint callback committed epoch {epoch} for reserved epoch {}",
                    attempt.epoch
                );
                Self::cleanup_checkpoint_attempt(
                    callback,
                    attempt,
                    &reason,
                    admission.assignment_fence.clone(),
                )
                .await?;
                callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                self.fail_manual_attempt(attempt, reason);
            }
            BarrierOutcome::Async => {}
            BarrierOutcome::Skipped(reason) => {
                callback.abort_subscription_cut(attempt);
                tracing::debug!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason = %reason,
                    "source-less checkpoint skipped"
                );
                Self::cleanup_checkpoint_attempt(
                    callback,
                    attempt,
                    &reason.to_string(),
                    admission.assignment_fence.clone(),
                )
                .await?;
                self.fail_manual_attempt(attempt, format!("manual checkpoint skipped: {reason}"));
            }
            BarrierOutcome::Failed => {
                callback.abort_subscription_cut(attempt);
                Self::cleanup_checkpoint_attempt(
                    callback,
                    attempt,
                    "source-less checkpoint failed before durable tail",
                    admission.assignment_fence.clone(),
                )
                .await?;
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    "source-less checkpoint failed",
                );
                self.fail_manual_attempt(
                    attempt,
                    "manual source-less checkpoint failed before the durable tail",
                );
                tracing::warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "source-less checkpoint failed"
                );
            }
        }
        Ok(())
    }

    async fn admit_source_less_checkpoint(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
    ) {
        let attempt_started = Instant::now();
        let attempt = match self
            .reserve_prepared_checkpoint_attempt(callback, admission, attempt_started)
            .await
        {
            Ok(attempt) => attempt,
            Err(error) => {
                tracing::error!(
                    error = %error,
                    "durable source-less checkpoint attempt reservation failed"
                );
                callback.record_checkpoint_admission_failure(&error);
                if admission.manual {
                    self.fail_waiting_manual(format!(
                        "manual checkpoint attempt reservation failed: {error}"
                    ));
                }
                return;
            }
        };
        if admission.manual {
            self.activate_manual_attempt(attempt);
        }
        let attempt_deadline =
            tokio::time::Instant::from_std(attempt_started) + self.config.checkpoint_timeout;
        if let Err(error) = callback
            .drain_checkpoint_edges_until(attempt_deadline)
            .await
        {
            tracing::error!(%error, "source-less checkpoint graph drain failed");
            if let Err(cleanup_error) = self
                .handle_source_less_checkpoint_outcome(
                    callback,
                    admission,
                    attempt,
                    BarrierOutcome::Failed,
                )
                .await
            {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("checkpoint cleanup failed after graph drain: {cleanup_error}"),
                );
            }
            self.last_checkpoint = Instant::now();
            return;
        }
        if let Err(error) = callback.reserve_subscription_cut(attempt) {
            tracing::error!(%error, "source-less subscription cut reservation failed");
            if let Err(cleanup_error) = self
                .handle_source_less_checkpoint_outcome(
                    callback,
                    admission,
                    attempt,
                    BarrierOutcome::Failed,
                )
                .await
            {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{error}; checkpoint cleanup failed: {cleanup_error}"),
                );
            }
            self.last_checkpoint = Instant::now();
            return;
        }
        let outcome = callback
            .checkpoint_with_barrier(
                FxHashMap::default(),
                attempt,
                attempt_started,
                admission.assignment_fence.clone(),
            )
            .await;
        if let Err(error) = self
            .handle_source_less_checkpoint_outcome(callback, admission, attempt, outcome)
            .await
        {
            callback.record_checkpoint_continuation_fault(attempt, &error);
        }
        self.last_checkpoint = Instant::now();
    }

    async fn admit_source_barrier_checkpoint(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
    ) {
        if self
            .source_handles
            .iter()
            .any(|handle| !handle.barrier_injector.can_trigger())
        {
            tracing::debug!(
                "checkpoint admission deferred: a source barrier injector is still busy"
            );
            return;
        }

        let attempt_started = Instant::now();
        let attempt = match self
            .reserve_prepared_checkpoint_attempt(callback, admission, attempt_started)
            .await
        {
            Ok(attempt) => attempt,
            Err(error) => {
                tracing::error!(error = %error, "durable checkpoint attempt reservation failed");
                callback.record_checkpoint_admission_failure(&error);
                if admission.manual {
                    self.fail_waiting_manual(format!(
                        "manual checkpoint attempt reservation failed: {error}"
                    ));
                }
                return;
            }
        };
        if admission.manual {
            self.activate_manual_attempt(attempt);
        }
        self.pending_barrier.reset_with_assignment(
            attempt,
            self.source_handles.len(),
            admission.assignment_fence.clone(),
        );
        // Attempt time includes reservation, alignment, capture, quorum, and publication.
        self.pending_barrier.started_at = attempt_started;
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);

        for handle in &self.source_handles {
            if !handle.barrier_injector.trigger(barrier) {
                self.pending_barrier.clear();
                self.cancel_local_source_barriers(barrier);
                let cleanup = Self::cleanup_checkpoint_attempt(
                    callback,
                    attempt,
                    "source barrier injection was rejected after preflight",
                    admission.assignment_fence.clone(),
                )
                .await;
                if cleanup.is_ok() {
                    self.release_source_barrier_attempt(attempt);
                } else if let Err(error) = cleanup {
                    callback.record_checkpoint_failure(
                        attempt.checkpoint_id,
                        &format!("source barrier injection cleanup failed: {error}"),
                    );
                }
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    "source barrier injection was rejected after preflight",
                );
                self.fail_manual_attempt(
                    attempt,
                    "manual checkpoint source barrier injection was rejected after preflight",
                );
                return;
            }
        }
    }

    /// Service periodic, manual, or leader-announced checkpoint admission.
    async fn maybe_checkpoint(&mut self, callback: &mut impl PipelineCallback) {
        self.drain_manual_requests();

        // Followers do not originate attempts. Preserve their resource cap while servicing the
        // leader's exact control announcement; leader/local admission applies its own cap below.
        if !callback.is_leader() {
            if !self.manual_waiting.is_empty() {
                self.fail_waiting_manual("only the cluster leader may admit a manual checkpoint");
            }
            if !self.checkpoint_capacity_available() {
                return;
            }
            match callback
                .service_checkpoint_control(self.current_source_offsets())
                .await
            {
                CheckpointControlOutcome::Idle => {}
                CheckpointControlOutcome::Started { attempt, captured } => {
                    if !captured {
                        self.pending_barrier
                            .reset(attempt, self.source_handles.len());
                    }
                }
                CheckpointControlOutcome::Failed { attempt, error } => {
                    callback.record_checkpoint_failure(attempt.checkpoint_id, &error);
                }
            }
            return;
        }
        let Some(admission) = self.checkpoint_admission(callback).await else {
            return;
        };
        if self.source_handles.is_empty() {
            self.admit_source_less_checkpoint(callback, &admission)
                .await;
        } else {
            self.admit_source_barrier_checkpoint(callback, &admission)
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::callback::CycleOutcome;
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parking_lot::{Condvar, Mutex};
    use std::sync::Arc;

    #[test]
    fn barrier_release_high_watermark_cannot_be_overwritten_by_stale_attempt() {
        let injector = CheckpointBarrierInjector::new();
        let (release_tx, release_rx) = tokio::sync::watch::channel(None);
        let control = SourceBarrierControl::new(injector, release_tx);
        let old = CheckpointAttempt::new(7, 70);
        let newer = CheckpointAttempt::new(8, 80);

        control.release_exact(old);
        control.release_exact(newer);
        control.release_exact(old);

        assert_eq!(
            *release_rx.borrow(),
            Some(SourceBarrierSignal::Release(newer))
        );
        assert!(source_barrier_release_covers(newer, old));
        assert!(!source_barrier_release_covers(old, newer));

        let (equivocal_tx, equivocal_rx) = tokio::sync::watch::channel(None);
        let equivocal = SourceBarrierControl::new(CheckpointBarrierInjector::new(), equivocal_tx);
        let first = CheckpointAttempt::new(9, 90);
        let conflicting = CheckpointAttempt::new(9, 91);
        equivocal.release_exact(first);
        equivocal.release_exact(conflicting);
        assert_eq!(
            *equivocal_rx.borrow(),
            Some(SourceBarrierSignal::Release(first))
        );
        assert!(!source_barrier_release_covers(first, conflicting));

        for conflicting in [
            CheckpointAttempt::new(10, 90),
            CheckpointAttempt::new(10, 89),
            CheckpointAttempt::new(8, 91),
        ] {
            equivocal.release_exact(conflicting);
            assert_eq!(
                *equivocal_rx.borrow(),
                Some(SourceBarrierSignal::Release(first)),
                "a conflicting release must not overwrite the retained high-watermark"
            );
            assert!(!source_barrier_release_covers(conflicting, first));
            assert!(!source_barrier_release_covers(first, conflicting));
        }
    }

    #[test]
    fn stale_cancelled_barrier_does_not_fence_later_source_data() {
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::new(tokio::sync::Notify::new()),
            DeliveryGuarantee::ExactlyOnce,
            None,
        );
        coordinator
            .pending_barrier
            .reset(CheckpointAttempt::new(8, 80), 1);
        let mut callback = MockCallback::new();
        let mut barriers = Vec::new();
        let mut events = 0;

        coordinator
            .process_msg(
                SourceMsg::Barrier {
                    source_idx: 0,
                    barrier: CheckpointBarrier::new(70, 7),
                    checkpoint: checkpoint_at(7),
                },
                &mut callback,
                &mut barriers,
                &mut events,
            )
            .unwrap();
        assert!(barriers.is_empty());
        assert!(!coordinator.barrier_seen.contains(&0));

        coordinator
            .process_msg(
                SourceMsg::Batch {
                    source_idx: 0,
                    batch: int_batch(11),
                    checkpoint: checkpoint_at(8),
                },
                &mut callback,
                &mut barriers,
                &mut events,
            )
            .expect("data after a released stale barrier belongs to the open epoch");
        assert_eq!(
            coordinator
                .source_batches_buf
                .get("test_source")
                .map(Vec::len),
            Some(1)
        );
    }

    #[tokio::test]
    async fn ready_completion_does_not_drop_the_parked_intake_message() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);
        let attempt = CheckpointAttempt::new(7, 70);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        )
        .with_checkpoint_complete_rx(completion_rx);
        coordinator.parked_source_msg = Some(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(42),
            checkpoint: checkpoint_at(8),
        });
        completion_tx
            .send(CheckpointCompletion::new(attempt, FxHashMap::default()))
            .await
            .unwrap();

        let callback = MockCallback::new();
        let written_rows = Arc::clone(&callback.written_rows);
        let published = Arc::clone(&callback.published_barriers);
        let observed_rows = Arc::clone(&written_rows);
        let observed_published = Arc::clone(&published);
        let stop = tokio::spawn(async move {
            while observed_rows.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(observed_published.lock().as_slice(), &[(7, 70)]);
            shutdown.notify_one();
        });

        let exit = tokio::time::timeout(Duration::from_secs(2), coordinator.run(callback))
            .await
            .expect("parked message did not run after the higher-priority completion");
        stop.await.unwrap();
        drop(source_tx);
        drop(completion_tx);
        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(written_rows.load(Ordering::SeqCst), 1);
    }

    fn assignment_fence(
        version: u64,
        participants: &[u64],
    ) -> laminar_core::checkpoint::CheckpointAssignmentFence {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        let participants = participants
            .iter()
            .map(|node_id| CheckpointParticipant {
                node_id: *node_id,
                boot_incarnation: format!("00000000-0000-0000-0000-{node_id:012x}")
                    .parse()
                    .unwrap(),
            })
            .collect::<Vec<_>>();
        let owners = participants
            .iter()
            .map(|participant| participant.node_id)
            .collect::<Vec<_>>();
        CheckpointAssignmentFence::from_owner_map(version, &owners, participants).unwrap()
    }

    struct MockRuntimeState {
        leader: bool,
        recovering: bool,
        assignment_ready: bool,
    }

    /// Minimal mock callback for testing the coordinator loop.
    struct MockCallback {
        cycle_count: u32,
        attempt_to_reserve: CheckpointAttempt,
        reserve_error: Option<String>,
        reserve_calls: u64,
        control_checkpoint_calls: u64,
        control_checkpoint_call_audit: Arc<AtomicU64>,
        barrier_captures: Vec<(CheckpointAttempt, usize)>,
        runtime: MockRuntimeState,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        prepared_attempts: Vec<(
            CheckpointAttempt,
            Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        )>,
        prepare_error: Option<String>,
        checkpoint_order: Arc<Mutex<Vec<&'static str>>>,
        checkpoint_drain_error: Option<String>,
        abandon_error: Option<String>,
        abandoned_attempts: Arc<Mutex<Vec<(CheckpointAttempt, String)>>>,
        abandoned_fences:
            Arc<Mutex<Vec<Option<laminar_core::cluster::control::CheckpointAssignmentFence>>>>,
        checkpoint_failures: Vec<(u64, String)>,
        barrier_outcome: Option<BarrierOutcome>,
        results: Vec<FxHashMap<Arc<str>, Vec<RecordBatch>>>,
        watermark: i64,
        /// Fail on this 1-based cycle number.
        fatal_at_cycle: Option<u32>,
        /// Require recovery on this 1-based cycle number, independent of delivery guarantee.
        recovery_at_cycle: Option<u32>,
        /// Retain this cycle's batches and report a replay-preserving deferral once.
        defer_at_cycle: Option<u32>,
        retained_results: Option<FxHashMap<Arc<str>, Vec<RecordBatch>>>,
        cycle_input_rows: Arc<Mutex<Vec<usize>>>,
        cycle_errors: Arc<AtomicU64>,
        /// Whether a fatal cycle error should fault (exactly-once) vs drop-and-continue.
        fault_on_error: bool,
        /// Returned once by `take_pipeline_fault`.
        pipeline_fault: Option<String>,
        /// Exact downstream checkpoint identities published by async completions.
        published_barriers: Arc<Mutex<Vec<(u64, u64)>>>,
        publish_barrier_error: Arc<Mutex<Option<String>>>,
        publication_error: Arc<Mutex<Option<String>>>,
        written_rows: Arc<AtomicU64>,
        published_barriers_observed_at_close: Arc<AtomicU64>,
        invalidated_subscriptions: Arc<Mutex<Vec<String>>>,
        close_error: Option<String>,
        barrier_control_installed: Arc<AtomicBool>,
        intake_gate: Arc<AtomicBool>,
    }

    impl MockCallback {
        fn new() -> Self {
            Self {
                cycle_count: 0,
                attempt_to_reserve: CheckpointAttempt::new(1, 1),
                reserve_error: None,
                reserve_calls: 0,
                control_checkpoint_calls: 0,
                control_checkpoint_call_audit: Arc::new(AtomicU64::new(0)),
                barrier_captures: Vec::new(),
                runtime: MockRuntimeState {
                    leader: true,
                    recovering: false,
                    assignment_ready: true,
                },
                assignment_fence: None,
                prepared_attempts: Vec::new(),
                prepare_error: None,
                checkpoint_order: Arc::new(Mutex::new(Vec::new())),
                checkpoint_drain_error: None,
                abandon_error: None,
                abandoned_attempts: Arc::new(Mutex::new(Vec::new())),
                abandoned_fences: Arc::new(Mutex::new(Vec::new())),
                checkpoint_failures: Vec::new(),
                barrier_outcome: None,
                results: Vec::new(),
                watermark: 0,
                fatal_at_cycle: None,
                recovery_at_cycle: None,
                defer_at_cycle: None,
                retained_results: None,
                cycle_input_rows: Arc::new(Mutex::new(Vec::new())),
                cycle_errors: Arc::new(AtomicU64::new(0)),
                fault_on_error: false,
                pipeline_fault: None,
                published_barriers: Arc::new(Mutex::new(Vec::new())),
                publish_barrier_error: Arc::new(Mutex::new(None)),
                publication_error: Arc::new(Mutex::new(None)),
                written_rows: Arc::new(AtomicU64::new(0)),
                published_barriers_observed_at_close: Arc::new(AtomicU64::new(0)),
                invalidated_subscriptions: Arc::new(Mutex::new(Vec::new())),
                close_error: None,
                barrier_control_installed: Arc::new(AtomicBool::new(false)),
                intake_gate: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl PipelineCallback for MockCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            _watermark: i64,
        ) -> Result<CycleOutcome, CycleError> {
            self.cycle_count += 1;
            let input_rows = source_batches
                .values()
                .flat_map(|batches| batches.iter())
                .map(RecordBatch::num_rows)
                .sum();
            self.cycle_input_rows.lock().push(input_rows);
            if self.recovery_at_cycle == Some(self.cycle_count) {
                return Err(CycleError::Recovery(format!(
                    "injected recovery at cycle {}",
                    self.cycle_count
                )));
            }
            if self.fatal_at_cycle == Some(self.cycle_count) {
                return Err(CycleError::Fatal(format!(
                    "injected fatal at cycle {}",
                    self.cycle_count
                )));
            }
            if self.defer_at_cycle == Some(self.cycle_count) {
                self.retained_results = Some(
                    source_batches
                        .iter()
                        .map(|(name, batches)| (Arc::clone(name), batches.clone()))
                        .collect(),
                );
                let mut outcome = CycleOutcome::clean(FxHashMap::default());
                outcome.any_deferred = true;
                outcome.deferred_sources = source_batches.keys().cloned().collect();
                return Ok(outcome);
            }
            // Pass through source batches as results.
            let results: FxHashMap<Arc<str>, Vec<RecordBatch>> =
                self.retained_results.take().unwrap_or_else(|| {
                    source_batches
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                });
            self.results.push(results.clone());
            Ok(CycleOutcome::clean(results))
        }

        async fn drain_checkpoint_edges_until(
            &mut self,
            _deadline: tokio::time::Instant,
        ) -> Result<(), CycleError> {
            self.checkpoint_order.lock().push("drain");
            match self.checkpoint_drain_error.take() {
                Some(error) => Err(CycleError::Recovery(error)),
                None => Ok(()),
            }
        }

        fn note_cycle_error(&self) {
            self.cycle_errors.fetch_add(1, Ordering::SeqCst);
        }

        fn intake_paused(&self) -> bool {
            self.intake_gate.load(Ordering::Acquire)
        }

        fn fault_on_cycle_error(&self) -> bool {
            self.fault_on_error
        }

        fn take_pipeline_fault(&mut self) -> Option<String> {
            self.pipeline_fault.take()
        }

        fn is_leader(&self) -> bool {
            self.runtime.leader
        }

        fn is_recovering(&self) -> bool {
            self.runtime.recovering
        }

        async fn checkpoint_assignment_for_admission(
            &mut self,
        ) -> Result<Option<laminar_core::cluster::control::CheckpointAssignmentFence>, String>
        {
            if self.runtime.assignment_ready {
                Ok(self.assignment_fence.clone())
            } else {
                Err("assignment is not checkpoint-ready".into())
            }
        }

        async fn reserve_checkpoint_attempt(
            &mut self,
            _attempt_started: Instant,
        ) -> Result<CheckpointAttempt, String> {
            self.reserve_calls += 1;
            match self.reserve_error.take() {
                Some(error) => Err(error),
                None => Ok(self.attempt_to_reserve),
            }
        }

        async fn publish_checkpoint_prepare(
            &mut self,
            attempt: CheckpointAttempt,
            _attempt_started: Instant,
            assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        ) -> Result<(), String> {
            self.prepared_attempts.push((attempt, assignment_fence));
            match self.prepare_error.take() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }

        async fn abandon_checkpoint_attempt(
            &mut self,
            attempt: CheckpointAttempt,
            reason: &str,
            assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        ) -> Result<(), String> {
            self.checkpoint_order.lock().push("cleanup");
            self.abandoned_attempts
                .lock()
                .push((attempt, reason.to_owned()));
            self.abandoned_fences.lock().push(assignment_fence);
            match self.abandon_error.take() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }

        async fn cancel_source_barrier_attempt(
            &mut self,
            _attempt: CheckpointAttempt,
            _reason: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        fn record_checkpoint_failure(&mut self, checkpoint_id: u64, reason: &str) {
            self.checkpoint_failures
                .push((checkpoint_id, reason.to_owned()));
        }

        fn push_to_streams(
            &self,
            _results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        ) -> Result<(), CycleError> {
            match self.publication_error.lock().take() {
                Some(error) => Err(CycleError::Recovery(error)),
                None => Ok(()),
            }
        }
        async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
            let rows = results
                .values()
                .flat_map(|batches| batches.iter())
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            self.written_rows
                .fetch_add(u64::try_from(rows).unwrap(), Ordering::SeqCst);
        }

        fn extract_watermark(&mut self, _source_name: &str, batch: &RecordBatch) {
            // Use row count as a simple watermark proxy.
            #[allow(clippy::cast_possible_wrap)]
            {
                self.watermark += batch.num_rows() as i64;
            }
        }

        fn filter_late_rows(&self, _source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
            Some(batch.clone())
        }

        fn current_watermark(&self) -> i64 {
            self.watermark
        }

        fn publish_barrier(&self, attempt: CheckpointAttempt) -> Result<(), String> {
            if let Some(error) = self.publish_barrier_error.lock().take() {
                return Err(error);
            }
            self.published_barriers
                .lock()
                .push((attempt.epoch, attempt.checkpoint_id));
            Ok(())
        }

        async fn service_checkpoint_control(
            &mut self,
            _source_offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> CheckpointControlOutcome {
            self.control_checkpoint_calls += 1;
            self.control_checkpoint_call_audit
                .fetch_add(1, Ordering::SeqCst);
            CheckpointControlOutcome::Idle
        }

        async fn checkpoint_with_barrier(
            &mut self,
            source_checkpoints: FxHashMap<String, SourceCheckpoint>,
            attempt: CheckpointAttempt,
            _attempt_started: Instant,
            _assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        ) -> BarrierOutcome {
            self.checkpoint_order.lock().push("capture");
            self.barrier_captures
                .push((attempt, source_checkpoints.len()));
            self.barrier_outcome
                .take()
                .unwrap_or(BarrierOutcome::Committed(attempt.epoch))
        }

        fn record_cycle(&self, _events: u64, _batches: u64, _elapsed_ns: u64) {}
        fn apply_control(&mut self, _msg: crate::pipeline::ControlMsg) {}

        async fn close_sinks(&mut self) -> Result<(), String> {
            let published = self.published_barriers.lock().len();
            self.published_barriers_observed_at_close
                .store(u64::try_from(published).unwrap(), Ordering::SeqCst);
            match self.close_error.take() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }

        fn invalidate_subscriptions(&self, reason: &str) {
            self.invalidated_subscriptions
                .lock()
                .push(reason.to_owned());
        }

        fn set_barrier_injectors(&mut self, _injectors: Vec<SourceBarrierControl>) {
            self.barrier_control_installed
                .store(true, Ordering::Release);
        }
    }

    fn empty_source_fault_rx() -> tokio::sync::mpsc::UnboundedReceiver<SourceFault> {
        tokio::sync::mpsc::unbounded_channel().1
    }

    #[tokio::test]
    async fn coordinator_exit_invalidates_provisional_subscription_delivery() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );
        let callback = MockCallback::new();
        let invalidated = Arc::clone(&callback.invalidated_subscriptions);
        shutdown.notify_one();

        let exit = coordinator.run(callback).await;

        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(invalidated.lock().len(), 1);
        assert!(invalidated.lock()[0].contains("last committed progress frontier"));
    }

    /// Build a source-less coordinator over a direct channel (bypasses source spawning).
    fn test_coordinator(
        rx: SourceMsgRx,
        control_rx: ControlMsgRx,
        shutdown: Arc<tokio::sync::Notify>,
        delivery_guarantee: DeliveryGuarantee,
        checkpoint_interval: Option<Duration>,
    ) -> StreamingCoordinator {
        StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval,
                delivery_guarantee,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_fault_rx: empty_source_fault_rx(),
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown,
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            source_batches_buf: FxHashMap::default(),
            parked_source_msg: None,
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            replay_pending: false,
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        }
    }

    fn int_batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    fn checkpoint_at(position: u64) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("test_position", position.to_string());
        checkpoint
    }

    fn successful_checkpoint_result(
        attempt: CheckpointAttempt,
    ) -> crate::checkpoint_coordinator::CheckpointResult {
        crate::checkpoint_coordinator::CheckpointResult {
            success: true,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration: Duration::from_millis(1),
            error: None,
            failure_disposition: None,
        }
    }

    #[tokio::test]
    async fn runtime_ready_is_published_after_barrier_control_is_installed() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );
        let callback = MockCallback::new();
        let installed = Arc::clone(&callback.barrier_control_installed);
        let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();

        let run = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
        ready_rx
            .await
            .expect("coordinator must retain the startup sender")
            .expect("coordinator startup must succeed");
        assert!(
            installed.load(Ordering::Acquire),
            "ready was published before barrier control was installed"
        );

        shutdown.notify_one();
        assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
    }

    #[tokio::test]
    async fn source_less_runtime_stays_live_until_explicit_shutdown() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = StreamingCoordinator::new(
            Vec::new(),
            PipelineConfig::default(),
            Arc::clone(&shutdown),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("a source-less pipeline is valid");
        let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();

        let run = tokio::spawn(async move {
            coordinator
                .run_with_ready(MockCallback::new(), ready_tx)
                .await
        });
        ready_rx
            .await
            .expect("source-less coordinator retained readiness sender")
            .expect("source-less coordinator entered its control loop");
        tokio::task::yield_now().await;
        assert!(
            !run.is_finished(),
            "disconnected source channel stopped a valid source-less runtime"
        );

        shutdown.notify_one();
        assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
    }

    #[tokio::test]
    async fn configured_source_channel_exhaustion_is_a_fault() {
        let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        drop(source_tx);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::new(tokio::sync::Notify::new()),
            DeliveryGuarantee::BestEffort,
            None,
        );

        let exit = coordinator.run(MockCallback::new()).await;
        assert!(
            matches!(exit, ExitReason::Fault(ref reason)
                if reason.contains("all configured source tasks exited unexpectedly")),
            "configured-source exhaustion was reported as a clean stop: {exit:?}"
        );
    }

    #[tokio::test]
    async fn shutdown_drain_wakes_committer_and_waits_for_zero_exact_lag() {
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::new(tokio::sync::Notify::new()),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::from_secs(1)),
        );
        let pending = Arc::new(AtomicU64::new(1));
        let known = Arc::new(AtomicBool::new(true));
        let admission = crate::checkpoint_coordinator::CoordinatedCommitAdmission::for_test(
            Arc::clone(&pending),
            known,
            4,
        );
        let wake = admission.committer_wakeup_for_test();
        let progress = admission.progress_notify();
        coordinator.coordinated_commit_admission = Some(admission);

        let worker = tokio::spawn(async move {
            wake.notified().await;
            pending.store(0, Ordering::Release);
            progress.notify_one();
        });
        tokio::time::timeout(
            Duration::from_millis(250),
            coordinator.drain_coordinated_commits(),
        )
        .await
        .expect("shutdown drain should be event-driven")
        .expect("zero exact lag should complete the drain");
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn external_commit_hard_bound_backpressures_source_consumption() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            None,
        );
        let pending = Arc::new(AtomicU64::new(1));
        let known = Arc::new(AtomicBool::new(true));
        let admission = crate::checkpoint_coordinator::CoordinatedCommitAdmission::for_test(
            Arc::clone(&pending),
            known,
            1,
        );
        let progress = admission.progress_notify();
        coordinator.coordinated_commit_admission = Some(admission);

        let callback = MockCallback::new();
        let written_rows = Arc::clone(&callback.written_rows);
        let join = tokio::spawn(async move { coordinator.run(callback).await });
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(7),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            written_rows.load(Ordering::Acquire),
            0,
            "source data must remain queued while the external hard bound is closed"
        );

        pending.store(0, Ordering::Release);
        progress.notify_one();
        tokio::time::timeout(Duration::from_millis(500), async {
            while written_rows.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source consumption should resume on exact external progress");

        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("coordinator must shut down")
            .unwrap();
        assert!(matches!(exit, ExitReason::Shutdown));
    }

    #[tokio::test]
    async fn recovery_intake_gate_blocks_compute_and_discards_shutdown_open_epoch() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );
        let callback = MockCallback::new();
        let intake_gate = Arc::clone(&callback.intake_gate);
        let written_rows = Arc::clone(&callback.written_rows);
        intake_gate.store(true, Ordering::Release);
        let run = tokio::spawn(async move { coordinator.run(callback).await });

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(written_rows.load(Ordering::Acquire), 0);

        intake_gate.store(false, Ordering::Release);
        tokio::time::timeout(Duration::from_millis(500), async {
            while written_rows.load(Ordering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("compute did not resume after the intake fence opened");

        intake_gate.store(true, Ordering::Release);
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(2),
            checkpoint: checkpoint_at(2),
        })
        .await
        .unwrap();
        shutdown.notify_one();
        assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
        assert_eq!(
            written_rows.load(Ordering::Acquire),
            1,
            "a recovery-fenced shutdown must discard the open epoch"
        );
    }

    #[tokio::test]
    async fn intake_gate_close_after_receive_parks_fifo_message_until_reopen() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );
        coordinator.config.batch_window = Duration::from_millis(200);

        let callback = MockCallback::new();
        let intake_gate = Arc::clone(&callback.intake_gate);
        let written_rows = Arc::clone(&callback.written_rows);

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();
        let run = tokio::spawn(async move { coordinator.run(callback).await });

        // Capacity one makes completion of this send proof that the coordinator removed the
        // first message from the FIFO and is inside its batch window.
        tokio::time::timeout(
            Duration::from_secs(1),
            tx.send(SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(2),
                checkpoint: checkpoint_at(2),
            }),
        )
        .await
        .expect("coordinator did not receive the first FIFO message")
        .unwrap();
        intake_gate.store(true, Ordering::Release);

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            written_rows.load(Ordering::Acquire),
            0,
            "a message received just before gate closure must remain unexecuted"
        );

        intake_gate.store(false, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while written_rows.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("parked and queued FIFO messages did not resume after gate reopen");

        shutdown.notify_one();
        assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
    }

    #[tokio::test]
    async fn source_fault_bypasses_external_commit_data_backpressure() {
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::new(tokio::sync::Notify::new()),
            DeliveryGuarantee::ExactlyOnce,
            None,
        );
        let pending = Arc::new(AtomicU64::new(1));
        coordinator.coordinated_commit_admission = Some(
            crate::checkpoint_coordinator::CoordinatedCommitAdmission::for_test(
                pending,
                Arc::new(AtomicBool::new(true)),
                1,
            ),
        );
        let (fault_tx, fault_rx) = tokio::sync::mpsc::unbounded_channel();
        coordinator.source_fault_rx = fault_rx;

        fault_tx
            .send(SourceFault {
                source: Arc::from("faulted-source"),
                error: "injected control-plane fault".into(),
            })
            .unwrap();
        let exit =
            tokio::time::timeout(Duration::from_secs(1), coordinator.run(MockCallback::new()))
                .await
                .expect("source fault was hidden behind external-commit backpressure");
        assert!(matches!(exit, ExitReason::Fault(ref reason)
                if reason.contains("injected control-plane fault")));
    }

    fn checkpoint_source_handle(
        name: &str,
    ) -> (SourceHandle, laminar_core::checkpoint::BarrierPollHandle) {
        let barrier_injector = CheckpointBarrierInjector::new();
        let barrier_handle = barrier_injector.handle();
        let (epoch_committed_tx, _epoch_committed_rx) = tokio::sync::watch::channel(None);
        let (barrier_release_tx, _barrier_release_rx) = tokio::sync::watch::channel(None);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let expected_shutdown = Arc::new(AtomicBool::new(false));
        let task = SourceTaskLease::supervise(
            Arc::from(name),
            ConnectorCancellationPolicy::CancelSafe,
            shutdown,
            expected_shutdown,
            tokio::spawn(async {}),
            &tokio::runtime::Handle::current(),
        );
        (
            SourceHandle {
                recovery_cursor: true,
                task,
                startup_activation: None,
                barrier_injector,
                barrier_release_tx,
                epoch_committed_tx,
            },
            barrier_handle,
        )
    }

    fn admission_coordinator(source_handles: Vec<SourceHandle>) -> StreamingCoordinator {
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let source_names = source_handles
            .iter()
            .map(|handle| Arc::clone(&handle.task.state.name))
            .collect::<Vec<_>>();
        let source_count = source_handles.len();
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::new(tokio::sync::Notify::new()),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::ZERO),
        );
        coordinator.source_handles = source_handles;
        coordinator.source_names = source_names;
        coordinator.committed_offsets = vec![None; source_count];
        coordinator.pending_offsets = vec![None; source_count];
        coordinator
    }

    #[test]
    fn durable_completion_publication_requires_strict_identity_progress() {
        let mut coordinator = admission_coordinator(Vec::new());
        let mut callback = MockCallback::new();
        let newer = CheckpointAttempt::new(12, 120);

        let completion = CheckpointCompletion::validated(
            newer,
            successful_checkpoint_result(newer),
            FxHashMap::default(),
        )
        .unwrap();
        assert!(coordinator
            .handle_checkpoint_completion(completion, &mut callback)
            .is_none());

        for invalid in [
            CheckpointAttempt::new(13, 119),
            CheckpointAttempt::new(11, 121),
            CheckpointAttempt::new(12, 121),
            CheckpointAttempt::new(13, 120),
            CheckpointAttempt::new(12, 120),
            CheckpointAttempt::new(11, 119),
        ] {
            let completion = CheckpointCompletion::validated(
                invalid,
                successful_checkpoint_result(invalid),
                FxHashMap::default(),
            )
            .unwrap();
            let error = coordinator
                .handle_checkpoint_completion(completion, &mut callback)
                .expect("non-strict checkpoint identity progress must fault");
            assert!(error.contains("not strictly newer"), "{error}");
        }
        assert_eq!(*callback.published_barriers.lock(), vec![(12, 120)]);
        assert_eq!(coordinator.last_published_checkpoint, Some(newer));
    }

    #[tokio::test]
    async fn durable_completion_acks_sources_when_subscription_publication_fails() {
        let (source, _poll) = checkpoint_source_handle("source");
        let mut committed_rx = source.epoch_committed_tx.subscribe();
        let mut coordinator = admission_coordinator(vec![source]);
        let mut callback = MockCallback::new();
        *callback.publish_barrier_error.lock() = Some("injected publication failure".into());

        let attempt = CheckpointAttempt::new(13, 130);
        let mut checkpoint = checkpoint_at(attempt.epoch);
        checkpoint.set_offset("partition-0", "offset-13");
        let mut source_checkpoints = FxHashMap::default();
        source_checkpoints.insert("source".to_owned(), checkpoint.clone());
        let completion = CheckpointCompletion::validated(
            attempt,
            successful_checkpoint_result(attempt),
            source_checkpoints,
        )
        .unwrap();

        let error = coordinator
            .handle_checkpoint_completion(completion, &mut callback)
            .expect("publication failure must fault continuation");

        assert_eq!(error, "injected publication failure");
        assert_eq!(
            committed_rx.borrow_and_update().clone(),
            Some((attempt.epoch, checkpoint))
        );
        assert!(callback.checkpoint_failures.is_empty());
        assert_eq!(coordinator.last_published_checkpoint, Some(attempt));
    }

    #[test]
    fn checkpoint_admission_serializes_every_durable_tail() {
        let coordinator = admission_coordinator(Vec::new());
        assert!(coordinator.checkpoint_capacity_available());
        coordinator
            .checkpoint_in_flight
            .store(1, std::sync::atomic::Ordering::Release);
        assert!(!coordinator.checkpoint_capacity_available());
    }

    #[tokio::test]
    async fn sourced_pipeline_without_output_streams_has_one_periodic_barrier_path() {
        // `MockCallback` has no output-stream registrations. Admission must depend on the
        // coordinator's input handles, never on production callback stream publication.
        let (source, poll) = checkpoint_source_handle("input-only");
        let mut coordinator = admission_coordinator(vec![source]);
        let mut callback = MockCallback::new();
        let reserved = CheckpointAttempt::new(101, 10_001);
        callback.attempt_to_reserve = reserved;

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.control_checkpoint_calls, 0);
        assert_eq!(callback.reserve_calls, 1);
        assert!(callback.barrier_captures.is_empty());
        assert_eq!(
            poll.poll(),
            Some(CheckpointBarrier::new(
                reserved.checkpoint_id,
                reserved.epoch
            ))
        );
    }

    #[tokio::test]
    async fn coordinated_external_bound_defers_before_attempt_reservation() {
        let (source, poll) = checkpoint_source_handle("input-only");
        let mut coordinator = admission_coordinator(vec![source]);
        let pending = Arc::new(AtomicU64::new(0));
        let known = Arc::new(AtomicBool::new(false));
        coordinator.coordinated_commit_admission = Some(
            crate::checkpoint_coordinator::CoordinatedCommitAdmission::for_test(
                Arc::clone(&pending),
                Arc::clone(&known),
                2,
            ),
        );
        let mut callback = MockCallback::new();
        let reserved = CheckpointAttempt::new(101, 10_001);
        callback.attempt_to_reserve = reserved;

        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 0, "unknown cursor state must gate");
        assert_eq!(poll.poll(), None);

        known.store(true, Ordering::Release);
        pending.store(2, Ordering::Release);
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 0, "the exact cap must gate");
        assert_eq!(poll.poll(), None);

        pending.store(1, Ordering::Release);
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 1);
        assert_eq!(
            poll.poll(),
            Some(CheckpointBarrier::new(
                reserved.checkpoint_id,
                reserved.epoch
            ))
        );
    }

    #[tokio::test]
    async fn periodic_checkpoint_waits_for_recovery_and_exact_assignment_fence() {
        let mut coordinator = admission_coordinator(Vec::new());
        let mut callback = MockCallback::new();

        callback.runtime.recovering = true;
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 0);

        callback.runtime.recovering = false;
        callback.runtime.assignment_ready = false;
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 0);

        callback.runtime.assignment_ready = true;
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 1);
    }

    #[tokio::test]
    async fn prepare_publication_failure_prevents_source_cut_and_retains_exact_abort_fence() {
        let (source, poll) = checkpoint_source_handle("prepare-failure");
        let mut coordinator = admission_coordinator(vec![source]);
        coordinator.config.checkpoint_interval = Some(Duration::ZERO);
        let assignment_fence = assignment_fence(9, &[1, 2]);
        let attempt = CheckpointAttempt::new(107, 10_007);
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = attempt;
        callback.assignment_fence = Some(assignment_fence.clone());
        callback.prepare_error = Some("injected Prepare publication failure".into());

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(
            poll.poll(),
            None,
            "source cut must follow certified Prepare"
        );
        assert_eq!(
            callback.prepared_attempts,
            vec![(attempt, Some(assignment_fence.clone()))]
        );
        assert_eq!(
            callback.abandoned_fences.lock().as_slice(),
            &[Some(assignment_fence)]
        );
    }

    #[tokio::test]
    async fn manual_checkpoint_rejects_unready_assignment_without_burning_attempt() {
        let mut coordinator = admission_coordinator(Vec::new());
        coordinator.config.checkpoint_interval = None;
        let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointReply>(2);
        coordinator = coordinator.with_force_checkpoint_rx(force_rx);
        let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
        force_tx.send(reply_tx).await.unwrap();
        let mut callback = MockCallback::new();
        callback.runtime.assignment_ready = false;

        coordinator.maybe_checkpoint(&mut callback).await;

        let error = reply_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("LDB-6056"));
        assert_eq!(callback.reserve_calls, 0);
        assert!(coordinator.manual_waiting.is_empty());
        assert!(coordinator.manual_active.is_none());
    }

    #[tokio::test]
    async fn source_less_local_periodic_checkpoint_uses_exact_attempt_capture() {
        let mut coordinator = admission_coordinator(Vec::new());
        let mut callback = MockCallback::new();
        let reserved = CheckpointAttempt::new(102, 10_002);
        callback.attempt_to_reserve = reserved;

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.control_checkpoint_calls, 0);
        assert_eq!(callback.reserve_calls, 1);
        assert_eq!(callback.barrier_captures, vec![(reserved, 0)]);
        assert!(callback.abandoned_attempts.lock().is_empty());
        assert_eq!(
            *callback.published_barriers.lock(),
            vec![(reserved.epoch, reserved.checkpoint_id)]
        );
    }

    #[tokio::test]
    async fn source_less_cluster_follower_never_originates_checkpoint_attempt() {
        let mut coordinator = admission_coordinator(Vec::new());
        let mut callback = MockCallback::new();
        callback.runtime.leader = false;

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.control_checkpoint_calls, 1);
        assert_eq!(callback.reserve_calls, 0);
        assert!(callback.barrier_captures.is_empty());
    }

    #[tokio::test]
    async fn cluster_follower_rejects_manual_checkpoint_instead_of_stranding_caller() {
        let mut coordinator = admission_coordinator(Vec::new());
        coordinator.config.checkpoint_interval = None;
        let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointReply>(2);
        coordinator = coordinator.with_force_checkpoint_rx(force_rx);
        let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
        force_tx.send(reply_tx).await.unwrap();
        let mut callback = MockCallback::new();
        callback.runtime.leader = false;

        coordinator.maybe_checkpoint(&mut callback).await;

        let reply = tokio::time::timeout(Duration::from_secs(1), reply_rx)
            .await
            .expect("follower must answer the manual caller")
            .unwrap();
        let error = reply.expect_err("a follower cannot originate a checkpoint");
        assert!(error.to_string().contains("only the cluster leader"));
        assert_eq!(callback.reserve_calls, 0);
        assert_eq!(callback.control_checkpoint_calls, 1);
        assert!(coordinator.manual_waiting.is_empty());
    }

    #[tokio::test]
    async fn busy_source_injector_preflight_does_not_reserve_an_attempt() {
        let (busy_source, busy_poll) = checkpoint_source_handle("busy");
        let (idle_source, idle_poll) = checkpoint_source_handle("idle");
        let already_pending = CheckpointBarrier::new(71, 7);
        assert!(busy_source.barrier_injector.trigger(already_pending));

        let mut coordinator = admission_coordinator(vec![busy_source, idle_source]);
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = CheckpointAttempt::new(42, 9_001);

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(
            callback.reserve_calls, 0,
            "preflight must not burn a durable checkpoint ID while any injector is busy"
        );
        assert!(!coordinator.pending_barrier.active);
        assert_eq!(busy_poll.poll(), Some(already_pending));
        assert_eq!(idle_poll.poll(), None);
    }

    #[tokio::test]
    async fn admitted_checkpoint_injects_the_exact_durably_reserved_attempt() {
        let (source_0, poll_0) = checkpoint_source_handle("source-0");
        let (source_1, poll_1) = checkpoint_source_handle("source-1");
        let mut coordinator = admission_coordinator(vec![source_0, source_1]);
        let mut callback = MockCallback::new();
        let reserved = CheckpointAttempt::new(37, u64::from(u32::MAX) + 8_192);
        callback.attempt_to_reserve = reserved;

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.reserve_calls, 1);
        assert_eq!(coordinator.pending_barrier.attempt, Some(reserved));
        assert!(coordinator.pending_barrier.active);
        for injected in [poll_0.poll(), poll_1.poll()] {
            let injected = injected.expect("every preflighted source must receive the barrier");
            assert_eq!(injected.epoch, reserved.epoch);
            assert_eq!(injected.checkpoint_id, reserved.checkpoint_id);
        }
    }

    #[tokio::test]
    async fn ephemeral_source_aligns_without_publishing_a_recovery_cursor() {
        let (mut source, _poll) = checkpoint_source_handle("local-ingress");
        source.recovery_cursor = false;
        let mut coordinator = admission_coordinator(vec![source]);
        let mut local_progress = SourceCheckpoint::new();
        local_progress.set_offset("records_polled", "41");
        coordinator.committed_offsets[0] = Some(local_progress.clone());

        assert!(
            coordinator.current_source_offsets().is_empty(),
            "follower readiness must not publish non-replayable local progress"
        );

        let attempt = CheckpointAttempt::new(42, 9_042);
        coordinator.pending_barrier.reset(attempt, 1);
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
        let mut callback = MockCallback::new();
        coordinator
            .handle_barrier(0, &barrier, &local_progress, &mut callback)
            .await
            .unwrap();

        assert_eq!(
            callback.barrier_captures,
            vec![(attempt, 0)],
            "an ephemeral source must align the state cut without entering its manifest"
        );
    }

    #[tokio::test]
    async fn checkpoint_drain_precedes_capture_and_exact_source_release() {
        let (source, _poll) = checkpoint_source_handle("source");
        let release = source.barrier_release_tx.subscribe();
        let mut coordinator = admission_coordinator(vec![source]);
        let attempt = CheckpointAttempt::new(61, 9_061);
        coordinator.pending_barrier.reset(attempt, 1);
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
        let mut callback = MockCallback::new();
        let order = Arc::clone(&callback.checkpoint_order);

        coordinator
            .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
            .await
            .unwrap();

        assert_eq!(order.lock().as_slice(), &["drain", "capture"]);
        assert_eq!(
            *release.borrow(),
            Some(SourceBarrierSignal::Release(attempt))
        );
    }

    #[tokio::test]
    async fn checkpoint_drain_failure_cleans_up_and_keeps_source_held() {
        let (source, _poll) = checkpoint_source_handle("source");
        let release = source.barrier_release_tx.subscribe();
        let mut coordinator = admission_coordinator(vec![source]);
        let attempt = CheckpointAttempt::new(62, 9_062);
        coordinator.pending_barrier.reset(attempt, 1);
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
        let mut callback = MockCallback::new();
        callback.checkpoint_drain_error = Some("injected graph drain failure".into());
        let order = Arc::clone(&callback.checkpoint_order);

        let error = coordinator
            .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
            .await
            .unwrap_err();

        assert!(error.contains("injected graph drain failure"));
        assert_eq!(order.lock().as_slice(), &["drain", "cleanup"]);
        assert_eq!(*release.borrow(), None);
    }

    #[tokio::test]
    async fn checkpoint_cleanup_failure_keeps_source_held() {
        let (source, _poll) = checkpoint_source_handle("source");
        let release = source.barrier_release_tx.subscribe();
        let mut coordinator = admission_coordinator(vec![source]);
        let attempt = CheckpointAttempt::new(63, 9_063);
        coordinator.pending_barrier.reset(attempt, 1);
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
        let mut callback = MockCallback::new();
        callback.barrier_outcome = Some(BarrierOutcome::Failed);
        callback.abandon_error = Some("injected rollback failure".into());
        let order = Arc::clone(&callback.checkpoint_order);

        let error = coordinator
            .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
            .await
            .unwrap_err();

        assert!(error.contains("injected rollback failure"));
        assert_eq!(order.lock().as_slice(), &["drain", "capture", "cleanup"]);
        assert_eq!(*release.borrow(), None);
    }

    #[tokio::test]
    async fn mutable_capture_fault_keeps_source_held_for_recovery() {
        let (source, _poll) = checkpoint_source_handle("source");
        let release = source.barrier_release_tx.subscribe();
        let mut coordinator = admission_coordinator(vec![source]);
        let attempt = CheckpointAttempt::new(64, 9_064);
        coordinator.pending_barrier.reset(attempt, 1);
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
        let mut callback = MockCallback::new();
        callback.barrier_outcome = Some(BarrierOutcome::Failed);
        callback.pipeline_fault = Some(
            "operator state checkpoint capture failed; recovery from the last committed checkpoint is required"
                .into(),
        );
        let order = Arc::clone(&callback.checkpoint_order);

        let error = coordinator
            .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
            .await
            .unwrap_err();

        assert!(error.contains("recovery from the last committed checkpoint is required"));
        assert_eq!(order.lock().as_slice(), &["drain", "capture", "cleanup"]);
        assert_eq!(*release.borrow(), None);
    }

    #[tokio::test]
    async fn manual_requests_coalesce_onto_one_new_exact_source_barrier() {
        let (source, poll) = checkpoint_source_handle("manual-source");
        let mut coordinator = admission_coordinator(vec![source]);
        coordinator.config.checkpoint_interval = None;
        let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointReply>(8);
        coordinator = coordinator.with_force_checkpoint_rx(force_rx);

        let (first_sender, first_completion) = crossfire::oneshot::oneshot();
        let (second_sender, second_completion) = crossfire::oneshot::oneshot();
        force_tx.send(first_sender).await.unwrap();
        force_tx.send(second_sender).await.unwrap();

        let attempt = CheckpointAttempt::new(80, 8_080);
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = attempt;
        callback.barrier_outcome = Some(BarrierOutcome::Async);

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.reserve_calls, 1);
        let active = coordinator
            .manual_active
            .as_ref()
            .expect("manual callers must attach at admission");
        assert_eq!(active.attempt, attempt);
        assert_eq!(active.replies.len(), 2);
        assert!(coordinator.manual_waiting.is_empty());

        let barrier = poll
            .poll()
            .expect("manual attempt must inject a source barrier");
        assert_eq!(barrier, CheckpointBarrier::new(8_080, 80));
        coordinator
            .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
            .await
            .unwrap();
        assert!(coordinator.manual_active.is_some());

        let result = successful_checkpoint_result(attempt);
        let completion =
            CheckpointCompletion::validated(attempt, result.clone(), FxHashMap::default()).unwrap();
        assert!(coordinator
            .handle_checkpoint_completion(completion, &mut callback)
            .is_none());

        for reply in [first_completion, second_completion] {
            let completed = reply.await.unwrap().unwrap();
            assert_eq!(completed.epoch, attempt.epoch);
            assert_eq!(completed.checkpoint_id, attempt.checkpoint_id);
        }
        assert!(coordinator.manual_active.is_none());
    }

    #[tokio::test]
    async fn manual_reservation_failure_replies_instead_of_hanging() {
        let mut coordinator = admission_coordinator(Vec::new());
        coordinator.config.checkpoint_interval = None;
        let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointReply>(2);
        coordinator = coordinator.with_force_checkpoint_rx(force_rx);
        let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
        force_tx.send(reply_tx).await.unwrap();

        let mut callback = MockCallback::new();
        callback.reserve_error = Some("decision store unavailable".into());
        coordinator.maybe_checkpoint(&mut callback).await;

        let error = reply_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("decision store unavailable"));
        assert!(coordinator.manual_waiting.is_empty());
        assert!(coordinator.manual_active.is_none());
    }

    #[tokio::test]
    async fn manual_request_after_admission_waits_for_the_next_attempt() {
        let (source, poll) = checkpoint_source_handle("manual-source");
        let mut coordinator = admission_coordinator(vec![source]);
        coordinator.config.checkpoint_interval = None;
        let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointReply>(8);
        coordinator = coordinator.with_force_checkpoint_rx(force_rx);

        let first = CheckpointAttempt::new(81, 8_081);
        let second = CheckpointAttempt::new(82, 8_099);
        let (first_tx, first_rx) = crossfire::oneshot::oneshot();
        force_tx.send(first_tx).await.unwrap();
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = first;
        callback.barrier_outcome = Some(BarrierOutcome::Async);
        coordinator.maybe_checkpoint(&mut callback).await;
        let first_barrier = poll.poll().unwrap();

        let (second_tx, second_rx) = crossfire::oneshot::oneshot();
        force_tx.send(second_tx).await.unwrap();
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(coordinator.manual_waiting.len(), 1);
        assert_eq!(coordinator.manual_active.as_ref().unwrap().attempt, first);

        coordinator
            .handle_barrier(
                0,
                &first_barrier,
                &checkpoint_at(first.epoch),
                &mut callback,
            )
            .await
            .unwrap();
        assert!(coordinator
            .handle_checkpoint_completion(
                CheckpointCompletion::validated(
                    first,
                    successful_checkpoint_result(first),
                    FxHashMap::default(),
                )
                .unwrap(),
                &mut callback,
            )
            .is_none());
        assert_eq!(
            first_rx.await.unwrap().unwrap().checkpoint_id,
            first.checkpoint_id
        );
        assert_eq!(coordinator.manual_waiting.len(), 1);

        callback.attempt_to_reserve = second;
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(coordinator.manual_active.as_ref().unwrap().attempt, second);
        let second_barrier = poll.poll().unwrap();
        assert_eq!(second_barrier, CheckpointBarrier::new(8_099, 82));
        callback.barrier_outcome = Some(BarrierOutcome::Async);
        coordinator
            .handle_barrier(
                0,
                &second_barrier,
                &checkpoint_at(second.epoch),
                &mut callback,
            )
            .await
            .unwrap();

        assert!(coordinator
            .handle_checkpoint_completion(
                CheckpointCompletion::failed(second, "injected durable-tail failure"),
                &mut callback,
            )
            .is_none());
        let error = second_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("injected durable-tail failure"));
    }

    #[tokio::test]
    async fn skipped_or_failed_aligned_checkpoint_abandons_the_exact_attempt() {
        use super::super::callback::SkipReason;

        let outcomes = [
            (
                BarrierOutcome::Skipped(SkipReason::PreservingReplayWindowAfterSinkTimeout),
                "preserving_replay_window_after_sink_timeout",
                false,
            ),
            (
                BarrierOutcome::Failed,
                "barrier-aligned checkpoint failed before durable tail",
                true,
            ),
        ];

        for (outcome, expected_reason, records_failure) in outcomes {
            let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
            let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
            let mut coordinator = test_coordinator(
                rx,
                control_rx,
                Arc::new(tokio::sync::Notify::new()),
                DeliveryGuarantee::ExactlyOnce,
                None,
            );
            let attempt = CheckpointAttempt::new(53, 90_053);
            coordinator.pending_barrier.reset(attempt, 1);
            let mut callback = MockCallback::new();
            callback.barrier_outcome = Some(outcome);
            let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);

            coordinator
                .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
                .await
                .unwrap();

            let abandoned = callback.abandoned_attempts.lock();
            assert_eq!(abandoned.len(), 1);
            assert_eq!(abandoned[0].0, attempt);
            assert_eq!(abandoned[0].1, expected_reason);
            assert_eq!(
                callback.checkpoint_failures.len(),
                usize::from(records_failure)
            );
            if records_failure {
                assert_eq!(callback.checkpoint_failures[0].0, attempt.checkpoint_id);
            }
        }
    }

    #[tokio::test]
    async fn alignment_timeout_abandons_the_exact_reserved_attempt() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            None,
        );
        coordinator.config.checkpoint_timeout = Duration::ZERO;
        let attempt = CheckpointAttempt::new(61, 600_061);
        coordinator.pending_barrier.reset(attempt, 2);
        coordinator.pending_barrier.sources_aligned.insert(0);

        let callback = MockCallback::new();
        let abandoned_attempts = Arc::clone(&callback.abandoned_attempts);
        let observed_abandoned_attempts = Arc::clone(&abandoned_attempts);
        let stop = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let observed = !observed_abandoned_attempts.lock().is_empty();
                    if observed {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("alignment timeout was not observed");
            shutdown.notify_one();
        });

        let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
            .await
            .expect("coordinator must stop after the timeout is observed");
        stop.await.expect("timeout observer must not panic");

        assert!(matches!(exit, ExitReason::Shutdown));
        let abandoned = abandoned_attempts.lock();
        assert_eq!(abandoned.len(), 1);
        assert_eq!(abandoned[0].0, attempt);
        assert_eq!(abandoned[0].1, "source barrier alignment timeout");
    }

    #[derive(Default)]
    struct StartupSourceState {
        open: AtomicBool,
        open_calls: AtomicU64,
        start_completions: AtomicU64,
        restore_calls: AtomicU64,
        close_calls: AtomicU64,
        poll_calls: AtomicU64,
    }

    struct StartupSource {
        state: Arc<StartupSourceState>,
        schema: Arc<Schema>,
        start_delay: Duration,
        fail_open: bool,
        fail_restore: bool,
        cancellation_policy: ConnectorCancellationPolicy,
    }

    #[derive(Default)]
    struct BarrierRetrySourceState {
        allow_capture: AtomicBool,
        block_checkpoint_ready: AtomicBool,
        emit_batch: AtomicBool,
        assignment_version: AtomicU64,
        capture_attempts: AtomicU64,
        successful_captures: AtomicU64,
        polls: AtomicU64,
    }

    struct BarrierRetrySource {
        state: Arc<BarrierRetrySourceState>,
    }

    #[derive(Default)]
    struct BarrierHoldProbeState {
        polls: AtomicU64,
        control_drives: AtomicU64,
        data_ready: Arc<tokio::sync::Notify>,
        #[cfg(feature = "cluster")]
        drain_begins: AtomicU64,
        #[cfg(feature = "cluster")]
        drain_finishes: AtomicU64,
    }

    struct BarrierHoldProbeSource {
        state: Arc<BarrierHoldProbeState>,
    }

    #[async_trait::async_trait]
    impl SourceConnector for BarrierHoldProbeSource {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            self.state.polls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::empty())
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
            Ok(true)
        }

        fn drive_control_plane(&mut self) {
            self.state.control_drives.fetch_add(1, Ordering::SeqCst);
        }

        fn data_ready_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
            Some(Arc::clone(&self.state.data_ready))
        }

        #[cfg(feature = "cluster")]
        fn begin_drain(
            &mut self,
            _request: &SourceDrainRequest,
            _deadline: tokio::time::Instant,
        ) -> Result<(), ConnectorError> {
            self.state.drain_begins.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        #[cfg(feature = "cluster")]
        fn poll_drain_ready(&mut self, _round: AssignmentDrainId) -> Result<bool, ConnectorError> {
            Ok(true)
        }

        #[cfg(feature = "cluster")]
        async fn finish_drain(
            &mut self,
            _resolution: SourceDrainResolution,
            _deadline: tokio::time::Instant,
        ) -> Result<(), ConnectorError> {
            self.state.drain_finishes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn source_drain_receipts_reject_stale_processes_and_duplicate_tasks() {
        let round = AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let receipt = SourceDrainReceipt {
            round,
            participant,
            source_task_incarnation: uuid::Uuid::from_u128(101),
        };
        validate_source_drain_receipts(round, participant, std::slice::from_ref(&receipt)).unwrap();
        assert!(validate_source_drain_receipts(
            round,
            participant,
            &[receipt.clone(), receipt.clone()]
        )
        .is_err());

        let mut stale = receipt;
        stale.participant.boot_incarnation = uuid::Uuid::from_u128(12);
        assert!(validate_source_drain_receipts(round, participant, &[stale]).is_err());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn source_drain_ready_is_retained_before_coordinator_subscription() {
        let mut connector = BarrierHoldProbeSource {
            state: Arc::new(BarrierHoldProbeState::default()),
        };
        let round = AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        let request = SourceDrainRequest::new(round).unwrap();
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let (command_tx, mut command_rx) =
            tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
        let (status_tx, status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        drop(status_rx);
        let control = SourceDrainLeaseControl {
            task_incarnation: uuid::Uuid::from_u128(101),
            command_tx,
            status_tx,
            wake: Arc::new(tokio::sync::Notify::new()),
        };
        control
            .command_tx
            .send(Some(SourceDrainCommand::Begin {
                request,
                participant,
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();

        let mut active = None;
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &control.status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();
        publish_source_drain_ready(&mut connector, &control, &mut active).unwrap();

        let status_rx = control.status_tx.subscribe();
        assert!(matches!(
            status_rx.borrow().clone(),
            SourceDrainTaskStatus::Ready(receipt)
                if receipt.round == round
                    && receipt.participant == participant
                    && receipt.source_task_incarnation == control.task_incarnation
                    && receipt.is_canonical()
        ));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn source_drain_deadlines_fail_before_provider_work() {
        let state = Arc::new(BarrierHoldProbeState::default());
        let mut connector = BarrierHoldProbeSource {
            state: Arc::clone(&state),
        };
        let round = AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        let request = SourceDrainRequest::new(round).unwrap();
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let (command_tx, mut command_rx) =
            tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
        let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        command_tx
            .send(Some(SourceDrainCommand::Begin {
                request: request.clone(),
                participant,
                deadline: tokio::time::Instant::now(),
            }))
            .unwrap();

        let error = apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &status_tx,
            &mut None,
            true,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ConnectorError::Internal(_)));
        assert_eq!(state.drain_begins.load(Ordering::SeqCst), 0);

        let mut active = Some(ActiveSourceDrain {
            request,
            participant,
            provider_drain: true,
            prepare_deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            ready: true,
            pending_resolution: Some(PendingSourceDrainResolution {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Abort,
                },
                deadline: tokio::time::Instant::now(),
            }),
        });
        let error = resolve_pending_source_drain(&mut connector, &status_tx, &mut active)
            .await
            .unwrap_err();
        assert!(matches!(error, ConnectorError::Internal(_)));
        assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 0);
        assert!(active.is_some());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(start_paused = true)]
    async fn ready_source_drain_outlives_prepare_deadline() {
        let state = Arc::new(BarrierHoldProbeState::default());
        let mut connector = BarrierHoldProbeSource {
            state: Arc::clone(&state),
        };
        let round = AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let (command_tx, mut command_rx) =
            tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
        let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        let control = SourceDrainLeaseControl {
            task_incarnation: uuid::Uuid::from_u128(101),
            command_tx,
            status_tx,
            wake: Arc::new(tokio::sync::Notify::new()),
        };
        control
            .command_tx
            .send(Some(SourceDrainCommand::Begin {
                request: SourceDrainRequest::new(round).unwrap(),
                participant,
                deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            }))
            .unwrap();

        let mut active = None;
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &control.status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();
        publish_source_drain_ready(&mut connector, &control, &mut active).unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;

        control
            .command_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Commit,
                },
                deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            }))
            .unwrap();
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &control.status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();

        assert!(active.is_none());
        assert_eq!(state.drain_begins.load(Ordering::SeqCst), 1);
        assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            control.status_tx.borrow().clone(),
            SourceDrainTaskStatus::Resolved {
                round: resolved,
                outcome: SourceDrainOutcome::Commit,
            } if resolved == round
        ));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(start_paused = true)]
    async fn source_drain_retries_cannot_extend_phase_deadlines() {
        let state = Arc::new(BarrierHoldProbeState::default());
        let mut connector = BarrierHoldProbeSource { state };
        let round = AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        let request = SourceDrainRequest::new(round).unwrap();
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let (command_tx, mut command_rx) =
            tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
        let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        let prepare_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        command_tx
            .send(Some(SourceDrainCommand::Begin {
                request: request.clone(),
                participant,
                deadline: prepare_deadline,
            }))
            .unwrap();
        let mut active = None;
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();
        command_tx
            .send(Some(SourceDrainCommand::Begin {
                request,
                participant,
                deadline: prepare_deadline + Duration::from_secs(10),
            }))
            .unwrap();
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();
        assert_eq!(active.as_ref().unwrap().prepare_deadline, prepare_deadline);

        let first_resolution_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let resolution = SourceDrainResolution {
            round,
            outcome: SourceDrainOutcome::Abort,
        };
        command_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution,
                deadline: first_resolution_deadline,
            }))
            .unwrap();
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();
        command_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution,
                deadline: first_resolution_deadline + Duration::from_secs(10),
            }))
            .unwrap();
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            active
                .as_ref()
                .unwrap()
                .pending_resolution
                .unwrap()
                .deadline,
            first_resolution_deadline
        );

        tokio::time::advance(Duration::from_secs(3)).await;
        let control = SourceDrainLeaseControl {
            task_incarnation: uuid::Uuid::from_u128(101),
            command_tx,
            status_tx: status_tx.clone(),
            wake: Arc::new(tokio::sync::Notify::new()),
        };
        publish_source_drain_ready(&mut connector, &control, &mut active).unwrap();
        let error = resolve_pending_source_drain(&mut connector, &status_tx, &mut active)
            .await
            .unwrap_err();
        assert!(matches!(error, ConnectorError::Internal(_)));
        assert!(active.is_some());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn ready_global_source_drain_holds_polling_but_still_emits_barriers() {
        let state = Arc::new(BarrierHoldProbeState::default());
        let source = SourceRegistration {
            name: "global-drain-probe".into(),
            connector: Box::new(BarrierHoldProbeSource {
                state: Arc::clone(&state),
            }),
            config: laminar_connectors::config::ConnectorConfig::new("global-drain-probe"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            assignment_scoped: false,
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let mut coordinator = StreamingCoordinator::new_with_source_registry(
            vec![source],
            PipelineConfig {
                fallback_poll_interval: Duration::from_millis(1),
                checkpoint_interval: None,
                ..PipelineConfig::default()
            },
            Arc::new(tokio::sync::Notify::new()),
            control_rx,
            Arc::new(AtomicBool::new(false)),
            Arc::new(parking_lot::Mutex::new(Vec::new())),
            true,
        )
        .await
        .unwrap();

        let task = coordinator.source_handles[0].task.clone();
        let drain = task
            .drain_control()
            .expect("every cluster source has drain control");
        let barrier_control = coordinator.source_handles[0].barrier_control();
        let barrier_injector = coordinator.source_handles[0].barrier_injector.clone();
        coordinator.source_handles[0]
            .startup_activation
            .take()
            .unwrap()
            .send(());
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.polls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source was not activated");

        let round = AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let mut status_rx = drain.status_tx.subscribe();
        drain
            .command_tx
            .send(Some(SourceDrainCommand::Begin {
                request: SourceDrainRequest::new(round).unwrap(),
                participant,
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();
        drain.wake.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    status_rx.borrow_and_update().clone(),
                    SourceDrainTaskStatus::Ready(receipt) if receipt.round == round
                ) {
                    break;
                }
                status_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("source did not publish its global cut");

        let polls_at_cut = state.polls.load(Ordering::SeqCst);
        let control_at_cut = state.control_drives.load(Ordering::SeqCst);
        for _ in 0..4 {
            state.data_ready.notify_one();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(
            state.polls.load(Ordering::SeqCst),
            polls_at_cut,
            "source polled data after publishing its global cut"
        );
        assert!(
            state.control_drives.load(Ordering::SeqCst) > control_at_cut,
            "held source stopped servicing its control plane"
        );

        let barrier = CheckpointBarrier::new(80, 8);
        assert!(barrier_injector.trigger(barrier));
        drain.wake.notify_one();
        let received = tokio::time::timeout(Duration::from_secs(2), coordinator.rx.recv())
            .await
            .expect("held source did not emit the checkpoint barrier")
            .unwrap();
        assert!(matches!(received, SourceMsg::Barrier { barrier: seen, .. } if seen == barrier));
        assert_eq!(state.polls.load(Ordering::SeqCst), polls_at_cut);

        barrier_control.release_exact(CheckpointAttempt::new(8, 80));
        drain
            .command_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Abort,
                },
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();
        drain.wake.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    status_rx.borrow_and_update().clone(),
                    SourceDrainTaskStatus::Resolved {
                        round: resolved,
                        outcome: SourceDrainOutcome::Abort,
                    } if resolved == round
                ) {
                    break;
                }
                status_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("source did not resolve the global cut");
        state.data_ready.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.polls.load(Ordering::SeqCst) == polls_at_cut {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source did not resume polling after drain resolution");

        task.mark_expected_shutdown();
        barrier_control.stop_hold();
        task.notify_shutdown();
        let handle = coordinator.source_handles.pop().unwrap();
        drop(handle.epoch_committed_tx);
        drop(handle.barrier_release_tx);
        assert!(
            task.wait_until(tokio::time::Instant::now() + Duration::from_secs(2))
                .await,
            "source task did not stop"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn local_runtime_rejects_assignment_scoped_sources() {
        let state = Arc::new(BarrierHoldProbeState::default());
        let source = SourceRegistration {
            name: "local-drain-probe".into(),
            connector: Box::new(BarrierHoldProbeSource {
                state: Arc::clone(&state),
            }),
            config: laminar_connectors::config::ConnectorConfig::new("local-drain-probe"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Splittable,
            ),
            assignment_scoped: true,
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let result = StreamingCoordinator::new(
            vec![source],
            PipelineConfig {
                fallback_poll_interval: Duration::from_millis(1),
                checkpoint_interval: None,
                ..PipelineConfig::default()
            },
            Arc::new(tokio::sync::Notify::new()),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("local runtime accepted an assignment-scoped source"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("database-owned cluster runtime"));
        assert_eq!(state.polls.load(Ordering::SeqCst), 0);
        assert_eq!(state.drain_begins.load(Ordering::SeqCst), 0);
        assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn idle_source_drain_resolution_accepts_only_a_reconciled_replacement_commit() {
        let state = Arc::new(BarrierHoldProbeState::default());
        let mut connector = BarrierHoldProbeSource {
            state: Arc::clone(&state),
        };
        let round = AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        let (command_tx, mut command_rx) =
            tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
        let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        command_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Abort,
                },
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();

        let mut active = None;
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();

        assert!(active.is_none());
        assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 0);
        assert!(matches!(
            status_tx.borrow().clone(),
            SourceDrainTaskStatus::Resolved {
                round: resolved,
                outcome: SourceDrainOutcome::Abort,
            } if resolved == round
        ));

        let commit_state = Arc::new(BarrierRetrySourceState::default());
        commit_state.allow_capture.store(true, Ordering::Release);
        commit_state
            .block_checkpoint_ready
            .store(true, Ordering::Release);
        commit_state
            .assignment_version
            .store(round.target_version, Ordering::Release);
        let mut commit_connector = BarrierRetrySource {
            state: Arc::clone(&commit_state),
        };
        let (commit_tx, mut commit_rx) =
            tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
        let (commit_status_tx, _commit_status_rx) =
            tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        commit_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Commit,
                },
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();
        let mut commit_active = None;
        apply_latest_source_drain_command(
            &mut commit_connector,
            &mut commit_rx,
            &commit_status_tx,
            &mut commit_active,
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            commit_status_tx.borrow().clone(),
            SourceDrainTaskStatus::Idle
        ));
        assert!(commit_rx.has_changed().unwrap());
        assert_eq!(commit_state.capture_attempts.load(Ordering::Acquire), 0);

        commit_state
            .block_checkpoint_ready
            .store(false, Ordering::Release);
        apply_latest_source_drain_command(
            &mut commit_connector,
            &mut commit_rx,
            &commit_status_tx,
            &mut commit_active,
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            commit_status_tx.borrow().clone(),
            SourceDrainTaskStatus::Resolved {
                round: resolved,
                outcome: SourceDrainOutcome::Commit,
            } if resolved == round
        ));

        commit_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Commit,
                },
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();
        apply_latest_source_drain_command(
            &mut commit_connector,
            &mut commit_rx,
            &commit_status_tx,
            &mut commit_active,
            true,
        )
        .await
        .unwrap();

        let wrong_state = Arc::new(BarrierRetrySourceState::default());
        wrong_state.allow_capture.store(true, Ordering::Release);
        wrong_state
            .assignment_version
            .store(round.predecessor_version, Ordering::Release);
        let mut wrong_connector = BarrierRetrySource { state: wrong_state };
        let (wrong_tx, mut wrong_rx) =
            tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
        let (wrong_status_tx, _wrong_status_rx) =
            tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        wrong_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Commit,
                },
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();
        let error = apply_latest_source_drain_command(
            &mut wrong_connector,
            &mut wrong_rx,
            &wrong_status_tx,
            &mut None,
            true,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ConnectorError::InvalidState { .. }));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn active_source_drain_abort_waits_for_the_fifo_cut() {
        let state = Arc::new(BarrierHoldProbeState::default());
        let mut connector = BarrierHoldProbeSource {
            state: Arc::clone(&state),
        };
        let round = AssignmentDrainId {
            predecessor_version: 7,
            target_version: 8,
            digest: [9; 32],
        };
        let request = SourceDrainRequest::new(round).unwrap();
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let (command_tx, mut command_rx) =
            tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
        let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        command_tx
            .send(Some(SourceDrainCommand::Begin {
                request,
                participant,
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();
        let mut active = None;
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();
        command_tx
            .send(Some(SourceDrainCommand::Resolve {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Abort,
                },
                deadline: tokio::time::Instant::now() + Duration::from_secs(2),
            }))
            .unwrap();
        apply_latest_source_drain_command(
            &mut connector,
            &mut command_rx,
            &status_tx,
            &mut active,
            true,
        )
        .await
        .unwrap();

        resolve_pending_source_drain(&mut connector, &status_tx, &mut active)
            .await
            .unwrap();
        assert!(active.is_some(), "abort must not resolve before Ready");
        assert!(matches!(
            status_tx.borrow().clone(),
            SourceDrainTaskStatus::Pausing(active_round) if active_round == round
        ));

        let control = SourceDrainLeaseControl {
            task_incarnation: uuid::Uuid::from_u128(101),
            command_tx,
            status_tx,
            wake: Arc::new(tokio::sync::Notify::new()),
        };
        publish_source_drain_ready(&mut connector, &control, &mut active).unwrap();
        resolve_pending_source_drain(&mut connector, &control.status_tx, &mut active)
            .await
            .unwrap();
        assert!(active.is_none());
        assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            control.status_tx.borrow().clone(),
            SourceDrainTaskStatus::Resolved {
                round: resolved,
                outcome: SourceDrainOutcome::Abort,
            } if resolved == round
        ));
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SourceConnector for BarrierRetrySource {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            self.state.polls.fetch_add(1, Ordering::SeqCst);
            if self.state.emit_batch.swap(false, Ordering::AcqRel) {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )]));
                let batch =
                    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
                        .unwrap();
                Ok(Some(SourceBatch::new(batch)))
            } else {
                Ok(None)
            }
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]))
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
            self.state.capture_attempts.fetch_add(1, Ordering::SeqCst);
            if !self.state.allow_capture.load(Ordering::Acquire) {
                return Ok(None);
            }
            self.state
                .successful_captures
                .fetch_add(1, Ordering::SeqCst);
            let mut checkpoint = SourceCheckpoint::new();
            if let Some(version) =
                std::num::NonZeroU64::new(self.state.assignment_version.load(Ordering::Acquire))
            {
                checkpoint.bind_assignment_version(version);
            }
            Ok(Some(checkpoint))
        }

        fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
            Ok(!self.state.block_checkpoint_ready.load(Ordering::Acquire))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SourceConnector for StartupSource {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            self.cancellation_policy
        }

        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            self.state.open_calls.fetch_add(1, Ordering::SeqCst);
            // Model a connector that acquired resources inside the atomic startup operation
            // before discovering that startup failed. The coordinator must still close it.
            self.state.open.store(true, Ordering::SeqCst);
            if !self.start_delay.is_zero() {
                tokio::time::sleep(self.start_delay).await;
            }
            if self.fail_open {
                return Err(ConnectorError::ConnectionFailed(
                    "injected open failure".into(),
                ));
            }

            self.state.start_completions.fetch_add(1, Ordering::SeqCst);

            if matches!(request.position, SourcePosition::Resume { .. }) {
                self.state.restore_calls.fetch_add(1, Ordering::SeqCst);
                if self.fail_restore {
                    return Err(ConnectorError::Internal(
                        "injected resume-position failure".into(),
                    ));
                }
            }

            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            self.state.poll_calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.state.close_calls.fetch_add(1, Ordering::SeqCst);
            self.state.open.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    enum RuntimeSourceFailure {
        TerminalPoll,
        CommitNotification,
        Panic,
    }

    #[derive(Default)]
    struct RuntimeSourceState {
        polls: AtomicU64,
        commit_notifications: AtomicU64,
        closes: AtomicU64,
    }

    struct RuntimeFailureSource {
        state: Arc<RuntimeSourceState>,
        failure: RuntimeSourceFailure,
    }

    #[derive(Default)]
    struct CancellationSafePollState {
        poll_calls: AtomicU64,
        cancelled_polls: AtomicU64,
        commit_notification_calls: AtomicU64,
        first_poll_started: tokio::sync::Notify,
        release_first_poll: tokio::sync::Notify,
    }

    struct PollCancellationGuard {
        state: Arc<CancellationSafePollState>,
        completed: bool,
    }

    impl Drop for PollCancellationGuard {
        fn drop(&mut self) {
            if !self.completed {
                self.state.cancelled_polls.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    struct CancellationSafePollSource {
        state: Arc<CancellationSafePollState>,
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SourceConnector for CancellationSafePollSource {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            let call = self.state.poll_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let mut guard = PollCancellationGuard {
                    state: Arc::clone(&self.state),
                    completed: false,
                };
                self.state.first_poll_started.notify_one();
                self.state.release_first_poll.notified().await;
                guard.completed = true;
            }
            Ok(None)
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::empty())
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        async fn notify_epoch_committed(
            &mut self,
            _epoch: u64,
            _checkpoint: &SourceCheckpoint,
        ) -> Result<(), ConnectorError> {
            self.state
                .commit_notification_calls
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SourceConnector for RuntimeFailureSource {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            self.state.polls.fetch_add(1, Ordering::SeqCst);
            match self.failure {
                RuntimeSourceFailure::TerminalPoll => Err(ConnectorError::Internal(
                    "injected terminal poll failure".into(),
                )),
                RuntimeSourceFailure::CommitNotification => Ok(None),
                RuntimeSourceFailure::Panic => panic!("injected source-task panic"),
            }
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::empty())
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        async fn notify_epoch_committed(
            &mut self,
            _epoch: u64,
            _checkpoint: &SourceCheckpoint,
        ) -> Result<(), ConnectorError> {
            self.state
                .commit_notifications
                .fetch_add(1, Ordering::SeqCst);
            match self.failure {
                RuntimeSourceFailure::CommitNotification => Err(ConnectorError::Internal(
                    "injected commit notification failure".into(),
                )),
                RuntimeSourceFailure::TerminalPoll | RuntimeSourceFailure::Panic => Ok(()),
            }
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.state.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn runtime_failure_coordinator(
        delivery_guarantee: DeliveryGuarantee,
        failure: RuntimeSourceFailure,
        state: Arc<RuntimeSourceState>,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> StreamingCoordinator {
        let source = SourceRegistration {
            name: "runtime-failure-source".into(),
            connector: Box::new(RuntimeFailureSource { state, failure }),
            config: laminar_connectors::config::ConnectorConfig::new("runtime-failure-test"),
            contract: laminar_connectors::generator::GeneratorSource::default()
                .contract(&laminar_connectors::config::ConnectorConfig::new(
                    "generator",
                ))
                .expect("static generator contract"),
            assignment_scoped: false,
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let config = PipelineConfig {
            delivery_guarantee,
            checkpoint_interval: Some(Duration::from_secs(60)),
            fallback_poll_interval: Duration::from_millis(1),
            ..PipelineConfig::default()
        };

        StreamingCoordinator::new(
            vec![source],
            config,
            shutdown,
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("runtime failure source must start")
    }

    async fn shut_down_after_observed(counter: &AtomicU64, shutdown: &tokio::sync::Notify) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while counter.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("injected source failure was not observed");
        shutdown.notify_one();
    }

    fn startup_source(
        name: &str,
        state: Arc<StartupSourceState>,
        fail_open: bool,
        fail_restore: bool,
        position: SourcePosition,
    ) -> SourceRegistration {
        startup_source_with_delay(
            name,
            state,
            fail_open,
            fail_restore,
            Duration::ZERO,
            position,
        )
    }

    fn startup_source_with_delay(
        name: &str,
        state: Arc<StartupSourceState>,
        fail_open: bool,
        fail_restore: bool,
        start_delay: Duration,
        position: SourcePosition,
    ) -> SourceRegistration {
        startup_source_with_policy(
            name,
            state,
            fail_open,
            fail_restore,
            start_delay,
            position,
            ConnectorCancellationPolicy::CancelSafe,
        )
    }

    fn startup_source_with_policy(
        name: &str,
        state: Arc<StartupSourceState>,
        fail_open: bool,
        fail_restore: bool,
        start_delay: Duration,
        position: SourcePosition,
        cancellation_policy: ConnectorCancellationPolicy,
    ) -> SourceRegistration {
        SourceRegistration {
            name: name.into(),
            connector: Box::new(StartupSource {
                state,
                schema: Arc::new(Schema::empty()),
                start_delay,
                fail_open,
                fail_restore,
                cancellation_policy,
            }),
            config: laminar_connectors::config::ConnectorConfig::new("startup-test"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            assignment_scoped: false,
            position,
        }
    }

    async fn startup_result(
        sources: Vec<SourceRegistration>,
    ) -> Result<StreamingCoordinator, DbError> {
        startup_result_with_config(sources, PipelineConfig::default()).await
    }

    async fn startup_result_with_config(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
    ) -> Result<StreamingCoordinator, DbError> {
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        StreamingCoordinator::new(
            sources,
            config,
            Arc::new(tokio::sync::Notify::new()),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }

    #[tokio::test]
    async fn uncertified_exact_source_is_rejected_before_start() {
        let state = Arc::new(StartupSourceState::default());
        let source = startup_source(
            "uncertified-exact-source",
            Arc::clone(&state),
            false,
            false,
            SourcePosition::Initial,
        );
        let result = startup_result_with_config(
            vec![source],
            PipelineConfig {
                delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
                checkpoint_interval: Some(Duration::from_secs(60)),
                ..PipelineConfig::default()
            },
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("an uncertified exact source must fail before connector startup"),
        };

        assert!(
            error
                .to_string()
                .contains(laminar_core::error_codes::EXACTLY_ONCE_SOURCE_UNCERTIFIED),
            "unexpected error: {error}"
        );
        assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
        assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
        assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn source_io_waits_for_the_runtime_ready_boundary() {
        let state = Arc::new(StartupSourceState::default());
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let source = startup_source(
            "activation-fenced",
            Arc::clone(&state),
            false,
            false,
            SourcePosition::Initial,
        );
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = StreamingCoordinator::new(
            vec![source],
            PipelineConfig::default(),
            Arc::clone(&shutdown),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            state.poll_calls.load(Ordering::SeqCst),
            0,
            "source polled before the compute loop published readiness"
        );

        let callback = MockCallback::new();
        let installed = Arc::clone(&callback.barrier_control_installed);
        let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
        let run = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
        ready_rx
            .await
            .expect("coordinator retained readiness sender")
            .expect("coordinator entered its control loop");
        assert!(installed.load(Ordering::Acquire));
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.poll_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source was not activated after readiness");

        shutdown.notify_one();
        assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
        assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn claimed_barrier_is_retained_while_source_cursor_is_unreconciled() {
        let state = Arc::new(BarrierRetrySourceState::default());
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let source = SourceRegistration {
            name: "barrier-retry".into(),
            connector: Box::new(BarrierRetrySource {
                state: Arc::clone(&state),
            }),
            config: laminar_connectors::config::ConnectorConfig::new("barrier-retry"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            assignment_scoped: false,
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            checkpoint_interval: None,
            ..PipelineConfig::default()
        };
        let coordinator = StreamingCoordinator::new(
            vec![source],
            config,
            Arc::clone(&shutdown),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let injector = coordinator.source_handles[0].barrier_injector.clone();
        let callback = MockCallback::new();
        let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
        let run = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
        ready_rx.await.unwrap().unwrap();

        assert!(injector.trigger(CheckpointBarrier::new(77, 7)));
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.capture_attempts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source did not claim the injected barrier");
        let polls_after_claim = state.polls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            state.polls.load(Ordering::SeqCst),
            polls_after_claim,
            "source polled data after claiming an unreconciled barrier"
        );

        state.allow_capture.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.successful_captures.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained barrier was not retried after reconciliation");
        assert!(state.capture_attempts.load(Ordering::SeqCst) >= 2);

        shutdown.notify_one();
        assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
    }

    #[test]
    fn source_checkpoint_scope_is_validated_before_publication() {
        let state = Arc::new(BarrierRetrySourceState::default());
        state.allow_capture.store(true, Ordering::Release);
        let source = BarrierRetrySource {
            state: Arc::clone(&state),
        };

        assert!(try_source_checkpoint(&source, false).unwrap().is_some());
        let error = try_source_checkpoint(&source, true).unwrap_err();
        assert!(error.to_string().contains("missing its assignment version"));

        state.assignment_version.store(7, Ordering::Release);
        assert_eq!(
            try_source_checkpoint(&source, true)
                .unwrap()
                .unwrap()
                .assignment_version()
                .map(|version| version.get()),
            Some(7)
        );
        let error = try_source_checkpoint(&source, false).unwrap_err();
        assert!(error
            .to_string()
            .contains("unexpectedly carries cluster assignment version 7"));
    }

    #[tokio::test]
    async fn emitted_barrier_holds_polling_until_an_applicable_release() {
        let state = Arc::new(BarrierHoldProbeState::default());
        let source = SourceRegistration {
            name: "barrier-hold-probe".into(),
            connector: Box::new(BarrierHoldProbeSource {
                state: Arc::clone(&state),
            }),
            config: laminar_connectors::config::ConnectorConfig::new("barrier-hold-probe"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            assignment_scoped: false,
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let mut coordinator = StreamingCoordinator::new(
            vec![source],
            PipelineConfig {
                fallback_poll_interval: Duration::from_secs(60),
                checkpoint_interval: None,
                ..PipelineConfig::default()
            },
            Arc::new(tokio::sync::Notify::new()),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let barrier = CheckpointBarrier::new(70, 7);
        let control = coordinator.source_handles[0].barrier_control();
        assert!(coordinator.source_handles[0]
            .barrier_injector
            .trigger(barrier));
        state.data_ready.notify_one();
        coordinator.source_handles[0]
            .startup_activation
            .take()
            .unwrap()
            .send(());

        let received = tokio::time::timeout(Duration::from_secs(2), coordinator.rx.recv())
            .await
            .expect("source did not emit the injected barrier")
            .unwrap();
        assert!(matches!(received, SourceMsg::Barrier { barrier: seen, .. } if seen == barrier));
        let polls_at_barrier = state.polls.load(Ordering::SeqCst);
        let control_before_stale = state.control_drives.load(Ordering::SeqCst);

        control.release_exact(CheckpointAttempt::new(6, 60));
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.control_drives.load(Ordering::SeqCst) <= control_before_stale {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("held source did not service its control plane");
        assert_eq!(
            state.polls.load(Ordering::SeqCst),
            polls_at_barrier,
            "a stale release resumed source polling"
        );

        control.release_exact(CheckpointAttempt::new(7, 70));
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.polls.load(Ordering::SeqCst) == polls_at_barrier {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("exact barrier release did not resume source polling");

        coordinator.source_handles[0].task.mark_expected_shutdown();
        control.stop_hold();
        coordinator.source_handles[0].task.notify_shutdown();
        let handle = coordinator.source_handles.pop().unwrap();
        drop(handle.epoch_committed_tx);
        drop(handle.barrier_release_tx);
        assert!(
            handle
                .task
                .wait_until(tokio::time::Instant::now() + Duration::from_secs(2))
                .await,
            "source task did not stop"
        );
    }

    #[tokio::test]
    async fn returned_batch_is_retained_while_source_cursor_is_unreconciled() {
        let state = Arc::new(BarrierRetrySourceState::default());
        state.emit_batch.store(true, Ordering::Release);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let source = SourceRegistration {
            name: "batch-retry".into(),
            connector: Box::new(BarrierRetrySource {
                state: Arc::clone(&state),
            }),
            config: laminar_connectors::config::ConnectorConfig::new("batch-retry"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            assignment_scoped: false,
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            checkpoint_interval: None,
            ..PipelineConfig::default()
        };
        let coordinator = StreamingCoordinator::new(
            vec![source],
            config,
            Arc::clone(&shutdown),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let callback = MockCallback::new();
        let written_rows = Arc::clone(&callback.written_rows);
        let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
        let run = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
        ready_rx.await.unwrap().unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while state.capture_attempts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("returned batch did not attempt cursor capture");
        let polls_after_batch = state.polls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(written_rows.load(Ordering::SeqCst), 0);
        assert_eq!(
            state.polls.load(Ordering::SeqCst),
            polls_after_batch,
            "source polled past a batch whose cursor was not reconciled"
        );

        state.allow_capture.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), async {
            while written_rows.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained batch was not delivered after cursor reconciliation");

        shutdown.notify_one();
        assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
    }

    #[tokio::test]
    async fn dropping_before_runtime_ready_closes_source_without_polling() {
        let state = Arc::new(StartupSourceState::default());
        let source = startup_source(
            "cancelled-before-activation",
            Arc::clone(&state),
            false,
            false,
            SourcePosition::Initial,
        );
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = StreamingCoordinator::new(
            vec![source],
            PipelineConfig::default(),
            Arc::new(tokio::sync::Notify::new()),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();

        drop(coordinator);
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.close_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source was not closed when startup ownership disappeared");
        assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
        assert!(!state.open.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn source_start_stage_uses_one_deadline_and_rolls_back_current_and_prior() {
        let prior = Arc::new(StartupSourceState::default());
        let current = Arc::new(StartupSourceState::default());
        let config = PipelineConfig {
            checkpoint_timeout: Duration::from_secs(10),
            ..PipelineConfig::default()
        };
        let result = startup_result_with_config(
            vec![
                startup_source_with_delay(
                    "prior",
                    Arc::clone(&prior),
                    false,
                    false,
                    Duration::from_secs(6),
                    SourcePosition::Initial,
                ),
                startup_source_with_delay(
                    "current",
                    Arc::clone(&current),
                    false,
                    false,
                    Duration::from_secs(6),
                    SourcePosition::Initial,
                ),
            ],
            config,
        )
        .await;

        let Err(error) = result else {
            panic!("the second source must consume the remaining shared startup budget");
        };
        assert!(
            matches!(error, DbError::Config(ref message)
                if message.contains("source 'current' start failed at initial position")
                    && message.contains("shared 10s source-start stage deadline")),
            "unexpected error: {error}"
        );
        assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(current.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(current.close_calls.load(Ordering::SeqCst), 1);
        assert!(!prior.open.load(Ordering::SeqCst));
        assert!(!current.open.load(Ordering::SeqCst));
        assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
        assert_eq!(current.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn expired_source_start_budget_never_polls_initial_start() {
        let state = Arc::new(StartupSourceState::default());
        let result = startup_result_with_config(
            vec![startup_source(
                "unattempted-initial",
                Arc::clone(&state),
                false,
                false,
                SourcePosition::Initial,
            )],
            PipelineConfig {
                checkpoint_timeout: Duration::ZERO,
                ..PipelineConfig::default()
            },
        )
        .await;

        assert!(
            matches!(result, Err(DbError::Config(ref message))
                if message.contains("source 'unattempted-initial' start was not attempted")),
            "an unattempted initial start must retain configuration-error classification"
        );
        assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
        assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
        assert_eq!(state.close_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn expired_source_start_budget_never_polls_resume_start() {
        let state = Arc::new(StartupSourceState::default());
        let attempt = CheckpointAttempt::new(9, 901);
        let result = startup_result_with_config(
            vec![startup_source(
                "unattempted-resume",
                Arc::clone(&state),
                false,
                false,
                SourcePosition::Resume {
                    attempt,
                    checkpoint: checkpoint_at(9),
                },
            )],
            PipelineConfig {
                checkpoint_timeout: Duration::ZERO,
                ..PipelineConfig::default()
            },
        )
        .await;

        assert!(
            matches!(result, Err(DbError::Checkpoint(ref message))
                if message.contains("[LDB-6003]")
                    && message.contains("source 'unattempted-resume' start was not attempted")
                    && message.contains("epoch=9 id=901")),
            "an unattempted resume must retain checkpoint-error classification"
        );
        assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
        assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
        assert_eq!(state.restore_calls.load(Ordering::SeqCst), 0);
        assert_eq!(state.close_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_unsafe_source_start_finishes_before_startup_rollback() {
        let state = Arc::new(StartupSourceState::default());
        let config = PipelineConfig {
            checkpoint_timeout: Duration::from_secs(10),
            ..PipelineConfig::default()
        };
        let started = tokio::time::Instant::now();
        let result = startup_result_with_config(
            vec![startup_source_with_policy(
                "complete-started",
                Arc::clone(&state),
                false,
                false,
                Duration::from_secs(12),
                SourcePosition::Initial,
                ConnectorCancellationPolicy::CompleteStarted,
            )],
            config,
        )
        .await;

        assert!(matches!(
            result,
            Err(DbError::Config(ref message))
                if message.contains("shared 10s source-start stage deadline")
        ));
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_secs(12)
        );
        assert_eq!(state.start_completions.load(Ordering::SeqCst), 1);
        assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
        assert!(!state.open.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn source_resume_failure_is_fatal_and_closes_all_started_sources() {
        let prior = Arc::new(StartupSourceState::default());
        let failing = Arc::new(StartupSourceState::default());
        let result = startup_result(vec![
            startup_source(
                "prior",
                Arc::clone(&prior),
                false,
                false,
                SourcePosition::Resume {
                    attempt: CheckpointAttempt::new(7, 7),
                    checkpoint: checkpoint_at(7),
                },
            ),
            startup_source(
                "failing",
                Arc::clone(&failing),
                false,
                true,
                SourcePosition::Resume {
                    attempt: CheckpointAttempt::new(7, 7),
                    checkpoint: checkpoint_at(7),
                },
            ),
        ])
        .await;

        let Err(err) = result else {
            panic!("source resume-position failure must abort startup");
        };
        assert!(
            matches!(err, DbError::Checkpoint(ref msg) if msg.contains("source 'failing' start failed while resuming exact checkpoint epoch=7 id=7")),
            "unexpected error: {err}"
        );
        assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.restore_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.restore_calls.load(Ordering::SeqCst), 1);
        assert!(!prior.open.load(Ordering::SeqCst));
        assert!(!failing.open.load(Ordering::SeqCst));
        assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
        assert_eq!(failing.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn source_initial_start_failure_closes_prior_and_partially_started_source() {
        let prior = Arc::new(StartupSourceState::default());
        let failing = Arc::new(StartupSourceState::default());
        let result = startup_result(vec![
            startup_source(
                "prior",
                Arc::clone(&prior),
                false,
                false,
                SourcePosition::Initial,
            ),
            startup_source(
                "failing",
                Arc::clone(&failing),
                true,
                false,
                SourcePosition::Initial,
            ),
        ])
        .await;

        let Err(err) = result else {
            panic!("source initial-start failure must abort startup");
        };
        assert!(
            matches!(err, DbError::Config(ref msg) if msg.contains("source 'failing' start failed at initial position")),
            "unexpected error: {err}"
        );
        assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.restore_calls.load(Ordering::SeqCst), 0);
        assert_eq!(failing.restore_calls.load(Ordering::SeqCst), 0);
        assert!(!prior.open.load(Ordering::SeqCst));
        assert!(!failing.open.load(Ordering::SeqCst));
        assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
        assert_eq!(failing.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn immediate_source_fault_is_ordered_after_runtime_ready() {
        let state = Arc::new(RuntimeSourceState::default());
        let coordinator = runtime_failure_coordinator(
            DeliveryGuarantee::AtLeastOnce,
            RuntimeSourceFailure::TerminalPoll,
            Arc::clone(&state),
            Arc::new(tokio::sync::Notify::new()),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            state.polls.load(Ordering::SeqCst),
            0,
            "terminal source fault was produced before runtime readiness"
        );

        let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
        let run = tokio::spawn(async move {
            coordinator
                .run_with_ready(MockCallback::new(), ready_tx)
                .await
        });
        ready_rx
            .await
            .expect("coordinator retained readiness sender")
            .expect("runtime readiness must precede source activation");
        let exit = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("terminal source fault was not observed")
            .unwrap();
        assert!(matches!(exit, ExitReason::Fault(ref reason)
                if reason.contains("terminal poll failure")));
    }

    #[tokio::test]
    async fn terminal_source_poll_failure_faults_all_delivery_modes() {
        for guarantee in [
            DeliveryGuarantee::BestEffort,
            DeliveryGuarantee::AtLeastOnce,
            DeliveryGuarantee::ExactlyOnce,
        ] {
            let state = Arc::new(RuntimeSourceState::default());
            let shutdown = Arc::new(tokio::sync::Notify::new());
            let coordinator = runtime_failure_coordinator(
                guarantee,
                RuntimeSourceFailure::TerminalPoll,
                Arc::clone(&state),
                shutdown,
            )
            .await;

            let exit =
                tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
                    .await
                    .expect("terminal source poll failure must stop the pipeline");

            assert!(
                matches!(exit, ExitReason::Fault(ref error) if error.contains("terminal poll failure")),
                "{guarantee} must not stay live after losing a configured source, got {exit:?}"
            );
            assert!(state.polls.load(Ordering::SeqCst) > 0);
            assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn source_commit_notification_failure_faults_replay_guaranteed_modes() {
        for guarantee in [
            DeliveryGuarantee::AtLeastOnce,
            DeliveryGuarantee::ExactlyOnce,
        ] {
            let state = Arc::new(RuntimeSourceState::default());
            let shutdown = Arc::new(tokio::sync::Notify::new());
            let coordinator = runtime_failure_coordinator(
                guarantee,
                RuntimeSourceFailure::CommitNotification,
                Arc::clone(&state),
                shutdown,
            )
            .await;
            coordinator.broadcast_epoch_committed(11, &FxHashMap::default());

            let exit =
                tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
                    .await
                    .expect("source commit-notification failure must stop the pipeline");

            assert!(
                matches!(exit, ExitReason::Fault(ref error) if error.contains("commit notification failed at epoch 11")),
                "{guarantee} must fault for recovery after commit notification fails, got {exit:?}"
            );
            assert_eq!(state.commit_notifications.load(Ordering::SeqCst), 1);
            assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn epoch_commit_waits_for_in_flight_poll_without_cancelling_it() {
        let state = Arc::new(CancellationSafePollState::default());
        let source = SourceRegistration {
            name: "cancellation-safe-source".into(),
            connector: Box::new(CancellationSafePollSource {
                state: Arc::clone(&state),
            }),
            config: laminar_connectors::config::ConnectorConfig::new("cancellation-safe-test"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            assignment_scoped: false,
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let config = PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_interval: Some(Duration::from_secs(60)),
            fallback_poll_interval: Duration::from_millis(1),
            ..PipelineConfig::default()
        };
        let coordinator = StreamingCoordinator::new(
            vec![source],
            config,
            Arc::clone(&shutdown),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let epoch_committed_tx = coordinator.source_handles[0].epoch_committed_tx.clone();
        let run = tokio::spawn(async move { coordinator.run(MockCallback::new()).await });

        tokio::time::timeout(Duration::from_secs(2), state.first_poll_started.notified())
            .await
            .expect("source never entered its first poll");
        epoch_committed_tx
            .send(Some((17, SourceCheckpoint::new())))
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(state.cancelled_polls.load(Ordering::SeqCst), 0);
        assert_eq!(
            state.commit_notification_calls.load(Ordering::SeqCst),
            0,
            "commit notification must wait for the connector borrow to return"
        );

        state.release_first_poll.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.commit_notification_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("commit notification was not applied after poll completion");
        assert_eq!(state.cancelled_polls.load(Ordering::SeqCst), 0);

        drop(epoch_committed_tx);
        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("coordinator must stop after shutdown")
            .unwrap();
        assert!(matches!(exit, ExitReason::Shutdown));
    }

    #[tokio::test]
    async fn best_effort_commit_notification_failure_does_not_claim_recovery() {
        let state = Arc::new(RuntimeSourceState::default());
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let coordinator = runtime_failure_coordinator(
            DeliveryGuarantee::BestEffort,
            RuntimeSourceFailure::CommitNotification,
            Arc::clone(&state),
            Arc::clone(&shutdown),
        )
        .await;
        coordinator.broadcast_epoch_committed(11, &FxHashMap::default());

        let run = coordinator.run(MockCallback::new());
        let stop = shut_down_after_observed(&state.commit_notifications, &shutdown);
        let (exit, ()) =
            tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(run, stop) })
                .await
                .expect("best-effort pipeline must stop cleanly after shutdown");

        assert!(
            matches!(exit, ExitReason::Shutdown),
            "an advisory commit failure must not claim replay in best-effort mode, got {exit:?}"
        );
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fatal_cycle_error_faults_exactly_once() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::from_secs(60)),
        );

        let mut callback = MockCallback::new();
        callback.fatal_at_cycle = Some(1);
        callback.fault_on_error = true; // exactly-once: a fatal cycle error must fault

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
            .await
            .expect("run() must return after a fatal cycle error");

        assert!(
            matches!(exit, ExitReason::Fault(_)),
            "exactly-once fatal cycle error must fault, got {exit:?}"
        );
        drop(tx);
    }

    #[tokio::test]
    async fn recovery_cycle_error_faults_best_effort() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            shutdown,
            DeliveryGuarantee::BestEffort,
            None,
        );
        let mut callback = MockCallback::new();
        callback.recovery_at_cycle = Some(1);

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("recovery error must stop best-effort execution");
        assert!(matches!(exit, ExitReason::Fault(ref error)
            if error.contains("injected recovery")));
    }

    #[tokio::test]
    async fn publication_failure_does_not_settle_offsets_or_write_sinks_and_faults_all_modes() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            shutdown,
            DeliveryGuarantee::AtLeastOnce,
            None,
        );
        coordinator.pending_offsets[0] = Some(checkpoint_at(7));
        let mut callback = MockCallback::new();
        *callback.publication_error.lock() = Some("injected subscription admission failure".into());
        let written_rows = Arc::clone(&callback.written_rows);
        let mut results = FxHashMap::default();
        results.insert(Arc::from("test_source"), vec![int_batch(1)]);

        let error = coordinator
            .publish_cycle_outputs(&mut callback, &CycleOutcome::clean(results))
            .await
            .expect_err("publication admission must fail closed");
        assert!(matches!(error, CycleError::Recovery(ref reason)
            if reason.contains("injected subscription admission failure")));
        assert!(coordinator.pending_offsets[0].is_none());
        assert!(coordinator.committed_offsets[0].is_none());
        assert_eq!(written_rows.load(Ordering::SeqCst), 0);

        for guarantee in [
            DeliveryGuarantee::BestEffort,
            DeliveryGuarantee::AtLeastOnce,
            DeliveryGuarantee::ExactlyOnce,
        ] {
            let shutdown = Arc::new(tokio::sync::Notify::new());
            let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
            let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
            let coordinator = test_coordinator(rx, control_rx, shutdown, guarantee, None);
            let callback = MockCallback::new();
            *callback.publication_error.lock() =
                Some("injected subscription admission failure".into());
            let written_rows = Arc::clone(&callback.written_rows);
            tx.send(SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(1),
                checkpoint: checkpoint_at(7),
            })
            .await
            .unwrap();

            let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
                .await
                .expect("publication failure must stop the pipeline");
            assert!(matches!(exit, ExitReason::Fault(ref reason)
                if reason.contains("injected subscription admission failure")));
            assert_eq!(written_rows.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn fatal_cycle_error_continues_at_least_once() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );

        let mut callback = MockCallback::new();
        callback.fatal_at_cycle = Some(1);
        let errors = Arc::clone(&callback.cycle_errors);

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_clone.notify_one();
        });

        let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
            .await
            .expect("run() must return on shutdown");

        assert!(
            matches!(exit, ExitReason::Shutdown),
            "at-least-once must not fault on a cycle error, got {exit:?}"
        );
        assert_eq!(
            errors.load(Ordering::SeqCst),
            1,
            "at-least-once must drop-and-continue and count the error"
        );
        drop(tx);
    }

    #[test]
    fn source_data_after_barrier_returns_invariant_fault() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::from_secs(60)),
        );
        coordinator.source_names = vec![Arc::from("src0")];
        coordinator.committed_offsets = vec![None];
        coordinator.pending_offsets = vec![None];
        let mut callback = MockCallback::new();
        let mut barriers = Vec::new();
        let mut events = 0u64;
        coordinator.barrier_seen.insert(0);
        let error = coordinator
            .process_msg(
                SourceMsg::Batch {
                    source_idx: 0,
                    batch: int_batch(99),
                    checkpoint: checkpoint_at(8),
                },
                &mut callback,
                &mut barriers,
                &mut events,
            )
            .expect_err("post-barrier data must fail closed");
        assert!(error.contains("without an exact release"));
    }

    /// CP-4: an exactly-once sink failure poisons the epoch and aborts its transaction; the
    /// coordinator must fault for recovery (via `take_pipeline_fault`) rather than continue and seal
    /// offsets past the dropped rows on the next checkpoint.
    #[tokio::test]
    async fn exactly_once_sink_fault_faults_pipeline() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::from_secs(60)),
        );

        let mut callback = MockCallback::new();
        callback.pipeline_fault = Some("sink 's' write error at epoch 1".to_string());

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
            .await
            .expect("run() must return after a sink fault");

        assert!(
            matches!(exit, ExitReason::Fault(_)),
            "an exactly-once sink fault must fault the pipeline, got {exit:?}"
        );
        drop(tx);
    }

    /// Test that the coordinator processes messages via direct mpsc channel.
    #[tokio::test]
    async fn test_coordinator_direct_channel() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);

        // Create coordinator directly (bypassing source spawning).
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_fault_rx: empty_source_fault_rx(),
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            source_batches_buf: FxHashMap::default(),
            parked_source_msg: None,
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            replay_pending: false,
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let callback = MockCallback::new();

        // Send a batch.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch,
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        // Signal shutdown after a brief delay.
        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_clone.notify_one();
        });

        // Run coordinator — it should process the batch and exit on shutdown.
        coordinator.run(callback).await;

        // The callback was consumed by run(), so we can't inspect it directly.
        // But the test proves: no panics, no deadlocks, clean shutdown.
    }

    /// An already-running blocking task ignores Tokio abort. The coordinator reaper stays bounded,
    /// but the lease must not report terminal completion until the blocking work actually exits.
    #[tokio::test]
    async fn shutdown_retains_source_lease_for_task_that_ignores_abort() {
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let task_release = Arc::clone(&release);
        let task_started = Arc::new(AtomicBool::new(false));
        let task_started_flag = Arc::clone(&task_started);
        let wedged = tokio::task::spawn_blocking(move || {
            task_started_flag.store(true, Ordering::Release);
            let (released, wake) = &*task_release;
            let mut released = released.lock();
            while !*released {
                wake.wait(&mut released);
            }
        });
        while !task_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        let lease = SourceTaskLease::supervise(
            Arc::from("wedged"),
            ConnectorCancellationPolicy::CancelSafe,
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(AtomicBool::new(false)),
            wedged,
            &tokio::runtime::Handle::current(),
        );

        // Always release the blocking worker before asserting so the test runtime can tear down.
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            StreamingCoordinator::reap_source_task(lease.clone()),
        )
        .await;
        assert!(!lease.is_finished());
        let (released, wake) = &*release;
        *released.lock() = true;
        wake.notify_all();

        result.expect("shutdown reaper awaited a task that ignores Tokio abort");
        assert!(
            lease
                .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
                .await,
            "source lease did not observe the blocking task's actual exit"
        );
    }

    #[tokio::test]
    async fn shutdown_does_not_abort_complete_started_source_task() {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let completed = Arc::new(AtomicBool::new(false));
        let task_completed = Arc::clone(&completed);
        let task = tokio::spawn(async move {
            let _ = release_rx.await;
            task_completed.store(true, Ordering::Release);
        });
        let lease = SourceTaskLease::supervise(
            Arc::from("complete-started"),
            ConnectorCancellationPolicy::CompleteStarted,
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(AtomicBool::new(false)),
            task,
            &tokio::runtime::Handle::current(),
        );

        let reaper = tokio::spawn(StreamingCoordinator::reap_source_task(lease));
        tokio::task::yield_now().await;
        assert!(!completed.load(Ordering::Acquire));

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), reaper)
            .await
            .expect("cancellation-unsafe source reaper did not observe terminal completion")
            .unwrap();
        assert!(completed.load(Ordering::Acquire));
    }

    #[test]
    fn completion_rejects_result_for_a_different_attempt() {
        let admitted = CheckpointAttempt::new(7, 42);
        let error = CheckpointCompletion::validated(
            admitted,
            crate::checkpoint_coordinator::CheckpointResult {
                success: true,
                checkpoint_id: 43,
                epoch: admitted.epoch,
                duration: Duration::ZERO,
                error: None,
                failure_disposition: None,
            },
            FxHashMap::default(),
        )
        .expect_err("a different durable checkpoint ID must be rejected");
        assert!(error.contains("identity mismatch"));
        assert!(error.contains("id=42"));
        assert!(error.contains("id=43"));
    }

    /// A burned durable ID makes checkpoint ID diverge from epoch. The async completion path
    /// must preserve that exact identity rather than reconstructing `checkpoint_id = epoch`.
    #[tokio::test]
    async fn async_completion_publishes_exact_burned_gap_id() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);

        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::BestEffort,
            None,
        )
        .with_checkpoint_complete_rx(completion_rx);
        let callback = MockCallback::new();
        let published = Arc::clone(&callback.published_barriers);
        let join = tokio::spawn(async move { coordinator.run(callback).await });

        let attempt = CheckpointAttempt::new(7, 42);
        completion_tx
            .send(CheckpointCompletion::new(attempt, FxHashMap::default()))
            .await
            .expect("completion receiver must be live");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let has_published = !published.lock().is_empty();
                if has_published {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("async completion was not published");

        shutdown.notify_one();
        drop(source_tx);
        drop(completion_tx);
        let _ = join.await.expect("coordinator task panicked");

        assert_eq!(
            published.lock().as_slice(),
            &[(attempt.epoch, attempt.checkpoint_id)]
        );
    }

    #[tokio::test]
    async fn one_source_task_panic_faults_while_its_peer_remains_connected() {
        let panic_state = Arc::new(RuntimeSourceState::default());
        let peer_state = Arc::new(StartupSourceState::default());
        let panic_source = SourceRegistration {
            name: "panic-source".into(),
            connector: Box::new(RuntimeFailureSource {
                state: Arc::clone(&panic_state),
                failure: RuntimeSourceFailure::Panic,
            }),
            config: laminar_connectors::config::ConnectorConfig::new("panic-source-test"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            assignment_scoped: false,
            position: SourcePosition::Initial,
        };
        let peer = startup_source(
            "live-peer",
            Arc::clone(&peer_state),
            false,
            false,
            SourcePosition::Initial,
        );
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = StreamingCoordinator::new(
            vec![panic_source, peer],
            PipelineConfig::default(),
            Arc::new(tokio::sync::Notify::new()),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();

        let exit =
            tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
                .await
                .expect("a single panicked source task was not supervised");
        assert!(
            matches!(exit, ExitReason::Fault(ref reason)
                if reason.contains("panic-source")
                    && reason.contains("without coordinator shutdown")),
            "panicked source was hidden by its live peer: {exit:?}"
        );
        assert_eq!(panic_state.polls.load(Ordering::SeqCst), 1);
        assert_eq!(peer_state.close_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn committed_cut_with_successor_failure_acks_then_faults_before_next_write() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);
        let (source, _barrier_poll) = checkpoint_source_handle("test_source");
        let committed_rx = source.epoch_committed_tx.subscribe();

        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            None,
        )
        .with_checkpoint_complete_rx(completion_rx);
        coordinator.source_handles = vec![source];

        let callback = MockCallback::new();
        let published = Arc::clone(&callback.published_barriers);
        let published_at_close = Arc::clone(&callback.published_barriers_observed_at_close);
        let written_rows = Arc::clone(&callback.written_rows);
        let attempt = CheckpointAttempt::new(11, 8_111);
        let mut result = successful_checkpoint_result(attempt);
        result.error = Some(
            "checkpoint 8111 epoch 11 committed, but successor sink epoch 12 failed to begin"
                .into(),
        );
        let mut source_checkpoints = FxHashMap::default();
        let mut source_checkpoint = checkpoint_at(attempt.epoch);
        source_checkpoint.set_offset("partition-0", "committed-11");
        source_checkpoints.insert("test_source".to_string(), source_checkpoint);

        // Make both branches ready before run starts. The completion branch is biased ahead of
        // source intake and must publish checkpoint N, then terminally fence the queued N+1 row.
        source_tx
            .send(SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(12),
                checkpoint: checkpoint_at(attempt.epoch + 1),
            })
            .await
            .unwrap();
        completion_tx
            .send(
                CheckpointCompletion::validated(attempt, result, source_checkpoints)
                    .expect("completion identity must match"),
            )
            .await
            .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("successor-open failure must terminate the pipeline");

        assert!(
            matches!(exit, ExitReason::Fault(ref error) if error.contains("successor sink epoch 12 failed to begin")),
            "pipeline must report the successor-open fault, got {exit:?}"
        );
        assert_eq!(
            published.lock().as_slice(),
            &[(attempt.epoch, attempt.checkpoint_id)],
            "the durable checkpoint must be published before faulting"
        );
        assert_eq!(
            published_at_close.load(Ordering::Acquire),
            1,
            "checkpoint acknowledgement must precede lifecycle teardown"
        );
        let committed = committed_rx
            .borrow()
            .clone()
            .expect("source must receive the durable checkpoint acknowledgement");
        assert_eq!(committed.0, attempt.epoch);
        assert_eq!(committed.1.get_offset("partition-0"), Some("committed-11"));
        assert_eq!(
            written_rows.load(Ordering::Acquire),
            0,
            "no successor-epoch row may reach a sink after begin_epoch failed"
        );
        drop(source_tx);
        drop(completion_tx);
    }

    /// Shutdown drains the open epoch but must not synthesize an unaligned final checkpoint.
    #[tokio::test]
    async fn shutdown_does_not_synthesize_final_checkpoint() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: Some(Duration::from_secs(60)),
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_fault_rx: empty_source_fault_rx(),
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            source_batches_buf: FxHashMap::default(),
            parked_source_msg: None,
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            replay_pending: false,
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let callback = MockCallback::new();
        let control_calls = Arc::clone(&callback.control_checkpoint_call_audit);
        let written_rows = Arc::clone(&callback.written_rows);

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch,
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("shutdown drain must terminate");

        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(written_rows.load(Ordering::SeqCst), 1);
        assert_eq!(
            control_calls.load(Ordering::SeqCst),
            0,
            "shutdown must not invoke checkpoint control or originate a final attempt"
        );
    }

    #[tokio::test]
    async fn shutdown_abandons_exact_pending_barrier_and_fails_manual_caller() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            Some(Duration::from_secs(60)),
        );
        let attempt = CheckpointAttempt::new(31, 9_031);
        coordinator.pending_barrier.reset(attempt, 1);
        let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
        coordinator.manual_active = Some(ManualCheckpointAttempt {
            attempt,
            replies: vec![reply_tx],
        });

        let callback = MockCallback::new();
        let abandoned = Arc::clone(&callback.abandoned_attempts);
        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("pending alignment cancellation must not stall shutdown");

        assert!(matches!(exit, ExitReason::Shutdown));
        {
            let audit = abandoned.lock();
            assert_eq!(audit.len(), 1);
            assert_eq!(audit[0].0, attempt);
            assert!(audit[0].1.contains("shutdown interrupted"));
        }
        let error = reply_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("shutdown interrupted"));
    }

    #[tokio::test]
    async fn shutdown_drain_ignores_barrier_and_processes_following_batch() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );

        let attempt = CheckpointAttempt::new(41, 9_041);
        source_tx
            .send(SourceMsg::Barrier {
                source_idx: 0,
                barrier: CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
                checkpoint: checkpoint_at(attempt.epoch),
            })
            .await
            .unwrap();
        source_tx
            .send(SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(7),
                checkpoint: checkpoint_at(attempt.epoch + 1),
            })
            .await
            .unwrap();

        let callback = MockCallback::new();
        let written_rows = Arc::clone(&callback.written_rows);
        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("a shutdown barrier must not requeue the following batch forever");

        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(written_rows.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_settles_async_tail_before_closing_sinks() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(8);
        let in_flight = Arc::new(AtomicU64::new(1));
        let staged_bytes = Arc::new(AtomicU64::new(0));
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        )
        .with_checkpoint_admission(Arc::clone(&in_flight), staged_bytes, u64::MAX)
        .with_checkpoint_complete_rx(completion_rx);

        let callback = MockCallback::new();
        let published = Arc::clone(&callback.published_barriers);
        let published_at_close = Arc::clone(&callback.published_barriers_observed_at_close);
        let attempt = CheckpointAttempt::new(51, 9_051);
        let tail_in_flight = Arc::clone(&in_flight);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            completion_tx
                .send(CheckpointCompletion::new(attempt, FxHashMap::default()))
                .await
                .unwrap();
            tail_in_flight.fetch_sub(1, Ordering::AcqRel);
        });

        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("shutdown must wait for the captured durable tail");

        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(
            published.lock().as_slice(),
            &[(attempt.epoch, attempt.checkpoint_id)]
        );
        assert_eq!(
            published_at_close.load(Ordering::SeqCst),
            1,
            "sink close raced the terminal completion"
        );
    }

    #[tokio::test]
    async fn replay_guarantee_faults_when_sink_shutdown_is_not_acknowledged() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );
        let mut callback = MockCallback::new();
        callback.fault_on_error = true;
        callback.close_error = Some("flush acknowledgement failed".to_string());

        shutdown.notify_one();
        let exit = coordinator.run(callback).await;
        let ExitReason::Fault(reason) = exit else {
            panic!("replay guarantee accepted an unacknowledged sink close");
        };
        assert!(reason.contains("flush acknowledgement failed"));
    }

    #[tokio::test]
    async fn best_effort_reports_sink_shutdown_failure_without_recovery_fault() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::BestEffort,
            None,
        );
        let mut callback = MockCallback::new();
        callback.close_error = Some("close acknowledgement failed".to_string());
        let errors = Arc::clone(&callback.cycle_errors);

        shutdown.notify_one();
        let exit = coordinator.run(callback).await;
        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(errors.load(Ordering::SeqCst), 1);
    }

    /// Test that post-barrier batches are excluded from the current cycle's
    /// `source_batches_buf` and deferred to the next cycle.
    #[tokio::test]
    #[allow(clippy::too_many_lines, clippy::similar_names)]
    async fn test_barrier_excludes_post_barrier_data() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));

        let (_control_tx2, control_rx2) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
        let mut coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx: mpsc::bounded_async::<SourceMsg>(64).1, // dummy, not used
            source_fault_rx: empty_source_fault_rx(),
            source_handles: Vec::new(),
            source_names: vec![Arc::from("s0"), Arc::from("s1")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            source_batches_buf: FxHashMap::default(),
            parked_source_msg: None,
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None, None],
            pending_offsets: vec![None, None],
            replay_pending: false,
            control_rx: control_rx2,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let mut callback = MockCallback::new();
        let mut barriers = Vec::new();
        let mut cycle_events: u64 = 0;
        coordinator
            .pending_barrier
            .reset(CheckpointAttempt::new(1, 1), 2);

        // Source 0: one pre-barrier batch, then the exact barrier hold begins.
        let batch_1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let barrier = CheckpointBarrier::new(1, 1);

        coordinator
            .process_msg(
                SourceMsg::Batch {
                    source_idx: 0,
                    batch: batch_1,
                    checkpoint: checkpoint_at(10),
                },
                &mut callback,
                &mut barriers,
                &mut cycle_events,
            )
            .unwrap();
        coordinator
            .process_msg(
                SourceMsg::Barrier {
                    source_idx: 0,
                    barrier,
                    checkpoint: checkpoint_at(10),
                },
                &mut callback,
                &mut barriers,
                &mut cycle_events,
            )
            .unwrap();
        // Source 1: batch(ts=1), barrier
        let batch_s1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        coordinator
            .process_msg(
                SourceMsg::Batch {
                    source_idx: 1,
                    batch: batch_s1,
                    checkpoint: checkpoint_at(5),
                },
                &mut callback,
                &mut barriers,
                &mut cycle_events,
            )
            .unwrap();
        coordinator
            .process_msg(
                SourceMsg::Barrier {
                    source_idx: 1,
                    barrier,
                    checkpoint: checkpoint_at(5),
                },
                &mut callback,
                &mut barriers,
                &mut cycle_events,
            )
            .unwrap();

        // Verify that only the pre-barrier data is staged for each source.
        let s0_batches = coordinator.source_batches_buf.get("s0").unwrap();
        assert_eq!(
            s0_batches.len(),
            1,
            "s0 should have exactly 1 pre-barrier batch"
        );
        let s0_col = s0_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(s0_col.value(0), 1, "s0 batch should contain ts=1");

        let s1_batches = coordinator.source_batches_buf.get("s1").unwrap();
        assert_eq!(s1_batches.len(), 1, "s1 should have exactly 1 batch");

        // Pending offsets stop at the barrier cut.
        assert_eq!(
            coordinator.pending_offsets[0]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("10"),
            "s0 pending offset should be the pre-barrier batch"
        );
        assert_eq!(
            coordinator.pending_offsets[1]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("5"),
            "s1 pending offset should be epoch 5"
        );
        // committed_offsets must still be None — no execute_cycle has run.
        assert!(
            coordinator.committed_offsets[0].is_none(),
            "s0 committed offset should be None before execute_cycle"
        );
        assert!(
            coordinator.committed_offsets[1].is_none(),
            "s1 committed offset should be None before execute_cycle"
        );

        // Simulate successful cycle → commit.
        coordinator.commit_pending_offsets();
        assert_eq!(
            coordinator.committed_offsets[0]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("10"),
            "s0 committed after cycle"
        );
        assert_eq!(
            coordinator.committed_offsets[1]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("5"),
            "s1 committed after cycle"
        );

        // Barriers should have both sources.
        assert_eq!(barriers.len(), 2, "should have barriers from both sources");
    }

    // A faulted domain's source offset is held back while a healthy sibling source commits.
    #[tokio::test]
    async fn test_settle_pending_offsets_holds_failed_source() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
        let mut coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx: mpsc::bounded_async::<SourceMsg>(64).1,
            source_fault_rx: empty_source_fault_rx(),
            source_handles: Vec::new(),
            source_names: vec![Arc::from("s0"), Arc::from("s1")],
            shutdown,
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            source_batches_buf: FxHashMap::default(),
            parked_source_msg: None,
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None, None],
            pending_offsets: vec![Some(checkpoint_at(10)), Some(checkpoint_at(20))],
            replay_pending: false,
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let mut failed: FxHashSet<Arc<str>> = FxHashSet::default();
        failed.insert(Arc::from("s0"));
        coordinator.settle_pending_offsets(&failed, &FxHashSet::default());

        assert!(
            coordinator.committed_offsets[0].is_none(),
            "faulted s0 must not commit"
        );
        assert!(
            coordinator.pending_offsets[0].is_none(),
            "faulted s0 staged offset is discarded for replay"
        );
        assert_eq!(
            coordinator.committed_offsets[1]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("20"),
            "healthy s1 commits and advances"
        );

        coordinator.committed_offsets = vec![None, None];
        coordinator.pending_offsets = vec![Some(checkpoint_at(10)), Some(checkpoint_at(20))];
        let failed = FxHashSet::default();
        let deferred = FxHashSet::from_iter([Arc::from("s0")]);
        coordinator.settle_pending_offsets(&failed, &deferred);
        assert!(coordinator.committed_offsets[0].is_none());
        assert_eq!(
            coordinator.pending_offsets[0]
                .as_ref()
                .and_then(|cp| cp.get_offset("test_position")),
            Some("10")
        );
        assert_eq!(
            coordinator.committed_offsets[1]
                .as_ref()
                .and_then(|cp| cp.get_offset("test_position")),
            Some("20")
        );

        coordinator.settle_pending_offsets(&failed, &FxHashSet::default());
        assert!(coordinator.pending_offsets[0].is_none());
        assert_eq!(
            coordinator.committed_offsets[0]
                .as_ref()
                .and_then(|cp| cp.get_offset("test_position")),
            Some("10")
        );
    }

    #[tokio::test]
    async fn quiet_source_deferral_retries_before_reading_another_message() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (source_tx, source_rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let coordinator = test_coordinator(
            source_rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );
        let mut callback = MockCallback::new();
        callback.defer_at_cycle = Some(1);
        let cycle_input_rows = Arc::clone(&callback.cycle_input_rows);
        let written_rows = Arc::clone(&callback.written_rows);

        source_tx
            .send(SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(7),
                checkpoint: checkpoint_at(10),
            })
            .await
            .unwrap();

        let stop = {
            let shutdown = Arc::clone(&shutdown);
            let written_rows = Arc::clone(&written_rows);
            tokio::spawn(async move {
                while written_rows.load(Ordering::Acquire) == 0 {
                    tokio::task::yield_now().await;
                }
                shutdown.notify_one();
            })
        };
        let exit = tokio::time::timeout(Duration::from_secs(2), coordinator.run(callback))
            .await
            .expect("deferred quiet-source input was not retried");
        stop.await.unwrap();

        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(written_rows.load(Ordering::Acquire), 1);
        assert_eq!(
            cycle_input_rows.lock().get(..2),
            Some(&[1, 0][..]),
            "the retry must use graph-retained input before another source drain"
        );
        drop(source_tx);
    }

    struct BackpressuredCallback {
        inner: MockCallback,
        cycle_count: Arc<std::sync::atomic::AtomicU32>,
        events_per_cycle: Arc<Mutex<Vec<u64>>>,
    }

    impl BackpressuredCallback {
        fn new(
            cycle_count: Arc<std::sync::atomic::AtomicU32>,
            events_per_cycle: Arc<Mutex<Vec<u64>>>,
        ) -> Self {
            Self {
                inner: MockCallback::new(),
                cycle_count,
                events_per_cycle,
            }
        }
    }

    impl PipelineCallback for BackpressuredCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            watermark: i64,
        ) -> Result<CycleOutcome, CycleError> {
            self.cycle_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let total: u64 = source_batches
                .values()
                .flat_map(|bs| bs.iter())
                .map(|b| b.num_rows() as u64)
                .sum();
            self.events_per_cycle.lock().push(total);
            self.inner.execute_cycle(source_batches, watermark).await
        }

        async fn drain_checkpoint_edges_until(
            &mut self,
            deadline: tokio::time::Instant,
        ) -> Result<(), CycleError> {
            self.inner.drain_checkpoint_edges_until(deadline).await
        }

        fn push_to_streams(
            &self,
            r: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        ) -> Result<(), CycleError> {
            self.inner.push_to_streams(r)
        }
        async fn write_to_sinks(&mut self, r: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
            self.inner.write_to_sinks(r).await;
        }
        fn extract_watermark(&mut self, s: &str, b: &RecordBatch) {
            self.inner.extract_watermark(s, b);
        }
        fn filter_late_rows(&self, s: &str, b: &RecordBatch) -> Option<RecordBatch> {
            self.inner.filter_late_rows(s, b)
        }
        fn current_watermark(&self) -> i64 {
            self.inner.current_watermark()
        }
        fn publish_barrier(&self, attempt: CheckpointAttempt) -> Result<(), String> {
            self.inner.publish_barrier(attempt)
        }
        async fn service_checkpoint_control(
            &mut self,
            offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> CheckpointControlOutcome {
            self.inner.service_checkpoint_control(offsets).await
        }
        async fn checkpoint_with_barrier(
            &mut self,
            cp: FxHashMap<String, SourceCheckpoint>,
            attempt: CheckpointAttempt,
            attempt_started: Instant,
            assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        ) -> BarrierOutcome {
            self.inner
                .checkpoint_with_barrier(cp, attempt, attempt_started, assignment_fence)
                .await
        }
        async fn reserve_checkpoint_attempt(
            &mut self,
            attempt_started: Instant,
        ) -> Result<CheckpointAttempt, String> {
            self.inner.reserve_checkpoint_attempt(attempt_started).await
        }
        async fn abandon_checkpoint_attempt(
            &mut self,
            attempt: CheckpointAttempt,
            reason: &str,
            assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        ) -> Result<(), String> {
            self.inner
                .abandon_checkpoint_attempt(attempt, reason, assignment_fence)
                .await
        }
        async fn cancel_source_barrier_attempt(
            &mut self,
            attempt: CheckpointAttempt,
            reason: &str,
        ) -> Result<(), String> {
            self.inner
                .cancel_source_barrier_attempt(attempt, reason)
                .await
        }
        fn record_cycle(&self, e: u64, b: u64, ns: u64) {
            self.inner.record_cycle(e, b, ns);
        }
        fn apply_control(&mut self, msg: crate::pipeline::ControlMsg) {
            self.inner.apply_control(msg);
        }

        fn is_backpressured(&self) -> bool {
            true // Always backpressured — drain loop should never fire.
        }
    }

    /// With `is_backpressured() == true`, the coordinator processes only
    /// the first wakeup message per cycle (no drain coalescing). With 5
    /// messages pre-loaded and `batch_window=0`, each cycle should see
    /// exactly 1 event, spread across multiple cycles.
    #[tokio::test]
    async fn test_drain_skip_under_backpressure() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_fault_rx: empty_source_fault_rx(),
            source_handles: Vec::new(),
            source_names: vec![Arc::from("src")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            source_batches_buf: FxHashMap::default(),
            parked_source_msg: None,
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            replay_pending: false,
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        // Pre-load 5 batches (1 row each).
        for i in 0..5 {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![i]))],
            )
            .unwrap();
            tx.send(SourceMsg::Batch {
                source_idx: 0,
                batch,
                checkpoint: checkpoint_at(u64::try_from(i).unwrap()),
            })
            .await
            .unwrap();
        }

        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            shutdown_clone.notify_one();
        });

        let cycle_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let events_per_cycle = Arc::new(Mutex::new(Vec::new()));
        let callback =
            BackpressuredCallback::new(Arc::clone(&cycle_count), Arc::clone(&events_per_cycle));
        coordinator.run(callback).await;

        let cycles = cycle_count.load(std::sync::atomic::Ordering::SeqCst);
        let epc = events_per_cycle.lock();
        let total: u64 = epc.iter().sum();

        // All 5 events must be processed (no data loss).
        assert_eq!(total, 5, "all events must be processed, got {total}");
        // Under backpressure each cycle gets only the wakeup message (1
        // event), so we need at least 5 cycles for 5 messages. Without
        // backpressure, cycle 1 would drain all 5 in one shot.
        assert!(
            cycles >= 5,
            "expected >=5 cycles (1 event each), got {cycles} cycles with events/cycle: {epc:?}"
        );
        // Each cycle sees at most 1 event (the wakeup message; drain skipped).
        for (i, &events) in epc.iter().enumerate() {
            assert!(
                events <= 1,
                "cycle {i} saw {events} events, expected <=1 under backpressure"
            );
        }
    }
}
