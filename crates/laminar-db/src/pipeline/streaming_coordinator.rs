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
use laminar_connectors::connector::SourceBatch;
use laminar_connectors::connector::{
    schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
    strip_source_row_positions, ConnectorCancellationPolicy, ConnectorTaskTracker,
    DeliveryGuarantee, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceRowPositionCapability, SourceStart, SOURCE_MUTATION_COLUMN,
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
type ForceCheckpointReply = crate::db::ForceCheckpointReply;

#[cfg(feature = "cluster")]
struct SourceProcessAuthority {
    controller: Arc<ClusterController>,
    lost: tokio_util::sync::CancellationToken,
    watcher_abort: Option<tokio::task::AbortHandle>,
}

#[cfg(feature = "cluster")]
impl Drop for SourceProcessAuthority {
    fn drop(&mut self) {
        if let Some(watcher) = &self.watcher_abort {
            watcher.abort();
        }
    }
}

/// Shared process-lease observation for every source task in one coordinator.
///
/// One watcher owns the deadline timer. Per-poll code selects only on the retained cancellation
/// token and rechecks the controller's monotonic deadline at publication boundaries.
#[cfg(feature = "cluster")]
impl SourceProcessAuthority {
    fn new(controller: Arc<ClusterController>) -> Arc<Self> {
        let lost = tokio_util::sync::CancellationToken::new();
        if !controller.process_lease_is_live() {
            lost.cancel();
        }
        let watcher_abort = if lost.is_cancelled() {
            None
        } else {
            let task_controller = Arc::clone(&controller);
            let task_lost = lost.clone();
            let watcher = tokio::spawn(async move {
                task_controller.wait_for_process_lease_loss().await;
                task_lost.cancel();
            });
            Some(watcher.abort_handle())
        };
        Arc::new(Self {
            controller,
            lost,
            watcher_abort,
        })
    }

    #[inline]
    fn is_live(&self) -> bool {
        !self.lost.is_cancelled() && self.controller.process_lease_is_live()
    }

    async fn cancelled(&self) {
        self.lost.cancelled().await;
    }
}

#[cfg(feature = "cluster")]
#[inline]
fn source_process_authority_is_live(authority: Option<&SourceProcessAuthority>) -> bool {
    authority.is_none_or(SourceProcessAuthority::is_live)
}

/// Wait for coordinator batching or idle work without letting process-lease loss remain hidden
/// behind the timer.
async fn wait_coordinator_delay(
    duration: Duration,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
) -> bool {
    #[cfg(feature = "cluster")]
    if let Some(authority) = process_authority {
        return tokio::select! {
            biased;
            () = authority.cancelled() => true,
            () = tokio::time::sleep(duration) => false,
        };
    }

    tokio::time::sleep(duration).await;
    false
}

/// Message from a source task to the coordinator; carries the [`SourceCheckpoint`]
/// captured at production time so no offset is checkpointed for unprocessed data.
enum SourceMsg {
    Batch {
        source_idx: usize,
        batch: RecordBatch,
        /// Committed to `committed_offsets` only after successful cycle publication.
        checkpoint: SourceCheckpoint,
    },
    Barrier {
        source_idx: usize,
        barrier: CheckpointBarrier,
        checkpoint: SourceCheckpoint,
    },
}

/// Publish into the bounded source FIFO without allowing backpressure to hide terminal shutdown or
/// process-lease loss. Process authority is revalidated after a successful send because its fence
/// may cross while the queue wakes the producer.
async fn send_source_msg(
    tx: &SourceMsgTx,
    msg: SourceMsg,
    shutdown: &tokio::sync::Notify,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
) -> bool {
    #[cfg(feature = "cluster")]
    if let Some(authority) = process_authority {
        if !authority.is_live() {
            return false;
        }
        let sent = tokio::select! {
            biased;
            () = authority.cancelled() => false,
            () = shutdown.notified() => false,
            result = tx.send(msg) => result.is_ok(),
        };
        return sent && authority.is_live();
    }

    tokio::select! {
        biased;
        () = shutdown.notified() => false,
        result = tx.send(msg) => result.is_ok(),
    }
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

struct SourceActorTerminalState {
    finished: AtomicBool,
    notify: tokio::sync::Notify,
}

impl SourceActorTerminalState {
    fn new() -> Self {
        Self {
            finished: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn finish(&self) {
        if !self.finished.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    async fn wait_finished(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_finished() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
struct SourceActorLifetime(Arc<SourceActorTerminalState>);

#[cfg(test)]
impl Drop for SourceActorLifetime {
    fn drop(&mut self) {
        self.0.finish();
    }
}

#[cfg(test)]
fn source_actor_terminal_guard() -> (SourceActorLifetime, Arc<SourceActorTerminalState>) {
    let terminal = Arc::new(SourceActorTerminalState::new());
    (SourceActorLifetime(Arc::clone(&terminal)), terminal)
}

struct SourceActorFuture<F> {
    actor: Option<std::pin::Pin<Box<F>>>,
    terminal: Arc<SourceActorTerminalState>,
}

// Moving this wrapper never moves the separately pinned actor allocation.
impl<F> Unpin for SourceActorFuture<F> {}

impl<F> std::future::Future for SourceActorFuture<F>
where
    F: std::future::Future<Output = ()>,
{
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let actor = self
            .actor
            .as_mut()
            .expect("source actor polled after terminal completion");
        if actor.as_mut().poll(context).is_pending() {
            return std::task::Poll::Pending;
        }
        // Drop the complete actor future, including its connector, before publishing exit.
        self.actor.take();
        self.terminal.finish();
        std::task::Poll::Ready(())
    }
}

impl<F> Drop for SourceActorFuture<F> {
    fn drop(&mut self) {
        // Cancellation must drop the actor and its connector before another generation can observe
        // terminal completion.
        self.actor.take();
        self.terminal.finish();
    }
}

fn spawn_source_actor<F>(
    runtime: &tokio::runtime::Handle,
    actor: F,
) -> (tokio::task::JoinHandle<()>, Arc<SourceActorTerminalState>)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let terminal = Arc::new(SourceActorTerminalState::new());
    let join = runtime.spawn(SourceActorFuture {
        actor: Some(Box::pin(actor)),
        terminal: Arc::clone(&terminal),
    });
    (join, terminal)
}

#[derive(Clone)]
enum SourceTaskOutcome {
    Success,
    Cancelled,
    Failed(Arc<str>),
}

struct SourceTaskState {
    name: Arc<str>,
    shutdown: Arc<tokio::sync::Notify>,
    expected_shutdown: Arc<AtomicBool>,
    abort: tokio::task::AbortHandle,
    actor_terminal: Arc<SourceActorTerminalState>,
    terminal_tasks: Option<ConnectorTaskTracker>,
    outcome: parking_lot::Mutex<Option<SourceTaskOutcome>>,
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
/// Actor exit is published by a wrapper that owns the connector future before spawn, and connector
/// children are observed through the exact tracker captured at construction. Neither proof depends
/// on the detached outcome supervisor continuing to run.
#[derive(Clone)]
pub(crate) struct SourceTaskLease {
    state: Arc<SourceTaskState>,
}

pub(crate) type OwnedSourceTasks = Arc<parking_lot::Mutex<Vec<SourceTaskLease>>>;

impl SourceTaskLease {
    fn supervise(
        name: Arc<str>,
        shutdown: Arc<tokio::sync::Notify>,
        expected_shutdown: Arc<AtomicBool>,
        join: tokio::task::JoinHandle<()>,
        actor_terminal: Arc<SourceActorTerminalState>,
        terminal_tasks: Option<ConnectorTaskTracker>,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        let state = Arc::new(SourceTaskState {
            name,
            shutdown,
            expected_shutdown,
            abort: join.abort_handle(),
            actor_terminal,
            terminal_tasks,
            outcome: parking_lot::Mutex::new(None),
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
        });
        // Tokio owns the scheduled outcome observer. Terminal replacement fencing is independent
        // of this detached task and comes from the actor wrapper plus connector tracker above.
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

    pub(crate) fn is_finished(&self) -> bool {
        self.state.actor_terminal.is_finished()
            && self
                .state
                .terminal_tasks
                .as_ref()
                .is_none_or(ConnectorTaskTracker::is_terminated)
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

    pub(crate) fn abort(&self) {
        self.state.abort.abort();
    }

    async fn wait_finished(&self) {
        let actor = self.state.actor_terminal.wait_finished();
        let connector_tasks = async {
            if let Some(tasks) = self.state.terminal_tasks.as_ref() {
                tasks.wait_terminated().await;
            }
        };
        tokio::join!(actor, connector_tasks);
    }

    pub(crate) async fn wait_until(&self, deadline: tokio::time::Instant) -> bool {
        if self.is_finished() {
            return true;
        }
        tokio::time::timeout_at(deadline, self.wait_finished())
            .await
            .is_ok()
            || self.is_finished()
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

/// Stable owner for source generations constructed through [`StreamingCoordinator::new`].
#[derive(Clone)]
pub struct StreamingCoordinatorRuntime {
    owned_source_tasks: OwnedSourceTasks,
    owned_connector_task_fences: OwnedConnectorTaskFences,
    construction: Arc<tokio::sync::Mutex<()>>,
    active_coordinator: Arc<AtomicBool>,
}

struct StreamingCoordinatorGeneration {
    runtime: StreamingCoordinatorRuntime,
}

impl Drop for StreamingCoordinatorGeneration {
    fn drop(&mut self) {
        self.runtime
            .active_coordinator
            .store(false, Ordering::Release);
    }
}

impl StreamingCoordinatorRuntime {
    /// Creates an empty runtime owner with no active source generation.
    #[must_use]
    pub fn new() -> Self {
        Self {
            owned_source_tasks: Arc::new(parking_lot::Mutex::new(Vec::new())),
            owned_connector_task_fences: Arc::new(parking_lot::Mutex::new(Vec::new())),
            construction: Arc::new(tokio::sync::Mutex::new(())),
            active_coordinator: Arc::new(AtomicBool::new(false)),
        }
    }

    fn claim_generation(&self) -> Result<StreamingCoordinatorGeneration, DbError> {
        self.active_coordinator
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                DbError::Pipeline(
                    "cannot construct a replacement streaming coordinator while the prior \
                     coordinator generation is still active"
                        .into(),
                )
            })?;
        Ok(StreamingCoordinatorGeneration {
            runtime: self.clone(),
        })
    }

    fn prune_and_require_idle(&self) -> Result<(), DbError> {
        let source_names = {
            let mut sources = self.owned_source_tasks.lock();
            sources.retain(|source| !source.is_finished());
            sources
                .iter()
                .map(|source| source.name().to_owned())
                .collect::<Vec<_>>()
        };
        let connector_names = {
            let mut connectors = self.owned_connector_task_fences.lock();
            connectors.retain(|connector| !connector.is_finished());
            connectors
                .iter()
                .map(|connector| connector.name().to_owned())
                .collect::<Vec<_>>()
        };
        if source_names.is_empty() && connector_names.is_empty() {
            return Ok(());
        }
        Err(DbError::Pipeline(format!(
            "cannot construct a replacement streaming coordinator while prior connector \
             generations remain unresolved: sources=[{}], connector_tasks=[{}]",
            source_names.join(", "),
            connector_names.join(", ")
        )))
    }
}

impl Default for StreamingCoordinatorRuntime {
    fn default() -> Self {
        Self::new()
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
        let task_finished = task.wait_finished();
        tokio::pin!(task_finished);
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
            let task_finished = task.wait_finished();
            tokio::pin!(task_finished);
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

#[cfg(all(test, feature = "cluster"))]
pub(crate) fn install_replacement_source_drain_task_for_test(
    tasks: &OwnedSourceTasks,
    name: &str,
) -> SourceTaskLease {
    let (command_tx, mut command_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    let wake = Arc::new(tokio::sync::Notify::new());
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let actor_wake = Arc::clone(&wake);
    let actor_shutdown = Arc::clone(&shutdown);
    let actor_status = status_tx.clone();
    let runtime = tokio::runtime::Handle::current();
    let (join, actor_terminal) = spawn_source_actor(&runtime, async move {
        loop {
            tokio::select! {
                () = actor_shutdown.notified() => return,
                () = actor_wake.notified() => {
                    let command = command_rx.borrow_and_update().clone();
                    if let Some(SourceDrainCommand::Resolve { resolution, .. }) = command {
                        actor_status.send_replace(SourceDrainTaskStatus::Resolved {
                            round: resolution.round,
                            outcome: resolution.outcome,
                        });
                    }
                }
            }
        }
    });
    let task = SourceTaskLease::supervise(
        Arc::from(name),
        shutdown,
        Arc::new(AtomicBool::new(false)),
        join,
        actor_terminal,
        None,
        &runtime,
    );
    task.install_drain_control(SourceDrainLeaseControl {
        task_incarnation: uuid::Uuid::new_v4(),
        command_tx,
        status_tx,
        wake,
    });
    tasks.lock().push(task.clone());
    task
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

pub(crate) struct TrackedSourceRegistration {
    source: SourceRegistration,
    contract: SourceContract,
    expected_schema: arrow_schema::SchemaRef,
    positioned_schema: arrow_schema::SchemaRef,
    mutation_schema: arrow_schema::SchemaRef,
    primary_key: Vec<String>,
    primary_key_indices: Vec<usize>,
    schema_admitted: bool,
    temporal_right_mutations: bool,
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
            temporal_right_mutations: false,
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
            temporal_right_mutations: false,
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
        self.temporal_right_mutations = true;
        self
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
    source_mutations_admitted: Vec<bool>,
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
    pending_watermark_batches: Vec<(Arc<str>, RecordBatch)>,
    /// Sources that delivered a barrier this drain cycle. A later batch from one of these sources
    /// violates the source hold protocol and faults the pipeline.
    barrier_seen: FxHashSet<usize>,
    /// Per-source offset merged from `pending_offsets` after successful cycle publication.
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

struct CoordinatorRunState {
    batch_window: Duration,
    checkpoint_control_wake: Option<CheckpointControlWake>,
    checkpoint_control_poll_at: tokio::time::Instant,
    checkpoint_control_pending: bool,
    barriers: Vec<(usize, CheckpointBarrier, SourceCheckpoint)>,
    fault: Option<String>,
    halted: bool,
    source_channel_expected: bool,
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
    replies: Vec<ForceCheckpointReply>,
}

struct CheckpointAdmission {
    manual: bool,
    flags: u64,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
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
    ) {
        self.reset_inner(
            attempt,
            sources_total,
            flags,
            assignment_fence,
            CheckpointCleanupOwner::Originator,
        );
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
        self.cleanup_owner = CheckpointCleanupOwner::Originator;
    }
}

/// Fallback timeout for idle wake.
const IDLE_TIMEOUT: Duration = Duration::from_millis(100);

/// Internal topology-retry floor and cap. Assignment admission remains the authoritative gate.
const CHECKPOINT_RETRY_BASE: Duration = Duration::from_millis(100);
const CHECKPOINT_RETRY_MAX: Duration = Duration::from_secs(5);

/// Cap on a source task's post-shutdown flush so a hot source can't stall shutdown.
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(2);

/// Cap on awaiting a source task at shutdown before retiring its connector generation.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(8);

/// Shutdown-only poll cadence. It closes the atomic/channel race when a tail drops its in-flight
/// guard without producing another wakeup (for example an aborted cluster follower tail).
const SHUTDOWN_COMPLETION_TICK: Duration = Duration::from_millis(10);

/// Grace period for already-captured asynchronous checkpoint tails. On expiry their tracked
/// tasks are cancelled before sources or sinks are torn down; exact attempt namespaces leave any
/// ambiguous remote write safe for recovery.
const SHUTDOWN_CHECKPOINT_TAIL_TIMEOUT: Duration = Duration::from_secs(8);

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
/// fails because silently continuing can exhaust upstream retention or acknowledgement headroom.
async fn acknowledge_latest_source_commit(
    connector: &mut dyn SourceConnector,
    epoch_committed_rx: &mut tokio::sync::watch::Receiver<Option<(u64, SourceCheckpoint)>>,
    delivery_guarantee: DeliveryGuarantee,
    src_name: &str,
    fault_tx: &tokio::sync::mpsc::UnboundedSender<SourceFault>,
    deadline: tokio::time::Instant,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
) -> bool {
    let Some((epoch, checkpoint)) = epoch_committed_rx.borrow_and_update().clone() else {
        return true;
    };
    let result = match run_source_operation(
        deadline,
        #[cfg(feature = "cluster")]
        process_authority,
        || connector.notify_epoch_committed(epoch, &checkpoint),
    )
    .await
    {
        SourceOperationOutcome::Completed(result) => result,
        SourceOperationOutcome::Deadline => {
            lifecycle.cancelled(cancellation_policy);
            Err(source_operation_deadline_error(
                src_name,
                "commit notification",
            ))
        }
        #[cfg(feature = "cluster")]
        SourceOperationOutcome::ProcessAuthorityLost => {
            lifecycle.authority_lost();
            return false;
        }
    };
    if let Err(error) = result {
        if delivery_guarantee == DeliveryGuarantee::BestEffort {
            tracing::warn!(
                source = src_name,
                %error,
                epoch,
                "notify_epoch_committed failed",
            );
            return lifecycle.may_invoke_connector();
        }
        lifecycle.fault_data_plane();
        let _ = fault_tx.send(SourceFault {
            source: Arc::from(src_name),
            error: format!("commit notification failed at epoch {epoch}: {error}"),
        });
        return false;
    }
    true
}

#[cfg(feature = "cluster")]
async fn apply_latest_source_drain_command_fenced(
    connector: &mut dyn SourceConnector,
    command_rx: &mut tokio::sync::watch::Receiver<Option<SourceDrainCommand>>,
    status_tx: &tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    active: &mut Option<ActiveSourceDrain>,
    provider_drain: bool,
    source_name: &str,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    process_authority: Option<&SourceProcessAuthority>,
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
                lifecycle.run_sync_hook(
                    source_name,
                    "drain preparation",
                    deadline,
                    cancellation_policy,
                    process_authority,
                    || connector.begin_drain(&request, deadline),
                )?;
            } else {
                check_source_sync_fence(
                    source_name,
                    "drain preparation",
                    deadline,
                    false,
                    cancellation_policy,
                    lifecycle,
                    process_authority,
                )?;
            }
            lifecycle.run_sync_hook(
                source_name,
                "drain preparation publication",
                deadline,
                cancellation_policy,
                process_authority,
                || {
                    let _ = status_tx.send_replace(SourceDrainTaskStatus::Pausing(request.round));
                    Ok(())
                },
            )?;
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
                    lifecycle.run_sync_hook(
                        source_name,
                        "replacement drain abort publication",
                        deadline,
                        cancellation_policy,
                        process_authority,
                        || {
                            let _ = status_tx.send_replace(SourceDrainTaskStatus::Resolved {
                                round: resolution.round,
                                outcome: resolution.outcome,
                            });
                            Ok(())
                        },
                    )?;
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(ConnectorError::Internal(format!(
                        "source drain resolution {:?} expired before replacement reconciliation",
                        resolution.round
                    )));
                }
                if provider_drain {
                    let checkpoint_ready = lifecycle.run_sync_hook(
                        source_name,
                        "replacement checkpoint readiness",
                        deadline,
                        cancellation_policy,
                        process_authority,
                        || connector.checkpoint_ready(),
                    )?;
                    if !checkpoint_ready {
                        command_rx.mark_changed();
                        return Ok(());
                    }
                    let checkpoint = lifecycle.run_sync_hook(
                        source_name,
                        "replacement checkpoint capture",
                        deadline,
                        cancellation_policy,
                        process_authority,
                        || try_source_checkpoint(connector, true),
                    )?;
                    let Some(checkpoint) = checkpoint else {
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
                lifecycle.run_sync_hook(
                    source_name,
                    "replacement drain resolution publication",
                    deadline,
                    cancellation_policy,
                    process_authority,
                    || {
                        let _ = status_tx.send_replace(SourceDrainTaskStatus::Resolved {
                            round: resolution.round,
                            outcome: resolution.outcome,
                        });
                        Ok(())
                    },
                )?;
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
                match run_source_operation(resolution_deadline, process_authority, || {
                    connector.finish_drain(resolution, resolution_deadline)
                })
                .await
                {
                    SourceOperationOutcome::Completed(result) => result?,
                    SourceOperationOutcome::Deadline => {
                        lifecycle.cancelled(cancellation_policy);
                        return Err(source_operation_deadline_error(
                            source_name,
                            "drain resolution",
                        ));
                    }
                    SourceOperationOutcome::ProcessAuthorityLost => {
                        lifecycle.authority_lost();
                        return Err(source_operation_authority_error(
                            source_name,
                            "drain resolution",
                        ));
                    }
                }
            }
            lifecycle.run_sync_hook(
                source_name,
                "drain resolution publication",
                resolution_deadline,
                cancellation_policy,
                process_authority,
                || {
                    let _ = status_tx.send_replace(SourceDrainTaskStatus::Resolved {
                        round: resolution.round,
                        outcome: resolution.outcome,
                    });
                    Ok(())
                },
            )?;
            *active = None;
        }
    }
    Ok(())
}

#[cfg(feature = "cluster")]
async fn resolve_pending_source_drain_fenced(
    connector: &mut dyn SourceConnector,
    status_tx: &tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    active: &mut Option<ActiveSourceDrain>,
    source_name: &str,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    process_authority: Option<&SourceProcessAuthority>,
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
        match run_source_operation(pending.deadline, process_authority, || {
            connector.finish_drain(pending.resolution, pending.deadline)
        })
        .await
        {
            SourceOperationOutcome::Completed(result) => result?,
            SourceOperationOutcome::Deadline => {
                lifecycle.cancelled(cancellation_policy);
                return Err(source_operation_deadline_error(
                    source_name,
                    "pending drain resolution",
                ));
            }
            SourceOperationOutcome::ProcessAuthorityLost => {
                lifecycle.authority_lost();
                return Err(source_operation_authority_error(
                    source_name,
                    "pending drain resolution",
                ));
            }
        }
    }
    lifecycle.run_sync_hook(
        source_name,
        "pending drain resolution publication",
        pending.deadline,
        cancellation_policy,
        process_authority,
        || {
            let _ = status_tx.send_replace(SourceDrainTaskStatus::Resolved {
                round: pending.resolution.round,
                outcome: pending.resolution.outcome,
            });
            Ok(())
        },
    )?;
    *active = None;
    Ok(())
}

#[cfg(all(test, feature = "cluster"))]
async fn apply_latest_source_drain_command(
    connector: &mut dyn SourceConnector,
    command_rx: &mut tokio::sync::watch::Receiver<Option<SourceDrainCommand>>,
    status_tx: &tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    active: &mut Option<ActiveSourceDrain>,
    provider_drain: bool,
) -> Result<(), ConnectorError> {
    let mut lifecycle = SourceConnectorLifecycle::default();
    apply_latest_source_drain_command_fenced(
        connector,
        command_rx,
        status_tx,
        active,
        provider_drain,
        "test-source",
        ConnectorCancellationPolicy::RetireConnector,
        &mut lifecycle,
        None,
    )
    .await
}

#[cfg(all(test, feature = "cluster"))]
async fn resolve_pending_source_drain(
    connector: &mut dyn SourceConnector,
    status_tx: &tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    active: &mut Option<ActiveSourceDrain>,
) -> Result<(), ConnectorError> {
    let mut lifecycle = SourceConnectorLifecycle::default();
    resolve_pending_source_drain_fenced(
        connector,
        status_tx,
        active,
        "test-source",
        ConnectorCancellationPolicy::RetireConnector,
        &mut lifecycle,
        None,
    )
    .await
}

#[cfg(feature = "cluster")]
fn publish_source_drain_ready_fenced(
    connector: &mut dyn SourceConnector,
    control: &SourceDrainLeaseControl,
    active: &mut Option<ActiveSourceDrain>,
    source_name: &str,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    process_authority: Option<&SourceProcessAuthority>,
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
        lifecycle.run_sync_hook(
            source_name,
            "drain readiness",
            current.prepare_deadline,
            cancellation_policy,
            process_authority,
            || connector.poll_drain_ready(current.request.round),
        )?
    } else {
        check_source_sync_fence(
            source_name,
            "drain readiness",
            current.prepare_deadline,
            false,
            cancellation_policy,
            lifecycle,
            process_authority,
        )?;
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
    lifecycle.run_sync_hook(
        source_name,
        "drain readiness publication",
        current.prepare_deadline,
        cancellation_policy,
        process_authority,
        || {
            let _ = control
                .status_tx
                .send_replace(SourceDrainTaskStatus::Ready(receipt));
            Ok(())
        },
    )?;
    current.ready = true;
    Ok(())
}

#[cfg(all(test, feature = "cluster"))]
fn publish_source_drain_ready(
    connector: &mut dyn SourceConnector,
    control: &SourceDrainLeaseControl,
    active: &mut Option<ActiveSourceDrain>,
) -> Result<(), ConnectorError> {
    let mut lifecycle = SourceConnectorLifecycle::default();
    publish_source_drain_ready_fenced(
        connector,
        control,
        active,
        "test-source",
        ConnectorCancellationPolicy::RetireConnector,
        &mut lifecycle,
        None,
    )
}

#[cfg(feature = "cluster")]
fn source_drain_flushing(active: Option<&ActiveSourceDrain>) -> bool {
    active.is_some_and(|drain| !drain.ready)
}

#[cfg(feature = "cluster")]
fn source_drain_held(active: Option<&ActiveSourceDrain>) -> bool {
    active.is_some_and(|drain| drain.ready)
}

enum SourcePollOutcome {
    Completed(Result<Option<SourceBatch>, ConnectorError>),
    Deadline,
    Shutdown,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

enum SourceOperationOutcome<T> {
    Completed(T),
    Deadline,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

#[derive(Default)]
struct SourceConnectorLifecycle {
    retired: bool,
    data_plane_faulted: bool,
    #[cfg(feature = "cluster")]
    process_authority_lost: bool,
}

impl SourceConnectorLifecycle {
    fn cancelled(&mut self, cancellation_policy: ConnectorCancellationPolicy) {
        if cancellation_policy == ConnectorCancellationPolicy::RetireConnector {
            self.retired = true;
        }
    }

    #[cfg(feature = "cluster")]
    fn authority_lost(&mut self) {
        self.process_authority_lost = true;
    }

    #[cfg(feature = "cluster")]
    fn process_authority_lost(&self) -> bool {
        self.process_authority_lost
    }

    fn may_invoke_connector(&self) -> bool {
        !self.retired && {
            #[cfg(feature = "cluster")]
            {
                !self.process_authority_lost
            }
            #[cfg(not(feature = "cluster"))]
            {
                true
            }
        }
    }

    fn fault_data_plane(&mut self) {
        self.data_plane_faulted = true;
    }

    fn may_poll_or_ack(&self) -> bool {
        self.may_invoke_connector() && !self.data_plane_faulted
    }

    fn run_sync_hook<T>(
        &mut self,
        source: &str,
        operation: &str,
        deadline: tokio::time::Instant,
        cancellation_policy: ConnectorCancellationPolicy,
        #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
        hook: impl FnOnce() -> Result<T, ConnectorError>,
    ) -> Result<T, ConnectorError> {
        check_source_sync_fence(
            source,
            operation,
            deadline,
            false,
            cancellation_policy,
            self,
            #[cfg(feature = "cluster")]
            process_authority,
        )?;
        let result = hook();
        check_source_sync_fence(
            source,
            operation,
            deadline,
            true,
            cancellation_policy,
            self,
            #[cfg(feature = "cluster")]
            process_authority,
        )?;
        result
    }
}

/// Run one connector lifecycle operation behind the process-authority and absolute-deadline
/// fences. The future is not constructed after either fence has crossed. Authority and deadline
/// also win a ready tie, and a completed branch is revalidated before its result is admitted.
async fn run_source_operation<T, F, Fut>(
    deadline: tokio::time::Instant,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
    make_future: F,
) -> SourceOperationOutcome<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    #[cfg(feature = "cluster")]
    if !source_process_authority_is_live(process_authority) {
        return SourceOperationOutcome::ProcessAuthorityLost;
    }
    if tokio::time::Instant::now() >= deadline {
        return SourceOperationOutcome::Deadline;
    }

    let future = make_future();
    tokio::pin!(future);

    #[cfg(feature = "cluster")]
    if let Some(authority) = process_authority {
        return tokio::select! {
            biased;
            () = authority.cancelled() => SourceOperationOutcome::ProcessAuthorityLost,
            () = tokio::time::sleep_until(deadline) => SourceOperationOutcome::Deadline,
            result = &mut future => {
                if !authority.is_live() {
                    SourceOperationOutcome::ProcessAuthorityLost
                } else if tokio::time::Instant::now() >= deadline {
                    SourceOperationOutcome::Deadline
                } else {
                    SourceOperationOutcome::Completed(result)
                }
            }
        };
    }

    tokio::select! {
        biased;
        () = tokio::time::sleep_until(deadline) => SourceOperationOutcome::Deadline,
        result = &mut future => {
            if tokio::time::Instant::now() >= deadline {
                SourceOperationOutcome::Deadline
            } else {
                SourceOperationOutcome::Completed(result)
            }
        }
    }
}

fn source_operation_deadline_error(source: &str, operation: &str) -> ConnectorError {
    ConnectorError::Internal(format!(
        "source '{source}' {operation} exceeded its end-to-end deadline"
    ))
}

fn check_source_sync_fence(
    source: &str,
    operation: &str,
    deadline: tokio::time::Instant,
    operation_started: bool,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
) -> Result<(), ConnectorError> {
    #[cfg(feature = "cluster")]
    if !source_process_authority_is_live(process_authority) {
        lifecycle.authority_lost();
        return Err(source_operation_authority_error(source, operation));
    }
    if tokio::time::Instant::now() >= deadline {
        if operation_started {
            lifecycle.cancelled(cancellation_policy);
        }
        return Err(source_operation_deadline_error(source, operation));
    }
    Ok(())
}

#[cfg(feature = "cluster")]
fn source_operation_authority_error(source: &str, operation: &str) -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "live cluster process lease".into(),
        actual: format!("source '{source}' lost process authority during {operation}"),
    }
}

enum SourceStartOutcome {
    Completed(Result<(), ConnectorError>),
    TimedOut,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

enum SourceStartFailure {
    Connector(String),
    Retired(String),
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost(String),
}

async fn start_source_once(
    connector: &mut dyn SourceConnector,
    request: SourceStart,
    deadline: tokio::time::Instant,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
) -> SourceStartOutcome {
    match run_source_operation(
        deadline,
        #[cfg(feature = "cluster")]
        process_authority,
        || connector.start(request),
    )
    .await
    {
        SourceOperationOutcome::Completed(result) => SourceStartOutcome::Completed(result),
        SourceOperationOutcome::Deadline => SourceStartOutcome::TimedOut,
        #[cfg(feature = "cluster")]
        SourceOperationOutcome::ProcessAuthorityLost => SourceStartOutcome::ProcessAuthorityLost,
    }
}

async fn poll_source_once(
    connector: &mut dyn SourceConnector,
    max_records: usize,
    deadline: tokio::time::Instant,
    shutdown: &tokio::sync::Notify,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
) -> SourcePollOutcome {
    #[cfg(feature = "cluster")]
    if !source_process_authority_is_live(process_authority) {
        return SourcePollOutcome::ProcessAuthorityLost;
    }
    if tokio::time::Instant::now() >= deadline {
        return SourcePollOutcome::Deadline;
    }
    let mut poll = std::pin::pin!(connector.poll_batch(max_records));
    #[cfg(feature = "cluster")]
    if let Some(authority) = process_authority {
        return tokio::select! {
            biased;
            () = authority.cancelled() => {
                SourcePollOutcome::ProcessAuthorityLost
            }
            () = shutdown.notified() => SourcePollOutcome::Shutdown,
            () = tokio::time::sleep_until(deadline) => SourcePollOutcome::Deadline,
            result = poll.as_mut() => {
                if !authority.is_live() {
                    SourcePollOutcome::ProcessAuthorityLost
                } else if tokio::time::Instant::now() >= deadline {
                    SourcePollOutcome::Deadline
                } else {
                    SourcePollOutcome::Completed(result)
                }
            },
        };
    }
    tokio::select! {
        biased;
        () = shutdown.notified() => SourcePollOutcome::Shutdown,
        () = tokio::time::sleep_until(deadline) => SourcePollOutcome::Deadline,
        result = poll.as_mut() => {
            if tokio::time::Instant::now() >= deadline {
                SourcePollOutcome::Deadline
            } else {
                SourcePollOutcome::Completed(result)
            }
        },
    }
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
    operation_timeout: Duration,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
    poll_interval: Duration,
) -> bool {
    let data_ready = connector.data_ready_notify();
    #[cfg(feature = "cluster")]
    if let Some(authority) = process_authority {
        return tokio::select! {
            biased;
            () = authority.cancelled() => {
                lifecycle.authority_lost();
                false
            },
            () = shutdown.notified() => false,
            changed = epoch_committed_rx.changed() => if changed.is_ok() {
                acknowledge_latest_source_commit(
                    connector,
                    epoch_committed_rx,
                    delivery_guarantee,
                    src_name,
                    fault_tx,
                    tokio::time::Instant::now() + operation_timeout,
                    cancellation_policy,
                    lifecycle,
                    Some(authority),
                ).await
            } else {
                lifecycle.fault_data_plane();
                false
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
        };
    }

    tokio::select! {
        biased;
        () = shutdown.notified() => false,
        changed = epoch_committed_rx.changed() => if changed.is_ok() {
            acknowledge_latest_source_commit(
                connector,
                epoch_committed_rx,
                delivery_guarantee,
                src_name,
                fault_tx,
                tokio::time::Instant::now() + operation_timeout,
                cancellation_policy,
                lifecycle,
                #[cfg(feature = "cluster")]
                None,
            ).await
        } else {
            lifecycle.fault_data_plane();
            false
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
    operation_timeout: Duration,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    process_authority: Option<&SourceProcessAuthority>,
    poll_interval: Duration,
) -> bool {
    if let Some(authority) = process_authority {
        return tokio::select! {
            biased;
            () = authority.cancelled() => {
                lifecycle.authority_lost();
                false
            },
            () = shutdown.notified() => false,
            changed = epoch_committed_rx.changed() => if changed.is_ok() {
                acknowledge_latest_source_commit(
                    connector,
                    epoch_committed_rx,
                    delivery_guarantee,
                    src_name,
                    fault_tx,
                    tokio::time::Instant::now() + operation_timeout,
                    cancellation_policy,
                    lifecycle,
                    Some(authority),
                ).await
            } else {
                lifecycle.fault_data_plane();
                false
            },
            () = control_wake.notified() => true,
            () = tokio::time::sleep(poll_interval) => true,
        };
    }

    tokio::select! {
        biased;
        () = shutdown.notified() => false,
        changed = epoch_committed_rx.changed() => if changed.is_ok() {
            acknowledge_latest_source_commit(
                connector,
                epoch_committed_rx,
                delivery_guarantee,
                src_name,
                fault_tx,
                tokio::time::Instant::now() + operation_timeout,
                cancellation_policy,
                lifecycle,
                None,
            ).await
        } else {
            lifecycle.fault_data_plane();
            false
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
    operation_timeout: Duration,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
    poll_interval: Duration,
    barrier: CheckpointBarrier,
) -> bool {
    let attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
    loop {
        #[cfg(feature = "cluster")]
        if !source_process_authority_is_live(process_authority) {
            lifecycle.authority_lost();
            return false;
        }
        let signal = *barrier_release_rx.borrow_and_update();
        match signal {
            Some(SourceBarrierSignal::Release(released))
                if source_barrier_release_covers(released, attempt) =>
            {
                return true;
            }
            Some(SourceBarrierSignal::Stop) => {
                lifecycle.fault_data_plane();
                return false;
            }
            _ => {}
        }

        let control_deadline = tokio::time::Instant::now() + operation_timeout;
        if let Err(error) = lifecycle.run_sync_hook(
            src_name,
            "barrier-hold control-plane drive",
            control_deadline,
            cancellation_policy,
            #[cfg(feature = "cluster")]
            process_authority,
            || {
                connector.drive_control_plane();
                Ok(())
            },
        ) {
            lifecycle.fault_data_plane();
            let _ = fault_tx.send(SourceFault {
                source: Arc::from(src_name),
                error: error.to_string(),
            });
            return false;
        }
        #[cfg(feature = "cluster")]
        if let Some(authority) = process_authority {
            tokio::select! {
                biased;
                () = authority.cancelled() => {
                    lifecycle.authority_lost();
                    return false;
                },
                () = shutdown.notified() => {
                    lifecycle.fault_data_plane();
                    return false;
                },
                changed = barrier_release_rx.changed() => {
                    if changed.is_err() {
                        lifecycle.fault_data_plane();
                        return false;
                    }
                }
                changed = epoch_committed_rx.changed() => {
                    if changed.is_err() {
                        lifecycle.fault_data_plane();
                        return false;
                    }
                    if !acknowledge_latest_source_commit(
                        connector,
                        epoch_committed_rx,
                        delivery_guarantee,
                        src_name,
                        fault_tx,
                        tokio::time::Instant::now() + operation_timeout,
                        cancellation_policy,
                        lifecycle,
                        Some(authority),
                    ).await {
                        return false;
                    }
                },
                () = tokio::time::sleep(poll_interval) => {}
            }
            continue;
        }

        tokio::select! {
            biased;
            () = shutdown.notified() => {
                lifecycle.fault_data_plane();
                return false;
            },
            changed = barrier_release_rx.changed() => {
                if changed.is_err() {
                    lifecycle.fault_data_plane();
                    return false;
                }
            }
            changed = epoch_committed_rx.changed() => {
                if changed.is_err() {
                    lifecycle.fault_data_plane();
                    return false;
                }
                if !acknowledge_latest_source_commit(
                    connector,
                    epoch_committed_rx,
                    delivery_guarantee,
                    src_name,
                    fault_tx,
                    tokio::time::Instant::now() + operation_timeout,
                    cancellation_policy,
                    lifecycle,
                    #[cfg(feature = "cluster")]
                    None,
                ).await {
                    return false;
                }
            },
            () = tokio::time::sleep(poll_interval) => {}
        }
    }
}

impl StreamingCoordinator {
    fn admit_public_source_shapes(sources: &[TrackedSourceRegistration]) -> Result<(), DbError> {
        for source in sources {
            if source.expected_schema.fields().is_empty() {
                return Err(DbError::Config(format!(
                    "source '{}' must expose a non-empty schema before public coordinator startup; late-bound schemas require database-owned catalog admission",
                    source.name
                )));
            }
            if source.temporal_right_mutations {
                if source.contract().input_mode != SourceInputMode::KeyedUpsert
                    || source.has_reserved_mutation_columns()
                {
                    return Err(DbError::Config(format!(
                        "source '{}' lost its admitted temporal-right mutation contract",
                        source.name
                    )));
                }
            } else {
                admit_append_only_source(
                    source.contract(),
                    source.has_reserved_mutation_columns(),
                )
                .map_err(|reason| {
                    DbError::Config(format!(
                        "source '{}' is not admissible through the public coordinator: {reason} (contract: {:?})",
                        source.name,
                        source.contract()
                    ))
                })?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    #[inline]
    fn require_process_authority(&self, boundary: &str) -> Result<(), CycleError> {
        if self
            .process_authority
            .as_deref()
            .is_none_or(SourceProcessAuthority::is_live)
        {
            Ok(())
        } else {
            Err(CycleError::Recovery(format!(
                "cluster process lease expired before {boundary}"
            )))
        }
    }

    async fn close_startup_source(
        source: &mut SourceRegistration,
        cleanup_deadline: tokio::time::Instant,
        #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
    ) {
        match run_source_operation(
            cleanup_deadline,
            #[cfg(feature = "cluster")]
            process_authority,
            || source.connector.close(),
        )
        .await
        {
            SourceOperationOutcome::Completed(Ok(())) => {}
            SourceOperationOutcome::Completed(Err(error)) => {
                tracing::warn!(
                    source = %source.name,
                    %error,
                    "source close failed while rolling back pipeline startup"
                );
            }
            SourceOperationOutcome::Deadline => {
                tracing::warn!(
                    source = %source.name,
                    "source close exceeded its pipeline-startup cleanup deadline"
                );
            }
            #[cfg(feature = "cluster")]
            SourceOperationOutcome::ProcessAuthorityLost => {}
        }
    }

    async fn close_prepared_sources(
        sources: &mut Vec<PreparedSourceGeneration>,
        cleanup_timeout: Duration,
        #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
    ) {
        let cleanup_deadline = tokio::time::Instant::now() + cleanup_timeout;
        futures::future::join_all(sources.iter_mut().map(|source| {
            Self::close_startup_source(
                &mut source.registration,
                cleanup_deadline,
                #[cfg(feature = "cluster")]
                process_authority,
            )
        }))
        .await;
        sources.clear();
    }

    /// Reap a source task while preserving its generation fence until actual termination.
    fn reap_source_task(task: SourceTaskLease) {
        if !task.is_finished() {
            tracing::warn!(
                source = %task.name(),
                "source task did not exit within shutdown budget; retiring its connector generation"
            );
            task.abort();
            // The DB-owned lease remains registered until the actor wrapper and connector tracker
            // prove actual termination. Coordinator shutdown stays bounded without admitting an
            // overlapping replacement generation.
            drop(task);
            return;
        }
        task.log_terminal_outcome();
        drop(task);
    }

    /// Notify every source of a committed epoch so it can release retained upstream data.
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
        runtime: &StreamingCoordinatorRuntime,
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self, DbError> {
        let _construction = runtime.construction.lock().await;
        runtime.prune_and_require_idle()?;
        let generation = runtime.claim_generation()?;
        let sources = sources
            .into_iter()
            .map(|source| {
                TrackedSourceRegistration::capture(source, &runtime.owned_connector_task_fences)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::admit_public_source_shapes(&sources)?;
        if let Some(source) = sources.iter().find(|source| source.assignment_scoped) {
            return Err(DbError::Config(format!(
                "assignment-scoped source '{}' requires the database-owned cluster runtime",
                source.name
            )));
        }
        let mut coordinator = Self::new_with_tracked_source_registry(
            sources,
            config,
            shutdown,
            control_rx,
            source_gate,
            #[cfg(feature = "cluster")]
            None,
            Arc::clone(&runtime.owned_source_tasks),
            crate::db::RuntimeMode::Local,
        )
        .await?;
        coordinator.public_generation = Some(generation);
        Ok(coordinator)
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) async fn new_with_source_registry(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
        #[cfg(feature = "cluster")] source_process_authority: Option<Arc<ClusterController>>,
        owned_source_tasks: OwnedSourceTasks,
        owned_connector_task_fences: OwnedConnectorTaskFences,
        runtime_mode: crate::db::RuntimeMode,
    ) -> Result<Self, DbError> {
        let sources = sources
            .into_iter()
            .map(|source| TrackedSourceRegistration::capture(source, &owned_connector_task_fences))
            .collect::<Result<Vec<_>, _>>()?;
        Self::admit_public_source_shapes(&sources)?;
        Self::new_with_tracked_source_registry(
            sources,
            config,
            shutdown,
            control_rx,
            source_gate,
            #[cfg(feature = "cluster")]
            source_process_authority,
            owned_source_tasks,
            runtime_mode,
        )
        .await
    }

    pub(crate) async fn new_with_tracked_source_registry(
        sources: Vec<TrackedSourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
        #[cfg(feature = "cluster")] source_process_authority: Option<Arc<ClusterController>>,
        owned_source_tasks: OwnedSourceTasks,
        #[cfg(feature = "cluster")] runtime_mode: crate::db::RuntimeMode,
        #[cfg(not(feature = "cluster"))] _runtime_mode: crate::db::RuntimeMode,
    ) -> Result<Self, DbError> {
        if config
            .checkpoint_schedule
            .periodic_interval()
            .is_some_and(|interval| interval.is_zero())
        {
            return Err(DbError::Config(
                "checkpoint interval must be greater than zero; use manual checkpointing instead"
                    .into(),
            ));
        }
        if config.delivery_guarantee == DeliveryGuarantee::BestEffort {
            for src in &sources {
                if src.contract().consistency == SourceConsistency::CommitCoupled {
                    return Err(DbError::Config(format!(
                        "source '{}' is commit-coupled; commit-coupled sources currently support only at-least-once delivery",
                        src.name
                    )));
                }
            }
        }
        if config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            for src in &sources {
                if !src.contract().is_exact_delivery_certified() {
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
                if !src.contract().supports_replay() {
                    return Err(DbError::Config(format!(
                        "[LDB-5031] {} requires source '{}' to support replay",
                        config.delivery_guarantee, src.name
                    )));
                }
            }
            if !config.checkpoint_schedule.is_enabled() {
                return Err(DbError::Config(format!(
                    "[LDB-5032] {} requires checkpointing to be enabled",
                    config.delivery_guarantee
                )));
            }
        }

        // A source that releases externally retained data only on durable commit needs
        // checkpointing; otherwise that data can grow without bound. Reject the combination up
        // front.
        if !config.checkpoint_schedule.is_enabled() {
            for src in &sources {
                if src.contract().requires_checkpointing() {
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

        #[cfg(feature = "cluster")]
        if runtime_mode.is_cluster() {
            let controller = source_process_authority.as_ref().ok_or_else(|| {
                DbError::Config(
                    "cluster source runtime requires a cluster controller with process lease authority"
                        .into(),
                )
            })?;
            if controller.process_lease_deadline().is_none() {
                return Err(DbError::Config(
                    "cluster source runtime requires one shared process lease deadline before construction"
                        .into(),
                ));
            }
        } else if source_process_authority.is_some() {
            return Err(DbError::Config(
                "local source runtime cannot install cluster process lease authority".into(),
            ));
        }

        #[cfg(feature = "cluster")]
        let source_process_authority = source_process_authority.map(SourceProcessAuthority::new);

        let mut source_starts = Vec::with_capacity(sources.len());
        for src in sources {
            let start = SourceStart::new(
                src.config.clone(),
                src.position.clone(),
                config.delivery_guarantee,
            )
            .map_err(|error| match &src.position {
                SourcePosition::Initial => DbError::Config(format!(
                    "source '{}' has an invalid initial startup request: {error}",
                    src.name
                )),
                SourcePosition::Resume { attempt, .. } => DbError::Checkpoint(format!(
                    "[LDB-6003] source '{}' has an invalid resume request for checkpoint epoch={} id={}: {error}",
                    src.name, attempt.epoch, attempt.checkpoint_id
                )),
            })?;
            source_starts.push((src, start));
        }
        let source_count = source_starts.len();
        let mut prepared_sources = Vec::with_capacity(source_count);
        let mut committed_offsets = Vec::with_capacity(source_count);
        let source_start_timeout = config.checkpoint_timeout;
        let source_start_deadline = tokio::time::Instant::now() + source_start_timeout;

        // Do not spawn a polling task until every source has atomically installed its startup
        // position. Otherwise a later startup failure detaches the earlier tasks and they keep
        // polling without an owner capable of shutting them down.
        for (mut src, start) in source_starts {
            let src_name = src.name.clone();
            let start_position = src.position.clone();
            if tokio::time::Instant::now() >= source_start_deadline {
                #[cfg(feature = "cluster")]
                if !source_process_authority_is_live(source_process_authority.as_deref()) {
                    return Err(DbError::Connector(format!(
                        "source '{src_name}' start was not attempted: cluster process lease expired"
                    )));
                }
                // `timeout_at` polls its inner future once even when already expired. Starting a
                // source can acquire a lease/slot, so never construct or poll it after the shared
                // stage budget has been consumed by earlier sources.
                Self::close_prepared_sources(
                    &mut prepared_sources,
                    PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                    #[cfg(feature = "cluster")]
                    source_process_authority.as_deref(),
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
            let cancellation_policy = src.connector.cancellation_policy();
            let source_start_authorized = {
                #[cfg(feature = "cluster")]
                {
                    source_process_authority_is_live(source_process_authority.as_deref())
                }
                #[cfg(not(feature = "cluster"))]
                {
                    true
                }
            };
            let mut start_error = if source_start_authorized {
                match start_source_once(
                    src.connector.as_mut(),
                    start,
                    source_start_deadline,
                    #[cfg(feature = "cluster")]
                    source_process_authority.as_deref(),
                )
                .await
                {
                    SourceStartOutcome::Completed(Ok(())) => {
                        #[cfg(feature = "cluster")]
                        if source_process_authority_is_live(source_process_authority.as_deref()) {
                            None
                        } else {
                            Some(SourceStartFailure::ProcessAuthorityLost(
                                "process lease expired as source start completed".to_owned(),
                            ))
                        }
                        #[cfg(not(feature = "cluster"))]
                        None
                    }
                    SourceStartOutcome::Completed(Err(error)) => {
                        Some(if error.is_outcome_unknown() {
                            SourceStartFailure::Retired(error.to_string())
                        } else {
                            SourceStartFailure::Connector(error.to_string())
                        })
                    }
                    SourceStartOutcome::TimedOut => Some(
                        if cancellation_policy == ConnectorCancellationPolicy::RetireConnector {
                            SourceStartFailure::Retired(format!(
                                "exceeded the shared {source_start_timeout:?} source-start stage deadline"
                            ))
                        } else {
                            SourceStartFailure::Connector(format!(
                                "exceeded the shared {source_start_timeout:?} source-start stage deadline"
                            ))
                        },
                    ),
                    #[cfg(feature = "cluster")]
                    SourceStartOutcome::ProcessAuthorityLost => {
                        Some(SourceStartFailure::ProcessAuthorityLost(
                            "process lease expired while source start was in flight".to_owned(),
                        ))
                    }
                }
            } else {
                #[cfg(feature = "cluster")]
                {
                    Some(SourceStartFailure::ProcessAuthorityLost(
                        "process lease expired before source start began".to_owned(),
                    ))
                }
                #[cfg(not(feature = "cluster"))]
                {
                    unreachable!("local source startup is always authorized")
                }
            };
            if start_error.is_none() {
                let started_schema = src.connector.schema();
                if src.schema_admitted {
                    if started_schema.as_ref() != src.expected_schema.as_ref() {
                        start_error = Some(SourceStartFailure::Connector(format!(
                            "schema after start does not match the admitted schema for source '{src_name}'"
                        )));
                    }
                } else {
                    src.expected_schema = started_schema;
                    if let Err(reason) = admit_append_only_source(
                        src.contract,
                        schema_has_reserved_mutation_columns(src.expected_schema.as_ref()),
                    ) {
                        start_error = Some(SourceStartFailure::Connector(format!(
                            "source '{src_name}' schema after start is not admissible: {reason}"
                        )));
                    }
                }
                if start_error.is_none() {
                    match TrackedSourceRegistration::metadata_schemas(
                        &src_name,
                        src.contract,
                        &src.expected_schema,
                    ) {
                        Ok((positioned, mutations)) => {
                            src.positioned_schema = positioned;
                            src.mutation_schema = mutations;
                        }
                        Err(error) => {
                            start_error = Some(SourceStartFailure::Connector(error.to_string()));
                        }
                    }
                }
            }
            if let Some(failure) = start_error {
                let error = match failure {
                    SourceStartFailure::Connector(error) => {
                        // A completed failure may have acquired resources, but the connector is
                        // still valid for its bounded terminal cleanup.
                        prepared_sources.push(PreparedSourceGeneration { registration: src });
                        Self::close_prepared_sources(
                            &mut prepared_sources,
                            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                            #[cfg(feature = "cluster")]
                            source_process_authority.as_deref(),
                        )
                        .await;
                        error
                    }
                    SourceStartFailure::Retired(error) => {
                        // The cancelled current generation is terminal. Drop it without invoking
                        // close and clean up only sources whose starts completed.
                        Self::close_prepared_sources(
                            &mut prepared_sources,
                            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                            #[cfg(feature = "cluster")]
                            source_process_authority.as_deref(),
                        )
                        .await;
                        error
                    }
                    #[cfg(feature = "cluster")]
                    SourceStartFailure::ProcessAuthorityLost(error) => {
                        // Generic close may publish externally. Drop every generation without
                        // another connector call after the process authority fence.
                        error
                    }
                };
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
            prepared_sources.push(PreparedSourceGeneration { registration: src });
        }

        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(config.channel_capacity);
        let (source_fault_tx, source_fault_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut source_handles = Vec::with_capacity(source_count);
        let mut source_names = Vec::with_capacity(source_count);
        let mut source_mutations_admitted = Vec::with_capacity(source_count);
        let source_runtime = tokio::runtime::Handle::current();

        for (idx, prepared) in prepared_sources.into_iter().enumerate() {
            let PreparedSourceGeneration { registration } = prepared;
            let TrackedSourceRegistration {
                source: src,
                contract,
                expected_schema,
                positioned_schema,
                mutation_schema,
                primary_key,
                primary_key_indices,
                schema_admitted: _,
                temporal_right_mutations,
                task_fence,
            } = registration;
            let terminal_tasks = task_fence.tracker();
            let task_shutdown = Arc::new(tokio::sync::Notify::new());
            let task_shutdown_clone = Arc::clone(&task_shutdown);
            let task_tx = tx.clone();
            let task_fault_tx = source_fault_tx.clone();
            let task_gate = Arc::clone(&source_gate);
            #[cfg(feature = "cluster")]
            let task_process_authority = source_process_authority.clone();
            let max_poll = config.max_poll_records;
            let poll_interval = config.fallback_poll_interval;
            let source_operation_timeout = config.checkpoint_timeout;
            let delivery_guarantee = config.delivery_guarantee;
            let src_name = src.name.clone();
            let recovery_cursor = contract.supports_replay();
            let assignment_scoped = src.assignment_scoped;
            let cancellation_policy = src.connector.cancellation_policy();
            let mut connector = src.connector;

            #[cfg(feature = "cluster")]
            let drain_control = runtime_mode.is_cluster().then(|| {
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

            let task_exit_guard = SourceTaskExitGuard {
                source: Arc::from(src_name.as_str()),
                expected_shutdown: task_expected_shutdown,
                fault_tx: task_fault_tx.clone(),
            };
            let (join, actor_terminal) = spawn_source_actor(&source_runtime, async move {
                let _exit_guard = task_exit_guard;
                // Starting connectors and starting their I/O are separate phases. Keeping every
                // task behind this one-shot fence prevents a fast poll/commit failure from being
                // queued before the dedicated compute loop can publish its readiness boundary.
                // Cancellation before activation closes the connector without polling it.
                #[cfg(feature = "cluster")]
                let activated = if let Some(authority) = task_process_authority.as_deref() {
                    tokio::select! {
                        biased;
                        () = authority.cancelled() => false,
                        () = task_shutdown_clone.notified() => false,
                        activation = startup_activation_rx => activation.is_ok(),
                    }
                } else {
                    tokio::select! {
                        biased;
                        () = task_shutdown_clone.notified() => false,
                        activation = startup_activation_rx => activation.is_ok(),
                    }
                };
                #[cfg(not(feature = "cluster"))]
                let activated = tokio::select! {
                    biased;
                    () = task_shutdown_clone.notified() => false,
                    activation = startup_activation_rx => activation.is_ok(),
                };

                if !activated {
                    let deadline = tokio::time::Instant::now() + SHUTDOWN_DRAIN_BUDGET;
                    match run_source_operation(
                        deadline,
                        #[cfg(feature = "cluster")]
                        task_process_authority.as_deref(),
                        || connector.close(),
                    )
                    .await
                    {
                        SourceOperationOutcome::Completed(Ok(())) => {}
                        SourceOperationOutcome::Completed(Err(error)) => {
                            tracing::warn!(source = %src_name, %error, "source close error");
                        }
                        SourceOperationOutcome::Deadline => {
                            tracing::warn!(
                                source = %src_name,
                                "source close exceeded its shutdown deadline"
                            );
                        }
                        #[cfg(feature = "cluster")]
                        SourceOperationOutcome::ProcessAuthorityLost => {}
                    }
                    return;
                }

                let mut lifecycle = SourceConnectorLifecycle::default();
                let mut pending_barrier = None;
                let mut pending_batch: Option<RecordBatch> = None;
                #[cfg(feature = "cluster")]
                let mut active_source_drain: Option<ActiveSourceDrain> = None;

                // Acknowledge a fresh commit before polling more to preserve upstream retention
                // and acknowledgement headroom.
                loop {
                    #[cfg(feature = "cluster")]
                    if !source_process_authority_is_live(task_process_authority.as_deref()) {
                        lifecycle.authority_lost();
                        break;
                    }

                    #[cfg(feature = "cluster")]
                    if let (Some(control), Some(command_rx)) =
                        (task_drain_control.as_ref(), task_drain_command_rx.as_mut())
                    {
                        if let Err(error) = apply_latest_source_drain_command_fenced(
                            connector.as_mut(),
                            command_rx,
                            &control.status_tx,
                            &mut active_source_drain,
                            assignment_scoped,
                            &src_name,
                            cancellation_policy,
                            &mut lifecycle,
                            task_process_authority.as_deref(),
                        )
                        .await
                        {
                            if !lifecycle.process_authority_lost() {
                                lifecycle.fault_data_plane();
                                let _ = task_fault_tx.send(SourceFault {
                                    source: Arc::from(src_name.as_str()),
                                    error: error.to_string(),
                                });
                            }
                            break;
                        }
                        // At loop entry every earlier source-channel send has completed. A cut
                        // observed in the preceding poll can therefore become externally Ready.
                        if pending_batch.is_none() {
                            if let Err(error) = publish_source_drain_ready_fenced(
                                connector.as_mut(),
                                control,
                                &mut active_source_drain,
                                &src_name,
                                cancellation_policy,
                                &mut lifecycle,
                                task_process_authority.as_deref(),
                            ) {
                                if !lifecycle.process_authority_lost() {
                                    lifecycle.fault_data_plane();
                                    let _ = task_fault_tx.send(SourceFault {
                                        source: Arc::from(src_name.as_str()),
                                        error: error.to_string(),
                                    });
                                }
                                break;
                            }
                        }
                        if let Err(error) = resolve_pending_source_drain_fenced(
                            connector.as_mut(),
                            &control.status_tx,
                            &mut active_source_drain,
                            &src_name,
                            cancellation_policy,
                            &mut lifecycle,
                            task_process_authority.as_deref(),
                        )
                        .await
                        {
                            if !lifecycle.process_authority_lost() {
                                lifecycle.fault_data_plane();
                                let _ = task_fault_tx.send(SourceFault {
                                    source: Arc::from(src_name.as_str()),
                                    error: error.to_string(),
                                });
                            }
                            break;
                        }
                    }

                    #[cfg(feature = "cluster")]
                    if !source_process_authority_is_live(task_process_authority.as_deref()) {
                        lifecycle.authority_lost();
                        break;
                    }

                    match epoch_committed_rx.has_changed() {
                        Ok(true) => {
                            if !acknowledge_latest_source_commit(
                                connector.as_mut(),
                                &mut epoch_committed_rx,
                                delivery_guarantee,
                                &src_name,
                                &task_fault_tx,
                                tokio::time::Instant::now() + source_operation_timeout,
                                cancellation_policy,
                                &mut lifecycle,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
                            )
                            .await
                            {
                                break;
                            }
                            #[cfg(feature = "cluster")]
                            if !source_process_authority_is_live(task_process_authority.as_deref())
                            {
                                lifecycle.authority_lost();
                                break;
                            }
                            continue;
                        }
                        Ok(false) => {}
                        Err(_) => {
                            lifecycle.fault_data_plane();
                            break;
                        }
                    }

                    let control_deadline = tokio::time::Instant::now() + source_operation_timeout;
                    if let Err(error) = lifecycle.run_sync_hook(
                        &src_name,
                        "control-plane drive",
                        control_deadline,
                        cancellation_policy,
                        #[cfg(feature = "cluster")]
                        task_process_authority.as_deref(),
                        || {
                            connector.drive_control_plane();
                            Ok(())
                        },
                    ) {
                        lifecycle.fault_data_plane();
                        let _ = task_fault_tx.send(SourceFault {
                            source: Arc::from(src_name.as_str()),
                            error: error.to_string(),
                        });
                        break;
                    }

                    // A cluster-aware source may observe a new ownership publication before its
                    // external consumer has rebound and validated the version-bound handoff
                    // cursor. Do not poll data or consume a barrier during that window.
                    let checkpoint_ready = match lifecycle.run_sync_hook(
                        &src_name,
                        "checkpoint readiness",
                        control_deadline,
                        cancellation_policy,
                        #[cfg(feature = "cluster")]
                        task_process_authority.as_deref(),
                        || connector.checkpoint_ready(),
                    ) {
                        Ok(ready) => ready,
                        Err(error) => {
                            lifecycle.fault_data_plane();
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
                            source_operation_timeout,
                            cancellation_policy,
                            &mut lifecycle,
                            #[cfg(feature = "cluster")]
                            task_process_authority.as_deref(),
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
                        match lifecycle.run_sync_hook(
                            &src_name,
                            "pending batch checkpoint capture",
                            control_deadline,
                            cancellation_policy,
                            #[cfg(feature = "cluster")]
                            task_process_authority.as_deref(),
                            || try_source_checkpoint(connector.as_ref(), assignment_scoped),
                        ) {
                            Ok(Some(checkpoint)) => {
                                let msg = SourceMsg::Batch {
                                    source_idx: idx,
                                    batch,
                                    checkpoint,
                                };
                                if !send_source_msg(
                                    &task_tx,
                                    msg,
                                    &task_shutdown_clone,
                                    #[cfg(feature = "cluster")]
                                    task_process_authority.as_deref(),
                                )
                                .await
                                {
                                    lifecycle.fault_data_plane();
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
                                    source_operation_timeout,
                                    cancellation_policy,
                                    &mut lifecycle,
                                    #[cfg(feature = "cluster")]
                                    task_process_authority.as_deref(),
                                    poll_interval,
                                )
                                .await
                                {
                                    break;
                                }
                            }
                            Err(error) => {
                                lifecycle.fault_data_plane();
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
                            source_drain_flushing(active_source_drain.as_ref())
                        }
                        #[cfg(not(feature = "cluster"))]
                        {
                            false
                        }
                    };
                    let drain_held = {
                        #[cfg(feature = "cluster")]
                        {
                            source_drain_held(active_source_drain.as_ref())
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
                            match lifecycle.run_sync_hook(
                                &src_name,
                                "barrier checkpoint capture",
                                control_deadline,
                                cancellation_policy,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
                                || try_source_checkpoint(connector.as_ref(), assignment_scoped),
                            ) {
                                Ok(Some(checkpoint)) => {
                                    let msg = SourceMsg::Barrier {
                                        source_idx: idx,
                                        barrier,
                                        checkpoint,
                                    };
                                    if !send_source_msg(
                                        &task_tx,
                                        msg,
                                        &task_shutdown_clone,
                                        #[cfg(feature = "cluster")]
                                        task_process_authority.as_deref(),
                                    )
                                    .await
                                    {
                                        lifecycle.fault_data_plane();
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
                                        source_operation_timeout,
                                        cancellation_policy,
                                        &mut lifecycle,
                                        #[cfg(feature = "cluster")]
                                        task_process_authority.as_deref(),
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
                                        source_operation_timeout,
                                        cancellation_policy,
                                        &mut lifecycle,
                                        #[cfg(feature = "cluster")]
                                        task_process_authority.as_deref(),
                                        poll_interval,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                }
                                Err(error) => {
                                    lifecycle.fault_data_plane();
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
                            source_operation_timeout,
                            cancellation_policy,
                            &mut lifecycle,
                            task_process_authority.as_deref(),
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
                            match lifecycle.run_sync_hook(
                                &src_name,
                                "gated barrier checkpoint capture",
                                control_deadline,
                                cancellation_policy,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
                                || try_source_checkpoint(connector.as_ref(), assignment_scoped),
                            ) {
                                Ok(Some(checkpoint)) => {
                                    let msg = SourceMsg::Barrier {
                                        source_idx: idx,
                                        barrier,
                                        checkpoint,
                                    };
                                    if !send_source_msg(
                                        &task_tx,
                                        msg,
                                        &task_shutdown_clone,
                                        #[cfg(feature = "cluster")]
                                        task_process_authority.as_deref(),
                                    )
                                    .await
                                    {
                                        lifecycle.fault_data_plane();
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
                                        source_operation_timeout,
                                        cancellation_policy,
                                        &mut lifecycle,
                                        #[cfg(feature = "cluster")]
                                        task_process_authority.as_deref(),
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
                                    lifecycle.fault_data_plane();
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
                            source_operation_timeout,
                            cancellation_policy,
                            &mut lifecycle,
                            #[cfg(feature = "cluster")]
                            task_process_authority.as_deref(),
                            poll_interval,
                        )
                        .await
                        {
                            break;
                        }
                        continue;
                    }
                    #[cfg(feature = "cluster")]
                    if !source_process_authority_is_live(task_process_authority.as_deref()) {
                        lifecycle.authority_lost();
                        break;
                    }
                    let poll_deadline = tokio::time::Instant::now() + source_operation_timeout;
                    let poll_result = match poll_source_once(
                        connector.as_mut(),
                        max_poll,
                        poll_deadline,
                        &task_shutdown_clone,
                        #[cfg(feature = "cluster")]
                        task_process_authority.as_deref(),
                    )
                    .await
                    {
                        SourcePollOutcome::Completed(result) => result,
                        SourcePollOutcome::Deadline => {
                            lifecycle.cancelled(cancellation_policy);
                            lifecycle.fault_data_plane();
                            let error = source_operation_deadline_error(&src_name, "poll");
                            let _ = task_fault_tx.send(SourceFault {
                                source: Arc::from(src_name.as_str()),
                                error: error.to_string(),
                            });
                            break;
                        }
                        SourcePollOutcome::Shutdown => {
                            lifecycle.cancelled(cancellation_policy);
                            break;
                        }
                        #[cfg(feature = "cluster")]
                        SourcePollOutcome::ProcessAuthorityLost => {
                            lifecycle.authority_lost();
                            break;
                        }
                    };

                    #[cfg(feature = "cluster")]
                    if !source_process_authority_is_live(task_process_authority.as_deref()) {
                        lifecycle.authority_lost();
                        break;
                    }

                    match poll_result {
                        Ok(Some(batch)) => {
                            let batch = match prepare_encoded_source_batch(
                                &src_name,
                                &expected_schema,
                                &positioned_schema,
                                &mutation_schema,
                                &primary_key,
                                &primary_key_indices,
                                contract.row_positions,
                                batch,
                            ) {
                                Ok(batch) => batch,
                                Err(error) => {
                                    lifecycle.fault_data_plane();
                                    let _ = task_fault_tx.send(SourceFault {
                                        source: Arc::from(src_name.as_str()),
                                        error: error.to_string(),
                                    });
                                    break;
                                }
                            };
                            let checkpoint = match lifecycle.run_sync_hook(
                                &src_name,
                                "polled batch checkpoint capture",
                                poll_deadline,
                                cancellation_policy,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
                                || try_source_checkpoint(connector.as_ref(), assignment_scoped),
                            ) {
                                Ok(Some(checkpoint)) => checkpoint,
                                Ok(None) => {
                                    pending_batch = Some(batch);
                                    continue;
                                }
                                Err(error) => {
                                    lifecycle.fault_data_plane();
                                    let _ = task_fault_tx.send(SourceFault {
                                        source: Arc::from(src_name.as_str()),
                                        error: error.to_string(),
                                    });
                                    break;
                                }
                            };
                            let msg = SourceMsg::Batch {
                                source_idx: idx,
                                batch,
                                checkpoint,
                            };
                            if !send_source_msg(
                                &task_tx,
                                msg,
                                &task_shutdown_clone,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
                            )
                            .await
                            {
                                lifecycle.fault_data_plane();
                                break; // Coordinator dropped
                            }
                        }
                        Ok(None) => {
                            #[cfg(feature = "cluster")]
                            let drain_became_ready = if let Some(control) =
                                task_drain_control.as_ref()
                            {
                                let was_flushing =
                                    source_drain_flushing(active_source_drain.as_ref());
                                if let Err(error) = publish_source_drain_ready_fenced(
                                    connector.as_mut(),
                                    control,
                                    &mut active_source_drain,
                                    &src_name,
                                    cancellation_policy,
                                    &mut lifecycle,
                                    task_process_authority.as_deref(),
                                ) {
                                    lifecycle.fault_data_plane();
                                    let _ = task_fault_tx.send(SourceFault {
                                        source: Arc::from(src_name.as_str()),
                                        error: error.to_string(),
                                    });
                                    break;
                                }
                                was_flushing && !source_drain_flushing(active_source_drain.as_ref())
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
                                source_operation_timeout,
                                cancellation_policy,
                                &mut lifecycle,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
                                poll_interval,
                            )
                            .await
                            {
                                break;
                            }
                        }
                        Err(e) if !e.is_transient() => {
                            lifecycle.fault_data_plane();
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
                                source_operation_timeout,
                                cancellation_policy,
                                &mut lifecycle,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
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
                            source_drain_flushing(active_source_drain.as_ref())
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
                            let barrier_deadline =
                                tokio::time::Instant::now() + source_operation_timeout;
                            match lifecycle.run_sync_hook(
                                &src_name,
                                "post-poll barrier checkpoint capture",
                                barrier_deadline,
                                cancellation_policy,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
                                || try_source_checkpoint(connector.as_ref(), assignment_scoped),
                            ) {
                                Ok(Some(checkpoint)) => {
                                    let msg = SourceMsg::Barrier {
                                        source_idx: idx,
                                        barrier,
                                        checkpoint,
                                    };
                                    if !send_source_msg(
                                        &task_tx,
                                        msg,
                                        &task_shutdown_clone,
                                        #[cfg(feature = "cluster")]
                                        task_process_authority.as_deref(),
                                    )
                                    .await
                                    {
                                        lifecycle.fault_data_plane();
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
                                        source_operation_timeout,
                                        cancellation_policy,
                                        &mut lifecycle,
                                        #[cfg(feature = "cluster")]
                                        task_process_authority.as_deref(),
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
                                    lifecycle.fault_data_plane();
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
                if !source_process_authority_is_live(task_process_authority.as_deref()) {
                    lifecycle.authority_lost();
                }
                #[cfg(feature = "cluster")]
                let may_flush_on_shutdown =
                    lifecycle.may_poll_or_ack() && active_source_drain.is_none();
                #[cfg(not(feature = "cluster"))]
                let may_flush_on_shutdown = lifecycle.may_poll_or_ack();

                let shutdown_deadline = tokio::time::Instant::now() + SHUTDOWN_DRAIN_BUDGET;
                if may_flush_on_shutdown {
                    // Tail polling, durable acknowledgement, and close share one absolute
                    // shutdown budget. Unflushed rows resume from the committed offset.
                    let mut tail_poll_allowed = true;
                    if let Some(batch) = pending_batch.take() {
                        match lifecycle.run_sync_hook(
                            &src_name,
                            "shutdown pending batch checkpoint capture",
                            shutdown_deadline,
                            cancellation_policy,
                            #[cfg(feature = "cluster")]
                            task_process_authority.as_deref(),
                            || try_source_checkpoint(connector.as_ref(), assignment_scoped),
                        ) {
                            Ok(Some(checkpoint)) => {
                                if task_tx
                                    .try_send(SourceMsg::Batch {
                                        source_idx: idx,
                                        batch,
                                        checkpoint,
                                    })
                                    .is_err()
                                {
                                    lifecycle.fault_data_plane();
                                    tail_poll_allowed = false;
                                }
                            }
                            Ok(None) => tail_poll_allowed = false,
                            Err(error) => {
                                lifecycle.fault_data_plane();
                                tail_poll_allowed = false;
                                let _ = task_fault_tx.send(SourceFault {
                                    source: Arc::from(src_name.as_str()),
                                    error: error.to_string(),
                                });
                            }
                        }
                    }
                    while tail_poll_allowed
                        && lifecycle.may_poll_or_ack()
                        && tokio::time::Instant::now() < shutdown_deadline
                    {
                        let poll_result = match run_source_operation(
                            shutdown_deadline,
                            #[cfg(feature = "cluster")]
                            task_process_authority.as_deref(),
                            || connector.poll_batch(max_poll),
                        )
                        .await
                        {
                            SourceOperationOutcome::Completed(result) => Some(result),
                            SourceOperationOutcome::Deadline => {
                                lifecycle.cancelled(cancellation_policy);
                                None
                            }
                            #[cfg(feature = "cluster")]
                            SourceOperationOutcome::ProcessAuthorityLost => {
                                lifecycle.authority_lost();
                                None
                            }
                        };
                        if !lifecycle.may_invoke_connector() {
                            break;
                        }
                        match poll_result {
                            Some(Ok(Some(batch))) => {
                                let batch = match prepare_encoded_source_batch(
                                    &src_name,
                                    &expected_schema,
                                    &positioned_schema,
                                    &mutation_schema,
                                    &primary_key,
                                    &primary_key_indices,
                                    contract.row_positions,
                                    batch,
                                ) {
                                    Ok(batch) => batch,
                                    Err(error) => {
                                        lifecycle.fault_data_plane();
                                        let _ = task_fault_tx.send(SourceFault {
                                            source: Arc::from(src_name.as_str()),
                                            error: error.to_string(),
                                        });
                                        break;
                                    }
                                };
                                let checkpoint = match lifecycle.run_sync_hook(
                                    &src_name,
                                    "shutdown tail checkpoint capture",
                                    shutdown_deadline,
                                    cancellation_policy,
                                    #[cfg(feature = "cluster")]
                                    task_process_authority.as_deref(),
                                    || try_source_checkpoint(connector.as_ref(), assignment_scoped),
                                ) {
                                    Ok(Some(checkpoint)) => checkpoint,
                                    Ok(None) => break,
                                    Err(error) => {
                                        lifecycle.fault_data_plane();
                                        let _ = task_fault_tx.send(SourceFault {
                                            source: Arc::from(src_name.as_str()),
                                            error: error.to_string(),
                                        });
                                        break;
                                    }
                                };
                                let msg = SourceMsg::Batch {
                                    source_idx: idx,
                                    batch,
                                    checkpoint,
                                };
                                if task_tx.try_send(msg).is_err() {
                                    lifecycle.fault_data_plane();
                                    break;
                                }
                                if tokio::time::Instant::now() >= shutdown_deadline {
                                    break;
                                }
                            }
                            Some(Err(error)) if !error.is_transient() => {
                                lifecycle.fault_data_plane();
                                let _ = task_fault_tx.send(SourceFault {
                                    source: Arc::from(src_name.as_str()),
                                    error: error.to_string(),
                                });
                                break;
                            }
                            Some(Ok(None) | Err(_)) | None => break,
                        }
                    }
                }

                if lifecycle.may_poll_or_ack() {
                    // Drain EpochCommitted broadcasts before close so a durable tail settled
                    // during shutdown is acknowledged upstream. The watch retains the newest
                    // value; waiting for another change here would consume the close budget.
                    while matches!(epoch_committed_rx.has_changed(), Ok(true)) {
                        if !acknowledge_latest_source_commit(
                            connector.as_mut(),
                            &mut epoch_committed_rx,
                            delivery_guarantee,
                            &src_name,
                            &task_fault_tx,
                            shutdown_deadline,
                            cancellation_policy,
                            &mut lifecycle,
                            #[cfg(feature = "cluster")]
                            task_process_authority.as_deref(),
                        )
                        .await
                        {
                            break;
                        }
                    }
                }

                if lifecycle.may_invoke_connector() {
                    match run_source_operation(
                        shutdown_deadline,
                        #[cfg(feature = "cluster")]
                        task_process_authority.as_deref(),
                        || connector.close(),
                    )
                    .await
                    {
                        SourceOperationOutcome::Completed(Ok(())) => {}
                        SourceOperationOutcome::Completed(Err(error)) => {
                            tracing::warn!(source = %src_name, %error, "source close error");
                        }
                        SourceOperationOutcome::Deadline => {
                            tracing::warn!(
                                source = %src_name,
                                "source close exceeded its shutdown deadline"
                            );
                        }
                        #[cfg(feature = "cluster")]
                        SourceOperationOutcome::ProcessAuthorityLost => {}
                    }
                }
            });

            let arc_name: Arc<str> = Arc::from(src.name.as_str());
            let task = SourceTaskLease::supervise(
                Arc::clone(&arc_name),
                Arc::clone(&task_shutdown),
                Arc::clone(&expected_shutdown),
                join,
                actor_terminal,
                terminal_tasks,
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
            source_mutations_admitted.push(temporal_right_mutations);
            task_fence.handoff();
        }

        Ok(Self {
            config,
            rx,
            source_fault_rx,
            source_handles,
            source_names,
            source_mutations_admitted,
            shutdown,
            terminal_shutdown: tokio_util::sync::CancellationToken::new(),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_retry_not_before: None,
            checkpoint_retry_backoff: Duration::ZERO,
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
            manual_handoff_required: false,
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            last_published_checkpoint: None,
            public_generation: None,
            #[cfg(feature = "cluster")]
            process_authority: source_process_authority,
        })
    }

    /// Wire in the callback's admission counter so the coordinator gates new barriers.
    pub(crate) fn with_checkpoint_admission(mut self, in_flight: Arc<AtomicU64>) -> Self {
        self.checkpoint_in_flight = in_flight;
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

    pub(crate) fn with_terminal_shutdown(
        mut self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        self.terminal_shutdown = shutdown;
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

    fn activate_manual_attempt(&mut self, attempt: CheckpointAttempt, flags: u64) {
        if self.manual_waiting.is_empty() {
            return;
        }
        debug_assert!(self.manual_active.is_none());
        self.manual_active = Some(ManualCheckpointAttempt {
            attempt,
            flags,
            replies: std::mem::take(&mut self.manual_waiting),
        });
        self.manual_handoff_required = false;
    }

    fn retry_manual_handoff(&mut self, attempt: CheckpointAttempt) {
        let Some(active) = self.manual_active.take() else {
            return;
        };
        if active.attempt != attempt {
            self.manual_active = Some(active);
            return;
        }
        if active.flags & laminar_core::checkpoint::flags::HANDOFF == 0 {
            for reply in active.replies {
                reply.send(Err(DbError::Checkpoint(
                    "non-handoff checkpoint reported pending handoff replay".into(),
                )));
            }
            return;
        }
        let mut replies = active.replies;
        replies.append(&mut self.manual_waiting);
        self.manual_waiting = replies;
        self.manual_handoff_required = true;
    }

    fn fail_waiting_manual(&mut self, error: impl Into<String>) {
        let error = error.into();
        for reply in self.manual_waiting.drain(..) {
            reply.send(Err(DbError::Checkpoint(error.clone())));
        }
        self.manual_handoff_required = false;
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
        cleanup_owner: CheckpointCleanupOwner,
        attempt: CheckpointAttempt,
        reason: &str,
        flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        match cleanup_owner {
            CheckpointCleanupOwner::Originator => {
                callback
                    .abandon_checkpoint_attempt(attempt, reason, flags, assignment_fence)
                    .await
            }
            CheckpointCleanupOwner::Follower => {
                callback
                    .cancel_source_barrier_attempt(attempt, reason)
                    .await
            }
        }
    }

    async fn cancel_pending_barrier_for_stop(
        &mut self,
        callback: &mut impl PipelineCallback,
        reason: &str,
        release_sources: bool,
    ) -> Result<(), String> {
        let was_active = self.pending_barrier.active;
        let flags = self.pending_barrier.flags;
        let assignment_fence = self.pending_barrier.assignment_fence.clone();
        let attempt = self.pending_barrier.take_active_attempt();
        self.barrier_seen.clear();

        match attempt {
            Some((attempt, cleanup_owner)) => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason,
                    "abandoning checkpoint interrupted before source alignment"
                );
                if cleanup_owner == CheckpointCleanupOwner::Originator {
                    self.cancel_local_source_barriers(CheckpointBarrier {
                        checkpoint_id: attempt.checkpoint_id,
                        epoch: attempt.epoch,
                        flags,
                    });
                }
                Self::cleanup_checkpoint_attempt(
                    callback,
                    cleanup_owner,
                    attempt,
                    reason,
                    flags,
                    assignment_fence,
                )
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
                handoff_replay_pending,
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
                    #[cfg(feature = "cluster")]
                    if let Err(error) =
                        self.require_process_authority("durable checkpoint publication")
                    {
                        let reason = error.to_string();
                        callback.abort_subscription_cut(attempt);
                        self.fail_manual_attempt(attempt, &reason);
                        callback.record_checkpoint_continuation_fault(attempt, &reason);
                        return Some(reason);
                    }
                    let continuation_error = result.continuation_error().map(str::to_owned);
                    // Ordering is semantic: N reached its durable point, so source and public
                    // acknowledgements for N must be published even when N+1 cannot be opened.
                    self.last_published_checkpoint = Some(attempt);
                    let publication_error = callback.publish_barrier(attempt).err();
                    self.broadcast_epoch_committed(attempt.epoch, &source_checkpoints);
                    let continuation_error = publication_error.or(continuation_error);
                    if handoff_replay_pending {
                        self.replay_pending = true;
                        if let Some(reason) = continuation_error.as_deref() {
                            self.fail_manual_attempt(attempt, reason);
                        } else {
                            self.retry_manual_handoff(attempt);
                        }
                    } else {
                        self.finish_manual_success(attempt, &result);
                        self.advance_checkpoint_cadence();
                    }
                    if let Some(reason) = continuation_error.as_deref() {
                        callback.record_checkpoint_continuation_fault(attempt, reason);
                    }
                    return continuation_error;
                }
            }
            CheckpointCompletion::Failed { error, .. } => {
                callback.abort_subscription_cut(attempt);
                self.fail_manual_attempt(attempt, &error);
                callback.record_checkpoint_failure(attempt.checkpoint_id, &error);
                self.advance_checkpoint_cadence();
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

    async fn wait_for_cycle<C: PipelineCallback>(
        &mut self,
        callback: &mut C,
        state: &mut CoordinatorRunState,
    ) -> CoordinatorWait {
        if let Err(error) = callback.prepare_source_intake() {
            state.fault = Some(format!(
                "source recovery handoff could not be installed before intake: {error}"
            ));
            return CoordinatorWait::stop();
        }
        let intake_paused = callback.intake_paused();
        if intake_paused || self.replay_pending {
            let _ = callback.is_recovering();
        }
        let replay_ready = self.replay_pending && !intake_paused;
        let parked_ready =
            !self.replay_pending && !intake_paused && self.parked_source_msg.is_some();
        let mut retrying_replay = false;
        let mut checkpoint_control_due = false;

        let message = tokio::select! {
            biased;
            () = self.terminal_shutdown.cancelled() => return CoordinatorWait::stop(),
            () = self.shutdown.notified() => return CoordinatorWait::stop(),
            Some(source_fault) = self.source_fault_rx.recv() => {
                state.fault = Some(format!(
                    "source '{}' fault: {}",
                    source_fault.source, source_fault.error
                ));
                return CoordinatorWait::stop();
            }
            Some(completion) = async {
                if let Some(ref mut rx) = self.checkpoint_complete_rx {
                    rx.recv().await.ok()
                } else {
                    futures::future::pending::<Option<CheckpointCompletion>>().await
                }
            } => {
                if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                    state.fault = Some(error);
                    return CoordinatorWait::stop();
                }
                if !state.checkpoint_control_pending {
                    return CoordinatorWait::continue_loop();
                }
                checkpoint_control_due = true;
                if state.checkpoint_control_wake.is_some() {
                    state.checkpoint_control_poll_at =
                        tokio::time::Instant::now() + CheckpointControlWake::capacity_retry();
                }
                None
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
            () = async {
                match state.checkpoint_control_wake.as_mut() {
                    Some(wake) => wake.wait_until(state.checkpoint_control_poll_at).await,
                    None => std::future::pending().await,
                }
            }, if !state.checkpoint_control_pending && !callback.is_leader() => {
                state.checkpoint_control_pending = true;
                checkpoint_control_due = true;
                if state.checkpoint_control_wake.is_some() {
                    state.checkpoint_control_poll_at =
                        tokio::time::Instant::now() + CheckpointControlWake::capacity_retry();
                }
                None
            },
            () = tokio::time::sleep_until(state.checkpoint_control_poll_at),
                if state.checkpoint_control_pending && !callback.is_leader() =>
            {
                checkpoint_control_due = true;
                if state.checkpoint_control_wake.is_some() {
                    state.checkpoint_control_poll_at =
                        tokio::time::Instant::now() + CheckpointControlWake::capacity_retry();
                }
                None
            },
            () = std::future::ready(()), if replay_ready => {
                retrying_replay = true;
                None
            },
            () = std::future::ready(()), if parked_ready => self.parked_source_msg.take(),
            msg = self.rx.recv(),
                if state.source_channel_expected && !intake_paused =>
            {
                if let Ok(message) = msg {
                    if !state.batch_window.is_zero() {
                        let authority_lost = wait_coordinator_delay(
                            state.batch_window,
                            #[cfg(feature = "cluster")]
                            self.process_authority.as_deref(),
                        )
                        .await;
                        if authority_lost {
                            state.fault = Some(
                                "cluster process lease expired during source batch window".into(),
                            );
                            return CoordinatorWait::stop();
                        }
                    }
                    Some(message)
                } else {
                    state.fault = Some("all configured source tasks exited unexpectedly".into());
                    return CoordinatorWait::stop();
                }
            }
            authority_lost = wait_coordinator_delay(
                IDLE_TIMEOUT,
                #[cfg(feature = "cluster")]
                self.process_authority.as_deref(),
            ) => {
                if authority_lost {
                    state.fault =
                        Some("cluster process lease expired while coordinator was idle".into());
                    return CoordinatorWait::stop();
                }
                None
            },
        };

        CoordinatorWait::cycle(CoordinatorWake {
            message,
            retrying_replay,
            checkpoint_control_due,
            gates: CoordinatorGates { intake_paused },
        })
    }

    async fn service_background_work<C: PipelineCallback>(
        &mut self,
        callback: &mut C,
        state: &mut CoordinatorRunState,
        checkpoint_control_due: bool,
    ) -> bool {
        let started = Instant::now();
        if self.replay_pending && self.pending_barrier.active {
            if let Err(error) = self
                .cancel_pending_barrier_for_stop(
                    callback,
                    "operator input remained deferred before source barrier alignment",
                    true,
                )
                .await
            {
                state.fault = Some(error);
                return false;
            }
        }

        for (source_idx, barrier, checkpoint) in &state.barriers {
            match self
                .handle_barrier(*source_idx, barrier, checkpoint, callback)
                .await
            {
                Ok(()) => {}
                Err(CycleError::Halt(reason)) => {
                    tracing::warn!(%reason, "[LDB-3022] checkpoint drain halted the pipeline");
                    state.halted = true;
                    return false;
                }
                Err(CycleError::Fatal(reason) | CycleError::Recovery(reason)) => {
                    state.fault = Some(reason);
                    return false;
                }
            }
        }
        if self.terminal_shutdown.is_cancelled() {
            return false;
        }
        if let Some(reason) = callback.take_pipeline_fault() {
            self.discard_pending_offsets();
            tracing::error!(
                reason = %reason,
                "[LDB-3024] pipeline consistency fault; stopping for recovery"
            );
            state.fault = Some(reason);
            return false;
        }

        let within_budget =
            started.elapsed() < Duration::from_nanos(self.config.background_budget_ns);
        let follower_control_ready =
            state.checkpoint_control_pending && checkpoint_control_due && !callback.is_leader();
        let checkpoint_work_due =
            callback.is_leader() || follower_control_ready || !self.manual_waiting.is_empty();
        if !self.replay_pending && checkpoint_work_due && (follower_control_ready || within_budget)
        {
            let control_serviced = self.maybe_checkpoint(callback).await;
            if follower_control_ready {
                #[cfg(feature = "cluster")]
                if let Some(wake) = state.checkpoint_control_wake.as_ref() {
                    if control_serviced {
                        state.checkpoint_control_pending = false;
                        state.checkpoint_control_poll_at =
                            tokio::time::Instant::now() + wake.fallback();
                    } else {
                        state.checkpoint_control_poll_at =
                            tokio::time::Instant::now() + CheckpointControlWake::capacity_retry();
                    }
                }
                #[cfg(not(feature = "cluster"))]
                if state.checkpoint_control_wake.is_some() {
                    state.checkpoint_control_poll_at = tokio::time::Instant::now()
                        + if control_serviced {
                            CheckpointControlWake::fallback()
                        } else {
                            CheckpointControlWake::capacity_retry()
                        };
                    state.checkpoint_control_pending &= !control_serviced;
                }
            }
            if let Some(reason) = callback.take_pipeline_fault() {
                self.discard_pending_offsets();
                tracing::error!(
                    reason = %reason,
                    "[LDB-3024] checkpoint control fault; stopping for recovery"
                );
                state.fault = Some(reason);
                return false;
            }
        }

        while let Ok(message) = self.control_rx.try_recv() {
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("pipeline control mutation") {
                state.fault = Some(error.to_string());
                return false;
            }
            callback.apply_control(message);
        }

        if self.pending_barrier.active
            && self.pending_barrier.started_at.elapsed() > self.config.checkpoint_timeout
        {
            if let Err(error) = self
                .cancel_pending_barrier_for_stop(callback, "source barrier alignment timeout", true)
                .await
            {
                state.fault = Some(error);
                return false;
            }
        }
        true
    }

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
        let cancelled_before_ready = self.terminal_shutdown.is_cancelled();
        if let Some(ready) = ready {
            if cancelled_before_ready {
                ready.send(Err(
                    "pipeline runtime generation was cancelled before readiness".into(),
                ));
            } else {
                ready.send(Ok(()));
            }
        }
        if !cancelled_before_ready && !self.terminal_shutdown.is_cancelled() {
            // Readiness is the linearization point: source tasks are released only after it is
            // published, so none can enqueue a batch, barrier, or fault on the pre-ready side.
            for handle in &mut self.source_handles {
                if let Some(activation) = handle.startup_activation.take() {
                    activation.send(());
                }
            }
        }

        let mut state = CoordinatorRunState {
            batch_window: self.config.batch_window,
            checkpoint_control_wake: callback.checkpoint_control_wake(),
            checkpoint_control_poll_at: tokio::time::Instant::now(),
            checkpoint_control_pending: false,
            barriers: Vec::new(),
            fault: None,
            halted: false,
            source_channel_expected: !self.source_names.is_empty(),
        };

        loop {
            if self.terminal_shutdown.is_cancelled() {
                break;
            }
            let wait = self.wait_for_cycle(&mut callback, &mut state).await;
            match wait.action {
                CoordinatorWaitAction::Cycle => {}
                CoordinatorWaitAction::Continue => continue,
                CoordinatorWaitAction::Stop => break,
            }
            let CoordinatorWake {
                message: msg,
                retrying_replay,
                checkpoint_control_due,
                gates: CoordinatorGates { intake_paused },
            } = wait.wake;
            // Recheck after the await: recovery may have closed the gate after this loop removed
            // a message from the source FIFO. Keep that message ahead of later FIFO entries so a
            // transient close/reopen cannot silently lose it. A fenced shutdown still discards
            // all open-epoch data below, where recovery owns the rewind.
            if intake_paused || callback.intake_paused() {
                if let Some(message) = msg {
                    if self.parked_source_msg.is_some() {
                        state.fault = Some(
                            "source intake gate race exceeded its single parked-message slot"
                                .into(),
                        );
                        break;
                    }
                    self.parked_source_msg = Some(message);
                }
                #[cfg(feature = "cluster")]
                if let Err(error) =
                    self.require_process_authority("fenced vnode transition completion")
                {
                    state.fault = Some(error.to_string());
                    break;
                }
                match callback.complete_pending_vnode_transition().await {
                    Ok(_) => {}
                    Err(CycleError::Halt(reason)) => {
                        tracing::warn!(%reason, "[LDB-3022] fenced vnode transition halted");
                        break;
                    }
                    Err(CycleError::Recovery(reason) | CycleError::Fatal(reason)) => {
                        state.fault = Some(format!(
                            "fenced vnode transition completion failed: {reason}"
                        ));
                        break;
                    }
                }
                continue;
            }

            self.source_batches_buf.clear();
            self.reset_barrier_seen_for_cycle();
            if !retrying_replay && !self.replay_pending {
                self.discard_pending_offsets();
            }
            state.barriers.clear();
            let mut cycle_events: u64 = 0;
            let cycle_start = Instant::now();

            let had_data = msg.is_some();
            if let Some(first_msg) = msg {
                if let Err(error) = self.process_msg(
                    first_msg,
                    &mut callback,
                    &mut state.barriers,
                    &mut cycle_events,
                ) {
                    state.fault = Some(error.to_string());
                }
            }
            if state.fault.is_some() {
                self.discard_pending_offsets();
                break;
            }

            // Coalesce additional buffered messages; stop at count, time budget, or backpressure.
            let mut drain_count = 0;
            let drain_budget = Duration::from_nanos(self.config.drain_budget_ns);
            // `is_backpressured()` bumps a counter, so call it only on active wakeups rather than
            // idle timeouts.
            let backpressured = had_data && callback.is_backpressured();
            if backpressured {
                tracing::debug!("operator graph backpressured — skipping drain");
            }
            while !backpressured
                && drain_count < MAX_DRAIN_PER_CYCLE
                && cycle_start.elapsed() < drain_budget
            {
                match self.rx.try_recv() {
                    Ok(msg) => {
                        if let Err(error) = self.process_msg(
                            msg,
                            &mut callback,
                            &mut state.barriers,
                            &mut cycle_events,
                        ) {
                            state.fault = Some(error.to_string());
                            break;
                        }
                        drain_count += 1;
                    }
                    Err(_) => break,
                }
            }
            if let Ok(source_fault) = self.source_fault_rx.try_recv() {
                state.fault = Some(format!(
                    "source '{}' fault: {}",
                    source_fault.source, source_fault.error
                ));
            }
            if state.fault.is_some() {
                self.discard_pending_offsets();
                break;
            }
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("folding a drained source cycle") {
                self.discard_pending_offsets();
                state.fault = Some(error.to_string());
                break;
            }

            let staged_source_progress = !self.pending_watermark_batches.is_empty();
            let watermark_result = self
                .pending_watermark_batches
                .drain(..)
                .try_for_each(|(name, batch)| callback.extract_watermark(&name, &batch));
            if let Err(error) = watermark_result {
                self.discard_pending_offsets();
                state.fault = Some(error.to_string());
                break;
            }

            if !self.replay_pending {
                callback.tick_idle_watermark();
            }

            // Run empty cycles for filtered source progress and deferred operator work so cursors,
            // watermarks, and retained data do not stall when a source goes quiet.
            if !self.source_batches_buf.is_empty()
                || staged_source_progress
                || self.replay_pending
                || callback.has_deferred_input()
            {
                let wm = callback.current_watermark();
                #[cfg(feature = "cluster")]
                if let Err(error) = self.require_process_authority("operator execution") {
                    self.discard_pending_offsets();
                    state.fault = Some(error.to_string());
                    break;
                }
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(out) => {
                        // Exactly-once / coordinated recovery rewinds the whole pipeline, so
                        // don't partial-commit siblings — recover instead.
                        if out.any_failed && callback.fault_on_cycle_error() {
                            self.discard_pending_offsets();
                            tracing::error!(
                                "[LDB-3021] failure domain faulted; faulting for recovery"
                            );
                            state.fault = Some("isolated domain fault (exactly-once)".to_string());
                            break;
                        }
                        if let Err(error) = self.publish_cycle_outputs(&mut callback, &out).await {
                            let reason = error.to_string();
                            tracing::error!(
                                error = %reason,
                                "cycle output publication failed; faulting for recovery"
                            );
                            state.fault = Some(reason);
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
                                state.fault = Some(msg);
                                break;
                            }
                            // Shutdown already signaled; restarting would just re-trip it.
                            CycleError::Halt(msg) => {
                                tracing::warn!(reason = %msg, "[LDB-3022] cycle halted");
                                break;
                            }
                            // Continuing would drop the drained rows (EO gap), so fault for
                            // recovery under exactly-once or coordinated recovery.
                            CycleError::Fatal(msg) if callback.fault_on_cycle_error() => {
                                tracing::error!(
                                    error = %msg,
                                    "[LDB-3021] fatal SQL cycle error; faulting for recovery"
                                );
                                state.fault = Some(msg);
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
                let elapsed_ns =
                    u64::try_from(cycle_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                callback.record_cycle(cycle_events, 0, elapsed_ns);

                if elapsed_ns >= self.config.cycle_budget_ns {
                    tracing::debug!(
                        elapsed_ms = elapsed_ns / 1_000_000,
                        budget_ms = self.config.cycle_budget_ns / 1_000_000,
                        "cycle budget exceeded — proceeding to background work"
                    );
                }
            }

            if !self
                .service_background_work(&mut callback, &mut state, checkpoint_control_due)
                .await
            {
                break;
            }
        }

        self.finish_run(&mut callback, state.fault).await
    }

    async fn finish_run<C: PipelineCallback>(
        &mut self,
        callback: &mut C,
        mut fault: Option<String>,
    ) -> ExitReason {
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
            .cancel_pending_barrier_for_stop(callback, interrupted_reason, false)
            .await
        {
            fault.get_or_insert(error);
        }

        // Captured tails own durable state and may still need to publish source acknowledgements.
        // Settling them while sources and sinks remain open prevents close from racing commit.
        if let Some(error) = self.settle_checkpoint_tails(callback).await {
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
                if let Some(reason) = self.process_shutdown_msg(msg, callback, &mut drain_events) {
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
        let mut source_channel_closed = false;
        while stopping_sources.iter().any(|task| !task.is_finished())
            && tokio::time::Instant::now() < source_deadline
        {
            while let Ok(msg) = self.rx.try_recv() {
                if fault.is_none() && !intake_fenced {
                    if let Some(reason) =
                        self.process_shutdown_msg(msg, callback, &mut drain_events)
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
            if source_channel_closed {
                // A disconnected receive is immediately ready. Keep yielding so the stable task
                // actor and tracker proofs can publish terminal completion on a current-thread
                // runtime.
                tokio::time::sleep(tick).await;
                continue;
            }
            match tokio::time::timeout(tick, self.rx.recv()).await {
                Ok(Ok(msg)) if fault.is_none() && !intake_fenced => {
                    if let Some(reason) =
                        self.process_shutdown_msg(msg, callback, &mut drain_events)
                    {
                        fault = Some(reason);
                        self.source_batches_buf.clear();
                        self.pending_watermark_batches.clear();
                        self.discard_pending_offsets();
                    }
                }
                Ok(Err(_)) => source_channel_closed = true,
                Ok(Ok(_)) | Err(_) => {}
            }
        }

        for source in stopping_sources {
            Self::reap_source_task(source);
        }

        // Capture messages enqueued immediately before the last source task exited.
        while let Ok(msg) = self.rx.try_recv() {
            if fault.is_none() && !intake_fenced {
                if let Some(reason) = self.process_shutdown_msg(msg, callback, &mut drain_events) {
                    fault = Some(reason);
                    self.source_batches_buf.clear();
                    self.pending_watermark_batches.clear();
                    self.discard_pending_offsets();
                }
            }
        }

        #[cfg(feature = "cluster")]
        if fault.is_none() && !intake_fenced {
            if let Err(error) = self.require_process_authority("folding the shutdown source drain")
            {
                self.source_batches_buf.clear();
                self.pending_watermark_batches.clear();
                self.discard_pending_offsets();
                fault = Some(error.to_string());
            }
        }

        if fault.is_none() && !intake_fenced {
            let staged_source_progress = !self.pending_watermark_batches.is_empty();
            let watermark_result = self
                .pending_watermark_batches
                .drain(..)
                .try_for_each(|(name, batch)| callback.extract_watermark(&name, &batch));
            let watermarks_valid = match watermark_result {
                Ok(()) => true,
                Err(error) => {
                    self.discard_pending_offsets();
                    fault = Some(error.to_string());
                    false
                }
            };
            if watermarks_valid {
                callback.tick_idle_watermark();
            }
            if watermarks_valid
                && (!self.source_batches_buf.is_empty()
                    || staged_source_progress
                    || self.replay_pending
                    || callback.has_deferred_input())
            {
                let cycle_start = Instant::now();
                let wm = callback.current_watermark();
                #[cfg(feature = "cluster")]
                if let Err(error) = self.require_process_authority(
                    "operator execution during the shutdown source drain",
                ) {
                    self.discard_pending_offsets();
                    fault = Some(error.to_string());
                }
                if fault.is_none() {
                    match callback.execute_cycle(&self.source_batches_buf, wm).await {
                        Ok(out) if out.any_failed && callback.fault_on_cycle_error() => {
                            self.discard_pending_offsets();
                            fault = Some(
                        "isolated domain fault during shutdown drain under replay guarantee"
                            .to_string(),
                    );
                        }
                        Ok(out) => match self.publish_cycle_outputs(callback, &out).await {
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
                }
                let elapsed_ns =
                    u64::try_from(cycle_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                callback.record_cycle(drain_events, 0, elapsed_ns);
            }
        }

        // Resolve the durable open-epoch witness before close terminates the actor that owns its
        // rollback. On failure, keep actors live: lifecycle teardown retains their stable handles
        // and retries settlement before issuing close.
        let sink_epoch_settled = match callback.settle_sink_epoch_for_shutdown().await {
            Ok(()) => true,
            Err(error) => {
                match fault.as_mut() {
                    Some(existing) => {
                        existing.push_str("; sink epoch settlement also failed: ");
                        existing.push_str(&error);
                    }
                    None => fault = Some(format!("sink epoch settlement failed: {error}")),
                }
                false
            }
        };

        // No final snapshot is synthesized: open-epoch rows deliberately replay from the last
        // committed cut. Sink close confirms queued writes and releases connector resources only
        // after durable epoch ownership is settled.
        if sink_epoch_settled {
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
    fn stage_batch(
        &mut self,
        source_idx: usize,
        batch: RecordBatch,
        checkpoint: SourceCheckpoint,
        callback: &mut impl PipelineCallback,
        cycle_events: &mut u64,
    ) -> Result<(), CycleError> {
        let name = self.source_names.get(source_idx).cloned().ok_or_else(|| {
            CycleError::Recovery(format!(
                "source batch referenced unknown runtime index {source_idx}"
            ))
        })?;
        let has_mutations = batch.column_by_name(SOURCE_MUTATION_COLUMN).is_some();
        let mutations_admitted = self
            .source_mutations_admitted
            .get(source_idx)
            .copied()
            .ok_or_else(|| {
                CycleError::Recovery(format!(
                    "source '{name}' has no mutation-admission slot at runtime index {source_idx}"
                ))
            })?;
        let visible = strip_source_row_positions(&batch).map_err(|error| {
            CycleError::Recovery(format!(
                "source '{name}' emitted invalid hidden metadata: {error}"
            ))
        })?;
        if has_mutations && !mutations_admitted {
            return Err(CycleError::Recovery(format!(
                "source '{name}' emitted mutations on the ordinary append-only route"
            )));
        }

        // Filter against the pre-drain watermark. Extraction is deferred until after all batches
        // are filtered so one batch cannot make the next batch appear late.
        let filtered = callback.filter_late_rows(&name, &batch)?;
        let pending = self.pending_offsets.get_mut(source_idx).ok_or_else(|| {
            CycleError::Recovery(format!(
                "source '{name}' has no runtime offset slot at index {source_idx}"
            ))
        })?;
        *pending = Some(checkpoint);
        *cycle_events += visible.num_rows() as u64;
        if let Some(filtered) = filtered {
            self.source_batches_buf
                .entry(Arc::clone(&name))
                .or_default()
                .push(filtered);
        }
        self.pending_watermark_batches.push((name, visible));
        Ok(())
    }

    /// Process one source message under the exact source-barrier ordering invariant.
    fn process_msg(
        &mut self,
        msg: SourceMsg,
        callback: &mut impl PipelineCallback,
        barriers: &mut Vec<(usize, CheckpointBarrier, SourceCheckpoint)>,
        cycle_events: &mut u64,
    ) -> Result<(), CycleError> {
        match msg {
            SourceMsg::Batch {
                source_idx,
                batch,
                checkpoint,
            } => {
                if self.barrier_seen.contains(&source_idx) {
                    return Err(CycleError::Recovery(format!(
                        "source {} emitted data after its checkpoint barrier without an exact release",
                        self.source_names
                            .get(source_idx)
                            .map_or("<unknown>", AsRef::as_ref)
                    )));
                }
                self.stage_batch(source_idx, batch, checkpoint, callback, cycle_events)?;
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
            } => self
                .stage_batch(source_idx, batch, checkpoint, callback, cycle_events)
                .err()
                .map(|error| error.to_string()),
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

    /// Merge staged offsets into `committed_offsets` after successful cycle publication.
    fn commit_pending_offsets(&mut self) {
        for (i, pending) in self.pending_offsets.iter_mut().enumerate() {
            if let Some(cp) = pending.take() {
                self.committed_offsets[i] = Some(cp);
            }
        }
    }

    /// Publish materialized views, streams, and sink work before advancing source cursors.
    /// Publication failure is a shared-runtime consistency fault, so every mode recovers.
    async fn publish_cycle_outputs(
        &mut self,
        callback: &mut impl PipelineCallback,
        outcome: &CycleOutcome,
    ) -> Result<(), CycleError> {
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("materialized-view publication") {
            self.discard_pending_offsets();
            return Err(error);
        }
        if let Err(error) = callback.update_mv_stores(&outcome.results) {
            self.discard_pending_offsets();
            return Err(CycleError::Recovery(format!(
                "materialized-view publication failed: {error}"
            )));
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("stream publication") {
            self.discard_pending_offsets();
            return Err(error);
        }
        if let Err(error) = callback.push_to_streams(&outcome.results) {
            self.discard_pending_offsets();
            return Err(CycleError::Recovery(format!(
                "stream publication failed: {error}"
            )));
        }

        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("sink publication") {
            self.discard_pending_offsets();
            return Err(error);
        }
        if let Err(error) = callback.write_to_sinks(&outcome.results, None).await {
            self.discard_pending_offsets();
            return Err(error);
        }

        // A sink command admitted under authority may still be queued when the lease is fenced.
        // Recheck before advancing the in-memory source cursor; the checkpoint path separately
        // FIFO-fences every sink before persisting it.
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("source cursor advancement") {
            self.discard_pending_offsets();
            return Err(error);
        }
        self.settle_pending_offsets(&outcome.failed_sources, &outcome.deferred_sources);
        self.replay_pending = outcome.any_deferred;
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
        if failed.is_empty() && deferred.is_empty() {
            self.commit_pending_offsets();
            return;
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

    /// Discard staged offsets when cycle execution or publication fails.
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
        context: AlignedCheckpointContext,
        source_checkpoints: &FxHashMap<String, SourceCheckpoint>,
    ) -> Result<(), String> {
        let AlignedCheckpointContext {
            cleanup_owner,
            attempt,
            started_at,
            flags,
            assignment_fence,
        } = context;
        let authoritative_abort = matches!(&outcome, BarrierOutcome::Aborted);
        let (cleanup_reason, manual_reason, record_failure) = match outcome {
            BarrierOutcome::Committed(epoch) if epoch == attempt.epoch => {
                #[cfg(feature = "cluster")]
                if let Err(error) = self.require_process_authority("aligned checkpoint publication")
                {
                    let reason = error.to_string();
                    callback.abort_subscription_cut(attempt);
                    self.fail_manual_attempt(attempt, &reason);
                    return Err(reason);
                }
                let publication_error = callback.publish_barrier(attempt).err();
                self.broadcast_epoch_committed(epoch, source_checkpoints);
                self.finish_manual_success(
                    attempt,
                    &crate::checkpoint_coordinator::CheckpointResult {
                        success: true,
                        checkpoint_id: attempt.checkpoint_id,
                        epoch: attempt.epoch,
                        duration: started_at.elapsed(),
                        error: None,
                        failure_disposition: None,
                    },
                );
                return publication_error.map_or(Ok(()), Err);
            }
            BarrierOutcome::Async => {
                return Ok(());
            }
            BarrierOutcome::Committed(epoch) => {
                let reason = format!(
                    "checkpoint callback committed epoch {epoch} for reserved epoch {}",
                    attempt.epoch
                );
                (reason.clone(), reason, true)
            }
            BarrierOutcome::Skipped(reason) => {
                tracing::debug!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason = %reason,
                    "barrier checkpoint skipped"
                );
                (
                    reason.to_string(),
                    format!("manual checkpoint skipped: {reason}"),
                    false,
                )
            }
            BarrierOutcome::CancelledBeforeCapture => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "barrier checkpoint topology closed before state capture"
                );
                let reason = "checkpoint topology closed before state capture";
                (reason.into(), format!("manual {reason}"), false)
            }
            BarrierOutcome::Aborted => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "barrier checkpoint was authoritatively aborted"
                );
                let reason =
                    "checkpoint was aborted by authoritative cluster control before state capture";
                (reason.into(), format!("manual {reason}"), false)
            }
            BarrierOutcome::Failed => {
                tracing::warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "barrier checkpoint failed"
                );
                (
                    "barrier-aligned checkpoint failed before durable tail".into(),
                    "manual barrier-aligned checkpoint failed before the durable tail".into(),
                    true,
                )
            }
        };
        callback.abort_subscription_cut(attempt);
        if authoritative_abort && cleanup_owner == CheckpointCleanupOwner::Follower {
            callback.resolve_authoritative_follower_abort(attempt)?;
        } else {
            Self::cleanup_checkpoint_attempt(
                callback,
                cleanup_owner,
                attempt,
                &cleanup_reason,
                flags,
                assignment_fence,
            )
            .await?;
        }
        if record_failure {
            callback.record_checkpoint_failure(attempt.checkpoint_id, &cleanup_reason);
        }
        self.fail_manual_attempt(attempt, manual_reason);
        Ok(())
    }

    /// Handle a barrier from a source.
    async fn handle_barrier(
        &mut self,
        source_idx: usize,
        barrier: &CheckpointBarrier,
        barrier_checkpoint: &SourceCheckpoint,
        callback: &mut impl PipelineCallback,
    ) -> Result<(), CycleError> {
        let barrier_attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
        if !self.pending_barrier.active || self.pending_barrier.attempt != Some(barrier_attempt) {
            self.release_source_barrier_for(source_idx, barrier_attempt);
            return Ok(());
        }
        if self.pending_barrier.flags != barrier.flags {
            let reason = format!(
                "source barrier flags {:#x} do not match admitted checkpoint flags {:#x}",
                barrier.flags, self.pending_barrier.flags
            );
            self.cancel_pending_barrier_for_stop(callback, &reason, true)
                .await
                .map_err(CycleError::Recovery)?;
            return Err(CycleError::Recovery(reason));
        }
        #[cfg(feature = "cluster")]
        self.require_process_authority("source barrier handling")
            .map_err(|error| CycleError::Recovery(error.to_string()))?;

        self.capture_replayable_barrier_cursor(source_idx, barrier_checkpoint);

        self.pending_barrier.sources_aligned.insert(source_idx);

        if self.pending_barrier.sources_aligned.len() >= self.pending_barrier.sources_total {
            let checkpoints = std::mem::take(&mut self.pending_barrier.source_checkpoints);
            // Clone for fan-out so each source gets the exact checkpoint that was persisted.
            let fan_out = checkpoints.clone();
            let attempt = barrier_attempt;
            let attempt_started = self.pending_barrier.started_at;
            let flags = self.pending_barrier.flags;
            let assignment_fence = self.pending_barrier.assignment_fence.clone();
            let cleanup_owner = self.pending_barrier.cleanup_owner;
            self.pending_barrier.clear();
            let attempt_deadline =
                tokio::time::Instant::from_std(attempt_started) + self.config.checkpoint_timeout;
            if let Err(error) = callback
                .drain_checkpoint_edges_until(attempt_deadline)
                .await
            {
                let error = match error {
                    CycleError::Halt(error) => CycleError::Halt(error),
                    CycleError::Fatal(error) | CycleError::Recovery(error) => {
                        CycleError::Recovery(error)
                    }
                };
                let reason = error.to_string();
                self.handle_aligned_checkpoint_outcome(
                    callback,
                    BarrierOutcome::Failed,
                    AlignedCheckpointContext {
                        cleanup_owner,
                        attempt,
                        started_at: attempt_started,
                        flags,
                        assignment_fence,
                    },
                    &fan_out,
                )
                .await
                .map_err(|cleanup| {
                    CycleError::Recovery(format!("{reason}; checkpoint cleanup failed: {cleanup}"))
                })?;
                return Err(error);
            }
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("aligned checkpoint capture") {
                let reason = error.to_string();
                let cleanup = Self::cleanup_checkpoint_attempt(
                    callback,
                    cleanup_owner,
                    attempt,
                    &reason,
                    flags,
                    assignment_fence.clone(),
                )
                .await;
                self.fail_manual_attempt(attempt, &reason);
                self.release_source_barrier_attempt(attempt);
                cleanup.map_err(|cleanup| {
                    CycleError::Recovery(format!("{reason}; checkpoint cleanup failed: {cleanup}"))
                })?;
                return Err(CycleError::Recovery(reason));
            }
            if let Err(error) = callback.reserve_subscription_cut(attempt) {
                self.handle_aligned_checkpoint_outcome(
                    callback,
                    BarrierOutcome::Failed,
                    AlignedCheckpointContext {
                        cleanup_owner,
                        attempt,
                        started_at: attempt_started,
                        flags,
                        assignment_fence,
                    },
                    &fan_out,
                )
                .await
                .map_err(|cleanup| {
                    CycleError::Recovery(format!("{error}; checkpoint cleanup failed: {cleanup}"))
                })?;
                return Err(CycleError::Recovery(error));
            }
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("aligned checkpoint capture start") {
                let reason = error.to_string();
                callback.abort_subscription_cut(attempt);
                let cleanup = Self::cleanup_checkpoint_attempt(
                    callback,
                    cleanup_owner,
                    attempt,
                    &reason,
                    flags,
                    assignment_fence.clone(),
                )
                .await;
                self.fail_manual_attempt(attempt, &reason);
                self.release_source_barrier_attempt(attempt);
                cleanup.map_err(|cleanup| {
                    CycleError::Recovery(format!("{reason}; checkpoint cleanup failed: {cleanup}"))
                })?;
                return Err(CycleError::Recovery(reason));
            }
            let outcome = callback
                .checkpoint_with_barrier(
                    checkpoints,
                    attempt,
                    attempt_started,
                    flags,
                    assignment_fence.clone(),
                )
                .await;
            let topology_cancelled = matches!(&outcome, BarrierOutcome::CancelledBeforeCapture);
            let durable_tail_pending = matches!(&outcome, BarrierOutcome::Async);
            self.handle_aligned_checkpoint_outcome(
                callback,
                outcome,
                AlignedCheckpointContext {
                    cleanup_owner,
                    attempt,
                    started_at: attempt_started,
                    flags,
                    assignment_fence,
                },
                &fan_out,
            )
            .await
            .map_err(CycleError::Recovery)?;
            if let Some(error) = callback.take_pipeline_fault() {
                return Err(CycleError::Recovery(error));
            }
            // Capture or exact cleanup has completed. A cleanup failure or sticky replay fault
            // returns above and deliberately leaves the sources held for coordinated recovery.
            self.release_source_barrier_attempt(attempt);
            if topology_cancelled && cleanup_owner == CheckpointCleanupOwner::Originator {
                self.defer_checkpoint_until_topology_ready();
            } else if !topology_cancelled && !durable_tail_pending {
                self.advance_checkpoint_cadence();
            }
        }
        Ok(())
    }

    fn checkpoint_capacity_available(&self) -> bool {
        !self.pending_barrier.active && self.checkpoint_in_flight.load(Ordering::Acquire) < 1
    }

    fn advance_checkpoint_cadence(&mut self) {
        self.last_checkpoint = Instant::now();
        self.checkpoint_retry_not_before = None;
        self.checkpoint_retry_backoff = Duration::ZERO;
    }

    fn defer_checkpoint_until_topology_ready(&mut self) {
        let backoff = if self.checkpoint_retry_backoff.is_zero() {
            CHECKPOINT_RETRY_BASE
        } else {
            self.checkpoint_retry_backoff
                .saturating_mul(2)
                .min(CHECKPOINT_RETRY_MAX)
        };
        self.checkpoint_retry_backoff = backoff;
        self.checkpoint_retry_not_before = Some(Instant::now() + backoff);
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
                .checkpoint_schedule
                .periodic_interval()
                .is_some_and(|value| self.last_checkpoint.elapsed() >= value);
        let retry_ready = self
            .checkpoint_retry_not_before
            .is_none_or(|deadline| Instant::now() >= deadline);
        if !manual && (!interval || !retry_ready) {
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
        let (assignment_fence, flags) = match callback.checkpoint_assignment_for_admission().await {
            CheckpointAssignmentAdmission::Ready {
                assignment_fence,
                flags,
            } => (assignment_fence, flags),
            CheckpointAssignmentAdmission::Deferred(reason) => {
                tracing::debug!(reason = %reason, "checkpoint admission waits for stable topology");
                self.defer_checkpoint_until_topology_ready();
                if manual {
                    self.fail_waiting_manual(format!(
                        "[LDB-6056] manual checkpoint rejected: {reason}"
                    ));
                }
                return None;
            }
            CheckpointAssignmentAdmission::Fault(reason) => {
                callback.record_checkpoint_admission_failure(&reason);
                if manual {
                    self.fail_waiting_manual(format!(
                        "[LDB-6056] manual checkpoint rejected: {reason}"
                    ));
                }
                return None;
            }
        };
        if manual
            && self.manual_handoff_required
            && flags & laminar_core::checkpoint::flags::HANDOFF == 0
        {
            let reason = "assignment handoff ended before its replay-quiescent checkpoint";
            callback.record_checkpoint_admission_failure(reason);
            self.fail_waiting_manual(reason);
            return None;
        }
        if !self.checkpoint_capacity_available() {
            return None;
        }
        Some(CheckpointAdmission {
            manual,
            flags,
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
        tracing::info!(
            checkpoint_id = attempt.checkpoint_id,
            epoch = attempt.epoch,
            "checkpoint attempt reserved"
        );
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("checkpoint prepare publication") {
            let reason = error.to_string();
            callback
                .abandon_checkpoint_attempt(
                    attempt,
                    &reason,
                    admission.flags,
                    admission.assignment_fence.clone(),
                )
                .await
                .map_err(|cleanup| {
                    format!("{reason}; reserved checkpoint cleanup failed: {cleanup}")
                })?;
            return Err(reason);
        }
        if let Err(error) = callback
            .publish_checkpoint_prepare(
                attempt,
                attempt_started,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await
        {
            if let Err(cleanup_error) = callback
                .abandon_checkpoint_attempt(
                    attempt,
                    &error,
                    admission.flags,
                    admission.assignment_fence.clone(),
                )
                .await
            {
                return Err(format!(
                    "{error}; reserved checkpoint cleanup failed: {cleanup_error}"
                ));
            }
            return Err(error);
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("checkpoint prepare completion") {
            let reason = error.to_string();
            callback
                .abandon_checkpoint_attempt(
                    attempt,
                    &reason,
                    admission.flags,
                    admission.assignment_fence.clone(),
                )
                .await
                .map_err(|cleanup| {
                    format!("{reason}; prepared checkpoint cleanup failed: {cleanup}")
                })?;
            return Err(reason);
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
        let (cleanup_reason, manual_reason, record_failure) = match outcome {
            BarrierOutcome::Committed(epoch) if epoch == attempt.epoch => {
                #[cfg(feature = "cluster")]
                if let Err(error) =
                    self.require_process_authority("source-less checkpoint publication")
                {
                    let reason = error.to_string();
                    callback.abort_subscription_cut(attempt);
                    self.fail_manual_attempt(attempt, &reason);
                    return Err(reason);
                }
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
                return publication_error.map_or(Ok(()), Err);
            }
            BarrierOutcome::Committed(epoch) => {
                let reason = format!(
                    "checkpoint callback committed epoch {epoch} for reserved epoch {}",
                    attempt.epoch
                );
                (reason.clone(), reason, true)
            }
            BarrierOutcome::Async => return Ok(()),
            BarrierOutcome::Skipped(reason) => {
                tracing::debug!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason = %reason,
                    "source-less checkpoint skipped"
                );
                (
                    reason.to_string(),
                    format!("manual checkpoint skipped: {reason}"),
                    false,
                )
            }
            BarrierOutcome::CancelledBeforeCapture => {
                let reason = "checkpoint topology closed before state capture";
                (reason.into(), format!("manual {reason}"), false)
            }
            BarrierOutcome::Aborted => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "source-less checkpoint was authoritatively aborted"
                );
                let reason =
                    "checkpoint was aborted by authoritative cluster control before state capture";
                (reason.into(), format!("manual {reason}"), false)
            }
            BarrierOutcome::Failed => (
                "source-less checkpoint failed before durable tail".into(),
                "manual source-less checkpoint failed before the durable tail".into(),
                true,
            ),
        };
        callback.abort_subscription_cut(attempt);
        Self::cleanup_checkpoint_attempt(
            callback,
            CheckpointCleanupOwner::Originator,
            attempt,
            &cleanup_reason,
            admission.flags,
            admission.assignment_fence.clone(),
        )
        .await?;
        if record_failure {
            callback.record_checkpoint_failure(attempt.checkpoint_id, &cleanup_reason);
        }
        self.fail_manual_attempt(attempt, manual_reason);
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
                callback.record_checkpoint_admission_failure(&error);
                if admission.manual {
                    self.fail_waiting_manual(format!(
                        "manual checkpoint attempt reservation failed: {error}"
                    ));
                }
                return;
            }
        };
        self.complete_prepared_source_less_checkpoint(
            callback,
            admission,
            attempt,
            attempt_started,
        )
        .await;
    }

    async fn complete_prepared_source_less_checkpoint(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt: CheckpointAttempt,
        attempt_started: Instant,
    ) {
        if admission.manual {
            self.activate_manual_attempt(attempt, admission.flags);
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
            self.advance_checkpoint_cadence();
            return;
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("source-less checkpoint capture") {
            let reason = error.to_string();
            let cleanup = Self::cleanup_checkpoint_attempt(
                callback,
                CheckpointCleanupOwner::Originator,
                attempt,
                &reason,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
            self.fail_manual_attempt(attempt, &reason);
            callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
            if let Err(cleanup_error) = cleanup {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{reason}; checkpoint cleanup failed: {cleanup_error}"),
                );
            }
            self.advance_checkpoint_cadence();
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
            self.advance_checkpoint_cadence();
            return;
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("source-less checkpoint capture start") {
            let reason = error.to_string();
            callback.abort_subscription_cut(attempt);
            let cleanup = Self::cleanup_checkpoint_attempt(
                callback,
                CheckpointCleanupOwner::Originator,
                attempt,
                &reason,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
            self.fail_manual_attempt(attempt, &reason);
            callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
            if let Err(cleanup_error) = cleanup {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{reason}; checkpoint cleanup failed: {cleanup_error}"),
                );
            }
            self.advance_checkpoint_cadence();
            return;
        }
        let outcome = callback
            .checkpoint_with_barrier(
                FxHashMap::default(),
                attempt,
                attempt_started,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
        let retry_after_topology_change =
            matches!(&outcome, BarrierOutcome::CancelledBeforeCapture);
        let durable_tail_pending = matches!(&outcome, BarrierOutcome::Async);
        if let Err(error) = self
            .handle_source_less_checkpoint_outcome(callback, admission, attempt, outcome)
            .await
        {
            callback.record_checkpoint_continuation_fault(attempt, &error);
        }
        if retry_after_topology_change {
            self.defer_checkpoint_until_topology_ready();
        } else if !durable_tail_pending {
            self.advance_checkpoint_cadence();
        }
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
                callback.record_checkpoint_admission_failure(&error);
                if admission.manual {
                    self.fail_waiting_manual(format!(
                        "manual checkpoint attempt reservation failed: {error}"
                    ));
                }
                return;
            }
        };
        self.inject_prepared_source_barrier_attempt(callback, admission, attempt, attempt_started)
            .await;
    }

    async fn inject_prepared_source_barrier_attempt(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt: CheckpointAttempt,
        attempt_started: Instant,
    ) {
        if admission.manual {
            self.activate_manual_attempt(attempt, admission.flags);
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("source barrier injection") {
            let reason = error.to_string();
            let cleanup = Self::cleanup_checkpoint_attempt(
                callback,
                CheckpointCleanupOwner::Originator,
                attempt,
                &reason,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
            self.fail_manual_attempt(attempt, &reason);
            callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
            if let Err(cleanup_error) = cleanup {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{reason}; checkpoint cleanup failed: {cleanup_error}"),
                );
            }
            return;
        }
        self.pending_barrier.reset_with_assignment(
            attempt,
            self.source_handles.len(),
            admission.flags,
            admission.assignment_fence.clone(),
        );
        // Attempt time includes reservation, alignment, capture, quorum, and publication.
        self.pending_barrier.started_at = attempt_started;
        let barrier = CheckpointBarrier {
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            flags: admission.flags,
        };

        for handle in &self.source_handles {
            if !handle.barrier_injector.trigger(barrier) {
                self.pending_barrier.clear();
                self.cancel_local_source_barriers(barrier);
                let cleanup = Self::cleanup_checkpoint_attempt(
                    callback,
                    CheckpointCleanupOwner::Originator,
                    attempt,
                    "source barrier injection was rejected after preflight",
                    admission.flags,
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
    async fn maybe_checkpoint(&mut self, callback: &mut impl PipelineCallback) -> bool {
        self.drain_manual_requests();
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("checkpoint admission") {
            let reason = error.to_string();
            callback.record_checkpoint_admission_failure(&reason);
            self.fail_waiting_manual(reason);
            return true;
        }

        // Followers do not originate attempts. Preserve their resource cap while servicing the
        // leader's exact control announcement; leader/local admission applies its own cap below.
        if !callback.is_leader() {
            if !self.manual_waiting.is_empty() {
                self.fail_waiting_manual("only the cluster leader may admit a manual checkpoint");
            }
            if !self.checkpoint_capacity_available() {
                return false;
            }
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("checkpoint control admission") {
                callback.record_checkpoint_admission_failure(&error.to_string());
                return true;
            }
            let outcome = callback
                .service_checkpoint_control(self.current_source_offsets())
                .await;
            #[cfg(feature = "cluster")]
            if let Err(error) =
                self.require_process_authority("follower checkpoint control application")
            {
                let authority_reason = error.to_string();
                match &outcome {
                    CheckpointControlOutcome::Started { attempt, .. }
                    | CheckpointControlOutcome::Failed { attempt, .. } => {
                        callback
                            .record_checkpoint_failure(attempt.checkpoint_id, &authority_reason);
                    }
                    CheckpointControlOutcome::AdmissionFailed { error } => callback
                        .record_checkpoint_admission_failure(&format!(
                            "{error}; {authority_reason}"
                        )),
                    CheckpointControlOutcome::Idle
                    | CheckpointControlOutcome::Aborted { .. }
                    | CheckpointControlOutcome::Cancelled { .. } => {
                        callback.record_checkpoint_admission_failure(&authority_reason);
                    }
                }
                return true;
            }
            match outcome {
                CheckpointControlOutcome::Idle => {}
                CheckpointControlOutcome::AdmissionFailed { error } => {
                    callback.record_checkpoint_admission_failure(&error);
                }
                CheckpointControlOutcome::Started {
                    attempt,
                    captured,
                    flags,
                } => {
                    if !captured {
                        self.pending_barrier.reset_follower(
                            attempt,
                            self.source_handles.len(),
                            flags,
                        );
                    }
                }
                CheckpointControlOutcome::Aborted { attempt } => {
                    if self.pending_barrier.attempt == Some(attempt) {
                        self.pending_barrier.clear();
                        self.barrier_seen.clear();
                    }
                    self.release_source_barrier_attempt(attempt);
                    self.fail_manual_attempt(
                        attempt,
                        "manual checkpoint was aborted by authoritative cluster control",
                    );
                }
                CheckpointControlOutcome::Cancelled { attempt } => {
                    if self.pending_barrier.attempt == Some(attempt) {
                        self.pending_barrier.clear();
                        self.barrier_seen.clear();
                    }
                    self.release_source_barrier_attempt(attempt);
                    self.fail_manual_attempt(
                        attempt,
                        "manual checkpoint was cancelled after its shuffle scope closed",
                    );
                }
                CheckpointControlOutcome::Failed { attempt, error } => {
                    callback.record_checkpoint_failure(attempt.checkpoint_id, &error);
                }
            }
            return true;
        }
        let Some(admission) = self.checkpoint_admission(callback).await else {
            return true;
        };
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("checkpoint attempt creation") {
            let reason = error.to_string();
            callback.record_checkpoint_admission_failure(&reason);
            self.fail_waiting_manual(reason);
            return true;
        }
        if self.source_handles.is_empty() {
            self.admit_source_less_checkpoint(callback, &admission)
                .await;
        } else {
            self.admit_source_barrier_checkpoint(callback, &admission)
                .await;
        }
        true
    }
}

#[cfg(test)]
mod tests;
