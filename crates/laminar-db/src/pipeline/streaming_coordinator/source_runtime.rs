//! Mechanically extracted coordinator responsibility.

use super::{
    Arc, AtomicBool, CheckpointBarrier, ConnectorTaskTracker, DbError, Duration, Ordering,
    OwnedConnectorTaskFences, RecordBatch, SourceBatchCursor, SourceCheckpoint, SourceMsgTx,
};
#[cfg(feature = "cluster")]
use super::{
    AssignmentDrainId, CheckpointParticipant, ClusterController, SourceDrainOutcome,
    SourceDrainRequest, SourceDrainResolution,
};

#[cfg(feature = "cluster")]
pub(super) struct SourceProcessAuthority {
    controller: Arc<ClusterController>,
    pub(super) lost: tokio_util::sync::CancellationToken,
    pub(super) watcher_abort: Option<tokio::task::AbortHandle>,
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
    pub(super) fn new(controller: Arc<ClusterController>) -> Arc<Self> {
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
    pub(super) fn is_live(&self) -> bool {
        !self.lost.is_cancelled() && self.controller.process_lease_is_live()
    }

    pub(super) async fn cancelled(&self) {
        self.lost.cancelled().await;
    }
}

#[cfg(feature = "cluster")]
#[inline]
pub(super) fn source_process_authority_is_live(authority: Option<&SourceProcessAuthority>) -> bool {
    authority.is_none_or(SourceProcessAuthority::is_live)
}

/// Wait for coordinator batching or idle work without letting process-lease loss remain hidden
/// behind the timer.
pub(super) async fn wait_coordinator_delay(
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

/// Message from a source task to the coordinator with its production-time cursor.
pub(super) enum SourceMsg {
    Batch {
        source_idx: usize,
        batch: RecordBatch,
        /// Committed to `committed_offsets` only after successful cycle publication.
        cursor: SourceBatchCursor,
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
pub(super) async fn send_source_msg(
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
pub(super) struct SourceFault {
    pub(super) source: Arc<str>,
    pub(super) error: String,
}

/// Reports every source-task exit that was not initiated by coordinator shutdown, including a
/// panic while polling connector code. `UnboundedSender::send` is synchronous and allocation-only,
/// so this remains reliable from `Drop` during unwinding without blocking the data path.
pub(super) struct SourceTaskExitGuard {
    pub(super) source: Arc<str>,
    pub(super) expected_shutdown: Arc<AtomicBool>,
    pub(super) fault_tx: tokio::sync::mpsc::UnboundedSender<SourceFault>,
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

pub(super) struct SourceActorTerminalState {
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

    pub(super) fn is_finished(&self) -> bool {
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
pub(super) struct SourceActorLifetime(Arc<SourceActorTerminalState>);

#[cfg(test)]
impl Drop for SourceActorLifetime {
    fn drop(&mut self) {
        self.0.finish();
    }
}

#[cfg(test)]
pub(super) fn source_actor_terminal_guard() -> (SourceActorLifetime, Arc<SourceActorTerminalState>)
{
    let terminal = Arc::new(SourceActorTerminalState::new());
    (SourceActorLifetime(Arc::clone(&terminal)), terminal)
}

pub(super) struct SourceActorFuture<F> {
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

pub(super) fn spawn_source_actor<F>(
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
pub(super) enum SourceTaskOutcome {
    Success,
    Cancelled,
    Failed(Arc<str>),
}

pub(super) struct SourceTaskState {
    pub(super) name: Arc<str>,
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
pub(super) enum SourceDrainCommand {
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
#[derive(Clone, Copy)]
pub(super) enum SourceDrainCommandPolicy {
    Any,
    ResolveOnly,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Debug)]
pub(super) enum SourceDrainTaskStatus {
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
pub(super) struct SourceDrainReceipt {
    pub(super) round: AssignmentDrainId,
    pub(super) participant: CheckpointParticipant,
    pub(super) source_task_incarnation: uuid::Uuid,
}

#[cfg(feature = "cluster")]
impl SourceDrainReceipt {
    pub(super) fn is_canonical(&self) -> bool {
        self.round.is_canonical()
            && self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && !self.source_task_incarnation.is_nil()
    }
}

#[cfg(feature = "cluster")]
pub(super) fn validate_source_drain_receipts(
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
pub(super) struct SourceDrainLeaseControl {
    pub(super) task_incarnation: uuid::Uuid,
    pub(super) command_tx: tokio::sync::watch::Sender<Option<SourceDrainCommand>>,
    pub(super) status_tx: tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    pub(super) wake: Arc<tokio::sync::Notify>,
}

#[cfg(feature = "cluster")]
pub(super) struct ActiveSourceDrain {
    pub(super) request: SourceDrainRequest,
    pub(super) participant: CheckpointParticipant,
    pub(super) provider_drain: bool,
    pub(super) prepare_deadline: tokio::time::Instant,
    pub(super) ready: bool,
    pub(super) pending_resolution: Option<PendingSourceDrainResolution>,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Copy)]
pub(super) struct PendingSourceDrainResolution {
    pub(super) resolution: SourceDrainResolution,
    pub(super) deadline: tokio::time::Instant,
}

/// DB-owned proof that one source generation has reached terminal completion.
///
/// Actor exit is published by a wrapper that owns the connector future before spawn, and connector
/// children are observed through the exact tracker captured at construction. Neither proof depends
/// on the detached outcome supervisor continuing to run.
#[derive(Clone)]
pub(crate) struct SourceTaskLease {
    pub(super) state: Arc<SourceTaskState>,
}

pub(crate) type OwnedSourceTasks = Arc<parking_lot::Mutex<Vec<SourceTaskLease>>>;

impl SourceTaskLease {
    pub(super) fn supervise(
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
    pub(super) fn install_drain_control(&self, control: SourceDrainLeaseControl) {
        *self.state.drain.lock() = Some(control);
    }

    #[cfg(feature = "cluster")]
    pub(super) fn drain_control(&self) -> Option<SourceDrainLeaseControl> {
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

    pub(super) fn mark_expected_shutdown(&self) {
        self.state.expected_shutdown.store(true, Ordering::Release);
    }

    pub(super) fn notify_shutdown(&self) {
        self.state.shutdown.notify_one();
    }

    pub(crate) fn abort(&self) {
        self.state.abort.abort();
    }

    pub(super) async fn wait_finished(&self) {
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

/// Stable owner for source generations constructed through
/// [`crate::pipeline::StreamingCoordinator::new`].
#[derive(Clone)]
pub struct StreamingCoordinatorRuntime {
    pub(super) owned_source_tasks: OwnedSourceTasks,
    pub(super) owned_connector_task_fences: OwnedConnectorTaskFences,
    pub(super) construction: Arc<tokio::sync::Mutex<()>>,
    pub(super) active_coordinator: Arc<AtomicBool>,
}

pub(super) struct StreamingCoordinatorGeneration {
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

    pub(super) fn claim_generation(&self) -> Result<StreamingCoordinatorGeneration, DbError> {
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

    pub(super) fn prune_and_require_idle(&self) -> Result<(), DbError> {
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
