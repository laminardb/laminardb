//! Per-sink task that owns a [`SinkConnector`] and processes commands
//! sequentially. Failures and timeouts are reported out of band via the
//! [`SinkEvent`] channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use crossfire::{mpsc, oneshot, AsyncRx, MAsyncTx, SendTimeoutError};
use futures::FutureExt;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, ConnectorTaskTracker, CoordinatedCommitBatch,
    CoordinatedCommitContext, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitter, SinkConnector, SinkContract,
};
use laminar_connectors::error::ConnectorError;
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;
use laminar_core::streaming::Producer;
use tokio::task::JoinHandle;
use tokio::time::Instant;

type SinkCommandTx = MAsyncTx<mpsc::Array<SinkCommand>>;
type SinkCommandRx = AsyncRx<mpsc::Array<SinkCommand>>;

/// Default capacity for the sink command channel.
pub(crate) const DEFAULT_CHANNEL_CAPACITY: usize = 128;

/// Default periodic flush interval for sink tasks.
#[cfg(test)]
pub(crate) const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) const SINK_EVENT_CHANNEL_CAPACITY: usize = 1024;
const SINK_CLOSE_TIMEOUT: Duration = Duration::from_secs(15);

enum ConnectorOperationOutcome<T> {
    Completed(T),
    Deadline,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

async fn await_connector_operation_local<T>(
    deadline: Instant,
    future: impl std::future::Future<Output = T>,
) -> ConnectorOperationOutcome<T> {
    if Instant::now() >= deadline {
        return ConnectorOperationOutcome::Deadline;
    }
    match tokio::time::timeout_at(deadline, future).await {
        Ok(_) if Instant::now() >= deadline => ConnectorOperationOutcome::Deadline,
        Ok(result) => ConnectorOperationOutcome::Completed(result),
        Err(_) => ConnectorOperationOutcome::Deadline,
    }
}

#[cfg(feature = "cluster")]
async fn await_connector_operation_fenced<T>(
    controller: &ClusterController,
    deadline: Instant,
    future: impl std::future::Future<Output = T>,
) -> ConnectorOperationOutcome<T> {
    tokio::pin!(future);

    tokio::select! {
        biased;
        () = controller.wait_for_process_lease_loss() => {
            ConnectorOperationOutcome::ProcessAuthorityLost
        }
        () = tokio::time::sleep_until(deadline) => {
            ConnectorOperationOutcome::Deadline
        }
        result = &mut future => {
            if !controller.process_lease_is_live() {
                ConnectorOperationOutcome::ProcessAuthorityLost
            } else if Instant::now() >= deadline {
                ConnectorOperationOutcome::Deadline
            } else {
                ConnectorOperationOutcome::Completed(result)
            }
        }
    }
}

#[cfg(feature = "cluster")]
async fn await_connector_operation<T, F, Fut>(
    deadline: Instant,
    process_authority: Option<Arc<ClusterController>>,
    make_future: F,
) -> ConnectorOperationOutcome<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let Some(controller) = process_authority else {
        return await_connector_operation_local(deadline, make_future()).await;
    };
    if !controller.process_lease_is_live() {
        return ConnectorOperationOutcome::ProcessAuthorityLost;
    }
    await_connector_operation_fenced(controller.as_ref(), deadline, make_future()).await
}

#[cfg(not(feature = "cluster"))]
async fn await_connector_operation<T, F, Fut>(
    deadline: Instant,
    make_future: F,
) -> ConnectorOperationOutcome<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    await_connector_operation_local(deadline, make_future()).await
}

async fn bounded_connector_operation<T, F, Fut>(
    sink_name: &str,
    operation: &str,
    deadline: Instant,
    cancellation_policy: ConnectorCancellationPolicy,
    #[cfg(feature = "cluster")] process_authority: Option<Arc<ClusterController>>,
    make_future: F,
) -> (Result<T, ConnectorError>, bool)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, ConnectorError>>,
{
    match await_connector_operation(
        deadline,
        #[cfg(feature = "cluster")]
        process_authority,
        make_future,
    )
    .await
    {
        ConnectorOperationOutcome::Completed(result) => {
            let retire = result
                .as_ref()
                .err()
                .is_some_and(ConnectorError::is_outcome_unknown);
            (result, retire)
        }
        ConnectorOperationOutcome::Deadline => (
            Err(protocol_deadline_error(sink_name, operation)),
            cancellation_policy == ConnectorCancellationPolicy::RetireConnector,
        ),
        #[cfg(feature = "cluster")]
        ConnectorOperationOutcome::ProcessAuthorityLost => {
            (Err(process_authority_error(sink_name, operation)), true)
        }
    }
}

#[cfg(feature = "cluster")]
fn process_authority_error(sink_name: &str, operation: &str) -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "live cluster process lease".into(),
        actual: format!("sink '{sink_name}' lost process authority before {operation}"),
    }
}

fn protocol_deadline_error(sink_name: &str, operation: &str) -> ConnectorError {
    ConnectorError::Internal(format!(
        "sink '{sink_name}' {operation} exceeded its end-to-end deadline"
    ))
}

fn operation_deadline(timeout: Duration) -> Instant {
    Instant::now() + timeout
}

fn close_deadline_error(sink_name: &str, phase: &str) -> ConnectorError {
    ConnectorError::Internal(format!(
        "sink task '{sink_name}' close {phase} exceeded its {SINK_CLOSE_TIMEOUT:?} end-to-end \
         deadline"
    ))
}

fn command_deadline_error(sink_name: &str, operation: &str, timeout: Duration) -> ConnectorError {
    ConnectorError::Internal(format!(
        "sink task '{sink_name}' {operation} exceeded its {timeout:?} end-to-end deadline"
    ))
}

/// Out-of-band events emitted by a sink task; drained once per cycle.
#[derive(Debug, Clone)]
pub(crate) enum SinkEvent {
    FlushError {
        sink_id: Arc<str>,
        epoch: u64,
        operation: &'static str,
        error: String,
    },
    WriteError {
        sink_id: Arc<str>,
        epoch: u64,
        rows: usize,
        error: String,
    },
    WriteTimeout {
        sink_id: Arc<str>,
        epoch: u64,
        rows: usize,
        timeout: Duration,
    },
    WriteEnqueueTimeout {
        sink_id: Arc<str>,
        rows: usize,
        timeout: Duration,
    },
    ChannelClosed {
        sink_id: Arc<str>,
    },
}

pub(crate) struct SinkTaskConfig {
    pub name: String,
    pub sink_id: Arc<str>,
    pub connector: Box<dyn SinkConnector>,
    /// Typed contract already validated by pipeline admission.
    pub contract: SinkContract,
    /// Whether an asynchronous sink failure requires replay/recovery. Best-effort local
    /// pipelines report the loss but deliberately do not leave future state checkpoints wedged.
    pub requires_recovery_on_error: bool,
    pub channel_capacity: usize,
    pub flush_interval: Duration,
    pub write_timeout: Duration,
    pub event_tx: Producer<SinkEvent>,
    /// Exact generation proof captured when the connector was created.
    pub terminal_tasks: Option<ConnectorTaskTracker>,
    #[cfg(feature = "cluster")]
    pub process_authority: Option<Arc<ClusterController>>,
}

pub(crate) struct SinkCommand {
    /// One deadline created before enqueue and shared by queueing, connector I/O and ack.
    deadline: Instant,
    operation: SinkOperation,
}

pub(crate) enum SinkOperation {
    WriteBatch {
        /// Exact epoch generation admitted by the handle-side write gate. Non-committable sinks
        /// do not participate in epoch gating and leave this unset.
        epoch: Option<SinkEpochAdmission>,
        batch: RecordBatch,
    },
    BeginEpoch {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Flush buffered rows without transaction semantics — used to durably land an
    /// at-least-once sink's buffer at checkpoint (CP-5).
    Flush {
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    PreCommit {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<Option<Vec<u8>>, ConnectorError>>,
    },
    /// Designated-committer path: aggregate every writer's descriptor for the
    /// epoch into one external commit (coordinated-commit sinks only).
    CommitAggregated {
        batch: CoordinatedCommitBatch,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Highest exact checkpoint and authority committed in this external namespace.
    CommittedCursor {
        namespace: CoordinatedCommitNamespace,
        ack: oneshot::TxOneshot<Result<Option<CoordinatedCommitCursor>, ConnectorError>>,
    },
    RollbackEpoch {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Acks once all prior commands have been processed.
    Sync {
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Close the connector (abort open transaction, flush) and exit the task. The task reports
    /// the connector result before terminating so shutdown cannot confuse enqueue with durability.
    Close {
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
}

/// Handle-side admission state for checkpoint-committable sink epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkEpochGateState {
    Unopened,
    Open(SinkEpochAdmission),
    Sealed(SinkEpochAdmission),
    Opening(SinkEpochAdmission),
    Begun(SinkEpochAdmission),
    Failed { generation: u64 },
}

/// Exact handle-side generation admitted for one writable sink epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SinkEpochAdmission {
    pub(crate) epoch: u64,
    pub(crate) generation: u64,
}

struct SinkBeginGateGuard {
    gate: tokio::sync::watch::Sender<SinkEpochGateState>,
    admission: SinkEpochAdmission,
    disarmed: bool,
}

impl SinkBeginGateGuard {
    fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for SinkBeginGateGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        self.gate.send_if_modified(|state| {
            if *state == SinkEpochGateState::Opening(self.admission) {
                *state = SinkEpochGateState::Failed {
                    generation: self.admission.generation,
                };
                true
            } else {
                false
            }
        });
    }
}

#[derive(Clone)]
enum SinkCloseOutcome {
    Success,
    Failure(Arc<str>),
}

impl SinkCloseOutcome {
    fn into_result(self) -> Result<(), ConnectorError> {
        match self {
            Self::Success => Ok(()),
            Self::Failure(error) => Err(ConnectorError::Internal(error.to_string())),
        }
    }
}

struct SinkCloseState {
    phase: parking_lot::Mutex<&'static str>,
    outcome: parking_lot::Mutex<Option<SinkCloseOutcome>>,
    notify: tokio::sync::Notify,
}

struct SinkTerminalState {
    actor: Arc<SinkActorState>,
    connector_tasks: Option<ConnectorTaskTracker>,
}

struct SinkActorState {
    accepting: AtomicBool,
    finished: AtomicBool,
    finished_notify: tokio::sync::Notify,
}

impl SinkActorState {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            finished: AtomicBool::new(false),
            finished_notify: tokio::sync::Notify::new(),
        }
    }

    fn stop_admission(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    fn finish(&self) {
        self.stop_admission();
        if !self.finished.swap(true, Ordering::AcqRel) {
            self.finished_notify.notify_waiters();
        }
    }

    async fn wait_finished_until(&self, deadline: Instant) -> bool {
        loop {
            let notified = self.finished_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.finished.load(Ordering::Acquire) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified.as_mut())
                .await
                .is_err()
            {
                return self.finished.load(Ordering::Acquire);
            }
        }
    }
}

#[cfg(test)]
struct SinkActorLifetime(Arc<SinkActorState>);

#[cfg(test)]
impl Drop for SinkActorLifetime {
    fn drop(&mut self) {
        self.0.finish();
    }
}

struct SinkActorFuture<F> {
    actor: Option<std::pin::Pin<Box<F>>>,
    terminal: Arc<SinkActorState>,
}

// Moving this wrapper never moves the separately pinned actor allocation.
impl<F> Unpin for SinkActorFuture<F> {}

impl<F> std::future::Future for SinkActorFuture<F>
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
            .expect("sink actor polled after terminal completion");
        if actor.as_mut().poll(context).is_pending() {
            return std::task::Poll::Pending;
        }
        // Drop the complete actor future, including its connector, before publishing exit.
        self.actor.take();
        self.terminal.finish();
        std::task::Poll::Ready(())
    }
}

impl<F> Drop for SinkActorFuture<F> {
    fn drop(&mut self) {
        // Cancellation must drop the actor and its connector before another generation can observe
        // terminal completion.
        self.actor.take();
        self.terminal.finish();
    }
}

fn spawn_sink_actor<F>(
    runtime: &tokio::runtime::Handle,
    actor: F,
    terminal: Arc<SinkActorState>,
) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    runtime.spawn(SinkActorFuture {
        actor: Some(Box::pin(actor)),
        terminal,
    })
}

impl SinkCloseState {
    fn new() -> Self {
        Self {
            phase: parking_lot::Mutex::new("admission"),
            outcome: parking_lot::Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn set_phase(&self, phase: &'static str) {
        *self.phase.lock() = phase;
    }

    fn phase(&self) -> &'static str {
        *self.phase.lock()
    }

    fn publish_outcome(&self, outcome: SinkCloseOutcome) {
        let mut current = self.outcome.lock();
        if current.is_none() {
            *current = Some(outcome);
        }
        drop(current);
        self.notify.notify_waiters();
    }

    fn finish(&self, outcome: SinkCloseOutcome) {
        self.publish_outcome(outcome);
    }

    fn outcome(&self) -> Option<SinkCloseOutcome> {
        self.outcome.lock().clone()
    }
}

impl SinkTerminalState {
    fn new(actor: Arc<SinkActorState>, connector_tasks: Option<ConnectorTaskTracker>) -> Self {
        Self {
            actor,
            connector_tasks,
        }
    }

    fn is_finished(&self) -> bool {
        self.actor.finished.load(Ordering::Acquire)
            && self
                .connector_tasks
                .as_ref()
                .is_none_or(ConnectorTaskTracker::is_terminated)
    }

    async fn wait_until(&self, deadline: Instant) -> bool {
        if !self.actor.wait_finished_until(deadline).await {
            return false;
        }
        let Some(tasks) = self.connector_tasks.as_ref() else {
            return true;
        };
        if tasks.is_terminated() {
            return true;
        }
        if tokio::time::timeout_at(deadline, tasks.wait_terminated())
            .await
            .is_err()
        {
            return tasks.is_terminated();
        }
        true
    }
}

struct OwnedSinkTask {
    actor_abort: tokio::task::AbortHandle,
    terminal_join: JoinHandle<Result<(), Arc<str>>>,
    terminal_state: Arc<SinkTerminalState>,
}

impl OwnedSinkTask {
    fn abort_actor(&self) {
        self.actor_abort.abort();
    }
}

fn supervise_sink_task(
    actor: JoinHandle<()>,
    terminal_tasks: Option<ConnectorTaskTracker>,
    actor_state: Arc<SinkActorState>,
    runtime: &tokio::runtime::Handle,
) -> OwnedSinkTask {
    let actor_abort = actor.abort_handle();
    let terminal_state = Arc::new(SinkTerminalState::new(actor_state, terminal_tasks.clone()));
    let terminal_join = runtime.spawn(async move {
        let actor_result = actor.await.map_err(|error| Arc::from(error.to_string()));
        if let Some(tasks) = terminal_tasks {
            tasks.wait_terminated().await;
        }
        actor_result
    });
    OwnedSinkTask {
        actor_abort,
        terminal_join,
        terminal_state,
    }
}

/// Handle for sending commands to a sink's dedicated task.
#[derive(Clone)]
pub(crate) struct SinkTaskHandle {
    name: Arc<str>,
    sink_id: Arc<str>,
    tx: SinkCommandTx,
    contract: SinkContract,
    requires_recovery_on_error: bool,
    /// End-to-end budget for enqueue, connector execution and acknowledgement.
    write_timeout: Duration,
    closing: Arc<AtomicBool>,
    /// Linearizes command admission with Close so no producer can enqueue behind it.
    admission: Arc<tokio::sync::Mutex<()>>,
    // The terminal driver takes this exactly once. Public close futures never own the actor or
    // its connector-child termination proof.
    task: Arc<parking_lot::Mutex<Option<OwnedSinkTask>>>,
    close_state: Arc<SinkCloseState>,
    terminal_state: Arc<SinkTerminalState>,
    actor_state: Arc<SinkActorState>,
    /// Runtime that owns the actor. Terminal cleanup must not be spawned on the short-lived
    /// compute callback runtime that happened to call `close()`.
    runtime: tokio::runtime::Handle,
    event_tx: Producer<SinkEvent>,
    /// Sticky for the current epoch. Shared with the actor so a write rejected before enqueue
    /// cannot be hidden from the checkpoint protocol.
    epoch_poisoned: Arc<AtomicBool>,
    /// Checkpoint-committable sinks remain non-writable until the whole sink group has begun the
    /// same allocator-owned epoch. Every clone observes the same transition stream.
    epoch_gate: Option<tokio::sync::watch::Sender<SinkEpochGateState>>,
    #[cfg(feature = "cluster")]
    process_authority: Option<Arc<ClusterController>>,
}

impl SinkTaskHandle {
    /// Spawns a sink task and returns a handle.
    ///
    /// # Panics
    ///
    /// Panics if `config.channel_capacity` is 0.
    pub fn spawn(config: SinkTaskConfig) -> Self {
        assert!(
            config.channel_capacity > 0,
            "sink channel_capacity must be > 0"
        );
        assert!(
            !config.write_timeout.is_zero(),
            "sink write_timeout must be > 0"
        );
        assert!(
            !config.flush_interval.is_zero(),
            "sink flush_interval must be > 0"
        );
        let SinkTaskConfig {
            name,
            sink_id,
            connector,
            contract,
            requires_recovery_on_error,
            channel_capacity,
            flush_interval,
            write_timeout,
            event_tx,
            terminal_tasks,
            #[cfg(feature = "cluster")]
            process_authority,
        } = config;
        let (tx, rx) = mpsc::bounded_async::<SinkCommand>(channel_capacity);
        let task_sink_id = Arc::clone(&sink_id);
        let task_event_tx = event_tx.clone();
        let task_name = name.clone();
        let epoch_poisoned = Arc::new(AtomicBool::new(false));
        let epoch_gate = contract
            .is_checkpoint_committable()
            .then(|| tokio::sync::watch::channel(SinkEpochGateState::Unopened).0);
        let admission = Arc::new(tokio::sync::Mutex::new(()));
        let actor_state = Arc::new(SinkActorState::new());
        let runtime = tokio::runtime::Handle::current();
        let actor_future = run_sink_task(
            SinkTaskInner {
                name: task_name,
                sink_id: task_sink_id,
                sink: connector,
                rx,
                flush_interval,
                write_timeout,
                contract,
                requires_recovery_on_error,
                event_tx: task_event_tx,
                #[cfg(feature = "cluster")]
                process_authority: process_authority.clone(),
                #[cfg(feature = "cluster")]
                admission: Arc::clone(&admission),
            },
            Arc::clone(&epoch_poisoned),
            Arc::clone(&actor_state),
        );
        let actor = spawn_sink_actor(&runtime, actor_future, Arc::clone(&actor_state));
        let task = supervise_sink_task(actor, terminal_tasks, Arc::clone(&actor_state), &runtime);
        let terminal_state = Arc::clone(&task.terminal_state);

        Self {
            name: Arc::from(name),
            sink_id,
            tx,
            contract,
            requires_recovery_on_error,
            write_timeout,
            closing: Arc::new(AtomicBool::new(false)),
            admission,
            task: Arc::new(parking_lot::Mutex::new(Some(task))),
            close_state: Arc::new(SinkCloseState::new()),
            terminal_state,
            actor_state,
            runtime,
            event_tx,
            epoch_poisoned,
            epoch_gate,
            #[cfg(feature = "cluster")]
            process_authority,
        }
    }

    fn closed_err(&self) -> ConnectorError {
        ConnectorError::ConnectionFailed(format!("sink task '{}' closed unexpectedly", self.name))
    }

    fn ack_dropped_err(&self, op: &'static str) -> ConnectorError {
        ConnectorError::ConnectionFailed(format!(
            "sink task '{}' dropped {op} acknowledgment",
            self.name
        ))
    }

    fn poison_epoch_if_recovery_required(&self) {
        if self.requires_recovery_on_error {
            self.epoch_poisoned.store(true, Ordering::Release);
        }
    }

    fn ensure_open(&self) -> Result<(), ConnectorError> {
        if self.closing.load(Ordering::Acquire) {
            return Err(self.closed_err());
        }
        if !self.actor_state.accepting.load(Ordering::Acquire) {
            return Err(self.closed_err());
        }
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.process_authority.as_ref() {
            if !controller.process_lease_is_live() {
                return Err(process_authority_error(&self.name, "command admission"));
            }
        }
        Ok(())
    }

    fn epoch_gate_error(
        &self,
        expected: impl Into<String>,
        actual: SinkEpochGateState,
    ) -> ConnectorError {
        ConnectorError::InvalidState {
            expected: expected.into(),
            actual: format!("sink '{}' epoch gate is {actual:?}", self.name),
        }
    }

    async fn wait_for_open_epoch_until(
        &self,
        deadline: Option<Instant>,
    ) -> Result<Option<SinkEpochAdmission>, ConnectorError> {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return Ok(None);
        };
        let mut state = gate.subscribe();
        loop {
            let observed = *state.borrow_and_update();
            match observed {
                SinkEpochGateState::Open(admission) => return Ok(Some(admission)),
                SinkEpochGateState::Failed { .. } => {
                    return Err(self.epoch_gate_error("a writable sink epoch", observed));
                }
                SinkEpochGateState::Unopened
                | SinkEpochGateState::Sealed(_)
                | SinkEpochGateState::Opening(_)
                | SinkEpochGateState::Begun(_) => {}
            }
            let changed = state.changed();
            tokio::pin!(changed);
            let actor_finished = self.actor_state.finished_notify.notified();
            tokio::pin!(actor_finished);
            actor_finished.as_mut().enable();
            if self.actor_state.finished.load(Ordering::Acquire) {
                return Err(self.closed_err());
            }
            match deadline {
                Some(deadline) => {
                    tokio::select! {
                        biased;
                        result = &mut changed => result.map_err(|_| self.closed_err())?,
                        () = actor_finished.as_mut() => return Err(self.closed_err()),
                        () = tokio::time::sleep_until(deadline) => {
                            return Err(command_deadline_error(
                                &self.name,
                                "sink epoch gate",
                                deadline.saturating_duration_since(Instant::now()),
                            ));
                        }
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        result = &mut changed => result.map_err(|_| self.closed_err())?,
                        () = actor_finished.as_mut() => return Err(self.closed_err()),
                    }
                }
            }
        }
    }

    /// Wait for the checkpoint-committable sink group to publish a writable epoch. This does not
    /// take command admission; `write_batch_before` locks and rechecks the exact generation.
    pub(crate) async fn wait_for_write_gate_until(
        &self,
        supplied_deadline: Option<Instant>,
    ) -> Result<Option<SinkEpochAdmission>, ConnectorError> {
        self.ensure_open()?;
        self.wait_for_open_epoch_until(supplied_deadline).await
    }

    pub(crate) fn begun_epoch_admission(&self, epoch: u64) -> Option<SinkEpochAdmission> {
        self.epoch_gate
            .as_ref()
            .and_then(|gate| match *gate.borrow() {
                SinkEpochGateState::Begun(admission) if admission.epoch == epoch => Some(admission),
                _ => None,
            })
    }

    pub(crate) fn current_begun_epoch_admission(&self) -> Option<SinkEpochAdmission> {
        self.epoch_gate
            .as_ref()
            .and_then(|gate| match *gate.borrow() {
                SinkEpochGateState::Begun(admission) => Some(admission),
                _ => None,
            })
    }

    /// Publish only after the coordinator has preflighted the whole group and made its allocator
    /// reservation Ready. There is deliberately no await or fallible work in this phase.
    pub(crate) fn publish_open_epoch(
        &self,
        admission: SinkEpochAdmission,
    ) -> Result<(), ConnectorError> {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return Ok(());
        };
        let changed = gate.send_if_modified(|state| {
            if *state == SinkEpochGateState::Begun(admission) {
                *state = SinkEpochGateState::Open(admission);
                true
            } else {
                false
            }
        });
        if changed {
            Ok(())
        } else {
            Err(self.epoch_gate_error(
                format!("begun epoch admission {admission:?}"),
                *gate.borrow(),
            ))
        }
    }

    pub(crate) fn fail_epoch_transition(&self, admission: SinkEpochAdmission) {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return;
        };
        gate.send_if_modified(|state| {
            let same_generation = match *state {
                SinkEpochGateState::Open(current)
                | SinkEpochGateState::Sealed(current)
                | SinkEpochGateState::Opening(current)
                | SinkEpochGateState::Begun(current) => current.generation == admission.generation,
                SinkEpochGateState::Failed { generation } => generation == admission.generation,
                SinkEpochGateState::Unopened => false,
            };
            if same_generation && !matches!(*state, SinkEpochGateState::Failed { .. }) {
                *state = SinkEpochGateState::Failed {
                    generation: admission.generation,
                };
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn fail_epoch_gate(&self) {
        if let Some(gate) = self.epoch_gate.as_ref() {
            let generation = match *gate.borrow() {
                SinkEpochGateState::Unopened => 0,
                SinkEpochGateState::Open(admission)
                | SinkEpochGateState::Sealed(admission)
                | SinkEpochGateState::Opening(admission)
                | SinkEpochGateState::Begun(admission) => admission.generation,
                SinkEpochGateState::Failed { generation } => generation,
            };
            gate.send_replace(SinkEpochGateState::Failed { generation });
        }
    }

    pub(crate) fn open_epoch_admission(
        &self,
        epoch: u64,
    ) -> Result<SinkEpochAdmission, ConnectorError> {
        let gate = self.epoch_gate.as_ref().ok_or_else(|| {
            self.epoch_gate_error("checkpoint-committable sink", SinkEpochGateState::Unopened)
        })?;
        match *gate.borrow() {
            SinkEpochGateState::Open(admission) if admission.epoch == epoch => Ok(admission),
            observed => Err(self.epoch_gate_error(format!("open epoch {epoch}"), observed)),
        }
    }

    pub(crate) async fn seal_epoch_until(
        &self,
        admission: SinkEpochAdmission,
        deadline: Instant,
    ) -> Result<SinkEpochAdmission, ConnectorError> {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return Ok(admission);
        };
        let _admission = tokio::time::timeout_at(deadline, self.admission.lock())
            .await
            .map_err(|_| command_deadline_error(&self.name, "epoch seal", self.write_timeout))?;
        self.ensure_open()?;
        let observed = *gate.borrow();
        if observed != SinkEpochGateState::Open(admission) {
            return Err(
                self.epoch_gate_error(format!("open epoch admission {admission:?}"), observed)
            );
        }
        let sealed = SinkEpochAdmission {
            epoch: admission.epoch,
            generation: admission.generation.checked_add(1).ok_or_else(|| {
                ConnectorError::InvalidState {
                    expected: "non-exhausted sink epoch generation".into(),
                    actual: format!("sink '{}' generation overflow", self.name),
                }
            })?,
        };
        gate.send_replace(SinkEpochGateState::Sealed(sealed));
        Ok(sealed)
    }

    /// Idempotent protocol seal for coordinator APIs that do not own a callback transition
    /// guard. It shares write admission, so every accepted write is ordered before the seal.
    pub(crate) async fn seal_epoch_for_protocol_until(
        &self,
        epoch: u64,
        deadline: Instant,
    ) -> Result<Option<SinkEpochAdmission>, ConnectorError> {
        let Some(gate) = self.epoch_gate.as_ref() else {
            return Ok(None);
        };
        let _admission = tokio::time::timeout_at(deadline, self.admission.lock())
            .await
            .map_err(|_| command_deadline_error(&self.name, "epoch seal", self.write_timeout))?;
        self.ensure_open()?;
        let observed = *gate.borrow();
        match observed {
            SinkEpochGateState::Open(admission) if admission.epoch == epoch => {
                let sealed = SinkEpochAdmission {
                    epoch,
                    generation: admission.generation.checked_add(1).ok_or_else(|| {
                        ConnectorError::InvalidState {
                            expected: "non-exhausted sink epoch generation".into(),
                            actual: format!("sink '{}' generation overflow", self.name),
                        }
                    })?,
                };
                gate.send_replace(SinkEpochGateState::Sealed(sealed));
                Ok(Some(sealed))
            }
            SinkEpochGateState::Sealed(admission) if admission.epoch == epoch => {
                Ok(Some(admission))
            }
            _ => Err(self.epoch_gate_error(format!("open or sealed epoch {epoch}"), observed)),
        }
    }

    async fn request<T>(
        &self,
        operation: &'static str,
        make_operation: impl FnOnce(oneshot::TxOneshot<Result<T, ConnectorError>>) -> SinkOperation,
    ) -> Result<T, ConnectorError>
    where
        T: Send + 'static,
    {
        self.request_until(
            operation,
            operation_deadline(self.write_timeout),
            make_operation,
        )
        .await
    }

    /// Submit one protocol command under the earlier of the sink's configured write deadline
    /// and a caller-owned absolute deadline. The selected instant covers queueing, connector I/O,
    /// and acknowledgement; it is stamped into the command before enqueue.
    async fn request_until<T>(
        &self,
        operation: &'static str,
        supplied_deadline: Instant,
        make_operation: impl FnOnce(oneshot::TxOneshot<Result<T, ConnectorError>>) -> SinkOperation,
    ) -> Result<T, ConnectorError>
    where
        T: Send + 'static,
    {
        let started = Instant::now();
        let deadline = operation_deadline(self.write_timeout).min(supplied_deadline);
        let effective_timeout = deadline.saturating_duration_since(started);
        if effective_timeout.is_zero() {
            return Err(command_deadline_error(
                &self.name,
                operation,
                effective_timeout,
            ));
        }
        let admission = tokio::time::timeout_at(deadline, self.admission.lock())
            .await
            .map_err(|_| command_deadline_error(&self.name, operation, effective_timeout))?;
        self.ensure_open()?;
        let (ack_tx, mut ack_rx) = oneshot::oneshot();
        let command = SinkCommand {
            deadline,
            operation: make_operation(ack_tx),
        };
        match self
            .tx
            .send_with_timer(command, tokio::time::sleep_until(deadline))
            .await
        {
            Ok(()) => {}
            Err(SendTimeoutError::Disconnected(_)) => return Err(self.closed_err()),
            Err(SendTimeoutError::Timeout(_)) => {
                return Err(command_deadline_error(
                    &self.name,
                    operation,
                    effective_timeout,
                ));
            }
        }
        drop(admission);
        let actor_finished = self.actor_state.finished_notify.notified();
        tokio::pin!(actor_finished);
        actor_finished.as_mut().enable();
        if self.actor_state.finished.load(Ordering::Acquire) {
            return tokio::select! {
                biased;
                result = &mut ack_rx => match result {
                    Ok(result) => result,
                    Err(_) => Err(self.ack_dropped_err(operation)),
                },
                () = std::future::ready(()) => Err(self.closed_err()),
            };
        }
        tokio::select! {
            biased;
            result = &mut ack_rx => match result {
                Ok(result) => result,
                Err(_) => Err(self.ack_dropped_err(operation)),
            },
            () = actor_finished.as_mut() => Err(self.closed_err()),
            () = tokio::time::sleep_until(deadline) => Err(command_deadline_error(
                &self.name,
                operation,
                effective_timeout,
            )),
        }
    }

    /// Send a batch; backpressures when the sink is behind.
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.write_batch_before(batch, None).await
    }

    /// Send a batch with queue admission and the actor command clamped to the caller's deadline.
    pub async fn write_batch_until(
        &self,
        batch: RecordBatch,
        supplied_deadline: Instant,
    ) -> Result<(), ConnectorError> {
        self.write_batch_before(batch, Some(supplied_deadline))
            .await
    }

    async fn write_batch_before(
        &self,
        batch: RecordBatch,
        supplied_deadline: Option<Instant>,
    ) -> Result<(), ConnectorError> {
        let rows = batch.num_rows();
        let (admission, admitted_epoch, deadline, effective_timeout) = loop {
            let expected_epoch = match self.wait_for_open_epoch_until(supplied_deadline).await {
                Ok(epoch) => epoch,
                Err(error) => {
                    self.poison_epoch_if_recovery_required();
                    return Err(error);
                }
            };
            // Waiting on a checkpoint tail is coordination backpressure, not connector work. The
            // sink's enqueue/I/O budget starts only after a writable generation is observed.
            let started = Instant::now();
            let deadline = supplied_deadline.map_or_else(
                || operation_deadline(self.write_timeout),
                |supplied| supplied.min(operation_deadline(self.write_timeout)),
            );
            let effective_timeout = deadline.saturating_duration_since(started);
            if effective_timeout.is_zero() {
                self.poison_epoch_if_recovery_required();
                let _ = self.event_tx.try_push(SinkEvent::WriteEnqueueTimeout {
                    sink_id: Arc::clone(&self.sink_id),
                    rows,
                    timeout: effective_timeout,
                });
                return Err(command_deadline_error(
                    &self.name,
                    "write admission",
                    effective_timeout,
                ));
            }
            let Ok(admission) = tokio::time::timeout_at(deadline, self.admission.lock()).await
            else {
                self.poison_epoch_if_recovery_required();
                let _ = self.event_tx.try_push(SinkEvent::WriteEnqueueTimeout {
                    sink_id: Arc::clone(&self.sink_id),
                    rows,
                    timeout: effective_timeout,
                });
                return Err(command_deadline_error(
                    &self.name,
                    "write admission",
                    effective_timeout,
                ));
            };
            self.ensure_open()?;
            let still_open = self.epoch_gate.as_ref().is_none_or(|gate| {
                expected_epoch
                    .is_some_and(|epoch| *gate.borrow() == SinkEpochGateState::Open(epoch))
            });
            if still_open {
                break (admission, expected_epoch, deadline, effective_timeout);
            }
            drop(admission);
        };
        let command = SinkCommand {
            deadline,
            operation: SinkOperation::WriteBatch {
                epoch: admitted_epoch,
                batch,
            },
        };
        match self
            .tx
            .send_with_timer(command, tokio::time::sleep_until(deadline))
            .await
        {
            Ok(()) => {
                drop(admission);
                Ok(())
            }
            Err(SendTimeoutError::Disconnected(_)) => {
                self.poison_epoch_if_recovery_required();
                let _ = self.event_tx.try_push(SinkEvent::ChannelClosed {
                    sink_id: Arc::clone(&self.sink_id),
                });
                Err(self.closed_err())
            }
            Err(SendTimeoutError::Timeout(_)) => {
                self.poison_epoch_if_recovery_required();
                let _ = self.event_tx.try_push(SinkEvent::WriteEnqueueTimeout {
                    sink_id: Arc::clone(&self.sink_id),
                    rows,
                    timeout: effective_timeout,
                });
                Err(command_deadline_error(
                    &self.name,
                    "write enqueue",
                    effective_timeout,
                ))
            }
        }
    }

    /// Wait until all previously queued commands have been processed. This is the checkpoint
    /// write fence, so a stuck sink must fail the attempt instead of hanging barrier capture.
    #[cfg(test)]
    pub async fn sync(&self) -> Result<(), ConnectorError> {
        self.request("sync", |ack| SinkOperation::Sync { ack })
            .await
    }

    /// Wait for all preceding writes, clamped by a caller-owned absolute deadline.
    pub async fn sync_until(&self, deadline: Instant) -> Result<(), ConnectorError> {
        self.request_until("sync", deadline, |ack| SinkOperation::Sync { ack })
            .await
    }

    /// Begin an epoch without allowing queueing or connector work past `deadline`.
    pub async fn begin_epoch_until(
        &self,
        epoch: u64,
        deadline: Instant,
    ) -> Result<(), ConnectorError> {
        let mut gate_guard = if let Some(gate) = self.epoch_gate.as_ref() {
            let admission_guard = tokio::time::timeout_at(deadline, self.admission.lock())
                .await
                .map_err(|_| {
                    command_deadline_error(&self.name, "begin-epoch admission", self.write_timeout)
                })?;
            self.ensure_open()?;
            let observed = *gate.borrow();
            let generation = match observed {
                SinkEpochGateState::Unopened => Some(0),
                SinkEpochGateState::Sealed(admission) => Some(admission.generation),
                SinkEpochGateState::Failed { generation } => generation.checked_add(1),
                SinkEpochGateState::Open(_)
                | SinkEpochGateState::Opening(_)
                | SinkEpochGateState::Begun(_) => None,
            };
            let generation = generation.ok_or_else(|| {
                self.epoch_gate_error(
                    format!("unopened, sealed, or failed gate before epoch {epoch}"),
                    observed,
                )
            })?;
            let admission = SinkEpochAdmission { epoch, generation };
            gate.send_replace(SinkEpochGateState::Opening(admission));
            drop(admission_guard);
            Some(SinkBeginGateGuard {
                gate: gate.clone(),
                admission,
                disarmed: false,
            })
        } else {
            None
        };
        let result = self
            .request_until("begin-epoch", deadline, |ack| SinkOperation::BeginEpoch {
                epoch,
                ack,
            })
            .await;
        if result.is_ok() {
            if let Some(guard) = gate_guard.as_mut() {
                let admission = guard.admission;
                let transitioned = guard.gate.send_if_modified(|state| {
                    if *state == SinkEpochGateState::Opening(admission) {
                        *state = SinkEpochGateState::Begun(admission);
                        true
                    } else {
                        false
                    }
                });
                if !transitioned {
                    return Err(self.epoch_gate_error(
                        format!("opening epoch admission {admission:?}"),
                        *guard.gate.borrow(),
                    ));
                }
                guard.disarm();
            }
        }
        result
    }

    /// Flush the sink's buffer (no transaction). Drives an at-least-once sink's durable landing
    /// at checkpoint so the manifest never seals offsets past still-buffered rows (CP-5).
    #[cfg(test)]
    pub async fn flush(&self) -> Result<(), ConnectorError> {
        if self.epoch_poisoned.load(Ordering::Acquire) {
            return Err(poisoned_epoch_error(&self.name));
        }
        self.request("flush", |ack| SinkOperation::Flush { ack })
            .await
    }

    /// Flush buffered rows without allowing the command to outlive `deadline`.
    pub async fn flush_until(&self, deadline: Instant) -> Result<(), ConnectorError> {
        if self.epoch_poisoned.load(Ordering::Acquire) {
            return Err(poisoned_epoch_error(&self.name));
        }
        self.request_until("flush", deadline, |ack| SinkOperation::Flush { ack })
            .await
    }

    /// Prepare an exact sink epoch without allowing the command to outlive `deadline`.
    pub async fn pre_commit_until(
        &self,
        epoch: u64,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, ConnectorError> {
        // Production Begin/PreCommit ownership is serialized by CheckpointCoordinator. Sealing
        // here closes concurrent writes; no independent Begin may cross the seal/request gap.
        self.seal_epoch_for_protocol_until(epoch, deadline).await?;
        self.request_until("pre-commit", deadline, |ack| SinkOperation::PreCommit {
            epoch,
            ack,
        })
        .await
    }

    /// Designated-committer commit of an exact validated batch.
    pub async fn commit_aggregated(
        &self,
        batch: CoordinatedCommitBatch,
    ) -> Result<(), ConnectorError> {
        self.request("commit-aggregated", |ack| SinkOperation::CommitAggregated {
            batch,
            ack,
        })
        .await
    }

    /// Highest exact checkpoint and authority committed in the external namespace.
    pub async fn committed_cursor(
        &self,
        namespace: CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
        self.request("committed-cursor", |ack| SinkOperation::CommittedCursor {
            namespace,
            ack,
        })
        .await
    }

    /// Roll back unconditionally (restart/recovery path).
    #[cfg(test)]
    pub async fn rollback_epoch(&self, epoch: u64) -> Result<(), ConnectorError> {
        self.request("rollback", |ack| SinkOperation::RollbackEpoch {
            epoch,
            ack,
        })
        .await
    }

    /// Roll back an epoch without allowing the command to outlive `deadline`.
    pub async fn rollback_epoch_until(
        &self,
        epoch: u64,
        deadline: Instant,
    ) -> Result<(), ConnectorError> {
        self.request_until("rollback", deadline, |ack| SinkOperation::RollbackEpoch {
            epoch,
            ack,
        })
        .await
    }

    /// Gracefully close the sink: aborts any open transaction (so an exactly-once producer does
    /// not fence the next incarnation), acknowledges connector flush/close, and joins the task.
    pub async fn close(&self) -> Result<(), ConnectorError> {
        let deadline = tokio::time::Instant::now() + SINK_CLOSE_TIMEOUT;
        let admission = tokio::time::timeout_at(deadline, self.admission.lock())
            .await
            .map_err(|_| close_deadline_error(&self.name, "admission"))?;
        // Publish terminal ownership synchronously after admission. Cancellation can only happen
        // at an await, so once `closing` flips the DB-owned driver is guaranteed to be spawned.
        if !self.closing.swap(true, Ordering::AcqRel) {
            self.close_state.set_phase("enqueue");
            if let Some(handle) = self.task.lock().take() {
                spawn_sink_close_driver(
                    Arc::clone(&self.name),
                    self.tx.clone(),
                    handle,
                    Arc::clone(&self.close_state),
                    Arc::clone(&self.actor_state),
                    deadline,
                    &self.runtime,
                );
            } else {
                self.close_state.finish(SinkCloseOutcome::Success);
            }
        }
        drop(admission);

        wait_for_sink_close(&self.name, Arc::clone(&self.close_state), deadline).await
    }

    pub fn checkpoint_committable(&self) -> bool {
        self.contract.is_checkpoint_committable()
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// True until the actor and all connector-owned child tasks are terminal.
    pub(crate) fn has_unresolved_task(&self) -> bool {
        !self.terminal_state.is_finished()
    }

    /// Wait for the actor and its exact connector-task generation under a caller-owned deadline.
    pub(crate) async fn wait_terminal_until(&self, deadline: Instant) -> bool {
        self.terminal_state.wait_until(deadline).await
    }

    #[cfg(test)]
    pub(crate) fn close_outcome_published(&self) -> bool {
        self.close_state.outcome().is_some()
    }

    pub(crate) fn same_actor(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.close_state, &other.close_state)
    }
}

fn spawn_sink_close_driver(
    name: Arc<str>,
    tx: SinkCommandTx,
    task: OwnedSinkTask,
    state: Arc<SinkCloseState>,
    actor_state: Arc<SinkActorState>,
    deadline: Instant,
    runtime: &tokio::runtime::Handle,
) {
    let close = drive_sink_close(
        Arc::clone(&name),
        tx,
        task,
        Arc::clone(&state),
        actor_state,
        deadline,
    );
    spawn_sink_close_driver_future(name, state, close, runtime);
}

fn spawn_sink_close_driver_future<F>(
    name: Arc<str>,
    state: Arc<SinkCloseState>,
    close: F,
    runtime: &tokio::runtime::Handle,
) where
    F: std::future::Future<Output = SinkCloseOutcome> + Send + 'static,
{
    let supervisor = runtime.spawn(async move {
        if let Ok(outcome) = std::panic::AssertUnwindSafe(close).catch_unwind().await {
            state.finish(outcome);
        } else {
            state.set_phase("terminal driver panic");
            state.finish(SinkCloseOutcome::Failure(Arc::from(format!(
                "sink task '{name}' terminal close driver panicked"
            ))));
            tracing::error!(sink = %name, "sink terminal close driver panicked");
        }
    });
    drop(supervisor); // detached by design; shared state and the DB registry retain ownership
}

async fn wait_for_sink_close(
    name: &str,
    state: Arc<SinkCloseState>,
    deadline: Instant,
) -> Result<(), ConnectorError> {
    loop {
        let notified = state.notify.notified();
        tokio::pin!(notified);
        // Register before inspecting the outcome so `notify_waiters` cannot land in the gap
        // between the check and the first poll of `Notified`.
        notified.as_mut().enable();
        if let Some(outcome) = state.outcome() {
            return outcome.into_result();
        }
        if tokio::time::timeout_at(deadline, notified.as_mut())
            .await
            .is_err()
        {
            // Deadline and completion can become ready in the same scheduler turn.
            return state.outcome().map_or_else(
                || Err(close_deadline_error(name, state.phase())),
                SinkCloseOutcome::into_result,
            );
        }
    }
}

async fn drive_sink_close(
    name: Arc<str>,
    tx: SinkCommandTx,
    mut task: OwnedSinkTask,
    state: Arc<SinkCloseState>,
    actor_state: Arc<SinkActorState>,
    deadline: Instant,
) -> SinkCloseOutcome {
    if !actor_state.accepting.load(Ordering::Acquire) {
        task.abort_actor();
        let outcome = SinkCloseOutcome::Failure(Arc::from(format!(
            "sink task '{name}' retired before close"
        )));
        state.publish_outcome(outcome.clone());
        let _ = wait_for_sink_terminal(&name, &mut task).await;
        return outcome;
    }
    let (ack_tx, mut ack_rx) = oneshot::oneshot();
    let command = SinkCommand {
        deadline,
        operation: SinkOperation::Close { ack: ack_tx },
    };

    state.set_phase("enqueue");
    match tx
        .send_with_timer(command, tokio::time::sleep_until(deadline))
        .await
    {
        Ok(()) => {}
        Err(SendTimeoutError::Disconnected(_)) => {
            return finish_disconnected_sink_close(&name, task, &state).await;
        }
        Err(SendTimeoutError::Timeout(_)) => {
            task.abort_actor();
            let outcome = SinkCloseOutcome::Failure(Arc::from(
                close_deadline_error(&name, "enqueue").to_string(),
            ));
            state.publish_outcome(outcome.clone());
            let _ = wait_for_sink_terminal(&name, &mut task).await;
            return outcome;
        }
    }

    state.set_phase("acknowledgement");
    let connector_result = match tokio::time::timeout_at(deadline, &mut ack_rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(ConnectorError::ConnectionFailed(format!(
            "sink task '{name}' dropped close acknowledgment"
        ))),
        Err(_) => {
            task.abort_actor();
            let outcome = SinkCloseOutcome::Failure(Arc::from(
                close_deadline_error(&name, "acknowledgement").to_string(),
            ));
            state.publish_outcome(outcome.clone());
            let _ = wait_for_sink_terminal(&name, &mut task).await;
            return outcome;
        }
    };

    state.set_phase("join");
    let Ok(join_result) =
        tokio::time::timeout_at(deadline, wait_for_sink_terminal(&name, &mut task)).await
    else {
        task.abort_actor();
        let outcome =
            SinkCloseOutcome::Failure(Arc::from(close_deadline_error(&name, "join").to_string()));
        state.publish_outcome(outcome.clone());
        let _ = wait_for_sink_terminal(&name, &mut task).await;
        return outcome;
    };

    match (connector_result, join_result) {
        (Ok(()), Ok(())) => SinkCloseOutcome::Success,
        (Err(error), Ok(())) | (Ok(()), Err(error)) => {
            SinkCloseOutcome::Failure(Arc::from(error.to_string()))
        }
        (Err(connector), Err(join)) => SinkCloseOutcome::Failure(Arc::from(format!(
            "sink '{name}' connector close failed: {connector}; task join also failed: {join}"
        ))),
    }
}

async fn wait_for_sink_terminal(
    name: &str,
    task: &mut OwnedSinkTask,
) -> Result<(), ConnectorError> {
    match (&mut task.terminal_join).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ConnectorError::Internal(format!(
            "sink task '{name}' failed while joining after close: {error}"
        ))),
        Err(error) => Err(ConnectorError::Internal(format!(
            "sink task '{name}' terminal supervisor failed: {error}"
        ))),
    }
}

async fn finish_disconnected_sink_close(
    name: &str,
    mut task: OwnedSinkTask,
    state: &SinkCloseState,
) -> SinkCloseOutcome {
    task.abort_actor();
    let outcome = SinkCloseOutcome::Failure(Arc::from(format!(
        "sink task '{name}' rejected close command: channel closed"
    )));
    state.publish_outcome(outcome.clone());
    let _ = wait_for_sink_terminal(name, &mut task).await;
    outcome
}

struct SinkTaskInner {
    name: String,
    sink_id: Arc<str>,
    sink: Box<dyn SinkConnector>,
    rx: SinkCommandRx,
    flush_interval: Duration,
    write_timeout: Duration,
    /// Checkpoint-committable writers may only flush inside checkpoint protocol commands.
    contract: SinkContract,
    requires_recovery_on_error: bool,
    event_tx: Producer<SinkEvent>,
    #[cfg(feature = "cluster")]
    process_authority: Option<Arc<ClusterController>>,
    #[cfg(feature = "cluster")]
    admission: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkActorEpochState {
    Open(u64),
    Prepared(u64),
}

impl SinkActorEpochState {
    fn epoch(self) -> u64 {
        match self {
            Self::Open(epoch) | Self::Prepared(epoch) => epoch,
        }
    }
}

// In replay-required modes, `epoch_poisoned` rejects checkpoint Flush/PreCommit so no durable cut
// can pass a dropped write. Local best-effort mode reports loss without permanently fencing state.
async fn run_sink_task(
    inner: SinkTaskInner,
    epoch_poisoned: Arc<AtomicBool>,
    actor_state: Arc<SinkActorState>,
) {
    #[cfg(feature = "cluster")]
    if let Some(controller) = inner.process_authority.clone() {
        run_process_fenced_sink_task(inner, epoch_poisoned, controller, actor_state.as_ref()).await;
        return;
    }

    run_local_sink_task(inner, epoch_poisoned, actor_state.as_ref()).await;
}

async fn run_local_sink_task(
    mut inner: SinkTaskInner,
    epoch_poisoned: Arc<AtomicBool>,
    actor_state: &SinkActorState,
) {
    let mut flush_timer = tokio::time::interval(inner.flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    flush_timer.tick().await; // skip the first immediate tick

    let mut epoch_state = SinkActorEpochState::Open(0);
    loop {
        tokio::select! {
            cmd = inner.rx.recv() => {
                let Ok(cmd) = cmd else {
                    close_disconnected_sink(&mut inner).await;
                    break;
                };
                let stop = handle_sink_command(
                    &mut inner,
                    cmd.operation,
                    cmd.deadline,
                    &mut epoch_state,
                    epoch_poisoned.as_ref(),
                    actor_state,
                )
                .await;
                if stop {
                    break;
                }
            }
            _ = flush_timer.tick() => {
                if flush_sink_periodically(
                    &mut inner,
                    epoch_state.epoch(),
                    epoch_poisoned.as_ref(),
                ).await {
                    actor_state.stop_admission();
                    break;
                }
            }
        }
    }
}

#[cfg(feature = "cluster")]
async fn run_process_fenced_sink_task(
    mut inner: SinkTaskInner,
    epoch_poisoned: Arc<AtomicBool>,
    controller: Arc<ClusterController>,
    actor_state: &SinkActorState,
) {
    let mut flush_timer = tokio::time::interval(inner.flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    flush_timer.tick().await;

    let mut epoch_state = SinkActorEpochState::Open(0);
    loop {
        if !controller.process_lease_is_live() {
            terminate_after_process_authority_loss(
                &mut inner,
                epoch_state.epoch(),
                epoch_poisoned.as_ref(),
                None,
            )
            .await;
            return;
        }

        tokio::select! {
            biased;
            () = controller.wait_for_process_lease_loss() => {
                terminate_after_process_authority_loss(
                    &mut inner,
                    epoch_state.epoch(),
                    epoch_poisoned.as_ref(),
                    None,
                ).await;
                return;
            }
            command = inner.rx.recv() => {
                let Ok(command) = command else {
                    close_disconnected_sink(&mut inner).await;
                    return;
                };
                if !controller.process_lease_is_live() {
                    terminate_after_process_authority_loss(
                        &mut inner,
                        epoch_state.epoch(),
                        epoch_poisoned.as_ref(),
                        Some(command.operation),
                    ).await;
                    return;
                }
                let stop = handle_sink_command(
                    &mut inner,
                    command.operation,
                    command.deadline,
                    &mut epoch_state,
                    epoch_poisoned.as_ref(),
                    actor_state,
                ).await;
                if !controller.process_lease_is_live() {
                    terminate_after_process_authority_loss(
                        &mut inner,
                        epoch_state.epoch(),
                        epoch_poisoned.as_ref(),
                        None,
                    ).await;
                    return;
                }
                if stop {
                    return;
                }
            }
            _ = flush_timer.tick() => {
                let retire = flush_sink_periodically(
                    &mut inner,
                    epoch_state.epoch(),
                    epoch_poisoned.as_ref(),
                ).await;
                if retire {
                    actor_state.stop_admission();
                    return;
                }
                if !controller.process_lease_is_live() {
                    terminate_after_process_authority_loss(
                        &mut inner,
                        epoch_state.epoch(),
                        epoch_poisoned.as_ref(),
                        None,
                    ).await;
                    return;
                }
            }
        }
    }
}

async fn flush_sink_periodically(
    inner: &mut SinkTaskInner,
    current_epoch: u64,
    epoch_poisoned: &AtomicBool,
) -> bool {
    if inner.contract.is_checkpoint_committable() {
        return false;
    }
    let (result, retire) = bounded_connector_operation(
        &inner.name,
        "periodic flush",
        operation_deadline(inner.write_timeout),
        inner.sink.cancellation_policy(),
        #[cfg(feature = "cluster")]
        inner.process_authority.clone(),
        || inner.sink.flush(),
    )
    .await;
    if let Err(error) = result {
        record_flush_error(
            inner,
            current_epoch,
            "periodic flush",
            &error,
            epoch_poisoned,
        );
    }
    retire
}

async fn close_disconnected_sink(inner: &mut SinkTaskInner) {
    tracing::debug!(sink = %inner.name, "Sink command channel closed");
    if !inner.contract.is_checkpoint_committable() {
        let (result, retire) = bounded_connector_operation(
            &inner.name,
            "flush on channel close",
            operation_deadline(inner.write_timeout),
            inner.sink.cancellation_policy(),
            #[cfg(feature = "cluster")]
            inner.process_authority.clone(),
            || inner.sink.flush(),
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(sink = %inner.name, %error, "Sink flush failed on channel close");
        }
        if retire {
            return;
        }
    }
    #[cfg(feature = "cluster")]
    if inner
        .process_authority
        .as_ref()
        .is_some_and(|controller| !controller.process_lease_is_live())
    {
        return;
    }
    let (result, _) = bounded_connector_operation(
        &inner.name,
        "connector close",
        operation_deadline(SINK_CLOSE_TIMEOUT),
        inner.sink.cancellation_policy(),
        #[cfg(feature = "cluster")]
        inner.process_authority.clone(),
        || inner.sink.close(),
    )
    .await;
    if let Err(error) = result {
        tracing::warn!(sink = %inner.name, %error, "Sink close failed on channel close");
    }
}

#[cfg(feature = "cluster")]
async fn terminate_after_process_authority_loss(
    inner: &mut SinkTaskInner,
    current_epoch: u64,
    epoch_poisoned: &AtomicBool,
    first: Option<SinkOperation>,
) {
    if inner.requires_recovery_on_error {
        epoch_poisoned.store(true, Ordering::Release);
    }
    if let Some(operation) = first {
        reject_unstarted_sink_operation(inner, operation, current_epoch, epoch_poisoned);
    }
    while let Ok(command) = inner.rx.try_recv() {
        reject_unstarted_sink_operation(inner, command.operation, current_epoch, epoch_poisoned);
    }

    let admission = Arc::clone(&inner.admission);
    let _admission = admission.lock().await;
    while let Ok(command) = inner.rx.try_recv() {
        reject_unstarted_sink_operation(inner, command.operation, current_epoch, epoch_poisoned);
    }
    tracing::warn!(sink = %inner.name, "sink actor stopped after cluster process lease loss");
}

#[cfg(feature = "cluster")]
fn reject_unstarted_sink_operation(
    inner: &SinkTaskInner,
    operation: SinkOperation,
    current_epoch: u64,
    epoch_poisoned: &AtomicBool,
) {
    match operation {
        SinkOperation::WriteBatch { batch, .. } => {
            let error = process_authority_error(&inner.name, "queued write");
            record_write_error(
                &inner.name,
                &inner.sink_id,
                inner.requires_recovery_on_error,
                &inner.event_tx,
                current_epoch,
                batch.num_rows(),
                &error,
                epoch_poisoned,
            );
        }
        SinkOperation::BeginEpoch { ack, .. } => {
            ack.send(Err(process_authority_error(&inner.name, "begin-epoch")));
        }
        SinkOperation::Flush { ack } => {
            ack.send(Err(process_authority_error(&inner.name, "flush")));
        }
        SinkOperation::PreCommit { ack, .. } => {
            ack.send(Err(process_authority_error(&inner.name, "pre-commit")));
        }
        SinkOperation::CommitAggregated { ack, .. } => {
            ack.send(Err(process_authority_error(
                &inner.name,
                "coordinated external commit",
            )));
        }
        SinkOperation::CommittedCursor { ack, .. } => {
            ack.send(Err(process_authority_error(
                &inner.name,
                "external commit cursor read",
            )));
        }
        SinkOperation::RollbackEpoch { ack, .. } => {
            ack.send(Err(process_authority_error(&inner.name, "rollback")));
        }
        SinkOperation::Sync { ack } => {
            ack.send(Err(process_authority_error(&inner.name, "sync")));
        }
        SinkOperation::Close { ack } => {
            ack.send(Err(process_authority_error(&inner.name, "close")));
        }
    }
}

/// Returns `true` when the task should stop.
async fn handle_sink_command(
    inner: &mut SinkTaskInner,
    operation: SinkOperation,
    deadline: Instant,
    epoch_state: &mut SinkActorEpochState,
    epoch_poisoned: &AtomicBool,
    actor_state: &SinkActorState,
) -> bool {
    let mut retire = false;
    match operation {
        SinkOperation::WriteBatch { epoch, batch } => {
            let current_epoch = epoch_state.epoch();
            let gate_error = if inner.contract.is_checkpoint_committable() {
                match (*epoch_state, epoch) {
                    (SinkActorEpochState::Open(current), Some(admitted))
                        if current == admitted.epoch =>
                    {
                        None
                    }
                    (state, _) => Some(ConnectorError::InvalidState {
                        expected: format!("open sink epoch {current_epoch}"),
                        actual: format!(
                            "sink '{}' actor is {state:?} for write admitted as {epoch:?}",
                            inner.name
                        ),
                    }),
                }
            } else {
                None
            };
            if let Some(error) = gate_error {
                record_write_error(
                    &inner.name,
                    &inner.sink_id,
                    inner.requires_recovery_on_error,
                    &inner.event_tx,
                    current_epoch,
                    batch.num_rows(),
                    &error,
                    epoch_poisoned,
                );
            } else {
                retire =
                    handle_write_batch(inner, batch, deadline, current_epoch, epoch_poisoned).await;
            }
        }
        SinkOperation::BeginEpoch { epoch, ack } => {
            let (result, operation_retired) =
                begin_sink_epoch(inner, epoch, deadline, epoch_state, epoch_poisoned).await;
            if operation_retired {
                actor_state.stop_admission();
            }
            ack.send(result);
            retire = operation_retired;
        }
        SinkOperation::Flush { ack } => {
            let (result, operation_retired) =
                flush_checkpoint_sink(inner, deadline, epoch_state.epoch(), epoch_poisoned).await;
            if operation_retired {
                actor_state.stop_admission();
            }
            ack.send(result);
            retire = operation_retired;
        }
        SinkOperation::PreCommit { epoch, ack } => {
            let (result, operation_retired) = if inner.contract.is_checkpoint_committable() {
                match *epoch_state {
                    SinkActorEpochState::Open(current) if current == epoch => {
                        // Once phase one starts, no queued/private write may cross it. A failed
                        // pre-commit remains Prepared until rollback begins a successor epoch.
                        *epoch_state = SinkActorEpochState::Prepared(epoch);
                        pre_commit_sink(inner, epoch, deadline, epoch_poisoned).await
                    }
                    state => (
                        Err(ConnectorError::InvalidState {
                            expected: format!("open sink epoch {epoch}"),
                            actual: format!("sink '{}' actor is {state:?}", inner.name),
                        }),
                        false,
                    ),
                }
            } else {
                pre_commit_sink(inner, epoch, deadline, epoch_poisoned).await
            };
            if operation_retired {
                actor_state.stop_admission();
            }
            ack.send(result);
            retire = operation_retired;
        }
        SinkOperation::CommitAggregated { batch, ack } => {
            let cancellation_policy = inner.sink.cancellation_policy();
            let committer = inner.sink.as_coordinated_committer();
            let (result, operation_retired) = commit_aggregated_sink(
                &inner.name,
                committer,
                batch,
                deadline,
                cancellation_policy,
                #[cfg(feature = "cluster")]
                inner.process_authority.clone(),
            )
            .await;
            if operation_retired {
                actor_state.stop_admission();
            }
            ack.send(result);
            retire = operation_retired;
        }
        SinkOperation::CommittedCursor { namespace, ack } => {
            let cancellation_policy = inner.sink.cancellation_policy();
            let committer = inner.sink.as_coordinated_committer();
            let (result, operation_retired) = committed_cursor(
                &inner.name,
                committer,
                &namespace,
                deadline,
                cancellation_policy,
                #[cfg(feature = "cluster")]
                inner.process_authority.clone(),
            )
            .await;
            if operation_retired {
                actor_state.stop_admission();
            }
            ack.send(result);
            retire = operation_retired;
        }
        SinkOperation::RollbackEpoch { epoch, ack } => {
            let (result, operation_retired) = handle_rollback_epoch(inner, epoch, deadline).await;
            if operation_retired {
                actor_state.stop_admission();
            }
            ack.send(result);
            retire = operation_retired;
        }
        SinkOperation::Sync { ack } => {
            ack.send(validate_sync_deadline(&inner.name, deadline));
        }
        SinkOperation::Close { ack } => {
            actor_state.stop_admission();
            let result = close_sink_connector(inner, deadline).await;
            ack.send(result);
            tracing::debug!(sink = %inner.name, "Sink task closed");
            return true;
        }
    }
    if retire {
        actor_state.stop_admission();
    }
    retire
}

async fn begin_sink_epoch(
    inner: &mut SinkTaskInner,
    epoch: u64,
    deadline: Instant,
    epoch_state: &mut SinkActorEpochState,
    epoch_poisoned: &AtomicBool,
) -> (Result<(), ConnectorError>, bool) {
    let (result, retire) = bounded_connector_operation(
        &inner.name,
        "begin_epoch",
        deadline,
        inner.sink.cancellation_policy(),
        #[cfg(feature = "cluster")]
        inner.process_authority.clone(),
        || inner.sink.begin_epoch(epoch),
    )
    .await;
    if result.is_ok() {
        *epoch_state = SinkActorEpochState::Open(epoch);
        epoch_poisoned.store(false, Ordering::Release);
    }
    (result, retire)
}

async fn flush_checkpoint_sink(
    inner: &mut SinkTaskInner,
    deadline: Instant,
    current_epoch: u64,
    epoch_poisoned: &AtomicBool,
) -> (Result<(), ConnectorError>, bool) {
    // A write rejected before enqueue never reaches this actor. The shared poison bit is therefore
    // the durable-cut fence for at-least-once sinks and a race-safe actor-side recheck.
    let already_poisoned = epoch_poisoned.load(Ordering::Acquire);
    let (result, retire) = if already_poisoned {
        (Err(poisoned_epoch_error(&inner.name)), false)
    } else {
        bounded_connector_operation(
            &inner.name,
            "checkpoint flush",
            deadline,
            inner.sink.cancellation_policy(),
            #[cfg(feature = "cluster")]
            inner.process_authority.clone(),
            || inner.sink.flush(),
        )
        .await
    };
    if let (false, Err(error)) = (already_poisoned, &result) {
        record_flush_error(
            inner,
            current_epoch,
            "checkpoint flush",
            error,
            epoch_poisoned,
        );
    }
    (result, retire)
}

async fn pre_commit_sink(
    inner: &mut SinkTaskInner,
    epoch: u64,
    deadline: Instant,
    epoch_poisoned: &AtomicBool,
) -> (Result<Option<Vec<u8>>, ConnectorError>, bool) {
    if epoch_poisoned.load(Ordering::Acquire) {
        (Err(poisoned_epoch_error(&inner.name)), false)
    } else {
        bounded_connector_operation(
            &inner.name,
            "pre_commit",
            deadline,
            inner.sink.cancellation_policy(),
            #[cfg(feature = "cluster")]
            inner.process_authority.clone(),
            || inner.sink.pre_commit(epoch),
        )
        .await
    }
}

async fn commit_aggregated_sink(
    sink_name: &str,
    committer: Option<&dyn CoordinatedCommitter>,
    batch: CoordinatedCommitBatch,
    deadline: Instant,
    cancellation_policy: ConnectorCancellationPolicy,
    #[cfg(feature = "cluster")] process_authority: Option<Arc<ClusterController>>,
) -> (Result<(), ConnectorError>, bool) {
    match committer {
        Some(committer) => {
            let context = CoordinatedCommitContext::new(deadline);
            bounded_connector_operation(
                sink_name,
                "coordinated external commit",
                deadline,
                cancellation_policy,
                #[cfg(feature = "cluster")]
                process_authority,
                || committer.commit_aggregated(batch, context),
            )
            .await
        }
        None => (
            Err(ConnectorError::InvalidState {
                expected: "coordinated committer".into(),
                actual: format!("sink '{sink_name}' is not coordinated"),
            }),
            false,
        ),
    }
}

async fn committed_cursor(
    sink_name: &str,
    committer: Option<&dyn CoordinatedCommitter>,
    namespace: &CoordinatedCommitNamespace,
    deadline: Instant,
    cancellation_policy: ConnectorCancellationPolicy,
    #[cfg(feature = "cluster")] process_authority: Option<Arc<ClusterController>>,
) -> (
    Result<Option<CoordinatedCommitCursor>, ConnectorError>,
    bool,
) {
    match committer {
        Some(committer) => {
            bounded_connector_operation(
                sink_name,
                "external commit cursor read",
                deadline,
                cancellation_policy,
                #[cfg(feature = "cluster")]
                process_authority,
                || committer.committed_cursor(namespace),
            )
            .await
        }
        None => (Ok(None), false),
    }
}

fn validate_sync_deadline(sink_name: &str, deadline: Instant) -> Result<(), ConnectorError> {
    if deadline <= Instant::now() {
        Err(protocol_deadline_error(sink_name, "sync"))
    } else {
        Ok(())
    }
}

async fn close_sink_connector(
    inner: &mut SinkTaskInner,
    deadline: Instant,
) -> Result<(), ConnectorError> {
    // Checkpoint-committable sinks finalize only through checkpoint protocol; close aborts their
    // open transaction. Weaker sinks must first land every queued write. While process authority
    // remains live, always call close even when flush fails so resources are not leaked.
    let cancellation_policy = inner.sink.cancellation_policy();
    let (flush_result, flush_retired) = if inner.contract.is_checkpoint_committable() {
        (Ok(()), false)
    } else {
        bounded_connector_operation(
            &inner.name,
            "shutdown flush",
            deadline,
            cancellation_policy,
            #[cfg(feature = "cluster")]
            inner.process_authority.clone(),
            || inner.sink.flush(),
        )
        .await
    };
    #[cfg(feature = "cluster")]
    if inner
        .process_authority
        .as_ref()
        .is_some_and(|controller| !controller.process_lease_is_live())
    {
        return match flush_result {
            Ok(()) => Err(process_authority_error(&inner.name, "connector close")),
            Err(error) => Err(error),
        };
    }
    if flush_retired {
        return flush_result;
    }
    let close_result = if Instant::now() >= deadline {
        Err(protocol_deadline_error(&inner.name, "connector close"))
    } else {
        bounded_connector_operation(
            &inner.name,
            "connector close",
            deadline,
            cancellation_policy,
            #[cfg(feature = "cluster")]
            inner.process_authority.clone(),
            || inner.sink.close(),
        )
        .await
        .0
    };
    let result = match (flush_result, close_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(flush_error), Err(close_error)) => {
            tracing::warn!(
                sink = %inner.name,
                error = %close_error,
                "sink close also failed after shutdown flush failed"
            );
            Err(ConnectorError::Internal(format!(
                "sink shutdown flush failed: {flush_error}; connector close also failed: \
                 {close_error}"
            )))
        }
    };
    if let Err(ref error) = result {
        tracing::warn!(sink = %inner.name, %error, "sink shutdown failed");
    }
    result
}

fn poisoned_epoch_error(sink_name: &str) -> ConnectorError {
    ConnectorError::WriteError(format!(
        "sink '{sink_name}' epoch poisoned by a prior dropped write"
    ))
}

fn record_flush_error(
    inner: &SinkTaskInner,
    current_epoch: u64,
    operation: &'static str,
    error: &ConnectorError,
    epoch_poisoned: &AtomicBool,
) {
    if inner.requires_recovery_on_error {
        epoch_poisoned.store(true, Ordering::Release);
    }
    tracing::warn!(
        sink = %inner.name,
        epoch = current_epoch,
        requires_recovery = inner.requires_recovery_on_error,
        %error,
        "Sink durability flush failed"
    );
    let _ = inner.event_tx.try_push(SinkEvent::FlushError {
        sink_id: Arc::clone(&inner.sink_id),
        epoch: current_epoch,
        operation,
        error: error.to_string(),
    });
}

fn record_write_error(
    sink_name: &str,
    sink_id: &Arc<str>,
    requires_recovery_on_error: bool,
    event_tx: &Producer<SinkEvent>,
    current_epoch: u64,
    rows: usize,
    error: &ConnectorError,
    epoch_poisoned: &AtomicBool,
) {
    if requires_recovery_on_error {
        epoch_poisoned.store(true, Ordering::Release);
    }
    tracing::warn!(
        sink = %sink_name,
        %error,
        rows,
        requires_recovery = requires_recovery_on_error,
        "Sink write error"
    );
    let _ = event_tx.try_push(SinkEvent::WriteError {
        sink_id: Arc::clone(sink_id),
        epoch: current_epoch,
        rows,
        error: error.to_string(),
    });
}

/// Write a batch before the enqueue-time deadline; reports every error and poisons replay-required
/// modes so their durable cut cannot advance.
async fn handle_write_batch(
    inner: &mut SinkTaskInner,
    batch: RecordBatch,
    deadline: Instant,
    current_epoch: u64,
    epoch_poisoned: &AtomicBool,
) -> bool {
    let rows = batch.num_rows();
    let cancellation_policy = inner.sink.cancellation_policy();
    let outcome = await_connector_operation(
        deadline,
        #[cfg(feature = "cluster")]
        inner.process_authority.clone(),
        || inner.sink.write_batch(&batch),
    )
    .await;

    match outcome {
        ConnectorOperationOutcome::Completed(Ok(_)) => false,
        ConnectorOperationOutcome::Completed(Err(error)) => {
            let retire = error.is_outcome_unknown();
            record_write_error(
                &inner.name,
                &inner.sink_id,
                inner.requires_recovery_on_error,
                &inner.event_tx,
                current_epoch,
                rows,
                &error,
                epoch_poisoned,
            );
            retire
        }
        ConnectorOperationOutcome::Deadline => {
            record_write_timeout(
                &inner.name,
                &inner.sink_id,
                inner.write_timeout,
                inner.requires_recovery_on_error,
                &inner.event_tx,
                current_epoch,
                rows,
                epoch_poisoned,
            );
            cancellation_policy == ConnectorCancellationPolicy::RetireConnector
        }
        #[cfg(feature = "cluster")]
        ConnectorOperationOutcome::ProcessAuthorityLost => {
            let error = process_authority_error(&inner.name, "write");
            record_write_error(
                &inner.name,
                &inner.sink_id,
                inner.requires_recovery_on_error,
                &inner.event_tx,
                current_epoch,
                rows,
                &error,
                epoch_poisoned,
            );
            true
        }
    }
}

fn record_write_timeout(
    sink_name: &str,
    sink_id: &Arc<str>,
    write_timeout: Duration,
    requires_recovery_on_error: bool,
    event_tx: &Producer<SinkEvent>,
    current_epoch: u64,
    rows: usize,
    epoch_poisoned: &AtomicBool,
) {
    if requires_recovery_on_error {
        epoch_poisoned.store(true, Ordering::Release);
    }
    tracing::error!(
        sink = %sink_name,
        timeout_secs = write_timeout.as_secs(),
        rows,
        requires_recovery = requires_recovery_on_error,
        "Sink write end-to-end deadline exceeded"
    );
    let _ = event_tx.try_push(SinkEvent::WriteTimeout {
        sink_id: Arc::clone(sink_id),
        epoch: current_epoch,
        rows,
        timeout: write_timeout,
    });
}

/// Roll back an undecided coordinated epoch's local pending output.
async fn handle_rollback_epoch(
    inner: &mut SinkTaskInner,
    epoch: u64,
    deadline: Instant,
) -> (Result<(), ConnectorError>, bool) {
    let (result, retire) = bounded_connector_operation(
        &inner.name,
        "rollback_epoch",
        deadline,
        inner.sink.cancellation_policy(),
        #[cfg(feature = "cluster")]
        inner.process_authority.clone(),
        || inner.sink.rollback_epoch(epoch),
    )
    .await;
    if let Err(ref e) = result {
        tracing::warn!(
            sink = %inner.name, epoch, error = %e,
            "[LDB-6004] Sink rollback failed"
        );
    }
    (result, retire)
}

#[cfg(test)]
mod tests;
