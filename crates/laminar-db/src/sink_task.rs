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
        self.write_batch_before(
            batch,
            operation_deadline(self.write_timeout),
            self.write_timeout,
        )
        .await
    }

    /// Send a batch with queue admission and the actor command clamped to the caller's deadline.
    pub async fn write_batch_until(
        &self,
        batch: RecordBatch,
        supplied_deadline: Instant,
    ) -> Result<(), ConnectorError> {
        let started = Instant::now();
        let deadline = operation_deadline(self.write_timeout).min(supplied_deadline);
        let effective_timeout = deadline.saturating_duration_since(started);
        self.write_batch_before(batch, deadline, effective_timeout)
            .await
    }

    async fn write_batch_before(
        &self,
        batch: RecordBatch,
        deadline: Instant,
        effective_timeout: Duration,
    ) -> Result<(), ConnectorError> {
        let rows = batch.num_rows();
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
        let Ok(admission) = tokio::time::timeout_at(deadline, self.admission.lock()).await else {
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
        let command = SinkCommand {
            deadline,
            operation: SinkOperation::WriteBatch { batch },
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
        self.request_until("begin-epoch", deadline, |ack| SinkOperation::BeginEpoch {
            epoch,
            ack,
        })
        .await
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

    let mut current_epoch: u64 = 0;
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
                    &mut current_epoch,
                    epoch_poisoned.as_ref(),
                    actor_state,
                )
                .await;
                if stop {
                    break;
                }
            }
            _ = flush_timer.tick() => {
                if flush_sink_periodically(&mut inner, current_epoch, epoch_poisoned.as_ref()).await {
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

    let mut current_epoch = 0;
    loop {
        if !controller.process_lease_is_live() {
            terminate_after_process_authority_loss(
                &mut inner,
                current_epoch,
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
                    current_epoch,
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
                        current_epoch,
                        epoch_poisoned.as_ref(),
                        Some(command.operation),
                    ).await;
                    return;
                }
                let stop = handle_sink_command(
                    &mut inner,
                    command.operation,
                    command.deadline,
                    &mut current_epoch,
                    epoch_poisoned.as_ref(),
                    actor_state,
                ).await;
                if !controller.process_lease_is_live() {
                    terminate_after_process_authority_loss(
                        &mut inner,
                        current_epoch,
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
                    current_epoch,
                    epoch_poisoned.as_ref(),
                ).await;
                if retire {
                    actor_state.stop_admission();
                    return;
                }
                if !controller.process_lease_is_live() {
                    terminate_after_process_authority_loss(
                        &mut inner,
                        current_epoch,
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
        SinkOperation::WriteBatch { batch } => {
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
    current_epoch: &mut u64,
    epoch_poisoned: &AtomicBool,
    actor_state: &SinkActorState,
) -> bool {
    let mut retire = false;
    match operation {
        SinkOperation::WriteBatch { batch } => {
            retire =
                handle_write_batch(inner, batch, deadline, *current_epoch, epoch_poisoned).await;
        }
        SinkOperation::BeginEpoch { epoch, ack } => {
            let (result, operation_retired) =
                begin_sink_epoch(inner, epoch, deadline, current_epoch, epoch_poisoned).await;
            if operation_retired {
                actor_state.stop_admission();
            }
            ack.send(result);
            retire = operation_retired;
        }
        SinkOperation::Flush { ack } => {
            let (result, operation_retired) =
                flush_checkpoint_sink(inner, deadline, *current_epoch, epoch_poisoned).await;
            if operation_retired {
                actor_state.stop_admission();
            }
            ack.send(result);
            retire = operation_retired;
        }
        SinkOperation::PreCommit { epoch, ack } => {
            let (result, operation_retired) =
                pre_commit_sink(inner, epoch, deadline, epoch_poisoned).await;
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
    current_epoch: &mut u64,
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
        *current_epoch = epoch;
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

#[allow(clippy::too_many_arguments)] // Copied fields avoid borrowing the actor across sink I/O.
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

#[allow(clippy::too_many_arguments)] // Copied fields avoid borrowing the actor across sink I/O.
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
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_connectors::connector::{
        ConnectorTaskOwner, SinkConsistency, SinkInputMode, SinkTopology, WriteResult,
    };
    use laminar_core::streaming::AsyncConsumer;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(feature = "cluster")]
    use crossfire::AsyncTxTrait as _;

    fn supervise_test_actor<F>(
        actor: F,
        terminal_tasks: Option<ConnectorTaskTracker>,
    ) -> (OwnedSinkTask, Arc<SinkActorState>)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let runtime = tokio::runtime::Handle::current();
        let actor_state = Arc::new(SinkActorState::new());
        let actor = spawn_sink_actor(&runtime, actor, Arc::clone(&actor_state));
        let task = supervise_sink_task(actor, terminal_tasks, Arc::clone(&actor_state), &runtime);
        (task, actor_state)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_blocking_completion_is_a_deadline_and_retires_the_generation() {
        let deadline = Instant::now() + Duration::from_millis(5);
        let (result, retire) = bounded_connector_operation(
            "late-completion",
            "flush",
            deadline,
            ConnectorCancellationPolicy::RetireConnector,
            #[cfg(feature = "cluster")]
            None,
            || async {
                std::thread::sleep(Duration::from_millis(25));
                Ok::<_, ConnectorError>(())
            },
        )
        .await;

        assert!(result.unwrap_err().to_string().contains("deadline"));
        assert!(retire);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_blocking_completion_cannot_cross_the_process_fence() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

        let node = laminar_core::state::NodeId(91);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
        let controller = ClusterController::new(node, kv, None, members_rx);
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
            .unwrap();

        let outcome = await_connector_operation_fenced(
            &controller,
            Instant::now() + Duration::from_millis(5),
            async {
                std::thread::sleep(Duration::from_millis(25));
                7_u64
            },
        )
        .await;
        assert!(matches!(outcome, ConnectorOperationOutcome::Deadline));
    }

    /// Minimal mock sink for testing the task infrastructure.
    struct CountingSink {
        writes: Arc<AtomicU64>,
        flushes: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    struct AlternatingTrackerSink {
        _first_owner: ConnectorTaskOwner,
        _second_owner: ConnectorTaskOwner,
        first_tracker: ConnectorTaskTracker,
        second_tracker: ConnectorTaskTracker,
        tracker_calls: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl SinkConnector for AlternatingTrackerSink {
        fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
            let call = self.tracker_calls.fetch_add(1, Ordering::SeqCst);
            Some(if call == 0 {
                self.first_tracker.clone()
            } else {
                self.second_tracker.clone()
            })
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(0, 0))
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(1)
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    impl CountingSink {
        fn new() -> (Self, Arc<AtomicU64>, Arc<AtomicU64>) {
            let writes = Arc::new(AtomicU64::new(0));
            let flushes = Arc::new(AtomicU64::new(0));
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            (
                Self {
                    writes: Arc::clone(&writes),
                    flushes: Arc::clone(&flushes),
                    schema,
                },
                writes,
                flushes,
            )
        }
    }

    #[async_trait::async_trait]
    impl SinkConnector for CountingSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            Ok(WriteResult {
                records_written: 1,
                bytes_written: 0,
            })
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[cfg(feature = "cluster")]
    struct InFlightWriteGuard {
        cancellations: Arc<AtomicU64>,
        completed: bool,
    }

    #[cfg(feature = "cluster")]
    impl Drop for InFlightWriteGuard {
        fn drop(&mut self) {
            if !self.completed {
                self.cancellations.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[cfg(feature = "cluster")]
    struct AuthorityBlockingSink {
        policy: ConnectorCancellationPolicy,
        writes: Arc<AtomicU64>,
        flushes: Arc<AtomicU64>,
        completions: Arc<AtomicU64>,
        cancellations: Arc<AtomicU64>,
        gate: Arc<tokio::sync::Semaphore>,
        schema: arrow::datatypes::SchemaRef,
    }

    #[cfg(feature = "cluster")]
    #[async_trait::async_trait]
    impl SinkConnector for AuthorityBlockingSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            self.policy
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            let mut guard = InFlightWriteGuard {
                cancellations: Arc::clone(&self.cancellations),
                completed: false,
            };
            let permit = self.gate.acquire().await.unwrap();
            permit.forget();
            guard.completed = true;
            self.completions.fetch_add(1, Ordering::SeqCst);
            Ok(WriteResult::new(1, 0))
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(60)
        }
    }

    #[cfg(feature = "cluster")]
    struct AuthoritySinkProbe {
        handle: SinkTaskHandle,
        events: AsyncConsumer<SinkEvent>,
        controller: Arc<ClusterController>,
        writes: Arc<AtomicU64>,
        flushes: Arc<AtomicU64>,
        completions: Arc<AtomicU64>,
        cancellations: Arc<AtomicU64>,
    }

    #[cfg(feature = "cluster")]
    fn authority_sink_probe(node: u64, policy: ConnectorCancellationPolicy) -> AuthoritySinkProbe {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

        let node_id = laminar_core::state::NodeId(node);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
            .unwrap();
        let writes = Arc::new(AtomicU64::new(0));
        let flushes = Arc::new(AtomicU64::new(0));
        let completions = Arc::new(AtomicU64::new(0));
        let cancellations = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = AuthorityBlockingSink {
            policy,
            writes: Arc::clone(&writes),
            flushes: Arc::clone(&flushes),
            completions: Arc::clone(&completions),
            cancellations: Arc::clone(&cancellations),
            gate: Arc::clone(&gate),
            schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
        };
        let (event_tx, events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "authority-probe".into(),
            sink_id: Arc::from("authority-probe"),
            connector: Box::new(sink),
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            channel_capacity: 8,
            flush_interval: Duration::from_secs(60),
            write_timeout: Duration::from_secs(60),
            event_tx,
            terminal_tasks: None,
            process_authority: Some(Arc::clone(&controller)),
        });
        AuthoritySinkProbe {
            handle,
            events,
            controller,
            writes,
            flushes,
            completions,
            cancellations,
        }
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_actor_queue(handle: &SinkTaskHandle, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while handle.tx.len() < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sink command did not reach the actor queue");
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_actor_exit(handle: &SinkTaskHandle) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !handle.actor_state.finished.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sink actor did not terminate after process lease loss");
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_connector_write(writes: &AtomicU64) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while writes.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sink connector write did not start");
    }

    #[cfg(feature = "cluster")]
    async fn receive_authority_write_error(events: &mut AsyncConsumer<SinkEvent>) {
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("sink did not report process authority loss")
            .expect("sink event channel closed unexpectedly");
        assert!(matches!(
            event,
            SinkEvent::WriteError {
                sink_id,
                epoch: 0,
                rows: 3,
                error,
            } if &*sink_id == "authority-probe" && error.contains("process authority")
        ));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn process_lease_loss_wakes_idle_sink_actor() {
        let probe = authority_sink_probe(31, ConnectorCancellationPolicy::CancelSafe);

        probe.controller.fence_process_lease();
        wait_for_actor_exit(&probe.handle).await;

        assert_eq!(probe.writes.load(Ordering::SeqCst), 0);
        assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
        assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
        assert_eq!(probe.cancellations.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn process_lease_loss_cancels_cancel_safe_write_and_rejects_queued_commands() {
        let mut probe = authority_sink_probe(32, ConnectorCancellationPolicy::CancelSafe);
        probe.handle.write_batch(test_batch()).await.unwrap();
        wait_for_connector_write(&probe.writes).await;

        probe.handle.write_batch(test_batch()).await.unwrap();
        let flush_handle = probe.handle.clone();
        let queued_flush = tokio::spawn(async move { flush_handle.flush().await });
        wait_for_actor_queue(&probe.handle, 2).await;

        probe.controller.fence_process_lease();
        wait_for_actor_exit(&probe.handle).await;

        let error = queued_flush.await.unwrap().unwrap_err().to_string();
        assert!(error.contains("process authority"), "{error}");
        receive_authority_write_error(&mut probe.events).await;
        receive_authority_write_error(&mut probe.events).await;
        assert_eq!(probe.writes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
        assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
        assert_eq!(probe.cancellations.load(Ordering::SeqCst), 1);
        assert!(probe.handle.epoch_poisoned.load(Ordering::Acquire));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn process_lease_loss_retires_started_write_and_rejects_queued_commands() {
        let mut probe = authority_sink_probe(33, ConnectorCancellationPolicy::RetireConnector);
        probe.handle.write_batch(test_batch()).await.unwrap();
        wait_for_connector_write(&probe.writes).await;

        probe.handle.write_batch(test_batch()).await.unwrap();
        let flush_handle = probe.handle.clone();
        let queued_flush = tokio::spawn(async move { flush_handle.flush().await });
        wait_for_actor_queue(&probe.handle, 2).await;

        probe.controller.fence_process_lease();
        wait_for_actor_exit(&probe.handle).await;

        let error = queued_flush.await.unwrap().unwrap_err().to_string();
        assert!(error.contains("process authority"), "{error}");
        receive_authority_write_error(&mut probe.events).await;
        receive_authority_write_error(&mut probe.events).await;
        assert_eq!(probe.writes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
        assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
        assert_eq!(probe.cancellations.load(Ordering::SeqCst), 1);
        assert!(probe.handle.epoch_poisoned.load(Ordering::Acquire));
    }

    struct ShutdownFailureSink {
        fail_flush: bool,
        fail_close: bool,
        closes: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    impl ShutdownFailureSink {
        fn new(fail_flush: bool, fail_close: bool) -> (Self, Arc<AtomicU64>) {
            let closes = Arc::new(AtomicU64::new(0));
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            (
                Self {
                    fail_flush,
                    fail_close,
                    closes: Arc::clone(&closes),
                    schema,
                },
                closes,
            )
        }
    }

    #[async_trait::async_trait]
    impl SinkConnector for ShutdownFailureSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(1, 0))
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            if self.fail_flush {
                Err(ConnectorError::WriteError("injected shutdown flush".into()))
            } else {
                Ok(())
            }
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                Err(ConnectorError::ConnectionFailed(
                    "injected connector close".into(),
                ))
            } else {
                Ok(())
            }
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    struct FailFirstFlushSink {
        flushes: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl SinkConnector for FailFirstFlushSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(1, 0))
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            let call = self.flushes.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err(ConnectorError::WriteError(
                    "injected deferred acknowledgement failure".into(),
                ))
            } else {
                Ok(())
            }
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    fn at_least_once_contract() -> SinkContract {
        SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        )
    }

    fn checkpoint_committable_contract() -> SinkContract {
        SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        )
    }

    fn spawn_fail_first_periodic_flush(
        requires_recovery_on_error: bool,
    ) -> (SinkTaskHandle, AsyncConsumer<SinkEvent>, Arc<AtomicU64>) {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let flushes = Arc::new(AtomicU64::new(0));
        let sink = FailFirstFlushSink {
            flushes: Arc::clone(&flushes),
            schema,
        };
        let (event_tx, event_rx) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "deferred-ack".into(),
            sink_id: Arc::from("deferred-ack"),
            connector: Box::new(sink),
            contract: at_least_once_contract(),
            requires_recovery_on_error,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
        (handle, event_rx, flushes)
    }

    fn spawn_with_defaults(
        name: &str,
        connector: Box<dyn SinkConnector>,
        write_timeout: Duration,
    ) -> (SinkTaskHandle, AsyncConsumer<SinkEvent>) {
        let (event_tx, event_rx) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: name.into(),
            sink_id: Arc::from(name),
            connector,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            write_timeout,
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
        (handle, event_rx)
    }

    #[tokio::test]
    async fn test_sink_task_write_and_close() {
        let (sink, writes, _flushes) = CountingSink::new();
        let (handle, _events) = spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));

        handle.write_batch(test_batch()).await.unwrap();
        handle.write_batch(test_batch()).await.unwrap();
        handle.close().await.unwrap();

        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn spawn_uses_the_exact_captured_connector_tracker() {
        let (first_owner, first_tracker) = ConnectorTaskOwner::new();
        let first_guard = first_owner.track().expect("first tracker generation");
        let (second_owner, second_tracker) = ConnectorTaskOwner::new();
        let second_guard = second_owner.track().expect("second tracker generation");
        let tracker_calls = Arc::new(AtomicU64::new(0));
        let connector: Box<dyn SinkConnector> = Box::new(AlternatingTrackerSink {
            _first_owner: first_owner,
            _second_owner: second_owner,
            first_tracker,
            second_tracker,
            tracker_calls: Arc::clone(&tracker_calls),
            schema: Arc::new(Schema::empty()),
        });
        let terminal_tasks = connector.terminal_task_tracker();
        let (event_tx, _events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);

        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "exact-tracker".into(),
            sink_id: Arc::from("exact-tracker"),
            connector,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_secs(5),
            event_tx,
            terminal_tasks,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
        assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);

        let close = tokio::spawn({
            let handle = handle.clone();
            async move { handle.close().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !handle.actor_state.finished.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sink actor did not finish close");
        assert!(!close.is_finished());

        drop(first_guard);
        tokio::time::timeout(Duration::from_secs(1), close)
            .await
            .expect("captured tracker did not release terminal supervision")
            .expect("close task panicked")
            .expect("sink close failed");
        assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);
        drop(second_guard);
    }

    #[tokio::test]
    async fn close_linearizes_before_a_waiting_write_admission() {
        let (sink, writes, _flushes) = CountingSink::new();
        let (handle, _events) =
            spawn_with_defaults("close-race", Box::new(sink), Duration::from_secs(5));

        let admission = handle.admission.lock().await;
        let close_handle = handle.clone();
        let close = tokio::spawn(async move { close_handle.close().await });
        tokio::task::yield_now().await;
        let write_handle = handle.clone();
        let write = tokio::spawn(async move { write_handle.write_batch(test_batch()).await });
        tokio::task::yield_now().await;
        drop(admission);

        close.await.unwrap().unwrap();
        assert!(write.await.unwrap().is_err());
        assert_eq!(
            writes.load(Ordering::Acquire),
            0,
            "a write queued behind Close must never be acknowledged"
        );
    }

    #[tokio::test]
    async fn repeated_close_is_idempotent() {
        let (sink, _writes, _flushes) = CountingSink::new();
        let (handle, _events) =
            spawn_with_defaults("repeated-close", Box::new(sink), Duration::from_secs(5));

        handle.close().await.unwrap();
        handle.close().await.unwrap();
    }

    struct GatedCloseSink {
        close_started: Arc<tokio::sync::Semaphore>,
        close_release: Arc<tokio::sync::Semaphore>,
        closes: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl SinkConnector for GatedCloseSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(1, 0))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            self.close_started.add_permits(1);
            self.close_release.acquire().await.unwrap().forget();
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test]
    async fn cancelling_close_after_enqueue_keeps_one_terminal_driver() {
        let close_started = Arc::new(tokio::sync::Semaphore::new(0));
        let close_release = Arc::new(tokio::sync::Semaphore::new(0));
        let closes = Arc::new(AtomicU64::new(0));
        let sink = GatedCloseSink {
            close_started: Arc::clone(&close_started),
            close_release: Arc::clone(&close_release),
            closes: Arc::clone(&closes),
            schema: Arc::new(Schema::empty()),
        };
        let (handle, _events) =
            spawn_with_defaults("cancel-close-ack", Box::new(sink), Duration::from_secs(5));

        let caller_handle = handle.clone();
        let caller = tokio::spawn(async move { caller_handle.close().await });
        close_started.acquire().await.unwrap().forget();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(handle.has_unresolved_task());

        close_release.add_permits(1);
        handle
            .close()
            .await
            .expect("a retry observes the original driver's terminal result");
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelling_close_before_admission_does_not_publish_a_partial_close() {
        let (sink, _writes, _flushes) = CountingSink::new();
        let (handle, _events) = spawn_with_defaults(
            "cancel-close-admission",
            Box::new(sink),
            Duration::from_secs(5),
        );
        let admission = handle.admission.lock().await;

        let caller_handle = handle.clone();
        let caller = tokio::spawn(async move { caller_handle.close().await });
        tokio::task::yield_now().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(!handle.closing.load(Ordering::Acquire));

        drop(admission);
        handle.close().await.unwrap();
    }

    struct GatedWriteSink {
        write_started: tokio::sync::mpsc::UnboundedSender<()>,
        write_release: Arc<tokio::sync::Semaphore>,
        closes: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl SinkConnector for GatedWriteSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            let _ = self.write_started.send(());
            self.write_release.acquire().await.unwrap().forget();
            Ok(WriteResult::new(1, 0))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test]
    async fn cancelling_close_while_enqueue_is_full_preserves_the_actor_fence() {
        let (write_started_tx, mut write_started_rx) = tokio::sync::mpsc::unbounded_channel();
        let write_release = Arc::new(tokio::sync::Semaphore::new(0));
        let closes = Arc::new(AtomicU64::new(0));
        let sink = GatedWriteSink {
            write_started: write_started_tx,
            write_release: Arc::clone(&write_release),
            closes: Arc::clone(&closes),
            schema: Arc::new(Schema::empty()),
        };
        let (event_tx, _event_rx) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "cancel-close-enqueue".into(),
            sink_id: Arc::from("cancel-close-enqueue"),
            connector: Box::new(sink),
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            channel_capacity: 1,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_secs(5),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });

        handle.write_batch(test_batch()).await.unwrap();
        write_started_rx.recv().await.unwrap();
        handle.write_batch(test_batch()).await.unwrap();

        let caller_handle = handle.clone();
        let caller = tokio::spawn(async move { caller_handle.close().await });
        while !handle.closing.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(handle.has_unresolved_task());

        write_release.add_permits(2);
        handle.close().await.unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_sink_task_flush() {
        let (sink, _writes, flushes) = CountingSink::new();
        let (handle, _events) = spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));

        handle.flush().await.unwrap();
        handle.close().await.unwrap();

        // At least 1 explicit flush + 1 from close
        assert!(flushes.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn configured_interval_bounds_low_volume_buffer_residence() {
        let (sink, _writes, flushes) = CountingSink::new();
        let (event_tx, _event_rx) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "low-volume".into(),
            sink_id: Arc::from("low-volume"),
            connector: Box::new(sink),
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_millis(250),
            write_timeout: Duration::from_secs(5),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });

        handle.write_batch(test_batch()).await.unwrap();
        handle.sync().await.unwrap();
        tokio::time::advance(Duration::from_millis(249)).await;
        tokio::task::yield_now().await;
        assert_eq!(flushes.load(Ordering::Acquire), 0);

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(flushes.load(Ordering::Acquire), 1);

        handle.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_ack_reports_flush_and_connector_failures_after_attempting_both() {
        for (fail_flush, fail_close, expected) in [
            (true, false, &["shutdown flush"][..]),
            (false, true, &["connector close"][..]),
            (true, true, &["shutdown flush", "connector close"][..]),
        ] {
            let (sink, closes) = ShutdownFailureSink::new(fail_flush, fail_close);
            let (handle, _events) =
                spawn_with_defaults("failing", Box::new(sink), Duration::from_secs(5));

            let error = handle.close().await.unwrap_err().to_string();
            for needle in expected {
                assert!(error.contains(needle), "missing '{needle}' in '{error}'");
            }
            let repeated = handle.close().await.unwrap_err().to_string();
            for needle in expected {
                assert!(
                    repeated.contains(needle),
                    "repeated close lost terminal failure '{needle}' in '{repeated}'"
                );
            }
            assert_eq!(
                closes.load(Ordering::SeqCst),
                1,
                "connector close must run exactly once and persist its terminal result"
            );
        }
    }

    struct PanicCloseSink {
        closes: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl SinkConnector for PanicCloseSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(1, 0))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            panic!("injected close panic");
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test]
    async fn close_panic_is_terminal_and_persisted_without_a_second_command() {
        let closes = Arc::new(AtomicU64::new(0));
        let sink = PanicCloseSink {
            closes: Arc::clone(&closes),
            schema: Arc::new(Schema::empty()),
        };
        let (handle, _events) =
            spawn_with_defaults("panic-close", Box::new(sink), Duration::from_secs(5));

        let first = handle.close().await.unwrap_err().to_string();
        let second = handle.close().await.unwrap_err().to_string();
        assert!(first.contains("close") || first.contains("join"), "{first}");
        assert_eq!(first, second);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exactly_once_task_never_periodically_or_implicitly_flushes() {
        let (sink, _writes, flushes) = CountingSink::new();
        let (event_tx, _event_rx) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "eo".into(),
            sink_id: Arc::from("eo"),
            connector: Box::new(sink),
            contract: checkpoint_committable_contract(),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_millis(5),
            write_timeout: Duration::from_secs(5),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        handle.close().await.unwrap();

        assert_eq!(
            flushes.load(Ordering::Relaxed),
            0,
            "exactly-once data may only flush through checkpoint protocol commands"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_flush_failure_poison_rejects_durable_checkpoint_flush() {
        let (handle, mut events, flushes) = spawn_fail_first_periodic_flush(true);

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        let event = events.recv().await.unwrap();
        assert!(matches!(
            event,
            SinkEvent::FlushError {
                sink_id,
                epoch: 0,
                operation: "periodic flush",
                error,
            } if &*sink_id == "deferred-ack"
                && error.contains("deferred acknowledgement failure")
        ));

        let error = handle.flush().await.unwrap_err().to_string();
        assert!(error.contains("poisoned"), "{error}");
        assert_eq!(
            flushes.load(Ordering::SeqCst),
            1,
            "checkpoint flush must reject the sticky failure without observing a false-empty queue"
        );
        handle.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn best_effort_periodic_flush_failure_does_not_permanently_poison_checkpoints() {
        let (handle, mut events, flushes) = spawn_fail_first_periodic_flush(false);

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(matches!(
            events.recv().await.unwrap(),
            SinkEvent::FlushError {
                operation: "periodic flush",
                ..
            }
        ));

        handle.flush().await.unwrap();
        assert_eq!(
            flushes.load(Ordering::SeqCst),
            2,
            "best-effort policy must report loss but allow a later state checkpoint to recover"
        );
        handle.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sink_task_handle_clone() {
        let (sink, writes, _flushes) = CountingSink::new();
        let (handle1, _events) =
            spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));
        let handle2 = handle1.clone();

        handle1.write_batch(test_batch()).await.unwrap();
        handle2.write_batch(test_batch()).await.unwrap();
        handle1.close().await.unwrap();

        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }

    /// Records `rollback_epoch` calls.
    struct RollbackProbeSink {
        rollbacks: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    impl RollbackProbeSink {
        fn new() -> (Self, Arc<AtomicU64>) {
            let rollbacks = Arc::new(AtomicU64::new(0));
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            (
                Self {
                    rollbacks: Arc::clone(&rollbacks),
                    schema,
                },
                rollbacks,
            )
        }
    }

    #[async_trait::async_trait]
    impl SinkConnector for RollbackProbeSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult {
                records_written: 1,
                bytes_written: 0,
            })
        }

        async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
            self.rollbacks.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    /// Without a durable external decision, rollback discards local staged output.
    #[tokio::test]
    async fn rollback_discards_staged_output() {
        let (sink, rollbacks) = RollbackProbeSink::new();
        let (handle, _ev) = spawn_with_defaults("rollback", Box::new(sink), Duration::from_secs(5));
        handle.rollback_epoch(1).await.unwrap();
        assert_eq!(
            rollbacks.load(Ordering::Relaxed),
            1,
            "rollback must discard staged output"
        );
        handle.close().await.unwrap();
    }

    /// Sink whose `write_batch` sleeps longer than the configured timeout.
    struct SlowSink {
        schema: arrow::datatypes::SchemaRef,
        sleep: Duration,
    }

    #[async_trait::async_trait]
    impl SinkConnector for SlowSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            tokio::time::sleep(self.sleep).await;
            Ok(WriteResult::new(1, 0))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_sink_task_write_timeout_emits_event() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let sink = SlowSink {
            schema,
            sleep: Duration::from_secs(60),
        };
        let (handle, events) =
            spawn_with_defaults("slow", Box::new(sink), Duration::from_millis(50));

        handle.write_batch(test_batch()).await.unwrap();
        // With paused time, sleep auto-advances when all tasks are
        // blocked on time, firing the sink task's 50ms timeout first.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let event = events
            .try_recv()
            .expect("expected a SinkEvent::WriteTimeout");
        match event {
            SinkEvent::WriteTimeout {
                sink_id,
                rows,
                timeout,
                ..
            } => {
                assert_eq!(&*sink_id, "slow");
                assert_eq!(rows, 3);
                assert_eq!(timeout, Duration::from_millis(50));
            }
            other => panic!("expected WriteTimeout, got {other:?}"),
        }
    }

    struct LateBlockingWriteSink {
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl SinkConnector for LateBlockingWriteSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            std::thread::sleep(Duration::from_millis(25));
            Ok(WriteResult::new(1, 0))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_millis(5)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_blocking_write_is_a_timeout_and_retires_the_actor() {
        let sink = LateBlockingWriteSink {
            schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
        };
        let (handle, mut events) =
            spawn_with_defaults("late-write", Box::new(sink), Duration::from_millis(5));

        handle.write_batch(test_batch()).await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("late write did not report its deadline")
            .expect("sink event channel closed unexpectedly");

        assert!(matches!(
            event,
            SinkEvent::WriteTimeout { sink_id, rows: 3, .. } if &*sink_id == "late-write"
        ));
        assert!(
            handle
                .wait_terminal_until(Instant::now() + Duration::from_secs(1))
                .await,
            "late write did not retire its connector generation"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cluster_late_blocking_write_cannot_cross_its_deadline() {
        use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

        let node = laminar_core::state::NodeId(92);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
            .unwrap();
        let sink = LateBlockingWriteSink {
            schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
        };
        let (event_tx, mut events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "cluster-late-write".into(),
            sink_id: Arc::from("cluster-late-write"),
            connector: Box::new(sink),
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_millis(5),
            event_tx,
            terminal_tasks: None,
            process_authority: Some(controller),
        });

        handle.write_batch(test_batch()).await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("cluster late write did not report its deadline")
            .expect("sink event channel closed unexpectedly");

        assert!(matches!(
            event,
            SinkEvent::WriteTimeout { sink_id, rows: 3, .. }
                if &*sink_id == "cluster-late-write"
        ));
        assert!(
            handle
                .wait_terminal_until(Instant::now() + Duration::from_secs(1))
                .await,
            "cluster late write did not retire its connector generation"
        );
    }

    struct RetiredWriteSink {
        schema: arrow::datatypes::SchemaRef,
        completed: Arc<AtomicBool>,
        flushes: Arc<AtomicU64>,
        closed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl SinkConnector for RetiredWriteSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::RetireConnector
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            self.completed.store(true, Ordering::Release);
            Ok(WriteResult::new(1, 0))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closed.store(true, Ordering::Release);
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_write_retires_actor_without_late_completion_or_cleanup() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let completed = Arc::new(AtomicBool::new(false));
        let flushes = Arc::new(AtomicU64::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let sink = RetiredWriteSink {
            schema,
            completed: Arc::clone(&completed),
            flushes: Arc::clone(&flushes),
            closed: Arc::clone(&closed),
        };
        let (handle, events) =
            spawn_with_defaults("retired-write", Box::new(sink), Duration::from_millis(50));

        handle.write_batch(test_batch()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            events.try_recv(),
            Ok(SinkEvent::WriteTimeout { sink_id, .. }) if &*sink_id == "retired-write"
        ));
        assert!(!completed.load(Ordering::Acquire));
        assert!(
            !handle.has_unresolved_task(),
            "the retired actor must terminate after dropping the overdue write"
        );

        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert!(!completed.load(Ordering::Acquire));
        assert_eq!(flushes.load(Ordering::Acquire), 0);
        assert!(!closed.load(Ordering::Acquire));
    }

    struct UnknownOutcomeSink {
        schema: arrow::datatypes::SchemaRef,
        flushes: Arc<AtomicU64>,
        closes: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl SinkConnector for UnknownOutcomeSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CancelSafe
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Err(ConnectorError::outcome_unknown(
                "remote acknowledgement was lost",
                true,
            ))
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test]
    async fn connector_reported_unknown_outcome_retires_even_cancel_safe_generation() {
        let flushes = Arc::new(AtomicU64::new(0));
        let closes = Arc::new(AtomicU64::new(0));
        let sink = UnknownOutcomeSink {
            schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
            flushes: Arc::clone(&flushes),
            closes: Arc::clone(&closes),
        };
        let (handle, events) =
            spawn_with_defaults("unknown-outcome", Box::new(sink), Duration::from_secs(5));

        handle.write_batch(test_batch()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while handle.has_unresolved_task() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retired sink terminal proof did not settle");
        assert!(!handle.has_unresolved_task());
        assert!(matches!(
            events.try_recv(),
            Ok(SinkEvent::WriteError { sink_id, error, .. })
                if &*sink_id == "unknown-outcome" && error.contains("outcome unknown")
        ));
        let close_error = handle.close().await.unwrap_err().to_string();
        assert!(
            close_error.contains("retired before close"),
            "{close_error}"
        );
        assert_eq!(flushes.load(Ordering::SeqCst), 0);
        assert_eq!(closes.load(Ordering::SeqCst), 0);
    }

    struct UnknownProtocolSink {
        schema: arrow::datatypes::SchemaRef,
        closes: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl SinkConnector for UnknownProtocolSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CancelSafe
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(1, 0))
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            Err(ConnectorError::outcome_unknown(
                "flush acknowledgement was lost",
                true,
            ))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test]
    async fn acked_unknown_protocol_outcome_is_reported_before_actor_retirement() {
        let closes = Arc::new(AtomicU64::new(0));
        let sink = UnknownProtocolSink {
            schema: Arc::new(Schema::empty()),
            closes: Arc::clone(&closes),
        };
        let (handle, _events) =
            spawn_with_defaults("unknown-flush", Box::new(sink), Duration::from_secs(5));

        let error = handle.flush().await.unwrap_err();
        assert!(error.is_outcome_unknown(), "{error}");
        assert!(error.to_string().contains("flush acknowledgement was lost"));
        for _ in 0..100 {
            if !handle.has_unresolved_task() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(!handle.has_unresolved_task());
        let close_error = handle.close().await.unwrap_err().to_string();
        assert!(
            close_error.contains("retired before close"),
            "{close_error}"
        );
        assert_eq!(closes.load(Ordering::SeqCst), 0);
    }

    /// A slow write holds the actor while a following protocol command waits in the queue.
    /// The queued command must retain its enqueue-time deadline and must not call the connector
    /// after that deadline has elapsed.
    struct QueueDeadlineSink {
        schema: arrow::datatypes::SchemaRef,
        flushes: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl SinkConnector for QueueDeadlineSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CancelSafe
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(WriteResult::new(1, 0))
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn queued_protocol_command_cannot_refresh_its_deadline() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let flushes = Arc::new(AtomicU64::new(0));
        let sink = QueueDeadlineSink {
            schema,
            flushes: Arc::clone(&flushes),
        };
        let (handle, _events) =
            spawn_with_defaults("queued", Box::new(sink), Duration::from_millis(50));

        handle.write_batch(test_batch()).await.unwrap();
        let error = handle.flush().await.unwrap_err().to_string();
        assert!(error.contains("end-to-end deadline"), "{error}");

        // Fence behind the expired flush to prove the actor inspected it without invoking the
        // connector. A fresh per-operation timeout in the actor would increment this counter.
        handle.sync().await.unwrap();
        assert_eq!(flushes.load(Ordering::SeqCst), 0);
        handle.close().await.unwrap();
    }

    struct QueueCommitDeadlineSink {
        schema: arrow::datatypes::SchemaRef,
        write_started: Arc<AtomicBool>,
        write_gate: Arc<tokio::sync::Notify>,
        observed_remaining: Arc<parking_lot::Mutex<Option<Duration>>>,
    }

    #[async_trait::async_trait]
    impl SinkConnector for QueueCommitDeadlineSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CancelSafe
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            self.write_started.store(true, Ordering::Release);
            self.write_gate.notified().await;
            Ok(WriteResult::new(1, 0))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_millis(100)
        }

        fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl CoordinatedCommitter for QueueCommitDeadlineSink {
        async fn commit_aggregated(
            &self,
            _batch: CoordinatedCommitBatch,
            context: CoordinatedCommitContext,
        ) -> Result<(), ConnectorError> {
            *self.observed_remaining.lock() = Some(context.remaining());
            Ok(())
        }

        async fn committed_cursor(
            &self,
            _namespace: &CoordinatedCommitNamespace,
        ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
            Ok(None)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn queued_coordinated_commit_receives_only_its_remaining_budget() {
        use laminar_connectors::connector::{CoordinatedCommitNamespace, CoordinatedCommitPayload};
        use laminar_core::state::CheckpointAttempt;
        use laminar_core::storage::checkpoint_manifest::PipelineIdentity;

        let write_started = Arc::new(AtomicBool::new(false));
        let write_gate = Arc::new(tokio::sync::Notify::new());
        let observed_remaining = Arc::new(parking_lot::Mutex::new(None));
        let sink = QueueCommitDeadlineSink {
            schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
            write_started: Arc::clone(&write_started),
            write_gate: Arc::clone(&write_gate),
            observed_remaining: Arc::clone(&observed_remaining),
        };
        let (event_tx, _events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "queued-commit".into(),
            sink_id: Arc::from("queued-commit"),
            connector: Box::new(sink),
            contract: checkpoint_committable_contract(),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_millis(100),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });
        handle.write_batch(test_batch()).await.unwrap();
        while !write_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        let attempt = CheckpointAttempt::new(1, 101);
        let namespace = CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "queued-commit",
        )
        .unwrap();
        let commit = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle
                    .commit_aggregated(CoordinatedCommitBatch {
                        namespace,
                        expected_predecessor: CoordinatedCommitCursor {
                            checkpoint_id: 0,
                            fencing_token: 0,
                        },
                        fencing_token: 1,
                        target: attempt,
                        entries: vec![CoordinatedCommitPayload {
                            attempt,
                            participant_id: 0,
                            payload: None,
                        }],
                    })
                    .await
            }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(40)).await;
        write_gate.notify_waiters();
        commit.await.unwrap().unwrap();

        let remaining = observed_remaining.lock().unwrap();
        assert!(remaining <= Duration::from_millis(60), "got {remaining:?}");
        assert!(
            remaining > Duration::ZERO,
            "commit reached connector expired"
        );
        handle.close().await.unwrap();
    }

    struct SlowFlushSink {
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl SinkConnector for SlowFlushSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CancelSafe
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(1, 0))
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    struct CloseDeadlineSink {
        schema: arrow::datatypes::SchemaRef,
        close_calls: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl SinkConnector for CloseDeadlineSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CancelSafe
        }

        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(1, 0))
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn close_uses_one_deadline_and_never_starts_late_connector_close() {
        let close_calls = Arc::new(AtomicU64::new(0));
        let sink = CloseDeadlineSink {
            schema: Arc::new(Schema::empty()),
            close_calls: Arc::clone(&close_calls),
        };
        let (handle, _events) =
            spawn_with_defaults("close-deadline", Box::new(sink), Duration::from_secs(5));
        let started = Instant::now();
        let admission = handle.admission.lock().await;
        let close_handle = handle.clone();
        let close = tokio::spawn(async move { close_handle.close().await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        drop(admission);

        close
            .await
            .unwrap()
            .expect_err("shutdown flush must consume the one close deadline");

        assert_eq!(Instant::now() - started, SINK_CLOSE_TIMEOUT);
        assert_eq!(close_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn protocol_connector_operation_uses_configured_budget() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let (event_tx, _event_rx) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "slow-flush".into(),
            sink_id: Arc::from("slow-flush"),
            connector: Box::new(SlowFlushSink { schema }),
            // Avoid a second implicit flush during close; explicit flush is still valid here.
            contract: checkpoint_committable_contract(),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_millis(25),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });

        let started = Instant::now();
        let error = handle.flush().await.unwrap_err().to_string();
        assert!(error.contains("end-to-end deadline"), "{error}");
        assert_eq!(Instant::now() - started, Duration::from_millis(25));
        handle.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn protocol_operation_uses_earlier_caller_deadline() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let (event_tx, _event_rx) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: "attempt-clamped-flush".into(),
            sink_id: Arc::from("attempt-clamped-flush"),
            connector: Box::new(SlowFlushSink { schema }),
            contract: checkpoint_committable_contract(),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_secs(5),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        });

        let started = Instant::now();
        let deadline = started + Duration::from_millis(25);
        let error = handle.flush_until(deadline).await.unwrap_err().to_string();
        assert!(error.contains("end-to-end deadline"), "{error}");
        assert_eq!(Instant::now() - started, Duration::from_millis(25));
        handle.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn expired_caller_deadline_never_enqueues_protocol_command() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let flushes = Arc::new(AtomicU64::new(0));
        let sink = QueueDeadlineSink {
            schema,
            flushes: Arc::clone(&flushes),
        };
        let (handle, _events) =
            spawn_with_defaults("expired", Box::new(sink), Duration::from_secs(5));

        let error = handle
            .flush_until(Instant::now())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("end-to-end deadline"), "{error}");
        handle.sync().await.unwrap();
        assert_eq!(flushes.load(Ordering::SeqCst), 0);
        handle.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn expired_write_deadline_never_enqueues_batch() {
        let (sink, writes, _flushes) = CountingSink::new();
        let (handle, _events) =
            spawn_with_defaults("expired-write", Box::new(sink), Duration::from_secs(5));

        let error = handle
            .write_batch_until(test_batch(), Instant::now())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("end-to-end deadline"), "{error}");
        handle.sync().await.unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 0);
        handle.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn write_enqueue_timeout_poison_rejects_checkpoint_flush() {
        let write_timeout = Duration::from_millis(25);
        let (event_tx, events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);

        // Keep the sole queue slot occupied without running an actor. This isolates the handle's
        // enqueue deadline from connector execution and makes the dropped command deterministic.
        let (filler_ack, _filler_rx) = oneshot::oneshot();
        tx.send(SinkCommand {
            deadline: operation_deadline(Duration::from_secs(60)),
            operation: SinkOperation::Sync { ack: filler_ack },
        })
        .await
        .unwrap();
        let epoch_poisoned = Arc::new(AtomicBool::new(false));
        let (task, actor_state) = supervise_test_actor(async {}, None);
        let terminal_state = Arc::clone(&task.terminal_state);
        let handle = SinkTaskHandle {
            name: Arc::from("saturated"),
            sink_id: Arc::from("saturated"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout,
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(task))),
            close_state: Arc::new(SinkCloseState::new()),
            terminal_state,
            actor_state,
            runtime: tokio::runtime::Handle::current(),
            event_tx,
            epoch_poisoned: Arc::clone(&epoch_poisoned),
            #[cfg(feature = "cluster")]
            process_authority: None,
        };

        let error = handle.write_batch(test_batch()).await.unwrap_err();
        assert!(error.to_string().contains("write enqueue"), "{error}");
        assert!(epoch_poisoned.load(Ordering::Acquire));

        let flush_error = handle.flush().await.unwrap_err();
        assert!(
            flush_error.to_string().contains("poisoned"),
            "{flush_error}"
        );
        assert!(matches!(
            events.try_recv(),
            Ok(SinkEvent::WriteEnqueueTimeout {
                sink_id,
                rows: 3,
                timeout,
            }) if &*sink_id == "saturated" && timeout == write_timeout
        ));

        drop(rx);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_actor_is_retained_until_uncooperative_join_is_terminal() {
        let (event_tx, _events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);
        let (filler_ack, _filler_rx) = oneshot::oneshot();
        tx.send(SinkCommand {
            deadline: operation_deadline(Duration::from_secs(60)),
            operation: SinkOperation::Sync { ack: filler_ack },
        })
        .await
        .unwrap();

        let started = Arc::new(AtomicBool::new(false));
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let task_started = Arc::clone(&started);
        let task_gate = Arc::clone(&gate);
        let actor_state = Arc::new(SinkActorState::new());
        let actor_lifetime = SinkActorLifetime(Arc::clone(&actor_state));
        let task = tokio::task::spawn_blocking(move || {
            let _lifetime = actor_lifetime;
            task_started.store(true, Ordering::Release);
            let (lock, released) = &*task_gate;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                ready = released.wait(ready).unwrap();
            }
        });
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        let task = supervise_sink_task(
            task,
            None,
            Arc::clone(&actor_state),
            &tokio::runtime::Handle::current(),
        );
        let terminal_state = Arc::clone(&task.terminal_state);
        let handle = SinkTaskHandle {
            name: Arc::from("uncooperative-cancel-safe"),
            sink_id: Arc::from("uncooperative-cancel-safe"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout: Duration::from_secs(1),
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(task))),
            close_state: Arc::new(SinkCloseState::new()),
            terminal_state,
            actor_state,
            runtime: tokio::runtime::Handle::current(),
            event_tx,
            epoch_poisoned: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            process_authority: None,
        };

        let close = tokio::spawn({
            let handle = handle.clone();
            async move { handle.close().await }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(SINK_CLOSE_TIMEOUT).await;
        let error = close
            .await
            .unwrap()
            .expect_err("the public close deadline must remain bounded");
        let unresolved_before_release = handle.has_unresolved_task();

        let (lock, released) = &*gate;
        *lock.lock().unwrap() = true;
        released.notify_all();
        assert!(error.to_string().contains("enqueue"), "{error}");
        assert!(
            unresolved_before_release,
            "aborting an uncooperative task must not erase the replacement fence"
        );
        let repeated = handle
            .close()
            .await
            .expect_err("terminal timeout result must persist after the actor exits");
        assert!(repeated.to_string().contains("enqueue"), "{repeated}");
        tokio::time::timeout(Duration::from_secs(1), async {
            while handle.has_unresolved_task() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal supervisor did not observe the released blocking actor");
        assert!(!handle.has_unresolved_task());
        drop(rx);
    }

    #[tokio::test]
    async fn connector_child_task_holds_replacement_fence_after_actor_exit() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let guard = owner.track().expect("terminal task owner must be live");
        drop(owner);

        let (event_tx, _events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);
        drop(rx);
        let runtime = tokio::runtime::Handle::current();
        let (task, actor_state) = supervise_test_actor(async {}, Some(tracker));
        let terminal_state = Arc::clone(&task.terminal_state);
        let handle = SinkTaskHandle {
            name: Arc::from("terminal-child"),
            sink_id: Arc::from("terminal-child"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout: Duration::from_secs(1),
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(task))),
            close_state: Arc::new(SinkCloseState::new()),
            terminal_state,
            actor_state,
            runtime,
            event_tx,
            epoch_poisoned: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            process_authority: None,
        };

        while !handle.actor_state.finished.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        let error = handle
            .close()
            .await
            .expect_err("retired actor must fail close");
        assert!(
            error.to_string().contains("retired before close"),
            "{error}"
        );
        assert!(handle.has_unresolved_task());

        drop(guard);
        tokio::time::timeout(Duration::from_secs(1), async {
            while handle.has_unresolved_task() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal child did not release replacement fence");
    }

    #[test]
    fn abort_before_first_poll_drops_sink_actor_before_publishing_terminal() {
        struct DropProbe {
            terminal: Arc<parking_lot::Mutex<Option<Arc<SinkActorState>>>>,
            dropped: Arc<AtomicBool>,
            terminal_was_finished: Arc<AtomicBool>,
        }

        impl std::future::Future for DropProbe {
            type Output = ();

            fn poll(
                self: std::pin::Pin<&mut Self>,
                _context: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                panic!("sink actor was polled before its immediate abort");
            }
        }

        impl Drop for DropProbe {
            fn drop(&mut self) {
                let terminal = self
                    .terminal
                    .lock()
                    .clone()
                    .expect("terminal state must be installed before abort");
                self.terminal_was_finished
                    .store(terminal.finished.load(Ordering::Acquire), Ordering::Release);
                self.dropped.store(true, Ordering::Release);
            }
        }

        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        executor.block_on(async {
            let terminal_slot = Arc::new(parking_lot::Mutex::new(None));
            let dropped = Arc::new(AtomicBool::new(false));
            let terminal_was_finished = Arc::new(AtomicBool::new(false));
            let terminal = Arc::new(SinkActorState::new());
            let join = spawn_sink_actor(
                &tokio::runtime::Handle::current(),
                DropProbe {
                    terminal: Arc::clone(&terminal_slot),
                    dropped: Arc::clone(&dropped),
                    terminal_was_finished: Arc::clone(&terminal_was_finished),
                },
                Arc::clone(&terminal),
            );
            *terminal_slot.lock() = Some(Arc::clone(&terminal));

            join.abort();
            assert!(join
                .await
                .expect_err("the unpolled sink actor must be cancelled")
                .is_cancelled());
            assert!(dropped.load(Ordering::Acquire));
            assert!(!terminal_was_finished.load(Ordering::Acquire));
            assert!(terminal.finished.load(Ordering::Acquire));
        });
    }

    #[tokio::test]
    async fn cancelled_terminal_supervisor_cannot_publish_false_terminal() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let guard = owner.track().expect("live connector child");
        drop(owner);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let (task, actor_state) = supervise_test_actor(
            async move {
                let _ = release_rx.await;
            },
            Some(tracker),
        );
        task.terminal_join.abort();
        let terminal_state = Arc::clone(&task.terminal_state);
        let (event_tx, _events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let (tx, _rx) = mpsc::bounded_async::<SinkCommand>(1);
        let handle = SinkTaskHandle {
            name: Arc::from("cancelled-terminal-supervisor"),
            sink_id: Arc::from("cancelled-terminal-supervisor"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout: Duration::from_secs(1),
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(task))),
            close_state: Arc::new(SinkCloseState::new()),
            terminal_state,
            actor_state,
            runtime: tokio::runtime::Handle::current(),
            event_tx,
            epoch_poisoned: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            process_authority: None,
        };

        assert!(handle.has_unresolved_task());
        assert!(
            !handle
                .wait_terminal_until(Instant::now() + Duration::from_millis(20))
                .await,
            "supervisor cancellation must not substitute for actor exit"
        );

        let _ = release_tx.send(());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !handle.actor_state.finished.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actor did not exit after release");
        assert!(handle.has_unresolved_task());
        assert!(
            !handle
                .wait_terminal_until(Instant::now() + Duration::from_millis(20))
                .await,
            "actor exit must not substitute for connector-child termination"
        );

        drop(guard);
        assert!(
            handle
                .wait_terminal_until(Instant::now() + Duration::from_secs(1))
                .await
        );
        assert!(!handle.has_unresolved_task());
    }

    #[tokio::test]
    async fn close_driver_panic_is_sticky_but_terminal_proof_remains_observable() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let guard = owner.track().expect("live connector child");
        drop(owner);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let runtime = tokio::runtime::Handle::current();
        let (task, actor_state) = supervise_test_actor(
            async move {
                let _ = release_rx.await;
            },
            Some(tracker),
        );
        let terminal_state = Arc::clone(&task.terminal_state);
        let (event_tx, _events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let (tx, _rx) = mpsc::bounded_async::<SinkCommand>(1);
        let handle = SinkTaskHandle {
            name: Arc::from("panicked-close-driver"),
            sink_id: Arc::from("panicked-close-driver"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout: Duration::from_secs(1),
            closing: Arc::new(AtomicBool::new(true)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(None)),
            close_state: Arc::new(SinkCloseState::new()),
            terminal_state,
            actor_state,
            runtime: runtime.clone(),
            event_tx,
            epoch_poisoned: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            process_authority: None,
        };
        let state = Arc::clone(&handle.close_state);
        spawn_sink_close_driver_future(
            Arc::clone(&handle.name),
            Arc::clone(&state),
            async move {
                let _task = task;
                panic!("injected terminal driver panic");
                #[allow(unreachable_code)]
                SinkCloseOutcome::Success
            },
            &runtime,
        );

        let error = wait_for_sink_close(
            handle.name(),
            Arc::clone(&state),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect_err("driver panic must publish an immediate close failure");
        assert!(error.to_string().contains("terminal close driver panicked"));
        assert!(handle.has_unresolved_task());

        let _ = release_tx.send(());
        drop(guard);
        tokio::time::timeout(Duration::from_secs(1), async {
            while handle.has_unresolved_task() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached terminal proof did not observe actor and child termination");
        let repeated = handle
            .close()
            .await
            .expect_err("driver panic result must remain sticky");
        assert!(repeated
            .to_string()
            .contains("terminal close driver panicked"));
    }

    #[tokio::test]
    async fn checkpoint_flush_actor_rechecks_shared_poison() {
        let (sink, _writes, flushes) = CountingSink::new();
        let (handle, _events) =
            spawn_with_defaults("poisoned", Box::new(sink), Duration::from_secs(5));
        handle.epoch_poisoned.store(true, Ordering::Release);

        // Bypass SinkTaskHandle::flush to exercise the actor-side race check directly.
        let (ack_tx, ack_rx) = oneshot::oneshot();
        handle
            .tx
            .send(SinkCommand {
                deadline: operation_deadline(Duration::from_secs(5)),
                operation: SinkOperation::Flush { ack: ack_tx },
            })
            .await
            .unwrap();
        let error = ack_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("poisoned"), "{error}");
        assert_eq!(flushes.load(Ordering::Acquire), 0);

        handle.close().await.unwrap();
    }

    /// Verifies channel-closed errors emit a `SinkEvent::ChannelClosed`.
    #[tokio::test]
    async fn test_sink_task_channel_closed_emits_event() {
        let (event_tx, events) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);
        drop(rx);
        let (task, actor_state) = supervise_test_actor(async {}, None);
        let terminal_state = Arc::clone(&task.terminal_state);
        let handle = SinkTaskHandle {
            name: Arc::from("dead"),
            sink_id: Arc::from("dead"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout: Duration::from_secs(5),
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(task))),
            close_state: Arc::new(SinkCloseState::new()),
            terminal_state,
            actor_state,
            runtime: tokio::runtime::Handle::current(),
            event_tx,
            epoch_poisoned: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            process_authority: None,
        };

        // A disconnected actor must reject the write, poison the replay-required epoch, and
        // report the unexpected channel loss.
        let err = handle.write_batch(test_batch()).await.unwrap_err();
        assert!(matches!(err, ConnectorError::ConnectionFailed(_)));
        let flush_error = handle.flush().await.unwrap_err();
        assert!(
            flush_error.to_string().contains("poisoned"),
            "{flush_error}"
        );

        let event = events
            .try_recv()
            .expect("expected SinkEvent::ChannelClosed");
        assert!(matches!(event, SinkEvent::ChannelClosed { sink_id } if &*sink_id == "dead"));
    }
}
