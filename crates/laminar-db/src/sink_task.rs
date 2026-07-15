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
    ConnectorCancellationPolicy, CoordinatedCommitBatch, CoordinatedCommitContext,
    CoordinatedCommitCursor, CoordinatedCommitNamespace, CoordinatedCommitter, SinkConnector,
    SinkContract,
};
use laminar_connectors::error::ConnectorError;
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

async fn bounded_connector_operation<T, F, Fut>(
    sink_name: &str,
    operation: &str,
    deadline: Instant,
    cancellation_policy: ConnectorCancellationPolicy,
    make_future: F,
) -> Result<T, ConnectorError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, ConnectorError>>,
{
    if deadline <= Instant::now() {
        return Err(protocol_deadline_error(sink_name, operation));
    }
    let mut future = std::pin::pin!(make_future());
    match tokio::time::timeout_at(deadline, future.as_mut()).await {
        Ok(result) => result,
        Err(_) => {
            if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
                match future.await {
                    Ok(_) => tracing::warn!(
                        sink = sink_name,
                        operation,
                        "connector operation completed after its deadline"
                    ),
                    Err(error) => tracing::warn!(
                        sink = sink_name,
                        operation,
                        %error,
                        "connector operation failed after its deadline"
                    ),
                }
            }
            Err(protocol_deadline_error(sink_name, operation))
        }
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
    finished: AtomicBool,
    phase: parking_lot::Mutex<&'static str>,
    outcome: parking_lot::Mutex<Option<SinkCloseOutcome>>,
    notify: tokio::sync::Notify,
}

impl SinkCloseState {
    fn new() -> Self {
        Self {
            finished: AtomicBool::new(false),
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

    fn finish(&self, outcome: SinkCloseOutcome) {
        *self.outcome.lock() = Some(outcome);
        self.finished.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn outcome(&self) -> Option<SinkCloseOutcome> {
        self.outcome.lock().clone()
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
    cancellation_policy: ConnectorCancellationPolicy,
    closing: Arc<AtomicBool>,
    /// Linearizes command admission with Close so no producer can enqueue behind it.
    admission: Arc<tokio::sync::Mutex<()>>,
    // The terminal driver takes this exactly once. Public close futures never own the actor.
    task: Arc<parking_lot::Mutex<Option<JoinHandle<()>>>>,
    close_state: Arc<SinkCloseState>,
    /// Runtime that owns the actor. Terminal cleanup must not be spawned on the short-lived
    /// compute callback runtime that happened to call `close()`.
    runtime: tokio::runtime::Handle,
    event_tx: Producer<SinkEvent>,
    /// Sticky for the current epoch. Shared with the actor so a write rejected before enqueue
    /// cannot be hidden from the checkpoint protocol.
    epoch_poisoned: Arc<AtomicBool>,
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
        } = config;
        let (tx, rx) = mpsc::bounded_async::<SinkCommand>(channel_capacity);
        let cancellation_policy = connector.cancellation_policy();
        let task_sink_id = Arc::clone(&sink_id);
        let task_event_tx = event_tx.clone();
        let task_name = name.clone();
        let epoch_poisoned = Arc::new(AtomicBool::new(false));
        let runtime = tokio::runtime::Handle::current();
        let handle = runtime.spawn(run_sink_task(
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
            },
            Arc::clone(&epoch_poisoned),
        ));

        Self {
            name: Arc::from(name),
            sink_id,
            tx,
            contract,
            requires_recovery_on_error,
            write_timeout,
            cancellation_policy,
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(handle))),
            close_state: Arc::new(SinkCloseState::new()),
            runtime,
            event_tx,
            epoch_poisoned,
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
            Err(self.closed_err())
        } else {
            Ok(())
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
        let (ack_tx, ack_rx) = oneshot::oneshot();
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
        match tokio::time::timeout_at(deadline, ack_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(self.ack_dropped_err(operation)),
            Err(_) => Err(command_deadline_error(
                &self.name,
                operation,
                effective_timeout,
            )),
        }
    }

    /// Send a batch; backpressures when the sink is behind.
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<(), ConnectorError> {
        let rows = batch.num_rows();
        let deadline = operation_deadline(self.write_timeout);
        let admission = match tokio::time::timeout_at(deadline, self.admission.lock()).await {
            Ok(admission) => admission,
            Err(_) => {
                self.poison_epoch_if_recovery_required();
                let _ = self.event_tx.try_push(SinkEvent::WriteEnqueueTimeout {
                    sink_id: Arc::clone(&self.sink_id),
                    rows,
                    timeout: self.write_timeout,
                });
                return Err(command_deadline_error(
                    &self.name,
                    "write admission",
                    self.write_timeout,
                ));
            }
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
                    timeout: self.write_timeout,
                });
                Err(command_deadline_error(
                    &self.name,
                    "write enqueue",
                    self.write_timeout,
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
                    self.cancellation_policy,
                    handle,
                    Arc::clone(&self.close_state),
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

    /// True while a connector operation or terminal close still owns the sink actor.
    pub(crate) fn has_unresolved_task(&self) -> bool {
        if self.closing.load(Ordering::Acquire) {
            !self.close_state.finished.load(Ordering::Acquire)
        } else {
            self.task
                .lock()
                .as_ref()
                .is_some_and(|handle| !handle.is_finished())
        }
    }

    pub(crate) fn same_actor(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.close_state, &other.close_state)
    }
}

fn spawn_sink_close_driver(
    name: Arc<str>,
    tx: SinkCommandTx,
    cancellation_policy: ConnectorCancellationPolicy,
    handle: JoinHandle<()>,
    state: Arc<SinkCloseState>,
    runtime: &tokio::runtime::Handle,
) {
    let supervisor = runtime.spawn(async move {
        let close = drive_sink_close(
            Arc::clone(&name),
            tx,
            cancellation_policy,
            handle,
            Arc::clone(&state),
        );
        match std::panic::AssertUnwindSafe(close).catch_unwind().await {
            Ok(outcome) => state.finish(outcome),
            Err(_) => {
                // The actor JoinHandle was inside the unwound future, so terminal completion is
                // no longer provable. Fail closed: keep the generation fence permanently set.
                state.set_phase("terminal driver panic");
                state.notify.notify_waiters();
                tracing::error!(sink = %name, "sink terminal close driver panicked; replacement remains fenced");
            }
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
    cancellation_policy: ConnectorCancellationPolicy,
    mut handle: JoinHandle<()>,
    state: Arc<SinkCloseState>,
) -> SinkCloseOutcome {
    let first_deadline = operation_deadline(SINK_CLOSE_TIMEOUT);
    let (ack_tx, mut ack_rx) = oneshot::oneshot();
    let mut command = SinkCommand {
        deadline: first_deadline,
        operation: SinkOperation::Close { ack: ack_tx },
    };

    loop {
        state.set_phase("enqueue");
        let deadline = if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
            operation_deadline(SINK_CLOSE_TIMEOUT)
        } else {
            first_deadline
        };
        command.deadline = deadline;
        match tx
            .send_with_timer(command, tokio::time::sleep_until(deadline))
            .await
        {
            Ok(()) => break,
            Err(SendTimeoutError::Disconnected(_)) => {
                return finish_disconnected_sink_close(&name, cancellation_policy, handle).await;
            }
            Err(SendTimeoutError::Timeout(returned)) => {
                if cancellation_policy == ConnectorCancellationPolicy::CancelSafe {
                    handle.abort();
                    let _ = handle.await;
                    return SinkCloseOutcome::Failure(Arc::from(
                        close_deadline_error(&name, "enqueue").to_string(),
                    ));
                }
                tracing::warn!(
                    sink = %name,
                    "sink close enqueue is still blocked; retaining terminal ownership"
                );
                command = returned;
            }
        }
    }

    state.set_phase("acknowledgement");
    let connector_result = if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
        match (&mut ack_rx).await {
            Ok(result) => result,
            Err(_) => Err(ConnectorError::ConnectionFailed(format!(
                "sink task '{name}' dropped close acknowledgment"
            ))),
        }
    } else {
        match tokio::time::timeout_at(first_deadline, &mut ack_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConnectorError::ConnectionFailed(format!(
                "sink task '{name}' dropped close acknowledgment"
            ))),
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                return SinkCloseOutcome::Failure(Arc::from(
                    close_deadline_error(&name, "acknowledgement").to_string(),
                ));
            }
        }
    };

    state.set_phase("join");
    let join_result = if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
        handle.await.map_err(|error| {
            ConnectorError::Internal(format!(
                "sink task '{name}' failed while joining after close: {error}"
            ))
        })
    } else {
        match tokio::time::timeout_at(first_deadline, &mut handle).await {
            Ok(result) => result.map_err(|error| {
                ConnectorError::Internal(format!(
                    "sink task '{name}' failed while joining after close: {error}"
                ))
            }),
            Err(_) => {
                // CancelSafe is an audited promise that dropping the connector future cannot
                // leave an external mutation in flight. The stable driver, rather than the
                // public caller, owns the unbounded terminal observation after abort.
                handle.abort();
                let _ = handle.await;
                Err(close_deadline_error(&name, "join"))
            }
        }
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

async fn finish_disconnected_sink_close(
    name: &str,
    cancellation_policy: ConnectorCancellationPolicy,
    handle: JoinHandle<()>,
) -> SinkCloseOutcome {
    if cancellation_policy == ConnectorCancellationPolicy::CancelSafe {
        handle.abort();
    }
    let _ = handle.await;
    SinkCloseOutcome::Failure(Arc::from(format!(
        "sink task '{name}' rejected close command: channel closed"
    )))
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
}

// In replay-required modes, `epoch_poisoned` rejects checkpoint Flush/PreCommit so no durable cut
// can pass a dropped write. Local best-effort mode reports loss without permanently fencing state.
async fn run_sink_task(mut inner: SinkTaskInner, epoch_poisoned: Arc<AtomicBool>) {
    let mut flush_timer = tokio::time::interval(inner.flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    flush_timer.tick().await; // skip the first immediate tick

    let mut current_epoch: u64 = 0;
    loop {
        tokio::select! {
            cmd = inner.rx.recv() => {
                let Ok(cmd) = cmd else {
                    tracing::debug!(sink = %inner.name, "Sink command channel closed");
                    if !inner.contract.is_checkpoint_committable() {
                        if let Err(e) = bounded_connector_operation(
                            &inner.name,
                            "flush on channel close",
                            operation_deadline(inner.write_timeout),
                            inner.sink.cancellation_policy(),
                            || inner.sink.flush(),
                        ).await {
                            tracing::warn!(sink = %inner.name, error = %e,
                                "Sink flush failed on channel close");
                        }
                    }
                    if let Err(e) = bounded_connector_operation(
                        &inner.name,
                        "connector close",
                        operation_deadline(SINK_CLOSE_TIMEOUT),
                        inner.sink.cancellation_policy(),
                        || inner.sink.close(),
                    ).await {
                        tracing::warn!(sink = %inner.name, error = %e,
                            "Sink close failed on channel close");
                    }
                    break;
                };
                let stop = handle_sink_command(
                    &mut inner,
                    cmd.operation,
                    cmd.deadline,
                    &mut current_epoch,
                    epoch_poisoned.as_ref(),
                )
                .await;
                if stop {
                    break;
                }
            }
            _ = flush_timer.tick() => {
                if !inner.contract.is_checkpoint_committable() {
                    if let Err(error) = bounded_connector_operation(
                        &inner.name,
                        "periodic flush",
                        operation_deadline(inner.write_timeout),
                        inner.sink.cancellation_policy(),
                        || inner.sink.flush(),
                    ).await {
                        record_flush_error(
                            &inner,
                            current_epoch,
                            "periodic flush",
                            &error,
                            epoch_poisoned.as_ref(),
                        );
                    }
                }
            }
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
) -> bool {
    match operation {
        SinkOperation::WriteBatch { batch } => {
            handle_write_batch(inner, batch, deadline, *current_epoch, epoch_poisoned).await;
        }
        SinkOperation::BeginEpoch { epoch, ack } => {
            ack.send(begin_sink_epoch(inner, epoch, deadline, current_epoch, epoch_poisoned).await);
        }
        SinkOperation::Flush { ack } => {
            ack.send(flush_checkpoint_sink(inner, deadline, *current_epoch, epoch_poisoned).await);
        }
        SinkOperation::PreCommit { epoch, ack } => {
            ack.send(pre_commit_sink(inner, epoch, deadline, epoch_poisoned).await);
        }
        SinkOperation::CommitAggregated { batch, ack } => {
            let cancellation_policy = inner.sink.cancellation_policy();
            let committer = inner.sink.as_coordinated_committer();
            ack.send(
                commit_aggregated_sink(
                    &inner.name,
                    committer,
                    batch,
                    deadline,
                    cancellation_policy,
                )
                .await,
            );
        }
        SinkOperation::CommittedCursor { namespace, ack } => {
            let cancellation_policy = inner.sink.cancellation_policy();
            let committer = inner.sink.as_coordinated_committer();
            ack.send(
                committed_cursor(
                    &inner.name,
                    committer,
                    &namespace,
                    deadline,
                    cancellation_policy,
                )
                .await,
            );
        }
        SinkOperation::RollbackEpoch { epoch, ack } => {
            let result = handle_rollback_epoch(inner, epoch, deadline).await;
            ack.send(result);
        }
        SinkOperation::Sync { ack } => {
            ack.send(validate_sync_deadline(&inner.name, deadline));
        }
        SinkOperation::Close { ack } => {
            // Queue residence is covered by the public close budget, but terminal cleanup owns
            // its own budget once it reaches the actor. Reusing an expired enqueue timestamp
            // would skip the final at-least-once flush after a CompleteStarted write drains.
            let result = close_sink_connector(inner, operation_deadline(SINK_CLOSE_TIMEOUT)).await;
            ack.send(result);
            tracing::debug!(sink = %inner.name, "Sink task closed");
            return true;
        }
    }
    false
}

async fn begin_sink_epoch(
    inner: &mut SinkTaskInner,
    epoch: u64,
    deadline: Instant,
    current_epoch: &mut u64,
    epoch_poisoned: &AtomicBool,
) -> Result<(), ConnectorError> {
    let result = bounded_connector_operation(
        &inner.name,
        "begin_epoch",
        deadline,
        inner.sink.cancellation_policy(),
        || inner.sink.begin_epoch(epoch),
    )
    .await;
    if result.is_ok() {
        *current_epoch = epoch;
        epoch_poisoned.store(false, Ordering::Release);
    }
    result
}

async fn flush_checkpoint_sink(
    inner: &mut SinkTaskInner,
    deadline: Instant,
    current_epoch: u64,
    epoch_poisoned: &AtomicBool,
) -> Result<(), ConnectorError> {
    // A write rejected before enqueue never reaches this actor. The shared poison bit is therefore
    // the durable-cut fence for at-least-once sinks and a race-safe actor-side recheck.
    let already_poisoned = epoch_poisoned.load(Ordering::Acquire);
    let result = if already_poisoned {
        Err(poisoned_epoch_error(&inner.name))
    } else {
        bounded_connector_operation(
            &inner.name,
            "checkpoint flush",
            deadline,
            inner.sink.cancellation_policy(),
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
    result
}

async fn pre_commit_sink(
    inner: &mut SinkTaskInner,
    epoch: u64,
    deadline: Instant,
    epoch_poisoned: &AtomicBool,
) -> Result<Option<Vec<u8>>, ConnectorError> {
    if epoch_poisoned.load(Ordering::Acquire) {
        Err(poisoned_epoch_error(&inner.name))
    } else {
        bounded_connector_operation(
            &inner.name,
            "pre_commit",
            deadline,
            inner.sink.cancellation_policy(),
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
) -> Result<(), ConnectorError> {
    match committer {
        Some(committer) => {
            let context = CoordinatedCommitContext::new(deadline);
            bounded_connector_operation(
                sink_name,
                "coordinated external commit",
                deadline,
                cancellation_policy,
                || committer.commit_aggregated(batch, context),
            )
            .await
        }
        None => Err(ConnectorError::InvalidState {
            expected: "coordinated committer".into(),
            actual: format!("sink '{sink_name}' is not coordinated"),
        }),
    }
}

async fn committed_cursor(
    sink_name: &str,
    committer: Option<&dyn CoordinatedCommitter>,
    namespace: &CoordinatedCommitNamespace,
    deadline: Instant,
    cancellation_policy: ConnectorCancellationPolicy,
) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
    match committer {
        Some(committer) => {
            bounded_connector_operation(
                sink_name,
                "external commit cursor read",
                deadline,
                cancellation_policy,
                || committer.committed_cursor(namespace),
            )
            .await
        }
        None => Ok(None),
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
    // open transaction. Weaker sinks must first land every queued write. Always call connector
    // close even when flush fails so resources are not leaked.
    let cancellation_policy = inner.sink.cancellation_policy();
    let flush_result = if inner.contract.is_checkpoint_committable() {
        Ok(())
    } else {
        bounded_connector_operation(
            &inner.name,
            "shutdown flush",
            deadline,
            cancellation_policy,
            || inner.sink.flush(),
        )
        .await
    };
    // A cancellation-unsafe flush may legitimately finish after the command's
    // protocol deadline. Connector teardown is still mandatory and receives a
    // fresh terminal budget rather than inheriting an already-expired instant.
    let close_result = bounded_connector_operation(
        &inner.name,
        "connector close",
        operation_deadline(SINK_CLOSE_TIMEOUT),
        cancellation_policy,
        || inner.sink.close(),
    )
    .await;
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

/// Write a batch before the enqueue-time deadline; reports every error and poisons replay-required
/// modes so their durable cut cannot advance.
async fn handle_write_batch(
    inner: &mut SinkTaskInner,
    batch: RecordBatch,
    deadline: Instant,
    current_epoch: u64,
    epoch_poisoned: &AtomicBool,
) {
    let rows = batch.num_rows();
    if deadline <= Instant::now() {
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
        return;
    }
    let cancellation_policy = inner.sink.cancellation_policy();
    let write_result = {
        let mut write = std::pin::pin!(inner.sink.write_batch(&batch));
        match tokio::time::timeout_at(deadline, write.as_mut()).await {
            Ok(result) => Some((result, false)),
            Err(_elapsed)
                if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted =>
            {
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
                Some((write.await, true))
            }
            Err(_elapsed) => None,
        }
    };
    let Some((write_result, timed_out)) = write_result else {
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
        return;
    };
    if timed_out {
        match write_result {
            Ok(_) => tracing::warn!(
                sink = %inner.name,
                rows,
                "sink write completed after its deadline"
            ),
            Err(error) => tracing::warn!(
                sink = %inner.name,
                rows,
                %error,
                "sink write failed after its deadline"
            ),
        }
        return;
    }
    match write_result {
        Ok(_) => {}
        Err(e) => {
            if inner.requires_recovery_on_error {
                epoch_poisoned.store(true, Ordering::Release);
            }
            tracing::warn!(
                sink = %inner.name, error = %e, rows,
                requires_recovery = inner.requires_recovery_on_error,
                "Sink write error"
            );
            let _ = inner.event_tx.try_push(SinkEvent::WriteError {
                sink_id: Arc::clone(&inner.sink_id),
                epoch: current_epoch,
                rows,
                error: e.to_string(),
            });
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
) -> Result<(), ConnectorError> {
    let result = bounded_connector_operation(
        &inner.name,
        "rollback_epoch",
        deadline,
        inner.sink.cancellation_policy(),
        || inner.sink.rollback_epoch(epoch),
    )
    .await;
    if let Err(ref e) = result {
        tracing::warn!(
            sink = %inner.name, epoch, error = %e,
            "[LDB-6004] Sink rollback failed"
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_connectors::connector::{
        SinkConsistency, SinkInputMode, SinkTopology, WriteResult,
    };
    use laminar_core::streaming::AsyncConsumer;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Minimal mock sink for testing the task infrastructure.
    struct CountingSink {
        writes: Arc<AtomicU64>,
        flushes: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
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
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CompleteStarted
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
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CompleteStarted
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

    struct CompleteStartedSink {
        schema: arrow::datatypes::SchemaRef,
        completed: Arc<AtomicBool>,
        flushes: Arc<AtomicU64>,
        closed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl SinkConnector for CompleteStartedSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            ConnectorCancellationPolicy::CompleteStarted
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
    async fn cancellation_unsafe_write_is_finished_after_timeout_before_actor_reuse() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let completed = Arc::new(AtomicBool::new(false));
        let flushes = Arc::new(AtomicU64::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let sink = CompleteStartedSink {
            schema,
            completed: Arc::clone(&completed),
            flushes: Arc::clone(&flushes),
            closed: Arc::clone(&closed),
        };
        let (handle, events) = spawn_with_defaults(
            "complete-started",
            Box::new(sink),
            Duration::from_millis(50),
        );

        handle.write_batch(test_batch()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            events.try_recv(),
            Ok(SinkEvent::WriteTimeout { sink_id, .. }) if &*sink_id == "complete-started"
        ));
        assert!(!completed.load(Ordering::Acquire));

        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert!(completed.load(Ordering::Acquire));
        handle.close().await.unwrap();
        assert_eq!(
            flushes.load(Ordering::Acquire),
            2,
            "the overdue periodic flush and terminal close flush must both complete"
        );
        assert!(closed.load(Ordering::Acquire));
    }

    #[tokio::test(start_paused = true)]
    async fn close_timeout_retains_complete_started_write_until_a_terminal_retry() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let completed = Arc::new(AtomicBool::new(false));
        let flushes = Arc::new(AtomicU64::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let sink = CompleteStartedSink {
            schema,
            completed: Arc::clone(&completed),
            flushes: Arc::clone(&flushes),
            closed: Arc::clone(&closed),
        };
        let (handle, _events) = spawn_with_defaults(
            "complete-started-close",
            Box::new(sink),
            Duration::from_millis(50),
        );

        handle.write_batch(test_batch()).await.unwrap();
        tokio::task::yield_now().await;
        let error = handle
            .close()
            .await
            .expect_err("outer close budget must expire while the write completes");
        assert!(error.to_string().contains("acknowledgement"), "{error}");
        assert!(!completed.load(Ordering::Acquire));
        assert!(!closed.load(Ordering::Acquire));

        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert!(completed.load(Ordering::Acquire));
        assert!(
            closed.load(Ordering::Acquire),
            "connector close was skipped after the original command deadline expired"
        );
        let flush_count = flushes.load(Ordering::Acquire);
        assert!(
            (1..=2).contains(&flush_count),
            "late buffered writes require a terminal flush and may first observe one overdue periodic flush; got {flush_count}"
        );
        handle
            .close()
            .await
            .expect("terminal retry must join the retained task");
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
        let handle = SinkTaskHandle {
            name: Arc::from("saturated"),
            sink_id: Arc::from("saturated"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout,
            cancellation_policy: ConnectorCancellationPolicy::CancelSafe,
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(tokio::spawn(async {})))),
            close_state: Arc::new(SinkCloseState::new()),
            runtime: tokio::runtime::Handle::current(),
            event_tx,
            epoch_poisoned: Arc::clone(&epoch_poisoned),
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
        let task = tokio::task::spawn_blocking(move || {
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

        let handle = SinkTaskHandle {
            name: Arc::from("uncooperative-cancel-safe"),
            sink_id: Arc::from("uncooperative-cancel-safe"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout: Duration::from_secs(1),
            cancellation_policy: ConnectorCancellationPolicy::CancelSafe,
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(task))),
            close_state: Arc::new(SinkCloseState::new()),
            runtime: tokio::runtime::Handle::current(),
            event_tx,
            epoch_poisoned: Arc::new(AtomicBool::new(false)),
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
        assert!(!handle.has_unresolved_task());
        drop(rx);
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
        let handle = SinkTaskHandle {
            name: Arc::from("dead"),
            sink_id: Arc::from("dead"),
            tx,
            contract: at_least_once_contract(),
            requires_recovery_on_error: true,
            write_timeout: Duration::from_secs(5),
            cancellation_policy: ConnectorCancellationPolicy::CancelSafe,
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(tokio::sync::Mutex::new(())),
            task: Arc::new(parking_lot::Mutex::new(Some(tokio::spawn(async {})))),
            close_state: Arc::new(SinkCloseState::new()),
            runtime: tokio::runtime::Handle::current(),
            event_tx,
            epoch_poisoned: Arc::new(AtomicBool::new(false)),
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
