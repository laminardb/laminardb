//! The public sink task handle: spawn, command submission, epoch admission,
//! checkpoint participation, and shutdown entry points.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "cluster")]
use super::operation::process_authority_error;
use arrow::array::RecordBatch;
use crossfire::{mpsc, oneshot, SendTimeoutError};
use laminar_connectors::connector::CoordinatedCommitBatch;
use laminar_connectors::error::ConnectorError;
use laminar_core::streaming::Producer;
use tokio::time::Instant;

#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;

use super::actor::{
    spawn_sink_actor, supervise_sink_task, OwnedSinkTask, SinkActorState, SinkCloseOutcome,
    SinkCloseState, SinkTerminalState,
};
use super::close::{spawn_sink_close_driver, wait_for_sink_close};
use super::commands::poisoned_epoch_error;
use super::lifecycle::{run_sink_task, SinkTaskInner};
use super::operation::{close_deadline_error, command_deadline_error, operation_deadline};
use super::protocol::{
    CoordinatedCommitCursor, CoordinatedCommitNamespace, SinkBeginGateGuard, SinkCommand,
    SinkCommandTx, SinkContract, SinkEpochAdmission, SinkEpochGateState, SinkEvent, SinkOperation,
    SinkTaskConfig, SINK_CLOSE_TIMEOUT,
};

#[derive(Clone)]
pub(crate) struct SinkTaskHandle {
    pub(super) name: Arc<str>,
    pub(super) sink_id: Arc<str>,
    pub(super) tx: SinkCommandTx,
    pub(super) contract: SinkContract,
    pub(super) requires_recovery_on_error: bool,
    /// End-to-end budget for enqueue, connector execution and acknowledgement.
    pub(super) write_timeout: Duration,
    pub(super) closing: Arc<AtomicBool>,
    /// Linearizes command admission with Close so no producer can enqueue behind it.
    pub(super) admission: Arc<tokio::sync::Mutex<()>>,
    // The terminal driver takes this exactly once. Public close futures never own the actor or
    // its connector-child termination proof.
    pub(super) task: Arc<parking_lot::Mutex<Option<OwnedSinkTask>>>,
    pub(super) close_state: Arc<SinkCloseState>,
    pub(super) terminal_state: Arc<SinkTerminalState>,
    pub(super) actor_state: Arc<SinkActorState>,
    /// Runtime that owns the actor. Terminal cleanup must not be spawned on the short-lived
    /// compute callback runtime that happened to call `close()`.
    pub(super) runtime: tokio::runtime::Handle,
    pub(super) event_tx: Producer<SinkEvent>,
    /// Sticky for the current epoch. Shared with the actor so a write rejected before enqueue
    /// cannot be hidden from the checkpoint protocol.
    pub(super) epoch_poisoned: Arc<AtomicBool>,
    /// Checkpoint-committable sinks remain non-writable until the whole sink group has begun the
    /// same allocator-owned epoch. Every clone observes the same transition stream.
    pub(super) epoch_gate: Option<tokio::sync::watch::Sender<SinkEpochGateState>>,
    #[cfg(feature = "cluster")]
    pub(super) process_authority: Option<Arc<ClusterController>>,
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

    pub(crate) async fn wait_for_open_epoch_until(
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
