//! Sink command execution: the command dispatch loop, epoch begin/rollback
//! transitions, flush/pre-commit/commit paths, write-batch handling, and
//! error/timeout publication.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use crossfire::oneshot;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, CoordinatedAbortBatch, CoordinatedCommitBatch,
    CoordinatedCommitContext, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitter,
};
use laminar_connectors::error::ConnectorError;
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;
use laminar_core::streaming::Producer;
use tokio::time::Instant;

use super::actor::SinkActorState;
use super::lifecycle::{SinkActorEpochState, SinkTaskInner};
#[cfg(feature = "cluster")]
use super::operation::process_authority_error;
use super::operation::{
    await_connector_operation, bounded_connector_operation, protocol_deadline_error,
    ConnectorOperationOutcome,
};
use super::protocol::{SinkEpochAdmission, SinkEvent, SinkOperation};

/// Returns `true` when the task should stop.
///
/// INVARIANT: an operation that retires the actor stops admission before its
/// acknowledgement is delivered, so no further command can be admitted behind
/// a terminal transition.
pub(super) async fn handle_sink_command(
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
            match write_batch_gate_error(inner, epoch_state, epoch) {
                Some(error) => {
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
                None => {
                    retire =
                        handle_write_batch(inner, batch, deadline, current_epoch, epoch_poisoned)
                            .await;
                }
            }
        }
        SinkOperation::BeginEpoch { epoch, ack } => {
            let (result, operation_retired) =
                begin_sink_epoch(inner, epoch, deadline, epoch_state, epoch_poisoned).await;
            retire = finish_command(ack, result, operation_retired, actor_state);
        }
        SinkOperation::Flush { ack } => {
            let (result, operation_retired) =
                flush_checkpoint_sink(inner, deadline, epoch_state.epoch(), epoch_poisoned).await;
            retire = finish_command(ack, result, operation_retired, actor_state);
        }
        SinkOperation::PreCommit { epoch, ack } => {
            let transition = pre_commit_epoch_transition(inner, epoch_state, epoch);
            let (result, operation_retired) = match transition {
                Ok(()) => pre_commit_sink(inner, epoch, deadline, epoch_poisoned).await,
                Err(error) => (Err(error), false),
            };
            retire = finish_command(ack, result, operation_retired, actor_state);
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
            retire = finish_command(ack, result, operation_retired, actor_state);
        }
        SinkOperation::CleanupAborted { batch, ack } => {
            let cancellation_policy = inner.sink.cancellation_policy();
            let committer = inner.sink.as_coordinated_committer();
            let (result, operation_retired) = cleanup_aborted_sink(
                &inner.name,
                committer,
                batch,
                deadline,
                cancellation_policy,
                #[cfg(feature = "cluster")]
                inner.process_authority.clone(),
            )
            .await;
            retire = finish_command(ack, result, operation_retired, actor_state);
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
            retire = finish_command(ack, result, operation_retired, actor_state);
        }
        SinkOperation::RollbackEpoch { epoch, ack } => {
            let (result, operation_retired) = handle_rollback_epoch(inner, epoch, deadline).await;
            retire = finish_command(ack, result, operation_retired, actor_state);
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

/// Deliver an acknowledgement; a retired operation stops admission first so
/// no command is admitted behind the terminal transition.
fn finish_command<T>(
    ack: oneshot::TxOneshot<Result<T, ConnectorError>>,
    result: Result<T, ConnectorError>,
    retired: bool,
    actor_state: &SinkActorState,
) -> bool {
    if retired {
        actor_state.stop_admission();
    }
    ack.send(result);
    retired
}

/// Epoch-gate check for one write on a checkpoint-committable sink; `None`
/// admits the write. Non-committable sinks do not participate in gating.
fn write_batch_gate_error(
    inner: &SinkTaskInner,
    epoch_state: &SinkActorEpochState,
    epoch: Option<SinkEpochAdmission>,
) -> Option<ConnectorError> {
    if !inner.contract.is_checkpoint_committable() {
        return None;
    }
    let current = epoch_state.epoch();
    match (*epoch_state, epoch) {
        (SinkActorEpochState::Open(open), Some(admitted)) if open == admitted.epoch => None,
        (state, _) => Some(ConnectorError::InvalidState {
            expected: format!("open sink epoch {current}"),
            actual: format!(
                "sink '{}' actor is {state:?} for write admitted as {epoch:?}",
                inner.name
            ),
        }),
    }
}

/// Phase-one gate: transitions an open epoch to Prepared, or explains why the
/// pre-commit cannot start. A failed pre-commit remains Prepared until
/// rollback begins a successor epoch — no queued or private write may cross
/// phase one.
fn pre_commit_epoch_transition(
    inner: &SinkTaskInner,
    epoch_state: &mut SinkActorEpochState,
    epoch: u64,
) -> Result<(), ConnectorError> {
    if !inner.contract.is_checkpoint_committable() {
        return Ok(());
    }
    match *epoch_state {
        SinkActorEpochState::Open(open) if open == epoch => {
            *epoch_state = SinkActorEpochState::Prepared(epoch);
            Ok(())
        }
        state => Err(ConnectorError::InvalidState {
            expected: format!("open sink epoch {epoch}"),
            actual: format!("sink '{}' actor is {state:?}", inner.name),
        }),
    }
}

pub(super) async fn begin_sink_epoch(
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

pub(super) async fn flush_checkpoint_sink(
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

pub(super) async fn pre_commit_sink(
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

pub(super) async fn commit_aggregated_sink(
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

pub(super) async fn cleanup_aborted_sink(
    sink_name: &str,
    committer: Option<&dyn CoordinatedCommitter>,
    batch: CoordinatedAbortBatch,
    deadline: Instant,
    cancellation_policy: ConnectorCancellationPolicy,
    #[cfg(feature = "cluster")] process_authority: Option<Arc<ClusterController>>,
) -> (Result<(), ConnectorError>, bool) {
    match committer {
        Some(committer) => {
            let context = CoordinatedCommitContext::new(deadline);
            bounded_connector_operation(
                sink_name,
                "aborted coordinated artifact cleanup",
                deadline,
                cancellation_policy,
                #[cfg(feature = "cluster")]
                process_authority,
                || committer.cleanup_aborted(batch, context),
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

pub(super) async fn committed_cursor(
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

pub(super) fn validate_sync_deadline(
    sink_name: &str,
    deadline: Instant,
) -> Result<(), ConnectorError> {
    if deadline <= Instant::now() {
        Err(protocol_deadline_error(sink_name, "sync"))
    } else {
        Ok(())
    }
}

pub(super) async fn close_sink_connector(
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

pub(super) fn poisoned_epoch_error(sink_name: &str) -> ConnectorError {
    ConnectorError::WriteError(format!(
        "sink '{sink_name}' epoch poisoned by a prior dropped write"
    ))
}

pub(super) fn record_flush_error(
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

pub(super) fn record_write_error(
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
pub(super) async fn handle_write_batch(
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

pub(super) fn record_write_timeout(
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
pub(super) async fn handle_rollback_epoch(
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
