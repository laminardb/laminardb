//! Sink actor run loops: local and process-fenced (cluster) loops, periodic
//! flush, disconnected close, and authority-loss termination.

use std::sync::atomic::AtomicBool;
#[cfg(feature = "cluster")]
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;

use laminar_connectors::connector::{SinkConnector, SinkContract};
use laminar_core::streaming::Producer;

use super::actor::SinkActorState;
#[cfg(feature = "cluster")]
use super::commands::record_write_error;
use super::commands::{handle_sink_command, record_flush_error};
#[cfg(feature = "cluster")]
use super::operation::process_authority_error;
use super::operation::{bounded_connector_operation, operation_deadline};
#[cfg(feature = "cluster")]
use super::protocol::SinkOperation;
use super::protocol::SINK_CLOSE_TIMEOUT;
use super::protocol::{SinkCommandRx, SinkEvent};

pub(super) struct SinkTaskInner {
    pub(super) name: String,
    pub(super) sink_id: Arc<str>,
    pub(super) sink: Box<dyn SinkConnector>,
    pub(super) rx: SinkCommandRx,
    pub(super) flush_interval: Duration,
    pub(super) write_timeout: Duration,
    /// Checkpoint-committable writers may only flush inside checkpoint protocol commands.
    pub(super) contract: SinkContract,
    pub(super) requires_recovery_on_error: bool,
    pub(super) event_tx: Producer<SinkEvent>,
    #[cfg(feature = "cluster")]
    pub(super) process_authority: Option<Arc<ClusterController>>,
    #[cfg(feature = "cluster")]
    pub(super) admission: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SinkActorEpochState {
    Open(u64),
    Prepared(u64),
}

impl SinkActorEpochState {
    pub(super) fn epoch(self) -> u64 {
        match self {
            Self::Open(epoch) | Self::Prepared(epoch) => epoch,
        }
    }
}

// In replay-required modes, `epoch_poisoned` rejects checkpoint Flush/PreCommit so no durable cut
// can pass a dropped write. Local best-effort mode reports loss without permanently fencing state.
pub(super) async fn run_sink_task(
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

pub(super) async fn run_local_sink_task(
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
pub(super) async fn run_process_fenced_sink_task(
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
                if !controller.process_lease_is_live() {
                    terminate_after_process_authority_loss(
                        &mut inner,
                        epoch_state.epoch(),
                        epoch_poisoned.as_ref(),
                        None,
                    ).await;
                    return;
                }
                if retire {
                    actor_state.stop_admission();
                    return;
                }
            }
        }
    }
}

pub(super) async fn flush_sink_periodically(
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

pub(super) async fn close_disconnected_sink(inner: &mut SinkTaskInner) {
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
pub(super) async fn terminate_after_process_authority_loss(
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
pub(super) fn reject_unstarted_sink_operation(
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
        SinkOperation::ArtifactIntent { ack, .. } => {
            ack.send(Err(process_authority_error(
                &inner.name,
                "checkpoint artifact intent",
            )));
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
