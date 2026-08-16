//! Mechanically extracted coordinator responsibility.

#[cfg(feature = "cluster")]
use super::{
    check_source_sync_fence, source_operation_authority_error, ActiveSourceDrain,
    PendingSourceDrainResolution, SourceDrainCommand, SourceDrainCommandPolicy,
    SourceDrainLeaseControl, SourceDrainOutcome, SourceDrainReceipt, SourceDrainTaskStatus,
    SourceProcessAuthority,
};
use super::{
    run_source_operation, source_operation_deadline_error, Arc, ConnectorCancellationPolicy,
    ConnectorError, DeliveryGuarantee, SourceBatch, SourceBatchCursor, SourceCheckpoint,
    SourceConnector, SourceConnectorLifecycle, SourceFault, SourceOperationOutcome,
};

pub(super) fn try_source_checkpoint(
    connector: &dyn SourceConnector,
    assignment_scoped: bool,
) -> Result<Option<SourceCheckpoint>, ConnectorError> {
    let checkpoint = connector.try_checkpoint()?;
    let Some(captured) = checkpoint.as_ref() else {
        return Ok(None);
    };
    validate_source_checkpoint_scope(captured, assignment_scoped)?;
    Ok(checkpoint)
}

pub(super) fn validate_source_checkpoint_scope(
    checkpoint: &SourceCheckpoint,
    assignment_scoped: bool,
) -> Result<(), ConnectorError> {
    match (assignment_scoped, checkpoint.assignment_version()) {
        (true, None) => Err(ConnectorError::Internal(
            "cluster-assigned source checkpoint is missing its assignment version".into(),
        )),
        (false, Some(version)) => Err(ConnectorError::Internal(format!(
            "local source checkpoint unexpectedly carries cluster assignment version {version}"
        ))),
        _ => Ok(()),
    }
}

pub(super) fn take_assignment_bound_batch_cursor(
    batch: &mut SourceBatch,
    assignment_scoped: bool,
) -> Result<Option<SourceBatchCursor>, ConnectorError> {
    if !assignment_scoped {
        return Ok(None);
    }
    let cursor = batch.take_cursor().ok_or_else(|| {
        ConnectorError::Internal(
            "cluster-assigned source batch is missing its assignment-bound checkpoint".into(),
        )
    })?;
    if let SourceBatchCursor::Complete(checkpoint) = &cursor {
        validate_source_checkpoint_scope(checkpoint, true)?;
        if checkpoint.input_channels().is_none() {
            return Err(ConnectorError::Internal(
                "cluster-assigned source batch checkpoint is missing its input-channel inventory"
                    .into(),
            ));
        }
    }
    Ok(Some(cursor))
}

/// Apply the newest durable commit notification while no source poll borrows
/// the connector. Non-best-effort pipelines fault if upstream acknowledgement
/// fails because silently continuing can exhaust upstream retention or acknowledgement headroom.
pub(super) async fn acknowledge_latest_source_commit(
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
pub(super) async fn apply_latest_source_drain_command_fenced(
    connector: &mut dyn SourceConnector,
    command_rx: &mut tokio::sync::watch::Receiver<Option<SourceDrainCommand>>,
    status_tx: &tokio::sync::watch::Sender<SourceDrainTaskStatus>,
    active: &mut Option<ActiveSourceDrain>,
    policy: SourceDrainCommandPolicy,
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
    if matches!(policy, SourceDrainCommandPolicy::ResolveOnly)
        && matches!(&command, SourceDrainCommand::Begin { .. })
    {
        // A provider Begin may need normal source polling to reach its FIFO cut, which this
        // earlier barrier forbids. Re-mark the captured generation changed: if Resolve already
        // overwrote it, that newer generation remains changed; otherwise this Begin remains
        // pending for the normal loop after barrier release.
        command_rx.mark_changed();
        return Ok(());
    }
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
pub(super) async fn resolve_pending_source_drain_fenced(
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
pub(super) async fn apply_latest_source_drain_command(
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
        SourceDrainCommandPolicy::Any,
        provider_drain,
        "test-source",
        ConnectorCancellationPolicy::RetireConnector,
        &mut lifecycle,
        None,
    )
    .await
}

#[cfg(all(test, feature = "cluster"))]
pub(super) async fn resolve_pending_source_drain(
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
pub(super) fn publish_source_drain_ready_fenced(
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
pub(super) fn publish_source_drain_ready(
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
pub(super) fn source_drain_flushing(active: Option<&ActiveSourceDrain>) -> bool {
    active.is_some_and(|drain| !drain.ready)
}

#[cfg(feature = "cluster")]
pub(super) fn source_drain_held(active: Option<&ActiveSourceDrain>) -> bool {
    active.is_some_and(|drain| drain.ready)
}
