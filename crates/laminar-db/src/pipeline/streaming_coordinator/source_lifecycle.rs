//! Mechanically extracted coordinator responsibility.

use super::{
    acknowledge_latest_source_commit, Arc, CheckpointAttempt, CheckpointAttemptRelation,
    CheckpointBarrier, ConnectorCancellationPolicy, ConnectorError, DeliveryGuarantee, Duration,
    SourceBarrierSignal, SourceBatch, SourceCheckpoint, SourceConnector, SourceFault, SourceStart,
};
#[cfg(feature = "cluster")]
use super::{
    apply_latest_source_drain_command_fenced, publish_source_drain_ready_fenced,
    resolve_pending_source_drain_fenced, source_process_authority_is_live, ActiveSourceDrain,
    SourceDrainCommand, SourceDrainCommandPolicy, SourceDrainLeaseControl, SourceProcessAuthority,
};

// Keep `SourceBatch` inline: boxing every successful poll would allocate on the source hot path.
#[allow(clippy::large_enum_variant)]
pub(super) enum SourcePollOutcome {
    Completed(Result<Option<SourceBatch>, ConnectorError>),
    Deadline,
    Shutdown,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

pub(super) enum SourceOperationOutcome<T> {
    Completed(T),
    Deadline,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

#[derive(Default)]
pub(super) struct SourceConnectorLifecycle {
    pub(super) retired: bool,
    data_plane_faulted: bool,
    #[cfg(feature = "cluster")]
    pub(super) process_authority_lost: bool,
}

impl SourceConnectorLifecycle {
    pub(super) fn cancelled(&mut self, cancellation_policy: ConnectorCancellationPolicy) {
        if cancellation_policy == ConnectorCancellationPolicy::RetireConnector {
            self.retired = true;
        }
    }

    #[cfg(feature = "cluster")]
    pub(super) fn authority_lost(&mut self) {
        self.process_authority_lost = true;
    }

    #[cfg(feature = "cluster")]
    pub(super) fn process_authority_lost(&self) -> bool {
        self.process_authority_lost
    }

    pub(super) fn may_invoke_connector(&self) -> bool {
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

    pub(super) fn fault_data_plane(&mut self) {
        self.data_plane_faulted = true;
    }

    pub(super) fn report_fault(
        &mut self,
        fault_tx: &tokio::sync::mpsc::UnboundedSender<SourceFault>,
        source: &str,
        error: &impl std::fmt::Display,
    ) {
        self.fault_data_plane();
        let _ = fault_tx.send(SourceFault {
            source: Arc::from(source),
            error: error.to_string(),
        });
    }

    pub(super) fn may_poll_or_ack(&self) -> bool {
        self.may_invoke_connector() && !self.data_plane_faulted
    }

    pub(super) fn run_sync_hook<T>(
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
pub(super) async fn run_source_operation<T, F, Fut>(
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

pub(super) fn source_operation_deadline_error(source: &str, operation: &str) -> ConnectorError {
    ConnectorError::Internal(format!(
        "source '{source}' {operation} exceeded its end-to-end deadline"
    ))
}

pub(super) fn check_source_sync_fence(
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
pub(super) fn source_operation_authority_error(source: &str, operation: &str) -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "live cluster process lease".into(),
        actual: format!("source '{source}' lost process authority during {operation}"),
    }
}

pub(super) enum SourceStartOutcome {
    Completed(Result<(), ConnectorError>),
    TimedOut,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

pub(super) enum SourceStartFailure {
    Connector(String),
    Retired(String),
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost(String),
}

pub(super) async fn start_source_once(
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

pub(super) async fn poll_source_once(
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
pub(super) async fn wait_source_idle(
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
pub(super) async fn wait_source_drain_hold(
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

pub(super) fn source_barrier_release_covers(
    released: CheckpointAttempt,
    held: CheckpointAttempt,
) -> bool {
    matches!(
        released.relation_to(held),
        CheckpointAttemptRelation::Exact | CheckpointAttemptRelation::Newer
    )
}

#[cfg(feature = "cluster")]
pub(super) async fn service_source_drain_resolution_during_barrier_hold(
    connector: &mut dyn SourceConnector,
    command_rx: &mut tokio::sync::watch::Receiver<Option<SourceDrainCommand>>,
    control: &SourceDrainLeaseControl,
    active: &mut Option<ActiveSourceDrain>,
    provider_drain: bool,
    source_name: &str,
    cancellation_policy: ConnectorCancellationPolicy,
    lifecycle: &mut SourceConnectorLifecycle,
    process_authority: Option<&SourceProcessAuthority>,
) -> Result<(), ConnectorError> {
    apply_latest_source_drain_command_fenced(
        connector,
        command_rx,
        &control.status_tx,
        active,
        SourceDrainCommandPolicy::ResolveOnly,
        provider_drain,
        source_name,
        cancellation_policy,
        lifecycle,
        process_authority,
    )
    .await?;
    publish_source_drain_ready_fenced(
        connector,
        control,
        active,
        source_name,
        cancellation_policy,
        lifecycle,
        process_authority,
    )?;
    resolve_pending_source_drain_fenced(
        connector,
        &control.status_tx,
        active,
        source_name,
        cancellation_policy,
        lifecycle,
        process_authority,
    )
    .await
}

/// Hold a source at an emitted barrier until the coordinator releases that exact attempt.
///
/// The retained watch value closes the release-before-wait race. While held, the source keeps its
/// connector control plane, source-drain control, and durable upstream acknowledgements live, but
/// never polls data.
pub(super) async fn wait_source_barrier_release(
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
    #[cfg(feature = "cluster")] drain_control: Option<&SourceDrainLeaseControl>,
    #[cfg(feature = "cluster")] mut drain_command_rx: Option<
        &mut tokio::sync::watch::Receiver<Option<SourceDrainCommand>>,
    >,
    #[cfg(feature = "cluster")] active_source_drain: &mut Option<ActiveSourceDrain>,
    #[cfg(feature = "cluster")] provider_drain: bool,
    #[cfg(feature = "cluster")] process_authority: Option<&SourceProcessAuthority>,
    poll_interval: Duration,
    barrier: CheckpointBarrier,
) -> bool {
    let attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
    #[cfg(feature = "cluster")]
    let drain_wake = drain_control.map(|control| control.wake.as_ref());
    #[cfg(not(feature = "cluster"))]
    let drain_wake: Option<&tokio::sync::Notify> = None;
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

        #[cfg(feature = "cluster")]
        if let (Some(control), Some(command_rx)) = (drain_control, drain_command_rx.as_deref_mut())
        {
            if let Err(error) = service_source_drain_resolution_during_barrier_hold(
                connector,
                command_rx,
                control,
                active_source_drain,
                provider_drain,
                src_name,
                cancellation_policy,
                lifecycle,
                process_authority,
            )
            .await
            {
                if !lifecycle.process_authority_lost() {
                    lifecycle.fault_data_plane();
                    let _ = fault_tx.send(SourceFault {
                        source: Arc::from(src_name),
                        error: error.to_string(),
                    });
                }
                return false;
            }
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
                () = async move {
                    match drain_wake {
                        Some(notify) => notify.notified().await,
                        None => std::future::pending().await,
                    }
                } => {},
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
            () = async move {
                match drain_wake {
                    Some(notify) => notify.notified().await,
                    None => std::future::pending().await,
                }
            } => {},
            () = tokio::time::sleep(poll_interval) => {}
        }
    }
}
