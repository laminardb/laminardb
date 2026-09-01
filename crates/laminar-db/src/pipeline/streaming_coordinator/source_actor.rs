//! Source actor construction and the allocation-free polling protocol.

use super::{
    acknowledge_latest_source_commit, poll_source_once, prepare_encoded_source_batch,
    run_source_operation, send_source_msg, source_operation_deadline_error, spawn_source_actor,
    take_batch_cursor, try_source_checkpoint, wait_source_barrier_release, wait_source_idle, Arc,
    AtomicBool, CheckpointBarrierInjector, OwnedSourceTasks, PipelineConfig,
    PreparedSourceGeneration, RecordBatch, SourceBarrierSignal, SourceBatchCursor,
    SourceCheckpoint, SourceCheckpointUnavailablePolicy, SourceConnectorLifecycle, SourceFault,
    SourceHandle, SourceInputMode, SourceMsg, SourceMsgTx, SourceOperationOutcome,
    SourcePollOutcome, SourceTaskExitGuard, SourceTaskLease, TrackedSourceRegistration,
    SHUTDOWN_DRAIN_BUDGET,
};
#[cfg(feature = "cluster")]
use super::{
    apply_latest_source_drain_command_fenced, publish_source_drain_ready_fenced,
    resolve_pending_source_drain_fenced, source_drain_flushing, source_drain_held,
    source_process_authority_is_live, wait_source_drain_hold, ActiveSourceDrain,
    SourceDrainCommand, SourceDrainCommandPolicy, SourceDrainLeaseControl, SourceDrainTaskStatus,
    SourceProcessAuthority,
};

pub(super) struct SpawnedSource {
    pub(super) handle: SourceHandle,
    pub(super) name: Arc<str>,
    pub(super) input_mode: Option<SourceInputMode>,
}

pub(super) struct SourceActorSpawner<'a> {
    pub(super) tx: &'a SourceMsgTx,
    pub(super) source_fault_tx: &'a tokio::sync::mpsc::UnboundedSender<SourceFault>,
    pub(super) source_gate: &'a Arc<AtomicBool>,
    pub(super) config: &'a PipelineConfig,
    pub(super) runtime: &'a tokio::runtime::Handle,
    pub(super) owned_source_tasks: &'a OwnedSourceTasks,
    #[cfg(feature = "cluster")]
    pub(super) source_process_authority: Option<&'a Arc<SourceProcessAuthority>>,
    #[cfg(feature = "cluster")]
    pub(super) runtime_mode: crate::db::RuntimeMode,
}

impl SourceActorSpawner<'_> {
    // PERF: The biased control, barrier, poll, commit, and bounded shutdown paths deliberately
    // remain in one spawned future. Splitting an iteration into boxed callbacks or detached tasks
    // would add work to every source record and obscure the ordering fences.
    pub(super) fn spawn(&self, idx: usize, prepared: PreparedSourceGeneration) -> SpawnedSource {
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
            admitted_non_append_mode,
            task_fence,
        } = registration;
        let terminal_tasks = task_fence.tracker();
        let task_shutdown = Arc::new(tokio::sync::Notify::new());
        let task_shutdown_clone = Arc::clone(&task_shutdown);
        let task_tx = self.tx.clone();
        let task_fault_tx = self.source_fault_tx.clone();
        let task_gate = Arc::clone(self.source_gate);
        #[cfg(feature = "cluster")]
        let task_process_authority = self.source_process_authority.cloned();
        let max_poll = self.config.max_poll_records;
        let poll_interval = self.config.fallback_poll_interval;
        let source_operation_timeout = self.config.checkpoint_timeout;
        let delivery_guarantee = self.config.delivery_guarantee;
        let src_name = src.name.clone();
        let recovery_cursor = contract.supports_replay();
        let assignment_scoped = src.assignment_scoped;
        let policy = src.connector.checkpoint_unavailable_policy();
        let cancellation_policy = src.connector.cancellation_policy();
        let mut connector = src.connector;

        #[cfg(feature = "cluster")]
        let drain_control = self.runtime_mode.is_cluster().then(|| {
            let (command_tx, _) = tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
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
        let (startup_activation_tx, startup_activation_rx) = crossfire::oneshot::oneshot::<()>();
        let expected_shutdown = Arc::new(AtomicBool::new(false));
        let task_expected_shutdown = Arc::clone(&expected_shutdown);

        let task_exit_guard = SourceTaskExitGuard {
            source: Arc::from(src_name.as_str()),
            expected_shutdown: task_expected_shutdown,
            fault_tx: task_fault_tx.clone(),
        };
        let (join, actor_terminal) = spawn_source_actor(self.runtime, async move {
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
                        SourceDrainCommandPolicy::Any,
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
                        if !source_process_authority_is_live(task_process_authority.as_deref()) {
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

                // Local connectors may defer cursor capture after polling. Assignment-scoped
                // connectors bind the exact ownership cut to the batch and never enter here.
                if let Some(batch) = pending_batch.take() {
                    match lifecycle.run_sync_hook(
                        &src_name,
                        "pending batch checkpoint capture",
                        control_deadline,
                        cancellation_policy,
                        #[cfg(feature = "cluster")]
                        task_process_authority.as_deref(),
                        || try_source_checkpoint(connector.as_ref(), false),
                    ) {
                        Ok(Some(checkpoint)) => {
                            let msg = SourceMsg::Batch {
                                source_idx: idx,
                                batch,
                                cursor: SourceBatchCursor::Complete(checkpoint),
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
                                    task_drain_control.as_ref(),
                                    #[cfg(feature = "cluster")]
                                    task_drain_command_rx.as_mut(),
                                    #[cfg(feature = "cluster")]
                                    &mut active_source_drain,
                                    #[cfg(feature = "cluster")]
                                    assignment_scoped,
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
                            Ok(None)
                                if policy
                                    == SourceCheckpointUnavailablePolicy::PollToReplayBoundary =>
                            {
                                pending_barrier = Some(barrier);
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
                        }
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
                    if let Some(barrier) = pending_barrier.take().or_else(|| barrier_handle.poll())
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
                                    task_drain_control.as_ref(),
                                    #[cfg(feature = "cluster")]
                                    task_drain_command_rx.as_mut(),
                                    #[cfg(feature = "cluster")]
                                    &mut active_source_drain,
                                    #[cfg(feature = "cluster")]
                                    assignment_scoped,
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
                    Ok(Some(mut input)) => {
                        let cursor = take_batch_cursor(&mut input, assignment_scoped, policy);
                        let bound_checkpoint = match cursor {
                            Ok(checkpoint) => checkpoint,
                            Err(error) => {
                                lifecycle.report_fault(&task_fault_tx, &src_name, &error);
                                break;
                            }
                        };
                        let batch = match prepare_encoded_source_batch(
                            &src_name,
                            &expected_schema,
                            &positioned_schema,
                            &mutation_schema,
                            &primary_key,
                            &primary_key_indices,
                            contract.row_positions,
                            input,
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
                        let cursor = if let Some(cursor) = bound_checkpoint {
                            cursor
                        } else {
                            let checkpoint = match lifecycle.run_sync_hook(
                                &src_name,
                                "polled batch checkpoint capture",
                                poll_deadline,
                                cancellation_policy,
                                #[cfg(feature = "cluster")]
                                task_process_authority.as_deref(),
                                || try_source_checkpoint(connector.as_ref(), false),
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
                            SourceBatchCursor::Complete(checkpoint)
                        };
                        let msg = SourceMsg::Batch {
                            source_idx: idx,
                            batch,
                            cursor,
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
                        let drain_became_ready = if let Some(control) = task_drain_control.as_ref()
                        {
                            let was_flushing = source_drain_flushing(active_source_drain.as_ref());
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
                    if let Some(barrier) = pending_barrier.take().or_else(|| barrier_handle.poll())
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
                                    task_drain_control.as_ref(),
                                    #[cfg(feature = "cluster")]
                                    task_drain_command_rx.as_mut(),
                                    #[cfg(feature = "cluster")]
                                    &mut active_source_drain,
                                    #[cfg(feature = "cluster")]
                                    assignment_scoped,
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
                        || try_source_checkpoint(connector.as_ref(), false),
                    ) {
                        Ok(Some(checkpoint)) => {
                            if task_tx
                                .try_send(SourceMsg::Batch {
                                    source_idx: idx,
                                    batch,
                                    cursor: SourceBatchCursor::Complete(checkpoint),
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
                        Some(Ok(Some(mut input))) => {
                            let cursor = take_batch_cursor(&mut input, assignment_scoped, policy);
                            let bound_checkpoint = match cursor {
                                Ok(checkpoint) => checkpoint,
                                Err(error) => {
                                    lifecycle.report_fault(&task_fault_tx, &src_name, &error);
                                    break;
                                }
                            };
                            let batch = match prepare_encoded_source_batch(
                                &src_name,
                                &expected_schema,
                                &positioned_schema,
                                &mutation_schema,
                                &primary_key,
                                &primary_key_indices,
                                contract.row_positions,
                                input,
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
                            let cursor = if let Some(cursor) = bound_checkpoint {
                                cursor
                            } else {
                                let checkpoint = match lifecycle.run_sync_hook(
                                    &src_name,
                                    "shutdown tail checkpoint capture",
                                    shutdown_deadline,
                                    cancellation_policy,
                                    #[cfg(feature = "cluster")]
                                    task_process_authority.as_deref(),
                                    || try_source_checkpoint(connector.as_ref(), false),
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
                                SourceBatchCursor::Complete(checkpoint)
                            };
                            let msg = SourceMsg::Batch {
                                source_idx: idx,
                                batch,
                                cursor,
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
            self.runtime,
        );
        #[cfg(feature = "cluster")]
        if let Some(control) = drain_control {
            task.install_drain_control(control);
        }
        self.owned_source_tasks.lock().push(task.clone());
        let handle = SourceHandle {
            recovery_cursor,
            task,
            startup_activation: Some(startup_activation_tx),
            barrier_injector,
            barrier_release_tx,
            epoch_committed_tx,
        };
        task_fence.handoff();
        SpawnedSource {
            handle,
            name: arc_name,
            input_mode: admitted_non_append_mode,
        }
    }
}
