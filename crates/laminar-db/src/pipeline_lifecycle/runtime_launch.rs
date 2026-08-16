#[cfg(feature = "cluster")]
use super::{
    checked_pipeline_deadline, latch_cluster_terminal_data_plane,
    publish_cluster_compute_fault_state, publish_cluster_terminal_compute_halt_state,
    queue_owned_cluster_compute_fault, report_cluster_compute_fault, report_cluster_terminal_halt,
    retire_cluster_compute_generation, PendingVnodeTransitionLaunchGuard, RuntimeMode,
    CLUSTER_COMPUTE_THREAD_STACK_BYTES,
};
use super::{
    publish_runtime_fault_state, recovered_source_watermark, resolve_stream_output_schemas,
    runtime_exit_is_covered_by_terminal_stop, spawn_supervised_restart, Arc, DbError, FxHashMap,
    HashMap, LaminarDB, OrderedIntervalAdmissions, PipelineRecoveryState, PipelineRuntimeSetup,
    PreparedPipelineRuntime, TemporalSourceRole,
};

impl LaminarDB {
    pub(super) async fn launch_pipeline_runtime(
        &self,
        setup: PipelineRuntimeSetup,
        shutdown: Arc<tokio::sync::Notify>,
        runtime_shutdown: tokio_util::sync::CancellationToken,
        #[cfg(feature = "cluster")] mut startup_generation_fence: Option<
            tokio::sync::OwnedRwLockWriteGuard<()>,
        >,
    ) -> Result<(), DbError> {
        let PipelineRuntimeSetup {
            sources,
            config: pipeline_config,
            callback,
            force_checkpoint_rx: force_ckpt_rx,
            checkpoint_complete_rx,
            checkpoint_in_flight,
            #[cfg(feature = "cluster")]
            source_process_authority,
            runtime_mode,
        } = setup;
        let (control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
        *self.control_tx.lock() = Some(control_tx);

        #[cfg(feature = "cluster")]
        let source_gate = Arc::clone(&self.source_gate);
        #[cfg(not(feature = "cluster"))]
        let source_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let coordinator = crate::pipeline::StreamingCoordinator::new_with_tracked_source_registry(
            sources,
            pipeline_config,
            Arc::clone(&shutdown),
            control_rx,
            source_gate,
            #[cfg(feature = "cluster")]
            source_process_authority,
            Arc::clone(&self.owned_source_tasks),
            runtime_mode,
        )
        .await?
        .with_terminal_shutdown(runtime_shutdown.clone())
        .with_force_checkpoint_rx(force_ckpt_rx)
        .with_checkpoint_complete_rx(checkpoint_complete_rx)
        .with_checkpoint_admission(checkpoint_in_flight);

        let (done_tx, done_rx) = crossfire::oneshot::oneshot::<crate::pipeline::ExitReason>();
        let (startup_tx, startup_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
        // Captured by the compute thread so an operator panic is recorded
        // (surfaced via pipeline status) rather than only logged.
        let fault_slot = Arc::clone(&self.last_fault);
        let fault_state = Arc::clone(&self.state);
        let fault_metrics = self.engine_metrics.lock().clone();
        #[cfg(feature = "cluster")]
        let compute_fault_source_gate = Arc::clone(&self.source_gate);
        #[cfg(feature = "cluster")]
        let compute_fault_recovery_fence = Arc::clone(&self.coordinated_recovery_fenced);
        #[cfg(feature = "cluster")]
        let compute_fault_authority_transition = Arc::clone(&self.cluster_authority_transition);
        let compute_terminal_pipeline_halt = Arc::clone(&self.terminal_pipeline_halt);
        #[cfg(feature = "cluster")]
        let compute_fault_is_cluster = runtime_mode == RuntimeMode::Cluster;
        #[cfg(feature = "cluster")]
        let compute_fault_installed_vnode_state = Arc::clone(&self.installed_vnode_state);
        #[cfg(feature = "cluster")]
        let compute_fault_pending_vnode_transition = Arc::clone(&self.pending_vnode_transition);
        #[cfg(feature = "cluster")]
        let compute_fault_rotation_execution_fence = Arc::clone(&self.rotation_execution_fence);
        #[cfg(feature = "cluster")]
        let compute_fault_controller = self.cluster_controller.lock().clone();
        #[cfg(feature = "cluster")]
        let compute_fault_pending = Arc::clone(&self.pending_recovery_fault);
        let compute_fault_runtime_shutdown = runtime_shutdown.clone();
        #[cfg(all(test, feature = "cluster"))]
        let compute_before_ready_panic = Arc::clone(&self.compute_before_ready_panic);
        let compute_thread = std::thread::Builder::new().name("laminar-compute".into());
        #[cfg(feature = "cluster")]
        // The local and clustered runtimes share the cluster-enabled coordinator state machine.
        // Windows' default thread stack is too small for that debug/test layout even when the
        // binary is running a local pipeline. Keep the allocation explicit and bounded: there is
        // one compute thread per running pipeline, and cluster I/O workers use the same 4 MiB.
        let compute_thread = compute_thread.stack_size(CLUSTER_COMPUTE_THREAD_STACK_BYTES);
        match compute_thread
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        startup_tx.send(Err(format!("compute runtime: {e}")));
                        return;
                    }
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    #[cfg(all(test, feature = "cluster"))]
                    assert!(
                        !compute_before_ready_panic
                            .swap(false, std::sync::atomic::Ordering::SeqCst),
                        "injected cluster compute panic before readiness"
                    );
                    rt.block_on(async move {
                        Box::pin(coordinator.run_with_ready(callback, startup_tx)).await
                    })
                }));
                // Runtime shutdown waits for non-abortable `spawn_blocking` filesystem work.
                // Publish neither clean completion nor a fault until those workers are gone;
                // otherwise lifecycle teardown could release the exact namespace lock while
                // an old local decision hard-link was still able to appear.
                drop(rt);
                let exit = match result {
                    Ok(exit) => exit,
                    Err(panic) => {
                        let msg = panic
                            .downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| panic.downcast_ref::<&str>().copied())
                            .unwrap_or("unknown");
                        tracing::error!(panic = msg, "laminar-compute thread panicked");
                        crate::pipeline::ExitReason::Fault(msg.to_string())
                    }
                };
                let exit = match exit {
                    crate::pipeline::ExitReason::Shutdown => {
                        crate::pipeline::ExitReason::Shutdown
                    }
                    crate::pipeline::ExitReason::Halt(reason) => {
                        #[cfg(feature = "cluster")]
                        if !compute_fault_is_cluster {
                            compute_terminal_pipeline_halt
                                .store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                        #[cfg(not(feature = "cluster"))]
                        compute_terminal_pipeline_halt
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                        #[cfg(feature = "cluster")]
                        if compute_fault_is_cluster {
                            // The local disposition must precede request allocation so a monitor
                            // that observes the retained ordinal can only publish it as Terminal.
                            // The state helper idempotently repeats this fence before retirement.
                            latch_cluster_terminal_data_plane(
                                &compute_fault_authority_transition,
                                &compute_terminal_pipeline_halt,
                                &compute_fault_source_gate,
                                &compute_fault_recovery_fence,
                            );
                            if let Some(controller) = compute_fault_controller.as_deref() {
                                if let Err(error) = crate::coordinated_recovery::queue_local_fault(
                                    controller,
                                    &compute_fault_pending,
                                ) {
                                    tracing::error!(
                                        %error,
                                        "could not allocate terminal pipeline fault request"
                                    );
                                }
                            }
                        }
                        #[cfg(feature = "cluster")]
                        let owns_fault_state = if compute_fault_is_cluster {
                            publish_cluster_terminal_compute_halt_state(
                                &fault_state,
                                &compute_fault_authority_transition,
                                &compute_terminal_pipeline_halt,
                                &compute_fault_source_gate,
                                &compute_fault_recovery_fence,
                                &compute_fault_rotation_execution_fence,
                                &compute_fault_pending_vnode_transition,
                                &compute_fault_installed_vnode_state,
                            )
                        } else {
                            publish_runtime_fault_state(&fault_state)
                        };
                        #[cfg(not(feature = "cluster"))]
                        let owns_fault_state = publish_runtime_fault_state(&fault_state);
                        let _ = owns_fault_state;
                        tracing::error!(
                            reason = %reason,
                            "pipeline halted after a permanent error; automatic recovery is disabled"
                        );
                        *fault_slot.lock() = Some(reason.clone());
                        if let Some(ref metrics) = fault_metrics {
                            metrics.pipeline_faults_total.inc();
                        }
                        crate::pipeline::ExitReason::Halt(reason)
                    }
                    crate::pipeline::ExitReason::Fault(reason) => {
                        #[cfg(feature = "cluster")]
                        if compute_fault_is_cluster {
                            // Fence public restarts before publishing Faulted. The state CAS
                            // then orders this fault against a concurrent coordinated stop.
                            compute_fault_source_gate
                                .store(true, std::sync::atomic::Ordering::SeqCst);
                            compute_fault_recovery_fence
                                .store(true, std::sync::atomic::Ordering::Release);
                        }

                        // Publish before notifying the watcher. The cluster path first retires all
                        // authority owned by this now-dropped graph generation under the rotation
                        // fence; a full checkpoint restore can then publish the current assignment
                        // without replaying a stale predecessor transition.
                        #[cfg(feature = "cluster")]
                        let owns_fault_state = if compute_fault_is_cluster {
                            publish_cluster_compute_fault_state(
                                &fault_state,
                                &compute_fault_rotation_execution_fence,
                                &compute_fault_pending_vnode_transition,
                                &compute_fault_installed_vnode_state,
                            )
                        } else {
                            publish_runtime_fault_state(&fault_state)
                        };
                        #[cfg(not(feature = "cluster"))]
                        let owns_fault_state = publish_runtime_fault_state(&fault_state);
                        let covered_by_terminal_stop = runtime_exit_is_covered_by_terminal_stop(
                            owns_fault_state,
                            &fault_state,
                            &compute_fault_runtime_shutdown,
                        );

                        if covered_by_terminal_stop {
                            crate::pipeline::ExitReason::Shutdown
                        } else {
                            tracing::error!(
                                reason = %reason,
                                "pipeline faulted on a fatal cycle error; recovering from last checkpoint"
                            );
                            *fault_slot.lock() = Some(reason.clone());
                            #[cfg(feature = "cluster")]
                            if compute_fault_is_cluster {
                                if let Some(controller) = compute_fault_controller.as_deref() {
                                    if let Err(error) = queue_owned_cluster_compute_fault(
                                        controller,
                                        &compute_fault_pending,
                                        owns_fault_state,
                                        &compute_fault_runtime_shutdown,
                                    ) {
                                        tracing::error!(
                                            %error,
                                            "could not allocate a recovery fault request"
                                        );
                                    }
                                }
                                // A Release may have raced the close-before-queue edge.
                                compute_fault_source_gate
                                    .store(true, std::sync::atomic::Ordering::SeqCst);
                                compute_fault_recovery_fence
                                    .store(true, std::sync::atomic::Ordering::Release);
                            }
                            if let Some(ref metrics) = fault_metrics {
                                metrics.pipeline_faults_total.inc();
                            }
                            crate::pipeline::ExitReason::Fault(reason)
                        }
                    }
                };
                done_tx.send(exit);
            })
        {
            Ok(_) => {}
            Err(e) => {
                return Err(DbError::Config(format!(
                    "failed to spawn compute thread: {e}"
                )));
            }
        }

        match startup_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(_)) if runtime_shutdown.is_cancelled() || self.is_closed() => {
                #[cfg(feature = "cluster")]
                drop(startup_generation_fence.take());
                let _ = done_rx.await;
                return Err(DbError::Shutdown);
            }
            Ok(Err(e)) => {
                #[cfg(feature = "cluster")]
                drop(startup_generation_fence.take());
                let _ = done_rx.await;
                return Err(DbError::Config(e));
            }
            Err(_) => {
                #[cfg(feature = "cluster")]
                drop(startup_generation_fence.take());
                let _ = done_rx.await;
                return Err(DbError::Config(
                    "compute thread exited before entering the runtime control loop".into(),
                ));
            }
        }

        // Readiness transfers the recovered MV image and fully wired graph to the live loop. The
        // caller installed any pre-audited no-work success marker before launch, so an immediate
        // runtime fault can only clear it, never race a post-ready write that resurrects it.
        #[cfg(feature = "cluster")]
        drop(startup_generation_fence);

        let watcher_state = Arc::clone(&self.state);
        let watcher_shutdown = Arc::clone(&self.shutdown_signal);
        let watcher_fault = Arc::clone(&self.last_fault);
        let watcher_supervisor = Arc::clone(&self.supervisor_self);
        let watcher_restart_history = Arc::clone(&self.restart_history);
        let watcher_metrics = self.engine_metrics.lock().clone();
        #[cfg(feature = "cluster")]
        let watcher_controller = self.cluster_controller.lock().clone();
        #[cfg(feature = "cluster")]
        let watcher_source_gate = Arc::clone(&self.source_gate);
        #[cfg(feature = "cluster")]
        let watcher_recovery_fence = Arc::clone(&self.coordinated_recovery_fenced);
        #[cfg(feature = "cluster")]
        let watcher_authority_transition = Arc::clone(&self.cluster_authority_transition);
        let watcher_terminal_pipeline_halt = Arc::clone(&self.terminal_pipeline_halt);
        #[cfg(feature = "cluster")]
        let watcher_is_cluster = runtime_mode == RuntimeMode::Cluster;
        #[cfg(feature = "cluster")]
        let watcher_installed_vnode_state = Arc::clone(&self.installed_vnode_state);
        #[cfg(feature = "cluster")]
        let watcher_pending_vnode_transition = Arc::clone(&self.pending_vnode_transition);
        #[cfg(feature = "cluster")]
        let watcher_rotation_execution_fence = Arc::clone(&self.rotation_execution_fence);
        #[cfg(feature = "cluster")]
        let watcher_pending_compute_fault = Arc::clone(&self.pending_recovery_fault);
        #[cfg(feature = "cluster")]
        let watcher_runtime_shutdown = runtime_shutdown.clone();
        let handle = tokio::spawn(async move {
            let exit = done_rx.await.unwrap_or_else(|_| {
                crate::pipeline::ExitReason::Fault(
                    "compute thread exited without a terminal result".to_string(),
                )
            });
            match exit {
                crate::pipeline::ExitReason::Shutdown => {
                    // Lifecycle ownership finalizes the state only after every remote decision
                    // writer has settled. The watcher cannot prove that merely because the
                    // compute thread exited, so a timed-out stop remains ShuttingDown until retry.
                }
                crate::pipeline::ExitReason::Halt(reason) => {
                    tracing::error!(
                        %reason,
                        "laminar-compute thread exited after a permanent error"
                    );
                    #[cfg(feature = "cluster")]
                    if watcher_is_cluster {
                        latch_cluster_terminal_data_plane(
                            &watcher_authority_transition,
                            &watcher_terminal_pipeline_halt,
                            &watcher_source_gate,
                            &watcher_recovery_fence,
                        );
                    } else {
                        watcher_terminal_pipeline_halt
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    #[cfg(not(feature = "cluster"))]
                    watcher_terminal_pipeline_halt.store(true, std::sync::atomic::Ordering::SeqCst);
                    watcher_fault.lock().get_or_insert(reason);
                    #[cfg(feature = "cluster")]
                    if watcher_is_cluster {
                        // Repeat the fail-closed publication in case the compute thread lost its
                        // terminal channel after returning the disposition.
                        let generation = Arc::clone(&watcher_rotation_execution_fence)
                            .write_owned()
                            .await;
                        retire_cluster_compute_generation(
                            &watcher_pending_vnode_transition,
                            &watcher_installed_vnode_state,
                        );
                        publish_runtime_fault_state(&watcher_state);
                        drop(generation);
                    }
                    #[cfg(feature = "cluster")]
                    if !watcher_is_cluster {
                        publish_runtime_fault_state(&watcher_state);
                    }
                    #[cfg(not(feature = "cluster"))]
                    publish_runtime_fault_state(&watcher_state);
                    watcher_shutdown.notify_one();
                    #[cfg(feature = "cluster")]
                    if watcher_is_cluster {
                        report_cluster_terminal_halt(
                            watcher_controller,
                            watcher_pending_compute_fault,
                        )
                        .await;
                    }
                    // A permanent halt is operator-owned. Do not invoke the local supervisor.
                }
                crate::pipeline::ExitReason::Fault(reason) => {
                    tracing::error!(%reason, "laminar-compute thread exited with a recoverable fault");
                    watcher_fault.lock().get_or_insert(reason);
                    #[cfg(feature = "cluster")]
                    if watcher_is_cluster {
                        // Also cover a lost terminal channel or a compute-thread exit before
                        // its normal fault publication reached this watcher.
                        watcher_source_gate.store(true, std::sync::atomic::Ordering::SeqCst);
                        watcher_recovery_fence.store(true, std::sync::atomic::Ordering::Release);
                        let generation = Arc::clone(&watcher_rotation_execution_fence)
                            .write_owned()
                            .await;
                        retire_cluster_compute_generation(
                            &watcher_pending_vnode_transition,
                            &watcher_installed_vnode_state,
                        );
                        publish_runtime_fault_state(&watcher_state);
                        drop(generation);
                    }
                    #[cfg(feature = "cluster")]
                    if !watcher_is_cluster {
                        publish_runtime_fault_state(&watcher_state);
                    }
                    #[cfg(not(feature = "cluster"))]
                    publish_runtime_fault_state(&watcher_state);
                    watcher_shutdown.notify_one();
                    // Cluster mode: report the fault and let the leader drive a global
                    // restart; the monitor restores this node. A local restart would rewind
                    // only this node while peers advanced — an inconsistent cut.
                    #[cfg(feature = "cluster")]
                    if watcher_is_cluster {
                        tokio::select! {
                            biased;
                            () = watcher_runtime_shutdown.cancelled() => {}
                            () = report_cluster_compute_fault(
                                watcher_controller,
                                watcher_pending_compute_fault,
                            ) => {}
                        }
                        return;
                    }
                    // Auto-restart if supervised; otherwise the pipeline stays Faulted.
                    let supervised = watcher_supervisor.lock().upgrade();
                    if let Some(db) = supervised {
                        let _ =
                            spawn_supervised_restart(db, watcher_restart_history, watcher_metrics);
                    }
                }
            }
        });

        *self.runtime_handle.lock().await = Some(handle);
        Ok(())
    }

    pub(super) async fn start_connector_pipeline(
        &self,
        source_regs: HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: HashMap<String, crate::connector_manager::TableRegistration>,
        has_external: bool,
        pipeline_identity: Option<laminar_core::checkpoint::PipelineIdentity>,
        temporal_source_roles: FxHashMap<String, TemporalSourceRole>,
        ordered_interval_admissions: OrderedIntervalAdmissions,
        runtime_shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), DbError> {
        use crate::pipeline::{CheckpointSchedule, PipelineConfig};

        let runtime_mode = self.runtime_mode();

        #[cfg(feature = "cluster")]
        let startup_generation_fence = if runtime_mode == RuntimeMode::Cluster {
            let generation_fence = Arc::clone(&self.rotation_execution_fence)
                .write_owned()
                .await;
            Some(generation_fence)
        } else {
            None
        };

        self.revalidate_persisted_cluster_query_shapes(&stream_regs)
            .await?;

        let checkpoint_schedule =
            self.config
                .checkpoint
                .as_ref()
                .map_or(CheckpointSchedule::Disabled, |config| {
                    config
                        .interval_ms
                        .map_or(CheckpointSchedule::Manual, |interval_ms| {
                            CheckpointSchedule::Periodic(std::time::Duration::from_millis(
                                interval_ms,
                            ))
                        })
                });
        let checkpointing_enabled = checkpoint_schedule.is_enabled();
        let pipeline_checkpoint_timeout = self
            .config
            .checkpoint
            .as_ref()
            .and_then(|config| config.timeout_ms)
            .map_or(
                crate::checkpoint_coordinator::CheckpointConfig::default().checkpoint_timeout,
                std::time::Duration::from_millis,
            );

        let reference_tables: rustc_hash::FxHashSet<String> = self
            .table_store
            .read()
            .table_names()
            .into_iter()
            .filter(|name| {
                !table_regs
                    .get(name)
                    .is_some_and(|registration| registration.on_demand)
            })
            .collect();
        let resolved_stream_outputs = resolve_stream_output_schemas(
            &self.ctx,
            &stream_regs,
            &reference_tables,
            &ordered_interval_admissions.joins,
        )
        .await?;
        let stream_output_schemas = &resolved_stream_outputs.schemas;
        {
            let mut schemas = self.stream_schemas.write();
            schemas.clear();
            schemas.extend(
                stream_output_schemas
                    .iter()
                    .map(|(name, schema)| (name.clone(), Arc::clone(schema))),
            );
        }

        let mut graph = self.build_connector_operator_graph(
            &stream_regs,
            &table_regs,
            &resolved_stream_outputs.changelog_carrying,
            &ordered_interval_admissions.joins,
            pipeline_identity.as_ref(),
        )?;
        for (name, schema) in stream_output_schemas {
            graph.register_intermediate_schema(name, schema);
        }
        graph.set_max_managed_state_bytes(
            self.config
                .pipeline_max_managed_state_bytes
                .expect("managed-state budget must be resolved at database construction"),
        );
        let graph = graph.initialize_managed_state().await?;

        let prom_registry = self.prometheus_registry.lock().clone();
        let mut sources = self.build_pipeline_sources(
            &source_regs,
            &temporal_source_roles,
            &ordered_interval_admissions.source_modes,
            checkpointing_enabled,
            runtime_mode,
            prom_registry.as_ref(),
        )?;

        let sink_setup = self
            .prepare_pipeline_sinks(
                &sources,
                &sink_regs,
                stream_output_schemas,
                &resolved_stream_outputs.changelog_carrying,
                runtime_mode,
                checkpointing_enabled,
                pipeline_checkpoint_timeout,
                prom_registry.as_ref(),
            )
            .await?;
        let recovery = self
            .recover_pipeline_state(
                graph,
                &mut sources,
                runtime_mode,
                pipeline_checkpoint_timeout,
            )
            .await?;
        #[cfg(feature = "cluster")]
        let mut vnode_transition_launch = (runtime_mode == RuntimeMode::Cluster)
            .then(|| PendingVnodeTransitionLaunchGuard::capture(self));
        let PipelineRecoveryState {
            graph,
            recovered_mv_store,
            recovered_channel_progress,
            recovered_input_channels,
            recovered_source_watermarks,
            recovered_checkpoint_index_version,
            recovered_watermark_frontier,
            restored_reference_tables,
        } = recovery;
        let previous_mv_store = {
            let mut live = self.mv_store.write();
            std::mem::replace(&mut *live, recovered_mv_store)
        };
        drop(previous_mv_store);

        for source_name in self.catalog.list_sources() {
            if let Some(entry) = self.catalog.get_source(&source_name) {
                let (watermark, _) = recovered_source_watermark(
                    recovered_channel_progress.get(&source_name),
                    recovered_input_channels
                        .get(&source_name)
                        .is_some_and(|inventory| inventory.is_empty()),
                    recovered_source_watermarks.get(&source_name).copied(),
                );
                entry
                    .source
                    .restore_watermark_for_recovery(watermark.unwrap_or(i64::MIN));
            }
        }

        self.initialize_reference_tables(&table_regs, &stream_regs, restored_reference_tables)
            .await?;
        let watermarks = self.prepare_pipeline_watermarks(
            &sources,
            &stream_regs,
            &recovered_channel_progress,
            &recovered_input_channels,
            &recovered_source_watermarks,
            recovered_checkpoint_index_version,
            recovered_watermark_frontier,
        )?;
        let max_poll = self.config.default_buffer_size.min(1024);
        tracing::info!(
            sources = sources.len(),
            sinks = sink_setup.sinks.len(),
            streams = stream_regs.len(),
            watermark_sources = watermarks.source_ids.len(),
            "Starting event-driven connector pipeline"
        );

        let drain_budget_ns = self.config.pipeline_drain_budget_ns.unwrap_or(1_000_000);
        let query_budget_ns = self.config.pipeline_query_budget_ns.unwrap_or(8_000_000);
        let pipeline_config = PipelineConfig {
            max_poll_records: max_poll,
            channel_capacity: self.config.pipeline_channel_capacity.unwrap_or(64),
            fallback_poll_interval: if has_external {
                std::time::Duration::from_millis(10)
            } else {
                std::time::Duration::from_millis(1)
            },
            checkpoint_schedule,
            batch_window: self
                .config
                .pipeline_batch_window
                .unwrap_or(if has_external {
                    std::time::Duration::from_millis(5)
                } else {
                    std::time::Duration::ZERO
                }),
            checkpoint_timeout: pipeline_checkpoint_timeout,
            delivery_guarantee: self.config.delivery_guarantee,
            cycle_budget_ns: 10_000_000_u64.max(drain_budget_ns + query_budget_ns),
            drain_budget_ns,
            query_budget_ns,
            max_input_buf_batches: self.config.pipeline_max_input_buf_batches.unwrap_or(256),
            max_input_buf_bytes: self.config.pipeline_max_input_buf_bytes,
            backpressure_policy: self.config.pipeline_backpressure_policy,
            shared_source_isolation: self.config.shared_source_isolation,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        };

        let PreparedPipelineRuntime { runtime } = self
            .prepare_pipeline_runtime(
                sources,
                graph,
                sink_setup,
                watermarks,
                pipeline_config,
                runtime_mode,
            )
            .await?;

        #[cfg(feature = "cluster")]
        let graph_ready_vnode_state = if runtime_mode == RuntimeMode::Cluster {
            let graph_ready_deadline = checked_pipeline_deadline(
                pipeline_checkpoint_timeout,
                "pipeline graph-ready checkpoint",
            )?;
            self.prepare_graph_ready_vnode_state_binding(graph_ready_deadline)
                .await?
        } else {
            None
        };
        #[cfg(feature = "cluster")]
        if let Some(installed) = graph_ready_vnode_state.as_ref() {
            *self.installed_vnode_state.lock() = Some(installed.clone());
        }
        let launch = self
            .launch_pipeline_runtime(
                runtime,
                Arc::clone(&self.shutdown_signal),
                runtime_shutdown,
                #[cfg(feature = "cluster")]
                startup_generation_fence,
            )
            .await;
        #[cfg(feature = "cluster")]
        let launch = launch.inspect_err(|_| {
            if let Some(expected) = graph_ready_vnode_state.as_ref() {
                let mut installed = self.installed_vnode_state.lock();
                if installed.as_ref() == Some(expected) {
                    installed.take();
                }
            }
        });
        launch?;
        #[cfg(feature = "cluster")]
        if let Some(guard) = vnode_transition_launch.as_mut() {
            guard.complete();
        }

        Ok(())
    }
}
