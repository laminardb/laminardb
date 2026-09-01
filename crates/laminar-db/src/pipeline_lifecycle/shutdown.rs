use super::{
    checked_pipeline_deadline, Arc, DbError, DbState, LaminarDB, PipelineLifecycleAuthority,
    PUBLIC_PIPELINE_STOP_TIMEOUT,
};
#[cfg(feature = "cluster")]
use super::{
    configured_checkpoint_timeout, coordinated_recovery_stop_ceiling, report_cluster_terminal_halt,
    retire_cluster_compute_generation_until,
};

impl LaminarDB {
    pub(super) async fn quiesce_checkpoint_decision_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let _coordinator = tokio::time::timeout_at(deadline, self.coordinator.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6038] teardown could not acquire checkpoint coordinator ownership; durable decision writes remain fenced"
                        .into(),
                )
            })?;
        Ok(())
    }

    pub(super) async fn reconcile_sink_open_witness_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let mut coordinator = tokio::time::timeout_at(deadline, self.coordinator.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6050] teardown could not acquire checkpoint coordinator ownership for sink-open reconciliation"
                        .into(),
                )
            })?;
        if let Some(coordinator) = coordinator.as_mut() {
            coordinator
                .reconcile_sink_open_witness_until(deadline)
                .await?;
        }
        Ok(())
    }

    /// Shut down the streaming pipeline gracefully. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the watcher task panicked.
    pub async fn shutdown(&self) -> Result<(), DbError> {
        const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
        // Terminal intent takes precedence over a concurrent restartable stop. Publishing it
        // before lifecycle arbitration also prevents a brief stop-created state from admitting
        // a new startup while shutdown is queued behind that stop.
        self.close();
        #[cfg(feature = "cluster")]
        if self.is_cluster_runtime()
            && self
                .terminal_pipeline_halt
                .load(std::sync::atomic::Ordering::Acquire)
        {
            // Catalog bootstrap can fail before the recovery monitor exists. Shutdown is then the
            // last live owner capable of proving the permanent disposition to shared authority;
            // it must not quiesce that authority or publish Stopped until durable proof exists.
            let controller = { self.cluster_controller.lock().clone() };
            report_cluster_terminal_halt(controller, Arc::clone(&self.pending_recovery_fault))
                .await;
            self.latch_durable_terminal_recovery_fence();
        }
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        #[cfg(feature = "cluster")]
        self.quiesce_recovery_monitor_until(deadline).await?;
        let first_shutdown = loop {
            let startup = {
                let owned = self.startup_attempt.lock();
                if let Some(in_flight) = owned.as_ref().filter(|attempt| !attempt.is_complete()) {
                    Arc::clone(in_flight)
                } else {
                    match DbState::load(&self.state) {
                        DbState::Stopped => {
                            drop(owned);
                            return Ok(());
                        }
                        DbState::Starting => {
                            return Err(DbError::Pipeline(
                                "shutdown found Starting without an incomplete owned startup attempt"
                                    .into(),
                            ));
                        }
                        DbState::ShuttingDown => break false,
                        observed @ (DbState::Created | DbState::Running | DbState::Faulted) => {
                            if DbState::compare_exchange(
                                observed,
                                DbState::ShuttingDown,
                                &self.state,
                            )
                            .is_ok()
                            {
                                break true;
                            }
                            continue;
                        }
                    }
                }
            };
            self.await_startup_attempt_until(&startup, deadline, "pipeline shutdown")
                .await?;
        };
        self.runtime_shutdown.write().cancel();
        if first_shutdown {
            *self.force_ckpt_tx.lock() = None;
            self.shutdown_signal.notify_one();
        }

        let _topology = tokio::time::timeout_at(deadline, self.topology_ddl_lock.write())
            .await
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "pipeline shutdown could not acquire topology ownership within \
                     {SHUTDOWN_TIMEOUT:?}; catalog mutation remains fenced"
                ))
            })?;
        let _lifecycle = tokio::time::timeout_at(deadline, self.lifecycle_lock.lock())
            .await
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "pipeline shutdown could not acquire lifecycle ownership within \
                     {SHUTDOWN_TIMEOUT:?}; startup/stop remains fenced"
                ))
            })?;

        let mut runtime_handle = tokio::time::timeout_at(deadline, self.runtime_handle.lock())
            .await
            .map_err(|_| {
                DbError::Pipeline(
                    "pipeline shutdown could not reacquire runtime watcher ownership; runtime remains fenced in ShuttingDown"
                        .into(),
                )
            })?;
        let mut watcher_error = None;
        if let Some(handle) = runtime_handle.as_mut() {
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(Ok(())) => {
                    runtime_handle.take();
                }
                Ok(Err(error)) => {
                    runtime_handle.take();
                    watcher_error = Some(DbError::Pipeline(format!(
                        "pipeline watcher failed during shutdown: {error}"
                    )));
                }
                Err(_) => {
                    return Err(DbError::Pipeline(format!(
                        "pipeline shutdown exceeded {SHUTDOWN_TIMEOUT:?}; runtime is still \
                         draining and remains fenced in ShuttingDown; retry shutdown"
                    )));
                }
            }
        }
        drop(runtime_handle);

        // The runtime can call `complete_pending_vnode_transition` until its join handle has
        // finished. Retire the staged transition and its predecessor binding only after that
        // generation is gone, and keep the graph rotation write fence through terminal lifecycle
        // publication so no callback or assignment adoption can observe a split pair.
        #[cfg(feature = "cluster")]
        let _retired_cluster_generation = if self.is_cluster_runtime() {
            Some(
                retire_cluster_compute_generation_until(
                    &self.rotation_execution_fence,
                    &self.pending_vnode_transition,
                    &self.installed_vnode_state,
                    deadline,
                )
                .await
                .map_err(|_| {
                    DbError::Pipeline(
                        "pipeline shutdown could not retire cluster vnode state before its \
                         deadline; runtime remains fenced in ShuttingDown"
                            .into(),
                    )
                })?,
            )
        } else {
            None
        };
        if watcher_error.is_none() {
            if let Some(fault) = self.last_fault.lock().clone() {
                watcher_error = Some(DbError::Pipeline(format!(
                    "pipeline faulted while shutting down: {fault}"
                )));
            } else {
                tracing::info!("Pipeline shut down cleanly");
            }
        }

        // Compute has stopped producing new checkpoint work. Keep every deployment/state fence
        // until an already-issued remote decision create reaches a terminal client-side state.
        self.quiesce_checkpoint_decision_until(deadline).await?;
        self.reconcile_sink_open_witness_until(deadline).await?;
        self.quiesce_connector_generation_until(deadline).await?;

        *self.checkpoint_namespace_lock.lock() = None;
        DbState::Stopped.store(&self.state);
        watcher_error.map_or(Ok(()), Err)
    }

    /// Stop the streaming pipeline so it can be restarted.
    ///
    /// # Errors
    /// Returns [`DbError::InvalidOperation`] if the pipeline is still starting
    /// or the coordinator does not exit within the stop timeout.
    pub async fn stop_pipeline(&self) -> Result<(), DbError> {
        self.stop_pipeline_with_lifecycle_authority(PipelineLifecycleAuthority::Public)
            .await
    }

    /// End-to-end deadline owned by the recovery stop itself. This is derived only from the
    /// immutable database configuration: consulting the live coordinator here can deadlock with
    /// the coordinator task that stop is joining. Two cleanup windows cover an admitted attempt
    /// and its retained tail, with a final margin for lifecycle and connector settlement.
    #[cfg(feature = "cluster")]
    pub(crate) fn coordinated_recovery_stop_timeout(&self) -> std::time::Duration {
        let cleanup_timeout =
            crate::checkpoint_coordinator::CheckpointConfig::default().cleanup_timeout;
        coordinated_recovery_stop_ceiling(
            configured_checkpoint_timeout(&self.config),
            cleanup_timeout,
        )
    }

    /// Recovery-owned stop after the lifecycle fence is published.
    #[cfg(feature = "cluster")]
    pub(crate) async fn stop_pipeline_for_coordinated_recovery(&self) -> Result<(), DbError> {
        self.stop_pipeline_with_lifecycle_authority(PipelineLifecycleAuthority::CoordinatedRecovery)
            .await
    }

    pub(super) async fn stop_pipeline_with_lifecycle_authority(
        &self,
        authority: PipelineLifecycleAuthority,
    ) -> Result<(), DbError> {
        let stop_timeout = match authority {
            PipelineLifecycleAuthority::Public => PUBLIC_PIPELINE_STOP_TIMEOUT,
            #[cfg(feature = "cluster")]
            PipelineLifecycleAuthority::CoordinatedRecovery => {
                self.coordinated_recovery_stop_timeout()
            }
        };
        let deadline = checked_pipeline_deadline(stop_timeout, "pipeline stop")?;
        let first_stop = loop {
            let startup = {
                let owned = self.startup_attempt.lock();
                #[cfg(feature = "cluster")]
                self.ensure_pipeline_lifecycle_authorized(authority, "stop")?;
                #[cfg(not(feature = "cluster"))]
                Self::ensure_pipeline_lifecycle_authorized(authority, "stop");
                if let Some(in_flight) = owned.as_ref().filter(|attempt| !attempt.is_complete()) {
                    Arc::clone(in_flight)
                } else {
                    match DbState::load(&self.state) {
                        DbState::Created | DbState::Stopped => {
                            drop(owned);
                            return Ok(());
                        }
                        DbState::Starting => {
                            return Err(DbError::InvalidOperation(
                                "pipeline stop found Starting without an incomplete owned startup attempt"
                                    .into(),
                            ));
                        }
                        DbState::ShuttingDown => break false,
                        observed @ (DbState::Running | DbState::Faulted) => {
                            if DbState::compare_exchange(
                                observed,
                                DbState::ShuttingDown,
                                &self.state,
                            )
                            .is_ok()
                            {
                                break true;
                            }
                            continue;
                        }
                    }
                }
            };
            self.await_startup_attempt_until(&startup, deadline, "pipeline stop")
                .await
                .map_err(|error| DbError::InvalidOperation(error.to_string()))?;
        };
        self.runtime_shutdown.write().cancel();
        if first_stop {
            *self.force_ckpt_tx.lock() = None;
            // Clear up front so DDL during/after shutdown registers for the next start()
            // instead of hot-adding into the dying coordinator's channel.
            *self.control_tx.lock() = None;
            self.shutdown_signal.notify_one();
        }

        #[cfg(test)]
        {
            let stop_after_claim_gate = { self.stop_after_claim_gate.lock().clone() };
            if let Some((entered, release)) = stop_after_claim_gate {
                entered.notify_one();
                release.notified().await;
            }
        }

        let _topology = tokio::time::timeout_at(deadline, self.topology_ddl_lock.write())
            .await
            .map_err(|_| {
                DbError::InvalidOperation(format!(
                    "pipeline stop could not acquire topology ownership within {stop_timeout:?}; catalog mutation remains fenced"
                ))
            })?;
        let _lifecycle = tokio::time::timeout_at(deadline, self.lifecycle_lock.lock())
            .await
            .map_err(|_| {
                DbError::InvalidOperation(format!(
                    "pipeline stop could not acquire lifecycle ownership within {stop_timeout:?}; \
                     an earlier lifecycle operation remains fenced"
                ))
            })?;
        let mut runtime_handle = tokio::time::timeout_at(deadline, self.runtime_handle.lock())
            .await
            .map_err(|_| {
                DbError::InvalidOperation(
                    "pipeline stop could not reacquire runtime watcher ownership; runtime remains fenced in ShuttingDown"
                        .into(),
                )
            })?;
        if let Some(handle) = runtime_handle.as_mut() {
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(Ok(())) => {
                    runtime_handle.take();
                    tracing::info!("Pipeline stopped cleanly");
                }
                Ok(Err(e)) => {
                    runtime_handle.take();
                    tracing::warn!(error = %e, "Pipeline task panicked during stop");
                }
                Err(_) => {
                    tracing::warn!(
                        timeout = ?stop_timeout,
                        "Pipeline stop still draining; will finalize when the coordinator exits"
                    );
                    return Err(DbError::InvalidOperation(
                        "pipeline stop is taking longer than expected; coordinator still \
                         draining, retry shortly"
                            .into(),
                    ));
                }
            }
        }
        drop(runtime_handle);

        // Do not clear the installed predecessor binding while the old compute generation can
        // still observe its staged transition. The joined runtime cannot hold a graph read fence;
        // taking the write side now retires both claims atomically with respect to callbacks and
        // assignment adoption, and retaining it through `Created` publication closes the final
        // lifecycle race.
        #[cfg(feature = "cluster")]
        let _retired_cluster_generation = if self.is_cluster_runtime() {
            Some(
                retire_cluster_compute_generation_until(
                    &self.rotation_execution_fence,
                    &self.pending_vnode_transition,
                    &self.installed_vnode_state,
                    deadline,
                )
                .await
                .map_err(|_| {
                    DbError::InvalidOperation(
                        "pipeline stop could not retire cluster vnode state before its deadline; \
                         runtime remains fenced in ShuttingDown"
                            .into(),
                    )
                })?,
            )
        } else {
            None
        };

        // Do not announce Created or release the exclusive deployment lock while a timed-out
        // decision create can still mutate the recovery frontier. A later stop retry resumes here.
        self.quiesce_checkpoint_decision_until(deadline).await?;
        self.reconcile_sink_open_witness_until(deadline).await?;
        self.quiesce_connector_generation_until(deadline).await?;

        *self.checkpoint_namespace_lock.lock() = None;
        if self.is_closed() {
            // A concurrent shutdown owns the terminal transition. Leaving ShuttingDown in place
            // is intentional if that shutdown was cancelled; a retry must finish its teardown.
            return Ok(());
        }
        if self
            .terminal_pipeline_halt
            .load(std::sync::atomic::Ordering::Acquire)
        {
            match DbState::compare_exchange(DbState::ShuttingDown, DbState::Faulted, &self.state) {
                Ok(_) | Err(DbState::Faulted) => return Ok(()),
                Err(observed) => {
                    return Err(DbError::InvalidOperation(format!(
                        "terminal pipeline stop completed from unexpected lifecycle state {observed:?}; restart remains fenced"
                    )));
                }
            }
        }
        match DbState::compare_exchange(DbState::ShuttingDown, DbState::Created, &self.state) {
            Ok(_) | Err(DbState::Created | DbState::Stopped) => Ok(()),
            Err(observed) => Err(DbError::InvalidOperation(format!(
                "pipeline stop completed from unexpected lifecycle state {observed:?}; restart remains fenced"
            ))),
        }
    }
}
