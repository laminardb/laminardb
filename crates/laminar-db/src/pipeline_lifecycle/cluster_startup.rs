#[cfg(feature = "cluster")]
use super::{Arc, ClusterStartupDisposition, StartupCheckpointArtifactAudit};
use super::{DbError, LaminarDB, StartupAttempt};

impl LaminarDB {
    #[cfg(feature = "cluster")]
    pub(super) fn should_defer_initial_sink_epoch_for_artifact_recovery(
        &self,
    ) -> Result<bool, DbError> {
        if !self.is_cluster_runtime() {
            return Ok(false);
        }
        let Some(StartupCheckpointArtifactAudit::Artifacts(audit_process)) =
            *self.startup_checkpoint_artifact_audit.lock()
        else {
            return Ok(false);
        };
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("cluster artifact recovery has no process authority".into())
        })?;
        let current_process = controller
            .try_live_local_process_authority_identity()
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "cluster artifact recovery lost process authority: {error}"
                ))
            })?;
        if current_process != audit_process || !self.cluster_intake_fenced() {
            return Err(DbError::Checkpoint(
                "cluster artifact recovery cannot defer sink admission without its exact fenced startup authority"
                    .into(),
            ));
        }
        Ok(true)
    }

    /// Finish clustered startup after the exact assignment fence is available.
    ///
    /// Fresh nodes open intake only when no durable recovery round exists. A process that restored
    /// local state, or one that observes an active/stale round, remains fenced and requests a full
    /// coordinated rewind.
    ///
    /// # Errors
    ///
    /// Returns a checkpoint error when the local assignment has not been certified. Intake remains
    /// closed when recovery authority is unavailable so the monitor can retry without admitting
    /// records.
    #[cfg(feature = "cluster")]
    pub async fn finish_cluster_startup(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<ClusterStartupDisposition, DbError> {
        let authority_revision = self
            .assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire);
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("cluster startup has no recovery controller".into())
        })?;
        let registry =
            self.vnode_registry.lock().clone().ok_or_else(|| {
                DbError::Checkpoint("cluster startup has no vnode assignment".into())
            })?;
        let assignment = registry.versioned_snapshot();
        let assignment_version = assignment.version();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        let assignment_fence = controller
            .checkpoint_assignment_fence(assignment_version)
            .filter(|fence| fence.matches_owner_map(&owners))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "cluster assignment {assignment_version} is not certified for source intake"
                ))
            })?;
        let local_id = controller.instance_id().0;
        let local_incarnation = assignment_fence.participant_incarnation(local_id);
        let idle = match local_incarnation {
            Some(incarnation) if incarnation == controller.recovery_incarnation() => false,
            Some(_) => {
                return Err(DbError::Checkpoint(format!(
                    "cluster assignment {assignment_version} certifies another incarnation of process {local_id}"
                )));
            }
            None if owners.contains(&local_id) => {
                return Err(DbError::Checkpoint(format!(
                    "cluster assignment {assignment_version} gives process {local_id} ownership without checkpoint authority"
                )));
            }
            None => true,
        };
        if idle {
            // This process is absent from the exact owner-complete checkpoint roster. A global
            // artifact inventory can therefore belong only to the active owners; treating it as
            // local crash residue would let an idle join interrupt their live checkpoint. Install
            // topology metadata while retaining the local data-plane fence, without publishing a
            // recovery fault or attempting to settle another process's artifacts.
            controller.set_recovering(false);
            let drain_transition = controller
                .checkpoint_drain_transition()
                .filter(|transition| transition.predecessor == assignment_fence);
            let activation = self
                .activate_assignment_authority(
                    &assignment_fence,
                    drain_transition,
                    authority_revision,
                    deadline,
                )
                .await?;
            if !activation.installed {
                self.set_source_gate(true);
                return Ok(ClusterStartupDisposition::RecoveryFenced);
            }
            return Ok(ClusterStartupDisposition::Idle);
        }
        // A clean pre-start audit linearizes before this process can publish graph readiness. Once
        // the owner-complete assignment fence exists, every participant has crossed that same
        // boundary, so a later inventory belongs to live checkpoint work and is not crash residue.
        // Evidence from any other process generation is unusable and falls back to the durable
        // read below.
        let audited_artifacts = self
            .startup_checkpoint_artifact_audit
            .lock()
            .as_ref()
            .copied()
            .filter(|audit| {
                controller
                    .try_live_local_process_authority_identity()
                    .is_ok_and(|process| process == audit.process())
            });
        let unresolved_artifacts = match audited_artifacts {
            Some(StartupCheckpointArtifactAudit::Clean(_)) => false,
            Some(StartupCheckpointArtifactAudit::Artifacts(_)) => true,
            None => {
                let checkpoint_authority = controller.checkpoint_authority().map_err(|error| {
                    DbError::Checkpoint(format!(
                        "cluster startup checkpoint authority is unavailable: {error}"
                    ))
                })?;
                tokio::time::timeout_at(
                    deadline,
                    checkpoint_authority.cluster_checkpoint_artifacts(),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint("cluster startup artifact inventory read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "cluster startup artifact inventory read failed: {error}"
                    ))
                })?
                .is_some()
            }
        };
        if unresolved_artifacts {
            controller.set_recovering(true);
            tokio::time::timeout_at(
                deadline,
                crate::coordinated_recovery::request_local_fault(
                    &controller,
                    &self.pending_recovery_fault,
                ),
            )
            .await
            .map_err(|_| {
                DbError::Checkpoint("startup artifact recovery fault publication timed out".into())
            })?
            .map_err(DbError::Checkpoint)?;
            return Ok(ClusterStartupDisposition::RecoveryFenced);
        }
        let fault_inventory =
            match tokio::time::timeout_at(deadline, controller.read_recovery_fault_inventory())
                .await
            {
                Err(_) => {
                    controller.set_recovering(true);
                    return Err(DbError::Checkpoint(
                        "cluster startup recovery fault audit timed out".into(),
                    ));
                }
                Ok(Err(error)) => {
                    controller.set_recovering(true);
                    return Err(DbError::Checkpoint(format!(
                        "cluster startup recovery fault audit failed: {error}"
                    )));
                }
                Ok(Ok(inventory)) => inventory,
            };
        if fault_inventory.has_terminal_fault() {
            self.latch_durable_terminal_recovery_fence();
            controller.set_recovering(true);
            let reason = "cluster startup is fenced by a durable terminal pipeline fault";
            self.last_fault.lock().get_or_insert(reason.into());
            return Err(DbError::PipelineTerminal(reason.into()));
        }
        let pending_fault = !fault_inventory.faults().is_empty();
        let Ok(active) = tokio::time::timeout_at(deadline, controller.observe_recover()).await
        else {
            return Err(DbError::Checkpoint(
                "cluster startup recovery authority read timed out".into(),
            ));
        };
        let active = match active {
            Ok(active) => active,
            Err(error) => {
                controller.set_recovering(true);
                tracing::error!(%error, "startup recovery authority is not currently valid");
                tokio::time::timeout_at(
                    deadline,
                    crate::coordinated_recovery::request_local_fault(
                        &controller,
                        &self.pending_recovery_fault,
                    ),
                )
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(
                            "startup recovery fault publication timed out".into(),
                        )
                    })?
                    .map_err(|report_error| {
                        DbError::Checkpoint(format!(
                            "startup recovery authority failed ({error}); fault publication failed: {report_error}"
                        ))
                    })?;
                return Ok(ClusterStartupDisposition::RecoveryFenced);
            }
        };
        let open_intake = if pending_fault {
            controller.set_recovering(true);
            false
        } else if let Some(active) = active {
            controller.set_recovering(true);
            if !controller.recovery_driver_is_current(&active.round)
                || !controller.recovery_round_contains_current_process(&active.round)
            {
                tokio::time::timeout_at(
                    deadline,
                    crate::coordinated_recovery::request_local_fault(
                        &controller,
                        &self.pending_recovery_fault,
                    ),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint("startup recovery fault publication timed out".into())
                })?
                .map_err(DbError::Checkpoint)?;
            }
            false
        } else if self.last_recovery_epoch.lock().is_some() {
            controller.set_recovering(true);
            tokio::time::timeout_at(
                deadline,
                crate::coordinated_recovery::request_local_fault(
                    &controller,
                    &self.pending_recovery_fault,
                ),
            )
            .await
            .map_err(|_| {
                DbError::Checkpoint("startup recovery fault publication timed out".into())
            })?
            .map_err(DbError::Checkpoint)?;
            false
        } else {
            controller.set_recovering(false);
            true
        };

        let drain_transition = controller
            .checkpoint_drain_transition()
            .filter(|transition| transition.predecessor == assignment_fence);
        let activation = self
            .activate_assignment_authority(
                &assignment_fence,
                drain_transition,
                authority_revision,
                deadline,
            )
            .await?;
        if !activation.installed {
            controller.set_recovering(true);
            self.set_source_gate(true);
            return Ok(ClusterStartupDisposition::RecoveryFenced);
        }
        Ok(if open_intake && activation.intake_open {
            ClusterStartupDisposition::Serving
        } else {
            ClusterStartupDisposition::RecoveryFenced
        })
    }

    /// Advance both shuffle directions to the coordinated recovery generation. Old streams are
    /// rejected so pre-rewind frames cannot be folded and then replayed.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_shuffle_recovery_gen(&self, gen: u64) {
        // Fence inbound old-generation streams before outbound streams can emit the new one.
        if let Some(receiver) = self.shuffle_receiver.lock().as_ref() {
            receiver.set_recovery_gen(gen);
        }
        if let Some(sender) = self.shuffle_sender.lock().as_ref() {
            sender.set_recovery_gen(gen);
        }
    }

    /// Import the last recovery generation that reached an irrevocable cluster Release before a
    /// fresh pipeline starts. The allocation high-watermark is deliberately not used: a driver
    /// can reserve a generation and fail before any data plane rewinds to it.
    ///
    /// # Errors
    /// Returns an error when durable recovery authority is unavailable, the process lease is
    /// lost, or an already-active transport conflicts with the committed terminal.
    #[cfg(feature = "cluster")]
    pub async fn prepare_cluster_startup_recovery_generation(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        // Never reuse evidence from an earlier or failed bootstrap attempt. Only the complete
        // authority-checked read below publishes a replacement latch.
        *self.startup_checkpoint_artifact_audit.lock() = None;
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("cluster recovery generation bootstrap has no controller".into())
        })?;
        if !controller.process_lease_is_live() {
            return Err(DbError::Checkpoint(
                "cluster recovery generation bootstrap lost its process lease".into(),
            ));
        }
        let audit_process = controller
            .try_live_local_process_authority_identity()
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "cluster startup artifact audit has no live process authority: {error}"
                ))
            })?;
        let terminal =
            tokio::time::timeout_at(deadline, controller.latest_committed_recover_release())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "committed recovery Release lookup exceeded its deadline".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "committed recovery Release authority is unavailable: {error}"
                    ))
                })?;
        let committed = terminal
            .as_ref()
            .map_or(0, |release| release.round.id.generation);
        let checkpoint_authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!(
                "cluster startup checkpoint authority is unavailable: {error}"
            ))
        })?;
        let unresolved_artifacts = tokio::time::timeout_at(
            deadline,
            checkpoint_authority.cluster_checkpoint_artifacts(),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint("cluster startup artifact inventory read timed out".into())
        })?
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "cluster startup artifact inventory read failed: {error}"
            ))
        })?
        .is_some();
        let final_process = controller
            .try_live_local_process_authority_identity()
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "cluster startup artifact audit lost process authority: {error}"
                ))
            })?;
        if final_process != audit_process {
            return Err(DbError::Checkpoint(
                "cluster startup artifact audit changed process authority during its read".into(),
            ));
        }
        *self.startup_checkpoint_artifact_audit.lock() = Some(if unresolved_artifacts {
            StartupCheckpointArtifactAudit::Artifacts(audit_process)
        } else {
            StartupCheckpointArtifactAudit::Clean(audit_process)
        });
        let current = self.shuffle_recovery_generation()?.unwrap_or(0);
        if current == committed {
            return Ok(());
        }
        let assignment_active = self
            .shuffle_receiver
            .lock()
            .as_ref()
            .is_some_and(|receiver| receiver.assignment_version() != 0)
            || self
                .shuffle_sender
                .lock()
                .as_ref()
                .is_some_and(|sender| sender.assignment_version() != 0);
        let exact_terminal_participant = terminal.as_ref().is_some_and(|release| {
            controller.recovery_round_requires_current_process_stop(&release.round)
        });
        if current != 0 || assignment_active || exact_terminal_participant {
            return Err(DbError::Checkpoint(format!(
                "startup shuffle recovery generation {current} conflicts with committed generation {committed}"
            )));
        }
        self.set_shuffle_recovery_gen(committed);
        let installed = self.shuffle_recovery_generation()?.unwrap_or(committed);
        if installed != committed {
            return Err(DbError::Checkpoint(format!(
                "startup shuffle recovery generation {installed} does not match committed generation {committed}"
            )));
        }
        if !controller.process_lease_is_live() {
            return Err(DbError::Checkpoint(
                "cluster recovery generation bootstrap lost its process lease".into(),
            ));
        }
        if committed != 0 {
            let epoch = terminal.as_ref().and_then(|release| match release.phase {
                laminar_core::cluster::control::RecoverPhase::ReleaseCommitted { epoch } => {
                    Some(epoch)
                }
                _ => None,
            });
            tracing::info!(
                generation = committed,
                ?epoch,
                "restored committed shuffle recovery generation"
            );
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn shuffle_recovery_generation(&self) -> Result<Option<u64>, DbError> {
        let receiver = self.shuffle_receiver.lock().clone();
        let sender = self.shuffle_sender.lock().clone();
        let receiver_generation = receiver.as_ref().map(|receiver| receiver.recovery_gen());
        let sender_generation = sender.as_ref().map(|sender| sender.recovery_gen());
        if let (Some(receiver), Some(sender)) = (receiver_generation, sender_generation) {
            if receiver != sender {
                return Err(DbError::Checkpoint(format!(
                    "shuffle endpoints disagree on recovery generation: receiver {receiver}, sender {sender}"
                )));
            }
        }
        Ok(receiver_generation.or(sender_generation))
    }

    /// Resolve only the cumulative shuffle-loss cutoff captured when this exact generation
    /// started. This must succeed before publishing local `Release` readiness.
    #[cfg(feature = "cluster")]
    pub(crate) fn complete_shuffle_recovery(&self, gen: u64) -> bool {
        self.shuffle_receiver
            .lock()
            .as_ref()
            .is_none_or(|receiver| receiver.complete_recovery(gen))
    }

    /// Start the database-owned per-node recovery supervisor once. Coordinated recovery is the only cluster
    /// fault path — a local-only restart rewinds one node while peers advance, an
    /// inconsistent cut.
    ///
    /// # Errors
    ///
    /// Returns an error if the database-owned control runtime cannot be initialized.
    #[cfg(feature = "cluster")]
    pub fn enable_coordinated_recovery(self: &Arc<Self>) -> Result<(), DbError> {
        if !self.is_cluster_runtime() || self.cluster_controller.lock().is_none() {
            return Err(DbError::Config(
                "coordinated recovery requires a cluster runtime and controller".into(),
            ));
        }
        let runtime = self.control_runtime.handle()?;
        let mut owned = self.recovery_monitor.lock();
        if owned.as_ref().is_some_and(|monitor| !monitor.is_finished()) {
            return Ok(());
        }
        if owned.take().is_some() {
            tracing::warn!("replacing an unexpectedly terminated coordinated recovery supervisor");
        }
        *owned = Some(crate::coordinated_recovery::spawn_monitor(self, &runtime));
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(super) async fn quiesce_recovery_monitor_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let Some(mut monitor) = self.recovery_monitor.lock().take() else {
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, &mut monitor).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(DbError::Pipeline(format!(
                "coordinated recovery supervisor failed during shutdown: {error}"
            ))),
            Err(_) => {
                *self.recovery_monitor.lock() = Some(monitor);
                Err(DbError::Pipeline(
                    "coordinated recovery supervisor did not quiesce before the shutdown deadline"
                        .into(),
                ))
            }
        }
    }

    /// Close resources created by an unsuccessful `start_inner` attempt.
    pub(super) async fn cleanup_failed_start(&self) -> Result<(), DbError> {
        const CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
        // INVARIANT: startup installs its generation token before preparing connectors. Cancel it
        // even when startup fails before runtime launch so recovery can prove the Created process
        // has no live generation and a partially launched task observes the same terminal signal.
        self.runtime_shutdown.read().cancel();
        self.shutdown_signal.notify_one();
        self.quiesce_checkpoint_decision_until(deadline).await?;
        {
            let mut coordinator = tokio::time::timeout_at(deadline, self.coordinator.lock())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "failed-start cleanup could not reacquire checkpoint coordinator ownership; durability fences remain held"
                            .into(),
                    )
            })?;
            if let Some(coordinator) = coordinator.as_mut() {
                tokio::time::timeout_at(deadline, coordinator.reconcile_sink_open_witness())
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "failed-start checkpoint reconciliation exceeded {CLEANUP_TIMEOUT:?}; durability fences remain held"
                        ))
                    })??;
                coordinator
                    .reconcile_sink_open_witness_until(deadline)
                    .await?;
                coordinator.clear_sinks()?;
            }
        }
        *self.control_tx.lock() = None;
        *self.force_ckpt_tx.lock() = None;
        self.quiesce_connector_generation_until(deadline).await?;
        *self.checkpoint_namespace_lock.lock() = None;
        Ok(())
    }

    pub(super) async fn await_startup_attempt_until(
        &self,
        attempt: &StartupAttempt,
        deadline: tokio::time::Instant,
        operation: &str,
    ) -> Result<(), DbError> {
        match tokio::time::timeout_at(deadline, attempt.wait()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                tracing::debug!(%error, %operation, "startup reached a failed terminal state");
                Ok(())
            }
            Err(_) => Err(DbError::Pipeline(format!(
                "{operation} could not observe terminal startup before its deadline; startup remains fenced"
            ))),
        }
    }

    pub(super) async fn quiesce_connector_generation_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let (sources, sinks, startup) = tokio::join!(
            self.quiesce_owned_source_tasks_until(deadline),
            self.quiesce_owned_sink_handles_until(deadline),
            self.quiesce_owned_connector_task_fences_until(deadline),
        );
        let mut failures = Vec::new();
        if let Err(error) = sources {
            failures.push(format!("sources: {error}"));
        }
        if let Err(error) = sinks {
            failures.push(format!("sinks: {error}"));
        }
        if let Err(error) = startup {
            failures.push(format!("startup: {error}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::Connector(format!(
                "connector generation remains fenced: {}",
                failures.join("; ")
            )))
        }
    }

    pub(super) async fn quiesce_owned_connector_task_fences_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let fences = {
            let mut owned = self.owned_connector_task_fences.lock();
            owned.retain(|fence| !fence.is_finished());
            owned.clone()
        };
        if fences.is_empty() {
            return Ok(());
        }

        futures::future::join_all(fences.iter().map(|fence| fence.wait_until(deadline))).await;
        let unresolved_names = {
            let mut owned = self.owned_connector_task_fences.lock();
            owned.retain(|fence| !fence.is_finished());
            owned
                .iter()
                .map(|fence| fence.name().to_owned())
                .collect::<Vec<_>>()
        };
        if unresolved_names.is_empty() {
            Ok(())
        } else {
            Err(DbError::Connector(format!(
                "cannot replace a pipeline while pre-actor connector tasks remain unresolved: {}",
                unresolved_names.join(", ")
            )))
        }
    }

    pub(super) async fn quiesce_owned_source_tasks_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let tasks = self.owned_source_tasks.lock().clone();
        if tasks.is_empty() {
            return Ok(());
        }

        // Signal every task before awaiting any one task. Aborting retires the owned connector
        // generation; its lease remains fenced until the stable supervisor observes task exit.
        for task in &tasks {
            task.request_shutdown();
            task.abort();
        }
        let completions =
            futures::future::join_all(tasks.iter().map(|task| task.wait_until(deadline))).await;
        for (task, finished) in tasks.iter().zip(completions) {
            if finished {
                task.log_terminal_outcome();
            }
        }

        let unresolved_names = {
            let mut owned = self.owned_source_tasks.lock();
            owned.retain(|task| !task.is_finished());
            owned
                .iter()
                .map(|task| task.name().to_owned())
                .collect::<Vec<_>>()
        };
        if unresolved_names.is_empty() {
            return Ok(());
        }
        Err(DbError::Connector(format!(
            "cannot replace a pipeline while prior source tasks remain unresolved: {}",
            unresolved_names.join(", ")
        )))
    }

    pub(super) async fn quiesce_owned_sink_handles_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let handles = {
            let mut owned = self.owned_sink_handles.lock();
            owned.retain(crate::sink_task::SinkTaskHandle::has_unresolved_task);
            owned.clone()
        };
        if handles.is_empty() {
            return Ok(());
        }
        if deadline <= tokio::time::Instant::now() {
            return Err(DbError::Connector(
                "sink-generation quiescence budget was exhausted before terminal cleanup began; \
                 prior actors remain fenced"
                    .into(),
            ));
        }

        // Poll every close in the same turn so independent actors share one restart budget. Each
        // close has its own wrapper at the shared deadline: one slow actor cannot discard an
        // already-published sticky failure from another actor. Cancellation leaves the DB-owned
        // handles in place and any close that crossed admission continues in its stable driver.
        let close_results =
            futures::future::join_all(handles.iter().cloned().map(|handle| async move {
                let name = handle.name().to_owned();
                let result = tokio::time::timeout_at(deadline, handle.close()).await;
                (name, result)
            }))
            .await;
        let mut failures = close_results
            .into_iter()
            .filter_map(|(name, result)| match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(format!("{name}: {error}")),
                Err(_) => Some(format!(
                    "{name}: shared sink-generation close deadline expired"
                )),
            })
            .collect::<Vec<_>>();

        // A close result is not a terminal proof: timeout, disconnection, or a panicked close
        // driver can publish immediately while the actor or a connector child remains live.
        // Spend the remainder of the same generation deadline observing both proofs before the
        // registry decides whether replacement is safe.
        futures::future::join_all(
            handles
                .iter()
                .map(|handle| handle.wait_terminal_until(deadline)),
        )
        .await;

        let unresolved_names = {
            let mut owned = self.owned_sink_handles.lock();
            owned.retain(crate::sink_task::SinkTaskHandle::has_unresolved_task);
            owned
                .iter()
                .map(|handle| handle.name().to_owned())
                .collect::<Vec<_>>()
        };
        if unresolved_names.is_empty() {
            for failure in failures {
                tracing::warn!(%failure, "terminal sink cleanup reported an error");
            }
            return Ok(());
        }
        failures.push(format!("still active: {}", unresolved_names.join(", ")));
        Err(DbError::Connector(format!(
            "cannot replace a pipeline while prior sink actors remain unresolved: {}",
            failures.join("; ")
        )))
    }
}
