#[cfg(feature = "cluster")]
use super::latch_cluster_terminal_data_plane;
use super::{Arc, DbError, LaminarDB, PipelineLifecycleAuthority};

impl LaminarDB {
    /// Permanently fence this process after a deterministic pipeline halt. This is process-local;
    /// remote durable terminal evidence uses [`Self::latch_durable_terminal_recovery_fence`].
    #[cfg(feature = "cluster")]
    pub(crate) fn latch_local_terminal_pipeline_halt(&self) {
        latch_cluster_terminal_data_plane(
            &self.cluster_authority_transition,
            &self.terminal_pipeline_halt,
            &self.source_gate,
            &self.coordinated_recovery_fenced,
        );
    }

    /// Permanently fence this DB instance after terminal evidence is durable anywhere in the
    /// cluster authority namespace. Healthy peers retain a distinct local-halt latch so they can
    /// still quiesce and acknowledge the terminal Prepare.
    #[cfg(feature = "cluster")]
    pub(crate) fn latch_durable_terminal_recovery_fence(&self) {
        latch_cluster_terminal_data_plane(
            &self.cluster_authority_transition,
            &self.durable_terminal_recovery_fence,
            &self.source_gate,
            &self.coordinated_recovery_fenced,
        );
    }

    #[cfg(feature = "cluster")]
    pub(super) fn validate_fresh_cluster_vnode_start(&self) -> Result<(), DbError> {
        if self.has_unapplied_vnode_transition() {
            return Err(DbError::Checkpoint(
                "[LDB-6031] cluster startup found staged vnode state but no exact recovered \
                 checkpoint; refusing a fresh graph"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Prepare the success marker for a graph generation with no vnode transition callbacks.
    /// Startup holds `assignment_adoption_lock`, so the registry cannot be overtaken while durable
    /// history is revalidated. The marker is installed at the compute-generation ready boundary.
    #[cfg(feature = "cluster")]
    pub(super) async fn prepare_graph_ready_vnode_state_binding(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<crate::vnode_transition_staging::InstalledVnodeStateBinding>, DbError> {
        let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
            DbError::Checkpoint(
                "cluster graph readiness has no vnode registry for installed-state binding".into(),
            )
        })?;
        let assignment = registry.versioned_snapshot();
        if assignment.version() == 0 || self.has_unapplied_vnode_transition() {
            return Ok(None);
        }

        let pipeline_identity = self
            .coordinator
            .lock()
            .await
            .as_ref()
            .map(crate::checkpoint_coordinator::CheckpointCoordinator::bound_pipeline_identity)
            .transpose()?;
        let Some(pipeline_identity) = pipeline_identity else {
            return Ok(None);
        };
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint(
                "cluster graph readiness has no controller for assignment validation".into(),
            )
        })?;
        let store = self
            .assignment_snapshot_store
            .lock()
            .clone()
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "cluster graph readiness has no durable assignment history".into(),
                )
            })?;
        let durable = tokio::time::timeout_at(deadline, store.load_version(assignment.version()))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "assignment {} history read timed out at graph readiness",
                    assignment.version()
                ))
            })?
            .map_err(|error| DbError::Checkpoint(error.to_string()))?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "assignment {} is absent from durable history at graph readiness",
                    assignment.version()
                ))
            })?;
        tokio::time::timeout_at(
            deadline,
            crate::rebalance::audit_assignment_snapshot_authority(
                &store,
                Some(controller.as_ref()),
                &durable,
            ),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "assignment {} authority audit timed out at graph readiness",
                assignment.version()
            ))
        })?
        .map_err(DbError::Checkpoint)?;
        let durable_owners = durable
            .to_vnode_vec(registry.vnode_count())
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        if durable.draining
            || durable.version != assignment.version()
            || durable_owners.as_slice() != assignment.owners()
        {
            return Err(DbError::Checkpoint(format!(
                "assignment {} durable history does not match the graph-ready registry",
                assignment.version()
            )));
        }
        let fence = durable
            .assignment_fence()
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        match fence.participant_incarnation(controller.instance_id().0) {
            Some(boot_incarnation) if boot_incarnation == controller.recovery_incarnation() => {}
            Some(_) => {
                return Err(DbError::Checkpoint(format!(
                    "assignment {} names a different local process incarnation at graph readiness",
                    assignment.version()
                )));
            }
            None => return Ok(None),
        }

        // Revalidate after external I/O even though startup still owns assignment adoption.
        let current = registry.versioned_snapshot();
        if current.version() != assignment.version()
            || current.owners() != assignment.owners()
            || self.has_unapplied_vnode_transition()
        {
            return Err(DbError::Checkpoint(format!(
                "assignment {} changed or gained vnode work before graph-ready publication",
                assignment.version()
            )));
        }
        Ok(Some(
            crate::vnode_transition_staging::InstalledVnodeStateBinding::new(
                fence,
                pipeline_identity,
            )?,
        ))
    }

    /// Returns `true` if the database has been shut down.
    pub fn is_closed(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Fence new work and wake the runtime so it can shut down.
    pub fn close(&self) {
        let runtime_shutdown = self.runtime_shutdown.write();
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Release);
        #[cfg(feature = "cluster")]
        self.assignment_restore_shutdown.cancel();
        runtime_shutdown.cancel();
        self.shutdown_signal.notify_one();
    }

    /// Enable auto-restart from the last checkpoint on a fault. Without it, a fault parks
    /// in `Faulted` for manual restart (the embedded default).
    pub fn enable_supervision(self: &Arc<Self>) {
        *self.supervisor_self.lock() = Arc::downgrade(self);
    }

    /// Select the next coordinated start's recovery cut. `None` selects the latest durable head.
    /// The value is taken by startup when a checkpoint coordinator is present.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_recover_target_epoch(&self, epoch: Option<u64>) {
        *self.recover_target_epoch.lock() = epoch;
    }

    /// Open or close the source-intake gate. Closed (`true`) during a coordinated round until
    /// the restore quorum, so no node re-shuffles its replay into a peer that hasn't rebound.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_source_gate(&self, closed: bool) {
        if closed {
            self.source_gate
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        let _transition = self.cluster_authority_transition.lock();
        if self
            .terminal_pipeline_halt
            .load(std::sync::atomic::Ordering::Acquire)
            || self
                .durable_terminal_recovery_fence
                .load(std::sync::atomic::Ordering::Acquire)
            || self
                .pending_recovery_fault
                .load(std::sync::atomic::Ordering::Acquire)
                != 0
        {
            self.source_gate
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        let process_authority_live = !self.is_cluster_runtime()
            || self
                .cluster_controller
                .lock()
                .as_ref()
                .is_some_and(|controller| controller.process_lease_is_live());
        if !process_authority_live {
            self.source_gate
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        if !self
            .cluster_authority_revoked
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.source_gate
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Keep clustered source intake closed while startup restores state and certifies assignment.
    /// Call before [`Self::start`]; [`Self::finish_cluster_startup`] is the only startup path that
    /// opens the gate.
    #[cfg(feature = "cluster")]
    pub fn fence_cluster_startup(&self) {
        // Every startup boundary invalidates prior same-process evidence. A later graph attempt
        // must either publish a fresh pre-start audit or use finish's fail-closed durable fallback.
        *self.startup_checkpoint_artifact_audit.lock() = None;
        self.set_source_gate(true);
        if let Some(controller) = self.cluster_controller.lock().as_ref() {
            controller.set_recovering(true);
        }
    }

    /// Retain exclusive lifecycle ownership across a coordinated stop/report/start/release round.
    #[cfg(feature = "cluster")]
    pub(crate) fn fence_coordinated_recovery_lifecycle(&self) {
        let _lifecycle_claim = self.startup_attempt.lock();
        self.set_source_gate(true);
        self.coordinated_recovery_fenced
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Release lifecycle ownership after terminal consumption unless a replacement fault won the
    /// transition; its outstanding latch keeps public mutation fenced.
    #[cfg(feature = "cluster")]
    pub(crate) fn release_coordinated_recovery_lifecycle(&self) {
        let _lifecycle_claim = self.startup_attempt.lock();
        let keep_fenced = self
            .terminal_pipeline_halt
            .load(std::sync::atomic::Ordering::Acquire)
            || self
                .durable_terminal_recovery_fence
                .load(std::sync::atomic::Ordering::Acquire)
            || self
                .pending_recovery_fault
                .load(std::sync::atomic::Ordering::Acquire)
                != 0;
        self.coordinated_recovery_fenced
            .store(keep_fenced, std::sync::atomic::Ordering::Release);
        if keep_fenced {
            self.set_source_gate(true);
        }
    }

    #[cfg(feature = "cluster")]
    pub(super) fn ensure_pipeline_lifecycle_authorized(
        &self,
        authority: PipelineLifecycleAuthority,
        operation: &str,
    ) -> Result<(), DbError> {
        if self
            .coordinated_recovery_fenced
            .load(std::sync::atomic::Ordering::Acquire)
            && authority == PipelineLifecycleAuthority::Public
        {
            return Err(DbError::InvalidOperation(format!(
                "pipeline {operation} is fenced by coordinated recovery"
            )));
        }
        Ok(())
    }

    pub(super) fn ensure_terminal_halt_allows_start(
        &self,
        authority: PipelineLifecycleAuthority,
    ) -> Result<(), DbError> {
        let _ = authority;
        let local_halt = self
            .terminal_pipeline_halt
            .load(std::sync::atomic::Ordering::Acquire);
        #[cfg(feature = "cluster")]
        let durable_halt = self.is_cluster_runtime()
            && self
                .durable_terminal_recovery_fence
                .load(std::sync::atomic::Ordering::Acquire);
        #[cfg(not(feature = "cluster"))]
        let durable_halt = false;
        if local_halt || durable_halt {
            return Err(DbError::PipelineTerminal(format!(
                "cannot restart a permanently halted pipeline in this process: {}",
                self.last_fault()
                    .unwrap_or_else(|| "terminal halt reason was not recorded".into())
            )));
        }
        Ok(())
    }

    #[cfg(not(feature = "cluster"))]
    pub(super) fn ensure_pipeline_lifecycle_authorized(
        authority: PipelineLifecycleAuthority,
        operation: &str,
    ) {
        let _ = (authority, operation);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn ensure_coordinated_recovery_mutation_unfenced(
        &self,
        operation: &str,
    ) -> Result<(), DbError> {
        self.ensure_pipeline_lifecycle_authorized(PipelineLifecycleAuthority::Public, operation)
    }

    /// Permanently withdraw this process's clustered data-plane authority after lease loss.
    #[cfg(feature = "cluster")]
    pub fn revoke_cluster_authority(&self) {
        if !self.is_cluster_runtime() {
            return;
        }
        let _transition = self.cluster_authority_transition.lock();
        let first_revocation = !self
            .cluster_authority_revoked
            .swap(true, std::sync::atomic::Ordering::AcqRel);
        self.source_gate
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if first_revocation {
            self.invalidate_shuffle_assignment_fence();
        }
        let controller = self.cluster_controller.lock().clone();
        if let Some(controller) = controller.as_ref() {
            controller.fence_process_lease();
        }
    }

    /// Whether clustered source and shuffle intake is still fenced.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn cluster_intake_fenced(&self) -> bool {
        self.cluster_authority_revoked
            .load(std::sync::atomic::Ordering::Acquire)
            || self.source_gate.load(std::sync::atomic::Ordering::Acquire)
            || (self.is_cluster_runtime()
                && self
                    .cluster_controller
                    .lock()
                    .as_ref()
                    .is_none_or(|controller| !controller.process_lease_is_live()))
    }

    /// Whether a clustered runtime is held by coordinated recovery lifecycle authority.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn coordinated_recovery_in_progress(&self) -> bool {
        self.is_cluster_runtime()
            && self
                .coordinated_recovery_fenced
                .load(std::sync::atomic::Ordering::Acquire)
    }
}
