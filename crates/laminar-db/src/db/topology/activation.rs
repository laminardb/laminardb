//! Participant runtime ownership, durable Release and local intake activation.

use std::sync::{atomic::Ordering, Arc};

use laminar_core::cluster::control::{
    TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyError, TopologyOperationId,
    TopologyRestoreInput,
};
use uuid::Uuid;

use super::{DbError, DbState, LaminarDB};

#[derive(Clone)]
pub(crate) struct InstalledTopologyRuntime {
    pub(crate) input: TopologyRestoreInput,
    pub(crate) runtime_id: Uuid,
    pub(crate) shutdown: tokio_util::sync::CancellationToken,
    pub(crate) released_sequence: Option<u64>,
    pub(crate) recovery: Option<super::restore::TopologyRecoveryRuntime>,
}

#[derive(Clone, Copy)]
enum ActivationAction {
    Certify,
    Release,
    Apply,
}

impl LaminarDB {
    pub(crate) async fn refresh_released_topology_assignment(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        revision: u64,
        deadline: tokio::time::Instant,
    ) -> Result<Option<crate::db::AssignmentAuthorityActivation>, DbError> {
        let binding = self.installed_topology_runtime.lock().clone();
        if let Some(binding) = binding
            .as_ref()
            .filter(|binding| binding.recovery.is_some())
        {
            if binding.input.assignment() != fence
                || !self.recovered_topology_runtime_is_active(binding).await?
            {
                return Err(TopologyError::Fenced.into());
            }
            let proof = controller
                .audit_assignment_leader_authority(fence, None, deadline)
                .await
                .map_err(TopologyError::Conflict)?;
            return Ok(Some(
                self.open_assignment_intake_after_audit(
                    controller,
                    fence,
                    None,
                    proof.owner.node_id,
                    revision,
                    deadline,
                )
                .await?,
            ));
        }
        let Some(binding) = binding.filter(|binding| binding.released_sequence.is_some()) else {
            return Ok(None);
        };
        let fresh = controller
            .committed_topology_restore_input(binding.input.operation().operation_id)
            .await?;
        Self::require_runtime_release(
            &binding,
            &fresh,
            binding.released_sequence.ok_or(TopologyError::Fenced)?,
        )?;
        self.ensure_topology_runtime_live(&fresh, None).await?;
        if fresh.assignment() != fence
            || !controller
                .authorize_topology_release(&fresh, binding.runtime_id)
                .await?
            || binding.shutdown.is_cancelled()
            || DbState::load(&self.state) != DbState::Running
            || self.is_closed()
            || self.pending_recovery_fault.load(Ordering::Acquire) != 0
            || self.cluster_authority_revoked.load(Ordering::Acquire)
            || self.terminal_pipeline_halt.load(Ordering::Acquire)
            || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || self.coordinated_recovery_in_progress()
            || self
                .owned_source_tasks
                .lock()
                .iter()
                .any(|source| !source.is_running())
            || self
                .owned_sink_handles
                .lock()
                .iter()
                .any(|sink| !sink.is_ready())
        {
            return Err(TopologyError::Fenced.into());
        }
        let leader = fresh.current_leader().ok_or(TopologyError::Fenced)?;
        Ok(Some(
            self.open_assignment_intake_after_audit(
                controller,
                fence,
                None,
                leader.owner.node_id,
                revision,
                deadline,
            )
            .await?,
        ))
    }

    /// Certify this process's exact installed runtime while intake remains held. The DB-owned
    /// executor continues after caller disconnect. This receipt alone never authorizes output.
    ///
    /// # Errors
    /// Rejects absent/dead actors, stale process/assignment/transport, faults or uncertain writes.
    pub async fn certify_installed_cluster_topology(
        self: &Arc<Self>,
        operation: TopologyOperationId,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        self.run_owned_topology_activation(operation, ActivationAction::Certify)
            .await
    }

    /// As the current leader, publish participant-complete Release and apply it locally. Each
    /// follower must separately apply the same durable Release against its own runtime receipt.
    /// A committed Release remains authoritative if local activation fails; intake stays held.
    ///
    /// # Errors
    /// Rejects incomplete/currently stale rosters, leader loss, local runtime failure or the budget.
    pub async fn release_installed_cluster_topology(
        self: &Arc<Self>,
        operation: TopologyOperationId,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        self.run_owned_topology_activation(operation, ActivationAction::Release)
            .await
    }

    /// Apply the authoritative Release to this exact runtime. Reconcile/admit sink epochs through
    /// the existing coordinator before opening intake. Caller cancellation never owns that work.
    ///
    /// # Errors
    /// Rejects missing Release, another runtime's receipt, stale authority or unresolved sink work.
    pub async fn apply_cluster_topology_release(
        self: &Arc<Self>,
        operation: TopologyOperationId,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        self.run_owned_topology_activation(operation, ActivationAction::Apply)
            .await
    }

    async fn run_owned_topology_activation(
        self: &Arc<Self>,
        operation: TopologyOperationId,
        action: ActivationAction,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        let executor = self.control_runtime.handle()?;
        let permit = Arc::clone(&self.topology_validation_lock)
            .try_lock_owned()
            .map_err(|_| TopologyError::Contended)?;
        let owner = Arc::clone(self);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task = executor.spawn(async move {
            let _permit = permit;
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(45);
            let result = tokio::time::timeout_at(
                deadline,
                owner.activate_installed_topology(operation, action, deadline),
            )
            .await
            .map_err(|_| DbError::from(TopologyError::Contended))
            .and_then(std::convert::identity);
            let _ = result_tx.send(result);
        });
        // This bounded control-path owner retains the DB and its executor until completion. The
        // observer owns only the response; disconnect cannot drop a sink/authority write future.
        drop(task);
        result_rx.await.map_err(|_| DbError::Pipeline("owned topology activation task exited without its result; reread the operation and retry".into()))?
    }

    async fn activate_installed_topology(
        &self,
        operation: TopologyOperationId,
        action: ActivationAction,
        deadline: tokio::time::Instant,
    ) -> Result<TopologyAdmissionStatus, DbError> {
        // Lock ordering matches startup/stop: topology -> lifecycle -> assignment. No synchronous
        // lock crosses an await. Fault/process revocation retains its separate transition fence.
        let _topology = self.topology_ddl_lock.write().await;
        let _lifecycle = self.lifecycle_lock.lock().await;
        let _assignment = self.assignment_adoption_lock.lock().await;
        let binding = self
            .installed_topology_runtime
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        if binding.input.operation().operation_id != operation
            || binding.shutdown.is_cancelled()
            || DbState::load(&self.state) != DbState::Running
        {
            return Err(TopologyError::Fenced.into());
        }
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let revision = self.assignment_authority_revision.load(Ordering::Acquire);
        let fresh = controller
            .committed_topology_restore_input(operation)
            .await?;
        if !fresh.same_installed_generation(&binding.input) {
            return Err(TopologyError::Fenced.into());
        }
        if let Some(sequence) = binding.released_sequence {
            Self::require_runtime_release(&binding, &fresh, sequence)?;
            if !controller
                .authorize_topology_release(&fresh, binding.runtime_id)
                .await?
            {
                return Err(TopologyError::Fenced.into());
            }
            self.ensure_released_topology_runtime(&binding, &controller)?;
            self.ensure_topology_runtime_live(&fresh, None).await?;
            return Ok(fresh.operation().clone());
        }
        self.validate_topology_installation(&fresh).await?;
        self.ensure_topology_runtime_ready(&fresh).await?;
        let sinks = self.owned_sink_handles.lock().clone();
        for sink in &sinks {
            sink.sync_until(deadline).await.map_err(|error| {
                DbError::Connector(format!(
                    "topology readiness for sink '{}': {error}",
                    sink.name()
                ))
            })?;
        }
        let sender = self
            .shuffle_sender
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        sender
            .establish_assignment_mesh(fresh.assignment())
            .await
            .map_err(|error| {
                TopologyError::Conflict(format!("target receiver mesh is not ready: {error}"))
            })?;
        self.ensure_topology_runtime_ready(&fresh).await?;
        self.ensure_topology_runtime_held()?;
        if !matches!(action, ActivationAction::Apply) {
            controller
                .certify_topology_installation(&fresh, binding.runtime_id)
                .await?;
        }
        if matches!(action, ActivationAction::Certify) {
            self.ensure_topology_runtime_ready(&fresh).await?;
            return Ok(controller
                .committed_topology_restore_input(operation)
                .await?
                .operation()
                .clone());
        }
        let fresh = controller
            .committed_topology_restore_input(operation)
            .await?;
        if matches!(action, ActivationAction::Release) {
            controller.release_topology_target(&fresh).await?;
        }
        let released = controller
            .committed_topology_restore_input(operation)
            .await?;
        let sequence = released
            .operation()
            .activation
            .as_ref()
            .and_then(|round| round.release.as_ref())
            .map(|release| release.authority_sequence)
            .ok_or_else(|| {
                TopologyError::Conflict(
                    "the exact target runtime has no participant-complete Release".into(),
                )
            })?;
        Self::require_runtime_release(&binding, &released, sequence)?;
        self.ensure_topology_runtime_ready(&released).await?;
        {
            let mut coordinator = self.coordinator.lock().await;
            let coordinator = coordinator.as_mut().ok_or(TopologyError::Fenced)?;
            coordinator
                .reconcile_sink_open_witness_until(deadline)
                .await?;
            coordinator
                .ensure_assignment_sink_epoch_until(deadline)
                .await?;
        }
        let after = controller
            .committed_topology_restore_input(operation)
            .await?;
        Self::require_runtime_release(&binding, &after, sequence)?;
        if !controller
            .authorize_topology_release(&after, binding.runtime_id)
            .await?
        {
            return Err(TopologyError::Fenced.into());
        }
        self.validate_topology_installation(&after).await?;
        self.ensure_topology_runtime_ready(&after).await?;
        {
            let _transition = self.cluster_authority_transition.lock();
            self.ensure_topology_runtime_held()?;
            if binding.shutdown.is_cancelled()
                || controller.is_recovering()
                || controller.is_draining()
                || controller.try_live_local_process_authority_identity().ok()
                    != Some(after.process())
                || self.assignment_authority_revision.load(Ordering::Acquire) != revision
                || self
                    .owned_sink_handles
                    .lock()
                    .iter()
                    .any(|sink| !sink.is_ready())
                || self
                    .owned_source_tasks
                    .lock()
                    .iter()
                    .any(|source| !source.is_running())
            {
                return Err(TopologyError::Fenced.into());
            }
            let mut installed = self.installed_topology_runtime.lock();
            let installed = installed
                .as_mut()
                .filter(|installed| {
                    installed.runtime_id == binding.runtime_id && !installed.shutdown.is_cancelled()
                })
                .ok_or(TopologyError::Fenced)?;
            installed.released_sequence = Some(sequence);
            self.topology_cut_hold.store(false, Ordering::Release);
            self.source_gate.store(false, Ordering::SeqCst);
        }
        Ok(after.operation().clone())
    }

    fn require_runtime_release(
        binding: &InstalledTopologyRuntime,
        input: &TopologyRestoreInput,
        sequence: u64,
    ) -> Result<(), DbError> {
        let round = input
            .operation()
            .activation
            .as_ref()
            .ok_or(TopologyError::Fenced)?;
        if !input.same_installed_generation(&binding.input)
            || input.operation().phase != TopologyAdmissionPhase::Active
            || round.assignment != *input.assignment()
            || round.processes != input.processes()
            || !round.installation_complete()
            || round
                .release
                .as_ref()
                .is_none_or(|release| release.authority_sequence != sequence)
            || !round.installations.iter().any(|receipt| {
                receipt.process == input.process() && receipt.runtime_id == binding.runtime_id
            })
        {
            return Err(TopologyError::Fenced.into());
        }
        Ok(())
    }

    fn ensure_released_topology_runtime(
        &self,
        binding: &InstalledTopologyRuntime,
        controller: &laminar_core::cluster::control::ClusterController,
    ) -> Result<(), DbError> {
        if self.is_closed()
            || binding.shutdown.is_cancelled()
            || DbState::load(&self.state) != DbState::Running
            || self.source_gate.load(Ordering::Acquire)
            || self.topology_cut_hold.load(Ordering::Acquire)
            || self.cluster_authority_revoked.load(Ordering::Acquire)
            || self.terminal_pipeline_halt.load(Ordering::Acquire)
            || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || self.pending_recovery_fault.load(Ordering::Acquire) != 0
            || self.coordinated_recovery_in_progress()
            || controller.is_recovering()
            || controller.is_draining()
            || controller.try_live_local_process_authority_identity().ok()
                != Some(binding.input.process())
        {
            return Err(TopologyError::Fenced.into());
        }
        Ok(())
    }
}
