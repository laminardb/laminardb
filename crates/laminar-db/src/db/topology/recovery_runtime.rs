//! Target reconstruction owned by the existing stopped/restore/readiness recovery round.

use std::sync::atomic::Ordering;

use laminar_core::cluster::control::{
    RecoverPhase, RecoveryAnnouncement, TopologyError, TopologyRecoveryInput, TopologyRestoreInput,
};

use super::restore::{TopologyRestorePurpose, TopologyRuntimeMetadata};
use super::{DbError, DbState, LaminarDB, PreparedTopologyRestore};

pub(super) fn start_epoch(start: &RecoveryAnnouncement) -> Result<u64, DbError> {
    match start.phase {
        RecoverPhase::Start { epoch } => Ok(epoch),
        _ => Err(TopologyError::Fenced.into()),
    }
}

impl LaminarDB {
    pub(crate) async fn recovered_topology_runtime_is_active(
        &self,
        binding: &super::InstalledTopologyRuntime,
    ) -> Result<bool, DbError> {
        let Some(recovery) = &binding.recovery else {
            return Ok(false);
        };
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        if !recovery.released
            || binding.shutdown.is_cancelled()
            || DbState::load(&self.state) != DbState::Running
            || self.is_closed()
            || self.coordinated_recovery_in_progress()
            || controller.is_recovering()
            || controller.is_draining()
            || self.pending_recovery_fault.load(Ordering::Acquire) != 0
            || self.last_fault.lock().is_some()
            || self.cluster_authority_revoked.load(Ordering::Acquire)
            || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || self.terminal_pipeline_halt.load(Ordering::Acquire)
        {
            return Ok(false);
        }
        self.validate_topology_transport_identity(&binding.input)?;
        self.ensure_topology_runtime_live(&binding.input, Some(&recovery.selection))
            .await?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        let snapshot = controller
            .read_recovery_admission_snapshot()
            .await
            .map_err(|error| TopologyError::Conflict(error.to_string()))?;
        let expected = RecoveryAnnouncement {
            round: recovery.start.round.clone(),
            phase: RecoverPhase::ReleaseCommitted {
                epoch: recovery.selection.outcome().epoch,
            },
        };
        if snapshot.committed_release() != Some(&expected)
            || snapshot.topology_commit() != binding.input.operation().commit.as_ref()
        {
            return Ok(false);
        }
        Box::pin(controller.audit_recovery_topology(&recovery.start.round, None))
            .await
            .map_err(|error| TopologyError::Conflict(error.to_string()))?;
        let proof = controller
            .audit_assignment_leader_authority(binding.input.assignment(), None, deadline)
            .await
            .map_err(TopologyError::Conflict)?;
        controller
            .recovery_admission_is_current(&snapshot, &proof)
            .await
            .map_err(|error| DbError::from(TopologyError::Conflict(error.to_string())))
    }

    pub(crate) async fn prepare_coordinated_topology_restore(
        &self,
        start: &RecoveryAnnouncement,
    ) -> Result<PreparedTopologyRestore, DbError> {
        self.ensure_topology_recovery_stopped(start)?;
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        if controller
            .observe_recover_control()
            .await
            .map_err(|error| TopologyError::Conflict(error.to_string()))?
            .as_ref()
            != Some(start)
        {
            return Err(TopologyError::Fenced.into());
        }
        let commit = start
            .round
            .topology_binding()
            .ok_or(TopologyError::Fenced)?
            .commit();
        self.prepare_topology_restore_image(
            commit.operation_id,
            TopologyRestorePurpose::CoordinatedRecovery(Box::new(start.clone())),
        )
        .await
    }

    fn ensure_topology_recovery_boundary(
        &self,
        start: &RecoveryAnnouncement,
    ) -> Result<(), DbError> {
        start_epoch(start)?;
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        if self.is_closed()
            || !self.is_cluster_runtime()
            || !self.coordinated_recovery_in_progress()
            || !controller.is_recovering()
            || controller.is_draining()
            || !self.source_gate.load(Ordering::Acquire)
            || self.cluster_authority_revoked.load(Ordering::Acquire)
            || self.durable_terminal_recovery_fence.load(Ordering::Acquire)
            || self.terminal_pipeline_halt.load(Ordering::Acquire)
            || start.round.topology_binding().is_none_or(|binding| {
                controller
                    .try_live_local_process_authority_identity()
                    .ok()
                    .is_none_or(|process| !binding.processes().contains(&process))
            })
        {
            return Err(TopologyError::Fenced.into());
        }
        self.ensure_catalog_cleanup_unfenced("coordinated topology recovery")
    }

    pub(super) fn ensure_topology_recovery_stopped(
        &self,
        start: &RecoveryAnnouncement,
    ) -> Result<(), DbError> {
        self.ensure_topology_recovery_boundary(start)?;
        if !matches!(
            DbState::load(&self.state),
            DbState::Created | DbState::Faulted
        ) || self
            .startup_attempt
            .lock()
            .as_ref()
            .is_some_and(|attempt| !attempt.is_complete())
            || self
                .runtime_handle
                .try_lock()
                .map_err(|_| TopologyError::Fenced)?
                .is_some()
            || !self.owned_source_tasks.lock().is_empty()
            || !self.owned_sink_handles.lock().is_empty()
            || !self.owned_connector_task_fences.lock().is_empty()
        {
            return Err(TopologyError::Fenced.into());
        }
        Ok(())
    }

    pub(crate) fn ensure_topology_recovery_runtime_held(
        &self,
        start: &RecoveryAnnouncement,
    ) -> Result<(), DbError> {
        self.ensure_topology_recovery_boundary(start)?;
        if !matches!(
            DbState::load(&self.state),
            DbState::Starting | DbState::Running
        ) || !self.topology_cut_hold.load(Ordering::Acquire)
            || self.runtime_shutdown.read().is_cancelled()
            || self.last_fault.lock().is_some()
        {
            return Err(TopologyError::Fenced.into());
        }
        Ok(())
    }

    pub(crate) async fn validate_topology_runtime_image(
        &self,
        image: &mut PreparedTopologyRestore,
    ) -> Result<(), DbError> {
        if let Some(start) = image.recovery_start.as_ref() {
            if !image.belongs_to_recovery(self, start) {
                return Err(TopologyError::Fenced.into());
            }
            self.validate_topology_recovery_installation(
                &image.input,
                image.recovery_input().ok_or(TopologyError::Fenced)?,
                start,
            )
            .await
        } else {
            self.validate_topology_installation(&image.input).await
        }
    }

    pub(crate) async fn validate_topology_runtime_metadata(
        &self,
        metadata: &TopologyRuntimeMetadata,
    ) -> Result<(), DbError> {
        if let Some(recovery) = &metadata.recovery {
            self.validate_topology_recovery_installation(
                &metadata.input,
                &recovery.selection,
                &recovery.start,
            )
            .await
        } else {
            self.validate_topology_installation(&metadata.input).await
        }
    }

    async fn validate_topology_recovery_installation(
        &self,
        input: &TopologyRestoreInput,
        selection: &TopologyRecoveryInput,
        start: &RecoveryAnnouncement,
    ) -> Result<(), DbError> {
        self.ensure_topology_recovery_runtime_held(start)?;
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let epoch = start_epoch(start)?;
        let observed = controller
            .observe_recover_control()
            .await
            .map_err(|error| TopologyError::Conflict(error.to_string()))?
            .ok_or(TopologyError::Fenced)?;
        if observed.round != start.round
            || !matches!(observed.phase,
                RecoverPhase::Start { epoch: selected } | RecoverPhase::Release { epoch: selected }
                | RecoverPhase::ReleaseCommitted { epoch: selected } if selected == epoch)
        {
            return Err(TopologyError::Fenced.into());
        }
        let fresh = controller
            .topology_recovery_input(&start.round, epoch)
            .await?;
        if !fresh.same_restore_requirements(selection)
            || !fresh.migration().same_restore_requirements(input)
        {
            return Err(TopologyError::Fenced.into());
        }
        self.validate_topology_transport_identity(input)?;
        self.ensure_topology_recovery_runtime_held(start)
    }

    pub(crate) async fn certify_recovered_cluster_topology(
        &self,
        start: &RecoveryAnnouncement,
    ) -> Result<(), DbError> {
        let binding = self
            .installed_topology_runtime
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let recovery = binding
            .recovery
            .as_ref()
            .filter(|recovery| recovery.start == *start && !recovery.released)
            .ok_or(TopologyError::Fenced)?;
        self.validate_topology_recovery_installation(&binding.input, &recovery.selection, start)
            .await?;
        self.ensure_topology_runtime_live(&binding.input, Some(&recovery.selection))
            .await?;
        if binding.shutdown.is_cancelled() {
            return Err(TopologyError::Fenced.into());
        }
        let controller = self
            .cluster_controller
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        controller
            .certify_topology_recovery_installation(start, &recovery.selection, binding.runtime_id)
            .await?;
        self.ensure_topology_runtime_live(&binding.input, Some(&recovery.selection))
            .await?;
        self.ensure_topology_recovery_runtime_held(start)
    }

    pub(crate) async fn validate_recovered_cluster_topology_release(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<(), DbError> {
        if release.round.topology_binding().is_none() {
            return Ok(());
        }
        let binding = self
            .installed_topology_runtime
            .lock()
            .clone()
            .ok_or(TopologyError::Fenced)?;
        let recovery = binding.recovery.as_ref().ok_or(TopologyError::Fenced)?;
        if recovery.start.round != release.round
            || !matches!(release.phase, RecoverPhase::Release { epoch } | RecoverPhase::ReleaseCommitted { epoch }
                if epoch == recovery.selection.outcome().epoch)
            || binding.shutdown.is_cancelled()
        {
            return Err(TopologyError::Fenced.into());
        }
        self.validate_topology_recovery_installation(
            &binding.input,
            &recovery.selection,
            &recovery.start,
        )
        .await?;
        self.ensure_topology_runtime_live(&binding.input, Some(&recovery.selection))
            .await
    }

    pub(crate) fn record_recovered_topology_release(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<(), DbError> {
        if release.round.topology_binding().is_none() {
            return Ok(());
        }
        let mut installed = self.installed_topology_runtime.lock();
        let binding = installed.as_mut().ok_or(TopologyError::Fenced)?;
        let recovery = binding.recovery.as_mut().ok_or(TopologyError::Fenced)?;
        if recovery.start.round != release.round
            || release.phase
                != (RecoverPhase::ReleaseCommitted {
                    epoch: recovery.selection.outcome().epoch,
                })
            || binding.shutdown.is_cancelled()
        {
            return Err(TopologyError::Fenced.into());
        }
        recovery.released = true;
        Ok(())
    }
}
