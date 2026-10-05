//! Participant preparation and old-topology cuts through configured authority and process gates.

use super::{ClusterController, LeaderProof};
use crate::checkpoint::CheckpointAttempt;
use crate::checkpoint_decision::CheckpointArtifactInventory;
use crate::cluster::control::topology::{
    TopologyAdmissionStatus, TopologyError, TopologyOperationId, TopologyPlanRef,
};

impl ClusterController {
    /// Audit permission to open this exact installed runtime under a durable topology Release.
    ///
    /// # Errors
    /// Rejects recovery, draining, stale local adoption/process or unavailable authority.
    pub async fn authorize_topology_release(
        &self,
        input: &crate::cluster::control::TopologyRestoreInput,
        runtime_id: uuid::Uuid,
    ) -> Result<bool, TopologyError> {
        if self.is_recovering()
            || self.is_draining()
            || self.try_live_local_process_authority_identity().ok() != Some(input.process())
        {
            return Ok(false);
        }
        let allowed = self
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?
            .authorize_topology_release(
                self.snapshot.as_ref().ok_or(TopologyError::Fenced)?,
                self.process_lease_authority
                    .get()
                    .ok_or(TopologyError::Fenced)?,
                input,
                runtime_id,
            )
            .await?;
        Ok(allowed
            && !self.is_recovering()
            && !self.is_draining()
            && self.try_live_local_process_authority_identity().ok() == Some(input.process())
            && self
                .checkpoint_assignment_fence(input.assignment().assignment_version)
                .as_ref()
                == Some(input.assignment()))
    }

    /// Certify a held installed runtime after its DB owner has observed source/sink/graph readiness.
    ///
    /// # Errors
    /// Rejects stale local process/adoption, recovery/draining, or conflicting durable evidence.
    pub async fn certify_topology_installation(
        &self,
        input: &crate::cluster::control::TopologyRestoreInput,
        runtime_id: uuid::Uuid,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        let before = self
            .committed_topology_restore_input(input.operation().operation_id)
            .await?;
        if !before.same_restore_requirements(input) || self.is_recovering() || self.is_draining() {
            return Err(TopologyError::Fenced);
        }
        self.checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?
            .certify_topology_installation(
                self.snapshot.as_ref().ok_or(TopologyError::Fenced)?,
                self.process_lease_authority
                    .get()
                    .ok_or(TopologyError::Fenced)?,
                input,
                runtime_id,
                crate::cluster::control::TOPOLOGY_INSTALLATION_PROTOCOL_VERSION,
            )
            .await?;
        let after = self
            .committed_topology_restore_input(input.operation().operation_id)
            .await?;
        if !after.same_restore_requirements(input) || self.is_recovering() || self.is_draining() {
            return Err(TopologyError::Fenced);
        }
        Ok(after.operation().clone())
    }

    /// Commit participant-complete Release for the current installation round.
    ///
    /// # Errors
    /// Rejects a follower, stale local adoption, recovery, incomplete roster or uncertain authority.
    pub async fn release_topology_target(
        &self,
        input: &crate::cluster::control::TopologyRestoreInput,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        let proof = self.capture_leader_proof().ok_or(TopologyError::Fenced)?;
        if self.is_recovering() || self.is_draining() {
            return Err(TopologyError::Fenced);
        }
        let status = self
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?
            .release_topology_target(
                &proof,
                self.snapshot.as_ref().ok_or(TopologyError::Fenced)?,
                self.process_lease_authority
                    .get()
                    .ok_or(TopologyError::Fenced)?,
                input,
            )
            .await?;
        if self.capture_leader_proof().as_ref() != Some(&proof)
            || self.is_recovering()
            || self.is_draining()
            || self.try_live_local_process_authority_identity().ok() != Some(input.process())
        {
            return Err(TopologyError::Fenced);
        }
        Ok(status)
    }

    /// Record this process's exact-root private restore and observed parent retirement.
    /// The caller must observe termination through runtime-owned actor/connector handles.
    /// A receipt records historical preparation, never installation or output authorization.
    ///
    /// # Errors
    /// Rejects changed restore requirements, local process/adoption, recovery or draining.
    /// On an uncertain/cancelled append, read status and retry the same retained image/input.
    pub async fn certify_topology_target_preparation(
        &self,
        input: &crate::cluster::control::TopologyRestoreInput,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        let before = self
            .topology_restore_input(input.operation().operation_id)
            .await?;
        if !before.same_restore_requirements(input) {
            return Err(TopologyError::Fenced);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|e| TopologyError::Protocol(e.to_string()))?;
        let status = authority
            .certify_topology_target_preparation(
                self.snapshot.as_ref().ok_or_else(|| {
                    TopologyError::Protocol("target preparation has no assignment authority".into())
                })?,
                self.process_lease_authority.get().ok_or_else(|| {
                    TopologyError::Protocol("target preparation has no process authority".into())
                })?,
                input,
                crate::cluster::control::TOPOLOGY_COMMIT_PROTOCOL_VERSION,
            )
            .await?;
        let after = self
            .topology_restore_input(input.operation().operation_id)
            .await?;
        if !after.same_restore_requirements(input)
            || status
                .target_preparations
                .iter()
                .any(|receipt| !after.operation().target_preparations.contains(receipt))
        {
            return Err(TopologyError::Fenced);
        }
        Ok(after.operation().clone())
    }

    /// Read current private restore input using this controller's exact local process adoption.
    /// This does not install a graph or authorize source/output work.
    ///
    /// # Errors
    /// Rejects recovery, draining, stale process/assignment adoption and damaged root evidence.
    pub async fn topology_restore_input(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<crate::cluster::control::TopologyRestoreInput, TopologyError> {
        self.read_topology_restore_input(operation_id, false).await
    }

    /// Read explicit committed-root reconstruction authority for the exact locally adopted process.
    /// Recovery may replace boots with the same vnode owners. No actors or Release are authorized.
    ///
    /// # Errors
    /// Rejects obsolete/uncommitted targets, changed current process/assignment or damaged evidence.
    pub async fn committed_topology_restore_input(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<crate::cluster::control::TopologyRestoreInput, TopologyError> {
        self.read_topology_restore_input(operation_id, true).await
    }

    /// Select exact target recovery progress using this controller's live process adoption.
    /// A target checkpoint takes precedence over the retained root. This grants no actor or
    /// output permission; replacement runtimes still require the coordinated recovery quorum.
    ///
    /// # Errors
    /// Rejects stale local adoption, changed target/assignment and damaged selected evidence.
    pub async fn committed_topology_recovery_input(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<crate::cluster::control::TopologyRecoveryInput, TopologyError> {
        let before = self
            .try_live_local_process_authority_identity()
            .map_err(|_| TopologyError::Fenced)?;
        if self.is_draining() {
            return Err(TopologyError::Fenced);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?;
        let assignments = self.snapshot.as_ref().ok_or_else(|| {
            TopologyError::Protocol("recovery has no assignment authority".into())
        })?;
        let processes = self
            .process_lease_authority
            .get()
            .ok_or_else(|| TopologyError::Protocol("recovery has no process authority".into()))?;
        let input = authority
            .committed_topology_recovery_input(assignments, processes, operation_id, before)
            .await?;
        self.require_topology_restore_adoption(input.migration(), before, true)
            .await?;
        Ok(input)
    }

    /// Private state selection while an exact topology-bound recovery round owns the held plane.
    /// The assignment certificate may be suspended during Prepare; its durable adoption, complete
    /// frozen process terms and current owner map remain mandatory. This never authorizes actors.
    ///
    /// # Errors
    /// Rejects another round/target/cut, obsolete leader/process/adoption or damaged evidence.
    pub async fn topology_recovery_input(
        &self,
        round: &super::RecoveryRound,
        epoch: u64,
    ) -> Result<crate::cluster::control::TopologyRecoveryInput, TopologyError> {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let binding = round.topology_binding().ok_or(TopologyError::Fenced)?;
            if !self.is_recovering()
                || self.is_draining()
                || !self.recovery_round_contains_current_process(round)
            {
                return Err(TopologyError::Fenced);
            }
            self.audit_recovery_topology(round, Some(epoch))
                .await
                .map_err(|error| TopologyError::Conflict(error.to_string()))?;
            let before = self
                .try_live_local_process_authority_identity()
                .map_err(|_| TopologyError::Fenced)?;
            if !binding.processes().contains(&before) {
                return Err(TopologyError::Fenced);
            }
            let authority = self
                .checkpoint_authority()
                .map_err(|error| TopologyError::Protocol(error.to_string()))?;
            let input = authority
                .committed_topology_recovery_input(
                    self.snapshot.as_deref().ok_or(TopologyError::Fenced)?,
                    self.process_lease_authority
                        .get()
                        .ok_or(TopologyError::Fenced)?,
                    binding.commit().operation_id,
                    before,
                )
                .await?;
            let evidence = self
                .read_local_process_authority_evidence()
                .await
                .map_err(|error| TopologyError::Conflict(error.to_string()))?;
            if input.outcome().epoch != epoch
                || input.migration().operation().commit.as_ref() != Some(binding.commit())
                || input.migration().assignment() != &round.assignment_fence
                || input.migration().processes() != binding.processes()
                || input.migration().current_leader().as_ref() != Some(&round.leader_proof)
                || evidence.participant != before.participant
                || evidence.process_term != before.process_term
                || !evidence
                    .adopted_assignment
                    .matches_fence(&round.assignment_fence)
                || self
                    .checkpoint_assignment_fence(round.assignment_fence.assignment_version)
                    .is_some_and(|fence| fence != round.assignment_fence)
                || self.try_live_local_process_authority_identity().ok() != Some(before)
                || !self.is_recovering()
                || self.is_draining()
            {
                return Err(TopologyError::Fenced);
            }
            Ok(input)
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    /// Certify the actual held recovered runtime before its restore acknowledgement. An already
    /// released original topology keeps its old receipt roster; the new recovery round owns output.
    ///
    /// # Errors
    /// Rejects a noncurrent Start, dead process/adoption, changed cut or competing runtime receipt.
    pub async fn certify_topology_recovery_installation(
        &self,
        start: &super::RecoveryAnnouncement,
        input: &crate::cluster::control::TopologyRecoveryInput,
        runtime_id: uuid::Uuid,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        let super::RecoverPhase::Start { epoch } = start.phase else {
            return Err(TopologyError::Fenced);
        };
        if self
            .observe_recover_control()
            .await
            .map_err(|error| TopologyError::Conflict(error.to_string()))?
            .as_ref()
            != Some(start)
        {
            return Err(TopologyError::Fenced);
        }
        let fresh = self.topology_recovery_input(&start.round, epoch).await?;
        if !fresh.same_restore_requirements(input) {
            return Err(TopologyError::Fenced);
        }
        self.checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?
            .certify_topology_recovery_installation(
                self.snapshot.as_deref().ok_or(TopologyError::Fenced)?,
                self.process_lease_authority
                    .get()
                    .ok_or(TopologyError::Fenced)?,
                fresh.migration(),
                runtime_id,
                &start.round,
                epoch,
            )
            .await?;
        let after = self.topology_recovery_input(&start.round, epoch).await?;
        if !after.same_restore_requirements(input)
            || self
                .observe_recover_control()
                .await
                .map_err(|error| TopologyError::Conflict(error.to_string()))?
                .as_ref()
                != Some(start)
        {
            return Err(TopologyError::Fenced);
        }
        Ok(after.migration().operation().clone())
    }

    async fn read_topology_restore_input(
        &self,
        operation_id: TopologyOperationId,
        committed: bool,
    ) -> Result<crate::cluster::control::TopologyRestoreInput, TopologyError> {
        let before = self
            .try_live_local_process_authority_identity()
            .map_err(|_| TopologyError::Fenced)?;
        if (!committed && self.is_recovering()) || self.is_draining() {
            return Err(TopologyError::Fenced);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|e| TopologyError::Protocol(e.to_string()))?;
        let assignments = self
            .snapshot
            .as_ref()
            .ok_or_else(|| TopologyError::Protocol("restore has no assignment authority".into()))?;
        let processes = self
            .process_lease_authority
            .get()
            .ok_or_else(|| TopologyError::Protocol("restore has no process authority".into()))?;
        let input = if committed {
            authority
                .committed_topology_restore_input(assignments, processes, operation_id, before)
                .await?
        } else {
            authority
                .topology_restore_input(assignments, processes, operation_id, before)
                .await?
        };
        self.require_topology_restore_adoption(&input, before, committed)
            .await?;
        Ok(input)
    }

    async fn require_topology_restore_adoption(
        &self,
        input: &crate::cluster::control::TopologyRestoreInput,
        before: super::LocalProcessAuthorityIdentity,
        committed: bool,
    ) -> Result<(), TopologyError> {
        let evidence = self
            .read_local_process_authority_evidence()
            .await
            .map_err(|e| TopologyError::Conflict(e.to_string()))?;
        if (!committed && self.is_recovering())
            || self.is_draining()
            || self.try_live_local_process_authority_identity().ok() != Some(before)
            || evidence.participant != before.participant
            || evidence.process_term != before.process_term
            || !evidence
                .adopted_assignment
                .matches_fence(input.assignment())
            || self
                .checkpoint_assignment_fence(input.assignment().assignment_version)
                .as_ref()
                != Some(input.assignment())
        {
            return Err(TopologyError::Fenced);
        }
        Ok(())
    }

    /// Commit the exact privately restored target after runtime-owned parent retirement.
    /// This publishes the catalog/root decision only; installation and Release stay fenced.
    ///
    /// # Errors
    /// Rejects stale local adoption/leader, incomplete protocol-four roster or conflicting authority.
    /// An uncertain result must be resolved from this operation's durable status.
    pub async fn commit_topology_target(
        &self,
        input: &crate::cluster::control::TopologyRestoreInput,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        let proof = self.capture_leader_proof().ok_or(TopologyError::Fenced)?;
        if self.is_recovering()
            || self.is_draining()
            || !self.proof_is_live(&proof)
            || self.try_live_local_process_authority_identity().ok() != Some(input.process())
        {
            return Err(TopologyError::Fenced);
        }
        let evidence = self
            .read_local_process_authority_evidence()
            .await
            .map_err(|_| TopologyError::Fenced)?;
        if evidence.participant != input.process().participant
            || evidence.process_term != input.process().process_term
            || !evidence
                .adopted_assignment
                .matches_fence(input.assignment())
        {
            return Err(TopologyError::Fenced);
        }
        let status = self
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?
            .commit_topology_target(
                &proof,
                self.snapshot.as_ref().ok_or_else(|| {
                    TopologyError::Protocol("Commit has no assignment authority".into())
                })?,
                self.process_lease_authority.get().ok_or_else(|| {
                    TopologyError::Protocol("Commit has no process authority".into())
                })?,
                input,
            )
            .await?;
        if !self.proof_is_live(&proof)
            || self.try_live_local_process_authority_identity().ok() != Some(input.process())
        {
            return Err(TopologyError::Fenced);
        }
        Ok(status)
    }

    /// Pin restore requirements with this controller's actual assignment/process authority.
    /// This leaves the old cut held and grants no target execution or output authority.
    ///
    /// # Errors
    /// Rejects recovery, process/leader/assignment changes or invalid exact-cut metadata.
    pub async fn stage_topology_migration_root(
        &self,
        checkpoint_store: &dyn crate::checkpoint::CheckpointStore,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        self.stage_topology_migration_root_with_initialization(
            checkpoint_store,
            operation_id,
            expected_plan,
            |_, _| async {
                Err(TopologyError::Unsupported(
                    "new sources require connector initialization".into(),
                ))
            },
        )
        .await
    }

    /// Stage connector-owned initial positions under this controller's configured live fences.
    /// The DB supplies the read-only resolver; no caller-supplied cursor enters the public DB API.
    /// Existing sealed positions bypass the resolver, including after cancellation before append.
    ///
    /// # Errors
    /// Rejects the same process/assignment/recovery fences as downstream-only root staging.
    pub async fn stage_topology_migration_root_with_initialization<F, Fut>(
        &self,
        checkpoint_store: &dyn crate::checkpoint::CheckpointStore,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
        initialize: F,
    ) -> Result<TopologyAdmissionStatus, TopologyError>
    where
        F: FnOnce(
            crate::cluster::control::CatalogManifest,
            crate::cluster::control::ClusterTopologyValidation,
        ) -> Fut,
        Fut: std::future::Future<
            Output = Result<
                Vec<crate::cluster::control::TopologySourceInitialization>,
                TopologyError,
            >,
        >,
    {
        let before = self
            .try_live_local_process_authority_identity()
            .map_err(|_| TopologyError::Fenced)?;
        let proof = self.capture_leader_proof().ok_or(TopologyError::Fenced)?;
        if self.is_recovering() || self.is_draining() || !self.proof_is_live(&proof) {
            return Err(TopologyError::Fenced);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|e| TopologyError::Protocol(e.to_string()))?;
        let (_, plan, _, _) = authority.topology_preparation_input(operation_id).await?;
        let evidence = self
            .read_local_process_authority_evidence()
            .await
            .map_err(|e| TopologyError::Conflict(e.to_string()))?;
        if evidence.participant != before.participant
            || evidence.process_term != before.process_term
            || !evidence.adopted_assignment.matches_fence(&plan.assignment)
        {
            return Err(TopologyError::Fenced);
        }
        let status = authority
            .stage_topology_migration_root_with_initialization(
                &proof,
                self.snapshot.as_ref().ok_or_else(|| {
                    TopologyError::Protocol("root has no assignment authority".into())
                })?,
                self.process_lease_authority.get().ok_or_else(|| {
                    TopologyError::Protocol("root has no process authority".into())
                })?,
                checkpoint_store,
                operation_id,
                expected_plan,
                initialize,
            )
            .await?;
        if self.is_recovering()
            || self.is_draining()
            || !self.proof_is_live(&proof)
            || self.try_live_local_process_authority_identity().ok() != Some(before)
            || self
                .checkpoint_assignment_fence(plan.assignment.assignment_version)
                .as_ref()
                != Some(&plan.assignment)
        {
            return Err(TopologyError::Fenced);
        }
        Ok(status)
    }

    /// Publish this process's independently compiled descriptor under its configured namespace.
    /// The identity must have been sampled before compilation. This grants no actor readiness.
    ///
    /// # Errors
    /// Rejects process/assignment changes, recovery, divergence or bounded authority contention.
    pub async fn certify_topology_candidate(
        &self,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
        before: super::LocalProcessAuthorityIdentity,
        compiled: &crate::cluster::control::topology::ClusterTopologyValidation,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        if self.is_recovering()
            || self.is_draining()
            || self.try_live_local_process_authority_identity().ok() != Some(before)
        {
            return Err(TopologyError::Fenced);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?;
        let assignments = self.snapshot.as_ref().ok_or_else(|| {
            TopologyError::Protocol("preparation has no configured assignment authority".into())
        })?;
        let (_, plan, _, _) = authority.topology_preparation_input(operation_id).await?;
        let evidence = self
            .read_local_process_authority_evidence()
            .await
            .map_err(|error| TopologyError::Conflict(error.to_string()))?;
        if evidence.participant != before.participant
            || evidence.process_term != before.process_term
            || !evidence.adopted_assignment.matches_fence(&plan.assignment)
        {
            return Err(TopologyError::Fenced);
        }
        let status = authority
            .certify_topology_participant(
                assignments,
                self.process_lease_authority.get().ok_or_else(|| {
                    TopologyError::Protocol("process lease authority is not installed".into())
                })?,
                operation_id,
                expected_plan,
                before,
                plan.protocol_version,
                compiled,
            )
            .await?;
        if self.is_recovering()
            || self.is_draining()
            || self.try_live_local_process_authority_identity().ok() != Some(before)
            || self
                .checkpoint_assignment_fence(plan.assignment.assignment_version)
                .as_ref()
                != Some(&plan.assignment)
        {
            return Err(TopologyError::Fenced);
        }
        Ok(status)
    }

    /// Bind an old-topology cut using this controller's assignment store and exact live leader.
    /// No candidate runtime is authorized.
    ///
    /// # Errors
    /// Rejects missing configured authority, process/leader fencing or incompatible cut evidence.
    pub async fn begin_topology_checkpoint_cut(
        &self,
        proof: &LeaderProof,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
        inventory: CheckpointArtifactInventory,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        if !self.process_lease_is_live() || !self.proof_is_live(proof) {
            return Err(TopologyError::Fenced);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?;
        let assignments = self.snapshot.as_ref().ok_or_else(|| {
            TopologyError::Protocol("topology cut has no configured assignment store".into())
        })?;
        let status = authority
            .begin_topology_checkpoint_cut(
                proof,
                assignments,
                self.process_lease_authority.get().ok_or_else(|| {
                    TopologyError::Protocol("process lease authority is not installed".into())
                })?,
                operation_id,
                expected_plan,
                inventory,
            )
            .await?;
        if !self.process_lease_is_live() || !self.proof_is_live(proof) {
            return Err(TopologyError::Fenced);
        }
        Ok(status)
    }

    /// Report this exact process after its runtime-owned cut tail finishes.
    /// The leader settles aggregated external sinks before reporting; followers apply the local cut.
    /// The caller must keep intake and successor sink epochs held. This is not actor retirement.
    ///
    /// # Errors
    /// Rejects process/authority fencing, missing Commit, divergent binding or bounded contention.
    pub async fn complete_topology_checkpoint_cut(
        &self,
        proof: &LeaderProof,
        attempt: CheckpointAttempt,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        let before = self
            .try_live_local_process_authority_identity()
            .map_err(|_| TopologyError::Fenced)?;
        let authority = self
            .checkpoint_authority()
            .map_err(|error| TopologyError::Protocol(error.to_string()))?;
        let participant = before.participant;
        let status = authority
            .complete_topology_checkpoint_cut(proof, attempt, participant)
            .await?;
        if self
            .try_live_local_process_authority_identity()
            .ok()
            .as_ref()
            != Some(&before)
        {
            return Err(TopologyError::Fenced);
        }
        Ok(status)
    }
}
