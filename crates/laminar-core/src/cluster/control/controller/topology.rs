//! Participant preparation and old-topology cuts through configured authority and process gates.

use super::{ClusterController, LeaderProof};
use crate::checkpoint::CheckpointAttempt;
use crate::checkpoint_decision::CheckpointArtifactInventory;
use crate::cluster::control::topology::{
    TopologyAdmissionStatus, TopologyError, TopologyOperationId, TopologyPlanRef,
};

impl ClusterController {
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
                crate::cluster::control::topology::TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
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
