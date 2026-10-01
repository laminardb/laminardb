//! Old-topology checkpoint control with the controller's configured namespace and process gates.

use super::{ClusterController, LeaderProof};
use crate::checkpoint::CheckpointAttempt;
use crate::checkpoint_decision::CheckpointArtifactInventory;
use crate::cluster::control::topology::{
    TopologyAdmissionStatus, TopologyError, TopologyOperationId, TopologyPlanRef,
};

impl ClusterController {
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
