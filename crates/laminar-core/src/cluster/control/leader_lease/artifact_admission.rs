use super::{
    CheckpointArtifactInventory, ClusterCheckpointAuthorityError, LeaderLeaseStore, LeaderProof,
};

impl LeaderLeaseStore {
    /// Read the unresolved cluster checkpoint artifact inventory, if any.
    ///
    /// # Errors
    /// Fails when the durable authority head is unavailable or invalid.
    pub async fn cluster_checkpoint_artifacts(
        &self,
    ) -> Result<Option<CheckpointArtifactInventory>, ClusterCheckpointAuthorityError> {
        Ok(self
            .cluster_checkpoint_artifact_admission()
            .await?
            .map(|(inventory, _)| inventory))
    }

    /// Read the unresolved cluster checkpoint artifact inventory and its admitting leader term.
    ///
    /// The returned proof identifies the term that admitted the inventory; callers must still
    /// certify that term against their current assignment before acting on it.
    ///
    /// # Errors
    /// Fails when the durable authority head is unavailable or invalid.
    pub async fn cluster_checkpoint_artifact_admission(
        &self,
    ) -> Result<Option<(CheckpointArtifactInventory, LeaderProof)>, ClusterCheckpointAuthorityError>
    {
        Ok(self.load_record().await?.and_then(|head| {
            head.active_checkpoint_artifacts
                .zip(head.active_checkpoint_artifact_leader_proof)
        }))
    }
}
