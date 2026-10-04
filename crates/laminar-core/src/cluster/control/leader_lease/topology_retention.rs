//! Read exact retained root metadata without reviving ordinary expired checkpoint authority.

use super::topology_admission::{topology_checkpoint_error, CONTROL_TIMEOUT};
use super::{read_authority_record, CheckpointDecisionStore, LeaderLeaseStore};
use crate::checkpoint::{CommittedCheckpointIndex, CommittedCheckpointRef};
use crate::checkpoint_decision::CheckpointOutcome;
use crate::cluster::control::{TopologyAdmissionStatus, TopologyError};

impl LeaderLeaseStore {
    /// Return the exact checkpoint roots still pinned by irreversible topology decisions.
    /// The operation journal bounds this inventory. These references protect state and replay;
    /// they grant neither assignment authority, actor installation nor input Release.
    ///
    /// # Errors
    /// Rejects missing/corrupt retained evidence, a changed topology/proof or the 15-second budget.
    pub async fn retained_topology_checkpoints(
        &self,
    ) -> Result<Vec<CommittedCheckpointRef>, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let before = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            let mut roots = Vec::new();
            for operation in before
                .topology_operations
                .iter()
                .filter(|operation| operation.has_target_commit())
            {
                Box::pin(self.audit_topology_operation(operation)).await?;
                let (outcome, _) =
                    Box::pin(self.load_retained_topology_root_checkpoint(operation)).await?;
                roots.push(outcome.committed_checkpoint.ok_or(TopologyError::Fenced)?);
            }
            let after = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            if before.lease.proof() != after.lease.proof()
                || before.topology_operations != after.topology_operations
            {
                return Err(TopologyError::Fenced);
            }
            roots.sort_by_key(|reference| (reference.epoch, reference.checkpoint_id));
            roots.dedup();
            Ok(roots)
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    /// The caller audits the retained operation first. Its immutable cut Commit is the sole
    /// historical exception to the ordinary artifact floor; GC keeps this exact metadata pinned.
    pub(super) async fn load_retained_topology_root_checkpoint(
        &self,
        operation: &TopologyAdmissionStatus,
    ) -> Result<(CheckpointOutcome, CommittedCheckpointIndex), TopologyError> {
        if !operation.has_target_commit() {
            return Err(TopologyError::Fenced);
        }
        let cut = operation.cut.as_ref().ok_or(TopologyError::Fenced)?;
        let commit = cut.committed.as_ref().ok_or(TopologyError::Fenced)?;
        let record = read_authority_record(self.store.as_ref(), commit.authority_sequence)
            .await?
            .ok_or_else(|| {
                TopologyError::Invalid("retained root Commit anchor is missing".into())
            })?;
        let outcome = record.checkpoint_outcome.ok_or(TopologyError::Fenced)?;
        if !outcome.is_commit()
            || outcome.committed_checkpoint.as_ref() != Some(&commit.checkpoint)
            || outcome.assignment_fence != cut.inventory.assignment_fence
            || outcome.deployment_id != cut.inventory.deployment_id
        {
            return Err(TopologyError::Invalid(
                "retained root differs from its exact immutable Commit".into(),
            ));
        }
        let index = CheckpointDecisionStore::new(self.store.clone())
            .validate_committed_checkpoint_for_outcome(&outcome)
            .await
            .map_err(|error| topology_checkpoint_error(error.into()))?;
        if index.pipeline_identity != cut.inventory.pipeline_identity {
            return Err(TopologyError::Invalid(
                "retained root differs from its historical pipeline identity".into(),
            ));
        }
        Ok((outcome, index))
    }
}
