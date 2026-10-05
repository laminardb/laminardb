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

impl LeaderLeaseStore {
    pub(super) async fn retain_topology_history(
        &self,
        head: &super::LeaderAuthorityRecord,
        retained: &mut std::collections::BTreeSet<u64>,
    ) -> Result<(), super::LeaseError> {
        if let Some(baseline) = head.topology_baseline.as_ref() {
            self.audit_topology_adoption(baseline).await?;
            retained.insert(baseline.authority_sequence);
        }
        for operation in &head.topology_operations {
            self.audit_topology_operation(operation).await?;
            retained.insert(operation.admitted_sequence);
            retained.insert(operation.status_sequence);
            retained.extend(
                operation
                    .target_preparations
                    .iter()
                    .map(|receipt| receipt.authority_sequence),
            );
            if let Some(commit) = &operation.commit {
                retained.insert(commit.authority_sequence);
            }
            if let Some(activation) = &operation.activation {
                retained.insert(activation.authority_sequence);
                retained.extend(
                    activation
                        .installations
                        .iter()
                        .map(|receipt| receipt.authority_sequence),
                );
                if let Some(release) = &activation.release {
                    retained.insert(release.authority_sequence);
                }
            }
            if let Some(root) = &operation.migration_root {
                retained.insert(root.authority_sequence);
            }
            if let Some(preparation) = &operation.preparation {
                retained.extend(
                    preparation
                        .certificates
                        .iter()
                        .map(|certificate| certificate.authority_sequence),
                );
            }
            if let Some(cut) = &operation.cut {
                retained.insert(cut.bound_sequence);
                if let Some(commit) = &cut.committed {
                    retained.insert(commit.authority_sequence);
                }
            }
        }
        Ok(())
    }
}

impl super::LeaderAuthorityRecord {
    pub(super) fn cleanup_is_pinned(&self, protected: &CommittedCheckpointRef) -> bool {
        if self.assignment_handoff_pin.as_ref().is_some_and(|pin| {
            protected.epoch > pin.checkpoint.epoch
                && protected.checkpoint_id > pin.checkpoint.checkpoint_id
        }) {
            return true;
        }
        if self.topology_cut_blocks_cleanup(protected) {
            return true;
        }
        if self
            .outcome_floor
            .as_ref()
            .is_some_and(|floor| floor.artifact_before_epoch >= protected.epoch)
        {
            return true;
        }
        false
    }
}

impl LeaderLeaseStore {
    pub(super) async fn audit_topology_cleanup_roots(&self) -> Result<(), super::DecisionError> {
        Box::pin(self.retained_topology_checkpoints())
            .await
            .map_err(|error| super::DecisionError::Conflict(error.to_string()))?;
        Ok(())
    }
}
