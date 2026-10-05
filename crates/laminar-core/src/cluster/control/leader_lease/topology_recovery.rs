//! Exact target checkpoint selection; damaged newer progress never falls back to a parent root.

use super::topology_admission::{topology_checkpoint_error, CONTROL_TIMEOUT};
use super::topology_migration_root::validate_manifest_budget;
use super::{AssignmentSnapshotStore, LeaderLeaseStore};
use crate::checkpoint::CheckpointScope;
use crate::cluster::control::{
    LocalProcessAuthorityIdentity, ProcessLeaseAuthority, TopologyAdmissionPhase, TopologyError,
    TopologyOperationId, TopologyRecoveryInput,
};

impl LeaderLeaseStore {
    /// Select the current target's greatest committed checkpoint, or its immutable migration root
    /// when no target checkpoint has committed. Replacement boots must retain the owner map and
    /// stable roster. This permits private reads only, never actors, sink epochs or intake.
    ///
    /// # Errors
    /// Rejects stale process/assignment/leader, an obsolete target, damaged retained evidence or
    /// any newer Commit that cannot be proved to belong to the target. No older-cut fallback is
    /// permitted. The complete selection and authority recheck share a 15 second budget.
    pub async fn committed_topology_recovery_input(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        operation_id: TopologyOperationId,
        process: LocalProcessAuthorityIdentity,
    ) -> Result<TopologyRecoveryInput, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let before = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            let migration = self
                .committed_topology_restore_input(assignments, processes, operation_id, process)
                .await?;
            let operation = migration.operation();
            let commit = operation.commit.as_ref().ok_or(TopologyError::Fenced)?;
            if before.committed_topology_operation() != Some(operation)
                || Some(before.lease.proof()) != migration.current_leader()
            {
                return Err(TopologyError::Fenced);
            }
            let head = before.commit_head.ok_or_else(|| {
                TopologyError::Invalid("committed topology has no checkpoint Commit head".into())
            })?;
            let root = &migration.root().cut;
            let target_checkpoint = if head.sequence == root.authority_sequence {
                if head.epoch != root.checkpoint.epoch
                    || head.checkpoint_id != root.checkpoint.checkpoint_id
                {
                    return Err(TopologyError::Invalid(
                        "migration root differs from its current checkpoint Commit head".into(),
                    ));
                }
                None
            } else {
                if head.sequence <= commit.authority_sequence
                    || head.epoch <= root.checkpoint.epoch
                    || operation.phase != TopologyAdmissionPhase::Active
                {
                    return Err(TopologyError::Invalid(
                        "newer checkpoint Commit is not authorized target progress".into(),
                    ));
                }
                let (outcome, checkpoint) = self
                    .cluster_outcome_with_committed_checkpoint(head.epoch)
                    .await
                    .map_err(topology_checkpoint_error)?
                    .ok_or_else(|| {
                        TopologyError::Invalid(
                            "selected target checkpoint outcome is missing".into(),
                        )
                    })?;
                let checkpoint = checkpoint.ok_or_else(|| {
                    TopologyError::Invalid("selected target checkpoint is not committed".into())
                })?;
                let assignment = checkpoint.assignment_fence.as_ref().ok_or_else(|| {
                    TopologyError::Invalid("target checkpoint has no assignment certificate".into())
                })?;
                let current = migration.assignment();
                if !outcome.is_commit()
                    || outcome.epoch != head.epoch
                    || outcome.checkpoint_id != head.checkpoint_id
                    || checkpoint.pipeline_identity != migration.descriptor().target_pipeline
                    || checkpoint.deployment_id != migration.descriptor().deployment_id
                    || checkpoint.scope != CheckpointScope::Cluster
                    || assignment.assignment_version
                        < migration.plan().assignment.assignment_version
                    || assignment.assignment_version > current.assignment_version
                    || assignment.assignment_digest != current.assignment_digest
                    || assignment.vnode_count != current.vnode_count
                    || assignment.partitioning_abi_version != current.partitioning_abi_version
                    || !assignment
                        .participants
                        .iter()
                        .map(|participant| participant.node_id)
                        .eq(current
                            .participants
                            .iter()
                            .map(|participant| participant.node_id))
                {
                    return Err(TopologyError::Invalid(
                        "selected newer checkpoint differs from the committed target/owner map"
                            .into(),
                    ));
                }
                validate_manifest_budget(&checkpoint)?;
                Some(Box::new((outcome, checkpoint)))
            };
            let fresh = self
                .committed_topology_restore_input(assignments, processes, operation_id, process)
                .await?;
            let after = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            if !fresh.same_restore_requirements(&migration)
                || before.commit_head != after.commit_head
                || before.lease.proof() != after.lease.proof()
                || after.committed_topology_operation() != Some(operation)
            {
                return Err(TopologyError::Fenced);
            }
            Ok(TopologyRecoveryInput {
                migration,
                target_checkpoint,
            })
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }
}
