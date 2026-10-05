//! Read-only authorization for private target restore preparation under the held parent cut.

use super::topology_admission::{
    topology_assignment_error, topology_checkpoint_error, CONTROL_TIMEOUT,
};
use super::topology_migration_root::validate_manifest_budget;
use super::{AssignmentSnapshotStore, LeaderLeaseStore};
use crate::cluster::control::{
    LocalProcessAuthorityIdentity, ProcessLeaseAuthority, TopologyAdmissionPhase, TopologyError,
    TopologyOperationId, TopologyRestoreInput,
};

impl LeaderLeaseStore {
    /// Read exact target restore input for a frozen, currently certified participant.
    /// Call through the controller with its configured authorities. This grants only private
    /// state preparation and never source start, actor installation, output or intake release.
    ///
    /// # Errors
    /// Requires `CutPrepared`, a published root, the admitting leader, complete current process
    /// certificates and unchanged assignment. Rejects damaged evidence or a 15 second deadline.
    pub async fn topology_restore_input(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        operation_id: TopologyOperationId,
        process: LocalProcessAuthorityIdentity,
    ) -> Result<TopologyRestoreInput, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let current = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            let operation = current
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == operation_id)
                .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
            if operation.phase != TopologyAdmissionPhase::CutPrepared {
                return Err(TopologyError::Conflict(
                    "target restore requires the held CutPrepared operation".into(),
                ));
            }
            if !current.lease.matches_proof(&operation.admitted_by) {
                return Err(TopologyError::Fenced);
            }
            self.audit_topology_operation(operation).await?;
            let plan = self.load_topology_plan(&operation.plan).await?;
            let descriptor = self
                .require_topology_prepared(operation, &plan, processes)
                .await?;
            if operation.preparation.as_ref().is_none_or(|preparation| {
                !preparation.certificates.iter().any(|certificate| {
                    certificate.participant == process.participant
                        && certificate.process_term == process.process_term
                })
            }) {
                return Err(TopologyError::Fenced);
            }
            self.require_topology_process(processes, process).await?;
            let assignment = assignments
                .load()
                .await
                .map_err(topology_assignment_error)?
                .ok_or(TopologyError::Fenced)?;
            if assignment.draining
                || assignment
                    .assignment_fence()
                    .map_err(|e| TopologyError::Invalid(e.to_string()))?
                    != plan.assignment
            {
                return Err(TopologyError::Fenced);
            }
            self.reject_consumed_checkpoint_assignment(&current, &plan.assignment)
                .await
                .map_err(topology_checkpoint_error)?;
            let binding = operation.migration_root.as_ref().ok_or_else(|| {
                TopologyError::Conflict(
                    "target restore requires the published migration root".into(),
                )
            })?;
            let root = self.load_topology_root(&binding.root).await?;
            let (outcome, checkpoint) = self
                .cluster_outcome_with_committed_checkpoint(root.cut.checkpoint.epoch)
                .await
                .map_err(topology_checkpoint_error)?
                .ok_or_else(|| TopologyError::Invalid("restore cut outcome is missing".into()))?;
            let checkpoint = checkpoint
                .ok_or_else(|| TopologyError::Invalid("restore cut is not committed".into()))?;
            validate_parent_cut(&outcome, &checkpoint, &root, &plan, &descriptor)?;
            let parent = self.load_catalog_manifest(&plan.parent_manifest).await?;
            let target = self.load_catalog_manifest(&plan.target_manifest).await?;
            // Renewals and target preparation receipts can advance while this read runs.
            // Restore requirements, leader and assignment must stay exact. This writes no receipt.
            self.require_topology_prepared(operation, &plan, processes)
                .await?;
            let after = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            if !after.lease.matches_proof(&operation.admitted_by)
                || after
                    .topology_operations
                    .iter()
                    .find(|entry| entry.operation_id == operation_id)
                    .is_none_or(|entry| !operation.same_restore_binding(entry))
                || assignments
                    .load()
                    .await
                    .map_err(topology_assignment_error)?
                    .as_ref()
                    != Some(&assignment)
            {
                return Err(TopologyError::Fenced);
            }
            let owned_vnodes = assignment
                .vnodes
                .iter()
                .filter_map(|(vnode, owner)| {
                    (owner.0 == process.participant.node_id).then_some(*vnode)
                })
                .collect();
            Ok(TopologyRestoreInput {
                operation: operation.clone(),
                restore_assignment: plan.assignment.clone(),
                restore_processes: Vec::new(),
                committed_leader: None,
                plan,
                parent,
                target,
                descriptor,
                root,
                outcome,
                checkpoint,
                owned_vnodes,
                process,
            })
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }
}

pub(super) fn validate_parent_cut(
    outcome: &crate::checkpoint_decision::CheckpointOutcome,
    checkpoint: &crate::checkpoint::CommittedCheckpointIndex,
    root: &crate::cluster::control::TopologyMigrationRoot,
    plan: &crate::cluster::control::TopologyAdmissionPlan,
    descriptor: &crate::cluster::control::ClusterTopologyValidation,
) -> Result<(), TopologyError> {
    if !outcome.is_commit()
        || outcome.committed_checkpoint.as_ref() != Some(&root.cut.checkpoint)
        || checkpoint.pipeline_identity != descriptor.parent_pipeline
        || checkpoint.deployment_id != descriptor.deployment_id
        || checkpoint.assignment_fence.as_ref() != Some(&plan.assignment)
    {
        return Err(TopologyError::Invalid(
            "topology restore differs from the exact historical parent cut".into(),
        ));
    }
    validate_manifest_budget(checkpoint)
}
