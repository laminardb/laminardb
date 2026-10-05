//! Explicit committed-root reconstruction, retaining historical checkpoint identities.

use super::topology_admission::{topology_assignment_error, CONTROL_TIMEOUT};
use super::topology_restore::validate_parent_cut;
use super::{AssignmentSnapshotStore, LeaderLeaseStore, LeaseError};
use crate::cluster::control::{
    LocalProcessAuthorityIdentity, ProcessLeaseAuthority, TopologyError, TopologyOperationId,
    TopologyRestoreInput,
};

impl LeaderLeaseStore {
    /// Authorize private reconstruction of the current committed target before its first checkpoint.
    /// The exact immutable migration root is the only authorized parent cut. Replacement leaders
    /// and new process boots can reconstruct with the same vnode owner map and stable roster;
    /// ownership changes/rescaling remain rejected. This never authorizes actors or intake.
    ///
    /// # Errors
    /// Rejects an uncommitted/aborted/obsolete target, stale current process/assignment, changed
    /// vnode ownership or damaged retained evidence. Reads/rechecks have a total 15 second budget.
    pub async fn committed_topology_restore_input(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        operation_id: TopologyOperationId,
        process: LocalProcessAuthorityIdentity,
    ) -> Result<TopologyRestoreInput, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let current = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            let operation = current
                .committed_topology_operation()
                .filter(|operation| {
                    operation.operation_id == operation_id && operation.has_target_commit()
                })
                .ok_or_else(|| {
                    TopologyError::Conflict(
                        "reconstruction requires the current committed target".into(),
                    )
                })?;
            self.audit_topology_operation(operation).await?;
            let plan = self.load_topology_plan(&operation.plan).await?;
            let descriptor = self
                .audit_topology_compatibility(&plan)
                .await?
                .ok_or_else(|| {
                    TopologyError::Invalid(
                        "committed target has no compatibility descriptor".into(),
                    )
                })?;
            self.require_topology_deployment(&descriptor.deployment_id)
                .await?;
            let assignment = assignments
                .load()
                .await
                .map_err(topology_assignment_error)?
                .ok_or(TopologyError::Fenced)?;
            let fence = assignment
                .assignment_fence()
                .map_err(|error| TopologyError::Invalid(error.to_string()))?;
            if assignment.draining
                || fence.assignment_digest != plan.assignment.assignment_digest
                || fence.vnode_count != plan.assignment.vnode_count
                || fence.partitioning_abi_version != plan.assignment.partitioning_abi_version
                || fence.assignment_version < plan.assignment.assignment_version
                || !fence
                    .participants
                    .iter()
                    .map(|participant| participant.node_id)
                    .eq(plan
                        .assignment
                        .participants
                        .iter()
                        .map(|participant| participant.node_id))
                || fence.participant_incarnation(process.participant.node_id)
                    != Some(process.participant.boot_incarnation)
            {
                return Err(TopologyError::Fenced);
            }
            self.require_topology_process(processes, process).await?;
            let mut restore_processes = Vec::with_capacity(fence.participants.len());
            for participant in &fence.participants {
                let lease = processes
                    .store_for(super::NodeId(participant.node_id))
                    .load()
                    .await
                    .map_err(|error| TopologyError::Authority(LeaseError::Io(error.to_string())))?
                    .ok_or(TopologyError::Fenced)?;
                let identity = LocalProcessAuthorityIdentity {
                    participant: *participant,
                    process_term: lease.term,
                };
                self.require_topology_process(processes, identity).await?;
                restore_processes.push(identity);
            }
            let binding = operation
                .migration_root
                .as_ref()
                .ok_or_else(|| TopologyError::Invalid("committed target has no root".into()))?;
            let root = self.load_topology_root(&binding.root).await?;
            let (outcome, checkpoint) = self
                .load_retained_topology_root_checkpoint(operation)
                .await?;
            validate_parent_cut(&outcome, &checkpoint, &root, &plan, &descriptor)?;
            let parent = self.load_catalog_manifest(&plan.parent_manifest).await?;
            let target = self.load_catalog_manifest(&plan.target_manifest).await?;
            for identity in &restore_processes {
                self.require_topology_process(processes, *identity).await?;
            }
            self.require_topology_process(processes, process).await?;
            let after = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            if after.lease.proof() != current.lease.proof()
                || after.committed_topology_operation() != Some(operation)
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
                plan,
                parent,
                target,
                descriptor,
                root,
                outcome,
                checkpoint,
                owned_vnodes,
                process,
                restore_assignment: fence,
                restore_processes,
                committed_leader: Some(current.lease.proof()),
            })
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }
}
