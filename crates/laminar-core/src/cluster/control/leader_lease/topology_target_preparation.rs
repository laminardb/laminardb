//! Exact-root target preparation observations use the existing authority append.

use super::topology_admission::{CONTROL_TIMEOUT, MAX_ADMISSION_ATTEMPTS};
use super::{
    read_authority_record, AssignmentSnapshotStore, AuthorityCreateOutcome, LeaderLeaseStore,
    LeaseError, TOPOLOGY_COMMIT_RECORD_VERSION, TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION,
};
use crate::cluster::control::{
    ProcessLeaseAuthority, TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyError,
    TopologyRestoreInput, TopologyTargetPreparationReceipt, TOPOLOGY_COMMIT_PROTOCOL_VERSION,
    TOPOLOGY_TARGET_PREPARATION_PROTOCOL_VERSION,
};

impl LeaderLeaseStore {
    /// Record one participant after it restored the exact private target and observed its old
    /// compute/source/sink actors and connector children terminal. The runtime owns that proof;
    /// call through its configured controller. This historical observation grants no install,
    /// Commit, output or intake release, and does not certify that an image remains resident.
    ///
    /// # Errors
    /// Rejects missing root/cut, changed requirements, old protocol, stale leader/process/assignment
    /// or recovery. The budget is 16 CAS attempts within 15 seconds. A cancelled/uncertain write
    /// may have appended: read operation status and retry the same input; never invent a new ID.
    pub async fn certify_topology_target_preparation(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        input: &TopologyRestoreInput,
        protocol_version: u16,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        if !matches!(
            protocol_version,
            TOPOLOGY_TARGET_PREPARATION_PROTOCOL_VERSION | TOPOLOGY_COMMIT_PROTOCOL_VERSION
        ) {
            return Err(TopologyError::Protocol(
                "participant lacks target preparation protocol three".into(),
            ));
        }
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            for _ in 0..MAX_ADMISSION_ATTEMPTS {
                let published = self
                    .load_published_authority_head()
                    .await?
                    .ok_or(TopologyError::Fenced)?;
                let current = &published.record;
                let index = current
                    .topology_operations
                    .iter()
                    .position(|entry| entry.operation_id == input.operation().operation_id)
                    .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
                let operation = &current.topology_operations[index];
                if operation.phase != TopologyAdmissionPhase::CutPrepared
                    || !current.lease.matches_proof(&operation.admitted_by)
                    || !operation.same_restore_binding(input.operation())
                {
                    return Err(TopologyError::Fenced);
                }
                let fresh = self
                    .topology_restore_input(
                        assignments,
                        processes,
                        operation.operation_id,
                        input.process(),
                    )
                    .await?;
                if !fresh.same_restore_requirements(input) {
                    return Err(TopologyError::Fenced);
                }
                if let Some(existing) =
                    fresh
                        .operation()
                        .target_preparations
                        .iter()
                        .find(|receipt| {
                            receipt.participant.node_id == input.process().participant.node_id
                        })
                {
                    if existing.participant != input.process().participant
                        || existing.process_term != input.process().process_term
                        || existing.protocol_version != protocol_version
                    {
                        return Err(TopologyError::Fenced);
                    }
                    return Ok(fresh.operation().clone());
                }
                self.require_topology_process(processes, input.process())
                    .await?;
                let mut lease = current.lease.clone();
                lease.seq = lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                next.version =
                    next.version
                        .max(if protocol_version == TOPOLOGY_COMMIT_PROTOCOL_VERSION {
                            TOPOLOGY_COMMIT_RECORD_VERSION
                        } else {
                            TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION
                        });
                let operation = &mut next.topology_operations[index];
                operation.status_sequence = sequence;
                operation
                    .target_preparations
                    .push(TopologyTargetPreparationReceipt {
                        participant: input.process().participant,
                        process_term: input.process().process_term,
                        protocol_version,
                        authority_sequence: sequence,
                    });
                operation
                    .target_preparations
                    .sort_by_key(|receipt| receipt.participant.node_id);
                match self
                    .create_authority_record(Some(&published), &next)
                    .await?
                {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                        let after = self
                            .topology_restore_input(
                                assignments,
                                processes,
                                input.operation().operation_id,
                                input.process(),
                            )
                            .await?;
                        if !after.same_restore_requirements(input) {
                            return Err(TopologyError::Fenced);
                        }
                        return Ok(after.operation().clone());
                    }
                    AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
                }
            }
            Err(TopologyError::Contended)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    pub(super) async fn audit_topology_target_preparations(
        &self,
        operation: &TopologyAdmissionStatus,
    ) -> Result<(), LeaseError> {
        for receipt in &operation.target_preparations {
            let record = read_authority_record(self.store.as_ref(), receipt.authority_sequence)
                .await?
                .ok_or_else(|| {
                    LeaseError::Invalid("target preparation authority anchor is missing".into())
                })?;
            if record.version < TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION
                || (receipt.protocol_version == TOPOLOGY_COMMIT_PROTOCOL_VERSION
                    && record.version < TOPOLOGY_COMMIT_RECORD_VERSION)
                || record
                    .topology_operations
                    .iter()
                    .find(|entry| entry.operation_id == operation.operation_id)
                    .is_none_or(|anchored| {
                        // An abort retains the historical prepared observation under the old term.
                        anchored.phase != TopologyAdmissionPhase::CutPrepared
                            || anchored.operation_id != operation.operation_id
                            || anchored.plan != operation.plan
                            || anchored.admitted_by != operation.admitted_by
                            || anchored.admitted_sequence != operation.admitted_sequence
                            || anchored.cut != operation.cut
                            || anchored.preparation != operation.preparation
                            || anchored.migration_root != operation.migration_root
                            || anchored.status_sequence != receipt.authority_sequence
                            || !anchored.target_preparations.contains(receipt)
                    })
            {
                return Err(LeaseError::Invalid(
                    "target preparation differs from its immutable authority append".into(),
                ));
            }
        }
        Ok(())
    }
}
