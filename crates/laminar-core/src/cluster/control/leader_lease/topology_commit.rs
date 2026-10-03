//! Catalog Commit shares the existing authority append with the exact retained migration root.

use super::topology_admission::{CONTROL_TIMEOUT, MAX_ADMISSION_ATTEMPTS};
use super::{
    read_authority_record, AssignmentSnapshotStore, AuthorityCreateOutcome,
    ClusterCheckpointAuthorityError, LeaderAuthorityRecord, LeaderLeaseStore, LeaseError,
    RecoveryFaultDisposition, TOPOLOGY_COMMIT_RECORD_VERSION,
};
use crate::checkpoint::LeaderProof;
use crate::checkpoint_decision::DecisionError;
use crate::cluster::control::{
    CatalogManifestRef, LegacyTopologyBaseline, ProcessLeaseAuthority, TopologyAdmissionPhase,
    TopologyAdmissionPlan, TopologyAdmissionStatus, TopologyCommit, TopologyError,
    TopologyRestoreInput, TopologyVersion, TOPOLOGY_COMMIT_PROTOCOL_VERSION,
};

impl LeaderAuthorityRecord {
    pub(super) fn committed_topology_operation(&self) -> Option<&TopologyAdmissionStatus> {
        self.topology_operations
            .iter()
            .rev()
            .find(|operation| operation.commit.is_some())
    }

    pub(super) fn committed_topology_identity(
        &self,
    ) -> Option<(TopologyVersion, &CatalogManifestRef)> {
        self.committed_topology_operation()
            .and_then(|operation| operation.commit.as_ref())
            .map(|commit| (commit.topology_version, &commit.manifest))
            .or_else(|| {
                self.topology_baseline
                    .as_ref()
                    .map(|baseline| (baseline.topology_version, &baseline.manifest))
            })
    }

    pub(super) fn validate_committed_topology_chain(
        &self,
        baseline: &LegacyTopologyBaseline,
    ) -> Result<(), LeaseError> {
        let mut version = baseline.topology_version;
        let mut manifest = &baseline.manifest;
        let mut sequence = baseline.authority_sequence;
        for operation in &self.topology_operations {
            let Some(commit) = &operation.commit else {
                continue;
            };
            commit
                .validate()
                .map_err(|error| LeaseError::Invalid(error.to_string()))?;
            if self.version < TOPOLOGY_COMMIT_RECORD_VERSION
                || commit.parent_version != version
                || &commit.parent_manifest != manifest
                || commit.authority_sequence <= sequence
                || commit.authority_sequence > self.lease.seq
            {
                return Err(LeaseError::Invalid(
                    "committed topology chain differs from its exact predecessor".into(),
                ));
            }
            version = commit.topology_version;
            manifest = &commit.manifest;
            sequence = commit.authority_sequence;
        }
        if self.lease.catalog_manifest.as_ref() != Some(manifest) {
            return Err(LeaseError::Invalid(
                "catalog head differs from its committed topology decision".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn validate_committed_topology_successor(
        &self,
        next: &Self,
    ) -> Result<(), LeaseError> {
        let prior = self
            .committed_topology_operation()
            .and_then(|operation| operation.commit.as_ref());
        let after = next
            .committed_topology_operation()
            .and_then(|operation| operation.commit.as_ref());
        if prior == after {
            // Initial legacy seal is the only inventory change without a topology Commit.
            if self.lease.catalog_manifest.is_some()
                && self.lease.catalog_manifest != next.lease.catalog_manifest
            {
                return Err(LeaseError::Invalid(
                    "catalog replacement requires an atomic topology Commit".into(),
                ));
            }
            return Ok(());
        }
        let commit = after.ok_or_else(|| {
            LeaseError::Invalid("authority cannot forget a topology Commit".into())
        })?;
        let operation = next
            .committed_topology_operation()
            .ok_or_else(|| LeaseError::Invalid("Commit operation is missing".into()))?;
        if next.version < TOPOLOGY_COMMIT_RECORD_VERSION
            || self.committed_topology_identity()
                != Some((commit.parent_version, &commit.parent_manifest))
            || next.lease.catalog_manifest.as_ref() != Some(&commit.manifest)
            || commit.authority_sequence != next.lease.seq
            || !next.lease.matches_proof(&operation.admitted_by)
            || self
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == operation.operation_id)
                .is_none_or(|entry| {
                    entry.phase != TopologyAdmissionPhase::CutPrepared || entry.commit.is_some()
                })
        {
            return Err(LeaseError::Invalid(
                "new catalog and target Commit must share their exact authority append".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn reject_pending_topology_commit(
        &self,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        if self.topology_operations.iter().any(|operation| {
            operation.has_target_commit()
                && (operation.phase != TopologyAdmissionPhase::Active
                    || self.commit_head.as_ref().is_none_or(|head| {
                        operation
                            .cut
                            .as_ref()
                            .and_then(|cut| cut.committed.as_ref())
                            .is_none_or(|cut| head.sequence <= cut.authority_sequence)
                    }))
        }) {
            return Err(DecisionError::Conflict("committed topology requires target installation and Release; parent recovery cannot release intake".into()).into());
        }
        Ok(())
    }
}

impl LeaderLeaseStore {
    /// Atomically change the catalog to the exact restored/retired candidate and pin its root.
    /// Every frozen process must have protocol-four preparation evidence and still hold its
    /// original process/assignment fence. This decision is irreversible; it never opens intake.
    /// Call through the configured controller after runtime-owned terminal observation.
    ///
    /// # Errors
    /// Rejects stale leader/process/assignment, incomplete capabilities, changed image/root,
    /// unresolved checkpoint/recovery/cleanup or 16 CAS attempts within 15 seconds. A cancelled
    /// or uncertain write may have committed: query the same operation, never roll back or retry
    /// with a new identity. Explicit committed-root reconstruction remains available afterward.
    pub async fn commit_topology_target(
        &self,
        proof: &LeaderProof,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        input: &TopologyRestoreInput,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            for _ in 0..MAX_ADMISSION_ATTEMPTS {
                let published = self.load_published_authority_head().await?.ok_or(TopologyError::Fenced)?;
                let current = &published.record;
                if !proof.is_canonical() || !current.lease.matches_proof(proof)
                    || proof.owner.node_id != input.process().participant.node_id
                    || proof.owner.boot_id != input.process().participant.boot_incarnation {
                    return Err(TopologyError::Fenced);
                }
                let index = current.topology_operations.iter().position(|entry| entry.operation_id == input.operation().operation_id)
                    .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
                let operation = &current.topology_operations[index];
                if operation.has_target_commit() {
                    let fresh = self.committed_topology_restore_input(assignments, processes, operation.operation_id, input.process()).await?;
                    if !fresh.is_committed_successor_of(input) {
                        return Err(TopologyError::Fenced);
                    }
                    return Ok(fresh.operation().clone());
                }
                if operation.phase != TopologyAdmissionPhase::CutPrepared || operation.admitted_by != *proof
                    || !operation.same_restore_binding(input.operation()) {
                    return Err(TopologyError::Fenced);
                }
                if !operation.target_preparation_complete() || operation.target_preparations.iter().any(|receipt| receipt.protocol_version != TOPOLOGY_COMMIT_PROTOCOL_VERSION) {
                    return Err(TopologyError::Protocol("Commit requires protocol four from every frozen owner/evidence process; protocol-three observations cannot be upgraded in place".into()));
                }
                if current.committed_topology_identity() != Some((input.plan().expected_parent, &input.plan().parent_manifest))
                    || current.active_checkpoint_artifacts.is_some() || current.artifact_cleanup.is_some()
                    || current.assignment_drain_reservation.is_some() || current.assignment_handoff_pin.is_some()
                    || current.recovery_fault_slots.iter().any(|slot| slot.active || slot.disposition == RecoveryFaultDisposition::Terminal)
                    || current.commit_head.as_ref().is_none_or(|head| head.sequence != input.root().cut.authority_sequence) {
                    return Err(TopologyError::Conflict("Commit requires the exact held parent and settled cut without competing authority".into()));
                }
                let fresh = self.topology_restore_input(assignments, processes, operation.operation_id, input.process()).await?;
                if !fresh.same_restore_requirements(input) { return Err(TopologyError::Fenced); }
                let mut lease = current.lease.clone();
                lease.seq = lease.seq.checked_add(1).ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                lease.catalog_manifest = Some(input.plan().target_manifest.clone());
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                next.version = next.version.max(TOPOLOGY_COMMIT_RECORD_VERSION);
                let operation = &mut next.topology_operations[index];
                operation.phase = TopologyAdmissionPhase::Committed;
                operation.status_sequence = sequence;
                operation.commit = Some(TopologyCommit {
                    protocol_version: TOPOLOGY_COMMIT_PROTOCOL_VERSION,
                    operation_id: operation.operation_id,
                    parent_version: input.plan().expected_parent,
                    parent_manifest: input.plan().parent_manifest.clone(),
                    topology_version: input.descriptor().target_version,
                    manifest: input.plan().target_manifest.clone(),
                    authority_sequence: sequence,
                });
                match self.create_authority_record(Some(&published), &next).await? {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                        let fresh = self.committed_topology_restore_input(assignments, processes, input.operation().operation_id, input.process()).await?;
                        if !fresh.is_committed_successor_of(input) { return Err(TopologyError::Fenced); }
                        return Ok(fresh.operation().clone());
                    }
                    AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
                }
            }
            Err(TopologyError::Contended)
        }).await.map_err(|_| TopologyError::Contended)?
    }

    pub(super) async fn audit_topology_commit(
        &self,
        operation: &TopologyAdmissionStatus,
        plan: &TopologyAdmissionPlan,
    ) -> Result<(), LeaseError> {
        let Some(commit) = &operation.commit else {
            return Ok(());
        };
        let record = read_authority_record(self.store.as_ref(), commit.authority_sequence)
            .await?
            .ok_or_else(|| {
                LeaseError::Invalid("topology Commit authority anchor is missing".into())
            })?;
        if commit.parent_version != plan.expected_parent
            || commit.parent_manifest != plan.parent_manifest
            || commit.manifest != plan.target_manifest
            || record.version < TOPOLOGY_COMMIT_RECORD_VERSION
            || record.lease.catalog_manifest.as_ref() != Some(&commit.manifest)
            || !record.lease.matches_proof(&operation.admitted_by)
            || record
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == operation.operation_id)
                .is_none_or(|anchored| {
                    anchored.phase != TopologyAdmissionPhase::Committed
                        || anchored.commit != operation.commit
                        || !anchored.same_migration_binding(operation)
                        || anchored.target_preparations != operation.target_preparations
                        || anchored.activation.is_some()
                        || anchored.status_sequence != commit.authority_sequence
                })
        {
            return Err(LeaseError::Invalid(
                "topology Commit differs from its atomic catalog/root append".into(),
            ));
        }
        Ok(())
    }
}
