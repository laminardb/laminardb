//! Bounded exact-cut metadata staging through the existing shared authority append.

use super::topology_admission::{
    topology_assignment_error, topology_checkpoint_error, CONTROL_TIMEOUT, MAX_ADMISSION_ATTEMPTS,
};
use super::{
    read_authority_record, AssignmentSnapshotStore, AuthorityCreateOutcome, LeaderLeaseStore,
    OsPath, TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION,
};
use crate::checkpoint::{CheckpointStore, CommittedCheckpointIndex, LeaderProof};
use crate::checkpoint_decision::CheckpointDecisionStore;
use crate::cluster::control::{
    ProcessLeaseAuthority, TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyError,
    TopologyMigrationRoot, TopologyMigrationRootBinding, TopologyMigrationRootRef,
    TopologyOperationId, TopologyPlanRef, MAX_TOPOLOGY_ROOT_MANIFEST_BYTES,
};

fn root_path(reference: &TopologyMigrationRootRef) -> OsPath {
    OsPath::from(format!(
        "control/topology-migration-roots/v1/{}.json",
        reference.sha256
    ))
}

impl LeaderLeaseStore {
    async fn build_topology_root(
        &self,
        checkpoint_store: &dyn CheckpointStore,
        operation: &TopologyAdmissionStatus,
        descriptor: &crate::cluster::control::ClusterTopologyValidation,
    ) -> Result<TopologyMigrationRoot, TopologyError> {
        // Reject unresolved source starts before any metadata read or durable root write.
        TopologyMigrationRoot::object_mappings(descriptor)?;
        let reference = &operation
            .cut
            .as_ref()
            .and_then(|cut| cut.committed.as_ref())
            .ok_or_else(|| TopologyError::Conflict("root requires a committed parent cut".into()))?
            .checkpoint;
        let index = CheckpointDecisionStore::new(self.store.clone())
            .load_committed_checkpoint(reference)
            .await
            .map_err(|error| TopologyError::Invalid(error.to_string()))?;
        validate_manifest_budget(&index)?;
        if checkpoint_store.key_group_count().get() != index.vnode_count {
            return Err(TopologyError::Invalid(
                "root reader has a different vnode domain".into(),
            ));
        }
        // Sequential reads keep the aggregate 16 MiB metadata budget explicit. No state, sink
        // payload or Arrow output segment is loaded, cloned, restored or rewritten here.
        let mut manifests = Vec::with_capacity(index.participants.len());
        for participant in &index.participants {
            let manifest = checkpoint_store
                .load_manifest_verified(
                    participant.participant_id,
                    index.checkpoint_id,
                    participant.manifest_len,
                    &participant.manifest_sha256,
                )
                .await
                .map_err(|e| TopologyError::Invalid(e.to_string()))?
                .ok_or_else(|| {
                    TopologyError::Invalid("migration root participant manifest is missing".into())
                })?;
            manifests.push(manifest);
        }
        TopologyMigrationRoot::build(operation, descriptor, &index, &manifests)
    }

    /// Pin exact-cut restore requirements for certified stateless downstream additions.
    /// Call through the live controller with its configured authorities and checkpoint reader.
    /// No candidate actor, source, output, target catalog Commit or Release is authorized.
    /// Identical retries return the retained append, including after a lost response.
    ///
    /// # Errors
    /// Requires the full certified/current roster, unchanged assignment, current admitting leader
    /// and `CutPrepared`. Rejects unresolved new sources, damaged metadata, aggregate manifests over
    /// 16 MiB, roots over 1 MiB or 16 CAS attempts within 15 seconds. Cancellation can leave a
    /// successful immutable append; query status or retry the same request identity.
    #[allow(clippy::too_many_arguments)] // Existing independently configured authority domains.
    pub async fn stage_topology_migration_root(
        &self,
        proof: &LeaderProof,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        checkpoint_store: &dyn CheckpointStore,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        expected_plan.validate()?;
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let mut staged = None;
            for _ in 0..MAX_ADMISSION_ATTEMPTS {
                let published = self
                    .load_published_authority_head()
                    .await?
                    .ok_or(TopologyError::Fenced)?;
                let current = &published.record;
                let index = current
                    .topology_operations
                    .iter()
                    .position(|entry| entry.operation_id == operation_id)
                    .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
                let operation = &current.topology_operations[index];
                if !current.lease.matches_proof(proof) || operation.admitted_by != *proof {
                    return Err(TopologyError::Fenced);
                }
                if operation.plan != *expected_plan
                    || operation.phase != TopologyAdmissionPhase::CutPrepared
                {
                    return Err(TopologyError::Conflict(
                        "root staging requires the exact prepared request payload".into(),
                    ));
                }
                self.audit_topology_operation(operation).await?;
                let plan = self.load_topology_plan(expected_plan).await?;
                let descriptor = self
                    .require_topology_prepared(operation, &plan, processes)
                    .await?;
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
                self.reject_consumed_checkpoint_assignment(current, &plan.assignment)
                    .await
                    .map_err(topology_checkpoint_error)?;
                if operation.migration_root.is_some() {
                    return Ok(operation.clone());
                }
                if staged.is_none() {
                    let root = self
                        .build_topology_root(checkpoint_store, operation, &descriptor)
                        .await?;
                    root.validate_binding(operation, &plan, &descriptor)?;
                    let (bytes, reference) = root.encode_and_reference()?;
                    self.stage_admission_blob(&root_path(&reference), &bytes)
                        .await?;
                    staged = Some(reference);
                }
                let mut lease = current.lease.clone();
                lease.seq = lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                next.version = next.version.max(TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION);
                let operation = &mut next.topology_operations[index];
                operation.status_sequence = sequence;
                operation.migration_root = Some(TopologyMigrationRootBinding {
                    root: staged
                        .as_ref()
                        .ok_or_else(|| {
                            TopologyError::Invalid("root staging lost its content reference".into())
                        })?
                        .clone(),
                    authority_sequence: sequence,
                });
                let result = operation.clone();
                match self
                    .create_authority_record(Some(&published), &next)
                    .await?
                {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                        self.require_topology_prepared(&result, &plan, processes)
                            .await?;
                        return Ok(result);
                    }
                    AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
                }
            }
            Err(TopologyError::Contended)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    async fn load_topology_root(
        &self,
        reference: &TopologyMigrationRootRef,
    ) -> Result<TopologyMigrationRoot, TopologyError> {
        reference.validate()?;
        let bytes = self
            .read_admission_blob(&root_path(reference), reference.encoded_len)
            .await?;
        let root: TopologyMigrationRoot =
            serde_json::from_slice(&bytes).map_err(|e| TopologyError::Invalid(e.to_string()))?;
        let (canonical, actual) = root.encode_and_reference()?;
        if actual != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(TopologyError::Invalid(
                "migration root differs from its canonical immutable reference".into(),
            ));
        }
        Ok(root)
    }

    pub(super) async fn audit_topology_migration_root(
        &self,
        operation: &TopologyAdmissionStatus,
        plan: &crate::cluster::control::TopologyAdmissionPlan,
        descriptor: Option<&crate::cluster::control::ClusterTopologyValidation>,
    ) -> Result<(), TopologyError> {
        let Some(binding) = &operation.migration_root else {
            return Ok(());
        };
        let root = self.load_topology_root(&binding.root).await?;
        let descriptor =
            descriptor.ok_or_else(|| TopologyError::Protocol("root has no descriptor".into()))?;
        root.validate_binding(operation, plan, descriptor)?;
        let record = read_authority_record(self.store.as_ref(), binding.authority_sequence)
            .await?
            .ok_or_else(|| {
                TopologyError::Invalid("migration root authority anchor is missing".into())
            })?;
        if record
            .topology_operations
            .iter()
            .find(|entry| entry.operation_id == operation.operation_id)
            .is_none_or(|anchored| {
                anchored.plan != operation.plan
                    || anchored.admitted_by != operation.admitted_by
                    || anchored.admitted_sequence != operation.admitted_sequence
                    || anchored.status_sequence != binding.authority_sequence
                    || anchored.phase != TopologyAdmissionPhase::CutPrepared
                    || anchored.cut != operation.cut
                    || anchored.preparation != operation.preparation
                    || anchored.migration_root.as_ref() != Some(binding)
            })
        {
            return Err(TopologyError::Invalid(
                "migration root differs from its first immutable authority append".into(),
            ));
        }
        Ok(())
    }

    /// Read immutable staged requirements for inspection. They grant no restore/output authority.
    ///
    /// # Errors
    /// Rejects damaged/missing evidence or a read exceeding 15 seconds.
    pub async fn topology_migration_root(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<Option<TopologyMigrationRoot>, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let Some(current) = self.load_record().await? else {
                return Ok(None);
            };
            let Some(operation) = current
                .topology_operations
                .iter()
                .find(|e| e.operation_id == operation_id)
            else {
                return Ok(None);
            };
            self.audit_topology_operation(operation).await?;
            match &operation.migration_root {
                Some(binding) => self.load_topology_root(&binding.root).await.map(Some),
                None => Ok(None),
            }
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }
}

pub(super) fn validate_manifest_budget(
    index: &CommittedCheckpointIndex,
) -> Result<(), TopologyError> {
    index
        .participants
        .iter()
        .try_fold(0_u64, |total, p| total.checked_add(p.manifest_len))
        .filter(|total| *total <= MAX_TOPOLOGY_ROOT_MANIFEST_BYTES)
        .ok_or_else(|| {
            TopologyError::Unsupported(
                "migration root manifest metadata exceeds the aggregate 16 MiB budget".into(),
            )
        })?;
    Ok(())
}
