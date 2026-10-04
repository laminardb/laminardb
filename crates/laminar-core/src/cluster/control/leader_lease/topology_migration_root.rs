//! Bounded exact-cut metadata staging through the existing shared authority append.

use super::topology_admission::{
    topology_assignment_error, topology_checkpoint_error, CONTROL_TIMEOUT, MAX_ADMISSION_ATTEMPTS,
};
use super::{
    read_authority_record, AssignmentSnapshotStore, AuthorityCreateOutcome, LeaderLeaseStore,
    OsPath, TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION, TOPOLOGY_SOURCE_ROOT_RECORD_VERSION,
};
use crate::checkpoint::{CheckpointStore, CommittedCheckpointIndex, LeaderProof};
use crate::checkpoint_decision::CheckpointDecisionStore;
use crate::cluster::control::{
    CatalogManifest, ProcessLeaseAuthority, TopologyAdmissionPhase, TopologyAdmissionStatus,
    TopologyError, TopologyMigrationRoot, TopologyMigrationRootBinding, TopologyMigrationRootRef,
    TopologyOperationId, TopologyPlanRef, TopologySourceInitialization,
    MAX_TOPOLOGY_ROOT_MANIFEST_BYTES,
};
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use std::future::Future;

fn root_path(reference: &TopologyMigrationRootRef) -> OsPath {
    OsPath::from(format!(
        "control/topology-migration-roots/v1/{}.json",
        reference.sha256
    ))
}

// Create-only staging, not a second authority head. The first successfully sealed vector survives
// a disconnect before the shared append. A replacement leader aborts this pre-commit operation;
// it cannot use the slot to commit or run a target. Existing artifact cleanup never sweeps this
// control prefix. Root publication retains the content-addressed body independently of the slot.
fn source_root_slot(operation: &TopologyAdmissionStatus) -> OsPath {
    OsPath::from(format!(
        "control/topology-source-root-staging/v1/{}/{}.json",
        operation.operation_id.get(),
        operation.plan.sha256,
    ))
}

impl LeaderLeaseStore {
    async fn build_topology_root(
        &self,
        checkpoint_store: &dyn CheckpointStore,
        operation: &TopologyAdmissionStatus,
        descriptor: &crate::cluster::control::ClusterTopologyValidation,
        sources: Vec<TopologySourceInitialization>,
    ) -> Result<TopologyMigrationRoot, TopologyError> {
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
        if sources.is_empty() {
            TopologyMigrationRoot::build(operation, descriptor, &index, &manifests)
        } else {
            TopologyMigrationRoot::build_with_sources(
                operation, descriptor, &index, &manifests, sources,
            )
        }
    }

    /// Pin exact-cut restore requirements for a certified topology candidate.
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
        self.stage_topology_migration_root_with_initialization(
            proof,
            assignments,
            processes,
            checkpoint_store,
            operation_id,
            expected_plan,
            |_, _| async {
                Err(TopologyError::Unsupported(
                    "new sources require the configured connector initialization path".into(),
                ))
            },
        )
        .await
    }

    /// Stage a root with connector-owned new-source cursors resolved by the DB control path.
    /// The callback runs only on the admitting leader after full preparation/assignment checks,
    /// only if no sealed source root exists, and at most once in this call. It must not start or
    /// consume sources or create sink effects. The first create-only sealed vector wins concurrent
    /// attempts; all callers use that vector. Reads before a successful seal grant no boundary.
    /// Cancellation after sealing, lost responses and CAS retries never reevaluate that vector.
    /// No target Commit, restore or output permit is granted by either the slot or the shared append.
    ///
    /// # Errors
    /// Requires the same exact certified held cut as downstream-only staging. Rejects damaged or
    /// divergent slots, unsupported connector positions, stale authority and exceeded size/deadline
    /// bounds. Query the same operation after an ambiguous outcome; never invent another identity.
    #[allow(clippy::too_many_arguments)] // Existing authorities and one connector control callback.
    pub async fn stage_topology_migration_root_with_initialization<F, Fut>(
        &self,
        proof: &LeaderProof,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        checkpoint_store: &dyn CheckpointStore,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
        initialize: F,
    ) -> Result<TopologyAdmissionStatus, TopologyError>
    where
        F: FnOnce(CatalogManifest, crate::cluster::control::ClusterTopologyValidation) -> Fut,
        Fut: Future<Output = Result<Vec<TopologySourceInitialization>, TopologyError>>,
    {
        expected_plan.validate()?;
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let mut staged = None;
            let mut initialize = Some(initialize);
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
                    let has_sources = descriptor.objects.iter().any(|object| {
                        object.kind == crate::cluster::control::CatalogObjectKind::Source
                            && object.transition == crate::cluster::control::topology::ClusterTopologyObjectTransition::AddFutureOnly
                    });
                    let sealed = if has_sources {
                        self.load_source_root_slot(operation, &plan, &descriptor).await?
                    } else { None };
                    let sources = if let Some(root) = &sealed {
                        root.source_initializations.clone()
                    } else if has_sources {
                        let target = self.load_catalog_manifest(&plan.target_manifest).await
                            .map_err(TopologyError::from)?;
                        initialize.take().ok_or_else(|| TopologyError::Invalid(
                            "source initialization callback was already consumed".into(),
                        ))?(target, descriptor.clone()).await?
                    } else { Vec::new() };
                    let root = self.build_topology_root(checkpoint_store, operation, &descriptor, sources).await?;
                    root.validate_binding(operation, &plan, &descriptor)?;
                    let root = if let Some(sealed) = sealed {
                        if root != sealed {
                            return Err(TopologyError::Invalid("sealed source root differs from the exact cut metadata".into()));
                        }
                        sealed
                    } else if has_sources {
                        self.seal_source_root_slot(operation, &plan, &descriptor, &root).await?
                    } else { root };
                    let (bytes, reference) = root.encode_and_reference()?;
                    self.stage_admission_blob(&root_path(&reference), &bytes)
                        .await?;
                    let record_version = if root.format_version == 2 {
                        TOPOLOGY_SOURCE_ROOT_RECORD_VERSION
                    } else { TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION };
                    staged = Some((reference, record_version));
                }
                let mut lease = current.lease.clone();
                lease.seq = lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                next.version = next.version.max(staged.as_ref().ok_or_else(|| {
                    TopologyError::Invalid("root staging lost its content reference".into())
                })?.1);
                let operation = &mut next.topology_operations[index];
                operation.status_sequence = sequence;
                operation.migration_root = Some(TopologyMigrationRootBinding {
                    root: staged
                        .as_ref()
                        .ok_or_else(|| {
                            TopologyError::Invalid("root staging lost its content reference".into())
                        })?
                        .0.clone(),
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

    async fn load_source_root_slot(
        &self,
        operation: &TopologyAdmissionStatus,
        plan: &crate::cluster::control::TopologyAdmissionPlan,
        descriptor: &crate::cluster::control::ClusterTopologyValidation,
    ) -> Result<Option<TopologyMigrationRoot>, TopologyError> {
        let result = match self.store.get(&source_root_slot(operation)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => {
                return Err(TopologyError::Authority(super::LeaseError::Io(
                    error.to_string(),
                )))
            }
        };
        let expected_len = result.meta.size;
        if expected_len == 0
            || expected_len > crate::cluster::control::topology::MAX_TOPOLOGY_ROOT_BYTES
        {
            return Err(TopologyError::Invalid(
                "sealed source root exceeds its 1 MiB bound".into(),
            ));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|e| super::LeaseError::Io(e.to_string()))?;
        let root: TopologyMigrationRoot =
            serde_json::from_slice(&bytes).map_err(|e| TopologyError::Invalid(e.to_string()))?;
        let (canonical, _) = root.encode_and_reference()?;
        if root.format_version != 2
            || canonical.as_slice() != bytes.as_ref()
            || bytes.len() as u64 != expected_len
        {
            return Err(TopologyError::Invalid(
                "sealed source root has a noncanonical body".into(),
            ));
        }
        root.validate_binding(operation, plan, descriptor)?;
        Ok(Some(root))
    }

    async fn seal_source_root_slot(
        &self,
        operation: &TopologyAdmissionStatus,
        plan: &crate::cluster::control::TopologyAdmissionPlan,
        descriptor: &crate::cluster::control::ClusterTopologyValidation,
        proposed: &TopologyMigrationRoot,
    ) -> Result<TopologyMigrationRoot, TopologyError> {
        let (bytes, _) = proposed.encode_and_reference()?;
        let write = self
            .store
            .put_opts(
                &source_root_slot(operation),
                PutPayload::from(bytes),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await;
        // An unsuccessful response is not proof of failure. An existing valid slot is the winning
        // boundary, including when a concurrent caller resolved a later broker high watermark.
        let winner = self
            .load_source_root_slot(operation, plan, descriptor)
            .await?
            .ok_or_else(|| match write {
                Err(error) => TopologyError::Authority(super::LeaseError::Io(error.to_string())),
                Ok(_) => {
                    TopologyError::Invalid("sealed source root disappeared after creation".into())
                }
            })?;
        let mut expected = proposed.clone();
        expected
            .source_initializations
            .clone_from(&winner.source_initializations);
        if winner != expected {
            return Err(TopologyError::Invalid(
                "source root slot changed preserved cut requirements".into(),
            ));
        }
        Ok(winner)
    }

    pub(super) async fn load_topology_root(
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
        if root.format_version == 2 && record.version < TOPOLOGY_SOURCE_ROOT_RECORD_VERSION {
            return Err(TopologyError::Protocol(
                "source initialization roots require authority format 18".into(),
            ));
        }
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
