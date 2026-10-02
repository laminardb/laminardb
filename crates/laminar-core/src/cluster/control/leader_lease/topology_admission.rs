//! Pre-cut admission and assignment publication share the existing authority CAS.

use super::*;
use crate::cluster::control::topology::{
    TopologyAbortReason, TopologyAdmissionPhase, TopologyAdmissionPlan, TopologyAdmissionStatus,
    TopologyError, TopologyOperationId, TopologyPlanRef, MAX_TOPOLOGY_OPERATIONS,
    MAX_TOPOLOGY_PLAN_BYTES,
};

pub(super) const CONTROL_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const MAX_ADMISSION_ATTEMPTS: usize = 16;
const PLAN_PREFIX: &str = "control/topology-plans/v1/";

fn plan_path(reference: &TopologyPlanRef) -> OsPath {
    OsPath::from(format!("{PLAN_PREFIX}{}.json", reference.sha256))
}

fn encode_plan(plan: &TopologyAdmissionPlan) -> Result<(Vec<u8>, TopologyPlanRef), TopologyError> {
    plan.validate()?;
    let bytes =
        serde_json::to_vec(plan).map_err(|error| TopologyError::Invalid(error.to_string()))?;
    if bytes.len() > MAX_TOPOLOGY_PLAN_BYTES {
        return Err(TopologyError::Invalid(
            "topology plan exceeds 32 KiB".into(),
        ));
    }
    let reference = TopologyPlanRef {
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        encoded_len: bytes.len() as u64,
    };
    reference.validate()?;
    Ok((bytes, reference))
}

impl LeaderAuthorityRecord {
    pub(super) fn validate_topology_admission(&self) -> Result<(), LeaseError> {
        if self.version < TOPOLOGY_ADMISSION_RECORD_VERSION
            && (!self.topology_operations.is_empty() || self.assignment_drain_reservation.is_some())
        {
            return Err(LeaseError::Invalid(
                "admission evidence requires authority format 14".into(),
            ));
        }
        if self.topology_operations.len() > MAX_TOPOLOGY_OPERATIONS {
            return Err(LeaseError::Invalid(
                "topology request journal exceeds its fixed bound".into(),
            ));
        }
        let mut identities = BTreeSet::new();
        let mut planned = 0;
        let mut previous = 0;
        for operation in &self.topology_operations {
            operation
                .validate(self.lease.seq)
                .map_err(|e| LeaseError::Invalid(e.to_string()))?;
            if !identities.insert(operation.operation_id.get())
                || operation.admitted_sequence <= previous
                || self
                    .topology_baseline
                    .as_ref()
                    .is_some_and(|baseline| baseline.operation_id == operation.operation_id)
            {
                return Err(LeaseError::Invalid(
                    "topology request journal is not canonical".into(),
                ));
            }
            previous = operation.admitted_sequence;
            if (operation.commit.is_some()
                || operation.target_preparations.iter().any(|receipt| {
                    receipt.protocol_version
                        == super::super::topology::TOPOLOGY_COMMIT_PROTOCOL_VERSION
                }))
                && self.version < TOPOLOGY_COMMIT_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "topology Commit capability requires authority format 20".into(),
                ));
            }
            if !operation.target_preparations.is_empty()
                && self.version < TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "target preparation observations require authority format 19".into(),
                ));
            }
            if operation.migration_root.is_some()
                && self.version < TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "migration roots require authority format 17".into(),
                ));
            }
            if operation.cut.is_some() && self.version < TOPOLOGY_CUT_RECORD_VERSION {
                return Err(LeaseError::Invalid(
                    "checkpoint-bound topology requires authority format 15".into(),
                ));
            }
            if operation.preparation.is_some() && self.version < TOPOLOGY_PREPARATION_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "participant preparation requires authority format 16".into(),
                ));
            }
            if operation.blocks_admission() {
                planned += 1;
                if operation.is_preparing() && !self.lease.matches_proof(&operation.admitted_by) {
                    return Err(LeaseError::Invalid(
                        "planned topology belongs to an obsolete term".into(),
                    ));
                }
                if let Some(cut) = &operation.cut {
                    if cut.committed.is_none()
                        && self.active_checkpoint_artifacts.as_ref() != Some(&cut.inventory)
                    {
                        return Err(LeaseError::Invalid(
                            "unsettled topology cut lost its admitted artifacts".into(),
                        ));
                    }
                }
            }
        }
        if let Some(reservation) = &self.assignment_drain_reservation {
            reservation
                .proposal
                .validate()
                .map_err(|e| LeaseError::Invalid(e.to_string()))?;
            if !reservation.transition.is_canonical()
                || reservation.proposal.version != reservation.transition.target.assignment_version
                || reservation.authority_sequence == 0
                || reservation.authority_sequence > self.lease.seq
            {
                return Err(LeaseError::Invalid(
                    "invalid assignment drain reservation".into(),
                ));
            }
        }
        if planned > 1
            || (planned != 0
                && (self.assignment_drain_reservation.is_some()
                    || self
                        .active_checkpoint_artifacts
                        .as_ref()
                        .is_some_and(|active| {
                            self.topology_operations
                                .iter()
                                .filter(|entry| entry.is_preparing())
                                .all(|entry| {
                                    entry
                                        .cut
                                        .as_ref()
                                        .is_none_or(|cut| cut.inventory != *active)
                                })
                        })
                    || self.assignment_handoff_pin.is_some()
                    || (self
                        .topology_operations
                        .iter()
                        .any(TopologyAdmissionStatus::is_preparing)
                        && self.recovery_fault_slots.iter().any(|slot| slot.active))))
        {
            return Err(LeaseError::Invalid(
                "topology preparation overlaps incompatible authority".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn abort_topology_preparation(&mut self, reason: TopologyAbortReason) {
        for operation in &mut self.topology_operations {
            if operation.is_preparing() {
                operation.phase = TopologyAdmissionPhase::Aborted { reason };
                operation.status_sequence = self.lease.seq;
            }
        }
    }

    pub(super) fn reject_topology_preparation(
        &self,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        if let Some(operation) = self
            .topology_operations
            .iter()
            .find(|operation| operation.blocks_admission())
        {
            return Err(DecisionError::Conflict(format!(
                "topology operation {} reserves checkpoint and assignment admission",
                operation.operation_id.get()
            ))
            .into());
        }
        Ok(())
    }

    pub(super) fn validate_reserved_assignment_decision(
        &self,
        decision: &AuthorityAssignmentDecision,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let Some(reservation) = &self.assignment_drain_reservation else {
            return Ok(());
        };
        if decision.predecessor() != &reservation.transition.predecessor
            || decision.target_version() != reservation.proposal.version
            || matches!(decision, AuthorityAssignmentDecision::Drain(drain) if drain.transition != reservation.transition)
        {
            return Err(DecisionError::Conflict(
                "assignment decision does not settle the exact reserved drain".into(),
            )
            .into());
        }
        Ok(())
    }

    pub(super) fn validate_topology_admission_successor(
        &self,
        next: &Self,
    ) -> Result<(), LeaseError> {
        if next.version < self.version
            || next.topology_operations.len() < self.topology_operations.len()
        {
            return Err(LeaseError::Invalid(
                "authority cannot downgrade or forget topology requests".into(),
            ));
        }
        for (prior, after) in self
            .topology_operations
            .iter()
            .zip(&next.topology_operations)
        {
            prior
                .validate_successor(after, next.lease.seq)
                .map_err(|error| LeaseError::Invalid(error.to_string()))?;
            if let Some(cut) = &after.cut {
                let prior_commit = prior.cut.as_ref().and_then(|cut| cut.committed.as_ref());
                if prior_commit.is_none() {
                    if let Some(commit) = &cut.committed {
                        if commit.authority_sequence != next.lease.seq
                            || next.checkpoint_outcome.as_ref().is_none_or(|outcome| {
                                !outcome.is_commit()
                                    || outcome.committed_checkpoint.as_ref()
                                        != Some(&commit.checkpoint)
                                    || outcome.assignment_fence != cut.inventory.assignment_fence
                                    || outcome.deployment_id != cut.inventory.deployment_id
                                    || outcome.leader_proof.as_ref() != Some(&after.admitted_by)
                            })
                        {
                            return Err(LeaseError::Invalid(
                                "new topology cut Commit must bind its terminal authority append"
                                    .into(),
                            ));
                        }
                    }
                }
            }
        }
        for added in next
            .topology_operations
            .iter()
            .skip(self.topology_operations.len())
        {
            if !added.is_planned()
                || added.admitted_sequence != next.lease.seq
                || !next.lease.matches_proof(&added.admitted_by)
            {
                return Err(LeaseError::Invalid(
                    "new topology request must bind its exact admission append".into(),
                ));
            }
        }
        if let Some(reservation) = &self.assignment_drain_reservation {
            if next.assignment_drain_reservation.as_ref() != Some(reservation) {
                let decision = next.assignment_decision.as_ref().ok_or_else(|| {
                    LeaseError::Invalid(
                        "reserved assignment can be cleared only by its definitive decision".into(),
                    )
                })?;
                self.validate_reserved_assignment_decision(decision)
                    .map_err(|e| LeaseError::Invalid(e.to_string()))?;
                if next.assignment_drain_reservation.is_some() {
                    return Err(LeaseError::Invalid(
                        "cannot replace an unresolved assignment reservation".into(),
                    ));
                }
            }
        } else if let Some(reservation) = &next.assignment_drain_reservation {
            if reservation.authority_sequence != next.lease.seq
                || !next.lease.matches_proof(&reservation.transition.leader)
            {
                return Err(LeaseError::Invalid(
                    "new assignment reservation must bind its admission append".into(),
                ));
            }
        }
        Ok(())
    }
}

impl LeaderLeaseStore {
    pub(super) async fn read_admission_blob(
        &self,
        path: &OsPath,
        expected_len: u64,
    ) -> Result<Bytes, LeaseError> {
        let result = self.store.get(path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                LeaseError::Invalid(format!("admission artifact '{path}' is missing"))
            }
            e => LeaseError::Io(e.to_string()),
        })?;
        if result.meta.size != expected_len {
            return Err(LeaseError::Invalid(format!(
                "admission artifact '{path}' has the wrong length"
            )));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|e| LeaseError::Io(e.to_string()))?;
        if bytes.len() as u64 != expected_len {
            return Err(LeaseError::Invalid(
                "admission artifact length changed while reading".into(),
            ));
        }
        Ok(bytes)
    }

    pub(super) async fn stage_admission_blob(
        &self,
        path: &OsPath,
        bytes: &[u8],
    ) -> Result<(), LeaseError> {
        let put_error = self
            .store
            .put_opts(
                path,
                PutPayload::from(Bytes::copy_from_slice(bytes)),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .err();
        let stored = self
            .read_admission_blob(path, bytes.len() as u64)
            .await
            .map_err(|e| match put_error {
                Some(put) => LeaseError::Io(format!(
                    "admission write failed ({put}); read-back failed ({e})"
                )),
                None => e,
            })?;
        if stored.as_ref() != bytes {
            return Err(LeaseError::Invalid(
                "immutable admission artifact differs from its payload".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn load_topology_plan(
        &self,
        reference: &TopologyPlanRef,
    ) -> Result<TopologyAdmissionPlan, TopologyError> {
        reference.validate()?;
        let bytes = self
            .read_admission_blob(&plan_path(reference), reference.encoded_len)
            .await?;
        let plan: TopologyAdmissionPlan =
            serde_json::from_slice(&bytes).map_err(|e| TopologyError::Invalid(e.to_string()))?;
        let (canonical, actual) = encode_plan(&plan)?;
        if actual != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(TopologyError::Invalid(
                "topology plan does not match its immutable reference".into(),
            ));
        }
        Ok(plan)
    }

    pub(super) async fn audit_topology_operation(
        &self,
        operation: &TopologyAdmissionStatus,
    ) -> Result<(), LeaseError> {
        let plan = self
            .load_topology_plan(&operation.plan)
            .await
            .map_err(topology_lease_error)?;
        if plan.operation_id != operation.operation_id
            || plan
                .assignment
                .participant_incarnation(operation.admitted_by.owner.node_id)
                != Some(operation.admitted_by.owner.boot_id)
        {
            return Err(LeaseError::Invalid(
                "topology request differs from its retained payload".into(),
            ));
        }
        for reference in [&plan.parent_manifest, &plan.target_manifest] {
            self.load_catalog_manifest(reference)
                .await
                .map_err(TopologyError::from)
                .map_err(topology_lease_error)?;
        }
        if operation
            .cut
            .as_ref()
            .is_some_and(|cut| cut.inventory.assignment_fence.as_ref() != Some(&plan.assignment))
        {
            return Err(LeaseError::Invalid(
                "topology cut changed its frozen assignment".into(),
            ));
        }
        for sequence in [operation.admitted_sequence, operation.status_sequence] {
            let record = read_authority_record(self.store.as_ref(), sequence)
                .await?
                .ok_or_else(|| {
                    LeaseError::Invalid("topology operation authority anchor is missing".into())
                })?;
            let anchored = record
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == operation.operation_id)
                .ok_or_else(|| {
                    LeaseError::Invalid(
                        "topology operation is absent from its authority anchor".into(),
                    )
                })?;
            let valid = if sequence == operation.admitted_sequence {
                anchored.is_planned()
                    && anchored.admitted_sequence == sequence
                    && anchored.plan == operation.plan
                    && anchored.admitted_by == operation.admitted_by
                    && anchored
                        .preparation
                        .as_ref()
                        .map(|preparation| &preparation.compatibility)
                        == operation
                            .preparation
                            .as_ref()
                            .map(|preparation| &preparation.compatibility)
                    && record.topology_baseline.as_ref().is_some_and(|baseline| {
                        record.committed_topology_identity()
                            == Some((plan.expected_parent, &plan.parent_manifest))
                            && operation.cut.as_ref().is_none_or(|cut| {
                                cut.inventory.deployment_id == baseline.deployment_id
                            })
                    })
            } else {
                anchored == operation
            };
            if !valid {
                return Err(LeaseError::Invalid(
                    "topology operation differs from its retained authority anchor".into(),
                ));
            }
        }
        let descriptor = self
            .audit_topology_preparation(operation, &plan)
            .await
            .map_err(topology_lease_error)?;
        self.audit_topology_cut(operation).await?;
        self.audit_topology_migration_root(operation, &plan, descriptor.as_ref())
            .await
            .map_err(topology_lease_error)?;
        self.audit_topology_target_preparations(operation).await?;
        self.audit_topology_commit(operation, &plan).await
    }

    /// Read the definitive, payload-bound pre-cut request status without allocating identities.
    ///
    /// # Errors
    /// Rejects missing/corrupt referenced evidence or a read that exceeds 15 seconds.
    pub async fn topology_operation_status(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<Option<TopologyAdmissionStatus>, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let Some(head) = self.load_record().await? else {
                return Ok(None);
            };
            let Some(operation) = head
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == operation_id)
            else {
                return Ok(None);
            };
            self.audit_topology_operation(operation).await?;
            Ok(Some(operation.clone()))
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    /// Reserve a candidate under current authority. This does not commit or start the graph.
    ///
    /// The supplied assignment store must be the configured, namespace-verified store used by
    /// the cluster controller. Its normal drain writers must use `publish_assignment_drain`.
    /// Call only after coordinated binary upgrade; cached old actors are not revoked by format 14.
    /// Cancellation/lost responses require retry with exactly the same plan and request identity.
    ///
    /// # Errors
    /// Rejects changed parents, id reuse with different payload, concurrent operations, unresolved
    /// checkpoint/recovery/assignment authority, malformed evidence, or bounded contention.
    pub async fn admit_topology_plan(
        &self,
        proof: &LeaderProof,
        assignments: &AssignmentSnapshotStore,
        plan: &TopologyAdmissionPlan,
        target: &CatalogManifest,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        tokio::time::timeout(
            CONTROL_TIMEOUT,
            self.admit_topology_plan_inner(proof, assignments, plan, target),
        )
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    async fn admit_topology_plan_inner(
        &self,
        proof: &LeaderProof,
        assignments: &AssignmentSnapshotStore,
        plan: &TopologyAdmissionPlan,
        target: &CatalogManifest,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        let (encoded, reference) = encode_plan(plan)?;
        let (target_bytes, target_ref) = target.encode_and_reference()?;
        if target_ref != plan.target_manifest {
            return Err(TopologyError::Invalid(
                "proposal does not bind its target inventory".into(),
            ));
        }
        if !proof.is_canonical() {
            return Err(TopologyError::Fenced);
        }
        for _ in 0..MAX_ADMISSION_ATTEMPTS {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(TopologyError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(TopologyError::Fenced);
            }
            if let Some(existing) = current
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == plan.operation_id)
            {
                if existing.plan != reference {
                    return Err(TopologyError::Conflict(
                        "operation identity was already bound to another payload".into(),
                    ));
                }
                self.audit_topology_operation(existing).await?;
                return Ok(existing.clone());
            }
            if plan.assignment.participant_incarnation(proof.owner.node_id)
                != Some(proof.owner.boot_id)
            {
                return Err(TopologyError::Fenced);
            }
            let baseline = current.topology_baseline.as_ref().ok_or_else(|| {
                TopologyError::Protocol(
                    "explicit legacy topology adoption is required before migration admission"
                        .into(),
                )
            })?;
            if baseline.operation_id == plan.operation_id {
                return Err(TopologyError::Conflict(
                    "operation identity was already used for legacy adoption".into(),
                ));
            }
            if current.committed_topology_identity()
                != Some((plan.expected_parent, &plan.parent_manifest))
            {
                return Err(TopologyError::Conflict(
                    "expected topology parent does not match committed authority".into(),
                ));
            }
            self.audit_topology_adoption(baseline).await?;
            self.require_topology_deployment(&baseline.deployment_id)
                .await?;
            if current.topology_operations.len() >= MAX_TOPOLOGY_OPERATIONS {
                return Err(TopologyError::Conflict("topology journal is full; retention is required before admitting further requests".into()));
            }
            if current
                .topology_operations
                .iter()
                .any(TopologyAdmissionStatus::blocks_admission)
                || current.assignment_drain_reservation.is_some()
                || current.active_checkpoint_artifacts.is_some()
                || current.assignment_handoff_pin.is_some()
                || current.artifact_cleanup.is_some()
                || current.recovery_fault_slots.iter().any(|slot| {
                    slot.active || slot.disposition == RecoveryFaultDisposition::Terminal
                })
            {
                return Err(TopologyError::Conflict(
                    "checkpoint, recovery, assignment or another topology operation is unresolved"
                        .into(),
                ));
            }
            let assignment = assignments
                .load()
                .await
                .map_err(topology_assignment_error)?
                .ok_or_else(|| {
                    TopologyError::Conflict("assignment inventory is uninitialized".into())
                })?;
            if assignment.draining
                || assignment
                    .assignment_fence()
                    .map_err(|e| TopologyError::Invalid(e.to_string()))?
                    != plan.assignment
            {
                return Err(TopologyError::Conflict(
                    "required assignment/boot roster changed".into(),
                ));
            }
            self.reject_consumed_checkpoint_assignment(current, &plan.assignment)
                .await
                .map_err(topology_checkpoint_error)?;
            let parent = self.load_catalog_manifest(&plan.parent_manifest).await?;
            // Only additive inventory reservation is implemented. DB semantic compatibility
            // certificates and a cut are required before any future target commit transition.
            if target.entries.len() <= parent.entries.len()
                || !target.entries.starts_with(&parent.entries)
            {
                return Err(TopologyError::Invalid("admission currently requires exact preservation of the ordered parent inventory".into()));
            }
            if let Some(reference) = &plan.compatibility {
                let descriptor = self.load_topology_compatibility(reference).await?;
                descriptor.validate_catalogs(&parent, target)?;
                if descriptor.parent_version != plan.expected_parent
                    || descriptor.deployment_id != baseline.deployment_id
                {
                    return Err(TopologyError::Conflict(
                        "descriptor parent/deployment differs from admission authority".into(),
                    ));
                }
            }
            self.ensure_catalog_manifest_blob(&target_bytes, &target_ref)
                .await?;
            self.stage_admission_blob(&plan_path(&reference), &encoded)
                .await?;
            let mut lease = current.lease.clone();
            lease.seq = lease
                .seq
                .checked_add(1)
                .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
            let sequence = lease.seq;
            let mut next = current.preserve_with_lease(lease);
            next.version = next.version.max(if plan.compatibility.is_some() {
                TOPOLOGY_PREPARATION_RECORD_VERSION
            } else {
                TOPOLOGY_ADMISSION_RECORD_VERSION
            });
            let operation = TopologyAdmissionStatus {
                operation_id: plan.operation_id,
                plan: reference.clone(),
                admitted_by: proof.clone(),
                admitted_sequence: sequence,
                status_sequence: sequence,
                phase: TopologyAdmissionPhase::Planned,
                cut: None,
                migration_root: None,
                target_preparations: Vec::new(),
                commit: None,
                preparation: plan.compatibility.clone().map(|compatibility| {
                    crate::cluster::control::topology::TopologyPreparation {
                        compatibility,
                        certificates: Vec::new(),
                        complete_sequence: None,
                    }
                }),
            };
            next.topology_operations.push(operation.clone());
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
                AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(operation)
                }
                AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
            }
        }
        Err(TopologyError::Contended)
    }

    /// Durably abort an exact pre-cut request under current authority. No graph rollback occurs.
    ///
    /// # Errors
    /// Rejects a stale proof, unknown request, different payload, or bounded contention.
    pub async fn abort_topology_plan(
        &self,
        proof: &LeaderProof,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            expected_plan.validate()?;
            for _ in 0..MAX_ADMISSION_ATTEMPTS {
                let published = self
                    .load_published_authority_head()
                    .await?
                    .ok_or(TopologyError::Fenced)?;
                let current = &published.record;
                if !current.lease.matches_proof(proof) {
                    return Err(TopologyError::Fenced);
                }
                let index = current
                    .topology_operations
                    .iter()
                    .position(|entry| entry.operation_id == operation_id)
                    .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
                let operation = &current.topology_operations[index];
                if operation.plan != *expected_plan {
                    return Err(TopologyError::Conflict("operation payload differs".into()));
                }
                self.audit_topology_operation(operation).await?;
                if operation.has_target_commit() {
                    return Err(TopologyError::Conflict("committed topology requires target recovery and cannot abort".into()));
                }
                if !operation.is_preparing() {
                    return Ok(operation.clone());
                }
                if operation.phase == TopologyAdmissionPhase::Quiescing {
                    return Err(TopologyError::Conflict("cut sink settlement is unresolved; coordinated recovery must reconcile it before resuming the parent".into()));
                }
                let mut lease = current.lease.clone();
                lease.seq = lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let mut next = current.preserve_with_lease(lease);
                next.abort_topology_preparation(TopologyAbortReason::Requested);
                let result = next.topology_operations[index].clone();
                match self
                    .create_authority_record(Some(&published), &next)
                    .await?
                {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                        return Ok(result)
                    }
                    AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
                }
            }
            Err(TopologyError::Contended)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }
}

pub(super) fn topology_checkpoint_error(error: ClusterCheckpointAuthorityError) -> TopologyError {
    match error {
        ClusterCheckpointAuthorityError::Authority(error) => TopologyError::Authority(error),
        ClusterCheckpointAuthorityError::Decision(DecisionError::Io(reason)) => {
            TopologyError::Authority(LeaseError::Io(reason))
        }
        ClusterCheckpointAuthorityError::Fenced => TopologyError::Fenced,
        error => TopologyError::Conflict(error.to_string()),
    }
}

pub(super) fn topology_assignment_error(error: SnapshotError) -> TopologyError {
    match error {
        SnapshotError::Io(reason) => TopologyError::Authority(LeaseError::Io(reason)),
        error => TopologyError::Invalid(error.to_string()),
    }
}

fn topology_lease_error(error: TopologyError) -> LeaseError {
    match error {
        TopologyError::Authority(error) => error,
        error => LeaseError::Invalid(error.to_string()),
    }
}
