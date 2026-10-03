//! Candidate binding and exact-process preparation use the existing fenced authority append.

use super::topology_admission::{
    topology_assignment_error, CONTROL_TIMEOUT, MAX_ADMISSION_ATTEMPTS,
};
use super::{
    read_authority_record, AssignmentSnapshotStore, AuthorityCreateOutcome, CatalogManifest,
    LeaderLeaseStore, LeaseError, OsPath,
};
use crate::cluster::control::topology::{
    ClusterTopologyValidation, TopologyAdmissionPhase, TopologyAdmissionPlan,
    TopologyAdmissionStatus, TopologyCompatibilityRef, TopologyError, TopologyOperationId,
    TopologyParticipantCertificate, TopologyPlanRef, TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
    TOPOLOGY_SUBMISSION_PROTOCOL_VERSION,
};
use crate::cluster::control::{LocalProcessAuthorityIdentity, ProcessLeaseAuthority};

fn compatibility_path(reference: &TopologyCompatibilityRef) -> OsPath {
    OsPath::from(format!(
        "control/topology-compatibility/v1/{}.json",
        reference.sha256
    ))
}

impl LeaderLeaseStore {
    /// Stage the canonical local report without admitting a request or authorizing execution.
    ///
    /// # Errors
    /// Rejects malformed/oversized evidence, divergent immutable bytes or a 15 second deadline.
    pub async fn stage_topology_compatibility(
        &self,
        descriptor: &ClusterTopologyValidation,
    ) -> Result<TopologyCompatibilityRef, TopologyError> {
        let (bytes, reference) = descriptor.encode_and_reference()?;
        tokio::time::timeout(
            CONTROL_TIMEOUT,
            self.stage_admission_blob(&compatibility_path(&reference), &bytes),
        )
        .await
        .map_err(|_| TopologyError::Contended)??;
        Ok(reference)
    }

    pub(super) async fn load_topology_compatibility(
        &self,
        reference: &TopologyCompatibilityRef,
    ) -> Result<ClusterTopologyValidation, TopologyError> {
        reference.validate()?;
        let bytes = self
            .read_admission_blob(&compatibility_path(reference), reference.encoded_len)
            .await?;
        let descriptor: ClusterTopologyValidation = serde_json::from_slice(&bytes)
            .map_err(|error| TopologyError::Invalid(error.to_string()))?;
        let (canonical, actual) = descriptor.encode_and_reference()?;
        if actual != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(TopologyError::Invalid(
                "descriptor differs from its immutable canonical reference".into(),
            ));
        }
        Ok(descriptor)
    }

    pub(super) async fn audit_topology_compatibility(
        &self,
        plan: &TopologyAdmissionPlan,
    ) -> Result<Option<ClusterTopologyValidation>, TopologyError> {
        let Some(reference) = &plan.compatibility else {
            return Ok(None);
        };
        let descriptor = self.load_topology_compatibility(reference).await?;
        if descriptor.parent_version != plan.expected_parent
            || descriptor.parent_manifest != plan.parent_manifest
            || descriptor.target_manifest != plan.target_manifest
        {
            return Err(TopologyError::Invalid(
                "descriptor is not bound to the admitted catalog parent/target".into(),
            ));
        }
        let parent = self.load_catalog_manifest(&plan.parent_manifest).await?;
        let target = self.load_catalog_manifest(&plan.target_manifest).await?;
        descriptor.validate_catalogs(&parent, &target)?;
        Ok(Some(descriptor))
    }

    /// Read a complete immutable preparation input; participants independently compile its target.
    ///
    /// # Errors
    /// Rejects unknown requests, legacy protocols, damaged bindings and a 15 second read deadline.
    pub async fn topology_preparation_input(
        &self,
        operation_id: TopologyOperationId,
    ) -> Result<
        (
            TopologyAdmissionStatus,
            TopologyAdmissionPlan,
            CatalogManifest,
            ClusterTopologyValidation,
        ),
        TopologyError,
    > {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let current = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            let operation = current
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == operation_id)
                .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
            self.audit_topology_operation(operation).await?;
            let plan = self.load_topology_plan(&operation.plan).await?;
            let descriptor = self
                .audit_topology_compatibility(&plan)
                .await?
                .ok_or_else(|| {
                    TopologyError::Protocol(
                        "legacy plan has no participant compatibility binding".into(),
                    )
                })?;
            let target = self.load_catalog_manifest(&plan.target_manifest).await?;
            Ok((operation.clone(), plan, target, descriptor))
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    pub(super) async fn require_topology_process(
        &self,
        processes: &ProcessLeaseAuthority,
        identity: LocalProcessAuthorityIdentity,
    ) -> Result<(), TopologyError> {
        if !identity.is_canonical() {
            return Err(TopologyError::Fenced);
        }
        // The process namespace is configured separately from checkpoint/catalog storage.
        // Use its existing exact-term fence verifier, never infer a store from this authority.
        let current = processes
            .verify_current_participant_term(
                identity.participant,
                identity.process_term,
                tokio::time::Instant::now() + CONTROL_TIMEOUT,
            )
            .await
            .map_err(|error| TopologyError::Authority(LeaseError::Io(error.to_string())))?;
        if !current {
            return Err(TopologyError::Fenced);
        }
        Ok(())
    }

    pub(super) async fn require_topology_prepared(
        &self,
        operation: &TopologyAdmissionStatus,
        plan: &TopologyAdmissionPlan,
        processes: &ProcessLeaseAuthority,
    ) -> Result<ClusterTopologyValidation, TopologyError> {
        let descriptor = self
            .audit_topology_compatibility(plan)
            .await?
            .ok_or_else(|| {
                TopologyError::Protocol(
                    "new cuts require preparation protocol two and a durable candidate descriptor"
                        .into(),
                )
            })?;
        let preparation = operation.preparation.as_ref().ok_or_else(|| {
            TopologyError::Protocol("operation has no durable preparation".into())
        })?;
        if preparation.complete_sequence.is_none()
            || preparation
                .certificates
                .iter()
                .map(|cert| cert.participant)
                .collect::<Vec<_>>()
                != plan.assignment.participants
        {
            return Err(TopologyError::Conflict(
                "every frozen owner/evidence process must certify the candidate before the cut"
                    .into(),
            ));
        }
        for certificate in &preparation.certificates {
            self.require_topology_process(
                processes,
                LocalProcessAuthorityIdentity {
                    participant: certificate.participant,
                    process_term: certificate.process_term,
                },
            )
            .await?;
        }
        Ok(descriptor)
    }

    /// Persist one participant's independently compiled descriptor using the operation's leader
    /// and exact frozen assignment. Call through the local controller after checking live process
    /// authority around compilation. A certificate does not authorize restore or target output.
    ///
    /// # Errors
    /// Rejects divergence, missing capability, stale process/assignment/leader, changed payload,
    /// aborted requests, or 16 CAS attempts within 15 seconds. Retry identical evidence on ambiguity.
    #[allow(clippy::too_many_arguments)] // Explicit independent control authorities and exact request evidence.
    pub async fn certify_topology_participant(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
        identity: LocalProcessAuthorityIdentity,
        protocol_version: u16,
        compiled: &ClusterTopologyValidation,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        if !matches!(
            protocol_version,
            TOPOLOGY_PREPARATION_PROTOCOL_VERSION | TOPOLOGY_SUBMISSION_PROTOCOL_VERSION
        ) {
            return Err(TopologyError::Protocol(
                "participant lacks the exact preparation protocol".into(),
            ));
        }
        let (_, compiled_ref) = compiled.encode_and_reference()?;
        expected_plan.validate()?;
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
                    .position(|entry| entry.operation_id == operation_id)
                    .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
                let operation = &current.topology_operations[index];
                if operation.plan != *expected_plan {
                    return Err(TopologyError::Conflict(
                        "certificate request payload differs".into(),
                    ));
                }
                if !current.lease.matches_proof(&operation.admitted_by) || !operation.is_preparing()
                {
                    return Err(TopologyError::Fenced);
                }
                let plan = self.load_topology_plan(expected_plan).await?;
                if plan.compatibility.as_ref() != Some(&compiled_ref)
                    || plan.protocol_version != protocol_version
                {
                    return Err(TopologyError::Conflict(
                        "participant compiled a divergent candidate descriptor".into(),
                    ));
                }
                if plan
                    .assignment
                    .participant_incarnation(identity.participant.node_id)
                    != Some(identity.participant.boot_incarnation)
                {
                    return Err(TopologyError::Fenced);
                }
                self.audit_topology_operation(operation).await?;
                self.require_topology_process(processes, identity).await?;
                let assignment = assignments
                    .load()
                    .await
                    .map_err(topology_assignment_error)?
                    .ok_or(TopologyError::Fenced)?;
                if assignment.draining
                    || assignment
                        .assignment_fence()
                        .map_err(|error| TopologyError::Invalid(error.to_string()))?
                        != plan.assignment
                {
                    return Err(TopologyError::Conflict(
                        "participant assignment or frozen roster changed".into(),
                    ));
                }
                self.reject_consumed_checkpoint_assignment(current, &plan.assignment)
                    .await
                    .map_err(super::topology_admission::topology_checkpoint_error)?;
                let preparation = operation.preparation.as_ref().ok_or_else(|| {
                    TopologyError::Protocol("operation has no preparation binding".into())
                })?;
                if let Some(existing) = preparation
                    .certificates
                    .iter()
                    .find(|cert| cert.participant.node_id == identity.participant.node_id)
                {
                    if existing.participant != identity.participant
                        || existing.process_term != identity.process_term
                        || existing.protocol_version != protocol_version
                    {
                        return Err(TopologyError::Fenced);
                    }
                    return Ok(operation.clone());
                }
                if !matches!(
                    operation.phase,
                    TopologyAdmissionPhase::Planned | TopologyAdmissionPhase::Preparing
                ) {
                    return Err(TopologyError::Conflict(
                        "cut cannot acquire new preparation certificates".into(),
                    ));
                }
                let mut lease = current.lease.clone();
                lease.seq = lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                let operation = &mut next.topology_operations[index];
                operation.phase = TopologyAdmissionPhase::Preparing;
                operation.status_sequence = sequence;
                let preparation = operation
                    .preparation
                    .as_mut()
                    .ok_or_else(|| TopologyError::Invalid("preparation binding lost".into()))?;
                preparation
                    .certificates
                    .push(TopologyParticipantCertificate {
                        participant: identity.participant,
                        process_term: identity.process_term,
                        protocol_version,
                        authority_sequence: sequence,
                    });
                preparation
                    .certificates
                    .sort_by_key(|cert| cert.participant.node_id);
                if preparation.certificates.len() == plan.assignment.participants.len() {
                    preparation.complete_sequence = Some(sequence);
                }
                let result = operation.clone();
                match self
                    .create_authority_record(Some(&published), &next)
                    .await?
                {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                        self.require_topology_process(processes, identity).await?;
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

    pub(super) async fn audit_topology_preparation(
        &self,
        operation: &TopologyAdmissionStatus,
        plan: &TopologyAdmissionPlan,
    ) -> Result<Option<ClusterTopologyValidation>, TopologyError> {
        match (&plan.compatibility, &operation.preparation) {
            (None, None) => return Ok(None),
            (Some(reference), Some(preparation)) if reference == &preparation.compatibility => {}
            _ => {
                return Err(TopologyError::Invalid(
                    "operation changed its admitted descriptor binding".into(),
                ))
            }
        }
        let descriptor = self
            .audit_topology_compatibility(plan)
            .await?
            .ok_or_else(|| TopologyError::Invalid("missing descriptor".into()))?;
        let preparation = operation
            .preparation
            .as_ref()
            .ok_or_else(|| TopologyError::Invalid("missing preparation".into()))?;
        if preparation.certificates.iter().any(|cert| {
            cert.protocol_version != plan.protocol_version
                || plan
                    .assignment
                    .participant_incarnation(cert.participant.node_id)
                    != Some(cert.participant.boot_incarnation)
        }) || preparation.complete_sequence.is_some()
            != (preparation.certificates.len() == plan.assignment.participants.len())
            || operation.cut.as_ref().is_some_and(|cut| {
                cut.inventory.pipeline_identity != descriptor.parent_pipeline
                    || cut.inventory.deployment_id != descriptor.deployment_id
            })
        {
            return Err(TopologyError::Invalid(
                "preparation does not bind the frozen roster and parent pipeline".into(),
            ));
        }
        for certificate in &preparation.certificates {
            let record = read_authority_record(self.store.as_ref(), certificate.authority_sequence)
                .await?
                .ok_or_else(|| {
                    TopologyError::Invalid("preparation authority anchor is missing".into())
                })?;
            if record
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == operation.operation_id)
                .is_none_or(|anchored| {
                    anchored.plan != operation.plan
                        || anchored.admitted_by != operation.admitted_by
                        || anchored.admitted_sequence != operation.admitted_sequence
                        || anchored.status_sequence != certificate.authority_sequence
                        || anchored.phase != TopologyAdmissionPhase::Preparing
                        || anchored.preparation.as_ref().is_none_or(|prepared| {
                            prepared.compatibility != preparation.compatibility
                                || !prepared.certificates.contains(certificate)
                        })
                })
            {
                return Err(TopologyError::Invalid(
                    "certificate differs from its immutable authority append".into(),
                ));
            }
        }
        Ok(Some(descriptor))
    }
}
