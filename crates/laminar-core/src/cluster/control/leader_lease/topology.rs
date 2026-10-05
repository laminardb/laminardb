//! Baseline adoption shares the exact authority append with lease/checkpoint/recovery writes.

use std::time::Duration;

use super::{
    AuthorityCreateOutcome, CheckpointDecisionStore, LeaderAuthorityRecord, LeaderLeaseStore,
    LeaderProof, LeaseError, AUTHORITY_RECORD_VERSION, TOPOLOGY_ADMISSION_RECORD_VERSION,
    TOPOLOGY_AUTHORITY_RECORD_VERSION, TOPOLOGY_COMMIT_RECORD_VERSION, TOPOLOGY_CUT_RECORD_VERSION,
    TOPOLOGY_INSTALLATION_RECORD_VERSION, TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION,
    TOPOLOGY_PREPARATION_RECORD_VERSION, TOPOLOGY_RECOVERY_RECORD_VERSION,
    TOPOLOGY_SOURCE_ROOT_RECORD_VERSION, TOPOLOGY_SUBMISSION_RECORD_VERSION,
    TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION,
};
use crate::cluster::control::topology::{
    LegacyTopologyBaseline, TopologyAdoptionOutcome, TopologyCatalogState, TopologyError,
    TopologyOperationId, TopologyVersion, TOPOLOGY_PROTOCOL_VERSION,
};
use crate::cluster::control::CatalogManifestRef;

const MAX_TOPOLOGY_CAS_ATTEMPTS: usize = 16;
const TOPOLOGY_ADOPTION_TIMEOUT: Duration = Duration::from_secs(15);
const TOPOLOGY_READ_TIMEOUT: Duration = Duration::from_secs(15);

impl LeaderAuthorityRecord {
    pub(super) fn validate_topology_baseline(&self) -> Result<(), LeaseError> {
        match (self.version, self.topology_baseline.as_ref()) {
            (AUTHORITY_RECORD_VERSION, None) => Ok(()),
            (TOPOLOGY_ADMISSION_RECORD_VERSION, None) if self.topology_operations.is_empty() => {
                Ok(())
            }
            (
                TOPOLOGY_AUTHORITY_RECORD_VERSION
                | TOPOLOGY_ADMISSION_RECORD_VERSION
                | TOPOLOGY_CUT_RECORD_VERSION
                | TOPOLOGY_PREPARATION_RECORD_VERSION
                | TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION
                | TOPOLOGY_SOURCE_ROOT_RECORD_VERSION
                | TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION
                | TOPOLOGY_COMMIT_RECORD_VERSION
                | TOPOLOGY_INSTALLATION_RECORD_VERSION
                | TOPOLOGY_RECOVERY_RECORD_VERSION
                | TOPOLOGY_SUBMISSION_RECORD_VERSION,
                // Installation capability is an explicit coordinated binary format upgrade.
                Some(baseline),
            ) => {
                baseline
                    .validate()
                    .map_err(|error| LeaseError::Invalid(error.to_string()))?;
                if baseline.authority_sequence > self.lease.seq {
                    return Err(LeaseError::Invalid(
                        "topology baseline does not bind the sealed catalog and authority sequence"
                            .into(),
                    ));
                }
                self.validate_committed_topology_chain(baseline)?;
                Ok(())
            }
            _ => Err(LeaseError::Invalid(
                "authority encoding and topology baseline presence disagree".into(),
            )),
        }
    }

    pub(super) fn validate_topology_successor(&self, next: &Self) -> Result<(), LeaseError> {
        if let Some(baseline) = self.topology_baseline.as_ref() {
            if next.version < self.version || next.topology_baseline.as_ref() != Some(baseline) {
                return Err(LeaseError::Invalid(
                    "authority append cannot downgrade or replace an adopted topology baseline"
                        .into(),
                ));
            }
        } else if let Some(baseline) = next.topology_baseline.as_ref() {
            if !matches!(
                next.version,
                TOPOLOGY_AUTHORITY_RECORD_VERSION
                    | TOPOLOGY_ADMISSION_RECORD_VERSION
                    | TOPOLOGY_CUT_RECORD_VERSION
                    | TOPOLOGY_PREPARATION_RECORD_VERSION
            ) || baseline.authority_sequence != next.lease.seq
                || self.lease.catalog_manifest.as_ref() != Some(&baseline.manifest)
            {
                return Err(LeaseError::Invalid(
                    "topology adoption must preserve the predecessor inventory at its exact append"
                        .into(),
                ));
            }
        }
        self.validate_committed_topology_successor(next)?;
        Ok(())
    }
}

impl LeaderLeaseStore {
    /// Read explicit topology metadata and validate its referenced inventory/deployment.
    ///
    /// Missing version metadata means `LegacySealed`, never an inferred topology version. This
    /// method never appends a topology operation or initializes a deployment identity. Existing
    /// authority head reconciliation may finish publication of a previously created record.
    ///
    /// # Errors
    /// Fails closed on malformed, missing or corrupt referenced content or authority.
    pub async fn topology_catalog_state(&self) -> Result<TopologyCatalogState, TopologyError> {
        Ok(self
            .catalog_with_topology()
            .await?
            .map_or(TopologyCatalogState::Uninitialized, |(_, state)| state))
    }

    pub(in crate::cluster::control) async fn catalog_with_topology(
        &self,
    ) -> Result<Option<(super::CatalogManifest, TopologyCatalogState)>, TopologyError> {
        tokio::time::timeout(TOPOLOGY_READ_TIMEOUT, self.catalog_with_topology_inner())
            .await
            .map_err(|_| TopologyError::ReadTimedOut)?
    }

    async fn catalog_with_topology_inner(
        &self,
    ) -> Result<Option<(super::CatalogManifest, TopologyCatalogState)>, TopologyError> {
        let Some(head) = self.load_record().await? else {
            return Ok(None);
        };
        let Some(manifest) = head.lease.catalog_manifest.as_ref() else {
            return Ok(None);
        };
        let inventory = self.load_catalog_manifest(manifest).await?;
        let state = match head.topology_baseline.as_ref() {
            None => TopologyCatalogState::LegacySealed {
                manifest: manifest.clone(),
            },
            Some(baseline) => {
                self.audit_topology_adoption(baseline).await?;
                self.require_topology_deployment(&baseline.deployment_id)
                    .await?;
                let committed = if let Some(operation) = head.committed_topology_operation() {
                    self.audit_topology_operation(operation).await?;
                    operation.commit.clone()
                } else {
                    None
                };
                TopologyCatalogState::Versioned {
                    baseline: baseline.clone(),
                    committed,
                }
            }
        };
        Ok(Some((inventory, state)))
    }

    pub(super) async fn audit_topology_adoption(
        &self,
        baseline: &LegacyTopologyBaseline,
    ) -> Result<(), LeaseError> {
        let record = super::read_authority_record(self.store.as_ref(), baseline.authority_sequence)
            .await?
            .ok_or_else(|| LeaseError::Invalid("topology adoption authority is missing".into()))?;
        if !matches!(
            record.version,
            TOPOLOGY_AUTHORITY_RECORD_VERSION
                | TOPOLOGY_ADMISSION_RECORD_VERSION
                | TOPOLOGY_CUT_RECORD_VERSION
                | TOPOLOGY_PREPARATION_RECORD_VERSION
        ) || record.topology_baseline.as_ref() != Some(baseline)
        {
            return Err(LeaseError::Invalid(
                "topology baseline disagrees with its retained adoption authority".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn require_topology_deployment(
        &self,
        expected: &str,
    ) -> Result<(), TopologyError> {
        let actual = CheckpointDecisionStore::new(self.store.clone())
            .load_deployment_id()
            .await
            .map_err(|error| match error {
                crate::checkpoint_decision::DecisionError::Io(reason) => {
                    TopologyError::Authority(LeaseError::Io(reason))
                }
                error => TopologyError::Invalid(error.to_string()),
            })?;
        if actual.as_deref() != Some(expected) {
            return Err(TopologyError::Conflict(
                "topology deployment does not match the existing authority deployment; do not recreate or reset the namespace".into(),
            ));
        }
        Ok(())
    }

    /// Adopt the exact legacy inventory as topology one, preserving all existing authority.
    ///
    /// Call only after a coordinated binary upgrade. The new encoding causes older authority
    /// readers/writers to fail closed; it does not revoke already-running data-plane actors.
    /// Adoption itself never changes the graph, opens gates, or authorizes runtime DDL.
    ///
    /// A timeout/cancelled request may have committed. Reread `topology_catalog_state` and retry
    /// the same operation identity/reference. Never infer abort from a lost response.
    ///
    /// # Errors
    /// Rejects a stale leader proof, changed manifest/deployment, unsealed namespace, malformed
    /// artifacts, storage errors, or bounded deadline/CAS exhaustion.
    pub async fn adopt_legacy_topology(
        &self,
        proof: &LeaderProof,
        operation_id: TopologyOperationId,
        expected_manifest: &CatalogManifestRef,
        expected_deployment: &str,
    ) -> Result<TopologyAdoptionOutcome, TopologyError> {
        tokio::time::timeout(
            TOPOLOGY_ADOPTION_TIMEOUT,
            self.adopt_legacy_topology_inner(
                proof,
                operation_id,
                expected_manifest,
                expected_deployment,
            ),
        )
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    async fn adopt_legacy_topology_inner(
        &self,
        proof: &LeaderProof,
        operation_id: TopologyOperationId,
        expected_manifest: &CatalogManifestRef,
        expected_deployment: &str,
    ) -> Result<TopologyAdoptionOutcome, TopologyError> {
        if !proof.is_canonical() {
            return Err(TopologyError::Fenced);
        }
        expected_manifest.validate()?;
        self.require_topology_deployment(expected_deployment)
            .await?;
        let initial = self.load_record().await?.ok_or(TopologyError::Fenced)?;
        if !initial.lease.matches_proof(proof) {
            return Err(TopologyError::Fenced);
        }
        if initial.lease.catalog_manifest.as_ref() != Some(expected_manifest) {
            return Err(TopologyError::Conflict(
                "legacy adoption requires the exact currently sealed manifest".into(),
            ));
        }
        // Validate the original blob before any upgrade append. Do not re-encode or replace it.
        self.load_catalog_manifest(expected_manifest).await?;

        for _ in 0..MAX_TOPOLOGY_CAS_ATTEMPTS {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(TopologyError::Fenced)?;
            let current = &published.record;
            if !current.lease.matches_proof(proof) {
                return Err(TopologyError::Fenced);
            }
            if current.lease.catalog_manifest.as_ref() != Some(expected_manifest) {
                return Err(TopologyError::Conflict(
                    "legacy adoption requires the exact currently sealed manifest".into(),
                ));
            }
            if let Some(baseline) = current.topology_baseline.as_ref() {
                if baseline.deployment_id != expected_deployment {
                    return Err(TopologyError::Conflict(
                        "the baseline belongs to another deployment".into(),
                    ));
                }
                self.audit_topology_adoption(baseline).await?;
                return Ok(TopologyAdoptionOutcome::Existing(baseline.clone()));
            }

            let sequence = current
                .lease
                .seq
                .checked_add(1)
                .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
            let baseline = LegacyTopologyBaseline {
                protocol_version: TOPOLOGY_PROTOCOL_VERSION,
                topology_version: TopologyVersion::LEGACY_BASELINE,
                manifest: expected_manifest.clone(),
                deployment_id: expected_deployment.to_owned(),
                operation_id,
                authority_sequence: sequence,
            };
            baseline.validate()?;
            let mut lease = current.lease.clone();
            lease.seq = sequence;
            let mut candidate = current.preserve_with_lease(lease);
            candidate.version = candidate.version.max(TOPOLOGY_AUTHORITY_RECORD_VERSION);
            candidate.topology_baseline = Some(baseline.clone());
            match self
                .create_authority_record(Some(&published), &candidate)
                .await?
            {
                AuthorityCreateOutcome::Created => {
                    return Ok(TopologyAdoptionOutcome::Created(baseline));
                }
                AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(TopologyAdoptionOutcome::Existing(baseline));
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if !winner.lease.matches_proof(proof) {
                        return Err(TopologyError::Fenced);
                    }
                    if winner.lease.seq <= current.lease.seq {
                        return Err(TopologyError::Invalid(
                            "topology adoption contention did not advance authority".into(),
                        ));
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
        Err(TopologyError::Contended)
    }
}
