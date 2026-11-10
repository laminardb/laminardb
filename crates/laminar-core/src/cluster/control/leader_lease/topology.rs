//! The catalog seal binds topology one in the same authority append that publishes the inventory.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use super::{
    AuthorityCreateOutcome, CatalogManifest, CatalogManifestError, CatalogManifestRef,
    CheckpointDecisionStore, DecisionError, LeaderAuthorityRecord, LeaderLease, LeaderLeaseStore,
    LeaderProof, LeaseError, AUTHORITY_RECORD_VERSION, TOPOLOGY_ADMISSION_RECORD_VERSION,
    TOPOLOGY_AUTHORITY_RECORD_VERSION, TOPOLOGY_COMMIT_RECORD_VERSION, TOPOLOGY_CUT_RECORD_VERSION,
    TOPOLOGY_INSTALLATION_RECORD_VERSION, TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION,
    TOPOLOGY_PREPARATION_RECORD_VERSION, TOPOLOGY_RECOVERY_RECORD_VERSION,
    TOPOLOGY_SOURCE_ROOT_RECORD_VERSION, TOPOLOGY_SUBMISSION_RECORD_VERSION,
    TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION,
};
use crate::cluster::control::catalog_manifest::CatalogSealOutcome;
use crate::cluster::control::topology::{
    TopologyBaseline, TopologyCatalogState, TopologyCommit, TopologyError, TopologyJournalUsage,
    TopologyOperationId, TopologyVersion, MAX_TOPOLOGY_OPERATIONS, TOPOLOGY_PROTOCOL_VERSION,
};

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
                    "authority append cannot downgrade or replace the topology baseline".into(),
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
                || self.lease.catalog_manifest.is_some()
                || next.lease.catalog_manifest.as_ref() != Some(&baseline.manifest)
            {
                return Err(LeaseError::Invalid(
                    "the topology baseline must be written by the append that seals the catalog"
                        .into(),
                ));
            }
        } else if self.lease.catalog_manifest.is_none() && next.lease.catalog_manifest.is_some() {
            return Err(LeaseError::Invalid(
                "sealing the catalog must bind topology one in the same append".into(),
            ));
        }
        self.validate_committed_topology_successor(next)?;
        Ok(())
    }
}

impl LeaderLeaseStore {
    /// Read explicit topology metadata and validate its referenced inventory/deployment.
    ///
    /// A sealed catalog without topology metadata predates topology versioning and fails closed.
    /// This method never appends a topology operation or initializes a deployment identity.
    /// Existing authority head reconciliation may finish publication of a previously created
    /// record.
    ///
    /// # Errors
    /// Fails closed on malformed, missing or corrupt referenced content or authority.
    pub async fn topology_catalog_state(&self) -> Result<TopologyCatalogState, TopologyError> {
        Ok(self
            .catalog_with_topology()
            .await?
            .map_or(TopologyCatalogState::Uninitialized, |(_, state)| state))
    }

    /// Commit of the newest migration while it still owns startup, assignment and recovery.
    /// `None` means no committed migration or a settled target: Active with its own committed
    /// checkpoint, which then follows ordinary startup, reassignment and recovery.
    ///
    /// # Errors
    /// Fails closed on malformed or missing referenced evidence or the bounded read deadline.
    pub async fn live_topology_commit(&self) -> Result<Option<TopologyCommit>, TopologyError> {
        tokio::time::timeout(TOPOLOGY_READ_TIMEOUT, async {
            let Some(head) = self.load_record().await? else {
                return Ok(None);
            };
            let Some(operation) = head.live_topology_migration() else {
                return Ok(None);
            };
            self.audit_topology_operation(operation).await?;
            Ok(operation.commit.clone())
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    /// Retained topology request identities against the fixed journal capacity.
    ///
    /// # Errors
    /// Fails closed on malformed authority or the bounded read deadline.
    pub async fn topology_journal_usage(&self) -> Result<TopologyJournalUsage, TopologyError> {
        tokio::time::timeout(TOPOLOGY_READ_TIMEOUT, async {
            let retained = self
                .load_record()
                .await?
                .map_or(0, |head| head.topology_operations.len());
            Ok(TopologyJournalUsage {
                retained,
                capacity: MAX_TOPOLOGY_OPERATIONS,
            })
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    /// Subscription certificates preserved by the newest committed migration. Ordinary startup
    /// of a settled target binds these exact generations; deriving them from the changed pipeline
    /// identity would silently start new subscription streams.
    ///
    /// # Errors
    /// Fails closed on malformed or missing referenced evidence or the bounded read deadline.
    pub async fn committed_topology_subscriptions(
        &self,
    ) -> Result<Vec<crate::checkpoint::OutputDistributionCertificate>, TopologyError> {
        tokio::time::timeout(TOPOLOGY_READ_TIMEOUT, async {
            let Some(head) = self.load_record().await? else {
                return Ok(Vec::new());
            };
            let Some(operation) = head.committed_topology_operation() else {
                return Ok(Vec::new());
            };
            self.audit_topology_operation(operation).await?;
            let binding = operation.migration_root.as_ref().ok_or_else(|| {
                TopologyError::Invalid("committed topology has no migration root".into())
            })?;
            let root = self.load_topology_root(&binding.root).await?;
            Ok(root
                .subscriptions
                .into_iter()
                .map(|subscription| subscription.target_certificate)
                .collect())
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
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
        let baseline = head.topology_baseline.as_ref().ok_or_else(|| {
            TopologyError::Invalid(
                "the sealed cluster catalog predates topology versioning and must be recreated"
                    .into(),
            )
        })?;
        let inventory = self.load_catalog_manifest(manifest).await?;
        self.audit_topology_baseline(baseline).await?;
        self.require_topology_deployment(&baseline.deployment_id)
            .await?;
        let committed = if let Some(operation) = head.committed_topology_operation() {
            self.audit_topology_operation(operation).await?;
            operation.commit.clone()
        } else {
            None
        };
        Ok(Some((
            inventory,
            TopologyCatalogState::Versioned {
                baseline: baseline.clone(),
                committed,
            },
        )))
    }

    pub(super) async fn audit_topology_baseline(
        &self,
        baseline: &TopologyBaseline,
    ) -> Result<(), LeaseError> {
        let record = super::read_authority_record(self.store.as_ref(), baseline.authority_sequence)
            .await?
            .ok_or_else(|| LeaseError::Invalid("topology baseline authority is missing".into()))?;
        if !matches!(
            record.version,
            TOPOLOGY_AUTHORITY_RECORD_VERSION
                | TOPOLOGY_ADMISSION_RECORD_VERSION
                | TOPOLOGY_CUT_RECORD_VERSION
                | TOPOLOGY_PREPARATION_RECORD_VERSION
        ) || record.topology_baseline.as_ref() != Some(baseline)
        {
            return Err(LeaseError::Invalid(
                "topology baseline disagrees with its retained authority record".into(),
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

    /// Seal the complete catalog as topology one by appending it to the exact durable leader term.
    ///
    /// Renewals that race this operation are retried under the same proof. A takeover either
    /// observes and preserves the seal or wins the next sequence and fences this writer.
    ///
    /// # Errors
    /// Rejects malformed inventories, divergent existing inventories, stale proofs, and durable
    /// storage failures.
    pub(in crate::cluster::control) async fn seal_catalog(
        &self,
        proof: &LeaderProof,
        manifest: &CatalogManifest,
    ) -> Result<CatalogSealOutcome, CatalogManifestError> {
        if !proof.is_canonical() {
            return Err(CatalogManifestError::Fenced);
        }
        let (encoded, reference) = manifest.encode_and_reference()?;

        let current = self
            .load_record()
            .await?
            .ok_or(CatalogManifestError::Fenced)?;
        if let Some(sealed) = &current.lease.catalog_manifest {
            return self.existing_seal_outcome(sealed, manifest).await;
        }
        if !current.lease.matches_proof(proof) {
            return Err(CatalogManifestError::Fenced);
        }
        self.ensure_catalog_manifest_blob(&encoded, &reference)
            .await?;
        let deployment_id = CheckpointDecisionStore::new(Arc::clone(&self.store))
            .load_or_create_deployment_id()
            .await
            .map_err(|error| match error {
                DecisionError::Io(reason) => {
                    CatalogManifestError::Authority(LeaseError::Io(reason))
                }
                error => CatalogManifestError::Invalid(error.to_string()),
            })?;
        let operation_id = TopologyOperationId::try_from(Uuid::now_v7())
            .map_err(|error| CatalogManifestError::Invalid(error.to_string()))?;

        loop {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(CatalogManifestError::Fenced)?;
            let current = &published.record;
            if let Some(sealed) = &current.lease.catalog_manifest {
                return self.existing_seal_outcome(sealed, manifest).await;
            }
            if !current.lease.matches_proof(proof) {
                return Err(CatalogManifestError::Fenced);
            }

            let base_sequence = current.lease.seq;
            let candidate_lease = LeaderLease {
                seq: current.lease.seq.checked_add(1).ok_or_else(|| {
                    CatalogManifestError::Authority(LeaseError::Invalid(
                        "lease sequence exhausted".into(),
                    ))
                })?,
                renewal_sequence: current.lease.renewal_sequence,
                token: current.lease.token,
                owner: current.lease.owner.clone(),
                expires_at_ms: current.lease.expires_at_ms,
                catalog_manifest: Some(reference.clone()),
            };
            let mut candidate = current.preserve_with_lease(candidate_lease);
            candidate.version = candidate.version.max(TOPOLOGY_AUTHORITY_RECORD_VERSION);
            let baseline = TopologyBaseline {
                protocol_version: TOPOLOGY_PROTOCOL_VERSION,
                topology_version: TopologyVersion::BASELINE,
                manifest: reference.clone(),
                deployment_id: deployment_id.clone(),
                operation_id,
                authority_sequence: candidate.lease.seq,
            };
            candidate.topology_baseline = Some(baseline);
            match self
                .create_authority_record(Some(&published), &candidate)
                .await?
            {
                AuthorityCreateOutcome::Created => return Ok(CatalogSealOutcome::Created),
                AuthorityCreateOutcome::ExistingIdentical => {
                    return Ok(CatalogSealOutcome::ExistingIdentical);
                }
                AuthorityCreateOutcome::Contended(winner) => {
                    if let Some(sealed) = &winner.lease.catalog_manifest {
                        return self.existing_seal_outcome(sealed, manifest).await;
                    }
                    if !winner.lease.matches_proof(proof) {
                        return Err(CatalogManifestError::Fenced);
                    }
                    if winner.lease.seq <= base_sequence {
                        return Err(CatalogManifestError::Authority(LeaseError::Invalid(
                            "catalog seal contention did not advance the authority sequence".into(),
                        )));
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    async fn existing_seal_outcome(
        &self,
        sealed: &CatalogManifestRef,
        manifest: &CatalogManifest,
    ) -> Result<CatalogSealOutcome, CatalogManifestError> {
        if self.load_catalog_manifest(sealed).await? == *manifest {
            Ok(CatalogSealOutcome::ExistingIdentical)
        } else {
            Err(CatalogManifestError::Conflict)
        }
    }
}
