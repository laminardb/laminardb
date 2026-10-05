//! Historical subscription certificate continuity through released migration roots.

use super::topology_admission::CONTROL_TIMEOUT;
use super::{ClusterCheckpointAuthorityError, DecisionError, LeaderLeaseStore};
use crate::checkpoint::{CommittedCheckpointIndex, OutputDistributionCertificate};
use crate::cluster::control::{TopologyAdmissionPhase, TopologyError};

fn certificate_error(error: impl std::fmt::Display) -> ClusterCheckpointAuthorityError {
    DecisionError::Conflict(format!("subscription topology continuity: {error}")).into()
}

fn topology_error(error: TopologyError) -> ClusterCheckpointAuthorityError {
    match error {
        TopologyError::Authority(error) => error.into(),
        error => certificate_error(error),
    }
}

impl LeaderLeaseStore {
    /// Validate one unchanged subscription incarnation against an exact historical checkpoint.
    /// A changed pipeline identity requires immutable, participant-complete released roots.
    /// All other certificate fields retain strict equality. This grants artifact reads only.
    ///
    /// # Errors
    /// Rejects foreign deployment/pipeline identities, missing or corrupt transition evidence,
    /// changed incarnations/contracts, and the existing 15 second authority read budget.
    pub async fn validate_cluster_subscription_certificate(
        &self,
        index: &CommittedCheckpointIndex,
        expected: &OutputDistributionCertificate,
        actual: &OutputDistributionCertificate,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        if actual.pipeline_identity != index.pipeline_identity {
            return Err(certificate_error(
                "certificate differs from its checkpoint pipeline",
            ));
        }
        if expected.pipeline_identity == actual.pipeline_identity {
            return expected.require_match(actual).map_err(certificate_error);
        }
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let head = self
                .load_record()
                .await?
                .ok_or_else(|| certificate_error("shared authority is missing"))?;
            if head
                .topology_baseline
                .as_ref()
                .map(|baseline| baseline.deployment_id.as_str())
                != Some(index.deployment_id.as_str())
            {
                return Err(certificate_error(
                    "checkpoint deployment differs from the adopted baseline",
                ));
            }
            // Both certificates move forward along the same linear committed topology chain.
            // This supports an old live reader and a new reader replaying older checkpoints
            // without rewriting either historical manifests or segment authority bindings.
            let mut left = expected.clone();
            let mut right = actual.clone();
            for operation in head.topology_operations.iter().filter(|operation| {
                operation.phase == TopologyAdmissionPhase::Active && operation.commit.is_some()
            }) {
                let plan = self
                    .load_topology_plan(&operation.plan)
                    .await
                    .map_err(topology_error)?;
                let descriptor = self
                    .audit_topology_compatibility(&plan)
                    .await
                    .map_err(topology_error)?
                    .ok_or_else(|| {
                        certificate_error("released topology has no compatibility descriptor")
                    })?;
                if left.pipeline_identity != descriptor.parent_pipeline
                    && right.pipeline_identity != descriptor.parent_pipeline
                {
                    continue;
                }
                self.audit_topology_operation(operation).await?;
                let binding = operation
                    .migration_root
                    .as_ref()
                    .ok_or_else(|| certificate_error("released topology has no sealed root"))?;
                let root = self
                    .load_topology_root(&binding.root)
                    .await
                    .map_err(topology_error)?;
                if (index.pipeline_identity == descriptor.parent_pipeline
                    && index.epoch > root.cut.checkpoint.epoch)
                    || (index.pipeline_identity == descriptor.target_pipeline
                        && index.epoch <= root.cut.checkpoint.epoch)
                {
                    return Err(certificate_error(
                        "checkpoint lies on the wrong side of its migration cut",
                    ));
                }
                for certificate in [&mut left, &mut right] {
                    if certificate.pipeline_identity != descriptor.parent_pipeline {
                        continue;
                    }
                    let mapping = root
                        .subscriptions
                        .iter()
                        .find(|mapping| {
                            mapping.parent_certificate.stream_id == certificate.stream_id
                        })
                        .ok_or_else(|| {
                            certificate_error(
                                "stream incarnation has no preserved subscription mapping",
                            )
                        })?;
                    certificate
                        .require_match(&mapping.parent_certificate)
                        .map_err(certificate_error)?;
                    certificate.clone_from(&mapping.target_certificate);
                }
                if left == right {
                    return Ok(());
                }
            }
            Err(certificate_error(TopologyError::Conflict(
                "certificates have no exact retained released topology path".into(),
            )))
        })
        .await
        .map_err(|_| certificate_error("authority read timed out"))?
    }
}
