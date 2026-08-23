use super::{
    OutputPartitionId, StreamGeneration, SubscriptionContractError, SubscriptionDigest,
    SubscriptionProtocolVersion,
};
use crate::checkpoint::PipelineIdentity;
use crate::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};

/// Current canonical output-distribution certificate format.
pub const OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION: u16 = 1;

const MAX_STREAM_ID_BYTES: usize = 256;
const MAX_OPERATOR_ID_BYTES: usize = 512;

/// Recoverable mapping from final operator output to subscription partitions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum OutputDistribution {
    /// Group keys use the stable partition ABI and produce output on the same vnode.
    VnodePartitioned {
        /// Digest of canonical grouping-key expressions and types.
        key_expressions_fingerprint: SubscriptionDigest,
        /// Exact key encoding and vnode mapping ABI.
        partition_abi: u16,
        /// Complete logical vnode domain.
        vnode_count: u16,
    },
    /// Existing global-aggregate execution on the singleton vnode.
    Singleton {
        /// Singleton/global vnode.
        partition: OutputPartitionId,
    },
}

impl OutputDistribution {
    /// Number of certified output partitions.
    #[must_use]
    pub const fn partition_count(&self) -> u16 {
        match self {
            Self::VnodePartitioned { vnode_count, .. } => *vnode_count,
            Self::Singleton { .. } => 1,
        }
    }

    pub(crate) fn validate(
        &self,
        key_group_count: KeyGroupCount,
    ) -> Result<(), SubscriptionContractError> {
        match self {
            Self::VnodePartitioned {
                key_expressions_fingerprint,
                partition_abi,
                vnode_count,
            } => {
                key_expressions_fingerprint.validate("key_expressions_fingerprint")?;
                if *partition_abi != PARTITIONING_ABI_VERSION {
                    return Err(SubscriptionContractError::PartitionAbi {
                        actual: *partition_abi,
                        expected: PARTITIONING_ABI_VERSION,
                    });
                }
                if *vnode_count != key_group_count.get() {
                    return Err(SubscriptionContractError::VnodeCount {
                        actual: *vnode_count,
                        expected: key_group_count.get(),
                    });
                }
            }
            Self::Singleton { partition } => {
                partition.validate(key_group_count)?;
                if partition.get() != 0 {
                    return Err(SubscriptionContractError::NonCanonicalSingleton {
                        partition: partition.get(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Whether `partition` belongs to this certified output domain.
    #[must_use]
    pub fn contains(&self, partition: OutputPartitionId) -> bool {
        match self {
            Self::VnodePartitioned { vnode_count, .. } => partition.get() < *vnode_count,
            Self::Singleton {
                partition: singleton,
            } => partition == *singleton,
        }
    }
}

/// Changelog representation bound into an output-distribution certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangelogMode {
    /// Rows are append-only.
    Append,
    /// Each frame replaces the visible contents of its output partition.
    FullPartitionSnapshot,
    /// Updates use the existing `__weight` retract-before-insert convention.
    WeightedRetractInsert,
}

/// Planner proof that a final operator has a stable, recoverable output distribution.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputDistributionCertificate {
    /// Exact certificate envelope format.
    pub version: u16,
    /// Subscription protocol used by the output.
    pub protocol_version: SubscriptionProtocolVersion,
    /// Canonical catalog stream name.
    pub stream_id: String,
    /// Monotonic durable catalog incarnation.
    pub catalog_generation: u64,
    /// Derived durable incarnation identity.
    pub stream_generation: StreamGeneration,
    /// Stable final graph operator identity.
    pub final_operator_id: String,
    /// Certified output-to-partition mapping.
    pub distribution: OutputDistribution,
    /// Fingerprint of the unchanged user output schema.
    pub schema_fingerprint: SubscriptionDigest,
    /// Changelog representation emitted by the final operator.
    pub changelog_mode: ChangelogMode,
    /// Maximum committed output bytes retained for epoch replay; zero admits tail only.
    pub history_retention_bytes: u64,
    /// Canonical query fingerprint.
    pub query_fingerprint: SubscriptionDigest,
    /// Complete pipeline/state ABI identity.
    pub pipeline_identity: PipelineIdentity,
}

impl OutputDistributionCertificate {
    /// Validate the canonical certificate and all external bindings.
    ///
    /// # Errors
    /// Returns the first non-canonical or mismatched durable binding.
    pub fn validate(
        &self,
        key_group_count: KeyGroupCount,
    ) -> Result<(), SubscriptionContractError> {
        if self.version != OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION {
            return Err(SubscriptionContractError::DistributionCertificateVersion {
                actual: self.version,
                expected: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
            });
        }
        self.protocol_version.validate()?;
        validate_identity("stream_id", &self.stream_id, MAX_STREAM_ID_BYTES)?;
        validate_identity(
            "final_operator_id",
            &self.final_operator_id,
            MAX_OPERATOR_ID_BYTES,
        )?;
        if self.catalog_generation == 0 {
            return Err(SubscriptionContractError::ZeroCatalogGeneration);
        }
        self.stream_generation.validate()?;
        self.schema_fingerprint.validate("schema_fingerprint")?;
        self.query_fingerprint.validate("query_fingerprint")?;
        if !self.pipeline_identity.is_canonical() {
            return Err(SubscriptionContractError::PipelineIdentity);
        }
        self.distribution.validate(key_group_count)
    }

    /// Validate exact equality with the certificate selected by the current planner.
    ///
    /// # Errors
    /// Returns a certificate mismatch when any durable binding differs.
    pub fn require_match(&self, expected: &Self) -> Result<(), SubscriptionContractError> {
        if self != expected {
            return Err(SubscriptionContractError::DistributionCertificateMismatch);
        }
        Ok(())
    }

    /// Whether this certificate represents the initial externally admitted stream class.
    #[must_use]
    pub fn certifies_keyed_aggregate(&self) -> bool {
        matches!(
            self.distribution,
            OutputDistribution::VnodePartitioned { .. }
        ) && self.changelog_mode == ChangelogMode::WeightedRetractInsert
    }
}

fn validate_identity(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), SubscriptionContractError> {
    if value.is_empty() || value.trim() != value || value.len() > max_bytes {
        return Err(SubscriptionContractError::NonCanonicalIdentity { field, max_bytes });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn certificate() -> OutputDistributionCertificate {
        OutputDistributionCertificate {
            version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            stream_id: "positions".into(),
            catalog_generation: 1,
            stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes(
                [1; 32],
            )),
            final_operator_id: "stream:positions".into(),
            distribution: OutputDistribution::VnodePartitioned {
                key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
                partition_abi: PARTITIONING_ABI_VERSION,
                vnode_count: 4,
            },
            schema_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
            changelog_mode: ChangelogMode::WeightedRetractInsert,
            history_retention_bytes: 0,
            query_fingerprint: SubscriptionDigest::from_bytes([4; 32]),
            pipeline_identity: PipelineIdentity::empty(),
        }
    }

    #[test]
    fn certificate_mismatch_is_structured() {
        let actual = certificate();
        let mut expected = actual.clone();
        expected.schema_fingerprint = SubscriptionDigest::from_bytes([9; 32]);
        assert_eq!(
            actual.require_match(&expected),
            Err(SubscriptionContractError::DistributionCertificateMismatch)
        );
    }

    #[test]
    fn protocol_and_schema_fingerprint_fail_closed() {
        let groups = KeyGroupCount::try_from(4_u16).unwrap();
        let mut wrong_protocol = certificate();
        wrong_protocol.protocol_version = SubscriptionProtocolVersion::new(2);
        assert!(matches!(
            wrong_protocol.validate(groups),
            Err(SubscriptionContractError::ProtocolVersion { .. })
        ));

        let mut wrong_schema = certificate();
        wrong_schema.schema_fingerprint = SubscriptionDigest::from_bytes([0; 32]);
        assert_eq!(
            wrong_schema.validate(groups),
            Err(SubscriptionContractError::ZeroDigest {
                field: "schema_fingerprint"
            })
        );
    }
}
