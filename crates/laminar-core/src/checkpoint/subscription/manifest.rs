use serde::Serialize;

use super::{
    OutputDistribution, OutputDistributionCertificate, OutputPartitionId, PartitionSequence,
    StreamGeneration, SubscriptionDigest, SubscriptionProtocolVersion,
};
use crate::checkpoint::{canonical_json_bytes, CheckpointAssignmentFence};
use crate::state::KeyGroupCount;

/// Hard segment-reference count bound for one stream checkpoint manifest.
pub const MAX_OUTPUT_SEGMENTS_PER_MANIFEST: usize = 65_536;

/// Hard canonical encoded size bound for one stream checkpoint manifest.
pub const MAX_SUBSCRIPTION_MANIFEST_BYTES: usize = 8 * 1024 * 1024;

/// Hard encoded-object bound for one immutable Arrow output segment.
pub const MAX_OUTPUT_SEGMENT_BYTES: usize = 16 * 1024 * 1024;

/// Hard frame-count bound for one immutable Arrow output segment.
pub const MAX_OUTPUT_FRAMES_PER_SEGMENT: u64 = 1_024;

const MAX_OBJECT_KEY_BYTES: usize = 2_048;

/// First partition sequence not covered by a committed checkpoint.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct PartitionFrontier {
    /// Stable vnode output partition.
    pub partition: OutputPartitionId,
    /// Every sequence below this exclusive frontier is covered by the checkpoint.
    pub through_sequence: PartitionSequence,
}

/// Exact immutable object containing a contiguous partition-frame range.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputSegmentRef {
    /// Exact segment envelope protocol.
    pub protocol_version: SubscriptionProtocolVersion,
    /// Provider-neutral object key inside the checkpoint namespace.
    pub object_key: String,
    /// Durable stream incarnation.
    pub stream_generation: StreamGeneration,
    /// Stable vnode partition.
    pub partition: OutputPartitionId,
    /// First frame sequence in the segment.
    pub first_sequence: PartitionSequence,
    /// Exclusive frame sequence at the end of the segment.
    pub exclusive_end_sequence: PartitionSequence,
    /// Exact number of frames.
    pub frame_count: u64,
    /// Exact number of Arrow rows across all frames.
    pub row_count: u64,
    /// Exact encoded object length.
    pub encoded_length: u64,
    /// Fingerprint of every frame's user schema.
    pub schema_fingerprint: SubscriptionDigest,
    /// SHA-256 of the exact object bytes.
    pub payload_digest: SubscriptionDigest,
}

impl OutputSegmentRef {
    pub(crate) fn validate(
        &self,
        certificate: &OutputDistributionCertificate,
    ) -> Result<(), SubscriptionContractError> {
        self.protocol_version.validate()?;
        if self.object_key.is_empty()
            || self.object_key.len() > MAX_OBJECT_KEY_BYTES
            || self.object_key.starts_with('/')
            || self.object_key.contains('\\')
            || self
                .object_key
                .split('/')
                .any(|component| component.is_empty() || matches!(component, "." | ".."))
        {
            return Err(SubscriptionContractError::NonCanonicalObjectKey);
        }
        if self.stream_generation != certificate.stream_generation {
            return Err(SubscriptionContractError::GenerationMismatch);
        }
        if !certificate.distribution.contains(self.partition) {
            return Err(SubscriptionContractError::PartitionOutOfRange {
                partition: self.partition.get(),
                vnode_count: certificate.distribution.partition_count(),
            });
        }
        if self.schema_fingerprint != certificate.schema_fingerprint {
            return Err(SubscriptionContractError::SchemaMismatch);
        }
        self.payload_digest.validate("payload_digest")?;
        let range = self
            .exclusive_end_sequence
            .get()
            .checked_sub(self.first_sequence.get())
            .ok_or(SubscriptionContractError::InvalidSegmentRange)?;
        if range == 0
            || range != self.frame_count
            || self.frame_count > MAX_OUTPUT_FRAMES_PER_SEGMENT
            || self.row_count == 0
            || self.encoded_length == 0
            || self.encoded_length > u64::try_from(MAX_OUTPUT_SEGMENT_BYTES).unwrap_or(u64::MAX)
        {
            return Err(SubscriptionContractError::InvalidSegmentRange);
        }
        Ok(())
    }
}

/// Canonical subscription output bound into one whole-cluster checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionCheckpointManifest {
    /// Exact subscription protocol.
    pub protocol_version: SubscriptionProtocolVersion,
    /// Planner-owned distribution proof.
    pub distribution_certificate: OutputDistributionCertificate,
    /// Durable stream incarnation, repeated for direct validation.
    pub stream_generation: StreamGeneration,
    /// Checkpoint epoch.
    pub epoch: u64,
    /// Checkpoint id; equal to `epoch`.
    pub checkpoint_id: u64,
    /// Exact assignment and process-incarnation fence.
    pub assignment_certificate: CheckpointAssignmentFence,
    /// Fingerprint of the unchanged user output schema.
    pub schema_fingerprint: SubscriptionDigest,
    /// Complete canonical output-partition roster and exclusive frontiers.
    pub frontiers: Vec<PartitionFrontier>,
    /// Canonically sorted immutable segment references.
    pub segments: Vec<OutputSegmentRef>,
    /// SHA-256 of every preceding field in this manifest.
    pub manifest_digest: SubscriptionDigest,
}

impl SubscriptionCheckpointManifest {
    /// Compute and install the canonical manifest digest.
    ///
    /// # Errors
    /// Returns the first encoding, binding, roster, range, or size validation failure.
    pub fn seal(&mut self) -> Result<(), SubscriptionContractError> {
        self.manifest_digest = self.computed_digest()?;
        self.validate()
    }

    /// Validate canonical shape, exact roster, bindings, and digest.
    ///
    /// # Errors
    /// Returns the first malformed, mismatched, discontinuous, or oversized field.
    pub fn validate(&self) -> Result<(), SubscriptionContractError> {
        self.protocol_version.validate()?;
        if self.epoch == 0 || self.checkpoint_id != self.epoch {
            return Err(SubscriptionContractError::CheckpointAttempt);
        }
        let key_group_count = KeyGroupCount::try_from(self.assignment_certificate.vnode_count)
            .map_err(|_| SubscriptionContractError::AssignmentCertificate)?;
        if !self.assignment_certificate.is_canonical() {
            return Err(SubscriptionContractError::AssignmentCertificate);
        }
        self.distribution_certificate.validate(key_group_count)?;
        if self.stream_generation != self.distribution_certificate.stream_generation {
            return Err(SubscriptionContractError::GenerationMismatch);
        }
        if self.schema_fingerprint != self.distribution_certificate.schema_fingerprint {
            return Err(SubscriptionContractError::SchemaMismatch);
        }
        validate_frontiers(
            &self.frontiers,
            &self.distribution_certificate.distribution,
            key_group_count,
        )?;
        validate_segments(
            &self.segments,
            &self.frontiers,
            &self.distribution_certificate,
        )?;
        if self.manifest_digest != self.computed_digest()? {
            return Err(SubscriptionContractError::ManifestDigest);
        }
        let encoded = canonical_json_bytes(self)
            .map_err(|error| SubscriptionContractError::Encode(error.to_string()))?;
        if encoded.len() > MAX_SUBSCRIPTION_MANIFEST_BYTES {
            return Err(SubscriptionContractError::ManifestTooLarge {
                actual: encoded.len(),
                limit: MAX_SUBSCRIPTION_MANIFEST_BYTES,
            });
        }
        Ok(())
    }

    fn computed_digest(&self) -> Result<SubscriptionDigest, SubscriptionContractError> {
        let body = ManifestBody {
            protocol_version: self.protocol_version,
            distribution_certificate: &self.distribution_certificate,
            stream_generation: self.stream_generation,
            epoch: self.epoch,
            checkpoint_id: self.checkpoint_id,
            assignment_certificate: &self.assignment_certificate,
            schema_fingerprint: self.schema_fingerprint,
            frontiers: &self.frontiers,
            segments: &self.segments,
        };
        let encoded = canonical_json_bytes(&body)
            .map_err(|error| SubscriptionContractError::Encode(error.to_string()))?;
        Ok(SubscriptionDigest::for_bytes(
            b"laminardb-subscription-checkpoint-manifest-v1",
            &encoded,
        ))
    }
}

#[derive(Serialize)]
struct ManifestBody<'a> {
    protocol_version: SubscriptionProtocolVersion,
    distribution_certificate: &'a OutputDistributionCertificate,
    stream_generation: StreamGeneration,
    epoch: u64,
    checkpoint_id: u64,
    assignment_certificate: &'a CheckpointAssignmentFence,
    schema_fingerprint: SubscriptionDigest,
    frontiers: &'a [PartitionFrontier],
    segments: &'a [OutputSegmentRef],
}

fn validate_frontiers(
    frontiers: &[PartitionFrontier],
    distribution: &OutputDistribution,
    key_group_count: KeyGroupCount,
) -> Result<(), SubscriptionContractError> {
    if frontiers.is_empty() {
        return Err(SubscriptionContractError::MissingPartitionFrontier);
    }
    for frontier in frontiers {
        frontier.partition.validate(key_group_count)?;
        if !distribution.contains(frontier.partition) {
            return Err(SubscriptionContractError::PartitionOutOfRange {
                partition: frontier.partition.get(),
                vnode_count: distribution.partition_count(),
            });
        }
    }
    if !frontiers
        .windows(2)
        .all(|pair| pair[0].partition < pair[1].partition)
    {
        return Err(SubscriptionContractError::NonCanonicalFrontiers);
    }
    let exact_roster = match distribution {
        OutputDistribution::VnodePartitioned { vnode_count, .. } => {
            frontiers.len() == usize::from(*vnode_count)
                && frontiers
                    .iter()
                    .map(|frontier| frontier.partition.get())
                    .eq(0..*vnode_count)
        }
        OutputDistribution::Singleton { partition } => {
            frontiers.len() == 1 && frontiers[0].partition == *partition
        }
    };
    if !exact_roster {
        return Err(SubscriptionContractError::MissingPartitionFrontier);
    }
    Ok(())
}

fn validate_segments(
    segments: &[OutputSegmentRef],
    frontiers: &[PartitionFrontier],
    certificate: &OutputDistributionCertificate,
) -> Result<(), SubscriptionContractError> {
    if segments.len() > MAX_OUTPUT_SEGMENTS_PER_MANIFEST {
        return Err(SubscriptionContractError::TooManySegments {
            actual: segments.len(),
            limit: MAX_OUTPUT_SEGMENTS_PER_MANIFEST,
        });
    }
    for segment in segments {
        segment.validate(certificate)?;
        let frontier = frontiers
            .binary_search_by_key(&segment.partition, |frontier| frontier.partition)
            .ok()
            .map(|index| frontiers[index].through_sequence)
            .ok_or(SubscriptionContractError::MissingPartitionFrontier)?;
        if segment.exclusive_end_sequence > frontier {
            return Err(SubscriptionContractError::SegmentBeyondFrontier);
        }
    }
    if !segments.windows(2).all(|pair| {
        (pair[0].partition, pair[0].first_sequence) < (pair[1].partition, pair[1].first_sequence)
    }) {
        return Err(SubscriptionContractError::NonCanonicalSegments);
    }
    for pair in segments.windows(2) {
        if pair[0].partition == pair[1].partition
            && pair[0].exclusive_end_sequence > pair[1].first_sequence
        {
            return Err(SubscriptionContractError::OverlappingSegments);
        }
    }
    Ok(())
}

/// Structured validation failure for durable subscription contracts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SubscriptionContractError {
    /// Unsupported persisted or wire protocol.
    #[error("unsupported subscription protocol version {actual}; expected {expected}")]
    ProtocolVersion {
        /// Decoded protocol version.
        actual: u16,
        /// Runtime protocol version.
        expected: u16,
    },
    /// Unsupported planner certificate encoding.
    #[error("unsupported output-distribution certificate version {actual}; expected {expected}")]
    DistributionCertificateVersion {
        /// Decoded certificate version.
        actual: u16,
        /// Runtime certificate version.
        expected: u16,
    },
    /// Digest is the reserved all-zero value.
    #[error("subscription field '{field}' uses the reserved zero digest")]
    ZeroDigest {
        /// Field containing the reserved digest.
        field: &'static str,
    },
    /// Catalog generations begin at one.
    #[error("subscription catalog generation must be nonzero")]
    ZeroCatalogGeneration,
    /// Catalog or graph identity is empty, padded, or oversized.
    #[error("subscription identity '{field}' is not canonical (maximum {max_bytes} bytes)")]
    NonCanonicalIdentity {
        /// Identity field.
        field: &'static str,
        /// Hard encoded bound.
        max_bytes: usize,
    },
    /// Pipeline identity is not canonical.
    #[error("subscription pipeline identity is not canonical")]
    PipelineIdentity,
    /// Stable partitioning ABI differs.
    #[error("subscription partition ABI {actual} differs from runtime ABI {expected}")]
    PartitionAbi {
        /// Persisted ABI.
        actual: u16,
        /// Runtime ABI.
        expected: u16,
    },
    /// Certified vnode domain differs.
    #[error("subscription vnode count {actual} differs from runtime count {expected}")]
    VnodeCount {
        /// Certified vnode count.
        actual: u16,
        /// Runtime vnode count.
        expected: u16,
    },
    /// Output partition lies outside its certified vnode domain.
    #[error("subscription partition {partition} is outside vnode domain 0..{vnode_count}")]
    PartitionOutOfRange {
        /// Invalid partition.
        partition: u16,
        /// Exclusive vnode-domain end.
        vnode_count: u16,
    },
    /// Global singleton output must use vnode zero.
    #[error("global singleton output uses non-canonical partition {partition}")]
    NonCanonicalSingleton {
        /// Invalid singleton partition.
        partition: u16,
    },
    /// Restored and planned certificates are not identical.
    #[error("subscription output-distribution certificate mismatch")]
    DistributionCertificateMismatch,
    /// Partition sequence cannot advance without wrapping.
    #[error("subscription partition sequence overflow")]
    SequenceOverflow,
    /// Checkpoint attempt is zero or internally inconsistent.
    #[error("subscription checkpoint must use one nonzero canonical attempt")]
    CheckpointAttempt,
    /// Assignment fence is absent or malformed.
    #[error("subscription assignment certificate is not canonical")]
    AssignmentCertificate,
    /// Generation binding differs.
    #[error("subscription stream generation mismatch")]
    GenerationMismatch,
    /// User schema fingerprint differs.
    #[error("subscription output schema fingerprint mismatch")]
    SchemaMismatch,
    /// Frontier vector is not sorted and duplicate-free.
    #[error("subscription partition frontiers must be strictly ascending and unique")]
    NonCanonicalFrontiers,
    /// Complete certified partition roster is absent.
    #[error("subscription checkpoint is missing a certified partition frontier")]
    MissingPartitionFrontier,
    /// Segment object key is not a bounded provider-neutral relative path.
    #[error("subscription segment object key is not canonical")]
    NonCanonicalObjectKey,
    /// Segment range or counts are inconsistent.
    #[error("subscription segment has an invalid sequence range or count")]
    InvalidSegmentRange,
    /// Segment references are not canonically sorted and unique.
    #[error("subscription segment references are not canonical")]
    NonCanonicalSegments,
    /// Segment ranges overlap within one partition.
    #[error("subscription segment ranges overlap")]
    OverlappingSegments,
    /// Segment extends beyond the checkpoint frontier.
    #[error("subscription segment extends beyond its checkpoint frontier")]
    SegmentBeyondFrontier,
    /// Segment reference count exceeds the hard limit.
    #[error("subscription manifest has {actual} segments; maximum is {limit}")]
    TooManySegments {
        /// Segment count.
        actual: usize,
        /// Hard segment-count bound.
        limit: usize,
    },
    /// Encoded manifest exceeds the hard limit.
    #[error("subscription manifest is {actual} bytes; maximum is {limit}")]
    ManifestTooLarge {
        /// Encoded byte count.
        actual: usize,
        /// Hard encoded-size bound.
        limit: usize,
    },
    /// Canonical metadata encoding failed.
    #[error("subscription manifest encoding failed: {0}")]
    Encode(String),
    /// Manifest body does not match its content digest.
    #[error("subscription manifest digest mismatch")]
    ManifestDigest,
    /// Participant-local stream inventory is not sorted and unique.
    #[error("subscription node streams must be strictly sorted and unique")]
    NonCanonicalNodeStreams,
    /// Participant-local partition ranges are not sorted and unique.
    #[error("subscription node partition ranges must be strictly sorted and unique")]
    NonCanonicalPartitionRanges,
    /// A participant claimed an output partition it does not own.
    #[error("subscription partition {partition} was published by the wrong owner")]
    PartitionOwnerMismatch {
        /// Partition claimed by the wrong participant.
        partition: u16,
    },
    /// Participant identity is missing from its assignment certificate.
    #[error("subscription node participant binding is invalid")]
    NodeParticipant,
    /// Participant manifest body does not match its content digest.
    #[error("subscription node manifest digest mismatch")]
    NodeManifestDigest,
    /// A committed partition interval is discontinuous.
    #[error(
        "subscription partition {partition} sequence gap: expected {expected}, found {actual}"
    )]
    SequenceGap {
        /// Partition containing the gap.
        partition: u16,
        /// Required next sequence.
        expected: u64,
        /// Observed next sequence.
        actual: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CheckpointParticipant, PipelineIdentity};
    use crate::state::PARTITIONING_ABI_VERSION;

    fn certificate() -> OutputDistributionCertificate {
        OutputDistributionCertificate {
            version: super::super::OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
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
            changelog_mode: super::super::ChangelogMode::WeightedRetractInsert,
            history_retention_bytes: 0,
            query_fingerprint: SubscriptionDigest::from_bytes([4; 32]),
            pipeline_identity: PipelineIdentity::empty(),
        }
    }

    fn assignment() -> CheckpointAssignmentFence {
        CheckpointAssignmentFence::from_owner_map(
            1,
            &[7, 7, 7, 7],
            vec![CheckpointParticipant {
                node_id: 7,
                boot_incarnation: uuid::Uuid::parse_str("77777777-7777-4777-8777-777777777777")
                    .unwrap(),
            }],
        )
        .unwrap()
    }

    fn manifest() -> SubscriptionCheckpointManifest {
        let certificate = certificate();
        let mut manifest = SubscriptionCheckpointManifest {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            stream_generation: certificate.stream_generation,
            schema_fingerprint: certificate.schema_fingerprint,
            distribution_certificate: certificate,
            epoch: 10,
            checkpoint_id: 10,
            assignment_certificate: assignment(),
            frontiers: (0..4)
                .map(|partition| PartitionFrontier {
                    partition: OutputPartitionId::new(partition),
                    through_sequence: PartitionSequence::new(0),
                })
                .collect(),
            segments: Vec::new(),
            manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
        };
        manifest.seal().unwrap();
        manifest
    }

    #[test]
    fn canonical_frontier_vector_round_trips() {
        let manifest = manifest();
        let encoded = canonical_json_bytes(&manifest).unwrap();
        let decoded: SubscriptionCheckpointManifest = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, manifest);
        decoded.validate().unwrap();
    }

    #[test]
    fn duplicate_missing_and_out_of_range_partitions_are_rejected() {
        let mut duplicate = manifest();
        duplicate.frontiers[2].partition = duplicate.frontiers[1].partition;
        assert_eq!(
            duplicate.validate(),
            Err(SubscriptionContractError::NonCanonicalFrontiers)
        );

        let mut missing = manifest();
        missing.frontiers.pop();
        assert_eq!(
            missing.validate(),
            Err(SubscriptionContractError::MissingPartitionFrontier)
        );

        let mut out_of_range = manifest();
        out_of_range.frontiers[3].partition = OutputPartitionId::new(4);
        assert_eq!(
            out_of_range.validate(),
            Err(SubscriptionContractError::PartitionOutOfRange {
                partition: 4,
                vnode_count: 4,
            })
        );
    }

    #[test]
    fn generation_and_schema_mismatch_are_rejected_before_digest() {
        let mut wrong_generation = manifest();
        wrong_generation.stream_generation =
            StreamGeneration::from_digest(SubscriptionDigest::from_bytes([8; 32]));
        assert_eq!(
            wrong_generation.validate(),
            Err(SubscriptionContractError::GenerationMismatch)
        );

        let mut wrong_schema = manifest();
        wrong_schema.schema_fingerprint = SubscriptionDigest::from_bytes([8; 32]);
        assert_eq!(
            wrong_schema.validate(),
            Err(SubscriptionContractError::SchemaMismatch)
        );
    }

    #[test]
    fn manifest_digest_detects_metadata_mutation() {
        let mut manifest = manifest();
        manifest.frontiers[0].through_sequence = PartitionSequence::new(1);
        assert_eq!(
            manifest.validate(),
            Err(SubscriptionContractError::ManifestDigest)
        );
    }

    #[test]
    fn segment_reference_rejects_allocation_sized_counts_and_lengths() {
        let certificate = certificate();
        let mut segment = OutputSegmentRef {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            object_key: "subscription-output/deployment/stream/generation/0/segment.arrow".into(),
            stream_generation: certificate.stream_generation,
            partition: OutputPartitionId::new(0),
            first_sequence: PartitionSequence::FIRST,
            exclusive_end_sequence: PartitionSequence::new(MAX_OUTPUT_FRAMES_PER_SEGMENT + 1),
            frame_count: MAX_OUTPUT_FRAMES_PER_SEGMENT + 1,
            row_count: 1,
            encoded_length: 1,
            schema_fingerprint: certificate.schema_fingerprint,
            payload_digest: SubscriptionDigest::from_bytes([5; 32]),
        };
        assert_eq!(
            segment.validate(&certificate),
            Err(SubscriptionContractError::InvalidSegmentRange)
        );

        segment.exclusive_end_sequence = PartitionSequence::new(1);
        segment.frame_count = 1;
        segment.encoded_length = u64::try_from(MAX_OUTPUT_SEGMENT_BYTES).unwrap() + 1;
        assert_eq!(
            segment.validate(&certificate),
            Err(SubscriptionContractError::InvalidSegmentRange)
        );
    }

    #[test]
    fn overlapping_segment_ranges_are_rejected() {
        let mut manifest = manifest();
        manifest.frontiers[0].through_sequence = PartitionSequence::new(3);
        let segment = |first, end, marker| OutputSegmentRef {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            object_key: format!("subscription-output/test/{marker}.arrow"),
            stream_generation: manifest.stream_generation,
            partition: OutputPartitionId::new(0),
            first_sequence: PartitionSequence::new(first),
            exclusive_end_sequence: PartitionSequence::new(end),
            frame_count: end - first,
            row_count: 1,
            encoded_length: 1,
            schema_fingerprint: manifest.schema_fingerprint,
            payload_digest: SubscriptionDigest::from_bytes([5; 32]),
        };
        manifest.segments = vec![segment(0, 2, "first"), segment(1, 3, "second")];
        assert_eq!(
            manifest.validate(),
            Err(SubscriptionContractError::OverlappingSegments)
        );
    }
}
