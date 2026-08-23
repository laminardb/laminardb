use std::collections::BTreeMap;

use serde::Serialize;

use super::{
    OutputDistributionCertificate, OutputPartitionId, OutputSegmentRef, PartitionFrontier,
    PartitionSequence, SubscriptionCheckpointManifest, SubscriptionContractError,
    SubscriptionDigest, SubscriptionProtocolVersion, MAX_OUTPUT_SEGMENTS_PER_MANIFEST,
    MAX_SUBSCRIPTION_MANIFEST_BYTES,
};
use crate::checkpoint::{canonical_json_bytes, CheckpointAssignmentFence};
use crate::state::KeyGroupCount;

/// One participant-owned output interval in a checkpoint attempt.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct NodePartitionRange {
    /// Stable vnode output partition.
    pub partition: OutputPartitionId,
    /// First sequence introduced after the preceding committed cut.
    pub first_sequence: PartitionSequence,
    /// First sequence not covered by this checkpoint.
    pub through_sequence: PartitionSequence,
}

/// One stream's participant-local portion of a checkpointed output cut.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSubscriptionStreamManifest {
    /// Planner-owned proof of the stable output distribution.
    pub distribution_certificate: OutputDistributionCertificate,
    /// Canonical participant-owned partition ranges.
    pub ranges: Vec<NodePartitionRange>,
    /// Canonical immutable segments covering every non-empty range exactly.
    pub segments: Vec<OutputSegmentRef>,
}

impl NodeSubscriptionStreamManifest {
    fn validate(
        &self,
        key_group_count: KeyGroupCount,
        owned_vnodes: &[u16],
    ) -> Result<(), SubscriptionContractError> {
        self.distribution_certificate.validate(key_group_count)?;
        if self.ranges.is_empty()
            || !self
                .ranges
                .windows(2)
                .all(|pair| pair[0].partition < pair[1].partition)
        {
            return Err(SubscriptionContractError::NonCanonicalPartitionRanges);
        }
        for range in &self.ranges {
            range.partition.validate(key_group_count)?;
            if !self
                .distribution_certificate
                .distribution
                .contains(range.partition)
                || owned_vnodes.binary_search(&range.partition.get()).is_err()
            {
                return Err(SubscriptionContractError::PartitionOwnerMismatch {
                    partition: range.partition.get(),
                });
            }
            if range.first_sequence > range.through_sequence {
                return Err(SubscriptionContractError::InvalidSegmentRange);
            }
        }
        validate_node_segments(self)
    }
}

fn validate_node_segments(
    stream: &NodeSubscriptionStreamManifest,
) -> Result<(), SubscriptionContractError> {
    if stream.segments.len() > MAX_OUTPUT_SEGMENTS_PER_MANIFEST {
        return Err(SubscriptionContractError::TooManySegments {
            actual: stream.segments.len(),
            limit: MAX_OUTPUT_SEGMENTS_PER_MANIFEST,
        });
    }
    if !stream.segments.windows(2).all(|pair| {
        (pair[0].partition, pair[0].first_sequence) < (pair[1].partition, pair[1].first_sequence)
    }) {
        return Err(SubscriptionContractError::NonCanonicalSegments);
    }
    for segment in &stream.segments {
        segment.validate(&stream.distribution_certificate)?;
    }
    let mut segment_index = 0;
    for range in &stream.ranges {
        let mut cursor = range.first_sequence;
        while let Some(segment) = stream.segments.get(segment_index) {
            if segment.partition < range.partition {
                return Err(SubscriptionContractError::NonCanonicalSegments);
            }
            if segment.partition != range.partition {
                break;
            }
            if segment.first_sequence != cursor {
                return Err(SubscriptionContractError::SequenceGap {
                    partition: range.partition.get(),
                    expected: cursor.get(),
                    actual: segment.first_sequence.get(),
                });
            }
            cursor = segment.exclusive_end_sequence;
            if cursor > range.through_sequence {
                return Err(SubscriptionContractError::SegmentBeyondFrontier);
            }
            segment_index += 1;
        }
        if cursor != range.through_sequence {
            return Err(SubscriptionContractError::SequenceGap {
                partition: range.partition.get(),
                expected: range.through_sequence.get(),
                actual: cursor.get(),
            });
        }
    }
    if segment_index != stream.segments.len() {
        return Err(SubscriptionContractError::NonCanonicalSegments);
    }
    Ok(())
}

/// Participant-local subscription output bound into its checkpoint manifest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSubscriptionManifest {
    /// Exact subscription protocol.
    pub protocol_version: SubscriptionProtocolVersion,
    /// Checkpoint epoch.
    pub epoch: u64,
    /// Checkpoint id; equal to `epoch`.
    pub checkpoint_id: u64,
    /// Participant publishing this manifest.
    pub participant_id: u64,
    /// Exact assignment and process-incarnation fence.
    pub assignment_certificate: CheckpointAssignmentFence,
    /// Canonical stream inventory.
    pub streams: Vec<NodeSubscriptionStreamManifest>,
    /// SHA-256 of every preceding field.
    pub manifest_digest: SubscriptionDigest,
}

impl NodeSubscriptionManifest {
    /// Compute and install the canonical participant-manifest digest.
    ///
    /// # Errors
    /// Returns the first malformed binding, range, segment, or size error.
    pub fn seal(&mut self, owned_vnodes: &[u16]) -> Result<(), SubscriptionContractError> {
        self.manifest_digest = self.computed_digest()?;
        self.validate(owned_vnodes)
    }

    /// Validate the exact participant binding and owned partition ranges.
    ///
    /// # Errors
    /// Returns the first malformed binding, range, segment, digest, or size error.
    pub fn validate(&self, owned_vnodes: &[u16]) -> Result<(), SubscriptionContractError> {
        self.protocol_version.validate()?;
        if self.epoch == 0 || self.checkpoint_id != self.epoch {
            return Err(SubscriptionContractError::CheckpointAttempt);
        }
        if self.participant_id == 0
            || !self.assignment_certificate.is_canonical()
            || self
                .assignment_certificate
                .participant_incarnation(self.participant_id)
                .is_none()
        {
            return Err(SubscriptionContractError::NodeParticipant);
        }
        let key_group_count = KeyGroupCount::try_from(self.assignment_certificate.vnode_count)
            .map_err(|_| SubscriptionContractError::AssignmentCertificate)?;
        if !self.streams.windows(2).all(|pair| {
            pair[0].distribution_certificate.stream_id < pair[1].distribution_certificate.stream_id
        }) {
            return Err(SubscriptionContractError::NonCanonicalNodeStreams);
        }
        for stream in &self.streams {
            stream.validate(key_group_count, owned_vnodes)?;
        }
        if self.manifest_digest != self.computed_digest()? {
            return Err(SubscriptionContractError::NodeManifestDigest);
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
        let body = NodeManifestBody {
            protocol_version: self.protocol_version,
            epoch: self.epoch,
            checkpoint_id: self.checkpoint_id,
            participant_id: self.participant_id,
            assignment_certificate: &self.assignment_certificate,
            streams: &self.streams,
        };
        let encoded = canonical_json_bytes(&body)
            .map_err(|error| SubscriptionContractError::Encode(error.to_string()))?;
        Ok(SubscriptionDigest::for_bytes(
            b"laminardb-node-subscription-manifest-v1",
            &encoded,
        ))
    }
}

#[derive(Serialize)]
struct NodeManifestBody<'a> {
    protocol_version: SubscriptionProtocolVersion,
    epoch: u64,
    checkpoint_id: u64,
    participant_id: u64,
    assignment_certificate: &'a CheckpointAssignmentFence,
    streams: &'a [NodeSubscriptionStreamManifest],
}

/// Whole-cluster stream cut reconstructed from exact participant manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedSubscriptionCheckpoint {
    /// Canonical checkpoint manifest exposed to committed readers.
    pub manifest: SubscriptionCheckpointManifest,
    /// Exact newly committed range per output partition.
    pub ranges: Vec<NodePartitionRange>,
}

impl MergedSubscriptionCheckpoint {
    /// Validate this cut's starting frontiers against an explicitly loaded predecessor.
    ///
    /// A missing predecessor is valid only for ranges beginning at sequence zero. A stream absent
    /// from the predecessor is a new generation and follows the same rule.
    ///
    /// # Errors
    /// Returns a distribution mismatch or the first partition sequence gap.
    pub fn validate_continuity(
        &self,
        predecessor: Option<&SubscriptionCheckpointManifest>,
    ) -> Result<(), SubscriptionContractError> {
        let predecessor = predecessor
            .filter(|manifest| manifest.stream_generation == self.manifest.stream_generation);
        if let Some(predecessor) = predecessor {
            self.manifest
                .distribution_certificate
                .require_match(&predecessor.distribution_certificate)?;
        }
        for range in &self.ranges {
            let expected = predecessor
                .and_then(|manifest| {
                    manifest
                        .frontiers
                        .binary_search_by_key(&range.partition, |frontier| frontier.partition)
                        .ok()
                        .map(|index| manifest.frontiers[index].through_sequence)
                })
                .unwrap_or(PartitionSequence::FIRST);
            if range.first_sequence != expected {
                return Err(SubscriptionContractError::SequenceGap {
                    partition: range.partition.get(),
                    expected: expected.get(),
                    actual: range.first_sequence.get(),
                });
            }
        }
        Ok(())
    }
}

struct MergedStream {
    certificate: OutputDistributionCertificate,
    ranges: Vec<NodePartitionRange>,
    segments: Vec<OutputSegmentRef>,
}

/// Merge one complete participant roster into canonical whole-cluster subscription cuts.
///
/// # Errors
/// Returns an error for a missing participant manifest, certificate disagreement, duplicate or
/// missing partition, wrong owner contribution, sequence gap, or malformed segment.
pub fn merge_node_subscription_manifests(
    epoch: u64,
    checkpoint_id: u64,
    assignment_certificate: &CheckpointAssignmentFence,
    manifests: &[&NodeSubscriptionManifest],
) -> Result<Vec<MergedSubscriptionCheckpoint>, SubscriptionContractError> {
    let expected_participants = assignment_certificate.participant_ids();
    let actual_participants = manifests
        .iter()
        .map(|manifest| manifest.participant_id)
        .collect::<Vec<_>>();
    if actual_participants != expected_participants {
        return Err(SubscriptionContractError::NodeParticipant);
    }
    let mut streams = BTreeMap::<String, MergedStream>::new();
    for node in manifests {
        if node.epoch != epoch
            || node.checkpoint_id != checkpoint_id
            || node.assignment_certificate != *assignment_certificate
        {
            return Err(SubscriptionContractError::AssignmentCertificate);
        }
        for stream in &node.streams {
            let stream_id = stream.distribution_certificate.stream_id.clone();
            let merged = streams.entry(stream_id).or_insert_with(|| MergedStream {
                certificate: stream.distribution_certificate.clone(),
                ranges: Vec::new(),
                segments: Vec::new(),
            });
            merged
                .certificate
                .require_match(&stream.distribution_certificate)?;
            merged.ranges.extend_from_slice(&stream.ranges);
            merged.segments.extend_from_slice(&stream.segments);
        }
    }

    let mut merged_checkpoints = Vec::with_capacity(streams.len());
    for (_, mut stream) in streams {
        stream.ranges.sort_unstable_by_key(|range| range.partition);
        if !stream
            .ranges
            .windows(2)
            .all(|pair| pair[0].partition < pair[1].partition)
        {
            return Err(SubscriptionContractError::NonCanonicalPartitionRanges);
        }
        stream
            .segments
            .sort_unstable_by_key(|segment| (segment.partition, segment.first_sequence));
        let frontiers = stream
            .ranges
            .iter()
            .map(|range| PartitionFrontier {
                partition: range.partition,
                through_sequence: range.through_sequence,
            })
            .collect();
        let mut manifest = SubscriptionCheckpointManifest {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            stream_generation: stream.certificate.stream_generation,
            schema_fingerprint: stream.certificate.schema_fingerprint,
            distribution_certificate: stream.certificate,
            epoch,
            checkpoint_id,
            assignment_certificate: assignment_certificate.clone(),
            frontiers,
            segments: stream.segments,
            manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
        };
        manifest.seal()?;
        merged_checkpoints.push(MergedSubscriptionCheckpoint {
            manifest,
            ranges: stream.ranges,
        });
    }
    Ok(merged_checkpoints)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{
        ChangelogMode, CheckpointParticipant, PipelineIdentity, StreamGeneration,
        OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
    };
    use crate::state::PARTITIONING_ABI_VERSION;

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
            distribution: super::super::OutputDistribution::VnodePartitioned {
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

    fn assignment() -> CheckpointAssignmentFence {
        CheckpointAssignmentFence::from_owner_map(
            5,
            &[1, 2, 1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: uuid::Uuid::parse_str("11111111-1111-4111-8111-111111111111")
                        .unwrap(),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::parse_str("22222222-2222-4222-8222-222222222222")
                        .unwrap(),
                },
            ],
        )
        .unwrap()
    }

    fn node(
        participant_id: u64,
        partitions: &[u16],
        first_sequence: u64,
        through_sequence: u64,
    ) -> NodeSubscriptionManifest {
        let assignment = assignment();
        let mut manifest = NodeSubscriptionManifest {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            epoch: 10,
            checkpoint_id: 10,
            participant_id,
            assignment_certificate: assignment,
            streams: vec![NodeSubscriptionStreamManifest {
                distribution_certificate: certificate(),
                ranges: partitions
                    .iter()
                    .map(|partition| NodePartitionRange {
                        partition: OutputPartitionId::new(*partition),
                        first_sequence: PartitionSequence::new(first_sequence),
                        through_sequence: PartitionSequence::new(through_sequence),
                    })
                    .collect(),
                segments: Vec::new(),
            }],
            manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
        };
        manifest.seal(partitions).unwrap();
        manifest
    }

    fn segment(partition: u16, first: u64, end: u64, suffix: &str) -> OutputSegmentRef {
        OutputSegmentRef {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            object_key: format!("subscription-output/test/{partition}/{suffix}.arrow"),
            stream_generation: certificate().stream_generation,
            partition: OutputPartitionId::new(partition),
            first_sequence: PartitionSequence::new(first),
            exclusive_end_sequence: PartitionSequence::new(end),
            frame_count: end - first,
            row_count: 1,
            encoded_length: 1,
            schema_fingerprint: certificate().schema_fingerprint,
            payload_digest: SubscriptionDigest::from_bytes([5; 32]),
        }
    }

    #[test]
    fn participant_ranges_merge_into_one_exact_frontier_vector() {
        let first = node(1, &[0, 2], 0, 0);
        let second = node(2, &[1, 3], 0, 0);
        let merged =
            merge_node_subscription_manifests(10, 10, &assignment(), &[&first, &second]).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0]
                .manifest
                .frontiers
                .iter()
                .map(|frontier| frontier.partition.get())
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn missing_participant_and_wrong_owner_fail_closed() {
        let first = node(1, &[0, 2], 0, 0);
        assert_eq!(
            merge_node_subscription_manifests(10, 10, &assignment(), &[&first]),
            Err(SubscriptionContractError::NodeParticipant)
        );

        let mut wrong_owner = node(1, &[0, 2], 0, 0);
        wrong_owner.streams[0].ranges[1].partition = OutputPartitionId::new(1);
        wrong_owner.manifest_digest = wrong_owner.computed_digest().unwrap();
        assert_eq!(
            wrong_owner.validate(&[0, 2]),
            Err(SubscriptionContractError::PartitionOwnerMismatch { partition: 1 })
        );
    }

    #[test]
    fn predecessor_frontier_gap_is_explicit() {
        let predecessor_first = node(1, &[0, 2], 0, 0);
        let predecessor_second = node(2, &[1, 3], 0, 0);
        let predecessor = merge_node_subscription_manifests(
            10,
            10,
            &assignment(),
            &[&predecessor_first, &predecessor_second],
        )
        .unwrap()
        .remove(0);

        let current_first = node(1, &[0, 2], 1, 1);
        let current_second = node(2, &[1, 3], 1, 1);
        let current = merge_node_subscription_manifests(
            10,
            10,
            &assignment(),
            &[&current_first, &current_second],
        )
        .unwrap()
        .remove(0);
        assert_eq!(
            current.validate_continuity(Some(&predecessor.manifest)),
            Err(SubscriptionContractError::SequenceGap {
                partition: 0,
                expected: 0,
                actual: 1,
            })
        );
    }

    #[test]
    fn duplicate_segment_and_partition_gap_are_rejected() {
        let mut duplicate = node(1, &[0, 2], 0, 0);
        duplicate.streams[0].ranges[0].through_sequence = PartitionSequence::new(1);
        let output = segment(0, 0, 1, "duplicate");
        duplicate.streams[0].segments = vec![output.clone(), output];
        duplicate.manifest_digest = duplicate.computed_digest().unwrap();
        assert_eq!(
            duplicate.validate(&[0, 2]),
            Err(SubscriptionContractError::NonCanonicalSegments)
        );

        let mut gap = node(1, &[0, 2], 0, 0);
        gap.streams[0].ranges[0].through_sequence = PartitionSequence::new(3);
        gap.streams[0].segments = vec![segment(0, 0, 1, "first"), segment(0, 2, 3, "last")];
        gap.manifest_digest = gap.computed_digest().unwrap();
        assert_eq!(
            gap.validate(&[0, 2]),
            Err(SubscriptionContractError::SequenceGap {
                partition: 0,
                expected: 1,
                actual: 2,
            })
        );
    }
}
