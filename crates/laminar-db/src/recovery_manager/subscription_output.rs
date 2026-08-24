use futures::{StreamExt, TryStreamExt};
use laminar_core::checkpoint::{
    CheckpointAttempt, CheckpointManifest, CheckpointParticipant, CheckpointStore,
};

use crate::error::DbError;
use crate::subscription::cluster::{decode_bound_output_segment, OutputSegmentBinding};
use crate::subscription::ClusterSubscriptionError;

const PARALLEL_SUBSCRIPTION_SEGMENT_READS: usize = 4;

pub(super) async fn validate_committed_subscription_segments(
    store: &dyn CheckpointStore,
    manifests: &[CheckpointManifest],
) -> Result<(), DbError> {
    let reads = manifests.iter().flat_map(|manifest| {
        manifest.subscription_output.iter().flat_map(move |node| {
            node.streams.iter().flat_map(move |stream| {
                stream
                    .segments
                    .iter()
                    .map(move |segment| (manifest, node, stream, segment))
            })
        })
    });
    futures::stream::iter(reads)
        .map(|(manifest, node, stream, segment)| async move {
            let participant =
                checkpoint_participant(node, manifest.participant_id).ok_or_else(|| {
                    DbError::from(ClusterSubscriptionError::ManifestCorrupt {
                        reason: "segment participant is absent from its assignment certificate"
                            .into(),
                    })
                })?;
            let payload = store
                .load_subscription_segment(segment)
                .await
                .map_err(|error| match error {
                    laminar_core::checkpoint::checkpoint_store::CheckpointStoreError::Invalid(
                        _,
                    ) => DbError::from(ClusterSubscriptionError::SegmentCorrupt {
                        partition: segment.partition,
                        first: segment.first_sequence,
                    }),
                    _ => DbError::from(ClusterSubscriptionError::BackendUnavailable),
                })?
                .ok_or_else(|| {
                    DbError::from(ClusterSubscriptionError::SegmentMissing {
                        partition: segment.partition,
                        first: segment.first_sequence,
                    })
                })?;
            let binding = OutputSegmentBinding {
                deployment_id: &manifest.deployment_id,
                stream_id: &stream.distribution_certificate.stream_id,
                attempt: CheckpointAttempt::new(manifest.epoch, manifest.checkpoint_id),
                participant,
                assignment_version: node.assignment_certificate.assignment_version,
                assignment_digest: node.assignment_certificate.digest(),
            };
            decode_bound_output_segment(segment, &payload, &binding).map_err(|_| {
                DbError::from(ClusterSubscriptionError::SegmentCorrupt {
                    partition: segment.partition,
                    first: segment.first_sequence,
                })
            })?;
            Ok::<(), DbError>(())
        })
        .buffer_unordered(PARALLEL_SUBSCRIPTION_SEGMENT_READS)
        .try_for_each(|()| async { Ok(()) })
        .await
}

fn checkpoint_participant(
    node: &laminar_core::checkpoint::NodeSubscriptionManifest,
    participant_id: u64,
) -> Option<CheckpointParticipant> {
    node.assignment_certificate
        .participants
        .binary_search_by_key(&participant_id, |participant| participant.node_id)
        .ok()
        .map(|index| node.assignment_certificate.participants[index])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use laminar_core::checkpoint::{
        ChangelogMode, CheckpointAssignmentFence, NodePartitionRange, NodeSubscriptionManifest,
        NodeSubscriptionStreamManifest, ObjectStoreCheckpointStore, OutputDistribution,
        OutputDistributionCertificate, OutputPartitionId, PartitionSequence, PipelineIdentity,
        StreamGeneration, SubscriptionDigest, SubscriptionProtocolVersion,
        OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
    };
    use laminar_core::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;
    use crate::subscription::cluster::{
        encode_output_segment, OutputSegmentIdentity, OutputWriterAuthority,
    };

    const DEPLOYMENT: &str = "11111111-1111-4111-8111-111111111111";

    async fn fixture() -> (
        Arc<InMemory>,
        ObjectStoreCheckpointStore,
        CheckpointManifest,
    ) {
        let objects = Arc::new(InMemory::new());
        let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
        let store = ObjectStoreCheckpointStore::new(objects.clone(), "recovery-output")
            .with_key_group_count(key_groups);
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        };
        let assignment =
            CheckpointAssignmentFence::from_owner_map(1, &[1], vec![participant]).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();
        let schema_fingerprint =
            crate::pipeline_identity::subscription_schema_fingerprint(&schema).unwrap();
        let generation = StreamGeneration::from_digest(SubscriptionDigest::from_bytes([1; 32]));
        let certificate = OutputDistributionCertificate {
            version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            stream_id: "positions".into(),
            catalog_generation: 1,
            stream_generation: generation,
            final_operator_id: "stream:positions".into(),
            distribution: OutputDistribution::VnodePartitioned {
                key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
                partition_abi: PARTITIONING_ABI_VERSION,
                vnode_count: 1,
            },
            schema_fingerprint,
            changelog_mode: ChangelogMode::WeightedRetractInsert,
            history_retention_bytes: 0,
            query_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
            pipeline_identity: PipelineIdentity::empty(),
        };
        let encoded = encode_output_segment(
            &OutputSegmentIdentity {
                deployment_id: DEPLOYMENT,
                stream_id: "positions",
                stream_generation: generation,
                partition: OutputPartitionId::new(0),
                schema_fingerprint,
                attempt: CheckpointAttempt::canonical(1),
                authority: OutputWriterAuthority {
                    participant,
                    process_term: 1,
                    assignment_version: 1,
                    assignment_digest: assignment.digest(),
                },
            },
            &[batch],
            PartitionSequence::FIRST,
        )
        .unwrap();
        store
            .save_subscription_segment(&encoded.reference, encoded.bytes)
            .await
            .unwrap();
        let mut node = NodeSubscriptionManifest {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            epoch: 1,
            checkpoint_id: 1,
            participant_id: 1,
            assignment_certificate: assignment.clone(),
            streams: vec![NodeSubscriptionStreamManifest {
                distribution_certificate: certificate,
                ranges: vec![NodePartitionRange {
                    partition: OutputPartitionId::new(0),
                    first_sequence: PartitionSequence::FIRST,
                    through_sequence: PartitionSequence::new(1),
                }],
                segments: vec![encoded.reference],
            }],
            manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
        };
        node.seal(&[0]).unwrap();
        let mut manifest = CheckpointManifest::new_with_key_group_count(1, 1, key_groups);
        manifest.deployment_id = DEPLOYMENT.into();
        manifest.assignment_fence = Some(assignment);
        manifest.owned_vnodes = vec![0];
        manifest.subscription_output = Some(node);
        (objects, store, manifest)
    }

    #[tokio::test]
    async fn recovery_requires_every_referenced_segment() {
        let (_objects, store, manifest) = fixture().await;
        validate_committed_subscription_segments(&store, std::slice::from_ref(&manifest))
            .await
            .unwrap();
        let segment = &manifest.subscription_output.as_ref().unwrap().streams[0].segments[0];
        store
            .delete_subscription_segment(&segment.object_key)
            .await
            .unwrap();
        assert!(matches!(
            validate_committed_subscription_segments(&store, &[manifest]).await,
            Err(DbError::Subscription(
                ClusterSubscriptionError::SegmentMissing { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn recovery_rejects_corrupt_referenced_segment() {
        let (objects, store, manifest) = fixture().await;
        let segment = &manifest.subscription_output.as_ref().unwrap().streams[0].segments[0];
        let encoded_length = usize::try_from(segment.encoded_length).unwrap();
        let path = Path::from(format!("recovery-output/{}", segment.object_key));
        objects
            .put(
                &path,
                PutPayload::from_bytes(Bytes::from(vec![0; encoded_length])),
            )
            .await
            .unwrap();
        assert!(matches!(
            validate_committed_subscription_segments(&store, &[manifest]).await,
            Err(DbError::Subscription(
                ClusterSubscriptionError::SegmentCorrupt { .. }
            ))
        ));
    }
}
