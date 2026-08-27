use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use laminar_core::checkpoint::{
    checkpoint_manifest_bytes, ChangelogMode, CheckpointAssignmentFence, CheckpointManifest,
    CheckpointParticipant, CheckpointScope, CheckpointStore, CommittedCheckpointIndex,
    CommittedCheckpointRef, CommittedParticipantRef, NodePartitionRange, NodeSubscriptionManifest,
    NodeSubscriptionStreamManifest, ObjectStoreCheckpointStore, OutputDistribution,
    OutputDistributionCertificate, OutputFrameId, OutputPartitionId, OutputSegmentRef,
    PartitionFrontier, PartitionSequence, PipelineIdentity, StreamGeneration, SubscriptionDigest,
    SubscriptionProtocolVersion, COMMITTED_CHECKPOINT_INDEX_VERSION,
    OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
};
use laminar_core::checkpoint_decision::CheckpointDecisionStore;
use laminar_core::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};
use object_store::memory::InMemory;

use super::subscription_output::{
    cluster_subscription_retention_horizon, encode_node_subscription_output, retention_caps,
};
use super::{delete_retired_data, live_chunk_inventory};
use crate::error::DbError;
use crate::subscription::cluster::{
    ClusterSubscriptionOutputState, OutputWriterAuthority, PreparedNodeSubscriptionOutput,
};
use crate::subscription::{
    CertifiedSubscriptionFrontiers, ClusterSubscriptionError, PartitionedOutputBatch,
    PreparedSubscriptionOutput,
};

const DEPLOYMENT: &str = "11111111-1111-4111-8111-111111111111";

struct HistoryFixture {
    store: ObjectStoreCheckpointStore,
    decisions: CheckpointDecisionStore,
    latest: CommittedCheckpointIndex,
}

fn assignment() -> CheckpointAssignmentFence {
    CheckpointAssignmentFence::from_owner_map(
        1,
        &[1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        }],
    )
    .unwrap()
}

fn certificate(retention_bytes: u64) -> OutputDistributionCertificate {
    OutputDistributionCertificate {
        version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        stream_id: "positions".into(),
        catalog_generation: 1,
        stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes([1; 32])),
        final_operator_id: "stream:positions".into(),
        distribution: OutputDistribution::VnodePartitioned {
            key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
            partition_abi: PARTITIONING_ABI_VERSION,
            vnode_count: 1,
        },
        schema_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
        changelog_mode: ChangelogMode::WeightedRetractInsert,
        history_retention_bytes: retention_bytes,
        query_fingerprint: SubscriptionDigest::from_bytes([4; 32]),
        pipeline_identity: PipelineIdentity::empty(),
    }
}

fn prepared_output(
    assignment: &CheckpointAssignmentFence,
    writer_assignment_version: u64,
) -> Arc<PreparedNodeSubscriptionOutput> {
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
    let mut certificate = certificate(0);
    certificate.schema_fingerprint =
        crate::pipeline_identity::subscription_schema_fingerprint(&schema).unwrap();
    let certificate = Arc::new(certificate);
    let participant = assignment.participants[0];
    let mut state =
        ClusterSubscriptionOutputState::new(vec![Arc::clone(&certificate)], None).unwrap();
    state
        .stage_cycle(
            vec![PreparedSubscriptionOutput {
                certificate: Arc::clone(&certificate),
                frames: vec![PartitionedOutputBatch {
                    id: OutputFrameId {
                        stream_generation: certificate.stream_generation,
                        partition: OutputPartitionId::new(0),
                        sequence: PartitionSequence::FIRST,
                    },
                    batch,
                }],
            }],
            OutputWriterAuthority {
                participant,
                process_term: 1,
                assignment_version: writer_assignment_version,
                assignment_digest: assignment.digest(),
            },
        )
        .unwrap();
    state.commit_cycle();
    let attempt = laminar_core::checkpoint::CheckpointAttempt::canonical(1);
    state.reserve_checkpoint(attempt).unwrap();
    state
        .prepare_checkpoint(
            attempt,
            vec![CertifiedSubscriptionFrontiers {
                certificate,
                frontiers: vec![PartitionFrontier {
                    partition: OutputPartitionId::new(0),
                    through_sequence: PartitionSequence::new(1),
                }],
            }],
        )
        .unwrap()
        .unwrap()
}

fn segment(epoch: u64, generation: StreamGeneration, encoded_length: u64) -> OutputSegmentRef {
    OutputSegmentRef {
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        object_key: format!(
            "subscription-output/{DEPLOYMENT}/stream/{generation}/0/{:020}-{:020}-digest.arrow",
            epoch - 1,
            epoch
        ),
        stream_generation: generation,
        partition: OutputPartitionId::new(0),
        first_sequence: PartitionSequence::new(epoch - 1),
        exclusive_end_sequence: PartitionSequence::new(epoch),
        frame_count: 1,
        row_count: 1,
        encoded_length,
        schema_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
        payload_digest: SubscriptionDigest::from_bytes([5; 32]),
    }
}

fn manifest(
    deployment_id: &str,
    epoch: u64,
    retention_bytes: u64,
    encoded_length: u64,
) -> CheckpointManifest {
    let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
    let assignment = assignment();
    let certificate = certificate(retention_bytes);
    let output_segment = segment(epoch, certificate.stream_generation, encoded_length);
    let mut output = NodeSubscriptionManifest {
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        epoch,
        checkpoint_id: epoch,
        participant_id: 1,
        assignment_certificate: assignment.clone(),
        streams: vec![NodeSubscriptionStreamManifest {
            distribution_certificate: certificate,
            ranges: vec![NodePartitionRange {
                partition: OutputPartitionId::new(0),
                first_sequence: PartitionSequence::new(epoch - 1),
                through_sequence: PartitionSequence::new(epoch),
            }],
            segments: vec![output_segment],
        }],
        manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
    };
    output.seal(&[0]).unwrap();

    let mut manifest = CheckpointManifest::new_with_key_group_count(epoch, epoch, key_groups);
    manifest.deployment_id = deployment_id.into();
    manifest.assignment_fence = Some(assignment);
    manifest.reassignment_portable = true;
    manifest.owned_vnodes = vec![0];
    manifest.subscription_output = Some(output);
    manifest
}

fn index(
    manifest: &CheckpointManifest,
    predecessor: Option<CommittedCheckpointRef>,
) -> CommittedCheckpointIndex {
    let encoded = checkpoint_manifest_bytes(manifest).unwrap();
    let participant = CommittedParticipantRef::from_manifest(manifest, &encoded).unwrap();
    CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: manifest.deployment_id.clone(),
        pipeline_identity: manifest.pipeline_identity.clone(),
        epoch: manifest.epoch,
        checkpoint_id: manifest.checkpoint_id,
        predecessor,
        scope: CheckpointScope::Cluster,
        vnode_count: 1,
        assignment_fence: manifest.assignment_fence.clone(),
        reassignment_portable: true,
        participants: vec![participant],
        source_names: Vec::new(),
        source_offsets: BTreeMap::new(),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    }
}

async fn history(retention_bytes: u64) -> HistoryFixture {
    let objects = Arc::new(InMemory::new());
    let store = ObjectStoreCheckpointStore::new(objects.clone(), "history")
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap());
    let decisions = CheckpointDecisionStore::new(objects);
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut predecessor = None;
    let mut latest = None;
    for epoch in 1..=4 {
        let manifest = manifest(&deployment_id, epoch, retention_bytes, 10);
        store
            .save_checkpoint(&manifest, &[Bytes::new()])
            .await
            .unwrap();
        let checkpoint = index(&manifest, predecessor);
        predecessor = Some(
            decisions
                .create_committed_checkpoint(&checkpoint)
                .await
                .unwrap(),
        );
        latest = Some(checkpoint);
    }
    HistoryFixture {
        store,
        decisions,
        latest: latest.unwrap(),
    }
}

#[tokio::test]
async fn byte_cap_selects_the_oldest_replayable_checkpoint_boundary() {
    let fixture = history(25).await;
    let horizon = cluster_subscription_retention_horizon(
        &fixture.store,
        &fixture.decisions,
        &fixture.latest,
        0,
    )
    .await
    .unwrap();
    assert_eq!(horizon.epoch, 3);
}

#[tokio::test]
async fn prior_authoritative_floor_is_never_crossed() {
    let fixture = history(100).await;
    let horizon = cluster_subscription_retention_horizon(
        &fixture.store,
        &fixture.decisions,
        &fixture.latest,
        3,
    )
    .await
    .unwrap();
    assert_eq!(horizon.epoch, 3);
}

#[tokio::test]
async fn zero_history_cap_keeps_only_the_current_tail_boundary() {
    let fixture = history(0).await;
    let horizon = cluster_subscription_retention_horizon(
        &fixture.store,
        &fixture.decisions,
        &fixture.latest,
        0,
    )
    .await
    .unwrap();
    assert_eq!(horizon.epoch, 4);
}

#[test]
fn mixed_retention_roster_preserves_the_zero_cap() {
    let tail_only = certificate(0);
    let tail_generation = tail_only.stream_generation;
    let mut retained = certificate(100);
    retained.stream_id = "retained_positions".into();
    retained.stream_generation =
        StreamGeneration::from_digest(SubscriptionDigest::from_bytes([9; 32]));
    let certificates = BTreeMap::from([
        (tail_generation, tail_only),
        (retained.stream_generation, retained),
    ]);

    let caps = retention_caps(&certificates);

    assert_eq!(caps.get(&tail_generation), Some(&0));
    assert_eq!(caps.len(), 2);
}

#[test]
fn segment_preparation_rejects_stale_writer_authority() {
    let assignment = assignment();
    let participant = assignment.participants[0];
    let prepared = prepared_output(&assignment, assignment.assignment_version + 1);
    assert!(matches!(
        encode_node_subscription_output(
            prepared.as_ref(),
            DEPLOYMENT,
            participant.node_id,
            &assignment,
            &[0],
        ),
        Err(DbError::Subscription(
            ClusterSubscriptionError::StaleOutputWriter
        ))
    ));
}

#[tokio::test]
async fn immutable_upload_ack_loss_retries_and_uncommitted_object_is_collectable() {
    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(super::artifact_tests::CreateCommitThenIoStore {
        inner,
        lose_create_ack: std::sync::atomic::AtomicBool::new(true),
        create_suffix: ".arrow",
        block_get: std::sync::atomic::AtomicBool::new(false),
        deny_list: std::sync::atomic::AtomicBool::new(false),
    });
    let checkpoint_objects: Arc<dyn object_store::ObjectStore> = flaky.clone();
    let store = ObjectStoreCheckpointStore::new(checkpoint_objects, "subscription-upload")
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap());
    let mut coordinator =
        super::CheckpointCoordinator::new(super::CheckpointConfig::default(), Box::new(store))
            .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator.bind_deployment_id(DEPLOYMENT.into()).unwrap();
    coordinator.set_vnode_set(vec![0]);
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    coordinator.set_metrics(Arc::clone(&metrics));
    let assignment = assignment();
    let prepared = prepared_output(&assignment, assignment.assignment_version);
    let attempt = laminar_core::checkpoint::CheckpointAttempt::canonical(1);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    assert!(coordinator
        .prepare_subscription_output_until(
            attempt,
            Some(&assignment),
            Some(Arc::clone(&prepared)),
            deadline,
        )
        .await
        .is_err());
    assert_eq!(
        metrics
            .cluster_subscription
            .segment_write_failures_total
            .get(),
        1
    );
    let manifest = coordinator
        .prepare_subscription_output_until(attempt, Some(&assignment), Some(prepared), deadline)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metrics.cluster_subscription.segments_written_total.get(), 1);
    assert_eq!(
        metrics
            .cluster_subscription
            .checkpoint_prepare_seconds
            .get_sample_count(),
        2
    );
    let segment = manifest.streams[0].segments[0].clone();

    let fresh_objects: Arc<dyn object_store::ObjectStore> = flaky.clone();
    let fresh = ObjectStoreCheckpointStore::new(fresh_objects, "subscription-upload")
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap());
    flaky
        .deny_list
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(fresh
        .load_subscription_segment(&segment)
        .await
        .unwrap()
        .is_some());
    flaky
        .deny_list
        .store(false, std::sync::atomic::Ordering::Release);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let report = fresh
        .delete_subscription_orphans(
            &std::collections::BTreeSet::new(),
            attempt.checkpoint_id,
            i64::try_from(now_ms).unwrap().saturating_add(1_000),
        )
        .await
        .unwrap();
    assert_eq!(report.objects_deleted, 1);
    assert!(fresh
        .load_subscription_segment(&segment)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn retention_deletes_only_exact_unreachable_subscription_segments() {
    let objects = Arc::new(InMemory::new());
    let store = ObjectStoreCheckpointStore::new(objects, "retention-segments")
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap());
    let generation = certificate(100).stream_generation;
    let old_payload = Bytes::from_static(b"old-segment");
    let retained_payload = Bytes::from_static(b"retained-segment");
    let mut old_segment = segment(1, generation, old_payload.len() as u64);
    old_segment.payload_digest =
        SubscriptionDigest::for_bytes(b"laminardb-subscription-segment-v1", &old_payload);
    let mut retained_segment = segment(2, generation, retained_payload.len() as u64);
    retained_segment.payload_digest =
        SubscriptionDigest::for_bytes(b"laminardb-subscription-segment-v1", &retained_payload);
    store
        .save_subscription_segment(&old_segment, old_payload)
        .await
        .unwrap();
    store
        .save_subscription_segment(&retained_segment, retained_payload.clone())
        .await
        .unwrap();

    let mut retired = manifest(DEPLOYMENT, 1, 100, old_segment.encoded_length);
    let retired_output = retired.subscription_output.as_mut().unwrap();
    retired_output.streams[0].segments[0] = old_segment.clone();
    retired_output.seal(&[0]).unwrap();
    let mut retained = manifest(DEPLOYMENT, 2, 100, retained_segment.encoded_length);
    let retained_output = retained.subscription_output.as_mut().unwrap();
    retained_output.streams[0].segments[0] = retained_segment.clone();
    retained_output.seal(&[0]).unwrap();

    let live = live_chunk_inventory(std::slice::from_ref(&retained));
    delete_retired_data(&store, &[retired], &live)
        .await
        .unwrap();
    assert!(store
        .load_subscription_segment(&old_segment)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .load_subscription_segment(&retained_segment)
            .await
            .unwrap(),
        Some(retained_payload)
    );
}
