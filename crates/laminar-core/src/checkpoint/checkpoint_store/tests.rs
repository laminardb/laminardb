use super::*;
use std::num::NonZeroU32;

use crate::checkpoint::checkpoint_manifest::{
    checkpoint_sha256, PreparedSinkDescriptor, ReferencedStateChunk, StateFrameKey,
    PREPARED_SINK_DESCRIPTOR_VERSION,
};
use crate::checkpoint::{
    OutputPartitionId, OutputSegmentRef, PartitionSequence, StateFrame, StreamGeneration,
    SubscriptionDigest, SubscriptionProtocolVersion,
};

fn manifest_with_payload(payload: &[u8]) -> CheckpointManifest {
    let one = KeyGroupCount::try_from(1_u16).unwrap();
    let mut manifest = CheckpointManifest::new_with_key_group_count(7, 7, one);
    manifest.deployment_id = uuid::Uuid::from_u128(1).to_string();
    manifest.sink_names = vec!["sink".into()];
    manifest.node_data.object_length = u64::try_from(payload.len()).unwrap();
    manifest.node_data.sha256 = checkpoint_sha256(payload);
    manifest.state_frames.push(StateFrame {
        key: StateFrameKey::Vnode {
            operator_id: "join".into(),
            vnode: 0,
        },
        chunk: manifest.node_data.chunk,
        range: ByteRange {
            offset: 0,
            length: 2,
        },
        sha256: checkpoint_sha256(&payload[..2]),
    });
    manifest.prepared_sinks.push(PreparedSinkDescriptor {
        sink_name: "sink".into(),
        format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
        payload: Some(ByteRange {
            offset: 2,
            length: u64::try_from(payload.len() - 2).unwrap(),
        }),
        sha256: checkpoint_descriptor_sha256(Some(&payload[2..])),
    });
    manifest
}

fn store(backing: Arc<dyn ObjectStore>) -> ObjectStoreCheckpointStore {
    ObjectStoreCheckpointStore::new(backing, "test")
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap())
}

#[test]
fn checkpoint_node_data_limit_respects_the_allocation_ceiling() {
    validate_max_checkpoint_node_data_bytes(isize::MAX as u64).unwrap();
    let error = validate_max_checkpoint_node_data_bytes((isize::MAX as u64) + 1).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exceeds this process address space"),
        "{error}"
    );
}

#[tokio::test]
async fn one_node_object_supports_verified_range_reads() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(Arc::clone(&backing));
    let payload = Bytes::from_static(b"bcdef");
    let manifest = manifest_with_payload(&payload);

    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();

    assert_eq!(
        store.load_manifest(7).await.unwrap(),
        Some(manifest.clone())
    );
    assert_eq!(
        store
            .load_node_data_ranges(
                manifest.node_data.chunk,
                manifest.node_data.object_length,
                &[manifest.state_frames[0].range],
            )
            .await
            .unwrap(),
        Some(vec![Bytes::from_static(b"bc")])
    );
    assert!(matches!(
        store
            .load_node_data_ranges(
                manifest.node_data.chunk,
                manifest.node_data.object_length + 1,
                &[manifest.state_frames[0].range],
            )
            .await,
        Err(CheckpointStoreError::Invalid(message)) if message.contains("expected 6")
    ));
    assert_eq!(
        store
            .load_prepared_sink_descriptor(&manifest, &manifest.prepared_sinks[0])
            .await
            .unwrap(),
        Some(Bytes::from_static(b"def"))
    );

    let prefix = object_store::path::Path::from("test/nodes/1/checkpoints/00000000000000000007");
    use futures::TryStreamExt;
    let objects = backing
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(objects.len(), 2, "one manifest and one node data object");
}

#[tokio::test]
async fn explicit_empty_descriptor_is_range_read_without_collapsing_to_absence() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(backing);
    let payload = Bytes::from_static(b"bc");
    let mut manifest = manifest_with_payload(&payload);
    manifest.prepared_sinks[0].payload = Some(ByteRange {
        offset: 2,
        length: 0,
    });
    manifest.prepared_sinks[0].sha256 = checkpoint_descriptor_sha256(Some(b""));

    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();
    assert_eq!(
        store
            .load_prepared_sink_descriptor(&manifest, &manifest.prepared_sinks[0])
            .await
            .unwrap(),
        Some(Bytes::new())
    );
}

#[tokio::test]
async fn immutable_manifest_and_node_conflicts_fail_closed() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(backing);
    let payload = Bytes::from_static(b"bcdef");
    let manifest = manifest_with_payload(&payload);
    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();
    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();

    let mut conflicting = manifest.clone();
    conflicting.timestamp_ms += 1;
    assert!(matches!(
        store
            .save_checkpoint(&conflicting, std::slice::from_ref(&payload))
            .await,
        Err(CheckpointStoreError::Invalid(message)) if message.contains("different immutable content")
    ));
}

#[tokio::test]
async fn abort_seals_preserve_manifest_and_block_late_creates() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(Arc::clone(&backing));
    let payload = Bytes::from_static(b"bcdef");
    let manifest = manifest_with_payload(&payload);
    let inventory = CheckpointArtifactInventory {
        deployment_id: manifest.deployment_id.clone(),
        pipeline_identity: manifest.pipeline_identity.clone(),
        attempt: crate::checkpoint::CheckpointAttempt::new(manifest.epoch, manifest.checkpoint_id),
        assignment_fence: manifest.assignment_fence.clone(),
    };
    let chunk = manifest.node_data.chunk;
    let identity = checkpoint_artifact_identity_sha256(&inventory, chunk).unwrap();
    let canonical = Bytes::from(checkpoint_manifest_bytes(&manifest).unwrap());
    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();

    let wrong_identity = checkpoint_sha256(b"different artifact identity");
    assert!(matches!(
        store
            .seal_aborted_manifest(chunk, &wrong_identity)
            .await,
        Err(CheckpointStoreError::Invalid(message)) if message.contains("different artifact identity")
    ));
    assert_eq!(
        store.load_manifest(7).await.unwrap(),
        Some(manifest.clone())
    );

    let expected = Some((manifest.clone(), canonical.clone()));
    assert_eq!(
        store.seal_aborted_manifest(chunk, &identity).await.unwrap(),
        expected
    );
    store
        .seal_aborted_node_data(chunk, &identity)
        .await
        .unwrap();
    assert_eq!(
        store.seal_aborted_manifest(chunk, &identity).await.unwrap(),
        expected
    );
    store
        .seal_aborted_node_data(chunk, &identity)
        .await
        .unwrap();

    let manifest_seal: CheckpointArtifactAbortSeal = serde_json::from_slice(
        &backing
            .get(&store.manifest_path(chunk))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(manifest_seal.artifact_identity_sha256, identity);
    assert_eq!(manifest_seal.chunk, chunk);
    assert_eq!(manifest_seal.original_manifest, Some(manifest));
    let node_seal: CheckpointArtifactAbortSeal = serde_json::from_slice(
        &backing
            .get(&store.node_data_path(chunk))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(node_seal.original_manifest, None);

    for path in [store.manifest_path(chunk), store.node_data_path(chunk)] {
        assert!(matches!(
            backing
                .put_opts(
                    &path,
                    PutPayload::from_static(b"late artifact create"),
                    PutOptions {
                        mode: PutMode::Create,
                        ..PutOptions::default()
                    },
                )
                .await,
            Err(object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. })
        ));
    }

    let mut missing_inventory = inventory;
    missing_inventory.attempt = crate::checkpoint::CheckpointAttempt::new(8, 8);
    let missing_chunk = StateChunkId {
        participant_id: chunk.participant_id,
        checkpoint_id: 8,
    };
    let missing_identity =
        checkpoint_artifact_identity_sha256(&missing_inventory, missing_chunk).unwrap();
    assert_eq!(
        store
            .seal_aborted_manifest(missing_chunk, &missing_identity)
            .await
            .unwrap(),
        None
    );
    store
        .seal_aborted_node_data(missing_chunk, &missing_identity)
        .await
        .unwrap();
    assert_eq!(
        store
            .seal_aborted_manifest(missing_chunk, &missing_identity)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn manifest_conflict_requires_exact_canonical_bytes() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let payload = Bytes::from_static(b"bcdef");
    let manifest = manifest_with_payload(&payload);
    let path = object_store::path::Path::from(
        "test/nodes/1/checkpoints/00000000000000000007/manifest.json",
    );
    backing
        .put(
            &path,
            PutPayload::from(serde_json::to_vec_pretty(&manifest).unwrap()),
        )
        .await
        .unwrap();

    assert!(matches!(
        store(backing)
            .save_checkpoint(&manifest, std::slice::from_ref(&payload))
            .await,
        Err(CheckpointStoreError::Invalid(message)) if message.contains("different immutable content")
    ));
}

#[tokio::test]
async fn prior_chunk_ranges_use_explicit_identity_without_listing() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(backing);
    let prior_payload = Bytes::from_static(b"ld");
    let mut prior = manifest_with_payload(&prior_payload);
    prior.prepared_sinks.clear();
    prior.sink_names.clear();
    prior.node_data.object_length = prior_payload.len() as u64;
    prior.node_data.sha256 = checkpoint_sha256(&prior_payload);
    store
        .save_checkpoint(&prior, std::slice::from_ref(&prior_payload))
        .await
        .unwrap();

    let current_payload = Bytes::from_static(b"ew");
    let mut current = manifest_with_payload(&current_payload);
    current.checkpoint_id = 8;
    current.epoch = 8;
    current.node_data.chunk.checkpoint_id = 8;
    current.state_frames[0].chunk = prior.node_data.chunk;
    current.state_frames[0].sha256 = checkpoint_sha256(b"ld");
    current.node_data.object_length = 0;
    current.node_data.sha256 = checkpoint_sha256(b"");
    current.prepared_sinks.clear();
    current.sink_names.clear();
    current.referenced_chunks.push(ReferencedStateChunk {
        chunk: prior.node_data.chunk,
        object_length: prior_payload.len() as u64,
        sha256: checkpoint_sha256(&prior_payload),
        ref_count: NonZeroU32::new(1).unwrap(),
    });
    store.save_checkpoint(&current, &[]).await.unwrap();

    assert_eq!(
        store
            .load_node_data_ranges(
                current.state_frames[0].chunk,
                prior.node_data.object_length,
                &[current.state_frames[0].range],
            )
            .await
            .unwrap(),
        Some(vec![Bytes::from_static(b"ld")])
    );
}

#[tokio::test]
async fn in_memory_store_passes_conditional_put_probe() {
    let backing = object_store::memory::InMemory::new();
    probe_object_store_conditional_create(&backing, "test", Duration::from_secs(1))
        .await
        .unwrap();
    probe_object_store_conditional_update(&backing, "test", Duration::from_secs(1))
        .await
        .unwrap();
}

fn subscription_segment(payload: &[u8], object_key: &str) -> OutputSegmentRef {
    OutputSegmentRef {
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        object_key: object_key.into(),
        stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes([1; 32])),
        partition: OutputPartitionId::new(0),
        first_sequence: PartitionSequence::FIRST,
        exclusive_end_sequence: PartitionSequence::new(1),
        frame_count: 1,
        row_count: 1,
        encoded_length: payload.len() as u64,
        schema_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
        payload_digest: SubscriptionDigest::for_bytes(
            b"laminardb-subscription-segment-v1",
            payload,
        ),
    }
}

#[tokio::test]
async fn subscription_segment_create_is_idempotent_but_conflicts_fail_closed() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(backing);
    let object_key = "subscription-output/deployment/stream/generation/0/0-1-digest.arrow";
    let payload = Bytes::from_static(b"immutable subscription segment");
    let segment = subscription_segment(&payload, object_key);

    store
        .save_subscription_segment(&segment, payload.clone())
        .await
        .unwrap();
    store
        .save_subscription_segment(&segment, payload.clone())
        .await
        .unwrap();
    assert_eq!(
        store.load_subscription_segment(&segment).await.unwrap(),
        Some(payload)
    );

    let conflicting_payload = Bytes::from_static(b"conflicting immutable segment");
    let conflicting = subscription_segment(&conflicting_payload, object_key);
    assert!(matches!(
        store
            .save_subscription_segment(&conflicting, conflicting_payload)
            .await,
        Err(CheckpointStoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn subscription_segment_delete_uses_an_explicit_validated_key() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(backing);
    let payload = Bytes::from_static(b"retired subscription segment");
    let segment = subscription_segment(
        &payload,
        "subscription-output/deployment/stream/generation/0/1-2-digest.arrow",
    );
    store
        .save_subscription_segment(&segment, payload)
        .await
        .unwrap();
    store
        .delete_subscription_segment(&segment.object_key)
        .await
        .unwrap();
    assert_eq!(
        store.load_subscription_segment(&segment).await.unwrap(),
        None
    );
    assert!(store
        .delete_subscription_segment("../escape")
        .await
        .is_err());
}

fn canonical_subscription_key(checkpoint_id: u64, segment: &OutputSegmentRef) -> String {
    let deployment = uuid::Uuid::from_u128(1);
    let stream_key = SubscriptionDigest::from_bytes([3; 32]);
    format!(
        "subscription-output/{deployment}/{stream_key}/{}/0/checkpoint={checkpoint_id:020}/{:020}-{:020}-{}.arrow",
        segment.stream_generation,
        segment.first_sequence.get(),
        segment.exclusive_end_sequence.get(),
        segment.payload_digest,
    )
}

#[tokio::test]
async fn orphan_cleanup_uses_committed_reachability_and_attempt_bounds() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(backing);
    let orphan_payload = Bytes::from_static(b"orphan");
    let retained_payload = Bytes::from_static(b"retained");
    let newer_payload = Bytes::from_static(b"newer");
    let mut orphan = subscription_segment(&orphan_payload, "subscription-output/placeholder");
    orphan.object_key = canonical_subscription_key(1, &orphan);
    let mut retained = subscription_segment(&retained_payload, "subscription-output/placeholder");
    retained.object_key = canonical_subscription_key(1, &retained);
    let mut newer = subscription_segment(&newer_payload, "subscription-output/placeholder");
    newer.object_key = canonical_subscription_key(2, &newer);
    for (segment, payload) in [
        (&orphan, orphan_payload.clone()),
        (&retained, retained_payload.clone()),
        (&newer, newer_payload.clone()),
    ] {
        store
            .save_subscription_segment(segment, payload)
            .await
            .unwrap();
    }

    let reachable = std::collections::BTreeSet::from([retained.object_key.clone()]);
    let grace_before_ms = chrono::Utc::now().timestamp_millis().saturating_add(1_000);
    let report = store
        .delete_subscription_orphans(&reachable, 1, grace_before_ms)
        .await
        .unwrap();
    assert_eq!(report.objects_scanned, 3);
    assert_eq!(report.objects_deleted, 1);
    assert_eq!(report.bytes_deleted, orphan.encoded_length);
    assert!(store
        .load_subscription_segment(&orphan)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store.load_subscription_segment(&retained).await.unwrap(),
        Some(retained_payload)
    );
    assert_eq!(
        store.load_subscription_segment(&newer).await.unwrap(),
        Some(newer_payload)
    );
}
use std::time::Duration;
