use super::*;
use object_store::{ObjectStoreExt, PutMode, PutOptions};
use std::num::NonZeroU32;

use crate::checkpoint::checkpoint_manifest::{
    checkpoint_artifact_intent_sha256, checkpoint_sha256, PreparedSinkArtifactIntent,
    PreparedSinkDescriptor, ReferencedStateChunk, StateFrameKey, PREPARED_SINK_DESCRIPTOR_VERSION,
};
use crate::checkpoint::{
    OutputPartitionId, OutputSegmentRef, PartitionSequence, StateFrame, StreamGeneration,
    SubscriptionDigest, SubscriptionProtocolVersion,
};
use crate::checkpoint_decision::CheckpointArtifactInventory;

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

fn artifact_inventory(manifest: &CheckpointManifest) -> CheckpointArtifactInventory {
    CheckpointArtifactInventory {
        deployment_id: manifest.deployment_id.clone(),
        pipeline_identity: manifest.pipeline_identity.clone(),
        attempt: crate::checkpoint::CheckpointAttempt::new(manifest.epoch, manifest.checkpoint_id),
        assignment_fence: manifest.assignment_fence.clone(),
        sink_artifact_intent_protocol: !manifest.sink_artifact_intents.is_empty(),
    }
}

fn manifest_with_intent(intent: &[u8], descriptor: &[u8]) -> (CheckpointManifest, Bytes) {
    let mut payload = b"bc".to_vec();
    payload.extend_from_slice(intent);
    payload.extend_from_slice(descriptor);
    let payload = Bytes::from(payload);
    let mut manifest = manifest_with_payload(&payload);
    let intent_length = u64::try_from(intent.len()).unwrap();
    manifest.sink_artifact_intents = vec![PreparedSinkArtifactIntent {
        sink_name: "sink".into(),
        format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
        payload: Some(ByteRange {
            offset: 2,
            length: intent_length,
        }),
        sha256: checkpoint_artifact_intent_sha256(Some(intent)),
    }];
    manifest.prepared_sinks[0].payload = Some(ByteRange {
        offset: 2 + intent_length,
        length: u64::try_from(descriptor.len()).unwrap(),
    });
    manifest.prepared_sinks[0].sha256 = checkpoint_descriptor_sha256(Some(descriptor));
    (manifest, payload)
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
async fn durable_sink_intent_promotes_to_the_exact_participant_manifest() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(backing);
    let secret = b"artifact-secret-root";
    let (manifest, payload) = manifest_with_intent(secret, b"descriptor");
    let inventory = artifact_inventory(&manifest);
    let identity =
        checkpoint_artifact_identity_sha256(&inventory, manifest.node_data.chunk).unwrap();
    let intent =
        CheckpointSinkArtifactIntent::try_new("sink".into(), Some(secret.to_vec())).unwrap();

    store
        .save_sink_artifact_intents(manifest.node_data.chunk, &identity, vec![intent.clone()])
        .await
        .unwrap();
    assert_eq!(
        store.load_manifest(manifest.checkpoint_id).await.unwrap(),
        None,
        "the intent is not a participant readiness marker"
    );
    store
        .save_sink_artifact_intents(manifest.node_data.chunk, &identity, vec![intent.clone()])
        .await
        .unwrap();
    let conflict =
        CheckpointSinkArtifactIntent::try_new("sink".into(), Some(b"different-root".to_vec()))
            .unwrap();
    assert!(store
        .save_sink_artifact_intents(manifest.node_data.chunk, &identity, vec![conflict])
        .await
        .unwrap_err()
        .to_string()
        .contains("different durable artifact state"));

    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();
    let restored = store
        .load_manifest(manifest.checkpoint_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored, manifest);
    assert_eq!(
        store
            .load_sink_artifact_intent(&restored, &restored.sink_artifact_intents[0])
            .await
            .unwrap(),
        Some(Bytes::copy_from_slice(secret))
    );
    let debug = format!("{intent:?}");
    assert!(debug.contains("payload_bytes"));
    assert!(!debug.contains("artifact-secret"));
}

#[tokio::test]
async fn local_checkpoint_store_conditionally_promotes_sink_intent() {
    let directory = tempfile::tempdir().unwrap();
    let backing: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap());
    let store = store(backing).with_exclusive_writer();
    let intent_payload = b"local-artifact-root";
    let (manifest, payload) = manifest_with_intent(intent_payload, b"descriptor");
    let identity = checkpoint_artifact_identity_sha256(
        &artifact_inventory(&manifest),
        manifest.node_data.chunk,
    )
    .unwrap();
    let intent =
        CheckpointSinkArtifactIntent::try_new("sink".into(), Some(intent_payload.to_vec()))
            .unwrap();

    store
        .save_sink_artifact_intents(manifest.node_data.chunk, &identity, vec![intent])
        .await
        .unwrap();
    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();

    assert_eq!(
        store.load_manifest(manifest.checkpoint_id).await.unwrap(),
        Some(manifest)
    );
}

#[tokio::test]
async fn abort_seal_retains_open_sink_intent_without_a_manifest() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(backing);
    let manifest = manifest_with_payload(b"bcdef");
    let inventory = artifact_inventory(&manifest);
    let identity =
        checkpoint_artifact_identity_sha256(&inventory, manifest.node_data.chunk).unwrap();
    let intent = CheckpointSinkArtifactIntent::try_new(
        "sink".into(),
        Some(b"artifact-secret-root".to_vec()),
    )
    .unwrap();
    store
        .save_sink_artifact_intents(manifest.node_data.chunk, &identity, vec![intent.clone()])
        .await
        .unwrap();

    let sealed = store
        .seal_aborted_manifest(
            manifest.node_data.chunk,
            &identity,
            inventory.sink_artifact_intent_protocol,
        )
        .await
        .unwrap();
    assert!(sealed.original_manifest.is_none());
    assert!(sealed.sink_artifact_intent_protocol);
    assert_eq!(sealed.open_sink_artifact_intents, vec![intent]);
    assert!(!sealed.sink_cleanup_complete);
    assert!(!format!("{sealed:?}").contains("artifact-secret"));
    let completed = store
        .complete_aborted_sink_cleanup(manifest.node_data.chunk, &identity)
        .await
        .unwrap();
    assert!(completed.sink_cleanup_complete);
    assert_eq!(completed.open_sink_artifact_intents.len(), 1);
    store
        .seal_aborted_node_data(manifest.node_data.chunk, &identity)
        .await
        .unwrap();
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
        sink_artifact_intent_protocol: false,
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
            .seal_aborted_manifest(chunk, &wrong_identity, false)
            .await,
        Err(CheckpointStoreError::Invalid(message)) if message.contains("different artifact identity")
    ));
    assert_eq!(
        store.load_manifest(7).await.unwrap(),
        Some(manifest.clone())
    );

    let expected = CheckpointManifestAbortSeal {
        original_manifest: Some((manifest.clone(), canonical.clone())),
        sink_artifact_intent_protocol: false,
        open_sink_artifact_intents: Vec::new(),
        sink_cleanup_complete: false,
    };
    assert_eq!(
        store
            .seal_aborted_manifest(chunk, &identity, false)
            .await
            .unwrap(),
        expected
    );
    let completed = CheckpointManifestAbortSeal {
        original_manifest: expected.original_manifest.clone(),
        sink_artifact_intent_protocol: false,
        open_sink_artifact_intents: Vec::new(),
        sink_cleanup_complete: true,
    };
    assert_eq!(
        store
            .complete_aborted_sink_cleanup(chunk, &identity)
            .await
            .unwrap(),
        completed
    );
    assert_eq!(
        store
            .complete_aborted_sink_cleanup(chunk, &identity)
            .await
            .unwrap(),
        completed
    );
    store
        .seal_aborted_node_data(chunk, &identity)
        .await
        .unwrap();
    assert_eq!(
        store
            .seal_aborted_manifest(chunk, &identity, false)
            .await
            .unwrap(),
        completed
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
    assert!(manifest_seal.sink_cleanup_complete);
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
    assert!(!node_seal.sink_cleanup_complete);

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
    missing_inventory.sink_artifact_intent_protocol = true;
    let missing_chunk = StateChunkId {
        participant_id: chunk.participant_id,
        checkpoint_id: 8,
    };
    let missing_identity =
        checkpoint_artifact_identity_sha256(&missing_inventory, missing_chunk).unwrap();
    let missing = CheckpointManifestAbortSeal {
        original_manifest: None,
        sink_artifact_intent_protocol: true,
        open_sink_artifact_intents: Vec::new(),
        sink_cleanup_complete: false,
    };
    assert_eq!(
        store
            .seal_aborted_manifest(missing_chunk, &missing_identity, true)
            .await
            .unwrap(),
        missing
    );
    assert!(
        store
            .complete_aborted_sink_cleanup(missing_chunk, &missing_identity)
            .await
            .unwrap()
            .sink_cleanup_complete
    );
    store
        .seal_aborted_node_data(missing_chunk, &missing_identity)
        .await
        .unwrap();
    assert_eq!(
        store
            .seal_aborted_manifest(missing_chunk, &missing_identity, true)
            .await
            .unwrap(),
        CheckpointManifestAbortSeal {
            original_manifest: None,
            sink_artifact_intent_protocol: true,
            open_sink_artifact_intents: Vec::new(),
            sink_cleanup_complete: true,
        }
    );
}

#[tokio::test]
async fn legacy_abort_seal_requires_cleanup_before_node_data_sealing() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = store(Arc::clone(&backing));
    let payload = Bytes::from_static(b"bcdef");
    let manifest = manifest_with_payload(&payload);
    let inventory = CheckpointArtifactInventory {
        deployment_id: manifest.deployment_id.clone(),
        pipeline_identity: manifest.pipeline_identity.clone(),
        attempt: crate::checkpoint::CheckpointAttempt::new(manifest.epoch, manifest.checkpoint_id),
        assignment_fence: manifest.assignment_fence.clone(),
        sink_artifact_intent_protocol: false,
    };
    let chunk = manifest.node_data.chunk;
    let identity = checkpoint_artifact_identity_sha256(&inventory, chunk).unwrap();
    let canonical = Bytes::from(checkpoint_manifest_bytes(&manifest).unwrap());
    store
        .save_checkpoint(&manifest, std::slice::from_ref(&payload))
        .await
        .unwrap();

    let legacy = CheckpointArtifactAbortSeal {
        version: CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION_V1,
        artifact_identity_sha256: identity.clone(),
        chunk,
        original_manifest: Some(manifest.clone()),
        sink_artifact_intent_protocol: false,
        open_sink_artifact_intents: Vec::new(),
        sink_cleanup_complete: false,
    };
    backing
        .put(
            &store.manifest_path(chunk),
            PutPayload::from_bytes(checkpoint_artifact_abort_seal_bytes(&legacy).unwrap()),
        )
        .await
        .unwrap();

    let error = store
        .seal_aborted_node_data(chunk, &identity)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("sink cleanup is incomplete"));
    assert_eq!(
        store
            .seal_aborted_manifest(chunk, &identity, false)
            .await
            .unwrap(),
        CheckpointManifestAbortSeal {
            original_manifest: Some((manifest, canonical)),
            sink_artifact_intent_protocol: false,
            open_sink_artifact_intents: Vec::new(),
            sink_cleanup_complete: false,
        }
    );
    assert!(
        store
            .complete_aborted_sink_cleanup(chunk, &identity)
            .await
            .unwrap()
            .sink_cleanup_complete
    );
    store
        .seal_aborted_node_data(chunk, &identity)
        .await
        .unwrap();
    let upgraded: CheckpointArtifactAbortSeal = serde_json::from_slice(
        &backing
            .get(&store.manifest_path(chunk))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(upgraded.version, CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION);
    assert!(upgraded.sink_cleanup_complete);
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
        Some(payload.clone())
    );

    let conflicting_payload = Bytes::from_static(b"conflicting immutable segment!");
    assert_eq!(conflicting_payload.len(), payload.len());
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
    let now_ms = chrono::Utc::now().timestamp_millis();
    let grace_report = store
        .delete_subscription_orphans(&reachable, 1, now_ms.saturating_sub(1_000))
        .await
        .unwrap();
    assert_eq!(grace_report.objects_deleted, 0);
    assert_eq!(grace_report.bytes_remaining, orphan.encoded_length);

    let grace_before_ms = now_ms.saturating_add(1_000);
    let report = store
        .delete_subscription_orphans(&reachable, 1, grace_before_ms)
        .await
        .unwrap();
    assert_eq!(report.objects_scanned, 3);
    assert_eq!(report.objects_deleted, 1);
    assert_eq!(report.bytes_deleted, orphan.encoded_length);
    assert_eq!(report.bytes_remaining, 0);
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
