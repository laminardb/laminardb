use super::*;
use std::num::NonZeroU32;

use crate::checkpoint::checkpoint_manifest::{
    checkpoint_sha256, PreparedSinkDescriptor, ReferencedStateChunk, StateFrameKey,
    PREPARED_SINK_DESCRIPTOR_VERSION,
};
use crate::checkpoint::StateFrame;

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
