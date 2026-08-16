use super::*;
use laminar_core::checkpoint::{checkpoint_sha256, ObjectStoreCheckpointStore};
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;

#[tokio::test]
async fn local_request_cannot_claim_cluster_reassignment_portability() {
    let store =
        ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "local-portability-validation");
    let coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    let request = CheckpointRequest {
        reassignment_portable: true,
        ..CheckpointRequest::default()
    };

    let error = coordinator.validate_request(&request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("local checkpoint cannot claim vnode reassignment portability"),
        "{error}"
    );
}

#[tokio::test]
async fn sparse_capture_carries_only_live_owned_frames_and_refcounts_chunks() {
    let key_groups = KeyGroupCount::try_from(3_u16).unwrap();
    let store = ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "sparse-capture")
        .with_key_group_count(key_groups);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator
        .bind_deployment_id(uuid::Uuid::from_u128(1).to_string())
        .unwrap();

    let mut prior = CheckpointManifest::new_with_key_group_count(1, 1, key_groups);
    prior.bind_participant(coordinator.store.participant_id());
    prior.deployment_id = uuid::Uuid::from_u128(1).to_string();
    prior.owned_vnodes = vec![0, 1, 2];
    let mut prior_bytes = Vec::new();
    for (operator_id, vnode) in [
        ("graph:dropped", 0),
        ("graph:dropped", 1),
        ("graph:dropped", 2),
        ("graph:global", 0),
        ("graph:keep", 0),
        ("graph:keep", 1),
        ("graph:keep", 2),
    ] {
        let payload = format!("{operator_id}-{vnode}").into_bytes();
        let offset = prior_bytes.len() as u64;
        prior_bytes.extend_from_slice(&payload);
        prior.state_frames.push(StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: operator_id.into(),
                vnode,
            },
            chunk: prior.node_data.chunk,
            range: ByteRange {
                offset,
                length: payload.len() as u64,
            },
            sha256: checkpoint_sha256(&payload),
        });
    }
    prior.node_data.object_length = prior_bytes.len() as u64;
    prior.node_data.sha256 = checkpoint_sha256(&prior_bytes);
    coordinator.last_committed_manifest = Some(Arc::new(prior));
    let request = || CheckpointRequest {
        state_frames: vec![CapturedStateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "graph:keep".into(),
                vnode: 2,
            },
            state: Some(Bytes::from_static(b"new-two")),
        }],
        managed_vnode_operators: vec![
            ManagedVnodeOperator {
                operator_id: "graph:keep".into(),
                placement: ManagedVnodePlacement::VnodeKeyed,
            },
            ManagedVnodeOperator {
                operator_id: "graph:global".into(),
                placement: ManagedVnodePlacement::GlobalSingleton,
            },
        ],
        ..CheckpointRequest::default()
    };

    coordinator.set_vnode_set(vec![0, 2]);
    let mut reassigned = request();
    reassigned
        .state_frames
        .sort_unstable_by(|left, right| left.key.cmp(&right.key));
    coordinator
        .complete_sparse_vnode_captures(&mut reassigned)
        .unwrap();
    assert!(reassigned
        .state_frames
        .iter()
        .all(|capture| match &capture.key {
            StateFrameKey::Vnode { operator_id, vnode } => {
                operator_id != "graph:dropped" && *vnode != 1
            }
            StateFrameKey::OperatorWhole { .. } => true,
        }));

    coordinator.set_vnode_set(vec![0, 1, 2]);
    let packed = coordinator
        .pack_checkpoint(
            CheckpointAttempt::canonical(2),
            request(),
            BTreeMap::new(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

    let keys = packed
        .manifest
        .state_frames
        .iter()
        .map(|frame| frame.key.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec![
            StateFrameKey::Vnode {
                operator_id: "graph:global".into(),
                vnode: 0,
            },
            StateFrameKey::Vnode {
                operator_id: "graph:keep".into(),
                vnode: 0,
            },
            StateFrameKey::Vnode {
                operator_id: "graph:keep".into(),
                vnode: 1,
            },
            StateFrameKey::Vnode {
                operator_id: "graph:keep".into(),
                vnode: 2,
            },
        ]
    );
    assert_eq!(packed.manifest.referenced_chunks.len(), 1);
    assert_eq!(packed.manifest.referenced_chunks[0].ref_count.get(), 3);
    assert_eq!(packed.node_data, vec![Bytes::from_static(b"new-two")]);
    assert!(!packed.manifest.reassignment_portable);
}

#[tokio::test]
async fn committed_manifest_rebases_at_referenced_chunk_threshold() {
    let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
    let mut coordinator = CheckpointCoordinator::new(
        CheckpointConfig::default(),
        Box::new(
            ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "chunk-threshold")
                .with_key_group_count(key_groups),
        ),
    )
    .unwrap();
    let mut manifest = CheckpointManifest::new_with_key_group_count(65, 65, key_groups);
    manifest.deployment_id = uuid::Uuid::from_u128(1).to_string();
    manifest.referenced_chunks = (1..=REFERENCED_CHUNK_REBASE_THRESHOLD)
        .map(|checkpoint_id| ReferencedStateChunk {
            chunk: StateChunkId {
                participant_id: 1,
                checkpoint_id: u64::try_from(checkpoint_id).unwrap(),
            },
            object_length: 1,
            sha256: checkpoint_sha256(b"x"),
            ref_count: NonZeroU32::new(1).unwrap(),
        })
        .collect();
    manifest.state_frames = (1..=REFERENCED_CHUNK_REBASE_THRESHOLD)
        .map(|checkpoint_id| StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: format!("graph:{checkpoint_id:020}"),
                vnode: 0,
            },
            chunk: StateChunkId {
                participant_id: 1,
                checkpoint_id: u64::try_from(checkpoint_id).unwrap(),
            },
            range: ByteRange {
                offset: 0,
                length: 1,
            },
            sha256: checkpoint_sha256(b"x"),
        })
        .collect();
    assert!(manifest.validate(key_groups).is_empty());
    coordinator.last_committed_manifest = Some(Arc::new(manifest));

    assert!(coordinator.committed_manifest_needs_vnode_rebase(CheckpointAttempt::canonical(65)));
    assert!(!coordinator.committed_manifest_needs_vnode_rebase(CheckpointAttempt::canonical(66)));
    Arc::make_mut(
        coordinator
            .last_committed_manifest
            .as_mut()
            .expect("installed manifest"),
    )
    .referenced_chunks
    .pop();
    assert!(!coordinator.committed_manifest_needs_vnode_rebase(CheckpointAttempt::canonical(65)));
}
