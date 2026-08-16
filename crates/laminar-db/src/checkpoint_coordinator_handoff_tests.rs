use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use laminar_core::checkpoint::{
    checkpoint_sha256, CheckpointAssignmentFence, CheckpointManifest, CheckpointParticipant,
    CheckpointScope, CheckpointStore, CommittedCheckpointIndex, CommittedCheckpointRef,
    CommittedParticipantRef, ObjectStoreCheckpointStore, StateFrame, StateFrameKey,
    COMMITTED_CHECKPOINT_INDEX_VERSION,
};
use laminar_core::checkpoint_decision::CheckpointDecisionStore;
use laminar_core::state::{KeyGroupCount, NodeId};
use object_store::memory::InMemory;

use super::*;

struct HandoffFixture {
    coordinator: CheckpointCoordinator,
    reference: CommittedCheckpointRef,
    fence: CheckpointAssignmentFence,
    owners: Vec<NodeId>,
    manifest_bytes: usize,
}

fn vnode_payload(vnode: u16) -> Vec<u8> {
    let mut payload = format!("vnode-{vnode}").into_bytes();
    payload.resize(64 * 1024, u8::try_from(vnode).unwrap());
    payload
}

fn donor_manifest(
    participant_id: u64,
    owned_vnodes: Vec<u16>,
    fence: &CheckpointAssignmentFence,
    deployment_id: &str,
    key_group_count: KeyGroupCount,
) -> (CheckpointManifest, Bytes) {
    let mut manifest = CheckpointManifest::new_with_key_group_count(1, 1, key_group_count);
    manifest.bind_participant(participant_id);
    manifest.deployment_id = deployment_id.into();
    manifest.assignment_fence = Some(fence.clone());
    manifest.reassignment_portable = true;
    manifest.owned_vnodes.clone_from(&owned_vnodes);

    let mut entries = vec![
        (
            StateFrameKey::OperatorWhole {
                operator_id: "graph:join".into(),
            },
            format!("whole-{participant_id}").into_bytes(),
        ),
        (
            StateFrameKey::OperatorWhole {
                operator_id: "mv:view".into(),
            },
            format!("mv-whole-{participant_id}").into_bytes(),
        ),
    ];
    for vnode in owned_vnodes {
        entries.push((
            StateFrameKey::Vnode {
                operator_id: "graph:join".into(),
                vnode,
            },
            vnode_payload(vnode),
        ));
        entries.push((
            StateFrameKey::Vnode {
                operator_id: "mv:view".into(),
                vnode,
            },
            format!("mv-vnode-{vnode}").into_bytes(),
        ));
    }
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut payload = Vec::new();
    for (key, bytes) in entries {
        let offset = payload.len() as u64;
        payload.extend_from_slice(&bytes);
        manifest.state_frames.push(StateFrame {
            key,
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset,
                length: bytes.len() as u64,
            },
            sha256: checkpoint_sha256(&bytes),
        });
    }
    manifest.node_data.object_length = payload.len() as u64;
    manifest.node_data.sha256 = checkpoint_sha256(&payload);
    (manifest, Bytes::from(payload))
}

async fn handoff_fixture() -> HandoffFixture {
    let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&objects)));
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let key_group_count = KeyGroupCount::try_from(4_u16).unwrap();
    let owners = vec![NodeId(1), NodeId(1), NodeId(2), NodeId(2)];
    let fence = CheckpointAssignmentFence::from_owner_map(
        7,
        &[1, 1, 2, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();

    let mut participants = Vec::new();
    let mut pipeline_identity = None;
    for (participant_id, owned_vnodes) in [(1, vec![0, 1]), (2, vec![2, 3])] {
        let store = ObjectStoreCheckpointStore::new(Arc::clone(&objects), "handoff-frames")
            .with_key_group_count(key_group_count)
            .with_participant_id(participant_id);
        let (manifest, payload) = donor_manifest(
            participant_id,
            owned_vnodes,
            &fence,
            &deployment_id,
            key_group_count,
        );
        pipeline_identity.get_or_insert_with(|| manifest.pipeline_identity.clone());
        let encoded = store
            .save_checkpoint(&manifest, std::slice::from_ref(&payload))
            .await
            .unwrap();
        participants.push(CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap());
    }
    let pipeline_identity = pipeline_identity.unwrap();
    let index = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id,
        pipeline_identity: pipeline_identity.clone(),
        epoch: 1,
        checkpoint_id: 1,
        scope: CheckpointScope::Cluster,
        vnode_count: key_group_count.get(),
        assignment_fence: Some(fence.clone()),
        reassignment_portable: true,
        predecessor: None,
        participants,
        source_names: Vec::new(),
        source_offsets: BTreeMap::new(),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    };
    let reference = decisions.create_committed_checkpoint(&index).await.unwrap();
    let manifest_bytes = index
        .participants
        .iter()
        .map(|participant| usize::try_from(participant.manifest_len).unwrap())
        .sum();

    let target_store = ObjectStoreCheckpointStore::new(objects, "handoff-frames")
        .with_key_group_count(key_group_count)
        .with_participant_id(3);
    let mut coordinator =
        CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(target_store)).unwrap();
    coordinator
        .bind_durable_decision_store(decisions)
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(pipeline_identity)
        .unwrap();
    HandoffFixture {
        coordinator,
        reference,
        fence,
        owners,
        manifest_bytes,
    }
}

#[tokio::test]
async fn handoff_loads_only_acquired_graph_frames_from_each_donor() {
    let fixture = handoff_fixture().await;
    let frames = fixture
        .coordinator
        .load_handoff_state_frames(
            &fixture.reference,
            &fixture.fence,
            &fixture.owners,
            &[1, 2],
            true,
            1024 * 1024,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();

    let actual = frames
        .into_iter()
        .map(|frame| (frame.participant_id, frame.key, frame.payload))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (
                1,
                StateFrameKey::OperatorWhole {
                    operator_id: "graph:join".into(),
                },
                Bytes::from_static(b"whole-1"),
            ),
            (
                1,
                StateFrameKey::Vnode {
                    operator_id: "graph:join".into(),
                    vnode: 1,
                },
                Bytes::from(vnode_payload(1)),
            ),
            (
                2,
                StateFrameKey::OperatorWhole {
                    operator_id: "graph:join".into(),
                },
                Bytes::from_static(b"whole-2"),
            ),
            (
                2,
                StateFrameKey::Vnode {
                    operator_id: "graph:join".into(),
                    vnode: 2,
                },
                Bytes::from(vnode_payload(2)),
            ),
        ]
    );

    let vnode_only = fixture
        .coordinator
        .load_handoff_state_frames(
            &fixture.reference,
            &fixture.fence,
            &fixture.owners,
            &[1, 2],
            false,
            1024 * 1024,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(vnode_only.len(), 2);
    assert!(vnode_only
        .iter()
        .all(|frame| matches!(&frame.key, StateFrameKey::Vnode { .. })));
}

#[tokio::test]
async fn handoff_rejects_selected_payload_above_the_pipeline_budget() {
    let fixture = handoff_fixture().await;
    let limit = fixture.manifest_bytes + 1;
    let error = fixture
        .coordinator
        .load_handoff_state_frames(
            &fixture.reference,
            &fixture.fence,
            &fixture.owners,
            &[1, 2],
            true,
            limit,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        DbError::ManagedStateBudgetExceeded {
            context,
            limit_bytes,
            ..
        } if context.contains("LDB-6050") && limit_bytes == limit
    ));
}
