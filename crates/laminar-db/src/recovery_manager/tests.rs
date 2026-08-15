use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use laminar_core::checkpoint::{
    checkpoint_manifest_bytes, checkpoint_sha256, ChannelProgress, CheckpointAssignmentFence,
    CheckpointManifest, CheckpointParticipant, CheckpointScope, CheckpointStore,
    CommittedCheckpointIndex, CommittedParticipantRef, ConnectorCheckpoint, LeaderProof,
    LeaderProofOwner, ObjectStoreCheckpointStore, StateFrame, StateFrameKey,
    COMMITTED_CHECKPOINT_INDEX_VERSION,
};
use laminar_core::checkpoint_decision::{CheckpointOutcome, CheckpointVerdict};
use laminar_core::state::KeyGroupCount;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};

use super::{ClusterRecoveryTarget, RecoveryManager};

const DEPLOYMENT_ID: &str = "00000000-0000-0000-0000-000000000001";

struct Fixture {
    objects: Arc<InMemory>,
    store: ObjectStoreCheckpointStore,
    manifest: CheckpointManifest,
    committed: CommittedCheckpointIndex,
    outcome: CheckpointOutcome,
}

async fn committed_checkpoint() -> Fixture {
    let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
    let object_store = Arc::new(InMemory::new());
    let store = ObjectStoreCheckpointStore::new(object_store.clone(), "recovery")
        .with_key_group_count(key_groups);

    let mut manifest = CheckpointManifest::new_with_key_group_count(1, 1, key_groups);
    manifest.deployment_id = DEPLOYMENT_ID.into();
    manifest.source_names = vec!["source".into()];
    manifest.source_offsets.insert(
        "source".into(),
        ConnectorCheckpoint::with_offsets(HashMap::from([("partition-0".into(), "41".into())])),
    );
    manifest.channel_progress.push(ChannelProgress {
        participant_id: 1,
        source_name: "source".into(),
        input_channel: b"partition-0".to_vec(),
        watermark: Some(1_000),
        idle: false,
    });
    manifest.checkpoint_watermark = Some(1_000);

    let node_data = b"graph-state".to_vec();
    manifest.node_data.object_length = node_data.len() as u64;
    manifest.node_data.sha256 = checkpoint_sha256(&node_data);
    manifest.state_frames = vec![StateFrame {
        key: StateFrameKey::OperatorWhole {
            operator_id: "graph:test".into(),
        },
        chunk: manifest.node_data.chunk,
        range: laminar_core::checkpoint::ByteRange {
            offset: 0,
            length: 11,
        },
        sha256: checkpoint_sha256(b"graph-state"),
    }];

    store
        .save_checkpoint(&manifest, &[Bytes::from(node_data)])
        .await
        .unwrap();
    let persisted_manifest = checkpoint_manifest_bytes(&manifest).unwrap();
    let participant =
        CommittedParticipantRef::from_manifest(&manifest, &persisted_manifest).unwrap();
    let committed = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: DEPLOYMENT_ID.into(),
        pipeline_identity: manifest.pipeline_identity.clone(),
        epoch: 1,
        checkpoint_id: 1,
        predecessor: None,
        scope: CheckpointScope::Local,
        vnode_count: 1,
        assignment_fence: None,
        reassignment_portable: false,
        participants: vec![participant],
        source_names: manifest.source_names.clone(),
        source_offsets: BTreeMap::from([(
            "source".into(),
            manifest.source_offsets["source"].clone(),
        )]),
        channel_progress: manifest.channel_progress.clone(),
        source_watermarks: BTreeMap::from([("source".into(), 1_000)]),
        checkpoint_watermark: Some(1_000),
    };
    let (_, reference) = committed.encode_and_reference().unwrap();
    let outcome = CheckpointOutcome {
        version: 3,
        scope: CheckpointScope::Local,
        epoch: 1,
        checkpoint_id: 1,
        deployment_id: DEPLOYMENT_ID.into(),
        assignment_fence: None,
        leader_proof: None,
        committed_checkpoint: Some(reference),
        verdict: CheckpointVerdict::Commit,
    };
    Fixture {
        objects: object_store,
        store,
        manifest,
        committed,
        outcome,
    }
}

#[tokio::test]
async fn exact_commit_restores_complete_checked_frames_and_progress() {
    let fixture = committed_checkpoint().await;
    let manager = RecoveryManager::new(
        &fixture.store,
        &fixture.manifest.pipeline_identity,
        DEPLOYMENT_ID,
        CheckpointScope::Local,
    );

    let recovered = manager
        .recover_committed(&fixture.outcome, &fixture.committed)
        .await
        .unwrap();

    assert_eq!(recovered.epoch(), 1);
    assert_eq!(recovered.state_frames.len(), 1);
    assert!(recovered
        .state_frames
        .iter()
        .all(|frame| frame.participant_id == 1));
    assert_eq!(recovered.state_frames[0].payload, &b"graph-state"[..]);
    assert_eq!(
        recovered.source_offsets()["source"].offsets["partition-0"],
        "41"
    );
    assert_eq!(recovered.checkpoint_watermark(), Some(1_000));
    assert!(!recovered.reassigned);
    assert!(recovered.predecessor_owners.is_empty());
    assert!(recovered.target_vnodes.is_empty());
}

#[tokio::test]
async fn cluster_recovery_selects_exact_and_newer_target_frames() {
    let key_groups = KeyGroupCount::try_from(2_u16).unwrap();
    let objects = Arc::new(InMemory::new());
    let local_store = ObjectStoreCheckpointStore::new(objects.clone(), "cluster-recovery")
        .with_key_group_count(key_groups)
        .with_participant_id(1);
    let remote_store = ObjectStoreCheckpointStore::new(objects.clone(), "cluster-recovery")
        .with_key_group_count(key_groups)
        .with_participant_id(2);
    let fresh_store = ObjectStoreCheckpointStore::new(objects, "cluster-recovery")
        .with_key_group_count(key_groups)
        .with_participant_id(3);
    let local_boot = uuid::Uuid::from_u128(1);
    let remote_boot = uuid::Uuid::from_u128(2);
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: local_boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: remote_boot,
            },
        ],
    )
    .unwrap();

    let mut local = CheckpointManifest::new_with_key_group_count(1, 1, key_groups);
    local.deployment_id = DEPLOYMENT_ID.into();
    local.assignment_fence = Some(fence.clone());
    local.reassignment_portable = true;
    local.owned_vnodes = vec![0];
    local.node_data.object_length = 11;
    local.node_data.sha256 = checkpoint_sha256(b"whole1local");
    local.state_frames = vec![
        StateFrame {
            key: StateFrameKey::OperatorWhole {
                operator_id: "graph:join".into(),
            },
            chunk: local.node_data.chunk,
            range: laminar_core::checkpoint::ByteRange {
                offset: 0,
                length: 6,
            },
            sha256: checkpoint_sha256(b"whole1"),
        },
        StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "graph:join".into(),
                vnode: 0,
            },
            chunk: local.node_data.chunk,
            range: laminar_core::checkpoint::ByteRange {
                offset: 6,
                length: 5,
            },
            sha256: checkpoint_sha256(b"local"),
        },
    ];

    let mut remote = CheckpointManifest::new_with_key_group_count(1, 1, key_groups);
    remote.bind_participant(2);
    remote.deployment_id = DEPLOYMENT_ID.into();
    remote.assignment_fence = Some(fence.clone());
    remote.reassignment_portable = true;
    remote.owned_vnodes = vec![1];
    remote.node_data.object_length = 12;
    remote.node_data.sha256 = checkpoint_sha256(b"whole2remote");
    remote.state_frames = vec![
        StateFrame {
            key: StateFrameKey::OperatorWhole {
                operator_id: "graph:join".into(),
            },
            chunk: remote.node_data.chunk,
            range: laminar_core::checkpoint::ByteRange {
                offset: 0,
                length: 6,
            },
            sha256: checkpoint_sha256(b"whole2"),
        },
        StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "graph:join".into(),
                vnode: 1,
            },
            chunk: remote.node_data.chunk,
            range: laminar_core::checkpoint::ByteRange {
                offset: 6,
                length: 6,
            },
            sha256: checkpoint_sha256(b"remote"),
        },
    ];

    local_store
        .save_checkpoint(&local, &[Bytes::from_static(b"whole1local")])
        .await
        .unwrap();
    remote_store
        .save_checkpoint(&remote, &[Bytes::from_static(b"whole2remote")])
        .await
        .unwrap();

    let local_ref =
        CommittedParticipantRef::from_manifest(&local, &checkpoint_manifest_bytes(&local).unwrap())
            .unwrap();
    let remote_ref = CommittedParticipantRef::from_manifest(
        &remote,
        &checkpoint_manifest_bytes(&remote).unwrap(),
    )
    .unwrap();
    let committed = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: DEPLOYMENT_ID.into(),
        pipeline_identity: local.pipeline_identity.clone(),
        epoch: 1,
        checkpoint_id: 1,
        scope: CheckpointScope::Cluster,
        vnode_count: 2,
        assignment_fence: Some(fence.clone()),
        reassignment_portable: true,
        predecessor: None,
        participants: vec![local_ref, remote_ref],
        source_names: Vec::new(),
        source_offsets: BTreeMap::new(),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    };
    let (_, committed_ref) = committed.encode_and_reference().unwrap();
    let outcome = CheckpointOutcome {
        version: 3,
        scope: CheckpointScope::Cluster,
        epoch: 1,
        checkpoint_id: 1,
        deployment_id: DEPLOYMENT_ID.into(),
        assignment_fence: Some(fence.clone()),
        leader_proof: Some(LeaderProof {
            owner: LeaderProofOwner {
                node_id: 1,
                boot_id: local_boot,
                process_term: 1,
            },
            fencing_token: 1,
        }),
        committed_checkpoint: Some(committed_ref),
        verdict: CheckpointVerdict::Commit,
    };

    let fresh_manager = RecoveryManager::new(
        &fresh_store,
        &local.pipeline_identity,
        DEPLOYMENT_ID,
        CheckpointScope::Cluster,
    );
    let zero_owner = fresh_manager
        .recover_committed_for_target(
            &outcome,
            &committed,
            Some(ClusterRecoveryTarget {
                assignment: fence.clone(),
                owned_vnodes: Vec::new(),
                max_graph_payload_bytes: 64,
            }),
        )
        .await
        .unwrap();
    assert!(zero_owner.state_frames.is_empty());
    assert!(!zero_owner.reassigned);

    let successor = CheckpointAssignmentFence::from_owner_map(
        3,
        &[3, 3],
        vec![CheckpointParticipant {
            node_id: 3,
            boot_incarnation: uuid::Uuid::from_u128(3),
        }],
    )
    .unwrap();
    let mut nonportable = committed.clone();
    nonportable.reassignment_portable = false;
    let nonportable_error = match fresh_manager.select_state_frames(
        &nonportable,
        &[local.clone(), remote.clone()],
        Some(&ClusterRecoveryTarget {
            assignment: successor.clone(),
            owned_vnodes: vec![0, 1],
            max_graph_payload_bytes: 64,
        }),
    ) {
        Ok(_) => panic!("a nonportable checkpoint must not drive reassignment recovery"),
        Err(error) => error,
    };
    assert!(
        nonportable_error.to_string().contains("not a portable"),
        "{nonportable_error}"
    );
    let reassigned = fresh_manager
        .recover_committed_for_target(
            &outcome,
            &committed,
            Some(ClusterRecoveryTarget {
                assignment: successor.clone(),
                owned_vnodes: vec![0, 1],
                max_graph_payload_bytes: 64,
            }),
        )
        .await
        .unwrap();
    assert!(reassigned.reassigned);
    assert_eq!(reassigned.target_vnodes, vec![0, 1]);
    assert_eq!(reassigned.state_frames.len(), 4);
    assert_eq!(
        reassigned
            .state_frames
            .iter()
            .filter(|frame| matches!(&frame.key, StateFrameKey::OperatorWhole { .. }))
            .count(),
        2
    );

    let budget_error = fresh_manager
        .recover_committed_for_target(
            &outcome,
            &committed,
            Some(ClusterRecoveryTarget {
                assignment: successor.clone(),
                owned_vnodes: vec![0, 1],
                max_graph_payload_bytes: 22,
            }),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        budget_error,
        crate::error::DbError::ManagedStateBudgetExceeded {
            accounted_bytes: 23,
            limit_bytes: 22,
            ..
        }
    ));

    let mut stateless_local = local.clone();
    let mut stateless_remote = remote.clone();
    stateless_local.state_frames.clear();
    stateless_remote.state_frames.clear();
    let stateless_target = ClusterRecoveryTarget {
        assignment: successor,
        owned_vnodes: vec![0, 1],
        max_graph_payload_bytes: 64,
    };
    let stateless = fresh_manager
        .select_state_frames(
            &committed,
            &[stateless_local, stateless_remote],
            Some(&stateless_target),
        )
        .unwrap();
    assert!(stateless.reassigned);
    assert!(stateless.plans.is_empty());

    remote_store
        .delete_node_data(remote.node_data.chunk)
        .await
        .unwrap();
    let manager = RecoveryManager::new(
        &local_store,
        &local.pipeline_identity,
        DEPLOYMENT_ID,
        CheckpointScope::Cluster,
    );
    let recovered = manager
        .recover_committed_for_target(
            &outcome,
            &committed,
            Some(ClusterRecoveryTarget {
                assignment: fence,
                owned_vnodes: vec![0],
                max_graph_payload_bytes: 64,
            }),
        )
        .await
        .unwrap();

    assert_eq!(recovered.manifests.len(), 2);
    assert_eq!(recovered.state_frames.len(), 2);
    assert!(recovered
        .state_frames
        .iter()
        .all(|frame| frame.participant_id == 1));
    assert!(!recovered.reassigned);
    assert_eq!(
        recovered.predecessor_owners,
        vec![
            laminar_core::state::NodeId(1),
            laminar_core::state::NodeId(2)
        ]
    );
    assert_eq!(recovered.target_vnodes, vec![0]);

    let direct = manager
        .recover_committed(&outcome, &committed)
        .await
        .unwrap();
    assert_eq!(direct.state_frames.len(), 2);
}

#[tokio::test]
async fn state_range_corruption_fails_the_whole_restore() {
    let fixture = committed_checkpoint().await;
    let corrupt = b"graph-statf".to_vec();
    fixture
        .objects
        .put(
            &Path::from("recovery/nodes/1/checkpoints/00000000000000000001/node-data.bin"),
            PutPayload::from_bytes(Bytes::from(corrupt)),
        )
        .await
        .unwrap();
    let manager = RecoveryManager::new(
        &fixture.store,
        &fixture.manifest.pipeline_identity,
        DEPLOYMENT_ID,
        CheckpointScope::Local,
    );

    let error = manager
        .recover_committed(&fixture.outcome, &fixture.committed)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("checksum mismatch"));
}

#[tokio::test]
async fn outcome_cannot_authorize_a_different_valid_index() {
    let mut fixture = committed_checkpoint().await;
    fixture
        .committed
        .source_offsets
        .get_mut("source")
        .unwrap()
        .metadata
        .insert("format".into(), "v1".into());
    let manager = RecoveryManager::new(
        &fixture.store,
        &fixture.manifest.pipeline_identity,
        DEPLOYMENT_ID,
        CheckpointScope::Local,
    );

    let error = manager
        .recover_committed(&fixture.outcome, &fixture.committed)
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("does not bind the supplied committed checkpoint index"));
}
