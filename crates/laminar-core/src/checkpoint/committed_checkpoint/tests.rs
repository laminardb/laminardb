use super::*;
use crate::checkpoint::{checkpoint_manifest_bytes, checkpoint_sha256, CheckpointParticipant};

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn local_index() -> CommittedCheckpointIndex {
    CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: uuid::Uuid::new_v4().to_string(),
        pipeline_identity: PipelineIdentity::empty(),
        epoch: 7,
        checkpoint_id: 7,
        scope: CheckpointScope::Local,
        vnode_count: 4,
        assignment_fence: None,
        reassignment_portable: false,
        predecessor: None,
        participants: vec![CommittedParticipantRef {
            participant_id: LOCAL_NODE_ID.0,
            manifest_len: 100,
            manifest_sha256: digest(1),
            node_data_len: 0,
            node_data_sha256: digest(2),
        }],
        source_names: vec!["source".into()],
        source_offsets: BTreeMap::from([(
            "source".into(),
            ConnectorCheckpoint {
                input_channels: Some(vec![b"orders".to_vec()]),
                ..ConnectorCheckpoint::default()
            },
        )]),
        channel_progress: vec![ChannelProgress {
            participant_id: LOCAL_NODE_ID.0,
            source_name: "source".into(),
            input_channel: b"orders".to_vec(),
            watermark: Some(42),
            idle: false,
        }],
        source_watermarks: BTreeMap::from([("source".into(), 42)]),
        checkpoint_watermark: Some(42),
    }
}

fn cluster_cut() -> (CommittedCheckpointIndex, Vec<CheckpointManifest>) {
    let key_groups = KeyGroupCount::try_from(2_u16).unwrap();
    let participants = vec![
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        },
        CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(2),
        },
    ];
    let fence = CheckpointAssignmentFence::from_owner_map(1, &[1, 2], participants).unwrap();
    let deployment_id = uuid::Uuid::from_u128(3).to_string();
    let manifests = [1_u64, 2]
        .into_iter()
        .enumerate()
        .map(|(vnode, participant_id)| {
            let mut manifest = CheckpointManifest::new_with_key_group_count(7, 7, key_groups);
            manifest.bind_participant(participant_id);
            manifest.deployment_id = deployment_id.clone();
            manifest.assignment_fence = Some(fence.clone());
            manifest.reassignment_portable = true;
            manifest.owned_vnodes = vec![u16::try_from(vnode).unwrap()];
            manifest.source_names = vec!["source".into()];
            manifest.sink_names = vec!["sink".into()];
            manifest.node_data.sha256 = checkpoint_sha256(b"");
            manifest
        })
        .collect::<Vec<_>>();
    let mut index = CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id,
        pipeline_identity: PipelineIdentity::empty(),
        epoch: 7,
        checkpoint_id: 7,
        scope: CheckpointScope::Cluster,
        vnode_count: 2,
        assignment_fence: Some(fence),
        reassignment_portable: true,
        predecessor: None,
        participants: Vec::new(),
        source_names: vec!["source".into()],
        source_offsets: BTreeMap::from([("source".into(), ConnectorCheckpoint::default())]),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    };
    bind_manifests(&mut index, &manifests);
    (index, manifests)
}

fn bind_manifests(
    index: &mut CommittedCheckpointIndex,
    manifests: &[CheckpointManifest],
) -> Vec<Vec<u8>> {
    let encoded = manifests
        .iter()
        .map(|manifest| checkpoint_manifest_bytes(manifest).unwrap())
        .collect::<Vec<_>>();
    index.participants = manifests
        .iter()
        .zip(&encoded)
        .map(|(manifest, bytes)| CommittedParticipantRef::from_manifest(manifest, bytes).unwrap())
        .collect();
    encoded
}

fn validate_manifests(
    index: &CommittedCheckpointIndex,
    manifests: &[CheckpointManifest],
    encoded: &[Vec<u8>],
) -> Result<(), String> {
    let views = manifests
        .iter()
        .zip(encoded)
        .map(|(manifest, bytes)| (manifest, bytes.as_slice()))
        .collect::<Vec<_>>();
    index.validate_participant_manifests(&views)
}

#[test]
fn local_index_requires_the_single_local_participant() {
    let mut index = local_index();
    assert!(index.validate().is_ok());
    index.participants[0].participant_id = 9;
    assert!(index.validate().is_err());
}

#[test]
fn exact_scope_requires_reassignment_portability() {
    let mut local = local_index();
    local.reassignment_portable = true;
    assert!(local
        .validate()
        .unwrap_err()
        .contains("local committed checkpoint cannot claim"));

    let (mut cluster, _) = cluster_cut();
    cluster.reassignment_portable = false;
    assert!(cluster
        .validate()
        .unwrap_err()
        .contains("cluster committed checkpoint must be portable"));
}

#[test]
fn version_three_index_remains_canonical_and_version_two_fails_closed() {
    let mut index = local_index();
    index.version = 2;
    assert!(index
        .validate()
        .unwrap_err()
        .contains("unsupported committed checkpoint index version 2"));

    let mut v3_shape = serde_json::to_value(local_index()).unwrap();
    let object = v3_shape.as_object_mut().unwrap();
    object.insert("version".into(), serde_json::Value::from(3));
    object.remove("source_watermarks");
    let v3_bytes = canonical_json_bytes(&v3_shape).unwrap();
    let restored: CommittedCheckpointIndex = serde_json::from_slice(&v3_bytes).unwrap();
    restored.validate().unwrap();
    assert_eq!(
        restored.effective_source_watermarks().unwrap()["source"],
        42
    );
    assert_eq!(canonical_json_bytes(&restored).unwrap(), v3_bytes);

    let mut impossible_v3 = restored;
    impossible_v3.source_watermarks.insert("source".into(), 42);
    assert!(impossible_v3
        .validate()
        .unwrap_err()
        .contains("legacy committed checkpoint cannot carry"));
}

#[test]
fn reference_binds_exact_canonical_bytes() {
    let index = local_index();
    let (bytes, reference) = index.encode_and_reference().unwrap();
    assert_eq!(reference.len, bytes.len() as u64);
    assert_eq!(reference.sha256, sha256_hex(&bytes));

    let mut changed = index;
    changed.checkpoint_watermark = None;
    changed.channel_progress[0].watermark = None;
    changed.source_watermarks.clear();
    let (_, changed_reference) = changed.encode_and_reference().unwrap();
    assert_ne!(reference.sha256, changed_reference.sha256);
}

#[test]
fn empty_source_inventory_retains_the_exact_predecessor_decision() {
    let predecessor = local_index();
    let (_, predecessor_ref) = predecessor.encode_and_reference().unwrap();
    let mut successor = predecessor.clone();
    successor.epoch = 8;
    successor.checkpoint_id = 8;
    successor.predecessor = Some(predecessor_ref);
    successor.channel_progress.clear();
    successor.checkpoint_watermark = None;

    successor.validate().unwrap();
    successor.validate_predecessor_index(&predecessor).unwrap();
    assert_eq!(successor.source_watermarks["source"], 42);

    successor.source_watermarks.insert("source".into(), 43);
    assert!(successor
        .validate_predecessor_index(&predecessor)
        .unwrap_err()
        .contains("do not exactly continue"));
}

#[test]
fn legacy_successor_cannot_erase_a_version_four_source_cut() {
    let predecessor = local_index();
    let (_, predecessor_ref) = predecessor.encode_and_reference().unwrap();
    let mut successor = predecessor.clone();
    successor.version = LEGACY_COMMITTED_CHECKPOINT_INDEX_VERSION;
    successor.epoch = 8;
    successor.checkpoint_id = 8;
    successor.predecessor = Some(predecessor_ref);
    successor.source_watermarks.clear();

    successor.validate().unwrap();
    assert!(successor
        .validate_predecessor_index(&predecessor)
        .unwrap_err()
        .contains("version regresses"));
}

#[test]
fn predecessor_continuity_rejects_a_source_topology_change() {
    let predecessor = local_index();
    let (_, predecessor_ref) = predecessor.encode_and_reference().unwrap();
    let mut successor = predecessor.clone();
    successor.epoch = 8;
    successor.checkpoint_id = 8;
    successor.predecessor = Some(predecessor_ref);
    successor.source_names = vec!["other-source".into()];
    let offset = successor.source_offsets.remove("source").unwrap();
    successor
        .source_offsets
        .insert("other-source".into(), offset);
    successor.channel_progress[0].source_name = "other-source".into();
    let watermark = successor.source_watermarks.remove("source").unwrap();
    successor
        .source_watermarks
        .insert("other-source".into(), watermark);

    successor.validate().unwrap();
    assert!(successor
        .validate_predecessor_index(&predecessor)
        .unwrap_err()
        .contains("breaks recovery continuity"));
}

#[test]
fn initial_index_cannot_invent_a_channel_less_source_decision() {
    let mut index = local_index();
    index.channel_progress.clear();
    index.checkpoint_watermark = None;

    assert!(index
        .validate()
        .unwrap_err()
        .contains("initial committed source watermarks"));
}

#[test]
fn entirely_uninitialized_channels_have_no_watermark() {
    let mut index = local_index();
    index.channel_progress[0].watermark = None;
    assert!(index.validate().is_err());
    index.checkpoint_watermark = None;
    index.source_watermarks.clear();
    assert!(index.validate().is_ok());
}

#[test]
fn physical_input_channel_has_one_cluster_owner() {
    let (mut index, _) = cluster_cut();
    index.channel_progress = vec![
        ChannelProgress {
            participant_id: 1,
            source_name: "source".into(),
            input_channel: b"partition-0".to_vec(),
            watermark: Some(42),
            idle: false,
        },
        ChannelProgress {
            participant_id: 2,
            source_name: "source".into(),
            input_channel: b"partition-0".to_vec(),
            watermark: Some(42),
            idle: false,
        },
    ];
    index.checkpoint_watermark = Some(42);

    let error = index.validate().unwrap_err();
    assert!(error.contains("owned by multiple participants"));
}

#[test]
fn empty_participant_marker_can_share_a_source_with_remote_physical_channels() {
    let (mut index, mut manifests) = cluster_cut();
    index
        .source_offsets
        .get_mut("source")
        .unwrap()
        .input_channels = Some(vec![b"partition-0".to_vec()]);
    index.channel_progress = vec![
        ChannelProgress {
            participant_id: 1,
            source_name: "source".into(),
            input_channel: b"partition-0".to_vec(),
            watermark: Some(42),
            idle: false,
        },
        ChannelProgress {
            participant_id: 2,
            source_name: "source".into(),
            input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
            watermark: Some(41),
            idle: true,
        },
    ];
    index.source_watermarks.insert("source".into(), 42);
    index.checkpoint_watermark = Some(42);
    manifests[0].source_offsets.insert(
        "source".into(),
        ConnectorCheckpoint {
            input_channels: Some(vec![b"partition-0".to_vec()]),
            ..ConnectorCheckpoint::default()
        },
    );
    manifests[0].channel_progress = vec![index.channel_progress[0].clone()];
    manifests[0].checkpoint_watermark = Some(42);
    manifests[1].source_offsets.insert(
        "source".into(),
        ConnectorCheckpoint {
            input_channels: Some(Vec::new()),
            ..ConnectorCheckpoint::default()
        },
    );
    manifests[1].channel_progress = vec![index.channel_progress[1].clone()];
    let encoded = bind_manifests(&mut index, &manifests);
    index.validate().unwrap();
    validate_manifests(&index, &manifests, &encoded).unwrap();

    index.channel_progress = vec![
        ChannelProgress {
            participant_id: 1,
            source_name: "source".into(),
            input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
            watermark: Some(41),
            idle: true,
        },
        ChannelProgress {
            participant_id: 1,
            source_name: "source".into(),
            input_channel: b"partition-0".to_vec(),
            watermark: Some(42),
            idle: false,
        },
    ];
    let error = index.validate().unwrap_err();
    assert!(error.contains("participant 1"), "{error}");
    assert!(error.contains("mixes logical and physical"), "{error}");
}

#[test]
fn source_input_channels_match_merged_progress() {
    let mut index = local_index();
    index
        .source_offsets
        .get_mut("source")
        .unwrap()
        .input_channels = Some(vec![b"different".to_vec()]);

    let error = index.validate().unwrap_err();
    assert!(error.contains("channel progress roster"));

    index
        .source_offsets
        .get_mut("source")
        .unwrap()
        .input_channels = Some(vec![SINGLETON_WATERMARK_CHANNEL.to_vec()]);
    let error = index.validate().unwrap_err();
    assert!(error.contains("reserved logical watermark channel"));
}

#[test]
fn logical_singleton_is_participant_local_and_requires_a_known_source() {
    let (mut index, _) = cluster_cut();
    index
        .source_offsets
        .get_mut("source")
        .unwrap()
        .input_channels = Some(vec![b"partition-0".to_vec(), b"partition-1".to_vec()]);
    index.channel_progress = vec![
        ChannelProgress {
            participant_id: 1,
            source_name: "source".into(),
            input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
            watermark: Some(42),
            idle: false,
        },
        ChannelProgress {
            participant_id: 2,
            source_name: "source".into(),
            input_channel: SINGLETON_WATERMARK_CHANNEL.to_vec(),
            watermark: Some(41),
            idle: false,
        },
    ];
    index.source_watermarks.insert("source".into(), 41);
    index.checkpoint_watermark = Some(41);
    assert!(index.validate().is_ok());
    index.source_offsets.clear();
    assert!(index.validate().is_ok());

    index.channel_progress[0].source_name = "missing".into();
    let error = index.validate().unwrap_err();
    assert!(error.contains("absent from the committed source topology"));
}

#[test]
fn participant_vnode_owners_must_match_the_assignment_fence() {
    let (mut index, mut manifests) = cluster_cut();
    manifests[0].owned_vnodes = vec![1];
    manifests[1].owned_vnodes = vec![0];
    let encoded = bind_manifests(&mut index, &manifests);

    let error = validate_manifests(&index, &manifests, &encoded).unwrap_err();
    assert!(error.contains("vnode owners do not match the assignment fence"));
}

#[test]
fn every_cluster_manifest_must_bind_the_portability_proof() {
    let (mut index, mut manifests) = cluster_cut();
    manifests[1].reassignment_portable = false;
    let encoded = bind_manifests(&mut index, &manifests);

    let error = validate_manifests(&index, &manifests, &encoded).unwrap_err();
    assert!(error.contains("must be proven portable across vnode reassignment"));
}

#[test]
fn participant_source_and_sink_inventories_must_match() {
    let (mut index, mut manifests) = cluster_cut();
    manifests[1].source_names = vec!["other-source".into()];
    let encoded = bind_manifests(&mut index, &manifests);
    let error = validate_manifests(&index, &manifests, &encoded).unwrap_err();
    assert!(error.contains("source topology"));

    manifests[1].source_names = manifests[0].source_names.clone();
    manifests[1].sink_names = vec!["other-sink".into()];
    let encoded = bind_manifests(&mut index, &manifests);
    let error = validate_manifests(&index, &manifests, &encoded).unwrap_err();
    assert!(error.contains("sink topology"));
}
