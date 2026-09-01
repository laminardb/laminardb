use super::*;

fn valid_manifest(id: u64) -> CheckpointManifest {
    let mut manifest = CheckpointManifest::new_with_key_group_count(
        id,
        id,
        KeyGroupCount::try_from(1_u16).unwrap(),
    );
    manifest.deployment_id = uuid::Uuid::from_u128(1).to_string();
    manifest.source_names = vec!["source".into()];
    manifest.sink_names = vec!["sink".into()];
    manifest.node_data.sha256 = checkpoint_sha256(b"");
    manifest
}

fn channel(id: &str, watermark: Option<i64>, idle: bool) -> ChannelProgress {
    ChannelProgress {
        participant_id: LOCAL_NODE_ID.0,
        source_name: "source".into(),
        input_channel: id.as_bytes().to_vec(),
        watermark,
        idle,
    }
}

#[test]
fn active_uninitialized_channel_withholds_the_watermark() {
    let channels = vec![channel("a", Some(20), false), channel("b", None, false)];
    assert_eq!(
        crate::checkpoint::classify_channel_progress(&channels),
        Ok(crate::checkpoint::CheckpointWatermark::Uninitialized)
    );

    let mut manifest = valid_manifest(1);
    manifest.channel_progress = channels;
    assert!(manifest
        .validate(KeyGroupCount::try_from(1_u16).unwrap())
        .is_empty());
    manifest.checkpoint_watermark = Some(20);
    assert!(manifest
        .validate(KeyGroupCount::try_from(1_u16).unwrap())
        .iter()
        .any(|error| error.message.contains("does not match channel progress")));
}

#[test]
fn idle_uninitialized_channel_is_excluded() {
    let channels = vec![channel("a", Some(20), false), channel("b", None, true)];
    assert_eq!(
        crate::checkpoint::classify_channel_progress(&channels),
        Ok(crate::checkpoint::CheckpointWatermark::Active(20))
    );

    let mut manifest = valid_manifest(1);
    manifest.channel_progress = channels;
    manifest.checkpoint_watermark = Some(20);
    assert!(manifest
        .validate(KeyGroupCount::try_from(1_u16).unwrap())
        .is_empty());
}

#[test]
fn all_idle_channels_have_no_watermark() {
    let channels = vec![channel("a", None, true), channel("b", None, true)];
    assert_eq!(
        crate::checkpoint::classify_channel_progress(&[]),
        Ok(crate::checkpoint::CheckpointWatermark::Idle)
    );
    assert_eq!(
        crate::checkpoint::classify_channel_progress(&channels),
        Ok(crate::checkpoint::CheckpointWatermark::Idle)
    );
    let retained = vec![channel("a", Some(10), true), channel("b", Some(20), true)];
    assert_eq!(
        crate::checkpoint::channel_progress_frontier(&retained),
        Ok(Some(20))
    );

    let mut manifest = valid_manifest(1);
    manifest.channel_progress = channels;
    assert!(manifest
        .validate(KeyGroupCount::try_from(1_u16).unwrap())
        .is_empty());
    manifest.checkpoint_watermark = Some(20);
    assert!(manifest
        .validate(KeyGroupCount::try_from(1_u16).unwrap())
        .iter()
        .any(|error| error.message.contains("does not match channel progress")));

    let invalid = vec![channel("idle", Some(i64::MIN), true)];
    assert!(crate::checkpoint::classify_channel_progress(&invalid).is_err());
}

#[test]
fn v10_round_trip_carries_portability_channels_ranges_sinks_and_prior_chunk_refs() {
    let mut manifest = valid_manifest(9);
    manifest.source_offsets.insert(
        "source".into(),
        ConnectorCheckpoint {
            input_channels: Some(vec![b"partition-0".to_vec(), b"partition-1".to_vec()]),
            ..ConnectorCheckpoint::default()
        },
    );
    let prior = StateChunkId {
        participant_id: 2,
        checkpoint_id: 8,
    };
    let current_data = b"ew";
    manifest.node_data.object_length = current_data.len() as u64;
    manifest.node_data.sha256 = checkpoint_sha256(current_data);
    manifest.state_frames = vec![
        StateFrame {
            key: StateFrameKey::OperatorWhole {
                operator_id: "graph".into(),
            },
            chunk: prior,
            range: ByteRange {
                offset: 0,
                length: 3,
            },
            sha256: checkpoint_sha256(b"old"),
        },
        StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "join".into(),
                vnode: 0,
            },
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset: 0,
                length: 2,
            },
            sha256: checkpoint_sha256(b"ew"),
        },
    ];
    manifest.referenced_chunks.push(ReferencedStateChunk {
        chunk: prior,
        object_length: 3,
        sha256: checkpoint_sha256(b"old"),
        ref_count: NonZeroU32::new(1).unwrap(),
    });
    manifest.prepared_sinks.push(PreparedSinkDescriptor {
        sink_name: "sink".into(),
        format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
        payload: Some(ByteRange {
            offset: 2,
            length: 0,
        }),
        sha256: checkpoint_descriptor_sha256(Some(b"")),
    });

    let one = KeyGroupCount::try_from(1_u16).unwrap();
    assert!(manifest.validate(one).is_empty());
    let encoded = serde_json::to_vec(&manifest).unwrap();
    let restored: CheckpointManifest = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(restored, manifest);
    assert!(!restored.reassignment_portable);
}

#[test]
fn previous_manifest_versions_are_not_accepted() {
    let mut manifest = valid_manifest(1);
    manifest.version = 8;
    let errors = manifest.validate(KeyGroupCount::try_from(1_u16).unwrap());
    assert!(errors
        .iter()
        .any(|error| error.message.contains("unsupported manifest version 8")));

    let mut json = serde_json::to_value(valid_manifest(1)).unwrap();
    json.as_object_mut().unwrap().remove("node_data");
    assert!(serde_json::from_value::<CheckpointManifest>(json).is_err());

    let mut v8_shape = serde_json::to_value(valid_manifest(1)).unwrap();
    let object = v8_shape.as_object_mut().unwrap();
    object.insert("version".into(), serde_json::Value::from(8));
    object.remove("reassignment_portable");
    assert!(serde_json::from_value::<CheckpointManifest>(v8_shape).is_err());
}

#[test]
fn local_manifest_cannot_claim_reassignment_portability() {
    let mut manifest = valid_manifest(1);
    manifest.reassignment_portable = true;

    let errors = manifest.validate(KeyGroupCount::try_from(1_u16).unwrap());
    assert!(errors.iter().any(|error| {
        error
            .message
            .contains("local manifest cannot claim vnode reassignment portability")
    }));
}

#[test]
fn validation_rejects_noncanonical_or_mismatched_input_channels() {
    let mut manifest = valid_manifest(1);
    manifest.source_offsets.insert(
        "source".into(),
        ConnectorCheckpoint {
            input_channels: Some(vec![b"partition-1".to_vec(), b"partition-0".to_vec()]),
            ..ConnectorCheckpoint::default()
        },
    );

    assert!(manifest
        .validate(KeyGroupCount::try_from(1_u16).unwrap())
        .iter()
        .any(|error| error.message.contains("input_channels")));

    manifest
        .source_offsets
        .get_mut("source")
        .unwrap()
        .input_channels = Some(vec![crate::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec()]);
    assert!(manifest
        .validate(KeyGroupCount::try_from(1_u16).unwrap())
        .iter()
        .any(|error| error.message.contains("reserved logical watermark channel")));

    manifest
        .source_offsets
        .get_mut("source")
        .unwrap()
        .input_channels = Some(vec![b"partition-0".to_vec(), b"partition-1".to_vec()]);
    manifest.channel_progress = vec![channel("partition-0", Some(1), false)];
    manifest.checkpoint_watermark = Some(1);
    assert!(manifest
        .validate(KeyGroupCount::try_from(1_u16).unwrap())
        .iter()
        .any(|error| error.message.contains("channel_progress roster")));
}

#[test]
fn logical_singleton_is_independent_of_the_connector_roster() {
    let mut manifest = valid_manifest(1);
    manifest.source_offsets.insert(
        "source".into(),
        ConnectorCheckpoint {
            input_channels: Some(vec![b"partition-0".to_vec(), b"partition-1".to_vec()]),
            ..ConnectorCheckpoint::default()
        },
    );
    manifest.channel_progress = vec![ChannelProgress {
        participant_id: LOCAL_NODE_ID.0,
        source_name: "source".into(),
        input_channel: crate::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec(),
        watermark: Some(1),
        idle: false,
    }];
    manifest.checkpoint_watermark = Some(1);

    let one = KeyGroupCount::try_from(1_u16).unwrap();
    assert!(manifest.validate(one).is_empty());

    manifest.channel_progress[0].source_name = "missing".into();
    assert!(manifest
        .validate(one)
        .iter()
        .any(|error| error.message.contains("not in source_names")));
}

#[test]
fn validation_rejects_gaps_bad_refcounts_and_out_of_bounds_vnodes() {
    let mut manifest = valid_manifest(3);
    let current_data = b"xta";
    manifest.node_data.object_length = current_data.len() as u64;
    manifest.node_data.sha256 = checkpoint_sha256(current_data);
    manifest.state_frames.push(StateFrame {
        key: StateFrameKey::Vnode {
            operator_id: "join".into(),
            vnode: manifest.vnode_count,
        },
        chunk: manifest.node_data.chunk,
        range: ByteRange {
            offset: 1,
            length: 2,
        },
        sha256: checkpoint_sha256(b"ta"),
    });

    let errors = manifest.validate(KeyGroupCount::try_from(1_u16).unwrap());
    assert!(errors.iter().any(|error| error.message.contains("outside")));
    assert!(errors
        .iter()
        .any(|error| error.message.contains("starts at 1, expected 0")));
}

#[test]
fn absent_and_empty_sink_descriptors_remain_distinct() {
    let mut absent = valid_manifest(2);
    absent.prepared_sinks.push(PreparedSinkDescriptor {
        sink_name: "sink".into(),
        format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
        payload: None,
        sha256: checkpoint_descriptor_sha256(None),
    });
    let mut empty = absent.clone();
    empty.prepared_sinks[0].payload = Some(ByteRange {
        offset: 0,
        length: 0,
    });
    empty.prepared_sinks[0].sha256 = checkpoint_descriptor_sha256(Some(b""));

    let one = KeyGroupCount::try_from(1_u16).unwrap();
    assert!(absent.validate(one).is_empty());
    assert!(empty.validate(one).is_empty());
    assert_ne!(
        absent.prepared_sinks[0].sha256,
        empty.prepared_sinks[0].sha256
    );
    assert_ne!(
        serde_json::to_vec(&absent).unwrap(),
        serde_json::to_vec(&empty).unwrap()
    );
}
