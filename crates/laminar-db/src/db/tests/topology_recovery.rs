//! Actual aggregate/state/source/subscription codecs on a private target checkpoint image.
//! Installation evidence here is an authority fixture; no runtime or broker certification is claimed.

use super::*;
use crate::subscription::cluster::{ClusterSubscriptionOutputState, OutputWriterAuthority};
use laminar_core::checkpoint::{ChannelProgress, CheckpointScope};
use laminar_core::cluster::control::TopologyRecoveryCut;

pub(super) async fn checkpointed() -> (
    Fixture,
    laminar_core::cluster::control::TopologyOperationId,
    CheckpointManifest,
) {
    let (fixture, committed) = committed_fixture().await;
    let mut image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let input = image.input.clone();
    let controller = &fixture.authority.controller;
    controller
        .certify_topology_installation(&input, uuid::Uuid::from_u128(808))
        .await
        .unwrap();
    let fresh = controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    controller.release_topology_target(&fresh).await.unwrap();
    let before = image.graph.capture_subscription_frontiers().unwrap();
    let output = image
        .graph
        .execute_cycle(&super::input(5), 100, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 45);
    let outputs = image.graph.take_prepared_subscription_outputs();
    image.graph.commit_prepared_subscription_outputs();
    let frontiers = image.graph.capture_subscription_frontiers().unwrap();
    let mut output_state = ClusterSubscriptionOutputState::new(
        before
            .iter()
            .map(|capture| Arc::clone(&capture.certificate))
            .collect(),
        Some(input.process()),
    )
    .unwrap();
    output_state
        .stage_cycle(
            outputs,
            OutputWriterAuthority {
                participant: input.process().participant,
                process_term: input.process().process_term,
                assignment_version: input.assignment().assignment_version,
                assignment_digest: input.assignment().digest(),
            },
        )
        .unwrap();
    output_state.commit_cycle();
    output_state
        .reserve_checkpoint(CheckpointAttempt::canonical(2))
        .unwrap();
    let prepared_output = output_state
        .prepare_checkpoint(CheckpointAttempt::canonical(2), frontiers)
        .unwrap();
    let capture = image.graph.capture_state(1024 * 1024).unwrap();
    let mut staged_bytes = capture.retained_bytes();
    let mut payload = bytes::BytesMut::new();
    let mut frames = Vec::new();
    for whole in capture.whole {
        let state = whole
            .state
            .materialize(&mut staged_bytes, 1024 * 1024)
            .unwrap();
        frames.push(append_frame(
            &mut payload,
            StateFrameKey::OperatorWhole {
                operator_id: format!("graph:{}", whole.operator_id),
            },
            state,
        ));
    }
    for (name, vnode) in capture.vnodes {
        let state = vnode
            .state
            .unwrap()
            .materialize(&mut staged_bytes, 1024 * 1024)
            .unwrap();
        frames.push(append_frame(
            &mut payload,
            StateFrameKey::Vnode {
                operator_id: format!("graph:{name}"),
                vnode: u16::try_from(vnode.vnode).unwrap(),
            },
            state,
        ));
    }
    let store = ObjectStoreCheckpointStore::new(fixture.authority.checkpoint_store.clone(), "")
        .with_participant_id(1)
        .with_key_group_count(fixture.db.checkpoint_key_groups());
    let mut manifest = CheckpointManifest::new_with_key_group_count(2, 2, store.key_group_count());
    manifest.bind_participant(1);
    manifest.assignment_fence = Some(input.assignment().clone());
    manifest
        .deployment_id
        .clone_from(&input.descriptor().deployment_id);
    manifest
        .pipeline_identity
        .clone_from(&input.descriptor().target_pipeline);
    manifest.reassignment_portable = true;
    manifest.source_names = vec!["added_source".into(), "trades".into()];
    manifest.sink_names = vec!["added_sink".into(), "existing_sink".into()];
    for (name, key, position, channel) in [
        ("added_source", "partition-0-next", "94", vec![2]),
        ("trades", "old.cursor", "6", vec![1]),
    ] {
        let mut checkpoint = ConnectorCheckpoint::new();
        checkpoint.offsets.insert(key.into(), position.into());
        checkpoint
            .metadata
            .insert("connector".into(), "planning-source".into());
        checkpoint.input_channels = Some(vec![channel.clone()]);
        checkpoint.source_assignment_version =
            std::num::NonZeroU64::new(input.assignment().assignment_version);
        manifest.source_offsets.insert(name.into(), checkpoint);
        manifest.channel_progress.push(ChannelProgress {
            participant_id: 1,
            source_name: name.into(),
            input_channel: channel,
            watermark: Some(100),
            idle: false,
        });
    }
    manifest.channel_progress.sort_by(|left, right| {
        left.source_name
            .cmp(&right.source_name)
            .then_with(|| left.input_channel.cmp(&right.input_channel))
    });
    manifest.checkpoint_watermark = Some(100);
    let payload = payload.freeze();
    manifest.node_data.object_length = payload.len() as u64;
    manifest.node_data.sha256 = laminar_core::checkpoint::checkpoint_sha256(&payload);
    for frame in &mut frames {
        frame.chunk = manifest.node_data.chunk;
    }
    frames.sort_by(|a, b| a.key.cmp(&b.key));
    manifest.state_frames = frames;
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        Box::new(
            ObjectStoreCheckpointStore::new(fixture.authority.checkpoint_store.clone(), "")
                .with_participant_id(1)
                .with_key_group_count(fixture.db.checkpoint_key_groups()),
        ),
    )
    .unwrap();
    coordinator
        .bind_pipeline_identity(input.descriptor().target_pipeline.clone())
        .unwrap();
    coordinator
        .bind_deployment_id(input.descriptor().deployment_id.clone())
        .unwrap();
    coordinator
        .set_decision_store(Arc::new(CheckpointDecisionStore::new(
            fixture.authority.checkpoint_store.clone(),
        )))
        .unwrap();
    coordinator.set_cluster_controller(Arc::clone(controller));
    manifest.subscription_output = coordinator
        .prepare_subscription_output_until(
            CheckpointAttempt::canonical(2),
            Some(input.assignment()),
            prepared_output,
            tokio::time::Instant::now() + Duration::from_secs(15),
        )
        .await
        .unwrap();
    let encoded = store.save_checkpoint(&manifest, &[payload]).await.unwrap();
    let predecessor = input.outcome().committed_checkpoint.clone();
    let manifests = vec![(manifest.clone(), encoded)];
    let index = coordinator
        .build_validated_committed_index_until(
            CheckpointAttempt::canonical(2),
            CheckpointScope::Cluster,
            Some(input.assignment().clone()),
            predecessor,
            &BTreeMap::new(),
            &manifests,
            None,
            tokio::time::Instant::now() + Duration::from_secs(15),
        )
        .await
        .unwrap();
    fixture
        .authority
        .lease_store
        .begin_cluster_checkpoint_artifacts(
            &input.current_leader().unwrap(),
            CheckpointArtifactInventory {
                deployment_id: index.deployment_id.clone(),
                pipeline_identity: index.pipeline_identity.clone(),
                attempt: CheckpointAttempt::canonical(2),
                assignment_fence: index.assignment_fence.clone(),
                sink_artifact_intent_protocol: true,
            },
        )
        .await
        .unwrap();
    let reference = fixture
        .authority
        .lease_store
        .create_committed_checkpoint(&index)
        .await
        .unwrap();
    fixture
        .authority
        .lease_store
        .record_cluster_outcome(
            &input.current_leader().unwrap(),
            2,
            2,
            input.assignment().clone(),
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap();
    drop(image);
    (fixture, committed.operation_id, manifest)
}

#[tokio::test]
async fn topology_recovery_image_restores_newer_aggregate_cursors_and_subscription_frontiers() {
    let (fixture, operation, manifest) = Box::pin(checkpointed()).await;
    let before = fixture.authority.lease_store.load().await.unwrap();
    assert!(fixture
        .db
        .recover_committed_cluster_topology(operation)
        .await
        .is_err());
    let mut image = fixture
        .db
        .prepare_cluster_topology_recovery(operation)
        .await
        .unwrap();
    assert_eq!(
        image.recovery_input().unwrap().cut(),
        TopologyRecoveryCut::TargetCheckpoint
    );
    assert_eq!(image.parent_checkpoint().epoch, 1);
    assert_eq!(image.recovery_checkpoint().epoch, 2);
    assert_ne!(
        image.parent_checkpoint().pipeline_identity,
        image.recovery_checkpoint().pipeline_identity
    );
    for (name, key, value) in [
        ("trades", "old.cursor", "6"),
        ("added_source", "partition-0-next", "94"),
    ] {
        assert!(
            matches!(&image.source_positions()[name], PreparedTopologySourcePosition::Preserved { attempt, checkpoint }
            if *attempt == CheckpointAttempt::canonical(2) && checkpoint.offsets()[key] == value)
        );
    }
    assert_eq!(
        image.recovery_checkpoint().source_watermarks,
        BTreeMap::from([("added_source".into(), 100), ("trades".into(), 100)])
    );
    let frontiers = image.graph.capture_subscription_frontiers().unwrap();
    assert_eq!(
        frontiers[0].certificate.as_ref(),
        &manifest.subscription_output.as_ref().unwrap().streams[0].distribution_certificate
    );
    assert_eq!(
        frontiers[0].frontiers[0].through_sequence,
        manifest.subscription_output.as_ref().unwrap().streams[0].ranges[0].through_sequence
    );
    let output = image
        .graph
        .execute_cycle(&input(5), 100, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 60);
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
}

#[tokio::test]
async fn topology_recovery_image_cannot_borrow_original_installation_or_release() {
    let (fixture, operation, _) = Box::pin(checkpointed()).await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_recovery(operation)
        .await
        .unwrap();
    let sender = fixture.db.shuffle_sender.lock().clone().unwrap();
    let fence = sender.topology_fence();
    assert!(fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .is_err());
    assert_eq!(sender.topology_fence(), fence);
    assert!(fixture
        .db
        .install_committed_cluster_topology(image)
        .await
        .is_err());
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
}

#[tokio::test]
async fn topology_recovery_image_corrupt_target_state_never_loads_valid_parent_root() {
    let (fixture, operation, manifest) = Box::pin(checkpointed()).await;
    fixture
        .authority
        .checkpoint_store
        .put(
            &object_store::path::Path::from(
                "nodes/1/checkpoints/00000000000000000002/node-data.bin",
            ),
            bytes::Bytes::from_static(b"corrupt target state").into(),
        )
        .await
        .unwrap();
    assert!(fixture
        .db
        .prepare_cluster_topology_recovery(operation)
        .await
        .is_err());
    assert!(fixture
        .db
        .recover_committed_cluster_topology(operation)
        .await
        .is_err());
    assert_eq!(manifest.epoch, 2);
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
}
