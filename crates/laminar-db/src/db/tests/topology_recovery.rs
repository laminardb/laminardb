//! Actual aggregate/state/source/subscription codecs on a private target checkpoint image.
//! Installation evidence here is an authority fixture; no runtime or broker certification is claimed.

use super::*;
use crate::subscription::cluster::{ClusterSubscriptionOutputState, OutputWriterAuthority};
use laminar_core::checkpoint::{ChannelProgress, CheckpointScope};
use laminar_core::cluster::control::TopologyRecoveryCut;

#[path = "topology_subscription_replay.rs"]
mod subscriptions;

#[path = "topology_stateful.rs"]
mod stateful;

fn future_input(
    value: i64,
    first: u64,
    timestamp_us: i64,
) -> rustc_hash::FxHashMap<Arc<str>, Vec<RecordBatch>> {
    let mut input = positioned_input(value, first);
    let batch = &mut input.get_mut("trades").unwrap()[0];
    let mut columns = batch.columns().to_vec();
    columns[1] = Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
        timestamp_us,
        timestamp_us + 1000,
        timestamp_us + 2000,
    ]));
    *batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
    input
}

pub(super) async fn checkpointed() -> (
    Fixture,
    laminar_core::cluster::control::TopologyOperationId,
    CheckpointManifest,
) {
    let (fixture, operation, manifest, _) = Box::pin(checkpointed_with_reader(false)).await;
    (fixture, operation, manifest)
}

pub(super) async fn checkpointed_with_reader(
    attach_reader: bool,
) -> (
    Fixture,
    laminar_core::cluster::control::TopologyOperationId,
    CheckpointManifest,
    Option<crate::subscription::cluster::ClusterSubscriptionReader>,
) {
    Box::pin(checkpointed_with_additions(attach_reader, Vec::new())).await
}

async fn checkpointed_with_additions(
    attach_reader: bool,
    additions: Vec<laminar_core::cluster::control::CatalogManifestEntry>,
) -> (
    Fixture,
    laminar_core::cluster::control::TopologyOperationId,
    CheckpointManifest,
    Option<crate::subscription::cluster::ClusterSubscriptionReader>,
) {
    let (fixture, committed) = committed_fixture_with_additions(additions).await;
    let mut image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    // This fixture captures a complete cycle, independent of CI scheduling delays.
    image.graph.set_query_budget_ns(u64::MAX);
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
    let reader = if attach_reader {
        let certificate = input
            .root()
            .subscriptions
            .iter()
            .find(|mapping| mapping.parent_certificate.stream_id == "totals")
            .unwrap();
        Some(
            crate::subscription::cluster::ClusterSubscriptionReader::open(
                Arc::clone(&fixture.authority.lease_store),
                Arc::new(
                    ObjectStoreCheckpointStore::new(
                        Arc::clone(&fixture.authority.checkpoint_store),
                        "",
                    )
                    .with_key_group_count(fixture.db.checkpoint_key_groups()),
                ),
                Arc::new(certificate.parent_certificate.clone()),
                crate::subscription::SubscribeStart::Tail,
                None,
            )
            .await
            .unwrap(),
        )
    } else {
        None
    };
    let before = image.graph.capture_subscription_frontiers().unwrap();
    let output = image
        .graph
        .execute_cycle(&future_input(5, 3, 150_000), 100, None)
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
    (fixture, committed.operation_id, manifest, reader)
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

pub(super) async fn advance_decode_assignment(
    fixture: &Fixture,
    operation: laminar_core::cluster::control::TopologyOperationId,
) {
    use laminar_core::cluster::control::{
        AssignmentDrainDecision, AssignmentSnapshotStore, RotateOutcome,
    };

    // Preserve the complete owner map and process roster through an authority-settled drain
    // Commit. The actor recovery monitor audits this successor edge; a storage-only version
    // increment would correctly lose Prepare ownership before selecting a checkpoint.
    let input = fixture
        .authority
        .controller
        .committed_topology_restore_input(operation)
        .await
        .unwrap();
    if input.operation().phase != laminar_core::cluster::control::TopologyAdmissionPhase::Active {
        fixture
            .authority
            .controller
            .certify_topology_installation(&input, uuid::Uuid::from_u128(808))
            .await
            .unwrap();
        let fresh = fixture
            .authority
            .controller
            .committed_topology_restore_input(operation)
            .await
            .unwrap();
        fixture
            .authority
            .controller
            .release_topology_target(&fresh)
            .await
            .unwrap();
    }
    let assignments = AssignmentSnapshotStore::new(Arc::clone(&fixture.authority.checkpoint_store));
    let prior = assignments.load().await.unwrap().unwrap();
    let proof = fixture.authority.controller.capture_leader_proof().unwrap();
    let draining = prior
        .next_draining(
            prior.vnodes.clone(),
            prior.participants.clone(),
            proof.clone(),
        )
        .unwrap();
    assert!(matches!(
        assignments
            .save_if_version(&draining, prior.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));
    let checkpoint = fixture
        .authority
        .lease_store
        .highest_cluster_committed_outcome()
        .await
        .unwrap()
        .unwrap()
        .committed_checkpoint
        .unwrap();
    let decision = AssignmentDrainDecision::commit(
        draining.drain_transition.as_ref().unwrap(),
        proof.clone(),
        checkpoint,
    )
    .unwrap();
    fixture
        .authority
        .lease_store
        .record_assignment_drain_decision(&proof, decision)
        .await
        .unwrap();
    let next = draining.committed_target().unwrap();
    assignments.finalize_drain(&draining, &next).await.unwrap();
    crate::rebalance::audit_assignment_snapshot_authority(
        &assignments,
        Some(&fixture.authority.controller),
        &next,
    )
    .await
    .unwrap();
    let fence = next.assignment_fence().unwrap();
    let registry = fixture.db.vnode_registry.lock().clone().unwrap();
    let owners = registry.versioned_snapshot().owners().to_vec();
    registry.set_assignment_and_version(owners.into(), fence.assignment_version);
    let owner_ids = vec![1; 8];
    fixture
        .db
        .shuffle_sender
        .lock()
        .clone()
        .unwrap()
        .install_assignment_fence(&fence, &owner_ids)
        .unwrap();
    fixture
        .db
        .shuffle_receiver
        .lock()
        .clone()
        .unwrap()
        .install_assignment_fence(&fence, &owner_ids)
        .unwrap();
    fixture
        .authority
        .controller
        .publish_checkpoint_assignment_fence(Some(fence.clone()));
    fixture
        .authority
        .controller
        .announce_adopted_assignment(&laminar_core::checkpoint::CheckpointAssignmentAdoption {
            participant: fence.participants[0],
            assignment_version: fence.assignment_version,
            partitioning_abi_version: fence.partitioning_abi_version,
            vnode_count: fence.vnode_count,
            assignment_digest: fence.assignment_digest,
            vnode_state_ready: true,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn topology_recovery_older_assignment_uses_checkpoint_bootstrap_without_relabelling() {
    for target_checkpoint in [false, true] {
        let (fixture, operation) = if target_checkpoint {
            let (fixture, operation, _) = Box::pin(checkpointed()).await;
            (fixture, operation)
        } else {
            let (fixture, committed) = committed_fixture().await;
            (fixture, committed.operation_id)
        };
        advance_decode_assignment(&fixture, operation).await;
        let selected = fixture
            .authority
            .controller
            .committed_topology_recovery_input(operation)
            .await
            .unwrap();
        assert_eq!(
            selected
                .checkpoint()
                .assignment_fence
                .as_ref()
                .unwrap()
                .assignment_version,
            1
        );
        assert_eq!(selected.migration().assignment().assignment_version, 2);
        let store =
            ObjectStoreCheckpointStore::new(Arc::clone(&fixture.authority.checkpoint_store), "")
                .with_participant_id(1)
                .with_key_group_count(fixture.db.checkpoint_key_groups());
        let reader = crate::recovery_manager::RecoveryManager::new(
            &store,
            &selected.checkpoint().pipeline_identity,
            &selected.checkpoint().deployment_id,
            CheckpointScope::Cluster,
        );
        let recovered = reader
            .recover_topology_selection(&selected, 1024 * 1024)
            .await
            .unwrap();
        assert!(
            recovered.reassigned,
            "older topology cut bypassed checkpoint bootstrap"
        );
        assert_eq!(recovered.committed, *selected.checkpoint());
        assert_eq!(recovered.target_vnodes, selected.migration().owned_vnodes());
        recovered
            .validate_topology_assignment(selected.migration())
            .unwrap();
        for case in 0..7 {
            let mut incompatible = recovered.clone();
            match case {
                0 => incompatible.reassigned = false,
                1 => incompatible.committed.reassignment_portable = false,
                2 => incompatible.predecessor_owners[0] = NodeId(2),
                3 => {
                    incompatible.target_vnodes.pop();
                }
                4 => {
                    incompatible
                        .committed
                        .assignment_fence
                        .as_mut()
                        .unwrap()
                        .assignment_version = 3;
                }
                5 => {
                    incompatible
                        .committed
                        .assignment_fence
                        .as_mut()
                        .unwrap()
                        .participants[0]
                        .node_id = 2;
                }
                6 => {
                    incompatible
                        .committed
                        .assignment_fence
                        .as_mut()
                        .unwrap()
                        .assignment_digest[0] ^= 0xff;
                }
                _ => unreachable!(),
            }
            assert!(matches!(
                incompatible.validate_topology_assignment(selected.migration()),
                Err(DbError::Topology(TopologyError::Fenced))
            ));
        }
        let mut image = fixture
            .db
            .prepare_cluster_topology_recovery(operation)
            .await
            .unwrap();
        let output = image
            .graph
            .execute_cycle(&input(5), 100, None)
            .await
            .unwrap();
        assert_eq!(
            total(&output["totals"]),
            if target_checkpoint { 60 } else { 45 }
        );
        assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
        assert!(fixture.db.owned_source_tasks.lock().is_empty());
        assert!(fixture.db.owned_sink_handles.lock().is_empty());
    }
}
