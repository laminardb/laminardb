//! Real operator capture/restore through the DB API; no connector lifecycle effects are allowed.

use super::*;
use crate::PreparedTopologySourcePosition;
use laminar_core::checkpoint::{
    ByteRange, CheckpointAttempt, CheckpointManifest, CheckpointStore, CommittedCheckpointIndex,
    CommittedParticipantRef, ConnectorCheckpoint, NodePartitionRange, NodeSubscriptionManifest,
    NodeSubscriptionStreamManifest, ObjectStoreCheckpointStore, StateFrame, StateFrameKey,
};
use laminar_core::checkpoint_decision::{CheckpointArtifactInventory, CheckpointVerdict};
use std::collections::BTreeMap;

#[path = "topology_retirement.rs"]
mod retirement;

async fn restorable_fixture() -> (
    Fixture,
    laminar_core::cluster::control::TopologyAdmissionStatus,
) {
    restorable_fixture_with_additions(Vec::new()).await
}

async fn restorable_fixture_with_additions(
    additions: Vec<laminar_core::cluster::control::CatalogManifestEntry>,
) -> (
    Fixture,
    laminar_core::cluster::control::TopologyAdmissionStatus,
) {
    let mut entries: Vec<_> = independent_pipeline()
        .into_iter()
        .zip([
            (
                "added_source",
                laminar_core::cluster::control::CatalogObjectKind::Source,
            ),
            (
                "added_stream",
                laminar_core::cluster::control::CatalogObjectKind::Stream,
            ),
            (
                "added_sink",
                laminar_core::cluster::control::CatalogObjectKind::Sink,
            ),
        ])
        .map(
            |(ddl, (name, kind))| laminar_core::cluster::control::CatalogManifestEntry {
                canonical_name: name.into(),
                kind,
                catalog_generation: 1,
                ddl,
            },
        )
        .collect();
    entries.extend(additions);
    restorable_fixture_with_statements(entries.into_iter().map(|entry| entry.ddl).collect()).await
}

async fn restorable_fixture_with_statements(
    statements: Vec<String>,
) -> (
    Fixture,
    laminar_core::cluster::control::TopologyAdmissionStatus,
) {
    let (fixture, assignments) = preparation_fixture_with_generation(7).await;
    let admitted = admit_preparation_statements(&fixture, &assignments, statements).await;
    fixture
        .db
        .prepare_cluster_topology_operation(admitted.operation_id)
        .await
        .unwrap();
    let (_, plan, _, descriptor) = fixture
        .authority
        .lease_store
        .topology_preparation_input(admitted.operation_id)
        .await
        .unwrap();
    let owner_ids = vec![1; 8];
    let registry = fixture.db.vnode_registry.lock().clone().unwrap();
    assert_eq!(
        registry.assignment_version(),
        plan.assignment.assignment_version
    );
    let sender = fixture.db.shuffle_sender.lock().clone().unwrap();
    let receiver = fixture.db.shuffle_receiver.lock().clone().unwrap();
    sender
        .install_assignment_fence(&plan.assignment, &owner_ids)
        .unwrap();
    receiver
        .install_assignment_fence(&plan.assignment, &owner_ids)
        .unwrap();
    fixture
        .db
        .coordinator
        .lock()
        .await
        .as_mut()
        .unwrap()
        .bind_deployment_id(descriptor.deployment_id.clone())
        .unwrap();
    let mut streams = fixture.db.connector_manager.lock().streams().clone();
    fixture
        .db
        .bind_subscription_output_certificates(&mut streams, Some(&descriptor.parent_pipeline))
        .await
        .unwrap();
    fixture
        .db
        .connector_manager
        .lock()
        .install_stream_subscription_certificates(&streams)
        .unwrap();
    let mut parent_graph = fixture
        .db
        .build_topology_restore_operator_graph(
            &streams,
            &HashMap::new(),
            &rustc_hash::FxHashSet::from_iter(["totals".into()]),
            &rustc_hash::FxHashMap::default(),
            &descriptor.parent_pipeline,
            crate::operator::sql_query::ClusterShuffleConfig {
                registry,
                sender,
                receiver,
                topology: None,
                self_id: NodeId(1),
            },
        )
        .unwrap()
        .initialize_managed_state()
        .await
        .unwrap();
    let output = parent_graph
        .execute_cycle(&input(10), i64::MIN, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 30);
    parent_graph.take_prepared_subscription_outputs();
    parent_graph.commit_prepared_subscription_outputs();
    let subscription_frontiers = parent_graph.capture_subscription_frontiers().unwrap();
    let capture = parent_graph.capture_state(1024 * 1024).unwrap();
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
    // Drop the test's parent image before loading its target; production keeps the old held graph.
    drop(parent_graph);
    fixture
        .authority
        .controller
        .begin_topology_checkpoint_cut(
            &fixture.authority.lease.proof(),
            admitted.operation_id,
            &admitted.plan,
            CheckpointArtifactInventory {
                deployment_id: descriptor.deployment_id.clone(),
                pipeline_identity: descriptor.parent_pipeline.clone(),
                attempt: CheckpointAttempt::canonical(1),
                assignment_fence: Some(plan.assignment.clone()),
                sink_artifact_intent_protocol: true,
            },
        )
        .await
        .unwrap();
    let store = ObjectStoreCheckpointStore::new(fixture.authority.checkpoint_store.clone(), "")
        .with_key_group_count(fixture.db.checkpoint_key_groups())
        .with_participant_id(1);
    let mut manifest = CheckpointManifest::new_with_key_group_count(1, 1, store.key_group_count());
    manifest.bind_participant(1);
    manifest.assignment_fence = Some(plan.assignment.clone());
    manifest.deployment_id.clone_from(&descriptor.deployment_id);
    manifest
        .pipeline_identity
        .clone_from(&descriptor.parent_pipeline);
    manifest.reassignment_portable = true;
    manifest.source_names = vec!["trades".into()];
    let mut source = ConnectorCheckpoint::new();
    source.offsets.insert("old.cursor".into(), "3".into());
    source
        .metadata
        .insert("connector".into(), "planning-source".into());
    source.source_assignment_version =
        std::num::NonZeroU64::new(plan.assignment.assignment_version);
    manifest.source_offsets.insert("trades".into(), source);
    manifest.sink_names = vec!["existing_sink".into()];
    let payload = payload.freeze();
    manifest.node_data.object_length = payload.len() as u64;
    manifest.node_data.sha256 = laminar_core::checkpoint::checkpoint_sha256(&payload);
    for frame in &mut frames {
        frame.chunk = manifest.node_data.chunk;
    }
    frames.sort_by(|a, b| a.key.cmp(&b.key));
    manifest.state_frames = frames;
    let mut subscription = NodeSubscriptionManifest {
        protocol_version: laminar_core::checkpoint::SubscriptionProtocolVersion::CURRENT,
        epoch: 1,
        checkpoint_id: 1,
        participant_id: 1,
        assignment_certificate: plan.assignment.clone(),
        streams: subscription_frontiers
            .into_iter()
            .map(|capture| NodeSubscriptionStreamManifest {
                distribution_certificate: capture.certificate.as_ref().clone(),
                segments: Vec::new(),
                ranges: capture
                    .frontiers
                    .into_iter()
                    .map(|frontier| NodePartitionRange {
                        partition: frontier.partition,
                        first_sequence: frontier.through_sequence,
                        through_sequence: frontier.through_sequence,
                    })
                    .collect(),
            })
            .collect(),
        manifest_digest: laminar_core::checkpoint::SubscriptionDigest::from_bytes([0; 32]),
    };
    subscription.seal(&manifest.owned_vnodes).unwrap();
    manifest.subscription_output = Some(subscription);
    let encoded = store.save_checkpoint(&manifest, &[payload]).await.unwrap();
    let index = CommittedCheckpointIndex {
        version: laminar_core::checkpoint::COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: descriptor.deployment_id,
        pipeline_identity: descriptor.parent_pipeline,
        epoch: 1,
        checkpoint_id: 1,
        scope: laminar_core::checkpoint::CheckpointScope::Cluster,
        vnode_count: 8,
        assignment_fence: Some(plan.assignment.clone()),
        reassignment_portable: true,
        predecessor: None,
        participants: vec![CommittedParticipantRef::from_manifest(&manifest, &encoded).unwrap()],
        source_names: manifest.source_names.clone(),
        source_offsets: manifest.source_offsets.clone().into_iter().collect(),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    };
    let reference = CheckpointDecisionStore::new(fixture.authority.checkpoint_store.clone())
        .create_committed_checkpoint(&index)
        .await
        .unwrap();
    fixture
        .authority
        .lease_store
        .record_cluster_outcome(
            &fixture.authority.lease.proof(),
            1,
            1,
            plan.assignment,
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap();
    fixture
        .authority
        .controller
        .complete_topology_checkpoint_cut(
            &fixture.authority.lease.proof(),
            CheckpointAttempt::canonical(1),
        )
        .await
        .unwrap();
    fixture.db.topology_cut_hold.store(true, Ordering::Release);
    let staged = fixture
        .db
        .stage_cluster_topology_migration_root(admitted.operation_id)
        .await
        .unwrap();
    (fixture, staged)
}

fn append_frame(
    payload: &mut bytes::BytesMut,
    key: StateFrameKey,
    state: bytes::Bytes,
) -> StateFrame {
    let offset = payload.len() as u64;
    payload.extend_from_slice(&state);
    StateFrame {
        key,
        chunk: laminar_core::checkpoint::StateChunkId {
            participant_id: 1,
            checkpoint_id: 1,
        },
        range: ByteRange {
            offset,
            length: state.len() as u64,
        },
        sha256: laminar_core::checkpoint::checkpoint_sha256(&state),
    }
}

fn input(value: i64) -> rustc_hash::FxHashMap<Arc<str>, Vec<RecordBatch>> {
    let batch = RecordBatch::try_new(
        crate::temporal_test_source::schema(),
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1000, 2000, 3000,
            ])),
            Arc::new(arrow::array::Int64Array::from(vec![value; 3])),
        ],
    )
    .unwrap();
    rustc_hash::FxHashMap::from_iter([(Arc::from("trades"), vec![batch])])
}

fn total(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|batch| {
            let values = batch
                .column_by_name("total")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            let weights = batch
                .column_by_name(laminar_core::changelog::WEIGHT_COLUMN)
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap()
                });
            // Compare the inserted sum, not the retraction of its preceding value.
            (0..batch.num_rows())
                .filter(|&row| weights.is_none_or(|w| w.value(row) > 0))
                .map(|row| values.value(row))
                .sum::<i64>()
        })
        .sum()
}

fn positioned_input(value: i64, first: u64) -> rustc_hash::FxHashMap<Arc<str>, Vec<RecordBatch>> {
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceRowPositionCapability,
    };
    let batch = input(value)["trades"][0].clone();
    let positioned = schema_with_source_row_positions(&batch.schema()).unwrap();
    let mutations = schema_with_source_mutations_and_row_positions(&batch.schema()).unwrap();
    let batch = runtime_probe::positioned(batch, first)
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &positioned,
            &mutations,
        )
        .unwrap();
    rustc_hash::FxHashMap::from_iter([(Arc::from("trades"), vec![batch])])
}

#[tokio::test]
async fn topology_restore_db_preserves_real_state_cursors_incarnation_and_sequences() {
    let (fixture, staged) = restorable_fixture().await;
    let paths = object_paths(fixture.authority.checkpoint_store.as_ref()).await;
    let inventory = fixture.db.catalog_manifest_inventory().unwrap();
    let authority = fixture.authority.lease_store.load().await.unwrap();
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    assert_eq!(image.target_version().get(), 2);
    assert_eq!(image.restored_frame_count(), 9);
    assert!(image.managed_state_bytes() > 0);
    assert_eq!(
        image.root().subscriptions[0]
            .target_certificate
            .catalog_generation,
        7
    );
    assert!(image.root().subscriptions[0]
        .frontiers
        .iter()
        .any(|f| f.through_sequence > laminar_core::checkpoint::PartitionSequence::FIRST));
    assert!(
        matches!(&image.source_positions()["trades"], PreparedTopologySourcePosition::Preserved { attempt, checkpoint }
        if *attempt == CheckpointAttempt::canonical(1) && checkpoint.offsets()["old.cursor"] == "3" && checkpoint.assignment_version().is_some())
    );
    assert!(
        matches!(&image.source_positions()["added_source"], PreparedTopologySourcePosition::Initialized { checkpoint }
        if checkpoint.offsets()["partition-0-next"] == "91" && checkpoint.assignment_version().is_none())
    );
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_restore(staged.operation_id)
            .await,
        Err(DbError::Topology(TopologyError::PlanningBusy))
    ));
    assert_eq!(
        object_paths(fixture.authority.checkpoint_store.as_ref()).await,
        paths
    );
    assert_eq!(
        fixture.authority.lease_store.load().await.unwrap(),
        authority
    );
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), inventory);
    assert_eq!(
        fixture
            .db
            .coordinator
            .lock()
            .await
            .as_ref()
            .unwrap()
            .bound_pipeline_identity()
            .unwrap(),
        image.parent_checkpoint().pipeline_identity
    );
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    // Direct graph execution is test-only: an independent sum proves the restored image retained
    // the prefix rather than cold-starting. No production installation API exposes this graph.
    let output = image
        .graph
        .execute_cycle(&input(5), i64::MIN, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 45);
    drop(image);
    let retry = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    assert_eq!(retry.restored_frame_count(), 9);
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn topology_restore_db_rejects_missing_hold_divergent_environment_and_shutdown() {
    let (fixture, staged) = restorable_fixture().await;
    fixture.db.topology_cut_hold.store(false, Ordering::Release);
    assert!(fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .is_err());
    fixture.db.topology_cut_hold.store(true, Ordering::Release);
    let mut changed = fixture.db.connector_manager.lock().sources()["trades"].clone();
    changed
        .connector_options
        .insert("topic".into(), "other".into());
    fixture.db.connector_manager.lock().register_source(changed);
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_restore(staged.operation_id)
            .await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    fixture.db.shutdown.store(true, Ordering::Release);
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_restore(staged.operation_id)
            .await,
        Err(DbError::Shutdown)
    ));
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
}

#[tokio::test]
async fn topology_restore_db_cancellation_drops_the_decoded_image_and_releases_its_slot() {
    let (fixture, staged) = restorable_fixture().await;
    fixture
        .restore_validation
        .block
        .store(true, Ordering::Release);
    let db = Arc::clone(&fixture.db);
    let task = tokio::spawn(async move {
        db.prepare_cluster_topology_restore(staged.operation_id)
            .await
    });
    fixture.restore_validation.entered.notified().await;
    assert!(fixture.db.topology_validation_lock.try_lock().is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    fixture
        .restore_validation
        .block
        .store(false, Ordering::Release);
    let retry = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    assert_eq!(retry.restored_frame_count(), 9);
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
}

#[tokio::test]
async fn topology_restore_db_recovery_fences_a_late_decoded_image() {
    let (fixture, staged) = restorable_fixture().await;
    fixture
        .restore_validation
        .block
        .store(true, Ordering::Release);
    let db = Arc::clone(&fixture.db);
    let task = tokio::spawn(async move {
        db.prepare_cluster_topology_restore(staged.operation_id)
            .await
    });
    fixture.restore_validation.entered.notified().await;
    fixture.authority.controller.set_recovering(true);
    fixture.restore_validation.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
}

#[tokio::test]
async fn topology_restore_db_keeps_ordinary_recovery_strict_and_bounds_payload_reads() {
    let (fixture, staged) = restorable_fixture().await;
    let input = fixture
        .authority
        .controller
        .topology_restore_input(staged.operation_id)
        .await
        .unwrap();
    let store = ObjectStoreCheckpointStore::new(fixture.authority.checkpoint_store.clone(), "")
        .with_key_group_count(fixture.db.checkpoint_key_groups())
        .with_participant_id(1);
    let target_reader = crate::RecoveryManager::new(
        &store,
        &input.descriptor().target_pipeline,
        &input.descriptor().deployment_id,
        laminar_core::checkpoint::CheckpointScope::Cluster,
    );
    assert!(target_reader
        .recover_committed(input.outcome(), input.checkpoint())
        .await
        .is_err());
    assert!(target_reader
        .recover_topology_root(&input, 1024 * 1024)
        .await
        .is_err());
    let parent_reader = crate::RecoveryManager::new(
        &store,
        &input.descriptor().parent_pipeline,
        &input.descriptor().deployment_id,
        laminar_core::checkpoint::CheckpointScope::Cluster,
    );
    assert!(parent_reader
        .recover_topology_root(&input, 1)
        .await
        .is_err());
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
}

#[tokio::test]
async fn topology_restore_db_rejects_missing_and_corrupt_payloads_without_releasing_the_hold() {
    let (fixture, staged) = restorable_fixture().await;
    let objects = &fixture.authority.checkpoint_store;
    let paths = object_paths(objects.as_ref()).await;
    let path = object_store::path::Path::from(
        paths
            .iter()
            .find(|path| path.ends_with("/node-data.bin"))
            .unwrap()
            .as_str(),
    );
    let original = objects.get(&path).await.unwrap().bytes().await.unwrap();
    objects.delete(&path).await.unwrap();
    for corrupt in [false, true] {
        if corrupt {
            objects
                .put(
                    &path,
                    object_store::PutPayload::from_bytes(bytes::Bytes::from(vec![
                        0;
                        original.len()
                    ])),
                )
                .await
                .unwrap();
        }
        assert!(fixture
            .db
            .prepare_cluster_topology_restore(staged.operation_id)
            .await
            .is_err());
        assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
        assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
        assert!(fixture.db.source_gate.load(Ordering::Acquire));
        assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    }
    objects
        .put(&path, object_store::PutPayload::from_bytes(original))
        .await
        .unwrap();
    let image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    assert_eq!(image.restored_frame_count(), 9);
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn topology_restore_db_deadline_drops_the_decoded_image_and_releases_its_slot() {
    let (fixture, staged) = restorable_fixture().await;
    fixture
        .restore_validation
        .block
        .store(true, Ordering::Release);
    tokio::time::pause();
    let db = Arc::clone(&fixture.db);
    let task = tokio::spawn(async move {
        db.prepare_cluster_topology_restore(staged.operation_id)
            .await
    });
    fixture.restore_validation.entered.notified().await;
    assert!(fixture.db.topology_validation_lock.try_lock().is_err());
    tokio::time::advance(Duration::from_secs(46)).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(DbError::Topology(TopologyError::Contended))
    ));
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
}
