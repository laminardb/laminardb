//! New managed operators share the exact migration cut, then require strict target recovery.

use super::*;

#[tokio::test]
async fn topology_stateful_root_starts_empty_and_preserves_existing_state_across_commit_loss() {
    let (fixture, staged) = restorable_fixture_with_additions(stateful_additions()).await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let frontiers = image.graph.capture_subscription_frontiers().unwrap();
    assert_eq!(frontiers.len(), 3);
    for capture in frontiers
        .iter()
        .filter(|capture| capture.certificate.stream_id != "totals")
    {
        assert!(capture
            .frontiers
            .iter()
            .all(|frontier| frontier.through_sequence
                == laminar_core::checkpoint::PartitionSequence::FIRST));
    }
    fixture
        .db
        .commit_cluster_topology_target(&mut image)
        .await
        .unwrap();
    let root = image.root().clone();
    drop(image);
    for _ in 0..2 {
        let mut image = fixture
            .db
            .recover_committed_cluster_topology(staged.operation_id)
            .await
            .unwrap();
        // Recovery assertions require complete cycle output, independent of wall-clock budget.
        image.graph.set_query_budget_ns(u64::MAX);
        assert_eq!(image.root(), &root);
        assert_eq!(image.restored_frame_count(), 9);
        let output = image
            .graph
            .execute_cycle(&future_input(5, 3, 200_000), i64::MIN, None)
            .await
            .unwrap();
        assert_eq!(total(&output["totals"]), 45);
        assert_eq!(total(&output["new_total"]), 15);
        assert_eq!(total(&output["new_global"]), 15);
        assert!(output.get("new_join").is_none_or(Vec::is_empty));
        assert!(output.get("new_tumble").is_none_or(Vec::is_empty));
        image.graph.take_prepared_subscription_outputs();
        image.graph.commit_prepared_subscription_outputs();
        image
            .graph
            .set_local_source_frontiers(&rustc_hash::FxHashMap::from_iter([
                (
                    Arc::from("trades"),
                    crate::operator_graph::InputFrontier {
                        watermark: Some(3000),
                        idle: false,
                    },
                ),
                (
                    Arc::from("added_source"),
                    crate::operator_graph::InputFrontier {
                        watermark: Some(3000),
                        idle: false,
                    },
                ),
            ]));
        let closed = image
            .graph
            .execute_cycle(&rustc_hash::FxHashMap::default(), 3000, None)
            .await
            .unwrap();
        assert_eq!(total(&closed["new_tumble"]), 15);
        assert_eq!(total(&closed["new_hop"]), 30);
        assert_eq!(total(&closed["new_session"]), 15);
    }
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_stateful_target_checkpoint_restores_new_state_and_sequences_without_reset() {
    let (fixture, operation, manifest, _) =
        Box::pin(checkpointed_with_additions(false, stateful_additions())).await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_recovery(operation)
        .await
        .unwrap();
    // Recovery assertions require complete cycle output, independent of wall-clock budget.
    image.graph.set_query_budget_ns(u64::MAX);
    assert_eq!(
        image.recovery_input().unwrap().cut(),
        TopologyRecoveryCut::TargetCheckpoint
    );
    let frontiers = image.graph.capture_subscription_frontiers().unwrap();
    for capture in &frontiers {
        let committed = manifest
            .subscription_output
            .as_ref()
            .unwrap()
            .streams
            .iter()
            .find(|stream| {
                stream.distribution_certificate.stream_id == capture.certificate.stream_id
            })
            .unwrap();
        assert_eq!(
            capture.certificate.as_ref(),
            &committed.distribution_certificate
        );
        assert_eq!(
            capture
                .frontiers
                .iter()
                .map(|frontier| frontier.through_sequence)
                .collect::<Vec<_>>(),
            committed
                .ranges
                .iter()
                .map(|range| range.through_sequence)
                .collect::<Vec<_>>()
        );
    }
    let output = image
        .graph
        .execute_cycle(&future_input(7, 6, 200_000), 100, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 66);
    assert_eq!(total(&output["new_total"]), 36);
    assert_eq!(total(&output["new_global"]), 36);
    image.graph.take_prepared_subscription_outputs();
    image.graph.commit_prepared_subscription_outputs();
    image
        .graph
        .set_local_source_frontiers(&rustc_hash::FxHashMap::from_iter([
            (
                Arc::from("trades"),
                crate::operator_graph::InputFrontier {
                    watermark: Some(3000),
                    idle: false,
                },
            ),
            (
                Arc::from("added_source"),
                crate::operator_graph::InputFrontier {
                    watermark: Some(3000),
                    idle: false,
                },
            ),
        ]));
    let closed = image
        .graph
        .execute_cycle(&rustc_hash::FxHashMap::default(), 3000, None)
        .await
        .unwrap();
    assert_eq!(total(&closed["new_tumble"]), 36);
    assert_eq!(total(&closed["new_hop"]), 72);
    assert_eq!(total(&closed["new_session"]), 36);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_stateful_joins_use_only_post_cut_rows_and_preserve_their_checkpointed_buffers() {
    let (fixture, operation, _, _) =
        Box::pin(checkpointed_with_additions(false, stateful_additions())).await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_recovery(operation)
        .await
        .unwrap();
    // Recovery assertions require complete cycle output, independent of wall-clock budget.
    image.graph.set_query_budget_ns(u64::MAX);
    let right = future_input(7, 91, 200_000).remove("trades").unwrap();
    let right = rustc_hash::FxHashMap::from_iter([(Arc::from("added_source"), right)]);
    let output = image.graph.execute_cycle(&right, 100, None).await.unwrap();
    let joined = &output["new_join"];
    assert_eq!(joined.iter().map(RecordBatch::num_rows).sum::<usize>(), 9);
    for batch in joined {
        let left = batch
            .column_by_name("left_value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let right = batch
            .column_by_name("right_value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(left.values().iter().all(|value| *value == 5));
        assert!(right.values().iter().all(|value| *value == 7));
    }
    // The left buffer restored here contains the three future rows, never the old 10-valued prefix.
    image.graph.take_prepared_subscription_outputs();
    image.graph.commit_prepared_subscription_outputs();
    image
        .graph
        .execute_cycle(&future_input(9, 6, 200_000), 100, None)
        .await
        .unwrap();
    image.graph.take_prepared_subscription_outputs();
    image.graph.commit_prepared_subscription_outputs();
    image
        .graph
        .set_local_source_frontiers(&rustc_hash::FxHashMap::from_iter([
            (
                Arc::from("trades"),
                crate::operator_graph::InputFrontier {
                    watermark: Some(300),
                    idle: false,
                },
            ),
            (
                Arc::from("added_source"),
                crate::operator_graph::InputFrontier {
                    watermark: Some(300),
                    idle: false,
                },
            ),
        ]));
    let output = image
        .graph
        .execute_cycle(&rustc_hash::FxHashMap::default(), 300, None)
        .await
        .unwrap();
    let temporal = &output["new_temporal"];
    let mut matches = Vec::new();
    for batch in temporal {
        let right = batch
            .column_by_name("right_value")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        matches.extend(right.iter());
    }
    matches.sort_unstable();
    assert_eq!(matches, [None, None, None, Some(7), Some(7), Some(7)]);
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_stateful_target_missing_frames_fail_closed_instead_of_reinitializing() {
    let (fixture, operation, _, _) =
        Box::pin(checkpointed_with_additions(false, stateful_additions())).await;
    let image = fixture
        .db
        .prepare_cluster_topology_recovery(operation)
        .await
        .unwrap();
    let store =
        ObjectStoreCheckpointStore::new(Arc::clone(&fixture.authority.checkpoint_store), "")
            .with_participant_id(1)
            .with_key_group_count(fixture.db.checkpoint_key_groups());
    let reader = crate::recovery_manager::RecoveryManager::new(
        &store,
        &image.input.descriptor().target_pipeline,
        &image.input.descriptor().deployment_id,
        laminar_core::checkpoint::CheckpointScope::Cluster,
    );
    let mut recovered = reader
        .recover_topology_selection(image.recovery_input().unwrap(), 1024 * 1024)
        .await
        .unwrap();
    let original = recovered.state_frames.clone();
    for (name, whole) in [
        ("new_total", false),
        ("new_tumble", false),
        ("new_join", false),
        ("new_temporal", false),
        ("new_total", true),
        ("totals", false),
    ] {
        recovered.state_frames.clone_from(&original);
        let removed = recovered
            .state_frames
            .iter()
            .position(|frame| match &frame.key {
                StateFrameKey::OperatorWhole { operator_id } => {
                    whole && operator_id == &format!("graph:{name}")
                }
                StateFrameKey::Vnode { operator_id, .. } => {
                    !whole && operator_id == &format!("graph:{name}")
                }
            })
            .unwrap();
        recovered.state_frames.remove(removed);
        let sender = fixture.db.shuffle_sender.lock().clone().unwrap();
        let scope = crate::operator::sql_query::ClusterShuffleConfig {
            registry: fixture.db.vnode_registry.lock().clone().unwrap(),
            topology: sender.topology_fence(),
            sender,
            receiver: fixture.db.shuffle_receiver.lock().clone().unwrap(),
            self_id: NodeId(1),
        };
        let (_, graph) = image
            .candidate
            .compile_topology_restore_graph(&image.input, scope)
            .await
            .unwrap();
        let error = graph
            .restore_topology_state_frames(&recovered, &image.input)
            .err()
            .unwrap();
        assert!(
            error.to_string().contains(if whole {
                "channel/frontier state"
            } else {
                "vnode roster"
            }),
            "{error}"
        );
    }
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_stateful_root_recovers_under_a_new_assignment_incarnation() {
    let (fixture, committed) = committed_fixture_with_additions(stateful_additions()).await;
    advance_decode_assignment(&fixture, committed.operation_id).await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_recovery(committed.operation_id)
        .await
        .unwrap();
    // Recovery assertions require complete cycle output, independent of wall-clock budget.
    image.graph.set_query_budget_ns(u64::MAX);
    assert_eq!(
        image.recovery_input().unwrap().cut(),
        TopologyRecoveryCut::MigrationRoot
    );
    assert_eq!(image.input.assignment().assignment_version, 2);
    let output = image
        .graph
        .execute_cycle(&future_input(5, 3, 200_000), i64::MIN, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 45);
    assert_eq!(total(&output["new_total"]), 15);
    assert_eq!(total(&output["new_global"]), 15);
    fixture.db.shutdown().await.unwrap();
}
