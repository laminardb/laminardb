use super::*;
use laminar_core::checkpoint::{CheckpointAttempt, CheckpointBarrier};

#[tokio::test]
async fn drained_restore_rebroadcasts_frontiers_and_retains_timer_and_activation_progress() {
    let pair = Pair::new().await;
    let fixture = &pair.nodes[0];
    let key = key_for(0);
    let mut original = pair.operator(0);
    original
        .process_with_frontiers(&[vec![input_batch(&[(&key, 7, 100_000)])]], &frontier(100))
        .await
        .unwrap();
    drain_local(&mut original, 100).await;
    original
        .stage_checkpointed_shuffle_frontier("activity", 8, frontier(100)[0], 7, 3)
        .unwrap();
    drain_local(&mut original, 100).await;
    let before = state_image(&original);
    let next_activation = original.next_activation_id;
    let whole = original.checkpoint().unwrap().unwrap();
    let frames = original
        .checkpoint_vnodes(&[0, 2], 4, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|frame| {
            (
                frame.vnode,
                frame.state.unwrap().materialize(&mut 0, u64::MAX).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    for node in &pair.nodes {
        node.scope.sender.set_recovery_gen(4);
        node.scope.receiver.set_recovery_gen(4);
    }
    let mut restored =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    restored
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .unwrap();
    restored.restore(whole).unwrap();
    for (vnode, frame) in frames {
        restored.restore_vnode(vnode, 4, &frame).unwrap();
    }
    fixture.bind_operator(&mut restored);
    assert_eq!(state_image(&restored), before);
    assert_eq!(restored.next_activation_id, next_activation);
    assert!(
        !restored.wants_input(),
        "the restored local frontier must be broadcast first"
    );
    assert_eq!(
        restored.restored_output_frontier().unwrap().watermark,
        Some(100)
    );
    restored
        .process_with_frontiers(&[], &frontier(90))
        .await
        .unwrap();
    drain_local(&mut restored, 90).await;
    assert!(restored.wants_input());
    restored
        .stage_checkpointed_shuffle_frontier("activity", 8, frontier(90)[0], 7, 4)
        .unwrap();
    restored
        .stage_checkpointed_shuffle(
            "activity",
            RetainedBatch::restored_channel(
                input_batch(&[(&key, 2, 100_000)]),
                8,
                7,
                4,
                Arc::from([0]),
            ),
            100,
        )
        .unwrap();
    let output = drain_local(&mut restored, 100).await;
    assert_eq!(activity_rows(&output)[0].2, 9);
    assert_eq!(restored.next_activation_id, next_activation + 1);
    restored
        .stage_checkpointed_shuffle_frontier("activity", 8, frontier(120)[0], 7, 4)
        .unwrap();
    drain_local(&mut restored, 120).await;
    let mut output = restored
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap();
    output.extend(drain_local(&mut restored, 120).await);
    assert_eq!(activity_rows(&output)[0].1, "inactive");
    assert_eq!(activity_rows(&output)[0].2, 9);
    assert_eq!(restored.timer_count, 0);
    assert_eq!(restored.watermark_us, 120_000);
}

#[tokio::test]
async fn restore_rejects_inconsistent_frontiers_and_a_different_assignment() {
    let pair = Pair::new().await;
    let original = pair.operator(0).checkpoint().unwrap().unwrap().data;
    let metadata: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let mut local =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    let error = local
        .restore(OperatorCheckpoint { data: original })
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("requires cluster execution selection"));
    assert!(!local.metadata_restored);
    assert_eq!(local.next_activation_id, 0);
    for (field, value) in [
        ("version", serde_json::json!(2)),
        ("peers", serde_json::json!([])),
        (
            "peers",
            serde_json::json!([[7, {"watermark":null,"idle":false}]]),
        ),
        (
            "peers",
            serde_json::json!([[8, {"watermark":null,"idle":false}], [8, {"watermark":null,"idle":false}]]),
        ),
        (
            "effective",
            serde_json::json!({"watermark":100,"idle":false}),
        ),
        (
            "local",
            serde_json::json!({"watermark":i64::MIN,"idle":false}),
        ),
    ] {
        let mut invalid = metadata.clone();
        invalid["shuffle"][field] = value;
        let mut candidate =
            ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
        assert!(
            candidate
                .restore(OperatorCheckpoint {
                    data: serde_json::to_vec(&invalid).unwrap()
                })
                .is_err(),
            "{field}"
        );
        assert_eq!(candidate.next_activation_id, 0);
        assert!(!candidate.metadata_restored);
    }
    let mut invalid = metadata;
    invalid["shuffle"]["assignment_version"] = serde_json::json!(8);
    let mut candidate =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    candidate
        .require_cluster_execution("activity", tokio::runtime::Handle::current(), NodeId(7))
        .unwrap();
    candidate
        .restore(OperatorCheckpoint {
            data: serde_json::to_vec(&invalid).unwrap(),
        })
        .unwrap();
    candidate
        .bind_startup_assignment(pair.nodes[0].binding.assignment(), &[0, 2])
        .unwrap();
    assert!(candidate
        .bind_process_execution_authority(
            &pair.nodes[0].scope,
            pair.nodes[0].controller.process_lease_deadline().unwrap()
        )
        .is_err());
    assert!(candidate
        .process_with_frontiers(&[vec![input_batch(&[("a", 1, 100_000)])]], &frontier(100))
        .await
        .unwrap_err()
        .is_shuffle_not_ready());
}

#[tokio::test]
async fn graph_barrier_alignment_retains_pre_cut_input_until_the_existing_drain_applies_it() {
    let pair = Pair::new().await;
    let mut graph = pair.graph(0);
    let peer = &pair.nodes[1];
    let key = key_for(0);
    let attempt = CheckpointAttempt::new(70, 70);
    peer.scope
        .sender
        .send_to(
            7,
            &ShuffleMessage::checkpointed("activity".into(), 0, input_batch(&[(&key, 7, 100_000)])),
        )
        .await
        .unwrap();
    peer.scope
        .sender
        .send_to(7, &peer_frontier(Some(100), false))
        .await
        .unwrap();
    peer.scope
        .sender
        .fan_out_barrier(
            &[7],
            CheckpointBarrier::new(70, 70),
            peer.binding.assignment(),
        )
        .await
        .unwrap();
    graph
        .align_shuffle_barriers(
            attempt,
            100,
            pair.nodes[0].binding.assignment(),
            tokio::time::Instant::now() + DEADLINE,
            None,
        )
        .await
        .unwrap();
    assert!(!graph.checkpoint_is_quiescent());
    assert!(graph.capture_state(u64::MAX).is_err());
    let frozen =
        rustc_hash::FxHashMap::from_iter([(Arc::from("events"), InputFrontier::default())]);
    let mut output = Vec::new();
    tokio::time::timeout(DEADLINE, async {
        while !graph.checkpoint_is_quiescent() {
            let emitted = graph
                .execute_checkpoint_drain_cycle(100, Some(&frozen))
                .await
                .unwrap();
            if let Some(batches) = emitted.get("activity") {
                output.extend(batches.iter().cloned());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(activity_rows(&output)[0].2, 7);
    let (_, vnodes) = materialize(graph.capture_state(u64::MAX).unwrap());
    let bytes = &vnodes.iter().find(|(_, vnode, _)| *vnode == 0).unwrap().2;
    let vnode: crate::process_function::operator::VnodeFrame =
        serde_json::from_slice(bytes).unwrap();
    assert_eq!(
        vnode.entries[0].1.value,
        crate::process_function::ValueState::Value(7)
    );
    assert_eq!(vnode.entries[0].1.timers.len(), 1);
}
