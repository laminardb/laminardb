use super::*;
use crate::vnode_transition_staging::{
    InstalledVnodeStateHandle, PendingVnodeTransition, PendingVnodeTransitionHandle,
};
use laminar_core::checkpoint::StateFrameKey;

#[tokio::test]
async fn committed_shared_cut_restores_rescaled_owners_with_state_timers_and_source_positions() {
    let pair = Pair::new().await;
    let mut original = populated(
        &pair,
        descriptor(),
        ProcessHandler::Native(Arc::new(AccountActivity)),
    )
    .await;
    let cut = SharedCut::persist(&pair, &mut original).await;
    for node in &pair.nodes {
        node.controller.fence_process_lease();
    }
    drop(original);
    let owners = [7, 9, 8, 9];
    let fence = target_fence(8, owners);
    let nodes = target_nodes(&pair, &fence, owners).await;
    cut.publish_target([7, 8, 7, 8], &fence, owners).await;
    let mut graphs = Vec::new();
    for node in &nodes {
        let recovered = cut.recover(node).await.unwrap();
        assert!(recovered.reassigned);
        assert_eq!(recovered.checkpoint_watermark(), Some(105));
        assert_eq!(
            recovered.source_offsets()["events"].offsets,
            std::collections::HashMap::from([
                ("partition-7".into(), "2".into()),
                ("partition-8".into(), "2".into())
            ])
        );
        for frame in &recovered.state_frames {
            if let StateFrameKey::Vnode { vnode, .. } = frame.key {
                assert_eq!(frame.participant_id, [7, 8, 7, 8][usize::from(vnode)]);
                assert_eq!(owners[usize::from(vnode)], node.scope.self_id.0);
            }
        }
        graphs.push(
            restore_graph(
                node,
                &recovered,
                descriptor(),
                ProcessHandler::Native(Arc::new(AccountActivity)),
            )
            .unwrap(),
        );
    }
    assert!(progress(&mut graphs, 105).await.iter().all(Vec::is_empty));
    let timers = progress(&mut graphs, 115).await;
    let mut rows = timers
        .iter()
        .flat_map(|output| activity_rows(output))
        .collect::<Vec<_>>();
    rows.sort();
    let mut expected = (0..4)
        .map(|vnode| {
            (
                key_for(vnode),
                "inactive".into(),
                [7, 11, 13, 17][vnode as usize],
                false,
                110_000 + i64::from(vnode) * 1_000,
            )
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(rows, expected);
    for (node, output) in nodes.iter().zip(&timers) {
        for row in activity_rows(output) {
            let vnode =
                laminar_core::shuffle::row_vnodes(&input_batch(&[(&row.0, 1, 120_000)]), &[0], 4)
                    .unwrap()[0];
            assert_eq!(owners[vnode as usize], node.scope.self_id.0);
        }
    }
    assert!(progress(&mut graphs, 115).await.iter().all(Vec::is_empty));
}

#[tokio::test]
async fn live_graph_transfer_publishes_committed_donor_state_and_replaces_execution_authority() {
    let pair = Pair::new().await;
    let mut graphs = populated(
        &pair,
        descriptor(),
        ProcessHandler::Native(Arc::new(AccountActivity)),
    )
    .await;
    let cut = SharedCut::persist(&pair, &mut graphs).await;
    let owners = [8, 7, 8, 7];
    let target = target_fence(8, owners);
    cut.publish_target([7, 8, 7, 8], &target, owners).await;
    let rotation = Arc::new(tokio::sync::RwLock::new(()));
    let mut installed_slots = Vec::new();
    let mut pending_slots = Vec::new();
    for (index, graph) in graphs.iter_mut().enumerate() {
        let node = &pair.nodes[index];
        let acquired: &[u32] = if index == 0 { &[1, 3] } else { &[0, 2] };
        let frames = cut.handoff(node, acquired).await;
        let pending = PendingVnodeTransition::assignment_change(
            cut.fence.clone(),
            &[NodeId(7), NodeId(8), NodeId(7), NodeId(8)],
            target.clone(),
            &owners.map(NodeId),
            CheckpointParticipant {
                node_id: node.scope.self_id.0,
                boot_incarnation: node.scope.sender.incarnation(),
            },
            PipelineIdentity::empty(),
            frames,
            None,
        )
        .unwrap();
        let installed: InstalledVnodeStateHandle =
            Arc::new(parking_lot::Mutex::new(Some(node.binding.clone())));
        let staged: PendingVnodeTransitionHandle =
            Arc::new(parking_lot::Mutex::new(Some(Arc::new(pending))));
        graph.set_installed_vnode_state_handle(Arc::clone(&installed));
        graph.set_pending_vnode_transition_handle(Arc::clone(&staged));
        graph.set_rotation_execution_fence(Arc::clone(&rotation));
        installed_slots.push(installed);
        pending_slots.push(staged);
    }
    let guard = rotation.write().await;
    for node in &pair.nodes {
        node.scope
            .registry
            .set_assignment_and_version(Arc::from(owners.map(NodeId)), 8);
        node.scope
            .sender
            .install_assignment_fence(&target, &owners)
            .unwrap();
        node.scope
            .receiver
            .install_assignment_fence(&target, &owners)
            .unwrap();
        node.scope.sender.set_recovery_gen(4);
        node.scope.receiver.set_recovery_gen(4);
    }
    drop(guard);
    assert!(progress(&mut graphs, 105).await.iter().all(Vec::is_empty));
    for (index, graph) in graphs.iter_mut().enumerate() {
        assert!(pending_slots[index].lock().is_none());
        assert!(installed_slots[index]
            .lock()
            .as_ref()
            .unwrap()
            .matches(&target, &PipelineIdentity::empty()));
        let (whole, vnodes) = materialize(graph.capture_state(1024 * 1024).unwrap());
        assert!(vnodes
            .iter()
            .all(|(_, vnode, _)| owners[*vnode as usize] == pair.nodes[index].scope.self_id.0));
        let metadata: serde_json::Value = serde_json::from_slice(&whole[0].1).unwrap();
        assert_eq!(metadata["shuffle"]["assignment_version"], 8);
        assert_eq!(metadata["next_activation_id"], 2);
        assert_eq!(metadata["next_timer_generation"], 2);
    }
    let timers = progress(&mut graphs, 115).await;
    let mut totals = timers
        .iter()
        .flat_map(|output| activity_rows(output))
        .map(|row| row.2)
        .collect::<Vec<_>>();
    totals.sort_unstable();
    assert_eq!(totals, [7, 11, 13, 17]);
    let keys = [key_for(0), key_for(1), key_for(2), key_for(3)];
    graphs[0]
        .execute_cycle(
            &source_batch(
                &keys
                    .iter()
                    .map(|key| (key.as_str(), 1, 120_000))
                    .collect::<Vec<_>>(),
            ),
            120,
            None,
        )
        .await
        .unwrap();
    let output = progress(&mut graphs, 120).await;
    let mut totals = output
        .iter()
        .flat_map(|output| activity_rows(output))
        .map(|row| row.2)
        .collect::<Vec<_>>();
    totals.sort_unstable();
    assert_eq!(totals, [8, 12, 14, 18]);
}

#[tokio::test]
async fn committed_restore_rejects_missing_wrong_and_inconsistent_donor_metadata() {
    let pair = Pair::new().await;
    let mut original = populated(
        &pair,
        descriptor(),
        ProcessHandler::Native(Arc::new(AccountActivity)),
    )
    .await;
    let cut = SharedCut::persist(&pair, &mut original).await;
    for node in &pair.nodes {
        node.controller.fence_process_lease();
    }
    let owners = [9; 4];
    let target = target_fence(8, owners);
    let nodes = target_nodes(&pair, &target, owners).await;
    cut.publish_target([7, 8, 7, 8], &target, owners).await;
    for case in 0..6 {
        let mut recovered = cut.recover(&nodes[0]).await.unwrap();
        let position = recovered
            .state_frames
            .iter()
            .position(|frame| {
                frame.participant_id == 8
                    && matches!(frame.key, StateFrameKey::OperatorWhole { .. })
            })
            .unwrap();
        if case == 0 {
            recovered.state_frames.remove(position);
        } else {
            let frame = &mut recovered.state_frames[position];
            let mut metadata: serde_json::Value = serde_json::from_slice(&frame.payload).unwrap();
            match case {
                1 => metadata["shuffle"]["assignment_version"] = serde_json::json!(8),
                2 => metadata["shuffle"]["self_id"] = serde_json::json!(7),
                3 => metadata["shuffle"]["peers"][0][0] = serde_json::json!(9),
                4 => {
                    metadata["shuffle"]["local"]["idle"] = serde_json::json!(true);
                    metadata["shuffle"]["peers"][0][1]["idle"] = serde_json::json!(true);
                    metadata["shuffle"]["effective"]["idle"] = serde_json::json!(true);
                }
                5 => metadata["shuffle"] = serde_json::Value::Null,
                _ => unreachable!(),
            }
            frame.payload = bytes::Bytes::from(serde_json::to_vec(&metadata).unwrap());
        }
        assert!(
            restore_graph(
                &nodes[0],
                &recovered,
                descriptor(),
                ProcessHandler::Native(Arc::new(AccountActivity))
            )
            .is_err(),
            "case {case}"
        );
    }
    let recovered = cut.recover(&nodes[0]).await.unwrap();
    let mut graph = restore_graph(
        &nodes[0],
        &recovered,
        descriptor(),
        ProcessHandler::Native(Arc::new(AccountActivity)),
    )
    .unwrap();
    let output = graph
        .execute_cycle(&rustc_hash::FxHashMap::default(), 115, None)
        .await
        .unwrap();
    let mut totals = activity_rows(&output["activity"])
        .into_iter()
        .map(|row| row.2)
        .collect::<Vec<_>>();
    totals.sort_unstable();
    assert_eq!(totals, [7, 11, 13, 17]);
}
