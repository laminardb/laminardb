use super::*;
use crate::recovery_manager::{ClusterRecoveryTarget, RecoveredState};

mod node_loss;
mod ordering;
#[cfg(not(target_arch = "wasm32"))]
mod peers;
#[cfg(all(feature = "process-remote", target_os = "linux"))]
mod python;
#[cfg(feature = "process-remote")]
mod remote;
mod replay;
mod store;
mod transfer;

use store::SharedCut;

fn source_batch(rows: &[(&str, i64, i64)]) -> rustc_hash::FxHashMap<Arc<str>, Vec<RecordBatch>> {
    rustc_hash::FxHashMap::from_iter([(Arc::from("events"), vec![input_batch(rows)])])
}

async fn progress(graphs: &mut [OperatorGraph], watermark: i64) -> Vec<Vec<RecordBatch>> {
    tokio::time::timeout(DEADLINE, async {
        let mut output = vec![Vec::new(); graphs.len()];
        // A graph may be locally quiescent before its peer's send is scheduled. Require the
        // effective output cut as well as drained work before capturing any participant.
        loop {
            for (index, graph) in graphs.iter_mut().enumerate() {
                let emitted = graph
                    .execute_cycle(&rustc_hash::FxHashMap::default(), watermark, None)
                    .await
                    .unwrap();
                if let Some(batches) = emitted.get("activity") {
                    output[index].extend(batches.iter().cloned());
                }
            }
            if graphs.iter_mut().all(|graph| {
                if !graph.checkpoint_is_quiescent() {
                    return false;
                }
                let (whole, _) = materialize(graph.capture_state(u64::MAX).unwrap());
                let metadata: serde_json::Value = serde_json::from_slice(&whole[0].1).unwrap();
                metadata["watermark_us"] == watermark.saturating_mul(1_000)
            }) {
                return output;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

async fn populated(
    pair: &Pair,
    binding: ProcessFunctionDescriptor,
    handler: ProcessHandler,
) -> Vec<OperatorGraph> {
    let mut graphs = pair
        .nodes
        .iter()
        .map(|fixture| {
            fixture
                .graph(binding.clone(), handler.clone())
                .bind_startup_assignment(&fixture.binding, &fixture.controller)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let keys = [key_for(0), key_for(1), key_for(2), key_for(3)];
    graphs[0]
        .execute_cycle(
            &source_batch(&[(&keys[0], 7, 100_000), (&keys[1], 11, 101_000)]),
            105,
            None,
        )
        .await
        .unwrap();
    graphs[1]
        .execute_cycle(
            &source_batch(&[(&keys[2], 13, 102_000), (&keys[3], 17, 103_000)]),
            105,
            None,
        )
        .await
        .unwrap();
    let emitted = progress(&mut graphs, 105).await;
    assert_eq!(
        emitted
            .iter()
            .flatten()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        4
    );
    graphs
}

fn target_fence(version: u64, owners: [u64; 4]) -> CheckpointAssignmentFence {
    let participants = owners
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|node| CheckpointParticipant {
            node_id: node,
            boot_incarnation: Uuid::from_u128(u128::from(node)),
        })
        .collect();
    CheckpointAssignmentFence::from_owner_map(version, &owners, participants).unwrap()
}

async fn target_nodes(
    pair: &Pair,
    fence: &CheckpointAssignmentFence,
    owners: [u64; 4],
) -> Vec<Fixture> {
    let mut nodes = Vec::new();
    for participant in &fence.participants {
        nodes.push(
            Fixture::for_assignment(
                Arc::clone(&pair.nodes[0].authority),
                NodeId(participant.node_id),
                fence.clone(),
                owners,
                4,
                Duration::from_secs(60),
            )
            .await,
        );
    }
    for sender in &nodes {
        for receiver in &nodes {
            if sender.scope.self_id != receiver.scope.self_id {
                sender.scope.sender.register_peer(
                    receiver.scope.self_id.0,
                    receiver.scope.receiver.local_addr(),
                );
            }
        }
    }
    nodes
}

fn restore_graph(
    fixture: &Fixture,
    recovered: &RecoveredState,
    binding: ProcessFunctionDescriptor,
    handler: ProcessHandler,
) -> Result<OperatorGraph, DbError> {
    let graph = fixture.graph(binding, handler);
    let graph = if recovered.reassigned {
        graph
            .restore_reassigned_vnode_state(
                recovered.committed.assignment_fence.as_ref().unwrap(),
                &recovered.predecessor_owners,
                fixture.binding.assignment(),
                &recovered.state_frames,
            )?
            .0
    } else {
        let mut whole = Vec::new();
        let mut vnodes = Vec::new();
        for frame in &recovered.state_frames {
            match &frame.key {
                laminar_core::checkpoint::StateFrameKey::OperatorWhole { operator_id } => {
                    whole.push((
                        operator_id.strip_prefix("graph:").unwrap().to_owned(),
                        frame.payload.clone(),
                    ));
                }
                laminar_core::checkpoint::StateFrameKey::Vnode { operator_id, vnode } => {
                    vnodes.push((
                        operator_id.strip_prefix("graph:").unwrap().to_owned(),
                        u32::from(*vnode),
                        frame.payload.clone(),
                    ));
                }
            }
        }
        graph.restore_state_frames(&whole, &vnodes, 4)?.0
    };
    graph.bind_startup_assignment(&fixture.binding, &fixture.controller)
}
