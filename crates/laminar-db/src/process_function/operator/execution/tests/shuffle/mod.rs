use super::*;
use crate::operator::RetainedBatch;
use laminar_core::shuffle::{ReceivedShuffle, ShuffleMessage};

const DEADLINE: Duration = Duration::from_secs(5);

mod committed;

struct Pair {
    nodes: [Fixture; 2],
    objects: Arc<dyn object_store::ObjectStore>,
}

impl Pair {
    async fn new() -> Self {
        let objects: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let ttl = Duration::from_secs(60);
        let authority = Arc::new(ProcessLeaseAuthority::new(Arc::clone(&objects), ttl).unwrap());
        let owners = [7, 8, 7, 8];
        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &owners,
            vec![
                CheckpointParticipant {
                    node_id: 7,
                    boot_incarnation: Uuid::from_u128(7),
                },
                CheckpointParticipant {
                    node_id: 8,
                    boot_incarnation: Uuid::from_u128(8),
                },
            ],
        )
        .unwrap();
        let first = Fixture::for_assignment(
            Arc::clone(&authority),
            NodeId(7),
            fence.clone(),
            owners,
            3,
            ttl,
        )
        .await;
        let second = Fixture::for_assignment(authority, NodeId(8), fence, owners, 3, ttl).await;
        first
            .scope
            .sender
            .register_peer(8, second.scope.receiver.local_addr());
        second
            .scope
            .sender
            .register_peer(7, first.scope.receiver.local_addr());
        Self {
            nodes: [first, second],
            objects,
        }
    }

    fn operator(&self, node: usize) -> ProcessFunctionOperator {
        let mut operator =
            ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
        self.nodes[node].bind_operator(&mut operator);
        operator
    }

    fn graph(&self, node: usize) -> OperatorGraph {
        let fixture = &self.nodes[node];
        fixture
            .graph(
                descriptor(),
                ProcessHandler::Native(Arc::new(AccountActivity)),
            )
            .bind_startup_assignment(&fixture.binding, &fixture.controller)
            .unwrap()
    }

    async fn ship(&self, to: usize, message: ShuffleMessage) -> ReceivedShuffle {
        let peer = self.nodes[to].scope.self_id.0;
        tokio::time::timeout(
            DEADLINE,
            self.nodes[1 - to].scope.sender.send_to(peer, &message),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(DEADLINE, self.nodes[to].scope.receiver.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

fn key_for(vnode: u32) -> String {
    (0..1_000)
        .map(|index| format!("key-{index}"))
        .find(|key| {
            laminar_core::shuffle::row_vnodes(&input_batch(&[(key, 1, 100_000)]), &[0], 4).unwrap()
                [0]
                == vnode
        })
        .unwrap()
}

fn stage(operator: &mut ProcessFunctionOperator, received: ReceivedShuffle) {
    let peer = received.peer();
    let assignment = received.assignment_version();
    let recovery = received.recovery_gen();
    let (message, admission) = received.into_parts();
    match message {
        ShuffleMessage::Data {
            stage,
            routed_vnodes,
            batch,
        } => operator
            .stage_checkpointed_shuffle(
                &stage,
                RetainedBatch::admitted(
                    batch,
                    admission,
                    peer,
                    assignment,
                    recovery,
                    routed_vnodes,
                ),
                i64::MIN,
            )
            .unwrap(),
        ShuffleMessage::Frontier {
            stage,
            watermark,
            idle,
        } => operator
            .stage_checkpointed_shuffle_frontier(
                &stage,
                peer,
                InputFrontier { watermark, idle },
                assignment,
                recovery,
            )
            .unwrap(),
        ShuffleMessage::Barrier(_) => panic!("unexpected barrier in process input fixture"),
    }
}

fn peer_frontier(watermark: Option<i64>, idle: bool) -> ShuffleMessage {
    ShuffleMessage::Frontier {
        stage: "activity".into(),
        watermark,
        idle,
    }
}

async fn drain_local(operator: &mut ProcessFunctionOperator, watermark: i64) -> Vec<RecordBatch> {
    tokio::time::timeout(DEADLINE, async {
        let mut output = Vec::new();
        while operator.checkpoint_drain_pending() {
            output.extend(
                operator
                    .process_with_frontiers(&[], &frontier(watermark))
                    .await
                    .unwrap(),
            );
            tokio::task::yield_now().await;
        }
        output
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn graph_ships_canonical_rows_and_preserves_source_channel_key_order() {
    let pair = Pair::new().await;
    let local_key = key_for(0);
    let remote_key = key_for(1);
    let returning_key = key_for(2);
    let mut graphs = [pair.graph(0), pair.graph(1)];
    let batches = [
        input_batch(&[
            (&remote_key, 3, 100_000),
            (&local_key, 2, 100_000),
            (&remote_key, 5, 101_000),
        ]),
        input_batch(&[(&returning_key, 9, 102_000)]),
    ];
    let mut all_output = [Vec::new(), Vec::new()];
    for node in 0..2 {
        let output = graphs[node]
            .execute_cycle(
                &rustc_hash::FxHashMap::from_iter([(
                    Arc::from("events"),
                    vec![batches[node].clone()],
                )]),
                102,
                None,
            )
            .await
            .unwrap();
        assert!(output.get("activity").is_none_or(Vec::is_empty));
        assert!(!graphs[node].checkpoint_is_quiescent());
        assert!(graphs[node].capture_state(u64::MAX).is_err());
    }
    tokio::time::timeout(DEADLINE, async {
        loop {
            for node in 0..2 {
                let output = graphs[node]
                    .execute_cycle(&rustc_hash::FxHashMap::default(), 102, None)
                    .await
                    .unwrap();
                if let Some(batches) = output.get("activity") {
                    all_output[node].extend(batches.iter().cloned());
                }
            }
            if all_output
                .iter()
                .flatten()
                .map(RecordBatch::num_rows)
                .sum::<usize>()
                == 4
                && graphs.iter().all(OperatorGraph::checkpoint_is_quiescent)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let first = activity_rows(&all_output[0]);
    let second = activity_rows(&all_output[1]);
    assert!(first
        .iter()
        .all(|row| row.0 == local_key || row.0 == returning_key));
    assert_eq!(second.iter().map(|row| row.2).collect::<Vec<_>>(), [3, 8]);
    assert!(second.iter().all(|row| row.0 == remote_key));
    for (node, graph) in graphs.iter_mut().enumerate() {
        let (whole, vnodes) = materialize(graph.capture_state(u64::MAX).unwrap());
        assert_eq!(whole.len(), 1);
        assert!(vnodes
            .iter()
            .all(|(_, vnode, _)| (*vnode % 2) as usize == node));
        let metadata: serde_json::Value = serde_json::from_slice(&whole[0].1).unwrap();
        assert_eq!(metadata["shuffle"]["effective"]["watermark"], 102);
    }
}

#[tokio::test]
async fn peer_frontier_waits_for_prior_data_and_timer_outputs() {
    let pair = Pair::new().await;
    let mut operator = pair.operator(0);
    let key = key_for(0);
    operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap();
    drain_local(&mut operator, 120).await;
    assert_eq!(operator.output_frontier(frontier(120)[0]).watermark, None);
    let initial_charge = operator.managed_state_accounting().unwrap().live;
    let received = pair
        .ship(
            0,
            ShuffleMessage::checkpointed(
                "activity".into(),
                0,
                input_batch(&[(&key, 3, 100_000), (&key, 5, 101_000)]),
            ),
        )
        .await;
    stage(&mut operator, received);
    stage(
        &mut operator,
        pair.ship(0, peer_frontier(Some(120), false)).await,
    );
    assert!(operator.managed_state_accounting().unwrap().live > initial_charge);
    assert!(!operator.wants_input());
    assert!(operator.checkpoint().is_err());
    assert!(operator.checkpoint_vnodes(&[0], 4, u64::MAX).is_err());
    let updates = operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap();
    assert_eq!(
        activity_rows(&updates)
            .iter()
            .map(|row| row.2)
            .collect::<Vec<_>>(),
        [3, 8]
    );
    assert_eq!(operator.watermark_us, i64::MIN);
    assert_eq!(operator.output_frontier(frontier(120)[0]).watermark, None);
    let timers = operator
        .process_with_frontiers(&[], &frontier(120))
        .await
        .unwrap();
    assert_eq!(activity_rows(&timers)[0].1, "inactive");
    assert_eq!(activity_rows(&timers)[0].2, 8);
    assert_eq!(operator.watermark_us, 120_000);
    assert_eq!(operator.output_frontier(frontier(120)[0]), frontier(120)[0]);
    assert!(operator.checkpoint().is_ok());
}

#[tokio::test]
async fn peer_idleness_requires_ordered_revival_before_data() {
    let pair = Pair::new().await;
    let mut operator = pair.operator(0);
    operator
        .process_with_frontiers(&[], &frontier(100))
        .await
        .unwrap();
    drain_local(&mut operator, 100).await;
    stage(
        &mut operator,
        pair.ship(0, peer_frontier(Some(80), true)).await,
    );
    drain_local(&mut operator, 100).await;
    assert_eq!(operator.watermark_us, 100_000);
    let key = key_for(0);
    let before = state_image(&operator);
    let batch = RetainedBatch::restored_channel(
        input_batch(&[(&key, 1, 100_000)]),
        8,
        7,
        3,
        Arc::from([0]),
    );
    assert!(operator
        .stage_checkpointed_shuffle("activity", batch.clone(), 100)
        .is_err());
    assert_eq!(state_image(&operator), before);
    operator
        .stage_checkpointed_shuffle_frontier("activity", 8, frontier(90)[0], 7, 3)
        .unwrap();
    operator
        .stage_checkpointed_shuffle("activity", batch, 100)
        .unwrap();
    assert_eq!(
        operator.output_frontier(frontier(100)[0]).watermark,
        Some(99)
    );
    let output = drain_local(&mut operator, 100).await;
    assert_eq!(activity_rows(&output)[0].2, 1);
    assert_eq!(operator.watermark_us, 100_000);
    assert_eq!(operator.output_frontier(frontier(100)[0]), frontier(100)[0]);
}

mod recovery;
#[cfg(feature = "process-remote")]
mod remote;
mod validation;
