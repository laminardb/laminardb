//! Two `ClusterRepartitionExec`s on loopback TCP: injecting a barrier
//! on both sides aligns both trackers and fires both
//! `aligned_epoch_watch` channels.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use laminar_core::checkpoint::barrier::{flags, CheckpointBarrier};
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{NodeId, VnodeRegistry};
use laminar_sql::datafusion::cluster_repartition::ClusterRepartitionExec;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn ctx() -> Arc<TaskContext> {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .build();
    Arc::new(TaskContext::from(&state))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn barrier_fans_out_and_both_nodes_align() {
    // 4 vnodes: node 1 owns {0, 2}, node 2 owns {1, 3}. Manual
    // assignment (not round-robin) makes the test's expected state
    // explicit.
    let registry = Arc::new(VnodeRegistry::new(4));
    registry.set_assignment(vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)].into());

    // Chicken-and-egg: each node needs the other's bound address to
    // register its sender. We bind B's receiver first, then build A
    // pointing at B's address, then reach into A's receiver for the
    // reverse direction.
    let recv_b = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_b = recv_b.local_addr();

    let recv_a = ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_a = recv_a.local_addr();

    let sender_a = ShuffleSender::new(1);
    sender_a.register_peer(2, addr_b).await;
    let sender_b = ShuffleSender::new(2);
    sender_b.register_peer(1, addr_a).await;

    let input_a: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap();
    let input_b: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap();

    let exec_a = Arc::new(
        ClusterRepartitionExec::try_new(
            input_a,
            vec![0],
            Arc::clone(&registry),
            Arc::new(sender_a),
            Arc::new(recv_a),
            NodeId(1),
        )
        .unwrap(),
    );
    let exec_b = Arc::new(
        ClusterRepartitionExec::try_new(
            input_b,
            vec![0],
            Arc::clone(&registry),
            Arc::new(sender_b),
            Arc::new(recv_b),
            NodeId(2),
        )
        .unwrap(),
    );
    // Silence warnings about unused struct field `addr_a` — we needed
    // to publish it via `recv_a` even though the variable isn't reused.
    let _ = (&exec_a, &exec_b, addr_a);

    // Start both nodes so the router + dispatcher tasks spawn.
    for p in 0..exec_a.properties().partitioning.partition_count() {
        let _ = exec_a.execute(p, ctx()).unwrap();
    }
    for p in 0..exec_b.properties().partitioning.partition_count() {
        let _ = exec_b.execute(p, ctx()).unwrap();
    }

    // Pre-execute check: aligned watches are now available.
    let mut watch_a = exec_a.aligned_epoch_watch().expect("runtime started");
    let mut watch_b = exec_b.aligned_epoch_watch().expect("runtime started");
    assert_eq!(*watch_a.borrow_and_update(), 0);
    assert_eq!(*watch_b.borrow_and_update(), 0);

    let barrier = CheckpointBarrier {
        checkpoint_id: 7,
        epoch: 7,
        flags: flags::FULL_SNAPSHOT,
    };

    // Inject on A: fans out to B; A's local port observes.
    exec_a.inject_barrier(barrier).unwrap();
    // Inject on B too: B's local port observes; simultaneously B's
    // router has now fanned back to A, and A's dispatcher will feed
    // A's peer port.
    exec_b.inject_barrier(barrier).unwrap();

    // Both nodes should align within a generous deadline (gossip over
    // loopback is near-instant).
    let deadline = Duration::from_secs(5);
    let a_aligned = tokio::time::timeout(deadline, watch_a.changed()).await;
    let b_aligned = tokio::time::timeout(deadline, watch_b.changed()).await;
    assert!(a_aligned.is_ok(), "A did not align within deadline");
    assert!(b_aligned.is_ok(), "B did not align within deadline");
    assert_eq!(*watch_a.borrow(), 7);
    assert_eq!(*watch_b.borrow(), 7);
}
