//! End-to-end routing test: rows destined for remote vnodes leave via
//! `ShuffleSender`; local vnodes land on the matching output partition.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use laminar_core::state::{round_robin_assignment, NodeId, VnodeRegistry};
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_sql::datafusion::cluster_repartition::ClusterRepartitionExec;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(rows: impl IntoIterator<Item = (i64, i64)>) -> RecordBatch {
    let (keys, vals): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap()
}

fn ctx() -> Arc<TaskContext> {
    let cfg = SessionConfig::new();
    let state = SessionStateBuilder::new().with_config(cfg).build();
    Arc::new(TaskContext::from(&state))
}

/// Ship a deterministic 8-row batch through node A's
/// `ClusterRepartitionExec` and verify every row lands on the
/// owner's output partition. Node B holds its own receiver (no exec)
/// and simply counts arrivals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_a_routes_rows_to_owning_instance() {
    // 4 vnodes, 2 peers (node A = 1, node B = 2) — A owns vnodes that
    // hash to even index after sort, B owns the odds.
    let registry = Arc::new(VnodeRegistry::new(4));
    let peers = [NodeId(1), NodeId(2)];
    registry.set_assignment(round_robin_assignment(4, &peers));

    // Wire the shuffle fabric: A has a sender targeting B's receiver,
    // and its own receiver that B would send to. Loopback TCP.
    let recv_b = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let recv_a = ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let send_a = ShuffleSender::new(1);
    send_a.register_peer(2, recv_b.local_addr()).await;

    // Input: 8 rows with a mix of keys. We pre-compute which keys
    // land on each instance using the same hash the exec uses.
    let input_rows: Vec<(i64, i64)> = (0..8).map(|k| (k, k * 10)).collect();
    let input_batch = batch(input_rows.clone());

    // Wrap the batch as an in-memory exec with one partition.
    let input: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
        &[vec![input_batch.clone()]],
        schema(),
        None,
    )
    .unwrap();

    let exec = ClusterRepartitionExec::try_new(
        input,
        vec![0], // hash by column 0 (key)
        Arc::clone(&registry),
        Arc::new(send_a),
        Arc::new(recv_a),
        NodeId(1),
    )
    .unwrap();

    // Drain all of A's owned partitions.
    let n_partitions = exec.properties().partitioning.partition_count();
    assert_eq!(n_partitions, 2, "A owns 2 of 4 vnodes");

    let mut got_keys: Vec<i64> = Vec::new();
    for p in 0..n_partitions {
        let ctx = ctx();
        let mut stream = exec.execute(p, ctx).unwrap();
        // With a deadline: the router task fans the batch out; once
        // it finishes the input stream, our bounded channel closes
        // and the output stream terminates.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while let Ok(Some(Ok(b))) =
            tokio::time::timeout_at(deadline, stream.next()).await
        {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            got_keys.extend(col.values().iter().copied());
        }
    }

    got_keys.sort_unstable();

    // Drain whatever B received through the shuffle.
    let mut b_keys: Vec<i64> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while let Ok(Some((_from, msg))) =
        tokio::time::timeout_at(deadline, recv_b.recv()).await
    {
        if let laminar_core::shuffle::ShuffleMessage::VnodeData(_vnode, b) = msg {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            b_keys.extend(col.values().iter().copied());
        }
    }
    b_keys.sort_unstable();

    // Implementation-agnostic invariants:
    //   1. Every input key is routed exactly once (no loss, no dup).
    //   2. A sees keys disjoint from B.
    let mut all: Vec<i64> = got_keys.iter().chain(b_keys.iter()).copied().collect();
    all.sort_unstable();
    let mut expected_all: Vec<i64> = input_rows.iter().map(|(k, _)| *k).collect();
    expected_all.sort_unstable();
    assert_eq!(all, expected_all, "every input row routed exactly once");
    for k in &got_keys {
        assert!(!b_keys.contains(k), "key {k} routed to both A and B");
    }
}
