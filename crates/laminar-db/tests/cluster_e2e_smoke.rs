//! Happy-path cluster smoke: 2 nodes, row-shuffle, MV, 2PC checkpoint.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::collections::HashSet;
use std::time::Duration;

use tokio::time::sleep;

#[path = "common/cluster_harness.rs"]
mod cluster_harness;

use cluster_harness::{
    input_batch, manifest_epoch, pick_keys_per_owner, read_mv_sums, ClusterEngineHarness,
};

const VNODE_COUNT: u32 = 4;
const N_NODES: usize = 2;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_path_eight_keys_correct_sums() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    let leader_idx = harness.leader_idx();
    let follower_idx = harness.follower_idxs()[0];

    // Pick keys whose hashes split across both owners so the test
    // actually exercises the cross-node shuffle.
    let key_buckets = {
        let leader = &harness.nodes[leader_idx];
        let follower = &harness.nodes[follower_idx];
        let owners = vec![
            (leader.instance_id, leader.owned_vnodes()),
            (follower.instance_id, follower.owned_vnodes()),
        ];
        pick_keys_per_owner(VNODE_COUNT, &owners, 4)
            .expect("pick_keys_per_owner: search range too small")
    };
    let all_keys: Vec<i64> = key_buckets
        .iter()
        .flat_map(|(_, ks)| ks.iter().copied())
        .collect();
    assert_eq!(all_keys.len(), 8, "want 4 keys per owner");

    for node in &harness.nodes {
        node.db
            .execute("CREATE SOURCE src (key BIGINT, value BIGINT)")
            .await
            .expect("CREATE SOURCE");
        node.db
            .execute(
                "CREATE MATERIALIZED VIEW sums AS \
                 SELECT key, SUM(value) AS total FROM src GROUP BY key",
            )
            .await
            .expect("CREATE MATERIALIZED VIEW");
    }
    harness.start_all().await;

    let leader_node = &harness.nodes[leader_idx];
    let follower_node = &harness.nodes[follower_idx];
    let src = leader_node
        .db
        .source_untyped("src")
        .expect("source_untyped on leader");
    src.push_arrow(input_batch(&all_keys)).expect("push_arrow");

    // Manual checkpoint acts as a synchronization barrier — returns
    // once 2PC commits, which requires the follower has processed the
    // post-shuffle data.
    let result = leader_node
        .db
        .checkpoint()
        .await
        .expect("leader checkpoint");
    assert!(
        result.success,
        "leader checkpoint failed: {:?}",
        result.error,
    );

    // The MV emit lags one coordinator step behind FinalAggregate —
    // short settle window covers that gap.
    sleep(Duration::from_millis(500)).await;

    let leader_rows = read_mv_sums(&leader_node.db, "sums").await;
    let follower_rows = read_mv_sums(&follower_node.db, "sums").await;

    // Follower row count proves shuffle ran; key sets are disjoint;
    // union matches input with total = key * 10.
    assert!(
        !follower_rows.is_empty(),
        "follower MV is empty — shuffle didn't deliver any partials. \
         leader_rows={leader_rows:?}",
    );

    let leader_keys: HashSet<i64> = leader_rows.iter().map(|(k, _)| *k).collect();
    let follower_keys: HashSet<i64> = follower_rows.iter().map(|(k, _)| *k).collect();
    assert!(
        leader_keys.is_disjoint(&follower_keys),
        "key appears on both nodes: leader={leader_keys:?} follower={follower_keys:?}",
    );

    let mut union: Vec<(i64, i64)> = leader_rows
        .iter()
        .chain(follower_rows.iter())
        .copied()
        .collect();
    union.sort_by_key(|(k, _)| *k);

    let mut expected: Vec<(i64, i64)> = all_keys.iter().map(|&k| (k, k * 10)).collect();
    expected.sort_by_key(|(k, _)| *k);

    assert_eq!(
        union, expected,
        "union of MVs does not match input:\n got  {union:?}\n want {expected:?}",
    );

    // Both nodes' manifest epoch should track the committed epoch ±1;
    // wider drift signals a 2PC-mirror miss.
    let leader_epoch = manifest_epoch(&leader_node.db).await;
    let follower_epoch = manifest_epoch(&follower_node.db).await;
    assert!(
        leader_epoch.abs_diff(follower_epoch) <= 1,
        "manifest epoch drift > 1: leader={leader_epoch} follower={follower_epoch}",
    );

    harness.shutdown().await;
}
