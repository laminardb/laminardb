//! Phase 0 smoke test: 2 nodes, real planner, row-shuffle pre-aggregate,
//! materialized-view aggregate, no faults.
//!
//! What this proves:
//! - The streaming aggregate path (`IncrementalAggState` + row-shuffle
//!   bridge) routes rows to the vnode owner.
//! - The MV result store reflects the post-shuffle sums.
//! - 2PC checkpoint completes end-to-end (leader Prepare → follower
//!   `follower_checkpoint` → leader Commit).

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

    let harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    let leader_idx = harness.leader_idx();
    let follower_idx = harness.follower_idxs()[0];
    let leader_node = &harness.nodes[leader_idx];
    let follower_node = &harness.nodes[follower_idx];

    // Pre-compute keys whose hashes split across both owners.
    // Without this, target_partitions=4 + 4 vnodes could let every key
    // land on the leader and the test would pass without exercising the
    // cross-node shuffle (review item H4: "gate test that hides bugs").
    let owners = vec![
        (leader_node.instance_id, leader_node.owned_vnodes()),
        (follower_node.instance_id, follower_node.owned_vnodes()),
    ];
    let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 4)
        .expect("pick_keys_per_owner: search range too small");
    let all_keys: Vec<i64> = key_buckets
        .iter()
        .flat_map(|(_, ks)| ks.iter().copied())
        .collect();
    assert_eq!(all_keys.len(), 8, "want 4 keys per owner");

    // DDL on every node so the planner has identical catalogs. DDL
    // precedes start_all() so the pipeline coordinator picks up the
    // sources and MV at startup (pipeline_lifecycle.rs:237).
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

    // Push all 8 rows to the leader. Rows hashed to follower's vnodes
    // travel via `ShuffleSender` -> follower's `ShuffleReceiver` ->
    // FinalPartitioned on the follower.
    let src = leader_node
        .db
        .source_untyped("src")
        .expect("source_untyped on leader");
    src.push_arrow(input_batch(&all_keys)).expect("push_arrow");

    // Trigger a manual checkpoint — this acts as a synchronization
    // barrier (review item H2: never poll for row count, can lock onto
    // a transient wrong state). Leader's `checkpoint()` returns once
    // 2PC commits, which requires the follower has also processed the
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

    // The barrier guarantees state-backend partials are durable, but
    // the per-cycle MV emit lags one streaming-coordinator step behind
    // the FinalAggregate output. A short settle window covers that gap.
    sleep(Duration::from_millis(500)).await;

    let leader_rows = read_mv_sums(&leader_node.db, "sums").await;
    let follower_rows = read_mv_sums(&follower_node.db, "sums").await;

    // Implementation-agnostic assertions:
    //   1. Follower has at least one row → cross-node shuffle ran.
    //   2. Keys are disjoint across nodes (vnode ownership is disjoint).
    //   3. Union exactly equals the input keys with `total = key * 10`.
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

    // Cross-node epoch sanity: both nodes' persisted manifest should be
    // at the epoch the leader's checkpoint just committed, ±1 (review
    // item H5). Wider drift signals a 2PC-mirror miss.
    let leader_epoch = manifest_epoch(&leader_node.db);
    let follower_epoch = manifest_epoch(&follower_node.db);
    assert!(
        leader_epoch.abs_diff(follower_epoch) <= 1,
        "manifest epoch drift > 1: leader={leader_epoch} follower={follower_epoch}",
    );

    harness.shutdown().await;
}
