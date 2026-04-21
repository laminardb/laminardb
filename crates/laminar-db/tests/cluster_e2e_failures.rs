//! Cluster failure-mode acceptance tests. Scenarios are feature-gated
//! per phase so CI flips assertions when the matching fix lands.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::collections::HashSet;
use std::time::Duration;

use tokio::time::sleep;

#[path = "common/cluster_harness.rs"]
mod cluster_harness;

use cluster_harness::{input_batch, pick_keys_per_owner, read_mv_sums, ClusterEngineHarness};
// `manifest_epoch` is only used under `phase-1-recovery`; pulling it in
// unconditionally trips `unused_imports` in the default build.
#[cfg(feature = "phase-1-recovery")]
use cluster_harness::manifest_epoch;

const VNODE_COUNT: u32 = 4;
const N_NODES: usize = 2;

/// Two backends over the same shared-state dir; the stale writer's
/// `write_partial` at an older assignment version must be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_brain_write_partial_rejected() {
    use bytes::Bytes;
    use laminar_core::state::{ObjectStoreBackend, StateBackend, StateBackendError};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;

    let dir = tempfile::tempdir().expect("tempdir");
    let store: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"));

    let fresh = ObjectStoreBackend::new(std::sync::Arc::clone(&store), "leader", 4);
    let stale = ObjectStoreBackend::new(std::sync::Arc::clone(&store), "ex-leader", 4);

    // Fresh side has adopted snapshot version 3 (simulates a new
    // assignment generation rolling through via AssignmentSnapshotStore).
    fresh.set_authoritative_version(3);
    fresh
        .write_partial(0, 1, 3, Bytes::from_static(b"fresh"))
        .await
        .expect("fresh write at current version");

    // Stale side learned about the new generation too (e.g. via its
    // own harness loading the same snapshot on reconnect), but a
    // lingering writer from its in-flight pipeline is still stamping
    // writes with caller=2 — that's the split-brain footprint.
    stale.set_authoritative_version(3);
    let err = stale
        .write_partial(0, 1, 2, Bytes::from_static(b"stale"))
        .await
        .expect_err("stale write must be rejected");
    match err {
        StateBackendError::StaleVersion {
            caller,
            authoritative,
        } => {
            assert_eq!(caller, 2);
            assert_eq!(authoritative, 3);
        }
        other => panic!("expected StaleVersion, got {other:?}"),
    }

    let got = fresh.read_partial(0, 1).await.unwrap().unwrap();
    assert_eq!(&got[..], b"fresh");
}

/// Post-restart cluster must adopt the stored assignment instead of
/// recomputing from its (possibly partitioned) peer view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assignment_snapshot_unifies_cluster_view() {
    let harness_a = cluster_harness::ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    let assignment_a: Vec<cluster_harness::NodeIdView> = harness_a
        .nodes
        .iter()
        .map(|n| (n.instance_id, n.owned_vnodes()))
        .collect();
    let (shared_dir, _cp_dirs) = harness_a.shutdown_keep_dirs().await;

    let cp_dirs2: Vec<_> = (0..N_NODES).map(|_| tempfile::tempdir().unwrap()).collect();
    let harness_b = cluster_harness::ClusterEngineHarness::spawn_with_dirs(
        N_NODES,
        VNODE_COUNT,
        shared_dir,
        cp_dirs2,
    )
    .await;
    let assignment_b: Vec<cluster_harness::NodeIdView> = harness_b
        .nodes
        .iter()
        .map(|n| (n.instance_id, n.owned_vnodes()))
        .collect();

    assert_eq!(assignment_a, assignment_b);
    harness_b.shutdown().await;
}

/// A successful leader checkpoint must leave a durable commit marker
/// for the epoch. Without it, a new leader elected mid-2PC defaults
/// to Abort and splits state against followers that already
/// committed on the old leader's Commit announcement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_records_durable_commit_marker() {
    let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    for node in &harness.nodes {
        setup_query(&node.db).await;
    }
    harness.start_all().await;

    let leader = &harness.nodes[harness.leader_idx()];
    let result = leader.db.checkpoint().await.expect("leader checkpoint");
    assert!(result.success, "leader checkpoint: {:?}", result.error);

    assert!(
        leader
            .decision_store
            .is_committed(result.epoch)
            .await
            .expect("marker read"),
        "commit marker must exist for the just-completed epoch",
    );
    // The store is cluster-shared: every follower sees the same marker.
    for idx in harness.follower_idxs() {
        assert!(harness.nodes[idx]
            .decision_store
            .is_committed(result.epoch)
            .await
            .unwrap());
    }

    harness.shutdown().await;
}

/// Boot path lifts the registry's snapshot version into the backend
/// fence so the Phase 1.4 guard fires in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_wires_backend_fence_from_snapshot() {
    let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

    for node in &harness.nodes {
        assert_eq!(node.state_backend.authoritative_version(), 0);
    }

    // DDL is required so `start()` takes the branch that installs
    // the fence; the embedded branch returns before touching it.
    for node in &harness.nodes {
        setup_query(&node.db).await;
    }
    harness.start_all().await;

    for node in &harness.nodes {
        let registry_v = node.vnode_registry.assignment_version();
        let backend_v = node.state_backend.authoritative_version();
        assert!(registry_v > 0);
        assert_eq!(backend_v, registry_v);
    }

    use bytes::Bytes;
    use laminar_core::state::StateBackendError;
    let node = &harness.nodes[0];
    let authoritative = node.state_backend.authoritative_version();
    let stale_caller = authoritative - 1;
    let err = node
        .state_backend
        .write_partial(0, 9_999, stale_caller, Bytes::from_static(b"stale"))
        .await
        .expect_err("stale write must be rejected by the live fence");
    match err {
        StateBackendError::StaleVersion {
            caller,
            authoritative: got,
        } => {
            assert_eq!(caller, stale_caller);
            assert_eq!(got, authoritative);
        }
        other => panic!("expected StaleVersion, got {other:?}"),
    }

    harness.shutdown().await;
}

async fn setup_query(db: &laminar_db::LaminarDB) {
    db.execute("CREATE SOURCE src (key BIGINT, value BIGINT)")
        .await
        .expect("CREATE SOURCE");
    db.execute(
        "CREATE MATERIALIZED VIEW sums AS \
         SELECT key, SUM(value) AS total FROM src GROUP BY key",
    )
    .await
    .expect("CREATE MATERIALIZED VIEW");
}

/// Concatenate the `sums` MV rows from every node — preserves
/// duplicates so tests catch the invariant violation where a key
/// shows up on more than one node.
async fn union_sums(harness: &ClusterEngineHarness) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for node in &harness.nodes {
        out.extend(read_mv_sums(&node.db, "sums").await);
    }
    out
}

/// Sorted copy for set-equality comparisons across restarts.
#[cfg(feature = "phase-1-recovery")]
fn sorted(mut rows: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    rows.sort_unstable();
    rows
}

/// Follower crashes mid-stream via phi-accrual (no graceful Left).
/// Default build asserts current behavior: rows for the dead node's
/// vnodes are dropped. `phase-2-rebalance` flips to "leader picks up
/// the dead node's vnodes; totals remain correct".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_stream_loses_in_flight() {
    let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    let leader_idx = harness.leader_idx();
    let follower_idx = harness.follower_idxs()[0];

    for node in &harness.nodes {
        setup_query(&node.db).await;
    }
    harness.start_all().await;

    let owners = vec![
        (
            harness.nodes[leader_idx].instance_id,
            harness.nodes[leader_idx].owned_vnodes(),
        ),
        (
            harness.nodes[follower_idx].instance_id,
            harness.nodes[follower_idx].owned_vnodes(),
        ),
    ];
    let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 4)
        .expect("pick_keys_per_owner: search range too small");
    let follower_keys = key_buckets[1].1.clone();

    // Phase A: push 8 keys, verify both MVs populated.
    let phase_a: Vec<i64> = key_buckets
        .iter()
        .flat_map(|(_, ks)| ks.iter().copied())
        .collect();
    let src = harness.nodes[leader_idx]
        .db
        .source_untyped("src")
        .expect("source_untyped");
    src.push_arrow(input_batch(&phase_a)).expect("push phase_a");
    harness.nodes[leader_idx]
        .db
        .checkpoint()
        .await
        .expect("leader checkpoint phase_a");
    sleep(Duration::from_millis(500)).await;

    let pre_crash = union_sums(&harness).await;
    assert_eq!(
        pre_crash.len(),
        8,
        "want 8 rows total; duplicates here would signal a key on >1 node",
    );

    // Crash the follower — drop both the engine and the gossip
    // handle so phi-accrual fires.
    let crashed_runtime = harness.nodes.swap_remove(follower_idx);
    let crashed_node = harness.cluster.nodes.swap_remove(follower_idx);
    drop(crashed_runtime);
    crashed_node.crash().await;
    sleep(Duration::from_secs(4)).await;

    let phase_c = input_batch(&follower_keys);
    let src = harness.nodes[0]
        .db
        .source_untyped("src")
        .expect("source_untyped on surviving leader");
    src.push_arrow(phase_c).expect("push phase_c");
    sleep(Duration::from_millis(500)).await;

    let post_crash_leader = read_mv_sums(&harness.nodes[0].db, "sums").await;
    let post_crash_leader_keys: HashSet<i64> = post_crash_leader.iter().map(|(k, _)| *k).collect();

    // Phi-accrual in a 2-node MiniCluster doesn't propagate the
    // dead peer out of membership (no third gossip peer to relay
    // the detection), so rebalance never fires here. The rotation
    // + adoption path is covered directly by
    // `cluster_rebalance_flow.rs`.
    let expected_keys: HashSet<i64> = key_buckets[0].1.iter().copied().collect();
    assert_eq!(post_crash_leader_keys, expected_keys);

    harness.shutdown().await;
}

/// Restart preserves SUM aggregate state. Includes an
/// intermediate-snapshot assertion so a "restart dropped state; second
/// push repopulated" regression doesn't masquerade as recovery.
#[cfg(feature = "phase-1-recovery")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_sum_aggregate() {
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

    for node in &harness.nodes {
        setup_query(&node.db).await;
    }
    harness.start_all().await;

    let owners = vec![
        (
            harness.nodes[leader_idx].instance_id,
            harness.nodes[leader_idx].owned_vnodes(),
        ),
        (
            harness.nodes[follower_idx].instance_id,
            harness.nodes[follower_idx].owned_vnodes(),
        ),
    ];
    // 4 keys per owner = 8 total; first half = first 2 of each.
    let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 4).expect("pick_keys_per_owner");
    let first_half: Vec<i64> = key_buckets[0].1[..2]
        .iter()
        .chain(key_buckets[1].1[..2].iter())
        .copied()
        .collect();
    let second_half: Vec<i64> = key_buckets[0].1[2..4]
        .iter()
        .chain(key_buckets[1].1[2..4].iter())
        .copied()
        .collect();
    let all_keys: Vec<i64> = first_half
        .iter()
        .chain(second_half.iter())
        .copied()
        .collect();

    // Phase A: first half.
    let src = harness.nodes[leader_idx]
        .db
        .source_untyped("src")
        .expect("source_untyped");
    src.push_arrow(input_batch(&first_half))
        .expect("push first_half");
    harness.nodes[leader_idx]
        .db
        .checkpoint()
        .await
        .expect("checkpoint pre-shutdown");
    sleep(Duration::from_millis(500)).await;

    let pre_shutdown = union_sums(&harness).await;
    assert_eq!(pre_shutdown.len(), 4, "4 keys in pre-shutdown state");

    // Phase B: shutdown, keep dirs, spawn fresh cluster pointing at them.
    let (shared_dir, checkpoint_dirs) = harness.shutdown_keep_dirs().await;
    let mut harness2 =
        ClusterEngineHarness::spawn_with_dirs(N_NODES, VNODE_COUNT, shared_dir, checkpoint_dirs)
            .await;

    for node in &harness2.nodes {
        setup_query(&node.db).await;
    }
    harness2.start_all().await;
    sleep(Duration::from_millis(1000)).await;

    // Intermediate snapshot: state must survive restart before the
    // second-half push. A broken recovery that drops state and lets
    // the second push repopulate would otherwise pass silently.
    let post_restart = union_sums(&harness2).await;
    assert_eq!(
        sorted(post_restart),
        sorted(pre_shutdown),
        "recovered aggregate state must equal pre-shutdown state",
    );

    // Phase C: second half. Totals must equal the unbroken-stream case.
    let src2 = harness2.nodes[harness2.leader_idx()]
        .db
        .source_untyped("src")
        .expect("source_untyped");
    src2.push_arrow(input_batch(&second_half))
        .expect("push second_half");
    harness2.nodes[harness2.leader_idx()]
        .db
        .checkpoint()
        .await
        .expect("checkpoint post-restart");
    sleep(Duration::from_millis(500)).await;

    let final_state = union_sums(&harness2).await;
    let mut expected: Vec<(i64, i64)> = all_keys.iter().map(|&k| (k, k * 10)).collect();
    expected.sort_unstable();
    assert_eq!(
        sorted(final_state),
        expected,
        "final state must equal uninterrupted-stream totals",
    );

    // Epoch drift check (review item H5): per-node `CheckpointStore`
    // counters can diverge across crashes. ≤1 is acceptable.
    let leader_epoch = manifest_epoch(&harness2.nodes[harness2.leader_idx()].db).await;
    let follower_epoch = manifest_epoch(&harness2.nodes[harness2.follower_idxs()[0]].db).await;
    assert!(
        leader_epoch.abs_diff(follower_epoch) <= 1,
        "epoch drift > 1 after restart: leader={leader_epoch} follower={follower_epoch}",
    );

    harness2.shutdown().await;
}
