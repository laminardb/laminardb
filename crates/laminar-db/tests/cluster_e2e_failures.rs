//! Phase 0b failure-mode scenarios for the cluster acceptance suite.
//! See `docs/plans/cluster-production-readiness.md` Phase 0b.
//!
//! Each scenario has two expected outcomes:
//! - **Default build** (no phase-N feature): asserts the CURRENT buggy
//!   behavior. Test passes on this branch today, documenting what's
//!   broken and proving the test itself runs end-to-end.
//! - **Feature build** (`--features phase-1-recovery` / `phase-2-rebalance`):
//!   flipping the gate on in CI once the matching phase lands should
//!   flip these tests to asserting CORRECT behavior. If it doesn't,
//!   the engineer landing the fix must also flip the assertion — which
//!   is a one-line PR change, unlike `#[ignore]` which rots silently.
//!
//! Using feature gates instead of `#[ignore]` is deliberate (review
//! item H1): ignored tests go untouched for months; a feature flip in
//! CI is impossible to forget.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::collections::HashSet;
use std::time::Duration;

use tokio::time::sleep;

#[path = "common/cluster_harness.rs"]
mod cluster_harness;

use cluster_harness::{
    input_batch, pick_keys_per_owner, read_mv_sums, ClusterEngineHarness,
};
// `manifest_epoch` is only used under `phase-1-recovery`; pulling it in
// unconditionally trips `unused_imports` in the default build.
#[cfg(feature = "phase-1-recovery")]
use cluster_harness::manifest_epoch;

const VNODE_COUNT: u32 = 4;
const N_NODES: usize = 2;

/// Phase 1.4 — split-brain fence on `write_partial`.
///
/// Plan AC: "force two nodes to claim same vnode; one is rejected."
/// Here the "two nodes" are modelled as two `ObjectStoreBackend`
/// instances over the same shared-state dir — exactly what a
/// split-brained cluster would look like from the object-store's
/// point of view. One side has advanced its authoritative generation
/// (via a fresh snapshot); the stale side tries to write at the old
/// version and must be rejected.
///
/// The harness already wires `set_authoritative_version` from the
/// loaded snapshot, so this test operates directly on the underlying
/// backend type — end-to-end fence-through-checkpoint is covered by
/// the smoke test (which passes `registry.assignment_version()`
/// through the full write path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_brain_write_partial_rejected() {
    use bytes::Bytes;
    use laminar_core::state::{ObjectStoreBackend, StateBackend, StateBackendError};
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;

    let dir = tempfile::tempdir().expect("tempdir");
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).expect("local fs"),
    );

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

    // Final correctness: the only bytes on disk at the shared path
    // are the fresh side's. The stale attempt never touched storage.
    let got = fresh
        .read_partial(0, 1)
        .await
        .expect("read_partial")
        .expect("present");
    assert_eq!(&got[..], b"fresh");
}

/// Phase 1.2 — coordinated assignment via stored snapshot.
///
/// The plan's AC: "partition cluster, both halves independently arrive
/// at the same assignment from the stored snapshot." We stand in for
/// the partition by spawning two harnesses sequentially against the
/// same backing store. The first writes the initial snapshot via
/// CAS-create; the second reads the snapshot back rather than
/// recomputing. Without coordination both halves would compute
/// `round_robin_assignment` independently against whatever peers each
/// half could see — which during a partition is a subset, producing
/// disjoint vnode ownership and silently split state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assignment_snapshot_unifies_cluster_view() {
    // First cluster: creates the snapshot.
    let harness_a = cluster_harness::ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    let assignment_a: Vec<cluster_harness::NodeIdView> = harness_a
        .nodes
        .iter()
        .map(|n| (n.instance_id, n.owned_vnodes()))
        .collect();
    let (shared_dir, _cp_dirs) = harness_a.shutdown_keep_dirs().await;

    // Second cluster pointing at the same state dir (and therefore the
    // same `control/assignment-snapshot.json`). The boot-time resolve
    // path must load the existing snapshot instead of re-computing.
    let cp_dirs2: Vec<_> = (0..N_NODES)
        .map(|_| tempfile::tempdir().expect("cp dir"))
        .collect();
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

    assert_eq!(
        assignment_a, assignment_b,
        "post-restart cluster must adopt the stored assignment \
         (independent round-robin recompute would disagree here if, \
         say, the sort order of peer ids ever changed or a new peer \
         joined)",
    );
    harness_b.shutdown().await;
}

/// Install `CREATE SOURCE` + `CREATE MATERIALIZED VIEW sums` on a single
/// engine. Idempotent-ish: succeeds the first time; on a post-restart
/// engine the MV may already exist from restored state (tracked under
/// Phase 1.1 — OR REPLACE semantics).
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

/// Snapshot the `sums` MV across every node in a harness and union the
/// rows. With row-shuffle, each node only holds its locally-owned keys,
/// so the cluster's observable state is always the union.
async fn union_sums(harness: &ClusterEngineHarness) -> HashSet<(i64, i64)> {
    let mut out = HashSet::new();
    for node in &harness.nodes {
        for row in read_mv_sums(&node.db, "sums").await {
            out.insert(row);
        }
    }
    out
}

/// Scenario 1 — crash mid-stream.
///
/// The follower is killed without a clean `Left` announcement, exercising
/// phi-accrual detection rather than the gossip-primitive graceful-leave
/// path (review item C5: `kill()` would mask the production failure
/// mode). Phi-accrual at `phi=3.0, gossip=50ms, grace=1s` detects loss
/// in 1-3 seconds; the scenario budgets 4s of slack.
///
/// Default build asserts the CURRENT bug: rows hashed to the dead node's
/// vnodes are silently dropped (the `send_to` write to a broken socket
/// returns `Err` and the row-shuffle path intentionally drops rather
/// than retrying — at-least-once is the source's responsibility). Phase
/// 2.3 (dynamic rebalance) flips this to "leader picks up the dead
/// node's vnodes; totals remain correct"; the `phase-2-rebalance`
/// feature gate reconciles.
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
    assert_eq!(pre_crash.len(), 8, "all 8 keys present before crash");

    // Phase B: CRASH follower (no graceful Left). Both the gossip handle
    // and the engine runtime go: the former so phi-accrual fires, the
    // latter so the shuffle receiver stops reading. We drop both and let
    // the tokio runtime behind the engine wind down on its own; a real
    // crash wouldn't unwind gracefully either.
    let crashed_runtime = harness.nodes.swap_remove(follower_idx);
    let crashed_node = harness.cluster.nodes.swap_remove(follower_idx);
    drop(crashed_runtime);
    crashed_node.crash();

    // Phi-accrual convergence window.
    sleep(Duration::from_secs(4)).await;

    // Phase C: push rows targeting follower's vnodes. Without Phase 2.3,
    // these row-shuffle sends hit a dead TCP peer and drop. With it,
    // the leader rebalances and accepts them locally.
    let phase_c = input_batch(&follower_keys);
    let src = harness.nodes[0]
        .db
        .source_untyped("src")
        .expect("source_untyped on surviving leader");
    src.push_arrow(phase_c).expect("push phase_c");
    sleep(Duration::from_millis(500)).await;

    let post_crash_leader = read_mv_sums(&harness.nodes[0].db, "sums").await;
    let post_crash_leader_keys: HashSet<i64> =
        post_crash_leader.iter().map(|(k, _)| *k).collect();

    #[cfg(not(feature = "phase-2-rebalance"))]
    {
        // Current behavior: leader's MV has ONLY the keys it originally
        // owned (phase_a). Follower's keys from phase_a died with the
        // follower; phase_c rows are lost on ship.
        let expected_keys: HashSet<i64> = key_buckets[0].1.iter().copied().collect();
        assert_eq!(
            post_crash_leader_keys, expected_keys,
            "default build asserts row-loss for crashed-vnode shipments; \
             leader should hold ONLY its originally-owned keys. \
             post_crash_leader={post_crash_leader:?}",
        );
    }
    #[cfg(feature = "phase-2-rebalance")]
    {
        // Phase 2.3 contract: leader takes over follower's vnodes;
        // pre-crash follower-bound rows survive AND phase_c rows land
        // on the leader's MV. All 8 keys present on the leader, with
        // follower_keys' values doubled (phase_a + phase_c both
        // contributed, both at value = key * 10).
        let all_keys: HashSet<i64> =
            phase_a.iter().copied().collect::<HashSet<_>>();
        assert_eq!(
            post_crash_leader_keys, all_keys,
            "phase-2-rebalance: leader must hold all keys after rebalance",
        );
        for (k, total) in &post_crash_leader {
            let expected = if follower_keys.contains(k) {
                k * 10 + k * 10 // phase_a + phase_c contributions
            } else {
                k * 10
            };
            assert_eq!(
                *total, expected,
                "key {k} total should be {expected}, got {total}",
            );
        }
    }

    harness.shutdown().await;
}

/// Scenario 2 — restart preserves aggregate state.
///
/// Feature-gated behind `phase-1-recovery`: needs the MV catalog
/// re-create path to handle already-existing MVs (CREATE OR REPLACE, or
/// restore-first semantics).
///
/// Scope: `SUM` + `MvStorageMode::Aggregate` only.
///
/// Includes an intermediate-snapshot assertion: after restart, before
/// the second push, the union MUST equal the pre-shutdown state.
/// Without it a "restart dropped state; second push repopulated by
/// coincidence" bug would masquerade as recovery.
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

    let harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
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
    let key_buckets = pick_keys_per_owner(VNODE_COUNT, &owners, 4)
        .expect("pick_keys_per_owner");
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
    let all_keys: Vec<i64> = first_half.iter().chain(second_half.iter()).copied().collect();

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
    let harness2 = ClusterEngineHarness::spawn_with_dirs(
        N_NODES,
        VNODE_COUNT,
        shared_dir,
        checkpoint_dirs,
    )
    .await;

    // Re-register DDL on each engine. OR REPLACE semantics aren't yet
    // supported; if the MV store restored the MV on startup, plain
    // CREATE will error. Phase 1.1 owns reconciling this path.
    for node in &harness2.nodes {
        setup_query(&node.db).await;
    }
    harness2.start_all().await;
    sleep(Duration::from_millis(1000)).await;

    // Intermediate snapshot: state must survive restart, BEFORE the
    // second-half push. A broken recovery that drops state and lets
    // the second push repopulate would silently pass without this check.
    let post_restart = union_sums(&harness2).await;
    assert_eq!(
        post_restart, pre_shutdown,
        "recovered aggregate state must equal pre-shutdown state \
         (intermediate assertion — no second-half push yet)",
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
    let expected: HashSet<(i64, i64)> = all_keys.iter().map(|&k| (k, k * 10)).collect();
    assert_eq!(
        final_state, expected,
        "final state must equal uninterrupted-stream totals",
    );

    // Epoch drift check (review item H5): per-node `CheckpointStore`
    // counters can diverge across crashes. ≤1 is acceptable.
    let leader_epoch = manifest_epoch(&harness2.nodes[harness2.leader_idx()].db);
    let follower_epoch =
        manifest_epoch(&harness2.nodes[harness2.follower_idxs()[0]].db);
    assert!(
        leader_epoch.abs_diff(follower_epoch) <= 1,
        "epoch drift > 1 after restart: leader={leader_epoch} follower={follower_epoch}",
    );

    harness2.shutdown().await;
}
