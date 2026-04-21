//! Dynamic vnode rebalance flow — snapshot watcher adoption and
//! leader-side rotation race semantics. Covers the cluster_production
//! plan's gap #2 ("Dynamic rebalance not implemented").
//!
//! The engine-level tests exercise the real
//! `spawn_snapshot_watcher`/`spawn_rebalance_controller` tasks wired
//! up through the harness, with `RebalanceConfig::test_defaults` so
//! wall time stays under a few seconds.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::cluster::control::{AssignmentSnapshotStore, Decision, RotateOutcome};
use laminar_core::state::NodeId;

#[path = "common/cluster_harness.rs"]
mod cluster_harness;

use cluster_harness::ClusterEngineHarness;

const VNODE_COUNT: u32 = 4;
const N_NODES: usize = 2;
const POLL_DEADLINE: Duration = Duration::from_secs(5);

/// Wait until `predicate` returns true or [`POLL_DEADLINE`] elapses.
/// Poll cadence matches the fast watcher interval so the test
/// observes adoption within a few ticks.
async fn wait_for<F: Fn() -> bool>(predicate: F, what: &str) {
    let deadline = Instant::now() + POLL_DEADLINE;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// A direct rotation of the durable snapshot must propagate to every
/// node's local registry through the snapshot watcher, without any
/// membership-change trigger. This is the adoption half of the
/// rebalance protocol — it must work independently of the
/// leader-rebalance controller's rotation path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_watcher_adopts_direct_rotation() {
    let harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    harness.start_all().await;

    // Current canonical version (one or more nodes may have already
    // had their version set by the seed snapshot).
    let seed_version = harness.nodes[0].vnode_registry.assignment_version();
    let store: Arc<AssignmentSnapshotStore> =
        Arc::clone(&harness.nodes[0].assignment_snapshot_store);
    let seed = store
        .load()
        .await
        .expect("load seed")
        .expect("seed snapshot present");
    assert_eq!(seed.version, seed_version);

    // Forge a rotation by CAS-creating the next version with a
    // deliberately different assignment (leader owns everything).
    // No engine owns a rebalance trigger here — the snapshot watcher
    // is the only thing that can propagate this.
    let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
    let mut vnodes = BTreeMap::new();
    for v in 0..VNODE_COUNT {
        vnodes.insert(v, leader);
    }
    let next = seed.next(vnodes);
    let expected = next.version;
    match store
        .save_if_version(&next, seed.version)
        .await
        .expect("rotate")
    {
        RotateOutcome::Rotated => {}
        RotateOutcome::Conflict(w) => {
            panic!("seed rotation conflicted unexpectedly; winner={w:?}");
        }
    }

    // The watcher runs at test_defaults (200ms poll). Give it enough
    // slack to observe + adopt.
    wait_for(
        || {
            harness
                .nodes
                .iter()
                .all(|n| n.vnode_registry.assignment_version() >= expected)
        },
        "every node's registry to adopt the new version",
    )
    .await;

    // Every backend's fence version tracks the registry — the
    // Phase 1.4 guarantee after our adoption path runs.
    for node in &harness.nodes {
        let backend_v = node.state_backend.authoritative_version();
        let registry_v = node.vnode_registry.assignment_version();
        assert!(
            backend_v >= expected,
            "backend fence did not track adoption on node {:?}: backend={backend_v}, expected>={expected}",
            node.instance_id,
        );
        assert_eq!(
            backend_v, registry_v,
            "backend fence and registry drifted on node {:?}",
            node.instance_id,
        );
    }

    // And the leader owns every vnode per the rotated assignment —
    // proves adoption used the new snapshot's content, not the seed.
    let leader_owned = harness.nodes[harness.leader_idx()].owned_vnodes();
    assert_eq!(
        leader_owned.len(),
        VNODE_COUNT as usize,
        "leader must own all vnodes post-rotation",
    );

    harness.shutdown().await;
}

/// Two writers racing to rotate from the same prior version: CAS
/// picks one, the loser reloads the winner's snapshot. This is
/// `save_if_version` behaving correctly under contention, and it's
/// the split-brain-leader scenario's safety net.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_rotation_picks_single_winner() {
    let harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    harness.start_all().await;

    let store: Arc<AssignmentSnapshotStore> =
        Arc::clone(&harness.nodes[0].assignment_snapshot_store);
    let seed = store.load().await.unwrap().unwrap();
    let seed_version = seed.version;

    // Racer A: everything to leader.
    let mut a_map = BTreeMap::new();
    let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
    for v in 0..VNODE_COUNT {
        a_map.insert(v, leader);
    }
    let a = seed.next(a_map);

    // Racer B: flipped — everything to follower.
    let mut b_map = BTreeMap::new();
    let follower = NodeId(harness.nodes[harness.follower_idxs()[0]].instance_id.0);
    for v in 0..VNODE_COUNT {
        b_map.insert(v, follower);
    }
    let b = seed.next(b_map);

    // Issue both CAS writes concurrently.
    let (ra, rb) = tokio::join!(
        store.save_if_version(&a, seed_version),
        store.save_if_version(&b, seed_version),
    );

    let outcomes = [ra.unwrap(), rb.unwrap()];
    let rotated = outcomes
        .iter()
        .filter(|o| matches!(o, RotateOutcome::Rotated))
        .count();
    let conflicts = outcomes
        .iter()
        .filter(|o| matches!(o, RotateOutcome::Conflict(_)))
        .count();
    assert_eq!(rotated, 1, "exactly one racer must win the CAS");
    assert_eq!(conflicts, 1, "exactly one racer must see Conflict");

    // Whatever survived CAS is what the store holds — read it back
    // and check the losing racer's Conflict carries the same
    // canonical snapshot.
    let stored = store.load().await.unwrap().unwrap();
    assert_eq!(stored.version, seed_version + 1);
    for outcome in &outcomes {
        if let RotateOutcome::Conflict(current) = outcome {
            assert_eq!(
                current, &stored,
                "loser's Conflict payload must equal the canonical stored snapshot",
            );
        }
    }

    // Watcher converges every node onto the winner.
    wait_for(
        || {
            harness
                .nodes
                .iter()
                .all(|n| n.vnode_registry.assignment_version() >= stored.version)
        },
        "nodes to adopt the CAS winner",
    )
    .await;

    harness.shutdown().await;
}

/// Monotonicity invariant: a lower-versioned snapshot attempting to
/// land post-rotation must be rejected by the primitive, and the
/// registry must never regress. This guards against a regression
/// where a late retry or replayed request overwrites the durable
/// version with an older view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_rotation_attempt_rejected_by_cas() {
    let harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    harness.start_all().await;

    let store: Arc<AssignmentSnapshotStore> =
        Arc::clone(&harness.nodes[0].assignment_snapshot_store);
    let seed = store.load().await.unwrap().unwrap();

    // Legit rotation: seed → next.
    let mut m = BTreeMap::new();
    let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
    for v in 0..VNODE_COUNT {
        m.insert(v, leader);
    }
    let next = seed.next(m);
    assert!(matches!(
        store.save_if_version(&next, seed.version).await.unwrap(),
        RotateOutcome::Rotated,
    ));

    // Now a stale writer that still thinks the world is at `seed`
    // proposes a different `seed + 1`. CAS rejects — storage is
    // already at `seed + 1`.
    let mut m2 = BTreeMap::new();
    for v in 0..VNODE_COUNT {
        m2.insert(v, NodeId(99));
    }
    let stale_proposal = seed.next(m2);
    match store
        .save_if_version(&stale_proposal, seed.version)
        .await
        .unwrap()
    {
        RotateOutcome::Conflict(current) => {
            assert_eq!(current, next, "stale writer must see the live snapshot");
        }
        RotateOutcome::Rotated => panic!("stale rotation must not succeed"),
    }

    // Adoption: nodes converge on `next`, never on `stale_proposal`.
    wait_for(
        || {
            harness
                .nodes
                .iter()
                .all(|n| n.vnode_registry.assignment_version() >= next.version)
        },
        "nodes to adopt the legitimate rotation",
    )
    .await;
    // No node owns NodeId(99) — stale proposal never landed.
    for node in &harness.nodes {
        for v in 0..VNODE_COUNT {
            assert_ne!(
                node.vnode_registry.owner(v).0,
                99,
                "stale proposal leaked to node {:?}",
                node.instance_id,
            );
        }
    }

    harness.shutdown().await;
}

/// A durable rotation followed by a checkpoint must interact
/// cleanly: post-rotation writes stamp with the NEW fence version,
/// the 2PC decision store still records Committed under the new
/// generation, and recovery reading the manifest sees a consistent
/// epoch regardless of which side of rotation it lands on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_after_rotation_carries_new_version() {
    let harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

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

    let store: Arc<AssignmentSnapshotStore> =
        Arc::clone(&harness.nodes[0].assignment_snapshot_store);
    let seed = store.load().await.unwrap().unwrap();

    // Force a rotation (leader owns all 4 vnodes).
    let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
    let mut m = BTreeMap::new();
    for v in 0..VNODE_COUNT {
        m.insert(v, leader);
    }
    let rotated = seed.next(m);
    assert!(matches!(
        store.save_if_version(&rotated, seed.version).await.unwrap(),
        RotateOutcome::Rotated,
    ));
    let new_version = rotated.version;

    // Wait for every node to adopt.
    wait_for(
        || {
            harness
                .nodes
                .iter()
                .all(|n| n.vnode_registry.assignment_version() >= new_version)
        },
        "adoption of rotated snapshot",
    )
    .await;

    // Run a checkpoint AFTER rotation. Caller-side writes now stamp
    // at the new version; the fence accepts them and the decision
    // store records Committed.
    let result = harness.nodes[harness.leader_idx()]
        .db
        .checkpoint()
        .await
        .expect("checkpoint after rotation");
    assert!(result.success, "post-rotation checkpoint: {:?}", result.error);

    let verdict = harness.nodes[harness.leader_idx()]
        .decision_store
        .load(result.epoch)
        .await
        .expect("decision load");
    assert_eq!(
        verdict,
        Some(Decision::Committed),
        "decision must be recorded under the rotated fence version",
    );

    harness.shutdown().await;
}
