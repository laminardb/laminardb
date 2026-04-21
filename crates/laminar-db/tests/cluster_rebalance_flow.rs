//! Dynamic vnode rebalance flow.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::cluster::control::{AssignmentSnapshotStore, RotateOutcome};
use laminar_core::state::NodeId;

#[path = "common/cluster_harness.rs"]
mod cluster_harness;

use cluster_harness::ClusterEngineHarness;

const VNODE_COUNT: u32 = 4;
const N_NODES: usize = 2;
const POLL_DEADLINE: Duration = Duration::from_secs(5);

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

/// Direct rotation of the durable snapshot propagates to every node's
/// registry via the watcher, independent of the rebalance controller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_watcher_adopts_direct_rotation() {
    let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    harness.start_all().await;

    let store: Arc<AssignmentSnapshotStore> =
        Arc::clone(&harness.nodes[0].assignment_snapshot_store);
    let seed = store.load().await.unwrap().unwrap();

    let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
    let mut vnodes = BTreeMap::new();
    for v in 0..VNODE_COUNT {
        vnodes.insert(v, leader);
    }
    let next = seed.next(vnodes);
    let expected = next.version;
    assert!(matches!(
        store.save_if_version(&next, seed.version).await.unwrap(),
        RotateOutcome::Rotated,
    ));

    wait_for(
        || {
            harness
                .nodes
                .iter()
                .all(|n| n.vnode_registry.assignment_version() >= expected)
        },
        "every node to adopt the new version",
    )
    .await;

    for node in &harness.nodes {
        let backend_v = node.state_backend.authoritative_version();
        let registry_v = node.vnode_registry.assignment_version();
        assert!(backend_v >= expected);
        assert_eq!(backend_v, registry_v);
    }
    assert_eq!(
        harness.nodes[harness.leader_idx()].owned_vnodes().len(),
        VNODE_COUNT as usize,
    );

    harness.shutdown().await;
}

/// Two rotations from the same prior: CAS picks one, the loser
/// reloads the winner's snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_rotation_picks_single_winner() {
    let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    harness.start_all().await;

    let store: Arc<AssignmentSnapshotStore> =
        Arc::clone(&harness.nodes[0].assignment_snapshot_store);
    let seed = store.load().await.unwrap().unwrap();

    let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
    let follower = NodeId(harness.nodes[harness.follower_idxs()[0]].instance_id.0);
    let mut a_map = BTreeMap::new();
    let mut b_map = BTreeMap::new();
    for v in 0..VNODE_COUNT {
        a_map.insert(v, leader);
        b_map.insert(v, follower);
    }
    let (a, b) = (seed.next(a_map), seed.next(b_map));

    let (ra, rb) = tokio::join!(
        store.save_if_version(&a, seed.version),
        store.save_if_version(&b, seed.version),
    );
    let outcomes = [ra.unwrap(), rb.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, RotateOutcome::Rotated))
            .count(),
        1,
    );

    let stored = store.load().await.unwrap().unwrap();
    assert_eq!(stored.version, seed.version + 1);
    for outcome in &outcomes {
        if let RotateOutcome::Conflict(current) = outcome {
            assert_eq!(current, &stored);
        }
    }

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

/// A stale writer proposing the next version after someone else has
/// already landed there must see Conflict and the registry must not
/// regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_rotation_attempt_rejected_by_cas() {
    let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;
    harness.start_all().await;

    let store: Arc<AssignmentSnapshotStore> =
        Arc::clone(&harness.nodes[0].assignment_snapshot_store);
    let seed = store.load().await.unwrap().unwrap();

    let leader = NodeId(harness.nodes[harness.leader_idx()].instance_id.0);
    let mut m = BTreeMap::new();
    for v in 0..VNODE_COUNT {
        m.insert(v, leader);
    }
    let next = seed.next(m);
    assert!(matches!(
        store.save_if_version(&next, seed.version).await.unwrap(),
        RotateOutcome::Rotated,
    ));

    let mut stale_map = BTreeMap::new();
    for v in 0..VNODE_COUNT {
        stale_map.insert(v, NodeId(99));
    }
    let stale = seed.next(stale_map);
    match store.save_if_version(&stale, seed.version).await.unwrap() {
        RotateOutcome::Conflict(current) => assert_eq!(current, next),
        RotateOutcome::Rotated => panic!("stale rotation must not succeed"),
    }

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
    for node in &harness.nodes {
        for v in 0..VNODE_COUNT {
            assert_ne!(node.vnode_registry.owner(v).0, 99);
        }
    }

    harness.shutdown().await;
}

/// Post-rotation checkpoints stamp writes at the new fence version
/// and the commit marker lands under that epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_after_rotation_carries_new_version() {
    let mut harness = ClusterEngineHarness::spawn(N_NODES, VNODE_COUNT).await;

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

    wait_for(
        || {
            harness
                .nodes
                .iter()
                .all(|n| n.vnode_registry.assignment_version() >= rotated.version)
        },
        "adoption of rotated snapshot",
    )
    .await;

    let result = harness.nodes[harness.leader_idx()]
        .db
        .checkpoint()
        .await
        .expect("checkpoint");
    assert!(
        result.success,
        "post-rotation checkpoint: {:?}",
        result.error
    );

    assert!(harness.nodes[harness.leader_idx()]
        .decision_store
        .is_committed(result.epoch)
        .await
        .unwrap());

    harness.shutdown().await;
}
