//! 2-node 2PC checkpoint over real `MiniCluster` gossip + shared
//! `InProcessBackend`.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::control::{BarrierAnnouncement, CheckpointDecisionStore, Phase};
use laminar_core::cluster::testing::MiniCluster;
use laminar_core::state::{owned_vnodes, InProcessBackend, NodeId, StateBackend, VnodeRegistry};
use laminar_db::checkpoint_coordinator::{
    CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
};
use laminar_storage::checkpoint_store::FileSystemCheckpointStore;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;

const CONVERGENCE: Duration = Duration::from_secs(5);

async fn make_coord(
    dir: &std::path::Path,
    backend: Arc<InProcessBackend>,
    vnodes: Vec<u32>,
    controller: Arc<laminar_core::cluster::control::ClusterController>,
) -> CheckpointCoordinator {
    let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    coord.set_state_backend(backend);
    coord.set_vnode_set(vnodes);
    coord.set_cluster_controller(controller);
    coord
}

fn make_decision_store(dir: &TempDir) -> Arc<CheckpointDecisionStore> {
    let os: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).expect("LocalFileSystem for decision store"),
    );
    Arc::new(CheckpointDecisionStore::new(os))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_node_leader_commits_follower_mirrors() {
    let cluster = MiniCluster::spawn(2).await;
    cluster
        .wait_for_convergence(CONVERGENCE)
        .await
        .expect("cluster converges");

    let (leader_node, follower_node) = if cluster.nodes[0].controller.is_leader() {
        (&cluster.nodes[0], &cluster.nodes[1])
    } else {
        (&cluster.nodes[1], &cluster.nodes[0])
    };

    let backend = Arc::new(InProcessBackend::new(4));
    let registry = VnodeRegistry::new(4);
    registry.set_assignment(
        vec![
            NodeId(leader_node.instance_id.0),
            NodeId(leader_node.instance_id.0),
            NodeId(follower_node.instance_id.0),
            NodeId(follower_node.instance_id.0),
        ]
        .into(),
    );

    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader_coord = make_coord(
        leader_dir.path(),
        backend.clone(),
        owned_vnodes(&registry, NodeId(leader_node.instance_id.0)),
        Arc::clone(&leader_node.controller),
    )
    .await;
    let mut follower_coord = make_coord(
        follower_dir.path(),
        backend.clone(),
        owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
        Arc::clone(&follower_node.controller),
    )
    .await;

    // Synthetic PREPARE — production's pipeline observes the leader's
    // announcement off the KV. Fresh store starts at epoch/id = 1.
    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };

    let follower_handle = tokio::spawn(async move {
        follower_coord
            .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(15))
            .await
    });

    // Give the follower a beat to start its poll loop before the
    // leader races past it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let leader_result = leader_coord
        .checkpoint(CheckpointRequest::default())
        .await
        .expect("leader checkpoint call");
    assert!(
        leader_result.success,
        "leader checkpoint must succeed: {:?}",
        leader_result.error
    );

    let committed = follower_handle
        .await
        .expect("follower task join")
        .expect("follower checkpoint Result");
    assert!(committed, "follower must commit on leader's Commit");

    // Every vnode across both nodes has a marker on the shared backend.
    for v in 0..4 {
        assert!(backend.read_partial(v, 1).await.unwrap().is_some());
    }

    cluster.shutdown().await;
}

/// Leader writes a durable commit marker before announcing `Commit`,
/// so a new leader mid-2PC can recover the cluster vote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_records_commit_decision_before_announce() {
    let cluster = MiniCluster::spawn(2).await;
    cluster.wait_for_convergence(CONVERGENCE).await.unwrap();
    let (leader_node, follower_node) = if cluster.nodes[0].controller.is_leader() {
        (&cluster.nodes[0], &cluster.nodes[1])
    } else {
        (&cluster.nodes[1], &cluster.nodes[0])
    };

    let backend = Arc::new(InProcessBackend::new(4));
    let registry = VnodeRegistry::new(4);
    registry.set_assignment(
        vec![
            NodeId(leader_node.instance_id.0),
            NodeId(leader_node.instance_id.0),
            NodeId(follower_node.instance_id.0),
            NodeId(follower_node.instance_id.0),
        ]
        .into(),
    );

    let decision_dir = tempfile::tempdir().unwrap();
    let decision_store = make_decision_store(&decision_dir);

    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader_coord = make_coord(
        leader_dir.path(),
        backend.clone(),
        owned_vnodes(&registry, NodeId(leader_node.instance_id.0)),
        Arc::clone(&leader_node.controller),
    )
    .await;
    leader_coord.set_decision_store(Arc::clone(&decision_store));
    let mut follower_coord = make_coord(
        follower_dir.path(),
        backend.clone(),
        owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
        Arc::clone(&follower_node.controller),
    )
    .await;
    follower_coord.set_decision_store(Arc::clone(&decision_store));

    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let follower_handle = tokio::spawn(async move {
        follower_coord
            .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(15))
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let leader_result = leader_coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(
        leader_result.success,
        "leader checkpoint: {:?}",
        leader_result.error
    );
    let _ = follower_handle.await.unwrap().unwrap();

    // The cluster's durable 2PC verdict for epoch 1 is Committed.
    // A new leader elected mid-2PC would read exactly this.
    assert!(
        decision_store.is_committed(1).await.unwrap(),
        "leader must record commit marker before announcing Commit",
    );

    cluster.shutdown().await;
}

/// Follower that times out waiting for `Commit` must consult the
/// marker rather than blanket-rolling back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_timeout_commits_when_decision_recorded() {
    let cluster = MiniCluster::spawn(2).await;
    cluster.wait_for_convergence(CONVERGENCE).await.unwrap();
    let follower_node = if cluster.nodes[0].controller.is_leader() {
        &cluster.nodes[1]
    } else {
        &cluster.nodes[0]
    };

    let backend = Arc::new(InProcessBackend::new(4));
    let registry = VnodeRegistry::new(4);
    registry.set_assignment(vec![NodeId(follower_node.instance_id.0); 4].into());

    let decision_dir = tempfile::tempdir().unwrap();
    let decision_store = make_decision_store(&decision_dir);
    // Simulate leader that recorded Committed then died — no Commit
    // announcement ever reaches the follower.
    decision_store.record_committed(42).await.unwrap();

    let follower_dir = tempfile::tempdir().unwrap();
    let mut follower_coord = make_coord(
        follower_dir.path(),
        backend.clone(),
        owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
        Arc::clone(&follower_node.controller),
    )
    .await;
    follower_coord.set_decision_store(Arc::clone(&decision_store));

    let ann = BarrierAnnouncement {
        epoch: 42,
        checkpoint_id: 100,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let committed = follower_coord
        .follower_checkpoint(
            CheckpointRequest::default(),
            ann,
            Duration::from_millis(500),
        )
        .await
        .unwrap();

    assert!(committed);
    cluster.shutdown().await;
}

/// No marker → follower times out and rolls back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_timeout_rolls_back_when_no_decision() {
    let cluster = MiniCluster::spawn(2).await;
    cluster.wait_for_convergence(CONVERGENCE).await.unwrap();
    let follower_node = if cluster.nodes[0].controller.is_leader() {
        &cluster.nodes[1]
    } else {
        &cluster.nodes[0]
    };

    let backend = Arc::new(InProcessBackend::new(4));
    let registry = VnodeRegistry::new(4);
    registry.set_assignment(vec![NodeId(follower_node.instance_id.0); 4].into());

    let decision_dir = tempfile::tempdir().unwrap();
    let decision_store = make_decision_store(&decision_dir);

    let follower_dir = tempfile::tempdir().unwrap();
    let mut follower_coord = make_coord(
        follower_dir.path(),
        backend.clone(),
        owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
        Arc::clone(&follower_node.controller),
    )
    .await;
    follower_coord.set_decision_store(Arc::clone(&decision_store));

    let ann = BarrierAnnouncement {
        epoch: 99,
        checkpoint_id: 200,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let committed = follower_coord
        .follower_checkpoint(
            CheckpointRequest::default(),
            ann,
            Duration::from_millis(500),
        )
        .await
        .unwrap();
    assert!(!committed);
    cluster.shutdown().await;
}
