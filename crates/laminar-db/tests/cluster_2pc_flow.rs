//! 2-node 2PC checkpoint over real `MiniCluster` gossip + shared
//! `InProcessBackend`. See `docs/plans/checkpoint-2pc-sequencing.md`.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
use laminar_core::cluster::testing::MiniCluster;
use laminar_core::state::{owned_vnodes, InProcessBackend, StateBackend, NodeId, VnodeRegistry};
use laminar_db::checkpoint_coordinator::{
    CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
};
use laminar_storage::checkpoint_store::FileSystemCheckpointStore;

const CONVERGENCE: Duration = Duration::from_secs(5);

fn make_coord(
    dir: &std::path::Path,
    backend: Arc<InProcessBackend>,
    vnodes: Vec<u32>,
    controller: Arc<laminar_core::cluster::control::ClusterController>,
) -> CheckpointCoordinator {
    let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store);
    coord.set_state_backend(backend);
    coord.set_vnode_set(vnodes);
    coord.set_cluster_controller(controller);
    coord
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_node_leader_commits_follower_mirrors() {
    let cluster = MiniCluster::spawn(2).await;
    cluster
        .wait_for_convergence(CONVERGENCE)
        .await
        .expect("cluster converges");

    // Identify leader / follower from instance ids — lowest id wins.
    let (leader_node, follower_node) = if cluster.nodes[0].controller.is_leader() {
        (&cluster.nodes[0], &cluster.nodes[1])
    } else {
        (&cluster.nodes[1], &cluster.nodes[0])
    };
    assert!(leader_node.controller.is_leader());
    assert!(!follower_node.controller.is_leader());

    // Shared backend + vnode topology. 4 vnodes split 2-2.
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
    );
    let mut follower_coord = make_coord(
        follower_dir.path(),
        backend.clone(),
        owned_vnodes(&registry, NodeId(follower_node.instance_id.0)),
        Arc::clone(&follower_node.controller),
    );

    // The test drives the follower task manually with a synthetic
    // PREPARE announcement — in production the pipeline observes the
    // leader's announcement off the KV. Here the epoch and id must
    // match what the leader will assign (fresh store starts at 1).
    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Prepare,
        flags: 0,
    };

    // Run both halves concurrently.
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

    // Every owned vnode across both nodes has a marker on the shared
    // backend — demonstrates cross-instance durability.
    for v in 0..4 {
        assert!(
            backend.read_partial(v, 1).await.unwrap().is_some(),
            "missing marker for vnode {v}",
        );
    }

    cluster.shutdown().await;
}
