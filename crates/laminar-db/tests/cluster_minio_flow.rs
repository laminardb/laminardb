//! 2-node checkpoint against a shared MinIO bucket via
//! `ObjectStoreBackend`. Skips when MinIO is unreachable.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::sync::Arc;
use std::time::Duration;

use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
use object_store::ObjectStoreExt;
use laminar_core::cluster::testing::MiniCluster;
use laminar_core::state::{
    owned_vnodes, round_robin_assignment, ObjectStoreBackend, NodeId, VnodeRegistry,
};
use laminar_db::checkpoint_coordinator::{
    CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
};
use laminar_storage::checkpoint_store::FileSystemCheckpointStore;

mod common;
use common::{minio_endpoint, minio_store};

const CONVERGENCE: Duration = Duration::from_secs(5);

fn make_coord(
    dir: &std::path::Path,
    backend: Arc<ObjectStoreBackend>,
    vnodes: Vec<u32>,
    gate_vnodes: Vec<u32>,
    controller: Arc<laminar_core::cluster::control::ClusterController>,
) -> CheckpointCoordinator {
    let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store);
    coord.set_state_backend(backend);
    coord.set_vnode_set(vnodes);
    coord.set_gate_vnode_set(gate_vnodes);
    coord.set_cluster_controller(controller);
    coord
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_node_minio_leader_commits_follower_mirrors() {
    if minio_endpoint().is_none() {
        eprintln!("skipping: MinIO not reachable at 127.0.0.1:19000");
        return;
    }
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

    // Unique bucket per run — timestamped to avoid cross-test leakage.
    let bucket = format!(
        "laminar-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let store = minio_store(&bucket).await;

    // Each instance wraps the SAME underlying MinIO bucket with its
    // own backend instance, advertising its own `instance_id`.
    let leader_backend = Arc::new(ObjectStoreBackend::new(
        Arc::clone(&store),
        leader_node.instance_id.0.to_string(),
        4,
    ));
    let follower_backend = Arc::new(ObjectStoreBackend::new(
        Arc::clone(&store),
        follower_node.instance_id.0.to_string(),
        4,
    ));

    // Round-robin 4 vnodes across the 2 peers — the production pattern.
    let registry = VnodeRegistry::new(4);
    let peers = [
        NodeId(leader_node.instance_id.0),
        NodeId(follower_node.instance_id.0),
    ];
    registry.set_assignment(round_robin_assignment(4, &peers));

    let leader_owned = owned_vnodes(&registry, NodeId(leader_node.instance_id.0));
    let follower_owned = owned_vnodes(&registry, NodeId(follower_node.instance_id.0));
    assert_eq!(leader_owned.len() + follower_owned.len(), 4);
    let full = (0..4).collect::<Vec<_>>();

    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader_coord = make_coord(
        leader_dir.path(),
        leader_backend,
        leader_owned,
        full.clone(),
        Arc::clone(&leader_node.controller),
    );
    let mut follower_coord = make_coord(
        follower_dir.path(),
        follower_backend,
        follower_owned,
        full,
        Arc::clone(&follower_node.controller),
    );

    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Prepare,
        flags: 0,
    };

    let follower_handle = tokio::spawn(async move {
        follower_coord
            .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(20))
            .await
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    let leader_result = leader_coord
        .checkpoint(CheckpointRequest::default())
        .await
        .expect("leader checkpoint");
    assert!(
        leader_result.success,
        "leader checkpoint over MinIO must succeed: {:?}",
        leader_result.error,
    );

    let committed = follower_handle.await.expect("join").expect("follower");
    assert!(committed, "follower must commit on leader's Commit");

    // Every vnode's partial.bin and the epoch _COMMIT marker are
    // present on MinIO — the cross-instance durability proof.
    for v in 0..4 {
        let path = object_store::path::Path::from(format!("epoch=1/vnode={v}/partial.bin"));
        let meta = store.head(&path).await;
        assert!(meta.is_ok(), "missing partial for vnode {v}: {meta:?}");
    }
    let commit_path = object_store::path::Path::from("epoch=1/_COMMIT");
    assert!(
        store.head(&commit_path).await.is_ok(),
        "missing epoch=1/_COMMIT marker on MinIO",
    );

    cluster.shutdown().await;
}
