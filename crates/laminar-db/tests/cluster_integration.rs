#![cfg(feature = "cluster")]
//! Public cluster checkpoint-storage admission test.

use std::sync::Arc;
use std::time::Duration;

use laminar_core::checkpoint::CheckpointParticipant;
use laminar_core::cluster::control::{
    prove_shared_object_store_namespaces, ClusterController, ClusterKv, InMemoryKv, LeaseDeadline,
};
use laminar_core::state::NodeId;
use laminar_db::LaminarDB;

#[tokio::test]
async fn cluster_runtime_uses_one_verified_checkpoint_object_store() {
    let node = NodeId(1);
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(
        node,
        Arc::clone(&control),
        None,
        members_rx,
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(30))))
        .unwrap();

    let checkpoint_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let participant = CheckpointParticipant {
        node_id: node.0,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let namespaces = prove_shared_object_store_namespaces(
        participant,
        &[participant],
        control,
        checkpoint_store,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .verified_cluster_namespaces(namespaces)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
        .build()
        .await
        .unwrap();

    assert!(db.is_checkpoint_enabled());
}
