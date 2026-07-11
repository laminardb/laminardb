//! Smoke tests for the public state-backend API: load a config, call
//! `build()`, exercise the resulting `Arc<dyn StateBackend>`.

use std::sync::Arc;

use bytes::Bytes;
use laminar_core::state::{
    CheckpointAttempt, ObjectStoreBackend, StateBackend, StateBackendConfig, StateBackendDurability,
};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::tempdir;

#[tokio::test]
async fn config_roundtrip_in_process_local_object_store() {
    let attempt = CheckpointAttempt::new(1, 1);
    let c = StateBackendConfig::in_process();
    let b = c.build().await.unwrap();
    assert_eq!(b.durability_scope(), StateBackendDurability::Volatile);
    b.write_partial(attempt, 0, 0, Bytes::from_static(b"a"))
        .await
        .unwrap();
    assert_eq!(
        &b.read_partial(attempt, 0).await.unwrap().unwrap()[..],
        b"a"
    );

    let dir = tempdir().unwrap();
    let c = StateBackendConfig::local(dir.path());
    let b = c.build().await.unwrap();
    assert_eq!(b.durability_scope(), StateBackendDurability::NodeDurable);
    b.write_partial(attempt, 0, 0, Bytes::from_static(b"b"))
        .await
        .unwrap();
    assert_eq!(
        &b.read_partial(attempt, 0).await.unwrap().unwrap()[..],
        b"b"
    );

    let dir = tempdir().unwrap();
    let url = format!(
        "file://{}",
        dir.path().display().to_string().replace('\\', "/")
    );
    let c = StateBackendConfig::object_store(url, "node-0");
    let b = c.build().await.unwrap();
    assert_eq!(b.durability_scope(), StateBackendDurability::NodeDurable);
    b.write_partial(attempt, 0, 0, Bytes::from_static(b"c"))
        .await
        .unwrap();
    assert_eq!(
        &b.read_partial(attempt, 0).await.unwrap().unwrap()[..],
        b"c"
    );
}

/// Two instances writing to a shared object store root: partials are
/// visible cross-instance and the exact-attempt seal is CAS-created so only
/// one execution incarnation wins.
///
#[tokio::test]
async fn distributed_embedded_static_two_instances_shared_store() {
    use laminar_core::state::StateBackendError;

    let dir = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    let node_a = ObjectStoreBackend::cluster_shared(Arc::clone(&store), "node-a", 4);
    let node_b = ObjectStoreBackend::cluster_shared(Arc::clone(&store), "node-b", 4);
    assert_eq!(
        node_a.durability_scope(),
        StateBackendDurability::ClusterShared
    );
    let attempt = CheckpointAttempt::new(1, 100);

    node_a
        .write_partial(attempt, 0, 0, Bytes::from_static(b"A0"))
        .await
        .unwrap();
    node_a
        .write_partial(attempt, 1, 0, Bytes::from_static(b"A1"))
        .await
        .unwrap();
    node_b
        .write_partial(attempt, 2, 0, Bytes::from_static(b"B2"))
        .await
        .unwrap();
    node_b
        .write_partial(attempt, 3, 0, Bytes::from_static(b"B3"))
        .await
        .unwrap();

    assert_eq!(
        &node_a.read_partial(attempt, 2).await.unwrap().unwrap()[..],
        b"B2"
    );
    assert_eq!(
        &node_b.read_partial(attempt, 0).await.unwrap().unwrap()[..],
        b"A0"
    );

    assert!(node_a
        .seal_checkpoint(attempt, 0, &[0, 1, 2, 3], &[])
        .await
        .unwrap());
    assert!(node_a
        .seal_checkpoint(attempt, 0, &[0, 1, 2, 3], &[])
        .await
        .unwrap());

    // node_b loses — it must not keep driving the commit phase.
    let err = node_b
        .seal_checkpoint(attempt, 0, &[0, 1, 2, 3], &[])
        .await
        .unwrap_err();
    assert!(matches!(err, StateBackendError::Conflict { .. }));

    let next = CheckpointAttempt::new(2, 101);
    node_a
        .write_partial(next, 0, 0, Bytes::from_static(b"A0@2"))
        .await
        .unwrap();
    assert!(!node_a
        .seal_checkpoint(next, 0, &[0, 1, 2, 3], &[])
        .await
        .unwrap());
}
