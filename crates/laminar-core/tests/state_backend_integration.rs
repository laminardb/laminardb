//! Developer-experience smoke tests for the state backend.
//!
//! These exercise the public API the way an embedded user would — load
//! a config (or call a builder), invoke `build()`, use the resulting
//! `Arc<dyn StateBackend>`. They do not reach into internals.

use std::sync::Arc;

use bytes::Bytes;
use laminar_core::state::{
    ObjectStoreBackend, StateBackend, StateBackendConfig,
};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::tempdir;

#[tokio::test]
async fn config_roundtrip_in_process_local_object_store() {
    // in_process
    let c = StateBackendConfig::in_process();
    let b = c.build().await.unwrap();
    b.publish_watermark(0, 1).await.unwrap();
    assert_eq!(b.global_watermark(&[0]).await.unwrap(), 1);

    // local
    let dir = tempdir().unwrap();
    let c = StateBackendConfig::local(dir.path());
    let b = c.build().await.unwrap();
    b.publish_watermark(0, 2).await.unwrap();
    assert_eq!(b.global_watermark(&[0]).await.unwrap(), 2);

    // object_store via file:// URL
    let dir = tempdir().unwrap();
    let url = format!(
        "file://{}",
        dir.path().display().to_string().replace('\\', "/")
    );
    let c = StateBackendConfig::object_store(url, "node-0");
    let b = c.build().await.unwrap();
    b.write_partial(0, 1, Bytes::from_static(b"ok")).await.unwrap();
    let got = b.read_partial(0, 1).await.unwrap().unwrap();
    assert_eq!(&got[..], b"ok");
}

/// Distributed-embedded static tier: two instances writing to a shared
/// `object_store` root. Partials and commit manifests are visible
/// cross-instance; watermark unification is out of scope until the
/// chitchat-gossip tier lands.
#[tokio::test]
async fn distributed_embedded_static_two_instances_shared_store() {
    let dir = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    let node_a = ObjectStoreBackend::new(Arc::clone(&store), "node-a", 4);
    let node_b = ObjectStoreBackend::new(Arc::clone(&store), "node-b", 4);

    // Each instance writes its own vnode subset.
    node_a
        .write_partial(0, 1, Bytes::from_static(b"A0"))
        .await
        .unwrap();
    node_a
        .write_partial(1, 1, Bytes::from_static(b"A1"))
        .await
        .unwrap();
    node_b
        .write_partial(2, 1, Bytes::from_static(b"B2"))
        .await
        .unwrap();
    node_b
        .write_partial(3, 1, Bytes::from_static(b"B3"))
        .await
        .unwrap();

    // Either instance reads the other instance's partials through the
    // shared object store.
    let got = node_a.read_partial(2, 1).await.unwrap().unwrap();
    assert_eq!(&got[..], b"B2");
    let got = node_b.read_partial(0, 1).await.unwrap().unwrap();
    assert_eq!(&got[..], b"A0");

    // Either instance can seal the epoch manifest — whichever gets the
    // CAS first wins and subsequent callers see the commit.
    assert!(node_a.epoch_complete(1, &[0, 1, 2, 3]).await.unwrap());
    assert!(node_b.epoch_complete(1, &[0, 1, 2, 3]).await.unwrap());

    // Partials from a later epoch: manifest absent → not complete.
    node_a
        .write_partial(0, 2, Bytes::from_static(b"A0@2"))
        .await
        .unwrap();
    assert!(!node_a.epoch_complete(2, &[0, 1, 2, 3]).await.unwrap());
}

/// Split-brain fence: a paused instance that wakes up after a rebalance
/// must not overwrite state owned by its successor.
#[tokio::test]
async fn object_store_stale_writer_is_fenced() {
    let dir = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    let old_owner = ObjectStoreBackend::new(Arc::clone(&store), "node-a", 4);
    let new_owner = ObjectStoreBackend::new(Arc::clone(&store), "node-b", 4);

    // Coordination bumps authoritative past old_owner; new_owner advances
    // its local version to match.
    let auth_a = old_owner.authoritative_version_handle();
    auth_a.store(
        2,
        std::sync::atomic::Ordering::Release,
    );
    new_owner.advance_assignment_version(2);
    let auth_b = new_owner.authoritative_version_handle();
    auth_b.store(
        2,
        std::sync::atomic::Ordering::Release,
    );

    // old_owner tries to write at its stale version → rejected.
    let err = old_owner
        .write_partial(0, 1, Bytes::from_static(b"stale"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        laminar_core::state::StateBackendError::StaleAssignment { .. }
    ));

    // new_owner writes cleanly.
    new_owner
        .write_partial(0, 1, Bytes::from_static(b"fresh"))
        .await
        .unwrap();
    let got = new_owner.read_partial(0, 1).await.unwrap().unwrap();
    assert_eq!(&got[..], b"fresh");
}
