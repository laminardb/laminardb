//! Smoke tests for the public state-backend API: load a config, call
//! `build()`, exercise the resulting `Arc<dyn StateBackend>`.

use std::sync::Arc;

use bytes::Bytes;
use laminar_core::state::{ObjectStoreBackend, StateBackend, StateBackendConfig};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::tempdir;

#[tokio::test]
async fn config_roundtrip_in_process_local_object_store() {
    let c = StateBackendConfig::in_process();
    let b = c.build().await.unwrap();
    b.write_partial(0, 1, 0, Bytes::from_static(b"a"))
        .await
        .unwrap();
    assert_eq!(&b.read_partial(0, 1).await.unwrap().unwrap()[..], b"a");

    let dir = tempdir().unwrap();
    let c = StateBackendConfig::local(dir.path());
    let b = c.build().await.unwrap();
    b.write_partial(0, 1, 0, Bytes::from_static(b"b"))
        .await
        .unwrap();
    assert_eq!(&b.read_partial(0, 1).await.unwrap().unwrap()[..], b"b");

    let dir = tempdir().unwrap();
    let url = format!(
        "file://{}",
        dir.path().display().to_string().replace('\\', "/")
    );
    let c = StateBackendConfig::object_store(url, "node-0");
    let b = c.build().await.unwrap();
    b.write_partial(0, 1, 0, Bytes::from_static(b"c"))
        .await
        .unwrap();
    assert_eq!(&b.read_partial(0, 1).await.unwrap().unwrap()[..], b"c");
}

/// Two instances writing to a shared object store root: partials are
/// visible cross-instance and the commit marker is CAS-sealed so only
/// one committer wins.
///
/// Before the split-brain commit fix, both nodes' `epoch_complete`
/// calls returned `Ok(true)` — the second swallowed `AlreadyExists`
/// as success and both leaders would have proceeded to commit sinks.
/// Now the loser gets `SplitBrainCommit` and must abort.
#[tokio::test]
async fn distributed_embedded_static_two_instances_shared_store() {
    use laminar_core::state::StateBackendError;

    let dir = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    let node_a = ObjectStoreBackend::new(Arc::clone(&store), "node-a", 4);
    let node_b = ObjectStoreBackend::new(Arc::clone(&store), "node-b", 4);

    node_a
        .write_partial(0, 1, 0, Bytes::from_static(b"A0"))
        .await
        .unwrap();
    node_a
        .write_partial(1, 1, 0, Bytes::from_static(b"A1"))
        .await
        .unwrap();
    node_b
        .write_partial(2, 1, 0, Bytes::from_static(b"B2"))
        .await
        .unwrap();
    node_b
        .write_partial(3, 1, 0, Bytes::from_static(b"B3"))
        .await
        .unwrap();

    assert_eq!(
        &node_a.read_partial(2, 1).await.unwrap().unwrap()[..],
        b"B2"
    );
    assert_eq!(
        &node_b.read_partial(0, 1).await.unwrap().unwrap()[..],
        b"A0"
    );

    // node_a wins the CAS — it committed the epoch.
    assert!(node_a.epoch_complete(1, &[0, 1, 2, 3]).await.unwrap());
    // node_a calling again is idempotent (same committer id in the
    // audit body), so it still gets Ok(true).
    assert!(node_a.epoch_complete(1, &[0, 1, 2, 3]).await.unwrap());

    // node_b loses — it must not keep driving the commit phase.
    let err = node_b.epoch_complete(1, &[0, 1, 2, 3]).await.unwrap_err();
    match err {
        StateBackendError::SplitBrainCommit { committer, self_id } => {
            assert_eq!(committer, "node-a");
            assert_eq!(self_id, "node-b");
        }
        other => panic!("expected SplitBrainCommit, got {other:?}"),
    }

    node_a
        .write_partial(0, 2, 0, Bytes::from_static(b"A0@2"))
        .await
        .unwrap();
    assert!(!node_a.epoch_complete(2, &[0, 1, 2, 3]).await.unwrap());
}
