use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;

use super::{spawn_worker, StateTierStore, TierRequest};

fn tier_dir() -> (tempfile::TempDir, std::path::PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("state-tier");
    (tmp, dir)
}

#[test]
fn round_trip_overwrite_and_remove() {
    let (_tmp, dir) = tier_dir();
    let store = StateTierStore::open(&dir, None).unwrap();

    assert_eq!(store.get("agg", 7).unwrap(), None);
    store.put("agg", 7, b"slice-v1").unwrap();
    assert_eq!(store.get("agg", 7).unwrap().unwrap().as_ref(), b"slice-v1");
    assert_eq!(store.logical_slices(), 1);
    let bytes_v1 = store.logical_bytes();
    assert!(bytes_v1 > 0);

    // Overwrite adjusts bytes, not the slice count.
    store.put("agg", 7, b"slice-v2-longer").unwrap();
    assert_eq!(
        store.get("agg", 7).unwrap().unwrap().as_ref(),
        b"slice-v2-longer"
    );
    assert_eq!(store.logical_slices(), 1);
    assert!(store.logical_bytes() > bytes_v1);

    store.remove("agg", 7).unwrap();
    assert_eq!(store.get("agg", 7).unwrap(), None);
    assert_eq!(store.logical_slices(), 0);
    assert_eq!(store.logical_bytes(), 0);

    // Removing an absent slice is a no-op, not an error or a negative count.
    store.remove("agg", 7).unwrap();
    assert_eq!(store.logical_slices(), 0);
}

#[test]
fn open_wipes_leftover_dir() {
    let (_tmp, dir) = tier_dir();
    {
        let store = StateTierStore::open(&dir, None).unwrap();
        store.put("agg", 1, b"doomed").unwrap();
    }
    // The tier never survives a restart — reopening starts empty.
    let store = StateTierStore::open(&dir, None).unwrap();
    assert_eq!(store.get("agg", 1).unwrap(), None);
    assert_eq!(store.logical_slices(), 0);
}

#[tokio::test]
async fn worker_demote_fetch_drop() {
    let (_tmp, dir) = tier_dir();
    let store = Arc::new(StateTierStore::open(&dir, None).unwrap());
    let tx = spawn_worker(&tokio::runtime::Handle::current(), store, 16);

    let op: Arc<str> = Arc::from("agg");
    let (reply, rx) = oneshot::channel();
    tx.try_send(TierRequest::Demote {
        operator: Arc::clone(&op),
        vnode: 3,
        bytes: Bytes::from_static(b"demoted"),
        reply,
    })
    .unwrap();
    rx.await.unwrap().unwrap();

    let (reply, rx) = oneshot::channel();
    tx.try_send(TierRequest::Fetch {
        operator: Arc::clone(&op),
        vnode: 3,
        reply,
    })
    .unwrap();
    assert_eq!(rx.await.unwrap().unwrap().unwrap().as_ref(), b"demoted");

    let (reply, rx) = oneshot::channel();
    tx.try_send(TierRequest::Drop {
        operator: Arc::clone(&op),
        vnode: 3,
        reply,
    })
    .unwrap();
    rx.await.unwrap().unwrap();

    let (reply, rx) = oneshot::channel();
    tx.try_send(TierRequest::Fetch {
        operator: op,
        vnode: 3,
        reply,
    })
    .unwrap();
    assert_eq!(rx.await.unwrap().unwrap(), None);
}
