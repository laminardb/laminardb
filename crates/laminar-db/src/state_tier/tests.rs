use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::oneshot;

use super::{spawn_worker, StateTierStore, TierRequest, MARKER_FILE};

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
fn remove_operator_only_touches_that_prefix() {
    let (_tmp, dir) = tier_dir();
    let store = StateTierStore::open(&dir, None).unwrap();

    store.put("agg_a", 1, b"a1").unwrap();
    store.put("agg_a", 2, b"a2").unwrap();
    // Prefix-collision probe: "agg_a" must not match "agg_ab".
    store.put("agg_ab", 1, b"ab1").unwrap();

    let removed = store.remove_operator("agg_a").unwrap();
    assert_eq!(removed, 2);
    assert_eq!(store.get("agg_a", 1).unwrap(), None);
    assert_eq!(store.get("agg_a", 2).unwrap(), None);
    assert_eq!(store.get("agg_ab", 1).unwrap().unwrap().as_ref(), b"ab1");
    assert_eq!(store.logical_slices(), 1);
}

#[test]
fn clean_shutdown_reuses_and_restores_counters() {
    let (_tmp, dir) = tier_dir();
    let (slices, bytes);
    {
        let store = StateTierStore::open(&dir, None).unwrap();
        store.put("agg", 1, b"keep-me").unwrap();
        store.put("agg", 2, b"and-me").unwrap();
        slices = store.logical_slices();
        bytes = store.logical_bytes();
        store.shutdown().unwrap();
    }
    let store = StateTierStore::open(&dir, None).unwrap();
    assert_eq!(store.get("agg", 1).unwrap().unwrap().as_ref(), b"keep-me");
    assert_eq!(store.logical_slices(), slices);
    assert_eq!(store.logical_bytes(), bytes);
}

#[test]
fn unclean_shutdown_wipes() {
    let (_tmp, dir) = tier_dir();
    {
        let store = StateTierStore::open(&dir, None).unwrap();
        store.put("agg", 1, b"doomed").unwrap();
        // No shutdown(): the marker still says clean=false.
    }
    let store = StateTierStore::open(&dir, None).unwrap();
    assert_eq!(store.get("agg", 1).unwrap(), None, "unclean tier must wipe");
    assert_eq!(store.logical_slices(), 0);
}

#[test]
fn engine_version_mismatch_wipes() {
    let (_tmp, dir) = tier_dir();
    {
        let store = StateTierStore::open(&dir, None).unwrap();
        store.put("agg", 1, b"old-engine").unwrap();
        store.shutdown().unwrap();
    }
    // Forge a clean marker from a different engine version.
    let text = std::fs::read_to_string(dir.join(MARKER_FILE)).unwrap();
    let forged = text.replace(
        &format!("engine={}", env!("CARGO_PKG_VERSION")),
        "engine=0.0.0-other",
    );
    assert_ne!(text, forged, "test must actually change the version");
    std::fs::write(dir.join(MARKER_FILE), forged).unwrap();

    let store = StateTierStore::open(&dir, None).unwrap();
    assert_eq!(store.get("agg", 1).unwrap(), None);
}

#[test]
fn corrupt_marker_wipes() {
    let (_tmp, dir) = tier_dir();
    {
        let store = StateTierStore::open(&dir, None).unwrap();
        store.put("agg", 1, b"x").unwrap();
        store.shutdown().unwrap();
    }
    std::fs::write(dir.join(MARKER_FILE), "not a marker").unwrap();
    let store = StateTierStore::open(&dir, None).unwrap();
    assert_eq!(store.get("agg", 1).unwrap(), None);
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

#[tokio::test]
async fn worker_fetches_under_concurrent_demotes() {
    let (_tmp, dir) = tier_dir();
    let store = Arc::new(StateTierStore::open(&dir, None).unwrap());
    let tx = spawn_worker(&tokio::runtime::Handle::current(), store, 256);

    let op: Arc<str> = Arc::from("agg");
    // Seed 32 slices.
    for v in 0..32u32 {
        let (reply, rx) = oneshot::channel();
        tx.send(TierRequest::Demote {
            operator: Arc::clone(&op),
            vnode: v,
            bytes: Bytes::from(vec![u8::try_from(v).unwrap(); 64]),
            reply,
        })
        .await
        .unwrap();
        rx.await.unwrap().unwrap();
    }

    // Interleave rewrites and fetches; every fetch must observe a complete
    // value (one of the two versions, never torn or missing).
    let writer = {
        let tx = tx.clone();
        let op = Arc::clone(&op);
        tokio::spawn(async move {
            for round in 0..4u8 {
                for v in 0..32u32 {
                    let (reply, rx) = oneshot::channel();
                    tx.send(TierRequest::Demote {
                        operator: Arc::clone(&op),
                        vnode: v,
                        bytes: Bytes::from(vec![round, 64]),
                        reply,
                    })
                    .await
                    .unwrap();
                    rx.await.unwrap().unwrap();
                }
            }
        })
    };
    let reader = {
        let tx = tx.clone();
        let op = Arc::clone(&op);
        tokio::spawn(async move {
            for _ in 0..4 {
                for v in 0..32u32 {
                    let (reply, rx) = oneshot::channel();
                    tx.send(TierRequest::Fetch {
                        operator: Arc::clone(&op),
                        vnode: v,
                        reply,
                    })
                    .await
                    .unwrap();
                    let got = rx.await.unwrap().unwrap();
                    let got = got.expect("slice must stay resident under rewrites");
                    assert!(got.len() == 64 || got.len() == 2, "torn value: {got:?}");
                }
            }
        })
    };
    writer.await.unwrap();
    reader.await.unwrap();
}
