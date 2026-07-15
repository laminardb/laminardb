//! End-to-end: a WebSocket stub serving nested-JSON events -> `FROM WEBSOCKET`
//! source with `json.column` dot-path decoding (including a nested string
//! array) -> a materialized view -> a SUBSCRIBE portal. Verifies the
//! websocket-ingest + nested-decode loop with no real network. Gated on the
//! `websocket` feature.
#![cfg(feature = "websocket")]

use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray};
use futures_util::SinkExt;
use laminar_db::subscription::{PortalFrame, SubscribeStart, SubscriptionPortal};
use laminar_db::LaminarDB;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// Nested-JSON events. Only the two open orders (1, 2) survive the view's
/// filter; the closed order (3) and the heartbeat (4) are dropped.
const EVENTS: &[&str] = &[
    r#"{"id":1,"kind":"order","meta":{"region":"us","status":"open"},"tags":["a","b"]}"#,
    r#"{"id":2,"kind":"order","meta":{"region":"eu"},"tags":["b"]}"#,
    r#"{"id":3,"kind":"order","meta":{"region":"us","status":"closed"},"tags":["a"]}"#,
    r#"{"id":4,"kind":"heartbeat","meta":{"region":"us"},"tags":[]}"#,
];

/// Binds an ephemeral port, serves one stable client, then idles so the source
/// doesn't see a disconnect/reconnect storm.
async fn spawn_ws_stub() -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
            return;
        };
        for event in EVENTS {
            if ws.send(Message::Text((*event).into())).await.is_err() {
                return;
            }
        }
        std::future::pending::<()>().await;
    });
    (port, task)
}

async fn spawn_controlled_ws_stub() -> (
    u16,
    tokio::sync::mpsc::Sender<String>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(16);
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(event) = rx.recv().await {
            if ws.send(Message::Text(event.into())).await.is_err() {
                break;
            }
        }
    });
    (port, tx, task)
}

async fn collect_until_id(
    portal: &mut SubscriptionPortal,
    wanted: i64,
    rows: &mut Vec<(i64, i64)>,
) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match portal.next_frame().await {
                Some(PortalFrame::Batch { batch, .. }) => {
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let timestamps = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        rows.push((ids.value(row), timestamps.value(row)));
                    }
                    if rows.iter().any(|(id, _)| *id == wanted) {
                        return;
                    }
                }
                Some(_) => {}
                None => panic!("subscription closed before id {wanted}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for id {wanted}"));
}

#[tokio::test]
async fn websocket_source_decodes_nested_json_into_materialized_view() {
    let (port, server) = spawn_ws_stub().await;
    let db = LaminarDB::open().unwrap();

    db.execute(&format!(
        "CREATE SOURCE feed (\
            id BIGINT, kind TEXT, region TEXT, status TEXT, tags ARRAY<TEXT>\
         ) FROM WEBSOCKET (\
            url = 'ws://127.0.0.1:{port}', format = 'json', \
            'json.column.region' = 'meta.region', \
            'json.column.status' = 'meta.status', \
            'json.column.tags'   = 'tags')"
    ))
    .await
    .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW decoded AS \
         SELECT id, region, tags FROM feed \
         WHERE kind = 'order' AND (status IS NULL OR status = 'open')",
    )
    .await
    .unwrap();

    // Subscribe before start() so no emitted batch is missed (Tail = new only).
    let mut portal = db
        .open_subscription("decoded", None, SubscribeStart::Tail)
        .await
        .unwrap();
    db.start().await.unwrap();

    // (id, region, tags) for each decoded row.
    let mut rows: Vec<(i64, String, Vec<String>)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while rows.len() < 2 && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), portal.next_frame()).await {
            Ok(Some(PortalFrame::Batch { batch: b, .. })) => {
                let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let regions = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                let tags = b.column(2).as_any().downcast_ref::<ListArray>().unwrap();
                for i in 0..b.num_rows() {
                    let v = tags.value(i);
                    let s = v.as_any().downcast_ref::<StringArray>().unwrap();
                    let row_tags = (0..s.len()).map(|j| s.value(j).to_string()).collect();
                    rows.push((ids.value(i), regions.value(i).to_string(), row_tags));
                }
            }
            Ok(Some(_)) => {}  // barrier / lagged
            Ok(None) => break, // portal closed
            Err(_) => {}       // poll timeout; keep waiting
        }
    }

    db.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;

    rows.sort_by_key(|r| r.0);
    assert_eq!(rows.len(), 2, "only open orders survive the filter");
    // Nested dot-path decode (meta.region) + nested string-array decode (tags).
    assert_eq!(
        rows[0],
        (1, "us".to_string(), vec!["a".to_string(), "b".to_string()])
    );
    assert_eq!(rows[1], (2, "eu".to_string(), vec!["b".to_string()]));
}

#[tokio::test]
async fn websocket_event_time_uses_typed_json_and_sql_watermark() {
    const BASE_US: i64 = 1_700_000_000_000_000;

    let (port, events, server) = spawn_controlled_ws_stub().await;
    let db = LaminarDB::open().unwrap();
    db.execute(&format!(
        "CREATE SOURCE timed_feed (
            id BIGINT, ts TIMESTAMP,
            WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
         ) FROM WEBSOCKET (
            url = 'ws://127.0.0.1:{port}', format = 'json',
            'json.column.ts' = 'meta.time_us',
            'json.column.ts.epoch_unit' = 'micros')"
    ))
    .await
    .unwrap();
    db.execute("CREATE MATERIALIZED VIEW timed_out AS SELECT id, ts FROM timed_feed")
        .await
        .unwrap();

    let mut portal = db
        .open_subscription("timed_out", None, SubscribeStart::Tail)
        .await
        .unwrap();
    db.start().await.unwrap();

    let mut rows = Vec::new();
    events
        .send(format!(r#"{{"id":1,"meta":{{"time_us":{BASE_US}}}}}"#))
        .await
        .unwrap();
    collect_until_id(&mut portal, 1, &mut rows).await;

    let phase_two_us = BASE_US + 10_000_000;
    events
        .send(format!(r#"{{"id":2,"meta":{{"time_us":{phase_two_us}}}}}"#))
        .await
        .unwrap();
    collect_until_id(&mut portal, 2, &mut rows).await;
    let phase_two_watermark = db.pipeline_watermark();

    let late_us = BASE_US + 5_000_000;
    events
        .send(format!(r#"{{"id":3,"meta":{{"time_us":{late_us}}}}}"#))
        .await
        .unwrap();
    let sentinel_us = BASE_US + 11_000_000;
    events
        .send(format!(r#"{{"id":4,"meta":{{"time_us":{sentinel_us}}}}}"#))
        .await
        .unwrap();
    collect_until_id(&mut portal, 4, &mut rows).await;
    let final_watermark = db.pipeline_watermark();

    db.shutdown().await.unwrap();
    drop(events);
    server.abort();
    let _ = server.await;

    rows.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![(1, BASE_US), (2, phase_two_us), (4, sentinel_us)]
    );
    assert_eq!(phase_two_watermark, BASE_US / 1000 + 9_000);
    assert_eq!(final_watermark, BASE_US / 1000 + 10_000);
}

#[tokio::test]
async fn websocket_removed_event_time_options_fail_before_network_io() {
    for (key, value) in [
        ("event.time.field", "ts"),
        ("event.time.format", "epoch_millis"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let db = LaminarDB::open().unwrap();
        db.execute(&format!(
            "CREATE SOURCE rejected_{key_suffix} (id BIGINT, ts TIMESTAMP) \
             FROM WEBSOCKET (url = 'ws://127.0.0.1:{port}', '{key}' = '{value}')",
            key_suffix = key.replace('.', "_")
        ))
        .await
        .unwrap();
        let error = db.start().await.unwrap_err().to_string();
        let connection = tokio::time::timeout(Duration::from_millis(250), listener.accept()).await;
        db.shutdown().await.unwrap();

        assert!(error.contains(key), "{error}");
        assert!(error.contains("WATERMARK FOR"), "{error}");
        assert!(
            connection.is_err(),
            "removed option {key} reached WebSocket network I/O before admission failed"
        );
    }
}
