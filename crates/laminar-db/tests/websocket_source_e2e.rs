//! End-to-end: a WebSocket stub serving nested-JSON events -> `FROM WEBSOCKET`
//! source with `json.column` dot-path decoding (including a nested string
//! array) -> a materialized view -> a SUBSCRIBE portal. Verifies the
//! websocket-ingest + nested-decode loop with no real network. Gated on the
//! `websocket` feature.
#![cfg(feature = "websocket")]

use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, ListArray, StringArray};
use futures_util::SinkExt;
use laminar_db::subscription::{PortalFrame, SubscribeStart};
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

/// Binds an ephemeral port and serves the events to every client, then idles
/// so the source doesn't see a disconnect/reconnect storm.
async fn spawn_ws_stub() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                for event in EVENTS {
                    if ws.send(Message::Text((*event).into())).await.is_err() {
                        return;
                    }
                }
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            });
        }
    });
    port
}

#[tokio::test]
async fn websocket_source_decodes_nested_json_into_materialized_view() {
    let port = spawn_ws_stub().await;
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
            Ok(Some(PortalFrame::Batch(b))) => {
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

    rows.sort_by_key(|r| r.0);
    assert_eq!(rows.len(), 2, "only open orders survive the filter");
    // Nested dot-path decode (meta.region) + nested string-array decode (tags).
    assert_eq!(rows[0], (1, "us".to_string(), vec!["a".to_string(), "b".to_string()]));
    assert_eq!(rows[1], (2, "eu".to_string(), vec!["b".to_string()]));
}
