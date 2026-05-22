//! End-to-end: a WebSocket stub serving captured Jetstream events ->
//! `FROM WEBSOCKET` source with `json.column` nested-path decoding -> a
//! materialized view -> a SUBSCRIBE portal. Verifies the firehose ingest +
//! nested-JSON decode loop the Bluesky demo relies on, with no real network.
//! Gated on the `websocket` feature.
#![cfg(feature = "websocket")]

use std::time::{Duration, Instant};

use arrow::array::{Array, ListArray, StringArray};
use futures_util::SinkExt;
use laminar_db::subscription::{PortalFrame, SubscribeStart};
use laminar_db::LaminarDB;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// Captured-shape Jetstream events. Only the three English post-creates
/// (a, b, c) should survive the stream's filter; the Japanese post (d), the
/// delete (e), and the account event (f) are dropped.
const EVENTS: &[&str] = &[
    r#"{"did":"did:plc:a","time_us":1,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","record":{"text":"the fed signals a rate cut","langs":["en"]}}}"#,
    r#"{"did":"did:plc:b","time_us":2,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","record":{"text":"$NVDA to the moon","langs":["en","ja"]}}}"#,
    r#"{"did":"did:plc:c","time_us":3,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","record":{"text":"no langs here"}}}"#,
    r#"{"did":"did:plc:d","time_us":4,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","record":{"text":"toukou","langs":["ja"]}}}"#,
    r#"{"did":"did:plc:e","time_us":5,"kind":"commit","commit":{"operation":"delete","collection":"app.bsky.feed.post"}}"#,
    r#"{"did":"did:plc:f","time_us":6,"kind":"account","account":{"active":true}}"#,
];

/// Binds an ephemeral port and serves the captured events to every client,
/// then idles so the source doesn't see a disconnect/reconnect storm.
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
async fn jetstream_websocket_decodes_into_materialized_view() {
    let port = spawn_ws_stub().await;
    let db = LaminarDB::open().unwrap();

    db.execute(&format!(
        "CREATE SOURCE bsky_jetstream_raw (\
            event_time_us BIGINT, author_did TEXT, text TEXT, langs ARRAY<TEXT>, \
            kind TEXT, operation TEXT, collection TEXT\
         ) FROM WEBSOCKET (\
            url = 'ws://127.0.0.1:{port}', format = 'json', \
            'json.column.event_time_us' = 'time_us', \
            'json.column.author_did' = 'did', \
            'json.column.text' = 'commit.record.text', \
            'json.column.langs' = 'commit.record.langs', \
            'json.column.kind' = 'kind', \
            'json.column.operation' = 'commit.operation', \
            'json.column.collection' = 'commit.collection')"
    ))
    .await
    .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW decoded AS \
         SELECT author_did, text, langs FROM bsky_jetstream_raw \
         WHERE collection = 'app.bsky.feed.post' AND operation = 'create' \
           AND (langs IS NULL OR array_has(langs, 'en'))",
    )
    .await
    .unwrap();

    // Subscribe before start() so no emitted batch is missed (Tail = new only).
    let mut portal = db
        .open_subscription("decoded", None, SubscribeStart::Tail)
        .await
        .unwrap();
    db.start().await.unwrap();

    // (author_did, text, langs) for each decoded row.
    let mut rows: Vec<(String, String, Option<Vec<String>>)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while rows.len() < 3 && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), portal.next_frame()).await {
            Ok(Some(PortalFrame::Batch(b))) => {
                let dids = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                let texts = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                let langs = b.column(2).as_any().downcast_ref::<ListArray>().unwrap();
                for i in 0..b.num_rows() {
                    let lang = (!langs.is_null(i)).then(|| {
                        let v = langs.value(i);
                        let s = v.as_any().downcast_ref::<StringArray>().unwrap();
                        (0..s.len()).map(|j| s.value(j).to_string()).collect()
                    });
                    rows.push((dids.value(i).to_string(), texts.value(i).to_string(), lang));
                }
            }
            Ok(Some(_)) => {}        // barrier / lagged
            Ok(None) => break,       // portal closed
            Err(_) => {}             // poll timeout; keep waiting
        }
    }

    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let dids: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(dids, ["did:plc:a", "did:plc:b", "did:plc:c"], "only en post-creates survive");

    // Nested text path + nested langs array decode (the json.column + List path).
    assert_eq!(rows[0].1, "the fed signals a rate cut");
    assert_eq!(rows[0].2.as_deref(), Some(["en".to_string()].as_slice()));
    assert_eq!(rows[1].2, Some(vec!["en".to_string(), "ja".to_string()]));
    assert_eq!(rows[2].2, None, "absent langs decodes to NULL, not an empty list");
}
