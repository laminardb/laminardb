//! RabbitMQ through the public SQL path: `CREATE SOURCE ... FROM RABBITMQ`, a stream, and
//! `CREATE SINK ... INTO RABBITMQ`, with checkpointed at-least-once delivery.
//!
//! Needs `tests/docker/compose.yml` service `rabbitmq`; skips when the broker is unreachable
//! unless `LAMINAR_REQUIRE_RABBITMQ=1`.

#![cfg(feature = "rabbitmq")]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use laminar_db::{DeliveryGuarantee, LaminarDB};
use lapin::options::{
    BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};

/// Broker endpoint without credentials: the durable catalog accepts secrets only as references.
const ENDPOINT: &str = "amqp://127.0.0.1:5672/%2f";
const USER: &str = "guest";
const PASSWORD: &str = "guest";
/// Connector options shared by every source and sink of these tests.
const CREDENTIALS: &str = "'username' = 'guest', 'password' = '${E2E_RABBITMQ_PASSWORD}'";
static NEXT: AtomicU64 = AtomicU64::new(0);

fn unique(name: &str) -> String {
    format!(
        "laminar.e2e.{name}.{}.{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn broker() -> Option<(String, Channel)> {
    let url = ENDPOINT.to_string();
    let admin = format!("amqp://{USER}:{PASSWORD}@127.0.0.1:5672/%2f");
    let connected = tokio::time::timeout(
        Duration::from_secs(3),
        Connection::connect(&admin, ConnectionProperties::default()),
    )
    .await;
    match connected {
        Ok(Ok(connection)) => {
            let channel = connection.create_channel().await.expect("channel");
            // The channel keeps its connection alive for the test.
            std::mem::forget(connection);
            Some((url, channel))
        }
        _ if std::env::var("LAMINAR_REQUIRE_RABBITMQ").is_ok_and(|v| v == "1") => {
            panic!("RabbitMQ is required but {url} is unreachable")
        }
        _ => {
            eprintln!("skipping: RabbitMQ not reachable");
            None
        }
    }
}

async fn open(storage: &Path, delivery: DeliveryGuarantee) -> std::sync::Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(delivery)
        .config_var("E2E_RABBITMQ_PASSWORD", PASSWORD)
        .build()
        .await
        .expect("open database")
}

async fn queue(channel: &Channel, name: &str) {
    channel
        .queue_declare(
            name.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare");
}

async fn delete(channel: &Channel, names: &[&str]) {
    for name in names {
        channel
            .queue_delete((*name).into(), QueueDeleteOptions::default())
            .await
            .expect("delete queue");
    }
}

async fn publish(channel: &Channel, queue: &str, payloads: impl IntoIterator<Item = Vec<u8>>) {
    for payload in payloads {
        channel
            .basic_publish(
                "".into(),
                queue.into(),
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .expect("publish");
    }
}

async fn ready(channel: &Channel, queue: &str) -> u32 {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare")
        .message_count()
}

/// Consume until `predicate` holds over everything received, or fail after `wait`.
async fn drain_until(
    channel: &Channel,
    queue: &str,
    wait: Duration,
    predicate: impl Fn(&[Vec<u8>]) -> bool,
) -> Vec<Vec<u8>> {
    let mut consumer = channel
        .basic_consume(
            queue.into(),
            "".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let deadline = tokio::time::Instant::now() + wait;
    let mut received = Vec::new();
    while !predicate(&received) {
        let next = tokio::time::timeout_at(deadline, consumer.next())
            .await
            .unwrap_or_else(|_| panic!("timed out with {} messages from {queue}", received.len()));
        received.push(next.expect("consumer open").expect("delivery").data);
    }
    channel
        .basic_cancel(
            consumer.tag(),
            lapin::options::BasicCancelOptions::default(),
        )
        .await
        .expect("cancel");
    received
}

fn json_ids(messages: &[Vec<u8>]) -> BTreeMap<i64, usize> {
    let mut counts = BTreeMap::new();
    for message in messages {
        let value: serde_json::Value = serde_json::from_slice(message).expect("json output");
        assert_eq!(
            value["doubled"],
            value["id"].as_i64().unwrap() * 2,
            "{value}"
        );
        *counts.entry(value["id"].as_i64().unwrap()).or_insert(0) += 1;
    }
    counts
}

async fn json_pipeline(db: &LaminarDB, url: &str, input: &str, output: &str) {
    db.execute(&format!(
        "CREATE SOURCE events (id BIGINT, label VARCHAR) FROM RABBITMQ (\
             'url' = '{url}', {CREDENTIALS}, 'queue' = '{input}', 'prefetch.count' = '500') \
         FORMAT JSON"
    ))
    .await
    .expect("create source");
    db.execute("CREATE STREAM doubled AS SELECT id, id * 2 AS doubled, label FROM events")
        .await
        .expect("create stream");
    db.execute(&format!(
        "CREATE SINK out FROM doubled INTO RABBITMQ (\
             'url' = '{url}', {CREDENTIALS}, 'routing.key' = '{output}') FORMAT JSON"
    ))
    .await
    .expect("create sink");
}

fn json_events(ids: std::ops::Range<i64>) -> impl Iterator<Item = Vec<u8>> {
    ids.map(|id| format!(r#"{{"id":{id},"label":"e{id}"}}"#).into_bytes())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_roundtrip_acknowledges_input_after_checkpoint() {
    let Some((url, channel)) = broker().await else {
        return;
    };
    let (input, output) = (unique("json.in"), unique("json.out"));
    queue(&channel, &input).await;
    queue(&channel, &output).await;
    publish(&channel, &input, json_events(0..200)).await;

    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path(), DeliveryGuarantee::AtLeastOnce).await;
    json_pipeline(&db, &url, &input, &output).await;
    db.start().await.expect("start");
    let received = drain_until(&channel, &output, Duration::from_secs(30), |seen| {
        seen.len() >= 200
    })
    .await;
    assert_eq!(json_ids(&received), (0..200).map(|id| (id, 1)).collect());
    // Before a checkpoint every input delivery is still owned (unacknowledged) by the source.
    assert_eq!(ready(&channel, &input).await, 0);
    let checkpoint = db.checkpoint().await.expect("checkpoint");
    assert!(checkpoint.success, "{checkpoint:?}");
    db.shutdown().await.expect("shutdown");
    // Acknowledged inputs are gone; nothing was requeued at shutdown.
    assert_eq!(ready(&channel, &input).await, 0);
    delete(&channel, &[&input, &output]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncheckpointed_input_is_redelivered_after_restart_without_loss() {
    let Some((url, channel)) = broker().await else {
        return;
    };
    let (input, output) = (unique("restart.in"), unique("restart.out"));
    queue(&channel, &input).await;
    queue(&channel, &output).await;
    let storage = tempfile::tempdir().unwrap();

    publish(&channel, &input, json_events(0..100)).await;
    {
        let db = open(storage.path(), DeliveryGuarantee::AtLeastOnce).await;
        json_pipeline(&db, &url, &input, &output).await;
        db.start().await.expect("start");
        drain_until(&channel, &output, Duration::from_secs(30), |seen| {
            seen.len() >= 100
        })
        .await;
        assert!(db.checkpoint().await.expect("checkpoint").success);
        // Processed after the last checkpoint: the source keeps these unacknowledged.
        publish(&channel, &input, json_events(100..150)).await;
        drain_until(&channel, &output, Duration::from_secs(30), |seen| {
            seen.len() >= 50
        })
        .await;
        db.shutdown().await.expect("shutdown");
    }
    assert_eq!(
        ready(&channel, &input).await,
        50,
        "uncommitted deliveries must be requeued"
    );
    publish(&channel, &input, json_events(150..200)).await;
    {
        let db = open(storage.path(), DeliveryGuarantee::AtLeastOnce).await;
        json_pipeline(&db, &url, &input, &output).await;
        db.start().await.expect("restart");
        let replayed = drain_until(&channel, &output, Duration::from_secs(30), |seen| {
            let ids = json_ids(seen);
            (100..200).all(|id| ids.contains_key(&id))
        })
        .await;
        // At-least-once: every input after the committed cut arrives; ids 100..150 were output
        // before the restart and are replayed (duplicates), none is lost.
        let ids = json_ids(&replayed);
        assert!(ids.keys().all(|id| (100..200).contains(id)), "{ids:?}");
        assert!(db.checkpoint().await.expect("checkpoint").success);
        db.shutdown().await.expect("shutdown");
    }
    assert_eq!(ready(&channel, &input).await, 0);
    delete(&channel, &[&input, &output]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_binary_payloads_round_trip_byte_for_byte() {
    let Some((url, channel)) = broker().await else {
        return;
    };
    let (input, output) = (unique("bin.in"), unique("bin.out"));
    queue(&channel, &input).await;
    queue(&channel, &output).await;
    let payloads: Vec<Vec<u8>> = (0..50_u8)
        .map(|i| vec![0xff, 0x00, i, 0xc3, 0x28, 0x80, i.wrapping_mul(7)])
        .collect();
    publish(&channel, &input, payloads.clone()).await;

    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path(), DeliveryGuarantee::AtLeastOnce).await;
    db.execute(&format!(
        "CREATE SOURCE blobs (value BYTEA) FROM RABBITMQ (\
             'url' = '{url}', {CREDENTIALS}, 'queue' = '{input}') FORMAT RAW"
    ))
    .await
    .expect("create source");
    db.execute("CREATE STREAM passthrough AS SELECT value FROM blobs")
        .await
        .expect("create stream");
    db.execute(&format!(
        "CREATE SINK copied FROM passthrough INTO RABBITMQ (\
             'url' = '{url}', {CREDENTIALS}, 'routing.key' = '{output}') FORMAT RAW"
    ))
    .await
    .expect("create sink");
    db.start().await.expect("start");
    let mut received = drain_until(&channel, &output, Duration::from_secs(30), |seen| {
        seen.len() >= 50
    })
    .await;
    received.sort();
    let mut expected = payloads;
    expected.sort();
    assert_eq!(received, expected);
    assert!(db.checkpoint().await.expect("checkpoint").success);
    db.shutdown().await.expect("shutdown");
    assert_eq!(ready(&channel, &input).await, 0);
    delete(&channel, &[&input, &output]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_requires_checkpointed_delivery() {
    let Some((url, channel)) = broker().await else {
        return;
    };
    let input = unique("besteffort.in");
    queue(&channel, &input).await;
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path(), DeliveryGuarantee::BestEffort).await;
    db.execute(&format!(
        "CREATE SOURCE events (id BIGINT) FROM RABBITMQ (\
             'url' = '{url}', {CREDENTIALS}, 'queue' = '{input}') FORMAT JSON"
    ))
    .await
    .expect("create source");
    db.execute("CREATE STREAM s AS SELECT id FROM events")
        .await
        .unwrap();
    let error = db
        .start()
        .await
        .expect_err("best-effort must reject a commit-coupled source");
    assert!(error.to_string().contains("commit-coupled"), "{error}");
    delete(&channel, &[&input]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_columns_survive_a_restart_with_the_same_storage() {
    let Some((url, channel)) = broker().await else {
        return;
    };
    let (input, output) = (unique("meta.in"), unique("meta.out"));
    queue(&channel, &input).await;
    queue(&channel, &output).await;
    let storage = tempfile::tempdir().unwrap();
    let ddl = |input: &str, output: &str| {
        [
            format!(
                "CREATE SOURCE tagged (id BIGINT) FROM RABBITMQ ('url' = '{url}', {CREDENTIALS}, \
                 'queue' = '{input}', 'metadata.columns' = 'routing_key,redelivered,message_id') \
                 FORMAT JSON"
            ),
            "CREATE STREAM routed AS SELECT id, _routing_key AS routed_by, _redelivered AS again \
             FROM tagged"
                .to_string(),
            format!(
                "CREATE SINK routed_out FROM routed INTO RABBITMQ ('url' = '{url}', {CREDENTIALS}, \
                 'routing.key' = '{output}') FORMAT JSON"
            ),
        ]
    };
    for (round, ids) in [(0, 0..20_i64), (1, 20..40)] {
        let db = open(storage.path(), DeliveryGuarantee::AtLeastOnce).await;
        for statement in ddl(&input, &output) {
            db.execute(&statement)
                .await
                .unwrap_or_else(|error| panic!("round {round}: {statement}: {error}"));
        }
        db.start()
            .await
            .unwrap_or_else(|error| panic!("round {round} start: {error}"));
        publish(
            &channel,
            &input,
            ids.clone()
                .map(|id| format!(r#"{{"id":{id}}}"#).into_bytes()),
        )
        .await;
        let received = drain_until(&channel, &output, Duration::from_secs(30), |seen| {
            seen.len() >= 20
        })
        .await;
        for message in &received {
            let value: serde_json::Value = serde_json::from_slice(message).unwrap();
            assert_eq!(value["routed_by"], input.as_str(), "{value}");
            assert!(ids.contains(&value["id"].as_i64().unwrap()), "{value}");
        }
        assert!(db.checkpoint().await.expect("checkpoint").success);
        db.shutdown().await.expect("shutdown");
    }
    assert_eq!(ready(&channel, &input).await, 0);
    delete(&channel, &[&input, &output]).await;
}

#[cfg(feature = "fpml")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fpml_documents_feed_sql_across_a_restart() {
    const CORPUS: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../laminar-connectors/tests/fixtures/fpml/fpml-5-13-8-rec-2/confirmation/"
    );
    let Some((url, channel)) = broker().await else {
        return;
    };
    let (input, output) = (unique("fpml.in"), unique("fpml.out"));
    queue(&channel, &input).await;
    queue(&channel, &output).await;
    let storage = tempfile::tempdir().unwrap();
    let ddl = [
        format!(
            "CREATE SOURCE confirmations FROM RABBITMQ ('url' = '{url}', {CREDENTIALS}, \
             'queue' = '{input}', 'metadata.columns' = 'message_id') FORMAT FPML"
        ),
        "CREATE STREAM roots AS SELECT document_root, _message_id AS id FROM confirmations"
            .to_string(),
        format!(
            "CREATE SINK roots_out FROM roots INTO RABBITMQ ('url' = '{url}', {CREDENTIALS}, \
             'routing.key' = '{output}') FORMAT JSON"
        ),
    ];
    let rounds = [
        vec![
            (
                "swap",
                "products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml",
            ),
            (
                "clearing",
                "business-processes/clearing/msg-ex10-clearingConfirmed-clearing-performed.xml",
            ),
        ],
        vec![(
            "allocation",
            "business-processes/allocation/msg-ex20-cds-request-allocation.xml",
        )],
    ];
    let expected = [
        BTreeMap::from([("swap", "dataDocument"), ("clearing", "clearingConfirmed")]),
        BTreeMap::from([("allocation", "requestAllocation")]),
    ];
    for (round, documents) in rounds.iter().enumerate() {
        let db = open(storage.path(), DeliveryGuarantee::AtLeastOnce).await;
        for statement in &ddl {
            db.execute(statement)
                .await
                .unwrap_or_else(|error| panic!("round {round}: {statement}: {error}"));
        }
        db.start()
            .await
            .unwrap_or_else(|error| panic!("round {round} start: {error}"));
        for (id, path) in documents {
            let body = std::fs::read(format!("{CORPUS}{path}")).expect("corpus document");
            channel
                .basic_publish(
                    "".into(),
                    input.as_str().into(),
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default()
                        .with_delivery_mode(2)
                        .with_message_id((*id).into()),
                )
                .await
                .expect("publish");
        }
        let received = drain_until(&channel, &output, Duration::from_secs(30), |seen| {
            seen.len() >= documents.len()
        })
        .await;
        let roots: BTreeMap<String, String> = received
            .iter()
            .map(|message| {
                let value: serde_json::Value = serde_json::from_slice(message).unwrap();
                (
                    value["id"].as_str().unwrap().to_string(),
                    value["document_root"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        let wanted: BTreeMap<String, String> = expected[round]
            .iter()
            .map(|(id, root)| ((*id).to_string(), (*root).to_string()))
            .collect();
        assert_eq!(roots, wanted, "round {round}; fault {:?}", db.last_fault());
        assert!(db.checkpoint().await.expect("checkpoint").success);
        db.shutdown().await.expect("shutdown");
    }
    assert_eq!(ready(&channel, &input).await, 0);
    delete(&channel, &[&input, &output]).await;
}
