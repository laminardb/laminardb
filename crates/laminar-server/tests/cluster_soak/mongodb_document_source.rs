//! Three-node at-least-once soak for a MongoDB document source on a temporal join's right side,
//! which one node at a time follows. Each key's rate document is written, acknowledged by the
//! majority, before any trade can use it, and a heartbeat document changes every 250 ms so the
//! rates' change-time watermark keeps passing the trades. A Kafka writer produces trades `0..`
//! stamped with the wall time; a temporal `LEFT JOIN` prices each trade with its key's rate, a
//! stream keeps only the trades priced with their key's rate, and a MongoDB sink receives their
//! ids. The observed leader and each follower are killed inside a checkpoint and rejoin
//! (`kill_rejoin`): the node that follows the change stream moves at rejoins and is replaced from
//! the committed cut when it dies, while every node restores its part of the joined rates.
//!
//! A trade that joins no rate or another one is filtered out, so it shows as a missing id: every
//! produced id must reach the output and no other id may; duplicates are reported. More than one
//! node must have opened the rates source, and every node must keep committing checkpoints.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,mongodb-cdc --test cluster_soak \
//!   mongodb_document_source::three_node_mongodb_document_temporal_kill9_soak \
//!   -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the kills, default 30), `LAMINAR_SOAK_RPS` (default 400), the Kafka broker
//! in `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`, the replica set in `LAMINAR_SOAK_MONGODB_SOURCE_URI`,
//! and the output database of the MongoDB sink soak.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mongodb::bson::{doc, Document};
use mongodb::Collection;

use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, steady_phase, SoakSettings, CHECKPOINTS_COMPLETED,
};
use super::mongodb_source::{
    connect, connect_database, SOURCE_DATABASE, SOURCE_URI, WRITE_ATTEMPTS, WRITE_TIMEOUT,
};
use super::multi_writer_sinks::mongo::MongoDb;
use super::multi_writer_sinks::{block_on, Destination as _};
use super::temporal_pricing::{
    priced_trades, spawn_trader, trades_and_pricing, warm_up, PricedTrades, Worker,
    HEARTBEAT_EVERY, HEARTBEAT_KEY, KEYS, RATES_SETTLE, RATE_BASE, TRADE_PARTITIONS,
};
use super::{
    kafka_create_topic, scoped_soak_storage_url, soak_run_id,
    wait_for_converged_durable_checkpoint, JoinDelivery, ResolvedExecutable, NODES,
};

const BROKERS: &str = "LAMINAR_SOAK_KAFKA_SOURCE_BROKERS";
/// Grows only on the node that follows the rates' change stream; every node reads trades.
const RATES_RECEIVED: &str = "laminardb_mongodb_cdc_events_received_total";

#[test]
#[ignore = "spawns 3 real laminardb processes with S3, Kafka and a MongoDB replica set"]
fn three_node_mongodb_document_temporal_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let sink = MongoDb::from_environment();
    let source_uri = std::env::var(SOURCE_URI)
        .unwrap_or_else(|_| panic!("the MongoDB document soak requires {SOURCE_URI}"));
    let brokers = std::env::var(BROKERS)
        .unwrap_or_else(|_| panic!("the MongoDB document soak requires {BROKERS}"));
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let (rates, trades, output) = (
        format!("rates_{}", run_id.replace('-', "_")),
        format!("soak-trades-{run_id}"),
        sink.name(&run_id),
    );
    block_on(create_rates(&source_uri, &rates));
    kafka_create_topic(&brokers, &trades, TRADE_PARTITIONS);
    sink.create(&output);
    let mut oracle = sink.spawn_oracle(&output);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!(
        "soak: MongoDB document source node logs in {}",
        log_dir.display()
    );
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        "",
        &workload(&brokers, &trades, &rates, &sink.sink_properties(&output)),
    );

    let mut latest = boot(
        &mut nodes,
        &mut || oracle.assert_running(),
        &settings,
        CHECKPOINTS_COMPLETED,
    );
    let mut heartbeat = spawn_heartbeat(source_uri.clone(), rates.clone());
    latest = warm_up(
        &mut nodes,
        &mut heartbeat,
        &settings,
        latest,
        RATES_RECEIVED,
    );
    block_on(write_rates(&source_uri, &rates));
    std::thread::sleep(RATES_SETTLE);
    let mut trader = spawn_trader(brokers.clone(), trades.clone(), settings.rps);
    let mut live = || {
        heartbeat.assert_running();
        trader.assert_running();
        oracle.assert_running();
    };
    let mut killed_followers = BTreeSet::new();
    for round in 0..settings.kills {
        (_, latest) = kill_round(
            &mut nodes,
            &mut live,
            round,
            &mut killed_followers,
            &settings,
            CHECKPOINTS_COMPLETED,
            latest,
        );
    }
    steady_phase(
        &mut nodes,
        &mut live,
        settings.steady,
        CHECKPOINTS_COMPLETED,
    );
    wait_for_converged_durable_checkpoint(
        &mut nodes,
        Instant::now() + settings.recovery_ceiling,
        "steady durable progress",
        Some(latest),
    );
    let PricedTrades {
        produced,
        duplicates,
        owners,
    } = priced_trades(&nodes, trader, heartbeat, oracle, &settings);
    eprintln!(
        "soak: MongoDB document source across {NODES} nodes: produced={produced} priced \
         correctly, lost=0 duplicates={duplicates} owners={owners:?} kills={} (followers \
         {killed_followers:?})",
        settings.kills
    );
    drop(nodes);
    block_on(async {
        connect(&source_uri, &rates)
            .await
            .drop()
            .await
            .expect("drop the rates collection");
    });
    sink.delete(&output);
}

fn workload(brokers: &str, trades: &str, rates: &str, sink: &str) -> String {
    format!(
        r#"
[[source]]
name = "rates"
connector = "mongodb-cdc"
primary_key = ["_id"]
[source.properties]
"connection.uri" = "$${{{SOURCE_URI}}}"
database = "{SOURCE_DATABASE}"
collection = "{rates}"
"output.mode" = "document"
"full.document.mode" = "required"
"change.time.column" = "changed_at"
[[source.schema]]
name = "_id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "rate"
type = "BIGINT"
nullable = true
[[source.schema]]
name = "changed_at"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "changed_at"
max_out_of_orderness = "1s"

{pricing}
[[sink]]
name = "priced_output"
pipeline = "correctly_priced"
connector = "mongodb-sink"
[sink.properties]
{sink}"#,
        pricing = trades_and_pricing(brokers, trades, "_id"),
    )
}

/// Create the rates collection with post-images before any node starts: the source binds its
/// UUID, and document mode needs every update's post-image.
async fn create_rates(uri: &str, name: &str) {
    connect_database(uri)
        .await
        .run_command(doc! {"create": name, "changeStreamPreAndPostImages": {"enabled": true}})
        .await
        .expect("create the rates collection");
}

/// Write every key's one rate. A retry rewrites the same document.
async fn write_rates(uri: &str, name: &str) {
    let collection = connect(uri, name).await;
    for key in 0..KEYS {
        retry(&format!("rate {key}"), || {
            upsert(&collection, key, RATE_BASE + key)
        })
        .await;
    }
}

/// Set document `key`'s rate, creating it if absent.
async fn upsert(collection: &Collection<Document>, key: i64, rate: i64) -> Result<(), String> {
    let write = collection
        .replace_one(doc! {"_id": key}, doc! {"rate": rate})
        .upsert(true);
    tokio::time::timeout(WRITE_TIMEOUT, write)
        .await
        .map_err(|_| "write timed out".to_owned())?
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn retry<F, Fut>(what: &str, mut write: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    for attempt in 1..=WRITE_ATTEMPTS {
        match write().await {
            Ok(()) => return,
            Err(error) => {
                assert!(
                    attempt < WRITE_ATTEMPTS,
                    "MongoDB never acknowledged {what}: {error}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

/// Changes the heartbeat document every [`HEARTBEAT_EVERY`].
fn spawn_heartbeat(uri: String, rates: String) -> Worker {
    Worker::spawn("heartbeat writer", move |stop| async move {
        let collection = connect(&uri, &rates).await;
        let mut beats = 0;
        while !stop.load(Ordering::Acquire) {
            let beat = i64::try_from(beats).expect("beats fit i64");
            retry("a heartbeat", || upsert(&collection, HEARTBEAT_KEY, beat)).await;
            beats += 1;
            tokio::time::sleep(HEARTBEAT_EVERY).await;
        }
        beats
    })
}
