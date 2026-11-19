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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
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
use super::multi_writer_sinks::{block_on, wait_for_delivery, Destination as _};
use super::{
    kafka_create_topic, matrix_event_time_ms, scoped_soak_storage_url, soak_run_id, wait_for,
    wait_for_converged_durable_checkpoint, JoinDelivery, Node, ResolvedExecutable, NODES,
    SOAK_PRODUCER_MAX_IN_FLIGHT,
};

const BROKERS: &str = "LAMINAR_SOAK_KAFKA_SOURCE_BROKERS";
const KEYS: i64 = 64;
/// Key `k`'s only rate is `RATE_BASE + k`.
const RATE_BASE: i64 = 1_000;
/// The heartbeat document's key, which no trade uses.
const HEARTBEAT_KEY: i64 = -1;
const HEARTBEAT_EVERY: Duration = Duration::from_millis(250);
/// Longer than the clock difference between the soak host and the replica set, so every trade
/// is stamped after its key's rate changed.
const RATES_SETTLE: Duration = Duration::from_secs(2);
const TRADE_PARTITIONS: i32 = 4;
/// Grows only on the node that follows the rates' change stream; every node reads trades.
const RATES_RECEIVED: &str = "laminardb_mongodb_cdc_events_received_total";
/// What a node logs when it takes the rates source.
const RATES_OPENED: &str = "placed singleton source opened on this node source=rates";

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
    latest = warm_up(&mut nodes, &mut heartbeat, &settings, latest);
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
    let owners = rates_owners(&nodes);

    let produced = trader.stop();
    // The heartbeat keeps moving the rates' watermark past the last trades until they are priced.
    let observed = wait_for_delivery(oracle, produced, settings.recovery_ceiling);
    heartbeat.stop();
    let unexpected: Vec<u64> = observed.range(produced..).map(|(id, _)| *id).collect();
    assert!(
        unexpected.is_empty(),
        "the output holds {} ids that were never produced, first {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(8)]
    );
    assert!(
        owners.len() > 1,
        "only node {owners:?} ever opened the rates source: it never moved"
    );
    let duplicates: u64 = observed.values().map(|count| u64::from(count - 1)).sum();
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

[[source]]
name = "trades"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{trades}"
"group.id" = "{trades}"
"startup.mode" = "earliest"
"json.column.at.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "at"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "at"
max_out_of_orderness = "1s"

[[pipeline]]
name = "priced"
sql = """
SELECT t.id AS id, t.key AS key, r.rate AS rate
FROM trades t
LEFT JOIN rates FOR SYSTEM_TIME AS OF t.at AS r
  ON t.key = r._id
"""

# A trade priced with no rate or another key's is dropped here, so it shows as a lost id.
[[pipeline]]
name = "correctly_priced"
sql = "SELECT id FROM priced WHERE rate = key + {RATE_BASE}"

[[sink]]
name = "priced_output"
pipeline = "correctly_priced"
connector = "mongodb-sink"
[sink.properties]
{sink}"#
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

/// Wait until the owner has read a heartbeat, then for two checkpoints so the committed cut
/// holds the stream's position: a new change stream starts at the latest change. Returns the
/// last of them.
fn warm_up(
    nodes: &mut [Node],
    heartbeat: &mut Worker,
    settings: &SoakSettings,
    latest: super::DurableCheckpointStatus,
) -> super::DurableCheckpointStatus {
    wait_for(
        "the source owner to read a heartbeat",
        settings.recovery_ceiling,
        || {
            heartbeat.assert_running();
            nodes.iter().any(|node| {
                node.metric(RATES_RECEIVED)
                    .is_some_and(|events| events > 0.0)
            })
        },
    );
    let deadline = Instant::now() + settings.recovery_ceiling;
    let first = wait_for_converged_durable_checkpoint(nodes, deadline, "warm-up", Some(latest));
    wait_for_converged_durable_checkpoint(nodes, deadline, "warm-up", Some(first))
}

/// Every node whose log shows it opened the rates source. Logs are appended across restarts, and
/// a survivor may hold the source only for the seconds before the dead owner rejoins, too briefly
/// to count events on.
fn rates_owners(nodes: &[Node]) -> BTreeSet<usize> {
    nodes
        .iter()
        .filter(|node| {
            std::fs::read_to_string(&node.log_path).is_ok_and(|log| log.contains(RATES_OPENED))
        })
        .map(|node| node.id)
        .collect()
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

/// A background thread that runs `work` until stopped and returns its count.
struct Worker {
    name: &'static str,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<u64>>,
}

impl Worker {
    fn spawn<F, Fut>(name: &'static str, work: F) -> Self
    where
        F: FnOnce(Arc<AtomicBool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = u64>,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || block_on(work(stop)))
        };
        Self {
            name,
            stop,
            handle: Some(handle),
        }
    }

    fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("worker handle").join() {
            Ok(_) => panic!("the {} stopped before the soak completed", self.name),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .unwrap_or_else(|| panic!("the {} was already stopped", self.name))
            .join()
            .unwrap_or_else(|_| panic!("the {} thread failed", self.name))
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
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

/// Produces trades `0..` at `rps`, each on key `id % KEYS` and stamped with the wall time. Every
/// send must be acknowledged before the count is returned, so the acknowledged ids are the prefix
/// `0..produced`.
fn spawn_trader(brokers: String, topic: String, rps: u64) -> Worker {
    Worker::spawn("trade producer", move |stop| async move {
        produce(&brokers, &topic, rps, &stop).await
    })
}

async fn produce(brokers: &str, topic: &str, rps: u64, stop: &AtomicBool) -> u64 {
    use futures::stream::{FuturesUnordered, StreamExt as _};
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let acknowledge =
        |delivery: Option<Result<_, futures::channel::oneshot::Canceled>>| match delivery {
            Some(Ok(Ok(_))) => {}
            Some(Ok(Err((error, _)))) => panic!("Kafka never acknowledged a trade: {error}"),
            Some(Err(_)) | None => panic!("a trade's delivery was cancelled"),
        };
    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .create()
        .expect("trade producer");
    let start = tokio::time::Instant::now();
    let mut deliveries = FuturesUnordered::new();
    let mut produced = 0_u64;
    while !stop.load(Ordering::Acquire) {
        let due = start + Duration::from_secs_f64(produced as f64 / rps as f64);
        // Settle deliveries while the next trade is not yet due or too many are in flight.
        while deliveries.len() >= SOAK_PRODUCER_MAX_IN_FLIGHT || tokio::time::Instant::now() < due {
            tokio::select! {
                delivery = deliveries.next(), if !deliveries.is_empty() => acknowledge(delivery),
                () = tokio::time::sleep_until(due),
                    if deliveries.len() < SOAK_PRODUCER_MAX_IN_FLIGHT => {}
            }
        }
        let id = i64::try_from(produced).expect("trade ids fit i64");
        let payload = format!(
            r#"{{"id":{id},"key":{},"at":{}}}"#,
            id % KEYS,
            matrix_event_time_ms()
        );
        let key = id.to_string();
        let delivery = producer
            .send_result(FutureRecord::to(topic).payload(&payload).key(&key))
            .unwrap_or_else(|(error, _)| panic!("could not queue trade {id}: {error}"));
        deliveries.push(delivery);
        produced += 1;
    }
    while !deliveries.is_empty() {
        acknowledge(deliveries.next().await);
    }
    produced
}
