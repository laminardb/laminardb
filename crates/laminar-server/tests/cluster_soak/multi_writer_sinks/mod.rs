//! Three-node at-least-once soaks for sinks that every node writes to: RabbitMQ, IBM MQ,
//! PostgreSQL append, MongoDB insert, and NATS JetStream, and an exactly-once soak for Kafka. A
//! Kafka source feeds a projection, every node writes its share of the rows to one destination, and
//! the observed leader and each follower are hard-killed while paused inside a checkpoint, after the
//! sinks flushed. An independent oracle reads the destination: every Kafka-acknowledged id must
//! arrive, no other id may, and duplicates are reported. Every node must write before the first
//! kill and again after it rejoins, so a run in which a single node does all the writing fails.
//! Exactly-once then freezes the leader with `SIGSTOP`, once before the committed index and once
//! before the sink's external commit, resumes it after a successor commits, and allows no
//! duplicate.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,rabbitmq --test cluster_soak \
//!   multi_writer_sinks::three_node_rabbitmq_sink_kill9_soak -- --ignored --exact --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,ibmmq --test cluster_soak \
//!   multi_writer_sinks::three_node_ibmmq_sink_kill9_soak -- --ignored --exact --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,postgres-sink --test cluster_soak \
//!   multi_writer_sinks::three_node_postgres_sink_kill9_soak -- --ignored --exact --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,mongodb-cdc --test cluster_soak \
//!   multi_writer_sinks::three_node_mongodb_sink_kill9_soak -- --ignored --exact --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,nats --test cluster_soak \
//!   multi_writer_sinks::three_node_nats_sink_kill9_soak -- --ignored --exact --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka --test cluster_soak \
//!   multi_writer_sinks::three_node_kafka_exactly_once_sink_kill9_soak -- --ignored --exact \
//!   --nocapture
//! ```
//!
//! All read the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`, `LAMINAR_SOAK_KAFKA_PARTITIONS`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS`, and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4),
//! `LAMINAR_SOAK_SECONDS` (steady phase after the kills, default 30), and `LAMINAR_SOAK_RPS`
//! (default 400). Destination settings, with defaults matching `tests/docker/compose.yml`:
//! - RabbitMQ: `LAMINAR_SOAK_RABBITMQ_URL` (default `amqp://127.0.0.1:5672/%2f`),
//!   `LAMINAR_SOAK_RABBITMQ_USERNAME` (default `guest`), and `LAMINAR_SOAK_RABBITMQ_PASSWORD`.
//! - IBM MQ, against the queue manager `QM1` (channel `DEV.APP.SVRCONN`, user `app`):
//!   `LAMINAR_SOAK_IBMMQ_LIBRARY` (the MQ client library), `LAMINAR_SOAK_IBMMQ_CONTAINER` (where
//!   `runmqsc` defines the queue), `LAMINAR_SOAK_IBMMQ_CONNECTION` (default `127.0.0.1(1414)`),
//!   and `LAMINAR_SOAK_IBMMQ_PASSWORD`.
//! - PostgreSQL: `LAMINAR_SOAK_POSTGRES_HOST` (default `127.0.0.1`), `LAMINAR_SOAK_POSTGRES_PORT`
//!   (default `15434`), `LAMINAR_SOAK_POSTGRES_DATABASE` and `LAMINAR_SOAK_POSTGRES_USERNAME`
//!   (default `laminar`), and `LAMINAR_SOAK_POSTGRES_PASSWORD`.
//! - MongoDB: `LAMINAR_SOAK_MONGODB_URI` (the compose service is
//!   `mongodb://127.0.0.1:27217/?directConnection=true&tls=false`; the sink enables TLS unless the
//!   URI disables it).
//! - NATS JetStream: `LAMINAR_SOAK_NATS_URL` (default `nats://127.0.0.1:14222`).
//!
//! The sink DDL carries each password, and the MongoDB URI, as a `${VAR}` reference that each node
//! resolves from the environment it inherits, so no secret reaches the shared catalog.

#[cfg(feature = "ibmmq")]
pub(crate) mod ibmmq;
mod kafka;
#[cfg(feature = "mongodb-cdc")]
pub(crate) mod mongo;
#[cfg(feature = "nats")]
mod nats;
#[cfg(feature = "postgres-sink")]
pub(crate) mod postgres;
#[cfg(feature = "rabbitmq")]
pub(crate) mod rabbitmq;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, pause_round, steady_phase, SoakSettings,
};
use super::paused_leader::PauseGate;
use super::{
    cluster_kafka_partition_count, kafka_create_topic, scoped_soak_storage_url, soak_run_id,
    wait_for_converged_durable_checkpoint, wait_until, JoinDelivery, ResolvedExecutable, NODES,
    SOAK_PRODUCER_MAX_IN_FLIGHT,
};

const SINK: &str = "sink_out";

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka, S3, and RabbitMQ"]
#[cfg(feature = "rabbitmq")]
fn three_node_rabbitmq_sink_kill9_soak() {
    run_three_node_sink_kill9_soak(&rabbitmq::RabbitMq::from_environment());
}

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka, S3, and IBM MQ"]
#[cfg(feature = "ibmmq")]
fn three_node_ibmmq_sink_kill9_soak() {
    run_three_node_sink_kill9_soak(&ibmmq::IbmMq::from_environment());
}

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka, S3, and PostgreSQL"]
#[cfg(feature = "postgres-sink")]
fn three_node_postgres_sink_kill9_soak() {
    run_three_node_sink_kill9_soak(&postgres::Postgres::from_environment());
}

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka, S3, and MongoDB"]
#[cfg(feature = "mongodb-cdc")]
fn three_node_mongodb_sink_kill9_soak() {
    run_three_node_sink_kill9_soak(&mongo::MongoDb::from_environment());
}

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka, S3, and NATS JetStream"]
#[cfg(feature = "nats")]
fn three_node_nats_sink_kill9_soak() {
    run_three_node_sink_kill9_soak(&nats::Nats::from_environment());
}

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka and S3, exactly-once"]
fn three_node_kafka_exactly_once_sink_kill9_soak() {
    run_three_node_sink_kill9_soak(&kafka::KafkaExactlyOnce::from_environment());
}

/// The environment value of `name`, or `default`; without a default the setting is required.
fn setting(name: &str, default: Option<&str>) -> String {
    match (std::env::var(name), default) {
        (Ok(value), _) if !value.trim().is_empty() => value,
        (_, Some(default)) => default.to_owned(),
        _ => panic!("the multi-writer sink soak requires {name}"),
    }
}

/// The external system the sink writes to, and how the test provisions and reads it.
pub(crate) trait Destination {
    /// The `connector` of the sink.
    fn connector(&self) -> &'static str;

    /// The pipeline's guarantee. Exactly-once runs paused-leader rounds after the kill rounds
    /// and allows no duplicate.
    fn delivery(&self) -> JoinDelivery {
        JoinDelivery::AtLeastOnce
    }

    /// A per-node counter that grows only when that node's sink writes.
    fn written_metric(&self) -> &'static str;

    /// The sink's `format`, for connectors that take one.
    fn format(&self) -> Option<&'static str>;

    /// The run's queue, table, collection, or stream.
    fn name(&self, run_id: &str) -> String;

    /// The `[sink.properties]` entries that target `name`.
    fn sink_properties(&self, name: &str) -> String;

    /// Create `name` before any node starts: a missing destination fails every write.
    fn create(&self, name: &str);

    fn delete(&self, name: &str);

    fn spawn_oracle(&self, name: &str) -> Oracle;
}

pub(crate) fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("soak client runtime")
        .block_on(future)
}

/// How many times the oracle has seen each id.
#[derive(Clone, Default)]
struct Observed(Arc<Mutex<BTreeMap<u64, u32>>>);

impl Observed {
    /// Count one delivery from a queue or stream.
    fn add(&self, payload: &[u8]) {
        let value: serde_json::Value =
            serde_json::from_slice(payload).expect("sink output is a JSON object");
        let id = value["id"].as_u64().expect("sink output carries a u64 id");
        *self.lock().entry(id).or_default() += 1;
    }

    /// Replace every count with a snapshot of a table or collection.
    #[cfg(any(feature = "postgres-sink", feature = "mongodb-cdc"))]
    fn replace(&self, counts: BTreeMap<u64, u32>) {
        *self.lock() = counts;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, u32>> {
        self.0.lock().expect("oracle map lock")
    }
}

/// Independent reader of the destination. Queue readers acknowledge or commit only after
/// recording, so a reconnect can add duplicates but never hide a loss; table readers count rows
/// in full snapshots.
pub(crate) struct Oracle {
    observed: Observed,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Oracle {
    fn spawn(consume: impl FnOnce(Observed, Arc<AtomicBool>) + Send + 'static) -> Self {
        let observed = Observed::default();
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let (observed, stop) = (observed.clone(), Arc::clone(&stop));
            std::thread::spawn(move || consume(observed, stop))
        };
        Self {
            observed,
            stop,
            handle: Some(handle),
        }
    }

    pub(crate) fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("oracle handle").join() {
            Ok(()) => panic!("output oracle stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// Ids in `0..produced` not yet observed: their count and the first few.
    fn missing(&self, produced: u64) -> (u64, Vec<u64>) {
        let observed = self.observed.lock();
        let mut count = 0;
        let mut sample = Vec::new();
        for id in (0..produced).filter(|id| !observed.contains_key(id)) {
            count += 1;
            if sample.len() < 8 {
                sample.push(id);
            }
        }
        (count, sample)
    }

    fn finish(mut self) -> BTreeMap<u64, u32> {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            if let Err(payload) = handle.join() {
                std::panic::resume_unwind(payload);
            }
        }
        std::mem::take(&mut *self.observed.lock())
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Paced Kafka input with ids `0..` round-robin over the partitions. A failed delivery fails the
/// soak, so the acknowledged ids are always the contiguous prefix `0..produced`.
struct InputProducer {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<u64>>,
}

impl InputProducer {
    fn spawn(brokers: String, topic: String, partitions: i32, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || produce(&brokers, &topic, partitions, rps, &stop))
        };
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("producer handle").join() {
            Ok(_) => panic!("Kafka producer stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .expect("Kafka producer was already stopped")
            .join()
            .expect("Kafka producer thread failed")
    }
}

impl Drop for InputProducer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn produce(brokers: &str, topic: &str, partitions: i32, rps: u64, stop: &AtomicBool) -> u64 {
    use futures::stream::{FuturesUnordered, StreamExt as _};
    use rdkafka::producer::{FutureProducer, FutureRecord};

    fn acknowledge(
        result: Result<
            rdkafka::producer::future_producer::OwnedDeliveryResult,
            futures::channel::oneshot::Canceled,
        >,
    ) {
        result
            .expect("Kafka delivery future was cancelled before the producer stopped")
            .unwrap_or_else(|(error, _)| panic!("Kafka delivery failed: {error}"));
    }

    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .create()
        .expect("producer");
    let partitions = u64::try_from(partitions).expect("positive partition count");
    block_on(async {
        let start = tokio::time::Instant::now();
        let mut deliveries = FuturesUnordered::new();
        let mut produced = 0_u64;
        while !stop.load(Ordering::Acquire) {
            if deliveries.len() >= SOAK_PRODUCER_MAX_IN_FLIGHT {
                acknowledge(deliveries.next().await.expect("an in-flight delivery"));
                continue;
            }
            let due = start + Duration::from_secs_f64(produced as f64 / rps as f64);
            tokio::select! {
                Some(delivery) = deliveries.next(), if !deliveries.is_empty() => {
                    acknowledge(delivery);
                    continue;
                }
                () = tokio::time::sleep_until(due) => {}
            }
            let partition = i32::try_from(produced % partitions).expect("partition fits i32");
            let payload = format!(r#"{{"id":{produced},"payload":"p{produced}"}}"#);
            let delivery = producer
                .send_result(
                    FutureRecord::<(), _>::to(topic)
                        .payload(&payload)
                        .partition(partition),
                )
                .unwrap_or_else(|(error, _)| panic!("Kafka enqueue failed: {error}"));
            deliveries.push(delivery);
            produced += 1;
        }
        while let Some(delivery) = deliveries.next().await {
            acknowledge(delivery);
        }
        produced
    })
}

fn workload(
    brokers: &str,
    topic: &str,
    group: &str,
    destination: &dyn Destination,
    name: &str,
) -> String {
    let format = destination
        .format()
        .map(|format| format!("format = \"{format}\"\n"))
        .unwrap_or_default();
    format!(
        r#"
[[source]]
name = "sink_input"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{topic}"
"group.id" = "{group}"
"startup.mode" = "earliest"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "payload"
type = "VARCHAR"
nullable = false

# A cluster sink may not read a source directly.
[[pipeline]]
name = "sink_forwarded"
sql = "SELECT id, payload FROM sink_input"

[[sink]]
name = "{SINK}"
pipeline = "sink_forwarded"
connector = "{connector}"
{format}[sink.properties]
{properties}"#,
        connector = destination.connector(),
        properties = destination.sink_properties(name),
    )
}

fn run_three_node_sink_kill9_soak(destination: &dyn Destination) {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let delivery = destination.delivery();
    let settings = SoakSettings::from_environment(delivery);
    let partitions = cluster_kafka_partition_count();
    let brokers = setting("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS", None);
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let topic = format!("soak-sink-in-{run_id}");
    kafka_create_topic(&brokers, &topic, partitions);
    let name = destination.name(&run_id);
    destination.create(&name);
    let mut oracle = destination.spawn_oracle(&name);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!(
        "soak: {} sink node logs in {}",
        destination.connector(),
        log_dir.display()
    );
    let workload = workload(
        &brokers,
        &topic,
        &format!("soak-sink-{run_id}"),
        destination,
        &name,
    );
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        "",
        &workload,
    );
    let exactly_once = delivery == JoinDelivery::ExactlyOnce;
    let mut producer = InputProducer::spawn(brokers, topic, partitions, settings.rps);
    let metric = destination.written_metric();

    let mut latest = boot(
        &mut nodes,
        &mut || producer.assert_running(),
        &settings,
        metric,
    );
    let mut killed_followers = BTreeSet::new();
    for round in 0..settings.kills {
        (_, latest) = kill_round(
            &mut nodes,
            &mut || producer.assert_running(),
            round,
            &mut killed_followers,
            &settings,
            metric,
            latest,
        );
    }
    // Pausing signals the process, which only Unix does, at gates only debug builds compile.
    if exactly_once && cfg!(unix) && cfg!(debug_assertions) {
        for gate in [PauseGate::CommittedIndex, PauseGate::SinkCommit(SINK)] {
            latest = pause_round(
                &mut nodes,
                &mut || producer.assert_running(),
                gate,
                &settings,
                metric,
                latest,
            );
        }
    }
    steady_phase(
        &mut nodes,
        &mut || {
            producer.assert_running();
            oracle.assert_running();
        },
        settings.steady,
        metric,
    );
    wait_for_converged_durable_checkpoint(
        &mut nodes,
        Instant::now() + settings.recovery_ceiling,
        "steady durable progress",
        Some(latest),
    );

    let produced = producer.stop();
    let observed = wait_for_delivery(oracle, produced, settings.recovery_ceiling);
    let unexpected: Vec<u64> = observed
        .keys()
        .copied()
        .filter(|id| *id >= produced)
        .collect();
    assert!(
        unexpected.is_empty(),
        "the destination holds {} ids that were never produced, first {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(8)]
    );
    let duplicates: u64 = observed.values().map(|count| u64::from(count - 1)).sum();
    assert!(
        !exactly_once || duplicates == 0,
        "exactly-once output holds {duplicates} duplicate rows, first {:?}",
        observed
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(id, _)| *id)
            .take(8)
            .collect::<Vec<_>>()
    );
    eprintln!(
        "soak: {} sink across {NODES} nodes: produced={produced} lost=0 duplicates={duplicates} \
         kills={} (followers {killed_followers:?})",
        destination.connector(),
        settings.kills
    );
    drop(nodes);
    destination.delete(&name);
}

/// Wait until every produced id has reached the destination, then stop the oracle.
pub(crate) fn wait_for_delivery(
    mut oracle: Oracle,
    produced: u64,
    recovery_ceiling: Duration,
) -> BTreeMap<u64, u32> {
    // A recovery round can be in flight when the producer stops.
    let caught_up = wait_until(recovery_ceiling + Duration::from_secs(60), || {
        oracle.assert_running();
        oracle.missing(produced).0 == 0
    });
    if !caught_up {
        let (missing, sample) = oracle.missing(produced);
        panic!(
            "{missing} of {produced} produced ids never reached the destination, first {sample:?}"
        );
    }
    oracle.finish()
}
