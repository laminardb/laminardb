//! Three-node at-least-once soaks for sinks that every node writes to: RabbitMQ, IBM MQ,
//! PostgreSQL append, MongoDB insert, and NATS JetStream. A Kafka source feeds a projection, every
//! node writes its share of the rows to one destination, and the observed leader and each follower
//! are hard-killed while paused inside a checkpoint, after the sinks flushed. An independent oracle
//! reads the destination: every Kafka-acknowledged id must arrive, no other id may, and duplicates
//! are reported. Every node must write before the first kill and again after it rejoins, so a run
//! in which a single node does all the writing fails.
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
mod ibmmq;
#[cfg(feature = "mongodb-cdc")]
mod mongo;
#[cfg(feature = "nats")]
mod nats;
#[cfg(feature = "postgres-sink")]
mod postgres;
#[cfg(feature = "rabbitmq")]
mod rabbitmq;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::{
    assert_running_nodes, cluster_kafka_partition_count, cluster_key_group_count,
    cluster_node_config, configured_cluster_checkpoint_timeout, custom_s3_endpoint_configured,
    env_u64, has_full_membership, kafka_create_topic, observed_leader, recovery_ceiling,
    remaining_progress_window, scoped_soak_storage_url, select_follower_victim, soak_run_id,
    validate_checkpoint_liveness, wait_for, wait_for_converged_durable_checkpoint, wait_until,
    DurableCheckpointStatus, JoinDelivery, Node, ResolvedExecutable, BASE_PORT, NODES,
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

/// The environment value of `name`, or `default`; without a default the setting is required.
fn setting(name: &str, default: Option<&str>) -> String {
    match (std::env::var(name), default) {
        (Ok(value), _) if !value.trim().is_empty() => value,
        (_, Some(default)) => default.to_owned(),
        _ => panic!("the multi-writer sink soak requires {name}"),
    }
}

/// The external system the sink writes to, and how the test provisions and reads it.
trait Destination {
    /// The `connector` of the sink.
    fn connector(&self) -> &'static str;

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

fn block_on<F: std::future::Future>(future: F) -> F::Output {
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
    #[cfg(any(feature = "rabbitmq", feature = "ibmmq", feature = "nats"))]
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
struct Oracle {
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

    fn assert_running(&mut self) {
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

struct SoakSettings {
    interval_ms: u64,
    checkpoint_timeout: Duration,
    recovery_ceiling: Duration,
    kills: u64,
    steady: Duration,
    rps: u64,
    partitions: i32,
    key_groups: u32,
}

impl SoakSettings {
    fn from_environment() -> Self {
        let interval_ms = env_u64("LAMINAR_SOAK_INTERVAL_MS", 500);
        assert!(
            interval_ms >= 100,
            "LAMINAR_SOAK_INTERVAL_MS must be at least 100"
        );
        let recovery_ceiling = recovery_ceiling();
        let checkpoint_timeout = configured_cluster_checkpoint_timeout(
            JoinDelivery::AtLeastOnce,
            custom_s3_endpoint_configured(),
        );
        validate_checkpoint_liveness(interval_ms, checkpoint_timeout, recovery_ceiling);
        let kills = env_u64("LAMINAR_SOAK_KILLS", 4);
        assert!(
            cfg!(debug_assertions) || kills == 0,
            "killing inside a checkpoint needs the debug-only gate of `cargo test --profile soak`"
        );
        let rps = env_u64("LAMINAR_SOAK_RPS", 400);
        assert!(rps > 0, "LAMINAR_SOAK_RPS must be greater than zero");
        Self {
            interval_ms,
            checkpoint_timeout,
            recovery_ceiling,
            kills,
            steady: Duration::from_secs(env_u64("LAMINAR_SOAK_SECONDS", 30)),
            rps,
            partitions: cluster_kafka_partition_count(),
            key_groups: cluster_key_group_count(),
        }
    }
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

fn cluster_nodes(
    executable: &Arc<ResolvedExecutable>,
    dir: &Path,
    log_dir: &Path,
    settings: &SoakSettings,
    checkpoint_url: &str,
    workload: &str,
) -> Vec<Node> {
    (0..NODES)
        .map(|id| {
            let config = format!(
                "{}{workload}",
                cluster_node_config(
                    id,
                    settings.interval_ms,
                    settings.checkpoint_timeout,
                    settings.key_groups,
                    checkpoint_url,
                    JoinDelivery::AtLeastOnce,
                )
            );
            let config_path = dir.join(format!("node{id}.toml"));
            std::fs::write(&config_path, config).expect("write node config");
            Node {
                id,
                executable: Arc::clone(executable),
                config_path,
                log_path: log_dir.join(format!("node{id}.log")),
                child: None,
                process_generation: 0,
                http_port: BASE_PORT + id as u16,
                fault_trigger_path: None,
                checkpoint_gate_path: Some(dir.join(format!("checkpoint-node-{id}.arm"))),
                sink_commit_gate_path: None,
            }
        })
        .collect()
}

fn run_three_node_sink_kill9_soak(destination: &dyn Destination) {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment();
    let brokers = setting("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS", None);
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let topic = format!("soak-sink-in-{run_id}");
    kafka_create_topic(&brokers, &topic, settings.partitions);
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
        &workload,
    );
    let mut producer = InputProducer::spawn(brokers, topic, settings.partitions, settings.rps);
    let metric = destination.written_metric();

    let mut latest = boot(&mut nodes, &mut producer, &settings, metric);
    let mut killed_followers = BTreeSet::new();
    for round in 0..settings.kills {
        latest = kill_round(
            &mut nodes,
            &mut producer,
            round,
            &mut killed_followers,
            &settings,
            metric,
            latest,
        );
    }
    steady_phase(
        &mut nodes,
        &mut producer,
        &mut oracle,
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
    eprintln!(
        "soak: {} sink across {NODES} nodes: produced={produced} lost=0 duplicates={duplicates} \
         kills={} (followers {killed_followers:?})",
        destination.connector(),
        settings.kills
    );
    drop(nodes);
    destination.delete(&name);
}

/// Start every node, wait for one durable cluster checkpoint, and require every node to write.
fn boot(
    nodes: &mut [Node],
    producer: &mut InputProducer,
    settings: &SoakSettings,
    metric: &str,
) -> DurableCheckpointStatus {
    for node in nodes.iter_mut() {
        let permit = node.verify_executable_for_spawn();
        node.spawn(permit);
        // Stagger process startup to reduce formation churn, as the join soak does.
        std::thread::sleep(Duration::from_millis(500));
    }
    wait_for(
        "every node to become ready",
        Duration::from_secs(60),
        || {
            producer.assert_running();
            nodes.iter_mut().all(|node| {
                node.assert_running();
                node.epoch().is_some() && node.is_ready()
            })
        },
    );
    wait_for(
        "every node to observe full cluster membership",
        Duration::from_secs(60),
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            has_full_membership(nodes)
        },
    );
    // A pre-join epoch can burn a full gate timeout before the first durable checkpoint.
    let deadline = Instant::now() + settings.recovery_ceiling + Duration::from_secs(60);
    let first = wait_for_converged_durable_checkpoint(nodes, deadline, "first checkpoint", None);
    wait_for(
        "every node to publish to the sink",
        remaining_progress_window(deadline, "first publication"),
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            nodes
                .iter()
                .all(|node| node.metric(metric).is_some_and(|published| published > 0.0))
        },
    );
    first
}

fn stable_leader(nodes: &mut [Node], producer: &mut InputProducer) -> usize {
    let mut candidate = None;
    let mut samples = 0_u8;
    wait_for(
        "one stable observed cluster leader",
        Duration::from_secs(30),
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            let leader = observed_leader(nodes);
            samples = if leader.is_some() && leader == candidate {
                samples + 1
            } else {
                1
            };
            candidate = leader;
            candidate.is_some() && samples >= 3
        },
    );
    candidate.expect("stable leader wait completed without a leader")
}

/// Kill the leader (every `NODES`th round) or a follower while it is paused inside a checkpoint,
/// require the survivors to commit, then restart it and require it to publish and commit again.
fn kill_round(
    nodes: &mut [Node],
    producer: &mut InputProducer,
    round: u64,
    killed_followers: &mut BTreeSet<usize>,
    settings: &SoakSettings,
    metric: &str,
    latest: DurableCheckpointStatus,
) -> DurableCheckpointStatus {
    let leader = stable_leader(nodes, producer);
    let (victim, role) = if round.is_multiple_of(NODES as u64) {
        (leader, "leader")
    } else {
        let rotation = usize::try_from(round).unwrap_or(usize::MAX);
        let victim = select_follower_victim(leader, killed_followers, rotation);
        (victim, "follower")
    };
    nodes[victim].arm_checkpoint_kill(role);
    // A spontaneous recovery round exposes no checkpoint to arm until it completes.
    wait_for(
        "the selected node to pause inside its armed checkpoint phase",
        settings.recovery_ceiling + Duration::from_secs(45),
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            nodes[victim].checkpoint_kill_ready(role)
        },
    );
    let failover_deadline = Instant::now() + settings.recovery_ceiling;
    nodes[victim].kill9();
    nodes[victim].disarm_checkpoint_kill();
    if role == "follower" {
        killed_followers.insert(victim);
    }
    eprintln!("soak round {round}: kill -9 delivered to {role} node {victim} inside a checkpoint");
    let survived = wait_for_converged_durable_checkpoint(
        nodes,
        failover_deadline,
        "survivor progress after kill",
        Some(latest),
    );

    let permit = nodes[victim].verify_executable_for_spawn();
    let rejoin_deadline = Instant::now() + settings.recovery_ceiling;
    nodes[victim].spawn(permit);
    wait_for(
        "the restarted node to rejoin and publish to the sink",
        remaining_progress_window(rejoin_deadline, "restarted-node recovery"),
        || {
            assert_running_nodes(nodes);
            producer.assert_running();
            has_full_membership(nodes)
                && nodes[victim]
                    .metric(metric)
                    .is_some_and(|published| published > 0.0)
        },
    );
    let rejoined = wait_for_converged_durable_checkpoint(
        nodes,
        rejoin_deadline,
        "progress after rejoin",
        Some(survived),
    );
    eprintln!(
        "soak round {round}: node {victim} rejoined; checkpoint {} committed",
        rejoined.checkpoint_id
    );
    rejoined
}

fn published(node: &Node, metric: &str) -> f64 {
    let mut value = None;
    wait_for(
        &format!("node{} to expose {metric}", node.id),
        Duration::from_secs(10),
        || {
            value = node.metric(metric);
            value.is_some()
        },
    );
    value.expect("sink metric wait completed without a value")
}

/// Run the steady phase and require every node to keep publishing through it, so a node that
/// published once and then stalled cannot pass for a writer.
fn steady_phase(
    nodes: &mut [Node],
    producer: &mut InputProducer,
    oracle: &mut Oracle,
    steady: Duration,
    metric: &str,
) {
    let before: Vec<f64> = nodes.iter().map(|node| published(node, metric)).collect();
    let until = Instant::now() + steady;
    while Instant::now() < until {
        assert_running_nodes(nodes);
        producer.assert_running();
        oracle.assert_running();
        std::thread::sleep(Duration::from_millis(500));
    }
    let after: Vec<f64> = nodes.iter().map(|node| published(node, metric)).collect();
    let during: Vec<f64> = after.iter().zip(&before).map(|(a, b)| a - b).collect();
    eprintln!("soak: published per node during the steady phase: {during:?}");
    assert!(
        during.iter().all(|published| *published > 0.0),
        "every node must keep publishing through the steady phase: {during:?}"
    );
}

/// Wait until every produced id has reached the destination, then stop the oracle.
fn wait_for_delivery(
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
