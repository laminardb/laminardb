//! Three-node at-least-once soaks for the RabbitMQ and IBM MQ sinks. A Kafka source feeds a
//! projection, every node publishes its share of the rows to one queue, and the observed leader
//! and each follower are hard-killed while paused inside a checkpoint, after the sinks flushed.
//! An independent consumer drains the queue: every Kafka-acknowledged id must arrive, no other id
//! may, and duplicates are reported. Every node must publish before the first kill and again
//! after it rejoins, so a run in which a single node does all the writing fails.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,rabbitmq --test cluster_soak \
//!   messaging_sinks::three_node_rabbitmq_sink_kill9_soak -- --ignored --exact --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,ibmmq --test cluster_soak \
//!   messaging_sinks::three_node_ibmmq_sink_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! Both read the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`, `LAMINAR_SOAK_KAFKA_PARTITIONS`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS`, and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4),
//! `LAMINAR_SOAK_SECONDS` (steady phase after the kills, default 30), and `LAMINAR_SOAK_RPS`
//! (default 400). Broker settings:
//! - RabbitMQ: `LAMINAR_SOAK_RABBITMQ_URL` (default `amqp://127.0.0.1:5672/%2f`),
//!   `LAMINAR_SOAK_RABBITMQ_USERNAME` (default `guest`), and `LAMINAR_SOAK_RABBITMQ_PASSWORD`.
//! - IBM MQ, against the `tests/docker/compose.yml` queue manager `QM1` (channel
//!   `DEV.APP.SVRCONN`, user `app`): `LAMINAR_SOAK_IBMMQ_LIBRARY` (the MQ client library),
//!   `LAMINAR_SOAK_IBMMQ_CONTAINER` (where `runmqsc` defines the queue),
//!   `LAMINAR_SOAK_IBMMQ_CONNECTION` (default `127.0.0.1(1414)`), and
//!   `LAMINAR_SOAK_IBMMQ_PASSWORD`.
//!
//! The sink DDL carries the password as a `${VAR}` reference that each node resolves from the
//! environment it inherits, so the secret never reaches the shared catalog.

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

const SINK: &str = "mq_out";

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka, S3, and RabbitMQ"]
#[cfg(feature = "rabbitmq")]
fn three_node_rabbitmq_sink_kill9_soak() {
    run_three_node_sink_kill9_soak(&Broker::rabbitmq());
}

#[test]
#[ignore = "spawns 3 real laminardb processes with Kafka, S3, and IBM MQ"]
#[cfg(feature = "ibmmq")]
fn three_node_ibmmq_sink_kill9_soak() {
    run_three_node_sink_kill9_soak(&Broker::ibmmq());
}

fn required(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("the messaging sink soak requires {name}"))
}

fn optional(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// The broker the sink publishes to, and how the test provisions and reads its queue.
enum Broker {
    #[cfg(feature = "rabbitmq")]
    RabbitMq { url: String, username: String },
    #[cfg(feature = "ibmmq")]
    IbmMq {
        library: String,
        container: String,
        connection: String,
    },
}

impl Broker {
    #[cfg(feature = "rabbitmq")]
    fn rabbitmq() -> Self {
        required("LAMINAR_SOAK_RABBITMQ_PASSWORD");
        Self::RabbitMq {
            url: optional("LAMINAR_SOAK_RABBITMQ_URL", "amqp://127.0.0.1:5672/%2f"),
            username: optional("LAMINAR_SOAK_RABBITMQ_USERNAME", "guest"),
        }
    }

    #[cfg(feature = "ibmmq")]
    fn ibmmq() -> Self {
        required("LAMINAR_SOAK_IBMMQ_PASSWORD");
        Self::IbmMq {
            library: required("LAMINAR_SOAK_IBMMQ_LIBRARY"),
            container: required("LAMINAR_SOAK_IBMMQ_CONTAINER"),
            connection: optional("LAMINAR_SOAK_IBMMQ_CONNECTION", "127.0.0.1(1414)"),
        }
    }

    fn connector(&self) -> &'static str {
        match self {
            #[cfg(feature = "rabbitmq")]
            Self::RabbitMq { .. } => "rabbitmq",
            #[cfg(feature = "ibmmq")]
            Self::IbmMq { .. } => "ibmmq",
        }
    }

    fn published_metric(&self) -> &'static str {
        match self {
            #[cfg(feature = "rabbitmq")]
            Self::RabbitMq { .. } => "laminardb_rabbitmq_sink_published_total",
            #[cfg(feature = "ibmmq")]
            Self::IbmMq { .. } => "laminardb_ibmmq_sink_put_total",
        }
    }

    fn queue_name(&self, run_id: &str) -> String {
        match self {
            #[cfg(feature = "rabbitmq")]
            Self::RabbitMq { .. } => format!("soak.sink.{run_id}"),
            // The developer image authorizes `app` for `DEV.**`; MQ names allow no hyphen.
            #[cfg(feature = "ibmmq")]
            Self::IbmMq { .. } => format!("DEV.SOAK.SINK.{}", run_id.replace('-', ".")),
        }
    }

    fn sink_properties(&self, queue: &str) -> String {
        match self {
            #[cfg(feature = "rabbitmq")]
            Self::RabbitMq { url, username } => format!(
                r#"url = "{url}"
username = "{username}"
password = "$${{LAMINAR_SOAK_RABBITMQ_PASSWORD}}"
"routing.key" = "{queue}"
"#
            ),
            #[cfg(feature = "ibmmq")]
            Self::IbmMq {
                library,
                connection,
                ..
            } => format!(
                r#""client.library" = "{library}"
"queue.manager" = "QM1"
channel = "DEV.APP.SVRCONN"
"connection.name" = "{connection}"
username = "app"
password = "$${{LAMINAR_SOAK_IBMMQ_PASSWORD}}"
queue = "{queue}"
"#
            ),
        }
    }

    /// Create the output queue before any node starts: an unroutable or missing destination
    /// fails every write.
    fn create_queue(&self, queue: &str) {
        match self {
            #[cfg(feature = "rabbitmq")]
            Self::RabbitMq { .. } => block_on(rabbitmq::declare(self.amqp_uri(), queue)),
            #[cfg(feature = "ibmmq")]
            Self::IbmMq { container, .. } => ibmmq::mqsc(
                container,
                &format!("DEFINE QLOCAL('{queue}') MAXDEPTH(999999999) REPLACE\n"),
            ),
        }
    }

    fn delete_queue(&self, queue: &str) {
        match self {
            #[cfg(feature = "rabbitmq")]
            Self::RabbitMq { .. } => block_on(rabbitmq::delete(self.amqp_uri(), queue)),
            #[cfg(feature = "ibmmq")]
            Self::IbmMq { container, .. } => {
                ibmmq::mqsc(container, &format!("DELETE QLOCAL('{queue}') PURGE\n"));
            }
        }
    }

    fn spawn_oracle(&self, queue: &str) -> Oracle {
        let queue = queue.to_owned();
        match self {
            #[cfg(feature = "rabbitmq")]
            Self::RabbitMq { .. } => {
                let uri = self.amqp_uri();
                Oracle::spawn(move |observed, stop| {
                    block_on(rabbitmq::consume(uri, queue, observed, stop));
                })
            }
            #[cfg(feature = "ibmmq")]
            Self::IbmMq {
                library,
                connection,
                ..
            } => {
                let config = ibmmq::oracle_config(library, connection, &queue);
                Oracle::spawn(move |observed, stop| {
                    block_on(ibmmq::consume(config, observed, stop));
                })
            }
        }
    }

    #[cfg(feature = "rabbitmq")]
    fn amqp_uri(&self) -> lapin::uri::AMQPUri {
        let Self::RabbitMq { url, username } = self else {
            unreachable!("only the RabbitMQ broker has an AMQP URI");
        };
        let mut uri: lapin::uri::AMQPUri = url
            .parse()
            .unwrap_or_else(|error| panic!("LAMINAR_SOAK_RABBITMQ_URL is invalid: {error}"));
        uri.authority.userinfo = lapin::uri::AMQPUserInfo {
            username: username.clone(),
            password: required("LAMINAR_SOAK_RABBITMQ_PASSWORD"),
        };
        uri
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("soak client runtime")
        .block_on(future)
}

type Observed = Arc<Mutex<BTreeMap<u64, u32>>>;

fn record(observed: &Mutex<BTreeMap<u64, u32>>, payload: &[u8]) {
    let value: serde_json::Value =
        serde_json::from_slice(payload).expect("sink output is a JSON object");
    let id = value["id"].as_u64().expect("sink output carries a u64 id");
    *observed
        .lock()
        .expect("oracle map lock")
        .entry(id)
        .or_default() += 1;
}

/// Independent consumer of the output queue. It acknowledges or commits only after recording, so
/// a consumer reconnect can add duplicates but never hide a loss.
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
            let (observed, stop) = (Arc::clone(&observed), Arc::clone(&stop));
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
        let observed = self.observed.lock().expect("oracle map lock");
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
        std::mem::take(&mut *self.observed.lock().expect("oracle map lock"))
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

#[cfg(feature = "rabbitmq")]
mod rabbitmq {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::StreamExt as _;
    use lapin::options::{
        BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions, QueueDeleteOptions,
    };
    use lapin::types::FieldTable;
    use lapin::uri::AMQPUri;
    use lapin::{Connection, ConnectionProperties};

    use super::{record, Observed};

    pub(super) async fn declare(uri: AMQPUri, queue: &str) {
        let connection = Connection::connect_uri(uri, ConnectionProperties::default())
            .await
            .expect("connect to RabbitMQ");
        let channel = connection.create_channel().await.expect("channel");
        channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .expect("declare the output queue");
        let _ = connection.close(200, "declared".into()).await;
    }

    pub(super) async fn delete(uri: AMQPUri, queue: &str) {
        let Ok(connection) = Connection::connect_uri(uri, ConnectionProperties::default()).await
        else {
            return;
        };
        if let Ok(channel) = connection.create_channel().await {
            let _ = channel
                .queue_delete(queue.into(), QueueDeleteOptions::default())
                .await;
        }
        let _ = connection.close(200, "deleted".into()).await;
    }

    pub(super) async fn consume(
        uri: AMQPUri,
        queue: String,
        observed: Observed,
        stop: Arc<AtomicBool>,
    ) {
        while !stop.load(Ordering::Acquire) {
            let Ok(connection) =
                Connection::connect_uri(uri.clone(), ConnectionProperties::default()).await
            else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            };
            if let Ok(channel) = connection.create_channel().await {
                if let Ok(mut consumer) = channel
                    .basic_consume(
                        queue.as_str().into(),
                        "".into(),
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                {
                    while !stop.load(Ordering::Acquire) {
                        let Ok(next) =
                            tokio::time::timeout(Duration::from_millis(250), consumer.next()).await
                        else {
                            continue;
                        };
                        let Some(Ok(delivery)) = next else {
                            break;
                        };
                        record(&observed, &delivery.data);
                        if delivery.ack(BasicAckOptions::default()).await.is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = connection.close(200, "oracle".into()).await;
        }
    }
}

#[cfg(feature = "ibmmq")]
mod ibmmq {
    use std::io::Write as _;
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::cast::AsArray as _;
    use arrow_schema::{DataType, Field, Schema};
    use laminar_connectors::config::ConnectorConfig;
    use laminar_connectors::connector::{
        DeliveryGuarantee, SourceConnector as _, SourcePosition, SourceStart,
    };
    use laminar_connectors::ibmmq::IbmMqSource;

    use super::{record, required, Observed};

    pub(super) fn mqsc(container: &str, commands: &str) {
        let mut child = Command::new("docker")
            .args(["exec", "-i", container, "runmqsc", "QM1"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("run runmqsc in the IBM MQ container");
        child
            .stdin
            .take()
            .expect("runmqsc stdin")
            .write_all(commands.as_bytes())
            .expect("write MQSC commands");
        let output = child.wait_with_output().expect("runmqsc output");
        assert!(
            output.status.success(),
            "runmqsc failed: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    pub(super) fn oracle_config(library: &str, connection: &str, queue: &str) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("ibmmq");
        for (key, value) in [
            ("client.library", library),
            ("queue.manager", "QM1"),
            ("channel", "DEV.APP.SVRCONN"),
            ("connection.name", connection),
            ("username", "app"),
            ("queue", queue),
            ("format", "raw"),
        ] {
            config.set(key, value);
        }
        config.set("password", required("LAMINAR_SOAK_IBMMQ_PASSWORD"));
        config
    }

    /// Commit each polled batch after recording it; a failed generation restarts the source,
    /// and the queue manager returns its uncommitted messages.
    pub(super) async fn consume(
        config: ConnectorConfig,
        observed: Observed,
        stop: Arc<AtomicBool>,
    ) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let mut epoch = 0;
        while !stop.load(Ordering::Acquire) {
            let mut source = IbmMqSource::new(Arc::clone(&schema), None);
            let start = SourceStart::new(
                config.clone(),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .expect("oracle start request");
            if source.start(start).await.is_err() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            while !stop.load(Ordering::Acquire) {
                let batch = match source.poll_batch(1_000).await {
                    Ok(Some(batch)) => batch,
                    Ok(None) => continue,
                    Err(_) => break,
                };
                for text in batch.records.column(0).as_string::<i32>().iter().flatten() {
                    record(&observed, text.as_bytes());
                }
                epoch += 1;
                let cut = source.checkpoint();
                if source.notify_epoch_committed(epoch, &cut).await.is_err() {
                    break;
                }
            }
            let _ = source.close().await;
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

fn workload(brokers: &str, topic: &str, group: &str, broker: &Broker, queue: &str) -> String {
    format!(
        r#"
[[source]]
name = "mq_input"
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
name = "mq_forwarded"
sql = "SELECT id, payload FROM mq_input"

[[sink]]
name = "{SINK}"
pipeline = "mq_forwarded"
connector = "{connector}"
format = "json"
[sink.properties]
{properties}"#,
        connector = broker.connector(),
        properties = broker.sink_properties(queue),
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

fn run_three_node_sink_kill9_soak(broker: &Broker) {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment();
    let brokers = required("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS");
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let topic = format!("soak-sink-in-{run_id}");
    kafka_create_topic(&brokers, &topic, settings.partitions);
    let queue = broker.queue_name(&run_id);
    broker.create_queue(&queue);
    let mut oracle = broker.spawn_oracle(&queue);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!(
        "soak: {} sink node logs in {}",
        broker.connector(),
        log_dir.display()
    );
    let workload = workload(
        &brokers,
        &topic,
        &format!("soak-sink-{run_id}"),
        broker,
        &queue,
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
    let metric = broker.published_metric();

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
        "the queue holds {} ids that were never produced, first {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(8)]
    );
    let duplicates: u64 = observed.values().map(|count| u64::from(count - 1)).sum();
    eprintln!(
        "soak: {} sink across {NODES} nodes: produced={produced} lost=0 duplicates={duplicates} \
         kills={} (followers {killed_followers:?})",
        broker.connector(),
        settings.kills
    );
    drop(nodes);
    broker.delete_queue(&queue);
}

/// Start every node, wait for one durable cluster checkpoint, and require every node to publish.
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

/// Wait until every produced id has reached the queue, then stop the oracle.
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
        panic!("{missing} of {produced} produced ids never reached the queue, first {sample:?}");
    }
    oracle.finish()
}
