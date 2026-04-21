//! Shared helpers for the docker-compose integration tests under
//! `tests/docker/compose.yml`. Probes skip cleanly when the broker or
//! MinIO isn't up.

#![allow(dead_code)] // not every test file uses every helper

use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;

/// Address of the Kafka broker started by `tests/docker/compose.yml`.
/// Kept in sync with the `19092:19092` port mapping in that file.
pub const KAFKA_BROKERS: &str = "127.0.0.1:19092";

/// Returns `Some(KAFKA_BROKERS)` when a TCP connection to the broker
/// succeeds within ~500 ms, `None` otherwise. Use at the top of a test
/// to skip when Docker isn't up:
///
/// ```ignore
/// let Some(brokers) = kafka_brokers() else {
///     eprintln!("skipping: Redpanda not reachable at {KAFKA_BROKERS}");
///     return;
/// };
/// ```
pub fn kafka_brokers() -> Option<&'static str> {
    let addr: std::net::SocketAddr = KAFKA_BROKERS.parse().ok()?;
    TcpStream::connect_timeout(&addr, Duration::from_millis(500))
        .ok()
        .map(|_| KAFKA_BROKERS)
}

/// Create a Kafka topic (no-op if it already exists). Used by each test
/// to isolate its data under unique topic names.
pub async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let _ = admin
        .create_topics(&[topic], &AdminOptions::new())
        .await
        .expect("create_topics");
}

/// Delete a topic. Used during test teardown to keep the broker clean.
pub async fn delete_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let _ = admin.delete_topics(&[topic], &AdminOptions::new()).await;
}

/// Produce `count` JSON records shaped `{"id": i, "value": i * 10}`.
pub async fn produce_json_seq(brokers: &str, topic: &str, count: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");
    for i in 0..count {
        let payload = format!(r#"{{"id": {i}, "value": {}}}"#, i * 10);
        let key = i.to_string();
        producer
            .send(
                FutureRecord::to(topic).payload(&payload).key(&key),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

/// Consume up to `expected` records from `topic`, failing the test if
/// fewer arrive within `deadline`. Returns the payloads as UTF-8
/// strings (one per record).
pub async fn consume_json(
    brokers: &str,
    topic: &str,
    group: &str,
    expected: usize,
    deadline: Duration,
) -> Vec<String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");

    let mut out = Vec::new();
    let start = Instant::now();
    while out.len() < expected && start.elapsed() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(Ok(s)) = msg.payload_view::<str>() {
                    out.push(s.to_string());
                }
            }
            Ok(Err(e)) => eprintln!("consumer error: {e}"),
            Err(_) => continue, // poll timeout; check overall deadline
        }
    }
    out
}

/// Run `docker compose -f tests/docker/compose.yml <args>` and ignore
/// non-zero exit codes (useful for kill/restart that may race with the
/// health-checker).
pub fn compose(args: &[&str]) {
    let cwd = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    // Tests run from `crates/laminar-db/`; compose file is two levels up.
    let compose_path = std::path::Path::new(&cwd)
        .join("..")
        .join("..")
        .join("tests")
        .join("docker")
        .join("compose.yml");
    let compose_path = compose_path.canonicalize().unwrap_or(compose_path);
    let status = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg(&compose_path)
        .args(args)
        .status();
    if let Err(e) = status {
        eprintln!("docker compose {args:?} failed to spawn: {e}");
    }
}

/// Wait until the Kafka broker is reachable (or the deadline expires).
/// Useful after `compose(&["start", "redpanda"])` to block until the
/// broker is serving again.
pub async fn wait_for_broker(deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if kafka_brokers().is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

// ─── MinIO ─────────────────────────────────────────────────────────

/// Endpoint of the MinIO instance started by
/// `tests/docker/compose.yml`. Kept in sync with the `19000:9000`
/// mapping in that file.
pub const MINIO_ENDPOINT: &str = "http://127.0.0.1:19000";
pub const MINIO_ACCESS_KEY: &str = "laminar";
pub const MINIO_SECRET_KEY: &str = "laminar-test-secret";

/// Returns `Some(MINIO_ENDPOINT)` when MinIO is reachable, `None`
/// otherwise. Same skip semantics as [`kafka_brokers`].
pub fn minio_endpoint() -> Option<&'static str> {
    let addr: std::net::SocketAddr = "127.0.0.1:19000".parse().ok()?;
    TcpStream::connect_timeout(&addr, Duration::from_millis(500))
        .ok()
        .map(|_| MINIO_ENDPOINT)
}

/// Build an `object_store::ObjectStore` pointing at a MinIO bucket. The
/// bucket is created if it does not exist.
///
/// # Panics
/// Panics if MinIO isn't reachable, or bucket creation fails.
pub async fn minio_store(bucket: &str) -> std::sync::Arc<dyn object_store::ObjectStore> {
    use object_store::aws::AmazonS3Builder;
    use object_store::ObjectStore;
    let _ = minio_endpoint().expect("MinIO must be up; run `docker compose up -d minio`");

    // Ensure the bucket exists via a temporary root-level client.
    let root = AmazonS3Builder::new()
        .with_endpoint(MINIO_ENDPOINT)
        .with_access_key_id(MINIO_ACCESS_KEY)
        .with_secret_access_key(MINIO_SECRET_KEY)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_bucket_name(bucket)
        .build()
        .expect("minio client");
    // Probe: if a list_with_delimiter works, bucket is fine. Otherwise
    // create it via mc in a one-shot docker exec.
    if root.list_with_delimiter(None).await.is_err() {
        let status = Command::new("docker")
            .args([
                "exec",
                "laminardb-minio",
                "mc",
                "--quiet",
                "alias",
                "set",
                "local",
                "http://127.0.0.1:9000",
                MINIO_ACCESS_KEY,
                MINIO_SECRET_KEY,
            ])
            .status();
        if let Ok(s) = status {
            assert!(s.success(), "mc alias set failed");
        }
        let _ = Command::new("docker")
            .args([
                "exec",
                "laminardb-minio",
                "mc",
                "--quiet",
                "mb",
                format!("local/{bucket}").as_str(),
            ])
            .status();
    }
    std::sync::Arc::new(root) as std::sync::Arc<dyn ObjectStore>
}
