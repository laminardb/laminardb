//! Sealed server startup and worker-loss recovery over real Kafka queues and shared checkpoints.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use super::*;
use crate::cluster_config::ClusterConfig;
use crate::config::ServerConfig;

fn config(brokers: &str) -> ServerConfig {
    let package = PathBuf::from(std::env::var_os("LAMINAR_PROCESS_REPLAY_PACKAGE").unwrap());
    let endpoint = std::env::var("LAMINAR_PROCESS_TEST_S3_ENDPOINT").unwrap();
    let endpoint_address: std::net::SocketAddr =
        endpoint.strip_prefix("http://").unwrap().parse().unwrap();
    assert!(endpoint_address.ip().is_loopback() && endpoint_address.port() != 0);
    let bucket = std::env::var("LAMINAR_PROCESS_TEST_S3_BUCKET").unwrap();
    let gossip_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut config: ServerConfig = toml::from_str(&format!(
        r#"
node_id = "python-worker-qualification"
[server]
mode = "cluster"
bind = "127.0.0.1:0"
delivery = "at_least_once"
key_groups = 4
[checkpoint]
url = "s3://{bucket}/python-server-{}"
interval = "1h"
timeout = "30s"
[discovery]
strategy = "static"
seeds = ["127.0.0.1:{gossip_port}"]
gossip_port = {gossip_port}
[[process_function]]
source = "events"
output = "activity"
source_sql = """
CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL,
ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)
FROM KAFKA ('bootstrap.servers' = '{brokers}', 'group.id' = 'python-server',
'topic' = 'events', 'startup.mode' = 'earliest', 'replay.order' = 'partition_rounds') FORMAT JSON
"""
manifest = "{}/manifest.json"
handler_file = "{}/handler/replay_handler.py"
function = "handle"
python = "{}/runtime/bin/python3.13"
runtime_root = "{}/runtime"
python_paths = ["{}/runtime/lib/python3.13/site-packages"]
timeout = "10s"
[[sink]]
name = "activity_output"
pipeline = "activity"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = "{brokers}"
topic = "activity_output"
"#,
        uuid::Uuid::new_v4(),
        package.display(),
        package.display(),
        package.display(),
        package.display(),
        package.display(),
    ))
    .unwrap();
    config.checkpoint.storage = std::collections::HashMap::from([
        ("aws_endpoint".into(), endpoint),
        ("aws_allow_http".into(), "true".into()),
        ("aws_region".into(), "us-east-1".into()),
        ("aws_access_key_id".into(), "minioadmin".into()),
        ("aws_secret_access_key".into(), "minioadmin".into()),
    ]);
    crate::config::validate_process_functions(&config).unwrap();
    config
}

async fn start(config: &ServerConfig, path: &std::path::Path) -> ClusterHandle {
    let cluster = ClusterConfig::from_server_config(config).unwrap().unwrap();
    let handle = tokio::time::timeout(
        Duration::from_secs(90),
        start_cluster(config.clone(), cluster, path.to_path_buf()),
    )
    .await
    .expect("sealed server startup exceeded its bound")
    .unwrap();
    assert!(!handle.db.cluster_intake_fenced());
    assert_eq!(handle.process_workers.len(), 1);
    assert!(handle.process_workers[0].is_alive());
    handle
}

async fn send(producer: &FutureProducer, partition: i32, account: &str, amount: i64, ts_ms: i64) {
    let payload = format!("{{\"account\":\"{account}\",\"amount\":{amount},\"ts\":{ts_ms}}}");
    producer
        .send(
            FutureRecord::to("events")
                .partition(partition)
                .key(account)
                .payload(&payload),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
}

async fn output(consumer: &StreamConsumer, db: &LaminarDB, count: usize) -> Vec<serde_json::Value> {
    let mut rows = Vec::with_capacity(count);
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..count {
            let message = consumer.recv().await.unwrap();
            rows.push(serde_json::from_slice(message.payload().unwrap()).unwrap());
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "expected {count} process sink rows, received {rows:?}; fault: {:?}",
        db.last_fault()
    );
    rows
}

fn worker_pid(manifest: &std::path::Path) -> u32 {
    let mut matches = BTreeSet::new();
    for task in std::fs::read_dir("/proc/self/task").unwrap() {
        let children = std::fs::read_to_string(task.unwrap().path().join("children")).unwrap();
        for child in children.split_whitespace() {
            let pid: u32 = child.parse().unwrap();
            let Ok(command) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
                continue;
            };
            if command
                .split(|byte| *byte == 0)
                .any(|arg| arg == manifest.as_os_str().as_encoded_bytes())
            {
                matches.insert(pid);
            }
        }
    }
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one owned manifest-bound child"
    );
    *matches.first().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a sealed read-only Linux package and loopback MinIO"]
async fn cluster_python_startup_worker_exit_and_shared_checkpoint_recovery() {
    let kafka = MockCluster::new(1).unwrap();
    kafka.create_topic("events", 2, 1).unwrap();
    kafka.create_topic("activity_output", 1, 1).unwrap();
    let brokers = kafka.bootstrap_servers();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "python-output-oracle")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset("activity_output", 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let config = config(&brokers);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("server.toml");
    std::fs::write(&path, "# qualification config anchor\n").unwrap();
    let handle = start(&config, &path).await;
    let db = Arc::clone(&handle.db);
    let gate = Arc::clone(&handle.serving_gate);
    let controller = Arc::clone(&handle.cluster_controller);
    let deadline = Arc::clone(&handle.process_lease.deadline);
    send(&producer, 0, "a", 60, 100).await;
    send(&producer, 1, "b", 10, 100).await;
    let prefix = output(&consumer, &db, 2).await;
    assert!(prefix
        .iter()
        .any(|row| row["account"] == "a" && row["total"] == 60));
    let cut = db.checkpoint().await.unwrap();
    assert!(cut.success, "{cut:?}");
    send(&producer, 0, "a", 50, 108).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let pid = worker_pid(&config.process_functions[0].manifest);
    assert!(tokio::process::Command::new("/bin/kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .await
        .unwrap()
        .success());
    let error = tokio::time::timeout(Duration::from_secs(20), handle.wait_for_shutdown())
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("process worker exited unexpectedly"),
        "{error}"
    );
    assert!(error.to_string().contains("worker cleanup"), "{error}");
    assert!(db.cluster_intake_fenced());
    assert!(!gate.open());
    assert!(!controller.process_lease_is_live());
    assert!(!deadline.is_live());
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    drop(db);

    let recovered = start(&config, &path).await;
    send(&producer, 1, "c", 7, 112).await;
    send(&producer, 1, "e", 4, 120).await;
    send(&producer, 0, "d", 3, 120).await;
    send(&producer, 1, "d", 6, 130).await;
    send(&producer, 0, "c", 5, 121).await;
    let mut rows = output(&consumer, &recovered.db, 6).await;
    // RECOVERY: cluster timers follow the committed watermark cut, not speculative intake.
    assert!(recovered.db.checkpoint().await.unwrap().success);
    rows.extend(output(&consumer, &recovered.db, 3).await);
    assert!(
        rows.iter().any(|row| row["account"] == "a"
            && row["total"] == 110
            && row["kind"].as_str().unwrap().starts_with("inactive:")),
        "{rows:?}"
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row["kind"].as_str().unwrap().starts_with("running:"))
            .count(),
        6
    );
    let ids = rows
        .iter()
        .map(|row| row["kind"].as_str().unwrap().split_once(':').unwrap().1)
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), rows.len());
    assert!(recovered.db.checkpoint().await.unwrap().success);
    recovered.process_lease.fence_authority();
    assert!(matches!(
        recovered.wait_for_shutdown().await,
        Err(ClusterStartupError::AuthorityLost(_))
    ));
}
