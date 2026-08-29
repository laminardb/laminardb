//! Real-process Kafka-to-Iceberg cluster recovery test.

#![cfg(all(
    feature = "cluster",
    feature = "aws",
    feature = "kafka",
    feature = "iceberg"
))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use arrow_array::{Array as _, Int64Array};
use futures::TryStreamExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore as _;
use object_store::ObjectStoreExt as _;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Deserialize;

const NODE_COUNT: usize = 3;
const INPUT_PARTITIONS: i32 = 3;
const CONSOLE_TOKEN: &str = "laminardb-iceberg-cluster-soak";
const BROKERS: &str = "127.0.0.1:19092";
const CATALOG_URI: &str = "http://127.0.0.1:8181";
const S3_ENDPOINT: &str = "http://127.0.0.1:9000";
const S3_BUCKET: &str = "warehouse";
const S3_ACCESS_KEY: &str = "minioadmin";
const S3_SECRET_KEY: &str = "minioadmin";
const S3_REGION: &str = "us-east-1";
const S3_SECRET_ENV: &str = "ICEBERG_CLUSTER_S3_SECRET_KEY";
const SINK_NAME: &str = "iceberg_out";
const FIRST_ROWS: i64 = 30;
const SECOND_ROWS: i64 = 30;
const THIRD_ROWS: i64 = 30;
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(120);
const CATALOG_RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
const DATA_FILE_ORACLE_LIMIT: u64 = 8 * 1024 * 1024;

struct Node {
    id: usize,
    http_port: u16,
    config_path: PathBuf,
    log_path: PathBuf,
    gate_path: PathBuf,
    child: Option<Child>,
}

struct ClusterFixture<'a> {
    directory: &'a Path,
    http_ports: &'a [u16],
    gossip_ports: &'a [u16],
    run_id: &'a str,
    namespace: &'a str,
    table: &'a str,
    topic: &'a str,
}

impl Node {
    fn spawn(&mut self) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .expect("open Iceberg cluster node log");
        let child = Command::new(env!("CARGO_BIN_EXE_laminardb"))
            .arg("--config")
            .arg(&self.config_path)
            .env(S3_SECRET_ENV, S3_SECRET_KEY)
            .env("LAMINAR_EXTERNAL_SINK_COMMIT_GATE_FILE", &self.gate_path)
            .env("LAMINAR_SOAK_ALLOW_S3_EMULATOR", "1")
            .env(
                "RUST_LOG",
                "laminardb=info,laminar_server=info,laminar_db=info",
            )
            .env("NO_COLOR", "1")
            .stdout(Stdio::from(log.try_clone().expect("clone node log")))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap_or_else(|error| panic!("spawn Iceberg cluster node{}: {error}", self.id));
        self.child = Some(child);
    }

    fn assert_running(&mut self) {
        let child = self
            .child
            .as_mut()
            .unwrap_or_else(|| panic!("node{} is not expected to be running", self.id));
        match child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => {
                self.dump_log();
                panic!("Iceberg cluster node{} exited: {status}", self.id);
            }
            Err(error) => {
                self.dump_log();
                panic!("inspect Iceberg cluster node{}: {error}", self.id);
            }
        }
    }

    fn kill9(&mut self) {
        let mut child = self
            .child
            .take()
            .unwrap_or_else(|| panic!("node{} has no process to kill", self.id));
        child
            .kill()
            .unwrap_or_else(|error| panic!("hard-kill node{}: {error}", self.id));
        let status = child
            .wait()
            .unwrap_or_else(|error| panic!("reap node{}: {error}", self.id));
        assert!(
            !status.success(),
            "hard-killed node{} exited cleanly",
            self.id
        );
    }

    fn arm_external_commit_gate(&self) {
        remove_file_if_present(&self.gate_path);
        remove_file_if_present(&self.gate_path.with_extension("ready"));
        std::fs::write(&self.gate_path, SINK_NAME).expect("arm external sink commit gate");
    }

    fn disarm_external_commit_gate(&self) {
        remove_file_if_present(&self.gate_path);
        remove_file_if_present(&self.gate_path.with_extension("ready"));
    }

    fn dump_log(&self) {
        eprintln!("--- Iceberg cluster node{} log tail", self.id);
        if let Ok(log) = std::fs::read_to_string(&self.log_path) {
            for line in log.lines().rev().take(60).collect::<Vec<_>>().iter().rev() {
                eprintln!("{line}");
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.disarm_external_commit_gate();
    }
}

#[derive(Debug)]
struct GateEvidence {
    checkpoint_id: u64,
    epoch: u64,
    fencing_token: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LoadTableResponse {
    metadata: TableMetadata,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct TableMetadata {
    location: String,
    current_snapshot_id: Option<i64>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Snapshot {
    snapshot_id: i64,
    summary: BTreeMap<String, String>,
}

impl TableMetadata {
    fn total_records(&self) -> u64 {
        self.current_snapshot()
            .and_then(|snapshot| snapshot.summary.get("total-records"))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    fn total_data_files(&self) -> u64 {
        self.current_snapshot()
            .and_then(|snapshot| snapshot.summary.get("total-data-files"))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    fn current_snapshot(&self) -> Option<&Snapshot> {
        let current = self.current_snapshot_id?;
        self.snapshots
            .iter()
            .find(|snapshot| snapshot.snapshot_id == current)
    }

    fn commit_uuid(&self, checkpoint_id: u64) -> Option<&str> {
        self.snapshots.iter().find_map(|snapshot| {
            let committed_checkpoint = snapshot
                .summary
                .get("laminardb.checkpoint.id")
                .and_then(|value| value.parse().ok());
            if committed_checkpoint != Some(checkpoint_id) {
                return None;
            }
            snapshot
                .summary
                .get("laminardb.commit.uuid")
                .map(String::as_str)
        })
    }

    fn validate_exact_commit_identities(&self) {
        let mut checkpoints = BTreeSet::new();
        let mut commit_uuids = BTreeSet::new();
        for snapshot in &self.snapshots {
            let Some(checkpoint) = snapshot.summary.get("laminardb.checkpoint.id") else {
                continue;
            };
            let checkpoint = checkpoint
                .parse::<u64>()
                .expect("LaminarDB snapshot checkpoint must be a u64");
            let commit_uuid = snapshot
                .summary
                .get("laminardb.commit.uuid")
                .expect("LaminarDB snapshot must carry a commit UUID");
            assert!(
                checkpoints.insert(checkpoint),
                "checkpoint {checkpoint} produced more than one Iceberg snapshot"
            );
            assert!(
                commit_uuids.insert(commit_uuid),
                "Iceberg commit UUID {commit_uuid} was reused across checkpoints"
            );
            for required in [
                "laminardb.commit.namespace",
                "laminardb.deployment.id",
                "laminardb.sink.id",
                "laminardb.fencing.token",
                "laminardb.batch.fingerprint",
                "laminardb.file-set.fingerprint",
            ] {
                assert!(
                    snapshot
                        .summary
                        .get(required)
                        .is_some_and(|value| !value.is_empty()),
                    "Iceberg snapshot {} lacks {required}",
                    snapshot.snapshot_id
                );
            }
        }
    }

    fn laminar_snapshot_count(&self) -> usize {
        self.snapshots
            .iter()
            .filter(|snapshot| snapshot.summary.contains_key("laminardb.checkpoint.id"))
            .count()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three real laminardb processes with Kafka, Iceberg REST, and MinIO"]
async fn leader_restart_reconciles_one_iceberg_snapshot_per_checkpoint() {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build integration HTTP client");
    require_dependencies(&client).await;

    let run_id = uuid::Uuid::now_v7().simple().to_string();
    let namespace = format!("cluster_{run_id}");
    let table = "events";
    let topic = format!("iceberg-cluster-{run_id}");
    create_table(&client, &namespace, table).await;
    create_topic(&topic).await;

    let directory = tempfile::tempdir().expect("create Iceberg cluster test directory");
    let ports = free_ports(NODE_COUNT * 2);
    let (http_ports, gossip_ports) = ports.split_at(NODE_COUNT);
    let fixture = ClusterFixture {
        directory: directory.path(),
        http_ports,
        gossip_ports,
        run_id: &run_id,
        namespace: &namespace,
        table,
        topic: &topic,
    };
    let mut nodes = (0..NODE_COUNT)
        .map(|id| {
            let config_path = fixture.write_node_config(id);
            Node {
                id,
                http_port: http_ports[id],
                config_path,
                log_path: directory.path().join(format!("node{id}.log")),
                gate_path: directory
                    .path()
                    .join(format!("node{id}-external-commit.arm")),
                child: None,
            }
        })
        .collect::<Vec<_>>();
    for node in &mut nodes {
        node.spawn();
    }
    wait_for_full_cluster(&client, &mut nodes).await;

    produce_rows(&topic, 0, FIRST_ROWS).await;
    let first =
        wait_for_total_records(&client, &mut nodes, &namespace, table, FIRST_ROWS as u64).await;
    first.validate_exact_commit_identities();

    let leader = wait_for_stable_leader(&client, &mut nodes).await;
    nodes[leader].arm_external_commit_gate();
    produce_rows(&topic, FIRST_ROWS, SECOND_ROWS).await;
    let gate = wait_for_external_commit_gate(&mut nodes[leader]).await;
    assert!(gate.epoch > 0 && gate.fencing_token > 0);
    let fault_state =
        wait_for_checkpoint_snapshot(&client, &mut nodes, &namespace, table, gate.checkpoint_id)
            .await;
    let rows_at_fault = fault_state.total_records();
    assert!(
        rows_at_fault > FIRST_ROWS as u64 && rows_at_fault <= (FIRST_ROWS + SECOND_ROWS) as u64,
        "faulted commit published an unexpected row cut: {rows_at_fault}"
    );
    let fault_commit_uuid = fault_state
        .commit_uuid(gate.checkpoint_id)
        .expect("faulted checkpoint snapshot must carry a commit UUID")
        .to_owned();

    nodes[leader].kill9();
    nodes[leader].disarm_external_commit_gate();
    wait_for_stable_leader(&client, &mut nodes).await;
    let recovered = wait_for_total_records(
        &client,
        &mut nodes,
        &namespace,
        table,
        (FIRST_ROWS + SECOND_ROWS) as u64,
    )
    .await;
    recovered.validate_exact_commit_identities();
    assert_eq!(
        recovered.commit_uuid(gate.checkpoint_id),
        Some(fault_commit_uuid.as_str())
    );

    nodes[leader].spawn();
    wait_for_full_cluster(&client, &mut nodes).await;
    produce_rows(&topic, FIRST_ROWS + SECOND_ROWS, THIRD_ROWS).await;
    let expected_rows = (FIRST_ROWS + SECOND_ROWS + THIRD_ROWS) as u64;
    let final_state =
        wait_for_total_records(&client, &mut nodes, &namespace, table, expected_rows).await;
    final_state.validate_exact_commit_identities();
    assert!(
        final_state.laminar_snapshot_count() >= 3,
        "three input waves must publish at least three non-empty checkpoints"
    );
    assert_eq!(
        final_state.commit_uuid(gate.checkpoint_id),
        Some(fault_commit_uuid.as_str()),
        "leader restart changed the logical Iceberg commit identity"
    );
    verify_exact_data_files(&final_state, expected_rows).await;
}

async fn require_dependencies(client: &reqwest::Client) {
    let config = client
        .get(format!("{CATALOG_URI}/v1/config"))
        .send()
        .await
        .expect("Iceberg REST catalog must be reachable");
    assert!(
        config.status().is_success(),
        "Iceberg REST /v1/config failed"
    );

    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("socket.timeout.ms", "2000")
        .create()
        .expect("create Kafka dependency probe");
    admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(2))
        .expect("Kafka/Redpanda must be reachable");
}

async fn create_topic(topic: &str) {
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .create()
        .expect("create Kafka admin client");
    let new_topic = NewTopic::new(topic, INPUT_PARTITIONS, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .expect("create Iceberg cluster input topic");
    for result in results {
        if let Err((failed_topic, error)) = result {
            panic!("create Kafka topic {failed_topic}: {error}");
        }
    }
}

async fn create_table(client: &reqwest::Client, namespace: &str, table: &str) {
    let namespace_response = client
        .post(format!("{CATALOG_URI}/v1/namespaces"))
        .json(&serde_json::json!({
            "namespace": [namespace],
            "properties": {}
        }))
        .send()
        .await
        .expect("create Iceberg namespace");
    assert!(
        namespace_response.status().is_success(),
        "create Iceberg namespace returned {}",
        namespace_response.status()
    );
    let table_response = client
        .post(format!("{CATALOG_URI}/v1/namespaces/{namespace}/tables"))
        .json(&serde_json::json!({
            "name": table,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [{
                    "id": 1,
                    "name": "id",
                    "required": true,
                    "type": "long"
                }],
                "identifier-field-ids": []
            },
            "stage-create": false,
            "properties": { "format-version": "2" }
        }))
        .send()
        .await
        .expect("create Iceberg table");
    assert!(
        table_response.status().is_success(),
        "create Iceberg table returned {}",
        table_response.status()
    );
}

async fn produce_rows(topic: &str, start: i64, count: i64) {
    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .create()
        .expect("create Kafka producer");
    for id in start..start + count {
        let payload = format!(r#"{{"id":{id}}}"#);
        let key = id.to_string();
        let partition = i32::try_from(id.rem_euclid(i64::from(INPUT_PARTITIONS)))
            .expect("Kafka partition fits i32");
        producer
            .send_result(
                FutureRecord::to(topic)
                    .payload(&payload)
                    .key(&key)
                    .partition(partition),
            )
            .unwrap_or_else(|(error, _)| panic!("enqueue Kafka input: {error}"))
            .await
            .expect("Kafka delivery future was cancelled")
            .unwrap_or_else(|(error, _)| panic!("deliver Kafka input: {error}"));
    }
}

impl ClusterFixture<'_> {
    fn write_node_config(&self, id: usize) -> PathBuf {
        let seeds = self
            .gossip_ports
            .iter()
            .map(|port| format!(r#""127.0.0.1:{port}""#))
            .collect::<Vec<_>>()
            .join(", ");
        let checkpoint_url = format!("s3://{S3_BUCKET}/cluster-checkpoints/{}", self.run_id);
        let config = format!(
            r#"
node_id = "iceberg-n{id}"

[server]
mode = "cluster"
bind = "127.0.0.1:{http_port}"
delivery = "exactly_once"
key_groups = 12
console_token = "{CONSOLE_TOKEN}"

[discovery]
strategy = "static"
seeds = [{seeds}]
gossip_port = {gossip_port}
advertise_host = "127.0.0.1"

[checkpoint]
url = "{checkpoint_url}"
interval = "1s"
timeout = "30s"

[checkpoint.storage]
endpoint = "{S3_ENDPOINT}"
aws_access_key_id = "{S3_ACCESS_KEY}"
aws_secret_access_key = "${{ICEBERG_CLUSTER_S3_SECRET_KEY}}"
region = "{S3_REGION}"
allow_http = "true"

[[source]]
name = "events_in"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{BROKERS}"
topic = "{topic}"
"group.id" = "iceberg-cluster-{run_id}"
"startup.mode" = "earliest"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false

[[pipeline]]
name = "events"
sql = "SELECT id FROM events_in"

[[sink]]
name = "{SINK_NAME}"
pipeline = "events"
connector = "iceberg"
[sink.properties]
"catalog.type" = "rest"
"catalog.uri" = "{CATALOG_URI}"
"catalog.warehouse" = "s3://{S3_BUCKET}/wh"
"catalog.request_timeout" = "10s"
"catalog.commit_timeout" = "30s"
"storage.type" = "s3"
namespace = "{namespace}"
"table.name" = "{table}"
"write.mode" = "append"
"target.file.size.bytes" = "1048576"
"parquet.row.group.size.bytes" = "262144"
"max.buffer.rows" = "128"
"max.buffer.bytes" = "1048576"
"max.open.partitions" = "1"
"max.files.per.checkpoint" = "64"
"max.descriptor.bytes" = "1048576"
"catalog.property.s3.endpoint" = "{S3_ENDPOINT}"
"catalog.property.s3.access-key-id" = "{S3_ACCESS_KEY}"
"catalog.property.s3.secret-access-key" = "$${{ICEBERG_CLUSTER_S3_SECRET_KEY}}"
"catalog.property.s3.region" = "{S3_REGION}"
"catalog.property.s3.path-style-access" = "true"
"#,
            http_port = self.http_ports[id],
            gossip_port = self.gossip_ports[id],
            run_id = self.run_id,
            namespace = self.namespace,
            table = self.table,
            topic = self.topic,
        );
        let path = self.directory.join(format!("node{id}.toml"));
        std::fs::write(&path, config).expect("write Iceberg cluster node config");
        path
    }
}

fn free_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve test port"))
        .collect::<Vec<_>>();
    listeners
        .iter()
        .map(|listener| listener.local_addr().expect("read test port").port())
        .collect()
}

async fn wait_for_full_cluster(client: &reqwest::Client, nodes: &mut [Node]) {
    let deadline = tokio::time::Instant::now() + RECOVERY_TIMEOUT;
    loop {
        let mut ready = true;
        for node in nodes.iter_mut() {
            node.assert_running();
            ready &= node_ready(client, node).await;
            ready &= active_peer_count(client, node).await == Some(NODE_COUNT - 1);
        }
        if ready && observed_leader(client, nodes).await.is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Iceberg cluster did not reach full ready membership"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_stable_leader(client: &reqwest::Client, nodes: &mut [Node]) -> usize {
    let deadline = tokio::time::Instant::now() + RECOVERY_TIMEOUT;
    let mut candidate = None;
    let mut stable_samples = 0_u8;
    loop {
        for node in nodes.iter_mut().filter(|node| node.child.is_some()) {
            node.assert_running();
        }
        let observed = observed_leader(client, nodes).await;
        if observed == candidate && observed.is_some() {
            stable_samples = stable_samples.saturating_add(1);
        } else {
            candidate = observed;
            stable_samples = u8::from(observed.is_some());
        }
        if stable_samples >= 3 {
            return candidate.expect("stable leader candidate");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Iceberg cluster did not elect one stable leader"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn observed_leader(client: &reqwest::Client, nodes: &[Node]) -> Option<usize> {
    let mut leaders = Vec::new();
    for node in nodes.iter().filter(|node| node.child.is_some()) {
        let response = node_json(client, node, "/api/v1/cluster/leader").await?;
        if response.get("is_leader")?.as_bool()? {
            leaders.push(node.id);
        }
    }
    match leaders.as_slice() {
        [leader] => Some(*leader),
        _ => None,
    }
}

async fn node_ready(client: &reqwest::Client, node: &Node) -> bool {
    client
        .get(format!("http://127.0.0.1:{}/ready", node.http_port))
        .bearer_auth(CONSOLE_TOKEN)
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

async fn active_peer_count(client: &reqwest::Client, node: &Node) -> Option<usize> {
    Some(
        node_json(client, node, "/api/v1/cluster/nodes")
            .await?
            .as_array()?
            .iter()
            .filter(|peer| peer.get("state").and_then(serde_json::Value::as_str) == Some("Active"))
            .count(),
    )
}

async fn node_json(client: &reqwest::Client, node: &Node, path: &str) -> Option<serde_json::Value> {
    client
        .get(format!("http://127.0.0.1:{}{path}", node.http_port))
        .bearer_auth(CONSOLE_TOKEN)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()
}

async fn wait_for_external_commit_gate(node: &mut Node) -> GateEvidence {
    let deadline = tokio::time::Instant::now() + RECOVERY_TIMEOUT;
    let ready_path = node.gate_path.with_extension("ready");
    loop {
        node.assert_running();
        if let Ok(evidence) = std::fs::read_to_string(&ready_path) {
            let fields = evidence.split_ascii_whitespace().collect::<Vec<_>>();
            assert_eq!(
                fields.len(),
                4,
                "external commit gate evidence is malformed"
            );
            assert_eq!(fields[0], SINK_NAME);
            return GateEvidence {
                checkpoint_id: fields[1].parse().expect("gate checkpoint ID"),
                epoch: fields[2].parse().expect("gate epoch"),
                fencing_token: fields[3].parse().expect("gate fencing token"),
            };
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader did not reach the non-empty external sink commit gate"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_total_records(
    client: &reqwest::Client,
    nodes: &mut [Node],
    namespace: &str,
    table: &str,
    expected: u64,
) -> TableMetadata {
    let deadline = tokio::time::Instant::now() + RECOVERY_TIMEOUT;
    loop {
        for node in nodes.iter_mut().filter(|node| node.child.is_some()) {
            node.assert_running();
        }
        if let Ok(metadata) = load_table(client, namespace, table).await {
            let actual = metadata.total_records();
            assert!(
                actual <= expected,
                "Iceberg table contains duplicate rows: expected {expected}, found {actual}"
            );
            if actual == expected {
                return metadata;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Iceberg table did not reach {expected} committed rows"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_checkpoint_snapshot(
    client: &reqwest::Client,
    nodes: &mut [Node],
    namespace: &str,
    table: &str,
    checkpoint_id: u64,
) -> TableMetadata {
    let deadline = tokio::time::Instant::now() + RECOVERY_TIMEOUT;
    loop {
        for node in nodes.iter_mut().filter(|node| node.child.is_some()) {
            node.assert_running();
        }
        if let Ok(metadata) = load_table(client, namespace, table).await {
            if metadata.commit_uuid(checkpoint_id).is_some() {
                metadata.validate_exact_commit_identities();
                return metadata;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "faulted checkpoint {checkpoint_id} did not become visible in Iceberg"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn load_table(
    client: &reqwest::Client,
    namespace: &str,
    table: &str,
) -> Result<TableMetadata, String> {
    let response = client
        .get(format!(
            "{CATALOG_URI}/v1/namespaces/{namespace}/tables/{table}"
        ))
        .send()
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?;
    let bytes = response.bytes().await.map_err(|error| error.to_string())?;
    if bytes.len() > CATALOG_RESPONSE_LIMIT {
        return Err(format!(
            "Iceberg load-table response exceeds {CATALOG_RESPONSE_LIMIT} bytes"
        ));
    }
    serde_json::from_slice::<LoadTableResponse>(&bytes)
        .map(|response| response.metadata)
        .map_err(|error| error.to_string())
}

async fn verify_exact_data_files(metadata: &TableMetadata, expected_rows: u64) {
    assert_eq!(metadata.total_records(), expected_rows);
    let prefix = metadata
        .location
        .strip_prefix(&format!("s3://{S3_BUCKET}/"))
        .expect("Iceberg table location must use the test bucket");
    let store = AmazonS3Builder::new()
        .with_bucket_name(S3_BUCKET)
        .with_access_key_id(S3_ACCESS_KEY)
        .with_secret_access_key(S3_SECRET_KEY)
        .with_region(S3_REGION)
        .with_endpoint(S3_ENDPOINT)
        .with_allow_http(true)
        .build()
        .expect("build MinIO data-file verifier");
    let expected_files = metadata.total_data_files();
    let mut objects = store.list(Some(&ObjectPath::from(format!("{prefix}/data"))));
    let mut parquet_count = 0_u64;
    let expected_end = i64::try_from(expected_rows).expect("expected row count fits i64");
    let mut observed_ids = BTreeSet::new();
    while let Some(object) = objects.try_next().await.expect("list Iceberg data files") {
        assert!(
            !object.location.as_ref().contains("-stage-"),
            "a publish-ineligible staging object survived recovery"
        );
        if !object.location.as_ref().ends_with(".parquet") {
            continue;
        }
        parquet_count = parquet_count
            .checked_add(1)
            .expect("data-file count must not overflow");
        assert!(
            parquet_count <= expected_files,
            "listed Parquet files exceed Iceberg's current data-file count"
        );
        assert!(object.size >= 8, "Iceberg data file is incomplete");
        assert!(
            object.size <= DATA_FILE_ORACLE_LIMIT,
            "Iceberg data file exceeds the bounded soak oracle"
        );
        let object_size = usize::try_from(object.size).expect("bounded data-file size fits usize");
        let bytes = store
            .get(&object.location)
            .await
            .expect("read Iceberg data file")
            .bytes()
            .await
            .expect("collect bounded Iceberg data file");
        assert_eq!(bytes.len(), object_size, "Iceberg data file was truncated");
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("read Iceberg Parquet metadata")
            .with_batch_size(128)
            .build()
            .expect("build Iceberg Parquet row oracle");
        for batch in reader {
            let batch = batch.expect("decode Iceberg Parquet rows");
            let id_index = batch.schema().index_of("id").expect("Iceberg id column");
            let ids = batch
                .column(id_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Iceberg id column must be Int64");
            for row in 0..ids.len() {
                assert!(!ids.is_null(row), "Iceberg id must not be null");
                let id = ids.value(row);
                assert!(
                    (0..expected_end).contains(&id),
                    "Iceberg contains out-of-range id {id}"
                );
                assert!(
                    observed_ids.insert(id),
                    "Iceberg contains duplicate id {id} after checkpoint replay"
                );
            }
        }
    }
    assert_eq!(
        parquet_count, expected_files,
        "listed Parquet files differ from Iceberg's current data-file count"
    );
    assert_eq!(
        observed_ids.len(),
        usize::try_from(expected_rows).expect("expected row count fits usize"),
        "Iceberg is missing committed input rows"
    );
}

fn remove_file_if_present(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("remove {}: {error}", path.display()),
    }
}
