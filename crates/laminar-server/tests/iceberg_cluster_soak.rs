//! Real-process Kafka-to-Iceberg cluster recovery test.
//!
//! The default `minio` profile targets the repository's local REST/MinIO stack. Set
//! `ICEBERG_CLUSTER_PROFILE=aws` to certify an external HTTPS REST catalog and real S3. The AWS
//! profile uses the standard provider credential chain and requires an explicit visibility SLO.

#![cfg(all(
    feature = "cluster",
    feature = "aws",
    feature = "kafka",
    feature = "iceberg"
))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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
const DEFAULT_BROKERS: &str = "127.0.0.1:19092";
const MINIO_CATALOG_URI: &str = "http://127.0.0.1:8181";
const MINIO_S3_ENDPOINT: &str = "http://127.0.0.1:9000";
const MINIO_S3_BUCKET: &str = "warehouse";
const MINIO_S3_ACCESS_KEY: &str = "minioadmin";
const MINIO_S3_SECRET_KEY: &str = "minioadmin";
const DEFAULT_S3_REGION: &str = "us-east-1";
const S3_SECRET_ENV: &str = "ICEBERG_CLUSTER_S3_SECRET_KEY";
const CATALOG_BEARER_TOKEN_ENV: &str = "ICEBERG_CLUSTER_CATALOG_BEARER_TOKEN";
const PROFILE_ENV: &str = "ICEBERG_CLUSTER_PROFILE";
const CATALOG_URI_ENV: &str = "ICEBERG_CLUSTER_CATALOG_URI";
const CATALOG_AUTH_ENV: &str = "ICEBERG_CLUSTER_CATALOG_AUTH_TYPE";
const S3_BUCKET_ENV: &str = "ICEBERG_CLUSTER_S3_BUCKET";
const S3_REGION_ENV: &str = "ICEBERG_CLUSTER_S3_REGION";
const S3_PREFIX_ENV: &str = "ICEBERG_CLUSTER_S3_PREFIX";
const BROKERS_ENV: &str = "ICEBERG_CLUSTER_KAFKA_BROKERS";
const VISIBILITY_SLO_ENV: &str = "ICEBERG_CLUSTER_VISIBILITY_SLO_MS";
const RECOVERY_TIMEOUT_ENV: &str = "ICEBERG_CLUSTER_RECOVERY_TIMEOUT_MS";
const SINK_NAME: &str = "iceberg_out";
const FIRST_ROWS: i64 = 30;
const SECOND_ROWS: i64 = 30;
const THIRD_ROWS: i64 = 30;
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(120);
const CATALOG_RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
const DATA_FILE_ORACLE_LIMIT: u64 = 8 * 1024 * 1024;
const MAX_EXTERNAL_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClusterProfileKind {
    Minio,
    Aws,
}

impl ClusterProfileKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Minio => "minio",
            Self::Aws => "aws",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CatalogAuthentication {
    None,
    Bearer,
}

#[derive(Clone, Debug)]
struct ClusterProfile {
    kind: ClusterProfileKind,
    brokers: String,
    catalog_uri: String,
    catalog_authentication: CatalogAuthentication,
    s3_bucket: String,
    s3_region: String,
    s3_prefix: String,
    recovery_timeout: Duration,
    visibility_slo: Option<Duration>,
}

impl ClusterProfile {
    fn from_environment() -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, String> {
        let kind = match lookup(PROFILE_ENV).as_deref().unwrap_or("minio") {
            "minio" => ClusterProfileKind::Minio,
            "aws" => ClusterProfileKind::Aws,
            value => {
                return Err(format!(
                    "{PROFILE_ENV} must be 'minio' or 'aws', got {value:?}"
                ))
            }
        };
        let brokers = optional_nonempty(&mut lookup, BROKERS_ENV, DEFAULT_BROKERS)?;
        validate_inline_value(BROKERS_ENV, &brokers)?;
        let recovery_timeout = optional_duration(
            &mut lookup,
            RECOVERY_TIMEOUT_ENV,
            RECOVERY_TIMEOUT,
            MAX_EXTERNAL_TIMEOUT,
        )?;
        if kind == ClusterProfileKind::Minio {
            return Ok(Self {
                kind,
                brokers,
                catalog_uri: MINIO_CATALOG_URI.into(),
                catalog_authentication: CatalogAuthentication::None,
                s3_bucket: MINIO_S3_BUCKET.into(),
                s3_region: DEFAULT_S3_REGION.into(),
                s3_prefix: String::new(),
                recovery_timeout,
                visibility_slo: None,
            });
        }

        let catalog_uri = required_nonempty(&mut lookup, CATALOG_URI_ENV)?;
        validate_external_catalog_uri(&catalog_uri)?;
        reject_aws_endpoint_overrides(&mut lookup)?;
        let catalog_authentication = match lookup(CATALOG_AUTH_ENV).as_deref().unwrap_or("none") {
            "none" => CatalogAuthentication::None,
            "bearer" => {
                let token = required_nonempty(&mut lookup, CATALOG_BEARER_TOKEN_ENV)?;
                if token.len() > 16 * 1024 {
                    return Err(format!(
                        "{CATALOG_BEARER_TOKEN_ENV} exceeds the 16384-byte test limit"
                    ));
                }
                CatalogAuthentication::Bearer
            }
            value => {
                return Err(format!(
                    "{CATALOG_AUTH_ENV} must be 'none' or 'bearer', got {value:?}"
                ));
            }
        };
        let s3_bucket = required_nonempty(&mut lookup, S3_BUCKET_ENV)?;
        validate_s3_bucket(&s3_bucket)?;
        let s3_region = required_nonempty(&mut lookup, S3_REGION_ENV)?;
        validate_region(&s3_region)?;
        let s3_prefix = optional_nonempty(
            &mut lookup,
            S3_PREFIX_ENV,
            "laminardb-iceberg-certification",
        )?
        .trim_matches('/')
        .to_owned();
        validate_s3_prefix(&s3_prefix)?;
        let visibility_slo =
            required_duration(&mut lookup, VISIBILITY_SLO_ENV, MAX_EXTERNAL_TIMEOUT)?;
        Ok(Self {
            kind,
            brokers,
            catalog_uri: catalog_uri.trim_end_matches('/').to_owned(),
            catalog_authentication,
            s3_bucket,
            s3_region,
            s3_prefix,
            recovery_timeout,
            visibility_slo: Some(visibility_slo),
        })
    }

    fn warehouse_uri(&self) -> String {
        self.s3_uri("wh")
    }

    fn checkpoint_url(&self, run_id: &str) -> String {
        self.s3_uri(&format!("cluster-checkpoints/{run_id}"))
    }

    fn s3_uri(&self, suffix: &str) -> String {
        let key = if self.s3_prefix.is_empty() {
            suffix.to_owned()
        } else {
            format!("{}/{suffix}", self.s3_prefix)
        };
        format!("s3://{}/{key}", self.s3_bucket)
    }

    fn catalog_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.catalog_authentication {
            CatalogAuthentication::None => request,
            CatalogAuthentication::Bearer => request.bearer_auth(
                std::env::var(CATALOG_BEARER_TOKEN_ENV)
                    .expect("validated Iceberg catalog bearer token is unavailable"),
            ),
        }
    }

    fn catalog_auth_toml(&self) -> String {
        match self.catalog_authentication {
            CatalogAuthentication::None => String::new(),
            CatalogAuthentication::Bearer => format!(
                "\"catalog.auth.type\" = \"bearer\"\n\"catalog.property.token\" = \"$${{{CATALOG_BEARER_TOKEN_ENV}}}\"\n"
            ),
        }
    }

    fn checkpoint_storage_toml(&self) -> String {
        match self.kind {
            ClusterProfileKind::Minio => format!(
                "endpoint = \"{MINIO_S3_ENDPOINT}\"\naws_access_key_id = \"{MINIO_S3_ACCESS_KEY}\"\naws_secret_access_key = \"${{{S3_SECRET_ENV}}}\"\nregion = \"{}\"\nallow_http = \"true\"\n",
                self.s3_region
            ),
            ClusterProfileKind::Aws => format!("region = \"{}\"\n", self.s3_region),
        }
    }

    fn iceberg_storage_toml(&self) -> String {
        match self.kind {
            ClusterProfileKind::Minio => format!(
                "\"storage.endpoint\" = \"{MINIO_S3_ENDPOINT}\"\n\"storage.region\" = \"{}\"\n\"storage.path_style\" = \"true\"\n\"storage.property.s3.access-key-id\" = \"{MINIO_S3_ACCESS_KEY}\"\n\"storage.property.s3.secret-access-key\" = \"$${{{S3_SECRET_ENV}}}\"\n",
                self.s3_region
            ),
            ClusterProfileKind::Aws => format!("\"storage.region\" = \"{}\"\n", self.s3_region),
        }
    }

    fn report_visibility(&self, label: &str, elapsed: Duration) {
        eprintln!(
            "Iceberg cluster {} profile {label} visibility_ms={:.3}",
            self.kind.label(),
            elapsed.as_secs_f64() * 1_000.0
        );
        if let Some(limit) = self.visibility_slo {
            assert!(
                elapsed <= limit,
                "Iceberg cluster AWS profile {label} visibility {elapsed:?} exceeded configured {limit:?} SLO"
            );
        }
    }
}

fn required_nonempty(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<String, String> {
    let value = lookup(name).ok_or_else(|| format!("{name} is required"))?;
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    Ok(value.to_owned())
}

fn optional_nonempty(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: &str,
) -> Result<String, String> {
    match lookup(name) {
        Some(value) if value.trim().is_empty() => Err(format!("{name} must not be empty")),
        Some(value) => Ok(value.trim().to_owned()),
        None => Ok(default.to_owned()),
    }
}

fn required_duration(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    maximum: Duration,
) -> Result<Duration, String> {
    let value = required_nonempty(lookup, name)?;
    parse_duration(name, &value, maximum)
}

fn optional_duration(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: Duration,
    maximum: Duration,
) -> Result<Duration, String> {
    match lookup(name) {
        Some(value) => parse_duration(name, value.trim(), maximum),
        None => Ok(default),
    }
}

fn parse_duration(name: &str, milliseconds: &str, maximum: Duration) -> Result<Duration, String> {
    let milliseconds = milliseconds
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer number of milliseconds"))?;
    let duration = Duration::from_millis(milliseconds);
    if duration.is_zero() || duration > maximum {
        return Err(format!(
            "{name} must be between 1 and {} milliseconds",
            maximum.as_millis()
        ));
    }
    Ok(duration)
}

fn validate_inline_value(name: &str, value: &str) -> Result<(), String> {
    if value.len() > 2_048
        || value
            .chars()
            .any(|character| character.is_control() || matches!(character, '"' | '\\'))
    {
        return Err(format!(
            "{name} cannot be represented safely in the generated TOML"
        ));
    }
    Ok(())
}

fn validate_external_catalog_uri(value: &str) -> Result<(), String> {
    validate_inline_value(CATALOG_URI_ENV, value)?;
    let uri = reqwest::Url::parse(value)
        .map_err(|_| format!("{CATALOG_URI_ENV} must be a valid HTTPS URL"))?;
    if uri.scheme() != "https"
        || !uri.username().is_empty()
        || uri.password().is_some()
        || uri.query().is_some()
        || uri.fragment().is_some()
    {
        return Err(format!(
            "{CATALOG_URI_ENV} must be an HTTPS URL without credentials, query, or fragment"
        ));
    }
    Ok(())
}

fn validate_s3_bucket(value: &str) -> Result<(), String> {
    let valid_length = (3..=63).contains(&value.len());
    let valid_characters = value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
    });
    let bounded = value
        .as_bytes()
        .first()
        .zip(value.as_bytes().last())
        .is_some_and(|(first, last)| first.is_ascii_alphanumeric() && last.is_ascii_alphanumeric());
    if !valid_length
        || !valid_characters
        || !bounded
        || value.contains("..")
        || value.contains(".-")
        || value.contains("-.")
    {
        return Err(format!("{S3_BUCKET_ENV} is not a valid S3 bucket name"));
    }
    Ok(())
}

fn validate_region(value: &str) -> Result<(), String> {
    if value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(format!("{S3_REGION_ENV} is not a valid AWS region"));
    }
    Ok(())
}

fn validate_s3_prefix(value: &str) -> Result<(), String> {
    validate_inline_value(S3_PREFIX_ENV, value)?;
    if value.is_empty() || value.len() > 1_024 || ObjectPath::parse(value).is_err() {
        return Err(format!("{S3_PREFIX_ENV} is not a valid object-store path"));
    }
    Ok(())
}

fn reject_aws_endpoint_overrides(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<(), String> {
    for name in [
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT",
        "AWS_S3_ENDPOINT",
        "AWS_ENDPOINT_URL_S3",
    ] {
        if lookup(name).is_some() {
            return Err(format!(
                "{name} is not allowed with {PROFILE_ENV}=aws because it would bypass real S3"
            ));
        }
    }
    Ok(())
}

struct Node {
    id: usize,
    profile_kind: ClusterProfileKind,
    http_port: u16,
    config_path: PathBuf,
    log_path: PathBuf,
    gate_path: PathBuf,
    child: Option<Child>,
}

struct ClusterFixture<'a> {
    profile: &'a ClusterProfile,
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
        let mut command = Command::new(env!("CARGO_BIN_EXE_laminardb"));
        command
            .arg("--config")
            .arg(&self.config_path)
            .env("LAMINAR_EXTERNAL_SINK_COMMIT_GATE_FILE", &self.gate_path)
            .env(
                "RUST_LOG",
                "laminardb=info,laminar_server=info,laminar_db=info",
            )
            .env("NO_COLOR", "1")
            .stdout(Stdio::from(log.try_clone().expect("clone node log")))
            .stderr(Stdio::from(log));
        if self.profile_kind == ClusterProfileKind::Minio {
            command.env(S3_SECRET_ENV, MINIO_S3_SECRET_KEY);
        }
        let child = command
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
            for line in log.lines().rev().take(240).collect::<Vec<_>>().iter().rev() {
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
#[ignore = "spawns three real laminardb processes with Kafka, Iceberg REST, and S3"]
async fn leader_restart_reconciles_one_iceberg_snapshot_per_checkpoint() {
    let profile = ClusterProfile::from_environment()
        .unwrap_or_else(|error| panic!("invalid Iceberg cluster soak profile: {error}"));
    eprintln!("Iceberg cluster soak profile={}", profile.kind.label());
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .expect("build integration HTTP client");
    require_dependencies(&client, &profile).await;

    let run_id = uuid::Uuid::now_v7().simple().to_string();
    let namespace = format!("cluster_{run_id}");
    let table = "events";
    let topic = format!("iceberg-cluster-{run_id}");
    create_table(&client, &profile, &namespace, table).await;
    create_topic(&profile, &topic).await;

    let directory = tempfile::tempdir().expect("create Iceberg cluster test directory");
    eprintln!("Iceberg cluster node logs: {}", directory.path().display());
    let ports = free_ports(NODE_COUNT * 2);
    let (http_ports, gossip_ports) = ports.split_at(NODE_COUNT);
    let fixture = ClusterFixture {
        profile: &profile,
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
                profile_kind: profile.kind,
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
    wait_for_full_cluster(&client, &profile, &mut nodes).await;

    let first_started = Instant::now();
    produce_rows(&profile, &topic, 0, FIRST_ROWS).await;
    let first = wait_for_total_records(
        &client,
        &profile,
        &mut nodes,
        &namespace,
        table,
        FIRST_ROWS as u64,
    )
    .await;
    profile.report_visibility("initial append", first_started.elapsed());
    first.validate_exact_commit_identities();

    let leader = wait_for_stable_leader(&client, &profile, &mut nodes).await;
    nodes[leader].arm_external_commit_gate();
    produce_rows(&profile, &topic, FIRST_ROWS, SECOND_ROWS).await;
    let gate = wait_for_external_commit_gate(&profile, &mut nodes[leader]).await;
    assert!(gate.epoch > 0 && gate.fencing_token > 0);
    let fault_visibility_started = Instant::now();
    let fault_state = wait_for_checkpoint_snapshot(
        &client,
        &profile,
        &mut nodes,
        &namespace,
        table,
        gate.checkpoint_id,
    )
    .await;
    profile.report_visibility(
        "faulted checkpoint reconciliation",
        fault_visibility_started.elapsed(),
    );
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
    wait_for_stable_leader(&client, &profile, &mut nodes).await;
    let recovery_started = Instant::now();
    let recovered = wait_for_total_records(
        &client,
        &profile,
        &mut nodes,
        &namespace,
        table,
        (FIRST_ROWS + SECOND_ROWS) as u64,
    )
    .await;
    profile.report_visibility("leader recovery", recovery_started.elapsed());
    recovered.validate_exact_commit_identities();
    assert_eq!(
        recovered.commit_uuid(gate.checkpoint_id),
        Some(fault_commit_uuid.as_str())
    );

    nodes[leader].spawn();
    wait_for_full_cluster(&client, &profile, &mut nodes).await;
    let post_restart_started = Instant::now();
    produce_rows(&profile, &topic, FIRST_ROWS + SECOND_ROWS, THIRD_ROWS).await;
    let expected_rows = (FIRST_ROWS + SECOND_ROWS + THIRD_ROWS) as u64;
    let final_state = wait_for_total_records(
        &client,
        &profile,
        &mut nodes,
        &namespace,
        table,
        expected_rows,
    )
    .await;
    profile.report_visibility("post-restart append", post_restart_started.elapsed());
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
    verify_exact_data_files(&profile, &final_state, expected_rows).await;
}

async fn require_dependencies(client: &reqwest::Client, profile: &ClusterProfile) {
    let warehouse = profile.warehouse_uri();
    let mut config_url = reqwest::Url::parse(&format!("{}/v1/config", profile.catalog_uri))
        .expect("validated Iceberg catalog URI must form a config URL");
    config_url
        .query_pairs_mut()
        .append_pair("warehouse", &warehouse);
    let config = profile
        .catalog_request(client.get(config_url))
        .send()
        .await
        .expect("Iceberg REST catalog must be reachable");
    assert!(
        config.status().is_success(),
        "Iceberg REST /v1/config failed"
    );

    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &profile.brokers)
        .set("socket.timeout.ms", "2000")
        .create()
        .expect("create Kafka dependency probe");
    admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(2))
        .expect("Kafka/Redpanda must be reachable");
}

async fn create_topic(profile: &ClusterProfile, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &profile.brokers)
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

async fn create_table(
    client: &reqwest::Client,
    profile: &ClusterProfile,
    namespace: &str,
    table: &str,
) {
    let namespace_response = profile
        .catalog_request(client.post(format!("{}/v1/namespaces", profile.catalog_uri)))
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
    let table_response = profile
        .catalog_request(client.post(format!(
            "{}/v1/namespaces/{namespace}/tables",
            profile.catalog_uri
        )))
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

async fn produce_rows(profile: &ClusterProfile, topic: &str, start: i64, count: i64) {
    let producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &profile.brokers)
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
        let checkpoint_url = self.profile.checkpoint_url(self.run_id);
        let checkpoint_storage = self.profile.checkpoint_storage_toml();
        let catalog_auth = self.profile.catalog_auth_toml();
        let iceberg_storage = self.profile.iceberg_storage_toml();
        let warehouse_uri = self.profile.warehouse_uri();
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
{checkpoint_storage}

[[source]]
name = "events_in"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
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
"catalog.uri" = "{catalog_uri}"
"catalog.warehouse" = "{warehouse_uri}"
"catalog.request_timeout" = "10s"
"catalog.commit_timeout" = "30s"
{catalog_auth}
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
{iceberg_storage}
"#,
            http_port = self.http_ports[id],
            gossip_port = self.gossip_ports[id],
            brokers = self.profile.brokers.as_str(),
            catalog_uri = self.profile.catalog_uri.as_str(),
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

async fn wait_for_full_cluster(
    client: &reqwest::Client,
    profile: &ClusterProfile,
    nodes: &mut [Node],
) {
    let deadline = tokio::time::Instant::now() + profile.recovery_timeout;
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

async fn wait_for_stable_leader(
    client: &reqwest::Client,
    profile: &ClusterProfile,
    nodes: &mut [Node],
) -> usize {
    let deadline = tokio::time::Instant::now() + profile.recovery_timeout;
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

async fn wait_for_external_commit_gate(profile: &ClusterProfile, node: &mut Node) -> GateEvidence {
    let deadline = tokio::time::Instant::now() + profile.recovery_timeout;
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
        if tokio::time::Instant::now() >= deadline {
            node.dump_log();
            panic!("leader did not reach the non-empty external sink commit gate");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_total_records(
    client: &reqwest::Client,
    profile: &ClusterProfile,
    nodes: &mut [Node],
    namespace: &str,
    table: &str,
    expected: u64,
) -> TableMetadata {
    let deadline = tokio::time::Instant::now() + profile.recovery_timeout;
    loop {
        for node in nodes.iter_mut().filter(|node| node.child.is_some()) {
            node.assert_running();
        }
        if let Ok(metadata) = load_table(client, profile, namespace, table).await {
            let actual = metadata.total_records();
            assert!(
                actual <= expected,
                "Iceberg table contains duplicate rows: expected {expected}, found {actual}"
            );
            if actual == expected {
                return metadata;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            for node in nodes.iter() {
                node.dump_log();
            }
            panic!("Iceberg table did not reach {expected} committed rows");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_checkpoint_snapshot(
    client: &reqwest::Client,
    profile: &ClusterProfile,
    nodes: &mut [Node],
    namespace: &str,
    table: &str,
    checkpoint_id: u64,
) -> TableMetadata {
    let deadline = tokio::time::Instant::now() + profile.recovery_timeout;
    loop {
        for node in nodes.iter_mut().filter(|node| node.child.is_some()) {
            node.assert_running();
        }
        if let Ok(metadata) = load_table(client, profile, namespace, table).await {
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
    profile: &ClusterProfile,
    namespace: &str,
    table: &str,
) -> Result<TableMetadata, String> {
    let response = profile
        .catalog_request(client.get(format!(
            "{}/v1/namespaces/{namespace}/tables/{table}",
            profile.catalog_uri
        )))
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

async fn verify_exact_data_files(
    profile: &ClusterProfile,
    metadata: &TableMetadata,
    expected_rows: u64,
) {
    assert_eq!(metadata.total_records(), expected_rows);
    let prefix = metadata
        .location
        .strip_prefix(&format!("s3://{}/", profile.s3_bucket))
        .expect("Iceberg table location must use the configured test bucket");
    assert!(
        metadata.location.starts_with(&profile.warehouse_uri()),
        "Iceberg table location must remain under the configured warehouse"
    );
    let builder = match profile.kind {
        ClusterProfileKind::Minio => AmazonS3Builder::new()
            .with_access_key_id(MINIO_S3_ACCESS_KEY)
            .with_secret_access_key(MINIO_S3_SECRET_KEY)
            .with_endpoint(MINIO_S3_ENDPOINT)
            .with_allow_http(true),
        ClusterProfileKind::Aws => AmazonS3Builder::from_env(),
    };
    let store = builder
        .with_bucket_name(&profile.s3_bucket)
        .with_region(&profile.s3_region)
        .build()
        .expect("build S3 data-file verifier");
    let expected_files = metadata.total_data_files();
    let data_prefix = ObjectPath::parse(format!("{prefix}/data"))
        .expect("Iceberg table data prefix must be a valid object-store path");
    let mut objects = store.list(Some(&data_prefix));
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

#[cfg(test)]
mod profile_tests {
    use super::*;

    const TEST_SECRET: &str = "catalog-token-that-must-not-be-retained";

    fn aws_values() -> BTreeMap<&'static str, &'static str> {
        BTreeMap::from([
            (PROFILE_ENV, "aws"),
            (CATALOG_URI_ENV, "https://catalog.example.test/api"),
            (CATALOG_AUTH_ENV, "bearer"),
            (CATALOG_BEARER_TOKEN_ENV, TEST_SECRET),
            (S3_BUCKET_ENV, "laminardb-certification"),
            (S3_REGION_ENV, "eu-west-1"),
            (S3_PREFIX_ENV, "team/iceberg-certification"),
            (BROKERS_ENV, "kafka.example.test:9092"),
            (VISIBILITY_SLO_ENV, "120000"),
            (RECOVERY_TIMEOUT_ENV, "180000"),
        ])
    }

    fn profile_from(values: &BTreeMap<&str, &str>) -> Result<ClusterProfile, String> {
        ClusterProfile::from_lookup(|name| values.get(name).map(|value| (*value).to_owned()))
    }

    #[test]
    fn minio_profile_is_the_credential_safe_default() {
        let profile = profile_from(&BTreeMap::new()).expect("default MinIO profile");

        assert_eq!(profile.kind, ClusterProfileKind::Minio);
        assert_eq!(profile.catalog_uri, MINIO_CATALOG_URI);
        assert_eq!(profile.warehouse_uri(), "s3://warehouse/wh");
        assert_eq!(profile.recovery_timeout, RECOVERY_TIMEOUT);
        assert_eq!(profile.visibility_slo, None);
        assert!(profile.catalog_auth_toml().is_empty());
        assert!(profile
            .checkpoint_storage_toml()
            .contains("${ICEBERG_CLUSTER_S3_SECRET_KEY}"));
        let storage = profile.iceberg_storage_toml();
        assert!(!storage.contains("catalog.property"));
        assert!(storage.contains(
            "\"storage.property.s3.secret-access-key\" = \"$${ICEBERG_CLUSTER_S3_SECRET_KEY}\""
        ));
        assert!(!storage.contains("\"storage.property.s3.secret-access-key\" = \"minioadmin\""));
        assert!(!format!("{profile:?}").contains(MINIO_S3_SECRET_KEY));
    }

    #[test]
    fn aws_profile_retains_references_not_bearer_material() {
        let profile = profile_from(&aws_values()).expect("valid AWS profile");

        assert_eq!(profile.kind, ClusterProfileKind::Aws);
        assert_eq!(profile.recovery_timeout, Duration::from_secs(180));
        assert_eq!(profile.visibility_slo, Some(Duration::from_secs(120)));
        assert_eq!(
            profile.warehouse_uri(),
            "s3://laminardb-certification/team/iceberg-certification/wh"
        );
        let auth = profile.catalog_auth_toml();
        assert!(auth.contains("$${ICEBERG_CLUSTER_CATALOG_BEARER_TOKEN}"));
        assert!(!auth.contains(TEST_SECRET));
        assert!(!format!("{profile:?}").contains(TEST_SECRET));
        assert_eq!(
            profile.checkpoint_storage_toml(),
            "region = \"eu-west-1\"\n"
        );
        assert_eq!(
            profile.iceberg_storage_toml(),
            "\"storage.region\" = \"eu-west-1\"\n"
        );
    }

    #[test]
    fn aws_profile_requires_a_visibility_slo() {
        let mut values = aws_values();
        values.remove(VISIBILITY_SLO_ENV);

        assert_eq!(
            profile_from(&values).expect_err("missing SLO must fail"),
            "ICEBERG_CLUSTER_VISIBILITY_SLO_MS is required"
        );
    }

    #[test]
    fn aws_profile_rejects_unsafe_external_locations() {
        let mut values = aws_values();
        values.insert(CATALOG_URI_ENV, "http://catalog.example.test");
        assert!(profile_from(&values)
            .expect_err("plain HTTP catalog must fail")
            .contains("HTTPS URL"));

        values = aws_values();
        values.insert(S3_BUCKET_ENV, "Invalid_Bucket");
        assert!(profile_from(&values)
            .expect_err("invalid bucket must fail")
            .contains("valid S3 bucket"));

        values = aws_values();
        values.insert(S3_PREFIX_ENV, "../outside");
        assert!(profile_from(&values)
            .expect_err("parent path must fail")
            .contains("object-store path"));

        values = aws_values();
        values.insert("AWS_ENDPOINT_URL", "https://storage.example.test");
        assert!(profile_from(&values)
            .expect_err("endpoint override must fail")
            .contains("bypass real S3"));
    }

    #[test]
    fn profile_rejects_unbounded_or_injectable_values() {
        let mut values = aws_values();
        values.insert(BROKERS_ENV, "broker\"\n[server]");
        assert!(profile_from(&values)
            .expect_err("TOML injection must fail")
            .contains("generated TOML"));

        values = aws_values();
        values.insert(RECOVERY_TIMEOUT_ENV, "600001");
        assert!(profile_from(&values)
            .expect_err("unbounded timeout must fail")
            .contains("between 1 and 600000 milliseconds"));
    }
}
