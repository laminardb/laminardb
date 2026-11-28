//! Three-node at-least-once soak for an Iceberg append read split by data file. A writer appends
//! snapshots of ids `0..` to a REST-catalog table on S3. The source hashes each data file into
//! one of `read.splits = <vnodes>` channels, each read by the owner of the vnode the engine's
//! channel route gives it, so every node reads part of the table. A stream forwards each id to a
//! MongoDB sink, and an independent oracle counts the ids in the output collection. The observed
//! leader and each follower are killed inside a checkpoint and rejoin (`kill_rejoin`): survivors
//! resume a dead node's channels from the committed cut, and channels moved at a rejoin resume
//! from the handoff cut.
//!
//! The writer seeds snapshots before any node starts, so every node first reads its channels'
//! files of the snapshot it starts at (`read.bootstrap = initial`), then the appends after it.
//!
//! Every appended id must reach the output and no other id may; duplicates are reported. More than
//! one node must have read the table, and every node must keep committing checkpoints.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,iceberg,mongodb-cdc --test cluster_soak \
//!   iceberg_source::three_node_iceberg_source_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the kills, default 30), `LAMINAR_SOAK_RPS` (default 400), the Iceberg leg's
//! `LAMINAR_SOAK_ICEBERG_*` settings (`tests/docker/iceberg-compose.yml` by default) and the output
//! database of the MongoDB sink soak.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::connector::SinkConnector as _;
use laminar_connectors::error::ConnectorError;
use laminar_connectors::lakehouse::iceberg::IcebergSink;
use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;

use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, record_consumers, steady_phase, SoakSettings,
    CHECKPOINTS_COMPLETED,
};
use super::multi_writer_sinks::mongo::MongoDb;
use super::multi_writer_sinks::{block_on, wait_for_delivery, Destination as _};
use super::{
    scoped_soak_storage_url, soak_run_id, wait_for, wait_for_converged_durable_checkpoint,
    JoinDelivery, ResolvedExecutable, NODES,
};

const SECRET_ENV: &str = "LAMINAR_SOAK_ICEBERG_S3_SECRET_KEY";
const NAMESPACE: &str = "laminardb_soak";
const REGION: &str = "us-east-1";
const SNAPSHOTS_PER_SECOND: u64 = 2;
/// Snapshots the writer commits before any node starts.
const SEED_SNAPSHOTS: u64 = 3;
const APPEND_TIMEOUT: Duration = Duration::from_secs(30);
const APPEND_ATTEMPTS: u32 = 20;

#[test]
#[ignore = "spawns 3 real laminardb processes with S3, an Iceberg REST catalog and MongoDB"]
fn three_node_iceberg_source_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let sink = MongoDb::from_environment();
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let table = Table::from_environment(&run_id);
    let output = sink.name(&run_id);
    sink.create(&output);
    let mut oracle = sink.spawn_oracle(&output);

    let mut writer = Writer::spawn(table.clone(), settings.rps);
    wait_for(
        "the writer to seed the input table",
        settings.recovery_ceiling,
        || {
            writer.assert_running();
            writer.snapshots() >= SEED_SNAPSHOTS
        },
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: Iceberg source node logs in {}", log_dir.display());
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        "",
        &workload(&table, settings.key_groups, &sink.sink_properties(&output)),
    );

    let mut live = || {
        writer.assert_running();
        oracle.assert_running();
    };
    let mut consumers = BTreeSet::new();
    let mut latest = boot(&mut nodes, &mut live, &settings, CHECKPOINTS_COMPLETED);
    record_consumers(&nodes, &mut consumers);
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
        record_consumers(&nodes, &mut consumers);
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
    record_consumers(&nodes, &mut consumers);

    let (written, snapshots) = writer.stop();
    let observed = wait_for_delivery(oracle, 0..written, settings.recovery_ceiling);
    let unexpected: Vec<u64> = observed.range(written..).map(|(id, _)| *id).collect();
    assert!(
        unexpected.is_empty(),
        "the output holds {} ids that were never appended, first {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(8)]
    );
    assert!(
        consumers.len() > 1,
        "only node {consumers:?} ever read the table: the splits never spread"
    );
    let duplicates: u64 = observed.values().map(|count| u64::from(count - 1)).sum();
    eprintln!(
        "soak: Iceberg source across {NODES} nodes: {} splits, written={written} in {snapshots} \
         snapshots, lost=0 duplicates={duplicates} consumers={consumers:?} kills={} (followers \
         {killed_followers:?})",
        settings.key_groups, settings.kills
    );
    drop(nodes);
    sink.delete(&output);
}

/// The input table on the REST catalog and its S3 warehouse.
#[derive(Clone)]
struct Table {
    catalog_uri: String,
    s3_endpoint: String,
    bucket: String,
    access_key: String,
    name: String,
}

impl Table {
    fn from_environment(run_id: &str) -> Self {
        let setting =
            |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_owned());
        let secret = std::env::var(SECRET_ENV)
            .unwrap_or_else(|_| panic!("{SECRET_ENV} must be set for the Iceberg source soak"));
        assert!(!secret.is_empty(), "{SECRET_ENV} must not be empty");
        Self {
            catalog_uri: setting("LAMINAR_SOAK_ICEBERG_CATALOG_URI", "http://127.0.0.1:8181"),
            s3_endpoint: setting("LAMINAR_SOAK_ICEBERG_S3_ENDPOINT", "http://127.0.0.1:9000"),
            bucket: setting("LAMINAR_SOAK_ICEBERG_S3_BUCKET", "warehouse"),
            access_key: setting("LAMINAR_SOAK_ICEBERG_S3_ACCESS_KEY", "minioadmin"),
            name: format!("source_{}", run_id.replace('-', "_")),
        }
    }

    /// The catalog and storage properties shared by the nodes' source and the writer's sink.
    fn properties(&self, secret: &str) -> Vec<(&'static str, String)> {
        vec![
            ("catalog.type", "rest".into()),
            ("catalog.uri", self.catalog_uri.clone()),
            ("catalog.warehouse", format!("s3://{}/wh", self.bucket)),
            ("storage.type", "s3".into()),
            ("storage.endpoint", self.s3_endpoint.clone()),
            ("storage.region", REGION.into()),
            ("storage.path_style", "true".into()),
            ("storage.property.s3.access-key-id", self.access_key.clone()),
            ("storage.property.s3.secret-access-key", secret.into()),
            ("namespace", NAMESPACE.into()),
            ("table.name", self.name.clone()),
        ]
    }

    fn sink_config(&self) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("iceberg");
        let secret = std::env::var(SECRET_ENV).expect("Iceberg S3 secret");
        for (key, value) in self.properties(&secret) {
            config.set(key, value);
        }
        config.set("auto.create", "true");
        config.set("_arrow_schema", encode_arrow_schema_ipc(&schema()));
        config
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn workload(table: &Table, splits: u32, sink: &str) -> String {
    let properties: String = table
        .properties(&format!("$${{{SECRET_ENV}}}"))
        .into_iter()
        .map(|(key, value)| format!("\"{key}\" = \"{value}\"\n"))
        .collect();
    format!(
        r#"
[[source]]
name = "iceberg_input"
connector = "iceberg"
[source.properties]
{properties}"read.mode" = "append"
"read.splits" = "{splits}"
"poll.interval" = "250ms"

# A cluster sink may not read a source directly.
[[pipeline]]
name = "iceberg_forwarded"
sql = "SELECT id FROM iceberg_input"

[[sink]]
name = "iceberg_output"
pipeline = "iceberg_forwarded"
connector = "mongodb-sink"
[sink.properties]
{sink}"#
    )
}

/// Paced writer of one snapshot of ids at a time. A snapshot is retried until it commits, so the
/// appended ids are the prefix `0..written` (a retry after an unacknowledged commit may append some
/// twice) and no later id reaches the table.
struct Writer {
    stop: Arc<AtomicBool>,
    snapshots: Arc<AtomicU64>,
    handle: Option<JoinHandle<(u64, u64)>>,
}

impl Writer {
    fn spawn(table: Table, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let snapshots = Arc::new(AtomicU64::new(0));
        let handle = {
            let (stop, snapshots) = (Arc::clone(&stop), Arc::clone(&snapshots));
            std::thread::spawn(move || block_on(write(&table, rps, &stop, &snapshots)))
        };
        Self {
            stop,
            snapshots,
            handle: Some(handle),
        }
    }

    fn snapshots(&self) -> u64 {
        self.snapshots.load(Ordering::Acquire)
    }

    fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("writer handle").join() {
            Ok(_) => panic!("Iceberg writer stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// Stop after the snapshot in flight; returns the ids and snapshots appended.
    fn stop(mut self) -> (u64, u64) {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .expect("Iceberg writer was already stopped")
            .join()
            .expect("Iceberg writer thread failed")
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

async fn write(table: &Table, rps: u64, stop: &AtomicBool, snapshots: &AtomicU64) -> (u64, u64) {
    let config = table.sink_config();
    let per_snapshot = (rps / SNAPSHOTS_PER_SECOND).max(1);
    let interval = Duration::from_secs(1) / u32::try_from(SNAPSHOTS_PER_SECOND).unwrap();
    let start = tokio::time::Instant::now();
    let mut sink = None;
    let (mut written, mut committed, mut failures) = (0_u64, 0_u64, 0_u32);
    // A failed snapshot is retried even after the stop, so the appended ids stay a prefix.
    while failures > 0 || !stop.load(Ordering::Acquire) {
        tokio::time::sleep_until(start + interval * u32::try_from(committed).unwrap()).await;
        let ids = written..written + per_snapshot;
        match append(&mut sink, &config, ids).await {
            Ok(()) => {
                written += per_snapshot;
                committed += 1;
                snapshots.store(committed, Ordering::Release);
                failures = 0;
            }
            Err(error) => {
                sink = None;
                failures += 1;
                assert!(
                    failures < APPEND_ATTEMPTS,
                    "Iceberg never committed ids from {written}: {error}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    (written, committed)
}

/// Commit one snapshot holding `ids`, opening a sink if the last attempt dropped it.
async fn append(
    sink: &mut Option<IcebergSink>,
    config: &ConnectorConfig,
    ids: std::ops::Range<u64>,
) -> Result<(), ConnectorError> {
    if sink.is_none() {
        let mut opened = IcebergSink::new(IcebergSinkConfig::from_config(config)?, None);
        tokio::time::timeout(APPEND_TIMEOUT, opened.open(config))
            .await
            .map_err(|_| {
                ConnectorError::Internal("opening the Iceberg writer timed out".into())
            })??;
        *sink = Some(opened);
    }
    let writer = sink.as_mut().expect("the writer was opened above");
    let ids: Vec<i64> = ids
        .map(|id| i64::try_from(id).expect("ids fit i64"))
        .collect();
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))])
        .map_err(|error| ConnectorError::Internal(error.to_string()))?;
    writer.write_batch(&batch).await?;
    tokio::time::timeout(APPEND_TIMEOUT, writer.flush())
        .await
        .map_err(|_| ConnectorError::Internal("an Iceberg append commit timed out".into()))?
}
