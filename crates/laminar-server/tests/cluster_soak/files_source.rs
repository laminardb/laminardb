//! Three-node at-least-once soak for a files source split over an S3 directory. A writer puts
//! JSON-lines objects of ids `0..` into a run-scoped prefix. The source hashes each object's key
//! into one of `splits = <vnodes>` channels, each read by the owner of the vnode the engine's
//! channel route gives it, so every node reads part of the directory. A stream forwards each id to
//! a MongoDB sink, and an independent oracle counts the ids in the output collection. The observed
//! leader and each follower are killed inside a checkpoint and rejoin (`kill_rejoin`): survivors
//! resume a dead node's channels from the committed cut, and channels moved at a rejoin resume
//! from the handoff cut.
//!
//! Every written id must reach the output and no other id may; duplicates are reported. More than
//! one node must have read the directory, and every node must keep committing checkpoints.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,files,mongodb-cdc --test cluster_soak \
//!   files_source::three_node_files_source_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! The input directory is a run-scoped prefix of the shared `LAMINAR_SOAK_CHECKPOINT_URL`, written
//! and read with the `LAMINAR_SOAK_S3_*` settings. It also reads `LAMINAR_SOAK_KEY_GROUPS`,
//! `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS`, `LAMINAR_SOAK_DISCOVERY`,
//! `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS` (steady phase after the kills, default
//! 30), `LAMINAR_SOAK_RPS` (default 400) and the output database of the MongoDB sink soak.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt as _, PutPayload};

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

const OBJECTS_PER_SECOND: u64 = 4;
/// Objects the writer puts before any node starts.
const SEED_OBJECTS: u64 = 8;
const PUT_TIMEOUT: Duration = Duration::from_secs(30);
const PUT_ATTEMPTS: u32 = 20;

#[test]
#[ignore = "spawns 3 real laminardb processes with S3 and MongoDB"]
fn three_node_files_source_kill9_soak() {
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
    let input = Input::from_environment(&run_id);
    let output = sink.name(&run_id);
    sink.create(&output);
    let mut oracle = sink.spawn_oracle(&output);

    let mut writer = Writer::spawn(input.clone(), settings.rps);
    wait_for(
        "the writer to seed the input directory",
        settings.recovery_ceiling,
        || {
            writer.assert_running();
            writer.objects() >= SEED_OBJECTS
        },
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: files source node logs in {}", log_dir.display());
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        "",
        &workload(&input, settings.key_groups, &sink.sink_properties(&output)),
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

    let (written, objects) = writer.stop();
    let observed = wait_for_delivery(oracle, written, settings.recovery_ceiling);
    let unexpected: Vec<u64> = observed.range(written..).map(|(id, _)| *id).collect();
    assert!(
        unexpected.is_empty(),
        "the output holds {} ids that were never written, first {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(8)]
    );
    assert!(
        consumers.len() > 1,
        "only node {consumers:?} ever read the directory: the splits never spread"
    );
    let duplicates: u64 = observed.values().map(|count| u64::from(count - 1)).sum();
    eprintln!(
        "soak: files source across {NODES} nodes: {} splits, written={written} in {objects} \
         objects, lost=0 duplicates={duplicates} consumers={consumers:?} kills={} (followers \
         {killed_followers:?})",
        settings.key_groups, settings.kills
    );
    drop(nodes);
    sink.delete(&output);
}

/// The input directory: a run-scoped prefix beside the checkpoints, on the same S3 endpoint.
#[derive(Clone)]
struct Input {
    url: String,
    endpoint: String,
    access_key: String,
    region: String,
}

impl Input {
    fn from_environment(run_id: &str) -> Self {
        let required = |name| {
            std::env::var(name).unwrap_or_else(|_| panic!("files source soak requires {name}"))
        };
        Self {
            url: scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", run_id, "files-input"),
            endpoint: required("LAMINAR_SOAK_S3_ENDPOINT"),
            access_key: required("LAMINAR_SOAK_S3_ACCESS_KEY"),
            region: required("LAMINAR_SOAK_S3_REGION"),
        }
    }

    /// `storage.*` options, with the secret as written into the node configuration.
    fn storage_options(&self, secret: &str) -> Vec<(&'static str, String)> {
        vec![
            ("aws_endpoint", self.endpoint.clone()),
            ("aws_access_key_id", self.access_key.clone()),
            ("aws_secret_access_key", secret.to_owned()),
            ("aws_region", self.region.clone()),
            ("aws_allow_http", "true".to_owned()),
        ]
    }
}

fn workload(input: &Input, splits: u32, sink: &str) -> String {
    let storage: String = input
        .storage_options("$${LAMINAR_SOAK_S3_SECRET_KEY}")
        .into_iter()
        .map(|(key, value)| format!("\"storage.{key}\" = \"{value}\"\n"))
        .collect();
    format!(
        r#"
[[source]]
name = "files_input"
connector = "files"
format = "json"
[source.properties]
path = "{url}"
splits = "{splits}"
poll_interval = "250ms"
{storage}
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false

# A cluster sink may not read a source directly.
[[pipeline]]
name = "files_forwarded"
sql = "SELECT id FROM files_input"

[[sink]]
name = "files_output"
pipeline = "files_forwarded"
connector = "mongodb-sink"
[sink.properties]
{sink}"#,
        url = input.url,
    )
}

/// Paced writer of one object of ids at a time. An object is retried until its put succeeds, so
/// the written ids are the prefix `0..written` and no later id reaches the directory.
struct Writer {
    stop: Arc<AtomicBool>,
    objects: Arc<AtomicU64>,
    handle: Option<JoinHandle<(u64, u64)>>,
}

impl Writer {
    fn spawn(input: Input, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let objects = Arc::new(AtomicU64::new(0));
        let handle = {
            let (stop, objects) = (Arc::clone(&stop), Arc::clone(&objects));
            std::thread::spawn(move || block_on(write(&input, rps, &stop, &objects)))
        };
        Self {
            stop,
            objects,
            handle: Some(handle),
        }
    }

    fn objects(&self) -> u64 {
        self.objects.load(Ordering::Acquire)
    }

    fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("writer handle").join() {
            Ok(_) => panic!("files writer stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// Stop after the object in flight; returns the ids and objects written.
    fn stop(mut self) -> (u64, u64) {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .expect("files writer was already stopped")
            .join()
            .expect("files writer thread failed")
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

async fn write(input: &Input, rps: u64, stop: &AtomicBool, objects: &AtomicU64) -> (u64, u64) {
    let store = super::topology_baseline::objects_for_namespace(&input.url);
    let per_object = (rps / OBJECTS_PER_SECOND).max(1);
    let interval = Duration::from_secs(1) / u32::try_from(OBJECTS_PER_SECOND).unwrap();
    let start = tokio::time::Instant::now();
    let (mut written, mut put, mut failures) = (0_u64, 0_u64, 0_u32);
    // A failed put is retried even after the stop, so the written ids stay a prefix.
    while failures > 0 || !stop.load(Ordering::Acquire) {
        tokio::time::sleep_until(start + interval * u32::try_from(put).unwrap()).await;
        let body: String = (written..written + per_object)
            .map(|id| format!("{{\"id\":{id}}}\n"))
            .collect();
        let key = ObjectPath::from(format!("{put:010}.json"));
        let result = tokio::time::timeout(
            PUT_TIMEOUT,
            store.put(&key, PutPayload::from(body.into_bytes())),
        )
        .await;
        match result {
            Ok(Ok(_)) => {
                written += per_object;
                put += 1;
                objects.store(put, Ordering::Release);
                failures = 0;
            }
            Ok(Err(error)) => {
                failures += 1;
                assert!(
                    failures < PUT_ATTEMPTS,
                    "S3 never stored object {put}: {error}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(_) => {
                failures += 1;
                assert!(failures < PUT_ATTEMPTS, "putting object {put} timed out");
            }
        }
    }
    (written, put)
}
