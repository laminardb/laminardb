//! Three-node at-least-once soak for a MongoDB change-stream source, which one node at a time
//! follows. A writer inserts documents with ids `0..` into a replica-set collection, acknowledged
//! by the majority. The node that owns the source's home vnode reads the change stream; a stream
//! forwards each insert's id to a MongoDB sink, and an independent oracle counts the ids in the
//! output collection. The observed leader and each follower are killed inside a checkpoint and
//! rejoin (`kill_rejoin`): when the reading node dies a survivor resumes the stream after the
//! committed resume token, and when the source moves at a rejoin the acquirer resumes after the
//! handoff cut.
//!
//! A new change stream starts at the latest change, so the writer starts only once the owner has
//! read a warm-up document and two checkpoints have committed its position.
//!
//! Every acknowledged id must reach the output and no other id may; duplicates are reported. More
//! than one node must have read the stream, and every node must keep committing checkpoints.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,mongodb-cdc --test cluster_soak \
//!   mongodb_source::three_node_mongodb_source_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the kills, default 30), `LAMINAR_SOAK_RPS` (default 400), the replica set
//! in `LAMINAR_SOAK_MONGODB_SOURCE_URI`, and the output database of the MongoDB sink soak.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mongodb::bson::{doc, Document};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::options::{CollectionOptions, WriteConcern};
use mongodb::{Client, Collection};

use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, record_consumers, steady_phase, SoakSettings,
    CHECKPOINTS_COMPLETED, EVENTS_INGESTED,
};
use super::multi_writer_sinks::mongo::MongoDb;
use super::multi_writer_sinks::{block_on, wait_for_delivery, Destination as _};
use super::{
    scoped_soak_storage_url, soak_run_id, wait_for, wait_for_converged_durable_checkpoint,
    JoinDelivery, Node, ResolvedExecutable, NODES,
};

/// Passed to the nodes as a reference, because a connection URI can carry credentials.
pub(super) const SOURCE_URI: &str = "LAMINAR_SOAK_MONGODB_SOURCE_URI";
pub(super) const SOURCE_DATABASE: &str = "laminar_soak_source";
/// Warm-up documents take ids from here, above any id the writer reaches.
const WARM_UP: u64 = 1 << 40;
pub(super) const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const WRITE_ATTEMPTS: u32 = 40;
const DUPLICATE_KEY: i32 = 11_000;

#[test]
#[ignore = "spawns 3 real laminardb processes with S3 and MongoDB"]
fn three_node_mongodb_source_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let sink = MongoDb::from_environment();
    let source_uri = std::env::var(SOURCE_URI)
        .unwrap_or_else(|_| panic!("the MongoDB source soak requires {SOURCE_URI}"));
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let (input, output) = (
        format!("source_{}", run_id.replace('-', "_")),
        sink.name(&run_id),
    );
    block_on(create_input(&source_uri, &input));
    sink.create(&output);
    let mut oracle = sink.spawn_oracle(&output);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: MongoDB source node logs in {}", log_dir.display());
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        "",
        &workload(&input, &sink.sink_properties(&output)),
    );

    let mut consumers = BTreeSet::new();
    let mut latest = boot(
        &mut nodes,
        &mut || oracle.assert_running(),
        &settings,
        CHECKPOINTS_COMPLETED,
    );
    latest = warm_up(&mut nodes, &source_uri, &input, &settings, latest);
    let mut writer = Writer::spawn(source_uri.clone(), input.clone(), settings.rps);
    let mut live = || {
        writer.assert_running();
        oracle.assert_running();
    };
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

    let written = writer.stop();
    let observed = wait_for_delivery(oracle, 0..written, settings.recovery_ceiling);
    let unexpected: Vec<u64> = observed
        .keys()
        .copied()
        .filter(|id| (written..WARM_UP).contains(id))
        .collect();
    assert!(
        unexpected.is_empty(),
        "the output holds {} ids that were never written, first {:?}",
        unexpected.len(),
        &unexpected[..unexpected.len().min(8)]
    );
    assert!(
        consumers.len() > 1,
        "only node {consumers:?} ever read the stream: the source never moved"
    );
    let duplicates: u64 = observed
        .range(..written)
        .map(|(_, count)| u64::from(count - 1))
        .sum();
    eprintln!(
        "soak: MongoDB source across {NODES} nodes: written={written} lost=0 \
         duplicates={duplicates} consumers={consumers:?} kills={} (followers \
         {killed_followers:?})",
        settings.kills
    );
    drop(nodes);
    block_on(async {
        let input = connect(&source_uri, &input).await;
        input.drop().await.expect("drop the input collection");
    });
    sink.delete(&output);
}

fn workload(input: &str, sink: &str) -> String {
    format!(
        r#"
[[source]]
name = "history_input"
connector = "mongodb-cdc"
[source.properties]
"connection.uri" = "$${{{SOURCE_URI}}}"
database = "{SOURCE_DATABASE}"
collection = "{input}"

# A cluster sink may not read a source directly. The document key is {{"_id": <id>}} in
# canonical Extended JSON, so its digits are the id.
[[pipeline]]
name = "history_forwarded"
sql = "SELECT CAST(regexp_replace(document_key, '[^0-9]', '', 'g') AS BIGINT) AS id FROM history_input WHERE operation = 'insert'"

[[sink]]
name = "history_output"
pipeline = "history_forwarded"
connector = "mongodb-sink"
[sink.properties]
{sink}"#
    )
}

/// Create the input collection before any node starts: the source binds its UUID.
async fn create_input(uri: &str, name: &str) {
    connect_database(uri)
        .await
        .create_collection(name)
        .await
        .expect("create the input collection");
}

/// The input database, through a client owned by the calling runtime: the driver monitors the
/// server from tasks on that runtime.
pub(super) async fn connect_database(uri: &str) -> mongodb::Database {
    Client::with_uri_str(uri)
        .await
        .expect("connect to the MongoDB replica set")
        .database(SOURCE_DATABASE)
}

/// The input collection, written with the majority's acknowledgement.
pub(super) async fn connect(uri: &str, name: &str) -> Collection<Document> {
    connect_database(uri).await.collection_with_options(
        name,
        CollectionOptions::builder()
            .write_concern(WriteConcern::majority())
            .build(),
    )
}

/// Insert warm-up documents until the owner has read one, then wait for two checkpoints so the
/// committed cut holds the stream's position. Returns the last of them.
fn warm_up(
    nodes: &mut [Node],
    uri: &str,
    input: &str,
    settings: &SoakSettings,
    latest: super::DurableCheckpointStatus,
) -> super::DurableCheckpointStatus {
    let mut next = WARM_UP;
    wait_for(
        "the source owner to read a warm-up document",
        settings.recovery_ceiling,
        || {
            block_on(async { insert(&connect(uri, input).await, next).await })
                .expect("insert a warm-up document");
            next += 1;
            nodes
                .iter()
                .any(|node| node.metric(EVENTS_INGESTED).is_some_and(|rows| rows > 0.0))
        },
    );
    let deadline = Instant::now() + settings.recovery_ceiling;
    let first = wait_for_converged_durable_checkpoint(nodes, deadline, "warm-up", Some(latest));
    wait_for_converged_durable_checkpoint(nodes, deadline, "warm-up", Some(first))
}

/// Insert `{_id: id}`. A duplicate key means an earlier attempt already wrote it.
async fn insert(collection: &Collection<Document>, id: u64) -> Result<(), String> {
    let id = i64::try_from(id).expect("ids fit i64");
    let write = tokio::time::timeout(WRITE_TIMEOUT, collection.insert_one(doc! { "_id": id }));
    match write.await.map_err(|_| "insert timed out".to_owned())? {
        Ok(_) => Ok(()),
        Err(error) => match error.kind.as_ref() {
            ErrorKind::Write(WriteFailure::WriteError(failure))
                if failure.code == DUPLICATE_KEY =>
            {
                Ok(())
            }
            _ => Err(error.to_string()),
        },
    }
}

/// Paced writer of ids `0..`. An id is retried until the majority acknowledges it, so the written
/// ids are the prefix `0..written` and no later id reaches the collection.
struct Writer {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<u64>>,
}

impl Writer {
    fn spawn(uri: String, input: String, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                block_on(async { write(&connect(&uri, &input).await, rps, &stop).await })
            })
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
        match self.handle.take().expect("writer handle").join() {
            Ok(_) => panic!("MongoDB writer stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .expect("MongoDB writer was already stopped")
            .join()
            .expect("MongoDB writer thread failed")
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

async fn write(collection: &Collection<Document>, rps: u64, stop: &AtomicBool) -> u64 {
    let start = tokio::time::Instant::now();
    let mut written = 0_u64;
    let mut failures = 0_u32;
    // A failed id is retried even after the stop, so the written ids stay a prefix.
    while failures > 0 || !stop.load(Ordering::Acquire) {
        tokio::time::sleep_until(start + Duration::from_secs_f64(written as f64 / rps as f64))
            .await;
        match insert(collection, written).await {
            Ok(()) => {
                written += 1;
                failures = 0;
            }
            Err(error) => {
                failures += 1;
                assert!(
                    failures < WRITE_ATTEMPTS,
                    "MongoDB never acknowledged id {written}: {error}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    written
}
