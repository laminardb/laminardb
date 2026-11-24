//! Three-node at-least-once soak for a Delta Lake change feed, which one node at a time reads. A
//! writer commits inserts, updates and deletes to a change-feed table on S3, and a keyed
//! materialized view folds the feed. Before any node starts, the writer seeds the table with one
//! large snapshot, so the first owner's bootstrap is one long replay unit that every node's
//! checkpoint waits for. The observed leader and each follower are killed inside a checkpoint and
//! rejoin (`kill_rejoin`). One more round freezes the reading node with `SIGSTOP` until a survivor
//! reads the table; resumed, the frozen node must exit fenced.
//!
//! Operator state and the source cursor commit in one cut, so once the writer stops, every node's
//! committed read of the view must equal the same aggregate over the table's final snapshot, even
//! at-least-once. More than one node must have read the table, and every node must keep
//! committing checkpoints.
//!
//! The exactly-once leg runs the same rounds with `delivery = exactly_once` and also writes the
//! aggregate's `EMIT CHANGES` stream to a Delta append table on S3, which keeps each change's
//! `__weight`. A change committed twice leaves a row whose weights do not net to zero or one, so
//! after every round the table's weights must net to one row per bucket, and once the writer
//! stops those rows must equal the aggregate over the final snapshot.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,delta-lake-s3 --test cluster_soak \
//!   delta_cdf_source::three_node_delta_cdf_kill9_soak -- --ignored --exact --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,delta-lake-s3 --test cluster_soak \
//!   delta_cdf_source::three_node_delta_cdf_delta_eo_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL` (the input table is created under it),
//! `LAMINAR_SOAK_S3_*`, `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`,
//! `LAMINAR_SOAK_MAX_RECOVERY_MS` and `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS`
//! (default 4), `LAMINAR_SOAK_SECONDS` (steady phase after the rounds, default 30),
//! `LAMINAR_SOAK_RPS` (rows per second the writer inserts, default 400) and
//! `LAMINAR_SOAK_DELTA_SEED_ROWS` (rows of the seeded snapshot, default 200000). The exactly-once
//! leg also reads `LAMINAR_SOAK_DELTA_BUCKET`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;
use deltalake::{DeltaTable, TableProperty};

use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, record_consumers, rejoin, steady_phase, SoakSettings,
    CHECKPOINTS_COMPLETED, EVENTS_INGESTED,
};
use super::view_reads::{connect, ReadFailure};
use super::{
    delta_append_sink_config, delta_soak_table_uri, env_u64, scoped_soak_storage_url, soak_run_id,
    wait_for, wait_for_converged_durable_checkpoint, wait_until, DeltaSoakStorage,
    DurableCheckpointStatus, JoinDelivery, Node, ResolvedExecutable, NODES,
};

const VIEW: &str = "cdf_buckets";
const CHANGES: &str = "cdf_bucket_changes";
const BUCKETS: i64 = 16;
/// Each key of a bucket: `(bucket, n, total, peak)`.
type Buckets = Vec<[i64; 4]>;
const COMMIT_INTERVAL: Duration = Duration::from_millis(500);
const COMMIT_ATTEMPTS: u32 = 20;
/// Updates and deletes touch the ids of this many most recent inserts.
const RECENT_INSERTS: i64 = 8;
const READ_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
#[ignore = "spawns 3 real laminardb processes reading a Delta change feed on S3"]
fn three_node_delta_cdf_kill9_soak() {
    run(JoinDelivery::AtLeastOnce);
}

#[test]
#[ignore = "spawns 3 real laminardb processes reading a Delta change feed on S3 into a Delta sink"]
fn three_node_delta_cdf_delta_eo_kill9_soak() {
    run(JoinDelivery::ExactlyOnce);
}

fn run(delivery: JoinDelivery) {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(delivery);
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let table = Table {
        uri: scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "delta-cdf-input"),
        storage: DeltaSoakStorage::from_environment(),
    };
    let seed_rows = i64::try_from(env_u64("LAMINAR_SOAK_DELTA_SEED_ROWS", 200_000))
        .expect("LAMINAR_SOAK_DELTA_SEED_ROWS fits i64");
    runtime().block_on(table.seed(seed_rows));
    let output = (delivery == JoinDelivery::ExactlyOnce).then(|| Table {
        uri: delta_soak_table_uri(&run_id, "delta-cdf"),
        storage: table.storage.clone(),
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: Delta change feed node logs in {}", log_dir.display());
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        &view_sql(),
        &workload(&table, output.as_ref()),
    );
    let mut writer = Writer::spawn(table.clone(), seed_rows, settings.rps);
    let mut live = || writer.assert_running();
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
        if let Some(output) = &output {
            let buckets = runtime()
                .block_on(netted_buckets(output))
                .unwrap_or_else(|error| {
                    panic!("read the Delta output after round {round}: {error}")
                });
            eprintln!(
                "soak round {round}: the Delta output nets to {} buckets",
                buckets.len()
            );
        }
    }
    latest = paused_reader_round(&mut nodes, &mut live, &settings, latest);
    record_consumers(&nodes, &mut consumers);
    steady_phase(
        &mut nodes,
        &mut live,
        settings.steady,
        CHECKPOINTS_COMPLETED,
    );
    let commits = writer.stop();
    let expected = runtime().block_on(table.buckets());
    let epoch = converged_reads(&mut nodes, &expected, &settings, latest);
    if let Some(output) = &output {
        converge_output(output, &expected, settings.recovery_ceiling);
        eprintln!("soak: the Delta output nets to the final snapshot's buckets");
    }
    assert!(
        consumers.len() > 1,
        "only node {consumers:?} ever read the table: the change feed never moved"
    );
    eprintln!(
        "soak: Delta change feed across {NODES} nodes ({delivery:?}): {seed_rows} seeded rows, \
         {commits} later commits, every node read the final snapshot's {} buckets at epoch \
         {epoch}, consumers={consumers:?} kills={} (followers {killed_followers:?})",
        expected.len(),
        settings.kills
    );
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("soak runtime")
}

/// The aggregate the view, the output stream and the oracle compute over the table.
fn bucket_sql(table: &str) -> String {
    format!(
        "SELECT id % {BUCKETS} AS bucket, COUNT(*) AS n, SUM(amount) AS total, MAX(amount) AS peak \
         FROM {table} GROUP BY id % {BUCKETS}"
    )
}

fn view_sql() -> String {
    format!(
        r#"sql = """
CREATE MATERIALIZED VIEW {VIEW} AS {};
"""
"#,
        bucket_sql("cdf_input")
    )
}

/// The change-feed source, plus for the exactly-once leg the aggregate's changes into `output`.
fn workload(table: &Table, output: Option<&Table>) -> String {
    let source = format!(
        r#"
[[source]]
name = "cdf_input"
connector = "delta-lake"
[source.properties]
"table.path" = "{}"
"poll.interval.ms" = "250"
{}"#,
        table.uri,
        table.storage.connector_properties()
    );
    let Some(output) = output else {
        return source;
    };
    format!(
        r#"{source}
[[pipeline]]
name = "{CHANGES}"
sql = "{} EMIT CHANGES"
{}"#,
        bucket_sql("cdf_input"),
        delta_append_sink_config("cdf_delta", CHANGES, &output.uri, &output.storage)
    )
}

/// A Delta table on S3: the change-feed input, or the exactly-once leg's output.
#[derive(Clone)]
struct Table {
    uri: String,
    storage: DeltaSoakStorage,
}

impl Table {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]))
    }

    fn rows(ids: std::ops::Range<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(ids.clone())),
                Arc::new(Int64Array::from_iter_values(ids.map(|id| id % 100))),
            ],
        )
        .expect("rows match the table schema")
    }

    async fn open(&self) -> Result<DeltaTable, String> {
        let uri = deltalake::ensure_table_uri(&self.uri)
            .map_err(|error| format!("invalid Delta table URI: {error}"))?;
        deltalake::open_table_with_storage_options(uri, self.storage.options())
            .await
            .map_err(|error| format!("open {}: {error}", self.uri))
    }

    /// BIGINT rows that `sql` returns over the table's current snapshot, registered as `t`.
    async fn query<const N: usize>(&self, sql: &str) -> Result<Vec<[i64; N]>, String> {
        let table = self.open().await?;
        let context = deltalake::datafusion::prelude::SessionContext::new();
        table
            .update_datafusion_session(&context.state())
            .map_err(|error| format!("register the Delta object store: {error}"))?;
        let provider = table
            .table_provider()
            .build()
            .await
            .map_err(|error| format!("Delta table provider: {error}"))?;
        context
            .register_table("t", Arc::new(provider))
            .map_err(|error| format!("register {}: {error}", self.uri))?;
        let batches = context
            .sql(sql)
            .await
            .map_err(|error| format!("plan {sql}: {error}"))?
            .collect()
            .await
            .map_err(|error| format!("scan {}: {error}", self.uri))?;
        let mut rows = Vec::new();
        for batch in &batches {
            let columns = batch
                .columns()
                .iter()
                .map(|column| column.as_any().downcast_ref::<Int64Array>())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| format!("{sql} returned a column that is not BIGINT"))?;
            rows.extend(
                (0..batch.num_rows()).map(|row| std::array::from_fn(|i| columns[i].value(row))),
            );
        }
        Ok(rows)
    }

    /// Create the change-feed table holding `rows` ids in one commit.
    async fn seed(&self, rows: i64) {
        let uri = deltalake::ensure_table_uri(&self.uri).expect("Delta table URI");
        let columns: deltalake::kernel::StructType = Self::schema()
            .as_ref()
            .try_into_kernel()
            .expect("kernel schema");
        let table = DeltaTable::try_from_url_with_storage_options(uri, self.storage.options())
            .await
            .expect("Delta table handle")
            .create()
            .with_columns(columns.fields().cloned())
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .expect("create the change-feed table");
        table
            .write(vec![Self::rows(0..rows)])
            .await
            .expect("seed the change-feed table");
    }

    /// The view's rows over the table's current snapshot.
    async fn buckets(&self) -> Buckets {
        self.query(&format!("{} ORDER BY bucket", bucket_sql("t")))
            .await
            .unwrap_or_else(|error| panic!("aggregate the final snapshot: {error}"))
    }
}

/// The buckets the exactly-once leg's committed changes net to. Each change is committed once, so
/// every row's weights net to zero or one and each bucket keeps at most one row. The aggregate
/// writes a bucket's change as a retraction of its old row with the new one, so output committed
/// twice leaves an old row at -1 or a second current row for its bucket.
async fn netted_buckets(output: &Table) -> Result<Buckets, String> {
    let netted: Vec<[i64; 5]> = output
        .query(
            "SELECT bucket, n, total, peak, SUM(__weight) AS net FROM t \
             GROUP BY bucket, n, total, peak HAVING SUM(__weight) <> 0 ORDER BY bucket",
        )
        .await?;
    let doubled: Vec<_> = netted.iter().filter(|row| row[4] != 1).take(8).collect();
    assert!(
        doubled.is_empty(),
        "Delta output rows whose weights net outside zero and one (bucket, n, total, peak, net): \
         {doubled:?}"
    );
    let buckets: Buckets = netted
        .iter()
        .map(|row| [row[0], row[1], row[2], row[3]])
        .collect();
    let repeated: Vec<_> = buckets
        .windows(2)
        .filter(|pair| pair[0][0] == pair[1][0])
        .collect();
    assert!(
        repeated.is_empty(),
        "Delta output buckets with more than one current row: {repeated:?}"
    );
    Ok(buckets)
}

/// Wait until the exactly-once leg's committed changes net to `expected`.
fn converge_output(output: &Table, expected: &Buckets, timeout: Duration) {
    let runtime = runtime();
    let mut last = None;
    let converged = wait_until(timeout, || {
        last = Some(runtime.block_on(netted_buckets(output)));
        matches!(&last, Some(Ok(buckets)) if buckets == expected)
    });
    assert!(
        converged,
        "the Delta output never netted to the final snapshot {expected:?}; last {last:?}"
    );
}

/// Paced writer that cycles an insert of new ids, an update of every seventh recent id, and a
/// delete of some even recent ids, one commit each. Updates and deletes touch only the last few
/// inserts, so file statistics prune the rest of the table. A failed commit is retried on a
/// reopened table, so a commit whose acknowledgement was lost may apply twice; the oracle reads
/// the table itself.
struct Writer {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<u64>>,
}

impl Writer {
    fn spawn(table: Table, first_id: i64, rps: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || runtime().block_on(write(&table, first_id, rps, &stop)))
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
            Ok(_) => panic!("Delta writer stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// Stop after the commit in flight; returns the commits made after the seed.
    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .expect("Delta writer was already stopped")
            .join()
            .expect("Delta writer thread failed")
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

async fn write(table: &Table, first_id: i64, rps: u64, stop: &AtomicBool) -> u64 {
    // One commit in three inserts, at two commits a second.
    let per_insert = i64::try_from(rps).expect("rps fits i64") * 3 / 2;
    let mut next_id = first_id;
    let mut round = 0;
    let mut delta = None;
    while !stop.load(Ordering::Acquire) {
        tokio::time::sleep(COMMIT_INTERVAL).await;
        let mut attempt = 1;
        delta = loop {
            match commit(delta.take(), table, round, next_id, per_insert).await {
                Ok(committed) => break Some(committed),
                Err(error) => {
                    assert!(
                        attempt < COMMIT_ATTEMPTS,
                        "Delta commit {round} never succeeded: {error}"
                    );
                    attempt += 1;
                    tokio::time::sleep(COMMIT_INTERVAL).await;
                }
            }
        };
        if round % 3 == 0 {
            next_id += per_insert;
        }
        round += 1;
    }
    round
}

/// Commit round `round` on `delta`, or on a reopened table after a failed attempt.
async fn commit(
    delta: Option<DeltaTable>,
    table: &Table,
    round: u64,
    next_id: i64,
    per_insert: i64,
) -> Result<DeltaTable, String> {
    let delta = match delta {
        Some(delta) => delta,
        None => table.open().await?,
    };
    let recent = next_id - per_insert * RECENT_INSERTS;
    let shard = round / 3;
    match round % 3 {
        0 => {
            delta
                .write(vec![Table::rows(next_id..next_id + per_insert)])
                .await
        }
        1 => delta
            .update()
            .with_predicate(format!("id >= {recent} AND id % 7 = {}", shard % 7))
            .with_update("amount", "amount + 1")
            .await
            .map(|(delta, _)| delta),
        _ => delta
            .delete()
            .with_predicate(format!(
                "id >= {recent} AND id % 11 = {} AND id % 2 = 0",
                shard % 11
            ))
            .await
            .map(|(delta, _)| delta),
    }
    .map_err(|error| format!("Delta commit {round}: {error}"))
}

fn ingested(nodes: &[Node]) -> Vec<f64> {
    nodes
        .iter()
        .map(|node| node.metric(EVENTS_INGESTED).unwrap_or(0.0))
        .collect()
}

/// The node reading the table: the one whose ingested-event count grows over a window in which
/// the writer commits.
fn table_reader(nodes: &mut [Node], live: &mut dyn FnMut(), settings: &SoakSettings) -> usize {
    let mut reader = None;
    wait_for(
        "one node to read the change feed",
        settings.recovery_ceiling,
        || {
            live();
            let before = ingested(nodes);
            std::thread::sleep(COMMIT_INTERVAL * 4);
            let grew: Vec<usize> = ingested(nodes)
                .iter()
                .zip(&before)
                .enumerate()
                .filter(|(_, (after, before))| after > before)
                .map(|(id, _)| id)
                .collect();
            reader = (grew.len() == 1).then(|| grew[0]);
            reader.is_some()
        },
    );
    reader.expect("the reader wait returned a node")
}

/// Freeze the node reading the table past its process lease, require a survivor to read it from
/// the committed cut, then resume the frozen node: it must exit fenced. Returns the checkpoint
/// the cluster commits after the node rejoined.
fn paused_reader_round(
    nodes: &mut [Node],
    live: &mut dyn FnMut(),
    settings: &SoakSettings,
    latest: DurableCheckpointStatus,
) -> DurableCheckpointStatus {
    let reader = table_reader(nodes, live, settings);
    let before = ingested(nodes);
    let paused = nodes[reader].pause();
    eprintln!("soak: SIGSTOP delivered to node {reader}, which reads the change feed");
    let survived = wait_for_converged_durable_checkpoint(
        nodes,
        Instant::now() + settings.recovery_ceiling,
        "survivor progress with the change-feed reader paused",
        Some(latest),
    );
    wait_for(
        "a survivor to read the change feed",
        settings.recovery_ceiling,
        || {
            live();
            nodes.iter().any(|node| {
                node.id != reader
                    && node
                        .metric(EVENTS_INGESTED)
                        .is_some_and(|rows| rows > before[node.id])
            })
        },
    );
    eprintln!("soak: a survivor reads the change feed in place of node {reader}");
    paused.resume_until_fenced_exit(settings.recovery_ceiling);
    rejoin(
        nodes,
        live,
        reader,
        settings,
        CHECKPOINTS_COMPLETED,
        survived,
    )
}

/// Wait until the first node's committed read of the view equals `expected`, then require every
/// node to read the same rows at that epoch or later. Returns the epoch.
fn converged_reads(
    nodes: &mut [Node],
    expected: &Buckets,
    settings: &SoakSettings,
    latest: DurableCheckpointStatus,
) -> u64 {
    wait_for_converged_durable_checkpoint(
        nodes,
        Instant::now() + settings.recovery_ceiling,
        "durable progress after the writer stopped",
        Some(latest),
    );
    let runtime = runtime();
    let mut last = None;
    let converged = wait_until(settings.recovery_ceiling + Duration::from_secs(60), || {
        match runtime.block_on(committed_read(nodes[0].id, 0)) {
            Ok(read) => last = Some(read),
            Err(ReadFailure::Retryable(_)) => {}
            Err(ReadFailure::Fatal(reason)) => panic!("node0 committed view read: {reason}"),
        }
        last.as_ref().is_some_and(|(_, rows)| rows == expected)
    });
    let epoch = match (converged, last) {
        (true, Some((epoch, _))) => epoch,
        (_, last) => panic!(
            "no committed view read equals the final snapshot {expected:?}; last read {last:?}"
        ),
    };
    for node in nodes.iter() {
        match runtime.block_on(committed_read(node.id, epoch)) {
            Ok((read_epoch, rows)) => {
                assert!(
                    read_epoch >= epoch,
                    "node{} read epoch {read_epoch}",
                    node.id
                );
                assert_eq!(&rows, expected, "node{} view at {read_epoch}", node.id);
            }
            Err(ReadFailure::Retryable(reason) | ReadFailure::Fatal(reason)) => {
                panic!("node{} committed view read failed: {reason}", node.id)
            }
        }
    }
    epoch
}

/// One committed read of the view on node `node` at epoch `floor` or later.
async fn committed_read(node: usize, floor: u64) -> Result<(u64, Buckets), ReadFailure> {
    let read = async {
        let client = connect(node, "committed").await?;
        client
            .batch_execute(&format!("SET laminar.min_epoch = {floor}"))
            .await?;
        let rows = client
            .simple_query(&format!(
                "SELECT bucket, n, total, peak FROM {VIEW} ORDER BY bucket"
            ))
            .await?;
        let epoch = client.simple_query("SHOW laminar.read_epoch").await?;
        Ok::<_, ReadFailure>((rows, epoch))
    };
    let (rows, epoch) = tokio::time::timeout(READ_TIMEOUT, read)
        .await
        .map_err(|_| ReadFailure::Retryable("read timeout".to_owned()))??;
    let fatal = |reason: &str| ReadFailure::Fatal(reason.to_owned());
    let epoch = rows_of(&epoch)
        .next()
        .and_then(|row| row.get(0)?.parse::<u64>().ok())
        .ok_or_else(|| fatal("SHOW laminar.read_epoch returned no epoch"))?;
    let buckets = rows_of(&rows)
        .map(|row| {
            let mut values = [0; 4];
            for (index, value) in values.iter_mut().enumerate() {
                *value = row
                    .get(index)
                    .and_then(|text| text.parse().ok())
                    .ok_or_else(|| fatal("a view column is not a BIGINT"))?;
            }
            Ok(values)
        })
        .collect::<Result<_, ReadFailure>>()?;
    Ok((epoch, buckets))
}

fn rows_of(
    messages: &[tokio_postgres::SimpleQueryMessage],
) -> impl Iterator<Item = &tokio_postgres::SimpleQueryRow> {
    messages.iter().filter_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    })
}
