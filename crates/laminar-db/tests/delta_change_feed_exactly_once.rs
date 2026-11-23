//! Exactly-once delivery from a Delta Lake change feed into a Delta Lake append sink across
//! process kills. The parent commits inserts, updates and deletes to a change-feed table; a child
//! process (this test binary, re-executed) runs an exactly-once pipeline that forwards the feed
//! through a stream into an append table, and the parent kills it without warning several times.
//! Every change of the feed must be committed to the append table exactly once. An insert spans
//! several of the engine's polls, so a checkpoint can wait inside a commit and a kill can land
//! inside one. This is process-kill evidence only; it does not model power loss.
#![cfg(feature = "delta-lake")]
#![allow(clippy::disallowed_types)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use laminar_connectors::lakehouse::delta_io::open_or_create_table;
use laminar_db::{DeliveryGuarantee, LaminarDB};

const CHILD_DIR: &str = "LAMINAR_DELTA_EXACT_DIR";
const KILLS: u64 = 4;
/// Rows of each insert commit: several of the engine's 1024-row polls.
const INSERT_ROWS: i64 = 3000;
/// Updates and deletes touch the ids of this many most recent inserts.
const RECENT_INSERTS: i64 = 4;
const COMMIT_INTERVAL: Duration = Duration::from_millis(150);

/// How often each `(id, amount, weight)` change occurs.
type Changes = BTreeMap<(i64, i64, i64), u32>;

fn location(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Child role: run the pipeline until the parent kills this process.
#[tokio::test(flavor = "multi_thread")]
async fn exact_child_pipeline() {
    let Some(dir) = std::env::var_os(CHILD_DIR).map(PathBuf::from) else {
        return;
    };
    let db = LaminarDB::builder()
        .storage_dir(dir.join("storage"))
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(200),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
        .build()
        .await
        .expect("open");
    let statements = [
        format!(
            "CREATE SOURCE feed FROM \"delta-lake\" ('table.path' = '{}', \
             'starting.version' = '1', 'poll.interval.ms' = '50')",
            location(&dir.join("input"))
        ),
        "CREATE STREAM changes AS SELECT id, amount FROM feed".to_string(),
        format!(
            "CREATE SINK out FROM changes INTO \"delta-lake\" ('table.path' = '{}', \
             'write.mode' = 'append', 'auto.create' = 'true')",
            location(&dir.join("output"))
        ),
    ];
    let deadline = Instant::now() + Duration::from_secs(60);
    for statement in &statements {
        // The previous generation may still hold the checkpoint lock briefly.
        while let Err(error) = db.execute(statement).await {
            assert!(Instant::now() < deadline, "{statement}: {error}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    while let Err(error) = db.start().await {
        assert!(Instant::now() < deadline, "start: {error}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    loop {
        assert_eq!(db.last_fault(), None, "pipeline fault");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The child pipeline process, killed when dropped so a failed run leaves none behind.
struct ChildPipeline(Child);

impl ChildPipeline {
    fn spawn(dir: &Path) -> Self {
        Self(
            Command::new(std::env::current_exe().expect("test binary"))
                .args([
                    "--exact",
                    "exact_child_pipeline",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD_DIR, dir)
                .spawn()
                .expect("spawn child pipeline"),
        )
    }

    fn assert_running(&mut self) {
        if let Some(status) = self.0.try_wait().expect("child status") {
            panic!("the child pipeline exited: {status}");
        }
    }
}

impl Drop for ChildPipeline {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn rows(ids: std::ops::Range<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(ids.clone())),
            Arc::new(Int64Array::from_iter_values(ids.map(|id| id % 100))),
        ],
    )
    .expect("rows match the schema")
}

/// Create the change-feed table and seed it with one insert at version 1.
async fn create_feed(input: &Path) {
    std::fs::create_dir_all(input).expect("input directory");
    let seed = rows(0..INSERT_ROWS);
    open_or_create_table(&location(input), HashMap::new(), Some(&seed.schema()))
        .await
        .expect("create the input table")
        .set_tbl_properties()
        .with_properties(HashMap::from([(
            "delta.enableChangeDataFeed".to_string(),
            "true".to_string(),
        )]))
        .await
        .expect("enable the change data feed")
        .write(vec![seed])
        .await
        .expect("seed the input table");
}

/// Cycle an insert of new ids, an update of every seventh recent id, and a delete of some even
/// recent ids, one commit each, until `stop`. Returns the commits made after the seed.
async fn write(input: PathBuf, stop: Arc<AtomicBool>) -> u64 {
    let mut table = open_or_create_table(&location(&input), HashMap::new(), None)
        .await
        .expect("open the input table");
    let mut next_id = INSERT_ROWS;
    let mut round = 0;
    while !stop.load(Ordering::Acquire) {
        tokio::time::sleep(COMMIT_INTERVAL).await;
        let recent = next_id - INSERT_ROWS * RECENT_INSERTS;
        let shard = round / 3;
        table = match round % 3 {
            0 => {
                next_id += INSERT_ROWS;
                table
                    .write(vec![rows(next_id - INSERT_ROWS..next_id)])
                    .await
                    .expect("insert")
            }
            1 => {
                table
                    .update()
                    .with_predicate(format!("id >= {recent} AND id % 7 = {}", shard % 7))
                    .with_update("amount", "amount + 1")
                    .await
                    .expect("update")
                    .0
            }
            _ => {
                table
                    .delete()
                    .with_predicate(format!(
                        "id >= {recent} AND id % 11 = {} AND id % 2 = 0",
                        shard % 11
                    ))
                    .await
                    .expect("delete")
                    .0
            }
        };
        round += 1;
    }
    round
}

fn add(changes: &mut Changes, batch: &RecordBatch, weight: impl Fn(usize) -> i64) {
    let column = |name| {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no {name} column"))
            .as_primitive::<Int64Type>()
    };
    let (ids, amounts) = (column("id"), column("amount"));
    for row in 0..batch.num_rows() {
        *changes
            .entry((ids.value(row), amounts.value(row), weight(row)))
            .or_default() += 1;
    }
}

/// The feed from version 1, read independently of the source through delta-rs.
async fn feed(input: &Path) -> Changes {
    let table = open_or_create_table(&location(input), HashMap::new(), None)
        .await
        .expect("open the input table");
    let ctx = datafusion::prelude::SessionContext::new();
    let scan = table
        .scan_cdf()
        .with_starting_version(1)
        .build(&ctx.state(), None)
        .await
        .expect("plan the change data feed");
    let mut changes = Changes::new();
    for batch in datafusion::physical_plan::collect(scan, ctx.task_ctx())
        .await
        .expect("read the change data feed")
    {
        let kinds = batch.column_by_name("_change_type").expect("_change_type");
        let kinds = arrow::compute::cast(kinds, &DataType::Utf8).expect("change type");
        let kinds = kinds.as_string::<i32>();
        add(&mut changes, &batch, |row| match kinds.value(row) {
            "insert" | "update_postimage" => 1,
            "delete" | "update_preimage" => -1,
            kind => panic!("unknown change type {kind}"),
        });
    }
    changes
}

/// The changes the sink committed; empty until it created the table.
async fn committed(output: &Path) -> Changes {
    let mut changes = Changes::new();
    let ctx = datafusion::prelude::SessionContext::new();
    if laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "t",
        &location(output),
        HashMap::new(),
    )
    .await
    .is_err()
    {
        return changes;
    }
    for batch in ctx
        .sql("SELECT id, amount, __weight FROM t")
        .await
        .expect("plan the output scan")
        .collect()
        .await
        .expect("read the output table")
    {
        let weights = batch.column(2).as_primitive::<Int64Type>();
        add(&mut changes, &batch, |row| weights.value(row));
    }
    changes
}

/// Every committed change must be a change of the feed, committed at most as often as it
/// occurs there. The output is read before the feed, so the feed holds everything committed.
fn assert_committed_once(committed: &Changes, feed: &Changes) {
    let excess: Vec<_> = committed
        .iter()
        .filter(|(change, count)| feed.get(change).is_none_or(|expected| expected < *count))
        .take(8)
        .collect();
    assert!(
        excess.is_empty(),
        "committed changes beyond the feed (change, count): {excess:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn process_kills_commit_every_change_of_the_feed_exactly_once() {
    if std::env::var_os(CHILD_DIR).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("dir");
    let (input, output) = (dir.path().join("input"), dir.path().join("output"));
    create_feed(&input).await;
    let mut child = ChildPipeline::spawn(dir.path());
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn(write(input.clone(), Arc::clone(&stop)));

    // Each generation commits new changes before it is killed, so every kill follows a committed
    // epoch while later changes are uncommitted: a broken abort or replay path shows up as a
    // change committed twice.
    let mut seen = 0;
    for kill in 0..KILLS {
        let deadline = Instant::now() + Duration::from_secs(60);
        let now = loop {
            child.assert_running();
            let now = committed(&output).await;
            let count = now.values().map(|count| u64::from(*count)).sum::<u64>();
            if count > seen {
                seen = count;
                break now;
            }
            assert!(
                Instant::now() < deadline,
                "generation {kill} committed nothing new"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_committed_once(&now, &feed(&input).await);
        tokio::time::sleep(Duration::from_millis(100 + 173 * kill % 400)).await;
        drop(child);
        child = ChildPipeline::spawn(dir.path());
    }
    stop.store(true, Ordering::Release);
    let commits = writer.await.expect("writer");

    let expected = feed(&input).await;
    let deadline = Instant::now() + Duration::from_secs(120);
    let got = loop {
        child.assert_running();
        let got = committed(&output).await;
        assert_committed_once(&got, &expected);
        if got == expected {
            break got;
        }
        assert!(
            Instant::now() < deadline,
            "after {KILLS} kills, {} of the feed's {} changes were committed",
            got.len(),
            expected.len()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    drop(child);
    eprintln!(
        "exactly-once process-kill run: {KILLS} kills, {commits} commits after the seed, {} \
         changes committed once",
        got.len()
    );
}
