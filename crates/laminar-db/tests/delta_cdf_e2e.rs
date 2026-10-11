//! Delta Lake change data feed through the engine: DDL, snapshot bootstrap, retractable SQL,
//! direct changelog sinks, checkpoints, and restarts. Runs on local tables; no services needed.

#![cfg(feature = "delta-lake")]
#![allow(clippy::disallowed_types)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use laminar_connectors::lakehouse::delta_io::open_or_create_table;
use laminar_db::{DeliveryGuarantee, LaminarDB};

const CONVERGE: Duration = Duration::from_secs(60);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, false),
    ]))
}

fn orders(rows: &[(i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(arrow::array::Int64Array::from_iter_values(
                rows.iter().map(|row| row.0),
            )),
            Arc::new(arrow::array::StringArray::from_iter_values(
                rows.iter().map(|row| row.1),
            )),
            Arc::new(arrow::array::Int64Array::from_iter_values(
                rows.iter().map(|row| row.2),
            )),
        ],
    )
    .unwrap()
}

fn location(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// A Delta table with change data feed enabled from its second version on.
async fn source_table(path: &Path, rows: &[(i64, &str, i64)]) {
    std::fs::create_dir_all(path).unwrap();
    let table = open_or_create_table(&location(path), HashMap::new(), Some(&schema()))
        .await
        .unwrap();
    let table = table
        .set_tbl_properties()
        .with_properties(HashMap::from([(
            "delta.enableChangeDataFeed".to_string(),
            "true".to_string(),
        )]))
        .await
        .unwrap();
    table.write(vec![orders(rows)]).await.unwrap();
}

async fn open(storage: &Path) -> Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(300),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await
        .expect("open database")
}

async fn first_error(db: &LaminarDB, statements: &[String]) -> Option<String> {
    for statement in statements {
        if let Err(error) = db.execute(statement).await {
            return Some(format!("{statement}: {error}"));
        }
    }
    None
}

/// Reopen after a shutdown, waiting for the previous generation's namespace lock.
async fn reopen(storage: &Path, statements: &[String]) -> Arc<LaminarDB> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let db = open(storage).await;
        match first_error(&db, statements).await {
            None => return db,
            Some(error) if error.contains("LDB-0014") && tokio::time::Instant::now() < deadline => {
                drop(db);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Some(error) => panic!("{error}"),
        }
    }
}

/// Rows of `query` over the Delta table at `path`, as `key -> [values]`.
async fn query(path: &Path, sql: &str) -> BTreeMap<String, Vec<i64>> {
    if !path.join("_delta_log").exists() {
        return BTreeMap::new();
    }
    let ctx = datafusion::prelude::SessionContext::new();
    if laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "t",
        &location(path),
        HashMap::new(),
    )
    .await
    .is_err()
    {
        return BTreeMap::new();
    }
    let mut out = BTreeMap::new();
    for batch in ctx.sql(sql).await.unwrap().collect().await.unwrap() {
        let key = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
        let key = key.as_string::<i32>();
        for row in 0..batch.num_rows() {
            let values = (1..batch.num_columns())
                .map(|column| {
                    arrow::compute::cast(batch.column(column), &DataType::Int64)
                        .unwrap()
                        .as_primitive::<Int64Type>()
                        .value(row)
                })
                .collect();
            out.insert(key.value(row).to_string(), values);
        }
    }
    out
}

struct Views {
    source: std::path::PathBuf,
    totals: std::path::PathBuf,
    open: std::path::PathBuf,
    mirror: std::path::PathBuf,
}

impl Views {
    async fn converge(&self) {
        for (target, expected, observed) in [
            (
                &self.totals,
                "SELECT label, SUM(qty), COUNT(*) FROM t GROUP BY label",
                "SELECT label, total, n FROM t",
            ),
            (
                &self.open,
                "SELECT id, qty FROM t WHERE label = 'OPEN'",
                "SELECT id, qty FROM t",
            ),
            (
                &self.mirror,
                "SELECT id, qty FROM t",
                "SELECT id, qty FROM t",
            ),
        ] {
            let want = query(&self.source, expected).await;
            let deadline = tokio::time::Instant::now() + CONVERGE;
            loop {
                let got = query(target, observed).await;
                if got == want {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{} did not converge:\n  want {want:?}\n  got  {got:?}",
                    target.display()
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

fn upsert_sink(name: &str, input: &str, path: &Path, key: &str) -> String {
    format!(
        "CREATE SINK {name} FROM {input} INTO \"delta-lake\" ('table.path' = '{}', \
         'write.mode' = 'upsert', 'merge.key.columns' = '{key}', 'auto.create' = 'true')",
        location(path)
    )
}

fn source_ddl(path: &Path) -> String {
    format!(
        "CREATE SOURCE orders FROM \"delta-lake\" ('table.path' = '{}', 'poll.interval.ms' = '50')",
        location(path)
    )
}

/// Retractable SQL and a direct mirror over a Delta change feed stay exact through updates,
/// key changes, deletes, and a restart that resumes from the checkpointed commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_change_feed_maintains_aggregates_filters_and_mirrors_across_restart() {
    let lake = tempfile::tempdir().unwrap();
    let views = Views {
        source: lake.path().join("orders"),
        totals: lake.path().join("totals"),
        open: lake.path().join("open"),
        mirror: lake.path().join("mirror"),
    };
    let seed: Vec<(i64, &str, i64)> = (1..=200)
        .map(|id| (id, if id % 3 == 0 { "OPEN" } else { "CLOSED" }, id))
        .collect();
    source_table(&views.source, &seed).await;

    let statements = vec![
        source_ddl(&views.source),
        "CREATE STREAM totals AS SELECT label, SUM(qty) AS total, COUNT(*) AS n \
         FROM orders GROUP BY label EMIT CHANGES"
            .to_string(),
        "CREATE STREAM open_orders AS SELECT id, label, qty FROM orders WHERE label = 'OPEN'"
            .to_string(),
        upsert_sink("totals_sink", "totals", &views.totals, "label"),
        upsert_sink("open_sink", "open_orders", &views.open, "id"),
        upsert_sink("mirror_sink", "orders", &views.mirror, "id"),
    ];
    let storage = tempfile::tempdir().unwrap();
    {
        let db = open(storage.path()).await;
        if let Some(error) = first_error(&db, &statements).await {
            panic!("{error}");
        }
        db.start().await.expect("start");
        views.converge().await;

        let table = open_or_create_table(&location(&views.source), HashMap::new(), None)
            .await
            .unwrap();
        let table = table
            .update()
            .with_predicate("id = 3")
            .with_update("qty", "300")
            .await
            .unwrap()
            .0;
        let table = table
            .update()
            .with_predicate("id = 6")
            .with_update("label", "'CLOSED'")
            .await
            .unwrap()
            .0;
        let table = table
            .update()
            .with_predicate("id = 9")
            .with_update("id", "1009")
            .await
            .unwrap()
            .0;
        table
            .delete()
            .with_predicate("id BETWEEN 10 AND 30")
            .await
            .unwrap();
        views.converge().await;
        assert!(db.checkpoint().await.unwrap().success);
        db.shutdown().await.expect("shutdown");
    }

    let table = open_or_create_table(&location(&views.source), HashMap::new(), None)
        .await
        .unwrap();
    let table = table
        .update()
        .with_predicate("id BETWEEN 40 AND 60")
        .with_update("label", "'OPEN'")
        .await
        .unwrap()
        .0;
    let table = table.delete().with_predicate("id = 1").await.unwrap().0;
    table
        .write(vec![orders(&[(5000, "OPEN", 7), (5001, "NEW", 1)])])
        .await
        .unwrap();
    {
        let db = reopen(storage.path(), &statements).await;
        db.start().await.expect("restart");
        views.converge().await;
        db.shutdown().await.expect("shutdown");
    }
}

/// Consumers that cannot apply retractions are rejected before any data moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_change_feed_consumers_that_cannot_retract_are_rejected() {
    let lake = tempfile::tempdir().unwrap();
    let path = lake.path().join("orders");
    source_table(&path, &[(1, "OPEN", 1)]).await;
    let append = lake.path().join("append");
    let cases = [
        (
            "CREATE STREAM top AS SELECT label, MAX(qty) AS m FROM orders GROUP BY label"
                .to_string(),
            "MIN/MAX",
        ),
        (
            format!(
                "CREATE SINK raw FROM orders INTO \"delta-lake\" ('table.path' = '{}', \
                 'write.mode' = 'overwrite', 'auto.create' = 'true')",
                location(&append)
            ),
            "retractions",
        ),
    ];
    for (statement, needle) in cases {
        let storage = tempfile::tempdir().unwrap();
        let db = open(storage.path()).await;
        let error = match first_error(&db, &[source_ddl(&path), statement.clone()]).await {
            Some(error) => error,
            None => db
                .start()
                .await
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default(),
        };
        assert!(
            !error.is_empty() && error.contains(needle),
            "{statement}: {error}"
        );
        let _ = db.shutdown().await;
    }
}

/// End-to-end latency from a source commit to the row being readable in the Delta mirror, and
/// the duration of a manual checkpoint (flush initiation to durable sink visibility). Run with
/// `cargo test -p laminar-db --features delta-lake --release --test delta_cdf_e2e -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement harness; run explicitly"]
async fn delta_change_feed_latency() {
    fn percentile(samples: &mut [Duration], p: f64) -> Duration {
        samples.sort_unstable();
        samples[((samples.len() - 1) as f64 * p).round() as usize]
    }

    let lake = tempfile::tempdir().unwrap();
    let source_path = lake.path().join("orders");
    let mirror = lake.path().join("mirror");
    source_table(&source_path, &[(0, "SEED", 0)]).await;
    let storage = tempfile::tempdir().unwrap();
    let db = LaminarDB::builder()
        .storage_dir(storage.path())
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await
        .unwrap();
    let statements = [
        format!(
            "CREATE SOURCE orders FROM \"delta-lake\" ('table.path' = '{}', \
             'poll.interval.ms' = '10')",
            location(&source_path)
        ),
        format!(
            "CREATE SINK mirror_sink FROM orders INTO \"delta-lake\" ('table.path' = '{}', \
             'write.mode' = 'upsert', 'merge.key.columns' = 'id', 'auto.create' = 'true', \
             'max.buffer.duration.ms' = '0')",
            location(&mirror)
        ),
    ];
    if let Some(error) = first_error(&db, &statements).await {
        panic!("{error}");
    }
    db.start().await.unwrap();

    let mut visible = Vec::new();
    let mut checkpoints = Vec::new();
    for id in 1..=20_i64 {
        let table = open_or_create_table(&location(&source_path), HashMap::new(), None)
            .await
            .unwrap();
        table.write(vec![orders(&[(id, "NEW", id)])]).await.unwrap();
        let committed = tokio::time::Instant::now();
        let probe = format!("SELECT id, qty FROM t WHERE id = {id}");
        while query(&mirror, &probe).await.is_empty() {
            assert!(committed.elapsed() < CONVERGE, "row {id} never arrived");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        visible.push(committed.elapsed());
        let started = tokio::time::Instant::now();
        assert!(db.checkpoint().await.unwrap().success);
        checkpoints.push(started.elapsed());
    }
    println!(
        "commit -> visible in Delta mirror: p50 {:?} p95 {:?} max {:?}",
        percentile(&mut visible, 0.5),
        percentile(&mut visible, 0.95),
        percentile(&mut visible, 1.0)
    );
    println!(
        "manual checkpoint (initiation -> durable): p50 {:?} p95 {:?} max {:?}",
        percentile(&mut checkpoints, 0.5),
        percentile(&mut checkpoints, 0.95),
        percentile(&mut checkpoints, 1.0)
    );
    db.shutdown().await.unwrap();
}
