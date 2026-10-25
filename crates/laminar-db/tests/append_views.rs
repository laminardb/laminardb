#![allow(clippy::disallowed_types)]
//! Append-only materialized views keep a committed log under a declared retention: each
//! checkpoint writes the rows appended since the previous one, reads load committed segments
//! back from the checkpoint store, and retention decides what reads return and checkpoints keep.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{ExecuteResult, LaminarConfig, LaminarDB};

const TRADES: &str = "CREATE SOURCE trades (id BIGINT, ts TIMESTAMP, \
                      WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)";
const EVENTS: &str = "CREATE SOURCE events (k BIGINT, v BIGINT)";

fn config(dir: &std::path::Path) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(dir.to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None,
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    }
}

/// `ts TIMESTAMP` is microseconds; `times_ms` are milliseconds.
fn trades(ids: &[i64], times_ms: &[i64]) -> RecordBatch {
    let micros: Vec<i64> = times_ms.iter().map(|ms| ms * 1000).collect();
    RecordBatch::try_from_iter_with_nullable(vec![
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as _, true),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(micros)) as _,
            true,
        ),
    ])
    .unwrap()
}

fn events(ks: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable(vec![
        ("k", Arc::new(Int64Array::from(ks.to_vec())) as _, true),
        ("v", Arc::new(Int64Array::from(ks.to_vec())) as _, true),
    ])
    .unwrap()
}

/// The first column of `sql`'s rows, sorted.
async fn first_column(db: &LaminarDB, sql: &str) -> Vec<i64> {
    let ExecuteResult::Query(mut query) = db.execute(sql).await.unwrap() else {
        panic!("expected a query result");
    };
    let mut rows = query.subscribe_raw().unwrap();
    let first = tokio::time::timeout(Duration::from_secs(30), rows.recv_async())
        .await
        .expect("query produced no result within 30 seconds")
        .expect("query disconnected before its result");
    let mut values = Vec::new();
    let mut batch = Some(first);
    for _ in 0..1024 {
        let Some(current) = batch.take() else {
            break;
        };
        let column = current.column(0);
        let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
        values.extend((0..column.len()).map(|row| column.value(row)));
        batch = rows.poll();
    }
    values.sort_unstable();
    values
}

async fn restart(db: &Arc<LaminarDB>) {
    assert!(
        db.checkpoint().await.unwrap().success,
        "checkpoint must succeed"
    );
    db.stop_pipeline().await.unwrap();
    db.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
}

fn view_log_objects(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(path: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path.file_name().is_some_and(|name| name == "view-log.bin") {
                found.push(path);
            }
        }
    }
    let mut found = Vec::new();
    walk(dir, &mut found);
    found.sort();
    found
}

/// Rows behind the event-time horizon leave reads at once and leave the log at the next
/// checkpoint, whose GC deletes the expired segment's object. The log survives a restart.
#[tokio::test]
async fn an_event_time_log_survives_restart_and_expires_rows_behind_its_horizon() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(LaminarDB::open_with_config(config(dir.path())).unwrap());
    db.execute(TRADES).await.unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW recent AS SELECT id, ts FROM trades \
         WITH (retention = '10 seconds')",
    )
    .await
    .unwrap();
    db.start().await.unwrap();
    let source = db.source_untyped("trades").unwrap();
    source.push_arrow(trades(&[1, 2], &[1_000, 2_000])).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let sql = "SELECT id FROM recent";
    assert_eq!(first_column(&db, sql).await, [1, 2]);
    assert!(db.checkpoint().await.unwrap().success);
    assert_eq!(
        first_column(&db, sql).await,
        [1, 2],
        "committed rows read back"
    );
    assert_eq!(view_log_objects(dir.path()).len(), 1);

    // Watermark 19 s sets the horizon at 9 s.
    source.push_arrow(trades(&[3], &[20_000])).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(first_column(&db, sql).await, [3]);

    restart(&db).await;
    assert_eq!(first_column(&db, sql).await, [3]);
    let mut objects = view_log_objects(dir.path());
    for _ in 0..50 {
        if objects.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        objects = view_log_objects(dir.path());
    }
    assert_eq!(
        objects.len(),
        1,
        "the expired segment's object must be deleted: {objects:?}"
    );

    db.source_untyped("trades")
        .unwrap()
        .push_arrow(trades(&[4], &[21_000]))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(first_column(&db, sql).await, [3, 4]);
    db.shutdown().await.unwrap();
}

/// A row cap keeps the latest rows across committed segments and memory.
#[tokio::test]
async fn a_row_capped_log_keeps_the_latest_rows_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(LaminarDB::open_with_config(config(dir.path())).unwrap());
    db.execute(EVENTS).await.unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW tail AS SELECT k, v FROM events WITH (retention = '3 rows')",
    )
    .await
    .unwrap();
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();
    source.push_arrow(events(&[1, 2])).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(db.checkpoint().await.unwrap().success);
    source.push_arrow(events(&[3, 4])).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let sql = "SELECT k FROM tail";
    assert_eq!(first_column(&db, sql).await, [2, 3, 4]);

    restart(&db).await;
    assert_eq!(first_column(&db, sql).await, [2, 3, 4]);
    db.source_untyped("events")
        .unwrap()
        .push_arrow(events(&[5, 6, 7]))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(first_column(&db, sql).await, [5, 6, 7]);
    db.shutdown().await.unwrap();
}

/// A windowed view keeps windows by their window column under the default retention.
#[tokio::test]
async fn a_windowed_view_keeps_windows_by_their_window_column() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(LaminarDB::open_with_config(config(dir.path())).unwrap());
    db.execute(TRADES).await.unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW per_second AS \
         SELECT COUNT(*) AS n, tumble(ts, INTERVAL '1' SECOND) AS window_start FROM trades \
         GROUP BY tumble(ts, INTERVAL '1' SECOND) EMIT ON WINDOW CLOSE",
    )
    .await
    .unwrap();
    db.start().await.unwrap();
    let source = db.source_untyped("trades").unwrap();
    source
        .push_arrow(trades(&[1, 2, 3], &[1_000, 1_500, 2_000]))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    source.push_arrow(trades(&[4], &[10_000])).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let sql = "SELECT n FROM per_second";
    assert_eq!(first_column(&db, sql).await, [1, 2]);

    restart(&db).await;
    assert_eq!(first_column(&db, sql).await, [1, 2]);
    // A watermark more than an hour past the earlier windows expires them and closes `late`.
    let late = 3_600_000 + 40_000;
    db.source_untyped("trades")
        .unwrap()
        .push_arrow(trades(&[5, 6], &[late, late + 5_000]))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(first_column(&db, sql).await, [1]);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn append_views_must_declare_a_retention_they_can_keep() {
    let db = LaminarDB::open().unwrap();
    db.execute(TRADES).await.unwrap();
    db.execute(EVENTS).await.unwrap();

    let error = |sql: &'static str| {
        let db = &db;
        async move { db.execute(sql).await.unwrap_err().to_string() }
    };
    assert!(error("CREATE MATERIALIZED VIEW a AS SELECT k FROM events")
        .await
        .contains("no event-time column"));
    assert!(error(
        "CREATE MATERIALIZED VIEW b AS SELECT k FROM events WITH (retention = '1 hour')"
    )
    .await
    .contains("no event-time column"));
    assert!(error(
        "CREATE MATERIALIZED VIEW c AS SELECT id, ts FROM trades WITH (retention = 'unbounded')"
    )
    .await
    .contains("unbounded log"));
    assert!(error(
        "CREATE MATERIALIZED VIEW d AS SELECT k, COUNT(*) AS n FROM events GROUP BY k \
         WITH (retention = '10 rows')"
    )
    .await
    .contains("maintains a changelog"));

    db.execute("CREATE MATERIALIZED VIEW e AS SELECT id, ts FROM trades")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW f AS SELECT k FROM events WITH (retention = '5 rows')")
        .await
        .unwrap();
}
