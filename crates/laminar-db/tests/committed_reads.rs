#![allow(clippy::disallowed_types)]
//! Committed reads serve every materialized view in a statement from one committed checkpoint:
//! rows a later cycle applied stay invisible until a checkpoint commits them, and each result
//! names the epoch it read.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{
    CommittedQuery, CommittedRead, CommittedReadError, DbError, ExecuteResult, LaminarConfig,
    LaminarDB,
};

const EVENTS: &str = "CREATE SOURCE events (k BIGINT, v BIGINT)";
const COUNTS: &str = "CREATE MATERIALIZED VIEW counts AS \
                      SELECT k, COUNT(*) AS n FROM events GROUP BY k";
const TAIL: &str = "CREATE MATERIALIZED VIEW tail AS SELECT k, v FROM events \
                    WITH (retention = '100 rows')";

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

fn events(ks: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable(vec![
        ("k", Arc::new(Int64Array::from(ks.to_vec())) as _, true),
        ("v", Arc::new(Int64Array::from(ks.to_vec())) as _, true),
    ])
    .unwrap()
}

async fn pipeline(dir: &std::path::Path) -> Arc<LaminarDB> {
    let db = LaminarDB::open_with_config(config(dir)).unwrap();
    for sql in [EVENTS, COUNTS, TAIL] {
        db.execute(sql).await.unwrap();
    }
    db.start().await.unwrap();
    db
}

async fn push(db: &LaminarDB, ks: &[i64]) {
    db.source_untyped("events")
        .unwrap()
        .push_arrow(events(ks))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
}

async fn checkpoint(db: &LaminarDB) -> u64 {
    let result = db.checkpoint().await.unwrap();
    assert!(
        result.success,
        "checkpoint must succeed: {:?}",
        result.error
    );
    result.epoch
}

/// Every value of `column` in `batches`, sorted.
fn column(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    let mut values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    values
}

async fn committed(db: &LaminarDB, sql: &str, at: CommittedRead) -> CommittedQuery {
    db.query_committed(sql, at).await.unwrap()
}

async fn latest_rows(db: &LaminarDB, sql: &str) -> Vec<i64> {
    let ExecuteResult::Query(mut query) = db.execute(sql).await.unwrap() else {
        panic!("expected a query result");
    };
    let mut rows = query.subscribe_raw().unwrap();
    let mut batches = vec![
        tokio::time::timeout(Duration::from_secs(30), rows.recv_async())
            .await
            .unwrap()
            .unwrap(),
    ];
    while let Some(batch) = rows.poll() {
        batches.push(batch);
    }
    column(&batches, 0)
}

fn committed_error(result: Result<CommittedQuery, DbError>) -> CommittedReadError {
    match result {
        Err(DbError::CommittedRead(error)) => error,
        other => panic!("expected a committed-read error, got {other:?}"),
    }
}

#[tokio::test]
async fn committed_reads_see_only_checkpointed_rows_of_every_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = pipeline(dir.path()).await;
    push(&db, &[1, 2, 2]).await;
    let first = checkpoint(&db).await;

    let tail = committed(&db, "SELECT k FROM tail", CommittedRead::Latest).await;
    assert_eq!(tail.epoch, first);
    assert_eq!(column(&tail.batches, 0), [1, 2, 2]);

    push(&db, &[2, 3]).await;
    assert_eq!(
        latest_rows(&db, "SELECT k FROM tail").await,
        [1, 2, 2, 2, 3]
    );
    let both = "SELECT c.k, c.n FROM counts c WHERE c.k IN (SELECT k FROM tail)";
    let before = committed(&db, both, CommittedRead::Latest).await;
    assert_eq!(before.epoch, first);
    assert_eq!(column(&before.batches, 0), [1, 2]);
    assert_eq!(column(&before.batches, 1), [1, 2]);

    let second = checkpoint(&db).await;
    assert!(second > first);
    let after = committed(&db, both, CommittedRead::AtLeast(second)).await;
    assert_eq!(after.epoch, second);
    assert_eq!(column(&after.batches, 0), [1, 2, 3]);
    assert_eq!(column(&after.batches, 1), [1, 1, 3]);
    assert!(after.taken_at_ms >= before.taken_at_ms);

    let keyed = committed(
        &db,
        "SELECT n FROM counts WHERE k = 2",
        CommittedRead::Exact(second),
    )
    .await;
    assert_eq!(column(&keyed.batches, 0), [3]);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_epoch_retention_retired_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = pipeline(dir.path()).await;
    push(&db, &[1]).await;
    let first = checkpoint(&db).await;
    push(&db, &[2]).await;
    checkpoint(&db).await;

    let mut error = None;
    for _ in 0..50 {
        match db
            .query_committed("SELECT k FROM tail", CommittedRead::Exact(first))
            .await
        {
            Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(other) => {
                error = Some(committed_error(Err(other)));
                break;
            }
        }
    }
    assert_eq!(
        error,
        Some(CommittedReadError::Retired { requested: first })
    );
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_restarted_pipeline_serves_its_recovered_cut() {
    let dir = tempfile::tempdir().unwrap();
    let db = pipeline(dir.path()).await;
    push(&db, &[5, 6]).await;
    let epoch = checkpoint(&db).await;
    db.stop_pipeline().await.unwrap();
    db.start().await.unwrap();

    let tail = committed(&db, "SELECT k FROM tail", CommittedRead::Latest).await;
    assert_eq!(tail.epoch, epoch);
    assert_eq!(column(&tail.batches, 0), [5, 6]);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn committed_reads_need_a_checkpoint_and_views_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = pipeline(dir.path()).await;
    assert_eq!(
        committed_error(
            db.query_committed("SELECT k FROM tail", CommittedRead::Latest)
                .await
        ),
        CommittedReadError::NotCommitted {
            requested: None,
            latest: None
        }
    );
    push(&db, &[1]).await;
    checkpoint(&db).await;
    assert!(matches!(
        committed_error(
            db.query_committed("SELECT k FROM events", CommittedRead::Latest)
                .await
        ),
        CommittedReadError::LiveTable { .. }
    ));
    db.shutdown().await.unwrap();

    let unchecked = LaminarDB::open().unwrap();
    unchecked.execute(EVENTS).await.unwrap();
    unchecked.execute(COUNTS).await.unwrap();
    assert_eq!(
        committed_error(
            unchecked
                .query_committed("SELECT k FROM counts", CommittedRead::Latest)
                .await
        ),
        CommittedReadError::CheckpointingDisabled
    );
}

#[tokio::test]
async fn freshness_must_be_at_least_the_checkpoint_interval() {
    let view = |freshness: &str| {
        format!(
            "CREATE MATERIALIZED VIEW fresh AS SELECT k, COUNT(*) AS n FROM events GROUP BY k \
             WITH (freshness = '{freshness}')"
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let manual = LaminarDB::open_with_config(config(dir.path())).unwrap();
    manual.execute(EVENTS).await.unwrap();
    let error = manual.execute(&view("1 minute")).await.unwrap_err();
    assert!(
        error.to_string().contains("periodic checkpoint interval"),
        "{error}"
    );

    let dir = tempfile::tempdir().unwrap();
    let periodic = LaminarDB::open_with_config(LaminarConfig {
        storage_dir: Some(dir.path().to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: Some(1_000),
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    })
    .unwrap();
    periodic.execute(EVENTS).await.unwrap();
    let error = periodic
        .execute(&view("500 milliseconds"))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("below the checkpoint interval of 1s"),
        "{error}"
    );
    periodic.execute(&view("2 seconds")).await.unwrap();
}
