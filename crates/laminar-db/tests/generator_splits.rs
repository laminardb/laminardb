//! A split generator resumes from the cursor the engine committed: state and every split's
//! position come from one checkpoint, so totals stay exact across a restart.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_db::{ExecuteResult, LaminarDB};

const ROWS: i64 = 8_000;
const CONVERGE: Duration = Duration::from_secs(30);

fn statements() -> Vec<String> {
    vec![
        format!(
            "CREATE SOURCE gen FROM GENERATOR ('splits' = '4', 'max.rows' = '{ROWS}', \
             'rows.per.second' = '4000', 'batch.max.size' = '64')"
        ),
        "CREATE MATERIALIZED VIEW totals AS SELECT COUNT(*) AS n, SUM(seq) AS total FROM gen"
            .into(),
    ]
}

async fn open(storage: &Path) -> Arc<LaminarDB> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let db = LaminarDB::builder()
            .storage_dir(storage)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            })
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .build()
            .await
            .expect("open database");
        let mut failure = None;
        for statement in statements() {
            if let Err(error) = db.execute(&statement).await {
                failure = Some(format!("{statement}: {error}"));
                break;
            }
        }
        match failure {
            None => return db,
            // The previous generation still holds the storage lock.
            Some(error) if error.contains("LDB-0014") && tokio::time::Instant::now() < deadline => {
                drop(db);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Some(error) => panic!("{error}"),
        }
    }
}

/// `(rows counted, sum of seq)` in the view, `(0, 0)` before the first row.
async fn totals(db: &LaminarDB) -> (i64, i64) {
    let ExecuteResult::Query(mut query) = db
        .execute("SELECT n, total FROM totals")
        .await
        .expect("query")
    else {
        panic!("expected a query result");
    };
    let rows = query.subscribe_raw().expect("subscribe");
    let batches = tokio::time::timeout(CONVERGE, rows.read_all())
        .await
        .expect("snapshot in time")
        .expect("query");
    let value = |batch: &arrow::array::RecordBatch, column: usize| {
        let values = batch.column(column);
        let values = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT");
        if values.is_null(0) {
            0
        } else {
            values.value(0)
        }
    };
    batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .map_or((0, 0), |batch| (value(batch, 0), value(batch, 1)))
}

async fn checkpoint(db: &LaminarDB) {
    let result = db.checkpoint().await.expect("checkpoint");
    assert!(result.success, "checkpoint failed: {result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_split_generator_resumes_every_split_from_the_committed_cursor() {
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path()).await;
    db.start().await.expect("start");
    let deadline = tokio::time::Instant::now() + CONVERGE;
    while totals(&db).await.0 < ROWS / 8 {
        assert!(tokio::time::Instant::now() < deadline, "no rows arrived");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    checkpoint(&db).await;
    let (before, _) = totals(&db).await;
    assert!(before < ROWS, "the restart must fall inside the sequence");
    db.shutdown().await.expect("shutdown");
    drop(db);

    let db = open(storage.path()).await;
    db.start().await.expect("restart from the committed cursor");
    let expected = (ROWS, ROWS * (ROWS - 1) / 2);
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let observed = totals(&db).await;
        assert!(observed.0 <= ROWS, "rows were counted twice: {observed:?}");
        if observed == expected {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "totals stopped at {observed:?}, expected {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    db.shutdown().await.expect("shutdown");
}
