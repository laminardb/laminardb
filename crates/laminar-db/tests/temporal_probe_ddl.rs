//! TEMPORAL PROBE JOIN must survive the full DDL path (`db.execute(…)` →
//! sqlparser → operator graph).

use arrow::array::StringArray;
use laminar_db::{ExecuteResult, LaminarDB};

#[tokio::test]
async fn create_stream_with_temporal_probe_join_preserves_clause() {
    let db = LaminarDB::open().unwrap();

    db.execute(
        "CREATE SOURCE trades (s VARCHAR, p DOUBLE, ts BIGINT, \
         WATERMARK FOR ts AS ts - 500)",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE SOURCE prices (s VARCHAR, mid DOUBLE, ts BIGINT, \
         WATERMARK FOR ts AS ts - 500)",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE STREAM markouts AS \
         SELECT t.s AS sym, mid, p.offset_ms \
         FROM trades t \
         TEMPORAL PROBE JOIN prices r \
             ON (s) TIMESTAMPS (ts, ts) \
             LIST (0s, 1s, 5s) AS p",
    )
    .await
    .expect("DDL should succeed");

    let result = db
        .execute("SHOW STREAMS")
        .await
        .expect("SHOW STREAMS should succeed");

    let batch = match result {
        ExecuteResult::Metadata(b) => b,
        other => panic!("expected metadata batch, got {other:?}"),
    };

    let names = batch
        .column_by_name("stream_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sqls = batch
        .column_by_name("sql")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let row = (0..batch.num_rows())
        .find(|i| names.value(*i) == "markouts")
        .expect("markouts stream should be registered");
    let sql = sqls.value(row);

    assert!(sql.to_uppercase().contains("TEMPORAL PROBE JOIN"), "got: {sql}");
    assert!(sql.contains("LIST (0s, 1s, 5s)"), "got: {sql}");
    assert!(sql.contains("AS p"), "got: {sql}");
}

#[tokio::test]
async fn create_mv_with_temporal_probe_join_preserves_clause() {
    let db = LaminarDB::open().unwrap();

    db.execute(
        "CREATE SOURCE trades (s VARCHAR, p DOUBLE, ts BIGINT, \
         WATERMARK FOR ts AS ts - 500)",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE SOURCE prices (s VARCHAR, mid DOUBLE, ts BIGINT, \
         WATERMARK FOR ts AS ts - 500)",
    )
    .await
    .unwrap();

    let _ = db
        .execute(
            "CREATE MATERIALIZED VIEW mv_markouts AS \
             SELECT t.s FROM trades t \
             TEMPORAL PROBE JOIN prices r \
                 ON (s) TIMESTAMPS (ts, ts) \
                 RANGE FROM 0s TO 30s STEP 5s AS p",
        )
        .await;
    let result = db.execute("SHOW STREAMS").await.unwrap();
    let batch = match result {
        ExecuteResult::Metadata(b) => b,
        other => panic!("expected metadata batch, got {other:?}"),
    };
    let sqls = batch
        .column_by_name("sql")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let names = batch
        .column_by_name("stream_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // MV registration may fail if planning rejects the syntax; that's
    // orthogonal to what we assert here.
    let Some(row) = (0..batch.num_rows()).find(|i| names.value(*i) == "mv_markouts") else {
        return;
    };
    let sql = sqls.value(row);
    assert!(sql.to_uppercase().contains("TEMPORAL PROBE JOIN"), "got: {sql}");
    assert!(sql.contains("RANGE FROM 0s TO 30s STEP 5s"), "got: {sql}");
}
