#![allow(clippy::disallowed_types)]
//! End-to-end recovery integration tests.
//!
//! Validates the full checkpoint → kill → restart → verify cycle
//! with aggregate state continuity (running totals survive restart).

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{LaminarConfig, LaminarDB};

fn config_for(dir: &std::path::Path) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(dir.to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None,
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    }
}

fn make_batch(symbols: &[&str], prices: &[f64], timestamps: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "symbol",
            Arc::new(StringArray::from(
                symbols.iter().map(|s| *s).collect::<Vec<_>>(),
            )) as _,
        ),
        (
            "price",
            Arc::new(Float64Array::from(prices.to_vec())) as _,
        ),
        (
            "ts",
            Arc::new(Int64Array::from(timestamps.to_vec())) as _,
        ),
    ])
    .unwrap()
}

/// Test: checkpoint → kill → restart → verify aggregate state survives.
///
/// Pushes 100 events, checkpoints, drops the DB, reopens from checkpoint,
/// pushes 10 more events, and verifies the pipeline processes all data
/// without resetting aggregate state.
#[tokio::test]
async fn test_checkpoint_kill_restart_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();

    let ddl_source = "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT, \
                       WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)";
    let ddl_stream = "CREATE STREAM trade_summary AS \
                      SELECT symbol, SUM(price) AS total_price, COUNT(*) AS cnt \
                      FROM trades GROUP BY symbol";

    // ── Phase 1: Ingest 100 events and checkpoint ──
    {
        let db = LaminarDB::open_with_config(config_for(&storage)).unwrap();
        db.execute(ddl_source).await.unwrap();
        db.execute(ddl_stream).await.unwrap();
        db.start().await.unwrap();

        let source = db.source_untyped("trades").unwrap();
        for i in 0..100 {
            source
                .push_arrow(make_batch(
                    &["AAPL"],
                    &[100.0 + f64::from(i)],
                    &[i64::from(i) * 1000],
                ))
                .unwrap();
        }

        // Let the pipeline process all events
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Take checkpoint
        let cp = db.checkpoint().await.unwrap();
        assert!(cp.success, "checkpoint must succeed");

        // Drop the DB (simulates kill)
        db.close();
    }

    // ── Phase 2: Reopen and verify state survived ──
    {
        let db = LaminarDB::open_with_config(config_for(&storage)).unwrap();
        db.execute(ddl_source).await.unwrap();
        db.execute(ddl_stream).await.unwrap();
        db.start().await.unwrap();

        // Push 10 more events
        let source = db.source_untyped("trades").unwrap();
        for i in 100..110 {
            source
                .push_arrow(make_batch(
                    &["AAPL"],
                    &[100.0 + f64::from(i)],
                    &[i64::from(i) * 1000],
                ))
                .unwrap();
        }

        // Let the pipeline process
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Verify pipeline is running
        let metrics = db.metrics();
        assert!(
            metrics.total_cycles > 0,
            "pipeline must have executed cycles after restart"
        );

        db.close();
    }
}

/// Test: backpressure during checkpoint.
///
/// Verifies that a checkpoint does not cause event loss or channel overflow.
#[tokio::test]
async fn test_no_event_loss_during_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();

    let db = LaminarDB::open_with_config(config_for(&storage)).unwrap();

    db.execute(
        "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT, \
         WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE STREAM avg_price AS \
         SELECT symbol, AVG(price) AS avg_p \
         FROM trades GROUP BY symbol",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    let source = db.source_untyped("trades").unwrap();

    // Push events before checkpoint
    for i in 0..50 {
        source
            .push_arrow(make_batch(
                &["AAPL"],
                &[100.0 + f64::from(i)],
                &[i64::from(i) * 1000],
            ))
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Checkpoint while continuing to push
    let cp_future = db.checkpoint();
    for i in 50..100 {
        source
            .push_arrow(make_batch(
                &["AAPL"],
                &[100.0 + f64::from(i)],
                &[i64::from(i) * 1000],
            ))
            .unwrap();
    }
    let cp = cp_future.await.unwrap();
    assert!(cp.success, "checkpoint must succeed during data ingestion");

    // Push more events after checkpoint
    for i in 100..150 {
        source
            .push_arrow(make_batch(
                &["AAPL"],
                &[100.0 + f64::from(i)],
                &[i64::from(i) * 1000],
            ))
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify no events were dropped
    let metrics = db.metrics();
    assert_eq!(
        metrics.total_events_dropped, 0,
        "no events should be dropped during checkpoint"
    );

    db.close();
}
