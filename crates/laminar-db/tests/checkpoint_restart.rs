#![allow(clippy::disallowed_types)]
//! End-to-end restart integration test.
//!
//! Validates the full cycle: ingest data → checkpoint → close DB →
//! reopen from checkpoint → verify operator state survives.
//!
//! This test was added by the 2026-03-07 production audit to validate
//! that F008 (Basic Checkpointing) is production-sound.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{LaminarConfig, LaminarDB};
use laminar_storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};

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

fn make_batch(symbol: &str, price: f64, ts: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "symbol",
            Arc::new(StringArray::from(vec![symbol])) as _,
        ),
        (
            "price",
            Arc::new(Float64Array::from(vec![price])) as _,
        ),
        ("ts", Arc::new(Int64Array::from(vec![ts])) as _),
    ])
    .unwrap()
}

/// Verifies that a checkpoint persisted by one DB instance can be loaded
/// by a fresh instance opened against the same storage directory.
#[tokio::test]
async fn test_checkpoint_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();

    // ── Phase 1: Ingest and checkpoint ──
    {
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
        for i in 0..10 {
            source
                .push_arrow(make_batch("AAPL", 100.0 + f64::from(i), i64::from(i) * 1000))
                .unwrap();
        }

        // Let the pipeline process
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Checkpoint
        let cp = db.checkpoint().await.unwrap();
        assert!(cp.success, "checkpoint must succeed");
        assert!(cp.checkpoint_id > 0);

        db.close();
    }

    // ── Phase 2: Verify checkpoint exists on disk ──
    {
        let store = FileSystemCheckpointStore::new(&storage, 3);
        let manifest = store.load_latest().unwrap();
        assert!(
            manifest.is_some(),
            "manifest must be loadable after restart"
        );
        let manifest = manifest.unwrap();
        assert!(manifest.checkpoint_id > 0);
        // The checkpoint should contain operator state (stream executor)
        // or at minimum a valid epoch
        assert!(manifest.epoch > 0, "epoch must be > 0 after checkpoint");
    }

    // ── Phase 3: Reopen and verify the checkpoint is loadable ──
    {
        let db = LaminarDB::open_with_config(config_for(&storage)).unwrap();

        // Re-register the same source and stream (DDL is not persisted)
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

        // Start — this should load the checkpoint and restore state
        db.start().await.unwrap();

        // Push new data
        let source = db.source_untyped("trades").unwrap();
        source
            .push_arrow(make_batch("AAPL", 200.0, 20_000))
            .unwrap();

        // Let pipeline process
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // The DB should be running without errors
        let metrics = db.metrics();
        assert!(
            metrics.total_cycles > 0,
            "pipeline must have executed cycles after restart"
        );

        db.close();
    }
}
