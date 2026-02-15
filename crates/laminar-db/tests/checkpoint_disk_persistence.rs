//! Integration test: verify that `db.checkpoint()` persists to disk.
//!
//! Reproduces the scenario from laminardb/laminardb-python#4 where
//! `checkpoint()` returned a valid ID but wrote nothing to disk.

use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{LaminarConfig, LaminarDB};
use laminar_storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};

/// Build a `LaminarConfig` with checkpoint enabled and `storage_dir`
/// pointed at the given directory.
fn config_with_storage(dir: &std::path::Path) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(dir.to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None, // manual only
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    }
}

#[tokio::test]
async fn test_manual_checkpoint_writes_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();

    let db = LaminarDB::open_with_config(config_with_storage(&storage)).unwrap();

    db.execute("CREATE SOURCE sensors (ts BIGINT, device VARCHAR, value DOUBLE)")
        .await
        .unwrap();
    db.execute(
        "CREATE STREAM avg_val AS SELECT device, AVG(value) AS avg_v FROM sensors GROUP BY device",
    )
    .await
    .unwrap();
    db.execute("CREATE SINK out FROM avg_val").await.unwrap();

    db.start().await.unwrap();

    // Insert some data
    let source = db.source_untyped("sensors").unwrap();
    let schema = source.schema();
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
            std::sync::Arc::new(arrow::array::StringArray::from(vec!["a", "b", "a"])),
            std::sync::Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();
    source.push_arrow(batch).unwrap();

    // Manual checkpoint — this should persist to disk
    let result = db.checkpoint().await.unwrap();
    assert!(result.success, "checkpoint should succeed");
    assert_eq!(result.checkpoint_id, 1);

    // Verify files exist on disk
    let checkpoint_dir = storage.join("checkpoints");
    assert!(
        checkpoint_dir.exists(),
        "checkpoints directory should be created at {checkpoint_dir:?}"
    );

    // Verify the store can load the manifest
    let store = FileSystemCheckpointStore::new(&storage, 3);
    let manifest = store.load_latest().unwrap();
    assert!(manifest.is_some(), "manifest should be loadable from disk");

    let manifest = manifest.unwrap();
    assert_eq!(manifest.checkpoint_id, 1);
    assert_eq!(manifest.epoch, 1);

    db.close();
}

#[tokio::test]
async fn test_checkpoint_errors_when_not_enabled() {
    let db = LaminarDB::open().unwrap(); // default config, no checkpoint

    let err = db.checkpoint().await;
    assert!(err.is_err(), "checkpoint should fail when not enabled");
}

#[tokio::test]
async fn test_checkpoint_errors_before_start() {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(config_with_storage(dir.path())).unwrap();

    // checkpoint enabled but start() not called — coordinator not initialized
    let err = db.checkpoint().await;
    assert!(err.is_err(), "checkpoint should fail before start()");
}
