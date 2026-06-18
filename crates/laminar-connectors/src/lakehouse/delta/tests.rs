use super::super::delta_config::DeltaCatalogType;
use super::*;
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn test_config() -> DeltaLakeSinkConfig {
    use std::sync::atomic::{AtomicU64, Ordering};
    // Unique per call so a leftover dir from a prior run can't make a later run
    // hit an existing path (the hardcoded "8f3a" suffix caused flakes).
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let uniq = format!(
        "{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    #[cfg(unix)]
    let path = format!("/tmp/delta_test_nonexistent_{uniq}");
    #[cfg(windows)]
    let path = format!("C:\\delta_test_nonexistent_{uniq}");
    DeltaLakeSinkConfig::new(&path)
}

fn upsert_config() -> DeltaLakeSinkConfig {
    let mut cfg = test_config();
    cfg.write_mode = DeltaWriteMode::Upsert;
    cfg.merge_key_columns = vec!["id".to_string()];
    cfg
}

fn test_batch(n: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<&str> = (0..n).map(|_| "test").collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

// ── Constructor tests ──

#[test]
fn test_new_defaults() {
    let sink = DeltaLakeSink::new(test_config(), None);
    assert_eq!(sink.state(), ConnectorState::Created);
    assert_eq!(sink.current_epoch(), 0);
    assert_eq!(sink.last_committed_epoch(), 0);
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.buffered_bytes(), 0);
    assert_eq!(sink.delta_version(), 0);
    assert!(sink.schema.is_none());
}

#[test]
fn test_with_schema() {
    let schema = test_schema();
    let sink = DeltaLakeSink::with_schema(test_config(), schema.clone());
    assert_eq!(sink.schema(), schema);
}

#[test]
fn test_schema_empty_when_none() {
    let sink = DeltaLakeSink::new(test_config(), None);
    let schema = sink.schema();
    assert_eq!(schema.fields().len(), 0);
}

#[cfg(feature = "delta-lake")]
#[test]
fn test_deferred_init_flag_default_false() {
    let sink = DeltaLakeSink::new(test_config(), None);
    assert!(!sink.needs_deferred_delta_init);
}

fn unity_config() -> DeltaLakeSinkConfig {
    let mut config = test_config();
    config.catalog_type = DeltaCatalogType::Unity {
        workspace_url: "https://test.azuredatabricks.net".to_string(),
        access_token: "dapi123".to_string(),
    };
    config.catalog_name = Some("main".to_string());
    config.catalog_schema = Some("default".to_string());
    config.catalog_storage_location = Some("abfss://c@acct.dfs.core.windows.net/t".to_string());
    config
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn test_open_defers_init_for_unity_no_schema() {
    use crate::config::ConnectorConfig;

    let config = unity_config();
    let mut sink = DeltaLakeSink::new(config, None);

    // open() with empty ConnectorConfig (simulates factory path)
    let connector_config = ConnectorConfig::new("delta-lake");
    // open() will re-parse but table.path is "/tmp/delta_test" (local),
    // so it won't actually reach UC REST. However from_config requires
    // table.path, so we use the sink's existing config by passing empty.
    // The sink skips re-parse when properties are empty.
    let result = sink.open(&connector_config).await;
    assert!(result.is_ok());

    // Should be in Initializing state with deferred flag set.
    assert!(sink.needs_deferred_delta_init);
    assert_eq!(sink.state(), ConnectorState::Initializing);
    assert!(sink.schema.is_none());
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn test_deferred_init_transitions_to_failed_on_error() {
    // When deferred init fails, the sink must transition to Failed
    // to prevent an unbounded retry storm.
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Initializing;
    sink.needs_deferred_delta_init = true;
    sink.schema = Some(test_schema());

    // begin_epoch will try init_delta_table() which will fail
    // (no real Delta table at /tmp/delta_test). The sink should
    // transition to Failed.
    let result = sink.begin_epoch(1).await;
    assert!(result.is_err());
    assert_eq!(sink.state(), ConnectorState::Failed);
    // Flag may still be set, but Failed state prevents further usage.
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn test_write_batch_accepts_initializing_state() {
    // During deferred init, write_batch must accept Initializing state
    // so the first batch can provide the schema.
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Initializing;
    sink.needs_deferred_delta_init = true;

    let batch = test_batch(5);
    // write_batch sets schema, then tries init_delta_table which fails.
    // Sink transitions to Failed.
    let result = sink.write_batch(&batch).await;
    assert!(result.is_err());
    assert_eq!(sink.state(), ConnectorState::Failed);
    // Schema was set before init was attempted.
    assert!(sink.schema.is_some());
}

#[test]
fn test_no_deferred_init_without_catalog_storage_location() {
    // Unity catalog without catalog.storage.location should NOT defer.
    let mut config = unity_config();
    config.catalog_storage_location = None;
    let sink = DeltaLakeSink::new(config, None);

    assert!(!sink.needs_deferred_delta_init);
}

#[test]
fn test_no_deferred_init_with_schema() {
    // Unity catalog with schema already set should NOT defer.
    let config = unity_config();
    let sink = DeltaLakeSink::with_schema(config, test_schema());

    assert!(!sink.needs_deferred_delta_init);
}

// ── Batch size estimation ──

#[test]
fn test_estimate_batch_size() {
    let batch = test_batch(100);
    let size = DeltaLakeSink::estimate_batch_size(&batch);
    assert!(size > 0);
}

#[test]
fn test_estimate_batch_size_empty() {
    let batch = RecordBatch::new_empty(test_schema());
    let size = DeltaLakeSink::estimate_batch_size(&batch);
    // Arrow arrays have baseline buffer allocation even with 0 rows,
    // so size may be small but not necessarily zero.
    assert!(size < 1024);
}

// ── Should flush tests ──

#[test]
fn test_should_flush_by_rows() {
    let mut config = test_config();
    config.max_buffer_records = 100;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.buffered_rows = 99;
    assert!(!sink.should_flush());
    sink.buffered_rows = 100;
    assert!(sink.should_flush());
}

#[test]
fn test_should_flush_by_bytes() {
    let mut config = test_config();
    config.target_file_size = 1000;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.buffered_bytes = 999;
    assert!(!sink.should_flush());
    sink.buffered_bytes = 1000;
    assert!(sink.should_flush());
}

#[test]
fn test_should_flush_empty() {
    let sink = DeltaLakeSink::new(test_config(), None);
    assert!(!sink.should_flush());
}

#[tokio::test]
async fn test_exactly_once_buffer_backpressure() {
    // Exactly-once mode rejects writes once the cumulative pending
    // buffer exceeds the hard cap (4× max_buffer_records). A single
    // incoming batch that by itself exceeds the cap is admitted (we
    // cannot split it without breaking exactly-once), but subsequent
    // batches hit backpressure.
    let mut config = test_config();
    config.delivery_guarantee = crate::connector::DeliveryGuarantee::ExactlyOnce;
    config.max_buffer_records = 10;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    // First batch of 50 rows exceeds 4× cap (40) but we admit it —
    // rejecting would wedge the pipeline with no way to split.
    let first = test_batch(50);
    sink.write_batch(&first)
        .await
        .expect("single oversized batch must be admitted");
    assert_eq!(sink.buffered_rows(), 50);

    // A second normal batch must now be rejected: cumulative pending
    // (50 + 5 = 55) exceeds the effective cap (max(40, 5) = 40).
    let second = test_batch(5);
    let err = sink
        .write_batch(&second)
        .await
        .expect_err("should reject once cumulative buffer exceeds cap");
    let msg = err.to_string();
    assert!(
        msg.contains("buffer full"),
        "expected backpressure error, got: {msg}"
    );
    // Rejected batch must NOT have been buffered.
    assert_eq!(sink.buffered_rows(), 50);
}

// ── Batch buffering tests ──

#[tokio::test]
async fn test_write_batch_buffering() {
    let mut config = test_config();
    config.max_buffer_records = 100;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(10);
    let result = sink.write_batch(&batch).await.unwrap();

    // Should buffer, not flush (10 < 100)
    assert_eq!(result.records_written, 0);
    assert_eq!(sink.buffered_rows(), 10);
    assert!(sink.buffered_bytes() > 0);
}

#[tokio::test]
async fn test_write_batch_empty() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(0);
    let result = sink.write_batch(&batch).await.unwrap();
    assert_eq!(result.records_written, 0);
    assert_eq!(sink.buffered_rows(), 0);
}

#[tokio::test]
async fn test_write_batch_not_running() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    // state is Created, not Running

    let batch = test_batch(10);
    let result = sink.write_batch(&batch).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_write_batch_sets_schema() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Running;
    assert!(sink.schema.is_none());

    let batch = test_batch(5);
    sink.write_batch(&batch).await.unwrap();
    assert!(sink.schema.is_some());
    assert_eq!(sink.schema.as_ref().unwrap().fields().len(), 3);
}

#[tokio::test]
async fn test_multiple_write_batches_accumulate() {
    let mut config = test_config();
    config.max_buffer_records = 100;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(10);
    sink.write_batch(&batch).await.unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.write_batch(&batch).await.unwrap();

    assert_eq!(sink.buffered_rows(), 30);
}

// ── Epoch lifecycle tests ──
// Note: Epoch lifecycle with real I/O is tested in delta_io.rs integration tests.

#[tokio::test]
async fn test_rollback_clears_buffer() {
    let mut config = test_config();
    config.max_buffer_records = 1000;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(50);
    sink.write_batch(&batch).await.unwrap();
    assert_eq!(sink.buffered_rows(), 50);

    sink.rollback_epoch(0).await.unwrap();
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.buffered_bytes(), 0);
}

/// D001: Rollback after `pre_commit` must discard staged data.
/// `pre_commit` stages batches; rollback discards them without writing to Delta.
#[tokio::test]
async fn test_rollback_after_pre_commit_discards_staged() {
    let mut config = test_config();
    config.max_buffer_records = 1000;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    sink.begin_epoch(1).await.unwrap();
    let batch = test_batch(50);
    sink.write_batch(&batch).await.unwrap();
    assert_eq!(sink.buffered_rows(), 50);

    // pre_commit stages the buffer
    sink.pre_commit(1).await.unwrap();
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.staged_rows, 50);
    assert!(!sink.staged_batches.is_empty());

    // rollback discards both buffer and staged data
    sink.rollback_epoch(1).await.unwrap();
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.staged_rows, 0);
    assert_eq!(sink.staged_bytes, 0);
    assert!(sink.staged_batches.is_empty());
    assert_eq!(sink.delta_version(), 0); // no Delta write occurred
}

/// Staged data is preserved across `pre_commit` → failed commit → rollback.
/// This verifies that `pre_commit` does not destroy staged state, so a
/// subsequent rollback can discard it cleanly.
#[tokio::test]
async fn test_staged_data_preserved_until_commit_or_rollback() {
    let mut config = test_config();
    config.max_buffer_records = 1000;
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    sink.begin_epoch(1).await.unwrap();
    sink.write_batch(&test_batch(25)).await.unwrap();
    sink.write_batch(&test_batch(25)).await.unwrap();

    // pre_commit moves buffer → staged
    sink.pre_commit(1).await.unwrap();
    assert_eq!(sink.staged_rows, 50);
    assert_eq!(sink.staged_batches.len(), 2);
    assert_eq!(sink.buffered_rows(), 0);

    // Simulate: commit_epoch would write to Delta, but without the
    // feature we exercise the local path. Verify staged state is
    // consumed only on success.
    // (Without delta-lake feature, commit_epoch calls commit_local
    // which succeeds.)

    // Instead, test rollback: staged data should be discarded.
    sink.rollback_epoch(1).await.unwrap();
    assert!(sink.staged_batches.is_empty());
    assert_eq!(sink.staged_rows, 0);
    assert_eq!(sink.staged_bytes, 0);
}

#[tokio::test]
async fn test_commit_empty_epoch() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Running;

    sink.begin_epoch(1).await.unwrap();
    // No writes
    sink.commit_epoch(1).await.unwrap();
    assert_eq!(sink.last_committed_epoch(), 1);
    assert_eq!(sink.delta_version(), 0); // No version bump (no files)
}

// ── Flush tests ──
// Note: These tests bypass open() and test business logic only.

#[tokio::test]
async fn test_flush_coalesces_buffer() {
    let mut config = test_config();
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    config.writer_id = "test-writer".to_string();
    let mut sink = DeltaLakeSink::new(config, None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(10);
    sink.write_batch(&batch).await.unwrap();
    sink.write_batch(&batch).await.unwrap();
    assert_eq!(sink.buffer.len(), 2);

    // flush() coalesces batches but does not write to Delta.
    sink.flush().await.unwrap();
    assert_eq!(sink.buffer.len(), 1);
    assert_eq!(sink.buffered_rows(), 20);
}

// ── Open and close tests ──
// Note: These tests use fake paths that don't exist.
// With the delta-lake feature, open() tries to actually access the path.
// See delta_io.rs for integration tests with real I/O.

#[tokio::test]
async fn test_close() {
    let mut sink = DeltaLakeSink::new(test_config(), None);
    sink.state = ConnectorState::Running;

    sink.close().await.unwrap();
    assert_eq!(sink.state(), ConnectorState::Closed);
}

// ── Capabilities tests ──

#[test]
fn test_capabilities_append_exactly_once() {
    let mut config = test_config();
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    let sink = DeltaLakeSink::new(config, None);
    let caps = sink.capabilities();
    assert!(caps.exactly_once);
    assert!(caps.idempotent);
    assert!(!caps.upsert);
    assert!(!caps.changelog);
    assert!(!caps.schema_evolution);
    assert!(!caps.partitioned);
}

#[test]
fn test_capabilities_upsert() {
    let sink = DeltaLakeSink::new(upsert_config(), None);
    let caps = sink.capabilities();
    assert!(caps.upsert);
    assert!(caps.changelog);
    assert!(caps.idempotent);
}

#[test]
fn test_capabilities_schema_evolution() {
    let mut config = test_config();
    config.schema_evolution = true;
    let sink = DeltaLakeSink::new(config, None);
    let caps = sink.capabilities();
    assert!(caps.schema_evolution);
}

#[test]
fn test_capabilities_partitioned() {
    let mut config = test_config();
    config.partition_columns = vec!["trade_date".to_string()];
    let sink = DeltaLakeSink::new(config, None);
    let caps = sink.capabilities();
    assert!(caps.partitioned);
}

#[test]
fn test_capabilities_at_least_once() {
    let mut config = test_config();
    config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
    let sink = DeltaLakeSink::new(config, None);
    let caps = sink.capabilities();
    assert!(!caps.exactly_once);
    assert!(caps.idempotent);
}

// ── Changelog splitting tests ──

fn changelog_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_ts_ms", DataType::Int64, false),
    ]))
}

fn changelog_batch() -> RecordBatch {
    RecordBatch::try_new(
        changelog_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(StringArray::from(vec!["I", "U", "D", "I", "D"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )
    .unwrap()
}

#[test]
fn test_split_changelog_batch() {
    let batch = changelog_batch();
    let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();

    // Inserts: rows 0 (I), 1 (U), 3 (I) = 3 rows
    assert_eq!(inserts.num_rows(), 3);
    // Deletes: rows 2 (D), 4 (D) = 2 rows
    assert_eq!(deletes.num_rows(), 2);

    // Metadata columns should be stripped
    assert_eq!(inserts.num_columns(), 2); // id, name only
    assert_eq!(deletes.num_columns(), 2);

    // Verify insert values
    let insert_ids = inserts
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(insert_ids.value(0), 1);
    assert_eq!(insert_ids.value(1), 2);
    assert_eq!(insert_ids.value(2), 4);

    // Verify delete values
    let delete_ids = deletes
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(delete_ids.value(0), 3);
    assert_eq!(delete_ids.value(1), 5);
}

#[test]
fn test_split_changelog_all_inserts() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["I", "I"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();

    let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
    assert_eq!(inserts.num_rows(), 2);
    assert_eq!(deletes.num_rows(), 0);
}

#[test]
fn test_split_changelog_all_deletes() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["D", "D"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();

    let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
    assert_eq!(inserts.num_rows(), 0);
    assert_eq!(deletes.num_rows(), 2);
}

#[test]
fn test_split_changelog_missing_op_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

    let result = DeltaLakeSink::split_changelog_batch(&batch);
    assert!(result.is_err());
}

#[test]
fn test_split_changelog_snapshot_read() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["r"])), // snapshot read
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .unwrap();

    let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
    assert_eq!(inserts.num_rows(), 1);
    assert_eq!(deletes.num_rows(), 0);
}

// ── Debug output test ──

#[test]
fn test_debug_output() {
    let sink = DeltaLakeSink::new(test_config(), None);
    let debug = format!("{sink:?}");
    assert!(debug.contains("DeltaLakeSink"));
    assert!(debug.contains("delta_test_nonexistent_"));
}

// ── End-to-end upsert collapse (aggregating-MV changelog → Delta table) ──

/// A Z-set changelog batch shaped like aggregating-MV output:
/// `[region: Utf8, total: Int64, __weight: Int64]`.
#[cfg(feature = "delta-lake")]
fn zset_changelog(rows: &[(&str, i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Drive one full epoch through the exactly-once sink lifecycle.
#[cfg(feature = "delta-lake")]
async fn run_epoch(sink: &mut DeltaLakeSink, epoch: u64, batch: &RecordBatch) {
    sink.begin_epoch(epoch).await.unwrap();
    sink.write_batch(batch).await.unwrap();
    sink.pre_commit(epoch).await.unwrap();
    sink.commit_epoch(epoch).await.unwrap();
}

/// Read the table back and return its current `(region, total)` rows, sorted.
#[cfg(feature = "delta-lake")]
async fn read_regions(path: &str) -> Vec<(String, i64)> {
    let ctx = datafusion::prelude::SessionContext::new();
    crate::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "t",
        path,
        std::collections::HashMap::new(),
    )
    .await
    .unwrap();
    let batches = ctx
        .sql("SELECT region, total FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for b in &batches {
        // DataFusion may return strings as Utf8View; cast to the concrete
        // types we downcast to.
        let region_arr = arrow_cast::cast(
            b.column(b.schema().index_of("region").unwrap()),
            &DataType::Utf8,
        )
        .unwrap();
        let total_arr = arrow_cast::cast(
            b.column(b.schema().index_of("total").unwrap()),
            &DataType::Int64,
        )
        .unwrap();
        let regions = region_arr.as_any().downcast_ref::<StringArray>().unwrap();
        let totals = total_arr.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((regions.value(i).to_string(), totals.value(i)));
        }
    }
    out.sort();
    out
}

/// An aggregating MV emits a Z-set changelog; the upsert sink must collapse
/// it into the table's current per-key state — surviving value updates,
/// group disappearance, and multiple updates to one key within a single
/// epoch (the case that triggers a delta-rs cardinality violation without
/// collapse).
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn upsert_collapses_aggregating_mv_to_current_state() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("agg");
    // delta-rs' local object store requires the table directory to exist.
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let mut cfg = DeltaLakeSinkConfig::new(&path);
    cfg.write_mode = DeltaWriteMode::Upsert;
    cfg.merge_key_columns = vec!["region".to_string()];
    // Exactly-once buffers the whole epoch and flushes once at commit, so
    // each epoch's changelog is collapsed together (deterministic).
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;

    // No explicit schema: the schema (and table) is derived from the first
    // batch, exactly like the production pipeline — exercising the
    // `target_schema` strip of `__weight`.
    let mut sink = DeltaLakeSink::new(cfg, None);
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();

    // Epoch 1: two brand-new groups.
    run_epoch(
        &mut sink,
        1,
        &zset_changelog(&[("east", 10, 1), ("west", 5, 1)]),
    )
    .await;
    assert_eq!(
        read_regions(&path).await,
        vec![("east".into(), 10), ("west".into(), 5)]
    );

    // Epoch 2: update east (retract 10 + insert 30), drop west, add north.
    run_epoch(
        &mut sink,
        2,
        &zset_changelog(&[
            ("east", 10, -1),
            ("east", 30, 1),
            ("west", 5, -1),
            ("north", 7, 1),
        ]),
    )
    .await;
    assert_eq!(
        read_regions(&path).await,
        vec![("east".into(), 30), ("north".into(), 7)]
    );

    // Epoch 3: two consecutive updates to "east" in one epoch. Without
    // collapse this is multiple source rows for one merge key → delta-rs
    // cardinality violation. With collapse it folds to the final value.
    run_epoch(
        &mut sink,
        3,
        &zset_changelog(&[
            ("east", 30, -1),
            ("east", 40, 1),
            ("east", 40, -1),
            ("east", 55, 1),
        ]),
    )
    .await;
    assert_eq!(
        read_regions(&path).await,
        vec![("east".into(), 55), ("north".into(), 7)]
    );

    // The target table never carried the Z-set weight column.
    assert!(sink.schema.as_ref().unwrap().index_of("__weight").is_err());

    // Collapse observability fired across the three epochs.
    let m = sink.sink_metrics();
    assert_eq!(m.collapse_rows_in.get(), 10);
    assert!(m.collapse_deletes_out.get() >= 1, "west was dropped");
    assert!(m.collapse_upserts_out.get() >= 4);

    sink.close().await.unwrap();
}

/// Exactly-once replay: a writer that recovers an already-committed epoch
/// (from Delta `txn` metadata) and re-applies it must leave the table
/// byte-for-byte unchanged — no new snapshot, same per-key state.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn upsert_replay_of_committed_epoch_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("agg_replay");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let mk_cfg = || {
        let mut cfg = DeltaLakeSinkConfig::new(&path);
        cfg.write_mode = DeltaWriteMode::Upsert;
        cfg.merge_key_columns = vec!["region".to_string()];
        cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        // Exactly-once recovery keys off a STABLE writer_id (default is a
        // random UUID); a restart must reuse it to find its prior epoch.
        cfg.writer_id = "replay-writer".to_string();
        cfg
    };
    let epoch2 = || {
        zset_changelog(&[
            ("east", 10, -1),
            ("east", 30, 1),
            ("west", 5, -1),
            ("north", 7, 1),
        ])
    };

    // Writer A commits epochs 1 and 2, then "crashes" (close).
    let mut sink_a = DeltaLakeSink::new(mk_cfg(), None);
    sink_a
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    run_epoch(
        &mut sink_a,
        1,
        &zset_changelog(&[("east", 10, 1), ("west", 5, 1)]),
    )
    .await;
    run_epoch(&mut sink_a, 2, &epoch2()).await;
    let expected = vec![("east".to_string(), 30), ("north".to_string(), 7)];
    assert_eq!(read_regions(&path).await, expected);
    let version_after_2 = sink_a.delta_version();
    sink_a.close().await.unwrap();

    // Writer B recovers against the same table + writer_id.
    let mut sink_b = DeltaLakeSink::new(mk_cfg(), None);
    sink_b
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    assert_eq!(
        sink_b.last_committed_epoch(),
        2,
        "epoch must be recovered from Delta txn metadata"
    );

    // Replay the already-committed epoch 2: it must be skipped end-to-end.
    sink_b.begin_epoch(2).await.unwrap();
    sink_b.write_batch(&epoch2()).await.unwrap();
    sink_b.pre_commit(2).await.unwrap();
    sink_b.commit_epoch(2).await.unwrap();

    assert_eq!(
        read_regions(&path).await,
        expected,
        "replay must not change state"
    );
    assert_eq!(
        sink_b.delta_version(),
        version_after_2,
        "replayed epoch must not create a new Delta version"
    );

    // The recovered writer continues normally with a fresh epoch.
    run_epoch(
        &mut sink_b,
        3,
        &zset_changelog(&[("east", 30, -1), ("east", 55, 1)]),
    )
    .await;
    assert_eq!(
        read_regions(&path).await,
        vec![("east".to_string(), 55), ("north".to_string(), 7)]
    );

    sink_b.close().await.unwrap();
}

// ── Coordinated-commit (designated-committer) regressions ──

#[cfg(feature = "delta-lake")]
fn coordinated_config(path: &str) -> DeltaLakeSinkConfig {
    let mut cfg = DeltaLakeSinkConfig::new(path);
    cfg.write_mode = DeltaWriteMode::Append;
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    cfg.writer_id = "writer-A".to_string();
    cfg
}

/// `is_coordinated()` drives the recovery-id selection; it must be true only
/// for append + exactly-once (the path where the committer, not the writer,
/// owns the Delta txn id).
#[test]
fn coordinated_only_for_append_exactly_once() {
    let mut cfg = test_config();
    cfg.write_mode = DeltaWriteMode::Append;
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    assert!(DeltaLakeSink::new(cfg, None).is_coordinated());

    let mut cfg = test_config();
    cfg.write_mode = DeltaWriteMode::Append;
    cfg.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
    assert!(!DeltaLakeSink::new(cfg, None).is_coordinated());

    let mut cfg = test_config();
    cfg.write_mode = DeltaWriteMode::Upsert;
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    assert!(!DeltaLakeSink::new(cfg, None).is_coordinated());
}

/// Finding A: a coordinated sink recovering against an already-committed table
/// must read the committer's txn id, not the (never-committed) `writer_id`. Before
/// the fix recovery always saw 0 and re-staged sealed epochs into orphan files.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_recovery_reads_committer_epoch() {
    use crate::connector::CoordinatedCommitter;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_recover");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    // Writer stages epoch 1 and 2; the designated committer seals them under
    // COORDINATED_COMMITTER_ID (nothing is ever committed under "writer-A").
    let mut writer = DeltaLakeSink::with_schema(coordinated_config(&path), test_schema());
    writer
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();

    for epoch in 1..=2u64 {
        writer.begin_epoch(epoch).await.unwrap();
        writer.write_batch(&test_batch(3)).await.unwrap();
        let descriptor = writer
            .pre_commit(epoch)
            .await
            .unwrap()
            .expect("coordinated pre_commit returns a descriptor");
        writer.commit_epoch(epoch).await.unwrap();
        writer
            .commit_aggregated(epoch, vec![descriptor])
            .await
            .unwrap();
    }
    writer.close().await.unwrap();

    // A fresh writer (same writer_id) recovers from the table. The committer
    // never used "writer-A", so reading writer_id would yield 0.
    let mut recovered = DeltaLakeSink::with_schema(coordinated_config(&path), test_schema());
    recovered
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    assert_eq!(
        recovered.last_committed_epoch(),
        2,
        "coordinated recovery must read the committer's sealed epoch"
    );

    // Replaying a sealed epoch must be a no-op: pre_commit returns no descriptor.
    recovered.begin_epoch(2).await.unwrap();
    recovered.write_batch(&test_batch(3)).await.unwrap();
    assert!(
        recovered.pre_commit(2).await.unwrap().is_none(),
        "sealed epoch must not re-stage Parquet"
    );
    recovered.close().await.unwrap();
}

/// Finding B: the coordinated descriptor write must honor configured Parquet
/// properties (cached at open), not the writer's hard-coded Snappy default.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_open_caches_configured_writer_properties() {
    use deltalake::parquet::basic::Compression;
    use deltalake::parquet::schema::types::ColumnPath;

    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_props");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let mut cfg = coordinated_config(&path);
    cfg.parquet.compression = "gzip".to_string();
    cfg.parquet.compression_level = 6;

    let mut sink = DeltaLakeSink::with_schema(cfg, test_schema());
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();

    let props = sink
        .cached_writer_properties
        .as_ref()
        .expect("open() caches writer properties");
    assert!(
        matches!(
            props.compression(&ColumnPath::from(Vec::<String>::new())),
            Compression::GZIP(_)
        ),
        "descriptor writer must apply configured (non-default) compression"
    );
    sink.close().await.unwrap();
}

/// Finding C: the coordinated staging write is wrapped in the same write
/// timeout the commit path uses, so a wedged object store can't hang the sink
/// task forever. A near-zero timeout forces the wrapper to fire.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn coordinated_pre_commit_honors_write_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("coord_timeout");
    std::fs::create_dir_all(&table_dir).unwrap();
    let path = table_dir.to_string_lossy().to_string();

    let mut cfg = coordinated_config(&path);
    cfg.write_timeout = Duration::from_nanos(1);

    let mut sink = DeltaLakeSink::with_schema(cfg, test_schema());
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();

    sink.begin_epoch(1).await.unwrap();
    sink.write_batch(&test_batch(3)).await.unwrap();
    let err = sink
        .pre_commit(1)
        .await
        .expect_err("a 1ns write timeout must trip the wrapper");
    assert!(
        err.to_string().contains("timed out"),
        "expected a timeout error, got: {err}"
    );
    sink.close().await.unwrap();
}
