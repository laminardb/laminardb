
use super::*;
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use tempfile::TempDir;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
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

#[tokio::test]
async fn test_open_creates_table() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Open with schema should create the table.
    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    assert_eq!(table.version(), Some(0));

    // Verify _delta_log directory was created.
    let delta_log = temp_dir.path().join("_delta_log");
    assert!(delta_log.exists(), "_delta_log directory should exist");
}

#[tokio::test]
async fn test_open_existing_table() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Create the table.
    let schema = test_schema();
    let _ = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Reopen without schema - should work.
    let table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();

    assert_eq!(table.version(), Some(0));
}

#[tokio::test]
async fn test_open_nonexistent_without_schema_defers() {
    let temp_dir = TempDir::new().unwrap();
    let nonexistent_table = temp_dir.path().join("nonexistent");
    std::fs::create_dir_all(&nonexistent_table).unwrap();
    let table_path = nonexistent_table.to_str().unwrap();

    // Open without schema returns an uninitialized table (deferred creation).
    let result = open_or_create_table(table_path, HashMap::new(), None).await;
    assert!(result.is_ok());
    let table = result.unwrap();
    assert!(table.version().is_none(), "table should be uninitialized");
}

#[tokio::test]
async fn test_write_batch_creates_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Create table.
    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write a batch.
    let batch = test_batch(100);
    let (table, version) = write_batches(
        table,
        vec![batch],
        "test-writer",
        1,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();

    assert_eq!(version, 1);
    assert_eq!(table.version(), Some(1));

    // Verify Parquet files were created (in the table directory).
    let parquet_files: Vec<_> = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();

    assert!(
        !parquet_files.is_empty(),
        "should have created Parquet files"
    );
}

#[tokio::test]
async fn test_exactly_once_epoch_skip() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let writer_id = "exactly-once-writer";

    // Create table and write epoch 1.
    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    let batch = test_batch(10);
    let (table, _) = write_batches(
        table,
        vec![batch.clone()],
        writer_id,
        1,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();

    // Check last committed epoch.
    let last_epoch = get_last_committed_epoch(&table, writer_id).await;
    assert_eq!(last_epoch, 1);

    // Simulate recovery: reopen table.
    let reopened_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();

    // Verify we can read the last committed epoch.
    let recovered_epoch = get_last_committed_epoch(&reopened_table, writer_id).await;
    assert_eq!(recovered_epoch, 1);
}

#[tokio::test]
async fn test_multiple_epochs_sequential() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let writer_id = "sequential-writer";

    let schema = test_schema();
    let mut table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write epochs 1, 2, 3.
    for epoch in 1..=3 {
        let batch = test_batch(10);
        let result = write_batches(
            table,
            vec![batch],
            writer_id,
            epoch,
            SaveMode::Append,
            None,
            false,
            None,
            false,
            None,
        )
        .await
        .unwrap();
        table = result.0;
        assert_eq!(result.1, epoch as i64);
    }

    // Final version should be 3.
    assert_eq!(table.version(), Some(3));

    // Last committed epoch should be 3.
    let last_epoch = get_last_committed_epoch(&table, writer_id).await;
    assert_eq!(last_epoch, 3);
}

#[tokio::test]
async fn test_get_table_schema() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let expected_schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&expected_schema))
        .await
        .unwrap();

    let actual_schema = get_table_schema(&table).unwrap();

    // Verify field count and names match.
    assert_eq!(actual_schema.fields().len(), expected_schema.fields().len());
    for (expected, actual) in expected_schema.fields().iter().zip(actual_schema.fields()) {
        assert_eq!(expected.name(), actual.name());
    }
}

#[tokio::test]
async fn test_write_empty_batches() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write empty batch list - should be no-op.
    let (table, version) = write_batches(
        table,
        vec![],
        "test-writer",
        1,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();

    // Version should still be 0 (no write happened).
    assert_eq!(version, 0);
    assert_eq!(table.version(), Some(0));
}

#[tokio::test]
async fn test_write_multiple_batches() {
    // Test writing multiple batches in a single transaction.
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write multiple batches.
    let batch1 = test_batch(50);
    let batch2 = test_batch(50);
    let (table, version) = write_batches(
        table,
        vec![batch1, batch2],
        "multi-batch-writer",
        1,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();

    assert_eq!(version, 1);
    assert_eq!(table.version(), Some(1));

    // Reopen and verify we can read the state.
    let reopened = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    assert_eq!(reopened.version(), Some(1));
}

#[test]
fn test_path_to_url_local() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let url = path_to_url(path).unwrap();
    assert!(url.scheme() == "file");
}

#[test]
fn test_path_to_url_s3() {
    let url = path_to_url("s3://my-bucket/path/to/table").unwrap();
    assert_eq!(url.scheme(), "s3");
    assert_eq!(url.host_str(), Some("my-bucket"));
}

#[test]
fn test_path_to_url_azure() {
    let url = path_to_url("az://my-container/path/to/table").unwrap();
    assert_eq!(url.scheme(), "az");
}

#[test]
fn test_path_to_url_gcs() {
    let url = path_to_url("gs://my-bucket/path/to/table").unwrap();
    assert_eq!(url.scheme(), "gs");
}

// ── End-to-end tests for new functionality ──

#[tokio::test]
async fn test_get_latest_version() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let mut table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Initial version is 0.
    let v = get_latest_version(&mut table).await.unwrap();
    assert_eq!(v, 0);

    // Write a batch -> version 1.
    let batch = test_batch(10);
    let (returned_table, version) = write_batches(
        table,
        vec![batch],
        "writer",
        1,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();
    assert_eq!(version, 1);
    table = returned_table;

    let v = get_latest_version(&mut table).await.unwrap();
    assert_eq!(v, 1);
}

#[tokio::test]
async fn test_read_batches_at_version() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write 50 rows at version 1.
    let batch = test_batch(50);
    let (table, _) = write_batches(
        table,
        vec![batch],
        "writer",
        1,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();

    // Write 30 more rows at version 2.
    let batch = test_batch(30);
    let (_table, _) = write_batches(
        table,
        vec![batch],
        "writer",
        2,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();

    // Read version 1 — should get 50 rows.
    let mut read_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let (batches, _) = read_batches_at_version(&mut read_table, 1, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 50);

    // Read version 2 — should get 80 rows (cumulative).
    let (batches, _) = read_batches_at_version(&mut read_table, 2, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 80);
}

#[tokio::test]
async fn test_sink_source_roundtrip() {
    use super::super::delta::DeltaLakeSink;
    use super::super::delta_config::DeltaLakeSinkConfig;
    use super::super::delta_source::DeltaSource;
    use super::super::delta_source_config::DeltaSourceConfig;
    use crate::config::ConnectorConfig;
    use crate::connector::{SinkConnector, SourceConnector};

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Write data via sink.
    let sink_config = DeltaLakeSinkConfig::new(table_path);
    let mut sink = DeltaLakeSink::with_schema(sink_config, test_schema());
    let connector_config = ConnectorConfig::new("delta-lake");
    sink.open(&connector_config).await.unwrap();

    sink.begin_epoch(1).await.unwrap();
    let batch = test_batch(25);
    sink.write_batch(&batch).await.unwrap();
    sink.pre_commit(1).await.unwrap();
    sink.commit_epoch(1).await.unwrap();
    sink.close().await.unwrap();

    // Read data via source.
    let mut source_config = DeltaSourceConfig::new(table_path);
    source_config.starting_version = Some(0);
    let mut source = DeltaSource::new(source_config, None);
    let source_connector_config = ConnectorConfig::new("delta-lake");
    source.open(&source_connector_config).await.unwrap();

    // Poll — should get version 1 data (25 rows).
    let result = source.poll_batch(10000).await.unwrap();
    assert!(result.is_some(), "should have received a batch");
    let total_rows: usize = {
        let mut rows = result.unwrap().records.num_rows();
        // Drain any remaining buffered batches.
        while let Ok(Some(batch)) = source.poll_batch(10000).await {
            rows += batch.records.num_rows();
        }
        rows
    };
    assert_eq!(total_rows, 25);

    source.close().await.unwrap();
}

#[tokio::test]
async fn test_source_checkpoint_restore() {
    use super::super::delta_source::DeltaSource;
    use super::super::delta_source_config::DeltaSourceConfig;
    use crate::config::ConnectorConfig;
    use crate::connector::SourceConnector;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Create table and write 2 versions.
    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    let (table, _) = write_batches(
        table,
        vec![test_batch(10)],
        "writer",
        1,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();
    let (_table, _) = write_batches(
        table,
        vec![test_batch(20)],
        "writer",
        2,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();

    // Open source starting from version 0. The source jumps to the
    // latest version (2) in a single poll, reading the full snapshot.
    let mut source_config = DeltaSourceConfig::new(table_path);
    source_config.starting_version = Some(0);
    let mut source = DeltaSource::new(source_config.clone(), None);
    let connector_config = ConnectorConfig::new("delta-lake");
    source.open(&connector_config).await.unwrap();

    // Poll to consume latest version (2).
    let _ = source.poll_batch(10000).await.unwrap();
    // Drain buffered.
    while let Ok(Some(_)) = source.poll_batch(10000).await {}

    // Checkpoint reflects the fully-consumed latest version.
    let cp = source.checkpoint();
    assert_eq!(cp.get_offset("delta_version"), Some("2"));
    source.close().await.unwrap();

    // Restore from checkpoint — should resume at version 2.
    let mut source2 = DeltaSource::new(source_config, None);
    source2.open(&connector_config).await.unwrap();
    source2.restore(&cp).await.unwrap();

    assert_eq!(source2.current_version(), 2);

    // No new data — already at latest.
    let result = source2.poll_batch(10000).await.unwrap();
    assert!(result.is_none());

    source2.close().await.unwrap();
}

#[tokio::test]
async fn test_auto_flush_writes_data() {
    use super::super::delta::DeltaLakeSink;
    use super::super::delta_config::DeltaLakeSinkConfig;
    use crate::config::ConnectorConfig;
    use crate::connector::SinkConnector;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Configure a small buffer to trigger auto-flush.
    let mut sink_config = DeltaLakeSinkConfig::new(table_path);
    sink_config.max_buffer_records = 10;
    let mut sink = DeltaLakeSink::with_schema(sink_config, test_schema());

    let connector_config = ConnectorConfig::new("delta-lake");
    sink.open(&connector_config).await.unwrap();

    sink.begin_epoch(1).await.unwrap();

    // Write 25 rows — should trigger auto-flush after 10.
    let batch = test_batch(25);
    sink.write_batch(&batch).await.unwrap();

    // Commit the rest.
    sink.pre_commit(1).await.unwrap();
    sink.commit_epoch(1).await.unwrap();
    sink.close().await.unwrap();

    // Verify all 25 rows are in the Delta table.
    let mut table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let latest = get_latest_version(&mut table).await.unwrap();
    assert!(latest >= 1, "should have at least 1 version");

    let (batches, _) = read_batches_at_version(&mut table, latest, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 25,
        "all 25 rows should be written, not dropped by auto-flush"
    );
}

#[tokio::test]
async fn test_sink_exactly_once_epoch() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();
    let writer_id = "exactly-once-test";

    let schema = test_schema();
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write epoch 1 with 10 rows.
    let (table, v1) = write_batches(
        table,
        vec![test_batch(10)],
        writer_id,
        1,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();
    assert_eq!(v1, 1);

    // Write epoch 2 with 15 rows using the same writer.
    let (table, v2) = write_batches(
        table,
        vec![test_batch(15)],
        writer_id,
        2,
        SaveMode::Append,
        None,
        false,
        None,
        false,
        None,
    )
    .await
    .unwrap();
    assert_eq!(v2, 2);

    // Verify the last committed epoch is 2.
    let last_epoch = get_last_committed_epoch(&table, writer_id).await;
    assert_eq!(last_epoch, 2);

    // Verify total rows = 25 (10 + 15).
    let mut read_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let (batches, _) = read_batches_at_version(&mut read_table, 2, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 25);
}

#[tokio::test]
async fn test_schema_evolution_adds_column() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Create table with 2-column schema.
    let schema_v1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let table = open_or_create_table(table_path, HashMap::new(), Some(&schema_v1))
        .await
        .unwrap();

    // Write batch with 2 columns.
    let batch_v1 = RecordBatch::try_new(
        schema_v1.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let (table, _) = write_batches(
        table,
        vec![batch_v1],
        "evo-writer",
        1,
        SaveMode::Append,
        None,
        true, // schema_evolution enabled
        None,
        false,
        None,
    )
    .await
    .unwrap();

    // Write batch with 3 columns (extra "score" column).
    let schema_v2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let batch_v2 = RecordBatch::try_new(
        schema_v2,
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(Float64Array::from(vec![99.5])),
        ],
    )
    .unwrap();
    let (table, _) = write_batches(
        table,
        vec![batch_v2],
        "evo-writer",
        2,
        SaveMode::Append,
        None,
        true,
        None,
        false,
        None,
    )
    .await
    .unwrap();

    // Verify table schema now has all 3 columns.
    let final_schema = get_table_schema(&table).unwrap();
    assert_eq!(final_schema.fields().len(), 3);
    assert_eq!(final_schema.field(0).name(), "id");
    assert_eq!(final_schema.field(1).name(), "name");
    assert_eq!(final_schema.field(2).name(), "score");

    // Verify all rows are readable.
    let mut read_table = open_or_create_table(table_path, HashMap::new(), None)
        .await
        .unwrap();
    let (batches, _) = read_batches_at_version(&mut read_table, 2, 10000)
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_compaction_reduces_files() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let schema = test_schema();
    let mut table = open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    // Write 10 small batches (1 file each = 10 versions).
    for epoch in 1..=10u64 {
        let batch = test_batch(5);
        let (t, _) = write_batches(
            table,
            vec![batch],
            "compaction-writer",
            epoch,
            SaveMode::Append,
            None,
            false,
            None,
            false,
            None,
        )
        .await
        .unwrap();
        table = t;
    }

    // Count Parquet files before compaction.
    let parquet_before: Vec<_> = std::fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    assert!(
        parquet_before.len() >= 10,
        "should have at least 10 Parquet files before compaction, got {}",
        parquet_before.len()
    );

    // Run compaction.
    let (table, result) = run_compaction(table, 128 * 1024 * 1024, &[], None)
        .await
        .unwrap();
    assert!(
        result.files_removed > 0,
        "compaction should have removed files"
    );

    // Compaction itself proves file reduction via metrics.
    // Vacuum is not tested here — delta-rs enforces a 168h minimum
    // retention, and test files are too fresh to be vacuumed.
    drop(table);
}
