//! Delta Lake I/O integration module.
//!
//! This module provides the actual I/O operations for Delta Lake tables via the
//! `deltalake` crate. All functions are feature-gated behind `delta-lake`.
//!
//! # Architecture
//!
//! The I/O module is separate from the business logic in [`delta.rs`](super::delta)
//! to allow:
//! - Testing business logic without the `deltalake` dependency
//! - Clean separation of concerns (buffering/epoch management vs. actual writes)
//! - Easy mocking for unit tests
//!
//! # Exactly-Once Semantics
//!
//! Delta Lake's transaction log supports application-level transaction metadata
//! via the `txn` action. We use this to store `(writer_id, epoch)` pairs, enabling
//! exactly-once semantics:
//!
//! 1. On recovery, read `txn` metadata to find the last committed epoch for this writer
//! 2. Skip epochs <= last committed (idempotent replay)
//! 3. Each write includes the epoch in `txn` metadata

#[cfg(feature = "delta-lake")]
use std::collections::HashMap;

#[cfg(feature = "delta-lake")]
use std::sync::Arc;

#[cfg(feature = "delta-lake")]
use arrow_array::RecordBatch;

#[cfg(feature = "delta-lake")]
use arrow_schema::SchemaRef;

// delta_kernel's TryIntoKernel trait is re-exported via deltalake.
#[cfg(feature = "delta-lake")]
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;

#[cfg(feature = "delta-lake")]
use deltalake::kernel::transaction::CommitProperties;

#[cfg(feature = "delta-lake")]
use deltalake::kernel::Transaction;

#[cfg(feature = "delta-lake")]
use deltalake::protocol::SaveMode;

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

#[cfg(feature = "delta-lake")]
use tracing::{debug, info, warn};

#[cfg(feature = "delta-lake")]
use url::Url;

#[cfg(feature = "delta-lake")]
use crate::error::ConnectorError;

/// Converts a path string to a URL.
#[cfg(feature = "delta-lake")]
fn path_to_url(path: &str) -> Result<Url, ConnectorError> {
    // If it already looks like a URL, parse it directly.
    if path.contains("://") {
        Url::parse(path)
            .map_err(|e| ConnectorError::ConfigurationError(format!("invalid URL '{path}': {e}")))
    } else {
        // Local path - convert to file URL.
        // First canonicalize if it exists, otherwise use as-is.
        let path_buf = std::path::Path::new(path);
        let normalized = if path_buf.exists() {
            std::fs::canonicalize(path_buf).map_err(|e| {
                ConnectorError::ConfigurationError(format!("invalid path '{path}': {e}"))
            })?
        } else {
            // For new tables, the path might not exist yet.
            // Use absolute path if possible.
            if path_buf.is_absolute() {
                path_buf.to_path_buf()
            } else {
                std::env::current_dir()
                    .map_err(|e| {
                        ConnectorError::ConfigurationError(format!("cannot get current dir: {e}"))
                    })?
                    .join(path_buf)
            }
        };

        Url::from_directory_path(&normalized).map_err(|()| {
            ConnectorError::ConfigurationError(format!(
                "cannot convert path to URL: {}",
                normalized.display()
            ))
        })
    }
}

/// Opens an existing Delta Lake table or creates a new one.
///
/// # Arguments
///
/// * `table_path` - Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`)
/// * `storage_options` - Storage credentials and configuration
/// * `schema` - Optional Arrow schema for table creation (required if table doesn't exist)
///
/// # Returns
///
/// The opened `DeltaTable` handle.
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if the table cannot be opened or created.
#[cfg(feature = "delta-lake")]
#[allow(clippy::implicit_hasher)]
pub async fn open_or_create_table(
    table_path: &str,
    storage_options: HashMap<String, String>,
    schema: Option<&SchemaRef>,
) -> Result<DeltaTable, ConnectorError> {
    info!(table_path, "opening Delta Lake table");

    let url = path_to_url(table_path)?;

    // Try to open or initialize the table.
    let table = DeltaTable::try_from_url_with_storage_options(url.clone(), storage_options.clone())
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("failed to open table: {e}")))?;

    // Check if the table is initialized (has state).
    if table.version().is_some() {
        info!(
            table_path,
            version = table.version(),
            "opened existing Delta Lake table"
        );
        return Ok(table);
    }

    // Table doesn't exist - create it if we have a schema.
    let schema = schema.ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "cannot create Delta Lake table without schema - \
             write at least one batch first"
                .into(),
        )
    })?;

    info!(table_path, "creating new Delta Lake table");

    // Convert Arrow schema to Delta Lake schema using TryIntoKernel.
    let delta_schema: deltalake::kernel::StructType = schema
        .as_ref()
        .try_into_kernel()
        .map_err(|e| ConnectorError::SchemaMismatch(format!("schema conversion failed: {e}")))?;

    // Create the table.
    let table = table
        .create()
        .with_columns(delta_schema.fields().cloned())
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("failed to create table: {e}")))?;

    info!(
        table_path,
        version = table.version(),
        "created new Delta Lake table"
    );

    Ok(table)
}

/// Writes batches to a Delta Lake table with exactly-once semantics.
///
/// # Arguments
///
/// * `table` - The Delta Lake table handle (consumed and returned)
/// * `batches` - Record batches to write
/// * `writer_id` - Unique writer identifier for exactly-once deduplication
/// * `epoch` - The epoch number for this write (stored in txn metadata)
/// * `save_mode` - Delta Lake save mode (Append, Overwrite, etc.)
/// * `partition_columns` - Optional partition column names
///
/// # Returns
///
/// A tuple of (updated table handle, new Delta version).
///
/// # Errors
///
/// Returns `ConnectorError::WriteError` if the write fails.
#[cfg(feature = "delta-lake")]
pub async fn write_batches(
    table: DeltaTable,
    batches: Vec<RecordBatch>,
    writer_id: &str,
    epoch: u64,
    save_mode: SaveMode,
    partition_columns: Option<Vec<String>>,
) -> Result<(DeltaTable, i64), ConnectorError> {
    if batches.is_empty() {
        debug!("no batches to write, skipping");
        let version = table.version().unwrap_or(0);
        return Ok((table, version));
    }

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

    debug!(
        writer_id,
        epoch,
        total_rows,
        num_batches = batches.len(),
        "writing batches to Delta Lake"
    );

    // Build the write operation with transaction metadata for exactly-once.
    // Note: Delta Lake uses i64 for epoch, but our API uses u64. This is safe
    // as epochs won't exceed i64::MAX in practice.
    #[allow(clippy::cast_possible_wrap)]
    let epoch_i64 = epoch as i64;

    let mut write_builder = table
        .write(batches)
        .with_save_mode(save_mode)
        .with_commit_properties(
            CommitProperties::default()
                .with_application_transaction(Transaction::new(writer_id, epoch_i64)),
        );

    // Add partition columns if specified.
    if let Some(cols) = partition_columns {
        if !cols.is_empty() {
            write_builder = write_builder.with_partition_columns(cols);
        }
    }

    // Execute the write.
    let table = write_builder
        .await
        .map_err(|e| ConnectorError::WriteError(format!("Delta Lake write failed: {e}")))?;

    let version = table.version().unwrap_or(0);

    info!(
        writer_id,
        epoch, version, total_rows, "committed Delta Lake transaction"
    );

    Ok((table, version))
}

/// Retrieves the last committed epoch for a writer from Delta Lake's txn metadata.
///
/// This is used for exactly-once recovery: on startup, we check what epoch was
/// last committed and skip any epochs <= that value.
///
/// # Arguments
///
/// * `table` - The Delta Lake table handle
/// * `writer_id` - The writer identifier to look up
///
/// # Returns
///
/// The last committed epoch for this writer, or 0 if no commits found.
#[cfg(feature = "delta-lake")]
pub async fn get_last_committed_epoch(table: &DeltaTable, writer_id: &str) -> u64 {
    // Query the table's application transaction version.
    let Ok(snapshot) = table.snapshot() else {
        debug!(writer_id, "no snapshot available, assuming epoch 0");
        return 0;
    };

    match snapshot
        .transaction_version(&table.log_store(), writer_id)
        .await
    {
        Ok(Some(version)) => {
            // Note: Delta Lake uses i64 for version, but our epoch is u64.
            // Versions are always non-negative, so this is safe.
            #[allow(clippy::cast_sign_loss)]
            let epoch = version as u64;
            debug!(
                writer_id,
                epoch, "found last committed epoch from txn metadata"
            );
            epoch
        }
        Ok(None) => {
            debug!(
                writer_id,
                "no txn metadata found for writer, assuming epoch 0"
            );
            0
        }
        Err(e) => {
            warn!(writer_id, error = %e, "failed to read txn metadata, assuming epoch 0");
            0
        }
    }
}

/// Extracts the Arrow schema from a Delta Lake table.
///
/// # Arguments
///
/// * `table` - The Delta Lake table handle
///
/// # Returns
///
/// The table's Arrow schema.
///
/// # Errors
///
/// Returns `ConnectorError::SchemaMismatch` if schema extraction fails.
#[cfg(feature = "delta-lake")]
pub fn get_table_schema(table: &DeltaTable) -> Result<SchemaRef, ConnectorError> {
    let state = table
        .snapshot()
        .map_err(|e| ConnectorError::SchemaMismatch(format!("table has no snapshot: {e}")))?;

    // Use the pre-computed Arrow schema from the EagerSnapshot.
    Ok(state.snapshot().arrow_schema())
}

/// Returns the latest committed version of a Delta Lake table.
///
/// This refreshes the table state from storage before checking.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the table state cannot be refreshed.
#[cfg(feature = "delta-lake")]
pub async fn get_latest_version(table: &mut DeltaTable) -> Result<i64, ConnectorError> {
    // DeltaTable::update() takes ownership, so clone and replace.
    let (updated, _metrics) = table
        .clone()
        .update()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to refresh Delta table: {e}")))?;

    *table = updated;
    Ok(table.version().unwrap_or(0))
}

/// Reads record batches from a specific Delta Lake table version.
///
/// Loads the requested version, then executes a full scan to collect
/// all record batches using a table provider registered with `DataFusion`.
///
/// # Arguments
///
/// * `table` - Mutable reference to the Delta Lake table handle
/// * `version` - The table version to read
/// * `max_records` - Hint for maximum records to return (best-effort)
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the version cannot be loaded or scanned.
#[cfg(feature = "delta-lake")]
pub async fn read_batches_at_version(
    table: &mut DeltaTable,
    version: i64,
    _max_records: usize,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    use datafusion::prelude::SessionContext;

    // Load the specific version.
    table
        .load_version(version)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to load version {version}: {e}")))?;

    debug!(version, "Delta Lake: loaded version for reading");

    // Build a DeltaTableProvider via the builder and register it with DataFusion.
    let provider = table
        .table_provider()
        .build()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to build table provider: {e}")))?;

    let ctx = SessionContext::new();
    ctx.register_table("delta_source_scan", Arc::new(provider))
        .map_err(|e| ConnectorError::ReadError(format!("failed to register scan table: {e}")))?;

    let df = ctx
        .sql("SELECT * FROM delta_source_scan")
        .await
        .map_err(|e| ConnectorError::ReadError(format!("scan query failed: {e}")))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("scan execution failed: {e}")))?;

    debug!(
        version,
        num_batches = batches.len(),
        total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        "Delta Lake: scanned version"
    );

    Ok(batches)
}

// ============================================================================
// Integration tests (require delta-lake feature)
// ============================================================================

#[cfg(all(test, feature = "delta-lake"))]
mod tests {
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
    async fn test_open_nonexistent_without_schema_fails() {
        let temp_dir = TempDir::new().unwrap();
        // Create the directory so the path exists, but it's not a Delta table.
        let nonexistent_table = temp_dir.path().join("nonexistent");
        std::fs::create_dir_all(&nonexistent_table).unwrap();
        let table_path = nonexistent_table.to_str().unwrap();

        // Open without schema when table doesn't exist should fail.
        let result = open_or_create_table(table_path, HashMap::new(), None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("schema"), "error should mention schema: {err}");
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
        let (table, version) =
            write_batches(table, vec![batch], "test-writer", 1, SaveMode::Append, None)
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
            let result =
                write_batches(table, vec![batch], writer_id, epoch, SaveMode::Append, None)
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
        let (table, version) =
            write_batches(table, vec![], "test-writer", 1, SaveMode::Append, None)
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
        let (returned_table, version) =
            write_batches(table, vec![batch], "writer", 1, SaveMode::Append, None)
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
        let (table, _) =
            write_batches(table, vec![batch], "writer", 1, SaveMode::Append, None)
                .await
                .unwrap();

        // Write 30 more rows at version 2.
        let batch = test_batch(30);
        let (_table, _) =
            write_batches(table, vec![batch], "writer", 2, SaveMode::Append, None)
                .await
                .unwrap();

        // Read version 1 — should get 50 rows.
        let mut read_table = open_or_create_table(table_path, HashMap::new(), None)
            .await
            .unwrap();
        let batches = read_batches_at_version(&mut read_table, 1, 10000).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 50);

        // Read version 2 — should get 80 rows (cumulative).
        let batches = read_batches_at_version(&mut read_table, 2, 10000).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
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
        let mut source = DeltaSource::new(source_config);
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
        use crate::checkpoint::SourceCheckpoint;
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
        )
        .await
        .unwrap();

        // Open source starting from version 0, read version 1.
        let mut source_config = DeltaSourceConfig::new(table_path);
        source_config.starting_version = Some(0);
        let mut source = DeltaSource::new(source_config.clone());
        let connector_config = ConnectorConfig::new("delta-lake");
        source.open(&connector_config).await.unwrap();

        // Poll to consume version 1.
        let _ = source.poll_batch(10000).await.unwrap();
        // Drain buffered.
        while let Ok(Some(_)) = source.poll_batch(10000).await {}

        // Checkpoint.
        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("delta_version"), Some("1"));
        source.close().await.unwrap();

        // Restore from checkpoint — should resume at version 1.
        let mut source2 = DeltaSource::new(source_config);
        source2.open(&connector_config).await.unwrap();
        source2.restore(&cp).await.unwrap();

        assert_eq!(source2.current_version(), 1);

        // Next poll should get version 2.
        let result = source2.poll_batch(10000).await.unwrap();
        assert!(result.is_some());
        let mut total = result.unwrap().records.num_rows();
        while let Ok(Some(batch)) = source2.poll_batch(10000).await {
            total += batch.records.num_rows();
        }
        // Version 2 has cumulative 30 rows (10 + 20).
        assert_eq!(total, 30);

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

        let batches = read_batches_at_version(&mut table, latest, 10000)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 25, "all 25 rows should be written, not dropped by auto-flush");
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
        let batches = read_batches_at_version(&mut read_table, 2, 10000)
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 25);
    }
}
