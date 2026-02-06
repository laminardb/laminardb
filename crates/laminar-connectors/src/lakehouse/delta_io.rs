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
}
