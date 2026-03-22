//! Incremental snapshot diff for Iceberg tables.
//!
//! Reads only data files added since a previous snapshot by walking the
//! manifest tree and filtering on `ManifestFile.added_snapshot_id`.
#![cfg(feature = "iceberg")]

use arrow_array::RecordBatch;
use iceberg::spec::ManifestStatus;
use iceberg::table::Table;
use tracing::{debug, warn};

use crate::error::ConnectorError;

/// Reads only data files added between `old_snapshot_id` and `new_snapshot_id`.
///
/// Walks the new snapshot's manifest list, selects manifests where
/// `added_snapshot_id > old_snapshot_id`, loads those manifests, and reads
/// the `ADDED` data files via the table's scan API.
///
/// If any manifest entry has a `Deleted` status (compaction, overwrite, or
/// explicit delete), the incremental path is unsafe — falls back to a full
/// scan of the new snapshot so the caller gets the complete table state.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` on I/O or scan failure.
pub async fn scan_incremental(
    table: &Table,
    old_snapshot_id: i64,
    new_snapshot_id: i64,
    select_columns: &[String],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let metadata = table.metadata();
    let file_io = table.file_io();

    let snapshot = metadata.snapshot_by_id(new_snapshot_id).ok_or_else(|| {
        ConnectorError::ReadError(format!("snapshot {new_snapshot_id} not found"))
    })?;

    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("load manifest list: {e}")))?;

    let mut new_file_paths = Vec::new();

    for manifest_file in manifest_list.entries() {
        if manifest_file.added_snapshot_id <= old_snapshot_id {
            continue;
        }

        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("load manifest: {e}")))?;

        for entry in manifest.entries() {
            match entry.status {
                ManifestStatus::Added => {
                    new_file_paths.push(entry.file_path().to_string());
                }
                ManifestStatus::Deleted => {
                    // Non-append snapshot (compaction, overwrite, or delete).
                    // Incremental diff cannot represent deletions — fall back
                    // to a full scan so the caller gets the complete state.
                    warn!(
                        old_snapshot_id,
                        new_snapshot_id,
                        deleted_file = entry.file_path(),
                        "incremental scan: detected deleted entry, falling back to full scan"
                    );
                    return super::iceberg_io::scan_table(
                        table,
                        Some(new_snapshot_id),
                        select_columns,
                    )
                    .await;
                }
                ManifestStatus::Existing => {}
            }
        }
    }

    if new_file_paths.is_empty() {
        debug!(
            old_snapshot_id,
            new_snapshot_id, "incremental scan: no new data files"
        );
        return Ok(Vec::new());
    }

    debug!(
        old_snapshot_id,
        new_snapshot_id,
        files = new_file_paths.len(),
        "incremental scan: reading new data files"
    );

    read_files_via_scan(table, new_snapshot_id, select_columns, &new_file_paths).await
}

/// Scans the snapshot but only reads files whose paths are in `file_paths`.
async fn read_files_via_scan(
    table: &Table,
    snapshot_id: i64,
    select_columns: &[String],
    file_paths: &[String],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    use tokio_stream::StreamExt;

    let mut scan_builder = table.scan().snapshot_id(snapshot_id);

    if select_columns.is_empty() {
        scan_builder = scan_builder.select_all();
    } else {
        scan_builder = scan_builder.select(select_columns.iter().map(String::as_str));
    }

    let scan = scan_builder
        .build()
        .map_err(|e| ConnectorError::ReadError(format!("build scan: {e}")))?;

    // plan_files returns FileScanTasks — filter to only our target paths.
    let file_tasks = scan
        .plan_files()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("plan files: {e}")))?;

    let mut file_tasks = std::pin::pin!(file_tasks);
    let mut target_tasks = Vec::new();
    while let Some(task_result) = file_tasks.next().await {
        let task = task_result.map_err(|e| ConnectorError::ReadError(format!("file task: {e}")))?;
        if file_paths.iter().any(|p| task.data_file_path() == p) {
            target_tasks.push(task);
        }
    }

    if target_tasks.is_empty() {
        return Ok(Vec::new());
    }

    // Read the filtered tasks via ArrowReader.
    let reader = table.reader_builder().with_batch_size(8192).build();

    let batch_stream = reader
        .read(Box::pin(tokio_stream::iter(
            target_tasks.into_iter().map(Ok),
        )))
        .map_err(|e| ConnectorError::ReadError(format!("build reader: {e}")))?;

    let mut batch_stream = std::pin::pin!(batch_stream);
    let mut batches = Vec::new();
    while let Some(result) = batch_stream.next().await {
        let batch = result.map_err(|e| ConnectorError::ReadError(format!("read batch: {e}")))?;
        batches.push(batch);
    }

    Ok(batches)
}
