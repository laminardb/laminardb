//! Bounded snapshot and change-data-feed reads.

use super::{
    debug, from_delta_version, to_delta_version, Arc, ConnectorError, DeltaTable, RecordBatch,
    SchemaRef,
};

/// Returns the table's partition columns, or an empty list if the snapshot is
/// unavailable. Best-effort: used for clustering diagnostics, never for
/// correctness, so a missing snapshot is not an error.
#[cfg(feature = "delta-lake")]
#[must_use]
pub fn get_partition_columns(table: &DeltaTable) -> Vec<String> {
    match table.snapshot() {
        Ok(snapshot) => snapshot.snapshot().metadata().partition_columns().to_vec(),
        Err(_) => Vec::new(),
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

/// Returns the latest committed version via the log store.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` on failure.
#[cfg(feature = "delta-lake")]
pub async fn get_latest_version(table: &mut DeltaTable) -> Result<i64, ConnectorError> {
    let log_store = table.log_store();
    let current = table.version().unwrap_or(0);
    let version = log_store
        .get_latest_version(current)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to get latest version: {e}")))?;
    from_delta_version(version)
}

/// Reads record batches from a specific Delta Lake table version.
///
/// Loads the requested version, applies a `LIMIT` to bound memory usage,
/// then streams results via `execute_stream` to avoid materializing the
/// entire version in memory.
///
/// # Arguments
///
/// * `table` - Mutable reference to the Delta Lake table handle
/// * `version` - The table version to read
/// * `max_records` - Maximum number of records to return. Pass `usize::MAX`
///   to read all records (unbounded).
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the version cannot be loaded or scanned.
///
/// Returns `(batches, fully_consumed)` — `fully_consumed` is `false` when
/// `max_records` truncated the result and more rows remain.
#[cfg(feature = "delta-lake")]
pub(crate) async fn read_batches_at_version(
    table: &mut DeltaTable,
    version: i64,
    max_records: usize,
) -> Result<(Vec<RecordBatch>, bool), ConnectorError> {
    use datafusion::prelude::SessionContext;
    use tokio_stream::StreamExt;

    // Load the specific version.
    let delta_version = to_delta_version(version)?;
    table
        .load_version(delta_version)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to load version {version}: {e}")))?;

    debug!(version, "Delta Lake: loaded version for reading");

    // Build a DeltaTableProvider via the builder and register it with DataFusion.
    let provider =
        table.table_provider().build().await.map_err(|e| {
            ConnectorError::ReadError(format!("failed to build table provider: {e}"))
        })?;

    let ctx = SessionContext::new();
    ctx.register_table("delta_source_scan", Arc::new(provider))
        .map_err(|e| ConnectorError::ReadError(format!("failed to register scan table: {e}")))?;

    // Apply LIMIT to bound memory: prevents OOM on large versions.
    let df = ctx
        .sql("SELECT * FROM delta_source_scan")
        .await
        .map_err(|e| ConnectorError::ReadError(format!("scan query failed: {e}")))?;

    let df = if max_records < usize::MAX {
        df.limit(0, Some(max_records))
            .map_err(|e| ConnectorError::ReadError(format!("limit failed: {e}")))?
    } else {
        df
    };

    // Stream results instead of collect() to avoid materializing everything.
    let mut stream = df
        .execute_stream()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("stream execution failed: {e}")))?;

    let mut batches = Vec::new();
    let mut total_rows: usize = 0;

    while let Some(result) = stream.next().await {
        let batch =
            result.map_err(|e| ConnectorError::ReadError(format!("stream batch failed: {e}")))?;
        if batch.num_rows() == 0 {
            continue;
        }
        total_rows += batch.num_rows();
        batches.push(batch);

        // Respect max_records even between DataFusion batches.
        if total_rows >= max_records {
            break;
        }
    }

    // If we stopped due to max_records, probe whether the stream has more.
    // Without this, a version with exactly max_records rows would be
    // misclassified as truncated and re-read forever.
    let fully_consumed = if total_rows >= max_records {
        stream.next().await.is_none()
    } else {
        true
    };

    debug!(
        version,
        num_batches = batches.len(),
        total_rows,
        fully_consumed,
        "Delta Lake: scanned version"
    );

    Ok((batches, fully_consumed))
}

/// Reads CDF batches for a version range via `scan_cdf()`.
///
/// `scan_cdf(self)` consumes the supplied `DeltaTable`; callers retaining a
/// table handle can pass a clone.
/// Output includes `_change_type`, `_commit_version`, `_commit_timestamp`.
///
/// # Errors
///
/// Returns a read error on scan failure or a non-transient configuration error
/// when the collected version range exceeds either hard limit.
#[cfg(feature = "delta-lake")]
pub async fn read_cdf_batches(
    table: DeltaTable,
    start_version: i64,
    end_version: i64,
    max_rows: usize,
    max_bytes: usize,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    use datafusion::prelude::SessionContext;
    use tokio_stream::StreamExt;

    debug!(start_version, end_version, "reading CDF batches");

    let ctx = SessionContext::new();

    // Clone session state so the RwLockReadGuard is dropped before await.
    let session_state = ctx.state();

    let start_delta_version = to_delta_version(start_version)?;
    let end_delta_version = to_delta_version(end_version)?;
    let cdf_builder = table
        .scan_cdf()
        .with_starting_version(start_delta_version)
        .with_ending_version(end_delta_version);

    let plan = cdf_builder
        .build(&session_state, None)
        .await
        .map_err(map_cdf_scan_build_error)?;

    // Execute the plan via DataFusion to get record batches.
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx)
        .map_err(|e| ConnectorError::ReadError(format!("CDF stream execution failed: {e}")))?;

    let mut batches = Vec::new();
    let mut rows = 0;
    let mut bytes = 0;
    while let Some(result) = stream.next().await {
        let batch: RecordBatch = result
            .map_err(|e| ConnectorError::ReadError(format!("CDF stream batch failed: {e}")))?;
        if batch.num_rows() > 0 {
            (rows, bytes) = checked_cdf_commit_usage(rows, bytes, &batch, max_rows, max_bytes)?;
            batches.push(batch);
        }
    }

    debug!(
        start_version,
        end_version,
        num_batches = batches.len(),
        rows,
        bytes,
        "CDF scan complete"
    );

    Ok(batches)
}

#[cfg(feature = "delta-lake")]
pub(super) fn map_cdf_scan_build_error(error: deltalake::DeltaTableError) -> ConnectorError {
    match error {
        error @ (deltalake::DeltaTableError::ChangeDataNotEnabled { .. }
        | deltalake::DeltaTableError::ChangeDataNotRecorded { .. }) => {
            ConnectorError::ConfigurationError(format!(
                "Delta CDF is unavailable for the requested history: {error}"
            ))
        }
        error => ConnectorError::ReadError(format!("CDF scan build failed: {error}")),
    }
}

#[cfg(feature = "delta-lake")]
pub(super) fn checked_cdf_commit_usage(
    rows: usize,
    bytes: usize,
    batch: &RecordBatch,
    max_rows: usize,
    max_bytes: usize,
) -> Result<(usize, usize), ConnectorError> {
    let rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
        ConnectorError::ConfigurationError("Delta CDF commit row count overflowed".into())
    })?;
    let bytes = bytes
        .checked_add(batch.get_array_memory_size())
        .ok_or_else(|| {
            ConnectorError::ConfigurationError("Delta CDF commit byte count overflowed".into())
        })?;
    if rows > max_rows {
        return Err(ConnectorError::ConfigurationError(format!(
            "Delta CDF commit exceeds the hard row limit: rows={rows}, limit={max_rows}"
        )));
    }
    if bytes > max_bytes {
        return Err(ConnectorError::ConfigurationError(format!(
            "Delta CDF commit exceeds the hard byte limit: bytes={bytes}, limit={max_bytes}"
        )));
    }
    Ok((rows, bytes))
}

/// Maps Delta CDF rows to the canonical signed-weight changelog and drops CDF
/// metadata columns (`_change_type`, `_commit_version`, `_commit_timestamp`).
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` when CDF metadata is missing or invalid,
/// or when the output batch cannot be built.
#[cfg(feature = "delta-lake")]
pub fn map_cdf_to_changelog(batch: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::{Array, Int64Array, StringArray};
    use laminar_core::changelog::WEIGHT_COLUMN;

    let schema = batch.schema();
    let ct_idx = schema
        .index_of("_change_type")
        .map_err(|_| ConnectorError::ReadError("CDF batch is missing _change_type".into()))?;
    if let Some(field) = schema.fields().iter().find(|field| {
        ["_op", "__op", WEIGHT_COLUMN]
            .iter()
            .any(|reserved| field.name().eq_ignore_ascii_case(reserved))
    }) {
        return Err(ConnectorError::ReadError(format!(
            "CDF batch contains reserved mutation column '{}'",
            field.name()
        )));
    }

    let change_type = batch
        .column(ct_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ConnectorError::ReadError("_change_type is not Utf8".into()))?;

    let mut weights = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if change_type.is_null(row) {
            return Err(ConnectorError::ReadError(format!(
                "CDF _change_type is null at row {row}"
            )));
        }
        weights.push(match change_type.value(row) {
            "insert" | "update_postimage" => 1,
            "delete" | "update_preimage" => -1,
            value => {
                return Err(ConnectorError::ReadError(format!(
                    "unknown CDF _change_type '{value}' at row {row}"
                )));
            }
        });
    }

    // Rebuild batch: keep user columns, drop CDF metadata, append __weight.
    let cdf_meta = ["_change_type", "_commit_version", "_commit_timestamp"];
    let mut fields = Vec::new();
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        if !cdf_meta.contains(&field.name().as_str()) {
            fields.push(field.clone());
            columns.push(batch.column(i).clone());
        }
    }
    fields.push(Arc::new(arrow_schema::Field::new(
        WEIGHT_COLUMN,
        arrow_schema::DataType::Int64,
        false,
    )));
    columns.push(Arc::new(Int64Array::from(weights)));

    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        )),
        columns,
    )
    .map_err(|e| ConnectorError::ReadError(format!("CDF batch rebuild: {e}")))
}
