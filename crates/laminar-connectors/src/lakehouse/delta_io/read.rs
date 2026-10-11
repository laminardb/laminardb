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

/// Streams the snapshot at `version` through the delta-rs table provider, which applies
/// deletion vectors and column mapping. Batches are produced on demand, so a caller that
/// consumes them one at a time never holds the whole snapshot.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the version cannot be loaded or planned.
#[cfg(feature = "delta-lake")]
pub(crate) async fn snapshot_stream(
    table: &mut DeltaTable,
    version: i64,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream, ConnectorError> {
    use datafusion::prelude::SessionContext;

    let delta_version = to_delta_version(version)?;
    table
        .load_version(delta_version)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to load version {version}: {e}")))?;
    debug!(version, "Delta Lake: loaded version for reading");

    let provider =
        table.table_provider().build().await.map_err(|e| {
            ConnectorError::ReadError(format!("failed to build table provider: {e}"))
        })?;
    let ctx = SessionContext::new();
    ctx.register_table("delta_source_scan", Arc::new(provider))
        .map_err(|e| ConnectorError::ReadError(format!("failed to register scan table: {e}")))?;
    ctx.sql("SELECT * FROM delta_source_scan")
        .await
        .map_err(|e| ConnectorError::ReadError(format!("scan query failed: {e}")))?
        .execute_stream()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("stream execution failed: {e}")))
}

/// Collects up to `max_records` rows of the snapshot at `version`.
#[cfg(all(feature = "delta-lake", test))]
pub(crate) async fn read_batches_at_version(
    table: &mut DeltaTable,
    version: i64,
    max_records: usize,
) -> Result<(Vec<RecordBatch>, bool), ConnectorError> {
    use tokio_stream::StreamExt;

    let mut stream = snapshot_stream(table, version).await?;
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
        if total_rows >= max_records {
            break;
        }
    }
    let fully_consumed = total_rows < max_records || stream.next().await.is_none();
    Ok((batches, fully_consumed))
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
