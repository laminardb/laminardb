//! Changelog MERGE execution and result accounting.

use super::{debug, info, ConnectorError, DeltaTable, DeltaWriteAttemptError, RecordBatch};

/// Result of a MERGE (upsert) operation.
#[cfg(feature = "delta-lake")]
#[derive(Debug)]
pub struct MergeResult {
    /// Number of rows inserted.
    pub rows_inserted: usize,
    /// Number of rows updated.
    pub rows_updated: usize,
    /// Number of rows deleted.
    pub rows_deleted: usize,
}

/// Atomic changelog MERGE: inserts, updates, and deletes in one Delta commit.
///
/// The source batch must contain an `_op` column (Utf8) with values:
/// - `"I"`, `"U"`, `"r"` → upsert (update if matched, insert if not)
/// - `"D"` → delete matched rows
///
/// Columns prefixed with `_` are excluded from SET clauses but remain
/// in the source `DataFrame` for predicate filtering.
///
/// # Errors
///
/// Returns `ConnectorError::WriteError` if the merge fails.
#[cfg(feature = "delta-lake")]
#[allow(clippy::too_many_lines)]
pub(crate) async fn merge_changelog(
    table: DeltaTable,
    source_batch: RecordBatch,
    key_columns: &[String],
    schema_evolution: bool,
    writer_properties: Option<deltalake::parquet::file::properties::WriterProperties>,
    ctx: &datafusion::prelude::SessionContext,
) -> Result<(DeltaTable, MergeResult), DeltaWriteAttemptError> {
    use datafusion::prelude::*;
    use deltalake::kernel::transaction::CommitProperties;

    const CDC_COLUMNS: &[&str] = &["_op", "_ts_ms"];

    if source_batch.num_rows() == 0 {
        return Ok((
            table,
            MergeResult {
                rows_inserted: 0,
                rows_updated: 0,
                rows_deleted: 0,
            },
        ));
    }

    debug!(
        key_columns = ?key_columns,
        source_rows = source_batch.num_rows(),
        "performing atomic changelog MERGE"
    );

    let source_df = ctx.read_batch(source_batch).map_err(|e| {
        ConnectorError::WriteError(format!("failed to create source DataFrame: {e}"))
    })?;

    // Join predicate: target.k1 = source.k1 AND ...
    let predicate = key_columns
        .iter()
        .map(|k| col(format!("target.{k}")).eq(col(format!("source.{k}"))))
        .reduce(Expr::and)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError("merge requires at least one key column".into())
        })?;

    let source_schema = source_df.schema().clone();
    let key_set: std::collections::HashSet<&str> = key_columns.iter().map(String::as_str).collect();

    // Exclude CDC metadata columns from SET clauses (preserve user columns like _id).
    let all_user_columns: Vec<String> = source_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|name| !CDC_COLUMNS.contains(&name.as_str()))
        .collect();

    let non_key_user_columns: Vec<String> = all_user_columns
        .iter()
        .filter(|c| !key_set.contains(c.as_str()))
        .cloned()
        .collect();

    // Predicates for conditional clause execution.
    let upsert_pred = col("source._op").in_list(vec![lit("I"), lit("U"), lit("r")], false);
    let delete_pred = col("source._op").eq(lit("D"));

    let non_key_for_update = non_key_user_columns;
    let all_for_insert = all_user_columns;

    let mut merge_builder = table
        .merge(source_df, predicate)
        .with_source_alias("source")
        .with_target_alias("target")
        .with_commit_properties(CommitProperties::default())
        .when_matched_update(|update| {
            let mut u = update.predicate(upsert_pred.clone());
            for col_name in &non_key_for_update {
                u = u.update(col_name.as_str(), col(format!("source.{col_name}")));
            }
            u
        })
        .map_err(|e| ConnectorError::WriteError(format!("merge matched-update failed: {e}")))?
        .when_matched_delete(|delete| delete.predicate(delete_pred))
        .map_err(|e| ConnectorError::WriteError(format!("merge matched-delete failed: {e}")))?
        .when_not_matched_insert(|insert| {
            let mut ins = insert.predicate(upsert_pred);
            for col_name in &all_for_insert {
                ins = ins.set(col_name.as_str(), col(format!("source.{col_name}")));
            }
            ins
        })
        .map_err(|e| ConnectorError::WriteError(format!("merge not-matched-insert failed: {e}")))?;

    if schema_evolution {
        merge_builder = merge_builder.with_merge_schema(true);
    }

    if let Some(props) = writer_properties {
        merge_builder = merge_builder.with_writer_properties(props);
    }

    let (table, metrics) = merge_builder.await.map_err(DeltaWriteAttemptError::Delta)?;

    let result = MergeResult {
        rows_inserted: metrics.num_target_rows_inserted,
        rows_updated: metrics.num_target_rows_updated,
        rows_deleted: metrics.num_target_rows_deleted,
    };

    info!(
        rows_inserted = result.rows_inserted,
        rows_updated = result.rows_updated,
        rows_deleted = result.rows_deleted,
        "Delta Lake changelog MERGE complete"
    );

    Ok((table, result))
}
