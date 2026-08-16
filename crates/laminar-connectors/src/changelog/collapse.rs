//! Collapse a changelog epoch batch into a cardinality-safe, key-unique upsert
//! batch. See the module docs for why this is necessary.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{DataType, Field, Schema};

use laminar_core::changelog::WEIGHT_COLUMN;

use crate::error::ConnectorError;

/// Collapse a concatenated changelog epoch `batch` into a key-unique batch
/// carrying a `_op` column of `U` (upsert) or `D` (delete), one row per
/// `merge_key`.
///
/// Two input encodings are detected automatically:
///
/// - **Z-set** (the `__weight` column is present): identical full rows are
///   consolidated by summing their weights, net-zero rows are dropped, then the
///   survivors are grouped by `merge_key`. A key with a net-positive (live) row
///   becomes a `U` carrying that value; a key with only net-negative rows
///   becomes a `D`. The `__weight` column is stripped from the output.
/// - **CDC** (no `__weight`): the last-arriving row per `merge_key` wins (row
///   order is arrival order). Its op is normalized to `D` for deletes
///   (`_op ∈ {D, U-}`) and `U` for everything else. A batch with neither column
///   is treated as all-upsert.
///
/// The output reuses the existing key-by-key MERGE (`_op ∈ {U, D}`) unchanged,
/// and contains at most one row per merge key, so the writer never sees a
/// cardinality violation.
///
/// # Errors
///
/// - [`ConnectorError::ConfigurationError`] if `merge_key` is empty, names a
///   column absent from the batch, or is not unique over the collapsed output
///   (more than one live row for a single key — a misdeclared merge key).
/// - [`ConnectorError::Internal`] if an Arrow row-conversion or take fails, or
///   the `__weight` column is not Int64.
pub fn collapse_changelog(
    batch: &RecordBatch,
    merge_key: &[String],
) -> Result<RecordBatch, ConnectorError> {
    if merge_key.is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "changelog collapse requires at least one merge key column".into(),
        ));
    }
    let schema = batch.schema();
    for k in merge_key {
        if is_metadata_column(k) {
            return Err(ConnectorError::ConfigurationError(format!(
                "merge key column '{k}' is reserved changelog metadata and cannot be a merge key"
            )));
        }
        if schema.index_of(k).is_err() {
            return Err(ConnectorError::ConfigurationError(format!(
                "merge key column '{k}' is not present in the changelog output schema"
            )));
        }
    }

    if let Ok(weight_idx) = schema.index_of(WEIGHT_COLUMN) {
        collapse_zset(batch, merge_key, weight_idx)
    } else {
        collapse_cdc(batch, merge_key)
    }
}

/// Build comparable [`Rows`] over the given column indices of `batch`.
fn rows_over(batch: &RecordBatch, indices: &[usize]) -> Result<Rows, ConnectorError> {
    let schema = batch.schema();
    let fields: Vec<SortField> = indices
        .iter()
        .map(|&i| SortField::new(schema.field(i).data_type().clone()))
        .collect();
    let arrays: Vec<ArrayRef> = indices.iter().map(|&i| batch.column(i).clone()).collect();
    let converter = RowConverter::new(fields)
        .map_err(|e| ConnectorError::Internal(format!("row converter: {e}")))?;
    converter
        .convert_columns(&arrays)
        .map_err(|e| ConnectorError::Internal(format!("convert columns to rows: {e}")))
}

/// Column indices for the named `columns` (which the caller has validated exist).
fn index_of_all(batch: &RecordBatch, columns: &[String]) -> Vec<usize> {
    let schema = batch.schema();
    columns
        .iter()
        .map(|name| schema.index_of(name).expect("merge key columns validated"))
        .collect()
}

/// Changelog metadata columns, excluded from the collapsed output's user
/// columns (`_op` is re-emitted normalized; `__weight`/`_ts_ms` are dropped).
fn is_metadata_column(name: &str) -> bool {
    name == "_op" || name == "_ts_ms" || name == WEIGHT_COLUMN
}

/// Z-set collapse: consolidate by full row, then pick one row per merge key.
fn collapse_zset(
    batch: &RecordBatch,
    merge_key: &[String],
    weight_idx: usize,
) -> Result<RecordBatch, ConnectorError> {
    let num_rows = batch.num_rows();
    let weights = batch
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ConnectorError::Internal(format!("{WEIGHT_COLUMN} column is not Int64")))?;

    // User columns = every column except changelog metadata.
    let schema = batch.schema();
    let user_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| !is_metadata_column(schema.field(i).name()))
        .collect();

    // 1. Consolidate identical full rows, summing weights; keep net-nonzero.
    //    `survivors` holds (representative row index, net weight).
    let full_rows = rows_over(batch, &user_indices)?;
    let mut order: Vec<usize> = (0..num_rows).collect();
    order.sort_unstable_by(|&a, &b| full_rows.row(a).cmp(&full_rows.row(b)));

    let mut survivors: Vec<(usize, i64)> = Vec::new();
    let mut i = 0;
    while i < order.len() {
        let rep = order[i];
        let rep_row = full_rows.row(rep);
        let mut sum = 0i64;
        let mut j = i;
        while j < order.len() && full_rows.row(order[j]) == rep_row {
            sum += weights.value(order[j]);
            j += 1;
        }
        if sum != 0 {
            survivors.push((rep, sum));
        }
        i = j;
    }

    if survivors.is_empty() {
        return build_output(batch, &user_indices, &[], &[]);
    }

    // 2. Group survivors by merge key; one output row per key.
    let key_rows = rows_over(batch, &index_of_all(batch, merge_key))?;
    survivors
        .sort_unstable_by(|&(a, _), &(b, _)| key_rows.row(a).cmp(&key_rows.row(b)).then(a.cmp(&b)));

    let mut selected: Vec<usize> = Vec::new();
    let mut ops: Vec<&str> = Vec::new();
    let mut g = 0;
    while g < survivors.len() {
        let key = key_rows.row(survivors[g].0);
        let mut live: Option<usize> = None;
        let mut live_count = 0usize;
        let mut first_negative: Option<usize> = None;
        let mut h = g;
        while h < survivors.len() && key_rows.row(survivors[h].0) == key {
            let (idx, weight) = survivors[h];
            if weight > 0 {
                live_count += 1;
                if live.is_none() {
                    live = Some(idx);
                }
            } else if first_negative.is_none() {
                first_negative = Some(idx);
            }
            h += 1;
        }
        if live_count > 1 {
            return Err(ConnectorError::ConfigurationError(format!(
                "changelog collapse: merge.key.columns {merge_key:?} is not unique — {live_count} \
                 distinct live rows share one key in a single epoch; declare a merge key that is \
                 unique over the materialized-view output"
            )));
        }
        if let Some(idx) = live {
            selected.push(idx);
            ops.push("U");
        } else if let Some(idx) = first_negative {
            selected.push(idx);
            ops.push("D");
        }
        g = h;
    }

    build_output(batch, &user_indices, &selected, &ops)
}

/// CDC collapse: last-arriving row per merge key wins; op normalized to U/D.
fn collapse_cdc(batch: &RecordBatch, merge_key: &[String]) -> Result<RecordBatch, ConnectorError> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let op_values = match schema.index_of("_op") {
        Ok(idx) => Some(
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ConnectorError::Internal("_op column is not Utf8".into()))?,
        ),
        Err(_) => None,
    };

    // Keep the highest (last-arriving) row index per merge key.
    let key_rows = rows_over(batch, &index_of_all(batch, merge_key))?;
    let mut order: Vec<usize> = (0..num_rows).collect();
    order.sort_unstable_by(|&a, &b| key_rows.row(a).cmp(&key_rows.row(b)).then(a.cmp(&b)));

    let mut selected: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < order.len() {
        let key = key_rows.row(order[i]);
        let mut last = order[i];
        let mut j = i;
        while j < order.len() && key_rows.row(order[j]) == key {
            last = order[j]; // order is index-ascending within a key group
            j += 1;
        }
        selected.push(last);
        i = j;
    }
    // Deterministic output order (correctness is unaffected — keys are unique).
    selected.sort_unstable();

    // Normalize to {U, D}: a delete iff the surviving op is D or U- (a before
    // image); otherwise the row is the current image for its key.
    let ops: Vec<&str> = selected
        .iter()
        .map(|&idx| match op_values {
            Some(values) if !values.is_null(idx) => {
                if matches!(values.value(idx), "D" | "U-") {
                    "D"
                } else {
                    "U"
                }
            }
            _ => "U",
        })
        .collect();

    // User columns = every column except changelog metadata.
    let user_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| !is_metadata_column(schema.field(i).name()))
        .collect();

    build_output(batch, &user_indices, &selected, &ops)
}

/// Take `user_indices` columns at the `selected` rows of `batch` and append a
/// fresh `_op` column built from `ops`. `selected` and `ops` must be parallel.
fn build_output(
    batch: &RecordBatch,
    user_indices: &[usize],
    selected: &[usize],
    ops: &[&str],
) -> Result<RecordBatch, ConnectorError> {
    debug_assert_eq!(selected.len(), ops.len());
    let schema = batch.schema();
    // Row counts are bounded by the epoch buffer cap, well under u32::MAX.
    #[allow(clippy::cast_possible_truncation)]
    let take_idx = UInt32Array::from(selected.iter().map(|&i| i as u32).collect::<Vec<_>>());

    let mut fields: Vec<Field> = Vec::with_capacity(user_indices.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(user_indices.len() + 1);
    for &idx in user_indices {
        let taken = arrow_select::take::take(batch.column(idx), &take_idx, None)
            .map_err(|e| ConnectorError::Internal(format!("take column: {e}")))?;
        fields.push(schema.field(idx).as_ref().clone());
        columns.push(taken);
    }
    fields.push(Field::new("_op", DataType::Utf8, false));
    columns.push(Arc::new(StringArray::from(ops.to_vec())));

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| ConnectorError::Internal(format!("build collapsed batch: {e}")))
}

#[cfg(test)]
#[allow(clippy::too_many_lines)]
mod tests;
