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
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};

    fn keys(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| (*s).to_string()).collect()
    }

    /// Build a Z-set changelog batch: schema [region: Utf8, total: Int64, __weight: Int64].
    fn zset_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("total", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
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

    /// Build a CDC changelog batch: schema [id: Int64, value: Float64, _op: Utf8].
    fn cdc_batch(rows: &[(i64, f64, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("_op", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn col_str(batch: &RecordBatch, name: &str) -> Vec<String> {
        let idx = batch.schema().index_of(name).unwrap();
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
    }

    fn col_i64(batch: &RecordBatch, name: &str) -> Vec<i64> {
        let idx = batch.schema().index_of(name).unwrap();
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    /// Sort output rows by (region/id key, op) so assertions are order-stable.
    fn sorted_pairs(
        regions: &[String],
        values: &[i64],
        ops: &[String],
    ) -> Vec<(String, i64, String)> {
        let mut v: Vec<_> = regions
            .iter()
            .zip(values)
            .zip(ops)
            .map(|((r, t), o)| (r.clone(), *t, o.clone()))
            .collect();
        v.sort();
        v
    }

    #[test]
    fn zset_multi_update_per_key_keeps_final_value() {
        // Two emit cycles concatenated: 10→20→35. The intermediate 20 cancels.
        let out = collapse_changelog(
            &zset_batch(&[
                ("east", 10, -1),
                ("east", 20, 1),
                ("east", 20, -1),
                ("east", 35, 1),
            ]),
            &keys(&["region"]),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(col_str(&out, "_op"), vec!["U"]);
        assert_eq!(col_i64(&out, "total"), vec![35]);
    }

    #[test]
    fn zset_insert_then_delete_within_epoch_is_noop() {
        // +1 then -1 on the same full row nets zero → dropped entirely.
        let out = collapse_changelog(
            &zset_batch(&[("east", 10, 1), ("east", 10, -1)]),
            &keys(&["region"]),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 0);
        assert!(out.schema().index_of(WEIGHT_COLUMN).is_err());
        assert!(out.schema().index_of("_op").is_ok());
    }

    #[test]
    fn zset_multiple_keys_mixed_ops() {
        // east updated, west dropped, north newly inserted — in one epoch.
        let out = collapse_changelog(
            &zset_batch(&[
                ("east", 10, -1),
                ("east", 30, 1),
                ("west", 5, -1),
                ("north", 99, 1),
            ]),
            &keys(&["region"]),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 3);
        let got = sorted_pairs(
            &col_str(&out, "region"),
            &col_i64(&out, "total"),
            &col_str(&out, "_op"),
        );
        assert_eq!(
            got,
            vec![
                ("east".into(), 30, "U".into()),
                ("north".into(), 99, "U".into()),
                ("west".into(), 5, "D".into()),
            ]
        );
    }

    #[test]
    fn zset_higher_multiplicity_is_single_live_row() {
        // Cascaded aggregation can emit weight > 1; still one live row per key.
        let out = collapse_changelog(&zset_batch(&[("east", 10, 3)]), &keys(&["region"])).unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(col_str(&out, "_op"), vec!["U"]);
    }

    #[test]
    fn zset_non_unique_merge_key_errors() {
        // Two distinct live rows for the same key → misdeclared merge key.
        let err = collapse_changelog(
            &zset_batch(&[("east", 10, 1), ("east", 20, 1)]),
            &keys(&["region"]),
        )
        .unwrap_err();
        assert!(
            matches!(err, ConnectorError::ConfigurationError(_)),
            "expected ConfigurationError, got {err:?}"
        );
        assert!(format!("{err}").contains("not unique"));
    }

    #[test]
    fn zset_composite_merge_key() {
        // Merge key over both columns: distinct (region,total) live rows are
        // distinct keys, not a uniqueness violation.
        let out = collapse_changelog(
            &zset_batch(&[("east", 10, 1), ("east", 20, 1)]),
            &keys(&["region", "total"]),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(col_str(&out, "_op"), vec!["U", "U"]);
    }

    #[test]
    fn cdc_dedup_keeps_last_arrival() {
        // id=1 inserted then updated within an epoch → one U with the last value.
        let out = collapse_changelog(
            &cdc_batch(&[(1, 10.0, "I"), (1, 15.0, "U"), (2, 20.0, "I")]),
            &keys(&["id"]),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 2);
        let idx = out.schema().index_of("id").unwrap();
        let ids = out
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(col_str(&out, "_op"), vec!["U", "U"]);
        let vidx = out.schema().index_of("value").unwrap();
        let vals = out
            .column(vidx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (vals.value(0) - 15.0).abs() < f64::EPSILON,
            "last value wins"
        );
    }

    #[test]
    fn cdc_delete_is_preserved() {
        let out = collapse_changelog(
            &cdc_batch(&[(1, 10.0, "I"), (1, 10.0, "D")]),
            &keys(&["id"]),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(col_str(&out, "_op"), vec!["D"]);
    }

    #[test]
    fn cdc_update_before_normalizes_to_delete_and_after_to_upsert() {
        // U- alone → D; the U+ after-image → U.
        let out_before =
            collapse_changelog(&cdc_batch(&[(1, 10.0, "U-")]), &keys(&["id"])).unwrap();
        assert_eq!(col_str(&out_before, "_op"), vec!["D"]);
        let out_after = collapse_changelog(&cdc_batch(&[(1, 10.0, "U+")]), &keys(&["id"])).unwrap();
        assert_eq!(col_str(&out_after, "_op"), vec!["U"]);
    }

    #[test]
    fn cdc_no_op_column_treated_as_upsert() {
        // Plain MV (no _op, no __weight) → all upserts, deduped by key.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        let out = collapse_changelog(&batch, &keys(&["id"])).unwrap();
        assert_eq!(out.num_rows(), 2);
        assert!(out.schema().index_of("_op").is_ok());
        assert_eq!(col_str(&out, "_op"), vec!["U", "U"]);
    }

    #[test]
    fn cdc_strips_ts_ms_and_emits_single_op() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_ts_ms", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(StringArray::from(vec!["I", "U"])),
            ],
        )
        .unwrap();
        let out = collapse_changelog(&batch, &keys(&["id"])).unwrap();
        assert!(out.schema().index_of("_ts_ms").is_err(), "_ts_ms stripped");
        assert_eq!(
            out.schema()
                .fields()
                .iter()
                .filter(|f| f.name() == "_op")
                .count(),
            1,
            "exactly one _op column"
        );
    }

    #[test]
    fn empty_batch_yields_empty_with_op_column() {
        let out = collapse_changelog(&zset_batch(&[]), &keys(&["region"])).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert!(out.schema().index_of(WEIGHT_COLUMN).is_err());
        assert!(out.schema().index_of("_op").is_ok());
    }

    #[test]
    fn empty_merge_key_errors() {
        let err = collapse_changelog(&zset_batch(&[("east", 10, 1)]), &[]).unwrap_err();
        assert!(matches!(err, ConnectorError::ConfigurationError(_)));
    }

    #[test]
    fn missing_merge_key_column_errors() {
        let err =
            collapse_changelog(&zset_batch(&[("east", 10, 1)]), &keys(&["nope"])).unwrap_err();
        assert!(matches!(err, ConnectorError::ConfigurationError(_)));
        assert!(format!("{err}").contains("not present"));
    }
}
