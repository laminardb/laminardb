//! Batch-level ASOF join execution on `RecordBatch`es.
//!
//! Implements the ASOF join algorithm for batch data, matching each left row
//! to the closest right row by timestamp within the same key partition.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use laminar_sql::parser::join_parser::AsofSqlDirection;
use laminar_sql::translator::{AsofJoinTranslatorConfig, AsofSqlJoinType};

use crate::error::DbError;

/// A borrowed reference to a key column, avoiding per-row String allocations.
enum KeyColumn<'a> {
    Utf8(&'a StringArray),
    Int64(&'a Int64Array),
}

impl KeyColumn<'_> {
    /// Computes a hash for the key at row `i`.
    fn hash_at(&self, i: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        match self {
            KeyColumn::Utf8(a) => a.value(i).hash(&mut hasher),
            KeyColumn::Int64(a) => a.value(i).hash(&mut hasher),
        }
        hasher.finish()
    }

    /// Returns true if the keys at the given indices in two `KeyColumn`s are equal.
    fn keys_equal(&self, i: usize, other: &KeyColumn<'_>, j: usize) -> bool {
        match (self, other) {
            (KeyColumn::Utf8(a), KeyColumn::Utf8(b)) => a.value(i) == b.value(j),
            (KeyColumn::Int64(a), KeyColumn::Int64(b)) => a.value(i) == b.value(j),
            _ => false,
        }
    }
}

/// Extracts a key column from a `RecordBatch` without per-row allocation.
fn extract_key_column<'a>(
    batch: &'a RecordBatch,
    col_name: &str,
) -> Result<KeyColumn<'a>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Column '{col_name}' not found")))?;
    let array = batch.column(col_idx);

    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Utf8")))?;
            Ok(KeyColumn::Utf8(string_array))
        }
        DataType::Int64 => {
            let int_array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Int64")))?;
            Ok(KeyColumn::Int64(int_array))
        }
        other => Err(DbError::Pipeline(format!(
            "Unsupported key column type: {other}"
        ))),
    }
}

/// Execute an ASOF join on two sets of `RecordBatch`es.
///
/// Matches each left row to the closest right row by timestamp, partitioned
/// by key column, according to the direction and tolerance in `config`.
///
/// # Errors
///
/// Returns `DbError::Pipeline` if schemas are invalid or column extraction fails.
pub(crate) fn execute_asof_join_batch(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &AsofJoinTranslatorConfig,
) -> Result<RecordBatch, DbError> {
    if left_batches.is_empty() {
        let schema = if right_batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            build_output_schema(
                &Arc::new(Schema::empty()),
                &right_batches[0].schema(),
                config,
            )
        };
        return Ok(RecordBatch::new_empty(schema));
    }

    let left_schema = left_batches[0].schema();
    let left = concat_batches(&left_schema, left_batches)
        .map_err(|e| DbError::query_pipeline_arrow("ASOF join (left)", &e))?;

    let right_schema = if right_batches.is_empty() {
        // Build a schema with the same structure but no rows
        Arc::new(Schema::empty())
    } else {
        right_batches[0].schema()
    };

    let right = if right_batches.is_empty() {
        RecordBatch::new_empty(right_schema.clone())
    } else {
        concat_batches(&right_schema, right_batches)
            .map_err(|e| DbError::query_pipeline_arrow("ASOF join (right)", &e))?
    };

    let output_schema = build_output_schema(&left_schema, &right_schema, config);

    // Build right-side index: key_hash -> BTreeMap<timestamp, row_index>
    // Keyed by hash to avoid per-row String allocations.
    let mut right_index: FxHashMap<u64, BTreeMap<i64, usize>> =
        FxHashMap::with_capacity_and_hasher(right.num_rows(), rustc_hash::FxBuildHasher);
    let right_keys_col;
    if right.num_rows() > 0 {
        right_keys_col = Some(extract_key_column(&right, &config.key_column)?);
        let right_timestamps = extract_column_as_timestamps(&right, &config.right_time_column)?;
        let rk = right_keys_col.as_ref().unwrap();

        for (i, &ts) in right_timestamps.iter().enumerate() {
            let key_hash = rk.hash_at(i);
            right_index.entry(key_hash).or_default().insert(ts, i);
        }
    } else {
        right_keys_col = None;
    }

    // Extract left key and timestamp columns (zero-alloc borrow)
    let left_keys_col = extract_key_column(&left, &config.key_column)?;
    let left_timestamps = extract_column_as_timestamps(&left, &config.left_time_column)?;

    let tolerance_ms = config
        .tolerance
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));

    // For each left row, find matching right row
    let mut left_indices: Vec<usize> = Vec::with_capacity(left.num_rows());
    let mut right_indices: Vec<Option<usize>> = Vec::with_capacity(left.num_rows());

    for (left_idx, &left_ts) in left_timestamps.iter().enumerate() {
        let left_hash = left_keys_col.hash_at(left_idx);

        // Look up by hash, then verify key equality on candidates to handle collisions
        let matched_right = right_index.get(&left_hash).and_then(|btree| {
            let candidate = find_match(btree, left_ts, config.direction, tolerance_ms)?;
            // Verify key equality (collision check)
            if let Some(ref rk) = right_keys_col {
                if left_keys_col.keys_equal(left_idx, rk, candidate) {
                    return Some(candidate);
                }
            }
            None
        });

        match (&config.join_type, matched_right) {
            (_, Some(right_idx)) => {
                left_indices.push(left_idx);
                right_indices.push(Some(right_idx));
            }
            (AsofSqlJoinType::Left, None) => {
                left_indices.push(left_idx);
                right_indices.push(None);
            }
            (AsofSqlJoinType::Inner, None) => {
                // Skip unmatched rows for inner join
            }
        }
    }

    // Build output columns
    build_output_batch(
        &left,
        &right,
        &left_indices,
        &right_indices,
        &output_schema,
        config,
    )
}

/// Find the best matching right row given direction and tolerance.
fn find_match(
    btree: &BTreeMap<i64, usize>,
    left_ts: i64,
    direction: AsofSqlDirection,
    tolerance_ms: Option<i64>,
) -> Option<usize> {
    let candidate = match direction {
        AsofSqlDirection::Backward => {
            // Find most recent right row <= left_ts
            btree
                .range(..=left_ts)
                .next_back()
                .map(|(&ts, &idx)| (ts, idx))
        }
        AsofSqlDirection::Forward => {
            // Find earliest right row >= left_ts
            btree.range(left_ts..).next().map(|(&ts, &idx)| (ts, idx))
        }
    };

    candidate.and_then(|(right_ts, right_idx)| {
        if let Some(tol) = tolerance_ms {
            if (left_ts - right_ts).abs() <= tol {
                Some(right_idx)
            } else {
                None
            }
        } else {
            Some(right_idx)
        }
    })
}

/// Extract a column's values as `i64` timestamps (epoch millis).
fn extract_column_as_timestamps(batch: &RecordBatch, col_name: &str) -> Result<Vec<i64>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Timestamp column '{col_name}' not found")))?;
    let array = batch.column(col_idx);

    match array.data_type() {
        DataType::Int64 => {
            let int_array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Int64")))?;
            Ok(int_array.values().to_vec())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ts_array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline(format!("Column '{col_name}' is not TimestampMillisecond"))
                })?;
            Ok(ts_array.values().to_vec())
        }
        DataType::Float64 => {
            // Support float timestamps (cast to i64 millis)
            let f_array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Float64")))?;
            #[allow(clippy::cast_possible_truncation)]
            Ok(f_array.values().iter().map(|v| *v as i64).collect())
        }
        other => Err(DbError::Pipeline(format!(
            "Unsupported timestamp column type for '{col_name}': {other}"
        ))),
    }
}

/// Build the merged output schema from left and right schemas.
///
/// Right-side columns are made nullable for Left joins. Duplicate column
/// names (collisions between left and right) are disambiguated by appending
/// `_{right_table}` to the right-side field.
fn build_output_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &AsofJoinTranslatorConfig,
) -> SchemaRef {
    let mut fields: Vec<Field> = left_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    let left_names: FxHashSet<&str> = left_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let make_nullable = config.join_type == AsofSqlJoinType::Left;
    for field in right_schema.fields() {
        // Skip duplicate key column (already in left side)
        if field.name() == &config.key_column {
            continue;
        }
        let mut f = field.as_ref().clone();
        if make_nullable {
            f = f.with_nullable(true);
        }
        // Disambiguate duplicate names by appending _{right_table}
        if left_names.contains(f.name().as_str()) {
            let suffixed_name = format!("{}_{}", f.name(), config.right_table);
            f = f.with_name(suffixed_name);
        }
        fields.push(f);
    }

    Arc::new(Schema::new(fields))
}

/// Build the output `RecordBatch` from matched indices.
fn build_output_batch(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &[usize],
    right_indices: &[Option<usize>],
    output_schema: &SchemaRef,
    config: &AsofJoinTranslatorConfig,
) -> Result<RecordBatch, DbError> {
    let num_rows = left_indices.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(left.num_columns() + right.num_columns());

    // Left-side columns: take selected rows
    #[allow(clippy::cast_possible_truncation)]
    let left_idx_array =
        arrow::array::UInt32Array::from(left_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
    for col_idx in 0..left.num_columns() {
        let array = left.column(col_idx);
        let taken = arrow::compute::take(array, &left_idx_array, None)
            .map_err(|e| DbError::query_pipeline_arrow("ASOF join (left take)", &e))?;
        columns.push(taken);
    }

    // Right-side columns: take selected rows (with nulls for unmatched)
    let right_schema = right.schema();
    for col_idx in 0..right.num_columns() {
        let field_name = right_schema.field(col_idx).name();
        // Skip duplicate key column
        if field_name == &config.key_column {
            continue;
        }

        let array = right.column(col_idx);
        let taken = take_with_nulls(array, right_indices, num_rows)?;
        columns.push(taken);
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| DbError::query_pipeline_arrow("ASOF join (result)", &e))
}

/// Take rows from an array using optional indices (None = null).
fn take_with_nulls(
    array: &dyn Array,
    indices: &[Option<usize>],
    num_rows: usize,
) -> Result<ArrayRef, DbError> {
    if array.is_empty() {
        // Right side is empty — produce typed all-null array matching the source dtype
        return Ok(arrow::array::new_null_array(array.data_type(), num_rows));
    }

    // Build a UInt32Array with null entries for unmatched rows
    #[allow(clippy::cast_possible_truncation)]
    let index_array = arrow::array::UInt32Array::from(
        indices
            .iter()
            .map(|opt| opt.map(|i| i as u32))
            .collect::<Vec<Option<u32>>>(),
    );

    arrow::compute::take(array, &index_array, None)
        .map_err(|e| DbError::query_pipeline_arrow("ASOF join (right take)", &e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn trades_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "AAPL", "GOOG", "AAPL"])),
                Arc::new(Int64Array::from(vec![100, 200, 150, 300])),
                Arc::new(Float64Array::from(vec![150.0, 152.0, 2800.0, 155.0])),
            ],
        )
        .unwrap()
    }

    fn quotes_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("quote_ts", DataType::Int64, false),
            Field::new("bid", DataType::Float64, false),
            Field::new("ask", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "AAPL", "AAPL", "GOOG", "AAPL", "GOOG",
                ])),
                Arc::new(Int64Array::from(vec![90, 180, 140, 250, 160])),
                Arc::new(Float64Array::from(vec![
                    149.0, 151.0, 2790.0, 153.0, 2795.0,
                ])),
                Arc::new(Float64Array::from(vec![
                    150.0, 152.0, 2800.0, 154.0, 2805.0,
                ])),
            ],
        )
        .unwrap()
    }

    fn backward_config() -> AsofJoinTranslatorConfig {
        AsofJoinTranslatorConfig {
            left_table: "trades".to_string(),
            right_table: "quotes".to_string(),
            key_column: "symbol".to_string(),
            left_time_column: "trade_ts".to_string(),
            right_time_column: "quote_ts".to_string(),
            direction: AsofSqlDirection::Backward,
            tolerance: None,
            join_type: AsofSqlJoinType::Left,
        }
    }

    #[test]
    fn test_backward_join_basic() {
        let config = backward_config();
        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        // 4 left rows → 4 output rows (Left join)
        assert_eq!(result.num_rows(), 4);
        // Output should have: symbol, trade_ts, price, quote_ts, bid, ask
        assert_eq!(result.num_columns(), 6);

        // Verify AAPL trade at ts=100 matches quote at ts=90 (backward: 90 <= 100)
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 90); // trade@100 → quote@90
        assert_eq!(quote_ts.value(1), 180); // trade@200 → quote@180
    }

    #[test]
    fn test_forward_join_basic() {
        let mut config = backward_config();
        config.direction = AsofSqlDirection::Forward;

        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        assert_eq!(result.num_rows(), 4);
        // AAPL trade at ts=100 → forward match is quote@180 (earliest >= 100)
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 180); // trade@100 → quote@180 (forward)
        assert_eq!(quote_ts.value(1), 250); // trade@200 → quote@250 (earliest >= 200)
    }

    #[test]
    fn test_left_join_emits_unmatched_with_nulls() {
        // Create trades with a symbol that has no quotes
        let trades_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let trades = RecordBatch::try_new(
            trades_schema,
            vec![
                Arc::new(StringArray::from(vec!["MSFT"])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Float64Array::from(vec![300.0])),
            ],
        )
        .unwrap();

        let config = backward_config();
        let result = execute_asof_join_batch(&[trades], &[quotes_batch()], &config).unwrap();

        // Left join: MSFT has no match, should still emit with nulls
        assert_eq!(result.num_rows(), 1);
        assert!(result.column(3).is_null(0)); // quote_ts is null
    }

    #[test]
    fn test_inner_join_skips_unmatched() {
        let trades_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let trades = RecordBatch::try_new(
            trades_schema,
            vec![
                Arc::new(StringArray::from(vec!["MSFT", "AAPL"])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Float64Array::from(vec![300.0, 152.0])),
            ],
        )
        .unwrap();

        let mut config = backward_config();
        config.join_type = AsofSqlJoinType::Inner;

        let result = execute_asof_join_batch(&[trades], &[quotes_batch()], &config).unwrap();

        // Inner join: MSFT skipped, only AAPL matches
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_tolerance_filtering() {
        let mut config = backward_config();
        config.tolerance = Some(Duration::from_millis(15));

        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        // AAPL trade@100 → quote@90 (diff=10, within 15ms tolerance) ✓
        // AAPL trade@200 → quote@180 (diff=20, exceeds 15ms) → null (Left join)
        // GOOG trade@150 → quote@140 (diff=10, within 15ms) ✓
        // AAPL trade@300 → quote@250 (diff=50, exceeds 15ms) → null
        assert_eq!(result.num_rows(), 4); // Left join, all left rows emitted
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(quote_ts.value(0), 90); // matched
        assert!(result.column(3).is_null(1)); // no match within tolerance
        assert_eq!(quote_ts.value(2), 140); // matched
        assert!(result.column(3).is_null(3)); // no match within tolerance
    }

    #[test]
    fn test_empty_left_input() {
        let config = backward_config();
        let result = execute_asof_join_batch(&[], &[quotes_batch()], &config).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_empty_right_input() {
        let config = backward_config();
        let result = execute_asof_join_batch(&[trades_batch()], &[], &config).unwrap();

        // Left join with no right data: all rows emitted with nulls
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_multiple_keys() {
        // Both AAPL and GOOG trades should match their respective quotes
        let config = backward_config();
        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        assert_eq!(result.num_rows(), 4);

        // Check GOOG trade@150 matches GOOG quote@140 (not an AAPL quote)
        let symbols = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Row 2 is GOOG
        assert_eq!(symbols.value(2), "GOOG");
        assert_eq!(quote_ts.value(2), 140); // GOOG quote, not AAPL
    }

    #[test]
    fn test_multiple_right_matches_picks_closest() {
        // For backward: AAPL trade@200 should pick quote@180 (closest), not quote@90
        let config = backward_config();
        let result =
            execute_asof_join_batch(&[trades_batch()], &[quotes_batch()], &config).unwrap();

        let quote_ts = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // AAPL trade@200: backward match picks 180 (closest <= 200), not 90
        assert_eq!(quote_ts.value(1), 180);
    }
}
