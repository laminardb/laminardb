//! Batch-level temporal join execution on `RecordBatch`es.
//!
//! Implements the temporal join algorithm (FOR `SYSTEM_TIME` AS OF) for batch
//! data. For each stream (left) row, finds the matching table (right) row
//! with the highest version timestamp <= the stream row's event time.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use laminar_sql::translator::TemporalJoinTranslatorConfig;

use crate::error::DbError;

/// A borrowed reference to a key column.
enum KeyColumn<'a> {
    Utf8(&'a StringArray),
    Int64(&'a Int64Array),
}

impl KeyColumn<'_> {
    fn hash_at(&self, i: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        match self {
            KeyColumn::Utf8(a) => a.value(i).hash(&mut hasher),
            KeyColumn::Int64(a) => a.value(i).hash(&mut hasher),
        }
        hasher.finish()
    }

    fn keys_equal(&self, i: usize, other: &KeyColumn<'_>, j: usize) -> bool {
        match (self, other) {
            (KeyColumn::Utf8(a), KeyColumn::Utf8(b)) => a.value(i) == b.value(j),
            (KeyColumn::Int64(a), KeyColumn::Int64(b)) => a.value(i) == b.value(j),
            _ => false,
        }
    }
}

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

fn extract_column_as_timestamps(batch: &RecordBatch, col_name: &str) -> Result<Vec<i64>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Column '{col_name}' not found")))?;
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

/// Execute a temporal join on two sets of `RecordBatch`es.
///
/// For each stream (left) row, finds the table (right) row with the same key
/// whose version timestamp is the highest value <= the stream row's lookup
/// time (point-in-time lookup).
///
/// # Errors
///
/// Returns `DbError::Pipeline` if schemas are invalid or column extraction
/// fails.
pub(crate) fn execute_temporal_join_batch(
    stream_batches: &[RecordBatch],
    table_batches: &[RecordBatch],
    config: &TemporalJoinTranslatorConfig,
) -> Result<RecordBatch, DbError> {
    let is_left = config.join_type == "left";

    if stream_batches.is_empty() {
        let schema = if table_batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            build_temporal_output_schema(
                &Arc::new(Schema::empty()),
                &table_batches[0].schema(),
                &config.stream_key_column,
            )
        };
        return Ok(RecordBatch::new_empty(schema));
    }

    let stream_schema = stream_batches[0].schema();
    let stream = concat_batches(&stream_schema, stream_batches)
        .map_err(|e| DbError::query_pipeline_arrow("temporal join (stream)", &e))?;

    let table_schema = if table_batches.is_empty() {
        Arc::new(Schema::empty())
    } else {
        table_batches[0].schema()
    };

    let table = if table_batches.is_empty() {
        RecordBatch::new_empty(table_schema.clone())
    } else {
        concat_batches(&table_schema, table_batches)
            .map_err(|e| DbError::query_pipeline_arrow("temporal join (table)", &e))?
    };

    let output_schema =
        build_temporal_output_schema(&stream_schema, &table_schema, &config.stream_key_column);

    // Build table index: key_hash -> BTreeMap<version_ts, row_index>
    // BTreeMap ensures we can efficiently find the most recent version <= lookup_ts
    let mut table_index: HashMap<u64, BTreeMap<i64, usize>> =
        HashMap::with_capacity(table.num_rows());
    let table_keys_col;
    if table.num_rows() > 0 {
        table_keys_col = Some(extract_key_column(&table, &config.table_key_column)?);
        let table_versions = extract_column_as_timestamps(&table, &config.table_version_column)?;
        let tk = table_keys_col.as_ref().unwrap();

        for (i, &ver_ts) in table_versions.iter().enumerate() {
            let key_hash = tk.hash_at(i);
            table_index.entry(key_hash).or_default().insert(ver_ts, i);
        }
    } else {
        table_keys_col = None;
    }

    // Extract stream key column and lookup timestamps
    let stream_keys_col = extract_key_column(&stream, &config.stream_key_column)?;
    let stream_timestamps = extract_column_as_timestamps(&stream, &config.stream_time_column)?;

    let mut stream_indices: Vec<usize> = Vec::with_capacity(stream.num_rows());
    let mut table_indices: Vec<Option<usize>> = Vec::with_capacity(stream.num_rows());

    for (stream_idx, &lookup_ts) in stream_timestamps.iter().enumerate() {
        let stream_hash = stream_keys_col.hash_at(stream_idx);

        let matched = table_index.get(&stream_hash).and_then(|btree| {
            // Find most recent table version <= lookup_ts
            let (_, &row_idx) = btree.range(..=lookup_ts).next_back()?;
            // Verify key equality (collision check)
            if let Some(ref tk) = table_keys_col {
                if stream_keys_col.keys_equal(stream_idx, tk, row_idx) {
                    return Some(row_idx);
                }
            }
            None
        });

        match (is_left, matched) {
            (_, Some(table_idx)) => {
                stream_indices.push(stream_idx);
                table_indices.push(Some(table_idx));
            }
            (true, None) => {
                stream_indices.push(stream_idx);
                table_indices.push(None);
            }
            (false, None) => {
                // Inner join: skip unmatched stream rows
            }
        }
    }

    build_temporal_output_batch(
        &stream,
        &table,
        &stream_indices,
        &table_indices,
        &output_schema,
        &config.stream_key_column,
    )
}

fn build_temporal_output_schema(
    stream_schema: &SchemaRef,
    table_schema: &SchemaRef,
    stream_key_column: &str,
) -> SchemaRef {
    let mut fields: Vec<Field> = stream_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    let stream_names: HashSet<&str> = stream_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    for field in table_schema.fields() {
        // Skip duplicate key column (already in stream side)
        if field.name() == stream_key_column {
            continue;
        }
        let mut f = field.as_ref().clone();
        // Table side is always nullable in temporal joins (may not have a
        // matching version)
        f = f.with_nullable(true);
        if stream_names.contains(f.name().as_str()) {
            let suffixed = format!("{}_table", f.name());
            f = f.with_name(suffixed);
        }
        fields.push(f);
    }

    Arc::new(Schema::new(fields))
}

fn build_temporal_output_batch(
    stream: &RecordBatch,
    table: &RecordBatch,
    stream_indices: &[usize],
    table_indices: &[Option<usize>],
    output_schema: &SchemaRef,
    stream_key_column: &str,
) -> Result<RecordBatch, DbError> {
    let num_rows = stream_indices.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(stream.num_columns() + table.num_columns());

    // Stream-side columns
    #[allow(clippy::cast_possible_truncation)]
    let stream_idx_array = arrow::array::UInt32Array::from(
        stream_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );
    for col_idx in 0..stream.num_columns() {
        let array = stream.column(col_idx);
        let taken = arrow::compute::take(array, &stream_idx_array, None)
            .map_err(|e| DbError::query_pipeline_arrow("temporal join (stream take)", &e))?;
        columns.push(taken);
    }

    // Table-side columns (with nulls for unmatched)
    let table_schema = table.schema();
    for col_idx in 0..table.num_columns() {
        let field_name = table_schema.field(col_idx).name();
        if field_name == stream_key_column {
            continue;
        }

        let array = table.column(col_idx);
        let taken = take_with_nulls(array, table_indices, num_rows)?;
        columns.push(taken);
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| DbError::query_pipeline_arrow("temporal join (result)", &e))
}

fn take_with_nulls(
    array: &dyn Array,
    indices: &[Option<usize>],
    num_rows: usize,
) -> Result<ArrayRef, DbError> {
    if array.is_empty() {
        return Ok(arrow::array::new_null_array(array.data_type(), num_rows));
    }

    #[allow(clippy::cast_possible_truncation)]
    let index_array = arrow::array::UInt32Array::from(
        indices
            .iter()
            .map(|opt| opt.map(|i| i as u32))
            .collect::<Vec<Option<u32>>>(),
    );

    arrow::compute::take(array, &index_array, None)
        .map_err(|e| DbError::query_pipeline_arrow("temporal join (take)", &e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orders_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("order_time", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["C1", "C1", "C2", "C1"])),
                Arc::new(Int64Array::from(vec![100, 200, 150, 300])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 15.0, 30.0])),
            ],
        )
        .unwrap()
    }

    fn products_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("valid_from", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["C1", "C1", "C2"])),
                Arc::new(Int64Array::from(vec![50, 180, 100])),
                Arc::new(StringArray::from(vec!["silver", "gold", "bronze"])),
            ],
        )
        .unwrap()
    }

    fn temporal_config(join_type: &str) -> TemporalJoinTranslatorConfig {
        TemporalJoinTranslatorConfig {
            stream_table: "orders".to_string(),
            table_name: "products".to_string(),
            stream_key_column: "customer_id".to_string(),
            table_key_column: "customer_id".to_string(),
            stream_time_column: "order_time".to_string(),
            table_version_column: "valid_from".to_string(),
            semantics: "event_time".to_string(),
            join_type: join_type.to_string(),
        }
    }

    #[test]
    fn test_inner_temporal_join() {
        let config = temporal_config("inner");
        let result =
            execute_temporal_join_batch(&[orders_batch()], &[products_batch()], &config).unwrap();

        // C1@100 -> version@50 (silver), C1@200 -> version@180 (gold),
        // C2@150 -> version@100 (bronze), C1@300 -> version@180 (gold)
        assert_eq!(result.num_rows(), 4);

        // Output: customer_id(0), order_time(1), amount(2), valid_from(3), tier(4)
        let tier = result
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(tier.value(0), "silver"); // C1@100 -> version@50
        assert_eq!(tier.value(1), "gold"); // C1@200 -> version@180
        assert_eq!(tier.value(2), "bronze"); // C2@150 -> version@100
        assert_eq!(tier.value(3), "gold"); // C1@300 -> version@180
    }

    #[test]
    fn test_left_temporal_join_with_nulls() {
        let config = temporal_config("left");
        // Create a stream with a customer that has no table entry
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("order_time", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let stream = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["C3"])),
                Arc::new(Int64Array::from(vec![200])),
                Arc::new(Float64Array::from(vec![50.0])),
            ],
        )
        .unwrap();

        let result = execute_temporal_join_batch(&[stream], &[products_batch()], &config).unwrap();

        assert_eq!(result.num_rows(), 1);
        assert!(result.column(4).is_null(0)); // tier is null (no C3)
    }

    #[test]
    fn test_inner_temporal_join_skips_unmatched() {
        let config = temporal_config("inner");
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("order_time", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let stream = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["C3", "C1"])),
                Arc::new(Int64Array::from(vec![200, 100])),
                Arc::new(Float64Array::from(vec![50.0, 10.0])),
            ],
        )
        .unwrap();

        let result = execute_temporal_join_batch(&[stream], &[products_batch()], &config).unwrap();

        // C3 skipped (inner), C1@100 matches
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_no_version_before_lookup_time() {
        let config = temporal_config("left");
        // C1 orders at ts=10, but earliest version is ts=50
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("order_time", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let stream = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["C1"])),
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(Float64Array::from(vec![5.0])),
            ],
        )
        .unwrap();

        let result = execute_temporal_join_batch(&[stream], &[products_batch()], &config).unwrap();

        assert_eq!(result.num_rows(), 1);
        assert!(result.column(4).is_null(0)); // no version <= 10
    }

    #[test]
    fn test_empty_stream_input() {
        let config = temporal_config("inner");
        let result = execute_temporal_join_batch(&[], &[products_batch()], &config).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_empty_table_input() {
        let config = temporal_config("left");
        let result = execute_temporal_join_batch(&[orders_batch()], &[], &config).unwrap();

        // Left join: all stream rows emitted with null table columns
        assert_eq!(result.num_rows(), 4);
    }
}
