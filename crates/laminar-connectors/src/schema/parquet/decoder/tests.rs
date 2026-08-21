use std::sync::Arc;

use super::*;
use arrow_array::cast::AsArray;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

/// Helper: write a `RecordBatch` to Parquet bytes.
fn to_parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    buf
}

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn make_batch(schema: &SchemaRef, ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

#[test]
fn test_decode_empty_batch() {
    let schema = make_schema();
    let decoder = ParquetDecoder::new(schema.clone());
    let batch = decoder.decode_batch(&[]).unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_decode_single_record() {
    let schema = make_schema();
    let input = make_batch(&schema, vec![1, 2, 3], vec!["a", "b", "c"]);
    let parquet_bytes = to_parquet_bytes(&input);

    let decoder = ParquetDecoder::new(schema);
    let record = RawRecord::new(parquet_bytes);
    let output = decoder.decode_batch(&[record]).unwrap();

    assert_eq!(output.num_rows(), 3);
    let ids = output
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
    let names = output.column(1).as_string::<i32>();
    assert_eq!(names.value(0), "a");
}

#[test]
fn test_decode_multiple_records() {
    let schema = make_schema();
    let b1 = make_batch(&schema, vec![1, 2], vec!["x", "y"]);
    let b2 = make_batch(&schema, vec![3, 4], vec!["z", "w"]);

    let r1 = RawRecord::new(to_parquet_bytes(&b1));
    let r2 = RawRecord::new(to_parquet_bytes(&b2));

    let decoder = ParquetDecoder::new(schema);
    let output = decoder.decode_batch(&[r1, r2]).unwrap();
    assert_eq!(output.num_rows(), 4);
}

#[test]
fn test_decode_with_batch_size() {
    let schema = make_schema();
    let input = make_batch(&schema, vec![1, 2, 3], vec!["a", "b", "c"]);
    let parquet_bytes = to_parquet_bytes(&input);

    let config = ParquetDecoderConfig::default().with_batch_size(1);
    let decoder = ParquetDecoder::with_config(schema, config);
    let record = RawRecord::new(parquet_bytes);
    let output = decoder.decode_batch(&[record]).unwrap();

    // All rows should still be present (batch_size only affects internal
    // chunking, concat merges them back).
    assert_eq!(output.num_rows(), 3);
}

#[test]
fn test_format_name() {
    let schema = make_schema();
    let decoder = ParquetDecoder::new(schema);
    assert_eq!(decoder.format_name(), "parquet");
}

#[test]
fn test_decode_one() {
    let schema = make_schema();
    let input = make_batch(&schema, vec![42], vec!["hello"]);
    let parquet_bytes = to_parquet_bytes(&input);

    let decoder = ParquetDecoder::new(schema);
    let record = RawRecord::new(parquet_bytes);
    let output = decoder.decode_one(&record).unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        42
    );
}

#[test]
fn test_decode_invalid_bytes() {
    let schema = make_schema();
    let decoder = ParquetDecoder::new(schema);
    let record = RawRecord::new(b"not parquet".to_vec());
    let result = decoder.decode_batch(&[record]);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Parquet"));
}

#[test]
fn test_config_builder() {
    let config = ParquetDecoderConfig::default()
        .with_projection(vec![0, 2])
        .with_row_groups(vec![0])
        .with_batch_size(4096)
        .with_predicate(RowGroupPredicate::Eq {
            column: "id".into(),
            value: "42".into(),
        });

    assert_eq!(config.projection_indices, vec![0, 2]);
    assert_eq!(config.row_group_indices, vec![0]);
    assert_eq!(config.batch_size, 4096);
    assert!(config.predicate.is_some());
}
