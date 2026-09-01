use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{GzipLevel, ZstdLevel};
use std::sync::Arc;

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

#[test]
fn test_encode_empty_batch() {
    let schema = make_schema();
    let batch = RecordBatch::new_empty(schema.clone());
    let encoder = ParquetEncoder::new(schema);
    let result = encoder.encode_batch(&batch).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_encode_roundtrip() {
    let schema = make_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let encoder = ParquetEncoder::new(schema);
    let result = encoder.encode_batch(&batch).unwrap();
    assert_eq!(result.len(), 1);

    // Decode back to verify roundtrip.
    let bytes = bytes::Bytes::from(result.into_iter().next().unwrap());
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
}

#[test]
fn test_encode_with_compression() {
    let schema = make_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();

    let config =
        ParquetEncoderConfig::default().with_compression(Compression::GZIP(GzipLevel::default()));
    let encoder = ParquetEncoder::with_config(schema, config);
    let result = encoder.encode_batch(&batch).unwrap();
    assert_eq!(result.len(), 1);
    assert!(!result[0].is_empty());
}

#[test]
fn test_format_name() {
    let schema = make_schema();
    let encoder = ParquetEncoder::new(schema);
    assert_eq!(encoder.format_name(), "parquet");
}

#[test]
fn test_config_builder() {
    let config = ParquetEncoderConfig::default()
        .with_compression(Compression::ZSTD(ZstdLevel::default()))
        .with_writer_version(1)
        .with_max_row_group_size(500)
        .with_statistics(false);

    assert!(matches!(config.compression, Compression::ZSTD(_)));
    assert_eq!(config.writer_version, 1);
    assert_eq!(config.max_row_group_size, 500);
    assert!(!config.write_statistics);
}
