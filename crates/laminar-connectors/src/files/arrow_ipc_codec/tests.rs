use super::*;
use std::sync::Arc;

use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn test_batch(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap()
}

#[test]
fn test_encode_decode_roundtrip() {
    let schema = test_schema();
    let batch = test_batch(&schema);

    let encoder = ArrowIpcEncoder::new(schema.clone());
    let encoded = encoder.encode_batch(&batch).unwrap();
    assert_eq!(encoded.len(), 1);

    let decoder = ArrowIpcDecoder::new(schema);
    let record = RawRecord::new(encoded.into_iter().next().unwrap());
    let decoded = decoder.decode_batch(&[record]).unwrap();

    assert_eq!(decoded.num_rows(), 3);
    assert_eq!(decoded.num_columns(), 2);
    assert_eq!(
        decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert!(decoded.column(1).is_null(1));
}

#[test]
fn test_encode_empty_batch() {
    let schema = test_schema();
    let batch = RecordBatch::new_empty(schema.clone());
    let encoder = ArrowIpcEncoder::new(schema);
    let encoded = encoder.encode_batch(&batch).unwrap();
    assert!(encoded.is_empty());
}

#[test]
fn test_decode_empty_records() {
    let schema = test_schema();
    let decoder = ArrowIpcDecoder::new(schema.clone());
    let batch = decoder.decode_batch(&[]).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), schema);
}
