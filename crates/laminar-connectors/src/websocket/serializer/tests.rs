use std::sync::Arc;

use arrow_array::{Decimal128Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use super::*;

fn test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap()
}

#[test]
fn test_serialize_rows() {
    let batch = test_batch();
    let serializer = BatchSerializer::new(batch.schema());
    let rows = serializer.serialize_rows(&batch).unwrap();

    assert_eq!(rows.len(), 3);
    let parsed: serde_json::Value = serde_json::from_slice(&rows[0]).unwrap();
    assert_eq!(parsed["id"], 1);
    assert_eq!(parsed["name"], "Alice");
    let last: serde_json::Value = serde_json::from_slice(&rows[2]).unwrap();
    assert_eq!(last["name"], "Charlie");
}

#[test]
fn test_serialize_empty_batch() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch = RecordBatch::new_empty(schema);
    let serializer = BatchSerializer::new(batch.schema());
    assert!(serializer.serialize_rows(&batch).unwrap().is_empty());
}

#[test]
fn test_serialize_with_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec![Some("Alice"), None])),
        ],
    )
    .unwrap();

    let serializer = BatchSerializer::new(batch.schema());
    let rows = serializer.serialize_rows(&batch).unwrap();
    let second: serde_json::Value = serde_json::from_slice(&rows[1]).unwrap();
    assert!(second["id"].is_null());
    assert!(second["name"].is_null());
}

#[test]
fn test_string_that_looks_numeric_stays_a_string() {
    let schema = Arc::new(Schema::new(vec![Field::new("code", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["00123", "true"]))],
    )
    .unwrap();
    let serializer = BatchSerializer::new(batch.schema());

    let rows = serializer.serialize_rows(&batch).unwrap();

    assert_eq!(
        rows,
        [
            br#"{"code":"00123"}"#.to_vec(),
            br#"{"code":"true"}"#.to_vec()
        ]
    );
}

#[test]
fn test_large_unsigned_and_decimal_values_keep_precision() {
    let decimal = Decimal128Array::from(vec![Some(12_345_678_901_234_567_890_i128)])
        .with_precision_and_scale(38, 4)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("unsigned", DataType::UInt64, false),
        Field::new("decimal", DataType::Decimal128(38, 4), false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            Arc::new(decimal),
        ],
    )
    .unwrap();
    let serializer = BatchSerializer::new(batch.schema());

    let rows = serializer.serialize_rows(&batch).unwrap();

    let row = std::str::from_utf8(&rows[0]).unwrap();
    assert!(row.contains("18446744073709551615"));
    assert!(row.contains("1234567890123456.7890"));
}

#[test]
fn test_rejects_batch_schema_drift() {
    let declared = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let actual = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(actual, vec![Arc::new(StringArray::from(vec!["1"]))]).unwrap();
    let serializer = BatchSerializer::new(declared);

    assert!(matches!(
        serializer.serialize_rows(&batch),
        Err(ConnectorError::SchemaMismatch(_))
    ));
}
