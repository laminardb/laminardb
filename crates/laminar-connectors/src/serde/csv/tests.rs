use super::*;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]))
}

#[test]
fn test_csv_deserialize_basic() {
    let deser = CsvDeserializer::new();
    let schema = test_schema();
    let data = b"1,Alice,95.5";

    let batch = deser.deserialize(data, &schema).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
}

#[test]
fn test_csv_serialize_roundtrip() {
    let deser = CsvDeserializer::new();
    let ser = CsvSerializer::new();
    let schema = test_schema();

    let data = b"42,Charlie,88.5";
    let batch = deser.deserialize(data, &schema).unwrap();

    let serialized = ser.serialize(&batch).unwrap();
    assert_eq!(serialized.len(), 1);

    let line = std::str::from_utf8(&serialized[0]).unwrap();
    assert!(line.contains("42"));
    assert!(line.contains("Charlie"));
}

#[test]
fn test_csv_null_handling() {
    let deser = CsvDeserializer::new();
    let schema = test_schema();
    let data = b"1,Bob,";

    let batch = deser.deserialize(data, &schema).unwrap();
    assert!(batch.column(2).is_null(0));
}

#[test]
fn test_csv_quoted_fields() {
    let deser = CsvDeserializer::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("desc", DataType::Utf8, false),
    ]));
    let data = br#"1,"hello, world""#;

    let batch = deser.deserialize(data, &schema).unwrap();
    let descs = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(descs.value(0), "hello, world");
}
