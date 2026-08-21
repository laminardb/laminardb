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
fn test_json_deserialize_basic() {
    let deser = JsonDeserializer::new();
    let schema = test_schema();
    let data = br#"{"id": 1, "name": "Alice", "score": 95.5}"#;

    let batch = deser.deserialize(data, &schema).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);
}

#[test]
fn test_json_serialize_roundtrip() {
    let deser = JsonDeserializer::new();
    let ser = JsonSerializer::new();
    let schema = test_schema();

    let data = br#"{"id": 42, "name": "Charlie", "score": 88.5}"#;
    let batch = deser.deserialize(data, &schema).unwrap();

    let serialized = ser.serialize(&batch).unwrap();
    assert_eq!(serialized.len(), 1);

    let roundtrip: Value = serde_json::from_slice(&serialized[0]).unwrap();
    assert_eq!(roundtrip["id"], 42);
    assert_eq!(roundtrip["name"], "Charlie");
}

#[test]
fn test_json_deserialize_batch() {
    let deser = JsonDeserializer::new();
    let schema = test_schema();

    let r1 = br#"{"id": 1, "name": "A", "score": 10.0}"#;
    let r2 = br#"{"id": 2, "name": "B", "score": 20.0}"#;
    let records: Vec<&[u8]> = vec![r1, r2];

    let batch = deser.deserialize_batch(&records, &schema).unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_json_deserialize_coercion() {
    let deser = JsonDeserializer::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("qty", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]));

    let data = br#"{"qty": "100", "price": "187.52"}"#;
    let batch = deser.deserialize(data, &schema).unwrap();

    let qty = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(qty.value(0), 100);
}
