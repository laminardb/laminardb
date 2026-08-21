use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::Field;

fn user_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

#[test]
fn test_debezium_insert() {
    let deser = DebeziumDeserializer::new();
    let schema = user_schema();

    let data = br#"{
        "before": null,
        "after": {"id": 1, "name": "Alice"},
        "op": "c",
        "ts_ms": 1700000000000
    }"#;

    let batch = deser.deserialize(data, &schema).unwrap();
    assert_eq!(batch.num_rows(), 1);
    // data columns + __op + __ts_ms
    assert_eq!(batch.num_columns(), 4);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);

    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");

    let ops = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ops.value(0), "c");

    let ts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ts.value(0), 1_700_000_000_000);
}

#[test]
fn test_debezium_update() {
    let deser = DebeziumDeserializer::new();
    let schema = user_schema();

    let data = br#"{
        "before": {"id": 1, "name": "Alice"},
        "after": {"id": 1, "name": "Alicia"},
        "op": "u",
        "ts_ms": 1700000001000
    }"#;

    let batch = deser.deserialize(data, &schema).unwrap();
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alicia");

    let ops = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ops.value(0), "u");
}

#[test]
fn test_debezium_delete() {
    let deser = DebeziumDeserializer::new();
    let schema = user_schema();

    let data = br#"{
        "before": {"id": 1, "name": "Alice"},
        "after": null,
        "op": "d",
        "ts_ms": 1700000002000
    }"#;

    let batch = deser.deserialize(data, &schema).unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);

    let ops = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ops.value(0), "d");
}

#[test]
fn test_debezium_invalid_op() {
    let deser = DebeziumDeserializer::new();
    let schema = user_schema();

    let data = br#"{
        "before": null,
        "after": {"id": 1, "name": "Alice"},
        "op": "x",
        "ts_ms": 1700000000000
    }"#;

    let result = deser.deserialize(data, &schema);
    assert!(result.is_err());
}
