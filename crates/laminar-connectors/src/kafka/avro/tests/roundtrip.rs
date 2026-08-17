use std::sync::Arc;

use arrow_array::{
    BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::kafka::avro::AvroDeserializer;
use crate::kafka::avro_serializer::AvroSerializer;
use crate::kafka::schema_registry::arrow_to_avro_schema;
use crate::serde::{RecordDeserializer, RecordSerializer};

/// Serializes a batch, deserializes each record, and asserts equality.
fn roundtrip(batch: &RecordBatch, schema: &SchemaRef) -> RecordBatch {
    let avro_schema_json =
        arrow_to_avro_schema(schema, "roundtrip_test").expect("Arrow→Avro schema");

    let ser = AvroSerializer::new(schema.clone(), 1);
    let records = ser.serialize(batch).expect("serialize");

    let mut deser = AvroDeserializer::new();
    deser
        .register_schema(1, &avro_schema_json)
        .expect("register schema");

    let record_refs: Vec<&[u8]> = records.iter().map(Vec::as_slice).collect();
    deser
        .deserialize_batch(&record_refs, schema)
        .expect("deserialize")
}

#[test]
fn test_roundtrip_primitives() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["AAPL", "GOOG", "MSFT"])),
            Arc::new(Float64Array::from(vec![150.0, 2800.0, 300.0])),
        ],
    )
    .unwrap();

    let result = roundtrip(&batch, &schema);
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.num_columns(), 3);

    let ids = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);

    let names = result
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "AAPL");
    assert_eq!(names.value(1), "GOOG");
    assert_eq!(names.value(2), "MSFT");

    let prices = result
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((prices.value(0) - 150.0).abs() < f64::EPSILON);
    assert!((prices.value(1) - 2800.0).abs() < f64::EPSILON);
}

#[test]
fn test_roundtrip_all_primitive_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, false),
        Field::new("i32", DataType::Int32, false),
        Field::new("i64", DataType::Int64, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int32Array::from(vec![42, -1])),
            Arc::new(Int64Array::from(vec![100_000_000, -999])),
            Arc::new(Float32Array::from(vec![3.14f32, -0.001f32])),
            Arc::new(Float64Array::from(vec![2.718, 1e10])),
            Arc::new(StringArray::from(vec!["hello", "world"])),
        ],
    )
    .unwrap();

    let result = roundtrip(&batch, &schema);
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.num_columns(), 6);

    let bools = result
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(bools.value(0));
    assert!(!bools.value(1));

    let ints = result
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ints.value(0), 42);
    assert_eq!(ints.value(1), -1);
}

#[test]
fn test_roundtrip_single_row() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![99])),
            Arc::new(StringArray::from(vec!["single"])),
        ],
    )
    .unwrap();

    let result = roundtrip(&batch, &schema);
    assert_eq!(result.num_rows(), 1);
    let val = result
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(val.value(0), "single");
}

#[test]
fn test_roundtrip_confluent_wire_format() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();

    let ser = AvroSerializer::new(schema.clone(), 42);
    let records = ser.serialize(&batch).unwrap();

    // Verify Confluent wire format: 0x00 + 4-byte BE schema ID.
    for record in &records {
        assert!(record.len() >= 5, "record too short");
        assert_eq!(record[0], 0x00, "magic byte");
        let schema_id = u32::from_be_bytes([record[1], record[2], record[3], record[4]]);
        assert_eq!(schema_id, 42, "schema ID in header");
    }

    // Also verify round-trip works with schema ID 42.
    let avro_schema_json = arrow_to_avro_schema(&schema, "test").unwrap();
    let mut deser = AvroDeserializer::new();
    deser.register_schema(42, &avro_schema_json).unwrap();

    let record_refs: Vec<&[u8]> = records.iter().map(Vec::as_slice).collect();
    let result = deser.deserialize_batch(&record_refs, &schema).unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_roundtrip_empty_batch() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::new_empty(schema.clone());

    let ser = AvroSerializer::new(schema.clone(), 1);
    let records = ser.serialize(&batch).unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_roundtrip_many_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let n = 100;
    let ids: Vec<i64> = (0..n).collect();
    let labels: Vec<String> = (0..n).map(|i| format!("row-{i}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(labels)),
        ],
    )
    .unwrap();

    let result = roundtrip(&batch, &schema);
    assert_eq!(result.num_rows(), n as usize);

    let ids = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..n as usize {
        assert_eq!(ids.value(i), i as i64);
    }
}
