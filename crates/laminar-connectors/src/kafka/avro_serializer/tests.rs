use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

const CONFLUENT_HEADER_SIZE: usize = 5;
const CONFLUENT_MAGIC: u8 = 0x00;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn test_batch(n: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<String> = (0..n).map(|i| format!("name-{i}")).collect();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

#[test]
fn test_new_serializer() {
    let ser = AvroSerializer::new(test_schema(), 42);
    assert_eq!(ser.schema_id(), 42);
    assert!(!ser.has_schema_registry());
    assert_eq!(ser.format(), Format::Avro);
}

#[test]
fn test_shared_schema_id() {
    let ser = AvroSerializer::new(test_schema(), 1);
    assert_eq!(ser.schema_id(), 1);
    let handle = ser.schema_id_handle();
    handle.store(99, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(ser.schema_id(), 99);
}

#[test]
fn test_serialize_empty_batch() {
    let ser = AvroSerializer::new(test_schema(), 1);
    let batch = RecordBatch::new_empty(test_schema());
    let result = ser.serialize(&batch).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_serialize_batch_produces_records() {
    let ser = AvroSerializer::new(test_schema(), 7);
    let batch = test_batch(3);
    let records = ser.serialize(&batch).unwrap();
    assert_eq!(records.len(), 3);

    for record in &records {
        assert!(record.len() >= CONFLUENT_HEADER_SIZE);
        assert_eq!(record[0], CONFLUENT_MAGIC);
        assert_eq!(&record[1..5], &7u32.to_be_bytes());
    }
}

#[test]
fn test_serialize_batch_to_single_buffer() {
    let ser = AvroSerializer::new(test_schema(), 1);
    let batch = test_batch(2);
    let buf = ser.serialize_batch(&batch).unwrap();
    assert!(!buf.is_empty());
    assert_eq!(buf[0], CONFLUENT_MAGIC);
}

#[test]
fn test_debug_output() {
    let ser = AvroSerializer::new(test_schema(), 42);
    let debug = format!("{ser:?}");
    assert!(debug.contains("AvroSerializer"));
    assert!(debug.contains("42"));
}
