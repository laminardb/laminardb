use super::*;

#[test]
fn test_raw_deserialize() {
    let deser = RawBytesDeserializer::new();
    let schema = raw_schema();
    let data = b"hello world";

    let batch = deser.deserialize(data, &schema).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "hello world");
}

#[test]
fn test_raw_roundtrip() {
    let deser = RawBytesDeserializer::new();
    let ser = RawBytesSerializer::new();
    let schema = raw_schema();

    let data = b"test data";
    let batch = deser.deserialize(data, &schema).unwrap();
    let serialized = ser.serialize(&batch).unwrap();

    assert_eq!(serialized.len(), 1);
    assert_eq!(serialized[0], b"test data");
}

#[test]
fn test_raw_batch() {
    let deser = RawBytesDeserializer::new();
    let schema = raw_schema();

    let records: Vec<&[u8]> = vec![b"line1", b"line2", b"line3"];
    let batch = deser.deserialize_batch(&records, &schema).unwrap();
    assert_eq!(batch.num_rows(), 3);
}
