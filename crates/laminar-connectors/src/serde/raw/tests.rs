use super::*;
use arrow_array::{Array, BinaryArray, StringArray};

fn binary_schema(data_type: DataType) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", data_type, false)]))
}

#[test]
fn declared_binary_value_preserves_non_utf8_bytes() {
    let payloads: Vec<&[u8]> = vec![&[0xff, 0x00, 0xfe], b"", &[0x80]];
    for data_type in [DataType::Binary, DataType::LargeBinary] {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            data_type.clone(),
            true,
        )]));
        let batch = RawBytesDeserializer::new()
            .deserialize_batch(&payloads, &schema)
            .unwrap();
        assert_eq!(batch.schema(), schema);
        let encoded = RawBytesSerializer::new().serialize(&batch).unwrap();
        assert_eq!(
            encoded,
            payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>()
        );
    }
    let single = RawBytesDeserializer::new()
        .deserialize(&[0xc3, 0x28], &binary_schema(DataType::Binary))
        .unwrap();
    let column = single
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(column.value(0), &[0xc3, 0x28]);
}

#[test]
fn utf8_value_still_rejects_invalid_text_and_protocol_selection_is_declared() {
    assert!(RawBytesDeserializer::new()
        .deserialize(&[0xc3, 0x28], &raw_schema())
        .is_err());
    assert_eq!(raw_protocol_for(None), raw_schema());
    let binary = binary_schema(DataType::Binary);
    assert_eq!(raw_protocol_for(Some(&binary)), binary);
    let two = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("other", DataType::Binary, false),
    ]));
    assert_eq!(raw_protocol_for(Some(&two)), raw_schema());
}

#[test]
fn serializer_rejects_non_payload_column_types() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(RawBytesSerializer::new().serialize(&batch).is_err());
}

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
