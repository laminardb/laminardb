use super::*;
use arrow_array::{BinaryArray, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lapin::types::{AMQPValue, ShortString};
use std::sync::Arc;

fn parsed(pairs: &[(&str, &str)]) -> RabbitMqSinkConfig {
    let mut config = ConnectorConfig::new("rabbitmq");
    config.set("url", "amqp://127.0.0.1/%2f");
    for (key, value) in pairs {
        config.set(*key, *value);
    }
    RabbitMqSinkConfig::from_config(&config).unwrap()
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("prio", DataType::Int64, true),
        Field::new("tenant", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from(vec![&[0xff_u8, 0x00][..], &[0x80][..]])),
            Arc::new(StringArray::from(vec!["a.b", "c.d"])),
            Arc::new(StringArray::from(vec![Some("m-1"), None])),
            Arc::new(Int64Array::from(vec![Some(9), None])),
            Arc::new(Int64Array::from(vec![Some(42), None])),
        ],
    )
    .unwrap()
}

fn mapped() -> RabbitMqSinkConfig {
    parsed(&[
        ("format", "raw"),
        ("routing.key.column", "key"),
        ("message.id.column", "id"),
        ("priority.column", "prio"),
        ("header.columns", "tenant"),
    ])
}

#[test]
fn property_columns_are_projected_out_of_the_payload() {
    let batch = batch();
    let writer = RabbitMqSink::writer(&mapped(), &batch.schema()).unwrap();
    assert_eq!(writer.payload_indices, vec![0]);
    let payload = batch.project(&writer.payload_indices).unwrap();
    let records = writer.serializer.serialize(&payload).unwrap();
    assert_eq!(records, vec![vec![0xff, 0x00], vec![0x80]]);
    assert_eq!(
        writer.base.content_type().as_ref().map(ShortString::as_str),
        Some("application/octet-stream")
    );
    assert_eq!(*writer.base.delivery_mode(), Some(2));
}

#[test]
fn rows_bind_to_native_typed_properties() {
    let config = mapped();
    let batch = batch();
    let rows = properties::BoundProperties::bind(&config, &batch).unwrap();
    let base = BasicProperties::default();
    assert_eq!(rows.routing_key(1), "c.d");
    let first = rows.properties(0, &base);
    assert_eq!(
        first.message_id().as_ref().map(ShortString::as_str),
        Some("m-1")
    );
    assert_eq!(*first.priority(), Some(9));
    let headers = first.headers().as_ref().unwrap();
    assert_eq!(
        headers.inner().get(&ShortString::from("tenant")),
        Some(&AMQPValue::LongLongInt(42))
    );
    let second = rows.properties(1, &base);
    assert!(second.message_id().is_none() && second.priority().is_none());
    assert!(second.headers().as_ref().unwrap().inner().is_empty());
}

#[test]
fn invalid_property_values_fail_before_any_publish() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, true),
        Field::new("prio", DataType::Int64, true),
    ]));
    let rows = |key: Option<&str>, prio: Option<i64>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(StringArray::from(vec![key])),
                Arc::new(Int64Array::from(vec![prio])),
            ],
        )
        .unwrap()
    };
    let config = parsed(&[
        ("format", "raw"),
        ("routing.key.column", "key"),
        ("priority.column", "prio"),
    ]);
    assert!(properties::BoundProperties::bind(&config, &rows(None, None)).is_err());
    assert!(properties::BoundProperties::bind(&config, &rows(Some("k"), Some(300))).is_err());
    let long = "k".repeat(256);
    assert!(properties::BoundProperties::bind(&config, &rows(Some(&long), None)).is_err());
    assert!(properties::BoundProperties::bind(&config, &rows(Some("k"), Some(0))).is_ok());
}

#[test]
fn writer_rejects_unrepresentable_property_columns_and_payloads() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("key", DataType::Int64, false),
    ]));
    let config = parsed(&[("format", "raw"), ("routing.key.column", "key")]);
    assert!(RabbitMqSink::writer(&config, &schema).is_err());
    let missing = parsed(&[("format", "raw"), ("routing.key.column", "absent")]);
    assert!(RabbitMqSink::writer(&missing, &schema).is_err());
    let raw_two_columns = parsed(&[("format", "raw"), ("routing.key", "")]);
    assert!(RabbitMqSink::writer(&raw_two_columns, &schema).is_err());
}

#[test]
fn confirm_outcomes_never_hide_a_lost_or_returned_publish() {
    let ok = Drained {
        confirmed: 3,
        ..Drained::default()
    };
    assert_eq!(ok.into_result().unwrap(), 3);
    let ambiguous = Drained {
        confirmed: 1,
        ambiguous: Some("connection reset".into()),
        returned: Some("312 NO_ROUTE".into()),
        ..Drained::default()
    };
    assert!(ambiguous.into_result().unwrap_err().is_outcome_unknown());
    let returned = Drained {
        returned: Some("312 NO_ROUTE".into()),
        ..Drained::default()
    };
    assert!(returned
        .into_result()
        .unwrap_err()
        .to_string()
        .contains("unroutable"));
    let nacked = Drained {
        nacked: 2,
        ..Drained::default()
    };
    assert!(nacked.into_result().unwrap_err().is_transient());
}

#[test]
fn contract_follows_delivery_mode() {
    let sink = RabbitMqSink::new(Arc::new(Schema::empty()), None);
    let mut config = ConnectorConfig::new("rabbitmq");
    config.set("url", "amqp://127.0.0.1/%2f");
    config.set("routing.key", "q");
    assert_eq!(
        sink.contract(&config).unwrap().consistency,
        SinkConsistency::DurableAtLeastOnce
    );
    config.set("delivery.mode", "transient");
    assert_eq!(
        sink.contract(&config).unwrap().consistency,
        SinkConsistency::Ephemeral
    );
}
