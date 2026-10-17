use super::*;
use arrow_array::{BinaryArray, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use mqi::properties::Value;
use std::sync::Arc;

fn parsed(pairs: &[(&str, &str)]) -> IbmMqSinkConfig {
    let mut config = ConnectorConfig::new("ibmmq");
    config.set("queue", "DEV.QUEUE.1");
    for (key, value) in pairs {
        config.set(*key, *value);
    }
    IbmMqSinkConfig::from_config(&config).unwrap()
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("id", DataType::Binary, true),
        Field::new("correl", DataType::Utf8, true),
        Field::new("prio", DataType::Int64, true),
        Field::new("tenant", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from(vec![&[0xff_u8, 0x00][..], &[0x80][..]])),
            Arc::new(BinaryArray::from(vec![Some(&[1_u8; 24][..]), None])),
            Arc::new(StringArray::from(vec![Some("c-1"), None])),
            Arc::new(Int64Array::from(vec![Some(3), None])),
            Arc::new(Int64Array::from(vec![Some(42), None])),
        ],
    )
    .unwrap()
}

fn mapped() -> IbmMqSinkConfig {
    parsed(&[
        ("format", "raw"),
        ("message.id.column", "id"),
        ("correlation.id.column", "correl"),
        ("priority.column", "prio"),
        ("property.columns", "tenant"),
    ])
}

#[test]
fn rows_become_binary_descriptors_and_typed_properties() {
    let config = mapped();
    let batch = batch();
    let writer = IbmMqSink::writer(&config, &batch.schema()).unwrap();
    assert_eq!(writer.payload_indices, vec![0]);
    assert_eq!(writer.base.Persistence, mqi::constants::MQPER_PERSISTENT.0);
    assert_eq!(
        super::super::metadata::text(&writer.base.Format),
        "",
        "binary is MQFMT_NONE"
    );
    let payloads = writer
        .serializer
        .serialize(&batch.project(&writer.payload_indices).unwrap())
        .unwrap();
    let messages = message::messages(&config, &writer.base, &batch, payloads).unwrap();
    assert_eq!(messages[0].data, vec![0xff, 0x00]);
    assert_eq!(messages[0].md.MsgId, [1; 24]);
    assert!(!messages[0].new_message_id);
    assert_eq!(&messages[0].md.CorrelId[..3], b"c-1");
    assert_eq!(messages[0].md.Priority, 3);
    assert!(
        matches!(messages[0].properties.as_slice(), [(name, Value::Int64(42))] if name == "tenant")
    );
    assert!(
        messages[1].new_message_id,
        "a NULL id asks the queue manager for a new one"
    );
    assert_eq!(
        messages[1].md.Priority,
        mqi::constants::MQPRI_PRIORITY_AS_Q_DEF.0
    );
    assert!(messages[1].properties.is_empty());
}

#[test]
fn text_payloads_are_mqstr_in_the_configured_ccsid() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let writer =
        IbmMqSink::writer(&parsed(&[("format", "json"), ("ccsid", "819")]), &schema).unwrap();
    assert_eq!(super::super::metadata::text(&writer.base.Format), "MQSTR");
    assert_eq!(writer.base.CodedCharSetId, 819);
}

#[test]
fn invalid_identities_and_priorities_fail_before_any_put() {
    let config = mapped();
    let schema = batch().schema();
    let rows = |id: &[u8], prio: i64| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BinaryArray::from(vec![&b"x"[..]])),
                Arc::new(BinaryArray::from(vec![Some(id)])),
                Arc::new(StringArray::from(vec![Some("c")])),
                Arc::new(Int64Array::from(vec![Some(prio)])),
                Arc::new(Int64Array::from(vec![Some(1)])),
            ],
        )
        .unwrap()
    };
    let writer = IbmMqSink::writer(&config, &schema).unwrap();
    let base = writer.base;
    assert!(message::messages(&config, &base, &rows(&[0; 25], 1), vec![b"x".to_vec()]).is_err());
    assert!(message::messages(&config, &base, &rows(&[0; 24], 10), vec![b"x".to_vec()]).is_err());
    assert!(message::messages(&config, &base, &rows(&[0; 24], 9), vec![b"x".to_vec()]).is_ok());
    let wrong_type = parsed(&[("format", "raw"), ("message.id.column", "prio")]);
    assert!(IbmMqSink::writer(&wrong_type, &schema).is_err());
}

#[test]
fn contract_is_durable_only_for_explicitly_persistent_messages() {
    let sink = IbmMqSink::new(Arc::new(Schema::empty()), None);
    let mut config = ConnectorConfig::new("ibmmq");
    config.set("queue", "Q");
    assert_eq!(
        sink.contract(&config).unwrap().consistency,
        SinkConsistency::DurableAtLeastOnce
    );
    config.set("persistence", "as-queue-definition");
    assert_eq!(
        sink.contract(&config).unwrap().consistency,
        SinkConsistency::Ephemeral
    );
}
