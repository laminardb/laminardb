use super::*;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::{DataType, Field, Schema};

fn delivery(tag: u64, data: &[u8]) -> Delivery {
    Delivery::mock(
        tag,
        "ex".into(),
        format!("rk.{tag}").into(),
        tag.is_multiple_of(2),
        data.to_vec(),
    )
}

fn decoder(format: serde::Format, pairs: &[(&str, &str)], payload: SchemaRef) -> Decoder {
    let mut config = ConnectorConfig::new("rabbitmq");
    config.set("url", "amqp://127.0.0.1/%2f");
    config.set("queue", "q");
    config.set("format", format.as_str());
    for (key, value) in pairs {
        config.set(*key, *value);
    }
    let parsed = RabbitMqSourceConfig::from_config(&config).unwrap();
    Decoder {
        deserializer: serde::create_deserializer(format).unwrap(),
        output: parsed.metadata.output_schema(&payload),
        payload,
        config: parsed,
    }
}

fn ids() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn id_values(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

#[test]
fn ledger_acknowledges_only_committed_tags_of_its_own_generation() {
    let mut ledger = AckLedger::new(7);
    assert_eq!(
        ledger.commit_target(&SourceCheckpoint::new()).unwrap(),
        None
    );
    ledger.hand_off(3, 3);
    let first = ledger.cursor();
    ledger.hand_off(5, 2);
    assert_eq!(ledger.unacked(), 5);
    assert_eq!(ledger.commit_target(&first).unwrap(), Some(3));
    assert_eq!(ledger.acknowledge(3), 3);
    assert_eq!(ledger.unacked(), 2);
    // A superseded or replayed notification never acknowledges a settled tag twice.
    assert_eq!(ledger.commit_target(&first).unwrap(), None);
    let stale = AckLedger {
        generation: 8,
        ..AckLedger::new(8)
    }
    .cursor();
    assert_eq!(ledger.commit_target(&stale).unwrap(), None);
    let mut ahead = ledger.cursor();
    ahead.set_offset(DELIVERY_TAG, "9");
    assert!(ledger.commit_target(&ahead).is_err());
    let mut corrupt = ledger.cursor();
    corrupt.set_offset(GENERATION, "not-hex");
    assert!(ledger.commit_target(&corrupt).is_err());
    assert_eq!(ledger.acknowledge(5), 2);
    assert_eq!(ledger.unacked(), 0);
}

#[test]
fn json_batches_carry_per_message_metadata() {
    let codec = decoder(
        serde::Format::Json,
        &[("metadata.columns", "routing_key,redelivered")],
        ids(),
    );
    let messages = [delivery(1, br#"{"id":10}"#), delivery(2, br#"{"id":20}"#)];
    let decoded = codec.decode(&messages).unwrap();
    let records = decoded.records.unwrap();
    assert_eq!(id_values(&records), vec![10, 20]);
    assert_eq!(records.schema().field(1).name(), "_routing_key");
    assert_eq!(records.column(1).as_string::<i32>().value(1), "rk.2");
    assert!(records.column(2).as_boolean().value(1));
    assert!(decoded.rejected.is_empty());
}

#[test]
fn multi_row_and_empty_messages_keep_their_boundaries() {
    let decoder = decoder(
        serde::Format::Csv,
        &[("metadata.columns", "routing_key")],
        ids(),
    );
    let deliveries = [delivery(1, b"1\n2\n3"), delivery(2, b""), delivery(3, b"4")];
    let records = decoder.decode(&deliveries).unwrap().records.unwrap();
    assert_eq!(id_values(&records), vec![1, 2, 3, 4]);
    let keys: Vec<&str> = records
        .column(1)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert_eq!(keys, vec!["rk.1", "rk.1", "rk.1", "rk.3"]);
}

#[test]
fn undecodable_messages_fail_or_are_isolated_by_policy() {
    let deliveries = [
        delivery(1, br#"{"id":1}"#),
        delivery(2, b"not json"),
        delivery(3, br#"{"id":3}"#),
    ];
    let failing = decoder(serde::Format::Json, &[], ids());
    assert!(failing.decode(&deliveries).is_err());
    let rejecting = decoder(serde::Format::Json, &[("on.decode.error", "reject")], ids());
    let decoded = rejecting.decode(&deliveries).unwrap();
    assert_eq!(decoded.rejected, vec![2]);
    assert_eq!(id_values(&decoded.records.unwrap()), vec![1, 3]);
    let all_bad = rejecting.decode(&deliveries[1..2]).unwrap();
    assert!(all_bad.records.is_none());
    assert_eq!(all_bad.rejected, vec![2]);
}

#[test]
fn binary_payloads_are_preserved_byte_for_byte() {
    let payload = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        false,
    )]));
    let decoder = decoder(serde::Format::Raw, &[], payload);
    let bytes: &[u8] = &[0xff, 0x00, 0xfe, 0x80];
    let records = decoder
        .decode(&[delivery(1, bytes)])
        .unwrap()
        .records
        .unwrap();
    assert_eq!(records.column(0).as_binary::<i32>().value(0), bytes);
}

#[test]
fn stream_queue_deliveries_are_detected() {
    let mut stream = delivery(1, b"x");
    let mut headers = lapin::types::FieldTable::default();
    headers.insert(
        "x-stream-offset".into(),
        lapin::types::AMQPValue::LongLongInt(0),
    );
    stream.properties = lapin::BasicProperties::default().with_headers(headers);
    assert!(is_stream_delivery(&stream));
    assert!(!is_stream_delivery(&delivery(2, b"x")));
}
