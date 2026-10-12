//! Kafka source wire fidelity against a real broker: binary, empty and NULL keys, NULL values,
//! ordered duplicate and binary headers, timestamps, and a non-UTF-8 header name.
//!
//! It skips when the default local address is absent, but a configured `LAMINAR_KAFKA_BROKERS`
//! must be reachable. Run with:
//!   LAMINAR_KAFKA_BROKERS=127.0.0.1:19092 \
//!     cargo test -p laminar-connectors --features kafka --test kafka_record_fidelity

#![cfg(feature = "kafka")]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::kafka::{KafkaSource, KafkaSourceConfig};

const DEFAULT_BROKERS: &str = "127.0.0.1:19092";

fn broker() -> Result<Option<String>, String> {
    let configured = std::env::var("LAMINAR_KAFKA_BROKERS").ok();
    let brokers = configured.clone().unwrap_or_else(|| DEFAULT_BROKERS.into());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set(
            "group.id",
            format!("fidelity-probe-{}", uuid::Uuid::new_v4()),
        )
        .create()
        .map_err(|error| format!("invalid Kafka broker configuration '{brokers}': {error}"))?;
    match consumer.fetch_metadata(None, Duration::from_secs(1)) {
        Ok(metadata) if !metadata.brokers().is_empty() => Ok(Some(brokers)),
        Ok(_) | Err(_) if configured.is_none() => Ok(None),
        Ok(_) => Err(format!(
            "configured Kafka broker '{brokers}' returned no brokers"
        )),
        Err(error) => Err(format!(
            "configured Kafka broker '{brokers}' is unreachable: {error}"
        )),
    }
}

async fn create_topic(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let results = admin
        .create_topics(
            [&NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics request");
    for result in results {
        result.expect("create topic");
    }
}

fn producer(brokers: &str) -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("enable.idempotence", "true")
        .create()
        .expect("producer")
}

fn produce(
    producer: &BaseProducer,
    topic: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: Option<OwnedHeaders>,
) {
    let mut record: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(topic)
        .partition(0)
        .timestamp(1_700_000_000_000);
    if let Some(key) = key {
        record = record.key(key);
    }
    if let Some(value) = value {
        record = record.payload(value);
    }
    if let Some(headers) = headers {
        record = record.headers(headers);
    }
    producer
        .send(record)
        .map_err(|(error, _)| error)
        .expect("enqueue");
    producer.flush(Duration::from_secs(10)).expect("flush");
}

/// Produce one record whose header name is not UTF-8. The safe rdkafka API only accepts `&str`
/// header names, so this goes through librdkafka's `rd_kafka_produceva`.
fn produce_non_utf8_header_name(producer: &BaseProducer, topic: &str) {
    use rdkafka::bindings::{
        rd_kafka_error_destroy, rd_kafka_produceva, rd_kafka_vtype_t, rd_kafka_vu_s__bindgen_ty_1,
        rd_kafka_vu_s__bindgen_ty_1__bindgen_ty_1, rd_kafka_vu_s__bindgen_ty_1__bindgen_ty_2,
        rd_kafka_vu_t,
    };
    let topic = std::ffi::CString::new(topic).unwrap();
    let value = br#"{"id":9,"value":"bad-header"}"#;
    let name = b"\xff\xfe\0";
    let header_value = b"v";
    let vus = [
        rd_kafka_vu_t {
            vtype: rd_kafka_vtype_t::RD_KAFKA_VTYPE_TOPIC,
            u: rd_kafka_vu_s__bindgen_ty_1 {
                cstr: topic.as_ptr(),
            },
        },
        rd_kafka_vu_t {
            vtype: rd_kafka_vtype_t::RD_KAFKA_VTYPE_PARTITION,
            u: rd_kafka_vu_s__bindgen_ty_1 { i32_: 0 },
        },
        rd_kafka_vu_t {
            vtype: rd_kafka_vtype_t::RD_KAFKA_VTYPE_VALUE,
            u: rd_kafka_vu_s__bindgen_ty_1 {
                mem: rd_kafka_vu_s__bindgen_ty_1__bindgen_ty_1 {
                    ptr: value.as_ptr().cast_mut().cast(),
                    size: value.len(),
                },
            },
        },
        rd_kafka_vu_t {
            vtype: rd_kafka_vtype_t::RD_KAFKA_VTYPE_HEADER,
            u: rd_kafka_vu_s__bindgen_ty_1 {
                header: rd_kafka_vu_s__bindgen_ty_1__bindgen_ty_2 {
                    name: name.as_ptr().cast(),
                    val: header_value.as_ptr().cast(),
                    size: isize::try_from(header_value.len()).unwrap(),
                },
            },
        },
    ];
    // SAFETY: every pointer in `vus` refers to a live local for the duration of the call, the
    // name is NUL-terminated, and librdkafka copies the record before returning.
    let error =
        unsafe { rd_kafka_produceva(producer.client().native_ptr(), vus.as_ptr(), vus.len()) };
    if !error.is_null() {
        // SAFETY: a non-null error returned by `rd_kafka_produceva` is owned by the caller.
        unsafe { rd_kafka_error_destroy(error) };
        panic!("rd_kafka_produceva rejected the record");
    }
    producer.flush(Duration::from_secs(10)).expect("flush");
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

async fn started_source(brokers: &str, topic: &str, delivery: DeliveryGuarantee) -> KafkaSource {
    let mut config = ConnectorConfig::new("kafka");
    for (key, value) in [
        ("bootstrap.servers", brokers.to_string()),
        ("group.id", format!("fidelity-{}", uuid::Uuid::new_v4())),
        ("topic", topic.to_string()),
        ("startup.mode", "earliest".into()),
        ("format", "json".into()),
        (
            "metadata.columns",
            "key,topic,partition,offset,timestamp,timestamp_type,headers".into(),
        ),
        ("laminar.source.name", "fidelity".into()),
    ] {
        config.set(key, value);
    }
    let mut source = KafkaSource::new(schema(), KafkaSourceConfig::default(), None);
    source
        .start(SourceStart::new(config, SourcePosition::Initial, delivery).unwrap())
        .await
        .unwrap();
    source
}

/// Poll until the cursor for partition 0 has accepted `last_offset`, returning every emitted batch.
async fn poll_to(source: &mut KafkaSource, topic: &str, last_offset: i64) -> Vec<RecordBatch> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut batches = Vec::new();
    let key = format!("{topic}:0");
    while source
        .checkpoint()
        .get_offset(&key)
        .and_then(|offset| offset.parse::<i64>().ok())
        != Some(last_offset)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cursor never reached {last_offset}: {:?}, rows so far {}",
            source.checkpoint(),
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        );
        if let Some(batch) = source.poll_batch(100).await.unwrap() {
            batches.push(batch.records);
        } else {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    batches
}

#[tokio::test]
async fn source_preserves_record_bytes_and_advances_over_null_values() {
    let Some(brokers) = broker().unwrap() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topic = format!("fidelity-{}", uuid::Uuid::new_v4());
    create_topic(&brokers, &topic).await;
    let producer = producer(&brokers);
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "dup",
            value: Some(&[0x80_u8, 0x81][..]),
        })
        .insert(Header::<&[u8]> {
            key: "dup",
            value: None,
        })
        .insert(Header {
            key: "empty",
            value: Some(&b""[..]),
        });
    produce(
        &producer,
        &topic,
        Some(&[0xff, 0x00]),
        Some(br#"{"id":0,"value":"a"}"#),
        Some(headers),
    );
    produce(
        &producer,
        &topic,
        Some(b""),
        Some(br#"{"id":1,"value":"b"}"#),
        None,
    );
    produce(
        &producer,
        &topic,
        None,
        Some(br#"{"id":2,"value":"c"}"#),
        None,
    );
    produce(&producer, &topic, Some(b"k"), None, None);
    produce(&producer, &topic, Some(b"k"), None, None);

    let mut source = started_source(&brokers, &topic, DeliveryGuarantee::AtLeastOnce).await;
    let batches = poll_to(&mut source, &topic, 4).await;
    let rows = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(rows.num_rows(), 3, "NULL-value records produce no rows");

    let keys = rows.column_by_name("_key").unwrap().as_binary::<i32>();
    assert_eq!(keys.value(0), &[0xff, 0x00]);
    assert!(!keys.is_null(1));
    assert_eq!(keys.value(1), b"");
    assert!(keys.is_null(2));

    let offsets = rows
        .column_by_name("_offset")
        .unwrap()
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(offsets.values(), &[0, 1, 2]);
    let timestamps = rows
        .column_by_name("_timestamp")
        .unwrap()
        .as_primitive::<arrow_array::types::TimestampMillisecondType>();
    assert_eq!(timestamps.value(0), 1_700_000_000_000);
    assert_eq!(
        rows.column_by_name("_timestamp_type")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "create_time"
    );
    assert_eq!(
        rows.column_by_name("_topic")
            .unwrap()
            .as_string::<i32>()
            .value(0),
        topic
    );

    let headers = rows.column_by_name("_headers").unwrap().as_list::<i32>();
    let first = headers.value(0);
    let first = first.as_struct();
    let names = first.column(0).as_string::<i32>();
    let values = first.column(1).as_binary::<i32>();
    assert_eq!(
        names.iter().flatten().collect::<Vec<_>>(),
        ["dup", "dup", "empty"]
    );
    assert_eq!(values.value(0), &[0x80, 0x81]);
    assert!(values.is_null(1));
    assert!(!values.is_null(2));
    assert_eq!(values.value(2), b"");
    assert_eq!(headers.value(1).len(), 0);

    // A non-UTF-8 header name cannot become a row: a guaranteed source stops at it instead of
    // panicking or skipping it, and a best-effort source skips it with its offset.
    produce_non_utf8_header_name(&producer, &topic);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let error = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no rejection observed"
        );
        match source.poll_batch(100).await {
            Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(error) => break error,
        }
    };
    assert!(error.to_string().contains("not valid UTF-8"), "{error}");
    assert_eq!(
        source.checkpoint().get_offset(&format!("{topic}:0")),
        Some("4")
    );
    source.close().await.unwrap();

    let mut best_effort = started_source(&brokers, &topic, DeliveryGuarantee::BestEffort).await;
    let batches = poll_to(&mut best_effort, &topic, 5).await;
    let emitted = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(emitted, 3);
    best_effort.close().await.unwrap();
}
