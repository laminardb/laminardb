//! Native broker traffic and independent Avro decoding using the existing test services.
#![cfg(feature = "kafka")]

use std::sync::Arc;
use std::time::Duration;

use apache_avro::types::Value;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::registry::ConnectorRegistry;
use laminar_core::checkpoint::CheckpointAttempt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const READER: &str = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"label","type":"string","default":"missing"}]}"#;
const OLD: &str = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#;
const REORDERED: &str = r#"{"type":"record","name":"Event","fields":[{"name":"label","type":"string"},{"name":"extra","type":"boolean"},{"name":"id","type":"long"}]}"#;
const BUDGET: Duration = Duration::from_secs(30);

fn registry() -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    laminar_connectors::kafka::register_kafka_source(&registry).unwrap();
    laminar_connectors::kafka::register_kafka_sink(&registry).unwrap();
    registry
}

async fn topic() -> (String, String) {
    let brokers =
        std::env::var("LAMINAR_SCHEMA_TEST_KAFKA").expect("set LAMINAR_SCHEMA_TEST_KAFKA");
    let name = format!("schema-contract-{}", uuid::Uuid::now_v7().simple());
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new(&name, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(BUDGET)),
        )
        .await
        .unwrap();
    assert!(created.into_iter().all(|result| result.is_ok()));
    (brokers, name)
}

fn config(brokers: &str, topic: &str, server: &MockServer) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", brokers);
    config.set("topic", topic);
    config.set("group.id", format!("{topic}-reader"));
    config.set("laminar.source.name", "schema_contract_source");
    config.set("startup.mode", "earliest");
    config.set("format", "avro");
    config.set("schema.registry.url", server.uri());
    config.set("max.poll.records", "64");
    config.set("reader.channel.capacity", "128");
    config.set("schema.evolution.strategy", "ignore");
    config
}

async fn latest(server: &MockServer, topic: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/subjects/{topic}-value/versions/latest")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id":7,"version":2,"schema":READER,"schemaType":"AVRO"
        })))
        .expect(1)
        .mount(server)
        .await;
}

async fn writer(server: &MockServer, id: i32, schema: &str, delay: Duration) {
    Mock::given(method("GET"))
        .and(path(format!("/schemas/ids/{id}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(delay)
                .set_body_json(serde_json::json!({"schema":schema,"schemaType":"AVRO"})),
        )
        .expect(1)
        .mount(server)
        .await;
}

fn wire(schema: &str, id: i32, values: Vec<(&str, Value)>) -> Vec<u8> {
    let native = apache_avro::Schema::parse_str(schema).unwrap();
    let mut bytes = vec![0];
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend(
        apache_avro::to_avro_datum(
            &native,
            Value::Record(
                values
                    .into_iter()
                    .map(|(name, value)| (name.to_owned(), value))
                    .collect(),
            ),
        )
        .unwrap(),
    );
    bytes
}

fn producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap()
}

async fn send(producer: &FutureProducer, topic: &str, bytes: &[u8]) {
    producer
        .send(
            FutureRecord::<str, [u8]>::to(topic)
                .partition(0)
                .payload(bytes),
            BUDGET,
        )
        .await
        .unwrap();
}

async fn next_batch(source: &mut dyn SourceConnector) -> RecordBatch {
    tokio::time::timeout(BUDGET, async {
        loop {
            if let Some(batch) = source.poll_batch(64).await.unwrap() {
                return batch.records;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("broker batch within the existing integration budget")
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_KAFKA"]
async fn empty_topic_resolves_before_consumption_and_historical_writers_keep_order() {
    let (brokers, topic) = topic().await;
    let server = MockServer::start().await;
    latest(&server, &topic).await;
    writer(&server, 3, OLD, Duration::ZERO).await;
    writer(&server, 11, REORDERED, Duration::ZERO).await;
    let registry = registry();
    let mut config = config(&brokers, &topic, &server);
    let binding = registry.resolve_source_schema(&config, None).await.unwrap();
    assert_eq!(binding.logical.field(0).name(), "id");
    assert_eq!(binding.logical.fields().len(), 2);
    config.set_schema_binding(binding.clone()).unwrap();
    let mut source = registry.create_source(&config, None).unwrap();
    assert!(
        source.checkpoint().is_empty(),
        "metadata resolution cannot create accepted progress"
    );
    source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(source.poll_batch(64).await.unwrap().is_none());
    assert_eq!(
        source.checkpoint().get_offset(&format!("{topic}:0")),
        None,
        "sealed initial metadata is not accepted record progress"
    );

    let producer = producer(&brokers);
    for id in 0..128_i64 {
        let bytes = if id % 2 == 0 {
            wire(OLD, 3, vec![("id", Value::Long(id))])
        } else {
            wire(
                REORDERED,
                11,
                vec![
                    ("label", Value::String(format!("row-{id}"))),
                    ("extra", Value::Boolean(true)),
                    ("id", Value::Long(id)),
                ],
            )
        };
        send(&producer, &topic, &bytes).await;
    }
    let mut observed = 0_i64;
    while observed < 128 {
        let batch = next_batch(source.as_mut()).await;
        assert_eq!(batch.schema().as_ref(), &binding.logical);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(ids.value(row), observed);
            assert_eq!(
                labels.value(row),
                if observed % 2 == 0 {
                    "missing".to_owned()
                } else {
                    format!("row-{observed}")
                }
            );
            observed += 1;
        }
    }
    assert_eq!(
        source.checkpoint().get_offset(&format!("{topic}:0")),
        Some("127")
    );
    source.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_KAFKA"]
async fn sink_uses_query_mapping_and_committed_identity_without_registry_mutation_on_restart() {
    let (brokers, topic) = topic().await;
    let server = MockServer::start().await;
    latest(&server, &topic).await;
    let registry = registry();
    let mut config = config(&brokers, &topic, &server);
    let input = Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    config.set("_arrow_schema", encode_arrow_schema_ipc(&input));
    let binding = registry
        .resolve_sink_schema(&config, input.clone())
        .await
        .unwrap();
    config.set_schema_binding(binding).unwrap();
    let batch = RecordBatch::try_new(
        input,
        vec![
            Arc::new(StringArray::from(vec!["mapped"])),
            Arc::new(Int64Array::from(vec![84])),
        ],
    )
    .unwrap();
    for _ in 0..2 {
        let mut sink = registry.create_sink(&config, None).unwrap();
        sink.open(&config).await.unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.flush().await.unwrap();
        sink.close().await.unwrap();
    }
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("{topic}-independent"))
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut positions = TopicPartitionList::new();
    positions
        .add_partition_offset(&topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&positions).unwrap();
    let native = apache_avro::Schema::parse_str(READER).unwrap();
    for _ in 0..2 {
        let message = tokio::time::timeout(BUDGET, consumer.recv())
            .await
            .unwrap()
            .unwrap();
        let bytes = message.payload().unwrap();
        assert_eq!(&bytes[..5], &[0, 0, 0, 0, 7]);
        let values = apache_avro::from_avro_datum(&native, &mut &bytes[5..], None).unwrap();
        assert_eq!(
            values,
            Value::Record(vec![
                ("id".into(), Value::Long(84)),
                ("label".into(), Value::String("mapped".into()))
            ])
        );
    }
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "restart must use the committed writer without selecting latest or registering"
    );
    assert!(requests.iter().all(|request| request.method == "GET"));
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_KAFKA"]
async fn unavailable_unknown_writer_holds_progress_and_recovery_replays_sustained_input() {
    let (brokers, topic) = topic().await;
    let server = MockServer::start().await;
    latest(&server, &topic).await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/77"))
        .respond_with(ResponseTemplate::new(503))
        .expect(3)
        .mount(&server)
        .await;
    let registry = registry();
    let mut config = config(&brokers, &topic, &server);
    config
        .set_schema_binding(registry.resolve_source_schema(&config, None).await.unwrap())
        .unwrap();
    let metrics = Arc::new(prometheus::Registry::new());
    let mut source = registry.create_source(&config, Some(&metrics)).unwrap();
    source
        .start(
            SourceStart::new(
                config.clone(),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let producer = producer(&brokers);
    send(
        &producer,
        &topic,
        &wire(
            READER,
            7,
            vec![
                ("id", Value::Long(0)),
                ("label", Value::String("initial".into())),
            ],
        ),
    )
    .await;
    assert_eq!(next_batch(source.as_mut()).await.num_rows(), 1);
    let committed = source.try_checkpoint().unwrap().unwrap();
    assert_eq!(committed.get_offset(&format!("{topic}:0")), Some("0"));
    for id in 1..=1024_i64 {
        send(
            &producer,
            &topic,
            &wire(OLD, 77, vec![("id", Value::Long(id))]),
        )
        .await;
    }
    let error = tokio::time::timeout(BUDGET, async {
        loop {
            match source.poll_batch(64).await {
                Err(error) => break error,
                Ok(Some(batch)) => panic!(
                    "unresolved writer emitted {} rows",
                    batch.records.num_rows()
                ),
                Ok(None) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .unwrap();
    assert!(error.to_string().contains("503"), "{error}");
    let retained = source.try_checkpoint().unwrap().unwrap();
    assert_eq!(retained.durable_offsets(), committed.durable_offsets());
    assert!(source.poll_batch(64).await.is_err());
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/schemas/ids/77")
            .count(),
        3,
        "an unavailable writer receives only the bounded retry budget"
    );
    source.close().await.unwrap();
    server.reset().await;
    writer(&server, 77, OLD, Duration::from_millis(500)).await;

    let mut recovered = registry
        .create_source(&config, Some(&Arc::new(prometheus::Registry::new())))
        .unwrap();
    recovered
        .start(
            SourceStart::new(
                config,
                SourcePosition::Resume {
                    attempt: CheckpointAttempt::canonical(1),
                    checkpoint: committed,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut next = 1_i64;
    while next <= 1024 {
        let batch = next_batch(recovered.as_mut()).await;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for id in ids.values() {
            assert_eq!(
                *id, next,
                "unresolved records must preserve partition order and replay without omission"
            );
            next += 1;
        }
    }
    assert_eq!(
        recovered.checkpoint().get_offset(&format!("{topic}:0")),
        Some("1024")
    );
    recovered.close().await.unwrap();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "bounded writer cache must fetch the repeated unknown identity once"
    );
    assert_eq!(requests[0].url.path(), "/schemas/ids/77");
}
