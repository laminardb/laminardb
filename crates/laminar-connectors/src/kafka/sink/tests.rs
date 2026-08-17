use super::*;
use crate::error::SerdeError;
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};

struct MismatchedSerializer;

impl RecordSerializer for MismatchedSerializer {
    fn serialize(&self, _batch: &arrow_array::RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
        Ok(vec![b"one-payload".to_vec()])
    }

    fn format(&self) -> Format {
        Format::Json
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn test_config() -> KafkaSinkConfig {
    let mut cfg = KafkaSinkConfig::default();
    cfg.bootstrap_servers = "localhost:9092".into();
    cfg.topic = "output-events".into();
    cfg
}

fn two_row_batch() -> arrow_array::RecordBatch {
    arrow_array::RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap()
}

#[test]
fn test_new_defaults() {
    let sink = KafkaSink::new(test_schema(), test_config(), None);
    assert_eq!(sink.state(), ConnectorState::Created);
    assert!(sink.producer.is_none());
    assert_eq!(sink.topic_partition_count, None);
}

#[test]
fn local_producer_creation_failure_is_terminal_configuration() {
    let mut invalid = ClientConfig::new();
    invalid.set("laminardb.invalid.kafka.property", "value");
    let Err(error) = invalid.create::<FutureProducer>() else {
        panic!("an unknown local librdkafka option must fail client creation");
    };

    let mapped = producer_creation_error("main", &error);
    assert!(matches!(mapped, ConnectorError::ConfigurationError(_)));
    assert!(!mapped.is_transient());
}

#[test]
fn malformed_broker_metadata_is_terminal_and_has_no_partition_fallback() {
    let error = invalid_response("orders", "metadata response omitted the topic");
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(!error.is_transient());

    let sink = KafkaSink::new(test_schema(), test_config(), None);
    assert_eq!(sink.topic_partition_count, None);
}

#[tokio::test]
async fn append_cardinality_mismatch_fails_before_producer_access() {
    let mut sink = KafkaSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;
    sink.topic_partition_count = Some(3);
    sink.serializer = Box::new(MismatchedSerializer);

    let error = sink.write_batch(&two_row_batch()).await.unwrap_err();
    assert!(matches!(
        error,
        ConnectorError::Serde(SerdeError::RecordCountMismatch {
            expected: 2,
            got: 1
        })
    ));
}

#[tokio::test]
async fn upsert_cardinality_mismatch_fails_before_producer_access() {
    let mut config = test_config();
    config.envelope = SinkEnvelope::Upsert;
    config.key_column = Some("id".into());
    let mut sink = KafkaSink::new(test_schema(), config, None);
    sink.state = ConnectorState::Running;
    sink.topic_partition_count = Some(3);
    sink.serializer = Box::new(MismatchedSerializer);

    let error = sink.write_batch(&two_row_batch()).await.unwrap_err();
    assert!(matches!(
        error,
        ConnectorError::Serde(SerdeError::RecordCountMismatch {
            expected: 2,
            got: 1
        })
    ));
}

#[tokio::test]
async fn schema_registration_preserves_terminal_registry_error() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/subjects/output-events-value/versions"))
        .respond_with(ResponseTemplate::new(422).set_body_string("invalid schema"))
        .mount(&server)
        .await;
    let mut config = test_config();
    config.format = Format::Avro;
    config.schema_registry_url = Some(server.uri());
    let registry = SchemaRegistryClient::new(server.uri(), None).unwrap();
    let mut sink = KafkaSink::with_schema_registry(test_schema(), config, registry);

    let error = sink.ensure_schema_ready(&test_schema()).await.unwrap_err();
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(!error.is_transient());
    assert!(error.to_string().contains("output-events-value"));
}

#[tokio::test]
async fn compatibility_put_preserves_terminal_registry_error() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/config/output-events-value"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid credentials"))
        .mount(&server)
        .await;
    let mut config = test_config();
    config.format = Format::Avro;
    config.schema_registry_url = Some(server.uri());
    config.schema_compatibility = Some(crate::kafka::config::CompatibilityLevel::Backward);
    let registry = SchemaRegistryClient::new(server.uri(), None).unwrap();
    let mut sink = KafkaSink::with_schema_registry(test_schema(), config, registry);

    let error = sink.open(&ConnectorConfig::new("kafka")).await.unwrap_err();
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(!error.is_transient());
    assert!(error.to_string().contains("output-events-value"));
}

#[test]
fn terminal_tracker_seals_when_sink_is_dropped() {
    let sink = KafkaSink::new(test_schema(), test_config(), None);
    let terminal = sink.terminal_task_tracker().unwrap();
    assert!(!terminal.is_terminated());
    drop(sink);
    assert!(terminal.is_terminated());
}

#[test]
fn test_schema_returned() {
    let schema = test_schema();
    let sink = KafkaSink::new(schema.clone(), test_config(), None);
    assert_eq!(sink.schema(), schema);
}

#[test]
fn contract_is_multi_writer_durable_at_least_once() {
    let sink = KafkaSink::new(test_schema(), test_config(), None);
    let contract = sink.contract(&ConnectorConfig::new("kafka")).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.topology, SinkTopology::MultiWriter);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert!(!contract.is_cluster_exact_delivery_certified());
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(126));
}

#[test]
fn delivery_error_codes_never_claim_non_persistence_without_native_status() {
    for code in [
        RDKafkaErrorCode::MessageTimedOut,
        RDKafkaErrorCode::TimedOutQueue,
        RDKafkaErrorCode::PurgeQueue,
        RDKafkaErrorCode::PurgeInflight,
        RDKafkaErrorCode::MessageSizeTooLarge,
        RDKafkaErrorCode::TopicAuthorizationFailed,
    ] {
        let error = KafkaError::MessageProduction(code);
        assert_eq!(
            KafkaFailure::delivery(&error, "test").certainty,
            KafkaFailureCertainty::OutcomeUnknown,
            "delivery code {code:?}"
        );
    }
}

#[test]
fn only_terminal_record_local_enqueue_failures_are_dlq_eligible() {
    let too_large = KafkaFailure::enqueue(
        &KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge),
        "test",
    );
    assert_eq!(
        too_large.certainty,
        KafkaFailureCertainty::DefinitelyNotPersisted
    );
    assert_eq!(too_large.scope, KafkaFailureScope::Record);
    assert!(!too_large.retryable);
    assert!(too_large.dlq_eligible());

    let queue_full = KafkaFailure::enqueue(
        &KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
        "test",
    );
    assert_eq!(queue_full.scope, KafkaFailureScope::Infrastructure);
    assert!(queue_full.retryable);
    assert!(!queue_full.dlq_eligible());

    let unauthorized = KafkaFailure::enqueue(
        &KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed),
        "test",
    );
    assert_eq!(unauthorized.scope, KafkaFailureScope::Connector);
    assert!(!unauthorized.retryable);
    assert!(!unauthorized.dlq_eligible());
}

#[test]
fn fatal_and_unknown_codes_fail_closed() {
    for code in [
        RDKafkaErrorCode::Unknown,
        RDKafkaErrorCode::Fatal,
        RDKafkaErrorCode::ProducerFenced,
    ] {
        let failure = KafkaFailure::delivery(&KafkaError::MessageProduction(code), "test");
        assert_eq!(failure.scope, KafkaFailureScope::Connector);
        assert!(!failure.retryable, "delivery code {code:?}");
    }
}

#[test]
fn aggregate_retryability_is_the_conjunction_of_every_failure() {
    let transient = KafkaFailure::delivery(
        &KafkaError::MessageProduction(RDKafkaErrorCode::RequestTimedOut),
        "test",
    );
    let terminal = KafkaFailure::enqueue(
        &KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge),
        "test",
    );
    let mut definitely_not_persisted = 0;
    let mut ambiguous = 0;
    let mut first_error = None;
    let mut retryable = true;
    record_failure(
        &transient,
        1,
        &mut definitely_not_persisted,
        &mut ambiguous,
        &mut first_error,
        &mut retryable,
    );
    record_failure(
        &terminal,
        2,
        &mut definitely_not_persisted,
        &mut ambiguous,
        &mut first_error,
        &mut retryable,
    );

    assert_eq!(definitely_not_persisted, 2);
    assert_eq!(ambiguous, 1);
    assert!(!retryable);
}

#[test]
fn suggested_timeout_tracks_driver_deadline_with_constant_headroom() {
    let mut config = test_config();
    config.delivery_timeout = Duration::from_secs(42);
    let sink = KafkaSink::new(test_schema(), config, None);
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(48));
}

#[test]
fn queue_retry_wait_is_bounded_across_records() {
    let start = Instant::now();
    let deadline = start + QUEUE_RETRY_TIMEOUT;
    let mut now = start;
    let mut total_wait = Duration::ZERO;

    for _ in 0..32 {
        if let Some(delay) = queue_retry_delay(deadline, now) {
            total_wait += delay;
            now += delay;
        }
    }

    assert_eq!(total_wait, QUEUE_RETRY_TIMEOUT);
    assert_eq!(now, deadline);
    assert_eq!(queue_retry_delay(deadline, now), None);
}

#[test]
fn later_record_cannot_restart_an_expired_queue_retry_budget() {
    let start = Instant::now();
    let deadline = start + QUEUE_RETRY_TIMEOUT;

    assert_eq!(
        queue_retry_delay(
            deadline,
            deadline.checked_sub(Duration::from_millis(25)).unwrap(),
        ),
        Some(Duration::from_millis(25))
    );
    assert_eq!(
        queue_retry_delay(deadline, deadline + Duration::from_secs(1)),
        None
    );
}

#[test]
fn partial_or_ambiguous_batch_requires_generation_retirement() {
    let error = unresolved_delivery_error("produce", 3, 1, 2, 0, Some("rejected".into()), false);
    assert!(error.is_outcome_unknown());
    assert!(!error.is_transient());

    let error = unresolved_delivery_error("produce", 1, 0, 0, 1, Some("timed out".into()), true);
    assert!(error.is_outcome_unknown());
    assert!(error.is_transient());

    let error = unresolved_delivery_error("produce", 1, 0, 1, 0, Some("too large".into()), false);
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
}

#[test]
fn upsert_contract_requires_singleton_writer() {
    let mut config = test_config();
    config.envelope = SinkEnvelope::Upsert;
    config.key_column = Some("id".into());
    let sink = KafkaSink::new(test_schema(), config, None);

    let contract = sink.contract(&ConnectorConfig::new("kafka")).unwrap();

    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.topology, SinkTopology::Singleton);
    assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
}

#[test]
fn test_serializer_selection_json() {
    let sink = KafkaSink::new(test_schema(), test_config(), None);
    assert_eq!(sink.serializer.format(), Format::Json);
}

#[test]
fn test_serializer_selection_avro() {
    let mut cfg = test_config();
    cfg.format = Format::Avro;
    let sink = KafkaSink::new(test_schema(), cfg, None);
    assert_eq!(sink.serializer.format(), Format::Avro);
}

#[test]
fn test_with_schema_registry() {
    let sr = SchemaRegistryClient::new("http://localhost:8081", None).unwrap();
    let mut cfg = test_config();
    cfg.format = Format::Avro;
    cfg.schema_registry_url = Some("http://localhost:8081".into());

    let sink = KafkaSink::with_schema_registry(test_schema(), cfg, sr);
    assert!(sink.has_schema_registry());
    assert_eq!(sink.serializer.format(), Format::Avro);
}

#[test]
fn test_debug_output() {
    let sink = KafkaSink::new(test_schema(), test_config(), None);
    let debug = format!("{sink:?}");
    assert!(debug.contains("KafkaSink"));
    assert!(debug.contains("output-events"));
}

#[test]
fn test_extract_keys_no_key_column() {
    let sink = KafkaSink::new(test_schema(), test_config(), None);
    let batch = arrow_array::RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    assert!(sink.extract_keys(&batch).unwrap().is_none());
}

#[test]
fn test_extract_keys_with_key_column() {
    let mut cfg = test_config();
    cfg.key_column = Some("value".into());
    let sink = KafkaSink::new(test_schema(), cfg, None);
    let batch = arrow_array::RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["key-a", "key-b"])),
        ],
    )
    .unwrap();
    let keys = sink.extract_keys(&batch).unwrap().unwrap();
    assert_eq!(&keys[0], b"key-a");
    assert_eq!(&keys[1], b"key-b");
}
