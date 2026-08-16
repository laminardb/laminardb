use super::*;

fn sink_config(mode: Mode) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("nats");
    config.set("servers", "nats://localhost:4222");
    config.set("subject", "events");
    config.set(
        "mode",
        match mode {
            Mode::Core => "core",
            Mode::JetStream => "jetstream",
        },
    );
    if mode == Mode::JetStream {
        config.set("stream", "EVENTS");
    }
    config
}

fn durable_stream_config() -> jetstream::stream::Config {
    jetstream::stream::Config {
        name: "EVENTS".into(),
        storage: jetstream::stream::StorageType::File,
        num_replicas: MIN_DURABLE_REPLICAS,
        no_ack: false,
        ..Default::default()
    }
}

fn broker_error(code: u16) -> jetstream::Error {
    serde_json::from_value(serde_json::json!({
        "code": code,
        "err_code": 10008,
        "description": "test response"
    }))
    .unwrap()
}

#[test]
fn core_contract_is_ephemeral_multi_writer() {
    let sink = NatsSink::new(std::sync::Arc::new(arrow_schema::Schema::empty()), None);
    let contract = sink.contract(&sink_config(Mode::Core)).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
    assert_eq!(contract.topology, SinkTopology::MultiWriter);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
}

#[test]
fn sink_generation_exposes_terminal_task_proof() {
    let sink = NatsSink::new(std::sync::Arc::new(arrow_schema::Schema::empty()), None);
    let terminal = sink.terminal_task_tracker().unwrap();
    assert!(!terminal.is_terminated());
    assert_eq!(
        sink.cancellation_policy(),
        crate::connector::ConnectorCancellationPolicy::RetireConnector
    );

    drop(sink);

    assert!(terminal.is_terminated());
}

#[test]
fn named_jetstream_contract_is_durable_pending_open_validation() {
    let sink = NatsSink::new(std::sync::Arc::new(arrow_schema::Schema::empty()), None);
    let contract = sink.contract(&sink_config(Mode::JetStream)).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.topology, SinkTopology::MultiWriter);
}

#[test]
fn durable_stream_validation_accepts_file_quorum_with_acks() {
    validate_durable_stream("EVENTS", &durable_stream_config()).unwrap();
}

#[test]
fn durable_stream_validation_rejects_memory_storage() {
    let mut config = durable_stream_config();
    config.storage = jetstream::stream::StorageType::Memory;
    let error = validate_durable_stream("EVENTS", &config).unwrap_err();
    assert!(error.to_string().contains("LDB-5072"));
}

#[test]
fn durable_stream_validation_rejects_non_quorum_replication() {
    let mut config = durable_stream_config();
    config.num_replicas = MIN_DURABLE_REPLICAS - 1;
    let error = validate_durable_stream("EVENTS", &config).unwrap_err();
    assert!(error.to_string().contains("LDB-5073"));
}

#[test]
fn durable_stream_validation_rejects_disabled_acks() {
    let mut config = durable_stream_config();
    config.no_ack = true;
    let error = validate_durable_stream("EVENTS", &config).unwrap_err();
    assert!(error.to_string().contains("LDB-5074"));
}

#[tokio::test(start_paused = true)]
async fn enqueue_timeout_is_definite_for_current_message_and_bounded() {
    let timeout = Duration::from_millis(25);
    let started = tokio::time::Instant::now();
    let failure = bounded_publish_enqueue(
        started + timeout,
        timeout,
        std::future::pending::<Result<(), async_nats::PublishError>>(),
    )
    .await
    .unwrap_err();
    let error = classify_core_publish_failure(failure, false);
    assert!(matches!(error, ConnectorError::WriteError(_)));
    assert!(!error.is_outcome_unknown());
    assert_eq!(tokio::time::Instant::now() - started, timeout);

    let setup_error = bounded_nats_setup_until(
        tokio::time::Instant::now() + timeout,
        timeout,
        std::future::pending::<Result<(), async_nats::ConnectError>>(),
        |error| classify_connect_error(&error),
    )
    .await
    .unwrap_err();
    assert!(matches!(setup_error, ConnectorError::Timeout(25)));
    assert!(!setup_error.is_outcome_unknown());
}

#[tokio::test(start_paused = true)]
async fn setup_steps_share_one_absolute_admission_deadline() {
    let timeout = Duration::from_millis(25);
    let started = tokio::time::Instant::now();
    let deadline = started + timeout;

    bounded_nats_setup_until(
        deadline,
        timeout,
        async {
            tokio::time::sleep(Duration::from_millis(15)).await;
            Ok::<_, async_nats::ConnectError>(())
        },
        |error| classify_connect_error(&error),
    )
    .await
    .unwrap();
    let error = bounded_nats_setup_until(
        deadline,
        timeout,
        std::future::pending::<Result<(), async_nats::ConnectError>>(),
        |error| classify_connect_error(&error),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ConnectorError::Timeout(25)));
    assert_eq!(tokio::time::Instant::now() - started, timeout);
}

#[tokio::test(start_paused = true)]
async fn enqueue_windows_share_one_absolute_write_deadline() {
    let timeout = Duration::from_millis(25);
    let started = tokio::time::Instant::now();
    let deadline = started + timeout;

    bounded_publish_enqueue(deadline, timeout, async {
        tokio::time::sleep(Duration::from_millis(15)).await;
        Ok::<_, async_nats::PublishError>(())
    })
    .await
    .unwrap();
    let failure = bounded_publish_enqueue(
        deadline,
        timeout,
        std::future::pending::<Result<(), async_nats::PublishError>>(),
    )
    .await
    .unwrap_err();

    assert!(matches!(failure, PublishEnqueueFailure::TimedOut(value) if value == timeout));
    assert_eq!(tokio::time::Instant::now() - started, timeout);
}

#[test]
fn core_enqueue_errors_are_definite_until_prior_output_exists() {
    use async_nats::client::PublishErrorKind;

    let invalid = classify_core_publish_failure(
        PublishEnqueueFailure::Client(async_nats::PublishError::new(
            PublishErrorKind::InvalidSubject,
        )),
        false,
    );
    assert!(matches!(invalid, ConnectorError::ConfigurationError(_)));
    assert!(!invalid.is_outcome_unknown());

    let disconnected = classify_core_publish_failure(
        PublishEnqueueFailure::Client(async_nats::PublishError::new(PublishErrorKind::Send)),
        false,
    );
    assert!(matches!(disconnected, ConnectorError::WriteError(_)));
    assert!(disconnected.is_transient());

    let partial = classify_core_publish_failure(
        PublishEnqueueFailure::Client(async_nats::PublishError::new(
            PublishErrorKind::InvalidSubject,
        )),
        true,
    );
    assert!(partial.is_outcome_unknown());
    assert!(!partial.is_transient());
}

#[test]
fn dynamic_headers_are_validated_before_publish() {
    let name = HeaderName::from_str("trace_id").unwrap();
    let values = StringArray::from(vec![Some("valid"), Some("invalid\r\nvalue")]);

    let error = validate_headers_and_encoded_len(None, None, &[(&name, &values)], 1).unwrap_err();
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(error.to_string().contains("row 1"));
}

#[test]
fn later_row_publish_wildcard_is_rejected_by_batch_preflight() {
    let subjects = StringArray::from(vec!["events.valid", "events.*", "never.reached"]);
    let configured = SubjectSpec::Column("subject".into());

    let error = validate_publish_subjects(&configured, Some(&subjects), subjects.len())
        .expect_err("publish wildcards are subscriptions, never concrete targets");

    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(error.to_string().contains("row 1"));
}

#[test]
fn max_payload_preflight_includes_encoded_headers() {
    let name = HeaderName::from_str("trace_id").unwrap();
    let values = StringArray::from(vec![Some("abc")]);
    let headers = build_headers(None, None, &[(&name, &values)], 0)
        .unwrap()
        .unwrap();
    let encoded_len = validate_headers_and_encoded_len(None, None, &[(&name, &values)], 0).unwrap();
    assert_eq!(encoded_len, 27);
    assert_eq!(encoded_header_len(&headers), 27);

    validate_message_size(0, 5, encoded_len, encoded_len + 5).unwrap();
    let error = validate_message_size(0, 5, encoded_len, encoded_len + 4).unwrap_err();
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(error.to_string().contains("including headers"));
}

#[test]
fn acknowledged_prior_batch_makes_later_enqueue_failure_partial() {
    assert!(operation_has_prior_output(0, 0, 1));
    let error = classify_jetstream_enqueue_failure(
        PublishEnqueueFailure::Client(jetstream::context::PublishError::new(
            jetstream::context::PublishErrorKind::StreamNotFound,
        )),
        operation_has_prior_output(0, 0, 1),
    );
    assert!(error.is_outcome_unknown());
    assert!(!error.is_transient());
}

#[tokio::test]
async fn unresolved_core_generation_cannot_be_reopened() {
    let mut sink = NatsSink::new(std::sync::Arc::new(arrow_schema::Schema::empty()), None);
    sink.core_dirty = true;

    let error = sink.open(&sink_config(Mode::Core)).await.unwrap_err();
    assert!(matches!(error, ConnectorError::InvalidState { .. }));
}

#[test]
fn jetstream_enqueue_timeout_is_not_an_ack_timeout() {
    use jetstream::context::{PublishError, PublishErrorKind};

    let current = classify_jetstream_enqueue_failure(
        PublishEnqueueFailure::Client(PublishError::new(PublishErrorKind::TimedOut)),
        false,
    );
    assert!(matches!(current, ConnectorError::WriteError(_)));
    assert!(!current.is_outcome_unknown());

    let partial = classify_jetstream_enqueue_failure(
        PublishEnqueueFailure::Client(PublishError::new(PublishErrorKind::TimedOut)),
        true,
    );
    assert!(partial.is_outcome_unknown());
    assert!(partial.is_transient());
}

#[test]
fn jetstream_ack_classifier_distinguishes_rejection_and_ambiguity() {
    use jetstream::context::{PublishError, PublishErrorKind};

    let rejected =
        classify_jetstream_ack_failure(&PublishError::new(PublishErrorKind::StreamNotFound));
    assert_eq!(rejected.certainty, AckCertainty::Rejected);
    assert!(!rejected.retryable);

    for kind in [PublishErrorKind::TimedOut, PublishErrorKind::BrokenPipe] {
        let ambiguous = classify_jetstream_ack_failure(&PublishError::new(kind));
        assert_eq!(ambiguous.certainty, AckCertainty::OutcomeUnknown);
        assert!(ambiguous.retryable);
    }

    for code in [408, 429, 500, 503, 599] {
        let ambiguous = classify_jetstream_ack_failure(&PublishError::with_source(
            PublishErrorKind::Other,
            broker_error(code),
        ));
        assert_eq!(ambiguous.certainty, AckCertainty::OutcomeUnknown);
        assert!(ambiguous.retryable);
    }
    for code in [400, 401, 403, 404, 422] {
        let rejected = classify_jetstream_ack_failure(&PublishError::with_source(
            PublishErrorKind::Other,
            broker_error(code),
        ));
        assert_eq!(rejected.certainty, AckCertainty::Rejected);
        assert!(!rejected.retryable);
    }

    let malformed_json = serde_json::from_slice::<serde_json::Value>(b"{").unwrap_err();
    let malformed = classify_jetstream_ack_failure(&PublishError::with_source(
        PublishErrorKind::Other,
        malformed_json,
    ));
    assert_eq!(malformed.certainty, AckCertainty::OutcomeUnknown);
    assert!(!malformed.retryable);

    for impossible in [PublishErrorKind::Other, PublishErrorKind::MaxAckPending] {
        let failure = classify_jetstream_ack_failure(&PublishError::new(impossible));
        assert_eq!(failure.certainty, AckCertainty::OutcomeUnknown);
        assert!(!failure.retryable);
    }
}

#[test]
fn ack_aggregation_returns_definite_error_when_all_are_rejected() {
    let mut aggregate = AckAggregate::default();
    aggregate.record_failure(AckFailure {
        certainty: AckCertainty::Rejected,
        detail: "stream rejected publish".into(),
        retryable: false,
    });

    let error = aggregate.into_result().unwrap_err();
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(!error.is_outcome_unknown());
}

#[test]
fn ack_aggregation_makes_partial_success_sticky() {
    let mut aggregate = AckAggregate::default();
    aggregate.record_applied();
    aggregate.record_failure(AckFailure {
        certainty: AckCertainty::Rejected,
        detail: "stream rejected publish".into(),
        retryable: false,
    });

    let error = aggregate.into_result().unwrap_err();
    assert!(error.is_outcome_unknown());
    assert!(!error.is_transient());
    assert!(error.to_string().contains("1 acknowledged"));
}

#[test]
fn prior_successful_drain_makes_later_rejection_sticky() {
    let rejected = ConnectorError::ConfigurationError("stream rejected publish".into());
    let error = preserve_prior_applied(rejected, 2);

    assert!(error.is_outcome_unknown());
    assert!(!error.is_transient());
    assert!(error.to_string().contains("2 earlier publish(es)"));
}

#[test]
fn ack_aggregation_gives_ambiguity_correctness_precedence() {
    let mut aggregate = AckAggregate::default();
    aggregate.record_failure(AckFailure {
        certainty: AckCertainty::Rejected,
        detail: "stream rejected publish".into(),
        retryable: false,
    });
    aggregate.record_unresolved_timeout(2, Duration::from_millis(50));

    let error = aggregate.into_result().unwrap_err();
    assert!(error.is_outcome_unknown());
    assert!(!error.is_transient());
    assert!(error.to_string().contains("2 outcome unknown"));
}
