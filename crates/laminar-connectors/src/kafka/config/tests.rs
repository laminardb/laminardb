use super::*;
use crate::config::ConnectorConfig;

fn make_config(extra: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", "localhost:9092");
    config.set("group.id", "test-group");
    config.set("topic", "events");
    for (k, v) in extra {
        config.set(*k, *v);
    }
    config
}

#[test]
#[allow(deprecated)]
fn test_parse_required_fields() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[])).unwrap();
    assert_eq!(cfg.bootstrap_servers, "localhost:9092");
    assert_eq!(cfg.group_id, "test-group");
    assert_eq!(cfg.subscription.topics().unwrap(), &["events"]);
    assert!(matches!(
        cfg.subscription,
        TopicSubscription::Topics(ref t) if t == &["events"]
    ));
}

#[test]
fn test_parse_missing_required() {
    let config = ConnectorConfig::new("kafka");
    assert!(KafkaSourceConfig::from_config(&config).is_err());
}

#[test]
#[allow(deprecated)]
fn test_parse_multi_topic() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[("topic", "a, b, c")])).unwrap();
    assert_eq!(cfg.subscription.topics().unwrap(), &["a", "b", "c"]);
    assert!(matches!(
        cfg.subscription,
        TopicSubscription::Topics(ref t) if t == &["a", "b", "c"]
    ));
}

#[test]
fn test_parse_topic_pattern() {
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", "localhost:9092");
    config.set("group.id", "test-group");
    config.set("topic.pattern", "events-.*");

    let cfg = KafkaSourceConfig::from_config(&config).unwrap();
    assert!(matches!(
        cfg.subscription,
        TopicSubscription::Pattern(ref p) if p == "events-.*"
    ));
    assert!(cfg.subscription.is_pattern());
    assert_eq!(cfg.subscription.pattern(), Some("events-.*"));
    assert!(cfg.subscription.topics().is_none());
}

#[test]
fn test_parse_defaults() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[])).unwrap();
    assert_eq!(cfg.format, Format::Json);
    assert_eq!(cfg.auto_offset_reset, OffsetReset::Earliest);
    assert_eq!(cfg.isolation_level, IsolationLevel::ReadCommitted);
    assert_eq!(cfg.max_poll_records, 1000);
    assert_eq!(cfg.partition_assignment_strategy, AssignmentStrategy::Range);
    assert!(!cfg.include_metadata);
    assert!(!cfg.include_headers);
    assert!(cfg.schema_registry_url.is_none());
    assert_eq!(cfg.security_protocol, SecurityProtocol::Plaintext);
    assert!(cfg.sasl_mechanism.is_none());
    assert!(cfg.broker_commit_on_checkpoint);
    assert_eq!(cfg.reader_channel_capacity, 8192);
    assert_eq!(cfg.backpressure_high_watermark, 0.8);
    assert_eq!(cfg.backpressure_low_watermark, 0.25);
}

#[test]
fn test_parse_broker_commit_on_checkpoint_disabled() {
    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("broker.commit.on.checkpoint", "false")]))
            .unwrap();
    assert!(!cfg.broker_commit_on_checkpoint);
}

#[test]
fn test_parse_broker_commit_interval_rejected() {
    let err =
        KafkaSourceConfig::from_config(&make_config(&[("broker.commit.interval.ms", "5000")]))
            .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("broker.commit.interval.ms"),
        "expected hard-error mentioning the deprecated key, got: {msg}"
    );
}

#[test]
fn test_parse_optional_fields() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("format", "csv"),
        ("auto.offset.reset", "latest"),
        ("max.poll.records", "500"),
        ("include.metadata", "true"),
        ("include.headers", "true"),
        ("partition.assignment.strategy", "roundrobin"),
        ("isolation.level", "read_uncommitted"),
    ]))
    .unwrap();

    assert_eq!(cfg.format, Format::Csv);
    assert_eq!(cfg.auto_offset_reset, OffsetReset::Latest);
    assert_eq!(cfg.isolation_level, IsolationLevel::ReadUncommitted);
    assert_eq!(cfg.max_poll_records, 500);
    assert!(cfg.include_metadata);
    assert!(cfg.include_headers);
    assert_eq!(
        cfg.partition_assignment_strategy,
        AssignmentStrategy::RoundRobin
    );
}

#[test]
fn test_parse_security_sasl_ssl() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("security.protocol", "sasl_ssl"),
        ("sasl.mechanism", "SCRAM-SHA-256"),
        ("sasl.username", "alice"),
        ("sasl.password", "secret"),
        ("ssl.ca.location", "/etc/ssl/ca.pem"),
    ]))
    .unwrap();

    assert_eq!(cfg.security_protocol, SecurityProtocol::SaslSsl);
    assert_eq!(cfg.sasl_mechanism, Some(SaslMechanism::ScramSha256));
    assert_eq!(cfg.sasl_username, Some("alice".to_string()));
    assert_eq!(cfg.sasl_password, Some("secret".to_string()));
    assert_eq!(cfg.ssl_ca_location, Some("/etc/ssl/ca.pem".to_string()));
}

#[test]
fn test_parse_security_ssl_only() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("security.protocol", "ssl"),
        ("ssl.ca.location", "/etc/ssl/ca.pem"),
        ("ssl.certificate.location", "/etc/ssl/client.pem"),
        ("ssl.key.location", "/etc/ssl/client.key"),
        ("ssl.key.password", "keypass"),
    ]))
    .unwrap();

    assert_eq!(cfg.security_protocol, SecurityProtocol::Ssl);
    assert!(cfg.security_protocol.uses_ssl());
    assert!(!cfg.security_protocol.uses_sasl());
    assert_eq!(cfg.ssl_ca_location, Some("/etc/ssl/ca.pem".to_string()));
    assert_eq!(
        cfg.ssl_certificate_location,
        Some("/etc/ssl/client.pem".to_string())
    );
    assert_eq!(
        cfg.ssl_key_location,
        Some("/etc/ssl/client.key".to_string())
    );
    assert_eq!(cfg.ssl_key_password, Some("keypass".to_string()));
}

#[test]
fn test_parse_fetch_tuning() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("fetch.min.bytes", "1024"),
        ("fetch.max.bytes", "52428800"),
        ("fetch.max.wait.ms", "500"),
        ("max.partition.fetch.bytes", "1048576"),
    ]))
    .unwrap();

    assert_eq!(cfg.fetch_min_bytes, Some(1024));
    assert_eq!(cfg.fetch_max_bytes, Some(52_428_800));
    assert_eq!(cfg.fetch_max_wait_ms, Some(500));
    assert_eq!(cfg.max_partition_fetch_bytes, Some(1_048_576));
}

#[test]
fn test_parse_kafka_passthrough() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("kafka.session.timeout.ms", "30000"),
        ("kafka.max.poll.interval.ms", "300000"),
        ("kafka.fetch.message.max.bytes", "2097152"),
    ]))
    .unwrap();

    assert_eq!(cfg.kafka_properties.len(), 3);
    // session.timeout.ms and max.poll.interval.ms are passed through as
    // kafka_properties, but blocked by is_blocked_passthrough_key() in
    // to_rdkafka_config() — they won't override explicit settings.
    assert_eq!(
        cfg.kafka_properties.get("session.timeout.ms"),
        Some(&"30000".to_string())
    );
    assert_eq!(
        cfg.kafka_properties.get("max.poll.interval.ms"),
        Some(&"300000".to_string())
    );
}

#[test]
fn test_parse_schema_registry() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("format", "avro"),
        ("schema.registry.url", "http://localhost:8081"),
        ("schema.registry.username", "user"),
        ("schema.registry.password", "pass"),
        ("schema.compatibility", "FULL_TRANSITIVE"),
        ("schema.registry.ssl.ca.location", "/etc/ssl/sr-ca.pem"),
    ]))
    .unwrap();

    assert_eq!(cfg.format, Format::Avro);
    assert_eq!(
        cfg.schema_registry_url,
        Some("http://localhost:8081".to_string())
    );
    assert!(cfg.schema_registry_auth.is_some());
    let auth = cfg.schema_registry_auth.unwrap();
    assert_eq!(auth.username, "user");
    assert_eq!(auth.password, "pass");
    assert_eq!(
        cfg.schema_compatibility,
        Some(CompatibilityLevel::FullTransitive)
    );
    assert_eq!(
        cfg.schema_registry_ssl_ca_location,
        Some("/etc/ssl/sr-ca.pem".to_string())
    );
}

#[test]
fn test_parse_sr_auth_partial() {
    let config = make_config(&[
        ("schema.registry.url", "http://localhost:8081"),
        ("schema.registry.username", "user"),
        // missing password
    ]);
    assert!(KafkaSourceConfig::from_config(&config).is_err());
}

#[test]
fn test_validate_avro_without_sr() {
    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = "localhost:9092".into();
    cfg.group_id = "g".into();
    cfg.subscription = TopicSubscription::Topics(vec!["t".into()]);
    cfg.format = Format::Avro;
    // No schema_registry_url
    assert!(cfg.validate().is_err());
}

#[test]
fn test_validate_backpressure_watermarks() {
    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = "localhost:9092".into();
    cfg.group_id = "g".into();
    cfg.subscription = TopicSubscription::Topics(vec!["t".into()]);
    cfg.backpressure_high_watermark = 0.3;
    cfg.backpressure_low_watermark = 0.5;
    assert!(cfg.validate().is_err());
}

#[test]
fn test_validate_sasl_without_mechanism() {
    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = "localhost:9092".into();
    cfg.group_id = "g".into();
    cfg.subscription = TopicSubscription::Topics(vec!["t".into()]);
    cfg.security_protocol = SecurityProtocol::SaslPlaintext;
    // sasl_mechanism not set
    assert!(cfg.validate().is_err());
}

#[test]
fn test_validate_sasl_plain_without_credentials() {
    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = "localhost:9092".into();
    cfg.group_id = "g".into();
    cfg.subscription = TopicSubscription::Topics(vec!["t".into()]);
    cfg.security_protocol = SecurityProtocol::SaslPlaintext;
    cfg.sasl_mechanism = Some(SaslMechanism::Plain);
    // username/password not set
    assert!(cfg.validate().is_err());
}

#[test]
fn test_validate_empty_topic_pattern() {
    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = "localhost:9092".into();
    cfg.group_id = "g".into();
    cfg.subscription = TopicSubscription::Pattern(String::new());
    assert!(cfg.validate().is_err());
}

#[test]
fn test_rdkafka_config() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("auto.offset.reset", "latest"),
        ("kafka.fetch.min.bytes", "1024"),
    ]))
    .unwrap();

    let rdkafka = cfg.to_rdkafka_config();
    assert_eq!(rdkafka.get("bootstrap.servers"), Some("localhost:9092"));
    assert_eq!(rdkafka.get("group.id"), Some("test-group"));
    assert_eq!(rdkafka.get("enable.auto.commit"), Some("false"));
    assert_eq!(rdkafka.get("auto.offset.reset"), Some("latest"));
    assert_eq!(rdkafka.get("fetch.min.bytes"), Some("1024"));
    assert_eq!(rdkafka.get("security.protocol"), Some("plaintext"));
    assert_eq!(rdkafka.get("isolation.level"), Some("read_committed"));
}

#[test]
fn test_rdkafka_config_with_security() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("security.protocol", "sasl_ssl"),
        ("sasl.mechanism", "PLAIN"),
        ("sasl.username", "user"),
        ("sasl.password", "pass"),
        ("ssl.ca.location", "/ca.pem"),
    ]))
    .unwrap();

    let rdkafka = cfg.to_rdkafka_config();
    assert_eq!(rdkafka.get("security.protocol"), Some("sasl_ssl"));
    assert_eq!(rdkafka.get("sasl.mechanism"), Some("PLAIN"));
    assert_eq!(rdkafka.get("sasl.username"), Some("user"));
    assert_eq!(rdkafka.get("sasl.password"), Some("pass"));
    assert_eq!(rdkafka.get("ssl.ca.location"), Some("/ca.pem"));
}

#[test]
fn test_rdkafka_config_with_fetch_tuning() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("fetch.min.bytes", "1024"),
        ("fetch.max.bytes", "1048576"),
        ("fetch.max.wait.ms", "500"),
        ("max.partition.fetch.bytes", "262144"),
        ("isolation.level", "read_uncommitted"),
    ]))
    .unwrap();

    let rdkafka = cfg.to_rdkafka_config();
    assert_eq!(rdkafka.get("fetch.min.bytes"), Some("1024"));
    assert_eq!(rdkafka.get("fetch.max.bytes"), Some("1048576"));
    assert_eq!(rdkafka.get("fetch.wait.max.ms"), Some("500"));
    assert_eq!(rdkafka.get("max.partition.fetch.bytes"), Some("262144"));
    assert_eq!(rdkafka.get("isolation.level"), Some("read_uncommitted"));
}

#[test]
fn test_offset_reset_parsing() {
    assert_eq!(
        "earliest".parse::<OffsetReset>().unwrap(),
        OffsetReset::Earliest
    );
    assert_eq!(
        "latest".parse::<OffsetReset>().unwrap(),
        OffsetReset::Latest
    );
    assert_eq!("none".parse::<OffsetReset>().unwrap(), OffsetReset::None);
    assert!("invalid".parse::<OffsetReset>().is_err());
}

#[test]
fn test_compatibility_level_parsing() {
    assert_eq!(
        "BACKWARD".parse::<CompatibilityLevel>().unwrap(),
        CompatibilityLevel::Backward
    );
    assert_eq!(
        "full_transitive".parse::<CompatibilityLevel>().unwrap(),
        CompatibilityLevel::FullTransitive
    );
    assert_eq!(
        "NONE".parse::<CompatibilityLevel>().unwrap(),
        CompatibilityLevel::None
    );
    assert!("invalid".parse::<CompatibilityLevel>().is_err());
}

#[test]
fn test_security_protocol_parsing() {
    assert_eq!(
        "plaintext".parse::<SecurityProtocol>().unwrap(),
        SecurityProtocol::Plaintext
    );
    assert_eq!(
        "SSL".parse::<SecurityProtocol>().unwrap(),
        SecurityProtocol::Ssl
    );
    assert_eq!(
        "sasl_plaintext".parse::<SecurityProtocol>().unwrap(),
        SecurityProtocol::SaslPlaintext
    );
    assert_eq!(
        "SASL_SSL".parse::<SecurityProtocol>().unwrap(),
        SecurityProtocol::SaslSsl
    );
    assert_eq!(
        "sasl-ssl".parse::<SecurityProtocol>().unwrap(),
        SecurityProtocol::SaslSsl
    );
    assert!("invalid".parse::<SecurityProtocol>().is_err());
}

#[test]
fn test_sasl_mechanism_parsing() {
    assert_eq!(
        "PLAIN".parse::<SaslMechanism>().unwrap(),
        SaslMechanism::Plain
    );
    assert_eq!(
        "SCRAM-SHA-256".parse::<SaslMechanism>().unwrap(),
        SaslMechanism::ScramSha256
    );
    assert_eq!(
        "scram_sha_512".parse::<SaslMechanism>().unwrap(),
        SaslMechanism::ScramSha512
    );
    assert_eq!(
        "GSSAPI".parse::<SaslMechanism>().unwrap(),
        SaslMechanism::Gssapi
    );
    assert_eq!(
        "OAUTHBEARER".parse::<SaslMechanism>().unwrap(),
        SaslMechanism::Oauthbearer
    );
    assert!("invalid".parse::<SaslMechanism>().is_err());
}

#[test]
fn test_isolation_level_parsing() {
    assert_eq!(
        "read_uncommitted".parse::<IsolationLevel>().unwrap(),
        IsolationLevel::ReadUncommitted
    );
    assert_eq!(
        "read_committed".parse::<IsolationLevel>().unwrap(),
        IsolationLevel::ReadCommitted
    );
    assert_eq!(
        "read-committed".parse::<IsolationLevel>().unwrap(),
        IsolationLevel::ReadCommitted
    );
    assert!("invalid".parse::<IsolationLevel>().is_err());
}

#[test]
fn test_topic_subscription_accessors() {
    let topics = TopicSubscription::Topics(vec!["a".into(), "b".into()]);
    assert_eq!(
        topics.topics(),
        Some(&["a".to_string(), "b".to_string()][..])
    );
    assert!(topics.pattern().is_none());
    assert!(!topics.is_pattern());

    let pattern = TopicSubscription::Pattern("events-.*".into());
    assert!(pattern.topics().is_none());
    assert_eq!(pattern.pattern(), Some("events-.*"));
    assert!(pattern.is_pattern());
}

#[test]
fn test_security_protocol_helpers() {
    assert!(!SecurityProtocol::Plaintext.uses_ssl());
    assert!(!SecurityProtocol::Plaintext.uses_sasl());

    assert!(SecurityProtocol::Ssl.uses_ssl());
    assert!(!SecurityProtocol::Ssl.uses_sasl());

    assert!(!SecurityProtocol::SaslPlaintext.uses_ssl());
    assert!(SecurityProtocol::SaslPlaintext.uses_sasl());

    assert!(SecurityProtocol::SaslSsl.uses_ssl());
    assert!(SecurityProtocol::SaslSsl.uses_sasl());
}

#[test]
fn test_sasl_mechanism_helpers() {
    assert!(SaslMechanism::Plain.requires_credentials());
    assert!(SaslMechanism::ScramSha256.requires_credentials());
    assert!(SaslMechanism::ScramSha512.requires_credentials());
    assert!(!SaslMechanism::Gssapi.requires_credentials());
    assert!(!SaslMechanism::Oauthbearer.requires_credentials());
}

#[test]
fn test_enum_display() {
    assert_eq!(SecurityProtocol::SaslSsl.to_string(), "sasl_ssl");
    assert_eq!(SaslMechanism::ScramSha256.to_string(), "SCRAM-SHA-256");
    assert_eq!(IsolationLevel::ReadCommitted.to_string(), "read_committed");
}

#[test]
fn test_startup_mode_parsing() {
    assert_eq!(
        "group-offsets".parse::<StartupMode>().unwrap(),
        StartupMode::GroupOffsets
    );
    assert_eq!(
        "group_offsets".parse::<StartupMode>().unwrap(),
        StartupMode::GroupOffsets
    );
    assert_eq!(
        "earliest".parse::<StartupMode>().unwrap(),
        StartupMode::Earliest
    );
    assert_eq!(
        "latest".parse::<StartupMode>().unwrap(),
        StartupMode::Latest
    );
    assert!("invalid".parse::<StartupMode>().is_err());
}

#[test]
fn test_startup_mode_display() {
    assert_eq!(StartupMode::GroupOffsets.to_string(), "group-offsets");
    assert_eq!(StartupMode::Earliest.to_string(), "earliest");
    assert_eq!(StartupMode::Latest.to_string(), "latest");

    let specific = StartupMode::SpecificOffsets(HashMap::from([(0, 100), (1, 200)]));
    assert!(specific.to_string().contains("2 partitions"));

    let ts = StartupMode::Timestamp(1234567890000);
    assert!(ts.to_string().contains("1234567890000"));
}

#[test]
fn test_startup_mode_latest_overrides_offset_reset() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[("startup.mode", "latest")])).unwrap();
    assert_eq!(cfg.auto_offset_reset, OffsetReset::Latest);
    let rdkafka = cfg.to_rdkafka_config();
    assert_eq!(rdkafka.get("auto.offset.reset"), Some("latest"));
}

#[test]
fn test_startup_mode_earliest_overrides_offset_reset() {
    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("startup.mode", "earliest")])).unwrap();
    assert_eq!(cfg.auto_offset_reset, OffsetReset::Earliest);
}

#[test]
fn test_startup_mode_group_offsets_uses_explicit_offset_reset() {
    // When startup.mode is group-offsets (default), the explicit
    // auto.offset.reset setting should be used.
    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("auto.offset.reset", "latest")])).unwrap();
    assert_eq!(cfg.auto_offset_reset, OffsetReset::Latest);
}

#[test]
fn test_parse_specific_offsets() {
    let offsets = parse_specific_offsets("0:100,1:200,2:300").unwrap();
    assert_eq!(offsets.get(&0), Some(&100));
    assert_eq!(offsets.get(&1), Some(&200));
    assert_eq!(offsets.get(&2), Some(&300));
}

#[test]
fn test_parse_specific_offsets_with_spaces() {
    let offsets = parse_specific_offsets(" 0:100 , 1:200 ").unwrap();
    assert_eq!(offsets.get(&0), Some(&100));
    assert_eq!(offsets.get(&1), Some(&200));
}

#[test]
fn test_parse_specific_offsets_errors() {
    assert!(parse_specific_offsets("").is_err());
    assert!(parse_specific_offsets("0").is_err());
    assert!(parse_specific_offsets("0:abc").is_err());
    assert!(parse_specific_offsets("abc:100").is_err());
    assert!(parse_specific_offsets("0:100:extra").is_err());
}

#[test]
fn test_parse_startup_mode_from_config() {
    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("startup.mode", "earliest")])).unwrap();
    assert_eq!(cfg.startup_mode, StartupMode::Earliest);

    let cfg = KafkaSourceConfig::from_config(&make_config(&[("startup.mode", "latest")])).unwrap();
    assert_eq!(cfg.startup_mode, StartupMode::Latest);

    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("startup.mode", "group-offsets")])).unwrap();
    assert_eq!(cfg.startup_mode, StartupMode::GroupOffsets);
}

#[test]
fn test_parse_startup_specific_offsets_from_config() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[(
        "startup.specific.offsets",
        "0:100,1:200",
    )]))
    .unwrap();

    match cfg.startup_mode {
        StartupMode::SpecificOffsets(offsets) => {
            assert_eq!(offsets.get(&0), Some(&100));
            assert_eq!(offsets.get(&1), Some(&200));
        }
        _ => panic!("expected SpecificOffsets"),
    }
}

#[test]
fn test_parse_startup_timestamp_from_config() {
    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("startup.timestamp.ms", "1234567890")]))
            .unwrap();

    assert_eq!(cfg.startup_mode, StartupMode::Timestamp(1234567890));
}

#[test]
fn test_parse_schema_registry_ssl_fields() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("schema.registry.url", "https://sr:8081"),
        ("schema.registry.ssl.ca.location", "/ca.pem"),
        ("schema.registry.ssl.certificate.location", "/cert.pem"),
        ("schema.registry.ssl.key.location", "/key.pem"),
    ]))
    .unwrap();

    assert_eq!(
        cfg.schema_registry_ssl_ca_location,
        Some("/ca.pem".to_string())
    );
    assert_eq!(
        cfg.schema_registry_ssl_certificate_location,
        Some("/cert.pem".to_string())
    );
    assert_eq!(
        cfg.schema_registry_ssl_key_location,
        Some("/key.pem".to_string())
    );
}

// -- startup.mode = timestamp error --

#[test]
fn test_startup_mode_timestamp_error() {
    let config = make_config(&[("startup.mode", "timestamp")]);
    let err = KafkaSourceConfig::from_config(&config).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("startup.timestamp.ms"),
        "error should mention startup.timestamp.ms, got: {msg}"
    );
}

// -- session timeout / heartbeat interval --

#[test]
fn test_session_timeout_heartbeat_defaults() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[])).unwrap();
    assert_eq!(cfg.session_timeout, Duration::from_secs(45));
    assert_eq!(cfg.heartbeat_interval, Duration::from_secs(10));
}

#[test]
fn test_session_timeout_heartbeat_custom() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("session.timeout.ms", "60000"),
        ("heartbeat.interval.ms", "15000"),
    ]))
    .unwrap();
    assert_eq!(cfg.session_timeout, Duration::from_secs(60));
    assert_eq!(cfg.heartbeat_interval, Duration::from_secs(15));
}

#[test]
fn test_session_timeout_heartbeat_validation_fails() {
    // heartbeat=20s * 3 = 60s >= session=45s → error
    let config = make_config(&[
        ("session.timeout.ms", "45000"),
        ("heartbeat.interval.ms", "20000"),
    ]);
    let err = KafkaSourceConfig::from_config(&config).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("heartbeat.interval.ms"), "got: {msg}");
}

#[test]
fn test_session_timeout_heartbeat_validation_passes() {
    // heartbeat=10s * 3 = 30s < session=45s → ok
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("session.timeout.ms", "45000"),
        ("heartbeat.interval.ms", "10000"),
    ]))
    .unwrap();
    assert_eq!(cfg.session_timeout, Duration::from_secs(45));
    assert_eq!(cfg.heartbeat_interval, Duration::from_secs(10));
}

#[test]
fn test_session_timeout_heartbeat_in_rdkafka_config() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("session.timeout.ms", "60000"),
        ("heartbeat.interval.ms", "15000"),
    ]))
    .unwrap();
    let rdkafka = cfg.to_rdkafka_config();
    assert_eq!(rdkafka.get("session.timeout.ms"), Some("60000"));
    assert_eq!(rdkafka.get("heartbeat.interval.ms"), Some("15000"));
}

#[test]
fn test_session_timeout_passthrough_blocked() {
    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("kafka.session.timeout.ms", "99999")]))
            .unwrap();
    // The pass-through should be blocked; rdkafka config should use the default (45000).
    let rdkafka = cfg.to_rdkafka_config();
    assert_eq!(rdkafka.get("session.timeout.ms"), Some("45000"));
}

// -- queued.max.messages.kbytes --

#[test]
fn test_queued_max_messages_kbytes_default() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[])).unwrap();
    assert_eq!(cfg.queued_max_messages_kbytes, 16384);
}

#[test]
fn test_queued_max_messages_kbytes_custom() {
    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("queued.max.messages.kbytes", "32768")]))
            .unwrap();
    assert_eq!(cfg.queued_max_messages_kbytes, 32768);
}

#[test]
fn test_queued_max_messages_kbytes_in_rdkafka_config() {
    let cfg =
        KafkaSourceConfig::from_config(&make_config(&[("queued.max.messages.kbytes", "8192")]))
            .unwrap();
    let rdkafka = cfg.to_rdkafka_config();
    assert_eq!(rdkafka.get("queued.max.messages.kbytes"), Some("8192"));
}

// ── Schema Registry subject-name strategy ──

#[test]
fn resolve_subject_topic_name() {
    assert_eq!(
        resolve_value_subject(SubjectNameStrategy::TopicName, None, "orders"),
        "orders-value"
    );
}

#[test]
fn resolve_subject_record_name() {
    assert_eq!(
        resolve_value_subject(
            SubjectNameStrategy::RecordName,
            Some("com.acme.Order"),
            "orders"
        ),
        "com.acme.Order-value"
    );
}

#[test]
fn resolve_subject_topic_record_name() {
    assert_eq!(
        resolve_value_subject(
            SubjectNameStrategy::TopicRecordName,
            Some("com.acme.Order"),
            "orders"
        ),
        "orders-com.acme.Order-value"
    );
}

#[test]
fn parse_subject_strategy_from_config() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("schema.registry.url", "http://sr:8081"),
        ("schema.registry.subject.name.strategy", "record-name"),
        ("schema.registry.record.name", "com.acme.Order"),
    ]))
    .unwrap();
    assert_eq!(
        cfg.schema_registry_subject_strategy,
        SubjectNameStrategy::RecordName
    );
    assert_eq!(
        cfg.schema_registry_record_name.as_deref(),
        Some("com.acme.Order")
    );
}

#[test]
fn parse_subject_strategy_rejects_missing_record_name() {
    let err = KafkaSourceConfig::from_config(&make_config(&[
        ("schema.registry.url", "http://sr:8081"),
        ("schema.registry.subject.name.strategy", "record-name"),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConnectorError::ConfigurationError(_)));
}

#[test]
fn parse_discovery_timeout_default_ten_seconds() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[("schema.registry.url", "http://sr")]))
        .unwrap();
    assert_eq!(
        cfg.schema_registry_discovery_timeout,
        Duration::from_secs(10)
    );
}

#[test]
fn parse_discovery_timeout_override() {
    let cfg = KafkaSourceConfig::from_config(&make_config(&[
        ("schema.registry.url", "http://sr"),
        ("schema.registry.discovery.timeout.ms", "25000"),
    ]))
    .unwrap();
    assert_eq!(
        cfg.schema_registry_discovery_timeout,
        Duration::from_secs(25)
    );
}
