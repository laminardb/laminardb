use super::*;

fn make_config(pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("kafka");
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    config
}

fn required_pairs() -> Vec<(&'static str, &'static str)> {
    vec![
        ("bootstrap.servers", "localhost:9092"),
        ("topic", "output-events"),
    ]
}

#[test]
fn test_parse_required_fields() {
    let config = make_config(&required_pairs());
    let cfg = KafkaSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.bootstrap_servers, "localhost:9092");
    assert_eq!(cfg.topic, "output-events");
    assert_eq!(cfg.format, Format::Json);
    assert_eq!(cfg.security_protocol, SecurityProtocol::Plaintext);
}

#[test]
fn test_missing_bootstrap_servers() {
    let config = make_config(&[("topic", "t")]);
    assert!(KafkaSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_missing_topic() {
    let config = make_config(&[("bootstrap.servers", "b:9092")]);
    assert!(KafkaSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_parse_security_sasl_ssl() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("security.protocol", "sasl_ssl"),
        ("sasl.mechanism", "SCRAM-SHA-512"),
        ("sasl.username", "producer"),
        ("sasl.password", "secret123"),
        ("ssl.ca.location", "/etc/ssl/ca.pem"),
    ]);
    let config = make_config(&pairs);
    let cfg = KafkaSinkConfig::from_config(&config).unwrap();

    assert_eq!(cfg.security_protocol, SecurityProtocol::SaslSsl);
    assert_eq!(cfg.sasl_mechanism, Some(SaslMechanism::ScramSha512));
    assert_eq!(cfg.sasl_username, Some("producer".to_string()));
    assert_eq!(cfg.sasl_password, Some("secret123".to_string()));
    assert_eq!(cfg.ssl_ca_location, Some("/etc/ssl/ca.pem".to_string()));
}

#[test]
fn test_parse_security_ssl_only() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("security.protocol", "ssl"),
        ("ssl.ca.location", "/etc/ssl/ca.pem"),
        ("ssl.certificate.location", "/etc/ssl/client.pem"),
        ("ssl.key.location", "/etc/ssl/client.key"),
        ("ssl.key.password", "keypass"),
    ]);
    let config = make_config(&pairs);
    let cfg = KafkaSinkConfig::from_config(&config).unwrap();

    assert_eq!(cfg.security_protocol, SecurityProtocol::Ssl);
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
fn test_parse_all_optional_fields() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("format", "avro"),
        ("key.column", "order_id"),
        ("partitioner", "round-robin"),
        ("linger.ms", "10"),
        ("batch.size", "32768"),
        ("batch.num.messages", "5000"),
        ("compression.type", "zstd"),
        ("max.in.flight.requests", "3"),
        ("delivery.timeout.ms", "60000"),
        ("dlq.topic", "my-dlq"),
        ("schema.registry.url", "http://sr:8081"),
        ("schema.registry.username", "user"),
        ("schema.registry.password", "pass"),
        ("schema.registry.ssl.ca.location", "/etc/ssl/sr-ca.pem"),
    ]);
    let config = make_config(&pairs);
    let cfg = KafkaSinkConfig::from_config(&config).unwrap();

    assert_eq!(cfg.format, Format::Avro);
    assert_eq!(cfg.key_column.as_deref(), Some("order_id"));
    assert_eq!(cfg.partitioner, PartitionStrategy::RoundRobin);
    assert_eq!(cfg.linger_ms, 10);
    assert_eq!(cfg.batch_size, 32_768);
    assert_eq!(cfg.batch_num_messages, Some(5000));
    assert_eq!(cfg.compression, CompressionType::Zstd);
    assert_eq!(cfg.max_in_flight, 3);
    assert_eq!(cfg.delivery_timeout, Duration::from_secs(60));
    assert_eq!(cfg.dlq_topic.as_deref(), Some("my-dlq"));
    assert_eq!(cfg.schema_registry_url.as_deref(), Some("http://sr:8081"));
    assert!(cfg.schema_registry_auth.is_some());
    assert_eq!(
        cfg.schema_registry_ssl_ca_location,
        Some("/etc/ssl/sr-ca.pem".to_string())
    );
}

#[test]
fn delivery_timeout_rejects_infinite_or_excessive_values() {
    for value in ["0", "300001"] {
        let mut pairs = required_pairs();
        pairs.push(("delivery.timeout.ms", value));
        let error = KafkaSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        assert!(
            matches!(error, ConnectorError::ConfigurationError(_)),
            "unexpected error for delivery.timeout.ms={value}: {error}"
        );
    }
}

#[test]
fn delivery_timeout_accepts_fixed_maximum() {
    let mut pairs = required_pairs();
    pairs.push(("delivery.timeout.ms", "300000"));
    let cfg = KafkaSinkConfig::from_config(&make_config(&pairs)).unwrap();
    assert_eq!(cfg.delivery_timeout, MAX_DELIVERY_TIMEOUT);
    assert_eq!(
        cfg.to_rdkafka_config().get("message.timeout.ms"),
        Some("300000")
    );
    assert_eq!(
        cfg.to_dlq_rdkafka_config().get("message.timeout.ms"),
        Some("300000")
    );
}

#[test]
fn test_validate_avro_requires_sr() {
    let mut cfg = KafkaSinkConfig::default();
    cfg.bootstrap_servers = "b:9092".into();
    cfg.topic = "t".into();
    cfg.format = Format::Avro;
    assert!(cfg.validate().is_err());
}

#[test]
fn test_validate_sasl_without_mechanism() {
    let mut cfg = KafkaSinkConfig::default();
    cfg.bootstrap_servers = "b:9092".into();
    cfg.topic = "t".into();
    cfg.security_protocol = SecurityProtocol::SaslSsl;
    // sasl_mechanism not set
    assert!(cfg.validate().is_err());
}

#[test]
fn test_validate_sasl_plain_without_credentials() {
    let mut cfg = KafkaSinkConfig::default();
    cfg.bootstrap_servers = "b:9092".into();
    cfg.topic = "t".into();
    cfg.security_protocol = SecurityProtocol::SaslPlaintext;
    cfg.sasl_mechanism = Some(SaslMechanism::ScramSha256);
    // username/password not set
    assert!(cfg.validate().is_err());
}

#[test]
fn test_rdkafka_config_at_least_once() {
    let mut cfg = KafkaSinkConfig::default();
    cfg.bootstrap_servers = "b:9092".into();
    cfg.topic = "t".into();
    let rdk = cfg.to_rdkafka_config();
    assert_eq!(rdk.get("enable.idempotence"), Some("true"));
    assert_eq!(rdk.get("acks"), Some("all"));
    assert_eq!(rdk.get("max.in.flight.requests.per.connection"), Some("5"));
    assert!(rdk.get("transactional.id").is_none());
    assert_eq!(rdk.get("security.protocol"), Some("plaintext"));

    let dlq = cfg.to_dlq_rdkafka_config();
    assert_eq!(dlq.get("enable.idempotence"), Some("true"));
    assert_eq!(dlq.get("acks"), Some("all"));
    assert_eq!(dlq.get("max.in.flight.requests.per.connection"), Some("5"));
}

#[test]
fn acks_option_is_rejected_instead_of_downgrading_durability() {
    for value in ["0", "1", "all"] {
        let mut pairs = required_pairs();
        pairs.push(("acks", value));
        let error = KafkaSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
        assert!(error.to_string().contains("always requires acks=all"));
    }
}

#[test]
fn idempotent_producer_rejects_invalid_in_flight_boundaries() {
    for value in ["0", "6", "100"] {
        let mut pairs = required_pairs();
        pairs.push(("max.in.flight.requests", value));
        let error = KafkaSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    }
    for value in ["1", "5"] {
        let mut pairs = required_pairs();
        pairs.push(("max.in.flight.requests", value));
        assert!(KafkaSinkConfig::from_config(&make_config(&pairs)).is_ok());
    }
}

#[test]
fn test_rdkafka_config_with_security() {
    let mut cfg = KafkaSinkConfig::default();
    cfg.bootstrap_servers = "b:9092".into();
    cfg.topic = "t".into();
    cfg.security_protocol = SecurityProtocol::SaslSsl;
    cfg.sasl_mechanism = Some(SaslMechanism::Plain);
    cfg.sasl_username = Some("user".into());
    cfg.sasl_password = Some("pass".into());
    cfg.ssl_ca_location = Some("/ca.pem".into());

    let rdk = cfg.to_rdkafka_config();
    assert_eq!(rdk.get("security.protocol"), Some("sasl_ssl"));
    assert_eq!(rdk.get("sasl.mechanism"), Some("PLAIN"));
    assert_eq!(rdk.get("sasl.username"), Some("user"));
    assert_eq!(rdk.get("sasl.password"), Some("pass"));
    assert_eq!(rdk.get("ssl.ca.location"), Some("/ca.pem"));
}

#[test]
fn test_rdkafka_config_with_batch_num_messages() {
    let mut cfg = KafkaSinkConfig::default();
    cfg.bootstrap_servers = "b:9092".into();
    cfg.topic = "t".into();
    cfg.batch_num_messages = Some(10_000);

    let rdk = cfg.to_rdkafka_config();
    assert_eq!(rdk.get("batch.num.messages"), Some("10000"));
}

#[test]
fn test_kafka_passthrough_properties() {
    let mut pairs = required_pairs();
    pairs.push(("kafka.socket.timeout.ms", "5000"));
    pairs.push(("kafka.queue.buffering.max.messages", "100000"));
    let config = make_config(&pairs);
    let cfg = KafkaSinkConfig::from_config(&config).unwrap();
    assert_eq!(
        cfg.kafka_properties.get("socket.timeout.ms").unwrap(),
        "5000"
    );
}

#[test]
fn passthrough_cannot_disable_delivery_deadline() {
    let mut pairs = required_pairs();
    pairs.push(("kafka.message.timeout.ms", "0"));
    pairs.push(("kafka.delivery.timeout.ms", "0"));
    let config = make_config(&pairs);
    let cfg = KafkaSinkConfig::from_config(&config).unwrap();

    assert_eq!(
        cfg.to_rdkafka_config().get("message.timeout.ms"),
        Some("120000")
    );
    assert_eq!(
        cfg.to_dlq_rdkafka_config().get("message.timeout.ms"),
        Some("120000")
    );
}

#[test]
fn passthrough_cannot_override_idempotent_delivery_invariants() {
    let mut pairs = required_pairs();
    pairs.push(("kafka.acks", "0"));
    pairs.push(("kafka.max.in.flight", "99"));
    pairs.push(("kafka.max.in.flight.requests.per.connection", "99"));
    let cfg = KafkaSinkConfig::from_config(&make_config(&pairs)).unwrap();

    for rdk in [cfg.to_rdkafka_config(), cfg.to_dlq_rdkafka_config()] {
        assert_eq!(rdk.get("acks"), Some("all"));
        assert_eq!(rdk.get("max.in.flight.requests.per.connection"), Some("5"));
    }
}

#[test]
fn test_defaults() {
    let cfg = KafkaSinkConfig::default();
    assert_eq!(cfg.partitioner, PartitionStrategy::KeyHash);
    assert_eq!(cfg.compression, CompressionType::None);
    assert_eq!(cfg.linger_ms, 5);
    assert_eq!(cfg.batch_size, 16_384);
    assert_eq!(cfg.max_in_flight, 5);
    assert_eq!(cfg.delivery_timeout, Duration::from_secs(120));
    assert_eq!(cfg.security_protocol, SecurityProtocol::Plaintext);
    assert!(cfg.sasl_mechanism.is_none());
    assert!(cfg.batch_num_messages.is_none());
}

#[test]
fn test_enum_display() {
    assert_eq!(PartitionStrategy::KeyHash.to_string(), "key-hash");
    assert_eq!(PartitionStrategy::RoundRobin.to_string(), "round-robin");
    assert_eq!(PartitionStrategy::Sticky.to_string(), "sticky");
    assert_eq!(CompressionType::Zstd.to_string(), "zstd");
}

#[test]
fn test_enum_parse() {
    assert_eq!(
        "key-hash".parse::<PartitionStrategy>().unwrap(),
        PartitionStrategy::KeyHash
    );
    assert_eq!(
        "round-robin".parse::<PartitionStrategy>().unwrap(),
        PartitionStrategy::RoundRobin
    );
    assert_eq!(
        "sticky".parse::<PartitionStrategy>().unwrap(),
        PartitionStrategy::Sticky
    );
    assert_eq!(
        "gzip".parse::<CompressionType>().unwrap(),
        CompressionType::Gzip
    );
    assert_eq!(
        "snappy".parse::<CompressionType>().unwrap(),
        CompressionType::Snappy
    );
    assert_eq!(
        "lz4".parse::<CompressionType>().unwrap(),
        CompressionType::Lz4
    );
    assert_eq!(
        "zstd".parse::<CompressionType>().unwrap(),
        CompressionType::Zstd
    );
}
