//! Kafka sink connector configuration.
//!
//! [`KafkaSinkConfig`] encapsulates all tuning knobs for the Kafka producer,
//! parsed from a SQL `WITH (...)` clause via [`ConnectorConfig`].

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::ClientConfig;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::kafka::config::{CompatibilityLevel, SaslMechanism, SecurityProtocol, SrAuth};
use crate::serde::Format;

/// Configuration for the Kafka Sink Connector.
///
/// Parsed from SQL `WITH (...)` clause options.
#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    /// Kafka broker addresses (comma-separated).
    pub bootstrap_servers: String,
    /// Target Kafka topic name.
    pub topic: String,
    /// Security protocol for broker connections.
    pub security_protocol: SecurityProtocol,
    /// SASL authentication mechanism.
    pub sasl_mechanism: Option<SaslMechanism>,
    /// SASL username (for PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
    pub sasl_username: Option<String>,
    /// SASL password (for PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
    pub sasl_password: Option<String>,
    /// Path to SSL CA certificate file (PEM format).
    pub ssl_ca_location: Option<String>,
    /// Path to client SSL certificate file (PEM format).
    pub ssl_certificate_location: Option<String>,
    /// Path to client SSL private key file (PEM format).
    pub ssl_key_location: Option<String>,
    /// Password for encrypted SSL private key.
    pub ssl_key_password: Option<String>,
    /// Serialization format.
    pub format: Format,
    /// Schema Registry URL for Avro/Protobuf.
    pub schema_registry_url: Option<String>,
    /// Schema Registry authentication.
    pub schema_registry_auth: Option<SrAuth>,
    /// Schema compatibility level override.
    pub schema_compatibility: Option<CompatibilityLevel>,
    /// Schema Registry SSL CA certificate path.
    pub schema_registry_ssl_ca_location: Option<String>,
    /// Delivery guarantee level.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Transactional ID prefix for exactly-once.
    pub transactional_id: Option<String>,
    /// Transaction timeout.
    pub transaction_timeout: Duration,
    /// Acknowledgment level.
    pub acks: Acks,
    /// Maximum number of in-flight requests per connection.
    pub max_in_flight: usize,
    /// Maximum time to wait for delivery confirmation.
    pub delivery_timeout: Duration,
    /// Key column name for partitioning.
    pub key_column: Option<String>,
    /// Partitioning strategy.
    pub partitioner: PartitionStrategy,
    /// Maximum time to wait before sending a batch (milliseconds).
    pub linger_ms: u64,
    /// Maximum batch size in bytes.
    pub batch_size: usize,
    /// Maximum number of messages per batch.
    pub batch_num_messages: Option<usize>,
    /// Compression algorithm.
    pub compression: CompressionType,
    /// Dead letter queue topic for failed records.
    pub dlq_topic: Option<String>,
    /// Maximum records to buffer before flushing.
    pub flush_batch_size: usize,
    /// Additional rdkafka client properties (pass-through).
    pub kafka_properties: HashMap<String, String>,
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            topic: String::new(),
            security_protocol: SecurityProtocol::default(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            format: Format::Json,
            schema_registry_url: None,
            schema_registry_auth: None,
            schema_compatibility: None,
            schema_registry_ssl_ca_location: None,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            transactional_id: None,
            transaction_timeout: Duration::from_secs(60),
            acks: Acks::All,
            max_in_flight: 5,
            delivery_timeout: Duration::from_secs(120),
            key_column: None,
            partitioner: PartitionStrategy::KeyHash,
            linger_ms: 5,
            batch_size: 16_384,
            batch_num_messages: None,
            compression: CompressionType::None,
            dlq_topic: None,
            flush_batch_size: 1_000,
            kafka_properties: HashMap::new(),
        }
    }
}

impl KafkaSinkConfig {
    /// Parses a sink config from a [`ConnectorConfig`] (SQL WITH clause).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if required keys are absent,
    /// or `ConnectorError::ConfigurationError` on invalid values.
    #[allow(clippy::too_many_lines, clippy::field_reassign_with_default)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self::default();

        cfg.bootstrap_servers = config
            .get("bootstrap.servers")
            .ok_or_else(|| ConnectorError::MissingConfig("bootstrap.servers".into()))?
            .to_string();

        cfg.topic = config
            .get("topic")
            .ok_or_else(|| ConnectorError::MissingConfig("topic".into()))?
            .to_string();

        if let Some(s) = config.get("security.protocol") {
            cfg.security_protocol = s.parse()?;
        }

        if let Some(s) = config.get("sasl.mechanism") {
            cfg.sasl_mechanism = Some(s.parse()?);
        }

        cfg.sasl_username = config.get("sasl.username").map(String::from);
        cfg.sasl_password = config.get("sasl.password").map(String::from);
        cfg.ssl_ca_location = config.get("ssl.ca.location").map(String::from);
        cfg.ssl_certificate_location = config.get("ssl.certificate.location").map(String::from);
        cfg.ssl_key_location = config.get("ssl.key.location").map(String::from);
        cfg.ssl_key_password = config.get("ssl.key.password").map(String::from);

        if let Some(fmt) = config.get("format") {
            cfg.format = fmt.parse().map_err(ConnectorError::Serde)?;
        }

        cfg.schema_registry_url = config.get("schema.registry.url").map(String::from);

        let sr_user = config.get("schema.registry.username");
        let sr_pass = config.get("schema.registry.password");
        if let (Some(user), Some(pass)) = (sr_user, sr_pass) {
            cfg.schema_registry_auth = Some(SrAuth {
                username: user.to_string(),
                password: pass.to_string(),
            });
        }

        if let Some(c) = config.get("schema.compatibility") {
            cfg.schema_compatibility = Some(c.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid schema.compatibility: '{c}'"))
            })?);
        }

        cfg.schema_registry_ssl_ca_location = config
            .get("schema.registry.ssl.ca.location")
            .map(String::from);

        if let Some(dg) = config.get("delivery.guarantee") {
            cfg.delivery_guarantee = dg.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid delivery.guarantee: '{dg}' (expected 'at-least-once' or 'exactly-once')"
                ))
            })?;
        }

        cfg.transactional_id = config.get("transactional.id").map(String::from);

        if let Some(v) = config.get("transaction.timeout.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid transaction.timeout.ms: '{v}'"))
            })?;
            cfg.transaction_timeout = Duration::from_millis(ms);
        }

        if let Some(a) = config.get("acks") {
            cfg.acks = a.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid acks: '{a}' (expected 'all', '1', or '0')"
                ))
            })?;
        }

        if let Some(v) = config.get("max.in.flight.requests") {
            cfg.max_in_flight = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid max.in.flight.requests: '{v}'"))
            })?;
        }

        if let Some(v) = config.get("delivery.timeout.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid delivery.timeout.ms: '{v}'"))
            })?;
            cfg.delivery_timeout = Duration::from_millis(ms);
        }

        cfg.key_column = config.get("key.column").map(String::from);

        if let Some(p) = config.get("partitioner") {
            cfg.partitioner = p.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid partitioner: '{p}' (expected 'key-hash', 'round-robin', or 'sticky')"
                ))
            })?;
        }

        if let Some(v) = config.get("linger.ms") {
            cfg.linger_ms = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid linger.ms: '{v}'"))
            })?;
        }

        if let Some(v) = config.get("batch.size") {
            cfg.batch_size = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid batch.size: '{v}'"))
            })?;
        }

        if let Some(v) = config.get("batch.num.messages") {
            cfg.batch_num_messages = Some(v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid batch.num.messages: '{v}'"))
            })?);
        }

        if let Some(c) = config.get("compression.type") {
            cfg.compression = c.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid compression.type: '{c}'"))
            })?;
        }

        cfg.dlq_topic = config.get("dlq.topic").map(String::from);

        if let Some(v) = config.get("flush.batch.size") {
            cfg.flush_batch_size = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid flush.batch.size: '{v}'"))
            })?;
        }

        for (key, value) in config.properties_with_prefix("kafka.") {
            cfg.kafka_properties.insert(key, value);
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on invalid combinations.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.bootstrap_servers.is_empty() {
            return Err(ConnectorError::MissingConfig("bootstrap.servers".into()));
        }
        if self.topic.is_empty() {
            return Err(ConnectorError::MissingConfig("topic".into()));
        }

        if self.security_protocol.uses_sasl() && self.sasl_mechanism.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "sasl.mechanism is required when security.protocol is sasl_plaintext or sasl_ssl"
                    .into(),
            ));
        }

        if let Some(mechanism) = &self.sasl_mechanism {
            if mechanism.requires_credentials()
                && (self.sasl_username.is_none() || self.sasl_password.is_none())
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "sasl.username and sasl.password are required for {mechanism} mechanism"
                )));
            }
        }

        if self.security_protocol.uses_ssl() {
            if let Some(ref ca) = self.ssl_ca_location {
                if ca.is_empty() {
                    return Err(ConnectorError::ConfigurationError(
                        "ssl.ca.location cannot be empty when specified".into(),
                    ));
                }
            }
        }

        if self.format == Format::Avro && self.schema_registry_url.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "Avro format requires 'schema.registry.url'".into(),
            ));
        }

        if self.max_in_flight == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.in.flight.requests must be > 0".into(),
            ));
        }

        if self.delivery_guarantee == DeliveryGuarantee::ExactlyOnce && self.max_in_flight > 5 {
            return Err(ConnectorError::ConfigurationError(
                "exactly-once requires max.in.flight.requests <= 5".into(),
            ));
        }

        Ok(())
    }

    /// Builds an rdkafka [`ClientConfig`] from this configuration.
    ///
    /// Always sets `enable.idempotence=true`. For exactly-once delivery,
    /// also sets `transactional.id` and `transaction.timeout.ms`.
    #[must_use]
    pub fn to_rdkafka_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        config.set("bootstrap.servers", &self.bootstrap_servers);
        config.set("security.protocol", self.security_protocol.as_rdkafka_str());

        if let Some(ref mechanism) = self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism.as_rdkafka_str());
        }

        if let Some(ref username) = self.sasl_username {
            config.set("sasl.username", username);
        }

        if let Some(ref password) = self.sasl_password {
            config.set("sasl.password", password);
        }

        if let Some(ref ca) = self.ssl_ca_location {
            config.set("ssl.ca.location", ca);
        }

        if let Some(ref cert) = self.ssl_certificate_location {
            config.set("ssl.certificate.location", cert);
        }

        if let Some(ref key) = self.ssl_key_location {
            config.set("ssl.key.location", key);
        }

        if let Some(ref key_pass) = self.ssl_key_password {
            config.set("ssl.key.password", key_pass);
        }

        config
            .set("enable.idempotence", "true")
            .set("acks", self.acks.as_rdkafka_str())
            .set("linger.ms", self.linger_ms.to_string())
            .set("batch.size", self.batch_size.to_string())
            .set("compression.type", self.compression.as_rdkafka_str())
            .set(
                "max.in.flight.requests.per.connection",
                self.max_in_flight.to_string(),
            )
            .set(
                "message.timeout.ms",
                self.delivery_timeout.as_millis().to_string(),
            );

        if let Some(num_msgs) = self.batch_num_messages {
            config.set("batch.num.messages", num_msgs.to_string());
        }

        if self.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            let txn_id = self
                .transactional_id
                .clone()
                .unwrap_or_else(|| format!("laminardb-sink-{}", self.topic));
            config.set("transactional.id", txn_id);
            config.set(
                "transaction.timeout.ms",
                self.transaction_timeout.as_millis().to_string(),
            );
        }

        // Apply pass-through properties (can override any of the above).
        for (key, value) in &self.kafka_properties {
            config.set(key, value);
        }

        config
    }
}

/// Delivery guarantee level for the Kafka sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// At-least-once: idempotent producer, no transactions.
    AtLeastOnce,
    /// Exactly-once: transactional producer with epoch-aligned commits.
    ExactlyOnce,
}

str_enum!(DeliveryGuarantee, lowercase_udash, String, "unknown delivery guarantee",
    AtLeastOnce => "at-least-once", "atleastonce";
    ExactlyOnce => "exactly-once", "exactlyonce"
);

/// Partitioning strategy for distributing records across Kafka partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    /// Hash the key column (Murmur2, Kafka-compatible).
    KeyHash,
    /// Round-robin across all partitions.
    RoundRobin,
    /// Sticky: batch records to the same partition until full.
    Sticky,
}

str_enum!(PartitionStrategy, lowercase_udash, String, "unknown partition strategy",
    KeyHash => "key-hash", "keyhash", "hash";
    RoundRobin => "round-robin", "roundrobin";
    Sticky => "sticky"
);

/// Compression type for produced Kafka messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression.
    None,
    /// Gzip compression.
    Gzip,
    /// Snappy compression.
    Snappy,
    /// LZ4 compression.
    Lz4,
    /// Zstandard compression.
    Zstd,
}

impl CompressionType {
    /// Returns the rdkafka configuration string.
    #[must_use]
    pub fn as_rdkafka_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

str_enum!(fromstr CompressionType, lowercase_nodash, String, "unknown compression type",
    None => "none";
    Gzip => "gzip";
    Snappy => "snappy";
    Lz4 => "lz4";
    Zstd => "zstd", "zstandard"
);

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_rdkafka_str())
    }
}

/// Acknowledgment level for Kafka producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    /// No acknowledgment (fire-and-forget).
    None,
    /// Leader acknowledgment only.
    Leader,
    /// All in-sync replica acknowledgment.
    All,
}

impl Acks {
    /// Returns the rdkafka configuration string.
    #[must_use]
    pub fn as_rdkafka_str(&self) -> &'static str {
        match self {
            Self::None => "0",
            Self::Leader => "1",
            Self::All => "all",
        }
    }
}

str_enum!(fromstr Acks, lowercase_nodash, String, "unknown acks value",
    None => "0", "none";
    Leader => "1", "leader";
    All => "-1", "all"
);

impl std::fmt::Display for Acks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_rdkafka_str())
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
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
    fn test_parse_delivery_guarantee() {
        let mut pairs = required_pairs();
        pairs.push(("delivery.guarantee", "exactly-once"));
        let config = make_config(&pairs);
        let cfg = KafkaSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
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
            ("delivery.guarantee", "exactly-once"),
            ("transactional.id", "my-txn"),
            ("transaction.timeout.ms", "30000"),
            ("key.column", "order_id"),
            ("partitioner", "round-robin"),
            ("linger.ms", "10"),
            ("batch.size", "32768"),
            ("batch.num.messages", "5000"),
            ("compression.type", "zstd"),
            ("acks", "1"),
            ("max.in.flight.requests", "3"),
            ("delivery.timeout.ms", "60000"),
            ("dlq.topic", "my-dlq"),
            ("flush.batch.size", "500"),
            ("schema.registry.url", "http://sr:8081"),
            ("schema.registry.username", "user"),
            ("schema.registry.password", "pass"),
            ("schema.registry.ssl.ca.location", "/etc/ssl/sr-ca.pem"),
        ]);
        let config = make_config(&pairs);
        let cfg = KafkaSinkConfig::from_config(&config).unwrap();

        assert_eq!(cfg.format, Format::Avro);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
        assert_eq!(cfg.transactional_id.as_deref(), Some("my-txn"));
        assert_eq!(cfg.transaction_timeout, Duration::from_millis(30_000));
        assert_eq!(cfg.key_column.as_deref(), Some("order_id"));
        assert_eq!(cfg.partitioner, PartitionStrategy::RoundRobin);
        assert_eq!(cfg.linger_ms, 10);
        assert_eq!(cfg.batch_size, 32_768);
        assert_eq!(cfg.batch_num_messages, Some(5000));
        assert_eq!(cfg.compression, CompressionType::Zstd);
        assert_eq!(cfg.acks, Acks::Leader);
        assert_eq!(cfg.max_in_flight, 3);
        assert_eq!(cfg.delivery_timeout, Duration::from_millis(60_000));
        assert_eq!(cfg.dlq_topic.as_deref(), Some("my-dlq"));
        assert_eq!(cfg.flush_batch_size, 500);
        assert_eq!(cfg.schema_registry_url.as_deref(), Some("http://sr:8081"));
        assert!(cfg.schema_registry_auth.is_some());
        assert_eq!(
            cfg.schema_registry_ssl_ca_location,
            Some("/etc/ssl/sr-ca.pem".to_string())
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
    fn test_validate_exactly_once_max_in_flight() {
        let mut cfg = KafkaSinkConfig::default();
        cfg.bootstrap_servers = "b:9092".into();
        cfg.topic = "t".into();
        cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        cfg.max_in_flight = 10;
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
        assert!(rdk.get("transactional.id").is_none());
        assert_eq!(rdk.get("security.protocol"), Some("plaintext"));
    }

    #[test]
    fn test_rdkafka_config_exactly_once() {
        let mut cfg = KafkaSinkConfig::default();
        cfg.bootstrap_servers = "b:9092".into();
        cfg.topic = "t".into();
        cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let rdk = cfg.to_rdkafka_config();
        assert_eq!(rdk.get("enable.idempotence"), Some("true"));
        assert!(rdk.get("transactional.id").is_some());
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
    fn test_defaults() {
        let cfg = KafkaSinkConfig::default();
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
        assert_eq!(cfg.partitioner, PartitionStrategy::KeyHash);
        assert_eq!(cfg.compression, CompressionType::None);
        assert_eq!(cfg.acks, Acks::All);
        assert_eq!(cfg.linger_ms, 5);
        assert_eq!(cfg.batch_size, 16_384);
        assert_eq!(cfg.max_in_flight, 5);
        assert_eq!(cfg.flush_batch_size, 1_000);
        assert_eq!(cfg.security_protocol, SecurityProtocol::Plaintext);
        assert!(cfg.sasl_mechanism.is_none());
        assert!(cfg.batch_num_messages.is_none());
    }

    #[test]
    fn test_enum_display() {
        assert_eq!(DeliveryGuarantee::AtLeastOnce.to_string(), "at-least-once");
        assert_eq!(DeliveryGuarantee::ExactlyOnce.to_string(), "exactly-once");
        assert_eq!(PartitionStrategy::KeyHash.to_string(), "key-hash");
        assert_eq!(PartitionStrategy::RoundRobin.to_string(), "round-robin");
        assert_eq!(PartitionStrategy::Sticky.to_string(), "sticky");
        assert_eq!(CompressionType::Zstd.to_string(), "zstd");
        assert_eq!(Acks::All.to_string(), "all");
    }

    #[test]
    fn test_enum_parse() {
        assert_eq!(
            "at-least-once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            "exactly-once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::ExactlyOnce
        );
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
        assert_eq!("all".parse::<Acks>().unwrap(), Acks::All);
        assert_eq!("1".parse::<Acks>().unwrap(), Acks::Leader);
        assert_eq!("0".parse::<Acks>().unwrap(), Acks::None);
    }
}
