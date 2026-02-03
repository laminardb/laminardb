//! Kafka sink connector configuration.
//!
//! [`KafkaSinkConfig`] encapsulates all tuning knobs for the Kafka producer,
//! parsed from a SQL `WITH (...)` clause via [`ConnectorConfig`].

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::ClientConfig;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::kafka::config::{CompatibilityLevel, SrAuth};
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
    /// Serialization format.
    pub format: Format,
    /// Delivery guarantee level.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Transactional ID prefix for exactly-once.
    pub transactional_id: Option<String>,
    /// Transaction timeout.
    pub transaction_timeout: Duration,
    /// Key column name for partitioning.
    pub key_column: Option<String>,
    /// Partitioning strategy.
    pub partitioner: PartitionStrategy,
    /// Maximum time to wait before sending a batch (milliseconds).
    pub linger_ms: u64,
    /// Maximum batch size in bytes.
    pub batch_size: usize,
    /// Compression algorithm.
    pub compression: CompressionType,
    /// Acknowledgment level.
    pub acks: Acks,
    /// Maximum number of in-flight requests per connection.
    pub max_in_flight: usize,
    /// Maximum time to wait for delivery confirmation.
    pub delivery_timeout: Duration,
    /// Dead letter queue topic for failed records.
    pub dlq_topic: Option<String>,
    /// Maximum records to buffer before flushing.
    pub flush_batch_size: usize,
    /// Schema Registry URL for Avro/Protobuf.
    pub schema_registry_url: Option<String>,
    /// Schema Registry authentication.
    pub schema_registry_auth: Option<SrAuth>,
    /// Schema compatibility level override.
    pub schema_compatibility: Option<CompatibilityLevel>,
    /// Additional rdkafka client properties (pass-through).
    pub kafka_properties: HashMap<String, String>,
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            topic: String::new(),
            format: Format::Json,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            transactional_id: None,
            transaction_timeout: Duration::from_secs(60),
            key_column: None,
            partitioner: PartitionStrategy::KeyHash,
            linger_ms: 5,
            batch_size: 16_384,
            compression: CompressionType::None,
            acks: Acks::All,
            max_in_flight: 5,
            delivery_timeout: Duration::from_secs(120),
            dlq_topic: None,
            flush_batch_size: 1_000,
            schema_registry_url: None,
            schema_registry_auth: None,
            schema_compatibility: None,
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

        if let Some(fmt) = config.get("format") {
            cfg.format = fmt.parse().map_err(ConnectorError::Serde)?;
        }

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

        if let Some(c) = config.get("compression.type") {
            cfg.compression = c.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid compression.type: '{c}'"))
            })?;
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

        cfg.dlq_topic = config.get("dlq.topic").map(String::from);

        if let Some(v) = config.get("flush.batch.size") {
            cfg.flush_batch_size = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid flush.batch.size: '{v}'"))
            })?;
        }

        cfg.schema_registry_url = config.get("schema.registry.url").map(String::from);

        // Schema Registry auth
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

        // Collect kafka.* pass-through properties (prefix already stripped).
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

        // Exactly-once requires max 5 in-flight for idempotent producer
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

        config
            .set("bootstrap.servers", &self.bootstrap_servers)
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

        // Apply pass-through properties.
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

impl std::str::FromStr for DeliveryGuarantee {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('_', "-").as_str() {
            "at-least-once" | "atleastonce" => Ok(Self::AtLeastOnce),
            "exactly-once" | "exactlyonce" => Ok(Self::ExactlyOnce),
            other => Err(format!("unknown delivery guarantee: '{other}'")),
        }
    }
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AtLeastOnce => write!(f, "at-least-once"),
            Self::ExactlyOnce => write!(f, "exactly-once"),
        }
    }
}

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

impl std::str::FromStr for PartitionStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('_', "-").as_str() {
            "key-hash" | "keyhash" | "hash" => Ok(Self::KeyHash),
            "round-robin" | "roundrobin" => Ok(Self::RoundRobin),
            "sticky" => Ok(Self::Sticky),
            other => Err(format!("unknown partition strategy: '{other}'")),
        }
    }
}

impl std::fmt::Display for PartitionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyHash => write!(f, "key-hash"),
            Self::RoundRobin => write!(f, "round-robin"),
            Self::Sticky => write!(f, "sticky"),
        }
    }
}

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

impl std::str::FromStr for CompressionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            "zstd" | "zstandard" => Ok(Self::Zstd),
            other => Err(format!("unknown compression type: '{other}'")),
        }
    }
}

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

impl std::str::FromStr for Acks {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "0" | "none" => Ok(Self::None),
            "1" | "leader" => Ok(Self::Leader),
            "-1" | "all" => Ok(Self::All),
            other => Err(format!("unknown acks value: '{other}'")),
        }
    }
}

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
            ("compression.type", "zstd"),
            ("acks", "1"),
            ("max.in.flight.requests", "3"),
            ("delivery.timeout.ms", "60000"),
            ("dlq.topic", "my-dlq"),
            ("flush.batch.size", "500"),
            ("schema.registry.url", "http://sr:8081"),
            ("schema.registry.username", "user"),
            ("schema.registry.password", "pass"),
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
        assert_eq!(cfg.compression, CompressionType::Zstd);
        assert_eq!(cfg.acks, Acks::Leader);
        assert_eq!(cfg.max_in_flight, 3);
        assert_eq!(cfg.delivery_timeout, Duration::from_millis(60_000));
        assert_eq!(cfg.dlq_topic.as_deref(), Some("my-dlq"));
        assert_eq!(cfg.flush_batch_size, 500);
        assert_eq!(cfg.schema_registry_url.as_deref(), Some("http://sr:8081"));
        assert!(cfg.schema_registry_auth.is_some());
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
    fn test_rdkafka_config_at_least_once() {
        let mut cfg = KafkaSinkConfig::default();
        cfg.bootstrap_servers = "b:9092".into();
        cfg.topic = "t".into();
        let rdk = cfg.to_rdkafka_config();
        assert_eq!(rdk.get("enable.idempotence"), Some("true"));
        assert!(rdk.get("transactional.id").is_none());
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
