//! Kafka sink connector configuration.
//!
//! [`KafkaSinkConfig`] encapsulates all tuning knobs for the Kafka producer,
//! parsed from the resolved sink [`ConnectorConfig`].

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::ClientConfig;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::kafka::config::{CompatibilityLevel, SaslMechanism, SecurityProtocol, SrAuth};
use crate::serde::Format;

const MAX_DELIVERY_TIMEOUT: Duration = Duration::from_secs(300);

/// Configuration for the Kafka Sink Connector.
///
/// Parsed from resolved sink connector and format options.
///
/// Uses a custom `Debug` impl that redacts `sasl_password` and
/// `ssl_key_password` to prevent credential leakage in logs.
#[derive(Clone)]
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
    /// Schema Registry URL for Avro.
    pub schema_registry_url: Option<String>,
    /// Schema Registry authentication.
    pub schema_registry_auth: Option<SrAuth>,
    /// Schema compatibility level override.
    pub schema_compatibility: Option<CompatibilityLevel>,
    /// Schema Registry SSL CA certificate path.
    pub schema_registry_ssl_ca_location: Option<String>,
    /// Maximum number of in-flight requests per connection.
    pub max_in_flight: usize,
    /// Maximum time to wait for delivery confirmation.
    pub delivery_timeout: Duration,
    /// Key column name for partitioning. In `envelope = upsert` mode this is the merge key
    /// (the group identity), and is required.
    pub key_column: Option<String>,
    /// How an updating (changelog) input is encoded to the topic (`append` vs `upsert`).
    pub envelope: SinkEnvelope,
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
    /// Additional rdkafka client properties (pass-through).
    pub kafka_properties: HashMap<String, String>,
}

/// How the sink encodes an updating (changelog) input to Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SinkEnvelope {
    /// Append-only: rows are produced as-is. Weighted changelogs are rejected during sink
    /// admission and again at the runtime boundary because this envelope cannot carry retractions.
    #[default]
    Append,
    /// Upsert: the Z-set changelog is collapsed per key each batch — a live group becomes a keyed
    /// record, a removed group a null-value tombstone. Consume via a log-compacted topic.
    Upsert,
}

impl std::fmt::Debug for KafkaSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSinkConfig")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("topic", &self.topic)
            .field("format", &self.format)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_password", &self.sasl_password.as_ref().map(|_| "***"))
            .field(
                "ssl_key_password",
                &self.ssl_key_password.as_ref().map(|_| "***"),
            )
            .field("partitioner", &self.partitioner)
            .finish_non_exhaustive()
    }
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
            max_in_flight: 5,
            delivery_timeout: Duration::from_secs(120),
            key_column: None,
            envelope: SinkEnvelope::default(),
            partitioner: PartitionStrategy::KeyHash,
            linger_ms: 5,
            batch_size: 16_384,
            batch_num_messages: None,
            compression: CompressionType::None,
            dlq_topic: None,
            kafka_properties: HashMap::new(),
        }
    }
}

impl KafkaSinkConfig {
    /// Parses a sink config from a resolved [`ConnectorConfig`].
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
            .ok_or_else(|| ConnectorError::missing_config("bootstrap.servers"))?
            .to_string();

        cfg.topic = config
            .get("topic")
            .ok_or_else(|| ConnectorError::missing_config("topic"))?
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

        if config.get("acks").is_some() {
            return Err(ConnectorError::ConfigurationError(
                "'acks' is not configurable; the durable Kafka sink always requires acks=all"
                    .into(),
            ));
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

        cfg.envelope = match config
            .get("envelope")
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("append") => SinkEnvelope::Append,
            Some("upsert") => SinkEnvelope::Upsert,
            Some(other) => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unknown envelope '{other}' (expected 'append' or 'upsert')"
                )));
            }
        };
        if cfg.envelope == SinkEnvelope::Upsert
            && cfg
                .key_column
                .as_deref()
                .is_none_or(|k| k.trim().is_empty())
        {
            return Err(ConnectorError::ConfigurationError(
                "envelope = 'upsert' requires a non-empty 'key.column' (the merge key)".into(),
            ));
        }

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
        if cfg.dlq_topic.is_some() && cfg.envelope == SinkEnvelope::Upsert {
            // Upsert failures poison the epoch; a lone tombstone in a DLQ would corrupt the
            // compacted topic. Reject rather than silently ignore the DLQ.
            return Err(ConnectorError::ConfigurationError(
                "'dlq.topic' is not supported with envelope = 'upsert' (upsert failures poison the \
                 epoch instead of routing to a DLQ)"
                    .into(),
            ));
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
            return Err(ConnectorError::missing_config("bootstrap.servers"));
        }
        if self.topic.is_empty() {
            return Err(ConnectorError::missing_config("topic"));
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

        if self.format == Format::Debezium {
            return Err(ConnectorError::ConfigurationError(
                "Debezium is a deserialization-only format and cannot be used for sinks".into(),
            ));
        }

        if self.format == Format::Avro && self.schema_registry_url.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "Avro format requires 'schema.registry.url'".into(),
            ));
        }

        if !(1..=5).contains(&self.max_in_flight) {
            return Err(ConnectorError::ConfigurationError(
                "max.in.flight.requests must be between 1 and 5 when idempotence is enabled".into(),
            ));
        }

        if self.delivery_timeout.is_zero() {
            return Err(ConnectorError::ConfigurationError(
                "delivery.timeout.ms must be > 0; zero disables librdkafka's delivery deadline"
                    .into(),
            ));
        }
        if self.delivery_timeout > MAX_DELIVERY_TIMEOUT {
            return Err(ConnectorError::ConfigurationError(format!(
                "delivery.timeout.ms must be <= {}",
                MAX_DELIVERY_TIMEOUT.as_millis()
            )));
        }

        Ok(())
    }

    /// Builds an rdkafka [`ClientConfig`] from this configuration.
    ///
    /// Always sets `enable.idempotence=true` for durable at-least-once writes.
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
            .set("acks", "all")
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

        // Apply pass-through properties, blocking security-critical keys
        // that could silently downgrade authentication or break semantics.
        for (key, value) in &self.kafka_properties {
            if is_blocked_passthrough_key(key) {
                tracing::warn!(
                    key,
                    "ignoring kafka.* pass-through property that overrides a protected connector setting"
                );
                continue;
            }
            config.set(key, value);
        }

        config
    }

    /// Builds an rdkafka [`ClientConfig`] for the dead letter queue producer.
    ///
    /// Inherits security settings (SASL, SSL) from the main config but is
    /// non-transactional. Does not set `transactional.id`.
    #[must_use]
    pub fn to_dlq_rdkafka_config(&self) -> ClientConfig {
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
            .set("acks", "all")
            .set(
                "max.in.flight.requests.per.connection",
                self.max_in_flight.to_string(),
            )
            .set(
                "message.timeout.ms",
                self.delivery_timeout.as_millis().to_string(),
            );

        config
    }
}

/// Returns `true` if a pass-through kafka.* key must not override explicit settings.
fn is_blocked_passthrough_key(key: &str) -> bool {
    key.starts_with("sasl.kerberos.")
        || matches!(
            key,
            "security.protocol"
                | "sasl.mechanism"
                | "sasl.username"
                | "sasl.password"
                | "sasl.oauthbearer.config"
                | "ssl.ca.location"
                | "ssl.certificate.location"
                | "ssl.key.location"
                | "ssl.key.password"
                | "ssl.endpoint.identification.algorithm"
                | "enable.auto.commit"
                | "enable.idempotence"
                | "acks"
                | "max.in.flight"
                | "max.in.flight.requests.per.connection"
                | "message.timeout.ms"
                | "delivery.timeout.ms"
                | "transactional.id"
        )
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

#[cfg(test)]
mod tests;
