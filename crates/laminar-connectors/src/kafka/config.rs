//! Kafka source connector configuration.
//!
//! Provides [`KafkaSourceConfig`] for configuring the Kafka consumer,
//! including broker connection, format, Schema Registry, backpressure,
//! and pass-through `rdkafka` properties.

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::config::ClientConfig;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::serde::Format;

/// Auto-offset reset policy for new consumer groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetReset {
    /// Start from the earliest available offset.
    Earliest,
    /// Start from the latest offset (only new messages).
    Latest,
    /// Fail if no committed offset exists.
    None,
}

impl OffsetReset {
    /// Returns the rdkafka config value string.
    #[must_use]
    pub fn as_rdkafka_str(&self) -> &'static str {
        match self {
            OffsetReset::Earliest => "earliest",
            OffsetReset::Latest => "latest",
            OffsetReset::None => "error",
        }
    }
}

impl std::str::FromStr for OffsetReset {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "earliest" | "beginning" => Ok(OffsetReset::Earliest),
            "latest" | "end" => Ok(OffsetReset::Latest),
            "none" | "error" => Ok(OffsetReset::None),
            other => Err(ConnectorError::ConfigurationError(format!(
                "invalid auto.offset.reset: '{other}' (expected earliest/latest/none)"
            ))),
        }
    }
}

/// Kafka partition assignment strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentStrategy {
    /// Range assignment (default).
    Range,
    /// Round-robin assignment.
    RoundRobin,
    /// Cooperative sticky assignment.
    CooperativeSticky,
}

impl AssignmentStrategy {
    /// Returns the rdkafka config value string.
    #[must_use]
    pub fn as_rdkafka_str(&self) -> &'static str {
        match self {
            AssignmentStrategy::Range => "range",
            AssignmentStrategy::RoundRobin => "roundrobin",
            AssignmentStrategy::CooperativeSticky => "cooperative-sticky",
        }
    }
}

impl std::str::FromStr for AssignmentStrategy {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "range" => Ok(AssignmentStrategy::Range),
            "roundrobin" | "round-robin" | "round_robin" => Ok(AssignmentStrategy::RoundRobin),
            "cooperative-sticky" | "cooperative_sticky" => {
                Ok(AssignmentStrategy::CooperativeSticky)
            }
            other => Err(ConnectorError::ConfigurationError(format!(
                "invalid partition.assignment.strategy: '{other}'"
            ))),
        }
    }
}

/// Schema Registry compatibility level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityLevel {
    /// New schema can read old data.
    Backward,
    /// Backward compatible with all prior versions.
    BackwardTransitive,
    /// Old schema can read new data.
    Forward,
    /// Forward compatible with all prior versions.
    ForwardTransitive,
    /// Both backward and forward compatible.
    Full,
    /// Full compatible with all prior versions.
    FullTransitive,
    /// No compatibility checking.
    None,
}

impl CompatibilityLevel {
    /// Returns the Schema Registry API string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            CompatibilityLevel::Backward => "BACKWARD",
            CompatibilityLevel::BackwardTransitive => "BACKWARD_TRANSITIVE",
            CompatibilityLevel::Forward => "FORWARD",
            CompatibilityLevel::ForwardTransitive => "FORWARD_TRANSITIVE",
            CompatibilityLevel::Full => "FULL",
            CompatibilityLevel::FullTransitive => "FULL_TRANSITIVE",
            CompatibilityLevel::None => "NONE",
        }
    }
}

impl std::str::FromStr for CompatibilityLevel {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BACKWARD" => Ok(CompatibilityLevel::Backward),
            "BACKWARD_TRANSITIVE" => Ok(CompatibilityLevel::BackwardTransitive),
            "FORWARD" => Ok(CompatibilityLevel::Forward),
            "FORWARD_TRANSITIVE" => Ok(CompatibilityLevel::ForwardTransitive),
            "FULL" => Ok(CompatibilityLevel::Full),
            "FULL_TRANSITIVE" => Ok(CompatibilityLevel::FullTransitive),
            "NONE" => Ok(CompatibilityLevel::None),
            other => Err(ConnectorError::ConfigurationError(format!(
                "invalid schema.compatibility: '{other}'"
            ))),
        }
    }
}

impl std::fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Schema Registry authentication credentials.
#[derive(Debug, Clone)]
pub struct SrAuth {
    /// Basic auth username.
    pub username: String,
    /// Basic auth password.
    pub password: String,
}

/// Kafka source connector configuration.
#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    // -- Required --
    /// Comma-separated list of broker addresses.
    pub bootstrap_servers: String,
    /// Consumer group identifier.
    pub group_id: String,
    /// Topics to subscribe to.
    pub topics: Vec<String>,

    // -- Format & Schema --
    /// Data format for deserialization.
    pub format: Format,
    /// Confluent Schema Registry URL.
    pub schema_registry_url: Option<String>,
    /// Schema Registry authentication credentials.
    pub schema_registry_auth: Option<SrAuth>,
    /// Override compatibility level for the subject.
    pub schema_compatibility: Option<CompatibilityLevel>,
    /// Column name containing the event timestamp.
    pub event_time_column: Option<String>,
    /// Whether to include Kafka metadata columns (_partition, _offset, _timestamp).
    pub include_metadata: bool,

    // -- Consumer tuning --
    /// Where to start reading when no committed offset exists.
    pub auto_offset_reset: OffsetReset,
    /// Maximum records per poll batch.
    pub max_poll_records: usize,
    /// Timeout for each poll call.
    pub poll_timeout: Duration,
    /// Partition assignment strategy.
    pub partition_assignment_strategy: AssignmentStrategy,
    /// How often to commit offsets to Kafka.
    pub commit_interval: Duration,

    // -- Watermark --
    /// Maximum expected out-of-orderness for watermark generation.
    pub max_out_of_orderness: Duration,
    /// Timeout before marking a partition as idle.
    pub idle_timeout: Duration,

    // -- Backpressure --
    /// Channel fill ratio at which to pause consumption.
    pub backpressure_high_watermark: f64,
    /// Channel fill ratio at which to resume consumption.
    pub backpressure_low_watermark: f64,

    // -- Pass-through --
    /// Additional rdkafka properties passed directly to librdkafka.
    pub kafka_properties: HashMap<String, String>,
}

impl Default for KafkaSourceConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            group_id: String::new(),
            topics: Vec::new(),
            format: Format::Json,
            schema_registry_url: None,
            schema_registry_auth: None,
            schema_compatibility: None,
            event_time_column: None,
            include_metadata: false,
            auto_offset_reset: OffsetReset::Earliest,
            max_poll_records: 1000,
            poll_timeout: Duration::from_millis(100),
            partition_assignment_strategy: AssignmentStrategy::Range,
            commit_interval: Duration::from_secs(5),
            max_out_of_orderness: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            backpressure_high_watermark: 0.8,
            backpressure_low_watermark: 0.5,
            kafka_properties: HashMap::new(),
        }
    }
}

impl KafkaSourceConfig {
    /// Parses a [`KafkaSourceConfig`] from a [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required fields are missing or values are invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let bootstrap_servers = config.require("bootstrap.servers")?.to_string();
        let group_id = config.require("group.id")?.to_string();

        let topics = config.require("topic")?;
        let topics: Vec<String> = topics.split(',').map(|s| s.trim().to_string()).collect();

        let format = match config.get("format") {
            Some(f) => f
                .parse::<Format>()
                .map_err(|e| ConnectorError::ConfigurationError(e.to_string()))?,
            None => Format::Json,
        };

        let schema_registry_url = config.get("schema.registry.url").map(String::from);

        let schema_registry_auth = match (
            config.get("schema.registry.username"),
            config.get("schema.registry.password"),
        ) {
            (Some(u), Some(p)) => Some(SrAuth {
                username: u.to_string(),
                password: p.to_string(),
            }),
            (Some(_), None) | (None, Some(_)) => {
                return Err(ConnectorError::ConfigurationError(
                    "schema.registry.username and schema.registry.password must both be set"
                        .to_string(),
                ));
            }
            (None, None) => None,
        };

        let schema_compatibility = match config.get("schema.compatibility") {
            Some(s) => Some(s.parse::<CompatibilityLevel>()?),
            None => None,
        };

        let event_time_column = config.get("event.time.column").map(String::from);

        let include_metadata = config
            .get_parsed::<bool>("include.metadata")?
            .unwrap_or(false);

        let auto_offset_reset = match config.get("auto.offset.reset") {
            Some(s) => s.parse::<OffsetReset>()?,
            None => OffsetReset::Earliest,
        };

        let max_poll_records = config
            .get_parsed::<usize>("max.poll.records")?
            .unwrap_or(1000);

        let poll_timeout_ms = config.get_parsed::<u64>("poll.timeout.ms")?.unwrap_or(100);

        let partition_assignment_strategy = match config.get("partition.assignment.strategy") {
            Some(s) => s.parse::<AssignmentStrategy>()?,
            None => AssignmentStrategy::Range,
        };

        let commit_interval_ms = config
            .get_parsed::<u64>("commit.interval.ms")?
            .unwrap_or(5000);

        let max_out_of_orderness_ms = config
            .get_parsed::<u64>("max.out.of.orderness.ms")?
            .unwrap_or(5000);

        let idle_timeout_ms = config
            .get_parsed::<u64>("idle.timeout.ms")?
            .unwrap_or(30_000);

        let backpressure_high_watermark = config
            .get_parsed::<f64>("backpressure.high.watermark")?
            .unwrap_or(0.8);

        let backpressure_low_watermark = config
            .get_parsed::<f64>("backpressure.low.watermark")?
            .unwrap_or(0.5);

        let kafka_properties = config.properties_with_prefix("kafka.");

        let cfg = Self {
            bootstrap_servers,
            group_id,
            topics,
            format,
            schema_registry_url,
            schema_registry_auth,
            schema_compatibility,
            event_time_column,
            include_metadata,
            auto_offset_reset,
            max_poll_records,
            poll_timeout: Duration::from_millis(poll_timeout_ms),
            partition_assignment_strategy,
            commit_interval: Duration::from_millis(commit_interval_ms),
            max_out_of_orderness: Duration::from_millis(max_out_of_orderness_ms),
            idle_timeout: Duration::from_millis(idle_timeout_ms),
            backpressure_high_watermark,
            backpressure_low_watermark,
            kafka_properties,
        };

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the configuration is invalid.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.bootstrap_servers.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "bootstrap.servers cannot be empty".into(),
            ));
        }
        if self.group_id.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "group.id cannot be empty".into(),
            ));
        }
        if self.topics.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "at least one topic is required".into(),
            ));
        }
        if self.max_poll_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.poll.records must be > 0".into(),
            ));
        }
        if self.backpressure_high_watermark <= self.backpressure_low_watermark {
            return Err(ConnectorError::ConfigurationError(
                "backpressure.high.watermark must be > backpressure.low.watermark".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.backpressure_high_watermark) {
            return Err(ConnectorError::ConfigurationError(
                "backpressure.high.watermark must be between 0.0 and 1.0".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.backpressure_low_watermark) {
            return Err(ConnectorError::ConfigurationError(
                "backpressure.low.watermark must be between 0.0 and 1.0".into(),
            ));
        }
        if self.format == Format::Avro && self.schema_registry_url.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "schema.registry.url is required when format is 'avro'".into(),
            ));
        }
        Ok(())
    }

    /// Builds an rdkafka [`ClientConfig`] from this configuration.
    #[must_use]
    pub fn to_rdkafka_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);
        config.set("group.id", &self.group_id);
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", self.auto_offset_reset.as_rdkafka_str());
        config.set(
            "partition.assignment.strategy",
            self.partition_assignment_strategy.as_rdkafka_str(),
        );

        // Apply pass-through properties (these can override defaults).
        for (key, value) in &self.kafka_properties {
            config.set(key, value);
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_parse_required_fields() {
        let cfg = KafkaSourceConfig::from_config(&make_config(&[])).unwrap();
        assert_eq!(cfg.bootstrap_servers, "localhost:9092");
        assert_eq!(cfg.group_id, "test-group");
        assert_eq!(cfg.topics, vec!["events"]);
    }

    #[test]
    fn test_parse_missing_required() {
        let config = ConnectorConfig::new("kafka");
        assert!(KafkaSourceConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_parse_multi_topic() {
        let cfg = KafkaSourceConfig::from_config(&make_config(&[("topic", "a, b, c")])).unwrap();
        assert_eq!(cfg.topics, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_defaults() {
        let cfg = KafkaSourceConfig::from_config(&make_config(&[])).unwrap();
        assert_eq!(cfg.format, Format::Json);
        assert_eq!(cfg.auto_offset_reset, OffsetReset::Earliest);
        assert_eq!(cfg.max_poll_records, 1000);
        assert_eq!(cfg.poll_timeout, Duration::from_millis(100));
        assert_eq!(cfg.partition_assignment_strategy, AssignmentStrategy::Range);
        assert_eq!(cfg.commit_interval, Duration::from_secs(5));
        assert!(!cfg.include_metadata);
        assert!(cfg.schema_registry_url.is_none());
    }

    #[test]
    fn test_parse_optional_fields() {
        let cfg = KafkaSourceConfig::from_config(&make_config(&[
            ("format", "csv"),
            ("auto.offset.reset", "latest"),
            ("max.poll.records", "500"),
            ("poll.timeout.ms", "200"),
            ("commit.interval.ms", "10000"),
            ("include.metadata", "true"),
            ("event.time.column", "ts"),
            ("partition.assignment.strategy", "roundrobin"),
        ]))
        .unwrap();

        assert_eq!(cfg.format, Format::Csv);
        assert_eq!(cfg.auto_offset_reset, OffsetReset::Latest);
        assert_eq!(cfg.max_poll_records, 500);
        assert_eq!(cfg.poll_timeout, Duration::from_millis(200));
        assert_eq!(cfg.commit_interval, Duration::from_secs(10));
        assert!(cfg.include_metadata);
        assert_eq!(cfg.event_time_column, Some("ts".to_string()));
        assert_eq!(
            cfg.partition_assignment_strategy,
            AssignmentStrategy::RoundRobin
        );
    }

    #[test]
    fn test_parse_kafka_passthrough() {
        let cfg = KafkaSourceConfig::from_config(&make_config(&[
            ("kafka.session.timeout.ms", "30000"),
            ("kafka.max.poll.interval.ms", "300000"),
        ]))
        .unwrap();

        assert_eq!(cfg.kafka_properties.len(), 2);
        assert_eq!(
            cfg.kafka_properties.get("session.timeout.ms"),
            Some(&"30000".to_string())
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
        cfg.topics = vec!["t".into()];
        cfg.format = Format::Avro;
        // No schema_registry_url
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_backpressure_watermarks() {
        let mut cfg = KafkaSourceConfig::default();
        cfg.bootstrap_servers = "localhost:9092".into();
        cfg.group_id = "g".into();
        cfg.topics = vec!["t".into()];
        cfg.backpressure_high_watermark = 0.3;
        cfg.backpressure_low_watermark = 0.5;
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
}
