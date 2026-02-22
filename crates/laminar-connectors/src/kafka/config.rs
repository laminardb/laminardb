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

/// Kafka security protocol for broker connections.
///
/// Determines encryption (SSL/TLS) and authentication (SASL) requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecurityProtocol {
    /// Plain-text communication (no encryption, no authentication).
    #[default]
    Plaintext,
    /// SSL/TLS encryption without SASL authentication.
    Ssl,
    /// SASL authentication over plain-text connection.
    SaslPlaintext,
    /// SASL authentication over SSL/TLS encrypted connection.
    SaslSsl,
}

impl SecurityProtocol {
    /// Returns the rdkafka config value string.
    #[must_use]
    pub fn as_rdkafka_str(&self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "plaintext",
            SecurityProtocol::Ssl => "ssl",
            SecurityProtocol::SaslPlaintext => "sasl_plaintext",
            SecurityProtocol::SaslSsl => "sasl_ssl",
        }
    }

    /// Returns true if this protocol uses SSL/TLS.
    #[must_use]
    pub fn uses_ssl(&self) -> bool {
        matches!(self, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl)
    }

    /// Returns true if this protocol uses SASL authentication.
    #[must_use]
    pub fn uses_sasl(&self) -> bool {
        matches!(
            self,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        )
    }
}

str_enum!(fromstr SecurityProtocol, lowercase, ConnectorError, "invalid security.protocol",
    Plaintext => "plaintext";
    Ssl => "ssl";
    SaslPlaintext => "sasl_plaintext";
    SaslSsl => "sasl_ssl"
);

impl std::fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_rdkafka_str())
    }
}

/// SASL authentication mechanism for Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SaslMechanism {
    /// PLAIN: Simple username/password authentication.
    #[default]
    Plain,
    /// SCRAM-SHA-256: Salted Challenge Response Authentication Mechanism.
    ScramSha256,
    /// SCRAM-SHA-512: Salted Challenge Response Authentication Mechanism (stronger).
    ScramSha512,
    /// GSSAPI: Kerberos authentication.
    Gssapi,
    /// OAUTHBEARER: OAuth 2.0 bearer token authentication.
    Oauthbearer,
}

impl SaslMechanism {
    /// Returns the rdkafka config value string.
    #[must_use]
    pub fn as_rdkafka_str(&self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            SaslMechanism::Gssapi => "GSSAPI",
            SaslMechanism::Oauthbearer => "OAUTHBEARER",
        }
    }

    /// Returns true if this mechanism requires username/password.
    #[must_use]
    pub fn requires_credentials(&self) -> bool {
        matches!(
            self,
            SaslMechanism::Plain | SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512
        )
    }
}

str_enum!(fromstr SaslMechanism, uppercase, ConnectorError, "invalid sasl.mechanism",
    Plain => "PLAIN";
    ScramSha256 => "SCRAM_SHA_256", "SCRAM_SHA256";
    ScramSha512 => "SCRAM_SHA_512", "SCRAM_SHA512";
    Gssapi => "GSSAPI", "KERBEROS";
    Oauthbearer => "OAUTHBEARER", "OAUTH"
);

impl std::fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_rdkafka_str())
    }
}

/// Consumer isolation level for reading transactional messages.
///
/// Controls whether to read uncommitted messages from transactional producers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Read all messages including uncommitted transactional messages.
    ReadUncommitted,
    /// Only read committed messages (recommended for transactional pipelines).
    #[default]
    ReadCommitted,
}

impl IsolationLevel {
    /// Returns the rdkafka config value string.
    #[must_use]
    pub fn as_rdkafka_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "read_uncommitted",
            IsolationLevel::ReadCommitted => "read_committed",
        }
    }
}

str_enum!(fromstr IsolationLevel, lowercase, ConnectorError, "invalid isolation.level",
    ReadUncommitted => "read_uncommitted";
    ReadCommitted => "read_committed"
);

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_rdkafka_str())
    }
}

/// Consumer startup mode controlling where to begin consuming.
///
/// This is a higher-level abstraction than `auto.offset.reset` that provides
/// more control over initial positioning, including timestamp-based and
/// partition-specific offset assignment.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StartupMode {
    /// Use committed group offsets, fall back to `auto.offset.reset` if none exist.
    #[default]
    GroupOffsets,
    /// Start from the earliest available offset in each partition.
    Earliest,
    /// Start from the latest offset in each partition (only new messages).
    Latest,
    /// Start from specific offsets per partition (`partition_id` -> offset).
    SpecificOffsets(HashMap<i32, i64>),
    /// Start from a specific timestamp (milliseconds since epoch).
    /// The consumer seeks to the first message with timestamp >= this value.
    Timestamp(i64),
}

impl StartupMode {
    /// Returns true if this mode overrides auto.offset.reset behavior.
    #[must_use]
    pub fn overrides_offset_reset(&self) -> bool {
        !matches!(self, StartupMode::GroupOffsets)
    }

    /// Returns the equivalent auto.offset.reset value, if applicable.
    #[must_use]
    pub fn as_offset_reset(&self) -> Option<&'static str> {
        match self {
            StartupMode::Earliest => Some("earliest"),
            StartupMode::Latest => Some("latest"),
            StartupMode::GroupOffsets
            | StartupMode::SpecificOffsets(_)
            | StartupMode::Timestamp(_) => None,
        }
    }
}

impl std::str::FromStr for StartupMode {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "group_offsets" | "group" => Ok(StartupMode::GroupOffsets),
            "earliest" => Ok(StartupMode::Earliest),
            "latest" => Ok(StartupMode::Latest),
            other => Err(ConnectorError::ConfigurationError(format!(
                "invalid startup.mode: '{other}' (expected group-offsets/earliest/latest)"
            ))),
        }
    }
}

impl std::fmt::Display for StartupMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartupMode::GroupOffsets => write!(f, "group-offsets"),
            StartupMode::Earliest => write!(f, "earliest"),
            StartupMode::Latest => write!(f, "latest"),
            StartupMode::SpecificOffsets(offsets) => {
                write!(f, "specific-offsets({} partitions)", offsets.len())
            }
            StartupMode::Timestamp(ts) => write!(f, "timestamp({ts})"),
        }
    }
}

/// Topic subscription mode: explicit list or regex pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicSubscription {
    /// Subscribe to a specific list of topic names.
    Topics(Vec<String>),
    /// Subscribe to topics matching a regex pattern (e.g., `events-.*`).
    Pattern(String),
}

impl TopicSubscription {
    /// Returns the topic names if this is a `Topics` subscription.
    #[must_use]
    pub fn topics(&self) -> Option<&[String]> {
        match self {
            TopicSubscription::Topics(t) => Some(t),
            TopicSubscription::Pattern(_) => None,
        }
    }

    /// Returns the pattern if this is a `Pattern` subscription.
    #[must_use]
    pub fn pattern(&self) -> Option<&str> {
        match self {
            TopicSubscription::Topics(_) => None,
            TopicSubscription::Pattern(p) => Some(p),
        }
    }

    /// Returns true if this is a pattern-based subscription.
    #[must_use]
    pub fn is_pattern(&self) -> bool {
        matches!(self, TopicSubscription::Pattern(_))
    }
}

impl Default for TopicSubscription {
    fn default() -> Self {
        TopicSubscription::Topics(Vec::new())
    }
}

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

str_enum!(fromstr OffsetReset, lowercase_nodash, ConnectorError, "invalid auto.offset.reset",
    Earliest => "earliest", "beginning";
    Latest => "latest", "end";
    None => "none", "error"
);

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

str_enum!(fromstr AssignmentStrategy, lowercase_nodash, ConnectorError,
    "invalid partition.assignment.strategy",
    Range => "range";
    RoundRobin => "roundrobin", "round-robin", "round_robin";
    CooperativeSticky => "cooperative-sticky", "cooperative_sticky"
);

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

str_enum!(fromstr CompatibilityLevel, uppercase, ConnectorError, "invalid schema.compatibility",
    Backward => "BACKWARD";
    BackwardTransitive => "BACKWARD_TRANSITIVE";
    Forward => "FORWARD";
    ForwardTransitive => "FORWARD_TRANSITIVE";
    Full => "FULL";
    FullTransitive => "FULL_TRANSITIVE";
    None => "NONE"
);

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
    /// Topic subscription (explicit list or regex pattern).
    pub subscription: TopicSubscription,
    /// Topics to subscribe to (deprecated: use `subscription` instead).
    #[deprecated(since = "0.2.0", note = "use `subscription` field instead")]
    pub topics: Vec<String>,

    // -- Security --
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

    // -- Format & Schema --
    /// Data format for deserialization.
    pub format: Format,
    /// Confluent Schema Registry URL.
    pub schema_registry_url: Option<String>,
    /// Schema Registry authentication credentials.
    pub schema_registry_auth: Option<SrAuth>,
    /// Override compatibility level for the subject.
    pub schema_compatibility: Option<CompatibilityLevel>,
    /// Schema Registry SSL CA certificate path.
    pub schema_registry_ssl_ca_location: Option<String>,
    /// Schema Registry SSL client certificate path.
    pub schema_registry_ssl_certificate_location: Option<String>,
    /// Schema Registry SSL client key path.
    pub schema_registry_ssl_key_location: Option<String>,
    /// Column name containing the event timestamp.
    pub event_time_column: Option<String>,
    /// Whether to include Kafka metadata columns (_partition, _offset, _timestamp).
    pub include_metadata: bool,
    /// Whether to include Kafka headers as a map column (_headers).
    pub include_headers: bool,

    // -- Consumer tuning --
    /// Consumer startup mode (controls initial offset positioning).
    pub startup_mode: StartupMode,
    /// Where to start reading when no committed offset exists.
    pub auto_offset_reset: OffsetReset,
    /// Consumer transaction isolation level.
    pub isolation_level: IsolationLevel,
    /// Maximum records per poll batch.
    pub max_poll_records: usize,
    /// Timeout for each poll call.
    pub poll_timeout: Duration,
    /// Partition assignment strategy.
    pub partition_assignment_strategy: AssignmentStrategy,
    /// How often to commit offsets to Kafka.
    pub commit_interval: Duration,
    /// Minimum bytes to return from a fetch (allows batching).
    pub fetch_min_bytes: Option<i32>,
    /// Maximum bytes to return from broker per request.
    pub fetch_max_bytes: Option<i32>,
    /// Maximum time broker waits for fetch.min.bytes.
    pub fetch_max_wait_ms: Option<i32>,
    /// Maximum bytes per partition to return from broker.
    pub max_partition_fetch_bytes: Option<i32>,

    // -- Watermark --
    /// Maximum expected out-of-orderness for watermark generation.
    pub max_out_of_orderness: Duration,
    /// Timeout before marking a partition as idle.
    pub idle_timeout: Duration,
    /// Enable per-partition watermark tracking (integrates with watermark tracking).
    pub enable_watermark_tracking: bool,
    /// Alignment group ID for multi-source coordination (integrates with watermark tracking).
    pub alignment_group_id: Option<String>,
    /// Maximum allowed drift between sources in alignment group.
    pub alignment_max_drift: Option<Duration>,
    /// Enforcement mode for watermark alignment.
    pub alignment_mode: Option<super::watermarks::KafkaAlignmentMode>,

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
    #[allow(deprecated)]
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            group_id: String::new(),
            subscription: TopicSubscription::default(),
            topics: Vec::new(),
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
            schema_registry_ssl_certificate_location: None,
            schema_registry_ssl_key_location: None,
            event_time_column: None,
            include_metadata: false,
            include_headers: false,
            startup_mode: StartupMode::default(),
            auto_offset_reset: OffsetReset::Earliest,
            isolation_level: IsolationLevel::default(),
            max_poll_records: 1000,
            poll_timeout: Duration::from_millis(100),
            partition_assignment_strategy: AssignmentStrategy::Range,
            commit_interval: Duration::from_secs(5),
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_wait_ms: None,
            max_partition_fetch_bytes: None,
            max_out_of_orderness: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            enable_watermark_tracking: false,
            alignment_group_id: None,
            alignment_max_drift: None,
            alignment_mode: None,
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
    #[allow(deprecated, clippy::too_many_lines)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let bootstrap_servers = config.require("bootstrap.servers")?.to_string();
        let group_id = config.require("group.id")?.to_string();

        let subscription = if let Some(pattern) = config.get("topic.pattern") {
            TopicSubscription::Pattern(pattern.to_string())
        } else {
            let topics_str = config.require("topic")?;
            let topics: Vec<String> = topics_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            TopicSubscription::Topics(topics.clone())
        };

        let topics = match &subscription {
            TopicSubscription::Topics(t) => t.clone(),
            TopicSubscription::Pattern(_) => Vec::new(),
        };

        let security_protocol = match config.get("security.protocol") {
            Some(s) => s.parse::<SecurityProtocol>()?,
            None => SecurityProtocol::default(),
        };

        let sasl_mechanism = match config.get("sasl.mechanism") {
            Some(s) => Some(s.parse::<SaslMechanism>()?),
            None => None,
        };

        let sasl_username = config.get("sasl.username").map(String::from);
        let sasl_password = config.get("sasl.password").map(String::from);
        let ssl_ca_location = config.get("ssl.ca.location").map(String::from);
        let ssl_certificate_location = config.get("ssl.certificate.location").map(String::from);
        let ssl_key_location = config.get("ssl.key.location").map(String::from);
        let ssl_key_password = config.get("ssl.key.password").map(String::from);

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

        let schema_registry_ssl_ca_location = config
            .get("schema.registry.ssl.ca.location")
            .map(String::from);
        let schema_registry_ssl_certificate_location = config
            .get("schema.registry.ssl.certificate.location")
            .map(String::from);
        let schema_registry_ssl_key_location = config
            .get("schema.registry.ssl.key.location")
            .map(String::from);

        let event_time_column = config.get("event.time.column").map(String::from);

        let include_metadata = config
            .get_parsed::<bool>("include.metadata")?
            .unwrap_or(false);

        let include_headers = config
            .get_parsed::<bool>("include.headers")?
            .unwrap_or(false);

        let startup_mode = if let Some(offsets_str) = config.get("startup.specific.offsets") {
            let offsets = parse_specific_offsets(offsets_str)?;
            StartupMode::SpecificOffsets(offsets)
        } else if let Some(ts_str) = config.get("startup.timestamp.ms") {
            let ts: i64 = ts_str.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid startup.timestamp.ms: '{ts_str}'"
                ))
            })?;
            StartupMode::Timestamp(ts)
        } else {
            match config.get("startup.mode") {
                Some(s) => s.parse::<StartupMode>()?,
                None => StartupMode::default(),
            }
        };

        let auto_offset_reset = match config.get("auto.offset.reset") {
            Some(s) => s.parse::<OffsetReset>()?,
            None => OffsetReset::Earliest,
        };

        let isolation_level = match config.get("isolation.level") {
            Some(s) => s.parse::<IsolationLevel>()?,
            None => IsolationLevel::default(),
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

        let fetch_min_bytes = config.get_parsed::<i32>("fetch.min.bytes")?;
        let fetch_max_bytes = config.get_parsed::<i32>("fetch.max.bytes")?;
        let fetch_max_wait_ms = config.get_parsed::<i32>("fetch.max.wait.ms")?;
        let max_partition_fetch_bytes = config.get_parsed::<i32>("max.partition.fetch.bytes")?;

        let max_out_of_orderness_ms = config
            .get_parsed::<u64>("max.out.of.orderness.ms")?
            .unwrap_or(5000);

        let idle_timeout_ms = config
            .get_parsed::<u64>("idle.timeout.ms")?
            .unwrap_or(30_000);

        let enable_watermark_tracking = config
            .get_parsed::<bool>("enable.watermark.tracking")?
            .unwrap_or(false);

        let alignment_group_id = config.get("alignment.group.id").map(String::from);

        let alignment_max_drift_ms = config.get_parsed::<u64>("alignment.max.drift.ms")?;

        let alignment_mode = match config.get("alignment.mode") {
            Some(s) => Some(
                s.parse::<super::watermarks::KafkaAlignmentMode>()
                    .map_err(ConnectorError::ConfigurationError)?,
            ),
            None => None,
        };

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
            subscription,
            topics,
            security_protocol,
            sasl_mechanism,
            sasl_username,
            sasl_password,
            ssl_ca_location,
            ssl_certificate_location,
            ssl_key_location,
            ssl_key_password,
            format,
            schema_registry_url,
            schema_registry_auth,
            schema_compatibility,
            schema_registry_ssl_ca_location,
            schema_registry_ssl_certificate_location,
            schema_registry_ssl_key_location,
            event_time_column,
            include_metadata,
            include_headers,
            startup_mode,
            auto_offset_reset,
            isolation_level,
            max_poll_records,
            poll_timeout: Duration::from_millis(poll_timeout_ms),
            partition_assignment_strategy,
            commit_interval: Duration::from_millis(commit_interval_ms),
            fetch_min_bytes,
            fetch_max_bytes,
            fetch_max_wait_ms,
            max_partition_fetch_bytes,
            max_out_of_orderness: Duration::from_millis(max_out_of_orderness_ms),
            idle_timeout: Duration::from_millis(idle_timeout_ms),
            enable_watermark_tracking,
            alignment_group_id,
            alignment_max_drift: alignment_max_drift_ms.map(Duration::from_millis),
            alignment_mode,
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

        // Validate topic subscription
        match &self.subscription {
            TopicSubscription::Topics(t) if t.is_empty() => {
                return Err(ConnectorError::ConfigurationError(
                    "at least one topic is required (or use topic.pattern)".into(),
                ));
            }
            TopicSubscription::Pattern(p) if p.is_empty() => {
                return Err(ConnectorError::ConfigurationError(
                    "topic.pattern cannot be empty".into(),
                ));
            }
            _ => {}
        }

        if self.max_poll_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.poll.records must be > 0".into(),
            ));
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

        config.set("isolation.level", self.isolation_level.as_rdkafka_str());

        if let Some(fetch_min) = self.fetch_min_bytes {
            config.set("fetch.min.bytes", fetch_min.to_string());
        }

        if let Some(fetch_max) = self.fetch_max_bytes {
            config.set("fetch.max.bytes", fetch_max.to_string());
        }

        if let Some(wait_ms) = self.fetch_max_wait_ms {
            config.set("fetch.wait.max.ms", wait_ms.to_string());
        }

        if let Some(partition_max) = self.max_partition_fetch_bytes {
            config.set("max.partition.fetch.bytes", partition_max.to_string());
        }

        // Apply pass-through properties (these can override defaults).
        for (key, value) in &self.kafka_properties {
            config.set(key, value);
        }

        config
    }
}

/// Parses a specific offsets string in the format "partition:offset,partition:offset,...".
///
/// Example: "0:100,1:200,2:300" maps partition 0 to offset 100, partition 1 to offset 200, etc.
fn parse_specific_offsets(s: &str) -> Result<HashMap<i32, i64>, ConnectorError> {
    let mut offsets = HashMap::new();

    for pair in s.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }

        let parts: Vec<&str> = pair.split(':').collect();
        if parts.len() != 2 {
            return Err(ConnectorError::ConfigurationError(format!(
                "invalid offset format '{pair}' (expected 'partition:offset')"
            )));
        }

        let partition: i32 = parts[0].trim().parse().map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "invalid partition number '{}' in '{pair}'",
                parts[0]
            ))
        })?;

        let offset: i64 = parts[1].trim().parse().map_err(|_| {
            ConnectorError::ConfigurationError(format!("invalid offset '{}' in '{pair}'", parts[1]))
        })?;

        offsets.insert(partition, offset);
    }

    if offsets.is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "startup.specific.offsets cannot be empty".into(),
        ));
    }

    Ok(offsets)
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
    #[allow(deprecated)]
    fn test_parse_required_fields() {
        let cfg = KafkaSourceConfig::from_config(&make_config(&[])).unwrap();
        assert_eq!(cfg.bootstrap_servers, "localhost:9092");
        assert_eq!(cfg.group_id, "test-group");
        assert_eq!(cfg.topics, vec!["events"]);
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
        assert_eq!(cfg.topics, vec!["a", "b", "c"]);
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
        assert_eq!(cfg.poll_timeout, Duration::from_millis(100));
        assert_eq!(cfg.partition_assignment_strategy, AssignmentStrategy::Range);
        assert_eq!(cfg.commit_interval, Duration::from_secs(5));
        assert!(!cfg.include_metadata);
        assert!(!cfg.include_headers);
        assert!(cfg.schema_registry_url.is_none());
        assert_eq!(cfg.security_protocol, SecurityProtocol::Plaintext);
        assert!(cfg.sasl_mechanism.is_none());
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
            ("include.headers", "true"),
            ("event.time.column", "ts"),
            ("partition.assignment.strategy", "roundrobin"),
            ("isolation.level", "read_uncommitted"),
        ]))
        .unwrap();

        assert_eq!(cfg.format, Format::Csv);
        assert_eq!(cfg.auto_offset_reset, OffsetReset::Latest);
        assert_eq!(cfg.isolation_level, IsolationLevel::ReadUncommitted);
        assert_eq!(cfg.max_poll_records, 500);
        assert_eq!(cfg.poll_timeout, Duration::from_millis(200));
        assert_eq!(cfg.commit_interval, Duration::from_secs(10));
        assert!(cfg.include_metadata);
        assert!(cfg.include_headers);
        assert_eq!(cfg.event_time_column, Some("ts".to_string()));
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
    fn test_startup_mode_helpers() {
        assert!(!StartupMode::GroupOffsets.overrides_offset_reset());
        assert!(StartupMode::Earliest.overrides_offset_reset());
        assert!(StartupMode::Latest.overrides_offset_reset());
        assert!(StartupMode::SpecificOffsets(HashMap::new()).overrides_offset_reset());
        assert!(StartupMode::Timestamp(0).overrides_offset_reset());

        assert!(StartupMode::GroupOffsets.as_offset_reset().is_none());
        assert_eq!(StartupMode::Earliest.as_offset_reset(), Some("earliest"));
        assert_eq!(StartupMode::Latest.as_offset_reset(), Some("latest"));
        assert!(StartupMode::SpecificOffsets(HashMap::new())
            .as_offset_reset()
            .is_none());
        assert!(StartupMode::Timestamp(0).as_offset_reset().is_none());
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

        let cfg =
            KafkaSourceConfig::from_config(&make_config(&[("startup.mode", "latest")])).unwrap();
        assert_eq!(cfg.startup_mode, StartupMode::Latest);

        let cfg =
            KafkaSourceConfig::from_config(&make_config(&[("startup.mode", "group-offsets")]))
                .unwrap();
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

    #[test]
    fn test_parse_watermark_defaults() {
        let cfg = KafkaSourceConfig::from_config(&make_config(&[])).unwrap();
        assert_eq!(cfg.max_out_of_orderness, Duration::from_secs(5));
        assert_eq!(cfg.idle_timeout, Duration::from_secs(30));
        assert!(!cfg.enable_watermark_tracking);
        assert!(cfg.alignment_group_id.is_none());
        assert!(cfg.alignment_max_drift.is_none());
        assert!(cfg.alignment_mode.is_none());
    }

    #[test]
    fn test_parse_watermark_tracking_enabled() {
        let cfg = KafkaSourceConfig::from_config(&make_config(&[
            ("enable.watermark.tracking", "true"),
            ("max.out.of.orderness.ms", "10000"),
            ("idle.timeout.ms", "60000"),
        ]))
        .unwrap();

        assert!(cfg.enable_watermark_tracking);
        assert_eq!(cfg.max_out_of_orderness, Duration::from_millis(10_000));
        assert_eq!(cfg.idle_timeout, Duration::from_millis(60_000));
    }

    #[test]
    fn test_parse_alignment_config() {
        use crate::kafka::KafkaAlignmentMode;

        let cfg = KafkaSourceConfig::from_config(&make_config(&[
            ("alignment.group.id", "orders-payments"),
            ("alignment.max.drift.ms", "300000"),
            ("alignment.mode", "pause"),
        ]))
        .unwrap();

        assert_eq!(cfg.alignment_group_id, Some("orders-payments".to_string()));
        assert_eq!(
            cfg.alignment_max_drift,
            Some(Duration::from_millis(300_000))
        );
        assert_eq!(cfg.alignment_mode, Some(KafkaAlignmentMode::Pause));
    }

    #[test]
    fn test_parse_alignment_mode_variants() {
        use crate::kafka::KafkaAlignmentMode;

        let cfg = KafkaSourceConfig::from_config(&make_config(&[("alignment.mode", "warn-only")]))
            .unwrap();
        assert_eq!(cfg.alignment_mode, Some(KafkaAlignmentMode::WarnOnly));

        let cfg =
            KafkaSourceConfig::from_config(&make_config(&[("alignment.mode", "drop-excess")]))
                .unwrap();
        assert_eq!(cfg.alignment_mode, Some(KafkaAlignmentMode::DropExcess));
    }

    #[test]
    fn test_parse_alignment_mode_invalid() {
        let config = make_config(&[("alignment.mode", "invalid")]);
        assert!(KafkaSourceConfig::from_config(&config).is_err());
    }
}
