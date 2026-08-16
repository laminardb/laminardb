//! Kafka source config — broker connection, format, Schema Registry,
//! backpressure, and pass-through `rdkafka` properties.

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::config::ClientConfig;

use crate::error::ConnectorError;
use crate::serde::Format;

mod source_options;

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

impl std::str::FromStr for StartupMode {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "group_offsets" | "group" => Ok(StartupMode::GroupOffsets),
            "earliest" => Ok(StartupMode::Earliest),
            "latest" => Ok(StartupMode::Latest),
            "timestamp" => Err(ConnectorError::ConfigurationError(
                "use 'startup.timestamp.ms' to start from a timestamp, \
                 not 'startup.mode = timestamp'"
                    .into(),
            )),
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

/// Strategy for handling Avro schema evolution at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaEvolutionStrategy {
    /// Log schema changes, continue processing.
    #[default]
    Log,
    /// Return an error on incompatible schema changes.
    Reject,
    /// No detection — skip schema diffing entirely.
    Ignore,
}

str_enum!(fromstr SchemaEvolutionStrategy, lowercase, ConnectorError,
    "invalid schema.evolution.strategy",
    Log => "log";
    Reject => "reject";
    Ignore => "ignore"
);

impl std::fmt::Display for SchemaEvolutionStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaEvolutionStrategy::Log => write!(f, "log"),
            SchemaEvolutionStrategy::Reject => write!(f, "reject"),
            SchemaEvolutionStrategy::Ignore => write!(f, "ignore"),
        }
    }
}

/// Confluent subject-name strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubjectNameStrategy {
    /// `{topic}-value` (Confluent default).
    #[default]
    TopicName,
    /// `{record_name}-value`. Requires `schema.registry.record.name`.
    RecordName,
    /// `{topic}-{record_name}-value`. Requires `schema.registry.record.name`.
    TopicRecordName,
}

str_enum!(fromstr SubjectNameStrategy, lowercase_nodash, ConnectorError,
    "invalid schema.registry.subject.name.strategy",
    TopicName => "topic-name", "topicname", "topicnamestrategy";
    RecordName => "record-name", "recordname", "recordnamestrategy";
    TopicRecordName => "topic-record-name", "topicrecordname", "topicrecordnamestrategy"
);

impl std::fmt::Display for SubjectNameStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubjectNameStrategy::TopicName => write!(f, "topic-name"),
            SubjectNameStrategy::RecordName => write!(f, "record-name"),
            SubjectNameStrategy::TopicRecordName => write!(f, "topic-record-name"),
        }
    }
}

/// Build the SR `-value` subject for a topic. `from_config` validates
/// that `record_name` is present for the record-based strategies, so
/// the `expect`s are unreachable in practice.
pub(crate) fn resolve_value_subject(
    strategy: SubjectNameStrategy,
    record_name: Option<&str>,
    topic: &str,
) -> String {
    let name = || record_name.expect("from_config validates record.name");
    match strategy {
        SubjectNameStrategy::TopicName => format!("{topic}-value"),
        SubjectNameStrategy::RecordName => format!("{}-value", name()),
        SubjectNameStrategy::TopicRecordName => format!("{topic}-{}-value", name()),
    }
}

/// Maps the Kafka-level [`CompatibilityLevel`] to the schema module's
/// `CompatibilityMode` for evolution evaluation.
impl From<CompatibilityLevel> for crate::schema::traits::CompatibilityMode {
    fn from(level: CompatibilityLevel) -> Self {
        use crate::schema::traits::CompatibilityMode;
        match level {
            CompatibilityLevel::Backward => CompatibilityMode::Backward,
            CompatibilityLevel::BackwardTransitive => CompatibilityMode::BackwardTransitive,
            CompatibilityLevel::Forward => CompatibilityMode::Forward,
            CompatibilityLevel::ForwardTransitive => CompatibilityMode::ForwardTransitive,
            CompatibilityLevel::Full => CompatibilityMode::Full,
            CompatibilityLevel::FullTransitive => CompatibilityMode::FullTransitive,
            CompatibilityLevel::None => CompatibilityMode::None,
        }
    }
}

/// Schema Registry authentication credentials.
#[derive(Clone)]
pub struct SrAuth {
    /// Basic auth username.
    pub username: String,
    /// Basic auth password.
    pub password: String,
}

impl std::fmt::Debug for SrAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SrAuth")
            .field("username", &self.username)
            .field("password", &"***")
            .finish()
    }
}

/// Kafka source connector configuration.
///
/// Uses a custom `Debug` impl that redacts `sasl_password` and
/// `ssl_key_password` to prevent credential leakage in logs.
#[derive(Clone)]
#[allow(clippy::struct_excessive_bools)] // Config struct — each bool is an independent user-facing knob.
pub struct KafkaSourceConfig {
    // -- Required --
    /// Comma-separated list of broker addresses.
    pub bootstrap_servers: String,
    /// Consumer group identifier.
    pub group_id: String,
    /// Topic subscription (explicit list or regex pattern).
    pub subscription: TopicSubscription,

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
    /// How to handle Avro schema evolution detected at runtime.
    pub schema_evolution_strategy: SchemaEvolutionStrategy,
    /// Schema Registry SSL CA certificate path.
    pub schema_registry_ssl_ca_location: Option<String>,
    /// Schema Registry SSL client certificate path.
    pub schema_registry_ssl_certificate_location: Option<String>,
    /// Schema Registry SSL client key path.
    pub schema_registry_ssl_key_location: Option<String>,
    /// Confluent subject-name strategy. Default: `TopicName`.
    pub schema_registry_subject_strategy: SubjectNameStrategy,
    /// Record name for `record-name` / `topic-record-name` strategies.
    pub schema_registry_record_name: Option<String>,
    /// Deadline for Schema Registry lookups performed during schema
    /// auto-discovery at DDL time. Default: 10s.
    pub schema_registry_discovery_timeout: Duration,
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
    /// Partition assignment strategy.
    pub partition_assignment_strategy: AssignmentStrategy,
    /// Minimum bytes to return from a fetch (allows batching).
    pub fetch_min_bytes: Option<i32>,
    /// Maximum bytes to return from broker per request.
    pub fetch_max_bytes: Option<i32>,
    /// Maximum time broker waits for fetch.min.bytes.
    pub fetch_max_wait_ms: Option<i32>,
    /// Maximum bytes per partition to return from broker.
    pub max_partition_fetch_bytes: Option<i32>,

    // -- Consumer group timing --
    /// Consumer session timeout (default: 45s — production-safe; rdkafka's
    /// aggressive 10s default causes rebalance storms under GC pauses).
    pub session_timeout: Duration,
    /// Consumer heartbeat interval (default: 10s). Must satisfy
    /// `heartbeat_interval * 3 < session_timeout` per Kafka broker requirement.
    pub heartbeat_interval: Duration,
    /// Maximum interval between calls to `rd_kafka_consumer_poll()` before
    /// the broker considers this consumer dead and triggers a rebalance.
    ///
    /// Default: 600s (10 minutes). rdkafka's default is 300s, but with
    /// reader-side pause/resume the reader keeps polling even under
    /// backpressure, so this is a safety margin. Must be >= 60s.
    pub max_poll_interval: Duration,
    /// Maximum per-partition pre-fetch queue size in kbytes (default: 16384 = 16MB).
    /// rdkafka's 64MB default is too aggressive for an embedded database.
    pub queued_max_messages_kbytes: u32,

    // -- Broker commit --
    /// Whether to commit consumed offsets to the Kafka broker after each
    /// `LaminarDB` checkpoint completes. Default: `true`.
    ///
    /// Advisory — `LaminarDB` recovery uses its own manifest, not broker-stored
    /// offsets. This exists only so external tooling (`kafka-consumer-groups`,
    /// kafka-exporter, Burrow) can observe progress. Guaranteed delivery always
    /// derives an unrecorded partition's start from engine configuration and
    /// deliberately ignores broker-stored group offsets from abandoned timelines.
    pub broker_commit_on_checkpoint: bool,

    // -- Backpressure --
    /// Capacity of the bounded channel between the background Kafka reader
    /// task and `poll_batch()` (default: 8192). Must be >= `max_poll_records`.
    pub reader_channel_capacity: usize,
    /// Channel fill ratio at which to pause consumption.
    pub backpressure_high_watermark: f64,
    /// Channel fill ratio at which to resume consumption.
    pub backpressure_low_watermark: f64,

    // -- Error handling --
    /// Maximum tolerated deserialization error rate per `BestEffort` batch (0.0-1.0).
    ///
    /// When the poison pill fallback is active and the error rate exceeds
    /// this threshold, the batch is rejected instead of returning partial
    /// results. Guaranteed-delivery modes reject any deserialization failure;
    /// they never use this threshold. Default: 0.5 (50%).
    pub max_deser_error_rate: f64,

    // -- Pass-through --
    /// Additional rdkafka properties passed directly to librdkafka.
    pub kafka_properties: HashMap<String, String>,
}

impl std::fmt::Debug for KafkaSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSourceConfig")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("group_id", &self.group_id)
            .field("subscription", &self.subscription)
            .field("format", &self.format)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &self.sasl_password.as_ref().map(|_| "***"))
            .field(
                "ssl_key_password",
                &self.ssl_key_password.as_ref().map(|_| "***"),
            )
            .field("max_poll_records", &self.max_poll_records)
            .finish_non_exhaustive()
    }
}

impl Default for KafkaSourceConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            group_id: String::new(),
            subscription: TopicSubscription::default(),
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
            schema_evolution_strategy: SchemaEvolutionStrategy::default(),
            schema_registry_ssl_ca_location: None,
            schema_registry_ssl_certificate_location: None,
            schema_registry_ssl_key_location: None,
            schema_registry_subject_strategy: SubjectNameStrategy::default(),
            schema_registry_record_name: None,
            schema_registry_discovery_timeout: Duration::from_secs(10),
            include_metadata: false,
            include_headers: false,
            startup_mode: StartupMode::default(),
            auto_offset_reset: OffsetReset::Earliest,
            isolation_level: IsolationLevel::default(),
            max_poll_records: 1000,
            partition_assignment_strategy: AssignmentStrategy::Range,
            fetch_min_bytes: None,
            fetch_max_bytes: None,
            fetch_max_wait_ms: None,
            max_partition_fetch_bytes: None,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(10),
            max_poll_interval: Duration::from_secs(600),
            queued_max_messages_kbytes: 16384,
            broker_commit_on_checkpoint: true,
            reader_channel_capacity: 8192,
            backpressure_high_watermark: 0.8,
            backpressure_low_watermark: 0.25,
            max_deser_error_rate: 0.5,
            kafka_properties: HashMap::new(),
        }
    }
}

impl KafkaSourceConfig {
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
        if self.reader_channel_capacity < self.max_poll_records {
            return Err(ConnectorError::ConfigurationError(format!(
                "reader.channel.capacity ({}) must be >= max.poll.records ({})",
                self.reader_channel_capacity, self.max_poll_records
            )));
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

        if self.max_poll_interval.as_secs() < 60 {
            return Err(ConnectorError::ConfigurationError(format!(
                "max.poll.interval.ms ({}) must be >= 60000 (60s)",
                self.max_poll_interval.as_millis()
            )));
        }

        // Kafka broker requires heartbeat_interval * 3 < session_timeout.
        if self.heartbeat_interval.as_millis() * 3 >= self.session_timeout.as_millis() {
            return Err(ConnectorError::ConfigurationError(format!(
                "heartbeat.interval.ms ({}) * 3 must be < session.timeout.ms ({})",
                self.heartbeat_interval.as_millis(),
                self.session_timeout.as_millis()
            )));
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

        if !(0.0..=1.0).contains(&self.max_deser_error_rate) {
            return Err(ConnectorError::ConfigurationError(
                "max.deser.error.rate must be between 0.0 and 1.0".into(),
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
        config.set("enable.auto.offset.store", "false");
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
        config.set(
            "session.timeout.ms",
            self.session_timeout.as_millis().to_string(),
        );
        config.set(
            "heartbeat.interval.ms",
            self.heartbeat_interval.as_millis().to_string(),
        );
        config.set(
            "queued.max.messages.kbytes",
            self.queued_max_messages_kbytes.to_string(),
        );
        config.set(
            "max.poll.interval.ms",
            self.max_poll_interval.as_millis().to_string(),
        );

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

        // Apply pass-through properties, blocking security-critical keys
        // that could silently downgrade authentication or break semantics.
        for (key, value) in &self.kafka_properties {
            if is_blocked_passthrough_key(key) {
                tracing::warn!(
                    key,
                    "ignoring kafka.* pass-through property that overrides a security setting"
                );
                continue;
            }
            config.set(key, value);
        }

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
                | "enable.auto.offset.store"
                | "enable.idempotence"
                | "auto.offset.reset"
                | "session.timeout.ms"
                | "heartbeat.interval.ms"
                | "max.poll.interval.ms"
                | "queued.max.messages.kbytes"
                // librdkafka's own auto-commit interval — only meaningful
                // when `enable.auto.commit=true`, which we hard-disable.
                | "auto.commit.interval.ms"
        )
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
mod tests;
