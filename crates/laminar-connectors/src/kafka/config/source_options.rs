//! Parsing of Kafka source connector properties.
//!
//! Each phase fills one coherent part of `KafkaSourceConfig`. Validation runs only after every
//! phase succeeds, so callers never observe a partially prepared configuration.

use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::serde::Format;

use super::{
    parse_specific_offsets, AssignmentStrategy, IsolationLevel, KafkaSourceConfig, OffsetReset,
    SchemaEvolutionStrategy, SecurityProtocol, SrAuth, StartupMode, SubjectNameStrategy,
    TopicSubscription,
};

impl KafkaSourceConfig {
    /// Parses a Kafka source configuration from connector properties.
    ///
    /// # Errors
    ///
    /// Returns an error when a required property is missing or any value is invalid.
    #[allow(deprecated)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut parsed = Self::default();
        parsed.parse_connection(config)?;
        parsed.parse_schema(config)?;
        parsed.parse_startup_and_fetch(config)?;
        parsed.parse_group_timing(config)?;
        parsed.parse_delivery_controls(config)?;
        parsed.kafka_properties = config.properties_with_prefix("kafka.");
        parsed.validate()?;
        Ok(parsed)
    }

    fn parse_connection(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.bootstrap_servers = config.require("bootstrap.servers")?.to_string();
        self.group_id = config.require("group.id")?.to_string();
        self.subscription = if let Some(pattern) = config.get("topic.pattern") {
            TopicSubscription::Pattern(pattern.to_string())
        } else {
            let topics = config
                .require("topic")?
                .split(',')
                .map(|topic| topic.trim().to_string())
                .collect();
            TopicSubscription::Topics(topics)
        };

        self.security_protocol = config
            .get("security.protocol")
            .map_or(Ok(SecurityProtocol::default()), str::parse)?;
        self.sasl_mechanism = config.get("sasl.mechanism").map(str::parse).transpose()?;
        self.sasl_username = config.get("sasl.username").map(String::from);
        self.sasl_password = config.get("sasl.password").map(String::from);
        self.ssl_ca_location = config.get("ssl.ca.location").map(String::from);
        self.ssl_certificate_location = config.get("ssl.certificate.location").map(String::from);
        self.ssl_key_location = config.get("ssl.key.location").map(String::from);
        self.ssl_key_password = config.get("ssl.key.password").map(String::from);
        Ok(())
    }

    fn parse_schema(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.format = config.get("format").map_or(Ok(Format::Json), |format| {
            format
                .parse::<Format>()
                .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))
        })?;
        self.schema_registry_url = config.get("schema.registry.url").map(String::from);
        self.schema_registry_auth = parse_schema_registry_auth(config)?;
        self.schema_compatibility = config
            .get("schema.compatibility")
            .map(str::parse)
            .transpose()?;
        self.schema_evolution_strategy = config
            .get("schema.evolution.strategy")
            .map_or(Ok(SchemaEvolutionStrategy::default()), str::parse)?;
        self.schema_registry_ssl_ca_location = config
            .get("schema.registry.ssl.ca.location")
            .map(String::from);
        self.schema_registry_ssl_certificate_location = config
            .get("schema.registry.ssl.certificate.location")
            .map(String::from);
        self.schema_registry_ssl_key_location = config
            .get("schema.registry.ssl.key.location")
            .map(String::from);
        self.schema_registry_subject_strategy = config
            .get("schema.registry.subject.name.strategy")
            .map_or(Ok(SubjectNameStrategy::default()), str::parse)?;
        self.schema_registry_record_name =
            config.get("schema.registry.record.name").map(String::from);
        validate_subject_name_strategy(
            self.schema_registry_subject_strategy,
            self.schema_registry_record_name.as_deref(),
        )?;
        self.schema_registry_discovery_timeout = config
            .get_parsed::<u64>("schema.registry.discovery.timeout.ms")?
            .map_or(Duration::from_secs(10), Duration::from_millis);
        self.include_metadata = config
            .get_parsed::<bool>("include.metadata")?
            .unwrap_or(false);
        self.include_headers = config
            .get_parsed::<bool>("include.headers")?
            .unwrap_or(false);
        Ok(())
    }

    fn parse_startup_and_fetch(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.startup_mode = parse_startup_mode(config)?;
        self.auto_offset_reset = match &self.startup_mode {
            StartupMode::Earliest => OffsetReset::Earliest,
            StartupMode::Latest => OffsetReset::Latest,
            _ => config
                .get("auto.offset.reset")
                .map_or(Ok(OffsetReset::Earliest), str::parse)?,
        };
        self.isolation_level = config
            .get("isolation.level")
            .map_or(Ok(IsolationLevel::default()), str::parse)?;
        self.max_poll_records = config
            .get_parsed::<usize>("max.poll.records")?
            .unwrap_or(1000);
        self.partition_assignment_strategy = config
            .get("partition.assignment.strategy")
            .map_or(Ok(AssignmentStrategy::Range), str::parse)?;
        self.fetch_min_bytes = config.get_parsed::<i32>("fetch.min.bytes")?;
        self.fetch_max_bytes = config.get_parsed::<i32>("fetch.max.bytes")?;
        self.fetch_max_wait_ms = config.get_parsed::<i32>("fetch.max.wait.ms")?;
        self.max_partition_fetch_bytes = config.get_parsed::<i32>("max.partition.fetch.bytes")?;
        Ok(())
    }

    fn parse_group_timing(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let session_timeout_ms = config
            .get_parsed::<u64>("session.timeout.ms")?
            .unwrap_or(45_000);
        let heartbeat_interval_ms = config
            .get_parsed::<u64>("heartbeat.interval.ms")?
            .unwrap_or(10_000);
        self.queued_max_messages_kbytes = config
            .get_parsed::<u32>("queued.max.messages.kbytes")?
            .unwrap_or(16384);
        let max_poll_interval_ms = config
            .get_parsed::<u64>("max.poll.interval.ms")?
            .unwrap_or(600_000);

        self.session_timeout = Duration::from_millis(session_timeout_ms);
        self.heartbeat_interval = Duration::from_millis(heartbeat_interval_ms);
        self.max_poll_interval = Duration::from_millis(max_poll_interval_ms);
        Ok(())
    }

    fn parse_delivery_controls(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        reject_deprecated_commit_interval(config)?;
        self.broker_commit_on_checkpoint = config
            .get_parsed::<bool>("broker.commit.on.checkpoint")?
            .unwrap_or(true);
        self.reader_channel_capacity = config
            .get_parsed::<usize>("reader.channel.capacity")?
            .unwrap_or(8192);
        self.backpressure_high_watermark = config
            .get_parsed::<f64>("backpressure.high.watermark")?
            .unwrap_or(0.8);
        self.backpressure_low_watermark = config
            .get_parsed::<f64>("backpressure.low.watermark")?
            .unwrap_or(0.25);
        self.max_deser_error_rate = config
            .get_parsed::<f64>("max.deser.error.rate")?
            .unwrap_or(0.5);
        Ok(())
    }
}

fn parse_schema_registry_auth(config: &ConnectorConfig) -> Result<Option<SrAuth>, ConnectorError> {
    match (
        config.get("schema.registry.username"),
        config.get("schema.registry.password"),
    ) {
        (Some(username), Some(password)) => Ok(Some(SrAuth {
            username: username.to_string(),
            password: password.to_string(),
        })),
        (Some(_), None) | (None, Some(_)) => Err(ConnectorError::ConfigurationError(
            "schema.registry.username and schema.registry.password must both be set".to_string(),
        )),
        (None, None) => Ok(None),
    }
}

fn validate_subject_name_strategy(
    strategy: SubjectNameStrategy,
    record_name: Option<&str>,
) -> Result<(), ConnectorError> {
    if matches!(
        strategy,
        SubjectNameStrategy::RecordName | SubjectNameStrategy::TopicRecordName
    ) && record_name.is_none()
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "schema.registry.subject.name.strategy={strategy} requires schema.registry.record.name"
        )));
    }
    Ok(())
}

fn parse_startup_mode(config: &ConnectorConfig) -> Result<StartupMode, ConnectorError> {
    if let Some(offsets) = config.get("startup.specific.offsets") {
        return Ok(StartupMode::SpecificOffsets(parse_specific_offsets(
            offsets,
        )?));
    }
    if let Some(timestamp) = config.get("startup.timestamp.ms") {
        let timestamp = timestamp.parse().map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "invalid startup.timestamp.ms: '{timestamp}'"
            ))
        })?;
        return Ok(StartupMode::Timestamp(timestamp));
    }
    config
        .get("startup.mode")
        .map_or(Ok(StartupMode::default()), str::parse)
}

fn reject_deprecated_commit_interval(config: &ConnectorConfig) -> Result<(), ConnectorError> {
    if config
        .properties()
        .contains_key("broker.commit.interval.ms")
    {
        return Err(ConnectorError::ConfigurationError(
            "broker.commit.interval.ms is no longer supported — broker offset commits now happen \
             on checkpoint completion. Set broker.commit.on.checkpoint=false to disable."
                .into(),
        ));
    }
    Ok(())
}
