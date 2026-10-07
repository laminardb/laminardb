//! Metadata-only configuration for callers of the typed connector API.

use super::config::{KafkaSourceConfig, SrAuth, TopicSubscription};
use super::sink_config::{KafkaSinkConfig, SinkEnvelope};
use crate::config::ConnectorConfig;

pub(super) fn source(config: &ConnectorConfig, typed: &KafkaSourceConfig) -> ConnectorConfig {
    if !config.properties().is_empty() {
        return config.clone();
    }
    let mut effective = ConnectorConfig::new("kafka");
    effective.set("bootstrap.servers", &typed.bootstrap_servers);
    effective.set("group.id", &typed.group_id);
    effective.set("format", typed.format.to_string());
    match &typed.subscription {
        TopicSubscription::Topics(topics) => effective.set("topic", topics.join(",")),
        TopicSubscription::Pattern(pattern) => effective.set("topic.pattern", pattern),
    }
    registry(
        &mut effective,
        typed.schema_registry_url.as_deref(),
        typed.schema_registry_auth.as_ref(),
        typed.schema_registry_ssl_ca_location.as_deref(),
    );
    for (key, value) in [
        (
            "schema.registry.ssl.certificate.location",
            typed.schema_registry_ssl_certificate_location.as_deref(),
        ),
        (
            "schema.registry.ssl.key.location",
            typed.schema_registry_ssl_key_location.as_deref(),
        ),
        (
            "schema.registry.record.name",
            typed.schema_registry_record_name.as_deref(),
        ),
    ] {
        if let Some(value) = value {
            effective.set(key, value);
        }
    }
    effective.set(
        "schema.registry.subject.name.strategy",
        typed.schema_registry_subject_strategy.to_string(),
    );
    effective.set("include.metadata", typed.include_metadata.to_string());
    effective.set("include.headers", typed.include_headers.to_string());
    effective
}

pub(super) fn sink(config: &ConnectorConfig, typed: &KafkaSinkConfig) -> ConnectorConfig {
    if !config.properties().is_empty() {
        return config.clone();
    }
    let mut effective = ConnectorConfig::new("kafka");
    effective.set("bootstrap.servers", &typed.bootstrap_servers);
    effective.set("topic", &typed.topic);
    effective.set("format", typed.format.to_string());
    registry(
        &mut effective,
        typed.schema_registry_url.as_deref(),
        typed.schema_registry_auth.as_ref(),
        typed.schema_registry_ssl_ca_location.as_deref(),
    );
    if let Some(level) = typed.schema_compatibility {
        effective.set("schema.compatibility", level.to_string());
    }
    if let Some(key) = &typed.key_column {
        effective.set("key.column", key);
    }
    if typed.envelope == SinkEnvelope::Upsert {
        effective.set("envelope", "upsert");
    }
    effective
}

fn registry(
    config: &mut ConnectorConfig,
    url: Option<&str>,
    auth: Option<&SrAuth>,
    ca: Option<&str>,
) {
    if let Some(url) = url {
        config.set("schema.registry.url", url);
    }
    if let Some(auth) = auth {
        config.set("schema.registry.username", &auth.username);
        config.set("schema.registry.password", &auth.password);
    }
    if let Some(ca) = ca {
        config.set("schema.registry.ssl.ca.location", ca);
    }
}
