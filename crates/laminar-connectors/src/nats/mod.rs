//! NATS source and sink connectors.
//!
//! Supports two modes:
//! - `core`: plain NATS pub/sub, non-durable, non-replayable, at-most-once.
//! - `jetstream` (default): durable streams with pull consumers, replayable,
//!   at-least-once by default and exactly-once with `Nats-Msg-Id` dedup.

pub mod config;
pub mod metrics;
pub mod sink;
pub mod source;

pub use metrics::{NatsSinkMetrics, NatsSourceMetrics};
pub use sink::NatsSink;
pub use source::NatsSource;

use std::sync::Arc;

use arrow_schema::Schema;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the NATS source connector.
pub fn register_nats_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "nats".to_string(),
        display_name: "NATS Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: source_config_keys(),
    };
    registry.register_source(
        "nats",
        info,
        Arc::new(|reg| Box::new(NatsSource::new(Arc::new(Schema::empty()), reg))),
    );
}

/// Registers the NATS sink connector.
pub fn register_nats_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "nats".to_string(),
        display_name: "NATS Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: sink_config_keys(),
    };
    registry.register_sink(
        "nats",
        info,
        Arc::new(|reg| Box::new(NatsSink::new(Arc::new(Schema::empty()), reg))),
    );
}

fn auth_and_tls_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    vec![
        K::optional("auth.mode", "none | user_pass | token", "none"),
        K::optional("user", "Username (auth.mode=user_pass)", ""),
        K::optional("password", "Password (auth.mode=user_pass)", ""),
        K::optional("token", "Bearer token (auth.mode=token)", ""),
        K::optional("tls.enabled", "Require TLS on the connection", "false"),
        K::optional(
            "tls.ca.location",
            "PEM CA certificate for server verification",
            "",
        ),
        K::optional(
            "tls.cert.location",
            "Client certificate for mutual TLS (pairs with tls.key.location)",
            "",
        ),
        K::optional("tls.key.location", "Client private key for mutual TLS", ""),
    ]
}

fn source_config_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    let mut keys = vec![
        K::required("servers", "NATS server URLs, comma-separated"),
        K::optional("mode", "core | jetstream", "jetstream"),
        // JetStream
        K::optional(
            "stream",
            "JetStream stream name (required in jetstream mode)",
            "",
        ),
        K::optional(
            "consumer",
            "Durable consumer name (required in jetstream mode)",
            "",
        ),
        K::optional("subject", "Single subject or wildcard (e.g., orders.>)", ""),
        K::optional(
            "subject.filters",
            "Comma-separated filter subjects (JS 2.10+)",
            "",
        ),
        K::optional(
            "deliver.policy",
            "all | new | by_start_sequence | by_start_time",
            "all",
        ),
        K::optional(
            "start.sequence",
            "Stream sequence for by_start_sequence",
            "",
        ),
        K::optional("start.time", "RFC3339 timestamp for by_start_time", ""),
        K::optional("ack.policy", "explicit | none", "explicit"),
        K::optional(
            "ack.wait.ms",
            "Per-message ack wait in milliseconds",
            "60000",
        ),
        K::optional(
            "max.deliver",
            "Max delivery attempts before poison action",
            "5",
        ),
        K::optional(
            "max.ack.pending",
            "Max unacked messages (server-side flow control)",
            "10000",
        ),
        K::optional("fetch.batch", "Messages per pull fetch", "500"),
        K::optional("fetch.max.wait.ms", "Max wait per fetch", "500"),
        K::optional("fetch.max.bytes", "Max bytes per fetch", "1048576"),
        K::optional(
            "fetch.error.threshold",
            "Consecutive fetch errors before the source reports Unhealthy",
            "10",
        ),
        // Core
        K::optional(
            "queue.group",
            "Queue group for load balancing (core mode only)",
            "",
        ),
        // Format / metadata
        K::optional("format", "json | csv | raw", "json"),
        K::optional(
            "include.metadata",
            "Emit _subject, _stream_seq, _timestamp columns",
            "false",
        ),
        K::optional("include.headers", "Emit _headers column (JSON)", "false"),
        K::optional(
            "event.time.column",
            "Column name for event time extraction",
            "",
        ),
        // Error handling
        K::optional(
            "poison.dlq.subject",
            "Republish Term'd messages to this subject",
            "",
        ),
    ];
    keys.extend(auth_and_tls_keys());
    keys
}

fn sink_config_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    let mut keys = vec![
        K::required("servers", "NATS server URLs, comma-separated"),
        K::optional("mode", "core | jetstream", "jetstream"),
        K::optional("stream", "Target stream (used for validation only)", ""),
        K::optional("subject", "Literal subject for every row", ""),
        K::optional(
            "subject.column",
            "Column name whose value is the subject",
            "",
        ),
        K::optional(
            "expected.stream",
            "Nats-Expected-Stream header for fail-fast",
            "",
        ),
        K::optional(
            "delivery.guarantee",
            "at_least_once | exactly_once",
            "at_least_once",
        ),
        K::optional(
            "dedup.id.column",
            "Column used as Nats-Msg-Id (required for exactly-once)",
            "",
        ),
        K::optional(
            "min.duplicate.window.ms",
            "Minimum stream duplicate_window accepted under exactly-once",
            "120000",
        ),
        K::optional("max.pending", "Max outstanding PubAck futures", "4096"),
        K::optional("ack.timeout.ms", "Per-publish ack timeout", "30000"),
        K::optional(
            "flush.batch.size",
            "Records buffered before publish flush",
            "1000",
        ),
        K::optional("format", "json | csv | raw", "json"),
        K::optional(
            "header.columns",
            "Comma-separated columns projected to NATS headers",
            "",
        ),
        K::optional(
            "poison.dlq.subject",
            "Subject for failed-after-retry publishes",
            "",
        ),
    ];
    keys.extend(auth_and_tls_keys());
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_source_appears_in_registry() {
        let registry = ConnectorRegistry::new();
        register_nats_source(&registry);
        assert!(registry.list_sources().contains(&"nats".to_string()));
    }

    #[test]
    fn register_sink_appears_in_registry() {
        let registry = ConnectorRegistry::new();
        register_nats_sink(&registry);
        assert!(registry.list_sinks().contains(&"nats".to_string()));
    }
}
