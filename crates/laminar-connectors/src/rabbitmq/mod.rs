//! RabbitMQ source and sink over AMQP 0-9-1 (the `lapin` client).
//!
//! The source consumes one classic or quorum queue with manual acknowledgements and releases a
//! message only after the checkpoint containing it is durable (at-least-once). The sink publishes
//! to one exchange with publisher confirms and, by default, mandatory routing. Routing, TTL,
//! dead-lettering, delivery limits, priorities, and queue limits stay broker-native: exchanges and
//! bindings route, and policies or declaration arguments own the rest.

pub mod config;
mod connection;
pub mod metadata;
mod metrics;
pub mod sink;
pub mod source;

pub use sink::RabbitMqSink;
pub use source::RabbitMqSource;

use std::sync::Arc;

use arrow_schema::Schema;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the RabbitMQ source connector.
///
/// # Errors
///
/// Returns an error if the source is already registered or the registry is frozen.
pub fn register_rabbitmq_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    let info = ConnectorInfo {
        schema_capabilities: crate::schema::resolution::SchemaCapabilities {
            built_in_formats: vec!["raw".into(), "bytes".into()],
            ..crate::schema::resolution::SchemaCapabilities::declared(false)
        },
        name: "rabbitmq".to_string(),
        display_name: "RabbitMQ Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: source_config_keys(),
    };
    registry.register_source(
        "rabbitmq",
        info,
        Arc::new(|registry| {
            Ok(Box::new(RabbitMqSource::new(
                Arc::new(Schema::empty()),
                registry.map(Arc::as_ref),
            )))
        }),
    )
}

/// Registers the RabbitMQ sink connector.
///
/// # Errors
///
/// Returns an error if the sink is already registered or the registry is frozen.
pub fn register_rabbitmq_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    let info = ConnectorInfo {
        schema_capabilities: crate::schema::resolution::SchemaCapabilities::declared(true),
        name: "rabbitmq".to_string(),
        display_name: "RabbitMQ Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: sink_config_keys(),
    };
    registry.register_sink(
        "rabbitmq",
        info,
        Arc::new(|_config, registry| {
            Ok(Box::new(RabbitMqSink::new(
                Arc::new(Schema::empty()),
                registry.map(Arc::as_ref),
            )))
        }),
    )
}

fn connection_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    vec![
        K::required(
            "url",
            "AMQP URI(s), comma-separated failover endpoints of one broker or cluster: \
             amqp[s]://[user:pass@]host[:port]/vhost[?heartbeat=&frame_max=&channel_max=\
             &connection_timeout=&auth_mechanism=]",
        ),
        K::optional("username", "SASL user (instead of URI userinfo)", ""),
        K::optional(
            "password",
            "SASL password; use a ${ENV} secret reference",
            "",
        ),
        K::optional(
            "connection.name",
            "Client-provided connection name shown by the broker",
            "",
        ),
        K::optional(
            "connect.timeout.ms",
            "Per-endpoint connect bound (or the URI connection_timeout, not both)",
            "10000",
        ),
        K::optional(
            "tls.ca.location",
            "PEM CA bundle trusted in addition to the Web PKI roots (amqps)",
            "",
        ),
        K::optional(
            "tls.cert.location",
            "PEM client certificate for mutual TLS (amqps)",
            "",
        ),
        K::optional(
            "tls.key.location",
            "PKCS#8 PEM client key for mutual TLS (amqps)",
            "",
        ),
    ]
}

fn source_config_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    let mut keys = connection_keys();
    keys.extend([
        K::required("queue", "Classic or quorum queue to consume"),
        K::optional(
            "queue.declare",
            "Declare the queue idempotently instead of requiring it to exist",
            "false",
        ),
        K::optional("queue.durable", "Durable flag when declaring", "true"),
        K::optional(
            "queue.type",
            "x-queue-type when declaring: classic | quorum",
            "",
        ),
        K::optional(
            "queue.arguments",
            "JSON object of declaration-only x-arguments; prefer broker policies for mutable \
             settings (TTL, dead-lettering, limits)",
            "",
        ),
        K::optional(
            "queue.bind.exchange",
            "Exchange to bind the queue to at startup",
            "",
        ),
        K::optional(
            "queue.bind.routing.keys",
            "Comma-separated binding keys",
            "",
        ),
        K::optional(
            "queue.bind.arguments",
            "JSON object of binding arguments",
            "",
        ),
        K::optional(
            "prefetch.count",
            "basic.qos prefetch: most unacknowledged deliveries; size it for one checkpoint \
             interval of traffic",
            "1000",
        ),
        K::optional(
            "consumer.tag",
            "Consumer tag (broker-generated when unset)",
            "",
        ),
        K::optional("consumer.exclusive", "Exclusive consumer", "false"),
        K::optional(
            "consumer.arguments",
            "JSON object of basic.consume arguments",
            "",
        ),
        K::optional(
            "max.message.bytes",
            "Largest accepted message body",
            "16777216",
        ),
        K::optional(
            "max.batch.bytes",
            "Payload bytes decoded per batch",
            "33554432",
        ),
        K::optional(
            "on.decode.error",
            "fail | reject (basic.reject without requeue: dead-lettered by the queue's DLX, \
             otherwise discarded)",
            "fail",
        ),
        K::optional(
            "metadata.columns",
            "Comma-separated delivery metadata columns (exchange, routing_key, redelivered, \
             delivery_count, message_id, correlation_id, timestamp, content_type, \
             content_encoding, delivery_mode, priority, expiration, reply_to, type, user_id, \
             app_id, headers)",
            "",
        ),
    ]);
    keys
}

fn sink_config_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    let mut keys = connection_keys();
    keys.extend([
        K::optional(
            "exchange",
            "Target exchange; empty is the default exchange",
            "",
        ),
        K::optional(
            "exchange.declare",
            "Declare the exchange idempotently",
            "false",
        ),
        K::optional("exchange.type", "Exchange type when declaring", ""),
        K::optional("exchange.durable", "Durable flag when declaring", "true"),
        K::optional("routing.key", "Literal routing key", ""),
        K::optional(
            "routing.key.column",
            "Utf8 column holding each message's routing key",
            "",
        ),
        K::optional(
            "mandatory",
            "Return unroutable messages; a return fails the write",
            "true",
        ),
        K::optional("delivery.mode", "persistent | transient", "persistent"),
        K::optional(
            "content.type",
            "content-type property (defaults by format)",
            "",
        ),
        K::optional("content.encoding", "content-encoding property", ""),
        K::optional(
            "message.id.column",
            "Utf8 column for the message-id property",
            "",
        ),
        K::optional(
            "correlation.id.column",
            "Utf8 column for the correlation-id property",
            "",
        ),
        K::optional("priority", "Literal priority property (0-255)", ""),
        K::optional(
            "priority.column",
            "Integer column for the priority property",
            "",
        ),
        K::optional(
            "expiration.ms",
            "Per-message TTL property in milliseconds",
            "",
        ),
        K::optional("reply.to", "reply-to property", ""),
        K::optional("type", "type property", ""),
        K::optional("app.id", "app-id property", ""),
        K::optional(
            "header.columns",
            "Comma-separated columns written as typed headers",
            "",
        ),
        K::optional("max.pending", "Most publishes awaiting a confirm", "4096"),
        K::optional(
            "max.pending.bytes",
            "Most payload bytes awaiting a confirm",
            "67108864",
        ),
        K::optional("confirm.timeout.ms", "Bound for awaiting confirms", "30000"),
    ]);
    keys
}
