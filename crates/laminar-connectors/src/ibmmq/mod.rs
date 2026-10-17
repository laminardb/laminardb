//! IBM MQ source and sink over MQI, using IBM's client library loaded at run time.
//!
//! The source gets messages under syncpoint and commits each checkpoint's unit of work only after
//! the checkpoint is durable (at-least-once). Poison messages follow the queue's own `BOTHRESH` and
//! `BOQNAME`, moved atomically within the same unit of work. The sink puts under syncpoint and
//! commits each unit of work before a write returns. Blocking MQI calls run on one owned thread per
//! connector generation, never on the async runtime.

pub mod config;
mod library;
pub mod metadata;
mod metrics;
mod session;
pub mod sink;
pub mod source;
mod worker;

pub use sink::IbmMqSink;
pub use source::IbmMqSource;

use std::sync::Arc;

use arrow_schema::Schema;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the IBM MQ source connector.
///
/// # Errors
///
/// Returns an error if the source is already registered or the registry is frozen.
pub fn register_ibmmq_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    let info = ConnectorInfo {
        schema_capabilities: crate::schema::resolution::SchemaCapabilities {
            built_in_formats: vec![
                "raw".into(),
                "bytes".into(),
                #[cfg(feature = "fpml")]
                "fpml".into(),
            ],
            ..crate::schema::resolution::SchemaCapabilities::declared(false)
        },
        name: "ibmmq".to_string(),
        display_name: "IBM MQ Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: source_config_keys(),
    };
    registry.register_source(
        "ibmmq",
        info,
        Arc::new(|registry| {
            Ok(Box::new(IbmMqSource::new(
                Arc::new(Schema::empty()),
                registry.map(Arc::as_ref),
            )))
        }),
    )
}

/// Registers the IBM MQ sink connector.
///
/// # Errors
///
/// Returns an error if the sink is already registered or the registry is frozen.
pub fn register_ibmmq_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    let info = ConnectorInfo {
        schema_capabilities: crate::schema::resolution::SchemaCapabilities::declared(true),
        name: "ibmmq".to_string(),
        display_name: "IBM MQ Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: sink_config_keys(),
    };
    registry.register_sink(
        "ibmmq",
        info,
        Arc::new(|_config, registry| {
            Ok(Box::new(IbmMqSink::new(
                Arc::new(Schema::empty()),
                registry.map(Arc::as_ref),
            )))
        }),
    )
}

fn connection_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    vec![
        K::optional("queue.manager", "Queue manager name (MQCONNX)", ""),
        K::optional(
            "channel",
            "Server-connection channel (MQCD ChannelName)",
            "",
        ),
        K::optional(
            "connection.name",
            "CONNAME list host(port)[,host(port)] (MQCD ConnectionName)",
            "",
        ),
        K::optional(
            "ccdt.url",
            "Client channel definition table URL (MQCNO CCDTUrl)",
            "",
        ),
        K::optional("username", "MQCSP user", ""),
        K::optional(
            "password",
            "MQCSP password; use a ${ENV} secret reference",
            "",
        ),
        K::optional("application.name", "MQCNO application name", "laminardb"),
        K::optional(
            "tls.cipher.spec",
            "SSLCIPH CipherSpec, e.g. ANY_TLS13_OR_HIGHER",
            "",
        ),
        K::optional(
            "tls.key.repository",
            "Key repository (MQSCO KeyRepository)",
            "",
        ),
        K::optional(
            "tls.key.repository.password",
            "Key repository password (secret)",
            "",
        ),
        K::optional("tls.certificate.label", "Client certificate label", ""),
        K::optional(
            "tls.peer.name",
            "SSLPEER filter the server certificate must match",
            "",
        ),
        K::optional(
            "reconnect",
            "disabled | qmgr | any (MQCNO reconnect; the source supports disabled only)",
            "disabled",
        ),
        K::optional("client.library", "Path to the IBM MQ client libmqm_r", ""),
        K::optional(
            "operation.timeout.ms",
            "How long one MQI operation may take before the generation is abandoned",
            "30000",
        ),
    ]
}

fn source_config_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    let mut keys = connection_keys();
    keys.extend([
        K::required("queue", "Queue to get from under syncpoint"),
        K::optional(
            "wait.interval.ms",
            "MQGMO WaitInterval for the first message of a poll",
            "100",
        ),
        K::optional(
            "max.message.bytes",
            "Receive buffer; longer messages are never truncated",
            "4194304",
        ),
        K::optional("max.batch.bytes", "Payload bytes per poll", "16777216"),
        K::optional(
            "max.uow.messages",
            "Messages per unit of work before intake waits for a checkpoint",
            "10000",
        ),
        K::optional(
            "max.uow.bytes",
            "Payload bytes per unit of work before intake waits for a checkpoint",
            "67108864",
        ),
        K::optional(
            "on.decode.error",
            "fail | backout-queue (move to BOQNAME in the same unit of work)",
            "fail",
        ),
        K::optional(
            "convert",
            "MQGMO_CONVERT to UTF-8 for convertible formats",
            "false",
        ),
        K::optional(
            "metadata.columns",
            "Comma-separated MQMD/property columns (msg_id, correl_id, group_id, msg_seq_number, \
             msg_flags, msg_type, format, ccsid, encoding, persistence, priority, expiry, \
             backout_count, reply_to_queue, reply_to_queue_manager, put_timestamp, \
             put_application_name, user_identifier, properties)",
            "",
        ),
    ]);
    #[cfg(feature = "fpml")]
    keys.extend(crate::fpml::message::config_keys());
    keys
}

fn sink_config_keys() -> Vec<ConfigKeySpec> {
    use ConfigKeySpec as K;
    let mut keys = connection_keys();
    keys.extend([
        K::required("queue", "Target queue"),
        K::optional(
            "target.queue.manager",
            "MQOD ObjectQMgrName for a remote queue",
            "",
        ),
        K::optional(
            "persistence",
            "persistent | not-persistent | as-queue-definition",
            "persistent",
        ),
        K::optional("priority", "Literal MQMD Priority (0-9)", ""),
        K::optional("priority.column", "Integer column for MQMD Priority", ""),
        K::optional("expiry", "MQMD Expiry in tenths of a second", ""),
        K::optional(
            "message.id.column",
            "Binary/Utf8 column for MsgId (else a new id)",
            "",
        ),
        K::optional(
            "correlation.id.column",
            "Binary/Utf8 column for CorrelId",
            "",
        ),
        K::optional("reply.to.queue", "MQMD ReplyToQ", ""),
        K::optional("reply.to.queue.manager", "MQMD ReplyToQMgr", ""),
        K::optional(
            "message.format",
            "MQMD Format override (defaults by codec)",
            "",
        ),
        K::optional("ccsid", "MQMD CodedCharSetId of text payloads", "1208"),
        K::optional(
            "property.columns",
            "Columns written as typed message properties",
            "",
        ),
        K::optional(
            "max.uow.messages",
            "Messages per committed unit of work",
            "1000",
        ),
    ]);
    keys
}
