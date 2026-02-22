//! WebSocket source and sink connectors for LaminarDB.
//!
//! Provides four connector modes:
//! - **Source client**: Connects to an external WebSocket server (e.g., exchange feeds)
//! - **Source server**: Listens for incoming WebSocket connections (e.g., `IoT` sensors)
//! - **Sink server**: Fans out streaming query results to connected subscribers
//! - **Sink client**: Pushes streaming query output to an external WebSocket server
//!
//! # Delivery Guarantees
//!
//! WebSocket is a **non-replayable** transport. Source connectors provide
//! **at-most-once** or **best-effort** delivery — there are no offsets to
//! seek to on recovery. Sink connectors optionally support a replay buffer
//! for client-side resume, but this is bounded and best-effort.
//!
//! # Architecture
//!
//! All WebSocket I/O runs in Ring 2 (Tokio tasks). Parsed Arrow
//! `RecordBatch` data crosses to Ring 0 via bounded channels.

pub mod backpressure;
pub mod checkpoint;
pub mod connection;
pub mod fanout;
pub mod metrics;
pub mod parser;
pub mod protocol;
pub mod serializer;
pub mod sink;
pub mod sink_client;
pub mod sink_config;
pub mod sink_metrics;
pub mod source;
pub mod source_config;
pub mod source_server;

pub use backpressure::BackpressureStrategy;
pub use checkpoint::WebSocketSourceCheckpoint;
pub use metrics::WebSocketSourceMetrics;
pub use protocol::{ClientMessage, ServerMessage};
pub use sink::WebSocketSinkServer;
pub use sink_client::WebSocketSinkClient;
pub use sink_config::{SinkFormat, SinkMode, SlowClientPolicy, WebSocketSinkConfig};
pub use sink_metrics::WebSocketSinkMetrics;
pub use source::WebSocketSource;
pub use source_config::{
    EventTimeFormat, MessageFormat, ReconnectConfig, SourceMode, WebSocketSourceConfig,
    WsAuthConfig,
};
pub use source_server::WebSocketSourceServer;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the WebSocket source connector with the given registry.
///
/// After registration, the runtime can instantiate `WebSocketSource` by
/// name when processing `CREATE SOURCE ... WITH (connector = 'websocket')`.
pub fn register_websocket_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "websocket".to_string(),
        display_name: "WebSocket Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: websocket_source_config_keys(),
    };

    registry.register_source(
        "websocket",
        info,
        Arc::new(|| {
            use arrow_schema::{DataType, Field, Schema};

            let default_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, false),
            ]));
            Box::new(WebSocketSource::new(
                default_schema,
                WebSocketSourceConfig::default(),
            ))
        }),
    );
}

/// Registers the WebSocket sink connector with the given registry.
///
/// After registration, the runtime can instantiate `WebSocketSinkServer` by
/// name when processing `CREATE SINK ... WITH (connector = 'websocket')`.
pub fn register_websocket_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "websocket".to_string(),
        display_name: "WebSocket Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: websocket_sink_config_keys(),
    };

    registry.register_sink(
        "websocket",
        info,
        Arc::new(|| {
            use arrow_schema::{DataType, Field, Schema};

            let default_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, false),
            ]));
            Box::new(WebSocketSinkServer::new(
                default_schema,
                WebSocketSinkConfig::default(),
            ))
        }),
    );
}

fn websocket_source_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("url", "WebSocket URL to connect to (ws:// or wss://)"),
        ConfigKeySpec::optional("mode", "Operating mode (client/server)", "client"),
        ConfigKeySpec::optional("format", "Message format (json/csv/binary)", "json"),
        ConfigKeySpec::optional(
            "subscribe.message",
            "JSON subscription message to send after handshake",
            "",
        ),
        ConfigKeySpec::optional("reconnect.enabled", "Enable automatic reconnection", "true"),
        ConfigKeySpec::optional(
            "reconnect.initial.delay.ms",
            "Initial reconnect delay in ms",
            "100",
        ),
        ConfigKeySpec::optional(
            "reconnect.max.delay.ms",
            "Maximum reconnect delay in ms",
            "30000",
        ),
        ConfigKeySpec::optional("ping.interval.ms", "WebSocket ping interval in ms", "30000"),
        ConfigKeySpec::optional("ping.timeout.ms", "Pong reply timeout in ms", "10000"),
        ConfigKeySpec::optional("bind.address", "Socket address for server mode", ""),
        ConfigKeySpec::optional(
            "max.connections",
            "Max concurrent connections (server mode)",
            "1024",
        ),
        ConfigKeySpec::optional(
            "on.backpressure",
            "Backpressure strategy (block/drop)",
            "block",
        ),
        ConfigKeySpec::optional(
            "max.message.size",
            "Max WebSocket message size in bytes",
            "67108864",
        ),
        ConfigKeySpec::optional(
            "event.time.field",
            "JSON field path for event time extraction",
            "",
        ),
        ConfigKeySpec::optional(
            "event.time.format",
            "Event time format (epoch_millis/iso8601)",
            "",
        ),
        ConfigKeySpec::optional("auth.type", "Authentication type (bearer/basic/hmac)", ""),
        ConfigKeySpec::optional("auth.token", "Bearer token for authentication", ""),
    ]
}

fn websocket_sink_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required(
            "bind.address",
            "Socket address to bind (e.g., 0.0.0.0:8080)",
        ),
        ConfigKeySpec::optional("mode", "Operating mode (server/client)", "server"),
        ConfigKeySpec::optional(
            "format",
            "Serialization format (json/jsonlines/arrow_ipc)",
            "json",
        ),
        ConfigKeySpec::optional(
            "max.connections",
            "Max concurrent client connections",
            "10000",
        ),
        ConfigKeySpec::optional(
            "per.client.buffer",
            "Per-client send buffer in bytes",
            "262144",
        ),
        ConfigKeySpec::optional(
            "slow.client.policy",
            "Slow client policy (drop_oldest/disconnect)",
            "drop_oldest",
        ),
        ConfigKeySpec::optional("ping.interval.ms", "Ping interval in ms", "30000"),
        ConfigKeySpec::optional("ping.timeout.ms", "Pong timeout in ms", "10000"),
        ConfigKeySpec::optional(
            "replay.buffer.size",
            "Messages to buffer for late joiners",
            "",
        ),
        ConfigKeySpec::optional("url", "WebSocket URL for client mode", ""),
        ConfigKeySpec::optional("auth.type", "Authentication type (bearer/basic/hmac)", ""),
        ConfigKeySpec::optional("auth.token", "Bearer token for authentication", ""),
    ]
}
