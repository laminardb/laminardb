//! WebSocket source/sink connectors. Four modes: source-client (connect
//! to a WS server), source-server (listen for clients), sink-server (fan
//! out results to subscribers), sink-client (push to an external server).
//!
//! WebSocket is non-replayable — source connectors are at-most-once / best-effort;
//! sinks have a bounded, best-effort replay buffer.

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

pub use backpressure::WsBackpressure;
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
pub fn register_websocket_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
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
        Arc::new(|registry: Option<&prometheus::Registry>| {
            use arrow_schema::{DataType, Field, Schema};

            let default_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, false),
            ]));
            Box::new(WebSocketSource::new(
                default_schema,
                WebSocketSourceConfig::default(),
                registry,
            ))
        }),
    )
}

/// Registers the WebSocket sink connector with the given registry.
///
/// The sink factory selects server or client mode from the validated config
/// before either implementation performs network I/O.
/// If `_arrow_schema` is absent, it uses the legacy nullable `key: Utf8` and
/// required `value: Utf8` placeholder schema.
pub fn register_websocket_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
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
        Arc::new(|config, registry: Option<&prometheus::Registry>| {
            use arrow_schema::{DataType, Field, Schema};

            let sink_config = WebSocketSinkConfig::from_config(config)?;
            // DDL normally injects `_arrow_schema`; programmatic callers may
            // rely on the documented key/value placeholder.
            let decoded_schema = config.arrow_schema();
            if config.get("_arrow_schema").is_some() && decoded_schema.is_none() {
                return Err(crate::error::ConnectorError::ConfigurationError(
                    "invalid WebSocket sink _arrow_schema encoding".into(),
                ));
            }
            let schema = decoded_schema.unwrap_or_else(|| {
                Arc::new(Schema::new(vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Utf8, false),
                ]))
            });
            let is_server = matches!(&sink_config.mode, SinkMode::Server { .. });
            let sink: Box<dyn crate::connector::SinkConnector> = if is_server {
                Box::new(WebSocketSinkServer::new(schema, sink_config, registry))
            } else {
                Box::new(WebSocketSinkClient::new(schema, sink_config, registry))
            };
            Ok(sink)
        }),
    )
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
        ConfigKeySpec::optional(
            "bind.address",
            "Socket address required in server mode (e.g., 0.0.0.0:8080)",
            "",
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
        ConfigKeySpec::optional("path", "URL path filter for server mode", ""),
        ConfigKeySpec::optional(
            "slow.client.threshold.pct",
            "Disconnect threshold percentage for slow server clients",
            "90",
        ),
        ConfigKeySpec::optional("url", "WebSocket URL required in client mode", ""),
        ConfigKeySpec::optional(
            "buffer.on.disconnect",
            "Client-mode disconnect buffer size in bytes",
            "",
        ),
        ConfigKeySpec::optional("batch.max.size", "Client-mode maximum batch size", ""),
        ConfigKeySpec::optional(
            "batch.interval.ms",
            "Client-mode batch interval in milliseconds",
            "",
        ),
        ConfigKeySpec::optional("auth.type", "Authentication type (bearer/basic/hmac)", ""),
        ConfigKeySpec::optional("auth.token", "Bearer token for authentication", ""),
        ConfigKeySpec::optional("auth.username", "Basic-auth username", ""),
        ConfigKeySpec::optional("auth.password", "Basic-auth password", ""),
        ConfigKeySpec::optional("auth.api.key", "HMAC API key", ""),
        ConfigKeySpec::optional("auth.secret", "HMAC secret", ""),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn config_with_schema(mode: &str) -> crate::config::ConnectorConfig {
        let mut config = crate::config::ConnectorConfig::new("websocket");
        config.set("mode", mode);
        let schema = Schema::new(vec![Field::new("payload", DataType::Utf8, false)]);
        config.set(
            "_arrow_schema",
            crate::config::encode_arrow_schema_ipc(&schema),
        );
        config
    }

    fn factory_error(
        registry: &ConnectorRegistry,
        config: &crate::config::ConnectorConfig,
    ) -> String {
        match registry.create_sink(config, None) {
            Ok(_) => panic!("expected sink factory error"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn sink_factory_dispatches_server_without_opening_a_socket() {
        let registry = ConnectorRegistry::new();
        register_websocket_sink(&registry).unwrap();
        let mut config = config_with_schema("server");
        config.set("bind.address", "127.0.0.1:0");

        let sink = registry.create_sink(&config, None).unwrap();

        assert!(sink.contract(&config).is_ok());
        assert_eq!(sink.schema().field(0).name(), "payload");
    }

    #[test]
    fn sink_factory_dispatches_client_without_opening_a_socket() {
        let registry = ConnectorRegistry::new();
        register_websocket_sink(&registry).unwrap();
        let mut config = config_with_schema("client");
        config.set("url", "wss://example.test/events");

        let sink = registry.create_sink(&config, None).unwrap();

        assert!(sink.contract(&config).is_ok());
        assert_eq!(sink.schema().field(0).name(), "payload");
    }

    #[test]
    fn sink_factory_rejects_missing_or_invalid_mode_specific_config() {
        let registry = ConnectorRegistry::new();
        register_websocket_sink(&registry).unwrap();

        let missing_server_bind = config_with_schema("server");
        assert!(factory_error(&registry, &missing_server_bind).contains("bind.address"));

        let mut invalid_server_bind = config_with_schema("server");
        invalid_server_bind.set("bind.address", "not-a-socket-address");
        assert!(factory_error(&registry, &invalid_server_bind).contains("invalid WebSocket server"));

        let mut missing_client_url = config_with_schema("client");
        assert!(factory_error(&registry, &missing_client_url).contains("url"));

        missing_client_url.set("url", "https://example.test/not-websocket");
        assert!(factory_error(&registry, &missing_client_url).contains("expected ws:// or wss://"));

        let mut invalid_schema = config_with_schema("server");
        invalid_schema.set("bind.address", "127.0.0.1:0");
        invalid_schema.set("_arrow_schema", "not-arrow-ipc");
        assert!(factory_error(&registry, &invalid_schema).contains("_arrow_schema"));
    }

    #[test]
    fn bind_address_metadata_is_mode_conditional() {
        let registry = ConnectorRegistry::new();
        register_websocket_sink(&registry).unwrap();
        let info = registry.sink_info("websocket").unwrap();
        let bind = info
            .config_keys
            .iter()
            .find(|key| key.key == "bind.address")
            .unwrap();
        assert!(!bind.required);
    }
}
