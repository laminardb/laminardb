//! WebSocket client source plus client/server sinks.
//!
//! WebSocket is non-replayable. Sources and sinks are best-effort, and failures
//! can produce gaps or duplicates.

mod backpressure;
mod connection;
mod fanout;
mod metrics;
mod parser;
mod protocol;
mod serializer;
mod sink;
mod sink_client;
mod sink_config;
mod sink_metrics;
mod source;
mod source_config;

use std::sync::{Arc, OnceLock, Weak};

use metrics::WebSocketSourceMetrics;
use sink::WebSocketSinkServer;
use sink_client::WebSocketSinkClient;
use sink_config::WebSocketSinkConfig;
use sink_metrics::WebSocketSinkMetrics;
use source::WebSocketSource;
use source_config::WebSocketSourceConfig;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

struct RegisteredMetricFamily<T> {
    registry: Weak<prometheus::Registry>,
    metrics: T,
}

struct MetricFamilyCache<T> {
    family: OnceLock<RegisteredMetricFamily<T>>,
    initialization: parking_lot::Mutex<()>,
}

impl<T> Default for MetricFamilyCache<T> {
    fn default() -> Self {
        Self {
            family: OnceLock::new(),
            initialization: parking_lot::Mutex::new(()),
        }
    }
}

impl<T: Clone> MetricFamilyCache<T> {
    fn get_or_try_init(
        &self,
        registry: &Arc<prometheus::Registry>,
        initialize: impl FnOnce() -> Result<T, crate::error::ConnectorError>,
    ) -> Result<T, crate::error::ConnectorError> {
        if let Some(family) = self.family.get() {
            return family.for_registry(registry);
        }
        let _guard = self.initialization.lock();
        if let Some(family) = self.family.get() {
            return family.for_registry(registry);
        }
        let family = RegisteredMetricFamily {
            registry: Arc::downgrade(registry),
            metrics: initialize()?,
        };
        self.family
            .set(family)
            .unwrap_or_else(|_| unreachable!("metric initialization is serialized"));
        Ok(self
            .family
            .get()
            .expect("metric family was initialized")
            .metrics
            .clone())
    }
}

impl<T: Clone> RegisteredMetricFamily<T> {
    fn for_registry(
        &self,
        registry: &Arc<prometheus::Registry>,
    ) -> Result<T, crate::error::ConnectorError> {
        if !Weak::ptr_eq(&self.registry, &Arc::downgrade(registry)) {
            return Err(crate::error::ConnectorError::ConfigurationError(
                "WebSocket connector registry is already bound to a different Prometheus registry"
                    .into(),
            ));
        }
        Ok(self.metrics.clone())
    }
}

/// Registers the WebSocket source connector with the given registry.
///
/// After registration, the runtime can instantiate `WebSocketSource` by
/// name when processing `CREATE SOURCE ... FROM WEBSOCKET (...)`.
///
/// # Errors
///
/// Returns an error when the source name is already registered or the
/// connector registry is frozen.
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

    let metric_cache = Arc::new(MetricFamilyCache::<WebSocketSourceMetrics>::default());
    registry.register_source(
        "websocket",
        info,
        Arc::new(move |registry: Option<&Arc<prometheus::Registry>>| {
            let metrics = if let Some(registry) = registry {
                metric_cache
                    .get_or_try_init(registry, || WebSocketSourceMetrics::register(registry))?
            } else {
                WebSocketSourceMetrics::local()
            };
            Ok(Box::new(WebSocketSource::new(
                Arc::new(arrow_schema::Schema::empty()),
                WebSocketSourceConfig::default(),
                metrics,
            )))
        }),
    )
}

/// Registers the WebSocket sink connector with the given registry.
///
/// The sink factory selects server or client mode from the validated config
/// before either implementation performs network I/O.
///
/// # Errors
///
/// Returns an error when the sink name is already registered or the connector
/// registry is frozen.
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

    let metric_cache = Arc::new(MetricFamilyCache::<WebSocketSinkMetrics>::default());
    registry.register_sink(
        "websocket",
        info,
        Arc::new(
            move |config, registry: Option<&Arc<prometheus::Registry>>| {
                let sink_config = WebSocketSinkConfig::from_config(config)?;
                let decoded_schema = config.arrow_schema();
                if config.get("_arrow_schema").is_some() && decoded_schema.is_none() {
                    return Err(crate::error::ConnectorError::ConfigurationError(
                        "invalid WebSocket sink _arrow_schema encoding".into(),
                    ));
                }
                let schema = decoded_schema.ok_or_else(|| {
                    crate::error::ConnectorError::ConfigurationError(
                        "WebSocket sink requires a declared Arrow schema".into(),
                    )
                })?;
                let metrics = if let Some(registry) = registry {
                    metric_cache
                        .get_or_try_init(registry, || WebSocketSinkMetrics::register(registry))?
                } else {
                    WebSocketSinkMetrics::local()
                };
                let is_server = matches!(&sink_config, WebSocketSinkConfig::Server { .. });
                let sink: Box<dyn crate::connector::SinkConnector> = if is_server {
                    Box::new(WebSocketSinkServer::new(schema, sink_config, metrics))
                } else {
                    Box::new(WebSocketSinkClient::new(schema, sink_config, metrics))
                };
                Ok(sink)
            },
        ),
    )
}

fn websocket_source_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("url", "WebSocket URL to connect to (ws:// or wss://)"),
        ConfigKeySpec::optional("format", "Message format (json/csv/binary)", "json"),
        ConfigKeySpec::optional(
            "subscribe.message",
            "Text subscription message to send after handshake",
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
        ConfigKeySpec::optional(
            "reconnect.max.retries",
            "Maximum reconnect attempts; empty means unlimited",
            "",
        ),
        ConfigKeySpec::optional(
            "on.backpressure",
            "Backpressure strategy (block/drop_newest)",
            "block",
        ),
        ConfigKeySpec::optional(
            "max.message.size",
            "Max WebSocket message size in bytes",
            "67108864",
        ),
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
            "max.connections",
            "Max concurrent client connections",
            "10000",
        ),
        ConfigKeySpec::optional("ping.interval.ms", "Ping interval in ms", "30000"),
        ConfigKeySpec::optional("ping.timeout.ms", "Pong timeout in ms", "10000"),
        ConfigKeySpec::optional("url", "WebSocket URL required in client mode", ""),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::thread;

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

        let mut missing_schema = crate::config::ConnectorConfig::new("websocket");
        missing_schema.set("mode", "server");
        missing_schema.set("bind.address", "127.0.0.1:0");
        assert!(factory_error(&registry, &missing_schema).contains("declared Arrow schema"));
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

    #[test]
    fn source_metadata_exposes_only_runtime_options() {
        let registry = ConnectorRegistry::new();
        register_websocket_source(&registry).unwrap();
        let info = registry.source_info("websocket").unwrap();
        let keys: std::collections::HashSet<&str> = info
            .config_keys
            .iter()
            .map(|key| key.key.as_str())
            .collect();
        let expected: std::collections::HashSet<&str> = [
            "url",
            "format",
            "subscribe.message",
            "reconnect.enabled",
            "reconnect.initial.delay.ms",
            "reconnect.max.delay.ms",
            "reconnect.max.retries",
            "on.backpressure",
            "max.message.size",
        ]
        .into_iter()
        .collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn sink_metadata_exposes_only_runtime_options() {
        let registry = ConnectorRegistry::new();
        register_websocket_sink(&registry).unwrap();
        let info = registry.sink_info("websocket").unwrap();
        let keys: std::collections::HashSet<&str> = info
            .config_keys
            .iter()
            .map(|key| key.key.as_str())
            .collect();
        let expected: std::collections::HashSet<&str> = [
            "bind.address",
            "mode",
            "max.connections",
            "ping.interval.ms",
            "ping.timeout.ms",
            "url",
        ]
        .into_iter()
        .collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn source_factory_registers_one_shared_metric_family() {
        let connectors = ConnectorRegistry::new();
        register_websocket_source(&connectors).unwrap();
        let metrics = Arc::new(prometheus::Registry::new());
        let config = crate::config::ConnectorConfig::new("websocket");

        connectors.create_source(&config, Some(&metrics)).unwrap();
        connectors.create_source(&config, Some(&metrics)).unwrap();

        let families = metrics.gather();
        assert_eq!(
            families
                .iter()
                .filter(|family| family.name().starts_with("websocket_source_"))
                .count(),
            5
        );
    }

    #[test]
    fn metric_family_cache_is_concurrent_and_shared() {
        let cache = Arc::new(MetricFamilyCache::<WebSocketSourceMetrics>::default());
        let registry = Arc::new(prometheus::Registry::new());
        let workers = (0..16)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let registry = Arc::clone(&registry);
                thread::spawn(move || {
                    let metrics = cache
                        .get_or_try_init(&registry, || WebSocketSourceMetrics::register(&registry))
                        .unwrap();
                    metrics.record_message(1);
                })
            })
            .collect::<Vec<_>>();

        for worker in workers {
            worker.join().unwrap();
        }

        assert_eq!(
            cache.family.get().unwrap().metrics.messages_received.get(),
            16
        );
    }

    #[test]
    fn source_factory_rejects_a_different_metrics_registry() {
        let connectors = ConnectorRegistry::new();
        register_websocket_source(&connectors).unwrap();
        let first = Arc::new(prometheus::Registry::new());
        let second = Arc::new(prometheus::Registry::new());
        let config = crate::config::ConnectorConfig::new("websocket");

        connectors.create_source(&config, Some(&first)).unwrap();
        let error = match connectors.create_source(&config, Some(&second)) {
            Ok(_) => panic!("expected metrics registry mismatch"),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("different Prometheus registry"), "{error}");
    }
}
