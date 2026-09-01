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
mod tests;
