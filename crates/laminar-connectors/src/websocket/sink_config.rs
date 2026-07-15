//! WebSocket sink configuration.

use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

const MAX_SERVER_CONNECTIONS: usize = 65_536;
const MIN_HEARTBEAT: Duration = Duration::from_secs(1);

const fn default_max_connections() -> usize {
    10_000
}

const SINK_OPTIONS: &[&str] = &[
    "_arrow_schema",
    "bind.address",
    "delivery.guarantee",
    "max.connections",
    "mode",
    "ping.interval.ms",
    "ping.timeout.ms",
    "sink.write.timeout.ms",
    "url",
];

fn require_non_empty(config: &ConnectorConfig, key: &str) -> Result<String, ConnectorError> {
    let value = config.require(key)?.trim();
    if value.is_empty() {
        return Err(ConnectorError::ConfigurationError(format!(
            "WebSocket sink option '{key}' must not be empty"
        )));
    }
    Ok(value.to_string())
}

/// Configuration and network ownership for a WebSocket sink.
#[derive(Debug, Clone)]
pub enum WebSocketSinkConfig {
    /// Bind an endpoint and fan out JSON records to connected clients.
    Server {
        /// Address to bind.
        bind_address: String,
        /// Maximum simultaneous clients.
        max_connections: usize,
        /// Heartbeat interval.
        ping_interval: Duration,
        /// Pong deadline.
        ping_timeout: Duration,
    },
    /// Connect to one external endpoint and push JSON records.
    Client {
        /// Destination URL.
        url: String,
    },
}

impl WebSocketSinkConfig {
    /// Builds sink configuration from a SQL/programmatic connector map.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for missing, invalid, or removed options.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        reject_removed_sink_options(config)?;
        config.reject_unknown_properties(SINK_OPTIONS, "WebSocket sink")?;
        let parsed = match config
            .get("mode")
            .unwrap_or("server")
            .to_ascii_lowercase()
            .as_str()
        {
            "server" => parse_server_mode(config)?,
            "client" => parse_client_mode(config)?,
            other => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid WebSocket sink mode '{other}': expected server or client"
                )));
            }
        };
        parsed.validate()?;
        Ok(parsed)
    }

    pub(super) fn validate(&self) -> Result<(), ConnectorError> {
        match self {
            Self::Server {
                bind_address,
                max_connections,
                ping_interval,
                ping_timeout,
            } => {
                bind_address
                    .parse::<std::net::SocketAddr>()
                    .map_err(|error| {
                        ConnectorError::ConfigurationError(format!(
                            "invalid WebSocket server bind.address '{bind_address}': {error}"
                        ))
                    })?;
                if !(1..=MAX_SERVER_CONNECTIONS).contains(max_connections) {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "WebSocket server max.connections must be between 1 and {MAX_SERVER_CONNECTIONS}"
                    )));
                }
                if *ping_interval < MIN_HEARTBEAT || *ping_timeout < MIN_HEARTBEAT {
                    return Err(ConnectorError::ConfigurationError(
                        "WebSocket server ping interval and timeout must be at least 1000 ms"
                            .into(),
                    ));
                }
            }
            Self::Client { url } => {
                let safe_url = crate::security::sanitize_identity_value("url", url);
                let parsed = url::Url::parse(url).map_err(|error| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid WebSocket client URL '{safe_url}': {error}"
                    ))
                })?;
                if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host().is_none() {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "invalid WebSocket client URL '{safe_url}': expected ws:// or wss:// with a host"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn parse_server_mode(config: &ConnectorConfig) -> Result<WebSocketSinkConfig, ConnectorError> {
    reject_present_options(config, &["url"], "server")?;
    Ok(WebSocketSinkConfig::Server {
        bind_address: require_non_empty(config, "bind.address")?,
        max_connections: config
            .get_parsed("max.connections")?
            .unwrap_or(default_max_connections()),
        ping_interval: Duration::from_millis(
            config.get_parsed("ping.interval.ms")?.unwrap_or(30_000),
        ),
        ping_timeout: Duration::from_millis(
            config.get_parsed("ping.timeout.ms")?.unwrap_or(10_000),
        ),
    })
}

fn parse_client_mode(config: &ConnectorConfig) -> Result<WebSocketSinkConfig, ConnectorError> {
    reject_present_options(
        config,
        &[
            "bind.address",
            "max.connections",
            "ping.interval.ms",
            "ping.timeout.ms",
        ],
        "client",
    )?;
    Ok(WebSocketSinkConfig::Client {
        url: require_non_empty(config, "url")?,
    })
}

fn reject_removed_sink_options(config: &ConnectorConfig) -> Result<(), ConnectorError> {
    if config.get("format").is_some() {
        return Err(ConnectorError::ConfigurationError(
            "WebSocket sink option 'format' was removed; the wire format is always JSON".into(),
        ));
    }
    for key in [
        "reconnect.enabled",
        "reconnect.initial.delay.ms",
        "reconnect.max.delay.ms",
        "reconnect.max.retries",
    ] {
        if config.get(key).is_some() {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket sink option '{key}' is not supported; client reconnect policy is fixed"
            )));
        }
    }
    for key in [
        "per.client.buffer",
        "per.client.buffer.bytes",
        "slow.client.policy",
        "replay.buffer.size",
        "replay.buffer.bytes",
        "buffer.on.disconnect",
        "path",
        "slow.client.threshold.pct",
        "batch.max.size",
        "batch.interval.ms",
        "auth.type",
        "auth.token",
        "auth.username",
        "auth.password",
        "auth.api.key",
        "auth.secret",
    ] {
        if config.get(key).is_some() {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket sink option '{key}' was removed because the runtime does not implement it"
            )));
        }
    }
    Ok(())
}

fn reject_present_options(
    config: &ConnectorConfig,
    keys: &[&str],
    mode: &str,
) -> Result<(), ConnectorError> {
    if let Some(key) = keys.iter().find(|key| config.get(key).is_some()) {
        return Err(ConnectorError::ConfigurationError(format!(
            "WebSocket sink option '{key}' is not valid in {mode} mode"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_server_runtime_options() {
        let mut config = ConnectorConfig::new("websocket");
        config.set("bind.address", "127.0.0.1:8080");
        config.set("max.connections", "2000");
        config.set("ping.interval.ms", "5000");
        config.set("ping.timeout.ms", "1000");

        let parsed = WebSocketSinkConfig::from_config(&config).unwrap();
        assert!(matches!(
            parsed,
            WebSocketSinkConfig::Server {
                max_connections: 2000,
                ..
            }
        ));
    }

    #[test]
    fn parses_client_runtime_options() {
        let mut config = ConnectorConfig::new("websocket");
        config.set("mode", "client");
        config.set("url", "wss://sink.example/events");

        assert!(matches!(
            WebSocketSinkConfig::from_config(&config).unwrap(),
            WebSocketSinkConfig::Client { ref url } if url == "wss://sink.example/events"
        ));
    }

    #[test]
    fn rejects_invalid_server_bounds() {
        let missing_bind = ConnectorConfig::new("websocket");
        assert!(WebSocketSinkConfig::from_config(&missing_bind).is_err());

        for (key, value) in [
            ("max.connections", (MAX_SERVER_CONNECTIONS + 1).to_string()),
            ("ping.interval.ms", "999".into()),
            ("ping.timeout.ms", "999".into()),
        ] {
            let mut config = ConnectorConfig::new("websocket");
            config.set("bind.address", "127.0.0.1:8080");
            config.set(key, value);
            let error = WebSocketSinkConfig::from_config(&config)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(if key == "max.connections" {
                    key
                } else {
                    "ping"
                }),
                "{error}"
            );
        }
    }

    #[test]
    fn rejects_invalid_client_url() {
        let mut config = ConnectorConfig::new("websocket");
        config.set("mode", "client");
        config.set("url", "https://sink.example/events");
        assert!(WebSocketSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn rejects_options_from_the_other_mode() {
        let mut server = ConnectorConfig::new("websocket");
        server.set("bind.address", "127.0.0.1:8080");
        server.set("url", "wss://sink.example/events");
        assert!(WebSocketSinkConfig::from_config(&server)
            .unwrap_err()
            .to_string()
            .contains("url"));

        for key in [
            "bind.address",
            "max.connections",
            "ping.interval.ms",
            "ping.timeout.ms",
        ] {
            let mut client = ConnectorConfig::new("websocket");
            client.set("mode", "client");
            client.set("url", "wss://sink.example/events");
            client.set(key, "1000");
            assert!(WebSocketSinkConfig::from_config(&client)
                .unwrap_err()
                .to_string()
                .contains(key));
        }
    }

    #[test]
    fn rejects_removed_options() {
        for key in [
            "format",
            "reconnect.enabled",
            "per.client.buffer.bytes",
            "slow.client.policy",
            "replay.buffer.size",
            "buffer.on.disconnect",
            "path",
            "batch.max.size",
            "auth.token",
        ] {
            let mut config = ConnectorConfig::new("websocket");
            config.set("bind.address", "127.0.0.1:8080");
            config.set(key, "removed");
            assert!(WebSocketSinkConfig::from_config(&config)
                .unwrap_err()
                .to_string()
                .contains(key));
        }
    }

    #[test]
    fn accepts_engine_options_and_rejects_typos() {
        let mut config = ConnectorConfig::new("websocket");
        config.set("bind.address", "127.0.0.1:8080");
        config.set("_arrow_schema", "engine-injected");
        config.set("delivery.guarantee", "at-least-once");
        config.set("sink.write.timeout.ms", "5000");
        WebSocketSinkConfig::from_config(&config).unwrap();

        config.set("max.conections", "1");
        assert!(WebSocketSinkConfig::from_config(&config)
            .unwrap_err()
            .to_string()
            .contains("max.conections"));
    }

    #[test]
    fn typed_configuration_uses_the_same_validation() {
        let invalid = WebSocketSinkConfig::Server {
            bind_address: "127.0.0.1:8080".into(),
            max_connections: 0,
            ping_interval: Duration::from_secs(1),
            ping_timeout: Duration::from_secs(1),
        };
        assert!(invalid.validate().is_err());
    }
}
