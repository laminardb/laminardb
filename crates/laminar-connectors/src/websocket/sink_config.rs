//! WebSocket sink connector configuration.
//!
//! [`WebSocketSinkConfig`] controls how the sink writes Arrow data to
//! WebSocket clients (server mode) or to an upstream WebSocket server
//! (client mode). Parsed from a SQL `WITH (...)` clause or constructed
//! programmatically.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::source_config::{ReconnectConfig, WsAuthConfig};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

// ---------------------------------------------------------------------------
// Serde default helpers
// ---------------------------------------------------------------------------

/// Default maximum number of concurrent WebSocket connections.
fn default_max_connections() -> usize {
    10_000
}

/// Default per-client send buffer size in bytes (256 KB).
fn default_per_client_buffer() -> usize {
    262_144
}

/// Default WebSocket ping interval.
fn default_ping_interval() -> Duration {
    Duration::from_secs(30)
}

/// Default timeout waiting for a pong response.
fn default_ping_timeout() -> Duration {
    Duration::from_secs(10)
}

fn require_non_empty(config: &ConnectorConfig, key: &str) -> Result<String, ConnectorError> {
    let value = config.require(key)?.trim();
    if value.is_empty() {
        return Err(ConnectorError::ConfigurationError(format!(
            "WebSocket sink option '{key}' must not be empty"
        )));
    }
    Ok(value.to_string())
}

fn parse_server_mode(config: &ConnectorConfig) -> Result<SinkMode, ConnectorError> {
    let bind_address = require_non_empty(config, "bind.address")?;
    bind_address.parse::<std::net::SocketAddr>().map_err(|e| {
        ConnectorError::ConfigurationError(format!(
            "invalid WebSocket server bind.address '{bind_address}': {e}"
        ))
    })?;
    let max_connections = config
        .get_parsed("max.connections")?
        .unwrap_or(default_max_connections());
    if max_connections == 0 {
        return Err(ConnectorError::ConfigurationError(
            "WebSocket server max.connections must be greater than zero".into(),
        ));
    }
    let per_client_buffer = config
        .get_parsed("per.client.buffer")?
        .unwrap_or(default_per_client_buffer());
    if per_client_buffer == 0 {
        return Err(ConnectorError::ConfigurationError(
            "WebSocket server per.client.buffer must be greater than zero".into(),
        ));
    }
    let ping_interval_ms = config.get_parsed("ping.interval.ms")?.unwrap_or(30_000);
    let ping_timeout_ms = config.get_parsed("ping.timeout.ms")?.unwrap_or(10_000);
    if ping_interval_ms == 0 || ping_timeout_ms == 0 {
        return Err(ConnectorError::ConfigurationError(
            "WebSocket server ping intervals must be greater than zero".into(),
        ));
    }
    let replay_buffer_size = config.get_parsed("replay.buffer.size")?;
    let path = config.get("path").map(ToString::to_string);

    let slow_client_policy = match config
        .get("slow.client.policy")
        .map(str::to_lowercase)
        .as_deref()
    {
        None | Some("drop_oldest") => SlowClientPolicy::DropOldest,
        Some("drop_newest") => SlowClientPolicy::DropNewest,
        Some("disconnect") => {
            let threshold_pct = config
                .get_parsed("slow.client.threshold.pct")?
                .unwrap_or(90);
            if !(1..=100).contains(&threshold_pct) {
                return Err(ConnectorError::ConfigurationError(
                    "slow.client.threshold.pct must be between 1 and 100".into(),
                ));
            }
            SlowClientPolicy::Disconnect { threshold_pct }
        }
        Some(other) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "invalid slow.client.policy '{other}': expected drop_oldest, drop_newest, or disconnect"
            )));
        }
    };

    Ok(SinkMode::Server {
        bind_address,
        path,
        max_connections,
        per_client_buffer,
        slow_client_policy,
        ping_interval: Duration::from_millis(ping_interval_ms),
        ping_timeout: Duration::from_millis(ping_timeout_ms),
        enable_subscription_filter: false,
        replay_buffer_size,
    })
}

fn parse_client_mode(config: &ConnectorConfig) -> Result<SinkMode, ConnectorError> {
    let url = require_non_empty(config, "url")?;
    let parsed_url = url::Url::parse(&url).map_err(|e| {
        ConnectorError::ConfigurationError(format!("invalid WebSocket client URL '{url}': {e}"))
    })?;
    if !matches!(parsed_url.scheme(), "ws" | "wss") || parsed_url.host().is_none() {
        return Err(ConnectorError::ConfigurationError(format!(
            "invalid WebSocket client URL '{url}': expected ws:// or wss:// with a host"
        )));
    }
    let buffer_on_disconnect = config.get_parsed("buffer.on.disconnect")?;
    let batch_max_size = config.get_parsed("batch.max.size")?;
    let batch_interval_ms = config.get_parsed("batch.interval.ms")?;
    if batch_max_size == Some(0) {
        return Err(ConnectorError::ConfigurationError(
            "WebSocket client batch.max.size must be greater than zero".into(),
        ));
    }
    if batch_interval_ms == Some(0) {
        return Err(ConnectorError::ConfigurationError(
            "WebSocket client batch.interval.ms must be greater than zero".into(),
        ));
    }

    Ok(SinkMode::Client {
        url,
        reconnect: ReconnectConfig::default(),
        buffer_on_disconnect,
        batch_interval: batch_interval_ms.map(Duration::from_millis),
        batch_max_size,
    })
}

fn parse_sink_format(config: &ConnectorConfig) -> Result<SinkFormat, ConnectorError> {
    match config.get("format").map(str::to_lowercase) {
        Some(ref value) if value == "json" => Ok(SinkFormat::Json),
        Some(ref value) if value == "jsonlines" || value == "json_lines" => {
            Ok(SinkFormat::JsonLines)
        }
        Some(ref value) if value == "arrow_ipc" => Ok(SinkFormat::ArrowIpc),
        Some(ref value) if value == "binary" => Ok(SinkFormat::Binary),
        Some(other) => Err(ConnectorError::ConfigurationError(format!(
            "invalid sink format '{other}': expected json, jsonlines, arrow_ipc, or binary"
        ))),
        None => Ok(SinkFormat::Json),
    }
}

fn parse_auth(config: &ConnectorConfig) -> Result<Option<WsAuthConfig>, ConnectorError> {
    match config.get("auth.type").map(str::to_lowercase).as_deref() {
        None | Some("none") => Ok(None),
        Some("bearer") => Ok(Some(WsAuthConfig::Bearer {
            token: require_non_empty(config, "auth.token")?,
        })),
        Some("basic") => Ok(Some(WsAuthConfig::Basic {
            username: require_non_empty(config, "auth.username")?,
            password: require_non_empty(config, "auth.password")?,
        })),
        Some("hmac") => Ok(Some(WsAuthConfig::Hmac {
            api_key: require_non_empty(config, "auth.api.key")?,
            secret: require_non_empty(config, "auth.secret")?,
        })),
        Some(other) => Err(ConnectorError::ConfigurationError(format!(
            "invalid WebSocket auth.type '{other}': expected none, bearer, basic, or hmac"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

/// Configuration for the WebSocket sink connector.
///
/// Supports two operating modes:
/// - **Server**: binds a local address and fans out records to connected
///   WebSocket clients.
/// - **Client**: connects to a remote WebSocket server and pushes records.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebSocketSinkConfig {
    /// The operating mode (server or client).
    pub mode: SinkMode,
    /// Serialization format for outgoing messages.
    pub format: SinkFormat,
    /// Optional authentication configuration shared with the source connector.
    pub auth: Option<WsAuthConfig>,
}

impl WebSocketSinkConfig {
    /// Builds a [`WebSocketSinkConfig`] from a flat [`ConnectorConfig`] property map.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if a required key is missing
    /// or a value cannot be parsed.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mode = match config
            .get("mode")
            .unwrap_or("server")
            .to_lowercase()
            .as_str()
        {
            "server" => parse_server_mode(config)?,
            "client" => parse_client_mode(config)?,
            other => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid WebSocket sink mode '{other}': expected 'server' or 'client'"
                )));
            }
        };

        Ok(Self {
            mode,
            format: parse_sink_format(config)?,
            auth: parse_auth(config)?,
        })
    }
}

// ---------------------------------------------------------------------------
// SinkMode
// ---------------------------------------------------------------------------

/// Operating mode for the WebSocket sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkMode {
    /// Bind a local address and fan out records to all connected clients.
    Server {
        /// Address to bind (e.g. `"0.0.0.0:8080"`).
        bind_address: String,
        /// Optional URL path filter (e.g. `"/stream"`).
        path: Option<String>,
        /// Maximum number of concurrent client connections.
        #[serde(default = "default_max_connections")]
        max_connections: usize,
        /// Per-client outbound buffer size in bytes (default 256 KB).
        #[serde(default = "default_per_client_buffer")]
        per_client_buffer: usize,
        /// Policy for handling clients whose send buffer is full.
        #[serde(default)]
        slow_client_policy: SlowClientPolicy,
        /// Interval between WebSocket ping frames.
        #[serde(default = "default_ping_interval")]
        ping_interval: Duration,
        /// Timeout waiting for a pong response before disconnecting.
        #[serde(default = "default_ping_timeout")]
        ping_timeout: Duration,
        /// When `true`, clients may send subscription filter messages.
        #[serde(default)]
        enable_subscription_filter: bool,
        /// Optional bounded replay buffer so late-joining clients can
        /// catch up. `None` means no replay.
        replay_buffer_size: Option<usize>,
    },
    /// Connect to an external WebSocket server and push records.
    Client {
        /// WebSocket URL to connect to (e.g. `"wss://host/path"`).
        url: String,
        /// Reconnection strategy when the connection drops.
        #[serde(default)]
        reconnect: ReconnectConfig,
        /// Number of records to buffer in memory while disconnected.
        /// `None` means records are dropped on disconnect.
        buffer_on_disconnect: Option<usize>,
        /// Optional interval to batch outgoing messages.
        batch_interval: Option<Duration>,
        /// Maximum number of records per batch.
        batch_max_size: Option<usize>,
    },
}

impl Default for SinkMode {
    fn default() -> Self {
        Self::Server {
            bind_address: "0.0.0.0:8080".to_string(),
            path: None,
            max_connections: default_max_connections(),
            per_client_buffer: default_per_client_buffer(),
            slow_client_policy: SlowClientPolicy::default(),
            ping_interval: default_ping_interval(),
            ping_timeout: default_ping_timeout(),
            enable_subscription_filter: false,
            replay_buffer_size: None,
        }
    }
}

// ---------------------------------------------------------------------------
// SlowClientPolicy
// ---------------------------------------------------------------------------

/// Policy applied when a client's outbound buffer is full.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum SlowClientPolicy {
    /// Drop the oldest buffered message to make room for the new one.
    #[default]
    DropOldest,
    /// Drop the newest (incoming) message when the buffer is full.
    DropNewest,
    /// Disconnect the client once its buffer reaches the given threshold
    /// percentage.
    Disconnect {
        /// Buffer fullness percentage (0-100) at which the client is
        /// disconnected.
        threshold_pct: u8,
    },
    /// Emit a warning when the buffer reaches `warn_pct`, then disconnect
    /// at `disconnect_pct`.
    WarnThenDisconnect {
        /// Buffer fullness percentage (0-100) at which a warning is emitted.
        warn_pct: u8,
        /// Buffer fullness percentage (0-100) at which the client is
        /// disconnected.
        disconnect_pct: u8,
    },
}

// ---------------------------------------------------------------------------
// SinkFormat
// ---------------------------------------------------------------------------

/// Serialization format for outgoing WebSocket messages.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum SinkFormat {
    /// One JSON object per message.
    #[default]
    Json,
    /// Newline-delimited JSON (one JSON object per line).
    JsonLines,
    /// Arrow IPC (streaming) format.
    ArrowIpc,
    /// Raw binary (application-defined).
    Binary,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Default impls -------------------------------------------------------

    #[test]
    fn test_slow_client_policy_default() {
        let policy = SlowClientPolicy::default();
        assert!(matches!(policy, SlowClientPolicy::DropOldest));
    }

    #[test]
    fn test_sink_format_default() {
        let format = SinkFormat::default();
        assert_eq!(format, SinkFormat::Json);
    }

    #[test]
    fn test_sink_mode_default_is_server() {
        let mode = SinkMode::default();
        match mode {
            SinkMode::Server {
                bind_address,
                path,
                max_connections,
                per_client_buffer,
                slow_client_policy,
                ping_interval,
                ping_timeout,
                enable_subscription_filter,
                replay_buffer_size,
            } => {
                assert_eq!(bind_address, "0.0.0.0:8080");
                assert!(path.is_none());
                assert_eq!(max_connections, 10_000);
                assert_eq!(per_client_buffer, 262_144);
                assert!(matches!(slow_client_policy, SlowClientPolicy::DropOldest));
                assert_eq!(ping_interval, Duration::from_secs(30));
                assert_eq!(ping_timeout, Duration::from_secs(10));
                assert!(!enable_subscription_filter);
                assert!(replay_buffer_size.is_none());
            }
            SinkMode::Client { .. } => panic!("expected Server, got Client"),
        }
    }

    #[test]
    fn test_websocket_sink_config_default() {
        let config = WebSocketSinkConfig::default();
        assert_eq!(config.format, SinkFormat::Json);
        assert!(config.auth.is_none());
        assert!(matches!(config.mode, SinkMode::Server { .. }));
    }

    // -- Serde default helpers -----------------------------------------------

    #[test]
    fn test_default_max_connections() {
        assert_eq!(default_max_connections(), 10_000);
    }

    #[test]
    fn test_default_per_client_buffer() {
        assert_eq!(default_per_client_buffer(), 262_144);
    }

    #[test]
    fn test_default_ping_interval() {
        assert_eq!(default_ping_interval(), Duration::from_secs(30));
    }

    #[test]
    fn test_default_ping_timeout() {
        assert_eq!(default_ping_timeout(), Duration::from_secs(10));
    }

    // -- Serialization round-trips -------------------------------------------

    #[test]
    fn test_server_mode_serde_roundtrip() {
        let mode = SinkMode::Server {
            bind_address: "127.0.0.1:9090".to_string(),
            path: Some("/ws".to_string()),
            max_connections: 500,
            per_client_buffer: 1024,
            slow_client_policy: SlowClientPolicy::DropNewest,
            ping_interval: Duration::from_secs(15),
            ping_timeout: Duration::from_secs(5),
            enable_subscription_filter: true,
            replay_buffer_size: Some(1000),
        };
        let json = serde_json::to_string(&mode).unwrap();
        let deser: SinkMode = serde_json::from_str(&json).unwrap();
        match deser {
            SinkMode::Server {
                bind_address,
                path,
                max_connections,
                per_client_buffer,
                replay_buffer_size,
                enable_subscription_filter,
                ..
            } => {
                assert_eq!(bind_address, "127.0.0.1:9090");
                assert_eq!(path.as_deref(), Some("/ws"));
                assert_eq!(max_connections, 500);
                assert_eq!(per_client_buffer, 1024);
                assert!(enable_subscription_filter);
                assert_eq!(replay_buffer_size, Some(1000));
            }
            SinkMode::Client { .. } => panic!("expected Server"),
        }
    }

    #[test]
    fn test_client_mode_serde_roundtrip() {
        let mode = SinkMode::Client {
            url: "wss://example.com/feed".to_string(),
            reconnect: ReconnectConfig::default(),
            buffer_on_disconnect: Some(5000),
            batch_interval: Some(Duration::from_millis(100)),
            batch_max_size: Some(64),
        };
        let json = serde_json::to_string(&mode).unwrap();
        let deser: SinkMode = serde_json::from_str(&json).unwrap();
        match deser {
            SinkMode::Client {
                url,
                buffer_on_disconnect,
                batch_interval,
                batch_max_size,
                ..
            } => {
                assert_eq!(url, "wss://example.com/feed");
                assert_eq!(buffer_on_disconnect, Some(5000));
                assert_eq!(batch_interval, Some(Duration::from_millis(100)));
                assert_eq!(batch_max_size, Some(64));
            }
            SinkMode::Server { .. } => panic!("expected Client"),
        }
    }

    #[test]
    fn test_sink_format_serde_variants() {
        for (format, expected) in [
            (SinkFormat::Json, "\"Json\""),
            (SinkFormat::JsonLines, "\"JsonLines\""),
            (SinkFormat::ArrowIpc, "\"ArrowIpc\""),
            (SinkFormat::Binary, "\"Binary\""),
        ] {
            let json = serde_json::to_string(&format).unwrap();
            assert_eq!(json, expected);
            let deser: SinkFormat = serde_json::from_str(&json).unwrap();
            assert_eq!(deser, format);
        }
    }

    #[test]
    fn test_slow_client_policy_serde_variants() {
        let disconnect = SlowClientPolicy::Disconnect { threshold_pct: 90 };
        let json = serde_json::to_string(&disconnect).unwrap();
        let deser: SlowClientPolicy = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            deser,
            SlowClientPolicy::Disconnect { threshold_pct: 90 }
        ));

        let warn = SlowClientPolicy::WarnThenDisconnect {
            warn_pct: 75,
            disconnect_pct: 95,
        };
        let json = serde_json::to_string(&warn).unwrap();
        let deser: SlowClientPolicy = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            deser,
            SlowClientPolicy::WarnThenDisconnect {
                warn_pct: 75,
                disconnect_pct: 95,
            }
        ));
    }

    #[test]
    fn test_full_config_serde_roundtrip() {
        let config = WebSocketSinkConfig {
            mode: SinkMode::Server {
                bind_address: "0.0.0.0:3000".to_string(),
                path: None,
                max_connections: 2000,
                per_client_buffer: 131_072,
                slow_client_policy: SlowClientPolicy::WarnThenDisconnect {
                    warn_pct: 80,
                    disconnect_pct: 95,
                },
                ping_interval: Duration::from_secs(20),
                ping_timeout: Duration::from_secs(8),
                enable_subscription_filter: false,
                replay_buffer_size: Some(500),
            },
            format: SinkFormat::ArrowIpc,
            auth: None,
        };

        let json = serde_json::to_string_pretty(&config).unwrap();
        let deser: WebSocketSinkConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.format, SinkFormat::ArrowIpc);
        assert!(deser.auth.is_none());
        match deser.mode {
            SinkMode::Server {
                max_connections,
                replay_buffer_size,
                ..
            } => {
                assert_eq!(max_connections, 2000);
                assert_eq!(replay_buffer_size, Some(500));
            }
            SinkMode::Client { .. } => panic!("expected Server"),
        }
    }

    // -- Serde defaults are applied on deserialization -----------------------

    #[test]
    fn test_server_mode_serde_defaults_applied() {
        // Minimal JSON — only required fields and the tag.
        let json = r#"{
            "type": "Server",
            "bind_address": "0.0.0.0:4000"
        }"#;
        let mode: SinkMode = serde_json::from_str(json).unwrap();
        match mode {
            SinkMode::Server {
                max_connections,
                per_client_buffer,
                slow_client_policy,
                ping_interval,
                ping_timeout,
                enable_subscription_filter,
                replay_buffer_size,
                ..
            } => {
                assert_eq!(max_connections, 10_000);
                assert_eq!(per_client_buffer, 262_144);
                assert!(matches!(slow_client_policy, SlowClientPolicy::DropOldest));
                assert_eq!(ping_interval, Duration::from_secs(30));
                assert_eq!(ping_timeout, Duration::from_secs(10));
                assert!(!enable_subscription_filter);
                assert!(replay_buffer_size.is_none());
            }
            SinkMode::Client { .. } => panic!("expected Server"),
        }
    }

    // -- from_config tests ---------------------------------------------------

    #[test]
    fn test_from_config_server_mode() {
        let mut config = ConnectorConfig::new("websocket");
        config.set("bind.address", "0.0.0.0:3000");
        config.set("max.connections", "2000");
        config.set("format", "jsonlines");

        let cfg = WebSocketSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.format, SinkFormat::JsonLines);
        match cfg.mode {
            SinkMode::Server {
                bind_address,
                max_connections,
                ..
            } => {
                assert_eq!(bind_address, "0.0.0.0:3000");
                assert_eq!(max_connections, 2000);
            }
            SinkMode::Client { .. } => panic!("expected Server mode"),
        }
    }

    #[test]
    fn test_from_config_client_mode() {
        let mut config = ConnectorConfig::new("websocket");
        config.set("mode", "client");
        config.set("url", "wss://upstream.example.com/feed");
        config.set("format", "json");

        let cfg = WebSocketSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.format, SinkFormat::Json);
        match cfg.mode {
            SinkMode::Client { url, .. } => {
                assert_eq!(url, "wss://upstream.example.com/feed");
            }
            SinkMode::Server { .. } => panic!("expected Client mode"),
        }
    }

    #[test]
    fn test_from_config_missing_bind_address_errors() {
        let config = ConnectorConfig::new("websocket");
        // Server mode is default — missing bind.address should error.
        let result = WebSocketSinkConfig::from_config(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("bind.address"));
    }

    #[test]
    fn test_from_config_bearer_auth() {
        let mut config = ConnectorConfig::new("websocket");
        config.set("bind.address", "0.0.0.0:8080");
        config.set("auth.type", "bearer");
        config.set("auth.token", "my-secret");

        let cfg = WebSocketSinkConfig::from_config(&config).unwrap();
        assert!(matches!(
            cfg.auth,
            Some(WsAuthConfig::Bearer { ref token }) if token == "my-secret"
        ));
    }
}
