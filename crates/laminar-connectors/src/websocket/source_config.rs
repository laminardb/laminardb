//! WebSocket source connector configuration.
//!
//! Provides [`WebSocketSourceConfig`] for configuring a WebSocket source
//! connector in either client mode (connecting to an upstream server) or
//! server mode (accepting incoming connections). Includes reconnection,
//! authentication, message format, and event-time extraction options.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::backpressure::BackpressureStrategy;

// ---------------------------------------------------------------------------
// Serde helper: Duration as milliseconds
// ---------------------------------------------------------------------------

/// Serde helper that encodes a [`Duration`] as a `u64` millisecond count.
mod duration_millis {
    use std::time::Duration;

    use serde::{self, Deserialize, Deserializer, Serializer};

    #[allow(clippy::cast_possible_truncation)]
    pub fn serialize<S>(d: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(d.as_millis() as u64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

// ---------------------------------------------------------------------------
// Default helpers
// ---------------------------------------------------------------------------

/// Default backpressure strategy: block the WebSocket read loop.
fn default_backpressure() -> BackpressureStrategy {
    BackpressureStrategy::Block
}

/// Default maximum message size: 64 MiB.
const fn default_max_message_size() -> usize {
    64 * 1024 * 1024
}

/// Default ping interval for client mode: 30 seconds.
const fn default_ping_interval() -> Duration {
    Duration::from_secs(30)
}

/// Default ping timeout for client mode: 10 seconds.
const fn default_ping_timeout() -> Duration {
    Duration::from_secs(10)
}

/// Default maximum concurrent connections for server mode: 1024.
const fn default_max_connections() -> usize {
    1024
}

/// Default reconnect initial delay: 100 ms.
const fn default_initial_delay() -> Duration {
    Duration::from_millis(100)
}

/// Default reconnect maximum delay: 30 seconds.
const fn default_max_delay() -> Duration {
    Duration::from_secs(30)
}

/// Default exponential backoff multiplier.
const fn default_backoff_multiplier() -> f64 {
    2.0
}

/// Returns `true` (used for `#[serde(default)]` on boolean fields).
const fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

/// WebSocket source connector configuration.
///
/// Supports two operating modes:
/// - **Client**: connects to one or more upstream WebSocket servers and
///   optionally sends a subscribe message after the handshake.
/// - **Server**: binds a local address and accepts incoming WebSocket
///   connections (e.g., from `IoT` devices or browser clients).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketSourceConfig {
    /// Operating mode (client or server).
    pub mode: SourceMode,

    /// Message format used for deserialization.
    pub format: MessageFormat,

    /// Backpressure strategy when the Ring 0 channel is full.
    #[serde(default = "default_backpressure")]
    pub on_backpressure: BackpressureStrategy,

    /// JSON field path used to extract event time from each message.
    ///
    /// When `None`, processing time is used as the event timestamp.
    pub event_time_field: Option<String>,

    /// Format of the event time value extracted from `event_time_field`.
    pub event_time_format: Option<EventTimeFormat>,

    /// Maximum accepted WebSocket message size in bytes.
    ///
    /// Messages exceeding this limit are rejected. Defaults to 64 MiB.
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// Optional authentication configuration for the WebSocket connection.
    pub auth: Option<WsAuthConfig>,
}

impl Default for WebSocketSourceConfig {
    fn default() -> Self {
        Self {
            mode: SourceMode::default(),
            format: MessageFormat::default(),
            on_backpressure: default_backpressure(),
            event_time_field: None,
            event_time_format: None,
            max_message_size: default_max_message_size(),
            auth: None,
        }
    }
}

// ---------------------------------------------------------------------------
// SourceMode
// ---------------------------------------------------------------------------

/// Operating mode for the WebSocket source connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceMode {
    /// Client mode: connect to upstream WebSocket server(s).
    Client {
        /// One or more WebSocket URLs to connect to (e.g., `wss://feed.example.com/v1`).
        urls: Vec<String>,

        /// Optional message to send after the WebSocket handshake completes
        /// (e.g., a JSON subscribe payload).
        subscribe_message: Option<String>,

        /// Reconnection policy applied when the connection drops.
        #[serde(default)]
        reconnect: ReconnectConfig,

        /// Interval between WebSocket ping frames.
        #[serde(default = "default_ping_interval", with = "duration_millis")]
        ping_interval: Duration,

        /// Time to wait for a pong reply before considering the connection dead.
        #[serde(default = "default_ping_timeout", with = "duration_millis")]
        ping_timeout: Duration,
    },

    /// Server mode: listen for incoming WebSocket connections.
    Server {
        /// Socket address to bind (e.g., `0.0.0.0:9443`).
        bind_address: String,

        /// Maximum number of concurrent WebSocket connections.
        #[serde(default = "default_max_connections")]
        max_connections: usize,

        /// Optional URL path to accept connections on (e.g., `/ingest`).
        ///
        /// When `None`, connections are accepted on any path.
        path: Option<String>,
    },
}

impl Default for SourceMode {
    fn default() -> Self {
        Self::Client {
            urls: vec![String::new()],
            subscribe_message: None,
            reconnect: ReconnectConfig::default(),
            ping_interval: default_ping_interval(),
            ping_timeout: default_ping_timeout(),
        }
    }
}

// ---------------------------------------------------------------------------
// MessageFormat
// ---------------------------------------------------------------------------

/// Deserialization format for incoming WebSocket messages.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum MessageFormat {
    /// Each message is a single JSON object.
    #[default]
    Json,

    /// Each message contains one or more newline-delimited JSON objects.
    JsonLines,

    /// Raw binary payload (passed through as-is).
    Binary,

    /// CSV-formatted payload.
    Csv {
        /// Field delimiter character (defaults to `,`).
        delimiter: char,
        /// Whether the first row is a header row.
        has_header: bool,
    },
}

// ---------------------------------------------------------------------------
// EventTimeFormat
// ---------------------------------------------------------------------------

/// Format of the event timestamp extracted from messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventTimeFormat {
    /// Milliseconds since the Unix epoch.
    EpochMillis,

    /// Microseconds since the Unix epoch.
    EpochMicros,

    /// Nanoseconds since the Unix epoch.
    EpochNanos,

    /// Seconds since the Unix epoch (integer or floating-point).
    EpochSeconds,

    /// ISO 8601 datetime string (e.g., `2026-02-21T12:00:00Z`).
    Iso8601,

    /// Custom `strftime`-compatible format string.
    Custom(String),
}

// ---------------------------------------------------------------------------
// ReconnectConfig
// ---------------------------------------------------------------------------

/// Exponential-backoff reconnection policy for client mode.
///
/// When the WebSocket connection is lost, the connector will attempt to
/// reconnect with exponentially increasing delays between attempts,
/// optionally capped at `max_retries`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectConfig {
    /// Whether automatic reconnection is enabled.
    pub enabled: bool,

    /// Initial delay before the first reconnection attempt.
    #[serde(default = "default_initial_delay", with = "duration_millis")]
    pub initial_delay: Duration,

    /// Maximum delay between reconnection attempts.
    #[serde(default = "default_max_delay", with = "duration_millis")]
    pub max_delay: Duration,

    /// Multiplier applied to the delay after each failed attempt.
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Optional upper bound on reconnection attempts.
    ///
    /// `None` means retry indefinitely.
    pub max_retries: Option<u32>,

    /// Whether to apply random jitter to backoff delays to avoid thundering-herd.
    #[serde(default = "default_true")]
    pub jitter: bool,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay: default_initial_delay(),
            max_delay: default_max_delay(),
            backoff_multiplier: default_backoff_multiplier(),
            max_retries: None,
            jitter: true,
        }
    }
}

// ---------------------------------------------------------------------------
// WsAuthConfig
// ---------------------------------------------------------------------------

/// Authentication configuration for WebSocket connections.
///
/// Applied during the HTTP upgrade handshake as headers, query parameters,
/// or used to compute a signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WsAuthConfig {
    /// Bearer token authentication (sent as `Authorization: Bearer <token>`).
    Bearer {
        /// The bearer token value.
        token: String,
    },

    /// HTTP Basic authentication (sent as `Authorization: Basic <base64>`).
    Basic {
        /// Username for basic auth.
        username: String,
        /// Password for basic auth.
        password: String,
    },

    /// Arbitrary HTTP headers added to the upgrade request.
    Headers {
        /// Key-value pairs added as HTTP headers.
        headers: Vec<(String, String)>,
    },

    /// Single query parameter appended to the WebSocket URL.
    QueryParam {
        /// Query parameter name.
        key: String,
        /// Query parameter value.
        value: String,
    },

    /// HMAC signature authentication (e.g., for exchange APIs).
    Hmac {
        /// API key (sent as a header or query parameter).
        api_key: String,
        /// HMAC secret used to sign requests.
        secret: String,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Default impls -------------------------------------------------------

    #[test]
    fn test_default_websocket_source_config() {
        let cfg = WebSocketSourceConfig::default();

        assert!(matches!(cfg.mode, SourceMode::Client { .. }));
        assert!(matches!(cfg.format, MessageFormat::Json));
        assert!(matches!(cfg.on_backpressure, BackpressureStrategy::Block));
        assert_eq!(cfg.max_message_size, 64 * 1024 * 1024);
        assert!(cfg.event_time_field.is_none());
        assert!(cfg.event_time_format.is_none());
        assert!(cfg.auth.is_none());
    }

    #[test]
    fn test_default_source_mode() {
        let mode = SourceMode::default();
        match mode {
            SourceMode::Client {
                urls,
                subscribe_message,
                reconnect,
                ping_interval,
                ping_timeout,
            } => {
                assert_eq!(urls.len(), 1);
                assert_eq!(urls[0], "");
                assert!(subscribe_message.is_none());
                assert!(reconnect.enabled);
                assert_eq!(ping_interval, Duration::from_secs(30));
                assert_eq!(ping_timeout, Duration::from_secs(10));
            }
            SourceMode::Server { .. } => panic!("expected Client mode"),
        }
    }

    #[test]
    fn test_default_message_format() {
        let fmt = MessageFormat::default();
        assert!(matches!(fmt, MessageFormat::Json));
    }

    #[test]
    fn test_default_reconnect_config() {
        let rc = ReconnectConfig::default();
        assert!(rc.enabled);
        assert_eq!(rc.initial_delay, Duration::from_millis(100));
        assert_eq!(rc.max_delay, Duration::from_secs(30));
        assert!((rc.backoff_multiplier - 2.0).abs() < f64::EPSILON);
        assert!(rc.max_retries.is_none());
        assert!(rc.jitter);
    }

    // -- Serde round-trip -----------------------------------------------------

    #[test]
    fn test_serde_round_trip_client_mode() {
        let cfg = WebSocketSourceConfig {
            mode: SourceMode::Client {
                urls: vec!["wss://feed.example.com/v1".into()],
                subscribe_message: Some(r#"{"op":"subscribe","channel":"trades"}"#.into()),
                reconnect: ReconnectConfig::default(),
                ping_interval: Duration::from_secs(15),
                ping_timeout: Duration::from_secs(5),
            },
            format: MessageFormat::Json,
            on_backpressure: BackpressureStrategy::Block,
            event_time_field: Some("timestamp".into()),
            event_time_format: Some(EventTimeFormat::EpochMillis),
            max_message_size: 1024 * 1024,
            auth: Some(WsAuthConfig::Bearer {
                token: "tok_abc123".into(),
            }),
        };

        let json = serde_json::to_string_pretty(&cfg).expect("serialize");
        let deser: WebSocketSourceConfig = serde_json::from_str(&json).expect("deserialize");

        // Verify key fields survived the round-trip.
        match &deser.mode {
            SourceMode::Client {
                urls,
                subscribe_message,
                ping_interval,
                ping_timeout,
                ..
            } => {
                assert_eq!(urls, &["wss://feed.example.com/v1"]);
                assert_eq!(
                    subscribe_message.as_deref(),
                    Some(r#"{"op":"subscribe","channel":"trades"}"#)
                );
                assert_eq!(*ping_interval, Duration::from_secs(15));
                assert_eq!(*ping_timeout, Duration::from_secs(5));
            }
            SourceMode::Server { .. } => panic!("expected Client"),
        }
        assert_eq!(deser.event_time_field.as_deref(), Some("timestamp"));
        assert!(matches!(
            deser.event_time_format,
            Some(EventTimeFormat::EpochMillis)
        ));
        assert_eq!(deser.max_message_size, 1024 * 1024);
        assert!(matches!(
            deser.auth,
            Some(WsAuthConfig::Bearer { ref token }) if token == "tok_abc123"
        ));
    }

    #[test]
    fn test_serde_round_trip_server_mode() {
        let cfg = WebSocketSourceConfig {
            mode: SourceMode::Server {
                bind_address: "0.0.0.0:9443".into(),
                max_connections: 512,
                path: Some("/ingest".into()),
            },
            format: MessageFormat::JsonLines,
            on_backpressure: BackpressureStrategy::Block,
            event_time_field: None,
            event_time_format: None,
            max_message_size: default_max_message_size(),
            auth: None,
        };

        let json = serde_json::to_string(&cfg).expect("serialize");
        let deser: WebSocketSourceConfig = serde_json::from_str(&json).expect("deserialize");

        match &deser.mode {
            SourceMode::Server {
                bind_address,
                max_connections,
                path,
            } => {
                assert_eq!(bind_address, "0.0.0.0:9443");
                assert_eq!(*max_connections, 512);
                assert_eq!(path.as_deref(), Some("/ingest"));
            }
            SourceMode::Client { .. } => panic!("expected Server"),
        }
    }

    #[test]
    fn test_serde_round_trip_reconnect_config() {
        let rc = ReconnectConfig {
            enabled: false,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 1.5,
            max_retries: Some(10),
            jitter: false,
        };

        let json = serde_json::to_string(&rc).expect("serialize");
        let deser: ReconnectConfig = serde_json::from_str(&json).expect("deserialize");

        assert!(!deser.enabled);
        assert_eq!(deser.initial_delay, Duration::from_millis(500));
        assert_eq!(deser.max_delay, Duration::from_secs(60));
        assert!((deser.backoff_multiplier - 1.5).abs() < f64::EPSILON);
        assert_eq!(deser.max_retries, Some(10));
        assert!(!deser.jitter);
    }

    #[test]
    fn test_serde_round_trip_csv_format() {
        let cfg = WebSocketSourceConfig {
            format: MessageFormat::Csv {
                delimiter: '|',
                has_header: true,
            },
            ..WebSocketSourceConfig::default()
        };

        let json = serde_json::to_string(&cfg).expect("serialize");
        let deser: WebSocketSourceConfig = serde_json::from_str(&json).expect("deserialize");

        match deser.format {
            MessageFormat::Csv {
                delimiter,
                has_header,
            } => {
                assert_eq!(delimiter, '|');
                assert!(has_header);
            }
            _ => panic!("expected Csv format"),
        }
    }

    #[test]
    fn test_serde_round_trip_auth_variants() {
        // Basic auth
        let basic = WsAuthConfig::Basic {
            username: "user".into(),
            password: "pass".into(),
        };
        let json = serde_json::to_string(&basic).expect("serialize");
        let deser: WsAuthConfig = serde_json::from_str(&json).expect("deserialize");
        assert!(matches!(
            deser,
            WsAuthConfig::Basic { ref username, ref password }
                if username == "user" && password == "pass"
        ));

        // Headers auth
        let headers = WsAuthConfig::Headers {
            headers: vec![("X-Api-Key".into(), "key123".into())],
        };
        let json = serde_json::to_string(&headers).expect("serialize");
        let deser: WsAuthConfig = serde_json::from_str(&json).expect("deserialize");
        assert!(matches!(deser, WsAuthConfig::Headers { ref headers } if headers.len() == 1));

        // QueryParam auth
        let qp = WsAuthConfig::QueryParam {
            key: "token".into(),
            value: "abc".into(),
        };
        let json = serde_json::to_string(&qp).expect("serialize");
        let deser: WsAuthConfig = serde_json::from_str(&json).expect("deserialize");
        assert!(matches!(
            deser,
            WsAuthConfig::QueryParam { ref key, ref value }
                if key == "token" && value == "abc"
        ));

        // Hmac auth
        let hmac = WsAuthConfig::Hmac {
            api_key: "ak".into(),
            secret: "sk".into(),
        };
        let json = serde_json::to_string(&hmac).expect("serialize");
        let deser: WsAuthConfig = serde_json::from_str(&json).expect("deserialize");
        assert!(matches!(
            deser,
            WsAuthConfig::Hmac { ref api_key, ref secret }
                if api_key == "ak" && secret == "sk"
        ));
    }

    #[test]
    fn test_serde_round_trip_event_time_formats() {
        let formats = vec![
            EventTimeFormat::EpochMillis,
            EventTimeFormat::EpochMicros,
            EventTimeFormat::EpochNanos,
            EventTimeFormat::EpochSeconds,
            EventTimeFormat::Iso8601,
            EventTimeFormat::Custom("%Y-%m-%dT%H:%M:%S".into()),
        ];

        for fmt in formats {
            let json = serde_json::to_string(&fmt).expect("serialize");
            let deser: EventTimeFormat = serde_json::from_str(&json).expect("deserialize");

            // Verify variant is preserved.
            match (&fmt, &deser) {
                (EventTimeFormat::EpochMillis, EventTimeFormat::EpochMillis)
                | (EventTimeFormat::EpochMicros, EventTimeFormat::EpochMicros)
                | (EventTimeFormat::EpochNanos, EventTimeFormat::EpochNanos)
                | (EventTimeFormat::EpochSeconds, EventTimeFormat::EpochSeconds)
                | (EventTimeFormat::Iso8601, EventTimeFormat::Iso8601) => {}
                (EventTimeFormat::Custom(a), EventTimeFormat::Custom(b)) => {
                    assert_eq!(a, b);
                }
                _ => panic!("event time format mismatch after round-trip"),
            }
        }
    }

    #[test]
    fn test_serde_defaults_applied() {
        // Minimal JSON: only required fields are set, all defaulted fields omitted.
        let json = r#"{
            "mode": {
                "type": "Server",
                "bind_address": "127.0.0.1:8080"
            },
            "format": "Json"
        }"#;

        let cfg: WebSocketSourceConfig =
            serde_json::from_str(json).expect("deserialize with defaults");

        assert!(matches!(cfg.on_backpressure, BackpressureStrategy::Block));
        assert_eq!(cfg.max_message_size, 64 * 1024 * 1024);
        assert!(cfg.event_time_field.is_none());
        assert!(cfg.auth.is_none());

        match cfg.mode {
            SourceMode::Server {
                max_connections, ..
            } => {
                assert_eq!(max_connections, 1024);
            }
            SourceMode::Client { .. } => panic!("expected Server"),
        }
    }

    // -- Default helper functions -------------------------------------------

    #[test]
    fn test_default_helper_values() {
        assert_eq!(default_max_message_size(), 64 * 1024 * 1024);
        assert_eq!(default_ping_interval(), Duration::from_secs(30));
        assert_eq!(default_ping_timeout(), Duration::from_secs(10));
        assert_eq!(default_max_connections(), 1024);
        assert_eq!(default_initial_delay(), Duration::from_millis(100));
        assert_eq!(default_max_delay(), Duration::from_secs(30));
        assert!((default_backoff_multiplier() - 2.0).abs() < f64::EPSILON);
        assert!(default_true());
    }
}
