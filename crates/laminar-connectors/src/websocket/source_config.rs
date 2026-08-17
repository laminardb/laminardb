//! WebSocket client-source configuration.

use std::time::Duration;

use super::backpressure::WsBackpressure;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

pub(super) const INGRESS_BUFFER_BYTES: usize = 64 * 1024 * 1024;

const SOURCE_OPTIONS: &[&str] = &[
    "_arrow_schema",
    "format",
    "json.explode",
    "json.path",
    "laminar.source.name",
    "max.message.size",
    "nested.as.jsonb",
    "on.backpressure",
    "reconnect.enabled",
    "reconnect.initial.delay.ms",
    "reconnect.max.delay.ms",
    "reconnect.max.retries",
    "schema.enforcement",
    "subscribe.message",
    "url",
];

const fn default_max_message_size() -> usize {
    64 * 1024 * 1024
}

const fn default_initial_delay() -> Duration {
    Duration::from_millis(100)
}

const fn default_max_delay() -> Duration {
    Duration::from_secs(30)
}

/// WebSocket client-source configuration.
#[derive(Debug, Clone)]
pub struct WebSocketSourceConfig {
    /// Upstream URLs in failover order.
    pub urls: Vec<String>,
    /// Optional message sent after each successful handshake.
    pub subscribe_message: Option<String>,
    /// Reconnection and failover policy.
    pub reconnect: ReconnectConfig,
    /// Incoming message representation.
    pub format: MessageFormat,
    /// Behavior when the bounded ingress channel is full.
    pub on_backpressure: WsBackpressure,
    /// Maximum accepted WebSocket message size in bytes.
    pub max_message_size: usize,
}

impl Default for WebSocketSourceConfig {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            subscribe_message: None,
            reconnect: ReconnectConfig::default(),
            format: MessageFormat::default(),
            on_backpressure: WsBackpressure::default(),
            max_message_size: default_max_message_size(),
        }
    }
}

impl WebSocketSourceConfig {
    /// Builds source configuration from a SQL/programmatic connector map.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for missing or invalid properties and for
    /// removed properties that previously had no production runtime behavior.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        reject_removed_source_options(config)?;
        reject_unknown_source_options(config)?;

        let urls = parse_urls(config.require("url")?)?;
        let format = match config.get("format").map(str::to_ascii_lowercase).as_deref() {
            None | Some("json") => MessageFormat::Json,
            Some("csv") => MessageFormat::Csv {
                delimiter: ',',
                has_header: false,
            },
            Some("binary") => MessageFormat::Binary,
            Some(other) => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid WebSocket source format '{other}': expected json, csv, or binary"
                )));
            }
        };
        reject_json_options_for_non_json(config, &format)?;

        let on_backpressure = match config
            .get("on.backpressure")
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("block") => WsBackpressure::Block,
            Some("drop_newest") => WsBackpressure::DropNewest,
            Some(other) => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid WebSocket source on.backpressure '{other}': expected block or drop_newest"
                )));
            }
        };

        let max_message_size = config
            .get_parsed("max.message.size")?
            .unwrap_or(default_max_message_size());

        let reconnect = ReconnectConfig {
            enabled: config.get_parsed("reconnect.enabled")?.unwrap_or(true),
            initial_delay: Duration::from_millis(
                config
                    .get_parsed("reconnect.initial.delay.ms")?
                    .unwrap_or(100),
            ),
            max_delay: Duration::from_millis(
                config
                    .get_parsed("reconnect.max.delay.ms")?
                    .unwrap_or(30_000),
            ),
            max_retries: config.get_parsed("reconnect.max.retries")?,
        };
        let parsed = Self {
            urls,
            subscribe_message: config
                .get("subscribe.message")
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            reconnect,
            format,
            on_backpressure,
            max_message_size,
        };
        parsed.validate()?;
        Ok(parsed)
    }

    pub(super) fn validate(&self) -> Result<(), ConnectorError> {
        validate_urls(&self.urls)?;
        if self.max_message_size == 0 || self.max_message_size > INGRESS_BUFFER_BYTES {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket source max.message.size must be between 1 and {INGRESS_BUFFER_BYTES} bytes"
            )));
        }
        self.reconnect.validate("source")?;
        Ok(())
    }
}

impl ReconnectConfig {
    pub(super) fn validate(&self, owner: &str) -> Result<(), ConnectorError> {
        if self.initial_delay.is_zero() || self.max_delay.is_zero() {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket {owner} reconnect delays must be greater than zero"
            )));
        }
        if self.initial_delay > self.max_delay {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket {owner} reconnect.initial.delay.ms must not exceed reconnect.max.delay.ms"
            )));
        }
        Ok(())
    }
}

fn parse_urls(value: &str) -> Result<Vec<String>, ConnectorError> {
    let urls: Vec<String> = value
        .split(',')
        .map(str::trim)
        .map(ToString::to_string)
        .collect();
    validate_urls(&urls)?;
    Ok(urls)
}

fn validate_urls(urls: &[String]) -> Result<(), ConnectorError> {
    if urls.is_empty() || urls.iter().any(String::is_empty) {
        return Err(ConnectorError::ConfigurationError(
            "WebSocket source url must contain one or more non-empty URLs".into(),
        ));
    }
    for value in urls {
        let safe_value = crate::security::sanitize_identity_value("url", value);
        let parsed = url::Url::parse(value).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "invalid WebSocket source URL '{safe_value}': {error}"
            ))
        })?;
        if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host().is_none() {
            return Err(ConnectorError::ConfigurationError(format!(
                "invalid WebSocket source URL '{safe_value}': expected ws:// or wss:// with a host"
            )));
        }
    }
    Ok(())
}

fn reject_removed_source_options(config: &ConnectorConfig) -> Result<(), ConnectorError> {
    for key in ["event.time.field", "event.time.format"] {
        if config.get(key).is_some() {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket source option '{key}' was removed; decode the value with json.column.<column>[.epoch_unit] and declare event-time policy with SQL WATERMARK FOR"
            )));
        }
    }
    for key in ["mode", "bind.address", "max.connections", "path"] {
        if config.get(key).is_some() {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket source option '{key}' was removed; WebSocket sources operate only as clients"
            )));
        }
    }
    for key in ["ping.interval.ms", "ping.timeout.ms"] {
        if config.get(key).is_some() {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket source option '{key}' is not supported"
            )));
        }
    }
    for key in [
        "auth.type",
        "auth.token",
        "auth.username",
        "auth.password",
        "auth.api.key",
        "auth.secret",
    ] {
        if config.get(key).is_some() {
            return Err(ConnectorError::ConfigurationError(format!(
                "WebSocket source option '{key}' is not supported"
            )));
        }
    }
    Ok(())
}

fn reject_unknown_source_options(config: &ConnectorConfig) -> Result<(), ConnectorError> {
    let mut unknown: Vec<&str> = config
        .properties()
        .keys()
        .map(String::as_str)
        .filter(|key| !SOURCE_OPTIONS.contains(key) && !is_json_column_option(key))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }

    unknown.sort_unstable();
    Err(ConnectorError::ConfigurationError(format!(
        "unknown WebSocket source propert{}: {}",
        if unknown.len() == 1 { "y" } else { "ies" },
        unknown.join(", ")
    )))
}

fn reject_json_options_for_non_json(
    config: &ConnectorConfig,
    format: &MessageFormat,
) -> Result<(), ConnectorError> {
    if matches!(format, MessageFormat::Json) {
        return Ok(());
    }
    let mut invalid = config
        .properties()
        .keys()
        .filter(|key| {
            matches!(
                key.as_str(),
                "json.path" | "json.explode" | "schema.enforcement" | "nested.as.jsonb"
            ) || key.starts_with("json.column.")
        })
        .map(String::as_str)
        .collect::<Vec<_>>();
    if invalid.is_empty() {
        return Ok(());
    }
    invalid.sort_unstable();
    Err(ConnectorError::ConfigurationError(format!(
        "WebSocket source JSON option{} {} require format = 'json'",
        if invalid.len() == 1 { "" } else { "s" },
        invalid.join(", ")
    )))
}

fn is_json_column_option(key: &str) -> bool {
    let Some(column) = key.strip_prefix("json.column.") else {
        return false;
    };
    if column.is_empty() {
        return false;
    }

    match column.rsplit_once('.') {
        None => true,
        Some((column, "epoch_unit")) => !column.is_empty() && !column.contains('.'),
        Some(_) => false,
    }
}

/// Incoming WebSocket message representation.
#[derive(Debug, Clone, Default)]
pub enum MessageFormat {
    /// One JSON object per WebSocket message.
    #[default]
    Json,
    /// Raw binary payload.
    Binary,
    /// One CSV record per message.
    Csv {
        /// Field delimiter.
        delimiter: char,
        /// Whether each message includes a header row.
        has_header: bool,
    },
}

/// Exponential-backoff reconnection policy.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Whether automatic reconnection is enabled.
    pub enabled: bool,
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Maximum retry delay.
    pub max_delay: Duration,
    /// Maximum retry attempts, or unlimited when absent.
    pub max_retries: Option<u32>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay: default_initial_delay(),
            max_delay: default_max_delay(),
            max_retries: None,
        }
    }
}

#[cfg(test)]
mod tests;
