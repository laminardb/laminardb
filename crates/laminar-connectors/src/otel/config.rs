//! OTel source connector configuration.
//!
//! Parses and validates configuration for the OTLP/gRPC receiver.

use crate::config::{ConfigKeySpec, ConnectorConfig};
use crate::error::ConnectorError;

/// Which OTel signal type to receive.
///
/// Each source handles exactly one signal type. Create separate sources
/// for different signals (each on its own port).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtelSignal {
    /// Trace spans.
    Traces,
    /// Metric data points.
    Metrics,
    /// Log records.
    Logs,
}

impl OtelSignal {
    /// Parse from a string value (case-insensitive).
    fn parse(s: &str) -> Result<Self, ConnectorError> {
        match s.to_lowercase().as_str() {
            "traces" | "trace" => Ok(Self::Traces),
            "metrics" | "metric" => Ok(Self::Metrics),
            "logs" | "log" => Ok(Self::Logs),
            other => Err(ConnectorError::ConfigurationError(format!(
                "unknown OTel signal type '{other}': expected traces, metrics, or logs \
                 (create separate sources for each signal type)"
            ))),
        }
    }
}

/// Configuration for the OTel source connector.
#[derive(Debug, Clone)]
pub struct OtelSourceConfig {
    /// Address to bind the gRPC server to.
    pub bind_address: String,
    /// Port for the gRPC server.
    pub port: u16,
    /// Which signal types to receive.
    pub signals: OtelSignal,
    /// Maximum rows per `RecordBatch`.
    pub batch_size: usize,
    /// Bounded channel capacity (number of batches).
    pub channel_capacity: usize,
}

impl Default for OtelSourceConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".to_string(),
            port: 4317,
            signals: OtelSignal::Traces,
            batch_size: 1024,
            channel_capacity: 64,
        }
    }
}

impl OtelSourceConfig {
    /// Parse configuration from a `ConnectorConfig`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if any value is invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self::default();

        if let Some(addr) = config.get("bind.address").or(config.get("bind_address")) {
            cfg.bind_address = addr.to_string();
        }

        if let Some(port_str) = config.get("port") {
            cfg.port = port_str.parse::<u16>().map_err(|e| {
                ConnectorError::ConfigurationError(format!("invalid port '{port_str}': {e}"))
            })?;
        }

        if let Some(sig) = config.get("signals").or(config.get("signal")) {
            cfg.signals = OtelSignal::parse(sig)?;
        }

        if let Some(bs) = config.get("batch_size").or(config.get("batch.size")) {
            cfg.batch_size = bs.parse::<usize>().map_err(|e| {
                ConnectorError::ConfigurationError(format!("invalid batch_size '{bs}': {e}"))
            })?;
            if cfg.batch_size == 0 {
                return Err(ConnectorError::ConfigurationError(
                    "batch_size must be > 0".into(),
                ));
            }
        }

        if let Some(cc) = config
            .get("channel_capacity")
            .or(config.get("channel.capacity"))
        {
            cfg.channel_capacity = cc.parse::<usize>().map_err(|e| {
                ConnectorError::ConfigurationError(format!("invalid channel_capacity '{cc}': {e}"))
            })?;
            if cfg.channel_capacity == 0 {
                return Err(ConnectorError::ConfigurationError(
                    "channel_capacity must be > 0".into(),
                ));
            }
        }

        Ok(cfg)
    }

    /// Full socket address for binding.
    #[must_use]
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }
}

/// Configuration keys accepted by the OTel source connector.
#[must_use]
pub fn otel_source_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::optional("port", "gRPC listen port", "4317"),
        ConfigKeySpec::optional("bind.address", "Listen address", "0.0.0.0"),
        ConfigKeySpec::optional("signals", "Signal type: traces, metrics, or logs", "traces"),
        ConfigKeySpec::optional("batch_size", "Max rows per RecordBatch", "1024"),
        ConfigKeySpec::optional("channel_capacity", "Batch channel capacity", "64"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = OtelSourceConfig::default();
        assert_eq!(cfg.port, 4317);
        assert_eq!(cfg.bind_address, "0.0.0.0");
        assert_eq!(cfg.signals, OtelSignal::Traces);
        assert_eq!(cfg.batch_size, 1024);
        assert_eq!(cfg.channel_capacity, 64);
    }

    #[test]
    fn test_from_config_all_fields() {
        let config = ConnectorConfig::with_properties(
            "otel",
            [
                ("port".to_string(), "4318".to_string()),
                ("bind.address".to_string(), "127.0.0.1".to_string()),
                ("signals".to_string(), "metrics".to_string()),
                ("batch_size".to_string(), "2048".to_string()),
                ("channel_capacity".to_string(), "128".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        let cfg = OtelSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.port, 4318);
        assert_eq!(cfg.bind_address, "127.0.0.1");
        assert_eq!(cfg.signals, OtelSignal::Metrics);
        assert_eq!(cfg.batch_size, 2048);
        assert_eq!(cfg.channel_capacity, 128);
    }

    #[test]
    fn test_from_config_defaults() {
        let config = ConnectorConfig::new("otel");
        let cfg = OtelSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.port, 4317);
        assert_eq!(cfg.signals, OtelSignal::Traces);
    }

    #[test]
    fn test_invalid_port() {
        let config = ConnectorConfig::with_properties(
            "otel",
            [("port".to_string(), "not_a_number".to_string())]
                .into_iter()
                .collect(),
        );
        assert!(OtelSourceConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_zero_batch_size() {
        let config = ConnectorConfig::with_properties(
            "otel",
            [("batch_size".to_string(), "0".to_string())]
                .into_iter()
                .collect(),
        );
        assert!(OtelSourceConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_invalid_signal() {
        let config = ConnectorConfig::with_properties(
            "otel",
            [("signals".to_string(), "invalid".to_string())]
                .into_iter()
                .collect(),
        );
        assert!(OtelSourceConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_signal_parse() {
        assert_eq!(OtelSignal::parse("traces").unwrap(), OtelSignal::Traces);
        assert_eq!(OtelSignal::parse("TRACE").unwrap(), OtelSignal::Traces);
        assert_eq!(OtelSignal::parse("metrics").unwrap(), OtelSignal::Metrics);
        assert_eq!(OtelSignal::parse("logs").unwrap(), OtelSignal::Logs);
        assert!(OtelSignal::parse("bad").is_err());
    }

    #[test]
    fn test_all_signal_rejected() {
        let err = OtelSignal::parse("all").unwrap_err();
        assert!(err.to_string().contains("separate sources"));
    }

    #[test]
    fn test_socket_addr() {
        let cfg = OtelSourceConfig::default();
        assert_eq!(cfg.socket_addr(), "0.0.0.0:4317");
    }
}
