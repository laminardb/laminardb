//! `PostgreSQL` sink connector configuration.
//!
//! [`PostgresSinkConfig`] encapsulates all settings for writing Arrow
//! `RecordBatch` data to `PostgreSQL`, parsed from SQL `WITH (...)` clauses.

use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

/// Configuration for the `PostgreSQL` sink connector.
///
/// Parsed from SQL `WITH (...)` clause options via [`from_config`](Self::from_config).
#[derive(Debug, Clone)]
pub struct PostgresSinkConfig {
    /// `PostgreSQL` hostname.
    pub hostname: String,

    /// `PostgreSQL` port (default: 5432).
    pub port: u16,

    /// Database name.
    pub database: String,

    /// Username for authentication.
    pub username: String,

    /// Password for authentication.
    pub password: String,

    /// Target schema name (default: `"public"`).
    pub schema_name: String,

    /// Target table name.
    pub table_name: String,

    /// Write mode: append (COPY BINARY) or upsert (ON CONFLICT).
    pub write_mode: WriteMode,

    /// Primary key columns (required for upsert mode).
    pub primary_key_columns: Vec<String>,

    /// Maximum records to buffer before flushing.
    pub batch_size: usize,

    /// Maximum time to buffer before flushing.
    pub flush_interval: Duration,

    /// Connection pool size.
    pub pool_size: usize,

    /// Connection timeout.
    pub connect_timeout: Duration,

    /// SSL mode for connections.
    pub ssl_mode: SslMode,

    /// Whether to create the target table if it doesn't exist.
    pub auto_create_table: bool,

    /// Whether to handle changelog/retraction records.
    pub changelog_mode: bool,

    /// Delivery guarantee level.
    pub delivery_guarantee: DeliveryGuarantee,

    /// Sink ID for offset tracking (auto-generated if empty).
    pub sink_id: String,
}

impl Default for PostgresSinkConfig {
    fn default() -> Self {
        Self {
            hostname: "localhost".to_string(),
            port: 5432,
            database: String::new(),
            username: String::new(),
            password: String::new(),
            schema_name: "public".to_string(),
            table_name: String::new(),
            write_mode: WriteMode::Append,
            primary_key_columns: Vec::new(),
            batch_size: 4096,
            flush_interval: Duration::from_secs(1),
            pool_size: 4,
            connect_timeout: Duration::from_secs(10),
            ssl_mode: SslMode::Prefer,
            auto_create_table: false,
            changelog_mode: false,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            sink_id: String::new(),
        }
    }
}

impl PostgresSinkConfig {
    /// Creates a minimal config for testing.
    #[must_use]
    pub fn new(hostname: &str, database: &str, table_name: &str) -> Self {
        Self {
            hostname: hostname.to_string(),
            database: database.to_string(),
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// Parses a sink config from a [`ConnectorConfig`] (SQL WITH clause).
    ///
    /// # Required keys
    ///
    /// - `hostname` - `PostgreSQL` server hostname
    /// - `database` - Target database name
    /// - `username` - Authentication username
    /// - `table.name` - Target table name
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if required keys are absent,
    /// or `ConnectorError::ConfigurationError` on invalid values.
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self::default();

        cfg.hostname = config.require("hostname")?.to_string();
        cfg.database = config.require("database")?.to_string();
        cfg.username = config.require("username")?.to_string();
        cfg.table_name = config.require("table.name")?.to_string();

        if let Some(v) = config.get("password") {
            cfg.password = v.to_string();
        }
        if let Some(v) = config.get("port") {
            cfg.port = v
                .parse()
                .map_err(|_| ConnectorError::ConfigurationError(format!("invalid port: '{v}'")))?;
        }
        if let Some(v) = config.get("schema.name") {
            cfg.schema_name = v.to_string();
        }
        if let Some(v) = config.get("write.mode") {
            cfg.write_mode = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid write.mode: '{v}' (expected 'append' or 'upsert')"
                ))
            })?;
        }
        if let Some(v) = config.get("primary.key") {
            cfg.primary_key_columns = v.split(',').map(|c| c.trim().to_string()).collect();
        }
        if let Some(v) = config.get("batch.size") {
            cfg.batch_size = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid batch.size: '{v}'"))
            })?;
        }
        if let Some(v) = config.get("flush.interval.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid flush.interval.ms: '{v}'"))
            })?;
            cfg.flush_interval = Duration::from_millis(ms);
        }
        if let Some(v) = config.get("pool.size") {
            cfg.pool_size = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid pool.size: '{v}'"))
            })?;
        }
        if let Some(v) = config.get("connect.timeout.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid connect.timeout.ms: '{v}'"))
            })?;
            cfg.connect_timeout = Duration::from_millis(ms);
        }
        if let Some(v) = config.get("ssl.mode") {
            cfg.ssl_mode = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid ssl.mode: '{v}' (expected disable/prefer/require/verify-ca/verify-full)"
                ))
            })?;
        }
        if let Some(v) = config.get("auto.create.table") {
            cfg.auto_create_table = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("changelog.mode") {
            cfg.changelog_mode = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("delivery.guarantee") {
            cfg.delivery_guarantee = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid delivery.guarantee: '{v}' \
                     (expected 'at_least_once' or 'exactly_once')"
                ))
            })?;
        }
        if let Some(v) = config.get("sink.id") {
            cfg.sink_id = v.to_string();
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the configuration for consistency.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on invalid combinations.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.table_name.is_empty() {
            return Err(ConnectorError::MissingConfig("table.name".into()));
        }
        if self.write_mode == WriteMode::Upsert && self.primary_key_columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "upsert mode requires 'primary.key' to be set".into(),
            ));
        }
        if self.batch_size == 0 {
            return Err(ConnectorError::ConfigurationError(
                "batch.size must be > 0".into(),
            ));
        }
        if self.pool_size == 0 {
            return Err(ConnectorError::ConfigurationError(
                "pool.size must be > 0".into(),
            ));
        }
        Ok(())
    }

    /// Returns the fully qualified table name (`schema.table`).
    #[must_use]
    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }

    /// Returns the effective sink ID, auto-generating from table name if empty.
    #[must_use]
    pub fn effective_sink_id(&self) -> String {
        if self.sink_id.is_empty() {
            format!("laminardb-sink-{}", self.qualified_table_name())
        } else {
            self.sink_id.clone()
        }
    }
}

/// Write mode for the `PostgreSQL` sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Append-only: uses COPY BINARY for maximum throughput.
    /// No deduplication — every record is inserted.
    Append,
    /// Upsert: `INSERT ... ON CONFLICT DO UPDATE`.
    /// Requires primary key columns. Deduplicates on key.
    Upsert,
}

str_enum!(WriteMode, lowercase_nodash, String, "unknown write mode",
    Append => "append", "copy";
    Upsert => "upsert", "insert"
);

/// Delivery guarantee for the `PostgreSQL` sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// At-least-once: records may be duplicated on failure recovery.
    AtLeastOnce,
    /// Exactly-once: co-transactional offset storage in `PostgreSQL`.
    ExactlyOnce,
}

str_enum!(DeliveryGuarantee, lowercase_nodash, String, "unknown delivery guarantee",
    AtLeastOnce => "at_least_once", "at-least-once", "atleastonce";
    ExactlyOnce => "exactly_once", "exactly-once", "exactlyonce"
);

/// SSL mode for `PostgreSQL` connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// No SSL.
    Disable,
    /// Use SSL if available, fall back to unencrypted.
    Prefer,
    /// Require SSL.
    Require,
    /// Require SSL and verify server certificate.
    VerifyCa,
    /// Require SSL, verify certificate and hostname.
    VerifyFull,
}

str_enum!(SslMode, lowercase_nodash, String, "unknown SSL mode",
    Disable => "disable", "off";
    Prefer => "prefer";
    Require => "require";
    VerifyCa => "verify-ca", "verify_ca", "verifyca";
    VerifyFull => "verify-full", "verify_full", "verifyfull"
);

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("postgres-sink");
        for (k, v) in pairs {
            config.set(*k, *v);
        }
        config
    }

    fn required_pairs() -> Vec<(&'static str, &'static str)> {
        vec![
            ("hostname", "localhost"),
            ("database", "mydb"),
            ("username", "writer"),
            ("table.name", "events"),
        ]
    }

    #[test]
    fn test_parse_required_fields() {
        let config = make_config(&required_pairs());
        let cfg = PostgresSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.hostname, "localhost");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.username, "writer");
        assert_eq!(cfg.table_name, "events");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.schema_name, "public");
        assert_eq!(cfg.write_mode, WriteMode::Append);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
    }

    #[test]
    fn test_missing_hostname() {
        let config = make_config(&[("database", "db"), ("username", "u"), ("table.name", "t")]);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_missing_database() {
        let config = make_config(&[("hostname", "h"), ("username", "u"), ("table.name", "t")]);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_missing_username() {
        let config = make_config(&[("hostname", "h"), ("database", "db"), ("table.name", "t")]);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_missing_table_name() {
        let config = make_config(&[("hostname", "h"), ("database", "db"), ("username", "u")]);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_parse_all_optional_fields() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("password", "secret"),
            ("port", "5433"),
            ("schema.name", "analytics"),
            ("write.mode", "upsert"),
            ("primary.key", "id, region"),
            ("batch.size", "8192"),
            ("flush.interval.ms", "500"),
            ("pool.size", "8"),
            ("connect.timeout.ms", "5000"),
            ("ssl.mode", "require"),
            ("auto.create.table", "true"),
            ("changelog.mode", "true"),
            ("delivery.guarantee", "exactly_once"),
            ("sink.id", "my-sink"),
        ]);
        let config = make_config(&pairs);
        let cfg = PostgresSinkConfig::from_config(&config).unwrap();

        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.schema_name, "analytics");
        assert_eq!(cfg.write_mode, WriteMode::Upsert);
        assert_eq!(cfg.primary_key_columns, vec!["id", "region"]);
        assert_eq!(cfg.batch_size, 8192);
        assert_eq!(cfg.flush_interval, Duration::from_millis(500));
        assert_eq!(cfg.pool_size, 8);
        assert_eq!(cfg.connect_timeout, Duration::from_millis(5000));
        assert_eq!(cfg.ssl_mode, SslMode::Require);
        assert!(cfg.auto_create_table);
        assert!(cfg.changelog_mode);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
        assert_eq!(cfg.sink_id, "my-sink");
    }

    #[test]
    fn test_upsert_requires_primary_key() {
        let mut pairs = required_pairs();
        pairs.push(("write.mode", "upsert"));
        let config = make_config(&pairs);
        let result = PostgresSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("primary.key"), "error: {err}");
    }

    #[test]
    fn test_batch_size_zero_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("batch.size", "0"));
        let config = make_config(&pairs);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_pool_size_zero_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("pool.size", "0"));
        let config = make_config(&pairs);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_qualified_table_name() {
        let cfg = PostgresSinkConfig::new("localhost", "db", "events");
        assert_eq!(cfg.qualified_table_name(), "public.events");

        let mut cfg2 = cfg;
        cfg2.schema_name = "analytics".to_string();
        assert_eq!(cfg2.qualified_table_name(), "analytics.events");
    }

    #[test]
    fn test_effective_sink_id() {
        let cfg = PostgresSinkConfig::new("localhost", "db", "events");
        assert_eq!(cfg.effective_sink_id(), "laminardb-sink-public.events");

        let mut cfg2 = cfg;
        cfg2.sink_id = "custom-sink".to_string();
        assert_eq!(cfg2.effective_sink_id(), "custom-sink");
    }

    #[test]
    fn test_defaults() {
        let cfg = PostgresSinkConfig::default();
        assert_eq!(cfg.hostname, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.schema_name, "public");
        assert_eq!(cfg.write_mode, WriteMode::Append);
        assert_eq!(cfg.batch_size, 4096);
        assert_eq!(cfg.pool_size, 4);
        assert_eq!(cfg.ssl_mode, SslMode::Prefer);
        assert!(!cfg.auto_create_table);
        assert!(!cfg.changelog_mode);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
    }

    #[test]
    fn test_write_mode_parse() {
        assert_eq!("append".parse::<WriteMode>().unwrap(), WriteMode::Append);
        assert_eq!("copy".parse::<WriteMode>().unwrap(), WriteMode::Append);
        assert_eq!("upsert".parse::<WriteMode>().unwrap(), WriteMode::Upsert);
        assert_eq!("insert".parse::<WriteMode>().unwrap(), WriteMode::Upsert);
        assert!("unknown".parse::<WriteMode>().is_err());
    }

    #[test]
    fn test_write_mode_display() {
        assert_eq!(WriteMode::Append.to_string(), "append");
        assert_eq!(WriteMode::Upsert.to_string(), "upsert");
    }

    #[test]
    fn test_delivery_guarantee_parse() {
        assert_eq!(
            "at_least_once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            "at-least-once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            "exactly_once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::ExactlyOnce
        );
        assert!("unknown".parse::<DeliveryGuarantee>().is_err());
    }

    #[test]
    fn test_delivery_guarantee_display() {
        assert_eq!(DeliveryGuarantee::AtLeastOnce.to_string(), "at_least_once");
        assert_eq!(DeliveryGuarantee::ExactlyOnce.to_string(), "exactly_once");
    }

    #[test]
    fn test_ssl_mode_parse() {
        assert_eq!("disable".parse::<SslMode>().unwrap(), SslMode::Disable);
        assert_eq!("prefer".parse::<SslMode>().unwrap(), SslMode::Prefer);
        assert_eq!("require".parse::<SslMode>().unwrap(), SslMode::Require);
        assert_eq!("verify-ca".parse::<SslMode>().unwrap(), SslMode::VerifyCa);
        assert_eq!(
            "verify-full".parse::<SslMode>().unwrap(),
            SslMode::VerifyFull
        );
        assert!("unknown".parse::<SslMode>().is_err());
    }

    #[test]
    fn test_ssl_mode_display() {
        assert_eq!(SslMode::Disable.to_string(), "disable");
        assert_eq!(SslMode::Prefer.to_string(), "prefer");
        assert_eq!(SslMode::Require.to_string(), "require");
        assert_eq!(SslMode::VerifyCa.to_string(), "verify-ca");
        assert_eq!(SslMode::VerifyFull.to_string(), "verify-full");
    }

    #[test]
    fn test_invalid_port() {
        let mut pairs = required_pairs();
        pairs.push(("port", "not_a_number"));
        let config = make_config(&pairs);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_invalid_batch_size() {
        let mut pairs = required_pairs();
        pairs.push(("batch.size", "abc"));
        let config = make_config(&pairs);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }
}
