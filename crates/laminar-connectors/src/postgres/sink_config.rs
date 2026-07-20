//! `PostgreSQL` sink connector configuration.
//!
//! [`PostgresSinkConfig`] encapsulates all settings for writing Arrow
//! `RecordBatch` data to `PostgreSQL`, parsed from SQL `WITH (...)` clauses.

use std::path::PathBuf;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::postgres::SslMode;

const MAX_POSTGRES_IDENTIFIER_BYTES: usize = 63;
const MAX_POSTGRES_STATEMENT_TIMEOUT_MS: u128 = 2_147_483_647;

const REMOVED_CONFIG_KEYS: &[&str] = &["batch.size", "pool.size"];

const ALLOWED_CONFIG_KEYS: &[&str] = &[
    "_arrow_schema",
    "auto.create.table",
    "changelog.mode",
    "connect.timeout.ms",
    "database",
    "flush.interval.ms",
    "hostname",
    "password",
    "port",
    "primary.key",
    "schema.name",
    "ssl.ca.cert.path",
    "ssl.mode",
    "statement.timeout.ms",
    "table.name",
    "username",
    "write.mode",
];

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

    /// Maximum time to buffer before flushing.
    pub flush_interval: Duration,

    /// Connection timeout.
    pub connect_timeout: Duration,

    /// SSL mode for connections.
    pub ssl_mode: SslMode,

    /// Optional PEM file containing trusted CA certificates.
    pub ssl_ca_cert_path: Option<PathBuf>,

    /// Whether to create the target table if it doesn't exist.
    pub auto_create_table: bool,

    /// Whether to handle changelog/retraction records.
    pub changelog_mode: bool,

    /// Per-query statement timeout (default: 30s).
    pub statement_timeout: Duration,
}

impl Default for PostgresSinkConfig {
    fn default() -> Self {
        Self {
            hostname: "localhost".to_string(),
            port: 5432,
            database: String::new(),
            username: "postgres".to_string(),
            password: String::new(),
            schema_name: "public".to_string(),
            table_name: String::new(),
            write_mode: WriteMode::Append,
            primary_key_columns: Vec::new(),
            flush_interval: Duration::from_millis(250),
            connect_timeout: Duration::from_secs(10),
            ssl_mode: SslMode::VerifyFull,
            ssl_ca_cert_path: None,
            auto_create_table: false,
            changelog_mode: false,
            statement_timeout: Duration::from_secs(30),
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
        Self::reject_removed_keys(config)?;
        config.reject_unknown_properties(ALLOWED_CONFIG_KEYS, "PostgreSQL sink")?;

        let mut cfg = Self::default();

        cfg.hostname = config.require("hostname")?.to_string();
        cfg.database = config.require("database")?.to_string();
        cfg.username = config.require("username")?.to_string();
        cfg.table_name = config.require("table.name")?.to_string();

        if let Some(v) = config.get("password") {
            cfg.password = v.to_string();
        }
        if let Some(v) = config.get("port") {
            cfg.port = crate::config::parse_port(v)?;
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
            if v.trim().is_empty() {
                cfg.primary_key_columns.clear();
            } else {
                let columns: Vec<_> = v.split(',').map(str::trim).collect();
                if columns.iter().any(|column| column.is_empty()) {
                    return Err(ConnectorError::ConfigurationError(
                        "primary.key contains an empty column name".into(),
                    ));
                }
                cfg.primary_key_columns = columns.into_iter().map(str::to_string).collect();
            }
        }
        if let Some(v) = config.get("flush.interval.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid flush.interval.ms: '{v}'"))
            })?;
            cfg.flush_interval = Duration::from_millis(ms);
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
                    "invalid ssl.mode: '{v}' (expected 'disable' or 'verify-full')"
                ))
            })?;
        }
        cfg.ssl_ca_cert_path = config.get("ssl.ca.cert.path").map(PathBuf::from);
        if let Some(v) = config.get("auto.create.table") {
            cfg.auto_create_table = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("changelog.mode") {
            cfg.changelog_mode = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("statement.timeout.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid statement.timeout.ms: '{v}'"))
            })?;
            cfg.statement_timeout = Duration::from_millis(ms);
        }
        cfg.validate()?;
        Ok(cfg)
    }

    fn reject_removed_keys(config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if let Some(key) = REMOVED_CONFIG_KEYS
            .iter()
            .find(|key| config.get(key).is_some())
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL sink property '{key}' is not supported: batching is owned by the runtime and retained-byte limit"
            )));
        }
        Ok(())
    }

    /// Validates the configuration for consistency.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on invalid combinations.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        crate::config::require_non_empty(&self.hostname, "hostname")?;
        crate::config::require_non_empty(&self.database, "database")?;
        crate::config::require_non_empty(&self.username, "username")?;
        crate::config::require_non_empty(&self.schema_name, "schema.name")?;
        crate::config::require_non_empty(&self.table_name, "table.name")?;
        validate_sql_identifier(&self.schema_name, "schema.name")?;
        validate_sql_identifier(&self.table_name, "table.name")?;
        let mut primary_keys = std::collections::HashSet::new();
        for column in &self.primary_key_columns {
            validate_sql_identifier(column, "primary.key column")?;
            if !primary_keys.insert(column) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "primary.key contains duplicate column '{column}'"
                )));
            }
        }
        if self.port == 0 {
            return Err(ConnectorError::ConfigurationError(
                "port must be > 0".into(),
            ));
        }
        if self
            .ssl_ca_cert_path
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ConnectorError::ConfigurationError(
                "ssl.ca.cert.path must not be empty".into(),
            ));
        }
        if self.ssl_mode == SslMode::Disable && self.ssl_ca_cert_path.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "ssl.ca.cert.path requires ssl.mode=verify-full".into(),
            ));
        }
        if self.write_mode == WriteMode::Upsert && self.primary_key_columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "upsert mode requires 'primary.key' to be set".into(),
            ));
        }
        if self.changelog_mode && self.write_mode != WriteMode::Upsert {
            return Err(ConnectorError::ConfigurationError(
                "changelog mode requires write.mode = 'upsert'".into(),
            ));
        }
        if self.flush_interval.is_zero() {
            return Err(ConnectorError::ConfigurationError(
                "flush.interval.ms must be > 0".into(),
            ));
        }
        if self.connect_timeout.is_zero() {
            return Err(ConnectorError::ConfigurationError(
                "connect.timeout.ms must be > 0".into(),
            ));
        }
        if self.statement_timeout < Duration::from_secs(1) {
            return Err(ConnectorError::ConfigurationError(
                "statement.timeout.ms must be >= 1000 (1 second)".into(),
            ));
        }
        if self.statement_timeout.as_millis() > MAX_POSTGRES_STATEMENT_TIMEOUT_MS {
            return Err(ConnectorError::ConfigurationError(format!(
                "statement.timeout.ms must be <= {MAX_POSTGRES_STATEMENT_TIMEOUT_MS}"
            )));
        }
        if !self
            .statement_timeout
            .subsec_nanos()
            .is_multiple_of(1_000_000)
        {
            return Err(ConnectorError::ConfigurationError(
                "statement timeout must be an integer number of milliseconds".into(),
            ));
        }
        Ok(())
    }

    /// `PostgreSQL` startup option applied to every connection created by the pool.
    #[must_use]
    pub(super) fn statement_timeout_startup_option(&self) -> String {
        format!(
            "-c statement_timeout={}",
            self.statement_timeout.as_millis()
        )
    }

    /// Returns the safely quoted fully qualified table name (`"schema"."table"`).
    #[must_use]
    pub fn qualified_table_name(&self) -> String {
        format!(
            "{}.{}",
            quote_sql_identifier(&self.schema_name),
            quote_sql_identifier(&self.table_name)
        )
    }
}

/// Validates one `PostgreSQL` identifier segment before it can reach generated SQL.
pub(super) fn validate_sql_identifier(identifier: &str, label: &str) -> Result<(), ConnectorError> {
    if identifier.is_empty() {
        return Err(ConnectorError::ConfigurationError(format!(
            "{label} must not be empty"
        )));
    }
    if identifier.contains('\0') {
        return Err(ConnectorError::ConfigurationError(format!(
            "{label} must not contain NUL"
        )));
    }
    if identifier.len() > MAX_POSTGRES_IDENTIFIER_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "{label} exceeds PostgreSQL's {MAX_POSTGRES_IDENTIFIER_BYTES}-byte identifier limit"
        )));
    }
    Ok(())
}

/// Quotes one already-validated identifier segment. `PostgreSQL` escapes embedded quotes by
/// doubling them; dots remain literal characters within the segment.
pub(super) fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
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
            ("flush.interval.ms", "500"),
            ("connect.timeout.ms", "5000"),
            ("ssl.mode", "verify-full"),
            ("ssl.ca.cert.path", "/certs/ca.pem"),
            ("auto.create.table", "true"),
            ("changelog.mode", "true"),
        ]);
        let config = make_config(&pairs);
        let cfg = PostgresSinkConfig::from_config(&config).unwrap();

        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.schema_name, "analytics");
        assert_eq!(cfg.write_mode, WriteMode::Upsert);
        assert_eq!(cfg.primary_key_columns, vec!["id", "region"]);
        assert_eq!(cfg.flush_interval, Duration::from_millis(500));
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
        assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
        assert_eq!(cfg.ssl_ca_cert_path, Some(PathBuf::from("/certs/ca.pem")));
        assert!(cfg.auto_create_table);
        assert!(cfg.changelog_mode);
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
    fn test_changelog_requires_upsert() {
        let mut pairs = required_pairs();
        pairs.push(("changelog.mode", "true"));
        let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        assert!(error.to_string().contains("write.mode = 'upsert'"));
    }

    #[test]
    fn removed_buffer_and_pool_options_are_rejected() {
        for key in REMOVED_CONFIG_KEYS {
            let mut pairs = required_pairs();
            pairs.push((*key, "1"));
            let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
            assert!(error.to_string().contains(key));
        }
    }

    #[test]
    fn test_qualified_table_name() {
        let cfg = PostgresSinkConfig::new("localhost", "db", "events");
        assert_eq!(cfg.qualified_table_name(), "\"public\".\"events\"");

        let mut cfg2 = cfg;
        cfg2.schema_name = "analytics".to_string();
        cfg2.table_name = "order\"items".to_string();
        assert_eq!(
            cfg2.qualified_table_name(),
            "\"analytics\".\"order\"\"items\""
        );
    }

    #[test]
    fn identifiers_fail_closed_before_sql_generation() {
        for invalid in ["", "bad\0name"] {
            let mut cfg = PostgresSinkConfig::new("localhost", "db", invalid);
            assert!(cfg.validate().is_err(), "table identifier {invalid:?}");
            cfg.table_name = "events".into();
            cfg.schema_name = invalid.into();
            assert!(cfg.validate().is_err(), "schema identifier {invalid:?}");
        }

        let mut cfg = PostgresSinkConfig::new("localhost", "db", "events");
        cfg.write_mode = WriteMode::Upsert;
        cfg.primary_key_columns = vec!["id".into(), "id".into()];
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
    }

    #[test]
    fn test_defaults() {
        let cfg = PostgresSinkConfig::default();
        assert_eq!(cfg.hostname, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.schema_name, "public");
        assert_eq!(cfg.write_mode, WriteMode::Append);
        assert_eq!(cfg.flush_interval, Duration::from_millis(250));
        assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
        assert!(!cfg.auto_create_table);
        assert!(!cfg.changelog_mode);
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
    fn legacy_ssl_modes_are_rejected() {
        for mode in ["off", "prefer", "require", "verify-ca"] {
            let mut pairs = required_pairs();
            pairs.push(("ssl.mode", mode));
            let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
            let message = error.to_string();
            assert!(message.contains("invalid ssl.mode"), "{message}");
        }
    }

    #[test]
    fn plaintext_rejects_unused_ca_path() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("ssl.mode", "disable"),
            ("ssl.ca.cert.path", "/certs/ca.pem"),
        ]);
        let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        assert!(error.to_string().contains("ssl.mode=verify-full"));
    }

    #[test]
    fn test_invalid_port() {
        let mut pairs = required_pairs();
        pairs.push(("port", "not_a_number"));
        let config = make_config(&pairs);
        assert!(PostgresSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_zero_flush_interval_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("flush.interval.ms", "0"));
        let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        assert!(error.to_string().contains("flush.interval.ms must be > 0"));
    }

    #[test]
    fn zero_connect_timeout_is_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("connect.timeout.ms", "0"));
        let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        assert!(error.to_string().contains("connect.timeout.ms must be > 0"));
    }

    #[test]
    fn statement_timeout_is_a_bounded_integer_startup_setting() {
        let mut cfg = PostgresSinkConfig::new("localhost", "db", "events");
        cfg.statement_timeout = Duration::from_millis(30_000);
        cfg.validate().unwrap();
        assert_eq!(
            cfg.statement_timeout_startup_option(),
            "-c statement_timeout=30000"
        );

        cfg.statement_timeout =
            Duration::from_millis(u64::try_from(MAX_POSTGRES_STATEMENT_TIMEOUT_MS).unwrap());
        cfg.validate().unwrap();
        cfg.statement_timeout =
            Duration::from_millis(u64::try_from(MAX_POSTGRES_STATEMENT_TIMEOUT_MS + 1).unwrap());
        assert!(cfg.validate().is_err());

        cfg.statement_timeout = Duration::from_secs(1) + Duration::from_micros(1);
        assert!(cfg.validate().is_err());
    }
}
