//! `PostgreSQL` sink connector configuration.
//!
//! [`PostgresSinkConfig`] encapsulates all settings for writing Arrow
//! `RecordBatch` data to `PostgreSQL`, parsed from resolved connector options.

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
/// Parsed from resolved sink connector options via [`from_config`](Self::from_config).
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

    /// Parses a sink config from a resolved [`ConnectorConfig`].
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
mod tests;
