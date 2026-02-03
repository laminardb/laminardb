//! `PostgreSQL` CDC source connector configuration.
//!
//! Provides [`PostgresCdcConfig`] with all settings needed to connect to
//! a `PostgreSQL` database and stream logical replication changes.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::lsn::Lsn;

/// Configuration for the `PostgreSQL` CDC source connector.
#[derive(Debug, Clone)]
pub struct PostgresCdcConfig {
    // ── Connection ──
    /// `PostgreSQL` host address.
    pub host: String,

    /// `PostgreSQL` port.
    pub port: u16,

    /// Database name.
    pub database: String,

    /// Username for authentication.
    pub username: String,

    /// Password for authentication.
    pub password: Option<String>,

    /// SSL mode for the connection.
    pub ssl_mode: SslMode,

    // ── Replication ──
    /// Name of the logical replication slot.
    pub slot_name: String,

    /// Name of the publication to subscribe to.
    pub publication: String,

    /// LSN to start replication from (None = slot's `confirmed_flush_lsn`).
    pub start_lsn: Option<Lsn>,

    /// Output plugin name (always `pgoutput` for logical replication).
    pub output_plugin: String,

    // ── Snapshot ──
    /// How to handle the initial data snapshot.
    pub snapshot_mode: SnapshotMode,

    // ── Tuning ──
    /// Timeout for each poll operation.
    pub poll_timeout: Duration,

    /// Maximum records to return per poll.
    pub max_poll_records: usize,

    /// Interval for sending keepalive/status updates to `PostgreSQL`.
    pub keepalive_interval: Duration,

    /// Maximum WAL sender timeout before the server drops the connection.
    pub wal_sender_timeout: Duration,

    // ── Schema ──
    /// Tables to include (empty = all tables in publication).
    pub table_include: Vec<String>,

    /// Tables to exclude from replication.
    pub table_exclude: Vec<String>,
}

impl Default for PostgresCdcConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: "postgres".to_string(),
            username: "postgres".to_string(),
            password: None,
            ssl_mode: SslMode::Prefer,
            slot_name: "laminar_slot".to_string(),
            publication: "laminar_pub".to_string(),
            start_lsn: None,
            output_plugin: "pgoutput".to_string(),
            snapshot_mode: SnapshotMode::Initial,
            poll_timeout: Duration::from_millis(100),
            max_poll_records: 1000,
            keepalive_interval: Duration::from_secs(10),
            wal_sender_timeout: Duration::from_secs(60),
            table_include: Vec::new(),
            table_exclude: Vec::new(),
        }
    }
}

impl PostgresCdcConfig {
    /// Creates a new config with required fields.
    #[must_use]
    pub fn new(host: &str, database: &str, slot_name: &str, publication: &str) -> Self {
        Self {
            host: host.to_string(),
            database: database.to_string(),
            slot_name: slot_name.to_string(),
            publication: publication.to_string(),
            ..Self::default()
        }
    }

    /// Builds a `PostgreSQL` connection string.
    #[must_use]
    pub fn connection_string(&self) -> String {
        use std::fmt::Write;
        let mut s = format!(
            "host={} port={} dbname={} user={}",
            self.host, self.port, self.database, self.username
        );
        if let Some(ref pw) = self.password {
            let _ = write!(s, " password={pw}");
        }
        let _ = write!(s, " sslmode={}", self.ssl_mode);
        s
    }

    /// Parses configuration from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required keys are missing or values are
    /// invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self {
            host: config.require("host")?.to_string(),
            database: config.require("database")?.to_string(),
            slot_name: config.require("slot.name")?.to_string(),
            publication: config.require("publication")?.to_string(),
            ..Self::default()
        };

        if let Some(port) = config.get("port") {
            cfg.port = port
                .parse()
                .map_err(|_| ConnectorError::ConfigurationError(format!("invalid port: {port}")))?;
        }
        if let Some(user) = config.get("username") {
            cfg.username = user.to_string();
        }
        cfg.password = config.get("password").map(String::from);

        if let Some(ssl) = config.get("ssl.mode") {
            cfg.ssl_mode = ssl.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid ssl.mode: {ssl}"))
            })?;
        }
        if let Some(lsn) = config.get("start.lsn") {
            cfg.start_lsn = Some(lsn.parse::<Lsn>().map_err(|e| {
                ConnectorError::ConfigurationError(format!("invalid start.lsn: {e}"))
            })?);
        }
        if let Some(mode) = config.get("snapshot.mode") {
            cfg.snapshot_mode = mode.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid snapshot.mode: {mode}"))
            })?;
        }
        if let Some(timeout) = config.get_parsed::<u64>("poll.timeout.ms")? {
            cfg.poll_timeout = Duration::from_millis(timeout);
        }
        if let Some(max) = config.get_parsed::<usize>("max.poll.records")? {
            cfg.max_poll_records = max;
        }
        if let Some(interval) = config.get_parsed::<u64>("keepalive.interval.ms")? {
            cfg.keepalive_interval = Duration::from_millis(interval);
        }
        if let Some(tables) = config.get("table.include") {
            cfg.table_include = tables.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Some(tables) = config.get("table.exclude") {
            cfg.table_exclude = tables.split(',').map(|s| s.trim().to_string()).collect();
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` for invalid settings.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.host.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "host must not be empty".to_string(),
            ));
        }
        if self.database.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "database must not be empty".to_string(),
            ));
        }
        if self.slot_name.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "slot.name must not be empty".to_string(),
            ));
        }
        if self.publication.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "publication must not be empty".to_string(),
            ));
        }
        if self.max_poll_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.poll.records must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    /// Returns whether a table should be included based on include/exclude lists.
    #[must_use]
    pub fn should_include_table(&self, table: &str) -> bool {
        if self.table_exclude.iter().any(|t| t == table) {
            return false;
        }
        if self.table_include.is_empty() {
            return true;
        }
        self.table_include.iter().any(|t| t == table)
    }
}

/// SSL connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// No SSL.
    Disable,
    /// Try SSL, fall back to unencrypted.
    #[default]
    Prefer,
    /// Require SSL.
    Require,
    /// Require SSL and verify CA certificate.
    VerifyCa,
    /// Require SSL and verify server hostname.
    VerifyFull,
}

impl fmt::Display for SslMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SslMode::Disable => write!(f, "disable"),
            SslMode::Prefer => write!(f, "prefer"),
            SslMode::Require => write!(f, "require"),
            SslMode::VerifyCa => write!(f, "verify-ca"),
            SslMode::VerifyFull => write!(f, "verify-full"),
        }
    }
}

impl FromStr for SslMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disable" => Ok(SslMode::Disable),
            "prefer" => Ok(SslMode::Prefer),
            "require" => Ok(SslMode::Require),
            "verify-ca" | "verify_ca" => Ok(SslMode::VerifyCa),
            "verify-full" | "verify_full" => Ok(SslMode::VerifyFull),
            _ => Err(format!("unknown SSL mode: {s}")),
        }
    }
}

/// How to handle the initial snapshot when no prior checkpoint exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotMode {
    /// Take a full snapshot on first start, then switch to streaming.
    #[default]
    Initial,
    /// Never take a snapshot; only stream from the replication slot's position.
    Never,
    /// Always take a snapshot on startup, even if a checkpoint exists.
    Always,
}

impl fmt::Display for SnapshotMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotMode::Initial => write!(f, "initial"),
            SnapshotMode::Never => write!(f, "never"),
            SnapshotMode::Always => write!(f, "always"),
        }
    }
}

impl FromStr for SnapshotMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "initial" => Ok(SnapshotMode::Initial),
            "never" => Ok(SnapshotMode::Never),
            "always" => Ok(SnapshotMode::Always),
            _ => Err(format!("unknown snapshot mode: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = PostgresCdcConfig::default();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.database, "postgres");
        assert_eq!(cfg.slot_name, "laminar_slot");
        assert_eq!(cfg.publication, "laminar_pub");
        assert_eq!(cfg.output_plugin, "pgoutput");
        assert_eq!(cfg.ssl_mode, SslMode::Prefer);
        assert_eq!(cfg.snapshot_mode, SnapshotMode::Initial);
        assert_eq!(cfg.max_poll_records, 1000);
    }

    #[test]
    fn test_new_config() {
        let cfg = PostgresCdcConfig::new("db.example.com", "mydb", "my_slot", "my_pub");
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.slot_name, "my_slot");
        assert_eq!(cfg.publication, "my_pub");
    }

    #[test]
    fn test_connection_string() {
        let mut cfg = PostgresCdcConfig::new("db.example.com", "mydb", "s", "p");
        cfg.password = Some("secret".to_string());
        let conn = cfg.connection_string();
        assert!(conn.contains("host=db.example.com"));
        assert!(conn.contains("dbname=mydb"));
        assert!(conn.contains("password=secret"));
        assert!(conn.contains("sslmode=prefer"));
    }

    #[test]
    fn test_from_connector_config() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "pg.local");
        config.set("database", "testdb");
        config.set("slot.name", "test_slot");
        config.set("publication", "test_pub");
        config.set("port", "5433");
        config.set("ssl.mode", "require");
        config.set("snapshot.mode", "never");
        config.set("max.poll.records", "500");

        let cfg = PostgresCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.host, "pg.local");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.database, "testdb");
        assert_eq!(cfg.ssl_mode, SslMode::Require);
        assert_eq!(cfg.snapshot_mode, SnapshotMode::Never);
        assert_eq!(cfg.max_poll_records, 500);
    }

    #[test]
    fn test_from_config_missing_required() {
        let config = ConnectorConfig::new("postgres-cdc");
        assert!(PostgresCdcConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_from_config_invalid_port() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "localhost");
        config.set("database", "db");
        config.set("slot.name", "s");
        config.set("publication", "p");
        config.set("port", "not_a_number");
        assert!(PostgresCdcConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_validate_empty_host() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.host = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_zero_max_poll() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.max_poll_records = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_ssl_mode_fromstr() {
        assert_eq!("disable".parse::<SslMode>().unwrap(), SslMode::Disable);
        assert_eq!("prefer".parse::<SslMode>().unwrap(), SslMode::Prefer);
        assert_eq!("require".parse::<SslMode>().unwrap(), SslMode::Require);
        assert_eq!("verify-ca".parse::<SslMode>().unwrap(), SslMode::VerifyCa);
        assert_eq!(
            "verify-full".parse::<SslMode>().unwrap(),
            SslMode::VerifyFull
        );
        assert!("invalid".parse::<SslMode>().is_err());
    }

    #[test]
    fn test_snapshot_mode_fromstr() {
        assert_eq!(
            "initial".parse::<SnapshotMode>().unwrap(),
            SnapshotMode::Initial
        );
        assert_eq!(
            "never".parse::<SnapshotMode>().unwrap(),
            SnapshotMode::Never
        );
        assert_eq!(
            "always".parse::<SnapshotMode>().unwrap(),
            SnapshotMode::Always
        );
        assert!("bad".parse::<SnapshotMode>().is_err());
    }

    #[test]
    fn test_ssl_mode_display() {
        assert_eq!(SslMode::Disable.to_string(), "disable");
        assert_eq!(SslMode::VerifyFull.to_string(), "verify-full");
    }

    #[test]
    fn test_table_filtering() {
        let mut cfg = PostgresCdcConfig::default();
        // No filters → include all
        assert!(cfg.should_include_table("public.users"));

        // Include list
        cfg.table_include = vec!["public.users".to_string(), "public.orders".to_string()];
        assert!(cfg.should_include_table("public.users"));
        assert!(!cfg.should_include_table("public.logs"));

        // Exclude overrides include
        cfg.table_exclude = vec!["public.users".to_string()];
        assert!(!cfg.should_include_table("public.users"));
    }

    #[test]
    fn test_from_config_with_start_lsn() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "localhost");
        config.set("database", "db");
        config.set("slot.name", "s");
        config.set("publication", "p");
        config.set("start.lsn", "0/1234ABCD");

        let cfg = PostgresCdcConfig::from_config(&config).unwrap();
        assert!(cfg.start_lsn.is_some());
        assert_eq!(cfg.start_lsn.unwrap().as_u64(), 0x1234_ABCD);
    }

    #[test]
    fn test_from_config_table_include() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "localhost");
        config.set("database", "db");
        config.set("slot.name", "s");
        config.set("publication", "p");
        config.set("table.include", "public.users, public.orders");

        let cfg = PostgresCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.table_include, vec!["public.users", "public.orders"]);
    }
}
