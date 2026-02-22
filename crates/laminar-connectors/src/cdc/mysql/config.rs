//! MySQL CDC source connector configuration.
//!
//! Provides [`MySqlCdcConfig`] with all settings needed to connect to
//! a MySQL database and stream binlog changes.

use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::gtid::GtidSet;

/// Configuration for the MySQL CDC source connector.
#[derive(Debug, Clone)]
pub struct MySqlCdcConfig {
    // ── Connection ──
    /// MySQL host address.
    pub host: String,

    /// MySQL port.
    pub port: u16,

    /// Database name (optional for binlog replication).
    pub database: Option<String>,

    /// Username for authentication.
    pub username: String,

    /// Password for authentication.
    pub password: Option<String>,

    /// SSL mode for the connection.
    pub ssl_mode: SslMode,

    // ── Replication ──
    /// Server ID for the replica (must be unique in the topology).
    /// MySQL requires each replica to have a unique server ID.
    pub server_id: u32,

    /// GTID set to start replication from (None = use binlog position).
    /// Using GTID is recommended for failover support.
    pub gtid_set: Option<GtidSet>,

    /// Binlog filename to start from (if not using GTID).
    pub binlog_filename: Option<String>,

    /// Binlog position to start from (if not using GTID).
    pub binlog_position: Option<u64>,

    /// Whether to use GTID-based replication (recommended).
    pub use_gtid: bool,

    // ── Snapshot ──
    /// How to handle the initial data snapshot.
    pub snapshot_mode: SnapshotMode,

    // ── Tuning ──
    /// Timeout for each poll operation.
    pub poll_timeout: Duration,

    /// Maximum records to return per poll.
    pub max_poll_records: usize,

    /// Heartbeat interval for the binlog connection.
    pub heartbeat_interval: Duration,

    /// Connection timeout.
    pub connect_timeout: Duration,

    /// Read timeout for binlog events.
    pub read_timeout: Duration,

    // ── Schema ──
    /// Tables to include (format: "database.table" or just "table").
    /// Empty = all tables.
    pub table_include: Vec<String>,

    /// Tables to exclude from replication.
    pub table_exclude: Vec<String>,

    /// Database filter (if set, only replicate from this database).
    pub database_filter: Option<String>,
}

impl Default for MySqlCdcConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            database: None,
            username: "root".to_string(),
            password: None,
            ssl_mode: SslMode::Preferred,
            server_id: 1001, // Default replica server ID
            gtid_set: None,
            binlog_filename: None,
            binlog_position: None,
            use_gtid: true, // GTID is the recommended approach
            snapshot_mode: SnapshotMode::Initial,
            poll_timeout: Duration::from_millis(100),
            max_poll_records: 1000,
            heartbeat_interval: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(60),
            table_include: Vec::new(),
            table_exclude: Vec::new(),
            database_filter: None,
        }
    }
}

impl MySqlCdcConfig {
    /// Creates a new config with required fields.
    #[must_use]
    pub fn new(host: &str, username: &str) -> Self {
        Self {
            host: host.to_string(),
            username: username.to_string(),
            ..Self::default()
        }
    }

    /// Creates a new config with server ID.
    #[must_use]
    pub fn with_server_id(host: &str, username: &str, server_id: u32) -> Self {
        Self {
            host: host.to_string(),
            username: username.to_string(),
            server_id,
            ..Self::default()
        }
    }

    /// Builds a MySQL connection URL.
    #[must_use]
    pub fn connection_url(&self) -> String {
        let mut url = format!("mysql://{}@{}:{}", self.username, self.host, self.port);
        if let Some(ref db) = self.database {
            url.push('/');
            url.push_str(db);
        }
        url
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
            username: config.require("username")?.to_string(),
            ..Self::default()
        };

        if let Some(port) = config.get("port") {
            cfg.port = port
                .parse()
                .map_err(|_| ConnectorError::ConfigurationError(format!("invalid port: {port}")))?;
        }
        cfg.database = config.get("database").map(String::from);
        cfg.password = config.get("password").map(String::from);

        if let Some(ssl) = config.get_parsed::<SslMode>("ssl.mode")? {
            cfg.ssl_mode = ssl;
        }

        if let Some(id) = config.get_parsed::<u32>("server.id")? {
            cfg.server_id = id;
        }

        if let Some(gtid) = config.get_parsed::<GtidSet>("gtid.set")? {
            cfg.gtid_set = Some(gtid);
        }

        cfg.binlog_filename = config.get("binlog.filename").map(String::from);

        if let Some(pos) = config.get_parsed::<u64>("binlog.position")? {
            cfg.binlog_position = Some(pos);
        }

        if let Some(use_gtid) = config.get("use.gtid") {
            cfg.use_gtid = use_gtid.parse().unwrap_or(true);
        }

        if let Some(mode) = config.get_parsed::<SnapshotMode>("snapshot.mode")? {
            cfg.snapshot_mode = mode;
        }

        if let Some(timeout) = config.get_parsed::<u64>("poll.timeout.ms")? {
            cfg.poll_timeout = Duration::from_millis(timeout);
        }
        if let Some(max) = config.get_parsed::<usize>("max.poll.records")? {
            cfg.max_poll_records = max;
        }
        if let Some(interval) = config.get_parsed::<u64>("heartbeat.interval.ms")? {
            cfg.heartbeat_interval = Duration::from_millis(interval);
        }
        if let Some(timeout) = config.get_parsed::<u64>("connect.timeout.ms")? {
            cfg.connect_timeout = Duration::from_millis(timeout);
        }
        if let Some(timeout) = config.get_parsed::<u64>("read.timeout.ms")? {
            cfg.read_timeout = Duration::from_millis(timeout);
        }

        if let Some(tables) = config.get("table.include") {
            cfg.table_include = tables.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Some(tables) = config.get("table.exclude") {
            cfg.table_exclude = tables.split(',').map(|s| s.trim().to_string()).collect();
        }
        cfg.database_filter = config.get("database.filter").map(String::from);

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` for invalid settings.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        crate::config::require_non_empty(&self.host, "host")?;
        crate::config::require_non_empty(&self.username, "username")?;
        if self.server_id == 0 {
            return Err(ConnectorError::ConfigurationError(
                "server.id must be > 0".to_string(),
            ));
        }
        if self.max_poll_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.poll.records must be > 0".to_string(),
            ));
        }

        // If not using GTID, binlog filename should be specified for explicit positioning
        if !self.use_gtid && self.binlog_filename.is_none() && self.binlog_position.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "binlog.filename required when binlog.position is set".to_string(),
            ));
        }

        Ok(())
    }

    /// Returns whether a table should be included based on include/exclude lists.
    #[must_use]
    pub fn should_include_table(&self, database: &str, table: &str) -> bool {
        let full_name = format!("{database}.{table}");

        // Check exclude list first
        if self
            .table_exclude
            .iter()
            .any(|t| t == table || t == &full_name || t.ends_with(&format!(".{table}")))
        {
            return false;
        }

        // Check database filter
        if let Some(ref db_filter) = self.database_filter {
            if db_filter != database {
                return false;
            }
        }

        // Check include list
        if self.table_include.is_empty() {
            return true;
        }

        self.table_include
            .iter()
            .any(|t| t == table || t == &full_name || t.ends_with(&format!(".{table}")))
    }

    /// Returns the replication mode description.
    #[must_use]
    pub fn replication_mode(&self) -> &'static str {
        if self.use_gtid {
            "GTID"
        } else {
            "binlog position"
        }
    }
}

/// SSL connection mode for MySQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// No SSL.
    Disabled,
    /// Try SSL, fall back to unencrypted.
    #[default]
    Preferred,
    /// Require SSL.
    Required,
    /// Require SSL and verify CA certificate.
    VerifyCa,
    /// Require SSL and verify server identity.
    VerifyIdentity,
}

str_enum!(SslMode, lowercase_nodash, String, "unknown SSL mode",
    Disabled => "disabled", "disable", "false";
    Preferred => "preferred", "prefer";
    Required => "required", "require", "true";
    VerifyCa => "verify_ca", "verify-ca";
    VerifyIdentity => "verify_identity", "verify-identity"
);

/// How to handle the initial snapshot when no prior checkpoint exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotMode {
    /// Take a full snapshot on first start, then switch to streaming.
    #[default]
    Initial,
    /// Never take a snapshot; only stream from current binlog position.
    Never,
    /// Always take a snapshot on startup, even if a checkpoint exists.
    Always,
    /// Only take a schema snapshot, no data.
    SchemaOnly,
}

str_enum!(SnapshotMode, lowercase_nodash, String, "unknown snapshot mode",
    Initial => "initial";
    Never => "never";
    Always => "always";
    SchemaOnly => "schema_only", "schema-only"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = MySqlCdcConfig::default();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 3306);
        assert_eq!(cfg.username, "root");
        assert!(cfg.use_gtid);
        assert_eq!(cfg.ssl_mode, SslMode::Preferred);
        assert_eq!(cfg.snapshot_mode, SnapshotMode::Initial);
        assert_eq!(cfg.server_id, 1001);
    }

    #[test]
    fn test_new_config() {
        let cfg = MySqlCdcConfig::new("db.example.com", "myuser");
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.username, "myuser");
    }

    #[test]
    fn test_with_server_id() {
        let cfg = MySqlCdcConfig::with_server_id("db.example.com", "myuser", 5000);
        assert_eq!(cfg.server_id, 5000);
    }

    #[test]
    fn test_connection_url() {
        let mut cfg = MySqlCdcConfig::new("db.example.com", "myuser");
        assert_eq!(cfg.connection_url(), "mysql://myuser@db.example.com:3306");

        cfg.database = Some("mydb".to_string());
        assert_eq!(
            cfg.connection_url(),
            "mysql://myuser@db.example.com:3306/mydb"
        );
    }

    #[test]
    fn test_from_connector_config() {
        let mut config = ConnectorConfig::new("mysql-cdc");
        config.set("host", "mysql.local");
        config.set("username", "repl_user");
        config.set("password", "secret");
        config.set("port", "3307");
        config.set("server.id", "2000");
        config.set("ssl.mode", "required");
        config.set("snapshot.mode", "never");
        config.set("use.gtid", "true");
        config.set("max.poll.records", "500");

        let cfg = MySqlCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.host, "mysql.local");
        assert_eq!(cfg.username, "repl_user");
        assert_eq!(cfg.password, Some("secret".to_string()));
        assert_eq!(cfg.port, 3307);
        assert_eq!(cfg.server_id, 2000);
        assert_eq!(cfg.ssl_mode, SslMode::Required);
        assert_eq!(cfg.snapshot_mode, SnapshotMode::Never);
        assert!(cfg.use_gtid);
        assert_eq!(cfg.max_poll_records, 500);
    }

    #[test]
    fn test_from_config_missing_required() {
        let config = ConnectorConfig::new("mysql-cdc");
        assert!(MySqlCdcConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_validate_empty_host() {
        let mut cfg = MySqlCdcConfig::default();
        cfg.host = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_zero_server_id() {
        let mut cfg = MySqlCdcConfig::default();
        cfg.server_id = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_binlog_position_without_filename() {
        let mut cfg = MySqlCdcConfig::default();
        cfg.use_gtid = false;
        cfg.binlog_position = Some(12345);
        cfg.binlog_filename = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_ssl_mode_fromstr() {
        assert_eq!("disabled".parse::<SslMode>().unwrap(), SslMode::Disabled);
        assert_eq!("preferred".parse::<SslMode>().unwrap(), SslMode::Preferred);
        assert_eq!("required".parse::<SslMode>().unwrap(), SslMode::Required);
        assert_eq!("verify_ca".parse::<SslMode>().unwrap(), SslMode::VerifyCa);
        assert_eq!(
            "verify_identity".parse::<SslMode>().unwrap(),
            SslMode::VerifyIdentity
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
        assert_eq!(
            "schema_only".parse::<SnapshotMode>().unwrap(),
            SnapshotMode::SchemaOnly
        );
        assert!("bad".parse::<SnapshotMode>().is_err());
    }

    #[test]
    fn test_table_filtering_simple() {
        let mut cfg = MySqlCdcConfig::default();
        // No filters → include all
        assert!(cfg.should_include_table("mydb", "users"));

        // Include list - exact match
        cfg.table_include = vec!["mydb.users".to_string()];
        assert!(cfg.should_include_table("mydb", "users"));
        assert!(!cfg.should_include_table("mydb", "orders"));
    }

    #[test]
    fn test_table_filtering_exclude() {
        let mut cfg = MySqlCdcConfig::default();
        cfg.table_exclude = vec!["mydb.logs".to_string()];
        assert!(cfg.should_include_table("mydb", "users"));
        assert!(!cfg.should_include_table("mydb", "logs"));
    }

    #[test]
    fn test_table_filtering_database() {
        let mut cfg = MySqlCdcConfig::default();
        cfg.database_filter = Some("production".to_string());
        assert!(cfg.should_include_table("production", "users"));
        assert!(!cfg.should_include_table("staging", "users"));
    }

    #[test]
    fn test_table_filtering_table_name_only() {
        let mut cfg = MySqlCdcConfig::default();
        cfg.table_include = vec!["users".to_string()];
        assert!(cfg.should_include_table("any_db", "users"));
        assert!(!cfg.should_include_table("any_db", "orders"));
    }

    #[test]
    fn test_replication_mode() {
        let mut cfg = MySqlCdcConfig::default();
        cfg.use_gtid = true;
        assert_eq!(cfg.replication_mode(), "GTID");

        cfg.use_gtid = false;
        assert_eq!(cfg.replication_mode(), "binlog position");
    }

    #[test]
    fn test_from_config_with_gtid_set() {
        let mut config = ConnectorConfig::new("mysql-cdc");
        config.set("host", "localhost");
        config.set("username", "root");
        config.set("gtid.set", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10");

        let cfg = MySqlCdcConfig::from_config(&config).unwrap();
        assert!(cfg.gtid_set.is_some());
    }

    #[test]
    fn test_from_config_with_binlog_position() {
        let mut config = ConnectorConfig::new("mysql-cdc");
        config.set("host", "localhost");
        config.set("username", "root");
        config.set("use.gtid", "false");
        config.set("binlog.filename", "mysql-bin.000003");
        config.set("binlog.position", "12345");

        let cfg = MySqlCdcConfig::from_config(&config).unwrap();
        assert!(!cfg.use_gtid);
        assert_eq!(cfg.binlog_filename, Some("mysql-bin.000003".to_string()));
        assert_eq!(cfg.binlog_position, Some(12345));
    }
}
