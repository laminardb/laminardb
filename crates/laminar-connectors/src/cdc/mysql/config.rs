//! MySQL CDC source connector configuration.
//!
//! Provides [`MySqlCdcConfig`] with all settings needed to connect to
//! a MySQL database and stream binlog changes.

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::gtid::GtidSet;

const REMOVED_CONFIG_KEYS: &[&str] = &[
    "database",
    "snapshot.mode",
    "poll.timeout.ms",
    "max.poll.records",
    "heartbeat.interval.ms",
    "connect.timeout.ms",
    "read.timeout.ms",
];

/// Configuration for the MySQL CDC source connector.
#[derive(Debug, Clone)]
pub struct MySqlCdcConfig {
    // ── Connection ──
    /// MySQL host address.
    pub host: String,

    /// MySQL port.
    pub port: u16,

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

    // ── Schema ──
    /// The one table to include, in fully-qualified `database.table` form.
    ///
    /// A source has one stable Arrow schema. Deploy another source for each
    /// additional table instead of multiplexing incompatible row schemas.
    pub table_include: Vec<String>,

    /// Maximum events to buffer (default: 100,000).
    pub max_buffered_events: usize,

    /// High watermark ratio (0.0–1.0) of `max_buffered_events`. When the
    /// buffer reaches this level, stop draining the binlog reader channel
    /// to apply backpressure (default: 0.8).
    pub backpressure_high_watermark: f64,
}

impl Default for MySqlCdcConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            username: String::new(),
            password: None,
            ssl_mode: SslMode::Preferred,
            // There is no safe shared replication client ID. Configuration parsing and
            // validation require callers to choose a topology-unique value.
            server_id: 0,
            gtid_set: None,
            binlog_filename: None,
            binlog_position: None,
            use_gtid: true, // GTID is the recommended approach
            table_include: Vec::new(),
            max_buffered_events: 100_000,
            backpressure_high_watermark: 0.8,
        }
    }
}

impl MySqlCdcConfig {
    /// Returns the high watermark as an absolute event count.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn backpressure_high_watermark(&self) -> usize {
        (self.max_buffered_events as f64 * self.backpressure_high_watermark) as usize
    }

    /// Parses configuration from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required keys are missing or values are
    /// invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        Self::reject_removed_keys(config)?;

        let mut cfg = Self {
            host: config.require("host")?.to_string(),
            username: config.require("username")?.to_string(),
            server_id: config.require_parsed("server.id")?,
            ..Self::default()
        };

        if let Some(port) = config.get("port") {
            cfg.port = crate::config::parse_port(port)?;
        }
        cfg.password = config.get("password").map(String::from);

        if let Some(ssl) = config.get_parsed::<SslMode>("ssl.mode")? {
            cfg.ssl_mode = ssl;
        }

        if let Some(gtid) = config.get_parsed::<GtidSet>("gtid.set")? {
            cfg.gtid_set = Some(gtid);
        }

        cfg.binlog_filename = config.get("binlog.filename").map(String::from);

        if let Some(pos) = config.get_parsed::<u64>("binlog.position")? {
            cfg.binlog_position = Some(pos);
        }

        if let Some(use_gtid) = config.get_parsed::<bool>("use.gtid")? {
            cfg.use_gtid = use_gtid;
        }

        if let Some(tables) = config.get("table.include") {
            cfg.table_include = tables
                .split(',')
                .map(str::trim)
                .filter(|table| !table.is_empty())
                .map(str::to_owned)
                .collect();
        }
        if let Some(max) = config.get_parsed::<usize>("max.buffered.events")? {
            cfg.max_buffered_events = max;
        }
        if let Some(hw) = config.get_parsed::<f64>("backpressure.high.watermark")? {
            cfg.backpressure_high_watermark = hw;
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
                "MySQL CDC property '{key}' is not supported: it was removed because the \
                 connector did not execute it"
            )));
        }
        Ok(())
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` for invalid settings.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        crate::config::require_non_empty(&self.host, "host")?;
        crate::config::require_non_empty(&self.username, "username")?;
        if self.port == 0 {
            return Err(ConnectorError::ConfigurationError(
                "port must be > 0".to_string(),
            ));
        }
        if self.server_id == 0 {
            return Err(ConnectorError::ConfigurationError(
                "server.id must be > 0".to_string(),
            ));
        }
        if self.max_buffered_events == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.buffered.events must be > 0".to_string(),
            ));
        }
        if !(self.backpressure_high_watermark.is_finite()
            && 0.0 < self.backpressure_high_watermark
            && self.backpressure_high_watermark <= 1.0)
        {
            return Err(ConnectorError::ConfigurationError(
                "backpressure.high.watermark must be finite and in (0, 1]".to_string(),
            ));
        }
        self.target_table()?;

        if self.use_gtid && (self.binlog_filename.is_some() || self.binlog_position.is_some()) {
            return Err(ConnectorError::ConfigurationError(
                "binlog.filename/binlog.position cannot be set when use.gtid=true".into(),
            ));
        }
        if !self.use_gtid && self.gtid_set.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "gtid.set cannot be set when use.gtid=false".into(),
            ));
        }
        if !self.use_gtid && self.binlog_filename.is_none() && self.binlog_position.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "binlog.filename required when binlog.position is set".to_string(),
            ));
        }

        Ok(())
    }

    fn target_table(&self) -> Result<(&str, &str), ConnectorError> {
        let [target] = self.table_include.as_slice() else {
            return Err(ConnectorError::ConfigurationError(
                "MySQL CDC currently requires exactly one table.include entry; multi-table or \
                 unfiltered capture cannot preserve one stable Arrow schema"
                    .into(),
            ));
        };
        let Some((database, table)) = target.split_once('.') else {
            return Err(ConnectorError::ConfigurationError(
                "MySQL CDC table.include must be one fully-qualified database.table name".into(),
            ));
        };
        if database.is_empty() || table.is_empty() || table.contains('.') {
            return Err(ConnectorError::ConfigurationError(
                "MySQL CDC table.include must be one fully-qualified database.table name".into(),
            ));
        }
        Ok((database, table))
    }

    /// Returns the connection's default database derived from `table.include`.
    pub(crate) fn target_database(&self) -> Result<&str, ConnectorError> {
        self.target_table().map(|(database, _)| database)
    }

    /// Returns whether a table is the source's one configured table.
    #[must_use]
    pub fn should_include_table(&self, database: &str, table: &str) -> bool {
        self.target_table()
            .is_ok_and(|target| target == (database, table))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> MySqlCdcConfig {
        MySqlCdcConfig {
            username: "repl_user".into(),
            server_id: 2000,
            table_include: vec!["app.users".into()],
            ..MySqlCdcConfig::default()
        }
    }

    fn valid_connector_config() -> ConnectorConfig {
        let mut config = ConnectorConfig::new("mysql-cdc");
        config.set("host", "mysql.local");
        config.set("username", "repl_user");
        config.set("server.id", "2000");
        config.set("table.include", "app.users");
        config
    }

    #[test]
    fn test_default_config() {
        let cfg = MySqlCdcConfig::default();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 3306);
        assert!(cfg.username.is_empty());
        assert!(cfg.use_gtid);
        assert_eq!(cfg.ssl_mode, SslMode::Preferred);
        assert_eq!(cfg.server_id, 0);
    }

    #[test]
    fn test_from_connector_config() {
        let mut config = valid_connector_config();
        config.set("password", "secret");
        config.set("port", "3307");
        config.set("ssl.mode", "required");
        config.set("use.gtid", "true");
        config.set("max.buffered.events", "50000");
        config.set("backpressure.high.watermark", "0.75");

        let cfg = MySqlCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.host, "mysql.local");
        assert_eq!(cfg.username, "repl_user");
        assert_eq!(cfg.password, Some("secret".to_string()));
        assert_eq!(cfg.port, 3307);
        assert_eq!(cfg.server_id, 2000);
        assert_eq!(cfg.ssl_mode, SslMode::Required);
        assert!(cfg.use_gtid);
        assert_eq!(cfg.max_buffered_events, 50_000);
        assert_eq!(cfg.backpressure_high_watermark, 0.75);
    }

    #[test]
    fn test_from_config_missing_required() {
        let config = ConnectorConfig::new("mysql-cdc");
        assert!(MySqlCdcConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_validate_empty_host() {
        let mut cfg = valid_config();
        cfg.host = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_zero_server_id() {
        let mut cfg = valid_config();
        cfg.server_id = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_binlog_position_without_filename() {
        let mut cfg = valid_config();
        cfg.use_gtid = false;
        cfg.binlog_position = Some(12345);
        cfg.binlog_filename = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_unbounded_or_multi_table_capture() {
        let mut cfg = valid_config();
        cfg.table_include.clear();
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exactly one"));

        cfg.table_include = vec!["app.users".into(), "app.orders".into()];
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exactly one"));
    }

    #[test]
    fn validate_requires_fully_qualified_table() {
        let cfg = MySqlCdcConfig {
            table_include: vec!["users".into()],
            ..valid_config()
        };
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("database.table"));
    }

    #[test]
    fn validate_rejects_zero_or_non_finite_runtime_bounds() {
        let mut cfg = valid_config();
        cfg.max_buffered_events = 0;
        assert!(cfg.validate().unwrap_err().to_string().contains("buffered"));

        let mut cfg = valid_config();
        cfg.backpressure_high_watermark = f64::NAN;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("watermark"));
    }

    #[test]
    fn removed_properties_are_rejected_explicitly() {
        for key in REMOVED_CONFIG_KEYS {
            let mut config = valid_connector_config();
            config.set(*key, "removed-value");
            let error = MySqlCdcConfig::from_config(&config).unwrap_err();
            assert!(error.to_string().contains(key));
        }
    }

    #[test]
    fn use_gtid_requires_a_boolean() {
        let mut config = valid_connector_config();
        config.set("use.gtid", "sometimes");
        let error = MySqlCdcConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("use.gtid"));
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
    fn test_table_filtering_simple() {
        let cfg = MySqlCdcConfig {
            table_include: vec!["mydb.users".to_string()],
            ..valid_config()
        };
        assert!(cfg.should_include_table("mydb", "users"));
        assert!(!cfg.should_include_table("mydb", "orders"));
        assert!(!cfg.should_include_table("other", "users"));
    }

    #[test]
    fn test_from_config_with_gtid_set() {
        let mut config = valid_connector_config();
        config.set("gtid.set", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10");

        let cfg = MySqlCdcConfig::from_config(&config).unwrap();
        assert!(cfg.gtid_set.is_some());
    }

    #[test]
    fn test_from_config_with_binlog_position() {
        let mut config = valid_connector_config();
        config.set("use.gtid", "false");
        config.set("binlog.filename", "mysql-bin.000003");
        config.set("binlog.position", "12345");

        let cfg = MySqlCdcConfig::from_config(&config).unwrap();
        assert!(!cfg.use_gtid);
        assert_eq!(cfg.binlog_filename, Some("mysql-bin.000003".to_string()));
        assert_eq!(cfg.binlog_position, Some(12345));
    }

    #[test]
    fn validate_rejects_conflicting_start_positions() {
        let mut cfg = valid_config();
        cfg.binlog_filename = Some("mysql-bin.000003".into());
        assert!(cfg.validate().unwrap_err().to_string().contains("use.gtid"));

        let mut cfg = valid_config();
        cfg.use_gtid = false;
        cfg.gtid_set = Some("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10".parse().unwrap());
        assert!(cfg.validate().unwrap_err().to_string().contains("gtid.set"));
    }

    #[test]
    fn target_database_is_derived_from_table_include() {
        assert_eq!(valid_config().target_database().unwrap(), "app");
    }
}
