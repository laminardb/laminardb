//! `PostgreSQL` CDC source connector configuration.
//!
//! Provides [`PostgresCdcConfig`] with all settings needed to connect to
//! a `PostgreSQL` database and stream logical replication changes.

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

    /// Path to CA certificate PEM file (for `VerifyCa` / `VerifyFull`).
    pub ca_cert_path: Option<String>,

    /// Path to client certificate PEM file (for mTLS).
    pub client_cert_path: Option<String>,

    /// Path to client private key PEM file (for mTLS).
    pub client_key_path: Option<String>,

    /// SNI hostname override (for proxy/load-balancer scenarios).
    pub sni_hostname: Option<String>,

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
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            sni_hostname: None,
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

        if let Some(ssl) = config.get_parsed::<SslMode>("ssl.mode")? {
            cfg.ssl_mode = ssl;
        }
        cfg.ca_cert_path = config.get("ssl.ca.cert.path").map(String::from);
        cfg.client_cert_path = config.get("ssl.client.cert.path").map(String::from);
        cfg.client_key_path = config.get("ssl.client.key.path").map(String::from);
        cfg.sni_hostname = config.get("ssl.sni.hostname").map(String::from);

        if let Some(lsn) = config.get_parsed::<Lsn>("start.lsn")? {
            cfg.start_lsn = Some(lsn);
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
        crate::config::require_non_empty(&self.host, "host")?;
        crate::config::require_non_empty(&self.database, "database")?;
        crate::config::require_non_empty(&self.slot_name, "slot.name")?;
        crate::config::require_non_empty(&self.publication, "publication")?;
        if self.max_poll_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.poll.records must be > 0".to_string(),
            ));
        }
        // VerifyCa/VerifyFull require a CA certificate path
        if matches!(self.ssl_mode, SslMode::VerifyCa | SslMode::VerifyFull)
            && self.ca_cert_path.is_none()
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "ssl.mode={} requires ssl.ca.cert.path",
                self.ssl_mode
            )));
        }
        // Client cert without key (or vice versa) is invalid
        if self.client_cert_path.is_some() != self.client_key_path.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "ssl.client.cert.path and ssl.client.key.path must both be set for mTLS"
                    .to_string(),
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

str_enum!(SslMode, lowercase_nodash, String, "unknown SSL mode",
    Disable => "disable";
    Prefer => "prefer";
    Require => "require";
    VerifyCa => "verify-ca", "verify_ca";
    VerifyFull => "verify-full", "verify_full"
);

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

str_enum!(SnapshotMode, lowercase_nodash, String, "unknown snapshot mode",
    Initial => "initial";
    Never => "never";
    Always => "always"
);

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

    // ── TLS cert path fields ──

    #[test]
    fn test_default_tls_fields_are_none() {
        let cfg = PostgresCdcConfig::default();
        assert!(cfg.ca_cert_path.is_none());
        assert!(cfg.client_cert_path.is_none());
        assert!(cfg.client_key_path.is_none());
        assert!(cfg.sni_hostname.is_none());
    }

    #[test]
    fn test_from_config_tls_cert_paths() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "localhost");
        config.set("database", "db");
        config.set("slot.name", "s");
        config.set("publication", "p");
        config.set("ssl.mode", "verify-full");
        config.set("ssl.ca.cert.path", "/certs/ca.pem");
        config.set("ssl.client.cert.path", "/certs/client.pem");
        config.set("ssl.client.key.path", "/certs/client-key.pem");
        config.set("ssl.sni.hostname", "db.example.com");

        let cfg = PostgresCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
        assert_eq!(cfg.ca_cert_path.as_deref(), Some("/certs/ca.pem"));
        assert_eq!(cfg.client_cert_path.as_deref(), Some("/certs/client.pem"));
        assert_eq!(
            cfg.client_key_path.as_deref(),
            Some("/certs/client-key.pem")
        );
        assert_eq!(cfg.sni_hostname.as_deref(), Some("db.example.com"));
    }

    #[test]
    fn test_validate_verify_ca_requires_ca_path() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.ssl_mode = SslMode::VerifyCa;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("ssl.ca.cert.path"));
    }

    #[test]
    fn test_validate_verify_full_requires_ca_path() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.ssl_mode = SslMode::VerifyFull;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("ssl.ca.cert.path"));
    }

    #[test]
    fn test_validate_verify_ca_with_ca_path_ok() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.ssl_mode = SslMode::VerifyCa;
        cfg.ca_cert_path = Some("/certs/ca.pem".to_string());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_validate_client_cert_without_key() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.client_cert_path = Some("/certs/client.pem".to_string());
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("mTLS"));
    }

    #[test]
    fn test_validate_client_key_without_cert() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.client_key_path = Some("/certs/client-key.pem".to_string());
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("mTLS"));
    }

    #[test]
    fn test_validate_client_cert_and_key_ok() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.client_cert_path = Some("/certs/client.pem".to_string());
        cfg.client_key_path = Some("/certs/client-key.pem".to_string());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_require_mode_no_ca_path_ok() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.ssl_mode = SslMode::Require;
        assert!(cfg.validate().is_ok());
    }
}
