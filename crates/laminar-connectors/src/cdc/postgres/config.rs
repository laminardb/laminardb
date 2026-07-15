//! `PostgreSQL` CDC source connector configuration.
//!
//! Provides [`PostgresCdcConfig`] with all settings needed to connect to
//! a `PostgreSQL` database and stream logical replication changes.

use std::path::PathBuf;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

const REMOVED_CONFIG_KEYS: &[&str] = &[
    "backpressure.high.watermark",
    "keepalive.interval.ms",
    "max.buffered.events",
    "max.poll.records",
    "poll.timeout.ms",
    "snapshot.mode",
    "start.lsn",
    "wal.sender.timeout.ms",
];

const DEFAULT_BUFFERED_BYTES: usize = 256 * 1024 * 1024;
const MIN_BUFFERED_BYTES: usize = 1024 * 1024;
const MAX_BUFFERED_BYTES: usize = 4 * 1024 * 1024 * 1024;
const WORKING_SET_STAGES: usize = 3;

const ALLOWED_CONFIG_KEYS: &[&str] = &[
    "_arrow_schema",
    "database",
    "host",
    "laminar.source.name",
    "max.buffered.bytes",
    "password",
    "port",
    "publication",
    "slot.name",
    "ssl.ca.cert.path",
    "ssl.mode",
    "table.exclude",
    "table.include",
    "username",
];

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

    /// Optional PEM file containing trusted CA certificates.
    pub ssl_ca_cert_path: Option<PathBuf>,

    // ── Replication ──
    /// Name of the logical replication slot.
    pub slot_name: String,

    /// Name of the publication to subscribe to.
    pub publication: String,

    // ── Schema ──
    /// Tables to include (empty = all tables in publication).
    pub table_include: Vec<String>,

    /// Tables to exclude from replication.
    pub table_exclude: Vec<String>,

    /// Total connector-owned payload budget across raw WAL, decoded state, and Arrow construction.
    pub max_buffered_bytes: usize,
}

impl Default for PostgresCdcConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: "postgres".to_string(),
            username: "postgres".to_string(),
            password: None,
            ssl_mode: SslMode::VerifyFull,
            ssl_ca_cert_path: None,
            slot_name: "laminar_slot".to_string(),
            publication: "laminar_pub".to_string(),
            table_include: Vec::new(),
            table_exclude: Vec::new(),
            max_buffered_bytes: DEFAULT_BUFFERED_BYTES,
        }
    }
}

impl PostgresCdcConfig {
    /// Decoded-stage high watermark used to stop admitting raw WAL before the hard limit.
    #[must_use]
    pub(super) fn decoded_high_watermark_bytes(&self) -> usize {
        self.decoded_event_bytes()
            .saturating_sub(self.decoded_event_bytes() / 5)
    }

    /// Raw pgwire/frame ownership share of the private working-set limit.
    #[must_use]
    pub(crate) fn raw_wal_bytes(&self) -> usize {
        self.max_buffered_bytes / WORKING_SET_STAGES
    }

    /// Decoded transaction ownership share of the private working-set limit.
    #[must_use]
    pub(crate) fn decoded_event_bytes(&self) -> usize {
        self.max_buffered_bytes / WORKING_SET_STAGES
    }

    /// Arrow construction share, including division remainder.
    #[must_use]
    pub(crate) fn arrow_build_bytes(&self) -> usize {
        self.max_buffered_bytes
            .saturating_sub(self.raw_wal_bytes())
            .saturating_sub(self.decoded_event_bytes())
    }

    pub(crate) fn normalize_table_filters(&mut self) {
        normalize_table_list(&mut self.table_include);
        normalize_table_list(&mut self.table_exclude);
    }

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

    /// Builds the typed control-plane connection configuration.
    ///
    /// Using setters keeps credentials and database names out of libpq-style
    /// tokenization, so whitespace, quotes, and backslashes are data rather
    /// than connection-string syntax.
    pub(super) fn control_connection_config(
        &self,
    ) -> Result<tokio_postgres::Config, ConnectorError> {
        self.validate()?;

        let mut config = tokio_postgres::Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.username)
            .ssl_mode(match self.ssl_mode {
                SslMode::Disable => tokio_postgres::config::SslMode::Disable,
                SslMode::VerifyFull => tokio_postgres::config::SslMode::Require,
            })
            .connect_timeout(super::postgres_io::CONNECT_TIMEOUT);
        if let Some(password) = &self.password {
            config.password(password);
        }
        Ok(config)
    }

    /// Parses configuration from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if required keys are missing or values are
    /// invalid.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        Self::reject_removed_keys(config)?;
        config.reject_unknown_properties(ALLOWED_CONFIG_KEYS, "PostgreSQL CDC")?;

        let mut cfg = Self {
            host: config.require("host")?.to_string(),
            database: config.require("database")?.to_string(),
            slot_name: config.require("slot.name")?.to_string(),
            publication: config.require("publication")?.to_string(),
            ssl_mode: config
                .get_parsed::<SslMode>("ssl.mode")?
                .unwrap_or_default(),
            ..Self::default()
        };

        if let Some(port) = config.get("port") {
            cfg.port = crate::config::parse_port(port)?;
        }
        if let Some(user) = config.get("username") {
            cfg.username = user.to_string();
        }
        cfg.password = config.get("password").map(String::from);
        cfg.ssl_ca_cert_path = config.get("ssl.ca.cert.path").map(PathBuf::from);

        if let Some(tables) = config.get("table.include") {
            cfg.table_include = tables.split(',').map(str::to_string).collect();
        }
        if let Some(tables) = config.get("table.exclude") {
            cfg.table_exclude = tables.split(',').map(str::to_string).collect();
        }
        if let Some(max) = config.get_parsed::<usize>("max.buffered.bytes")? {
            cfg.max_buffered_bytes = max;
        }
        cfg.normalize_table_filters();
        cfg.validate()?;
        Ok(cfg)
    }

    fn reject_removed_keys(config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if let Some(key) = REMOVED_CONFIG_KEYS
            .iter()
            .find(|key| config.get(key).is_some())
        {
            let reason = match *key {
                "start.lsn" => {
                    "recovery cursors are owned by a validated engine checkpoint or the durable slot"
                }
                "max.buffered.events" => {
                    "resource ownership is bounded by max.buffered.bytes instead"
                }
                _ => "the connector did not execute it",
            };
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL CDC property '{key}' is not supported: {reason}"
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
        crate::config::require_non_empty(&self.database, "database")?;
        crate::config::require_non_empty(&self.username, "username")?;
        crate::config::require_non_empty(&self.slot_name, "slot.name")?;
        crate::config::require_non_empty(&self.publication, "publication")?;
        for (value, label) in [
            (&self.host, "host"),
            (&self.database, "database"),
            (&self.username, "username"),
            (&self.slot_name, "slot.name"),
            (&self.publication, "publication"),
        ] {
            if value.contains('\0') {
                return Err(ConnectorError::ConfigurationError(format!(
                    "{label} must not contain NUL"
                )));
            }
        }
        if self.slot_name.len() > 63
            || !self
                .slot_name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ConnectorError::ConfigurationError(
                "slot.name must be at most 63 bytes and contain only lower-case ASCII letters, digits, or underscore"
                    .into(),
            ));
        }
        if self.publication.len() > 63 {
            return Err(ConnectorError::ConfigurationError(
                "publication must be at most 63 bytes".into(),
            ));
        }
        if self
            .password
            .as_deref()
            .is_some_and(|value| value.contains('\0'))
        {
            return Err(ConnectorError::ConfigurationError(
                "password must not contain NUL".into(),
            ));
        }
        if self.port == 0 {
            return Err(ConnectorError::ConfigurationError(
                "port must be > 0".to_string(),
            ));
        }
        if !(MIN_BUFFERED_BYTES..=MAX_BUFFERED_BYTES).contains(&self.max_buffered_bytes) {
            return Err(ConnectorError::ConfigurationError(format!(
                "max.buffered.bytes must be between {MIN_BUFFERED_BYTES} and {MAX_BUFFERED_BYTES}"
            )));
        }
        if self
            .ssl_ca_cert_path
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ConnectorError::ConfigurationError(
                "ssl.ca.cert.path must not be empty".to_string(),
            ));
        }
        if self.ssl_mode == SslMode::Disable && self.ssl_ca_cert_path.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "ssl.ca.cert.path requires ssl.mode=verify-full".to_string(),
            ));
        }
        for (label, tables) in [
            ("table.include", &self.table_include),
            ("table.exclude", &self.table_exclude),
        ] {
            for table in tables {
                let Some((schema, relation)) = table.split_once('.') else {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "{label} entry '{table}' must be schema-qualified as schema.table"
                    )));
                };
                if table.trim() != table || schema.is_empty() || relation.is_empty() {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "{label} entry '{table}' must contain nonempty schema and table names"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Returns whether a table should be included based on include/exclude lists.
    #[must_use]
    pub(crate) fn should_include_table(&self, table: &str) -> bool {
        debug_assert!(self.table_include.is_sorted());
        debug_assert!(self.table_exclude.is_sorted());
        if self
            .table_exclude
            .binary_search_by(|candidate| candidate.as_str().cmp(table))
            .is_ok()
        {
            return false;
        }
        if self.table_include.is_empty() {
            return true;
        }
        self.table_include
            .binary_search_by(|candidate| candidate.as_str().cmp(table))
            .is_ok()
    }
}

fn normalize_table_list(tables: &mut Vec<String>) {
    for table in tables.iter_mut() {
        *table = table.trim().to_string();
    }
    tables.retain(|table| !table.is_empty());
    tables.sort_unstable();
    tables.dedup();
}

pub use crate::connector::PostgresSslMode as SslMode;

#[cfg(test)]
mod tests {
    use super::*;

    fn connector_config() -> ConnectorConfig {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "localhost");
        config.set("database", "db");
        config.set("slot.name", "s");
        config.set("publication", "p");
        config.set("ssl.mode", "disable");
        config
    }

    #[test]
    fn test_default_config() {
        let cfg = PostgresCdcConfig::default();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.database, "postgres");
        assert_eq!(cfg.slot_name, "laminar_slot");
        assert_eq!(cfg.publication, "laminar_pub");
        assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn replication_identity_rejects_invalid_slot_and_nul() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.slot_name = "Mixed-Case".into();
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("slot.name"));

        cfg.slot_name = "valid_slot".into();
        cfg.publication = "bad\0publication".into();
        assert!(cfg.validate().unwrap_err().to_string().contains("NUL"));
    }

    #[test]
    fn test_new_config() {
        let cfg = PostgresCdcConfig::new("db.example.com", "mydb", "my_slot", "my_pub");
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.slot_name, "my_slot");
        assert_eq!(cfg.publication, "my_pub");
        assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
    }

    #[test]
    fn typed_control_config_preserves_adversarial_values() {
        let mut cfg =
            PostgresCdcConfig::new(" db\\host' ", " db name'\\ ", "valid_slot", "publication");
        cfg.username = " user name'\\ ".into();
        cfg.password = Some(" password with 'quotes' and \\slashes\\ ".into());

        let control = cfg.control_connection_config().unwrap();
        assert_eq!(
            control.get_hosts(),
            &[tokio_postgres::config::Host::Tcp(" db\\host' ".into())]
        );
        assert_eq!(control.get_ports(), &[5432]);
        assert_eq!(control.get_dbname(), Some(" db name'\\ "));
        assert_eq!(control.get_user(), Some(" user name'\\ "));
        assert_eq!(
            control.get_password(),
            Some(" password with 'quotes' and \\slashes\\ ".as_bytes())
        );
        assert_eq!(
            control.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        );
        assert_eq!(
            control.get_connect_timeout().copied(),
            Some(crate::cdc::postgres::postgres_io::CONNECT_TIMEOUT)
        );
    }

    #[test]
    fn typed_control_config_maps_disabled_tls_exactly() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.ssl_mode = SslMode::Disable;
        assert_eq!(
            cfg.control_connection_config().unwrap().get_ssl_mode(),
            tokio_postgres::config::SslMode::Disable
        );
    }

    #[test]
    fn test_from_connector_config() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "pg.local");
        config.set("database", "testdb");
        config.set("slot.name", "test_slot");
        config.set("publication", "test_pub");
        config.set("ssl.mode", "disable");
        config.set("port", "5433");
        config.set("max.buffered.bytes", "67108864");

        let cfg = PostgresCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.host, "pg.local");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.database, "testdb");
        assert_eq!(cfg.max_buffered_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn total_byte_budget_is_partitioned_without_loss() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.max_buffered_bytes = MIN_BUFFERED_BYTES;
        assert_eq!(
            cfg.decoded_high_watermark_bytes(),
            cfg.decoded_event_bytes() - cfg.decoded_event_bytes() / 5
        );
        assert_eq!(
            cfg.raw_wal_bytes() + cfg.decoded_event_bytes() + cfg.arrow_build_bytes(),
            MIN_BUFFERED_BYTES
        );
    }

    #[test]
    fn total_byte_budget_rejects_values_outside_the_operational_range() {
        for bytes in [MIN_BUFFERED_BYTES - 1, MAX_BUFFERED_BYTES + 1] {
            let mut cfg = PostgresCdcConfig::default();
            cfg.max_buffered_bytes = bytes;
            assert!(cfg.validate().is_err(), "{bytes}");
        }
    }

    #[test]
    fn test_from_config_missing_required() {
        let config = ConnectorConfig::new("postgres-cdc");
        assert!(PostgresCdcConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_from_config_invalid_port() {
        let mut config = connector_config();
        config.set("port", "not_a_number");
        assert!(PostgresCdcConfig::from_config(&config).is_err());
    }

    #[test]
    fn omitted_ssl_mode_uses_verified_tls() {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "localhost");
        config.set("database", "db");
        config.set("slot.name", "s");
        config.set("publication", "p");
        let config = PostgresCdcConfig::from_config(&config).unwrap();
        assert_eq!(config.ssl_mode, SslMode::VerifyFull);
    }

    #[test]
    fn unknown_properties_are_rejected_deterministically() {
        let mut config = connector_config();
        config.set("z.invalid", "1");
        config.set("a.invalid", "2");
        let error = PostgresCdcConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("a.invalid"), "{error}");
    }

    #[test]
    fn engine_metadata_properties_are_admitted() {
        let mut config = connector_config();
        config.set("laminar.source.name", "orders");
        config.set("_arrow_schema", "engine-owned");
        PostgresCdcConfig::from_config(&config).unwrap();
    }

    #[test]
    fn test_validate_empty_host() {
        let mut cfg = PostgresCdcConfig::default();
        cfg.host = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn removed_properties_are_rejected_explicitly() {
        for key in REMOVED_CONFIG_KEYS {
            let mut config = ConnectorConfig::new("postgres-cdc");
            config.set("host", "localhost");
            config.set("database", "db");
            config.set("slot.name", "s");
            config.set("publication", "p");
            config.set(*key, "removed-value");
            let error = PostgresCdcConfig::from_config(&config).unwrap_err();
            assert!(error.to_string().contains(key));
        }
    }

    #[test]
    fn test_ssl_mode_fromstr() {
        assert_eq!("disable".parse::<SslMode>().unwrap(), SslMode::Disable);
        assert_eq!(
            "verify-full".parse::<SslMode>().unwrap(),
            SslMode::VerifyFull
        );
        for rejected in [
            "off",
            "prefer",
            "require",
            "verify-ca",
            "verify_full",
            "verifyfull",
        ] {
            assert!(rejected.parse::<SslMode>().is_err(), "{rejected}");
        }
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
        cfg.normalize_table_filters();
        assert!(cfg.should_include_table("public.users"));
        assert!(!cfg.should_include_table("public.logs"));

        // Exclude overrides include
        cfg.table_exclude = vec!["public.users".to_string()];
        assert!(!cfg.should_include_table("public.users"));
    }

    #[test]
    fn manual_start_lsn_is_rejected() {
        let mut config = connector_config();
        config.set("start.lsn", "0/1234ABCD");
        let error = PostgresCdcConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("start.lsn"));
    }

    #[test]
    fn test_from_config_table_include() {
        let mut config = connector_config();
        config.set("table.include", "public.users, public.orders");

        let cfg = PostgresCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.table_include, vec!["public.orders", "public.users"]);
    }

    #[test]
    fn table_filters_are_trimmed_nonempty_sorted_and_deduplicated_once() {
        let mut config = connector_config();
        config.set(
            "table.include",
            " public.users,public.orders,, public.users,   ",
        );
        config.set(
            "table.exclude",
            " public.audit, ,public.archive,public.audit ",
        );

        let cfg = PostgresCdcConfig::from_config(&config).unwrap();
        assert_eq!(cfg.table_include, vec!["public.orders", "public.users"]);
        assert_eq!(cfg.table_exclude, vec!["public.archive", "public.audit"]);
        assert!(cfg.should_include_table("public.users"));
        assert!(!cfg.should_include_table("public.audit"));
        assert!(!cfg.should_include_table("users"));
    }

    #[test]
    fn table_filters_reject_unqualified_or_empty_components() {
        for table in ["users", ".users", "public."] {
            let mut config = connector_config();
            config.set("table.include", table);
            let error = PostgresCdcConfig::from_config(&config).unwrap_err();
            assert!(error.to_string().contains("schema"), "{table}: {error}");
        }
    }

    #[test]
    fn custom_ca_is_admitted_for_verified_tls() {
        let mut config = connector_config();
        config.set("ssl.mode", "verify-full");
        config.set("ssl.ca.cert.path", "/certs/ca.pem");
        let parsed = PostgresCdcConfig::from_config(&config).unwrap();
        assert_eq!(
            parsed.ssl_ca_cert_path,
            Some(PathBuf::from("/certs/ca.pem"))
        );
    }

    #[test]
    fn plaintext_rejects_unused_ca_path() {
        let mut config = connector_config();
        config.set("ssl.ca.cert.path", "/certs/ca.pem");
        let error = PostgresCdcConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("ssl.mode=verify-full"));
    }
}
