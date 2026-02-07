//! Connector Manager: SQL-to-Runtime bridge.
//!
//! Accumulates DDL registrations (CREATE SOURCE/SINK/STREAM) and translates
//! them into live connector instances when `start()` is called.

use std::collections::HashMap;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::reference::RefreshMode;

use crate::error::DbError;
use crate::table_cache_mode::TableCacheMode;

/// Registration of a source from DDL.
#[derive(Debug, Clone)]
pub(crate) struct SourceRegistration {
    /// Source name.
    pub name: String,
    /// Connector type (e.g., "KAFKA", "POSTGRES").
    pub connector_type: Option<String>,
    /// Connector-specific options (e.g., topic, bootstrap.servers).
    pub connector_options: HashMap<String, String>,
    /// Format type (e.g., "JSON", "AVRO").
    pub format: Option<String>,
    /// Format-specific options.
    pub format_options: HashMap<String, String>,
}

/// Registration of a sink from DDL.
#[derive(Debug, Clone)]
pub(crate) struct SinkRegistration {
    /// Sink name.
    pub name: String,
    /// Input source or stream name (used for schema lookup and routing).
    #[allow(dead_code)]
    pub input: String,
    /// Connector type (e.g., "KAFKA", "POSTGRES").
    pub connector_type: Option<String>,
    /// Connector-specific options.
    pub connector_options: HashMap<String, String>,
    /// Format type (e.g., "JSON", "AVRO").
    pub format: Option<String>,
    /// Format-specific options.
    pub format_options: HashMap<String, String>,
    /// Optional WHERE filter expression as SQL text.
    pub filter_expr: Option<String>,
}

/// Registration of a stream from DDL.
#[derive(Debug, Clone)]
pub(crate) struct StreamRegistration {
    /// Stream name.
    pub name: String,
    /// SQL query that defines the stream.
    pub query_sql: String,
}

/// Registration of a reference/dimension table from DDL.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct TableRegistration {
    /// Table name.
    pub name: String,
    /// Primary key column name.
    pub primary_key: String,
    /// Connector type backing this table (e.g., "kafka" for compacted topics).
    pub connector_type: Option<String>,
    /// Connector-specific options.
    pub connector_options: HashMap<String, String>,
    /// Format type (e.g., "JSON", "AVRO").
    pub format: Option<String>,
    /// Format-specific options.
    pub format_options: HashMap<String, String>,
    /// How the table should be refreshed from the connector.
    pub refresh: Option<RefreshMode>,
    /// Cache mode for this table (Full, Partial, or None).
    pub cache_mode: Option<TableCacheMode>,
    /// Override for maximum LRU cache entries (used with Partial mode).
    pub cache_max_entries: Option<usize>,
    /// Storage backend: `None` or `"persistent"` for RocksDB-backed tables.
    pub storage: Option<String>,
}

/// Build a [`ConnectorConfig`] from a [`SourceRegistration`].
///
/// Normalizes `connector_type` to lowercase, copies all options,
/// and validates the format property (rejecting unknown formats
/// at build time instead of silently defaulting).
/// Map SQL-friendly option names to connector-native names.
fn normalize_option_key(key: &str) -> String {
    match key {
        "brokers" => "bootstrap.servers".to_string(),
        "group_id" => "group.id".to_string(),
        "offset_reset" => "auto.offset.reset".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn build_source_config(reg: &SourceRegistration) -> Result<ConnectorConfig, DbError> {
    let connector_type = reg.connector_type.as_deref().ok_or_else(|| {
        DbError::Connector(format!("Source '{}' has no connector type", reg.name))
    })?;

    let mut config = ConnectorConfig::new(connector_type.to_lowercase());
    for (k, v) in &reg.connector_options {
        config.set(normalize_option_key(k), v.clone());
    }

    if let Some(ref fmt_str) = reg.format {
        laminar_connectors::serde::Format::parse(&fmt_str.to_lowercase()).map_err(|e| {
            DbError::Connector(format!(
                "Invalid format '{}' for source '{}': {e}",
                fmt_str, reg.name,
            ))
        })?;
        config.set("format".to_string(), fmt_str.to_lowercase());
    }

    for (k, v) in &reg.format_options {
        config.set(format!("format.{k}"), v.clone());
    }

    Ok(config)
}

/// Build a [`ConnectorConfig`] from a [`SinkRegistration`].
///
/// Same normalization and validation as [`build_source_config`].
pub(crate) fn build_sink_config(reg: &SinkRegistration) -> Result<ConnectorConfig, DbError> {
    let connector_type = reg
        .connector_type
        .as_deref()
        .ok_or_else(|| DbError::Connector(format!("Sink '{}' has no connector type", reg.name)))?;

    let mut config = ConnectorConfig::new(connector_type.to_lowercase());
    for (k, v) in &reg.connector_options {
        config.set(normalize_option_key(k), v.clone());
    }

    if let Some(ref fmt_str) = reg.format {
        laminar_connectors::serde::Format::parse(&fmt_str.to_lowercase()).map_err(|e| {
            DbError::Connector(format!(
                "Invalid format '{}' for sink '{}': {e}",
                fmt_str, reg.name,
            ))
        })?;
        config.set("format".to_string(), fmt_str.to_lowercase());
    }

    for (k, v) in &reg.format_options {
        config.set(format!("format.{k}"), v.clone());
    }

    Ok(config)
}

/// Build a [`ConnectorConfig`] from a [`TableRegistration`].
///
/// Same normalization and validation as [`build_source_config`].
pub(crate) fn build_table_config(reg: &TableRegistration) -> Result<ConnectorConfig, DbError> {
    let connector_type = reg
        .connector_type
        .as_deref()
        .ok_or_else(|| DbError::Connector(format!("Table '{}' has no connector type", reg.name)))?;

    let mut config = ConnectorConfig::new(connector_type.to_lowercase());
    for (k, v) in &reg.connector_options {
        config.set(normalize_option_key(k), v.clone());
    }

    if let Some(ref fmt_str) = reg.format {
        laminar_connectors::serde::Format::parse(&fmt_str.to_lowercase()).map_err(|e| {
            DbError::Connector(format!(
                "Invalid format '{}' for table '{}': {e}",
                fmt_str, reg.name,
            ))
        })?;
        config.set("format".to_string(), fmt_str.to_lowercase());
    }

    for (k, v) in &reg.format_options {
        config.set(format!("format.{k}"), v.clone());
    }

    Ok(config)
}

/// Parse a refresh mode string from DDL `WITH (refresh = '...')`.
///
/// Supported values:
/// - `"snapshot_only"` / `"snapshot"` → `RefreshMode::SnapshotOnly`
/// - `"cdc"` / `"snapshot_plus_cdc"` → `RefreshMode::SnapshotPlusCdc`
/// - `"periodic:<seconds>"` → `RefreshMode::Periodic { interval }`
/// - `"manual"` → `RefreshMode::Manual`
pub(crate) fn parse_refresh_mode(s: &str) -> Result<RefreshMode, DbError> {
    let lower = s.to_lowercase();
    match lower.as_str() {
        "snapshot_only" | "snapshot" => Ok(RefreshMode::SnapshotOnly),
        "cdc" | "snapshot_plus_cdc" => Ok(RefreshMode::SnapshotPlusCdc),
        "manual" => Ok(RefreshMode::Manual),
        _ if lower.starts_with("periodic:") => {
            let secs_str = lower.strip_prefix("periodic:").unwrap();
            let secs: u64 = secs_str.parse().map_err(|_| {
                DbError::Connector(format!(
                    "Invalid periodic interval '{secs_str}': expected integer seconds"
                ))
            })?;
            Ok(RefreshMode::Periodic {
                interval: std::time::Duration::from_secs(secs),
            })
        }
        _ => Err(DbError::Connector(format!(
            "Unknown refresh mode '{s}': expected snapshot_only, cdc, periodic:<secs>, or manual"
        ))),
    }
}

/// Manages connector registrations from SQL DDL.
///
/// Accumulates source, sink, and stream registrations during the DDL phase,
/// then constructs the runtime pipeline when `build()` is called.
pub struct ConnectorManager {
    sources: HashMap<String, SourceRegistration>,
    sinks: HashMap<String, SinkRegistration>,
    streams: HashMap<String, StreamRegistration>,
    tables: HashMap<String, TableRegistration>,
}

impl ConnectorManager {
    /// Create a new empty connector manager.
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
            streams: HashMap::new(),
            tables: HashMap::new(),
        }
    }

    /// Register a source from DDL.
    pub fn register_source(&mut self, reg: SourceRegistration) {
        self.sources.insert(reg.name.clone(), reg);
    }

    /// Register a sink from DDL.
    pub fn register_sink(&mut self, reg: SinkRegistration) {
        self.sinks.insert(reg.name.clone(), reg);
    }

    /// Register a stream from DDL.
    pub fn register_stream(&mut self, reg: StreamRegistration) {
        self.streams.insert(reg.name.clone(), reg);
    }

    /// Remove a source registration.
    pub fn unregister_source(&mut self, name: &str) -> bool {
        self.sources.remove(name).is_some()
    }

    /// Remove a sink registration.
    pub fn unregister_sink(&mut self, name: &str) -> bool {
        self.sinks.remove(name).is_some()
    }

    /// Remove a stream registration.
    pub fn unregister_stream(&mut self, name: &str) -> bool {
        self.streams.remove(name).is_some()
    }

    /// Register a reference table from DDL.
    pub fn register_table(&mut self, reg: TableRegistration) {
        self.tables.insert(reg.name.clone(), reg);
    }

    /// Remove a table registration.
    pub fn unregister_table(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    /// Get all table registrations.
    pub fn tables(&self) -> &HashMap<String, TableRegistration> {
        &self.tables
    }

    /// Get registered table names.
    #[allow(dead_code)]
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get registered source names.
    #[allow(dead_code)]
    pub fn source_names(&self) -> Vec<String> {
        self.sources.keys().cloned().collect()
    }

    /// Get registered sink names.
    #[allow(dead_code)]
    pub fn sink_names(&self) -> Vec<String> {
        self.sinks.keys().cloned().collect()
    }

    /// Get registered stream names.
    #[allow(dead_code)]
    pub fn stream_names(&self) -> Vec<String> {
        self.streams.keys().cloned().collect()
    }

    /// Get a source registration.
    #[allow(dead_code)]
    pub fn get_source(&self, name: &str) -> Option<&SourceRegistration> {
        self.sources.get(name)
    }

    /// Get a sink registration.
    #[allow(dead_code)]
    pub fn get_sink(&self, name: &str) -> Option<&SinkRegistration> {
        self.sinks.get(name)
    }

    /// Total number of registrations.
    #[allow(dead_code)]
    pub fn registration_count(&self) -> usize {
        self.sources.len() + self.sinks.len() + self.streams.len() + self.tables.len()
    }

    /// Check if a connector type requires external runtime (e.g., Kafka).
    pub fn has_external_connectors(&self) -> bool {
        self.sources.values().any(|s| s.connector_type.is_some())
            || self.sinks.values().any(|s| s.connector_type.is_some())
            || self.tables.values().any(|t| t.connector_type.is_some())
    }

    /// Get all source registrations.
    pub fn sources(&self) -> &HashMap<String, SourceRegistration> {
        &self.sources
    }

    /// Get all sink registrations.
    pub fn sinks(&self) -> &HashMap<String, SinkRegistration> {
        &self.sinks
    }

    /// Get all stream registrations.
    pub fn streams(&self) -> &HashMap<String, StreamRegistration> {
        &self.streams
    }

    /// Clear all registrations.
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.sources.clear();
        self.sinks.clear();
        self.streams.clear();
        self.tables.clear();
    }
}

impl Default for ConnectorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConnectorManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorManager")
            .field("sources", &self.sources.len())
            .field("sinks", &self.sinks.len())
            .field("streams", &self.streams.len())
            .field("tables", &self.tables.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_source() {
        let mut mgr = ConnectorManager::new();
        mgr.register_source(SourceRegistration {
            name: "clicks".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::from([("topic".to_string(), "clicks".to_string())]),
            format: Some("JSON".to_string()),
            format_options: HashMap::new(),
        });
        assert_eq!(mgr.source_names(), vec!["clicks"]);
        assert!(mgr.has_external_connectors());
    }

    #[test]
    fn test_register_sink() {
        let mut mgr = ConnectorManager::new();
        mgr.register_sink(SinkRegistration {
            name: "output".to_string(),
            input: "events".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            filter_expr: None,
        });
        assert_eq!(mgr.sink_names(), vec!["output"]);
    }

    #[test]
    fn test_register_stream() {
        let mut mgr = ConnectorManager::new();
        mgr.register_stream(StreamRegistration {
            name: "agg_stream".to_string(),
            query_sql: "SELECT count(*) FROM events".to_string(),
        });
        assert_eq!(mgr.stream_names(), vec!["agg_stream"]);
    }

    #[test]
    fn test_unregister() {
        let mut mgr = ConnectorManager::new();
        mgr.register_source(SourceRegistration {
            name: "test".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        });
        assert!(mgr.unregister_source("test"));
        assert!(!mgr.unregister_source("test"));
    }

    #[test]
    fn test_registration_count() {
        let mut mgr = ConnectorManager::new();
        assert_eq!(mgr.registration_count(), 0);
        mgr.register_source(SourceRegistration {
            name: "s1".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        });
        mgr.register_sink(SinkRegistration {
            name: "k1".to_string(),
            input: "s1".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            filter_expr: None,
        });
        assert_eq!(mgr.registration_count(), 2);
    }

    #[test]
    fn test_no_external_connectors() {
        let mut mgr = ConnectorManager::new();
        mgr.register_source(SourceRegistration {
            name: "test".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        });
        assert!(!mgr.has_external_connectors());
    }

    #[test]
    fn test_clear() {
        let mut mgr = ConnectorManager::new();
        mgr.register_source(SourceRegistration {
            name: "test".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        });
        mgr.clear();
        assert_eq!(mgr.registration_count(), 0);
    }

    #[test]
    fn test_default_trait() {
        let mgr = ConnectorManager::default();
        assert_eq!(mgr.registration_count(), 0);
    }

    #[test]
    fn test_debug_format() {
        let mgr = ConnectorManager::new();
        let debug = format!("{mgr:?}");
        assert!(debug.contains("ConnectorManager"));
        assert!(debug.contains("sources: 0"));
    }

    #[test]
    fn test_get_source() {
        let mut mgr = ConnectorManager::new();
        assert!(mgr.get_source("test").is_none());
        mgr.register_source(SourceRegistration {
            name: "test".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        });
        let src = mgr.get_source("test").unwrap();
        assert_eq!(src.connector_type.as_deref(), Some("KAFKA"));
    }

    #[test]
    fn test_get_sink() {
        let mut mgr = ConnectorManager::new();
        assert!(mgr.get_sink("test").is_none());
        mgr.register_sink(SinkRegistration {
            name: "test".to_string(),
            input: "events".to_string(),
            connector_type: Some("POSTGRES".to_string()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            filter_expr: Some("id > 10".to_string()),
        });
        let sink = mgr.get_sink("test").unwrap();
        assert_eq!(sink.connector_type.as_deref(), Some("POSTGRES"));
        assert_eq!(sink.filter_expr.as_deref(), Some("id > 10"));
    }

    #[test]
    fn test_overwrite_registration() {
        let mut mgr = ConnectorManager::new();
        mgr.register_source(SourceRegistration {
            name: "test".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        });
        mgr.register_source(SourceRegistration {
            name: "test".to_string(),
            connector_type: Some("POSTGRES".to_string()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        });
        assert_eq!(mgr.source_names().len(), 1);
        assert_eq!(
            mgr.get_source("test").unwrap().connector_type.as_deref(),
            Some("POSTGRES")
        );
    }

    #[test]
    fn test_unregister_sink_and_stream() {
        let mut mgr = ConnectorManager::new();
        mgr.register_sink(SinkRegistration {
            name: "s1".to_string(),
            input: "src".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            filter_expr: None,
        });
        mgr.register_stream(StreamRegistration {
            name: "st1".to_string(),
            query_sql: "SELECT 1".to_string(),
        });
        assert!(mgr.unregister_sink("s1"));
        assert!(!mgr.unregister_sink("s1"));
        assert!(mgr.unregister_stream("st1"));
        assert!(!mgr.unregister_stream("st1"));
        assert_eq!(mgr.registration_count(), 0);
    }

    // ── Table registration tests ──

    #[test]
    fn test_register_table() {
        let mut mgr = ConnectorManager::new();
        mgr.register_table(TableRegistration {
            name: "instruments".to_string(),
            primary_key: "symbol".to_string(),
            connector_type: Some("kafka".to_string()),
            connector_options: HashMap::from([("topic".to_string(), "instruments".to_string())]),
            format: Some("JSON".to_string()),
            format_options: HashMap::new(),
            refresh: None,
            cache_mode: None,
            cache_max_entries: None,
            storage: None,
        });
        assert_eq!(mgr.table_names().len(), 1);
        assert!(mgr.has_external_connectors());
    }

    #[test]
    fn test_unregister_table() {
        let mut mgr = ConnectorManager::new();
        mgr.register_table(TableRegistration {
            name: "t".to_string(),
            primary_key: "id".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            refresh: None,
            cache_mode: None,
            cache_max_entries: None,
            storage: None,
        });
        assert!(mgr.unregister_table("t"));
        assert!(!mgr.unregister_table("t"));
    }

    #[test]
    fn test_table_in_registration_count() {
        let mut mgr = ConnectorManager::new();
        assert_eq!(mgr.registration_count(), 0);
        mgr.register_table(TableRegistration {
            name: "t".to_string(),
            primary_key: "id".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            refresh: None,
            cache_mode: None,
            cache_max_entries: None,
            storage: None,
        });
        assert_eq!(mgr.registration_count(), 1);
        mgr.clear();
        assert_eq!(mgr.registration_count(), 0);
    }

    // ── build_source_config / build_sink_config tests ──

    #[test]
    fn test_build_source_config_valid() {
        let reg = SourceRegistration {
            name: "clicks".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::from([
                ("topic".to_string(), "clicks".to_string()),
                (
                    "bootstrap.servers".to_string(),
                    "localhost:9092".to_string(),
                ),
            ]),
            format: Some("JSON".to_string()),
            format_options: HashMap::from([("include_schema".to_string(), "true".to_string())]),
        };
        let config = build_source_config(&reg).unwrap();
        assert_eq!(config.connector_type(), "kafka"); // normalized lowercase
        assert_eq!(config.get("topic"), Some("clicks"));
        assert_eq!(config.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(config.get("format"), Some("json"));
        assert_eq!(config.get("format.include_schema"), Some("true"));
    }

    #[test]
    fn test_build_source_config_missing_type() {
        let reg = SourceRegistration {
            name: "clicks".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        };
        let err = build_source_config(&reg).unwrap_err();
        assert!(err.to_string().contains("no connector type"));
    }

    #[test]
    fn test_build_source_config_invalid_format() {
        let reg = SourceRegistration {
            name: "clicks".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::new(),
            format: Some("BADFORMAT".to_string()),
            format_options: HashMap::new(),
        };
        let err = build_source_config(&reg).unwrap_err();
        assert!(err.to_string().contains("Invalid format"));
        assert!(err.to_string().contains("BADFORMAT"));
    }

    #[test]
    fn test_build_source_config_no_format() {
        let reg = SourceRegistration {
            name: "clicks".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        };
        let config = build_source_config(&reg).unwrap();
        assert_eq!(config.connector_type(), "kafka");
        assert_eq!(config.get("format"), None); // not set when absent
    }

    #[test]
    fn test_build_sink_config_valid() {
        let reg = SinkRegistration {
            name: "output".to_string(),
            input: "events".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::from([("topic".to_string(), "output".to_string())]),
            format: Some("JSON".to_string()),
            format_options: HashMap::new(),
            filter_expr: Some("id > 10".to_string()),
        };
        let config = build_sink_config(&reg).unwrap();
        assert_eq!(config.connector_type(), "kafka");
        assert_eq!(config.get("topic"), Some("output"));
        assert_eq!(config.get("format"), Some("json"));
    }

    #[test]
    fn test_build_sink_config_missing_type() {
        let reg = SinkRegistration {
            name: "output".to_string(),
            input: "events".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            filter_expr: None,
        };
        let err = build_sink_config(&reg).unwrap_err();
        assert!(err.to_string().contains("no connector type"));
    }

    #[test]
    fn test_build_sink_config_invalid_format() {
        let reg = SinkRegistration {
            name: "output".to_string(),
            input: "events".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::new(),
            format: Some("NOPE".to_string()),
            format_options: HashMap::new(),
            filter_expr: None,
        };
        let err = build_sink_config(&reg).unwrap_err();
        assert!(err.to_string().contains("Invalid format"));
    }

    #[test]
    fn test_build_source_config_case_insensitive_format() {
        // Avro, avro, AVRO should all work
        for fmt in ["avro", "AVRO", "Avro"] {
            let reg = SourceRegistration {
                name: "s".to_string(),
                connector_type: Some("kafka".to_string()),
                connector_options: HashMap::new(),
                format: Some(fmt.to_string()),
                format_options: HashMap::new(),
            };
            let config = build_source_config(&reg).unwrap();
            assert_eq!(config.get("format"), Some("avro"));
        }
    }

    // ── build_table_config tests ──

    #[test]
    fn test_build_table_config_valid() {
        let reg = TableRegistration {
            name: "instruments".to_string(),
            primary_key: "symbol".to_string(),
            connector_type: Some("KAFKA".to_string()),
            connector_options: HashMap::from([("topic".to_string(), "instruments".to_string())]),
            format: Some("JSON".to_string()),
            format_options: HashMap::new(),
            refresh: None,
            cache_mode: None,
            cache_max_entries: None,
            storage: None,
        };
        let config = build_table_config(&reg).unwrap();
        assert_eq!(config.connector_type(), "kafka");
        assert_eq!(config.get("topic"), Some("instruments"));
        assert_eq!(config.get("format"), Some("json"));
    }

    #[test]
    fn test_build_table_config_missing_type() {
        let reg = TableRegistration {
            name: "t".to_string(),
            primary_key: "id".to_string(),
            connector_type: None,
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            refresh: None,
            cache_mode: None,
            cache_max_entries: None,
            storage: None,
        };
        let err = build_table_config(&reg).unwrap_err();
        assert!(err.to_string().contains("no connector type"));
    }

    // ── parse_refresh_mode tests ──

    #[test]
    fn test_parse_refresh_mode_variants() {
        use std::time::Duration;

        assert_eq!(
            parse_refresh_mode("snapshot_only").unwrap(),
            RefreshMode::SnapshotOnly
        );
        assert_eq!(
            parse_refresh_mode("snapshot").unwrap(),
            RefreshMode::SnapshotOnly
        );
        assert_eq!(
            parse_refresh_mode("cdc").unwrap(),
            RefreshMode::SnapshotPlusCdc
        );
        assert_eq!(
            parse_refresh_mode("snapshot_plus_cdc").unwrap(),
            RefreshMode::SnapshotPlusCdc
        );
        assert_eq!(parse_refresh_mode("manual").unwrap(), RefreshMode::Manual);
        assert_eq!(
            parse_refresh_mode("periodic:60").unwrap(),
            RefreshMode::Periodic {
                interval: Duration::from_secs(60)
            }
        );
        assert_eq!(
            parse_refresh_mode("PERIODIC:30").unwrap(),
            RefreshMode::Periodic {
                interval: Duration::from_secs(30)
            }
        );
    }

    #[test]
    fn test_parse_refresh_mode_invalid() {
        assert!(parse_refresh_mode("bogus").is_err());
        assert!(parse_refresh_mode("periodic:abc").is_err());
    }
}
