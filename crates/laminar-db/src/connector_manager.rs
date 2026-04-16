//! Connector Manager: SQL-to-Runtime bridge.
//!
//! Accumulates DDL registrations (CREATE SOURCE/SINK/STREAM) and translates
//! them into live connector instances when `start()` is called.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::reference::RefreshMode;

use crate::error::DbError;

#[derive(Debug, Clone)]
pub(crate) struct SourceRegistration {
    pub name: String,
    pub connector_type: Option<String>,
    pub connector_options: HashMap<String, String>,
    pub format: Option<String>,
    pub format_options: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub(crate) struct SinkRegistration {
    pub name: String,
    /// Input source or stream name (schema lookup + routing).
    pub input: String,
    pub connector_type: Option<String>,
    pub connector_options: HashMap<String, String>,
    pub format: Option<String>,
    pub format_options: HashMap<String, String>,
    /// WHERE filter expression as SQL text.
    pub filter_expr: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamRegistration {
    pub name: String,
    pub query_sql: String,
    pub emit_clause: Option<laminar_sql::parser::EmitClause>,
    pub window_config: Option<laminar_sql::translator::WindowOperatorConfig>,
    pub order_config: Option<laminar_sql::translator::OrderOperatorConfig>,
}

#[derive(Debug, Clone)]
pub(crate) struct TableRegistration {
    pub name: String,
    pub primary_key: String,
    pub connector_type: Option<String>,
    pub connector_options: HashMap<String, String>,
    pub format: Option<String>,
    pub format_options: HashMap<String, String>,
    pub refresh: Option<RefreshMode>,
    pub cache_max_entries: Option<usize>,
}

/// Lowercase + replace underscores with hyphens.
pub(crate) fn normalize_connector_type(raw: &str) -> String {
    raw.to_lowercase().replace('_', "-")
}

fn normalize_option_key(key: &str) -> String {
    match key {
        "brokers" => "bootstrap.servers".to_string(),
        "group_id" => "group.id".to_string(),
        "offset_reset" => "auto.offset.reset".to_string(),
        other => other.to_string(),
    }
}

/// Build a `ConnectorConfig` from any registration that has connector fields.
fn build_connector_config(
    kind: &str,
    name: &str,
    connector_type: Option<&str>,
    connector_options: &HashMap<String, String>,
    format: Option<&str>,
    format_options: &HashMap<String, String>,
) -> Result<ConnectorConfig, DbError> {
    let ct = connector_type
        .ok_or_else(|| DbError::Connector(format!("{kind} '{name}' has no connector type")))?;
    let mut config = ConnectorConfig::new(normalize_connector_type(ct));
    for (k, v) in connector_options {
        config.set(normalize_option_key(k), v.clone());
    }
    if let Some(fmt_str) = format {
        let lower = fmt_str.to_lowercase();
        laminar_connectors::serde::Format::parse(&lower).map_err(|e| {
            DbError::Connector(format!(
                "Invalid format '{fmt_str}' for {kind} '{name}': {e}"
            ))
        })?;
        config.set("format".to_string(), lower);
    }
    for (k, v) in format_options {
        config.set(format!("format.{k}"), v.clone());
    }
    Ok(config)
}

pub(crate) fn build_source_config(reg: &SourceRegistration) -> Result<ConnectorConfig, DbError> {
    build_connector_config(
        "Source",
        &reg.name,
        reg.connector_type.as_deref(),
        &reg.connector_options,
        reg.format.as_deref(),
        &reg.format_options,
    )
}

pub(crate) fn build_sink_config(reg: &SinkRegistration) -> Result<ConnectorConfig, DbError> {
    build_connector_config(
        "Sink",
        &reg.name,
        reg.connector_type.as_deref(),
        &reg.connector_options,
        reg.format.as_deref(),
        &reg.format_options,
    )
}

pub(crate) fn build_table_config(reg: &TableRegistration) -> Result<ConnectorConfig, DbError> {
    build_connector_config(
        "Table",
        &reg.name,
        reg.connector_type.as_deref(),
        &reg.connector_options,
        reg.format.as_deref(),
        &reg.format_options,
    )
}

/// Parse DDL `WITH (refresh = '...')` into a `RefreshMode`.
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

/// Accumulates DDL registrations; pipeline lifecycle reads them at start.
pub struct ConnectorManager {
    sources: HashMap<String, SourceRegistration>,
    sinks: HashMap<String, SinkRegistration>,
    streams: HashMap<String, StreamRegistration>,
    tables: HashMap<String, TableRegistration>,
    /// Original DDL text for SHOW CREATE, keyed by object name.
    ddl_store: HashMap<String, String>,
}

impl ConnectorManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
            streams: HashMap::new(),
            tables: HashMap::new(),
            ddl_store: HashMap::new(),
        }
    }

    /// Store DDL text for SHOW CREATE.
    pub fn store_ddl(&mut self, name: &str, ddl: &str) {
        self.ddl_store.insert(name.to_string(), ddl.to_string());
    }

    /// Retrieve stored DDL text.
    pub fn get_ddl(&self, name: &str) -> Option<&str> {
        self.ddl_store.get(name).map(String::as_str)
    }

    /// Register a source.
    pub fn register_source(&mut self, reg: SourceRegistration) {
        self.sources.insert(reg.name.clone(), reg);
    }

    /// Register a sink.
    pub fn register_sink(&mut self, reg: SinkRegistration) {
        self.sinks.insert(reg.name.clone(), reg);
    }

    /// Register a stream.
    pub fn register_stream(&mut self, reg: StreamRegistration) {
        self.streams.insert(reg.name.clone(), reg);
    }

    /// Returns `true` if it existed.
    pub fn unregister_source(&mut self, name: &str) -> bool {
        self.sources.remove(name).is_some()
    }

    /// Returns `true` if it existed.
    pub fn unregister_sink(&mut self, name: &str) -> bool {
        self.sinks.remove(name).is_some()
    }

    /// Returns `true` if it existed.
    pub fn unregister_stream(&mut self, name: &str) -> bool {
        self.streams.remove(name).is_some()
    }

    /// Register a reference/dimension table.
    pub fn register_table(&mut self, reg: TableRegistration) {
        self.tables.insert(reg.name.clone(), reg);
    }

    /// Returns `true` if it existed.
    pub fn unregister_table(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    /// All table registrations.
    pub fn tables(&self) -> &HashMap<String, TableRegistration> {
        &self.tables
    }

    /// True if any registration has a non-`None` connector type.
    pub fn has_external_connectors(&self) -> bool {
        self.sources.values().any(|s| s.connector_type.is_some())
            || self.sinks.values().any(|s| s.connector_type.is_some())
            || self.tables.values().any(|t| t.connector_type.is_some())
    }

    /// All source registrations.
    pub fn sources(&self) -> &HashMap<String, SourceRegistration> {
        &self.sources
    }

    /// All sink registrations.
    pub fn sinks(&self) -> &HashMap<String, SinkRegistration> {
        &self.sinks
    }

    /// All stream registrations.
    pub fn streams(&self) -> &HashMap<String, StreamRegistration> {
        &self.streams
    }
}

#[cfg(test)]
impl ConnectorManager {
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn source_names(&self) -> Vec<String> {
        self.sources.keys().cloned().collect()
    }

    pub fn sink_names(&self) -> Vec<String> {
        self.sinks.keys().cloned().collect()
    }

    pub fn stream_names(&self) -> Vec<String> {
        self.streams.keys().cloned().collect()
    }

    pub fn get_source(&self, name: &str) -> Option<&SourceRegistration> {
        self.sources.get(name)
    }

    pub fn get_sink(&self, name: &str) -> Option<&SinkRegistration> {
        self.sinks.get(name)
    }

    pub fn registration_count(&self) -> usize {
        self.sources.len() + self.sinks.len() + self.streams.len() + self.tables.len()
    }

    pub fn clear(&mut self) {
        self.sources.clear();
        self.sinks.clear();
        self.streams.clear();
        self.tables.clear();
        self.ddl_store.clear();
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
            .field("ddl_entries", &self.ddl_store.len())
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
            emit_clause: None,
            window_config: None,
            order_config: None,
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
            emit_clause: None,
            window_config: None,
            order_config: None,
        });
        assert!(mgr.unregister_sink("s1"));
        assert!(!mgr.unregister_sink("s1"));
        assert!(mgr.unregister_stream("st1"));
        assert!(!mgr.unregister_stream("st1"));
        assert_eq!(mgr.registration_count(), 0);
    }

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
            cache_max_entries: None,
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
            cache_max_entries: None,
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
            cache_max_entries: None,
        });
        assert_eq!(mgr.registration_count(), 1);
        mgr.clear();
        assert_eq!(mgr.registration_count(), 0);
    }

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
            cache_max_entries: None,
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
            cache_max_entries: None,
        };
        let err = build_table_config(&reg).unwrap_err();
        assert!(err.to_string().contains("no connector type"));
    }

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
                interval: Duration::from_mins(1)
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

    #[test]
    fn test_normalize_connector_type_variants() {
        // All forms of "delta-lake" must resolve to the same canonical name.
        assert_eq!(normalize_connector_type("delta-lake"), "delta-lake");
        assert_eq!(normalize_connector_type("delta_lake"), "delta-lake");
        assert_eq!(normalize_connector_type("DELTA_LAKE"), "delta-lake");
        assert_eq!(normalize_connector_type("DELTA-LAKE"), "delta-lake");
        assert_eq!(normalize_connector_type("Delta_Lake"), "delta-lake");
    }

    #[test]
    fn test_normalize_connector_type_simple_names() {
        // Names without hyphens or underscores are just lowercased.
        assert_eq!(normalize_connector_type("kafka"), "kafka");
        assert_eq!(normalize_connector_type("KAFKA"), "kafka");
        assert_eq!(normalize_connector_type("websocket"), "websocket");
    }

    #[test]
    fn test_normalize_connector_type_hyphenated() {
        assert_eq!(normalize_connector_type("postgres-cdc"), "postgres-cdc");
        assert_eq!(normalize_connector_type("POSTGRES_CDC"), "postgres-cdc");
        assert_eq!(normalize_connector_type("mysql-cdc"), "mysql-cdc");
        assert_eq!(normalize_connector_type("MYSQL_CDC"), "mysql-cdc");
        assert_eq!(normalize_connector_type("postgres-sink"), "postgres-sink");
        assert_eq!(normalize_connector_type("POSTGRES_SINK"), "postgres-sink");
    }

    #[test]
    fn test_build_source_config_normalizes_hyphenated_type() {
        let reg = SourceRegistration {
            name: "cdc".to_string(),
            connector_type: Some("POSTGRES_CDC".to_string()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
        };
        let config = build_source_config(&reg).unwrap();
        assert_eq!(config.connector_type(), "postgres-cdc");
    }

    #[test]
    fn test_build_sink_config_normalizes_hyphenated_type() {
        let reg = SinkRegistration {
            name: "lake".to_string(),
            input: "events".to_string(),
            connector_type: Some("DELTA_LAKE".to_string()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            filter_expr: None,
        };
        let config = build_sink_config(&reg).unwrap();
        assert_eq!(config.connector_type(), "delta-lake");
    }
}
