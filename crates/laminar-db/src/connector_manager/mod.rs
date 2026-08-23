//! Connector Manager: SQL-to-Runtime bridge.
//!
//! Accumulates DDL registrations (CREATE SOURCE/SINK/STREAM) and translates
//! them into live connector instances when `start()` is called.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use laminar_connectors::config::ConnectorConfig;

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
    pub input: String,
    pub query_inputs: Vec<String>,
    pub connector_type: Option<String>,
    pub connector_options: HashMap<String, String>,
    pub format: Option<String>,
    pub format_options: HashMap<String, String>,
    pub filter_expr: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamRegistration {
    pub name: String,
    pub query_sql: String,
    pub emit_clause: Option<laminar_sql::parser::EmitClause>,
    pub window_config: Option<laminar_sql::translator::WindowOperatorConfig>,
    pub order_config: Option<laminar_sql::translator::OrderOperatorConfig>,
    pub join_config: Option<Vec<laminar_sql::translator::JoinOperatorConfig>>,
    pub has_analytic: bool,
    pub has_frame: bool,
    /// Marks this MV to emit a dirty-only changelog into a keyed `Upsert` store. Decided at DDL
    /// time (`incremental_emit` flag + terminal non-windowed agg); drives operator + store mode.
    pub incremental: bool,
    /// Planner-owned stable output distribution for a potentially subscribable stream.
    pub subscription_output: Option<crate::subscription::distribution::PlannedSubscriptionOutput>,
    /// Durable object incarnation from the authoritative catalog manifest.
    pub catalog_generation: u64,
    /// Fully bound runtime certificate; populated before cluster graph construction.
    #[cfg_attr(not(feature = "cluster"), allow(dead_code))]
    pub subscription_certificate: Option<laminar_core::checkpoint::OutputDistributionCertificate>,
}

#[derive(Debug, Clone)]
pub(crate) struct TableRegistration {
    pub name: String,
    pub primary_key: String,
    pub connector_type: Option<String>,
    pub connector_options: HashMap<String, String>,
    pub format: Option<String>,
    pub format_options: HashMap<String, String>,
    /// Whether misses are served directly by a lookup-source factory.
    pub on_demand: bool,
    pub cache_max_bytes: Option<usize>,
    pub cache_ttl: Option<std::time::Duration>,
}

/// Connector identifiers are ASCII case-insensitive; punctuation remains provider-owned.
pub(crate) fn normalize_connector_type(raw: &str) -> String {
    raw.to_ascii_lowercase()
}

pub(crate) fn validate_connector_format_options(
    kind: &str,
    connector_options: &HashMap<String, String>,
    format: Option<&str>,
    format_options: &HashMap<String, String>,
) -> Result<(), DbError> {
    if format.is_none() && !format_options.is_empty() {
        return Err(DbError::Connector(format!(
            "{kind} format options require an explicit FORMAT clause"
        )));
    }
    if connector_options
        .keys()
        .chain(format_options.keys())
        .any(|key| key.eq_ignore_ascii_case("format"))
    {
        return Err(DbError::Connector(format!(
            "{kind} option 'format' is unsupported; declare the format with the FORMAT clause"
        )));
    }
    if let Some(key) = format_options.keys().find(|format_key| {
        connector_options
            .keys()
            .any(|connector_key| connector_key.eq_ignore_ascii_case(format_key))
    }) {
        return Err(DbError::Connector(format!(
            "{kind} option '{key}' is declared in both connector options and FORMAT WITH"
        )));
    }
    let reserved: &[&str] = if kind.eq_ignore_ascii_case("source") {
        &["laminar.source.name", "_arrow_schema"]
    } else if kind.eq_ignore_ascii_case("sink") {
        &["delivery.guarantee", "_arrow_schema"]
    } else {
        &[]
    };
    if let Some(key) = connector_options
        .keys()
        .chain(format_options.keys())
        .find(|key| {
            reserved
                .iter()
                .any(|reserved| key.eq_ignore_ascii_case(reserved))
        })
    {
        return Err(DbError::Connector(format!(
            "{kind} option '{key}' is owned by the runtime and cannot be configured"
        )));
    }
    Ok(())
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
    validate_connector_format_options(kind, connector_options, format, format_options)?;
    let mut config = ConnectorConfig::new(normalize_connector_type(ct));
    for (k, v) in connector_options {
        config.set(k.clone(), v.clone());
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
        config.set(k.clone(), v.clone());
    }
    Ok(config)
}

pub(crate) fn build_source_config(reg: &SourceRegistration) -> Result<ConnectorConfig, DbError> {
    let mut config = build_connector_config(
        "Source",
        &reg.name,
        reg.connector_type.as_deref(),
        &reg.connector_options,
        reg.format.as_deref(),
        &reg.format_options,
    )?;
    config.set("laminar.source.name", reg.name.clone());
    Ok(config)
}

pub(crate) fn build_sink_config(
    reg: &SinkRegistration,
    delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee,
) -> Result<ConnectorConfig, DbError> {
    let mut config = build_connector_config(
        "Sink",
        &reg.name,
        reg.connector_type.as_deref(),
        &reg.connector_options,
        reg.format.as_deref(),
        &reg.format_options,
    )?;
    // Internal connector behavior follows the one pipeline-wide delivery contract.
    config.set("delivery.guarantee", delivery_guarantee.to_string());
    Ok(config)
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

/// Accumulates DDL registrations; pipeline lifecycle reads them at start.
pub struct ConnectorManager {
    sources: HashMap<String, SourceRegistration>,
    sinks: HashMap<String, SinkRegistration>,
    streams: HashMap<String, StreamRegistration>,
    tables: HashMap<String, TableRegistration>,
    ddl_store: HashMap<String, String>,
    // Creation order for dependency-safe catalog manifest replay.
    ddl_order: Vec<String>,
}

impl ConnectorManager {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
            sinks: HashMap::new(),
            streams: HashMap::new(),
            tables: HashMap::new(),
            ddl_store: HashMap::new(),
            ddl_order: Vec::new(),
        }
    }

    /// Store DDL text for SHOW CREATE. OR REPLACE updates in place without reordering.
    pub fn store_ddl(&mut self, name: &str, ddl: &str) {
        if self
            .ddl_store
            .insert(name.to_string(), ddl.to_string())
            .is_none()
        {
            self.ddl_order.push(name.to_string());
        }
    }

    pub fn get_ddl(&self, name: &str) -> Option<&str> {
        self.ddl_store.get(name).map(String::as_str)
    }

    pub fn remove_ddl(&mut self, name: &str) {
        if self.ddl_store.remove(name).is_some() {
            self.ddl_order.retain(|n| n != name);
        }
    }

    /// Returns stored DDL in creation order for catalog manifest replay.
    #[cfg(feature = "cluster")]
    pub fn ordered_ddl(&self) -> Vec<(String, String, u64)> {
        self.ddl_order
            .iter()
            .filter_map(|name| {
                self.ddl_store.get(name).map(|ddl| {
                    let generation = self
                        .streams
                        .get(name)
                        .map_or(1, |stream| stream.catalog_generation);
                    (name.clone(), ddl.clone(), generation)
                })
            })
            .collect()
    }

    pub fn register_source(&mut self, reg: SourceRegistration) {
        self.sources.insert(reg.name.clone(), reg);
    }

    pub fn register_sink(&mut self, reg: SinkRegistration) {
        self.sinks.insert(reg.name.clone(), reg);
    }

    pub fn register_stream(&mut self, reg: StreamRegistration) {
        self.streams.insert(reg.name.clone(), reg);
    }

    /// Apply authoritative stream generations after complete manifest replay.
    #[cfg(feature = "cluster")]
    pub(crate) fn apply_stream_catalog_generations(
        &mut self,
        entries: &[laminar_core::cluster::control::CatalogManifestEntry],
    ) -> Result<(), DbError> {
        for entry in entries {
            if entry.kind != laminar_core::catalog::CatalogObjectKind::Stream {
                continue;
            }
            let stream = self.streams.get_mut(&entry.canonical_name).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "catalog manifest stream '{}' was not registered during replay",
                    entry.canonical_name
                ))
            })?;
            stream.catalog_generation = entry.catalog_generation;
        }
        Ok(())
    }

    /// Returns `true` if it existed.
    pub fn unregister_source(&mut self, name: &str) -> bool {
        self.remove_ddl(name);
        self.sources.remove(name).is_some()
    }

    /// Returns `true` if it existed.
    pub fn unregister_sink(&mut self, name: &str) -> bool {
        self.remove_ddl(name);
        self.sinks.remove(name).is_some()
    }

    /// Returns `true` if it existed.
    pub fn unregister_stream(&mut self, name: &str) -> bool {
        self.remove_ddl(name);
        self.streams.remove(name).is_some()
    }

    pub fn register_table(&mut self, reg: TableRegistration) {
        self.tables.insert(reg.name.clone(), reg);
    }

    /// Returns `true` if it existed.
    pub fn unregister_table(&mut self, name: &str) -> bool {
        self.remove_ddl(name);
        self.tables.remove(name).is_some()
    }

    pub fn tables(&self) -> &HashMap<String, TableRegistration> {
        &self.tables
    }

    /// True if any registration has a non-`None` connector type.
    pub fn has_external_connectors(&self) -> bool {
        self.sources.values().any(|s| s.connector_type.is_some())
            || self.sinks.values().any(|s| s.connector_type.is_some())
            || self.tables.values().any(|t| t.connector_type.is_some())
    }

    pub fn sources(&self) -> &HashMap<String, SourceRegistration> {
        &self.sources
    }

    pub fn sinks(&self) -> &HashMap<String, SinkRegistration> {
        &self.sinks
    }

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
        self.ddl_order.clear();
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
            .field("ddl_order", &self.ddl_order.len())
            .finish()
    }
}

#[cfg(test)]
mod tests;
