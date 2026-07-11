//! Deterministic logical-pipeline identity used to admit checkpoint recovery.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use arrow_schema::{Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use rustc_hash::FxHashMap;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::catalog::{SourceCatalog, SourceEntry};
use crate::config::LaminarConfig;
use crate::connector_manager::{
    build_sink_config, build_source_config, build_table_config, SinkRegistration,
    SourceRegistration, StreamRegistration, TableRegistration,
};
use crate::error::DbError;
use laminar_core::storage::checkpoint_manifest::{PipelineIdentity, PIPELINE_IDENTITY_VERSION};

/// Recovery-state serialization contract. Bump when persisted operator/vnode bytes become
/// incompatible even if the logical pipeline is unchanged.
const STATE_ABI_VERSION: u32 = crate::operator_graph::GRAPH_CHECKPOINT_VERSION;
const REDACTED: &str = "<redacted>";

#[derive(Serialize)]
struct CanonicalPipeline {
    canonical_version: u16,
    state_abi_version: u32,
    state_layout: &'static str,
    vnode_count: u16,
    delivery_guarantee: String,
    sources: Vec<CanonicalSource>,
    streams: Vec<CanonicalStream>,
    tables: Vec<CanonicalTable>,
    sinks: Vec<CanonicalSink>,
}

#[derive(Serialize)]
struct CanonicalSource {
    name: String,
    connector_type: String,
    options: BTreeMap<String, String>,
    schema: Option<CanonicalSchema>,
    watermark_column: Option<String>,
    max_out_of_orderness_ms: Option<u64>,
    processing_time: bool,
}

#[derive(Serialize)]
struct CanonicalStream {
    name: String,
    query_sql: String,
    emit_clause: String,
    window_config: String,
    order_config: String,
    join_config: String,
    incremental: bool,
}

#[derive(Serialize)]
struct CanonicalTable {
    name: String,
    primary_key: String,
    connector_type: String,
    options: BTreeMap<String, String>,
    schema: Option<CanonicalSchema>,
    refresh: String,
    cache_max_bytes: Option<usize>,
    cache_ttl_ms: Option<u64>,
}

#[derive(Serialize)]
struct CanonicalSink {
    name: String,
    input: String,
    connector_type: String,
    options: BTreeMap<String, String>,
    filter_expr: Option<String>,
}

#[derive(Serialize)]
struct CanonicalSchema {
    fields: Vec<CanonicalField>,
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct CanonicalField {
    name: String,
    nullable: bool,
    data_type: String,
    metadata: BTreeMap<String, String>,
}

/// Borrowed registration snapshot used only while computing the startup identity.
///
/// The connector manager currently owns standard hash maps for its cold DDL path. Converting
/// their borrowed values to `FxHashMap`s here keeps the identity module on the workspace's
/// canonical map type without cloning registration payloads.
pub(crate) struct PipelineRegistrations<'a> {
    sources: FxHashMap<&'a str, &'a SourceRegistration>,
    sinks: FxHashMap<&'a str, &'a SinkRegistration>,
    streams: FxHashMap<&'a str, &'a StreamRegistration>,
    tables: FxHashMap<&'a str, &'a TableRegistration>,
}

impl<'a> PipelineRegistrations<'a> {
    #[must_use]
    pub(crate) fn new(
        sources: impl Iterator<Item = &'a SourceRegistration>,
        sinks: impl Iterator<Item = &'a SinkRegistration>,
        streams: impl Iterator<Item = &'a StreamRegistration>,
        tables: impl Iterator<Item = &'a TableRegistration>,
    ) -> Self {
        Self {
            sources: sources.map(|reg| (reg.name.as_str(), reg)).collect(),
            sinks: sinks.map(|reg| (reg.name.as_str(), reg)).collect(),
            streams: streams.map(|reg| (reg.name.as_str(), reg)).collect(),
            tables: tables.map(|reg| (reg.name.as_str(), reg)).collect(),
        }
    }
}

/// Complete input to deterministic pipeline identity computation.
pub(crate) struct PipelineIdentityContext<'a> {
    config: &'a LaminarConfig,
    catalog: &'a SourceCatalog,
    registrations: PipelineRegistrations<'a>,
    vnode_count: u16,
    clustered: bool,
}

impl<'a> PipelineIdentityContext<'a> {
    #[must_use]
    pub(crate) const fn new(
        config: &'a LaminarConfig,
        catalog: &'a SourceCatalog,
        registrations: PipelineRegistrations<'a>,
        vnode_count: u16,
        clustered: bool,
    ) -> Self {
        Self {
            config,
            catalog,
            registrations,
            vnode_count,
            clustered,
        }
    }
}

/// Compute the checkpoint compatibility identity before recovery starts.
pub(crate) fn compute(context: &PipelineIdentityContext<'_>) -> Result<PipelineIdentity, DbError> {
    let payload = CanonicalPipeline {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        state_abi_version: STATE_ABI_VERSION,
        state_layout: state_layout(context.clustered, context.config.state_tier_dir.is_some()),
        vnode_count: context.vnode_count,
        delivery_guarantee: context.config.delivery_guarantee.to_string(),
        sources: canonical_sources(context.catalog, &context.registrations)?,
        streams: canonical_streams(&context.registrations),
        tables: canonical_tables(context.catalog, &context.registrations)?,
        sinks: canonical_sinks(context.config, &context.registrations)?,
    };
    let encoded = serde_json::to_vec(&payload)
        .map_err(|error| DbError::Checkpoint(format!("pipeline identity encode: {error}")))?;
    let digest = Sha256::digest(encoded);
    Ok(PipelineIdentity {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        sha256: format!("{digest:x}"),
    })
}

const fn state_layout(clustered: bool, tiered: bool) -> &'static str {
    match (clustered, tiered) {
        (true, true) => "partitioned-vnode-tiered",
        (true, false) => "partitioned-vnode",
        (false, true) => "local-tiered",
        (false, false) => "local",
    }
}

fn canonical_sources(
    catalog: &SourceCatalog,
    registrations: &PipelineRegistrations<'_>,
) -> Result<Vec<CanonicalSource>, DbError> {
    let mut sources = Vec::with_capacity(registrations.sources.len());
    for reg in registrations.sources.values() {
        let (connector_type, options) = if reg.connector_type.is_some() {
            canonical_connector(&build_source_config(reg)?)
        } else {
            ("catalog-bridge".into(), BTreeMap::new())
        };
        let entry = catalog.get_source(&reg.name);
        sources.push(canonical_source(
            reg.name.clone(),
            connector_type,
            options,
            entry.as_deref(),
        ));
    }
    // Programmatic/catalog sources do not necessarily have a connector-manager registration.
    for name in catalog.list_sources() {
        if registrations.sources.contains_key(name.as_str())
            || registrations.tables.contains_key(name.as_str())
        {
            continue;
        }
        let entry = catalog.get_source(&name);
        sources.push(canonical_source(
            name,
            "catalog-bridge".into(),
            BTreeMap::new(),
            entry.as_deref(),
        ));
    }
    sources.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(sources)
}

fn canonical_source(
    name: String,
    connector_type: String,
    options: BTreeMap<String, String>,
    entry: Option<&SourceEntry>,
) -> CanonicalSource {
    CanonicalSource {
        name,
        connector_type,
        options,
        schema: entry.map(|entry| canonical_schema(&entry.schema)),
        watermark_column: entry.and_then(|entry| entry.watermark_column.clone()),
        max_out_of_orderness_ms: entry
            .and_then(|entry| entry.max_out_of_orderness)
            .map(duration_millis),
        processing_time: entry
            .is_some_and(|entry| entry.is_processing_time.load(Ordering::Acquire)),
    }
}

fn canonical_streams(registrations: &PipelineRegistrations<'_>) -> Vec<CanonicalStream> {
    let mut streams: Vec<_> = registrations
        .streams
        .values()
        .map(|reg| CanonicalStream {
            name: reg.name.clone(),
            query_sql: canonical_sql(&reg.query_sql),
            emit_clause: format!("{:?}", reg.emit_clause),
            window_config: format!("{:?}", reg.window_config),
            order_config: format!("{:?}", reg.order_config),
            join_config: format!("{:?}", reg.join_config),
            incremental: reg.incremental,
        })
        .collect();
    streams.sort_by(|left, right| left.name.cmp(&right.name));
    streams
}

fn canonical_tables(
    catalog: &SourceCatalog,
    registrations: &PipelineRegistrations<'_>,
) -> Result<Vec<CanonicalTable>, DbError> {
    let mut tables = Vec::with_capacity(registrations.tables.len());
    for reg in registrations.tables.values() {
        let (connector_type, options) = if reg.connector_type.is_some() {
            canonical_connector(&build_table_config(reg)?)
        } else {
            ("catalog-table".into(), BTreeMap::new())
        };
        tables.push(CanonicalTable {
            name: reg.name.clone(),
            primary_key: reg.primary_key.clone(),
            connector_type,
            options,
            schema: catalog
                .get_source(&reg.name)
                .as_ref()
                .map(|entry| canonical_schema(&entry.schema)),
            refresh: format!("{:?}", reg.refresh),
            cache_max_bytes: reg.cache_max_bytes,
            cache_ttl_ms: reg.cache_ttl.map(duration_millis),
        });
    }
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tables)
}

fn canonical_sinks(
    config: &LaminarConfig,
    registrations: &PipelineRegistrations<'_>,
) -> Result<Vec<CanonicalSink>, DbError> {
    let mut sinks = Vec::with_capacity(registrations.sinks.len());
    for reg in registrations.sinks.values() {
        let (connector_type, options) = if reg.connector_type.is_some() {
            canonical_connector(&build_sink_config(reg, config.delivery_guarantee)?)
        } else {
            ("catalog-sink".into(), BTreeMap::new())
        };
        sinks.push(CanonicalSink {
            name: reg.name.clone(),
            input: reg.input.clone(),
            connector_type,
            options,
            filter_expr: reg.filter_expr.as_deref().map(canonical_sql),
        });
    }
    sinks.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(sinks)
}

fn canonical_connector(config: &ConnectorConfig) -> (String, BTreeMap<String, String>) {
    let options = config
        .properties()
        .iter()
        .map(|(key, value)| {
            let normalized = key.to_ascii_lowercase();
            let value = if is_secret_key(&normalized) {
                REDACTED.to_string()
            } else {
                value.clone()
            };
            (normalized, value)
        })
        .collect();
    (config.connector_type().to_string(), options)
}

fn is_secret_key(key: &str) -> bool {
    [
        "password",
        "passwd",
        "secret",
        "token",
        "credential",
        "private.key",
        "private_key",
        "sasl.jaas",
        "access.key",
        "access_key",
        "api.key",
        "api_key",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

fn canonical_schema(schema: &Schema) -> CanonicalSchema {
    CanonicalSchema {
        fields: schema
            .fields()
            .iter()
            .map(|field| canonical_field(field))
            .collect(),
        metadata: schema
            .metadata()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }
}

fn canonical_field(field: &Field) -> CanonicalField {
    CanonicalField {
        name: field.name().clone(),
        nullable: field.is_nullable(),
        // Arrow's Display implementation recursively includes nested fields and sorts metadata.
        data_type: field.data_type().to_string(),
        metadata: field
            .metadata()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }
}

fn canonical_sql(sql: &str) -> String {
    sql.replace("\r\n", "\n")
        .replace('\r', "\n")
        .trim_end()
        .to_string()
}

fn duration_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_property_order_is_canonical_and_credentials_are_ignored() {
        let mut left = ConnectorConfig::new("kafka");
        left.set("topic", "trades");
        left.set("password", "first");
        let mut right = ConnectorConfig::new("kafka");
        right.set("password", "rotated");
        right.set("topic", "trades");
        assert_eq!(canonical_connector(&left), canonical_connector(&right));
    }

    #[test]
    fn schema_metadata_order_is_canonical() {
        let left = Schema::new_with_metadata(
            vec![Field::new("id", arrow_schema::DataType::Int64, false)],
            [("b".into(), "2".into()), ("a".into(), "1".into())]
                .into_iter()
                .collect(),
        );
        let right = Schema::new_with_metadata(
            vec![Field::new("id", arrow_schema::DataType::Int64, false)],
            [("a".into(), "1".into()), ("b".into(), "2".into())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            serde_json::to_vec(&canonical_schema(&left)).unwrap(),
            serde_json::to_vec(&canonical_schema(&right)).unwrap()
        );
    }
}
