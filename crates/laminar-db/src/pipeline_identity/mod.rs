//! Deterministic logical-pipeline identity used to admit checkpoint recovery.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use arrow_schema::{Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{SourceInputMode, SourceRowPositionCapability};
use laminar_connectors::registry::ConnectorRegistry;
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
use laminar_core::checkpoint::checkpoint_manifest::{PipelineIdentity, PIPELINE_IDENTITY_VERSION};

/// Recovery-state serialization contract. Bump when persisted operator/vnode bytes become
/// incompatible even if the logical pipeline is unchanged.
const STATE_ABI_VERSION: u32 = crate::operator_graph::STATE_FRAME_ABI_VERSION;
const STATE_LAYOUT: &str = "vnode";

#[derive(Serialize)]
struct CanonicalPipeline {
    canonical_version: u16,
    state_abi_version: u32,
    partitioning_abi_version: u16,
    state_layout: &'static str,
    vnode_count: u16,
    delivery_guarantee: String,
    source_idle_timeout_ms: Option<u64>,
    event_time_max_future_skew_ms: i64,
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
    input_mode: &'static str,
    row_positions: &'static str,
    schema: Option<CanonicalSchema>,
    primary_key: Vec<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    temporal_join_idle_history_retention_ms: Option<i64>,
    incremental: bool,
}

#[derive(Serialize)]
struct CanonicalTable {
    name: String,
    primary_key: String,
    connector_type: String,
    options: BTreeMap<String, String>,
    schema: Option<CanonicalSchema>,
    on_demand: bool,
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
    connector_registry: &'a ConnectorRegistry,
    registrations: PipelineRegistrations<'a>,
    vnode_count: u16,
}

impl<'a> PipelineIdentityContext<'a> {
    #[must_use]
    pub(crate) const fn new(
        config: &'a LaminarConfig,
        catalog: &'a SourceCatalog,
        connector_registry: &'a ConnectorRegistry,
        registrations: PipelineRegistrations<'a>,
        vnode_count: u16,
    ) -> Self {
        Self {
            config,
            catalog,
            connector_registry,
            registrations,
            vnode_count,
        }
    }
}

/// Compute the exact checkpoint recovery identity.
pub(crate) fn compute(context: &PipelineIdentityContext<'_>) -> Result<PipelineIdentity, DbError> {
    let payload = CanonicalPipeline {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        state_abi_version: STATE_ABI_VERSION,
        partitioning_abi_version: laminar_core::state::PARTITIONING_ABI_VERSION,
        state_layout: STATE_LAYOUT,
        vnode_count: context.vnode_count,
        delivery_guarantee: context.config.delivery_guarantee.to_string(),
        source_idle_timeout_ms: crate::config::source_idle_timeout_ms(
            context.config.source_idle_timeout,
        )
        .map_err(|reason| DbError::Config(reason.to_string()))?,
        event_time_max_future_skew_ms: crate::config::event_time_max_future_skew_ms(
            context.config.event_time_max_future_skew,
        )
        .map_err(|reason| DbError::Config(reason.to_string()))?,
        sources: canonical_sources(
            context.catalog,
            context.connector_registry,
            &context.registrations,
        )?,
        streams: canonical_streams(context.config, &context.registrations)?,
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

fn canonical_sources(
    catalog: &SourceCatalog,
    connector_registry: &ConnectorRegistry,
    registrations: &PipelineRegistrations<'_>,
) -> Result<Vec<CanonicalSource>, DbError> {
    let mut sources = Vec::with_capacity(registrations.sources.len());
    for reg in registrations.sources.values() {
        let (connector_type, options, input_mode, row_positions) = if reg.connector_type.is_some() {
            canonical_source_connector(&build_source_config(reg)?, connector_registry)?
        } else {
            (
                "catalog-bridge".into(),
                BTreeMap::new(),
                SourceInputMode::AppendOnly,
                SourceRowPositionCapability::Unavailable,
            )
        };
        let entry = catalog.get_source(&reg.name);
        sources.push(canonical_source(
            reg.name.clone(),
            connector_type,
            options,
            input_mode,
            row_positions,
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
            SourceInputMode::AppendOnly,
            SourceRowPositionCapability::Unavailable,
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
    input_mode: SourceInputMode,
    row_positions: SourceRowPositionCapability,
    entry: Option<&SourceEntry>,
) -> CanonicalSource {
    CanonicalSource {
        name,
        connector_type,
        options,
        input_mode: canonical_source_input_mode(input_mode),
        row_positions: canonical_source_row_positions(row_positions),
        schema: entry.map(|entry| canonical_schema(&entry.schema)),
        primary_key: entry.map_or_else(Vec::new, |entry| entry.primary_key.clone()),
        watermark_column: entry.and_then(|entry| entry.watermark_column.clone()),
        max_out_of_orderness_ms: entry
            .and_then(|entry| entry.max_out_of_orderness)
            .map(duration_millis),
        processing_time: entry
            .is_some_and(|entry| entry.is_processing_time.load(Ordering::Acquire)),
    }
}

fn canonical_streams(
    config: &LaminarConfig,
    registrations: &PipelineRegistrations<'_>,
) -> Result<Vec<CanonicalStream>, DbError> {
    let mut streams: Vec<_> = registrations
        .streams
        .values()
        .map(|reg| {
            let is_temporal = reg.join_config.as_ref().is_some_and(|joins| {
                joins.iter().any(|join| {
                    matches!(
                        join,
                        laminar_sql::translator::JoinOperatorConfig::Temporal(_)
                    )
                })
            });
            let temporal_join_idle_history_retention_ms = is_temporal
                .then(|| {
                    crate::config::temporal_join_idle_history_retention_ms(
                        config.temporal_join_idle_history_retention,
                    )
                    .map_err(|reason| {
                        DbError::Config(format!("temporal stream '{}': {reason}", reg.name))
                    })
                })
                .transpose()?;
            Ok(CanonicalStream {
                name: reg.name.clone(),
                query_sql: canonical_sql(&reg.query_sql),
                emit_clause: format!("{:?}", reg.emit_clause),
                window_config: format!("{:?}", reg.window_config),
                order_config: format!("{:?}", reg.order_config),
                join_config: format!("{:?}", reg.join_config),
                temporal_join_idle_history_retention_ms,
                incremental: reg.incremental,
            })
        })
        .collect::<Result<_, DbError>>()?;
    streams.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(streams)
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
            on_demand: reg.on_demand,
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
            let value = laminar_connectors::security::sanitize_identity_value(&normalized, value);
            (normalized, value)
        })
        .collect();
    (config.connector_type().to_string(), options)
}

fn canonical_source_connector(
    config: &ConnectorConfig,
    connector_registry: &ConnectorRegistry,
) -> Result<
    (
        String,
        BTreeMap<String, String>,
        SourceInputMode,
        SourceRowPositionCapability,
    ),
    DbError,
> {
    let source = connector_registry
        .create_source(config, None)
        .map_err(|error| DbError::Checkpoint(format!("source recovery identity: {error}")))?;
    let contract = source
        .contract(config)
        .map_err(|error| DbError::Checkpoint(format!("source contract identity: {error}")))?;
    let options = source
        .recovery_identity_options(config)
        .map_err(|error| DbError::Checkpoint(format!("source recovery identity: {error}")))?;
    let (connector_type, options) = options.map_or_else(
        || canonical_connector(config),
        |options| (config.connector_type().to_string(), options),
    );
    Ok((
        connector_type,
        options,
        contract.input_mode,
        contract.row_positions,
    ))
}

const fn canonical_source_input_mode(input_mode: SourceInputMode) -> &'static str {
    match input_mode {
        SourceInputMode::AppendOnly => "append_only",
        SourceInputMode::KeyedUpsert => "keyed_upsert",
        SourceInputMode::FullChangelog => "full_changelog",
    }
}

const fn canonical_source_row_positions(capability: SourceRowPositionCapability) -> &'static str {
    match capability {
        SourceRowPositionCapability::Unavailable => "unavailable",
        SourceRowPositionCapability::OrderedDeterministic => "ordered_deterministic",
    }
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
mod tests;
