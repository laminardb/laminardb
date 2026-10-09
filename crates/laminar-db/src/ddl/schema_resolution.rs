//! Resolved definitions are execution inputs; original SQL remains the catalog intent.

use std::sync::Arc;

use laminar_connectors::config::ConnectorConfig;
use laminar_core::schema_binding::{SchemaBinding, SchemaDirection};
use laminar_sql::parser::{CreateSourceStatement, SinkFrom};
use laminar_sql::translator::streaming_ddl::{self, ColumnDefinition, SourceDefinition};

use crate::db::{canonical_object_name, exact_table_reference, LaminarDB};
use crate::error::DbError;

use super::source_sink::ResolvedConnector;

tokio::task_local! {
    /// A committed binding or a creation binding prepared before catalog mutation.
    pub(crate) static RESOLVED_SCHEMA: Option<SchemaBinding>;
}

pub(crate) fn supplied_binding() -> Option<SchemaBinding> {
    RESOLVED_SCHEMA.try_with(Clone::clone).ok().flatten()
}

pub(super) fn legacy_schema_replay_active() -> bool {
    RESOLVED_SCHEMA.try_with(|_| ()).is_ok() || crate::db::catalog_manifest_replay_active()
}

pub(super) fn resolution_config(resolved: &ResolvedConnector) -> Result<ConnectorConfig, DbError> {
    let connector = resolved
        .connector_type
        .as_deref()
        .ok_or_else(|| DbError::Config("schema resolution has no connector type".into()))?;
    let mut properties = resolved.connector_options.clone();
    if let Some(format) = &resolved.format {
        properties.insert("format".into(), format.to_ascii_lowercase());
    }
    properties.extend(resolved.format_options.clone());
    Ok(ConnectorConfig::with_properties(
        crate::connector_manager::normalize_connector_type(connector),
        properties,
    ))
}

impl LaminarDB {
    pub(super) async fn resolve_source_definition(
        &self,
        create: &CreateSourceStatement,
        resolved: Option<&ResolvedConnector>,
        source_name: &str,
    ) -> Result<(SourceDefinition, Option<SchemaBinding>), DbError> {
        let explicit = if create.columns.is_empty() {
            None
        } else {
            Some(
                streaming_ddl::translate_create_source(create.clone())
                    .map_err(|error| DbError::Sql(laminar_sql::Error::ParseError(error)))?,
            )
        };
        let Some(resolved) = resolved else {
            return Ok((
                streaming_ddl::translate_create_source(create.clone())
                    .map_err(|error| DbError::Sql(laminar_sql::Error::ParseError(error)))?,
                None,
            ));
        };
        let mut config = resolution_config(resolved)?;
        if let Some(definition) = explicit.as_ref() {
            set_primary_key_columns(&mut config, &definition.primary_key);
        }
        let binding = match supplied_binding() {
            Some(binding) => {
                validate_supplied(&binding, &config, SchemaDirection::Source)?;
                if let Some(explicit) = &explicit {
                    validate_explicit_source_fields(&binding, &config, &explicit.schema)?;
                }
                binding
            }
            None if legacy_schema_replay_active() => {
                let explicit = explicit.ok_or_else(|| DbError::Checkpoint(format!(
                    "legacy source '{source_name}' lacks an explicit durable schema; a controlled catalog migration is required"
                )))?;
                laminar_connectors::schema::resolution::logical_binding(
                    &config,
                    SchemaDirection::Source,
                    laminar_core::schema_binding::SchemaOrigin::Explicit,
                    &explicit.schema,
                )?
            }
            None => self
                .connector_registry
                .resolve_source_schema(&config, explicit.map(|definition| definition.schema))
                .await
                .map_err(|error| {
                    DbError::Connector(format!(
                        "source '{source_name}' ({}) schema resolution: {error}",
                        config.connector_type()
                    ))
                })?,
        };
        let columns = binding
            .logical
            .fields()
            .iter()
            .map(|field| ColumnDefinition {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            })
            .collect();
        let mut definition =
            streaming_ddl::translate_create_source_with_columns(create.clone(), columns)
                .map_err(|error| DbError::Sql(laminar_sql::Error::ParseError(error)))?;
        // Preserve nested-field and schema metadata that SQL column syntax cannot represent.
        definition.schema = Arc::new(binding.logical.clone());
        Ok((definition, Some(binding)))
    }

    pub(super) async fn resolve_sink_binding(
        &self,
        create: &laminar_sql::parser::CreateSinkStatement,
        resolved: &ResolvedConnector,
    ) -> Result<SchemaBinding, DbError> {
        let mut config = resolution_config(resolved)?;
        let input = match &create.from {
            SinkFrom::Table(table) => canonical_object_name(table)?,
            SinkFrom::Query(_) => {
                return Err(DbError::Unsupported(
                    "sink queries require a named CREATE STREAM; sink FROM that stream".into(),
                ))
            }
        };
        let provider = self
            .ctx
            .table_provider(exact_table_reference(&input))
            .await
            .map_err(|error| {
                DbError::Config(format!("sink input '{input}' cannot be bound: {error}"))
            })?;
        let input_schema = match self.keyed_mutation_source(&input)? {
            Some(source) => crate::direct_mutation::sink_input_schema(&source.schema),
            None => provider.schema(),
        };
        config.set(
            "_arrow_schema",
            crate::pipeline_callback::encode_arrow_schema(&input_schema),
        );
        config.set(
            "delivery.guarantee",
            self.config.delivery_guarantee.to_string(),
        );
        if let Some(binding) = supplied_binding() {
            validate_supplied(&binding, &config, SchemaDirection::Sink)?;
            validate_logical_fields(&binding, &input_schema)?;
            return Ok(binding);
        }
        if legacy_schema_replay_active() {
            return laminar_connectors::schema::resolution::logical_binding(
                &config,
                SchemaDirection::Sink,
                laminar_core::schema_binding::SchemaOrigin::Query,
                &input_schema,
            )
            .map_err(DbError::from);
        }
        self.connector_registry
            .resolve_sink_schema(&config, input_schema)
            .await
            .map_err(|error| {
                DbError::Connector(format!(
                    "sink '{}' ({}) schema resolution: {error}",
                    create.name,
                    config.connector_type()
                ))
            })
    }
}

/// Expose a declared `PRIMARY KEY` to source connectors that key their output by it.
pub(crate) fn set_primary_key_columns(config: &mut ConnectorConfig, primary_key: &[String]) {
    if !primary_key.is_empty() {
        config.set("_primary_key_columns", primary_key.join(","));
    }
}

fn validate_supplied(
    binding: &SchemaBinding,
    config: &ConnectorConfig,
    direction: SchemaDirection,
) -> Result<(), DbError> {
    binding
        .canonical_bytes()
        .map_err(|error| DbError::Checkpoint(error.to_string()))?;
    if binding.direction != direction || binding.connector != config.connector_type() {
        return Err(DbError::Checkpoint(
            "committed schema binding has a different connector or direction".into(),
        ));
    }
    Ok(())
}

fn validate_explicit_source_fields(
    binding: &SchemaBinding,
    config: &ConnectorConfig,
    explicit: &arrow_schema::SchemaRef,
) -> Result<(), DbError> {
    let extra = match config.connector_type() {
        "kafka" => {
            usize::from(
                config
                    .get_parsed::<bool>("include.metadata")?
                    .unwrap_or(false),
            ) * 3
                + usize::from(
                    config
                        .get_parsed::<bool>("include.headers")?
                        .unwrap_or(false),
                )
        }
        "files" => usize::from(
            config
                .get_parsed::<bool>("include_metadata")?
                .unwrap_or(false),
        ),
        _ => 0,
    };
    let expected = binding
        .logical
        .fields()
        .len()
        .checked_sub(extra)
        .ok_or_else(|| DbError::Checkpoint("committed source metadata is incomplete".into()))?;
    let payload = binding
        .logical
        .project(&(0..expected).collect::<Vec<_>>())
        .map_err(|error| DbError::Checkpoint(error.to_string()))?;
    let mut payload_binding = binding.clone();
    payload_binding.logical = payload;
    validate_logical_fields(&payload_binding, explicit)
}

fn validate_logical_fields(
    binding: &SchemaBinding,
    schema: &arrow_schema::SchemaRef,
) -> Result<(), DbError> {
    let fields_match = binding.logical.fields().len() == schema.fields().len()
        && binding
            .logical
            .fields()
            .iter()
            .zip(schema.fields())
            .all(|(a, b)| {
                a.name() == b.name()
                    && laminar_core::schema_binding::same_logical_type(a.data_type(), b.data_type())
                    && a.is_nullable() == b.is_nullable()
            });
    if !fields_match {
        return Err(DbError::Checkpoint("logical fields differ from the committed schema contract; use a controlled catalog migration".into()));
    }
    Ok(())
}
