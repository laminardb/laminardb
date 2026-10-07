//! Shared schema-resolution policy. Discovery never consumes records or creates a target.

use arrow_schema::SchemaRef;
use serde::Serialize;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

pub use laminar_core::schema_binding::{
    NativeSchema, NativeSchemaArtifact, SchemaBinding, SchemaDirection, SchemaFieldMapping,
    SchemaOrigin,
};

/// Available source of schema authority, independently of transport support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDiscovery {
    /// Explicit fields are required; no authoritative discovery exists.
    Explicit,
    /// A connector has a fixed protocol schema.
    BuiltIn,
    /// Read-only authoritative metadata is available for the listed formats.
    Metadata,
    /// Writer fields come from the bound query, without a remote schema authority.
    Query,
}

/// External preparation is independent of metadata discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaPreparation {
    /// No schema/table mutation is supported.
    None,
    /// Table creation requires an explicit connector setting.
    ExplicitTableCreation,
    /// Schema registration requires an explicit connector setting.
    ExplicitRegistration,
}

/// A factory must declare its direction's schema behavior in its connector metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaCapabilities {
    /// Schema authority for this registered direction.
    pub discovery: SchemaDiscovery,
    /// Formats with authoritative metadata; empty means a native protocol schema.
    pub metadata_formats: Vec<String>,
    /// Formats with a fixed decoder schema when this transport also supports explicit formats.
    pub built_in_formats: Vec<String>,
    /// Whether writer fields are derived from a bound input schema.
    pub query_derived: bool,
    /// Optional sampling is implemented, separately from metadata resolution.
    pub bounded_sampling: bool,
    /// Explicit external preparation capability.
    pub preparation: SchemaPreparation,
}

impl SchemaCapabilities {
    pub(crate) fn validate_native_format(
        &self,
        config: &ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        let native = match self.discovery {
            SchemaDiscovery::BuiltIn => self.built_in_formats.is_empty(),
            SchemaDiscovery::Metadata => self.metadata_formats.is_empty(),
            SchemaDiscovery::Explicit | SchemaDiscovery::Query => false,
        };
        if native && config.get("format").is_some() {
            return Err(ConnectorError::FeatureUnsupported(format!(
                "{} uses a fixed native protocol; omit FORMAT instead of selecting a serialization codec",
                config.connector_type()
            )));
        }
        Ok(())
    }

    /// Conservative declaration for custom connectors with user-supplied source fields
    /// or query-derived sink fields and no discovery or external preparation.
    #[must_use]
    pub fn declared(is_sink: bool) -> Self {
        Self {
            discovery: if is_sink {
                SchemaDiscovery::Query
            } else {
                SchemaDiscovery::Explicit
            },
            metadata_formats: Vec::new(),
            built_in_formats: Vec::new(),
            query_derived: is_sink,
            bounded_sampling: false,
            preparation: SchemaPreparation::None,
        }
    }

    /// A fixed source protocol schema.
    #[must_use]
    pub fn built_in() -> Self {
        Self {
            discovery: SchemaDiscovery::BuiltIn,
            ..Self::declared(false)
        }
    }

    /// Authoritative metadata, scoped to the actual codec formats.
    #[must_use]
    pub fn metadata(formats: &[&str], is_sink: bool, preparation: SchemaPreparation) -> Self {
        Self {
            discovery: SchemaDiscovery::Metadata,
            metadata_formats: formats.iter().map(|format| (*format).into()).collect(),
            preparation,
            ..Self::declared(is_sink)
        }
    }
}

/// Create a validated logical-only reader or writer binding.
///
/// # Errors
/// Rejects empty or incomplete schemas; no fallback to strings or sampling occurs.
pub fn logical_binding(
    config: &ConnectorConfig,
    direction: SchemaDirection,
    origin: SchemaOrigin,
    schema: &SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    SchemaBinding::logical(
        config.connector_type(),
        direction,
        origin,
        schema.as_ref().clone(),
    )
    .map_err(binding_error)
}

/// Attach an external schema under the default exact-name/type policy.
///
/// # Errors
/// Rejects directional type/nullability mismatches and unmapped sink fields.
pub fn bind_external(
    binding: &mut SchemaBinding,
    schema: &SchemaRef,
) -> Result<(), ConnectorError> {
    binding
        .bind_external(schema.as_ref().clone())
        .map_err(binding_error)
}

/// Preserve the existing connector schema-error taxonomy.
#[must_use]
#[allow(clippy::needless_pass_by_value)] // Adapter for owned Result::map_err errors.
pub fn binding_error(error: laminar_core::schema_binding::SchemaBindingError) -> ConnectorError {
    ConnectorError::SchemaMismatch(error.to_string())
}

/// Validate that authorized external preparation retains the resolved query contract.
/// Native identity, writer metadata and mappings may be finalized by the preparation hook.
///
/// # Errors
/// Rejects a changed connector, direction, logical schema or changelog policy, and malformed
/// native content. Call before catalog publication or connector activation.
pub fn validate_prepared_writer(
    config: &ConnectorConfig,
    resolved: &SchemaBinding,
    prepared: &SchemaBinding,
) -> Result<(), ConnectorError> {
    if prepared.connector != config.connector_type()
        || prepared.direction != SchemaDirection::Sink
        || prepared.logical != resolved.logical
        || prepared.control_fields != resolved.control_fields
    {
        return Err(ConnectorError::SchemaMismatch(format!(
            "{} sink preparation changed the resolved query schema or writer policy",
            config.connector_type()
        )));
    }
    prepared.canonical_bytes().map_err(binding_error)?;
    Ok(())
}

/// Bind a fixed protocol whose decoder emits the complete declared field list.
///
/// # Errors
/// Rejects unsupported projections, ordering and field types; explicit nullability may be weaker.
pub fn fixed_binding(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
    protocol: &SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    let origin = if explicit.is_some() {
        SchemaOrigin::Explicit
    } else {
        SchemaOrigin::BuiltIn
    };
    let logical = explicit.unwrap_or_else(|| std::sync::Arc::clone(protocol));
    if logical.fields().len() != protocol.fields().len()
        || logical
            .fields()
            .iter()
            .zip(protocol.fields())
            .any(|(logical, native)| logical.name() != native.name())
    {
        return Err(ConnectorError::FeatureUnsupported("fixed protocol fields must be declared in their complete protocol order; project in CREATE STREAM".into()));
    }
    let mut binding = logical_binding(config, SchemaDirection::Source, origin, &logical)?;
    bind_external(&mut binding, protocol)?;
    Ok(binding)
}

/// Apply a prepared exact-name mapping without casts or per-row conversion.
///
/// # Errors
/// Rejects changed types and actual nulls in non-nullable logical fields.
pub fn project_batch(
    batch: &arrow_array::RecordBatch,
    logical: &SchemaRef,
) -> Result<arrow_array::RecordBatch, ConnectorError> {
    let mut columns = Vec::with_capacity(logical.fields().len());
    for field in logical.fields() {
        let index = batch.schema().index_of(field.name()).map_err(|_| {
            ConnectorError::SchemaMismatch(format!("native batch is missing '{}'", field.name()))
        })?;
        let column = batch.column(index);
        if !laminar_core::schema_binding::same_logical_type(column.data_type(), field.data_type())
            || (!field.is_nullable() && column.null_count() > 0)
        {
            return Err(ConnectorError::SchemaMismatch(format!(
                "native field '{}' changed type or nullability",
                field.name()
            )));
        }
        let column = if column.data_type() == field.data_type() {
            std::sync::Arc::clone(column)
        } else {
            // Only native metadata differs; Arrow retains the value buffers in this retyping.
            arrow_cast::cast(column, field.data_type())
                .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?
        };
        columns.push(column);
    }
    arrow_array::RecordBatch::try_new(std::sync::Arc::clone(logical), columns)
        .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))
}
