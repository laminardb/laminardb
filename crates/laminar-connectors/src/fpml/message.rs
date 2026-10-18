//! FpML over message transports: each message is one complete document and becomes one row.

use super::{Artifact, FpmlConfig, Profile};
use crate::config::{ConfigKeySpec, ConnectorConfig};
use crate::error::ConnectorError;

#[cfg(any(feature = "kafka", feature = "rabbitmq", feature = "ibmmq"))]
use {
    super::FpmlDecoder,
    crate::connector::ConnectorTaskGuard,
    crate::schema::resolution::{
        bind_external, fixed_binding, logical_binding, SchemaBinding, SchemaDirection,
    },
    arrow_schema::SchemaRef,
    std::sync::Arc,
};
#[cfg(any(feature = "rabbitmq", feature = "ibmmq"))]
use {
    crate::error::SerdeError,
    crate::schema::traits::FormatDecoder,
    crate::serde::{Format, RecordDeserializer},
    arrow_array::RecordBatch,
};

/// A message is one row, so a source without a profile uses the document profile.
pub(crate) const MESSAGE_PROFILE: &str = "fpml-5-13-confirmation-document";

/// Every `fpml.*` option a message source accepts.
#[cfg(any(feature = "rabbitmq", feature = "ibmmq", feature = "fix"))]
pub(crate) const OPTION_KEYS: &[&str] = &[
    "fpml.profile",
    "fpml.profile.path",
    "fpml.schema.path",
    "fpml.references",
    "fpml.max.document.bytes",
    "fpml.max.depth",
    "fpml.max.elements",
    "fpml.max.attributes.per.element",
    "fpml.max.text.bytes",
    "fpml.max.namespace.declarations",
    "fpml.max.ids",
    "fpml.max.references",
    "fpml.max.rows",
    "fpml.max.staged.bytes",
];

/// Registry descriptions of the profile and closure options.
pub(crate) fn config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::optional(
            "fpml.profile",
            "Bundled FpML profile (format = 'fpml'); it must yield one row per document",
            MESSAGE_PROFILE,
        ),
        ConfigKeySpec::optional(
            "fpml.profile.path",
            "Operator FpML profile file, instead of a bundled profile",
            "",
        ),
        ConfigKeySpec::optional(
            "fpml.schema.path",
            "Directory of an operator FpML schema closure (closure.toml), instead of the \
             bundled closure",
            "",
        ),
        ConfigKeySpec::optional(
            "fpml.references",
            "FpML reference checking: resolve (every href resolves) or schema (also annotated \
             target types)",
            "resolve",
        ),
    ]
}

fn message_config(config: &ConnectorConfig) -> Result<FpmlConfig, ConnectorError> {
    if config.get("fpml.profile").is_some() || config.get("fpml.profile.path").is_some() {
        return FpmlConfig::from_connector_config(config);
    }
    let mut config = config.clone();
    config.set("fpml.profile", MESSAGE_PROFILE);
    FpmlConfig::from_connector_config(&config)
}

/// Resolve the closure and profile off the async runtime; only a profile that yields exactly
/// one row per document fits a one-row-per-message transport.
pub(crate) async fn resolve_profile(
    config: &ConnectorConfig,
    transport: &str,
) -> Result<(FpmlConfig, Artifact, Profile), ConnectorError> {
    let fpml = message_config(config)?;
    let resolving = fpml.clone();
    let (artifact, profile) = tokio::task::spawn_blocking(move || resolving.resolve())
        .await
        .map_err(|error| {
            ConnectorError::Internal(format!("FpML profile loader failed: {error}"))
        })??;
    if !profile.one_row_per_document() {
        return Err(ConnectorError::ConfigurationError(format!(
            "{transport} maps one message to one row, so an FpML profile must select the \
             document root (rows = \".\"); '{}' does not. Use '{MESSAGE_PROFILE}' or a \
             document-row operator profile",
            profile.name
        )));
    }
    Ok((fpml, artifact, profile))
}

/// Bind the profile's columns as the payload. `output` validates a payload schema against the
/// transport's metadata columns and appends them.
#[cfg(any(feature = "kafka", feature = "rabbitmq", feature = "ibmmq"))]
pub(crate) async fn source_binding(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
    transport: &str,
    output: impl Fn(&SchemaRef) -> Result<SchemaRef, ConnectorError>,
) -> Result<SchemaBinding, ConnectorError> {
    let (_, artifact, profile) = resolve_profile(config, transport).await?;
    let payload = profile.schema();
    let fixed = fixed_binding(config, explicit, &payload)?;
    let declared = Arc::new(fixed.logical);
    let mut binding = logical_binding(
        config,
        SchemaDirection::Source,
        fixed.origin,
        &output(&declared)?,
    )?;
    bind_external(&mut binding, &output(&payload)?)?;
    binding.value = Some(profile.native_schema(&artifact));
    Ok(binding)
}

/// Start the decoder for the closure and profile the committed binding names; any other closure
/// or profile is refused.
#[cfg(any(feature = "kafka", feature = "rabbitmq", feature = "ibmmq"))]
pub(crate) async fn start_decoder(
    config: &ConnectorConfig,
    binding: Option<&SchemaBinding>,
    transport: &str,
    registry: Option<&prometheus::Registry>,
    guard: ConnectorTaskGuard,
) -> Result<FpmlDecoder, ConnectorError> {
    let (fpml, artifact, profile) = resolve_profile(config, transport).await?;
    let committed = binding
        .and_then(|binding| binding.value.as_ref())
        .map(|native| &native.identity);
    if committed != Some(&profile.native_schema(&artifact).identity) {
        return Err(ConnectorError::SchemaMismatch(
            "the FpML profile or schema closure differs from the one this source was created \
             with; recreate the source to change it"
                .into(),
        ));
    }
    let source = config.get("laminar.source.name").unwrap_or(transport);
    FpmlDecoder::start(artifact, &profile, &fpml, source, registry, Some(guard)).await
}

/// The message codec: each payload is one document that must project exactly one row.
///
/// Decoding blocks the calling thread while the validator works; call it off the async runtime.
#[cfg(any(feature = "rabbitmq", feature = "ibmmq"))]
pub(crate) struct MessageDecoder(pub(crate) Arc<FpmlDecoder>);

#[cfg(any(feature = "rabbitmq", feature = "ibmmq"))]
impl RecordDeserializer for MessageDecoder {
    fn deserialize(&self, data: &[u8], _schema: &SchemaRef) -> Result<RecordBatch, SerdeError> {
        let batch = self
            .0
            .decode(data)
            .map_err(|rejection| SerdeError::MalformedInput(rejection.to_string()))?;
        if batch.num_rows() != 1 {
            return Err(SerdeError::RecordCountMismatch {
                expected: 1,
                got: batch.num_rows(),
            });
        }
        Ok(batch)
    }

    fn deserialize_batch(
        &self,
        records: &[&[u8]],
        _schema: &SchemaRef,
    ) -> Result<RecordBatch, SerdeError> {
        let schema = self.0.output_schema();
        let batches = records
            .iter()
            .map(|record| self.deserialize(record, &schema))
            .collect::<Result<Vec<_>, _>>()?;
        arrow_select::concat::concat_batches(&schema, &batches).map_err(|error| {
            SerdeError::MalformedInput(format!("failed to concat FpML rows: {error}"))
        })
    }

    fn format(&self) -> Format {
        Format::Fpml
    }
}
