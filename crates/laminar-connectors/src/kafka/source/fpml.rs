//! FpML messages: one complete document per Kafka message, validated and projected on the FpML
//! decoder's worker thread behind a tracked blocking task, never on the async runtime.
#![allow(clippy::doc_markdown)]

use std::sync::Arc;

use arrow_array::RecordBatch;

use super::{
    terminalize_guaranteed_poll_error, warn, ConnectorError, DeliveryGuarantee, KafkaSource,
    SerdeError,
};
use crate::config::{ConfigKeySpec, ConnectorConfig};
use crate::fpml::{Artifact, FpmlConfig, FpmlDecoder, Profile};
use crate::schema::resolution::SchemaBinding;
use crate::schema::traits::FormatDecoder;

/// Kafka maps one message to one row, so a source without a profile uses the document profile.
pub(crate) const KAFKA_DEFAULT_PROFILE: &str = "fpml-5-13-confirmation-document";

/// Source options of the `fpml` format.
pub(crate) fn config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::optional(
            "fpml.profile",
            "Bundled FpML profile (format = 'fpml'); it must yield one row per document",
            KAFKA_DEFAULT_PROFILE,
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

fn kafka_fpml_config(config: &ConnectorConfig) -> Result<FpmlConfig, ConnectorError> {
    if config.get("fpml.profile").is_some() || config.get("fpml.profile.path").is_some() {
        return FpmlConfig::from_connector_config(config);
    }
    let mut config = config.clone();
    config.set("fpml.profile", KAFKA_DEFAULT_PROFILE);
    FpmlConfig::from_connector_config(&config)
}

/// Resolve the closure and profile off the async runtime; only a profile that yields exactly
/// one row per document fits a one-row-per-message transport.
pub(crate) async fn resolve_kafka_profile(
    config: &ConnectorConfig,
) -> Result<(FpmlConfig, Artifact, Profile), ConnectorError> {
    let fpml = kafka_fpml_config(config)?;
    let resolving = fpml.clone();
    let (artifact, profile) = tokio::task::spawn_blocking(move || resolving.resolve())
        .await
        .map_err(|error| {
            ConnectorError::Internal(format!("FpML profile loader failed: {error}"))
        })??;
    if !profile.one_row_per_document() {
        return Err(ConnectorError::ConfigurationError(format!(
            "Kafka maps one message to one row, so an FpML profile must select the document root \
             (rows = \".\"); '{}' does not. Use '{KAFKA_DEFAULT_PROFILE}' or a document-row \
             operator profile",
            profile.name
        )));
    }
    Ok((fpml, artifact, profile))
}

impl KafkaSource {
    /// Start the decoder for the closure and profile the committed binding names.
    pub(super) async fn start_fpml(
        &mut self,
        config: &ConnectorConfig,
        binding: Option<&SchemaBinding>,
    ) -> Result<(), ConnectorError> {
        let (fpml, artifact, profile) = resolve_kafka_profile(config).await?;
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
        let guard = self
            .task_owner
            .track()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "live Kafka task owner".into(),
                actual: "retired".into(),
            })?;
        let source = config.get("laminar.source.name").unwrap_or("kafka");
        let decoder = FpmlDecoder::start(
            artifact,
            &profile,
            &fpml,
            source,
            self.metrics_registry.as_ref(),
            Some(guard),
        )
        .await?;
        self.fpml = Some(Arc::new(decoder));
        Ok(())
    }

    pub(super) async fn decode_fpml_payloads(
        &mut self,
        decoder: Arc<FpmlDecoder>,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), ConnectorError> {
        let payloads: Vec<Vec<u8>> = self
            .poll_payload_offsets
            .iter()
            .map(|&(start, len)| self.poll_payload_buf[start..start + len].to_vec())
            .collect();
        let total = payloads.len();
        let schema = decoder.output_schema();
        let results = self
            .blocking_tasks
            .run(move || {
                payloads
                    .iter()
                    .map(|payload| decoder.decode(payload))
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|error| ConnectorError::Internal(format!("FpML decode task: {error}")))?;
        let mut batches = Vec::with_capacity(total);
        let mut good = Vec::with_capacity(total);
        let mut errors = Vec::new();
        for (index, result) in results.into_iter().enumerate() {
            match result {
                Ok(batch) if batch.num_rows() == 1 => {
                    batches.push(batch);
                    good.push(index);
                }
                Ok(batch) => errors.push(SerdeError::RecordCountMismatch {
                    expected: 1,
                    got: batch.num_rows(),
                }),
                Err(rejection) => errors.push(SerdeError::MalformedInput(rejection.to_string())),
            }
        }
        let error_count = errors.len();
        let Some(first) = errors.into_iter().next() else {
            let batch =
                arrow_select::concat::concat_batches(&schema, &batches).map_err(|error| {
                    ConnectorError::Internal(format!("failed to concat FpML rows: {error}"))
                })?;
            return Ok((batch, None));
        };
        if self.delivery != DeliveryGuarantee::BestEffort {
            return Err(terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                ConnectorError::Serde(first),
            ));
        }
        for _ in 0..error_count {
            self.metrics.record_error();
        }
        warn!(
            rejected = error_count,
            total, "skipping rejected FpML messages"
        );
        self.isolate_poison_pills(&batches, good, error_count, total, first)
    }
}
