//! Cold native writer resolution and mapping installation.

#[cfg(feature = "delta-lake")]
use super::Arc;
use super::{ConnectorError, DeltaLakeSink};

impl DeltaLakeSink {
    #[cfg_attr(
        not(feature = "delta-lake"),
        allow(clippy::unused_async, clippy::unused_async_trait_impl)
    )]
    pub(super) async fn bind_writer_contract(
        &mut self,
        config: &crate::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        #[cfg(feature = "delta-lake")]
        {
            let binding = if let Some(binding) = config.schema_binding() {
                binding.clone()
            } else {
                let input = self.query_schema.clone().ok_or_else(|| {
                    ConnectorError::SchemaMismatch("Delta query schema is missing".into())
                })?;
                let deadline = self.operation_deadline();
                let mut binding = tokio::time::timeout_at(
                    deadline,
                    super::super::schema_resolution::delta_sink_with_config(
                        config,
                        &self.config,
                        input,
                    ),
                )
                .await
                .map_err(|_| {
                    ConnectorError::Timeout(
                        u64::try_from(self.config.write_timeout.as_millis()).unwrap_or(u64::MAX),
                    )
                })??;
                tokio::time::timeout_at(
                    deadline,
                    super::super::schema_resolution::prepare_delta_with_config(
                        &self.config,
                        &mut binding,
                    ),
                )
                .await
                .map_err(|_| {
                    ConnectorError::Timeout(
                        u64::try_from(self.config.write_timeout.as_millis()).unwrap_or(u64::MAX),
                    )
                })??;
                binding
            };
            let external = binding.external.as_ref().ok_or_else(|| {
                ConnectorError::SchemaMismatch("Delta writer has no prepared target fields".into())
            })?;
            self.writer_projection = Some(
                external
                    .fields()
                    .iter()
                    .map(|field| binding.logical.index_of(field.name()))
                    .chain(
                        binding
                            .control_fields
                            .iter()
                            .map(|name| binding.logical.index_of(name)),
                    )
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?,
            );
            self.schema = Some(Arc::new(external.clone()));
            self.schema_binding = Some(binding);
            Ok(())
        }
        #[cfg(not(feature = "delta-lake"))]
        {
            let _ = config;
            Err(ConnectorError::FeatureUnsupported(
                "Delta writer requires the delta-lake feature".into(),
            ))
        }
    }
}
