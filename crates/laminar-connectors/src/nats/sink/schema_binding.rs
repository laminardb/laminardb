//! Validate and install a writer before opening transport resources.

use arrow_schema::SchemaRef;

use super::{NatsSink, NatsSinkConfig};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::schema::resolution::{logical_binding, SchemaBinding, SchemaDirection, SchemaOrigin};

pub(super) fn resolve(
    config: &ConnectorConfig,
    input: &SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = NatsSinkConfig::from_config(config)?;
    crate::serde::schema_contract::validate_writer(parsed.format, input)?;
    logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, input)
}

impl NatsSink {
    pub(super) fn prepare_writer(
        &mut self,
        config: &ConnectorConfig,
        parsed: &NatsSinkConfig,
    ) -> Result<(), ConnectorError> {
        let input = config
            .arrow_schema()
            .or_else(|| {
                config
                    .schema_binding()
                    .map(|binding| std::sync::Arc::new(binding.logical.clone()))
            })
            .unwrap_or_else(|| self.schema.clone());
        let current = resolve(config, &input)?;
        if config
            .schema_binding()
            .is_some_and(|expected| expected != &current)
        {
            return Err(ConnectorError::SchemaMismatch(
                "NATS writer differs from its committed query contract".into(),
            ));
        }
        self.serializer = Some(crate::serde::create_serializer(parsed.format)?);
        self.schema = input;
        Ok(())
    }
}
