//! Serializer selection and schema-registry transitions for Kafka sink batches.

use std::sync::Arc;

use arrow_schema::SchemaRef;

use super::super::avro_serializer::AvroSerializer;
use super::super::schema_registry::SchemaRegistryClient;
use super::KafkaSink;
use crate::error::ConnectorError;
use crate::serde::{self, Format, RecordSerializer};

impl KafkaSink {
    pub(super) async fn install_schema_contract(
        &mut self,
        config: &crate::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        let input = config
            .arrow_schema()
            .unwrap_or_else(|| Arc::clone(&self.schema));
        let binding = if let Some(binding) = config.schema_binding() {
            binding.clone()
        } else {
            let metadata = super::super::schema_configuration::sink(config, &self.config);
            let mut binding = super::super::schema_resolution::resolve_sink_with_registry(
                &metadata,
                input,
                self.schema_registry.as_deref(),
            )
            .await?;
            super::super::schema_resolution::prepare_sink(&metadata, &mut binding).await?;
            binding
        };
        self.schema = Arc::new(binding.logical.clone());
        self.writer_schema = None;
        self.writer_projection.clear();
        if self.config.format == Format::Avro {
            let writer = super::super::schema_resolution::writer_schema(&binding)?;
            let values = super::super::schema_resolution::sink_value_schema(&binding)?;
            let projection = writer
                .fields()
                .iter()
                .map(|field| {
                    values.index_of(field.name()).map_err(|_| {
                        ConnectorError::SchemaMismatch(format!(
                            "writer field '{}' has no query output",
                            field.name()
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.avro_schema_id.store(
                super::super::schema_resolution::contract_schema_id(&binding)?,
                std::sync::atomic::Ordering::Relaxed,
            );
            self.serializer = select_serializer(
                self.config.format,
                &writer,
                Arc::clone(&self.avro_schema_id),
                None,
            )?;
            self.writer_projection = projection;
            self.writer_schema = Some(writer);
        } else {
            self.serializer = select_serializer(
                self.config.format,
                &self.schema,
                Arc::clone(&self.avro_schema_id),
                None,
            )?;
        }
        Ok(())
    }

    pub(super) fn ensure_schema_ready(&mut self, schema: &SchemaRef) -> Result<(), ConnectorError> {
        if schema.fields() != self.schema.fields() {
            return Err(ConnectorError::SchemaMismatch(
                "Kafka sink input differs from its committed query schema; migrate the catalog"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Selects the serializer for a configured Kafka format.
pub(super) fn select_serializer(
    format: Format,
    schema: &SchemaRef,
    schema_id: Arc<std::sync::atomic::AtomicU32>,
    registry: Option<Arc<SchemaRegistryClient>>,
) -> Result<Box<dyn RecordSerializer>, ConnectorError> {
    match format {
        Format::Avro => {
            let serializer =
                AvroSerializer::with_shared_schema_id(Arc::clone(schema), schema_id, registry);
            if serializer.schema_id() > 0 {
                serializer.prepare().map_err(ConnectorError::Serde)?;
            }
            Ok(Box::new(serializer))
        }
        other => serde::create_serializer(other).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "unsupported sink format '{other}': {error}"
            ))
        }),
    }
}
