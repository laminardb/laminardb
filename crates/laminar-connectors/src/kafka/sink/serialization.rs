//! Serializer selection and schema-registry transitions for Kafka sink batches.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use tracing::{debug, info};

use super::super::avro_serializer::AvroSerializer;
use super::super::schema_registry::SchemaRegistryClient;
use super::KafkaSink;
use crate::error::ConnectorError;
use crate::serde::{self, Format, RecordSerializer};

impl KafkaSink {
    /// Ensures the sink schema and registry entry match the incoming batch.
    pub(super) async fn ensure_schema_ready(
        &mut self,
        batch_schema: &SchemaRef,
    ) -> Result<(), ConnectorError> {
        let schema_changed = self.schema != *batch_schema;
        let needs_registration = self.config.format == Format::Avro
            && (schema_changed
                || self
                    .avro_schema_id
                    .load(std::sync::atomic::Ordering::Relaxed)
                    == 0);

        // Register before advancing the serializer so a failure cannot leave a new serializer
        // paired with the old registry ID.
        if needs_registration {
            if let Some(ref registry) = self.schema_registry {
                let subject = format!("{}-value", self.config.topic);
                let avro_schema = super::super::schema_registry::arrow_to_avro_schema(
                    batch_schema,
                    &self.config.topic,
                )
                .map_err(ConnectorError::Serde)?;
                let schema_id = registry
                    .register_schema(
                        &subject,
                        &avro_schema,
                        super::super::schema_registry::SchemaType::Avro,
                    )
                    .await?;
                #[allow(clippy::cast_sign_loss)]
                self.avro_schema_id
                    .store(schema_id as u32, std::sync::atomic::Ordering::Relaxed);
                info!(subject = %subject, schema_id, "registered Avro schema");
            }
        }

        if schema_changed {
            debug!(
                old = ?self.schema.fields().iter().map(|field| field.name()).collect::<Vec<_>>(),
                new = ?batch_schema.fields().iter().map(|field| field.name()).collect::<Vec<_>>(),
                "sink schema updated from incoming batch"
            );
            self.schema = batch_schema.clone();
            self.serializer = select_serializer(
                self.config.format,
                &self.schema,
                Arc::clone(&self.avro_schema_id),
                self.schema_registry.clone(),
            )?;
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
        Format::Avro => Ok(Box::new(AvroSerializer::with_shared_schema_id(
            schema.clone(),
            schema_id,
            registry,
        ))),
        other => serde::create_serializer(other).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "unsupported sink format '{other}': {error}"
            ))
        }),
    }
}
