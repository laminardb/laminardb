//! Validate and install a writer before opening transport resources.

use arrow_schema::{DataType, SchemaRef};

use super::{NatsSink, NatsSinkConfig, SubjectSpec};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::schema::require_column;
use crate::schema::resolution::{logical_binding, SchemaBinding, SchemaDirection, SchemaOrigin};
use crate::serde::Format;

pub(super) fn resolve(
    config: &ConnectorConfig,
    input: &SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = NatsSinkConfig::from_config(config)?;
    let routing = routing_columns(&parsed);
    for (key, name) in &routing {
        require_column(
            input,
            &format!("NATS {key}"),
            name,
            &[DataType::Utf8, DataType::Utf8View],
            "Utf8 or Utf8View",
        )?;
    }
    crate::serde::schema_contract::validate_writer(parsed.format, input).map_err(|error| {
        match error {
            ConnectorError::SchemaMismatch(message)
                if parsed.format == Format::Raw && !routing.is_empty() =>
            {
                ConnectorError::SchemaMismatch(format!(
                    "{message}; NATS writes every query column into the payload, subject, header \
                     and dedup columns included"
                ))
            }
            other => other,
        }
    })?;
    logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, input)
}

/// The query columns that carry per-message subjects, headers and dedup ids, with their keys.
fn routing_columns(parsed: &NatsSinkConfig) -> Vec<(&'static str, &str)> {
    let mut columns = Vec::new();
    if let SubjectSpec::Column(name) = &parsed.subject {
        columns.push(("subject.column", name.as_str()));
    }
    columns.extend(
        parsed
            .header_columns
            .iter()
            .map(|name| ("header.columns", name.as_ref())),
    );
    columns.extend(
        parsed
            .dedup_id_column
            .as_deref()
            .map(|name| ("dedup.id.column", name)),
    );
    columns
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
