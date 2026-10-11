//! Cold native writer resolution, mapping installation, and writer-contract validation.

use super::{Arc, ConnectorError, DeltaLakeSink, DeltaWriteMode, SchemaRef};
#[cfg(feature = "delta-lake")]
use super::{DataType, DeltaTable};

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

    /// Changelog metadata columns stripped from the target Delta table schema
    /// in upsert mode, so the table holds only user data — not the CDC `_op`/
    /// `_ts_ms` envelope or the Z-set `__weight` column. `collapse_changelog`
    /// strips the same columns from the MERGE source, keeping the two in sync.
    const CHANGELOG_METADATA_COLUMNS: &'static [&'static str] =
        &["_op", "_ts_ms", laminar_core::changelog::WEIGHT_COLUMN];

    pub(in crate::lakehouse) fn target_schema(
        batch_schema: &SchemaRef,
        write_mode: DeltaWriteMode,
    ) -> SchemaRef {
        if write_mode == DeltaWriteMode::Upsert {
            let fields: Vec<_> = batch_schema
                .fields()
                .iter()
                .filter(|f| !Self::CHANGELOG_METADATA_COLUMNS.contains(&f.name().as_str()))
                .cloned()
                .collect();
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            batch_schema.clone()
        }
    }

    #[cfg(feature = "delta-lake")]
    pub(super) fn validate_weight_column(
        schema: &SchemaRef,
        owner: &str,
    ) -> Result<(), ConnectorError> {
        let Ok(field) = schema.field_with_name(laminar_core::changelog::WEIGHT_COLUMN) else {
            return Ok(());
        };
        if field.data_type() != &DataType::Int64 || field.is_nullable() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "{owner} '{}' column must be non-null Int64",
                laminar_core::changelog::WEIGHT_COLUMN
            )));
        }
        Ok(())
    }

    #[cfg(feature = "delta-lake")]
    pub(super) fn validate_existing_table_schema(
        pipeline: &SchemaRef,
        table: &SchemaRef,
        schema_evolution: bool,
        exact_schema_required: bool,
    ) -> Result<(), ConnectorError> {
        Self::validate_weight_column(pipeline, "pipeline")?;
        Self::validate_weight_column(table, "Delta table")?;

        let weight = laminar_core::changelog::WEIGHT_COLUMN;
        if pipeline.field_with_name(weight).is_ok() != table.field_with_name(weight).is_ok() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "pipeline and existing Delta table must both contain '{weight}' for a weighted changelog"
            )));
        }

        // INVARIANT: identity is checked in Delta storage form (ms -> us).
        let pipeline = super::super::delta_io::widen_millisecond_timestamps(pipeline);

        if exact_schema_required {
            if pipeline.as_ref() != table.as_ref() {
                return Err(ConnectorError::SchemaMismatch(
                    "pipeline schema must exactly match the existing Delta table for coordinated append"
                        .into(),
                ));
            }
            return Ok(());
        }

        let allow_evolution = schema_evolution;
        for input_field in pipeline.fields() {
            let Ok(table_field) = table.field_with_name(input_field.name()) else {
                if allow_evolution {
                    continue;
                }
                return Err(ConnectorError::SchemaMismatch(format!(
                    "pipeline column '{}' is missing from the existing Delta table",
                    input_field.name()
                )));
            };
            // Only the lossless top-level ms -> us widening above is applied; any other
            // difference (precision, timezone, nested timestamp unit) would be a lossy cast.
            if !input_field
                .data_type()
                .equals_datatype(table_field.data_type())
            {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "pipeline column '{}' type {} differs from Delta type {}; cast it explicitly \
                     in CREATE STREAM",
                    input_field.name(),
                    input_field.data_type(),
                    table_field.data_type()
                )));
            }
            if input_field.is_nullable() && !table_field.is_nullable() {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "nullable pipeline column '{}' cannot target a non-null Delta column",
                    input_field.name()
                )));
            }
        }

        for table_field in table.fields() {
            if pipeline.field_with_name(table_field.name()).is_err()
                && (!allow_evolution || !table_field.is_nullable())
            {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "existing Delta column '{}' is missing from the pipeline schema",
                    table_field.name()
                )));
            }
        }
        Ok(())
    }

    /// MERGE matches keys with SQL equality, under which a NULL key never matches: every flush
    /// would insert a new row for it and its deletes would be lost. Merge keys must therefore be
    /// declared non-nullable, as the PostgreSQL and MongoDB upsert sinks require; Arrow batch
    /// validation then keeps NULL values out of the frozen query schema.
    #[cfg(feature = "delta-lake")]
    pub(in crate::lakehouse) fn validate_merge_key_fields(
        schema: &SchemaRef,
        merge_key_columns: &[String],
    ) -> Result<(), ConnectorError> {
        for key in merge_key_columns {
            let field = schema.field_with_name(key).map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "merge key column '{key}' is not a column of the Delta upsert target"
                ))
            })?;
            if field.is_nullable() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "merge key column '{key}' must be non-nullable; cast or filter it in \
                     CREATE STREAM (e.g. COALESCE) before upserting into Delta"
                )));
            }
        }
        Ok(())
    }

    /// A configured partitioning must match the existing table exactly; an empty configuration
    /// writes with the table's own partitioning.
    #[cfg(feature = "delta-lake")]
    pub(super) fn validate_partition_columns(
        &self,
        table: &DeltaTable,
    ) -> Result<(), ConnectorError> {
        if self.config.partition_columns.is_empty() {
            return Ok(());
        }
        let existing = super::super::delta_io::get_partition_columns(table);
        if existing != self.config.partition_columns {
            return Err(ConnectorError::SchemaMismatch(format!(
                "configured partition.columns {:?} differ from the existing Delta table's \
                 partitioning {existing:?}; repartitioning requires a table migration",
                self.config.partition_columns
            )));
        }
        Ok(())
    }

    #[cfg(feature = "delta-lake")]
    pub(super) fn validate_reopened_table(&self, table: &DeltaTable) -> Result<(), ConnectorError> {
        if table.version().is_none() {
            return Err(ConnectorError::SchemaMismatch(
                "Delta target disappeared after activation; a committed table is never recreated \
                 by a retry"
                    .into(),
            ));
        }
        super::super::schema_resolution::verify_identity(
            self.schema_binding.as_ref(),
            &super::super::schema_resolution::delta_native(table)?,
        )?;
        let table_schema = super::super::delta_io::get_table_schema(table)?;
        let pipeline_schema = self
            .schema
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "bound Delta writer schema".into(),
                actual: "writer schema missing".into(),
            })?;
        Self::validate_existing_table_schema(
            pipeline_schema,
            &table_schema,
            self.config.schema_evolution,
            self.is_coordinated(),
        )?;
        self.validate_partition_columns(table)
    }
}
