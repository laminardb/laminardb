//! Iceberg startup snapshot source for reference tables.

use std::collections::VecDeque;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tracing::info;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;

use super::iceberg_config::IcebergSourceConfig;
use super::snapshot_schema::{conform_snapshot_batch, validate_snapshot_schema};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Ready,
    Draining,
    Done,
    Closed,
}

/// A finite snapshot of one Iceberg table.
pub struct IcebergReferenceTableSource {
    config: IcebergSourceConfig,
    declared_schema: SchemaRef,
    phase: Phase,
    snapshot_batches: VecDeque<RecordBatch>,
}

impl IcebergReferenceTableSource {
    /// Creates a source from parsed configuration and the declared table schema.
    ///
    /// # Errors
    ///
    /// Returns an error when an explicit projection conflicts with the declared schema.
    pub fn new(
        config: IcebergSourceConfig,
        declared_schema: SchemaRef,
    ) -> Result<Self, ConnectorError> {
        let declared_columns = declared_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        if !config.select_columns.is_empty() && config.select_columns != declared_columns {
            return Err(ConnectorError::ConfigurationError(
                "Iceberg select.columns must exactly match the declared reference-table columns"
                    .into(),
            ));
        }
        let config = {
            let mut config = config;
            config.select_columns = declared_columns;
            config
        };
        Ok(Self {
            config,
            declared_schema,
            phase: Phase::Ready,
            snapshot_batches: VecDeque::new(),
        })
    }

    /// Creates a source from SQL connector properties and the declared table schema.
    ///
    /// # Errors
    ///
    /// Returns an error when the Iceberg configuration or projection is invalid.
    pub fn from_connector_config(
        config: &ConnectorConfig,
        declared_schema: SchemaRef,
    ) -> Result<Self, ConnectorError> {
        Self::new(IcebergSourceConfig::from_config(config)?, declared_schema)
    }

    async fn load_initial_snapshot(&mut self) -> Result<(), ConnectorError> {
        let catalog =
            super::iceberg_io::build_catalog(&self.config.catalog, &self.config.storage).await?;
        let table = super::iceberg_io::load_table_with_timeout(
            catalog.as_ref(),
            &self.config.catalog.namespace,
            &self.config.catalog.table_name,
            self.config.catalog.request_timeout,
        )
        .await?;

        let physical_schema =
            match iceberg::arrow::schema_to_arrow_schema(&table.current_schema_ref()) {
                Ok(schema) => schema,
                Err(error) => {
                    return Err(ConnectorError::SchemaMismatch(format!(
                        "Iceberg to Arrow schema: {error}"
                    )));
                }
            };
        let projected_fields = self
            .declared_schema
            .fields()
            .iter()
            .map(|declared| {
                physical_schema
                    .index_of(declared.name())
                    .map(|index| physical_schema.field(index).clone())
                    .map_err(|_| {
                        ConnectorError::ReadError(format!(
                            "Iceberg snapshot is missing declared column '{}'",
                            declared.name()
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let projected_schema = arrow_schema::Schema::new(projected_fields);
        validate_snapshot_schema(&projected_schema, self.declared_schema.as_ref())?;

        let snapshot_id = self
            .config
            .snapshot_id
            .or_else(|| super::iceberg_io::current_snapshot_id(&table));
        let batches =
            super::iceberg_io::scan_table(&table, snapshot_id, &self.config.select_columns)
                .await?
                .into_iter()
                .map(|batch| conform_snapshot_batch(&batch, &self.declared_schema))
                .collect::<Result<Vec<_>, _>>()?;
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        info!(snapshot = ?snapshot_id, rows, "Iceberg reference snapshot loaded");
        self.snapshot_batches = batches.into();
        Ok(())
    }
}

#[async_trait]
impl ReferenceTableSource for IcebergReferenceTableSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        match self.phase {
            Phase::Closed => {
                return Err(ConnectorError::InvalidState {
                    expected: "open reference snapshot source".into(),
                    actual: "closed".into(),
                });
            }
            Phase::Done => return Ok(None),
            Phase::Ready => {
                self.load_initial_snapshot().await?;
                self.phase = Phase::Draining;
            }
            Phase::Draining => {}
        }

        if let Some(batch) = self.snapshot_batches.pop_front() {
            return Ok(Some(batch));
        }
        self.phase = Phase::Done;
        Ok(None)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.phase = Phase::Closed;
        self.snapshot_batches.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
