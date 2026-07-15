//! Iceberg startup snapshot source for reference tables.

#[cfg(feature = "iceberg")]
use std::collections::VecDeque;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "iceberg")]
use tracing::info;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;
#[cfg(feature = "iceberg")]
use crate::reference::{conform_snapshot_batch, validate_snapshot_schema};

use super::iceberg_config::IcebergSourceConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Ready,
    #[cfg(feature = "iceberg")]
    Draining,
    #[cfg(feature = "iceberg")]
    Done,
    Closed,
}

/// A finite snapshot of one Iceberg table.
pub struct IcebergReferenceTableSource {
    #[cfg(feature = "iceberg")]
    config: IcebergSourceConfig,
    #[cfg(feature = "iceberg")]
    declared_schema: SchemaRef,
    phase: Phase,
    #[cfg(feature = "iceberg")]
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
        #[cfg(feature = "iceberg")]
        let config = {
            let mut config = config;
            config.select_columns = declared_columns;
            config
        };
        #[cfg(not(feature = "iceberg"))]
        let _ = (config, declared_schema, declared_columns);
        Ok(Self {
            #[cfg(feature = "iceberg")]
            config,
            #[cfg(feature = "iceberg")]
            declared_schema,
            phase: Phase::Ready,
            #[cfg(feature = "iceberg")]
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

    #[cfg(feature = "iceberg")]
    async fn load_initial_snapshot(&mut self) -> Result<(), ConnectorError> {
        let catalog = super::iceberg_io::build_catalog(&self.config.catalog).await?;
        let table = super::iceberg_io::load_table(
            catalog.as_ref(),
            &self.config.catalog.namespace,
            &self.config.catalog.table_name,
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
        #[cfg(not(feature = "iceberg"))]
        {
            return match self.phase {
                Phase::Closed => Err(ConnectorError::InvalidState {
                    expected: "open reference snapshot source".into(),
                    actual: "closed".into(),
                }),
                Phase::Ready => Err(ConnectorError::ConfigurationError(
                    "Iceberg reference tables require the 'iceberg' feature".into(),
                )),
            };
        }

        #[cfg(feature = "iceberg")]
        {
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
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.phase = Phase::Closed;
        #[cfg(feature = "iceberg")]
        self.snapshot_batches.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn test_source_config() -> IcebergSourceConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("warehouse", "s3://test/wh");
        config.set("namespace", "test");
        config.set("table.name", "dim_customers");
        IcebergSourceConfig::from_config(&config).unwrap()
    }

    fn declared_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn construction_carries_declared_non_null_key_schema() {
        let source =
            IcebergReferenceTableSource::new(test_source_config(), declared_schema()).unwrap();
        assert_eq!(source.config.select_columns, vec!["id", "name"]);
        assert_eq!(source.declared_schema.field(0).name(), "id");
        assert!(!source.declared_schema.field(0).is_nullable());
    }

    #[test]
    fn conflicting_explicit_projection_is_rejected() {
        let mut config = test_source_config();
        config.select_columns = vec!["name".into()];
        assert!(IcebergReferenceTableSource::new(config, declared_schema()).is_err());
    }

    #[cfg(feature = "iceberg")]
    #[tokio::test]
    async fn exhaustion_and_close_are_stable_without_external_io() {
        let mut source =
            IcebergReferenceTableSource::new(test_source_config(), declared_schema()).unwrap();
        source.phase = Phase::Draining;
        assert!(source.poll_snapshot().await.unwrap().is_none());
        assert!(source.poll_snapshot().await.unwrap().is_none());
        source.close().await.unwrap();
        source.close().await.unwrap();
        assert!(source.poll_snapshot().await.is_err());
    }
}
