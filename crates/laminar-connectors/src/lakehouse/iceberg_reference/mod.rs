//! Iceberg startup snapshot source for reference tables.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures_util::StreamExt;
use iceberg::scan::ArrowRecordBatchStream;
use iceberg::spec::SnapshotRef;
use tracing::info;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;

use super::iceberg_config::IcebergReadMode;
use super::iceberg_config::IcebergSourceConfig;
use super::iceberg_scan::{
    connector_scan_error, plan_files, preflight_snapshot, ManifestReadLimits,
};
use super::snapshot_schema::{conform_snapshot_batch, validate_snapshot_schema};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Ready,
    Draining,
    Done,
    Failed,
    Closed,
}

/// A finite snapshot of one Iceberg table.
pub struct IcebergReferenceTableSource {
    config: IcebergSourceConfig,
    declared_schema: SchemaRef,
    phase: Phase,
    snapshot_stream: Option<ArrowRecordBatchStream>,
    snapshot_id: Option<i64>,
    emitted_rows: u64,
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
        config.validate_read_limits()?;
        if config.read_mode != IcebergReadMode::Snapshot {
            return Err(ConnectorError::ConfigurationError(
                "Iceberg reference tables require read.mode=snapshot".into(),
            ));
        }
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
            snapshot_stream: None,
            snapshot_id: None,
            emitted_rows: 0,
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

        let snapshot = selected_snapshot(&table, &self.config)?;
        let snapshot_schema = match &snapshot {
            Some(snapshot) => snapshot
                .schema(table.metadata())
                .map_err(|error| connector_scan_error("resolve Iceberg snapshot schema", &error))?,
            None => table.current_schema_ref(),
        };
        let predicate = super::iceberg_scan::parse_and_bind_filter(
            self.config.filter.as_deref(),
            snapshot_schema.clone(),
        )?;
        let physical_schema = iceberg::arrow::schema_to_arrow_schema(&snapshot_schema)
            .map_err(|error| connector_scan_error("convert Iceberg snapshot schema", &error))?;
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
        let Some(snapshot) = snapshot else {
            return Ok(());
        };

        let mut builder = table
            .scan()
            .snapshot_id(snapshot.snapshot_id())
            .with_batch_size(Some(8_192))
            .with_concurrency_limit(self.config.scan_concurrency)
            .select(self.config.select_columns.iter().map(String::as_str));
        if let Some(predicate) = predicate {
            builder = builder.with_filter(predicate);
        }
        let scan = builder
            .build()
            .map_err(|error| connector_scan_error("build Iceberg reference scan", &error))?;
        let deadline = tokio::time::Instant::now() + self.config.storage.request_timeout;
        preflight_snapshot(
            &table,
            &snapshot,
            ManifestReadLimits::from_source(&self.config),
            deadline,
        )
        .await?;
        let tasks = plan_files(&scan, self.config.max_planned_files, deadline).await?;
        let reader = table
            .reader_builder()
            .with_batch_size(8_192)
            .with_data_file_concurrency_limit(self.config.scan_concurrency)
            .build()
            .read(tasks)
            .map_err(|error| connector_scan_error("create Iceberg reference reader", &error))?;
        self.snapshot_id = Some(snapshot.snapshot_id());
        self.snapshot_stream = Some(reader.stream());
        Ok(())
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        loop {
            let Some(stream) = self.snapshot_stream.as_mut() else {
                return Ok(None);
            };
            let next = tokio::time::timeout(self.config.storage.request_timeout, stream.next())
                .await
                .map_err(|_| {
                    ConnectorError::ReadError(
                        "[LDB-ICEBERG-STORAGE-TIMEOUT] reference snapshot read made no progress"
                            .into(),
                    )
                })?;
            let Some(result) = next else {
                self.snapshot_stream = None;
                return Ok(None);
            };
            let batch = result.map_err(|error| {
                connector_scan_error("Iceberg reference snapshot read failed", &error)
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = conform_snapshot_batch(&batch, &self.declared_schema)?;
            self.emitted_rows = self
                .emitted_rows
                .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
            return Ok(Some(batch));
        }
    }
}

fn selected_snapshot(
    table: &iceberg::table::Table,
    config: &IcebergSourceConfig,
) -> Result<Option<SnapshotRef>, ConnectorError> {
    if let Some(snapshot_id) = config.snapshot_id {
        return table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                ConnectorError::ReadError(format!(
                    "[LDB-ICEBERG-SNAPSHOT-MISSING] snapshot {snapshot_id} does not exist"
                ))
            });
    }
    if let Some(snapshot) = table.metadata().snapshot_for_ref(&config.table_ref) {
        return Ok(Some(snapshot.clone()));
    }
    if config.table_ref == "main" && table.metadata().current_snapshot().is_none() {
        return Ok(None);
    }
    Err(ConnectorError::ReadError(format!(
        "[LDB-ICEBERG-REF-MISSING] table ref '{}' does not exist",
        config.table_ref
    )))
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
            Phase::Failed => {
                return Err(ConnectorError::InvalidState {
                    expected: "readable reference snapshot source".into(),
                    actual: "failed".into(),
                });
            }
            Phase::Ready => {
                if let Err(error) = self.load_initial_snapshot().await {
                    self.phase = Phase::Failed;
                    return Err(error);
                }
                self.phase = Phase::Draining;
            }
            Phase::Draining => {}
        }

        match self.next_batch().await {
            Ok(Some(batch)) => Ok(Some(batch)),
            Ok(None) => {
                self.phase = Phase::Done;
                info!(
                    snapshot = ?self.snapshot_id,
                    rows = self.emitted_rows,
                    "Iceberg reference snapshot completed"
                );
                Ok(None)
            }
            Err(error) => {
                self.snapshot_stream = None;
                self.phase = Phase::Failed;
                Err(error)
            }
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.phase = Phase::Closed;
        self.snapshot_stream = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
