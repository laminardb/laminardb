//! Delta Lake startup snapshot source for reference tables.

use std::collections::VecDeque;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use deltalake::DeltaTable;
use tracing::info;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::lakehouse::delta_source_config::DeltaSourceConfig;
use crate::reference::ReferenceTableSource;

use super::snapshot_schema::{conform_snapshot_batch, validate_snapshot_schema};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Ready,
    Draining,
    Done,
    Closed,
}

/// A finite snapshot of one Delta Lake table.
pub struct DeltaReferenceTableSource {
    config: DeltaSourceConfig,
    declared_schema: SchemaRef,
    phase: Phase,
    table: Option<DeltaTable>,
    pending_batches: VecDeque<RecordBatch>,
}

impl DeltaReferenceTableSource {
    /// Creates a source from parsed connector configuration and the declared table schema.
    #[must_use]
    pub fn from_source_config(config: DeltaSourceConfig, declared_schema: SchemaRef) -> Self {
        Self {
            config,
            declared_schema,
            phase: Phase::Ready,
            table: None,
            pending_batches: VecDeque::new(),
        }
    }

    /// Creates a source from SQL connector properties and the declared table schema.
    ///
    /// # Errors
    ///
    /// Returns an error when the Delta connector configuration is invalid.
    pub fn from_connector_config(
        config: &ConnectorConfig,
        declared_schema: SchemaRef,
    ) -> Result<Self, ConnectorError> {
        Ok(Self::from_source_config(
            DeltaSourceConfig::from_config(config)?,
            declared_schema,
        ))
    }

    async fn open_table(&mut self) -> Result<(), ConnectorError> {
        use crate::lakehouse::delta_io;

        let (resolved_path, resolved_options) = delta_io::resolve_catalog_options(
            &self.config.catalog_type,
            self.config.catalog_database.as_deref(),
            self.config.catalog_name.as_deref(),
            self.config.catalog_schema.as_deref(),
            &self.config.table_path,
            &self.config.storage_options,
        )
        .await?;
        self.table =
            Some(delta_io::open_or_create_table(&resolved_path, resolved_options, None).await?);
        Ok(())
    }

    async fn load_snapshot(&mut self) -> Result<(), ConnectorError> {
        use crate::lakehouse::delta_io;

        let version = delta_io::get_latest_version(
            self.table
                .as_mut()
                .ok_or_else(|| ConnectorError::Internal("Delta table is not open".into()))?,
        )
        .await?;
        let batches = delta_io::read_batches_at_version(
            self.table
                .as_mut()
                .ok_or_else(|| ConnectorError::Internal("Delta table is not open".into()))?,
            version,
            usize::MAX,
        )
        .await?
        .0;

        if batches.is_empty() {
            let table_schema = delta_io::get_table_schema(
                self.table
                    .as_ref()
                    .ok_or_else(|| ConnectorError::Internal("Delta table is not open".into()))?,
            )
            .map_err(|error| {
                ConnectorError::ReadError(format!("read Delta snapshot schema: {error}"))
            })?;
            validate_snapshot_schema(table_schema.as_ref(), self.declared_schema.as_ref())?;
        }

        let batches = batches
            .into_iter()
            .map(|batch| conform_snapshot_batch(&batch, &self.declared_schema))
            .collect::<Result<Vec<_>, _>>()?;
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        info!(version, rows, "Delta reference snapshot loaded");
        self.pending_batches = batches.into();
        Ok(())
    }
}

#[async_trait::async_trait]
impl ReferenceTableSource for DeltaReferenceTableSource {
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
                self.open_table().await?;
                self.load_snapshot().await?;
                self.phase = Phase::Draining;
            }
            Phase::Draining => {}
        }

        if let Some(batch) = self.pending_batches.pop_front() {
            return Ok(Some(batch));
        }
        self.phase = Phase::Done;
        Ok(None)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.phase = Phase::Closed;
        self.pending_batches.clear();
        self.table = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
