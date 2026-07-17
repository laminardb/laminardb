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
        let batches = if self.config.partition_filter.is_some() {
            self.load_filtered_snapshot(version).await?
        } else {
            delta_io::read_batches_at_version(
                self.table
                    .as_mut()
                    .ok_or_else(|| ConnectorError::Internal("Delta table is not open".into()))?,
                version,
                usize::MAX,
            )
            .await?
            .0
        };

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

    async fn load_filtered_snapshot(
        &mut self,
        version: i64,
    ) -> Result<Vec<RecordBatch>, ConnectorError> {
        use tokio_stream::StreamExt;

        let table = self
            .table
            .as_mut()
            .ok_or_else(|| ConnectorError::Internal("Delta table is not open".into()))?;
        table
            .load_version(version)
            .await
            .map_err(|error| ConnectorError::ReadError(format!("load Delta version: {error}")))?;
        let provider = table
            .table_provider()
            .build()
            .await
            .map_err(|error| ConnectorError::ReadError(format!("build Delta scan: {error}")))?;

        let context = datafusion::prelude::SessionContext::new();
        context
            .register_table("delta_reference_scan", std::sync::Arc::new(provider))
            .map_err(|error| ConnectorError::ReadError(format!("register Delta scan: {error}")))?;
        let filter = self.config.partition_filter.as_deref().ok_or_else(|| {
            ConnectorError::Internal("filtered Delta snapshot has no filter".into())
        })?;
        let dataframe = context
            .sql(&format!(
                "SELECT * FROM delta_reference_scan WHERE {filter}"
            ))
            .await
            .map_err(|error| {
                ConnectorError::ReadError(format!("plan filtered Delta scan: {error}"))
            })?;
        let mut stream = dataframe.execute_stream().await.map_err(|error| {
            ConnectorError::ReadError(format!("execute filtered Delta scan: {error}"))
        })?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|error| {
                ConnectorError::ReadError(format!("read filtered Delta batch: {error}"))
            })?;
            if batch.num_rows() != 0 {
                batches.push(batch);
            }
        }
        Ok(batches)
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
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn declared_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    #[test]
    fn construction_carries_declared_schema() {
        let source = DeltaReferenceTableSource::from_source_config(
            DeltaSourceConfig::new("/tmp/test_delta"),
            declared_schema(),
        );
        assert_eq!(source.declared_schema.field(0).name(), "id");
        assert!(!source.declared_schema.field(0).is_nullable());
    }

    #[test]
    fn missing_table_path_is_rejected() {
        let config = ConnectorConfig::new("delta-lake");
        assert!(
            DeltaReferenceTableSource::from_connector_config(&config, declared_schema()).is_err()
        );
    }

    #[tokio::test]
    async fn close_is_idempotent_and_prevents_reads() {
        let mut source = DeltaReferenceTableSource::from_source_config(
            DeltaSourceConfig::new("/tmp/test_delta"),
            declared_schema(),
        );
        source.close().await.unwrap();
        source.close().await.unwrap();
        assert!(source.poll_snapshot().await.is_err());
    }

    mod integration {
        use std::collections::HashMap;

        use arrow_array::{Int64Array, StringArray};
        use deltalake::protocol::SaveMode;
        use tempfile::TempDir;

        use super::*;

        fn physical_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ]))
        }

        fn declared_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]))
        }

        async fn write_table(path: &str) {
            use crate::lakehouse::delta_io;

            let schema = physical_schema();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["one", "two"])),
                ],
            )
            .unwrap();
            let table = delta_io::open_or_create_table(path, HashMap::new(), Some(&schema))
                .await
                .unwrap();
            delta_io::write_batches(
                table,
                vec![batch],
                SaveMode::Append,
                None,
                false,
                None,
                None,
            )
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn snapshot_uses_declared_schema_and_exhausts() {
            let directory = TempDir::new().unwrap();
            let path = directory.path().to_str().unwrap();
            write_table(path).await;
            let mut source = DeltaReferenceTableSource::from_source_config(
                DeltaSourceConfig::new(path),
                declared_schema(),
            );

            let batch = source.poll_snapshot().await.unwrap().unwrap();
            assert_eq!(batch.schema(), declared_schema());
            assert_eq!(batch.num_rows(), 2);
            assert!(source.poll_snapshot().await.unwrap().is_none());
            assert!(source.poll_snapshot().await.unwrap().is_none());
        }

        #[tokio::test]
        async fn incompatible_declared_schema_fails_closed() {
            let directory = TempDir::new().unwrap();
            let path = directory.path().to_str().unwrap();
            write_table(path).await;
            let incompatible = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let mut source = DeltaReferenceTableSource::from_source_config(
                DeltaSourceConfig::new(path),
                incompatible,
            );

            assert!(source.poll_snapshot().await.is_err());
        }
    }
}
