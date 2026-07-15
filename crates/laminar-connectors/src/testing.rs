//! Testing utilities for connector implementations.
//!
//! Provides mock connectors and helper functions for testing
//! the connector SDK and concrete connector implementations.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use parking_lot::Mutex;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorInfo};
use crate::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, SourceBatch,
    SourceConnector, WriteResult,
};
use crate::error::ConnectorError;
use crate::registry::ConnectorRegistry;

/// Creates a test schema with `id` (Int64) and `value` (Utf8) columns.
#[must_use]
pub fn mock_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

/// Creates a test `RecordBatch` with `n` rows.
///
/// # Panics
///
/// Panics if the batch cannot be created (should not happen with valid inputs).
#[must_use]
pub fn mock_batch(n: usize) -> RecordBatch {
    #[allow(clippy::cast_possible_wrap)]
    let ids: Vec<i64> = (0..n as i64).collect();
    let values: Vec<String> = (0..n).map(|i| format!("value_{i}")).collect();
    let value_refs: Vec<&str> = values.iter().map(String::as_str).collect();

    RecordBatch::try_new(
        mock_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(value_refs)),
        ],
    )
    .unwrap()
}

/// Mock source connector for testing.
///
/// Returns a configurable number of batches, then returns `None`.
#[derive(Debug)]
pub struct MockSourceConnector {
    schema: SchemaRef,
    initial_batches: u64,
    batches_remaining: AtomicU64,
    batch_size: usize,
    records_produced: AtomicU64,
    is_open: std::sync::atomic::AtomicBool,
    committed_epochs: Arc<Mutex<Vec<u64>>>,
}

impl MockSourceConnector {
    /// Creates a new mock source that produces 10 batches of 5 records.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: mock_schema(),
            initial_batches: 10,
            batches_remaining: AtomicU64::new(10),
            batch_size: 5,
            records_produced: AtomicU64::new(0),
            is_open: std::sync::atomic::AtomicBool::new(false),
            committed_epochs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Creates a mock source with custom batch count and size.
    #[must_use]
    pub fn with_batches(count: u64, batch_size: usize) -> Self {
        Self {
            schema: mock_schema(),
            initial_batches: count,
            batches_remaining: AtomicU64::new(count),
            batch_size,
            records_produced: AtomicU64::new(0),
            is_open: std::sync::atomic::AtomicBool::new(false),
            committed_epochs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns the total records produced.
    #[must_use]
    pub fn records_produced(&self) -> u64 {
        self.records_produced.load(Ordering::Relaxed)
    }

    /// Returns a handle for inspecting epochs reported to
    /// `notify_epoch_committed` from outside the connector (the
    /// connector itself is moved into a background task when run via
    /// the pipeline).
    #[must_use]
    pub fn committed_epochs_handle(&self) -> Arc<Mutex<Vec<u64>>> {
        Arc::clone(&self.committed_epochs)
    }
}

impl Default for MockSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SourceConnector for MockSourceConnector {
    async fn start(
        &mut self,
        request: crate::connector::SourceStart,
    ) -> Result<(), ConnectorError> {
        let records = match request.position {
            crate::connector::SourcePosition::Initial => 0,
            crate::connector::SourcePosition::Resume { checkpoint, .. } => checkpoint
                .get_offset("records")
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "mock source checkpoint is missing 'records'".into(),
                    )
                })?
                .parse::<u64>()
                .map_err(|error| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid mock source record cursor: {error}"
                    ))
                })?,
        };
        let consumed_batches = if self.batch_size == 0 {
            0
        } else {
            records / self.batch_size as u64
        };
        self.records_produced.store(records, Ordering::Relaxed);
        self.batches_remaining.store(
            self.initial_batches.saturating_sub(consumed_batches),
            Ordering::Relaxed,
        );
        self.is_open
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let remaining = self.batches_remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            return Ok(None);
        }
        self.batches_remaining.fetch_sub(1, Ordering::Relaxed);

        let batch = mock_batch(self.batch_size);
        self.records_produced
            .fetch_add(self.batch_size as u64, Ordering::Relaxed);
        Ok(Some(SourceBatch::new(batch)))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new();
        cp.set_offset(
            "records",
            self.records_produced.load(Ordering::Relaxed).to_string(),
        );
        cp
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.is_open
            .store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        _checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.committed_epochs.lock().push(epoch);
        Ok(())
    }
}

/// Mock sink connector for testing.
///
/// Stores all written batches in memory for inspection.
#[derive(Debug)]
pub struct MockSinkConnector {
    schema: SchemaRef,
    written: Arc<Mutex<Vec<RecordBatch>>>,
    records_written: AtomicU64,
    is_open: std::sync::atomic::AtomicBool,
}

impl MockSinkConnector {
    /// Creates a new mock sink.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: mock_schema(),
            written: Arc::new(Mutex::new(Vec::new())),
            records_written: AtomicU64::new(0),
            is_open: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Returns the number of batches written.
    #[must_use]
    pub fn batch_count(&self) -> usize {
        self.written.lock().len()
    }

    /// Returns the total number of records written.
    #[must_use]
    pub fn records_written(&self) -> u64 {
        self.records_written.load(Ordering::Relaxed)
    }

    /// Returns a clone of all written batches.
    #[must_use]
    pub fn written_batches(&self) -> Vec<RecordBatch> {
        self.written.lock().clone()
    }
}

impl Default for MockSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SinkConnector for MockSinkConnector {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(SinkContract::new(
            SinkConsistency::Ephemeral,
            SinkTopology::NodeLocalEgress,
            SinkInputMode::AppendOnly,
        ))
    }

    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.is_open
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        let num_rows = batch.num_rows();
        let bytes = batch.get_array_memory_size() as u64;
        self.written.lock().push(batch.clone());
        self.records_written
            .fetch_add(num_rows as u64, Ordering::Relaxed);
        Ok(WriteResult::new(num_rows, bytes))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.is_open
            .store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

/// Registers a mock source connector with the registry.
pub fn register_mock_source(registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
    registry.register_source(
        "mock",
        ConnectorInfo {
            name: "mock".to_string(),
            display_name: "Mock Source".to_string(),
            version: "0.1.0".to_string(),
            is_source: true,
            is_sink: false,
            config_keys: vec![],
        },
        Arc::new(|_: Option<&Arc<prometheus::Registry>>| Ok(Box::new(MockSourceConnector::new()))),
    )
}

/// Registers a mock sink connector with the registry.
pub fn register_mock_sink(registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
    registry.register_sink(
        "mock",
        ConnectorInfo {
            name: "mock".to_string(),
            display_name: "Mock Sink".to_string(),
            version: "0.1.0".to_string(),
            is_source: false,
            is_sink: true,
            config_keys: vec![],
        },
        Arc::new(|_config, _registry| Ok(Box::new(MockSinkConnector::new()))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_batch() {
        let batch = mock_batch(10);
        assert_eq!(batch.num_rows(), 10);
        assert_eq!(batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_mock_source_connector() {
        let mut source = MockSourceConnector::with_batches(3, 5);
        source
            .start(crate::connector::SourceStart {
                config: ConnectorConfig::new("mock"),
                position: crate::connector::SourcePosition::Initial,
                delivery: crate::connector::DeliveryGuarantee::BestEffort,
            })
            .await
            .unwrap();

        let b1 = source.poll_batch(100).await.unwrap();
        assert!(b1.is_some());
        assert_eq!(b1.unwrap().num_rows(), 5);

        let b2 = source.poll_batch(100).await.unwrap();
        assert!(b2.is_some());

        let b3 = source.poll_batch(100).await.unwrap();
        assert!(b3.is_some());

        let b4 = source.poll_batch(100).await.unwrap();
        assert!(b4.is_none());

        assert_eq!(source.records_produced(), 15);

        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("records"), Some("15"));

        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_mock_sink_connector() {
        let mut sink = MockSinkConnector::new();
        sink.open(&ConnectorConfig::new("mock")).await.unwrap();

        let batch = mock_batch(10);
        let result = sink.write_batch(&batch).await.unwrap();
        assert_eq!(result.records_written, 10);

        assert_eq!(sink.batch_count(), 1);
        assert_eq!(sink.records_written(), 10);

        sink.write_batch(&mock_batch(5)).await.unwrap();

        assert_eq!(sink.records_written(), 15);
        assert_eq!(sink.batch_count(), 2);

        let contract = sink.contract(&ConnectorConfig::new("mock")).unwrap();
        assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
        assert_eq!(contract.topology, SinkTopology::NodeLocalEgress);
        assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
        assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(60));

        sink.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_mock_source_resume() {
        let mut source = MockSourceConnector::new();
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("records", "10");
        source
            .start(crate::connector::SourceStart {
                config: ConnectorConfig::new("mock"),
                position: crate::connector::SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(5, 5),
                    checkpoint,
                },
                delivery: crate::connector::DeliveryGuarantee::AtLeastOnce,
            })
            .await
            .unwrap();
        assert_eq!(source.records_produced(), 10);
    }

    #[test]
    fn test_register_helpers() {
        let registry = ConnectorRegistry::new();
        register_mock_source(&registry).unwrap();
        register_mock_sink(&registry).unwrap();

        assert!(registry.source_info("mock").is_some());
        assert!(registry.sink_info("mock").is_some());
    }
}
