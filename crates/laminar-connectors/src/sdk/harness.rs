//! Test harness for connector validation.
//!
//! Provides utilities for testing source and sink connectors with configurable
//! behaviors, error injection, and comprehensive validation.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use parking_lot::Mutex;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    SinkConnector, SinkConnectorCapabilities, SourceBatch, SourceConnector, WriteResult,
};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;
use crate::registry::ConnectorRegistry;
use crate::testing::{mock_batch, mock_schema};

/// Result type for test operations.
pub type TestResult<T> = Result<T, TestError>;

/// Errors that can occur during testing.
#[derive(Debug, thiserror::Error)]
pub enum TestError {
    /// Connector operation failed.
    #[error("connector error: {0}")]
    Connector(#[from] ConnectorError),
    /// Test assertion failed.
    #[error("assertion failed: {0}")]
    Assertion(String),
    /// Test timed out.
    #[error("test timed out after {0:?}")]
    Timeout(Duration),
}

/// Metrics collected during source testing.
#[derive(Debug, Clone, Default)]
pub struct SourceTestMetrics {
    /// Total batches received.
    pub batches_received: u64,
    /// Total records received.
    pub records_received: u64,
    /// Number of poll calls.
    pub poll_calls: u64,
    /// Number of checkpoint calls.
    pub checkpoint_calls: u64,
    /// Number of restore calls.
    pub restore_calls: u64,
}

/// Metrics collected during sink testing.
#[derive(Debug, Clone, Default)]
pub struct SinkTestMetrics {
    /// Total batches written.
    pub batches_written: u64,
    /// Total records written.
    pub records_written: u64,
    /// Number of epoch begins.
    pub epoch_begins: u64,
    /// Number of epoch commits.
    pub epoch_commits: u64,
    /// Number of epoch rollbacks.
    pub epoch_rollbacks: u64,
}

/// Test harness for connector validation.
///
/// Provides a controlled environment for testing connectors with configurable
/// behaviors and comprehensive validation.
pub struct ConnectorTestHarness {
    registry: ConnectorRegistry,
}

impl Default for ConnectorTestHarness {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorTestHarness {
    /// Creates a new test harness.
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: ConnectorRegistry::new(),
        }
    }

    /// Creates a test harness with the given registry.
    #[must_use]
    pub fn with_registry(registry: ConnectorRegistry) -> Self {
        Self { registry }
    }

    /// Returns a reference to the registry.
    #[must_use]
    pub fn registry(&self) -> &ConnectorRegistry {
        &self.registry
    }

    /// Tests a source connector by polling batches.
    ///
    /// # Arguments
    ///
    /// * `source` - The source connector to test
    /// * `config` - Configuration for the source
    /// * `max_batches` - Maximum number of batches to poll
    /// * `max_records_per_batch` - Maximum records per poll call
    ///
    /// # Errors
    ///
    /// Returns `TestError` if any connector operation fails.
    pub async fn test_source<S: SourceConnector>(
        &self,
        mut source: S,
        config: &ConnectorConfig,
        max_batches: usize,
        max_records_per_batch: usize,
    ) -> TestResult<SourceTestMetrics> {
        let mut metrics = SourceTestMetrics::default();

        // Open the source
        source.open(config).await?;

        // Poll batches
        for _ in 0..max_batches {
            metrics.poll_calls += 1;

            match source.poll_batch(max_records_per_batch).await? {
                Some(batch) => {
                    metrics.batches_received += 1;
                    metrics.records_received += batch.num_rows() as u64;
                }
                None => break,
            }
        }

        // Test checkpoint
        metrics.checkpoint_calls += 1;
        let checkpoint = source.checkpoint();

        // Test restore
        metrics.restore_calls += 1;
        source.restore(&checkpoint).await?;

        // Close
        source.close().await?;

        Ok(metrics)
    }

    /// Tests a sink connector by writing batches.
    ///
    /// # Arguments
    ///
    /// * `sink` - The sink connector to test
    /// * `config` - Configuration for the sink
    /// * `batches` - Batches to write
    ///
    /// # Errors
    ///
    /// Returns `TestError` if any connector operation fails.
    pub async fn test_sink<K: SinkConnector>(
        &self,
        mut sink: K,
        config: &ConnectorConfig,
        batches: Vec<RecordBatch>,
    ) -> TestResult<SinkTestMetrics> {
        let mut metrics = SinkTestMetrics::default();

        // Open the sink
        sink.open(config).await?;

        // Write batches with epochs
        for (epoch, batch) in batches.iter().enumerate() {
            metrics.epoch_begins += 1;
            sink.begin_epoch(epoch as u64).await?;

            let result = sink.write_batch(batch).await?;
            metrics.batches_written += 1;
            metrics.records_written += result.records_written as u64;

            metrics.epoch_commits += 1;
            sink.commit_epoch(epoch as u64).await?;
        }

        // Flush and close
        sink.flush().await?;
        sink.close().await?;

        Ok(metrics)
    }

    /// Tests checkpoint and recovery behavior for a source.
    ///
    /// # Errors
    ///
    /// Returns `TestError` if any connector operation fails.
    pub async fn test_checkpoint_recovery<S: SourceConnector + Clone>(
        &self,
        mut source: S,
        config: &ConnectorConfig,
        batches_before_checkpoint: usize,
    ) -> TestResult<()> {
        source.open(config).await?;

        // Poll some batches
        for _ in 0..batches_before_checkpoint {
            let _ = source.poll_batch(100).await?;
        }

        // Take checkpoint
        let checkpoint = source.checkpoint();

        // Close and reopen
        source.close().await?;

        let mut new_source = source.clone();
        new_source.open(config).await?;
        new_source.restore(&checkpoint).await?;

        // Verify we can continue
        let _ = new_source.poll_batch(100).await?;

        new_source.close().await?;

        Ok(())
    }

    /// Tests exactly-once semantics for a sink.
    ///
    /// # Errors
    ///
    /// Returns `TestError` if the sink doesn't support exactly-once or any
    /// operation fails.
    pub async fn test_exactly_once<K: SinkConnector>(
        &self,
        mut sink: K,
        config: &ConnectorConfig,
    ) -> TestResult<()> {
        sink.open(config).await?;

        let caps = sink.capabilities();
        if !caps.exactly_once {
            return Err(TestError::Assertion(
                "sink does not support exactly-once".to_string(),
            ));
        }

        // Test successful epoch
        sink.begin_epoch(0).await?;
        sink.write_batch(&mock_batch(10)).await?;
        sink.commit_epoch(0).await?;

        // Test rollback
        sink.begin_epoch(1).await?;
        sink.write_batch(&mock_batch(10)).await?;
        sink.rollback_epoch(1).await?;

        sink.close().await?;

        Ok(())
    }
}

/// Configurable test source connector.
///
/// Allows configuring batches, errors, and delays for testing.
#[derive(Debug)]
pub struct TestSourceConnector {
    schema: SchemaRef,
    batches: Arc<Mutex<VecDeque<RecordBatch>>>,
    error_schedule: Arc<Mutex<VecDeque<ConnectorError>>>,
    delay_schedule: Arc<Mutex<VecDeque<Duration>>>,
    records_produced: AtomicU64,
    is_open: AtomicBool,
    checkpoint_data: Arc<Mutex<SourceCheckpoint>>,
}

impl TestSourceConnector {
    /// Creates a new test source with the given batches.
    #[must_use]
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            schema: mock_schema(),
            batches: Arc::new(Mutex::new(batches.into())),
            error_schedule: Arc::new(Mutex::new(VecDeque::new())),
            delay_schedule: Arc::new(Mutex::new(VecDeque::new())),
            records_produced: AtomicU64::new(0),
            is_open: AtomicBool::new(false),
            checkpoint_data: Arc::new(Mutex::new(SourceCheckpoint::new(0))),
        }
    }

    /// Creates a test source that produces `n` batches of `size` records each.
    #[must_use]
    pub fn with_generated_batches(n: usize, size: usize) -> Self {
        let batches: Vec<RecordBatch> = (0..n).map(|_| mock_batch(size)).collect();
        Self::new(batches)
    }

    /// Schedules an error to be returned on the next poll.
    pub fn schedule_error(&self, error: ConnectorError) {
        self.error_schedule.lock().push_back(error);
    }

    /// Schedules a delay before the next poll completes.
    pub fn schedule_delay(&self, delay: Duration) {
        self.delay_schedule.lock().push_back(delay);
    }

    /// Returns the number of records produced.
    #[must_use]
    pub fn records_produced(&self) -> u64 {
        self.records_produced.load(Ordering::Relaxed)
    }
}

impl Clone for TestSourceConnector {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            batches: self.batches.clone(),
            error_schedule: self.error_schedule.clone(),
            delay_schedule: self.delay_schedule.clone(),
            records_produced: AtomicU64::new(self.records_produced.load(Ordering::Relaxed)),
            is_open: AtomicBool::new(false),
            checkpoint_data: self.checkpoint_data.clone(),
        }
    }
}

#[async_trait]
impl SourceConnector for TestSourceConnector {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.is_open.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        // Check for scheduled delay - drop guard before await
        let delay = self.delay_schedule.lock().pop_front();
        if let Some(d) = delay {
            tokio::time::sleep(d).await;
        }

        // Check for scheduled error
        let error = self.error_schedule.lock().pop_front();
        if let Some(e) = error {
            return Err(e);
        }

        // Return next batch
        let batch = self.batches.lock().pop_front();
        if let Some(ref b) = batch {
            self.records_produced
                .fetch_add(b.num_rows() as u64, Ordering::Relaxed);
        }

        Ok(batch.map(SourceBatch::new))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(0);
        cp.set_offset(
            "records",
            self.records_produced.load(Ordering::Relaxed).to_string(),
        );
        *self.checkpoint_data.lock() = cp.clone();
        cp
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        if let Some(offset) = checkpoint.get_offset("records") {
            if let Ok(records) = offset.parse::<u64>() {
                self.records_produced.store(records, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        if self.is_open.load(Ordering::Relaxed) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.records_produced.load(Ordering::Relaxed),
            ..Default::default()
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.is_open.store(false, Ordering::Relaxed);
        Ok(())
    }
}

/// Configurable test sink connector.
///
/// Tracks all written batches and supports error injection.
#[derive(Debug)]
pub struct TestSinkConnector {
    schema: SchemaRef,
    written: Arc<Mutex<Vec<RecordBatch>>>,
    committed_epochs: Arc<Mutex<Vec<u64>>>,
    error_schedule: Arc<Mutex<VecDeque<ConnectorError>>>,
    records_written: AtomicU64,
    current_epoch: AtomicU64,
    is_open: AtomicBool,
}

impl TestSinkConnector {
    /// Creates a new test sink.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema: mock_schema(),
            written: Arc::new(Mutex::new(Vec::new())),
            committed_epochs: Arc::new(Mutex::new(Vec::new())),
            error_schedule: Arc::new(Mutex::new(VecDeque::new())),
            records_written: AtomicU64::new(0),
            current_epoch: AtomicU64::new(0),
            is_open: AtomicBool::new(false),
        }
    }

    /// Schedules an error to be returned on the next write.
    pub fn schedule_error(&self, error: ConnectorError) {
        self.error_schedule.lock().push_back(error);
    }

    /// Returns all written batches.
    #[must_use]
    pub fn written_batches(&self) -> Vec<RecordBatch> {
        self.written.lock().clone()
    }

    /// Returns all committed epochs.
    #[must_use]
    pub fn committed_epochs(&self) -> Vec<u64> {
        self.committed_epochs.lock().clone()
    }

    /// Returns the number of records written.
    #[must_use]
    pub fn records_written(&self) -> u64 {
        self.records_written.load(Ordering::Relaxed)
    }
}

impl Default for TestSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TestSinkConnector {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            written: self.written.clone(),
            committed_epochs: self.committed_epochs.clone(),
            error_schedule: self.error_schedule.clone(),
            records_written: AtomicU64::new(self.records_written.load(Ordering::Relaxed)),
            current_epoch: AtomicU64::new(self.current_epoch.load(Ordering::Relaxed)),
            is_open: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl SinkConnector for TestSinkConnector {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.is_open.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        // Check for scheduled error
        if let Some(error) = self.error_schedule.lock().pop_front() {
            return Err(error);
        }

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

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.current_epoch.store(epoch, Ordering::Relaxed);
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.committed_epochs.lock().push(epoch);
        Ok(())
    }

    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        // In a real implementation, would discard buffered data
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        if self.is_open.load(Ordering::Relaxed) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.records_written.load(Ordering::Relaxed),
            ..Default::default()
        }
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        SinkConnectorCapabilities::default()
            .with_exactly_once()
            .with_idempotent()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.is_open.store(false, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_harness_test_source() {
        let harness = ConnectorTestHarness::new();
        let source = TestSourceConnector::with_generated_batches(5, 10);
        let config = ConnectorConfig::new("test");

        let metrics = harness.test_source(source, &config, 10, 100).await.unwrap();

        assert_eq!(metrics.batches_received, 5);
        assert_eq!(metrics.records_received, 50);
        assert_eq!(metrics.checkpoint_calls, 1);
        assert_eq!(metrics.restore_calls, 1);
    }

    #[tokio::test]
    async fn test_harness_test_sink() {
        let harness = ConnectorTestHarness::new();
        let sink = TestSinkConnector::new();
        let config = ConnectorConfig::new("test");

        let batches = vec![mock_batch(10), mock_batch(20), mock_batch(30)];
        let metrics = harness.test_sink(sink, &config, batches).await.unwrap();

        assert_eq!(metrics.batches_written, 3);
        assert_eq!(metrics.records_written, 60);
        assert_eq!(metrics.epoch_begins, 3);
        assert_eq!(metrics.epoch_commits, 3);
    }

    #[tokio::test]
    async fn test_source_connector_error_injection() {
        let source = TestSourceConnector::with_generated_batches(5, 10);
        source.schedule_error(ConnectorError::ReadError("injected".into()));

        let mut source = source;
        source.open(&ConnectorConfig::new("test")).await.unwrap();

        let result = source.poll_batch(100).await;
        assert!(result.is_err());

        // Next poll should succeed
        let result = source.poll_batch(100).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_source_connector_delay() {
        let source = TestSourceConnector::with_generated_batches(1, 10);
        source.schedule_delay(Duration::from_millis(50));

        let mut source = source;
        source.open(&ConnectorConfig::new("test")).await.unwrap();

        let start = std::time::Instant::now();
        let _ = source.poll_batch(100).await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_sink_connector_error_injection() {
        let sink = TestSinkConnector::new();
        sink.schedule_error(ConnectorError::WriteError("injected".into()));

        let mut sink = sink;
        sink.open(&ConnectorConfig::new("test")).await.unwrap();

        let result = sink.write_batch(&mock_batch(10)).await;
        assert!(result.is_err());

        // Next write should succeed
        let result = sink.write_batch(&mock_batch(10)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sink_committed_epochs() {
        let mut sink = TestSinkConnector::new();
        sink.open(&ConnectorConfig::new("test")).await.unwrap();

        sink.begin_epoch(0).await.unwrap();
        sink.write_batch(&mock_batch(10)).await.unwrap();
        sink.commit_epoch(0).await.unwrap();

        sink.begin_epoch(1).await.unwrap();
        sink.write_batch(&mock_batch(10)).await.unwrap();
        sink.commit_epoch(1).await.unwrap();

        assert_eq!(sink.committed_epochs(), vec![0, 1]);
    }

    #[tokio::test]
    async fn test_exactly_once_validation() {
        let harness = ConnectorTestHarness::new();
        let sink = TestSinkConnector::new();
        let config = ConnectorConfig::new("test");

        let result = harness.test_exactly_once(sink, &config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_source_checkpoint_restore() {
        let mut source = TestSourceConnector::with_generated_batches(5, 10);
        source.open(&ConnectorConfig::new("test")).await.unwrap();

        // Poll some batches
        source.poll_batch(100).await.unwrap();
        source.poll_batch(100).await.unwrap();
        assert_eq!(source.records_produced(), 20);

        // Take checkpoint
        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("records"), Some("20"));

        // Create new source and restore
        let mut new_source = TestSourceConnector::with_generated_batches(5, 10);
        new_source
            .open(&ConnectorConfig::new("test"))
            .await
            .unwrap();
        new_source.restore(&cp).await.unwrap();

        assert_eq!(new_source.records_produced(), 20);
    }

    #[test]
    fn test_source_clone_shares_batches() {
        let source = TestSourceConnector::with_generated_batches(5, 10);
        let _clone = source.clone();

        // Both should have access to same batch queue
        assert_eq!(source.batches.lock().len(), 5);
    }

    #[test]
    fn test_sink_clone_shares_written() {
        let sink = TestSinkConnector::new();
        let clone = sink.clone();

        // Both should see same written batches
        sink.written.lock().push(mock_batch(10));
        assert_eq!(clone.written.lock().len(), 1);
    }
}
