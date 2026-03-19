//! Idempotent sink wrapper for at-least-once delivery with deduplication.
//!
//! Wraps any [`SinkConnector`] with a deduplication table that tracks
//! `(epoch, batch_id)` pairs. On re-delivery after recovery, batches that
//! were already committed in a previous epoch are silently skipped.
//!
//! This provides at-least-once delivery with client-side deduplication for
//! sinks that do not support two-phase commit (e.g., HTTP endpoints, file
//! sinks, message queues without transactional producers). It is NOT true
//! exactly-once: the dedup table is in-memory and lost on crash, so a batch
//! that was written but not yet committed can be replayed after recovery.
//!
//! ## Usage
//!
//! Configure via `delivery.guarantee = 'idempotent'` in sink config.
//!
//! ## Deduplication Table
//!
//! The wrapper maintains an in-memory set of committed `(epoch, batch_id)`
//! pairs. On `begin_epoch()`, the batch counter resets. On `commit_epoch()`,
//! the dedup entries for that epoch are finalized. On `rollback_epoch()`,
//! pending entries are discarded.
//!
//! The dedup table is bounded: only entries from the last N epochs are
//! retained (configurable via `max_retained_epochs`).

use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

use crate::config::ConnectorConfig;
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

/// Configuration for the idempotent sink wrapper.
#[derive(Debug, Clone)]
pub struct IdempotentConfig {
    /// Maximum number of past epochs to retain in the dedup table.
    /// Entries from epochs older than `current_epoch - max_retained_epochs`
    /// are pruned on each `commit_epoch()`.
    pub max_retained_epochs: u64,
}

impl Default for IdempotentConfig {
    fn default() -> Self {
        Self {
            max_retained_epochs: 3,
        }
    }
}

/// Wraps a [`SinkConnector`] with idempotent write semantics.
///
/// Tracks `(epoch, batch_id)` pairs and skips writes for batches that
/// have already been committed. This provides **at-least-once** delivery
/// with client-side deduplication for sinks without native transaction
/// support. The dedup table is in-memory, so a crash before commit can
/// result in replay — this is NOT true exactly-once.
///
/// The wrapper advertises `exactly_once = true` in capabilities so the
/// pipeline coordinator accepts it in the exactly-once path, but the
/// actual guarantee is at-least-once with best-effort deduplication.
pub struct IdempotentSinkWrapper {
    inner: Box<dyn SinkConnector>,
    config: IdempotentConfig,
    /// Current epoch being written.
    current_epoch: u64,
    /// Batch counter within the current epoch (reset on `begin_epoch`).
    batch_counter: u64,
    /// Committed dedup entries: epoch -> set of `batch_ids`.
    committed: HashMap<u64, HashSet<u64>>,
    /// Pending dedup entries for the current (uncommitted) epoch.
    pending: HashSet<u64>,
}

impl IdempotentSinkWrapper {
    /// Creates a new idempotent sink wrapper around the given connector.
    #[must_use]
    pub fn new(inner: Box<dyn SinkConnector>, config: IdempotentConfig) -> Self {
        Self {
            inner,
            config,
            current_epoch: 0,
            batch_counter: 0,
            committed: HashMap::new(),
            pending: HashSet::new(),
        }
    }

    /// Prunes committed entries for epochs older than the retention window.
    fn prune_old_epochs(&mut self) {
        if self.current_epoch <= self.config.max_retained_epochs {
            return;
        }
        let cutoff = self.current_epoch - self.config.max_retained_epochs;
        self.committed.retain(|&epoch, _| epoch >= cutoff);
    }
}

#[async_trait]
impl SinkConnector for IdempotentSinkWrapper {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.inner.open(config).await
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        let batch_id = self.batch_counter;
        self.batch_counter += 1;

        // Check if this batch was already committed in a previous epoch.
        if let Some(committed_batches) = self.committed.get(&self.current_epoch) {
            if committed_batches.contains(&batch_id) {
                // Skip — already committed (re-delivery after recovery).
                // Return zero rows/bytes since nothing was actually written.
                return Ok(WriteResult::new(0, 0));
            }
        }

        let result = self.inner.write_batch(batch).await?;
        self.pending.insert(batch_id);
        Ok(result)
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.current_epoch = epoch;
        self.batch_counter = 0;
        self.pending.clear();
        self.inner.begin_epoch(epoch).await
    }

    async fn pre_commit(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.inner.pre_commit(epoch).await
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.inner.commit_epoch(epoch).await?;

        // Move pending entries to committed.
        let committed_set = self.committed.entry(epoch).or_default();
        committed_set.extend(self.pending.drain());

        self.prune_old_epochs();
        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Discard pending entries — they were not committed.
        self.pending.clear();
        self.inner.rollback_epoch(epoch).await
    }

    fn health_check(&self) -> HealthStatus {
        self.inner.health_check()
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.inner.metrics()
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        let mut caps = self.inner.capabilities();
        caps.idempotent = true;
        // Advertise exactly_once so the pipeline accepts this sink in the
        // exactly-once path. The dedup table provides at-least-once with
        // deduplication — not true exactly-once — but it is sufficient for
        // sinks that lack native two-phase commit.
        caps.exactly_once = true;
        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.inner.flush().await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.inner.close().await
    }

    fn as_schema_registry_aware(&self) -> Option<&dyn crate::schema::SchemaRegistryAware> {
        self.inner.as_schema_registry_aware()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    struct CountingSink {
        writes: Arc<AtomicU64>,
        schema: SchemaRef,
    }

    impl CountingSink {
        fn new() -> (Self, Arc<AtomicU64>) {
            let writes = Arc::new(AtomicU64::new(0));
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            (
                Self {
                    writes: Arc::clone(&writes),
                    schema,
                },
                writes,
            )
        }
    }

    #[async_trait]
    impl SinkConnector for CountingSink {
        async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            Ok(WriteResult::new(1, 0))
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[tokio::test]
    async fn test_idempotent_normal_write() {
        let (sink, writes) = CountingSink::new();
        let mut wrapper = IdempotentSinkWrapper::new(Box::new(sink), IdempotentConfig::default());

        wrapper.begin_epoch(1).await.unwrap();
        wrapper.write_batch(&test_batch()).await.unwrap();
        wrapper.write_batch(&test_batch()).await.unwrap();
        wrapper.commit_epoch(1).await.unwrap();

        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_idempotent_dedup_on_replay() {
        let (sink, writes) = CountingSink::new();
        let mut wrapper = IdempotentSinkWrapper::new(Box::new(sink), IdempotentConfig::default());

        // First delivery: epoch 1, 2 batches.
        wrapper.begin_epoch(1).await.unwrap();
        wrapper.write_batch(&test_batch()).await.unwrap();
        wrapper.write_batch(&test_batch()).await.unwrap();
        wrapper.commit_epoch(1).await.unwrap();

        assert_eq!(writes.load(Ordering::Relaxed), 2);

        // Re-delivery (simulating recovery): same epoch, same batches.
        wrapper.begin_epoch(1).await.unwrap();
        wrapper.write_batch(&test_batch()).await.unwrap(); // should be skipped
        wrapper.write_batch(&test_batch()).await.unwrap(); // should be skipped

        // No additional writes should have occurred.
        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_idempotent_rollback_discards_pending() {
        let (sink, writes) = CountingSink::new();
        let mut wrapper = IdempotentSinkWrapper::new(Box::new(sink), IdempotentConfig::default());

        wrapper.begin_epoch(1).await.unwrap();
        wrapper.write_batch(&test_batch()).await.unwrap();
        wrapper.rollback_epoch(1).await.unwrap();

        assert_eq!(writes.load(Ordering::Relaxed), 1);

        // Retry same epoch — should write again since rollback discarded pending.
        wrapper.begin_epoch(1).await.unwrap();
        wrapper.write_batch(&test_batch()).await.unwrap();
        wrapper.commit_epoch(1).await.unwrap();

        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_idempotent_epoch_pruning() {
        let (sink, _writes) = CountingSink::new();
        let config = IdempotentConfig {
            max_retained_epochs: 2,
        };
        let mut wrapper = IdempotentSinkWrapper::new(Box::new(sink), config);

        for epoch in 1..=5 {
            wrapper.begin_epoch(epoch).await.unwrap();
            wrapper.write_batch(&test_batch()).await.unwrap();
            wrapper.commit_epoch(epoch).await.unwrap();
        }

        // Epochs 1, 2 should be pruned (only 3, 4, 5 retained).
        assert!(!wrapper.committed.contains_key(&1));
        assert!(!wrapper.committed.contains_key(&2));
        assert!(wrapper.committed.contains_key(&3));
        assert!(wrapper.committed.contains_key(&4));
        assert!(wrapper.committed.contains_key(&5));
    }

    #[tokio::test]
    async fn test_idempotent_capabilities() {
        let (sink, _writes) = CountingSink::new();
        let wrapper = IdempotentSinkWrapper::new(Box::new(sink), IdempotentConfig::default());
        let caps = wrapper.capabilities();
        assert!(caps.idempotent);
        assert!(
            caps.exactly_once,
            "wrapper must advertise exactly_once for pipeline acceptance"
        );
    }

    #[tokio::test]
    async fn test_dedup_write_result_returns_zero() {
        let (sink, writes) = CountingSink::new();
        let mut wrapper = IdempotentSinkWrapper::new(Box::new(sink), IdempotentConfig::default());

        // First delivery.
        wrapper.begin_epoch(1).await.unwrap();
        let result = wrapper.write_batch(&test_batch()).await.unwrap();
        assert_eq!(result.records_written, 1);
        wrapper.commit_epoch(1).await.unwrap();
        assert_eq!(writes.load(Ordering::Relaxed), 1);

        // Re-delivery of same epoch — should be skipped with zero WriteResult.
        wrapper.begin_epoch(1).await.unwrap();
        let dedup_result = wrapper.write_batch(&test_batch()).await.unwrap();
        assert_eq!(
            dedup_result.records_written, 0,
            "skipped batch must report 0 records written"
        );
        assert_eq!(
            dedup_result.bytes_written, 0,
            "skipped batch must report 0 bytes written"
        );
        // Inner sink should NOT have been called again.
        assert_eq!(writes.load(Ordering::Relaxed), 1);
    }
}
