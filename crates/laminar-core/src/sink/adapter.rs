//! Epoch-based adapter bridging `reactor::Sink` to `ExactlyOnceSink`
//!
//! `ExactlyOnceSinkAdapter<E>` manages epoch-based transactions over any
//! `ExactlyOnceSink` implementation, allowing it to be used as a `reactor::Sink`.
//! Each epoch maps to one transaction: writes accumulate within an epoch, and
//! `notify_checkpoint` commits the current epoch's transaction.

use super::checkpoint::SinkCheckpoint;
use super::error::SinkError;
use super::traits::{ExactlyOnceSink, TransactionId};
use crate::operator::Output;
use crate::reactor::Sink;

/// Configuration for the adapter
#[derive(Debug, Clone)]
pub struct AdapterConfig {
    /// Maximum outputs per epoch before auto-flush (0 = unlimited)
    pub max_outputs_per_epoch: usize,
    /// Whether to auto-begin a transaction on the first write (default: true)
    pub auto_begin: bool,
}

impl Default for AdapterConfig {
    fn default() -> Self {
        Self {
            max_outputs_per_epoch: 0,
            auto_begin: true,
        }
    }
}

/// Statistics for the adapter
#[derive(Debug, Clone, Copy, Default)]
pub struct AdapterStats {
    /// Current epoch number
    pub epoch: u64,
    /// Number of outputs written in the current epoch
    pub epoch_output_count: u64,
    /// Total number of epochs committed
    pub committed_epochs: u64,
    /// Total outputs written across all epochs
    pub total_outputs: u64,
}

/// Bridges `reactor::Sink` to any `ExactlyOnceSink` with epoch-based transactions.
///
/// Each checkpoint epoch corresponds to exactly one transaction on the inner sink.
/// The adapter auto-begins transactions on the first write (configurable) and
/// commits them when `notify_checkpoint` is called.
///
/// # Usage
///
/// ```ignore
/// let inner = TransactionalSink::new(my_sink, "sink-1");
/// let mut adapter = ExactlyOnceSinkAdapter::new(inner);
///
/// // Write outputs (auto-begins transaction)
/// adapter.write(outputs)?;
///
/// // On checkpoint, commit the epoch
/// let checkpoint = adapter.notify_checkpoint(1)?;
/// ```
pub struct ExactlyOnceSinkAdapter<E: ExactlyOnceSink> {
    /// The inner exactly-once sink
    inner: E,
    /// Current transaction ID (if active)
    current_tx: Option<TransactionId>,
    /// Current epoch
    epoch: u64,
    /// Configuration
    config: AdapterConfig,
    /// Statistics
    stats: AdapterStats,
}

impl<E: ExactlyOnceSink> ExactlyOnceSinkAdapter<E> {
    /// Create a new adapter wrapping the given exactly-once sink
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            current_tx: None,
            epoch: 0,
            config: AdapterConfig::default(),
            stats: AdapterStats::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(inner: E, config: AdapterConfig) -> Self {
        Self {
            inner,
            current_tx: None,
            epoch: 0,
            config,
            stats: AdapterStats::default(),
        }
    }

    /// Get a reference to the inner sink
    #[must_use]
    pub fn inner(&self) -> &E {
        &self.inner
    }

    /// Get a mutable reference to the inner sink
    pub fn inner_mut(&mut self) -> &mut E {
        &mut self.inner
    }

    /// Get current statistics
    #[must_use]
    pub fn stats(&self) -> AdapterStats {
        self.stats
    }

    /// Get the current epoch
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Check if there is an active transaction
    #[must_use]
    pub fn has_active_transaction(&self) -> bool {
        self.current_tx.is_some()
    }

    /// Ensure a transaction is active, auto-beginning one if configured.
    fn ensure_transaction(&mut self) -> Result<TransactionId, SinkError> {
        if let Some(ref tx_id) = self.current_tx {
            return Ok(tx_id.clone());
        }

        if !self.config.auto_begin {
            return Err(SinkError::NoActiveTransaction);
        }

        let tx_id = self.inner.begin_transaction()?;
        self.current_tx = Some(tx_id.clone());
        Ok(tx_id)
    }

    /// Notify that a checkpoint has occurred at the given epoch.
    ///
    /// This commits the current transaction (if any), creates a checkpoint,
    /// and advances the epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails.
    pub fn notify_checkpoint(&mut self, epoch: u64) -> Result<SinkCheckpoint, SinkError> {
        // Commit current transaction if active
        if let Some(tx_id) = self.current_tx.take() {
            self.inner.commit(&tx_id)?;
            self.stats.committed_epochs += 1;
        }

        self.epoch = epoch;
        self.stats.epoch = epoch;
        self.stats.epoch_output_count = 0;

        // Get checkpoint from inner sink
        let mut checkpoint = self.inner.checkpoint();
        checkpoint.set_epoch(epoch);
        checkpoint.set_metadata("adapter_epoch", epoch.to_le_bytes().to_vec());
        checkpoint.set_metadata(
            "committed_epochs",
            self.stats.committed_epochs.to_le_bytes().to_vec(),
        );
        checkpoint.set_metadata(
            "total_outputs",
            self.stats.total_outputs.to_le_bytes().to_vec(),
        );

        Ok(checkpoint)
    }

    /// Restore the adapter from a checkpoint.
    ///
    /// Rolls back any pending transaction, restores the inner sink,
    /// and resets the epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if the inner sink cannot be restored.
    ///
    /// # Panics
    ///
    /// Will not panic — all array conversions are bounds-checked before unwrapping.
    #[allow(clippy::missing_panics_doc)]
    pub fn restore(&mut self, checkpoint: &SinkCheckpoint) -> Result<(), SinkError> {
        // Rollback pending transaction if any
        if let Some(tx_id) = self.current_tx.take() {
            // Best-effort rollback; ignore errors since we're recovering
            let _ = self.inner.rollback(&tx_id);
        }

        // Restore inner sink
        self.inner.restore(checkpoint)?;

        // Restore adapter state
        self.epoch = checkpoint.epoch();
        self.stats.epoch = checkpoint.epoch();
        self.stats.epoch_output_count = 0;

        if let Some(bytes) = checkpoint.get_metadata("committed_epochs") {
            if bytes.len() >= 8 {
                self.stats.committed_epochs = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            }
        }
        if let Some(bytes) = checkpoint.get_metadata("total_outputs") {
            if bytes.len() >= 8 {
                self.stats.total_outputs = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            }
        }

        Ok(())
    }

    /// Get the current checkpoint state without committing.
    #[must_use]
    pub fn checkpoint(&self) -> SinkCheckpoint {
        let mut cp = self.inner.checkpoint();
        cp.set_epoch(self.epoch);
        cp.set_metadata("adapter_epoch", self.epoch.to_le_bytes().to_vec());
        cp.set_metadata(
            "committed_epochs",
            self.stats.committed_epochs.to_le_bytes().to_vec(),
        );
        cp.set_metadata(
            "total_outputs",
            self.stats.total_outputs.to_le_bytes().to_vec(),
        );
        cp
    }
}

impl<E: ExactlyOnceSink> Sink for ExactlyOnceSinkAdapter<E> {
    fn write(&mut self, outputs: Vec<Output>) -> Result<(), crate::reactor::SinkError> {
        let tx_id = self
            .ensure_transaction()
            .map_err(|e| crate::reactor::SinkError::WriteFailed(e.to_string()))?;

        let count = outputs.len() as u64;

        self.inner
            .write(&tx_id, outputs)
            .map_err(crate::reactor::SinkError::from)?;

        self.stats.epoch_output_count += count;
        self.stats.total_outputs += count;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), crate::reactor::SinkError> {
        // Commit the current transaction on flush
        if let Some(tx_id) = self.current_tx.take() {
            self.inner
                .commit(&tx_id)
                .map_err(|e| crate::reactor::SinkError::FlushFailed(e.to_string()))?;
            self.stats.committed_epochs += 1;
            self.stats.epoch_output_count = 0;
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::cast_sign_loss)]
mod tests {
    use super::*;
    use crate::operator::Event;
    use crate::reactor::{BufferingSink, Sink as ReactorSink};
    use crate::sink::idempotent::IdempotentSink;
    use crate::sink::transactional::TransactionalSink;
    use crate::sink::InMemoryDedup;
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;

    fn make_event(timestamp: i64, value: i64) -> Event {
        let array = Arc::new(Int64Array::from(vec![value]));
        let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
        Event::new(timestamp, batch)
    }

    #[test]
    fn test_new_adapter() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        assert_eq!(adapter.epoch(), 0);
        assert!(!adapter.has_active_transaction());
        assert_eq!(adapter.stats().committed_epochs, 0);
    }

    #[test]
    fn test_auto_begin_on_write() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        let event = make_event(1000, 42);
        ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();

        assert!(adapter.has_active_transaction());
        assert_eq!(adapter.stats().epoch_output_count, 1);
        assert_eq!(adapter.stats().total_outputs, 1);
    }

    #[test]
    fn test_checkpoint_commits_transaction() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        // Write some data
        let event = make_event(1000, 42);
        ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();
        assert!(adapter.has_active_transaction());

        // Checkpoint should commit
        let checkpoint = adapter.notify_checkpoint(1).unwrap();
        assert!(!adapter.has_active_transaction());
        assert_eq!(adapter.epoch(), 1);
        assert_eq!(checkpoint.epoch(), 1);
        assert_eq!(adapter.stats().committed_epochs, 1);
    }

    #[test]
    fn test_epoch_advances() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        for epoch in 1..=3 {
            let event = make_event(epoch * 1000, epoch);
            ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();
            adapter.notify_checkpoint(epoch as u64).unwrap();
        }

        assert_eq!(adapter.epoch(), 3);
        assert_eq!(adapter.stats().committed_epochs, 3);
        assert_eq!(adapter.stats().total_outputs, 3);
    }

    #[test]
    fn test_restore_rollback() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        // Commit epoch 1
        let event = make_event(1000, 1);
        ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();
        let checkpoint = adapter.notify_checkpoint(1).unwrap();

        // Start epoch 2 but don't commit (simulates crash)
        let event2 = make_event(2000, 2);
        ReactorSink::write(&mut adapter, vec![Output::Event(event2)]).unwrap();
        assert!(adapter.has_active_transaction());

        // Restore from epoch 1 checkpoint
        adapter.restore(&checkpoint).unwrap();
        assert!(!adapter.has_active_transaction());
        assert_eq!(adapter.epoch(), 1);
        assert_eq!(adapter.stats().committed_epochs, 1);
    }

    #[test]
    fn test_flush_commits() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        let event = make_event(1000, 42);
        ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();

        ReactorSink::flush(&mut adapter).unwrap();
        assert!(!adapter.has_active_transaction());
        assert_eq!(adapter.stats().committed_epochs, 1);
    }

    #[test]
    fn test_stats_tracking() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        // Write 3 outputs, checkpoint
        for i in 0..3 {
            let event = make_event(i * 1000, i);
            ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();
        }

        let stats = adapter.stats();
        assert_eq!(stats.epoch_output_count, 3);
        assert_eq!(stats.total_outputs, 3);
        assert_eq!(stats.committed_epochs, 0);

        adapter.notify_checkpoint(1).unwrap();
        let stats = adapter.stats();
        assert_eq!(stats.epoch_output_count, 0); // reset after checkpoint
        assert_eq!(stats.total_outputs, 3);
        assert_eq!(stats.committed_epochs, 1);
    }

    #[test]
    fn test_compose_with_transactional_sink() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "tx-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        let event = make_event(1000, 42);
        ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();
        let checkpoint = adapter.notify_checkpoint(1).unwrap();

        assert_eq!(checkpoint.epoch(), 1);
        assert_eq!(adapter.inner().stats().committed, 1);
    }

    #[test]
    fn test_compose_with_idempotent_sink() {
        let inner_sink = BufferingSink::new();
        let dedup = InMemoryDedup::new(1000);
        let idem_sink = IdempotentSink::new(inner_sink, dedup).with_sink_id("idem-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(idem_sink);

        let event = make_event(1000, 42);
        ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();
        let checkpoint = adapter.notify_checkpoint(1).unwrap();

        assert_eq!(checkpoint.epoch(), 1);
        assert_eq!(adapter.stats().committed_epochs, 1);
    }

    #[test]
    fn test_multiple_writes_per_epoch() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        // Multiple writes within one epoch
        for i in 0..5 {
            let event = make_event(i * 100, i);
            ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();
        }

        assert_eq!(adapter.stats().epoch_output_count, 5);

        adapter.notify_checkpoint(1).unwrap();
        assert_eq!(adapter.stats().total_outputs, 5);
        assert_eq!(adapter.stats().committed_epochs, 1);
    }

    #[test]
    fn test_empty_epoch_checkpoint() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        // Checkpoint with no writes (no transaction to commit)
        let checkpoint = adapter.notify_checkpoint(1).unwrap();
        assert_eq!(checkpoint.epoch(), 1);
        assert_eq!(adapter.stats().committed_epochs, 0); // nothing was committed
    }

    #[test]
    fn test_auto_begin_disabled() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let config = AdapterConfig {
            auto_begin: false,
            ..Default::default()
        };
        let mut adapter = ExactlyOnceSinkAdapter::with_config(tx_sink, config);

        let event = make_event(1000, 42);
        let result = ReactorSink::write(&mut adapter, vec![Output::Event(event)]);
        assert!(result.is_err()); // Should fail because auto-begin is off
    }

    #[test]
    fn test_checkpoint_state_without_commit() {
        let inner_sink = BufferingSink::new();
        let tx_sink = TransactionalSink::new(inner_sink, "test-sink");
        let mut adapter = ExactlyOnceSinkAdapter::new(tx_sink);

        // Write without committing
        let event = make_event(1000, 42);
        ReactorSink::write(&mut adapter, vec![Output::Event(event)]).unwrap();

        // checkpoint() should return state without committing
        let cp = adapter.checkpoint();
        assert!(adapter.has_active_transaction()); // still active
        assert_eq!(cp.epoch(), 0); // epoch hasn't advanced
    }
}
