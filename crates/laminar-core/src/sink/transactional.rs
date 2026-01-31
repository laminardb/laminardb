//! Transactional sink wrapper for exactly-once delivery
//!
//! `TransactionalSink<S>` wraps any `reactor::Sink` with transaction buffering.
//! Writes are buffered in memory and only flushed to the inner sink on commit.
//! On rollback, the buffer is discarded, providing atomicity.

use super::checkpoint::SinkCheckpoint;
use super::error::SinkError;
use super::traits::{ExactlyOnceSink, SinkCapabilities, SinkState, TransactionId};
use super::transaction::{TransactionBuffer, TransactionState};
use crate::operator::Output;
use crate::reactor::Sink;

/// Configuration for a transactional sink
#[derive(Debug, Clone)]
pub struct TransactionalSinkConfig {
    /// Maximum buffer size in bytes before rejecting writes (default: 64 MiB)
    pub max_buffer_bytes: usize,
    /// Maximum number of buffered outputs before rejecting writes (default: 100,000)
    pub max_buffer_count: usize,
}

impl Default for TransactionalSinkConfig {
    fn default() -> Self {
        Self {
            max_buffer_bytes: 64 * 1024 * 1024, // 64 MiB
            max_buffer_count: 100_000,
        }
    }
}

/// Statistics for a transactional sink
#[derive(Debug, Clone, Copy, Default)]
pub struct TransactionalSinkStats {
    /// Total transactions committed
    pub committed: u64,
    /// Total transactions rolled back
    pub rolled_back: u64,
    /// Current buffer output count
    pub buffer_count: usize,
    /// Current buffer size in bytes
    pub buffer_bytes: usize,
    /// Total outputs written across all committed transactions
    pub total_outputs_written: u64,
}

/// A transactional sink that buffers writes and flushes only on commit.
///
/// Wraps any `reactor::Sink` to provide atomic transaction semantics:
/// - `begin_transaction()` clears the buffer and starts a new transaction
/// - `write()` appends outputs to the internal buffer
/// - `commit()` flushes the buffer to the inner sink
/// - `rollback()` discards the buffer
///
/// Also implements `reactor::Sink` for use in the reactor pipeline,
/// where it auto-begins a transaction on the first write and commits on flush.
pub struct TransactionalSink<S: Sink> {
    /// The underlying sink
    inner: S,
    /// Transaction state machine
    tx_state: TransactionState,
    /// Buffered outputs for current transaction
    buffer: TransactionBuffer,
    /// Configuration
    config: TransactionalSinkConfig,
    /// Sink identifier
    sink_id: String,
    /// Committed epoch (tracks last committed checkpoint epoch)
    epoch: u64,
    /// Statistics
    stats: TransactionalSinkStats,
}

impl<S: Sink> TransactionalSink<S> {
    /// Create a new transactional sink wrapping the given inner sink
    pub fn new(inner: S, sink_id: impl Into<String>) -> Self {
        Self {
            inner,
            tx_state: TransactionState::new(),
            buffer: TransactionBuffer::new(),
            config: TransactionalSinkConfig::default(),
            sink_id: sink_id.into(),
            epoch: 0,
            stats: TransactionalSinkStats::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(inner: S, sink_id: impl Into<String>, config: TransactionalSinkConfig) -> Self {
        Self {
            inner,
            tx_state: TransactionState::new(),
            buffer: TransactionBuffer::new(),
            config,
            sink_id: sink_id.into(),
            epoch: 0,
            stats: TransactionalSinkStats::default(),
        }
    }

    /// Get a reference to the inner sink
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner sink
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Get current statistics
    #[must_use]
    pub fn stats(&self) -> TransactionalSinkStats {
        TransactionalSinkStats {
            buffer_count: self.buffer.len(),
            buffer_bytes: self.buffer.size_bytes(),
            ..self.stats
        }
    }

    /// Get the sink identifier
    #[must_use]
    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }

    /// Check buffer limits before writing
    fn check_buffer_limits(&self, additional_count: usize) -> Result<(), SinkError> {
        if self.buffer.len() + additional_count > self.config.max_buffer_count {
            return Err(SinkError::WriteFailed(format!(
                "Buffer count limit exceeded: {} + {} > {}",
                self.buffer.len(),
                additional_count,
                self.config.max_buffer_count
            )));
        }
        // Size check is approximate since we haven't computed new size yet
        if self.buffer.size_bytes() > self.config.max_buffer_bytes {
            return Err(SinkError::WriteFailed(format!(
                "Buffer size limit exceeded: {} > {}",
                self.buffer.size_bytes(),
                self.config.max_buffer_bytes
            )));
        }
        Ok(())
    }

    /// Flush the buffer to the inner sink
    fn flush_buffer(&mut self) -> Result<(), SinkError> {
        let outputs = self.buffer.take();
        if outputs.is_empty() {
            return Ok(());
        }
        let count = outputs.len() as u64;
        self.inner
            .write(outputs)
            .map_err(|e| SinkError::WriteFailed(e.to_string()))?;
        self.inner
            .flush()
            .map_err(|e| SinkError::FlushFailed(e.to_string()))?;
        self.stats.total_outputs_written += count;
        Ok(())
    }
}

impl<S: Sink> ExactlyOnceSink for TransactionalSink<S> {
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::new().with_transactions()
    }

    fn state(&self) -> SinkState {
        self.tx_state.state()
    }

    fn begin_transaction(&mut self) -> Result<TransactionId, SinkError> {
        let tx_id = self.tx_state.begin_new()?;
        self.buffer.clear();
        Ok(tx_id)
    }

    fn write(&mut self, tx_id: &TransactionId, outputs: Vec<Output>) -> Result<(), SinkError> {
        // Validate transaction
        if self.tx_state.current_transaction() != Some(tx_id) {
            return Err(SinkError::TransactionIdMismatch {
                expected: self.tx_state.current_transaction()
                    .map_or_else(|| "none".to_string(), ToString::to_string),
                actual: tx_id.to_string(),
            });
        }
        if !self.tx_state.is_active() {
            return Err(SinkError::NoActiveTransaction);
        }

        self.check_buffer_limits(outputs.len())?;
        let record_count = outputs.len() as u64;
        self.buffer.push(outputs);
        self.tx_state.record_write(record_count)?;
        Ok(())
    }

    fn commit(&mut self, tx_id: &TransactionId) -> Result<(), SinkError> {
        // Validate and transition state
        self.tx_state.commit(tx_id)?;

        // Flush buffered outputs to inner sink
        self.flush_buffer()?;
        self.stats.committed += 1;
        Ok(())
    }

    fn rollback(&mut self, tx_id: &TransactionId) -> Result<(), SinkError> {
        self.tx_state.rollback(tx_id)?;
        self.buffer.clear();
        self.stats.rolled_back += 1;
        Ok(())
    }

    fn checkpoint(&self) -> SinkCheckpoint {
        let mut cp = SinkCheckpoint::new(&self.sink_id);
        cp.set_epoch(self.epoch);

        // Record pending transaction if any
        if let Some(tx_id) = self.tx_state.current_transaction() {
            cp.set_transaction_id(Some(tx_id.clone()));
        }

        // Store stats in metadata
        cp.set_metadata("committed", self.stats.committed.to_le_bytes().to_vec());
        cp.set_metadata(
            "total_outputs_written",
            self.stats.total_outputs_written.to_le_bytes().to_vec(),
        );

        cp
    }

    #[allow(clippy::missing_panics_doc)]
    fn restore(&mut self, checkpoint: &SinkCheckpoint) -> Result<(), SinkError> {
        // Force-rollback any pending transaction
        self.tx_state.force_rollback();
        self.buffer.clear();
        self.epoch = checkpoint.epoch();

        // Restore stats
        if let Some(bytes) = checkpoint.get_metadata("committed") {
            if bytes.len() >= 8 {
                self.stats.committed = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            }
        }
        if let Some(bytes) = checkpoint.get_metadata("total_outputs_written") {
            if bytes.len() >= 8 {
                self.stats.total_outputs_written =
                    u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            }
        }

        self.stats.rolled_back = 0;
        self.stats.buffer_count = 0;
        self.stats.buffer_bytes = 0;

        Ok(())
    }

    fn flush(&mut self) -> Result<(), SinkError> {
        self.inner
            .flush()
            .map_err(|e| SinkError::FlushFailed(e.to_string()))
    }

    fn close(&mut self) -> Result<(), SinkError> {
        // Rollback any pending transaction
        if self.tx_state.is_active() {
            self.tx_state.force_rollback();
            self.buffer.clear();
        }
        ExactlyOnceSink::flush(self)
    }
}

/// Implements `reactor::Sink` for use in the reactor pipeline.
///
/// Auto-begins a transaction on the first write. Flush commits the active transaction.
impl<S: Sink> Sink for TransactionalSink<S> {
    fn write(&mut self, outputs: Vec<Output>) -> Result<(), crate::reactor::SinkError> {
        // Auto-begin transaction if idle
        if self.tx_state.is_idle() {
            self.begin_transaction()
                .map_err(|e| crate::reactor::SinkError::WriteFailed(e.to_string()))?;
        }

        let tx_id = self
            .tx_state
            .current_transaction()
            .cloned()
            .ok_or_else(|| {
                crate::reactor::SinkError::WriteFailed("No active transaction".to_string())
            })?;

        ExactlyOnceSink::write(self, &tx_id, outputs)
            .map_err(crate::reactor::SinkError::from)
    }

    fn flush(&mut self) -> Result<(), crate::reactor::SinkError> {
        // Commit active transaction on flush
        if self.tx_state.is_active() {
            let tx_id = self.tx_state.current_transaction().cloned().ok_or_else(|| {
                crate::reactor::SinkError::FlushFailed("No active transaction".to_string())
            })?;
            self.commit(&tx_id)
                .map_err(|e| crate::reactor::SinkError::FlushFailed(e.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Event;
    use crate::reactor::{BufferingSink, Sink as ReactorSink};
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;

    fn make_event(timestamp: i64, value: i64) -> Event {
        let array = Arc::new(Int64Array::from(vec![value]));
        let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
        Event::new(timestamp, batch)
    }

    #[test]
    fn test_new_and_begin_commit() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        assert_eq!(sink.state(), SinkState::Idle);

        let tx_id = sink.begin_transaction().unwrap();
        assert_eq!(sink.state(), SinkState::InTransaction);

        let event = make_event(1000, 42);
        ExactlyOnceSink::write(&mut sink, &tx_id, vec![Output::Event(event)]).unwrap();

        // Buffer should have the output, inner should be empty
        assert_eq!(sink.buffer.len(), 1);

        sink.commit(&tx_id).unwrap();
        assert_eq!(sink.state(), SinkState::Idle);

        // After commit, inner should have received the output
        assert_eq!(sink.stats().committed, 1);
        assert_eq!(sink.stats().total_outputs_written, 1);
    }

    #[test]
    fn test_rollback_discards_buffer() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        let tx_id = sink.begin_transaction().unwrap();

        let event = make_event(1000, 42);
        ExactlyOnceSink::write(&mut sink, &tx_id, vec![Output::Event(event)]).unwrap();
        assert_eq!(sink.buffer.len(), 1);

        sink.rollback(&tx_id).unwrap();
        assert_eq!(sink.state(), SinkState::Idle);
        assert!(sink.buffer.is_empty());
        assert_eq!(sink.stats().rolled_back, 1);
        assert_eq!(sink.stats().total_outputs_written, 0);
    }

    #[test]
    fn test_buffer_count_limit() {
        let config = TransactionalSinkConfig {
            max_buffer_count: 2,
            max_buffer_bytes: 64 * 1024 * 1024,
        };
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::with_config(inner, "test-sink", config);

        let tx_id = sink.begin_transaction().unwrap();

        // Write 2 outputs (at limit)
        let events: Vec<Output> = (0..2)
            .map(|i| Output::Event(make_event(1000 + i, i)))
            .collect();
        ExactlyOnceSink::write(&mut sink, &tx_id, events).unwrap();

        // Third write should fail
        let event = Output::Event(make_event(1002, 2));
        let result = ExactlyOnceSink::write(&mut sink, &tx_id, vec![event]);
        assert!(result.is_err());
    }

    #[test]
    fn test_double_begin_error() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        let _tx_id = sink.begin_transaction().unwrap();
        let result = sink.begin_transaction();
        assert!(matches!(
            result,
            Err(SinkError::TransactionAlreadyActive(_))
        ));
    }

    #[test]
    fn test_wrong_transaction_id() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        let _tx_id = sink.begin_transaction().unwrap();

        let wrong_id = TransactionId::new(9999);
        let event = Output::Event(make_event(1000, 42));
        let result = ExactlyOnceSink::write(&mut sink, &wrong_id, vec![event]);
        assert!(matches!(
            result,
            Err(SinkError::TransactionIdMismatch { .. })
        ));
    }

    #[test]
    fn test_checkpoint_and_restore() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        // Commit some data
        let tx_id = sink.begin_transaction().unwrap();
        let event = make_event(1000, 42);
        ExactlyOnceSink::write(&mut sink, &tx_id, vec![Output::Event(event)]).unwrap();
        sink.commit(&tx_id).unwrap();

        // Take checkpoint
        let checkpoint = ExactlyOnceSink::checkpoint(&sink);
        assert_eq!(checkpoint.sink_id(), "test-sink");

        // Start another transaction (uncommitted)
        let tx_id2 = sink.begin_transaction().unwrap();
        let event2 = make_event(2000, 99);
        ExactlyOnceSink::write(&mut sink, &tx_id2, vec![Output::Event(event2)]).unwrap();

        // Restore from checkpoint (should rollback uncommitted tx)
        let inner2 = BufferingSink::new();
        let mut sink2 = TransactionalSink::new(inner2, "test-sink");
        sink2.restore(&checkpoint).unwrap();

        assert_eq!(sink2.state(), SinkState::Idle);
        assert!(sink2.buffer.is_empty());
        assert_eq!(sink2.stats().committed, 1);
        assert_eq!(sink2.stats().total_outputs_written, 1);
    }

    #[test]
    fn test_reactor_sink_auto_begin() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        // Using reactor::Sink interface auto-begins a transaction
        let event = make_event(1000, 42);
        ReactorSink::write(&mut sink, vec![Output::Event(event)]).unwrap();

        assert_eq!(sink.state(), SinkState::InTransaction);
        assert_eq!(sink.buffer.len(), 1);

        // Flush commits the transaction
        ReactorSink::flush(&mut sink).unwrap();
        assert_eq!(sink.state(), SinkState::Idle);
        assert_eq!(sink.stats().committed, 1);
    }

    #[test]
    fn test_stats_tracking() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        // Commit one transaction
        let tx_id = sink.begin_transaction().unwrap();
        ExactlyOnceSink::write(
            &mut sink,
            &tx_id,
            vec![
                Output::Event(make_event(1000, 1)),
                Output::Event(make_event(2000, 2)),
            ],
        )
        .unwrap();
        sink.commit(&tx_id).unwrap();

        // Rollback another
        let tx_id2 = sink.begin_transaction().unwrap();
        ExactlyOnceSink::write(
            &mut sink,
            &tx_id2,
            vec![Output::Event(make_event(3000, 3))],
        )
        .unwrap();
        sink.rollback(&tx_id2).unwrap();

        let stats = sink.stats();
        assert_eq!(stats.committed, 1);
        assert_eq!(stats.rolled_back, 1);
        assert_eq!(stats.total_outputs_written, 2);
        assert_eq!(stats.buffer_count, 0);
    }

    #[test]
    fn test_multiple_sequential_commits() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        for i in 0..3 {
            let tx_id = sink.begin_transaction().unwrap();
            ExactlyOnceSink::write(
                &mut sink,
                &tx_id,
                vec![Output::Event(make_event(i * 1000, i))],
            )
            .unwrap();
            sink.commit(&tx_id).unwrap();
        }

        assert_eq!(sink.stats().committed, 3);
        assert_eq!(sink.stats().total_outputs_written, 3);
    }

    #[test]
    fn test_close_rollbacks_pending() {
        let inner = BufferingSink::new();
        let mut sink = TransactionalSink::new(inner, "test-sink");

        let tx_id = sink.begin_transaction().unwrap();
        ExactlyOnceSink::write(
            &mut sink,
            &tx_id,
            vec![Output::Event(make_event(1000, 42))],
        )
        .unwrap();

        sink.close().unwrap();
        assert_eq!(sink.state(), SinkState::Idle);
        assert!(sink.buffer.is_empty());
    }
}
