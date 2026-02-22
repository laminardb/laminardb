//! Transaction management for exactly-once sinks

use std::sync::atomic::{AtomicU64, Ordering};

use super::error::SinkError;
use super::traits::{SinkState, TransactionId};
use crate::operator::Output;

/// State machine for transaction lifecycle
#[derive(Debug)]
pub struct TransactionState {
    /// Current transaction ID (if any)
    current_tx: Option<TransactionId>,

    /// Current state
    state: SinkState,

    /// Transaction counter for ID generation
    next_tx_id: AtomicU64,

    /// Number of writes in current transaction
    write_count: u64,

    /// Number of records in current transaction
    record_count: u64,
}

impl TransactionState {
    /// Create a new transaction state
    #[must_use]
    pub fn new() -> Self {
        Self {
            current_tx: None,
            state: SinkState::Idle,
            next_tx_id: AtomicU64::new(1),
            write_count: 0,
            record_count: 0,
        }
    }

    /// Check if the state is idle (no active transaction)
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.state == SinkState::Idle
    }

    /// Check if there's an active transaction
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state == SinkState::InTransaction
    }

    /// Get the current state
    #[must_use]
    pub fn state(&self) -> SinkState {
        self.state
    }

    /// Get the current transaction ID
    #[must_use]
    pub fn current_transaction(&self) -> Option<&TransactionId> {
        self.current_tx.as_ref()
    }

    /// Get the write count for the current transaction
    #[must_use]
    pub fn write_count(&self) -> u64 {
        self.write_count
    }

    /// Get the record count for the current transaction
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Begin a new transaction
    ///
    /// # Errors
    ///
    /// Returns an error if a transaction is already active.
    pub fn begin(&mut self, tx_id: TransactionId) -> Result<(), SinkError> {
        if !self.state.can_begin_transaction() {
            return Err(SinkError::TransactionAlreadyActive(
                self.current_tx
                    .as_ref()
                    .map_or_else(|| "unknown".to_string(), ToString::to_string),
            ));
        }

        self.current_tx = Some(tx_id);
        self.state = SinkState::InTransaction;
        self.write_count = 0;
        self.record_count = 0;

        Ok(())
    }

    /// Generate a new transaction ID and begin
    ///
    /// # Errors
    ///
    /// Returns an error if a transaction is already active.
    pub fn begin_new(&mut self) -> Result<TransactionId, SinkError> {
        let tx_id = TransactionId::new(self.next_tx_id.fetch_add(1, Ordering::Relaxed));
        self.begin(tx_id.clone())?;
        Ok(tx_id)
    }

    /// Record a write operation
    ///
    /// # Errors
    ///
    /// Returns an error if there's no active transaction.
    pub fn record_write(&mut self, record_count: u64) -> Result<(), SinkError> {
        if !self.is_active() {
            return Err(SinkError::NoActiveTransaction);
        }

        self.write_count += 1;
        self.record_count += record_count;

        Ok(())
    }

    /// Commit the current transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction ID doesn't match or if
    /// there's no active transaction.
    pub fn commit(&mut self, tx_id: &TransactionId) -> Result<(), SinkError> {
        self.validate_tx_id(tx_id)?;

        if !self.state.can_commit() {
            return Err(SinkError::NoActiveTransaction);
        }

        self.state = SinkState::Committing;
        // Reset after successful commit
        self.current_tx = None;
        self.state = SinkState::Idle;
        self.write_count = 0;
        self.record_count = 0;

        Ok(())
    }

    /// Rollback the current transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction ID doesn't match.
    pub fn rollback(&mut self, tx_id: &TransactionId) -> Result<(), SinkError> {
        self.validate_tx_id(tx_id)?;

        self.current_tx = None;
        self.state = SinkState::Idle;
        self.write_count = 0;
        self.record_count = 0;

        Ok(())
    }

    /// Force rollback without validation (for recovery)
    pub fn force_rollback(&mut self) {
        self.current_tx = None;
        self.state = SinkState::Idle;
        self.write_count = 0;
        self.record_count = 0;
    }

    /// Mark the state as error
    pub fn mark_error(&mut self) {
        self.state = SinkState::Error;
    }

    /// Validate that the given transaction ID matches the current one
    fn validate_tx_id(&self, tx_id: &TransactionId) -> Result<(), SinkError> {
        match &self.current_tx {
            Some(current) if current == tx_id => Ok(()),
            Some(current) => Err(SinkError::TransactionIdMismatch {
                expected: current.to_string(),
                actual: tx_id.to_string(),
            }),
            None => Err(SinkError::NoActiveTransaction),
        }
    }
}

impl Default for TransactionState {
    fn default() -> Self {
        Self::new()
    }
}

/// Coordinator for two-phase commit across multiple sinks
pub struct TransactionCoordinator {
    /// Current transaction ID
    current_tx: Option<TransactionId>,

    /// Transaction counter
    next_tx_id: AtomicU64,

    /// Participating sinks (by ID)
    participants: Vec<String>,

    /// Sinks that have voted to prepare
    prepared: Vec<String>,

    /// Sinks that have committed
    committed: Vec<String>,
}

impl TransactionCoordinator {
    /// Create a new transaction coordinator
    #[must_use]
    pub fn new() -> Self {
        Self {
            current_tx: None,
            next_tx_id: AtomicU64::new(1),
            participants: Vec::new(),
            prepared: Vec::new(),
            committed: Vec::new(),
        }
    }

    /// Register a participant sink
    pub fn register_participant(&mut self, sink_id: String) {
        if !self.participants.contains(&sink_id) {
            self.participants.push(sink_id);
        }
    }

    /// Begin a new coordinated transaction
    ///
    /// # Errors
    ///
    /// Returns an error if a transaction is already active.
    pub fn begin(&mut self) -> Result<TransactionId, SinkError> {
        if let Some(ref tx) = self.current_tx {
            return Err(SinkError::TransactionAlreadyActive(tx.to_string()));
        }

        let tx_id = TransactionId::new(self.next_tx_id.fetch_add(1, Ordering::Relaxed));
        self.current_tx = Some(tx_id.clone());
        self.prepared.clear();
        self.committed.clear();

        Ok(tx_id)
    }

    /// Record that a sink has prepared
    ///
    /// # Errors
    ///
    /// Returns an error if the sink is not registered.
    pub fn mark_prepared(&mut self, sink_id: &str) -> Result<(), SinkError> {
        if !self.participants.contains(&sink_id.to_string()) {
            return Err(SinkError::ConfigurationError(format!(
                "Unknown sink: {sink_id}"
            )));
        }

        if !self.prepared.contains(&sink_id.to_string()) {
            self.prepared.push(sink_id.to_string());
        }

        Ok(())
    }

    /// Check if all participants are prepared
    #[must_use]
    pub fn all_prepared(&self) -> bool {
        self.prepared.len() == self.participants.len()
    }

    /// Record that a sink has committed
    pub fn mark_committed(&mut self, sink_id: &str) {
        if !self.committed.contains(&sink_id.to_string()) {
            self.committed.push(sink_id.to_string());
        }
    }

    /// Check if all participants have committed
    #[must_use]
    pub fn all_committed(&self) -> bool {
        self.committed.len() == self.participants.len()
    }

    /// Complete the transaction
    pub fn complete(&mut self) {
        self.current_tx = None;
        self.prepared.clear();
        self.committed.clear();
    }

    /// Get the current transaction ID
    #[must_use]
    pub fn current_transaction(&self) -> Option<&TransactionId> {
        self.current_tx.as_ref()
    }

    /// Get the list of participants
    #[must_use]
    pub fn participants(&self) -> &[String] {
        &self.participants
    }
}

impl Default for TransactionCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for sinks that support two-phase commit
pub trait TwoPhaseCommitSink: Send {
    /// Prepare the transaction for commit (2PC phase 1)
    ///
    /// After prepare, the sink should be ready to commit atomically.
    ///
    /// # Errors
    ///
    /// Returns an error if the sink cannot prepare.
    fn prepare(&mut self, tx_id: &TransactionId) -> Result<(), SinkError>;

    /// Commit a prepared transaction (2PC phase 2)
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails.
    fn commit_prepared(&mut self, tx_id: &TransactionId) -> Result<(), SinkError>;

    /// Abort a prepared transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the abort fails.
    fn abort_prepared(&mut self, tx_id: &TransactionId) -> Result<(), SinkError>;

    /// Recover pending transactions after restart
    ///
    /// Returns a list of transaction IDs that were prepared but not committed.
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    fn recover_pending(&mut self) -> Result<Vec<TransactionId>, SinkError>;
}

/// Buffer for transactional writes
#[derive(Debug, Default)]
#[allow(dead_code)] // Public API for Phase 3 connector implementations
pub struct TransactionBuffer {
    /// Buffered outputs
    outputs: Vec<Output>,

    /// Total size estimate
    size_bytes: usize,
}

#[allow(dead_code)] // Public API for Phase 3 connector implementations
impl TransactionBuffer {
    /// Create a new transaction buffer
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with pre-allocated capacity
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            outputs: Vec::with_capacity(capacity),
            size_bytes: 0,
        }
    }

    /// Add outputs to the buffer
    pub fn push(&mut self, outputs: Vec<Output>) {
        // Estimate size (rough approximation)
        for output in &outputs {
            self.size_bytes += match output {
                Output::Event(e) => e.data.get_array_memory_size(),
                Output::Changelog(c) => c.event.data.get_array_memory_size() + 32,
                _ => 32,
            };
        }
        self.outputs.extend(outputs);
    }

    /// Get the number of outputs
    #[must_use]
    pub fn len(&self) -> usize {
        self.outputs.len()
    }

    /// Check if the buffer is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }

    /// Get the estimated size in bytes
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Take all buffered outputs
    pub fn take(&mut self) -> Vec<Output> {
        self.size_bytes = 0;
        std::mem::take(&mut self.outputs)
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.outputs.clear();
        self.size_bytes = 0;
    }

    /// Get a reference to the buffered outputs
    #[must_use]
    pub fn outputs(&self) -> &[Output] {
        &self.outputs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_state_new() {
        let state = TransactionState::new();
        assert!(state.is_idle());
        assert!(!state.is_active());
        assert!(state.current_transaction().is_none());
    }

    #[test]
    fn test_transaction_state_begin() {
        let mut state = TransactionState::new();
        let tx_id = TransactionId::new(1);

        state.begin(tx_id.clone()).unwrap();

        assert!(state.is_active());
        assert!(!state.is_idle());
        assert_eq!(state.current_transaction(), Some(&tx_id));
    }

    #[test]
    fn test_transaction_state_begin_new() {
        let mut state = TransactionState::new();

        let tx1 = state.begin_new().unwrap();
        state.commit(&tx1).unwrap();

        let tx2 = state.begin_new().unwrap();
        assert_ne!(tx1.id(), tx2.id());
    }

    #[test]
    fn test_transaction_state_double_begin() {
        let mut state = TransactionState::new();
        state.begin(TransactionId::new(1)).unwrap();

        let result = state.begin(TransactionId::new(2));
        assert!(matches!(
            result,
            Err(SinkError::TransactionAlreadyActive(_))
        ));
    }

    #[test]
    fn test_transaction_state_commit() {
        let mut state = TransactionState::new();
        let tx_id = TransactionId::new(1);

        state.begin(tx_id.clone()).unwrap();
        state.commit(&tx_id).unwrap();

        assert!(state.is_idle());
        assert!(state.current_transaction().is_none());
    }

    #[test]
    fn test_transaction_state_commit_wrong_id() {
        let mut state = TransactionState::new();
        state.begin(TransactionId::new(1)).unwrap();

        let result = state.commit(&TransactionId::new(2));
        assert!(matches!(
            result,
            Err(SinkError::TransactionIdMismatch { .. })
        ));
    }

    #[test]
    fn test_transaction_state_rollback() {
        let mut state = TransactionState::new();
        let tx_id = TransactionId::new(1);

        state.begin(tx_id.clone()).unwrap();
        state.record_write(100).unwrap();
        state.rollback(&tx_id).unwrap();

        assert!(state.is_idle());
        assert_eq!(state.write_count(), 0);
        assert_eq!(state.record_count(), 0);
    }

    #[test]
    fn test_transaction_state_force_rollback() {
        let mut state = TransactionState::new();
        state.begin(TransactionId::new(1)).unwrap();
        state.mark_error();

        state.force_rollback();

        assert!(state.is_idle());
    }

    #[test]
    fn test_transaction_coordinator_basic() {
        let mut coord = TransactionCoordinator::new();

        coord.register_participant("sink-1".to_string());
        coord.register_participant("sink-2".to_string());

        let _tx_id = coord.begin().unwrap();
        assert!(coord.current_transaction().is_some());

        coord.mark_prepared("sink-1").unwrap();
        assert!(!coord.all_prepared());

        coord.mark_prepared("sink-2").unwrap();
        assert!(coord.all_prepared());

        coord.mark_committed("sink-1");
        assert!(!coord.all_committed());

        coord.mark_committed("sink-2");
        assert!(coord.all_committed());

        coord.complete();
        assert!(coord.current_transaction().is_none());
    }

    #[test]
    fn test_transaction_buffer() {
        use crate::operator::Event;
        use arrow_array::{Int64Array, RecordBatch};
        use std::sync::Arc;

        let mut buffer = TransactionBuffer::new();

        let array = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("col", array as _)]).unwrap();
        let event = Event::new(1000, batch);

        buffer.push(vec![Output::Event(event)]);

        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_empty());
        assert!(buffer.size_bytes() > 0);

        let outputs = buffer.take();
        assert_eq!(outputs.len(), 1);
        assert!(buffer.is_empty());
    }
}
