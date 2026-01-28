//! Traits for exactly-once sink semantics

#![allow(clippy::cast_possible_truncation)]

use super::error::SinkError;
use super::checkpoint::SinkCheckpoint;
use crate::operator::Output;

/// Unique identifier for a transaction
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionId {
    /// The transaction ID value
    id: u64,
    /// Optional string identifier for external systems
    external_id: Option<String>,
}

impl TransactionId {
    /// Create a new transaction ID from a numeric value
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self {
            id,
            external_id: None,
        }
    }

    /// Create a transaction ID with an external identifier
    #[must_use]
    pub fn with_external(id: u64, external_id: String) -> Self {
        Self {
            id,
            external_id: Some(external_id),
        }
    }

    /// Get the numeric ID
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the external ID if present
    #[must_use]
    pub fn external_id(&self) -> Option<&str> {
        self.external_id.as_deref()
    }

    /// Serialize to bytes for checkpointing
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.id.to_le_bytes().to_vec();
        if let Some(ref ext) = self.external_id {
            bytes.extend_from_slice(&(ext.len() as u32).to_le_bytes());
            bytes.extend_from_slice(ext.as_bytes());
        }
        bytes
    }

    /// Deserialize from bytes
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        let id = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let external_id = if bytes.len() > 12 {
            let len = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;
            if bytes.len() >= 12 + len {
                Some(String::from_utf8_lossy(&bytes[12..12 + len]).to_string())
            } else {
                None
            }
        } else {
            None
        };
        Some(Self { id, external_id })
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref ext) = self.external_id {
            write!(f, "tx-{}-{}", self.id, ext)
        } else {
            write!(f, "tx-{}", self.id)
        }
    }
}

/// Capabilities that a sink may support
#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct SinkCapabilities {
    /// Supports transactional writes
    transactions: bool,
    /// Supports idempotent writes (safe to retry)
    idempotent_writes: bool,
    /// Supports upsert semantics (update or insert)
    upsert: bool,
    /// Supports changelog/CDC records
    changelog: bool,
    /// Supports two-phase commit
    two_phase_commit: bool,
    /// Supports partitioned writes
    partitioned: bool,
}

impl SinkCapabilities {
    /// Create new capabilities with all disabled
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable transaction support
    #[must_use]
    pub fn with_transactions(mut self) -> Self {
        self.transactions = true;
        self
    }

    /// Enable idempotent write support
    #[must_use]
    pub fn with_idempotent_writes(mut self) -> Self {
        self.idempotent_writes = true;
        self
    }

    /// Enable upsert support
    #[must_use]
    pub fn with_upsert(mut self) -> Self {
        self.upsert = true;
        self
    }

    /// Enable changelog support
    #[must_use]
    pub fn with_changelog_support(mut self) -> Self {
        self.changelog = true;
        self
    }

    /// Enable two-phase commit support
    #[must_use]
    pub fn with_two_phase_commit(mut self) -> Self {
        self.two_phase_commit = true;
        self
    }

    /// Enable partitioned write support
    #[must_use]
    pub fn with_partitioned(mut self) -> Self {
        self.partitioned = true;
        self
    }

    /// Check if transactions are supported
    #[must_use]
    pub fn supports_transactions(&self) -> bool {
        self.transactions
    }

    /// Check if idempotent writes are supported
    #[must_use]
    pub fn supports_idempotent_writes(&self) -> bool {
        self.idempotent_writes
    }

    /// Check if upsert is supported
    #[must_use]
    pub fn supports_upsert(&self) -> bool {
        self.upsert
    }

    /// Check if changelog is supported
    #[must_use]
    pub fn supports_changelog(&self) -> bool {
        self.changelog
    }

    /// Check if two-phase commit is supported
    #[must_use]
    pub fn supports_two_phase_commit(&self) -> bool {
        self.two_phase_commit
    }

    /// Check if partitioned writes are supported
    #[must_use]
    pub fn supports_partitioned(&self) -> bool {
        self.partitioned
    }
}

/// Current state of a sink
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkState {
    /// Sink is idle, ready to accept writes
    Idle,
    /// Sink has an active transaction
    InTransaction,
    /// Sink is preparing to commit (2PC phase 1)
    Preparing,
    /// Sink is committing
    Committing,
    /// Sink encountered an error
    Error,
    /// Sink is closed
    Closed,
}

impl SinkState {
    /// Check if the sink can accept writes
    #[must_use]
    pub fn can_write(&self) -> bool {
        matches!(self, Self::Idle | Self::InTransaction)
    }

    /// Check if the sink can begin a transaction
    #[must_use]
    pub fn can_begin_transaction(&self) -> bool {
        matches!(self, Self::Idle)
    }

    /// Check if the sink can commit
    #[must_use]
    pub fn can_commit(&self) -> bool {
        matches!(self, Self::InTransaction | Self::Preparing)
    }
}

/// Trait for sinks that support exactly-once delivery semantics.
///
/// This trait extends the basic `Sink` trait with transaction support,
/// enabling exactly-once delivery through atomic commits.
///
/// # Transaction Flow
///
/// ```text
/// begin_transaction() ──► write() ──► write() ──► commit()
///                              │              │
///                              ▼              ▼
///                         rollback() on failure
/// ```
///
/// # Recovery
///
/// On recovery:
/// 1. Load `SinkCheckpoint` from checkpoint
/// 2. If pending transaction exists, call `rollback()`
/// 3. Resume from last committed offset
pub trait ExactlyOnceSink: Send {
    /// Get the sink's capabilities
    fn capabilities(&self) -> SinkCapabilities;

    /// Get the current state of the sink
    fn state(&self) -> SinkState;

    /// Begin a new transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if a transaction is already active or if the
    /// sink cannot begin a transaction.
    fn begin_transaction(&mut self) -> Result<TransactionId, SinkError>;

    /// Write outputs within the current transaction.
    ///
    /// Writes are not visible until `commit()` is called.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The transaction ID returned by `begin_transaction()`
    /// * `outputs` - The outputs to write
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction ID is invalid or if the
    /// write operation fails.
    fn write(&mut self, tx_id: &TransactionId, outputs: Vec<Output>) -> Result<(), SinkError>;

    /// Commit the current transaction.
    ///
    /// After commit, all writes become visible and durable.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The transaction ID to commit
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction ID is invalid or if the
    /// commit operation fails.
    fn commit(&mut self, tx_id: &TransactionId) -> Result<(), SinkError>;

    /// Rollback the current transaction.
    ///
    /// Discards all writes made in the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The transaction ID to rollback
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction ID is invalid or if the
    /// rollback operation fails.
    fn rollback(&mut self, tx_id: &TransactionId) -> Result<(), SinkError>;

    /// Get the current checkpoint state for persistence.
    ///
    /// This should be called during checkpointing to capture the
    /// sink's state for recovery.
    fn checkpoint(&self) -> SinkCheckpoint;

    /// Restore the sink from a checkpoint.
    ///
    /// Called during recovery to restore the sink's state.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint is invalid or if the
    /// sink cannot be restored.
    fn restore(&mut self, checkpoint: &SinkCheckpoint) -> Result<(), SinkError>;

    /// Flush any buffered data.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush operation fails.
    fn flush(&mut self) -> Result<(), SinkError>;

    /// Close the sink and release resources.
    ///
    /// # Errors
    ///
    /// Returns an error if the sink cannot be closed cleanly.
    fn close(&mut self) -> Result<(), SinkError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_id_new() {
        let tx = TransactionId::new(123);
        assert_eq!(tx.id(), 123);
        assert_eq!(tx.external_id(), None);
        assert_eq!(tx.to_string(), "tx-123");
    }

    #[test]
    fn test_transaction_id_with_external() {
        let tx = TransactionId::with_external(456, "kafka-tx-abc".to_string());
        assert_eq!(tx.id(), 456);
        assert_eq!(tx.external_id(), Some("kafka-tx-abc"));
        assert!(tx.to_string().contains("kafka-tx-abc"));
    }

    #[test]
    fn test_transaction_id_serialization() {
        let tx = TransactionId::with_external(789, "test".to_string());
        let bytes = tx.to_bytes();
        let restored = TransactionId::from_bytes(&bytes).unwrap();
        assert_eq!(tx, restored);
    }

    #[test]
    fn test_sink_capabilities() {
        let caps = SinkCapabilities::new();
        assert!(!caps.supports_transactions());
        assert!(!caps.supports_upsert());

        let caps = SinkCapabilities::new()
            .with_transactions()
            .with_upsert();
        assert!(caps.supports_transactions());
        assert!(caps.supports_upsert());
        assert!(!caps.supports_changelog());
    }

    #[test]
    fn test_sink_state_transitions() {
        assert!(SinkState::Idle.can_begin_transaction());
        assert!(SinkState::Idle.can_write());
        assert!(!SinkState::Idle.can_commit());

        assert!(!SinkState::InTransaction.can_begin_transaction());
        assert!(SinkState::InTransaction.can_write());
        assert!(SinkState::InTransaction.can_commit());

        assert!(!SinkState::Error.can_write());
        assert!(!SinkState::Closed.can_write());
    }
}
