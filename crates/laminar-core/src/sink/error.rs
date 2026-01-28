//! Error types for exactly-once sinks

use crate::reactor::SinkError as ReactorSinkError;

/// Errors that can occur in exactly-once sinks
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    /// Transaction has already started
    #[error("Transaction already active: {0}")]
    TransactionAlreadyActive(String),

    /// No active transaction
    #[error("No active transaction")]
    NoActiveTransaction,

    /// Transaction ID mismatch
    #[error("Transaction ID mismatch: expected {expected}, got {actual}")]
    TransactionIdMismatch {
        /// Expected transaction ID
        expected: String,
        /// Actual transaction ID
        actual: String,
    },

    /// Transaction commit failed
    #[error("Transaction commit failed: {0}")]
    CommitFailed(String),

    /// Transaction rollback failed
    #[error("Transaction rollback failed: {0}")]
    RollbackFailed(String),

    /// Write operation failed
    #[error("Write failed: {0}")]
    WriteFailed(String),

    /// Flush operation failed
    #[error("Flush failed: {0}")]
    FlushFailed(String),

    /// Checkpoint serialization/deserialization failed
    #[error("Checkpoint error: {0}")]
    CheckpointError(String),

    /// Deduplication store error
    #[error("Deduplication error: {0}")]
    DeduplicationError(String),

    /// Sink is not connected
    #[error("Sink not connected")]
    NotConnected,

    /// Sink connection error
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// Timeout occurred
    #[error("Timeout after {0}ms")]
    Timeout(u64),

    /// Sink does not support the requested operation
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<SinkError> for ReactorSinkError {
    fn from(err: SinkError) -> Self {
        match err {
            SinkError::WriteFailed(msg) => ReactorSinkError::WriteFailed(msg),
            SinkError::FlushFailed(msg) => ReactorSinkError::FlushFailed(msg),
            SinkError::NotConnected => ReactorSinkError::Closed,
            other => ReactorSinkError::WriteFailed(other.to_string()),
        }
    }
}

impl From<ReactorSinkError> for SinkError {
    fn from(err: ReactorSinkError) -> Self {
        match err {
            ReactorSinkError::WriteFailed(msg) => SinkError::WriteFailed(msg),
            ReactorSinkError::FlushFailed(msg) => SinkError::FlushFailed(msg),
            ReactorSinkError::Closed => SinkError::NotConnected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = SinkError::TransactionAlreadyActive("tx-123".to_string());
        assert!(err.to_string().contains("tx-123"));

        let err = SinkError::TransactionIdMismatch {
            expected: "tx-1".to_string(),
            actual: "tx-2".to_string(),
        };
        assert!(err.to_string().contains("tx-1"));
        assert!(err.to_string().contains("tx-2"));
    }

    #[test]
    fn test_error_conversion_to_reactor() {
        let err = SinkError::WriteFailed("test".to_string());
        let reactor_err: ReactorSinkError = err.into();
        assert!(matches!(reactor_err, ReactorSinkError::WriteFailed(_)));

        let err = SinkError::NotConnected;
        let reactor_err: ReactorSinkError = err.into();
        assert!(matches!(reactor_err, ReactorSinkError::Closed));
    }

    #[test]
    fn test_error_conversion_from_reactor() {
        let reactor_err = ReactorSinkError::WriteFailed("test".to_string());
        let err: SinkError = reactor_err.into();
        assert!(matches!(err, SinkError::WriteFailed(_)));

        let reactor_err = ReactorSinkError::Closed;
        let err: SinkError = reactor_err.into();
        assert!(matches!(err, SinkError::NotConnected));
    }
}
