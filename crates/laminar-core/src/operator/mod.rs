//! Streaming operator types and window assigners for stream processing.

use std::sync::Arc;

use arrow_array::RecordBatch;
use smallvec::SmallVec;

/// Timer key type optimized for window IDs (16 bytes).
pub type TimerKey = SmallVec<[u8; 16]>;

/// An event flowing through the system.
#[derive(Debug, Clone)]
pub struct Event {
    /// Timestamp of the event
    pub timestamp: i64,
    /// Event payload as Arrow `RecordBatch` wrapped in `Arc` for zero-copy multicast.
    pub data: Arc<RecordBatch>,
}

impl Event {
    /// Create a new event, wrapping the batch in `Arc` for zero-copy sharing.
    #[must_use]
    pub fn new(timestamp: i64, data: RecordBatch) -> Self {
        Self {
            timestamp,
            data: Arc::new(data),
        }
    }
}

/// Errors that can occur in operators.
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    /// State access error
    #[error("State access failed: {0}")]
    StateAccessFailed(String),

    /// Serialization error
    #[error("Serialization failed: {0}")]
    SerializationFailed(String),

    /// Processing error
    #[error("Processing failed: {0}")]
    ProcessingFailed(String),

    /// Configuration error (e.g., missing required builder field)
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

impl From<arrow_schema::ArrowError> for OperatorError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::SerializationFailed(e.to_string())
    }
}

pub mod sliding_window;
pub mod window;

#[cfg(test)]
mod tests;
