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

/// Serialized operator state for checkpointing.
#[derive(Debug, Clone)]
pub struct OperatorState {
    /// Operator ID
    pub operator_id: String,
    /// State format version (for forward/backward compatibility detection)
    pub version: u32,
    /// Serialized state data
    pub data: Vec<u8>,
}

impl OperatorState {
    /// Create a version-1 operator state.
    #[must_use]
    pub fn v1(operator_id: String, data: Vec<u8>) -> Self {
        Self {
            operator_id,
            version: 1,
            data,
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
pub mod table_cache;
pub mod window;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;

    #[test]
    fn test_event_creation() {
        let array = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_from_iter(vec![("col1", array as _)]).unwrap();

        let event = Event::new(12345, batch);

        assert_eq!(event.timestamp, 12345);
        assert_eq!(event.data.num_rows(), 3);
    }
}
