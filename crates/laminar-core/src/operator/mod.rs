//! # Operator Module
//!
//! Streaming operators for transforming and processing events.
//!
//! ## Operator Types
//!
//! - **Stateless**: map, filter, flatmap
//! - **Stateful**: window, aggregate, join
//!
//! All operators implement the `Operator` trait and can be composed into
//! directed acyclic graphs (DAGs) for complex stream processing.

use arrow_array::RecordBatch;

/// An event flowing through the system
#[derive(Debug, Clone)]
pub struct Event {
    /// Timestamp of the event
    pub timestamp: i64,
    /// Event payload as Arrow `RecordBatch`
    pub data: RecordBatch,
}

/// Output from an operator
#[derive(Debug)]
pub enum Output {
    /// Regular event output
    Event(Event),
    /// Watermark update
    Watermark(i64),
    /// Late event that arrived after watermark
    LateEvent(Event),
}

/// Context provided to operators during processing
pub struct OperatorContext<'a> {
    /// Current event time
    pub event_time: i64,
    /// Current processing time (system time in microseconds)
    pub processing_time: i64,
    /// Timer registration
    pub timers: &'a mut crate::time::TimerService,
    /// State store access
    pub state: &'a mut dyn crate::state::StateStore,
    /// Watermark generator
    pub watermark_generator: &'a mut dyn crate::time::WatermarkGenerator,
    /// Operator index in the chain
    pub operator_index: usize,
}

/// Trait implemented by all streaming operators
pub trait Operator: Send {
    /// Process an incoming event
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> Vec<Output>;

    /// Handle timer expiration
    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> Vec<Output>;

    /// Checkpoint the operator's state
    fn checkpoint(&self) -> OperatorState;

    /// Restore from a checkpoint
    ///
    /// # Errors
    ///
    /// Returns `OperatorError::StateAccessFailed` if the state cannot be accessed
    /// Returns `OperatorError::SerializationFailed` if the state cannot be deserialized
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError>;
}

/// A timer registration
#[derive(Debug, Clone)]
pub struct Timer {
    /// Timer key
    pub key: Vec<u8>,
    /// Expiration timestamp
    pub timestamp: i64,
}

/// Serialized operator state for checkpointing
#[derive(Debug, Clone)]
pub struct OperatorState {
    /// Operator ID
    pub operator_id: String,
    /// Serialized state data
    pub data: Vec<u8>,
}

/// Errors that can occur in operators
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
}

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

        let event = Event {
            timestamp: 12345,
            data: batch,
        };

        assert_eq!(event.timestamp, 12345);
        assert_eq!(event.data.num_rows(), 3);
    }
}