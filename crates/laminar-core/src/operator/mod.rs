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
use smallvec::SmallVec;

/// Timer key type optimized for window IDs (16 bytes).
/// Re-exported from time module for convenience.
pub type TimerKey = SmallVec<[u8; 16]>;

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
    /// Late event that arrived after watermark (no side output configured)
    LateEvent(Event),
    /// Late event routed to a named side output
    SideOutput {
        /// The name of the side output to route to
        name: String,
        /// The late event
        event: Event,
    },
    /// Changelog record with Z-set weight (F011B).
    ///
    /// Used by `EmitStrategy::Changelog` to emit structured change records
    /// for CDC pipelines and cascading materialized views.
    Changelog(window::ChangelogRecord),
    /// Checkpoint completion with snapshotted operator states.
    ///
    /// Emitted when a `CheckpointRequest` is processed by a core thread.
    /// Carries the checkpoint ID and all operator states for persistence by Ring 1.
    CheckpointComplete {
        /// The checkpoint ID from the request
        checkpoint_id: u64,
        /// Snapshotted states from all operators on this core
        operator_states: Vec<OperatorState>,
    },
}

/// Collection type for operator outputs.
///
/// Uses `SmallVec` to avoid heap allocation for common cases (0-3 outputs).
/// The size 4 is chosen based on typical operator patterns:
/// - 0 outputs: filter that drops events
/// - 1 output: most common case (map, regular processing)
/// - 2 outputs: event + watermark
/// - 3+ outputs: flatmap or window emission
pub type OutputVec = SmallVec<[Output; 4]>;

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
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec;

    /// Handle timer expiration
    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> OutputVec;

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
    /// Timer key (uses `SmallVec` to avoid heap allocation for keys up to 16 bytes)
    pub key: TimerKey,
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

pub mod asof_join;
pub mod changelog;
pub mod lookup_join;
pub mod partitioned_topk;
pub mod session_window;
pub mod sliding_window;
pub mod stream_join;
pub mod temporal_join;
pub mod topk;
pub mod watermark_sort;
pub mod window;
pub mod window_sort;

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
