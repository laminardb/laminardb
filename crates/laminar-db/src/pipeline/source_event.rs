//! Events emitted by per-source tasks to the pipeline coordinator.

use arrow_array::RecordBatch;

/// An event sent from a per-source task to the pipeline coordinator.
///
/// Uses a `usize` source index (not `String`) to avoid heap allocation on the
/// hot path.
#[derive(Debug)]
pub enum SourceEvent {
    /// A batch of records from source `idx`.
    Batch {
        /// Source index (position in the sources vec).
        idx: usize,
        /// The record batch.
        batch: RecordBatch,
    },

    /// Source `idx` encountered a poll error.
    Error {
        /// Source index.
        idx: usize,
        /// Error message.
        message: String,
    },

    /// Source `idx` has been exhausted (returned `None` from `poll_batch`).
    Exhausted {
        /// Source index.
        idx: usize,
    },
}
