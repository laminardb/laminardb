//! Events emitted by per-source tasks to the pipeline coordinator.

use arrow_array::RecordBatch;
use laminar_core::checkpoint::CheckpointBarrier;

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

    /// A checkpoint barrier from source `idx`.
    ///
    /// Emitted when the source task detects a pending barrier via its
    /// [`BarrierPollHandle`](laminar_core::checkpoint::BarrierPollHandle). The source captures its checkpoint before
    /// sending this event, so the coordinator can read the watch channel
    /// to get the barrier-consistent offset.
    Barrier {
        /// Source index.
        idx: usize,
        /// The checkpoint barrier.
        barrier: CheckpointBarrier,
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
