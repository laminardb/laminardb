//! Checkpoint barrier protocol.
//!
//! Coordinator-triggered barriers flow through sources to trigger consistent
//! state snapshots. The fast path is a single `AtomicU64` load (~10ns).

/// Checkpoint barrier types and cross-thread injection.
pub mod barrier;

pub use barrier::{
    flags, BarrierPollHandle, CheckpointBarrier, CheckpointBarrierInjector, StreamMessage,
};
