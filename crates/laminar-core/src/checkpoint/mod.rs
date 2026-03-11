//! # Distributed Checkpoint Module
//!
//! Chandy-Lamport style barrier protocol for consistent distributed snapshots.
//!
//! ## Module Overview
//!
//! - `barrier`: Checkpoint barrier types, `StreamMessage<T>` enum, and
//!   cross-thread barrier injection
//! - `unaligned`: Timeout-based fallback for unaligned checkpoints

/// Checkpoint barrier types and cross-thread injection.
pub mod barrier;
/// Unaligned checkpoint protocol with timeout-based fallback.
pub mod unaligned;

// Re-export key types
pub use barrier::{
    flags, BarrierPollHandle, CheckpointBarrier, CheckpointBarrierInjector, StreamMessage,
};
pub use unaligned::{
    InFlightChannelData, UnalignedAction, UnalignedCheckpointConfig, UnalignedCheckpointer,
    UnalignedSnapshot,
};
