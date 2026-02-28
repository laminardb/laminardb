//! # Distributed Checkpoint Module
//!
//! Chandy-Lamport style barrier protocol for consistent distributed snapshots.
//!
//! ## Module Overview
//!
//! - `barrier`: Checkpoint barrier types, `StreamMessage<T>` enum, and
//!   cross-thread barrier injection
//! - `alignment`: Multi-input barrier alignment with buffering

/// Barrier alignment for multi-input operators.
pub mod alignment;
/// Checkpoint barrier types and cross-thread injection.
pub mod barrier;
/// Unaligned checkpoint protocol with timeout-based fallback.
pub mod unaligned;

// Re-export key types
pub use alignment::{AlignmentAction, BarrierAligner};
pub use barrier::{BarrierPollHandle, CheckpointBarrier, CheckpointBarrierInjector, StreamMessage};
pub use unaligned::{
    InFlightChannelData, UnalignedAction, UnalignedCheckpointConfig, UnalignedCheckpointer,
    UnalignedSnapshot,
};
