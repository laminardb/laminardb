//! # Distributed Checkpoint Module
//!
//! Chandy-Lamport style barrier protocol for consistent distributed snapshots.
//!
//! ## Module Overview
//!
//! - [`barrier`]: Checkpoint barrier types, `StreamMessage<T>` enum, and
//!   cross-thread barrier injection
//! - [`alignment`]: Multi-input barrier alignment with buffering

/// Barrier alignment for multi-input operators (F-DCKP-002).
pub mod alignment;
/// Checkpoint barrier types and cross-thread injection (F-DCKP-001).
pub mod barrier;

// Re-export key types
pub use alignment::{AlignmentAction, BarrierAligner};
pub use barrier::{
    BarrierPollHandle, CheckpointBarrier, CheckpointBarrierInjector, StreamMessage,
};
