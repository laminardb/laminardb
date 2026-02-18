//! # Distributed Checkpoint Module
//!
//! Chandy-Lamport style barrier protocol for consistent distributed snapshots.
//!
//! ## Module Overview
//!
//! - [`barrier`]: Checkpoint barrier types, `StreamMessage<T>` enum, and
//!   cross-thread barrier injection

pub mod barrier;

// Re-export key types
pub use barrier::{
    BarrierPollHandle, CheckpointBarrier, CheckpointBarrierInjector, StreamMessage,
};
