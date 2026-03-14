//! # `LaminarDB` Core
//!
//! The core streaming engine for `LaminarDB`.
//!
//! This crate provides:
//! - **Operators**: Streaming operators (map, filter, window, join)
//! - **DAG**: Dataflow graph execution and checkpoint coordination
//! - **State Store**: Lock-free state management with sub-microsecond lookup
//! - **Time**: Event time processing, watermarks, and timers
//! - **Streaming**: SPSC/MPSC channels, sources, sinks, subscriptions
//!
//! ## Design Principles
//!
//! 1. **Zero allocations on hot path** - Uses arena allocators
//! 2. **No locks on hot path** - SPSC queues, lock-free structures
//! 3. **Predictable latency** - < 1μs event processing
//! 4. **CPU cache friendly** - Data structures optimized for cache locality

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
// Allow unsafe in alloc module for zero-copy optimizations
#![allow(unsafe_code)]

/// Cross-partition aggregation.
pub mod aggregation;
pub mod alloc;
pub mod budget;
/// Distributed checkpoint barrier protocol.
pub mod checkpoint;
pub mod compiler;
pub mod dag;
pub mod detect;
/// Structured error code registry (`LDB-NNNN`) and Ring 0 hot path error type.
pub mod error_codes;
/// Lookup table types and predicate pushdown.
pub mod lookup;
pub mod mv;
pub mod operator;
/// Shared Arrow IPC serialization for `RecordBatch` ↔ bytes.
pub mod serialization;
pub mod state;
pub mod streaming;
pub mod subscription;
pub mod time;

/// Distributed delta mode (multi-node coordination).
#[cfg(feature = "delta")]
pub mod delta;

/// Result type for laminar-core operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for laminar-core
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// State store errors
    #[error("State error: {0}")]
    State(#[from] state::StateError),

    /// Operator errors
    #[error("Operator error: {0}")]
    Operator(#[from] operator::OperatorError),

    /// Time-related errors
    #[error("Time error: {0}")]
    Time(#[from] time::TimeError),

    /// Materialized view errors
    #[error("MV error: {0}")]
    Mv(#[from] mv::MvError),

    /// DAG topology errors
    #[error("DAG error: {0}")]
    Dag(#[from] dag::DagError),
}
