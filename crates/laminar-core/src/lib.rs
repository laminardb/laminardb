//! Core streaming engine for `LaminarDB`.

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
// Allow unsafe in alloc module for zero-copy optimizations
#![allow(unsafe_code)]

pub mod alloc;
/// Distributed checkpoint barrier protocol.
pub mod checkpoint;
/// Structured error code registry (`LDB-NNNN`) and Ring 0 hot path error type.
pub mod error_codes;
/// Lookup table types and predicate pushdown.
pub mod lookup;
pub mod mv;
pub mod operator;
/// Shared Arrow IPC serialization for `RecordBatch` ↔ bytes.
pub mod serialization;
pub mod streaming;
pub mod time;

/// Distributed cluster coordination. Unstable: gated behind `cluster-unstable`.
#[cfg(feature = "cluster-unstable")]
pub mod delta;

/// Result type for laminar-core operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for laminar-core
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Operator errors
    #[error("Operator error: {0}")]
    Operator(#[from] operator::OperatorError),

    /// Time-related errors
    #[error("Time error: {0}")]
    Time(#[from] time::TimeError),

    /// Materialized view errors
    #[error("MV error: {0}")]
    Mv(#[from] mv::MvError),
}
