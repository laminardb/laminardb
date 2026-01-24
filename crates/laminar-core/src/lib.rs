//! # `LaminarDB` Core
//!
//! The core streaming engine for `LaminarDB`, implementing the Ring 0 (hot path) components.
//!
//! This crate provides:
//! - **Reactor**: Single-threaded event loop with zero allocations
//! - **Operators**: Streaming operators (map, filter, window, join)
//! - **State Store**: Lock-free state management with sub-microsecond lookup
//! - **Time**: Event time processing, watermarks, and timers
//!
//! ## Design Principles
//!
//! 1. **Zero allocations on hot path** - Uses arena allocators
//! 2. **No locks on hot path** - SPSC queues, lock-free structures
//! 3. **Predictable latency** - < 1μs event processing
//! 4. **CPU cache friendly** - Data structures optimized for cache locality
//!
//! ## Example
//!
//! ```rust,ignore
//! use laminar_core::{Reactor, Config};
//!
//! let config = Config::default();
//! let mut reactor = Reactor::new(config)?;
//!
//! // Run the event loop
//! reactor.run()?;
//! ```

#![deny(missing_docs)]
#![deny(unsafe_code)] // Will selectively allow where needed with justification
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod operator;
pub mod reactor;
pub mod state;
pub mod time;
pub mod tpc;

// Re-export key types
pub use reactor::{Config, Reactor};

/// Result type for laminar-core operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for laminar-core
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Reactor-related errors
    #[error("Reactor error: {0}")]
    Reactor(#[from] reactor::ReactorError),

    /// State store errors
    #[error("State error: {0}")]
    State(#[from] state::StateError),

    /// Operator errors
    #[error("Operator error: {0}")]
    Operator(#[from] operator::OperatorError),

    /// Time-related errors
    #[error("Time error: {0}")]
    Time(#[from] time::TimeError),

    /// Thread-per-core runtime errors
    #[error("TPC error: {0}")]
    Tpc(#[from] tpc::TpcError),
}
