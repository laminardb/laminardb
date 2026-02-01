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
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
// Allow unsafe in alloc module for zero-copy optimizations
#![allow(unsafe_code)]

pub mod alloc;
pub mod budget;
pub mod dag;
pub mod io_uring;
pub mod mv;
pub mod numa;
pub mod operator;
pub mod reactor;
pub mod sink;
pub mod state;
pub mod streaming;
pub mod subscription;
pub mod time;
pub mod tpc;
pub mod xdp;

// Re-export key types
pub use reactor::{ReactorConfig, Reactor};

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

    /// `io_uring` errors
    #[error("io_uring error: {0}")]
    IoUring(#[from] io_uring::IoUringError),

    /// NUMA errors
    #[error("NUMA error: {0}")]
    Numa(#[from] numa::NumaError),

    /// Sink errors
    #[error("Sink error: {0}")]
    Sink(#[from] sink::SinkError),

    /// Materialized view errors
    #[error("MV error: {0}")]
    Mv(#[from] mv::MvError),

    /// XDP/eBPF errors
    #[error("XDP error: {0}")]
    Xdp(#[from] xdp::XdpError),

    /// DAG topology errors
    #[error("DAG error: {0}")]
    Dag(#[from] dag::DagError),
}
