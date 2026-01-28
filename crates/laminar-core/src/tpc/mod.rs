//! # Thread-Per-Core (TPC) Module
//!
//! Implements thread-per-core architecture for linear scaling on multi-core systems.
//!
//! ## Architecture
//!
//! Each CPU core runs a dedicated [`Reactor`](crate::reactor::Reactor) with its own state
//! partition. Events are routed to cores based on key hash, ensuring state locality.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   ThreadPerCoreRuntime                       │
//! │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
//! │  │ Core 0   │  │ Core 1   │  │ Core 2   │  │ Core N   │    │
//! │  │ Reactor  │  │ Reactor  │  │ Reactor  │  │ Reactor  │    │
//! │  │ State₀   │  │ State₁   │  │ State₂   │  │ StateN   │    │
//! │  └────▲─────┘  └────▲─────┘  └────▲─────┘  └────▲─────┘    │
//! │       │             │             │             │          │
//! │       └─────────────┴──────┬──────┴─────────────┘          │
//! │                            │                                │
//! │                    ┌───────┴───────┐                        │
//! │                    │  KeyRouter    │                        │
//! │                    │ hash(key) % N │                        │
//! │                    └───────┬───────┘                        │
//! │                            │                                │
//! └────────────────────────────┼────────────────────────────────┘
//!                              │
//!                        Input Events
//! ```
//!
//! ## Components
//!
//! - [`SpscQueue`] - Lock-free single-producer single-consumer queue
//! - [`KeyRouter`] - Routes events to cores based on key hash
//! - [`CoreHandle`] - Manages a single core's reactor thread
//! - [`ThreadPerCoreRuntime`] - Orchestrates multi-core processing
//!
//! ## Example
//!
//! ```rust,ignore
//! use laminar_core::tpc::{TpcConfig, ThreadPerCoreRuntime, KeySpec};
//!
//! // Configure for 4 cores, routing by "user_id" column
//! let config = TpcConfig::builder()
//!     .num_cores(4)
//!     .key_columns(vec!["user_id"])
//!     .build();
//!
//! let mut runtime = ThreadPerCoreRuntime::new(config)?;
//!
//! // Submit events - automatically routed by key
//! runtime.submit(event)?;
//!
//! // Poll all cores for outputs
//! let outputs = runtime.poll();
//! ```
//!
//! ## Performance Targets
//!
//! | Metric | Target |
//! |--------|--------|
//! | SPSC push/pop | < 50ns |
//! | Scaling efficiency | > 80% |
//! | Inter-core latency | < 1μs |

mod backpressure;
mod core_handle;
mod router;
mod runtime;
mod spsc;
#[cfg(test)]
mod zero_alloc_tests;

pub use backpressure::{
    BackpressureConfig, BackpressureConfigBuilder, CreditAcquireResult, CreditChannel,
    CreditGate, CreditMetrics, CreditMetricsSnapshot, CreditReceiver, CreditSender,
    OverflowStrategy,
};
pub use core_handle::{CoreConfig, CoreHandle, CoreMessage};
pub use router::{KeyRouter, KeySpec, RouterError};
pub use runtime::{OutputBuffer, ThreadPerCoreRuntime, TpcConfig, TpcConfigBuilder};
pub use spsc::{CachePadded, SpscQueue};

/// Errors that can occur in the TPC runtime.
#[derive(Debug, thiserror::Error)]
pub enum TpcError {
    /// Failed to spawn a core thread
    #[error("Failed to spawn core {core_id}: {message}")]
    SpawnFailed {
        /// The core ID that failed to spawn
        core_id: usize,
        /// Error message
        message: String,
    },

    /// Failed to set CPU affinity
    #[error("Failed to set CPU affinity for core {core_id}: {message}")]
    AffinityFailed {
        /// The core ID
        core_id: usize,
        /// Error message
        message: String,
    },

    /// Queue is full, cannot accept more events
    #[error("Queue full for core {core_id}")]
    QueueFull {
        /// The core ID whose queue is full
        core_id: usize,
    },

    /// Backpressure active, no credits available
    #[error("Backpressure active for core {core_id}")]
    Backpressure {
        /// The core ID that is backpressured
        core_id: usize,
    },

    /// Runtime is not running
    #[error("Runtime is not running")]
    NotRunning,

    /// Runtime is already running
    #[error("Runtime is already running")]
    AlreadyRunning,

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Reactor error from a core
    #[error("Reactor error on core {core_id}: {source}")]
    ReactorError {
        /// The core ID
        core_id: usize,
        /// The underlying reactor error
        #[source]
        source: crate::reactor::ReactorError,
    },

    /// Key extraction failed
    #[error("Key extraction failed: {0}")]
    KeyExtractionFailed(String),

    /// Router error (zero-allocation variant)
    #[error("Router error: {0}")]
    RouterError(#[from] RouterError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = TpcError::QueueFull { core_id: 3 };
        assert_eq!(err.to_string(), "Queue full for core 3");
    }
}
