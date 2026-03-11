//! # Thread-Per-Core (TPC) Module
//!
//! Implements thread-per-core architecture for linear scaling on multi-core systems.
//!
//! ## Components
//!
//! - [`SpscQueue`] - Lock-free single-producer single-consumer queue
//! - [`CoreHandle`] - Manages a single core's reactor thread
//! - [`CreditGate`] - Credit-based backpressure
//! - [`TpcConfig`] / [`OutputBuffer`] - Runtime configuration and output collection

mod backpressure;
mod core_handle;
mod partitioned_router;
mod router;
mod runtime;
mod spsc;
#[cfg(test)]
mod zero_alloc_tests;

pub use backpressure::{
    BackpressureConfig, BackpressureConfigBuilder, CreditAcquireResult, CreditGate, CreditMetrics,
    CreditMetricsSnapshot, OverflowStrategy,
};
pub use core_handle::{CoreConfig, CoreHandle, CoreMessage, TaggedOutput};
pub use partitioned_router::PartitionedRouter;
pub use router::{KeySpec, RouterError};
pub use runtime::{OutputBuffer, TpcConfig, TpcConfigBuilder};
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

    /// An operator panicked inside Ring 0.
    #[error("Operator panic on core {core_id}: {message}")]
    OperatorPanic {
        /// The core ID where the panic occurred.
        core_id: usize,
        /// Panic message extracted from the payload.
        message: String,
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
