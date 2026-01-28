//! # NUMA Error Types
//!
//! Error types for NUMA-aware memory operations.

/// Errors that can occur during NUMA operations.
#[derive(Debug, thiserror::Error)]
pub enum NumaError {
    /// Allocation failed
    #[error("NUMA allocation failed: {0}")]
    AllocationFailed(String),

    /// Invalid NUMA node
    #[error("Invalid NUMA node: {node} (system has {available} nodes)")]
    InvalidNode {
        /// The requested node
        node: usize,
        /// Number of available nodes
        available: usize,
    },

    /// Invalid CPU ID
    #[error("Invalid CPU ID: {cpu} (system has {available} CPUs)")]
    InvalidCpu {
        /// The requested CPU
        cpu: usize,
        /// Number of available CPUs
        available: usize,
    },

    /// Memory binding failed
    #[error("Memory binding failed: {0}")]
    BindFailed(String),

    /// Topology detection failed
    #[error("Topology detection failed: {0}")]
    TopologyError(String),

    /// System call failed
    #[error("System call failed: {0}")]
    SyscallFailed(#[from] std::io::Error),

    /// NUMA not available on this platform
    #[error("NUMA not available on this platform")]
    NotAvailable,
}
