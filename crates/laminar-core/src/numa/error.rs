//! # NUMA Error Types
//!
//! Error types for NUMA-aware memory operations.

/// Errors that can occur during NUMA operations.
#[derive(Debug, thiserror::Error)]
pub enum NumaError {
    /// Topology detection failed
    #[error("Topology detection failed: {0}")]
    TopologyError(String),

    /// System call failed
    #[error("System call failed: {0}")]
    SyscallFailed(#[from] std::io::Error),

    /// NUMA memory bind failed
    #[error("NUMA memory bind failed: {0}")]
    BindFailed(std::io::Error),
}
