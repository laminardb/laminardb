//! # NUMA Topology Detection
//!
//! Detects system NUMA topology for thread-per-core architecture.
//! On multi-socket systems, memory access latency varies by 2-3x depending on
//! whether memory is local or remote to the CPU.
//!
//! ## Components
//!
//! - [`NumaTopology`] - Detects system NUMA topology
//!
//! ## Platform Support
//!
//! | Platform | Support |
//! |----------|---------|
//! | Linux | Full NUMA support |
//! | macOS | Degraded (single node) |
//! | Windows | Degraded (single node) |

mod error;
mod topology;

pub use error::NumaError;
pub use topology::NumaTopology;

/// Result type for NUMA operations.
pub type Result<T> = std::result::Result<T, NumaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_detection() {
        let topo = NumaTopology::detect();
        assert!(topo.num_nodes() >= 1);
        assert!(!topo.cpus_for_node(0).is_empty());
    }
}
