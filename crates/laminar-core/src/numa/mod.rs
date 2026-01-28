//! # NUMA-Aware Memory Allocation
//!
//! Implements NUMA-aware memory allocation for thread-per-core architecture.
//! On multi-socket systems, memory access latency varies by 2-3x depending on
//! whether memory is local or remote to the CPU.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────┐         ┌─────────────────────┐
//! │      Socket 0       │         │      Socket 1       │
//! │  ┌───────────────┐  │         │  ┌───────────────┐  │
//! │  │  Cores 0-15   │  │         │  │  Cores 16-31  │  │
//! │  └───────┬───────┘  │         │  └───────┬───────┘  │
//! │          │          │         │          │          │
//! │  ┌───────▼───────┐  │  QPI    │  ┌───────▼───────┐  │
//! │  │  Local DRAM   │◄─┼─────────┼─►│  Local DRAM   │  │
//! │  │  (~100ns)     │  │ (~150ns)│  │  (~100ns)     │  │
//! │  └───────────────┘  │         │  └───────────────┘  │
//! └─────────────────────┘         └─────────────────────┘
//! ```
//!
//! ## Components
//!
//! - [`NumaTopology`] - Detects system NUMA topology
//! - [`NumaAllocator`] - NUMA-local memory allocation
//! - [`NumaPlacement`] - Placement strategy for data structures
//!
//! ## Example
//!
//! ```rust,ignore
//! use laminar_core::numa::{NumaTopology, NumaAllocator, NumaPlacement};
//!
//! // Detect topology
//! let topology = NumaTopology::detect();
//! topology.log_topology();
//!
//! // Allocate on NUMA node local to core 4
//! let allocator = NumaAllocator::new(&topology);
//! let ptr = allocator.alloc_on_node(0, 4096, 64)?;
//!
//! // Free when done
//! unsafe { allocator.dealloc(ptr, 4096); }
//! ```
//!
//! ## Platform Support
//!
//! | Platform | Support |
//! |----------|---------|
//! | Linux | Full NUMA support |
//! | macOS | Degraded (single node) |
//! | Windows | Degraded (single node) |

mod allocator;
mod error;
mod topology;

pub use allocator::{NumaAllocator, NumaPlacement};
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

    #[test]
    fn test_allocator_local() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        // Allocate local to current CPU
        let ptr = allocator.alloc_local(4096, 64).unwrap();
        assert!(!ptr.is_null());

        // Free
        unsafe { allocator.dealloc(ptr, 4096) };
    }

    #[test]
    fn test_allocator_on_node() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        // Allocate on node 0
        let ptr = allocator.alloc_on_node(0, 4096, 64).unwrap();
        assert!(!ptr.is_null());

        // Free
        unsafe { allocator.dealloc(ptr, 4096) };
    }

    #[test]
    fn test_allocator_interleaved() {
        let topo = NumaTopology::detect();
        let allocator = NumaAllocator::new(&topo);

        // Allocate interleaved (for shared read-only data)
        let ptr = allocator.alloc_interleaved(64 * 1024, 64).unwrap();
        assert!(!ptr.is_null());

        // Free
        unsafe { allocator.dealloc(ptr, 64 * 1024) };
    }

    #[test]
    fn test_numa_placement() {
        let placement = NumaPlacement::for_state_store(0);
        assert!(matches!(placement, NumaPlacement::Local(0)));

        let placement = NumaPlacement::for_lookup_table();
        assert!(matches!(placement, NumaPlacement::Interleaved));
    }
}
