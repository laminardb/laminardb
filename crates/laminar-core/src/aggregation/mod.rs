//! # Cross-Partition Aggregation Module
//!
//! Lock-free data structures for aggregating state across partitions
//! in a partition-parallel system.
//!
//! ## Module Overview
//!
//! - `cross_partition`: Concurrent partial aggregate store backed by `papaya::HashMap`
//! - `two_phase`: Two-phase aggregation (partial per partition → merge on coordinator)

/// Lock-free cross-partition aggregate store.
pub mod cross_partition;
/// Gossip-based partial aggregate replication across nodes.
#[cfg(feature = "delta")]
pub mod gossip_aggregates;
/// Two-phase cross-partition aggregation.
pub mod two_phase;

pub use cross_partition::CrossPartitionAggregateStore;
