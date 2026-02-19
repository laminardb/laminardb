//! # Cross-Partition Aggregation Module
//!
//! Lock-free data structures for aggregating state across partitions
//! in a partition-parallel system.
//!
//! ## Module Overview
//!
//! - [`cross_partition`]: Concurrent partial aggregate store backed by `papaya::HashMap`

/// Lock-free cross-partition aggregate store.
pub mod cross_partition;

pub use cross_partition::CrossPartitionAggregateStore;
