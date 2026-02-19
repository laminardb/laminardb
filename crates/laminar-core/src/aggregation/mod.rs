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
/// Gossip-based partial aggregate replication across nodes.
#[cfg(feature = "constellation")]
pub mod gossip_aggregates;
/// gRPC aggregate fan-out across constellation nodes (F-XAGG-003).
#[cfg(feature = "constellation")]
pub mod grpc_fanout;

pub use cross_partition::CrossPartitionAggregateStore;
