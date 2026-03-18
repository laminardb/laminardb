//! Distributed query execution for LaminarDB cluster mode.
//!
//! When partitions span multiple nodes, queries must be fanned out to each
//! partition owner, executed locally, and the partial results gathered and
//! merged at the coordinator node.
//!
//! # Execution strategies
//!
//! - **Local-only**: All referenced partitions reside on this node. Execute
//!   via the local [`StreamExecutor`](crate::stream_executor) directly.
//! - **Fan-out + merge**: Partitions span multiple nodes. The coordinator
//!   sends the query to each relevant peer, collects partial results, and
//!   merges (concat for non-agg, partial-aggregate merge for GROUP BY).
//! - **Broadcast**: Lookup/reference table queries sent to all nodes.

mod partition_map;
mod query_coordinator;

pub use partition_map::{NodeAssignment, PartitionMap};
pub use query_coordinator::{
    DistributedQueryCoordinator, ExecutionStrategy, GatherResult, MergeStrategy,
    NodeExecutionStats, PeerInfo,
};
