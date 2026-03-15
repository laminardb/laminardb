//! # DAG Pipeline Topology
//!
//! Directed Acyclic Graph (DAG) topology data structures for complex streaming
//! workflows with fan-out, fan-in, and shared intermediate stages.
//!
//! ## Overview
//!
//! This module provides the topology layer for DAG pipelines:
//!
//! - **`StreamingDag`**: The complete DAG topology with topological ordering
//! - **`DagBuilder`**: Fluent builder API for programmatic DAG construction
//! - **`DagNode`** / **`DagEdge`**: Adjacency list representation
//! - **`DagChannelType`**: Auto-derived channel types (SPSC/SPMC/MPSC)
//! - **`MulticastBuffer`**: Zero-copy SPMC multicast for shared stages
//! - **`RoutingTable`**: Pre-computed O(1) dispatch table
//! - **`DagExecutor`**: Ring 0 event processing engine
//!
//! ## Key Design Principles
//!
//! 1. **Channel type is auto-derived** - SPSC/SPMC/MPSC inferred from topology
//! 2. **Cycle detection** - Rejected at construction time
//! 3. **Schema validation** - Connected edges must have compatible schemas
//! 4. **Immutable once finalized** - Topology is frozen after `build()`
//! 5. **Zero-alloc hot path** - Multicast and routing are allocation-free
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     RING 2: CONTROL PLANE                       │
//! │  DagBuilder constructs StreamingDag topology                    │
//! │  ┌──────────┐   ┌──────────────┐   ┌───────────────────┐       │
//! │  │DagBuilder│──▶│ StreamingDag │──▶│ RoutingTable      │       │
//! │  │ (Ring 2) │   │  (immutable) │   │ (cache-aligned)   │       │
//! │  └──────────┘   └──────────────┘   └───────────────────┘       │
//! │                                                                 │
//! │  MulticastBuffer<T> per shared stage (pre-allocated slots)      │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use laminar_core::dag::{DagBuilder, RoutingTable};
//!
//! let dag = DagBuilder::new()
//!     .source("trades", schema.clone())
//!     .operator("normalize", schema.clone())
//!     .connect("trades", "normalize")
//!     .fan_out("normalize", |b| {
//!         b.branch("vwap", schema.clone())
//!          .branch("anomaly", schema.clone())
//!     })
//!     .sink_for("vwap", "analytics", schema.clone())
//!     .sink_for("anomaly", "alerts", schema.clone())
//!     .build()?;
//!
//! let routing = RoutingTable::from_dag(&dag);
//! assert_eq!(dag.node_count(), 5);
//! assert_eq!(dag.sources().len(), 1);
//! assert_eq!(dag.sinks().len(), 2);
//! ```

pub mod builder;
pub mod changelog;
pub mod checkpoint;
pub mod error;
pub mod executor;
pub mod multicast;
pub mod recovery;
pub mod routing;
pub mod topology;
pub mod watermark;

#[cfg(test)]
mod tests;

// Re-export key types
pub use builder::{DagBuilder, FanOutBuilder};
pub use changelog::DagChangelogPropagator;
pub use checkpoint::{
    AlignmentResult, BarrierAligner, BarrierType, CheckpointBarrier, CheckpointId,
    DagCheckpointConfig, DagCheckpointCoordinator,
};
pub use error::DagError;
pub use executor::{DagExecutor, DagExecutorMetrics, OperatorNodeMetrics};
pub use multicast::MulticastBuffer;
pub use recovery::{
    DagCheckpointSnapshot, DagRecoveryManager, RecoveredDagState, SerializableOperatorState,
};
pub use routing::{RoutingEntry, RoutingTable, MAX_PORTS};
pub use topology::{
    DagChannelType, DagEdge, DagNode, DagNodeType, EdgeId, NodeId, PartitioningStrategy,
    SharedStageMetadata, StatePartitionId, StreamingDag, MAX_FAN_OUT,
};
pub use watermark::{DagWatermarkCheckpoint, DagWatermarkTracker};
