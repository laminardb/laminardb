//! # Partition Ownership
//!
//! Epoch-fenced partition ownership. The `ConsistentHashAssigner` that
//! used to live here was removed in Phase A of the distributed-pipelines
//! plan — real assignment ships via Kafka consumer group + per-connector
//! dispatch in Phase C.

/// Epoch-fenced partition guards.
pub mod guard;

pub use guard::{PartitionGuard, PartitionGuardSet};
