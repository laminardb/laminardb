//! # Partition Ownership and Assignment
//!
//! Epoch-fenced partition ownership and assignment algorithms.
//!
//! ## Modules
//!
//! - `guard`: `PartitionGuard` for epoch-fenced access
//! - `assignment`: Consistent-hash partition assignment

/// Epoch-fenced partition guards.
pub mod guard;

/// Partition assignment algorithms.
pub mod assignment;

pub use assignment::{
    AssignmentConstraints, AssignmentPlan, ConsistentHashAssigner, PartitionAssigner,
};
pub use guard::{PartitionGuard, PartitionGuardSet};
