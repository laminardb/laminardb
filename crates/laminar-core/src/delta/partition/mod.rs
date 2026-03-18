//! # Partition Ownership and Assignment
//!
//! Epoch-fenced partition ownership and assignment algorithms.
//!
//! ## Modules
//!
//! - `guard`: `PartitionGuard` for epoch-fenced access
//! - `assignment`: Consistent-hash partition assignment
//! - `migration`: Partition migration protocol

/// Epoch-fenced partition guards.
pub mod guard;

/// Partition assignment algorithms.
pub mod assignment;

/// Partition migration protocol for cluster rebalancing.
pub mod migration;

pub use assignment::{AssignmentConstraints, AssignmentPlan, ConsistentHashAssigner};
pub use guard::{PartitionGuard, PartitionGuardSet};
pub use migration::{MigrationCoordinator, MigrationError, MigrationPhase, PartitionSnapshot};
