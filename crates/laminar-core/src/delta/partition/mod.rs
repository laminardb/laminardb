//! # Partition Ownership and Migration
//!
//! Epoch-fenced partition ownership, assignment algorithms, and
//! partition migration protocol.
//!
//! ## Modules
//!
//! - `guard`: `PartitionGuard` for epoch-fenced access
//! - `assignment`: Consistent-hash partition assignment
//! - `migration`: Partition reassignment protocol

/// Epoch-fenced partition guards.
pub mod guard;

/// Partition assignment algorithms.
pub mod assignment;

/// Partition migration protocol.
pub mod migration;

pub use assignment::{AssignmentPlan, ConsistentHashAssigner, PartitionAssigner};
pub use guard::{PartitionGuard, PartitionGuardSet};
pub use migration::{MigrationExecutor, MigrationPhase, MigrationTask};
