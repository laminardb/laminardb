//! # Partition Ownership and Migration
//!
//! Epoch-fenced partition ownership, assignment algorithms, and
//! partition migration protocol.
//!
//! ## Modules
//!
//! - [`guard`]: `PartitionGuard` for epoch-fenced access (F-EPOCH-001)
//! - [`assignment`]: Consistent-hash partition assignment (F-EPOCH-002)
//! - [`migration`]: Partition reassignment protocol (F-EPOCH-003)

/// Epoch-fenced partition guards.
pub mod guard;

/// Partition assignment algorithms.
pub mod assignment;

/// Partition migration protocol.
pub mod migration;

pub use guard::{PartitionGuard, PartitionGuardSet};
pub use assignment::{AssignmentPlan, ConsistentHashAssigner, PartitionAssigner};
pub use migration::{MigrationExecutor, MigrationPhase, MigrationTask};
