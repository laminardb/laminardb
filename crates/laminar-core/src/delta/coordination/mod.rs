//! # Delta Coordination
//!
//! Implements the metadata and orchestration layer for the delta.
//! This module manages the state machine that tracks partition
//! ownership, node states, and checkpoint metadata.
//!
//! ## Architecture
//!
//! - `metadata`: `DeltaMetadata` state machine with apply logic
//! - `orchestrator`: `DeltaManager` lifecycle orchestration
//!
//! Note: Raft-based consensus (openraft) is planned for Phase 6c.
//! The `raft` module was removed in the 2026-03-07 production audit
//! because it contained only a stub struct with no openraft integration.

/// Delta metadata state machine.
pub mod metadata;

/// Top-level orchestration of the delta lifecycle.
pub mod orchestrator;

pub use metadata::{DeltaMetadata, MetadataLogEntry, PartitionOwnership};
pub use orchestrator::{DeltaManager, NodeLifecyclePhase};
