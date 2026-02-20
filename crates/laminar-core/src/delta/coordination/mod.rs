//! # Raft Metadata Consensus
//!
//! Implements the Raft-based metadata consensus layer for the delta.
//! This module manages the distributed state machine that tracks partition
//! ownership, node states, and checkpoint metadata.
//!
//! ## Architecture
//!
//! - `metadata`: `DeltaMetadata` state machine with apply logic
//! - `raft`: `RaftCoordinator` wrapping `openraft::Raft`

/// Delta metadata state machine.
pub mod metadata;

/// Raft coordinator wrapper.
pub mod raft;

/// Top-level orchestration of the delta lifecycle.
pub mod orchestrator;

pub use metadata::{DeltaMetadata, MetadataLogEntry, PartitionOwnership};
pub use orchestrator::DeltaManager;
pub use raft::{CoordinationError, RaftCoordinator};
