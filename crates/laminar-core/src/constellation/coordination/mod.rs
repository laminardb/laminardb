//! # Raft Metadata Consensus
//!
//! Implements the Raft-based metadata consensus layer for the constellation.
//! This module manages the distributed state machine that tracks partition
//! ownership, node states, and checkpoint metadata.
//!
//! ## Architecture
//!
//! - [`metadata`]: `ConstellationMetadata` state machine with apply logic
//! - [`raft`]: `RaftCoordinator` wrapping `openraft::Raft`

/// Constellation metadata state machine.
pub mod metadata;

/// Raft coordinator wrapper.
pub mod raft;

/// Top-level orchestration of the constellation lifecycle.
pub mod orchestrator;

pub use metadata::{
    ConstellationMetadata, MetadataLogEntry, PartitionOwnership,
};
pub use raft::{CoordinationError, RaftCoordinator};
pub use orchestrator::ConstellationManager;
