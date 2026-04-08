//! Distributed coordination: discovery, partition ownership, metadata consensus.

/// Node discovery and membership.
pub mod discovery;

/// Raft-based metadata consensus and coordination.
pub mod coordination;

/// Epoch-fenced partition ownership, assignment, and migration.
pub mod partition;
