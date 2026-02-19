//! # Distributed Checkpoint Coordination
//!
//! Coordinates distributed checkpoint operations across the constellation.
//! Implements a two-phase commit protocol: inject barriers and wait for
//! prepare acks, then aggregate manifests and commit.

/// Distributed checkpoint coordinator.
pub mod distributed;

pub use distributed::{
    CheckpointPhase, CheckpointRpc, DistributedCheckpointCoordinator,
};
