//! Cluster coordination: discovery and control plane.

/// Node discovery and membership.
#[cfg(feature = "cluster")]
pub mod discovery;

/// Control plane: leader election, assignment snapshots, barrier
/// coordination.
#[cfg(feature = "cluster")]
pub mod control;

/// Feature-neutral control-plane value types for non-cluster builds.
#[cfg(not(feature = "cluster"))]
pub mod control {
    pub use crate::catalog::CatalogObjectKind;
    pub use crate::checkpoint::{
        CheckpointAssignmentAdoption, CheckpointAssignmentFence, CheckpointParticipant,
        LeaderProof, LeaderProofOwner,
    };
}

/// In-process harness for cluster integration tests. Gated because
/// it pulls in chitchat on loopback UDP.
#[cfg(feature = "cluster")]
pub mod testing;
