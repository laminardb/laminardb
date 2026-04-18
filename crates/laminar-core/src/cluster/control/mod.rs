//! Cluster control plane: leader election, assignment snapshots, and
//! barrier coordination. See `docs/plans/distributed-stateful-pipelines.md`
//! §7 for the overall design.
//!
//! State is split by lifetime:
//! - **Ephemeral** (barriers, leader identity, assignment version) lives
//!   in chitchat's KV tier for ~100 ms gossip propagation.
//! - **Durable** (completed assignment snapshots) lives on the object
//!   store so full-cluster restart can recover.

pub mod barrier;
pub mod controller;
pub mod leader;
pub mod snapshot;

pub use barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, InMemoryKv, QuorumOutcome,
};
pub use controller::ClusterController;
pub use leader::leader_of;
pub use snapshot::{AssignmentSnapshot, AssignmentSnapshotStore, SnapshotError};

/// Chitchat-backed [`ClusterKv`] — gated on `cluster-unstable`
/// because it pulls in the chitchat runtime.
#[cfg(feature = "cluster-unstable")]
pub mod chitchat_kv;
#[cfg(feature = "cluster-unstable")]
pub use chitchat_kv::ChitchatKv;
