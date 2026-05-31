//! Cluster control plane: leader election, assignment snapshots, and
//! barrier coordination.

pub mod barrier;
pub mod catalog_manifest;
pub mod controller;
pub mod leader;
pub mod snapshot;

pub use barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, InMemoryKv, Phase,
    QuorumOutcome, ACK_KEY, ANNOUNCEMENT_KEY,
};
pub use controller::ClusterController;
// Re-exported from `crate::checkpoint_decision` (lives outside the
// cluster gate because single-instance also relies on it for crash-safe
// 2PC). Callers that already qualify with `cluster::control::…` keep
// working.
pub use crate::checkpoint_decision::{CheckpointDecisionStore, DecisionError};
pub use catalog_manifest::{
    CatalogManifest, CatalogManifestEntry, CatalogManifestError, CatalogManifestStore,
};
pub use leader::leader_of;
pub use snapshot::{AssignmentSnapshot, AssignmentSnapshotStore, RotateOutcome, SnapshotError};

#[cfg(feature = "cluster-unstable")]
pub mod chitchat_kv;
#[cfg(feature = "cluster-unstable")]
pub use chitchat_kv::ChitchatKv;
