//! Cluster control plane: leader election, assignment snapshots, and
//! barrier coordination.

pub mod barrier;
pub mod controller;
pub mod decision;
pub mod leader;
pub mod snapshot;

pub use barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, InMemoryKv, Phase,
    QuorumOutcome, ACK_KEY, ANNOUNCEMENT_KEY,
};
pub use controller::ClusterController;
pub use decision::{CheckpointDecisionStore, Decision, DecisionError, RecordOutcome};
pub use leader::leader_of;
pub use snapshot::{
    AssignmentSnapshot, AssignmentSnapshotStore, RotateOutcome, SnapshotError,
};

#[cfg(feature = "cluster-unstable")]
pub mod chitchat_kv;
#[cfg(feature = "cluster-unstable")]
pub use chitchat_kv::ChitchatKv;
