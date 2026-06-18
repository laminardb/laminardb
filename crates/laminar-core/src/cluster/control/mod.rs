//! Cluster control plane: leader election, assignment snapshots, and
//! barrier coordination.

pub mod barrier;
pub mod catalog_manifest;
pub mod controller;
pub mod leader;
pub mod leader_lease;
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
#[cfg(feature = "cluster")]
pub use leader_lease::lease_currently_grants;
pub use leader_lease::{
    lease_grants_leadership, LeaderLease, LeaderLeaseConfig, LeaderLeaseManager, LeaderLeaseStore,
    LeaseError, LeaseOutcome,
};
pub use snapshot::{AssignmentSnapshot, AssignmentSnapshotStore, RotateOutcome, SnapshotError};

#[cfg(feature = "cluster")]
pub mod chitchat_kv;
#[cfg(feature = "cluster")]
pub use chitchat_kv::ChitchatKv;

#[cfg(feature = "cluster")]
pub mod query;
#[cfg(feature = "cluster")]
pub use query::{
    remote_scan_client, QueryClientPool, QueryHandlerSlot, RemoteBatchStream, RemoteQueryHandler,
};

#[cfg(feature = "cluster")]
pub mod tls;
#[cfg(feature = "cluster")]
pub use tls::{set_cluster_tls, ClusterTls};
