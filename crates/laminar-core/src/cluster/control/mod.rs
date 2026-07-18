//! Cluster control plane: leader election, assignment snapshots, and
//! barrier coordination.

pub mod barrier;
pub mod catalog_manifest;
pub mod controller;
pub mod leader;
pub mod leader_lease;
mod lease_deadline;
pub mod process_lease;
pub mod snapshot;

pub use crate::checkpoint::{
    CheckpointAssignmentAdoption, CheckpointAssignmentFence, CheckpointParticipant, LeaderProof,
    LeaderProofOwner,
};
pub use barrier::{
    BarrierAck, BarrierAnnouncement, BarrierCoordinator, ClusterKv, InMemoryKv, Phase,
    QuorumOutcome, ACK_KEY, ANNOUNCEMENT_KEY,
};
#[cfg(feature = "cluster")]
pub use controller::CheckpointPrepareObservation;
pub use controller::{
    ClusterController, RecoverPhase, RecoveryAnnouncement, RecoveryControlError, RecoveryFault,
    RecoveryFaultReportOutcome, RecoveryFaultRequest, RecoveryRound, RecoveryRoundId,
    RecoveryStoppedReport, ReleaseCommitStatus,
};
// Re-exported from `crate::checkpoint_decision` (lives outside the
// cluster gate because single-instance also relies on it for crash-safe
// 2PC). Callers that already qualify with `cluster::control::…` keep
// working.
pub use crate::checkpoint_decision::{CheckpointDecisionStore, DecisionError};
pub use catalog_manifest::{
    CatalogManifest, CatalogManifestEntry, CatalogManifestError, CatalogManifestRef,
    CatalogManifestStore, CatalogObjectKind, CatalogSealOutcome,
};
pub use leader::leader_of;
pub use leader_lease::{
    lease_grants_leadership, lease_grants_proof, AssignmentDrainDecision, AssignmentDrainVerdict,
    AssignmentRecoveryDecision, ClusterCheckpointAuthorityError, ClusterOutcomeRetentionBoundary,
    LeaderCandidacy, LeaderLease, LeaderLeaseConfig, LeaderLeaseManager, LeaderLeaseObservation,
    LeaderLeaseOwner, LeaderLeaseStore, LeaseError, LeaseOutcome,
    RecordAssignmentDrainDecisionResult, RecordAssignmentRecoveryDecisionResult,
};
pub use lease_deadline::LeaseDeadline;
pub use process_lease::{
    ProcessLease, ProcessLeaseAuthority, ProcessLeaseConfig, ProcessLeaseError, ProcessLeaseFence,
    ProcessLeaseManager, ProcessLeaseObservation, ProcessLeaseOutcome, ProcessLeaseStore,
};
pub use snapshot::{
    AssignmentSnapshot, AssignmentSnapshotRef, AssignmentSnapshotStore, RotateOutcome,
    SnapshotError,
};

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
pub use tls::{claim_cluster_plaintext, set_cluster_tls, ClusterTls};
