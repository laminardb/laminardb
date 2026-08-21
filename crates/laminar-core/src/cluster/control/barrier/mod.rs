//! Cross-instance barrier protocol. Direct gRPC leader-to-follower calls
//! under `cluster`, falling back to gossip-KV announce/ack/poll.

#[cfg(feature = "cluster")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use crate::checkpoint::CheckpointAttempt;
use crate::checkpoint::CheckpointWatermark;
use crate::cluster::discovery::NodeId;
#[cfg(feature = "cluster")]
use crate::cluster::discovery::NodeInfo;
#[cfg(feature = "cluster")]
use tokio::sync::watch;

/// KV key for the leader's barrier announcement.
pub const ANNOUNCEMENT_KEY: &str = "control:barrier";

/// KV key for a follower's barrier ack.
pub const ACK_KEY: &str = "control:barrier-ack";

/// Gossip KV key used by follower barrier servers to advertise their bound address.
#[cfg(feature = "cluster")]
pub const BARRIER_ADDR_KEY: &str = "barrier:addr";

#[cfg(feature = "cluster")]
const BARRIER_ENDPOINT_VERSION: u8 = 1;

#[cfg(feature = "cluster")]
const MAX_BARRIER_ENDPOINT_BYTES: usize = 1_024;

#[derive(Default)]
struct AnnouncementPublicationState {
    initialized: bool,
    latest: Option<BarrierAnnouncement>,
}

/// Cross-instance barrier coordination.
pub struct BarrierCoordinator {
    kv: Arc<dyn ClusterKv>,
    /// Serializes local publication across every runtime mode. The latest admitted value advances
    /// before cancellable I/O so an ambiguous write result cannot reopen an older phase.
    publication: tokio::sync::Mutex<AnnouncementPublicationState>,
    #[cfg(feature = "cluster")]
    grpc: Arc<parking_lot::Mutex<Option<Arc<GrpcState>>>>,
    /// First local observation time for each exact Prepare identity. Direct gRPC receipt is the
    /// preferred clock, while this transport-independent registry gives the gossip fallback the
    /// same non-refreshing attempt deadline across repeated observations.
    #[cfg(feature = "cluster")]
    prepare_observed_at: parking_lot::Mutex<FxHashMap<BarrierIdentity, std::time::Instant>>,
    /// Highest checkpoint ID whose immutable terminal authority closes any retained Prepare at or
    /// below it. Recovery may settle an attempt after its mutable transport hint becomes stale.
    #[cfg(feature = "cluster")]
    settled_prepare_floor: AtomicU64,
    #[cfg(feature = "cluster")]
    leader_election: Arc<parking_lot::Mutex<ActiveLeaderState>>,
    #[cfg(feature = "cluster")]
    leader_lease_store: Arc<parking_lot::Mutex<Option<Arc<super::LeaderLeaseStore>>>>,
    #[cfg(feature = "cluster")]
    local_leader_proof: Arc<parking_lot::Mutex<Option<LocalLeaderProofProvider>>>,
    #[cfg(feature = "cluster")]
    local_process: Arc<std::sync::OnceLock<BarrierProcessIdentity>>,
    #[cfg(feature = "cluster")]
    unbound_endpoint_started: parking_lot::Mutex<bool>,
    #[cfg(feature = "cluster")]
    process_lease_deadline: Arc<std::sync::OnceLock<Arc<super::LeaseDeadline>>>,
}

impl std::fmt::Debug for BarrierCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BarrierCoordinator").finish_non_exhaustive()
    }
}

impl Drop for BarrierCoordinator {
    fn drop(&mut self) {
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().take();
            if let Some(state) = grpc_opt {
                abort_grpc_tasks(&state);
            }
        }
    }
}

mod grpc;
mod kv;
mod prepare;
mod protocol;

#[cfg(feature = "cluster")]
pub(crate) use grpc::barrier_v1;
pub use kv::{ClusterKv, InMemoryKv};
pub use protocol::{BarrierAck, BarrierAckDisposition, BarrierAnnouncement, Phase, QuorumOutcome};

#[cfg(feature = "cluster")]
use grpc::{
    abort_grpc_tasks, ack_disposition_from_wire, assignment_fence_to_wire,
    checkpoint_watermark_from_wire, evict_barrier_client, get_barrier_client,
    leader_proof_ack_matches, leader_proof_to_wire, send_local_phase_notification,
    send_phase_notifications, ActiveLeaderState, BarrierClientPool, BarrierClientResolutionError,
    GrpcBarrierServer, GrpcState, LocalLeaderProofProvider, WireAssignmentFence,
};
use prepare::PrepareFanoutBudget;
#[cfg(feature = "cluster")]
use prepare::{
    canonical_expected_roster, clustered_phase_roster, install_prepare_fanout,
    mark_capture_quorum_reached, preflight_prepare_fanout, prepare_fanout_budget,
    prepare_fanout_plan, require_aligned_quorum, retire_prepare_fanout, BarrierFlavor,
    BarrierIdentity, PeerFailure, PendingPrepareWaiter, PrepareAckState, PrepareFanoutBatch,
    PrepareFanoutState, PrepareWaiterRegistration, MAX_PREPARE_WAITERS_PER_IDENTITY,
    MAX_RETAINED_BARRIER_IDENTITIES, PREPARE_RPC_TIMEOUT,
};
#[cfg(feature = "cluster")]
use protocol::{
    decode_barrier_endpoint, encode_barrier_endpoint, merge_direct_announcement,
    validate_wire_checkpoint_attempt, BarrierProcessIdentity, ExpectedBarrierProcess,
    PHASE_RPC_TIMEOUT, PREPARE_RETRY_INITIAL_BACKOFF, PREPARE_RETRY_MAX_BACKOFF,
};
use protocol::{
    is_terminal_phase, merge_observed_announcement, same_announcement_identity,
    validate_ack_attempt, validate_announcement_attempt, validate_publication_order,
    validate_scanned_announcements,
};

mod coordinator {
    pub(super) mod authority;
    pub(super) mod publication;
    pub(super) mod quorum;
}

#[cfg(test)]
mod tests;
