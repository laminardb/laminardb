use std::time::Duration;

use laminar_core::checkpoint::CheckpointAttempt;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointWatermark, LeaderProof};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum CheckpointPhase {
    Idle,
    PreCommitting,
    Persisting,
    Deciding,
}

impl std::fmt::Display for CheckpointPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => formatter.write_str("Idle"),
            Self::PreCommitting => formatter.write_str("PreCommitting"),
            Self::Persisting => formatter.write_str("Persisting"),
            Self::Deciding => formatter.write_str("Deciding"),
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointFailureDisposition {
    Retryable,
    RequiresRecovery,
}

/// Determines who publishes a prepared successor sink epoch as writable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkEpochPublication {
    /// Startup and direct coordinator APIs have no callback-owned transition guard.
    Immediate,
    /// A spawned pipeline tail publishes only after its terminal result is known successful.
    DeferredToTail,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointResult {
    pub success: bool,
    pub checkpoint_id: u64,
    pub epoch: u64,
    pub duration: Duration,
    pub error: Option<String>,
    pub failure_disposition: Option<CheckpointFailureDisposition>,
}

impl CheckpointResult {
    #[must_use]
    pub fn continuation_error(&self) -> Option<&str> {
        self.success.then_some(self.error.as_deref()).flatten()
    }

    #[must_use]
    pub fn requires_recovery(&self) -> bool {
        !self.success
            && self.failure_disposition == Some(CheckpointFailureDisposition::RequiresRecovery)
    }
}

#[cfg(feature = "cluster")]
pub(crate) type QuorumPeer = laminar_core::cluster::discovery::NodeId;

#[derive(Debug, Clone)]
pub(crate) enum QuorumStage {
    RunInline,
    #[cfg(feature = "cluster")]
    Captured {
        cluster_watermark: CheckpointWatermark,
        participants: Vec<QuorumPeer>,
        leader_proof: LeaderProof,
    },
}

/// Follower-local durability state after immutable capture ownership has been acknowledged.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FollowerPrepareOutcome {
    /// Manifest persistence completed with an acknowledgement.
    Prepared,
    /// Manifest Create may be visible even though its acknowledgement was lost. The captured
    /// phase-one state must remain intact until an authoritative Commit or Abort is observed.
    InDoubt,
}

#[cfg(feature = "cluster")]
pub(crate) struct PrepareQuorum<'a> {
    pub(super) attempt: CheckpointAttempt,
    pub(super) local_watermark: CheckpointWatermark,
    pub(super) assignment_fence: &'a CheckpointAssignmentFence,
    pub(super) leader_proof: &'a LeaderProof,
    pub(super) flags: u64,
}

#[cfg(feature = "cluster")]
impl<'a> PrepareQuorum<'a> {
    pub(crate) const fn new(
        attempt: CheckpointAttempt,
        local_watermark: CheckpointWatermark,
        assignment_fence: &'a CheckpointAssignmentFence,
        leader_proof: &'a LeaderProof,
        flags: u64,
    ) -> Self {
        Self {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            flags,
        }
    }
}
