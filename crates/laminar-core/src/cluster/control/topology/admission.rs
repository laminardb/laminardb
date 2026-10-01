//! Evidence for a reserved, pre-cut topology operation. No transition here commits a graph.

use serde::{Deserialize, Serialize};

use super::{TopologyError, TopologyOperationId, TopologyVersion, TOPOLOGY_PROTOCOL_VERSION};
use crate::checkpoint::{CheckpointAssignmentFence, LeaderProof};
use crate::cluster::control::CatalogManifestRef;

/// Maximum retained request identities until topology journal retention is implemented.
pub const MAX_TOPOLOGY_OPERATIONS: usize = 64;
pub(crate) const MAX_TOPOLOGY_PLAN_BYTES: usize = 32 * 1024;

/// Exact request and assignment evidence reserved before checkpoint cutover.
///
/// Admission does not certify the candidate's operator/connector compatibility or authorize its
/// execution. The DB planner must supply those certificates before any future cut transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyAdmissionPlan {
    /// Protocol supported by the coordinator.
    pub protocol_version: u16,
    /// Caller identity, reused on retries.
    pub operation_id: TopologyOperationId,
    /// Expected committed logical parent.
    pub expected_parent: TopologyVersion,
    /// Exact unchanged predecessor inventory.
    pub parent_manifest: CatalogManifestRef,
    /// Complete candidate inventory, staged without changing the active catalog.
    pub target_manifest: CatalogManifestRef,
    /// Owner-complete map and process roster; never a reachable-node majority.
    pub assignment: CheckpointAssignmentFence,
}

impl TopologyAdmissionPlan {
    pub(crate) fn validate(&self) -> Result<(), TopologyError> {
        if self.protocol_version != TOPOLOGY_PROTOCOL_VERSION {
            return Err(TopologyError::Protocol(
                "unsupported admission protocol".into(),
            ));
        }
        self.expected_parent.successor()?;
        self.parent_manifest.validate()?;
        self.target_manifest.validate()?;
        if !self.assignment.is_canonical() || self.parent_manifest == self.target_manifest {
            return Err(TopologyError::Invalid(
                "admission requires an exact assignment and a changed candidate inventory".into(),
            ));
        }
        Ok(())
    }
}

/// Definitive disposition of an operation that has not started cutover.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyAbortReason {
    /// Explicit coordinator cancellation before quiescence.
    Requested,
    /// Current leader term changed before cutover.
    LeaderChanged,
    /// Recovery was durably requested before cutover.
    Recovery,
}

/// Implemented pre-cut states. There is deliberately no commit/activation variant yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum TopologyAdmissionPhase {
    /// Durable reservation; active graph and catalog still belong to the parent.
    Planned,
    /// Definitive pre-cut abort; no candidate actors were authorized.
    Aborted {
        /// Durable reason.
        reason: TopologyAbortReason,
    },
}

/// Small immutable plan reference carried by authority renewals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyPlanRef {
    /// SHA-256 of canonical request evidence, including its operation identity.
    pub sha256: String,
    /// Exact encoded length.
    pub encoded_len: u64,
}

impl TopologyPlanRef {
    pub(crate) fn validate(&self) -> Result<(), TopologyError> {
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.encoded_len == 0
            || self.encoded_len > MAX_TOPOLOGY_PLAN_BYTES as u64
        {
            return Err(TopologyError::Invalid(
                "invalid topology plan reference".into(),
            ));
        }
        Ok(())
    }
}

/// Durable status of one payload-bound request. A reserved target is never a committed catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyAdmissionStatus {
    /// Original request identity.
    pub operation_id: TopologyOperationId,
    /// Exact immutable request payload.
    pub plan: TopologyPlanRef,
    /// Leader term that won admission.
    pub admitted_by: LeaderProof,
    /// Authority append that won admission.
    pub admitted_sequence: u64,
    /// Exact append of the current disposition.
    pub status_sequence: u64,
    /// Definitive pre-cut phase.
    #[serde(rename = "state")]
    pub phase: TopologyAdmissionPhase,
}

impl TopologyAdmissionStatus {
    pub(crate) fn is_planned(&self) -> bool {
        self.phase == TopologyAdmissionPhase::Planned
    }

    pub(crate) fn validate(&self, head: u64) -> Result<(), TopologyError> {
        self.plan.validate()?;
        if !self.admitted_by.is_canonical()
            || self.admitted_sequence == 0
            || self.status_sequence < self.admitted_sequence
            || self.status_sequence > head
            || (self.is_planned() && self.status_sequence != self.admitted_sequence)
            || (!self.is_planned() && self.status_sequence == self.admitted_sequence)
        {
            return Err(TopologyError::Invalid(
                "invalid topology admission status".into(),
            ));
        }
        Ok(())
    }
}
