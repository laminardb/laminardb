//! Immutable request, held parent cut and irreversible target catalog decision.

use serde::{Deserialize, Serialize};

use super::{TopologyError, TopologyOperationId, TopologyVersion, TOPOLOGY_PROTOCOL_VERSION};
use crate::checkpoint::{
    CheckpointAssignmentFence, CheckpointParticipant, CommittedCheckpointRef, LeaderProof,
};
use crate::checkpoint_decision::CheckpointArtifactInventory;
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
    /// Exact canonical DB candidate report, required by preparation protocol two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<super::TopologyCompatibilityRef>,
}

impl TopologyAdmissionPlan {
    pub(crate) fn validate(&self) -> Result<(), TopologyError> {
        if !matches!(
            self.protocol_version,
            TOPOLOGY_PROTOCOL_VERSION
                | super::TOPOLOGY_PREPARATION_PROTOCOL_VERSION
                | super::TOPOLOGY_SUBMISSION_PROTOCOL_VERSION
        ) {
            return Err(TopologyError::Protocol(
                "unsupported admission protocol".into(),
            ));
        }
        if self.protocol_version != TOPOLOGY_PROTOCOL_VERSION {
            self.compatibility
                .as_ref()
                .ok_or_else(|| {
                    TopologyError::Protocol("preparation requires a candidate descriptor".into())
                })?
                .validate()?;
        } else if self.compatibility.is_some() {
            return Err(TopologyError::Protocol(
                "legacy admission cannot carry preparation evidence".into(),
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
    /// The bound old-topology checkpoint received a definitive Abort.
    CheckpointAborted,
}

/// Old-topology preparation states. No state here authorizes the candidate graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum TopologyAdmissionPhase {
    /// Durable reservation; active graph and catalog still belong to the parent.
    Planned,
    /// Participants are durably certifying the exact candidate; intake remains open.
    Preparing,
    /// One exact old-topology attempt is admitted; its barrier and sink settlement are pending.
    Quiescing,
    /// Every frozen process has applied the exact Commit and held intake and sink succession.
    /// The leader's receipt also certifies globally aggregated external sink settlement.
    CutPrepared,
    /// Catalog and exact migration root are committed. Target installation/Release remain pending.
    /// Leader changes and recovery faults must preserve this decision; it cannot abort.
    Committed,
    /// Exact committed runtime installation receipts are being collected with intake held.
    Activating,
    /// Participant-complete Release is committed. Local activation remains separately observed.
    Active,
    /// Definitive pre-target-commit abort; no candidate actors were authorized.
    Aborted {
        /// Durable reason.
        reason: TopologyAbortReason,
    },
}

/// Exact old-topology Commit retained while a cut is prepared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyCutCommit {
    /// Complete source/channel/state/output cut; never a scalar source offset.
    pub checkpoint: CommittedCheckpointRef,
    /// Immutable shared authority append of the definitive Commit.
    pub authority_sequence: u64,
}

/// Binding installed atomically with artifact admission, before any source barrier is injected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyCheckpointCut {
    /// Exact deployment, old pipeline ABI, attempt and owner-complete process roster.
    pub inventory: CheckpointArtifactInventory,
    /// Shared append that bound this attempt and admitted its artifacts.
    pub bound_sequence: u64,
    /// A Commit alone does not prove that the leader finished external sink settlement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed: Option<TopologyCutCommit>,
    /// Sorted processes whose runtime-owned tails finished applying the cut.
    /// The leader reports after external sink settlement; followers finish their local checkpoint.
    /// Their intake remains held; these receipts do not prove actor retirement or target readiness.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completed_participants: Vec<CheckpointParticipant>,
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

/// Durable status of one payload-bound request, including its optional target catalog decision.
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
    /// Definitive migration phase; Commit does not authorize target activation.
    #[serde(rename = "state")]
    pub phase: TopologyAdmissionPhase,
    /// Exact old-topology cut, absent until barrier preparation is authorized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cut: Option<TopologyCheckpointCut>,
    /// Immutable descriptor and monotonic exact-process compatibility certificates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation: Option<super::TopologyPreparation>,
    /// Exact-cut restore/initialization requirements. This is never a target Commit or Release.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_root: Option<super::TopologyMigrationRootBinding>,
    /// Historical exact-process target restore and parent retirement observations.
    /// These receipts do not authorize installation, output or intake release.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_preparations: Vec<super::TopologyTargetPreparationReceipt>,
    /// Irreversible target catalog decision; absent throughout preparation and pre-commit abort.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<super::TopologyCommit>,
    /// Current exact-runtime installation round and immutable participant-complete Release.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<super::TopologyActivation>,
}

impl TopologyAdmissionStatus {
    pub(crate) fn is_planned(&self) -> bool {
        self.phase == TopologyAdmissionPhase::Planned
    }

    pub(crate) fn is_preparing(&self) -> bool {
        matches!(
            self.phase,
            TopologyAdmissionPhase::Planned
                | TopologyAdmissionPhase::Preparing
                | TopologyAdmissionPhase::Quiescing
                | TopologyAdmissionPhase::CutPrepared
        )
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
        self.validate_preparation(head)?;
        self.validate_target_preparations(head)?;
        self.validate_commit(head)?;
        self.validate_activation()?;
        if let Some(binding) = &self.migration_root {
            binding.root.validate()?;
            if !matches!(
                self.phase,
                TopologyAdmissionPhase::CutPrepared
                    | TopologyAdmissionPhase::Committed
                    | TopologyAdmissionPhase::Activating
                    | TopologyAdmissionPhase::Active
                    | TopologyAdmissionPhase::Aborted { .. }
            ) || self
                .preparation
                .as_ref()
                .is_none_or(|p| p.complete_sequence.is_none())
                || self.cut.as_ref().is_none_or(|cut| {
                    cut.committed.as_ref().is_none_or(|commit| {
                        binding.authority_sequence <= commit.authority_sequence
                    }) || cut
                        .inventory
                        .assignment_fence
                        .as_ref()
                        .is_none_or(|fence| cut.completed_participants != fence.participants)
                })
                || binding.authority_sequence > self.status_sequence
                || binding.authority_sequence > head
            {
                return Err(TopologyError::Invalid(
                    "migration root requires the complete certified prepared cut".into(),
                ));
            }
        }
        match (&self.cut, self.phase) {
            (
                None,
                TopologyAdmissionPhase::Planned
                | TopologyAdmissionPhase::Preparing
                | TopologyAdmissionPhase::Aborted { .. },
            ) => {}
            (
                Some(cut),
                TopologyAdmissionPhase::Quiescing
                | TopologyAdmissionPhase::CutPrepared
                | TopologyAdmissionPhase::Committed
                | TopologyAdmissionPhase::Activating
                | TopologyAdmissionPhase::Active
                | TopologyAdmissionPhase::Aborted { .. },
            ) => {
                cut.inventory.validate().map_err(TopologyError::Invalid)?;
                let fence = cut.inventory.assignment_fence.as_ref().ok_or_else(|| {
                    TopologyError::Invalid("topology cut requires an assignment fence".into())
                })?;
                if cut.bound_sequence <= self.admitted_sequence
                    || cut.bound_sequence > self.status_sequence
                    || fence.participant_incarnation(self.admitted_by.owner.node_id)
                        != Some(self.admitted_by.owner.boot_id)
                    || !cut
                        .completed_participants
                        .windows(2)
                        .all(|pair| pair[0].node_id < pair[1].node_id)
                    || cut.completed_participants.iter().any(|participant| {
                        fence.participant_incarnation(participant.node_id)
                            != Some(participant.boot_incarnation)
                    })
                {
                    return Err(TopologyError::Invalid(
                        "invalid topology checkpoint binding or completion roster".into(),
                    ));
                }
                if let Some(commit) = &cut.committed {
                    commit
                        .checkpoint
                        .validate()
                        .map_err(TopologyError::Invalid)?;
                    if commit.checkpoint.epoch != cut.inventory.attempt.epoch
                        || commit.checkpoint.checkpoint_id != cut.inventory.attempt.checkpoint_id
                        || commit.authority_sequence <= cut.bound_sequence
                        || commit.authority_sequence > self.status_sequence
                    {
                        return Err(TopologyError::Invalid(
                            "topology cut Commit differs from its bound attempt".into(),
                        ));
                    }
                } else if !cut.completed_participants.is_empty() {
                    return Err(TopologyError::Invalid(
                        "uncommitted cut cannot have completion receipts".into(),
                    ));
                }
                if matches!(
                    self.phase,
                    TopologyAdmissionPhase::CutPrepared
                        | TopologyAdmissionPhase::Committed
                        | TopologyAdmissionPhase::Activating
                        | TopologyAdmissionPhase::Active
                ) && (cut.committed.is_none()
                    || cut.completed_participants != fence.participants)
                {
                    return Err(TopologyError::Invalid(
                        "prepared cut requires every exact process completion".into(),
                    ));
                }
                if self.phase == TopologyAdmissionPhase::Quiescing
                    && cut.completed_participants == fence.participants
                {
                    return Err(TopologyError::Invalid(
                        "every process completion must advance the cut to prepared".into(),
                    ));
                }
            }
            _ => {
                return Err(TopologyError::Invalid(
                    "topology phase and checkpoint binding disagree".into(),
                ))
            }
        }
        Ok(())
    }

    pub(crate) fn validate_successor(
        &self,
        after: &Self,
        sequence: u64,
    ) -> Result<(), TopologyError> {
        if self.operation_id != after.operation_id
            || self.plan != after.plan
            || self.admitted_sequence != after.admitted_sequence
            || self.admitted_by != after.admitted_by
            || (!self.is_preparing() && !self.has_target_commit() && self != after)
            || (self != after && (after.is_planned() || after.status_sequence != sequence))
        {
            return Err(TopologyError::Invalid(
                "authority cannot rewrite a topology request or its terminal result".into(),
            ));
        }
        self.validate_preparation_successor(after, sequence)?;
        self.validate_target_preparation_successor(after, sequence)?;
        self.validate_commit_successor(after, sequence)?;
        self.validate_activation_successor(after, sequence)?;
        match (&self.migration_root, &after.migration_root) {
            (None, None) => {}
            (Some(prior), Some(next)) if prior == next => {}
            (None, Some(binding))
                if self.phase == TopologyAdmissionPhase::CutPrepared
                    && after.phase == TopologyAdmissionPhase::CutPrepared
                    && binding.authority_sequence == sequence
                    && self.cut == after.cut
                    && self.preparation == after.preparation => {}
            _ => {
                return Err(TopologyError::Invalid(
                    "authority cannot replace a migration root or stage it outside a prepared cut"
                        .into(),
                ))
            }
        }
        if let Some(prior_cut) = &self.cut {
            let next_cut = after.cut.as_ref().ok_or_else(|| {
                TopologyError::Invalid("authority cannot forget a topology cut".into())
            })?;
            if prior_cut.inventory != next_cut.inventory
                || prior_cut.bound_sequence != next_cut.bound_sequence
                || prior_cut
                    .committed
                    .as_ref()
                    .is_some_and(|commit| next_cut.committed.as_ref() != Some(commit))
                || prior_cut
                    .completed_participants
                    .iter()
                    .any(|participant| !next_cut.completed_participants.contains(participant))
                || (self.phase == TopologyAdmissionPhase::CutPrepared
                    && self != after
                    && !matches!(after.phase, TopologyAdmissionPhase::Aborted { .. })
                    && !(after.phase == TopologyAdmissionPhase::CutPrepared
                        && self.migration_root.is_none()
                        && after.migration_root.is_some()
                        && prior_cut == next_cut
                        && self.preparation == after.preparation)
                    && !(self.same_restore_binding(after)
                        && after.target_preparations.len() == self.target_preparations.len() + 1)
                    && !(after.phase == TopologyAdmissionPhase::Committed
                        && prior_cut == next_cut
                        && self.preparation == after.preparation
                        && self.migration_root == after.migration_root
                        && self.target_preparations == after.target_preparations))
            {
                return Err(TopologyError::Invalid(
                    "authority cannot replace a cut or rewind its evidence".into(),
                ));
            }
        } else if let Some(cut) = &after.cut {
            if !matches!(
                self.phase,
                TopologyAdmissionPhase::Planned | TopologyAdmissionPhase::Preparing
            ) || after.phase != TopologyAdmissionPhase::Quiescing
                || cut.bound_sequence != sequence
                || cut.committed.is_some()
                || !cut.completed_participants.is_empty()
            {
                return Err(TopologyError::Invalid(
                    "new cut must bind its exact artifact admission append".into(),
                ));
            }
        }
        Ok(())
    }
}
