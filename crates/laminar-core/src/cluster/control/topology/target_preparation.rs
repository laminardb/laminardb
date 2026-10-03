//! Historical target preparation observations under the still-held parent cut.

use serde::{Deserialize, Serialize};

use super::{TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyError};
use crate::checkpoint::{CheckpointParticipant, MAX_CHECKPOINT_PARTICIPANTS};

/// Every reporting process must implement target restore and observed parent retirement.
pub const TOPOLOGY_TARGET_PREPARATION_PROTOCOL_VERSION: u16 = 3;

/// One process restored the bound target and observed all its parent actors terminal.
/// The operation's immutable plan/root/cut bind the observation. It is historical evidence,
/// not proof that an image is still resident, receivers/sinks are installed or intake may open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyTargetPreparationReceipt {
    /// Exact frozen owner/evidence participant, including its boot incarnation.
    pub participant: CheckpointParticipant,
    /// Exact process term from its original candidate certificate.
    pub process_term: u64,
    /// Exact target preparation protocol understood by this process.
    pub protocol_version: u16,
    /// First immutable shared authority append carrying this observation.
    pub authority_sequence: u64,
}

impl TopologyAdmissionStatus {
    /// Whether the still-prepared operation has observations from every frozen participant.
    /// This does not authorize a target Commit, installation or Release. Commit must revalidate
    /// current processes and assignment; installation must restore/retain its own target image.
    #[must_use]
    pub fn target_preparation_complete(&self) -> bool {
        self.phase == TopologyAdmissionPhase::CutPrepared
            && self.target_preparation_roster_complete()
    }

    pub(crate) fn target_preparation_roster_complete(&self) -> bool {
        self.migration_root.is_some()
            && self.cut.as_ref().is_some_and(|cut| {
                cut.inventory
                    .assignment_fence
                    .as_ref()
                    .is_some_and(|fence| {
                        self.target_preparations.len() == fence.participants.len()
                            && self
                                .target_preparations
                                .iter()
                                .zip(&fence.participants)
                                .all(|(receipt, participant)| receipt.participant == *participant)
                    })
            })
    }

    // Only receipt/status progress can change while an image is retained. Every field defining
    // its restore authority remains exact. Destructuring forces future fields to be considered.
    pub(crate) fn same_restore_binding(&self, other: &Self) -> bool {
        (self.phase == other.phase || (self.has_target_commit() && other.has_target_commit()))
            && self.commit == other.commit
            && self.same_migration_binding(other)
    }

    // A Commit preserves these historical requirements while changing phase/catalog authority.
    pub(crate) fn same_migration_binding(&self, other: &Self) -> bool {
        let Self {
            operation_id,
            plan,
            admitted_by,
            admitted_sequence,
            status_sequence: _,
            phase: _,
            cut,
            preparation,
            migration_root,
            target_preparations: _,
            commit: _,
            activation: _,
        } = self;
        *operation_id == other.operation_id
            && *plan == other.plan
            && *admitted_by == other.admitted_by
            && *admitted_sequence == other.admitted_sequence
            && *cut == other.cut
            && *preparation == other.preparation
            && *migration_root == other.migration_root
    }

    pub(crate) fn validate_target_preparations(&self, head: u64) -> Result<(), TopologyError> {
        if self.target_preparations.is_empty() {
            return Ok(());
        }
        let root = self.migration_root.as_ref().ok_or_else(|| {
            TopologyError::Invalid("target preparation requires a published root".into())
        })?;
        let preparation = self.preparation.as_ref().ok_or_else(|| {
            TopologyError::Invalid("target preparation requires candidate certificates".into())
        })?;
        if !matches!(
            self.phase,
            TopologyAdmissionPhase::CutPrepared
                | TopologyAdmissionPhase::Committed
                | TopologyAdmissionPhase::Activating
                | TopologyAdmissionPhase::Active
                | TopologyAdmissionPhase::Aborted { .. }
        ) || self.target_preparations.len() > MAX_CHECKPOINT_PARTICIPANTS
            || !self
                .target_preparations
                .windows(2)
                .all(|pair| pair[0].participant.node_id < pair[1].participant.node_id)
            || self.target_preparations.iter().any(|receipt| {
                !matches!(
                    receipt.protocol_version,
                    TOPOLOGY_TARGET_PREPARATION_PROTOCOL_VERSION
                        | super::TOPOLOGY_COMMIT_PROTOCOL_VERSION
                ) || receipt.authority_sequence <= root.authority_sequence
                    || receipt.authority_sequence > self.status_sequence
                    || receipt.authority_sequence > head
                    || !preparation.certificates.iter().any(|certificate| {
                        certificate.participant == receipt.participant
                            && certificate.process_term == receipt.process_term
                    })
            })
        {
            return Err(TopologyError::Invalid(
                "invalid target preparation observations".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_target_preparation_successor(
        &self,
        after: &Self,
        sequence: u64,
    ) -> Result<(), TopologyError> {
        if self.target_preparations == after.target_preparations {
            return Ok(());
        }
        after.validate_target_preparations(sequence)?;
        if self.phase != TopologyAdmissionPhase::CutPrepared
            || self.migration_root.is_none()
            || !self.same_restore_binding(after)
            || after.target_preparations.len() != self.target_preparations.len() + 1
            || self
                .target_preparations
                .iter()
                .any(|receipt| !after.target_preparations.contains(receipt))
            || after
                .target_preparations
                .iter()
                .filter(|receipt| !self.target_preparations.contains(receipt))
                .any(|receipt| receipt.authority_sequence != sequence)
        {
            return Err(TopologyError::Invalid(
                "authority cannot rewrite, forget or append stale target preparation observations"
                    .into(),
            ));
        }
        Ok(())
    }
}
