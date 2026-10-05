//! Monotonic compatibility evidence; neither readiness nor authority for target actors.

use super::{
    TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyCompatibilityRef, TopologyError,
};
use crate::checkpoint::{CheckpointParticipant, MAX_CHECKPOINT_PARTICIPANTS};
use serde::{Deserialize, Serialize};

/// Candidate preparation requires this exact protocol on every frozen process.
pub const TOPOLOGY_PREPARATION_PROTOCOL_VERSION: u16 = 2;

/// Complete public migration support, including exact-target installation and recovery.
/// Every frozen process must certify this plan protocol before the parent cut begins.
pub const TOPOLOGY_SUBMISSION_PROTOCOL_VERSION: u16 = 6;

/// One process's independently compiled candidate, bound to its first immutable authority append.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyParticipantCertificate {
    /// Exact frozen node slot and boot incarnation, including required evidence participants.
    pub participant: CheckpointParticipant,
    /// Stable-node process term sampled around compilation/publication by its controller.
    pub process_term: u64,
    /// Exact supported preparation protocol; advertisements alone are insufficient.
    pub protocol_version: u16,
    /// Shared append that first recorded this certificate.
    pub authority_sequence: u64,
}

/// Candidate binding retained by admission, renewals, cut preparation and definitive abort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyPreparation {
    /// Canonical compiled descriptor bound by the immutable admission plan.
    pub compatibility: TopologyCompatibilityRef,
    /// Sorted exact-process certificates, never a majority of reachable processes.
    pub certificates: Vec<TopologyParticipantCertificate>,
    /// The final certificate append, only after the complete frozen roster agrees.
    pub complete_sequence: Option<u64>,
}

impl TopologyAdmissionStatus {
    pub(crate) fn validate_preparation(&self, head: u64) -> Result<(), TopologyError> {
        let Some(preparation) = &self.preparation else {
            return if self.phase == TopologyAdmissionPhase::Preparing {
                Err(TopologyError::Invalid(
                    "preparing operation has no descriptor".into(),
                ))
            } else {
                Ok(())
            };
        };
        preparation.compatibility.validate()?;
        if preparation.certificates.len() > MAX_CHECKPOINT_PARTICIPANTS
            || !preparation
                .certificates
                .windows(2)
                .all(|pair| pair[0].participant.node_id < pair[1].participant.node_id)
            || preparation.certificates.iter().any(|certificate| {
                certificate.participant.node_id == 0
                    || certificate.participant.boot_incarnation.is_nil()
                    || certificate.process_term == 0
                    || !matches!(
                        certificate.protocol_version,
                        TOPOLOGY_PREPARATION_PROTOCOL_VERSION
                            | TOPOLOGY_SUBMISSION_PROTOCOL_VERSION
                    )
                    || certificate.authority_sequence <= self.admitted_sequence
                    || certificate.authority_sequence > self.status_sequence
                    || certificate.authority_sequence > head
            })
            || !preparation
                .certificates
                .windows(2)
                .all(|pair| pair[0].protocol_version == pair[1].protocol_version)
            || preparation.complete_sequence.is_some_and(|sequence| {
                preparation
                    .certificates
                    .iter()
                    .map(|cert| cert.authority_sequence)
                    .max()
                    != Some(sequence)
            })
            || (self.is_planned()
                && (!preparation.certificates.is_empty()
                    || preparation.complete_sequence.is_some()))
            || (self.phase == TopologyAdmissionPhase::Preparing
                && preparation.certificates.is_empty())
            || (self.cut.is_some()
                && preparation.complete_sequence.is_none_or(|sequence| {
                    sequence >= self.cut.as_ref().map_or(0, |cut| cut.bound_sequence)
                }))
        {
            return Err(TopologyError::Invalid(
                "invalid participant preparation evidence".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_preparation_successor(
        &self,
        after: &Self,
        sequence: u64,
    ) -> Result<(), TopologyError> {
        match (&self.preparation, &after.preparation) {
            (None, None) => Ok(()),
            (Some(prior), Some(next)) if prior.compatibility == next.compatibility => {
                if prior
                    .certificates
                    .iter()
                    .any(|cert| !next.certificates.contains(cert))
                    || prior
                        .complete_sequence
                        .is_some_and(|complete| next.complete_sequence != Some(complete))
                    || (prior != next
                        && (!matches!(
                            self.phase,
                            TopologyAdmissionPhase::Planned | TopologyAdmissionPhase::Preparing
                        ) || after.phase != TopologyAdmissionPhase::Preparing
                            || next.certificates.len() != prior.certificates.len() + 1
                            || next
                                .certificates
                                .iter()
                                .filter(|cert| !prior.certificates.contains(cert))
                                .any(|cert| cert.authority_sequence != sequence)
                            || next
                                .complete_sequence
                                .is_some_and(|complete| complete != sequence)))
                {
                    return Err(TopologyError::Invalid(
                        "authority cannot rewrite, forget or append stale preparation certificates"
                            .into(),
                    ));
                }
                Ok(())
            }
            _ => Err(TopologyError::Invalid(
                "authority cannot replace an admitted descriptor binding".into(),
            )),
        }
    }
}
