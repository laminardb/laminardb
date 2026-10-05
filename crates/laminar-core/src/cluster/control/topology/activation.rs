//! Exact runtime installation and owner-complete Release under the committed catalog.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyError};
use crate::checkpoint::{CheckpointAssignmentFence, LeaderProof};
use crate::cluster::control::LocalProcessAuthorityIdentity;

/// Requires held actors, generation-fenced completions and exact installation receipts.
pub const TOPOLOGY_INSTALLATION_PROTOCOL_VERSION: u16 = 5;

/// One still-held runtime observed by its owning process. Connector children remaining after
/// actor exit are termination obligations, never readiness evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyInstallationReceipt {
    /// Exact current boot and process term.
    pub process: LocalProcessAuthorityIdentity,
    /// Unique local runtime installation, retained on an identical request retry.
    pub runtime_id: Uuid,
    /// Capability of the actual installed runtime, not its historical preparation.
    pub protocol_version: u16,
    /// First shared append carrying the observation.
    pub authority_sequence: u64,
}

/// Durable permission to activate the complete installation roster. This is separate from
/// local gate opening; each process must still validate its own installed runtime and authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyRelease {
    /// Exact append after all installation receipts, with current leader/process/assignment.
    pub authority_sequence: u64,
}

/// A single current installation round. A replacement leader may supersede an unreleased round;
/// it must collect every runtime again. A published Release is immutable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyActivation {
    /// Current leader that owns this installation round.
    pub leader: LeaderProof,
    /// Existing recovery round that owns a replacement held installation before first Release.
    /// Original released rounds remain immutable; later recoveries use their own recovery terminal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_round: Option<crate::cluster::control::RecoveryRoundId>,
    /// Exact assignment, retaining the committed plan's owner map and stable participant IDs.
    pub assignment: CheckpointAssignmentFence,
    /// Complete sorted current owner/evidence process roster.
    pub processes: Vec<LocalProcessAuthorityIdentity>,
    /// Round identity and immutable first installation append.
    pub authority_sequence: u64,
    /// Monotonic sorted receipts from this round, never an arbitrary quorum.
    pub installations: Vec<TopologyInstallationReceipt>,
    /// Irreversible Release, absent until the complete roster has been revalidated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<TopologyRelease>,
}

impl TopologyActivation {
    /// Exact participant coverage; a current-authority audit is additionally required for Release.
    #[must_use]
    pub fn installation_complete(&self) -> bool {
        self.installations.len() == self.processes.len()
            && self
                .installations
                .iter()
                .zip(&self.processes)
                .all(|(receipt, process)| receipt.process == *process)
    }

    pub(crate) fn validate(&self, commit: u64, status: u64) -> Result<(), TopologyError> {
        if !self.leader.is_canonical()
            || self.recovery_round.is_some_and(|round| {
                round.generation == 0
                    || round.nonce.is_nil()
                    || round.driver.0 != self.leader.owner.node_id
            })
            || !self.assignment.is_canonical()
            || self.authority_sequence <= commit
            || self.authority_sequence > status
            || self.processes.len() != self.assignment.participants.len()
            || !self
                .processes
                .iter()
                .zip(&self.assignment.participants)
                .all(|(process, participant)| {
                    process.participant == *participant && process.process_term != 0
                })
            || !self.processes.iter().any(|process| {
                process.participant.node_id == self.leader.owner.node_id
                    && process.participant.boot_incarnation == self.leader.owner.boot_id
            })
            || self.installations.is_empty()
            || self.installations.len() > self.processes.len()
            || !self
                .installations
                .iter()
                .any(|receipt| receipt.authority_sequence == self.authority_sequence)
            || self
                .installations
                .iter()
                .enumerate()
                .any(|(index, receipt)| {
                    self.installations[..index]
                        .iter()
                        .any(|prior| prior.authority_sequence == receipt.authority_sequence)
                })
            || !self.installations.windows(2).all(|pair| {
                pair[0].process.participant.node_id < pair[1].process.participant.node_id
            })
            || self.installations.iter().any(|receipt| {
                receipt.protocol_version != TOPOLOGY_INSTALLATION_PROTOCOL_VERSION
                    || receipt.runtime_id.is_nil()
                    || !self.processes.contains(&receipt.process)
                    || receipt.authority_sequence < self.authority_sequence
                    || receipt.authority_sequence > status
            })
            || self.release.as_ref().is_some_and(|release| {
                !self.installation_complete()
                    || release.authority_sequence > status
                    || self
                        .installations
                        .iter()
                        .any(|receipt| receipt.authority_sequence >= release.authority_sequence)
            })
        {
            return Err(TopologyError::Invalid(
                "invalid exact-runtime installation or Release roster".into(),
            ));
        }
        Ok(())
    }
}

impl TopologyAdmissionStatus {
    pub(crate) fn validate_activation(&self) -> Result<(), TopologyError> {
        match (&self.activation, self.phase) {
            (None, TopologyAdmissionPhase::Activating | TopologyAdmissionPhase::Active) => Err(
                TopologyError::Invalid("activated phase has no installation round".into()),
            ),
            (None, _) => Ok(()),
            (
                Some(activation),
                TopologyAdmissionPhase::Activating | TopologyAdmissionPhase::Active,
            ) => {
                let commit = self.commit.as_ref().ok_or_else(|| {
                    TopologyError::Invalid("installation has no topology Commit".into())
                })?;
                activation.validate(commit.authority_sequence, self.status_sequence)?;
                let latest_evidence = activation.release.as_ref().map_or_else(
                    || {
                        activation
                            .installations
                            .iter()
                            .map(|receipt| receipt.authority_sequence)
                            .max()
                    },
                    |release| Some(release.authority_sequence),
                );
                if (self.phase == TopologyAdmissionPhase::Active) != activation.release.is_some()
                    || latest_evidence != Some(self.status_sequence)
                {
                    return Err(TopologyError::Invalid(
                        "activation phase and Release disagree".into(),
                    ));
                }
                Ok(())
            }
            (Some(_), _) => Err(TopologyError::Invalid(
                "installation outside committed activation".into(),
            )),
        }
    }

    pub(crate) fn validate_activation_successor(
        &self,
        after: &Self,
        sequence: u64,
    ) -> Result<(), TopologyError> {
        if self.activation == after.activation {
            return if self.has_target_commit() && self != after {
                Err(TopologyError::Invalid(
                    "committed installation status can change only with its exact evidence append"
                        .into(),
                ))
            } else {
                Ok(())
            };
        }
        if self.commit.is_none()
            || self.commit != after.commit
            || !self.same_migration_binding(after)
            || self.target_preparations != after.target_preparations
            || after.status_sequence != sequence
        {
            return Err(TopologyError::Invalid(
                "installation cannot change the committed migration binding".into(),
            ));
        }
        after.validate_activation()?;
        let next = after.activation.as_ref().ok_or_else(|| {
            TopologyError::Invalid("authority cannot forget installation evidence".into())
        })?;
        match &self.activation {
            None if self.phase == TopologyAdmissionPhase::Committed
                && after.phase == TopologyAdmissionPhase::Activating
                && next.authority_sequence == sequence
                && next.installations.len() == 1
                && next.installations[0].authority_sequence == sequence
                && next.release.is_none() =>
            {
                Ok(())
            }
            Some(prior)
                if prior.release.is_none()
                    && (prior.leader != next.leader
                        || (next.recovery_round.is_some()
                            && prior.recovery_round != next.recovery_round
                            && prior.recovery_round.is_none_or(|prior| {
                                next.recovery_round
                                    .is_some_and(|next| next.generation > prior.generation)
                            })))
                    && after.phase == TopologyAdmissionPhase::Activating
                    && next.authority_sequence == sequence
                    && next.installations.len() == 1
                    && next.installations[0].authority_sequence == sequence
                    && next.release.is_none() =>
            {
                Ok(())
            }
            Some(prior)
                if prior.leader == next.leader
                    && prior.recovery_round == next.recovery_round
                    && prior.assignment == next.assignment
                    && prior.processes == next.processes
                    && prior.authority_sequence == next.authority_sequence
                    && prior.release.is_none() =>
            {
                let receipt_append = after.phase == TopologyAdmissionPhase::Activating
                    && next.release.is_none()
                    && next.installations.len() == prior.installations.len() + 1
                    && prior
                        .installations
                        .iter()
                        .all(|receipt| next.installations.contains(receipt))
                    && next
                        .installations
                        .iter()
                        .filter(|receipt| !prior.installations.contains(receipt))
                        .all(|receipt| receipt.authority_sequence == sequence);
                let release = after.phase == TopologyAdmissionPhase::Active
                    && prior.installations == next.installations
                    && next
                        .release
                        .as_ref()
                        .is_some_and(|release| release.authority_sequence == sequence);
                if receipt_append || release {
                    Ok(())
                } else {
                    Err(TopologyError::Invalid(
                        "authority cannot rewrite installed runtimes or Release".into(),
                    ))
                }
            }
            _ => Err(TopologyError::Invalid(
                "authority cannot rewind an installation round or Release".into(),
            )),
        }
    }
}
