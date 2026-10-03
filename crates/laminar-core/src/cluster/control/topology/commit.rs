//! Irreversible catalog decision, separate from installation and intake release.

use serde::{Deserialize, Serialize};

use super::{
    TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyError, TopologyOperationId,
    TopologyVersion,
};
use crate::cluster::control::CatalogManifestRef;

/// Every frozen participant must support Commit and explicit migration-root reconstruction.
pub const TOPOLOGY_COMMIT_PROTOCOL_VERSION: u16 = 4;

/// The exact shared append that changed the committed catalog. The containing operation's
/// immutable plan, root, cut, descriptor and complete preparation roster bind this decision.
/// This does not prove installation or authorize output, sink succession or intake release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopologyCommit {
    /// Required Commit/reconstruction protocol.
    pub protocol_version: u16,
    /// Original request identity, never replaced on an uncertain write.
    pub operation_id: TopologyOperationId,
    /// Previously committed logical version.
    pub parent_version: TopologyVersion,
    /// Exact previously committed inventory.
    pub parent_manifest: CatalogManifestRef,
    /// Exact successor logical version.
    pub topology_version: TopologyVersion,
    /// Inventory published atomically with this decision in the authority lease.
    pub manifest: CatalogManifestRef,
    /// Immutable decision append, retained across leader changes and recovery faults.
    pub authority_sequence: u64,
}

impl TopologyCommit {
    pub(crate) fn validate(&self) -> Result<(), TopologyError> {
        self.parent_manifest.validate()?;
        self.manifest.validate()?;
        if self.protocol_version != TOPOLOGY_COMMIT_PROTOCOL_VERSION
            || self.topology_version != self.parent_version.successor()?
            || self.manifest == self.parent_manifest
            || self.authority_sequence == 0
        {
            return Err(TopologyError::Invalid(
                "invalid topology Commit binding".into(),
            ));
        }
        Ok(())
    }
}

impl TopologyAdmissionStatus {
    pub(crate) fn has_target_commit(&self) -> bool {
        self.commit.is_some()
    }

    // A committed target reserves execution until a separately certified installation/Release.
    pub(crate) fn blocks_admission(&self) -> bool {
        self.is_preparing()
            || (self.has_target_commit() && self.phase != TopologyAdmissionPhase::Active)
    }

    pub(crate) fn validate_commit(&self, head: u64) -> Result<(), TopologyError> {
        match (&self.commit, self.phase) {
            (
                None,
                TopologyAdmissionPhase::Committed
                | TopologyAdmissionPhase::Activating
                | TopologyAdmissionPhase::Active,
            )
            | (Some(_), TopologyAdmissionPhase::Aborted { .. }) => Err(TopologyError::Invalid(
                "topology phase and Commit disagree".into(),
            )),
            (None, _) => Ok(()),
            (
                Some(commit),
                TopologyAdmissionPhase::Committed
                | TopologyAdmissionPhase::Activating
                | TopologyAdmissionPhase::Active,
            ) => {
                commit.validate()?;
                if commit.operation_id != self.operation_id
                    || commit.authority_sequence > self.status_sequence
                    || (self.phase == TopologyAdmissionPhase::Committed
                        && commit.authority_sequence != self.status_sequence)
                    || commit.authority_sequence > head
                    || !self.target_preparation_roster_complete()
                    || self.target_preparations.iter().any(|receipt| {
                        receipt.protocol_version != TOPOLOGY_COMMIT_PROTOCOL_VERSION
                            || receipt.authority_sequence >= commit.authority_sequence
                    })
                {
                    return Err(TopologyError::Invalid(
                        "Commit requires the complete protocol-four target preparation roster"
                            .into(),
                    ));
                }
                Ok(())
            }
            (Some(_), _) => Err(TopologyError::Invalid(
                "Commit outside a committed phase".into(),
            )),
        }
    }

    pub(crate) fn validate_commit_successor(
        &self,
        after: &Self,
        sequence: u64,
    ) -> Result<(), TopologyError> {
        match (&self.commit, &after.commit) {
            (None, None) => Ok(()),
            (Some(prior), Some(next))
                if prior == next
                    && self.same_migration_binding(after)
                    && self.target_preparations == after.target_preparations =>
            {
                Ok(())
            }
            (None, Some(commit))
                if self.phase == TopologyAdmissionPhase::CutPrepared
                    && after.phase == TopologyAdmissionPhase::Committed
                    && commit.authority_sequence == sequence
                    && self.operation_id == after.operation_id
                    && self.plan == after.plan
                    && self.admitted_by == after.admitted_by
                    && self.admitted_sequence == after.admitted_sequence
                    && self.cut == after.cut
                    && self.preparation == after.preparation
                    && self.migration_root == after.migration_root
                    && self.target_preparations == after.target_preparations =>
            {
                after.validate_commit(sequence)
            }
            _ => Err(TopologyError::Invalid(
                "authority cannot rewrite or roll back a topology Commit".into(),
            )),
        }
    }
}
