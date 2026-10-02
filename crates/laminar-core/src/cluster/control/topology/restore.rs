//! Current-authority input for a private, effect-free target state image.

use super::{
    ClusterTopologyValidation, TopologyAdmissionPlan, TopologyAdmissionStatus,
    TopologyMigrationRoot,
};
use crate::checkpoint::CommittedCheckpointIndex;
use crate::checkpoint_decision::CheckpointOutcome;
use crate::cluster::control::{CatalogManifest, LocalProcessAuthorityIdentity};

/// A live read of the exact prepared operation, cut and local ownership.
/// Only the configured controller/authority can construct this input. It permits private restore
/// preparation, never graph installation, source start, output or intake release. A retained input
/// can become stale; installation must revalidate authority and require a target Commit/Release.
#[derive(Debug, PartialEq, Eq)]
pub struct TopologyRestoreInput {
    pub(crate) operation: TopologyAdmissionStatus,
    pub(crate) plan: TopologyAdmissionPlan,
    pub(crate) target: CatalogManifest,
    pub(crate) descriptor: ClusterTopologyValidation,
    pub(crate) root: TopologyMigrationRoot,
    pub(crate) outcome: CheckpointOutcome,
    pub(crate) checkpoint: CommittedCheckpointIndex,
    pub(crate) owned_vnodes: Vec<u32>,
    pub(crate) process: LocalProcessAuthorityIdentity,
}

impl TopologyRestoreInput {
    /// Compare all restore requirements, allowing only monotonic receipt/status progress.
    /// Both inputs must come from current controller authorization; this comparison never
    /// replaces a fresh read or the authority's receipt, leader, process and assignment audits.
    #[must_use]
    pub fn same_restore_requirements(&self, other: &Self) -> bool {
        self.operation.same_restore_binding(&other.operation)
            && self.plan == other.plan
            && self.target == other.target
            && self.descriptor == other.descriptor
            && self.root == other.root
            && self.outcome == other.outcome
            && self.checkpoint == other.checkpoint
            && self.owned_vnodes == other.owned_vnodes
            && self.process == other.process
    }
    /// Exact still-prepared operation, including its root authority anchor.
    #[must_use]
    pub const fn operation(&self) -> &TopologyAdmissionStatus {
        &self.operation
    }
    /// Immutable admitted plan and assignment.
    #[must_use]
    pub const fn plan(&self) -> &TopologyAdmissionPlan {
        &self.plan
    }
    /// Complete candidate inventory; it is not the committed catalog.
    #[must_use]
    pub const fn target(&self) -> &CatalogManifest {
        &self.target
    }
    /// Independently certified parent/target and per-object contracts.
    #[must_use]
    pub const fn descriptor(&self) -> &ClusterTopologyValidation {
        &self.descriptor
    }
    /// Immutable requirements, including sealed source positions and subscription mappings.
    #[must_use]
    pub const fn root(&self) -> &TopologyMigrationRoot {
        &self.root
    }
    /// Definitive old-topology checkpoint Commit; never a target Commit.
    #[must_use]
    pub const fn outcome(&self) -> &CheckpointOutcome {
        &self.outcome
    }
    /// Exact historical index, retaining the parent pipeline identity.
    #[must_use]
    pub const fn checkpoint(&self) -> &CommittedCheckpointIndex {
        &self.checkpoint
    }
    /// Canonical local vnode roster from the unchanged current assignment.
    #[must_use]
    pub fn owned_vnodes(&self) -> &[u32] {
        &self.owned_vnodes
    }
    /// Exact process incarnation and term for this preparation.
    #[must_use]
    pub const fn process(&self) -> LocalProcessAuthorityIdentity {
        self.process
    }
}
