//! Current-authority input for a private, effect-free target state image.

use super::{
    ClusterTopologyValidation, TopologyAdmissionPlan, TopologyAdmissionStatus,
    TopologyMigrationRoot,
};
use crate::checkpoint::{CheckpointAssignmentFence, CommittedCheckpointIndex, LeaderProof};
use crate::checkpoint_decision::CheckpointOutcome;
use crate::cluster::control::{CatalogManifest, LocalProcessAuthorityIdentity};

/// A live read of the exact prepared operation, cut and local ownership.
/// Only the configured controller/authority can construct this input. It permits private restore
/// preparation, never graph installation, source start, output or intake release. A retained input
/// can become stale; installation must revalidate authority and require a target Commit/Release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyRestoreInput {
    pub(crate) operation: TopologyAdmissionStatus,
    pub(crate) plan: TopologyAdmissionPlan,
    pub(crate) parent: CatalogManifest,
    pub(crate) target: CatalogManifest,
    pub(crate) descriptor: ClusterTopologyValidation,
    pub(crate) root: TopologyMigrationRoot,
    pub(crate) outcome: CheckpointOutcome,
    pub(crate) checkpoint: CommittedCheckpointIndex,
    pub(crate) owned_vnodes: Vec<u32>,
    pub(crate) process: LocalProcessAuthorityIdentity,
    pub(crate) restore_assignment: CheckpointAssignmentFence,
    pub(crate) restore_processes: Vec<LocalProcessAuthorityIdentity>,
    pub(crate) committed_leader: Option<LeaderProof>,
}

impl TopologyRestoreInput {
    /// Exact current owner/evidence roster revalidated for committed reconstruction.
    #[must_use]
    pub fn processes(&self) -> &[LocalProcessAuthorityIdentity] {
        &self.restore_processes
    }

    /// Current committed reconstruction leader, absent for pre-Commit private preparation.
    #[must_use]
    pub fn current_leader(&self) -> Option<LeaderProof> {
        self.committed_leader.clone()
    }
    /// Compare all restore requirements, allowing only monotonic receipt/status progress.
    /// Both inputs must come from current controller authorization; this comparison never
    /// replaces a fresh read or the authority's receipt, leader, process and assignment audits.
    #[must_use]
    pub fn same_restore_requirements(&self, other: &Self) -> bool {
        self.same_installed_generation(other) && self.committed_leader == other.committed_leader
    }

    /// Immutable installed generation, excluding the current leader read. A leader change after
    /// Commit never changes the restored state; current installation/Release must be audited anew.
    #[must_use]
    pub fn same_installed_generation(&self, other: &Self) -> bool {
        self.operation.same_restore_binding(&other.operation)
            && self.plan == other.plan
            && self.parent == other.parent
            && self.target == other.target
            && self.descriptor == other.descriptor
            && self.root == other.root
            && self.outcome == other.outcome
            && self.checkpoint == other.checkpoint
            && self.owned_vnodes == other.owned_vnodes
            && self.process == other.process
            && self.restore_assignment == other.restore_assignment
            && self.restore_processes == other.restore_processes
    }

    /// Whether a fresh committed authorization retains the prepared image's exact historical
    /// requirements and local process. This compares images only, never grants output or Release.
    #[must_use]
    pub fn is_committed_successor_of(&self, prepared: &Self) -> bool {
        self.operation.has_target_commit()
            && self.committed_leader.is_some()
            && prepared.operation.phase == super::TopologyAdmissionPhase::CutPrepared
            && prepared.operation.commit.is_none()
            && self.operation.same_migration_binding(&prepared.operation)
            && self.plan == prepared.plan
            && self.parent == prepared.parent
            && self.target == prepared.target
            && self.descriptor == prepared.descriptor
            && self.root == prepared.root
            && self.outcome == prepared.outcome
            && self.checkpoint == prepared.checkpoint
            && self.owned_vnodes == prepared.owned_vnodes
            && self.process == prepared.process
            && self.restore_assignment == prepared.restore_assignment
    }

    /// Exact current assignment authorizing this private reconstruction. Historical state and
    /// source attempts retain `plan().assignment`; recovery may replace boots with the same owners.
    #[must_use]
    pub const fn assignment(&self) -> &CheckpointAssignmentFence {
        &self.restore_assignment
    }

    /// Whether this is explicit committed-root reconstruction, still without installation/Release.
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        self.committed_leader.is_some()
    }
    /// Exact prepared or committed operation, including its root authority anchor.
    #[must_use]
    pub const fn operation(&self) -> &TopologyAdmissionStatus {
        &self.operation
    }
    /// Immutable admitted plan and assignment.
    #[must_use]
    pub const fn plan(&self) -> &TopologyAdmissionPlan {
        &self.plan
    }
    /// Complete exact predecessor inventory. Removals cannot be inferred from a target prefix.
    #[must_use]
    pub const fn parent(&self) -> &CatalogManifest {
        &self.parent
    }
    /// Complete sealed target inventory, committed only when this authorization records Commit.
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
