//! Private recovery selection for an irreversible topology Commit.

use super::TopologyRestoreInput;
use crate::checkpoint::CommittedCheckpointIndex;
use crate::checkpoint_decision::CheckpointOutcome;

/// Which exact durable cut supplies a committed target's private state image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyRecoveryCut {
    /// The authorized historical parent cut and its explicit target state mapping.
    MigrationRoot,
    /// A later checkpoint produced by the committed target itself.
    TargetCheckpoint,
}

/// An audited choice of recovery cut, bound to the current target and local process adoption.
/// Construction belongs to the shared authority. This grants private state reads only; actor
/// installation and intake require a fresh coordinated recovery round and its complete quorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyRecoveryInput {
    pub(crate) migration: TopologyRestoreInput,
    pub(crate) target_checkpoint: Option<Box<(CheckpointOutcome, CommittedCheckpointIndex)>>,
}

impl TopologyRecoveryInput {
    /// The current target, retained mapping and exact current owner/process roster.
    #[must_use]
    pub const fn migration(&self) -> &TopologyRestoreInput {
        &self.migration
    }

    /// Selected state provenance. A target checkpoint never uses the parent state mapping.
    #[must_use]
    pub const fn cut(&self) -> TopologyRecoveryCut {
        if self.target_checkpoint.is_some() {
            TopologyRecoveryCut::TargetCheckpoint
        } else {
            TopologyRecoveryCut::MigrationRoot
        }
    }

    /// Definitive Commit of the selected cut, retaining its original checkpoint identity.
    #[must_use]
    pub fn outcome(&self) -> &CheckpointOutcome {
        self.target_checkpoint
            .as_deref()
            .map_or_else(|| self.migration.outcome(), |(outcome, _)| outcome)
    }

    /// Exact selected index. Root indexes retain the parent pipeline identity; later indexes
    /// must have the target pipeline identity. Neither is relabelled during reconstruction.
    #[must_use]
    pub fn checkpoint(&self) -> &CommittedCheckpointIndex {
        self.target_checkpoint
            .as_deref()
            .map_or_else(|| self.migration.checkpoint(), |(_, checkpoint)| checkpoint)
    }

    /// Compare two fresh authorizations, including the chosen cut and all local restore fences.
    /// The caller must still recheck the current recovery round before actor installation.
    #[must_use]
    pub fn same_restore_requirements(&self, other: &Self) -> bool {
        self.migration.same_restore_requirements(&other.migration)
            && self.target_checkpoint == other.target_checkpoint
    }
}
