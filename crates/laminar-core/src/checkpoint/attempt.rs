//! Identity and ordering for checkpoint attempts.

/// Exact identity of one checkpoint attempt.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct CheckpointAttempt {
    /// Logical pipeline epoch represented by this checkpoint.
    pub epoch: u64,
    /// Never-reused checkpoint ID within the deployment.
    pub checkpoint_id: u64,
}

/// Relation between two checkpoint attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointAttemptRelation {
    /// Both fields are identical.
    Exact,
    /// Both fields are lower than the compared attempt.
    Older,
    /// Both fields are higher than the compared attempt.
    Newer,
    /// The fields do not define a consistent order.
    Conflict,
}

impl CheckpointAttempt {
    /// Construct an exact checkpoint identity.
    #[must_use]
    pub const fn new(epoch: u64, checkpoint_id: u64) -> Self {
        Self {
            epoch,
            checkpoint_id,
        }
    }

    /// Construct an attempt in the single durable checkpoint order.
    #[must_use]
    pub const fn canonical(checkpoint_id: u64) -> Self {
        Self::new(checkpoint_id, checkpoint_id)
    }

    /// Whether both fields represent the same nonzero durable identity.
    #[must_use]
    pub const fn is_canonical(self) -> bool {
        self.epoch != 0 && self.epoch == self.checkpoint_id
    }

    /// Relate this attempt to `other` without inventing a lexicographic order.
    #[must_use]
    pub const fn relation_to(self, other: Self) -> CheckpointAttemptRelation {
        if self.epoch == other.epoch && self.checkpoint_id == other.checkpoint_id {
            CheckpointAttemptRelation::Exact
        } else if self.epoch < other.epoch && self.checkpoint_id < other.checkpoint_id {
            CheckpointAttemptRelation::Older
        } else if self.epoch > other.epoch && self.checkpoint_id > other.checkpoint_id {
            CheckpointAttemptRelation::Newer
        } else {
            CheckpointAttemptRelation::Conflict
        }
    }
}
