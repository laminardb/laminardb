//! Stable failures exposed by the committed cluster-subscription backend.

use laminar_core::checkpoint::{OutputPartitionId, PartitionSequence};

/// Structured terminal or admission error for a cluster subscription.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClusterSubscriptionError {
    /// The final operator has no stable recoverable output distribution.
    #[error("cluster subscription is unsupported for this plan: {reason}")]
    UnsupportedPlan {
        /// Planner-owned rejection reason.
        reason: String,
    },
    /// History or a cursor belongs to a different stream incarnation.
    #[error("subscription stream generation does not match the current catalog object")]
    GenerationMismatch,
    /// The requested epoch has not committed.
    #[error("subscription epoch {requested} is not committed")]
    EpochNotCommitted {
        /// Requested checkpoint epoch.
        requested: u64,
    },
    /// The requested epoch is older than retained history.
    #[error("subscription epoch {requested} is no longer retained")]
    ReplayPruned {
        /// Requested checkpoint epoch.
        requested: u64,
    },
    /// Authoritative committed metadata is malformed.
    #[error("committed subscription manifest is corrupt: {reason}")]
    ManifestCorrupt {
        /// Bounded validation detail.
        reason: String,
    },
    /// A segment referenced by committed authority is unavailable.
    #[error("committed subscription segment is missing for partition {partition:?} at {first:?}")]
    SegmentMissing {
        /// Affected output partition.
        partition: OutputPartitionId,
        /// First missing sequence.
        first: PartitionSequence,
    },
    /// A referenced segment failed metadata, length, or digest validation.
    #[error("committed subscription segment is corrupt for partition {partition:?} at {first:?}")]
    SegmentCorrupt {
        /// Affected output partition.
        partition: OutputPartitionId,
        /// First corrupt sequence.
        first: PartitionSequence,
    },
    /// User schema differs from the certified output schema.
    #[error("subscription output schema does not match its distribution certificate")]
    SchemaMismatch,
    /// A committed partition range is discontinuous.
    #[error(
        "subscription partition {partition:?} expected sequence {expected:?}, found {actual:?}"
    )]
    PartitionSequenceGap {
        /// Affected output partition.
        partition: OutputPartitionId,
        /// Required next sequence.
        expected: PartitionSequence,
        /// Observed sequence.
        actual: PartitionSequence,
    },
    /// The same frame identity names different content.
    #[error("subscription frame identity has conflicting immutable content")]
    ConflictingDuplicateSequence,
    /// Writer authority no longer covers the target vnode.
    #[error("subscription output writer is stale")]
    StaleOutputWriter,
    /// Assignment changed while an operation was being fenced.
    #[error("subscription output assignment changed")]
    AssignmentChanged,
    /// Shared storage or committed-index authority cannot currently be reached.
    #[error("committed subscription backend is unavailable")]
    BackendUnavailable,
    /// Bounded connection buffers could not preserve every frame.
    #[error("subscription reader exceeded its bounded lag allowance")]
    SubscriberLagged,
    /// Token is malformed, tampered with, or bound to another stream.
    #[error("subscription resume token is invalid")]
    ResumeTokenInvalid,
    /// Token is valid but outside its accepted lifetime.
    #[error("subscription resume token has expired")]
    ResumeTokenExpired,
    /// Required history disappeared after it was admitted for replay.
    #[error("subscription retention no longer covers the required committed range")]
    RetentionLost,
    /// Decoded envelope uses an unsupported version.
    #[error("subscription protocol version {actual} is unsupported")]
    ProtocolVersion {
        /// Decoded protocol value.
        actual: u16,
    },
}

impl ClusterSubscriptionError {
    /// Stable `LaminarDB` error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        use laminar_core::error_codes as codes;

        match self {
            Self::UnsupportedPlan { .. } => codes::SUBSCRIPTION_PLAN_UNSUPPORTED,
            Self::GenerationMismatch => codes::SUBSCRIPTION_GENERATION_MISMATCH,
            Self::EpochNotCommitted { .. } => codes::SUBSCRIPTION_EPOCH_NOT_COMMITTED,
            Self::ReplayPruned { .. } => codes::SUBSCRIPTION_REPLAY_PRUNED,
            Self::ManifestCorrupt { .. } => codes::SUBSCRIPTION_MANIFEST_CORRUPT,
            Self::SegmentMissing { .. } => codes::SUBSCRIPTION_SEGMENT_MISSING,
            Self::SegmentCorrupt { .. } => codes::SUBSCRIPTION_SEGMENT_CORRUPT,
            Self::SchemaMismatch => codes::SUBSCRIPTION_SCHEMA_MISMATCH,
            Self::PartitionSequenceGap { .. } => codes::SUBSCRIPTION_SEQUENCE_GAP,
            Self::ConflictingDuplicateSequence => codes::SUBSCRIPTION_CONFLICTING_DUPLICATE,
            Self::StaleOutputWriter => codes::SUBSCRIPTION_STALE_WRITER,
            Self::AssignmentChanged => codes::SUBSCRIPTION_ASSIGNMENT_CHANGED,
            Self::BackendUnavailable => codes::SUBSCRIPTION_BACKEND_UNAVAILABLE,
            Self::SubscriberLagged => codes::SUBSCRIPTION_LAGGED,
            Self::ResumeTokenInvalid => codes::SUBSCRIPTION_RESUME_TOKEN_INVALID,
            Self::ResumeTokenExpired => codes::SUBSCRIPTION_RESUME_TOKEN_EXPIRED,
            Self::RetentionLost => codes::SUBSCRIPTION_RETENTION_LOST,
            Self::ProtocolVersion { .. } => codes::SUBSCRIPTION_PROTOCOL_UNSUPPORTED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correctness_failures_have_distinct_stable_codes() {
        let errors = [
            ClusterSubscriptionError::GenerationMismatch,
            ClusterSubscriptionError::ManifestCorrupt {
                reason: "digest".into(),
            },
            ClusterSubscriptionError::ConflictingDuplicateSequence,
            ClusterSubscriptionError::StaleOutputWriter,
            ClusterSubscriptionError::SubscriberLagged,
            ClusterSubscriptionError::ResumeTokenInvalid,
        ];
        let codes = errors.map(|error| error.code());
        assert!(codes.iter().all(|code| code.starts_with("LDB-")));
        for (index, code) in codes.iter().enumerate() {
            assert!(!codes[..index].contains(code));
        }
    }
}
