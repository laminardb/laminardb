use std::fmt::Write as _;

use sha2::{Digest as _, Sha256};

use super::SubscriptionContractError;
use crate::checkpoint::PipelineIdentity;
use crate::state::KeyGroupCount;

/// Current committed-subscription protocol version.
pub const SUBSCRIPTION_PROTOCOL_VERSION: u16 = 1;

/// Version of the durable subscription metadata and segment envelope.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct SubscriptionProtocolVersion(u16);

impl SubscriptionProtocolVersion {
    /// Current protocol version.
    pub const CURRENT: Self = Self(SUBSCRIPTION_PROTOCOL_VERSION);

    /// Construct a version read from a wire or persisted envelope.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Raw protocol value.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    /// Require the exact production protocol.
    pub(crate) fn validate(self) -> Result<(), SubscriptionContractError> {
        if self != Self::CURRENT {
            return Err(SubscriptionContractError::ProtocolVersion {
                actual: self.0,
                expected: SUBSCRIPTION_PROTOCOL_VERSION,
            });
        }
        Ok(())
    }
}

/// Fixed-size SHA-256 used by subscription metadata.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct SubscriptionDigest([u8; 32]);

impl SubscriptionDigest {
    /// Digest one byte string with a domain separator.
    #[must_use]
    pub fn for_bytes(domain: &[u8], value: &[u8]) -> Self {
        let mut hash = Sha256::new();
        update_part(&mut hash, domain);
        update_part(&mut hash, value);
        Self(hash.finalize().into())
    }

    /// Construct from an already verified SHA-256.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hexadecimal encoding for content-addressed object keys.
    #[must_use]
    pub fn to_hex(self) -> String {
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            let _ = write!(&mut encoded, "{byte:02x}");
        }
        encoded
    }

    pub(crate) fn validate(self, field: &'static str) -> Result<(), SubscriptionContractError> {
        if self.0 == [0; 32] {
            return Err(SubscriptionContractError::ZeroDigest { field });
        }
        Ok(())
    }
}

impl std::fmt::Display for SubscriptionDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Durable identity of one catalog stream incarnation.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct StreamGeneration(SubscriptionDigest);

impl StreamGeneration {
    /// Derive a generation from the durable deployment, catalog incarnation, query, and pipeline.
    #[must_use]
    pub fn derive(
        deployment_id: uuid::Uuid,
        catalog_generation: u64,
        stream_name: &str,
        canonical_query: &str,
        pipeline_identity: &PipelineIdentity,
    ) -> Self {
        let mut hash = Sha256::new();
        update_part(&mut hash, b"laminardb-subscription-stream-generation-v1");
        update_part(&mut hash, deployment_id.as_bytes());
        update_part(&mut hash, &catalog_generation.to_le_bytes());
        update_part(&mut hash, stream_name.as_bytes());
        update_part(&mut hash, canonical_query.as_bytes());
        update_part(
            &mut hash,
            &pipeline_identity.canonical_version.to_le_bytes(),
        );
        update_part(&mut hash, pipeline_identity.sha256.as_bytes());
        Self(SubscriptionDigest(hash.finalize().into()))
    }

    /// Construct from a fixed digest, for persisted metadata and tests.
    #[must_use]
    pub const fn from_digest(digest: SubscriptionDigest) -> Self {
        Self(digest)
    }

    /// Exact generation digest.
    #[must_use]
    pub const fn digest(self) -> SubscriptionDigest {
        self.0
    }

    pub(crate) fn validate(self) -> Result<(), SubscriptionContractError> {
        self.0.validate("stream_generation")
    }
}

impl std::fmt::Display for StreamGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Logical output partition. Keyed aggregate partitions are stable vnodes.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct OutputPartitionId(u16);

impl OutputPartitionId {
    /// Construct from a vnode number.
    #[must_use]
    pub const fn new(vnode: u16) -> Self {
        Self(vnode)
    }

    /// Vnode number represented by this output partition.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    pub(crate) fn validate(
        self,
        key_group_count: KeyGroupCount,
    ) -> Result<(), SubscriptionContractError> {
        if self.0 >= key_group_count.get() {
            return Err(SubscriptionContractError::PartitionOutOfRange {
                partition: self.0,
                vnode_count: key_group_count.get(),
            });
        }
        Ok(())
    }
}

/// Monotonic frame position within one stream generation and output partition.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct PartitionSequence(u64);

impl PartitionSequence {
    /// The first assigned partition sequence.
    pub const FIRST: Self = Self(0);

    /// Construct a position from persisted metadata.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Raw sequence value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advance by one without wrapping.
    ///
    /// # Errors
    /// Returns sequence overflow at `u64::MAX`.
    pub fn checked_next(self) -> Result<Self, SubscriptionContractError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(SubscriptionContractError::SequenceOverflow)
    }
}

/// Globally unambiguous identity of one partition-local output frame.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct OutputFrameId {
    /// Durable stream incarnation.
    pub stream_generation: StreamGeneration,
    /// Stable vnode output partition.
    pub partition: OutputPartitionId,
    /// Partition-local sequence.
    pub sequence: PartitionSequence,
}

fn update_part(hash: &mut Sha256, bytes: &[u8]) {
    hash.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hash.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipeline() -> PipelineIdentity {
        PipelineIdentity::empty()
    }

    #[test]
    fn generation_round_trips_and_recreation_changes_it() {
        let deployment = uuid::Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let first = StreamGeneration::derive(deployment, 7, "positions", "select 1", &pipeline());
        let recreated =
            StreamGeneration::derive(deployment, 8, "positions", "select 1", &pipeline());
        assert_ne!(first, recreated);

        let encoded = serde_json::to_vec(&first).unwrap();
        let decoded: StreamGeneration = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, first);
        assert_eq!(first.to_string().len(), 64);
    }

    #[test]
    fn partition_sequence_starts_at_zero_and_never_wraps() {
        assert_eq!(PartitionSequence::FIRST.get(), 0);
        assert_eq!(PartitionSequence::FIRST.checked_next().unwrap().get(), 1);
        assert_eq!(
            PartitionSequence::new(u64::MAX).checked_next(),
            Err(SubscriptionContractError::SequenceOverflow)
        );
    }
}
