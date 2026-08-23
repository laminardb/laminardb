//! Versioned, feature-neutral contract for durable subscription output.

mod distribution;
mod identity;
mod manifest;

pub use distribution::{
    ChangelogMode, OutputDistribution, OutputDistributionCertificate,
    OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
};
pub use identity::{
    OutputFrameId, OutputPartitionId, PartitionSequence, StreamGeneration, SubscriptionDigest,
    SubscriptionProtocolVersion, SUBSCRIPTION_PROTOCOL_VERSION,
};
pub use manifest::{
    OutputSegmentRef, PartitionFrontier, SubscriptionCheckpointManifest, SubscriptionContractError,
    MAX_OUTPUT_SEGMENTS_PER_MANIFEST, MAX_SUBSCRIPTION_MANIFEST_BYTES,
};
