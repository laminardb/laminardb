//! Versioned, feature-neutral contract for durable subscription output.

mod distribution;
mod identity;
mod manifest;
mod node_manifest;

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
    MAX_OUTPUT_FRAMES_PER_SEGMENT, MAX_OUTPUT_SEGMENTS_PER_MANIFEST, MAX_OUTPUT_SEGMENT_BYTES,
    MAX_SUBSCRIPTION_MANIFEST_BYTES,
};
pub use node_manifest::{
    merge_node_subscription_manifests, MergedSubscriptionCheckpoint, NodePartitionRange,
    NodeSubscriptionManifest, NodeSubscriptionStreamManifest,
};
