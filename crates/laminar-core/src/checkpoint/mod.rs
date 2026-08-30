//! Checkpoint barrier protocol and storage.
//!
//! Coordinator-triggered barriers flow through sources to trigger consistent
//! state snapshots. The fast path is a single `AtomicU64` load (~10ns).

/// Checkpoint barrier types and cross-thread injection.
pub mod barrier;

/// Identity and ordering for checkpoint attempts.
pub mod attempt;

/// Feature-neutral assignment certificate retained by exact checkpoint attempts.
pub mod assignment;

/// Feature-neutral leader authority retained by durable protocol records.
pub mod authority;

/// Unified checkpoint manifest types
pub mod checkpoint_manifest;

/// Checkpoint persistence trait and filesystem/object store implementations
pub mod checkpoint_store;

/// Object store factory — builds S3, GCS, Azure, or local backends from URL schemes.
pub mod object_store_builder;

/// Canonical global index selected by a committed checkpoint.
pub mod committed_checkpoint;

/// Durable identities and canonical metadata for committed subscription output.
pub mod subscription;

pub use assignment::{
    AssignmentDrainId, AssignmentDrainTransition, CheckpointAssignmentAdoption,
    CheckpointAssignmentFence, CheckpointParticipant, MAX_CHECKPOINT_PARTICIPANTS,
};
pub use attempt::{CheckpointAttempt, CheckpointAttemptRelation};
pub use authority::{LeaderProof, LeaderProofOwner};
pub use barrier::{
    flags, BarrierPollHandle, CheckpointBarrier, CheckpointBarrierInjector, StreamMessage,
};

pub use checkpoint_manifest::{
    checkpoint_artifact_intent_sha256, checkpoint_descriptor_sha256, checkpoint_sha256, ByteRange,
    ChannelProgress, CheckpointManifest, ConnectorCheckpoint, NodeDataObject, PipelineIdentity,
    PreparedSinkArtifactIntent, PreparedSinkDescriptor, ReferencedStateChunk, StateChunkId,
    StateFrame, StateFrameKey, PIPELINE_IDENTITY_VERSION, PREPARED_SINK_DESCRIPTOR_VERSION,
};
pub use checkpoint_store::{
    checkpoint_artifact_identity_sha256, checkpoint_manifest_bytes,
    probe_object_store_conditional_create, probe_object_store_conditional_update,
    CheckpointManifestAbortSeal, CheckpointSinkArtifactIntent, CheckpointStore,
    CheckpointStoreError, ObjectStoreCheckpointStore,
    MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_AGGREGATE_BYTES, MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES,
};
pub use committed_checkpoint::{
    canonical_json_bytes, canonical_json_sha256, CheckpointScope, CheckpointWatermark,
    CommittedCheckpointIndex, CommittedCheckpointRef, CommittedParticipantRef,
    COMMITTED_CHECKPOINT_INDEX_VERSION, MAX_COMMITTED_CHECKPOINT_INDEX_BYTES,
};
pub use subscription::{
    merge_node_subscription_manifests, ChangelogMode, MergedSubscriptionCheckpoint,
    NodePartitionRange, NodeSubscriptionManifest, NodeSubscriptionStreamManifest,
    OutputDistribution, OutputDistributionCertificate, OutputFrameId, OutputPartitionId,
    OutputSegmentRef, PartitionFrontier, PartitionSequence, StreamGeneration,
    SubscriptionCheckpointManifest, SubscriptionContractError, SubscriptionDigest,
    SubscriptionProtocolVersion, MAX_OUTPUT_FRAMES_PER_SEGMENT, MAX_OUTPUT_SEGMENT_BYTES,
    OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION, SUBSCRIPTION_PROTOCOL_VERSION,
};

/// Reserved input-channel identity for one logical watermark per source and participant.
pub const SINGLETON_WATERMARK_CHANNEL: &[u8] = b"\0";

/// Reconstruct the event-time state represented by an exact channel cut.
///
/// # Errors
/// Returns an error when a channel uses the reserved uninitialized watermark value.
pub fn classify_channel_progress(
    channels: &[ChannelProgress],
) -> Result<CheckpointWatermark, String> {
    let mut minimum = None;
    for channel in channels {
        if channel.watermark == Some(i64::MIN) {
            return Err("channel progress uses the reserved uninitialized watermark".into());
        }
        if channel.idle {
            continue;
        }
        let Some(watermark) = channel.watermark else {
            return Ok(CheckpointWatermark::Uninitialized);
        };
        minimum = Some(minimum.map_or(watermark, |current: i64| current.min(watermark)));
    }
    Ok(minimum.map_or(CheckpointWatermark::Idle, CheckpointWatermark::Active))
}

/// Numeric frontier retained by a channel cut, including an all-idle cut.
///
/// # Errors
/// Returns an error when the channel cut contains an invalid watermark sentinel.
pub fn channel_progress_frontier(channels: &[ChannelProgress]) -> Result<Option<i64>, String> {
    Ok(match classify_channel_progress(channels)? {
        CheckpointWatermark::Active(watermark) => Some(watermark),
        CheckpointWatermark::Idle => channels
            .iter()
            .filter_map(|channel| channel.watermark)
            .max(),
        CheckpointWatermark::Uninitialized => None,
    })
}

/// Numeric decision frontier retained for each source in an exact channel cut.
///
/// Active channels contribute their minimum. An active uninitialized channel withholds only its
/// own source, while an all-idle source retains the greatest initialized channel watermark. This
/// is the source-keyed counterpart of [`channel_progress_frontier`]; callers must not use the
/// pipeline-wide minimum when advancing a source-specific ordered operator.
///
/// # Errors
/// Returns an error when a channel uses the reserved uninitialized watermark value.
pub fn channel_progress_frontiers_by_source(
    channels: &[ChannelProgress],
) -> Result<std::collections::BTreeMap<&str, Option<i64>>, String> {
    #[derive(Default)]
    struct SourceProgress {
        active_minimum: Option<i64>,
        active_uninitialized: bool,
        idle_maximum: Option<i64>,
    }

    let mut progress = std::collections::BTreeMap::<&str, SourceProgress>::new();
    for channel in channels {
        if channel.watermark == Some(i64::MIN) {
            return Err("channel progress uses the reserved uninitialized watermark".into());
        }
        let source = progress.entry(channel.source_name.as_str()).or_default();
        if channel.idle {
            if let Some(watermark) = channel.watermark {
                source.idle_maximum = Some(
                    source
                        .idle_maximum
                        .map_or(watermark, |current| current.max(watermark)),
                );
            }
        } else if let Some(watermark) = channel.watermark {
            source.active_minimum = Some(
                source
                    .active_minimum
                    .map_or(watermark, |current| current.min(watermark)),
            );
        } else {
            source.active_uninitialized = true;
        }
    }

    Ok(progress
        .into_iter()
        .map(|(source, progress)| {
            let frontier = if progress.active_uninitialized {
                None
            } else {
                progress.active_minimum.or(progress.idle_maximum)
            };
            (source, frontier)
        })
        .collect())
}
