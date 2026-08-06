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
    checkpoint_descriptor_sha256, checkpoint_sha256, ByteRange, ChannelProgress,
    CheckpointManifest, ConnectorCheckpoint, NodeDataObject, PipelineIdentity,
    PreparedSinkDescriptor, ReferencedStateChunk, StateChunkId, StateFrame, StateFrameKey,
    PIPELINE_IDENTITY_VERSION, PREPARED_SINK_DESCRIPTOR_VERSION,
};
pub use checkpoint_store::{
    checkpoint_manifest_bytes, probe_object_store_conditional_create,
    probe_object_store_conditional_update, CheckpointStore, CheckpointStoreError,
    ObjectStoreCheckpointStore,
};
pub use committed_checkpoint::{
    canonical_json_bytes, canonical_json_sha256, CheckpointScope, CheckpointWatermark,
    CommittedCheckpointIndex, CommittedCheckpointRef, CommittedParticipantRef,
    COMMITTED_CHECKPOINT_INDEX_VERSION, MAX_COMMITTED_CHECKPOINT_INDEX_BYTES,
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
