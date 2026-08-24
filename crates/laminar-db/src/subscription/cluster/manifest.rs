//! Exact committed-checkpoint loading for the cluster subscription gateway.

use std::sync::Arc;

use laminar_core::checkpoint::{
    checkpoint_manifest_bytes, merge_node_subscription_manifests, CheckpointAttempt,
    CheckpointManifest, CheckpointParticipant, CheckpointStore, CheckpointStoreError,
    CommittedCheckpointIndex, CommittedCheckpointRef, NodePartitionRange,
    OutputDistributionCertificate, OutputSegmentRef, SubscriptionCheckpointManifest,
};
use laminar_core::state::KeyGroupCount;

use super::OutputSegmentBinding;
use crate::error::DbError;
use crate::subscription::ClusterSubscriptionError;

/// Segment reference paired with the participant authority omitted from the public manifest.
#[derive(Clone)]
pub(super) struct BoundOutputSegment {
    pub(super) reference: OutputSegmentRef,
    deployment_id: String,
    stream_id: String,
    attempt: CheckpointAttempt,
    participant: CheckpointParticipant,
    assignment_version: u64,
    assignment_digest: [u8; 32],
}

impl BoundOutputSegment {
    pub(super) fn binding(&self) -> OutputSegmentBinding<'_> {
        OutputSegmentBinding {
            deployment_id: &self.deployment_id,
            stream_id: &self.stream_id,
            attempt: self.attempt,
            participant: self.participant,
            assignment_version: self.assignment_version,
            assignment_digest: self.assignment_digest,
        }
    }
}

/// One fully validated cluster checkpoint and the requested stream's optional cut.
pub(super) struct LoadedCheckpoint {
    pub(super) reference: CommittedCheckpointRef,
    pub(super) index: CommittedCheckpointIndex,
    pub(super) stream: Option<LoadedStreamCut>,
}

/// Complete newly committed interval for one stream generation.
pub(super) struct LoadedStreamCut {
    pub(super) manifest: SubscriptionCheckpointManifest,
    pub(super) ranges: Vec<NodePartitionRange>,
    pub(super) segments: Vec<BoundOutputSegment>,
}

/// Load and cross-check every participant manifest named by one authoritative index.
pub(super) async fn load_checkpoint(
    store: &Arc<dyn CheckpointStore>,
    index: CommittedCheckpointIndex,
    certificate: &OutputDistributionCertificate,
) -> Result<LoadedCheckpoint, DbError> {
    let reference = index.encode_and_reference().map_err(manifest_error)?.1;
    let manifests = load_participant_manifests(store, &index).await?;
    let encoded = manifests
        .iter()
        .map(checkpoint_manifest_bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| manifest_error(format!("encode participant manifest: {error}")))?;
    let borrowed = manifests
        .iter()
        .zip(&encoded)
        .map(|(manifest, bytes)| (manifest, bytes.as_slice()))
        .collect::<Vec<_>>();
    index
        .validate_participant_manifests(&borrowed)
        .map_err(manifest_error)?;
    let stream = select_stream_cut(&index, &manifests, certificate)?;
    Ok(LoadedCheckpoint {
        reference,
        index,
        stream,
    })
}

async fn load_participant_manifests(
    store: &Arc<dyn CheckpointStore>,
    index: &CommittedCheckpointIndex,
) -> Result<Vec<CheckpointManifest>, DbError> {
    let key_groups = KeyGroupCount::try_from(u32::from(index.vnode_count))
        .map_err(|_| manifest_error("committed checkpoint vnode count is invalid"))?;
    if store.key_group_count() != key_groups {
        return Err(manifest_error(
            "gateway checkpoint store vnode count differs from the committed index",
        ));
    }
    let mut manifests = Vec::new();
    manifests
        .try_reserve_exact(index.participants.len())
        .map_err(|error| manifest_error(format!("reserve participant manifests: {error}")))?;
    for participant in &index.participants {
        let manifest = store
            .load_manifest_verified(
                participant.participant_id,
                index.checkpoint_id,
                participant.manifest_len,
                &participant.manifest_sha256,
            )
            .await
            .map_err(map_manifest_store_error)?
            .ok_or_else(|| {
                manifest_error(format!(
                    "participant {} manifest is missing",
                    participant.participant_id
                ))
            })?;
        manifests.push(manifest);
    }
    Ok(manifests)
}

fn select_stream_cut(
    index: &CommittedCheckpointIndex,
    manifests: &[CheckpointManifest],
    certificate: &OutputDistributionCertificate,
) -> Result<Option<LoadedStreamCut>, DbError> {
    let assignment = index
        .assignment_fence
        .as_ref()
        .ok_or_else(|| manifest_error("cluster checkpoint has no assignment certificate"))?;
    let node_outputs = manifests
        .iter()
        .map(|manifest| {
            manifest.subscription_output.as_ref().ok_or_else(|| {
                manifest_error(format!(
                    "participant {} omitted subscription output",
                    manifest.participant_id
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let merged = merge_node_subscription_manifests(
        index.epoch,
        index.checkpoint_id,
        assignment,
        &node_outputs,
    )
    .map_err(|error| manifest_error(error.to_string()))?;
    let Some(selected) = merged
        .into_iter()
        .find(|stream| stream.manifest.distribution_certificate.stream_id == certificate.stream_id)
    else {
        return Ok(None);
    };
    validate_certificate(certificate, &selected.manifest.distribution_certificate)?;
    let segments = bound_segments(index, manifests, certificate)?;
    if segments
        .iter()
        .map(|segment| &segment.reference)
        .ne(selected.manifest.segments.iter())
    {
        return Err(manifest_error(
            "participant segment roster differs from the merged stream manifest",
        ));
    }
    Ok(Some(LoadedStreamCut {
        manifest: selected.manifest,
        ranges: selected.ranges,
        segments,
    }))
}

fn validate_certificate(
    expected: &OutputDistributionCertificate,
    actual: &OutputDistributionCertificate,
) -> Result<(), DbError> {
    if expected.stream_generation != actual.stream_generation {
        return Err(ClusterSubscriptionError::GenerationMismatch.into());
    }
    if expected.schema_fingerprint != actual.schema_fingerprint {
        return Err(ClusterSubscriptionError::SchemaMismatch.into());
    }
    expected
        .require_match(actual)
        .map_err(|error| manifest_error(format!("distribution certificate mismatch: {error}")))
}

fn bound_segments(
    index: &CommittedCheckpointIndex,
    manifests: &[CheckpointManifest],
    certificate: &OutputDistributionCertificate,
) -> Result<Vec<BoundOutputSegment>, DbError> {
    let assignment = index
        .assignment_fence
        .as_ref()
        .ok_or_else(|| manifest_error("cluster checkpoint has no assignment certificate"))?;
    let mut segments = Vec::new();
    for manifest in manifests {
        let participant = assignment
            .participants
            .iter()
            .copied()
            .find(|participant| participant.node_id == manifest.participant_id)
            .ok_or_else(|| manifest_error("participant is absent from the assignment"))?;
        let Some(output) = manifest.subscription_output.as_ref() else {
            return Err(manifest_error("participant omitted subscription output"));
        };
        let Some(stream) = output
            .streams
            .iter()
            .find(|stream| stream.distribution_certificate.stream_id == certificate.stream_id)
        else {
            continue;
        };
        validate_certificate(certificate, &stream.distribution_certificate)?;
        segments
            .try_reserve(stream.segments.len())
            .map_err(|error| manifest_error(format!("reserve segment roster: {error}")))?;
        segments.extend(
            stream
                .segments
                .iter()
                .cloned()
                .map(|reference| BoundOutputSegment {
                    reference,
                    deployment_id: index.deployment_id.clone(),
                    stream_id: certificate.stream_id.clone(),
                    attempt: CheckpointAttempt::new(index.epoch, index.checkpoint_id),
                    participant,
                    assignment_version: assignment.assignment_version,
                    assignment_digest: assignment.digest(),
                }),
        );
    }
    segments.sort_unstable_by_key(|segment| {
        (
            segment.reference.partition,
            segment.reference.first_sequence,
        )
    });
    Ok(segments)
}

fn map_manifest_store_error(error: CheckpointStoreError) -> DbError {
    match error {
        CheckpointStoreError::ObjectStore(_) => ClusterSubscriptionError::BackendUnavailable.into(),
        other => manifest_error(other.to_string()),
    }
}

fn manifest_error(reason: impl Into<String>) -> DbError {
    ClusterSubscriptionError::ManifestCorrupt {
        reason: reason.into(),
    }
    .into()
}
