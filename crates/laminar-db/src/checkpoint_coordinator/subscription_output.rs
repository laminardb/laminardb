use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use laminar_core::checkpoint::{
    merge_node_subscription_manifests, CheckpointAssignmentFence, CheckpointAttempt,
    CheckpointManifest, CommittedCheckpointIndex, CommittedCheckpointRef, LeaderProof,
    MergedSubscriptionCheckpoint, NodeSubscriptionManifest, NodeSubscriptionStreamManifest,
    OutputSegmentRef, StreamGeneration, SubscriptionContractError, SubscriptionDigest,
    SubscriptionProtocolVersion, MAX_OUTPUT_FRAMES_PER_SEGMENT, MAX_OUTPUT_SEGMENT_BYTES,
};

use super::CheckpointCoordinator;
use crate::error::DbError;
use crate::subscription::cluster::{
    decode_output_segment, encode_output_segment, EncodedOutputSegment, OutputSegmentIdentity,
    PreparedNodeSubscriptionOutput,
};
use crate::subscription::ClusterSubscriptionError;

const MAX_SUBSCRIPTION_UPLOAD_CONCURRENCY: usize = 4;
const TARGET_OUTPUT_SEGMENT_BYTES: usize = MAX_OUTPUT_SEGMENT_BYTES / 2;
const MAX_RETAINED_SUBSCRIPTION_INTERVALS: usize = 256;
const MAX_RETAINED_SUBSCRIPTION_SEGMENTS: usize = 262_144;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct SubscriptionCommitStats {
    pub(super) frames: u64,
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) segments: u64,
    pub(super) partitions: u64,
}

impl CheckpointCoordinator {
    pub(super) fn record_cluster_subscription_error(&self, error: &DbError) {
        let Some(metrics) = self.prom.as_ref() else {
            return;
        };
        let metrics = &metrics.cluster_subscription;
        match error {
            DbError::Subscription(ClusterSubscriptionError::ManifestCorrupt { .. }) => {
                metrics.manifest_failures_total.inc();
                metrics.integrity_failures_total.inc();
            }
            DbError::Subscription(ClusterSubscriptionError::PartitionSequenceGap { .. }) => {
                metrics.sequence_gaps_total.inc();
                metrics.integrity_failures_total.inc();
            }
            DbError::Subscription(ClusterSubscriptionError::StaleOutputWriter) => {
                metrics.stale_writer_rejections_total.inc();
            }
            DbError::Subscription(
                ClusterSubscriptionError::SegmentMissing { .. }
                | ClusterSubscriptionError::SegmentCorrupt { .. }
                | ClusterSubscriptionError::SchemaMismatch
                | ClusterSubscriptionError::ConflictingDuplicateSequence,
            ) => metrics.integrity_failures_total.inc(),
            _ => {}
        }
    }

    pub(super) fn record_subscription_commit(
        &self,
        stats: Option<SubscriptionCommitStats>,
        visibility: std::time::Duration,
        attempt: CheckpointAttempt,
    ) {
        let Some(stats) = stats else {
            return;
        };
        if let Some(metrics) = self.prom.as_ref() {
            let metrics = &metrics.cluster_subscription;
            metrics.frames_committed_total.inc_by(stats.frames);
            metrics.rows_committed_total.inc_by(stats.rows);
            metrics.bytes_committed_total.inc_by(stats.bytes);
            metrics
                .commit_visibility_seconds
                .observe(visibility.as_secs_f64());
        }
        tracing::info!(
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            partitions = stats.partitions,
            segments = stats.segments,
            frames = stats.frames,
            rows = stats.rows,
            bytes = stats.bytes,
            "published committed cluster subscription output"
        );
    }

    pub(super) async fn prepare_subscription_output_until(
        &self,
        attempt: CheckpointAttempt,
        assignment: Option<&CheckpointAssignmentFence>,
        prepared: Option<Arc<PreparedNodeSubscriptionOutput>>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<NodeSubscriptionManifest>, DbError> {
        let started = std::time::Instant::now();
        let result = self
            .prepare_subscription_output_inner(attempt, assignment, prepared, deadline)
            .await;
        if let Some(metrics) = self.prom.as_ref() {
            metrics
                .cluster_subscription
                .checkpoint_prepare_seconds
                .observe(started.elapsed().as_secs_f64());
            match &result {
                Err(DbError::Subscription(ClusterSubscriptionError::StaleOutputWriter)) => metrics
                    .cluster_subscription
                    .stale_writer_rejections_total
                    .inc(),
                Err(DbError::Subscription(ClusterSubscriptionError::PartitionSequenceGap {
                    ..
                })) => metrics.cluster_subscription.sequence_gaps_total.inc(),
                _ => {}
            }
        }
        result
    }

    async fn prepare_subscription_output_inner(
        &self,
        attempt: CheckpointAttempt,
        assignment: Option<&CheckpointAssignmentFence>,
        prepared: Option<Arc<PreparedNodeSubscriptionOutput>>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<NodeSubscriptionManifest>, DbError> {
        let Some(prepared) = prepared else {
            return Ok(None);
        };
        let assignment = assignment.cloned().ok_or_else(|| {
            DbError::Checkpoint(
                "subscription output checkpoint has no assignment certificate".into(),
            )
        })?;
        if prepared.attempt != attempt {
            return Err(DbError::Checkpoint(
                "subscription output belongs to a different checkpoint attempt".into(),
            ));
        }
        let deployment_id = self.expected_deployment_id()?.to_owned();
        let participant_id = self.store.participant_id();
        let owned_vnodes = self
            .owned_vnodes
            .iter()
            .map(|vnode| {
                u16::try_from(*vnode).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "owned vnode {vnode} exceeds the subscription partition ID space"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let encoding = tokio::task::spawn_blocking(move || {
            encode_node_subscription_output(
                prepared.as_ref(),
                &deployment_id,
                participant_id,
                &assignment,
                &owned_vnodes,
            )
        });
        let encoded = tokio::time::timeout_at(deadline, encoding)
            .await
            .map_err(|_| {
                DbError::Checkpoint("subscription segment encoding exceeded its deadline".into())
            })
            .and_then(|result| {
                result.map_err(|error| {
                    DbError::Checkpoint(format!("subscription segment encoder failed: {error}"))
                })
            });
        let encoded = encoded.inspect_err(|_| {
            if let Some(metrics) = self.prom.as_ref() {
                metrics.cluster_subscription.manifest_failures_total.inc();
            }
        })?;
        let (manifest, segments) = encoded.inspect_err(|_| {
            if let Some(metrics) = self.prom.as_ref() {
                metrics.cluster_subscription.manifest_failures_total.inc();
            }
        })?;

        let metrics = self.prom.clone();
        futures::stream::iter(segments)
            .map(|segment| {
                let metrics = metrics.clone();
                async move {
                    let result = match tokio::time::timeout_at(
                        deadline,
                        self.store
                            .save_subscription_segment(&segment.reference, segment.bytes),
                    )
                    .await
                    {
                        Ok(result) => result.map_err(DbError::from),
                        Err(_) => Err(DbError::Checkpoint(format!(
                            "subscription segment '{}' upload timed out",
                            segment.reference.object_key
                        ))),
                    };
                    if let Some(metrics) = metrics.as_ref() {
                        match &result {
                            Ok(()) => metrics.cluster_subscription.segments_written_total.inc(),
                            Err(_) => metrics
                                .cluster_subscription
                                .segment_write_failures_total
                                .inc(),
                        }
                    }
                    result
                }
            })
            .buffer_unordered(MAX_SUBSCRIPTION_UPLOAD_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(Some(manifest))
    }

    pub(super) async fn validate_subscription_continuity_until(
        &self,
        attempt: CheckpointAttempt,
        assignment: Option<&CheckpointAssignmentFence>,
        predecessor: Option<&CommittedCheckpointRef>,
        manifests: &[(CheckpointManifest, bytes::Bytes)],
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let current = merged_subscription_outputs(attempt, assignment, manifests)?;
        if current.is_empty() {
            return Ok(());
        }
        let predecessor = match predecessor {
            None => Vec::new(),
            Some(reference) => {
                let decisions = self.decision_store.as_ref().ok_or_else(|| {
                    DbError::Checkpoint(
                        "subscription predecessor validation requires a decision store".into(),
                    )
                })?;
                let index = tokio::time::timeout_at(
                    deadline,
                    decisions.load_committed_checkpoint(reference),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint("subscription predecessor checkpoint read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "subscription predecessor checkpoint read failed: {error}"
                    ))
                })?;
                let predecessor_manifests = tokio::time::timeout_at(
                    deadline,
                    super::load_index_manifests(self.store.as_ref(), &index),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "subscription predecessor participant read timed out".into(),
                    )
                })??;
                merged_subscription_outputs(
                    CheckpointAttempt::new(index.epoch, index.checkpoint_id),
                    index.assignment_fence.as_ref(),
                    &predecessor_manifests
                        .into_iter()
                        .map(|manifest| (manifest, bytes::Bytes::new()))
                        .collect::<Vec<_>>(),
                )?
            }
        };
        for checkpoint in &current {
            let prior = predecessor
                .iter()
                .find(|prior| {
                    prior.manifest.stream_generation == checkpoint.manifest.stream_generation
                })
                .map(|prior| &prior.manifest);
            checkpoint
                .validate_continuity(prior)
                .map_err(subscription_contract_error)?;
        }
        Ok(())
    }
}

pub(super) async fn cluster_subscription_retention_reference(
    store: &dyn laminar_core::checkpoint::CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    authority: &laminar_core::cluster::control::LeaderLeaseStore,
    proof: &LeaderProof,
    latest: Option<&CommittedCheckpointIndex>,
) -> Result<Option<CommittedCheckpointRef>, DbError> {
    let Some(latest) = latest else {
        return Ok(None);
    };
    let artifact_floor_epoch = authority
        .cluster_outcome_retention_boundary()
        .await
        .map_err(|error| DbError::Checkpoint(format!("load cluster retention boundary: {error}")))?
        .artifact_before_epoch;
    let horizon =
        cluster_subscription_retention_horizon(store, decisions, latest, artifact_floor_epoch)
            .await?;
    let selected_epoch = authority
        .reserve_subscription_cleanup_floor(proof, horizon.epoch)
        .await
        .map_err(|error| {
            DbError::Checkpoint(format!("reserve subscription cleanup floor: {error}"))
        })?;
    let horizon = if selected_epoch == horizon.epoch {
        horizon
    } else {
        let (_, selected) = authority
            .cluster_outcome_with_committed_checkpoint(selected_epoch)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("load replay-pinned cleanup horizon: {error}"))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "replay-pinned cleanup horizon is not a committed cluster epoch".into(),
                )
            })?;
        selected.ok_or_else(|| {
            DbError::Checkpoint("replay-pinned cleanup horizon has no checkpoint index".into())
        })?
    };
    horizon
        .encode_and_reference()
        .map(|(_, reference)| Some(reference))
        .map_err(DbError::Checkpoint)
}

pub(super) async fn cleanup_subscription_orphans(
    store: &dyn laminar_core::checkpoint::CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    latest: &CommittedCheckpointIndex,
    horizon: &CommittedCheckpointRef,
    grace_before_ms: i64,
) -> Result<SubscriptionOutputCleanup, DbError> {
    let retained = retained_subscription_segments(store, decisions, latest, horizon).await?;
    let orphan = store
        .delete_subscription_orphans(&retained.keys, latest.checkpoint_id, grace_before_ms)
        .await
        .map_err(DbError::from)?;
    Ok(SubscriptionOutputCleanup {
        retained_bytes: retained.bytes,
        orphan,
    })
}

pub(super) struct SubscriptionOutputCleanup {
    pub(super) retained_bytes: u64,
    pub(super) orphan: laminar_core::checkpoint::checkpoint_store::SubscriptionOrphanCleanup,
}

pub(super) fn record_subscription_cleanup(
    metrics: Option<&crate::engine_metrics::EngineMetrics>,
    cleanup: Option<&SubscriptionOutputCleanup>,
) {
    let retained_bytes = cleanup.map_or(0, |cleanup| cleanup.retained_bytes);
    let orphan_bytes = cleanup.map_or(0, |cleanup| cleanup.orphan.bytes_remaining);
    if let Some(metrics) = metrics {
        metrics
            .cluster_subscription
            .retained_bytes
            .set(i64::try_from(retained_bytes).unwrap_or(i64::MAX));
        metrics
            .cluster_subscription
            .orphan_bytes
            .set(i64::try_from(orphan_bytes).unwrap_or(i64::MAX));
    }
    let Some(cleanup) = cleanup else {
        return;
    };
    tracing::info!(
        retained_bytes,
        orphan_objects_scanned = cleanup.orphan.objects_scanned,
        orphan_objects_deleted = cleanup.orphan.objects_deleted,
        orphan_bytes_deleted = cleanup.orphan.bytes_deleted,
        orphan_bytes_remaining = orphan_bytes,
        "completed cluster subscription output cleanup"
    );
}

struct RetainedSubscriptionSegments {
    keys: std::collections::BTreeSet<String>,
    bytes: u64,
}

async fn retained_subscription_segments(
    store: &dyn laminar_core::checkpoint::CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    latest: &CommittedCheckpointIndex,
    horizon: &CommittedCheckpointRef,
) -> Result<RetainedSubscriptionSegments, DbError> {
    let mut current = latest.clone();
    let mut retained = RetainedSubscriptionSegments {
        keys: std::collections::BTreeSet::new(),
        bytes: 0,
    };
    for _ in 0..MAX_RETAINED_SUBSCRIPTION_INTERVALS {
        let manifests = super::load_index_manifests(store, &current).await?;
        for (object_key, encoded_length) in manifests
            .iter()
            .filter_map(|manifest| manifest.subscription_output.as_ref())
            .flat_map(|output| &output.streams)
            .flat_map(|stream| &stream.segments)
            .map(|segment| (&segment.object_key, segment.encoded_length))
        {
            if !retained.keys.insert(object_key.clone()) {
                return Err(ClusterSubscriptionError::ManifestCorrupt {
                    reason: "one segment object is referenced by multiple retained checkpoints"
                        .into(),
                }
                .into());
            }
            retained.bytes = retained.bytes.checked_add(encoded_length).ok_or_else(|| {
                DbError::Checkpoint("retained subscription byte count overflow".into())
            })?;
            if retained.keys.len() > MAX_RETAINED_SUBSCRIPTION_SEGMENTS {
                return Err(DbError::Checkpoint(format!(
                    "retained subscription segment roster exceeds {MAX_RETAINED_SUBSCRIPTION_SEGMENTS} objects"
                )));
            }
        }
        if current.epoch == horizon.epoch && current.checkpoint_id == horizon.checkpoint_id {
            return Ok(retained);
        }
        let predecessor = current.predecessor.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "subscription retention horizon is not in the committed chain".into(),
            )
        })?;
        let loaded = decisions
            .load_committed_checkpoint(predecessor)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("load retained subscription checkpoint: {error}"))
            })?;
        current
            .validate_predecessor_index(&loaded)
            .map_err(DbError::Checkpoint)?;
        current = loaded;
    }
    Err(DbError::Checkpoint(format!(
        "subscription retention chain exceeds {MAX_RETAINED_SUBSCRIPTION_INTERVALS} checkpoints"
    )))
}

pub(super) async fn cluster_subscription_retention_horizon(
    store: &dyn laminar_core::checkpoint::CheckpointStore,
    decisions: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    latest: &CommittedCheckpointIndex,
    artifact_floor_epoch: u64,
) -> Result<CommittedCheckpointIndex, DbError> {
    let latest_manifests = super::load_index_manifests(store, latest).await?;
    let latest_outputs = merged_subscription_manifests(
        CheckpointAttempt::new(latest.epoch, latest.checkpoint_id),
        latest.assignment_fence.as_ref(),
        latest_manifests.iter(),
    )?;
    let certificates = latest_outputs
        .iter()
        .map(|output| {
            (
                output.manifest.stream_generation,
                output.manifest.distribution_certificate.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let caps = retention_caps(&certificates);
    if caps.is_empty() {
        return Ok(latest.clone());
    }

    let mut retained_bytes = retained_output_bytes(&latest_outputs, &certificates)?;
    let mut current = latest.clone();
    let mut horizon = latest.clone();
    for _ in 0..MAX_RETAINED_SUBSCRIPTION_INTERVALS {
        if current.epoch <= artifact_floor_epoch || !retention_caps_fit(&retained_bytes, &caps) {
            break;
        }
        let Some(predecessor_ref) = current.predecessor.as_ref() else {
            break;
        };
        if predecessor_ref.epoch < artifact_floor_epoch {
            break;
        }
        let predecessor = decisions
            .load_committed_checkpoint(predecessor_ref)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("load subscription retention predecessor: {error}"))
            })?;
        current
            .validate_predecessor_index(&predecessor)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "subscription retention predecessor is invalid: {error}"
                ))
            })?;
        let manifests = super::load_index_manifests(store, &predecessor).await?;
        let outputs = merged_subscription_manifests(
            CheckpointAttempt::new(predecessor.epoch, predecessor.checkpoint_id),
            predecessor.assignment_fence.as_ref(),
            manifests.iter(),
        )?;
        let mut candidate_bytes = retained_bytes.clone();
        add_retained_output_bytes(&mut candidate_bytes, &outputs, &certificates)?;
        if !retention_caps_fit(&candidate_bytes, &caps) {
            break;
        }
        retained_bytes = candidate_bytes;
        horizon = predecessor.clone();
        current = predecessor;
    }
    Ok(horizon)
}

fn merged_subscription_manifests<'a>(
    attempt: CheckpointAttempt,
    assignment: Option<&CheckpointAssignmentFence>,
    manifests: impl IntoIterator<Item = &'a CheckpointManifest>,
) -> Result<Vec<MergedSubscriptionCheckpoint>, DbError> {
    let manifests = manifests.into_iter().collect::<Vec<_>>();
    let nodes = manifests
        .iter()
        .filter_map(|manifest| {
            manifest
                .subscription_output
                .as_ref()
                .map(|output| (output, manifest.owned_vnodes.as_slice()))
        })
        .collect::<Vec<_>>();
    if nodes.is_empty() {
        return Ok(Vec::new());
    }
    if nodes.len() != manifests.len() {
        return Err(DbError::Checkpoint(
            "participant manifests disagree on subscription output presence".into(),
        ));
    }
    let assignment = assignment.ok_or_else(|| {
        DbError::Checkpoint("cluster subscription output has no assignment certificate".into())
    })?;
    merge_node_subscription_manifests(attempt.epoch, attempt.checkpoint_id, assignment, &nodes)
        .map_err(subscription_contract_error)
}

fn retained_output_bytes(
    outputs: &[MergedSubscriptionCheckpoint],
    certificates: &BTreeMap<
        StreamGeneration,
        laminar_core::checkpoint::OutputDistributionCertificate,
    >,
) -> Result<BTreeMap<StreamGeneration, u64>, DbError> {
    let mut retained = BTreeMap::new();
    add_retained_output_bytes(&mut retained, outputs, certificates)?;
    Ok(retained)
}

fn add_retained_output_bytes(
    retained: &mut BTreeMap<StreamGeneration, u64>,
    outputs: &[MergedSubscriptionCheckpoint],
    certificates: &BTreeMap<
        StreamGeneration,
        laminar_core::checkpoint::OutputDistributionCertificate,
    >,
) -> Result<(), DbError> {
    if outputs.len() != certificates.len() {
        return Err(DbError::Checkpoint(
            "subscription retention stream roster changed across one checkpoint chain".into(),
        ));
    }
    for output in outputs {
        let generation = output.manifest.stream_generation;
        let expected = certificates.get(&generation).ok_or_else(|| {
            DbError::Checkpoint(
                "subscription retention encountered an unknown stream generation".into(),
            )
        })?;
        output
            .manifest
            .distribution_certificate
            .require_match(expected)
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "subscription retention certificate changed: {error}"
                ))
            })?;
        let bytes = output
            .manifest
            .segments
            .iter()
            .try_fold(0_u64, |total, segment| {
                total.checked_add(segment.encoded_length).ok_or_else(|| {
                    DbError::Checkpoint("subscription retained-byte count overflow".into())
                })
            })?;
        let total = retained.entry(generation).or_default();
        *total = total.checked_add(bytes).ok_or_else(|| {
            DbError::Checkpoint("subscription retained-byte count overflow".into())
        })?;
    }
    Ok(())
}

fn retention_caps_fit(
    retained: &BTreeMap<StreamGeneration, u64>,
    caps: &BTreeMap<StreamGeneration, u64>,
) -> bool {
    caps.iter()
        .all(|(generation, cap)| retained.get(generation).copied().unwrap_or(0) <= *cap)
}

pub(super) fn retention_caps(
    certificates: &BTreeMap<
        StreamGeneration,
        laminar_core::checkpoint::OutputDistributionCertificate,
    >,
) -> BTreeMap<StreamGeneration, u64> {
    certificates
        .iter()
        .map(|(generation, certificate)| (*generation, certificate.history_retention_bytes))
        .collect()
}

fn merged_subscription_outputs(
    attempt: CheckpointAttempt,
    assignment: Option<&CheckpointAssignmentFence>,
    manifests: &[(CheckpointManifest, bytes::Bytes)],
) -> Result<Vec<MergedSubscriptionCheckpoint>, DbError> {
    merged_subscription_manifests(
        attempt,
        assignment,
        manifests.iter().map(|(manifest, _)| manifest),
    )
}

pub(super) fn subscription_commit_stats(
    manifests: &[(CheckpointManifest, bytes::Bytes)],
) -> SubscriptionCommitStats {
    let mut stats = SubscriptionCommitStats::default();
    for stream in manifests
        .iter()
        .filter_map(|(manifest, _)| manifest.subscription_output.as_ref())
        .flat_map(|output| &output.streams)
    {
        stats.partitions = stats
            .partitions
            .saturating_add(u64::try_from(stream.ranges.len()).unwrap_or(u64::MAX));
        for segment in &stream.segments {
            stats.frames = stats.frames.saturating_add(segment.frame_count);
            stats.rows = stats.rows.saturating_add(segment.row_count);
            stats.bytes = stats.bytes.saturating_add(segment.encoded_length);
            stats.segments = stats.segments.saturating_add(1);
        }
    }
    stats
}

pub(super) fn encode_node_subscription_output(
    prepared: &PreparedNodeSubscriptionOutput,
    deployment_id: &str,
    participant_id: u64,
    assignment: &CheckpointAssignmentFence,
    owned_vnodes: &[u16],
) -> Result<(NodeSubscriptionManifest, Vec<EncodedOutputSegment>), DbError> {
    let mut streams = Vec::new();
    let mut encoded_segments = Vec::new();
    for stream in &prepared.streams {
        if stream.partitions.is_empty() {
            continue;
        }
        let mut ranges = Vec::with_capacity(stream.partitions.len());
        let mut segments = Vec::<OutputSegmentRef>::new();
        for partition in &stream.partitions {
            validate_partition_authority(partition, participant_id, assignment)?;
            ranges.push(partition.range);
            for frame_range in segment_frame_ranges(partition)? {
                let frames = &partition.frames[frame_range.clone()];
                let authority = frames
                    .first()
                    .ok_or_else(|| {
                        DbError::Checkpoint("subscription segment frame range is empty".into())
                    })?
                    .authority;
                let identity = OutputSegmentIdentity {
                    deployment_id,
                    stream_id: &stream.certificate.stream_id,
                    stream_generation: stream.certificate.stream_generation,
                    partition: partition.range.partition,
                    schema_fingerprint: stream.certificate.schema_fingerprint,
                    attempt: prepared.attempt,
                    authority,
                };
                let batches = frames
                    .iter()
                    .map(|frame| frame.batch.clone())
                    .collect::<Vec<_>>();
                let encoded = encode_output_segment(&identity, &batches, frames[0].id.sequence)?;
                decode_output_segment(&encoded.reference, &encoded.bytes)?;
                segments.push(encoded.reference.clone());
                encoded_segments.push(encoded);
            }
        }
        streams.push(NodeSubscriptionStreamManifest {
            distribution_certificate: (*stream.certificate).clone(),
            ranges,
            segments,
        });
    }
    let mut manifest = NodeSubscriptionManifest {
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        epoch: prepared.attempt.epoch,
        checkpoint_id: prepared.attempt.checkpoint_id,
        participant_id,
        assignment_certificate: assignment.clone(),
        streams,
        manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
    };
    manifest
        .seal(owned_vnodes)
        .map_err(subscription_contract_error)?;
    Ok((manifest, encoded_segments))
}

fn validate_partition_authority(
    partition: &crate::subscription::cluster::PreparedPartitionSubscriptionOutput,
    participant_id: u64,
    assignment: &CheckpointAssignmentFence,
) -> Result<(), DbError> {
    let mut expected = partition.range.first_sequence;
    for frame in &partition.frames {
        if frame.id.partition != partition.range.partition || frame.id.sequence != expected {
            return Err(DbError::Checkpoint(format!(
                "subscription partition {} is not contiguous at checkpoint preparation",
                partition.range.partition.get()
            )));
        }
        let authority = frame.authority;
        if authority.participant.node_id != participant_id
            || assignment.participant_incarnation(participant_id)
                != Some(authority.participant.boot_incarnation)
            || authority.assignment_version != assignment.assignment_version
            || authority.assignment_digest != assignment.digest()
            || authority.process_term == 0
        {
            return Err(ClusterSubscriptionError::StaleOutputWriter.into());
        }
        expected = expected.checked_next().map_err(|error| {
            DbError::Checkpoint(format!("advance subscription sequence: {error}"))
        })?;
    }
    if expected != partition.range.through_sequence {
        return Err(DbError::Checkpoint(format!(
            "subscription partition {} checkpoint frontier is discontinuous",
            partition.range.partition.get()
        )));
    }
    Ok(())
}

fn subscription_contract_error(error: SubscriptionContractError) -> DbError {
    match error {
        SubscriptionContractError::GenerationMismatch => {
            ClusterSubscriptionError::GenerationMismatch.into()
        }
        SubscriptionContractError::SchemaMismatch => {
            ClusterSubscriptionError::SchemaMismatch.into()
        }
        SubscriptionContractError::SequenceGap {
            partition,
            expected,
            actual,
        } => ClusterSubscriptionError::PartitionSequenceGap {
            partition: laminar_core::checkpoint::OutputPartitionId::new(partition),
            expected: laminar_core::checkpoint::PartitionSequence::new(expected),
            actual: laminar_core::checkpoint::PartitionSequence::new(actual),
        }
        .into(),
        SubscriptionContractError::ProtocolVersion { actual, .. } => {
            ClusterSubscriptionError::ProtocolVersion { actual }.into()
        }
        other => ClusterSubscriptionError::ManifestCorrupt {
            reason: other.to_string(),
        }
        .into(),
    }
}

fn segment_frame_ranges(
    partition: &crate::subscription::cluster::PreparedPartitionSubscriptionOutput,
) -> Result<Vec<std::ops::Range<usize>>, DbError> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < partition.frames.len() {
        let mut end = start;
        let mut estimated_bytes = 0usize;
        while end < partition.frames.len()
            && u64::try_from(end - start).unwrap_or(u64::MAX) < MAX_OUTPUT_FRAMES_PER_SEGMENT
        {
            let frame_bytes = partition.frames[end].batch.get_array_memory_size();
            let next = estimated_bytes.checked_add(frame_bytes).ok_or_else(|| {
                DbError::Checkpoint("subscription segment byte estimate overflow".into())
            })?;
            if end > start && next > TARGET_OUTPUT_SEGMENT_BYTES {
                break;
            }
            estimated_bytes = next;
            end += 1;
        }
        if end == start {
            return Err(DbError::Checkpoint(
                "subscription frame cannot fit a bounded output segment".into(),
            ));
        }
        ranges.push(start..end);
        start = end;
    }
    Ok(ranges)
}
