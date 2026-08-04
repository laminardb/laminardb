//! Exact-cut recovery from committed v7 checkpoint manifests.
#![allow(clippy::disallowed_types)] // bounded recovery metadata

use std::collections::BTreeMap;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use laminar_core::checkpoint::CheckpointStore;
use laminar_core::checkpoint::{
    checkpoint_manifest_bytes, checkpoint_sha256, ByteRange, ChannelProgress, CheckpointManifest,
    CheckpointScope, CommittedCheckpointIndex, ConnectorCheckpoint, PipelineIdentity, StateChunkId,
    StateFrame, StateFrameKey,
};
use laminar_core::checkpoint_decision::{CheckpointOutcome, CheckpointVerdict};

use crate::error::DbError;

const PARALLEL_MANIFEST_READS: usize = 8;
const PARALLEL_CHUNK_READS: usize = 4;

/// One checksummed state frame staged during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredStateFrame {
    /// Participant whose manifest declares the logical frame.
    pub participant_id: u64,
    /// Stable logical state identity.
    pub key: StateFrameKey,
    /// Verified state bytes.
    pub payload: Bytes,
}

/// Complete state selected by one immutable Commit outcome.
#[derive(Debug, Clone)]
pub struct RecoveredState {
    /// Terminal outcome authorizing this recovery cut.
    pub outcome: CheckpointOutcome,
    /// Verified global checkpoint index. It owns the authoritative source and time cut.
    pub committed: CommittedCheckpointIndex,
    /// Exact participant manifests, ordered by participant id.
    pub manifests: Vec<CheckpointManifest>,
    /// Verified frame inventory for this node's participant, ordered by logical key.
    pub state_frames: Vec<RecoveredStateFrame>,
}

impl RecoveredState {
    /// Recovered epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.committed.epoch
    }

    /// Authoritative source offsets for replay.
    #[must_use]
    pub const fn source_offsets(&self) -> &BTreeMap<String, ConnectorCheckpoint> {
        &self.committed.source_offsets
    }

    /// Authoritative per-channel watermark and idleness cut.
    #[must_use]
    pub fn channel_progress(&self) -> &[ChannelProgress] {
        &self.committed.channel_progress
    }

    /// Global checkpoint watermark derived from active channels.
    #[must_use]
    pub const fn checkpoint_watermark(&self) -> Option<i64> {
        self.committed.checkpoint_watermark
    }
}

/// Loads a single explicitly committed checkpoint cut.
pub struct RecoveryManager<'a> {
    store: &'a dyn CheckpointStore,
    pipeline_identity: PipelineIdentity,
    deployment_id: String,
    scope: CheckpointScope,
}

#[derive(Debug)]
struct PendingFrame {
    participant_id: u64,
    key: StateFrameKey,
    range: ByteRange,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkMetadata {
    object_length: u64,
    sha256: String,
}

#[derive(Debug)]
pub(crate) struct VerifiedStateFramePlan {
    chunks: BTreeMap<StateChunkId, ChunkMetadata>,
    frames: BTreeMap<StateChunkId, Vec<PendingFrame>>,
}

impl VerifiedStateFramePlan {
    pub(crate) fn new(
        manifest: &CheckpointManifest,
        selected: &[StateFrame],
    ) -> Result<Self, DbError> {
        if selected.windows(2).any(|pair| pair[0].key >= pair[1].key) {
            return Err(checkpoint_error(
                "selected state frames are not in canonical logical-key order",
            ));
        }

        let mut declared = BTreeMap::<StateChunkId, ChunkMetadata>::new();
        insert_chunk(
            &mut declared,
            manifest.node_data.chunk,
            ChunkMetadata {
                object_length: manifest.node_data.object_length,
                sha256: manifest.node_data.sha256.clone(),
            },
        )?;
        for reference in &manifest.referenced_chunks {
            insert_chunk(
                &mut declared,
                reference.chunk,
                ChunkMetadata {
                    object_length: reference.object_length,
                    sha256: reference.sha256.clone(),
                },
            )?;
        }

        let mut chunks = BTreeMap::new();
        let mut frames = BTreeMap::<StateChunkId, Vec<PendingFrame>>::new();
        for frame in selected {
            let Ok(index) = manifest
                .state_frames
                .binary_search_by(|candidate| candidate.key.cmp(&frame.key))
            else {
                return Err(checkpoint_error(format!(
                    "selected state frame {:?} is absent from its manifest",
                    frame.key
                )));
            };
            if manifest.state_frames[index] != *frame {
                return Err(checkpoint_error(format!(
                    "selected state frame {:?} differs from its manifest declaration",
                    frame.key
                )));
            }
            let metadata = declared.get(&frame.chunk).cloned().ok_or_else(|| {
                checkpoint_error(format!(
                    "state frame {:?} references undeclared node object {:?}",
                    frame.key, frame.chunk
                ))
            })?;
            insert_chunk(&mut chunks, frame.chunk, metadata)?;
            frames.entry(frame.chunk).or_default().push(PendingFrame {
                participant_id: manifest.participant_id,
                key: frame.key.clone(),
                range: frame.range,
                sha256: frame.sha256.clone(),
            });
        }
        Ok(Self { chunks, frames })
    }
}

impl<'a> RecoveryManager<'a> {
    /// Bind recovery to one runtime topology, deployment, and outcome domain.
    #[must_use]
    pub fn new(
        store: &'a dyn CheckpointStore,
        pipeline_identity: &PipelineIdentity,
        deployment_id: &str,
        scope: CheckpointScope,
    ) -> Self {
        Self {
            store,
            pipeline_identity: pipeline_identity.clone(),
            deployment_id: deployment_id.to_owned(),
            scope,
        }
    }

    /// Load every exact participant manifest and stage this node's verified state frames.
    ///
    /// Recovery never discovers checkpoints or falls back to an older cut. The caller must load
    /// the committed index through the exact reference carried by `outcome`.
    ///
    /// # Errors
    /// Returns an error when the committed cut or any referenced object fails validation.
    pub async fn recover_committed(
        &self,
        outcome: &CheckpointOutcome,
        committed: &CommittedCheckpointIndex,
    ) -> Result<RecoveredState, DbError> {
        self.validate_cut(outcome, committed)?;

        let checkpoint_id = committed.checkpoint_id;
        let reads = committed.participants.iter().map(|participant| {
            let store = self.store;
            async move {
                store
                    .load_manifest_verified(
                        participant.participant_id,
                        checkpoint_id,
                        participant.manifest_len,
                        &participant.manifest_sha256,
                    )
                    .await
                    .map_err(|error| {
                        checkpoint_error(format!(
                            "participant {} checkpoint {} manifest is unreadable: {error}",
                            participant.participant_id, checkpoint_id
                        ))
                    })?
                    .ok_or_else(|| {
                        checkpoint_error(format!(
                            "participant {} checkpoint {} manifest is missing",
                            participant.participant_id, checkpoint_id
                        ))
                    })
            }
        });
        let mut manifests = futures::stream::iter(reads)
            .buffer_unordered(PARALLEL_MANIFEST_READS)
            .try_collect::<Vec<_>>()
            .await?;
        manifests.sort_unstable_by_key(|manifest| manifest.participant_id);

        self.validate_manifests(committed, &manifests)?;
        let local_participant = self.store.participant_id();
        let local_manifest = manifests
            .iter()
            .find(|manifest| manifest.participant_id == local_participant)
            .ok_or_else(|| {
                checkpoint_error(format!(
                    "local participant {local_participant} is absent from the committed checkpoint"
                ))
            })?;
        let plan = VerifiedStateFramePlan::new(local_manifest, &local_manifest.state_frames)?;
        let state_frames = load_verified_state_frames(self.store, vec![plan]).await?;

        Ok(RecoveredState {
            outcome: outcome.clone(),
            committed: committed.clone(),
            manifests,
            state_frames,
        })
    }

    fn validate_cut(
        &self,
        outcome: &CheckpointOutcome,
        committed: &CommittedCheckpointIndex,
    ) -> Result<(), DbError> {
        committed
            .validate()
            .map_err(|error| checkpoint_error(format!("committed checkpoint index: {error}")))?;
        let (_, observed_reference) = committed
            .encode_and_reference()
            .map_err(|error| checkpoint_error(format!("committed checkpoint index: {error}")))?;
        if outcome.committed_checkpoint.as_ref() != Some(&observed_reference) {
            return Err(checkpoint_error(
                "outcome does not bind the supplied committed checkpoint index",
            ));
        }
        if outcome.verdict != CheckpointVerdict::Commit {
            return Err(checkpoint_error(format!(
                "epoch {} checkpoint {} has an Abort outcome",
                outcome.epoch, outcome.checkpoint_id
            )));
        }
        if outcome.scope != self.scope || committed.scope != self.scope {
            return Err(checkpoint_error(format!(
                "checkpoint scope does not match the active {:?} runtime",
                self.scope
            )));
        }
        if outcome.epoch != committed.epoch
            || outcome.checkpoint_id != committed.checkpoint_id
            || outcome.deployment_id != committed.deployment_id
        {
            return Err(checkpoint_error(
                "outcome does not identify the supplied committed checkpoint index",
            ));
        }
        if committed.pipeline_identity != self.pipeline_identity {
            return Err(checkpoint_error(format!(
                "checkpoint pipeline identity {} does not match runtime identity {}",
                committed.pipeline_identity.sha256, self.pipeline_identity.sha256
            )));
        }
        if committed.deployment_id != self.deployment_id {
            return Err(checkpoint_error(format!(
                "checkpoint deployment '{}' does not match runtime deployment '{}'",
                committed.deployment_id, self.deployment_id
            )));
        }
        if outcome.assignment_fence != committed.assignment_fence {
            return Err(checkpoint_error(
                "outcome and committed index assignment fences differ",
            ));
        }
        match (
            self.scope,
            committed.assignment_fence.as_ref(),
            outcome.leader_proof.as_ref(),
        ) {
            (CheckpointScope::Local, None, None) => {}
            (CheckpointScope::Cluster, Some(fence), Some(proof))
                if proof.is_canonical()
                    && fence.participant_incarnation(proof.owner.node_id)
                        == Some(proof.owner.boot_id) => {}
            _ => {
                return Err(checkpoint_error(
                    "outcome authority is not valid for the committed recovery scope",
                ));
            }
        }
        Ok(())
    }

    fn validate_manifests(
        &self,
        committed: &CommittedCheckpointIndex,
        manifests: &[CheckpointManifest],
    ) -> Result<(), DbError> {
        let encoded = manifests
            .iter()
            .map(|manifest| {
                checkpoint_manifest_bytes(manifest).map_err(|error| {
                    checkpoint_error(format!("encode recovered manifest: {error}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let views = manifests
            .iter()
            .zip(&encoded)
            .map(|(manifest, bytes)| (manifest, bytes.as_slice()))
            .collect::<Vec<_>>();
        committed
            .validate_participant_manifests(&views)
            .map_err(|error| {
                checkpoint_error(format!("committed checkpoint manifests: {error}"))
            })?;

        let mut source_offsets = BTreeMap::<String, ConnectorCheckpoint>::new();
        let mut channel_progress = BTreeMap::<(u64, String), ChannelProgress>::new();
        for manifest in manifests {
            merge_manifest_progress(manifest, &mut source_offsets, &mut channel_progress)?;
        }
        if source_offsets != committed.source_offsets {
            return Err(checkpoint_error(
                "participant source offsets do not exactly reconstruct the committed source cut",
            ));
        }
        let merged_channels = channel_progress.into_values().collect::<Vec<_>>();
        if merged_channels != committed.channel_progress {
            return Err(checkpoint_error(
                "participant channel progress does not exactly reconstruct the committed time cut",
            ));
        }
        Ok(())
    }
}

pub(crate) async fn load_verified_state_frames(
    store: &dyn CheckpointStore,
    plans: Vec<VerifiedStateFramePlan>,
) -> Result<Vec<RecoveredStateFrame>, DbError> {
    let mut chunks = BTreeMap::<StateChunkId, ChunkMetadata>::new();
    let mut frames = BTreeMap::<StateChunkId, Vec<PendingFrame>>::new();

    for plan in plans {
        for (chunk, metadata) in plan.chunks {
            insert_chunk(&mut chunks, chunk, metadata)?;
        }
        for (chunk, mut pending) in plan.frames {
            frames.entry(chunk).or_default().append(&mut pending);
        }
    }

    let work = frames.into_iter().map(|(chunk, requests)| {
        let expected = chunks.remove(&chunk);
        async move {
            let expected = expected.ok_or_else(|| {
                checkpoint_error(format!(
                    "state frames reference undeclared node object {chunk:?}"
                ))
            })?;
            let actual_len = store
                .node_data_len(chunk)
                .await
                .map_err(|error| checkpoint_error(format!("node object {chunk:?}: {error}")))?
                .ok_or_else(|| checkpoint_error(format!("node object {chunk:?} is missing")))?;
            if actual_len != expected.object_length {
                return Err(checkpoint_error(format!(
                    "node object {chunk:?} length {actual_len} differs from declared length {}",
                    expected.object_length
                )));
            }

            let ranges = requests
                .iter()
                .map(|request| request.range)
                .collect::<Vec<_>>();
            let payloads = store
                .load_node_data_ranges(chunk, &ranges)
                .await
                .map_err(|error| checkpoint_error(format!("node object {chunk:?}: {error}")))?
                .ok_or_else(|| checkpoint_error(format!("node object {chunk:?} is missing")))?;
            if payloads.len() != requests.len() {
                return Err(checkpoint_error(format!(
                    "node object {chunk:?} returned an incomplete range set"
                )));
            }

            requests
                .into_iter()
                .zip(payloads)
                .map(|(request, payload)| {
                    let actual = checkpoint_sha256(&payload);
                    if actual != request.sha256 {
                        return Err(checkpoint_error(format!(
                            "state frame {:?} checksum mismatch: expected {}, got {actual}",
                            request.key, request.sha256
                        )));
                    }
                    Ok(RecoveredStateFrame {
                        participant_id: request.participant_id,
                        key: request.key,
                        payload: Bytes::copy_from_slice(&payload),
                    })
                })
                .collect::<Result<Vec<_>, DbError>>()
        }
    });

    let mut recovered = futures::stream::iter(work)
        .buffer_unordered(PARALLEL_CHUNK_READS)
        .try_collect::<Vec<Vec<RecoveredStateFrame>>>()
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    recovered.sort_unstable_by(|left, right| {
        (left.participant_id, &left.key).cmp(&(right.participant_id, &right.key))
    });
    if recovered
        .windows(2)
        .any(|pair| pair[0].participant_id == pair[1].participant_id && pair[0].key == pair[1].key)
    {
        return Err(checkpoint_error(
            "recovered state contains duplicate logical frames",
        ));
    }
    Ok(recovered)
}

fn merge_manifest_progress(
    manifest: &CheckpointManifest,
    source_offsets: &mut BTreeMap<String, ConnectorCheckpoint>,
    channel_progress: &mut BTreeMap<(u64, String), ChannelProgress>,
) -> Result<(), DbError> {
    for (source, local) in &manifest.source_offsets {
        let merged = match source_offsets.entry(source.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(ConnectorCheckpoint {
                    offsets: std::collections::HashMap::new(),
                    metadata: std::collections::HashMap::new(),
                    source_assignment_version: local.source_assignment_version,
                })
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                if entry.get().source_assignment_version != local.source_assignment_version {
                    return Err(checkpoint_error(format!(
                        "participant {} source '{source}' has a conflicting assignment version",
                        manifest.participant_id
                    )));
                }
                entry.into_mut()
            }
        };
        merge_connector_map(
            manifest.participant_id,
            source,
            "offset",
            &mut merged.offsets,
            &local.offsets,
        )?;
        merge_connector_map(
            manifest.participant_id,
            source,
            "metadata",
            &mut merged.metadata,
            &local.metadata,
        )?;
    }

    for channel in &manifest.channel_progress {
        if channel.participant_id != manifest.participant_id {
            return Err(checkpoint_error(format!(
                "participant {} manifest contains channel '{}' owned by participant {}",
                manifest.participant_id, channel.channel_id, channel.participant_id
            )));
        }
        let key = (channel.participant_id, channel.channel_id.clone());
        if let Some(existing) = channel_progress.insert(key, channel.clone()) {
            if existing == *channel {
                continue;
            }
            return Err(checkpoint_error(format!(
                "participant {} channel '{}' has conflicting progress",
                manifest.participant_id, channel.channel_id
            )));
        }
    }
    Ok(())
}

fn merge_connector_map(
    participant_id: u64,
    source: &str,
    field: &str,
    merged: &mut std::collections::HashMap<String, String>,
    local: &std::collections::HashMap<String, String>,
) -> Result<(), DbError> {
    for (key, value) in local {
        if let Some(existing) = merged.insert(key.clone(), value.clone()) {
            if existing != *value {
                return Err(checkpoint_error(format!(
                    "participant {participant_id} source '{source}' has conflicting {field} '{key}'"
                )));
            }
        }
    }
    Ok(())
}

fn insert_chunk(
    chunks: &mut BTreeMap<StateChunkId, ChunkMetadata>,
    chunk: StateChunkId,
    metadata: ChunkMetadata,
) -> Result<(), DbError> {
    match chunks.entry(chunk) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(metadata);
        }
        std::collections::btree_map::Entry::Occupied(entry) => {
            if entry.get() != &metadata {
                return Err(checkpoint_error(format!(
                    "immutable node object {chunk:?} has conflicting metadata"
                )));
            }
        }
    }
    Ok(())
}

fn checkpoint_error(message: impl Into<String>) -> DbError {
    DbError::Checkpoint(format!("[LDB-6041] {}", message.into()))
}

#[cfg(test)]
mod tests;
