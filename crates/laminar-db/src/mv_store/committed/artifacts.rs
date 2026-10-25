//! A view's committed artifacts at a pinned cut, from every participant manifest: the vnode
//! frames of a changelog view and the log segments of an append view. Each artifact is checked
//! against its committed digest before it is decoded, and its decoded rows are cached by digest.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::logical_expr::Expr;
use futures::{StreamExt, TryStreamExt};
use laminar_core::checkpoint::{
    checkpoint_sha256, ByteRange, CheckpointManifest, StateChunkId, StateFrameKey, ViewLog,
    ViewLogSegment,
};

use super::cache::Rows;
use super::pruning::ScanPruning;
use super::{CommittedReadError, CommittedViews, PinnedCut, ViewShape};
use crate::error::DbError;
use crate::mv_store::append_log::{LogReadError, SegmentMeta, ViewRetention};
use crate::mv_store::checkpoint::ipc_to_schema_and_batches;
use crate::mv_store::frames::{decode_view_frame, EMPTY_SLICE_FRAME};
use crate::mv_store::{MvStorageMode, ViewKey};

/// Objects one read loads at once.
const MAX_CONCURRENT_OBJECT_READS: usize = 8;

/// Digest of the frame a vnode without rows is captured as; such a frame is never loaded.
static EMPTY_FRAME_SHA256: LazyLock<String> =
    LazyLock::new(|| checkpoint_sha256(&EMPTY_SLICE_FRAME));

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ObjectKind {
    NodeData,
    ViewLog,
}

/// One committed payload: a vnode frame or a log segment.
struct Artifact<'a> {
    kind: ObjectKind,
    chunk: StateChunkId,
    object_length: u64,
    range: ByteRange,
    sha256: &'a str,
    /// The frame's vnode, or the segment's sequence.
    position: u64,
}

impl PinnedCut {
    /// Every row of `shape` at this cut that `filters` may match.
    pub(crate) async fn read_view(
        &self,
        views: &CommittedViews,
        shape: &ViewShape,
        filters: &[Expr],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let pruning = ScanPruning::new(shape, filters);
        let read = async {
            match &shape.mode {
                MvStorageMode::Changelog { key, key_groups } => {
                    let vnode_count = u32::from(key_groups.get());
                    self.read_frames(views, shape, key, vnode_count, &pruning)
                        .await
                }
                MvStorageMode::AppendLog { retention } => {
                    self.read_logs(views, shape, retention, &pruning).await
                }
            }
        };
        tokio::time::timeout_at(tokio::time::Instant::from_std(self.deadline), read)
            .await
            .map_err(|_| CommittedReadError::DeadlineExceeded { epoch: self.epoch })?
    }

    async fn read_frames(
        &self,
        views: &CommittedViews,
        shape: &ViewShape,
        key: &ViewKey,
        vnode_count: u32,
        pruning: &ScanPruning,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let operator_id = format!(
            "graph:{}",
            laminar_core::mv::view_result_operator(&shape.name)
        );
        let mut frames = BTreeMap::new();
        for manifest in self.manifests.iter() {
            for frame in &manifest.state_frames {
                let StateFrameKey::Vnode {
                    operator_id: id,
                    vnode,
                } = &frame.key
                else {
                    continue;
                };
                if *id != operator_id {
                    continue;
                }
                let artifact = Artifact {
                    kind: ObjectKind::NodeData,
                    chunk: frame.chunk,
                    object_length: node_data_length(manifest, frame.chunk)?,
                    range: frame.range,
                    sha256: &frame.sha256,
                    position: u64::from(*vnode),
                };
                if frames.insert(*vnode, artifact).is_some() {
                    return Err(self.inconsistent(shape, "lists a vnode frame twice"));
                }
            }
        }
        if frames.is_empty() {
            return Err(self.not_committed(shape));
        }
        let expected = if *key == ViewKey::Global {
            1
        } else {
            vnode_count
        };
        let complete = frames.len() == usize::try_from(expected).unwrap_or(usize::MAX)
            && frames
                .last_key_value()
                .is_some_and(|(vnode, _)| u32::from(*vnode) + 1 == expected);
        if !complete {
            return Err(self.inconsistent(shape, "does not list a frame for every vnode"));
        }
        let wanted: Vec<_> = frames
            .into_values()
            .filter(|frame| frame.sha256 != EMPTY_FRAME_SHA256.as_str())
            .filter(|frame| pruning.keeps_vnode(frame.position))
            .collect();
        let rows = self
            .load(views, &shape.schema, &wanted, |frame, bytes| {
                let vnode = u32::try_from(frame.position).unwrap_or(u32::MAX);
                decode_view_frame(shape, vnode, vnode_count, bytes).map(|batch| vec![batch])
            })
            .await?;
        Ok(rows.iter().flat_map(|rows| rows.iter().cloned()).collect())
    }

    async fn read_logs(
        &self,
        views: &CommittedViews,
        shape: &ViewShape,
        retention: &ViewRetention,
        pruning: &ScanPruning,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut found = false;
        let mut batches = Vec::new();
        for manifest in self.manifests.iter() {
            let Some(log) = view_log(manifest, &shape.name) else {
                continue;
            };
            found = true;
            for partition in log
                .segments
                .chunk_by(|left, right| left.partition == right.partition)
            {
                let wanted = partition
                    .iter()
                    .filter(|segment| match retention {
                        ViewRetention::EventTime { .. } => {
                            !retention.expired(segment.event_time, log.frontier)
                                && pruning.keeps_segment(segment.event_time)
                        }
                        ViewRetention::Unbounded { .. } => {
                            pruning.keeps_segment(segment.event_time)
                        }
                        // A row cap keeps the newest rows of the whole partition, so every
                        // segment is read before the cap and the filters apply.
                        ViewRetention::Rows(_) => true,
                    })
                    .map(|segment| segment_artifact(manifest, segment))
                    .collect::<Result<Vec<_>, _>>()?;
                let rows = self
                    .load(views, &shape.schema, &wanted, |segment, bytes| {
                        decode_segment(&shape.name, &shape.schema, segment.position, bytes)
                    })
                    .await?;
                let rows = rows.iter().flat_map(|rows| rows.iter().cloned()).collect();
                batches.extend(retention.apply(rows, log.frontier)?);
            }
        }
        if !found {
            return Err(self.not_committed(shape));
        }
        Ok(batches)
    }

    /// This participant's log of `view` at this cut, with the manifest that lists it.
    fn local(&self, views: &CommittedViews, view: &str) -> Option<(&CheckpointManifest, &ViewLog)> {
        let participant = views.store.participant_id();
        self.manifests
            .iter()
            .filter(|manifest| manifest.participant_id == participant)
            .find_map(|manifest| Some((manifest, view_log(manifest, view)?)))
    }

    /// This participant's log of `view` at this cut.
    pub(in crate::mv_store) fn local_log(
        &self,
        views: &CommittedViews,
        view: &str,
    ) -> Option<&ViewLog> {
        self.local(views, view).map(|(_, log)| log)
    }

    /// The batches of `wanted` segments of this participant's log of `view`, in order.
    pub(in crate::mv_store) async fn local_segments(
        &self,
        views: &CommittedViews,
        view: &str,
        schema: &SchemaRef,
        wanted: &[SegmentMeta],
    ) -> Result<Vec<RecordBatch>, LogReadError> {
        let missing = |what: &str| {
            LogReadError::Failed(DbError::Checkpoint(format!(
                "committed checkpoint does not hold {what} of view '{view}'"
            )))
        };
        let (manifest, log) = self.local(views, view).ok_or_else(|| missing("its log"))?;
        let mut artifacts = Vec::with_capacity(wanted.len());
        for segment in wanted {
            let position = (segment.partition, segment.sequence);
            let found = log
                .segments
                .binary_search_by(|candidate| {
                    (candidate.partition, candidate.sequence).cmp(&position)
                })
                .map_err(|_| missing("a segment it retains"))?;
            artifacts.push(segment_artifact(manifest, &log.segments[found])?);
        }
        let rows = self
            .load(views, schema, &artifacts, |segment, bytes| {
                decode_segment(view, schema, segment.position, bytes)
            })
            .await?;
        Ok(rows.iter().flat_map(|rows| rows.iter().cloned()).collect())
    }

    /// The decoded rows of `artifacts`, in order: cached rows by digest, the rest loaded with
    /// one ranged read per object and verified before `decode` runs.
    async fn load(
        &self,
        views: &CommittedViews,
        schema: &SchemaRef,
        artifacts: &[Artifact<'_>],
        decode: impl Fn(&Artifact<'_>, &[u8]) -> Result<Vec<RecordBatch>, DbError>,
    ) -> Result<Vec<Rows>, DbError> {
        let mut rows: Vec<Option<Rows>> = artifacts
            .iter()
            .map(|artifact| views.artifacts.get(artifact.sha256, schema))
            .collect();
        let mut objects = BTreeMap::<(ObjectKind, StateChunkId), (u64, Vec<usize>)>::new();
        for (position, artifact) in artifacts.iter().enumerate() {
            if rows[position].is_none() {
                objects
                    .entry((artifact.kind, artifact.chunk))
                    .or_insert_with(|| (artifact.object_length, Vec::new()))
                    .1
                    .push(position);
            }
        }
        let loaded = futures::stream::iter(objects)
            .map(|((kind, chunk), (length, members))| async move {
                let ranges: Vec<ByteRange> = members
                    .iter()
                    .map(|&position| artifacts[position].range)
                    .collect();
                let payloads = load_ranges(views, kind, chunk, length, &ranges).await?;
                Ok::<_, DbError>((members, payloads))
            })
            .buffer_unordered(MAX_CONCURRENT_OBJECT_READS)
            .try_collect::<Vec<_>>()
            .await?;
        for (members, payloads) in loaded {
            for (position, payload) in members.into_iter().zip(payloads) {
                let artifact = &artifacts[position];
                if checkpoint_sha256(&payload) != artifact.sha256 {
                    return Err(DbError::Checkpoint(format!(
                        "committed artifact {:?} at {:?} does not match its digest",
                        artifact.chunk, artifact.range
                    )));
                }
                let decoded: Rows = decode(artifact, &payload)?.into();
                views
                    .artifacts
                    .insert(artifact.sha256, Arc::clone(&decoded));
                rows[position] = Some(decoded);
            }
        }
        rows.into_iter()
            .map(|rows| {
                rows.ok_or_else(|| DbError::Checkpoint("committed artifact was not loaded".into()))
            })
            .collect()
    }

    fn not_committed(&self, shape: &ViewShape) -> DbError {
        CommittedReadError::ViewNotCommitted {
            view: shape.name.clone(),
            epoch: self.epoch,
        }
        .into()
    }

    fn inconsistent(&self, shape: &ViewShape, problem: &str) -> DbError {
        DbError::Checkpoint(format!(
            "committed checkpoint {} of view '{}' {problem}",
            self.checkpoint_id, shape.name
        ))
    }
}

async fn load_ranges(
    views: &CommittedViews,
    kind: ObjectKind,
    chunk: StateChunkId,
    length: u64,
    ranges: &[ByteRange],
) -> Result<Vec<bytes::Bytes>, DbError> {
    let loaded = match kind {
        ObjectKind::NodeData => {
            views
                .store
                .load_node_data_ranges(chunk, length, ranges)
                .await
        }
        ObjectKind::ViewLog => {
            views
                .store
                .load_view_log_ranges(chunk, length, ranges)
                .await
        }
    };
    let payloads = loaded
        .map_err(|error| DbError::Checkpoint(format!("read committed view state: {error}")))?
        .ok_or_else(|| {
            DbError::Checkpoint(format!(
                "pinned committed object {chunk:?} is missing from the checkpoint store"
            ))
        })?;
    if payloads.len() != ranges.len() {
        return Err(DbError::Checkpoint(format!(
            "committed object {chunk:?} returned {} of {} ranges",
            payloads.len(),
            ranges.len()
        )));
    }
    Ok(payloads)
}

fn view_log<'a>(manifest: &'a CheckpointManifest, view: &str) -> Option<&'a ViewLog> {
    manifest
        .view_logs
        .binary_search_by(|log| log.view.as_str().cmp(view))
        .ok()
        .map(|index| &manifest.view_logs[index])
}

fn node_data_length(manifest: &CheckpointManifest, chunk: StateChunkId) -> Result<u64, DbError> {
    if manifest.node_data.chunk == chunk {
        return Ok(manifest.node_data.object_length);
    }
    manifest
        .referenced_chunks
        .iter()
        .find(|reference| reference.chunk == chunk)
        .map(|reference| reference.object_length)
        .ok_or_else(|| untracked(chunk))
}

fn segment_artifact<'a>(
    manifest: &CheckpointManifest,
    segment: &'a ViewLogSegment,
) -> Result<Artifact<'a>, DbError> {
    let object_length = manifest
        .view_log
        .as_ref()
        .filter(|object| object.chunk == segment.chunk)
        .map(|object| object.object_length)
        .or_else(|| {
            manifest
                .referenced_view_logs
                .iter()
                .find(|reference| reference.chunk == segment.chunk)
                .map(|reference| reference.object_length)
        })
        .ok_or_else(|| untracked(segment.chunk))?;
    Ok(Artifact {
        kind: ObjectKind::ViewLog,
        chunk: segment.chunk,
        object_length,
        range: segment.range,
        sha256: &segment.sha256,
        position: segment.sequence,
    })
}

fn untracked(chunk: StateChunkId) -> DbError {
    DbError::Checkpoint(format!(
        "committed manifest references untracked object {chunk:?}"
    ))
}

fn decode_segment(
    view: &str,
    schema: &SchemaRef,
    sequence: u64,
    payload: &[u8],
) -> Result<Vec<RecordBatch>, DbError> {
    let (encoded_schema, batches) = ipc_to_schema_and_batches(payload).map_err(|error| {
        DbError::Checkpoint(format!(
            "view '{view}' segment {sequence} cannot be decoded: {error}"
        ))
    })?;
    if encoded_schema != *schema {
        return Err(DbError::Checkpoint(format!(
            "view '{view}' segment {sequence} has a different schema"
        )));
    }
    Ok(batches)
}
