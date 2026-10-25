//! Reads an append-only view: its committed segments from the checkpoint store, then the rows
//! held in memory, filtered by the view's retention.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use laminar_core::checkpoint::{
    checkpoint_sha256, ByteRange, CheckpointManifest, CheckpointStore, StateChunkId, ViewLogSegment,
};

use super::{SegmentMeta, ViewRetention};
use crate::error::DbError;
use crate::mv_store::checkpoint::ipc_to_schema_and_batches;

/// Reads committed view-log segments of this participant, caching the last manifest it read.
pub(crate) struct ViewLogReader {
    store: Arc<dyn CheckpointStore>,
    manifest: parking_lot::Mutex<Option<Arc<CheckpointManifest>>>,
}

/// Why a log read could not finish.
#[derive(Debug)]
pub(crate) enum LogReadError {
    /// The checkpoint the read captured was retired before its segments were read; capture
    /// the view again.
    Retired,
    Failed(DbError),
}

impl From<DbError> for LogReadError {
    fn from(error: DbError) -> Self {
        Self::Failed(error)
    }
}

/// Everything one read of an append-only view needs, captured under the store read guard.
pub(crate) struct LogRead {
    pub(in crate::mv_store) view: String,
    pub(in crate::mv_store) schema: SchemaRef,
    pub(in crate::mv_store) retention: ViewRetention,
    pub(in crate::mv_store) frontier: Option<i64>,
    pub(in crate::mv_store) committed: Option<(Arc<ViewLogReader>, u64)>,
    pub(in crate::mv_store) segments: Vec<SegmentMeta>,
    pub(in crate::mv_store) memory: Vec<RecordBatch>,
}

impl ViewLogReader {
    pub(crate) fn new(store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            store,
            manifest: parking_lot::Mutex::new(None),
        }
    }

    async fn manifest(&self, checkpoint_id: u64) -> Result<Arc<CheckpointManifest>, LogReadError> {
        if let Some(manifest) = self
            .manifest
            .lock()
            .as_ref()
            .filter(|manifest| manifest.checkpoint_id == checkpoint_id)
        {
            return Ok(Arc::clone(manifest));
        }
        let manifest = self
            .store
            .load_manifest(checkpoint_id)
            .await
            .map_err(|error| DbError::Checkpoint(format!("load view-log manifest: {error}")))?
            .ok_or(LogReadError::Retired)?;
        let manifest = Arc::new(manifest);
        *self.manifest.lock() = Some(Arc::clone(&manifest));
        Ok(manifest)
    }

    /// The batches of `wanted`, in order, verified against the committed manifest.
    async fn segments(
        &self,
        checkpoint_id: u64,
        view: &str,
        schema: &SchemaRef,
        wanted: &[SegmentMeta],
    ) -> Result<Vec<RecordBatch>, LogReadError> {
        let manifest = self.manifest(checkpoint_id).await?;
        let log = manifest
            .view_logs
            .binary_search_by(|log| log.view.as_str().cmp(view))
            .ok()
            .map(|index| &manifest.view_logs[index])
            .ok_or_else(|| missing(view, "its log"))?;
        let mut refs = Vec::with_capacity(wanted.len());
        for segment in wanted {
            let position = (segment.partition, segment.sequence);
            let found = log
                .segments
                .binary_search_by(|candidate| {
                    (candidate.partition, candidate.sequence).cmp(&position)
                })
                .map_err(|_| missing(view, "a segment it retains"))?;
            refs.push(&log.segments[found]);
        }
        let mut batches = Vec::new();
        for (chunk, run) in chunk_runs(&refs) {
            let length =
                object_length(&manifest, chunk).ok_or_else(|| missing(view, "an object"))?;
            let ranges: Vec<ByteRange> = run.iter().map(|segment| segment.range).collect();
            let payloads = self
                .store
                .load_view_log_ranges(chunk, length, &ranges)
                .await
                .map_err(|error| DbError::Checkpoint(format!("read view log: {error}")))?
                .ok_or(LogReadError::Retired)?;
            for (segment, payload) in run.iter().zip(payloads) {
                batches.extend(decode_segment(view, schema, segment, &payload)?);
            }
        }
        Ok(batches)
    }
}

impl LogRead {
    /// Read the view's committed segments and memory rows, in log order.
    pub(crate) async fn materialize(self) -> Result<RecordBatch, LogReadError> {
        let mut batches = match &self.committed {
            Some((reader, checkpoint_id)) if !self.segments.is_empty() => {
                reader
                    .segments(*checkpoint_id, &self.view, &self.schema, &self.segments)
                    .await?
            }
            _ => Vec::new(),
        };
        batches.extend(self.memory);
        let batches = self.retention.apply(batches, self.frontier)?;
        arrow::compute::concat_batches(&self.schema, &batches).map_err(|error| {
            LogReadError::Failed(DbError::Storage(format!("MV batch concat: {error}")))
        })
    }
}

/// Consecutive segments stored in the same object, so each object is read once per run.
fn chunk_runs<'a>(refs: &[&'a ViewLogSegment]) -> Vec<(StateChunkId, Vec<&'a ViewLogSegment>)> {
    let mut runs: Vec<(StateChunkId, Vec<&ViewLogSegment>)> = Vec::new();
    for segment in refs {
        match runs.last_mut() {
            Some((chunk, run)) if *chunk == segment.chunk => run.push(segment),
            _ => runs.push((segment.chunk, vec![segment])),
        }
    }
    runs
}

fn object_length(manifest: &CheckpointManifest, chunk: StateChunkId) -> Option<u64> {
    if let Some(object) = manifest
        .view_log
        .as_ref()
        .filter(|object| object.chunk == chunk)
    {
        return Some(object.object_length);
    }
    manifest
        .referenced_view_logs
        .iter()
        .find(|reference| reference.chunk == chunk)
        .map(|reference| reference.object_length)
}

fn decode_segment(
    view: &str,
    schema: &SchemaRef,
    segment: &ViewLogSegment,
    payload: &[u8],
) -> Result<Vec<RecordBatch>, DbError> {
    if checkpoint_sha256(payload) != segment.sha256 {
        return Err(DbError::Checkpoint(format!(
            "view '{view}' segment {} does not match its committed digest",
            segment.sequence
        )));
    }
    let (encoded_schema, batches) = ipc_to_schema_and_batches(payload).map_err(|error| {
        DbError::Checkpoint(format!(
            "view '{view}' segment {} cannot be decoded: {error}",
            segment.sequence
        ))
    })?;
    if encoded_schema != *schema {
        return Err(DbError::Checkpoint(format!(
            "view '{view}' segment {} has a different schema",
            segment.sequence
        )));
    }
    Ok(batches)
}

fn missing(view: &str, what: &str) -> LogReadError {
    LogReadError::Failed(DbError::Checkpoint(format!(
        "committed checkpoint does not hold {what} of view '{view}'"
    )))
}
