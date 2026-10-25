//! Checkpoint capture of append-only view logs, and the deterministic row encodings shared
//! with changelog-view vnode frames.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
#[cfg(test)]
use arrow::ipc::writer::StreamWriter;
use arrow::row::{OwnedRow, RowConverter};
use datafusion_common::ScalarValue;
use laminar_core::checkpoint::{CheckpointManifest, ViewLog, MAX_VIEW_LOG_SEGMENTS};

use super::append_log::{AppendLogState, ViewLogCapture};
use super::{MvStorageMode, MvStore};
use crate::checkpoint_coordinator::{CapturedViewLog, CapturedViewLogSegment};
use crate::error::DbError;

const MULTISET_CHECKPOINT_COUNT_COLUMN: &str = "__laminardb_multiset_count";
const MULTISET_CHECKPOINT_FORMAT_KEY: &str = "laminardb.mv.multiset.format";
const MULTISET_CHECKPOINT_FORMAT_VERSION: &str = "counted-v1";

const CHECKPOINT_CAPTURE_ENTRY_OVERHEAD: usize = 256;
pub(super) const CHECKPOINT_CAPTURE_ROW_OVERHEAD: usize = 64;

/// Every append view's log at one checkpoint barrier; new rows are encoded off the cycle.
pub(crate) struct MvCheckpointCapture {
    logs: Vec<(String, SchemaRef, ViewLogCapture)>,
    estimated_bytes: u64,
}

impl MvCheckpointCapture {
    pub(crate) const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    /// Whether the capture moved rows out of a view's stage, so a failed checkpoint must
    /// recover rather than retry.
    pub(crate) fn sealed_rows(&self) -> bool {
        self.logs.iter().any(|(_, _, log)| log.sealed.is_some())
    }

    /// Encode each sealed segment as one Arrow IPC stream, within `max_encoded_bytes` of
    /// retained payload. Returns the logs and the bytes their payloads retain.
    pub(crate) fn encode(
        self,
        max_encoded_bytes: u64,
    ) -> Result<(Vec<CapturedViewLog>, u64), DbError> {
        let mut retained_bytes = 0u64;
        let mut logs = Vec::with_capacity(self.logs.len());
        for (view, schema, capture) in self.logs {
            let mut segments: Vec<CapturedViewLogSegment> = capture
                .retained
                .iter()
                .map(|&(partition, sequence)| CapturedViewLogSegment::Retained {
                    partition,
                    sequence,
                })
                .collect();
            if let Some(compacted) = capture.compacted {
                retained_bytes = retained_bytes
                    .checked_add(u64::try_from(compacted.payload.len()).unwrap_or(u64::MAX))
                    .filter(|bytes| *bytes <= max_encoded_bytes)
                    .ok_or_else(|| capture_size_overflow(&view))?;
                segments.push(CapturedViewLogSegment::New {
                    partition: compacted.meta.partition,
                    sequence: compacted.meta.sequence,
                    rows: compacted.meta.rows,
                    event_time: compacted.meta.event_time,
                    payload: compacted.payload,
                });
            }
            if let Some(sealed) = capture.sealed {
                let remaining = max_encoded_bytes.checked_sub(retained_bytes).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "view-log serialization exceeded its staged-state budget of {max_encoded_bytes} bytes"
                    ))
                })?;
                let payload = batches_to_ipc_bounded(&schema, &sealed.batches, remaining).map_err(
                    |error| {
                        DbError::Checkpoint(format!(
                            "view '{view}' log serialization failed: {error}"
                        ))
                    },
                )?;
                // `Bytes::from(Vec)` retains the Vec allocation, so charge its capacity.
                retained_bytes = retained_bytes
                    .checked_add(u64::try_from(payload.capacity()).unwrap_or(u64::MAX))
                    .filter(|bytes| *bytes <= max_encoded_bytes)
                    .ok_or_else(|| capture_size_overflow(&view))?;
                segments.push(CapturedViewLogSegment::New {
                    partition: sealed.partition,
                    sequence: sealed.sequence,
                    rows: sealed.rows,
                    event_time: sealed.event_time,
                    payload: bytes::Bytes::from(payload),
                });
            }
            logs.push(CapturedViewLog {
                view,
                next_sequence: capture.next_sequence,
                frontier: capture.frontier,
                segments,
            });
        }
        Ok((logs, retained_bytes))
    }
}

pub(super) fn multiset_checkpoint_schema(schema: &SchemaRef) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        MULTISET_CHECKPOINT_COUNT_COLUMN,
        DataType::Int64,
        false,
    )));
    let mut metadata = schema.metadata().clone();
    metadata.insert(
        MULTISET_CHECKPOINT_FORMAT_KEY.to_string(),
        MULTISET_CHECKPOINT_FORMAT_VERSION.to_string(),
    );
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

/// Rows sorted by key, so equal state always encodes to equal bytes.
pub(super) fn upsert_checkpoint_batch(
    schema: &SchemaRef,
    rows: &mut Vec<(OwnedRow, Vec<ScalarValue>)>,
) -> Result<RecordBatch, DbError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    rows.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    let column_count = schema.fields().len();
    let mut columns: Vec<Vec<ScalarValue>> = (0..column_count)
        .map(|_| Vec::with_capacity(rows.len()))
        .collect();
    for (_, values) in rows.drain(..) {
        if values.len() != column_count {
            return Err(DbError::Storage(
                "upsert MV checkpoint row width does not match its schema".into(),
            ));
        }
        for (column, value) in columns.iter_mut().zip(values) {
            column.push(value);
        }
    }
    let arrays = columns
        .into_iter()
        .map(|column| {
            ScalarValue::iter_to_array(column)
                .map_err(|error| DbError::Storage(format!("upsert MV column build: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|error| DbError::Storage(format!("upsert MV batch assembly: {error}")))
}

/// Distinct rows sorted by their encoding, each with its positive multiplicity.
pub(super) fn multiset_counted_checkpoint_batch(
    schema: &SchemaRef,
    row_converter: &RowConverter,
    counts: &mut [(OwnedRow, i64)],
) -> Result<RecordBatch, DbError> {
    let checkpoint_schema = multiset_checkpoint_schema(schema);
    if counts.is_empty() {
        return Ok(RecordBatch::new_empty(checkpoint_schema));
    }
    counts.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    if counts.iter().any(|(_, count)| *count <= 0) {
        return Err(DbError::Storage(
            "multiset MV contains an invalid checkpoint multiplicity".into(),
        ));
    }
    let mut arrays = super::multiset::decode_rows(
        row_converter,
        schema,
        counts.iter().map(|(key, _)| key.row()),
    )?;
    arrays.push(Arc::new(Int64Array::from_iter_values(
        counts.iter().map(|(_, count)| *count),
    )));
    RecordBatch::try_new(checkpoint_schema, arrays)
        .map_err(|error| DbError::Storage(format!("multiset MV checkpoint assembly: {error}")))
}

impl MvStore {
    fn append_logs(&mut self) -> impl Iterator<Item = (&String, &SchemaRef, &mut AppendLogState)> {
        self.entries
            .iter_mut()
            .filter_map(|(name, entry)| Some((name, &entry.schema, entry.log.as_mut()?)))
    }

    /// Seal every append view's staged rows for checkpoint `checkpoint_id`, with the prepared
    /// merges when they fit within `max_bytes` as well. Nothing changes when the capture does
    /// not fit.
    pub(crate) fn capture_view_logs(
        &mut self,
        checkpoint_id: u64,
        max_bytes: u64,
    ) -> Result<MvCheckpointCapture, DbError> {
        let (mut estimated_bytes, mut merged_bytes) = (0u64, 0u64);
        for (name, entry) in &self.entries {
            if let Some(log) = &entry.log {
                let bytes = CHECKPOINT_CAPTURE_ENTRY_OVERHEAD
                    .saturating_add(name.len())
                    .saturating_add(log.staged_bytes());
                estimated_bytes = estimated_bytes
                    .checked_add(u64::try_from(bytes).map_err(|_| capture_size_overflow(name))?)
                    .ok_or_else(|| capture_size_overflow(name))?;
                merged_bytes = merged_bytes
                    .saturating_add(u64::try_from(log.prepared_bytes()).unwrap_or(u64::MAX));
            }
        }
        if estimated_bytes > max_bytes {
            return Err(DbError::Checkpoint(format!(
                "view-log capture estimate {estimated_bytes} bytes exceeds the staged-state cap of {max_bytes} bytes"
            )));
        }
        let compact = estimated_bytes.saturating_add(merged_bytes) <= max_bytes;
        if compact {
            estimated_bytes += merged_bytes;
        }
        let captured: Result<Vec<_>, DbError> = self
            .append_logs()
            .map(|(name, schema, log)| {
                let capture = log.capture(checkpoint_id, compact)?;
                Ok((name.clone(), Arc::clone(schema), capture))
            })
            .collect();
        match captured.and_then(within_segment_limit) {
            Ok(logs) => {
                self.log_capture = Some(checkpoint_id);
                Ok(MvCheckpointCapture {
                    logs,
                    estimated_bytes,
                })
            }
            Err(error) => {
                self.abort_view_logs(checkpoint_id);
                Err(error)
            }
        }
    }

    /// Install the segments checkpoint `checkpoint_id` committed; its sealed rows leave memory.
    pub(crate) fn commit_view_logs(&mut self, checkpoint_id: u64) {
        if self
            .log_capture
            .take_if(|capture| *capture == checkpoint_id)
            .is_none()
        {
            return;
        }
        for (_, _, log) in self.append_logs() {
            log.commit(checkpoint_id);
        }
        self.log_checkpoint = Some(checkpoint_id);
        self.recount_log_bytes();
    }

    /// Return the rows a failed checkpoint sealed to their views' stages.
    pub(crate) fn abort_view_logs(&mut self, checkpoint_id: u64) {
        self.log_capture
            .take_if(|capture| *capture == checkpoint_id);
        for (_, _, log) in self.append_logs() {
            log.abort(checkpoint_id);
        }
    }

    fn recount_log_bytes(&mut self) {
        for entry in self.entries.values_mut() {
            if let Some(log) = &entry.log {
                entry.approx_bytes = log.bytes;
            }
        }
    }

    /// Restore every append view from the log `manifest` retains; with no manifest, every
    /// append view starts empty.
    pub(super) fn restore_view_logs(
        &mut self,
        manifest: Option<&CheckpointManifest>,
    ) -> Result<(), DbError> {
        let Some(manifest) = manifest else {
            return Ok(());
        };
        let mut logs: HashMap<&str, &ViewLog> = manifest
            .view_logs
            .iter()
            .map(|log| (log.view.as_str(), log))
            .collect();
        for (name, entry) in &mut self.entries {
            let MvStorageMode::AppendLog { retention } = &entry.mode else {
                continue;
            };
            let log = logs.remove(name.as_str()).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "checkpoint holds no log for append-only view '{name}'"
                ))
            })?;
            entry.log = Some(AppendLogState::restored(
                retention.clone(),
                log,
                manifest.checkpoint_id,
            ));
        }
        if let Some(view) = logs.keys().next() {
            return Err(DbError::Checkpoint(format!(
                "checkpoint holds a log for '{view}', which is not an append-only view"
            )));
        }
        self.log_checkpoint = Some(manifest.checkpoint_id);
        Ok(())
    }
}

/// The captured logs, unless they retain more segments than one manifest admits.
fn within_segment_limit(
    logs: Vec<(String, SchemaRef, ViewLogCapture)>,
) -> Result<Vec<(String, SchemaRef, ViewLogCapture)>, DbError> {
    let segments = |capture: &ViewLogCapture| {
        capture.retained.len()
            + usize::from(capture.compacted.is_some())
            + usize::from(capture.sealed.is_some())
    };
    let total = logs
        .iter()
        .map(|(_, _, capture)| segments(capture))
        .sum::<usize>();
    if total <= MAX_VIEW_LOG_SEGMENTS {
        return Ok(logs);
    }
    let (view, held) = logs
        .iter()
        .map(|(view, _, capture)| (view.as_str(), segments(capture)))
        .max_by_key(|(_, held)| *held)
        .unwrap_or_default();
    Err(DbError::Checkpoint(format!(
        "append-only views retain {total} log segments, above the {MAX_VIEW_LOG_SEGMENTS} one \
         checkpoint admits; view '{view}' retains {held}"
    )))
}

fn capture_size_overflow(name: &str) -> DbError {
    DbError::Checkpoint(format!(
        "MV '{name}' checkpoint capture size estimate overflowed"
    ))
}

#[cfg(test)]
pub(super) fn batches_to_ipc<'a, I>(schema: &SchemaRef, batches: I) -> Result<Vec<u8>, DbError>
where
    I: IntoIterator<Item = &'a RecordBatch>,
{
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| DbError::Storage(format!("IPC write: {e}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| DbError::Storage(format!("IPC write: {e}")))?;
    }
    writer
        .finish()
        .map_err(|e| DbError::Storage(format!("IPC finish: {e}")))?;
    Ok(buf)
}

pub(super) fn batches_to_ipc_bounded<'a, I>(
    schema: &SchemaRef,
    batches: I,
    max_bytes: u64,
) -> Result<Vec<u8>, DbError>
where
    I: IntoIterator<Item = &'a RecordBatch>,
{
    let max_bytes = usize::try_from(max_bytes).unwrap_or(usize::MAX);
    laminar_core::serialization::serialize_batches_stream_bounded(
        schema.as_ref(),
        batches,
        max_bytes,
    )
    .map_err(|error| DbError::Storage(format!("IPC write: {error}")))
}

pub(super) fn ipc_to_schema_and_batches(
    bytes: &[u8],
) -> Result<(SchemaRef, Vec<RecordBatch>), arrow::error::ArrowError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}
