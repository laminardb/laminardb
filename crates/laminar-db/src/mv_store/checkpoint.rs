//! Whole-frame checkpoint capture for append views, and the deterministic row encodings shared
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

use super::{MvEntry, MvStorageMode, MvStore};
use crate::error::DbError;

const MULTISET_CHECKPOINT_COUNT_COLUMN: &str = "__laminardb_multiset_count";
const MULTISET_CHECKPOINT_FORMAT_KEY: &str = "laminardb.mv.multiset.format";
const MULTISET_CHECKPOINT_FORMAT_VERSION: &str = "counted-v1";

const CHECKPOINT_CAPTURE_ENTRY_OVERHEAD: usize = 256;
const CHECKPOINT_CAPTURE_FIELD_OVERHEAD: usize = 128;
pub(super) const CHECKPOINT_CAPTURE_ROW_OVERHEAD: usize = 64;

/// Prefix for append-view whole-frame checkpoint identifiers.
pub(crate) const CHECKPOINT_KEY_PREFIX: &str = "mv:";

struct AppendCapture {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

pub(crate) struct MvCheckpointCapture {
    entries: Vec<(String, AppendCapture)>,
    estimated_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct EncodedMvCheckpoint {
    states: HashMap<String, bytes::Bytes>,
    retained_bytes: u64,
}

impl EncodedMvCheckpoint {
    pub(crate) fn into_parts(self) -> (HashMap<String, bytes::Bytes>, u64) {
        (self.states, self.retained_bytes)
    }

    #[cfg(test)]
    pub(super) fn states(&self) -> &HashMap<String, bytes::Bytes> {
        &self.states
    }
}

impl MvCheckpointCapture {
    pub(crate) const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    pub(crate) fn encode(mut self, max_encoded_bytes: u64) -> Result<EncodedMvCheckpoint, DbError> {
        self.entries
            .sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut out = HashMap::with_capacity(self.entries.len());
        let mut retained_bytes = 0u64;
        for (name, entry) in self.entries {
            let remaining_bytes = max_encoded_bytes.checked_sub(retained_bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "MV checkpoint serialization exceeded its staged-state budget of {max_encoded_bytes} bytes"
                ))
            })?;
            let bytes = batches_to_ipc_bounded(&entry.schema, &entry.batches, remaining_bytes)
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "MV '{name}' checkpoint serialization failed: {error}"
                    ))
                })?;
            // `Bytes::from(Vec)` retains the Vec allocation, so charge capacity rather than
            // payload length to keep the aggregate live allocation within the caller's cap.
            let retained_entry_bytes =
                u64::try_from(bytes.capacity()).map_err(|_| capture_size_overflow(&name))?;
            retained_bytes = retained_bytes
                .checked_add(retained_entry_bytes)
                .ok_or_else(|| capture_size_overflow(&name))?;
            debug_assert!(retained_bytes <= max_encoded_bytes);
            out.insert(
                format!("{CHECKPOINT_KEY_PREFIX}{name}"),
                bytes::Bytes::from(bytes),
            );
        }
        Ok(EncodedMvCheckpoint {
            states: out,
            retained_bytes,
        })
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

impl MvEntry {
    fn checkpoint_capture_estimate(&self, name: &str) -> Result<u64, DbError> {
        let mut bytes = 0;
        add_capture_estimate(&mut bytes, CHECKPOINT_CAPTURE_ENTRY_OVERHEAD, name)?;
        add_capture_estimate(&mut bytes, name.len(), name)?;
        for field in self.schema.fields() {
            add_capture_estimate(&mut bytes, CHECKPOINT_CAPTURE_FIELD_OVERHEAD, name)?;
            add_capture_estimate(&mut bytes, field.name().len(), name)?;
        }
        for (key, value) in self.schema.metadata() {
            add_capture_estimate(&mut bytes, key.len(), name)?;
            add_capture_estimate(&mut bytes, value.len(), name)?;
        }

        for batch in &self.batches {
            add_capture_estimate(&mut bytes, std::mem::size_of::<RecordBatch>(), name)?;
            add_capture_estimate(&mut bytes, batch.get_array_memory_size(), name)?;
        }
        Ok(bytes.max(u64::try_from(self.approx_bytes).map_err(|_| capture_size_overflow(name))?))
    }
}

impl MvStore {
    fn append_entries(&self) -> impl Iterator<Item = (&String, &MvEntry)> {
        self.entries
            .iter()
            .filter(|(_, entry)| matches!(entry.mode, MvStorageMode::Append { .. }))
    }

    pub(super) fn checkpoint_capture_estimated_bytes(&self) -> Result<u64, DbError> {
        self.append_entries()
            .try_fold(0u64, |total, (name, entry)| {
                total
                    .checked_add(entry.checkpoint_capture_estimate(name)?)
                    .ok_or_else(|| capture_size_overflow(name))
            })
    }

    /// Capture an immutable image of every append view. Changelog views checkpoint as vnode
    /// frames through their graph nodes instead.
    pub fn capture_checkpoint(&self, max_bytes: u64) -> Result<MvCheckpointCapture, DbError> {
        let estimated_bytes = self.checkpoint_capture_estimated_bytes()?;
        if estimated_bytes > max_bytes {
            return Err(DbError::Checkpoint(format!(
                "MV checkpoint capture estimate {estimated_bytes} bytes exceeds the staged-state cap of {max_bytes} bytes"
            )));
        }

        let entries = self
            .append_entries()
            .map(|(name, entry)| {
                let capture = AppendCapture {
                    schema: Arc::clone(&entry.schema),
                    batches: entry.batches.iter().cloned().collect(),
                };
                (name.clone(), capture)
            })
            .collect();
        Ok(MvCheckpointCapture {
            entries,
            estimated_bytes,
        })
    }

    #[cfg(test)]
    pub fn checkpoint_states(&self) -> Result<HashMap<String, bytes::Bytes>, DbError> {
        self.capture_checkpoint(u64::MAX)?
            .encode(u64::MAX)
            .map(|encoded| encoded.into_parts().0)
    }

    pub(super) fn restore_from_ipc(&mut self, name: &str, bytes: &[u8]) -> Result<(), DbError> {
        let Some(entry) = self.entries.get_mut(name) else {
            return Err(DbError::Storage(format!("MV '{name}' is not registered")));
        };
        let MvStorageMode::Append { max_batches } = entry.mode else {
            return Err(DbError::Storage(format!(
                "MV '{name}' is a changelog view; its state is restored from vnode frames"
            )));
        };
        let (checkpoint_schema, batches) = ipc_to_schema_and_batches(bytes)
            .map_err(|e| DbError::Storage(format!("MV restore '{name}': {e}")))?;

        // COMPAT: reject stale checkpoints from before a schema change rather than admitting
        // data that will fail or be misinterpreted during a later materialization.
        if checkpoint_schema != entry.schema {
            return Err(DbError::Storage(format!(
                "MV '{name}' schema mismatch on restore"
            )));
        }

        let (rows, restored_bytes) = super::admission::batch_usage(batches.iter())?;
        self.limits.validate(name, rows, restored_bytes)?;
        // Restore rejects over-limit state instead of silently evicting committed rows.
        if batches.iter().filter(|batch| batch.num_rows() > 0).count() > max_batches.max(1) {
            return Err(DbError::Storage(format!(
                "MV '{name}' checkpoint exceeds append batch retention"
            )));
        }
        entry.batches = batches
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();
        entry.rows = rows;
        entry.approx_bytes = restored_bytes;
        Ok(())
    }
}

fn capture_size_overflow(name: &str) -> DbError {
    DbError::Checkpoint(format!(
        "MV '{name}' checkpoint capture size estimate overflowed"
    ))
}

fn add_capture_estimate(total: &mut u64, bytes: usize, name: &str) -> Result<(), DbError> {
    let bytes = u64::try_from(bytes).map_err(|_| capture_size_overflow(name))?;
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| capture_size_overflow(name))?;
    Ok(())
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
