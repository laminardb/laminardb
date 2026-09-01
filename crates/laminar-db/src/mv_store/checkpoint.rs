//! Checkpoint capture, deterministic encoding, and restore validation for materialized views.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
#[cfg(test)]
use arrow::ipc::writer::StreamWriter;
use arrow::row::{OwnedRow, RowConverter};
use datafusion_common::ScalarValue;

use super::{MultisetState, MvEntry, MvStorageMode, MvStore, UpsertState};
use crate::error::DbError;

const MULTISET_CHECKPOINT_COUNT_COLUMN: &str = "__laminardb_multiset_count";
const MULTISET_CHECKPOINT_FORMAT_KEY: &str = "laminardb.mv.multiset.format";
const MULTISET_CHECKPOINT_FORMAT_VERSION: &str = "counted-v1";

const CHECKPOINT_CAPTURE_ENTRY_OVERHEAD: usize = 256;
const CHECKPOINT_CAPTURE_FIELD_OVERHEAD: usize = 128;
const CHECKPOINT_CAPTURE_ROW_OVERHEAD: usize = 64;

/// Prefix for materialized-view checkpoint frame identifiers.
pub(crate) const CHECKPOINT_KEY_PREFIX: &str = "mv:";

enum MvCheckpointEntryCapture {
    Batches {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
    Upsert {
        schema: SchemaRef,
        rows: Vec<(OwnedRow, Vec<ScalarValue>)>,
    },
    Multiset {
        schema: SchemaRef,
        row_converter: Arc<RowConverter>,
        counts: Vec<(OwnedRow, i64)>,
    },
}

pub(crate) struct MvCheckpointCapture {
    entries: Vec<(String, MvCheckpointEntryCapture)>,
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
            let bytes = match entry {
                MvCheckpointEntryCapture::Batches { schema, batches } => {
                    batches_to_ipc_bounded(&schema, &batches, remaining_bytes)
                }
                MvCheckpointEntryCapture::Upsert { schema, mut rows } => {
                    upsert_checkpoint_batch(&schema, &mut rows).and_then(|batch| {
                        batches_to_ipc_bounded(&schema, std::iter::once(&batch), remaining_bytes)
                    })
                }
                MvCheckpointEntryCapture::Multiset {
                    schema,
                    row_converter,
                    mut counts,
                } => multiset_counted_checkpoint_batch(&schema, &row_converter, &mut counts)
                    .and_then(|batch| {
                        batches_to_ipc_bounded(
                            &batch.schema(),
                            std::iter::once(&batch),
                            remaining_bytes,
                        )
                    }),
            }
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

fn upsert_checkpoint_batch(
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

fn multiset_counted_checkpoint_batch(
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
    let mut arrays = row_converter
        .convert_rows(counts.iter().map(|(key, _)| key.row()))
        .map_err(|error| DbError::Storage(format!("multiset MV checkpoint conversion: {error}")))?;
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

        match &self.mode {
            MvStorageMode::Aggregate | MvStorageMode::Append { .. } => {
                for batch in &self.batches {
                    add_capture_estimate(&mut bytes, std::mem::size_of::<RecordBatch>(), name)?;
                    add_capture_estimate(&mut bytes, batch.get_array_memory_size(), name)?;
                }
            }
            MvStorageMode::Upsert { .. } => {
                let upsert = self.upsert.as_ref().ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "MV '{name}' is missing its upsert checkpoint state"
                    ))
                })?;
                for (key, row) in &upsert.rows {
                    add_capture_estimate(&mut bytes, CHECKPOINT_CAPTURE_ROW_OVERHEAD, name)?;
                    add_capture_estimate(&mut bytes, key.row().data().len(), name)?;
                    add_capture_estimate(
                        &mut bytes,
                        std::mem::size_of::<ScalarValue>()
                            .checked_mul(row.len())
                            .ok_or_else(|| capture_size_overflow(name))?,
                        name,
                    )?;
                    for value in row {
                        add_capture_estimate(&mut bytes, value.size(), name)?;
                    }
                }
            }
            MvStorageMode::Multiset => {
                let multiset = self.multiset.as_ref().ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "MV '{name}' is missing its multiset checkpoint state"
                    ))
                })?;
                for key in multiset.counts.keys() {
                    add_capture_estimate(&mut bytes, CHECKPOINT_CAPTURE_ROW_OVERHEAD, name)?;
                    add_capture_estimate(&mut bytes, key.row().data().len(), name)?;
                }
            }
        }
        Ok(bytes)
    }
}

impl MvStore {
    pub(super) fn checkpoint_capture_estimated_bytes(&self) -> Result<u64, DbError> {
        self.entries.iter().try_fold(0u64, |total, (name, entry)| {
            total
                .checked_add(entry.checkpoint_capture_estimate(name)?)
                .ok_or_else(|| capture_size_overflow(name))
        })
    }

    /// Capture an immutable point-in-time image without materializing Arrow output.
    pub fn capture_checkpoint(&self, max_bytes: u64) -> Result<MvCheckpointCapture, DbError> {
        let estimated_bytes = self.checkpoint_capture_estimated_bytes()?;
        if estimated_bytes > max_bytes {
            return Err(DbError::Checkpoint(format!(
                "MV checkpoint capture estimate {estimated_bytes} bytes exceeds the staged-state cap of {max_bytes} bytes"
            )));
        }

        let mut entries = Vec::with_capacity(self.entries.len());
        for (name, entry) in &self.entries {
            let captured = match &entry.mode {
                MvStorageMode::Aggregate | MvStorageMode::Append { .. } => {
                    MvCheckpointEntryCapture::Batches {
                        schema: Arc::clone(&entry.schema),
                        batches: entry.batches.iter().cloned().collect(),
                    }
                }
                MvStorageMode::Upsert { .. } => {
                    let upsert = entry.upsert.as_ref().ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "MV '{name}' is missing its upsert checkpoint state"
                        ))
                    })?;
                    MvCheckpointEntryCapture::Upsert {
                        schema: Arc::clone(&entry.schema),
                        rows: upsert
                            .rows
                            .iter()
                            .map(|(key, values)| (key.clone(), values.clone()))
                            .collect(),
                    }
                }
                MvStorageMode::Multiset => {
                    let multiset = entry.multiset.as_ref().ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "MV '{name}' is missing its multiset checkpoint state"
                        ))
                    })?;
                    MvCheckpointEntryCapture::Multiset {
                        schema: Arc::clone(&entry.schema),
                        row_converter: Arc::clone(&multiset.row_converter),
                        counts: multiset
                            .counts
                            .iter()
                            .map(|(key, count)| (key.clone(), *count))
                            .collect(),
                    }
                }
            };
            entries.push((name.clone(), captured));
        }
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
        let (checkpoint_schema, batches) = ipc_to_schema_and_batches(bytes)
            .map_err(|e| DbError::Storage(format!("MV restore '{name}': {e}")))?;

        if matches!(&entry.mode, MvStorageMode::Multiset) {
            if checkpoint_schema != multiset_checkpoint_schema(&entry.schema) {
                return Err(DbError::Storage(format!(
                    "MV '{name}' multiset checkpoint schema or format mismatch on restore"
                )));
            }
            let mut restored = MultisetState::new(&entry.schema)?;
            for batch in &batches {
                restored.load_counted_snapshot(batch)?;
            }
            entry.approx_bytes = restored.approx_bytes;
            entry.multiset = Some(restored);
            return Ok(());
        }

        // COMPAT: reject stale checkpoints from before a schema change rather than admitting
        // data that will fail or be misinterpreted during a later materialization.
        if checkpoint_schema != entry.schema {
            return Err(DbError::Storage(format!(
                "MV '{name}' schema mismatch on restore"
            )));
        }

        if let MvStorageMode::Upsert { key_cols } = &entry.mode {
            let mut restored = UpsertState::new(&entry.schema, key_cols)?;
            for batch in &batches {
                restored.load_snapshot(batch)?;
            }
            entry.approx_bytes = restored.approx_bytes;
            entry.upsert = Some(restored);
            return Ok(());
        }
        let restored_bytes = batches.iter().fold(0usize, |total, batch| {
            total.saturating_add(batch.get_array_memory_size())
        });
        entry.batches = batches.into_iter().collect();
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

fn batches_to_ipc_bounded<'a, I>(
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
