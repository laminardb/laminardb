//! Materialized view result storage, queryable via `SELECT * FROM mv_name`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_common::ScalarValue;
use laminar_core::changelog::WEIGHT_COLUMN;

use crate::error::DbError;

/// Default maximum batches retained in append mode.
const DEFAULT_APPEND_MAX_BATCHES: usize = 1000;

/// Default byte limit per MV in append mode (256 MB).
const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Rough per-row memory estimate for the multiset snapshot (encoded key + count).
const APPROX_BYTES_PER_MULTISET_ROW: usize = 32;

/// A multiset read must fit in one Arrow batch. Refuse pathological multiplicities before
/// `RowConverter` allocates its expanded row vector and output arrays.
const MAX_MULTISET_MATERIALIZED_ROWS: usize = 1_000_000;
const MULTISET_MATERIALIZATION_ROW_OVERHEAD: usize = 64;

/// Multiset checkpoints are counted snapshots: one row per distinct value plus this column.
/// This is deliberately an internal, versioned format rather than a user-visible storage option.
const MULTISET_CHECKPOINT_COUNT_COLUMN: &str = "__laminardb_multiset_count";
const MULTISET_CHECKPOINT_FORMAT_KEY: &str = "laminardb.mv.multiset.format";
const MULTISET_CHECKPOINT_FORMAT_VERSION: &str = "counted-v1";

const CHECKPOINT_CAPTURE_ENTRY_OVERHEAD: usize = 256;
const CHECKPOINT_CAPTURE_FIELD_OVERHEAD: usize = 128;
const CHECKPOINT_CAPTURE_ROW_OVERHEAD: usize = 64;

fn multiset_checkpoint_schema(schema: &SchemaRef) -> SchemaRef {
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
    fn states(&self) -> &HashMap<String, bytes::Bytes> {
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

/// How a materialized view accumulates results.
#[derive(Debug, Clone)]
pub(crate) enum MvStorageMode {
    /// GROUP BY queries: replace the result set each cycle.
    Aggregate,
    /// Non-aggregate queries: append with bounded retention.
    Append { max_batches: usize },
    /// Incremental keyed snapshot from a dirty-only `__weight` changelog; `key_cols` index the GROUP BY columns.
    Upsert { key_cols: Vec<usize> },
    /// Chained projection/filter: Z-set multiset keyed by the full row; handles key-dropping dups.
    Multiset,
}

impl MvStorageMode {
    pub fn append_default() -> Self {
        Self::Append {
            max_batches: DEFAULT_APPEND_MAX_BATCHES,
        }
    }
}

/// Keyed running snapshot from a `__weight` changelog; stored rows omit the weight column.
struct UpsertState {
    key_cols: Vec<usize>,
    key_converter: RowConverter,
    rows: HashMap<OwnedRow, Vec<ScalarValue>>,
    approx_bytes: usize,
}

/// Split a `__weight` changelog batch into its Int64 weight column and the non-weight column indices.
fn weight_and_plain_cols(batch: &RecordBatch) -> Result<(&Int64Array, Vec<usize>), DbError> {
    let weight_idx = batch
        .schema()
        .index_of(WEIGHT_COLUMN)
        .map_err(|e| DbError::Storage(format!("MV changelog missing weight: {e}")))?;
    let weights = batch
        .column(weight_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DbError::Storage("MV weight column not Int64".into()))?;
    let plain_cols = (0..batch.num_columns())
        .filter(|&c| c != weight_idx)
        .collect();
    Ok((weights, plain_cols))
}

impl UpsertState {
    fn new(schema: &SchemaRef, key_cols: &[usize]) -> Result<Self, DbError> {
        let sort_fields: Vec<SortField> = key_cols
            .iter()
            .map(|&c| SortField::new(schema.field(c).data_type().clone()))
            .collect();
        let key_converter = RowConverter::new(sort_fields)
            .map_err(|e| DbError::Storage(format!("upsert MV key converter: {e}")))?;
        Ok(Self {
            key_cols: key_cols.to_vec(),
            key_converter,
            rows: HashMap::new(),
            approx_bytes: 0,
        })
    }

    fn keys(&self, batch: &RecordBatch) -> Result<arrow::row::Rows, DbError> {
        let key_arrays: Vec<ArrayRef> = self
            .key_cols
            .iter()
            .map(|&c| Arc::clone(batch.column(c)))
            .collect();
        self.key_converter
            .convert_columns(&key_arrays)
            .map_err(|e| DbError::Storage(format!("upsert MV key conversion: {e}")))
    }

    fn row_size(values: &[ScalarValue]) -> usize {
        values
            .iter()
            .fold(0, |bytes, value| bytes.saturating_add(value.size()))
    }

    fn replace_row(&mut self, key: OwnedRow, replacement: Option<Vec<ScalarValue>>) {
        if let Some(old) = self.rows.remove(&key) {
            self.approx_bytes = self.approx_bytes.saturating_sub(Self::row_size(&old));
        }
        if let Some(values) = replacement {
            self.approx_bytes = self.approx_bytes.saturating_add(Self::row_size(&values));
            self.rows.insert(key, values);
        }
    }

    /// Stage `+weight` upserts and `-weight` deletes without touching live state.
    fn stage_batch(
        &self,
        batch: &RecordBatch,
        staged: &mut HashMap<OwnedRow, Option<Vec<ScalarValue>>>,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (weights, plain_cols) = weight_and_plain_cols(batch)?;
        let keys = self.keys(batch)?;

        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                return Err(DbError::Storage(format!(
                    "upsert MV weight is null at row {row_idx}"
                )));
            }
            let key = keys.row(row_idx).owned();
            let w = weights.value(row_idx);
            if w > 0 {
                let mut vals = Vec::with_capacity(plain_cols.len());
                for &c in &plain_cols {
                    vals.push(
                        ScalarValue::try_from_array(batch.column(c), row_idx)
                            .map_err(|e| DbError::Storage(format!("upsert MV scalar: {e}")))?,
                    );
                }
                staged.insert(key, Some(vals));
            } else if w < 0 {
                staged.insert(key, None);
            }
        }
        Ok(())
    }

    fn apply_cycle(&mut self, batches: &[RecordBatch]) -> Result<(), DbError> {
        let mut staged = HashMap::new();
        for batch in batches {
            self.stage_batch(batch, &mut staged)?;
        }
        for (key, replacement) in staged {
            self.replace_row(key, replacement);
        }
        Ok(())
    }

    /// Restore from a materialized plain snapshot (no weight column): every row is an insert.
    fn load_snapshot(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let keys = self.keys(batch)?;
        for row_idx in 0..batch.num_rows() {
            let key = keys.row(row_idx).owned();
            let mut vals = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                vals.push(
                    ScalarValue::try_from_array(batch.column(c), row_idx)
                        .map_err(|e| DbError::Storage(format!("upsert MV restore scalar: {e}")))?,
                );
            }
            self.replace_row(key, Some(vals));
        }
        Ok(())
    }

    fn to_record_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        if self.rows.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        // One pass over the row map assembling all columns, rather than one full map scan per column.
        let ncols = schema.fields().len();
        let mut columns: Vec<Vec<ScalarValue>> = (0..ncols)
            .map(|_| Vec::with_capacity(self.rows.len()))
            .collect();
        for row in self.rows.values() {
            for (c, col) in columns.iter_mut().enumerate() {
                col.push(row[c].clone());
            }
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
        for col in columns {
            arrays.push(
                ScalarValue::iter_to_array(col)
                    .map_err(|e| DbError::Storage(format!("upsert MV column build: {e}")))?,
            );
        }
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DbError::Storage(format!("upsert MV batch assembly: {e}")))
    }
}

/// Z-set multiset from a `__weight` changelog: full output row keyed to an integer multiplicity.
struct MultisetState {
    row_converter: Arc<RowConverter>,
    counts: HashMap<OwnedRow, i64>,
    approx_bytes: usize,
}

impl MultisetState {
    fn new(schema: &SchemaRef) -> Result<Self, DbError> {
        let sort_fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        let row_converter = Arc::new(
            RowConverter::new(sort_fields)
                .map_err(|e| DbError::Storage(format!("multiset MV row converter: {e}")))?,
        );
        Ok(Self {
            row_converter,
            counts: HashMap::new(),
            approx_bytes: 0,
        })
    }

    fn stage_batch(
        &self,
        batch: &RecordBatch,
        deltas: &mut HashMap<OwnedRow, i128>,
    ) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (weights, plain_indices) = weight_and_plain_cols(batch)?;
        let plain_cols: Vec<ArrayRef> = plain_indices
            .iter()
            .map(|&c| Arc::clone(batch.column(c)))
            .collect();
        let rows = self
            .row_converter
            .convert_columns(&plain_cols)
            .map_err(|e| DbError::Storage(format!("multiset MV row conversion: {e}")))?;

        for row_idx in 0..batch.num_rows() {
            if weights.is_null(row_idx) {
                return Err(DbError::Storage(format!(
                    "multiset MV weight is null at row {row_idx}"
                )));
            }
            let w = weights.value(row_idx);
            if w == 0 {
                continue;
            }
            let key = rows.row(row_idx).owned();
            let prior = deltas.get(&key).copied().unwrap_or(0);
            let delta = prior.checked_add(i128::from(w)).ok_or_else(|| {
                DbError::Storage("multiset MV staged multiplicity overflow".into())
            })?;
            deltas.insert(key, delta);
        }
        Ok(())
    }

    /// Apply a cycle's Z-set deltas only after every touched row has a valid final multiplicity.
    fn apply_cycle(&mut self, batches: &[RecordBatch]) -> Result<(), DbError> {
        let mut deltas = HashMap::new();
        for batch in batches {
            self.stage_batch(batch, &mut deltas)?;
        }

        let mut resolved = Vec::with_capacity(deltas.len());
        for (key, delta) in deltas {
            let current = i128::from(self.counts.get(&key).copied().unwrap_or(0));
            let next = current
                .checked_add(delta)
                .ok_or_else(|| DbError::Storage("multiset MV multiplicity overflow".into()))?;
            if next < 0 {
                return Err(DbError::Storage(
                    "multiset MV retraction produced a negative multiplicity".into(),
                ));
            }
            let next = i64::try_from(next)
                .map_err(|_| DbError::Storage("multiset MV multiplicity overflow".into()))?;
            resolved.push((key, next));
        }

        for (key, count) in resolved {
            if count == 0 {
                self.counts.remove(&key);
            } else {
                self.counts.insert(key, count);
            }
        }
        self.approx_bytes = self
            .counts
            .len()
            .saturating_mul(APPROX_BYTES_PER_MULTISET_ROW);
        Ok(())
    }

    /// Restore a counted checkpoint batch. Checkpoints contain exactly one row per distinct
    /// value; duplicates are corruption rather than an alternate encoding.
    fn load_counted_snapshot(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let count_idx = batch.num_columns().checked_sub(1).ok_or_else(|| {
            DbError::Storage("multiset MV checkpoint is missing its count column".into())
        })?;
        let counts = batch
            .column(count_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DbError::Storage("multiset MV checkpoint count column is not Int64".into())
            })?;
        let rows = self
            .row_converter
            .convert_columns(&batch.columns()[..count_idx])
            .map_err(|e| DbError::Storage(format!("multiset MV restore conversion: {e}")))?;
        for row_idx in 0..batch.num_rows() {
            if counts.is_null(row_idx) {
                return Err(DbError::Storage(format!(
                    "multiset MV checkpoint count is null at row {row_idx}"
                )));
            }
            let count = counts.value(row_idx);
            if count <= 0 {
                return Err(DbError::Storage(format!(
                    "multiset MV checkpoint count must be positive at row {row_idx}"
                )));
            }
            let key = rows.row(row_idx).owned();
            if self.counts.insert(key, count).is_some() {
                return Err(DbError::Storage(format!(
                    "multiset MV checkpoint contains a duplicate value at row {row_idx}"
                )));
            }
        }
        self.approx_bytes = self
            .counts
            .len()
            .saturating_mul(APPROX_BYTES_PER_MULTISET_ROW);
        Ok(())
    }

    fn materialized_row_count(&self) -> Result<usize, DbError> {
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for (key, &count) in &self.counts {
            let count = usize::try_from(count).map_err(|_| {
                DbError::Storage("multiset MV contains an invalid multiplicity".into())
            })?;
            rows = rows.checked_add(count).ok_or_else(|| {
                DbError::Storage("multiset MV materialization row count overflow".into())
            })?;
            if rows > MAX_MULTISET_MATERIALIZED_ROWS {
                return Err(DbError::Storage(format!(
                    "multiset MV materialization exceeds the safe row limit of {MAX_MULTISET_MATERIALIZED_ROWS}"
                )));
            }

            let per_row = key
                .row()
                .data()
                .len()
                .checked_add(MULTISET_MATERIALIZATION_ROW_OVERHEAD)
                .ok_or_else(|| {
                    DbError::Storage("multiset MV materialization size overflow".into())
                })?;
            bytes = bytes
                .checked_add(per_row.checked_mul(count).ok_or_else(|| {
                    DbError::Storage("multiset MV materialization size overflow".into())
                })?)
                .ok_or_else(|| {
                    DbError::Storage("multiset MV materialization size overflow".into())
                })?;
            if bytes > DEFAULT_MAX_BYTES {
                return Err(DbError::Storage(format!(
                    "multiset MV materialization exceeds the safe byte limit of {DEFAULT_MAX_BYTES}"
                )));
            }
        }
        Ok(rows)
    }

    fn to_record_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        if self.counts.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        self.materialized_row_count()?;
        let rows = self.counts.iter().flat_map(|(key, &count)| {
            std::iter::repeat_n(key.row(), usize::try_from(count).unwrap_or(0))
        });
        let arrays = self
            .row_converter
            .convert_rows(rows)
            .map_err(|e| DbError::Storage(format!("multiset MV row conversion: {e}")))?;
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DbError::Storage(format!("multiset MV batch assembly: {e}")))
    }
}

/// Per-MV result store.
pub(crate) struct MvEntry {
    schema: SchemaRef,
    mode: MvStorageMode,
    batches: VecDeque<RecordBatch>,
    /// Present only in `Upsert` mode.
    upsert: Option<UpsertState>,
    /// Present only in `Multiset` mode.
    multiset: Option<MultisetState>,
    approx_bytes: usize,
}

impl MvEntry {
    fn new(schema: SchemaRef, mode: MvStorageMode) -> Result<Self, DbError> {
        let upsert = match &mode {
            MvStorageMode::Upsert { key_cols } => Some(UpsertState::new(&schema, key_cols)?),
            _ => None,
        };
        let multiset = match &mode {
            MvStorageMode::Multiset => Some(MultisetState::new(&schema)?),
            _ => None,
        };
        Ok(Self {
            schema,
            mode,
            batches: VecDeque::new(),
            upsert,
            multiset,
            approx_bytes: 0,
        })
    }

    /// Apply one cycle's worth of output batches. `Aggregate` replaces its whole result set
    /// once here — a non-incremental GROUP BY MV whose output exceeds one `DataFusion` batch
    /// (>8192 rows, or >1 partition in cluster) arrives as several batches, so clearing
    /// per-batch would keep only the last chunk (EX-1). Empty batches are skipped, and an
    /// all-empty cycle leaves the prior snapshot untouched.
    fn update_cycle(&mut self, batches: &[RecordBatch]) -> Result<(), DbError> {
        match &self.mode {
            MvStorageMode::Aggregate => {
                // Don't clear on an all-empty cycle — no recompute happened, keep the snapshot.
                if batches.iter().all(|b| b.num_rows() == 0) {
                    return Ok(());
                }
                self.batches.clear();
                self.approx_bytes = 0;
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.approx_bytes = self
                        .approx_bytes
                        .saturating_add(batch.get_array_memory_size());
                    self.batches.push_back(batch.clone());
                }
            }
            MvStorageMode::Append { max_batches } => {
                let max_batches = *max_batches;
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.approx_bytes = self
                        .approx_bytes
                        .saturating_add(batch.get_array_memory_size());
                    self.batches.push_back(batch.clone());
                    while self.batches.len() > 1
                        && (self.batches.len() > max_batches
                            || self.approx_bytes > DEFAULT_MAX_BYTES)
                    {
                        if let Some(evicted) = self.batches.pop_front() {
                            self.approx_bytes = self
                                .approx_bytes
                                .saturating_sub(evicted.get_array_memory_size());
                        } else {
                            break;
                        }
                    }
                }
            }
            MvStorageMode::Upsert { .. } => {
                if let Some(up) = self.upsert.as_mut() {
                    up.apply_cycle(batches)?;
                    self.approx_bytes = up.approx_bytes;
                }
            }
            MvStorageMode::Multiset => {
                if let Some(ms) = self.multiset.as_mut() {
                    ms.apply_cycle(batches)?;
                    self.approx_bytes = ms.approx_bytes;
                }
            }
        }
        Ok(())
    }

    fn to_record_batch(&self) -> Result<RecordBatch, DbError> {
        if let Some(up) = self.upsert.as_ref() {
            return up.to_record_batch(&self.schema);
        }
        if let Some(ms) = self.multiset.as_ref() {
            return ms.to_record_batch(&self.schema);
        }
        if self.batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let refs: Vec<&RecordBatch> = self.batches.iter().collect();
        arrow::compute::concat_batches(&self.schema, refs.iter().copied())
            .map_err(|e| DbError::Storage(format!("MV batch concat: {e}")))
    }

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

/// Store for all materialized view results; shared via `Arc<RwLock<MvStore>>`.
pub(crate) struct MvStore {
    entries: HashMap<String, MvEntry>,
    /// Lets the hot path skip the write lock when no MVs exist.
    has_any: Arc<AtomicBool>,
}

impl MvStore {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            has_any: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn has_any_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.has_any)
    }

    pub fn create_mv(
        &mut self,
        name: &str,
        schema: SchemaRef,
        mode: MvStorageMode,
    ) -> Result<(), DbError> {
        self.entries
            .insert(name.to_string(), MvEntry::new(schema, mode)?);
        self.has_any.store(true, Ordering::Release);
        Ok(())
    }

    pub fn drop_mv(&mut self, name: &str) -> bool {
        let removed = self.entries.remove(name).is_some();
        if self.entries.is_empty() {
            self.has_any.store(false, Ordering::Release);
        }
        removed
    }

    pub fn has_mv(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    #[cfg(feature = "cluster")]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Apply one cycle's output batches to an MV (see `MvEntry::update_cycle`).
    pub fn update_cycle(&mut self, name: &str, batches: &[RecordBatch]) -> Result<(), DbError> {
        if let Some(entry) = self.entries.get_mut(name) {
            entry.update_cycle(batches)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn update(&mut self, name: &str, batch: &RecordBatch) {
        self.update_cycle(name, std::slice::from_ref(batch))
            .expect("test MV update must succeed");
    }

    pub fn to_record_batch(&self, name: &str) -> Result<Option<RecordBatch>, DbError> {
        self.entries
            .get(name)
            .map(MvEntry::to_record_batch)
            .transpose()
    }

    pub fn total_bytes(&self) -> usize {
        self.entries
            .values()
            .fold(0, |total, entry| total.saturating_add(entry.approx_bytes))
    }

    /// Build an empty image with the current catalog shape and the same hot-path presence flag.
    pub fn fresh_image(&self) -> Result<Self, DbError> {
        let mut entries = HashMap::with_capacity(self.entries.len());
        for (name, entry) in &self.entries {
            entries.insert(
                name.clone(),
                MvEntry::new(Arc::clone(&entry.schema), entry.mode.clone())?,
            );
        }
        Ok(Self {
            entries,
            has_any: Arc::clone(&self.has_any),
        })
    }

    /// Restore a complete checkpoint into a private image. The live store is never mutated.
    pub fn recovery_image(&self, states: &HashMap<String, Vec<u8>>) -> Result<Self, DbError> {
        let mut image = self.fresh_image().map_err(|error| {
            DbError::Checkpoint(format!("cannot create an empty MV recovery image: {error}"))
        })?;
        let mut restored = HashSet::with_capacity(states.len());

        for (name, bytes) in states {
            if !image.entries.contains_key(name) {
                return Err(DbError::Checkpoint(format!(
                    "MV checkpoint '{name}' has no matching registered materialized view"
                )));
            }
            image.restore_from_ipc(name, bytes).map_err(|error| {
                DbError::Checkpoint(format!("MV restore failed for '{name}': {error}"))
            })?;
            restored.insert(name.as_str());
        }

        let mut missing: Vec<&str> = image
            .entries
            .keys()
            .map(String::as_str)
            .filter(|name| !restored.contains(name))
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(DbError::Checkpoint(format!(
                "MV checkpoint is missing required state for: {}",
                missing.join(", ")
            )));
        }

        Ok(image)
    }

    fn checkpoint_capture_estimated_bytes(&self) -> Result<u64, DbError> {
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

    fn restore_from_ipc(&mut self, name: &str, bytes: &[u8]) -> Result<(), DbError> {
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

        // Reject stale checkpoints from before a schema change rather than panicking later.
        if checkpoint_schema != entry.schema {
            return Err(DbError::Storage(format!(
                "MV '{name}' schema mismatch on restore"
            )));
        }

        if let MvStorageMode::Upsert { key_cols } = &entry.mode {
            let mut restored = UpsertState::new(&entry.schema, key_cols)?;
            for b in &batches {
                restored.load_snapshot(b)?;
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

/// Prefix for MV entries in the `operator_states` checkpoint map.
pub(crate) const CHECKPOINT_KEY_PREFIX: &str = "mv:";

pub(crate) fn batches_to_ipc<'a, I>(schema: &SchemaRef, batches: I) -> Result<Vec<u8>, DbError>
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

pub(crate) fn ipc_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    ipc_to_schema_and_batches(bytes).map(|(_, batches)| batches)
}

fn ipc_to_schema_and_batches(
    bytes: &[u8],
) -> Result<(SchemaRef, Vec<RecordBatch>), arrow::error::ArrowError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_batch(ids: &[i32], names: &[&str], values: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn id_names(store: &MvStore, name: &str) -> Vec<(i32, String)> {
        let batch = store.to_record_batch(name).unwrap().unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| (ids.value(row), names.value(row).to_string()))
            .collect()
    }

    /// Plain (weightless) schema for the upsert tests: `(k Int64, total Int64)`.
    fn upsert_plain_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("total", DataType::Int64, true),
        ]))
    }

    /// Changelog schema = plain schema + appended `__weight`.
    fn upsert_changelog_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("total", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]))
    }

    /// Build a `__weight` changelog batch from `(key, total, weight)` rows.
    fn changelog_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
        use arrow::array::Int64Array;
        let ks: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let totals: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let weights: Vec<i64> = rows.iter().map(|r| r.2).collect();
        RecordBatch::try_new(
            upsert_changelog_schema(),
            vec![
                Arc::new(Int64Array::from(ks)),
                Arc::new(Int64Array::from(totals)),
                Arc::new(Int64Array::from(weights)),
            ],
        )
        .unwrap()
    }

    fn upsert_snapshot_batch(rows: &[(i64, i64)]) -> RecordBatch {
        let keys: Vec<i64> = rows.iter().map(|row| row.0).collect();
        let totals: Vec<i64> = rows.iter().map(|row| row.1).collect();
        RecordBatch::try_new(
            upsert_plain_schema(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(totals)),
            ],
        )
        .unwrap()
    }

    fn nullable_upsert_changelog_batch(rows: &[(i64, i64, Option<i64>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("total", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// `(k, total)` snapshot rows sorted by key, for order-independent assertions.
    fn snapshot_rows(store: &MvStore, name: &str) -> Vec<(i64, i64)> {
        use arrow::array::Int64Array;
        let batch = store.to_record_batch(name).unwrap().unwrap();
        let ks = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let totals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut out: Vec<(i64, i64)> = (0..batch.num_rows())
            .map(|i| (ks.value(i), totals.value(i)))
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn create_and_drop() {
        let mut store = MvStore::new();
        store
            .create_mv("mv1", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        assert!(store.has_mv("mv1"));
        assert!(store.drop_mv("mv1"));
        assert!(!store.has_mv("mv1"));
        assert!(!store.drop_mv("mv1"));
    }

    #[test]
    fn aggregate_replaces_on_each_update() {
        let mut store = MvStore::new();
        store
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();

        store.update("agg", &make_batch(&[1], &["a"], &[1.0]));
        assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 1);

        store.update("agg", &make_batch(&[2, 3], &["b", "c"], &[2.0, 3.0]));
        assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 2);
    }

    #[test]
    fn aggregate_keeps_all_batches_of_a_multi_batch_cycle() {
        let mut store = MvStore::new();
        store
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();

        // A non-incremental GROUP BY MV whose output spans several DataFusion batches must
        // retain every chunk within the cycle, not just the last (EX-1).
        store
            .update_cycle(
                "agg",
                &[
                    make_batch(&[1, 2], &["a", "b"], &[1.0, 2.0]),
                    make_batch(&[3, 4], &["c", "d"], &[3.0, 4.0]),
                    make_batch(&[5], &["e"], &[5.0]),
                ],
            )
            .unwrap();
        assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 5);

        // The next cycle replaces the whole result set.
        store
            .update_cycle("agg", &[make_batch(&[9], &["z"], &[9.0])])
            .unwrap();
        assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 1);
    }

    #[test]
    fn append_evicts_oldest() {
        let mut store = MvStore::new();
        store
            .create_mv(
                "app",
                test_schema(),
                MvStorageMode::Append { max_batches: 3 },
            )
            .unwrap();

        for i in 0..4 {
            store.update("app", &make_batch(&[i], &["x"], &[f64::from(i)]));
        }

        let result = store.to_record_batch("app").unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);

        // Batch 0 evicted, should start at 1
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
    }

    #[test]
    fn empty_mv_returns_empty_batch() {
        let mut store = MvStore::new();
        store
            .create_mv("empty", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        let result = store.to_record_batch("empty").unwrap().unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema(), test_schema());
    }

    #[test]
    fn nonexistent_returns_none() {
        let store = MvStore::new();
        assert!(store.to_record_batch("nope").unwrap().is_none());
    }

    #[test]
    fn checkpoint_round_trip() {
        let mut store = MvStore::new();
        store
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        store.update("agg", &make_batch(&[1, 2], &["a", "b"], &[1.0, 2.0]));

        let states = store.checkpoint_states().unwrap();
        assert_eq!(states.len(), 1);
        assert!(states.contains_key("mv:agg"));

        // Simulate recovery into a fresh store
        let mut store2 = MvStore::new();
        store2
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        for (key, bytes) in &states {
            let name = key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap();
            store2.restore_from_ipc(name, bytes).unwrap();
        }
        assert_eq!(
            store2.to_record_batch("agg").unwrap().unwrap().num_rows(),
            2
        );
    }

    #[test]
    fn checkpoint_capture_is_point_in_time_after_live_mutation() {
        let mut store = MvStore::new();
        store
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        store
            .create_mv(
                "append",
                test_schema(),
                MvStorageMode::Append { max_batches: 8 },
            )
            .unwrap();
        store
            .create_mv(
                "upsert",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        store
            .create_mv("multiset", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();

        store.update("agg", &make_batch(&[1], &["old-agg"], &[1.0]));
        store.update("append", &make_batch(&[2], &["old-append"], &[2.0]));
        store.update("upsert", &changelog_batch(&[(1, 10, 1)]));
        store.update("multiset", &weight_batch_1col(&[(10, 2)]));
        let capture = store.capture_checkpoint(u64::MAX).unwrap();

        store.update("agg", &make_batch(&[9], &["new-agg"], &[9.0]));
        store.update("append", &make_batch(&[3], &["new-append"], &[3.0]));
        store.update("upsert", &changelog_batch(&[(1, 10, -1), (1, 99, 1)]));
        store.update("multiset", &weight_batch_1col(&[(10, -1), (20, 1)]));

        let states = capture
            .encode(u64::MAX)
            .unwrap()
            .into_parts()
            .0
            .into_iter()
            .map(|(key, bytes)| {
                (
                    key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap().to_string(),
                    bytes.to_vec(),
                )
            })
            .collect::<HashMap<_, _>>();
        let image = store.recovery_image(&states).unwrap();

        assert_eq!(id_names(&image, "agg"), vec![(1, "old-agg".to_string())]);
        assert_eq!(
            id_names(&image, "append"),
            vec![(2, "old-append".to_string())]
        );
        assert_eq!(snapshot_rows(&image, "upsert"), vec![(1, 10)]);
        assert_eq!(multiset_values(&image, "multiset"), vec![10, 10]);

        assert_eq!(id_names(&store, "agg"), vec![(9, "new-agg".to_string())]);
        assert_eq!(
            id_names(&store, "append"),
            vec![(2, "old-append".to_string()), (3, "new-append".to_string())]
        );
        assert_eq!(snapshot_rows(&store, "upsert"), vec![(1, 99)]);
        assert_eq!(multiset_values(&store, "multiset"), vec![10, 20]);
    }

    #[test]
    fn checkpoint_capture_cap_rejection_preserves_live_state() {
        let mut store = MvStore::new();
        store
            .create_mv(
                "upsert",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        store.update("upsert", &changelog_batch(&[(1, 10, 1), (2, 20, 1)]));
        let before = snapshot_rows(&store, "upsert");
        let before_bytes = store.total_bytes();
        let estimated = store.checkpoint_capture_estimated_bytes().unwrap();
        assert!(estimated > 0);

        let error = store
            .capture_checkpoint(estimated - 1)
            .err()
            .expect("capture above the remaining checkpoint budget must fail");

        assert!(error.to_string().contains("staged-state cap"));
        assert_eq!(snapshot_rows(&store, "upsert"), before);
        assert_eq!(store.total_bytes(), before_bytes);
    }

    #[test]
    fn checkpoint_ipc_encoding_enforces_dynamic_remaining_budget_without_mutation() {
        let mut store = MvStore::new();
        for name in ["a", "b"] {
            store
                .create_mv(name, test_schema(), MvStorageMode::Aggregate)
                .unwrap();
            store.update(name, &make_batch(&[1, 2], &["one", "two"], &[1.0, 2.0]));
        }
        let before_a = store.to_record_batch("a").unwrap().unwrap();
        let before_b = store.to_record_batch("b").unwrap().unwrap();
        let before_bytes = store.total_bytes();

        let full = store
            .capture_checkpoint(u64::MAX)
            .unwrap()
            .encode(u64::MAX)
            .unwrap();
        let full_bytes = full.states().values().try_fold(0u64, |total, bytes| {
            total.checked_add(u64::try_from(bytes.len()).unwrap())
        });
        let full_bytes = full_bytes.unwrap();

        let error = store
            .capture_checkpoint(u64::MAX)
            .unwrap()
            .encode(full_bytes - 1)
            .unwrap_err();
        assert!(error.to_string().contains("MV 'b'"));
        assert!(error.to_string().contains("configured bound"));

        let tiny_error = store
            .capture_checkpoint(u64::MAX)
            .unwrap()
            .encode(1)
            .unwrap_err();
        assert!(tiny_error.to_string().contains("MV 'a'"));
        assert!(tiny_error.to_string().contains("configured bound"));

        assert_eq!(store.to_record_batch("a").unwrap().unwrap(), before_a);
        assert_eq!(store.to_record_batch("b").unwrap().unwrap(), before_b);
        assert_eq!(store.total_bytes(), before_bytes);
    }

    #[test]
    fn empty_local_checkpoint_has_an_explicit_entry_for_every_storage_mode() {
        let mut store = MvStore::new();
        store
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        store
            .create_mv("append", test_schema(), MvStorageMode::append_default())
            .unwrap();
        store
            .create_mv(
                "upsert",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        store
            .create_mv("multiset", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();

        let states = store.checkpoint_states().unwrap();
        let mut keys: Vec<&str> = states.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["mv:agg", "mv:append", "mv:multiset", "mv:upsert"]);
        for (key, bytes) in &states {
            let (schema, batches) = ipc_to_schema_and_batches(bytes).unwrap();
            let expected = match key.as_str() {
                "mv:agg" | "mv:append" => test_schema(),
                "mv:upsert" => upsert_plain_schema(),
                "mv:multiset" => multiset_checkpoint_schema(&one_col_schema()),
                other => panic!("unexpected checkpoint key {other}"),
            };
            assert_eq!(schema, expected, "schema for {key}");
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                0,
                "rows for {key}"
            );
        }
    }

    #[test]
    fn update_nonexistent_is_noop() {
        let mut store = MvStore::new();
        store.update("nope", &make_batch(&[1], &["a"], &[1.0]));
        assert!(!store.has_mv("nope"));
    }

    #[test]
    fn create_replaces_existing() {
        let mut store = MvStore::new();
        store
            .create_mv("mv1", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        store.update("mv1", &make_batch(&[1], &["a"], &[1.0]));
        assert_eq!(store.to_record_batch("mv1").unwrap().unwrap().num_rows(), 1);

        store
            .create_mv("mv1", test_schema(), MvStorageMode::append_default())
            .unwrap();
        assert_eq!(store.to_record_batch("mv1").unwrap().unwrap().num_rows(), 0);
    }

    #[test]
    fn restore_rejects_schema_mismatch() {
        let schema_a = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]));

        // Serialize a batch with schema_b
        let batch_b = RecordBatch::try_new(
            schema_b.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        let mut deque = VecDeque::new();
        deque.push_back(batch_b);
        let ipc_bytes = batches_to_ipc(&schema_b, &deque).unwrap();

        // Try to restore into an MV with schema_a
        let mut store = MvStore::new();
        store
            .create_mv("mv1", schema_a, MvStorageMode::Aggregate)
            .unwrap();
        let err = store.restore_from_ipc("mv1", &ipc_bytes);
        assert!(err.is_err(), "should reject mismatched schema");
    }

    #[test]
    fn restore_rejects_schema_only_stream_with_wrong_schema() {
        let expected = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let wrong = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let bytes = batches_to_ipc(&wrong, std::iter::empty::<&RecordBatch>()).unwrap();
        let mut store = MvStore::new();
        store
            .create_mv("mv1", expected, MvStorageMode::Aggregate)
            .unwrap();

        let error = store.restore_from_ipc("mv1", &bytes).unwrap_err();
        assert!(error.to_string().contains("schema mismatch"));
    }

    #[test]
    fn fresh_image_is_empty_and_preserves_the_hot_path_handle() {
        let mut store = MvStore::new();
        store
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        store.update("agg", &make_batch(&[1], &["a"], &[1.0]));
        let handle = store.has_any_handle();

        let image = store.fresh_image().unwrap();

        assert!(Arc::ptr_eq(&handle, &image.has_any_handle()));
        assert_eq!(image.to_record_batch("agg").unwrap().unwrap().num_rows(), 0);
        assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 1);
    }

    #[test]
    fn recovery_image_requires_an_exact_inventory() {
        let mut store = MvStore::new();
        store
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();

        let missing = store
            .recovery_image(&HashMap::new())
            .err()
            .expect("missing inventory must fail");
        assert!(missing
            .to_string()
            .contains("missing required state for: agg"));

        let bytes = batches_to_ipc(
            &test_schema(),
            std::iter::once(&make_batch(&[1], &["a"], &[1.0])),
        )
        .unwrap();
        let unknown = store
            .recovery_image(&HashMap::from([("ghost".to_string(), bytes)]))
            .err()
            .expect("unknown inventory must fail");
        assert!(unknown.to_string().contains("no matching registered"));
    }

    #[test]
    fn failed_recovery_image_never_mutates_live_state() {
        let mut store = MvStore::new();
        store
            .create_mv("agg", test_schema(), MvStorageMode::Aggregate)
            .unwrap();
        store
            .create_mv("append", test_schema(), MvStorageMode::append_default())
            .unwrap();
        store.update("agg", &make_batch(&[1], &["live"], &[1.0]));
        store.update("append", &make_batch(&[2], &["live"], &[2.0]));
        let live_bytes = store.total_bytes();

        let valid = batches_to_ipc(
            &test_schema(),
            std::iter::once(&make_batch(&[9], &["checkpoint"], &[9.0])),
        )
        .unwrap();
        let states = HashMap::from([
            ("agg".to_string(), valid),
            ("append".to_string(), b"not arrow ipc".to_vec()),
        ]);

        let error = store
            .recovery_image(&states)
            .err()
            .expect("corrupt inventory must fail");
        assert!(error.to_string().contains("MV restore failed"));
        assert_eq!(store.total_bytes(), live_bytes);
        assert_eq!(id_names(&store, "agg"), [(1, "live".to_string())]);
        assert_eq!(id_names(&store, "append"), [(2, "live".to_string())]);
    }

    #[test]
    fn upsert_applies_inserts_changes_and_deletes() {
        let mut store = MvStore::new();
        store
            .create_mv(
                "u",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();

        // Two new keys.
        store.update("u", &changelog_batch(&[(1, 10, 1), (2, 20, 1)]));
        assert_eq!(snapshot_rows(&store, "u"), vec![(1, 10), (2, 20)]);

        // Change key 1: retract old (−1) then insert new (+1) nets to the new value.
        store.update("u", &changelog_batch(&[(1, 10, -1), (1, 15, 1)]));
        assert_eq!(snapshot_rows(&store, "u"), vec![(1, 15), (2, 20)]);

        // Delete key 2 (pure retract).
        store.update("u", &changelog_batch(&[(2, 20, -1)]));
        assert_eq!(snapshot_rows(&store, "u"), vec![(1, 15)]);
    }

    #[test]
    fn upsert_cycle_is_atomic_when_a_later_batch_has_null_weight() {
        let mut store = MvStore::new();
        store
            .create_mv(
                "u",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        store.update("u", &changelog_batch(&[(1, 10, 1)]));
        let before = snapshot_rows(&store, "u");
        let before_bytes = store.total_bytes();

        let error = store
            .update_cycle(
                "u",
                &[
                    changelog_batch(&[(2, 20, 1)]),
                    nullable_upsert_changelog_batch(&[(3, 30, None)]),
                ],
            )
            .expect_err("null weight must reject the whole cycle");
        assert!(error.to_string().contains("weight is null"));
        assert_eq!(snapshot_rows(&store, "u"), before);
        assert_eq!(store.total_bytes(), before_bytes);
    }

    #[test]
    fn upsert_snapshot_equals_full_recompute() {
        use std::collections::BTreeMap;

        // A changelog stream and the running ground-truth (last +weight value per key).
        let batches = [
            vec![(1i64, 5i64, 1i64), (2, 7, 1), (3, 9, 1)],
            vec![(2, 7, -1), (2, 8, 1), (4, 1, 1)],
            vec![(1, 5, -1)], // delete key 1
            vec![(3, 9, -1), (3, 100, 1)],
        ];
        let mut truth: BTreeMap<i64, i64> = BTreeMap::new();
        let mut store = MvStore::new();
        store
            .create_mv(
                "u",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        for rows in &batches {
            for &(k, v, w) in rows {
                if w > 0 {
                    truth.insert(k, v);
                } else {
                    truth.remove(&k);
                }
            }
            store.update("u", &changelog_batch(rows));
        }
        let expected: Vec<(i64, i64)> = truth.into_iter().collect();
        assert_eq!(snapshot_rows(&store, "u"), expected);
    }

    #[test]
    fn upsert_checkpoint_round_trip() {
        let mut store = MvStore::new();
        store
            .create_mv(
                "u",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        store.update("u", &changelog_batch(&[(1, 10, 1), (2, 20, 1), (3, 30, 1)]));
        store.update("u", &changelog_batch(&[(2, 20, -1)]));
        let before = snapshot_rows(&store, "u");

        let states = store.checkpoint_states().unwrap();
        assert!(states.contains_key("mv:u"));

        let mut store2 = MvStore::new();
        store2
            .create_mv(
                "u",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        for (key, bytes) in &states {
            let name = key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap();
            store2.restore_from_ipc(name, bytes).unwrap();
        }
        assert_eq!(snapshot_rows(&store2, "u"), before);

        // A restored store keeps applying changelog correctly.
        store2.update("u", &changelog_batch(&[(1, 10, -1), (1, 99, 1)]));
        assert_eq!(snapshot_rows(&store2, "u"), vec![(1, 99), (3, 30)]);
    }

    #[test]
    fn failed_upsert_restore_preserves_live_state() {
        let mut store = MvStore::new();
        store
            .create_mv(
                "u",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        store.update("u", &changelog_batch(&[(1, 10, 1)]));
        let before = snapshot_rows(&store, "u");
        let before_bytes = store.total_bytes();

        let wrong_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let bad = RecordBatch::try_new(
            wrong_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![9]))],
        )
        .unwrap();
        let bytes = batches_to_ipc(&wrong_schema, std::iter::once(&bad)).unwrap();
        assert!(store.restore_from_ipc("u", &bytes).is_err());
        assert_eq!(snapshot_rows(&store, "u"), before);
        assert_eq!(store.total_bytes(), before_bytes);
    }

    #[test]
    fn upsert_restore_duplicate_key_accounting_tracks_only_replacement() {
        let mut store = MvStore::new();
        store
            .create_mv(
                "u",
                upsert_plain_schema(),
                MvStorageMode::Upsert { key_cols: vec![0] },
            )
            .unwrap();
        let snapshot = upsert_snapshot_batch(&[(1, 10), (1, 20)]);
        let bytes = batches_to_ipc(&upsert_plain_schema(), std::iter::once(&snapshot)).unwrap();

        store.restore_from_ipc("u", &bytes).unwrap();
        assert_eq!(snapshot_rows(&store, "u"), vec![(1, 20)]);
        let expected = ScalarValue::Int64(Some(1))
            .size()
            .saturating_add(ScalarValue::Int64(Some(20)).size());
        assert_eq!(store.total_bytes(), expected);
    }

    // ── Multiset (Z-set) mode: chained projections/filters over a changelog ──

    /// Single-column changelog `(v, __weight)` for the key-dropping multiset case.
    fn weight_batch_1col(rows: &[(i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let vs: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let ws: Vec<i64> = rows.iter().map(|r| r.1).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vs)),
                Arc::new(arrow::array::Int64Array::from(ws)),
            ],
        )
        .unwrap()
    }

    fn nullable_weight_batch_1col(rows: &[(i64, Option<i64>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn plain_one_col_batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            one_col_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn counted_multiset_checkpoint_batch(rows: &[(i64, i64)]) -> RecordBatch {
        let schema = multiset_checkpoint_schema(&one_col_schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn one_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
    }

    /// `v` snapshot values sorted (with multiplicity).
    fn multiset_values(store: &MvStore, name: &str) -> Vec<i64> {
        use arrow::array::Int64Array;
        let batch = store.to_record_batch(name).unwrap().unwrap();
        let vs = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut out: Vec<i64> = (0..batch.num_rows()).map(|i| vs.value(i)).collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn multiset_nets_retractions_for_keyed_rows() {
        let mut store = MvStore::new();
        store
            .create_mv("m", upsert_plain_schema(), MvStorageMode::Multiset)
            .unwrap();
        store.update("m", &changelog_batch(&[(1, 10, 1), (2, 20, 1)]));
        assert_eq!(snapshot_rows(&store, "m"), vec![(1, 10), (2, 20)]);
        // Change k=1: retract old full row, insert new full row.
        store.update("m", &changelog_batch(&[(1, 10, -1), (1, 15, 1)]));
        assert_eq!(snapshot_rows(&store, "m"), vec![(1, 15), (2, 20)]);
    }

    #[test]
    fn multiset_tracks_duplicate_rows() {
        // Key-dropping projection: two upstream keys with the same value v=10 → multiplicity 2.
        let mut store = MvStore::new();
        store
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        store.update("m", &weight_batch_1col(&[(10, 1), (10, 1), (20, 1)]));
        assert_eq!(multiset_values(&store, "m"), vec![10, 10, 20]);

        // One source of the 10 changes 10→15: retract one (10), insert (15). The other 10 survives.
        store.update("m", &weight_batch_1col(&[(10, -1), (15, 1)]));
        assert_eq!(multiset_values(&store, "m"), vec![10, 15, 20]);

        // The remaining 10 retracts → gone.
        store.update("m", &weight_batch_1col(&[(10, -1)]));
        assert_eq!(multiset_values(&store, "m"), vec![15, 20]);
    }

    #[test]
    fn multiset_cycle_is_atomic_when_a_later_batch_is_invalid() {
        let mut store = MvStore::new();
        store
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        store.update("m", &weight_batch_1col(&[(10, 1)]));
        let before = multiset_values(&store, "m");
        let before_bytes = store.total_bytes();

        let error = store
            .update_cycle(
                "m",
                &[weight_batch_1col(&[(20, 1)]), plain_one_col_batch(&[30])],
            )
            .expect_err("missing weight must reject the whole cycle");
        assert!(error.to_string().contains("missing weight"));
        assert_eq!(multiset_values(&store, "m"), before);
        assert_eq!(store.total_bytes(), before_bytes);
    }

    #[test]
    fn multiset_rejects_negative_overflow_and_null_weight_without_mutation() {
        let mut store = MvStore::new();
        store
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        store.update("m", &weight_batch_1col(&[(10, 1)]));
        let before = multiset_values(&store, "m");
        let before_bytes = store.total_bytes();

        let negative = store
            .update_cycle("m", &[weight_batch_1col(&[(10, -2)])])
            .expect_err("negative multiplicity must fail");
        assert!(negative.to_string().contains("negative multiplicity"));
        assert_eq!(multiset_values(&store, "m"), before);
        assert_eq!(store.total_bytes(), before_bytes);

        let null = store
            .update_cycle(
                "m",
                &[nullable_weight_batch_1col(&[(20, Some(1)), (10, None)])],
            )
            .expect_err("null weight must fail");
        assert!(null.to_string().contains("weight is null"));
        assert_eq!(multiset_values(&store, "m"), before);
        assert_eq!(store.total_bytes(), before_bytes);

        let mut overflow_store = MvStore::new();
        overflow_store
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        overflow_store
            .update_cycle("m", &[weight_batch_1col(&[(10, i64::MAX)])])
            .unwrap();
        let before_overflow = overflow_store.total_bytes();
        let overflow = overflow_store
            .update_cycle("m", &[weight_batch_1col(&[(10, 1)])])
            .expect_err("multiplicity overflow must fail");
        assert!(overflow.to_string().contains("multiplicity overflow"));
        let count = overflow_store
            .entries
            .get("m")
            .and_then(|entry| entry.multiset.as_ref())
            .and_then(|state| state.counts.values().next())
            .copied();
        assert_eq!(count, Some(i64::MAX));
        assert_eq!(overflow_store.total_bytes(), before_overflow);
    }

    #[test]
    fn multiset_checkpoint_round_trip_preserves_multiplicity() {
        let mut store = MvStore::new();
        store
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        store.update("m", &weight_batch_1col(&[(10, 1), (10, 1), (20, 1)]));
        let before = multiset_values(&store, "m");
        assert_eq!(before, vec![10, 10, 20]);

        let states = store.checkpoint_states().unwrap();
        let mut store2 = MvStore::new();
        store2
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        for (key, bytes) in &states {
            let name = key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap();
            store2.restore_from_ipc(name, bytes).unwrap();
        }
        // Multiplicity (10 appears twice) survives the round-trip.
        assert_eq!(multiset_values(&store2, "m"), before);
        // And a restored store keeps netting: retract one 10.
        store2.update("m", &weight_batch_1col(&[(10, -1)]));
        assert_eq!(multiset_values(&store2, "m"), vec![10, 20]);
    }

    #[test]
    fn multiset_checkpoint_is_counted_and_does_not_materialize_multiplicity() {
        let mut store = MvStore::new();
        store
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        store
            .update_cycle("m", &[weight_batch_1col(&[(10, i64::MAX)])])
            .unwrap();
        let live_bytes = store.total_bytes();

        let read_error = store.to_record_batch("m").unwrap_err();
        assert!(read_error.to_string().contains("safe row limit"));
        assert_eq!(store.total_bytes(), live_bytes);

        let states = store.checkpoint_states().unwrap();
        let bytes = states.get("mv:m").unwrap();
        let (schema, batches) = ipc_to_schema_and_batches(bytes).unwrap();
        assert_eq!(schema, multiset_checkpoint_schema(&one_col_schema()));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let counts = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), i64::MAX);

        let mut restored = MvStore::new();
        restored
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        restored.restore_from_ipc("m", bytes).unwrap();
        let restored_count = restored
            .entries
            .get("m")
            .and_then(|entry| entry.multiset.as_ref())
            .and_then(|state| state.counts.values().next())
            .copied();
        assert_eq!(restored_count, Some(i64::MAX));
        assert!(restored.to_record_batch("m").is_err());
    }

    #[test]
    fn multiset_read_rejects_excessive_expanded_bytes_before_conversion() {
        let value = "x".repeat(300);
        let plain_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let changelog_schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let changelog = RecordBatch::try_new(
            changelog_schema,
            vec![
                Arc::new(StringArray::from(vec![value.as_str()])),
                Arc::new(Int64Array::from(vec![i64::try_from(
                    MAX_MULTISET_MATERIALIZED_ROWS,
                )
                .unwrap()])),
            ],
        )
        .unwrap();
        let mut store = MvStore::new();
        store
            .create_mv("m", plain_schema, MvStorageMode::Multiset)
            .unwrap();
        store.update_cycle("m", &[changelog]).unwrap();

        let error = store.to_record_batch("m").unwrap_err();
        assert!(error.to_string().contains("safe byte limit"));
    }

    #[test]
    fn multiset_restore_rejects_invalid_counts_atomically() {
        let mut store = MvStore::new();
        store
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        store.update("m", &weight_batch_1col(&[(10, 2)]));
        let before = multiset_values(&store, "m");
        let before_bytes = store.total_bytes();

        let zero = counted_multiset_checkpoint_batch(&[(20, 0)]);
        let zero_bytes = batches_to_ipc(&zero.schema(), std::iter::once(&zero)).unwrap();
        let zero_error = store.restore_from_ipc("m", &zero_bytes).unwrap_err();
        assert!(zero_error.to_string().contains("must be positive"));
        assert_eq!(multiset_values(&store, "m"), before);
        assert_eq!(store.total_bytes(), before_bytes);

        let duplicate = counted_multiset_checkpoint_batch(&[(20, 1), (20, 2)]);
        let duplicate_bytes =
            batches_to_ipc(&duplicate.schema(), std::iter::once(&duplicate)).unwrap();
        let duplicate_error = store.restore_from_ipc("m", &duplicate_bytes).unwrap_err();
        assert!(duplicate_error.to_string().contains("duplicate value"));
        assert_eq!(multiset_values(&store, "m"), before);
        assert_eq!(store.total_bytes(), before_bytes);
    }

    #[test]
    fn multiset_restore_rejects_legacy_expanded_snapshot_atomically() {
        let mut store = MvStore::new();
        store
            .create_mv("m", one_col_schema(), MvStorageMode::Multiset)
            .unwrap();
        store.update("m", &weight_batch_1col(&[(10, 2)]));
        let before = multiset_values(&store, "m");
        let before_bytes = store.total_bytes();

        let legacy = plain_one_col_batch(&[20, 20]);
        let bytes = batches_to_ipc(&legacy.schema(), std::iter::once(&legacy)).unwrap();
        let error = store.restore_from_ipc("m", &bytes).unwrap_err();
        assert!(error.to_string().contains("schema or format mismatch"));
        assert_eq!(multiset_values(&store, "m"), before);
        assert_eq!(store.total_bytes(), before_bytes);
    }
}
