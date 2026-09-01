//! Materialized view result storage, queryable via `SELECT * FROM mv_name`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_common::ScalarValue;
use laminar_core::changelog::WEIGHT_COLUMN;

use crate::error::DbError;

mod checkpoint;

#[cfg(test)]
use checkpoint::{batches_to_ipc, ipc_to_schema_and_batches, multiset_checkpoint_schema};
pub(crate) use checkpoint::{MvCheckpointCapture, CHECKPOINT_KEY_PREFIX};

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

    #[cfg(test)]
    pub(crate) fn storage_mode_for_test(&self, name: &str) -> Option<MvStorageMode> {
        self.entries.get(name).map(|entry| entry.mode.clone())
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
}

#[cfg(test)]
mod tests;
