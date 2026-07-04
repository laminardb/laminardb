//! Materialized view result storage, queryable via `SELECT * FROM mv_name`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
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

/// A resolved upsert row mutation, staged so a changelog batch applies all-or-nothing.
enum Mutation {
    Insert(OwnedRow, Vec<ScalarValue>),
    Remove(OwnedRow),
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

    /// `+weight` upserts the row, `−weight` deletes the key. Retracts precede inserts in a
    /// batch, so a changed key nets to the new row.
    fn apply(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (weights, plain_cols) = weight_and_plain_cols(batch)?;
        let keys = self.keys(batch)?;

        // Resolve all mutations before mutating state, so a fallible row leaves the batch all-or-nothing.
        let mut staged = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
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
                staged.push(Mutation::Insert(key, vals));
            } else if w < 0 {
                staged.push(Mutation::Remove(key));
            }
            // w == 0 is a net-zero changelog row: no-op, matching MultisetState::apply.
        }
        // All rows validated — apply in order (preserves net retract-before-insert per key).
        for m in staged {
            match m {
                Mutation::Insert(key, vals) => {
                    let added: usize = vals.iter().map(ScalarValue::size).sum();
                    if let Some(old) = self.rows.insert(key, vals) {
                        self.approx_bytes = self
                            .approx_bytes
                            .saturating_sub(old.iter().map(ScalarValue::size).sum());
                    }
                    self.approx_bytes += added;
                }
                Mutation::Remove(key) => {
                    if let Some(old) = self.rows.remove(&key) {
                        self.approx_bytes = self
                            .approx_bytes
                            .saturating_sub(old.iter().map(ScalarValue::size).sum());
                    }
                }
            }
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
            self.approx_bytes += vals.iter().map(ScalarValue::size).sum::<usize>();
            self.rows.insert(key, vals);
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
    row_converter: RowConverter,
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
        let row_converter = RowConverter::new(sort_fields)
            .map_err(|e| DbError::Storage(format!("multiset MV row converter: {e}")))?;
        Ok(Self {
            row_converter,
            counts: HashMap::new(),
            approx_bytes: 0,
        })
    }

    /// Apply a `__weight` changelog: the full (weightless) row's multiplicity += weight; drop at 0.
    fn apply(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
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
            let w = weights.value(row_idx);
            if w == 0 {
                continue;
            }
            let key = rows.row(row_idx).owned();
            let new_count = self.counts.get(&key).copied().unwrap_or(0) + w;
            if new_count <= 0 {
                self.counts.remove(&key);
            } else {
                self.counts.insert(key, new_count);
            }
        }
        self.approx_bytes = self.counts.len() * APPROX_BYTES_PER_MULTISET_ROW;
        Ok(())
    }

    /// Restore from a materialized snapshot: each row occurrence increments its multiplicity.
    fn load_snapshot(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let rows = self
            .row_converter
            .convert_columns(batch.columns())
            .map_err(|e| DbError::Storage(format!("multiset MV restore conversion: {e}")))?;
        for row_idx in 0..batch.num_rows() {
            let key = rows.row(row_idx).owned();
            *self.counts.entry(key).or_insert(0) += 1;
        }
        self.approx_bytes = self.counts.len() * APPROX_BYTES_PER_MULTISET_ROW;
        Ok(())
    }

    fn to_record_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        if self.counts.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        let mut rows: Vec<arrow::row::Row> = Vec::new();
        for (key, &count) in &self.counts {
            for _ in 0..count.max(0) {
                rows.push(key.row());
            }
        }
        let arrays = self
            .row_converter
            .convert_rows(rows.iter().copied())
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
                    self.approx_bytes += batch.get_array_memory_size();
                    self.batches.push_back(batch.clone());
                }
            }
            MvStorageMode::Append { max_batches } => {
                let max_batches = *max_batches;
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.approx_bytes += batch.get_array_memory_size();
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
                    // apply() is atomic (a failed batch leaves the prior snapshot); attempt every
                    // batch so one bad batch can't drop later batches' deltas, and surface the first.
                    let mut first_err = None;
                    for batch in batches {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        if let Err(e) = up.apply(batch) {
                            first_err.get_or_insert(e);
                        }
                    }
                    self.approx_bytes = up.approx_bytes;
                    if let Some(e) = first_err {
                        return Err(e);
                    }
                }
            }
            MvStorageMode::Multiset => {
                if let Some(ms) = self.multiset.as_mut() {
                    let mut first_err = None;
                    for batch in batches {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        if let Err(e) = ms.apply(batch) {
                            first_err.get_or_insert(e);
                        }
                    }
                    self.approx_bytes = ms.approx_bytes;
                    if let Some(e) = first_err {
                        return Err(e);
                    }
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

    /// Apply one cycle's output batches to an MV (see `MvEntry::update_cycle`).
    pub fn update_cycle(&mut self, name: &str, batches: &[RecordBatch]) {
        if let Some(entry) = self.entries.get_mut(name) {
            if let Err(e) = entry.update_cycle(batches) {
                tracing::error!(
                    mv = %name,
                    error = %e,
                    "MV update failed — snapshot may diverge from the changelog"
                );
            }
        }
    }

    #[cfg(test)]
    pub fn update(&mut self, name: &str, batch: &RecordBatch) {
        self.update_cycle(name, std::slice::from_ref(batch));
    }

    pub fn to_record_batch(&self, name: &str) -> Result<Option<RecordBatch>, DbError> {
        self.entries
            .get(name)
            .map(MvEntry::to_record_batch)
            .transpose()
    }

    pub fn total_bytes(&self) -> usize {
        self.entries.values().map(|e| e.approx_bytes).sum()
    }

    /// Serialize all MV results for checkpoint; keys are `"mv:{name}"`.
    pub fn checkpoint_states(&self) -> Result<HashMap<String, bytes::Bytes>, DbError> {
        let mut out = HashMap::new();
        for (name, entry) in &self.entries {
            let bytes = if entry.upsert.is_some() || entry.multiset.is_some() {
                // Upsert/Multiset serialize a snapshot; propagate a failure (naming the MV) rather
                // than silently omitting it — recovery would restore the MV empty.
                let batch = entry.to_record_batch().map_err(|e| {
                    DbError::Checkpoint(format!("MV '{name}' checkpoint snapshot failed: {e}"))
                })?;
                if batch.num_rows() == 0 {
                    continue;
                }
                batches_to_ipc(&entry.schema, std::iter::once(&batch))?
            } else {
                if entry.batches.is_empty() {
                    continue;
                }
                batches_to_ipc(&entry.schema, &entry.batches)?
            };
            out.insert(format!("mv:{name}"), bytes::Bytes::from(bytes));
        }
        Ok(out)
    }

    /// Restore a single MV from checkpoint IPC bytes; `Ok(false)` if not registered.
    pub fn restore_from_ipc(&mut self, name: &str, bytes: &[u8]) -> Result<bool, DbError> {
        let Some(entry) = self.entries.get_mut(name) else {
            return Ok(false);
        };
        let batches = ipc_to_batches(bytes)
            .map_err(|e| DbError::Storage(format!("MV restore '{name}': {e}")))?;
        // Reject stale checkpoints from before a schema change rather than panicking later.
        if let Some(first) = batches.first() {
            if first.schema() != entry.schema {
                return Err(DbError::Storage(format!(
                    "MV '{name}' schema mismatch on restore (checkpoint has {}, current has {})",
                    first.schema().fields().len(),
                    entry.schema.fields().len(),
                )));
            }
        }
        if let Some(up) = entry.upsert.as_mut() {
            up.rows.clear();
            up.approx_bytes = 0;
            for b in &batches {
                up.load_snapshot(b)?;
            }
            entry.approx_bytes = up.approx_bytes;
            return Ok(true);
        }
        if let Some(ms) = entry.multiset.as_mut() {
            ms.counts.clear();
            ms.approx_bytes = 0;
            for b in &batches {
                ms.load_snapshot(b)?;
            }
            entry.approx_bytes = ms.approx_bytes;
            return Ok(true);
        }
        entry.batches.clear();
        entry.approx_bytes = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        entry.batches.extend(batches);
        Ok(true)
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

pub(crate) fn ipc_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    reader.into_iter().collect()
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
        store.update_cycle(
            "agg",
            &[
                make_batch(&[1, 2], &["a", "b"], &[1.0, 2.0]),
                make_batch(&[3, 4], &["c", "d"], &[3.0, 4.0]),
                make_batch(&[5], &["e"], &[5.0]),
            ],
        );
        assert_eq!(store.to_record_batch("agg").unwrap().unwrap().num_rows(), 5);

        // The next cycle replaces the whole result set.
        store.update_cycle("agg", &[make_batch(&[9], &["z"], &[9.0])]);
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
            assert!(store2.restore_from_ipc(name, bytes).unwrap());
        }
        assert_eq!(
            store2.to_record_batch("agg").unwrap().unwrap().num_rows(),
            2
        );
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
            assert!(store2.restore_from_ipc(name, bytes).unwrap());
        }
        assert_eq!(snapshot_rows(&store2, "u"), before);

        // A restored store keeps applying changelog correctly.
        store2.update("u", &changelog_batch(&[(1, 10, -1), (1, 99, 1)]));
        assert_eq!(snapshot_rows(&store2, "u"), vec![(1, 99), (3, 30)]);
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
            assert!(store2.restore_from_ipc(name, bytes).unwrap());
        }
        // Multiplicity (10 appears twice) survives the round-trip.
        assert_eq!(multiset_values(&store2, "m"), before);
        // And a restored store keeps netting: retract one 10.
        store2.update("m", &weight_batch_1col(&[(10, -1)]));
        assert_eq!(multiset_values(&store2, "m"), vec![10, 20]);
    }
}
