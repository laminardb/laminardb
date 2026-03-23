#![deny(clippy::disallowed_types)]

//! Temporal probe join execution.
//!
//! For each left event, probes the right stream at multiple fixed time offsets.
//! Each left row produces N output rows (one per offset) with the ASOF-matched
//! right value at `event_time + offset_ms`.
//!
//! State is watermark-driven: probes with `probe_ts <= watermark` are emitted
//! immediately. Remaining probes are buffered until the watermark advances.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use laminar_sql::translator::TemporalProbeConfig;

use crate::error::DbError;

enum KeyColumn<'a> {
    Utf8(&'a StringArray),
    Int64(&'a Int64Array),
}

impl KeyColumn<'_> {
    fn is_null(&self, i: usize) -> bool {
        match self {
            KeyColumn::Utf8(a) => a.is_null(i),
            KeyColumn::Int64(a) => a.is_null(i),
        }
    }

    fn hash_at(&self, i: usize) -> Option<u64> {
        if self.is_null(i) {
            return None;
        }
        let mut hasher = DefaultHasher::new();
        match self {
            KeyColumn::Utf8(a) => a.value(i).hash(&mut hasher),
            KeyColumn::Int64(a) => a.value(i).hash(&mut hasher),
        }
        Some(hasher.finish())
    }

    fn keys_equal(&self, i: usize, other: &KeyColumn<'_>, j: usize) -> bool {
        if self.is_null(i) || other.is_null(j) {
            return false;
        }
        match (self, other) {
            (KeyColumn::Utf8(a), KeyColumn::Utf8(b)) => a.value(i) == b.value(j),
            (KeyColumn::Int64(a), KeyColumn::Int64(b)) => a.value(i) == b.value(j),
            _ => false,
        }
    }
}

fn extract_key_column<'a>(
    batch: &'a RecordBatch,
    col_name: &str,
) -> Result<KeyColumn<'a>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Column '{col_name}' not found")))?;
    let array = batch.column(col_idx);
    match array.data_type() {
        DataType::Utf8 => Ok(KeyColumn::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Utf8")))?,
        )),
        DataType::Int64 => Ok(KeyColumn::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Int64")))?,
        )),
        other => Err(DbError::Pipeline(format!(
            "Unsupported key column type: {other}"
        ))),
    }
}

fn extract_timestamps(batch: &RecordBatch, col_name: &str) -> Result<Vec<i64>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Timestamp column '{col_name}' not found")))?;
    let array = batch.column(col_idx);
    match array.data_type() {
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline("Not Int64".into()))?;
            Ok(a.values().to_vec())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| DbError::Pipeline("Not TimestampMillisecond".into()))?;
            Ok(a.values().to_vec())
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DbError::Pipeline("Not Float64".into()))?;
            #[allow(clippy::cast_possible_truncation)]
            Ok(a.values().iter().map(|v| *v as i64).collect())
        }
        other => Err(DbError::Pipeline(format!(
            "Unsupported timestamp type for '{col_name}': {other}"
        ))),
    }
}

const COMPACTION_THRESHOLD: u32 = 32;

/// Per-key reference stream buffer with ASOF lookup and bounded memory.
#[derive(Debug, Default)]
struct RefBuffer {
    index: FxHashMap<u64, BTreeMap<i64, Vec<usize>>>,
    right_concat: Option<RecordBatch>,
    ingest_count: u32,
}

impl RefBuffer {
    fn ingest(
        &mut self,
        batches: &[RecordBatch],
        key_col: &str,
        time_col: &str,
    ) -> Result<(), DbError> {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(());
        }

        let schema = batches[0].schema();
        let new_batch = concat_batches(&schema, batches)
            .map_err(|e| DbError::Pipeline(format!("ref buffer concat: {e}")))?;

        if new_batch.num_rows() == 0 {
            return Ok(());
        }

        let timestamps = extract_timestamps(&new_batch, time_col)?;
        let key_hashes: Vec<Option<u64>> = {
            let keys = extract_key_column(&new_batch, key_col)?;
            (0..new_batch.num_rows()).map(|i| keys.hash_at(i)).collect()
        };

        let (merged, offset) = if let Some(ref existing) = self.right_concat {
            let offset = existing.num_rows();
            let merged = concat_batches(&schema, &[existing.clone(), new_batch])
                .map_err(|e| DbError::Pipeline(format!("ref buffer merge: {e}")))?;
            (merged, offset)
        } else {
            (new_batch, 0)
        };

        for (i, &ts) in timestamps.iter().enumerate() {
            if let Some(key_hash) = key_hashes[i] {
                self.index
                    .entry(key_hash)
                    .or_default()
                    .entry(ts)
                    .or_default()
                    .push(offset + i);
            }
        }

        self.right_concat = Some(merged);
        self.ingest_count += 1;
        Ok(())
    }

    /// ASOF lookup with key verification: find the latest right row index
    /// with `ts <= probe_ts` whose key matches the left row's key.
    fn asof_lookup(
        &self,
        key_hash: u64,
        probe_ts: i64,
        left_key: &KeyColumn<'_>,
        left_row: usize,
        right_key: &KeyColumn<'_>,
    ) -> Option<usize> {
        let btree = self.index.get(&key_hash)?;
        for (_, indices) in btree.range(..=probe_ts).rev() {
            for &idx in indices.iter().rev() {
                if left_key.keys_equal(left_row, right_key, idx) {
                    return Some(idx);
                }
            }
        }
        None
    }

    /// Evict entries with `timestamp < cutoff`, then compact if needed.
    fn evict_before(&mut self, cutoff: i64) -> Result<(), DbError> {
        for btree in self.index.values_mut() {
            let keep = btree.split_off(&cutoff);
            *btree = keep;
        }
        self.index.retain(|_, btree| !btree.is_empty());

        if self.ingest_count >= COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Rebuild `right_concat` from only the rows still referenced by the index.
    fn compact(&mut self) -> Result<(), DbError> {
        let Some(ref batch) = self.right_concat else {
            return Ok(());
        };

        let mut live_rows: Vec<usize> = Vec::new();
        for btree in self.index.values() {
            for indices in btree.values() {
                live_rows.extend_from_slice(indices);
            }
        }

        if live_rows.is_empty() {
            self.right_concat = None;
            self.index.clear();
            self.ingest_count = 0;
            return Ok(());
        }

        live_rows.sort_unstable();
        live_rows.dedup();

        let mut idx_map: FxHashMap<usize, usize> = FxHashMap::default();
        for (new_idx, &old_idx) in live_rows.iter().enumerate() {
            idx_map.insert(old_idx, new_idx);
        }

        let slices: Vec<RecordBatch> = live_rows.iter().map(|&idx| batch.slice(idx, 1)).collect();
        let schema = batch.schema();
        let compacted = concat_batches(&schema, &slices)
            .map_err(|e| DbError::Pipeline(format!("ref buffer compact: {e}")))?;

        for btree in self.index.values_mut() {
            for indices in btree.values_mut() {
                for idx in indices.iter_mut() {
                    *idx = idx_map[idx];
                }
            }
        }

        self.right_concat = Some(compacted);
        self.ingest_count = 0;
        Ok(())
    }

    fn estimated_size_bytes(&self) -> usize {
        let index_size: usize = self
            .index
            .values()
            .map(|btree| btree.len() * (8 + 8 + 24))
            .sum();
        let batch_size = self
            .right_concat
            .as_ref()
            .map_or(0, RecordBatch::get_array_memory_size);
        index_size + batch_size
    }
}

/// Full operator state.
pub(crate) struct TemporalProbeState {
    ref_buffer: RefBuffer,
    /// Pending probes from previous cycles, each holding a copy of the left row.
    carried_probes: Vec<CarriedProbe>,
    last_watermark: i64,
}

#[derive(Debug, Clone)]
struct CarriedProbe {
    left_row_batch: RecordBatch,
    key_hash: u64,
    base_ts: i64,
    remaining_offsets_ms: Vec<i64>,
}

impl TemporalProbeState {
    pub fn new() -> Self {
        Self {
            ref_buffer: RefBuffer::default(),
            carried_probes: Vec::new(),
            last_watermark: i64::MIN,
        }
    }

    pub fn estimated_size_bytes(&self) -> usize {
        let carried_size: usize = self
            .carried_probes
            .iter()
            .map(|p| {
                p.left_row_batch.get_array_memory_size() + p.remaining_offsets_ms.len() * 8 + 32
            })
            .sum();
        self.ref_buffer.estimated_size_bytes() + carried_size
    }
}

/// Serializable checkpoint for the temporal probe state.
#[derive(Serialize, Deserialize)]
pub(crate) struct TemporalProbeCheckpoint {
    ref_buffer_ipc: Vec<u8>,
    ref_index: Vec<(u64, i64, Vec<usize>)>,
    pending_probes: Vec<PendingProbeCheckpointEntry>,
    last_watermark: i64,
}

#[derive(Serialize, Deserialize)]
struct PendingProbeCheckpointEntry {
    left_row_ipc: Vec<u8>,
    key_hash: u64,
    base_ts: i64,
    remaining_offsets_ms: Vec<i64>,
}

fn batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, DbError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| DbError::Pipeline(format!("IPC write: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| DbError::Pipeline(format!("IPC write batch: {e}")))?;
        writer
            .finish()
            .map_err(|e| DbError::Pipeline(format!("IPC finish: {e}")))?;
    }
    Ok(buf)
}

fn ipc_to_batch(data: &[u8]) -> Result<RecordBatch, DbError> {
    let cursor = std::io::Cursor::new(data);
    let reader = arrow_ipc::reader::FileReader::try_new(cursor, None)
        .map_err(|e| DbError::Pipeline(format!("IPC read: {e}")))?;
    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| DbError::Pipeline(format!("IPC read batch: {e}")))?;
        batches.push(batch);
    }
    if batches.is_empty() {
        return Err(DbError::Pipeline("IPC: no batches".into()));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    let schema = batches[0].schema();
    concat_batches(&schema, &batches).map_err(|e| DbError::Pipeline(format!("IPC concat: {e}")))
}

impl TemporalProbeState {
    pub fn snapshot_checkpoint(&self) -> Result<TemporalProbeCheckpoint, DbError> {
        let ref_buffer_ipc = if let Some(ref batch) = self.ref_buffer.right_concat {
            if batch.num_rows() > 0 {
                batch_to_ipc(batch)?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let mut ref_index = Vec::new();
        for (&key_hash, btree) in &self.ref_buffer.index {
            for (&ts, indices) in btree {
                ref_index.push((key_hash, ts, indices.clone()));
            }
        }

        let mut pending_probes = Vec::new();
        for probe in &self.carried_probes {
            let left_row_ipc = batch_to_ipc(&probe.left_row_batch)?;
            pending_probes.push(PendingProbeCheckpointEntry {
                left_row_ipc,
                key_hash: probe.key_hash,
                base_ts: probe.base_ts,
                remaining_offsets_ms: probe.remaining_offsets_ms.clone(),
            });
        }

        Ok(TemporalProbeCheckpoint {
            ref_buffer_ipc,
            ref_index,
            pending_probes,
            last_watermark: self.last_watermark,
        })
    }

    pub fn from_checkpoint(cp: &TemporalProbeCheckpoint) -> Result<Self, DbError> {
        let right_concat = if cp.ref_buffer_ipc.is_empty() {
            None
        } else {
            Some(ipc_to_batch(&cp.ref_buffer_ipc)?)
        };

        let mut index: FxHashMap<u64, BTreeMap<i64, Vec<usize>>> = FxHashMap::default();
        for &(key_hash, ts, ref indices) in &cp.ref_index {
            index
                .entry(key_hash)
                .or_default()
                .insert(ts, indices.clone());
        }

        let mut carried_probes = Vec::with_capacity(cp.pending_probes.len());
        for entry in &cp.pending_probes {
            let left_row_batch = ipc_to_batch(&entry.left_row_ipc)?;
            carried_probes.push(CarriedProbe {
                left_row_batch,
                key_hash: entry.key_hash,
                base_ts: entry.base_ts,
                remaining_offsets_ms: entry.remaining_offsets_ms.clone(),
            });
        }

        Ok(Self {
            ref_buffer: RefBuffer {
                index,
                right_concat,
                ingest_count: 0,
            },
            carried_probes,
            last_watermark: cp.last_watermark,
        })
    }
}

/// Build the output schema: left columns + right columns (disambiguated) +
/// probe columns (`{alias}_offset_ms`, `{alias}_probe_ts`).
fn build_probe_output_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    config: &TemporalProbeConfig,
) -> SchemaRef {
    let mut fields: Vec<Field> = left_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    let left_names: rustc_hash::FxHashSet<&str> = left_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    for field in right_schema.fields() {
        if field.name() == &config.key_column {
            continue;
        }
        let mut f = field.as_ref().clone().with_nullable(true);
        if left_names.contains(f.name().as_str()) {
            let suffixed = format!("{}_{}", f.name(), config.right_table);
            f = f.with_name(suffixed);
        }
        fields.push(f);
    }

    fields.push(Field::new(
        format!("{}_offset_ms", config.probe_alias),
        DataType::Int64,
        false,
    ));
    fields.push(Field::new(
        format!("{}_probe_ts", config.probe_alias),
        DataType::Int64,
        false,
    ));

    Arc::new(Schema::new(fields))
}

/// Execute one cycle of the temporal probe join.
///
/// 1. Ingest right-side batches into the ref buffer.
/// 2. For each left row, compute probe timestamps for all offsets.
/// 3. Emit immediately for probes where `watermark >= probe_ts`.
/// 4. Buffer remaining probes as `carried_probes`.
/// 5. Resolve carried probes from previous cycles.
/// 6. Evict ref buffer entries no longer needed.
#[allow(clippy::too_many_lines)]
pub(crate) fn execute_temporal_probe_cycle(
    state: &mut TemporalProbeState,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &TemporalProbeConfig,
    watermark: i64,
) -> Result<Vec<RecordBatch>, DbError> {
    state
        .ref_buffer
        .ingest(right_batches, &config.key_column, &config.right_time_column)?;

    let offsets = &config.expanded_offsets_ms;
    if offsets.is_empty() {
        return Ok(Vec::new());
    }

    let mut output_batches = Vec::new();

    // Process new left-side events
    if !left_batches.is_empty() && left_batches.iter().any(|b| b.num_rows() > 0) {
        let left_schema = left_batches[0].schema();
        let left_concat = concat_batches(&left_schema, left_batches)
            .map_err(|e| DbError::Pipeline(format!("temporal probe left concat: {e}")))?;

        if left_concat.num_rows() > 0 {
            let left_keys = extract_key_column(&left_concat, &config.key_column)?;
            let left_ts = extract_timestamps(&left_concat, &config.left_time_column)?;

            let right_schema = state
                .ref_buffer
                .right_concat
                .as_ref()
                .map(RecordBatch::schema);
            let output_schema = if let Some(ref rs) = right_schema {
                build_probe_output_schema(&left_schema, rs, config)
            } else {
                build_probe_output_schema(&left_schema, &Arc::new(Schema::empty()), config)
            };

            let right_key_col = state
                .ref_buffer
                .right_concat
                .as_ref()
                .map(|rc| extract_key_column(rc, &config.key_column))
                .transpose()?;

            let mut emit_left_indices = Vec::new();
            let mut emit_right_indices = Vec::new();
            let mut emit_offset_ms = Vec::new();
            let mut emit_probe_ts = Vec::new();
            let mut new_carried = Vec::new();

            for (row_idx, &base_ts) in left_ts.iter().enumerate() {
                let Some(key_hash) = left_keys.hash_at(row_idx) else {
                    continue;
                };

                let mut remaining = Vec::new();

                for &offset_ms in offsets {
                    let probe_ts = base_ts.saturating_add(offset_ms);

                    if watermark >= probe_ts {
                        let right_idx = if let Some(ref rk) = right_key_col {
                            state
                                .ref_buffer
                                .asof_lookup(key_hash, probe_ts, &left_keys, row_idx, rk)
                        } else {
                            None
                        };
                        emit_left_indices.push(row_idx);
                        emit_right_indices.push(right_idx);
                        emit_offset_ms.push(offset_ms);
                        emit_probe_ts.push(probe_ts);
                    } else {
                        remaining.push(offset_ms);
                    }
                }

                if !remaining.is_empty() {
                    new_carried.push(CarriedProbe {
                        left_row_batch: left_concat.slice(row_idx, 1),
                        key_hash,
                        base_ts,
                        remaining_offsets_ms: remaining,
                    });
                }
            }

            if !emit_left_indices.is_empty() {
                let batch = build_output_batch(
                    &left_concat,
                    state.ref_buffer.right_concat.as_ref(),
                    &emit_left_indices,
                    &emit_right_indices,
                    &emit_offset_ms,
                    &emit_probe_ts,
                    &output_schema,
                    config,
                )?;
                if batch.num_rows() > 0 {
                    output_batches.push(batch);
                }
            }

            state.carried_probes.extend(new_carried);
        }
    }

    // Resolve carried probes from previous cycles
    if !state.carried_probes.is_empty() && watermark > state.last_watermark {
        let mut still_pending = Vec::new();
        #[allow(clippy::type_complexity)]
        let mut carried_emissions: Vec<(
            RecordBatch,
            Vec<Option<usize>>,
            Vec<i64>,
            Vec<i64>,
        )> = Vec::new();

        let right_key_col = state
            .ref_buffer
            .right_concat
            .as_ref()
            .map(|rc| extract_key_column(rc, &config.key_column))
            .transpose()?;

        for probe in std::mem::take(&mut state.carried_probes) {
            let mut emit_right = Vec::new();
            let mut emit_offsets = Vec::new();
            let mut emit_pts = Vec::new();
            let mut remaining = Vec::new();

            let left_key_col = extract_key_column(&probe.left_row_batch, &config.key_column)?;

            for &offset_ms in &probe.remaining_offsets_ms {
                let probe_ts = probe.base_ts.saturating_add(offset_ms);
                if watermark >= probe_ts {
                    let right_idx = if let Some(ref rk) = right_key_col {
                        state
                            .ref_buffer
                            .asof_lookup(probe.key_hash, probe_ts, &left_key_col, 0, rk)
                    } else {
                        None
                    };
                    emit_right.push(right_idx);
                    emit_offsets.push(offset_ms);
                    emit_pts.push(probe_ts);
                } else {
                    remaining.push(offset_ms);
                }
            }

            if !emit_right.is_empty() {
                carried_emissions.push((
                    probe.left_row_batch.clone(),
                    emit_right,
                    emit_offsets,
                    emit_pts,
                ));
            }

            if !remaining.is_empty() {
                still_pending.push(CarriedProbe {
                    remaining_offsets_ms: remaining,
                    ..probe
                });
            }
        }

        state.carried_probes = still_pending;

        for (left_row, right_indices, offset_ms_vec, probe_ts_vec) in carried_emissions {
            let right_schema = state
                .ref_buffer
                .right_concat
                .as_ref()
                .map(RecordBatch::schema);
            let output_schema = if let Some(ref rs) = right_schema {
                build_probe_output_schema(&left_row.schema(), rs, config)
            } else {
                build_probe_output_schema(&left_row.schema(), &Arc::new(Schema::empty()), config)
            };

            let n = right_indices.len();
            let left_indices: Vec<usize> = vec![0; n];

            let batch = build_output_batch(
                &left_row,
                state.ref_buffer.right_concat.as_ref(),
                &left_indices,
                &right_indices,
                &offset_ms_vec,
                &probe_ts_vec,
                &output_schema,
                config,
            )?;
            if batch.num_rows() > 0 {
                output_batches.push(batch);
            }
        }
    }

    // Evict — safe cutoff that preserves data needed by carried probes
    if watermark > state.last_watermark {
        let min_offset = config.min_offset_ms();
        let base_cutoff = if min_offset < 0 {
            watermark.saturating_add(min_offset)
        } else {
            watermark
        };

        let min_pending_probe_ts = state
            .carried_probes
            .iter()
            .flat_map(|p| {
                p.remaining_offsets_ms
                    .iter()
                    .map(|&o| p.base_ts.saturating_add(o))
            })
            .min();

        let eviction_cutoff = match min_pending_probe_ts {
            Some(pts) => base_cutoff.min(pts),
            None => base_cutoff,
        };

        if eviction_cutoff > state.last_watermark {
            state.ref_buffer.evict_before(eviction_cutoff)?;
        }
        state.last_watermark = watermark;
    }

    Ok(output_batches)
}

#[allow(clippy::too_many_arguments)]
fn build_output_batch(
    left: &RecordBatch,
    right: Option<&RecordBatch>,
    left_indices: &[usize],
    right_indices: &[Option<usize>],
    offset_ms: &[i64],
    probe_ts: &[i64],
    output_schema: &SchemaRef,
    config: &TemporalProbeConfig,
) -> Result<RecordBatch, DbError> {
    let num_rows = left_indices.len();
    if num_rows == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    #[allow(clippy::cast_possible_truncation)]
    let left_idx_array =
        arrow::array::UInt32Array::from(left_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
    for col_idx in 0..left.num_columns() {
        let taken = arrow::compute::take(left.column(col_idx), &left_idx_array, None)
            .map_err(|e| DbError::Pipeline(format!("temporal probe left take: {e}")))?;
        columns.push(taken);
    }

    if let Some(right) = right {
        let right_schema = right.schema();
        for col_idx in 0..right.num_columns() {
            let field_name = right_schema.field(col_idx).name();
            if field_name == &config.key_column {
                continue;
            }
            let array = right.column(col_idx);
            let taken = take_with_nulls(array, right_indices, num_rows)?;
            columns.push(taken);
        }
    } else {
        let left_col_count = left.num_columns();
        let probe_col_count = 2;
        let right_col_count = output_schema.fields().len() - left_col_count - probe_col_count;
        for i in 0..right_col_count {
            let field = output_schema.field(left_col_count + i);
            columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
        }
    }

    columns.push(Arc::new(Int64Array::from(offset_ms.to_vec())));
    columns.push(Arc::new(Int64Array::from(probe_ts.to_vec())));

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| DbError::Pipeline(format!("temporal probe output: {e}")))
}

fn take_with_nulls(
    array: &dyn Array,
    indices: &[Option<usize>],
    num_rows: usize,
) -> Result<ArrayRef, DbError> {
    if array.is_empty() {
        return Ok(arrow::array::new_null_array(array.data_type(), num_rows));
    }
    #[allow(clippy::cast_possible_truncation)]
    let idx_array = arrow::array::UInt32Array::from(
        indices
            .iter()
            .map(|opt| opt.map(|i| i as u32))
            .collect::<Vec<Option<u32>>>(),
    );
    arrow::compute::take(array, &idx_array, None)
        .map_err(|e| DbError::Pipeline(format!("temporal probe right take: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_sql::translator::ProbeOffsetSpec;

    fn trades_batch(symbols: &[&str], timestamps: &[i64], prices: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(symbols.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    fn market_batch(symbols: &[&str], timestamps: &[i64], prices: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("mts", DataType::Int64, false),
            Field::new("mprice", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(symbols.to_vec())),
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    fn test_config(offsets: &ProbeOffsetSpec) -> TemporalProbeConfig {
        TemporalProbeConfig::new(
            "trades".into(),
            "market_data".into(),
            None,
            None,
            "symbol".into(),
            "ts".into(),
            "mts".into(),
            offsets,
            "p".into(),
        )
    }

    #[test]
    fn test_basic_probe_all_resolved() {
        let config = test_config(&ProbeOffsetSpec::List(vec![-5000, -1000, 0]));
        let mut state = TemporalProbeState::new();

        let market = market_batch(
            &["AAPL", "AAPL", "AAPL"],
            &[90_000, 95_000, 100_000],
            &[150.0, 151.0, 152.0],
        );
        let trades = trades_batch(&["AAPL"], &[100_000], &[152.5]);

        let result =
            execute_temporal_probe_cycle(&mut state, &[trades], &[market], &config, 100_000)
                .unwrap();

        let total_rows: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3);

        let batch = &result[0];
        let offsets = batch
            .column_by_name("p_offset_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(offsets.value(0), -5000);
        assert_eq!(offsets.value(1), -1000);
        assert_eq!(offsets.value(2), 0);

        let pts = batch
            .column_by_name("p_probe_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(pts.value(0), 95_000);
        assert_eq!(pts.value(1), 99_000);
        assert_eq!(pts.value(2), 100_000);

        assert!(state.carried_probes.is_empty());
    }

    #[test]
    fn test_pending_probes_resolved_on_watermark_advance() {
        let config = test_config(&ProbeOffsetSpec::List(vec![0, 5000, 10_000]));
        let mut state = TemporalProbeState::new();

        let market = market_batch(
            &["AAPL", "AAPL", "AAPL"],
            &[100_000, 105_000, 110_000],
            &[150.0, 155.0, 160.0],
        );
        let trades = trades_batch(&["AAPL"], &[100_000], &[152.5]);

        let result =
            execute_temporal_probe_cycle(&mut state, &[trades], &[market], &config, 102_000)
                .unwrap();

        let total: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1);
        assert_eq!(state.carried_probes.len(), 1);

        let result2 = execute_temporal_probe_cycle(&mut state, &[], &[], &config, 112_000).unwrap();

        let total2: usize = result2.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total2, 2);
        assert!(state.carried_probes.is_empty());
    }

    #[test]
    fn test_multi_key_independence() {
        let config = test_config(&ProbeOffsetSpec::List(vec![0]));
        let mut state = TemporalProbeState::new();

        let market = market_batch(&["AAPL", "GOOG"], &[100_000, 100_000], &[150.0, 2800.0]);
        let trades = trades_batch(&["AAPL", "GOOG"], &[100_000, 100_000], &[150.5, 2801.0]);

        let result =
            execute_temporal_probe_cycle(&mut state, &[trades], &[market], &config, 100_000)
                .unwrap();

        let total: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 2);

        let batch = &result[0];
        let mprices = batch
            .column_by_name("mprice")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((mprices.value(0) - 150.0).abs() < f64::EPSILON);
        assert!((mprices.value(1) - 2800.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_range_equals_list() {
        let range_config = test_config(&ProbeOffsetSpec::Range {
            start_ms: 0,
            end_ms: 3000,
            step_ms: 1000,
        });
        let list_config = test_config(&ProbeOffsetSpec::List(vec![0, 1000, 2000, 3000]));
        assert_eq!(
            range_config.expanded_offsets_ms,
            list_config.expanded_offsets_ms
        );
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let config = test_config(&ProbeOffsetSpec::List(vec![0, 5000]));
        let mut state = TemporalProbeState::new();

        let market = market_batch(&["AAPL"], &[100_000], &[150.0]);
        let trades = trades_batch(&["AAPL"], &[100_000], &[152.5]);

        let _ = execute_temporal_probe_cycle(&mut state, &[trades], &[market], &config, 102_000)
            .unwrap();

        assert_eq!(state.carried_probes.len(), 1);

        let cp = state.snapshot_checkpoint().unwrap();
        let data = serde_json::to_vec(&cp).unwrap();

        let cp2: TemporalProbeCheckpoint = serde_json::from_slice(&data).unwrap();
        let mut state2 = TemporalProbeState::from_checkpoint(&cp2).unwrap();

        assert_eq!(state2.carried_probes.len(), 1);
        assert_eq!(state2.last_watermark, 102_000);

        let result = execute_temporal_probe_cycle(&mut state2, &[], &[], &config, 110_000).unwrap();

        let total: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1);
        assert!(state2.carried_probes.is_empty());
    }

    #[test]
    fn test_empty_inputs() {
        let config = test_config(&ProbeOffsetSpec::List(vec![0]));
        let mut state = TemporalProbeState::new();
        let result = execute_temporal_probe_cycle(&mut state, &[], &[], &config, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_no_right_data_produces_nulls() {
        let config = test_config(&ProbeOffsetSpec::List(vec![0]));
        let mut state = TemporalProbeState::new();

        let trades = trades_batch(&["AAPL"], &[100_000], &[150.0]);
        let result =
            execute_temporal_probe_cycle(&mut state, &[trades], &[], &config, 100_000).unwrap();

        let total: usize = result.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1);
        assert_eq!(result[0].num_columns(), 5); // left(3) + probe(2)
    }

    #[test]
    fn test_eviction_preserves_data_for_carried_probes() {
        let config = test_config(&ProbeOffsetSpec::List(vec![0, 60_000]));
        let mut state = TemporalProbeState::new();

        // Ref data at 100k and 160k
        let market = market_batch(&["AAPL", "AAPL"], &[100_000, 160_000], &[150.0, 155.0]);
        let trades = trades_batch(&["AAPL"], &[100_000], &[152.5]);

        // Cycle 1: wm=102k, offset=0 resolves, offset=60k carried
        let r1 = execute_temporal_probe_cycle(&mut state, &[trades], &[market], &config, 102_000)
            .unwrap();
        assert_eq!(r1.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(state.carried_probes.len(), 1);

        // Cycle 2: wm=150k — past event time but before probe_ts=160k
        let r2 = execute_temporal_probe_cycle(&mut state, &[], &[], &config, 150_000).unwrap();
        assert!(r2.is_empty());

        // Cycle 3: wm=165k — carried probe resolves
        let r3 = execute_temporal_probe_cycle(&mut state, &[], &[], &config, 165_000).unwrap();
        let total: usize = r3.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1);

        // Verify the matched right value is 155.0 (at ts=160k), not null
        let batch = &r3[0];
        let mprices = batch
            .column_by_name("mprice")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((mprices.value(0) - 155.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compaction_reduces_memory() {
        let config = test_config(&ProbeOffsetSpec::List(vec![0]));
        let mut state = TemporalProbeState::new();

        // Ingest enough batches to trigger compaction
        for i in 0..40 {
            let ts = i64::from(i) * 1000;
            let market = market_batch(&["AAPL"], &[ts], &[100.0 + f64::from(i)]);
            execute_temporal_probe_cycle(&mut state, &[], &[market], &config, 0).unwrap();
        }

        let size_before = state.ref_buffer.estimated_size_bytes();
        assert!(size_before > 0);

        // Advance watermark to evict old entries — triggers compaction
        execute_temporal_probe_cycle(&mut state, &[], &[], &config, 35_000).unwrap();

        let size_after = state.ref_buffer.estimated_size_bytes();
        assert!(
            size_after < size_before,
            "compaction should reduce memory: before={size_before}, after={size_after}"
        );
    }

    #[test]
    fn test_state_eviction() {
        let config = test_config(&ProbeOffsetSpec::List(vec![0]));
        let mut state = TemporalProbeState::new();

        let market = market_batch(&["AAPL"], &[100_000], &[150.0]);
        let trades = trades_batch(&["AAPL"], &[100_000], &[152.5]);

        let _ = execute_temporal_probe_cycle(&mut state, &[trades], &[market], &config, 100_000)
            .unwrap();

        let _ = execute_temporal_probe_cycle(&mut state, &[], &[], &config, 200_000).unwrap();

        assert!(
            state.ref_buffer.index.is_empty() || {
                state.ref_buffer.index.values().all(BTreeMap::is_empty)
            }
        );
    }
}
