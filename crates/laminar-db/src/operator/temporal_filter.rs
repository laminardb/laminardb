//! Retracting temporal-filter operator: `WHERE time_col CMP now() ± INTERVAL`
//! as a Z-set changelog (+1 on entry, -1 on age-out). `now()` is the
//! second-floored watermark; aging is event-time driven so silent sources
//! never retract.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;

use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::TemporalFilterConfig;

/// Live buffer encoded as one Arrow IPC batch (data + `__enter`/`__exit`/
/// `__inserted` bookkeeping columns).
#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub(crate) struct TemporalFilterCheckpoint {
    fingerprint: u64,
    last_frontier: i64,
    batch_ipc: Vec<u8>,
}

/// `enter_key` = smallest frontier at which the row is a member
/// (`i64::MIN` ⇒ no upper bound); `exit_key` = smallest frontier at which
/// it ceases (`i64::MAX` ⇒ no lower bound). Rows share an
/// `Arc<RecordBatch>` so the per-row cost is one Arc bump.
#[derive(Clone)]
struct BufferedRow {
    batch: Arc<RecordBatch>,
    row_idx: u32,
    enter_key: i64,
    exit_key: i64,
}

/// Which map a row lives in *is* its membership state; `len` is
/// maintained at every push/remove so the per-cycle metric is O(1).
struct TfState {
    by_enter: BTreeMap<i64, Vec<BufferedRow>>,
    by_exit: BTreeMap<i64, Vec<BufferedRow>>,
    /// `i64::MIN` = no frontier seen yet, so any first watermark
    /// (including negative epoch ms) drives `advance`.
    last_frontier: i64,
    len: usize,
}

impl Default for TfState {
    fn default() -> Self {
        Self {
            by_enter: BTreeMap::new(),
            by_exit: BTreeMap::new(),
            last_frontier: i64::MIN,
            len: 0,
        }
    }
}

impl TfState {
    #[inline]
    fn buffered_rows(&self) -> usize {
        self.len
    }
    #[inline]
    fn push_enter(&mut self, key: i64, row: BufferedRow) {
        self.by_enter.entry(key).or_default().push(row);
        self.len += 1;
    }
    #[inline]
    fn push_exit(&mut self, key: i64, row: BufferedRow) {
        self.by_exit.entry(key).or_default().push(row);
        self.len += 1;
    }
    /// Drain a key from a map; returns the rows. Caller updates `len`
    /// based on what it does with them.
    #[inline]
    fn remove_from(map: &mut BTreeMap<i64, Vec<BufferedRow>>, k: i64) -> Vec<BufferedRow> {
        map.remove(&k).unwrap_or_default()
    }
}

/// `(enter_key, exit_key)` for a row at event time `t_ms` under `cfg`.
/// Saturating throughout so a pathological timestamp can never panic.
fn keys_for(t_ms: i64, cfg: &TemporalFilterConfig) -> (i64, i64) {
    // Lower bound `time_col {>|>=} now()+off` ⇒ member while
    // `W {<|<=} t-off`. Non-member (exit) at the first `W >= exit_key`.
    let exit_key = match cfg.lower {
        Some(b) => {
            let base = t_ms.saturating_sub(b.off_ms);
            if b.strict {
                base
            } else {
                base.saturating_add(1)
            }
        }
        None => i64::MAX,
    };
    // Upper bound `time_col {<|<=} now()+off` ⇒ member once
    // `W {>|>=} t-off`. Member at the first `W >= enter_key`.
    let enter_key = match cfg.upper {
        Some(b) => {
            let base = t_ms.saturating_sub(b.off_ms);
            if b.strict {
                base.saturating_add(1)
            } else {
                base
            }
        }
        None => i64::MIN,
    };
    (enter_key, exit_key)
}

fn take_for_schema(
    batch: &RecordBatch,
    indices: &[u32],
    schema: &SchemaRef,
) -> Result<RecordBatch, DbError> {
    let idx_arr = UInt32Array::from(indices.to_vec());
    let cols: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &idx_arr, None))
        .collect::<Result<_, _>>()
        .map_err(|e| DbError::Pipeline(format!("temporal filter take: {e}")))?;
    RecordBatch::try_new(Arc::clone(schema), cols)
        .map_err(|e| DbError::Pipeline(format!("temporal filter take batch: {e}")))
}

fn take_with_weights(
    batch: &RecordBatch,
    indices: &[u32],
    weights: &[i64],
    output_schema: &SchemaRef,
) -> Result<RecordBatch, DbError> {
    let idx_arr = UInt32Array::from(indices.to_vec());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);
    for col in batch.columns() {
        cols.push(
            arrow::compute::take(col, &idx_arr, None)
                .map_err(|e| DbError::Pipeline(format!("temporal filter take: {e}")))?,
        );
    }
    cols.push(Arc::new(Int64Array::from(weights.to_vec())));
    RecordBatch::try_new(Arc::clone(output_schema), cols)
        .map_err(|e| DbError::Pipeline(format!("temporal filter delta batch: {e}")))
}

fn time_col_to_ms(col: &ArrayRef) -> Result<Vec<Option<i64>>, DbError> {
    macro_rules! map_ts {
        ($arr:ty, $f:expr) => {{
            let a = col
                .as_any()
                .downcast_ref::<$arr>()
                .ok_or_else(|| DbError::Pipeline("temporal filter: time column downcast".into()))?;
            #[allow(clippy::redundant_closure_call)]
            Ok(a.iter().map(|v| v.map($f)).collect())
        }};
    }
    match col.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            map_ts!(TimestampNanosecondArray, |v: i64| v.div_euclid(1_000_000))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            map_ts!(TimestampMicrosecondArray, |v: i64| v.div_euclid(1_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            map_ts!(TimestampMillisecondArray, |v: i64| v)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            map_ts!(TimestampSecondArray, |v: i64| v.saturating_mul(1_000))
        }
        // Epoch milliseconds, consistent with the engine's watermark unit.
        DataType::Int64 => map_ts!(Int64Array, |v: i64| v),
        other => Err(DbError::Unsupported(format!(
            "[LDB-1001] retracting temporal filter time column has unsupported \
             type {other:?}; expected a TIMESTAMP or INT64 (epoch-ms) column"
        ))),
    }
}

pub(crate) struct TemporalFilterOperator {
    op_name: Arc<str>,
    sql: String,
    cfg: TemporalFilterConfig,
    input_schema: Option<SchemaRef>,
    /// Output minus `__weight`; may be set early by `restore`.
    row_schema: Option<SchemaRef>,
    output_schema: Option<SchemaRef>,
    /// Input-column indices to buffer in projected order; empty for `SELECT *`.
    proj_indices: Vec<usize>,
    time_idx: usize,
    state: TfState,
    prom: Option<Arc<EngineMetrics>>,
    /// Cap on far-future buffering (`LAMINAR_TEMPORAL_FILTER_HORIZON_MS`,
    /// falling back to `LAMINAR_MAX_FUTURE_SKEW_MS`; `0` disables).
    max_future_ms: i64,
}

impl TemporalFilterOperator {
    pub(crate) fn new(
        name: &str,
        sql: &str,
        cfg: TemporalFilterConfig,
        prom: Option<Arc<EngineMetrics>>,
    ) -> Self {
        // Dedicated knob so disabling the source-side future-skew guard
        // doesn't simultaneously uncap the temporal-filter future buffer.
        let max_future_ms = match std::env::var("LAMINAR_TEMPORAL_FILTER_HORIZON_MS")
            .or_else(|_| std::env::var("LAMINAR_MAX_FUTURE_SKEW_MS"))
        {
            Ok(v) => v
                .parse::<i64>()
                .unwrap_or(laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS),
            Err(_) => laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS,
        };
        Self {
            op_name: Arc::from(name),
            sql: sql.to_string(),
            cfg,
            input_schema: None,
            row_schema: None,
            output_schema: None,
            proj_indices: Vec::new(),
            time_idx: usize::MAX,
            state: TfState::default(),
            prom,
            max_future_ms,
        }
    }

    /// Second-floored frontier — matches `apply_dynamic_now_filter`.
    fn floor_frontier(wm_ms: i64) -> i64 {
        wm_ms.div_euclid(1000).saturating_mul(1000)
    }

    fn resolve_schema(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if self.input_schema.is_some() {
            return Ok(());
        }
        let schema = batch.schema();
        let col_idx = |name: &str| -> Option<usize> {
            schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .or_else(|| {
                    schema
                        .fields()
                        .iter()
                        .position(|f| f.name().eq_ignore_ascii_case(name))
                })
        };
        self.time_idx = col_idx(&self.cfg.time_col).ok_or_else(|| {
            DbError::Unsupported(format!(
                "[LDB-1001] retracting temporal filter time column `{}` not \
                 found in source `{}`",
                self.cfg.time_col, self.cfg.source_table
            ))
        })?;
        self.proj_indices = if self.cfg.proj_cols.is_empty() {
            (0..schema.fields().len()).collect()
        } else {
            let mut idx = Vec::with_capacity(self.cfg.proj_cols.len());
            for name in &self.cfg.proj_cols {
                idx.push(col_idx(name).ok_or_else(|| {
                    DbError::Unsupported(format!(
                        "[LDB-1001] retracting temporal filter projected column \
                         `{name}` not found in source `{}`",
                        self.cfg.source_table
                    ))
                })?);
            }
            idx
        };
        // `restore` may have set the output schemas already.
        if self.row_schema.is_none() {
            let row_fields: Vec<Field> = self
                .proj_indices
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect();
            let mut out_fields = row_fields.clone();
            out_fields.push(Field::new(
                crate::aggregate_state::WEIGHT_COLUMN,
                DataType::Int64,
                false,
            ));
            self.row_schema = Some(Arc::new(Schema::new(row_fields)));
            self.output_schema = Some(Arc::new(Schema::new(out_fields)));
        }
        self.input_schema = Some(schema);
        Ok(())
    }

    fn output_schema(&self) -> Result<&SchemaRef, DbError> {
        self.output_schema.as_ref().ok_or_else(|| {
            DbError::Pipeline(format!(
                "[LDB-1001] temporal filter '{}' schema unresolved",
                self.op_name
            ))
        })
    }

    fn fingerprint(&self) -> Result<u64, DbError> {
        let schema = self.output_schema()?;
        Ok(crate::aggregate_state::query_fingerprint(&self.sql, schema))
    }

    /// Retractions then insertions; consecutive rows sharing a source
    /// `Arc<RecordBatch>` are collapsed into one vectorised `take`.
    fn build_delta(
        &self,
        retracts: &[BufferedRow],
        inserts: &[BufferedRow],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if retracts.is_empty() && inserts.is_empty() {
            return Ok(Vec::new());
        }
        let schema = self.output_schema()?;
        let mut out_batches = Vec::new();
        let mut cur_batch: Option<Arc<RecordBatch>> = None;
        let mut cur_idx: Vec<u32> = Vec::new();
        let mut cur_w: Vec<i64> = Vec::new();
        let iter = retracts
            .iter()
            .map(|r| (r, -1_i64))
            .chain(inserts.iter().map(|r| (r, 1_i64)));
        for (row, weight) in iter {
            if let Some(cb) = &cur_batch {
                if Arc::ptr_eq(cb, &row.batch) {
                    cur_idx.push(row.row_idx);
                    cur_w.push(weight);
                    continue;
                }
                let flushed = take_with_weights(cb, &cur_idx, &cur_w, schema)?;
                out_batches.push(flushed);
                cur_idx.clear();
                cur_w.clear();
            }
            cur_batch = Some(Arc::clone(&row.batch));
            cur_idx.push(row.row_idx);
            cur_w.push(weight);
        }
        if let Some(cb) = cur_batch {
            out_batches.push(take_with_weights(&cb, &cur_idx, &cur_w, schema)?);
        }
        Ok(out_batches)
    }

    /// Already-aged-out rows are dropped (no phantom retract); rows in
    /// `[enter_key, exit_key)` insert now; the rest park in `by_enter`.
    fn ingest(
        &mut self,
        batch: &RecordBatch,
        has_frontier: bool,
        w: i64,
        inserts: &mut Vec<BufferedRow>,
        dropped: &mut u64,
    ) -> Result<(), DbError> {
        self.resolve_schema(batch)?;
        let t_ms = time_col_to_ms(batch.column(self.time_idx))?;
        let projected = if self.proj_indices.len() == batch.num_columns()
            && self.proj_indices.iter().enumerate().all(|(i, &p)| i == p)
        {
            batch.clone()
        } else {
            batch
                .project(&self.proj_indices)
                .map_err(|e| DbError::Pipeline(format!("temporal filter project: {e}")))?
        };
        let shared = Arc::new(projected);
        for (r, t) in t_ms.iter().enumerate() {
            let Some(t) = *t else {
                *dropped += 1; // null time → drop
                continue;
            };
            let (enter_key, exit_key) = keys_for(t, &self.cfg);
            if enter_key >= exit_key {
                *dropped += 1; // empty validity window → never a member
                continue;
            }
            if has_frontier && w >= exit_key {
                *dropped += 1; // arrived already aged out → no emit
                continue;
            }
            if has_frontier
                && self.max_future_ms > 0
                && enter_key != i64::MIN
                && enter_key > w.saturating_add(self.max_future_ms)
            {
                *dropped += 1; // beyond the future horizon → bound the buffer
                continue;
            }
            let row = BufferedRow {
                batch: Arc::clone(&shared),
                row_idx: u32::try_from(r).unwrap_or(u32::MAX),
                enter_key,
                exit_key,
            };
            if has_frontier && w >= enter_key {
                inserts.push(row.clone());
                self.state.push_exit(exit_key, row);
            } else {
                self.state.push_enter(enter_key, row);
            }
        }
        Ok(())
    }

    /// Promote rows whose `enter_key <= w`, retract rows whose `exit_key <= w`.
    fn advance(
        &mut self,
        w: i64,
        inserts: &mut Vec<BufferedRow>,
        retracts: &mut Vec<BufferedRow>,
        dropped: &mut u64,
    ) {
        let entered: Vec<i64> = self.state.by_enter.range(..=w).map(|(k, _)| *k).collect();
        for k in entered {
            // Each row either drops (len--) or moves to `by_exit` (net 0);
            // use `entry().push()` directly to avoid double-counting via push_exit.
            for row in TfState::remove_from(&mut self.state.by_enter, k) {
                if w >= row.exit_key {
                    *dropped += 1;
                    self.state.len -= 1;
                    continue;
                }
                inserts.push(row.clone());
                self.state
                    .by_exit
                    .entry(row.exit_key)
                    .or_default()
                    .push(row);
            }
        }
        let exited: Vec<i64> = self.state.by_exit.range(..=w).map(|(k, _)| *k).collect();
        for k in exited {
            let drained = TfState::remove_from(&mut self.state.by_exit, k);
            self.state.len -= drained.len();
            retracts.extend(drained);
        }
        self.state.last_frontier = w;
    }

    /// `batch` = first `ncols` data columns + `__tf_enter`/`__tf_exit`/
    /// `__tf_inserted`; inserted rows go to `by_exit`, the rest to `by_enter`.
    fn rebuild_state(
        batch: &RecordBatch,
        ncols: usize,
        last_frontier: i64,
    ) -> Result<TfState, DbError> {
        let i64col = |idx: usize, name: &str| -> Result<&Int64Array, DbError> {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("temporal filter restore: {name}")))
        };
        let enter = i64col(ncols, "__tf_enter")?;
        let exit = i64col(ncols + 1, "__tf_exit")?;
        let inserted = batch
            .column(ncols + 2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DbError::Pipeline("temporal filter restore: __tf_inserted".into()))?;

        let row_schema: SchemaRef = Arc::new(Schema::new(
            (0..ncols)
                .map(|c| batch.schema().field(c).clone())
                .collect::<Vec<_>>(),
        ));
        let row_cols: Vec<ArrayRef> = (0..ncols).map(|c| Arc::clone(batch.column(c))).collect();
        let shared = Arc::new(
            RecordBatch::try_new(row_schema, row_cols)
                .map_err(|e| DbError::Pipeline(format!("temporal filter rebuild batch: {e}")))?,
        );

        let mut state = TfState {
            last_frontier,
            ..TfState::default()
        };
        for r in 0..batch.num_rows() {
            let row = BufferedRow {
                batch: Arc::clone(&shared),
                row_idx: u32::try_from(r).unwrap_or(u32::MAX),
                enter_key: enter.value(r),
                exit_key: exit.value(r),
            };
            if inserted.value(r) {
                state.push_exit(row.exit_key, row);
            } else {
                state.push_enter(row.enter_key, row);
            }
        }
        Ok(state)
    }
}

#[async_trait]
impl GraphOperator for TemporalFilterOperator {
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let wm = watermarks.first().copied().unwrap_or(i64::MIN);
        let has_frontier = wm != i64::MIN;
        let w = if has_frontier {
            Self::floor_frontier(wm)
        } else {
            i64::MIN
        };

        let mut inserts: Vec<BufferedRow> = Vec::new();
        let mut retracts: Vec<BufferedRow> = Vec::new();
        let mut dropped = 0u64;

        for batch in inputs.first().map(Vec::as_slice).unwrap_or_default() {
            if batch.num_rows() > 0 {
                self.ingest(batch, has_frontier, w, &mut inserts, &mut dropped)?;
            }
        }
        if has_frontier && w > self.state.last_frontier {
            self.advance(w, &mut inserts, &mut retracts, &mut dropped);
        }

        if let Some(prom) = &self.prom {
            let n = |x: usize| u64::try_from(x).unwrap_or(u64::MAX);
            prom.temporal_filter_inserts.inc_by(n(inserts.len()));
            prom.temporal_filter_retracts.inc_by(n(retracts.len()));
            prom.temporal_filter_dropped.inc_by(dropped);
            prom.temporal_filter_buffered
                .set(i64::try_from(self.state.buffered_rows()).unwrap_or(i64::MAX));
        }

        self.build_delta(&retracts, &inserts)
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        // `by_enter` = inserted=false; `by_exit` = inserted=true.
        let live: Vec<(&BufferedRow, bool)> = self
            .state
            .by_enter
            .values()
            .flat_map(|v| v.iter().map(|r| (r, false)))
            .chain(
                self.state
                    .by_exit
                    .values()
                    .flat_map(|v| v.iter().map(|r| (r, true))),
            )
            .collect();
        if live.is_empty() {
            return Ok(None);
        }
        let Some(row_schema) = self.row_schema.as_ref() else {
            return Err(DbError::Pipeline(
                "temporal filter checkpoint: buffered rows but unresolved schema".into(),
            ));
        };
        // Per-source-batch `take` + `concat` — vectorised.
        let mut data_batches: Vec<RecordBatch> = Vec::new();
        let mut cur_batch: Option<Arc<RecordBatch>> = None;
        let mut cur_idx: Vec<u32> = Vec::new();
        for (row, _) in &live {
            if let Some(cb) = &cur_batch {
                if Arc::ptr_eq(cb, &row.batch) {
                    cur_idx.push(row.row_idx);
                    continue;
                }
                data_batches.push(take_for_schema(cb, &cur_idx, row_schema)?);
                cur_idx.clear();
            }
            cur_batch = Some(Arc::clone(&row.batch));
            cur_idx.push(row.row_idx);
        }
        if let Some(cb) = cur_batch {
            data_batches.push(take_for_schema(&cb, &cur_idx, row_schema)?);
        }
        let data_batch = arrow::compute::concat_batches(row_schema, &data_batches)
            .map_err(|e| DbError::Pipeline(format!("temporal filter ckpt concat: {e}")))?;

        let mut data_arrays: Vec<ArrayRef> = data_batch.columns().to_vec();
        data_arrays.push(Arc::new(Int64Array::from_iter_values(
            live.iter().map(|(r, _)| r.enter_key),
        )));
        data_arrays.push(Arc::new(Int64Array::from_iter_values(
            live.iter().map(|(r, _)| r.exit_key),
        )));
        let inserted: BooleanArray = live.iter().map(|(_, ins)| *ins).collect();
        data_arrays.push(Arc::new(inserted));

        let mut fields: Vec<Field> = row_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new("__tf_enter", DataType::Int64, false));
        fields.push(Field::new("__tf_exit", DataType::Int64, false));
        fields.push(Field::new("__tf_inserted", DataType::Boolean, false));
        let ckpt_schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(ckpt_schema, data_arrays)
            .map_err(|e| DbError::Pipeline(format!("temporal filter ckpt batch: {e}")))?;
        let batch_ipc = laminar_core::serialization::serialize_batch_stream(&batch)
            .map_err(|e| DbError::Pipeline(format!("temporal filter ckpt IPC: {e}")))?;

        let env = TemporalFilterCheckpoint {
            fingerprint: self.fingerprint()?,
            last_frontier: self.state.last_frontier,
            batch_ipc,
        };
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&env)
            .map(|v| v.to_vec())
            .map_err(|e| {
                DbError::Pipeline(format!(
                    "temporal filter checkpoint serialize for '{}': {e}",
                    self.op_name
                ))
            })?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let env: TemporalFilterCheckpoint =
            rkyv::from_bytes::<TemporalFilterCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
                .map_err(|e| {
                    DbError::Pipeline(format!(
                        "temporal filter checkpoint deserialize for '{}': {e}",
                        self.op_name
                    ))
                })?;
        if env.batch_ipc.is_empty() {
            return Ok(());
        }
        let batch = laminar_core::serialization::deserialize_batch_stream(&env.batch_ipc)
            .map_err(|e| DbError::Pipeline(format!("temporal filter restore IPC: {e}")))?;
        let total = batch.num_columns();
        if total < 4 {
            return Err(DbError::Pipeline(
                "temporal filter restore: malformed checkpoint batch".into(),
            ));
        }
        let ncols = total - 3;

        // `time_idx` and `proj_indices` are re-resolved on the first
        // post-restore cycle (the time column may not be projected).
        let row_fields: Vec<Field> = (0..ncols)
            .map(|c| batch.schema().field(c).clone())
            .collect();
        let row_schema: SchemaRef = Arc::new(Schema::new(row_fields.clone()));
        let mut out_fields = row_fields;
        out_fields.push(Field::new(
            crate::aggregate_state::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ));
        let out_schema: SchemaRef = Arc::new(Schema::new(out_fields));

        let current_fp = crate::aggregate_state::query_fingerprint(&self.sql, &out_schema);
        if current_fp != env.fingerprint {
            return Err(DbError::Pipeline(format!(
                "temporal filter checkpoint fingerprint mismatch for '{}': \
                 saved={}, current={}",
                self.op_name, env.fingerprint, current_fp
            )));
        }

        self.state = Self::rebuild_state(&batch, ncols, env.last_frontier)?;
        self.row_schema = Some(row_schema);
        self.output_schema = Some(out_schema);
        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        // Dedup batches by Arc identity so a shared batch's memory is
        // counted once.
        let mut seen: rustc_hash::FxHashSet<*const RecordBatch> = rustc_hash::FxHashSet::default();
        let mut batch_bytes = 0usize;
        let mut row_count = 0usize;
        for row in self
            .state
            .by_enter
            .values()
            .chain(self.state.by_exit.values())
            .flatten()
        {
            row_count += 1;
            let ptr: *const RecordBatch = Arc::as_ptr(&row.batch);
            if seen.insert(ptr) {
                batch_bytes += row.batch.get_array_memory_size();
            }
        }
        batch_bytes + row_count * std::mem::size_of::<BufferedRow>()
    }
}

/// Operator that fails every cycle with a fixed typed error — used to
/// surface planner-level rejections at run time (matches `nonwhere_now`).
pub(crate) struct RejectingOperator {
    message: String,
}

impl RejectingOperator {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
impl GraphOperator for RejectingOperator {
    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Err(DbError::Unsupported(self.message.clone()))
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_analysis::TemporalBound;

    fn lower_strict_ttl(x_ms: i64) -> TemporalFilterConfig {
        // `evt > now() - INTERVAL X` ⇒ lower strict, off = -X.
        TemporalFilterConfig {
            source_table: "events".into(),
            time_col: "evt".into(),
            proj_cols: Vec::new(),
            lower: Some(TemporalBound {
                off_ms: -x_ms,
                strict: true,
            }),
            upper: None,
        }
    }

    fn batch_evt(schema: &SchemaRef, evts: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(evts.to_vec()))],
        )
        .unwrap()
    }

    fn src_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("evt", DataType::Int64, false)]))
    }

    fn weights(batches: &[RecordBatch]) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let w = b
                .column(b.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            out.extend(w.iter().map(Option::unwrap));
        }
        out
    }

    fn evts_out(batches: &[RecordBatch]) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            out.extend(c.iter().map(Option::unwrap));
        }
        out
    }

    #[tokio::test]
    async fn projection_buffers_and_emits_only_selected_columns() {
        // SELECT id FROM events WHERE evt > now() - 10s  (drop `evt`).
        let cfg = TemporalFilterConfig {
            source_table: "events".into(),
            time_col: "evt".into(),
            proj_cols: vec!["id".into()],
            lower: Some(TemporalBound {
                off_ms: -10_000,
                strict: true,
            }),
            upper: None,
        };
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("evt", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![7_i64])),
                Arc::new(Int64Array::from(vec![50_000_i64])),
            ],
        )
        .unwrap();

        let mut op = TemporalFilterOperator::new("tf", "SELECT id FROM events", cfg.clone(), None);
        // Frontier 55_000 < exit 60_000 ⇒ insert.
        let out = op.process(&[vec![batch]], &[55_000]).await.unwrap();
        assert_eq!(out.len(), 1);
        let sch = out[0].schema();
        let names: Vec<&str> = sch.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "__weight"], "`evt` projected away");
        assert_eq!(weights(&out), vec![1]);
        assert_eq!(evts_out(&out), vec![7], "projected `id` value");

        // Checkpoint/restore preserves the projected schema, then the row
        // ages out correctly at the lower bound.
        let ck = op.checkpoint().unwrap().expect("state");
        let mut op2 = TemporalFilterOperator::new("tf", "SELECT id FROM events", cfg, None);
        op2.restore(ck).unwrap();
        let out = op2.process(&[vec![]], &[60_000]).await.unwrap();
        assert_eq!(weights(&out), vec![-1]);
        assert_eq!(evts_out(&out), vec![7]);
        assert_eq!(op2.state.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn no_frontier_buffers_emits_nothing() {
        let cfg = lower_strict_ttl(10_000);
        let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg, None);
        let s = src_schema();
        let out = op
            .process(&[vec![batch_evt(&s, &[50_000])]], &[i64::MIN])
            .await
            .unwrap();
        assert!(out.is_empty(), "no watermark ⇒ no emission");
        assert_eq!(op.state.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn insert_then_retract_as_frontier_advances() {
        // evt > now() - 10_000ms. Row at evt=50_000 ⇒ exit_key=60_000.
        let cfg = lower_strict_ttl(10_000);
        let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg, None);
        let s = src_schema();

        // Frontier at 55_000 (floored 55_000): 55_000 < 60_000 ⇒ member, +1.
        let out = op
            .process(&[vec![batch_evt(&s, &[50_000])]], &[55_000])
            .await
            .unwrap();
        assert_eq!(weights(&out), vec![1]);
        assert_eq!(evts_out(&out), vec![50_000]);

        // Frontier still < 60_000, no new rows ⇒ nothing.
        let out = op.process(&[vec![]], &[59_999]).await.unwrap();
        assert!(out.is_empty());

        // Frontier reaches 60_000 (>= exit_key) ⇒ retract -1.
        let out = op.process(&[vec![]], &[60_000]).await.unwrap();
        assert_eq!(weights(&out), vec![-1]);
        assert_eq!(evts_out(&out), vec![50_000]);

        // Fully GC'd.
        assert_eq!(op.state.buffered_rows(), 0);

        // Further advance ⇒ nothing (no double-retract).
        let out = op.process(&[vec![]], &[120_000]).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn late_row_dropped_no_phantom_retract() {
        let cfg = lower_strict_ttl(10_000);
        let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg, None);
        let s = src_schema();
        // Establish a frontier far ahead first.
        let _ = op.process(&[vec![]], &[500_000]).await.unwrap();
        // Row at evt=50_000 ⇒ exit_key=60_000 < frontier 500_000 ⇒ already
        // expired: dropped, NO insert and NO retract.
        let out = op
            .process(&[vec![batch_evt(&s, &[50_000])]], &[500_000])
            .await
            .unwrap();
        assert!(out.is_empty(), "late row must not emit a phantom retract");
        assert_eq!(op.state.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn upper_bound_future_row_enters_late() {
        // ts < now() + 0  (strict upper, off=0) ⇒ enter_key = t+1, never exits.
        let cfg = TemporalFilterConfig {
            source_table: "s".into(),
            time_col: "evt".into(),
            proj_cols: Vec::new(),
            lower: None,
            upper: Some(TemporalBound {
                off_ms: 0,
                strict: true,
            }),
        };
        let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM s", cfg, None);
        let s = src_schema();
        // evt=100_000, enter_key=100_001. Frontier 100_000 < enter ⇒ buffered, no emit.
        let out = op
            .process(&[vec![batch_evt(&s, &[100_000])]], &[100_000])
            .await
            .unwrap();
        assert!(out.is_empty());
        // Frontier advances past enter_key ⇒ +1, and never retracts.
        let out = op.process(&[vec![]], &[101_000]).await.unwrap();
        assert_eq!(weights(&out), vec![1]);
        let out = op.process(&[vec![]], &[10_000_000]).await.unwrap();
        assert!(out.is_empty(), "upper-only bound never retracts");
    }

    #[tokio::test]
    async fn future_horizon_bounds_the_buffer() {
        // Upper bound only ⇒ far-future rows would otherwise pile up.
        // Default LAMINAR_MAX_FUTURE_SKEW_MS = 5 min (300_000 ms).
        let cfg = TemporalFilterConfig {
            source_table: "s".into(),
            time_col: "evt".into(),
            proj_cols: Vec::new(),
            lower: None,
            upper: Some(TemporalBound {
                off_ms: 0,
                strict: true,
            }),
        };
        let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM s", cfg, None);
        let s = src_schema();
        // Frontier 1_000_000. Row 100s ahead is kept; row 600s ahead is
        // beyond the 300s horizon and dropped un-emitted.
        let out = op
            .process(
                &[vec![batch_evt(&s, &[1_100_000, 1_600_000])]],
                &[1_000_000],
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "neither has entered yet");
        assert_eq!(
            op.state.buffered_rows(),
            1,
            "only the in-horizon row is kept"
        );
    }

    #[tokio::test]
    async fn checkpoint_restore_round_trip() {
        let cfg = lower_strict_ttl(10_000);
        let mut op = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg.clone(), None);
        let s = src_schema();
        // One member (evt=50_000, exit 60_000) + one pending-future via upper? keep simple:
        let out = op
            .process(&[vec![batch_evt(&s, &[50_000, 58_000])]], &[55_000])
            .await
            .unwrap();
        assert_eq!(weights(&out), vec![1, 1]);

        let ck = op.checkpoint().unwrap().expect("state present");
        let mut op2 = TemporalFilterOperator::new("tf", "SELECT * FROM events", cfg, None);
        op2.restore(ck).unwrap();
        assert_eq!(op2.state.buffered_rows(), 2);
        assert_eq!(op2.state.last_frontier, 55_000);

        // After restore, advancing past evt=50_000's exit (60_000) retracts
        // exactly that row; 58_000's row (exit 68_000) still lives.
        let out = op2.process(&[vec![]], &[60_000]).await.unwrap();
        assert_eq!(weights(&out), vec![-1]);
        assert_eq!(evts_out(&out), vec![50_000]);
        assert_eq!(op2.state.buffered_rows(), 1);
    }
}
