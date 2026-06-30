//! Hand-rolled two-sided incremental (IVM) join over two changelogs.
//!
//! For `A ⋈ B` with per-cycle Z-set deltas δA, δB (`__weight` changelogs), the output delta is
//! `δA ⋈ B_new + A_old ⋈ δB` where `B_new = B_old + δB`; matched weights multiply and retractions
//! net naturally. Both side states are indexed Z-sets keyed by the join key. The joined changelog
//! feeds the join MV's `Multiset` store, which accumulates the snapshot.
//!
//! INNER or LEFT equi-join, in-memory state, plain-column projection (no wildcards, no WHERE, no
//! expression projections). A LEFT join NULL-pads unmatched left rows and tracks the pad↔inner
//! transition via per-key right-match presence (and back-fills on first sight of the right schema).
//! Both side Z-sets ARE checkpointed (each serialized as a `__weight` changelog where the weight
//! column carries the stored multiplicity), so a restart restores them consistently with the join
//! MV's `Multiset` snapshot. Single-node, default-OFF.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_common::ScalarValue;
use rustc_hash::FxHashMap;
#[cfg(feature = "state-tier")]
use rustc_hash::FxHashSet;

use laminar_core::changelog::WEIGHT_COLUMN;

use crate::error::DbError;
use crate::mv_store::{batches_to_ipc, ipc_to_batches};
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::{IncrementalJoinConfig, JoinProjItem, JoinSide};

/// Indexed Z-set: `join_key -> { full_row -> multiplicity }`. The in-memory impl validates IVM
/// correctness; a tier-backed impl over the group KV will replace it.
pub(crate) trait JoinStateStore: Send {
    /// Net `weight` into the Z-set for `key`; a row drops at multiplicity ≤ 0.
    fn upsert(&mut self, key: &[ScalarValue], row: &[ScalarValue], weight: i64);
    /// Every `(row, multiplicity)` currently held for `key`.
    fn get(&self, key: &[ScalarValue]) -> Vec<(Vec<ScalarValue>, i64)>;
    /// Whether any row is held for `key` (i.e. a left-join match count > 0).
    fn contains_key(&self, key: &[ScalarValue]) -> bool;
    /// All `(full_row, multiplicity)` entries, for checkpointing and left-join catch-up.
    fn snapshot(&self) -> Vec<(Vec<ScalarValue>, i64)>;
    fn estimated_bytes(&self) -> usize;
}

#[derive(Default)]
pub(crate) struct InMemoryJoinState {
    rows: FxHashMap<Vec<ScalarValue>, FxHashMap<Box<[ScalarValue]>, i64>>,
    bytes: usize,
}

fn scalars_bytes(vals: &[ScalarValue]) -> usize {
    vals.iter().map(ScalarValue::size).sum()
}

impl JoinStateStore for InMemoryJoinState {
    fn upsert(&mut self, key: &[ScalarValue], row: &[ScalarValue], weight: i64) {
        if weight == 0 {
            return;
        }
        let key_existed = self.rows.contains_key(key);
        let entry = self.rows.entry(key.to_vec()).or_default();
        let row_key: Box<[ScalarValue]> = row.into();
        let prev = entry.get(&row_key).copied().unwrap_or(0);
        let next = prev + weight;
        if next <= 0 {
            if prev > 0 {
                entry.remove(&row_key);
            }
        } else {
            entry.insert(row_key, next);
        }
        let now_empty = entry.is_empty();

        if prev <= 0 && next > 0 {
            self.bytes += scalars_bytes(row);
        } else if prev > 0 && next <= 0 {
            self.bytes = self.bytes.saturating_sub(scalars_bytes(row));
        }
        if !key_existed && !now_empty {
            self.bytes += scalars_bytes(key);
        } else if key_existed && now_empty {
            self.bytes = self.bytes.saturating_sub(scalars_bytes(key));
        }
        if now_empty {
            self.rows.remove(key);
        }
    }

    fn get(&self, key: &[ScalarValue]) -> Vec<(Vec<ScalarValue>, i64)> {
        match self.rows.get(key) {
            Some(inner) => inner.iter().map(|(r, &w)| (r.to_vec(), w)).collect(),
            None => Vec::new(),
        }
    }

    fn contains_key(&self, key: &[ScalarValue]) -> bool {
        self.rows.contains_key(key)
    }

    fn snapshot(&self) -> Vec<(Vec<ScalarValue>, i64)> {
        let mut out = Vec::new();
        for inner in self.rows.values() {
            for (row, &w) in inner {
                out.push((row.to_vec(), w));
            }
        }
        out
    }

    fn estimated_bytes(&self) -> usize {
        self.bytes
    }
}

/// Per-side schema resolution: where the join keys, plain (non-weight) columns, and weight live in
/// an input changelog batch. Resolved lazily on first sight of each side's schema.
struct SideInfo {
    weight_idx: usize,
    plain_cols: Vec<usize>,
    key_idx: Vec<usize>,
    // Position of each key column within `plain_cols` — lets the left-join catch-up re-derive a
    // key from a stored (plain) row.
    key_plain_pos: Vec<usize>,
    name_to_plain_pos: FxHashMap<String, usize>,
    schema: SchemaRef,
}

impl SideInfo {
    fn resolve(schema: &SchemaRef, key_names: &[String]) -> Result<Self, DbError> {
        let weight_idx = schema.index_of(WEIGHT_COLUMN).map_err(|e| {
            DbError::Pipeline(format!("incremental join: changelog missing weight: {e}"))
        })?;
        let plain_cols: Vec<usize> = (0..schema.fields().len())
            .filter(|&i| i != weight_idx)
            .collect();
        let mut name_to_plain_pos = FxHashMap::default();
        for (pos, &col) in plain_cols.iter().enumerate() {
            name_to_plain_pos.insert(schema.field(col).name().clone(), pos);
        }
        let key_idx = key_names
            .iter()
            .map(|n| {
                schema.index_of(n).map_err(|e| {
                    DbError::Pipeline(format!("incremental join: key column '{n}': {e}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key_plain_pos = key_idx
            .iter()
            .map(|k| {
                plain_cols
                    .iter()
                    .position(|c| c == k)
                    .expect("key column is a plain column")
            })
            .collect();
        Ok(Self {
            weight_idx,
            plain_cols,
            key_idx,
            key_plain_pos,
            name_to_plain_pos,
            schema: schema.clone(),
        })
    }
}

/// One parsed changelog row: its join key, its plain (weightless) values, and signed multiplicity.
struct DeltaRow {
    key: Vec<ScalarValue>,
    row: Vec<ScalarValue>,
    weight: i64,
}

/// INNER/LEFT IVM join operator (two-input: port 0 = left changelog, port 1 = right).
pub(crate) struct IncrementalJoinOperator {
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    projection: Vec<JoinProjItem>,
    left_outer: bool,
    left_state: Box<dyn JoinStateStore>,
    right_state: Box<dyn JoinStateStore>,
    left_info: Option<SideInfo>,
    right_info: Option<SideInfo>,
    // Flattened output columns `(side, plain_position)` + cached output schema; resolved once both
    // side schemas are known (from a live batch per side, or from restore).
    out_cols: Option<Vec<(JoinSide, usize)>>,
    out_schema: Option<SchemaRef>,
    #[cfg(feature = "state-tier")]
    tier: JoinTierState,
}

impl IncrementalJoinOperator {
    pub(crate) fn new(config: IncrementalJoinConfig) -> Self {
        Self {
            left_keys: config.left_keys,
            right_keys: config.right_keys,
            projection: config.projection,
            left_outer: config.left_outer,
            left_state: Box::new(InMemoryJoinState::default()),
            right_state: Box::new(InMemoryJoinState::default()),
            left_info: None,
            right_info: None,
            out_cols: None,
            out_schema: None,
            #[cfg(feature = "state-tier")]
            tier: JoinTierState::new(),
        }
    }

    /// Parse a changelog batch into `(key, row, weight)` deltas. `skip_null_keys` drops NULL-key
    /// rows — correct for the right side always, and for the left side of an INNER join (such rows
    /// never match). A LEFT join keeps NULL-key left rows so they are still NULL-padded.
    fn parse_side(
        info: &SideInfo,
        batches: &[RecordBatch],
        skip_null_keys: bool,
    ) -> Result<Vec<DeltaRow>, DbError> {
        let mut out = Vec::new();
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let weights = batch
                .column(info.weight_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline("incremental join: weight not Int64".into()))?;
            for r in 0..batch.num_rows() {
                let w = weights.value(r);
                if w == 0 {
                    continue;
                }
                let key = scalars_at(batch, &info.key_idx, r)?;
                if skip_null_keys && key.iter().any(ScalarValue::is_null) {
                    continue;
                }
                let row = scalars_at(batch, &info.plain_cols, r)?;
                out.push(DeltaRow {
                    key,
                    row,
                    weight: w,
                });
            }
        }
        Ok(out)
    }

    /// Resolve the output column map + schema once both side schemas are present. Idempotent.
    fn resolve_output(&mut self) -> Result<(), DbError> {
        if self.out_cols.is_some() {
            return Ok(());
        }
        let (Some(l), Some(r)) = (&self.left_info, &self.right_info) else {
            return Ok(());
        };
        // Equi-join key columns must share a type across sides — ScalarValue equality is
        // type-strict (Int64(1) != Int32(1)), so a mismatch would silently never match.
        for (lk, rk) in l.key_idx.iter().zip(&r.key_idx) {
            if l.schema.field(*lk).data_type() != r.schema.field(*rk).data_type() {
                return Err(DbError::Pipeline(
                    "incremental join: join key column types differ between sides".into(),
                ));
            }
        }
        let mut cols: Vec<(JoinSide, usize)> = Vec::new();
        for item in &self.projection {
            match item {
                JoinProjItem::Qualified { side, column, .. } => {
                    let info = match side {
                        JoinSide::Left => l,
                        JoinSide::Right => r,
                    };
                    let pos = *info.name_to_plain_pos.get(column).ok_or_else(|| {
                        DbError::Pipeline(format!("incremental join: column '{column}' not found"))
                    })?;
                    cols.push((*side, pos));
                }
                JoinProjItem::Unqualified { column, .. } => match (
                    l.name_to_plain_pos.get(column),
                    r.name_to_plain_pos.get(column),
                ) {
                    (Some(&p), None) => cols.push((JoinSide::Left, p)),
                    (None, Some(&p)) => cols.push((JoinSide::Right, p)),
                    (Some(_), Some(_)) => {
                        return Err(DbError::Pipeline(format!(
                            "incremental join: column '{column}' is ambiguous"
                        )))
                    }
                    (None, None) => {
                        return Err(DbError::Pipeline(format!(
                            "incremental join: column '{column}' not found"
                        )))
                    }
                },
            }
        }
        let left_outer = self.left_outer;
        // `cols` is built 1:1 in projection order, so zip recovers each item's optional alias.
        let mut fields: Vec<Field> = cols
            .iter()
            .zip(&self.projection)
            .map(|(&(side, pos), item)| {
                let info = match side {
                    JoinSide::Left => l,
                    JoinSide::Right => r,
                };
                let f = info.schema.field(info.plain_cols[pos]);
                // Right columns become NULL-able under a LEFT join (NULL-padded unmatched rows).
                let nullable = f.is_nullable() || (left_outer && side == JoinSide::Right);
                // An explicit projection alias renames the output column; else keep the source name.
                let alias = match item {
                    JoinProjItem::Qualified { alias, .. }
                    | JoinProjItem::Unqualified { alias, .. } => alias.as_deref(),
                };
                let name = alias.unwrap_or(f.name());
                Field::new(name, f.data_type().clone(), nullable)
            })
            .collect();
        fields.push(Field::new(
            WEIGHT_COLUMN,
            arrow::datatypes::DataType::Int64,
            false,
        ));
        self.out_cols = Some(cols);
        self.out_schema = Some(Arc::new(Schema::new(fields)));
        Ok(())
    }

    /// `deltas ⋈ build-side state` for one IVM term, appending projected output rows. The output
    /// column order is always left-then-right regardless of which side carries the delta.
    fn join_term(
        &self,
        deltas: &[DeltaRow],
        delta_side: JoinSide,
        out: &mut Vec<(Vec<ScalarValue>, i64)>,
    ) {
        let Some(resolved) = self.out_cols.as_ref() else {
            return;
        };
        let build = match delta_side {
            JoinSide::Left => &self.right_state,
            JoinSide::Right => &self.left_state,
        };
        for d in deltas {
            for (other_row, other_w) in build.get(&d.key) {
                let w = d.weight * other_w;
                if w == 0 {
                    continue;
                }
                let proj: Vec<ScalarValue> = resolved
                    .iter()
                    .map(|&(side, pos)| {
                        let row = if side == delta_side {
                            &d.row
                        } else {
                            &other_row
                        };
                        row[pos].clone()
                    })
                    .collect();
                out.push((proj, w));
            }
        }
    }

    fn build_output(&self, rows: &[(Vec<ScalarValue>, i64)]) -> Result<Vec<RecordBatch>, DbError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let schema = self.out_schema.clone().ok_or_else(|| {
            DbError::Pipeline("incremental join: output schema unresolved".into())
        })?;
        let ncols = schema.fields().len() - 1; // minus __weight
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols + 1);
        for c in 0..ncols {
            let arr = ScalarValue::iter_to_array(rows.iter().map(|(r, _)| r[c].clone()))
                .map_err(|e| DbError::Pipeline(format!("incremental join: build column: {e}")))?;
            arrays.push(arr);
        }
        let weights = Int64Array::from(rows.iter().map(|(_, w)| *w).collect::<Vec<i64>>());
        arrays.push(Arc::new(weights));
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| DbError::Pipeline(format!("incremental join: build batch: {e}")))?;
        Ok(vec![batch])
    }

    /// Project a left row with every right output column NULL-padded (LEFT join, unmatched row).
    fn nullpad_row(&self, left_row: &[ScalarValue]) -> Result<Vec<ScalarValue>, DbError> {
        let resolved = self
            .out_cols
            .as_ref()
            .ok_or_else(|| DbError::Pipeline("incremental join: null-pad unresolved".into()))?;
        let r = self.right_info.as_ref().ok_or_else(|| {
            DbError::Pipeline("incremental join: null-pad needs right schema".into())
        })?;
        resolved
            .iter()
            .map(|&(side, pos)| match side {
                JoinSide::Left => Ok(left_row[pos].clone()),
                JoinSide::Right => {
                    let dt = r.schema.field(r.plain_cols[pos]).data_type();
                    ScalarValue::try_from(dt)
                        .map_err(|e| DbError::Pipeline(format!("incremental join: null-pad: {e}")))
                }
            })
            .collect()
    }

    /// LEFT join catch-up: the first time the right side is observed, emit a NULL-padded row for
    /// every resident left row whose key has no right match (the join MV had emitted nothing while
    /// the right schema was unknown).
    fn emit_left_catchup(&self, out: &mut Vec<(Vec<ScalarValue>, i64)>) -> Result<(), DbError> {
        let Some(l) = self.left_info.as_ref() else {
            return Ok(());
        };
        for (row, mult) in self.left_state.snapshot() {
            let key: Vec<ScalarValue> = l.key_plain_pos.iter().map(|&p| row[p].clone()).collect();
            if !self.right_state.contains_key(&key) {
                out.push((self.nullpad_row(&row)?, mult));
            }
        }
        Ok(())
    }

    /// Serialize one side's Z-set as a `__weight` changelog batch (weight column carries the stored
    /// multiplicity), keyed by the side's live input schema. `None` only if the side was never
    /// observed; a seen-but-empty side still emits a 0-row, schema-carrying blob so restore
    /// re-establishes the side schema and "seen" status (else a LEFT join's first-right catch-up
    /// would fire again on recovery and double-emit NULL-pads).
    fn side_checkpoint_bytes(&self, side: JoinSide) -> Result<Option<Vec<u8>>, DbError> {
        let (info, store) = match side {
            JoinSide::Left => (&self.left_info, &self.left_state),
            JoinSide::Right => (&self.right_info, &self.right_state),
        };
        let Some(info) = info else {
            return Ok(None);
        };
        Ok(Some(side_rows_to_ipc(info, &store.snapshot())?))
    }

    /// Rebuild one side's Z-set from its checkpoint changelog (the inverse of
    /// [`side_checkpoint_bytes`](Self::side_checkpoint_bytes)); also resolves the side schema so a
    /// post-restart cycle with input on only the other port still finds matches.
    fn restore_side(&mut self, side: JoinSide, bytes: &[u8]) -> Result<(), DbError> {
        let batches = ipc_to_batches(bytes)
            .map_err(|e| DbError::Pipeline(format!("incremental join: restore ipc: {e}")))?;
        let Some(first) = batches.first() else {
            return Ok(());
        };
        let keys = match side {
            JoinSide::Left => &self.left_keys,
            JoinSide::Right => &self.right_keys,
        };
        let info = SideInfo::resolve(&first.schema(), keys)?;
        let skip_null = side == JoinSide::Right || !self.left_outer;
        let rows = Self::parse_side(&info, &batches, skip_null)?;
        let store = match side {
            JoinSide::Left => &mut self.left_state,
            JoinSide::Right => &mut self.right_state,
        };
        for d in &rows {
            store.upsert(&d.key, &d.row, d.weight);
        }
        match side {
            JoinSide::Left => self.left_info = Some(info),
            JoinSide::Right => self.right_info = Some(info),
        }
        Ok(())
    }
}

/// Frame two optional side blobs as `[left_len:u32 LE][left][right_len:u32 LE][right]`.
fn encode_frame(left: Option<&[u8]>, right: Option<&[u8]>) -> Vec<u8> {
    let l = left.unwrap_or(&[]);
    let r = right.unwrap_or(&[]);
    let mut out = Vec::with_capacity(8 + l.len() + r.len());
    out.extend_from_slice(&u32::try_from(l.len()).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(l);
    out.extend_from_slice(&u32::try_from(r.len()).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(r);
    out
}

/// The two optional side blobs decoded from a checkpoint frame.
type FramedSides<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

fn decode_frame(data: &[u8]) -> Result<FramedSides<'_>, DbError> {
    let bad = || DbError::Pipeline("incremental join: truncated checkpoint frame".into());
    let ll = u32::from_le_bytes(data.get(0..4).ok_or_else(bad)?.try_into().unwrap()) as usize;
    let lend = 4 + ll;
    let left = data.get(4..lend).ok_or_else(bad)?;
    let rl = u32::from_le_bytes(
        data.get(lend..lend + 4)
            .ok_or_else(bad)?
            .try_into()
            .unwrap(),
    ) as usize;
    let right = data.get(lend + 4..lend + 4 + rl).ok_or_else(bad)?;
    Ok((
        (!left.is_empty()).then_some(left),
        (!right.is_empty()).then_some(right),
    ))
}

fn scalars_at(
    batch: &RecordBatch,
    cols: &[usize],
    row: usize,
) -> Result<Vec<ScalarValue>, DbError> {
    cols.iter()
        .map(|&c| {
            ScalarValue::try_from_array(batch.column(c), row)
                .map_err(|e| DbError::Pipeline(format!("incremental join: scalar: {e}")))
        })
        .collect()
}

/// Serialize a row list (with multiplicities) as one `__weight` changelog IPC blob keyed by `info`'s
/// schema — the weight column carries each row's stored multiplicity. An empty list yields a 0-row,
/// schema-carrying batch (so a seen-but-empty side still round-trips its schema on restore).
fn side_rows_to_ipc(info: &SideInfo, rows: &[(Vec<ScalarValue>, i64)]) -> Result<Vec<u8>, DbError> {
    let batch = if rows.is_empty() {
        RecordBatch::new_empty(info.schema.clone())
    } else {
        let nfields = info.schema.fields().len();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(nfields);
        for si in 0..nfields {
            if si == info.weight_idx {
                let mults = Int64Array::from(rows.iter().map(|(_, m)| *m).collect::<Vec<i64>>());
                cols.push(Arc::new(mults));
            } else {
                let pos = info
                    .plain_cols
                    .iter()
                    .position(|&c| c == si)
                    .expect("non-weight column is a plain column");
                let arr = ScalarValue::iter_to_array(rows.iter().map(|(r, _)| r[pos].clone()))
                    .map_err(|e| {
                        DbError::Pipeline(format!("incremental join: side column: {e}"))
                    })?;
                cols.push(arr);
            }
        }
        RecordBatch::try_new(info.schema.clone(), cols)
            .map_err(|e| DbError::Pipeline(format!("incremental join: side batch: {e}")))?
    };
    batches_to_ipc(&info.schema, std::iter::once(&batch))
}

// ─── Tier-backed join state (A1-emit Stage 3b — Slice 4) ────────────────────────────────────────
// S4.1 (this slice): the join-key codec, the two-sided per-key cold blob, and cold/dirty tracking,
// all unit-tested. The demotion driver (S4.5), fetch-on-access promotion (S4.3), and cold-only-partial
// recovery (S4.4) wire these into the `GraphOperator` tier hooks; until they do, the items below have
// only test callers — hence the `allow(dead_code)` until the wiring slices land. Demotion unit = the
// join key (sides never co-partition onto a shared vnode).

/// Reversible, NULL-safe, multi-column join-key encoding for the tier group key + a restart-stable
/// vnode bucket. Reuses the engine's arrow-row encoding (matching the aggregate tier) and the
/// engine-wide xxh3 `key_hash`, so a key buckets to the same vnode before and after a restart.
#[cfg(feature = "state-tier")]
#[allow(dead_code)]
struct JoinKeyCodec {
    converter: arrow::row::RowConverter,
    key_types: Vec<arrow::datatypes::DataType>,
}

#[cfg(feature = "state-tier")]
#[allow(dead_code)]
impl JoinKeyCodec {
    fn new(key_types: Vec<arrow::datatypes::DataType>) -> Result<Self, DbError> {
        let fields: Vec<arrow::row::SortField> = key_types
            .iter()
            .map(|t| arrow::row::SortField::new(t.clone()))
            .collect();
        let converter = arrow::row::RowConverter::new(fields)
            .map_err(|e| DbError::Pipeline(format!("incremental join: key converter: {e}")))?;
        Ok(Self {
            converter,
            key_types,
        })
    }

    /// Encode a join key to its stable tier group-key bytes. The key columns also ride inside the
    /// value blob, so these bytes are only ever an opaque identifier + hash input (never decoded).
    fn encode(&self, key: &[ScalarValue]) -> Result<Vec<u8>, DbError> {
        let row =
            crate::aggregate_state::scalar_key_to_owned_row(&self.converter, key, &self.key_types)?;
        Ok(row.as_ref().to_vec())
    }

    fn vnode(&self, key: &[ScalarValue], vnode_count: u32) -> Result<u32, DbError> {
        let bytes = self.encode(key)?;
        let count = u64::from(vnode_count.max(1));
        // `% count` (count ≤ u32::MAX) keeps the result in u32 range.
        #[allow(clippy::cast_possible_truncation)]
        let vnode = (laminar_core::state::key_hash(&bytes) % count) as u32;
        Ok(vnode)
    }
}

/// Cold/dirty bookkeeping for key-granular demotion. The hot Z-sets stay in `left_state`/`right_state`;
/// this tracks which join keys are clean (demotable) and which are currently cold (their two-sided
/// Z-set lives in the tier, not in the hot stores).
#[cfg(feature = "state-tier")]
#[allow(dead_code)]
struct JoinTierState {
    codec: Option<JoinKeyCodec>,
    vnode_count: u32,
    delta_tracking: bool,
    touch_counter: u64,
    // Keys mutated since the last capture — not demotable (their tier blob would diverge from a
    // checkpoint taken at that capture).
    dirty: FxHashSet<Vec<ScalarValue>>,
    // Last touch order per resident key, for idle-first demotion.
    touch_seq: FxHashMap<Vec<ScalarValue>, u64>,
    cold_keys: FxHashSet<Vec<ScalarValue>>,
}

#[cfg(feature = "state-tier")]
#[allow(dead_code)]
impl JoinTierState {
    fn new() -> Self {
        Self {
            codec: None,
            vnode_count: 1,
            delta_tracking: false,
            touch_counter: 0,
            dirty: FxHashSet::default(),
            touch_seq: FxHashMap::default(),
            cold_keys: FxHashSet::default(),
        }
    }

    fn after_upsert(&mut self, key: &[ScalarValue], resident: bool) {
        if !self.delta_tracking {
            return;
        }
        if resident {
            self.touch_counter += 1;
            self.dirty.insert(key.to_vec());
            self.touch_seq.insert(key.to_vec(), self.touch_counter);
        } else {
            self.dirty.remove(key);
            self.touch_seq.remove(key);
        }
    }

    /// Mark every resident key clean — called when a checkpoint captures the current state.
    fn clear_dirty(&mut self) {
        self.dirty.clear();
    }

    fn mark_demoted(&mut self, key: &[ScalarValue]) {
        self.dirty.remove(key);
        self.touch_seq.remove(key);
        self.cold_keys.insert(key.to_vec());
    }

    fn mark_promoted(&mut self, key: &[ScalarValue]) {
        self.cold_keys.remove(key);
        self.touch_counter += 1;
        self.dirty.insert(key.to_vec());
        self.touch_seq.insert(key.to_vec(), self.touch_counter);
    }

    fn is_cold(&self, key: &[ScalarValue]) -> bool {
        self.cold_keys.contains(key)
    }
}

#[cfg(feature = "state-tier")]
#[allow(dead_code)]
impl IncrementalJoinOperator {
    /// Build the join-key codec once both side schemas are resolved. Key column types match across
    /// sides (`resolve_output` enforces this), so the left side's types suffice.
    fn ensure_codec(&mut self) -> Result<(), DbError> {
        if self.tier.codec.is_some() {
            return Ok(());
        }
        let (Some(l), Some(_)) = (&self.left_info, &self.right_info) else {
            return Ok(());
        };
        let key_types: Vec<arrow::datatypes::DataType> = l
            .key_idx
            .iter()
            .map(|&i| l.schema.field(i).data_type().clone())
            .collect();
        self.tier.codec = Some(JoinKeyCodec::new(key_types)?);
        Ok(())
    }

    /// Pack both sides' Z-set for one join key into a single cold blob (`encode_frame` of each side's
    /// `__weight` IPC). An empty side encodes as absent, so a LEFT-join key with left rows but no
    /// right match decodes to a right-absent state — a promote never fabricates a match.
    fn encode_key_blob(&self, key: &[ScalarValue]) -> Result<Vec<u8>, DbError> {
        let l = self.left_info.as_ref().ok_or_else(|| {
            DbError::Pipeline("incremental join: demote needs left schema".into())
        })?;
        let r = self.right_info.as_ref().ok_or_else(|| {
            DbError::Pipeline("incremental join: demote needs right schema".into())
        })?;
        let left_rows = self.left_state.get(key);
        let right_rows = self.right_state.get(key);
        let lb = if left_rows.is_empty() {
            None
        } else {
            Some(side_rows_to_ipc(l, &left_rows)?)
        };
        let rb = if right_rows.is_empty() {
            None
        } else {
            Some(side_rows_to_ipc(r, &right_rows)?)
        };
        Ok(encode_frame(lb.as_deref(), rb.as_deref()))
    }

    /// Inverse of [`encode_key_blob`](Self::encode_key_blob): parse each side's rows back out of a
    /// cold blob. An absent side yields no rows.
    fn decode_key_blob(&self, blob: &[u8]) -> Result<(Vec<DeltaRow>, Vec<DeltaRow>), DbError> {
        let (lb, rb) = decode_frame(blob)?;
        let left = match lb {
            Some(bytes) => {
                let info = self.left_info.as_ref().ok_or_else(|| {
                    DbError::Pipeline("incremental join: promote needs left schema".into())
                })?;
                let batches = ipc_to_batches(bytes).map_err(|e| {
                    DbError::Pipeline(format!("incremental join: promote left ipc: {e}"))
                })?;
                Self::parse_side(info, &batches, false)?
            }
            None => Vec::new(),
        };
        let right = match rb {
            Some(bytes) => {
                let info = self.right_info.as_ref().ok_or_else(|| {
                    DbError::Pipeline("incremental join: promote needs right schema".into())
                })?;
                let batches = ipc_to_batches(bytes).map_err(|e| {
                    DbError::Pipeline(format!("incremental join: promote right ipc: {e}"))
                })?;
                Self::parse_side(info, &batches, false)?
            }
            None => Vec::new(),
        };
        Ok((left, right))
    }

    /// Demote one join key: capture its two-sided blob, then drop its rows from both hot stores via
    /// negative upserts so `estimated_state_bytes` sheds the freed bytes. The caller persists the
    /// blob to the tier BEFORE this drops the rows (write-before-drop — wired in S4.5).
    fn demote_key(&mut self, key: &[ScalarValue]) -> Result<Vec<u8>, DbError> {
        let blob = self.encode_key_blob(key)?;
        for (row, w) in self.left_state.get(key) {
            self.left_state.upsert(key, &row, -w);
        }
        for (row, w) in self.right_state.get(key) {
            self.right_state.upsert(key, &row, -w);
        }
        self.tier.mark_demoted(key);
        Ok(blob)
    }

    /// Promote one cold join key back into the hot stores from its blob, re-incrementing
    /// `estimated_state_bytes` and re-marking the key dirty so the next capture carries it.
    fn promote_key(&mut self, key: &[ScalarValue], blob: &[u8]) -> Result<(), DbError> {
        let (left, right) = self.decode_key_blob(blob)?;
        for d in &left {
            self.left_state.upsert(&d.key, &d.row, d.weight);
        }
        for d in &right {
            self.right_state.upsert(&d.key, &d.row, d.weight);
        }
        self.tier.mark_promoted(key);
        Ok(())
    }

    /// Clean, idle-first, non-NULL join keys eligible for demotion, capped at `max`. A NULL join key
    /// only ever NULL-pads on the LEFT side (it can never match a right row), so it is never demoted.
    fn demotable_keys(&self, max: usize) -> Vec<Vec<ScalarValue>> {
        if !self.tier.delta_tracking || max == 0 {
            return Vec::new();
        }
        let mut candidates: FxHashMap<Vec<ScalarValue>, u64> = FxHashMap::default();
        for (store, info) in [
            (&self.left_state, &self.left_info),
            (&self.right_state, &self.right_info),
        ] {
            let Some(info) = info else {
                continue;
            };
            for (row, _) in store.snapshot() {
                let key: Vec<ScalarValue> =
                    info.key_plain_pos.iter().map(|&p| row[p].clone()).collect();
                if key.iter().any(ScalarValue::is_null) || self.tier.dirty.contains(&key) {
                    continue;
                }
                let seq = self.tier.touch_seq.get(&key).copied().unwrap_or(0);
                candidates.entry(key).or_insert(seq);
            }
        }
        let mut ranked: Vec<(Vec<ScalarValue>, u64)> = candidates.into_iter().collect();
        ranked.sort_by_key(|(_, seq)| *seq);
        ranked.into_iter().take(max).map(|(k, _)| k).collect()
    }

    /// The cold join keys referenced by `batches` on `side`, projected through THAT side's key
    /// columns (not a leading-column slice — the join's keys are not leading and differ per side).
    /// The fetch-on-access path (S4.3) defers a cycle whose batches touch any cold key.
    fn cold_keys_touched(
        &self,
        side: JoinSide,
        batches: &[RecordBatch],
    ) -> Result<Vec<Vec<ScalarValue>>, DbError> {
        if self.tier.cold_keys.is_empty() {
            return Ok(Vec::new());
        }
        let info = match side {
            JoinSide::Left => self.left_info.as_ref(),
            JoinSide::Right => self.right_info.as_ref(),
        };
        let Some(info) = info else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut seen: FxHashSet<Vec<ScalarValue>> = FxHashSet::default();
        for batch in batches {
            for r in 0..batch.num_rows() {
                let key = scalars_at(batch, &info.key_idx, r)?;
                if self.tier.is_cold(&key) && seen.insert(key.clone()) {
                    out.push(key);
                }
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl GraphOperator for IncrementalJoinOperator {
    #[allow(clippy::too_many_lines)]
    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let right_batches = inputs.get(1).map_or(&[][..], Vec::as_slice);

        let right_was_seen = self.right_info.is_some();
        if self.left_info.is_none() {
            if let Some(b) = left_batches.first() {
                self.left_info = Some(SideInfo::resolve(&b.schema(), &self.left_keys)?);
            }
        }
        if self.right_info.is_none() {
            if let Some(b) = right_batches.first() {
                self.right_info = Some(SideInfo::resolve(&b.schema(), &self.right_keys)?);
            }
        }
        self.resolve_output()?;
        #[cfg(feature = "state-tier")]
        self.ensure_codec()?;
        // First time the right schema is known: a LEFT join must back-fill NULL-padded rows for the
        // left state accumulated while it was unknown (the join MV emitted nothing until now).
        let first_right = self.left_outer && !right_was_seen && self.right_info.is_some();

        // A LEFT join keeps NULL-key left rows (they NULL-pad); the right side always drops them.
        let delta_a = match &self.left_info {
            Some(info) => Self::parse_side(info, left_batches, !self.left_outer)?,
            None => Vec::new(),
        };
        let delta_b = match &self.right_info {
            Some(info) => Self::parse_side(info, right_batches, true)?,
            None => Vec::new(),
        };
        if delta_a.is_empty() && delta_b.is_empty() && !first_right {
            return Ok(Vec::new());
        }

        // Right match presence per δB key BEFORE applying δB (for LEFT-join NULL-pad transitions).
        let presence_old: FxHashMap<Vec<ScalarValue>, bool> = if self.left_outer && !first_right {
            let mut m = FxHashMap::default();
            for d in &delta_b {
                m.entry(d.key.clone())
                    .or_insert_with(|| self.right_state.contains_key(&d.key));
            }
            m
        } else {
            FxHashMap::default()
        };

        let mut out: Vec<(Vec<ScalarValue>, i64)> = Vec::new();
        // term2 = A_old ⋈ δB (before any state mutation), then advance B, then term1 = δA ⋈ B_new.
        self.join_term(&delta_b, JoinSide::Right, &mut out);
        for d in &delta_b {
            self.right_state.upsert(&d.key, &d.row, d.weight);
            #[cfg(feature = "state-tier")]
            {
                let resident =
                    self.right_state.contains_key(&d.key) || self.left_state.contains_key(&d.key);
                self.tier.after_upsert(&d.key, resident);
            }
        }

        if self.left_outer {
            if first_right {
                self.emit_left_catchup(&mut out)?;
            } else {
                // A right key flipping empty↔non-empty retracts/re-emits the NULL-pad of every
                // resident left row at that key (A_old — δA not yet applied).
                for (key, &was_present) in &presence_old {
                    let now_present = self.right_state.contains_key(key);
                    if was_present != now_present {
                        let sign = if now_present { -1 } else { 1 };
                        for (a_row, wa) in self.left_state.get(key) {
                            out.push((self.nullpad_row(&a_row)?, sign * wa));
                        }
                    }
                }
            }
        }

        self.join_term(&delta_a, JoinSide::Left, &mut out);
        if self.left_outer && self.right_info.is_some() {
            for d in &delta_a {
                if !self.right_state.contains_key(&d.key) {
                    out.push((self.nullpad_row(&d.row)?, d.weight));
                }
            }
        }
        for d in &delta_a {
            self.left_state.upsert(&d.key, &d.row, d.weight);
            #[cfg(feature = "state-tier")]
            {
                let resident =
                    self.left_state.contains_key(&d.key) || self.right_state.contains_key(&d.key);
                self.tier.after_upsert(&d.key, resident);
            }
        }

        self.build_output(&out)
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let left = self.side_checkpoint_bytes(JoinSide::Left)?;
        let right = self.side_checkpoint_bytes(JoinSide::Right)?;
        if left.is_none() && right.is_none() {
            return Ok(None);
        }
        Ok(Some(OperatorCheckpoint {
            data: encode_frame(left.as_deref(), right.as_deref()),
        }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let (left, right) = decode_frame(&checkpoint.data)?;
        if let Some(bytes) = left {
            self.restore_side(JoinSide::Left, bytes)?;
        }
        if let Some(bytes) = right {
            self.restore_side(JoinSide::Right, bytes)?;
        }
        Ok(())
    }

    fn estimated_state_bytes(&self) -> usize {
        self.left_state.estimated_bytes() + self.right_state.estimated_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::DataType;
    use std::collections::BTreeMap;

    fn i64_scalar(v: i64) -> ScalarValue {
        ScalarValue::Int64(Some(v))
    }

    #[test]
    fn in_memory_state_nets_and_drops() {
        let mut s = InMemoryJoinState::default();
        let k = [i64_scalar(1)];
        s.upsert(&k, &[i64_scalar(1), i64_scalar(10)], 1);
        s.upsert(&k, &[i64_scalar(1), i64_scalar(10)], 1); // multiplicity 2
        assert_eq!(s.get(&k), vec![(vec![i64_scalar(1), i64_scalar(10)], 2)]);
        s.upsert(&k, &[i64_scalar(1), i64_scalar(10)], -2); // drop
        assert!(s.get(&k).is_empty());
        assert_eq!(s.estimated_bytes(), 0);
    }

    fn left_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("va", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]))
    }
    fn right_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("vb", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]))
    }

    fn left_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            left_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }
    fn right_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            right_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn config() -> IncrementalJoinConfig {
        IncrementalJoinConfig {
            left_table: "a".into(),
            right_table: "b".into(),
            left_keys: vec!["k".into()],
            right_keys: vec!["k".into()],
            projection: vec![
                JoinProjItem::Qualified {
                    side: JoinSide::Left,
                    column: "k".into(),
                    alias: None,
                },
                JoinProjItem::Qualified {
                    side: JoinSide::Left,
                    column: "va".into(),
                    alias: None,
                },
                JoinProjItem::Qualified {
                    side: JoinSide::Right,
                    column: "vb".into(),
                    alias: None,
                },
            ],
            left_outer: false,
        }
    }

    fn left_config() -> IncrementalJoinConfig {
        IncrementalJoinConfig {
            left_outer: true,
            ..config()
        }
    }

    // Net the emitted (k, va, vb, weight) deltas into a snapshot multiset, treating a NULL right
    // value (LEFT-join pad) as the sentinel i64::MIN.
    fn net_into_nullable(snapshot: &mut BTreeMap<(i64, i64, i64), i64>, batches: &[RecordBatch]) {
        for b in batches {
            let c = |i: usize| {
                b.column(i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone()
            };
            let (k, va, vb, w) = (c(0), c(1), c(2), c(3));
            for r in 0..b.num_rows() {
                let vb_val = if vb.is_null(r) { i64::MIN } else { vb.value(r) };
                let key = (k.value(r), va.value(r), vb_val);
                let e = snapshot.entry(key).or_insert(0);
                *e += w.value(r);
                if *e == 0 {
                    snapshot.remove(&key);
                }
            }
        }
    }

    // Net the emitted (k, va, vb, weight) deltas into a snapshot multiset.
    fn net_into(snapshot: &mut BTreeMap<(i64, i64, i64), i64>, batches: &[RecordBatch]) {
        for b in batches {
            let c = |i: usize| {
                b.column(i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone()
            };
            let (k, va, vb, w) = (c(0), c(1), c(2), c(3));
            for r in 0..b.num_rows() {
                let key = (k.value(r), va.value(r), vb.value(r));
                let e = snapshot.entry(key).or_insert(0);
                *e += w.value(r);
                if *e == 0 {
                    snapshot.remove(&key);
                }
            }
        }
    }

    #[tokio::test]
    async fn inner_ivm_join_nets_updates_on_both_sides() {
        let mut op = IncrementalJoinOperator::new(config());
        let mut snap: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        // Cycle 1: A{k1:va10}, B{k1:vb100} -> (1,10,100)
        let out = op
            .process(
                &[
                    vec![left_batch(&[(1, 10, 1)])],
                    vec![right_batch(&[(1, 100, 1)])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();
        net_into(&mut snap, &out);
        assert_eq!(snap.get(&(1, 10, 100)), Some(&1));

        // Cycle 2: A updates va 10->15 (retract + insert), B unchanged.
        let out = op
            .process(
                &[vec![left_batch(&[(1, 10, -1), (1, 15, 1)])], vec![]],
                &[0, 0],
            )
            .await
            .unwrap();
        net_into(&mut snap, &out);
        assert_eq!(snap.get(&(1, 10, 100)), None, "stale (1,10,100) retracted");
        assert_eq!(snap.get(&(1, 15, 100)), Some(&1));

        // Cycle 3: B updates vb 100->200 for k1.
        let out = op
            .process(
                &[vec![], vec![right_batch(&[(1, 100, -1), (1, 200, 1)])]],
                &[0, 0],
            )
            .await
            .unwrap();
        net_into(&mut snap, &out);
        assert_eq!(snap.get(&(1, 15, 100)), None, "stale right value retracted");
        assert_eq!(snap.get(&(1, 15, 200)), Some(&1));
        assert_eq!(snap.len(), 1);
    }

    #[tokio::test]
    async fn inner_ivm_join_simultaneous_both_sides_update() {
        let mut op = IncrementalJoinOperator::new(config());
        let mut snap: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        // Seed (1,10,100).
        let out = op
            .process(
                &[
                    vec![left_batch(&[(1, 10, 1)])],
                    vec![right_batch(&[(1, 100, 1)])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();
        net_into(&mut snap, &out);

        // Same cycle: A va 10->15 AND B vb 100->200. The cross terms must cancel; only the new
        // pair (1,15,200) survives.
        let out = op
            .process(
                &[
                    vec![left_batch(&[(1, 10, -1), (1, 15, 1)])],
                    vec![right_batch(&[(1, 100, -1), (1, 200, 1)])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();
        net_into(&mut snap, &out);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.get(&(1, 15, 200)), Some(&1));
    }

    // LEFT join: unmatched left rows are NULL-padded (NULL right value = i64::MIN sentinel here),
    // a first right match (0→1) retracts the pad and emits the inner row, and the last-match retract
    // re-emits the pad.
    #[tokio::test]
    async fn left_outer_nullpad_emit_retract_reemit() {
        const NULL: i64 = i64::MIN;
        let mut op = IncrementalJoinOperator::new(left_config());
        let mut snap: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        // Cycle 1: left k=1, right has only k=2 (so right schema is known, k=1 has no match).
        let out = op
            .process(
                &[
                    vec![left_batch(&[(1, 10, 1)])],
                    vec![right_batch(&[(2, 200, 1)])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();
        net_into_nullable(&mut snap, &out);
        assert_eq!(
            snap.get(&(1, 10, NULL)),
            Some(&1),
            "unmatched left NULL-padded"
        );
        assert_eq!(snap.len(), 1);

        // Cycle 2: right k=1 arrives (0→1). Pad retracts, inner (1,10,100) appears.
        let out = op
            .process(&[vec![], vec![right_batch(&[(1, 100, 1)])]], &[0, 0])
            .await
            .unwrap();
        net_into_nullable(&mut snap, &out);
        assert_eq!(
            snap.get(&(1, 10, NULL)),
            None,
            "pad retracted on first match"
        );
        assert_eq!(snap.get(&(1, 10, 100)), Some(&1));
        assert_eq!(snap.len(), 1);

        // Cycle 3: right k=1 retracts (last match gone). Inner retracts, pad re-emitted.
        let out = op
            .process(&[vec![], vec![right_batch(&[(1, 100, -1)])]], &[0, 0])
            .await
            .unwrap();
        net_into_nullable(&mut snap, &out);
        assert_eq!(snap.get(&(1, 10, 100)), None, "inner retracted");
        assert_eq!(snap.get(&(1, 10, NULL)), Some(&1), "pad re-emitted");
        assert_eq!(snap.len(), 1);
    }

    // LEFT join catch-up: left rows accumulate before the right side is ever seen; once the first
    // right batch arrives, the back-fill emits their NULL-pad.
    #[tokio::test]
    async fn left_outer_catchup_on_first_right() {
        const NULL: i64 = i64::MIN;
        let mut op = IncrementalJoinOperator::new(left_config());
        let mut snap: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        // Cycles 1-2: only left data; right never seen → no output yet.
        let out = op
            .process(&[vec![left_batch(&[(1, 10, 1)])], vec![]], &[0, 0])
            .await
            .unwrap();
        assert!(
            out.iter().all(|b| b.num_rows() == 0),
            "no output before right seen"
        );
        let out = op
            .process(&[vec![left_batch(&[(2, 20, 1)])], vec![]], &[0, 0])
            .await
            .unwrap();
        assert!(out.iter().all(|b| b.num_rows() == 0));

        // Cycle 3: right k=2 arrives. Catch-up pads k=1 (no match); k=2 joins inner.
        let out = op
            .process(&[vec![], vec![right_batch(&[(2, 200, 1)])]], &[0, 0])
            .await
            .unwrap();
        net_into_nullable(&mut snap, &out);
        assert_eq!(
            snap.get(&(1, 10, NULL)),
            Some(&1),
            "k=1 caught up as NULL-pad"
        );
        assert_eq!(snap.get(&(2, 20, 200)), Some(&1), "k=2 joined inner");
        assert_eq!(snap.len(), 2);
    }

    // A LEFT join must NULL-pad a left row with a NULL join key (it can never match), not drop it.
    #[tokio::test]
    async fn left_outer_nullpads_left_row_with_null_key() {
        let nullable_left = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("va", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let left = RecordBatch::try_new(
            nullable_left,
            vec![
                Arc::new(Int64Array::from(vec![None, Some(1)])), // k = NULL, 1
                Arc::new(Int64Array::from(vec![10, 11])),        // va
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        let right = right_batch(&[(1, 100, 1)]);

        let mut op = IncrementalJoinOperator::new(left_config());
        let out = op
            .process(&[vec![left], vec![right]], &[0, 0])
            .await
            .unwrap();

        let mut rows: Vec<(i64, bool)> = Vec::new();
        for b in &out {
            let col = |i: usize| {
                b.column(i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone()
            };
            let (va, vb, w) = (col(1), col(2), col(3));
            for r in 0..b.num_rows() {
                if w.value(r) > 0 {
                    rows.push((va.value(r), vb.is_null(r)));
                }
            }
        }
        rows.sort_unstable();
        assert!(
            rows.contains(&(10, true)),
            "NULL-key left row NULL-padded (va=10, vb NULL), not dropped"
        );
        assert!(
            rows.contains(&(11, false)),
            "k=1 left row joins inner (va=11)"
        );
    }

    #[tokio::test]
    async fn null_join_keys_do_not_match() {
        let left = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("va", DataType::Int64, false),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![None, Some(1)])),
                Arc::new(Int64Array::from(vec![10, 11])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("vb", DataType::Int64, false),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![None, Some(1)])),
                Arc::new(Int64Array::from(vec![100, 101])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();

        let mut op = IncrementalJoinOperator::new(config());
        let out = op
            .process(&[vec![left], vec![right]], &[0, 0])
            .await
            .unwrap();
        let mut snap: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        net_into(&mut snap, &out);
        // Only the real key=1 joins; NULL keys never match each other.
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.get(&(1, 11, 101)), Some(&1));
    }

    #[tokio::test]
    async fn checkpoint_restore_round_trips_side_state() {
        let mut op = IncrementalJoinOperator::new(config());
        // Populate both sides: left {k1:va10, k2:va20}, right {k1:vb100, k2:vb200}.
        op.process(
            &[
                vec![left_batch(&[(1, 10, 1), (2, 20, 1)])],
                vec![right_batch(&[(1, 100, 1), (2, 200, 1)])],
            ],
            &[0, 0],
        )
        .await
        .unwrap();
        let cp = op
            .checkpoint()
            .unwrap()
            .expect("non-empty state checkpoints");

        // Restore into a fresh operator, then update LEFT k1 (10->15). Netting the retraction of the
        // stale (1,10,100) requires the restored RIGHT state (k1:vb100).
        let mut restored = IncrementalJoinOperator::new(config());
        restored.restore(cp).unwrap();
        let out = restored
            .process(
                &[vec![left_batch(&[(1, 10, -1), (1, 15, 1)])], vec![]],
                &[0, 0],
            )
            .await
            .unwrap();
        let mut snap: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        // Seed snapshot with the pre-checkpoint joined rows (as the MV Multiset store would hold).
        snap.insert((1, 10, 100), 1);
        snap.insert((2, 20, 200), 1);
        net_into(&mut snap, &out);
        assert_eq!(
            snap.get(&(1, 10, 100)),
            None,
            "stale row retracted post-restore"
        );
        assert_eq!(snap.get(&(1, 15, 100)), Some(&1));
        assert_eq!(snap.get(&(2, 20, 200)), Some(&1), "untouched key intact");
        assert_eq!(snap.len(), 2);
    }

    // LEFT join: a checkpoint taken while the right side is SEEN-but-EMPTY must record that the
    // right was seen, so a post-restart right batch does NOT re-fire the catch-up (which would
    // double-emit NULL-pads and never retract the stale ones).
    #[tokio::test]
    async fn left_outer_checkpoint_with_empty_seen_right_no_catchup_resurrection() {
        const NULL: i64 = i64::MIN;
        let mut op = IncrementalJoinOperator::new(left_config());
        let mut snap: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        // Cycle 1: both sides present → inner rows.
        let out = op
            .process(
                &[
                    vec![left_batch(&[(1, 10, 1), (2, 20, 1)])],
                    vec![right_batch(&[(1, 100, 1), (2, 200, 1)])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();
        net_into_nullable(&mut snap, &out);

        // Cycle 2: right retracts everything → pads re-emitted, right_state now empty (but seen).
        let out = op
            .process(
                &[vec![], vec![right_batch(&[(1, 100, -1), (2, 200, -1)])]],
                &[0, 0],
            )
            .await
            .unwrap();
        net_into_nullable(&mut snap, &out);
        assert_eq!(snap.get(&(1, 10, NULL)), Some(&1));
        assert_eq!(snap.get(&(2, 20, NULL)), Some(&1));

        // Checkpoint (right is seen-but-empty), restore into a fresh operator.
        let cp = op.checkpoint().unwrap().expect("left state checkpoints");
        let mut restored = IncrementalJoinOperator::new(left_config());
        restored.restore(cp).unwrap();

        // Cycle 3: right returns at k1 only. k1's pad must retract + inner appear; k2's pad must
        // stay at multiplicity 1 (no catch-up resurrection).
        let out = restored
            .process(&[vec![], vec![right_batch(&[(1, 100, 1)])]], &[0, 0])
            .await
            .unwrap();
        net_into_nullable(&mut snap, &out);
        assert_eq!(snap.get(&(1, 10, NULL)), None, "k1 pad retracted on return");
        assert_eq!(snap.get(&(1, 10, 100)), Some(&1), "k1 inner row");
        assert_eq!(snap.get(&(2, 20, NULL)), Some(&1), "k2 pad NOT doubled");
        assert_eq!(snap.len(), 2);
    }

    // ─── Tier-backed join state (A1-emit 3b-S4.1) ───────────────────────────────────────────────

    #[cfg(feature = "state-tier")]
    #[test]
    fn tier_key_codec_is_stable_and_null_safe() {
        let codec = JoinKeyCodec::new(vec![DataType::Int64, DataType::Utf8]).unwrap();
        let k1 = vec![i64_scalar(1), ScalarValue::Utf8(Some("a".into()))];
        let k2 = vec![i64_scalar(1), ScalarValue::Utf8(Some("b".into()))];
        let k_null = vec![
            ScalarValue::Int64(None),
            ScalarValue::Utf8(Some("a".into())),
        ];

        // Deterministic: a key encodes to the same bytes every time, distinct keys differ.
        assert_eq!(codec.encode(&k1).unwrap(), codec.encode(&k1).unwrap());
        assert_ne!(codec.encode(&k1).unwrap(), codec.encode(&k2).unwrap());
        // NULL-safe: a NULL component encodes and is distinct from a non-NULL key.
        assert_ne!(codec.encode(&k_null).unwrap(), codec.encode(&k1).unwrap());

        // Restart-stable: a freshly-built converter of the same types yields identical bytes + vnode.
        let codec2 = JoinKeyCodec::new(vec![DataType::Int64, DataType::Utf8]).unwrap();
        assert_eq!(codec.encode(&k1).unwrap(), codec2.encode(&k1).unwrap());
        assert_eq!(codec.vnode(&k1, 8).unwrap(), codec2.vnode(&k1, 8).unwrap());
        assert!(codec.vnode(&k1, 8).unwrap() < 8);
    }

    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn tier_demote_then_promote_round_trips() {
        let mut op = IncrementalJoinOperator::new(config());
        op.tier.delta_tracking = true;
        op.process(
            &[
                vec![left_batch(&[(1, 10, 1), (2, 20, 1)])],
                vec![right_batch(&[(1, 100, 1), (2, 200, 1)])],
            ],
            &[0, 0],
        )
        .await
        .unwrap();

        let baseline = op.estimated_state_bytes();
        let k1 = vec![i64_scalar(1)];
        let left_before = op.left_state.get(&k1);
        let right_before = op.right_state.get(&k1);
        assert!(!left_before.is_empty() && !right_before.is_empty());

        let blob = op.demote_key(&k1).unwrap();
        assert!(!op.left_state.contains_key(&k1));
        assert!(!op.right_state.contains_key(&k1));
        assert!(op.tier.is_cold(&k1));
        assert!(op.estimated_state_bytes() < baseline, "demote sheds bytes");

        op.promote_key(&k1, &blob).unwrap();
        assert!(!op.tier.is_cold(&k1));
        assert_eq!(
            op.estimated_state_bytes(),
            baseline,
            "bytes return to baseline after promote"
        );
        // Both sides' Z-set content is preserved (one row per side for k1).
        assert_eq!(op.left_state.get(&k1), left_before);
        assert_eq!(op.right_state.get(&k1), right_before);
    }

    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn tier_demotable_clean_vs_dirty_gate() {
        let mut op = IncrementalJoinOperator::new(config());
        op.tier.delta_tracking = true;
        op.process(
            &[
                vec![left_batch(&[(1, 10, 1), (2, 20, 1)])],
                vec![right_batch(&[(1, 100, 1), (2, 200, 1)])],
            ],
            &[0, 0],
        )
        .await
        .unwrap();

        // Every key was just mutated → dirty → none demotable.
        assert!(op.demotable_keys(10).is_empty());
        // A capture clears dirty → both keys become demotable.
        op.tier.clear_dirty();
        assert_eq!(op.demotable_keys(10).len(), 2);
        // Touch k1 again → only the untouched k2 stays demotable.
        op.process(&[vec![left_batch(&[(1, 11, 1)])], vec![]], &[0, 0])
            .await
            .unwrap();
        assert_eq!(op.demotable_keys(10), vec![vec![i64_scalar(2)]]);
    }

    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn tier_demotable_excludes_null_keys() {
        // A LEFT join keeps a NULL-key left row; it must never be demoted (it can never match).
        let nullable_left = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("va", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let left = RecordBatch::try_new(
            nullable_left,
            vec![
                Arc::new(Int64Array::from(vec![None, Some(1)])),
                Arc::new(Int64Array::from(vec![10, 11])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        let mut op = IncrementalJoinOperator::new(left_config());
        op.tier.delta_tracking = true;
        op.process(&[vec![left], vec![right_batch(&[(1, 100, 1)])]], &[0, 0])
            .await
            .unwrap();
        op.tier.clear_dirty();

        let demotable = op.demotable_keys(10);
        assert!(demotable.iter().all(|k| k.iter().all(|s| !s.is_null())));
        assert_eq!(
            demotable,
            vec![vec![i64_scalar(1)]],
            "NULL key excluded; only k=1 demotable"
        );
    }

    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn tier_cold_keys_touched_projects_side_key() {
        let mut op = IncrementalJoinOperator::new(config());
        op.tier.delta_tracking = true;
        op.process(
            &[
                vec![left_batch(&[(1, 10, 1), (2, 20, 1)])],
                vec![right_batch(&[(1, 100, 1), (2, 200, 1)])],
            ],
            &[0, 0],
        )
        .await
        .unwrap();
        op.tier.clear_dirty();
        op.demote_key(&[i64_scalar(1)]).unwrap();

        // A left batch referencing cold key 1 and live key 2 → only 1 is reported.
        let touched = op
            .cold_keys_touched(JoinSide::Left, &[left_batch(&[(1, 99, 1), (2, 99, 1)])])
            .unwrap();
        assert_eq!(touched, vec![vec![i64_scalar(1)]]);
        // A right batch referencing only a live key → none.
        let touched = op
            .cold_keys_touched(JoinSide::Right, &[right_batch(&[(2, 5, 1)])])
            .unwrap();
        assert!(touched.is_empty());
    }

    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn tier_left_outer_unmatched_key_blob_stays_right_absent() {
        // A LEFT-join key with left rows but no right match must demote+promote to a right-ABSENT
        // state, so a later presence probe still reads "no match" (no fabricated NULL-pad flip).
        let mut op = IncrementalJoinOperator::new(left_config());
        op.tier.delta_tracking = true;
        op.process(
            &[
                vec![left_batch(&[(1, 10, 1)])],
                vec![right_batch(&[(2, 200, 1)])],
            ],
            &[0, 0],
        )
        .await
        .unwrap();
        op.tier.clear_dirty();

        let k1 = vec![i64_scalar(1)];
        assert!(op.left_state.contains_key(&k1) && !op.right_state.contains_key(&k1));
        let blob = op.demote_key(&k1).unwrap();
        assert!(!op.left_state.contains_key(&k1));

        op.promote_key(&k1, &blob).unwrap();
        assert!(op.left_state.contains_key(&k1), "left rows restored");
        assert!(
            !op.right_state.contains_key(&k1),
            "right side stays absent after promote"
        );
    }
}
