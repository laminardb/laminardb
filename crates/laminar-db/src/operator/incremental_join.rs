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

use laminar_core::changelog::WEIGHT_COLUMN;

use crate::error::DbError;
use crate::mv_store::{batches_to_ipc, ipc_to_batches};
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};
use crate::sql_analysis::{IncrementalJoinConfig, JoinProjItem, JoinSide};

/// Indexed Z-set: `join_key -> { full_row -> multiplicity }`.
#[derive(Default)]
pub(crate) struct InMemoryJoinState {
    rows: FxHashMap<Vec<ScalarValue>, FxHashMap<Box<[ScalarValue]>, i64>>,
}

#[derive(Clone, Copy, Debug)]
struct JoinStateUpsert {
    previous_weight: i64,
    next_weight: i64,
}

impl InMemoryJoinState {
    /// Plan the multiplicity change without touching the Z-set so validation failures are
    /// side-effect free.
    fn plan_upsert(
        &self,
        key: &[ScalarValue],
        row: &[ScalarValue],
        weight: i64,
    ) -> Result<JoinStateUpsert, DbError> {
        let inner = self.rows.get(key);
        let previous = inner.and_then(|rows| rows.get(row)).copied().unwrap_or(0);
        let next_weight = previous.checked_add(weight).ok_or_else(|| {
            DbError::Pipeline("incremental join: row multiplicity overflow".into())
        })?;
        Ok(JoinStateUpsert {
            previous_weight: previous,
            next_weight,
        })
    }

    /// Apply a previously validated mutation plan.
    fn apply_upsert(&mut self, key: &[ScalarValue], row: &[ScalarValue], plan: JoinStateUpsert) {
        if plan.next_weight > 0 {
            if let Some(inner) = self.rows.get_mut(key) {
                if let Some(weight) = inner.get_mut(row) {
                    *weight = plan.next_weight;
                } else {
                    inner.insert(row.into(), plan.next_weight);
                }
            } else {
                let mut inner = FxHashMap::default();
                inner.insert(row.into(), plan.next_weight);
                self.rows.insert(key.to_vec(), inner);
            }
        } else if let Some(inner) = self.rows.get_mut(key) {
            inner.remove(row);
            if inner.is_empty() {
                self.rows.remove(key);
            }
        }
    }

    fn rollback_upsert(&mut self, key: &[ScalarValue], row: &[ScalarValue], plan: JoinStateUpsert) {
        self.apply_upsert(
            key,
            row,
            JoinStateUpsert {
                previous_weight: plan.next_weight,
                next_weight: plan.previous_weight,
            },
        );
    }

    #[cfg(test)]
    fn upsert(&mut self, key: &[ScalarValue], row: &[ScalarValue], weight: i64) {
        let plan = self.plan_upsert(key, row, weight).unwrap();
        self.apply_upsert(key, row, plan);
    }

    /// Owned snapshot of the Z-set for `key`; use [`Self::rows_for`] unless mutating the set mid-scan.
    fn get(&self, key: &[ScalarValue]) -> Vec<(Vec<ScalarValue>, i64)> {
        match self.rows.get(key) {
            Some(inner) => inner.iter().map(|(r, &w)| (r.to_vec(), w)).collect(),
            None => Vec::new(),
        }
    }

    /// Borrowing view of the Z-set for `key` — `(row, multiplicity)` with no per-probe clone.
    fn rows_for<'a>(
        &'a self,
        key: &[ScalarValue],
    ) -> impl Iterator<Item = (&'a [ScalarValue], i64)> + 'a {
        self.rows
            .get(key)
            .into_iter()
            .flat_map(|inner| inner.iter().map(|(r, &w)| (r.as_ref(), w)))
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

struct DecodedJoinSide {
    info: SideInfo,
    rows: Vec<DeltaRow>,
}

#[derive(Default)]
struct JoinStateShadow {
    keys: FxHashMap<Vec<ScalarValue>, JoinKeyShadow>,
}

struct JoinKeyShadow {
    weights: FxHashMap<Box<[ScalarValue]>, i64>,
}

impl JoinStateShadow {
    fn plan_upsert(
        &mut self,
        state: &InMemoryJoinState,
        key: &[ScalarValue],
        row: &[ScalarValue],
        weight: i64,
    ) -> Result<JoinStateUpsert, DbError> {
        let key_shadow = self
            .keys
            .entry(key.to_vec())
            .or_insert_with(|| JoinKeyShadow {
                weights: FxHashMap::default(),
            });
        let previous = key_shadow.weights.get(row).copied().unwrap_or_else(|| {
            state
                .rows
                .get(key)
                .and_then(|rows| rows.get(row))
                .copied()
                .unwrap_or(0)
        });
        let next_weight = previous.checked_add(weight).ok_or_else(|| {
            DbError::Pipeline("incremental join: row multiplicity overflow".into())
        })?;
        key_shadow.weights.insert(row.into(), next_weight);
        Ok(JoinStateUpsert {
            previous_weight: previous,
            next_weight,
        })
    }
}

struct PlannedJoinMutation {
    state: JoinStateUpsert,
}

struct IvmMutationPlan {
    right: Vec<PlannedJoinMutation>,
    left: Vec<PlannedJoinMutation>,
}

/// INNER/LEFT IVM join operator (two-input: port 0 = left changelog, port 1 = right).
pub(crate) struct IncrementalJoinOperator {
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    projection: Vec<JoinProjItem>,
    left_outer: bool,
    left_state: InMemoryJoinState,
    right_state: InMemoryJoinState,
    left_info: Option<SideInfo>,
    right_info: Option<SideInfo>,
    // Flattened output columns `(side, plain_position)` + cached output schema; resolved once both
    // side schemas are known (from a live batch per side, or from restore).
    out_cols: Option<Vec<(JoinSide, usize)>>,
    out_schema: Option<SchemaRef>,
    // LEFT join: set once the first-sight catch-up has run (or once a checkpoint with a seen right side
    // is restored), so it fires exactly once.
    left_catchup_done: bool,
}

impl IncrementalJoinOperator {
    pub(crate) fn new(config: IncrementalJoinConfig) -> Self {
        Self {
            left_keys: config.left_keys,
            right_keys: config.right_keys,
            projection: config.projection,
            left_outer: config.left_outer,
            left_state: InMemoryJoinState::default(),
            right_state: InMemoryJoinState::default(),
            left_info: None,
            right_info: None,
            out_cols: None,
            out_schema: None,
            left_catchup_done: false,
        }
    }

    fn plan_ivm_mutations(
        &self,
        delta_a: &[DeltaRow],
        delta_b: &[DeltaRow],
    ) -> Result<IvmMutationPlan, DbError> {
        let mut left_shadow = JoinStateShadow::default();
        let mut right_shadow = JoinStateShadow::default();
        let mut right = Vec::with_capacity(delta_b.len());
        for delta in delta_b {
            let state = right_shadow.plan_upsert(
                &self.right_state,
                &delta.key,
                &delta.row,
                delta.weight,
            )?;
            right.push(PlannedJoinMutation { state });
        }
        let mut left = Vec::with_capacity(delta_a.len());
        for delta in delta_a {
            let state =
                left_shadow.plan_upsert(&self.left_state, &delta.key, &delta.row, delta.weight)?;
            left.push(PlannedJoinMutation { state });
        }
        Ok(IvmMutationPlan { right, left })
    }

    fn apply_planned_mutation(
        &mut self,
        side: JoinSide,
        delta: &DeltaRow,
        planned: &PlannedJoinMutation,
    ) {
        match side {
            JoinSide::Left => {
                self.left_state
                    .apply_upsert(&delta.key, &delta.row, planned.state);
            }
            JoinSide::Right => {
                self.right_state
                    .apply_upsert(&delta.key, &delta.row, planned.state);
            }
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
            for (other_row, other_w) in build.rows_for(&d.key) {
                let w = d.weight * other_w;
                if w == 0 {
                    continue;
                }
                let proj: Vec<ScalarValue> = resolved
                    .iter()
                    .map(|&(side, pos)| {
                        let row: &[ScalarValue] = if side == delta_side {
                            &d.row
                        } else {
                            other_row
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
    fn decode_restore_side(
        &self,
        side: JoinSide,
        bytes: &[u8],
    ) -> Result<Option<DecodedJoinSide>, DbError> {
        let batches = ipc_to_batches(bytes)
            .map_err(|e| DbError::Pipeline(format!("incremental join: restore ipc: {e}")))?;
        let Some(first) = batches.first() else {
            return Err(DbError::Checkpoint(format!(
                "incremental join checkpoint contains a present {} side without its schema batch",
                match side {
                    JoinSide::Left => "left",
                    JoinSide::Right => "right",
                }
            )));
        };
        let keys = match side {
            JoinSide::Left => &self.left_keys,
            JoinSide::Right => &self.right_keys,
        };
        let info = SideInfo::resolve(&first.schema(), keys)?;
        let skip_null = side == JoinSide::Right || !self.left_outer;
        let rows = Self::parse_side(&info, &batches, skip_null)?;
        Ok(Some(DecodedJoinSide { info, rows }))
    }

    fn apply_decoded_restore(
        &mut self,
        left: Option<DecodedJoinSide>,
        right: Option<DecodedJoinSide>,
    ) -> Result<(), DbError> {
        fn build_state(decoded: &DecodedJoinSide) -> Result<InMemoryJoinState, DbError> {
            let mut state = InMemoryJoinState::default();
            for delta in &decoded.rows {
                if delta.weight <= 0 {
                    return Err(DbError::Checkpoint(format!(
                        "incremental join checkpoint row has non-positive multiplicity {}",
                        delta.weight
                    )));
                }
                let plan = state.plan_upsert(&delta.key, &delta.row, delta.weight)?;
                state.apply_upsert(&delta.key, &delta.row, plan);
            }
            Ok(state)
        }

        let left_state = left
            .as_ref()
            .map(build_state)
            .transpose()?
            .unwrap_or_default();
        let right_state = right
            .as_ref()
            .map(build_state)
            .transpose()?
            .unwrap_or_default();
        let right_was_seen = right.is_some();
        let mut staged = Self {
            left_keys: self.left_keys.clone(),
            right_keys: self.right_keys.clone(),
            projection: self.projection.clone(),
            left_outer: self.left_outer,
            left_state,
            right_state,
            left_info: left.map(|decoded| decoded.info),
            right_info: right.map(|decoded| decoded.info),
            out_cols: None,
            out_schema: None,
            left_catchup_done: right_was_seen,
        };
        staged.resolve_output()?;

        self.left_state = staged.left_state;
        self.right_state = staged.right_state;
        self.left_info = staged.left_info;
        self.right_info = staged.right_info;
        self.out_cols = staged.out_cols;
        self.out_schema = staged.out_schema;
        self.left_catchup_done = staged.left_catchup_done;
        Ok(())
    }
}

/// Whole-operator checkpoint for both resident side Z-sets.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct JoinOpCheckpoint {
    left: Option<Vec<u8>>,
    right: Option<Vec<u8>>,
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

impl IncrementalJoinOperator {
    /// One IVM cycle over already-resolved schemas: `output = δA ⋈ B_new + A_old ⋈ δB`, plus the
    /// LEFT-join NULL-pad transitions and (when `first_right`) the first-sight catch-up. Mutates both
    /// side states in place. `process` resolves schemas and computes `first_right`.
    fn run_ivm_cycle(
        &mut self,
        left_batches: &[RecordBatch],
        right_batches: &[RecordBatch],
        first_right: bool,
    ) -> Result<Vec<RecordBatch>, DbError> {
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

        let mutation_plan = self.plan_ivm_mutations(&delta_a, &delta_b)?;
        let left_catchup_before = self.left_catchup_done;

        let mut out: Vec<(Vec<ScalarValue>, i64)> = Vec::new();
        // term2 = A_old ⋈ δB (before any state mutation), then advance B, then term1 = δA ⋈ B_new.
        self.join_term(&delta_b, JoinSide::Right, &mut out);
        for (delta, planned) in delta_b.iter().zip(&mutation_plan.right) {
            self.apply_planned_mutation(JoinSide::Right, delta, planned);
        }

        let output = (|| -> Result<Vec<RecordBatch>, DbError> {
            if self.left_outer {
                if first_right {
                    self.emit_left_catchup(&mut out)?;
                    self.left_catchup_done = true;
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
            self.build_output(&out)
        })();
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                for (delta, planned) in delta_b.iter().zip(&mutation_plan.right).rev() {
                    self.right_state
                        .rollback_upsert(&delta.key, &delta.row, planned.state);
                }
                self.left_catchup_done = left_catchup_before;
                return Err(error);
            }
        };
        for (delta, planned) in delta_a.iter().zip(&mutation_plan.left) {
            self.apply_planned_mutation(JoinSide::Left, delta, planned);
        }
        Ok(output)
    }
}

#[async_trait]
impl GraphOperator for IncrementalJoinOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::fixed(
            crate::operator::capability::OperatorImplementation::IncrementalJoin,
        )
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let right_batches = inputs.get(1).map_or(&[][..], Vec::as_slice);

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
        // First sight of the right schema (LEFT join): back-fill NULL-pads for left state accumulated
        // while it was unknown. Gated on `left_catchup_done` (not "was right seen this cycle") so the
        // trigger survives a checkpoint restore (set in `restore_side`).
        let first_right = self.left_outer && !self.left_catchup_done && self.right_info.is_some();

        self.run_ivm_cycle(left_batches, right_batches, first_right)
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let left = self.side_checkpoint_bytes(JoinSide::Left)?;
        let right = self.side_checkpoint_bytes(JoinSide::Right)?;
        if left.is_none() && right.is_none() {
            return Ok(None);
        }
        let cp = JoinOpCheckpoint { left, right };
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&cp)
            .map(|v| v.to_vec())
            .map_err(|e| {
                DbError::Pipeline(format!("incremental join: checkpoint serialize: {e}"))
            })?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let cp: JoinOpCheckpoint =
            rkyv::from_bytes::<JoinOpCheckpoint, rkyv::rancor::Error>(&checkpoint.data).map_err(
                |e| DbError::Checkpoint(format!("incremental join checkpoint deserialize: {e}")),
            )?;
        let left = cp
            .left
            .as_deref()
            .map(|bytes| self.decode_restore_side(JoinSide::Left, bytes))
            .transpose()
            .map_err(|error| {
                DbError::Checkpoint(format!("incremental join left restore failed: {error}"))
            })?
            .flatten();
        let right = cp
            .right
            .as_deref()
            .map(|bytes| self.decode_restore_side(JoinSide::Right, bytes))
            .transpose()
            .map_err(|error| {
                DbError::Checkpoint(format!("incremental join right restore failed: {error}"))
            })?
            .flatten();
        self.apply_decoded_restore(left, right).map_err(|error| {
            DbError::Checkpoint(format!("incremental join state restore failed: {error}"))
        })?;
        Ok(())
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

    #[test]
    fn corrupt_join_checkpoint_is_a_recovery_fault() {
        let mut operator = IncrementalJoinOperator::new(config());
        let error = operator
            .restore(OperatorCheckpoint {
                data: b"not-rkyv".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.requires_pipeline_recovery());
    }

    #[test]
    fn corrupt_join_side_state_is_a_recovery_fault() {
        let checkpoint = JoinOpCheckpoint {
            left: Some(b"not-arrow-ipc".to_vec()),
            right: None,
        };
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .unwrap()
            .to_vec();
        let mut operator = IncrementalJoinOperator::new(config());
        let error = operator.restore(OperatorCheckpoint { data }).unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("left restore failed"));
    }

    #[tokio::test]
    async fn output_build_failure_rolls_back_right_side() {
        let mut op = IncrementalJoinOperator::new(config());
        op.process(
            &[
                vec![left_batch(&[(1, 10, 1)])],
                vec![right_batch(&[(1, 100, 1)])],
            ],
            &[0, 0],
        )
        .await
        .unwrap();
        let key = vec![i64_scalar(1)];
        let left_before = op.left_state.get(&key);
        let right_before = op.right_state.get(&key);

        op.out_schema = Some(Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("va", DataType::Int64, false),
            Field::new("vb", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])));
        let error = op
            .process(
                &[Vec::new(), vec![right_batch(&[(1, 100, -1), (1, 200, 1)])]],
                &[0, 0],
            )
            .await
            .unwrap_err();

        assert!(matches!(error, DbError::Pipeline(_)));
        assert_eq!(op.left_state.get(&key), left_before);
        assert_eq!(op.right_state.get(&key), right_before);
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

    #[tokio::test]
    async fn restoring_same_join_checkpoint_twice_is_idempotent() {
        let mut source = IncrementalJoinOperator::new(config());
        source
            .process(
                &[
                    vec![left_batch(&[(1, 10, 2)])],
                    vec![right_batch(&[(1, 100, 3)])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();
        let data = source.checkpoint().unwrap().unwrap().data;
        let mut restored = IncrementalJoinOperator::new(config());

        restored
            .restore(OperatorCheckpoint { data: data.clone() })
            .unwrap();
        let left_once = restored.left_state.get(&[i64_scalar(1)]);
        let right_once = restored.right_state.get(&[i64_scalar(1)]);
        restored.restore(OperatorCheckpoint { data }).unwrap();

        assert_eq!(restored.left_state.get(&[i64_scalar(1)]), left_once);
        assert_eq!(restored.right_state.get(&[i64_scalar(1)]), right_once);
    }

    #[tokio::test]
    async fn restore_with_absent_side_clears_prior_side_state_and_schema() {
        let mut source = IncrementalJoinOperator::new(config());
        source
            .process(&[vec![left_batch(&[(1, 10, 1)])], vec![]], &[0, 0])
            .await
            .unwrap();
        let checkpoint = source.checkpoint().unwrap().unwrap();

        let mut restored = IncrementalJoinOperator::new(config());
        restored
            .process(
                &[
                    vec![left_batch(&[(9, 90, 1)])],
                    vec![right_batch(&[(9, 900, 1)])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();
        restored.restore(checkpoint).unwrap();

        assert!(restored.left_state.get(&[i64_scalar(9)]).is_empty());
        assert_eq!(restored.left_state.get(&[i64_scalar(1)]).len(), 1);
        assert!(restored.right_state.snapshot().is_empty());
        assert!(restored.right_info.is_none());
        assert!(restored.out_schema.is_none());
        assert!(!restored.left_catchup_done);
    }

    #[tokio::test]
    async fn late_side_decode_failure_preserves_both_live_sides() {
        let mut source = IncrementalJoinOperator::new(config());
        source
            .process(&[vec![left_batch(&[(1, 10, 1)])], vec![]], &[0, 0])
            .await
            .unwrap();
        let checkpoint = JoinOpCheckpoint {
            left: source.side_checkpoint_bytes(JoinSide::Left).unwrap(),
            right: Some(b"not-arrow-ipc".to_vec()),
        };
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .unwrap()
            .to_vec();

        let mut restored = IncrementalJoinOperator::new(config());
        restored
            .process(
                &[
                    vec![left_batch(&[(9, 90, 1)])],
                    vec![right_batch(&[(9, 900, 1)])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();
        let left_before = restored.left_state.get(&[i64_scalar(9)]);
        let right_before = restored.right_state.get(&[i64_scalar(9)]);

        let error = restored.restore(OperatorCheckpoint { data }).unwrap_err();
        assert!(error.requires_pipeline_recovery());
        assert_eq!(restored.left_state.get(&[i64_scalar(9)]), left_before);
        assert_eq!(restored.right_state.get(&[i64_scalar(9)]), right_before);
        assert!(restored.left_info.is_some());
        assert!(restored.right_info.is_some());
    }

    #[test]
    fn present_join_side_without_schema_batch_is_rejected() {
        let op = IncrementalJoinOperator::new(config());
        let bytes = batches_to_ipc(&left_schema(), std::iter::empty::<&RecordBatch>()).unwrap();

        let error = op
            .decode_restore_side(JoinSide::Left, &bytes)
            .err()
            .expect("a present side must retain its schema-carrying batch");

        assert!(error.to_string().contains("present left side"));
        assert!(error.to_string().contains("schema batch"));
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
}
