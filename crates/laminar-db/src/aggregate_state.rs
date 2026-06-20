//! Incremental aggregation state for streaming GROUP BY queries.
//!
//! One `IncrementalAggState` per pipeline; one `DataFusion` `Accumulator` per
//! aggregate per group. Cross-vnode partial merges live in
//! `laminar_core::state::partial_aggregate` and are a separate concern.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use rustc_hash::FxHashMap;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::AggregateUDF;

use crate::error::DbError;

mod checkpoints;
mod compile;
mod keys;
mod scalar_ipc;
pub(crate) use checkpoints::{
    query_fingerprint, AggStateCheckpoint, EmittedCheckpoint, EowcStateCheckpoint, GroupCheckpoint,
    JoinStateCheckpoint, WindowCheckpoint,
};
pub(crate) use compile::{
    apply_compiled_having, compile_having_filter, expr_to_sql, extract_clauses, find_aggregate,
    CompiledProjection, PreAggBuilder,
};
pub(crate) use keys::{
    global_aggregate_key, row_to_scalar_key_with_types, scalar_key_to_owned_row,
};
pub(crate) use scalar_ipc::{ipc_to_scalars, scalars_to_ipc};

/// Builds the per-window result batch for one closed window.
/// Output schema: `[group_cols..., agg_outputs...]`.
pub(crate) fn emit_window_batch(
    groups: ahash::AHashMap<arrow::row::OwnedRow, Vec<Box<dyn datafusion_expr::Accumulator>>>,
    row_converter: &arrow::row::RowConverter,
    num_group_cols: usize,
    agg_specs: &[AggFuncSpec],
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, DbError> {
    let num_rows = groups.len();
    if num_rows == 0 {
        return Ok(None);
    }

    // Collect keys and evaluate accumulators in a single drain pass.
    let mut row_keys: Vec<arrow::row::OwnedRow> = Vec::with_capacity(num_rows);
    let mut agg_scalars: Vec<Vec<ScalarValue>> = (0..agg_specs.len())
        .map(|_| Vec::with_capacity(num_rows))
        .collect();

    for (key, mut accs) in groups {
        row_keys.push(key);
        for (i, acc) in accs.iter_mut().enumerate() {
            let sv = acc
                .evaluate()
                .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;
            agg_scalars[i].push(sv);
        }
    }

    let group_arrays = if num_group_cols == 0 {
        Vec::new()
    } else {
        row_converter
            .convert_rows(row_keys.iter().map(arrow::row::OwnedRow::row))
            .map_err(|e| DbError::Pipeline(format!("group key array: {e}")))?
    };

    let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(agg_specs.len());
    for (agg_idx, scalars) in agg_scalars.into_iter().enumerate() {
        let spec = &agg_specs[agg_idx];
        let array = ScalarValue::iter_to_array(scalars)
            .map_err(|e| DbError::Pipeline(format!("agg result array: {e}")))?;
        if array.data_type() == &spec.return_type {
            agg_arrays.push(array);
        } else {
            let casted = arrow::compute::cast(&array, &spec.return_type).unwrap_or(array);
            agg_arrays.push(casted);
        }
    }

    let mut all_arrays = Vec::with_capacity(group_arrays.len() + agg_arrays.len());
    all_arrays.extend(group_arrays);
    all_arrays.extend(agg_arrays);

    let batch = RecordBatch::try_new(Arc::clone(output_schema), all_arrays)
        .map_err(|e| DbError::Pipeline(format!("result batch build: {e}")))?;

    Ok(Some(batch))
}

pub(crate) struct AggFuncSpec {
    pub(crate) udf: Arc<AggregateUDF>,
    pub(crate) input_types: Vec<DataType>,
    pub(crate) input_col_indices: Vec<usize>,
    pub(crate) output_name: String,
    pub(crate) return_type: DataType,
    pub(crate) distinct: bool,
    pub(crate) is_count_star: bool,
    pub(crate) filter_col_index: Option<usize>,
}

impl AggFuncSpec {
    pub(crate) fn create_accumulator(
        &self,
    ) -> Result<Box<dyn datafusion_expr::Accumulator>, DbError> {
        let return_field = Arc::new(Field::new(
            &self.output_name,
            self.return_type.clone(),
            true,
        ));
        let schema = Schema::new(
            self.input_types
                .iter()
                .enumerate()
                .map(|(i, dt)| Field::new(format!("col_{i}"), dt.clone(), true))
                .collect::<Vec<_>>(),
        );
        let expr_fields: Vec<Arc<Field>> = self
            .input_types
            .iter()
            .enumerate()
            .map(|(i, dt)| Arc::new(Field::new(format!("col_{i}"), dt.clone(), true)))
            .collect();
        let args = AccumulatorArgs {
            return_field,
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: self.udf.name(),
            is_distinct: self.distinct,
            exprs: &[],
            expr_fields: &expr_fields,
        };
        self.udf.accumulator(args).map_err(|e| {
            DbError::Pipeline(format!(
                "accumulator creation failed for '{}': {e}",
                self.udf.name()
            ))
        })
    }

    /// Create a retractable accumulator for changelog streams (`__weight` as last input).
    pub(crate) fn create_retractable_accumulator(
        &self,
    ) -> Result<Box<dyn datafusion_expr::Accumulator>, DbError> {
        crate::retractable_accumulator::create_retractable(
            &self.udf.name().to_lowercase(),
            &self.return_type,
            self.is_count_star,
        )
    }
}

/// Snapshot an accumulator's state for a checkpoint and rebuild it in place.
///
/// `Accumulator::state()` may drain internal state (e.g. DISTINCT hash sets);
/// we rebuild the accumulator from the snapshot so it keeps running correctly.
/// `retractable` must match how the accumulator was created.
pub(crate) fn snapshot_and_rebuild(
    acc: &mut Box<dyn datafusion_expr::Accumulator>,
    spec: &AggFuncSpec,
    retractable: bool,
) -> Result<Vec<u8>, DbError> {
    let state = acc
        .state()
        .map_err(|e| DbError::Pipeline(format!("accumulator state: {e}")))?;
    // Rebuild before serializing: a scalars_to_ipc failure must not leave
    // the accumulator empty for the next cycle.
    let arrays: Vec<ArrayRef> = state
        .iter()
        .map(|sv| {
            sv.to_array()
                .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))
        })
        .collect::<Result<_, _>>()?;
    let mut rebuilt = if retractable {
        spec.create_retractable_accumulator()?
    } else {
        spec.create_accumulator()?
    };
    rebuilt
        .merge_batch(&arrays)
        .map_err(|e| DbError::Pipeline(format!("accumulator rebuild: {e}")))?;
    *acc = rebuilt;
    let ipc = scalars_to_ipc(&state)?;
    Ok(ipc)
}

#[cfg(feature = "cluster")]
fn scalars_to_arrays(scalars: &[ScalarValue]) -> Result<Vec<ArrayRef>, DbError> {
    scalars
        .iter()
        .map(|sv| {
            sv.to_array()
                .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))
        })
        .collect()
}

/// Minimum interval between full O(groups) size re-walks.
const SIZE_REWALK_MIN_INTERVAL_MS: u64 = 2_000;

// Atomic fields let `estimated_size_bytes` update the cache through `&self`
// without requiring a write lock. Single-threaded in practice; Relaxed suffices.
struct SizeEstimateCache {
    bytes: AtomicUsize,
    // u64::MAX forces the next read to re-walk regardless of elapsed time.
    walked_gen: AtomicU64,
    walked_at_ms: AtomicU64,
}

impl SizeEstimateCache {
    fn new() -> Self {
        Self {
            bytes: AtomicUsize::new(0),
            walked_gen: AtomicU64::new(u64::MAX),
            walked_at_ms: AtomicU64::new(0),
        }
    }

    /// Force the next size probe to re-walk, bypassing the interval throttle.
    fn invalidate(&self) {
        self.walked_gen.store(u64::MAX, Ordering::Relaxed);
    }
}

fn epoch_ms_coarse() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

pub(crate) struct IncrementalAggState {
    pre_agg_sql: String,
    num_group_cols: usize,
    group_types: Vec<DataType>,
    agg_specs: Vec<AggFuncSpec>,
    groups: AHashMap<arrow::row::OwnedRow, GroupEntry>,
    state_gen: u64,
    size_cache: SizeEstimateCache,
    row_converter: arrow::row::RowConverter,
    output_schema: SchemaRef,
    compiled_projection: Option<CompiledProjection>,
    cached_pre_agg_physical: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    having_filter: Option<Arc<dyn PhysicalExpr>>,
    having_sql: Option<String>,
    max_groups: usize,
    emit_changelog: bool,
    last_emitted: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
    // Group keys touched since the last emit. The changelog path re-evaluates only
    // these instead of scanning every group. Transient (cleared each emit),
    // populated only when emit_changelog, so not checkpointed.
    dirty_keys: AHashSet<arrow::row::OwnedRow>,
    pub(crate) idle_ttl_ms: Option<u64>,
    weight_col_idx: Option<usize>,
    // None = no per-vnode capture yet; dirty tracking is off.
    #[cfg(feature = "state-tier")]
    tier_vnode_count: Option<u32>,
    // Vnodes touched since the last capture; demoting them would lose changes.
    #[cfg(feature = "state-tier")]
    dirty_vnodes: rustc_hash::FxHashSet<u32>,
    // Bulk restore/merge happened since the last capture; block all demotions.
    #[cfg(feature = "state-tier")]
    dirty_all: bool,
    // Groups for these vnodes were dropped; captures stage a cold marker.
    #[cfg(feature = "state-tier")]
    cold_vnodes: rustc_hash::FxHashSet<u32>,
}

impl IncrementalAggState {
    /// Number of leading GROUP BY columns; used by the shuffle path for hashing.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub(crate) fn num_group_cols(&self) -> usize {
        self.num_group_cols
    }
}

/// Z-set weight column name shared between the MV producer and upsert-sink consumers.
pub(crate) use laminar_core::changelog::WEIGHT_COLUMN;

/// Build a weighted `RecordBatch` from collected keys, values, and weights.
fn build_weighted_batch(
    keys: &[arrow::row::OwnedRow],
    vals: &[Vec<ScalarValue>],
    weights: &[i64],
    row_converter: &arrow::row::RowConverter,
    num_group_cols: usize,
    agg_specs: &[AggFuncSpec],
    output_schema: &SchemaRef,
) -> Result<RecordBatch, DbError> {
    let group_arrays = if num_group_cols > 0 {
        row_converter
            .convert_rows(keys.iter().map(arrow::row::OwnedRow::row))
            .map_err(|e| DbError::Pipeline(format!("group key array: {e}")))?
    } else {
        Vec::new()
    };

    let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(agg_specs.len());
    for (agg_idx, spec) in agg_specs.iter().enumerate() {
        let scalars: Vec<ScalarValue> = vals.iter().map(|v| v[agg_idx].clone()).collect();
        let array = ScalarValue::iter_to_array(scalars)
            .map_err(|e| DbError::Pipeline(format!("agg array: {e}")))?;
        if array.data_type() == &spec.return_type {
            agg_arrays.push(array);
        } else {
            let casted = arrow::compute::cast(&array, &spec.return_type).unwrap_or(array);
            agg_arrays.push(casted);
        }
    }

    let weight_array: ArrayRef = Arc::new(arrow::array::Int64Array::from_iter_values(
        weights.iter().copied(),
    ));

    let mut all_arrays = group_arrays;
    all_arrays.extend(agg_arrays);
    all_arrays.push(weight_array);

    RecordBatch::try_new(Arc::clone(output_schema), all_arrays)
        .map_err(|e| DbError::Pipeline(format!("weighted batch: {e}")))
}

pub(crate) struct GroupEntry {
    pub(crate) accs: Vec<Box<dyn datafusion_expr::Accumulator>>,
    pub(crate) last_updated_ms: i64,
}

impl IncrementalAggState {
    /// Attempt to build an `IncrementalAggState` by introspecting the logical
    /// plan of the given SQL query. Returns `None` if the query does not
    /// contain an `Aggregate` node (not an aggregation query).
    #[allow(clippy::too_many_lines)]
    pub async fn try_from_sql(
        ctx: &SessionContext,
        sql: &str,
        emit_changelog: bool,
    ) -> Result<Option<Self>, DbError> {
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| DbError::Pipeline(format!("plan error: {e}")))?;

        let plan = df.logical_plan();

        // Top-level schema preserves user aliases (e.g. `SUM(x) AS total`).
        let top_schema = Arc::new(plan.schema().as_arrow().clone());

        let Some(agg_info) = find_aggregate(plan) else {
            return Ok(None);
        };

        let group_exprs = agg_info.group_exprs;
        let aggr_exprs = agg_info.aggr_exprs;
        let agg_schema = agg_info.schema;
        let input_schema = agg_info.input_schema;
        let having_predicate = agg_info.having_predicate;

        if aggr_exprs.is_empty() {
            return Ok(None);
        }

        // Bail if there's a non-trivial projection above the Aggregate
        // (e.g. SUM(a)/SUM(b) AS ratio). Check field count AND types: a
        // coincidental count match can still hide a remapping projection.
        if top_schema.fields().len() != agg_schema.fields().len() {
            return Ok(None);
        }
        for (top_f, agg_f) in top_schema.fields().iter().zip(agg_schema.fields()) {
            if top_f.data_type() != agg_f.data_type() {
                return Ok(None);
            }
        }

        let num_group_cols = group_exprs.len();

        let mut group_col_names = Vec::new();
        let mut group_types = Vec::new();
        for i in 0..num_group_cols {
            let top_field = top_schema.field(i);
            let agg_field = agg_schema.field(i);
            group_col_names.push(top_field.name().clone());
            group_types.push(agg_field.data_type().clone());
        }

        // single_source_table rejects self-joins where compilation is unsafe.
        let compile_source = crate::sql_analysis::single_source_table(sql);
        let state = ctx.state();
        let props = state.execution_props();
        let input_df_schema = &agg_info.input_df_schema;
        let compile =
            |e: &datafusion_expr::Expr| create_physical_expr(e, input_df_schema, props).ok();
        let mut builder =
            PreAggBuilder::new(&input_schema, num_group_cols, compile_source.is_some());

        for (i, group_expr) in group_exprs.iter().enumerate() {
            builder.push_group_expr(i, group_expr, &compile);
        }

        for (i, expr) in aggr_exprs.iter().enumerate() {
            let agg_schema_idx = num_group_cols + i;
            let agg_field = agg_schema.field(agg_schema_idx);
            // Top-level schema carries the user alias (e.g. `SUM(x) AS total`).
            let output_name = if agg_schema_idx < top_schema.fields().len() {
                top_schema.field(agg_schema_idx).name().clone()
            } else {
                agg_field.name().clone()
            };
            if !builder.push_aggregate(expr, output_name, agg_field, &compile) {
                return Ok(None);
            }
        }

        let mut compile_ok = builder.compile_ok;
        let next_col_idx = builder.next_col_idx;
        let mut pre_agg_select_items = builder.pre_agg_select_items;
        let agg_specs = builder.agg_specs;
        let compiled_exprs = builder.compiled_exprs;
        let proj_fields = builder.proj_fields;

        let clauses = extract_clauses(sql);

        // Check the registered schema, not the pruned plan schema, to detect __weight.
        let source_has_weight = if let Ok(tp) = ctx
            .table_provider(clauses.from_clause.trim_matches('"'))
            .await
        {
            tp.schema().column_with_name(WEIGHT_COLUMN).is_some()
        } else {
            false
        };

        let weight_col_idx = if source_has_weight {
            for spec in &agg_specs {
                if spec.distinct {
                    return Err(DbError::Pipeline(format!(
                        "DISTINCT aggregates are not supported over changelog streams \
                         ({}(DISTINCT ...) requires per-value tracking not yet implemented).",
                        spec.udf.name()
                    )));
                }
                let name = spec.udf.name().to_lowercase();
                if !matches!(name.as_str(), "sum" | "count" | "avg" | "min" | "max") {
                    return Err(DbError::Pipeline(format!(
                        "Cannot compute {}() over a changelog stream. \
                         Supported: SUM, COUNT, AVG, MIN, MAX.",
                        spec.udf.name()
                    )));
                }
            }
            let idx = next_col_idx;
            pre_agg_select_items.push(format!("\"{WEIGHT_COLUMN}\""));
            Some(idx)
        } else {
            None
        };

        let pre_agg_sql = format!(
            "SELECT {} FROM {}{}",
            pre_agg_select_items.join(", "),
            clauses.from_clause,
            clauses.where_clause,
        );

        // Skip compilation when upstream has __weight: the compiled path omits
        // it, which would shift column indices in process_batch.
        let compiled_projection = if compile_ok && weight_col_idx.is_none() {
            let filter = if let Some(where_pred) = &agg_info.where_predicate {
                if let Ok(phys) = create_physical_expr(where_pred, input_df_schema, props) {
                    Some(phys)
                } else {
                    compile_ok = false;
                    None
                }
            } else {
                None
            };
            if compile_ok {
                Some(CompiledProjection {
                    exprs: compiled_exprs,
                    filter,
                    output_schema: Arc::new(Schema::new(proj_fields)),
                })
            } else {
                None
            }
        } else {
            None
        };

        let mut output_fields: Vec<Field> = Vec::new();
        for (name, dt) in group_col_names.iter().zip(group_types.iter()) {
            output_fields.push(Field::new(name, dt.clone(), true));
        }
        for spec in &agg_specs {
            output_fields.push(Field::new(
                &spec.output_name,
                spec.return_type.clone(),
                true,
            ));
        }
        if emit_changelog {
            output_fields.push(Field::new(WEIGHT_COLUMN, DataType::Int64, false));
        }
        let output_schema = Arc::new(Schema::new(output_fields));

        let having_filter = compile_having_filter(ctx, having_predicate.as_ref(), &output_schema);
        let having_sql = if having_filter.is_none() {
            having_predicate.as_ref().map(expr_to_sql)
        } else {
            None
        };

        // Plan once at init; LiveSourceProvider leaves carry fresh data per execute.
        let cached_pre_agg_physical =
            if compiled_projection.is_none() {
                let logical = ctx.sql(&pre_agg_sql).await.map_err(|e| {
                    DbError::Pipeline(format!("pre-agg SQL planning failed for aggregate: {e}"))
                })?;
                let plan = logical.logical_plan().clone();
                Some(ctx.state().create_physical_plan(&plan).await.map_err(|e| {
                    DbError::Pipeline(format!("pre-agg physical planning failed: {e}"))
                })?)
            } else {
                None
            };

        let sort_fields: Vec<arrow::row::SortField> = group_types
            .iter()
            .map(|dt| arrow::row::SortField::new(dt.clone()))
            .collect();
        let row_converter = arrow::row::RowConverter::new(sort_fields)
            .map_err(|e| DbError::Pipeline(format!("row converter init: {e}")))?;

        Ok(Some(Self {
            pre_agg_sql,
            num_group_cols,
            group_types,
            agg_specs,
            groups: AHashMap::new(),
            state_gen: 0,
            size_cache: SizeEstimateCache::new(),
            row_converter,
            output_schema,
            compiled_projection,
            cached_pre_agg_physical,
            having_filter,
            having_sql,
            max_groups: 1_000_000,
            emit_changelog,
            last_emitted: AHashMap::new(),
            dirty_keys: AHashSet::new(),
            idle_ttl_ms: None,
            weight_col_idx,
            #[cfg(feature = "state-tier")]
            tier_vnode_count: None,
            #[cfg(feature = "state-tier")]
            dirty_vnodes: rustc_hash::FxHashSet::default(),
            #[cfg(feature = "state-tier")]
            dirty_all: false,
            #[cfg(feature = "state-tier")]
            cold_vnodes: rustc_hash::FxHashSet::default(),
        }))
    }

    /// Evict idle groups and return retraction records. Requires both `emit_changelog` and `idle_ttl_ms`.
    pub fn evict_idle(&mut self, watermark: i64) -> Result<Vec<RecordBatch>, DbError> {
        let Some(ttl) = self.idle_ttl_ms else {
            return Ok(Vec::new());
        };
        if !self.emit_changelog {
            return Ok(Vec::new());
        }

        #[allow(clippy::cast_possible_wrap)]
        let cutoff = watermark.saturating_sub(ttl as i64);

        let idle_keys: Vec<arrow::row::OwnedRow> = self
            .groups
            .iter()
            .filter(|(_, e)| e.last_updated_ms < cutoff)
            .map(|(k, _)| k.clone())
            .collect();

        if idle_keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut retract_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut retract_vals: Vec<Vec<ScalarValue>> = Vec::new();

        for key in &idle_keys {
            #[cfg(feature = "state-tier")]
            if let Some(count) = self.tier_vnode_count {
                let v = if self.num_group_cols == 0 {
                    0
                } else {
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        (laminar_core::state::key_hash(key.as_ref()) % u64::from(count)) as u32
                    }
                };
                self.dirty_vnodes.insert(v);
            }
            if let Some(old) = self.last_emitted.remove(key) {
                retract_keys.push(key.clone());
                retract_vals.push(old);
            }
            self.groups.remove(key);
        }
        self.state_gen = self.state_gen.wrapping_add(1);

        if retract_keys.is_empty() {
            return Ok(Vec::new());
        }

        let weights = vec![-1i64; retract_keys.len()];
        let batch = build_weighted_batch(
            &retract_keys,
            &retract_vals,
            &weights,
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            &self.output_schema,
        )?;
        Ok(vec![batch])
    }

    pub fn process_batch(&mut self, batch: &RecordBatch, watermark_ms: i64) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if self.num_group_cols == 0 {
            return self.process_batch_no_groups(batch, watermark_ms);
        }

        let group_cols: Vec<ArrayRef> = (0..self.num_group_cols)
            .map(|i| Arc::clone(batch.column(i)))
            .collect();

        let rows = self
            .row_converter
            .convert_columns(&group_cols)
            .map_err(|e| DbError::Pipeline(format!("row conversion: {e}")))?;

        // One OwnedRow alloc per unique group, not per row.
        let estimated_groups = (batch.num_rows() / 4).max(16);
        let mut group_indices: FxHashMap<arrow::row::Row<'_>, Vec<u32>> =
            FxHashMap::with_capacity_and_hasher(estimated_groups, rustc_hash::FxBuildHasher);
        for row_idx in 0..batch.num_rows() {
            #[allow(clippy::cast_possible_truncation)]
            group_indices
                .entry(rows.row(row_idx))
                .or_default()
                .push(row_idx as u32);
        }

        let max_groups = self.max_groups;
        let mut groups_len = self.groups.len();
        for (row_ref, indices) in &group_indices {
            #[cfg(feature = "state-tier")]
            if let Some(count) = self.tier_vnode_count {
                #[allow(clippy::cast_possible_truncation)]
                let v = (laminar_core::state::key_hash(row_ref.as_ref()) % u64::from(count)) as u32;
                self.dirty_vnodes.insert(v);
            }
            let entry = match self.groups.entry(row_ref.owned()) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    if groups_len >= max_groups {
                        tracing::warn!(
                            max_groups,
                            current_groups = groups_len,
                            "group cardinality limit reached, dropping new group"
                        );
                        continue;
                    }
                    let mut accs = Vec::with_capacity(self.agg_specs.len());
                    for spec in &self.agg_specs {
                        let acc = if self.weight_col_idx.is_some() {
                            spec.create_retractable_accumulator()?
                        } else {
                            spec.create_accumulator()?
                        };
                        accs.push(acc);
                    }
                    groups_len += 1;
                    e.insert(GroupEntry {
                        accs,
                        last_updated_ms: watermark_ms,
                    })
                }
            };
            Self::update_group_accumulators(
                &mut entry.accs,
                batch,
                indices,
                &self.agg_specs,
                self.weight_col_idx,
            )?;
            entry.last_updated_ms = watermark_ms;
            if self.emit_changelog {
                self.dirty_keys.insert(row_ref.owned());
            }
        }
        self.state_gen = self.state_gen.wrapping_add(1);

        Ok(())
    }

    /// Fast path for global aggregates (no GROUP BY).
    fn process_batch_no_groups(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> Result<(), DbError> {
        #[cfg(feature = "state-tier")]
        if self.tier_vnode_count.is_some() {
            self.dirty_vnodes.insert(0);
        }
        let empty_key = global_aggregate_key();
        if !self.groups.contains_key(&empty_key) {
            let mut accs = Vec::with_capacity(self.agg_specs.len());
            for spec in &self.agg_specs {
                let acc = if self.weight_col_idx.is_some() {
                    spec.create_retractable_accumulator()?
                } else {
                    spec.create_accumulator()?
                };
                accs.push(acc);
            }
            self.groups.insert(
                empty_key.clone(),
                GroupEntry {
                    accs,
                    last_updated_ms: watermark_ms,
                },
            );
        }
        let entry = self.groups.get_mut(&empty_key).unwrap();
        entry.last_updated_ms = watermark_ms;
        #[allow(clippy::cast_possible_truncation)]
        let all_indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
        self.state_gen = self.state_gen.wrapping_add(1);
        let res = Self::update_group_accumulators(
            &mut entry.accs,
            batch,
            &all_indices,
            &self.agg_specs,
            self.weight_col_idx,
        );
        if self.emit_changelog {
            self.dirty_keys.insert(empty_key);
        }
        res
    }

    /// Update accumulators for a group: one `take()` per column per accumulator, no per-row allocation.
    pub(crate) fn update_group_accumulators(
        accs: &mut [Box<dyn datafusion_expr::Accumulator>],
        batch: &RecordBatch,
        indices: &[u32],
        agg_specs: &[AggFuncSpec],
        weight_col_idx: Option<usize>,
    ) -> Result<(), DbError> {
        let index_array = arrow::array::UInt32Array::from(indices.to_vec());

        let weight_arr = if let Some(w_idx) = weight_col_idx {
            Some(
                compute::take(batch.column(w_idx), &index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("weight take: {e}")))?,
            )
        } else {
            None
        };

        for (i, spec) in agg_specs.iter().enumerate() {
            let mut input_arrays: Vec<ArrayRef> = Vec::with_capacity(spec.input_col_indices.len());
            for &col_idx in &spec.input_col_indices {
                let arr = compute::take(batch.column(col_idx), &index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("array take failed: {e}")))?;
                input_arrays.push(arr);
            }

            let filtered_weight = if let Some(filter_idx) = spec.filter_col_index {
                let filter_arr = compute::take(batch.column(filter_idx), &index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("filter take: {e}")))?;
                if let Some(mask) = filter_arr
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                {
                    let mut filtered = Vec::with_capacity(input_arrays.len());
                    for arr in &input_arrays {
                        filtered.push(
                            compute::filter(arr, mask)
                                .map_err(|e| DbError::Pipeline(format!("filter apply: {e}")))?,
                        );
                    }
                    input_arrays = filtered;
                    weight_arr
                        .as_ref()
                        .map(|w| {
                            compute::filter(w, mask)
                                .map_err(|e| DbError::Pipeline(format!("weight filter: {e}")))
                        })
                        .transpose()?
                } else {
                    weight_arr.clone()
                }
            } else {
                weight_arr.clone()
            };

            if let Some(w) = &filtered_weight {
                input_arrays.push(Arc::clone(w));
            }

            accs[i]
                .update_batch(&input_arrays)
                .map_err(|e| DbError::Pipeline(format!("accumulator update: {e}")))?;
        }
        Ok(())
    }

    /// Emit current aggregate state; accumulators keep running (no reset).
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.emit_changelog {
            return self.emit_changelog_delta();
        }
        self.emit_running_state()
    }

    fn emit_running_state(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.groups.is_empty() {
            return Ok(Vec::new());
        }

        let num_rows = self.groups.len();

        let group_arrays = if self.num_group_cols > 0 {
            self.row_converter
                .convert_rows(self.groups.keys().map(arrow::row::OwnedRow::row))
                .map_err(|e| DbError::Pipeline(format!("group key array build: {e}")))?
        } else {
            Vec::new()
        };

        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(self.agg_specs.len());
        for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
            let mut scalars: Vec<ScalarValue> = Vec::with_capacity(num_rows);
            for entry in self.groups.values_mut() {
                let sv = entry.accs[agg_idx]
                    .evaluate()
                    .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;
                scalars.push(sv);
            }
            let array = ScalarValue::iter_to_array(scalars)
                .map_err(|e| DbError::Pipeline(format!("agg result array build: {e}")))?;
            if array.data_type() == &spec.return_type {
                agg_arrays.push(array);
            } else {
                let casted = arrow::compute::cast(&array, &spec.return_type).unwrap_or(array);
                agg_arrays.push(casted);
            }
        }

        let mut all_arrays = group_arrays;
        all_arrays.extend(agg_arrays);

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), all_arrays)
            .map_err(|e| DbError::Pipeline(format!("result batch build: {e}")))?;

        Ok(vec![batch])
    }

    fn emit_changelog_delta(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        // Without idle_ttl_ms, last_emitted never shrinks; warn periodically.
        if self.idle_ttl_ms.is_none() && self.last_emitted.len() > 10_000 {
            use std::sync::atomic::{AtomicI64, Ordering};
            static LAST_WARN_S: AtomicI64 = AtomicI64::new(0);
            let now_s = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
            if now_s - LAST_WARN_S.load(Ordering::Relaxed) > 300 {
                LAST_WARN_S.store(now_s, Ordering::Relaxed);
                tracing::warn!(
                    last_emitted_size = self.last_emitted.len(),
                    "EMIT CHANGES aggregate has {} group keys with no idle_ttl_ms; \
                     set `idle_ttl_ms` to bound memory.",
                    self.last_emitted.len()
                );
            }
        }
        let mut retract_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut retract_vals: Vec<Vec<ScalarValue>> = Vec::new();
        let mut insert_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut insert_vals: Vec<Vec<ScalarValue>> = Vec::new();

        // Only touched groups can differ from `last_emitted`. Take the set so the loop
        // can borrow `groups`/`last_emitted`; restore it cleared to reuse the allocation.
        let mut dirty = std::mem::take(&mut self.dirty_keys);
        for key in &dirty {
            // A dirty key absent from `groups` was removed by `evict_idle`, which
            // already emitted its retraction; skip it.
            let Some(entry) = self.groups.get_mut(key) else {
                continue;
            };
            let current: Vec<ScalarValue> = entry
                .accs
                .iter_mut()
                .map(|a| a.evaluate())
                .collect::<Result<_, _>>()
                .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;

            if let Some(old) = self.last_emitted.get(key) {
                // ScalarValue::eq treats NaN != NaN; short-circuit to avoid
                // an infinite retract+insert loop on float aggregates.
                let changed = old.iter().zip(current.iter()).any(|(a, b)| match (a, b) {
                    (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y)))
                        if x.is_nan() && y.is_nan() =>
                    {
                        false
                    }
                    (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y)))
                        if x.is_nan() && y.is_nan() =>
                    {
                        false
                    }
                    _ => a != b,
                });
                if changed {
                    retract_keys.push(key.clone());
                    retract_vals.push(old.clone());
                    insert_keys.push(key.clone());
                    insert_vals.push(current.clone());
                    self.last_emitted.insert(key.clone(), current);
                }
            } else {
                insert_keys.push(key.clone());
                insert_vals.push(current.clone());
                self.last_emitted.insert(key.clone(), current);
            }
        }
        dirty.clear();
        self.dirty_keys = dirty;

        // Every remover (evict_idle, demote_vnode, restore/merge) drops the group from
        // `last_emitted` too and evict_idle retracts, so no separate deletion pass is
        // needed; the invariant below holds.
        debug_assert!(
            self.last_emitted
                .keys()
                .all(|k| self.groups.contains_key(k)),
            "last_emitted must be a subset of groups"
        );

        let total = retract_keys.len() + insert_keys.len();
        if total == 0 {
            return Ok(Vec::new());
        }

        let mut all_keys = Vec::with_capacity(total);
        let mut all_vals = Vec::with_capacity(total);
        let mut weights = Vec::with_capacity(total);

        for (k, v) in retract_keys.into_iter().zip(retract_vals) {
            all_keys.push(k);
            all_vals.push(v);
            weights.push(-1i64);
        }
        for (k, v) in insert_keys.into_iter().zip(insert_vals) {
            all_keys.push(k);
            all_vals.push(v);
            weights.push(1i64);
        }

        let batch = build_weighted_batch(
            &all_keys,
            &all_vals,
            &weights,
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            &self.output_schema,
        )?;
        Ok(vec![batch])
    }

    pub fn having_filter(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.having_filter.as_ref()
    }

    pub fn having_sql(&self) -> Option<&str> {
        self.having_sql.as_deref()
    }

    pub fn compiled_projection(&self) -> Option<&CompiledProjection> {
        self.compiled_projection.as_ref()
    }

    pub fn cached_pre_agg_physical(
        &self,
    ) -> Option<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.cached_pre_agg_physical.as_ref()
    }

    pub(crate) fn query_fingerprint(&self) -> u64 {
        query_fingerprint(&self.pre_agg_sql, &self.output_schema)
    }

    /// Approximate bytes held by group state (lazily refreshed; O(1) between re-walks).
    pub(crate) fn estimated_size_bytes(&self) -> usize {
        let gen = self.state_gen;
        let walked_gen = self.size_cache.walked_gen.load(Ordering::Relaxed);
        if walked_gen == gen {
            return self.size_cache.bytes.load(Ordering::Relaxed);
        }
        let now = epoch_ms_coarse();
        if walked_gen != u64::MAX
            && now.saturating_sub(self.size_cache.walked_at_ms.load(Ordering::Relaxed))
                < SIZE_REWALK_MIN_INTERVAL_MS
        {
            return self.size_cache.bytes.load(Ordering::Relaxed);
        }
        let mut total = 0;
        for (key, entry) in &self.groups {
            total += key.as_ref().len();
            for acc in &entry.accs {
                total += acc.size();
            }
        }
        self.size_cache.bytes.store(total, Ordering::Relaxed);
        self.size_cache.walked_gen.store(gen, Ordering::Relaxed);
        self.size_cache.walked_at_ms.store(now, Ordering::Relaxed);
        total
    }

    pub(crate) fn checkpoint_groups(&mut self) -> Result<AggStateCheckpoint, DbError> {
        let fingerprint = self.query_fingerprint();
        let mut groups = Vec::with_capacity(self.groups.len());
        for (row_key, entry) in &mut self.groups {
            let sv_key =
                row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
            let key_ipc = scalars_to_ipc(&sv_key)?;
            let retractable = self.weight_col_idx.is_some();
            let mut acc_states = Vec::with_capacity(entry.accs.len());
            for (i, acc) in entry.accs.iter_mut().enumerate() {
                acc_states.push(snapshot_and_rebuild(acc, &self.agg_specs[i], retractable)?);
            }
            groups.push(GroupCheckpoint {
                key: key_ipc,
                acc_states,
                last_updated_ms: entry.last_updated_ms,
            });
        }
        let mut last_emitted_cp = Vec::new();
        if self.emit_changelog {
            for (row_key, vals) in &self.last_emitted {
                let sv_key =
                    row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
                last_emitted_cp.push(EmittedCheckpoint {
                    key: scalars_to_ipc(&sv_key)?,
                    values: scalars_to_ipc(vals)?,
                });
            }
        }

        Ok(AggStateCheckpoint {
            fingerprint,
            groups,
            last_emitted: last_emitted_cp,
        })
    }

    pub(crate) fn restore_groups(
        &mut self,
        checkpoint: &AggStateCheckpoint,
    ) -> Result<usize, DbError> {
        let current_fp = self.query_fingerprint();
        if checkpoint.fingerprint != current_fp {
            return Err(DbError::Pipeline(format!(
                "checkpoint fingerprint mismatch: saved={}, current={}",
                checkpoint.fingerprint, current_fp
            )));
        }
        // Build locally then swap so a mid-list decode error can't leave
        // last_emitted partially populated.
        let mut new_groups: AHashMap<arrow::row::OwnedRow, GroupEntry> =
            AHashMap::with_capacity(checkpoint.groups.len());
        for gc in &checkpoint.groups {
            let sv_key = ipc_to_scalars(&gc.key)?;
            let row_key = scalar_key_to_owned_row(&self.row_converter, &sv_key, &self.group_types)?;

            let mut accs = Vec::with_capacity(self.agg_specs.len());
            for (i, spec) in self.agg_specs.iter().enumerate() {
                let mut acc = if self.weight_col_idx.is_some() {
                    spec.create_retractable_accumulator()?
                } else {
                    spec.create_accumulator()?
                };
                if i < gc.acc_states.len() {
                    let state_scalars = ipc_to_scalars(&gc.acc_states[i])?;
                    let arrays: Vec<ArrayRef> = state_scalars
                        .iter()
                        .map(|sv| {
                            sv.to_array()
                                .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))
                        })
                        .collect::<Result<_, _>>()?;
                    acc.merge_batch(&arrays)
                        .map_err(|e| DbError::Pipeline(format!("accumulator merge: {e}")))?;
                }
                accs.push(acc);
            }
            new_groups.insert(
                row_key,
                GroupEntry {
                    accs,
                    last_updated_ms: gc.last_updated_ms,
                },
            );
        }

        let mut new_last_emitted: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>> =
            AHashMap::with_capacity(checkpoint.last_emitted.len());
        for ec in &checkpoint.last_emitted {
            let sv_key = ipc_to_scalars(&ec.key)?;
            let row_key = scalar_key_to_owned_row(&self.row_converter, &sv_key, &self.group_types)?;
            let vals = ipc_to_scalars(&ec.values)?;
            new_last_emitted.insert(row_key, vals);
        }

        self.groups = new_groups;
        self.last_emitted = new_last_emitted;
        // Restored state is internally consistent (groups == last_emitted), so
        // nothing is pending for the changelog path.
        self.dirty_keys.clear();
        self.state_gen = self.state_gen.wrapping_add(1);
        self.size_cache.invalidate();
        // All state is in memory; block demotion until the next capture re-baselines.
        #[cfg(feature = "state-tier")]
        {
            self.cold_vnodes.clear();
            self.dirty_all = true;
        }
        Ok(checkpoint.groups.len())
    }

    /// Partition state into one [`AggStateCheckpoint`] per vnode using the same hash as the shuffle.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // cold checkpoint path; vnode-keyed map
    pub(crate) fn checkpoint_groups_by_vnode(
        &mut self,
        vnode_count: u32,
    ) -> Result<std::collections::HashMap<u32, AggStateCheckpoint>, DbError> {
        if vnode_count == 0 {
            return Err(DbError::Pipeline("vnode_count must be > 0".to_string()));
        }
        let fingerprint = self.query_fingerprint();
        let global = self.num_group_cols == 0;
        let retractable = self.weight_col_idx.is_some();
        let vnode_of = |row_key: &arrow::row::OwnedRow| -> u32 {
            if global {
                0
            } else {
                #[allow(clippy::cast_possible_truncation)]
                let v = (laminar_core::state::key_hash(row_key.as_ref()) % u64::from(vnode_count))
                    as u32;
                v
            }
        };

        let mut buckets: std::collections::HashMap<u32, AggStateCheckpoint> =
            std::collections::HashMap::new();
        let mut new_bucket = || AggStateCheckpoint {
            fingerprint,
            groups: Vec::new(),
            last_emitted: Vec::new(),
        };

        for (row_key, entry) in &mut self.groups {
            let vnode = vnode_of(row_key);
            let sv_key =
                row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
            let key_ipc = scalars_to_ipc(&sv_key)?;
            let mut acc_states = Vec::with_capacity(entry.accs.len());
            for (i, acc) in entry.accs.iter_mut().enumerate() {
                acc_states.push(snapshot_and_rebuild(acc, &self.agg_specs[i], retractable)?);
            }
            buckets
                .entry(vnode)
                .or_insert_with(&mut new_bucket)
                .groups
                .push(GroupCheckpoint {
                    key: key_ipc,
                    acc_states,
                    last_updated_ms: entry.last_updated_ms,
                });
        }

        if self.emit_changelog {
            for (row_key, vals) in &self.last_emitted {
                let vnode = vnode_of(row_key);
                let sv_key =
                    row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
                buckets
                    .entry(vnode)
                    .or_insert_with(&mut new_bucket)
                    .last_emitted
                    .push(EmittedCheckpoint {
                        key: scalars_to_ipc(&sv_key)?,
                        values: scalars_to_ipc(vals)?,
                    });
            }
        }

        // New baseline: staged bytes match memory; dirty tracking restarts.
        #[cfg(feature = "state-tier")]
        {
            self.tier_vnode_count = Some(vnode_count);
            self.dirty_vnodes.clear();
            self.dirty_all = false;
        }

        Ok(buckets)
    }

    /// Fold a checkpoint's accumulator states into live state via `merge_batch`.
    ///
    /// Unlike `restore_groups` (wholesale replace), this merges into existing
    /// groups associatively. Used by the cross-node vnode rehydration path.
    #[cfg(feature = "cluster")]
    pub(crate) fn merge_groups(
        &mut self,
        checkpoint: &AggStateCheckpoint,
    ) -> Result<usize, DbError> {
        let current_fp = self.query_fingerprint();
        if checkpoint.fingerprint != current_fp {
            return Err(DbError::Pipeline(format!(
                "merge fingerprint mismatch: saved={}, current={current_fp}",
                checkpoint.fingerprint,
            )));
        }

        for gc in &checkpoint.groups {
            let sv_key = ipc_to_scalars(&gc.key)?;
            let row_key = scalar_key_to_owned_row(&self.row_converter, &sv_key, &self.group_types)?;
            // merge_batch changes the group's value relative to last_emitted, so the
            // changelog path must re-evaluate it (the diff still gates actual output).
            if self.emit_changelog {
                self.dirty_keys.insert(row_key.clone());
            }
            match self.groups.entry(row_key) {
                std::collections::hash_map::Entry::Occupied(mut occ) => {
                    let entry = occ.get_mut();
                    for (i, acc) in entry.accs.iter_mut().enumerate() {
                        if let Some(state_bytes) = gc.acc_states.get(i) {
                            let state_scalars = ipc_to_scalars(state_bytes)?;
                            let arrays = scalars_to_arrays(&state_scalars)?;
                            acc.merge_batch(&arrays).map_err(|e| {
                                DbError::Pipeline(format!("accumulator merge: {e}"))
                            })?;
                        }
                    }
                    entry.last_updated_ms = entry.last_updated_ms.max(gc.last_updated_ms);
                }
                std::collections::hash_map::Entry::Vacant(vac) => {
                    let mut accs = Vec::with_capacity(self.agg_specs.len());
                    for (i, spec) in self.agg_specs.iter().enumerate() {
                        let mut acc = if self.weight_col_idx.is_some() {
                            spec.create_retractable_accumulator()?
                        } else {
                            spec.create_accumulator()?
                        };
                        if let Some(state_bytes) = gc.acc_states.get(i) {
                            let state_scalars = ipc_to_scalars(state_bytes)?;
                            let arrays = scalars_to_arrays(&state_scalars)?;
                            acc.merge_batch(&arrays).map_err(|e| {
                                DbError::Pipeline(format!("accumulator merge: {e}"))
                            })?;
                        }
                        accs.push(acc);
                    }
                    vac.insert(GroupEntry {
                        accs,
                        last_updated_ms: gc.last_updated_ms,
                    });
                }
            }
        }

        for ec in &checkpoint.last_emitted {
            let sv_key = ipc_to_scalars(&ec.key)?;
            let row_key = scalar_key_to_owned_row(&self.row_converter, &sv_key, &self.group_types)?;
            let vals = ipc_to_scalars(&ec.values)?;
            self.last_emitted.entry(row_key).or_insert(vals);
        }

        self.state_gen = self.state_gen.wrapping_add(1);
        self.size_cache.invalidate();
        // No dirty_all: callers mark only the merged vnode dirty, so other
        // clean vnodes remain demotable.
        Ok(checkpoint.groups.len())
    }
}

#[cfg(feature = "state-tier")]
impl IncrementalAggState {
    /// Vnodes demoted to the cold tier.
    pub(crate) fn cold_vnodes(&self) -> &rustc_hash::FxHashSet<u32> {
        &self.cold_vnodes
    }

    /// Whether demotion of `vnode` is safe: changelog mode, untouched since last capture,
    /// and the capture used this `vnode_count`.
    pub(crate) fn can_demote(&self, vnode: u32, vnode_count: u32) -> bool {
        self.emit_changelog
            && !self.dirty_all
            && !self.dirty_vnodes.contains(&vnode)
            && self.tier_vnode_count == Some(vnode_count)
    }

    /// Drop a vnode's groups after cold-tier confirmation. No retractions; materialized rows stay.
    pub(crate) fn demote_vnode(&mut self, vnode: u32, vnode_count: u32) -> bool {
        if !self.can_demote(vnode, vnode_count) {
            return false;
        }
        let global = self.num_group_cols == 0;
        let keys: Vec<arrow::row::OwnedRow> = self
            .groups
            .keys()
            .filter(|k| {
                let v = if global {
                    0
                } else {
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        (laminar_core::state::key_hash(k.as_ref()) % u64::from(vnode_count)) as u32
                    }
                };
                v == vnode
            })
            .cloned()
            .collect();
        for k in &keys {
            self.groups.remove(k);
            self.last_emitted.remove(k);
        }
        self.cold_vnodes.insert(vnode);
        self.state_gen = self.state_gen.wrapping_add(1);
        self.size_cache.invalidate();
        true
    }

    /// Mark a vnode hot (promoted or tier write rolled back); dirty until next capture.
    pub(crate) fn mark_vnode_hot(&mut self, vnode: u32) {
        if self.cold_vnodes.remove(&vnode) {
            self.dirty_vnodes.insert(vnode);
        }
    }

    /// Mark a vnode dirty to block demotion until the next capture.
    pub(crate) fn mark_vnode_dirty(&mut self, vnode: u32) {
        self.dirty_vnodes.insert(vnode);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_try_from_sql_rejects_post_aggregate_projection() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
                Arc::new(arrow::array::Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        // SUM(a)/SUM(b) collapses 2 aggregates into 1 derived column →
        // top_schema fields != agg_schema fields → should return None.
        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(a) / SUM(b) AS ratio FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap();
        assert!(
            result.is_none(),
            "Post-aggregate projection should return None"
        );
    }

    #[test]
    fn test_extract_clauses_simple() {
        let c = extract_clauses("SELECT a, SUM(b) FROM trades GROUP BY a");
        assert_eq!(c.from_clause, "trades");
        assert!(c.where_clause.is_empty());
    }

    #[test]
    fn test_extract_clauses_with_where() {
        let c = extract_clauses("SELECT * FROM events WHERE x > 1 GROUP BY y");
        assert_eq!(c.from_clause, "events");
        assert!(
            c.where_clause.contains("WHERE"),
            "should contain WHERE: {}",
            c.where_clause
        );
        assert!(
            c.where_clause.contains("x > 1"),
            "should contain predicate: {}",
            c.where_clause
        );
    }

    #[test]
    fn test_extract_clauses_with_join() {
        let c = extract_clauses("SELECT * FROM events e JOIN dim d ON e.id = d.id");
        // AST preserves join structure
        assert!(
            c.from_clause.contains("events"),
            "should contain events: {}",
            c.from_clause
        );
        assert!(
            c.from_clause.contains("JOIN"),
            "should contain JOIN: {}",
            c.from_clause
        );
        assert!(
            c.from_clause.contains("dim"),
            "should contain dim: {}",
            c.from_clause
        );
    }

    #[test]
    fn test_extract_clauses_keyword_in_string_literal() {
        // This would break heuristic extraction but works with AST
        let c =
            extract_clauses("SELECT * FROM logs WHERE msg = 'joined GROUP chat' GROUP BY user_id");
        assert_eq!(c.from_clause, "logs");
        // WHERE should include the full predicate including the string
        assert!(
            c.where_clause.contains("GROUP chat"),
            "string literal should be preserved: {}",
            c.where_clause
        );
    }

    #[test]
    fn test_extract_clauses_no_where() {
        let c = extract_clauses("SELECT * FROM events GROUP BY y");
        assert_eq!(c.from_clause, "events");
        assert!(c.where_clause.is_empty());
    }

    #[tokio::test]
    async fn test_try_from_sql_non_aggregate() {
        let ctx = laminar_sql::create_session_context();
        // Register a dummy table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let result = IncrementalAggState::try_from_sql(&ctx, "SELECT * FROM events", false)
            .await
            .unwrap();
        assert!(result.is_none(), "Non-aggregate query should return None");
    }

    #[tokio::test]
    async fn test_try_from_sql_with_group_by() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap();
        assert!(result.is_some(), "Aggregate query should return Some");
        let state = result.unwrap();
        assert_eq!(state.num_group_cols, 1);
        assert_eq!(state.agg_specs.len(), 1);
    }

    #[tokio::test]
    async fn test_incremental_aggregation_across_batches() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        // Register table for plan creation
        let dummy_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy_batch]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // Simulate pre-agg output: batch 1
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch1, i64::MIN).unwrap();

        let result1 = state.emit().unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0].num_rows(), 2); // two groups: a, b

        // Batch 2: more data for existing groups
        let batch2 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "c"])),
                Arc::new(arrow::array::Float64Array::from(vec![5.0, 15.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch2, i64::MIN).unwrap();

        let result2 = state.emit().unwrap();
        assert_eq!(result2.len(), 1);
        assert_eq!(result2[0].num_rows(), 3); // three groups: a, b, c

        // Verify running totals: group "a" should have 10+30+5 = 45
        let names = result2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let totals = result2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        for i in 0..result2[0].num_rows() {
            match names.value(i) {
                "a" => assert!(
                    (totals.value(i) - 45.0).abs() < f64::EPSILON,
                    "Expected 45.0 for group 'a', got {}",
                    totals.value(i)
                ),
                "b" => assert!(
                    (totals.value(i) - 20.0).abs() < f64::EPSILON,
                    "Expected 20.0 for group 'b', got {}",
                    totals.value(i)
                ),
                "c" => assert!(
                    (totals.value(i) - 15.0).abs() < f64::EPSILON,
                    "Expected 15.0 for group 'c', got {}",
                    totals.value(i)
                ),
                other => panic!("Unexpected group: {other}"),
            }
        }
    }

    /// Helper: register a table and build an `IncrementalAggState` from SQL.
    async fn setup_agg_state(sql: &str) -> (SessionContext, IncrementalAggState) {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();
        let state = IncrementalAggState::try_from_sql(&ctx, sql, false)
            .await
            .unwrap()
            .expect("expected aggregate state");
        (ctx, state)
    }

    #[tokio::test]
    async fn test_distinct_flag_extracted() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");
        assert!(state.agg_specs[0].distinct, "DISTINCT flag should be set");
    }

    #[tokio::test]
    async fn test_distinct_count_produces_correct_result() {
        let (_, mut state) =
            setup_agg_state("SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name")
                .await;

        // Pre-agg schema: name, __agg_input_1
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));

        // Feed duplicates: value 10 appears 3 times for group "a"
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a", "a"])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 10.0, 10.0, 20.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        assert_eq!(result.len(), 1);
        let count_col = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count should be Int64");
        // DISTINCT count: {10.0, 20.0} = 2
        assert_eq!(count_col.value(0), 2, "COUNT(DISTINCT) should be 2");
    }

    #[tokio::test]
    async fn test_distinct_sum_produces_correct_result() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(DISTINCT value) as total FROM events GROUP BY name")
                .await;

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));

        // Feed duplicates: 10 appears twice
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 10.0, 20.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        let total_col = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("sum should be Float64");
        // DISTINCT sum: 10 + 20 = 30 (not 10+10+20=40)
        assert!(
            (total_col.value(0) - 30.0).abs() < f64::EPSILON,
            "SUM(DISTINCT) should be 30, got {}",
            total_col.value(0)
        );
    }

    #[tokio::test]
    async fn test_filter_clause_extracted() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) FILTER (WHERE value > 0) as pos_sum FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");
        assert!(
            state.agg_specs[0].filter_col_index.is_some(),
            "FILTER clause should set filter_col_index"
        );
    }

    #[tokio::test]
    async fn test_filter_clause_applied() {
        let (_, mut state) = setup_agg_state(
            "SELECT name, SUM(value) FILTER (WHERE value > 0) as pos_sum FROM events GROUP BY name",
        )
        .await;

        // The pre-agg SQL wraps the input with CASE WHEN and adds a
        // filter boolean column. Build a batch matching that schema.
        let filter_col_idx = state.agg_specs[0]
            .filter_col_index
            .expect("filter_col_index should be set");
        let num_cols = state.num_group_cols
            + state
                .agg_specs
                .iter()
                .map(|s| s.input_col_indices.len())
                .sum::<usize>()
            + state
                .agg_specs
                .iter()
                .filter(|s| s.filter_col_index.is_some())
                .count();
        assert!(
            filter_col_idx < num_cols,
            "filter col index should be in range"
        );

        // Build pre-agg batch manually: name, CASE value, CASE filter
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
            Field::new("__agg_filter_2", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a"])),
                // value > 0 wrapped: -5 becomes NULL, 10 stays, 20 stays
                Arc::new(arrow::array::Float64Array::from(vec![-5.0, 10.0, 20.0])),
                // filter mask: false, true, true
                Arc::new(arrow::array::BooleanArray::from(vec![false, true, true])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        let total_col = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("sum should be Float64");
        // Only 10 + 20 = 30 (the -5 row is filtered out)
        assert!(
            (total_col.value(0) - 30.0).abs() < f64::EPSILON,
            "SUM with FILTER should be 30, got {}",
            total_col.value(0)
        );
    }

    #[tokio::test]
    async fn test_having_clause_detected() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name HAVING SUM(value) > 100",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");
        assert!(
            state.having_sql.is_some(),
            "HAVING predicate should be extracted"
        );
    }

    #[tokio::test]
    async fn test_create_accumulator_error_propagated() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        // Verify create_accumulator returns Ok (not panic)
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        // This should succeed without panicking
        assert!(state.process_batch(&batch, i64::MIN).is_ok());
    }

    #[tokio::test]
    async fn test_type_inference_preserves_source_int32() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Int32Array::from(vec![0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("orders", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(amount) as total FROM orders GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        // Input type should be Int32 (source type), NOT Int64 (widened)
        assert_eq!(
            state.agg_specs[0].input_types[0],
            DataType::Int32,
            "SUM(int32_col) input type should be Int32, got {:?}",
            state.agg_specs[0].input_types[0]
        );
    }

    #[tokio::test]
    async fn test_type_inference_preserves_source_float32() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float32, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float32Array::from(vec![0.0f32])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("products", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, AVG(price) as avg_price FROM products GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        // AVG input should be Float32 (source), not Float64 (widened)
        assert_eq!(
            state.agg_specs[0].input_types[0],
            DataType::Float32,
            "AVG(float32_col) input type should be Float32, got {:?}",
            state.agg_specs[0].input_types[0]
        );
    }

    #[tokio::test]
    async fn test_type_inference_literal_expr() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Int64Array::from(vec![0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, MIN(value) as min_val FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        // Int64 in, Int64 out — should still be Int64
        assert_eq!(state.agg_specs[0].input_types[0], DataType::Int64,);
    }

    #[test]
    fn test_extract_clauses_subquery_in_where() {
        // Subquery with its own WHERE — AST handles nesting
        let c = extract_clauses(
            "SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders WHERE status = 'active') GROUP BY name",
        );
        assert_eq!(c.from_clause, "orders");
        assert!(
            c.where_clause.contains("AVG"),
            "subquery should be preserved: {}",
            c.where_clause
        );
    }

    #[test]
    fn test_expr_to_sql_column() {
        use datafusion_expr::col;
        assert_eq!(expr_to_sql(&col("price")), "\"price\"");
    }

    #[test]
    fn test_expr_to_sql_string_literal() {
        let e = datafusion_expr::Expr::Literal(ScalarValue::Utf8(Some("it's".to_string())), None);
        assert_eq!(expr_to_sql(&e), "'it''s'");
    }

    #[test]
    fn test_expr_to_sql_null_literal() {
        let e = datafusion_expr::Expr::Literal(ScalarValue::Null, None);
        assert_eq!(expr_to_sql(&e), "NULL");
    }

    #[test]
    fn test_expr_to_sql_boolean_literal() {
        let t = datafusion_expr::Expr::Literal(ScalarValue::Boolean(Some(true)), None);
        assert_eq!(expr_to_sql(&t), "TRUE");
        let f = datafusion_expr::Expr::Literal(ScalarValue::Boolean(Some(false)), None);
        assert_eq!(expr_to_sql(&f), "FALSE");
    }

    #[test]
    fn test_expr_to_sql_binary_expr() {
        use datafusion_expr::{col, lit};
        let e = col("x").gt(lit(10));
        let sql = expr_to_sql(&e);
        assert!(sql.contains("\"x\""), "should contain column: {sql}");
        assert!(sql.contains('>'), "should contain >: {sql}");
        assert!(sql.contains("10"), "should contain 10: {sql}");
    }

    #[test]
    fn test_expr_to_sql_cast() {
        use datafusion_expr::Expr;
        let e = Expr::Cast(datafusion_expr::expr::Cast {
            expr: Box::new(datafusion_expr::col("x")),
            data_type: DataType::Float64,
        });
        let sql = expr_to_sql(&e);
        assert!(sql.contains("CAST"), "should contain CAST: {sql}");
        assert!(sql.contains("Float64"), "should contain target type: {sql}");
    }

    #[test]
    fn test_expr_to_sql_scalar_function() {
        use datafusion_expr::Expr;
        // Build a scalar function expr via DataFusion
        let func = datafusion::functions::string::upper();
        let e = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction {
            func,
            args: vec![datafusion_expr::col("name")],
        });
        let sql = expr_to_sql(&e);
        assert!(sql.contains("upper"), "should contain function name: {sql}");
        assert!(sql.contains("\"name\""), "should contain arg: {sql}");
    }

    #[test]
    fn test_expr_to_sql_case() {
        use datafusion_expr::{col, lit};
        let e = datafusion_expr::Expr::Case(datafusion_expr::expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(col("x").gt(lit(0))), Box::new(lit(1)))],
            else_expr: Some(Box::new(lit(0))),
        });
        let sql = expr_to_sql(&e);
        assert!(sql.starts_with("CASE"), "should start with CASE: {sql}");
        assert!(sql.contains("WHEN"), "should contain WHEN: {sql}");
        assert!(sql.contains("THEN"), "should contain THEN: {sql}");
        assert!(sql.contains("ELSE"), "should contain ELSE: {sql}");
        assert!(sql.ends_with("END"), "should end with END: {sql}");
    }

    #[test]
    fn test_expr_to_sql_not() {
        use datafusion_expr::col;
        let e = datafusion_expr::Expr::Not(Box::new(col("active")));
        assert_eq!(expr_to_sql(&e), "(NOT \"active\")");
    }

    #[test]
    fn test_expr_to_sql_negative() {
        use datafusion_expr::col;
        let e = datafusion_expr::Expr::Negative(Box::new(col("x")));
        assert_eq!(expr_to_sql(&e), "(-\"x\")");
    }

    #[test]
    fn test_expr_to_sql_is_null() {
        use datafusion_expr::col;
        let e = datafusion_expr::Expr::IsNull(Box::new(col("x")));
        assert_eq!(expr_to_sql(&e), "(\"x\" IS NULL)");
    }

    #[test]
    fn test_expr_to_sql_is_not_null() {
        use datafusion_expr::col;
        let e = datafusion_expr::Expr::IsNotNull(Box::new(col("x")));
        assert_eq!(expr_to_sql(&e), "(\"x\" IS NOT NULL)");
    }

    #[test]
    fn test_expr_to_sql_between() {
        use datafusion_expr::{col, lit};
        let e = col("x").between(lit(1), lit(10));
        let sql = expr_to_sql(&e);
        assert!(sql.contains("BETWEEN"), "should contain BETWEEN: {sql}");
        assert!(sql.contains("AND"), "should contain AND: {sql}");
    }

    #[test]
    fn test_expr_to_sql_in_list() {
        use datafusion_expr::{col, lit};
        let e = col("status").in_list(vec![lit("a"), lit("b")], false);
        let sql = expr_to_sql(&e);
        assert!(sql.contains("IN"), "should contain IN: {sql}");
        assert!(sql.contains("'a'"), "should contain 'a': {sql}");
        assert!(sql.contains("'b'"), "should contain 'b': {sql}");
    }

    #[test]
    fn test_expr_to_sql_like() {
        use datafusion_expr::col;
        let e = col("name").like(datafusion_expr::lit("foo%"));
        let sql = expr_to_sql(&e);
        assert!(sql.contains("LIKE"), "should contain LIKE: {sql}");
        assert!(sql.contains("'foo%'"), "should contain pattern: {sql}");
    }

    #[test]
    fn test_expr_to_sql_aggregate_function() {
        // AggregateFunction in expr_to_sql is used for HAVING
        use datafusion_expr::Expr;
        let sum_udf = datafusion::functions_aggregate::sum::sum_udaf();
        let e = Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
            func: sum_udf,
            params: datafusion_expr::expr::AggregateFunctionParams {
                args: vec![datafusion_expr::col("x")],
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        let sql = expr_to_sql(&e);
        assert!(sql.contains("sum"), "should contain sum: {sql}");
        assert!(sql.contains("\"x\""), "should contain arg: {sql}");
    }

    #[test]
    fn test_expr_to_sql_aggregate_distinct() {
        use datafusion_expr::Expr;
        let count_udf = datafusion::functions_aggregate::count::count_udaf();
        let e = Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
            func: count_udf,
            params: datafusion_expr::expr::AggregateFunctionParams {
                args: vec![datafusion_expr::col("id")],
                distinct: true,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        let sql = expr_to_sql(&e);
        assert!(sql.contains("DISTINCT"), "should contain DISTINCT: {sql}");
    }

    #[tokio::test]
    async fn test_group_by_expression_scalar_function() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["hello"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT upper(name), SUM(value) as total FROM events GROUP BY upper(name)",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        // The pre-agg SQL should contain the expression, not a
        // quoted identifier
        assert!(
            state.pre_agg_sql.contains("upper("),
            "pre-agg SQL should contain expression: {}",
            state.pre_agg_sql
        );
        assert!(
            !state.pre_agg_sql.contains("\"upper("),
            "should NOT quote expression as identifier: {}",
            state.pre_agg_sql
        );
    }

    #[tokio::test]
    async fn test_group_by_simple_column_still_works() {
        let (_, state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        // Simple column ref should be a quoted identifier
        assert!(
            state.pre_agg_sql.contains("\"name\""),
            "simple column should be quoted: {}",
            state.pre_agg_sql
        );
    }

    #[tokio::test]
    async fn test_group_cardinality_limit_enforced() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        // Set a very small limit for testing
        state.max_groups = 3;

        // Feed 5 unique groups
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b", "c", "d", "e",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    1.0, 2.0, 3.0, 4.0, 5.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        assert_eq!(result.len(), 1);
        assert!(
            result[0].num_rows() <= 3,
            "should have at most 3 groups, got {}",
            result[0].num_rows()
        );
    }

    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn test_demote_vnode_lifecycle() {
        const VNODES: u32 = 4;
        // Changelog mode: demotion is only legal when downstream holds the
        // materialized rows.
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();
        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            true,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        // No capture yet: nothing is provably durable, demotion refuses.
        assert!(!state.demote_vnode(0, VNODES));

        let buckets = state.checkpoint_groups_by_vnode(VNODES).unwrap();
        let (&v, slice) = buckets.iter().next().unwrap();
        let demoted_groups = slice.groups.len();
        assert!(demoted_groups > 0);
        let groups_before = state.groups.len();

        // Untouched since capture → demote drops exactly that vnode's groups.
        assert!(state.demote_vnode(v, VNODES));
        assert!(state.cold_vnodes().contains(&v));
        assert_eq!(state.groups.len(), groups_before - demoted_groups);

        // The demoted vnode is absent from the next capture's buckets
        // (its cold marker is staged by the operator wrapper instead).
        let buckets2 = state.checkpoint_groups_by_vnode(VNODES).unwrap();
        assert!(!buckets2.contains_key(&v));

        // A vnode-count mismatch (rebalance changed the layout) refuses.
        let other = buckets2.keys().next().copied();
        if let Some(other) = other {
            assert!(!state.demote_vnode(other, VNODES + 1));
        }

        // Promotion path: hot again, and dirty until the next capture.
        state.mark_vnode_hot(v);
        assert!(!state.cold_vnodes().contains(&v));
        assert!(!state.demote_vnode(v, VNODES), "hot-but-dirty must refuse");
        let _ = state.checkpoint_groups_by_vnode(VNODES).unwrap();
        // Clean again after re-baselining (no groups in memory for v, but
        // the refusal must now come from emptiness, not dirtiness — demote
        // of an empty vnode is a no-op that still marks it cold).
        assert!(state.demote_vnode(v, VNODES));
    }

    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn test_demote_refused_when_dirty_or_full_emit() {
        const VNODES: u32 = 4;
        // Full-emit agg (emit_changelog = false): demotion always refuses —
        // it rebuilds its whole result from memory, dropping groups would
        // shrink the output.
        let (_, mut full) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();
        full.process_batch(&batch, i64::MIN).unwrap();
        let buckets = full.checkpoint_groups_by_vnode(VNODES).unwrap();
        let (&v, _) = buckets.iter().next().unwrap();
        assert!(
            !full.demote_vnode(v, VNODES),
            "full-emit agg must never demote"
        );

        // Changelog agg with rows since the capture: the touched vnode is
        // dirty and refuses; an untouched one (if any) still demotes.
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();
        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            true,
        )
        .await
        .unwrap()
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();
        let buckets = state.checkpoint_groups_by_vnode(VNODES).unwrap();
        let (&v, _) = buckets.iter().next().unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();
        assert!(
            !state.demote_vnode(v, VNODES),
            "vnode touched since capture must refuse demotion"
        );
    }

    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn merge_groups_does_not_block_demotion_of_other_vnodes() {
        // Regression: merge_groups used to set `dirty_all`, so a single
        // promotion or rebalance-acquired vnode blocked demotion of *every*
        // other clean vnode until the next capture. It must now leave other
        // untouched vnodes demotable (per-vnode dirtying is the caller's job).
        const VNODES: u32 = 8;
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
            .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();
        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            true,
        )
        .await
        .unwrap()
        .unwrap();

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            pre_agg_schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b", "c", "d", "e", "f", "g", "h",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        // Clean baseline spanning several vnodes.
        let buckets = state.checkpoint_groups_by_vnode(VNODES).unwrap();
        let vnodes: Vec<u32> = buckets.keys().copied().collect();
        assert!(vnodes.len() >= 2, "need groups in at least two vnodes");
        let (v_merge, v_other) = (vnodes[0], vnodes[1]);
        assert!(state.can_demote(v_merge, VNODES));
        assert!(state.can_demote(v_other, VNODES));

        // Merge a vnode's slice back (the promotion / rebalance apply path).
        let slice = buckets.get(&v_merge).unwrap();
        state.merge_groups(slice).unwrap();
        assert!(
            state.can_demote(v_other, VNODES),
            "merge_groups must not block demotion of other clean vnodes",
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn merge_groups_reemits_changed_group_on_changelog() {
        // Regression: merge_groups folds extra accumulator state into an existing
        // group, changing its value relative to last_emitted. The changelog emit
        // only visits dirty keys, so merge_groups must mark the merged group dirty
        // or the change is silently dropped downstream.
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
            .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();
        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            true,
        )
        .await
        .unwrap()
        .unwrap();

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            pre_agg_schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1.iter().map(RecordBatch::num_rows).sum::<usize>(), 1); // a: +10

        // Fold a's own slice back in (the rebalance/promotion apply path): a -> 20.
        let cp = state.checkpoint_groups().unwrap();
        state.merge_groups(&cp).unwrap();

        // a changed (10 -> 20) and must re-emit retract(old) + insert(new).
        let r2 = state.emit().unwrap();
        assert_eq!(
            r2.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2,
            "merged group must re-emit retract+insert",
        );
    }

    #[tokio::test]
    async fn test_size_estimate_throttled_cache_and_invalidate() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        assert_eq!(state.estimated_size_bytes(), 0);

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        // Mutated state still serves the previous walk's figure inside the
        // rewalk interval.
        assert_eq!(state.estimated_size_bytes(), 0);

        state.size_cache.invalidate();
        let two_groups = state.estimated_size_bytes();
        assert!(two_groups > 0, "walk after invalidate must see the groups");

        // Idle reads are stable (gen unchanged → cached, no walk).
        assert_eq!(state.estimated_size_bytes(), two_groups);

        let batch2 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["c"])),
                Arc::new(arrow::array::Float64Array::from(vec![3.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch2, i64::MIN).unwrap();
        // Cached inside the interval, fresh once forced.
        assert_eq!(state.estimated_size_bytes(), two_groups);
        state.size_cache.invalidate();
        assert!(state.estimated_size_bytes() > two_groups);
    }

    #[tokio::test]
    async fn test_size_estimate_refreshes_after_restore() {
        let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
        let (_, mut donor) = setup_agg_state(sql).await;
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();
        donor.process_batch(&batch, i64::MIN).unwrap();
        let cp = donor.checkpoint_groups().unwrap();

        let (_, mut fresh) = setup_agg_state(sql).await;
        assert_eq!(fresh.estimated_size_bytes(), 0);
        fresh.restore_groups(&cp).unwrap();
        assert!(
            fresh.estimated_size_bytes() > 0,
            "restore must invalidate the size cache so the next read re-walks"
        );
    }

    #[tokio::test]
    async fn test_group_cardinality_existing_groups_still_updated() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        state.max_groups = 2;

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));

        // Batch 1: create 2 groups (at limit)
        let batch1 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch1, i64::MIN).unwrap();

        // Batch 2: update existing groups + attempt new group "c"
        let batch2 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "c"])),
                Arc::new(arrow::array::Float64Array::from(vec![5.0, 100.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch2, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        assert_eq!(result[0].num_rows(), 2, "still only 2 groups");

        // Group "a" should have 10+5=15
        let names = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let totals = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        for i in 0..2 {
            if names.value(i) == "a" {
                assert!(
                    (totals.value(i) - 15.0).abs() < f64::EPSILON,
                    "group 'a' should be 15, got {}",
                    totals.value(i)
                );
            }
        }
    }

    #[test]
    fn test_extract_clauses_multiple_joins() {
        let c = extract_clauses(
            "SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id JOIN products p ON o.prod_id = p.id WHERE o.amount > 100 GROUP BY c.name",
        );
        assert!(
            c.from_clause.contains("orders"),
            "should contain orders: {}",
            c.from_clause
        );
        assert!(
            c.from_clause.contains("customers"),
            "should contain customers: {}",
            c.from_clause
        );
        assert!(
            c.from_clause.contains("products"),
            "should contain products: {}",
            c.from_clause
        );
        assert!(
            c.where_clause.contains("100"),
            "WHERE should contain predicate: {}",
            c.where_clause
        );
    }

    #[tokio::test]
    async fn test_agg_checkpoint_roundtrip_single_group() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        // Feed data
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        // Checkpoint
        let cp = state.checkpoint_groups().unwrap();
        assert_eq!(cp.groups.len(), 1);

        // Create a fresh state and restore
        let (_, mut state2) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        let restored = state2.restore_groups(&cp).unwrap();
        assert_eq!(restored, 1);

        // Emit and verify value matches
        let result = state2.emit().unwrap();
        assert_eq!(result.len(), 1);
        let total = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(
            (total.value(0) - 30.0).abs() < f64::EPSILON,
            "Restored SUM should be 30, got {}",
            total.value(0)
        );
    }

    #[tokio::test]
    async fn test_agg_checkpoint_roundtrip_multi_group() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b", "a", "b", "c",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 20.0, 30.0, 40.0, 50.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let cp = state.checkpoint_groups().unwrap();
        assert_eq!(cp.groups.len(), 3);

        let (_, mut state2) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        let restored = state2.restore_groups(&cp).unwrap();
        assert_eq!(restored, 3);

        let result = state2.emit().unwrap();
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_restore_fingerprint_mismatch_errors() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        // Feed data and checkpoint
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();
        let mut cp = state.checkpoint_groups().unwrap();

        // Tamper with fingerprint
        cp.fingerprint = 999_999;

        // Restore should fail
        let (_, mut state2) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        let result = state2.restore_groups(&cp);
        assert!(result.is_err(), "Fingerprint mismatch should error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("fingerprint mismatch"),
            "Error should mention fingerprint: {err}"
        );
    }

    #[tokio::test]
    async fn test_changelog_delta_emit() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, SUM(price) AS total FROM t GROUP BY symbol",
            true, // changelog mode
        )
        .await
        .unwrap()
        .unwrap();

        // Output schema should include __weight.
        assert_eq!(
            state
                .output_schema
                .field(state.output_schema.fields().len() - 1)
                .name(),
            WEIGHT_COLUMN
        );

        // Cycle 1: new data → all groups are +1 inserts.
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1.len(), 1);
        let batch1 = &r1[0];
        assert_eq!(batch1.num_rows(), 2); // AAPL +1, GOOG +1
        let w1 = batch1
            .column(batch1.num_columns() - 1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(w1.iter().all(|w| w == Some(1))); // all inserts

        // Cycle 2: AAPL changes, GOOG unchanged → -1 old AAPL, +1 new AAPL, GOOG skipped.
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
                Arc::new(arrow::array::Int64Array::from(vec![50])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        assert_eq!(r2.len(), 1);
        let batch2 = &r2[0];
        // Should be 2 rows: -1 (AAPL old), +1 (AAPL new). GOOG is unchanged → skipped.
        assert_eq!(batch2.num_rows(), 2);
        let w2 = batch2
            .column(batch2.num_columns() - 1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(w2.value(0), -1); // retraction
        assert_eq!(w2.value(1), 1); // insert

        // Cycle 3: no new data, nothing changed → empty output.
        let r3 = state.emit().unwrap();
        assert!(r3.is_empty() || r3.iter().all(|b| b.num_rows() == 0));
    }

    #[tokio::test]
    async fn changelog_restore_emits_no_duplicates_then_resumes() {
        // After recovery, restored groups are already reflected downstream (last_emitted
        // is restored in lockstep with groups), so the first post-restore emit must be
        // empty — re-emitting would duplicate. A later change must still emit normally.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let seed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![seed]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();

        let sql = "SELECT symbol, SUM(price) AS total FROM t GROUP BY symbol";
        let mut state = IncrementalAggState::try_from_sql(&ctx, sql, true)
            .await
            .unwrap()
            .unwrap();

        let pre_agg = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
        ]));
        let b1 = RecordBatch::try_new(
            Arc::clone(&pre_agg),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        assert_eq!(
            state
                .emit()
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        ); // AAPL +1, GOOG +1

        // Recover into a fresh state from the post-emit checkpoint.
        let cp = state.checkpoint_groups().unwrap();
        let mut restored = IncrementalAggState::try_from_sql(&ctx, sql, true)
            .await
            .unwrap()
            .unwrap();
        restored.restore_groups(&cp).unwrap();

        // First emit after restore: nothing new → empty (no duplicate inserts).
        let r0 = restored.emit().unwrap();
        assert!(
            r0.is_empty() || r0.iter().all(|b| b.num_rows() == 0),
            "restored groups must not be re-emitted"
        );

        // A real change resumes normally: AAPL 100 -> 150 emits retract + insert.
        let b2 = RecordBatch::try_new(
            pre_agg,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
                Arc::new(arrow::array::Int64Array::from(vec![50])),
            ],
        )
        .unwrap();
        restored.process_batch(&b2, 2000).unwrap();
        assert_eq!(
            restored
                .emit()
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2,
            "post-restore change must emit retract+insert"
        );
    }

    #[tokio::test]
    async fn test_cascaded_agg_retract_batch() {
        // Simulate a downstream aggregate consuming upstream changelog output
        // with a __weight column. Negative weights should trigger retract_batch.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("total", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, SUM(total) AS grand_total FROM upstream GROUP BY symbol",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // weight_col_idx should be detected from upstream schema.
        assert!(state.weight_col_idx.is_some());

        // Cycle 1: insert AAPL=100 (+1), GOOG=200 (+1).
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, true),
                Field::new("total", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1[0].num_rows(), 2);

        // Cycle 2: retract AAPL=100 (-1), insert AAPL=150 (+1). GOOG unchanged.
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, true),
                Field::new("total", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "AAPL"])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 150])),
                Arc::new(arrow::array::Int64Array::from(vec![-1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        // AAPL: was 100, retracted 100, added 150 → SUM=150. GOOG: still 200.
        assert_eq!(r2[0].num_rows(), 2);
        let totals = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let symbols = r2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..r2[0].num_rows() {
            match symbols.value(i) {
                "AAPL" => assert_eq!(totals.value(i), 150),
                "GOOG" => assert_eq!(totals.value(i), 200),
                other => panic!("unexpected symbol: {other}"),
            }
        }
    }

    #[tokio::test]
    async fn test_min_accepted_over_changelog_upstream() {
        // MIN is now supported over changelog streams via retractable accumulators.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, MIN(price) AS low FROM upstream GROUP BY symbol",
            false,
        )
        .await;
        assert!(result.is_ok(), "MIN should be accepted over changelog");
    }

    #[tokio::test]
    async fn test_unsupported_agg_rejected_over_changelog() {
        // STDDEV is NOT supported over changelog streams.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, STDDEV(price) AS sd FROM upstream GROUP BY symbol",
            false,
        )
        .await;
        match result {
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("Cannot compute"), "got: {msg}");
            }
            Ok(_) => panic!("expected error for STDDEV over changelog upstream"),
        }
    }

    #[tokio::test]
    async fn test_cascaded_count_star_over_changelog() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, COUNT(*) AS cnt FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(state.weight_col_idx.is_some());

        // Cycle 1: insert 3 rows.
        // Pre-agg schema for COUNT(*): [region, TRUE (dummy bool), __weight].
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Boolean, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "EU"])),
                Arc::new(arrow::array::BooleanArray::from(vec![true, true, true])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1[0].num_rows(), 2);

        // Cycle 2: retract one US row
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Boolean, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US"])),
                Arc::new(arrow::array::BooleanArray::from(vec![true])),
                Arc::new(arrow::array::Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        let counts = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let regions = r2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..r2[0].num_rows() {
            match regions.value(i) {
                "US" => assert_eq!(counts.value(i), 1, "US count should be 1 after retraction"),
                "EU" => assert_eq!(counts.value(i), 1, "EU count should remain 1"),
                other => panic!("unexpected region: {other}"),
            }
        }
    }

    #[tokio::test]
    async fn changelog_retractable_survives_checkpoint() {
        // checkpoint_groups() rebuilds each live accumulator from its snapshot.
        // For a changelog (`__weight`) aggregate the live accumulator is the
        // retractable variant; rebuilding it as a plain one would silently drop
        // retraction. Prove a retract still works *after* a mid-stream checkpoint.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, COUNT(*) AS cnt FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(state.weight_col_idx.is_some());

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Boolean, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let mk = |regions: Vec<&str>, weights: Vec<i64>| {
            let n = regions.len();
            RecordBatch::try_new(
                Arc::clone(&pre_agg_schema),
                vec![
                    Arc::new(arrow::array::StringArray::from(regions)),
                    Arc::new(arrow::array::BooleanArray::from(vec![true; n])),
                    Arc::new(arrow::array::Int64Array::from(weights)),
                ],
            )
            .unwrap()
        };

        state
            .process_batch(&mk(vec!["US", "US", "EU"], vec![1, 1, 1]), 1000)
            .unwrap();
        let _ = state.emit().unwrap();

        // Mid-stream checkpoint — must keep the live accumulators retractable.
        let _ = state.checkpoint_groups().unwrap();

        // Retract one US row; a downgraded plain accumulator could not.
        state
            .process_batch(&mk(vec!["US"], vec![-1]), 2000)
            .unwrap();
        let r = state.emit().unwrap();
        let regions = r[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let counts = r[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..r[0].num_rows() {
            match regions.value(i) {
                "US" => assert_eq!(counts.value(i), 1, "US count must be 1 after retract"),
                "EU" => assert_eq!(counts.value(i), 1),
                other => panic!("unexpected region: {other}"),
            }
        }
    }

    #[tokio::test]
    async fn test_cascaded_avg_over_changelog() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, AVG(price) AS avg_price FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // Insert: 10, 20, 30 for "US" -> avg = 20
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        let avg = r1[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((avg.value(0) - 20.0).abs() < 0.001, "avg should be 20.0");

        // Retract 10 -> {20, 30} -> avg = 25
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10])),
                Arc::new(arrow::array::Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        let avg2 = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(
            (avg2.value(0) - 25.0).abs() < 0.001,
            "avg should be 25.0 after retraction"
        );
    }

    #[tokio::test]
    async fn test_cascaded_min_over_changelog() {
        // Single MIN aggregate — pre-agg schema: [region, price, __weight]
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, MIN(price) AS lo FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // Insert 10, 20, 30
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        let mins = r1[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(mins.value(0), 10);

        // Retract current min (10) -> new min = 20
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10])),
                Arc::new(arrow::array::Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        let mins2 = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(mins2.value(0), 20, "min should be 20 after retracting 10");

        // Retract 20, retract 30 -> empty -> NULL
        let b3 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![20, 30])),
                Arc::new(arrow::array::Int64Array::from(vec![-1, -1])),
            ],
        )
        .unwrap();
        state.process_batch(&b3, 3000).unwrap();
        let r3 = state.emit().unwrap();
        assert!(
            r3[0].column(1).is_null(0),
            "min should be NULL after all values retracted"
        );
    }

    #[tokio::test]
    async fn test_cascaded_max_retract_over_changelog() {
        // Single MAX aggregate — pre-agg schema: [region, price, __weight]
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, MAX(price) AS hi FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // Insert 10, 20, 30
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        let maxs = r1[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(maxs.value(0), 30);

        // Retract current max (30) -> new max = 20
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US"])),
                Arc::new(arrow::array::Int64Array::from(vec![30])),
                Arc::new(arrow::array::Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        let maxs2 = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(maxs2.value(0), 20, "max should be 20 after retracting 30");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_cascaded_mixed_aggregates_over_changelog() {
        // Mixed: SUM + COUNT(*) + AVG + MIN + MAX on same column.
        // Pre-agg schema: [region, amount(SUM), TRUE(COUNT), amount(AVG),
        //                   amount(MIN), amount(MAX), __weight] = 7 columns.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt, \
             AVG(amount) AS avg_amt, MIN(amount) AS lo, MAX(amount) AS hi \
             FROM upstream GROUP BY region",
            false,
        )
        .await;
        assert!(result.is_ok(), "mixed aggregates should be accepted");
        let mut state = result.unwrap().unwrap();

        // Pre-agg has 7 cols: [region, amt, TRUE, amt, amt, amt, __weight].
        // Build matching batch.
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Int64, true),
                Field::new("__agg_input_2", DataType::Boolean, true),
                Field::new("__agg_input_3", DataType::Int64, true),
                Field::new("__agg_input_4", DataType::Int64, true),
                Field::new("__agg_input_5", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // SUM input
                Arc::new(arrow::array::BooleanArray::from(vec![true, true, true])), // COUNT(*)
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // AVG input
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // MIN input
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // MAX input
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),    // weight
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1[0].num_rows(), 1);

        // Retract 10, insert 40.
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Int64, true),
                Field::new("__agg_input_2", DataType::Boolean, true),
                Field::new("__agg_input_3", DataType::Int64, true),
                Field::new("__agg_input_4", DataType::Int64, true),
                Field::new("__agg_input_5", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
                Arc::new(arrow::array::BooleanArray::from(vec![true, true])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
                Arc::new(arrow::array::Int64Array::from(vec![-1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        // {20, 30, 40}: SUM=90, COUNT=3, AVG=30, MIN=20, MAX=40
        let b = &r2[0];
        let sum_col = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let cnt_col = b
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let avg_col = b
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        let min_col = b
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let max_col = b
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(sum_col.value(0), 90, "SUM should be 90");
        assert_eq!(cnt_col.value(0), 3, "COUNT should be 3");
        assert!((avg_col.value(0) - 30.0).abs() < 0.001, "AVG should be 30");
        assert_eq!(min_col.value(0), 20, "MIN should be 20");
        assert_eq!(max_col.value(0), 40, "MAX should be 40");
    }

    fn round_trip(sv: &ScalarValue) -> ScalarValue {
        let bytes = scalars_to_ipc(std::slice::from_ref(sv)).unwrap();
        let back = ipc_to_scalars(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        back.into_iter().next().unwrap()
    }

    #[test]
    fn scalar_ipc_round_trip() {
        // Arrow IPC preserves exact type — no widening, unlike the old JSON path.
        assert_eq!(round_trip(&ScalarValue::Null), ScalarValue::Null);
        assert_eq!(
            round_trip(&ScalarValue::Boolean(Some(true))),
            ScalarValue::Boolean(Some(true)),
        );
        assert_eq!(
            round_trip(&ScalarValue::Int64(Some(-42))),
            ScalarValue::Int64(Some(-42)),
        );
        assert_eq!(
            round_trip(&ScalarValue::Float64(Some(2.72))),
            ScalarValue::Float64(Some(2.72)),
        );
        assert_eq!(
            round_trip(&ScalarValue::Utf8(Some("hello".into()))),
            ScalarValue::Utf8(Some("hello".into())),
        );
        let tz: Option<Arc<str>> = Some(Arc::from("UTC"));
        assert_eq!(
            round_trip(&ScalarValue::TimestampNanosecond(
                Some(1_000_000),
                tz.clone()
            )),
            ScalarValue::TimestampNanosecond(Some(1_000_000), tz),
        );
        assert_eq!(
            round_trip(&ScalarValue::Date32(Some(19000))),
            ScalarValue::Date32(Some(19000)),
        );
        assert_eq!(
            round_trip(&ScalarValue::Date64(Some(1_700_000_000_000))),
            ScalarValue::Date64(Some(1_700_000_000_000)),
        );
    }

    #[test]
    fn binary_scalar_roundtrips_exactly() {
        // Under the old serde_json path, Binary was string-coerced via the
        // "STR" fallback. Arrow IPC preserves Binary natively.
        let sv = ScalarValue::Binary(Some(vec![1, 2, 3]));
        assert_eq!(round_trip(&sv), sv);
    }
}

/// Per-vnode checkpoint partitioning + merge-apply (the cross-node vnode
/// rehydration round-trip). Gated to cluster builds since that's where the
/// new methods compile.
#[cfg(all(test, feature = "cluster"))]
mod vnode_partition_tests {
    use super::*;

    const VNODES: u32 = 16;

    fn pre_agg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]))
    }

    async fn fresh_state() -> IncrementalAggState {
        let ctx = laminar_sql::create_session_context();
        // The seed row is for schema inference only — `try_from_sql` plans the
        // query, it does not fold table rows into the accumulators.
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("total", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let seed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["seed"])),
                Arc::new(arrow::array::Int64Array::from(vec![0])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![seed]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();
        IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, SUM(total) AS grand_total FROM upstream GROUP BY symbol",
            false,
        )
        .await
        .unwrap()
        .unwrap()
    }

    fn feed(state: &mut IncrementalAggState, rows: &[(&str, i64)]) {
        let syms: Vec<&str> = rows.iter().map(|(s, _)| *s).collect();
        let tots: Vec<i64> = rows.iter().map(|(_, t)| *t).collect();
        let n = rows.len();
        let batch = RecordBatch::try_new(
            pre_agg_schema(),
            vec![
                Arc::new(arrow::array::StringArray::from(syms)),
                Arc::new(arrow::array::Int64Array::from(tots)),
                Arc::new(arrow::array::Int64Array::from(vec![1i64; n])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, 1000).unwrap();
    }

    fn totals(state: &mut IncrementalAggState) -> std::collections::BTreeMap<String, i64> {
        let mut out = std::collections::BTreeMap::new();
        for b in state.emit().unwrap() {
            let syms = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let tots = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.insert(syms.value(i).to_string(), tots.value(i));
            }
        }
        out
    }

    #[tokio::test]
    async fn per_vnode_checkpoint_merge_round_trips() {
        let mut a = fresh_state().await;
        feed(
            &mut a,
            &[
                ("AAPL", 100),
                ("GOOG", 200),
                ("MSFT", 50),
                ("AMZN", 75),
                ("META", 25),
                ("NVDA", 10),
            ],
        );

        // Partition by vnode, and the full single-blob checkpoint as a baseline.
        let by_vnode = a.checkpoint_groups_by_vnode(VNODES).unwrap();
        let full = a.checkpoint_groups().unwrap();

        // Every group lands in exactly one vnode slice — union == the whole.
        let partitioned: usize = by_vnode.values().map(|cp| cp.groups.len()).sum();
        assert_eq!(
            partitioned,
            full.groups.len(),
            "per-vnode slices must cover every group exactly once",
        );

        // Reassemble on a fresh node by merging each vnode's slice; the
        // aggregated output must match the original.
        let mut b = fresh_state().await;
        for slice in by_vnode.values() {
            b.merge_groups(slice).unwrap();
        }
        assert_eq!(
            totals(&mut b),
            totals(&mut a),
            "merging the per-vnode slices reproduces the original aggregate",
        );
    }

    #[tokio::test]
    async fn merge_is_additive_over_already_processed_rows() {
        // Mirrors the rebalance race: the new owner processed a few rows for a
        // key before its committed state was applied. merge_groups must ADD the
        // restored partial, not replace it.
        let mut donor = fresh_state().await;
        feed(&mut donor, &[("AAPL", 100), ("GOOG", 200)]);
        let by_vnode = donor.checkpoint_groups_by_vnode(VNODES).unwrap();

        let mut acquirer = fresh_state().await;
        // Post-acquire rows land first…
        feed(&mut acquirer, &[("AAPL", 5), ("GOOG", 5)]);
        // …then the committed state is merged in.
        for slice in by_vnode.values() {
            acquirer.merge_groups(slice).unwrap();
        }

        let got = totals(&mut acquirer);
        assert_eq!(got.get("AAPL"), Some(&105), "100 restored + 5 fresh");
        assert_eq!(got.get("GOOG"), Some(&205), "200 restored + 5 fresh");
    }
}
