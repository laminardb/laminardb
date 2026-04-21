//! Incremental aggregation state for streaming GROUP BY queries.
//!
//! ## Scope
//!
//! This module owns the **single-process** aggregate operator: one
//! `IncrementalAggState` per pipeline, one `DataFusion` `Accumulator`
//! per aggregate per group. State is captured for checkpointing via
//! the DataFusion-canonical `Accumulator::state() -> Vec<ScalarValue>`
//! / `merge_batch(&arrays)` pair (see `checkpoint_groups` /
//! `restore_groups`), serialized through Arrow IPC.
//!
//! The cross-vnode fan-in path — where the same aggregate runs on many
//! vnodes and a merger instance combines their partials — lives in
//! [`laminar_core::state::partial_aggregate`] and is a distinct tool
//! solving a distinct problem (monoid `merge`, not format round-trip).
//! Do not try to unify the two contracts: `DataFusion` already covers
//! DISTINCT / FILTER / HAVING / UDAs on the single-process path, which
//! `PartialAggregate` deliberately does not attempt.

use std::sync::Arc;

use ahash::AHashMap;
use rustc_hash::FxHashMap;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDF, LogicalPlan};

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
    resolve_expr_type, CompiledProjection,
};
pub(crate) use keys::{
    global_aggregate_key, row_to_scalar_key_with_types, scalar_key_to_owned_row,
};
pub(crate) use scalar_ipc::{ipc_to_scalars, scalars_to_ipc};

pub(crate) fn emit_window_batch(
    window_start: i64,
    window_end: i64,
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

    let win_start_array: ArrayRef =
        Arc::new(arrow::array::Int64Array::from(vec![window_start; num_rows]));
    let win_end_array: ArrayRef =
        Arc::new(arrow::array::Int64Array::from(vec![window_end; num_rows]));

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

    let mut all_arrays = vec![win_start_array, win_end_array];
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

    /// Create a weight-aware retractable accumulator for changelog streams.
    ///
    /// These accumulators receive the `__weight` column as the last input
    /// in `update_batch` and handle retraction internally.
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

pub(crate) struct IncrementalAggState {
    pre_agg_sql: String,
    num_group_cols: usize,
    group_types: Vec<DataType>,
    agg_specs: Vec<AggFuncSpec>,
    groups: AHashMap<arrow::row::OwnedRow, GroupEntry>,
    row_converter: arrow::row::RowConverter,
    output_schema: SchemaRef,
    compiled_projection: Option<CompiledProjection>,
    cached_pre_agg_plan: Option<LogicalPlan>,
    having_filter: Option<Arc<dyn PhysicalExpr>>,
    having_sql: Option<String>,
    max_groups: usize,
    emit_changelog: bool,
    last_emitted: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
    pub(crate) idle_ttl_ms: Option<u64>,
    weight_col_idx: Option<usize>,
}

impl IncrementalAggState {
    /// Number of leading GROUP BY columns in this aggregate's pre-agg
    /// output. Used by the cluster row-shuffle path to know which
    /// columns to hash.
    #[cfg(feature = "cluster-unstable")]
    #[must_use]
    pub(crate) fn num_group_cols(&self) -> usize {
        self.num_group_cols
    }
}

/// Name of the Z-set weight column appended to changelog output.
pub(crate) const WEIGHT_COLUMN: &str = "__weight";

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

    let weight_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(weights.to_vec()));

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

        // Use the top-level plan's output schema for column names — this
        // includes user-specified aliases (e.g., `SUM(x) AS total`).
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

        // Bail out if the top-level plan has a non-trivial projection above
        // the Aggregate (e.g., SUM(a)/SUM(b) AS ratio, CASE WHEN ... END).
        // The incremental accumulator emits raw aggregate outputs and cannot
        // apply post-aggregate projections.  Fall through to EOWC raw-batch.
        //
        // Check both field count AND types — a coincidental count match (e.g.,
        // 2 group keys + 3 aggregates == 5 SELECT items after projection drops
        // a group key and adds a computed column) can mask a projection that
        // remaps group columns to aggregate aliases.
        if top_schema.fields().len() != agg_schema.fields().len() {
            return Ok(None);
        }
        for (top_f, agg_f) in top_schema.fields().iter().zip(agg_schema.fields()) {
            if top_f.data_type() != agg_f.data_type() {
                return Ok(None);
            }
        }

        let num_group_cols = group_exprs.len();

        // Resolve group-by column names and types.
        // Use top-level schema for names (preserves aliases) and agg schema
        // for types (accurate data types from the aggregate node).
        let mut group_col_names = Vec::new();
        let mut group_types = Vec::new();
        for i in 0..num_group_cols {
            let top_field = top_schema.field(i);
            let agg_field = agg_schema.field(i);
            group_col_names.push(top_field.name().clone());
            group_types.push(agg_field.data_type().clone());
        }

        // Determine if we should attempt to compile pre-agg expressions.
        // Use single_source_table (counts occurrences) to reject self-joins.
        let compile_source = crate::sql_analysis::single_source_table(sql);
        let state = ctx.state();
        let props = state.execution_props();
        let input_df_schema = &agg_info.input_df_schema;
        let mut compiled_exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let mut proj_fields: Vec<Field> = Vec::new();
        let mut compile_ok = compile_source.is_some();

        let mut agg_specs = Vec::new();
        let mut pre_agg_select_items: Vec<String> = Vec::new();

        // For simple column refs, quote as identifier. For complex
        // expressions (EXTRACT, CASE WHEN, etc.), generate the SQL
        // expression with an alias so the source query evaluates it.
        for (i, group_expr) in group_exprs.iter().enumerate() {
            if let datafusion_expr::Expr::Column(col) = group_expr {
                pre_agg_select_items.push(format!("\"{}\"", col.name));
            } else {
                let group_sql = expr_to_sql(group_expr);
                pre_agg_select_items.push(format!("{group_sql} AS \"__group_{i}\""));
            }

            // Compile group expression
            if compile_ok {
                match create_physical_expr(group_expr, input_df_schema, props) {
                    Ok(phys) => {
                        let dt = phys
                            .data_type(input_df_schema.as_arrow())
                            .unwrap_or(DataType::Utf8);
                        let name = match group_expr {
                            datafusion_expr::Expr::Column(col) => col.name.clone(),
                            _ => format!("__group_{i}"),
                        };
                        proj_fields.push(Field::new(name, dt, true));
                        compiled_exprs.push(phys);
                    }
                    Err(_) => compile_ok = false,
                }
            }
        }

        // Column index tracker for pre-agg output
        let mut next_col_idx = num_group_cols;

        for (i, expr) in aggr_exprs.iter().enumerate() {
            let agg_schema_idx = num_group_cols + i;
            let agg_field = agg_schema.field(agg_schema_idx);
            // Use the top-level schema for the output name (has user alias)
            let output_name = if agg_schema_idx < top_schema.fields().len() {
                top_schema.field(agg_schema_idx).name().clone()
            } else {
                agg_field.name().clone()
            };

            if let datafusion_expr::Expr::AggregateFunction(agg_func) = expr {
                let udf = Arc::clone(&agg_func.func);
                let is_distinct = agg_func.params.distinct;

                // Collect input column indices and add input expressions
                // to the pre-agg SELECT
                let mut input_col_indices = Vec::new();
                let mut input_types = Vec::new();

                if agg_func.params.args.is_empty() {
                    // COUNT(*) — no input columns needed, pass a dummy boolean
                    let col_idx = next_col_idx;
                    next_col_idx += 1;
                    pre_agg_select_items.push(format!("TRUE AS \"__agg_input_{col_idx}\""));
                    input_col_indices.push(col_idx);
                    input_types.push(DataType::Boolean);

                    // Compile literal TRUE
                    if compile_ok {
                        match create_physical_expr(
                            &datafusion_expr::lit(true),
                            input_df_schema,
                            props,
                        ) {
                            Ok(phys) => {
                                proj_fields.push(Field::new(
                                    format!("__agg_input_{col_idx}"),
                                    DataType::Boolean,
                                    true,
                                ));
                                compiled_exprs.push(phys);
                            }
                            Err(_) => compile_ok = false,
                        }
                    }
                } else {
                    for arg_expr in &agg_func.params.args {
                        let col_idx = next_col_idx;
                        next_col_idx += 1;
                        let expr_sql = expr_to_sql(arg_expr);

                        // If FILTER clause present, wrap input with
                        // CASE WHEN so filtered rows become NULL (ignored
                        // by accumulators)
                        if let Some(filter_expr) = &agg_func.params.filter {
                            let filter_sql = expr_to_sql(filter_expr);
                            pre_agg_select_items.push(format!(
                                "CASE WHEN {filter_sql} THEN {expr_sql} ELSE NULL END AS \"__agg_input_{col_idx}\""
                            ));

                            // Compile: CASE WHEN filter THEN arg ELSE NULL END
                            if compile_ok {
                                let case_expr =
                                    datafusion_expr::Expr::Case(datafusion_expr::expr::Case {
                                        expr: None,
                                        when_then_expr: vec![(
                                            Box::new(filter_expr.as_ref().clone()),
                                            Box::new(arg_expr.clone()),
                                        )],
                                        else_expr: Some(Box::new(datafusion_expr::lit(
                                            ScalarValue::Null,
                                        ))),
                                    });
                                match create_physical_expr(&case_expr, input_df_schema, props) {
                                    Ok(phys) => {
                                        let dt = resolve_expr_type(
                                            arg_expr,
                                            &input_schema,
                                            agg_field.data_type(),
                                        );
                                        proj_fields.push(Field::new(
                                            format!("__agg_input_{col_idx}"),
                                            dt,
                                            true,
                                        ));
                                        compiled_exprs.push(phys);
                                    }
                                    Err(_) => compile_ok = false,
                                }
                            }
                        } else {
                            pre_agg_select_items
                                .push(format!("{expr_sql} AS \"__agg_input_{col_idx}\""));

                            // Compile the arg expression directly
                            if compile_ok {
                                match create_physical_expr(arg_expr, input_df_schema, props) {
                                    Ok(phys) => {
                                        let dt = resolve_expr_type(
                                            arg_expr,
                                            &input_schema,
                                            agg_field.data_type(),
                                        );
                                        proj_fields.push(Field::new(
                                            format!("__agg_input_{col_idx}"),
                                            dt,
                                            true,
                                        ));
                                        compiled_exprs.push(phys);
                                    }
                                    Err(_) => compile_ok = false,
                                }
                            }
                        }

                        input_col_indices.push(col_idx);
                        let dt = resolve_expr_type(arg_expr, &input_schema, agg_field.data_type());
                        input_types.push(dt);
                    }
                }

                // Add a boolean filter column for masking rows
                // in process_batch when FILTER clause is present
                let filter_col_index = if let Some(filter_expr) = &agg_func.params.filter {
                    let col_idx = next_col_idx;
                    next_col_idx += 1;
                    let filter_sql = expr_to_sql(filter_expr);
                    pre_agg_select_items.push(format!(
                            "CASE WHEN {filter_sql} THEN TRUE ELSE FALSE END AS \"__agg_filter_{col_idx}\""
                        ));

                    // Compile: CASE WHEN filter THEN TRUE ELSE FALSE END
                    if compile_ok {
                        let case_expr = datafusion_expr::Expr::Case(datafusion_expr::expr::Case {
                            expr: None,
                            when_then_expr: vec![(
                                Box::new(filter_expr.as_ref().clone()),
                                Box::new(datafusion_expr::lit(true)),
                            )],
                            else_expr: Some(Box::new(datafusion_expr::lit(false))),
                        });
                        match create_physical_expr(&case_expr, input_df_schema, props) {
                            Ok(phys) => {
                                proj_fields.push(Field::new(
                                    format!("__agg_filter_{col_idx}"),
                                    DataType::Boolean,
                                    true,
                                ));
                                compiled_exprs.push(phys);
                            }
                            Err(_) => compile_ok = false,
                        }
                    }

                    Some(col_idx)
                } else {
                    None
                };

                let return_type = udf
                    .return_type(&input_types)
                    .unwrap_or_else(|_| agg_field.data_type().clone());

                agg_specs.push(AggFuncSpec {
                    udf,
                    input_types,
                    input_col_indices,
                    output_name,
                    return_type,
                    distinct: is_distinct,
                    is_count_star: agg_func.params.args.is_empty(),
                    filter_col_index,
                });
            } else {
                // Non-aggregate expression in the aggregate output —
                // this shouldn't happen for well-formed plans.
                return Ok(None);
            }
        }

        let clauses = extract_clauses(sql);

        // Detect __weight in upstream source for cascaded changelog aggregation.
        // Check the registered table schema (not the pruned plan schema, which
        // may have dropped unreferenced columns).
        let source_has_weight = if let Ok(tp) = ctx
            .table_provider(clauses.from_clause.trim_matches('"'))
            .await
        {
            tp.schema().column_with_name(WEIGHT_COLUMN).is_some()
        } else {
            false
        };

        let weight_col_idx = if source_has_weight {
            // Validate that all aggregates support retractable changelog
            // semantics. Retractable accumulators handle the weight column
            // internally (SUM, COUNT, AVG, MIN, MAX).
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

        // Build compiled projection for single-source queries.
        // Skip compilation when upstream has __weight — the compiled path
        // doesn't include the weight column, so process_batch would read
        // the wrong index. The cached plan path handles it via pre_agg_sql.
        let compiled_projection = if compile_ok && weight_col_idx.is_none() {
            let source_table = compile_source.unwrap();
            // Compile WHERE predicate
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
                    source_table,
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

        // Compile HAVING filter.
        let having_filter = compile_having_filter(ctx, having_predicate.as_ref(), &output_schema);
        let having_sql = if having_filter.is_none() {
            having_predicate.as_ref().map(expr_to_sql)
        } else {
            None
        };

        // ONE-TIME setup: cache the optimized logical plan for multi-source
        // pre-agg queries (when compiled projection is not available). This
        // ctx.sql() call runs ONLY at first-cycle initialization, never
        // per-cycle. Subsequent cycles use the cached plan directly.
        // Fail fast if the pre-agg SQL is invalid — it would fail every cycle.
        let cached_pre_agg_plan = if compiled_projection.is_none() {
            match ctx.sql(&pre_agg_sql).await {
                Ok(df) => Some(df.logical_plan().clone()),
                Err(e) => {
                    return Err(DbError::Pipeline(format!(
                        "pre-agg SQL planning failed for aggregate: {e}"
                    )));
                }
            }
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
            row_converter,
            output_schema,
            compiled_projection,
            cached_pre_agg_plan,
            having_filter,
            having_sql,
            max_groups: 1_000_000,
            emit_changelog,
            last_emitted: AHashMap::new(),
            idle_ttl_ms: None,
            weight_col_idx,
        }))
    }

    /// Evict idle groups and return retraction records. Only produces output
    /// when both `emit_changelog` and `idle_ttl_ms` are set.
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

        // Collect retraction data from last_emitted before removing.
        let mut retract_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut retract_vals: Vec<Vec<ScalarValue>> = Vec::new();

        for key in &idle_keys {
            if let Some(old) = self.last_emitted.remove(key) {
                retract_keys.push(key.clone());
                retract_vals.push(old);
            }
            self.groups.remove(key);
        }

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

        // Group row indices by key for batch accumulator updates.
        let estimated_groups = (batch.num_rows() / 4).max(16);
        let mut group_indices: FxHashMap<arrow::row::OwnedRow, Vec<u32>> =
            FxHashMap::with_capacity_and_hasher(estimated_groups, rustc_hash::FxBuildHasher);
        for row_idx in 0..batch.num_rows() {
            #[allow(clippy::cast_possible_truncation)]
            group_indices
                .entry(rows.row(row_idx).owned())
                .or_default()
                .push(row_idx as u32);
        }

        for (row_key, indices) in &group_indices {
            if !self.groups.contains_key(row_key) {
                if self.groups.len() >= self.max_groups {
                    tracing::warn!(
                        max_groups = self.max_groups,
                        current_groups = self.groups.len(),
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
                self.groups.insert(
                    row_key.clone(),
                    GroupEntry {
                        accs,
                        last_updated_ms: watermark_ms,
                    },
                );
            }
            let Some(entry) = self.groups.get_mut(row_key) else {
                continue;
            };
            Self::update_group_accumulators(
                &mut entry.accs,
                batch,
                indices,
                &self.agg_specs,
                self.weight_col_idx,
            )?;
            entry.last_updated_ms = watermark_ms;
        }

        Ok(())
    }

    /// Fast path for global aggregates (COUNT(*), SUM(x), etc. with no GROUP BY).
    fn process_batch_no_groups(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> Result<(), DbError> {
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
        Self::update_group_accumulators(
            &mut entry.accs,
            batch,
            &all_indices,
            &self.agg_specs,
            self.weight_col_idx,
        )
    }

    /// Update accumulators for a single group given the row indices.
    ///
    /// Takes a batch and a slice of row indices, does a single `take()` per
    /// column per accumulator, and feeds the resulting sub-arrays to
    /// `Accumulator::update_batch`. This avoids per-row allocation.
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

            // Apply per-accumulator FILTER mask.
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
                    // Also filter weight array through the same mask.
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

            // Retractable accumulators receive weight as the last input.
            if let Some(w) = &filtered_weight {
                input_arrays.push(Arc::clone(w));
            }

            accs[i]
                .update_batch(&input_arrays)
                .map_err(|e| DbError::Pipeline(format!("accumulator update: {e}")))?;
        }
        Ok(())
    }

    /// Does NOT reset the accumulators — they continue accumulating for the
    /// next cycle (running totals).
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.emit_changelog {
            return self.emit_changelog_delta();
        }
        self.emit_running_state()
    }

    /// Emit full running state (all groups, every cycle). No __weight column.
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
        // Collect retractions and inserts.
        let mut retract_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut retract_vals: Vec<Vec<ScalarValue>> = Vec::new();
        let mut insert_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut insert_vals: Vec<Vec<ScalarValue>> = Vec::new();

        for (key, entry) in &mut self.groups {
            let current: Vec<ScalarValue> = entry
                .accs
                .iter_mut()
                .map(|a| a.evaluate())
                .collect::<Result<_, _>>()
                .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;

            if let Some(old) = self.last_emitted.get(key) {
                if old != &current {
                    retract_keys.push(key.clone());
                    retract_vals.push(old.clone());
                    insert_keys.push(key.clone());
                    insert_vals.push(current.clone());
                    self.last_emitted.insert(key.clone(), current);
                }
                // Unchanged: skip (true delta semantics).
            } else {
                insert_keys.push(key.clone());
                insert_vals.push(current.clone());
                self.last_emitted.insert(key.clone(), current);
            }
        }

        // Detect deleted groups (in last_emitted but not in groups).
        let deleted: Vec<arrow::row::OwnedRow> = self
            .last_emitted
            .keys()
            .filter(|k| !self.groups.contains_key(*k))
            .cloned()
            .collect();
        for key in &deleted {
            let old = self.last_emitted.remove(key).unwrap();
            retract_keys.push(key.clone());
            retract_vals.push(old);
        }

        let total = retract_keys.len() + insert_keys.len();
        if total == 0 {
            return Ok(Vec::new());
        }

        // Build batch: retracts first (-1), then inserts (+1).
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

    pub fn cached_pre_agg_plan(&self) -> Option<&LogicalPlan> {
        self.cached_pre_agg_plan.as_ref()
    }

    pub(crate) fn query_fingerprint(&self) -> u64 {
        query_fingerprint(&self.pre_agg_sql, &self.output_schema)
    }

    pub(crate) fn estimated_size_bytes(&self) -> usize {
        let mut total = 0;
        for (key, entry) in &self.groups {
            total += key.as_ref().len();
            for acc in &entry.accs {
                total += acc.size();
            }
        }
        total
    }

    pub(crate) fn checkpoint_groups(&mut self) -> Result<AggStateCheckpoint, DbError> {
        let fingerprint = self.query_fingerprint();
        let mut groups = Vec::with_capacity(self.groups.len());
        for (row_key, entry) in &mut self.groups {
            let sv_key =
                row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
            let key_ipc = scalars_to_ipc(&sv_key)?;
            let mut acc_states = Vec::with_capacity(entry.accs.len());
            for acc in &mut entry.accs {
                let state = acc
                    .state()
                    .map_err(|e| DbError::Pipeline(format!("accumulator state: {e}")))?;
                acc_states.push(scalars_to_ipc(&state)?);
            }
            groups.push(GroupCheckpoint {
                key: key_ipc,
                acc_states,
                last_updated_ms: entry.last_updated_ms,
            });
        }
        // Serialize last_emitted for changelog recovery.
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
        self.groups.clear();
        for gc in &checkpoint.groups {
            // Decode IPC bytes → ScalarValue → Array → OwnedRow.
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
            self.groups.insert(
                row_key,
                GroupEntry {
                    accs,
                    last_updated_ms: gc.last_updated_ms,
                },
            );
        }

        // Restore last_emitted for changelog recovery.
        self.last_emitted.clear();
        for ec in &checkpoint.last_emitted {
            let sv_key = ipc_to_scalars(&ec.key)?;
            let row_key = scalar_key_to_owned_row(&self.row_converter, &sv_key, &self.group_types)?;
            let vals = ipc_to_scalars(&ec.values)?;
            self.last_emitted.insert(row_key, vals);
        }

        Ok(checkpoint.groups.len())
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
