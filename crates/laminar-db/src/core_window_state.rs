#![deny(clippy::disallowed_types)]

//! Core window state for EOWC queries routed through core operators.
//!
//! Routes qualifying SQL EOWC queries through the core engine's canonical
//! window assigners (`TumblingWindowAssigner`, `SlidingWindowAssigner`, or
//! session-gap logic) for window assignment while using `DataFusion`
//! `Accumulator`s for aggregation. This eliminates the duplicated window
//! assignment logic in `IncrementalEowcState`.
//!
use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::AHashMap;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;

use laminar_core::operator::sliding_window::SlidingWindowAssigner;
use laminar_core::operator::window::{
    EmitStrategy as CoreEmitStrategy, TumblingWindowAssigner, WindowAssigner,
};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{WindowOperatorConfig, WindowType};

use crate::aggregate_state::{
    compile_having_filter, expr_to_sql, extract_clauses, find_aggregate, query_fingerprint,
    resolve_expr_type, AggFuncSpec, CompiledProjection, GroupCheckpoint, WindowCheckpoint,
};
use crate::eowc_state::{extract_i64_timestamps, NULL_TIMESTAMP};
use crate::error::DbError;

/// Which core window assigner variant is in use.
enum CoreWindowAssigner {
    Tumbling(TumblingWindowAssigner),
    Hopping(SlidingWindowAssigner),
    Session { gap_ms: i64 },
}

/// Pre-compiled post-aggregate projection (e.g., `SUM(a)/SUM(b) AS ratio`).
struct PostProjection {
    /// One `PhysicalExpr` per column in `final_schema`, evaluated against
    /// an intermediate batch with the `Aggregate` node's output schema.
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Output schema after projection: `[window_start, window_end, projected...]`.
    final_schema: SchemaRef,
    /// Schema matching the `Aggregate` output: `[group_cols..., agg_results...]`.
    intermediate_schema: SchemaRef,
}

/// Accumulator state for a single session window instance.
struct SessionAccState {
    start: i64,
    end: i64,
    accs: Vec<Box<dyn datafusion_expr::Accumulator>>,
}

/// Per-group session state: active sessions keyed by start timestamp.
struct SessionGroupState {
    sessions: BTreeMap<i64, SessionAccState>,
}

/// Serializable checkpoint for a single session.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SessionCheckpoint {
    pub start: i64,
    pub end: i64,
    pub acc_states: Vec<Vec<serde_json::Value>>,
}

/// Serializable checkpoint for one group's sessions.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SessionGroupCheckpoint {
    pub key: Vec<serde_json::Value>,
    pub sessions: Vec<SessionCheckpoint>,
}

/// Serializable checkpoint for a core window pipeline state.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CoreWindowCheckpoint {
    /// Query fingerprint for schema validation.
    pub fingerprint: u64,
    /// Per-window checkpoint data (reuses EOWC format) — used by
    /// tumbling and hopping assigners.
    pub windows: Vec<WindowCheckpoint>,
    /// Per-group session checkpoint data — used by session assigner.
    #[serde(default)]
    pub session_state: Vec<SessionGroupCheckpoint>,
    /// Discriminant so restore can pick the right branch.
    #[serde(default = "default_window_type_tag")]
    pub window_type: String,
}

fn default_window_type_tag() -> String {
    "tumbling".to_string()
}

/// Core window state for windowed aggregate queries.
///
/// Uses the core engine's canonical window assigners for window assignment
/// and `DataFusion` `Accumulator`s for per-group aggregation.
pub(crate) struct CoreWindowState {
    assigner: CoreWindowAssigner,
    /// Per-window aggregate state: `window_start` -> per-group accumulators.
    /// Used by tumbling and hopping assigners.
    #[allow(clippy::type_complexity)]
    windows: BTreeMap<i64, AHashMap<Vec<ScalarValue>, Vec<Box<dyn datafusion_expr::Accumulator>>>>,
    /// Per-group session state. Only populated for session windows.
    session_groups: AHashMap<Vec<ScalarValue>, SessionGroupState>,
    agg_specs: Vec<AggFuncSpec>,
    num_group_cols: usize,
    #[allow(dead_code)]
    group_col_names: Vec<String>,
    group_types: Vec<DataType>,
    pre_agg_sql: String,
    time_col_index: usize,
    output_schema: SchemaRef,
    /// Converted emit strategy from SQL `EmitClause`.
    #[allow(dead_code)]
    emit_strategy: CoreEmitStrategy,
    having_sql: Option<String>,
    compiled_projection: Option<CompiledProjection>,
    having_filter: Option<Arc<dyn PhysicalExpr>>,
    max_groups_per_window: usize,
    /// Grace period (ms) after window end before closing. Late events
    /// arriving within this window are included instead of dropped.
    allowed_lateness_ms: i64,
    post_projection: Option<PostProjection>,
}

impl CoreWindowState {
    /// Attempt to build a `CoreWindowState` by introspecting the logical
    /// plan of the given SQL query. Returns `None` if the query is not a
    /// windowed aggregate that can be routed through the core pipeline.
    #[allow(clippy::too_many_lines)]
    pub async fn try_from_sql(
        ctx: &SessionContext,
        sql: &str,
        window_config: &WindowOperatorConfig,
        emit_clause: Option<&EmitClause>,
    ) -> Result<Option<Self>, DbError> {
        let size_ms = i64::try_from(window_config.size.as_millis()).unwrap_or(i64::MAX);

        let offset_ms = window_config.offset_ms;
        let assigner = match window_config.window_type {
            WindowType::Cumulate => {
                return Err(DbError::Unsupported(
                    "CUMULATE windows are not yet supported in the streaming pipeline. \
                     Use TUMBLE or HOP instead."
                        .into(),
                ));
            }
            WindowType::Tumbling => {
                if size_ms <= 0 {
                    return Ok(None);
                }
                CoreWindowAssigner::Tumbling(
                    TumblingWindowAssigner::from_millis(size_ms).with_offset_ms(offset_ms),
                )
            }
            WindowType::Sliding => {
                let slide_ms = i64::try_from(
                    window_config
                        .slide
                        .map_or(window_config.size, |s| s)
                        .as_millis(),
                )
                .unwrap_or(i64::MAX);
                if size_ms <= 0 || slide_ms <= 0 || slide_ms > size_ms {
                    return Ok(None);
                }
                CoreWindowAssigner::Hopping(
                    SlidingWindowAssigner::from_millis(size_ms, slide_ms).with_offset_ms(offset_ms),
                )
            }
            WindowType::Session => {
                let gap_ms = i64::try_from(
                    window_config
                        .gap
                        .map_or(std::time::Duration::ZERO, |g| g)
                        .as_millis(),
                )
                .unwrap_or(0);
                if gap_ms <= 0 {
                    return Ok(None);
                }
                CoreWindowAssigner::Session { gap_ms }
            }
        };

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| DbError::Pipeline(format!("plan error: {e}")))?;

        let plan = df.logical_plan();
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

        // Determine if we should attempt to compile pre-agg expressions.
        // Use single_source_table (counts occurrences) to reject self-joins.
        let compile_source = crate::stream_executor::single_source_table(sql);
        let state_ref = ctx.state();
        let compile_props = state_ref.execution_props();
        let input_df_schema = &agg_info.input_df_schema;
        let mut compiled_exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let mut proj_fields: Vec<Field> = Vec::new();
        let mut compile_ok = compile_source.is_some();

        // Detect post-aggregate projection: top schema differs from Aggregate
        // schema when a Projection sits above the Aggregate.
        let has_projection = {
            let same = top_schema.fields().len() == agg_schema.fields().len()
                && top_schema
                    .fields()
                    .iter()
                    .zip(agg_schema.fields())
                    .all(|(t, a)| t.data_type() == a.data_type());
            !same
        };

        // Extract projection expressions + DFSchema (carries table qualifiers).
        let projection_info = if has_projection {
            fn find_projection(
                plan: &datafusion_expr::LogicalPlan,
            ) -> Option<&datafusion_expr::logical_plan::Projection> {
                match plan {
                    datafusion_expr::LogicalPlan::Projection(p) => Some(p),
                    datafusion_expr::LogicalPlan::Sort(s) => find_projection(&s.input),
                    datafusion_expr::LogicalPlan::Limit(l) => find_projection(&l.input),
                    datafusion_expr::LogicalPlan::SubqueryAlias(a) => find_projection(&a.input),
                    _ => None,
                }
            }
            match find_projection(plan) {
                Some(proj) => Some((proj.expr.as_slice(), proj.input.schema().clone())),
                None => return Ok(None), // Unknown plan shape — bail
            }
        } else {
            None
        };

        let num_group_cols = group_exprs.len();

        let mut group_col_names = Vec::new();
        let mut group_types = Vec::new();
        for i in 0..num_group_cols {
            let name_field = if has_projection {
                agg_schema.field(i)
            } else {
                top_schema.field(i)
            };
            let agg_field = agg_schema.field(i);
            group_col_names.push(name_field.name().clone());
            group_types.push(agg_field.data_type().clone());
        }

        let mut agg_specs = Vec::new();
        let mut pre_agg_select_items: Vec<String> = Vec::new();

        for (i, group_expr) in group_exprs.iter().enumerate() {
            if let datafusion_expr::Expr::Column(col) = group_expr {
                pre_agg_select_items.push(format!("\"{}\"", col.name));
            } else {
                let group_sql = expr_to_sql(group_expr);
                pre_agg_select_items.push(format!("{group_sql} AS \"__group_{i}\""));
            }

            // Compile group expression
            if compile_ok {
                match create_physical_expr(group_expr, input_df_schema, compile_props) {
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

        let mut next_col_idx = num_group_cols;

        for (i, expr) in aggr_exprs.iter().enumerate() {
            let agg_schema_idx = num_group_cols + i;
            let agg_field = agg_schema.field(agg_schema_idx);
            // When a projection is present, use the Aggregate output name
            // so the intermediate batch columns match what PhysicalExpr expects.
            let output_name = if has_projection {
                agg_field.name().clone()
            } else if agg_schema_idx < top_schema.fields().len() {
                top_schema.field(agg_schema_idx).name().clone()
            } else {
                agg_field.name().clone()
            };

            if let datafusion_expr::Expr::AggregateFunction(agg_func) = expr {
                let udf = Arc::clone(&agg_func.func);
                let is_distinct = agg_func.params.distinct;

                let mut input_col_indices = Vec::new();
                let mut input_types = Vec::new();

                if agg_func.params.args.is_empty() {
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
                            compile_props,
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
                                match create_physical_expr(
                                    &case_expr,
                                    input_df_schema,
                                    compile_props,
                                ) {
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
                                match create_physical_expr(arg_expr, input_df_schema, compile_props)
                                {
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
                        match create_physical_expr(&case_expr, input_df_schema, compile_props) {
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
                    filter_col_index,
                });
            } else {
                return Ok(None);
            }
        }

        // Add time column for window assignment
        let time_col_index = next_col_idx;
        pre_agg_select_items.push(format!("\"{}\" AS \"__cw_ts\"", window_config.time_column));

        // Compile time column expression
        if compile_ok {
            let time_expr = datafusion_expr::Expr::Column(
                datafusion_common::Column::new_unqualified(&window_config.time_column),
            );
            match create_physical_expr(&time_expr, input_df_schema, compile_props) {
                Ok(phys) => {
                    let dt = phys
                        .data_type(input_df_schema.as_arrow())
                        .unwrap_or(DataType::Int64);
                    proj_fields.push(Field::new("__cw_ts", dt, true));
                    compiled_exprs.push(phys);
                }
                Err(_) => compile_ok = false,
            }
        }

        let clauses = extract_clauses(sql);
        let pre_agg_sql = format!(
            "SELECT {} FROM {}{}",
            pre_agg_select_items.join(", "),
            clauses.from_clause,
            clauses.where_clause,
        );

        // Build compiled projection for single-source queries.
        let compiled_projection = if compile_ok {
            let source_table = compile_source.unwrap();
            // Compile WHERE predicate
            let filter = if let Some(where_pred) = &agg_info.where_predicate {
                if let Ok(phys) = create_physical_expr(where_pred, input_df_schema, compile_props) {
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

        let mut intermediate_fields: Vec<Field> = Vec::new();
        for (name, dt) in group_col_names.iter().zip(group_types.iter()) {
            intermediate_fields.push(Field::new(name, dt.clone(), true));
        }
        for spec in &agg_specs {
            intermediate_fields.push(Field::new(
                &spec.output_name,
                spec.return_type.clone(),
                true,
            ));
        }
        let intermediate_schema = Arc::new(Schema::new(intermediate_fields));

        let mut output_fields: Vec<Field> = vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
        ];
        for f in intermediate_schema.fields() {
            output_fields.push(f.as_ref().clone());
        }
        let output_schema = Arc::new(Schema::new(output_fields));

        let post_projection = if let Some((proj_exprs, agg_df_schema)) = projection_info {
            let mut compiled = Vec::with_capacity(proj_exprs.len());
            for expr in proj_exprs {
                let phys =
                    create_physical_expr(expr, &agg_df_schema, compile_props).map_err(|e| {
                        DbError::Pipeline(format!("compile post-aggregate projection: {e}"))
                    })?;
                compiled.push(phys);
            }
            let mut final_fields = vec![
                Field::new("window_start", DataType::Int64, false),
                Field::new("window_end", DataType::Int64, false),
            ];
            for f in top_schema.fields() {
                final_fields.push(f.as_ref().clone());
            }
            let final_schema = Arc::new(Schema::new(final_fields));

            Some(PostProjection {
                exprs: compiled,
                final_schema,
                intermediate_schema: Arc::clone(&intermediate_schema),
            })
        } else {
            None
        };

        // Compile HAVING filter.
        let having_filter = compile_having_filter(ctx, having_predicate.as_ref(), &output_schema);
        let having_sql = if having_filter.is_none() {
            having_predicate.as_ref().map(expr_to_sql)
        } else {
            None
        };

        // Wire the SQL→Core emit strategy bridge
        let emit_strategy = match emit_clause {
            Some(ec) => crate::stream_executor::emit_clause_to_core(ec)
                .unwrap_or(CoreEmitStrategy::OnWindowClose),
            None => CoreEmitStrategy::OnWindowClose,
        };

        Ok(Some(Self {
            assigner,
            windows: BTreeMap::new(),
            session_groups: AHashMap::new(),
            agg_specs,
            num_group_cols,
            group_col_names,
            group_types,
            pre_agg_sql,
            output_schema,
            time_col_index,
            emit_strategy,
            having_sql,
            compiled_projection,
            having_filter,
            max_groups_per_window: 1_000_000,
            allowed_lateness_ms: i64::try_from(window_config.allowed_lateness.as_millis())
                .unwrap_or(0),
            post_projection,
        }))
    }

    /// Update per-window accumulators with a new pre-aggregation batch.
    ///
    /// Vectorized batch update: extracts timestamps and group keys at the
    /// batch level using `RowConverter`, groups row indices by
    /// `(window_start, group_key)`, then calls `update_group_accumulators`
    /// once per group with a single `take()` per column.
    ///
    /// Session windows fall back to per-row processing because session
    /// merging depends on insertion order.
    #[allow(clippy::too_many_lines)]
    pub fn update_batch(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let ts_array = extract_i64_timestamps(batch, self.time_col_index)?;

        // Session windows require per-row processing (merge depends on order)
        if matches!(self.assigner, CoreWindowAssigner::Session { .. }) {
            return self.update_batch_session(batch, &ts_array);
        }

        // ── Vectorized path for tumbling/hopping ────────────────────────

        // Batch-level group key extraction via RowConverter (one allocation)
        let (converter, rows) = if self.num_group_cols > 0 {
            let group_cols: Vec<ArrayRef> = (0..self.num_group_cols)
                .map(|i| Arc::clone(batch.column(i)))
                .collect();
            let sort_fields: Vec<arrow::row::SortField> = group_cols
                .iter()
                .map(|c| arrow::row::SortField::new(c.data_type().clone()))
                .collect();
            let converter = arrow::row::RowConverter::new(sort_fields)
                .map_err(|e| DbError::Pipeline(format!("row converter: {e}")))?;
            let rows = converter
                .convert_columns(&group_cols)
                .map_err(|e| DbError::Pipeline(format!("row conversion: {e}")))?;
            (Some(converter), Some(rows))
        } else {
            (None, None)
        };

        // Group row indices by (window_start, group_key).
        // Same pattern as aggregate_state.rs: OwnedRow as HashMap key.
        let has_groups = self.num_group_cols > 0;

        // For no-group case, just group by window_start
        if !has_groups {
            let mut grouped: AHashMap<i64, Vec<u32>> = AHashMap::new();
            for (row_idx, &ts_ms) in ts_array.iter().enumerate() {
                if ts_ms == NULL_TIMESTAMP {
                    continue; // skip rows with null timestamps
                }
                #[allow(clippy::cast_possible_truncation)]
                let idx = row_idx as u32;
                match &self.assigner {
                    CoreWindowAssigner::Tumbling(a) => {
                        grouped.entry(a.assign(ts_ms).start).or_default().push(idx);
                    }
                    CoreWindowAssigner::Hopping(a) => {
                        for wid in a.assign_windows(ts_ms) {
                            grouped.entry(wid.start).or_default().push(idx);
                        }
                    }
                    CoreWindowAssigner::Session { .. } => unreachable!("handled above"),
                }
            }
            for (window_start, indices) in &grouped {
                let sv_key: Vec<ScalarValue> = Vec::new();
                let needs_insert = {
                    let wg = self.windows.entry(*window_start).or_default();
                    !wg.contains_key(&sv_key)
                };
                if needs_insert {
                    let accs = self.create_fresh_accumulators()?;
                    self.windows
                        .entry(*window_start)
                        .or_default()
                        .insert(sv_key.clone(), accs);
                }
                let Some(accs) = self
                    .windows
                    .get_mut(window_start)
                    .and_then(|g| g.get_mut(&sv_key))
                else {
                    continue;
                };
                crate::aggregate_state::IncrementalAggState::update_group_accumulators(
                    accs,
                    batch,
                    indices,
                    &self.agg_specs,
                )?;
            }
            return Ok(());
        }

        // Grouped path: OwnedRow as key (one owned() per row, ~8-32 bytes each)
        let rows_ref = rows.as_ref().expect("rows set when has_groups");
        let mut grouped: AHashMap<(i64, arrow::row::OwnedRow), Vec<u32>> = AHashMap::new();

        for (row_idx, &ts_ms) in ts_array.iter().enumerate() {
            if ts_ms == NULL_TIMESTAMP {
                continue; // skip rows with null timestamps
            }
            let row_key = rows_ref.row(row_idx).owned();
            #[allow(clippy::cast_possible_truncation)]
            let idx = row_idx as u32;
            match &self.assigner {
                CoreWindowAssigner::Tumbling(a) => {
                    grouped
                        .entry((a.assign(ts_ms).start, row_key))
                        .or_default()
                        .push(idx);
                }
                CoreWindowAssigner::Hopping(a) => {
                    for wid in a.assign_windows(ts_ms) {
                        grouped
                            .entry((wid.start, row_key.clone()))
                            .or_default()
                            .push(idx);
                    }
                }
                CoreWindowAssigner::Session { .. } => unreachable!("handled above"),
            }
        }

        let conv = converter.as_ref().expect("converter set when has_groups");
        for ((window_start, row_key), indices) in &grouped {
            let sv_key = crate::aggregate_state::row_to_scalar_key_with_types(
                conv,
                row_key,
                &self.group_types,
            )?;

            // Ensure group exists in window state (borrow-split pattern)
            let needs_insert = {
                let window_groups = self.windows.entry(*window_start).or_default();
                if window_groups.contains_key(&sv_key) {
                    false
                } else if window_groups.len() >= self.max_groups_per_window {
                    tracing::warn!(
                        max_groups = self.max_groups_per_window,
                        window_start,
                        "Core window per-window group cardinality limit reached"
                    );
                    continue;
                } else {
                    true
                }
            };
            if needs_insert {
                let accs = self.create_fresh_accumulators()?;
                self.windows
                    .entry(*window_start)
                    .or_default()
                    .insert(sv_key.clone(), accs);
            }

            let Some(accs) = self
                .windows
                .get_mut(window_start)
                .and_then(|g| g.get_mut(&sv_key))
            else {
                continue;
            };

            crate::aggregate_state::IncrementalAggState::update_group_accumulators(
                accs,
                batch,
                indices,
                &self.agg_specs,
            )?;
        }

        Ok(())
    }

    /// Per-row fallback for session windows (merge depends on insertion order).
    fn update_batch_session(
        &mut self,
        batch: &RecordBatch,
        ts_array: &[i64],
    ) -> Result<(), DbError> {
        let CoreWindowAssigner::Session { gap_ms } = self.assigner else {
            unreachable!("update_batch_session called on non-session assigner");
        };
        for (row, &ts_ms) in ts_array.iter().enumerate() {
            if ts_ms == NULL_TIMESTAMP {
                continue; // skip rows with null timestamps
            }
            let key = self.extract_group_key_scalar(batch, row)?;
            self.update_session_window(ts_ms, gap_ms, &key, batch, row)?;
        }
        Ok(())
    }

    /// Extract group key for a single row (scalar fallback for session windows).
    fn extract_group_key_scalar(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Vec<ScalarValue>, DbError> {
        let mut key = Vec::with_capacity(self.num_group_cols);
        for col_idx in 0..self.num_group_cols {
            let sv = ScalarValue::try_from_array(batch.column(col_idx), row)
                .map_err(|e| DbError::Pipeline(format!("group key extraction: {e}")))?;
            key.push(sv);
        }
        Ok(key)
    }

    /// Update accumulators for a session window, merging overlapping sessions.
    #[allow(clippy::too_many_lines)]
    fn update_session_window(
        &mut self,
        ts_ms: i64,
        gap_ms: i64,
        key: &[ScalarValue],
        batch: &RecordBatch,
        row: usize,
    ) -> Result<(), DbError> {
        let new_start = ts_ms;
        let new_end = ts_ms.saturating_add(gap_ms);

        // Check overlap count before mutating
        let overlapping: Vec<i64> = self
            .session_groups
            .get(key)
            .map(|g| {
                g.sessions
                    .range(..=new_end)
                    .filter(|(_, s)| s.end >= new_start)
                    .map(|(&k, _)| k)
                    .collect()
            })
            .unwrap_or_default();

        match overlapping.len() {
            0 => {
                let mut accs = self.create_fresh_accumulators()?;
                Self::update_accumulators(&mut accs, &self.agg_specs, batch, row)?;
                let group =
                    self.session_groups
                        .entry(key.to_vec())
                        .or_insert_with(|| SessionGroupState {
                            sessions: BTreeMap::new(),
                        });
                group.sessions.insert(
                    new_start,
                    SessionAccState {
                        start: new_start,
                        end: new_end,
                        accs,
                    },
                );
            }
            1 => {
                let group = self.session_groups.get_mut(key).unwrap();
                let sess_key = overlapping[0];
                let sess = group.sessions.get_mut(&sess_key).unwrap();
                let merged_start = sess.start.min(new_start);
                let merged_end = sess.end.max(new_end);
                sess.start = merged_start;
                sess.end = merged_end;
                Self::update_accumulators(&mut sess.accs, &self.agg_specs, batch, row)?;
                if merged_start != sess_key {
                    let sess = group.sessions.remove(&sess_key).unwrap();
                    group.sessions.insert(merged_start, sess);
                }
            }
            _ => {
                let group = self.session_groups.get_mut(key).unwrap();
                let mut merged_start = new_start;
                let mut merged_end = new_end;
                let mut survivor_accs: Option<Vec<Box<dyn datafusion_expr::Accumulator>>> = None;

                for &sess_key in &overlapping {
                    let sess = group.sessions.remove(&sess_key).unwrap();
                    merged_start = merged_start.min(sess.start);
                    merged_end = merged_end.max(sess.end);

                    if let Some(ref mut surv) = survivor_accs {
                        for (i, mut acc) in sess.accs.into_iter().enumerate() {
                            let state = acc.state().map_err(|e| {
                                DbError::Pipeline(format!("session merge state: {e}"))
                            })?;
                            let arrays: Vec<ArrayRef> = state
                                .iter()
                                .map(|sv| {
                                    sv.to_array().map_err(|e| {
                                        DbError::Pipeline(format!("session merge array: {e}"))
                                    })
                                })
                                .collect::<Result<_, _>>()?;
                            surv[i]
                                .merge_batch(&arrays)
                                .map_err(|e| DbError::Pipeline(format!("session merge: {e}")))?;
                        }
                    } else {
                        survivor_accs = Some(sess.accs);
                    }
                }

                let mut accs = survivor_accs.unwrap();
                Self::update_accumulators(&mut accs, &self.agg_specs, batch, row)?;
                group.sessions.insert(
                    merged_start,
                    SessionAccState {
                        start: merged_start,
                        end: merged_end,
                        accs,
                    },
                );
            }
        }

        Ok(())
    }

    /// Create fresh accumulators from agg specs.
    fn create_fresh_accumulators(
        &self,
    ) -> Result<Vec<Box<dyn datafusion_expr::Accumulator>>, DbError> {
        let mut accs = Vec::with_capacity(self.agg_specs.len());
        for spec in &self.agg_specs {
            accs.push(spec.create_accumulator()?);
        }
        Ok(accs)
    }

    /// Feed a single row into the given accumulators.
    fn update_accumulators(
        accs: &mut [Box<dyn datafusion_expr::Accumulator>],
        agg_specs: &[AggFuncSpec],
        batch: &RecordBatch,
        row: usize,
    ) -> Result<(), DbError> {
        let index_array = arrow::array::UInt32Array::from(vec![
            #[allow(clippy::cast_possible_truncation)]
            (row as u32),
        ]);
        for (i, spec) in agg_specs.iter().enumerate() {
            let mut input_arrays: Vec<ArrayRef> = Vec::with_capacity(spec.input_col_indices.len());
            for &col_idx in &spec.input_col_indices {
                let arr = compute::take(batch.column(col_idx), &index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("array take: {e}")))?;
                input_arrays.push(arr);
            }

            if let Some(filter_idx) = spec.filter_col_index {
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
                }
            }

            accs[i]
                .update_batch(&input_arrays)
                .map_err(|e| DbError::Pipeline(format!("accumulator update: {e}")))?;
        }
        Ok(())
    }

    /// Close windows whose end <= watermark, returning emitted batches.
    pub fn close_windows(&mut self, watermark_ms: i64) -> Result<Vec<RecordBatch>, DbError> {
        let batches = match &self.assigner {
            CoreWindowAssigner::Tumbling(a) => self.close_fixed_windows(watermark_ms, a.size_ms()),
            CoreWindowAssigner::Hopping(a) => self.close_fixed_windows(watermark_ms, a.size_ms()),
            CoreWindowAssigner::Session { .. } => self.close_session_windows(watermark_ms),
        }?;
        self.apply_post_projection(batches)
    }

    /// Close fixed-size windows (tumbling/hopping) whose end <= watermark.
    fn close_fixed_windows(
        &mut self,
        watermark_ms: i64,
        size_ms: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        // Fast check: if the earliest window hasn't expired, nothing to close.
        if let Some((&first_ws, _)) = self.windows.first_key_value() {
            if first_ws
                .saturating_add(size_ms)
                .saturating_add(self.allowed_lateness_ms)
                > watermark_ms
            {
                return Ok(Vec::new());
            }
        } else {
            return Ok(Vec::new());
        }

        let to_close: Vec<i64> = self
            .windows
            .keys()
            .copied()
            .take_while(|&ws| {
                ws.saturating_add(size_ms)
                    .saturating_add(self.allowed_lateness_ms)
                    <= watermark_ms
            })
            .collect();

        let mut result_batches = Vec::new();

        for window_start in to_close {
            let Some(groups) = self.windows.remove(&window_start) else {
                continue;
            };
            if groups.is_empty() {
                continue;
            }
            let window_end = window_start.saturating_add(size_ms);
            if let Some(b) = self.emit_window(window_start, window_end, groups)? {
                result_batches.push(b);
            }
        }

        Ok(result_batches)
    }

    /// Close session windows whose end <= watermark.
    fn close_session_windows(&mut self, watermark_ms: i64) -> Result<Vec<RecordBatch>, DbError> {
        // Fast check: skip allocation if no sessions are closeable.
        let any_closeable = self.session_groups.values().any(|g| {
            g.sessions
                .values()
                .any(|s| s.end.saturating_add(self.allowed_lateness_ms) <= watermark_ms)
        });
        if !any_closeable {
            return Ok(Vec::new());
        }

        #[allow(clippy::type_complexity)]
        let mut rows: Vec<(
            i64,
            i64,
            Vec<ScalarValue>,
            Vec<Box<dyn datafusion_expr::Accumulator>>,
        )> = Vec::new();

        let mut empty_groups = Vec::new();

        for (key, group) in &mut self.session_groups {
            let to_close: Vec<i64> = group
                .sessions
                .iter()
                .filter(|(_, s)| s.end.saturating_add(self.allowed_lateness_ms) <= watermark_ms)
                .map(|(&k, _)| k)
                .collect();

            for sess_key in to_close {
                let sess = group.sessions.remove(&sess_key).unwrap();
                rows.push((sess.start, sess.end, key.clone(), sess.accs));
            }

            if group.sessions.is_empty() {
                empty_groups.push(key.clone());
            }
        }

        for key in empty_groups {
            self.session_groups.remove(&key);
        }

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // Sort by (window_start, window_end) for deterministic output
        rows.sort_by_key(|(ws, we, _, _)| (*ws, *we));

        self.emit_session_rows(rows)
    }

    /// Build a `RecordBatch` from closed session rows (variable start/end).
    #[allow(clippy::type_complexity)]
    fn emit_session_rows(
        &self,
        rows: Vec<(
            i64,
            i64,
            Vec<ScalarValue>,
            Vec<Box<dyn datafusion_expr::Accumulator>>,
        )>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let num_rows = rows.len();

        let mut starts = Vec::with_capacity(num_rows);
        let mut ends = Vec::with_capacity(num_rows);
        let mut group_scalars: Vec<Vec<ScalarValue>> = (0..self.num_group_cols)
            .map(|_| Vec::with_capacity(num_rows))
            .collect();
        let mut agg_scalars: Vec<Vec<ScalarValue>> = (0..self.agg_specs.len())
            .map(|_| Vec::with_capacity(num_rows))
            .collect();

        for (ws, we, key, mut accs) in rows {
            starts.push(ws);
            ends.push(we);
            for (i, sv) in key.into_iter().enumerate() {
                group_scalars[i].push(sv);
            }
            for (i, acc) in accs.iter_mut().enumerate() {
                let sv = acc
                    .evaluate()
                    .map_err(|e| DbError::Pipeline(format!("session accumulator evaluate: {e}")))?;
                agg_scalars[i].push(sv);
            }
        }

        let win_start_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(starts));
        let win_end_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(ends));

        let mut group_arrays: Vec<ArrayRef> = Vec::with_capacity(self.num_group_cols);
        for (col_idx, scalars) in group_scalars.into_iter().enumerate() {
            let array = ScalarValue::iter_to_array(scalars)
                .map_err(|e| DbError::Pipeline(format!("group key array: {e}")))?;
            let dt = &self.group_types[col_idx];
            if array.data_type() == dt {
                group_arrays.push(array);
            } else {
                let casted = arrow::compute::cast(&array, dt).unwrap_or(array);
                group_arrays.push(casted);
            }
        }

        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(self.agg_specs.len());
        for (agg_idx, scalars) in agg_scalars.into_iter().enumerate() {
            let array = ScalarValue::iter_to_array(scalars)
                .map_err(|e| DbError::Pipeline(format!("agg result array: {e}")))?;
            let dt = &self.agg_specs[agg_idx].return_type;
            if array.data_type() == dt {
                agg_arrays.push(array);
            } else {
                let casted = arrow::compute::cast(&array, dt).unwrap_or(array);
                agg_arrays.push(casted);
            }
        }

        let mut all_arrays = vec![win_start_array, win_end_array];
        all_arrays.extend(group_arrays);
        all_arrays.extend(agg_arrays);

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), all_arrays)
            .map_err(|e| DbError::Pipeline(format!("session result batch: {e}")))?;

        Ok(vec![batch])
    }

    /// Emit a single window's accumulated state as a `RecordBatch`.
    fn emit_window(
        &self,
        window_start: i64,
        window_end: i64,
        groups: AHashMap<Vec<ScalarValue>, Vec<Box<dyn datafusion_expr::Accumulator>>>,
    ) -> Result<Option<RecordBatch>, DbError> {
        crate::aggregate_state::emit_window_batch(
            window_start,
            window_end,
            groups,
            &self.group_types,
            &self.agg_specs,
            &self.output_schema,
        )
    }

    /// Apply compiled post-aggregate projection to emitted batches.
    fn apply_post_projection(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let Some(proj) = &self.post_projection else {
            return Ok(batches);
        };

        let mut result = Vec::with_capacity(batches.len());
        for batch in &batches {
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            let win_start = Arc::clone(batch.column(0));
            let win_end = Arc::clone(batch.column(1));

            let content_cols: Vec<ArrayRef> = (2..batch.num_columns())
                .map(|i| Arc::clone(batch.column(i)))
                .collect();
            let intermediate =
                RecordBatch::try_new(Arc::clone(&proj.intermediate_schema), content_cols).map_err(
                    |e| DbError::Pipeline(format!("post-projection intermediate batch: {e}")),
                )?;

            let mut projected_cols = Vec::with_capacity(2 + proj.exprs.len());
            projected_cols.push(win_start);
            projected_cols.push(win_end);
            for phys_expr in &proj.exprs {
                let col_val = phys_expr
                    .evaluate(&intermediate)
                    .map_err(|e| DbError::Pipeline(format!("post-projection evaluate: {e}")))?;
                let array = col_val
                    .into_array(num_rows)
                    .map_err(|e| DbError::Pipeline(format!("post-projection into_array: {e}")))?;
                projected_cols.push(array);
            }

            let projected_batch =
                RecordBatch::try_new(Arc::clone(&proj.final_schema), projected_cols)
                    .map_err(|e| DbError::Pipeline(format!("post-projection result batch: {e}")))?;
            result.push(projected_batch);
        }
        Ok(result)
    }

    /// Output schema for windowed aggregate results.
    ///
    /// Returns the post-projection schema when a post-aggregate projection
    /// is present, otherwise the intermediate aggregate schema.
    pub(crate) fn output_schema(&self) -> Arc<arrow::datatypes::Schema> {
        if let Some(proj) = &self.post_projection {
            Arc::clone(&proj.final_schema)
        } else {
            Arc::clone(&self.output_schema)
        }
    }

    /// Pre-aggregation SQL.
    pub fn pre_agg_sql(&self) -> &str {
        &self.pre_agg_sql
    }

    /// HAVING predicate SQL, if any.
    pub fn having_sql(&self) -> Option<&str> {
        self.having_sql.as_deref()
    }

    /// Compiled HAVING filter, if available.
    pub fn having_filter(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.having_filter.as_ref()
    }

    /// Compiled pre-aggregation projection, if available.
    pub fn compiled_projection(&self) -> Option<&CompiledProjection> {
        self.compiled_projection.as_ref()
    }

    /// Compute a fingerprint for this query (SQL + schema).
    pub(crate) fn query_fingerprint(&self) -> u64 {
        query_fingerprint(&self.pre_agg_sql, &self.output_schema)
    }

    /// Returns a tag string for the current assigner type.
    fn window_type_tag(&self) -> &'static str {
        match &self.assigner {
            CoreWindowAssigner::Tumbling(_) => "tumbling",
            CoreWindowAssigner::Hopping(_) => "hopping",
            CoreWindowAssigner::Session { .. } => "session",
        }
    }

    /// Checkpoint all per-window group states into a serializable struct.
    pub(crate) fn checkpoint_windows(&mut self) -> Result<CoreWindowCheckpoint, DbError> {
        use crate::aggregate_state::scalar_to_json;

        let fingerprint = self.query_fingerprint();
        let window_type = self.window_type_tag().to_string();

        match &self.assigner {
            CoreWindowAssigner::Tumbling(_) | CoreWindowAssigner::Hopping(_) => {
                let mut windows = Vec::with_capacity(self.windows.len());
                for (&window_start, groups) in &mut self.windows {
                    let mut group_checkpoints = Vec::with_capacity(groups.len());
                    for (key, accs) in groups {
                        let key_json: Vec<serde_json::Value> =
                            key.iter().map(scalar_to_json).collect();
                        let mut acc_states = Vec::with_capacity(accs.len());
                        for acc in accs {
                            let state = acc.state().map_err(|e| {
                                DbError::Pipeline(format!("accumulator state: {e}"))
                            })?;
                            acc_states.push(state.iter().map(scalar_to_json).collect());
                        }
                        group_checkpoints.push(GroupCheckpoint {
                            key: key_json,
                            acc_states,
                        });
                    }
                    windows.push(WindowCheckpoint {
                        window_start,
                        groups: group_checkpoints,
                    });
                }
                Ok(CoreWindowCheckpoint {
                    fingerprint,
                    windows,
                    session_state: Vec::new(),
                    window_type,
                })
            }
            CoreWindowAssigner::Session { .. } => {
                let mut session_state = Vec::with_capacity(self.session_groups.len());
                for (key, group) in &mut self.session_groups {
                    let key_json: Vec<serde_json::Value> = key.iter().map(scalar_to_json).collect();
                    let mut sessions = Vec::with_capacity(group.sessions.len());
                    for sess in group.sessions.values_mut() {
                        let mut acc_states = Vec::with_capacity(sess.accs.len());
                        for acc in &mut sess.accs {
                            let state = acc.state().map_err(|e| {
                                DbError::Pipeline(format!("session accumulator state: {e}"))
                            })?;
                            acc_states.push(state.iter().map(scalar_to_json).collect());
                        }
                        sessions.push(SessionCheckpoint {
                            start: sess.start,
                            end: sess.end,
                            acc_states,
                        });
                    }
                    session_state.push(SessionGroupCheckpoint {
                        key: key_json,
                        sessions,
                    });
                }
                Ok(CoreWindowCheckpoint {
                    fingerprint,
                    windows: Vec::new(),
                    session_state,
                    window_type,
                })
            }
        }
    }

    /// Restore per-window group states from a checkpoint.
    pub(crate) fn restore_windows(
        &mut self,
        checkpoint: &CoreWindowCheckpoint,
    ) -> Result<usize, DbError> {
        let current_fp = self.query_fingerprint();
        if checkpoint.fingerprint != current_fp {
            return Err(DbError::Pipeline(format!(
                "Core window checkpoint fingerprint mismatch: saved={}, current={}",
                checkpoint.fingerprint, current_fp
            )));
        }

        if checkpoint.window_type == "session" {
            self.restore_session_windows(checkpoint)
        } else {
            self.restore_fixed_windows(checkpoint)
        }
    }

    /// Restore fixed (tumbling/hopping) windows from checkpoint.
    fn restore_fixed_windows(
        &mut self,
        checkpoint: &CoreWindowCheckpoint,
    ) -> Result<usize, DbError> {
        use crate::aggregate_state::json_to_scalar;

        self.windows.clear();
        let mut total_groups = 0usize;
        for wc in &checkpoint.windows {
            let mut groups = AHashMap::new();
            for gc in &wc.groups {
                let key: Result<Vec<ScalarValue>, _> = gc.key.iter().map(json_to_scalar).collect();
                let key = key?;
                let mut accs = Vec::with_capacity(self.agg_specs.len());
                for (i, spec) in self.agg_specs.iter().enumerate() {
                    let mut acc = spec.create_accumulator()?;
                    if i < gc.acc_states.len() {
                        let state_scalars: Result<Vec<ScalarValue>, _> =
                            gc.acc_states[i].iter().map(json_to_scalar).collect();
                        let state_scalars = state_scalars?;
                        let arrays: Vec<arrow::array::ArrayRef> = state_scalars
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
                groups.insert(key, accs);
                total_groups += 1;
            }
            self.windows.insert(wc.window_start, groups);
        }
        Ok(total_groups)
    }

    /// Restore session windows from checkpoint.
    fn restore_session_windows(
        &mut self,
        checkpoint: &CoreWindowCheckpoint,
    ) -> Result<usize, DbError> {
        use crate::aggregate_state::json_to_scalar;

        self.session_groups.clear();
        let mut total_sessions = 0usize;
        for sgc in &checkpoint.session_state {
            let key: Result<Vec<ScalarValue>, _> = sgc.key.iter().map(json_to_scalar).collect();
            let key = key?;
            let mut sessions = BTreeMap::new();
            for sc in &sgc.sessions {
                let mut accs = Vec::with_capacity(self.agg_specs.len());
                for (i, spec) in self.agg_specs.iter().enumerate() {
                    let mut acc = spec.create_accumulator()?;
                    if i < sc.acc_states.len() {
                        let state_scalars: Result<Vec<ScalarValue>, _> =
                            sc.acc_states[i].iter().map(json_to_scalar).collect();
                        let state_scalars = state_scalars?;
                        let arrays: Vec<arrow::array::ArrayRef> = state_scalars
                            .iter()
                            .map(|sv| {
                                sv.to_array()
                                    .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))
                            })
                            .collect::<Result<_, _>>()?;
                        acc.merge_batch(&arrays).map_err(|e| {
                            DbError::Pipeline(format!("session accumulator merge: {e}"))
                        })?;
                    }
                    accs.push(acc);
                }
                sessions.insert(
                    sc.start,
                    SessionAccState {
                        start: sc.start,
                        end: sc.end,
                        accs,
                    },
                );
                total_sessions += 1;
            }
            self.session_groups
                .insert(key, SessionGroupState { sessions });
        }
        Ok(total_sessions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use datafusion::execution::FunctionRegistry;
    use laminar_core::operator::window::EmitStrategy as CoreEmit;

    /// Build a pre-agg batch with 1 group col (Utf8), 1 agg input (Int64),
    /// and 1 timestamp col (Int64).
    fn make_pre_agg_batch(
        groups: Vec<&str>,
        values: Vec<i64>,
        timestamps: Vec<i64>,
    ) -> RecordBatch {
        assert_eq!(groups.len(), values.len());
        assert_eq!(groups.len(), timestamps.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("__agg_input_1", DataType::Int64, false),
            Field::new("__cw_ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    groups.into_iter().map(String::from).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(values)),
                Arc::new(Int64Array::from(timestamps)),
            ],
        )
        .unwrap()
    }

    /// Build a `CoreWindowState` for SUM(Int64) with 1-second tumbling
    /// windows and a single Utf8 group-by column.
    fn make_core_window_state(size_ms: i64) -> CoreWindowState {
        let ctx = SessionContext::new();
        let udf = ctx.udaf("sum").expect("SUM should be registered");

        let agg_specs = vec![AggFuncSpec {
            udf,
            input_types: vec![DataType::Int64],
            input_col_indices: vec![1],
            output_name: "total".to_string(),
            return_type: DataType::Int64,
            distinct: false,
            filter_col_index: None,
        }];

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
        ]));

        CoreWindowState {
            assigner: CoreWindowAssigner::Tumbling(TumblingWindowAssigner::from_millis(size_ms)),
            windows: BTreeMap::new(),
            session_groups: AHashMap::new(),
            agg_specs,
            num_group_cols: 1,
            group_col_names: vec!["symbol".to_string()],
            group_types: vec![DataType::Utf8],
            pre_agg_sql: String::new(),
            output_schema,
            time_col_index: 2,
            emit_strategy: CoreEmit::OnWindowClose,
            having_sql: None,
            compiled_projection: None,
            having_filter: None,
            max_groups_per_window: 1_000_000,
            allowed_lateness_ms: 0,
            post_projection: None,
        }
    }

    /// Build a multi-aggregate core window state: SUM + COUNT.
    fn make_core_window_state_multi_agg(size_ms: i64) -> CoreWindowState {
        let ctx = SessionContext::new();
        let sum_udf = ctx.udaf("sum").expect("SUM");
        let count_udf = ctx.udaf("count").expect("COUNT");

        let agg_specs = vec![
            AggFuncSpec {
                udf: sum_udf,
                input_types: vec![DataType::Int64],
                input_col_indices: vec![1],
                output_name: "total".to_string(),
                return_type: DataType::Int64,
                distinct: false,
                filter_col_index: None,
            },
            AggFuncSpec {
                udf: count_udf,
                input_types: vec![DataType::Int64],
                input_col_indices: vec![1],
                output_name: "cnt".to_string(),
                return_type: DataType::Int64,
                distinct: false,
                filter_col_index: None,
            },
        ];

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, true),
        ]));

        CoreWindowState {
            assigner: CoreWindowAssigner::Tumbling(TumblingWindowAssigner::from_millis(size_ms)),
            windows: BTreeMap::new(),
            session_groups: AHashMap::new(),
            agg_specs,
            num_group_cols: 1,
            group_col_names: vec!["symbol".to_string()],
            group_types: vec![DataType::Utf8],
            pre_agg_sql: String::new(),
            output_schema,
            time_col_index: 2,
            emit_strategy: CoreEmit::OnWindowClose,
            having_sql: None,
            compiled_projection: None,
            having_filter: None,
            max_groups_per_window: 1_000_000,
            allowed_lateness_ms: 0,
            post_projection: None,
        }
    }

    /// Build a hopping (sliding) `CoreWindowState` for SUM(Int64).
    fn make_hopping_core_window_state(size_ms: i64, slide_ms: i64) -> CoreWindowState {
        let ctx = SessionContext::new();
        let udf = ctx.udaf("sum").expect("SUM should be registered");

        let agg_specs = vec![AggFuncSpec {
            udf,
            input_types: vec![DataType::Int64],
            input_col_indices: vec![1],
            output_name: "total".to_string(),
            return_type: DataType::Int64,
            distinct: false,
            filter_col_index: None,
        }];

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
        ]));

        CoreWindowState {
            assigner: CoreWindowAssigner::Hopping(SlidingWindowAssigner::from_millis(
                size_ms, slide_ms,
            )),
            windows: BTreeMap::new(),
            session_groups: AHashMap::new(),
            agg_specs,
            num_group_cols: 1,
            group_col_names: vec!["symbol".to_string()],
            group_types: vec![DataType::Utf8],
            pre_agg_sql: String::new(),
            output_schema,
            time_col_index: 2,
            emit_strategy: CoreEmit::OnWindowClose,
            having_sql: None,
            compiled_projection: None,
            having_filter: None,
            max_groups_per_window: 1_000_000,
            allowed_lateness_ms: 0,
            post_projection: None,
        }
    }

    /// Build a session `CoreWindowState` for SUM(Int64).
    fn make_session_core_window_state(gap_ms: i64) -> CoreWindowState {
        let ctx = SessionContext::new();
        let udf = ctx.udaf("sum").expect("SUM should be registered");

        let agg_specs = vec![AggFuncSpec {
            udf,
            input_types: vec![DataType::Int64],
            input_col_indices: vec![1],
            output_name: "total".to_string(),
            return_type: DataType::Int64,
            distinct: false,
            filter_col_index: None,
        }];

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
        ]));

        CoreWindowState {
            assigner: CoreWindowAssigner::Session { gap_ms },
            windows: BTreeMap::new(),
            session_groups: AHashMap::new(),
            agg_specs,
            num_group_cols: 1,
            group_col_names: vec!["symbol".to_string()],
            group_types: vec![DataType::Utf8],
            pre_agg_sql: String::new(),
            output_schema,
            time_col_index: 2,
            emit_strategy: CoreEmit::OnWindowClose,
            having_sql: None,
            compiled_projection: None,
            having_filter: None,
            max_groups_per_window: 1_000_000,
            allowed_lateness_ms: 0,
            post_projection: None,
        }
    }

    // ── Detection tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_detect_tumbling_aggregate_returns_core_window() {
        use laminar_sql::{create_session_context, register_streaming_functions};
        use std::time::Duration;

        let ctx = create_session_context();
        register_streaming_functions(&ctx);

        // Register a source table
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("trades", Arc::new(mem)).unwrap();

        let window_config = WindowOperatorConfig {
            window_type: WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: Duration::from_secs(60),
            slide: None,
            gap: None,
            offset_ms: 0,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        let sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";
        let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(result.is_some(), "Tumbling aggregate should return Some");
    }

    #[tokio::test]
    async fn test_detect_sliding_invalid_params_returns_none() {
        use laminar_sql::{create_session_context, register_streaming_functions};
        use std::time::Duration;

        let ctx = create_session_context();
        register_streaming_functions(&ctx);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        // Sliding with slide > size should return None
        let window_config = WindowOperatorConfig {
            window_type: WindowType::Sliding,
            time_column: "ts".to_string(),
            size: Duration::from_secs(10),
            slide: Some(Duration::from_secs(60)),
            gap: None,
            offset_ms: 0,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        let sql = "SELECT id, SUM(val) AS total FROM events GROUP BY id";
        let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "Sliding with slide > size should return None"
        );
    }

    #[tokio::test]
    async fn test_detect_projection_only_returns_none() {
        use laminar_sql::{create_session_context, register_streaming_functions};
        use std::time::Duration;

        let ctx = create_session_context();
        register_streaming_functions(&ctx);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        let window_config = WindowOperatorConfig {
            window_type: WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: Duration::from_secs(60),
            slide: None,
            gap: None,
            offset_ms: 0,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        // No aggregate → should return None
        let sql = "SELECT id, val FROM events";
        let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(result.is_none(), "Projection-only should return None");
    }

    // ── Pipeline tests ──────────────────────────────────────────────

    #[test]
    fn test_core_window_tumbling_sum() {
        let mut state = make_core_window_state(1000);

        // Two events in window [0, 1000)
        let batch1 = make_pre_agg_batch(vec!["AAPL", "AAPL"], vec![10, 20], vec![100, 500]);
        state.update_batch(&batch1).unwrap();

        // One more event in same window
        let batch2 = make_pre_agg_batch(vec!["AAPL"], vec![30], vec![800]);
        state.update_batch(&batch2).unwrap();

        // Close window at watermark 1000
        let batches = state.close_windows(1000).unwrap();
        assert_eq!(batches.len(), 1);

        let result = &batches[0];
        assert_eq!(result.num_rows(), 1);

        // Check window_start = 0
        let ws = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ws.value(0), 0);

        // Check window_end = 1000
        let we = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(we.value(0), 1000);

        // Check SUM = 10 + 20 + 30 = 60
        let total = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(total.value(0), 60);
    }

    #[test]
    fn test_core_window_tumbling_multi_aggregate_multi_group() {
        let mut state = make_core_window_state_multi_agg(1000);

        // Window [0, 1000): AAPL=10,20  GOOG=100  MSFT=50
        let batch1 = make_pre_agg_batch(
            vec!["AAPL", "GOOG", "AAPL", "MSFT"],
            vec![10, 100, 20, 50],
            vec![100, 200, 300, 400],
        );
        state.update_batch(&batch1).unwrap();

        // Window [1000, 2000): AAPL=5  GOOG=200,300
        let batch2 = make_pre_agg_batch(
            vec!["AAPL", "GOOG", "GOOG"],
            vec![5, 200, 300],
            vec![1100, 1200, 1500],
        );
        state.update_batch(&batch2).unwrap();

        // Close first window
        let batches = state.close_windows(1000).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3); // AAPL, GOOG, MSFT

        // Close second window
        let batches = state.close_windows(2000).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2); // AAPL, GOOG
    }

    #[test]
    fn test_core_window_close_windows_watermark() {
        let mut state = make_core_window_state(1000);

        // Events in three windows
        let batch = make_pre_agg_batch(vec!["A", "A", "A"], vec![1, 2, 3], vec![100, 1100, 2100]);
        state.update_batch(&batch).unwrap();

        // Watermark at 1500 → only window [0, 1000) closes
        let batches = state.close_windows(1500).unwrap();
        assert_eq!(batches.len(), 1);
        let ws = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ws.value(0), 0);

        // Watermark at 2000 → window [1000, 2000) closes
        let batches = state.close_windows(2000).unwrap();
        assert_eq!(batches.len(), 1);
        let ws = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ws.value(0), 1000);

        // Watermark at 2500 → nothing to close (window [2000,3000) still open)
        let batches = state.close_windows(2500).unwrap();
        assert!(batches.is_empty());
    }

    // ── Emit clause bridge test ─────────────────────────────────────

    #[test]
    fn test_emit_clause_to_core_all_variants() {
        use crate::stream_executor::{emit_clause_to_core, sql_emit_to_core};
        use laminar_sql::parser::EmitStrategy as SqlEmit;

        assert_eq!(
            sql_emit_to_core(&SqlEmit::OnWatermark),
            CoreEmit::OnWatermark
        );
        assert_eq!(
            sql_emit_to_core(&SqlEmit::OnWindowClose),
            CoreEmit::OnWindowClose
        );
        assert_eq!(sql_emit_to_core(&SqlEmit::OnUpdate), CoreEmit::OnUpdate);
        assert_eq!(sql_emit_to_core(&SqlEmit::Changelog), CoreEmit::Changelog);
        assert_eq!(sql_emit_to_core(&SqlEmit::FinalOnly), CoreEmit::Final);

        // EmitClause → Core via bridge
        assert_eq!(
            emit_clause_to_core(&EmitClause::AfterWatermark).unwrap(),
            CoreEmit::OnWatermark
        );
        assert_eq!(
            emit_clause_to_core(&EmitClause::OnWindowClose).unwrap(),
            CoreEmit::OnWindowClose
        );
        assert_eq!(
            emit_clause_to_core(&EmitClause::Final).unwrap(),
            CoreEmit::Final
        );
    }

    // ── Checkpoint/Restore tests ────────────────────────────────────

    #[test]
    fn test_core_window_checkpoint_roundtrip() {
        let mut state = make_core_window_state(1000);

        // Feed data into two windows
        let batch = make_pre_agg_batch(
            vec!["AAPL", "AAPL", "GOOG"],
            vec![10, 20, 100],
            vec![100, 200, 1500],
        );
        state.update_batch(&batch).unwrap();

        // Checkpoint
        let cp = state.checkpoint_windows().unwrap();
        assert_eq!(cp.windows.len(), 2);

        // Create fresh state and restore
        let mut state2 = make_core_window_state(1000);
        let restored = state2.restore_windows(&cp).unwrap();
        assert!(restored > 0, "Should have restored groups");

        // Close first window and verify SUM
        let batches = state2.close_windows(1000).unwrap();
        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 1); // Only AAPL in window [0,1000)

        let total = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(total.value(0), 30, "SUM should be 10+20=30");

        // Close second window
        let batches = state2.close_windows(2000).unwrap();
        assert_eq!(batches.len(), 1);
        let total2 = batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(total2.value(0), 100, "SUM should be 100");
    }

    #[test]
    fn test_core_window_checkpoint_fingerprint_mismatch() {
        let mut state = make_core_window_state(1000);

        let batch = make_pre_agg_batch(vec!["AAPL"], vec![10], vec![100]);
        state.update_batch(&batch).unwrap();

        let mut cp = state.checkpoint_windows().unwrap();
        cp.fingerprint = 12345; // tamper

        let mut state2 = make_core_window_state(1000);
        let result = state2.restore_windows(&cp);
        assert!(result.is_err(), "Should fail on fingerprint mismatch");
    }

    // ── New detection tests (hopping/session) ──────────────────────

    #[tokio::test]
    async fn test_detect_sliding_aggregate_returns_core_window() {
        use laminar_sql::{create_session_context, register_streaming_functions};
        use std::time::Duration;

        let ctx = create_session_context();
        register_streaming_functions(&ctx);

        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("trades", Arc::new(mem)).unwrap();

        let window_config = WindowOperatorConfig {
            window_type: WindowType::Sliding,
            time_column: "ts".to_string(),
            size: Duration::from_secs(60),
            slide: Some(Duration::from_secs(10)),
            gap: None,
            offset_ms: 0,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        let sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";
        let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(result.is_some(), "Sliding aggregate should return Some");
    }

    #[tokio::test]
    async fn test_detect_session_aggregate_returns_core_window() {
        use laminar_sql::{create_session_context, register_streaming_functions};
        use std::time::Duration;

        let ctx = create_session_context();
        register_streaming_functions(&ctx);

        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("trades", Arc::new(mem)).unwrap();

        let window_config = WindowOperatorConfig {
            window_type: WindowType::Session,
            time_column: "ts".to_string(),
            size: Duration::ZERO,
            slide: None,
            gap: Some(Duration::from_secs(30)),
            offset_ms: 0,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        let sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";
        let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(result.is_some(), "Session aggregate should return Some");
    }

    #[tokio::test]
    async fn test_detect_session_zero_gap_returns_none() {
        use laminar_sql::{create_session_context, register_streaming_functions};
        use std::time::Duration;

        let ctx = create_session_context();
        register_streaming_functions(&ctx);

        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("trades", Arc::new(mem)).unwrap();

        let window_config = WindowOperatorConfig {
            window_type: WindowType::Session,
            time_column: "ts".to_string(),
            size: Duration::ZERO,
            slide: None,
            gap: Some(Duration::ZERO),
            offset_ms: 0,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        let sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";
        let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(result.is_none(), "Session with gap=0 should return None");
    }

    // ── Hopping pipeline tests ─────────────────────────────────────

    #[test]
    fn test_hopping_basic_sum() {
        // 4s window, 2s slide → each event in 2 windows
        let mut state = make_hopping_core_window_state(4000, 2000);

        // ts=1000 → windows [-2000,2000) and [0,4000)
        // ts=3000 → windows [0,4000) and [2000,6000)
        let batch = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 3000]);
        state.update_batch(&batch).unwrap();

        // Close everything up to watermark 6000
        let batches = state.close_windows(6000).unwrap();

        // Collect all (window_start, sum) pairs
        let mut results: Vec<(i64, i64)> = Vec::new();
        for b in &batches {
            let ws = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let totals = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                results.push((ws.value(i), totals.value(i)));
            }
        }
        results.sort_unstable();

        // window [-2000, 2000): only ts=1000 → SUM=10
        // window [0, 4000): ts=1000 + ts=3000 → SUM=30
        // window [2000, 6000): only ts=3000 → SUM=20
        assert_eq!(results, vec![(-2000, 10), (0, 30), (2000, 20)]);
    }

    #[test]
    fn test_hopping_multi_group() {
        let mut state = make_hopping_core_window_state(4000, 2000);

        let batch = make_pre_agg_batch(
            vec!["A", "B", "A"],
            vec![10, 100, 20],
            vec![1000, 1000, 3000],
        );
        state.update_batch(&batch).unwrap();

        let batches = state.close_windows(6000).unwrap();

        let mut results: Vec<(i64, String, i64)> = Vec::new();
        for b in &batches {
            let ws = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let syms = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            let totals = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                results.push((ws.value(i), syms.value(i).to_string(), totals.value(i)));
            }
        }
        results.sort();

        // window [-2000, 2000): A=10, B=100
        // window [0, 4000): A=10+20=30, B=100
        // window [2000, 6000): A=20
        assert!(results.contains(&(-2000, "A".to_string(), 10)));
        assert!(results.contains(&(-2000, "B".to_string(), 100)));
        assert!(results.contains(&(0, "A".to_string(), 30)));
        assert!(results.contains(&(0, "B".to_string(), 100)));
        assert!(results.contains(&(2000, "A".to_string(), 20)));
    }

    #[test]
    fn test_hopping_watermark_ordering() {
        let mut state = make_hopping_core_window_state(4000, 2000);

        let batch = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 3000]);
        state.update_batch(&batch).unwrap();

        // Watermark at 2000 → only window [-2000, 2000) closes
        let batches = state.close_windows(2000).unwrap();
        assert_eq!(batches.len(), 1);
        let ws = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ws.value(0), -2000);

        // Watermark at 4000 → window [0, 4000) closes
        let batches = state.close_windows(4000).unwrap();
        assert_eq!(batches.len(), 1);
        let ws = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ws.value(0), 0);

        // Watermark at 5000 → nothing (window [2000,6000) still open)
        let batches = state.close_windows(5000).unwrap();
        assert!(batches.is_empty());
    }

    // ── Session pipeline tests ─────────────────────────────────────

    #[test]
    fn test_session_basic_sum() {
        // gap=5000ms — events within 5s merge into one session
        let mut state = make_session_core_window_state(5000);

        // Three events within gap for group A
        let batch = make_pre_agg_batch(
            vec!["A", "A", "A"],
            vec![10, 20, 30],
            vec![1000, 3000, 4000],
        );
        state.update_batch(&batch).unwrap();

        // Session: start=1000, end=4000+5000=9000
        // Watermark at 9000 → session closes
        let batches = state.close_windows(9000).unwrap();
        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 1);

        let ws = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ws.value(0), 1000);

        let we = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(we.value(0), 9000);

        let total = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(total.value(0), 60);
    }

    #[test]
    fn test_session_two_sessions() {
        // gap=2000ms
        let mut state = make_session_core_window_state(2000);

        // Two clusters: [1000] and [5000, 6000]
        let batch = make_pre_agg_batch(
            vec!["A", "A", "A"],
            vec![10, 20, 30],
            vec![1000, 5000, 6000],
        );
        state.update_batch(&batch).unwrap();

        // Session 1: [1000, 3000), Session 2: [5000, 8000)
        // Watermark at 8000 → both close
        let batches = state.close_windows(8000).unwrap();
        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 2);

        let ws = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let totals = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Sorted by window_start
        assert_eq!(ws.value(0), 1000);
        assert_eq!(totals.value(0), 10);
        assert_eq!(ws.value(1), 5000);
        assert_eq!(totals.value(1), 50);
    }

    #[test]
    fn test_session_merge() {
        // gap=3000ms
        let mut state = make_session_core_window_state(3000);

        // First two events create separate sessions
        let batch1 = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 5000]);
        state.update_batch(&batch1).unwrap();
        // Session 1: [1000, 4000), Session 2: [5000, 8000)

        // Late event at ts=3500 bridges the two sessions
        let batch2 = make_pre_agg_batch(vec!["A"], vec![30], vec![3500]);
        state.update_batch(&batch2).unwrap();
        // Merged: [1000, 8000) with SUM = 10+20+30 = 60

        let batches = state.close_windows(8000).unwrap();
        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 1);

        let ws = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ws.value(0), 1000);

        let we = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(we.value(0), 8000);

        let total = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(total.value(0), 60);
    }

    #[test]
    fn test_session_multi_group_independent() {
        let mut state = make_session_core_window_state(3000);

        let batch = make_pre_agg_batch(
            vec!["A", "B", "A", "B"],
            vec![10, 100, 20, 200],
            vec![1000, 2000, 2000, 3000],
        );
        state.update_batch(&batch).unwrap();

        // A: session [1000, 5000) SUM=30
        // B: session [2000, 6000) SUM=300
        let batches = state.close_windows(6000).unwrap();
        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 2);

        let ws = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let syms = result
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut results: Vec<(String, i64, i64)> = (0..result.num_rows())
            .map(|i| (syms.value(i).to_string(), ws.value(i), totals.value(i)))
            .collect();
        results.sort();

        assert_eq!(results[0], ("A".to_string(), 1000, 30));
        assert_eq!(results[1], ("B".to_string(), 2000, 300));
    }

    // ── Hopping checkpoint tests ───────────────────────────────────

    #[test]
    fn test_hopping_checkpoint_roundtrip() {
        let mut state = make_hopping_core_window_state(4000, 2000);

        let batch = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 3000]);
        state.update_batch(&batch).unwrap();

        let cp = state.checkpoint_windows().unwrap();
        assert_eq!(cp.window_type, "hopping");
        assert!(!cp.windows.is_empty());

        let mut state2 = make_hopping_core_window_state(4000, 2000);
        let restored = state2.restore_windows(&cp).unwrap();
        assert!(restored > 0);

        let batches = state2.close_windows(6000).unwrap();
        let mut results: Vec<(i64, i64)> = Vec::new();
        for b in &batches {
            let ws = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let totals = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                results.push((ws.value(i), totals.value(i)));
            }
        }
        results.sort_unstable();
        assert_eq!(results, vec![(-2000, 10), (0, 30), (2000, 20)]);
    }

    #[test]
    fn test_session_checkpoint_roundtrip() {
        let mut state = make_session_core_window_state(3000);

        // Create two sessions then merge them
        let batch1 = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 5000]);
        state.update_batch(&batch1).unwrap();

        let batch2 = make_pre_agg_batch(vec!["A"], vec![30], vec![3500]);
        state.update_batch(&batch2).unwrap();

        let cp = state.checkpoint_windows().unwrap();
        assert_eq!(cp.window_type, "session");
        assert!(!cp.session_state.is_empty());

        let mut state2 = make_session_core_window_state(3000);
        let restored = state2.restore_windows(&cp).unwrap();
        assert!(restored > 0);

        let batches = state2.close_windows(8000).unwrap();
        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 1);

        let total = result
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            total.value(0),
            60,
            "SUM should be 10+20+30=60 after restore"
        );
    }

    // ── Post-aggregate projection tests ──────────────────────────────

    #[tokio::test]
    async fn test_post_aggregate_projection_detection() {
        use arrow::datatypes::Field;

        let ctx = laminar_sql::create_streaming_context_with_validator(
            laminar_sql::StreamingValidatorMode::Off,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
                Arc::new(arrow::array::Float64Array::from(vec![2.0])),
                Arc::new(Int64Array::from(vec![1000])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let config = laminar_sql::translator::WindowOperatorConfig {
            window_type: laminar_sql::translator::WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: std::time::Duration::from_secs(10),
            slide: None,
            gap: None,
            offset_ms: 0,
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        // SUM(a)/SUM(b) is a post-aggregate projection — should now be accepted.
        let result = CoreWindowState::try_from_sql(
            &ctx,
            "SELECT symbol, SUM(a) / SUM(b) AS ratio \
             FROM events GROUP BY symbol, \
             TUMBLE(ts, INTERVAL '10' SECOND)",
            &config,
            Some(&laminar_sql::parser::EmitClause::OnWindowClose),
        )
        .await
        .unwrap();
        assert!(
            result.is_some(),
            "Post-aggregate projection should now be accepted"
        );
        let state = result.unwrap();
        assert!(
            state.post_projection.is_some(),
            "PostProjection should be compiled"
        );
    }

    #[tokio::test]
    async fn test_tumbling_ratio_projection_pipeline() {
        use arrow::datatypes::Field;

        let ctx = laminar_sql::create_streaming_context_with_validator(
            laminar_sql::StreamingValidatorMode::Off,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
                Arc::new(arrow::array::Float64Array::from(vec![2.0])),
                Arc::new(Int64Array::from(vec![1000])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let config = laminar_sql::translator::WindowOperatorConfig {
            window_type: laminar_sql::translator::WindowType::Tumbling,
            time_column: "ts".to_string(),
            size: std::time::Duration::from_secs(10),
            slide: None,
            gap: None,
            offset_ms: 0,
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        let mut state = CoreWindowState::try_from_sql(
            &ctx,
            "SELECT symbol, SUM(a) / SUM(b) AS ratio \
             FROM events GROUP BY symbol, \
             TUMBLE(ts, INTERVAL '10' SECOND)",
            &config,
            Some(&laminar_sql::parser::EmitClause::OnWindowClose),
        )
        .await
        .unwrap()
        .expect("should detect as core window");

        // Execute the pre-agg SQL to get correctly shaped input batches.
        let pre_agg_sql = state.pre_agg_sql().to_string();
        let pre_agg_df = ctx.sql(&pre_agg_sql).await.unwrap();
        let pre_agg_batches = pre_agg_df.collect().await.unwrap();
        for batch in &pre_agg_batches {
            state.update_batch(batch).unwrap();
        }

        // Close the window (watermark past window end = 10_000).
        let batches = state.close_windows(11_000).unwrap();
        assert_eq!(batches.len(), 1, "should emit one batch");
        let out = &batches[0];

        // The projection SELECT has 2 items (symbol, ratio), so the
        // final schema is [window_start, window_end, symbol, ratio].
        assert_eq!(out.num_columns(), 4, "schema: {:?}", out.schema());
        assert_eq!(out.num_rows(), 1);

        // MemTable has 1 row: a=1.0, b=2.0.
        // ratio = SUM(a) / SUM(b) = 1.0 / 2.0 = 0.5
        // Output columns: [window_start, window_end, symbol, ratio]
        // (group_1 = TUMBLE is consumed by window assignment, projection
        //  selects only symbol + ratio)
        let ratio_col = out
            .column(out.num_columns() - 1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("ratio should be Float64");
        let ratio = ratio_col.value(0);
        assert!(
            (ratio - 0.5).abs() < 1e-9,
            "expected ratio=0.5, got {ratio}"
        );
    }

    #[tokio::test]
    async fn test_session_with_projection() {
        use arrow::datatypes::Field;

        let ctx = laminar_sql::create_streaming_context_with_validator(
            laminar_sql::StreamingValidatorMode::Off,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(arrow::array::Float64Array::from(vec![100.0])),
                Arc::new(Int64Array::from(vec![1000])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let config = laminar_sql::translator::WindowOperatorConfig {
            window_type: laminar_sql::translator::WindowType::Session,
            time_column: "ts".to_string(),
            size: std::time::Duration::from_secs(5),
            slide: None,
            gap: Some(std::time::Duration::from_secs(5)),
            offset_ms: 0,
            allowed_lateness: std::time::Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        // Session window + derived column: SUM(amount) * 2 AS double_total
        let result = CoreWindowState::try_from_sql(
            &ctx,
            "SELECT user_id, SUM(amount) * 2 AS double_total \
             FROM events GROUP BY user_id, \
             SESSION(ts, INTERVAL '5' SECOND)",
            &config,
            Some(&laminar_sql::parser::EmitClause::OnWindowClose),
        )
        .await
        .unwrap();
        assert!(result.is_some(), "Session + projection should be accepted");
        let state = result.unwrap();
        assert!(state.post_projection.is_some());
    }

    #[test]
    fn test_apply_post_projection_passthrough() {
        // Without post_projection, batches pass through unchanged.
        let state = make_core_window_state(1000);
        let batch = make_pre_agg_batch(vec!["A"], vec![10], vec![100]);
        let result = state.apply_post_projection(vec![batch.clone()]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), batch.num_rows());
    }
}
