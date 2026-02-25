//! Ring 0 pipeline state for EOWC queries routed through core operators.
//!
//! Routes qualifying SQL EOWC queries through Ring 0's canonical
//! `TumblingWindowAssigner` for window assignment while using `DataFusion`
//! `Accumulator`s for aggregation. This eliminates the duplicated window
//! assignment logic in `IncrementalEowcState`.
//!
//! Phase 1 scope: Tumbling windows only.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;

use laminar_core::operator::window::{EmitStrategy as CoreEmitStrategy, TumblingWindowAssigner};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{WindowOperatorConfig, WindowType};

use crate::aggregate_state::{
    extract_clauses, expr_to_sql, find_aggregate, query_fingerprint, resolve_expr_type,
    AggFuncSpec, GroupCheckpoint, WindowCheckpoint,
};
use crate::error::DbError;
use crate::eowc_state::extract_i64_timestamps;

/// Serializable checkpoint for a Ring 0 pipeline state.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Ring0StateCheckpoint {
    /// Query fingerprint for schema validation.
    pub fingerprint: u64,
    /// Per-window checkpoint data (reuses EOWC format).
    pub windows: Vec<WindowCheckpoint>,
}

/// Ring 0 pipeline state for tumbling-window aggregate queries.
///
/// Uses Ring 0's `TumblingWindowAssigner` for O(1) window assignment
/// and `DataFusion` `Accumulator`s for per-group aggregation.
pub(crate) struct Ring0PipelineState {
    /// Ring 0 tumbling window assigner.
    assigner: TumblingWindowAssigner,
    /// Per-window aggregate state: `window_start` -> per-group accumulators.
    #[allow(clippy::type_complexity)]
    windows: BTreeMap<i64, HashMap<Vec<ScalarValue>, Vec<Box<dyn datafusion_expr::Accumulator>>>>,
    /// Aggregate function specs (for creating fresh accumulators).
    agg_specs: Vec<AggFuncSpec>,
    /// Number of group-by columns in pre-agg output.
    num_group_cols: usize,
    /// Group-by column names in output schema.
    #[allow(dead_code)]
    group_col_names: Vec<String>,
    /// Group-by column data types.
    group_types: Vec<DataType>,
    /// Pre-aggregation SQL for expression evaluation.
    pre_agg_sql: String,
    /// Index of the time column in the pre-agg output.
    time_col_index: usize,
    /// Output schema (`window_start` + `window_end` + group cols + agg results).
    output_schema: SchemaRef,
    /// Converted emit strategy from SQL `EmitClause`.
    #[allow(dead_code)] // Stored for Phase 2+ emit strategy variations
    emit_strategy: CoreEmitStrategy,
    /// HAVING predicate SQL to apply after emitting.
    having_sql: Option<String>,
    /// Maximum groups per window before new groups are dropped.
    max_groups_per_window: usize,
}

impl Ring0PipelineState {
    /// Attempt to build a `Ring0PipelineState` by introspecting the logical
    /// plan of the given SQL query. Returns `None` if the query is not a
    /// tumbling-window aggregate (falls through to `IncrementalEowcState`).
    #[allow(clippy::too_many_lines)]
    pub async fn try_from_sql(
        ctx: &SessionContext,
        sql: &str,
        window_config: &WindowOperatorConfig,
        emit_clause: Option<&EmitClause>,
    ) -> Result<Option<Self>, DbError> {
        // Phase 1 gate: only tumbling windows
        if !matches!(
            window_config.window_type,
            WindowType::Tumbling | WindowType::Cumulate
        ) {
            return Ok(None);
        }

        let size_ms = i64::try_from(window_config.size.as_millis()).unwrap_or(i64::MAX);
        if size_ms <= 0 {
            return Ok(None);
        }

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

        let num_group_cols = group_exprs.len();

        // Resolve group-by column names and types
        let mut group_col_names = Vec::new();
        let mut group_types = Vec::new();
        for i in 0..num_group_cols {
            let top_field = top_schema.field(i);
            let agg_field = agg_schema.field(i);
            group_col_names.push(top_field.name().clone());
            group_types.push(agg_field.data_type().clone());
        }

        // Build pre-agg SELECT items (same logic as IncrementalEowcState)
        let mut agg_specs = Vec::new();
        let mut pre_agg_select_items: Vec<String> = Vec::new();

        for (i, group_expr) in group_exprs.iter().enumerate() {
            if let datafusion_expr::Expr::Column(col) = group_expr {
                pre_agg_select_items.push(format!("\"{}\"", col.name));
            } else {
                let group_sql = expr_to_sql(group_expr);
                pre_agg_select_items.push(format!("{group_sql} AS \"__group_{i}\""));
            }
        }

        let mut next_col_idx = num_group_cols;

        for (i, expr) in aggr_exprs.iter().enumerate() {
            let agg_schema_idx = num_group_cols + i;
            let agg_field = agg_schema.field(agg_schema_idx);
            let output_name = if agg_schema_idx < top_schema.fields().len() {
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
                    pre_agg_select_items
                        .push(format!("TRUE AS \"__agg_input_{col_idx}\""));
                    input_col_indices.push(col_idx);
                    input_types.push(DataType::Boolean);
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
                        } else {
                            pre_agg_select_items.push(format!(
                                "{expr_sql} AS \"__agg_input_{col_idx}\""
                            ));
                        }

                        input_col_indices.push(col_idx);
                        let dt = resolve_expr_type(
                            arg_expr,
                            &input_schema,
                            agg_field.data_type(),
                        );
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
        pre_agg_select_items.push(format!(
            "\"{}\" AS \"__ring0_ts\"",
            window_config.time_column
        ));

        // Build pre-agg SQL
        let clauses = extract_clauses(sql);
        let pre_agg_sql = format!(
            "SELECT {} FROM {}{}",
            pre_agg_select_items.join(", "),
            clauses.from_clause,
            clauses.where_clause,
        );

        // Build output schema: window_start + window_end + group cols + agg results
        let mut output_fields: Vec<Field> = vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
        ];
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
        let output_schema = Arc::new(Schema::new(output_fields));

        let having_sql = having_predicate.as_ref().map(expr_to_sql);
        let assigner = TumblingWindowAssigner::from_millis(size_ms);

        // Wire the SQL→Core emit strategy bridge
        let emit_strategy = match emit_clause {
            Some(ec) => crate::stream_executor::emit_clause_to_core(ec)
                .unwrap_or(CoreEmitStrategy::OnWindowClose),
            None => CoreEmitStrategy::OnWindowClose,
        };

        Ok(Some(Self {
            assigner,
            windows: BTreeMap::new(),
            agg_specs,
            num_group_cols,
            group_col_names,
            group_types,
            pre_agg_sql,
            output_schema,
            time_col_index,
            emit_strategy,
            having_sql,
            max_groups_per_window: 1_000_000,
        }))
    }

    /// Update per-window accumulators with a new pre-aggregation batch.
    ///
    /// For each row: extracts the timestamp, uses Ring 0's
    /// `TumblingWindowAssigner::assign()` for O(1) window assignment,
    /// extracts the group key, and updates the corresponding accumulators.
    pub fn update_batch(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let ts_array = extract_i64_timestamps(batch, self.time_col_index)?;

        for (row, &ts_ms) in ts_array.iter().enumerate() {
            // Ring 0 O(1) window assignment
            let window_id = self.assigner.assign(ts_ms);
            let window_start = window_id.start;

            // Extract group key
            let mut key = Vec::with_capacity(self.num_group_cols);
            for col_idx in 0..self.num_group_cols {
                let sv = ScalarValue::try_from_array(batch.column(col_idx), row).map_err(|e| {
                    DbError::Pipeline(format!("group key extraction: {e}"))
                })?;
                key.push(sv);
            }

            let window_groups = self.windows.entry(window_start).or_default();

            if !window_groups.contains_key(&key) {
                if window_groups.len() >= self.max_groups_per_window {
                    tracing::warn!(
                        max_groups = self.max_groups_per_window,
                        window_start,
                        "Ring 0 per-window group cardinality limit reached"
                    );
                    continue;
                }
                let mut accs = Vec::with_capacity(self.agg_specs.len());
                for spec in &self.agg_specs {
                    accs.push(spec.create_accumulator()?);
                }
                window_groups.insert(key.clone(), accs);
            }

            let Some(accs) = window_groups.get_mut(&key) else {
                continue;
            };

            // Update each accumulator with this row's values
            let index_array =
                arrow::array::UInt32Array::from(vec![
                    #[allow(clippy::cast_possible_truncation)]
                    (row as u32),
                ]);
            for (i, spec) in self.agg_specs.iter().enumerate() {
                let mut input_arrays: Vec<ArrayRef> =
                    Vec::with_capacity(spec.input_col_indices.len());
                for &col_idx in &spec.input_col_indices {
                    let arr = compute::take(batch.column(col_idx), &index_array, None).map_err(
                        |e| DbError::Pipeline(format!("array take: {e}")),
                    )?;
                    input_arrays.push(arr);
                }

                // Apply FILTER mask
                if let Some(filter_idx) = spec.filter_col_index {
                    let filter_arr =
                        compute::take(batch.column(filter_idx), &index_array, None).map_err(
                            |e| DbError::Pipeline(format!("filter take: {e}")),
                        )?;
                    if let Some(mask) = filter_arr
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                    {
                        let mut filtered = Vec::with_capacity(input_arrays.len());
                        for arr in &input_arrays {
                            filtered.push(compute::filter(arr, mask).map_err(|e| {
                                DbError::Pipeline(format!("filter apply: {e}"))
                            })?);
                        }
                        input_arrays = filtered;
                    }
                }

                accs[i].update_batch(&input_arrays).map_err(|e| {
                    DbError::Pipeline(format!("accumulator update: {e}"))
                })?;
            }
        }

        Ok(())
    }

    /// Close windows whose end <= watermark, returning emitted batches.
    ///
    /// Uses `assigner.size_ms()` to compute window end from window start.
    pub fn close_windows(&mut self, watermark_ms: i64) -> Result<Vec<RecordBatch>, DbError> {
        let size_ms = self.assigner.size_ms();

        // Collect window starts that should close
        let to_close: Vec<i64> = self
            .windows
            .keys()
            .copied()
            .take_while(|&ws| ws.saturating_add(size_ms) <= watermark_ms)
            .collect();

        if to_close.is_empty() {
            return Ok(Vec::new());
        }

        let mut result_batches = Vec::new();

        for window_start in to_close {
            let Some(groups) = self.windows.remove(&window_start) else {
                continue;
            };

            if groups.is_empty() {
                continue;
            }

            let window_end = window_start.saturating_add(size_ms);
            let batch = self.emit_window(window_start, window_end, groups)?;
            if let Some(b) = batch {
                result_batches.push(b);
            }
        }

        Ok(result_batches)
    }

    /// Emit a single window's accumulated state as a `RecordBatch`.
    fn emit_window(
        &self,
        window_start: i64,
        window_end: i64,
        mut groups: HashMap<Vec<ScalarValue>, Vec<Box<dyn datafusion_expr::Accumulator>>>,
    ) -> Result<Option<RecordBatch>, DbError> {
        let num_rows = groups.len();
        if num_rows == 0 {
            return Ok(None);
        }

        let win_start_array: ArrayRef =
            Arc::new(arrow::array::Int64Array::from(vec![window_start; num_rows]));
        let win_end_array: ArrayRef =
            Arc::new(arrow::array::Int64Array::from(vec![window_end; num_rows]));

        // Build group-key columns
        let mut group_arrays: Vec<ArrayRef> = Vec::with_capacity(self.num_group_cols);
        for (col_idx, dt) in self.group_types.iter().enumerate() {
            let scalars: Vec<ScalarValue> =
                groups.keys().map(|key| key[col_idx].clone()).collect();
            let array = ScalarValue::iter_to_array(scalars)
                .map_err(|e| DbError::Pipeline(format!("group key array: {e}")))?;
            if array.data_type() == dt {
                group_arrays.push(array);
            } else {
                let casted = arrow::compute::cast(&array, dt).unwrap_or(array);
                group_arrays.push(casted);
            }
        }

        // Build aggregate result columns
        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(self.agg_specs.len());
        for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
            let mut scalars: Vec<ScalarValue> = Vec::with_capacity(num_rows);
            for accs in groups.values_mut() {
                let sv = accs[agg_idx]
                    .evaluate()
                    .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;
                scalars.push(sv);
            }
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

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), all_arrays)
            .map_err(|e| DbError::Pipeline(format!("result batch build: {e}")))?;

        Ok(Some(batch))
    }

    /// Returns the pre-aggregation SQL for this query.
    pub fn pre_agg_sql(&self) -> &str {
        &self.pre_agg_sql
    }

    /// Returns the HAVING predicate SQL, if any.
    pub fn having_sql(&self) -> Option<&str> {
        self.having_sql.as_deref()
    }

    /// Compute a fingerprint for this query (SQL + schema).
    pub(crate) fn query_fingerprint(&self) -> u64 {
        query_fingerprint(&self.pre_agg_sql, &self.output_schema)
    }

    /// Checkpoint all per-window group states into a serializable struct.
    pub(crate) fn checkpoint_windows(
        &mut self,
    ) -> Result<Ring0StateCheckpoint, DbError> {
        use crate::aggregate_state::scalar_to_json;

        let fingerprint = self.query_fingerprint();
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
        Ok(Ring0StateCheckpoint {
            fingerprint,
            windows,
        })
    }

    /// Restore per-window group states from a checkpoint.
    pub(crate) fn restore_windows(
        &mut self,
        checkpoint: &Ring0StateCheckpoint,
    ) -> Result<usize, DbError> {
        use crate::aggregate_state::json_to_scalar;

        let current_fp = self.query_fingerprint();
        if checkpoint.fingerprint != current_fp {
            return Err(DbError::Pipeline(format!(
                "Ring 0 checkpoint fingerprint mismatch: saved={}, current={}",
                checkpoint.fingerprint, current_fp
            )));
        }
        self.windows.clear();
        let mut total_groups = 0usize;
        for wc in &checkpoint.windows {
            let mut groups = HashMap::new();
            for gc in &wc.groups {
                let key: Result<Vec<ScalarValue>, _> =
                    gc.key.iter().map(json_to_scalar).collect();
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
                                sv.to_array().map_err(|e| {
                                    DbError::Pipeline(format!("scalar to array: {e}"))
                                })
                            })
                            .collect::<Result<_, _>>()?;
                        acc.merge_batch(&arrays).map_err(|e| {
                            DbError::Pipeline(format!("accumulator merge: {e}"))
                        })?;
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
            Field::new("__ring0_ts", DataType::Int64, false),
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

    /// Build a `Ring0PipelineState` for SUM(Int64) with 1-second tumbling
    /// windows and a single Utf8 group-by column.
    fn make_ring0_state(size_ms: i64) -> Ring0PipelineState {
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

        Ring0PipelineState {
            assigner: TumblingWindowAssigner::from_millis(size_ms),
            windows: BTreeMap::new(),
            agg_specs,
            num_group_cols: 1,
            group_col_names: vec!["symbol".to_string()],
            group_types: vec![DataType::Utf8],
            pre_agg_sql: String::new(),
            output_schema,
            time_col_index: 2,
            emit_strategy: CoreEmit::OnWindowClose,
            having_sql: None,
            max_groups_per_window: 1_000_000,
        }
    }

    /// Build a multi-aggregate Ring0 state: SUM + COUNT.
    fn make_ring0_state_multi_agg(size_ms: i64) -> Ring0PipelineState {
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

        Ring0PipelineState {
            assigner: TumblingWindowAssigner::from_millis(size_ms),
            windows: BTreeMap::new(),
            agg_specs,
            num_group_cols: 1,
            group_col_names: vec!["symbol".to_string()],
            group_types: vec![DataType::Utf8],
            pre_agg_sql: String::new(),
            output_schema,
            time_col_index: 2,
            emit_strategy: CoreEmit::OnWindowClose,
            having_sql: None,
            max_groups_per_window: 1_000_000,
        }
    }

    // ── Detection tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_detect_tumbling_aggregate_returns_ring0_config() {
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
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        let sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";
        let result = Ring0PipelineState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(result.is_some(), "Tumbling aggregate should return Some");
    }

    #[tokio::test]
    async fn test_detect_non_windowed_aggregate_returns_none() {
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

        // Sliding window → should return None (Phase 1 only supports tumbling)
        let window_config = WindowOperatorConfig {
            window_type: WindowType::Sliding,
            time_column: "ts".to_string(),
            size: Duration::from_secs(60),
            slide: Some(Duration::from_secs(10)),
            gap: None,
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        let sql = "SELECT id, SUM(val) AS total FROM events GROUP BY id";
        let result = Ring0PipelineState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(result.is_none(), "Sliding window should return None");
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
            allowed_lateness: Duration::ZERO,
            emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
            late_data_side_output: None,
        };

        // No aggregate → should return None
        let sql = "SELECT id, val FROM events";
        let result = Ring0PipelineState::try_from_sql(&ctx, sql, &window_config, None)
            .await
            .unwrap();
        assert!(result.is_none(), "Projection-only should return None");
    }

    // ── Pipeline tests ──────────────────────────────────────────────

    #[test]
    fn test_ring0_tumbling_sum() {
        let mut state = make_ring0_state(1000);

        // Two events in window [0, 1000)
        let batch1 = make_pre_agg_batch(
            vec!["AAPL", "AAPL"],
            vec![10, 20],
            vec![100, 500],
        );
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
        let ws = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ws.value(0), 0);

        // Check window_end = 1000
        let we = result.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(we.value(0), 1000);

        // Check SUM = 10 + 20 + 30 = 60
        let total = result.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(total.value(0), 60);
    }

    #[test]
    fn test_ring0_tumbling_multi_aggregate_multi_group() {
        let mut state = make_ring0_state_multi_agg(1000);

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
    fn test_ring0_close_windows_watermark() {
        let mut state = make_ring0_state(1000);

        // Events in three windows
        let batch = make_pre_agg_batch(
            vec!["A", "A", "A"],
            vec![1, 2, 3],
            vec![100, 1100, 2100],
        );
        state.update_batch(&batch).unwrap();

        // Watermark at 1500 → only window [0, 1000) closes
        let batches = state.close_windows(1500).unwrap();
        assert_eq!(batches.len(), 1);
        let ws = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ws.value(0), 0);

        // Watermark at 2000 → window [1000, 2000) closes
        let batches = state.close_windows(2000).unwrap();
        assert_eq!(batches.len(), 1);
        let ws = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
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
        assert_eq!(
            sql_emit_to_core(&SqlEmit::OnUpdate),
            CoreEmit::OnUpdate
        );
        assert_eq!(
            sql_emit_to_core(&SqlEmit::Changelog),
            CoreEmit::Changelog
        );
        assert_eq!(
            sql_emit_to_core(&SqlEmit::FinalOnly),
            CoreEmit::Final
        );

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
    fn test_ring0_pipeline_checkpoint_roundtrip() {
        let mut state = make_ring0_state(1000);

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
        let mut state2 = make_ring0_state(1000);
        let restored = state2.restore_windows(&cp).unwrap();
        assert!(restored > 0, "Should have restored groups");

        // Close first window and verify SUM
        let batches = state2.close_windows(1000).unwrap();
        assert_eq!(batches.len(), 1);
        let result = &batches[0];
        assert_eq!(result.num_rows(), 1); // Only AAPL in window [0,1000)

        let total = result.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(total.value(0), 30, "SUM should be 10+20=30");

        // Close second window
        let batches = state2.close_windows(2000).unwrap();
        assert_eq!(batches.len(), 1);
        let total2 = batches[0].column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(total2.value(0), 100, "SUM should be 100");
    }

    #[test]
    fn test_ring0_checkpoint_fingerprint_mismatch() {
        let mut state = make_ring0_state(1000);

        let batch = make_pre_agg_batch(vec!["AAPL"], vec![10], vec![100]);
        state.update_batch(&batch).unwrap();

        let mut cp = state.checkpoint_windows().unwrap();
        cp.fingerprint = 12345; // tamper

        let mut state2 = make_ring0_state(1000);
        let result = state2.restore_windows(&cp);
        assert!(result.is_err(), "Should fail on fingerprint mismatch");
    }
}
