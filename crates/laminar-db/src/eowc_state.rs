//! Incremental per-window accumulator state for EOWC queries.
//!
//! Replaces the raw-batch accumulation pattern (`EowcState.accumulated_sources`)
//! with per-window-per-group incremental `Accumulator` state. This reduces
//! memory from O(events) to O(windows * groups) and window-close latency
//! from O(N) to O(groups).
//!
//! # Architecture
//!
//! ```text
//! Source batch → pre-agg SQL → assign windows → per-window accumulators
//!                                                    ↕ (persists across cycles)
//! Watermark advance → close windows → evaluate() → emit RecordBatch
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;

use laminar_sql::translator::{WindowOperatorConfig, WindowType};

use crate::aggregate_state::{
    extract_clauses, expr_to_sql, find_aggregate, resolve_expr_type, AggFuncSpec,
    EowcStateCheckpoint,
};
use crate::error::DbError;

/// Window type with parameters for window assignment.
#[derive(Debug, Clone)]
pub(crate) enum EowcWindowType {
    /// Tumbling: non-overlapping, fixed-size windows.
    Tumbling { size_ms: i64 },
    /// Hopping: overlapping windows with fixed size and slide.
    Hopping { size_ms: i64, slide_ms: i64 },
    /// Session: gap-based windows (simplified — merge not yet implemented).
    Session { gap_ms: i64 },
}

impl EowcWindowType {
    /// Convert from a `WindowOperatorConfig`.
    fn from_config(config: &WindowOperatorConfig) -> Self {
        match config.window_type {
            WindowType::Tumbling | WindowType::Cumulate => {
                let size_ms = i64::try_from(config.size.as_millis())
                    .unwrap_or(i64::MAX);
                EowcWindowType::Tumbling { size_ms }
            }
            WindowType::Sliding => {
                let size_ms = i64::try_from(config.size.as_millis())
                    .unwrap_or(i64::MAX);
                let slide_ms = config
                    .slide
                    .map_or(size_ms, |s| {
                        i64::try_from(s.as_millis()).unwrap_or(i64::MAX)
                    });
                EowcWindowType::Hopping { size_ms, slide_ms }
            }
            WindowType::Session => {
                let gap_ms = config
                    .gap
                    .map_or(0, |g| {
                        i64::try_from(g.as_millis()).unwrap_or(i64::MAX)
                    });
                EowcWindowType::Session { gap_ms }
            }
        }
    }

    /// Window size in milliseconds (for close boundary computation).
    fn size_ms(&self) -> i64 {
        match self {
            EowcWindowType::Tumbling { size_ms }
            | EowcWindowType::Hopping { size_ms, .. } => *size_ms,
            EowcWindowType::Session { gap_ms } => *gap_ms,
        }
    }
}

/// Assign a timestamp to one or more window start boundaries.
fn assign_windows(ts_ms: i64, window_type: &EowcWindowType) -> Vec<i64> {
    match window_type {
        EowcWindowType::Tumbling { size_ms } => {
            if *size_ms <= 0 {
                return vec![0];
            }
            vec![ts_ms.div_euclid(*size_ms) * size_ms]
        }
        EowcWindowType::Hopping {
            size_ms, slide_ms, ..
        } => {
            if *slide_ms <= 0 || *size_ms <= 0 {
                return vec![0];
            }
            let mut windows = Vec::new();
            // The last slide-aligned start at or before ts
            let last_start = ts_ms.div_euclid(*slide_ms) * slide_ms;
            let mut start = last_start;
            while start + size_ms > ts_ms {
                windows.push(start);
                start -= slide_ms;
                if start < 0 {
                    break;
                }
            }
            windows
        }
        EowcWindowType::Session { .. } => {
            // Session windows use the event timestamp as the initial window start.
            // Merging of adjacent windows is deferred to a future PR.
            vec![ts_ms]
        }
    }
}

/// Per-window accumulator state for EOWC aggregate queries.
///
/// Keyed by window start timestamp (ms since epoch). Each window contains
/// per-group accumulators that are updated incrementally as batches arrive.
pub(crate) struct IncrementalEowcState {
    /// Window type determines assignment logic.
    window_type: EowcWindowType,
    /// Per-window aggregate state: `window_start` -> per-group accumulators.
    #[allow(clippy::type_complexity)]
    windows: BTreeMap<i64, HashMap<Vec<ScalarValue>, Vec<Box<dyn datafusion_expr::Accumulator>>>>,
    /// Aggregate function specs (extracted once from the query plan).
    agg_specs: Vec<AggFuncSpec>,
    /// Number of group-by columns in pre-agg output.
    num_group_cols: usize,
    /// Group-by column names in output schema.
    #[allow(dead_code)] // Used in future checkpoint serialization
    group_col_names: Vec<String>,
    /// Group-by column data types.
    group_types: Vec<DataType>,
    /// Pre-aggregation SQL for expression evaluation.
    pre_agg_sql: String,
    /// Output schema for emitted batches (`window_start` + `window_end` + group cols + agg results).
    output_schema: SchemaRef,
    /// Index of the time column in the pre-agg output.
    time_col_index: usize,
    /// HAVING predicate SQL to apply after emitting.
    having_sql: Option<String>,
    /// Maximum groups per window before new groups are dropped.
    max_groups_per_window: usize,
}

impl IncrementalEowcState {
    /// Attempt to build an `IncrementalEowcState` by introspecting the logical
    /// plan of the given SQL query. Returns `None` if the query does not
    /// contain an `Aggregate` node (not an aggregation query).
    #[allow(clippy::too_many_lines)]
    pub async fn try_from_sql(
        ctx: &SessionContext,
        sql: &str,
        window_config: &WindowOperatorConfig,
    ) -> Result<Option<Self>, DbError> {
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

        // Build pre-agg SELECT items (same logic as IncrementalAggState)
        let mut agg_specs = Vec::new();
        let mut pre_agg_select_items: Vec<String> = Vec::new();

        for (i, group_expr) in group_exprs.iter().enumerate() {
            if let datafusion_expr::Expr::Column(col) = group_expr {
                pre_agg_select_items.push(format!("\"{}\"", col.name));
            } else {
                let group_sql = expr_to_sql(group_expr);
                pre_agg_select_items.push(format!(
                    "{group_sql} AS \"__group_{i}\""
                ));
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

                let filter_col_index =
                    if let Some(filter_expr) = &agg_func.params.filter {
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
            "\"{}\" AS \"__eowc_ts\"",
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
        let window_type = EowcWindowType::from_config(window_config);

        Ok(Some(Self {
            window_type,
            windows: BTreeMap::new(),
            agg_specs,
            num_group_cols,
            group_col_names,
            group_types,
            pre_agg_sql,
            output_schema,
            time_col_index,
            having_sql,
            max_groups_per_window: 1_000_000,
        }))
    }

    /// Update per-window accumulators with a new pre-aggregation batch.
    ///
    /// For each row: extracts the timestamp, assigns to window(s), extracts
    /// the group key, and updates the corresponding accumulators.
    pub fn update_batch(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Extract timestamps from the time column
        let ts_array = extract_i64_timestamps(batch, self.time_col_index)?;

        for (row, &ts_ms) in ts_array.iter().enumerate() {

            // Assign to window(s)
            let window_starts = assign_windows(ts_ms, &self.window_type);

            // Extract group key
            let mut key = Vec::with_capacity(self.num_group_cols);
            for col_idx in 0..self.num_group_cols {
                let sv = ScalarValue::try_from_array(
                    batch.column(col_idx),
                    row,
                )
                .map_err(|e| {
                    DbError::Pipeline(format!("group key extraction: {e}"))
                })?;
                key.push(sv);
            }

            for window_start in &window_starts {
                let window_groups = self.windows.entry(*window_start).or_default();

                if !window_groups.contains_key(&key) {
                    if window_groups.len() >= self.max_groups_per_window {
                        tracing::warn!(
                            max_groups = self.max_groups_per_window,
                            window_start,
                            "EOWC per-window group cardinality limit reached"
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
                let index_array = arrow::array::UInt32Array::from(vec![
                    #[allow(clippy::cast_possible_truncation)]
                    (row as u32),
                ]);
                for (i, spec) in self.agg_specs.iter().enumerate() {
                    let mut input_arrays: Vec<ArrayRef> =
                        Vec::with_capacity(spec.input_col_indices.len());
                    for &col_idx in &spec.input_col_indices {
                        let arr = compute::take(
                            batch.column(col_idx),
                            &index_array,
                            None,
                        )
                        .map_err(|e| {
                            DbError::Pipeline(format!("array take: {e}"))
                        })?;
                        input_arrays.push(arr);
                    }

                    // Apply FILTER mask
                    if let Some(filter_idx) = spec.filter_col_index {
                        let filter_arr = compute::take(
                            batch.column(filter_idx),
                            &index_array,
                            None,
                        )
                        .map_err(|e| {
                            DbError::Pipeline(format!("filter take: {e}"))
                        })?;
                        if let Some(mask) = filter_arr
                            .as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                        {
                            let mut filtered =
                                Vec::with_capacity(input_arrays.len());
                            for arr in &input_arrays {
                                filtered.push(
                                    compute::filter(arr, mask).map_err(
                                        |e| {
                                            DbError::Pipeline(format!(
                                                "filter apply: {e}"
                                            ))
                                        },
                                    )?,
                                );
                            }
                            input_arrays = filtered;
                        }
                    }

                    accs[i].update_batch(&input_arrays).map_err(|e| {
                        DbError::Pipeline(format!("accumulator update: {e}"))
                    })?;
                }
            }
        }

        Ok(())
    }

    /// Close windows whose end <= watermark, returning emitted batches.
    ///
    /// Iterates the `BTreeMap` from earliest window and closes all windows
    /// where `window_start + window_size <= watermark_ms`. Closed windows
    /// are removed from state.
    pub fn close_windows(
        &mut self,
        watermark_ms: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let size_ms = self.window_type.size_ms();
        if size_ms <= 0 {
            return Ok(Vec::new());
        }

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
            let batch =
                self.emit_window(window_start, window_end, groups)?;
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

        // Build window_start and window_end columns
        let win_start_array: ArrayRef =
            Arc::new(arrow::array::Int64Array::from(vec![window_start; num_rows]));
        let win_end_array: ArrayRef =
            Arc::new(arrow::array::Int64Array::from(vec![window_end; num_rows]));

        // Build group-key columns
        let mut group_arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.num_group_cols);
        for (col_idx, dt) in self.group_types.iter().enumerate() {
            let scalars: Vec<ScalarValue> = groups
                .keys()
                .map(|key| key[col_idx].clone())
                .collect();
            let array =
                ScalarValue::iter_to_array(scalars).map_err(|e| {
                    DbError::Pipeline(format!("group key array: {e}"))
                })?;
            if array.data_type() == dt {
                group_arrays.push(array);
            } else {
                let casted =
                    arrow::compute::cast(&array, dt).unwrap_or(array);
                group_arrays.push(casted);
            }
        }

        // Build aggregate result columns
        let mut agg_arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.agg_specs.len());
        for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
            let mut scalars: Vec<ScalarValue> =
                Vec::with_capacity(num_rows);
            for accs in groups.values_mut() {
                let sv = accs[agg_idx].evaluate().map_err(|e| {
                    DbError::Pipeline(format!(
                        "accumulator evaluate: {e}"
                    ))
                })?;
                scalars.push(sv);
            }
            let array =
                ScalarValue::iter_to_array(scalars).map_err(|e| {
                    DbError::Pipeline(format!("agg result array: {e}"))
                })?;
            if array.data_type() == &spec.return_type {
                agg_arrays.push(array);
            } else {
                let casted = arrow::compute::cast(&array, &spec.return_type)
                    .unwrap_or(array);
                agg_arrays.push(casted);
            }
        }

        // Combine: window_start + window_end + group cols + agg results
        let mut all_arrays = vec![win_start_array, win_end_array];
        all_arrays.extend(group_arrays);
        all_arrays.extend(agg_arrays);

        let batch = RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            all_arrays,
        )
        .map_err(|e| {
            DbError::Pipeline(format!("result batch build: {e}"))
        })?;

        Ok(Some(batch))
    }

    /// Returns the pre-aggregation SQL for this query.
    pub fn pre_agg_sql(&self) -> &str {
        &self.pre_agg_sql
    }

    /// Returns the output schema.
    #[allow(dead_code)] // Public API for checkpoint serialization
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Returns the HAVING predicate SQL, if any.
    pub fn having_sql(&self) -> Option<&str> {
        self.having_sql.as_deref()
    }

    /// Return the number of open windows.
    #[allow(dead_code)] // Public API for observability
    pub fn open_window_count(&self) -> usize {
        self.windows.len()
    }

    /// Return the total number of groups across all open windows.
    #[allow(dead_code)]
    pub fn total_group_count(&self) -> usize {
        self.windows.values().map(HashMap::len).sum()
    }

    /// Compute a fingerprint for this query (SQL + schema).
    pub(crate) fn query_fingerprint(&self) -> u64 {
        crate::aggregate_state::query_fingerprint(
            &self.pre_agg_sql,
            &self.output_schema,
        )
    }

    /// Checkpoint all per-window group states into a serializable struct.
    pub(crate) fn checkpoint_windows(
        &mut self,
    ) -> Result<EowcStateCheckpoint, DbError> {
        use crate::aggregate_state::{
            scalar_to_json, GroupCheckpoint, WindowCheckpoint,
        };

        let fingerprint = self.query_fingerprint();
        let mut windows = Vec::with_capacity(self.windows.len());
        for (&window_start, groups) in &mut self.windows {
            let mut group_checkpoints =
                Vec::with_capacity(groups.len());
            for (key, accs) in groups {
                let key_json: Vec<serde_json::Value> =
                    key.iter().map(scalar_to_json).collect();
                let mut acc_states = Vec::with_capacity(accs.len());
                for acc in accs {
                    let state = acc.state().map_err(|e| {
                        DbError::Pipeline(format!(
                            "accumulator state: {e}"
                        ))
                    })?;
                    acc_states
                        .push(state.iter().map(scalar_to_json).collect());
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
        Ok(EowcStateCheckpoint {
            fingerprint,
            windows,
        })
    }

    /// Restore per-window group states from a checkpoint.
    pub(crate) fn restore_windows(
        &mut self,
        checkpoint: &EowcStateCheckpoint,
    ) -> Result<usize, DbError> {
        use crate::aggregate_state::json_to_scalar;

        let current_fp = self.query_fingerprint();
        if checkpoint.fingerprint != current_fp {
            return Err(DbError::Pipeline(format!(
                "EOWC checkpoint fingerprint mismatch: saved={}, current={}",
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
                let mut accs =
                    Vec::with_capacity(self.agg_specs.len());
                for (i, spec) in self.agg_specs.iter().enumerate() {
                    let mut acc = spec.create_accumulator()?;
                    if i < gc.acc_states.len() {
                        let state_scalars: Result<
                            Vec<ScalarValue>,
                            _,
                        > = gc.acc_states[i]
                            .iter()
                            .map(json_to_scalar)
                            .collect();
                        let state_scalars = state_scalars?;
                        let arrays: Vec<arrow::array::ArrayRef> =
                            state_scalars
                                .iter()
                                .map(|sv| {
                                    sv.to_array().map_err(|e| {
                                        DbError::Pipeline(format!(
                                            "scalar to array: {e}"
                                        ))
                                    })
                                })
                                .collect::<Result<_, _>>()?;
                        acc.merge_batch(&arrays).map_err(|e| {
                            DbError::Pipeline(format!(
                                "accumulator merge: {e}"
                            ))
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

/// Extract i64 timestamp values from a batch column.
///
/// Handles Int64 columns directly and Arrow Timestamp columns by extracting
/// the underlying i64 values (already in the native time unit).
pub(crate) fn extract_i64_timestamps(
    batch: &RecordBatch,
    col_index: usize,
) -> Result<Vec<i64>, DbError> {
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::TimeUnit;

    let col = batch.column(col_index);
    let mut result = Vec::with_capacity(batch.num_rows());

    match col.data_type() {
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DbError::Pipeline("expected Int64Array".to_string())
                })?;
            for i in 0..arr.len() {
                result.push(if arr.is_null(i) { 0 } else { arr.value(i) });
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline(
                        "expected TimestampMillisecondArray".to_string(),
                    )
                })?;
            for i in 0..arr.len() {
                result.push(if arr.is_null(i) { 0 } else { arr.value(i) });
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline(
                        "expected TimestampSecondArray".to_string(),
                    )
                })?;
            for i in 0..arr.len() {
                let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                result.push(v.saturating_mul(1000));
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline(
                        "expected TimestampMicrosecondArray".to_string(),
                    )
                })?;
            for i in 0..arr.len() {
                let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                result.push(v / 1000);
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| {
                    DbError::Pipeline(
                        "expected TimestampNanosecondArray".to_string(),
                    )
                })?;
            for i in 0..arr.len() {
                let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                result.push(v / 1_000_000);
            }
        }
        other => {
            return Err(DbError::Pipeline(format!(
                "unsupported timestamp type for EOWC: {other}"
            )));
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    // ── Window assignment tests ──────────────────────────────────────

    #[test]
    fn test_tumbling_window_assignment_aligns_to_boundary() {
        let wt = EowcWindowType::Tumbling { size_ms: 1000 };
        assert_eq!(assign_windows(0, &wt), vec![0]);
        assert_eq!(assign_windows(500, &wt), vec![0]);
        assert_eq!(assign_windows(999, &wt), vec![0]);
        assert_eq!(assign_windows(1000, &wt), vec![1000]);
        assert_eq!(assign_windows(1500, &wt), vec![1000]);
        assert_eq!(assign_windows(2000, &wt), vec![2000]);
    }

    #[test]
    fn test_hopping_window_assignment_returns_multiple_windows() {
        let wt = EowcWindowType::Hopping {
            size_ms: 1000,
            slide_ms: 500,
        };
        // ts=750 belongs to windows starting at [500, 0]
        let mut ws = assign_windows(750, &wt);
        ws.sort();
        assert_eq!(ws, vec![0, 500]);

        // ts=1250 belongs to windows starting at [1000, 500]
        let mut ws = assign_windows(1250, &wt);
        ws.sort();
        assert_eq!(ws, vec![500, 1000]);
    }

    #[test]
    fn test_session_window_assignment_uses_event_timestamp() {
        let wt = EowcWindowType::Session { gap_ms: 5000 };
        assert_eq!(assign_windows(1234, &wt), vec![1234]);
    }

    #[test]
    fn test_tumbling_window_zero_size_returns_zero() {
        let wt = EowcWindowType::Tumbling { size_ms: 0 };
        assert_eq!(assign_windows(500, &wt), vec![0]);
    }

    // ── Timestamp extraction tests ───────────────────────────────────

    #[test]
    fn test_extract_i64_timestamps_from_int64() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![100, 200, 300]))],
        )
        .unwrap();
        let ts = extract_i64_timestamps(&batch, 0).unwrap();
        assert_eq!(ts, vec![100, 200, 300]);
    }

    #[test]
    fn test_extract_i64_timestamps_from_timestamp_millis() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                arrow::array::TimestampMillisecondArray::from(vec![
                    100, 200, 300,
                ]),
            )],
        )
        .unwrap();
        let ts = extract_i64_timestamps(&batch, 0).unwrap();
        assert_eq!(ts, vec![100, 200, 300]);
    }

    // ── Incremental state tests ──────────────────────────────────────

    fn make_pre_agg_batch(
        groups: Vec<&str>,
        values: Vec<i64>,
        timestamps: Vec<i64>,
    ) -> RecordBatch {
        use arrow::array::StringArray;
        assert_eq!(groups.len(), values.len());
        assert_eq!(groups.len(), timestamps.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("__agg_input_1", DataType::Int64, false),
            Field::new("__eowc_ts", DataType::Int64, false),
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

    fn make_eowc_state(
        window_type: EowcWindowType,
    ) -> IncrementalEowcState {
        use datafusion::execution::FunctionRegistry;

        // Create a SUM(Int64) accumulator spec
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

        IncrementalEowcState {
            window_type,
            windows: BTreeMap::new(),
            agg_specs,
            num_group_cols: 1,
            group_col_names: vec!["symbol".to_string()],
            group_types: vec![DataType::Utf8],
            pre_agg_sql: String::new(),
            output_schema,
            time_col_index: 2,
            having_sql: None,
            max_groups_per_window: 1_000_000,
        }
    }

    #[test]
    fn test_incremental_eowc_tumbling_sum() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        // Batch 1: two events in window [0, 1000)
        let batch1 = make_pre_agg_batch(
            vec!["AAPL", "AAPL"],
            vec![10, 20],
            vec![100, 500],
        );
        state.update_batch(&batch1).unwrap();

        // Batch 2: one more event in same window
        let batch2 =
            make_pre_agg_batch(vec!["AAPL"], vec![30], vec![800]);
        state.update_batch(&batch2).unwrap();

        assert_eq!(state.open_window_count(), 1);

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

        // Window should be removed
        assert_eq!(state.open_window_count(), 0);
    }

    #[test]
    fn test_incremental_eowc_multi_group() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        let batch = make_pre_agg_batch(
            vec!["AAPL", "GOOG", "AAPL", "GOOG"],
            vec![10, 100, 20, 200],
            vec![100, 200, 300, 400],
        );
        state.update_batch(&batch).unwrap();

        let batches = state.close_windows(1000).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_incremental_eowc_multi_window() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        // Events across two windows
        let batch = make_pre_agg_batch(
            vec!["AAPL", "AAPL"],
            vec![10, 20],
            vec![500, 1500],
        );
        state.update_batch(&batch).unwrap();

        assert_eq!(state.open_window_count(), 2);

        // Close only the first window (watermark at 1000)
        let batches = state.close_windows(1000).unwrap();
        assert_eq!(batches.len(), 1);

        let ws = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ws.value(0), 0);

        // Second window still open
        assert_eq!(state.open_window_count(), 1);

        // Close the second window
        let batches = state.close_windows(2000).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(state.open_window_count(), 0);
    }

    #[test]
    fn test_close_windows_returns_empty_when_none_closed() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        let batch =
            make_pre_agg_batch(vec!["AAPL"], vec![10], vec![500]);
        state.update_batch(&batch).unwrap();

        // Watermark hasn't reached window end
        let batches = state.close_windows(500).unwrap();
        assert!(batches.is_empty());
        assert_eq!(state.open_window_count(), 1);
    }

    #[test]
    fn test_empty_batch_update_is_noop() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("__agg_input_1", DataType::Int64, false),
            Field::new("__eowc_ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::new_empty(schema);
        state.update_batch(&batch).unwrap();
        assert_eq!(state.open_window_count(), 0);
    }

    #[test]
    fn test_window_close_emits_correct_schema() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        let batch =
            make_pre_agg_batch(vec!["AAPL"], vec![42], vec![100]);
        state.update_batch(&batch).unwrap();

        let batches = state.close_windows(1000).unwrap();
        let result = &batches[0];
        let schema = result.schema();

        assert_eq!(schema.field(0).name(), "window_start");
        assert_eq!(schema.field(1).name(), "window_end");
        assert_eq!(schema.field(2).name(), "symbol");
        assert_eq!(schema.field(3).name(), "total");
        assert_eq!(schema.fields().len(), 4);
    }

    // ── Checkpoint/restore tests ─────────────────────────────────────

    #[test]
    fn test_eowc_checkpoint_roundtrip_tumbling() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        // Feed data into two windows
        let batch = make_pre_agg_batch(
            vec!["AAPL", "AAPL", "GOOG"],
            vec![10, 20, 100],
            vec![100, 200, 1500],
        );
        state.update_batch(&batch).unwrap();

        assert_eq!(state.open_window_count(), 2);

        // Checkpoint
        let cp = state.checkpoint_windows().unwrap();
        assert_eq!(cp.windows.len(), 2);

        // Create fresh state and restore
        let mut state2 =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });
        let restored = state2.restore_windows(&cp).unwrap();
        assert!(restored > 0, "Should have restored groups");
        assert_eq!(state2.open_window_count(), 2);

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
    fn test_eowc_checkpoint_empty_windows() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        // No data = no windows
        let cp = state.checkpoint_windows().unwrap();
        assert!(cp.windows.is_empty());
    }

    #[test]
    fn test_eowc_checkpoint_fingerprint_mismatch() {
        let mut state =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });

        let batch =
            make_pre_agg_batch(vec!["AAPL"], vec![10], vec![100]);
        state.update_batch(&batch).unwrap();

        let mut cp = state.checkpoint_windows().unwrap();
        cp.fingerprint = 12345; // tamper

        let mut state2 =
            make_eowc_state(EowcWindowType::Tumbling { size_ms: 1000 });
        let result = state2.restore_windows(&cp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("fingerprint mismatch")
        );
    }
}
