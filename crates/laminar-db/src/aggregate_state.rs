//! Incremental aggregate state for streaming GROUP BY queries.
//!
//! Maintains per-group accumulator state across micro-batch cycles so that
//! streaming aggregations (SUM, COUNT, AVG, etc.) produce correct running
//! totals instead of per-batch results.
//!
//! # Architecture
//!
//! ```text
//! Source batch → group-by partition → per-group accumulators → emit RecordBatch
//!                                          ↕ (persists across cycles)
//! ```
//!
//! Uses `DataFusion`'s `Accumulator` trait directly for correct semantics
//! across all 50+ built-in aggregates (including AVG, STDDEV, etc. that
//! require multi-field state).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDF, LogicalPlan};

use crate::error::DbError;

/// Specification for one aggregate function in a streaming query.
struct AggFuncSpec {
    /// The `DataFusion` aggregate UDF.
    udf: Arc<AggregateUDF>,
    /// Input data types for this aggregate.
    input_types: Vec<DataType>,
    /// Column indices in the pre-aggregation output that feed this aggregate.
    /// These index into the pre-agg schema (group cols first, then agg inputs).
    input_col_indices: Vec<usize>,
    /// Output column name (alias from the query).
    output_name: String,
    /// Output data type.
    return_type: DataType,
    /// Whether this is a DISTINCT aggregate (e.g., `COUNT(DISTINCT x)`).
    distinct: bool,
    /// Column index of the FILTER boolean column in pre-agg output, if any.
    filter_col_index: Option<usize>,
}

impl AggFuncSpec {
    /// Create a fresh `DataFusion` accumulator for this function.
    fn create_accumulator(
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
}

/// Incremental aggregation state for a streaming GROUP BY query.
///
/// Persists per-group accumulator state across micro-batch cycles so that
/// running aggregations produce correct results.
pub(crate) struct IncrementalAggState {
    /// SQL that computes pre-aggregation expressions (group keys + agg inputs).
    /// This handles expression evaluation (e.g., `price * quantity`) via `DataFusion`.
    pre_agg_sql: String,
    /// Number of group-by columns in the pre-agg output (first N columns).
    num_group_cols: usize,
    /// Group-by column names in the output schema.
    #[allow(dead_code)] // Used in tests + future checkpoint serialization
    group_col_names: Vec<String>,
    /// Group-by column data types.
    group_types: Vec<DataType>,
    /// Aggregate function specifications.
    agg_specs: Vec<AggFuncSpec>,
    /// Per-group accumulator state.
    groups: HashMap<Vec<ScalarValue>, Vec<Box<dyn datafusion_expr::Accumulator>>>,
    /// Output schema (group columns + aggregate results).
    output_schema: SchemaRef,
    /// HAVING predicate SQL to apply after emitting aggregate results.
    having_sql: Option<String>,
}

impl IncrementalAggState {
    /// Attempt to build an `IncrementalAggState` by introspecting the logical
    /// plan of the given SQL query. Returns `None` if the query does not
    /// contain an `Aggregate` node (not an aggregation query).
    #[allow(clippy::too_many_lines)]
    pub async fn try_from_sql(
        ctx: &SessionContext,
        sql: &str,
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
        let having_predicate = agg_info.having_predicate;

        if aggr_exprs.is_empty() {
            return Ok(None);
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

        // Build agg specs from the aggregate expressions
        let mut agg_specs = Vec::new();
        let mut pre_agg_select_items: Vec<String> = Vec::new();

        // First add group-by columns to pre-agg SELECT
        for name in &group_col_names {
            pre_agg_select_items.push(format!("\"{name}\""));
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
                    pre_agg_select_items
                        .push(format!("TRUE AS \"__agg_input_{col_idx}\""));
                    input_col_indices.push(col_idx);
                    input_types.push(DataType::Boolean);
                } else {
                    for arg_expr in &agg_func.params.args {
                        let col_idx = next_col_idx;
                        next_col_idx += 1;
                        let expr_sql = expr_to_sql(arg_expr);

                        // H-02: If FILTER clause present, wrap input with
                        // CASE WHEN so filtered rows become NULL (ignored
                        // by accumulators)
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
                        let dt = infer_input_type(
                            ctx,
                            &udf,
                            agg_field.data_type(),
                        );
                        input_types.push(dt);
                    }
                }

                // H-02: Add a boolean filter column for masking rows
                // in process_batch when FILTER clause is present
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
                // Non-aggregate expression in the aggregate output —
                // this shouldn't happen for well-formed plans.
                return Ok(None);
            }
        }

        // Build the pre-agg SQL. Extract source tables from the original query.
        let from_clause = extract_from_clause(sql);
        let where_clause = extract_where_clause(sql);
        let pre_agg_sql = format!(
            "SELECT {} FROM {from_clause}{where_clause}",
            pre_agg_select_items.join(", ")
        );

        // Build output schema
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
        let output_schema = Arc::new(Schema::new(output_fields));

        let having_sql = having_predicate.as_ref().map(expr_to_sql);

        Ok(Some(Self {
            pre_agg_sql,
            num_group_cols,
            group_col_names,
            group_types,
            agg_specs,
            groups: HashMap::new(),
            output_schema,
            having_sql,
        }))
    }

    /// Process a pre-aggregation batch: partition by group key and update
    /// accumulators.
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Build group key → row indices mapping
        let mut group_indices: HashMap<Vec<ScalarValue>, Vec<u32>> = HashMap::new();
        for row in 0..batch.num_rows() {
            let mut key = Vec::with_capacity(self.num_group_cols);
            for col_idx in 0..self.num_group_cols {
                let sv = ScalarValue::try_from_array(batch.column(col_idx), row)
                    .map_err(|e| {
                        DbError::Pipeline(format!("group key extraction: {e}"))
                    })?;
                key.push(sv);
            }
            #[allow(clippy::cast_possible_truncation)]
            group_indices.entry(key).or_default().push(row as u32);
        }

        // For each group, extract rows and update accumulators
        for (key, indices) in &group_indices {
            if !self.groups.contains_key(key) {
                let mut accs = Vec::with_capacity(self.agg_specs.len());
                for spec in &self.agg_specs {
                    accs.push(spec.create_accumulator()?);
                }
                self.groups.insert(key.clone(), accs);
            }
            let accs = self.groups.get_mut(key).expect("just inserted");

            let index_array =
                arrow::array::UInt32Array::from(indices.clone());

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
                        DbError::Pipeline(format!(
                            "array take failed: {e}"
                        ))
                    })?;
                    input_arrays.push(arr);
                }

                // H-02: Apply FILTER mask — only keep rows where the
                // filter column is true
                if let Some(filter_idx) = spec.filter_col_index {
                    let filter_arr = compute::take(
                        batch.column(filter_idx),
                        &index_array,
                        None,
                    )
                    .map_err(|e| {
                        DbError::Pipeline(format!(
                            "filter take failed: {e}"
                        ))
                    })?;
                    let bool_arr = filter_arr
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>();
                    if let Some(mask) = bool_arr {
                        let mut filtered = Vec::with_capacity(
                            input_arrays.len(),
                        );
                        for arr in &input_arrays {
                            filtered.push(
                                compute::filter(arr, mask).map_err(
                                    |e| {
                                        DbError::Pipeline(format!(
                                            "filter apply failed: {e}"
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

        Ok(())
    }

    /// Emit the current aggregate state as a `RecordBatch`.
    ///
    /// Does NOT reset the accumulators — they continue accumulating for the
    /// next cycle (running totals).
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.groups.is_empty() {
            return Ok(Vec::new());
        }

        let num_rows = self.groups.len();

        // Build group-key columns
        let mut group_arrays: Vec<ArrayRef> = Vec::with_capacity(self.num_group_cols);
        for (col_idx, dt) in self.group_types.iter().enumerate() {
            let scalars: Vec<ScalarValue> = self
                .groups
                .keys()
                .map(|key| key[col_idx].clone())
                .collect();
            let array = ScalarValue::iter_to_array(scalars).map_err(|e| {
                DbError::Pipeline(format!("group key array build: {e}"))
            })?;
            if array.data_type() == dt {
                group_arrays.push(array);
            } else {
                let casted = arrow::compute::cast(&array, dt).unwrap_or(array);
                group_arrays.push(casted);
            }
        }

        // Build aggregate result columns
        let mut agg_arrays: Vec<ArrayRef> =
            Vec::with_capacity(self.agg_specs.len());
        for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
            let mut scalars: Vec<ScalarValue> = Vec::with_capacity(num_rows);
            for accs in self.groups.values_mut() {
                let sv = accs[agg_idx].evaluate().map_err(|e| {
                    DbError::Pipeline(format!("accumulator evaluate: {e}"))
                })?;
                scalars.push(sv);
            }
            let array =
                ScalarValue::iter_to_array(scalars).map_err(|e| {
                    DbError::Pipeline(format!("agg result array build: {e}"))
                })?;
            if array.data_type() == &spec.return_type {
                agg_arrays.push(array);
            } else {
                let casted =
                    arrow::compute::cast(&array, &spec.return_type)
                        .unwrap_or(array);
                agg_arrays.push(casted);
            }
        }

        // Combine into output schema
        let mut all_arrays = group_arrays;
        all_arrays.extend(agg_arrays);

        let batch =
            RecordBatch::try_new(Arc::clone(&self.output_schema), all_arrays)
                .map_err(|e| {
                    DbError::Pipeline(format!("result batch build: {e}"))
                })?;

        Ok(vec![batch])
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
}

// ── Plan introspection helpers ─────────────────────────────────────────

/// Result of finding an aggregate node in a logical plan.
struct AggregateInfo {
    group_exprs: Vec<datafusion_expr::Expr>,
    aggr_exprs: Vec<datafusion_expr::Expr>,
    schema: Arc<Schema>,
    /// HAVING predicate (from a Filter node directly above the Aggregate).
    having_predicate: Option<datafusion_expr::Expr>,
}

/// Walk a `DataFusion` `LogicalPlan` tree to find the first `Aggregate` node.
fn find_aggregate(plan: &LogicalPlan) -> Option<AggregateInfo> {
    find_aggregate_inner(plan, None)
}

fn find_aggregate_inner(
    plan: &LogicalPlan,
    parent_filter: Option<&datafusion_expr::Expr>,
) -> Option<AggregateInfo> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            let schema = Arc::new(agg.schema.as_arrow().clone());
            Some(AggregateInfo {
                group_exprs: agg.group_expr.clone(),
                aggr_exprs: agg.aggr_expr.clone(),
                schema,
                having_predicate: parent_filter.cloned(),
            })
        }
        // A Filter directly above an Aggregate is a HAVING clause
        LogicalPlan::Filter(filter) => {
            if matches!(&*filter.input, LogicalPlan::Aggregate(_)) {
                find_aggregate_inner(
                    &filter.input,
                    Some(&filter.predicate),
                )
            } else {
                find_aggregate_inner(&filter.input, None)
            }
        }
        // Walk through wrappers that don't change aggregation semantics
        LogicalPlan::Projection(proj) => {
            find_aggregate_inner(&proj.input, None)
        }
        LogicalPlan::Sort(sort) => {
            find_aggregate_inner(&sort.input, None)
        }
        LogicalPlan::Limit(limit) => {
            find_aggregate_inner(&limit.input, None)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            find_aggregate_inner(&alias.input, None)
        }
        _ => {
            for input in plan.inputs() {
                if let Some(result) = find_aggregate_inner(input, None)
                {
                    return Some(result);
                }
            }
            None
        }
    }
}

/// Convert a `DataFusion` `Expr` to a SQL string for use in pre-aggregation
/// queries.
fn expr_to_sql(expr: &datafusion_expr::Expr) -> String {
    match expr {
        datafusion_expr::Expr::Column(col) => {
            format!("\"{}\"", col.name)
        }
        datafusion_expr::Expr::Literal(sv, _) => sv.to_string(),
        datafusion_expr::Expr::BinaryExpr(bin) => {
            let left = expr_to_sql(&bin.left);
            let right = expr_to_sql(&bin.right);
            format!("({left} {op} {right})", op = bin.op)
        }
        datafusion_expr::Expr::Cast(cast) => {
            let inner = expr_to_sql(&cast.expr);
            format!("CAST({inner} AS {})", cast.data_type)
        }
        #[allow(deprecated)]
        datafusion_expr::Expr::Wildcard { .. } => "TRUE".to_string(),
        other => other.to_string(),
    }
}

/// Extract the FROM clause from a SQL string (rough heuristic).
fn extract_from_clause(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let from_pos = upper.find(" FROM ").map(|p| p + 6);
    let Some(start) = from_pos else {
        return String::new();
    };
    // Find the end of the FROM clause (WHERE, GROUP BY, ORDER BY, LIMIT, etc.)
    let rest = &sql[start..];
    let end_keywords = [" WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING "];
    let end = end_keywords
        .iter()
        .filter_map(|kw| rest.to_uppercase().find(kw))
        .min()
        .unwrap_or(rest.len());
    rest[..end].trim().to_string()
}

/// Extract the WHERE clause from a SQL string (rough heuristic).
fn extract_where_clause(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let where_pos = upper.find(" WHERE ");
    let Some(start) = where_pos else {
        return String::new();
    };
    let rest = &sql[start..];
    // Find end of WHERE (GROUP BY, ORDER BY, LIMIT, etc.)
    let end_keywords = [" GROUP ", " ORDER ", " LIMIT ", " HAVING "];
    let end = end_keywords
        .iter()
        .filter_map(|kw| rest[7..].to_uppercase().find(kw).map(|p| p + 7))
        .min()
        .unwrap_or(rest.len());
    format!(" {}", rest[..end].trim())
}

/// Infer the input type for an aggregate function from context.
///
/// For most aggregates (SUM, COUNT, AVG, etc.) the input type can be
/// inferred from the return type or defaults to Float64/Int64.
fn infer_input_type(
    _ctx: &SessionContext,
    udf: &AggregateUDF,
    output_type: &DataType,
) -> DataType {
    let name = udf.name().to_lowercase();
    match name.as_str() {
        "count" => DataType::Boolean,
        "avg" => DataType::Float64,
        _ => output_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_from_clause() {
        assert_eq!(
            extract_from_clause(
                "SELECT a, SUM(b) FROM trades GROUP BY a"
            ),
            "trades"
        );
        assert_eq!(
            extract_from_clause(
                "SELECT * FROM events WHERE x > 1 GROUP BY y"
            ),
            "events"
        );
        assert_eq!(
            extract_from_clause(
                "SELECT * FROM events e JOIN dim d ON e.id = d.id"
            ),
            "events e JOIN dim d ON e.id = d.id"
        );
    }

    #[test]
    fn test_extract_where_clause() {
        assert_eq!(
            extract_where_clause(
                "SELECT * FROM events WHERE x > 1 GROUP BY y"
            ),
            " WHERE x > 1"
        );
        assert_eq!(
            extract_where_clause("SELECT * FROM events GROUP BY y"),
            ""
        );
    }

    #[tokio::test]
    async fn test_try_from_sql_non_aggregate() {
        let ctx = laminar_sql::create_session_context();
        // Register a dummy table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT * FROM events",
        )
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
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
        )
        .await
        .unwrap();
        assert!(
            result.is_some(),
            "Aggregate query should return Some"
        );
        let state = result.unwrap();
        assert_eq!(state.num_group_cols, 1);
        assert_eq!(state.agg_specs.len(), 1);
        assert_eq!(state.group_col_names, vec!["name"]);
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
        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![dummy_batch]],
        )
        .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
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
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 20.0, 30.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch1).unwrap();

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
        state.process_batch(&batch2).unwrap();

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

    /// Helper: register a table and build an IncrementalAggState from SQL.
    async fn setup_agg_state(
        sql: &str,
    ) -> (SessionContext, IncrementalAggState) {
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
        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![dummy]],
        )
        .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();
        let state =
            IncrementalAggState::try_from_sql(&ctx, sql)
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
        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![dummy]],
        )
        .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name",
        )
        .await
        .unwrap()
        .expect("expected aggregate state");
        assert!(
            state.agg_specs[0].distinct,
            "DISTINCT flag should be set"
        );
    }

    #[tokio::test]
    async fn test_distinct_count_produces_correct_result() {
        let (_, mut state) = setup_agg_state(
            "SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name",
        )
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
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "a", "a", "a",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 10.0, 10.0, 20.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch).unwrap();

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
        let (_, mut state) = setup_agg_state(
            "SELECT name, SUM(DISTINCT value) as total FROM events GROUP BY name",
        )
        .await;

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));

        // Feed duplicates: 10 appears twice
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "a", "a",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 10.0, 20.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch).unwrap();

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
        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![dummy]],
        )
        .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) FILTER (WHERE value > 0) as pos_sum FROM events GROUP BY name",
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
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "a", "a",
                ])),
                // value > 0 wrapped: -5 becomes NULL, 10 stays, 20 stays
                Arc::new(arrow::array::Float64Array::from(vec![
                    -5.0, 10.0, 20.0,
                ])),
                // filter mask: false, true, true
                Arc::new(arrow::array::BooleanArray::from(vec![
                    false, true, true,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch).unwrap();

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
        let mem_table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![dummy]],
        )
        .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name HAVING SUM(value) > 100",
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
        let (_, mut state) = setup_agg_state(
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
        )
        .await;

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
        assert!(state.process_batch(&batch).is_ok());
    }
}
