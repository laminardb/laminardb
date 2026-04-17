//! Plan introspection + SQL compilation helpers for aggregate operators.
//!
//! - `find_aggregate` walks a `LogicalPlan` to find the GROUP BY node.
//! - `expr_to_sql` reconstructs a SQL fragment from a `DataFusion` expression.
//! - `CompiledProjection` and the HAVING helpers evaluate precompiled
//!   physical expressions against `RecordBatch`es.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::LogicalPlan;

use crate::error::DbError;

pub(crate) struct AggregateInfo {
    pub(crate) group_exprs: Vec<datafusion_expr::Expr>,
    pub(crate) aggr_exprs: Vec<datafusion_expr::Expr>,
    pub(crate) schema: Arc<Schema>,
    /// Input schema (pre-widening) for resolving aggregate argument types.
    pub(crate) input_schema: Arc<Schema>,
    /// HAVING predicate (from a Filter node directly above the Aggregate).
    pub(crate) having_predicate: Option<datafusion_expr::Expr>,
    /// `DFSchema` of the Aggregate's input (for compiling pre-agg expressions).
    pub(crate) input_df_schema: Arc<DFSchema>,
    /// WHERE predicate from a Filter node below the Aggregate, if any.
    pub(crate) where_predicate: Option<datafusion_expr::Expr>,
}

pub(crate) fn find_aggregate(plan: &LogicalPlan) -> Option<AggregateInfo> {
    find_aggregate_inner(plan, None)
}

fn find_aggregate_inner(
    plan: &LogicalPlan,
    parent_filter: Option<&datafusion_expr::Expr>,
) -> Option<AggregateInfo> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            let schema = Arc::new(agg.schema.as_arrow().clone());
            let input_schema = Arc::new(agg.input.schema().as_arrow().clone());
            let input_df_schema = Arc::clone(agg.input.schema());
            let where_predicate = extract_where_predicate(&agg.input);
            Some(AggregateInfo {
                group_exprs: agg.group_expr.clone(),
                aggr_exprs: agg.aggr_expr.clone(),
                schema,
                input_schema,
                having_predicate: parent_filter.cloned(),
                input_df_schema,
                where_predicate,
            })
        }
        // A Filter directly above an Aggregate is a HAVING clause
        LogicalPlan::Filter(filter) => {
            if matches!(&*filter.input, LogicalPlan::Aggregate(_)) {
                find_aggregate_inner(&filter.input, Some(&filter.predicate))
            } else {
                find_aggregate_inner(&filter.input, None)
            }
        }
        // Walk through wrappers that don't change aggregation semantics
        LogicalPlan::Projection(proj) => find_aggregate_inner(&proj.input, None),
        LogicalPlan::Sort(sort) => find_aggregate_inner(&sort.input, None),
        LogicalPlan::Limit(limit) => find_aggregate_inner(&limit.input, None),
        LogicalPlan::SubqueryAlias(alias) => find_aggregate_inner(&alias.input, None),
        _ => {
            for input in plan.inputs() {
                if let Some(result) = find_aggregate_inner(input, None) {
                    return Some(result);
                }
            }
            None
        }
    }
}

fn extract_where_predicate(plan: &LogicalPlan) -> Option<datafusion_expr::Expr> {
    match plan {
        LogicalPlan::Filter(f) => Some(f.predicate.clone()),
        LogicalPlan::Projection(p) => extract_where_predicate(&p.input),
        LogicalPlan::Sort(s) => extract_where_predicate(&s.input),
        LogicalPlan::Limit(l) => extract_where_predicate(&l.input),
        LogicalPlan::SubqueryAlias(a) => extract_where_predicate(&a.input),
        _ => None,
    }
}

fn scalar_value_to_sql(sv: &ScalarValue) -> String {
    match sv {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::Null
        | ScalarValue::Boolean(None) => "NULL".to_string(),
        ScalarValue::Boolean(Some(b)) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        ScalarValue::IntervalDayTime(Some(v)) => {
            let mut parts = Vec::new();
            if v.days != 0 {
                parts.push(format!("{} days", v.days));
            }
            if v.milliseconds != 0 || parts.is_empty() {
                let abs_ms = v.milliseconds.unsigned_abs();
                let secs = abs_ms / 1000;
                let frac = abs_ms % 1000;
                let sign = if v.milliseconds < 0 { "-" } else { "" };
                if frac == 0 {
                    parts.push(format!("{sign}{secs} seconds"));
                } else {
                    parts.push(format!("{sign}{secs}.{frac:03} seconds"));
                }
            }
            format!("INTERVAL '{}'", parts.join(" "))
        }
        ScalarValue::IntervalYearMonth(Some(v)) => {
            let years = v / 12;
            let months = v % 12;
            let mut parts = Vec::new();
            if years != 0 {
                parts.push(format!("{years} years"));
            }
            if months != 0 || parts.is_empty() {
                parts.push(format!("{months} months"));
            }
            format!("INTERVAL '{}'", parts.join(" "))
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            let mut parts = Vec::new();
            if v.months != 0 {
                parts.push(format!("{} months", v.months));
            }
            if v.days != 0 {
                parts.push(format!("{} days", v.days));
            }
            let nanos = v.nanoseconds;
            if nanos != 0 || parts.is_empty() {
                let abs_ns = nanos.unsigned_abs();
                let secs = abs_ns / 1_000_000_000;
                let remainder_ns = abs_ns % 1_000_000_000;
                let sign = if nanos < 0 { "-" } else { "" };
                if remainder_ns == 0 {
                    parts.push(format!("{sign}{secs} seconds"));
                } else {
                    let millis = remainder_ns / 1_000_000;
                    parts.push(format!("{sign}{secs}.{millis:03} seconds"));
                }
            }
            format!("INTERVAL '{}'", parts.join(" "))
        }
        _ => sv.to_string(),
    }
}

fn case_to_sql(case: &datafusion_expr::expr::Case) -> String {
    use std::fmt::Write;
    let mut sql = String::from("CASE");
    if let Some(operand) = &case.expr {
        let _ = write!(sql, " {}", expr_to_sql(operand));
    }
    for (when_expr, then_expr) in &case.when_then_expr {
        let _ = write!(
            sql,
            " WHEN {} THEN {}",
            expr_to_sql(when_expr),
            expr_to_sql(then_expr)
        );
    }
    if let Some(else_expr) = &case.else_expr {
        let _ = write!(sql, " ELSE {}", expr_to_sql(else_expr));
    }
    sql.push_str(" END");
    sql
}

pub(crate) fn expr_to_sql(expr: &datafusion_expr::Expr) -> String {
    use datafusion_expr::Expr;
    match expr {
        Expr::Column(col) => format!("\"{}\"", col.name),
        Expr::Literal(sv, _) => scalar_value_to_sql(sv),
        Expr::Alias(alias) => expr_to_sql(&alias.expr),
        Expr::BinaryExpr(bin) => {
            let left = expr_to_sql(&bin.left);
            let right = expr_to_sql(&bin.right);
            format!("({left} {op} {right})", op = bin.op)
        }
        Expr::Cast(cast) => {
            let inner = expr_to_sql(&cast.expr);
            format!("CAST({inner} AS {})", cast.data_type)
        }
        Expr::TryCast(cast) => {
            let inner = expr_to_sql(&cast.expr);
            format!("TRY_CAST({inner} AS {})", cast.data_type)
        }
        Expr::ScalarFunction(func) => {
            let args: Vec<String> = func.args.iter().map(expr_to_sql).collect();
            format!("{}({})", func.func.name(), args.join(", "))
        }
        Expr::AggregateFunction(agg) => {
            let name = agg.func.name();
            let args: Vec<String> = agg.params.args.iter().map(expr_to_sql).collect();
            if agg.params.distinct {
                format!("{name}(DISTINCT {})", args.join(", "))
            } else {
                format!("{name}({})", args.join(", "))
            }
        }
        Expr::Case(case) => case_to_sql(case),
        Expr::Not(inner) => format!("(NOT {})", expr_to_sql(inner)),
        Expr::Negative(inner) => format!("(-{})", expr_to_sql(inner)),
        Expr::IsNull(inner) => {
            format!("({} IS NULL)", expr_to_sql(inner))
        }
        Expr::IsNotNull(inner) => {
            format!("({} IS NOT NULL)", expr_to_sql(inner))
        }
        Expr::IsTrue(inner) => {
            format!("({} IS TRUE)", expr_to_sql(inner))
        }
        Expr::IsFalse(inner) => {
            format!("({} IS FALSE)", expr_to_sql(inner))
        }
        Expr::IsNotTrue(inner) => {
            format!("({} IS NOT TRUE)", expr_to_sql(inner))
        }
        Expr::IsNotFalse(inner) => {
            format!("({} IS NOT FALSE)", expr_to_sql(inner))
        }
        Expr::Between(between) => {
            let e = expr_to_sql(&between.expr);
            let low = expr_to_sql(&between.low);
            let high = expr_to_sql(&between.high);
            let not = if between.negated { " NOT" } else { "" };
            format!("({e}{not} BETWEEN {low} AND {high})")
        }
        Expr::InList(in_list) => {
            let e = expr_to_sql(&in_list.expr);
            let items: Vec<String> = in_list.list.iter().map(expr_to_sql).collect();
            let not = if in_list.negated { " NOT" } else { "" };
            format!("({e}{not} IN ({}))", items.join(", "))
        }
        Expr::Like(like) => {
            let e = expr_to_sql(&like.expr);
            let pat = expr_to_sql(&like.pattern);
            let kw = if like.case_insensitive {
                "ILIKE"
            } else {
                "LIKE"
            };
            let not = if like.negated { " NOT" } else { "" };
            if let Some(esc) = &like.escape_char {
                format!("({e}{not} {kw} {pat} ESCAPE '{esc}')")
            } else {
                format!("({e}{not} {kw} {pat})")
            }
        }
        #[allow(deprecated)]
        Expr::Wildcard { .. } => "TRUE".to_string(),
        other => other.to_string(),
    }
}

/// Pre-compiled projection for evaluating pre-agg expressions without SQL.
///
/// Replaces the `ctx.sql(pre_agg_sql).await.collect().await` hot-path call
/// with direct `PhysicalExpr::evaluate()` on the source batch, eliminating
/// SQL parsing, logical planning, physical planning, and `MemTable` overhead.
pub(crate) struct CompiledProjection {
    #[allow(dead_code)]
    pub(crate) source_table: String,
    pub(crate) exprs: Vec<Arc<dyn PhysicalExpr>>,
    pub(crate) filter: Option<Arc<dyn PhysicalExpr>>,
    pub(crate) output_schema: SchemaRef,
}

impl CompiledProjection {
    /// Evaluate the projection against a source batch.
    ///
    /// Applies the WHERE filter (if any), then evaluates each expression
    /// to produce the projected output batch.
    pub(crate) fn evaluate(&self, batch: &RecordBatch) -> Result<RecordBatch, DbError> {
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }

        // Apply WHERE filter first
        let filtered = if let Some(ref filter) = self.filter {
            let result = filter
                .evaluate(batch)
                .map_err(|e| DbError::Pipeline(format!("WHERE filter evaluate: {e}")))?;
            let mask = result
                .into_array(batch.num_rows())
                .map_err(|e| DbError::Pipeline(format!("WHERE filter to array: {e}")))?;
            let bool_arr = mask
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| DbError::Pipeline("WHERE filter not boolean".into()))?;
            arrow::compute::filter_record_batch(batch, bool_arr)
                .map_err(|e| DbError::Pipeline(format!("WHERE filter: {e}")))?
        } else {
            batch.clone()
        };

        if filtered.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }

        // Evaluate each projection expression
        let mut arrays = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            let result = expr
                .evaluate(&filtered)
                .map_err(|e| DbError::Pipeline(format!("projection evaluate: {e}")))?;
            let arr = result
                .into_array(filtered.num_rows())
                .map_err(|e| DbError::Pipeline(format!("projection to array: {e}")))?;
            arrays.push(arr);
        }

        RecordBatch::try_new(Arc::clone(&self.output_schema), arrays)
            .map_err(|e| DbError::Pipeline(format!("projection batch build: {e}")))
    }
}

pub(crate) fn apply_compiled_having(
    batches: &[RecordBatch],
    having_filter: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<RecordBatch>, DbError> {
    let mut result = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let mask_result = having_filter
            .evaluate(batch)
            .map_err(|e| DbError::Pipeline(format!("HAVING evaluate: {e}")))?;
        let mask = mask_result
            .into_array(batch.num_rows())
            .map_err(|e| DbError::Pipeline(format!("HAVING to array: {e}")))?;
        let bool_arr = mask
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| DbError::Pipeline("HAVING filter not boolean".into()))?;
        let filtered = arrow::compute::filter_record_batch(batch, bool_arr)
            .map_err(|e| DbError::Pipeline(format!("HAVING filter: {e}")))?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }
    Ok(result)
}

pub(crate) fn compile_having_filter(
    ctx: &SessionContext,
    having_predicate: Option<&datafusion_expr::Expr>,
    output_schema: &SchemaRef,
) -> Option<Arc<dyn PhysicalExpr>> {
    let having_pred = having_predicate?;
    let df_schema = DFSchema::try_from(output_schema.as_ref().clone()).ok()?;
    let state = ctx.state();
    let props = state.execution_props();
    create_physical_expr(having_pred, &df_schema, props).ok()
}

pub(crate) struct SqlClauses {
    pub(crate) from_clause: String,
    pub(crate) where_clause: String,
}

pub(crate) fn extract_clauses(sql: &str) -> SqlClauses {
    if let Ok(clauses) = extract_clauses_ast(sql) {
        return clauses;
    }
    // Fallback: heuristic extraction for non-standard SQL
    SqlClauses {
        from_clause: extract_from_clause_heuristic(sql),
        where_clause: extract_where_clause_heuristic(sql),
    }
}

fn extract_clauses_ast(sql: &str) -> Result<SqlClauses, DbError> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| DbError::Pipeline(format!("SQL parse error: {e}")))?;

    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| DbError::Pipeline("empty SQL statement".to_string()))?;

    let sqlparser::ast::Statement::Query(query) = stmt else {
        return Err(DbError::Pipeline("expected SELECT statement".to_string()));
    };

    let sqlparser::ast::SetExpr::Select(select) = *query.body else {
        return Err(DbError::Pipeline("expected simple SELECT".to_string()));
    };

    let from_clause = select
        .from
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = select
        .selection
        .as_ref()
        .map(|expr| format!(" WHERE {expr}"))
        .unwrap_or_default();

    Ok(SqlClauses {
        from_clause,
        where_clause,
    })
}

fn extract_from_clause_heuristic(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let from_pos = upper.find(" FROM ").map(|p| p + 6);
    let Some(start) = from_pos else {
        return String::new();
    };
    let rest = &sql[start..];
    let end_keywords = [" WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING "];
    let end = end_keywords
        .iter()
        .filter_map(|kw| rest.to_uppercase().find(kw))
        .min()
        .unwrap_or(rest.len());
    rest[..end].trim().to_string()
}

fn extract_where_clause_heuristic(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let where_pos = upper.find(" WHERE ");
    let Some(start) = where_pos else {
        return String::new();
    };
    let rest = &sql[start..];
    let end_keywords = [" GROUP ", " ORDER ", " LIMIT ", " HAVING "];
    let end = end_keywords
        .iter()
        .filter_map(|kw| rest[7..].to_uppercase().find(kw).map(|p| p + 7))
        .min()
        .unwrap_or(rest.len());
    format!(" {}", rest[..end].trim())
}

pub(crate) fn resolve_expr_type(
    expr: &datafusion_expr::Expr,
    input_schema: &Schema,
    fallback_type: &DataType,
) -> DataType {
    match expr {
        datafusion_expr::Expr::Column(col) => input_schema
            .field_with_name(&col.name)
            .map_or_else(|_| fallback_type.clone(), |f| f.data_type().clone()),
        datafusion_expr::Expr::Literal(sv, _) => sv.data_type(),
        datafusion_expr::Expr::Cast(cast) => cast.data_type.clone(),
        datafusion_expr::Expr::TryCast(cast) => cast.data_type.clone(),
        datafusion_expr::Expr::BinaryExpr(bin) => {
            // For arithmetic, the result type is typically the wider
            // of the two operands. Resolve the left side as a
            // reasonable approximation.
            resolve_expr_type(&bin.left, input_schema, fallback_type)
        }
        datafusion_expr::Expr::ScalarFunction(func) => {
            // Try to get return type from input types
            let arg_types: Vec<DataType> = func
                .args
                .iter()
                .map(|a| resolve_expr_type(a, input_schema, fallback_type))
                .collect();
            func.func
                .return_type(&arg_types)
                .unwrap_or_else(|_| fallback_type.clone())
        }
        #[allow(deprecated)]
        datafusion_expr::Expr::Wildcard { .. } => DataType::Boolean,
        _ => fallback_type.clone(),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_day_time_seconds_only() {
        use arrow::datatypes::IntervalDayTime;
        let sv = ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(0, 10_000)));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '10 seconds'");
    }

    #[test]
    fn test_interval_day_time_days_only() {
        use arrow::datatypes::IntervalDayTime;
        let sv = ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(3, 0)));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '3 days'");
    }

    #[test]
    fn test_interval_day_time_mixed() {
        use arrow::datatypes::IntervalDayTime;
        let sv = ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(1, 5_500)));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '1 days 5.500 seconds'");
    }

    #[test]
    fn test_interval_year_month() {
        let sv = ScalarValue::IntervalYearMonth(Some(15));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '1 years 3 months'");
    }

    #[test]
    fn test_interval_month_day_nano() {
        use arrow::datatypes::IntervalMonthDayNano;
        let sv =
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(2, 1, 3_000_000_000)));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '2 months 1 days 3 seconds'");
    }
}
