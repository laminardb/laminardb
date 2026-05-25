//! SQL analysis and join detection utilities.

use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    SelectItem, SetExpr, Statement, TableFactor, WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan};

use laminar_ai::{BackendKind, ModelRegistry, Task};
use laminar_sql::parser::join_parser::analyze_joins;

use crate::error::DbError;
use crate::operator::window_frame::MomentFn;
#[cfg(test)]
use laminar_sql::parser::{EmitClause, EmitStrategy as SqlEmitStrategy};
use laminar_sql::translator::{
    AsofJoinTranslatorConfig, JoinOperatorConfig, StreamJoinConfig, StreamJoinType,
    TemporalJoinTranslatorConfig, WindowOperatorConfig, WindowType,
};

// ---------------------------------------------------------------------------
// Emit conversion
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) fn sql_emit_to_core(
    s: &SqlEmitStrategy,
) -> laminar_core::operator::window::EmitStrategy {
    use laminar_core::operator::window::EmitStrategy as CoreEmit;
    match s {
        SqlEmitStrategy::OnWatermark => CoreEmit::OnWatermark,
        SqlEmitStrategy::OnWindowClose => CoreEmit::OnWindowClose,
        SqlEmitStrategy::Periodic(d) => CoreEmit::Periodic(*d),
        SqlEmitStrategy::OnUpdate => CoreEmit::OnUpdate,
        SqlEmitStrategy::Changelog => CoreEmit::Changelog,
        SqlEmitStrategy::FinalOnly => CoreEmit::Final,
    }
}

#[cfg(test)]
pub(crate) fn emit_clause_to_core(
    clause: &EmitClause,
) -> Result<laminar_core::operator::window::EmitStrategy, laminar_sql::parser::ParseError> {
    let sql_strategy = clause.to_emit_strategy()?;
    Ok(sql_emit_to_core(&sql_strategy))
}

// ---------------------------------------------------------------------------
// Table reference extraction
// ---------------------------------------------------------------------------

/// Returns a deduplicated set of table names from FROM/JOIN clauses.
/// Not deduplicated by occurrence count -- for self-join detection use
/// [`single_source_table`] instead.
pub(crate) fn extract_table_references(sql: &str) -> FxHashSet<String> {
    let mut tables = FxHashSet::default();
    let dialect = GenericDialect {};
    if let Ok(statements) = Parser::parse_sql(&dialect, sql) {
        for stmt in &statements {
            if let Statement::Query(query) = stmt {
                collect_tables_from_set_expr(query.body.as_ref(), &mut tables);
            }
        }
    }
    // sqlparser drops TEMPORAL PROBE JOIN; merge its tables from the detector.
    if let (Some(config), _) = detect_temporal_probe_query(sql) {
        tables.insert(config.left_table.clone());
        tables.insert(config.right_table.clone());
    }
    tables
}

/// Unlike [`extract_table_references`] (which deduplicates), this counts every
/// FROM/JOIN table occurrence. A self-join like `events e1 JOIN events e2`
/// returns `None` because there are two occurrences even though the base table
/// name is the same.
pub(crate) fn single_source_table(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    let mut tables = Vec::new();
    for stmt in &statements {
        if let Statement::Query(query) = stmt {
            collect_tables_counting(query.body.as_ref(), &mut tables);
        }
    }
    if tables.len() == 1 {
        tables.into_iter().next()
    } else {
        None
    }
}

fn collect_tables_from_set_expr(set_expr: &SetExpr, tables: &mut FxHashSet<String>) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_tables_from_factor(&table_with_joins.relation, tables);
                for join in &table_with_joins.joins {
                    collect_tables_from_factor(&join.relation, tables);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_tables_from_set_expr(left.as_ref(), tables);
            collect_tables_from_set_expr(right.as_ref(), tables);
        }
        SetExpr::Query(query) => {
            collect_tables_from_set_expr(query.body.as_ref(), tables);
        }
        _ => {}
    }
}

fn collect_tables_from_factor(factor: &TableFactor, tables: &mut FxHashSet<String>) {
    match factor {
        TableFactor::Table { name, args, .. } => {
            tables.insert(resolve_tvf_source(name, args.as_ref()));
        }
        TableFactor::Derived { subquery, .. } => {
            collect_tables_from_set_expr(subquery.body.as_ref(), tables);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_tables_from_factor(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                collect_tables_from_factor(&join.relation, tables);
            }
        }
        _ => {}
    }
}

/// Not deduplicated -- collects every occurrence for counting.
fn collect_tables_counting(set_expr: &SetExpr, tables: &mut Vec<String>) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_factor_counting(&table_with_joins.relation, tables);
                for join in &table_with_joins.joins {
                    collect_factor_counting(&join.relation, tables);
                }
            }
            // UNNEST in the projection expands rows just like a FROM-clause
            // UNNEST; the single-source fast path can't, so force the full plan.
            if projection_has_unnest(&select.projection) {
                tables.push("\u{0}non_table_factor".to_string());
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_tables_counting(left.as_ref(), tables);
            collect_tables_counting(right.as_ref(), tables);
        }
        SetExpr::Query(query) => {
            collect_tables_counting(query.body.as_ref(), tables);
        }
        _ => {}
    }
}

/// True if any projection item is (or wraps) an `UNNEST(..)`. Checked on the
/// serialized item, so it's robust to the parser's UNNEST representation; a
/// false positive only forces the safe full-plan path.
fn projection_has_unnest(items: &[SelectItem]) -> bool {
    items
        .iter()
        .any(|item| item.to_string().to_ascii_lowercase().contains("unnest("))
}

fn collect_factor_counting(factor: &TableFactor, tables: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, args, .. } => {
            tables.push(resolve_tvf_source(name, args.as_ref()));
        }
        TableFactor::Derived { subquery, .. } => {
            collect_tables_counting(subquery.body.as_ref(), tables);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_factor_counting(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                collect_factor_counting(&join.relation, tables);
            }
        }
        // A non-table factor (lateral UNNEST, TVF, ...) can't be evaluated by
        // the single-source fast path; count it so the query uses the full
        // per-cycle plan instead of silently dropping the factor.
        _ => tables.push("\u{0}non_table_factor".to_string()),
    }
}

/// Rewrite `ASOF JOIN … MATCH_CONDITION(..)` to a `LEFT JOIN … ON ..` for
/// schema resolution: `DataFusion` can't lower `AsOf`, and the match condition
/// picks the matching right row at runtime, not the columns. Left keeps the
/// right side nullable. `None` if there is no ASOF join.
pub(crate) fn rewrite_asof_joins_for_planning(sql: &str) -> Option<String> {
    use sqlparser::ast::{JoinConstraint, JoinOperator};

    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).ok()?;
    let mut changed = false;
    for stmt in &mut stmts {
        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body.as_mut() {
                for twj in &mut select.from {
                    for join in &mut twj.joins {
                        if !matches!(join.join_operator, JoinOperator::AsOf { .. }) {
                            continue;
                        }
                        let op = std::mem::replace(
                            &mut join.join_operator,
                            JoinOperator::Inner(JoinConstraint::None),
                        );
                        if let JoinOperator::AsOf { constraint, .. } = op {
                            join.join_operator = JoinOperator::LeftOuter(constraint);
                            changed = true;
                        }
                    }
                }
            }
        }
    }
    changed.then(|| {
        stmts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    })
}

/// Resolve the source table name from a `TableFactor::Table`.
///
/// sqlparser parses `FROM TUMBLE(events, ts, ...)` as `TableFactor::Table`
/// with `name = "TUMBLE"` and `args = Some([events, ts, ...])`. For window
/// TVFs the first argument is the actual source table — return that instead
/// of the function name.
fn resolve_tvf_source(
    name: &sqlparser::ast::ObjectName,
    args: Option<&sqlparser::ast::TableFunctionArgs>,
) -> String {
    let name_str = name.to_string();
    // Handle schema-qualified names like "schema.TUMBLE"
    let base_name = name_str.rsplit('.').next().unwrap_or(&name_str);
    if let Some(tfa) = args {
        if is_window_tvf(base_name) {
            if let Some(source) = first_ident_arg(&tfa.args) {
                return source;
            }
        }
    }
    name_str
}

fn is_window_tvf(name: &str) -> bool {
    name.eq_ignore_ascii_case("TUMBLE")
        || name.eq_ignore_ascii_case("HOP")
        || name.eq_ignore_ascii_case("SESSION")
        || name.eq_ignore_ascii_case("SLIDE")
}

/// Extract the first bare identifier from a function arg list.
fn first_ident_arg(args: &[FunctionArg]) -> Option<String> {
    match args.first()? {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => Some(id.value.clone()),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
            let mut buf = String::new();
            for (i, part) in parts.iter().enumerate() {
                if i > 0 {
                    buf.push('.');
                }
                buf.push_str(&part.value);
            }
            Some(buf)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Compiled post-projection
// ---------------------------------------------------------------------------

/// Lazily compiled post-join projection for ASOF/temporal queries.
///
/// Compiled from the projection SQL (e.g., `SELECT a, b AS alias FROM __asof_tmp`)
/// on first execution when the join output schema is known.
pub(crate) struct CompiledPostProjection {
    pub(crate) exprs: Vec<Arc<dyn PhysicalExpr>>,
    pub(crate) output_schema: SchemaRef,
}

impl std::fmt::Debug for CompiledPostProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledPostProjection")
            .field("output_schema", &self.output_schema)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Projection / filter extraction
// ---------------------------------------------------------------------------

pub(crate) struct ProjectionFilterInfo {
    pub(crate) proj_exprs: Vec<datafusion_expr::Expr>,
    pub(crate) filter_predicate: Option<datafusion_expr::Expr>,
    pub(crate) input_df_schema: Arc<datafusion_common::DFSchema>,
}

/// Returns `Some` only for plans of the shape
/// `Projection? -> Filter? -> TableScan` (with optional `SubqueryAlias` wrappers).
pub(crate) fn extract_projection_filter(plan: &LogicalPlan) -> Option<ProjectionFilterInfo> {
    match plan {
        LogicalPlan::Projection(proj) => {
            let proj_exprs = proj.expr.clone();
            extract_filter_or_scan(&proj.input).map(|(filter_pred, input_schema, _)| {
                ProjectionFilterInfo {
                    proj_exprs,
                    filter_predicate: filter_pred,
                    input_df_schema: input_schema,
                }
            })
        }
        // No Projection wrapper — check for Filter -> TableScan directly
        _ => match extract_filter_or_scan(plan) {
            Some((filter_pred, input_schema, _)) => {
                // Build identity projection from the scan schema
                let proj_exprs: Vec<datafusion_expr::Expr> = input_schema
                    .fields()
                    .iter()
                    .map(|f| {
                        datafusion_expr::Expr::Column(datafusion_common::Column::new_unqualified(
                            f.name(),
                        ))
                    })
                    .collect();
                Some(ProjectionFilterInfo {
                    proj_exprs,
                    filter_predicate: filter_pred,
                    input_df_schema: input_schema,
                })
            }
            None => None,
        },
    }
}

fn extract_filter_or_scan(
    plan: &LogicalPlan,
) -> Option<(
    Option<datafusion_expr::Expr>,
    Arc<datafusion_common::DFSchema>,
    String,
)> {
    match plan {
        LogicalPlan::Filter(filter) => match &*filter.input {
            LogicalPlan::TableScan(scan) => Some((
                Some(filter.predicate.clone()),
                Arc::clone(filter.input.schema()),
                scan.table_name.to_string(),
            )),
            LogicalPlan::SubqueryAlias(alias) => {
                if let LogicalPlan::TableScan(scan) = &*alias.input {
                    Some((
                        Some(filter.predicate.clone()),
                        Arc::clone(filter.input.schema()),
                        scan.table_name.to_string(),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        },
        LogicalPlan::TableScan(scan) => {
            Some((None, Arc::clone(plan.schema()), scan.table_name.to_string()))
        }
        LogicalPlan::SubqueryAlias(alias) => extract_filter_or_scan(&alias.input),
        _ => None,
    }
}

pub(crate) fn extract_projection_exprs(
    plan: &LogicalPlan,
    input_schema: &SchemaRef,
    ctx: &SessionContext,
) -> Option<(Vec<Arc<dyn PhysicalExpr>>, SchemaRef)> {
    let proj = match plan {
        LogicalPlan::Projection(p) => p,
        LogicalPlan::SubqueryAlias(a) => {
            return extract_projection_exprs(&a.input, input_schema, ctx);
        }
        _ => return None,
    };

    let df_schema = datafusion_common::DFSchema::try_from(input_schema.as_ref().clone()).ok()?;
    let state = ctx.state();
    let exec_props = state.execution_props();

    let mut exprs = Vec::with_capacity(proj.expr.len());
    let mut fields = Vec::with_capacity(proj.expr.len());

    for (i, expr) in proj.expr.iter().enumerate() {
        let phys =
            datafusion::physical_expr::create_physical_expr(expr, &df_schema, exec_props).ok()?;
        let name = proj.schema.field(i).name().clone();
        let dt = phys.data_type(input_schema).ok()?;
        let nullable = phys.nullable(input_schema).unwrap_or(true);
        fields.push(arrow::datatypes::Field::new(name, dt, nullable));
        exprs.push(phys);
    }

    let output_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    Some((exprs, output_schema))
}

// ---------------------------------------------------------------------------
// Window boundary computation
// ---------------------------------------------------------------------------

/// Compute the closed-window boundary given a watermark and window config.
///
/// - **Tumbling**: floor to nearest window boundary.
/// - **Session**: `watermark - gap` (best approximation without session state).
/// - **Sliding**: align to slide interval — the earliest open window starts at
///   `((watermark - size) / slide + 1) * slide`, so data below that threshold
///   belongs only to closed windows.
pub(crate) fn compute_closed_boundary(watermark_ms: i64, config: &WindowOperatorConfig) -> i64 {
    match config.window_type {
        WindowType::Tumbling => {
            let size = i64::try_from(config.size.as_millis()).unwrap_or(i64::MAX);
            if size <= 0 {
                tracing::warn!("tumbling window size is zero or negative, EOWC filtering disabled");
                return watermark_ms;
            }
            // floor((ts - offset) / size) * size + offset, saturating so
            // the i64::MIN initial-watermark sentinel doesn't panic.
            let offset = config.offset_ms;
            let adjusted = watermark_ms.saturating_sub(offset);
            let floored = adjusted.div_euclid(size).saturating_mul(size);
            floored.saturating_add(offset)
        }
        WindowType::Session => {
            let gap = config
                .gap
                .map_or(0, |g| i64::try_from(g.as_millis()).unwrap_or(i64::MAX));
            watermark_ms.saturating_sub(gap)
        }
        WindowType::Sliding => {
            let size = i64::try_from(config.size.as_millis()).unwrap_or(i64::MAX);
            let slide = config
                .slide
                .map_or(size, |s| i64::try_from(s.as_millis()).unwrap_or(i64::MAX));
            if slide <= 0 || size <= 0 {
                tracing::warn!(
                    slide_ms = slide,
                    size_ms = size,
                    "sliding window size/slide is zero or negative, EOWC filtering disabled"
                );
                return watermark_ms;
            }
            // The earliest open window starts at the first slide-aligned
            // boundary after (watermark - size). Data below that start
            // belongs only to fully closed windows.
            // Account for window offset to match SlidingWindowAssigner.
            let offset = config.offset_ms;
            let base = watermark_ms.saturating_sub(offset).saturating_sub(size);
            let boundary = base
                .div_euclid(slide)
                .saturating_add(1)
                .saturating_mul(slide);
            boundary.saturating_add(offset)
        }
        WindowType::Cumulate => {
            // Cumulate windows share the same epoch alignment as tumbling.
            // A full epoch is closed when watermark >= epoch_end.
            let size = i64::try_from(config.size.as_millis()).unwrap_or(i64::MAX);
            if size <= 0 {
                tracing::warn!("cumulate window size is zero or negative, EOWC filtering disabled");
                return watermark_ms;
            }
            let offset = config.offset_ms;
            let adjusted = watermark_ms.saturating_sub(offset);
            let floored = adjusted.div_euclid(size).saturating_mul(size);
            floored.saturating_add(offset)
        }
    }
}

// ---------------------------------------------------------------------------
// Join detection
// ---------------------------------------------------------------------------

pub(crate) fn detect_asof_query(sql: &str) -> (Option<AsofJoinTranslatorConfig>, Option<String>) {
    // Parse using the streaming parser which understands ASOF syntax
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return (None, None);
    };

    // We need a raw sqlparser Statement::Query to inspect the SELECT AST
    let Some(laminar_sql::parser::StreamingStatement::Standard(stmt)) = statements.first() else {
        return (None, None);
    };

    let Statement::Query(query) = stmt.as_ref() else {
        return (None, None);
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return (None, None);
    };

    let Ok(Some(multi)) = analyze_joins(select) else {
        return (None, None);
    };

    // Find the first ASOF join step
    let Some(asof_analysis) = multi.joins.iter().find(|j| j.is_asof_join) else {
        return (None, None);
    };

    let JoinOperatorConfig::Asof(config) = JoinOperatorConfig::from_analysis(asof_analysis) else {
        return (None, None);
    };

    // Build a projection SQL that rewrites the original SELECT list to reference
    // the flattened __asof_tmp table (no table qualifiers, disambiguated names).
    let projection_sql = build_projection_sql(select, asof_analysis, &config);

    (Some(config), Some(projection_sql))
}

// ---------------------------------------------------------------------------
// AI function detection (plan-time seam)
// ---------------------------------------------------------------------------

/// AI-function detection, plan-time validation, and query planning. Consumed by
/// `add_query` routing in `operator_graph`.
mod ai {
    use super::{
        BackendKind, DbError, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
        ModelRegistry, ObjectName, ObjectNamePart, SelectItem, SetExpr, Statement, TableFactor,
        Task,
    };

    /// The temp table the AI operator registers its enriched batches under for
    /// the residual projection.
    pub(crate) const AI_TMP_TABLE: &str = "__ai_tmp";

    /// A query that contains exactly one `ai_*` call: the detected call plus the
    /// residual projection to run over the operator's enriched output, and the
    /// single source table the operator reads.
    pub(crate) struct AiQueryPlan {
        /// The detected AI call.
        pub call: AiCallSpec,
        /// Residual SQL over [`AI_TMP_TABLE`] with the `ai_*` projection item
        /// rewritten to its alias column.
        pub projection_sql: String,
        /// The source table the AI operator reads from.
        pub source_table: String,
    }

    /// Plan a query for AI routing. Returns `Some` only when the query has
    /// exactly one `ai_*` call, written with an `AS` alias, over a single
    /// (un-joined) source table — the supported shape for v0.1. Multiple AI
    /// calls, a missing alias, or joins return `None` (the caller rejects or
    /// routes normally).
    pub(crate) fn plan_ai_query(sql: &str) -> Option<AiQueryPlan> {
        let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
        let mut statement = match statements.into_iter().next()? {
            laminar_sql::parser::StreamingStatement::Standard(boxed) => *boxed,
            _ => return None,
        };
        let Statement::Query(query) = &mut statement else {
            return None;
        };
        let SetExpr::Select(select) = query.body.as_mut() else {
            return None;
        };
        if select.from.len() != 1 || !select.from[0].joins.is_empty() {
            return None;
        }
        let source_table = match &select.from[0].relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return None,
        };

        // Find the single AI-call projection item.
        let mut found: Option<(usize, AiCallSpec)> = None;
        for (index, item) in select.projection.iter().enumerate() {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => continue,
            };
            if let Some(spec) = ai_call_from_expr(expr, alias) {
                if found.is_some() {
                    return None; // more than one AI call — unsupported in v0.1
                }
                found = Some((index, spec));
            }
        }
        let (index, call) = found?;

        // Rewrite: the AI projection item becomes a plain alias column, and the
        // FROM table becomes the enriched temp table. Reuse the original alias
        // `Ident` (an alias is required) so quoted aliases stay quoted.
        let SelectItem::ExprWithAlias { alias, .. } = &select.projection[index] else {
            return None;
        };
        let alias = alias.clone();
        select.projection[index] = SelectItem::UnnamedExpr(Expr::Identifier(alias));
        if let TableFactor::Table { name, .. } = &mut select.from[0].relation {
            *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(AI_TMP_TABLE))]);
        }
        let projection_sql = statement.to_string();

        Some(AiQueryPlan {
            call,
            projection_sql,
            source_table,
        })
    }

    /// One detected `ai_*(...)` call in a query's SELECT projection.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct AiCallSpec {
        /// The task implied by the function name.
        pub task: Task,
        /// The model named via `model => '<name>'`, or `None` to use the task default.
        pub model: Option<String>,
        /// The candidate label set from `labels => [...]`, if supplied.
        pub labels: Option<Vec<String>>,
        /// The input column name (a simple identifier). Empty if the first
        /// argument was missing or not a plain column — a parse error.
        pub input: String,
        /// The projection alias, if the call was written `... AS <alias>`.
        pub output_alias: Option<String>,
        /// Argument-shape errors found while parsing (bad input, wrong argument
        /// types). Non-empty means the call is malformed; surfaced by
        /// [`validate_ai_calls`] as a build error rather than silently ignored.
        pub parse_errors: Vec<String>,
    }

    /// Detect `ai_*` calls in the top-level SELECT projection of `sql`.
    ///
    /// Only top-level projection calls are recognised in v0.1 (e.g.
    /// `SELECT ai_classify(text, model => 'finbert') AS label FROM s`); an `ai_*`
    /// nested inside another expression or in `WHERE` is left for the marker UDF to
    /// reject. Returns an empty vec if the SQL does not parse or has no AI calls.
    pub(crate) fn detect_ai_functions(sql: &str) -> Vec<AiCallSpec> {
        let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
            return Vec::new();
        };
        let Some(laminar_sql::parser::StreamingStatement::Standard(stmt)) = statements.first()
        else {
            return Vec::new();
        };
        let Statement::Query(query) = stmt.as_ref() else {
            return Vec::new();
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Vec::new();
        };

        let mut calls = Vec::new();
        for item in &select.projection {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => continue,
            };
            if let Some(spec) = ai_call_from_expr(expr, alias) {
                calls.push(spec);
            }
        }
        calls
    }

    /// Extract an [`AiCallSpec`] from a projection expression if it is an `ai_*` call.
    fn ai_call_from_expr(expr: &Expr, alias: Option<String>) -> Option<AiCallSpec> {
        let Expr::Function(func) = expr else {
            return None;
        };
        let task = task_from_ai_function(&func.name.to_string().to_ascii_lowercase())?;
        let FunctionArguments::List(list) = &func.args else {
            return None;
        };

        let mut input: Option<String> = None;
        let mut seen_input = false;
        let mut model: Option<String> = None;
        let mut labels: Option<Vec<String>> = None;
        let mut parse_errors: Vec<String> = Vec::new();

        for arg in &list.args {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(value)) => {
                    if seen_input {
                        parse_errors
                            .push("AI functions take a single positional input column".to_string());
                    } else {
                        seen_input = true;
                        // The operator looks the input up by column name in the
                        // source batch, so only a plain column reference is
                        // supported — an arbitrary expression would fail lookup.
                        match column_name(value) {
                            Some(col) => input = Some(col),
                            None => parse_errors.push(format!(
                                "AI function input must be a simple column reference, got `{value}`"
                            )),
                        }
                    }
                }
                FunctionArg::Named {
                    name,
                    arg: FunctionArgExpr::Expr(value),
                    ..
                } => match name.value.to_ascii_lowercase().as_str() {
                    "model" => match string_literal(value) {
                        Some(s) => model = Some(s),
                        None => parse_errors
                            .push("`model` argument must be a string literal".to_string()),
                    },
                    "labels" => match string_array_literal(value) {
                        Some(v) => labels = Some(v),
                        None => parse_errors.push(
                            "`labels` argument must be an array of string literals".to_string(),
                        ),
                    },
                    other => {
                        parse_errors.push(format!("unsupported AI function argument `{other}`"));
                    }
                },
                other => {
                    parse_errors.push(format!("unsupported AI function argument: {other}"));
                }
            }
        }

        if !seen_input {
            parse_errors
                .push("AI function requires a column reference as its first argument".to_string());
        }

        Some(AiCallSpec {
            task,
            model,
            labels,
            input: input.unwrap_or_default(),
            output_alias: alias,
            parse_errors,
        })
    }

    /// A plain column reference (`col`), which the operator can look up in the
    /// source batch. Anything else (a function call, arithmetic, a qualified
    /// name) is unsupported as AI input in v0.1.
    fn column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        }
    }

    /// Map an `ai_*` function name to its task. Must stay in step with the marker
    /// list in `laminar-sql`'s `ai_udf`.
    fn task_from_ai_function(name: &str) -> Option<Task> {
        match name {
            "ai_classify" => Some(Task::Classify),
            "ai_sentiment" => Some(Task::Sentiment),
            "ai_embed" => Some(Task::Embed),
            "ai_extract" => Some(Task::Extract),
            "ai_complete" => Some(Task::Complete),
            "ai_summarize" => Some(Task::Summarize),
            "ai_translate" => Some(Task::Translate),
            "ai_gen" => Some(Task::Gen),
            _ => None,
        }
    }

    /// A single- or double-quoted string literal value.
    fn string_literal(expr: &Expr) -> Option<String> {
        let Expr::Value(value) = expr else {
            return None;
        };
        match &value.value {
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// An array literal of string values (`['a', 'b']` or `ARRAY['a', 'b']`).
    fn string_array_literal(expr: &Expr) -> Option<Vec<String>> {
        let Expr::Array(array) = expr else {
            return None;
        };
        array.elem.iter().map(string_literal).collect()
    }

    /// Validate every detected AI call against the model registry. Fails the query
    /// at plan time for an unknown model, a model that cannot perform the task, or a
    /// labels-seam violation.
    pub(crate) fn validate_ai_calls(
        registry: &ModelRegistry,
        calls: &[AiCallSpec],
    ) -> Result<(), DbError> {
        for call in calls {
            validate_ai_call(registry, call)?;
        }
        Ok(())
    }

    fn validate_ai_call(registry: &ModelRegistry, call: &AiCallSpec) -> Result<(), DbError> {
        if let Some(err) = call.parse_errors.first() {
            return Err(DbError::InvalidOperation(err.clone()));
        }

        let model_name = match &call.model {
            Some(name) => name.clone(),
            None => registry
                .default_for(call.task)
                .map(str::to_string)
                .ok_or_else(|| {
                    DbError::InvalidOperation(format!(
                        "no model given for task '{}' and no [ai.defaults] default is configured",
                        call.task
                    ))
                })?,
        };

        let entry = registry
            .validate(&model_name, call.task)
            .map_err(|e| DbError::InvalidOperation(e.to_string()))?;

        // The labels seam: ONNX classifiers carry labels intrinsically; remote
        // classifiers take them as the candidate set at call time.
        match entry.kind() {
            BackendKind::Local => {
                if let Some(requested) = &call.labels {
                    let model_labels = entry.labels().ok_or_else(|| {
                        DbError::InvalidOperation(format!(
                            "local model '{model_name}' exposes no labels to validate against"
                        ))
                    })?;
                    if let Some(unknown) = requested
                        .iter()
                        .find(|label| !model_labels.iter().any(|known| known == *label))
                    {
                        return Err(DbError::InvalidOperation(format!(
                            "label '{unknown}' is not among local model '{model_name}' labels \
                         {model_labels:?}"
                        )));
                    }
                }
            }
            BackendKind::Remote => {
                // Remote classification needs a candidate set; remote sentiment is
                // numeric (the model returns a score), so it needs no labels.
                if call.task == Task::Classify && call.labels.is_none() {
                    return Err(DbError::InvalidOperation(format!(
                    "remote classification with model '{model_name}' requires a 'labels' argument"
                )));
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    mod ai_detection_tests {
        use super::{detect_ai_functions, validate_ai_calls, AiCallSpec};
        use laminar_ai::{ModelBackend, ModelEntry, ModelRegistry, Task};

        fn spec(task: Task, model: Option<&str>, labels: Option<Vec<&str>>) -> AiCallSpec {
            AiCallSpec {
                task,
                model: model.map(str::to_string),
                labels: labels.map(|ls| ls.into_iter().map(str::to_string).collect()),
                input: "x".to_string(),
                output_alias: None,
                parse_errors: Vec::new(),
            }
        }

        fn registry() -> ModelRegistry {
            let mut reg = ModelRegistry::new();
            reg.register(ModelEntry {
                id: "finbert".into(),
                tasks: vec![Task::Classify, Task::Sentiment],
                backend: ModelBackend::Local {
                    source: "hf:onnx-community/finbert".into(),
                    labels: Some(vec!["positive".into(), "negative".into(), "neutral".into()]),
                },
            })
            .unwrap();
            reg.register(ModelEntry {
                id: "haiku".into(),
                tasks: vec![Task::Classify, Task::Complete, Task::Sentiment],
                backend: ModelBackend::Remote {
                    provider: "anthropic".into(),
                    model: "claude-haiku-4-5-20251001".into(),
                },
            })
            .unwrap();
            reg.set_default(Task::Sentiment, "finbert");
            reg
        }

        #[test]
        fn detects_single_classify_with_model_and_alias() {
            let calls = detect_ai_functions(
                "SELECT id, ai_classify(headline, model => 'finbert') AS label FROM news",
            );
            assert_eq!(calls.len(), 1);
            let call = &calls[0];
            assert_eq!(call.task, Task::Classify);
            assert_eq!(call.model.as_deref(), Some("finbert"));
            assert_eq!(call.input, "headline");
            assert_eq!(call.output_alias.as_deref(), Some("label"));
            assert!(call.labels.is_none());
        }

        #[test]
        fn detects_labels_array() {
            let calls = detect_ai_functions(
                "SELECT ai_classify(text, model => 'haiku', labels => ARRAY['up','down']) FROM s",
            );
            assert_eq!(calls.len(), 1);
            assert_eq!(
                calls[0].labels,
                Some(vec!["up".to_string(), "down".to_string()])
            );
        }

        #[test]
        fn ignores_queries_without_ai_functions() {
            assert!(detect_ai_functions("SELECT a, b FROM s").is_empty());
        }

        #[test]
        fn malformed_arguments_are_rejected() {
            // Non-column input, missing input, and wrong-typed model/labels each
            // record a parse error that validation surfaces (not silently dropped).
            let cases = [
                "SELECT ai_classify(UPPER(headline), model => 'finbert') AS x FROM s",
                "SELECT ai_classify(model => 'finbert') AS x FROM s",
                "SELECT ai_classify(headline, model => 123) AS x FROM s",
                "SELECT ai_classify(headline, model => 'finbert', labels => 'up') AS x FROM s",
                "SELECT ai_classify(headline, extra, model => 'finbert') AS x FROM s",
                "SELECT ai_classify(headline, model => 'finbert', who => 'me') AS x FROM s",
            ];
            for sql in cases {
                let calls = detect_ai_functions(sql);
                assert_eq!(calls.len(), 1, "{sql}");
                assert!(!calls[0].parse_errors.is_empty(), "{sql}");
                assert!(validate_ai_calls(&registry(), &calls).is_err(), "{sql}");
            }
        }

        #[test]
        fn plan_rewrites_projection_over_tmp_table() {
            let plan = super::plan_ai_query(
                "SELECT id, ai_classify(headline, model => 'finbert') AS label FROM news WHERE id > 0",
            )
            .expect("single aliased AI call is plannable");
            assert_eq!(plan.source_table, "news");
            assert_eq!(plan.call.output_alias.as_deref(), Some("label"));
            let sql = plan.projection_sql.to_lowercase();
            assert!(sql.contains("__ai_tmp"), "{sql}");
            assert!(!sql.contains("ai_classify"), "{sql}");
            assert!(sql.contains("label"));
            assert!(sql.contains("where id > 0"), "{sql}");
        }

        #[test]
        fn plan_requires_alias_and_single_call() {
            assert!(super::plan_ai_query("SELECT ai_classify(t, model => 'm') FROM s").is_none());
            assert!(super::plan_ai_query(
                "SELECT ai_classify(a, model => 'm') AS x, ai_embed(b, model => 'e') AS y FROM s"
            )
            .is_none());
            assert!(super::plan_ai_query("SELECT a FROM s").is_none());
        }

        #[test]
        fn unknown_model_is_rejected() {
            let calls = [spec(Task::Classify, Some("ghost"), Some(vec!["a"]))];
            assert!(validate_ai_calls(&registry(), &calls).is_err());
        }

        #[test]
        fn unsupported_task_is_rejected() {
            let calls = [spec(Task::Summarize, Some("finbert"), None)];
            assert!(validate_ai_calls(&registry(), &calls).is_err());
        }

        #[test]
        fn local_labels_must_be_a_subset() {
            let bad = [spec(Task::Classify, Some("finbert"), Some(vec!["bullish"]))];
            assert!(validate_ai_calls(&registry(), &bad).is_err());
            let ok = [spec(
                Task::Classify,
                Some("finbert"),
                Some(vec!["positive"]),
            )];
            assert!(validate_ai_calls(&registry(), &ok).is_ok());
        }

        #[test]
        fn local_labels_are_optional() {
            let calls = [spec(Task::Classify, Some("finbert"), None)];
            assert!(validate_ai_calls(&registry(), &calls).is_ok());
        }

        #[test]
        fn remote_classification_requires_labels() {
            let without = [spec(Task::Classify, Some("haiku"), None)];
            assert!(validate_ai_calls(&registry(), &without).is_err());
            let with = [spec(Task::Classify, Some("haiku"), Some(vec!["a", "b"]))];
            assert!(validate_ai_calls(&registry(), &with).is_ok());
        }

        #[test]
        fn remote_sentiment_needs_no_labels() {
            // Sentiment is numeric — a remote model scores it without a candidate set.
            let calls = [spec(Task::Sentiment, Some("haiku"), None)];
            assert!(validate_ai_calls(&registry(), &calls).is_ok());
        }

        #[test]
        fn default_model_resolves_or_fails() {
            // Sentiment has a default (finbert).
            let defaulted = [spec(Task::Sentiment, None, None)];
            assert!(validate_ai_calls(&registry(), &defaulted).is_ok());
            // Embed has no default.
            let no_default = [spec(Task::Embed, None, None)];
            assert!(validate_ai_calls(&registry(), &no_default).is_err());
        }
    }
}

pub(crate) use ai::{detect_ai_functions, plan_ai_query, validate_ai_calls, AiQueryPlan};

/// Private table a window-frame operator registers its enriched batch under; the
/// residual projection reads from it. (Mirrors the AI path's `__ai_tmp`.)
pub(crate) const FRAME_TMP_TABLE: &str = "__frame_tmp";

/// A single-source query carrying one bivariate moment frame
/// (`CORR`/`COVAR_SAMP`/`COVAR_POP` `OVER (ORDER BY k ROWS N PRECEDING)`). The
/// call is lifted out — the operator computes it from carried moments — and
/// replaced by its alias in the residual projection.
pub(crate) struct FrameQueryPlan {
    pub func: MomentFn,
    pub x_column: String,
    pub y_column: String,
    pub output_alias: String,
    /// Residual SELECT over [`FRAME_TMP_TABLE`] with the stat call replaced by
    /// its alias column.
    pub projection_sql: String,
    pub source_table: String,
    /// `max(PRECEDING)` — rows of preceding history to retain per new row.
    pub retain: usize,
}

/// Plan a query for window-frame routing. Returns `Some` only for the supported
/// shape: a single (un-joined) source, exactly one bivariate moment call
/// (`CORR`/`COVAR_SAMP`/`COVAR_POP`) `OVER (ORDER BY … ROWS N PRECEDING) AS
/// alias`, no `PARTITION BY`, no `FOLLOWING` bound (streaming cannot buffer the
/// future). Anything else returns `None` (routed normally).
pub(crate) fn plan_frame_query(sql: &str) -> Option<FrameQueryPlan> {
    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
    let mut statement = match statements.into_iter().next()? {
        laminar_sql::parser::StreamingStatement::Standard(boxed) => *boxed,
        _ => return None,
    };

    // Validate the frame shape (single ordered series, preceding bounds only).
    let analysis = laminar_sql::parser::analytic_parser::analyze_window_frames(&statement)?;
    if !analysis.partition_columns.is_empty() || analysis.has_following() {
        return None;
    }
    let retain = usize::try_from(analysis.max_preceding()).ok()?;
    if retain == 0 {
        return None;
    }

    let Statement::Query(query) = &mut statement else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let source_table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return None,
    };

    // Find the single bivariate-moment OVER (...) AS alias projection item.
    let mut found: Option<(usize, MomentFn, String, String, String)> = None;
    for (index, item) in select.projection.iter().enumerate() {
        let SelectItem::ExprWithAlias { expr, alias } = item else {
            continue;
        };
        if let Some((func, x, y)) = moment_call(expr) {
            if found.is_some() {
                return None; // only one frame call is supported
            }
            found = Some((index, func, x, y, alias.value.clone()));
        }
    }
    let (index, func, x_column, y_column, output_alias) = found?;

    // Rewrite: the stat call AS alias → the bare alias column; FROM → temp table.
    select.projection[index] =
        SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(output_alias.clone())));
    if let TableFactor::Table { name, .. } = &mut select.from[0].relation {
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            FRAME_TMP_TABLE,
        ))]);
    }

    Some(FrameQueryPlan {
        func,
        x_column,
        y_column,
        output_alias,
        projection_sql: statement.to_string(),
        source_table,
        retain,
    })
}

/// The statistic and two plain-column arguments of a bivariate moment call with
/// an `OVER` clause (`CORR`/`COVAR_SAMP`/`COVAR_POP`), or `None` otherwise.
fn moment_call(expr: &Expr) -> Option<(MomentFn, String, String)> {
    let Expr::Function(func) = expr else {
        return None;
    };
    func.over.as_ref()?;
    let kind = match func.name.to_string().to_ascii_uppercase().as_str() {
        "CORR" => MomentFn::Corr,
        "COVAR_SAMP" | "COVAR" => MomentFn::CovarSamp,
        "COVAR_POP" => MomentFn::CovarPop,
        _ => return None,
    };
    let (x, y) = bivariate_column_args(func)?;
    Some((kind, x, y))
}

/// The two plain-column arguments of a 2-arg function call, or `None`.
fn bivariate_column_args(func: &sqlparser::ast::Function) -> Option<(String, String)> {
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    let cols: Vec<String> = list
        .args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                Some(id.value.clone())
            }
            _ => None,
        })
        .collect();
    match cols.as_slice() {
        [x, y] => Some((x.clone(), y.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod frame_plan_tests {
    use super::plan_frame_query;

    #[test]
    fn detects_corr_frame_and_rewrites_to_alias() {
        let plan = plan_frame_query(
            "SELECT bucket_start, close, mean_sentiment, \
             CORR(close, mean_sentiment) OVER (ORDER BY bucket_start ROWS 30 PRECEDING) AS corr_30 \
             FROM sentiment_price_join",
        )
        .expect("frame plan");
        assert_eq!(plan.x_column, "close");
        assert_eq!(plan.y_column, "mean_sentiment");
        assert_eq!(plan.output_alias, "corr_30");
        assert_eq!(plan.retain, 30);
        assert_eq!(plan.source_table, "sentiment_price_join");
        // The CORR term is gone; the residual reads the alias from the temp table.
        assert!(!plan.projection_sql.to_uppercase().contains("CORR("));
        assert!(plan.projection_sql.contains("corr_30"));
        assert!(plan.projection_sql.contains("__frame_tmp"));
    }

    #[test]
    fn detects_processtime_equijoin() {
        let d = super::detect_processtime_join(
            "SELECT p.bucket AS bucket, p.price AS price, s.ms AS ms \
             FROM price_b p JOIN sent_b s ON p.bucket = s.bucket",
        )
        .expect("plain INNER equi-join routes to the processing-time join");
        // No time columns is the marker `create_operator` keys on.
        assert!(d.config.left_time_column.is_empty() && d.config.right_time_column.is_empty());
        assert_eq!(d.config.left_key, "bucket");
        assert_eq!(d.config.right_key, "bucket");
        assert_eq!(d.config.left_table, "price_b");
        assert_eq!(d.config.right_table, "sent_b");
    }

    #[test]
    fn rejects_partition_by_and_non_frame_queries() {
        // PARTITION BY is not supported.
        assert!(plan_frame_query(
            "SELECT CORR(a, b) OVER (PARTITION BY g ORDER BY t ROWS 5 PRECEDING) AS c FROM s"
        )
        .is_none());
        // No window frame → not a frame query.
        assert!(plan_frame_query("SELECT a, b FROM s").is_none());
    }
}

pub(crate) fn detect_temporal_query(
    sql: &str,
) -> (Option<TemporalJoinTranslatorConfig>, Option<String>) {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return (None, None);
    };

    let Some(laminar_sql::parser::StreamingStatement::Standard(stmt)) = statements.first() else {
        return (None, None);
    };

    let Statement::Query(query) = stmt.as_ref() else {
        return (None, None);
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return (None, None);
    };

    let Ok(Some(multi)) = analyze_joins(select) else {
        return (None, None);
    };

    let Some(temporal_analysis) = multi.joins.iter().find(|j| j.is_temporal_join) else {
        return (None, None);
    };

    let JoinOperatorConfig::Temporal(config) = JoinOperatorConfig::from_analysis(temporal_analysis)
    else {
        return (None, None);
    };

    let projection_sql = build_temporal_projection_sql(select, temporal_analysis, &config);

    (Some(config), Some(projection_sql))
}

fn split_conjunction_sqlparser(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::And,
            right,
        } => {
            let mut parts = split_conjunction_sqlparser(left);
            parts.extend(split_conjunction_sqlparser(right));
            parts
        }
        Expr::Nested(inner)
            if matches!(
                inner.as_ref(),
                Expr::BinaryOp {
                    op: sqlparser::ast::BinaryOperator::And,
                    ..
                }
            ) =>
        {
            split_conjunction_sqlparser(inner)
        }
        other => vec![other.clone()],
    }
}

fn expr_mentions_alias(expr: &Expr, alias: &str) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            parts[0].value.eq_ignore_ascii_case(alias)
        }
        Expr::Value(_) | Expr::Identifier(_) => false,
        Expr::BinaryOp { left, right, .. } => {
            expr_mentions_alias(left, alias) || expr_mentions_alias(right, alias)
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::Cast { expr: e, .. }
        | Expr::Nested(e)
        | Expr::IsNull(e)
        | Expr::IsNotNull(e) => expr_mentions_alias(e, alias),
        Expr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(al) = &f.args {
                al.args.iter().any(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    )
                    | sqlparser::ast::FunctionArg::Named {
                        arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                        ..
                    } => expr_mentions_alias(e, alias),
                    _ => false,
                })
            } else {
                false
            }
        }
        Expr::InList { expr, list, .. } => {
            expr_mentions_alias(expr, alias) || list.iter().any(|i| expr_mentions_alias(i, alias))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_mentions_alias(expr, alias)
                || expr_mentions_alias(low, alias)
                || expr_mentions_alias(high, alias)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|o| expr_mentions_alias(o, alias))
                || conditions.iter().any(|cw| {
                    expr_mentions_alias(&cw.condition, alias)
                        || expr_mentions_alias(&cw.result, alias)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_mentions_alias(e, alias))
        }
        // Unknown expr variant — conservatively assume it references the alias
        _ => true,
    }
}

/// False for the non-preserved side of an outer join (removing its
/// WHERE predicate would let NULL-extended rows through).
fn can_remove_from_post_where(join_type: StreamJoinType, is_left_side: bool) -> bool {
    !matches!(
        (join_type, is_left_side),
        (StreamJoinType::Left | StreamJoinType::LeftAnti, false)
            | (StreamJoinType::Right, true)
            | (StreamJoinType::Full, _)
    )
}

/// `p.col` → `col`, literals and other nodes pass through via `to_string()`.
fn expr_to_sql_strip_alias(expr: &Expr, alias: &str) -> String {
    match expr {
        Expr::CompoundIdentifier(parts)
            if parts.len() >= 2 && parts[0].value.eq_ignore_ascii_case(alias) =>
        {
            parts[1..]
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".")
        }
        Expr::BinaryOp { left, op, right } => {
            let l = expr_to_sql_strip_alias(left, alias);
            let r = expr_to_sql_strip_alias(right, alias);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            format!("{op} {}", expr_to_sql_strip_alias(inner, alias))
        }
        Expr::Nested(inner) => format!("({})", expr_to_sql_strip_alias(inner, alias)),
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => format!(
            "CAST({} AS {data_type})",
            expr_to_sql_strip_alias(inner, alias)
        ),
        Expr::IsNull(inner) => format!("{} IS NULL", expr_to_sql_strip_alias(inner, alias)),
        Expr::IsNotNull(inner) => {
            format!("{} IS NOT NULL", expr_to_sql_strip_alias(inner, alias))
        }
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let e = expr_to_sql_strip_alias(inner, alias);
            let l = expr_to_sql_strip_alias(low, alias);
            let h = expr_to_sql_strip_alias(high, alias);
            if *negated {
                format!("{e} NOT BETWEEN {l} AND {h}")
            } else {
                format!("{e} BETWEEN {l} AND {h}")
            }
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let e = expr_to_sql_strip_alias(inner, alias);
            let items: Vec<String> = list
                .iter()
                .map(|i| expr_to_sql_strip_alias(i, alias))
                .collect();
            if *negated {
                format!("{e} NOT IN ({})", items.join(", "))
            } else {
                format!("{e} IN ({})", items.join(", "))
            }
        }
        Expr::Function(func) => {
            let name = &func.name;
            let args = match &func.args {
                sqlparser::ast::FunctionArguments::List(al) => al
                    .args
                    .iter()
                    .map(|a| match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => expr_to_sql_strip_alias(e, alias),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                other => other.to_string(),
            };
            format!("{name}({args})")
        }
        other => other.to_string(),
    }
}

fn conjoin_predicates(preds: &[Expr]) -> Option<Expr> {
    preds.iter().cloned().reduce(|acc, pred| Expr::BinaryOp {
        left: Box::new(acc),
        op: sqlparser::ast::BinaryOperator::And,
        right: Box::new(pred),
    })
}

struct SelfJoinPreFilters {
    left_sql: Option<String>,
    right_sql: Option<String>,
    post_join_where: Option<String>,
}

fn extract_self_join_pre_filters(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &StreamJoinConfig,
) -> Option<SelfJoinPreFilters> {
    let where_expr = select.selection.as_ref()?;
    let left_alias = analysis.left_alias.as_deref().unwrap_or(&config.left_table);
    let right_alias = analysis
        .right_alias
        .as_deref()
        .unwrap_or(&config.right_table);

    let preds = split_conjunction_sqlparser(where_expr);

    let mut left_strs = Vec::new();
    let mut right_strs = Vec::new();
    let mut post_join_preds = Vec::new();

    for pred in &preds {
        let refs_left = expr_mentions_alias(pred, left_alias);
        let refs_right = expr_mentions_alias(pred, right_alias);

        match (refs_left, refs_right) {
            (true, false) => {
                left_strs.push(expr_to_sql_strip_alias(pred, left_alias));
                if !can_remove_from_post_where(config.join_type, true) {
                    post_join_preds.push(pred.clone());
                }
            }
            (false, true) => {
                right_strs.push(expr_to_sql_strip_alias(pred, right_alias));
                if !can_remove_from_post_where(config.join_type, false) {
                    post_join_preds.push(pred.clone());
                }
            }
            _ => post_join_preds.push(pred.clone()),
        }
    }

    if left_strs.is_empty() && right_strs.is_empty() {
        return None;
    }

    let left_sql = if left_strs.is_empty() {
        None
    } else {
        Some(left_strs.join(" AND "))
    };
    let right_sql = if right_strs.is_empty() {
        None
    } else {
        Some(right_strs.join(" AND "))
    };

    let post_join_where = conjoin_predicates(&post_join_preds).map(|e| {
        rewrite_stream_join_expr(
            &e,
            analysis.left_alias.as_deref(),
            analysis.right_alias.as_deref(),
            config,
            None,
        )
    });

    Some(SelfJoinPreFilters {
        left_sql,
        right_sql,
        post_join_where,
    })
}

pub(crate) struct StreamJoinDetection {
    pub config: StreamJoinConfig,
    pub projection_sql: String,
    pub left_pre_filter: Option<String>,
    pub right_pre_filter: Option<String>,
}

pub(crate) fn detect_stream_join_query(sql: &str) -> Option<StreamJoinDetection> {
    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;

    let laminar_sql::parser::StreamingStatement::Standard(stmt) = statements.first()? else {
        return None;
    };

    let Statement::Query(query) = stmt.as_ref() else {
        return None;
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };

    let multi = analyze_joins(select).ok()??;

    // Find the first stream-stream join step (has time_bound, not ASOF/temporal/lookup)
    let stream_analysis = multi.joins.iter().find(|j| {
        j.time_bound.is_some() && !j.is_asof_join && !j.is_temporal_join && !j.is_lookup_join
    })?;

    let JoinOperatorConfig::StreamStream(config) =
        JoinOperatorConfig::from_analysis(stream_analysis)
    else {
        return None;
    };

    // Only route to interval join if we have time columns
    if config.left_time_column.is_empty() || config.right_time_column.is_empty() {
        return None;
    }

    // RightSemi/RightAnti mapped to Inner by translator — reject to avoid wrong semantics.
    if matches!(
        stream_analysis.join_type,
        laminar_sql::parser::join_parser::JoinType::RightSemi
            | laminar_sql::parser::join_parser::JoinType::RightAnti
    ) {
        tracing::warn!(
            join_type = ?stream_analysis.join_type,
            "RightSemi/RightAnti not implemented for streaming interval joins; \
             falling back to per-cycle batch join."
        );
        return None;
    }

    let pre_filters = if config.left_table == config.right_table {
        extract_self_join_pre_filters(select, stream_analysis, &config)
    } else {
        None
    };

    let where_clause = match &pre_filters {
        Some(f) => f
            .post_join_where
            .as_ref()
            .map(|w| format!(" WHERE {w}"))
            .unwrap_or_default(),
        None => select
            .selection
            .as_ref()
            .map(|expr| {
                let rewritten = rewrite_stream_join_expr(
                    expr,
                    stream_analysis.left_alias.as_deref(),
                    stream_analysis.right_alias.as_deref(),
                    &config,
                    None,
                );
                format!(" WHERE {rewritten}")
            })
            .unwrap_or_default(),
    };
    let projection_sql =
        build_stream_join_projection_sql(select, stream_analysis, &config, &where_clause);

    Some(StreamJoinDetection {
        config,
        projection_sql,
        left_pre_filter: pre_filters.as_ref().and_then(|f| f.left_sql.clone()),
        right_pre_filter: pre_filters.as_ref().and_then(|f| f.right_sql.clone()),
    })
}

/// Detect a processing-time equi-join: a single `INNER` `a JOIN b ON a.k = b.k`
/// with no temporal predicate. It routes to a per-cycle batch join that matches
/// the current cycle's emissions from both sides — what aligns two windowed
/// views closing the same bucket on processing time. Returns the same shape as
/// [`detect_stream_join_query`] but with **empty time columns**, which is how
/// `create_operator` tells the two join operators apart.
pub(crate) fn detect_processtime_join(sql: &str) -> Option<StreamJoinDetection> {
    use laminar_sql::parser::join_parser::JoinType;

    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
    let laminar_sql::parser::StreamingStatement::Standard(stmt) = statements.first()? else {
        return None;
    };
    let Statement::Query(query) = stmt.as_ref() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let multi = analyze_joins(select).ok()??;

    // Exactly one INNER equi-join step, no temporal predicate, distinct tables
    // (self-joins would collide on the suffixed output schema).
    if multi.joins.len() != 1 {
        return None;
    }
    let step = &multi.joins[0];
    // A no-time-bound equi-join is tagged `is_lookup_join` by `analyze_joins`
    // (it can't tell a windowed-view join from a stream/dim lookup); real lookup
    // joins never reach this detector — the planner emits a Lookup config, which
    // turns `needs_specialized_detection` off in `add_query`. So accept it here.
    if step.time_bound.is_some()
        || step.is_asof_join
        || step.is_temporal_join
        || !matches!(step.join_type, JoinType::Inner)
    {
        return None;
    }
    // Build the StreamStream config directly from the analysis (going through
    // `from_analysis` would yield a Lookup config for this shape). Empty time
    // columns are the processing-time marker `create_operator` keys on.
    let config = StreamJoinConfig {
        left_key: step.left_key_column.clone(),
        right_key: step.right_key_column.clone(),
        left_time_column: String::new(),
        right_time_column: String::new(),
        left_table: step.left_table.clone(),
        right_table: step.right_table.clone(),
        time_bound: std::time::Duration::ZERO,
        join_type: StreamJoinType::Inner,
    };
    if config.left_key.is_empty()
        || config.right_key.is_empty()
        || config.left_table == config.right_table
    {
        return None;
    }

    let where_clause = select
        .selection
        .as_ref()
        .map(|expr| {
            let rewritten = rewrite_stream_join_expr(
                expr,
                step.left_alias.as_deref(),
                step.right_alias.as_deref(),
                &config,
                None,
            );
            format!(" WHERE {rewritten}")
        })
        .unwrap_or_default();
    let projection_sql = build_stream_join_projection_sql(select, step, &config, &where_clause);

    Some(StreamJoinDetection {
        config,
        projection_sql,
        left_pre_filter: None,
        right_pre_filter: None,
    })
}

/// TEMPORAL PROBE JOIN is not in sqlparser's grammar — recognize it by
/// walking the token stream directly.
pub(crate) fn detect_temporal_probe_query(
    sql: &str,
) -> (
    Option<laminar_sql::translator::TemporalProbeConfig>,
    Option<String>,
) {
    match parse_probe(sql) {
        Some((cfg, proj)) => (Some(cfg), proj),
        None => (None, None),
    }
}

fn parse_probe(
    sql: &str,
) -> Option<(laminar_sql::translator::TemporalProbeConfig, Option<String>)> {
    let raw = sqlparser::tokenizer::Tokenizer::new(&GenericDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    let toks: Vec<&TokenWithSpan> = raw
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();

    let tpj = (0..toks.len().saturating_sub(2)).find(|&i| {
        kw(toks[i], "TEMPORAL") && kw(toks[i + 1], "PROBE") && kw(toks[i + 2], "JOIN")
    })?;
    let from = (0..tpj).rev().find(|&i| kw(toks[i], "FROM"))?;
    let left = parse_table_ref(&toks[from + 1..tpj])?;

    let after_tpj = tpj + 3;
    let on = (after_tpj..toks.len()).find(|&i| kw(toks[i], "ON"))?;
    let right = parse_table_ref(&toks[after_tpj..on])?;

    let mut i = on + 1;
    let keys = parse_key_columns(&toks, &mut i)?;

    let (left_time, right_time) = if kw_at(&toks, i, "TIMESTAMPS") {
        i += 1;
        parse_pair(&toks, &mut i)?
    } else {
        tracing::warn!(
            left = %left.name, right = %right.name,
            "TEMPORAL PROBE JOIN: no TIMESTAMPS clause; defaulting to 'ts'",
        );
        ("ts".into(), "ts".into())
    };

    let offsets = if kw_at(&toks, i, "LIST") {
        i += 1;
        parse_list(&toks, &mut i)?
    } else if kw_at(&toks, i, "RANGE") {
        i += 1;
        parse_range(&toks, &mut i)?
    } else {
        return None;
    };

    if !kw_at(&toks, i, "AS") {
        return None;
    }
    i += 1;
    let alias_tok = *toks.get(i)?;
    let probe_alias = ident(alias_tok)?;

    let cfg = laminar_sql::translator::TemporalProbeConfig::new(
        left.name,
        right.name,
        left.alias.clone(),
        right.alias.clone(),
        keys,
        left_time,
        right_time,
        &offsets,
        probe_alias,
    );

    let projection = build_probe_projection_via_ast(
        sql,
        location_to_byte(sql, toks[tpj].span.start),
        location_to_byte(sql, alias_tok.span.end),
        &cfg,
        left.alias.as_deref(),
        right.alias.as_deref(),
    );

    Some((cfg, projection))
}

struct TableRef {
    name: String,
    alias: Option<String>,
}

fn kw(t: &TokenWithSpan, s: &str) -> bool {
    matches!(&t.token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(s))
}

fn kw_at(toks: &[&TokenWithSpan], i: usize, s: &str) -> bool {
    toks.get(i).is_some_and(|t| kw(t, s))
}

fn ident(t: &TokenWithSpan) -> Option<String> {
    match &t.token {
        Token::Word(w) => Some(w.value.clone()),
        _ => None,
    }
}

fn expect(toks: &[&TokenWithSpan], i: &mut usize, want: &Token) -> Option<()> {
    if toks.get(*i).map(|t| &t.token) == Some(want) {
        *i += 1;
        Some(())
    } else {
        None
    }
}

/// Reads `a.b.c` and returns the last segment (the column name).
fn read_qualified(toks: &[&TokenWithSpan], i: &mut usize) -> Option<String> {
    let mut last = ident(toks.get(*i)?)?;
    *i += 1;
    while toks.get(*i).map(|t| &t.token) == Some(&Token::Period) {
        *i += 1;
        last = ident(toks.get(*i)?)?;
        *i += 1;
    }
    Some(last)
}

fn parse_table_ref(toks: &[&TokenWithSpan]) -> Option<TableRef> {
    let mut i = 0;
    let mut name = ident(toks.first()?)?;
    i += 1;
    while toks.get(i).map(|t| &t.token) == Some(&Token::Period) {
        let next = ident(toks.get(i + 1)?)?;
        name.push('.');
        name.push_str(&next);
        i += 2;
    }
    let alias = match toks.len() - i {
        0 => None,
        1 => ident(toks[i]),
        _ if kw(toks[i], "AS") => ident(toks.get(i + 1)?),
        _ => ident(toks[i]),
    };
    Some(TableRef { name, alias })
}

fn parse_key_columns(toks: &[&TokenWithSpan], i: &mut usize) -> Option<Vec<String>> {
    if toks.get(*i).map(|t| &t.token) != Some(&Token::LParen) {
        return Some(vec![read_qualified(toks, i)?]);
    }
    *i += 1;
    let mut cols = Vec::new();
    loop {
        cols.push(read_qualified(toks, i)?);
        match toks.get(*i).map(|t| &t.token) {
            Some(Token::Comma) => *i += 1,
            Some(Token::RParen) => {
                *i += 1;
                return (!cols.is_empty()).then_some(cols);
            }
            _ => return None,
        }
    }
}

fn parse_pair(toks: &[&TokenWithSpan], i: &mut usize) -> Option<(String, String)> {
    expect(toks, i, &Token::LParen)?;
    let l = read_qualified(toks, i)?;
    expect(toks, i, &Token::Comma)?;
    let r = read_qualified(toks, i)?;
    expect(toks, i, &Token::RParen)?;
    Some((l, r))
}

// `0s` / `-5s` tokenize as Number + Word / Minus + Number + Word; Display
// concatenation reassembles the literal for `parse_interval_to_ms`.
fn token_text(toks: &[&TokenWithSpan], start: usize, end: usize) -> String {
    toks[start..end]
        .iter()
        .map(|t| t.token.to_string())
        .collect()
}

fn parse_list(
    toks: &[&TokenWithSpan],
    i: &mut usize,
) -> Option<laminar_sql::translator::ProbeOffsetSpec> {
    expect(toks, i, &Token::LParen)?;
    let mut items = Vec::new();
    let mut item_start = *i;
    loop {
        match &toks.get(*i)?.token {
            Token::Comma => {
                items.push(laminar_sql::translator::parse_interval_to_ms(&token_text(
                    toks, item_start, *i,
                ))?);
                *i += 1;
                item_start = *i;
            }
            Token::RParen => {
                if *i > item_start {
                    items.push(laminar_sql::translator::parse_interval_to_ms(&token_text(
                        toks, item_start, *i,
                    ))?);
                }
                *i += 1;
                break;
            }
            _ => *i += 1,
        }
    }
    (!items.is_empty()).then_some(laminar_sql::translator::ProbeOffsetSpec::List(items))
}

fn parse_range(
    toks: &[&TokenWithSpan],
    i: &mut usize,
) -> Option<laminar_sql::translator::ProbeOffsetSpec> {
    if kw_at(toks, *i, "FROM") {
        *i += 1;
    }
    let start_ms = laminar_sql::translator::parse_interval_to_ms(&take_until(toks, i, &["TO"]))?;
    if !kw_at(toks, *i, "TO") {
        return None;
    }
    *i += 1;
    let end_ms =
        laminar_sql::translator::parse_interval_to_ms(&take_until(toks, i, &["STEP", "AS"]))?;
    let step_ms = if kw_at(toks, *i, "STEP") {
        *i += 1;
        laminar_sql::translator::parse_interval_to_ms(&take_until(toks, i, &["AS"]))?
    } else {
        1000
    };
    Some(laminar_sql::translator::ProbeOffsetSpec::Range {
        start_ms,
        end_ms,
        step_ms,
    })
}

fn take_until(toks: &[&TokenWithSpan], i: &mut usize, stops: &[&str]) -> String {
    let start = *i;
    while *i < toks.len() && !stops.iter().any(|s| kw(toks[*i], s)) {
        *i += 1;
    }
    token_text(toks, start, *i)
}

fn location_to_byte(sql: &str, loc: Location) -> usize {
    if loc.line == 0 {
        return 0;
    }
    let mut line: u64 = 1;
    let mut col: u64 = 1;
    for (b, ch) in sql.char_indices() {
        if line == loc.line && col == loc.column {
            return b;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    sql.len()
}

// ---------------------------------------------------------------------------
// Top-K filtering
// ---------------------------------------------------------------------------

/// Apply a Top-K filter to batches, keeping at most `k` rows total.
///
/// `DataFusion` applies `LIMIT N` per micro-batch, but streaming Top-K
/// needs a global limit across the combined result. This function
/// concatenates all batches and slices to the first `k` rows.
pub(crate) fn apply_topk_filter(batches: &[RecordBatch], k: usize) -> Vec<RecordBatch> {
    if batches.is_empty() || k == 0 {
        return Vec::new();
    }

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows <= k {
        return batches.to_vec();
    }

    // Slice across batches to keep exactly k rows
    let mut remaining = k;
    let mut result = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = remaining.min(batch.num_rows());
        result.push(batch.slice(0, take));
        remaining -= take;
    }
    result
}

// ---------------------------------------------------------------------------
// Private: ASOF projection rewriting
// ---------------------------------------------------------------------------

/// Build a `SELECT ... FROM __asof_tmp` projection query from the original
/// SELECT items, rewriting table-qualified references to plain column names.
fn build_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &AsofJoinTranslatorConfig,
) -> String {
    let left_alias = analysis.left_alias.as_deref();
    let right_alias = analysis.right_alias.as_deref();

    // Collect disambiguation mapping: right-side columns that collide with left
    // get suffixed with _{right_table} in the output schema.
    // We don't know the exact schemas here, but we know the key column is shared.
    // For the right time column, it often collides (e.g., both sides have "ts").
    // The actual renaming is done in build_output_schema; here we just need to
    // handle the common case of right-qualified columns referencing their
    // potentially-renamed counterparts.

    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| rewrite_select_item(item, left_alias, right_alias, config))
        .collect();

    let select_clause = items.join(", ");

    // Rewrite WHERE clause if present
    let where_clause = select.selection.as_ref().map(|expr| {
        let rewritten = rewrite_expr(expr, left_alias, right_alias, config);
        format!(" WHERE {rewritten}")
    });

    format!(
        "SELECT {select_clause} FROM __asof_tmp{}",
        where_clause.unwrap_or_default()
    )
}

/// Rewrite a single `SelectItem` to remove table qualifiers.
fn rewrite_select_item(
    item: &SelectItem,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &AsofJoinTranslatorConfig,
) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => rewrite_expr(expr, left_alias, right_alias, config),
        SelectItem::ExprWithAlias { expr, alias } => {
            let rewritten = rewrite_expr(expr, left_alias, right_alias, config);
            format!("{rewritten} AS {alias}")
        }
        SelectItem::Wildcard(WildcardAdditionalOptions { .. }) => "*".to_string(),
        SelectItem::QualifiedWildcard(name, _) => {
            let table = name.to_string();
            // t.* or q.* — just use * since all columns are flattened
            if Some(table.as_str()) == left_alias || Some(table.as_str()) == right_alias {
                "*".to_string()
            } else {
                format!("{table}.*")
            }
        }
    }
}

/// Recursively rewrite an expression tree to remove table qualifiers
/// and map right-side columns to their disambiguated names.
fn rewrite_expr(
    expr: &Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &AsofJoinTranslatorConfig,
) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = parts[0].value.as_str();
            let column = parts[1].value.as_str();

            let is_left = Some(table) == left_alias || table == config.left_table;
            let is_right = Some(table) == right_alias || table == config.right_table;

            if is_left {
                column.to_string()
            } else if is_right {
                // Check if this right column might be disambiguated.
                // The key column is excluded from the right side entirely,
                // and other duplicate columns get suffixed.
                // We suffix if the column name matches a "well-known" left-side
                // column that could collide — specifically the key column
                // (already excluded) or columns sharing the same name.
                // We use a heuristic: if the right column name equals the
                // left time column, suffix it.
                if column == config.left_time_column && column != config.right_time_column {
                    // Left and right time columns have different names — no collision
                    column.to_string()
                } else if column == config.key_column {
                    // Key column: just use the bare name (from left side)
                    column.to_string()
                } else {
                    // For other right-side columns, check if the column name
                    // matches any "standard" left-side column name.
                    // Since we don't have the full schema here, use a
                    // conservative approach: only suffix the right time column
                    // when it matches the left time column name.
                    if column == config.left_time_column && column == config.right_time_column {
                        // Same name for time columns — right side is suffixed
                        format!("{}_{}", column, config.right_table)
                    } else {
                        column.to_string()
                    }
                }
            } else {
                expr.to_string()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = rewrite_expr(left, left_alias, right_alias, config);
            let r = rewrite_expr(right, left_alias, right_alias, config);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            let e = rewrite_expr(inner, left_alias, right_alias, config);
            format!("{op} {e}")
        }
        Expr::Nested(inner) => {
            let e = rewrite_expr(inner, left_alias, right_alias, config);
            format!("({e})")
        }
        Expr::Function(func) => {
            // Rewrite function arguments
            let name = &func.name;
            let args: Vec<String> = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => arg_list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => rewrite_expr(e, left_alias, right_alias, config),
                        other => other.to_string(),
                    })
                    .collect(),
                other => vec![other.to_string()],
            };
            format!("{name}({})", args.join(", "))
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let e = rewrite_expr(inner, left_alias, right_alias, config);
            format!("CAST({e} AS {data_type})")
        }
        // For any other expression variant, fall back to sqlparser's Display
        _ => expr.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Private: Temporal join projection rewriting
// ---------------------------------------------------------------------------

/// Build a `SELECT ... FROM __temporal_tmp` projection query from the original
/// SELECT items, rewriting table-qualified references to plain column names.
fn build_temporal_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &TemporalJoinTranslatorConfig,
) -> String {
    let left_alias = analysis.left_alias.as_deref();
    let right_alias = analysis.right_alias.as_deref();

    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                rewrite_temporal_expr(expr, left_alias, right_alias, config)
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let rewritten = rewrite_temporal_expr(expr, left_alias, right_alias, config);
                format!("{rewritten} AS {alias}")
            }
            SelectItem::Wildcard(_) => "*".to_string(),
            SelectItem::QualifiedWildcard(name, _) => {
                let table = name.to_string();
                if Some(table.as_str()) == left_alias || Some(table.as_str()) == right_alias {
                    "*".to_string()
                } else {
                    format!("{table}.*")
                }
            }
        })
        .collect();

    let select_clause = items.join(", ");

    let where_clause = select.selection.as_ref().map(|expr| {
        let rewritten = rewrite_temporal_expr(expr, left_alias, right_alias, config);
        format!(" WHERE {rewritten}")
    });

    format!(
        "SELECT {select_clause} FROM __temporal_tmp{}",
        where_clause.unwrap_or_default()
    )
}

/// Rewrite an expression tree to remove table qualifiers for temporal joins.
/// Stream-side columns keep their names. Table-side columns keep their names
/// (temporal join output is a flat concatenation of both sides).
fn rewrite_temporal_expr(
    expr: &Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &TemporalJoinTranslatorConfig,
) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = parts[0].value.as_str();
            let column = parts[1].value.as_str();

            let is_left = Some(table) == left_alias || table == config.stream_table;
            let is_right = Some(table) == right_alias || table == config.table_name;

            if is_left || is_right {
                column.to_string()
            } else {
                expr.to_string()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = rewrite_temporal_expr(left, left_alias, right_alias, config);
            let r = rewrite_temporal_expr(right, left_alias, right_alias, config);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            let e = rewrite_temporal_expr(inner, left_alias, right_alias, config);
            format!("{op} {e}")
        }
        Expr::Nested(inner) => {
            let e = rewrite_temporal_expr(inner, left_alias, right_alias, config);
            format!("({e})")
        }
        Expr::Function(func) => {
            let name = &func.name;
            let args: Vec<String> = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => arg_list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => rewrite_temporal_expr(e, left_alias, right_alias, config),
                        other => other.to_string(),
                    })
                    .collect(),
                other => vec![other.to_string()],
            };
            format!("{name}({})", args.join(", "))
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let e = rewrite_temporal_expr(inner, left_alias, right_alias, config);
            format!("CAST({e} AS {data_type})")
        }
        _ => expr.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Private: Stream-stream join projection rewriting
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn build_stream_join_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &StreamJoinConfig,
    where_clause: &str,
) -> String {
    let left_alias = analysis.left_alias.as_deref();
    let right_alias = analysis.right_alias.as_deref();

    // When the original SELECT chains more joins past the stream-stream step
    // (`A JOIN B ... JOIN C ...`), they run as residual joins in the post-
    // projection over `__interval_tmp` plus the live source catalog. Step-0
    // column refs are then prefixed with `__interval_tmp.` so shared names
    // (e.g. `ka` present in both `__interval_tmp` and `c`) don't collide.
    let has_residual = select.from.len() == 1 && select.from[0].joins.len() > 1;
    let tmp_qual: Option<&str> = has_residual.then_some("__interval_tmp");

    // Count natural-name occurrences so colliding projections (`p.type`,
    // `a.type`) can be aliased as `{qual}_{col}` instead of duplicate `type`.
    let mut natural_counts: FxHashMap<String, usize> = FxHashMap::default();
    if has_residual {
        for item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) = item {
                let n = match expr {
                    Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
                    Expr::Identifier(ident) => Some(ident.value.clone()),
                    _ => None,
                };
                if let Some(n) = n {
                    *natural_counts.entry(n).or_insert(0) += 1;
                }
            }
        }
    }

    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                let rewritten =
                    rewrite_stream_join_expr(expr, left_alias, right_alias, config, tmp_qual);
                // Alias back to the natural column name so projection labels
                // stay user-visible. On self-join collisions, fall back to
                // `{qual}_{col}` so output names stay unique.
                if has_residual {
                    let (qual, natural) = match expr {
                        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                            (Some(parts[0].value.clone()), Some(parts[1].value.clone()))
                        }
                        Expr::Identifier(ident) => (None, Some(ident.value.clone())),
                        _ => (None, None),
                    };
                    if let Some(n) = natural {
                        let collides = natural_counts.get(&n).copied().unwrap_or(0) > 1;
                        if collides {
                            if let Some(q) = qual {
                                return format!("{rewritten} AS {q}_{n}");
                            }
                        }
                        if rewritten != n {
                            return format!("{rewritten} AS {n}");
                        }
                    }
                }
                rewritten
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let rewritten =
                    rewrite_stream_join_expr(expr, left_alias, right_alias, config, tmp_qual);
                format!("{rewritten} AS {alias}")
            }
            SelectItem::Wildcard(_) => "*".to_string(),
            SelectItem::QualifiedWildcard(name, _) => {
                let table = name.to_string();
                if table == config.left_table
                    || left_alias.is_some_and(|a| a == table)
                    || table == config.right_table
                    || right_alias.is_some_and(|a| a == table)
                {
                    "*".to_string()
                } else {
                    format!("{table}.*")
                }
            }
        })
        .collect();

    let residual = if has_residual {
        render_residual_joins(
            &select.from[0].joins[1..],
            config,
            left_alias,
            right_alias,
            tmp_qual,
        )
    } else {
        String::new()
    };

    // IntervalJoin matches symmetrically (`|left_ts - right_ts| <= N`) but
    // BETWEEN is directional (`right_ts >= left_ts AND right_ts <= left_ts +
    // N`). The upper bound is already covered by the symmetric tolerance; we
    // add `right_ts >= left_ts` here to enforce the lower bound.
    let directional_filter =
        if !config.left_time_column.is_empty() && !config.right_time_column.is_empty() {
            let left_ref = match tmp_qual {
                Some(q) => format!("{q}.{}", config.left_time_column),
                None => config.left_time_column.clone(),
            };
            let right_ref = match tmp_qual {
                Some(q) => format!("{q}.{}_{}", config.right_time_column, config.right_table),
                None => format!("{}_{}", config.right_time_column, config.right_table),
            };
            format!("{right_ref} >= {left_ref}")
        } else {
            String::new()
        };

    let combined_where = match (where_clause.is_empty(), directional_filter.is_empty()) {
        (true, true) => String::new(),
        (false, true) => where_clause.to_string(),
        (true, false) => format!(" WHERE {directional_filter}"),
        (false, false) => format!("{where_clause} AND ({directional_filter})"),
    };

    format!(
        "SELECT {} FROM __interval_tmp{residual}{combined_where}",
        items.join(", ")
    )
}

/// Append residual joins (steps 1..N) onto a projection that selects
/// from `__interval_tmp`. Unsupported join shapes (CROSS, USING, etc.)
/// pass through via sqlparser's Display — we only rewrite ON exprs.
fn render_residual_joins(
    joins: &[sqlparser::ast::Join],
    config: &StreamJoinConfig,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    tmp_qual: Option<&str>,
) -> String {
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    let mut out = String::new();
    for join in joins {
        let (kw, on) = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(e))
            | JoinOperator::Join(JoinConstraint::On(e))
            | JoinOperator::StraightJoin(JoinConstraint::On(e)) => ("JOIN", e),
            JoinOperator::Left(JoinConstraint::On(e))
            | JoinOperator::LeftOuter(JoinConstraint::On(e)) => ("LEFT JOIN", e),
            JoinOperator::Right(JoinConstraint::On(e))
            | JoinOperator::RightOuter(JoinConstraint::On(e)) => ("RIGHT JOIN", e),
            JoinOperator::FullOuter(JoinConstraint::On(e)) => ("FULL JOIN", e),
            // No ON expr to rewrite — let sqlparser format the whole join.
            _ => {
                out.push(' ');
                let _ = std::fmt::Write::write_fmt(&mut out, format_args!("{join}"));
                continue;
            }
        };
        let on_sql = rewrite_stream_join_expr(on, left_alias, right_alias, config, tmp_qual);
        let _ = std::fmt::Write::write_fmt(
            &mut out,
            format_args!(" {kw} {} ON {on_sql}", join.relation),
        );
    }
    out
}

/// Rewrite `t.col` references against the `__interval_tmp` schema:
/// step-0 left columns become bare names, right columns get the
/// `_<right_table>` suffix that `IntervalJoinState` adds. With
/// `tmp_qual = Some("__interval_tmp")` the rewritten names are also
/// prefixed to disambiguate from downstream tables in residual joins.
#[allow(clippy::too_many_lines)]
fn rewrite_stream_join_expr(
    expr: &sqlparser::ast::Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &StreamJoinConfig,
    tmp_qual: Option<&str>,
) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = &parts[0].value;
            let col = &parts[1].value;
            let is_left = table == &config.left_table || left_alias.is_some_and(|a| a == table);
            let is_right = table == &config.right_table || right_alias.is_some_and(|a| a == table);
            if is_left || is_right {
                let bare = if is_right {
                    format!("{col}_{}", config.right_table)
                } else {
                    col.clone()
                };
                if let Some(q) = tmp_qual {
                    format!("{q}.{bare}")
                } else {
                    bare
                }
            } else {
                expr.to_string()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = rewrite_stream_join_expr(left, left_alias, right_alias, config, tmp_qual);
            let r = rewrite_stream_join_expr(right, left_alias, right_alias, config, tmp_qual);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config, tmp_qual);
            format!("{op} {r}")
        }
        Expr::Nested(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config, tmp_qual);
            format!("({r})")
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config, tmp_qual);
            format!("CAST({r} AS {data_type})")
        }
        Expr::IsNull(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config, tmp_qual);
            format!("{r} IS NULL")
        }
        Expr::IsNotNull(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config, tmp_qual);
            format!("{r} IS NOT NULL")
        }
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let e = rewrite_stream_join_expr(inner, left_alias, right_alias, config, tmp_qual);
            let l = rewrite_stream_join_expr(low, left_alias, right_alias, config, tmp_qual);
            let h = rewrite_stream_join_expr(high, left_alias, right_alias, config, tmp_qual);
            if *negated {
                format!("{e} NOT BETWEEN {l} AND {h}")
            } else {
                format!("{e} BETWEEN {l} AND {h}")
            }
        }
        Expr::Function(func) => {
            let name = &func.name;
            let args_str = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => {
                    let rewritten_args: Vec<String> = arg_list
                        .args
                        .iter()
                        .map(|arg| match arg {
                            sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(e),
                            ) => rewrite_stream_join_expr(
                                e,
                                left_alias,
                                right_alias,
                                config,
                                tmp_qual,
                            ),
                            other => other.to_string(),
                        })
                        .collect();
                    rewritten_args.join(", ")
                }
                other => other.to_string(),
            };
            format!("{name}({args_str})")
        }
        _ => expr.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Private: Temporal probe join projection rewriting
// ---------------------------------------------------------------------------

/// Pre-process the temporal probe SQL into a standard JOIN that sqlparser
/// can parse, then walk the AST to build the projection SQL.
fn build_probe_projection_via_ast(
    original_sql: &str,
    tpj_pos: usize,
    probe_clause_end: usize,
    config: &laminar_sql::translator::TemporalProbeConfig,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
) -> Option<String> {
    let probe = &config.probe_alias;

    // Build standard JOIN to replace the probe clause
    let left_ref = left_alias.unwrap_or(&config.left_table);
    let right_ref = right_alias.unwrap_or(&config.right_table);
    let on_clause = config
        .key_columns
        .iter()
        .map(|k| format!("{left_ref}.{k} = {right_ref}.{k}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let right_part = right_alias.map_or_else(
        || config.right_table.clone(),
        |a| format!("{} {a}", config.right_table),
    );
    let join_clause = format!("JOIN {right_part} ON {on_clause}");

    let before_probe = &original_sql[..tpj_pos];
    let after_probe = &original_sql[probe_clause_end..];
    let mut rewritten = format!("{before_probe}{join_clause}{after_probe}");

    // Resolve probe pseudo-columns to plain identifiers before parsing
    rewritten = rewritten.replace(&format!("{probe}.offset_ms"), &format!("{probe}_offset_ms"));
    rewritten = rewritten.replace(&format!("{probe}.probe_ts"), &format!("{probe}_probe_ts"));
    rewritten = rewritten.replace(&format!("{probe}.offset_us"), &format!("{probe}_offset_ms"));
    rewritten = rewritten.replace(&format!("{probe}.timestamp"), &format!("{probe}_probe_ts"));

    // Parse the rewritten SQL
    let stmts = laminar_sql::parse_streaming_sql(&rewritten).ok()?;
    let laminar_sql::parser::StreamingStatement::Standard(stmt) = stmts.first()? else {
        return None;
    };
    let Statement::Query(query) = stmt.as_ref() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };

    // Walk AST to build projection SQL
    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| rewrite_probe_select_item(item, left_alias, right_alias, config))
        .collect();
    let select_clause = items.join(", ");

    let where_clause = select.selection.as_ref().map(|expr| {
        let r = rewrite_probe_expr(expr, left_alias, right_alias, config);
        format!(" WHERE {r}")
    });

    let group_by = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
            let cols: Vec<String> = exprs
                .iter()
                .map(|e| rewrite_probe_expr(e, left_alias, right_alias, config))
                .collect();
            format!(" GROUP BY {}", cols.join(", "))
        }
        _ => String::new(),
    };

    let having = select.having.as_ref().map_or(String::new(), |expr| {
        let r = rewrite_probe_expr(expr, left_alias, right_alias, config);
        format!(" HAVING {r}")
    });

    Some(format!(
        "SELECT {select_clause} FROM __temporal_probe_tmp{}{group_by}{having}",
        where_clause.unwrap_or_default()
    ))
}

fn rewrite_probe_select_item(
    item: &SelectItem,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &laminar_sql::translator::TemporalProbeConfig,
) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => rewrite_probe_expr(expr, left_alias, right_alias, config),
        SelectItem::ExprWithAlias { expr, alias } => {
            let r = rewrite_probe_expr(expr, left_alias, right_alias, config);
            format!("{r} AS {alias}")
        }
        SelectItem::Wildcard(_) => "*".to_string(),
        SelectItem::QualifiedWildcard(name, _) => {
            let table = name.to_string();
            if Some(table.as_str()) == left_alias || Some(table.as_str()) == right_alias {
                "*".to_string()
            } else {
                format!("{table}.*")
            }
        }
    }
}

/// AST-level expression rewriter for temporal probe projection.
/// Same recursive structure as ASOF `rewrite_expr`, parameterized for
/// `TemporalProbeConfig` with composite key support and time-column
/// disambiguation heuristic.
fn rewrite_probe_expr(
    expr: &Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &laminar_sql::translator::TemporalProbeConfig,
) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = parts[0].value.as_str();
            let column = parts[1].value.as_str();

            let is_left = Some(table) == left_alias || table == config.left_table;
            let is_right = Some(table) == right_alias || table == config.right_table;

            if is_left {
                column.to_string()
            } else if is_right {
                if config.key_columns.iter().any(|k| k == column) {
                    column.to_string()
                } else if column == config.left_time_column && column == config.right_time_column {
                    format!("{}_{}", column, config.right_table)
                } else {
                    column.to_string()
                }
            } else {
                expr.to_string()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = rewrite_probe_expr(left, left_alias, right_alias, config);
            let r = rewrite_probe_expr(right, left_alias, right_alias, config);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            let e = rewrite_probe_expr(inner, left_alias, right_alias, config);
            format!("{op} {e}")
        }
        Expr::Nested(inner) => {
            let e = rewrite_probe_expr(inner, left_alias, right_alias, config);
            format!("({e})")
        }
        Expr::Function(func) => {
            let name = &func.name;
            let args: Vec<String> = match &func.args {
                sqlparser::ast::FunctionArguments::List(arg_list) => arg_list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => rewrite_probe_expr(e, left_alias, right_alias, config),
                        other => other.to_string(),
                    })
                    .collect(),
                other => vec![other.to_string()],
            };
            format!("{name}({})", args.join(", "))
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let e = rewrite_probe_expr(inner, left_alias, right_alias, config);
            format!("CAST({e} AS {data_type})")
        }
        Expr::IsNull(inner) => {
            let e = rewrite_probe_expr(inner, left_alias, right_alias, config);
            format!("{e} IS NULL")
        }
        Expr::IsNotNull(inner) => {
            let e = rewrite_probe_expr(inner, left_alias, right_alias, config);
            format!("{e} IS NOT NULL")
        }
        _ => expr.to_string(),
    }
}

// --- Retracting temporal-filter recognition ---

/// One side of `time_col CMP now()+off_ms`. `strict` ⇒ `>`/`<`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TemporalBound {
    pub(crate) off_ms: i64,
    pub(crate) strict: bool,
}

/// `SELECT cols FROM <t> WHERE time_col {>|>=|<|<=} now() ± INTERVAL`
/// (or BETWEEN). Empty `proj_cols` ⇒ `SELECT *`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TemporalFilterConfig {
    pub(crate) source_table: String,
    pub(crate) time_col: String,
    pub(crate) proj_cols: Vec<String>,
    pub(crate) lower: Option<TemporalBound>,
    pub(crate) upper: Option<TemporalBound>,
}

pub(crate) enum TemporalFilterAnalysis {
    NotPresent,
    Recognized(Box<TemporalFilterConfig>),
    /// `now()` used in some shape we don't support.
    PresentUnrecognized,
}

fn ident_is_wallclock(name: &str) -> bool {
    name.eq_ignore_ascii_case("now") || name.eq_ignore_ascii_case("current_timestamp")
}

fn expr_is_wallclock(expr: &Expr) -> bool {
    // Only unquoted single-segment identifiers are the wallclock keyword;
    // `"now"` is a column, and `events.now` is a column reference.
    fn ident(i: &sqlparser::ast::Ident) -> bool {
        i.quote_style.is_none() && ident_is_wallclock(&i.value)
    }
    match strip_nested(expr) {
        Expr::Function(f) => ident_is_wallclock(&f.name.to_string()),
        Expr::Identifier(id) => ident(id),
        _ => false,
    }
}

fn expr_uses_wallclock(expr: &Expr) -> bool {
    if expr_is_wallclock(expr) {
        return true;
    }
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_uses_wallclock(left) || expr_uses_wallclock(right)
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::Cast { expr: e, .. }
        | Expr::Nested(e)
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::Collate { expr: e, .. } => expr_uses_wallclock(e),
        Expr::Between {
            expr: e, low, high, ..
        } => expr_uses_wallclock(e) || expr_uses_wallclock(low) || expr_uses_wallclock(high),
        Expr::InList { expr: e, list, .. } => {
            expr_uses_wallclock(e) || list.iter().any(expr_uses_wallclock)
        }
        Expr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(al) = &f.args {
                al.args.iter().any(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    )
                    | sqlparser::ast::FunctionArg::Named {
                        arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                        ..
                    } => expr_uses_wallclock(e),
                    _ => false,
                })
            } else {
                false
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(expr_uses_wallclock)
                || conditions
                    .iter()
                    .any(|w| expr_uses_wallclock(&w.condition) || expr_uses_wallclock(&w.result))
                || else_result.as_deref().is_some_and(expr_uses_wallclock)
        }
        Expr::Tuple(items) => items.iter().any(expr_uses_wallclock),
        Expr::Array(arr) => arr.elem.iter().any(expr_uses_wallclock),
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => set_expr_uses_wallclock(&q.body),
        Expr::InSubquery {
            expr: e, subquery, ..
        } => expr_uses_wallclock(e) || set_expr_uses_wallclock(&subquery.body),
        _ => false,
    }
}

fn set_expr_uses_wallclock(set: &SetExpr) -> bool {
    match set {
        SetExpr::Select(sel) => {
            sel.selection.as_ref().is_some_and(expr_uses_wallclock)
                || sel.having.as_ref().is_some_and(expr_uses_wallclock)
                || sel.qualify.as_ref().is_some_and(expr_uses_wallclock)
                || sel.projection.iter().any(|p| match p {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        expr_uses_wallclock(e)
                    }
                    _ => false,
                })
        }
        SetExpr::Query(q) => set_expr_uses_wallclock(&q.body),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_uses_wallclock(left) || set_expr_uses_wallclock(right)
        }
        _ => false,
    }
}

fn strip_nested(expr: &Expr) -> &Expr {
    let mut e = expr;
    while let Expr::Nested(inner) = e {
        e = inner.as_ref();
    }
    e
}

/// `INTERVAL '<n>' <unit>` → magnitude in ms (sub-ms truncated).
fn interval_to_ms(expr: &Expr) -> Option<i64> {
    let Expr::Interval(iv) = strip_nested(expr) else {
        return None;
    };
    let value_str = match strip_nested(iv.value.as_ref()) {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
            sqlparser::ast::Value::Number(n, _) => n.clone(),
            _ => return None,
        },
        _ => return None,
    };
    let value: i128 = value_str.trim().parse().ok()?;
    let us: i128 = match &iv.leading_field {
        Some(sqlparser::ast::DateTimeField::Microsecond) => 1,
        Some(sqlparser::ast::DateTimeField::Millisecond) => 1_000,
        Some(sqlparser::ast::DateTimeField::Second) | None => 1_000_000,
        Some(sqlparser::ast::DateTimeField::Minute) => 60_000_000,
        Some(sqlparser::ast::DateTimeField::Hour) => 3_600_000_000,
        Some(sqlparser::ast::DateTimeField::Day) => 86_400_000_000,
        Some(sqlparser::ast::DateTimeField::Week(_)) => 604_800_000_000,
        _ => return None,
    };
    i64::try_from(value.checked_mul(us)? / 1_000).ok()
}

/// `now()` ⇒ 0, `now() ± I` ⇒ ±I (ms).
fn now_offset_ms(expr: &Expr) -> Option<i64> {
    let expr = strip_nested(expr);
    if expr_is_wallclock(expr) {
        return Some(0);
    }
    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    let (now_side, iv_side) = if expr_is_wallclock(left) {
        (left.as_ref(), right.as_ref())
    } else if expr_is_wallclock(right) {
        (right.as_ref(), left.as_ref())
    } else {
        return None;
    };
    if !expr_is_wallclock(now_side) {
        return None;
    }
    let mag = interval_to_ms(iv_side)?;
    match op {
        sqlparser::ast::BinaryOperator::Plus => Some(mag),
        // `now() - I` only (subtraction is not commutative).
        sqlparser::ast::BinaryOperator::Minus if expr_is_wallclock(left) => Some(-mag),
        _ => None,
    }
}

fn column_name(expr: &Expr) -> Option<String> {
    match strip_nested(expr) {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
        _ => None,
    }
}

/// Recognise `col CMP now()±I` or `col BETWEEN now()-X AND now()+Y`.
/// Anything else (conjuncts, disjunctions, ...) ⇒ `None`.
fn parse_temporal_predicate(
    expr: &Expr,
) -> Option<(String, Option<TemporalBound>, Option<TemporalBound>)> {
    use sqlparser::ast::BinaryOperator as Bop;
    match strip_nested(expr) {
        Expr::Between {
            expr: col,
            negated: false,
            low,
            high,
        } => {
            let name = column_name(col)?;
            let off_lo = now_offset_ms(low)?;
            let off_hi = now_offset_ms(high)?;
            Some((
                name,
                Some(TemporalBound {
                    off_ms: off_lo,
                    strict: false,
                }),
                Some(TemporalBound {
                    off_ms: off_hi,
                    strict: false,
                }),
            ))
        }
        Expr::BinaryOp { left, op, right } => {
            // Normalise to `col OP now()+off` (flip if column is on the right).
            let (col_e, now_e, op) = if column_name(left).is_some() {
                (left.as_ref(), right.as_ref(), op.clone())
            } else {
                let flipped = match op {
                    Bop::Gt => Bop::Lt,
                    Bop::GtEq => Bop::LtEq,
                    Bop::Lt => Bop::Gt,
                    Bop::LtEq => Bop::GtEq,
                    _ => return None,
                };
                (right.as_ref(), left.as_ref(), flipped)
            };
            let name = column_name(col_e)?;
            let off = now_offset_ms(now_e)?;
            let (lower, upper) = match op {
                Bop::Gt => (
                    Some(TemporalBound {
                        off_ms: off,
                        strict: true,
                    }),
                    None,
                ),
                Bop::GtEq => (
                    Some(TemporalBound {
                        off_ms: off,
                        strict: false,
                    }),
                    None,
                ),
                Bop::Lt => (
                    None,
                    Some(TemporalBound {
                        off_ms: off,
                        strict: true,
                    }),
                ),
                Bop::LtEq => (
                    None,
                    Some(TemporalBound {
                        off_ms: off,
                        strict: false,
                    }),
                ),
                _ => return None,
            };
            Some((name, lower, upper))
        }
        _ => None,
    }
}

pub(crate) fn analyze_temporal_filter(sql: &str) -> TemporalFilterAnalysis {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return TemporalFilterAnalysis::NotPresent;
    };
    let Some(laminar_sql::parser::StreamingStatement::Standard(stmt)) = statements.first() else {
        return TemporalFilterAnalysis::NotPresent;
    };
    let Statement::Query(query) = stmt.as_ref() else {
        return TemporalFilterAnalysis::NotPresent;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return TemporalFilterAnalysis::NotPresent;
    };

    let uses_now = select.selection.as_ref().is_some_and(expr_uses_wallclock)
        || select.having.as_ref().is_some_and(expr_uses_wallclock)
        || select.qualify.as_ref().is_some_and(expr_uses_wallclock)
        || select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                expr_uses_wallclock(e)
            }
            _ => false,
        });
    if !uses_now {
        return TemporalFilterAnalysis::NotPresent;
    }

    // Canonical shape only — no DISTINCT/HAVING/TOP/ORDER/GROUP/JOIN/
    // LIMIT/FETCH; row-limiting doesn't compose with retraction.
    let recognised = (|| {
        if query.order_by.is_some() || query.limit_clause.is_some() || query.fetch.is_some() {
            return None;
        }
        let proj_cols = if select.projection.len() == 1
            && matches!(select.projection[0], SelectItem::Wildcard(_))
        {
            Vec::new()
        } else {
            let mut cols = Vec::with_capacity(select.projection.len());
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => cols.push(id.value.clone()),
                    _ => return None,
                }
            }
            cols
        };
        if select.distinct.is_some()
            || select.having.is_some()
            || select.top.is_some()
            || !select.sort_by.is_empty()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
        {
            return None;
        }
        if !matches!(
            &select.group_by,
            sqlparser::ast::GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty()
        ) {
            return None;
        }
        if select.from.len() != 1 {
            return None;
        }
        let twj = &select.from[0];
        if !twj.joins.is_empty() {
            return None;
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = &twj.relation else {
            return None;
        };
        let source_table = name.to_string();
        let where_expr = select.selection.as_ref()?;
        let (time_col, lower, upper) = parse_temporal_predicate(where_expr)?;
        if lower.is_none() && upper.is_none() {
            return None;
        }
        Some(TemporalFilterConfig {
            source_table,
            time_col,
            proj_cols,
            lower,
            upper,
        })
    })();

    match recognised {
        Some(cfg) => TemporalFilterAnalysis::Recognized(Box::new(cfg)),
        None => TemporalFilterAnalysis::PresentUnrecognized,
    }
}

#[cfg(test)]
mod temporal_filter_recognition_tests {
    use super::*;

    fn cfg(sql: &str) -> TemporalFilterConfig {
        match analyze_temporal_filter(sql) {
            TemporalFilterAnalysis::Recognized(c) => *c,
            TemporalFilterAnalysis::PresentUnrecognized => {
                panic!("expected Recognized, got PresentUnrecognized: {sql}")
            }
            TemporalFilterAnalysis::NotPresent => {
                panic!("expected Recognized, got NotPresent: {sql}")
            }
        }
    }

    #[test]
    fn projection_list_recognised() {
        let c = cfg("SELECT id, amount FROM events WHERE ts > now() - INTERVAL '1' MINUTE");
        assert_eq!(c.proj_cols, vec!["id".to_string(), "amount".to_string()]);
        assert_eq!(c.time_col, "ts");
        // Expression / aliased / qualified projections stay out of scope.
        assert!(matches!(
            analyze_temporal_filter(
                "SELECT id + 1 FROM events WHERE ts > now() - INTERVAL '1' MINUTE"
            ),
            TemporalFilterAnalysis::PresentUnrecognized
        ));
    }

    #[test]
    fn lower_bound_ttl_strict() {
        let c = cfg("SELECT * FROM events WHERE evt > now() - INTERVAL '10' MINUTE");
        assert_eq!(c.source_table, "events");
        assert!(c.proj_cols.is_empty(), "`SELECT *` ⇒ no explicit columns");
        assert_eq!(c.time_col, "evt");
        assert_eq!(
            c.lower,
            Some(TemporalBound {
                off_ms: -600_000,
                strict: true
            })
        );
        assert_eq!(c.upper, None);
    }

    #[test]
    fn between_inclusive_both_bounds() {
        let c = cfg(
            "SELECT * FROM e WHERE ts BETWEEN now() - INTERVAL '2' MINUTE \
             AND now() + INTERVAL '30' SECOND",
        );
        assert_eq!(
            c.lower,
            Some(TemporalBound {
                off_ms: -120_000,
                strict: false
            })
        );
        assert_eq!(
            c.upper,
            Some(TemporalBound {
                off_ms: 30_000,
                strict: false
            })
        );
    }

    #[test]
    fn unrecognised_when_extra_conjunct() {
        assert!(matches!(
            analyze_temporal_filter(
                "SELECT * FROM e WHERE region = 'us' AND ts > now() - INTERVAL '1' MINUTE"
            ),
            TemporalFilterAnalysis::PresentUnrecognized
        ));
    }

    #[test]
    fn not_present_for_ordinary_query() {
        // No false positives — ordinary queries are untouched.
        assert!(matches!(
            analyze_temporal_filter("SELECT * FROM e WHERE region = 'us'"),
            TemporalFilterAnalysis::NotPresent
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_table_refs_plain() {
        let refs = extract_table_references("SELECT * FROM events WHERE id > 1");
        assert_eq!(refs.len(), 1);
        assert!(refs.contains("events"));
    }

    #[test]
    fn test_temporal_probe_strips_quoted_timestamp_columns() {
        let sql = "SELECT t.s FROM trades t \
                   TEMPORAL PROBE JOIN book r \
                   ON (s) TIMESTAMPS (\"T\", \"E\") \
                   LIST (0s, 1s) AS p";
        let (config, _) = detect_temporal_probe_query(sql);
        let config = config.expect("temporal probe detected");
        assert_eq!(config.left_time_column, "T");
        assert_eq!(config.right_time_column, "E");
    }

    #[test]
    fn test_temporal_probe_ignores_literal_in_where() {
        let sql = "SELECT * FROM trades WHERE msg = 'TEMPORAL PROBE JOIN'";
        let (config, _) = detect_temporal_probe_query(sql);
        assert!(
            config.is_none(),
            "must not detect a probe join inside a string literal"
        );
    }

    #[test]
    fn test_temporal_probe_ignores_block_comment_literal() {
        let sql = "SELECT * FROM trades WHERE comment = '/* TEMPORAL PROBE JOIN */'";
        let (config, _) = detect_temporal_probe_query(sql);
        assert!(config.is_none());
    }

    #[test]
    fn test_temporal_probe_through_block_comments() {
        let sql = "SELECT t.s FROM trades t \
                   /* outer */ TEMPORAL PROBE JOIN /* inner */ book r \
                   ON (s) TIMESTAMPS (ts, ts) \
                   LIST (0s, 1s) AS p";
        let (config, _) = detect_temporal_probe_query(sql);
        let config = config.expect("block comments must not block detection");
        assert_eq!(config.left_table, "trades");
        assert_eq!(config.right_table, "book");
    }

    #[test]
    fn test_temporal_probe_qualified_quoted_timestamps() {
        let sql = "SELECT t.s FROM trades t \
                   TEMPORAL PROBE JOIN book r \
                   ON (s) TIMESTAMPS (t.\"T\", r.\"E\") \
                   LIST (0s, 1s) AS p";
        let (config, _) = detect_temporal_probe_query(sql);
        let config = config.expect("qualified quoted idents must resolve");
        assert_eq!(config.left_time_column, "T");
        assert_eq!(config.right_time_column, "E");
    }

    #[test]
    fn test_temporal_probe_range_spec() {
        let sql = "SELECT t.s FROM trades t \
                   TEMPORAL PROBE JOIN book r \
                   ON (s) TIMESTAMPS (ts, ts) \
                   RANGE FROM 0s TO 30s STEP 5s AS p";
        let (config, _) = detect_temporal_probe_query(sql);
        let config = config.expect("range spec must parse");
        // 0,5,10,15,20,25,30 = 7 offsets
        assert_eq!(
            config.expanded_offsets_ms.len(),
            7,
            "got {:?}",
            config.expanded_offsets_ms
        );
    }

    #[test]
    fn extract_table_refs_tumble_in_from() {
        let refs = extract_table_references(
            "SELECT COUNT(*) FROM TUMBLE(events, ts, INTERVAL '10' SECOND) \
             GROUP BY window_start",
        );
        assert_eq!(refs.len(), 1);
        assert!(refs.contains("events"), "got {refs:?}");
    }

    #[test]
    fn extract_table_refs_tumble_join() {
        let refs = extract_table_references(
            "SELECT * FROM TUMBLE(events, ts, INTERVAL '1' MINUTE) e \
             JOIN dim ON e.key = dim.key",
        );
        assert!(refs.contains("events"), "got {refs:?}");
        assert!(refs.contains("dim"), "got {refs:?}");
    }

    #[test]
    fn extract_table_refs_temporal_probe_join() {
        let refs = extract_table_references(
            "SELECT t.s FROM trades t \
             TEMPORAL PROBE JOIN prices r ON (s) TIMESTAMPS (ts, ts) \
             LIST (0s, 5s) AS p",
        );
        assert!(refs.contains("trades"), "got {refs:?}");
        assert!(refs.contains("prices"), "got {refs:?}");
    }

    #[test]
    fn single_source_tumble() {
        let name =
            single_source_table("SELECT COUNT(*) FROM TUMBLE(trades, ts, INTERVAL '5' SECOND)");
        assert_eq!(name.as_deref(), Some("trades"));
    }

    #[test]
    fn is_window_tvf_case_insensitive() {
        assert!(is_window_tvf("TUMBLE"));
        assert!(is_window_tvf("tumble"));
        assert!(is_window_tvf("Hop"));
        assert!(is_window_tvf("SESSION"));
        assert!(!is_window_tvf("my_func"));
    }
}

#[cfg(test)]
mod self_join_filter_tests {
    use super::*;

    #[test]
    fn test_basic_self_join_simple_predicates() {
        let d = detect_stream_join_query(
            "SELECT l.key, r.key FROM events l \
             JOIN events r ON l.key = r.key \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND \
             WHERE l.type = 'A' AND r.type = 'B'",
        )
        .expect("should detect self-join");

        assert_eq!(d.left_pre_filter.as_deref(), Some("type = 'A'"));
        assert_eq!(d.right_pre_filter.as_deref(), Some("type = 'B'"));
        // Only the directional filter remains (user's WHERE pushed to
        // pre-filters); no user-derived predicate stays post-join.
        assert!(
            !d.projection_sql.contains("type"),
            "user predicates should be pushed to pre-filters, got: {}",
            d.projection_sql
        );
    }

    #[test]
    fn test_cross_alias_predicate_stays_post_join() {
        let d = detect_stream_join_query(
            "SELECT p.key FROM events p \
             JOIN events a ON p.key = a.key \
             AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
             WHERE p.type = 'A' AND a.type = 'B' AND p.cost > a.cost",
        )
        .expect("should detect self-join");

        assert_eq!(d.left_pre_filter.as_deref(), Some("type = 'A'"));
        assert_eq!(d.right_pre_filter.as_deref(), Some("type = 'B'"));
        assert!(
            d.projection_sql.contains("WHERE"),
            "cross-alias p.cost > a.cost should stay post-join: {}",
            d.projection_sql
        );
    }

    #[test]
    fn test_unqualified_column_stays_post_join() {
        let d = detect_stream_join_query(
            "SELECT p.key FROM events p \
             JOIN events a ON p.key = a.key \
             AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
             WHERE p.type = 'A' AND status = 'active'",
        )
        .expect("should detect self-join");

        assert_eq!(d.left_pre_filter.as_deref(), Some("type = 'A'"));
        assert!(d.right_pre_filter.is_none());
        assert!(
            d.projection_sql.contains("WHERE"),
            "unqualified 'status' should stay post-join: {}",
            d.projection_sql
        );
    }

    #[test]
    fn test_non_self_join_no_pre_filters() {
        let d = detect_stream_join_query(
            "SELECT o.order_id FROM orders o \
             JOIN payments p ON o.order_id = p.order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR \
             WHERE o.amount > 100",
        )
        .expect("should detect interval join");

        assert!(d.left_pre_filter.is_none());
        assert!(d.right_pre_filter.is_none());
        assert!(d.projection_sql.contains("WHERE"));
    }

    #[test]
    fn test_left_join_keeps_right_predicate_in_post_where() {
        let d = detect_stream_join_query(
            "SELECT p.key FROM events p \
             LEFT JOIN events a ON p.key = a.key \
             AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
             WHERE p.type = 'A' AND a.type = 'B'",
        )
        .expect("should detect self-join");

        assert_eq!(d.left_pre_filter.as_deref(), Some("type = 'A'"));
        assert_eq!(d.right_pre_filter.as_deref(), Some("type = 'B'"));
        assert!(
            d.projection_sql.contains("WHERE"),
            "LEFT JOIN must keep right predicate in WHERE: {}",
            d.projection_sql
        );
    }

    #[test]
    fn test_residual_self_join_aliases_collisions() {
        // `p.type` and `a.type` both rewrite to step-0 columns and would
        // otherwise alias to `AS type` twice. Collision-aware aliasing
        // must emit `AS p_type` / `AS a_type` so output names stay unique.
        let d = detect_stream_join_query(
            "SELECT p.type, a.type, p.key FROM events p \
             JOIN events a ON p.key = a.key \
             AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
             JOIN dim d ON d.key = p.key",
        )
        .expect("should detect self-join");

        assert!(
            d.projection_sql.contains("AS p_type"),
            "expected `AS p_type`, got: {}",
            d.projection_sql
        );
        assert!(
            d.projection_sql.contains("AS a_type"),
            "expected `AS a_type`, got: {}",
            d.projection_sql
        );
        // Non-colliding `p.key` keeps the natural-name alias.
        assert!(
            d.projection_sql.contains("AS key"),
            "non-colliding `p.key` should still alias to `key`: {}",
            d.projection_sql
        );
    }

    #[test]
    fn test_self_join_no_where_clause() {
        let d = detect_stream_join_query(
            "SELECT l.key, r.key FROM events l \
             JOIN events r ON l.key = r.key \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND",
        )
        .expect("should detect self-join");

        assert!(d.left_pre_filter.is_none());
        assert!(d.right_pre_filter.is_none());
    }

    #[test]
    fn test_nested_function_predicate() {
        let d = detect_stream_join_query(
            "SELECT p.key FROM events p \
             JOIN events a ON p.key = a.key \
             AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
             WHERE jsonb_get_text(from_json(p.attrs), 'name') = 'prompt' \
             AND jsonb_get_text(from_json(a.attrs), 'name') = 'api'",
        )
        .expect("should detect self-join");

        assert!(d.left_pre_filter.is_some());
        assert!(d.right_pre_filter.is_some());
        let left = d.left_pre_filter.unwrap();
        assert!(!left.contains("p."), "alias should be stripped: {left}");
        assert!(left.contains("attrs"), "column name should survive: {left}");
    }

    #[test]
    fn test_cast_predicate_classified_correctly() {
        let d = detect_stream_join_query(
            "SELECT l.key FROM events l \
             JOIN events r ON l.key = r.key \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND \
             WHERE CAST(l.duration AS DOUBLE) > 1000",
        )
        .expect("should detect self-join");

        assert!(d.left_pre_filter.is_some());
        assert!(d.right_pre_filter.is_none());
        let left = d.left_pre_filter.unwrap();
        assert!(!left.contains("l."), "alias should be stripped: {left}");
    }

    #[test]
    fn test_is_not_null_predicate() {
        let d = detect_stream_join_query(
            "SELECT l.key FROM events l \
             JOIN events r ON l.key = r.key \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '10' SECOND \
             WHERE l.name IS NOT NULL AND r.name IS NOT NULL",
        )
        .expect("should detect self-join");

        assert!(d.left_pre_filter.is_some());
        assert!(d.right_pre_filter.is_some());
        let left = d.left_pre_filter.unwrap();
        assert!(
            left.contains("IS NOT NULL"),
            "should preserve IS NOT NULL: {left}"
        );
        assert!(!left.contains("l."), "alias should be stripped: {left}");
    }

    #[test]
    fn test_string_literal_containing_alias_not_corrupted() {
        let d = detect_stream_join_query(
            "SELECT p.key FROM events p \
             JOIN events a ON p.key = a.key \
             AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
             WHERE a.type = 'p.internal'",
        )
        .expect("should detect self-join");

        assert!(d.left_pre_filter.is_none());
        assert!(d.right_pre_filter.is_some());
        let right = d.right_pre_filter.unwrap();
        assert!(
            right.contains("'p.internal'"),
            "string literal must not be corrupted: {right}"
        );
    }
}
