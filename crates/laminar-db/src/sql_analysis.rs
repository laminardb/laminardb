//! SQL analysis and join detection utilities.

use std::sync::Arc;

use rustc_hash::FxHashSet;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, SelectItem, SetExpr, Statement, TableFactor,
    WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan};

use laminar_sql::parser::join_parser::analyze_joins;
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
        _ => {}
    }
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
    pub(crate) source_table: String,
}

/// Returns `Some` only for plans of the shape
/// `Projection? -> Filter? -> TableScan` (with optional `SubqueryAlias` wrappers).
pub(crate) fn extract_projection_filter(plan: &LogicalPlan) -> Option<ProjectionFilterInfo> {
    match plan {
        LogicalPlan::Projection(proj) => {
            let proj_exprs = proj.expr.clone();
            extract_filter_or_scan(&proj.input).map(|(filter_pred, input_schema, table_name)| {
                ProjectionFilterInfo {
                    proj_exprs,
                    filter_predicate: filter_pred,
                    input_df_schema: input_schema,
                    source_table: table_name,
                }
            })
        }
        // No Projection wrapper — check for Filter -> TableScan directly
        _ => match extract_filter_or_scan(plan) {
            Some((filter_pred, input_schema, table_name)) => {
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
                    source_table: table_name,
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
            #[allow(clippy::cast_possible_truncation)]
            let size = config.size.as_millis() as i64;
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
            #[allow(clippy::cast_possible_truncation)]
            let gap = config.gap.map_or(0, |g| g.as_millis() as i64);
            watermark_ms.saturating_sub(gap)
        }
        WindowType::Sliding => {
            #[allow(clippy::cast_possible_truncation)]
            let size = config.size.as_millis() as i64;
            #[allow(clippy::cast_possible_truncation)]
            let slide = config.slide.map_or(size, |s| s.as_millis() as i64);
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
            #[allow(clippy::cast_possible_truncation)]
            let size = config.size.as_millis() as i64;
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

fn build_stream_join_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &StreamJoinConfig,
    where_clause: &str,
) -> String {
    let left_alias = analysis.left_alias.as_deref();
    let right_alias = analysis.right_alias.as_deref();

    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                rewrite_stream_join_expr(expr, left_alias, right_alias, config)
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let rewritten = rewrite_stream_join_expr(expr, left_alias, right_alias, config);
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

    format!(
        "SELECT {} FROM __interval_tmp{where_clause}",
        items.join(", ")
    )
}

/// Rewrite table-qualified column refs for the `__interval_tmp` schema.
fn rewrite_stream_join_expr(
    expr: &sqlparser::ast::Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &StreamJoinConfig,
) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = &parts[0].value;
            let col = &parts[1].value;
            let is_left = table == &config.left_table || left_alias.is_some_and(|a| a == table);
            let is_right = table == &config.right_table || right_alias.is_some_and(|a| a == table);
            if is_left || is_right {
                if is_right {
                    format!("{col}_{}", config.right_table)
                } else {
                    col.clone()
                }
            } else {
                expr.to_string()
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = rewrite_stream_join_expr(left, left_alias, right_alias, config);
            let r = rewrite_stream_join_expr(right, left_alias, right_alias, config);
            format!("{l} {op} {r}")
        }
        Expr::UnaryOp { op, expr: inner } => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("{op} {r}")
        }
        Expr::Nested(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("({r})")
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("CAST({r} AS {data_type})")
        }
        Expr::IsNull(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("{r} IS NULL")
        }
        Expr::IsNotNull(inner) => {
            let r = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            format!("{r} IS NOT NULL")
        }
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let e = rewrite_stream_join_expr(inner, left_alias, right_alias, config);
            let l = rewrite_stream_join_expr(low, left_alias, right_alias, config);
            let h = rewrite_stream_join_expr(high, left_alias, right_alias, config);
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
                            ) => rewrite_stream_join_expr(e, left_alias, right_alias, config),
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
        assert!(
            !d.projection_sql.contains("WHERE"),
            "inner join should have no post-join WHERE, got: {}",
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
