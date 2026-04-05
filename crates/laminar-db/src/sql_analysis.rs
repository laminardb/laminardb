//! SQL analysis and join detection utilities.

use std::sync::Arc;

use rustc_hash::FxHashSet;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use sqlparser::ast::{
    Expr, SelectItem, SetExpr, Statement, TableFactor, WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
// String literal / comment stripping
// ---------------------------------------------------------------------------

fn strip_literals_and_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Single-quoted string: replace content with spaces
            out.push(' ');
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        // Escaped quote (''): skip both
                        out.push_str("  ");
                        i += 2;
                        continue;
                    }
                    out.push(' ');
                    i += 1;
                    break;
                }
                out.push(' ');
                i += 1;
            }
        } else if bytes[i] == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            // Line comment: replace until newline
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(' ');
                i += 1;
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
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
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return tables;
    };
    for stmt in &statements {
        if let Statement::Query(query) = stmt {
            collect_tables_from_set_expr(query.body.as_ref(), &mut tables);
        }
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
        TableFactor::Table { name, .. } => {
            // Use last component of potentially qualified name (e.g., "schema.table")
            tables.insert(name.to_string());
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
        TableFactor::Table { name, .. } => {
            tables.push(name.to_string());
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
            // Floor to nearest window boundary, accounting for offset.
            // Must match TumblingWindowAssigner::assign() formula:
            //   floor((ts - offset) / size) * size + offset
            let offset = config.offset_ms;
            let adjusted = watermark_ms - offset;
            let floored = if adjusted >= 0 {
                (adjusted / size) * size
            } else {
                ((adjusted - size + 1) / size) * size
            };
            floored + offset
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
            let base = (watermark_ms - offset).saturating_sub(size);
            let boundary = if base >= 0 {
                (base / slide).saturating_add(1).saturating_mul(slide)
            } else {
                ((base - slide + 1) / slide)
                    .saturating_add(1)
                    .saturating_mul(slide)
            };
            boundary + offset
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
            // Same offset-aware floor as Tumbling
            let offset = config.offset_ms;
            let adjusted = watermark_ms - offset;
            let floored = if adjusted >= 0 {
                (adjusted / size) * size
            } else {
                ((adjusted - size + 1) / size) * size
            };
            floored + offset
        }
    }
}

pub(crate) fn infer_ts_format_from_batch(
    batch: &RecordBatch,
    column: &str,
) -> laminar_core::time::TimestampFormat {
    if let Ok(idx) = batch.schema().index_of(column) {
        match batch.schema().field(idx).data_type() {
            DataType::Timestamp(_, _) => laminar_core::time::TimestampFormat::ArrowNative,
            _ => laminar_core::time::TimestampFormat::UnixMillis,
        }
    } else {
        laminar_core::time::TimestampFormat::UnixMillis
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

/// Not recognized by sqlparser, so detection works on the raw SQL string.
#[allow(clippy::too_many_lines)]
pub(crate) fn detect_temporal_probe_query(
    sql: &str,
) -> (
    Option<laminar_sql::translator::TemporalProbeConfig>,
    Option<String>,
) {
    // Strip string literals and single-line comments before keyword search
    // to avoid false positives from SQL like WHERE msg = 'TEMPORAL PROBE JOIN'.
    let stripped = strip_literals_and_comments(sql);
    let upper = stripped.to_uppercase();
    let Some(tpj_pos) = upper.find("TEMPORAL PROBE JOIN") else {
        return (None, None);
    };

    let from_pos = upper[..tpj_pos].rfind("FROM").unwrap_or(0);
    let between_from_and_tpj = sql[from_pos + 4..tpj_pos].trim();

    let left_parts: Vec<&str> = between_from_and_tpj.split_whitespace().collect();
    if left_parts.is_empty() {
        return (None, None);
    }
    let left_table = left_parts[0].to_string();
    let left_alias = if left_parts.len() >= 3 && left_parts[1].eq_ignore_ascii_case("AS") {
        Some(left_parts[2].to_string())
    } else {
        left_parts.get(1).map(ToString::to_string)
    };

    let after_tpj = &sql[tpj_pos + "TEMPORAL PROBE JOIN".len()..];
    let after_tpj_trimmed = after_tpj.trim_start();

    let after_upper = after_tpj_trimmed.to_uppercase();
    let Some(on_pos) = after_upper.find(" ON ") else {
        return (None, None);
    };

    let right_part = after_tpj_trimmed[..on_pos].trim();
    let right_parts: Vec<&str> = right_part.split_whitespace().collect();
    if right_parts.is_empty() {
        return (None, None);
    }
    let right_table = right_parts[0].to_string();
    let right_alias = if right_parts.len() >= 3 && right_parts[1].eq_ignore_ascii_case("AS") {
        Some(right_parts[2].to_string())
    } else {
        right_parts.get(1).map(ToString::to_string)
    };

    let after_on = after_tpj_trimmed[on_pos + 4..].trim_start();
    let key_columns: Vec<String> = if let Some(rest) = after_on.strip_prefix('(') {
        let end_paren = rest.find(')').unwrap_or(rest.len());
        rest[..end_paren]
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        vec![after_on.split_whitespace().next().unwrap_or("").to_string()]
    };

    // Parse optional TIMESTAMPS (left_col, right_col) clause
    let after_on_upper = after_on.to_uppercase();
    let (left_time_column, right_time_column) =
        if let Some(ts_pos) = after_on_upper.find("TIMESTAMPS") {
            let ts_text = &after_on[ts_pos + "TIMESTAMPS".len()..].trim_start();
            if let Some(rest) = ts_text.strip_prefix('(') {
                let end_paren = rest.find(')').unwrap_or(rest.len());
                let cols: Vec<&str> = rest[..end_paren].split(',').map(str::trim).collect();
                (
                    cols.first().unwrap_or(&"ts").to_string(),
                    cols.get(1).unwrap_or(&"ts").to_string(),
                )
            } else {
                ("ts".into(), "ts".into())
            }
        } else {
            tracing::warn!(
                %left_table,
                %right_table,
                "temporal probe join: no TIMESTAMPS clause, defaulting to 'ts'. \
                 Use TIMESTAMPS (left_col, right_col) to specify."
            );
            ("ts".into(), "ts".into())
        };

    let (offsets, alias_search_start) = if let Some(range_pos) = after_on_upper.find("RANGE ") {
        let range_text = &after_on[range_pos + 6..];
        let range_upper = range_text.to_uppercase();
        let from_pos = range_upper.find("FROM ").map_or(0, |p| p + 5);
        let to_pos = range_upper.find(" TO ").unwrap_or(range_text.len());
        let step_pos = range_upper.find(" STEP ");

        let start_str = range_text[from_pos..to_pos].trim();
        let start_ms = if let Some(v) = laminar_sql::translator::parse_interval_to_ms(start_str) {
            v
        } else {
            tracing::warn!(
                "TEMPORAL PROBE JOIN: invalid RANGE start '{start_str}', defaulting to 0"
            );
            0
        };

        let (end_str, step_str) = if let Some(sp) = step_pos {
            let to_end = &range_text[to_pos + 4..sp];
            let as_pos = range_upper[sp + 6..].find(" AS ");
            let step_end = as_pos.map_or(range_text.len(), |p| sp + 6 + p);
            (to_end.trim(), range_text[sp + 6..step_end].trim())
        } else {
            let as_pos = range_upper[to_pos + 4..].find(" AS ");
            let end_pos = as_pos.map_or(range_text.len(), |p| to_pos + 4 + p);
            (range_text[to_pos + 4..end_pos].trim(), "1s")
        };

        let end_ms = if let Some(v) = laminar_sql::translator::parse_interval_to_ms(end_str) {
            v
        } else {
            tracing::warn!("TEMPORAL PROBE JOIN: invalid RANGE end '{end_str}', defaulting to 0");
            0
        };
        let step_ms = if let Some(v) = laminar_sql::translator::parse_interval_to_ms(step_str) {
            v
        } else {
            tracing::warn!(
                "TEMPORAL PROBE JOIN: invalid RANGE step '{step_str}', defaulting to 1s"
            );
            1000
        };

        (
            laminar_sql::translator::ProbeOffsetSpec::Range {
                start_ms,
                end_ms,
                step_ms,
            },
            range_pos,
        )
    } else if let Some(list_pos) = after_on_upper.find("LIST ") {
        let list_text = &after_on[list_pos + 5..];
        let Some(open_paren) = list_text.find('(') else {
            return (None, None);
        };
        let Some(close_paren) = list_text.find(')') else {
            return (None, None);
        };
        let items_str = &list_text[open_paren + 1..close_paren];
        let offsets_ms: Vec<i64> = items_str
            .split(',')
            .filter_map(|s| laminar_sql::translator::parse_interval_to_ms(s.trim()))
            .collect();
        if offsets_ms.is_empty() {
            return (None, None);
        }
        (
            laminar_sql::translator::ProbeOffsetSpec::List(offsets_ms),
            list_pos,
        )
    } else {
        return (None, None);
    };

    let remaining = &after_on[alias_search_start..];
    let remaining_upper = remaining.to_uppercase();
    let Some(as_pos) = remaining_upper.rfind(" AS ") else {
        return (None, None);
    };

    let after_as = remaining[as_pos + 4..].trim();
    let probe_alias = after_as
        .split(|c: char| c.is_whitespace() || c == ';' || c == ')')
        .next()
        .unwrap_or("p")
        .to_string();

    let config = laminar_sql::translator::TemporalProbeConfig::new(
        left_table.clone(),
        right_table.clone(),
        left_alias.clone(),
        right_alias.clone(),
        key_columns,
        left_time_column,
        right_time_column,
        &offsets,
        probe_alias,
    );

    // Build projection SQL by pre-processing the original SQL into a form
    // sqlparser can parse, then walking the AST for safe rewriting.
    let projection_sql = build_probe_projection_via_ast(
        sql,
        tpj_pos,
        &config,
        left_alias.as_deref(),
        right_alias.as_deref(),
    );

    (Some(config), projection_sql)
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
    config: &laminar_sql::translator::TemporalProbeConfig,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
) -> Option<String> {
    let probe = &config.probe_alias;
    let upper = original_sql.to_uppercase();

    // Find end of the probe clause: last "AS <alias>" after TEMPORAL PROBE JOIN
    let after_tpj_upper = &upper[tpj_pos..];
    let as_needle = format!(" AS {}", probe.to_uppercase());
    let as_off = after_tpj_upper.rfind(&as_needle)?;
    let probe_clause_end = tpj_pos + as_off + as_needle.len();

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
        // RIGHT predicate must stay to filter NULL-extended rows
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
        // 'p.internal' literal must not trick classification into thinking
        // this predicate references the left alias `p`.
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
