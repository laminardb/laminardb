//! SQL analysis and detection utilities.
//!
//! Extracted from `stream_executor.rs` — these functions are used by
//! `OperatorGraph`, DDL processing, and operator implementations to inspect
//! SQL queries without running them.
//!
//! # Contents
//!
//! - **Table reference extraction**: [`extract_table_references`], [`single_source_table`]
//! - **Join detection**: [`detect_asof_query`], [`detect_temporal_query`],
//!   [`detect_stream_join_query`], [`detect_temporal_probe_query`]
//! - **Projection helpers**: [`extract_projection_exprs`], [`extract_projection_filter`],
//!   [`CompiledPostProjection`], [`ProjectionFilterInfo`]
//! - **Window helpers**: [`compute_closed_boundary`], [`infer_ts_format_from_batch`]
//! - **Emit conversion**: [`sql_emit_to_core`], [`emit_clause_to_core`]
//! - **Post-filter**: [`apply_topk_filter`]

use std::sync::Arc;

use rustc_hash::FxHashSet;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, SelectItem, SetExpr, Statement, TableFactor,
    WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use laminar_sql::parser::join_parser::analyze_joins;
use laminar_sql::parser::{EmitClause, EmitStrategy as SqlEmitStrategy};
use laminar_sql::translator::{
    AsofJoinTranslatorConfig, JoinOperatorConfig, StreamJoinConfig, TemporalJoinTranslatorConfig,
    WindowOperatorConfig, WindowType,
};

// ---------------------------------------------------------------------------
// Emit conversion
// ---------------------------------------------------------------------------

/// Convert a SQL-layer `EmitStrategy` to a core-layer `EmitStrategy`.
///
/// The SQL parser produces `EmitStrategy::FinalOnly`, while core operators
/// expect `EmitStrategy::Final`. This function maps all variants correctly.
///
/// Cannot use a `From` impl due to the orphan rule (neither type is local).
#[allow(dead_code)] // Used in tests (core_window_state).
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

/// Convert an `EmitClause` (SQL AST) to a core `EmitStrategy`.
///
/// Calls `EmitClause::to_emit_strategy()` to resolve the clause to a
/// runtime strategy, then converts via [`sql_emit_to_core`].
#[allow(dead_code)] // Used in tests (core_window_state).
pub(crate) fn emit_clause_to_core(
    clause: &EmitClause,
) -> Result<laminar_core::operator::window::EmitStrategy, laminar_sql::parser::ParseError> {
    let sql_strategy = clause.to_emit_strategy()?;
    Ok(sql_emit_to_core(&sql_strategy))
}

// ---------------------------------------------------------------------------
// String literal / comment stripping
// ---------------------------------------------------------------------------

/// Replace single-quoted string literals and `--` line comments with spaces.
///
/// Preserves byte offsets so that positions found in the stripped string
/// can be used on the original SQL. Used before keyword searches to avoid
/// false positives from keywords inside strings or comments.
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

/// Extract all table names referenced in FROM/JOIN clauses of a SQL query.
///
/// Parses the SQL and walks the AST to find `TableFactor::Table` references,
/// recursing into subqueries, nested joins, and set operations (UNION, etc.).
///
/// Returns a **deduplicated** set. For self-join detection use
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

/// Check whether `sql` references exactly one logical table occurrence.
///
/// Unlike [`extract_table_references`] (which deduplicates), this counts every
/// FROM/JOIN table occurrence. A self-join like `events e1 JOIN events e2`
/// returns `false` because there are two occurrences even though the base table
/// name is the same.
///
/// Returns `Some(table_name)` when there is exactly one occurrence, `None`
/// otherwise.
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

/// Recursively collect table names from a `SetExpr`.
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

/// Collect table names from a single `TableFactor`.
fn collect_tables_from_factor(factor: &TableFactor, tables: &mut FxHashSet<String>) {
    match factor {
        TableFactor::Table { name, args, .. } => {
            tables.insert(resolve_tvf_source(name, args));
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

/// Like [`collect_tables_from_set_expr`] but collects every occurrence (not deduplicated).
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

/// Collect a single table occurrence from a `TableFactor`.
fn collect_factor_counting(factor: &TableFactor, tables: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, args, .. } => {
            tables.push(resolve_tvf_source(name, args));
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
    args: &Option<sqlparser::ast::TableFunctionArgs>,
) -> String {
    let name_str = name.to_string();
    if let Some(ref tfa) = args {
        if is_window_tvf(&name_str) {
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
    /// Physical expressions to evaluate per output column.
    pub(crate) exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Output schema.
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

/// Information extracted from a simple Projection + Filter logical plan.
pub(crate) struct ProjectionFilterInfo {
    /// Projection expressions from the top-level Projection node.
    pub(crate) proj_exprs: Vec<datafusion_expr::Expr>,
    /// Optional WHERE predicate from a Filter node below the Projection.
    pub(crate) filter_predicate: Option<datafusion_expr::Expr>,
    /// `DFSchema` of the scan input (for compiling expressions).
    pub(crate) input_df_schema: Arc<datafusion_common::DFSchema>,
    /// Source table name from the `TableScan`.
    pub(crate) source_table: String,
}

/// Walk an optimized `LogicalPlan` to extract a simple Projection + Filter shape.
///
/// Returns `Some` only for plans of the shape:
///   `Projection? -> Filter? -> TableScan`
/// (with optional `SubqueryAlias` wrappers).
///
/// Returns `None` if the plan has Sort, Limit, Distinct, Join, Aggregate,
/// or any other node that `CompiledProjection` cannot handle.
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

/// Extract optional `Filter` predicate + `TableScan` from the inner plan.
/// Returns `(optional_predicate, input_df_schema, table_name)`.
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

/// Extract projection expressions from a logical plan for post-join compilation.
///
/// Walks the plan to find the Projection node, compiles each expression to a
/// `PhysicalExpr`, and returns the expressions with the output schema.
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

/// Infer the `TimestampFormat` from a `RecordBatch` column's `DataType`.
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

/// Detect an ASOF join in raw SQL text.
///
/// Returns `(config, projection_sql)` or `(None, None)` if not detected.
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

/// Detect a temporal join in raw SQL text.
///
/// Returns `(config, projection_sql)` or `(None, None)` if not detected.
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

/// Detect a stream-stream interval join in raw SQL text.
///
/// Returns `(config, projection_sql)` or `(None, None)` if not detected.
pub(crate) fn detect_stream_join_query(sql: &str) -> (Option<StreamJoinConfig>, Option<String>) {
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

    // Find the first stream-stream join step (has time_bound, not ASOF/temporal/lookup)
    let Some(stream_analysis) = multi.joins.iter().find(|j| {
        j.time_bound.is_some() && !j.is_asof_join && !j.is_temporal_join && !j.is_lookup_join
    }) else {
        return (None, None);
    };

    let JoinOperatorConfig::StreamStream(config) =
        JoinOperatorConfig::from_analysis(stream_analysis)
    else {
        return (None, None);
    };

    // Only route to interval join if we have time columns
    if config.left_time_column.is_empty() || config.right_time_column.is_empty() {
        return (None, None);
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
        return (None, None);
    }

    let projection_sql = build_stream_join_projection_sql(select, stream_analysis, &config);

    (Some(config), Some(projection_sql))
}

/// Detect a `TEMPORAL PROBE JOIN` in raw SQL text.
///
/// Not recognized by sqlparser, so detection works on the raw SQL string.
/// Returns `(config, projection_sql)` or `(None, None)` if not detected.
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

/// Build a `SELECT ... FROM __interval_tmp` projection query from the original
/// SELECT items, rewriting table-qualified references to plain column names.
fn build_stream_join_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &StreamJoinConfig,
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

    let where_clause = select
        .selection
        .as_ref()
        .map(|expr| {
            let rewritten = rewrite_stream_join_expr(expr, left_alias, right_alias, config);
            format!(" WHERE {rewritten}")
        })
        .unwrap_or_default();

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
mod tests {
    use super::*;

    #[test]
    fn extract_table_refs_plain() {
        let refs = extract_table_references("SELECT * FROM events WHERE id > 1");
        assert_eq!(refs.len(), 1);
        assert!(refs.contains("events"));
    }

    #[test]
    fn extract_table_refs_tumble_in_from() {
        let refs = extract_table_references(
            "SELECT COUNT(*) FROM TUMBLE(events, ts, INTERVAL '10' SECOND) \
             GROUP BY window_start",
        );
        assert_eq!(refs.len(), 1);
        assert!(refs.contains("events"), "got {:?}", refs);
    }

    #[test]
    fn extract_table_refs_tumble_join() {
        let refs = extract_table_references(
            "SELECT * FROM TUMBLE(events, ts, INTERVAL '1' MINUTE) e \
             JOIN dim ON e.key = dim.key",
        );
        assert!(refs.contains("events"), "got {:?}", refs);
        assert!(refs.contains("dim"), "got {:?}", refs);
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
