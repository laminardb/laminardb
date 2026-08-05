//! SQL analysis: table-reference extraction, join detection, and projection rewriting.

use std::ops::ControlFlow;
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_expr::LogicalPlan;
use sqlparser::ast::{
    visit_expressions, CastKind, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, Ident, ObjectName, ObjectNamePart, SelectFlavor, SelectItem, SetExpr, Statement,
    TableFactor, TableVersion, WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::ai::{BackendKind, ModelRegistry, Task};
use laminar_sql::parser::join_parser::{analyze_join, analyze_joins, JoinType};

use crate::error::DbError;
use crate::operator::window_frame::MomentFn;
#[cfg(test)]
use laminar_sql::parser::{EmitClause, EmitStrategy as SqlEmitStrategy};
use laminar_sql::translator::{JoinOperatorConfig, StreamJoinConfig, TemporalJoinTranslatorConfig};

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

/// Returns the deduplicated set of table names from FROM/JOIN clauses.
///
/// For self-join detection use [`single_source_table`] (which counts occurrences).
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
    tables
}

/// Column indices of `schema` to fetch for partial lookup `table`, for projection pushdown.
///
/// Returns the union of columns referenced by any query over `table`, plus `pk_cols`.
/// Returns an empty vec (fetch all) when a query uses wildcards, subqueries, or
/// non-table factors, or when the union already covers the whole schema.
pub(crate) fn compute_lookup_projection(
    schema: &SchemaRef,
    pk_cols: &[String],
    table: &str,
    queries: impl IntoIterator<Item = impl AsRef<str>>,
) -> Vec<u32> {
    let mut referenced: FxHashSet<String> = pk_cols.iter().cloned().collect();
    for sql in queries {
        let sql = sql.as_ref();
        if !extract_table_references(sql).contains(table) {
            continue;
        }
        match collect_referenced_columns(sql) {
            Some(cols) => referenced.extend(cols),
            None => return Vec::new(),
        }
    }

    let indices: Vec<u32> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| referenced.contains(f.name()))
        .map(|(i, _)| u32::try_from(i).expect("column index fits u32"))
        .collect();

    if indices.len() == schema.fields().len() {
        Vec::new()
    } else {
        indices
    }
}

// Returns None for shapes that could hide wildcards (subquery, SELECT *, set op, non-table factor).
fn collect_referenced_columns(sql: &str) -> Option<FxHashSet<String>> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let is_wildcard = |item: &SelectItem| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    };
    if select.projection.iter().any(is_wildcard) {
        return None;
    }
    for twj in &select.from {
        if !matches!(twj.relation, TableFactor::Table { .. }) {
            return None;
        }
        if twj
            .joins
            .iter()
            .any(|j| !matches!(j.relation, TableFactor::Table { .. }))
        {
            return None;
        }
    }

    let mut cols = FxHashSet::default();
    let mut complex = false;
    let _ = visit_expressions(query.as_ref(), |expr| {
        match expr {
            Expr::Identifier(id) => {
                cols.insert(id.value.clone());
            }
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last() {
                    cols.insert(last.value.clone());
                }
            }
            // A subquery could reference columns via a wildcard we can't see.
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                complex = true;
                return ControlFlow::Break(());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    });
    if complex {
        None
    } else {
        Some(cols)
    }
}

/// Returns the single source table name only if there is exactly one FROM/JOIN occurrence.
///
/// A self-join (`events e1 JOIN events e2`) returns `None` even though the base name repeats.
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
        TableFactor::Table { .. } if is_inline_unnest_factor(factor) => {}
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

fn collect_tables_counting(set_expr: &SetExpr, tables: &mut Vec<String>) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_factor_counting(&table_with_joins.relation, tables);
                for join in &table_with_joins.joins {
                    collect_factor_counting(&join.relation, tables);
                }
            }
            // UNNEST in the projection expands rows; the single-source path can't handle it.
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

// Checked on the serialized item; a false positive only forces the safe full-plan path.
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
        // Lateral UNNEST, TVFs, etc. block the single-source path.
        _ => tables.push("\u{0}non_table_factor".to_string()),
    }
}
/// Resolve the real source table from a `TableFactor::Table`.
///
/// sqlparser parses `FROM TUMBLE(events, ts, ...)` as a table named `TUMBLE` with args;
/// for window TVFs the first arg is the actual source.
fn resolve_tvf_source(
    name: &sqlparser::ast::ObjectName,
    args: Option<&sqlparser::ast::TableFunctionArgs>,
) -> String {
    let name_str = match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => normalize_ident(ident),
        _ => name.to_string(),
    };
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

fn first_ident_arg(args: &[FunctionArg]) -> Option<String> {
    match args.first()? {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
            Some(normalize_ident(id))
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
            let mut buf = String::new();
            for (i, part) in parts.iter().enumerate() {
                if i > 0 {
                    buf.push('.');
                }
                buf.push_str(&normalize_ident(part));
            }
            Some(buf)
        }
        _ => None,
    }
}

fn normalize_ident(ident: &Ident) -> String {
    ident.value.clone()
}

pub(crate) struct ProjectionFilterInfo {
    pub(crate) proj_exprs: Vec<datafusion_expr::Expr>,
    pub(crate) filter_predicate: Option<datafusion_expr::Expr>,
    pub(crate) input_df_schema: Arc<datafusion_common::DFSchema>,
}

/// Returns `Some` only for `Projection? -> Filter? -> TableScan` plans.
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
        _ => match extract_filter_or_scan(plan) {
            Some((filter_pred, input_schema, _)) => {
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

use crate::operator::lookup_enrich::{disambiguated_lookup_name, LookupEnrichConfig};

/// Detect a partial lookup-enrich join and return its operator config plus residual projection.
///
/// `partial_cols` maps each partial lookup table to its columns; `source_schemas` supplies
/// stream-side schemas for collision disambiguation.
///
/// Returns `None` (`DataFusion` path) for anything other than: single INNER/LEFT equi-join step,
/// single-column key, stream on the left, known stream schema.
pub(crate) fn detect_lookup_enrich_query(
    sql: &str,
    partial_cols: &FxHashMap<String, Vec<String>>,
    source_schemas: &FxHashMap<String, SchemaRef>,
) -> (Option<LookupEnrichConfig>, Option<String>) {
    use laminar_sql::datafusion::lookup_join::LookupJoinType;
    use laminar_sql::parser::join_parser::JoinType;

    if partial_cols.is_empty() {
        return (None, None);
    }
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
    let has_group_by = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        sqlparser::ast::GroupByExpr::All(_) => false,
    };
    if select.distinct.is_some()
        || has_group_by
        || select.having.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || query.with.is_some()
    {
        return (None, None);
    }
    let Ok(Some(multi)) = analyze_joins(select) else {
        return (None, None);
    };
    if multi.joins.len() != 1 {
        return (None, None);
    }
    let j = &multi.joins[0];
    if j.is_temporal_join() || j.time_bound.is_some() || !j.additional_key_columns.is_empty() {
        return (None, None);
    }
    let lookup_table = j.right_table.clone();
    let Some(lookup_cols) = partial_cols.get(&lookup_table) else {
        return (None, None);
    };
    let stream_table = j.left_table.clone();
    let Some(stream_schema) = source_schemas.get(&stream_table) else {
        return (None, None);
    };
    let stream_cols: Vec<String> = stream_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let join_type = match j.join_type {
        JoinType::Inner => LookupJoinType::Inner,
        JoinType::Left => LookupJoinType::LeftOuter,
        _ => return (None, None),
    };
    let projection_sql = build_lookup_projection_sql(
        select,
        j,
        &stream_table,
        &lookup_table,
        &stream_cols,
        lookup_cols,
    );
    let config = LookupEnrichConfig {
        table_name: lookup_table,
        key_columns: vec![j.left_key_column.clone()],
        join_type,
    };
    (Some(config), Some(projection_sql))
}

struct LookupRewriteCtx<'a> {
    stream_alias: Option<&'a str>,
    stream_table: &'a str,
    lookup_alias: Option<&'a str>,
    lookup_table: &'a str,
    stream_cols: &'a [String],
    lookup_cols: &'a [String],
}

impl LookupRewriteCtx<'_> {
    fn is_stream(&self, table: &str) -> bool {
        Some(table) == self.stream_alias || table == self.stream_table
    }
    fn is_lookup(&self, table: &str) -> bool {
        Some(table) == self.lookup_alias || table == self.lookup_table
    }
    fn lookup_name(&self, col: &str) -> String {
        disambiguated_lookup_name(col, self.stream_cols, self.lookup_table)
    }
}

fn build_lookup_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    stream_table: &str,
    lookup_table: &str,
    stream_cols: &[String],
    lookup_cols: &[String],
) -> String {
    let ctx = LookupRewriteCtx {
        stream_alias: analysis.left_alias.as_deref(),
        stream_table,
        lookup_alias: analysis.right_alias.as_deref(),
        lookup_table,
        stream_cols,
        lookup_cols,
    };
    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| rewrite_lookup_select_item(item, &ctx))
        .collect();
    let where_clause = select
        .selection
        .as_ref()
        .map(|e| format!(" WHERE {}", rewrite_lookup_expr(e, &ctx)));
    format!(
        "SELECT {} FROM __lookup_enrich_tmp{}",
        items.join(", "),
        where_clause.unwrap_or_default()
    )
}

/// Temp table name the changelog batch is registered under for the enrich-join SQL.
pub(crate) const CHANGELOG_ENRICH_TMP: &str = "__changelog_enrich_tmp";

/// A `<incremental MV> JOIN <static table>` dimension enrichment.
pub(crate) struct ChangelogEnrichConfig {
    /// The left (incremental MV / changelog) table the operator consumes from `input_bufs`.
    pub changelog_table: String,
    /// Static dimension relation on the right side.
    pub static_table: String,
    /// Ordered left equi-join keys certified by detection.
    pub left_keys: Vec<String>,
    /// Ordered right equi-join keys certified by detection.
    pub right_keys: Vec<String>,
    /// Whether this is a LEFT rather than INNER join.
    pub left_outer: bool,
    /// Temp-rewritten join SQL (over [`CHANGELOG_ENRICH_TMP`]) that preserves `__weight`.
    pub projection_sql: String,
}

/// Detect a single equi-join of an incremental MV (changelog) left and a static table right; returns
/// the changelog table and a `__weight`-preserving temp-rewritten join SQL, else `None`.
pub(crate) fn detect_changelog_enrich_query(
    sql: &str,
    incremental_mvs: &FxHashSet<String>,
    static_tables: &FxHashSet<String>,
) -> Option<ChangelogEnrichConfig> {
    use laminar_sql::parser::join_parser::JoinType;

    if incremental_mvs.is_empty() || static_tables.is_empty() {
        return None;
    }
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
    let has_group_by = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
        sqlparser::ast::GroupByExpr::All(_) => false,
    };
    if select.distinct.is_some()
        || has_group_by
        || select.having.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || query.with.is_some()
    {
        return None;
    }
    let multi = analyze_joins(select).ok()??;
    if multi.joins.len() != 1 {
        return None;
    }
    let j = &multi.joins[0];
    if j.is_temporal_join() || j.time_bound.is_some() {
        return None;
    }
    // Only changelog-left to static-right enrichment is supported. Every other changelog join
    // shape is rejected by DDL and graph admission.
    if !incremental_mvs.contains(&j.left_table) || !static_tables.contains(&j.right_table) {
        return None;
    }
    let join_kw = match j.join_type {
        JoinType::Inner => "JOIN",
        JoinType::Left => "LEFT JOIN",
        _ => return None,
    };

    // The ON clause is reconstructed from the extracted equi-keys only, so a non-equi residual
    // (e.g. `AND a.x > b.y`) would be silently dropped and widen the join — reject it so general
    // execution honors the residual instead.
    if !single_join_on_is_pure_equi(select) {
        return None;
    }
    // An aliasless left table is emitted as `... AS {name}`; a compound (schema-qualified) name
    // would produce invalid SQL (`AS schema.tbl`). Reject so the user adds an explicit alias. Use
    // the identifier part-count, not a `.` scan — a quoted `"a.b"` is a single legal identifier.
    if j.left_alias.is_none()
        && select.from.first().is_some_and(
            |t| matches!(&t.relation, TableFactor::Table { name, .. } if name.0.len() > 1),
        )
    {
        return None;
    }

    let weight = laminar_core::changelog::WEIGHT_COLUMN;
    let lalias = j.left_alias.as_deref().unwrap_or(&j.left_table);
    let ralias = j.right_alias.as_deref().unwrap_or(&j.right_table);

    let mut items: Vec<String> = select.projection.iter().map(ToString::to_string).collect();
    let has_wildcard = select.projection.iter().any(|i| {
        matches!(
            i,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    });
    if !has_wildcard {
        items.push(format!("{lalias}.\"{weight}\""));
    }

    let mut on_clauses = vec![format!(
        "{lalias}.\"{}\" = {ralias}.\"{}\"",
        j.left_key_column, j.right_key_column
    )];
    for (lk, rk) in &j.additional_key_columns {
        on_clauses.push(format!("{lalias}.\"{lk}\" = {ralias}.\"{rk}\""));
    }
    let on = on_clauses.join(" AND ");
    let right_from = match &j.right_alias {
        Some(a) => format!("{} AS {a}", j.right_table),
        None => j.right_table.clone(),
    };
    let where_clause = select
        .selection
        .as_ref()
        .map_or(String::new(), |e| format!(" WHERE {e}"));
    let projection_sql = format!(
        "SELECT {} FROM {CHANGELOG_ENRICH_TMP} AS {lalias} {join_kw} {right_from} ON {on}{where_clause}",
        items.join(", ")
    );
    let mut left_keys = vec![j.left_key_column.clone()];
    let mut right_keys = vec![j.right_key_column.clone()];
    left_keys.extend(
        j.additional_key_columns
            .iter()
            .map(|(left, _)| left.clone()),
    );
    right_keys.extend(
        j.additional_key_columns
            .iter()
            .map(|(_, right)| right.clone()),
    );
    Some(ChangelogEnrichConfig {
        changelog_table: j.left_table.clone(),
        static_table: j.right_table.clone(),
        left_keys,
        right_keys,
        left_outer: j.join_type == JoinType::Left,
        projection_sql,
    })
}

/// `true` if the single join's ON clause is a pure conjunction of `col = col` equalities (or a
/// `USING` list). Anything else (`>`, function, residual predicate) ⇒ the equi-key extractor would
/// silently drop it, so the IVM join must reject the query.
fn single_join_on_is_pure_equi(select: &sqlparser::ast::Select) -> bool {
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    if select.from.len() != 1 {
        return false;
    }
    let twj = &select.from[0];
    if twj.joins.len() != 1 {
        return false;
    }
    let (JoinOperator::Inner(constraint)
    | JoinOperator::Join(constraint)
    | JoinOperator::Left(constraint)
    | JoinOperator::LeftOuter(constraint)) = &twj.joins[0].join_operator
    else {
        return false;
    };
    match constraint {
        JoinConstraint::On(expr) => on_expr_is_pure_equi(expr),
        JoinConstraint::Using(_) => true,
        _ => false,
    }
}

fn on_expr_is_pure_equi(expr: &Expr) -> bool {
    use sqlparser::ast::BinaryOperator;
    let is_col = |e: &Expr| matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
    match expr {
        Expr::Nested(inner) => on_expr_is_pure_equi(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => on_expr_is_pure_equi(left) && on_expr_is_pure_equi(right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => is_col(left) && is_col(right),
        _ => false,
    }
}

fn rewrite_lookup_select_item(item: &SelectItem, ctx: &LookupRewriteCtx) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => rewrite_lookup_expr(expr, ctx),
        SelectItem::ExprWithAlias { expr, alias } => {
            format!("{} AS {alias}", rewrite_lookup_expr(expr, ctx))
        }
        SelectItem::Wildcard(WildcardAdditionalOptions { .. }) => "*".to_string(),
        SelectItem::QualifiedWildcard(name, _) => {
            let table = name.to_string();
            if ctx.is_stream(&table) {
                ctx.stream_cols.join(", ")
            } else if ctx.is_lookup(&table) {
                ctx.lookup_cols
                    .iter()
                    .map(|c| ctx.lookup_name(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                format!("{table}.*")
            }
        }
    }
}

fn rewrite_lookup_expr(expr: &Expr, ctx: &LookupRewriteCtx) -> String {
    rewrite_join_expr(expr, &|e: &Expr| {
        let Expr::CompoundIdentifier(parts) = e else {
            return None;
        };
        if parts.len() != 2 {
            return None;
        }
        let table = parts[0].value.as_str();
        let col = parts[1].value.as_str();
        Some(if ctx.is_stream(table) {
            col.to_string()
        } else if ctx.is_lookup(table) {
            ctx.lookup_name(col)
        } else {
            e.to_string()
        })
    })
}

mod ai {
    use super::{
        BackendKind, DbError, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
        ModelRegistry, ObjectName, ObjectNamePart, SelectItem, SetExpr, Statement, TableFactor,
        Task,
    };

    pub(crate) const AI_TMP_TABLE: &str = "__ai_tmp";

    /// A query routable to the AI operator: exactly one `ai_*` call plus residual projection.
    pub(crate) struct AiQueryPlan {
        pub call: AiCallSpec,
        /// Residual SQL over [`AI_TMP_TABLE`] with the `ai_*` item rewritten to its alias column.
        pub projection_sql: String,
        pub source_table: String,
    }

    /// Plan a query for AI routing: exactly one aliased `ai_*` call over a single source table.
    ///
    /// Returns `None` for multiple AI calls, missing alias, or any join.
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

        let mut found: Option<(usize, AiCallSpec)> = None;
        for (index, item) in select.projection.iter().enumerate() {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => continue,
            };
            if let Some(spec) = ai_call_from_expr(expr, alias) {
                if found.is_some() {
                    return None;
                }
                found = Some((index, spec));
            }
        }
        let (index, call) = found?;

        // Rewrite: AI item → bare alias column; FROM → temp table.
        // Reuse the original alias Ident so quoted aliases stay quoted.
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
        pub task: Task,
        pub model: Option<String>,
        pub labels: Option<Vec<String>>,
        /// Empty when the first argument was missing or not a plain column.
        pub input: String,
        pub output_alias: Option<String>,
        /// Non-empty means the call is malformed; surfaced by [`validate_ai_calls`].
        pub parse_errors: Vec<String>,
    }

    /// Detect `ai_*` calls in the top-level SELECT projection of `sql`.
    ///
    /// Calls nested in expressions or `WHERE` are not recognised here; the marker UDF
    /// rejects them. Returns an empty vec if the SQL does not parse or has no AI calls.
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
                        // Only a plain column reference — the operator does a name-based lookup.
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

    fn column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        }
    }

    // Must stay in step with the marker list in laminar-sql's ai_udf.
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

    fn string_array_literal(expr: &Expr) -> Option<Vec<String>> {
        let Expr::Array(array) = expr else {
            return None;
        };
        array.elem.iter().map(string_literal).collect()
    }

    /// Validate every detected AI call against the model registry.
    ///
    /// Fails at plan time for an unknown model, unsupported task, or a labels mismatch.
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
                // Remote sentiment returns a numeric score, so no candidate set needed.
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
    mod ai_detection_tests;
}

pub(crate) use ai::{detect_ai_functions, plan_ai_query, validate_ai_calls, AiQueryPlan};

/// Temp table a window-frame operator writes enriched batches to; the residual projection reads from it.
pub(crate) const FRAME_TMP_TABLE: &str = "__frame_tmp";

/// A routable bivariate moment frame query (`CORR`/`COVAR_SAMP`/`COVAR_POP OVER …`).
pub(crate) struct FrameQueryPlan {
    pub func: MomentFn,
    pub x_column: String,
    pub y_column: String,
    pub output_alias: String,
    /// Residual SELECT over [`FRAME_TMP_TABLE`] with the stat call replaced by its alias.
    pub projection_sql: String,
    pub source_table: String,
    /// Rows of preceding history to retain per new row (`max(PRECEDING)`).
    pub retain: usize,
}

/// Plan a query for window-frame routing.
///
/// Returns `Some` only for: single un-joined source, one `CORR`/`COVAR_SAMP`/`COVAR_POP`
/// `OVER (ORDER BY … ROWS N PRECEDING) AS alias`, no `PARTITION BY` or `FOLLOWING`.
pub(crate) fn plan_frame_query(sql: &str) -> Option<FrameQueryPlan> {
    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
    let mut statement = match statements.into_iter().next()? {
        laminar_sql::parser::StreamingStatement::Standard(boxed) => *boxed,
        _ => return None,
    };

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

    let mut found: Option<(usize, MomentFn, String, String, String)> = None;
    for (index, item) in select.projection.iter().enumerate() {
        let SelectItem::ExprWithAlias { expr, alias } = item else {
            continue;
        };
        if let Some((func, x, y)) = moment_call(expr) {
            if found.is_some() {
                return None;
            }
            found = Some((index, func, x, y, alias.value.clone()));
        }
    }
    let (index, func, x_column, y_column, output_alias) = found?;

    // Rewrite: stat call → bare alias column; FROM → temp table.
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

fn temporal_projection_error(reason: impl Into<String>) -> DbError {
    DbError::Unsupported(format!(
        "temporal join post-projection is unsupported: {}",
        reason.into()
    ))
}

fn unquoted_identifier_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn parse_temporal_query(
    sql: &str,
    config: &TemporalJoinTranslatorConfig,
) -> Result<
    (
        sqlparser::ast::Select,
        laminar_sql::parser::join_parser::JoinAnalysis,
    ),
    DbError,
> {
    let statements = laminar_sql::parse_streaming_sql(sql)
        .map_err(|error| temporal_projection_error(format!("invalid SQL: {error}")))?;
    let [statement] = statements.as_slice() else {
        return Err(temporal_projection_error(
            "exactly one SELECT statement is required",
        ));
    };

    let laminar_sql::parser::StreamingStatement::Standard(stmt) = statement else {
        return Err(temporal_projection_error(
            "the normalized direct SELECT form is required",
        ));
    };

    let Statement::Query(query) = stmt.as_ref() else {
        return Err(temporal_projection_error("a SELECT query is required"));
    };
    validate_temporal_query_shape(query)?;

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(temporal_projection_error(
            "set operations and nested query bodies are not supported",
        ));
    };

    validate_temporal_select_shape(select)?;
    let multi = analyze_joins(select)
        .map_err(|error| temporal_projection_error(format!("invalid join shape: {error}")))?
        .ok_or_else(|| temporal_projection_error("one temporal join is required"))?;
    let [temporal_analysis] = multi.joins.as_slice() else {
        return Err(temporal_projection_error(
            "exactly one temporal join is required",
        ));
    };
    if !temporal_analysis.is_temporal_join() {
        return Err(temporal_projection_error("the only join must be temporal"));
    }
    validate_temporal_config(temporal_analysis, config)?;
    validate_temporal_projection_items(select, temporal_analysis, config)?;

    Ok((select.as_ref().clone(), temporal_analysis.clone()))
}

fn validate_temporal_query_shape(query: &sqlparser::ast::Query) -> Result<(), DbError> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(temporal_projection_error(
            "WITH, ORDER BY, row limits, locks, output formats, settings, and pipe operators are not supported",
        ));
    }
    Ok(())
}

fn validate_temporal_select_shape(select: &sqlparser::ast::Select) -> Result<(), DbError> {
    let empty_group_by = matches!(
        &select.group_by,
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty()
    );
    if select.flavor != SelectFlavor::Standard
        || select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !empty_group_by
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return Err(temporal_projection_error(
            "DISTINCT, TOP, INTO, PREWHERE, grouping, HAVING, sorting, windows, QUALIFY, and other SELECT modifiers are not supported",
        ));
    }

    let [from] = select.from.as_slice() else {
        return Err(temporal_projection_error(
            "exactly one direct FROM relation is required",
        ));
    };
    let [join] = from.joins.as_slice() else {
        return Err(temporal_projection_error(
            "exactly one direct temporal join is required",
        ));
    };
    validate_temporal_table_factor(&from.relation, false)?;
    validate_temporal_table_factor(&join.relation, true)
}

fn validate_temporal_table_factor(factor: &TableFactor, versioned: bool) -> Result<(), DbError> {
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = factor
    else {
        return Err(temporal_projection_error(
            "derived tables and table functions are not supported",
        ));
    };
    let simple_name = matches!(
        name.0.as_slice(),
        [ObjectNamePart::Identifier(identifier)] if identifier.quote_style.is_none()
    );
    let simple_alias = alias
        .as_ref()
        .is_none_or(|alias| alias.name.quote_style.is_none() && alias.columns.is_empty());
    let expected_version = if versioned {
        matches!(version, Some(TableVersion::ForSystemTimeAsOf(_)))
    } else {
        version.is_none()
    };
    if !simple_name
        || !simple_alias
        || args.is_some()
        || !with_hints.is_empty()
        || !expected_version
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(temporal_projection_error(
            "join inputs must be unquoted single-part tables with optional simple aliases",
        ));
    }
    Ok(())
}

fn validate_temporal_config(
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &TemporalJoinTranslatorConfig,
) -> Result<(), DbError> {
    if config.left_key_columns.is_empty()
        || config.left_key_columns.len() != config.right_key_columns.len()
        || config
            .left_key_columns
            .iter()
            .chain(&config.right_key_columns)
            .any(String::is_empty)
        || config.left_time_column.is_empty()
        || config.right_time_column.is_empty()
        || config.probe_schedule.is_empty()
        || config.probe_schedule.is_multi_horizon() != config.probe_alias.is_some()
    {
        return Err(temporal_projection_error(
            "the translated temporal configuration is incomplete or inconsistent",
        ));
    }

    let left_qualifier = analysis
        .left_alias
        .as_deref()
        .unwrap_or(config.left_table.as_str());
    let right_qualifier = analysis
        .right_alias
        .as_deref()
        .unwrap_or(config.right_table.as_str());
    let qualifier_collision = unquoted_identifier_eq(left_qualifier, right_qualifier)
        || config.probe_alias.as_deref().is_some_and(|probe| {
            unquoted_identifier_eq(probe, left_qualifier)
                || unquoted_identifier_eq(probe, right_qualifier)
        });
    if qualifier_collision {
        return Err(temporal_projection_error(
            "left, right, and probe qualifiers must be distinct",
        ));
    }

    let left_keys = std::iter::once(analysis.left_key_column.as_str())
        .chain(
            analysis
                .additional_key_columns
                .iter()
                .map(|(left, _)| left.as_str()),
        )
        .collect::<Vec<_>>();
    let right_keys = std::iter::once(analysis.right_key_column.as_str())
        .chain(
            analysis
                .additional_key_columns
                .iter()
                .map(|(_, right)| right.as_str()),
        )
        .collect::<Vec<_>>();
    let join_kind_matches = matches!(
        (analysis.join_type, config.join_kind),
        (
            JoinType::Inner,
            laminar_sql::temporal::TemporalJoinKind::Inner
        ) | (
            JoinType::Left,
            laminar_sql::temporal::TemporalJoinKind::Left
        )
    );
    let right_time_matches = analysis
        .right_time_column
        .as_deref()
        .is_none_or(|column| unquoted_identifier_eq(column, &config.right_time_column));
    // TEMPORAL PROBE query SQL is persisted in normalized AS-OF form, so its LIST/RANGE
    // schedule and output alias exist only in the identity-bound translator config.
    let normalized_probe = config.probe_alias.is_some() && analysis.temporal_probe_alias.is_none();
    let probe_metadata_matches = normalized_probe
        || (analysis.temporal_probe_schedule.as_ref() == Some(&config.probe_schedule)
            && match (
                analysis.temporal_probe_alias.as_deref(),
                config.probe_alias.as_deref(),
            ) {
                (Some(left), Some(right)) => unquoted_identifier_eq(left, right),
                (None, None) => true,
                _ => false,
            });
    if !unquoted_identifier_eq(&analysis.left_table, &config.left_table)
        || !unquoted_identifier_eq(&analysis.right_table, &config.right_table)
        || left_keys.len() != config.left_key_columns.len()
        || !left_keys
            .iter()
            .zip(&config.left_key_columns)
            .all(|(left, right)| unquoted_identifier_eq(left, right))
        || right_keys.len() != config.right_key_columns.len()
        || !right_keys
            .iter()
            .zip(&config.right_key_columns)
            .all(|(left, right)| unquoted_identifier_eq(left, right))
        || !analysis
            .left_time_column
            .as_deref()
            .is_some_and(|column| unquoted_identifier_eq(column, &config.left_time_column))
        || !right_time_matches
        || !join_kind_matches
        || !probe_metadata_matches
    {
        return Err(temporal_projection_error(
            "the SQL join does not match its translated temporal configuration",
        ));
    }
    Ok(())
}

fn wildcard_has_options(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_some()
        || options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_replace.is_some()
        || options.opt_rename.is_some()
}

fn validate_temporal_projection_items(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &TemporalJoinTranslatorConfig,
) -> Result<(), DbError> {
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                validate_temporal_expr(expr, analysis, config)?;
            }
            SelectItem::Wildcard(options) if !wildcard_has_options(options) => {}
            SelectItem::Wildcard(_) => {
                return Err(temporal_projection_error(
                    "wildcard modifiers are not supported",
                ));
            }
            SelectItem::QualifiedWildcard(..) => {
                return Err(temporal_projection_error(
                    "qualified wildcards are not supported",
                ));
            }
        }
    }
    if let Some(selection) = &select.selection {
        validate_temporal_expr(selection, analysis, config)?;
    }
    Ok(())
}

fn validate_temporal_expr(
    expression: &Expr,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &TemporalJoinTranslatorConfig,
) -> Result<(), DbError> {
    let result = visit_expressions(expression, |expr| {
        let unsupported = match expr {
            Expr::CompoundIdentifier(parts) => {
                let [qualifier, column] = parts.as_slice() else {
                    return ControlFlow::Break(
                        "column references must have exactly one qualifier".to_string(),
                    );
                };
                let left_qualifier = analysis
                    .left_alias
                    .as_deref()
                    .unwrap_or(config.left_table.as_str());
                let right_qualifier = analysis
                    .right_alias
                    .as_deref()
                    .unwrap_or(config.right_table.as_str());
                let known = unquoted_identifier_eq(&qualifier.value, left_qualifier)
                    || unquoted_identifier_eq(&qualifier.value, right_qualifier)
                    || config
                        .probe_alias
                        .as_deref()
                        .is_some_and(|probe| unquoted_identifier_eq(&qualifier.value, probe));
                if qualifier.quote_style.is_some() || column.quote_style.is_some() || !known {
                    Some("column references must use an unquoted join or probe qualifier")
                } else {
                    None
                }
            }
            Expr::Identifier(_) => Some("unqualified column references are not supported"),
            Expr::Cast { kind, format, .. } if *kind != CastKind::Cast || format.is_some() => {
                Some("only standard CAST expressions without a format clause are supported")
            }
            Expr::Function(_) => Some("function calls are not supported"),
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                Some("subqueries are not supported")
            }
            Expr::Value(_)
            | Expr::BinaryOp { .. }
            | Expr::UnaryOp { .. }
            | Expr::Nested(_)
            | Expr::Cast { .. }
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::Case { .. }
            | Expr::Tuple(_)
            | Expr::Collate { .. } => None,
            _ => Some("this expression form is not supported"),
        };
        unsupported.map_or(ControlFlow::Continue(()), |reason| {
            ControlFlow::Break(reason.to_string())
        })
    });
    match result {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(reason) => Err(temporal_projection_error(reason)),
    }
}

pub(crate) fn has_temporal_query(sql: &str) -> bool {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return false;
    };
    statements.iter().any(|statement| match statement {
        laminar_sql::parser::StreamingStatement::Standard(statement) => {
            laminar_sql::temporal::temporal_table_version_count(statement) > 0
        }
        laminar_sql::parser::StreamingStatement::TemporalProbeQuery { .. } => true,
        _ => false,
    })
}

pub(crate) fn temporal_projection_sql(
    sql: &str,
    config: &TemporalJoinTranslatorConfig,
) -> Result<String, DbError> {
    let (select, temporal_analysis) = parse_temporal_query(sql, config)?;
    Ok(build_temporal_projection_sql(
        &select,
        &temporal_analysis,
        config,
    ))
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

// `p.col` → `col`; literals and other nodes pass through via `to_string()`.
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
            }
            (false, true) => {
                right_strs.push(expr_to_sql_strip_alias(pred, right_alias));
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

pub(crate) fn detect_unbounded_join_steps(sql: &str) -> Option<Vec<(String, String)>> {
    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
    let laminar_sql::parser::StreamingStatement::Standard(statement) = statements.first()? else {
        return None;
    };
    let Statement::Query(query) = statement.as_ref() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let multi = analyze_joins(select).ok()??;
    let steps: Vec<_> = multi
        .joins
        .iter()
        .filter(|join| !join.is_bounded())
        .map(|join| (join.left_table.clone(), join.right_table.clone()))
        .collect();
    (!steps.is_empty()).then_some(steps)
}

pub(crate) fn has_join_clause(sql: &str) -> bool {
    join_clause_count(sql) > 0
}

pub(crate) fn join_clause_count(sql: &str) -> usize {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return 0;
    };
    let Some(laminar_sql::parser::StreamingStatement::Standard(statement)) = statements.first()
    else {
        return 0;
    };
    let Statement::Query(query) = statement.as_ref() else {
        return 0;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return 0;
    };
    select
        .from
        .iter()
        .filter(|from| !is_inline_unnest_factor(&from.relation))
        .count()
        .saturating_sub(1)
        + select
            .from
            .iter()
            .map(|from| from.joins.len())
            .sum::<usize>()
}

fn is_inline_unnest_factor(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::UNNEST { .. } => true,
        TableFactor::Table {
            name,
            args: Some(_),
            ..
        }
        | TableFactor::Function { name, .. } => {
            name.0.len() == 1 && name.to_string().eq_ignore_ascii_case("unnest")
        }
        _ => false,
    }
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

    if select.from.len() != 1 || select.from[0].joins.len() != 1 {
        return None;
    }
    let stream_analysis = analyze_join(select).ok()??;
    if stream_analysis.time_bound.is_none()
        || stream_analysis.is_temporal_join()
        || stream_analysis.is_lookup_join
    {
        return None;
    }

    let Ok(JoinOperatorConfig::StreamStream(config)) =
        JoinOperatorConfig::from_analysis(&stream_analysis)
    else {
        return None;
    };

    if config.left_time_column.is_empty() || config.right_time_column.is_empty() {
        return None;
    }

    let pre_filters =
        if config.join_type == JoinType::Inner && config.left_table == config.right_table {
            extract_self_join_pre_filters(select, &stream_analysis, &config)
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
        build_stream_join_projection_sql(select, &stream_analysis, &config, &where_clause);

    Some(StreamJoinDetection {
        config,
        projection_sql,
        left_pre_filter: pre_filters.as_ref().and_then(|f| f.left_sql.clone()),
        right_pre_filter: pre_filters.as_ref().and_then(|f| f.right_sql.clone()),
    })
}

pub(crate) fn has_unaliased_projection(sql: &str) -> bool {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return false;
    };
    let Some(laminar_sql::parser::StreamingStatement::Standard(statement)) = statements.first()
    else {
        return false;
    };
    let Statement::Query(query) = statement.as_ref() else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select
        .projection
        .iter()
        .any(|item| !matches!(item, SelectItem::ExprWithAlias { .. }))
}

/// Apply a global Top-K limit across all batches.
///
/// `DataFusion`'s `LIMIT N` is per micro-batch; this slices the combined result to `k` rows.
pub(crate) fn apply_topk_filter(batches: &[RecordBatch], k: usize) -> Vec<RecordBatch> {
    if batches.is_empty() || k == 0 {
        return Vec::new();
    }

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows <= k {
        return batches.to_vec();
    }

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

/// Rewrite an expression to SQL over a flattened join temp table.
///
/// `leaf` resolves qualified column references for the specific join type, returning `None`
/// to fall through to the shared structural recursion. Shared by temporal,
/// stream-stream, and lookup rewriters.
fn rewrite_join_expr<F: Fn(&Expr) -> Option<String>>(expr: &Expr, leaf: &F) -> String {
    if let Some(s) = leaf(expr) {
        return s;
    }
    let r = |e: &Expr| rewrite_join_expr(e, leaf);
    match expr {
        Expr::BinaryOp { left, op, right } => format!("{} {op} {}", r(left), r(right)),
        Expr::UnaryOp { op, expr: inner } => format!("{op} {}", r(inner)),
        Expr::Nested(inner) => format!("({})", r(inner)),
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => format!("CAST({} AS {data_type})", r(inner)),
        Expr::IsNull(inner) => format!("{} IS NULL", r(inner)),
        Expr::IsNotNull(inner) => format!("{} IS NOT NULL", r(inner)),
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => {
            let (e, l, h) = (r(inner), r(low), r(high));
            if *negated {
                format!("{e} NOT BETWEEN {l} AND {h}")
            } else {
                format!("{e} BETWEEN {l} AND {h}")
            }
        }
        Expr::Function(func) => {
            let args: Vec<String> = match &func.args {
                sqlparser::ast::FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => r(e),
                        other => other.to_string(),
                    })
                    .collect(),
                other => vec![other.to_string()],
            };
            format!("{}({})", func.name, args.join(", "))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let list_str: Vec<String> = list.iter().map(r).collect();
            let op = if *negated { "NOT IN" } else { "IN" };
            format!("{} {op} ({})", r(expr), list_str.join(", "))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let op = if *negated { "NOT IN" } else { "IN" };
            format!("{} {op} ({subquery})", r(expr))
        }
        Expr::Exists { subquery, negated } => {
            let op = if *negated { "NOT EXISTS" } else { "EXISTS" };
            format!("{op} ({subquery})")
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            let operand_str = operand
                .as_ref()
                .map_or(String::new(), |op| format!("{} ", r(op)));
            let mut wens = Vec::new();
            for cw in conditions {
                wens.push(format!("WHEN {} THEN {}", r(&cw.condition), r(&cw.result)));
            }
            let else_str = else_result
                .as_ref()
                .map_or(String::new(), |el| format!(" ELSE {}", r(el)));
            format!("CASE {operand_str}{} {else_str} END", wens.join(" "))
        }
        Expr::Tuple(exprs) => {
            let tuple_str: Vec<String> = exprs.iter().map(r).collect();
            format!("({})", tuple_str.join(", "))
        }
        Expr::Collate { expr, collation } => {
            format!("{} COLLATE {collation}", r(expr))
        }
        Expr::Subquery(subquery) => {
            format!("({subquery})")
        }
        _ => expr.to_string(),
    }
}

fn build_temporal_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &TemporalJoinTranslatorConfig,
) -> String {
    let left_qualifier = analysis
        .left_alias
        .as_deref()
        .unwrap_or(config.left_table.as_str());
    let right_qualifier = analysis
        .right_alias
        .as_deref()
        .unwrap_or(config.right_table.as_str());

    let items: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                rewrite_temporal_expr(expr, left_qualifier, right_qualifier, config)
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let rewritten =
                    rewrite_temporal_expr(expr, left_qualifier, right_qualifier, config);
                format!("{rewritten} AS {alias}")
            }
            SelectItem::Wildcard(_) => "*".to_string(),
            SelectItem::QualifiedWildcard(name, _) => {
                let table = name.to_string();
                if unquoted_identifier_eq(&table, left_qualifier)
                    || unquoted_identifier_eq(&table, right_qualifier)
                {
                    "*".to_string()
                } else {
                    format!("{table}.*")
                }
            }
        })
        .collect();

    let select_clause = items.join(", ");

    let where_clause = select.selection.as_ref().map(|expr| {
        let rewritten = rewrite_temporal_expr(expr, left_qualifier, right_qualifier, config);
        format!(" WHERE {rewritten}")
    });

    format!(
        "SELECT {select_clause} FROM __temporal_tmp{}",
        where_clause.unwrap_or_default()
    )
}

fn rewrite_temporal_expr(
    expr: &Expr,
    left_qualifier: &str,
    right_qualifier: &str,
    config: &TemporalJoinTranslatorConfig,
) -> String {
    rewrite_join_expr(expr, &|e: &Expr| {
        let Expr::CompoundIdentifier(parts) = e else {
            return None;
        };
        if parts.len() != 2 {
            return None;
        }
        let table = parts[0].value.as_str();
        let column = parts[1].value.as_str();
        let is_left = unquoted_identifier_eq(table, left_qualifier);
        let is_right = unquoted_identifier_eq(table, right_qualifier);
        let is_probe = config
            .probe_alias
            .as_deref()
            .is_some_and(|probe| unquoted_identifier_eq(table, probe));
        Some(if is_left || is_probe {
            column.to_string()
        } else if is_right {
            format!("{column}_{}", config.right_table)
        } else {
            e.to_string()
        })
    })
}

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
        .map(|item| render_join_projection_item(item, left_alias, right_alias, config))
        .collect();

    format!(
        "SELECT {} FROM __interval_tmp{where_clause}",
        items.join(", ")
    )
}

fn render_join_projection_item(
    item: &SelectItem,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &StreamJoinConfig,
) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            rewrite_stream_join_expr(expr, left_alias, right_alias, config)
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            let rewritten = rewrite_stream_join_expr(expr, left_alias, right_alias, config);
            format!("{rewritten} AS {alias}")
        }
        SelectItem::Wildcard(_) => "*".to_string(),
        // DDL rejects qualified wildcards for bounded joins because the internal pair schema
        // renames right-side columns. Keeping the qualifier here makes any bypass fail closed.
        SelectItem::QualifiedWildcard(name, _) => format!("{name}.*"),
    }
}

// Left columns become bare names; right columns get the _<right_table> suffix from IntervalJoinState.
fn rewrite_stream_join_expr(
    expr: &sqlparser::ast::Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &StreamJoinConfig,
) -> String {
    rewrite_join_expr(expr, &|e: &Expr| {
        let Expr::CompoundIdentifier(parts) = e else {
            return None;
        };
        if parts.len() != 2 {
            return None;
        }
        let table = &parts[0].value;
        let col = &parts[1].value;
        let is_left = table == &config.left_table || left_alias.is_some_and(|a| a == table);
        let is_right = table == &config.right_table || right_alias.is_some_and(|a| a == table);
        Some(if is_left || is_right {
            let right_only = matches!(config.join_type, JoinType::RightSemi | JoinType::RightAnti);
            let bare = if is_right && !right_only {
                format!("{col}_{}", config.right_table)
            } else {
                col.clone()
            };
            bare
        } else {
            e.to_string()
        })
    })
}

/// One bound of a `time_col CMP now() ± offset` predicate. `strict` means `>`/`<`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TemporalBound {
    pub(crate) off_ms: i64,
    pub(crate) strict: bool,
}

/// Config for a retracting temporal filter (`WHERE time_col CMP now() ± INTERVAL`).
///
/// Empty `proj_cols` means `SELECT *`.
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
    /// `now()` appears in a shape the retracting filter doesn't support.
    PresentUnrecognized,
}

fn ident_is_wallclock(name: &str) -> bool {
    name.eq_ignore_ascii_case("now") || name.eq_ignore_ascii_case("current_timestamp")
}

fn expr_is_wallclock(expr: &Expr) -> bool {
    // `"now"` is a column; only unquoted bare `now` / `current_timestamp` is wallclock.
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

// now() → 0, now() ± I → ±I (ms).
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

// Returns None for conjuncts, disjunctions, and other unsupported shapes.
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
            // Normalise to col OP now()+off (flip the operator when the column is on the right).
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

    // Only the canonical shape is supported; row-limiting doesn't compose with retraction.
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
mod temporal_filter_recognition_tests;

#[cfg(test)]
mod frame_plan_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod self_join_filter_tests;
