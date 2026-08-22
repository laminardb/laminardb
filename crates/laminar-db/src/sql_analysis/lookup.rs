//! Partial lookup-enrich join detection and projection pushdown.
//!
//! Decides whether a query is the single INNER/LEFT equi-join a dedicated lookup operator can
//! execute, computes the partial-table column projection for fetch pushdown, and renders the
//! residual projection over the operator's flattened temp table.

use std::ops::ControlFlow;

use arrow::datatypes::SchemaRef;
use rustc_hash::{FxHashMap, FxHashSet};
use sqlparser::ast::{
    visit_expressions, Expr, SelectItem, SetExpr, Statement, TableFactor, WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::operator::lookup_enrich::{disambiguated_lookup_name, LookupEnrichConfig};

use super::expr_sql::rewrite_join_expr;
use super::table_refs::extract_table_references;

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
    let Ok(Some(multi)) = laminar_sql::parser::join_parser::analyze_joins(select) else {
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
