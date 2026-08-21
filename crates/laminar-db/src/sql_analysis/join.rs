//! Bounded stream-join detection and projection rewriting.
//!
//! Detects interval-kernel join steps, certifies the single bounded stream-stream join the
//! interval operator executes, and renders its projection over the flattened `__interval_tmp`
//! pair table — including self-join WHERE-split pre-filters.

use std::ops::ControlFlow;

use laminar_sql::parser::join_parser::{analyze_join, analyze_joins, JoinType};
use laminar_sql::translator::{JoinOperatorConfig, StreamJoinConfig};
use sqlparser::ast::{visit_expressions_mut, Expr, Ident, SelectItem, SetExpr, Statement};

use super::ast::is_inline_unnest_factor;
use super::expr_sql::{
    conjoin_predicates, expr_mentions_alias, expr_to_sql_strip_alias, split_conjunction_sqlparser,
};

pub(crate) struct StreamJoinDetection {
    pub config: StreamJoinConfig,
    pub projection_sql: String,
    /// Projection over the weighted interval-kernel output. The engine-owned trailing
    /// `__weight` is selected inside the same SQL projection so compiled and cached execution
    /// apply an identical filter to the row and its weight.
    pub weighted_projection_sql: String,
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

    if select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::ExprWithAliases { .. }))
    {
        return None;
    }

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
        build_stream_join_projection_sql(select, &stream_analysis, &config, &where_clause, false);
    let weighted_projection_sql =
        build_stream_join_projection_sql(select, &stream_analysis, &config, &where_clause, true);

    Some(StreamJoinDetection {
        config,
        projection_sql,
        weighted_projection_sql,
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

/// Whether a bounded-join projection/filter contains a column reference whose input side cannot be
/// proven from the SQL text alone. The interval kernel renames right-side fields, so accepting an
/// unqualified reference would make an otherwise unambiguous source column ambiguous after the pair
/// schema is built. Parse/shape failures are fail-closed; callers invoke this only for planned joins.
pub(crate) fn has_unqualified_interval_output_column(sql: &str) -> bool {
    let Ok(statements) = laminar_sql::parse_streaming_sql(sql) else {
        return true;
    };
    let Some(laminar_sql::parser::StreamingStatement::Standard(statement)) = statements.first()
    else {
        return true;
    };
    let Statement::Query(query) = statement.as_ref() else {
        return true;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return true;
    };
    let has_unqualified = |expr: &Expr| {
        sqlparser::ast::visit_expressions(expr, |nested| {
            if matches!(nested, Expr::Identifier(_)) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_break()
    };
    select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr)
        | SelectItem::ExprWithAlias { expr, .. }
        | SelectItem::ExprWithAliases { expr, .. } => has_unqualified(expr),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
    }) || select.selection.as_ref().is_some_and(has_unqualified)
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

fn build_stream_join_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &laminar_sql::parser::join_parser::JoinAnalysis,
    config: &StreamJoinConfig,
    where_clause: &str,
    preserve_weight: bool,
) -> String {
    let left_alias = analysis.left_alias.as_deref();
    let right_alias = analysis.right_alias.as_deref();

    let mut items: Vec<String> = select
        .projection
        .iter()
        .map(|item| render_join_projection_item(item, left_alias, right_alias, config))
        .collect();
    let unqualified_wildcard = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)));
    if preserve_weight && !unqualified_wildcard {
        let weight = laminar_core::changelog::WEIGHT_COLUMN;
        items.push(format!("\"{weight}\" AS \"{weight}\""));
    }

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
        SelectItem::ExprWithAliases { .. } => item.to_string(),
        SelectItem::Wildcard(_) => "*".to_string(),
        // DDL rejects qualified wildcards for bounded joins because the internal pair schema
        // renames right-side columns. Keeping the qualifier here makes any bypass fail closed.
        SelectItem::QualifiedWildcard(name, _) => format!("{name}.*"),
    }
}

// Left columns keep their source names; right columns get the _<right_table> suffix from
// IntervalJoinState. Rewrite the cloned AST instead of enumerating expression variants so qualified
// references nested in LIKE, truth tests, function clauses, or future sqlparser expressions cannot
// leak their source qualifier into the flattened temporary table. Always quote the generated field
// name: a valid source identifier may contain whitespace, punctuation, or a reserved word, and the
// temporary pair table exposes that exact Arrow field name.
fn rewrite_stream_join_expr(
    expr: &sqlparser::ast::Expr,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    config: &StreamJoinConfig,
) -> String {
    let mut rewritten = expr.clone();
    let _ = visit_expressions_mut(&mut rewritten, |nested| {
        let replacement = match nested {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let table = &parts[0].value;
                let column = &parts[1].value;
                let is_left =
                    table == &config.left_table || left_alias.is_some_and(|alias| alias == table);
                let is_right =
                    table == &config.right_table || right_alias.is_some_and(|alias| alias == table);
                let right_only =
                    matches!(config.join_type, JoinType::RightSemi | JoinType::RightAnti);
                if is_right {
                    Some(if right_only {
                        column.clone()
                    } else {
                        format!("{column}_{}", config.right_table)
                    })
                } else if is_left && !right_only {
                    Some(column.clone())
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(field) = replacement {
            *nested = Expr::Identifier(Ident::with_quote('"', field));
        }
        ControlFlow::<()>::Continue(())
    });
    rewritten.to_string()
}
