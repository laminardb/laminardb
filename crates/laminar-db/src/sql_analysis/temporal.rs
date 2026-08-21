//! Temporal join post-projection analysis.
//!
//! Validates that a temporal-join query matches its identity-bound translator configuration,
//! then renders the residual projection over the flattened temporal output table. Every
//! rejection is typed: the error text is the observable contract used by DDL admission.

use std::ops::ControlFlow;

use laminar_sql::parser::join_parser::{analyze_joins, JoinAnalysis, JoinType};
use laminar_sql::translator::TemporalJoinTranslatorConfig;
use sqlparser::ast::{
    visit_expressions, CastKind, Expr, SelectFlavor, SelectItem, SetExpr, Statement, TableFactor,
    TableVersion,
};

use crate::error::DbError;

use super::ast::{unquoted_identifier_eq, wildcard_has_options};
use super::expr_sql::rewrite_join_expr;

fn temporal_projection_error(reason: impl Into<String>) -> DbError {
    DbError::Unsupported(format!(
        "temporal join post-projection is unsupported: {}",
        reason.into()
    ))
}

fn parse_temporal_query(
    sql: &str,
    config: &TemporalJoinTranslatorConfig,
) -> Result<(sqlparser::ast::Select, JoinAnalysis), DbError> {
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
        sqlparser::ast::GroupByExpr::Expressions(expressions, modifiers)
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
        [sqlparser::ast::ObjectNamePart::Identifier(identifier)] if identifier.quote_style.is_none()
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
    analysis: &JoinAnalysis,
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

fn validate_temporal_projection_items(
    select: &sqlparser::ast::Select,
    analysis: &JoinAnalysis,
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
    analysis: &JoinAnalysis,
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
                let probe = config
                    .probe_alias
                    .as_deref()
                    .is_some_and(|probe| unquoted_identifier_eq(&qualifier.value, probe));
                let known_join = unquoted_identifier_eq(&qualifier.value, left_qualifier)
                    || unquoted_identifier_eq(&qualifier.value, right_qualifier);
                if qualifier.quote_style.is_some() || column.quote_style.is_some() {
                    Some("column references must use an unquoted join or probe qualifier")
                } else if probe
                    && !["offset_ms", "probe_time"]
                        .iter()
                        .any(|name| unquoted_identifier_eq(&column.value, name))
                {
                    Some("the probe qualifier exposes only offset_ms and probe_time")
                } else if !known_join && !probe {
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
    temporal_projection_sql_for_input(sql, config, "__temporal_tmp")
}

const TEMPORAL_PROJECTION_INPUT_ALIAS: &str = "__temporal_projection_input";

pub(crate) fn temporal_projection_sql_for_input(
    sql: &str,
    config: &TemporalJoinTranslatorConfig,
    input_table: &str,
) -> Result<String, DbError> {
    let (select, temporal_analysis) = parse_temporal_query(sql, config)?;
    Ok(build_temporal_projection_sql(
        &select,
        &temporal_analysis,
        config,
        input_table,
    ))
}

fn build_temporal_projection_sql(
    select: &sqlparser::ast::Select,
    analysis: &JoinAnalysis,
    config: &TemporalJoinTranslatorConfig,
    input_table: &str,
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
                let rewritten =
                    rewrite_temporal_expr(expr, left_qualifier, right_qualifier, config);
                if matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                    rewritten
                } else {
                    let alias = expr.to_string().replace('"', "\"\"");
                    format!("{rewritten} AS \"{alias}\"")
                }
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
        "SELECT {select_clause} FROM {input_table} AS {TEMPORAL_PROJECTION_INPUT_ALIAS}{}",
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
