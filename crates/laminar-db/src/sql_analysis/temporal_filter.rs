//! Retracting temporal-filter analysis (`WHERE time_col CMP now() ± INTERVAL`).
//!
//! Classifies whether a query is the canonical single-source retracting filter the managed
//! temporal-filter operator executes: `NotPresent`, `Recognized`, or `PresentUnrecognized`
//! (`now()` in a shape the operator does not support).

use std::ops::ControlFlow;

use sqlparser::ast::{visit_expressions, Expr, SelectItem, SetExpr, Statement};

use super::ast::{ident_is_wallclock, single_function_ident};

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

fn expr_is_wallclock(expr: &Expr) -> bool {
    // `"now"` is a column; only unquoted bare `now` / `current_timestamp` is wallclock.
    fn ident(i: &sqlparser::ast::Ident) -> bool {
        i.quote_style.is_none() && ident_is_wallclock(&i.value)
    }
    match strip_nested(expr) {
        Expr::Function(function) => single_function_ident(&function.name)
            .is_some_and(|ident| ident_is_wallclock(&ident.value)),
        Expr::Identifier(id) => ident(id),
        _ => false,
    }
}

fn query_ast_uses(query: &sqlparser::ast::Query, predicate: fn(&Expr) -> bool) -> bool {
    let mut found = false;
    let _ = visit_expressions(query, |expr| {
        if predicate(expr) {
            found = true;
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    found
}

fn query_ast_uses_wallclock(query: &sqlparser::ast::Query) -> bool {
    query_ast_uses(query, expr_is_wallclock)
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

    let uses_now = query_ast_uses_wallclock(query);
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
