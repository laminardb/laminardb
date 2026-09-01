//! Shared SQL-expression rendering and predicate splitting over flattened join temp tables.
//!
//! The join operators replace qualified references with the flattened temp-table field names;
//! these helpers render arbitrary sqlparser expressions to SQL under that rewrite and split
//! WHERE conjunctions so per-side pre-filters can be extracted.

use sqlparser::ast::{BinaryOperator, Expr};

/// Rewrite an expression to SQL over a flattened join temp table.
///
/// `leaf` resolves qualified column references for the specific join type, returning `None`
/// to fall through to the shared structural recursion. Shared by temporal and lookup rewriters;
/// bounded stream joins use an AST-mutating rewrite so every expression variant is preserved.
pub(super) fn rewrite_join_expr<F: Fn(&Expr) -> Option<String>>(expr: &Expr, leaf: &F) -> String {
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

pub(super) fn split_conjunction_sqlparser(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
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
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            split_conjunction_sqlparser(inner)
        }
        other => vec![other.clone()],
    }
}

pub(super) fn expr_mentions_alias(expr: &Expr, alias: &str) -> bool {
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
pub(super) fn expr_to_sql_strip_alias(expr: &Expr, alias: &str) -> String {
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

pub(super) fn conjoin_predicates(preds: &[Expr]) -> Option<Expr> {
    preds.iter().cloned().reduce(|acc, pred| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOperator::And,
        right: Box::new(pred),
    })
}
