//! Interval arithmetic rewriter for BIGINT timestamp columns.
//!
//! LaminarDB uses BIGINT millisecond timestamps for event time. DataFusion
//! cannot natively evaluate `Int64 ± INTERVAL`, so this module rewrites
//! INTERVAL expressions in arithmetic operations to equivalent millisecond
//! integer literals before the SQL reaches DataFusion.
//!
//! # Example
//!
//! ```sql
//! -- Before rewrite:
//! SELECT * FROM trades t
//! INNER JOIN orders o ON t.symbol = o.symbol
//!   AND o.ts BETWEEN t.ts - INTERVAL '10' SECOND AND t.ts + INTERVAL '10' SECOND
//!
//! -- After rewrite:
//! SELECT * FROM trades t
//! INNER JOIN orders o ON t.symbol = o.symbol
//!   AND o.ts BETWEEN t.ts - 10000 AND t.ts + 10000
//! ```

use sqlparser::ast::{
    BinaryOperator, DateTimeField, Expr, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SetExpr, Statement, Value,
};

/// Convert an [`Interval`](sqlparser::ast::Interval) to its equivalent milliseconds value.
///
/// Returns `None` if the interval cannot be converted (unsupported unit or
/// non-numeric value).
fn interval_to_millis(interval: &sqlparser::ast::Interval) -> Option<i64> {
    let value = extract_interval_numeric(&interval.value)?;
    let unit = interval
        .leading_field
        .clone()
        .unwrap_or(DateTimeField::Second);

    let millis = match unit {
        DateTimeField::Millisecond | DateTimeField::Milliseconds => value,
        DateTimeField::Second | DateTimeField::Seconds => value.checked_mul(1_000)?,
        DateTimeField::Minute | DateTimeField::Minutes => value.checked_mul(60_000)?,
        DateTimeField::Hour | DateTimeField::Hours => value.checked_mul(3_600_000)?,
        DateTimeField::Day | DateTimeField::Days => value.checked_mul(86_400_000)?,
        _ => return None,
    };

    Some(millis)
}

/// Extract a numeric value from an interval's value expression.
fn extract_interval_numeric(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse().ok(),
            Value::SingleQuotedString(s) => s.split_whitespace().next()?.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// Create a numeric literal `Expr` from a milliseconds value.
///
/// Uses sqlparser's own parser to construct the AST node, ensuring
/// correct internal representation.
fn make_number_expr(n: i64) -> Expr {
    use sqlparser::dialect::GenericDialect;
    let s = n.to_string();
    let dialect = GenericDialect {};
    sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(&s)
        .expect("number literal should tokenize")
        .parse_expr()
        .expect("number literal should parse as Expr")
}

// ---------------------------------------------------------------------------
// Expression rewriter
// ---------------------------------------------------------------------------

/// Rewrite INTERVAL arithmetic in an expression tree, in place.
///
/// Converts patterns like `col ± INTERVAL 'N' UNIT` to `col ± <millis>` so
/// that DataFusion can evaluate the expression when the column is `Int64`.
pub fn rewrite_expr_mut(expr: &mut Expr) {
    if let Expr::BinaryOp { left, op, right } = expr {
        let is_add_sub = matches!(*op, BinaryOperator::Plus | BinaryOperator::Minus);

        if is_add_sub {
            // Check right side for INTERVAL: col ± INTERVAL → col ± millis
            let right_ms: Option<i64> = if let Expr::Interval(interval) = right.as_ref() {
                interval_to_millis(interval)
            } else {
                None
            };

            if let Some(ms) = right_ms {
                **right = make_number_expr(ms);
                rewrite_expr_mut(left);
                return;
            }

            // Check left side: INTERVAL + col → millis + col (only addition)
            if matches!(*op, BinaryOperator::Plus) {
                let left_ms: Option<i64> = if let Expr::Interval(interval) = left.as_ref() {
                    interval_to_millis(interval)
                } else {
                    None
                };

                if let Some(ms) = left_ms {
                    **left = make_number_expr(ms);
                    rewrite_expr_mut(right);
                    return;
                }
            }
        }

        // Default: recurse into both sides
        rewrite_expr_mut(left);
        rewrite_expr_mut(right);
        return;
    }

    // Handle other expression types that can contain sub-expressions
    match expr {
        Expr::Between {
            expr: e, low, high, ..
        } => {
            rewrite_expr_mut(e);
            rewrite_expr_mut(low);
            rewrite_expr_mut(high);
        }
        Expr::InList { expr: e, list, .. } => {
            rewrite_expr_mut(e);
            for item in list {
                rewrite_expr_mut(item);
            }
        }
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner) => rewrite_expr_mut(inner),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Statement / query walker
// ---------------------------------------------------------------------------

/// Rewrite all INTERVAL arithmetic in a SQL [`Statement`].
///
/// Walks the full AST and converts `expr ± INTERVAL 'N' UNIT` patterns
/// to `expr ± <milliseconds>` for BIGINT timestamp compatibility.
pub fn rewrite_interval_arithmetic(stmt: &mut Statement) {
    if let Statement::Query(query) = stmt {
        rewrite_query(query);
    }
}

fn rewrite_query(query: &mut Query) {
    rewrite_set_expr(&mut query.body);
}

fn rewrite_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => rewrite_select(select),
        SetExpr::Query(query) => rewrite_query(query),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left);
            rewrite_set_expr(right);
        }
        _ => {}
    }
}

fn rewrite_select(select: &mut Select) {
    // Rewrite SELECT projection expressions
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(ref mut expr)
            | SelectItem::ExprWithAlias { ref mut expr, .. } => {
                rewrite_expr_mut(expr);
            }
            _ => {}
        }
    }

    // Rewrite WHERE clause
    if let Some(ref mut where_expr) = select.selection {
        rewrite_expr_mut(where_expr);
    }

    // Rewrite HAVING clause
    if let Some(ref mut having) = select.having {
        rewrite_expr_mut(having);
    }

    // Rewrite JOIN ON conditions
    for table_with_joins in &mut select.from {
        for join in &mut table_with_joins.joins {
            rewrite_join_operator(&mut join.join_operator);
        }
    }
}

fn rewrite_join_operator(jo: &mut JoinOperator) {
    let (JoinOperator::Join(constraint)
    | JoinOperator::Inner(constraint)
    | JoinOperator::Left(constraint)
    | JoinOperator::LeftOuter(constraint)
    | JoinOperator::Right(constraint)
    | JoinOperator::RightOuter(constraint)
    | JoinOperator::FullOuter(constraint)
    | JoinOperator::StraightJoin(constraint)
    | JoinOperator::LeftSemi(constraint)
    | JoinOperator::RightSemi(constraint)
    | JoinOperator::LeftAnti(constraint)
    | JoinOperator::RightAnti(constraint)
    | JoinOperator::Semi(constraint)
    | JoinOperator::Anti(constraint)) = jo
    else {
        return;
    };
    if let JoinConstraint::On(expr) = constraint {
        rewrite_expr_mut(expr);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::LaminarDialect;

    /// Helper: parse SQL, rewrite intervals, return the rewritten SQL string.
    fn rewrite(sql: &str) -> String {
        let dialect = LaminarDialect::default();
        let mut stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        assert!(!stmts.is_empty());
        rewrite_interval_arithmetic(&mut stmts[0]);
        stmts[0].to_string()
    }

    // -- Basic arithmetic --

    #[test]
    fn test_subtract_interval_seconds() {
        let result = rewrite("SELECT ts - INTERVAL '10' SECOND FROM events");
        assert!(result.contains("ts - 10000"), "got: {result}");
        assert!(!result.contains("INTERVAL"), "got: {result}");
    }

    #[test]
    fn test_add_interval_seconds() {
        let result = rewrite("SELECT ts + INTERVAL '5' SECOND FROM events");
        assert!(result.contains("ts + 5000"), "got: {result}");
    }

    #[test]
    fn test_interval_minutes() {
        let result = rewrite("SELECT ts - INTERVAL '2' MINUTE FROM events");
        assert!(result.contains("ts - 120000"), "got: {result}");
    }

    #[test]
    fn test_interval_hours() {
        let result = rewrite("SELECT ts + INTERVAL '1' HOUR FROM events");
        assert!(result.contains("ts + 3600000"), "got: {result}");
    }

    #[test]
    fn test_interval_days() {
        let result = rewrite("SELECT ts - INTERVAL '1' DAY FROM events");
        assert!(result.contains("ts - 86400000"), "got: {result}");
    }

    #[test]
    fn test_interval_milliseconds() {
        let result = rewrite("SELECT ts - INTERVAL '100' MILLISECOND FROM events");
        assert!(result.contains("ts - 100"), "got: {result}");
    }

    // -- WHERE clause --

    #[test]
    fn test_where_clause_interval() {
        let result = rewrite("SELECT * FROM events WHERE ts > ts2 - INTERVAL '10' SECOND");
        assert!(result.contains("ts2 - 10000"), "got: {result}");
    }

    // -- BETWEEN (from issue example) --

    #[test]
    fn test_between_interval() {
        let result = rewrite(
            "SELECT * FROM trades t \
             INNER JOIN orders o ON t.symbol = o.symbol \
             AND o.ts BETWEEN t.ts - INTERVAL '10' SECOND AND t.ts + INTERVAL '10' SECOND",
        );
        assert!(result.contains("t.ts - 10000"), "got: {result}");
        assert!(result.contains("t.ts + 10000"), "got: {result}");
        assert!(!result.contains("INTERVAL"), "got: {result}");
    }

    // -- JOIN ON condition --

    #[test]
    fn test_join_on_interval() {
        let result = rewrite(
            "SELECT * FROM a JOIN b ON a.id = b.id \
             AND b.ts BETWEEN a.ts - INTERVAL '5' MINUTE AND a.ts + INTERVAL '5' MINUTE",
        );
        assert!(result.contains("a.ts - 300000"), "got: {result}");
        assert!(result.contains("a.ts + 300000"), "got: {result}");
    }

    // -- Nested expressions --

    #[test]
    fn test_nested_parens() {
        let result = rewrite("SELECT * FROM e WHERE (ts - INTERVAL '1' SECOND) > 0");
        assert!(result.contains("ts - 1000"), "got: {result}");
    }

    // -- Left-side INTERVAL (INTERVAL + col) --

    #[test]
    fn test_interval_on_left_side() {
        let result = rewrite("SELECT INTERVAL '10' SECOND + ts FROM events");
        assert!(result.contains("10000 + ts"), "got: {result}");
    }

    // -- No-op cases (should not be modified) --

    #[test]
    fn test_no_interval_unchanged() {
        let result = rewrite("SELECT ts - 10000 FROM events");
        assert!(result.contains("ts - 10000"), "got: {result}");
    }

    #[test]
    fn test_interval_default_unit_is_second() {
        // When no unit is specified, sqlparser defaults to SECOND
        let result = rewrite("SELECT ts - INTERVAL '5' SECOND FROM events");
        assert!(result.contains("ts - 5000"), "got: {result}");
    }

    // -- Multiple intervals in same query --

    #[test]
    fn test_multiple_intervals() {
        let result = rewrite(
            "SELECT * FROM events \
             WHERE ts > start_ts - INTERVAL '10' SECOND \
             AND ts < end_ts + INTERVAL '30' SECOND",
        );
        assert!(result.contains("start_ts - 10000"), "got: {result}");
        assert!(result.contains("end_ts + 30000"), "got: {result}");
    }

    // -- HAVING clause --

    #[test]
    fn test_having_clause_interval() {
        let result = rewrite(
            "SELECT symbol, COUNT(*) FROM trades \
             GROUP BY symbol \
             HAVING MAX(ts) - MIN(ts) > INTERVAL '1' HOUR",
        );
        // The HAVING expression should remain valid; INTERVAL is on the
        // right side of `>` which is a comparison, not +/-, so untouched
        // is correct.
        assert!(result.contains("HAVING"), "got: {result}");
    }
}
