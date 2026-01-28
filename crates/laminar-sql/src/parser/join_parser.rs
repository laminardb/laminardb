//! Join query analysis and extraction
//!
//! This module analyzes JOIN clauses to extract:
//! - Join type (INNER, LEFT, RIGHT, FULL)
//! - Key columns for join condition
//! - Time bounds for stream-stream joins
//! - Detection of lookup joins vs stream-stream joins

use std::time::Duration;

use sqlparser::ast::{BinaryOperator, Expr, JoinConstraint, JoinOperator, Select, TableFactor};

use super::window_rewriter::WindowRewriter;
use super::ParseError;

/// Join type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// INNER JOIN
    Inner,
    /// LEFT [OUTER] JOIN
    Left,
    /// RIGHT [OUTER] JOIN
    Right,
    /// FULL [OUTER] JOIN
    Full,
}

/// Analysis result for a JOIN clause
#[derive(Debug, Clone)]
pub struct JoinAnalysis {
    /// Type of join (inner, left, right, full)
    pub join_type: JoinType,
    /// Left side table name
    pub left_table: String,
    /// Right side table name
    pub right_table: String,
    /// Left side key column
    pub left_key_column: String,
    /// Right side key column
    pub right_key_column: String,
    /// Time bound for stream-stream joins (None for lookup joins)
    pub time_bound: Option<Duration>,
    /// Whether this is a lookup join (no time bound)
    pub is_lookup_join: bool,
    /// Left side alias (if any)
    pub left_alias: Option<String>,
    /// Right side alias (if any)
    pub right_alias: Option<String>,
}

impl JoinAnalysis {
    /// Create a stream-stream join analysis
    #[must_use]
    pub fn stream_stream(
        left_table: String,
        right_table: String,
        left_key: String,
        right_key: String,
        time_bound: Duration,
        join_type: JoinType,
    ) -> Self {
        Self {
            join_type,
            left_table,
            right_table,
            left_key_column: left_key,
            right_key_column: right_key,
            time_bound: Some(time_bound),
            is_lookup_join: false,
            left_alias: None,
            right_alias: None,
        }
    }

    /// Create a lookup join analysis
    #[must_use]
    pub fn lookup(
        left_table: String,
        right_table: String,
        left_key: String,
        right_key: String,
        join_type: JoinType,
    ) -> Self {
        Self {
            join_type,
            left_table,
            right_table,
            left_key_column: left_key,
            right_key_column: right_key,
            time_bound: None,
            is_lookup_join: true,
            left_alias: None,
            right_alias: None,
        }
    }
}

/// Analyze a SELECT statement for join information.
///
/// # Errors
///
/// Returns `ParseError::StreamingError` if:
/// - Join constraint is not supported
/// - Cannot extract key columns
pub fn analyze_join(select: &Select) -> Result<Option<JoinAnalysis>, ParseError> {
    let from = &select.from;
    if from.is_empty() {
        return Ok(None);
    }

    let first_table = &from[0];
    if first_table.joins.is_empty() {
        return Ok(None);
    }

    // Extract left table information
    let left_table = extract_table_name(&first_table.relation)?;
    let left_alias = extract_table_alias(&first_table.relation);

    // Analyze the first join
    let join = &first_table.joins[0];
    let right_table = extract_table_name(&join.relation)?;
    let right_alias = extract_table_alias(&join.relation);

    let join_type = map_join_operator(&join.join_operator);

    // Analyze the join constraint
    let (left_key, right_key, time_bound) = analyze_join_constraint(&join.join_operator)?;

    let mut analysis = if let Some(tb) = time_bound {
        JoinAnalysis::stream_stream(left_table, right_table, left_key, right_key, tb, join_type)
    } else {
        JoinAnalysis::lookup(left_table, right_table, left_key, right_key, join_type)
    };

    analysis.left_alias = left_alias;
    analysis.right_alias = right_alias;

    Ok(Some(analysis))
}

/// Extract table name from a TableFactor.
fn extract_table_name(factor: &TableFactor) -> Result<String, ParseError> {
    match factor {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        TableFactor::Derived { alias, .. } => {
            if let Some(alias) = alias {
                Ok(alias.name.value.clone())
            } else {
                Err(ParseError::StreamingError(
                    "Derived table without alias not supported".to_string(),
                ))
            }
        }
        _ => Err(ParseError::StreamingError(
            "Unsupported table factor type".to_string(),
        )),
    }
}

/// Extract table alias from a TableFactor.
fn extract_table_alias(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
        TableFactor::Derived { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
        _ => None,
    }
}

/// Map sqlparser JoinOperator to our JoinType.
fn map_join_operator(op: &JoinOperator) -> JoinType {
    match op {
        JoinOperator::Inner(_) | JoinOperator::Join(_) => JoinType::Inner,
        JoinOperator::Left(_)
        | JoinOperator::LeftOuter(_)
        | JoinOperator::LeftSemi(_)
        | JoinOperator::LeftAnti(_)
        | JoinOperator::Semi(_) => JoinType::Left,
        JoinOperator::Right(_)
        | JoinOperator::RightOuter(_)
        | JoinOperator::RightSemi(_)
        | JoinOperator::RightAnti(_)
        | JoinOperator::Anti(_) => JoinType::Right,
        JoinOperator::FullOuter(_) => JoinType::Full,
        JoinOperator::CrossJoin(_)
        | JoinOperator::CrossApply
        | JoinOperator::OuterApply => JoinType::Inner,
        JoinOperator::AsOf { .. } => JoinType::Left,
        JoinOperator::StraightJoin(_) => JoinType::Inner,
    }
}

/// Analyze join constraint to extract key columns and time bound.
fn analyze_join_constraint(
    op: &JoinOperator,
) -> Result<(String, String, Option<Duration>), ParseError> {
    let constraint = get_join_constraint(op)?;

    match constraint {
        JoinConstraint::On(expr) => analyze_on_expression(expr),
        JoinConstraint::Using(cols) => {
            if cols.is_empty() {
                return Err(ParseError::StreamingError(
                    "USING clause requires at least one column".to_string(),
                ));
            }
            // For USING, both sides have the same column name
            // Use to_string() on the Ident to get the column name
            let col = cols[0].to_string();
            Ok((col.clone(), col, None))
        }
        JoinConstraint::Natural => Err(ParseError::StreamingError(
            "NATURAL JOIN not supported for streaming".to_string(),
        )),
        JoinConstraint::None => Err(ParseError::StreamingError(
            "JOIN without condition not supported for streaming".to_string(),
        )),
    }
}

/// Get the JoinConstraint from a JoinOperator.
fn get_join_constraint(op: &JoinOperator) -> Result<&JoinConstraint, ParseError> {
    match op {
        JoinOperator::Inner(constraint)
        | JoinOperator::Join(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::StraightJoin(constraint) => Ok(constraint),
        JoinOperator::CrossJoin(_)
        | JoinOperator::CrossApply
        | JoinOperator::OuterApply => Err(ParseError::StreamingError(
            "CROSS JOIN not supported for streaming".to_string(),
        )),
        JoinOperator::AsOf { constraint, .. } => Ok(constraint),
    }
}

/// Analyze ON expression to extract key columns and time bound.
fn analyze_on_expression(
    expr: &Expr,
) -> Result<(String, String, Option<Duration>), ParseError> {
    // Handle compound expressions (AND)
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            // Recursively analyze both sides
            let left_result = analyze_on_expression(left);
            let right_result = analyze_on_expression(right);

            // Combine results - one should have keys, the other might have time bound
            match (left_result, right_result) {
                (Ok((lk, rk, None)), Ok((_, _, time))) if !lk.is_empty() => {
                    Ok((lk, rk, time))
                }
                (Ok((_, _, time)), Ok((lk, rk, None))) if !lk.is_empty() => {
                    Ok((lk, rk, time))
                }
                (Ok(result), Err(_)) => Ok(result),
                (Err(_), Ok(result)) => Ok(result),
                (Ok((lk, rk, t1)), Ok((_, _, t2))) => {
                    // If both have keys, prefer the first
                    Ok((lk, rk, t1.or(t2)))
                }
                (Err(e), Err(_)) => Err(e),
            }
        }
        // Equality condition: a.col = b.col
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let left_col = extract_column_ref(left);
            let right_col = extract_column_ref(right);

            match (left_col, right_col) {
                (Some(l), Some(r)) => Ok((l, r, None)),
                _ => Err(ParseError::StreamingError(
                    "Cannot extract column references from equality condition".to_string(),
                )),
            }
        }
        // BETWEEN clause for time bound: p.ts BETWEEN o.ts AND o.ts + INTERVAL
        Expr::Between {
            expr: _,
            low: _,
            high,
            ..
        } => {
            // Try to extract time bound from high expression
            let time_bound = extract_time_bound_from_expr(high).ok();
            Ok((String::new(), String::new(), time_bound))
        }
        // Comparison operators for time bounds
        Expr::BinaryOp {
            left: _,
            op: BinaryOperator::LtEq | BinaryOperator::Lt | BinaryOperator::GtEq | BinaryOperator::Gt,
            right,
        } => {
            // Try to extract time bound from right side
            let time_bound = extract_time_bound_from_expr(right).ok();
            Ok((String::new(), String::new(), time_bound))
        }
        _ => Err(ParseError::StreamingError(format!(
            "Unsupported join condition expression: {expr:?}"
        ))),
    }
}

/// Extract column reference from expression (e.g., "a.id" -> "id")
fn extract_column_ref(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        _ => None,
    }
}

/// Extract time bound from an expression like "o.ts + INTERVAL '1' HOUR"
fn extract_time_bound_from_expr(expr: &Expr) -> Result<Duration, ParseError> {
    match expr {
        // Direct interval
        Expr::Interval(_) => WindowRewriter::parse_interval_to_duration(expr),
        // Addition: col + INTERVAL
        Expr::BinaryOp {
            left: _,
            op: BinaryOperator::Plus,
            right,
        } => extract_time_bound_from_expr(right),
        // Subtraction: col - INTERVAL (for lower bound)
        Expr::BinaryOp {
            left: _,
            op: BinaryOperator::Minus,
            right,
        } => extract_time_bound_from_expr(right),
        // Nested expression
        Expr::Nested(inner) => extract_time_bound_from_expr(inner),
        _ => Err(ParseError::StreamingError(format!(
            "Cannot extract time bound from: {expr:?}"
        ))),
    }
}

/// Check if a SELECT contains a join.
#[must_use]
pub fn has_join(select: &Select) -> bool {
    !select.from.is_empty() && !select.from[0].joins.is_empty()
}

/// Count the number of joins in a SELECT.
#[must_use]
pub fn count_joins(select: &Select) -> usize {
    select
        .from
        .iter()
        .map(|table_with_joins| table_with_joins.joins.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use sqlparser::ast::{SetExpr, Statement};

    fn parse_select(sql: &str) -> Select {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = query.body.as_ref() {
                return *select.clone();
            }
        }
        panic!("Expected SELECT query");
    }

    #[test]
    fn test_analyze_inner_join() {
        let sql = "SELECT * FROM orders o INNER JOIN payments p ON o.order_id = p.order_id";
        let select = parse_select(sql);

        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.join_type, JoinType::Inner);
        assert_eq!(analysis.left_table, "orders");
        assert_eq!(analysis.right_table, "payments");
        assert_eq!(analysis.left_key_column, "order_id");
        assert_eq!(analysis.right_key_column, "order_id");
        assert!(analysis.is_lookup_join); // No time bound = lookup join
    }

    #[test]
    fn test_analyze_left_join() {
        let sql = "SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id";
        let select = parse_select(sql);

        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.join_type, JoinType::Left);
        assert_eq!(analysis.left_key_column, "customer_id");
        assert_eq!(analysis.right_key_column, "id");
    }

    #[test]
    fn test_analyze_join_using() {
        let sql = "SELECT * FROM orders o JOIN payments p USING (order_id)";
        let select = parse_select(sql);

        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.left_key_column, "order_id");
        assert_eq!(analysis.right_key_column, "order_id");
    }

    #[test]
    fn test_analyze_stream_stream_join_with_time_bound() {
        let sql = "SELECT * FROM orders o
                   JOIN payments p ON o.order_id = p.order_id
                   AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR";
        let select = parse_select(sql);

        let analysis = analyze_join(&select).unwrap().unwrap();

        assert!(!analysis.is_lookup_join);
        assert!(analysis.time_bound.is_some());
        assert_eq!(analysis.time_bound.unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn test_no_join() {
        let sql = "SELECT * FROM orders";
        let select = parse_select(sql);

        let analysis = analyze_join(&select).unwrap();
        assert!(analysis.is_none());
    }

    #[test]
    fn test_has_join() {
        let sql_with_join = "SELECT * FROM orders o JOIN payments p ON o.id = p.order_id";
        let sql_without_join = "SELECT * FROM orders";

        let select_with = parse_select(sql_with_join);
        let select_without = parse_select(sql_without_join);

        assert!(has_join(&select_with));
        assert!(!has_join(&select_without));
    }

    #[test]
    fn test_count_joins() {
        let sql_one = "SELECT * FROM a JOIN b ON a.id = b.id";
        let sql_two = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id";
        let sql_none = "SELECT * FROM a";

        assert_eq!(count_joins(&parse_select(sql_one)), 1);
        assert_eq!(count_joins(&parse_select(sql_two)), 2);
        assert_eq!(count_joins(&parse_select(sql_none)), 0);
    }

    #[test]
    fn test_aliases() {
        let sql = "SELECT * FROM orders AS o JOIN payments AS p ON o.id = p.order_id";
        let select = parse_select(sql);

        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.left_alias, Some("o".to_string()));
        assert_eq!(analysis.right_alias, Some("p".to_string()));
    }
}
