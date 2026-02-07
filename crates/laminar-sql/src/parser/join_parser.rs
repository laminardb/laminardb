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
    /// LEFT \[OUTER\] JOIN
    Left,
    /// RIGHT \[OUTER\] JOIN
    Right,
    /// FULL \[OUTER\] JOIN
    Full,
    /// ASOF JOIN
    AsOf,
}

/// Direction for ASOF JOIN time matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsofSqlDirection {
    /// `left.ts >= right.ts` — find most recent right row
    Backward,
    /// `left.ts <= right.ts` — find next right row
    Forward,
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
    /// Whether this is an ASOF join
    pub is_asof_join: bool,
    /// ASOF join direction (Backward or Forward)
    pub asof_direction: Option<AsofSqlDirection>,
    /// Left side time column for ASOF join
    pub left_time_column: Option<String>,
    /// Right side time column for ASOF join
    pub right_time_column: Option<String>,
    /// ASOF join tolerance (max time difference)
    pub asof_tolerance: Option<Duration>,
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
            is_asof_join: false,
            asof_direction: None,
            left_time_column: None,
            right_time_column: None,
            asof_tolerance: None,
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
            is_asof_join: false,
            asof_direction: None,
            left_time_column: None,
            right_time_column: None,
            asof_tolerance: None,
        }
    }

    /// Create an ASOF join analysis
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn asof(
        left_table: String,
        right_table: String,
        left_key: String,
        right_key: String,
        direction: AsofSqlDirection,
        left_time_col: String,
        right_time_col: String,
        tolerance: Option<Duration>,
    ) -> Self {
        Self {
            join_type: JoinType::AsOf,
            left_table,
            right_table,
            left_key_column: left_key,
            right_key_column: right_key,
            time_bound: None,
            is_lookup_join: false,
            left_alias: None,
            right_alias: None,
            is_asof_join: true,
            asof_direction: Some(direction),
            left_time_column: Some(left_time_col),
            right_time_column: Some(right_time_col),
            asof_tolerance: tolerance,
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

    // Handle ASOF JOIN specially
    if let JoinOperator::AsOf {
        match_condition,
        constraint,
    } = &join.join_operator
    {
        let (direction, left_time, right_time, tolerance) =
            analyze_asof_match_condition(match_condition)?;

        // Extract key columns from the ON constraint
        let (left_key, right_key) = analyze_asof_constraint(constraint)?;

        let mut analysis = JoinAnalysis::asof(
            left_table,
            right_table,
            left_key,
            right_key,
            direction,
            left_time,
            right_time,
            tolerance,
        );
        analysis.left_alias = left_alias;
        analysis.right_alias = right_alias;
        return Ok(Some(analysis));
    }

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
        JoinOperator::Inner(_)
        | JoinOperator::Join(_)
        | JoinOperator::CrossJoin(_)
        | JoinOperator::CrossApply
        | JoinOperator::OuterApply
        | JoinOperator::StraightJoin(_) => JoinType::Inner,
        JoinOperator::Left(_)
        | JoinOperator::LeftOuter(_)
        | JoinOperator::LeftSemi(_)
        | JoinOperator::LeftAnti(_)
        | JoinOperator::Semi(_) => JoinType::Left,
        JoinOperator::AsOf { .. } => JoinType::AsOf,
        JoinOperator::Right(_)
        | JoinOperator::RightOuter(_)
        | JoinOperator::RightSemi(_)
        | JoinOperator::RightAnti(_)
        | JoinOperator::Anti(_) => JoinType::Right,
        JoinOperator::FullOuter(_) => JoinType::Full,
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
        | JoinOperator::StraightJoin(constraint)
        | JoinOperator::AsOf { constraint, .. } => Ok(constraint),
        JoinOperator::CrossJoin(_) | JoinOperator::CrossApply | JoinOperator::OuterApply => Err(
            ParseError::StreamingError("CROSS JOIN not supported for streaming".to_string()),
        ),
    }
}

/// Analyze ON expression to extract key columns and time bound.
fn analyze_on_expression(expr: &Expr) -> Result<(String, String, Option<Duration>), ParseError> {
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
                (Ok((lk, rk, None)), Ok((_, _, time))) if !lk.is_empty() => Ok((lk, rk, time)),
                (Ok((_, _, time)), Ok((lk, rk, None))) if !lk.is_empty() => Ok((lk, rk, time)),
                (Ok(result), Err(_)) | (Err(_), Ok(result)) => Ok(result),
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
            op:
                BinaryOperator::LtEq | BinaryOperator::Lt | BinaryOperator::GtEq | BinaryOperator::Gt,
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
        // Addition or subtraction: col +/- INTERVAL
        Expr::BinaryOp {
            left: _,
            op: BinaryOperator::Plus | BinaryOperator::Minus,
            right,
        } => extract_time_bound_from_expr(right),
        // Nested expression
        Expr::Nested(inner) => extract_time_bound_from_expr(inner),
        _ => Err(ParseError::StreamingError(format!(
            "Cannot extract time bound from: {expr:?}"
        ))),
    }
}

/// Analyze ASOF JOIN MATCH_CONDITION expression.
///
/// Extracts direction, time column names, and optional tolerance.
fn analyze_asof_match_condition(
    expr: &Expr,
) -> Result<(AsofSqlDirection, String, String, Option<Duration>), ParseError> {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        // Try to get direction from left, tolerance from right
        let dir_result = analyze_asof_direction(left);
        let tol_result = extract_asof_tolerance(right);

        match (dir_result, tol_result) {
            (Ok((dir, lt, rt)), Ok(tol)) => Ok((dir, lt, rt, Some(tol))),
            (Ok((dir, lt, rt)), Err(_)) => {
                // Maybe tolerance is on left and direction on right
                let dir2 = analyze_asof_direction(right);
                let tol2 = extract_asof_tolerance(left);
                match (dir2, tol2) {
                    (Ok((d, l, r)), Ok(t)) => Ok((d, l, r, Some(t))),
                    _ => Ok((dir, lt, rt, None)),
                }
            }
            (Err(_), _) => {
                // Try reversed
                let dir2 = analyze_asof_direction(right);
                let tol2 = extract_asof_tolerance(left);
                match (dir2, tol2) {
                    (Ok((d, l, r)), Ok(t)) => Ok((d, l, r, Some(t))),
                    (Ok((d, l, r)), Err(_)) => Ok((d, l, r, None)),
                    _ => Err(ParseError::StreamingError(
                        "Cannot extract ASOF direction from MATCH_CONDITION".to_string(),
                    )),
                }
            }
        }
    } else {
        let (dir, lt, rt) = analyze_asof_direction(expr)?;
        Ok((dir, lt, rt, None))
    }
}

/// Extract ASOF direction and time columns from a comparison expression.
fn analyze_asof_direction(expr: &Expr) -> Result<(AsofSqlDirection, String, String), ParseError> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::GtEq,
            right,
        } => {
            let left_col = extract_column_ref(left).ok_or_else(|| {
                ParseError::StreamingError(
                    "Cannot extract left time column from MATCH_CONDITION".to_string(),
                )
            })?;
            let right_col = extract_column_ref(right).ok_or_else(|| {
                ParseError::StreamingError(
                    "Cannot extract right time column from MATCH_CONDITION".to_string(),
                )
            })?;
            Ok((AsofSqlDirection::Backward, left_col, right_col))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::LtEq,
            right,
        } => {
            let left_col = extract_column_ref(left).ok_or_else(|| {
                ParseError::StreamingError(
                    "Cannot extract left time column from MATCH_CONDITION".to_string(),
                )
            })?;
            let right_col = extract_column_ref(right).ok_or_else(|| {
                ParseError::StreamingError(
                    "Cannot extract right time column from MATCH_CONDITION".to_string(),
                )
            })?;
            Ok((AsofSqlDirection::Forward, left_col, right_col))
        }
        _ => Err(ParseError::StreamingError(
            "ASOF MATCH_CONDITION must be >= or <= comparison".to_string(),
        )),
    }
}

/// Extract tolerance duration from an ASOF tolerance expression.
///
/// Handles: `left - right <= value` or `left - right <= INTERVAL '...'`
fn extract_asof_tolerance(expr: &Expr) -> Result<Duration, ParseError> {
    match expr {
        Expr::BinaryOp {
            left: _,
            op: BinaryOperator::LtEq,
            right,
        } => {
            // right side is either a literal number or INTERVAL
            match right.as_ref() {
                Expr::Value(v) => {
                    if let sqlparser::ast::Value::Number(n, _) = &v.value {
                        let ms: u64 = n.parse().map_err(|_| {
                            ParseError::StreamingError(format!(
                                "Cannot parse tolerance as number: {n}"
                            ))
                        })?;
                        Ok(Duration::from_millis(ms))
                    } else {
                        Err(ParseError::StreamingError(
                            "ASOF tolerance must be a number or INTERVAL".to_string(),
                        ))
                    }
                }
                Expr::Interval(_) => WindowRewriter::parse_interval_to_duration(right),
                _ => Err(ParseError::StreamingError(
                    "ASOF tolerance must be a number or INTERVAL".to_string(),
                )),
            }
        }
        _ => Err(ParseError::StreamingError(
            "ASOF tolerance expression must be <= comparison".to_string(),
        )),
    }
}

/// Extract key columns from an ASOF JOIN constraint (ON clause).
fn analyze_asof_constraint(constraint: &JoinConstraint) -> Result<(String, String), ParseError> {
    match constraint {
        JoinConstraint::On(expr) => extract_equality_columns(expr),
        JoinConstraint::Using(cols) => {
            if cols.is_empty() {
                return Err(ParseError::StreamingError(
                    "USING clause requires at least one column".to_string(),
                ));
            }
            let col = cols[0].to_string();
            Ok((col.clone(), col))
        }
        _ => Err(ParseError::StreamingError(
            "ASOF JOIN requires ON or USING constraint".to_string(),
        )),
    }
}

/// Extract left and right column names from an equality expression.
fn extract_equality_columns(expr: &Expr) -> Result<(String, String), ParseError> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let left_col = extract_column_ref(left).ok_or_else(|| {
                ParseError::StreamingError("Cannot extract left key column".to_string())
            })?;
            let right_col = extract_column_ref(right).ok_or_else(|| {
                ParseError::StreamingError("Cannot extract right key column".to_string())
            })?;
            Ok((left_col, right_col))
        }
        // If there's an AND, find the equality part
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => extract_equality_columns(left).or_else(|_| extract_equality_columns(right)),
        _ => Err(ParseError::StreamingError(
            "ASOF JOIN ON clause must contain an equality condition".to_string(),
        )),
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

/// Analysis result for multi-way JOINs (e.g., `A JOIN B ... JOIN C ...`).
///
/// Each step represents one left-deep join: step 0 joins the base table with
/// the first right table, step 1 joins the result with the next right table, etc.
#[derive(Debug, Clone)]
pub struct MultiJoinAnalysis {
    /// Ordered join steps (left-to-right)
    pub joins: Vec<JoinAnalysis>,
    /// All referenced tables in order (base table first, then each right table)
    pub tables: Vec<String>,
}

impl MultiJoinAnalysis {
    /// Number of join steps.
    #[must_use]
    pub fn len(&self) -> usize {
        self.joins.len()
    }

    /// Whether there are no join steps.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.joins.is_empty()
    }

    /// Whether this is a single join (backward-compatible case).
    #[must_use]
    pub fn is_single(&self) -> bool {
        self.joins.len() == 1
    }

    /// The first join step (convenience for single-join queries).
    #[must_use]
    pub fn first(&self) -> Option<&JoinAnalysis> {
        self.joins.first()
    }
}

/// Analyze a SELECT statement for all join steps (multi-way).
///
/// Returns `None` if the query has no joins. For a single join this
/// returns a `MultiJoinAnalysis` with one step, making it backward
/// compatible with `analyze_join()`.
///
/// # Errors
///
/// Returns `ParseError::StreamingError` if any join constraint is
/// not supported or key columns cannot be extracted.
pub fn analyze_joins(select: &Select) -> Result<Option<MultiJoinAnalysis>, ParseError> {
    let from = &select.from;
    if from.is_empty() {
        return Ok(None);
    }

    let first_table = &from[0];
    if first_table.joins.is_empty() {
        return Ok(None);
    }

    // Extract base table
    let base_table = extract_table_name(&first_table.relation)?;
    let base_alias = extract_table_alias(&first_table.relation);

    let mut join_steps = Vec::with_capacity(first_table.joins.len());
    let mut tables = vec![base_table.clone()];

    // Track the left table name for left-deep chaining
    let mut prev_left_table = base_table;
    let mut prev_left_alias = base_alias;

    for join in &first_table.joins {
        let right_table = extract_table_name(&join.relation)?;
        let right_alias = extract_table_alias(&join.relation);
        tables.push(right_table.clone());

        let join_type = map_join_operator(&join.join_operator);

        // Handle ASOF JOIN
        if let JoinOperator::AsOf {
            match_condition,
            constraint,
        } = &join.join_operator
        {
            let (direction, left_time, right_time, tolerance) =
                analyze_asof_match_condition(match_condition)?;
            let (left_key, right_key) = analyze_asof_constraint(constraint)?;

            let mut analysis = JoinAnalysis::asof(
                prev_left_table.clone(),
                right_table.clone(),
                left_key,
                right_key,
                direction,
                left_time,
                right_time,
                tolerance,
            );
            analysis.left_alias.clone_from(&prev_left_alias);
            analysis.right_alias = right_alias;
            join_steps.push(analysis);
        } else {
            // Regular join (inner, left, right, full)
            let (left_key, right_key, time_bound) =
                analyze_join_constraint(&join.join_operator)?;

            let mut analysis = if let Some(tb) = time_bound {
                JoinAnalysis::stream_stream(
                    prev_left_table.clone(),
                    right_table.clone(),
                    left_key,
                    right_key,
                    tb,
                    join_type,
                )
            } else {
                JoinAnalysis::lookup(
                    prev_left_table.clone(),
                    right_table.clone(),
                    left_key,
                    right_key,
                    join_type,
                )
            };
            analysis.left_alias.clone_from(&prev_left_alias);
            analysis.right_alias = right_alias;
            join_steps.push(analysis);
        }

        // Next step's left table is this step's right table (left-deep)
        prev_left_table = right_table;
        prev_left_alias = extract_table_alias(&join.relation);
    }

    Ok(Some(MultiJoinAnalysis {
        joins: join_steps,
        tables,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

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
        let sql_zero = "SELECT * FROM a";

        assert_eq!(count_joins(&parse_select(sql_one)), 1);
        assert_eq!(count_joins(&parse_select(sql_two)), 2);
        assert_eq!(count_joins(&parse_select(sql_zero)), 0);
    }

    #[test]
    fn test_aliases() {
        let sql = "SELECT * FROM orders AS o JOIN payments AS p ON o.id = p.order_id";
        let select = parse_select(sql);

        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.left_alias, Some("o".to_string()));
        assert_eq!(analysis.right_alias, Some("p".to_string()));
    }

    // -- ASOF JOIN tests --

    fn parse_select_snowflake(sql: &str) -> Select {
        let dialect = sqlparser::dialect::SnowflakeDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = query.body.as_ref() {
                return *select.clone();
            }
        }
        panic!("Expected SELECT query");
    }

    #[test]
    fn test_asof_join_backward() {
        let sql = "SELECT * FROM trades t \
                    ASOF JOIN quotes q \
                    MATCH_CONDITION(t.ts >= q.ts) \
                    ON t.symbol = q.symbol";
        let select = parse_select_snowflake(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert!(analysis.is_asof_join);
        assert_eq!(analysis.asof_direction, Some(AsofSqlDirection::Backward));
        assert_eq!(analysis.join_type, JoinType::AsOf);
        assert!(analysis.asof_tolerance.is_none());
    }

    #[test]
    fn test_asof_join_forward() {
        let sql = "SELECT * FROM trades t \
                    ASOF JOIN quotes q \
                    MATCH_CONDITION(t.ts <= q.ts) \
                    ON t.symbol = q.symbol";
        let select = parse_select_snowflake(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert!(analysis.is_asof_join);
        assert_eq!(analysis.asof_direction, Some(AsofSqlDirection::Forward));
    }

    #[test]
    fn test_asof_join_with_tolerance() {
        let sql = "SELECT * FROM trades t \
                    ASOF JOIN quotes q \
                    MATCH_CONDITION(t.ts >= q.ts AND t.ts - q.ts <= 5000) \
                    ON t.symbol = q.symbol";
        let select = parse_select_snowflake(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert!(analysis.is_asof_join);
        assert_eq!(analysis.asof_direction, Some(AsofSqlDirection::Backward));
        assert_eq!(analysis.asof_tolerance, Some(Duration::from_millis(5000)));
    }

    #[test]
    fn test_asof_join_with_interval_tolerance() {
        let sql = "SELECT * FROM trades t \
                    ASOF JOIN quotes q \
                    MATCH_CONDITION(t.ts >= q.ts AND t.ts - q.ts <= INTERVAL '5' SECOND) \
                    ON t.symbol = q.symbol";
        let select = parse_select_snowflake(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert!(analysis.is_asof_join);
        assert_eq!(analysis.asof_direction, Some(AsofSqlDirection::Backward));
        assert_eq!(analysis.asof_tolerance, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_asof_join_type_mapping() {
        let sql = "SELECT * FROM trades t \
                    ASOF JOIN quotes q \
                    MATCH_CONDITION(t.ts >= q.ts) \
                    ON t.symbol = q.symbol";
        let select = parse_select_snowflake(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.join_type, JoinType::AsOf);
        assert!(!analysis.is_lookup_join);
    }

    #[test]
    fn test_asof_join_extracts_time_columns() {
        let sql = "SELECT * FROM trades t \
                    ASOF JOIN quotes q \
                    MATCH_CONDITION(t.ts >= q.ts) \
                    ON t.symbol = q.symbol";
        let select = parse_select_snowflake(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.left_time_column, Some("ts".to_string()));
        assert_eq!(analysis.right_time_column, Some("ts".to_string()));
    }

    #[test]
    fn test_asof_join_extracts_key_columns() {
        let sql = "SELECT * FROM trades t \
                    ASOF JOIN quotes q \
                    MATCH_CONDITION(t.ts >= q.ts) \
                    ON t.symbol = q.symbol";
        let select = parse_select_snowflake(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.left_key_column, "symbol");
        assert_eq!(analysis.right_key_column, "symbol");
    }

    #[test]
    fn test_asof_join_aliases() {
        let sql = "SELECT * FROM trades AS t \
                    ASOF JOIN quotes AS q \
                    MATCH_CONDITION(t.ts >= q.ts) \
                    ON t.symbol = q.symbol";
        let select = parse_select_snowflake(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert_eq!(analysis.left_alias, Some("t".to_string()));
        assert_eq!(analysis.right_alias, Some("q".to_string()));
        assert_eq!(analysis.left_table, "trades");
        assert_eq!(analysis.right_table, "quotes");
    }

    // -- Multi-way JOIN tests (F-SQL-005) --

    #[test]
    fn test_multi_join_single_backward_compat() {
        let sql = "SELECT * FROM orders o JOIN payments p ON o.id = p.order_id";
        let select = parse_select(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert!(multi.is_single());
        assert_eq!(multi.len(), 1);
        assert!(!multi.is_empty());
        let first = multi.first().unwrap();
        assert_eq!(first.left_table, "orders");
        assert_eq!(first.right_table, "payments");
    }

    #[test]
    fn test_multi_join_two_way() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id";
        let select = parse_select(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert_eq!(multi.len(), 2);
        assert!(!multi.is_single());

        assert_eq!(multi.joins[0].left_table, "a");
        assert_eq!(multi.joins[0].right_table, "b");
        assert_eq!(multi.joins[0].left_key_column, "id");
        assert_eq!(multi.joins[0].right_key_column, "a_id");

        assert_eq!(multi.joins[1].left_table, "b");
        assert_eq!(multi.joins[1].right_table, "c");
        assert_eq!(multi.joins[1].left_key_column, "id");
        assert_eq!(multi.joins[1].right_key_column, "b_id");
    }

    #[test]
    fn test_multi_join_three_way() {
        let sql = "SELECT * FROM a \
                    JOIN b ON a.id = b.a_id \
                    JOIN c ON b.id = c.b_id \
                    JOIN d ON c.id = d.c_id";
        let select = parse_select(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert_eq!(multi.len(), 3);
        assert_eq!(multi.tables.len(), 4);
        assert_eq!(multi.tables, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_multi_join_mixed_asof_and_lookup() {
        // ASOF first, then lookup (use Snowflake dialect for ASOF)
        let sql = "SELECT * FROM trades t \
                    ASOF JOIN quotes q \
                    MATCH_CONDITION(t.ts >= q.ts) \
                    ON t.symbol = q.symbol \
                    JOIN products p ON q.product_id = p.id";
        let select = parse_select_snowflake(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert_eq!(multi.len(), 2);
        assert!(multi.joins[0].is_asof_join);
        assert!(multi.joins[1].is_lookup_join);
    }

    #[test]
    fn test_multi_join_stream_stream_and_lookup() {
        let sql = "SELECT * FROM orders o \
                    JOIN payments p ON o.id = p.order_id \
                        AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR \
                    JOIN customers c ON o.customer_id = c.id";
        let select = parse_select(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert_eq!(multi.len(), 2);
        assert!(!multi.joins[0].is_lookup_join); // stream-stream
        assert!(multi.joins[0].time_bound.is_some());
        assert!(multi.joins[1].is_lookup_join); // lookup
    }

    #[test]
    fn test_multi_join_tables_list() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id";
        let select = parse_select(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert_eq!(multi.tables, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_multi_join_aliases() {
        let sql = "SELECT * FROM orders AS o \
                    JOIN payments AS p ON o.id = p.order_id \
                    JOIN refunds AS r ON p.id = r.payment_id";
        let select = parse_select(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert_eq!(multi.joins[0].left_alias, Some("o".to_string()));
        assert_eq!(multi.joins[0].right_alias, Some("p".to_string()));
        assert_eq!(multi.joins[1].left_alias, Some("p".to_string()));
        assert_eq!(multi.joins[1].right_alias, Some("r".to_string()));
    }

    #[test]
    fn test_multi_join_no_join_returns_none() {
        let sql = "SELECT * FROM orders";
        let select = parse_select(sql);
        let multi = analyze_joins(&select).unwrap();
        assert!(multi.is_none());
    }
}
