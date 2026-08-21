//! Join query analysis and extraction
//!
//! This module analyzes JOIN clauses to extract:
//! - Join type (INNER, LEFT, RIGHT, FULL)
//! - Key columns for join condition
//! - Time bounds for stream-stream joins
//! - Detection of lookup joins vs stream-stream joins

use std::time::Duration;

use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, Select,
    TableFactor, TableVersion,
};

use super::window_rewriter::WindowRewriter;
use super::ParseError;
use crate::temporal::TemporalProbeSchedule;

mod temporal_probe;

pub(crate) use temporal_probe::parse_temporal_probe_query;

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
    /// LEFT SEMI JOIN — emit left rows with at least one match
    LeftSemi,
    /// LEFT ANTI JOIN — emit left rows with no match
    LeftAnti,
    /// RIGHT SEMI JOIN — emit right rows with at least one match
    RightSemi,
    /// RIGHT ANTI JOIN — emit right rows with no match
    RightAnti,
}

/// Unresolved time column refs from a BETWEEN clause.
#[derive(Debug, Clone)]
struct RawTimeCols {
    expr_qualifier: String,
    expr_col: String,
    low_qualifier: String,
    low_col: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy)]
struct JoinSides<'a> {
    left_table: &'a str,
    right_table: &'a str,
    left_alias: Option<&'a str>,
    right_alias: Option<&'a str>,
}

impl JoinSides<'_> {
    fn resolve_qualifier(&self, qualifier: &str, context: &str) -> Result<JoinSide, ParseError> {
        let is_left =
            qualifier == self.left_table || self.left_alias.is_some_and(|alias| qualifier == alias);
        let is_right = qualifier == self.right_table
            || self.right_alias.is_some_and(|alias| qualifier == alias);

        match (is_left, is_right) {
            (true, false) => Ok(JoinSide::Left),
            (false, true) => Ok(JoinSide::Right),
            (true, true) => Err(ParseError::StreamingError(format!(
                "{context} must use unambiguous left and right join input names; qualifier '{qualifier}' names both inputs"
            ))),
            (false, false) => Err(ParseError::StreamingError(format!(
                "{context} must use unambiguous left and right join input names; qualifier '{qualifier}' names neither input"
            ))),
        }
    }
}

/// Resolve BETWEEN time columns to `(left_time_col, right_time_col)` using
/// table qualifiers.
fn resolve_time_cols(
    raw: &RawTimeCols,
    left_table: &str,
    right_table: &str,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
) -> Result<(String, String), ParseError> {
    let sides = JoinSides {
        left_table,
        right_table,
        left_alias,
        right_alias,
    };
    let expr_side = sides.resolve_qualifier(&raw.expr_qualifier, "streaming interval timestamp")?;
    let low_side = sides.resolve_qualifier(&raw.low_qualifier, "streaming interval timestamp")?;

    match (expr_side, low_side) {
        (JoinSide::Right, JoinSide::Left) => Ok((raw.low_col.clone(), raw.expr_col.clone())),
        (JoinSide::Left, JoinSide::Right) => Err(ParseError::StreamingError(
            "streaming interval joins require the right timestamp BETWEEN the left timestamp and left timestamp + interval"
                .to_string(),
        )),
        _ => Err(ParseError::StreamingError(
            "streaming interval join timestamps must reference opposite join inputs".to_string(),
        )),
    }
}

/// Analysis result for a JOIN clause
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Left side time column for a bounded join
    pub left_time_column: Option<String>,
    /// Right side time column for a bounded join
    pub right_time_column: Option<String>,
    /// Target-time schedule for a temporal join.
    pub temporal_probe_schedule: Option<TemporalProbeSchedule>,
    /// Pseudo-table alias exposing `offset_ms` and `probe_time` for multi-horizon probes.
    pub temporal_probe_alias: Option<String>,
    /// Additional key columns for composite join keys (beyond the primary key pair)
    pub additional_key_columns: Vec<(String, String)>,
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
            left_time_column: None,
            right_time_column: None,
            temporal_probe_schedule: None,
            temporal_probe_alias: None,
            additional_key_columns: vec![],
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
            left_time_column: None,
            right_time_column: None,
            temporal_probe_schedule: None,
            temporal_probe_alias: None,
            additional_key_columns: vec![],
        }
    }

    /// Create a temporal join analysis (FOR SYSTEM_TIME AS OF).
    #[must_use]
    pub fn temporal(
        left_table: String,
        right_table: String,
        left_key: String,
        right_key: String,
        left_time_column: String,
        join_type: JoinType,
    ) -> Self {
        Self {
            join_type,
            left_table,
            right_table,
            left_key_column: left_key,
            right_key_column: right_key,
            time_bound: None,
            is_lookup_join: false,
            left_alias: None,
            right_alias: None,
            left_time_column: Some(left_time_column),
            right_time_column: None,
            temporal_probe_schedule: Some(TemporalProbeSchedule::as_of()),
            temporal_probe_alias: None,
            additional_key_columns: vec![],
        }
    }

    /// True if this step has a `BETWEEN` bound or `FOR SYSTEM_TIME AS OF`.
    #[must_use]
    pub fn is_bounded(&self) -> bool {
        self.time_bound.is_some() || self.is_temporal_join()
    }

    /// Whether this step uses versioned event-time lookup state.
    #[must_use]
    pub fn is_temporal_join(&self) -> bool {
        self.temporal_probe_schedule.is_some()
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

    let join_type = map_join_operator(&join.join_operator)?;
    let sides = JoinSides {
        left_table: &left_table,
        right_table: &right_table,
        left_alias: left_alias.as_deref(),
        right_alias: right_alias.as_deref(),
    };

    // Check for temporal join (FOR SYSTEM_TIME AS OF)
    if let Some(left_time_col) = extract_temporal_left_time(&join.relation, &sides)? {
        let (left_key, right_key, additional, time_bound, time_cols) =
            analyze_join_constraint(&join.join_operator, &sides)?;
        if time_bound.is_some() || time_cols.is_some() {
            return Err(ParseError::StreamingError(
                "temporal joins do not accept an additional time-bound predicate".into(),
            ));
        }
        let mut analysis = JoinAnalysis::temporal(
            left_table,
            right_table,
            left_key,
            right_key,
            left_time_col,
            join_type,
        );
        analysis.left_alias = left_alias;
        analysis.right_alias = right_alias;
        analysis.additional_key_columns = additional;
        return Ok(Some(analysis));
    }

    // Analyze the join constraint
    let (left_key, right_key, additional, time_bound, time_cols) =
        analyze_join_constraint(&join.join_operator, &sides)?;

    let mut analysis = if let Some(tb) = time_bound {
        JoinAnalysis::stream_stream(left_table, right_table, left_key, right_key, tb, join_type)
    } else {
        JoinAnalysis::lookup(left_table, right_table, left_key, right_key, join_type)
    };

    analysis.left_alias.clone_from(&left_alias);
    analysis.right_alias.clone_from(&right_alias);
    analysis.additional_key_columns = additional;

    if let Some(ref raw) = time_cols {
        let (lt, rt) = resolve_time_cols(
            raw,
            &analysis.left_table,
            &analysis.right_table,
            left_alias.as_deref(),
            right_alias.as_deref(),
        )?;
        analysis.left_time_column = Some(lt);
        analysis.right_time_column = Some(rt);
    }

    Ok(Some(analysis))
}

/// Extract table name from a TableFactor.
fn extract_table_name(factor: &TableFactor) -> Result<String, ParseError> {
    match factor {
        TableFactor::Table { name, .. } => match name.0.as_slice() {
            [ObjectNamePart::Identifier(ident)] => Ok(ident.value.clone()),
            _ => Err(ParseError::StreamingError(
                "streaming joins require single-part relation names; qualify the relation in the catalog before planning the join"
                    .to_string(),
            )),
        },
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

/// Extract and bind the left event-time column in `FOR SYSTEM_TIME AS OF`.
fn extract_temporal_left_time(
    factor: &TableFactor,
    sides: &JoinSides<'_>,
) -> Result<Option<String>, ParseError> {
    if let TableFactor::Table {
        version: Some(TableVersion::ForSystemTimeAsOf(expr)),
        ..
    } = factor
    {
        let Expr::CompoundIdentifier(parts) = expr else {
            return Err(ParseError::StreamingError(
                "FOR SYSTEM_TIME AS OF requires a qualified left event-time column".into(),
            ));
        };
        let [qualifier, column] = parts.as_slice() else {
            return Err(ParseError::StreamingError(
                "FOR SYSTEM_TIME AS OF requires a two-part left event-time column".into(),
            ));
        };
        if sides.resolve_qualifier(&qualifier.value, "temporal AS OF timestamp")? != JoinSide::Left
        {
            return Err(ParseError::StreamingError(
                "FOR SYSTEM_TIME AS OF must reference the left join input's event-time column"
                    .into(),
            ));
        }
        Ok(Some(column.value.clone()))
    } else {
        Ok(None)
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

/// Map sqlparser `JoinOperator` to our `JoinType`.
fn map_join_operator(op: &JoinOperator) -> Result<JoinType, ParseError> {
    Ok(match op {
        JoinOperator::Inner(_) | JoinOperator::Join(_) | JoinOperator::StraightJoin(_) => {
            JoinType::Inner
        }
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => JoinType::Left,
        JoinOperator::LeftSemi(_) | JoinOperator::Semi(_) => JoinType::LeftSemi,
        JoinOperator::LeftAnti(_) | JoinOperator::Anti(_) => JoinType::LeftAnti,
        JoinOperator::AsOf { .. } => {
            return Err(ParseError::StreamingError(
                "ASOF JOIN is unsupported; use a bounded JOIN with an explicit event-time interval"
                    .to_string(),
            ));
        }
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => JoinType::Right,
        JoinOperator::RightSemi(_) => JoinType::RightSemi,
        JoinOperator::RightAnti(_) => JoinType::RightAnti,
        JoinOperator::FullOuter(_) => JoinType::Full,
        // CrossJoin, CrossApply, OuterApply are rejected by get_join_constraint()
        _ => JoinType::Inner,
    })
}

/// Analyze join constraint to extract key columns, additional key columns,
/// time bound, and optional time column pair.
#[allow(clippy::type_complexity)]
fn analyze_join_constraint(
    op: &JoinOperator,
    sides: &JoinSides<'_>,
) -> Result<
    (
        String,
        String,
        Vec<(String, String)>,
        Option<Duration>,
        Option<RawTimeCols>,
    ),
    ParseError,
> {
    let constraint = get_join_constraint(op)?;

    match constraint {
        JoinConstraint::On(expr) => {
            let (key_pairs, time_bound, time_cols) = analyze_on_expression(expr, sides)?;
            if key_pairs.is_empty() {
                return Ok((String::new(), String::new(), vec![], time_bound, time_cols));
            }
            let (first_left, first_right) = key_pairs[0].clone();
            let additional = key_pairs[1..].to_vec();
            Ok((first_left, first_right, additional, time_bound, time_cols))
        }
        JoinConstraint::Using(cols) => {
            if cols.is_empty() {
                return Err(ParseError::StreamingError(
                    "USING clause requires at least one column".to_string(),
                ));
            }
            // First column is the primary key pair
            let first_col = extract_using_column(&cols[0])?;
            // Remaining columns are additional key pairs
            let additional: Vec<(String, String)> = cols[1..]
                .iter()
                .map(|c| {
                    let col = extract_using_column(c)?;
                    Ok((col.clone(), col))
                })
                .collect::<Result<_, ParseError>>()?;
            Ok((first_col.clone(), first_col, additional, None, None))
        }
        JoinConstraint::Natural => Err(ParseError::StreamingError(
            "NATURAL JOIN not supported for streaming".to_string(),
        )),
        JoinConstraint::None => Err(ParseError::StreamingError(
            "JOIN without condition not supported for streaming".to_string(),
        )),
    }
}

fn extract_using_column(name: &ObjectName) -> Result<String, ParseError> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => Ok(ident.value.clone()),
        _ => Err(ParseError::StreamingError(
            "streaming JOIN USING keys must be single column identifiers".to_string(),
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
        JoinOperator::ArrayJoin | JoinOperator::LeftArrayJoin | JoinOperator::InnerArrayJoin => {
            Err(ParseError::StreamingError(
                "ARRAY JOIN not supported for streaming".to_string(),
            ))
        }
    }
}

/// Analyze ON expression to extract all key column pairs, time bound,
/// and optional time column pair for stream-stream joins.
#[allow(clippy::type_complexity)]
fn analyze_on_expression(
    expr: &Expr,
    sides: &JoinSides<'_>,
) -> Result<(Vec<(String, String)>, Option<Duration>, Option<RawTimeCols>), ParseError> {
    // Handle compound expressions (AND)
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (mut key_pairs, left_bound, left_cols) = analyze_on_expression(left, sides)?;
            let (right_keys, right_bound, right_cols) = analyze_on_expression(right, sides)?;

            if left_bound.is_some() && right_bound.is_some() {
                return Err(ParseError::StreamingError(
                    "streaming interval joins require exactly one time-bound predicate".to_string(),
                ));
            }

            key_pairs.extend(right_keys);
            Ok((
                key_pairs,
                left_bound.or(right_bound),
                left_cols.or(right_cols),
            ))
        }
        // Equality condition: a.col = b.col
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => Ok((
            vec![orient_equality_columns(
                left,
                right,
                sides,
                "join equality",
            )?],
            None,
            None,
        )),
        // BETWEEN clause for time bound: p.ts BETWEEN o.ts AND o.ts + INTERVAL
        Expr::Between {
            expr: between_expr,
            negated,
            low,
            high,
        } => {
            if *negated {
                return Err(ParseError::StreamingError(
                    "NOT BETWEEN is not supported for streaming interval joins".to_string(),
                ));
            }

            let (expr_qualifier, expr_col) = extract_qualified_column_ref(between_expr)
                .ok_or_else(|| {
                    ParseError::StreamingError(
                        "streaming interval join timestamps must be qualified column references"
                            .to_string(),
                    )
                })?;
            let (low_qualifier, low_col) = extract_qualified_column_ref(low).ok_or_else(|| {
                ParseError::StreamingError(
                    "streaming interval join timestamps must be qualified column references"
                        .to_string(),
                )
            })?;
            let time_bound = extract_strict_interval_bound(high, &low_qualifier, &low_col)?;

            Ok((
                vec![],
                Some(time_bound),
                Some(RawTimeCols {
                    expr_qualifier,
                    expr_col,
                    low_qualifier,
                    low_col,
                }),
            ))
        }
        Expr::Nested(inner) => analyze_on_expression(inner, sides),
        _ => Err(ParseError::StreamingError(format!(
            "Unsupported join condition expression: {expr:?}"
        ))),
    }
}

fn extract_qualified_column_ref(expr: &Expr) -> Option<(String, String)> {
    match strip_nested(expr) {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((parts[0].value.clone(), parts[1].value.clone()))
        }
        _ => None,
    }
}

fn orient_equality_columns(
    expression_left: &Expr,
    expression_right: &Expr,
    sides: &JoinSides<'_>,
    context: &str,
) -> Result<(String, String), ParseError> {
    let (left_qualifier, left_column) =
        extract_qualified_column_ref(expression_left).ok_or_else(|| {
            ParseError::StreamingError(format!(
                "Cannot extract column references from {context}; operands must be qualified column references"
            ))
        })?;
    let (right_qualifier, right_column) = extract_qualified_column_ref(expression_right)
        .ok_or_else(|| {
            ParseError::StreamingError(format!(
                "Cannot extract column references from {context}; operands must be qualified column references"
            ))
        })?;
    let left_side = sides.resolve_qualifier(&left_qualifier, context)?;
    let right_side = sides.resolve_qualifier(&right_qualifier, context)?;

    match (left_side, right_side) {
        (JoinSide::Left, JoinSide::Right) => Ok((left_column, right_column)),
        (JoinSide::Right, JoinSide::Left) => Ok((right_column, left_column)),
        _ => Err(ParseError::StreamingError(format!(
            "{context} must compare one left-input column with one right-input column"
        ))),
    }
}

fn strip_nested(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

/// Parse the only admitted interval upper bound: the exact lower timestamp
/// column plus a positive interval.
fn extract_strict_interval_bound(
    high: &Expr,
    low_qualifier: &str,
    low_col: &str,
) -> Result<Duration, ParseError> {
    let Expr::BinaryOp { left, op, right } = strip_nested(high) else {
        return Err(ParseError::StreamingError(
            "streaming interval join upper bound must be the left timestamp plus an interval"
                .to_string(),
        ));
    };
    if !matches!(op, BinaryOperator::Plus) {
        return Err(ParseError::StreamingError(
            "streaming interval join upper bound must use addition".to_string(),
        ));
    }

    let Some((high_qualifier, high_col)) = extract_qualified_column_ref(left) else {
        return Err(ParseError::StreamingError(
            "streaming interval join upper bound must repeat the qualified lower timestamp"
                .to_string(),
        ));
    };
    if high_qualifier != low_qualifier || high_col != low_col {
        return Err(ParseError::StreamingError(
            "streaming interval join upper bound must use the same timestamp as its lower bound"
                .to_string(),
        ));
    }

    let interval = strip_nested(right);
    if !matches!(interval, Expr::Interval(_)) {
        return Err(ParseError::StreamingError(
            "streaming interval join upper bound must end with an INTERVAL".to_string(),
        ));
    }
    let duration = WindowRewriter::parse_interval_to_duration(interval)?;
    if duration.is_zero() {
        return Err(ParseError::StreamingError(
            "streaming interval joins require a positive finite time bound".to_string(),
        ));
    }
    Ok(duration)
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

        let join_type = map_join_operator(&join.join_operator)?;
        let sides = JoinSides {
            left_table: &prev_left_table,
            right_table: &right_table,
            left_alias: prev_left_alias.as_deref(),
            right_alias: right_alias.as_deref(),
        };

        if let Some(left_time_col) = extract_temporal_left_time(&join.relation, &sides)? {
            // Temporal join: right side has FOR SYSTEM_TIME AS OF
            let (left_key, right_key, additional, time_bound, time_cols) =
                analyze_join_constraint(&join.join_operator, &sides)?;
            if time_bound.is_some() || time_cols.is_some() {
                return Err(ParseError::StreamingError(
                    "temporal joins do not accept an additional time-bound predicate".into(),
                ));
            }

            let mut analysis = JoinAnalysis::temporal(
                prev_left_table.clone(),
                right_table.clone(),
                left_key,
                right_key,
                left_time_col,
                join_type,
            );
            analysis.left_alias.clone_from(&prev_left_alias);
            analysis.right_alias = right_alias;
            analysis.additional_key_columns = additional;
            join_steps.push(analysis);
        } else {
            // Regular join (inner, left, right, full)
            let (left_key, right_key, additional, time_bound, time_cols) =
                analyze_join_constraint(&join.join_operator, &sides)?;

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
            analysis.right_alias.clone_from(&right_alias);
            analysis.additional_key_columns = additional;

            if let Some(ref raw) = time_cols {
                let (lt, rt) = resolve_time_cols(
                    raw,
                    &analysis.left_table,
                    &analysis.right_table,
                    prev_left_alias.as_deref(),
                    right_alias.as_deref(),
                )?;
                analysis.left_time_column = Some(lt);
                analysis.right_time_column = Some(rt);
            }
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
mod tests;
