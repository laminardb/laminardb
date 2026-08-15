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
    SetExpr, Statement, TableFactor, TableVersion,
};
use sqlparser::tokenizer::{Token, TokenWithSpan, Word};

use super::window_rewriter::WindowRewriter;
use super::ParseError;
use crate::temporal::TemporalProbeSchedule;

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

pub(crate) struct ParsedTemporalProbeQuery {
    pub(crate) statement: Statement,
    pub(crate) analysis: JoinAnalysis,
    /// The normalized SQL that produced `statement`.
    ///
    /// Keep this alongside the AST because sqlparser's `Display` implementation
    /// currently renders aliased versioned tables as `table AS alias FOR
    /// SYSTEM_TIME ...`, while its parser accepts the version before the alias.
    pub(crate) normalized_sql: String,
}

#[derive(Clone)]
struct ProbeRelation {
    table: Word,
    alias: Option<Word>,
}

impl ProbeRelation {
    fn name(&self) -> &str {
        &self.table.value
    }

    fn reference(&self) -> &Word {
        self.alias.as_ref().unwrap_or(&self.table)
    }

    fn matches_qualifier(&self, qualifier: &Word) -> bool {
        qualifier.value == self.table.value
            || self
                .alias
                .as_ref()
                .is_some_and(|alias| qualifier.value == alias.value)
    }

    fn versioned_sql(&self, left_time: &Word, left: &Self) -> String {
        let alias = self
            .alias
            .as_ref()
            .map_or_else(String::new, |alias| format!(" AS {alias}"));
        format!(
            "{} FOR SYSTEM_TIME AS OF {}.{}{alias}",
            self.table,
            left.reference(),
            left_time
        )
    }
}

/// Parse the markout syntax and normalize it into the canonical AS-OF AST.
pub(crate) fn parse_temporal_probe_query(
    tokens: &[TokenWithSpan],
) -> Result<Option<ParsedTemporalProbeQuery>, ParseError> {
    let tokens: Vec<Token> = tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_) | Token::EOF))
        .map(|token| token.token.clone())
        .collect();
    let Some(temporal_index) = find_temporal_probe(&tokens)? else {
        return Ok(None);
    };

    let (clause_start, join_type) =
        if temporal_index > 0 && token_is_word(&tokens[temporal_index - 1], "LEFT") {
            (temporal_index - 1, JoinType::Left)
        } else {
            (temporal_index, JoinType::Inner)
        };
    if temporal_index > 0
        && ["RIGHT", "FULL", "ANTI", "SEMI"]
            .iter()
            .any(|kind| token_is_word(&tokens[temporal_index - 1], kind))
    {
        return Err(ParseError::StreamingError(
            "TEMPORAL PROBE JOIN supports only INNER or LEFT semantics".into(),
        ));
    }

    let from_index =
        find_last_top_level_word(&tokens[..clause_start], "FROM").ok_or_else(|| {
            ParseError::StreamingError("TEMPORAL PROBE JOIN requires one left relation".into())
        })?;
    let left = parse_probe_relation(&tokens[from_index + 1..clause_start], "left")?;

    let mut cursor = temporal_index + 3;
    let on_index = find_top_level_word_from(&tokens, cursor, "ON").ok_or_else(|| {
        ParseError::StreamingError("TEMPORAL PROBE JOIN requires ON (key, ...)".into())
    })?;
    let right = parse_probe_relation(&tokens[cursor..on_index], "right")?;
    cursor = on_index + 1;

    expect_token(&tokens, &mut cursor, &Token::LParen, "ON (")?;
    let mut keys = Vec::new();
    loop {
        keys.push(take_word(
            &tokens,
            &mut cursor,
            "temporal probe equality key",
        )?);
        match tokens.get(cursor) {
            Some(Token::Comma) => cursor += 1,
            Some(Token::RParen) => {
                cursor += 1;
                break;
            }
            _ => {
                return Err(ParseError::StreamingError(
                    "TEMPORAL PROBE JOIN keys must be a comma-separated identifier list".into(),
                ));
            }
        }
    }
    expect_word(&tokens, &mut cursor, "TIMESTAMPS")?;
    expect_token(&tokens, &mut cursor, &Token::LParen, "TIMESTAMPS (")?;
    let left_time = parse_probe_column(&tokens, &mut cursor, &Token::Comma)?;
    expect_token(
        &tokens,
        &mut cursor,
        &Token::Comma,
        "TIMESTAMPS (left, right)",
    )?;
    let right_time = parse_probe_column(&tokens, &mut cursor, &Token::RParen)?;
    expect_token(
        &tokens,
        &mut cursor,
        &Token::RParen,
        "TIMESTAMPS (left, right)",
    )?;
    validate_probe_column_side(&left_time, &left, "left")?;
    validate_probe_column_side(&right_time, &right, "right")?;

    let schedule = if consume_word(&tokens, &mut cursor, "LIST") {
        parse_probe_list(&tokens, &mut cursor)?
    } else if consume_word(&tokens, &mut cursor, "RANGE") {
        parse_probe_range(&tokens, &mut cursor)?
    } else {
        return Err(ParseError::StreamingError(
            "TEMPORAL PROBE JOIN requires LIST (...) or RANGE FROM ... TO ... STEP ...".into(),
        ));
    };
    expect_word(&tokens, &mut cursor, "AS")?;
    let probe_alias = take_word(&tokens, &mut cursor, "temporal probe output alias")?;

    let join = match join_type {
        JoinType::Inner => "JOIN",
        JoinType::Left => "LEFT JOIN",
        _ => unreachable!("temporal probe parser admits only INNER or LEFT"),
    };
    let right_sql = right.versioned_sql(&left_time.column, &left);
    let equality_predicate = keys
        .iter()
        .map(|key| {
            format!(
                "{}.{} = {}.{}",
                left.reference(),
                key,
                right.reference(),
                key
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let normalized_join = format!("{join} {right_sql} ON {equality_predicate}");
    let normalized_sql = tokens[..clause_start]
        .iter()
        .map(ToString::to_string)
        .chain(std::iter::once(normalized_join))
        .chain(tokens[cursor..].iter().map(ToString::to_string))
        .collect::<Vec<_>>()
        .join(" ");
    let dialect = crate::parser::dialect::LaminarDialect::default();
    let mut statements = sqlparser::parser::Parser::parse_sql(&dialect, &normalized_sql)
        .map_err(ParseError::SqlParseError)?;
    let [statement] = statements.as_mut_slice() else {
        return Err(ParseError::StreamingError(
            "TEMPORAL PROBE JOIN must contain exactly one SELECT query".into(),
        ));
    };
    let Statement::Query(query) = statement else {
        return Err(ParseError::StreamingError(
            "TEMPORAL PROBE JOIN is valid only in a SELECT query".into(),
        ));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(ParseError::StreamingError(
            "TEMPORAL PROBE JOIN requires a direct SELECT query".into(),
        ));
    };
    let mut analysis = analyze_join(select)?.ok_or_else(|| {
        ParseError::StreamingError("normalized temporal probe join is missing its join".into())
    })?;
    let normalized_keys = std::iter::once((
        analysis.left_key_column.as_str(),
        analysis.right_key_column.as_str(),
    ))
    .chain(
        analysis
            .additional_key_columns
            .iter()
            .map(|(left, right)| (left.as_str(), right.as_str())),
    );
    if analysis.left_table != left.name()
        || analysis.right_table != right.name()
        || !normalized_keys.eq(keys
            .iter()
            .map(|key| (key.value.as_str(), key.value.as_str())))
    {
        return Err(ParseError::StreamingError(
            "TEMPORAL PROBE JOIN normalization changed its relation or key binding".into(),
        ));
    }
    analysis.join_type = join_type;
    analysis.right_time_column = Some(right_time.column.value);
    analysis.temporal_probe_schedule = Some(schedule);
    analysis.temporal_probe_alias = Some(probe_alias.value);

    Ok(Some(ParsedTemporalProbeQuery {
        statement: statements.remove(0),
        analysis,
        normalized_sql,
    }))
}

#[derive(Clone)]
struct ProbeColumn {
    qualifier: Option<Word>,
    column: Word,
}

fn find_temporal_probe(tokens: &[Token]) -> Result<Option<usize>, ParseError> {
    let mut depth = 0i32;
    let mut found = None;
    for index in 0..tokens.len().saturating_sub(2) {
        match tokens[index] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ => {}
        }
        if depth == 0
            && token_is_word(&tokens[index], "TEMPORAL")
            && token_is_word(&tokens[index + 1], "PROBE")
            && token_is_word(&tokens[index + 2], "JOIN")
        {
            if found.is_some() {
                return Err(ParseError::StreamingError(
                    "a query may contain only one TEMPORAL PROBE JOIN".into(),
                ));
            }
            found = Some(index);
        }
    }
    Ok(found)
}

fn find_last_top_level_word(tokens: &[Token], expected: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut found = None;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ if depth == 0 && token_is_word(token, expected) => found = Some(index),
            _ => {}
        }
    }
    found
}

fn find_top_level_word_from(tokens: &[Token], start: usize, expected: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ if depth == 0 && token_is_word(token, expected) => return Some(index),
            _ => {}
        }
    }
    None
}

fn parse_probe_relation(tokens: &[Token], side: &str) -> Result<ProbeRelation, ParseError> {
    let error = || {
        ParseError::StreamingError(format!(
            "TEMPORAL PROBE JOIN {side} relation must be a single-part table with an optional alias"
        ))
    };
    let (table, alias) = match tokens {
        [Token::Word(table)] => (table.clone(), None),
        [Token::Word(table), Token::Word(alias)] if !token_is_word(&tokens[1], "AS") => {
            (table.clone(), Some(alias.clone()))
        }
        [Token::Word(table), as_token, Token::Word(alias)] if token_is_word(as_token, "AS") => {
            (table.clone(), Some(alias.clone()))
        }
        _ => return Err(error()),
    };
    Ok(ProbeRelation { table, alias })
}

fn parse_probe_column(
    tokens: &[Token],
    cursor: &mut usize,
    terminator: &Token,
) -> Result<ProbeColumn, ParseError> {
    let first = take_word(tokens, cursor, "temporal probe timestamp column")?;
    if tokens.get(*cursor) == Some(&Token::Period) {
        *cursor += 1;
        let column = take_word(tokens, cursor, "temporal probe timestamp column")?;
        if tokens.get(*cursor) != Some(terminator) {
            return Err(ParseError::StreamingError(
                "temporal probe timestamps must be one- or two-part column references".into(),
            ));
        }
        Ok(ProbeColumn {
            qualifier: Some(first),
            column,
        })
    } else {
        if tokens.get(*cursor) != Some(terminator) {
            return Err(ParseError::StreamingError(
                "temporal probe timestamps must be one- or two-part column references".into(),
            ));
        }
        Ok(ProbeColumn {
            qualifier: None,
            column: first,
        })
    }
}

fn validate_probe_column_side(
    column: &ProbeColumn,
    relation: &ProbeRelation,
    side: &str,
) -> Result<(), ParseError> {
    if column
        .qualifier
        .as_ref()
        .is_some_and(|qualifier| !relation.matches_qualifier(qualifier))
    {
        return Err(ParseError::StreamingError(format!(
            "TEMPORAL PROBE JOIN {side} timestamp must reference its {side} relation"
        )));
    }
    Ok(())
}

fn parse_probe_list(
    tokens: &[Token],
    cursor: &mut usize,
) -> Result<TemporalProbeSchedule, ParseError> {
    expect_token(tokens, cursor, &Token::LParen, "LIST (")?;
    let mut offsets = Vec::new();
    loop {
        offsets.push(parse_probe_duration_ms(tokens, cursor)?);
        if tokens.get(*cursor) == Some(&Token::Comma) {
            *cursor += 1;
            continue;
        }
        expect_token(tokens, cursor, &Token::RParen, "LIST (...)")?;
        break;
    }
    TemporalProbeSchedule::list(offsets).map_err(ParseError::StreamingError)
}

fn parse_probe_range(
    tokens: &[Token],
    cursor: &mut usize,
) -> Result<TemporalProbeSchedule, ParseError> {
    expect_word(tokens, cursor, "FROM")?;
    let start_ms = parse_probe_duration_ms(tokens, cursor)?;
    expect_word(tokens, cursor, "TO")?;
    let end_ms = parse_probe_duration_ms(tokens, cursor)?;
    expect_word(tokens, cursor, "STEP")?;
    let step_ms = parse_probe_duration_ms(tokens, cursor)?;
    TemporalProbeSchedule::range(start_ms, end_ms, step_ms).map_err(ParseError::StreamingError)
}

fn parse_probe_duration_ms(tokens: &[Token], cursor: &mut usize) -> Result<i64, ParseError> {
    let negative = if tokens.get(*cursor) == Some(&Token::Minus) {
        *cursor += 1;
        true
    } else {
        false
    };
    let value = match tokens.get(*cursor) {
        Some(Token::Number(value, false)) => value.parse::<i64>().map_err(|_| {
            ParseError::StreamingError(format!("invalid temporal probe duration '{value}'"))
        })?,
        other => {
            return Err(ParseError::StreamingError(format!(
                "temporal probe duration requires an integer, found {other:?}"
            )))
        }
    };
    *cursor += 1;
    let unit = take_word(tokens, cursor, "temporal probe duration unit")?;
    let multiplier = match unit.value.to_ascii_lowercase().as_str() {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(ParseError::StreamingError(format!(
                "unsupported temporal probe duration unit '{}'; use ms, s, m, h, or d",
                unit.value
            )))
        }
    };
    let value = value.checked_mul(multiplier).ok_or_else(|| {
        ParseError::StreamingError("temporal probe duration overflows milliseconds".into())
    })?;
    if negative {
        value.checked_neg().ok_or_else(|| {
            ParseError::StreamingError("temporal probe duration overflows milliseconds".into())
        })
    } else {
        Ok(value)
    }
}

fn token_is_word(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}

fn consume_word(tokens: &[Token], cursor: &mut usize, expected: &str) -> bool {
    if tokens
        .get(*cursor)
        .is_some_and(|token| token_is_word(token, expected))
    {
        *cursor += 1;
        true
    } else {
        false
    }
}

fn expect_word(tokens: &[Token], cursor: &mut usize, expected: &str) -> Result<(), ParseError> {
    if consume_word(tokens, cursor, expected) {
        Ok(())
    } else {
        Err(ParseError::StreamingError(format!(
            "TEMPORAL PROBE JOIN expected {expected}"
        )))
    }
}

fn take_word(tokens: &[Token], cursor: &mut usize, context: &str) -> Result<Word, ParseError> {
    let Some(Token::Word(word)) = tokens.get(*cursor) else {
        return Err(ParseError::StreamingError(format!(
            "expected {context} identifier"
        )));
    };
    *cursor += 1;
    Ok(word.clone())
}

fn expect_token(
    tokens: &[Token],
    cursor: &mut usize,
    expected: &Token,
    context: &str,
) -> Result<(), ParseError> {
    if tokens.get(*cursor) == Some(expected) {
        *cursor += 1;
        Ok(())
    } else {
        Err(ParseError::StreamingError(format!(
            "TEMPORAL PROBE JOIN expected {context}"
        )))
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

    fn join_error(sql: &str) -> String {
        analyze_join(&parse_select(sql)).unwrap_err().to_string()
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
        assert_eq!(analysis.left_time_column.as_deref(), Some("ts"));
        assert_eq!(analysis.right_time_column.as_deref(), Some("ts"));
    }

    #[test]
    fn test_interval_join_accepts_table_qualifiers() {
        let sql = "SELECT * FROM orders
                   JOIN payments ON orders.order_id = payments.order_id
                   AND payments.received_at BETWEEN orders.created_at
                       AND orders.created_at + INTERVAL '250' MILLISECOND";

        let analysis = analyze_join(&parse_select(sql)).unwrap().unwrap();

        assert_eq!(analysis.time_bound, Some(Duration::from_millis(250)));
        assert_eq!(analysis.left_time_column.as_deref(), Some("created_at"));
        assert_eq!(analysis.right_time_column.as_deref(), Some("received_at"));
    }

    #[test]
    fn test_interval_join_preserves_composite_equality_keys() {
        let sql = "SELECT * FROM orders o JOIN payments p
                   ON o.tenant_id = p.tenant_id
                   AND o.order_id = p.order_id
                   AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND";

        let analysis = analyze_join(&parse_select(sql)).unwrap().unwrap();

        assert_eq!(analysis.left_key_column, "tenant_id");
        assert_eq!(analysis.right_key_column, "tenant_id");
        assert_eq!(
            analysis.additional_key_columns,
            vec![("order_id".to_string(), "order_id".to_string())]
        );
    }

    #[test]
    fn test_join_orients_reversed_different_name_keys() {
        let analysis = analyze_join(&parse_select(
            "SELECT * FROM orders o JOIN payments p ON p.order_id = o.id",
        ))
        .unwrap()
        .unwrap();

        assert_eq!(analysis.left_key_column, "id");
        assert_eq!(analysis.right_key_column, "order_id");
    }

    #[test]
    fn test_composite_join_orients_each_key_independently() {
        let analysis = analyze_join(&parse_select(
            "SELECT * FROM orders o JOIN payments p
             ON p.order_id = o.id AND o.tenant = p.account",
        ))
        .unwrap()
        .unwrap();

        assert_eq!(analysis.left_key_column, "id");
        assert_eq!(analysis.right_key_column, "order_id");
        assert_eq!(
            analysis.additional_key_columns,
            vec![("tenant".to_string(), "account".to_string())]
        );
    }

    #[test]
    fn test_join_accepts_quoted_relation_and_key_identity() {
        let analysis = analyze_join(&parse_select(
            "SELECT * FROM \"Orders\"
             JOIN \"Payments\"
             ON \"Payments\".\"order id\" = \"Orders\".\"id\"",
        ))
        .unwrap()
        .unwrap();

        assert_eq!(analysis.left_table, "Orders");
        assert_eq!(analysis.right_table, "Payments");
        assert_eq!(analysis.left_key_column, "id");
        assert_eq!(analysis.right_key_column, "order id");
    }

    #[test]
    fn test_join_accepts_quoted_alias_identity() {
        let analysis = analyze_join(&parse_select(
            "SELECT * FROM orders AS \"left input\"
             JOIN payments AS \"right input\"
             ON \"left input\".id = \"right input\".order_id",
        ))
        .unwrap()
        .unwrap();

        assert_eq!(analysis.left_alias.as_deref(), Some("left input"));
        assert_eq!(analysis.right_alias.as_deref(), Some("right input"));
        assert_eq!(analysis.left_key_column, "id");
        assert_eq!(analysis.right_key_column, "order_id");
    }

    #[test]
    fn test_join_rejects_unqualified_key() {
        let error = join_error("SELECT * FROM orders o JOIN payments p ON id = p.order_id");
        assert!(error.contains("qualified column references"), "{error}");
    }

    #[test]
    fn test_join_rejects_unknown_key_qualifier() {
        let error = join_error("SELECT * FROM orders o JOIN payments p ON missing.id = p.order_id");
        assert!(error.contains("names neither input"), "{error}");
    }

    #[test]
    fn test_join_rejects_same_side_key_expression() {
        let error = join_error("SELECT * FROM orders o JOIN payments p ON o.id = o.parent_id");
        assert!(error.contains("one left-input column"), "{error}");
    }

    #[test]
    fn test_join_rejects_ambiguous_qualifier() {
        let error = join_error(
            "SELECT * FROM orders duplicate JOIN payments duplicate
             ON duplicate.id = payments.order_id",
        );
        assert!(error.contains("names both inputs"), "{error}");
    }

    #[test]
    fn test_join_rejects_compound_relation_identity() {
        let error = join_error(
            "SELECT * FROM catalog.orders JOIN payments
             ON catalog.orders.id = payments.order_id",
        );
        assert!(error.contains("single-part relation names"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_unqualified_timestamp() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN ts AND ts + INTERVAL '1' SECOND",
        );
        assert!(error.contains("qualified column references"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_unknown_qualifier() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND unknown.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND",
        );
        assert!(error.contains("unambiguous left and right"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_reversed_timestamps() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND o.ts BETWEEN p.ts AND p.ts + INTERVAL '1' SECOND",
        );
        assert!(
            error.contains("right timestamp BETWEEN the left"),
            "{error}"
        );
    }

    #[test]
    fn test_interval_join_rejects_not_between() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts NOT BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND",
        );
        assert!(error.contains("NOT BETWEEN"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_mismatched_upper_timestamp() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.other_ts + INTERVAL '1' SECOND",
        );
        assert!(error.contains("same timestamp"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_subtracted_bound() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.ts - INTERVAL '1' SECOND",
        );
        assert!(error.contains("must use addition"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_direct_interval_upper_bound() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND INTERVAL '1' SECOND",
        );
        assert!(error.contains("left timestamp plus an interval"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_zero_bound() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '0' SECOND",
        );
        assert!(error.contains("positive finite"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_negative_bound() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '-1' SECOND",
        );
        assert!(error.contains("Invalid interval value"), "{error}");
    }

    #[test]
    fn test_interval_join_rejects_multiple_time_bounds() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND
             AND p.created_at BETWEEN o.created_at
                 AND o.created_at + INTERVAL '1' SECOND",
        );
        assert!(
            error.contains("exactly one time-bound predicate"),
            "{error}"
        );
    }

    #[test]
    fn test_join_rejects_non_equi_residual_conjunct() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p
             ON o.id = p.id AND p.amount > o.amount",
        );
        assert!(error.contains("Unsupported join condition"), "{error}");
    }

    #[test]
    fn test_join_rejects_unsupported_equality_conjunct() {
        let error = join_error(
            "SELECT * FROM orders o JOIN payments p
             ON o.id = p.id AND ABS(o.amount) = ABS(p.amount)",
        );
        assert!(
            error.contains("Cannot extract column references"),
            "{error}"
        );
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

    fn parse_select_laminar(sql: &str) -> Select {
        let dialect = crate::parser::dialect::LaminarDialect::default();
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = query.body.as_ref() {
                return *select.clone();
            }
        }
        panic!("Expected SELECT query");
    }

    #[test]
    fn rejects_asof_join_with_bounded_interval_guidance() {
        let select = parse_select_snowflake(
            "SELECT * FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol",
        );
        let error = analyze_join(&select).unwrap_err().to_string();

        assert!(
            error.contains(
                "ASOF JOIN is unsupported; use a bounded JOIN with an explicit event-time interval"
            ),
            "{error}"
        );
    }

    // -- Multi-way JOIN tests --

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
        let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON c.b_id = b.id";
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
    fn test_multi_join_stream_stream_and_lookup() {
        let sql = "SELECT * FROM orders o \
                    JOIN payments p ON o.id = p.order_id \
                        AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR \
                    JOIN customers c ON p.customer_id = c.id";
        let select = parse_select(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert_eq!(multi.len(), 2);
        assert!(!multi.joins[0].is_lookup_join); // stream-stream
        assert!(multi.joins[0].time_bound.is_some());
        assert!(multi.joins[1].is_lookup_join); // lookup
    }

    #[test]
    fn test_multi_join_rejects_key_from_non_current_left_relation() {
        let select = parse_select(
            "SELECT * FROM a
             JOIN b ON a.id = b.a_id
             JOIN c ON a.id = c.a_id",
        );
        let error = analyze_joins(&select).unwrap_err().to_string();

        assert!(error.contains("names neither input"), "{error}");
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

    // -- Temporal JOIN tests (FOR SYSTEM_TIME AS OF) --

    #[test]
    fn test_temporal_join_detected() {
        let sql = "SELECT o.*, p.price \
                    FROM orders o \
                    JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p \
                    ON o.product_id = p.id";
        let select = parse_select_laminar(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert!(analysis.is_temporal_join());
        assert_eq!(analysis.left_time_column.as_deref(), Some("order_time"));
        assert_eq!(analysis.left_table, "orders");
        assert_eq!(analysis.right_table, "products");
        assert_eq!(analysis.left_key_column, "product_id");
        assert_eq!(analysis.right_key_column, "id");
        assert!(!analysis.is_lookup_join);
    }

    #[test]
    fn test_temporal_join_via_analyze_joins() {
        let sql = "SELECT o.*, p.price \
                    FROM orders o \
                    JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p \
                    ON o.product_id = p.id";
        let select = parse_select_laminar(sql);
        let multi = analyze_joins(&select).unwrap().unwrap();

        assert_eq!(multi.len(), 1);
        let first = multi.first().unwrap();
        assert!(first.is_temporal_join());
        assert_eq!(first.left_time_column.as_deref(), Some("order_time"));
    }

    #[test]
    fn test_non_temporal_join_not_flagged() {
        let sql = "SELECT * FROM orders o JOIN payments p ON o.id = p.order_id";
        let select = parse_select(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        assert!(!analysis.is_temporal_join());
        assert!(analysis.temporal_probe_schedule.is_none());
    }

    #[test]
    fn test_unqualified_anti_maps_to_left_anti() {
        let sql = "SELECT * FROM orders o ANTI JOIN returns r ON o.id = r.order_id";
        let select = parse_select(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();
        assert_eq!(analysis.join_type, JoinType::LeftAnti);
    }

    #[test]
    fn test_unqualified_semi_maps_to_left_semi() {
        let sql = "SELECT * FROM orders o SEMI JOIN payments p ON o.id = p.order_id";
        let select = parse_select(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();
        assert_eq!(analysis.join_type, JoinType::LeftSemi);
    }

    #[test]
    fn test_composite_join_keys() {
        let sql = "SELECT * FROM orders o \
                    JOIN shipments s \
                    ON o.order_id = s.order_id AND o.region = s.region";
        let select = parse_select(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        // First key pair is the primary key
        assert_eq!(analysis.left_key_column, "order_id");
        assert_eq!(analysis.right_key_column, "order_id");

        // Second key pair should be in additional_key_columns
        assert_eq!(
            analysis.additional_key_columns.len(),
            1,
            "Should have 1 additional key pair"
        );
        assert_eq!(analysis.additional_key_columns[0].0, "region");
        assert_eq!(analysis.additional_key_columns[0].1, "region");
    }

    #[test]
    fn test_composite_using_clause() {
        let sql = "SELECT * FROM orders o JOIN shipments s USING (order_id, region)";
        let select = parse_select(sql);
        let analysis = analyze_join(&select).unwrap().unwrap();

        // First column becomes primary key
        assert_eq!(analysis.left_key_column, "order_id");
        assert_eq!(analysis.right_key_column, "order_id");

        // Additional columns
        assert_eq!(
            analysis.additional_key_columns.len(),
            1,
            "USING(order_id, region) should have 1 additional key"
        );
        assert_eq!(analysis.additional_key_columns[0].0, "region");
        assert_eq!(analysis.additional_key_columns[0].1, "region");
    }

    #[test]
    fn test_using_preserves_quoted_key_identity() {
        let sql = "SELECT * FROM orders o JOIN shipments s USING (\"order id\")";
        let analysis = analyze_join(&parse_select(sql)).unwrap().unwrap();

        assert_eq!(analysis.left_key_column, "order id");
        assert_eq!(analysis.right_key_column, "order id");
    }
}
