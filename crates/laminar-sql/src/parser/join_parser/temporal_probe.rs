//! Parsing and canonical normalization for `TEMPORAL PROBE JOIN`.

use sqlparser::ast::{SetExpr, Statement};
use sqlparser::tokenizer::{Token, TokenWithSpan, Word};

use super::{analyze_join, JoinAnalysis, JoinType};
use crate::parser::ParseError;
use crate::temporal::TemporalProbeSchedule;

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
    let tokens = significant_tokens(tokens);
    let Some(temporal_index) = find_temporal_probe(&tokens)? else {
        return Ok(None);
    };
    let start = classify_probe_join(&tokens, temporal_index)?;
    let clause = parse_probe_clause(&tokens, start)?;
    let normalized_sql = clause.normalized_sql(&tokens);
    let (statement, analysis) = parse_normalized_join(&normalized_sql)?;
    clause.validate_normalized_analysis(&analysis)?;

    Ok(Some(ParsedTemporalProbeQuery {
        statement,
        analysis: clause.finish_analysis(analysis),
        normalized_sql,
    }))
}

fn significant_tokens(tokens: &[TokenWithSpan]) -> Vec<Token> {
    tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_) | Token::EOF))
        .map(|token| token.token.clone())
        .collect()
}

#[derive(Clone, Copy)]
struct ProbeJoinStart {
    temporal_index: usize,
    clause_start: usize,
    join_type: JoinType,
}

fn classify_probe_join(
    tokens: &[Token],
    temporal_index: usize,
) -> Result<ProbeJoinStart, ParseError> {
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
    Ok(ProbeJoinStart {
        temporal_index,
        clause_start,
        join_type,
    })
}

struct ProbeClause {
    clause_start: usize,
    suffix_start: usize,
    join_type: JoinType,
    left: ProbeRelation,
    right: ProbeRelation,
    keys: Vec<Word>,
    left_time: ProbeColumn,
    right_time: ProbeColumn,
    schedule: TemporalProbeSchedule,
    probe_alias: Word,
}

fn parse_probe_clause(tokens: &[Token], start: ProbeJoinStart) -> Result<ProbeClause, ParseError> {
    let left = parse_left_relation(tokens, start.clause_start)?;
    let (right, mut cursor) = parse_right_relation(tokens, start.temporal_index)?;
    let keys = parse_probe_keys(tokens, &mut cursor)?;
    let (left_time, right_time) = parse_probe_timestamps(tokens, &mut cursor)?;
    validate_probe_column_side(&left_time, &left, "left")?;
    validate_probe_column_side(&right_time, &right, "right")?;
    let schedule = parse_probe_schedule(tokens, &mut cursor)?;
    expect_word(tokens, &mut cursor, "AS")?;
    let probe_alias = take_word(tokens, &mut cursor, "temporal probe output alias")?;

    Ok(ProbeClause {
        clause_start: start.clause_start,
        suffix_start: cursor,
        join_type: start.join_type,
        left,
        right,
        keys,
        left_time,
        right_time,
        schedule,
        probe_alias,
    })
}

fn parse_left_relation(tokens: &[Token], clause_start: usize) -> Result<ProbeRelation, ParseError> {
    let from_index =
        find_last_top_level_word(&tokens[..clause_start], "FROM").ok_or_else(|| {
            ParseError::StreamingError("TEMPORAL PROBE JOIN requires one left relation".into())
        })?;
    parse_probe_relation(&tokens[from_index + 1..clause_start], "left")
}

fn parse_right_relation(
    tokens: &[Token],
    temporal_index: usize,
) -> Result<(ProbeRelation, usize), ParseError> {
    let mut cursor = temporal_index + 3;
    let on_index = find_top_level_word_from(tokens, cursor, "ON").ok_or_else(|| {
        ParseError::StreamingError("TEMPORAL PROBE JOIN requires ON (key, ...)".into())
    })?;
    let right = parse_probe_relation(&tokens[cursor..on_index], "right")?;
    cursor = on_index + 1;
    Ok((right, cursor))
}

fn parse_probe_keys(tokens: &[Token], cursor: &mut usize) -> Result<Vec<Word>, ParseError> {
    expect_token(tokens, cursor, &Token::LParen, "ON (")?;
    let mut keys = Vec::new();
    loop {
        keys.push(take_word(tokens, cursor, "temporal probe equality key")?);
        match tokens.get(*cursor) {
            Some(Token::Comma) => *cursor += 1,
            Some(Token::RParen) => {
                *cursor += 1;
                break;
            }
            _ => {
                return Err(ParseError::StreamingError(
                    "TEMPORAL PROBE JOIN keys must be a comma-separated identifier list".into(),
                ));
            }
        }
    }
    Ok(keys)
}

fn parse_probe_timestamps(
    tokens: &[Token],
    cursor: &mut usize,
) -> Result<(ProbeColumn, ProbeColumn), ParseError> {
    expect_word(tokens, cursor, "TIMESTAMPS")?;
    expect_token(tokens, cursor, &Token::LParen, "TIMESTAMPS (")?;
    let left_time = parse_probe_column(tokens, cursor, &Token::Comma)?;
    expect_token(tokens, cursor, &Token::Comma, "TIMESTAMPS (left, right)")?;
    let right_time = parse_probe_column(tokens, cursor, &Token::RParen)?;
    expect_token(tokens, cursor, &Token::RParen, "TIMESTAMPS (left, right)")?;
    Ok((left_time, right_time))
}

fn parse_probe_schedule(
    tokens: &[Token],
    cursor: &mut usize,
) -> Result<TemporalProbeSchedule, ParseError> {
    if consume_word(tokens, cursor, "LIST") {
        parse_probe_list(tokens, cursor)
    } else if consume_word(tokens, cursor, "RANGE") {
        parse_probe_range(tokens, cursor)
    } else {
        Err(ParseError::StreamingError(
            "TEMPORAL PROBE JOIN requires LIST (...) or RANGE FROM ... TO ... STEP ...".into(),
        ))
    }
}

impl ProbeClause {
    fn normalized_sql(&self, tokens: &[Token]) -> String {
        let join = match self.join_type {
            JoinType::Inner => "JOIN",
            JoinType::Left => "LEFT JOIN",
            _ => unreachable!("temporal probe parser admits only INNER or LEFT"),
        };
        let right_sql = self.right.versioned_sql(&self.left_time.column, &self.left);
        let equality_predicate = self
            .keys
            .iter()
            .map(|key| {
                format!(
                    "{}.{} = {}.{}",
                    self.left.reference(),
                    key,
                    self.right.reference(),
                    key
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let normalized_join = format!("{join} {right_sql} ON {equality_predicate}");

        tokens[..self.clause_start]
            .iter()
            .map(ToString::to_string)
            .chain(std::iter::once(normalized_join))
            .chain(tokens[self.suffix_start..].iter().map(ToString::to_string))
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn validate_normalized_analysis(&self, analysis: &JoinAnalysis) -> Result<(), ParseError> {
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
        if analysis.left_table != self.left.name()
            || analysis.right_table != self.right.name()
            || !normalized_keys.eq(self
                .keys
                .iter()
                .map(|key| (key.value.as_str(), key.value.as_str())))
        {
            return Err(ParseError::StreamingError(
                "TEMPORAL PROBE JOIN normalization changed its relation or key binding".into(),
            ));
        }
        Ok(())
    }

    fn finish_analysis(self, mut analysis: JoinAnalysis) -> JoinAnalysis {
        analysis.join_type = self.join_type;
        analysis.right_time_column = Some(self.right_time.column.value);
        analysis.temporal_probe_schedule = Some(self.schedule);
        analysis.temporal_probe_alias = Some(self.probe_alias.value);
        analysis
    }
}

fn parse_normalized_join(normalized_sql: &str) -> Result<(Statement, JoinAnalysis), ParseError> {
    let dialect = crate::parser::dialect::LaminarDialect::default();
    let mut statements = sqlparser::parser::Parser::parse_sql(&dialect, normalized_sql)
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
    let analysis = analyze_join(select)?.ok_or_else(|| {
        ParseError::StreamingError("normalized temporal probe join is missing its join".into())
    })?;
    Ok((statements.remove(0), analysis))
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
