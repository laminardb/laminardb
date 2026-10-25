//! Options of `CREATE MATERIALIZED VIEW ... WITH (...)`.

use std::time::Duration;

use super::{tokenizer, window_rewriter::WindowRewriter, ParseError};

/// How long an append-only materialized view keeps its rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializedViewRetention {
    /// Rows within this interval of the view's event-time frontier, e.g. `'1 hour'`.
    Interval(Duration),
    /// The latest rows, e.g. `'10000 rows'`.
    Rows(u64),
    /// Every row, e.g. `'unbounded'`.
    Unbounded,
}

/// Options declared in a view's trailing `WITH (...)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ViewOptions {
    pub(super) retention: Option<MaterializedViewRetention>,
    pub(super) freshness: Option<Duration>,
}

/// Parse the trailing `WITH (...)` tokens of a view: `retention` and `freshness`.
pub(super) fn parse_view_options(
    tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Result<ViewOptions, ParseError> {
    let dialect = super::dialect::LaminarDialect::default();
    let mut parser = sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
    let options = tokenizer::parse_with_options(&mut parser)?;
    let mut parsed = ViewOptions::default();
    for (key, value) in options {
        if key.eq_ignore_ascii_case("retention") {
            parsed.retention = Some(parse_retention(&value)?);
        } else if key.eq_ignore_ascii_case("freshness") {
            parsed.freshness = Some(parse_freshness(&value)?);
        } else {
            return Err(ParseError::StreamingError(format!(
                "unknown CREATE MATERIALIZED VIEW option '{key}' (expected retention or freshness)"
            )));
        }
    }
    Ok(parsed)
}

fn parse_freshness(value: &str) -> Result<Duration, ParseError> {
    let value = value.trim();
    WindowRewriter::parse_interval_string(value)
        .ok()
        .filter(|interval| !interval.is_zero())
        .ok_or_else(|| {
            ParseError::StreamingError(format!(
                "freshness '{value}' must be a nonzero interval such as '5 seconds'"
            ))
        })
}

fn parse_retention(value: &str) -> Result<MaterializedViewRetention, ParseError> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("unbounded") {
        return Ok(MaterializedViewRetention::Unbounded);
    }
    let invalid = || {
        ParseError::StreamingError(format!(
            "retention '{value}' must be an interval such as '1 hour', a row count such as \
             '10000 rows', or 'unbounded'"
        ))
    };
    let mut parts = value.split_whitespace();
    if let (Some(count), Some(unit), None) = (parts.next(), parts.next(), parts.next()) {
        if unit.eq_ignore_ascii_case("rows") || unit.eq_ignore_ascii_case("row") {
            let rows: u64 = count.parse().map_err(|_| invalid())?;
            return if rows == 0 {
                Err(invalid())
            } else {
                Ok(MaterializedViewRetention::Rows(rows))
            };
        }
    }
    let interval = WindowRewriter::parse_interval_string(value).map_err(|_| invalid())?;
    if interval.is_zero() {
        return Err(invalid());
    }
    Ok(MaterializedViewRetention::Interval(interval))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_accepts_intervals_row_counts_and_unbounded() {
        assert_eq!(
            parse_retention("1 hour").unwrap(),
            MaterializedViewRetention::Interval(Duration::from_secs(3600))
        );
        assert_eq!(
            parse_retention("30 MINUTES").unwrap(),
            MaterializedViewRetention::Interval(Duration::from_secs(1800))
        );
        assert_eq!(
            parse_retention("10000 rows").unwrap(),
            MaterializedViewRetention::Rows(10_000)
        );
        assert_eq!(
            parse_retention("UNBOUNDED").unwrap(),
            MaterializedViewRetention::Unbounded
        );
        assert_eq!(
            parse_freshness("500 milliseconds").unwrap(),
            Duration::from_millis(500)
        );
        for invalid in ["", "0 seconds", "10 rows"] {
            assert!(parse_freshness(invalid).is_err(), "{invalid}");
        }
        for invalid in [
            "",
            "0 rows",
            "0 seconds",
            "ten rows",
            "1 fortnight",
            "5 rows extra",
        ] {
            assert!(parse_retention(invalid).is_err(), "{invalid}");
        }
    }
}
