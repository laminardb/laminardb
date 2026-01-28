//! Late data clause parser using sqlparser tokens.
//!
//! Supported syntax:
//! - `ALLOW LATENESS INTERVAL 'N' UNIT`
//! - `LATE DATA TO <sink_name>`
//! - Both combined

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use super::statements::LateDataClause;
use super::tokenizer::{expect_custom_keyword, try_parse_custom_keyword};
use super::ParseError;

// Note: Keyword::ALLOW does not exist in sqlparser 0.60,
// so we use try_parse_custom_keyword instead.

/// Parse late data handling clause from the parser's current position.
///
/// Returns `Ok(None)` if neither ALLOW LATENESS nor LATE DATA TO is found.
///
/// # Errors
///
/// Returns `ParseError` if the clause syntax is invalid.
pub fn parse_late_data_clause(parser: &mut Parser) -> Result<Option<LateDataClause>, ParseError> {
    let mut clause = LateDataClause::default();
    let mut found = false;

    // ALLOW LATENESS INTERVAL ...
    if try_parse_custom_keyword(parser, "ALLOW") {
        expect_custom_keyword(parser, "LATENESS")?;
        let interval = parser.parse_expr().map_err(ParseError::SqlParseError)?;
        clause.allowed_lateness = Some(Box::new(interval));
        found = true;
    }

    // LATE DATA TO <sink_name>
    if try_parse_custom_keyword(parser, "LATE") {
        expect_custom_keyword(parser, "DATA")?;
        parser
            .expect_keyword(Keyword::TO)
            .map_err(ParseError::SqlParseError)?;
        let sink_name = parser
            .parse_identifier()
            .map_err(ParseError::SqlParseError)?;
        clause.side_output = Some(sink_name.value);
        found = true;
    }

    if found {
        Ok(Some(clause))
    } else {
        Ok(None)
    }
}

/// Parse late data handling clause from a raw SQL string.
///
/// Convenience wrapper for backward compatibility.
///
/// # Errors
///
/// Returns `ParseError` if the clause syntax is invalid.
pub fn parse_late_data_clause_from_sql(sql: &str) -> Result<Option<LateDataClause>, ParseError> {
    let sql_upper = sql.to_uppercase();

    let has_allow_lateness = sql_upper.contains("ALLOW LATENESS");
    let has_late_data_to = sql_upper.contains("LATE DATA TO");

    if !has_allow_lateness && !has_late_data_to {
        return Ok(None);
    }

    // Find the earliest clause position
    let start_pos = if has_allow_lateness {
        sql_upper.find("ALLOW").unwrap_or(0)
    } else {
        sql_upper
            .find("LATE DATA TO")
            .map_or(0, |p| {
                // Find the "LATE" word start (not "ALLOW LATENESS" overlap)
                sql_upper[..p]
                    .rfind("LATE")
                    .unwrap_or(p)
            })
    };

    let clause_sql = &sql[start_pos..];
    let dialect = super::dialect::LaminarDialect::default();
    let mut parser = Parser::new(&dialect)
        .try_with_sql(clause_sql)
        .map_err(ParseError::SqlParseError)?;

    parse_late_data_clause(&mut parser)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_from_sql(sql: &str) -> Option<LateDataClause> {
        parse_late_data_clause_from_sql(sql).unwrap()
    }

    #[test]
    fn test_parse_allow_lateness() {
        let clause = parse_from_sql(
            "SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) ALLOW LATENESS INTERVAL '5' MINUTE",
        );
        assert!(clause.is_some());
        let clause = clause.unwrap();
        assert!(clause.allowed_lateness.is_some());
        assert!(clause.side_output.is_none());
    }

    #[test]
    fn test_parse_late_data_to() {
        let clause = parse_from_sql(
            "SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) LATE DATA TO late_events",
        );
        assert!(clause.is_some());
        let clause = clause.unwrap();
        assert!(clause.allowed_lateness.is_none());
        assert_eq!(clause.side_output, Some("late_events".to_string()));
    }

    #[test]
    fn test_parse_both_combined() {
        let clause = parse_from_sql(
            "SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) ALLOW LATENESS INTERVAL '1' HOUR LATE DATA TO late_events",
        );
        assert!(clause.is_some());
        let clause = clause.unwrap();
        assert!(clause.allowed_lateness.is_some());
        assert_eq!(clause.side_output, Some("late_events".to_string()));
    }

    #[test]
    fn test_parse_no_late_data_clause() {
        let clause = parse_from_sql(
            "SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)",
        );
        assert!(clause.is_none());
    }

    #[test]
    fn test_parse_late_data_to_with_semicolon() {
        let clause = parse_from_sql("SELECT * FROM events LATE DATA TO my_side_output;");
        assert!(clause.is_some());
        let clause = clause.unwrap();
        assert_eq!(clause.side_output, Some("my_side_output".to_string()));
    }
}
