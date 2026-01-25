//! Simplified parser implementation for streaming SQL extensions
//! This provides a basic implementation that extends standard SQL parsing

use sqlparser::ast::{Expr, Ident, ObjectName, ObjectNamePart};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

use std::collections::HashMap;

use super::statements::{
    CreateSinkStatement, CreateSourceStatement, EmitClause, LateDataClause, SinkFrom,
    StreamingStatement,
};
use super::ParseError;

/// Parser for streaming SQL extensions
pub struct StreamingParser;

impl StreamingParser {
    /// Parse a SQL string with streaming extensions.
    ///
    /// # Errors
    ///
    /// Returns `ParserError` if the SQL syntax is invalid.
    pub fn parse_sql(sql: &str) -> Result<Vec<StreamingStatement>, ParserError> {
        // First, try to parse as standard SQL
        let dialect = GenericDialect {};

        // Check for streaming-specific keywords at the beginning
        let sql_trimmed = sql.trim();
        let sql_upper = sql_trimmed.to_uppercase();

        if sql_upper.starts_with("CREATE SOURCE") ||
           sql_upper.starts_with("CREATE SINK") ||
           sql_upper.starts_with("CREATE CONTINUOUS QUERY") {
            // For now, return a placeholder for streaming statements
            // In a full implementation, we'd parse these properly
            return Ok(vec![Self::parse_streaming_statement(sql_trimmed)?]);
        }

        // Parse as standard SQL
        let statements = Parser::parse_sql(&dialect, sql)?;

        // Convert to streaming statements and check for window functions
        let mut streaming_statements = Vec::new();
        for statement in statements {
            // TODO: Check for window functions in SELECT statements
            streaming_statements.push(StreamingStatement::Standard(Box::new(statement)));
        }

        Ok(streaming_statements)
    }

    /// Parse a streaming-specific statement
    fn parse_streaming_statement(sql: &str) -> Result<StreamingStatement, ParserError> {
        let sql_upper = sql.to_uppercase();

        if sql_upper.starts_with("CREATE SOURCE") {
            // Simplified CREATE SOURCE parsing
            Ok(StreamingStatement::CreateSource(Box::new(CreateSourceStatement {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
                columns: vec![],
                watermark: None,
                with_options: HashMap::new(),
                or_replace: sql_upper.contains("OR REPLACE"),
                if_not_exists: sql_upper.contains("IF NOT EXISTS"),
            })))
        } else if sql_upper.starts_with("CREATE SINK") {
            // Simplified CREATE SINK parsing
            Ok(StreamingStatement::CreateSink(Box::new(CreateSinkStatement {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("output_sink"))]),
                from: SinkFrom::Table(ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))])),
                with_options: HashMap::new(),
                or_replace: sql_upper.contains("OR REPLACE"),
                if_not_exists: sql_upper.contains("IF NOT EXISTS"),
            })))
        } else if sql_upper.starts_with("CREATE CONTINUOUS QUERY") {
            // Parse the EMIT clause using the improved parser
            let emit_clause = Self::parse_emit_clause(sql).ok().flatten();

            // For now, parse the actual query from the SQL string
            // In production, we'd properly parse the query portion
            let query_start = sql.find("AS").unwrap_or(sql.len());
            let emit_start = sql.to_uppercase().find("EMIT").unwrap_or(sql.len());
            let query_sql = sql[query_start..emit_start].trim();

            // Try to parse the query portion
            let query_stmt = if query_sql.len() > 2 && query_sql.starts_with("AS") {
                let actual_query = query_sql[2..].trim();
                if let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, actual_query) {
                    if stmts.is_empty() {
                        // Default to a simple SELECT
                        StreamingStatement::Standard(Box::new(Parser::parse_sql(&GenericDialect {}, "SELECT 1").unwrap().remove(0)))
                    } else {
                        StreamingStatement::Standard(Box::new(stmts.remove(0)))
                    }
                } else {
                    StreamingStatement::Standard(Box::new(Parser::parse_sql(&GenericDialect {}, "SELECT 1").unwrap().remove(0)))
                }
            } else {
                StreamingStatement::Standard(Box::new(Parser::parse_sql(&GenericDialect {}, "SELECT 1").unwrap().remove(0)))
            };

            Ok(StreamingStatement::CreateContinuousQuery {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("query"))]),
                query: Box::new(query_stmt),
                emit_clause,
            })
        } else {
            Err(ParserError::ParserError(
                "Unknown streaming statement type".to_string()
            ))
        }
    }

    /// Check if an expression contains a window function.
    #[must_use]
    pub fn has_window_function(expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                if let Some(name) = func.name.0.last() {
                    let func_name = name.to_string().to_uppercase();
                    matches!(func_name.as_str(), "TUMBLE" | "HOP" | "SESSION")
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Parse EMIT clause from SQL string.
    ///
    /// Supported syntax:
    /// - `EMIT AFTER WATERMARK` or `EMIT ON WATERMARK`
    /// - `EMIT ON WINDOW CLOSE`
    /// - `EMIT EVERY INTERVAL 'N' SECOND|MINUTE|HOUR` or `EMIT PERIODICALLY INTERVAL ...`
    /// - `EMIT ON UPDATE`
    /// - `EMIT CHANGES` (F011B)
    /// - `EMIT FINAL` (F011B)
    ///
    /// # Errors
    ///
    /// Returns `ParseError::StreamingError` if the EMIT clause syntax is invalid.
    pub fn parse_emit_clause(sql: &str) -> Result<Option<EmitClause>, ParseError> {
        let sql_upper = sql.to_uppercase();

        // Check for EMIT keyword
        let Some(emit_pos) = sql_upper.find("EMIT ") else {
            return Ok(None);
        };

        let emit_clause = &sql_upper[emit_pos..];

        // EMIT AFTER WATERMARK or EMIT ON WATERMARK
        if emit_clause.contains("AFTER WATERMARK") || emit_clause.contains("ON WATERMARK") {
            return Ok(Some(EmitClause::AfterWatermark));
        }

        // EMIT ON WINDOW CLOSE
        if emit_clause.contains("ON WINDOW CLOSE") {
            return Ok(Some(EmitClause::OnWindowClose));
        }

        // EMIT ON UPDATE
        if emit_clause.contains("ON UPDATE") {
            return Ok(Some(EmitClause::OnUpdate));
        }

        // F011B: EMIT CHANGES (changelog/Z-set)
        if emit_clause.starts_with("EMIT CHANGES") {
            return Ok(Some(EmitClause::Changes));
        }

        // F011B: EMIT FINAL (suppress intermediate)
        if emit_clause.starts_with("EMIT FINAL") {
            return Ok(Some(EmitClause::Final));
        }

        // EMIT EVERY INTERVAL or EMIT PERIODICALLY INTERVAL
        if emit_clause.contains("EVERY") || emit_clause.contains("PERIODICALLY") {
            // Extract the interval portion
            let interval_expr = Self::parse_interval_from_emit(sql, emit_pos);
            return Ok(Some(EmitClause::Periodically {
                interval: Box::new(interval_expr),
            }));
        }

        // Unknown EMIT clause
        Err(ParseError::StreamingError(format!(
            "Unknown EMIT clause syntax: {}",
            &sql[emit_pos..].chars().take(50).collect::<String>()
        )))
    }

    /// Parse interval expression from EMIT clause.
    ///
    /// Handles: `EMIT EVERY INTERVAL '10' SECOND` or `EMIT PERIODICALLY INTERVAL '5' MINUTE`
    fn parse_interval_from_emit(sql: &str, emit_pos: usize) -> Expr {
        let sql_upper = sql.to_uppercase();
        let emit_clause = &sql_upper[emit_pos..];

        // Find INTERVAL keyword
        let interval_pos = emit_clause.find("INTERVAL");
        if interval_pos.is_none() {
            // No INTERVAL keyword - look for a simple number with unit
            // e.g., "EMIT EVERY 10 SECONDS"
            return Self::parse_simple_interval(emit_clause);
        }

        let interval_start = emit_pos + interval_pos.unwrap();
        let interval_sql = &sql[interval_start..];

        // Try to parse the interval using sqlparser
        let dialect = GenericDialect {};
        let wrapped_sql = format!(
            "SELECT {}",
            interval_sql
                .split_whitespace()
                .take(4)
                .collect::<Vec<_>>()
                .join(" ")
        );

        match Parser::parse_sql(&dialect, &wrapped_sql) {
            Ok(stmts) if !stmts.is_empty() => {
                if let sqlparser::ast::Statement::Query(query) = &stmts[0] {
                    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                        if !select.projection.is_empty() {
                            if let sqlparser::ast::SelectItem::UnnamedExpr(expr) =
                                &select.projection[0]
                            {
                                return expr.clone();
                            }
                        }
                    }
                }
                // Fallback to identifier
                Expr::Identifier(Ident::new(
                    interval_sql
                        .split_whitespace()
                        .take(4)
                        .collect::<Vec<_>>()
                        .join(" "),
                ))
            }
            _ => {
                // Fallback: return the interval portion as an identifier
                Expr::Identifier(Ident::new(
                    interval_sql
                        .split_whitespace()
                        .take(4)
                        .collect::<Vec<_>>()
                        .join(" "),
                ))
            }
        }
    }

    /// Parse a simple interval like "10 SECONDS" or "5 MINUTES".
    fn parse_simple_interval(emit_clause: &str) -> Expr {
        // Look for patterns like "EVERY 10 SECOND" or "PERIODICALLY 5 MINUTE"
        let words: Vec<&str> = emit_clause.split_whitespace().collect();

        // Find the index after EVERY or PERIODICALLY
        let start_idx = words
            .iter()
            .position(|&w| w == "EVERY" || w == "PERIODICALLY")
            .map(|i| i + 1);

        if let Some(idx) = start_idx {
            if idx < words.len() {
                // Try to parse as number + unit
                let remaining: String = words[idx..].join(" ");

                // Create an interval expression
                let interval_sql = format!("INTERVAL '{}'", remaining.replace('\'', ""));
                return Expr::Identifier(Ident::new(interval_sql));
            }
        }

        // Default fallback
        Expr::Identifier(Ident::new("INTERVAL '1' SECOND"))
    }

    /// Parse late data handling clause from SQL string.
    ///
    /// Supported syntax:
    /// - `ALLOW LATENESS INTERVAL 'N' UNIT`
    /// - `LATE DATA TO <sink_name>`
    /// - Both combined: `ALLOW LATENESS INTERVAL '1' HOUR LATE DATA TO late_events`
    ///
    /// # Errors
    ///
    /// Returns `ParseError::StreamingError` if the late data clause syntax is invalid.
    pub fn parse_late_data_clause(sql: &str) -> Result<Option<LateDataClause>, ParseError> {
        let sql_upper = sql.to_uppercase();

        let has_allow_lateness = sql_upper.contains("ALLOW LATENESS");
        let has_late_data_to = sql_upper.contains("LATE DATA TO");

        if !has_allow_lateness && !has_late_data_to {
            return Ok(None);
        }

        let mut clause = LateDataClause::default();

        // Parse ALLOW LATENESS INTERVAL
        if has_allow_lateness {
            if let Some(pos) = sql_upper.find("ALLOW LATENESS") {
                let lateness_sql = &sql[pos..];
                let interval_expr = Self::parse_interval_after_keyword(lateness_sql, "LATENESS");
                clause.allowed_lateness = Some(Box::new(interval_expr));
            }
        }

        // Parse LATE DATA TO <sink_name>
        if has_late_data_to {
            if let Some(pos) = sql_upper.find("LATE DATA TO") {
                let after_to = &sql[pos + 12..].trim();
                // Extract the sink name (next identifier)
                let sink_name = after_to
                    .split_whitespace()
                    .next()
                    .map(|s| s.trim_end_matches(';').to_string())
                    .unwrap_or_default();

                if !sink_name.is_empty() {
                    clause.side_output = Some(sink_name);
                }
            }
        }

        Ok(Some(clause))
    }

    /// Parse interval expression after a keyword (e.g., "LATENESS INTERVAL '1' HOUR").
    fn parse_interval_after_keyword(sql: &str, keyword: &str) -> Expr {
        let sql_upper = sql.to_uppercase();

        // Find INTERVAL after the keyword
        let keyword_pos = sql_upper.find(keyword).unwrap_or(0);
        let after_keyword = &sql[keyword_pos + keyword.len()..];

        if let Some(interval_pos) = after_keyword.to_uppercase().find("INTERVAL") {
            let interval_start = keyword_pos + keyword.len() + interval_pos;
            let interval_sql = &sql[interval_start..];

            // Try to parse using sqlparser
            let dialect = GenericDialect {};
            let wrapped_sql = format!(
                "SELECT {}",
                interval_sql
                    .split_whitespace()
                    .take(4)
                    .collect::<Vec<_>>()
                    .join(" ")
            );

            if let Ok(stmts) = Parser::parse_sql(&dialect, &wrapped_sql) {
                if !stmts.is_empty() {
                    if let sqlparser::ast::Statement::Query(query) = &stmts[0] {
                        if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                            if !select.projection.is_empty() {
                                if let sqlparser::ast::SelectItem::UnnamedExpr(expr) =
                                    &select.projection[0]
                                {
                                    return expr.clone();
                                }
                            }
                        }
                    }
                }
            }

            // Fallback to identifier
            return Expr::Identifier(Ident::new(
                interval_sql
                    .split_whitespace()
                    .take(4)
                    .collect::<Vec<_>>()
                    .join(" "),
            ));
        }

        // Default fallback
        Expr::Identifier(Ident::new("INTERVAL '0' SECOND"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_source() {
        let sql = "CREATE SOURCE events (id BIGINT, timestamp TIMESTAMP)";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(matches!(&statements[0], StreamingStatement::CreateSource(_)));
    }

    #[test]
    fn test_parse_create_sink() {
        let sql = "CREATE SINK output_sink FROM events";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(matches!(&statements[0], StreamingStatement::CreateSink(_)));
    }

    #[test]
    fn test_parse_standard_sql() {
        let sql = "SELECT * FROM events WHERE id > 100";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        assert!(matches!(&statements[0], StreamingStatement::Standard(_)));
    }

    #[test]
    fn test_parse_continuous_query() {
        let sql = "CREATE CONTINUOUS QUERY live_stats AS SELECT COUNT(*) FROM events EMIT AFTER WATERMARK";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::AfterWatermark)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_emit_clause_parsing() {
        // Test EMIT AFTER WATERMARK
        let emit1 = StreamingParser::parse_emit_clause("SELECT * EMIT AFTER WATERMARK").unwrap();
        assert!(matches!(emit1, Some(EmitClause::AfterWatermark)));

        // Test EMIT ON WATERMARK (synonym)
        let emit1b = StreamingParser::parse_emit_clause("SELECT * EMIT ON WATERMARK").unwrap();
        assert!(matches!(emit1b, Some(EmitClause::AfterWatermark)));

        // Test EMIT ON WINDOW CLOSE
        let emit2 = StreamingParser::parse_emit_clause("SELECT * EMIT ON WINDOW CLOSE").unwrap();
        assert!(matches!(emit2, Some(EmitClause::OnWindowClose)));

        // Test EMIT ON UPDATE
        let emit3 = StreamingParser::parse_emit_clause("SELECT * EMIT ON UPDATE").unwrap();
        assert!(matches!(emit3, Some(EmitClause::OnUpdate)));

        // Test EMIT PERIODICALLY
        let emit4 = StreamingParser::parse_emit_clause("SELECT * EMIT PERIODICALLY INTERVAL '5' SECOND").unwrap();
        assert!(matches!(emit4, Some(EmitClause::Periodically { .. })));

        // Test EMIT EVERY
        let emit5 = StreamingParser::parse_emit_clause("SELECT * EMIT EVERY INTERVAL '10' SECOND").unwrap();
        assert!(matches!(emit5, Some(EmitClause::Periodically { .. })));

        // Test no EMIT clause
        let emit6 = StreamingParser::parse_emit_clause("SELECT * FROM events").unwrap();
        assert!(emit6.is_none());
    }

    #[test]
    fn test_continuous_query_with_emit_on_update() {
        let sql = "CREATE CONTINUOUS QUERY live_stats AS SELECT COUNT(*) FROM events EMIT ON UPDATE";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::OnUpdate)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_emit_every() {
        let sql = "CREATE CONTINUOUS QUERY dashboard AS SELECT SUM(amount) FROM sales EMIT EVERY INTERVAL '30' SECOND";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::Periodically { .. })));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    // ==================== Late Data Clause Tests ====================

    #[test]
    fn test_parse_allow_lateness() {
        let sql = "SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) ALLOW LATENESS INTERVAL '5' MINUTE";
        let clause = StreamingParser::parse_late_data_clause(sql).unwrap();
        assert!(clause.is_some());
        let clause = clause.unwrap();
        assert!(clause.allowed_lateness.is_some());
        assert!(clause.side_output.is_none());
    }

    #[test]
    fn test_parse_late_data_to() {
        let sql = "SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) LATE DATA TO late_events";
        let clause = StreamingParser::parse_late_data_clause(sql).unwrap();
        assert!(clause.is_some());
        let clause = clause.unwrap();
        assert!(clause.allowed_lateness.is_none());
        assert_eq!(clause.side_output, Some("late_events".to_string()));
    }

    #[test]
    fn test_parse_allow_lateness_with_late_data_to() {
        let sql = "SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR) ALLOW LATENESS INTERVAL '1' HOUR LATE DATA TO late_events";
        let clause = StreamingParser::parse_late_data_clause(sql).unwrap();
        assert!(clause.is_some());
        let clause = clause.unwrap();
        assert!(clause.allowed_lateness.is_some());
        assert_eq!(clause.side_output, Some("late_events".to_string()));
    }

    #[test]
    fn test_parse_no_late_data_clause() {
        let sql = "SELECT * FROM events GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)";
        let clause = StreamingParser::parse_late_data_clause(sql).unwrap();
        assert!(clause.is_none());
    }

    #[test]
    fn test_parse_late_data_to_with_semicolon() {
        let sql = "SELECT * FROM events LATE DATA TO my_side_output;";
        let clause = StreamingParser::parse_late_data_clause(sql).unwrap();
        assert!(clause.is_some());
        let clause = clause.unwrap();
        assert_eq!(clause.side_output, Some("my_side_output".to_string()));
    }

    // ==================== F011B Tests ====================

    #[test]
    fn test_emit_clause_changes() {
        let emit = StreamingParser::parse_emit_clause("SELECT * EMIT CHANGES").unwrap();
        assert!(matches!(emit, Some(EmitClause::Changes)));
    }

    #[test]
    fn test_emit_clause_final() {
        let emit = StreamingParser::parse_emit_clause("SELECT * EMIT FINAL").unwrap();
        assert!(matches!(emit, Some(EmitClause::Final)));
    }

    #[test]
    fn test_continuous_query_with_emit_changes() {
        let sql = "CREATE CONTINUOUS QUERY cdc_pipeline AS SELECT * FROM orders EMIT CHANGES";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::Changes)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_emit_final() {
        let sql = "CREATE CONTINUOUS QUERY bi_report AS SELECT SUM(amount) FROM sales EMIT FINAL";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::Final)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_emit_on_window_close_is_distinct() {
        // Test that ON WINDOW CLOSE is parsed correctly
        let emit1 = StreamingParser::parse_emit_clause("SELECT * EMIT ON WINDOW CLOSE").unwrap();
        assert!(matches!(emit1, Some(EmitClause::OnWindowClose)));

        // Test that it's distinct from AFTER WATERMARK
        let emit2 = StreamingParser::parse_emit_clause("SELECT * EMIT AFTER WATERMARK").unwrap();
        assert!(matches!(emit2, Some(EmitClause::AfterWatermark)));

        // They should be different variants - compare the inner enum
        let inner1 = emit1.unwrap();
        let inner2 = emit2.unwrap();
        assert_ne!(
            std::mem::discriminant(&inner1),
            std::mem::discriminant(&inner2),
            "OnWindowClose and AfterWatermark should be distinct"
        );
    }
}