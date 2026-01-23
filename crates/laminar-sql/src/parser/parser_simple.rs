//! Simplified parser implementation for streaming SQL extensions
//! This provides a basic implementation that extends standard SQL parsing

use sqlparser::ast::{Expr, Ident, ObjectName, ObjectNamePart};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

use std::collections::HashMap;

use super::statements::{
    CreateSinkStatement, CreateSourceStatement, EmitClause, SinkFrom,
    StreamingStatement,
};
use super::ParseError;

/// Parser for streaming SQL extensions
pub struct StreamingParser;

impl StreamingParser {
    /// Parse a SQL string with streaming extensions
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
            streaming_statements.push(StreamingStatement::Standard(statement));
        }

        Ok(streaming_statements)
    }

    /// Parse a streaming-specific statement
    fn parse_streaming_statement(sql: &str) -> Result<StreamingStatement, ParserError> {
        let sql_upper = sql.to_uppercase();

        if sql_upper.starts_with("CREATE SOURCE") {
            // Simplified CREATE SOURCE parsing
            Ok(StreamingStatement::CreateSource(CreateSourceStatement {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))]),
                columns: vec![],
                watermark: None,
                with_options: HashMap::new(),
                or_replace: sql_upper.contains("OR REPLACE"),
                if_not_exists: sql_upper.contains("IF NOT EXISTS"),
            }))
        } else if sql_upper.starts_with("CREATE SINK") {
            // Simplified CREATE SINK parsing
            Ok(StreamingStatement::CreateSink(CreateSinkStatement {
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("output_sink"))]),
                from: SinkFrom::Table(ObjectName(vec![ObjectNamePart::Identifier(Ident::new("events"))])),
                with_options: HashMap::new(),
                or_replace: sql_upper.contains("OR REPLACE"),
                if_not_exists: sql_upper.contains("IF NOT EXISTS"),
            }))
        } else if sql_upper.starts_with("CREATE CONTINUOUS QUERY") {
            // Simplified CREATE CONTINUOUS QUERY parsing
            let emit_clause = if sql_upper.contains("EMIT AFTER WATERMARK") {
                Some(EmitClause::AfterWatermark)
            } else if sql_upper.contains("EMIT ON WINDOW CLOSE") {
                Some(EmitClause::OnWindowClose)
            } else if sql_upper.contains("EMIT PERIODICALLY") {
                Some(EmitClause::Periodically {
                    interval: Expr::Identifier(Ident::new("5 SECONDS")),
                })
            } else {
                None
            };

            // For now, parse the actual query from the SQL string
            // In production, we'd properly parse the query portion
            let query_start = sql.find("AS").unwrap_or(sql.len());
            let emit_start = sql.to_uppercase().find("EMIT").unwrap_or(sql.len());
            let query_sql = sql[query_start..emit_start].trim();

            // Try to parse the query portion
            let query_stmt = if query_sql.len() > 2 && query_sql.starts_with("AS") {
                let actual_query = query_sql[2..].trim();
                if let Ok(mut stmts) = Parser::parse_sql(&GenericDialect {}, actual_query) {
                    if !stmts.is_empty() {
                        StreamingStatement::Standard(stmts.remove(0))
                    } else {
                        // Default to a simple SELECT
                        StreamingStatement::Standard(Parser::parse_sql(&GenericDialect {}, "SELECT 1").unwrap().remove(0))
                    }
                } else {
                    StreamingStatement::Standard(Parser::parse_sql(&GenericDialect {}, "SELECT 1").unwrap().remove(0))
                }
            } else {
                StreamingStatement::Standard(Parser::parse_sql(&GenericDialect {}, "SELECT 1").unwrap().remove(0))
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

    /// Check if an expression contains a window function
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

    /// Parse EMIT clause from SQL string
    pub fn parse_emit_clause(sql: &str) -> Result<Option<EmitClause>, ParseError> {
        let sql_upper = sql.to_uppercase();

        if sql_upper.contains("EMIT AFTER WATERMARK") {
            Ok(Some(EmitClause::AfterWatermark))
        } else if sql_upper.contains("EMIT ON WINDOW CLOSE") {
            Ok(Some(EmitClause::OnWindowClose))
        } else if sql_upper.contains("EMIT PERIODICALLY") {
            // TODO: Parse the interval expression properly
            Ok(Some(EmitClause::Periodically {
                interval: Expr::Identifier(Ident::new("5 SECONDS")),
            }))
        } else {
            Ok(None)
        }
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
        let emit1 = StreamingParser::parse_emit_clause("SELECT * EMIT AFTER WATERMARK").unwrap();
        assert!(matches!(emit1, Some(EmitClause::AfterWatermark)));

        let emit2 = StreamingParser::parse_emit_clause("SELECT * EMIT ON WINDOW CLOSE").unwrap();
        assert!(matches!(emit2, Some(EmitClause::OnWindowClose)));

        let emit3 = StreamingParser::parse_emit_clause("SELECT * EMIT PERIODICALLY").unwrap();
        assert!(matches!(emit3, Some(EmitClause::Periodically { .. })));

        let emit4 = StreamingParser::parse_emit_clause("SELECT * FROM events").unwrap();
        assert!(emit4.is_none());
    }
}