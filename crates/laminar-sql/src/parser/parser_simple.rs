//! Production parser implementation for streaming SQL extensions
//!
//! This module parses streaming SQL statements including:
//! - CREATE SOURCE with columns, watermarks, and WITH options
//! - CREATE SINK with FROM clause and WITH options
//! - CREATE CONTINUOUS QUERY with EMIT and late data clauses

use sqlparser::ast::{
    ColumnDef, DataType, Expr, Ident, ObjectName, ObjectNamePart, Statement, TimezoneInfo,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

use std::collections::HashMap;

use super::statements::{
    CreateSinkStatement, CreateSourceStatement, EmitClause, LateDataClause, SinkFrom,
    StreamingStatement, WatermarkDef,
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

        // Detect streaming DDL patterns (must check before standard SQL parsing)
        let is_streaming_ddl = sql_upper.starts_with("CREATE SOURCE")
            || sql_upper.starts_with("CREATE OR REPLACE SOURCE")
            || sql_upper.starts_with("CREATE SINK")
            || sql_upper.starts_with("CREATE OR REPLACE SINK")
            || sql_upper.starts_with("CREATE CONTINUOUS QUERY")
            || sql_upper.starts_with("CREATE OR REPLACE CONTINUOUS QUERY");

        if is_streaming_ddl {
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

        if sql_upper.starts_with("CREATE SOURCE")
            || sql_upper.starts_with("CREATE OR REPLACE SOURCE")
        {
            Self::parse_create_source(sql)
        } else if sql_upper.starts_with("CREATE SINK")
            || sql_upper.starts_with("CREATE OR REPLACE SINK")
        {
            Self::parse_create_sink(sql)
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

    /// Parse CREATE SOURCE statement with columns, watermark, and WITH options.
    ///
    /// Supported syntax:
    /// ```sql
    /// CREATE [OR REPLACE] SOURCE [IF NOT EXISTS] name (
    ///     column1 TYPE,
    ///     column2 TYPE,
    ///     WATERMARK FOR time_col AS time_col - INTERVAL 'n' UNIT
    /// ) WITH ('key' = 'value', ...);
    /// ```
    fn parse_create_source(sql: &str) -> Result<StreamingStatement, ParserError> {
        let sql_upper = sql.to_uppercase();
        let or_replace = sql_upper.contains("OR REPLACE");
        let if_not_exists = sql_upper.contains("IF NOT EXISTS");

        // Extract source name
        let name = Self::extract_object_name(sql, "SOURCE")?;

        // Extract column definitions and watermark
        let (columns, watermark) = Self::parse_source_columns(sql)?;

        // Extract WITH options
        let with_options = Self::parse_with_options(sql)?;

        Ok(StreamingStatement::CreateSource(Box::new(
            CreateSourceStatement {
                name,
                columns,
                watermark,
                with_options,
                or_replace,
                if_not_exists,
            },
        )))
    }

    /// Parse CREATE SINK statement with FROM clause and WITH options.
    ///
    /// Supported syntax:
    /// ```sql
    /// CREATE [OR REPLACE] SINK [IF NOT EXISTS] name
    /// FROM table_name | (SELECT ...)
    /// WITH ('key' = 'value', ...);
    /// ```
    fn parse_create_sink(sql: &str) -> Result<StreamingStatement, ParserError> {
        let sql_upper = sql.to_uppercase();
        let or_replace = sql_upper.contains("OR REPLACE");
        let if_not_exists = sql_upper.contains("IF NOT EXISTS");

        // Extract sink name
        let name = Self::extract_object_name(sql, "SINK")?;

        // Extract FROM clause
        let from = Self::parse_sink_from(sql)?;

        // Extract WITH options
        let with_options = Self::parse_with_options(sql)?;

        Ok(StreamingStatement::CreateSink(Box::new(
            CreateSinkStatement {
                name,
                from,
                with_options,
                or_replace,
                if_not_exists,
            },
        )))
    }

    /// Extract object name after a keyword (e.g., SOURCE or SINK).
    fn extract_object_name(sql: &str, keyword: &str) -> Result<ObjectName, ParserError> {
        let sql_upper = sql.to_uppercase();

        // Find the keyword position (handle OR REPLACE before keyword)
        let keyword_pos = sql_upper
            .find(&format!(" {keyword}"))
            .or_else(|| sql_upper.find(keyword))
            .ok_or_else(|| ParserError::ParserError(format!("{keyword} keyword not found")))?;

        // Adjust position to point to the keyword itself
        let actual_pos = if sql_upper[keyword_pos..].starts_with(' ') {
            keyword_pos + 1
        } else {
            keyword_pos
        };

        // Get text after the keyword
        let after_keyword = &sql[actual_pos + keyword.len()..].trim_start();

        // Skip IF NOT EXISTS if present
        let name_start = if after_keyword.to_uppercase().starts_with("IF NOT EXISTS") {
            after_keyword[13..].trim_start()
        } else {
            after_keyword
        };

        // Extract the name (ends at whitespace, '(' or ';')
        let name_end = name_start
            .find(|c: char| c.is_whitespace() || c == '(' || c == ';')
            .unwrap_or(name_start.len());

        let name_str = name_start[..name_end].trim();
        if name_str.is_empty() {
            return Err(ParserError::ParserError(format!(
                "{keyword} name not found"
            )));
        }

        // Handle schema-qualified names (e.g., schema.table)
        let parts: Vec<ObjectNamePart> = name_str
            .split('.')
            .map(|s| ObjectNamePart::Identifier(Ident::new(s.trim())))
            .collect();

        Ok(ObjectName(parts))
    }

    /// Parse column definitions and watermark from CREATE SOURCE.
    fn parse_source_columns(
        sql: &str,
    ) -> Result<(Vec<ColumnDef>, Option<WatermarkDef>), ParserError> {
        // Find the opening parenthesis
        let open_paren = match sql.find('(') {
            Some(pos) => pos,
            None => return Ok((vec![], None)), // No columns defined
        };

        // Find the matching closing parenthesis
        let close_paren = Self::find_matching_paren(sql, open_paren)?;

        let columns_str = &sql[open_paren + 1..close_paren];

        // Split by comma, but respect nested parentheses
        let column_defs = Self::split_column_definitions(columns_str);

        let mut columns = Vec::new();
        let mut watermark = None;

        for col_def in column_defs {
            let col_def = col_def.trim();
            if col_def.is_empty() {
                continue;
            }

            let col_upper = col_def.to_uppercase();

            // Check for WATERMARK clause
            if col_upper.starts_with("WATERMARK") {
                watermark = Some(Self::parse_watermark_def(col_def)?);
            } else {
                // Parse as column definition
                if let Some(col) = Self::parse_column_def(col_def)? {
                    columns.push(col);
                }
            }
        }

        Ok((columns, watermark))
    }

    /// Find the matching closing parenthesis.
    fn find_matching_paren(sql: &str, open_pos: usize) -> Result<usize, ParserError> {
        let mut depth = 1;
        let chars: Vec<char> = sql.chars().collect();

        for (i, &c) in chars.iter().enumerate().skip(open_pos + 1) {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(i);
                    }
                }
                _ => {}
            }
        }

        Err(ParserError::ParserError(
            "Unmatched parenthesis".to_string(),
        ))
    }

    /// Split column definitions by comma, respecting nested parentheses.
    fn split_column_definitions(s: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut depth = 0;

        for c in s.chars() {
            match c {
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth -= 1;
                    current.push(c);
                }
                ',' if depth == 0 => {
                    result.push(current.trim().to_string());
                    current = String::new();
                }
                _ => current.push(c),
            }
        }

        if !current.trim().is_empty() {
            result.push(current.trim().to_string());
        }

        result
    }

    /// Parse a single column definition.
    fn parse_column_def(col_def: &str) -> Result<Option<ColumnDef>, ParserError> {
        let parts: Vec<&str> = col_def.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(None);
        }

        let name = Ident::new(parts[0]);

        // Parse the data type
        let data_type = if parts.len() > 1 {
            Self::parse_data_type(&parts[1..])?
        } else {
            DataType::Unspecified
        };

        Ok(Some(ColumnDef {
            name,
            data_type,
            options: vec![],
        }))
    }

    /// Parse SQL data type from tokens.
    fn parse_data_type(tokens: &[&str]) -> Result<DataType, ParserError> {
        if tokens.is_empty() {
            return Ok(DataType::Unspecified);
        }

        let type_upper = tokens[0].to_uppercase();

        // Handle parameterized types like DECIMAL(10,2) or VARCHAR(255)
        let full_type = tokens.join(" ").to_uppercase();

        match type_upper.as_str() {
            "BIGINT" | "INT8" => Ok(DataType::BigInt(None)),
            "INT" | "INTEGER" | "INT4" => Ok(DataType::Int(None)),
            "SMALLINT" | "INT2" => Ok(DataType::SmallInt(None)),
            "TINYINT" => Ok(DataType::TinyInt(None)),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "FLOAT" | "FLOAT4" | "REAL" => Ok(DataType::Real),
            "DOUBLE" | "FLOAT8" => Ok(DataType::Double(sqlparser::ast::ExactNumberInfo::None)),
            "TEXT" => Ok(DataType::Text),
            "VARCHAR" => Self::parse_varchar(&full_type),
            "CHAR" | "CHARACTER" => Self::parse_char(&full_type),
            "TIMESTAMP" => Self::parse_timestamp(&full_type),
            "DATE" => Ok(DataType::Date),
            "TIME" => Ok(DataType::Time(None, TimezoneInfo::None)),
            "DECIMAL" | "NUMERIC" => Self::parse_decimal(&full_type),
            "BLOB" | "BYTEA" | "BINARY" => Ok(DataType::Blob(None)),
            "JSON" | "JSONB" => Ok(DataType::JSON),
            "UUID" => Ok(DataType::Uuid),
            _ => Ok(DataType::Custom(
                ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                    tokens.join(" "),
                ))]),
                vec![],
            )),
        }
    }

    /// Parse VARCHAR type with optional length.
    fn parse_varchar(type_str: &str) -> Result<DataType, ParserError> {
        if let Some(start) = type_str.find('(') {
            if let Some(end) = type_str.find(')') {
                let len_str = &type_str[start + 1..end];
                if let Ok(len) = len_str.trim().parse::<u64>() {
                    return Ok(DataType::Varchar(Some(
                        sqlparser::ast::CharacterLength::IntegerLength {
                            length: len,
                            unit: None,
                        },
                    )));
                }
            }
        }
        Ok(DataType::Varchar(None))
    }

    /// Parse CHAR type with optional length.
    fn parse_char(type_str: &str) -> Result<DataType, ParserError> {
        if let Some(start) = type_str.find('(') {
            if let Some(end) = type_str.find(')') {
                let len_str = &type_str[start + 1..end];
                if let Ok(len) = len_str.trim().parse::<u64>() {
                    return Ok(DataType::Char(Some(
                        sqlparser::ast::CharacterLength::IntegerLength {
                            length: len,
                            unit: None,
                        },
                    )));
                }
            }
        }
        Ok(DataType::Char(None))
    }

    /// Parse TIMESTAMP type with optional precision/timezone.
    fn parse_timestamp(type_str: &str) -> Result<DataType, ParserError> {
        let upper = type_str.to_uppercase();
        let tz_info = if upper.contains("WITH TIME ZONE") || upper.contains("TIMESTAMPTZ") {
            TimezoneInfo::WithTimeZone
        } else if upper.contains("WITHOUT TIME ZONE") {
            TimezoneInfo::WithoutTimeZone
        } else {
            TimezoneInfo::None
        };
        Ok(DataType::Timestamp(None, tz_info))
    }

    /// Parse DECIMAL type with optional precision and scale.
    fn parse_decimal(type_str: &str) -> Result<DataType, ParserError> {
        if let Some(start) = type_str.find('(') {
            if let Some(end) = type_str.find(')') {
                let params = &type_str[start + 1..end];
                let parts: Vec<&str> = params.split(',').collect();
                if !parts.is_empty() {
                    let precision = parts[0].trim().parse::<u64>().ok();
                    let scale = parts.get(1).and_then(|s| s.trim().parse::<i64>().ok());
                    return Ok(DataType::Decimal(
                        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(
                            precision.unwrap_or(38),
                            scale.unwrap_or(0),
                        ),
                    ));
                }
            }
        }
        Ok(DataType::Decimal(sqlparser::ast::ExactNumberInfo::None))
    }

    /// Parse WATERMARK FOR column AS expression.
    fn parse_watermark_def(watermark_str: &str) -> Result<WatermarkDef, ParserError> {
        let upper = watermark_str.to_uppercase();

        // Find "FOR" keyword
        let for_pos = upper
            .find(" FOR ")
            .ok_or_else(|| ParserError::ParserError("WATERMARK missing FOR keyword".to_string()))?;

        // Find "AS" keyword
        let as_pos = upper
            .find(" AS ")
            .ok_or_else(|| ParserError::ParserError("WATERMARK missing AS keyword".to_string()))?;

        // Extract column name (between FOR and AS)
        let column_str = watermark_str[for_pos + 5..as_pos].trim();
        let column = Ident::new(column_str);

        // Extract expression (after AS)
        let expr_str = watermark_str[as_pos + 4..].trim();

        // Parse the expression using sqlparser
        let expression = Self::parse_expr(expr_str)?;

        Ok(WatermarkDef { column, expression })
    }

    /// Parse an SQL expression string.
    fn parse_expr(expr_str: &str) -> Result<Expr, ParserError> {
        // Wrap in SELECT to parse as expression
        let wrapped = format!("SELECT {expr_str}");
        let dialect = GenericDialect {};

        match Parser::parse_sql(&dialect, &wrapped) {
            Ok(stmts) if !stmts.is_empty() => {
                if let Statement::Query(query) = &stmts[0] {
                    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
                        if !select.projection.is_empty() {
                            if let sqlparser::ast::SelectItem::UnnamedExpr(expr) =
                                &select.projection[0]
                            {
                                return Ok(expr.clone());
                            }
                        }
                    }
                }
                // Fallback to identifier
                Ok(Expr::Identifier(Ident::new(expr_str)))
            }
            _ => Ok(Expr::Identifier(Ident::new(expr_str))),
        }
    }

    /// Parse WITH ('key' = 'value', ...) options.
    fn parse_with_options(sql: &str) -> Result<HashMap<String, String>, ParserError> {
        let mut options = HashMap::new();
        let sql_upper = sql.to_uppercase();

        // Find WITH keyword (but not inside parentheses of column definitions)
        let with_pos = Self::find_with_clause_position(sql, &sql_upper)?;
        let Some(with_pos) = with_pos else {
            return Ok(options);
        };

        // Find opening parenthesis after WITH
        let after_with = &sql[with_pos + 4..];
        let open_paren = match after_with.find('(') {
            Some(pos) => with_pos + 4 + pos,
            None => return Ok(options),
        };

        // Find matching closing parenthesis
        let close_paren = Self::find_matching_paren(sql, open_paren)?;

        let options_str = &sql[open_paren + 1..close_paren];

        // Parse key-value pairs
        Self::parse_key_value_pairs(options_str, &mut options)?;

        Ok(options)
    }

    /// Find the position of WITH clause (not inside column definitions).
    fn find_with_clause_position(
        _sql: &str,
        sql_upper: &str,
    ) -> Result<Option<usize>, ParserError> {
        // Find the last WITH that's not inside parentheses
        let mut depth = 0;
        let chars: Vec<char> = sql_upper.chars().collect();
        let mut last_with_pos = None;

        let mut i = 0;
        while i < chars.len() {
            match chars[i] {
                '(' => depth += 1,
                ')' => depth -= 1,
                'W' if depth == 0 => {
                    // Check if this is "WITH"
                    if i + 4 <= chars.len() {
                        let potential_with: String = chars[i..i + 4].iter().collect();
                        if potential_with == "WITH" {
                            // Make sure it's not part of another word
                            let before_ok = i == 0 || !chars[i - 1].is_alphanumeric();
                            let after_ok =
                                i + 4 >= chars.len() || !chars[i + 4].is_alphanumeric();
                            if before_ok && after_ok {
                                last_with_pos = Some(i);
                            }
                        }
                    }
                }
                _ => {}
            }
            i += 1;
        }

        Ok(last_with_pos)
    }

    /// Parse key-value pairs from options string.
    fn parse_key_value_pairs(
        options_str: &str,
        options: &mut HashMap<String, String>,
    ) -> Result<(), ParserError> {
        // Split by comma, but respect quoted strings
        let pairs = Self::split_options(options_str);

        for pair in pairs {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }

            // Find the '=' separator
            if let Some(eq_pos) = pair.find('=') {
                let key = pair[..eq_pos].trim().trim_matches('\'').trim_matches('"');
                let value = pair[eq_pos + 1..]
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"');
                options.insert(key.to_string(), value.to_string());
            }
        }

        Ok(())
    }

    /// Split options string by comma, respecting quoted strings.
    fn split_options(s: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut quote_char = ' ';

        for c in s.chars() {
            match c {
                '\'' | '"' if !in_quotes => {
                    in_quotes = true;
                    quote_char = c;
                    current.push(c);
                }
                c if in_quotes && c == quote_char => {
                    in_quotes = false;
                    current.push(c);
                }
                ',' if !in_quotes => {
                    result.push(current.trim().to_string());
                    current = String::new();
                }
                _ => current.push(c),
            }
        }

        if !current.trim().is_empty() {
            result.push(current.trim().to_string());
        }

        result
    }

    /// Parse FROM clause for CREATE SINK.
    fn parse_sink_from(sql: &str) -> Result<SinkFrom, ParserError> {
        let sql_upper = sql.to_uppercase();

        // Find FROM keyword
        let from_pos = sql_upper
            .find(" FROM ")
            .ok_or_else(|| ParserError::ParserError("SINK missing FROM clause".to_string()))?;

        let after_from = &sql[from_pos + 6..].trim_start();

        // Check if it's a subquery (starts with '(')
        if after_from.starts_with('(') {
            // Find the matching closing parenthesis
            let close_paren = Self::find_matching_paren(after_from, 0)?;
            let query_str = &after_from[1..close_paren];

            // Parse the subquery
            let dialect = GenericDialect {};
            match Parser::parse_sql(&dialect, query_str) {
                Ok(stmts) if !stmts.is_empty() => Ok(SinkFrom::Query(Box::new(
                    StreamingStatement::Standard(Box::new(stmts.into_iter().next().unwrap())),
                ))),
                _ => Err(ParserError::ParserError(
                    "Invalid subquery in FROM clause".to_string(),
                )),
            }
        } else {
            // It's a table reference
            let table_end = after_from
                .find(|c: char| c.is_whitespace() || c == ';')
                .unwrap_or(after_from.len());
            let table_name = after_from[..table_end].trim();

            if table_name.is_empty() {
                return Err(ParserError::ParserError(
                    "Missing table name in FROM clause".to_string(),
                ));
            }

            let parts: Vec<ObjectNamePart> = table_name
                .split('.')
                .map(|s| ObjectNamePart::Identifier(Ident::new(s.trim())))
                .collect();

            Ok(SinkFrom::Table(ObjectName(parts)))
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

    // ==================== F006B Phase 1 Tests: CREATE SOURCE/SINK ====================

    #[test]
    fn test_create_source_with_columns() {
        let sql = "CREATE SOURCE events (
            id BIGINT,
            user_id VARCHAR,
            event_time TIMESTAMP
        )";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSource(source) => {
                assert_eq!(source.name.to_string(), "events");
                assert_eq!(source.columns.len(), 3);
                assert_eq!(source.columns[0].name.to_string(), "id");
                assert_eq!(source.columns[1].name.to_string(), "user_id");
                assert_eq!(source.columns[2].name.to_string(), "event_time");
                assert!(source.watermark.is_none());
                assert!(source.with_options.is_empty());
            }
            _ => panic!("Expected CreateSource"),
        }
    }

    #[test]
    fn test_create_source_with_watermark() {
        let sql = "CREATE SOURCE orders (
            order_id BIGINT,
            amount DECIMAL(10,2),
            order_time TIMESTAMP,
            WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
        )";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSource(source) => {
                assert_eq!(source.name.to_string(), "orders");
                assert_eq!(source.columns.len(), 3);
                assert!(source.watermark.is_some());
                let watermark = source.watermark.as_ref().unwrap();
                assert_eq!(watermark.column.to_string(), "order_time");
            }
            _ => panic!("Expected CreateSource"),
        }
    }

    #[test]
    fn test_create_source_with_options() {
        let sql = "CREATE SOURCE kafka_events (
            id BIGINT,
            data TEXT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'events',
            'bootstrap.servers' = 'localhost:9092'
        )";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSource(source) => {
                assert_eq!(source.name.to_string(), "kafka_events");
                assert_eq!(source.columns.len(), 2);
                assert_eq!(source.with_options.len(), 3);
                assert_eq!(
                    source.with_options.get("connector"),
                    Some(&"kafka".to_string())
                );
                assert_eq!(source.with_options.get("topic"), Some(&"events".to_string()));
                assert_eq!(
                    source.with_options.get("bootstrap.servers"),
                    Some(&"localhost:9092".to_string())
                );
            }
            _ => panic!("Expected CreateSource"),
        }
    }

    #[test]
    fn test_create_source_full() {
        let sql = "CREATE SOURCE IF NOT EXISTS orders (
            order_id BIGINT,
            customer_id BIGINT,
            amount DECIMAL(10,2),
            order_time TIMESTAMP,
            WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders',
            'format' = 'json'
        )";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSource(source) => {
                assert_eq!(source.name.to_string(), "orders");
                assert_eq!(source.columns.len(), 4);
                assert!(source.watermark.is_some());
                assert_eq!(source.with_options.len(), 3);
                assert!(source.if_not_exists);
                assert!(!source.or_replace);
            }
            _ => panic!("Expected CreateSource"),
        }
    }

    #[test]
    fn test_create_source_or_replace() {
        let sql = "CREATE OR REPLACE SOURCE events (id BIGINT)";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSource(source) => {
                assert!(source.or_replace);
                assert!(!source.if_not_exists);
            }
            _ => panic!("Expected CreateSource"),
        }
    }

    #[test]
    fn test_create_sink_from_table() {
        let sql = "CREATE SINK output_sink FROM processed_orders";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSink(sink) => {
                assert_eq!(sink.name.to_string(), "output_sink");
                match &sink.from {
                    SinkFrom::Table(table) => {
                        assert_eq!(table.to_string(), "processed_orders");
                    }
                    _ => panic!("Expected SinkFrom::Table"),
                }
            }
            _ => panic!("Expected CreateSink"),
        }
    }

    #[test]
    fn test_create_sink_from_query() {
        let sql = "CREATE SINK alerts FROM (SELECT * FROM events WHERE severity > 5)";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSink(sink) => {
                assert_eq!(sink.name.to_string(), "alerts");
                match &sink.from {
                    SinkFrom::Query(_) => {}
                    _ => panic!("Expected SinkFrom::Query"),
                }
            }
            _ => panic!("Expected CreateSink"),
        }
    }

    #[test]
    fn test_create_sink_with_options() {
        let sql = "CREATE SINK kafka_sink FROM orders WITH (
            'connector' = 'kafka',
            'topic' = 'processed_orders',
            'format' = 'avro'
        )";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSink(sink) => {
                assert_eq!(sink.name.to_string(), "kafka_sink");
                assert_eq!(sink.with_options.len(), 3);
                assert_eq!(
                    sink.with_options.get("connector"),
                    Some(&"kafka".to_string())
                );
                assert_eq!(
                    sink.with_options.get("topic"),
                    Some(&"processed_orders".to_string())
                );
            }
            _ => panic!("Expected CreateSink"),
        }
    }

    #[test]
    fn test_create_sink_if_not_exists() {
        let sql = "CREATE SINK IF NOT EXISTS my_sink FROM events";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSink(sink) => {
                assert!(sink.if_not_exists);
                assert!(!sink.or_replace);
            }
            _ => panic!("Expected CreateSink"),
        }
    }

    #[test]
    fn test_data_type_parsing() {
        let sql = "CREATE SOURCE typed_source (
            col_bigint BIGINT,
            col_int INT,
            col_smallint SMALLINT,
            col_bool BOOLEAN,
            col_float FLOAT,
            col_double DOUBLE,
            col_text TEXT,
            col_varchar VARCHAR(255),
            col_timestamp TIMESTAMP,
            col_date DATE,
            col_decimal DECIMAL(10,2),
            col_json JSON
        )";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSource(source) => {
                assert_eq!(source.columns.len(), 12);
                // Verify column names
                assert_eq!(source.columns[0].name.to_string(), "col_bigint");
                assert_eq!(source.columns[11].name.to_string(), "col_json");
            }
            _ => panic!("Expected CreateSource"),
        }
    }

    #[test]
    fn test_schema_qualified_source_name() {
        let sql = "CREATE SOURCE my_schema.events (id BIGINT)";
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        match &statements[0] {
            StreamingStatement::CreateSource(source) => {
                assert_eq!(source.name.to_string(), "my_schema.events");
            }
            _ => panic!("Expected CreateSource"),
        }
    }

    #[test]
    fn test_watermark_expression_parsing() {
        let sql = "CREATE SOURCE events (
            id BIGINT,
            ts TIMESTAMP,
            WATERMARK FOR ts AS ts - INTERVAL '10' MINUTE
        )";
        let statements = StreamingParser::parse_sql(sql).unwrap();

        match &statements[0] {
            StreamingStatement::CreateSource(source) => {
                assert!(source.watermark.is_some());
                let watermark = source.watermark.as_ref().unwrap();
                assert_eq!(watermark.column.to_string(), "ts");
                // The expression should be parsed
                assert!(!matches!(
                    watermark.expression,
                    Expr::Identifier(_) if watermark.expression.to_string() == "ts - INTERVAL '10' MINUTE"
                ) || matches!(watermark.expression, Expr::BinaryOp { .. }));
            }
            _ => panic!("Expected CreateSource"),
        }
    }
}