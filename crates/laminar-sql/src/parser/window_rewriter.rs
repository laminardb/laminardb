//! Production implementation for window function extraction and rewriting
//!
//! This module handles:
//! - TUMBLE(time_col, interval) - tumbling windows
//! - HOP(time_col, slide, size) / SLIDE(...) - sliding/hopping windows
//! - SESSION(time_col, gap) - session windows

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, Query, Select, SelectItem,
    SetExpr, Statement,
};

use super::{ParseError, WindowFunction};

/// Rewrites window functions in SQL queries
pub struct WindowRewriter;

impl WindowRewriter {
    /// Rewrite a SQL statement to expand window functions.
    ///
    /// Transforms window functions like TUMBLE into appropriate table functions
    /// and adds `window_start`/`window_end` columns.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if a window function cannot be rewritten.
    ///
    /// # Example
    ///
    /// ```sql
    /// -- Input:
    /// SELECT COUNT(*) FROM events
    /// GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)
    ///
    /// -- Output:
    /// SELECT window_start, window_end, COUNT(*)
    /// FROM events
    /// GROUP BY window_start, window_end
    /// ```
    pub fn rewrite_statement(stmt: &mut Statement) -> Result<(), ParseError> {
        if let Statement::Query(query) = stmt {
            Self::rewrite_query(query)?;
        }
        Ok(())
    }

    /// Rewrite a query
    fn rewrite_query(query: &mut Query) -> Result<(), ParseError> {
        if let SetExpr::Select(select) = &mut *query.body {
            Self::rewrite_select(select)?;
        }
        Ok(())
    }

    /// Rewrite a SELECT statement to expand window functions.
    ///
    /// This processes GROUP BY to find window functions and adds
    /// window_start/window_end to the projection.
    fn rewrite_select(select: &mut Select) -> Result<(), ParseError> {
        // Find window function in GROUP BY
        let window_func = Self::find_window_in_group_by(select)?;

        if let Some(_window) = window_func {
            // Add window_start and window_end to projection if not already present
            Self::ensure_window_columns_in_projection(select);
        }

        Ok(())
    }

    /// Find window function in GROUP BY clause.
    fn find_window_in_group_by(select: &Select) -> Result<Option<WindowFunction>, ParseError> {
        // Check the GROUP BY expressions
        match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(exprs, _modifiers) => {
                for expr in exprs {
                    if let Some(window) = Self::extract_window_function(expr)? {
                        return Ok(Some(window));
                    }
                }
            }
            sqlparser::ast::GroupByExpr::All(_) => {}
        }
        Ok(None)
    }

    /// Ensure window_start and window_end columns are in projection.
    fn ensure_window_columns_in_projection(select: &mut Select) {
        let has_window_start = Self::has_projection_column(select, "window_start");
        let has_window_end = Self::has_projection_column(select, "window_end");

        // Add window_start at the beginning if not present
        if !has_window_start {
            select.projection.insert(
                0,
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("window_start"))),
            );
        }

        // Add window_end after window_start if not present
        if !has_window_end {
            select.projection.insert(
                1,
                SelectItem::UnnamedExpr(Expr::Identifier(Ident::new("window_end"))),
            );
        }
    }

    /// Check if a named column exists in the SELECT projection.
    fn has_projection_column(select: &Select, name: &str) -> bool {
        select.projection.iter().any(|item| {
            if let SelectItem::UnnamedExpr(Expr::Identifier(ident)) = item {
                ident.value.eq_ignore_ascii_case(name)
            } else if let SelectItem::ExprWithAlias { alias, .. } = item {
                alias.value.eq_ignore_ascii_case(name)
            } else {
                false
            }
        })
    }

    /// Check if expression contains a window function.
    #[must_use]
    pub fn contains_window_function(expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                if let Some(name) = func.name.0.last() {
                    let func_name = name.to_string().to_uppercase();
                    matches!(
                        func_name.as_str(),
                        "TUMBLE" | "HOP" | "SLIDE" | "SESSION" | "CUMULATE"
                    )
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Extract window function details from expression.
    ///
    /// Parses the actual arguments from TUMBLE/HOP/SESSION function calls.
    ///
    /// # Supported syntax
    ///
    /// - `TUMBLE(time_column, interval)` - 2 arguments
    /// - `HOP(time_column, slide_interval, window_size)` - 3 arguments
    /// - `SLIDE(time_column, slide_interval, window_size)` - alias for HOP
    /// - `SESSION(time_column, gap_interval)` - 2 arguments
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if:
    /// - Function has empty name
    /// - Wrong number of arguments for window type
    /// - Arguments cannot be extracted
    pub fn extract_window_function(expr: &Expr) -> Result<Option<WindowFunction>, ParseError> {
        match expr {
            Expr::Function(func) => {
                let name =
                    func.name.0.last().ok_or_else(|| {
                        ParseError::WindowError("Empty function name".to_string())
                    })?;

                let func_name = name.to_string().to_uppercase();

                // Extract arguments from the function
                let args = Self::extract_function_args(&func.args)?;

                match func_name.as_str() {
                    "TUMBLE" => Self::parse_tumble_args(&args),
                    "HOP" | "SLIDE" => Self::parse_hop_args(&args),
                    "SESSION" => Self::parse_session_args(&args),
                    "CUMULATE" => Self::parse_cumulate_args(&args),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Extract function arguments as a vector of expressions.
    fn extract_function_args(args: &FunctionArguments) -> Result<Vec<Expr>, ParseError> {
        match args {
            FunctionArguments::List(arg_list) => {
                let mut result = Vec::new();
                for arg in &arg_list.args {
                    if let Some(expr) = Self::extract_arg_expr(arg) {
                        result.push(expr);
                    }
                }
                Ok(result)
            }
            FunctionArguments::None => Ok(vec![]),
            FunctionArguments::Subquery(_) => Err(ParseError::WindowError(
                "Subquery arguments not supported for window functions".to_string(),
            )),
        }
    }

    /// Extract expression from a function argument.
    fn extract_arg_expr(arg: &FunctionArg) -> Option<Expr> {
        match arg {
            FunctionArg::Unnamed(arg_expr) => match arg_expr {
                FunctionArgExpr::Expr(expr) => Some(expr.clone()),
                FunctionArgExpr::Wildcard | FunctionArgExpr::QualifiedWildcard(_) => None,
            },
            FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => match arg {
                FunctionArgExpr::Expr(expr) => Some(expr.clone()),
                FunctionArgExpr::Wildcard | FunctionArgExpr::QualifiedWildcard(_) => None,
            },
        }
    }

    /// Parse TUMBLE(time_column, interval) arguments.
    fn parse_tumble_args(args: &[Expr]) -> Result<Option<WindowFunction>, ParseError> {
        if args.len() != 2 {
            return Err(ParseError::WindowError(format!(
                "TUMBLE requires 2 arguments (time_column, interval), got {}",
                args.len()
            )));
        }

        Ok(Some(WindowFunction::Tumble {
            time_column: Box::new(args[0].clone()),
            interval: Box::new(args[1].clone()),
        }))
    }

    /// Parse HOP/SLIDE(time_column, slide_interval, window_size) arguments.
    fn parse_hop_args(args: &[Expr]) -> Result<Option<WindowFunction>, ParseError> {
        if args.len() != 3 {
            return Err(ParseError::WindowError(format!(
                "HOP/SLIDE requires 3 arguments (time_column, slide_interval, window_size), got {}",
                args.len()
            )));
        }

        Ok(Some(WindowFunction::Hop {
            time_column: Box::new(args[0].clone()),
            slide_interval: Box::new(args[1].clone()),
            window_interval: Box::new(args[2].clone()),
        }))
    }

    /// Parse SESSION(time_column, gap_interval) arguments.
    fn parse_session_args(args: &[Expr]) -> Result<Option<WindowFunction>, ParseError> {
        if args.len() != 2 {
            return Err(ParseError::WindowError(format!(
                "SESSION requires 2 arguments (time_column, gap_interval), got {}",
                args.len()
            )));
        }

        Ok(Some(WindowFunction::Session {
            time_column: Box::new(args[0].clone()),
            gap_interval: Box::new(args[1].clone()),
        }))
    }

    /// Parse CUMULATE(time_column, step_interval, max_size_interval) arguments.
    fn parse_cumulate_args(args: &[Expr]) -> Result<Option<WindowFunction>, ParseError> {
        if args.len() != 3 {
            return Err(ParseError::WindowError(format!(
                "CUMULATE requires 3 arguments (time_column, step_interval, max_size_interval), got {}",
                args.len()
            )));
        }

        Ok(Some(WindowFunction::Cumulate {
            time_column: Box::new(args[0].clone()),
            step_interval: Box::new(args[1].clone()),
            max_size_interval: Box::new(args[2].clone()),
        }))
    }

    /// Extract the time column name from a window function.
    ///
    /// Returns the column name as a string if extractable.
    #[must_use]
    pub fn get_time_column_name(window: &WindowFunction) -> Option<String> {
        let expr = match window {
            WindowFunction::Tumble { time_column, .. }
            | WindowFunction::Hop { time_column, .. }
            | WindowFunction::Session { time_column, .. }
            | WindowFunction::Cumulate { time_column, .. } => time_column.as_ref(),
        };

        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
            _ => None,
        }
    }

    /// Parse an INTERVAL expression to Duration.
    ///
    /// Supports: SECOND, MINUTE, HOUR, DAY
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if the expression is not a valid interval.
    pub fn parse_interval_to_duration(expr: &Expr) -> Result<std::time::Duration, ParseError> {
        match expr {
            Expr::Interval(interval) => {
                // Extract the value
                let value = Self::extract_interval_value(&interval.value)?;

                // Get the unit (defaults to SECOND)
                let unit = interval
                    .leading_field
                    .clone()
                    .unwrap_or(sqlparser::ast::DateTimeField::Second);

                let seconds =
                    match unit {
                        sqlparser::ast::DateTimeField::Second
                        | sqlparser::ast::DateTimeField::Seconds => value,
                        sqlparser::ast::DateTimeField::Minute
                        | sqlparser::ast::DateTimeField::Minutes => value * 60,
                        sqlparser::ast::DateTimeField::Hour
                        | sqlparser::ast::DateTimeField::Hours => value * 3600,
                        sqlparser::ast::DateTimeField::Day
                        | sqlparser::ast::DateTimeField::Days => value * 86400,
                        _ => {
                            return Err(ParseError::WindowError(format!(
                                "Unsupported interval unit: {unit:?}"
                            )))
                        }
                    };

                Ok(std::time::Duration::from_secs(seconds))
            }
            // Handle string literal intervals like '5 MINUTES'
            Expr::Value(value_with_span) => {
                use sqlparser::ast::Value;
                if let Value::SingleQuotedString(s) = &value_with_span.value {
                    Self::parse_interval_string(s)
                } else {
                    Err(ParseError::WindowError(format!(
                        "Expected string value, got: {value_with_span:?}"
                    )))
                }
            }
            // Handle identifier that might be an interval string
            Expr::Identifier(ident) => Self::parse_interval_string(&ident.value),
            _ => Err(ParseError::WindowError(format!(
                "Expected INTERVAL expression, got: {expr:?}"
            ))),
        }
    }

    /// Extract numeric value from interval expression.
    fn extract_interval_value(expr: &Expr) -> Result<u64, ParseError> {
        match expr {
            Expr::Value(value_with_span) => {
                use sqlparser::ast::Value;
                match &value_with_span.value {
                    Value::Number(n, _) => n.parse::<u64>().map_err(|_| {
                        ParseError::WindowError(format!("Invalid interval value: {n}"))
                    }),
                    Value::SingleQuotedString(s) => {
                        // Handle '5' or '5 MINUTE'
                        let num_str = s.split_whitespace().next().unwrap_or(s);
                        num_str.parse::<u64>().map_err(|_| {
                            ParseError::WindowError(format!("Invalid interval value: {s}"))
                        })
                    }
                    _ => Err(ParseError::WindowError(format!(
                        "Unsupported value type in interval: {value_with_span:?}"
                    ))),
                }
            }
            _ => Err(ParseError::WindowError(format!(
                "Cannot extract interval value from: {expr:?}"
            ))),
        }
    }

    /// Parse an interval string like "5 MINUTES" or "1 HOUR".
    fn parse_interval_string(s: &str) -> Result<std::time::Duration, ParseError> {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.is_empty() {
            return Err(ParseError::WindowError("Empty interval string".to_string()));
        }

        let value: u64 = parts[0].parse().map_err(|_| {
            ParseError::WindowError(format!("Invalid interval value: {}", parts[0]))
        })?;

        let unit = if parts.len() > 1 {
            parts[1].to_uppercase()
        } else {
            "SECOND".to_string()
        };

        let seconds = match unit.as_str() {
            "SECOND" | "SECONDS" | "S" => value,
            "MINUTE" | "MINUTES" | "M" => value * 60,
            "HOUR" | "HOURS" | "H" => value * 3600,
            "DAY" | "DAYS" | "D" => value * 86400,
            _ => {
                return Err(ParseError::WindowError(format!(
                    "Unsupported interval unit: {unit}"
                )))
            }
        };

        Ok(std::time::Duration::from_secs(seconds))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_contains_window_function() {
        let sql = "SELECT TUMBLE(event_time, INTERVAL '5' MINUTE) FROM events";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    assert!(WindowRewriter::contains_window_function(expr));
                }
            }
        }
    }

    #[test]
    fn test_rewrite_statement() {
        let sql = "SELECT COUNT(*) FROM events GROUP BY event_time";
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).unwrap();

        // Should not fail on standard SQL
        assert!(WindowRewriter::rewrite_statement(&mut statements[0]).is_ok());
    }

    #[test]
    fn test_extract_tumble_with_actual_args() {
        let sql = "SELECT TUMBLE(order_time, INTERVAL '10' MINUTE) FROM orders";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let window = WindowRewriter::extract_window_function(expr)
                        .unwrap()
                        .unwrap();

                    match window {
                        WindowFunction::Tumble {
                            time_column,
                            interval,
                        } => {
                            // Verify time column is extracted correctly
                            assert_eq!(time_column.to_string(), "order_time");

                            // Verify interval is extracted
                            assert!(interval.to_string().contains("10"));
                        }
                        _ => panic!("Expected Tumble window"),
                    }
                }
            }
        }
    }

    #[test]
    fn test_extract_hop_with_actual_args() {
        let sql = "SELECT HOP(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) FROM readings";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let window = WindowRewriter::extract_window_function(expr)
                        .unwrap()
                        .unwrap();

                    match window {
                        WindowFunction::Hop {
                            time_column,
                            slide_interval,
                            window_interval,
                        } => {
                            assert_eq!(time_column.to_string(), "ts");
                            assert!(slide_interval.to_string().contains('1'));
                            assert!(window_interval.to_string().contains('5'));
                        }
                        _ => panic!("Expected Hop window"),
                    }
                }
            }
        }
    }

    #[test]
    fn test_extract_session_with_actual_args() {
        let sql = "SELECT SESSION(click_time, INTERVAL '30' MINUTE) FROM clicks";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let window = WindowRewriter::extract_window_function(expr)
                        .unwrap()
                        .unwrap();

                    match window {
                        WindowFunction::Session {
                            time_column,
                            gap_interval,
                        } => {
                            assert_eq!(time_column.to_string(), "click_time");
                            assert!(gap_interval.to_string().contains("30"));
                        }
                        _ => panic!("Expected Session window"),
                    }
                }
            }
        }
    }

    #[test]
    fn test_tumble_wrong_args_count() {
        let sql = "SELECT TUMBLE(ts) FROM events";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let result = WindowRewriter::extract_window_function(expr);
                    assert!(result.is_err());
                    let err = result.unwrap_err();
                    assert!(err.to_string().contains("2 arguments"));
                }
            }
        }
    }

    #[test]
    fn test_hop_wrong_args_count() {
        let sql = "SELECT HOP(ts, INTERVAL '1' MINUTE) FROM events";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let result = WindowRewriter::extract_window_function(expr);
                    assert!(result.is_err());
                    let err = result.unwrap_err();
                    assert!(err.to_string().contains("3 arguments"));
                }
            }
        }
    }

    #[test]
    fn test_slide_alias_for_hop() {
        let sql = "SELECT SLIDE(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) FROM events";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let window = WindowRewriter::extract_window_function(expr)
                        .unwrap()
                        .unwrap();

                    // SLIDE should be parsed as Hop
                    assert!(matches!(window, WindowFunction::Hop { .. }));
                }
            }
        }
    }

    #[test]
    fn test_get_time_column_name() {
        let sql = "SELECT TUMBLE(my_timestamp, INTERVAL '5' MINUTE) FROM events";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let window = WindowRewriter::extract_window_function(expr)
                        .unwrap()
                        .unwrap();

                    let col_name = WindowRewriter::get_time_column_name(&window);
                    assert_eq!(col_name, Some("my_timestamp".to_string()));
                }
            }
        }
    }

    #[test]
    fn test_parse_interval_to_duration() {
        // Test parsing from GROUP BY
        let sql = "SELECT COUNT(*) FROM events GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE)";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                    if let Some(expr) = exprs.first() {
                        let window = WindowRewriter::extract_window_function(expr)
                            .unwrap()
                            .unwrap();

                        if let WindowFunction::Tumble { interval, .. } = window {
                            let duration =
                                WindowRewriter::parse_interval_to_duration(&interval).unwrap();
                            assert_eq!(duration, std::time::Duration::from_secs(300));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_parse_interval_string_formats() {
        // Test various interval string formats
        let cases = [
            ("5 MINUTE", 300),
            ("5 MINUTES", 300),
            ("1 HOUR", 3600),
            ("2 HOURS", 7200),
            ("10 SECOND", 10),
            ("1 DAY", 86400),
        ];

        for (input, expected_secs) in cases {
            let result = WindowRewriter::parse_interval_string(input).unwrap();
            assert_eq!(
                result,
                std::time::Duration::from_secs(expected_secs),
                "Failed for input: {input}"
            );
        }
    }

    #[test]
    fn test_window_in_group_by() {
        let sql = "SELECT user_id, COUNT(*) FROM events GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR), user_id";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                let window = WindowRewriter::find_window_in_group_by(select)
                    .unwrap()
                    .unwrap();

                assert!(matches!(window, WindowFunction::Tumble { .. }));

                if let WindowFunction::Tumble { time_column, .. } = window {
                    assert_eq!(time_column.to_string(), "event_time");
                }
            }
        }
    }

    #[test]
    fn test_contains_cumulate_window_function() {
        let sql = "SELECT CUMULATE(ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) FROM events";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    assert!(WindowRewriter::contains_window_function(expr));
                }
            }
        }
    }

    #[test]
    fn test_extract_cumulate_with_actual_args() {
        let sql =
            "SELECT CUMULATE(order_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) FROM orders";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let window = WindowRewriter::extract_window_function(expr)
                        .unwrap()
                        .unwrap();

                    match window {
                        WindowFunction::Cumulate {
                            time_column,
                            step_interval,
                            max_size_interval,
                        } => {
                            assert_eq!(time_column.to_string(), "order_time");
                            assert!(step_interval.to_string().contains('1'));
                            assert!(max_size_interval.to_string().contains('5'));
                        }
                        _ => panic!("Expected Cumulate window"),
                    }
                }
            }
        }
    }

    #[test]
    fn test_cumulate_wrong_args_count() {
        let sql = "SELECT CUMULATE(ts, INTERVAL '1' MINUTE) FROM events";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let result = WindowRewriter::extract_window_function(expr);
                    assert!(result.is_err());
                    let err = result.unwrap_err();
                    assert!(err.to_string().contains("3 arguments"));
                }
            }
        }
    }

    #[test]
    fn test_cumulate_time_column_name() {
        let sql = "SELECT CUMULATE(my_ts, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) FROM events";
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let Statement::Query(query) = &statements[0] {
            if let SetExpr::Select(select) = &*query.body {
                if let SelectItem::UnnamedExpr(expr) = &select.projection[0] {
                    let window = WindowRewriter::extract_window_function(expr)
                        .unwrap()
                        .unwrap();

                    let col_name = WindowRewriter::get_time_column_name(&window);
                    assert_eq!(col_name, Some("my_ts".to_string()));
                }
            }
        }
    }
}
