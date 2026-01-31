//! SQL utility functions for multi-statement parsing and config variable substitution.

use std::collections::HashMap;

use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::error::DbError;

/// Build a byte-offset index for each line in `sql`.
///
/// Returns a `Vec` where entry `i` is the byte offset of the start of
/// 1-based line `i+1`. Line 1 always starts at byte 0.
fn build_line_starts(sql: &str) -> Vec<usize> {
    let mut starts = vec![0usize];
    for (i, b) in sql.bytes().enumerate() {
        if b == b'\n' {
            starts.push(i + 1);
        }
    }
    starts
}

/// Convert a 1-based `(line, column)` from sqlparser's `Location` to a byte
/// offset in `sql`.
///
/// sqlparser's `column` counts *characters* (via `Peekable<Chars>`), not
/// bytes, so we walk with `char_indices()` for multi-byte correctness.
fn location_to_byte_offset(sql: &str, line_starts: &[usize], line: u64, column: u64) -> usize {
    let line_idx = usize::try_from(line).unwrap_or(1).saturating_sub(1);
    let line_start = line_starts.get(line_idx).copied().unwrap_or(0);
    let col_chars = usize::try_from(column).unwrap_or(1).saturating_sub(1); // 1-based → 0-based

    // Walk `col_chars` characters from `line_start` to find the byte offset.
    sql[line_start..]
        .char_indices()
        .nth(col_chars)
        .map_or(sql.len(), |(byte_off, _)| line_start + byte_off)
}

/// Split a SQL string into individual statements on unquoted semicolons.
///
/// Uses the sqlparser tokenizer to correctly handle quoted strings,
/// single-line comments (`--`), and block comments (`/* ... */`).
/// Empty statements (whitespace/comments only) are skipped.
pub fn split_statements(sql: &str) -> Vec<&str> {
    let dialect = GenericDialect {};
    // On tokenizer failure, return the whole string as one statement
    // so that the downstream parser can produce a proper error.
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize_with_location() else {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        return vec![trimmed];
    };

    let line_starts = build_line_starts(sql);
    let mut statements = Vec::new();
    let mut seg_start: usize = 0;
    let mut has_significant = false;

    for tws in &tokens {
        if tws.token == Token::SemiColon {
            if has_significant {
                let end =
                    location_to_byte_offset(sql, &line_starts, tws.span.start.line, tws.span.start.column);
                let stmt = sql[seg_start..end].trim();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
            }
            let after_semi =
                location_to_byte_offset(sql, &line_starts, tws.span.end.line, tws.span.end.column);
            seg_start = after_semi;
            has_significant = false;
        } else if !matches!(tws.token, Token::Whitespace(_) | Token::EOF) {
            has_significant = true;
        }
    }

    // Trailing segment (no semicolon)
    if has_significant {
        let stmt = sql[seg_start..].trim();
        if !stmt.is_empty() {
            statements.push(stmt);
        }
    }

    statements
}

/// Resolve `${VAR_NAME}` placeholders in a SQL string with values from the given map.
///
/// # Errors
///
/// Returns `DbError::InvalidOperation` if a referenced variable is not found
/// and `strict` is true. In permissive mode (strict=false), unresolved
/// variables are left as-is.
pub fn resolve_config_vars(
    sql: &str,
    vars: &HashMap<String, String>,
    strict: bool,
) -> Result<String, DbError> {
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        if bytes[i] == b'$' && i + 1 < len && bytes[i + 1] == b'{' {
            // Found ${
            let start = i;
            i += 2; // skip ${
            let var_start = i;

            // Find closing }
            while i < len && bytes[i] != b'}' {
                i += 1;
            }

            if i < len {
                let var_name = &sql[var_start..i];
                i += 1; // skip }

                if let Some(value) = vars.get(var_name) {
                    result.push_str(value);
                } else if strict {
                    return Err(DbError::InvalidOperation(format!(
                        "Unresolved config variable: ${{{var_name}}}"
                    )));
                } else {
                    // Permissive: leave as-is
                    result.push_str(&sql[start..i]);
                }
            } else {
                // No closing }, copy literal
                result.push_str(&sql[start..]);
            }
        } else {
            result.push(sql[i..].chars().next().unwrap());
            i += sql[i..].chars().next().unwrap().len_utf8();
        }
    }

    Ok(result)
}

// -- SQL value extraction helpers ----------------------------------------

/// Extract a string representation from a SQL expression.
///
/// Handles literals, quoted strings, numbers, booleans, NULL, and
/// unary minus (negative numbers).
pub fn expr_to_string(expr: Option<&sqlparser::ast::Expr>) -> Option<String> {
    use sqlparser::ast::{Expr, UnaryOperator, Value};

    let expr = expr?;
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Some(s.clone())
            }
            Value::Number(n, _) => Some(n.clone()),
            Value::Boolean(b) => Some(b.to_string()),
            Value::Null => None,
            other => Some(format!("{other}")),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => expr_to_string(Some(expr)).map(|s| format!("-{s}")),
        _ => None,
    }
}

/// Extract an `i64` from a SQL expression.
pub fn expr_to_i64(expr: Option<&sqlparser::ast::Expr>) -> Option<i64> {
    use sqlparser::ast::{Expr, UnaryOperator, Value};

    let expr = expr?;
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => expr_to_i64(Some(expr)).map(|v| -v),
        _ => None,
    }
}

/// Extract an `f64` from a SQL expression.
pub fn expr_to_f64(expr: Option<&sqlparser::ast::Expr>) -> Option<f64> {
    use sqlparser::ast::{Expr, UnaryOperator, Value};

    let expr = expr?;
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => expr_to_f64(Some(expr)).map(|v| -v),
        _ => None,
    }
}

/// Extract a `bool` from a SQL expression.
pub fn expr_to_bool(expr: Option<&sqlparser::ast::Expr>) -> Option<bool> {
    use sqlparser::ast::{Expr, Value};

    let expr = expr?;
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Boolean(b) => Some(*b),
            _ => None,
        },
        _ => None,
    }
}

/// Convert SQL `VALUES (...)` rows into an Arrow `RecordBatch`.
///
/// Each inner `Vec<Expr>` is one row. Columns are matched positionally
/// against the provided `schema`.
///
/// # Errors
///
/// Returns `DbError::InsertError` if the batch cannot be constructed
/// (e.g. column count mismatch).
#[allow(clippy::cast_possible_truncation)] // SQL literal values converted to Arrow numeric types
pub fn sql_values_to_record_batch(
    schema: &arrow::datatypes::SchemaRef,
    values: &[Vec<sqlparser::ast::Expr>],
) -> Result<arrow::array::RecordBatch, DbError> {
    use arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow::datatypes::DataType;

    let mut columns: Vec<std::sync::Arc<dyn Array>> =
        Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Boolean => {
                let arr: BooleanArray = values
                    .iter()
                    .map(|row| expr_to_bool(row.get(col_idx)))
                    .collect();
                columns.push(std::sync::Arc::new(arr));
            }
            DataType::Int8 => {
                let arr: Int8Array = values
                    .iter()
                    .map(|row| expr_to_i64(row.get(col_idx)).map(|v| v as i8))
                    .collect();
                columns.push(std::sync::Arc::new(arr));
            }
            DataType::Int16 => {
                let arr: Int16Array = values
                    .iter()
                    .map(|row| expr_to_i64(row.get(col_idx)).map(|v| v as i16))
                    .collect();
                columns.push(std::sync::Arc::new(arr));
            }
            DataType::Int32 => {
                let arr: Int32Array = values
                    .iter()
                    .map(|row| expr_to_i64(row.get(col_idx)).map(|v| v as i32))
                    .collect();
                columns.push(std::sync::Arc::new(arr));
            }
            DataType::Int64 => {
                let arr: Int64Array = values
                    .iter()
                    .map(|row| expr_to_i64(row.get(col_idx)))
                    .collect();
                columns.push(std::sync::Arc::new(arr));
            }
            DataType::Float32 => {
                let arr: Float32Array = values
                    .iter()
                    .map(|row| expr_to_f64(row.get(col_idx)).map(|v| v as f32))
                    .collect();
                columns.push(std::sync::Arc::new(arr));
            }
            DataType::Float64 => {
                let arr: Float64Array = values
                    .iter()
                    .map(|row| expr_to_f64(row.get(col_idx)))
                    .collect();
                columns.push(std::sync::Arc::new(arr));
            }
            _ => {
                // For Utf8 and any other type, convert to string
                let strs: Vec<Option<String>> = values
                    .iter()
                    .map(|row| expr_to_string(row.get(col_idx)))
                    .collect();
                let arr: StringArray =
                    strs.iter().map(|s| s.as_deref()).collect();
                columns.push(std::sync::Arc::new(arr));
            }
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DbError::InsertError(format!("Failed to create RecordBatch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── split_statements tests ──────────────────────────────────────

    #[test]
    fn test_single_statement() {
        let stmts = split_statements("SELECT 1");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn test_multiple_statements() {
        let stmts = split_statements(
            "CREATE SOURCE a (id INT); CREATE SOURCE b (id INT); CREATE SINK c FROM a",
        );
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "CREATE SOURCE a (id INT)");
        assert_eq!(stmts[1], "CREATE SOURCE b (id INT)");
        assert_eq!(stmts[2], "CREATE SINK c FROM a");
    }

    #[test]
    fn test_semicolons_in_strings() {
        let stmts = split_statements("SELECT 'hello; world'; SELECT 1");
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 'hello; world'");
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn test_trailing_semicolon() {
        let stmts = split_statements("SELECT 1;");
        assert_eq!(stmts, vec!["SELECT 1"]);
    }

    #[test]
    fn test_empty_statements() {
        let stmts = split_statements("SELECT 1; ; ; SELECT 2");
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_comments_between_statements() {
        let stmts = split_statements(
            "SELECT 1;\n-- this is a comment\nSELECT 2",
        );
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_comment_only_segment() {
        let stmts = split_statements("-- just a comment");
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_double_quoted_identifiers() {
        let stmts = split_statements(r#"SELECT "col;name" FROM t; SELECT 2"#);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_block_comment_with_semicolon() {
        // The old hand-rolled splitter couldn't handle /* ; */ — this is
        // the primary motivation for switching to the tokenizer.
        let stmts = split_statements("SELECT /* ; */ 1; SELECT 2");
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT /* ; */ 1");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_block_comment_only_segment() {
        let stmts = split_statements("/* just a block comment */");
        assert!(stmts.is_empty());
    }

    // ── resolve_config_vars tests ───────────────────────────────────

    #[test]
    fn test_basic_substitution() {
        let mut vars = HashMap::new();
        vars.insert("KAFKA_BROKERS".to_string(), "localhost:9092".to_string());
        let result =
            resolve_config_vars("'bootstrap.servers' = '${KAFKA_BROKERS}'", &vars, true)
                .unwrap();
        assert_eq!(result, "'bootstrap.servers' = 'localhost:9092'");
    }

    #[test]
    fn test_multiple_vars() {
        let mut vars = HashMap::new();
        vars.insert("HOST".to_string(), "localhost".to_string());
        vars.insert("PORT".to_string(), "9092".to_string());
        let result = resolve_config_vars("${HOST}:${PORT}", &vars, true).unwrap();
        assert_eq!(result, "localhost:9092");
    }

    #[test]
    fn test_missing_var_strict() {
        let vars = HashMap::new();
        let result = resolve_config_vars("${MISSING}", &vars, true);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unresolved config variable"));
    }

    #[test]
    fn test_missing_var_permissive() {
        let vars = HashMap::new();
        let result = resolve_config_vars("${MISSING}", &vars, false).unwrap();
        assert_eq!(result, "${MISSING}");
    }

    #[test]
    fn test_no_vars_in_sql() {
        let vars = HashMap::new();
        let result = resolve_config_vars("SELECT 1", &vars, true).unwrap();
        assert_eq!(result, "SELECT 1");
    }

    #[test]
    fn test_unclosed_var() {
        let vars = HashMap::new();
        let result = resolve_config_vars("${UNCLOSED", &vars, false).unwrap();
        assert_eq!(result, "${UNCLOSED");
    }
}
