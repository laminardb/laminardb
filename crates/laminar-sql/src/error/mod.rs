//! Error translation layer for DataFusion errors.
//!
//! Translates internal DataFusion/Arrow error messages into user-friendly
//! LaminarDB errors with structured error codes (`LDB-NNNN`) and hints.
//!
//! # Error Code Ranges
//!
//! | Range | Category |
//! |-------|----------|
//! | `LDB-1001`..`LDB-1099` | SQL syntax errors |
//! | `LDB-1100`..`LDB-1199` | Schema / column errors |
//! | `LDB-1200`..`LDB-1299` | Type errors |
//! | `LDB-2000`..`LDB-2099` | Window / watermark errors |
//! | `LDB-3000`..`LDB-3099` | Join errors |
//! | `LDB-9000`..`LDB-9099` | Internal errors |

pub mod suggest;

use suggest::closest_match;

/// Structured error code constants.
pub mod codes {
    /// Unsupported SQL syntax.
    pub const UNSUPPORTED_SQL: &str = "LDB-1001";
    /// Query planning failed.
    pub const PLANNING_FAILED: &str = "LDB-1002";
    /// Column not found.
    pub const COLUMN_NOT_FOUND: &str = "LDB-1100";
    /// Table or source not found.
    pub const TABLE_NOT_FOUND: &str = "LDB-1101";
    /// Type mismatch.
    pub const TYPE_MISMATCH: &str = "LDB-1200";
    /// Internal query error (unrecognized pattern).
    pub const INTERNAL: &str = "LDB-9000";
    /// Query execution failed.
    pub const EXECUTION_FAILED: &str = "LDB-9001";
}

/// A translated error with structured code, message, and optional hint.
#[derive(Debug, Clone)]
pub struct TranslatedError {
    /// Structured error code (e.g. `"LDB-1100"`).
    pub code: &'static str,
    /// User-friendly error message.
    pub message: String,
    /// Optional hint for fixing the error.
    pub hint: Option<String>,
}

impl std::fmt::Display for TranslatedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)?;
        if let Some(hint) = &self.hint {
            write!(f, " (hint: {hint})")?;
        }
        Ok(())
    }
}

/// Suggests a column name correction based on edit distance.
///
/// Returns a `"Did you mean '...'?"` string if a close match is found
/// within 2 edits.
#[must_use]
pub fn suggest_column(input: &str, available: &[&str]) -> Option<String> {
    closest_match(input, available, 2).map(|m| format!("Did you mean '{m}'?"))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Strips known DataFusion/Arrow prefixes from an error message.
fn sanitize(msg: &str) -> &str {
    const PREFIXES: &[&str] = &[
        "DataFusion error: ",
        "Arrow error: ",
        "Schema error: ",
        "External error: ",
    ];
    let mut s = msg;
    for prefix in PREFIXES {
        if let Some(rest) = s.strip_prefix(prefix) {
            s = rest;
        }
    }
    s
}

/// Extracts a quoted name from a `"No field named ..."` message.
fn extract_missing_column(msg: &str) -> Option<&str> {
    let needle = "No field named ";
    let idx = msg.find(needle)?;
    let after = &msg[idx + needle.len()..];
    extract_quoted(after)
}

/// Extracts a table name from a `"table '...' not found"` message.
fn extract_missing_table(msg: &str) -> Option<&str> {
    // DataFusion uses lowercase "table" in its messages
    let lower = msg.to_ascii_lowercase();
    let needle = "table '";
    let idx = lower.find(needle)?;
    let after = &msg[idx + needle.len()..];
    after.find('\'').map(|end| &after[..end])
}

/// Extracts content from single or double quotes at the start of a string.
fn extract_quoted(s: &str) -> Option<&str> {
    if let Some(rest) = s.strip_prefix('\'') {
        rest.find('\'').map(|end| &rest[..end])
    } else if let Some(rest) = s.strip_prefix('"') {
        rest.find('"').map(|end| &rest[..end])
    } else {
        // Bare word — up to whitespace or punctuation
        let end = s.find(|c: char| c.is_whitespace() || c == ',' || c == ')');
        let word = match end {
            Some(i) => &s[..i],
            None => s,
        };
        if word.is_empty() {
            None
        } else {
            Some(word)
        }
    }
}

/// Translates a DataFusion error message into a user-friendly [`TranslatedError`].
///
/// Pattern-matches known DataFusion error formats and rewrites them with
/// structured error codes and helpful messages. Unrecognized patterns fall
/// back to `LDB-9000` with the message sanitized (internal prefixes stripped).
#[must_use]
pub fn translate_datafusion_error(msg: &str) -> TranslatedError {
    let clean = sanitize(msg);

    // Column not found
    if let Some(col) = extract_missing_column(clean) {
        return TranslatedError {
            code: codes::COLUMN_NOT_FOUND,
            message: format!("Column '{col}' not found in query"),
            hint: None,
        };
    }

    // Table not found
    if let Some(table) = extract_missing_table(clean) {
        return TranslatedError {
            code: codes::TABLE_NOT_FOUND,
            message: format!("Table or source '{table}' not found"),
            hint: Some(
                "Use SHOW TABLES to see available sources and tables".to_string(),
            ),
        };
    }

    // Type mismatch
    if clean.contains("mismatch")
        || clean.contains("must match")
        || clean.contains("cannot be cast")
    {
        return TranslatedError {
            code: codes::TYPE_MISMATCH,
            message: format!("Type mismatch: {clean}"),
            hint: Some("Check column types with DESCRIBE <table>".to_string()),
        };
    }

    // Unsupported / not implemented
    if clean.contains("Unsupported")
        || clean.contains("NotImplemented")
        || clean.contains("This feature is not implemented")
    {
        return TranslatedError {
            code: codes::UNSUPPORTED_SQL,
            message: format!("Unsupported SQL syntax: {clean}"),
            hint: None,
        };
    }

    // Planning error — Plan("...")
    if clean.starts_with("Plan(\"") {
        let detail = clean
            .strip_prefix("Plan(\"")
            .and_then(|s| s.strip_suffix("\")"))
            .unwrap_or(clean);
        return TranslatedError {
            code: codes::PLANNING_FAILED,
            message: format!("Query planning failed: {detail}"),
            hint: None,
        };
    }

    // Planning error — "Error during planning"
    if clean.contains("Error during planning") {
        return TranslatedError {
            code: codes::PLANNING_FAILED,
            message: format!("Query planning failed: {clean}"),
            hint: None,
        };
    }

    // Execution error
    if clean.contains("Execution error") {
        return TranslatedError {
            code: codes::EXECUTION_FAILED,
            message: format!("Query execution failed: {clean}"),
            hint: None,
        };
    }

    // Fallback — unknown pattern
    TranslatedError {
        code: codes::INTERNAL,
        message: format!("Internal query error: {clean}"),
        hint: Some("If this persists, file a bug report".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- translate_datafusion_error tests --

    #[test]
    fn test_column_not_found_single_quotes() {
        let t = translate_datafusion_error("No field named 'foo'");
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.message.contains("foo"));
        assert!(t.message.contains("not found"));
        assert!(!t.message.contains("DataFusion"));
    }

    #[test]
    fn test_column_not_found_double_quotes() {
        let t = translate_datafusion_error("No field named \"bar\"");
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.message.contains("bar"));
    }

    #[test]
    fn test_column_not_found_with_prefix() {
        let t = translate_datafusion_error("Schema error: No field named 'baz'");
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.message.contains("baz"));
    }

    #[test]
    fn test_table_not_found() {
        let t = translate_datafusion_error("table 'orders' not found");
        assert_eq!(t.code, codes::TABLE_NOT_FOUND);
        assert!(t.message.contains("orders"));
        assert!(t.hint.is_some());
        assert!(t.hint.unwrap().contains("SHOW TABLES"));
    }

    #[test]
    fn test_type_mismatch() {
        let t = translate_datafusion_error("column types must match for UNION");
        assert_eq!(t.code, codes::TYPE_MISMATCH);
        assert!(t.hint.is_some());
        assert!(t.hint.unwrap().contains("DESCRIBE"));
    }

    #[test]
    fn test_type_cannot_cast() {
        let t = translate_datafusion_error("cannot be cast to Int64");
        assert_eq!(t.code, codes::TYPE_MISMATCH);
    }

    #[test]
    fn test_unsupported_sql() {
        let t = translate_datafusion_error("This feature is not implemented: LATERAL JOIN");
        assert_eq!(t.code, codes::UNSUPPORTED_SQL);
        assert!(t.message.contains("LATERAL JOIN"));
    }

    #[test]
    fn test_plan_error_with_column_extracts_column() {
        // When a Plan error wraps a "No field named" message, the more
        // specific column-not-found code is preferred over generic planning.
        let t =
            translate_datafusion_error("Plan(\"No field named 'x' in schema\")");
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.message.contains("'x'"));
    }

    #[test]
    fn test_plan_error_generic() {
        let t =
            translate_datafusion_error("Plan(\"aggregate function not found\")");
        assert_eq!(t.code, codes::PLANNING_FAILED);
        assert!(t.message.contains("aggregate function not found"));
    }

    #[test]
    fn test_error_during_planning() {
        let t =
            translate_datafusion_error("Error during planning: ambiguous reference 'id'");
        assert_eq!(t.code, codes::PLANNING_FAILED);
    }

    #[test]
    fn test_execution_error() {
        let t = translate_datafusion_error("Execution error: division by zero");
        assert_eq!(t.code, codes::EXECUTION_FAILED);
    }

    #[test]
    fn test_unknown_fallback() {
        let t = translate_datafusion_error("some totally unknown error");
        assert_eq!(t.code, codes::INTERNAL);
        assert!(t.message.contains("Internal query error"));
        assert!(t.hint.is_some());
    }

    #[test]
    fn test_prefix_stripping() {
        let t = translate_datafusion_error(
            "DataFusion error: Arrow error: No field named 'x'",
        );
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.message.contains("'x'"));
        assert!(!t.message.contains("DataFusion"));
        assert!(!t.message.contains("Arrow error"));
    }

    #[test]
    fn test_display_with_hint() {
        let t = translate_datafusion_error("table 'foo' not found");
        let display = t.to_string();
        assert!(display.starts_with("[LDB-1101]"));
        assert!(display.contains("(hint:"));
    }

    #[test]
    fn test_display_without_hint() {
        let t = translate_datafusion_error("Execution error: oops");
        let display = t.to_string();
        assert!(display.starts_with("[LDB-9001]"));
        assert!(!display.contains("hint"));
    }

    // -- suggest_column tests --

    #[test]
    fn test_suggest_column_found() {
        let result = suggest_column("user_ie", &["user_id", "email"]);
        assert_eq!(result, Some("Did you mean 'user_id'?".to_string()));
    }

    #[test]
    fn test_suggest_column_none() {
        let result = suggest_column("xyz", &["user_id", "email"]);
        assert_eq!(result, None);
    }
}
