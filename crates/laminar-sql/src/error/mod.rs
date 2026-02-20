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

    // Window / watermark errors (2000-2099)
    /// Watermark required for this operation.
    pub const WATERMARK_REQUIRED: &str = "LDB-2001";
    /// Invalid window specification.
    pub const WINDOW_INVALID: &str = "LDB-2002";
    /// Window size must be positive.
    pub const WINDOW_SIZE_INVALID: &str = "LDB-2003";
    /// Late data rejected by window policy.
    pub const LATE_DATA_REJECTED: &str = "LDB-2004";

    // Join errors (3000-3099)
    /// Join key column not found or invalid.
    pub const JOIN_KEY_MISSING: &str = "LDB-3001";
    /// Time bound required for stream-stream join.
    pub const JOIN_TIME_BOUND_MISSING: &str = "LDB-3002";
    /// Temporal join requires a primary key on the right-side table.
    pub const TEMPORAL_JOIN_NO_PK: &str = "LDB-3003";
    /// Unsupported join type for streaming queries.
    pub const JOIN_TYPE_UNSUPPORTED: &str = "LDB-3004";

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
///
/// When `available_columns` is provided, column-not-found errors include a
/// "Did you mean '...'?" hint based on edit distance.
#[must_use]
pub fn translate_datafusion_error(msg: &str) -> TranslatedError {
    translate_datafusion_error_with_context(msg, None)
}

/// Like [`translate_datafusion_error`] but accepts an optional list of
/// available column names for typo suggestions.
#[must_use]
pub fn translate_datafusion_error_with_context(
    msg: &str,
    available_columns: Option<&[&str]>,
) -> TranslatedError {
    let clean = sanitize(msg);

    // Column not found
    if let Some(col) = extract_missing_column(clean) {
        let hint = available_columns.and_then(|cols| suggest_column(col, cols));
        return TranslatedError {
            code: codes::COLUMN_NOT_FOUND,
            message: format!("Column '{col}' not found in query"),
            hint,
        };
    }

    // Table not found
    if let Some(table) = extract_missing_table(clean) {
        return TranslatedError {
            code: codes::TABLE_NOT_FOUND,
            message: format!("Table or source '{table}' not found"),
            hint: Some("Use SHOW TABLES to see available sources and tables".to_string()),
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

    // Window / watermark errors
    if let Some(translated) = check_window_errors(clean) {
        return translated;
    }

    // Join errors
    if let Some(translated) = check_join_errors(clean) {
        return translated;
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

/// Check for window/watermark-related error patterns.
fn check_window_errors(clean: &str) -> Option<TranslatedError> {
    let lower = clean.to_ascii_lowercase();

    // "Window error:" prefix from parser — classify as WINDOW_INVALID
    if lower.starts_with("window error:") {
        return Some(TranslatedError {
            code: codes::WINDOW_INVALID,
            message: format!("Invalid window specification: {clean}"),
            hint: Some("Supported window types: TUMBLE, HOP, SESSION, CUMULATE".to_string()),
        });
    }

    if lower.contains("watermark") && (lower.contains("required") || lower.contains("missing")) {
        return Some(TranslatedError {
            code: codes::WATERMARK_REQUIRED,
            message: format!("Watermark required: {clean}"),
            hint: Some(
                "Add WATERMARK FOR <column> AS <column> - INTERVAL '<n>' SECOND \
                 to the CREATE SOURCE statement"
                    .to_string(),
            ),
        });
    }

    if lower.contains("window") && (lower.contains("invalid") || lower.contains("not supported")) {
        return Some(TranslatedError {
            code: codes::WINDOW_INVALID,
            message: format!("Invalid window specification: {clean}"),
            hint: Some("Supported window types: TUMBLE, HOP, SESSION, CUMULATE".to_string()),
        });
    }

    if lower.contains("window")
        && lower.contains("size")
        && (lower.contains("zero") || lower.contains("negative") || lower.contains("positive"))
    {
        return Some(TranslatedError {
            code: codes::WINDOW_SIZE_INVALID,
            message: format!("Invalid window size: {clean}"),
            hint: Some("Window size must be a positive interval".to_string()),
        });
    }

    // Late data rejected/dropped
    if lower.contains("late")
        && (lower.contains("data") || lower.contains("event"))
        && (lower.contains("rejected") || lower.contains("dropped"))
    {
        return Some(TranslatedError {
            code: codes::LATE_DATA_REJECTED,
            message: format!("Late data rejected: {clean}"),
            hint: Some(
                "Increase the allowed lateness with ALLOWED LATENESS INTERVAL, \
                 or route late data to a side output"
                    .to_string(),
            ),
        });
    }

    None
}

/// Check for join-related error patterns.
fn check_join_errors(clean: &str) -> Option<TranslatedError> {
    let lower = clean.to_ascii_lowercase();

    // "Streaming SQL error:" prefix — classify sub-patterns
    if lower.starts_with("streaming sql error:") {
        if lower.contains("using clause requires") {
            return Some(TranslatedError {
                code: codes::JOIN_KEY_MISSING,
                message: format!("Join key error: {clean}"),
                hint: Some(
                    "Ensure the USING clause references columns that exist \
                     in both sides of the join"
                        .to_string(),
                ),
            });
        }
        if lower.contains("cannot extract time bound") || lower.contains("tolerance") {
            return Some(TranslatedError {
                code: codes::JOIN_TIME_BOUND_MISSING,
                message: format!("Join time bound required: {clean}"),
                hint: Some(
                    "Stream-stream joins require a time bound in the ON clause, e.g.: \
                     AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '1' HOUR"
                        .to_string(),
                ),
            });
        }
    }

    if lower.contains("join") && lower.contains("key") && lower.contains("not found") {
        return Some(TranslatedError {
            code: codes::JOIN_KEY_MISSING,
            message: format!("Join key error: {clean}"),
            hint: Some(
                "Ensure the ON clause references columns that exist \
                 in both sides of the join"
                    .to_string(),
            ),
        });
    }

    if lower.contains("join")
        && (lower.contains("time bound") || lower.contains("interval"))
        && lower.contains("required")
    {
        return Some(TranslatedError {
            code: codes::JOIN_TIME_BOUND_MISSING,
            message: format!("Join time bound required: {clean}"),
            hint: Some(
                "Stream-stream joins require a time bound in the ON clause, e.g.: \
                 AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '1' HOUR"
                    .to_string(),
            ),
        });
    }

    if lower.contains("temporal") && lower.contains("primary key") {
        return Some(TranslatedError {
            code: codes::TEMPORAL_JOIN_NO_PK,
            message: format!("Temporal join error: {clean}"),
            hint: Some(
                "The right-side table of a temporal join requires a PRIMARY KEY".to_string(),
            ),
        });
    }

    // Unsupported join types for streaming
    if (lower.contains("not supported for streaming")
        || lower.contains("natural join not supported")
        || lower.contains("cross join not supported")
        || lower.contains("unsupported join"))
        && lower.contains("join")
    {
        return Some(TranslatedError {
            code: codes::JOIN_TYPE_UNSUPPORTED,
            message: format!("Unsupported join type: {clean}"),
            hint: Some(
                "Streaming queries support INNER, LEFT, RIGHT, and FULL OUTER joins \
                 with time bounds. CROSS and NATURAL joins are not supported."
                    .to_string(),
            ),
        });
    }

    None
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
        let t = translate_datafusion_error("Plan(\"No field named 'x' in schema\")");
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.message.contains("'x'"));
    }

    #[test]
    fn test_plan_error_generic() {
        let t = translate_datafusion_error("Plan(\"aggregate function not found\")");
        assert_eq!(t.code, codes::PLANNING_FAILED);
        assert!(t.message.contains("aggregate function not found"));
    }

    #[test]
    fn test_error_during_planning() {
        let t = translate_datafusion_error("Error during planning: ambiguous reference 'id'");
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
        let t = translate_datafusion_error("DataFusion error: Arrow error: No field named 'x'");
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

    // -- translate_datafusion_error_with_context tests --

    #[test]
    fn test_column_not_found_with_suggestion() {
        let cols = &["user_id", "email", "price"];
        let t = translate_datafusion_error_with_context("No field named 'user_ie'", Some(cols));
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.message.contains("user_ie"));
        assert!(t.hint.is_some());
        assert!(
            t.hint.as_ref().unwrap().contains("user_id"),
            "hint should suggest 'user_id': {:?}",
            t.hint
        );
    }

    #[test]
    fn test_column_not_found_no_close_match() {
        let cols = &["user_id", "email"];
        let t = translate_datafusion_error_with_context("No field named 'zzzzz'", Some(cols));
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.hint.is_none());
    }

    #[test]
    fn test_column_not_found_without_context() {
        let t = translate_datafusion_error("No field named 'foo'");
        assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
        assert!(t.hint.is_none()); // no columns provided
    }

    // -- window error code tests --

    #[test]
    fn test_watermark_required_error() {
        let t = translate_datafusion_error("Watermark required for EMIT ON WINDOW CLOSE");
        assert_eq!(t.code, codes::WATERMARK_REQUIRED);
        assert!(t.hint.is_some());
        assert!(t.hint.unwrap().contains("WATERMARK FOR"));
    }

    #[test]
    fn test_window_invalid_error() {
        let t = translate_datafusion_error("Window type not supported for this operation");
        assert_eq!(t.code, codes::WINDOW_INVALID);
        assert!(t.hint.unwrap().contains("TUMBLE"));
    }

    // -- join error code tests --

    #[test]
    fn test_join_key_not_found_error() {
        let t = translate_datafusion_error("Join key 'user_id' not found in right table");
        assert_eq!(t.code, codes::JOIN_KEY_MISSING);
        assert!(t.hint.unwrap().contains("ON clause"));
    }

    #[test]
    fn test_temporal_join_pk_error() {
        let t = translate_datafusion_error("Temporal join requires a primary key on right table");
        assert_eq!(t.code, codes::TEMPORAL_JOIN_NO_PK);
        assert!(t.hint.unwrap().contains("PRIMARY KEY"));
    }

    // -- LDB-2004 LATE_DATA_REJECTED tests --

    #[test]
    fn test_late_data_rejected() {
        let t = translate_datafusion_error("late data rejected by window policy");
        assert_eq!(t.code, codes::LATE_DATA_REJECTED);
        assert!(t.hint.unwrap().contains("lateness"));
    }

    #[test]
    fn test_late_event_dropped() {
        let t = translate_datafusion_error("late event dropped after window close");
        assert_eq!(t.code, codes::LATE_DATA_REJECTED);
    }

    // -- "Window error:" prefix test --

    #[test]
    fn test_window_error_prefix() {
        let t = translate_datafusion_error("Window error: CUMULATE requires step <= size");
        assert_eq!(t.code, codes::WINDOW_INVALID);
        assert!(t.hint.unwrap().contains("CUMULATE"));
    }

    // -- LDB-3004 JOIN_TYPE_UNSUPPORTED tests --

    #[test]
    fn test_join_type_unsupported_cross() {
        let t = translate_datafusion_error("cross join not supported for streaming queries");
        assert_eq!(t.code, codes::JOIN_TYPE_UNSUPPORTED);
        assert!(t.hint.unwrap().contains("CROSS"));
    }

    #[test]
    fn test_join_type_unsupported_natural() {
        let t = translate_datafusion_error("natural join not supported in streaming context");
        assert_eq!(t.code, codes::JOIN_TYPE_UNSUPPORTED);
    }

    // -- "Streaming SQL error:" prefix tests --

    #[test]
    fn test_streaming_sql_error_using_clause() {
        let t = translate_datafusion_error(
            "Streaming SQL error: using clause requires matching columns",
        );
        assert_eq!(t.code, codes::JOIN_KEY_MISSING);
    }

    #[test]
    fn test_streaming_sql_error_time_bound() {
        let t = translate_datafusion_error(
            "Streaming SQL error: cannot extract time bound from ON clause",
        );
        assert_eq!(t.code, codes::JOIN_TIME_BOUND_MISSING);
    }
}
