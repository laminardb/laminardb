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

use suggest::{closest_match, resolve_column_name};

/// Structured error code constants.
///
/// Codes in the `LDB-1xxx`..`LDB-3xxx` ranges are re-exported from the
/// canonical registry in `laminar_core::error_codes`. The `LDB-9xxx` codes
/// are SQL-layer specific and defined here.
pub mod codes {
    // Re-export LDB-1xxx (SQL parsing & validation) from laminar-core.
    pub use laminar_core::error_codes::SQL_COLUMN_NOT_FOUND as COLUMN_NOT_FOUND;
    pub use laminar_core::error_codes::SQL_PLANNING_FAILED as PLANNING_FAILED;
    pub use laminar_core::error_codes::SQL_TABLE_NOT_FOUND as TABLE_NOT_FOUND;
    pub use laminar_core::error_codes::SQL_TYPE_MISMATCH as TYPE_MISMATCH;
    pub use laminar_core::error_codes::SQL_UNSUPPORTED as UNSUPPORTED_SQL;

    // Re-export LDB-2xxx (window / watermark) from laminar-core.
    pub use laminar_core::error_codes::LATE_DATA_REJECTED;
    pub use laminar_core::error_codes::WATERMARK_REQUIRED;
    pub use laminar_core::error_codes::WINDOW_INVALID;
    pub use laminar_core::error_codes::WINDOW_SIZE_INVALID;

    // Re-export LDB-3xxx (join) from laminar-core.
    pub use laminar_core::error_codes::JOIN_KEY_MISSING;
    pub use laminar_core::error_codes::JOIN_TIME_BOUND_MISSING;
    pub use laminar_core::error_codes::JOIN_TYPE_UNSUPPORTED;
    pub use laminar_core::error_codes::TEMPORAL_JOIN_NO_PK;

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
        // Bare word — up to whitespace or punctuation, then strip
        // sentence-ending period (DataFusion 52.x: "No field named col.")
        let end = s.find(|c: char| c.is_whitespace() || c == ',' || c == ')');
        let word = match end {
            Some(i) => &s[..i],
            None => s,
        };
        let word = word.strip_suffix('.').unwrap_or(word);
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
        let hint = available_columns.and_then(|cols| match resolve_column_name(col, cols) {
            Ok(actual) if actual != col => Some(format!(
                "Column is named '{actual}' (case differs). \
                         Use \"{actual}\" or match the exact casing."
            )),
            Err(suggest::ColumnResolveError::Ambiguous { matches, .. }) => Some(format!(
                "Multiple columns match case-insensitively: {}. \
                         Use double quotes for exact match.",
                matches.join(", ")
            )),
            _ => suggest_column(col, cols),
        });
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
mod tests;
