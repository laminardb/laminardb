//! Parse-only validator for the demo SQL file.
//!
//! Splits the input on `;`, hands each non-empty statement to
//! `laminar_sql::parser::parse_streaming_sql`, prints one line per
//! statement, exits non-zero on the first parse error.

use std::process::ExitCode;

fn main() -> ExitCode {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "../demo.sql".to_string());

    let sql = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot read {path}: {e}");
            return ExitCode::from(2);
        }
    };

    let mut failures = 0usize;
    let mut total = 0usize;
    for (idx, stmt) in split_statements(&sql).into_iter().enumerate() {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        total += 1;
        let preview: String = trimmed
            .lines()
            .find(|l| !l.trim_start().starts_with("--") && !l.trim().is_empty())
            .unwrap_or(trimmed)
            .chars()
            .take(80)
            .collect();
        match laminar_sql::parser::parse_streaming_sql(trimmed) {
            Ok(stmts) => {
                println!(
                    "[{:>2}] OK   ({} parsed) {}",
                    idx,
                    stmts.len(),
                    preview
                );
            }
            Err(e) => {
                failures += 1;
                println!("[{:>2}] FAIL          {}", idx, preview);
                eprintln!("     └── {e}");
            }
        }
    }

    println!(
        "\n{} statement(s) parsed, {} failure(s)",
        total, failures
    );

    if failures == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

/// Split a SQL file on `;` while ignoring `;` inside single-quoted strings
/// and inside `--` line comments. Block comments `/* … */` are not used in
/// the demo so they are not handled.
fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut in_squote = false;
    let mut in_line_comment = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if in_line_comment {
            if c == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_squote {
            if c == b'\'' {
                // Embedded '' is an escape; treat as still in quote.
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_squote = false;
            }
            i += 1;
            continue;
        }
        if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 2;
            continue;
        }
        if c == b'\'' {
            in_squote = true;
            i += 1;
            continue;
        }
        if c == b';' {
            out.push(&sql[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    if start < bytes.len() {
        out.push(&sql[start..]);
    }
    out
}
