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
fn test_column_not_found_bare_word_with_trailing_period() {
    let t = translate_datafusion_error("No field named ref_price.");
    assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
    assert!(
        t.message.contains("'ref_price'"),
        "should strip trailing period: {}",
        t.message
    );
    assert!(
        !t.message.contains("ref_price."),
        "should not include trailing period: {}",
        t.message
    );
}

#[test]
fn test_column_not_found_bare_word_with_valid_fields() {
    let t = translate_datafusion_error(
        "Schema error: No field named ref_price. Valid fields: symbol, event_time",
    );
    assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
    assert!(
        t.message.contains("'ref_price'"),
        "should extract clean column name: {}",
        t.message
    );
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

#[test]
fn test_column_not_found_case_insensitive_hint() {
    let cols = &["tradeId", "symbol", "lastPrice"];
    let t = translate_datafusion_error_with_context("No field named 'tradeid'", Some(cols));
    assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
    assert!(t.hint.is_some());
    let hint = t.hint.unwrap();
    assert!(
        hint.contains("tradeId"),
        "hint should mention actual name: {hint}"
    );
    assert!(hint.contains("case"), "hint should mention case: {hint}");
}

#[test]
fn test_column_not_found_ambiguous_case_hint() {
    let cols = &["price", "Price", "PRICE"];
    let t = translate_datafusion_error_with_context("No field named 'pRiCe'", Some(cols));
    assert_eq!(t.code, codes::COLUMN_NOT_FOUND);
    assert!(t.hint.is_some());
    let hint = t.hint.unwrap();
    assert!(
        hint.contains("Multiple"),
        "hint should mention ambiguity: {hint}"
    );
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
    let t =
        translate_datafusion_error("Streaming SQL error: using clause requires matching columns");
    assert_eq!(t.code, codes::JOIN_KEY_MISSING);
}

#[test]
fn test_streaming_sql_error_time_bound() {
    let t =
        translate_datafusion_error("Streaming SQL error: cannot extract time bound from ON clause");
    assert_eq!(t.code, codes::JOIN_TIME_BOUND_MISSING);
}
