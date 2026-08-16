use super::*;
use sqlparser::ast::{Expr, Ident};

fn make_tumble_window() -> WindowFunction {
    // Create a simple tumble window for testing
    WindowFunction::Tumble {
        time_column: Box::new(Expr::Identifier(Ident::new("event_time"))),
        interval: Box::new(Expr::Identifier(Ident::new("5 MINUTE"))),
        offset: None,
    }
}

#[test]
fn test_tumbling_config() {
    let config = WindowOperatorConfig::tumbling("event_time".to_string(), Duration::from_secs(300));

    assert_eq!(config.window_type, WindowType::Tumbling);
    assert_eq!(config.time_column, "event_time");
    assert_eq!(config.size, Duration::from_secs(300));
    assert!(config.slide.is_none());
    assert!(config.gap.is_none());
}

#[test]
fn test_sliding_config() {
    let config = WindowOperatorConfig::sliding(
        "ts".to_string(),
        Duration::from_secs(300),
        Duration::from_secs(60),
    );

    assert_eq!(config.window_type, WindowType::Sliding);
    assert_eq!(config.size, Duration::from_secs(300));
    assert_eq!(config.slide, Some(Duration::from_secs(60)));
}

#[test]
fn test_session_config() {
    let config = WindowOperatorConfig::session("click_time".to_string(), Duration::from_secs(1800));

    assert_eq!(config.window_type, WindowType::Session);
    assert_eq!(config.gap, Some(Duration::from_secs(1800)));
}

#[test]
fn test_from_window_function() {
    let window = make_tumble_window();
    let config = WindowOperatorConfig::from_window_function(&window).unwrap();

    assert_eq!(config.window_type, WindowType::Tumbling);
    assert_eq!(config.time_column, "event_time");
    assert_eq!(config.size, Duration::from_secs(300));
}

#[test]
fn test_with_emit_clause() {
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

    let config = config.with_emit_clause(&EmitClause::OnWindowClose).unwrap();
    assert_eq!(config.emit_strategy, EmitStrategy::OnWindowClose);

    let config2 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));
    let config2 = config2.with_emit_clause(&EmitClause::Changes).unwrap();
    assert_eq!(config2.emit_strategy, EmitStrategy::Changelog);
}

#[test]
fn test_with_late_data_clause_side_output_rejected() {
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

    // Side output is not yet wired in pipeline mode — must be rejected
    let late_clause = LateDataClause::side_output_only("late_events".to_string());
    let result = config.with_late_data_clause(&late_clause);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("not yet supported"));
}

#[test]
fn test_with_late_data_clause_lateness_only_accepted() {
    use sqlparser::ast::Expr;

    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

    // Allowed lateness without side output is fine
    let late_clause = LateDataClause::with_allowed_lateness(Expr::Value(
        sqlparser::ast::Value::SingleQuotedString("5 SECONDS".to_string()).into(),
    ));
    let config = config.with_late_data_clause(&late_clause).unwrap();
    assert_eq!(config.allowed_lateness, Duration::from_secs(5));
    assert!(config.late_data_side_output.is_none());
}

#[test]
fn test_append_only_compatible() {
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

    // Default emit strategy (OnWatermark) is append-only compatible
    assert!(config.is_append_only_compatible());

    // OnUpdate is NOT append-only compatible (produces retractions)
    let config2 = config.with_emit_strategy(EmitStrategy::OnUpdate);
    assert!(!config2.is_append_only_compatible());

    // Changelog is NOT append-only compatible
    let config3 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
        .with_emit_strategy(EmitStrategy::Changelog);
    assert!(!config3.is_append_only_compatible());
}

#[test]
fn test_has_late_data_handling() {
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

    // No late data handling by default
    assert!(!config.has_late_data_handling());

    // With allowed lateness
    let config2 = config
        .clone()
        .with_allowed_lateness(Duration::from_secs(60));
    assert!(config2.has_late_data_handling());

    // With side output
    let config3 = config.with_late_data_side_output("late".to_string());
    assert!(config3.has_late_data_handling());
}

#[test]
fn test_eowc_without_watermark_errors() {
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
        .with_emit_strategy(EmitStrategy::OnWindowClose);

    let result = config.validate(false, true);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("watermark"),
        "Expected watermark error, got: {err}"
    );
}

#[test]
fn test_eowc_without_window_errors() {
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
        .with_emit_strategy(EmitStrategy::OnWindowClose);

    let result = config.validate(true, false);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("windowed"),
        "Expected windowed query error, got: {err}"
    );
}

#[test]
fn test_eowc_with_watermark_and_window_passes() {
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
        .with_emit_strategy(EmitStrategy::OnWindowClose);

    assert!(config.validate(true, true).is_ok());
}

#[test]
fn test_final_without_watermark_errors() {
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
        .with_emit_strategy(EmitStrategy::FinalOnly);

    let result = config.validate(false, true);
    assert!(result.is_err());
}

#[test]
fn test_non_eowc_without_watermark_ok() {
    // OnUpdate does not require watermark
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
        .with_emit_strategy(EmitStrategy::OnUpdate);
    assert!(config.validate(false, false).is_ok());

    // Periodic does not require watermark
    let config2 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
        .with_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(5)));
    assert!(config2.validate(false, false).is_ok());

    // Changelog does not require watermark
    let config3 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
        .with_emit_strategy(EmitStrategy::Changelog);
    assert!(config3.validate(false, false).is_ok());
}

#[test]
fn test_display_tumbling_window() {
    let config = WindowOperatorConfig::tumbling("event_time".to_string(), Duration::from_secs(60));
    assert_eq!(format!("{config}"), "TUMBLE(event_time, 1m)");
}

#[test]
fn test_display_sliding_window() {
    let config = WindowOperatorConfig::sliding(
        "ts".to_string(),
        Duration::from_secs(300),
        Duration::from_secs(60),
    );
    assert_eq!(format!("{config}"), "HOP(ts, 5m SLIDE 1m)");
}

#[test]
fn test_display_session_window() {
    let config = WindowOperatorConfig::session("click_time".to_string(), Duration::from_secs(1800));
    assert_eq!(format!("{config}"), "SESSION(click_time, GAP 30m)");
}

#[test]
fn test_cumulate_config() {
    let config = WindowOperatorConfig::cumulate(
        "ts".to_string(),
        Duration::from_secs(60),
        Duration::from_secs(300),
    );

    assert_eq!(config.window_type, WindowType::Cumulate);
    assert_eq!(config.time_column, "ts");
    assert_eq!(config.size, Duration::from_secs(300));
    assert_eq!(config.slide, Some(Duration::from_secs(60)));
    assert!(config.gap.is_none());
}

#[test]
fn test_cumulate_from_window_function() {
    let window = WindowFunction::Cumulate {
        time_column: Box::new(Expr::Identifier(Ident::new("event_time"))),
        step_interval: Box::new(Expr::Identifier(Ident::new("1 MINUTE"))),
        max_size_interval: Box::new(Expr::Identifier(Ident::new("5 MINUTE"))),
    };
    let config = WindowOperatorConfig::from_window_function(&window).unwrap();

    assert_eq!(config.window_type, WindowType::Cumulate);
    assert_eq!(config.time_column, "event_time");
    assert_eq!(config.size, Duration::from_secs(300));
    assert_eq!(config.slide, Some(Duration::from_secs(60)));
}

#[test]
fn test_display_cumulate_window() {
    let config = WindowOperatorConfig::cumulate(
        "ts".to_string(),
        Duration::from_secs(60),
        Duration::from_secs(300),
    );
    assert_eq!(format!("{config}"), "CUMULATE(ts, STEP 1m SIZE 5m)");
}

#[test]
fn test_display_window_type() {
    assert_eq!(format!("{}", WindowType::Tumbling), "TUMBLE");
    assert_eq!(format!("{}", WindowType::Sliding), "HOP");
    assert_eq!(format!("{}", WindowType::Session), "SESSION");
    assert_eq!(format!("{}", WindowType::Cumulate), "CUMULATE");
}

#[test]
fn test_display_duration_formatting() {
    // Hours
    let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(3600));
    assert_eq!(format!("{config}"), "TUMBLE(ts, 1h)");

    // Seconds (non-round minutes)
    let config2 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(45));
    assert_eq!(format!("{config2}"), "TUMBLE(ts, 45s)");

    // Milliseconds (sub-second)
    let config3 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_millis(500));
    assert_eq!(format!("{config3}"), "TUMBLE(ts, 500ms)");
}
