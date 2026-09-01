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
                        ..
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
                        ..
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
                assert!(err.to_string().contains("2-3 arguments"));
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
                assert!(err.to_string().contains("3-4 arguments"));
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
    let sql = "SELECT CUMULATE(order_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) FROM orders";
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

#[test]
fn test_millisecond_interval() {
    // parse_interval_to_duration should handle MILLISECOND unit
    let sql = "SELECT TUMBLE(ts, INTERVAL '500' MILLISECOND) FROM events";
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
                        time_column: _,
                        interval,
                        ..
                    } => {
                        let duration =
                            WindowRewriter::parse_interval_to_duration(&interval).unwrap();
                        assert_eq!(
                            duration,
                            std::time::Duration::from_millis(500),
                            "INTERVAL '500' MILLISECOND should parse to 500ms"
                        );
                    }
                    _ => panic!("Expected Tumble window"),
                }
            }
        }
    }
}

#[test]
fn test_millisecond_interval_string() {
    // parse_interval_string should handle MS unit
    let duration = WindowRewriter::parse_interval_string("250 MS").unwrap();
    assert_eq!(duration, std::time::Duration::from_millis(250));

    let duration2 = WindowRewriter::parse_interval_string("100 MILLISECONDS").unwrap();
    assert_eq!(duration2, std::time::Duration::from_millis(100));

    let duration3 = WindowRewriter::parse_interval_string("750 MILLISECOND").unwrap();
    assert_eq!(duration3, std::time::Duration::from_millis(750));
}

#[test]
fn test_parse_tumble_with_offset() {
    let sql = "SELECT TUMBLE(ts, INTERVAL '1' HOUR, INTERVAL '30' MINUTE) FROM events";
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
                        interval, offset, ..
                    } => {
                        let dur = WindowRewriter::parse_interval_to_duration(&interval).unwrap();
                        assert_eq!(dur, std::time::Duration::from_secs(3600));
                        assert!(offset.is_some(), "Expected offset to be set");
                        let off_dur =
                            WindowRewriter::parse_interval_to_duration(offset.as_ref().unwrap())
                                .unwrap();
                        assert_eq!(off_dur, std::time::Duration::from_secs(1800));
                    }
                    _ => panic!("Expected Tumble window"),
                }
            }
        }
    }
}

#[test]
fn test_parse_hop_with_offset() {
    let sql = "SELECT HOP(ts, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE, INTERVAL '2' MINUTE) FROM events";
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
                        slide_interval,
                        window_interval,
                        offset,
                        ..
                    } => {
                        let slide =
                            WindowRewriter::parse_interval_to_duration(&slide_interval).unwrap();
                        let size =
                            WindowRewriter::parse_interval_to_duration(&window_interval).unwrap();
                        assert_eq!(slide, std::time::Duration::from_secs(300));
                        assert_eq!(size, std::time::Duration::from_secs(900));
                        assert!(offset.is_some(), "Expected offset to be set");
                        let off_dur =
                            WindowRewriter::parse_interval_to_duration(offset.as_ref().unwrap())
                                .unwrap();
                        assert_eq!(off_dur, std::time::Duration::from_secs(120));
                    }
                    _ => panic!("Expected Hop window"),
                }
            }
        }
    }
}
