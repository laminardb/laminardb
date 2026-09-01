use super::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn parse_stmt(sql: &str) -> Statement {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
    stmts.remove(0)
}

#[test]
fn test_lag_basic() {
    let sql = "SELECT price, LAG(price) OVER (ORDER BY ts) AS prev_price FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(analysis.functions.len(), 1);
    assert_eq!(
        analysis.functions[0].function_type,
        AnalyticFunctionType::Lag
    );
    assert_eq!(analysis.functions[0].column, "price");
    assert_eq!(analysis.functions[0].offset, 1);
    assert_eq!(analysis.functions[0].alias.as_deref(), Some("prev_price"));
}

#[test]
fn test_lag_with_offset() {
    let sql = "SELECT LAG(price, 3) OVER (ORDER BY ts) AS prev3 FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(analysis.functions[0].offset, 3);
}

#[test]
fn test_lag_with_default() {
    let sql = "SELECT LAG(price, 1, 0) OVER (ORDER BY ts) AS prev FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(analysis.functions[0].offset, 1);
    assert_eq!(analysis.functions[0].default_value.as_deref(), Some("0"));
}

#[test]
fn test_lead_basic() {
    let sql = "SELECT LEAD(price) OVER (ORDER BY ts) AS next_price FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(
        analysis.functions[0].function_type,
        AnalyticFunctionType::Lead
    );
    assert!(analysis.has_lookahead());
}

#[test]
fn test_lead_with_offset_and_default() {
    let sql = "SELECT LEAD(price, 2, -1) OVER (ORDER BY ts) AS next2 FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(analysis.functions[0].offset, 2);
    assert_eq!(analysis.functions[0].default_value.as_deref(), Some("-1"));
}

#[test]
fn test_partition_by_extraction() {
    let sql = "SELECT symbol, LAG(price) OVER (PARTITION BY symbol ORDER BY ts) FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(analysis.partition_columns, vec!["symbol".to_string()]);
    assert_eq!(analysis.order_columns, vec!["ts".to_string()]);
}

#[test]
fn test_multiple_analytic_functions() {
    let sql = "SELECT
            LAG(price) OVER (ORDER BY ts) AS prev,
            LEAD(price) OVER (ORDER BY ts) AS next
            FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(analysis.functions.len(), 2);
    assert_eq!(
        analysis.functions[0].function_type,
        AnalyticFunctionType::Lag
    );
    assert_eq!(
        analysis.functions[1].function_type,
        AnalyticFunctionType::Lead
    );
}

#[test]
fn test_first_value() {
    let sql =
        "SELECT FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY ts) AS first FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(
        analysis.functions[0].function_type,
        AnalyticFunctionType::FirstValue
    );
    assert_eq!(analysis.functions[0].column, "price");
}

#[test]
fn test_last_value() {
    let sql = "SELECT LAST_VALUE(price) OVER (ORDER BY ts) FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(
        analysis.functions[0].function_type,
        AnalyticFunctionType::LastValue
    );
}

#[test]
fn test_no_analytic_functions() {
    let sql = "SELECT price, volume FROM trades WHERE price > 100";
    let stmt = parse_stmt(sql);
    assert!(analyze_analytic_functions(&stmt).is_none());
}

#[test]
fn test_max_offset() {
    let sql = "SELECT
            LAG(price, 1) OVER (ORDER BY ts) AS p1,
            LAG(price, 5) OVER (ORDER BY ts) AS p5,
            LEAD(price, 3) OVER (ORDER BY ts) AS n3
            FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_analytic_functions(&stmt).unwrap();
    assert_eq!(analysis.max_offset(), 5);
}

// --- Window Frame tests ---

#[test]
fn test_frame_rows_preceding_current() {
    let sql = "SELECT AVG(price) OVER (ORDER BY ts \
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma \
                    FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(analysis.functions.len(), 1);
    assert_eq!(
        analysis.functions[0].function_type,
        WindowFrameFunction::Avg
    );
    assert_eq!(analysis.functions[0].column, "price");
    assert_eq!(analysis.functions[0].units, FrameUnits::Rows);
    assert_eq!(analysis.functions[0].start_bound, FrameBound::Preceding(9));
    assert_eq!(analysis.functions[0].end_bound, FrameBound::CurrentRow);
    assert_eq!(analysis.functions[0].alias.as_deref(), Some("ma"));
}

#[test]
fn test_frame_rows_preceding_following() {
    let sql = "SELECT SUM(amount) OVER (ORDER BY id \
                    ROWS BETWEEN 5 PRECEDING AND 3 FOLLOWING) AS total \
                    FROM orders";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(
        analysis.functions[0].function_type,
        WindowFrameFunction::Sum
    );
    assert_eq!(analysis.functions[0].start_bound, FrameBound::Preceding(5));
    assert_eq!(analysis.functions[0].end_bound, FrameBound::Following(3));
}

#[test]
fn test_frame_unbounded_preceding_running_sum() {
    let sql = "SELECT SUM(amount) OVER (ORDER BY id \
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running \
                    FROM orders";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(
        analysis.functions[0].start_bound,
        FrameBound::UnboundedPreceding
    );
    assert_eq!(analysis.functions[0].end_bound, FrameBound::CurrentRow);
}

#[test]
fn test_frame_range_units() {
    let sql = "SELECT AVG(price) OVER (ORDER BY ts \
                    RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS ra \
                    FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(analysis.functions[0].units, FrameUnits::Range);
    assert_eq!(analysis.functions[0].start_bound, FrameBound::Preceding(10));
}

#[test]
fn test_frame_partition_order_columns() {
    let sql = "SELECT AVG(price) OVER (PARTITION BY symbol ORDER BY ts \
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma \
                    FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(analysis.partition_columns, vec!["symbol".to_string()]);
    assert_eq!(analysis.order_columns, vec!["ts".to_string()]);
}

#[test]
fn test_frame_multiple_functions() {
    let sql = "SELECT \
                    AVG(price) OVER (ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma, \
                    SUM(volume) OVER (ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sv \
                    FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(analysis.functions.len(), 2);
    assert_eq!(
        analysis.functions[0].function_type,
        WindowFrameFunction::Avg
    );
    assert_eq!(analysis.functions[0].column, "price");
    assert_eq!(
        analysis.functions[1].function_type,
        WindowFrameFunction::Sum
    );
    assert_eq!(analysis.functions[1].column, "volume");
}

#[test]
fn test_frame_no_frame_returns_none() {
    // AVG with OVER but no explicit frame → None
    let sql = "SELECT AVG(price) OVER (ORDER BY ts) FROM trades";
    let stmt = parse_stmt(sql);
    assert!(analyze_window_frames(&stmt).is_none());
}

#[test]
fn test_frame_unbounded_following() {
    let sql = "SELECT SUM(amount) OVER (ORDER BY id \
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS rest \
                    FROM orders";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(analysis.functions[0].start_bound, FrameBound::CurrentRow);
    assert_eq!(
        analysis.functions[0].end_bound,
        FrameBound::UnboundedFollowing
    );
    assert!(analysis.has_following());
}

#[test]
fn test_frame_all_function_types() {
    let sql = "SELECT \
                    AVG(a) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f1, \
                    SUM(b) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f2, \
                    MIN(c) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f3, \
                    MAX(d) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f4, \
                    COUNT(e) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS f5 \
                    FROM t";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(analysis.functions.len(), 5);
    assert_eq!(
        analysis.functions[0].function_type,
        WindowFrameFunction::Avg
    );
    assert_eq!(
        analysis.functions[1].function_type,
        WindowFrameFunction::Sum
    );
    assert_eq!(
        analysis.functions[2].function_type,
        WindowFrameFunction::Min
    );
    assert_eq!(
        analysis.functions[3].function_type,
        WindowFrameFunction::Max
    );
    assert_eq!(
        analysis.functions[4].function_type,
        WindowFrameFunction::Count
    );
}

#[test]
fn test_frame_corr_bivariate() {
    let sql = "SELECT CORR(price, sentiment) OVER (ORDER BY bucket \
                    ROWS 30 PRECEDING) AS c FROM joined";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(
        analysis.functions[0].function_type,
        WindowFrameFunction::Corr
    );
    assert_eq!(analysis.functions[0].start_bound, FrameBound::Preceding(30));
    assert_eq!(analysis.order_columns, vec!["bucket".to_string()]);
}

#[test]
fn test_frame_max_preceding_helper() {
    let sql = "SELECT \
                    AVG(a) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS f1, \
                    SUM(b) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS f2 \
                    FROM t";
    let stmt = parse_stmt(sql);
    let analysis = analyze_window_frames(&stmt).unwrap();
    assert_eq!(analysis.max_preceding(), 10);
    assert!(!analysis.has_following());
}
