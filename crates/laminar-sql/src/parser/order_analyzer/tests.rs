use super::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn parse_stmt(sql: &str) -> Statement {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
    stmts.remove(0)
}

#[test]
fn test_analyze_simple_order_by() {
    let stmt = parse_stmt("SELECT id, value FROM events ORDER BY id");
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.order_columns.len(), 1);
    assert_eq!(analysis.order_columns[0].column, "id");
    assert!(!analysis.order_columns[0].descending);
    assert_eq!(analysis.pattern, OrderPattern::Unbounded);
}

#[test]
fn test_analyze_order_by_desc() {
    let stmt = parse_stmt("SELECT * FROM events ORDER BY price DESC");
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.order_columns.len(), 1);
    assert!(analysis.order_columns[0].descending);
}

#[test]
fn test_analyze_order_by_nulls_first() {
    let stmt = parse_stmt("SELECT * FROM events ORDER BY value ASC NULLS FIRST");
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.order_columns.len(), 1);
    assert!(!analysis.order_columns[0].descending);
    assert!(analysis.order_columns[0].nulls_first);
}

#[test]
fn test_analyze_order_by_multiple_columns() {
    let stmt = parse_stmt("SELECT * FROM events ORDER BY category ASC, price DESC NULLS LAST");
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.order_columns.len(), 2);
    assert_eq!(analysis.order_columns[0].column, "category");
    assert!(!analysis.order_columns[0].descending);
    assert_eq!(analysis.order_columns[1].column, "price");
    assert!(analysis.order_columns[1].descending);
}

#[test]
fn test_analyze_order_by_with_limit() {
    let stmt = parse_stmt("SELECT * FROM events ORDER BY price DESC LIMIT 10");
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.limit, Some(10));
    assert_eq!(analysis.pattern, OrderPattern::TopK { k: 10 });
}

#[test]
fn test_analyze_order_by_without_limit() {
    let stmt = parse_stmt("SELECT * FROM events ORDER BY id");
    let analysis = analyze_order_by(&stmt);
    assert!(analysis.limit.is_none());
    assert_eq!(analysis.pattern, OrderPattern::Unbounded);
    assert!(!analysis.is_streaming_safe());
}

#[test]
fn test_analyze_no_order_by() {
    let stmt = parse_stmt("SELECT * FROM events");
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.pattern, OrderPattern::None);
    assert!(analysis.order_columns.is_empty());
    assert!(analysis.is_streaming_safe());
}

#[test]
fn test_analyze_select_star() {
    let stmt = parse_stmt("SELECT * FROM events WHERE id > 5");
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.pattern, OrderPattern::None);
}

#[test]
fn test_detect_row_number_pattern() {
    let sql = "SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
            FROM trades
        ) sub WHERE rn <= 5";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);

    // Should detect per-group topk (rn <= 5, subquery pattern)
    assert_eq!(
        analysis.pattern,
        OrderPattern::PerGroupTopK {
            k: 5,
            partition_columns: vec!["category".to_string()],
            rank_type: RankType::RowNumber,
        }
    );
    assert!(analysis.is_streaming_safe());
}

#[test]
fn test_detect_row_number_with_partition() {
    let sql = "SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
            FROM trades
        ) sub WHERE rn <= 3 ORDER BY category LIMIT 100";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);

    // Should detect PerGroupTopK from the subquery pattern (k=3)
    assert_eq!(
        analysis.pattern,
        OrderPattern::PerGroupTopK {
            k: 3,
            partition_columns: vec!["category".to_string()],
            rank_type: RankType::RowNumber,
        }
    );
    assert!(analysis.is_streaming_safe());
}

#[test]
fn test_detect_row_number_without_filter() {
    let sql = "SELECT *, ROW_NUMBER() OVER (ORDER BY price DESC) AS rn FROM trades";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    // No ORDER BY on the outer query, no filter -> None pattern
    assert_eq!(analysis.pattern, OrderPattern::None);
}

// ── Ranking function tests ──────────────────────────────

#[test]
fn test_row_number_subquery_no_outer_order() {
    let sql = "SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts DESC) AS rn
            FROM trades
        ) sub WHERE rn <= 10";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    assert_eq!(
        analysis.pattern,
        OrderPattern::PerGroupTopK {
            k: 10,
            partition_columns: vec!["symbol".to_string()],
            rank_type: RankType::RowNumber,
        }
    );
    assert!(analysis.is_streaming_safe());
}

#[test]
fn test_row_number_direct_with_limit() {
    let sql = "SELECT *, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val DESC) AS rn
            FROM events LIMIT 5";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    assert_eq!(
        analysis.pattern,
        OrderPattern::PerGroupTopK {
            k: 5,
            partition_columns: vec!["cat".to_string()],
            rank_type: RankType::RowNumber,
        }
    );
}

#[test]
fn test_detect_rank_pattern() {
    let sql = "SELECT * FROM (
            SELECT *, RANK() OVER (PARTITION BY category ORDER BY price DESC) AS rn
            FROM trades
        ) sub WHERE rn <= 3";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    assert_eq!(
        analysis.pattern,
        OrderPattern::PerGroupTopK {
            k: 3,
            partition_columns: vec!["category".to_string()],
            rank_type: RankType::Rank,
        }
    );
    assert!(analysis.is_streaming_safe());
}

#[test]
fn test_detect_dense_rank_pattern() {
    let sql = "SELECT * FROM (
            SELECT *, DENSE_RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rn
            FROM sales
        ) sub WHERE rn <= 5";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    assert_eq!(
        analysis.pattern,
        OrderPattern::PerGroupTopK {
            k: 5,
            partition_columns: vec!["region".to_string()],
            rank_type: RankType::DenseRank,
        }
    );
}

#[test]
fn test_rank_multiple_partition_columns() {
    let sql = "SELECT * FROM (
            SELECT *, RANK() OVER (PARTITION BY region, category ORDER BY sales DESC) AS rn
            FROM revenue
        ) sub WHERE rn <= 3";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    match &analysis.pattern {
        OrderPattern::PerGroupTopK {
            k,
            partition_columns,
            rank_type,
        } => {
            assert_eq!(*k, 3);
            assert_eq!(
                partition_columns,
                &["region".to_string(), "category".to_string()]
            );
            assert_eq!(*rank_type, RankType::Rank);
        }
        _ => panic!("Expected PerGroupTopK, got {:?}", analysis.pattern),
    }
}

#[test]
fn test_rank_extracts_order_columns() {
    let sql = "SELECT *, RANK() OVER (PARTITION BY cat ORDER BY price DESC, ts ASC) AS rn
            FROM trades LIMIT 10";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    assert!(matches!(
        analysis.pattern,
        OrderPattern::PerGroupTopK {
            rank_type: RankType::Rank,
            ..
        }
    ));
}

#[test]
fn test_rank_pattern_is_streaming_safe() {
    let sql = "SELECT * FROM (
            SELECT *, DENSE_RANK() OVER (PARTITION BY cat ORDER BY val) AS rn
            FROM events
        ) sub WHERE rn <= 5";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    assert!(analysis.is_streaming_safe());
}

#[test]
fn test_no_ranking_function_none() {
    let sql = "SELECT id, name FROM events WHERE id > 5";
    let stmt = parse_stmt(sql);
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.pattern, OrderPattern::None);
}

#[test]
fn test_order_satisfied_exact_match() {
    use crate::datafusion::SortColumn;
    let required = vec![OrderColumn {
        column: "event_time".to_string(),
        descending: false,
        nulls_first: false,
    }];
    let source = vec![SortColumn::ascending("event_time")];
    assert!(is_order_satisfied(&required, &source));
}

#[test]
fn test_order_satisfied_prefix_match() {
    use crate::datafusion::SortColumn;
    let required = vec![OrderColumn {
        column: "event_time".to_string(),
        descending: false,
        nulls_first: false,
    }];
    let source = vec![
        SortColumn::ascending("event_time"),
        SortColumn::ascending("id"),
    ];
    assert!(is_order_satisfied(&required, &source));
}

#[test]
fn test_order_not_satisfied_different_direction() {
    use crate::datafusion::SortColumn;
    let required = vec![OrderColumn {
        column: "event_time".to_string(),
        descending: true,
        nulls_first: false,
    }];
    let source = vec![SortColumn::ascending("event_time")];
    assert!(!is_order_satisfied(&required, &source));
}

#[test]
fn test_order_not_satisfied_different_columns() {
    use crate::datafusion::SortColumn;
    let required = vec![OrderColumn {
        column: "id".to_string(),
        descending: false,
        nulls_first: false,
    }];
    let source = vec![SortColumn::ascending("event_time")];
    assert!(!is_order_satisfied(&required, &source));
}

#[test]
fn test_topk_pattern_streaming_safe() {
    let stmt = parse_stmt("SELECT * FROM trades ORDER BY price DESC LIMIT 5");
    let analysis = analyze_order_by(&stmt);
    assert!(analysis.is_streaming_safe());
    assert_eq!(analysis.pattern, OrderPattern::TopK { k: 5 });
}

#[test]
fn test_unbounded_pattern_not_streaming_safe() {
    let stmt = parse_stmt("SELECT * FROM trades ORDER BY price DESC");
    let analysis = analyze_order_by(&stmt);
    assert!(!analysis.is_streaming_safe());
    assert_eq!(analysis.pattern, OrderPattern::Unbounded);
}

#[test]
fn test_no_order_by_streaming_safe() {
    let stmt = parse_stmt("SELECT * FROM trades");
    let analysis = analyze_order_by(&stmt);
    assert!(analysis.is_streaming_safe());
}

#[test]
fn test_windowed_order_by() {
    let stmt = parse_stmt(
            "SELECT COUNT(*) FROM events GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE) ORDER BY event_time",
        );
    let analysis = analyze_order_by(&stmt);
    assert_eq!(analysis.pattern, OrderPattern::WindowLocal);
    assert!(analysis.is_windowed);
    assert!(analysis.is_streaming_safe());
}
