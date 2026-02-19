//! ORDER BY analysis for streaming SQL queries
//!
//! Extracts ORDER BY metadata from SQL AST, classifies streaming safety,
//! and rejects unsafe patterns (unbounded ORDER BY without LIMIT).

use sqlparser::ast::{Expr, OrderByKind, Query, SelectItem, SetExpr, Statement};

/// Result of analyzing ORDER BY in a SQL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderAnalysis {
    /// Columns specified in ORDER BY
    pub order_columns: Vec<OrderColumn>,
    /// LIMIT value if present
    pub limit: Option<usize>,
    /// Whether the query has a windowed GROUP BY
    pub is_windowed: bool,
    /// Classified streaming pattern
    pub pattern: OrderPattern,
}

/// A column referenced in ORDER BY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderColumn {
    /// Column name (simple identifier)
    pub column: String,
    /// Whether sorting is descending (false = ascending)
    pub descending: bool,
    /// Whether nulls sort first
    pub nulls_first: bool,
}

/// Classification of ORDER BY pattern for streaming safety.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderPattern {
    /// No ORDER BY present.
    None,
    /// Source already satisfies the ordering (elided by DataFusion).
    SourceSatisfied,
    /// ORDER BY ... LIMIT N — bounded top-K.
    TopK {
        /// Number of top entries to maintain
        k: usize,
    },
    /// ORDER BY inside a windowed aggregation — bounded by window.
    WindowLocal,
    /// ROW_NUMBER() / RANK() / DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...) WHERE rn <= N.
    PerGroupTopK {
        /// Per-partition limit
        k: usize,
        /// Partition key columns
        partition_columns: Vec<String>,
        /// Which ranking function was used
        rank_type: RankType,
    },
    /// Unbounded ORDER BY on an unbounded stream — rejected.
    Unbounded,
}

/// Type of ranking function used in a per-group top-K pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankType {
    /// ROW_NUMBER() — unique sequential ranking, no ties.
    RowNumber,
    /// RANK() — ties get the same rank, with gaps after ties.
    Rank,
    /// DENSE_RANK() — ties get the same rank, no gaps.
    DenseRank,
}

impl OrderAnalysis {
    /// Returns true if this ORDER BY pattern is safe for streaming.
    #[must_use]
    pub fn is_streaming_safe(&self) -> bool {
        !matches!(self.pattern, OrderPattern::Unbounded)
    }
}

/// Analyzes a SQL statement for ORDER BY patterns.
///
/// Extracts ORDER BY columns, detects LIMIT, checks for windowed context,
/// and classifies the pattern for streaming safety.
///
/// # Arguments
///
/// * `stmt` - The SQL statement to analyze
///
/// # Returns
///
/// An `OrderAnalysis` with the classified pattern.
#[must_use]
pub fn analyze_order_by(stmt: &Statement) -> OrderAnalysis {
    let Statement::Query(query) = stmt else {
        return OrderAnalysis {
            order_columns: vec![],
            limit: None,
            is_windowed: false,
            pattern: OrderPattern::None,
        };
    };

    let limit = extract_limit(query);
    let is_windowed = check_is_windowed(query);

    // Check for ROW_NUMBER()/RANK()/DENSE_RANK() OVER (...) WHERE rn <= N
    // Must run BEFORE the order_columns check: subquery patterns like
    // `SELECT * FROM (...ROW_NUMBER()...) WHERE rn <= 5` have no outer ORDER BY.
    if let Some((k, partition_columns, rank_type)) = detect_row_number_pattern(query) {
        let order_columns = extract_order_columns(query);
        return OrderAnalysis {
            order_columns,
            limit,
            is_windowed,
            pattern: OrderPattern::PerGroupTopK {
                k,
                partition_columns,
                rank_type,
            },
        };
    }

    let order_columns = extract_order_columns(query);
    if order_columns.is_empty() {
        return OrderAnalysis {
            order_columns: vec![],
            limit: None,
            is_windowed: false,
            pattern: OrderPattern::None,
        };
    }

    let pattern = if is_windowed {
        OrderPattern::WindowLocal
    } else if let Some(k) = limit {
        OrderPattern::TopK { k }
    } else {
        OrderPattern::Unbounded
    };

    OrderAnalysis {
        order_columns,
        limit,
        is_windowed,
        pattern,
    }
}

/// Checks whether a given ordering is satisfied by a source's declared ordering.
///
/// Returns true if `source_ordering` is a prefix match of `required_ordering`
/// (same columns, same direction).
#[must_use]
pub fn is_order_satisfied(
    required: &[OrderColumn],
    source: &[crate::datafusion::SortColumn],
) -> bool {
    if required.is_empty() {
        return true;
    }
    if source.len() < required.len() {
        return false;
    }
    required.iter().zip(source.iter()).all(|(req, src)| {
        req.column == src.name
            && req.descending == src.descending
            && req.nulls_first == src.nulls_first
    })
}

/// Extracts ORDER BY columns from a query.
fn extract_order_columns(query: &Query) -> Vec<OrderColumn> {
    let Some(order_by) = &query.order_by else {
        return vec![];
    };

    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        return vec![]; // ORDER BY ALL not supported for streaming
    };

    exprs
        .iter()
        .filter_map(|ob_expr| {
            let column = extract_column_name(&ob_expr.expr)?;
            let descending = !ob_expr.options.asc.unwrap_or(true);
            let nulls_first = ob_expr.options.nulls_first.unwrap_or(false);
            Some(OrderColumn {
                column,
                descending,
                nulls_first,
            })
        })
        .collect()
}

/// Extracts LIMIT value as usize if present.
fn extract_limit(query: &Query) -> Option<usize> {
    use sqlparser::ast::LimitClause;

    let limit_clause = query.limit_clause.as_ref()?;
    match limit_clause {
        LimitClause::LimitOffset { limit, .. } => {
            let expr = limit.as_ref()?;
            expr_to_usize(expr)
        }
        LimitClause::OffsetCommaLimit { limit, .. } => expr_to_usize(limit),
    }
}

/// Checks whether the query body has a windowed GROUP BY.
fn check_is_windowed(query: &Query) -> bool {
    if let SetExpr::Select(select) = query.body.as_ref() {
        use sqlparser::ast::GroupByExpr;
        match &select.group_by {
            GroupByExpr::Expressions(exprs, _modifiers) => {
                exprs.iter().any(is_window_function_call)
            }
            GroupByExpr::All(_) => false,
        }
    } else {
        false
    }
}

/// Detects ROW_NUMBER()/RANK()/DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...) WHERE rn <= N.
///
/// This is a simplified heuristic: it looks for a subquery in FROM with
/// a ranking function and a filter on the outer query. For Phase 1, we detect
/// common SQL patterns rather than doing full semantic analysis.
fn detect_row_number_pattern(query: &Query) -> Option<(usize, Vec<String>, RankType)> {
    // Look for ranking function in the SELECT items of the query body
    if let SetExpr::Select(select) = query.body.as_ref() {
        for item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                if let Some((partition_cols, _order_cols, rank_type)) =
                    extract_row_number_info(expr)
                {
                    // Look for a LIMIT to determine K
                    if let Some(k) = extract_limit(query) {
                        return Some((k, partition_cols, rank_type));
                    }
                }
            }
        }

        // Check if this is a subquery pattern: SELECT * FROM (SELECT ..., ROW_NUMBER() ...) WHERE rn <= N
        for from in &select.from {
            if let sqlparser::ast::TableFactor::Derived { subquery, .. } = &from.relation {
                if let SetExpr::Select(inner_select) = subquery.body.as_ref() {
                    for item in &inner_select.projection {
                        if let SelectItem::ExprWithAlias { expr, alias } = item {
                            if let Some((partition_cols, _order_cols, rank_type)) =
                                extract_row_number_info(expr)
                            {
                                // Found ranking function AS alias in subquery
                                // Check outer WHERE for alias <= N
                                if let Some(k) =
                                    extract_rn_filter_limit(select.selection.as_ref(), &alias.value)
                                {
                                    return Some((k, partition_cols, rank_type));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

/// Extracts ranking function info (partition cols, order cols, rank type) from an expression.
///
/// Recognizes ROW_NUMBER(), RANK(), and DENSE_RANK().
fn extract_row_number_info(expr: &Expr) -> Option<(Vec<String>, Vec<String>, RankType)> {
    if let Expr::Function(func) = expr {
        let name = func.name.to_string().to_uppercase();
        let rank_type = match name.as_str() {
            "ROW_NUMBER" => RankType::RowNumber,
            "RANK" => RankType::Rank,
            "DENSE_RANK" => RankType::DenseRank,
            _ => return None,
        };
        if let Some(ref window_spec) = func.over {
            match window_spec {
                sqlparser::ast::WindowType::WindowSpec(spec) => {
                    let partition_cols: Vec<String> = spec
                        .partition_by
                        .iter()
                        .filter_map(extract_column_name)
                        .collect();
                    let order_cols: Vec<String> = spec
                        .order_by
                        .iter()
                        .filter_map(|ob| extract_column_name(&ob.expr))
                        .collect();
                    return Some((partition_cols, order_cols, rank_type));
                }
                sqlparser::ast::WindowType::NamedWindow(_) => {}
            }
        }
    }
    None
}

/// Extracts a limit value from a WHERE clause like `alias <= N`.
fn extract_rn_filter_limit(selection: Option<&Expr>, alias: &str) -> Option<usize> {
    let where_expr = selection?;
    if let Expr::BinaryOp { left, op, right } = where_expr {
        use sqlparser::ast::BinaryOperator;
        match op {
            BinaryOperator::LtEq => {
                // rn <= N
                if extract_column_name(left)? == alias {
                    return expr_to_usize(right);
                }
            }
            BinaryOperator::Lt => {
                // rn < N -> k = N - 1
                if extract_column_name(left)? == alias {
                    return expr_to_usize(right).map(|n| n.saturating_sub(1));
                }
            }
            _ => {}
        }
    }
    None
}

/// Checks if an expression is a window function call (TUMBLE, HOP, SESSION).
fn is_window_function_call(expr: &Expr) -> bool {
    if let Expr::Function(func) = expr {
        let name = func.name.to_string().to_uppercase();
        matches!(name.as_str(), "TUMBLE" | "HOP" | "SESSION")
    } else {
        false
    }
}

/// Extracts a simple column name from an expression.
fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            // Use the last part (column name, ignoring table qualifier)
            parts.last().map(|p| p.value.clone())
        }
        _ => None,
    }
}

/// Converts a literal expression to usize.
fn expr_to_usize(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(value_with_span) => match &value_with_span.value {
            sqlparser::ast::Value::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
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
}
