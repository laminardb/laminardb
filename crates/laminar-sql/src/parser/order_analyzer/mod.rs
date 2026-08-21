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
/// a ranking function and a filter on the outer query. We detect common
/// SQL patterns rather than doing full semantic analysis.
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
            BinaryOperator::LtEq if extract_column_name(left)? == alias => {
                // rn <= N
                return expr_to_usize(right);
            }
            BinaryOperator::Lt if extract_column_name(left)? == alias => {
                // rn < N -> k = N - 1
                return expr_to_usize(right).map(|n| n.saturating_sub(1));
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
mod tests;
