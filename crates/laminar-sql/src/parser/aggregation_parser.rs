//! Aggregate function detection and extraction
//!
//! This module analyzes SQL queries to extract aggregate functions like
//! COUNT, SUM, MIN, MAX, AVG, and determines the aggregation strategy.

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr, Select, SelectItem, SetExpr,
    Statement,
};

/// Types of aggregate functions supported
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateType {
    /// COUNT function
    Count,
    /// COUNT DISTINCT function
    CountDistinct,
    /// SUM function
    Sum,
    /// MIN function
    Min,
    /// MAX function
    Max,
    /// AVG function
    Avg,
    /// `FIRST_VALUE` function
    FirstValue,
    /// `LAST_VALUE` function
    LastValue,
    /// Custom aggregate function
    Custom,
}

impl AggregateType {
    /// Check if this aggregate is order-sensitive.
    /// Order-sensitive aggregates require maintaining event order.
    #[must_use]
    pub fn is_order_sensitive(&self) -> bool {
        matches!(self, AggregateType::FirstValue | AggregateType::LastValue)
    }

    /// Check if this aggregate is decomposable (can be computed incrementally).
    #[must_use]
    pub fn is_decomposable(&self) -> bool {
        matches!(
            self,
            AggregateType::Count
                | AggregateType::Sum
                | AggregateType::Min
                | AggregateType::Max
        )
    }
}

/// Information about a detected aggregate function
#[derive(Debug, Clone)]
pub struct AggregateInfo {
    /// Type of aggregate
    pub aggregate_type: AggregateType,
    /// Column being aggregated (None for COUNT(*))
    pub column: Option<String>,
    /// Optional alias for the aggregate result
    pub alias: Option<String>,
    /// Whether DISTINCT is applied
    pub distinct: bool,
}

impl AggregateInfo {
    /// Create a new aggregate info.
    #[must_use]
    pub fn new(aggregate_type: AggregateType, column: Option<String>) -> Self {
        Self {
            aggregate_type,
            column,
            alias: None,
            distinct: false,
        }
    }

    /// Set the alias.
    #[must_use]
    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    /// Set distinct flag.
    #[must_use]
    pub fn with_distinct(mut self, distinct: bool) -> Self {
        self.distinct = distinct;
        self
    }
}

/// Analysis result for aggregations in a query
#[derive(Debug, Clone, Default)]
pub struct AggregationAnalysis {
    /// List of aggregate functions found
    pub aggregates: Vec<AggregateInfo>,
    /// GROUP BY columns
    pub group_by_columns: Vec<String>,
    /// Whether the query has a HAVING clause
    pub has_having: bool,
}

impl AggregationAnalysis {
    /// Check if this analysis contains any aggregates.
    #[must_use]
    pub fn has_aggregates(&self) -> bool {
        !self.aggregates.is_empty()
    }

    /// Check if any aggregate is order-sensitive.
    #[must_use]
    pub fn has_order_sensitive(&self) -> bool {
        self.aggregates.iter().any(|a| a.aggregate_type.is_order_sensitive())
    }

    /// Check if all aggregates are decomposable.
    #[must_use]
    pub fn all_decomposable(&self) -> bool {
        self.aggregates.iter().all(|a| a.aggregate_type.is_decomposable())
    }

    /// Get aggregates by type.
    #[must_use]
    pub fn get_by_type(&self, agg_type: AggregateType) -> Vec<&AggregateInfo> {
        self.aggregates
            .iter()
            .filter(|a| a.aggregate_type == agg_type)
            .collect()
    }
}

/// Analyze a SQL statement for aggregate functions.
#[must_use]
pub fn analyze_aggregates(stmt: &Statement) -> AggregationAnalysis {
    let mut analysis = AggregationAnalysis::default();

    if let Statement::Query(query) = stmt {
        if let SetExpr::Select(select) = query.body.as_ref() {
            analyze_select(&mut analysis, select);
        }
    }

    analysis
}

/// Analyze a SELECT statement for aggregates.
fn analyze_select(analysis: &mut AggregationAnalysis, select: &Select) {
    // Check SELECT items for aggregate functions
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(agg) = extract_aggregate(expr, None) {
                    analysis.aggregates.push(agg);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(agg) = extract_aggregate(expr, Some(alias.value.clone())) {
                    analysis.aggregates.push(agg);
                }
            }
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {}
        }
    }

    // Extract GROUP BY columns
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _modifiers) => {
            for expr in exprs {
                if let Some(col) = extract_column_name(expr) {
                    analysis.group_by_columns.push(col);
                }
            }
        }
        GroupByExpr::All(_) => {}
    }

    // Check for HAVING clause
    analysis.has_having = select.having.is_some();
}

/// Extract aggregate function from an expression.
fn extract_aggregate(expr: &Expr, alias: Option<String>) -> Option<AggregateInfo> {
    match expr {
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_uppercase();
            let agg_type = match func_name.as_str() {
                "COUNT" => {
                    if has_distinct_arg(func) {
                        AggregateType::CountDistinct
                    } else {
                        AggregateType::Count
                    }
                }
                "SUM" => AggregateType::Sum,
                "MIN" => AggregateType::Min,
                "MAX" => AggregateType::Max,
                "AVG" => AggregateType::Avg,
                "FIRST_VALUE" | "FIRST" => AggregateType::FirstValue,
                "LAST_VALUE" | "LAST" => AggregateType::LastValue,
                _ => return None, // Not a recognized aggregate
            };

            let column = extract_first_arg_column(func);
            let distinct = has_distinct_arg(func);

            let mut info = AggregateInfo::new(agg_type, column).with_distinct(distinct);
            if let Some(a) = alias {
                info = info.with_alias(a);
            }
            Some(info)
        }
        // Handle nested expressions (e.g., CAST(COUNT(*) AS INT))
        Expr::Cast { expr, .. } | Expr::Nested(expr) => extract_aggregate(expr, alias),
        _ => None,
    }
}

/// Check if the function has a DISTINCT argument.
fn has_distinct_arg(func: &Function) -> bool {
    // In sqlparser 0.60, DISTINCT is part of FunctionArgumentList
    match &func.args {
        sqlparser::ast::FunctionArguments::List(list) => list.duplicate_treatment.is_some(),
        _ => false,
    }
}

/// Extract the column name from the first argument of a function.
fn extract_first_arg_column(func: &Function) -> Option<String> {
    // Handle FunctionArguments::List
    match &func.args {
        sqlparser::ast::FunctionArguments::List(list) => {
            if list.args.is_empty() {
                return None;
            }
            match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => extract_column_name(expr),
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                    if let FunctionArgExpr::Expr(expr) = arg {
                        extract_column_name(expr)
                    } else {
                        None
                    }
                }
                // COUNT(*), QualifiedWildcard, etc.
                FunctionArg::Unnamed(_) => None,
            }
        }
        sqlparser::ast::FunctionArguments::Subquery(_)
        | sqlparser::ast::FunctionArguments::None => None,
    }
}

/// Extract column name from an expression.
fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        _ => None,
    }
}

/// Check if a SELECT statement contains any aggregate functions.
#[must_use]
pub fn has_aggregates(stmt: &Statement) -> bool {
    analyze_aggregates(stmt).has_aggregates()
}

/// Count the number of aggregate functions in a statement.
#[must_use]
pub fn count_aggregates(stmt: &Statement) -> usize {
    analyze_aggregates(stmt).aggregates.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_statement(sql: &str) -> Statement {
        let dialect = GenericDialect {};
        Parser::parse_sql(&dialect, sql).unwrap().remove(0)
    }

    #[test]
    fn test_analyze_count() {
        let stmt = parse_statement("SELECT COUNT(*) FROM events");
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.aggregates.len(), 1);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Count);
        assert!(analysis.aggregates[0].column.is_none());
    }

    #[test]
    fn test_analyze_count_column() {
        let stmt = parse_statement("SELECT COUNT(id) FROM events");
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.aggregates.len(), 1);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Count);
        assert_eq!(analysis.aggregates[0].column, Some("id".to_string()));
    }

    #[test]
    fn test_analyze_count_distinct() {
        let stmt = parse_statement("SELECT COUNT(DISTINCT user_id) FROM events");
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.aggregates.len(), 1);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::CountDistinct
        );
        assert!(analysis.aggregates[0].distinct);
    }

    #[test]
    fn test_analyze_sum() {
        let stmt = parse_statement("SELECT SUM(amount) FROM orders");
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.aggregates.len(), 1);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Sum);
        assert_eq!(analysis.aggregates[0].column, Some("amount".to_string()));
    }

    #[test]
    fn test_analyze_min_max() {
        let stmt = parse_statement("SELECT MIN(price), MAX(price) FROM products");
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.aggregates.len(), 2);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Min);
        assert_eq!(analysis.aggregates[1].aggregate_type, AggregateType::Max);
    }

    #[test]
    fn test_analyze_avg() {
        let stmt = parse_statement("SELECT AVG(score) AS avg_score FROM tests");
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.aggregates.len(), 1);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Avg);
        assert_eq!(analysis.aggregates[0].alias, Some("avg_score".to_string()));
    }

    #[test]
    fn test_analyze_first_last() {
        let stmt = parse_statement(
            "SELECT FIRST_VALUE(price) AS open, LAST_VALUE(price) AS close FROM trades",
        );
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.aggregates.len(), 2);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::FirstValue
        );
        assert_eq!(
            analysis.aggregates[1].aggregate_type,
            AggregateType::LastValue
        );
        assert!(analysis.has_order_sensitive());
    }

    #[test]
    fn test_analyze_group_by() {
        let stmt =
            parse_statement("SELECT category, COUNT(*) FROM products GROUP BY category");
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.aggregates.len(), 1);
        assert_eq!(analysis.group_by_columns.len(), 1);
        assert_eq!(analysis.group_by_columns[0], "category");
    }

    #[test]
    fn test_analyze_multiple_group_by() {
        let stmt = parse_statement(
            "SELECT region, category, SUM(sales) FROM orders GROUP BY region, category",
        );
        let analysis = analyze_aggregates(&stmt);

        assert_eq!(analysis.group_by_columns.len(), 2);
        assert_eq!(analysis.group_by_columns[0], "region");
        assert_eq!(analysis.group_by_columns[1], "category");
    }

    #[test]
    fn test_analyze_having() {
        let stmt = parse_statement(
            "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 10",
        );
        let analysis = analyze_aggregates(&stmt);

        assert!(analysis.has_having);
    }

    #[test]
    fn test_no_aggregates() {
        let stmt = parse_statement("SELECT id, name FROM users");
        let analysis = analyze_aggregates(&stmt);

        assert!(!analysis.has_aggregates());
        assert_eq!(analysis.aggregates.len(), 0);
    }

    #[test]
    fn test_has_aggregates() {
        let with_agg = parse_statement("SELECT COUNT(*) FROM events");
        let without_agg = parse_statement("SELECT * FROM events");

        assert!(has_aggregates(&with_agg));
        assert!(!has_aggregates(&without_agg));
    }

    #[test]
    fn test_count_aggregates() {
        let stmt = parse_statement(
            "SELECT COUNT(*), SUM(amount), AVG(price), MIN(qty), MAX(qty) FROM orders",
        );
        assert_eq!(count_aggregates(&stmt), 5);
    }

    #[test]
    fn test_decomposable() {
        let stmt = parse_statement("SELECT COUNT(*), SUM(amount), MIN(price), MAX(price) FROM orders");
        let analysis = analyze_aggregates(&stmt);
        assert!(analysis.all_decomposable());

        let stmt2 = parse_statement("SELECT AVG(price), FIRST_VALUE(price) FROM orders");
        let analysis2 = analyze_aggregates(&stmt2);
        assert!(!analysis2.all_decomposable());
    }

    #[test]
    fn test_get_by_type() {
        let stmt = parse_statement(
            "SELECT COUNT(*), COUNT(id), SUM(amount) FROM orders",
        );
        let analysis = analyze_aggregates(&stmt);

        let counts = analysis.get_by_type(AggregateType::Count);
        assert_eq!(counts.len(), 2);

        let sums = analysis.get_by_type(AggregateType::Sum);
        assert_eq!(sums.len(), 1);
    }
}
