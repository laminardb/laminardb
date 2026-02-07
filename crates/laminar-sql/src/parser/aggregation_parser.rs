//! Aggregate function detection and extraction
//!
//! This module analyzes SQL queries to extract aggregate functions like
//! COUNT, SUM, MIN, MAX, AVG, STDDEV, VARIANCE, PERCENTILE, and more.
//! It determines the aggregation strategy and maps to DataFusion names.

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr, OrderByExpr, Select, SelectItem,
    SetExpr, Statement,
};

/// Types of aggregate functions supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateType {
    // ── Core aggregates ─────────────────────────────────────────────
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

    // ── Statistical aggregates ──────────────────────────────────────
    /// Sample standard deviation (STDDEV / STDDEV_SAMP)
    StdDev,
    /// Population standard deviation (STDDEV_POP)
    StdDevPop,
    /// Sample variance (VARIANCE / VAR_SAMP)
    Variance,
    /// Population variance (VAR_POP / VARIANCE_POP)
    VariancePop,
    /// Median
    Median,

    // ── Percentile aggregates ───────────────────────────────────────
    /// PERCENTILE_CONT (continuous interpolation)
    PercentileCont,
    /// PERCENTILE_DISC (discrete, nearest-rank)
    PercentileDisc,

    // ── Boolean aggregates ──────────────────────────────────────────
    /// BOOL_AND / EVERY
    BoolAnd,
    /// BOOL_OR / ANY
    BoolOr,

    // ── Collection aggregates ───────────────────────────────────────
    /// STRING_AGG / LISTAGG / GROUP_CONCAT
    StringAgg,
    /// ARRAY_AGG
    ArrayAgg,

    // ── Approximate aggregates ──────────────────────────────────────
    /// APPROX_COUNT_DISTINCT
    ApproxCountDistinct,
    /// APPROX_PERCENTILE_CONT
    ApproxPercentile,
    /// APPROX_MEDIAN
    ApproxMedian,

    // ── Correlation / Regression ────────────────────────────────────
    /// Covariance sample (COVAR_SAMP)
    Covar,
    /// Covariance population (COVAR_POP)
    CovarPop,
    /// Pearson correlation (CORR)
    Corr,
    /// Linear regression slope (REGR_SLOPE)
    RegrSlope,
    /// Linear regression intercept (REGR_INTERCEPT)
    RegrIntercept,

    // ── Bit aggregates ──────────────────────────────────────────────
    /// BIT_AND
    BitAnd,
    /// BIT_OR
    BitOr,
    /// BIT_XOR
    BitXor,

    /// Custom / unrecognized aggregate function
    Custom,
}

impl AggregateType {
    /// Check if this aggregate is order-sensitive.
    /// Order-sensitive aggregates require maintaining event order.
    #[must_use]
    pub fn is_order_sensitive(&self) -> bool {
        matches!(
            self,
            AggregateType::FirstValue
                | AggregateType::LastValue
                | AggregateType::PercentileCont
                | AggregateType::PercentileDisc
                | AggregateType::StringAgg
                | AggregateType::ArrayAgg
        )
    }

    /// Check if this aggregate is decomposable (can be computed incrementally).
    ///
    /// Decomposable aggregates can be split into partial and final steps,
    /// enabling parallel or distributed computation.
    #[must_use]
    pub fn is_decomposable(&self) -> bool {
        matches!(
            self,
            AggregateType::Count
                | AggregateType::Sum
                | AggregateType::Min
                | AggregateType::Max
                | AggregateType::BoolAnd
                | AggregateType::BoolOr
                | AggregateType::BitAnd
                | AggregateType::BitOr
                | AggregateType::BitXor
        )
    }

    /// Returns the DataFusion function registry name for this aggregate type,
    /// or `None` if not directly mappable.
    #[must_use]
    pub fn datafusion_name(&self) -> Option<&'static str> {
        match self {
            AggregateType::Count | AggregateType::CountDistinct => Some("count"),
            AggregateType::Sum => Some("sum"),
            AggregateType::Min => Some("min"),
            AggregateType::Max => Some("max"),
            AggregateType::Avg => Some("avg"),
            AggregateType::FirstValue => Some("first_value"),
            AggregateType::LastValue => Some("last_value"),
            AggregateType::StdDev => Some("stddev"),
            AggregateType::StdDevPop => Some("stddev_pop"),
            AggregateType::Variance => Some("variance"),
            AggregateType::VariancePop => Some("variance_pop"),
            AggregateType::Median => Some("median"),
            AggregateType::PercentileCont => Some("percentile_cont"),
            AggregateType::PercentileDisc => Some("percentile_disc"),
            AggregateType::BoolAnd => Some("bool_and"),
            AggregateType::BoolOr => Some("bool_or"),
            AggregateType::StringAgg => Some("string_agg"),
            AggregateType::ArrayAgg => Some("array_agg"),
            AggregateType::ApproxCountDistinct => Some("approx_distinct"),
            AggregateType::ApproxPercentile => Some("approx_percentile_cont"),
            AggregateType::ApproxMedian => Some("approx_median"),
            AggregateType::Covar => Some("covar_samp"),
            AggregateType::CovarPop => Some("covar_pop"),
            AggregateType::Corr => Some("corr"),
            AggregateType::RegrSlope => Some("regr_slope"),
            AggregateType::RegrIntercept => Some("regr_intercept"),
            AggregateType::BitAnd => Some("bit_and"),
            AggregateType::BitOr => Some("bit_or"),
            AggregateType::BitXor => Some("bit_xor"),
            AggregateType::Custom => None,
        }
    }

    /// Returns the number of input columns required by this aggregate.
    #[must_use]
    pub fn arity(&self) -> usize {
        match self {
            AggregateType::Covar
            | AggregateType::CovarPop
            | AggregateType::Corr
            | AggregateType::RegrSlope
            | AggregateType::RegrIntercept => 2,
            _ => 1,
        }
    }
}

/// Information about a detected aggregate function.
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
    /// FILTER clause expression (e.g. `COUNT(x) FILTER (WHERE x > 5)`)
    pub filter: Option<Box<Expr>>,
    /// WITHIN GROUP ORDER BY expressions
    pub within_group: Vec<OrderByExpr>,
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
            filter: None,
            within_group: Vec::new(),
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

    /// Check whether a FILTER clause is present.
    #[must_use]
    pub fn has_filter(&self) -> bool {
        self.filter.is_some()
    }

    /// Check whether a WITHIN GROUP clause is present.
    #[must_use]
    pub fn has_within_group(&self) -> bool {
        !self.within_group.is_empty()
    }
}

/// Analysis result for aggregations in a query.
#[derive(Debug, Clone, Default)]
pub struct AggregationAnalysis {
    /// List of aggregate functions found
    pub aggregates: Vec<AggregateInfo>,
    /// GROUP BY columns
    pub group_by_columns: Vec<String>,
    /// Whether the query has a HAVING clause
    pub has_having: bool,
    /// The HAVING expression as SQL text (for downstream evaluation)
    pub having_expr: Option<String>,
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
        self.aggregates
            .iter()
            .any(|a| a.aggregate_type.is_order_sensitive())
    }

    /// Check if all aggregates are decomposable.
    #[must_use]
    pub fn all_decomposable(&self) -> bool {
        self.aggregates
            .iter()
            .all(|a| a.aggregate_type.is_decomposable())
    }

    /// Get aggregates by type.
    #[must_use]
    pub fn get_by_type(&self, agg_type: AggregateType) -> Vec<&AggregateInfo> {
        self.aggregates
            .iter()
            .filter(|a| a.aggregate_type == agg_type)
            .collect()
    }

    /// Check if any aggregate has a FILTER clause.
    #[must_use]
    pub fn has_any_filter(&self) -> bool {
        self.aggregates.iter().any(AggregateInfo::has_filter)
    }

    /// Check if any aggregate has a WITHIN GROUP clause.
    #[must_use]
    pub fn has_any_within_group(&self) -> bool {
        self.aggregates.iter().any(AggregateInfo::has_within_group)
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

    // Check for HAVING clause and extract expression
    analysis.has_having = select.having.is_some();
    analysis.having_expr = select.having.as_ref().map(std::string::ToString::to_string);
}

/// Resolve a SQL function name (upper-cased) to an [`AggregateType`], handling
/// both canonical names and common aliases.
fn resolve_aggregate_type(name: &str, func: &Function) -> Option<AggregateType> {
    match name {
        // ── Core ────────────────────────────────────────────────────
        "COUNT" => {
            if has_distinct_arg(func) {
                Some(AggregateType::CountDistinct)
            } else {
                Some(AggregateType::Count)
            }
        }
        "SUM" => Some(AggregateType::Sum),
        "MIN" => Some(AggregateType::Min),
        "MAX" => Some(AggregateType::Max),
        "AVG" | "MEAN" => Some(AggregateType::Avg),
        "FIRST_VALUE" | "FIRST" => Some(AggregateType::FirstValue),
        "LAST_VALUE" | "LAST" => Some(AggregateType::LastValue),

        // ── Statistical ────────────────────────────────────────────
        "STDDEV" | "STDDEV_SAMP" => Some(AggregateType::StdDev),
        "STDDEV_POP" => Some(AggregateType::StdDevPop),
        "VARIANCE" | "VAR_SAMP" | "VAR" => Some(AggregateType::Variance),
        "VAR_POP" | "VARIANCE_POP" => Some(AggregateType::VariancePop),
        "MEDIAN" => Some(AggregateType::Median),

        // ── Percentile ─────────────────────────────────────────────
        "PERCENTILE_CONT" => Some(AggregateType::PercentileCont),
        "PERCENTILE_DISC" => Some(AggregateType::PercentileDisc),

        // ── Boolean ────────────────────────────────────────────────
        "BOOL_AND" | "EVERY" => Some(AggregateType::BoolAnd),
        "BOOL_OR" | "ANY" => Some(AggregateType::BoolOr),

        // ── Collection ─────────────────────────────────────────────
        "STRING_AGG" | "LISTAGG" | "GROUP_CONCAT" => Some(AggregateType::StringAgg),
        "ARRAY_AGG" => Some(AggregateType::ArrayAgg),

        // ── Approximate ────────────────────────────────────────────
        "APPROX_COUNT_DISTINCT" | "APPROX_DISTINCT" => Some(AggregateType::ApproxCountDistinct),
        "APPROX_PERCENTILE_CONT" | "APPROX_PERCENTILE" => Some(AggregateType::ApproxPercentile),
        "APPROX_MEDIAN" => Some(AggregateType::ApproxMedian),

        // ── Correlation / Regression ───────────────────────────────
        "COVAR_SAMP" | "COVAR" => Some(AggregateType::Covar),
        "COVAR_POP" => Some(AggregateType::CovarPop),
        "CORR" => Some(AggregateType::Corr),
        "REGR_SLOPE" => Some(AggregateType::RegrSlope),
        "REGR_INTERCEPT" => Some(AggregateType::RegrIntercept),

        // ── Bit ────────────────────────────────────────────────────
        "BIT_AND" => Some(AggregateType::BitAnd),
        "BIT_OR" => Some(AggregateType::BitOr),
        "BIT_XOR" => Some(AggregateType::BitXor),

        _ => None,
    }
}

/// Extract aggregate function from an expression.
fn extract_aggregate(expr: &Expr, alias: Option<String>) -> Option<AggregateInfo> {
    match expr {
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_uppercase();
            let agg_type = resolve_aggregate_type(&func_name, func)?;

            let column = extract_first_arg_column(func);
            let distinct = has_distinct_arg(func);

            let mut info = AggregateInfo::new(agg_type, column).with_distinct(distinct);

            // Extract FILTER clause
            if let Some(filter_expr) = &func.filter {
                info.filter = Some(filter_expr.clone());
            }

            // Extract WITHIN GROUP clause
            if !func.within_group.is_empty() {
                info.within_group.clone_from(&func.within_group);
            }

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

    // ── Core aggregate tests (existing, preserved) ──────────────────

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
        let stmt = parse_statement("SELECT category, COUNT(*) FROM products GROUP BY category");
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
        let stmt =
            parse_statement("SELECT COUNT(*), SUM(amount), MIN(price), MAX(price) FROM orders");
        let analysis = analyze_aggregates(&stmt);
        assert!(analysis.all_decomposable());

        let stmt2 = parse_statement("SELECT AVG(price), FIRST_VALUE(price) FROM orders");
        let analysis2 = analyze_aggregates(&stmt2);
        assert!(!analysis2.all_decomposable());
    }

    #[test]
    fn test_get_by_type() {
        let stmt = parse_statement("SELECT COUNT(*), COUNT(id), SUM(amount) FROM orders");
        let analysis = analyze_aggregates(&stmt);

        let counts = analysis.get_by_type(AggregateType::Count);
        assert_eq!(counts.len(), 2);

        let sums = analysis.get_by_type(AggregateType::Sum);
        assert_eq!(sums.len(), 1);
    }

    // ── New aggregate type detection tests ──────────────────────────

    #[test]
    fn test_stddev() {
        let stmt = parse_statement("SELECT STDDEV(price) FROM trades");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates.len(), 1);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::StdDev);
    }

    #[test]
    fn test_stddev_pop() {
        let stmt = parse_statement("SELECT STDDEV_POP(latency) FROM requests");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::StdDevPop
        );
    }

    #[test]
    fn test_variance() {
        let stmt = parse_statement("SELECT VARIANCE(price) FROM trades");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::Variance
        );
    }

    #[test]
    fn test_variance_pop() {
        let stmt = parse_statement("SELECT VAR_POP(price) FROM trades");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::VariancePop
        );
    }

    #[test]
    fn test_median() {
        let stmt = parse_statement("SELECT MEDIAN(response_time) FROM requests");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Median);
    }

    #[test]
    fn test_percentile_cont() {
        let stmt = parse_statement("SELECT PERCENTILE_CONT(0.95) FROM latencies");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::PercentileCont
        );
    }

    #[test]
    fn test_percentile_disc() {
        let stmt = parse_statement("SELECT PERCENTILE_DISC(0.5) FROM scores");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::PercentileDisc
        );
    }

    #[test]
    fn test_bool_and() {
        let stmt = parse_statement("SELECT BOOL_AND(is_active) FROM users");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::BoolAnd
        );
    }

    #[test]
    fn test_bool_or() {
        let stmt = parse_statement("SELECT BOOL_OR(has_error) FROM events");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::BoolOr);
    }

    #[test]
    fn test_string_agg() {
        let stmt = parse_statement("SELECT STRING_AGG(name, ',') FROM users");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::StringAgg
        );
        assert!(analysis.aggregates[0].aggregate_type.is_order_sensitive());
    }

    #[test]
    fn test_array_agg() {
        let stmt = parse_statement("SELECT ARRAY_AGG(id) FROM events");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::ArrayAgg
        );
    }

    #[test]
    fn test_approx_count_distinct() {
        let stmt = parse_statement("SELECT APPROX_COUNT_DISTINCT(user_id) FROM events");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::ApproxCountDistinct
        );
    }

    #[test]
    fn test_approx_percentile() {
        let stmt = parse_statement("SELECT APPROX_PERCENTILE_CONT(latency, 0.99) FROM req");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::ApproxPercentile
        );
    }

    #[test]
    fn test_approx_median() {
        let stmt = parse_statement("SELECT APPROX_MEDIAN(price) FROM trades");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::ApproxMedian
        );
    }

    #[test]
    fn test_covar_samp() {
        let stmt = parse_statement("SELECT COVAR_SAMP(x, y) FROM points");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Covar);
    }

    #[test]
    fn test_covar_pop() {
        let stmt = parse_statement("SELECT COVAR_POP(x, y) FROM points");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::CovarPop
        );
    }

    #[test]
    fn test_corr() {
        let stmt = parse_statement("SELECT CORR(x, y) FROM points");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Corr);
    }

    #[test]
    fn test_regr_slope() {
        let stmt = parse_statement("SELECT REGR_SLOPE(y, x) FROM data");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::RegrSlope
        );
    }

    #[test]
    fn test_regr_intercept() {
        let stmt = parse_statement("SELECT REGR_INTERCEPT(y, x) FROM data");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::RegrIntercept
        );
    }

    #[test]
    fn test_bit_aggregates() {
        let stmt =
            parse_statement("SELECT BIT_AND(flags), BIT_OR(flags), BIT_XOR(flags) FROM events");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates.len(), 3);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::BitAnd);
        assert_eq!(analysis.aggregates[1].aggregate_type, AggregateType::BitOr);
        assert_eq!(analysis.aggregates[2].aggregate_type, AggregateType::BitXor);
    }

    // ── Alias synonym tests ────────────────────────────────────────

    #[test]
    fn test_alias_stddev_samp() {
        let stmt = parse_statement("SELECT STDDEV_SAMP(price) FROM trades");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::StdDev);
    }

    #[test]
    fn test_alias_var_samp() {
        let stmt = parse_statement("SELECT VAR_SAMP(price) FROM trades");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::Variance
        );
    }

    #[test]
    fn test_alias_every() {
        let stmt = parse_statement("SELECT EVERY(is_valid) FROM checks");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::BoolAnd
        );
    }

    #[test]
    fn test_alias_listagg() {
        let stmt = parse_statement("SELECT LISTAGG(name, ',') FROM users");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::StringAgg
        );
    }

    #[test]
    fn test_alias_group_concat() {
        let stmt = parse_statement("SELECT GROUP_CONCAT(name, ',') FROM users");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(
            analysis.aggregates[0].aggregate_type,
            AggregateType::StringAgg
        );
    }

    // ── FILTER clause tests ────────────────────────────────────────

    #[test]
    fn test_filter_clause_count() {
        let stmt = parse_statement("SELECT COUNT(*) FILTER (WHERE status = 'active') FROM users");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates.len(), 1);
        assert!(analysis.aggregates[0].has_filter());
        assert!(analysis.has_any_filter());
    }

    #[test]
    fn test_filter_clause_sum() {
        let stmt = parse_statement(
            "SELECT SUM(amount) FILTER (WHERE category = 'A') AS sum_a FROM orders",
        );
        let analysis = analyze_aggregates(&stmt);
        assert!(analysis.aggregates[0].has_filter());
        assert_eq!(analysis.aggregates[0].alias, Some("sum_a".to_string()));
    }

    #[test]
    fn test_filter_clause_mixed() {
        let stmt = parse_statement("SELECT COUNT(*), COUNT(*) FILTER (WHERE x > 0) FROM t");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates.len(), 2);
        assert!(!analysis.aggregates[0].has_filter());
        assert!(analysis.aggregates[1].has_filter());
    }

    #[test]
    fn test_no_filter() {
        let stmt = parse_statement("SELECT SUM(amount) FROM orders");
        let analysis = analyze_aggregates(&stmt);
        assert!(!analysis.aggregates[0].has_filter());
        assert!(!analysis.has_any_filter());
    }

    // ── WITHIN GROUP tests ─────────────────────────────────────────

    #[test]
    fn test_within_group_percentile_cont() {
        let stmt =
            parse_statement("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency) FROM req");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates.len(), 1);
        assert!(analysis.aggregates[0].has_within_group());
        assert_eq!(analysis.aggregates[0].within_group.len(), 1);
        assert!(analysis.has_any_within_group());
    }

    #[test]
    fn test_within_group_string_agg() {
        let stmt =
            parse_statement("SELECT STRING_AGG(name, ',') WITHIN GROUP (ORDER BY name) FROM users");
        let analysis = analyze_aggregates(&stmt);
        assert!(analysis.aggregates[0].has_within_group());
    }

    #[test]
    fn test_no_within_group() {
        let stmt = parse_statement("SELECT SUM(amount) FROM orders");
        let analysis = analyze_aggregates(&stmt);
        assert!(!analysis.aggregates[0].has_within_group());
        assert!(!analysis.has_any_within_group());
    }

    // ── datafusion_name() tests ────────────────────────────────────

    #[test]
    fn test_datafusion_name_core() {
        assert_eq!(AggregateType::Count.datafusion_name(), Some("count"));
        assert_eq!(AggregateType::Sum.datafusion_name(), Some("sum"));
        assert_eq!(AggregateType::Min.datafusion_name(), Some("min"));
        assert_eq!(AggregateType::Max.datafusion_name(), Some("max"));
        assert_eq!(AggregateType::Avg.datafusion_name(), Some("avg"));
    }

    #[test]
    fn test_datafusion_name_statistical() {
        assert_eq!(AggregateType::StdDev.datafusion_name(), Some("stddev"));
        assert_eq!(
            AggregateType::StdDevPop.datafusion_name(),
            Some("stddev_pop")
        );
        assert_eq!(AggregateType::Variance.datafusion_name(), Some("variance"));
        assert_eq!(
            AggregateType::VariancePop.datafusion_name(),
            Some("variance_pop")
        );
        assert_eq!(AggregateType::Median.datafusion_name(), Some("median"));
    }

    #[test]
    fn test_datafusion_name_approx() {
        assert_eq!(
            AggregateType::ApproxCountDistinct.datafusion_name(),
            Some("approx_distinct")
        );
        assert_eq!(
            AggregateType::ApproxPercentile.datafusion_name(),
            Some("approx_percentile_cont")
        );
        assert_eq!(
            AggregateType::ApproxMedian.datafusion_name(),
            Some("approx_median")
        );
    }

    #[test]
    fn test_datafusion_name_custom() {
        assert_eq!(AggregateType::Custom.datafusion_name(), None);
    }

    // ── is_decomposable() for new types ────────────────────────────

    #[test]
    fn test_decomposable_new_types() {
        // Decomposable: bit aggregates, bool aggregates
        assert!(AggregateType::BoolAnd.is_decomposable());
        assert!(AggregateType::BoolOr.is_decomposable());
        assert!(AggregateType::BitAnd.is_decomposable());
        assert!(AggregateType::BitOr.is_decomposable());
        assert!(AggregateType::BitXor.is_decomposable());

        // Not decomposable: statistical, percentile, approx, etc.
        assert!(!AggregateType::StdDev.is_decomposable());
        assert!(!AggregateType::Variance.is_decomposable());
        assert!(!AggregateType::Median.is_decomposable());
        assert!(!AggregateType::PercentileCont.is_decomposable());
        assert!(!AggregateType::Corr.is_decomposable());
    }

    #[test]
    fn test_order_sensitive_new_types() {
        // Order-sensitive: percentile, string_agg, array_agg
        assert!(AggregateType::PercentileCont.is_order_sensitive());
        assert!(AggregateType::PercentileDisc.is_order_sensitive());
        assert!(AggregateType::StringAgg.is_order_sensitive());
        assert!(AggregateType::ArrayAgg.is_order_sensitive());

        // Not order-sensitive: statistical aggregates
        assert!(!AggregateType::StdDev.is_order_sensitive());
        assert!(!AggregateType::Variance.is_order_sensitive());
        assert!(!AggregateType::Corr.is_order_sensitive());
    }

    // ── Multi-aggregate with new types ─────────────────────────────

    #[test]
    fn test_multi_aggregate_statistical() {
        let stmt = parse_statement(
            "SELECT AVG(price), STDDEV(price), VARIANCE(price), \
             MEDIAN(price) FROM trades GROUP BY symbol",
        );
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates.len(), 4);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::Avg);
        assert_eq!(analysis.aggregates[1].aggregate_type, AggregateType::StdDev);
        assert_eq!(
            analysis.aggregates[2].aggregate_type,
            AggregateType::Variance
        );
        assert_eq!(analysis.aggregates[3].aggregate_type, AggregateType::Median);
        assert!(!analysis.all_decomposable());
    }

    #[test]
    fn test_multi_aggregate_mixed_with_filter() {
        let stmt = parse_statement(
            "SELECT COUNT(*), \
             SUM(amount) FILTER (WHERE status = 'complete'), \
             APPROX_COUNT_DISTINCT(user_id) FROM orders",
        );
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates.len(), 3);
        assert!(!analysis.aggregates[0].has_filter());
        assert!(analysis.aggregates[1].has_filter());
        assert!(!analysis.aggregates[2].has_filter());
    }

    // ── Arity tests ────────────────────────────────────────────────

    #[test]
    fn test_arity() {
        assert_eq!(AggregateType::Count.arity(), 1);
        assert_eq!(AggregateType::Sum.arity(), 1);
        assert_eq!(AggregateType::Covar.arity(), 2);
        assert_eq!(AggregateType::CovarPop.arity(), 2);
        assert_eq!(AggregateType::Corr.arity(), 2);
        assert_eq!(AggregateType::RegrSlope.arity(), 2);
        assert_eq!(AggregateType::RegrIntercept.arity(), 2);
    }

    // ── Case insensitivity ─────────────────────────────────────────

    // ── HAVING expression extraction tests ──────────────────────────

    #[test]
    fn test_having_expr_simple() {
        let stmt = parse_statement(
            "SELECT symbol, COUNT(*) FROM trades GROUP BY symbol HAVING COUNT(*) > 10",
        );
        let analysis = analyze_aggregates(&stmt);
        assert!(analysis.has_having);
        assert!(analysis.having_expr.is_some());
        let expr = analysis.having_expr.unwrap();
        assert!(expr.contains("COUNT(*)"), "expr was: {expr}");
        assert!(expr.contains("10"), "expr was: {expr}");
    }

    #[test]
    fn test_having_expr_multiple_predicates() {
        let stmt = parse_statement(
            "SELECT symbol, SUM(volume) AS vol, AVG(price) AS avg_p \
             FROM trades GROUP BY symbol \
             HAVING SUM(volume) > 1000 AND AVG(price) < 50",
        );
        let analysis = analyze_aggregates(&stmt);
        assert!(analysis.has_having);
        let expr = analysis.having_expr.unwrap();
        assert!(expr.contains("SUM(volume)"), "expr was: {expr}");
        assert!(expr.contains("AVG(price)"), "expr was: {expr}");
        assert!(expr.contains("AND"), "expr was: {expr}");
    }

    #[test]
    fn test_having_expr_with_or() {
        let stmt = parse_statement(
            "SELECT category, COUNT(*) FROM products GROUP BY category \
             HAVING COUNT(*) > 100 OR SUM(price) > 5000",
        );
        let analysis = analyze_aggregates(&stmt);
        assert!(analysis.has_having);
        let expr = analysis.having_expr.unwrap();
        assert!(expr.contains("OR"), "expr was: {expr}");
    }

    #[test]
    fn test_having_expr_none_without_having() {
        let stmt = parse_statement("SELECT symbol, COUNT(*) FROM trades GROUP BY symbol");
        let analysis = analyze_aggregates(&stmt);
        assert!(!analysis.has_having);
        assert!(analysis.having_expr.is_none());
    }

    #[test]
    fn test_having_expr_with_alias_reference() {
        let stmt = parse_statement(
            "SELECT symbol, COUNT(*) AS cnt FROM trades GROUP BY symbol HAVING COUNT(*) >= 5",
        );
        let analysis = analyze_aggregates(&stmt);
        assert!(analysis.has_having);
        let expr = analysis.having_expr.unwrap();
        assert!(expr.contains("COUNT(*)"), "expr was: {expr}");
        assert!(expr.contains('5'), "expr was: {expr}");
    }

    #[test]
    fn test_case_insensitive_detection() {
        let stmt = parse_statement("SELECT stddev(price), Variance(price) FROM trades");
        let analysis = analyze_aggregates(&stmt);
        assert_eq!(analysis.aggregates.len(), 2);
        assert_eq!(analysis.aggregates[0].aggregate_type, AggregateType::StdDev);
        assert_eq!(
            analysis.aggregates[1].aggregate_type,
            AggregateType::Variance
        );
    }
}
