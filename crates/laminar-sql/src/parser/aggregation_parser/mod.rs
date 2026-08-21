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

    analysis.has_having = select.having.is_some();
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
mod tests;
