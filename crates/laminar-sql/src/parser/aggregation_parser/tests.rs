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
    let stmt =
        parse_statement("SELECT COUNT(*), SUM(amount), AVG(price), MIN(qty), MAX(qty) FROM orders");
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
    let stmt = parse_statement("SELECT BIT_AND(flags), BIT_OR(flags), BIT_XOR(flags) FROM events");
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
    let stmt =
        parse_statement("SELECT SUM(amount) FILTER (WHERE category = 'A') AS sum_a FROM orders");
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
