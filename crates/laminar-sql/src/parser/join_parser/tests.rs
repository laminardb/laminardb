use super::*;
use sqlparser::ast::{SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn parse_select(sql: &str) -> Select {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    if let Statement::Query(query) = &statements[0] {
        if let SetExpr::Select(select) = query.body.as_ref() {
            return *select.clone();
        }
    }
    panic!("Expected SELECT query");
}

fn join_error(sql: &str) -> String {
    analyze_join(&parse_select(sql)).unwrap_err().to_string()
}

#[test]
fn test_analyze_inner_join() {
    let sql = "SELECT * FROM orders o INNER JOIN payments p ON o.order_id = p.order_id";
    let select = parse_select(sql);

    let analysis = analyze_join(&select).unwrap().unwrap();

    assert_eq!(analysis.join_type, JoinType::Inner);
    assert_eq!(analysis.left_table, "orders");
    assert_eq!(analysis.right_table, "payments");
    assert_eq!(analysis.left_key_column, "order_id");
    assert_eq!(analysis.right_key_column, "order_id");
    assert!(analysis.is_lookup_join); // No time bound = lookup join
}

#[test]
fn test_analyze_left_join() {
    let sql = "SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id";
    let select = parse_select(sql);

    let analysis = analyze_join(&select).unwrap().unwrap();

    assert_eq!(analysis.join_type, JoinType::Left);
    assert_eq!(analysis.left_key_column, "customer_id");
    assert_eq!(analysis.right_key_column, "id");
}

#[test]
fn test_analyze_join_using() {
    let sql = "SELECT * FROM orders o JOIN payments p USING (order_id)";
    let select = parse_select(sql);

    let analysis = analyze_join(&select).unwrap().unwrap();

    assert_eq!(analysis.left_key_column, "order_id");
    assert_eq!(analysis.right_key_column, "order_id");
}

#[test]
fn test_analyze_stream_stream_join_with_time_bound() {
    let sql = "SELECT * FROM orders o
                   JOIN payments p ON o.order_id = p.order_id
                   AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR";
    let select = parse_select(sql);

    let analysis = analyze_join(&select).unwrap().unwrap();

    assert!(!analysis.is_lookup_join);
    assert!(analysis.time_bound.is_some());
    assert_eq!(analysis.time_bound.unwrap(), Duration::from_secs(3600));
    assert_eq!(analysis.left_time_column.as_deref(), Some("ts"));
    assert_eq!(analysis.right_time_column.as_deref(), Some("ts"));
}

#[test]
fn test_interval_join_accepts_table_qualifiers() {
    let sql = "SELECT * FROM orders
                   JOIN payments ON orders.order_id = payments.order_id
                   AND payments.received_at BETWEEN orders.created_at
                       AND orders.created_at + INTERVAL '250' MILLISECOND";

    let analysis = analyze_join(&parse_select(sql)).unwrap().unwrap();

    assert_eq!(analysis.time_bound, Some(Duration::from_millis(250)));
    assert_eq!(analysis.left_time_column.as_deref(), Some("created_at"));
    assert_eq!(analysis.right_time_column.as_deref(), Some("received_at"));
}

#[test]
fn test_interval_join_preserves_composite_equality_keys() {
    let sql = "SELECT * FROM orders o JOIN payments p
                   ON o.tenant_id = p.tenant_id
                   AND o.order_id = p.order_id
                   AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND";

    let analysis = analyze_join(&parse_select(sql)).unwrap().unwrap();

    assert_eq!(analysis.left_key_column, "tenant_id");
    assert_eq!(analysis.right_key_column, "tenant_id");
    assert_eq!(
        analysis.additional_key_columns,
        vec![("order_id".to_string(), "order_id".to_string())]
    );
}

#[test]
fn test_join_orients_reversed_different_name_keys() {
    let analysis = analyze_join(&parse_select(
        "SELECT * FROM orders o JOIN payments p ON p.order_id = o.id",
    ))
    .unwrap()
    .unwrap();

    assert_eq!(analysis.left_key_column, "id");
    assert_eq!(analysis.right_key_column, "order_id");
}

#[test]
fn test_composite_join_orients_each_key_independently() {
    let analysis = analyze_join(&parse_select(
        "SELECT * FROM orders o JOIN payments p
             ON p.order_id = o.id AND o.tenant = p.account",
    ))
    .unwrap()
    .unwrap();

    assert_eq!(analysis.left_key_column, "id");
    assert_eq!(analysis.right_key_column, "order_id");
    assert_eq!(
        analysis.additional_key_columns,
        vec![("tenant".to_string(), "account".to_string())]
    );
}

#[test]
fn test_join_accepts_quoted_relation_and_key_identity() {
    let analysis = analyze_join(&parse_select(
        "SELECT * FROM \"Orders\"
             JOIN \"Payments\"
             ON \"Payments\".\"order id\" = \"Orders\".\"id\"",
    ))
    .unwrap()
    .unwrap();

    assert_eq!(analysis.left_table, "Orders");
    assert_eq!(analysis.right_table, "Payments");
    assert_eq!(analysis.left_key_column, "id");
    assert_eq!(analysis.right_key_column, "order id");
}

#[test]
fn test_join_accepts_quoted_alias_identity() {
    let analysis = analyze_join(&parse_select(
        "SELECT * FROM orders AS \"left input\"
             JOIN payments AS \"right input\"
             ON \"left input\".id = \"right input\".order_id",
    ))
    .unwrap()
    .unwrap();

    assert_eq!(analysis.left_alias.as_deref(), Some("left input"));
    assert_eq!(analysis.right_alias.as_deref(), Some("right input"));
    assert_eq!(analysis.left_key_column, "id");
    assert_eq!(analysis.right_key_column, "order_id");
}

#[test]
fn test_join_rejects_unqualified_key() {
    let error = join_error("SELECT * FROM orders o JOIN payments p ON id = p.order_id");
    assert!(error.contains("qualified column references"), "{error}");
}

#[test]
fn test_join_rejects_unknown_key_qualifier() {
    let error = join_error("SELECT * FROM orders o JOIN payments p ON missing.id = p.order_id");
    assert!(error.contains("names neither input"), "{error}");
}

#[test]
fn test_join_rejects_same_side_key_expression() {
    let error = join_error("SELECT * FROM orders o JOIN payments p ON o.id = o.parent_id");
    assert!(error.contains("one left-input column"), "{error}");
}

#[test]
fn test_join_rejects_ambiguous_qualifier() {
    let error = join_error(
        "SELECT * FROM orders duplicate JOIN payments duplicate
             ON duplicate.id = payments.order_id",
    );
    assert!(error.contains("names both inputs"), "{error}");
}

#[test]
fn test_join_rejects_compound_relation_identity() {
    let error = join_error(
        "SELECT * FROM catalog.orders JOIN payments
             ON catalog.orders.id = payments.order_id",
    );
    assert!(error.contains("single-part relation names"), "{error}");
}

#[test]
fn test_interval_join_rejects_unqualified_timestamp() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN ts AND ts + INTERVAL '1' SECOND",
    );
    assert!(error.contains("qualified column references"), "{error}");
}

#[test]
fn test_interval_join_rejects_unknown_qualifier() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND unknown.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND",
    );
    assert!(error.contains("unambiguous left and right"), "{error}");
}

#[test]
fn test_interval_join_rejects_reversed_timestamps() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND o.ts BETWEEN p.ts AND p.ts + INTERVAL '1' SECOND",
    );
    assert!(
        error.contains("right timestamp BETWEEN the left"),
        "{error}"
    );
}

#[test]
fn test_interval_join_rejects_not_between() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts NOT BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND",
    );
    assert!(error.contains("NOT BETWEEN"), "{error}");
}

#[test]
fn test_interval_join_rejects_mismatched_upper_timestamp() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.other_ts + INTERVAL '1' SECOND",
    );
    assert!(error.contains("same timestamp"), "{error}");
}

#[test]
fn test_interval_join_rejects_subtracted_bound() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.ts - INTERVAL '1' SECOND",
    );
    assert!(error.contains("must use addition"), "{error}");
}

#[test]
fn test_interval_join_rejects_direct_interval_upper_bound() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND INTERVAL '1' SECOND",
    );
    assert!(error.contains("left timestamp plus an interval"), "{error}");
}

#[test]
fn test_interval_join_rejects_zero_bound() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '0' SECOND",
    );
    assert!(error.contains("positive finite"), "{error}");
}

#[test]
fn test_interval_join_rejects_negative_bound() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '-1' SECOND",
    );
    assert!(error.contains("Invalid interval value"), "{error}");
}

#[test]
fn test_interval_join_rejects_multiple_time_bounds() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p ON o.id = p.id
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND
             AND p.created_at BETWEEN o.created_at
                 AND o.created_at + INTERVAL '1' SECOND",
    );
    assert!(
        error.contains("exactly one time-bound predicate"),
        "{error}"
    );
}

#[test]
fn test_join_rejects_non_equi_residual_conjunct() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p
             ON o.id = p.id AND p.amount > o.amount",
    );
    assert!(error.contains("Unsupported join condition"), "{error}");
}

#[test]
fn test_join_rejects_unsupported_equality_conjunct() {
    let error = join_error(
        "SELECT * FROM orders o JOIN payments p
             ON o.id = p.id AND ABS(o.amount) = ABS(p.amount)",
    );
    assert!(
        error.contains("Cannot extract column references"),
        "{error}"
    );
}

#[test]
fn test_no_join() {
    let sql = "SELECT * FROM orders";
    let select = parse_select(sql);

    let analysis = analyze_join(&select).unwrap();
    assert!(analysis.is_none());
}

#[test]
fn test_has_join() {
    let sql_with_join = "SELECT * FROM orders o JOIN payments p ON o.id = p.order_id";
    let sql_without_join = "SELECT * FROM orders";

    let select_with = parse_select(sql_with_join);
    let select_without = parse_select(sql_without_join);

    assert!(has_join(&select_with));
    assert!(!has_join(&select_without));
}

#[test]
fn test_count_joins() {
    let sql_one = "SELECT * FROM a JOIN b ON a.id = b.id";
    let sql_two = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id";
    let sql_zero = "SELECT * FROM a";

    assert_eq!(count_joins(&parse_select(sql_one)), 1);
    assert_eq!(count_joins(&parse_select(sql_two)), 2);
    assert_eq!(count_joins(&parse_select(sql_zero)), 0);
}

#[test]
fn test_aliases() {
    let sql = "SELECT * FROM orders AS o JOIN payments AS p ON o.id = p.order_id";
    let select = parse_select(sql);

    let analysis = analyze_join(&select).unwrap().unwrap();

    assert_eq!(analysis.left_alias, Some("o".to_string()));
    assert_eq!(analysis.right_alias, Some("p".to_string()));
}
fn parse_select_snowflake(sql: &str) -> Select {
    let dialect = sqlparser::dialect::SnowflakeDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    if let Statement::Query(query) = &statements[0] {
        if let SetExpr::Select(select) = query.body.as_ref() {
            return *select.clone();
        }
    }
    panic!("Expected SELECT query");
}

fn parse_select_laminar(sql: &str) -> Select {
    let dialect = crate::parser::dialect::LaminarDialect::default();
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    if let Statement::Query(query) = &statements[0] {
        if let SetExpr::Select(select) = query.body.as_ref() {
            return *select.clone();
        }
    }
    panic!("Expected SELECT query");
}

#[test]
fn rejects_asof_join_with_bounded_interval_guidance() {
    let select = parse_select_snowflake(
        "SELECT * FROM trades t ASOF JOIN quotes q \
             MATCH_CONDITION(t.ts >= q.ts) ON t.symbol = q.symbol",
    );
    let error = analyze_join(&select).unwrap_err().to_string();

    assert!(
        error.contains(
            "ASOF JOIN is unsupported; use a bounded JOIN with an explicit event-time interval"
        ),
        "{error}"
    );
}

// -- Multi-way JOIN tests --

#[test]
fn test_multi_join_single_backward_compat() {
    let sql = "SELECT * FROM orders o JOIN payments p ON o.id = p.order_id";
    let select = parse_select(sql);
    let multi = analyze_joins(&select).unwrap().unwrap();

    assert!(multi.is_single());
    assert_eq!(multi.len(), 1);
    assert!(!multi.is_empty());
    let first = multi.first().unwrap();
    assert_eq!(first.left_table, "orders");
    assert_eq!(first.right_table, "payments");
}

#[test]
fn test_multi_join_two_way() {
    let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON c.b_id = b.id";
    let select = parse_select(sql);
    let multi = analyze_joins(&select).unwrap().unwrap();

    assert_eq!(multi.len(), 2);
    assert!(!multi.is_single());

    assert_eq!(multi.joins[0].left_table, "a");
    assert_eq!(multi.joins[0].right_table, "b");
    assert_eq!(multi.joins[0].left_key_column, "id");
    assert_eq!(multi.joins[0].right_key_column, "a_id");

    assert_eq!(multi.joins[1].left_table, "b");
    assert_eq!(multi.joins[1].right_table, "c");
    assert_eq!(multi.joins[1].left_key_column, "id");
    assert_eq!(multi.joins[1].right_key_column, "b_id");
}

#[test]
fn test_multi_join_three_way() {
    let sql = "SELECT * FROM a \
                    JOIN b ON a.id = b.a_id \
                    JOIN c ON b.id = c.b_id \
                    JOIN d ON c.id = d.c_id";
    let select = parse_select(sql);
    let multi = analyze_joins(&select).unwrap().unwrap();

    assert_eq!(multi.len(), 3);
    assert_eq!(multi.tables.len(), 4);
    assert_eq!(multi.tables, vec!["a", "b", "c", "d"]);
}
#[test]
fn test_multi_join_stream_stream_and_lookup() {
    let sql = "SELECT * FROM orders o \
                    JOIN payments p ON o.id = p.order_id \
                        AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR \
                    JOIN customers c ON p.customer_id = c.id";
    let select = parse_select(sql);
    let multi = analyze_joins(&select).unwrap().unwrap();

    assert_eq!(multi.len(), 2);
    assert!(!multi.joins[0].is_lookup_join); // stream-stream
    assert!(multi.joins[0].time_bound.is_some());
    assert!(multi.joins[1].is_lookup_join); // lookup
}

#[test]
fn test_multi_join_rejects_key_from_non_current_left_relation() {
    let select = parse_select(
        "SELECT * FROM a
             JOIN b ON a.id = b.a_id
             JOIN c ON a.id = c.a_id",
    );
    let error = analyze_joins(&select).unwrap_err().to_string();

    assert!(error.contains("names neither input"), "{error}");
}

#[test]
fn test_multi_join_tables_list() {
    let sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id";
    let select = parse_select(sql);
    let multi = analyze_joins(&select).unwrap().unwrap();

    assert_eq!(multi.tables, vec!["a", "b", "c"]);
}

#[test]
fn test_multi_join_aliases() {
    let sql = "SELECT * FROM orders AS o \
                    JOIN payments AS p ON o.id = p.order_id \
                    JOIN refunds AS r ON p.id = r.payment_id";
    let select = parse_select(sql);
    let multi = analyze_joins(&select).unwrap().unwrap();

    assert_eq!(multi.joins[0].left_alias, Some("o".to_string()));
    assert_eq!(multi.joins[0].right_alias, Some("p".to_string()));
    assert_eq!(multi.joins[1].left_alias, Some("p".to_string()));
    assert_eq!(multi.joins[1].right_alias, Some("r".to_string()));
}

#[test]
fn test_multi_join_no_join_returns_none() {
    let sql = "SELECT * FROM orders";
    let select = parse_select(sql);
    let multi = analyze_joins(&select).unwrap();
    assert!(multi.is_none());
}

// -- Temporal JOIN tests (FOR SYSTEM_TIME AS OF) --

#[test]
fn test_temporal_join_detected() {
    let sql = "SELECT o.*, p.price \
                    FROM orders o \
                    JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p \
                    ON o.product_id = p.id";
    let select = parse_select_laminar(sql);
    let analysis = analyze_join(&select).unwrap().unwrap();

    assert!(analysis.is_temporal_join());
    assert_eq!(analysis.left_time_column.as_deref(), Some("order_time"));
    assert_eq!(analysis.left_table, "orders");
    assert_eq!(analysis.right_table, "products");
    assert_eq!(analysis.left_key_column, "product_id");
    assert_eq!(analysis.right_key_column, "id");
    assert!(!analysis.is_lookup_join);
}

#[test]
fn test_temporal_join_via_analyze_joins() {
    let sql = "SELECT o.*, p.price \
                    FROM orders o \
                    JOIN products FOR SYSTEM_TIME AS OF o.order_time AS p \
                    ON o.product_id = p.id";
    let select = parse_select_laminar(sql);
    let multi = analyze_joins(&select).unwrap().unwrap();

    assert_eq!(multi.len(), 1);
    let first = multi.first().unwrap();
    assert!(first.is_temporal_join());
    assert_eq!(first.left_time_column.as_deref(), Some("order_time"));
}

#[test]
fn test_non_temporal_join_not_flagged() {
    let sql = "SELECT * FROM orders o JOIN payments p ON o.id = p.order_id";
    let select = parse_select(sql);
    let analysis = analyze_join(&select).unwrap().unwrap();

    assert!(!analysis.is_temporal_join());
    assert!(analysis.temporal_probe_schedule.is_none());
}

#[test]
fn test_unqualified_anti_maps_to_left_anti() {
    let sql = "SELECT * FROM orders o ANTI JOIN returns r ON o.id = r.order_id";
    let select = parse_select(sql);
    let analysis = analyze_join(&select).unwrap().unwrap();
    assert_eq!(analysis.join_type, JoinType::LeftAnti);
}

#[test]
fn test_unqualified_semi_maps_to_left_semi() {
    let sql = "SELECT * FROM orders o SEMI JOIN payments p ON o.id = p.order_id";
    let select = parse_select(sql);
    let analysis = analyze_join(&select).unwrap().unwrap();
    assert_eq!(analysis.join_type, JoinType::LeftSemi);
}

#[test]
fn test_composite_join_keys() {
    let sql = "SELECT * FROM orders o \
                    JOIN shipments s \
                    ON o.order_id = s.order_id AND o.region = s.region";
    let select = parse_select(sql);
    let analysis = analyze_join(&select).unwrap().unwrap();

    // First key pair is the primary key
    assert_eq!(analysis.left_key_column, "order_id");
    assert_eq!(analysis.right_key_column, "order_id");

    // Second key pair should be in additional_key_columns
    assert_eq!(
        analysis.additional_key_columns.len(),
        1,
        "Should have 1 additional key pair"
    );
    assert_eq!(analysis.additional_key_columns[0].0, "region");
    assert_eq!(analysis.additional_key_columns[0].1, "region");
}

#[test]
fn test_composite_using_clause() {
    let sql = "SELECT * FROM orders o JOIN shipments s USING (order_id, region)";
    let select = parse_select(sql);
    let analysis = analyze_join(&select).unwrap().unwrap();

    // First column becomes primary key
    assert_eq!(analysis.left_key_column, "order_id");
    assert_eq!(analysis.right_key_column, "order_id");

    // Additional columns
    assert_eq!(
        analysis.additional_key_columns.len(),
        1,
        "USING(order_id, region) should have 1 additional key"
    );
    assert_eq!(analysis.additional_key_columns[0].0, "region");
    assert_eq!(analysis.additional_key_columns[0].1, "region");
}

#[test]
fn test_using_preserves_quoted_key_identity() {
    let sql = "SELECT * FROM orders o JOIN shipments s USING (\"order id\")";
    let analysis = analyze_join(&parse_select(sql)).unwrap().unwrap();

    assert_eq!(analysis.left_key_column, "order id");
    assert_eq!(analysis.right_key_column, "order id");
}
