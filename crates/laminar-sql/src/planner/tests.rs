use super::*;
use crate::parser::StreamingParser;

fn register_temporal_sources(planner: &mut StreamingPlanner) {
    for sql in [
            "CREATE SOURCE trades (symbol VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)",
            "CREATE SOURCE quotes (symbol VARCHAR PRIMARY KEY, quote_time TIMESTAMP, WATERMARK FOR quote_time)",
        ] {
            let statement = StreamingParser::parse_sql(sql).unwrap();
            planner.plan(&statement[0]).unwrap();
        }
}

#[test]
fn test_plan_create_source() {
    let mut planner = StreamingPlanner::new();
    let statements =
        StreamingParser::parse_sql("CREATE SOURCE events (id INT, name VARCHAR)").unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::RegisterSource(info) => {
            assert_eq!(info.name, "events");
        }
        _ => panic!("Expected RegisterSource plan"),
    }
}

#[test]
fn test_plan_create_sink() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql("CREATE SINK output FROM events").unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::RegisterSink(info) => {
            assert_eq!(info.name, "output");
            assert_eq!(info.from, "events");
        }
        _ => panic!("Expected RegisterSink plan"),
    }
}

#[test]
fn test_plan_duplicate_source() {
    let mut planner = StreamingPlanner::new();

    // First source
    let statements =
        StreamingParser::parse_sql("CREATE SOURCE events (id INT, name VARCHAR)").unwrap();
    planner.plan(&statements[0]).unwrap();

    // Duplicate should fail
    let result = planner.plan(&statements[0]);
    assert!(result.is_err());
}

#[test]
fn test_plan_source_if_not_exists() {
    let mut planner = StreamingPlanner::new();

    // First source
    let statements =
        StreamingParser::parse_sql("CREATE SOURCE events (id INT, name VARCHAR)").unwrap();
    planner.plan(&statements[0]).unwrap();

    // IF NOT EXISTS should succeed
    let statements =
        StreamingParser::parse_sql("CREATE SOURCE IF NOT EXISTS events (id INT, name VARCHAR)")
            .unwrap();
    let result = planner.plan(&statements[0]);
    assert!(result.is_ok());
}

#[test]
fn test_plan_source_or_replace() {
    let mut planner = StreamingPlanner::new();

    // First source
    let statements =
        StreamingParser::parse_sql("CREATE SOURCE events (id INT, name VARCHAR)").unwrap();
    planner.plan(&statements[0]).unwrap();

    // OR REPLACE should succeed
    let statements =
        StreamingParser::parse_sql("CREATE OR REPLACE SOURCE events (id INT, name VARCHAR)")
            .unwrap();
    let result = planner.plan(&statements[0]);
    assert!(result.is_ok());
}

#[test]
fn test_plan_source_with_watermark() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "CREATE SOURCE events (
                id INT,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            )",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::RegisterSource(info) => {
            assert_eq!(info.name, "events");
            assert_eq!(info.watermark_column, Some("ts".to_string()));
        }
        _ => panic!("Expected RegisterSource plan"),
    }
}

#[test]
fn test_plan_standard_select() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql("SELECT * FROM events").unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Standard(_) => {}
        _ => panic!("Expected Standard plan for simple SELECT"),
    }
}

#[test]
fn test_list_sources_and_sinks() {
    let mut planner = StreamingPlanner::new();

    // Create sources
    let s1 = StreamingParser::parse_sql("CREATE SOURCE src1 (id INT)").unwrap();
    let s2 = StreamingParser::parse_sql("CREATE SOURCE src2 (id INT)").unwrap();
    planner.plan(&s1[0]).unwrap();
    planner.plan(&s2[0]).unwrap();

    // Create sinks
    let k1 = StreamingParser::parse_sql("CREATE SINK sink1 FROM src1").unwrap();
    planner.plan(&k1[0]).unwrap();

    assert_eq!(planner.list_sources().len(), 2);
    assert_eq!(planner.list_sinks().len(), 1);
    assert!(planner.get_source("src1").is_some());
    assert!(planner.get_sink("sink1").is_some());
}

#[test]
fn test_plan_query_with_window() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT COUNT(*) FROM events GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(query_plan) => {
            assert!(query_plan.window_config.is_some());
            let config = query_plan.window_config.unwrap();
            assert_eq!(config.time_column, "event_time");
            assert_eq!(config.size.as_secs(), 300);
        }
        _ => panic!("Expected Query plan"),
    }
}

#[test]
fn test_plan_query_with_join() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT * FROM orders o JOIN payments p \
             ON o.tenant_id = p.account_id AND o.order_id = p.payment_order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(query_plan) => {
            assert!(query_plan.join_config.is_some());
            let configs = query_plan.join_config.unwrap();
            assert_eq!(configs.len(), 1);
            assert_eq!(configs[0].left_keys(), ["tenant_id", "order_id"]);
            assert_eq!(configs[0].right_keys(), ["account_id", "payment_order_id"]);
        }
        _ => panic!("Expected Query plan"),
    }
}

#[test]
fn test_plan_rejects_unbounded_streaming_join() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "CREATE STREAM joined AS SELECT * FROM orders o JOIN payments p \
             ON o.order_id = p.order_id",
    )
    .unwrap();
    let err = planner.plan(&statements[0]).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("unbounded join"), "got: {msg}");
    assert!(msg.contains("lookup table"), "got: {msg}");
}

#[test]
fn test_plan_rejects_unbounded_standard_select_join() {
    let statements = StreamingParser::parse_sql(
        "SELECT * FROM orders o JOIN payments p ON o.order_id = p.order_id",
    )
    .unwrap();
    assert!(matches!(
        statements.first(),
        Some(StreamingStatement::Standard(_))
    ));

    let error = StreamingPlanner::new().plan(&statements[0]).unwrap_err();
    assert!(error.to_string().contains("unbounded join"));
}

#[test]
fn changelog_enrich_certificate_allows_only_its_join() {
    let statements = StreamingParser::parse_sql(
        "CREATE STREAM joined AS SELECT * FROM orders o JOIN payments p \
             ON o.order_id = p.order_id",
    )
    .unwrap();
    let admission = ChangelogEnrichAdmission::try_new(
        "orders",
        "payments",
        vec!["order_id".into()],
        vec!["order_id".into()],
        false,
    )
    .unwrap();
    StreamingPlanner::new()
        .plan_changelog_enrich(&statements[0], &admission)
        .unwrap();

    let wrong_key = ChangelogEnrichAdmission::try_new(
        "orders",
        "payments",
        vec!["tenant_id".into()],
        vec!["tenant_id".into()],
        false,
    )
    .unwrap();
    let error = StreamingPlanner::new()
        .plan_changelog_enrich(&statements[0], &wrong_key)
        .unwrap_err();
    assert!(error.to_string().contains("unbounded join"));
}

#[test]
fn composite_changelog_enrichment_is_rejected_until_operator_supports_it() {
    let statements = StreamingParser::parse_sql(
        "CREATE STREAM joined AS SELECT * FROM orders o JOIN payments p \
             ON o.tenant_id = p.account_id AND o.order_id = p.payment_order_id",
    )
    .unwrap();
    let exact = ChangelogEnrichAdmission::try_new(
        "orders",
        "payments",
        vec!["tenant_id".into(), "order_id".into()],
        vec!["account_id".into(), "payment_order_id".into()],
        false,
    )
    .unwrap();
    let error = StreamingPlanner::new()
        .plan_changelog_enrich(&statements[0], &exact)
        .unwrap_err();
    assert!(error.to_string().contains("exactly one equality key"));
}

#[test]
fn state_backed_certificate_binds_inner_vs_left() {
    let statements = StreamingParser::parse_sql(
        "CREATE STREAM joined AS SELECT * FROM orders o LEFT JOIN payments p \
             ON o.order_id = p.order_id",
    )
    .unwrap();
    let left = ChangelogEnrichAdmission::try_new(
        "orders",
        "payments",
        vec!["order_id".into()],
        vec!["order_id".into()],
        true,
    )
    .unwrap();
    StreamingPlanner::new()
        .plan_changelog_enrich(&statements[0], &left)
        .unwrap();

    let inner = ChangelogEnrichAdmission::try_new(
        "orders",
        "payments",
        vec!["order_id".into()],
        vec!["order_id".into()],
        false,
    )
    .unwrap();
    assert!(StreamingPlanner::new()
        .plan_changelog_enrich(&statements[0], &inner)
        .is_err());
}

#[test]
fn changelog_enrich_certificate_does_not_relax_right_or_multiway_joins() {
    let admission = ChangelogEnrichAdmission::try_new(
        "orders",
        "payments",
        vec!["order_id".into()],
        vec!["order_id".into()],
        false,
    )
    .unwrap();
    for sql in [
        "CREATE STREAM joined AS SELECT * FROM orders o RIGHT JOIN payments p \
             ON o.order_id = p.order_id",
        "CREATE STREAM joined AS SELECT * FROM orders o JOIN payments p \
             ON o.order_id = p.order_id JOIN refunds r ON p.order_id = r.order_id",
    ] {
        let statements = StreamingParser::parse_sql(sql).unwrap();
        assert!(StreamingPlanner::new()
            .plan_changelog_enrich(&statements[0], &admission)
            .is_err());
    }
}

#[test]
fn test_plan_rejects_implicit_cross_join() {
    let statements = StreamingParser::parse_sql("SELECT * FROM orders, payments").unwrap();
    let error = StreamingPlanner::new().plan(&statements[0]).unwrap_err();
    assert!(error.to_string().contains("implicit multi-source"));
}

#[test]
fn test_plan_allows_unnest_after_one_stream_source() {
    let statements = StreamingParser::parse_sql(
        "SELECT event_id, tag FROM events, UNNEST(make_array('a', 'b')) AS tags(tag)",
    )
    .unwrap();
    StreamingPlanner::new().plan(&statements[0]).unwrap();
}

#[test]
fn test_plan_rejects_unbounded_join_between_windowed_views() {
    let mut planner = StreamingPlanner::new();
    // Two windowed views register as bounded inputs.
    for sql in [
        "CREATE STREAM price_1m AS SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, \
             AVG(p) AS price FROM trades GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
        "CREATE STREAM sent_1m AS SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS bucket, \
             AVG(s) AS ms FROM posts GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
    ] {
        let st = StreamingParser::parse_sql(sql).unwrap();
        planner.plan(&st[0]).unwrap();
    }
    // Closed-window batches are not an ordering/alignment contract between two independent
    // streams. A per-cycle join can silently miss rows when the batches arrive in different
    // cycles, so this shape remains unbounded and must fail closed.
    let st = StreamingParser::parse_sql(
        "CREATE STREAM joined AS SELECT a.bucket, a.price, b.ms \
             FROM price_1m a JOIN sent_1m b ON a.bucket = b.bucket",
    )
    .unwrap();
    let error = planner.plan(&st[0]).unwrap_err();
    assert!(error.to_string().contains("unbounded join"));
}

#[test]
fn test_plan_allows_all_bounded_equi_join_kinds() {
    for (sql_join_type, expected) in [
        ("INNER", JoinType::Inner),
        ("LEFT", JoinType::Left),
        ("RIGHT", JoinType::Right),
        ("FULL", JoinType::Full),
        ("LEFT SEMI", JoinType::LeftSemi),
        ("LEFT ANTI", JoinType::LeftAnti),
        ("RIGHT SEMI", JoinType::RightSemi),
        ("RIGHT ANTI", JoinType::RightAnti),
    ] {
        let sql = format!(
            "SELECT * FROM orders o {sql_join_type} JOIN payments p \
                 ON o.order_id = p.order_id \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR"
        );
        let statements = StreamingParser::parse_sql(&sql).unwrap();
        let plan = StreamingPlanner::new().plan(&statements[0]).unwrap();
        let StreamingPlan::Query(query) = plan else {
            panic!("{sql_join_type} interval join did not produce a query plan");
        };
        let Some(JoinOperatorConfig::StreamStream(config)) =
            query.join_config.and_then(|mut configs| configs.pop())
        else {
            panic!("{sql_join_type} interval join did not produce a stream-join config");
        };
        assert_eq!(config.join_type, expected);
    }
}

#[test]
fn test_plan_rejects_zero_interval_bound() {
    let statements = StreamingParser::parse_sql(
        "SELECT * FROM orders o JOIN payments p ON o.order_id = p.order_id \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '0' SECOND",
    )
    .unwrap();
    let error = StreamingPlanner::new().plan(&statements[0]).unwrap_err();
    assert!(error.to_string().contains("positive finite time bound"));
}

#[test]
fn test_plan_query_with_lag() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT price, LAG(price) OVER (PARTITION BY symbol ORDER BY ts) AS prev FROM trades",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(query_plan) => {
            assert!(query_plan.analytic_config.is_some());
            let config = query_plan.analytic_config.unwrap();
            assert_eq!(config.functions.len(), 1);
            assert_eq!(config.partition_columns, vec!["symbol".to_string()]);
        }
        _ => panic!("Expected Query plan with analytic config"),
    }
}

#[test]
fn test_plan_query_with_having() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT symbol, COUNT(*) AS cnt FROM trades \
             GROUP BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE) \
             HAVING COUNT(*) > 10",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(query_plan) => {
            assert!(query_plan.window_config.is_some());
        }
        _ => panic!("Expected windowed Query plan"),
    }
}

#[test]
fn test_plan_having_only_produces_query_plan() {
    // HAVING without window function still produces a Query plan
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT category, SUM(amount) FROM orders GROUP BY category HAVING SUM(amount) > 1000",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(query_plan) => {
            assert!(query_plan.window_config.is_none());
        }
        _ => panic!("Expected Query plan for HAVING-only query"),
    }
}

#[test]
fn test_plan_query_with_lead() {
    let mut planner = StreamingPlanner::new();
    let statements =
        StreamingParser::parse_sql("SELECT LEAD(price, 2) OVER (ORDER BY ts) AS next2 FROM trades")
            .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(query_plan) => {
            assert!(query_plan.analytic_config.is_some());
            let config = query_plan.analytic_config.unwrap();
            assert!(config.has_lookahead());
            assert_eq!(config.functions[0].offset, 2);
        }
        _ => panic!("Expected Query plan with analytic config"),
    }
}

// -- Multi-way join planner tests --

#[test]
fn test_plan_single_join_produces_vec_of_one() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT * FROM a JOIN b ON a.id = b.a_id \
             AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '1' HOUR",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(qp) => {
            let configs = qp.join_config.unwrap();
            assert_eq!(configs.len(), 1);
        }
        _ => panic!("Expected Query plan"),
    }
}

#[test]
fn test_plan_rejects_multi_way_interval_join() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT * FROM a JOIN b ON a.id = b.a_id \
                 AND b.ts BETWEEN a.ts AND a.ts + INTERVAL '1' HOUR \
             JOIN c ON b.id = c.b_id \
                 AND c.ts BETWEEN b.ts AND b.ts + INTERVAL '1' HOUR",
    )
    .unwrap();

    let error = planner.plan(&statements[0]).unwrap_err();
    assert!(error
        .to_string()
        .contains("explicitly named two-way stages"));
}

#[test]
fn test_plan_rejects_mixed_multi_way_join() {
    let mut planner = StreamingPlanner::new();
    // A lookup-table final step does not make a multi-way operator atomic; users must expose
    // the interval result as a named stage before enriching it.
    let _ = planner.plan(
        &StreamingParser::parse_sql(
            "CREATE LOOKUP TABLE customers (id BIGINT NOT NULL, name VARCHAR, \
                 PRIMARY KEY (id)) WITH (connector = 'parquet', path = '/tmp/x.parquet')",
        )
        .unwrap()[0],
    );
    let statements = StreamingParser::parse_sql(
        "SELECT * FROM orders o \
             JOIN payments p ON o.id = p.order_id \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' HOUR \
             JOIN customers c ON p.cust_id = c.id",
    )
    .unwrap();

    let error = planner.plan(&statements[0]).unwrap_err();
    assert!(error
        .to_string()
        .contains("explicitly named two-way stages"));
}

#[test]
fn test_plan_non_join_query() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql("SELECT * FROM orders").unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Standard(_) => {} // No join → pass-through
        _ => panic!("Expected Standard plan for simple SELECT"),
    }
}

#[test]
fn temporal_as_of_resolves_both_event_time_contracts() {
    let mut planner = StreamingPlanner::new();
    register_temporal_sources(&mut planner);
    let statement = StreamingParser::parse_sql(
        "SELECT t.symbol, q.price FROM trades t \
             JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
             ON t.symbol = q.symbol",
    )
    .unwrap();
    let StreamingPlan::Query(plan) = planner.plan(&statement[0]).unwrap() else {
        panic!("expected temporal query plan");
    };
    let Some(JoinOperatorConfig::Temporal(config)) =
        plan.join_config.and_then(|mut configs| configs.pop())
    else {
        panic!("expected canonical temporal config");
    };
    assert_eq!(config.left_time_column, "trade_time");
    assert_eq!(config.right_time_column, "quote_time");
    assert_eq!(config.join_kind, crate::temporal::TemporalJoinKind::Inner);
    assert_eq!(config.probe_schedule.offsets_ms(), [0]);
}

#[test]
fn temporal_as_of_preserves_composite_key_order_and_requires_exact_primary_key() {
    for (primary_key, accepted) in [
        ("tenant, symbol", true),
        ("symbol, tenant", false),
        ("tenant", false),
    ] {
        let mut planner = StreamingPlanner::new();
        for sql in [
                "CREATE SOURCE trades (tenant VARCHAR, symbol VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)".to_string(),
                format!(
                    "CREATE SOURCE quotes (tenant VARCHAR NOT NULL, symbol VARCHAR NOT NULL, quote_time TIMESTAMP, PRIMARY KEY ({primary_key}), WATERMARK FOR quote_time)"
                ),
            ] {
                let statement = StreamingParser::parse_sql(&sql).unwrap();
                planner.plan(&statement[0]).unwrap();
            }

        let statement = StreamingParser::parse_sql(
            "SELECT * FROM trades t \
                 JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON t.tenant = q.tenant AND t.symbol = q.symbol",
        )
        .unwrap();
        let plan_result = planner.plan(&statement[0]);
        if accepted {
            let StreamingPlan::Query(plan) = plan_result.unwrap() else {
                panic!("expected temporal query plan");
            };
            let Some(JoinOperatorConfig::Temporal(config)) =
                plan.join_config.and_then(|mut configs| configs.pop())
            else {
                panic!("expected canonical temporal config");
            };
            assert_eq!(config.left_key_columns, ["tenant", "symbol"]);
            assert_eq!(config.right_key_columns, ["tenant", "symbol"]);
        } else {
            let error = plan_result.unwrap_err().to_string();
            assert!(error.contains("PRIMARY KEY (tenant, symbol)"), "{error}");
        }
    }
}

#[test]
fn temporal_as_of_rejects_invalid_equality_expressions() {
    for (query, expected) in [
            (
                "SELECT * FROM trades t \
                 JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON LOWER(t.symbol) = q.symbol",
                "qualified column references",
            ),
            (
                "SELECT * FROM trades AS quotes \
                 JOIN quotes FOR SYSTEM_TIME AS OF trades.trade_time AS q \
                 ON quotes.symbol = q.symbol",
                "names both inputs",
            ),
            (
                "SELECT * FROM trades t \
                 JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON t.symbol = q.symbol AND q.quote_time BETWEEN t.trade_time AND t.trade_time + INTERVAL '1' SECOND",
                "additional time-bound",
            ),
        ] {
            let mut planner = StreamingPlanner::new();
            register_temporal_sources(&mut planner);
            let statement = StreamingParser::parse_sql(query).unwrap();
            let error = planner.plan(&statement[0]).unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
        }
}

#[test]
fn temporal_probe_list_uses_the_as_of_config() {
    let mut planner = StreamingPlanner::new();
    for sql in [
            "CREATE SOURCE trades (symbol VARCHAR, venue VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)",
            "CREATE SOURCE quotes (symbol VARCHAR NOT NULL, venue VARCHAR NOT NULL, quote_time TIMESTAMP, PRIMARY KEY (symbol, venue), WATERMARK FOR quote_time)",
        ] {
            let statement = StreamingParser::parse_sql(sql).unwrap();
            planner.plan(&statement[0]).unwrap();
        }
    let statement = StreamingParser::parse_sql(
        "CREATE STREAM markouts AS SELECT probe.offset_ms, probe.probe_time, q.price \
             FROM trades t TEMPORAL PROBE JOIN quotes q ON (symbol, venue) \
             TIMESTAMPS (trade_time, quote_time) LIST (-1s, 0s, 5s) AS probe",
    )
    .unwrap();
    let StreamingPlan::Query(plan) = planner.plan(&statement[0]).unwrap() else {
        panic!("expected temporal probe query plan");
    };
    let Some(JoinOperatorConfig::Temporal(config)) =
        plan.join_config.and_then(|mut configs| configs.pop())
    else {
        panic!("expected canonical temporal config");
    };
    assert_eq!(config.left_key_columns, ["symbol", "venue"]);
    assert_eq!(config.right_key_columns, ["symbol", "venue"]);
    assert_eq!(config.probe_schedule.offsets_ms(), [-1_000, 0, 5_000]);
    assert_eq!(config.probe_alias.as_deref(), Some("probe"));
}

#[test]
fn temporal_join_requires_source_watermarks_and_matching_right_key() {
    for (left, right, expected) in [
            (
                "CREATE SOURCE trades (symbol VARCHAR, trade_time TIMESTAMP)",
                "CREATE SOURCE quotes (symbol VARCHAR PRIMARY KEY, quote_time TIMESTAMP, WATERMARK FOR quote_time)",
                "temporal left source 'trades' must declare WATERMARK",
            ),
            (
                "CREATE SOURCE trades (symbol VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)",
                "CREATE SOURCE quotes (symbol VARCHAR, quote_time TIMESTAMP, WATERMARK FOR quote_time)",
                "must declare PRIMARY KEY (symbol)",
            ),
            (
                "CREATE SOURCE trades (symbol VARCHAR, trade_time TIMESTAMP, WATERMARK FOR trade_time)",
                "CREATE SOURCE quotes (symbol VARCHAR PRIMARY KEY, quote_time TIMESTAMP)",
                "must declare WATERMARK FOR its version column",
            ),
        ] {
            let mut planner = StreamingPlanner::new();
            for sql in [left, right] {
                let statement = StreamingParser::parse_sql(sql).unwrap();
                planner.plan(&statement[0]).unwrap();
            }
            let query = StreamingParser::parse_sql(
                "SELECT * FROM trades t JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON t.symbol = q.symbol",
            )
            .unwrap();
            let error = planner.plan(&query[0]).unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
        }
}

#[test]
fn nested_temporal_versions_fail_closed() {
    let mut planner = StreamingPlanner::new();
    register_temporal_sources(&mut planner);
    let statement = StreamingParser::parse_sql(
        "WITH matched AS (\
                 SELECT t.symbol FROM trades t \
                 JOIN quotes FOR SYSTEM_TIME AS OF t.trade_time AS q \
                 ON t.symbol = q.symbol\
             ) SELECT * FROM matched",
    )
    .unwrap();
    let error = planner.plan(&statement[0]).unwrap_err().to_string();
    assert!(error.contains("nested and set-operation"), "{error}");
}

// -- Window Frame planner tests --

#[test]
fn test_plan_query_with_rows_frame() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT AVG(price) OVER (ORDER BY ts \
             ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma FROM trades",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(qp) => {
            assert!(qp.frame_config.is_some());
            let fc = qp.frame_config.unwrap();
            assert_eq!(fc.functions.len(), 1);
            assert_eq!(fc.functions[0].source_column, "price");
        }
        _ => panic!("Expected Query plan with frame_config"),
    }
}

#[test]
fn test_plan_frame_with_partition() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT AVG(price) OVER (PARTITION BY symbol ORDER BY ts \
             ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma FROM trades",
    )
    .unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Query(qp) => {
            let fc = qp.frame_config.unwrap();
            assert_eq!(fc.partition_columns, vec!["symbol".to_string()]);
            assert_eq!(fc.order_columns, vec!["ts".to_string()]);
        }
        _ => panic!("Expected Query plan with frame_config"),
    }
}

#[test]
fn test_plan_no_frame_is_standard() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql("SELECT * FROM trades").unwrap();

    let plan = planner.plan(&statements[0]).unwrap();
    match plan {
        StreamingPlan::Standard(_) => {} // No frame → pass-through
        _ => panic!("Expected Standard plan for simple SELECT"),
    }
}

#[test]
fn test_plan_unbounded_following_rejected() {
    let mut planner = StreamingPlanner::new();
    let statements = StreamingParser::parse_sql(
        "SELECT SUM(amount) OVER (ORDER BY id \
             ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS rest \
             FROM orders",
    )
    .unwrap();

    let result = planner.plan(&statements[0]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("UNBOUNDED FOLLOWING"), "error was: {err}");
}
