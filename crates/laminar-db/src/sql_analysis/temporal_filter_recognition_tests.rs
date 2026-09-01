use std::sync::Arc;

use datafusion_expr::Volatility;
use laminar_sql::translator::WindowOperatorConfig;

use super::*;

fn cfg(sql: &str) -> TemporalFilterConfig {
    match analyze_temporal_filter(sql) {
        TemporalFilterAnalysis::Recognized(c) => *c,
        TemporalFilterAnalysis::PresentUnrecognized => {
            panic!("expected Recognized, got PresentUnrecognized: {sql}")
        }
        TemporalFilterAnalysis::NotPresent => {
            panic!("expected Recognized, got NotPresent: {sql}")
        }
    }
}

#[test]
fn projection_list_recognised() {
    let c = cfg("SELECT id, amount FROM events WHERE ts > now() - INTERVAL '1' MINUTE");
    assert_eq!(c.proj_cols, vec!["id".to_string(), "amount".to_string()]);
    assert_eq!(c.time_col, "ts");
    // Expression / aliased / qualified projections stay out of scope.
    assert!(matches!(
        analyze_temporal_filter("SELECT id + 1 FROM events WHERE ts > now() - INTERVAL '1' MINUTE"),
        TemporalFilterAnalysis::PresentUnrecognized
    ));
}

#[test]
fn lower_bound_ttl_strict() {
    let c = cfg("SELECT * FROM events WHERE evt > now() - INTERVAL '10' MINUTE");
    assert_eq!(c.source_table, "events");
    assert!(c.proj_cols.is_empty(), "`SELECT *` ⇒ no explicit columns");
    assert_eq!(c.time_col, "evt");
    assert_eq!(
        c.lower,
        Some(TemporalBound {
            off_ms: -600_000,
            strict: true
        })
    );
    assert_eq!(c.upper, None);
}

#[test]
fn between_inclusive_both_bounds() {
    let c = cfg(
        "SELECT * FROM e WHERE ts BETWEEN now() - INTERVAL '2' MINUTE \
         AND now() + INTERVAL '30' SECOND",
    );
    assert_eq!(
        c.lower,
        Some(TemporalBound {
            off_ms: -120_000,
            strict: false
        })
    );
    assert_eq!(
        c.upper,
        Some(TemporalBound {
            off_ms: 30_000,
            strict: false
        })
    );
}

#[test]
fn unrecognised_when_extra_conjunct() {
    assert!(matches!(
        analyze_temporal_filter(
            "SELECT * FROM e WHERE region = 'us' AND ts > now() - INTERVAL '1' MINUTE"
        ),
        TemporalFilterAnalysis::PresentUnrecognized
    ));
}

#[test]
fn not_present_for_ordinary_query() {
    // No false positives — ordinary queries are untouched.
    assert!(matches!(
        analyze_temporal_filter("SELECT * FROM e WHERE region = 'us'"),
        TemporalFilterAnalysis::NotPresent
    ));
}

#[test]
fn wallclock_detection_covers_the_whole_query() {
    for sql in [
        "SELECT region, COUNT(*) FROM e GROUP BY region, now()",
        "SELECT region FROM e ORDER BY current_timestamp",
        "SELECT l.id FROM l JOIN r ON l.ts < now()",
    ] {
        assert!(query_uses_runtime_clock(sql), "{sql}");
        assert!(matches!(
            analyze_temporal_filter(sql),
            TemporalFilterAnalysis::PresentUnrecognized
        ));
    }
    assert!(query_uses_runtime_clock("SELECT proctime() FROM e"));
    assert!(!query_uses_runtime_clock("SELECT \"now\", proctime FROM e"));
}

#[test]
fn cluster_hazards_are_ast_wide_and_quote_safe() {
    for sql in [
        "SELECT \"now\"() FROM e",
        "SELECT current_date() FROM e",
        "SELECT today() FROM e",
        "SELECT current_time() FROM e",
        "SELECT watermark() FROM e",
        "SELECT id FROM e GROUP BY id, \"proctime\"()",
    ] {
        assert!(
            cluster_query_hazards(sql).unwrap().runtime_function,
            "{sql}"
        );
    }
    assert!(
        cluster_query_hazards("SELECT COUNT(\"ai_sentiment\"(text)) FROM e")
            .unwrap()
            .ai_function
    );
    assert!(
        cluster_query_hazards("SELECT COUNT(*) FROM e GROUP BY \"unnest\"(tags)")
            .unwrap()
            .unnest
    );
    assert!(
        cluster_query_hazards("SELECT (SELECT MAX(x) FROM r) FROM e")
            .unwrap()
            .nested_query
    );
}

#[test]
fn managed_core_window_requires_direct_event_time_provenance() {
    let window =
        WindowOperatorConfig::tumbling("ts".to_string(), std::time::Duration::from_secs(60));
    let valid = "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), COUNT(*) FROM events \
                 GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)";
    assert_eq!(
        managed_core_window_source(valid, &window).as_deref(),
        Some("events")
    );
    for sql in [
        "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), COUNT(*) FROM events AS e(other, ts) \
         GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
        "SELECT TUMBLE(payload.ts, INTERVAL '1' MINUTE), COUNT(*) FROM events \
         GROUP BY TUMBLE(payload.ts, INTERVAL '1' MINUTE)",
        "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), COUNT(*) FROM events \
         GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), UNNEST(tags)",
    ] {
        assert!(managed_core_window_source(sql, &window).is_none(), "{sql}");
    }
}

#[test]
fn managed_core_window_certifies_marker_config_and_group_shape() {
    let tumble =
        WindowOperatorConfig::tumbling("ts".to_string(), std::time::Duration::from_secs(60));
    let tumble_offset = tumble.clone().with_offset_ms(1_000);
    let hop = WindowOperatorConfig::sliding(
        "ts".to_string(),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_secs(10),
    );
    let hop_offset = hop.clone().with_offset_ms(1_000);

    for (window, sql) in [
        (
            &tumble,
            "SELECT TUMBLE(ts, INTERVAL '60' SECOND), TUMBLE_END(ts, INTERVAL '1' MINUTE), COUNT(*) \
             FROM events GROUP BY TUMBLE(ts, INTERVAL '60' SECOND), \
             TUMBLE_END(ts, INTERVAL '1' MINUTE)",
        ),
        (
            &hop,
            "SELECT HOP(ts, INTERVAL '10' SECOND, INTERVAL '60' SECOND), \
             HOP_END(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE), COUNT(*) \
             FROM events GROUP BY HOP(ts, INTERVAL '10' SECOND, INTERVAL '60' SECOND), \
             HOP_END(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE)",
        ),
    ] {
        assert_eq!(
            managed_core_window_source(sql, window).as_deref(),
            Some("events"),
            "{sql}"
        );
    }

    for (window, sql) in [
        (
            &tumble,
            "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), TUMBLE_END(other_ts, INTERVAL '1' MINUTE), COUNT(*) \
             FROM events GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), \
             TUMBLE_END(other_ts, INTERVAL '1' MINUTE)",
        ),
        (
            &tumble,
            "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), TUMBLE_END(ts, INTERVAL '5' MINUTE), COUNT(*) \
             FROM events GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), \
             TUMBLE_END(ts, INTERVAL '5' MINUTE)",
        ),
        (
            &tumble_offset,
            "SELECT TUMBLE(ts, INTERVAL '1' MINUTE, INTERVAL '1' SECOND), \
             TUMBLE_END(ts, INTERVAL '1' MINUTE, INTERVAL '2' SECOND), COUNT(*) \
             FROM events GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE, INTERVAL '1' SECOND), \
             TUMBLE_END(ts, INTERVAL '1' MINUTE, INTERVAL '2' SECOND)",
        ),
        (
            &hop,
            "SELECT HOP(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE), \
             HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '1' MINUTE), COUNT(*) \
             FROM events GROUP BY HOP(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE), \
             HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '1' MINUTE)",
        ),
        (
            &hop,
            "SELECT HOP(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE), \
             HOP_END(ts, INTERVAL '10' SECOND, INTERVAL '2' MINUTE), COUNT(*) \
             FROM events GROUP BY HOP(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE), \
             HOP_END(ts, INTERVAL '10' SECOND, INTERVAL '2' MINUTE)",
        ),
        (
            &hop_offset,
            "SELECT HOP(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE, INTERVAL '1' SECOND), \
             HOP_END(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE, INTERVAL '2' SECOND), COUNT(*) \
             FROM events GROUP BY HOP(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE, INTERVAL '1' SECOND), \
             HOP_END(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE, INTERVAL '2' SECOND)",
        ),
        (
            &tumble,
            "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), COUNT(*) FROM events \
             GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE) WITH ROLLUP",
        ),
        (
            &tumble,
            "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), COUNT(*) FROM events \
             GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), \
             CUMULATE(ts, INTERVAL '10' SECOND, INTERVAL '1' MINUTE)",
        ),
    ] {
        assert!(managed_core_window_source(sql, window).is_none(), "{sql}");
    }
}

#[tokio::test]
async fn planned_functions_must_be_replay_immutable() {
    use arrow::datatypes::DataType;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};

    let ctx = SessionContext::new();
    let implementation: ScalarFunctionImplementation = Arc::new(|_| {
        Ok(ColumnarValue::Scalar(
            datafusion_common::ScalarValue::Int64(Some(1)),
        ))
    });
    ctx.register_udf(datafusion_expr::expr_fn::create_udf(
        "custom_volatile",
        Vec::new(),
        DataType::Int64,
        Volatility::Volatile,
        implementation,
    ));

    for sql in [
        "SELECT random()",
        "SELECT uuid()",
        "SELECT custom_volatile()",
    ] {
        let dataframe = ctx.sql(sql).await.unwrap();
        assert!(
            !planned_functions_are_immutable(dataframe.logical_plan()),
            "{sql}"
        );
    }
    let dataframe = ctx.sql("SELECT abs(-1)").await.unwrap();
    assert!(planned_functions_are_immutable(dataframe.logical_plan()));
}
