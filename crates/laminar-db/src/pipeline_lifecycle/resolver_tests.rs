use super::resolve_stream_output_schemas;
use crate::connector_manager::StreamRegistration;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use std::time::Duration;

fn ctx_with_payments() -> SessionContext {
    let ctx = SessionContext::new_with_config(laminar_sql::datafusion::base_session_config());
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("method", DataType::Utf8, false),
        Field::new("amount_usd", DataType::Float64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    ctx.register_table("payments", Arc::new(EmptyTable::new(schema)))
        .unwrap();
    ctx.register_udf(datafusion_expr::ScalarUDF::from(
        laminar_sql::datafusion::TumbleWindowStart::new(),
    ));
    ctx
}

fn reg(name: &str, sql: &str, windowed: bool) -> StreamRegistration {
    StreamRegistration {
        name: name.to_string(),
        query_sql: sql.to_string(),
        emit_clause: None,
        // Resolver only checks `is_some()`; the size doesn't matter.
        window_config: windowed.then(|| {
            laminar_sql::translator::WindowOperatorConfig::tumbling(
                "event_time".into(),
                Duration::ZERO,
            )
        }),
        order_config: None,
        join_config: None,
        has_analytic: false,
        has_frame: false,
        incremental: false,
    }
}

#[tokio::test]
async fn windowed_stream_schema_matches_user_select() {
    let ctx = ctx_with_payments();
    let mut regs = std::collections::HashMap::new();
    regs.insert(
        "agg".to_string(),
        reg(
            "agg",
            "SELECT region, COUNT(*) AS n FROM payments \
             GROUP BY tumble(event_time, INTERVAL '1' MINUTE), region",
            true,
        ),
    );

    let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
    let names: Vec<&str> = out["agg"]
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["region", "n"]);
}

#[tokio::test]
async fn windowed_stream_with_explicit_window_columns() {
    let ctx = ctx_with_payments();
    ctx.register_udf(datafusion_expr::ScalarUDF::from(
        laminar_sql::datafusion::TumbleWindowEnd::new(),
    ));
    let mut regs = std::collections::HashMap::new();
    regs.insert(
        "agg".to_string(),
        reg(
            "agg",
            "SELECT \
                tumble(event_time, INTERVAL '1' MINUTE)     AS window_start, \
                tumble_end(event_time, INTERVAL '1' MINUTE) AS window_end, \
                region, \
                COUNT(*) AS n \
             FROM payments \
             GROUP BY \
                tumble(event_time, INTERVAL '1' MINUTE), \
                tumble_end(event_time, INTERVAL '1' MINUTE), \
                region",
            true,
        ),
    );

    let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
    let names: Vec<&str> = out["agg"]
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["window_start", "window_end", "region", "n"]);
    assert_eq!(
        out["agg"].field(0).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    );
    assert_eq!(
        out["agg"].field(1).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    );
}

#[tokio::test]
async fn non_windowed_stream_has_no_prefix() {
    let ctx = ctx_with_payments();
    let mut regs = std::collections::HashMap::new();
    regs.insert(
        "passthrough".to_string(),
        reg(
            "passthrough",
            "SELECT region, amount_usd FROM payments",
            false,
        ),
    );

    let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
    let names: Vec<&str> = out["passthrough"]
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["region", "amount_usd"]);
}

#[tokio::test]
async fn chained_streams_resolve_via_iterative_planning() {
    // `b` reads from `a`; iteration order doesn't matter — the loop
    // re-tries `b` after `a` is registered.
    let ctx = ctx_with_payments();
    let mut regs = std::collections::HashMap::new();
    regs.insert(
        "b".to_string(),
        reg("b", "SELECT region, n + 1 AS n_plus_one FROM a", false),
    );
    regs.insert(
        "a".to_string(),
        reg(
            "a",
            "SELECT region, COUNT(*) AS n FROM payments GROUP BY region",
            false,
        ),
    );

    let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
    let b_names: Vec<&str> = out["b"]
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(b_names, vec!["region", "n_plus_one"]);

    // Placeholders must not leak into the public ctx — `subscribe()`
    // is the data path for streams; `SELECT * FROM <stream>` should
    // not silently return zero rows from a left-over EmptyTable.
    assert!(!ctx.table_exist("a").unwrap_or(false));
    assert!(!ctx.table_exist("b").unwrap_or(false));
}

#[tokio::test]
async fn case_distinct_chained_streams_resolve_exactly() {
    let ctx = ctx_with_payments();
    let mut regs = std::collections::HashMap::new();
    regs.insert(
        "foo".to_string(),
        reg("foo", "SELECT region, n FROM Foo", false),
    );
    regs.insert(
        "Foo".to_string(),
        reg(
            "Foo",
            "SELECT region, COUNT(*) AS n FROM payments GROUP BY region",
            false,
        ),
    );

    let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
    assert!(out.contains_key("Foo"));
    assert!(out.contains_key("foo"));
}

#[tokio::test]
async fn unresolvable_streams_surface_planner_error() {
    let ctx = ctx_with_payments();
    let mut regs = std::collections::HashMap::new();
    // Cycle: a→b, b→a. Planning stalls; we report the unresolved set.
    regs.insert("a".to_string(), reg("a", "SELECT * FROM b", false));
    regs.insert("b".to_string(), reg("b", "SELECT * FROM a", false));

    let err = resolve_stream_output_schemas(&ctx, &regs)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("unresolvable stream dependency"), "got: {err}");
    assert!(err.contains('a') && err.contains('b'), "got: {err}");
}
