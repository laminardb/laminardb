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
        window_config: windowed.then(|| {
            laminar_sql::translator::WindowOperatorConfig::tumbling(
                "event_time".into(),
                Duration::from_secs(60),
            )
        }),
        order_config: None,
        join_config: None,
        has_analytic: false,
        has_frame: false,
        incremental: false,
        subscription_output: None,
        catalog_generation: 1,
        subscription_certificate: None,
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

    let out = resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
        .await
        .unwrap();
    let names: Vec<&str> = out.schemas["agg"]
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

    let out = resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
        .await
        .unwrap();
    let names: Vec<&str> = out.schemas["agg"]
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["window_start", "window_end", "region", "n"]);
    assert_eq!(
        out.schemas["agg"].field(0).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    );
    assert_eq!(
        out.schemas["agg"].field(1).data_type(),
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

    let out = resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
        .await
        .unwrap();
    let names: Vec<&str> = out.schemas["passthrough"]
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

    let out = resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
        .await
        .unwrap();
    let b_names: Vec<&str> = out.schemas["b"]
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

    let out = resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
        .await
        .unwrap();
    assert!(out.schemas.contains_key("Foo"));
    assert!(out.schemas.contains_key("foo"));
}

#[tokio::test]
async fn changelog_schema_tracks_real_emitters_and_safe_projection() {
    use laminar_core::changelog::WEIGHT_COLUMN;
    use laminar_sql::parser::EmitClause;

    let ctx = ctx_with_payments();
    let mut changes = reg(
        "changes",
        "SELECT region, SUM(amount_usd) AS total FROM payments GROUP BY region",
        false,
    );
    changes.emit_clause = Some(EmitClause::Changes);
    let mut stateless = reg(
        "stateless",
        "SELECT region, amount_usd FROM payments",
        false,
    );
    stateless.emit_clause = Some(EmitClause::Changes);

    let mut regs = std::collections::HashMap::new();
    regs.insert(changes.name.clone(), changes);
    regs.insert(
        "projected".to_string(),
        reg(
            "projected",
            "SELECT region, total FROM changes WHERE total > 0.0",
            false,
        ),
    );
    regs.insert(stateless.name.clone(), stateless);

    let out = resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
        .await
        .unwrap();

    for name in ["changes", "projected"] {
        assert!(out.changelog_carrying.contains(name));
        let field = out.schemas[name].field_with_name(WEIGHT_COLUMN).unwrap();
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(!field.is_nullable());
    }
    assert!(!out.changelog_carrying.contains("stateless"));
    assert!(out.schemas["stateless"]
        .field_with_name(WEIGHT_COLUMN)
        .is_err());
}

#[tokio::test]
async fn plain_stream_cannot_spoof_engine_weight() {
    let ctx = ctx_with_payments();
    let regs = std::collections::HashMap::from([(
        "spoofed".to_string(),
        reg(
            "spoofed",
            "SELECT region, CAST(1 AS BIGINT) AS __WEIGHT FROM payments",
            false,
        ),
    )]);

    let error =
        resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
            .await
            .unwrap_err()
            .to_string();
    assert!(
        error.contains("not a certified changelog producer")
            && error.contains("reserved engine-owned"),
        "{error}"
    );
}

#[tokio::test]
async fn ordered_changelog_enrich_uses_current_provenance_and_reserves_static_metadata() {
    use crate::operator::interval_join_input::BoundedJoinInputMode;

    let ctx = ctx_with_payments();
    ctx.register_table(
        "dimensions",
        Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
        ])))),
    )
    .unwrap();
    let changes = reg("changes", "SELECT region, amount_usd FROM payments", false);
    let enriched = reg(
        "enriched",
        "SELECT d.category, c.* FROM changes c JOIN dimensions d ON c.region = d.region",
        false,
    );
    let regs = std::collections::HashMap::from([
        (changes.name.clone(), changes.clone()),
        (enriched.name.clone(), enriched),
    ]);
    let reference_tables = rustc_hash::FxHashSet::from_iter(["dimensions".to_string()]);
    let ordered = rustc_hash::FxHashMap::from_iter([(
        "changes".to_string(),
        [
            BoundedJoinInputMode::AppendOnly,
            BoundedJoinInputMode::FullChangelog,
        ],
    )]);

    let resolved = resolve_stream_output_schemas(&ctx, &regs, &reference_tables, &ordered)
        .await
        .unwrap();
    assert!(resolved.changelog_carrying.contains("enriched"));
    assert_eq!(
        resolved.schemas["enriched"].fields().last().unwrap().name(),
        laminar_core::changelog::WEIGHT_COLUMN
    );

    let analytic_regs = std::collections::HashMap::from([
        (changes.name.clone(), changes.clone()),
        (
            "analytic_enrich".to_string(),
            reg(
                "analytic_enrich",
                "SELECT c.region, ROW_NUMBER() OVER (ORDER BY c.region) AS row_num \
                 FROM changes c JOIN dimensions d ON c.region = d.region",
                false,
            ),
        ),
    ]);
    let error = resolve_stream_output_schemas(&ctx, &analytic_regs, &reference_tables, &ordered)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("cannot safely consume a changelog"),
        "{error}"
    );

    let ordered_aggregate_regs = std::collections::HashMap::from([
        (changes.name.clone(), changes.clone()),
        (
            "top_aggregate".to_string(),
            reg(
                "top_aggregate",
                "SELECT region, SUM(amount_usd) AS total FROM changes GROUP BY region \
                 ORDER BY total DESC LIMIT 1",
                false,
            ),
        ),
    ]);
    let error =
        resolve_stream_output_schemas(&ctx, &ordered_aggregate_regs, &reference_tables, &ordered)
            .await
            .unwrap_err()
            .to_string();
    assert!(
        error.contains("ordering or row limits")
            || error.contains("managed streaming aggregates require"),
        "{error}"
    );

    let aggregate_enrich_regs = std::collections::HashMap::from([
        (changes.name.clone(), changes.clone()),
        (
            "aggregate_enrich".to_string(),
            reg(
                "aggregate_enrich",
                "SELECT COUNT(c.region) AS row_count FROM changes c \
                 JOIN dimensions d ON c.region = d.region",
                false,
            ),
        ),
    ]);
    let error =
        resolve_stream_output_schemas(&ctx, &aggregate_enrich_regs, &reference_tables, &ordered)
            .await
            .unwrap_err()
            .to_string();
    assert!(
        (error.contains("aggregate state") && error.contains("changelog enrichment"))
            || error.contains("aggregate could not be certified"),
        "{error}"
    );

    ctx.register_table(
        "bad_dimensions",
        Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("__WEIGHT", DataType::Int64, false),
        ])))),
    )
    .unwrap();
    let bad_regs = std::collections::HashMap::from([
        (changes.name.clone(), changes),
        (
            "bad_enriched".to_string(),
            reg(
                "bad_enriched",
                "SELECT d.*, c.* FROM changes c JOIN bad_dimensions d ON c.region = d.region",
                false,
            ),
        ),
    ]);
    let bad_reference_tables = rustc_hash::FxHashSet::from_iter(["bad_dimensions".to_string()]);
    let error = resolve_stream_output_schemas(&ctx, &bad_regs, &bad_reference_tables, &ordered)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("static enrich table") && error.contains("reserved"),
        "{error}"
    );
}

#[tokio::test]
async fn ambiguous_changelog_consumer_fails_closed() {
    use laminar_sql::parser::EmitClause;

    let ctx = ctx_with_payments();
    let mut changes = reg(
        "changes",
        "SELECT region, SUM(amount_usd) AS total FROM payments GROUP BY region",
        false,
    );
    changes.emit_clause = Some(EmitClause::Changes);

    let mut regs = std::collections::HashMap::new();
    regs.insert(changes.name.clone(), changes);
    regs.insert(
        "joined".to_string(),
        reg(
            "joined",
            "SELECT c.region, c.total, p.method FROM changes c \
             JOIN payments p ON c.region = p.region",
            false,
        ),
    );

    let error =
        resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
            .await
            .unwrap_err()
            .to_string();
    assert!(
        error.contains("cannot safely consume a changelog"),
        "{error}"
    );
}

#[tokio::test]
async fn volatile_changelog_consumer_fails_replay_admission() {
    use laminar_sql::parser::EmitClause;

    let ctx = ctx_with_payments();
    let mut changes = reg(
        "changes",
        "SELECT region, SUM(amount_usd) AS total FROM payments GROUP BY region",
        false,
    );
    changes.emit_clause = Some(EmitClause::Changes);
    let mut regs = std::collections::HashMap::new();
    regs.insert(changes.name.clone(), changes);
    regs.insert(
        "volatile_projection".to_string(),
        reg(
            "volatile_projection",
            "SELECT region, random() AS sample FROM changes",
            false,
        ),
    );

    let error =
        resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
            .await
            .unwrap_err()
            .to_string();
    assert!(error.contains("not replay-immutable"), "{error}");
}

#[tokio::test]
async fn wildcard_modifier_changelog_projection_fails_startup_admission() {
    use laminar_sql::parser::EmitClause;

    let ctx = ctx_with_payments();
    let mut changes = reg(
        "changes",
        "SELECT region, SUM(amount_usd) AS total FROM payments GROUP BY region",
        false,
    );
    changes.emit_clause = Some(EmitClause::Changes);
    let mut regs = std::collections::HashMap::new();
    regs.insert(changes.name.clone(), changes);
    regs.insert(
        "modified_wildcard".to_string(),
        reg(
            "modified_wildcard",
            "SELECT * EXCLUDE(total) FROM changes",
            false,
        ),
    );

    let error =
        resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
            .await
            .unwrap_err()
            .to_string();
    assert!(
        error.contains("cannot preserve")
            || error.contains("engine-owned")
            || error.contains("ordering or row limits"),
        "{error}"
    );
}

#[tokio::test]
async fn windowed_changelog_consumer_fails_closed() {
    use laminar_sql::parser::EmitClause;

    let ctx = ctx_with_payments();
    let mut changes = reg(
        "changes",
        "SELECT region, event_time, SUM(amount_usd) AS total FROM payments \
         GROUP BY region, event_time",
        false,
    );
    changes.emit_clause = Some(EmitClause::Changes);

    let mut regs = std::collections::HashMap::new();
    regs.insert(changes.name.clone(), changes);
    regs.insert(
        "windowed".to_string(),
        reg(
            "windowed",
            "SELECT region, COUNT(*) AS n FROM changes \
             GROUP BY tumble(event_time, INTERVAL '1' MINUTE), region",
            true,
        ),
    );

    let error =
        resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
            .await
            .unwrap_err()
            .to_string();
    assert!(
        error.contains("cannot safely consume a changelog") && error.contains("window"),
        "{error}"
    );

    let mut unsupported = std::collections::HashMap::new();
    unsupported.insert(
        "distinct".to_string(),
        reg(
            "distinct",
            "SELECT region, COUNT(DISTINCT method) AS n FROM payments GROUP BY region",
            false,
        ),
    );
    let error =
        resolve_stream_output_schemas(&ctx, &unsupported, &Default::default(), &Default::default())
            .await
            .unwrap_err()
            .to_string();
    assert!(error.contains("DISTINCT aggregates"), "{error}");
}

#[tokio::test]
async fn unresolvable_streams_surface_planner_error() {
    let ctx = ctx_with_payments();
    let mut regs = std::collections::HashMap::new();
    // Cycle: a→b, b→a. Planning stalls; we report the unresolved set.
    regs.insert("a".to_string(), reg("a", "SELECT * FROM b", false));
    regs.insert("b".to_string(), reg("b", "SELECT * FROM a", false));

    let err = resolve_stream_output_schemas(&ctx, &regs, &Default::default(), &Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("unresolvable stream dependency"), "got: {err}");
    assert!(err.contains('a') && err.contains('b'), "got: {err}");
}
