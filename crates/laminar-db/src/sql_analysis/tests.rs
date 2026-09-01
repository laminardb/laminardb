use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use laminar_sql::translator::TemporalJoinTranslatorConfig;
use rustc_hash::{FxHashMap, FxHashSet};

use super::table_refs::is_window_tvf;
use super::*;

#[test]
fn weighted_projection_rewrite_is_ast_owned_and_wildcard_safe() {
    let rewritten =
        projection_sql_preserving_weight("SELECT value + 1 AS adjusted FROM changes WHERE id > 1")
            .unwrap();
    assert!(rewritten.contains(", __weight AS __weight FROM changes WHERE"));
    assert_eq!(rewritten.matches("__weight").count(), 2);

    for wildcard in [
        "SELECT * FROM changes WHERE id > 1",
        "SELECT c.* FROM changes AS c WHERE id > 1",
        "SELECT id AS copy, * FROM changes WHERE id > 1",
    ] {
        assert_eq!(
            projection_sql_preserving_weight(wildcard).as_deref(),
            Some(wildcard)
        );
    }
    for noncanonical in [
        "SELECT *, id AS copy FROM changes WHERE id > 1",
        "SELECT c.*, id AS copy FROM changes AS c WHERE id > 1",
        "SELECT *, c.* FROM changes AS c WHERE id > 1",
    ] {
        assert!(projection_sql_preserving_weight(noncanonical).is_none());
    }
    assert!(
        projection_sql_preserving_weight("SELECT value AS __weight FROM changes WHERE id > 1")
            .is_none()
    );
    assert!(
        projection_sql_preserving_weight("SELECT value FROM changes WHERE __WEIGHT > 0").is_none()
    );
}

#[test]
fn sink_predicate_weight_reference_is_case_insensitive_and_fail_closed() {
    for predicate in ["__weight > 0", "row.__WEIGHT < 0", "(__Weight) = 1"] {
        assert!(predicate_references_weight(predicate), "{predicate}");
    }
    assert!(!predicate_references_weight(
        "value > 0 AND note = '__weight'"
    ));
    assert!(predicate_references_weight("("));
}

#[test]
fn mutable_changelog_modifier_scan_rejects_row_set_and_ordering_semantics() {
    assert!(!mutable_changelog_has_unsafe_modifiers(
        "SELECT l.id AS id FROM left_events l JOIN right_events r ON l.id = r.id WHERE l.id > 0"
    ));
    for query in [
        "SELECT DISTINCT l.id AS id FROM left_events l JOIN right_events r ON l.id = r.id",
        "SELECT l.id AS id FROM left_events l JOIN right_events r ON l.id = r.id ORDER BY id LIMIT 1",
        "SELECT l.id AS id FROM left_events l JOIN right_events r ON l.id = r.id FETCH FIRST 1 ROW ONLY",
        "SELECT ROW_NUMBER() OVER (ORDER BY l.id) AS row_num FROM left_events l JOIN right_events r ON l.id = r.id",
        "SELECT l.id AS id, EXISTS (SELECT 1 FROM other_events o WHERE o.id = l.id) AS present FROM left_events l JOIN right_events r ON l.id = r.id",
        "(",
    ] {
        assert!(mutable_changelog_has_unsafe_modifiers(query), "{query}");
    }
}

#[test]
fn query_weight_reference_scan_allows_only_implicit_wildcards() {
    assert!(!query_references_weight(
        "SELECT l.* FROM left_events l JOIN right_events r ON l.id = r.id"
    ));
    for query in [
        "SELECT l.__weight AS weight_copy FROM left_events l JOIN right_events r ON l.id = r.id",
        "SELECT l.id AS __WEIGHT FROM left_events l JOIN right_events r ON l.id = r.id",
        "SELECT l.id FROM left_events l JOIN right_events r ON l.id = r.id WHERE r.__Weight > 0",
        "(",
    ] {
        assert!(query_references_weight(query), "{query}");
    }
}

#[test]
fn changelog_enrich_static_wildcard_still_selects_the_left_weight() {
    let incremental = FxHashSet::from_iter(["changes".to_string()]);
    let static_tables = FxHashSet::from_iter(["dimension".to_string()]);
    let detected = detect_changelog_enrich_query(
        "SELECT d.* FROM changes c JOIN dimension d ON c.id = d.id",
        &incremental,
        &static_tables,
    )
    .unwrap();

    assert!(detected.projection_sql.contains("d.*, c.\"__weight\""));
    for unsupported in [
        "SELECT * FROM changes c JOIN dimension d ON c.id = d.id",
        "SELECT c.*, d.name FROM changes c JOIN dimension d ON c.id = d.id",
        "SELECT d.* EXCLUDE (name) FROM changes c JOIN dimension d ON c.id = d.id",
        "SELECT d.name AS __WEIGHT FROM changes c JOIN dimension d ON c.id = d.id",
        "SELECT c.__weight AS copied FROM changes c JOIN dimension d ON c.id = d.id",
        "SELECT \"c\".* FROM changes c JOIN dimension d ON c.id = d.id",
        "SELECT d.name FROM changes AS \"left input\" JOIN dimension d ON \"left input\".id = d.id",
        "SELECT \"C\".* FROM changes AS \"c\" JOIN dimension AS \"C\" ON \"c\".id = \"C\".id",
        "SELECT c.id AS id, ROW_NUMBER() OVER (ORDER BY c.id) AS row_num FROM changes c JOIN dimension d ON c.id = d.id",
        "SELECT TOP 1 c.id AS id FROM changes c JOIN dimension d ON c.id = d.id",
        "SELECT c.id AS id FROM changes c JOIN dimension d ON c.id = d.id QUALIFY ROW_NUMBER() OVER (ORDER BY c.id) = 1",
        "SELECT c.id AS id, EXISTS (SELECT 1 FROM other_stream o WHERE o.id = c.id) AS present FROM changes c JOIN dimension d ON c.id = d.id",
    ] {
        assert!(detect_changelog_enrich_query(unsupported, &incremental, &static_tables).is_none());
    }

    let left = detect_changelog_enrich_query(
        "SELECT d.name, c.* FROM changes c JOIN dimension d ON c.id = d.id",
        &incremental,
        &static_tables,
    )
    .unwrap();
    assert!(!left.projection_sql.contains("c.\"__weight\""));
}

fn lookup_fixtures() -> (FxHashMap<String, Vec<String>>, FxHashMap<String, SchemaRef>) {
    use arrow::datatypes::{DataType, Field, Schema};
    let mut partial = FxHashMap::default();
    partial.insert(
        "customers".to_string(),
        vec!["id".to_string(), "name".to_string()],
    );
    let mut schemas = FxHashMap::default();
    schemas.insert(
        "orders".to_string(),
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ])) as SchemaRef,
    );
    (partial, schemas)
}

fn dim_schema() -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
    ]))
}

#[test]
fn lookup_projection_unions_referenced_columns_plus_key() {
    let pk = vec!["id".to_string()];
    // q1 selects dim.name; q2 filters on dim.region. Union + key = id,name,region.
    let q1 = "SELECT o.x, d.name FROM orders o JOIN dim d ON o.k = d.id";
    let q2 = "SELECT o.y FROM orders o JOIN dim d ON o.k = d.id WHERE d.region = 'US'";
    let proj = compute_lookup_projection(&dim_schema(), &pk, "dim", [q1, q2]);
    assert_eq!(
        proj,
        vec![0, 1, 3],
        "id(0)+name(1)+region(3); email(2) dropped"
    );
}

#[test]
fn lookup_projection_bails_to_full_on_wildcard() {
    let pk = vec!["id".to_string()];
    let q = "SELECT * FROM orders o JOIN dim d ON o.k = d.id";
    assert!(
        compute_lookup_projection(&dim_schema(), &pk, "dim", [q]).is_empty(),
        "a wildcard references every column, so fetch all"
    );
}

#[test]
fn lookup_projection_empty_when_all_columns_used() {
    let pk = vec!["id".to_string()];
    let q = "SELECT d.name, d.email, d.region FROM orders o JOIN dim d ON o.k = d.id";
    assert!(
        compute_lookup_projection(&dim_schema(), &pk, "dim", [q]).is_empty(),
        "full coverage collapses to empty (= fetch all)"
    );
}

#[test]
fn lookup_enrich_detects_and_rewrites() {
    let (partial, schemas) = lookup_fixtures();
    let sql = "SELECT o.order_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id";
    let (cfg, proj) = detect_lookup_enrich_query(sql, &partial, &schemas);
    let cfg = cfg.expect("partial lookup join should be detected");
    assert_eq!(cfg.table_name, "customers");
    assert_eq!(cfg.key_columns, vec!["customer_id".to_string()]);
    let proj = proj.unwrap();
    assert!(proj.contains("__lookup_enrich_tmp"));
    // Qualifiers stripped; `c.name` collides with stream `name` → suffixed.
    assert!(proj.contains("order_id"));
    assert!(
        proj.contains("name_customers"),
        "collision not suffixed: {proj}"
    );
    assert!(
        !proj.contains("o."),
        "stream qualifier not stripped: {proj}"
    );
    assert!(
        !proj.contains("c."),
        "lookup qualifier not stripped: {proj}"
    );
}

#[test]
fn lookup_enrich_rewrites_where_clause() {
    let (partial, schemas) = lookup_fixtures();
    let sql = "SELECT o.order_id FROM orders o JOIN customers c ON o.customer_id = c.id \
               WHERE c.name = 'vip'";
    let (_, proj) = detect_lookup_enrich_query(sql, &partial, &schemas);
    let proj = proj.unwrap();
    // `c.name` in WHERE rewrites to the suffixed flattened column.
    assert!(proj.contains("WHERE name_customers = 'vip'"), "{proj}");
}

#[test]
fn lookup_enrich_skips_non_partial_table() {
    let (_partial, schemas) = lookup_fixtures();
    let empty = FxHashMap::default();
    let sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id";
    let (cfg, _) = detect_lookup_enrich_query(sql, &empty, &schemas);
    assert!(
        cfg.is_none(),
        "no partial tables → must fall through to DataFusion"
    );
}

#[test]
fn extract_table_refs_plain() {
    let refs = extract_table_references("SELECT * FROM events WHERE id > 1");
    assert_eq!(refs.len(), 1);
    assert!(refs.contains("events"));

    let refs = extract_table_references(
        "WITH hidden AS (SELECT * FROM right_events) SELECT * FROM hidden",
    );
    assert_eq!(refs, FxHashSet::from_iter(["right_events".to_string()]));
}

#[test]
fn extract_table_refs_tumble_in_from() {
    let refs = extract_table_references(
        "SELECT COUNT(*) FROM TUMBLE(events, ts, INTERVAL '10' SECOND) \
         GROUP BY window_start",
    );
    assert_eq!(refs.len(), 1);
    assert!(refs.contains("events"), "got {refs:?}");
}

#[test]
fn extract_table_refs_tumble_join() {
    let refs = extract_table_references(
        "SELECT * FROM TUMBLE(events, ts, INTERVAL '1' MINUTE) e \
         JOIN dim ON e.key = dim.key",
    );
    assert!(refs.contains("events"), "got {refs:?}");
    assert!(refs.contains("dim"), "got {refs:?}");
}

#[test]
fn inline_unnest_is_not_a_second_stream_or_join() {
    let sql = "SELECT event_id, tag FROM events, \
               UNNEST(make_array('a', 'b')) AS tags(tag)";
    let refs = extract_table_references(sql);
    assert_eq!(refs, FxHashSet::from_iter(["events".to_string()]));
    assert_eq!(join_clause_count(sql), 0);
    assert_eq!(single_source_table(sql), None);
}

#[test]
fn single_source_tumble() {
    let name = single_source_table("SELECT COUNT(*) FROM TUMBLE(trades, ts, INTERVAL '5' SECOND)");
    assert_eq!(name.as_deref(), Some("trades"));
}

#[test]
fn is_window_tvf_case_insensitive() {
    assert!(is_window_tvf("TUMBLE"));
    assert!(is_window_tvf("tumble"));
    assert!(is_window_tvf("Hop"));
    assert!(is_window_tvf("SESSION"));
    assert!(!is_window_tvf("my_func"));
}

fn temporal_projection_config() -> TemporalJoinTranslatorConfig {
    TemporalJoinTranslatorConfig {
        left_table: "trades".into(),
        right_table: "quotes".into(),
        left_key_columns: vec!["symbol".into()],
        right_key_columns: vec!["symbol".into()],
        left_time_column: "trade_time".into(),
        right_time_column: "quote_time".into(),
        join_kind: laminar_sql::temporal::TemporalJoinKind::Left,
        probe_schedule: laminar_sql::temporal::TemporalProbeSchedule::as_of(),
        probe_alias: None,
    }
}

const TEMPORAL_FROM: &str = "FROM trades t LEFT JOIN quotes \
    FOR SYSTEM_TIME AS OF t.trade_time AS q ON t.symbol = q.symbol";

#[test]
fn temporal_projection_rewrites_supported_scalar_select_and_filter() {
    let sql = format!(
        "SELECT t.trade_id AS id, q.price * 2 AS doubled {TEMPORAL_FROM} \
         WHERE q.price > 0 AND t.trade_id IN (1, 2)"
    );
    let projection = temporal_projection_sql(&sql, &temporal_projection_config()).unwrap();
    assert_eq!(
        projection,
        "SELECT trade_id AS id, price_quotes * 2 AS doubled FROM __temporal_tmp AS \
         __temporal_projection_input \
         WHERE price_quotes > 0 AND trade_id IN (1, 2)"
    );

    let mut probe_config = temporal_projection_config();
    probe_config.probe_schedule =
        laminar_sql::temporal::TemporalProbeSchedule::list(vec![5_000, 15_000]).unwrap();
    probe_config.probe_alias = Some("probe".into());
    let probe_sql = format!("SELECT probe.offset_ms, probe.probe_time, q.price {TEMPORAL_FROM}");
    assert_eq!(
        temporal_projection_sql(&probe_sql, &probe_config).unwrap(),
        "SELECT offset_ms, probe_time, price_quotes FROM __temporal_tmp AS \
         __temporal_projection_input"
    );
    let error = temporal_projection_sql(&format!("SELECT probe.id {TEMPORAL_FROM}"), &probe_config)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("probe qualifier exposes only offset_ms and probe_time"),
        "{error}"
    );
}

#[test]
fn temporal_projection_matches_unquoted_qualifiers_case_insensitively() {
    let sql = format!("SELECT T.trade_id, Q.price {TEMPORAL_FROM} WHERE Q.price > 0");
    assert_eq!(
        temporal_projection_sql(&sql, &temporal_projection_config()).unwrap(),
        "SELECT trade_id, price_quotes FROM __temporal_tmp AS __temporal_projection_input \
         WHERE price_quotes > 0"
    );
}

#[test]
fn temporal_projection_rejects_probe_and_join_qualifier_collision() {
    let mut config = temporal_projection_config();
    config.probe_schedule =
        laminar_sql::temporal::TemporalProbeSchedule::list(vec![5_000]).unwrap();
    config.probe_alias = Some("Q".into());

    let error =
        temporal_projection_sql(&format!("SELECT q.price {TEMPORAL_FROM}"), &config).unwrap_err();
    assert!(error.to_string().contains("qualifiers must be distinct"));
}

#[test]
fn temporal_projection_rejects_unpreserved_sql_shapes() {
    let cases = [
        (
            "distinct",
            format!("SELECT DISTINCT t.trade_id {TEMPORAL_FROM}"),
            "SELECT modifiers",
        ),
        (
            "grouping",
            format!(
                "SELECT t.symbol, COUNT(q.price) {TEMPORAL_FROM} \
                 GROUP BY t.symbol HAVING COUNT(q.price) > 1"
            ),
            "SELECT modifiers",
        ),
        (
            "ordering and limit",
            format!("SELECT t.trade_id {TEMPORAL_FROM} ORDER BY t.trade_id LIMIT 1"),
            "WITH, ORDER BY",
        ),
        (
            "fetch",
            format!("SELECT t.trade_id {TEMPORAL_FROM} FETCH FIRST 1 ROW ONLY"),
            "WITH, ORDER BY",
        ),
        (
            "window function",
            format!(
                "SELECT ROW_NUMBER() OVER (PARTITION BY t.symbol ORDER BY t.trade_time) AS rn \
                 {TEMPORAL_FROM}"
            ),
            "function calls",
        ),
        (
            "qualify",
            format!("SELECT t.trade_id {TEMPORAL_FROM} QUALIFY q.price > 0"),
            "SELECT modifiers",
        ),
        (
            "qualified wildcard",
            format!("SELECT t.* {TEMPORAL_FROM}"),
            "qualified wildcards",
        ),
        (
            "wildcard modifier",
            format!("SELECT * EXCLUDE (trade_time) {TEMPORAL_FROM}"),
            "invalid SQL",
        ),
        (
            "scalar function",
            format!("SELECT ABS(q.price) {TEMPORAL_FROM}"),
            "function calls",
        ),
        (
            "unsupported expression",
            format!("SELECT t.trade_id {TEMPORAL_FROM} WHERE q.symbol LIKE 'A%'"),
            "expression form",
        ),
        (
            "subquery",
            format!("SELECT (SELECT 1) AS one {TEMPORAL_FROM}"),
            "subqueries",
        ),
        (
            "unqualified column",
            format!("SELECT trade_id {TEMPORAL_FROM}"),
            "unqualified column",
        ),
        (
            "additional join",
            format!(
                "SELECT t.trade_id {TEMPORAL_FROM} \
                 LEFT JOIN venues v ON q.venue = v.venue"
            ),
            "exactly one direct temporal join",
        ),
        (
            "cte",
            format!("WITH seed AS (SELECT 1) SELECT t.trade_id {TEMPORAL_FROM}"),
            "WITH, ORDER BY",
        ),
        (
            "multiple statements",
            format!("SELECT t.trade_id {TEMPORAL_FROM}; SELECT 1"),
            "exactly one SELECT statement",
        ),
    ];

    let config = temporal_projection_config();
    for (name, sql, expected) in cases {
        let error = temporal_projection_sql(&sql, &config).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "{name}: expected {expected:?}, got {error}"
        );
    }
}
