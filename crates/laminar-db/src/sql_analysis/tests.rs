use super::*;

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
