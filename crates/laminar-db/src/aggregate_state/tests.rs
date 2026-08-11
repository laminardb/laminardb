use super::*;

async fn try_from_sql_local(
    ctx: &SessionContext,
    sql: &str,
    emit_changelog: bool,
) -> Result<Option<IncrementalAggState>, DbError> {
    try_from_sql_for_key_groups(
        ctx,
        sql,
        emit_changelog,
        laminar_core::state::DEFAULT_KEY_GROUP_COUNT,
    )
    .await
}

async fn try_from_sql_for_key_groups(
    ctx: &SessionContext,
    sql: &str,
    emit_changelog: bool,
    key_group_count: KeyGroupCount,
) -> Result<Option<IncrementalAggState>, DbError> {
    IncrementalAggState::try_from_sql(ctx, sql, emit_changelog, key_group_count).await
}

#[tokio::test]
async fn test_try_from_sql_rejects_post_aggregate_projection() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("a", DataType::Float64, false),
        Field::new("b", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            Arc::new(arrow::array::Float64Array::from(vec![2.0])),
        ],
    )
    .unwrap();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    for sql in [
        "SELECT name, SUM(a) * 2 AS doubled FROM events GROUP BY name",
        "SELECT name, SUM(a) AS total FROM events GROUP BY name \
         UNION ALL SELECT name, SUM(a) AS total FROM events GROUP BY name",
        "SELECT name, doubled FROM (SELECT name, SUM(a) * 2 AS doubled \
         FROM events GROUP BY name) q",
        "SELECT name, a, total FROM (SELECT name, a, SUM(b) AS total \
         FROM events GROUP BY name, a) q",
        "SELECT name, SUM(a) AS total FROM events GROUP BY name \
         ORDER BY total DESC LIMIT 1",
    ] {
        let error = match try_from_sql_local(&ctx, sql, false).await {
            Err(error) => error,
            Ok(_) => panic!("unsafe post-aggregate plan was admitted: {sql}"),
        };
        assert!(
            error.to_string().contains("post-aggregate projection")
                || error.to_string().contains("one aggregate stage"),
            "{error}"
        );
    }
}

#[test]
fn test_extract_clauses_simple() {
    let c = extract_clauses("SELECT a, SUM(b) FROM trades GROUP BY a");
    assert_eq!(c.from_clause, "trades");
    assert!(c.where_clause.is_empty());
}

#[test]
fn test_extract_clauses_with_where() {
    let c = extract_clauses("SELECT * FROM events WHERE x > 1 GROUP BY y");
    assert_eq!(c.from_clause, "events");
    assert!(
        c.where_clause.contains("WHERE"),
        "should contain WHERE: {}",
        c.where_clause
    );
    assert!(
        c.where_clause.contains("x > 1"),
        "should contain predicate: {}",
        c.where_clause
    );
}

#[test]
fn test_extract_clauses_with_join() {
    let c = extract_clauses("SELECT * FROM events e JOIN dim d ON e.id = d.id");
    // AST preserves join structure
    assert!(
        c.from_clause.contains("events"),
        "should contain events: {}",
        c.from_clause
    );
    assert!(
        c.from_clause.contains("JOIN"),
        "should contain JOIN: {}",
        c.from_clause
    );
    assert!(
        c.from_clause.contains("dim"),
        "should contain dim: {}",
        c.from_clause
    );
}

#[test]
fn test_extract_clauses_keyword_in_string_literal() {
    // This would break heuristic extraction but works with AST
    let c = extract_clauses("SELECT * FROM logs WHERE msg = 'joined GROUP chat' GROUP BY user_id");
    assert_eq!(c.from_clause, "logs");
    // WHERE should include the full predicate including the string
    assert!(
        c.where_clause.contains("GROUP chat"),
        "string literal should be preserved: {}",
        c.where_clause
    );
}

#[test]
fn test_extract_clauses_no_where() {
    let c = extract_clauses("SELECT * FROM events GROUP BY y");
    assert_eq!(c.from_clause, "events");
    assert!(c.where_clause.is_empty());
}

#[tokio::test]
async fn test_try_from_sql_non_aggregate() {
    let ctx = laminar_sql::create_session_context();
    // Register a dummy table
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let result = try_from_sql_local(&ctx, "SELECT * FROM events", false)
        .await
        .unwrap();
    assert!(result.is_none(), "Non-aggregate query should return None");
}

#[tokio::test]
async fn test_try_from_sql_with_group_by() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let result = try_from_sql_local(
        &ctx,
        "SELECT name, SUM(value) as total FROM events GROUP BY name",
        false,
    )
    .await
    .unwrap();
    assert!(result.is_some(), "Aggregate query should return Some");
    let state = result.unwrap();
    assert_eq!(state.num_group_cols, 1);
    assert_eq!(state.agg_specs.len(), 1);
    assert_eq!(
        state.key_group_count(),
        laminar_core::state::DEFAULT_KEY_GROUP_COUNT
    );
}

#[tokio::test]
async fn embedded_float_grouping_remains_supported_without_partition_codec_gate() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("bucket", DataType::Float64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            Arc::new(arrow::array::Float64Array::from(vec![0.0])),
        ],
    )
    .unwrap();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let mut state = try_from_sql_local(
        &ctx,
        "SELECT bucket, SUM(value) AS total FROM events GROUP BY bucket",
        false,
    )
    .await
    .unwrap()
    .expect("embedded aggregate planning must continue to accept float keys");

    let pre_agg = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Float64, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ])),
        vec![
            Arc::new(arrow::array::Float64Array::from(vec![1.5, 2.5, 1.5])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0, 30.0])),
        ],
    )
    .unwrap();
    state.process_batch(&pre_agg, i64::MIN).unwrap();

    let output = state.emit().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 2);
    let keys = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let totals = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let actual = (0..output[0].num_rows())
        .map(|row| (keys.value(row).to_bits(), totals.value(row)))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(actual[&1.5f64.to_bits()], 40.0);
    assert_eq!(actual[&2.5f64.to_bits()], 20.0);
}

#[tokio::test]
async fn test_incremental_aggregation_across_batches() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    // Register table for plan creation
    let dummy_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Float64Array::from(vec![0.0])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy_batch]])
            .unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let mut state = try_from_sql_local(
        &ctx,
        "SELECT name, SUM(value) as total FROM events GROUP BY name",
        false,
    )
    .await
    .unwrap()
    .unwrap();

    // Simulate pre-agg output: batch 1
    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));
    let batch1 = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "b", "a"])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0, 30.0])),
        ],
    )
    .unwrap();
    state.process_batch(&batch1, i64::MIN).unwrap();

    let result1 = state.emit().unwrap();
    assert_eq!(result1.len(), 1);
    assert_eq!(result1[0].num_rows(), 2); // two groups: a, b

    // Batch 2: more data for existing groups
    let batch2 = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "c"])),
            Arc::new(arrow::array::Float64Array::from(vec![5.0, 15.0])),
        ],
    )
    .unwrap();
    state.process_batch(&batch2, i64::MIN).unwrap();

    let result2 = state.emit().unwrap();
    assert_eq!(result2.len(), 1);
    assert_eq!(result2[0].num_rows(), 3); // three groups: a, b, c

    // Verify running totals: group "a" should have 10+30+5 = 45
    let names = result2[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let totals = result2[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();

    for i in 0..result2[0].num_rows() {
        match names.value(i) {
            "a" => assert!(
                (totals.value(i) - 45.0).abs() < f64::EPSILON,
                "Expected 45.0 for group 'a', got {}",
                totals.value(i)
            ),
            "b" => assert!(
                (totals.value(i) - 20.0).abs() < f64::EPSILON,
                "Expected 20.0 for group 'b', got {}",
                totals.value(i)
            ),
            "c" => assert!(
                (totals.value(i) - 15.0).abs() < f64::EPSILON,
                "Expected 15.0 for group 'c', got {}",
                totals.value(i)
            ),
            other => panic!("Unexpected group: {other}"),
        }
    }
}

/// Helper: register a table and build an `IncrementalAggState` from SQL.
async fn setup_agg_state(sql: &str) -> (SessionContext, IncrementalAggState) {
    setup_agg_state_with_changelog(sql, false).await
}

async fn setup_agg_state_with_changelog(
    sql: &str,
    emit_changelog: bool,
) -> (SessionContext, IncrementalAggState) {
    setup_agg_state_for_key_groups(
        sql,
        emit_changelog,
        laminar_core::state::DEFAULT_KEY_GROUP_COUNT,
    )
    .await
}

async fn setup_agg_state_for_key_groups(
    sql: &str,
    emit_changelog: bool,
    key_group_count: KeyGroupCount,
) -> (SessionContext, IncrementalAggState) {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Float64Array::from(vec![0.0])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();
    let state = try_from_sql_for_key_groups(&ctx, sql, emit_changelog, key_group_count)
        .await
        .unwrap()
        .expect("expected aggregate state");
    (ctx, state)
}

fn sum_pre_agg_batch(names: &[&str], values: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(names.to_vec())),
            Arc::new(arrow::array::Float64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn capture_all_vnodes(
    state: &mut IncrementalAggState,
) -> Result<Vec<(u32, AggStateCheckpoint)>, DbError> {
    state.force_full_vnode_capture();
    let vnode_count = u32::from(state.key_group_count().get());
    state.checkpoint_vnodes(&(0..vnode_count).collect::<Vec<_>>(), vnode_count)
}

fn checkpoint_bytes(state: &mut IncrementalAggState) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&capture_all_vnodes(state).unwrap())
        .unwrap()
        .to_vec()
}

#[tokio::test]
async fn retained_state_accounting_tracks_groups_without_drifting_on_fixed_size_updates() {
    let sql = "SELECT name, SUM(value) AS total FROM events GROUP BY name";
    let (_, mut state) = setup_agg_state(sql).await;
    let empty_bytes = state.accounted_state_bytes();
    assert!(state.cached_usage_matches_structural_recompute());

    state
        .process_batch(&sum_pre_agg_batch(&["a"], &[1.0]), 10)
        .unwrap();
    let one_group_bytes = state.accounted_state_bytes();
    assert!(one_group_bytes > empty_bytes);
    assert!(state.cached_usage_matches_structural_recompute());

    state
        .process_batch(&sum_pre_agg_batch(&["a"], &[2.0]), 20)
        .unwrap();
    assert_eq!(
        state.accounted_state_bytes(),
        one_group_bytes,
        "a fixed-size accumulator update must replace, not accumulate, its charge"
    );
    assert!(state.cached_usage_matches_structural_recompute());

    state.emit().unwrap();
    assert!(state.cached_usage_matches_structural_recompute());
    let reconciled_bytes = state.accounted_state_bytes();
    assert!(reconciled_bytes >= one_group_bytes);
}

#[tokio::test]
async fn retained_state_accounting_includes_topology_and_changelog_lifecycle() {
    let sql = "SELECT name, SUM(value) AS total FROM events GROUP BY name";
    let (_, smaller_topology) =
        setup_agg_state_for_key_groups(sql, false, KeyGroupCount::try_from(1_u32).unwrap()).await;
    let (_, larger_topology) =
        setup_agg_state_for_key_groups(sql, false, KeyGroupCount::try_from(16_u32).unwrap()).await;
    assert!(
        larger_topology.accounted_state_bytes() > smaller_topology.accounted_state_bytes(),
        "a larger immutable vnode address space must carry a larger topology charge"
    );

    let (_, mut changelog) = setup_agg_state_with_changelog(sql, true).await;
    let empty_bytes = changelog.accounted_state_bytes();
    changelog
        .process_batch(
            &sum_pre_agg_batch(&["retained-key-a", "retained-key-b"], &[1.0, 2.0]),
            10,
        )
        .unwrap();
    assert!(changelog.accounted_state_bytes() > empty_bytes);
    assert!(changelog.cached_usage_matches_structural_recompute());

    changelog.emit().unwrap();
    assert!(changelog.accounted_state_bytes() > empty_bytes);
    assert!(changelog.cached_usage_matches_structural_recompute());

    assert!(changelog.cached_usage_matches_structural_recompute());
}

#[tokio::test]
async fn local_keyed_state_routes_across_common_vnodes() {
    let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
    let (_, mut state) = setup_agg_state(sql).await;
    let batch = sum_pre_agg_batch(&["a", "b", "a"], &[1.0, 2.0, 3.0]);
    let rows = state
        .row_converter
        .convert_columns(&[Arc::clone(batch.column(0))])
        .unwrap();
    let mut expected_vnodes = (0..batch.num_rows())
        .map(|row| {
            IncrementalAggState::vnode_for_group_key(
                state.num_group_cols,
                &rows.row(row).owned(),
                state.routing_vnode_count(),
            )
        })
        .collect::<Vec<_>>();
    expected_vnodes.sort_unstable();
    expected_vnodes.dedup();
    assert_eq!(expected_vnodes.len(), 2);

    state.process_batch(&batch, 42).unwrap();

    assert_eq!(
        state.key_group_count(),
        laminar_core::state::DEFAULT_KEY_GROUP_COUNT
    );
    assert_eq!(state.logical_group_count_for_test(), 2);
    assert_eq!(state.active_vnodes_for_test(), expected_vnodes);

    let mut totals = std::collections::BTreeMap::new();
    for batch in state.emit().unwrap() {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("group name output");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("aggregate total output");
        for row in 0..batch.num_rows() {
            totals.insert(names.value(row).to_owned(), values.value(row));
        }
    }
    assert_eq!(totals.get("a"), Some(&4.0));
    assert_eq!(totals.get("b"), Some(&2.0));
}

#[tokio::test]
async fn distinct_aggregates_are_rejected_at_admission() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Float64Array::from(vec![0.0])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let error = match try_from_sql_local(
        &ctx,
        "SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name",
        false,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("DISTINCT aggregate was admitted"),
    };
    assert!(error.to_string().contains("DISTINCT aggregates"));
}

#[tokio::test]
async fn test_filter_clause_extracted() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Float64Array::from(vec![0.0])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let state = try_from_sql_local(
        &ctx,
        "SELECT name, SUM(value) FILTER (WHERE value > 0) as pos_sum FROM events GROUP BY name",
        false,
    )
    .await
    .unwrap()
    .expect("expected aggregate state");
    assert!(
        state.agg_specs[0].filter_col_index.is_some(),
        "FILTER clause should set filter_col_index"
    );
}

#[tokio::test]
async fn test_filter_clause_applied() {
    let (_, mut state) = setup_agg_state(
        "SELECT name, SUM(value) FILTER (WHERE value > 0) as pos_sum FROM events GROUP BY name",
    )
    .await;

    // The pre-agg SQL wraps the input with CASE WHEN and adds a
    // filter boolean column. Build a batch matching that schema.
    let filter_col_idx = state.agg_specs[0]
        .filter_col_index
        .expect("filter_col_index should be set");
    let num_cols = state.num_group_cols
        + state
            .agg_specs
            .iter()
            .map(|s| s.input_col_indices.len())
            .sum::<usize>()
        + state
            .agg_specs
            .iter()
            .filter(|s| s.filter_col_index.is_some())
            .count();
    assert!(
        filter_col_idx < num_cols,
        "filter col index should be in range"
    );

    // Build pre-agg batch manually: name, CASE value, CASE filter
    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
        Field::new("__agg_filter_2", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a"])),
            // value > 0 wrapped: -5 becomes NULL, 10 stays, 20 stays
            Arc::new(arrow::array::Float64Array::from(vec![-5.0, 10.0, 20.0])),
            // filter mask: false, true, true
            Arc::new(arrow::array::BooleanArray::from(vec![false, true, true])),
        ],
    )
    .unwrap();
    state.process_batch(&batch, i64::MIN).unwrap();

    let result = state.emit().unwrap();
    let total_col = result[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("sum should be Float64");
    // Only 10 + 20 = 30 (the -5 row is filtered out)
    assert!(
        (total_col.value(0) - 30.0).abs() < f64::EPSILON,
        "SUM with FILTER should be 30, got {}",
        total_col.value(0)
    );
}

#[tokio::test]
async fn aliased_having_filters_emitted_batch() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Float64Array::from(vec![0.0])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let state = try_from_sql_local(
        &ctx,
        "SELECT name, COUNT(*) AS row_count, COUNT(value) AS value_count, \
         SUM(value) AS total FROM events GROUP BY name \
         HAVING name = 'high' AND COUNT(*) = 2 \
         AND COUNT(value) = 2 AND SUM(value) > 100",
        false,
    )
    .await
    .unwrap()
    .expect("expected aggregate state");
    assert!(state.having_filter().is_some());

    let emitted = RecordBatch::try_new(
        Arc::clone(&state.output_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["low", "high"])),
            Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow::array::Float64Array::from(vec![50.0, 150.0])),
        ],
    )
    .unwrap();
    let filtered = apply_compiled_having(&[emitted], state.having_filter().unwrap()).unwrap();

    assert_eq!(filtered.len(), 1);
    let names = filtered[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "high");
}

#[tokio::test]
async fn test_create_accumulator_error_propagated() {
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

    // Verify create_accumulator returns Ok (not panic)
    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    // This should succeed without panicking
    assert!(state.process_batch(&batch, i64::MIN).is_ok());
}

#[tokio::test]
async fn test_sum_int32_input_is_coerced_before_accumulator_update() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Int32Array::from(vec![0])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("orders", Arc::new(mem_table)).unwrap();

    let mut state = try_from_sql_local(
        &ctx,
        "SELECT name, SUM(amount) as total FROM orders GROUP BY name",
        false,
    )
    .await
    .unwrap()
    .expect("expected aggregate state");

    assert_eq!(
        state.agg_specs[0].input_types[0],
        DataType::Int64,
        "SUM(Int32) must feed the DataFusion Int64 accumulator"
    );
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x", "x"])),
            Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let pre_agg = state
        .compiled_projection()
        .expect("single-source aggregate compiles")
        .evaluate(&input)
        .unwrap();
    assert_eq!(pre_agg.column(1).data_type(), &DataType::Int64);
    state.process_batch(&pre_agg, i64::MIN).unwrap();
    let output = state.emit().unwrap().pop().unwrap();
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        30,
        "coerced SUM must execute without an accumulator downcast panic"
    );
}

#[tokio::test]
async fn test_avg_float32_input_is_coerced_before_accumulator_update() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float32, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Float32Array::from(vec![0.0f32])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("products", Arc::new(mem_table)).unwrap();

    let mut state = try_from_sql_local(
        &ctx,
        "SELECT name, AVG(price) as avg_price FROM products GROUP BY name",
        false,
    )
    .await
    .unwrap()
    .expect("expected aggregate state");

    assert_eq!(
        state.agg_specs[0].input_types[0],
        DataType::Float64,
        "AVG(Float32) must feed the DataFusion Float64 accumulator"
    );
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x", "x"])),
            Arc::new(arrow::array::Float32Array::from(vec![10.0, 20.0])),
        ],
    )
    .unwrap();
    let pre_agg = state
        .compiled_projection()
        .expect("single-source aggregate compiles")
        .evaluate(&input)
        .unwrap();
    assert_eq!(pre_agg.column(1).data_type(), &DataType::Float64);
    state.process_batch(&pre_agg, i64::MIN).unwrap();
    let output = state.emit().unwrap().pop().unwrap();
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .value(0),
        15.0,
        "coerced AVG must execute without an accumulator downcast panic"
    );
}

#[tokio::test]
async fn test_type_inference_literal_expr() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["x"])),
            Arc::new(arrow::array::Int64Array::from(vec![0])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let state = try_from_sql_local(
        &ctx,
        "SELECT name, MIN(value) as min_val FROM events GROUP BY name",
        false,
    )
    .await
    .unwrap()
    .expect("expected aggregate state");

    // Int64 in, Int64 out — should still be Int64
    assert_eq!(state.agg_specs[0].input_types[0], DataType::Int64,);
}

#[test]
fn test_extract_clauses_subquery_in_where() {
    // Subquery with its own WHERE — AST handles nesting
    let c = extract_clauses(
        "SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders WHERE status = 'active') GROUP BY name",
    );
    assert_eq!(c.from_clause, "orders");
    assert!(
        c.where_clause.contains("AVG"),
        "subquery should be preserved: {}",
        c.where_clause
    );
}

#[test]
fn test_expr_to_sql_column() {
    use datafusion_expr::col;
    assert_eq!(expr_to_sql(&col("price")), "\"price\"");
}

#[test]
fn test_expr_to_sql_string_literal() {
    let e = datafusion_expr::Expr::Literal(ScalarValue::Utf8(Some("it's".to_string())), None);
    assert_eq!(expr_to_sql(&e), "'it''s'");
}

#[test]
fn test_expr_to_sql_null_literal() {
    let e = datafusion_expr::Expr::Literal(ScalarValue::Null, None);
    assert_eq!(expr_to_sql(&e), "NULL");
}

#[test]
fn test_expr_to_sql_boolean_literal() {
    let t = datafusion_expr::Expr::Literal(ScalarValue::Boolean(Some(true)), None);
    assert_eq!(expr_to_sql(&t), "TRUE");
    let f = datafusion_expr::Expr::Literal(ScalarValue::Boolean(Some(false)), None);
    assert_eq!(expr_to_sql(&f), "FALSE");
}

#[test]
fn test_expr_to_sql_binary_expr() {
    use datafusion_expr::{col, lit};
    let e = col("x").gt(lit(10));
    let sql = expr_to_sql(&e);
    assert!(sql.contains("\"x\""), "should contain column: {sql}");
    assert!(sql.contains('>'), "should contain >: {sql}");
    assert!(sql.contains("10"), "should contain 10: {sql}");
}

#[test]
fn test_expr_to_sql_cast() {
    use datafusion_expr::Expr;
    let e = Expr::Cast(datafusion_expr::expr::Cast {
        expr: Box::new(datafusion_expr::col("x")),
        data_type: DataType::Float64,
    });
    let sql = expr_to_sql(&e);
    assert!(sql.contains("CAST"), "should contain CAST: {sql}");
    assert!(sql.contains("Float64"), "should contain target type: {sql}");
}

#[test]
fn test_expr_to_sql_scalar_function() {
    use datafusion_expr::Expr;
    // Build a scalar function expr via DataFusion
    let func = datafusion::functions::string::upper();
    let e = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction {
        func,
        args: vec![datafusion_expr::col("name")],
    });
    let sql = expr_to_sql(&e);
    assert!(sql.contains("upper"), "should contain function name: {sql}");
    assert!(sql.contains("\"name\""), "should contain arg: {sql}");
}

#[test]
fn test_expr_to_sql_case() {
    use datafusion_expr::{col, lit};
    let e = datafusion_expr::Expr::Case(datafusion_expr::expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(col("x").gt(lit(0))), Box::new(lit(1)))],
        else_expr: Some(Box::new(lit(0))),
    });
    let sql = expr_to_sql(&e);
    assert!(sql.starts_with("CASE"), "should start with CASE: {sql}");
    assert!(sql.contains("WHEN"), "should contain WHEN: {sql}");
    assert!(sql.contains("THEN"), "should contain THEN: {sql}");
    assert!(sql.contains("ELSE"), "should contain ELSE: {sql}");
    assert!(sql.ends_with("END"), "should end with END: {sql}");
}

#[test]
fn test_expr_to_sql_not() {
    use datafusion_expr::col;
    let e = datafusion_expr::Expr::Not(Box::new(col("active")));
    assert_eq!(expr_to_sql(&e), "(NOT \"active\")");
}

#[test]
fn test_expr_to_sql_negative() {
    use datafusion_expr::col;
    let e = datafusion_expr::Expr::Negative(Box::new(col("x")));
    assert_eq!(expr_to_sql(&e), "(-\"x\")");
}

#[test]
fn test_expr_to_sql_is_null() {
    use datafusion_expr::col;
    let e = datafusion_expr::Expr::IsNull(Box::new(col("x")));
    assert_eq!(expr_to_sql(&e), "(\"x\" IS NULL)");
}

#[test]
fn test_expr_to_sql_is_not_null() {
    use datafusion_expr::col;
    let e = datafusion_expr::Expr::IsNotNull(Box::new(col("x")));
    assert_eq!(expr_to_sql(&e), "(\"x\" IS NOT NULL)");
}

#[test]
fn test_expr_to_sql_between() {
    use datafusion_expr::{col, lit};
    let e = col("x").between(lit(1), lit(10));
    let sql = expr_to_sql(&e);
    assert!(sql.contains("BETWEEN"), "should contain BETWEEN: {sql}");
    assert!(sql.contains("AND"), "should contain AND: {sql}");
}

#[test]
fn test_expr_to_sql_in_list() {
    use datafusion_expr::{col, lit};
    let e = col("status").in_list(vec![lit("a"), lit("b")], false);
    let sql = expr_to_sql(&e);
    assert!(sql.contains("IN"), "should contain IN: {sql}");
    assert!(sql.contains("'a'"), "should contain 'a': {sql}");
    assert!(sql.contains("'b'"), "should contain 'b': {sql}");
}

#[test]
fn test_expr_to_sql_like() {
    use datafusion_expr::col;
    let e = col("name").like(datafusion_expr::lit("foo%"));
    let sql = expr_to_sql(&e);
    assert!(sql.contains("LIKE"), "should contain LIKE: {sql}");
    assert!(sql.contains("'foo%'"), "should contain pattern: {sql}");
}

#[test]
fn test_expr_to_sql_aggregate_function() {
    use datafusion_expr::Expr;
    let sum_udf = datafusion::functions_aggregate::sum::sum_udaf();
    let e = Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
        func: sum_udf,
        params: datafusion_expr::expr::AggregateFunctionParams {
            args: vec![datafusion_expr::col("x")],
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
    });
    let sql = expr_to_sql(&e);
    assert!(sql.contains("sum"), "should contain sum: {sql}");
    assert!(sql.contains("\"x\""), "should contain arg: {sql}");
}

#[tokio::test]
async fn test_group_by_expression_scalar_function() {
    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["hello"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    let mem_table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let state = try_from_sql_local(
        &ctx,
        "SELECT upper(name), SUM(value) as total FROM events GROUP BY upper(name)",
        false,
    )
    .await
    .unwrap()
    .expect("expected aggregate state");

    // The pre-agg SQL should contain the expression, not a
    // quoted identifier
    assert!(
        state.pre_agg_sql.contains("upper("),
        "pre-agg SQL should contain expression: {}",
        state.pre_agg_sql
    );
    assert!(
        !state.pre_agg_sql.contains("\"upper("),
        "should NOT quote expression as identifier: {}",
        state.pre_agg_sql
    );
}

#[tokio::test]
async fn test_group_by_simple_column_still_works() {
    let (_, state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
    // Simple column ref should be a quoted identifier
    assert!(
        state.pre_agg_sql.contains("\"name\""),
        "simple column should be quoted: {}",
        state.pre_agg_sql
    );
}

#[tokio::test]
async fn group_cardinality_limit_rejects_the_whole_batch_and_retry() {
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
    state.set_max_groups_for_test(2);
    state
        .process_batch(&sum_pre_agg_batch(&["a", "b"], &[10.0, 20.0]), 1)
        .unwrap();
    let before = checkpoint_bytes(&mut state);

    // Updating a while introducing c must not partially apply a before rejecting c.
    let over_limit = sum_pre_agg_batch(&["a", "c"], &[5.0, 100.0]);
    for _ in 0..2 {
        let error = state.process_batch(&over_limit, 2).unwrap_err();
        assert!(error.to_string().contains("group limit exceeded"));
        assert_eq!(checkpoint_bytes(&mut state), before);
    }
}

#[tokio::test]
async fn group_cardinality_existing_groups_update_at_the_limit() {
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

    state.set_max_groups_for_test(2);

    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));

    // Batch 1: create 2 groups (at limit)
    let batch1 = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0])),
        ],
    )
    .unwrap();
    state.process_batch(&batch1, i64::MIN).unwrap();

    // Existing keys remain writable at the limit.
    let batch2 = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
            Arc::new(arrow::array::Float64Array::from(vec![5.0, 7.0])),
        ],
    )
    .unwrap();
    state.process_batch(&batch2, i64::MIN).unwrap();

    let result = state.emit().unwrap();
    assert_eq!(result[0].num_rows(), 2, "still only 2 groups");

    let names = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let totals = result[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    for i in 0..2 {
        match names.value(i) {
            "a" => assert_eq!(totals.value(i), 15.0),
            "b" => assert_eq!(totals.value(i), 27.0),
            other => panic!("unexpected group {other}"),
        }
    }
}

#[test]
fn test_extract_clauses_multiple_joins() {
    let c = extract_clauses(
        "SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id JOIN products p ON o.prod_id = p.id WHERE o.amount > 100 GROUP BY c.name",
    );
    assert!(
        c.from_clause.contains("orders"),
        "should contain orders: {}",
        c.from_clause
    );
    assert!(
        c.from_clause.contains("customers"),
        "should contain customers: {}",
        c.from_clause
    );
    assert!(
        c.from_clause.contains("products"),
        "should contain products: {}",
        c.from_clause
    );
    assert!(
        c.where_clause.contains("100"),
        "WHERE should contain predicate: {}",
        c.where_clause
    );
}

#[tokio::test]
async fn test_changelog_delta_emit() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(mem)).unwrap();

    let mut state = try_from_sql_local(
        &ctx,
        "SELECT symbol, SUM(price) AS total FROM t GROUP BY symbol",
        true, // changelog mode
    )
    .await
    .unwrap()
    .unwrap();

    // Output schema should include __weight.
    assert_eq!(
        state
            .output_schema
            .field(state.output_schema.fields().len() - 1)
            .name(),
        WEIGHT_COLUMN
    );

    // Cycle 1: new data → all groups are +1 inserts.
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    state.process_batch(&b1, 1000).unwrap();
    let r1 = state.emit().unwrap();
    assert_eq!(r1.len(), 1);
    let batch1 = &r1[0];
    assert_eq!(batch1.num_rows(), 2); // AAPL +1, GOOG +1
    let w1 = batch1
        .column(batch1.num_columns() - 1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert!(w1.iter().all(|w| w == Some(1))); // all inserts

    // Cycle 2: AAPL changes, GOOG unchanged → -1 old AAPL, +1 new AAPL, GOOG skipped.
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
            Arc::new(arrow::array::Int64Array::from(vec![50])),
        ],
    )
    .unwrap();
    state.process_batch(&b2, 2000).unwrap();
    let r2 = state.emit().unwrap();
    assert_eq!(r2.len(), 1);
    let batch2 = &r2[0];
    // Should be 2 rows: -1 (AAPL old), +1 (AAPL new). GOOG is unchanged → skipped.
    assert_eq!(batch2.num_rows(), 2);
    let w2 = batch2
        .column(batch2.num_columns() - 1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(w2.value(0), -1); // retraction
    assert_eq!(w2.value(1), 1); // insert

    // Cycle 3: no new data, nothing changed → empty output.
    let r3 = state.emit().unwrap();
    assert!(r3.is_empty() || r3.iter().all(|b| b.num_rows() == 0));
}

#[tokio::test]
async fn test_cascaded_agg_retract_batch() {
    // Simulate a downstream aggregate consuming upstream changelog output
    // with a __weight column. Negative weights should trigger retract_batch.
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("upstream", Arc::new(mem)).unwrap();

    let mut state = try_from_sql_local(
        &ctx,
        "SELECT symbol, SUM(total) AS grand_total FROM upstream GROUP BY symbol",
        false,
    )
    .await
    .unwrap()
    .unwrap();

    // weight_col_idx should be detected from upstream schema.
    assert!(state.weight_col_idx.is_some());

    // Cycle 1: insert AAPL=100 (+1), GOOG=200 (+1).
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
            Arc::new(arrow::array::Int64Array::from(vec![1, 1])),
        ],
    )
    .unwrap();
    state.process_batch(&b1, 1000).unwrap();
    let r1 = state.emit().unwrap();
    assert_eq!(r1[0].num_rows(), 2);

    // Cycle 2: retract AAPL=100 (-1), insert AAPL=150 (+1). GOOG unchanged.
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "AAPL"])),
            Arc::new(arrow::array::Int64Array::from(vec![100, 150])),
            Arc::new(arrow::array::Int64Array::from(vec![-1, 1])),
        ],
    )
    .unwrap();
    state.process_batch(&b2, 2000).unwrap();
    let r2 = state.emit().unwrap();
    // AAPL: was 100, retracted 100, added 150 → SUM=150. GOOG: still 200.
    assert_eq!(r2[0].num_rows(), 2);
    let totals = r2[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let symbols = r2[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    for i in 0..r2[0].num_rows() {
        match symbols.value(i) {
            "AAPL" => assert_eq!(totals.value(i), 150),
            "GOOG" => assert_eq!(totals.value(i), 200),
            other => panic!("unexpected symbol: {other}"),
        }
    }
}

#[tokio::test]
async fn weighted_min_and_max_are_rejected_at_admission() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("upstream", Arc::new(mem)).unwrap();

    for sql in [
        "SELECT symbol, MIN(price) AS value FROM upstream GROUP BY symbol",
        "SELECT symbol, MAX(price) AS value FROM upstream GROUP BY symbol",
    ] {
        assert!(try_from_sql_local(&ctx, sql, false).await.is_err(), "{sql}");
    }
}

#[tokio::test]
async fn unsupported_aggregate_is_rejected_at_admission() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("upstream", Arc::new(mem)).unwrap();

    assert!(try_from_sql_local(
        &ctx,
        "SELECT symbol, STDDEV(price) AS sd FROM upstream GROUP BY symbol",
        false,
    )
    .await
    .is_err());
}

#[tokio::test]
async fn test_cascaded_count_star_over_changelog() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("upstream", Arc::new(mem)).unwrap();

    let mut state = try_from_sql_local(
        &ctx,
        "SELECT region, COUNT(*) AS cnt FROM upstream GROUP BY region",
        false,
    )
    .await
    .unwrap()
    .unwrap();

    assert!(state.weight_col_idx.is_some());
    assert!(!state.output_schema.field(0).is_nullable());
    assert!(!state.output_schema.field(1).is_nullable());

    // Cycle 1: insert 3 rows.
    // Pre-agg schema for COUNT(*): [region, TRUE (dummy bool), __weight].
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Boolean, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US", "US", "EU"])),
            Arc::new(arrow::array::BooleanArray::from(vec![true, true, true])),
            Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
        ],
    )
    .unwrap();
    state.process_batch(&b1, 1000).unwrap();
    let r1 = state.emit().unwrap();
    assert_eq!(r1[0].num_rows(), 2);

    // Cycle 2: retract one US row
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Boolean, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US"])),
            Arc::new(arrow::array::BooleanArray::from(vec![true])),
            Arc::new(arrow::array::Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();
    state.process_batch(&b2, 2000).unwrap();
    let r2 = state.emit().unwrap();
    let counts = r2[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let regions = r2[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    for i in 0..r2[0].num_rows() {
        match regions.value(i) {
            "US" => assert_eq!(counts.value(i), 1, "US count should be 1 after retraction"),
            "EU" => assert_eq!(counts.value(i), 1, "EU count should remain 1"),
            other => panic!("unexpected region: {other}"),
        }
    }
}

#[tokio::test]
async fn pending_group_deletion_survives_vnode_checkpoint() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("upstream", Arc::new(mem)).unwrap();

    let sql = "SELECT region, COUNT(*) AS cnt FROM upstream GROUP BY region";
    let mut state = try_from_sql_local(&ctx, sql, true).await.unwrap().unwrap();
    assert!(state.weight_col_idx.is_some());

    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Boolean, true),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let mk = |regions: Vec<&str>, weights: Vec<i64>| {
        let n = regions.len();
        RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(regions)),
                Arc::new(arrow::array::BooleanArray::from(vec![true; n])),
                Arc::new(arrow::array::Int64Array::from(weights)),
            ],
        )
        .unwrap()
    };

    state.process_batch(&mk(vec!["US"], vec![1]), 1000).unwrap();
    let _ = state.emit().unwrap();
    let vnode = state.active_vnodes_for_test()[0];
    let vnode_count = u32::from(state.key_group_count().get());
    assert_eq!(
        state
            .checkpoint_vnodes(&[vnode], vnode_count)
            .unwrap()
            .len(),
        1
    );
    assert!(state
        .checkpoint_vnodes(&[vnode], vnode_count)
        .unwrap()
        .is_empty());

    state
        .process_batch(&mk(vec!["US"], vec![-1]), 2000)
        .unwrap();
    let mut captured = state.checkpoint_vnodes(&[vnode], vnode_count).unwrap();
    assert_eq!(captured.len(), 1);
    let (captured_vnode, checkpoint) = captured.pop().unwrap();
    assert_eq!(captured_vnode, vnode);

    let mut restored = try_from_sql_local(&ctx, sql, true).await.unwrap().unwrap();
    restored
        .restore_vnode(vnode, vnode_count, checkpoint)
        .unwrap();
    let output = restored.emit().unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    let regions = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let counts = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let weights = output[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(regions.value(0), "US");
    assert_eq!(counts.value(0), 1);
    assert_eq!(weights.value(0), -1);
    assert_eq!(restored.logical_group_count_for_test(), 0);
}

#[tokio::test]
async fn test_cascaded_avg_over_changelog() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("upstream", Arc::new(mem)).unwrap();

    let mut state = try_from_sql_local(
        &ctx,
        "SELECT region, AVG(price) AS avg_price FROM upstream GROUP BY region",
        false,
    )
    .await
    .unwrap()
    .unwrap();

    // Insert: 10, 20, 30 for "US" -> avg = 20
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0, 30.0])),
            Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
        ],
    )
    .unwrap();
    state.process_batch(&b1, 1000).unwrap();
    let r1 = state.emit().unwrap();
    let avg = r1[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!((avg.value(0) - 20.0).abs() < 0.001, "avg should be 20.0");

    // Retract 10 -> {20, 30} -> avg = 25
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US"])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0])),
            Arc::new(arrow::array::Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();
    state.process_batch(&b2, 2000).unwrap();
    let r2 = state.emit().unwrap();
    let avg2 = r2[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!(
        (avg2.value(0) - 25.0).abs() < 0.001,
        "avg should be 25.0 after retraction"
    );
}

fn round_trip(sv: &ScalarValue) -> ScalarValue {
    let bytes = scalars_to_ipc(std::slice::from_ref(sv)).unwrap();
    let back = ipc_to_scalars(&bytes).unwrap();
    assert_eq!(back.len(), 1);
    back.into_iter().next().unwrap()
}

#[test]
fn scalar_ipc_round_trip() {
    // Arrow IPC preserves exact type — no widening, unlike the old JSON path.
    assert_eq!(round_trip(&ScalarValue::Null), ScalarValue::Null);
    assert_eq!(
        round_trip(&ScalarValue::Boolean(Some(true))),
        ScalarValue::Boolean(Some(true)),
    );
    assert_eq!(
        round_trip(&ScalarValue::Int64(Some(-42))),
        ScalarValue::Int64(Some(-42)),
    );
    assert_eq!(
        round_trip(&ScalarValue::Float64(Some(2.72))),
        ScalarValue::Float64(Some(2.72)),
    );
    assert_eq!(
        round_trip(&ScalarValue::Utf8(Some("hello".into()))),
        ScalarValue::Utf8(Some("hello".into())),
    );
    let tz: Option<Arc<str>> = Some(Arc::from("UTC"));
    assert_eq!(
        round_trip(&ScalarValue::TimestampNanosecond(
            Some(1_000_000),
            tz.clone()
        )),
        ScalarValue::TimestampNanosecond(Some(1_000_000), tz),
    );
    assert_eq!(
        round_trip(&ScalarValue::Date32(Some(19000))),
        ScalarValue::Date32(Some(19000)),
    );
    assert_eq!(
        round_trip(&ScalarValue::Date64(Some(1_700_000_000_000))),
        ScalarValue::Date64(Some(1_700_000_000_000)),
    );
}

#[test]
fn binary_scalar_roundtrips_exactly() {
    // Under the old serde_json path, Binary was string-coerced via the
    // "STR" fallback. Arrow IPC preserves Binary natively.
    let sv = ScalarValue::Binary(Some(vec![1, 2, 3]));
    assert_eq!(round_trip(&sv), sv);
}
