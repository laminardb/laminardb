use super::*;

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

    // SUM(a)/SUM(b) collapses 2 aggregates into 1 derived column →
    // top_schema fields != agg_schema fields → should return None.
    let result = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT name, SUM(a) / SUM(b) AS ratio FROM events GROUP BY name",
        false,
    )
    .await
    .unwrap();
    assert!(
        result.is_none(),
        "Post-aggregate projection should return None"
    );
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

    let result = IncrementalAggState::try_from_sql(&ctx, "SELECT * FROM events", false)
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

    let result = IncrementalAggState::try_from_sql(
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

    let mut state = IncrementalAggState::try_from_sql(
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

    let mut state = IncrementalAggState::try_from_sql(
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
    let state = IncrementalAggState::try_from_sql(&ctx, sql, emit_changelog)
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

fn checkpoint_bytes(state: &mut IncrementalAggState) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&state.checkpoint_groups().unwrap())
        .unwrap()
        .to_vec()
}

#[tokio::test]
async fn test_distinct_flag_extracted() {
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

    let state = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name",
        false,
    )
    .await
    .unwrap()
    .expect("expected aggregate state");
    assert!(state.agg_specs[0].distinct, "DISTINCT flag should be set");
}

#[tokio::test]
async fn test_distinct_count_produces_correct_result() {
    let (_, mut state) =
        setup_agg_state("SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name")
            .await;

    // Pre-agg schema: name, __agg_input_1
    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));

    // Feed duplicates: value 10 appears 3 times for group "a"
    let batch = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a", "a"])),
            Arc::new(arrow::array::Float64Array::from(vec![
                10.0, 10.0, 10.0, 20.0,
            ])),
        ],
    )
    .unwrap();
    state.process_batch(&batch, i64::MIN).unwrap();

    let result = state.emit().unwrap();
    assert_eq!(result.len(), 1);
    let count_col = result[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count should be Int64");
    // DISTINCT count: {10.0, 20.0} = 2
    assert_eq!(count_col.value(0), 2, "COUNT(DISTINCT) should be 2");
}

#[tokio::test]
async fn test_distinct_sum_produces_correct_result() {
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(DISTINCT value) as total FROM events GROUP BY name")
            .await;

    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));

    // Feed duplicates: 10 appears twice
    let batch = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a"])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0, 10.0, 20.0])),
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
    // DISTINCT sum: 10 + 20 = 30 (not 10+10+20=40)
    assert!(
        (total_col.value(0) - 30.0).abs() < f64::EPSILON,
        "SUM(DISTINCT) should be 30, got {}",
        total_col.value(0)
    );
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

    let state = IncrementalAggState::try_from_sql(
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
async fn test_having_clause_detected() {
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

    let state = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT name, SUM(value) as total FROM events GROUP BY name HAVING SUM(value) > 100",
        false,
    )
    .await
    .unwrap()
    .expect("expected aggregate state");
    assert!(
        state.having_sql.is_some(),
        "HAVING predicate should be extracted"
    );
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

    let mut state = IncrementalAggState::try_from_sql(
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

    let mut state = IncrementalAggState::try_from_sql(
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

    let state = IncrementalAggState::try_from_sql(
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
    // AggregateFunction in expr_to_sql is used for HAVING
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

#[test]
fn test_expr_to_sql_aggregate_distinct() {
    use datafusion_expr::Expr;
    let count_udf = datafusion::functions_aggregate::count::count_udaf();
    let e = Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
        func: count_udf,
        params: datafusion_expr::expr::AggregateFunctionParams {
            args: vec![datafusion_expr::col("id")],
            distinct: true,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
    });
    let sql = expr_to_sql(&e);
    assert!(sql.contains("DISTINCT"), "should contain DISTINCT: {sql}");
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

    let state = IncrementalAggState::try_from_sql(
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
    state.max_groups = 2;
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

#[cfg(feature = "cluster")]
#[tokio::test]
async fn replay_rejects_changelog_state_without_its_group() {
    async fn changelog_sum_state(ctx: &SessionContext) -> IncrementalAggState {
        IncrementalAggState::try_from_sql(
            ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            true,
        )
        .await
        .unwrap()
        .unwrap()
    }

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
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem)).unwrap();

    let input_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));
    let input = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a"])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0])),
        ],
    )
    .unwrap();
    let mut source = changelog_sum_state(&ctx).await;
    source.process_batch(&input, 1_000).unwrap();
    source.emit().unwrap();
    let emitted = source.checkpoint_groups().unwrap().last_emitted;
    assert!(!emitted.is_empty());

    let mut empty_checkpoint = changelog_sum_state(&ctx).await.checkpoint_groups().unwrap();
    empty_checkpoint.last_emitted = emitted;

    let mut restored = changelog_sum_state(&ctx).await;
    let error = restored
        .restore_groups(&empty_checkpoint)
        .expect_err("whole-state restore must reject orphaned changelog state");
    assert!(error.to_string().contains("non-canonical empty"));
    assert!(restored.groups.is_empty());
    assert!(restored.last_emitted.is_empty());

    let mut merged = changelog_sum_state(&ctx).await;
    let error = merged.merge_groups(&empty_checkpoint).unwrap_err();
    assert!(error.to_string().contains("non-canonical empty"));
    assert!(merged.groups.is_empty());

    let delta = AggVnodeDelta {
        changed: empty_checkpoint,
    };
    let mut applied = changelog_sum_state(&ctx).await;
    let error = applied.apply_delta(&delta).unwrap_err();
    assert!(error.to_string().contains("non-canonical empty"));
    assert!(applied.groups.is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn delta_tracking_records_dirty_keys_per_vnode_and_resets_on_capture() {
    const VNODES: u32 = 4;
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
    state.set_delta_enabled(true);

    // First per-vnode capture establishes the delta baseline and starts a window.
    state.checkpoint_groups_by_vnode(VNODES).unwrap();
    assert!(state.dirty_keys_by_vnode.is_empty());

    let pre_agg = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        pre_agg,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();
    state.process_batch(&batch, 1000).unwrap();

    // Every mutated key is recorded, bucketed by vnode.
    let tracked: usize = state.dirty_keys_by_vnode.values().map(|s| s.len()).sum();
    assert_eq!(tracked, 3, "all mutated keys tracked in the delta window");

    // The next capture resets the window.
    state.checkpoint_groups_by_vnode(VNODES).unwrap();
    assert!(
        state.dirty_keys_by_vnode.is_empty(),
        "capture resets the per-vnode dirty set",
    );
}

/// FULL base + an ordered chain of deltas, replayed via `apply_vnode_chain`, reproduces the
/// producer exactly — and a chain re-bases to FULL once it reaches `chain_bound`.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn delta_chain_replay_reproduces_full_baseline() {
    use std::collections::BTreeMap;
    const V: u32 = 1; // single vnode → every key lands in vnode 0

    fn pre_agg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]))
    }
    fn feed(state: &mut IncrementalAggState, rows: &[(&str, f64)], ts: i64) {
        let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
        let vals: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
        let batch = RecordBatch::try_new(
            pre_agg_schema(),
            vec![
                Arc::new(arrow::array::StringArray::from(names)),
                Arc::new(arrow::array::Float64Array::from(vals)),
            ],
        )
        .unwrap();
        state.process_batch(&batch, ts).unwrap();
    }
    fn group_vals(state: &mut IncrementalAggState) -> BTreeMap<Vec<u8>, String> {
        state
            .groups
            .iter_mut()
            .map(|(k, v)| {
                (
                    k.as_ref().to_vec(),
                    format!("{:?}", v.accs[0].evaluate().unwrap()),
                )
            })
            .collect()
    }
    // Non-changelog agg: `checkpoint_delta_by_vnode` emits deltas (a changelog agg re-bases FULL).
    async fn agg(ctx: &SessionContext) -> IncrementalAggState {
        IncrementalAggState::try_from_sql(
            ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .unwrap()
    }
    fn delta_for_vnode0(cap: std::collections::HashMap<u32, VnodeCapture>) -> AggVnodeDelta {
        match cap.into_iter().find(|(v, _)| *v == 0).map(|(_, c)| c) {
            Some(VnodeCapture::Delta(d)) => d,
            _ => panic!("expected a DELTA for vnode 0"),
        }
    }

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
    let mem =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem)).unwrap();

    let mut producer = agg(&ctx).await;
    producer.set_delta_enabled(true);

    // Epoch 0: seed a,b,c — first capture re-bases FULL and opens the delta window.
    feed(&mut producer, &[("a", 1.0), ("b", 2.0), ("c", 3.0)], 1000);
    let cap0 = producer.checkpoint_delta_by_vnode(V, 8).unwrap();
    let Some(VnodeCapture::Full(base)) = cap0.into_iter().find(|(v, _)| *v == 0).map(|(_, c)| c)
    else {
        panic!("first capture must be FULL");
    };

    // Epoch 1: change a → DELTA. Epoch 2: change b + add e → DELTA.
    feed(&mut producer, &[("a", 10.0)], 2000);
    let d1 = delta_for_vnode0(producer.checkpoint_delta_by_vnode(V, 8).unwrap());
    feed(&mut producer, &[("b", 20.0), ("e", 5.0)], 3000);
    let d2 = delta_for_vnode0(producer.checkpoint_delta_by_vnode(V, 8).unwrap());

    // Replay FULL base + ordered deltas into a fresh consumer.
    let mut consumer = agg(&ctx).await;
    consumer.apply_vnode_chain(&base, &[d1, d2]).unwrap();
    assert_eq!(
        group_vals(&mut consumer),
        group_vals(&mut producer),
        "FULL base + ordered delta chain must reproduce the producer state",
    );

    // chain_bound = 1: the chain re-bases to FULL on the next capture.
    feed(&mut producer, &[("a", 11.0)], 4000);
    let rebased = producer.checkpoint_delta_by_vnode(V, 1).unwrap();
    assert!(
        matches!(rebased.get(&0), Some(VnodeCapture::Full(_))),
        "a chain at the bound must re-base to FULL",
    );
}

/// `force_full_rebase` makes the next capture re-base FULL even below `chain_bound`, so a failed
/// epoch's dirty-set clear can't silently drop its changes.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn force_full_rebase_recaptures_full_after_failed_epoch() {
    const V: u32 = 1; // single vnode → every key lands in vnode 0
    fn pre_agg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]))
    }
    fn feed(state: &mut IncrementalAggState, rows: &[(&str, f64)], ts: i64) {
        let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
        let vals: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
        let batch = RecordBatch::try_new(
            pre_agg_schema(),
            vec![
                Arc::new(arrow::array::StringArray::from(names)),
                Arc::new(arrow::array::Float64Array::from(vals)),
            ],
        )
        .unwrap();
        state.process_batch(&batch, ts).unwrap();
    }
    async fn agg(ctx: &SessionContext) -> IncrementalAggState {
        IncrementalAggState::try_from_sql(
            ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .unwrap()
    }
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
    let mem =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem)).unwrap();

    let mut producer = agg(&ctx).await;
    producer.set_delta_enabled(true);

    // Epoch 0 → FULL base; epoch 1 (well below chain_bound=8) → DELTA.
    feed(&mut producer, &[("a", 1.0), ("b", 2.0)], 1000);
    assert!(matches!(
        producer.checkpoint_delta_by_vnode(V, 8).unwrap().get(&0),
        Some(VnodeCapture::Full(_))
    ));
    feed(&mut producer, &[("a", 10.0)], 2000);
    assert!(
        matches!(
            producer.checkpoint_delta_by_vnode(V, 8).unwrap().get(&0),
            Some(VnodeCapture::Delta(_))
        ),
        "below the chain bound, a normal capture is a DELTA",
    );

    // Simulate the failed epoch's recovery hook: the next capture must re-base FULL.
    producer.force_full_rebase();
    feed(&mut producer, &[("b", 20.0)], 3000);
    assert!(
        matches!(
            producer.checkpoint_delta_by_vnode(V, 8).unwrap().get(&0),
            Some(VnodeCapture::Full(_))
        ),
        "force_full_rebase must re-base the next capture FULL, not chain a gapped delta",
    );
}

/// A changelog aggregate's delta chain must reproduce BOTH the group state and the
/// `last_emitted` dedup map, so the first post-recovery emit re-emits nothing and a
/// later change emits identically.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn delta_chain_replay_reproduces_changelog_last_emitted() {
    use std::collections::BTreeMap;
    const V: u32 = 1; // single vnode → every key lands in vnode 0

    fn pre_agg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]))
    }
    fn feed(state: &mut IncrementalAggState, rows: &[(&str, f64)], ts: i64) {
        let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
        let vals: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
        let batch = RecordBatch::try_new(
            pre_agg_schema(),
            vec![
                Arc::new(arrow::array::StringArray::from(names)),
                Arc::new(arrow::array::Float64Array::from(vals)),
            ],
        )
        .unwrap();
        state.process_batch(&batch, ts).unwrap();
    }
    // (groups, last_emitted) as comparable string maps.
    fn snapshot(
        state: &mut IncrementalAggState,
    ) -> (BTreeMap<Vec<u8>, String>, BTreeMap<Vec<u8>, String>) {
        let groups = state
            .groups
            .iter_mut()
            .map(|(k, v)| {
                (
                    k.as_ref().to_vec(),
                    format!("{:?}", v.accs[0].evaluate().unwrap()),
                )
            })
            .collect();
        let emitted = state
            .last_emitted
            .iter()
            .map(|(k, v)| (k.as_ref().to_vec(), format!("{v:?}")))
            .collect();
        (groups, emitted)
    }
    async fn agg(ctx: &SessionContext) -> IncrementalAggState {
        IncrementalAggState::try_from_sql(
            ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            true, // emit_changelog
        )
        .await
        .unwrap()
        .unwrap()
    }
    fn delta0(cap: std::collections::HashMap<u32, VnodeCapture>) -> AggVnodeDelta {
        match cap.into_iter().find(|(v, _)| *v == 0).map(|(_, c)| c) {
            Some(VnodeCapture::Delta(d)) => d,
            _ => panic!("expected a DELTA for vnode 0"),
        }
    }

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
    let mem =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem)).unwrap();

    let mut producer = agg(&ctx).await;
    producer.set_delta_enabled(true);

    // Epoch 0: seed + emit a,b,c, then FULL re-base (must carry last_emitted).
    feed(&mut producer, &[("a", 1.0), ("b", 2.0), ("c", 3.0)], 1000);
    producer.emit().unwrap();
    let Some(VnodeCapture::Full(base)) = producer
        .checkpoint_delta_by_vnode(V, 8)
        .unwrap()
        .into_iter()
        .find(|(v, _)| *v == 0)
        .map(|(_, c)| c)
    else {
        panic!("first capture must be FULL");
    };
    assert!(
        !base.last_emitted.is_empty(),
        "a changelog FULL re-base must carry the dedup map",
    );

    // Epoch 1: change a, emit → DELTA carries a's updated last_emitted.
    feed(&mut producer, &[("a", 10.0)], 2000);
    producer.emit().unwrap();
    let d1 = delta0(producer.checkpoint_delta_by_vnode(V, 8).unwrap());

    // Epoch 2: change b + add d, emit → DELTA.
    feed(&mut producer, &[("b", 20.0), ("d", 4.0)], 3000);
    producer.emit().unwrap();
    let d2 = delta0(producer.checkpoint_delta_by_vnode(V, 8).unwrap());

    // Replay FULL base + ordered deltas into a fresh consumer.
    let mut consumer = agg(&ctx).await;
    consumer.set_delta_enabled(true);
    consumer.apply_vnode_chain(&base, &[d1, d2]).unwrap();

    let (pg, pe) = snapshot(&mut producer);
    let (cg, ce) = snapshot(&mut consumer);
    assert_eq!(cg, pg, "groups must match after chain replay");
    assert_eq!(
        ce, pe,
        "last_emitted dedup map must match after chain replay"
    );

    // No new input → the recovered dedup map must re-emit NOTHING (no duplicates).
    let drained: usize = consumer
        .emit()
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(
        drained, 0,
        "recovered changelog state must not re-emit unchanged groups"
    );

    // A genuine change emits identically on both.
    feed(&mut producer, &[("a", 100.0)], 4000);
    feed(&mut consumer, &[("a", 100.0)], 4000);
    let pr: usize = producer
        .emit()
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    let cr: usize = consumer
        .emit()
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(
        cr, pr,
        "post-recovery emit must produce identical changelog output"
    );
}

/// A global (no-GROUP-BY) changelog aggregate with delta checkpoints must capture without
/// panicking on the empty group key (`row_to_scalar_key_with_types` on the global sentinel),
/// and the captured slice must restore to the same value.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn global_changelog_delta_checkpoint_roundtrips() {
    async fn agg(ctx: &SessionContext) -> IncrementalAggState {
        IncrementalAggState::try_from_sql(ctx, "SELECT SUM(value) as total FROM events", true)
            .await
            .unwrap()
            .unwrap()
    }

    let ctx = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let dummy = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(arrow::array::Float64Array::from(vec![0.0]))],
    )
    .unwrap();
    let mem =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem)).unwrap();

    let pre = Arc::new(Schema::new(vec![Field::new(
        "__agg_input_1",
        DataType::Float64,
        true,
    )]));
    let feed = |state: &mut IncrementalAggState, vals: Vec<f64>, ts: i64| {
        let batch = RecordBatch::try_new(
            Arc::clone(&pre),
            vec![Arc::new(arrow::array::Float64Array::from(vals))],
        )
        .unwrap();
        state.process_batch(&batch, ts).unwrap();
    };

    let mut state = agg(&ctx).await;
    state.set_delta_enabled(true);
    feed(&mut state, vec![1.0, 2.0, 3.0], 1000);
    state.emit().unwrap();

    // Before the fix this panicked: the empty global key hit convert_rows on a 0-field converter.
    let caps = state.checkpoint_delta_by_vnode(1, 8).unwrap();
    assert!(
        caps.contains_key(&0),
        "the global group is captured under vnode 0"
    );

    // Restore into a fresh aggregate; the single global group must total 6.0.
    let mut restored = agg(&ctx).await;
    match caps.get(&0).expect("vnode-0 capture") {
        VnodeCapture::Full(cp) => {
            restored.merge_groups(cp).unwrap();
        }
        VnodeCapture::Delta(d) => {
            restored.apply_delta(d).unwrap();
        }
    }
    let value = restored
        .groups
        .get_mut(&global_aggregate_key())
        .expect("global group restored")
        .accs[0]
        .evaluate()
        .unwrap();
    assert_eq!(value, ScalarValue::Float64(Some(6.0)));
}

/// `drop_vnodes` purges ALL state for a revoked vnode — resident groups, `last_emitted`, the
/// per-vnode delta maps, and the chain length — while a sibling vnode is untouched and
/// `last_emitted ⊆ groups` still holds. This prevents stale keys from surviving rehydration.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn drop_vnodes_purges_revoked_keeps_sibling() {
    use arrow::array::ArrayRef;
    const VC: u32 = 8;

    fn pre_agg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]))
    }
    fn feed(state: &mut IncrementalAggState, rows: &[(&str, f64)], ts: i64) {
        let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
        let vals: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
        let batch = RecordBatch::try_new(
            pre_agg_schema(),
            vec![
                Arc::new(arrow::array::StringArray::from(names)),
                Arc::new(arrow::array::Float64Array::from(vals)),
            ],
        )
        .unwrap();
        state.process_batch(&batch, ts).unwrap();
    }
    async fn agg(ctx: &SessionContext) -> IncrementalAggState {
        IncrementalAggState::try_from_sql(
            ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            true,
        )
        .await
        .unwrap()
        .unwrap()
    }

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
    let mem =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]]).unwrap();
    ctx.register_table("events", Arc::new(mem)).unwrap();

    let mut state = agg(&ctx).await;
    state.set_delta_enabled(true);

    let row_of = |state: &IncrementalAggState, key: &str| -> arrow::row::OwnedRow {
        let cols: Vec<ArrayRef> = vec![Arc::new(arrow::array::StringArray::from(vec![key]))];
        state
            .row_converter
            .convert_columns(&cols)
            .unwrap()
            .row(0)
            .owned()
    };
    let vnode_of = |state: &IncrementalAggState, key: &str| {
        IncrementalAggState::vnode_for_group_key(
            state.num_group_cols,
            &row_of(state, key),
            NonZeroU32::new(VC).unwrap(),
        )
    };

    // A vnode `y` with two keys and a distinct vnode `x`.
    let cands: Vec<String> = (0..64).map(|i| format!("k{i}")).collect();
    let mut by_v: std::collections::BTreeMap<u32, Vec<String>> = std::collections::BTreeMap::new();
    for c in &cands {
        by_v.entry(vnode_of(&state, c)).or_default().push(c.clone());
    }
    let vy = *by_v
        .iter()
        .find(|(_, ks)| ks.len() >= 2)
        .map(|(v, _)| v)
        .expect("a vnode with two keys");
    let vx = *by_v
        .keys()
        .find(|v| **v != vy)
        .expect("a second distinct vnode");
    let (y_first, y_second) = (by_v[&vy][0].clone(), by_v[&vy][1].clone());
    let x_key = by_v[&vx][0].clone();

    feed(
        &mut state,
        &[(&y_first, 1.0), (&y_second, 2.0), (&x_key, 3.0)],
        1000,
    );
    state.emit().unwrap();
    let _ = state.checkpoint_delta_by_vnode(VC, 8).unwrap(); // chain_len[vx]=[vy]=0

    let y_second_row = row_of(&state, &y_second);

    // Re-dirty both vnodes so the per-vnode delta maps are populated at drop time.
    feed(&mut state, &[(&y_first, 5.0), (&x_key, 7.0)], 2000);

    let y_first_row = row_of(&state, &y_first);
    let x_row = row_of(&state, &x_key);
    assert!(
        state.groups.contains_key(&y_first_row),
        "precondition: first y group present"
    );
    assert!(
        state.groups.contains_key(&x_row),
        "precondition: x resident"
    );

    // Revoke vy.
    let revoked: rustc_hash::FxHashSet<u32> = [vy].into_iter().collect();
    state.drop_vnodes(&revoked, VC).unwrap();

    // Every vy entry is gone.
    assert!(
        !state.groups.contains_key(&y_first_row),
        "revoked first group dropped"
    );
    assert!(
        !state.groups.contains_key(&y_second_row),
        "revoked second group dropped"
    );
    assert!(
        !state.last_emitted.contains_key(&y_first_row),
        "revoked last_emitted dropped"
    );
    assert!(!state.dirty_keys_by_vnode.contains_key(&vy));
    assert!(!state.last_emitted_dirty_by_vnode.contains_key(&vy));
    assert!(!state.delta_chain_len.contains_key(&vy));

    // The sibling vnode is untouched.
    assert!(
        state.groups.contains_key(&x_row),
        "sibling resident group kept"
    );
    assert!(
        state.delta_chain_len.contains_key(&vx),
        "sibling chain kept"
    );

    // Invariant preserved: the dedup map stays a subset of resident groups.
    for k in state.last_emitted.keys() {
        assert!(
            state.groups.contains_key(k),
            "last_emitted must remain a subset of groups",
        );
    }
}

#[tokio::test]
async fn empty_restore_rejects_every_noncanonical_payload() {
    let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
    let (_, mut state) = setup_agg_state(sql).await;
    let empty = state.checkpoint_groups().unwrap();
    assert!(empty.last_updated_ms.is_empty());

    let mut keys_only = empty.clone();
    keys_only.keys_ipc = vec![1];
    let mut accumulators_only = empty.clone();
    accumulators_only.acc_state_ipc = vec![vec![1]];
    let mut changelog_only = empty;
    changelog_only.last_emitted = vec![EmittedCheckpoint {
        key: vec![1],
        values: vec![1],
    }];

    for checkpoint in [keys_only, accumulators_only, changelog_only] {
        let error = state.restore_groups(&checkpoint).unwrap_err();
        assert!(error.to_string().contains("non-canonical empty"));
        assert!(state.groups.is_empty());
        assert!(state.last_emitted.is_empty());
    }
}

#[tokio::test]
async fn restore_rejects_malformed_changelog_values_before_mutation() {
    let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
    let (_, mut donor) = setup_agg_state_with_changelog(sql, true).await;
    donor
        .process_batch(&sum_pre_agg_batch(&["a"], &[10.0]), 1)
        .unwrap();
    donor.emit().unwrap();
    let valid = donor.checkpoint_groups().unwrap();
    assert_eq!(valid.last_emitted.len(), 1);

    let corruptions = [
        (Vec::new(), "arity mismatch"),
        (
            scalars_to_ipc(&[ScalarValue::Utf8(Some("wrong".into()))]).unwrap(),
            "type mismatch",
        ),
        (
            scalars_to_ipc(&[
                ScalarValue::Float64(Some(10.0)),
                ScalarValue::Float64(Some(20.0)),
            ])
            .unwrap(),
            "arity mismatch",
        ),
    ];

    for (values, expected) in corruptions {
        let (_, mut target) = setup_agg_state_with_changelog(sql, true).await;
        target
            .process_batch(&sum_pre_agg_batch(&["z"], &[9.0]), 1)
            .unwrap();
        target.emit().unwrap();
        let before = checkpoint_bytes(&mut target);

        let mut malformed = valid.clone();
        malformed.last_emitted[0].values = values;
        let error = target.restore_groups(&malformed).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(checkpoint_bytes(&mut target), before);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn vnode_replay_rejects_malformed_changelog_values_before_mutation() {
    let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
    let (_, mut donor) = setup_agg_state_with_changelog(sql, true).await;
    donor
        .process_batch(&sum_pre_agg_batch(&["a"], &[10.0]), 1)
        .unwrap();
    donor.emit().unwrap();
    let valid = donor.checkpoint_groups().unwrap();

    let (_, mut target) = setup_agg_state_with_changelog(sql, true).await;
    target
        .process_batch(&sum_pre_agg_batch(&["z"], &[9.0]), 1)
        .unwrap();
    target.emit().unwrap();
    let before = checkpoint_bytes(&mut target);

    let mut wrong_type = valid.clone();
    wrong_type.last_emitted[0].values =
        scalars_to_ipc(&[ScalarValue::Utf8(Some("wrong".into()))]).unwrap();
    let error = target.merge_groups(&wrong_type).unwrap_err();
    assert!(error.to_string().contains("type mismatch"), "{error}");
    assert_eq!(checkpoint_bytes(&mut target), before);

    let mut wrong_arity = valid;
    wrong_arity.last_emitted[0].values = scalars_to_ipc(&[
        ScalarValue::Float64(Some(10.0)),
        ScalarValue::Float64(Some(20.0)),
    ])
    .unwrap();
    let error = target
        .apply_delta(&AggVnodeDelta {
            changed: wrong_arity,
        })
        .unwrap_err();
    assert!(error.to_string().contains("arity mismatch"), "{error}");
    assert_eq!(checkpoint_bytes(&mut target), before);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn bulk_merge_and_restore_reject_duplicate_group_keys() {
    let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
    let (_, mut donor) = setup_agg_state(sql).await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    donor.process_batch(&batch, i64::MIN).unwrap();
    let one_group = donor.checkpoint_groups().unwrap();
    let encoded = bytes::Bytes::from(
        rkyv::to_bytes::<rkyv::rancor::Error>(&one_group)
            .unwrap()
            .to_vec(),
    );
    let error = merge_serialized_agg_cps(&[encoded.clone(), encoded]).unwrap_err();
    assert!(error.to_string().contains("not disjoint"));

    // Construct a corrupt duplicate image directly so every live restore/apply entry point is
    // still covered independently of the fail-closed bulk merge helper.
    let mut duplicated = one_group.clone();
    duplicated.keys_ipc = concat_columnar_ipc(&duplicated.keys_ipc, &one_group.keys_ipc).unwrap();
    for (dst, src) in duplicated
        .acc_state_ipc
        .iter_mut()
        .zip(&one_group.acc_state_ipc)
    {
        *dst = concat_columnar_ipc(dst, src).unwrap();
    }
    duplicated.last_updated_ms.extend(one_group.last_updated_ms);
    duplicated.last_emitted.extend(one_group.last_emitted);

    let (_, mut restored) = setup_agg_state(sql).await;
    let error = restored.restore_groups(&duplicated).unwrap_err();
    assert!(error.to_string().contains("duplicate group key"));
    assert!(restored.groups.is_empty());

    let (_, mut merged) = setup_agg_state(sql).await;
    let error = merged.merge_groups(&duplicated).unwrap_err();
    assert!(error.to_string().contains("duplicate group key"));
    assert!(merged.groups.is_empty());

    let (_, mut applied) = setup_agg_state(sql).await;
    let error = applied
        .apply_delta(&AggVnodeDelta {
            changed: duplicated,
        })
        .unwrap_err();
    assert!(error.to_string().contains("duplicate group key"));
    assert!(applied.groups.is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn vnode_chain_failure_is_atomic_and_successful_retry_is_idempotent() {
    let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
    let (ctx, mut live) = setup_agg_state(sql).await;
    let batch = |name: &str, value: f64| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![name])),
                Arc::new(arrow::array::Float64Array::from(vec![value])),
            ],
        )
        .unwrap()
    };
    live.process_batch(&batch("a", 10.0), 1).unwrap();

    let mut base_donor = IncrementalAggState::try_from_sql(&ctx, sql, false)
        .await
        .unwrap()
        .unwrap();
    base_donor.process_batch(&batch("a", 1.0), 2).unwrap();
    let base = base_donor.checkpoint_groups().unwrap();

    let mut delta_donor = IncrementalAggState::try_from_sql(&ctx, sql, false)
        .await
        .unwrap()
        .unwrap();
    delta_donor.process_batch(&batch("b", 5.0), 3).unwrap();
    let valid_changed = delta_donor.checkpoint_groups().unwrap();
    let mut invalid_changed = valid_changed.clone();
    invalid_changed.last_emitted.push(EmittedCheckpoint {
        key: scalars_to_ipc(&[ScalarValue::Utf8(Some("missing".into()))]).unwrap(),
        values: scalars_to_ipc(&[ScalarValue::Float64(Some(99.0))]).unwrap(),
    });
    let invalid_late_delta = AggVnodeDelta {
        changed: invalid_changed,
    };

    let before = rkyv::to_bytes::<rkyv::rancor::Error>(&live.checkpoint_groups().unwrap())
        .unwrap()
        .to_vec();
    let error = live
        .apply_vnode_chain(&base, &[invalid_late_delta])
        .unwrap_err();
    assert!(error.to_string().contains("missing group"));
    let after = rkyv::to_bytes::<rkyv::rancor::Error>(&live.checkpoint_groups().unwrap())
        .unwrap()
        .to_vec();
    assert_eq!(
        after, before,
        "failed chain changed the live checkpoint image"
    );

    let valid_delta = AggVnodeDelta {
        changed: valid_changed,
    };
    live.apply_vnode_chain(&base, std::slice::from_ref(&valid_delta))
        .unwrap();
    let mut first_values: Vec<f64> = live
        .groups
        .values_mut()
        .map(|entry| match entry.accs[0].evaluate().unwrap() {
            ScalarValue::Float64(Some(value)) => value,
            other => panic!("unexpected aggregate value {other:?}"),
        })
        .collect();
    first_values.sort_by(f64::total_cmp);
    assert_eq!(first_values, vec![1.0, 5.0]);

    live.apply_vnode_chain(&base, std::slice::from_ref(&valid_delta))
        .unwrap();
    let mut retry_values: Vec<f64> = live
        .groups
        .values_mut()
        .map(|entry| match entry.accs[0].evaluate().unwrap() {
            ScalarValue::Float64(Some(value)) => value,
            other => panic!("unexpected aggregate value {other:?}"),
        })
        .collect();
    retry_values.sort_by(f64::total_cmp);
    assert_eq!(retry_values, first_values);
}

#[tokio::test]
async fn group_cardinality_existing_groups_update_at_the_limit() {
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

    state.max_groups = 2;

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
async fn test_agg_checkpoint_roundtrip_single_group() {
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

    // Feed data
    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a", "a"])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0])),
        ],
    )
    .unwrap();
    state.process_batch(&batch, i64::MIN).unwrap();

    // Checkpoint
    let cp = state.checkpoint_groups().unwrap();
    assert_eq!(cp.last_updated_ms.len(), 1);

    // Create a fresh state and restore
    let (_, mut state2) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
    let restored = state2.restore_groups(&cp).unwrap();
    assert_eq!(restored, 1);

    // Emit and verify value matches
    let result = state2.emit().unwrap();
    assert_eq!(result.len(), 1);
    let total = result[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!(
        (total.value(0) - 30.0).abs() < f64::EPSILON,
        "Restored SUM should be 30, got {}",
        total.value(0)
    );
}

#[tokio::test]
async fn test_agg_checkpoint_roundtrip_multi_group() {
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec![
                "a", "b", "a", "b", "c",
            ])),
            Arc::new(arrow::array::Float64Array::from(vec![
                10.0, 20.0, 30.0, 40.0, 50.0,
            ])),
        ],
    )
    .unwrap();
    state.process_batch(&batch, i64::MIN).unwrap();

    let cp = state.checkpoint_groups().unwrap();
    assert_eq!(cp.last_updated_ms.len(), 3);

    let (_, mut state2) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
    let restored = state2.restore_groups(&cp).unwrap();
    assert_eq!(restored, 3);

    let result = state2.emit().unwrap();
    assert_eq!(result[0].num_rows(), 3);
}

/// Columnar checkpoint round-trip with a mix of accumulator shapes
/// (SUM, COUNT(*), MAX) across several groups: restored emit must equal
/// the original emit row-for-row.
#[tokio::test]
async fn test_agg_checkpoint_roundtrip_mixed_accumulators() {
    let sql = "SELECT name, SUM(value) AS s, COUNT(*) AS c, MAX(value) AS m \
               FROM events GROUP BY name";
    let (_, mut state) = setup_agg_state(sql).await;

    // Pre-agg layout for [name] + SUM(value), COUNT(*), MAX(value):
    // group col, then __agg_input_1 (SUM), __agg_input_2 (COUNT* dummy bool), __agg_input_3 (MAX).
    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
        Field::new("__agg_input_2", DataType::Boolean, true),
        Field::new("__agg_input_3", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec![
                "a", "b", "a", "b", "c", "a",
            ])),
            Arc::new(arrow::array::Float64Array::from(vec![
                10.0, 20.0, 30.0, 40.0, 50.0, 5.0,
            ])),
            Arc::new(arrow::array::BooleanArray::from(vec![true; 6])),
            Arc::new(arrow::array::Float64Array::from(vec![
                10.0, 20.0, 30.0, 40.0, 50.0, 5.0,
            ])),
        ],
    )
    .unwrap();
    state.process_batch(&batch, i64::MIN).unwrap();
    let original = state.emit().unwrap();

    let cp = state.checkpoint_groups().unwrap();
    assert_eq!(cp.last_updated_ms.len(), 3);

    let (_, mut state2) = setup_agg_state(sql).await;
    assert_eq!(state2.restore_groups(&cp).unwrap(), 3);
    let restored = state2.emit().unwrap();

    // Compare as (name -> (s, c, m)) maps so HashMap iteration order is irrelevant.
    let collect = |batches: &[RecordBatch]| {
        let mut out: std::collections::BTreeMap<String, (f64, i64, f64)> =
            std::collections::BTreeMap::new();
        for b in batches {
            let names = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let s = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            let c = b
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            let m = b
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.insert(
                    names.value(i).to_string(),
                    (s.value(i), c.value(i), m.value(i)),
                );
            }
        }
        out
    };
    assert_eq!(collect(&original), collect(&restored));
}

#[tokio::test]
async fn test_restore_fingerprint_mismatch_errors() {
    let (_, mut state) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

    // Feed data and checkpoint
    let pre_agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("__agg_input_1", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&pre_agg_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a"])),
            Arc::new(arrow::array::Float64Array::from(vec![10.0])),
        ],
    )
    .unwrap();
    state.process_batch(&batch, i64::MIN).unwrap();
    let mut cp = state.checkpoint_groups().unwrap();

    // Tamper with fingerprint
    cp.fingerprint = 999_999;

    // Restore should fail
    let (_, mut state2) =
        setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
    let result = state2.restore_groups(&cp);
    assert!(result.is_err(), "Fingerprint mismatch should error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("fingerprint mismatch"),
        "Error should mention fingerprint: {err}"
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

    let mut state = IncrementalAggState::try_from_sql(
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
async fn changelog_restore_emits_no_duplicates_then_resumes() {
    // After recovery, restored groups are already reflected downstream (last_emitted
    // is restored in lockstep with groups), so the first post-restore emit must be
    // empty — re-emitting would duplicate. A later change must still emit normally.
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let seed = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![seed]]).unwrap();
    ctx.register_table("t", Arc::new(mem)).unwrap();

    let sql = "SELECT symbol, SUM(price) AS total FROM t GROUP BY symbol";
    let mut state = IncrementalAggState::try_from_sql(&ctx, sql, true)
        .await
        .unwrap()
        .unwrap();

    let pre_agg = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("price", DataType::Int64, true),
    ]));
    let b1 = RecordBatch::try_new(
        Arc::clone(&pre_agg),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    state.process_batch(&b1, 1000).unwrap();
    assert_eq!(
        state
            .emit()
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    ); // AAPL +1, GOOG +1

    // Recover into a fresh state from the post-emit checkpoint.
    let cp = state.checkpoint_groups().unwrap();
    let mut restored = IncrementalAggState::try_from_sql(&ctx, sql, true)
        .await
        .unwrap()
        .unwrap();
    restored.restore_groups(&cp).unwrap();

    // First emit after restore: nothing new → empty (no duplicate inserts).
    let r0 = restored.emit().unwrap();
    assert!(
        r0.is_empty() || r0.iter().all(|b| b.num_rows() == 0),
        "restored groups must not be re-emitted"
    );

    // A real change resumes normally: AAPL 100 -> 150 emits retract + insert.
    let b2 = RecordBatch::try_new(
        pre_agg,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
            Arc::new(arrow::array::Int64Array::from(vec![50])),
        ],
    )
    .unwrap();
    restored.process_batch(&b2, 2000).unwrap();
    assert_eq!(
        restored
            .emit()
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2,
        "post-restore change must emit retract+insert"
    );
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

    let mut state = IncrementalAggState::try_from_sql(
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
async fn test_min_accepted_over_changelog_upstream() {
    // MIN is now supported over changelog streams via retractable accumulators.
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

    let result = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT symbol, MIN(price) AS low FROM upstream GROUP BY symbol",
        false,
    )
    .await;
    assert!(result.is_ok(), "MIN should be accepted over changelog");
}

#[tokio::test]
async fn test_unsupported_agg_rejected_over_changelog() {
    // STDDEV is NOT supported over changelog streams.
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

    let result = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT symbol, STDDEV(price) AS sd FROM upstream GROUP BY symbol",
        false,
    )
    .await;
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(msg.contains("Cannot compute"), "got: {msg}");
        }
        Ok(_) => panic!("expected error for STDDEV over changelog upstream"),
    }
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

    let mut state = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT region, COUNT(*) AS cnt FROM upstream GROUP BY region",
        false,
    )
    .await
    .unwrap()
    .unwrap();

    assert!(state.weight_col_idx.is_some());

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
async fn changelog_retractable_survives_checkpoint() {
    // checkpoint_groups() rebuilds each live accumulator from its snapshot.
    // For a changelog (`__weight`) aggregate the live accumulator is the
    // retractable variant; rebuilding it as a plain one would silently drop
    // retraction. Prove a retract still works *after* a mid-stream checkpoint.
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

    let mut state = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT region, COUNT(*) AS cnt FROM upstream GROUP BY region",
        false,
    )
    .await
    .unwrap()
    .unwrap();
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

    state
        .process_batch(&mk(vec!["US", "US", "EU"], vec![1, 1, 1]), 1000)
        .unwrap();
    let _ = state.emit().unwrap();

    // Mid-stream checkpoint — must keep the live accumulators retractable.
    let _ = state.checkpoint_groups().unwrap();

    // Retract one US row; a downgraded plain accumulator could not.
    state
        .process_batch(&mk(vec!["US"], vec![-1]), 2000)
        .unwrap();
    let r = state.emit().unwrap();
    let regions = r[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let counts = r[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    for i in 0..r[0].num_rows() {
        match regions.value(i) {
            "US" => assert_eq!(counts.value(i), 1, "US count must be 1 after retract"),
            "EU" => assert_eq!(counts.value(i), 1),
            other => panic!("unexpected region: {other}"),
        }
    }
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

    let mut state = IncrementalAggState::try_from_sql(
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
            Field::new("price", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
            Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
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
            Field::new("price", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US"])),
            Arc::new(arrow::array::Int64Array::from(vec![10])),
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

#[tokio::test]
async fn test_cascaded_min_over_changelog() {
    // Single MIN aggregate — pre-agg schema: [region, price, __weight]
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

    let mut state = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT region, MIN(price) AS lo FROM upstream GROUP BY region",
        false,
    )
    .await
    .unwrap()
    .unwrap();

    // Insert 10, 20, 30
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
            Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
            Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
        ],
    )
    .unwrap();
    state.process_batch(&b1, 1000).unwrap();
    let r1 = state.emit().unwrap();
    let mins = r1[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(mins.value(0), 10);

    // Retract current min (10) -> new min = 20
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US"])),
            Arc::new(arrow::array::Int64Array::from(vec![10])),
            Arc::new(arrow::array::Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();
    state.process_batch(&b2, 2000).unwrap();
    let r2 = state.emit().unwrap();
    let mins2 = r2[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(mins2.value(0), 20, "min should be 20 after retracting 10");

    // Retract 20, retract 30 -> empty -> NULL
    let b3 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US", "US"])),
            Arc::new(arrow::array::Int64Array::from(vec![20, 30])),
            Arc::new(arrow::array::Int64Array::from(vec![-1, -1])),
        ],
    )
    .unwrap();
    state.process_batch(&b3, 3000).unwrap();
    let r3 = state.emit().unwrap();
    assert!(
        r3[0].column(1).is_null(0),
        "min should be NULL after all values retracted"
    );
}

#[tokio::test]
async fn test_cascaded_max_retract_over_changelog() {
    // Single MAX aggregate — pre-agg schema: [region, price, __weight]
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

    let mut state = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT region, MAX(price) AS hi FROM upstream GROUP BY region",
        false,
    )
    .await
    .unwrap()
    .unwrap();

    // Insert 10, 20, 30
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
            Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
            Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
        ],
    )
    .unwrap();
    state.process_batch(&b1, 1000).unwrap();
    let r1 = state.emit().unwrap();
    let maxs = r1[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(maxs.value(0), 30);

    // Retract current max (30) -> new max = 20
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US"])),
            Arc::new(arrow::array::Int64Array::from(vec![30])),
            Arc::new(arrow::array::Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();
    state.process_batch(&b2, 2000).unwrap();
    let r2 = state.emit().unwrap();
    let maxs2 = r2[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(maxs2.value(0), 20, "max should be 20 after retracting 30");
}

#[tokio::test]
async fn test_cascaded_mixed_aggregates_over_changelog() {
    // Mixed: SUM + COUNT(*) + AVG + MIN + MAX on same column.
    // Pre-agg schema: [region, amount(SUM), TRUE(COUNT), amount(AVG),
    //                   amount(MIN), amount(MAX), __weight] = 7 columns.
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

    let result = IncrementalAggState::try_from_sql(
        &ctx,
        "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt, \
         AVG(amount) AS avg_amt, MIN(amount) AS lo, MAX(amount) AS hi \
         FROM upstream GROUP BY region",
        false,
    )
    .await;
    assert!(result.is_ok(), "mixed aggregates should be accepted");
    let mut state = result.unwrap().unwrap();

    // Pre-agg has 7 cols: [region, amt, TRUE, amt, amt, amt, __weight].
    // Build matching batch.
    let b1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Int64, true),
            Field::new("__agg_input_2", DataType::Boolean, true),
            Field::new("__agg_input_3", DataType::Int64, true),
            Field::new("__agg_input_4", DataType::Int64, true),
            Field::new("__agg_input_5", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
            Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // SUM input
            Arc::new(arrow::array::BooleanArray::from(vec![true, true, true])), // COUNT(*)
            Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // AVG input
            Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // MIN input
            Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // MAX input
            Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),    // weight
        ],
    )
    .unwrap();
    state.process_batch(&b1, 1000).unwrap();
    let r1 = state.emit().unwrap();
    assert_eq!(r1[0].num_rows(), 1);

    // Retract 10, insert 40.
    let b2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Int64, true),
            Field::new("__agg_input_2", DataType::Boolean, true),
            Field::new("__agg_input_3", DataType::Int64, true),
            Field::new("__agg_input_4", DataType::Int64, true),
            Field::new("__agg_input_5", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["US", "US"])),
            Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
            Arc::new(arrow::array::BooleanArray::from(vec![true, true])),
            Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
            Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
            Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
            Arc::new(arrow::array::Int64Array::from(vec![-1, 1])),
        ],
    )
    .unwrap();
    state.process_batch(&b2, 2000).unwrap();
    let r2 = state.emit().unwrap();
    // {20, 30, 40}: SUM=90, COUNT=3, AVG=30, MIN=20, MAX=40
    let b = &r2[0];
    let sum_col = b
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let cnt_col = b
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let avg_col = b
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let min_col = b
        .column(4)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let max_col = b
        .column(5)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(sum_col.value(0), 90, "SUM should be 90");
    assert_eq!(cnt_col.value(0), 3, "COUNT should be 3");
    assert!((avg_col.value(0) - 30.0).abs() < 0.001, "AVG should be 30");
    assert_eq!(min_col.value(0), 20, "MIN should be 20");
    assert_eq!(max_col.value(0), 40, "MAX should be 40");
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

/// Profiling (not a correctness test): measures the on-task whole-node
/// `checkpoint_groups` capture cost vs group count — the cost an incremental
/// (dirty-only) capture would shrink. Reports total time, ns/group,
/// and serialized size, so the incremental win for a given dirty ratio is
/// `ns/group * dirty_count`. `#[ignore]`d; run in release:
/// `cargo test -p laminar-db --release profile_checkpoint_capture -- --ignored --nocapture`
#[tokio::test]
#[ignore = "profiling; run with --release --ignored --nocapture"]
async fn profile_checkpoint_capture_cost() {
    for &n in &[10_000usize, 100_000, 1_000_000] {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![0i64])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
            .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();
        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT id, SUM(value) AS total FROM events GROUP BY id",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        let pre = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let ids: Vec<i64> = (0_i64..).take(n).collect();
        let mut values = Vec::with_capacity(n);
        let mut value = 0.0;
        for _ in 0..n {
            values.push(value);
            value += 1.0;
        }
        let batch = RecordBatch::try_new(
            pre,
            vec![
                Arc::new(arrow::array::Int64Array::from(ids)),
                Arc::new(arrow::array::Float64Array::from(values)),
            ],
        )
        .unwrap();
        state.process_batch(&batch, 0).unwrap();

        let t0 = std::time::Instant::now();
        let cp = state.checkpoint_groups().unwrap();
        let elapsed = t0.elapsed();

        let bytes: usize = cp.keys_ipc.len()
            + cp.acc_state_ipc.iter().map(Vec::len).sum::<usize>()
            + cp.last_updated_ms.len() * 8;
        let ns_per_group = elapsed.as_secs_f64() * 1_000_000_000.0 / value;
        println!(
            "checkpoint_groups: {n:>9} groups -> {elapsed:>11.2?}  ({ns_per_group:6.0} ns/group)  ~{} KiB",
            bytes / 1024
        );
        assert_eq!(cp.last_updated_ms.len(), n);
    }
}
