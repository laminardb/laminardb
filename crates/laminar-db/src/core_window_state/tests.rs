use super::*;
use arrow::array::{Int64Array, StringArray};
use datafusion::execution::FunctionRegistry;
use laminar_core::operator::window::EmitStrategy as CoreEmit;

/// Build a pre-agg batch with 1 group col (Utf8), 1 agg input (Int64),
/// and 1 timestamp col (Int64).
fn make_pre_agg_batch(groups: Vec<&str>, values: Vec<i64>, timestamps: Vec<i64>) -> RecordBatch {
    assert_eq!(groups.len(), values.len());
    assert_eq!(groups.len(), timestamps.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("__agg_input_1", DataType::Int64, false),
        Field::new("__cw_ts", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                groups.into_iter().map(String::from).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(values)),
            Arc::new(Int64Array::from(timestamps)),
        ],
    )
    .unwrap()
}

fn sql_window_context() -> (SessionContext, SchemaRef) {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap();
    ctx.register_table("events", Arc::new(table)).unwrap();
    (ctx, schema)
}

fn sum_total(batch: &RecordBatch) -> i64 {
    batch
        .column_by_name("total")
        .expect("output schema has 'total' column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("'total' is Int64")
        .value(0)
}

fn key_groups() -> KeyGroupCount {
    KeyGroupCount::try_from(1_u16).unwrap()
}

#[test]
fn timestamp_extraction_rejects_reserved_and_overflowing_values() {
    fn batch(array: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    let nullable = batch(Arc::new(Int64Array::from(vec![Some(42), None])));
    assert_eq!(
        extract_i64_timestamps(&nullable, 0).unwrap(),
        [42, NULL_TIMESTAMP]
    );

    let reserved = [
        batch(Arc::new(Int64Array::from(vec![i64::MIN]))),
        batch(Arc::new(arrow::array::TimestampMillisecondArray::from(
            vec![i64::MIN],
        ))),
    ];
    for batch in reserved {
        let error = extract_i64_timestamps(&batch, 0).unwrap_err();
        assert!(matches!(&error, DbError::PipelineTerminal(_)));
        assert!(error.requires_pipeline_halt());
    }

    let overflowing_seconds = batch(Arc::new(arrow::array::TimestampSecondArray::from(vec![
        i64::MAX,
    ])));
    let error = extract_i64_timestamps(&overflowing_seconds, 0).unwrap_err();
    assert!(matches!(&error, DbError::PipelineTerminal(_)));
    assert!(error.requires_pipeline_halt());

    let negative_submillisecond = [
        batch(Arc::new(arrow::array::TimestampMicrosecondArray::from(
            vec![-1],
        ))),
        batch(Arc::new(arrow::array::TimestampNanosecondArray::from(
            vec![-1],
        ))),
    ];
    for batch in negative_submillisecond {
        assert_eq!(extract_i64_timestamps(&batch, 0).unwrap(), [-1]);
    }
}

fn checkpoint_all(state: &mut CoreWindowState) -> Vec<(u32, CoreWindowVnodeCheckpoint)> {
    let vnode_count = u32::from(state.key_group_count());
    let vnodes = (0..vnode_count).collect::<Vec<_>>();
    state.checkpoint_vnodes(&vnodes, vnode_count).unwrap()
}

fn restore_all(
    state: &mut CoreWindowState,
    checkpoints: Vec<(u32, CoreWindowVnodeCheckpoint)>,
) -> Result<(), DbError> {
    let frontier = checkpoints
        .first()
        .map_or(i64::MIN, |(_, checkpoint)| checkpoint.frontier_floor_ms);
    let vnode_count = u32::from(state.key_group_count());
    state.restore_high_watermark_ms(frontier)?;
    for (vnode, checkpoint) in checkpoints {
        state.restore_vnode(vnode, vnode_count, checkpoint)?;
    }
    Ok(())
}

fn assert_session_deadline_index(state: &CoreWindowState) {
    let session_window = matches!(state.assigner, CoreWindowAssigner::Session { .. });
    for vnode_state in state.vnode_states.iter().filter_map(Option::as_deref) {
        if !session_window {
            assert!(vnode_state.session_deadlines.is_empty());
            continue;
        }

        let session_count = vnode_state
            .session_groups
            .values()
            .map(|group| group.sessions.len())
            .sum::<usize>();
        assert_eq!(vnode_state.session_deadlines.len(), session_count);
        assert_eq!(
            vnode_state.accounted_state_bytes,
            CoreWindowState::session_groups_bytes(
                &vnode_state.session_groups,
                &vnode_state.session_deadlines,
            )
        );
        for (key, group) in &vnode_state.session_groups {
            for (&start, session) in &group.sessions {
                assert_eq!(start, session.start);
                assert!(vnode_state
                    .session_deadlines
                    .contains(&SessionDeadline::new(
                        Arc::clone(key),
                        start,
                        session.end,
                        state.allowed_lateness_ms,
                    )));
            }
        }
        for deadline in &vnode_state.session_deadlines {
            let (key, group) = vnode_state
                .session_groups
                .get_key_value(deadline.key.as_ref())
                .expect("deadline group must exist");
            assert!(Arc::ptr_eq(key, &deadline.key));
            let session = group
                .sessions
                .get(&deadline.session_start)
                .expect("deadline interval must exist");
            assert_eq!(
                deadline.deadline_ms,
                session.end.saturating_add(state.allowed_lateness_ms)
            );
        }
    }
}

fn session_group_count(state: &CoreWindowState) -> usize {
    assert_session_deadline_index(state);
    let count = state
        .vnode_states
        .iter()
        .filter_map(Option::as_deref)
        .map(|state| state.session_groups.len())
        .sum();
    assert_eq!(count, state.session_group_count);
    count
}

/// Build a `CoreWindowState` for SUM(Int64) with 1-second tumbling
/// windows and a single Utf8 group-by column.
fn make_core_window_state(size_ms: i64) -> CoreWindowState {
    let ctx = SessionContext::new();
    let udf = ctx.udaf("sum").expect("SUM should be registered");

    let agg_specs = vec![AggFuncSpec {
        udf,
        input_types: vec![DataType::Int64],
        input_col_indices: vec![1],
        output_name: "total".to_string(),
        return_type: DataType::Int64,
        is_count_star: false,
        filter_col_index: None,
    }];

    let output_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("total", DataType::Int64, true),
    ]));

    CoreWindowState {
        assigner: CoreWindowAssigner::Tumbling(TumblingWindowAssigner::from_millis(size_ms)),
        key_group_count: KeyGroupCount::try_from(1_u16).unwrap(),
        vnode_states: std::iter::repeat_with(|| None)
            .take(1)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        active_vnodes: Vec::with_capacity(1),
        active_vnode_positions: vec![usize::MAX; 1].into_boxed_slice(),
        window_group_counts: FxHashMap::default(),
        session_group_count: 0,
        agg_specs,
        num_group_cols: 1,
        group_types: Arc::from(vec![DataType::Utf8]),
        row_converter: Arc::new(
            arrow::row::RowConverter::new(vec![arrow::row::SortField::new(DataType::Utf8)])
                .unwrap(),
        ),
        query_sql: String::new(),
        #[cfg(test)]
        pre_agg_sql: String::new(),
        state_output_schema: Arc::clone(&output_schema),
        group_output_sources: vec![GroupOutputSource::Key(0)],
        output_schema,
        time_col_index: 2,
        compiled_projection: None,
        planned_functions_immutable: true,
        cached_pre_agg_physical: None,
        now_where: None,
        now_filter_cache: None,
        having_filter: None,
        max_groups_per_window: 1_000_000,
        allowed_lateness_ms: 0,
        high_watermark_ms: i64::MIN,
        post_projection: None,
        prom: None,
        scratch_nogroup: FxHashMap::default(),
        scratch_grouped: FxHashMap::default(),
        scratch_group_keys: indexmap::IndexSet::default(),
        checkpoint_dirty_vnodes: vec![false; 1].into_boxed_slice(),
        checkpoint_dirty_vnode_roster: Vec::with_capacity(1),
        full_vnode_capture_required: true,
        required_frontier_floor_ms: i64::MIN,
    }
}

/// Build a multi-aggregate core window state: SUM + COUNT.
fn make_core_window_state_multi_agg(size_ms: i64) -> CoreWindowState {
    let ctx = SessionContext::new();
    let sum_udf = ctx.udaf("sum").expect("SUM");
    let count_udf = ctx.udaf("count").expect("COUNT");

    let agg_specs = vec![
        AggFuncSpec {
            udf: sum_udf,
            input_types: vec![DataType::Int64],
            input_col_indices: vec![1],
            output_name: "total".to_string(),
            return_type: DataType::Int64,
            is_count_star: false,
            filter_col_index: None,
        },
        AggFuncSpec {
            udf: count_udf,
            input_types: vec![DataType::Int64],
            input_col_indices: vec![1],
            output_name: "cnt".to_string(),
            return_type: DataType::Int64,
            is_count_star: false,
            filter_col_index: None,
        },
    ];

    let output_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("total", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
    ]));

    CoreWindowState {
        assigner: CoreWindowAssigner::Tumbling(TumblingWindowAssigner::from_millis(size_ms)),
        key_group_count: KeyGroupCount::try_from(1_u16).unwrap(),
        vnode_states: std::iter::repeat_with(|| None)
            .take(1)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        active_vnodes: Vec::with_capacity(1),
        active_vnode_positions: vec![usize::MAX; 1].into_boxed_slice(),
        window_group_counts: FxHashMap::default(),
        session_group_count: 0,
        agg_specs,
        num_group_cols: 1,
        group_types: Arc::from(vec![DataType::Utf8]),
        row_converter: Arc::new(
            arrow::row::RowConverter::new(vec![arrow::row::SortField::new(DataType::Utf8)])
                .unwrap(),
        ),
        query_sql: String::new(),
        #[cfg(test)]
        pre_agg_sql: String::new(),
        state_output_schema: Arc::clone(&output_schema),
        group_output_sources: vec![GroupOutputSource::Key(0)],
        output_schema,
        time_col_index: 2,
        compiled_projection: None,
        planned_functions_immutable: true,
        cached_pre_agg_physical: None,
        now_where: None,
        now_filter_cache: None,
        having_filter: None,
        max_groups_per_window: 1_000_000,
        allowed_lateness_ms: 0,
        high_watermark_ms: i64::MIN,
        post_projection: None,
        prom: None,
        scratch_nogroup: FxHashMap::default(),
        scratch_grouped: FxHashMap::default(),
        scratch_group_keys: indexmap::IndexSet::default(),
        checkpoint_dirty_vnodes: vec![false; 1].into_boxed_slice(),
        checkpoint_dirty_vnode_roster: Vec::with_capacity(1),
        full_vnode_capture_required: true,
        required_frontier_floor_ms: i64::MIN,
    }
}

/// Build a hopping (sliding) `CoreWindowState` for SUM(Int64).
fn make_hopping_core_window_state(size_ms: i64, slide_ms: i64) -> CoreWindowState {
    let ctx = SessionContext::new();
    let udf = ctx.udaf("sum").expect("SUM should be registered");

    let agg_specs = vec![AggFuncSpec {
        udf,
        input_types: vec![DataType::Int64],
        input_col_indices: vec![1],
        output_name: "total".to_string(),
        return_type: DataType::Int64,
        is_count_star: false,
        filter_col_index: None,
    }];

    let output_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("total", DataType::Int64, true),
    ]));

    CoreWindowState {
        assigner: CoreWindowAssigner::Hopping(SlidingWindowAssigner::from_millis(
            size_ms, slide_ms,
        )),
        key_group_count: KeyGroupCount::try_from(1_u16).unwrap(),
        vnode_states: std::iter::repeat_with(|| None)
            .take(1)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        active_vnodes: Vec::with_capacity(1),
        active_vnode_positions: vec![usize::MAX; 1].into_boxed_slice(),
        window_group_counts: FxHashMap::default(),
        session_group_count: 0,
        agg_specs,
        num_group_cols: 1,
        group_types: Arc::from(vec![DataType::Utf8]),
        row_converter: Arc::new(
            arrow::row::RowConverter::new(vec![arrow::row::SortField::new(DataType::Utf8)])
                .unwrap(),
        ),
        query_sql: String::new(),
        #[cfg(test)]
        pre_agg_sql: String::new(),
        state_output_schema: Arc::clone(&output_schema),
        group_output_sources: vec![GroupOutputSource::Key(0)],
        output_schema,
        time_col_index: 2,
        compiled_projection: None,
        planned_functions_immutable: true,
        cached_pre_agg_physical: None,
        now_where: None,
        now_filter_cache: None,
        having_filter: None,
        max_groups_per_window: 1_000_000,
        allowed_lateness_ms: 0,
        high_watermark_ms: i64::MIN,
        post_projection: None,
        prom: None,
        scratch_nogroup: FxHashMap::default(),
        scratch_grouped: FxHashMap::default(),
        scratch_group_keys: indexmap::IndexSet::default(),
        checkpoint_dirty_vnodes: vec![false; 1].into_boxed_slice(),
        checkpoint_dirty_vnode_roster: Vec::with_capacity(1),
        full_vnode_capture_required: true,
        required_frontier_floor_ms: i64::MIN,
    }
}

/// Build a session `CoreWindowState` for SUM(Int64).
fn make_session_core_window_state(gap_ms: i64) -> CoreWindowState {
    let ctx = SessionContext::new();
    let udf = ctx.udaf("sum").expect("SUM should be registered");

    let agg_specs = vec![AggFuncSpec {
        udf,
        input_types: vec![DataType::Int64],
        input_col_indices: vec![1],
        output_name: "total".to_string(),
        return_type: DataType::Int64,
        is_count_star: false,
        filter_col_index: None,
    }];

    let output_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("total", DataType::Int64, true),
    ]));

    CoreWindowState {
        assigner: CoreWindowAssigner::Session { gap_ms },
        key_group_count: KeyGroupCount::try_from(1_u16).unwrap(),
        vnode_states: std::iter::repeat_with(|| None)
            .take(1)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        active_vnodes: Vec::with_capacity(1),
        active_vnode_positions: vec![usize::MAX; 1].into_boxed_slice(),
        window_group_counts: FxHashMap::default(),
        session_group_count: 0,
        agg_specs,
        num_group_cols: 1,
        group_types: Arc::from(vec![DataType::Utf8]),
        row_converter: Arc::new(
            arrow::row::RowConverter::new(vec![arrow::row::SortField::new(DataType::Utf8)])
                .unwrap(),
        ),
        query_sql: String::new(),
        #[cfg(test)]
        pre_agg_sql: String::new(),
        state_output_schema: Arc::clone(&output_schema),
        group_output_sources: vec![GroupOutputSource::Key(0)],
        output_schema,
        time_col_index: 2,
        compiled_projection: None,
        planned_functions_immutable: true,
        cached_pre_agg_physical: None,
        now_where: None,
        now_filter_cache: None,
        having_filter: None,
        max_groups_per_window: 1_000_000,
        allowed_lateness_ms: 0,
        high_watermark_ms: i64::MIN,
        post_projection: None,
        prom: None,
        scratch_nogroup: FxHashMap::default(),
        scratch_grouped: FxHashMap::default(),
        scratch_group_keys: indexmap::IndexSet::default(),
        checkpoint_dirty_vnodes: vec![false; 1].into_boxed_slice(),
        checkpoint_dirty_vnode_roster: Vec::with_capacity(1),
        full_vnode_capture_required: true,
        required_frontier_floor_ms: i64::MIN,
    }
}

#[tokio::test]
async fn test_detect_tumbling_aggregate_returns_core_window() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);

    // Register a source table
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
    ctx.register_table("trades", Arc::new(mem)).unwrap();

    let window_config = WindowOperatorConfig {
        window_type: WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: Duration::from_secs(60),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    let sql = "SELECT symbol, SUM(price) AS total FROM trades \
                   GROUP BY symbol HAVING SUM(price) > 100";
    let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None, key_groups())
        .await
        .unwrap();
    assert!(result.is_some(), "Tumbling aggregate should return Some");
    let state = result.unwrap();
    assert!(state.having_filter.is_some());
}

#[tokio::test]
async fn window_output_preserves_logical_abi_and_applies_same_arity_projection() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![
            Arc::new(StringArray::from(vec!["west"])),
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![1_000])),
        ],
    )
    .unwrap();
    let table = datafusion::datasource::MemTable::try_new(input_schema, vec![vec![input]]).unwrap();
    ctx.register_table("events", Arc::new(table)).unwrap();

    let config = WindowOperatorConfig {
        window_type: WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: Duration::from_secs(10),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };
    let sql = "SELECT region, TUMBLE(ts, INTERVAL '10' SECOND) AS window_start, \
                   COUNT(*) AS row_count FROM events \
                   GROUP BY region, TUMBLE(ts, INTERVAL '10' SECOND)";
    let expected = ctx
        .sql(sql)
        .await
        .unwrap()
        .logical_plan()
        .schema()
        .as_arrow()
        .clone();
    let mut state = CoreWindowState::try_from_sql(
        &ctx,
        sql,
        &config,
        Some(&laminar_sql::parser::EmitClause::OnWindowClose),
        key_groups(),
    )
    .await
    .unwrap()
    .unwrap();

    assert!(state.post_projection.is_some());

    let pre_aggregate = ctx
        .sql(state.pre_agg_sql())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for batch in &pre_aggregate {
        state.update_batch(batch).unwrap();
    }
    let emitted = state.close_windows(11_000).unwrap();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].schema().as_ref(), &expected);
    let counts = emitted[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 1);

    let derived_sql = "SELECT region, TUMBLE(ts, INTERVAL '10' SECOND) AS window_start, \
                           COUNT(*) * 2 AS doubled FROM events \
                           GROUP BY region, TUMBLE(ts, INTERVAL '10' SECOND)";
    let derived_expected = ctx
        .sql(derived_sql)
        .await
        .unwrap()
        .logical_plan()
        .schema()
        .as_arrow()
        .clone();
    let mut derived = CoreWindowState::try_from_sql(
        &ctx,
        derived_sql,
        &config,
        Some(&laminar_sql::parser::EmitClause::OnWindowClose),
        key_groups(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(derived.post_projection.is_some());

    let pre_aggregate = ctx
        .sql(derived.pre_agg_sql())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for batch in &pre_aggregate {
        derived.update_batch(batch).unwrap();
    }
    let emitted = derived.close_windows(11_000).unwrap();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].schema().as_ref(), &derived_expected);
    let doubled = emitted[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(doubled.value(0), 2);
}

#[tokio::test]
async fn test_detect_sliding_invalid_params_returns_none() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
    ctx.register_table("events", Arc::new(mem)).unwrap();

    // Sliding with slide > size should return None
    let window_config = WindowOperatorConfig {
        window_type: WindowType::Sliding,
        time_column: "ts".to_string(),
        size: Duration::from_secs(10),
        slide: Some(Duration::from_secs(60)),
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    let sql = "SELECT id, SUM(val) AS total FROM events GROUP BY id";
    let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None, key_groups())
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "Sliding with slide > size should return None"
    );

    let maximum_fanout = WindowOperatorConfig {
        size: Duration::from_secs(128),
        slide: Some(Duration::from_secs(1)),
        ..window_config.clone()
    };
    assert!(
        CoreWindowState::try_from_sql(&ctx, sql, &maximum_fanout, None, key_groups())
            .await
            .unwrap()
            .is_some()
    );

    let excessive_fanout = WindowOperatorConfig {
        size: Duration::from_secs(129),
        ..maximum_fanout
    };
    let error = CoreWindowState::try_from_sql(&ctx, sql, &excessive_fanout, None, key_groups())
        .await
        .err()
        .expect("excessive hopping fan-out must be rejected");
    assert!(error.to_string().contains("Cap is 128"));

    let excessive_lateness = WindowOperatorConfig {
        window_type: WindowType::Tumbling,
        size: Duration::from_secs(1),
        slide: None,
        allowed_lateness: Duration::from_secs(u64::try_from(i64::MAX).unwrap() / 1000 + 1),
        ..window_config
    };
    let error = CoreWindowState::try_from_sql(&ctx, sql, &excessive_lateness, None, key_groups())
        .await
        .err()
        .expect("excessive allowed lateness must be rejected");
    assert!(error.to_string().contains("allowed lateness exceeds"));
}

#[tokio::test]
async fn test_detect_projection_only_returns_none() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
    ctx.register_table("events", Arc::new(mem)).unwrap();

    let window_config = WindowOperatorConfig {
        window_type: WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: Duration::from_secs(60),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    // No aggregate → should return None
    let sql = "SELECT id, val FROM events";
    let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None, key_groups())
        .await
        .unwrap();
    assert!(result.is_none(), "Projection-only should return None");
}

#[test]
fn test_core_window_tumbling_sum() {
    let mut state = make_core_window_state(1000);

    // Two events in window [0, 1000)
    let batch1 = make_pre_agg_batch(vec!["AAPL", "AAPL"], vec![10, 20], vec![100, 500]);
    state.update_batch(&batch1).unwrap();

    // One more event in same window
    let batch2 = make_pre_agg_batch(vec!["AAPL"], vec![30], vec![800]);
    state.update_batch(&batch2).unwrap();

    // Close window at watermark 1000.
    let batches = state.close_windows(1000).unwrap();
    assert_eq!(batches.len(), 1);

    let result = &batches[0];
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.schema().fields().len(), 2);

    // SUM = 10 + 20 + 30 = 60.
    let total = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 60);
}

#[test]
fn test_core_window_tumbling_multi_aggregate_multi_group() {
    let mut state = make_core_window_state_multi_agg(1000);

    // Window [0, 1000): AAPL=10,20  GOOG=100  MSFT=50
    let batch1 = make_pre_agg_batch(
        vec!["AAPL", "GOOG", "AAPL", "MSFT"],
        vec![10, 100, 20, 50],
        vec![100, 200, 300, 400],
    );
    state.update_batch(&batch1).unwrap();

    // Window [1000, 2000): AAPL=5  GOOG=200,300
    let batch2 = make_pre_agg_batch(
        vec!["AAPL", "GOOG", "GOOG"],
        vec![5, 200, 300],
        vec![1100, 1200, 1500],
    );
    state.update_batch(&batch2).unwrap();

    // Close first window
    let batches = state.close_windows(1000).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3); // AAPL, GOOG, MSFT

    // Close second window
    let batches = state.close_windows(2000).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2); // AAPL, GOOG
}

#[test]
fn test_core_window_close_windows_watermark() {
    let mut state = make_core_window_state(1000);

    // Events in three windows: SUMs by window are 1, 2, 3.
    let batch = make_pre_agg_batch(vec!["A", "A", "A"], vec![1, 2, 3], vec![100, 1100, 2100]);
    state.update_batch(&batch).unwrap();

    // Watermark at 1500 → only window [0, 1000) closes; SUM=1.
    let batches = state.close_windows(1500).unwrap();
    assert_eq!(batches.len(), 1);
    let total = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 1);

    // Watermark at 2000 → window [1000, 2000) closes; SUM=2.
    let batches = state.close_windows(2000).unwrap();
    assert_eq!(batches.len(), 1);
    let total = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 2);

    // Watermark at 2500 → nothing to close (window [2000,3000) still open).
    let batches = state.close_windows(2500).unwrap();
    assert!(batches.is_empty());
}

/// Late-event check must use the assigner's window-start, not naive
/// `ts.rem_euclid(size)`, when an offset is configured.
#[test]
fn test_late_events_with_window_offset_are_dropped() {
    let mut state = make_core_window_state(1000);
    let unshifted_fingerprint = state.query_fingerprint();
    state.assigner =
        CoreWindowAssigner::Tumbling(TumblingWindowAssigner::from_millis(1000).with_offset_ms(200));
    assert_ne!(state.query_fingerprint(), unshifted_fingerprint);

    let batch = make_pre_agg_batch(vec!["A"], vec![10], vec![500]);
    state.update_batch(&batch).unwrap();

    let first = state.close_windows(1200).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(sum_total(&first[0]), 10);

    // ts=1100 maps to [200, 1200), already closed at watermark 1200.
    let late = make_pre_agg_batch(vec!["A"], vec![1100], vec![999]);
    state.update_batch(&late).unwrap();

    let second = state.close_windows(2200).unwrap();
    assert!(second.is_empty());
}

/// Late events for an already-closed window must not re-create the
/// bucket — otherwise the next close re-emits the same row.
#[test]
fn test_late_events_for_closed_windows_are_dropped() {
    let mut state = make_core_window_state(1000);

    let batch1 = make_pre_agg_batch(vec!["A", "A", "A"], vec![10, 20, 30], vec![100, 200, 500]);
    state.update_batch(&batch1).unwrap();

    let first = state.close_windows(1000).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(sum_total(&first[0]), 60);

    let late = make_pre_agg_batch(vec!["A"], vec![999], vec![300]);
    state.update_batch(&late).unwrap();

    let second = state.close_windows(2000).unwrap();
    assert!(second.is_empty());
}

#[test]
fn test_emit_clause_to_core_all_variants() {
    use crate::sql_analysis::{emit_clause_to_core, sql_emit_to_core};
    use laminar_sql::parser::EmitStrategy as SqlEmit;

    assert_eq!(
        sql_emit_to_core(&SqlEmit::OnWatermark),
        CoreEmit::OnWatermark
    );
    assert_eq!(
        sql_emit_to_core(&SqlEmit::OnWindowClose),
        CoreEmit::OnWindowClose
    );
    assert_eq!(sql_emit_to_core(&SqlEmit::OnUpdate), CoreEmit::OnUpdate);
    assert_eq!(sql_emit_to_core(&SqlEmit::Changelog), CoreEmit::Changelog);
    assert_eq!(sql_emit_to_core(&SqlEmit::FinalOnly), CoreEmit::Final);

    // EmitClause → Core via bridge
    assert_eq!(
        emit_clause_to_core(&EmitClause::AfterWatermark).unwrap(),
        CoreEmit::OnWatermark
    );
    assert_eq!(
        emit_clause_to_core(&EmitClause::OnWindowClose).unwrap(),
        CoreEmit::OnWindowClose
    );
    assert_eq!(
        emit_clause_to_core(&EmitClause::Final).unwrap(),
        CoreEmit::Final
    );
}

#[test]
fn test_core_window_checkpoint_roundtrip() {
    let mut state = make_core_window_state(1000);

    // Feed data into two windows
    let batch = make_pre_agg_batch(
        vec!["AAPL", "AAPL", "GOOG"],
        vec![10, 20, 100],
        vec![100, 200, 1500],
    );
    state.update_batch(&batch).unwrap();

    // Checkpoint
    let cp = checkpoint_all(&mut state);
    assert_eq!(
        cp.iter()
            .map(|(_, checkpoint)| checkpoint.windows.len())
            .sum::<usize>(),
        2
    );

    // Create fresh state and restore
    let mut state2 = make_core_window_state(1000);
    restore_all(&mut state2, cp).unwrap();

    // Close first window and verify SUM.
    let batches = state2.close_windows(1000).unwrap();
    assert_eq!(batches.len(), 1);
    let result = &batches[0];
    assert_eq!(result.num_rows(), 1); // Only AAPL in window [0,1000)

    let total = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 30, "SUM should be 10+20=30");

    // Close second window
    let batches = state2.close_windows(2000).unwrap();
    assert_eq!(batches.len(), 1);
    let total2 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total2.value(0), 100, "SUM should be 100");
}

#[test]
fn test_core_window_checkpoint_fingerprint_mismatch() {
    let mut state = make_core_window_state(1000);

    let batch = make_pre_agg_batch(vec!["AAPL"], vec![10], vec![100]);
    state.update_batch(&batch).unwrap();

    let mut cp = checkpoint_all(&mut state);
    cp[0].1.fingerprint = 12345;

    let mut state2 = make_core_window_state(1000);
    let result = restore_all(&mut state2, cp);
    assert!(result.is_err(), "Should fail on fingerprint mismatch");
}

#[test]
fn core_window_checkpoint_rejects_a_different_state_query() {
    let mut state = make_core_window_state(1000);
    state.query_sql = "SELECT symbol, SUM(value) FROM trades GROUP BY symbol".into();
    let checkpoint = checkpoint_all(&mut state);

    let mut restored = make_core_window_state(1000);
    restored.query_sql = "SELECT symbol, MAX(value) FROM trades GROUP BY symbol".into();

    assert!(restore_all(&mut restored, checkpoint)
        .unwrap_err()
        .to_string()
        .contains("fingerprint mismatch"));
}

#[tokio::test]
async fn test_detect_sliding_aggregate_returns_core_window() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);

    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
    ctx.register_table("trades", Arc::new(mem)).unwrap();

    let window_config = WindowOperatorConfig {
        window_type: WindowType::Sliding,
        time_column: "ts".to_string(),
        size: Duration::from_secs(60),
        slide: Some(Duration::from_secs(10)),
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    let sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";
    let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None, key_groups())
        .await
        .unwrap();
    assert!(result.is_some(), "Sliding aggregate should return Some");
}

#[tokio::test]
async fn test_detect_session_aggregate_returns_core_window() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);

    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
    ctx.register_table("trades", Arc::new(mem)).unwrap();

    let window_config = WindowOperatorConfig {
        window_type: WindowType::Session,
        time_column: "ts".to_string(),
        size: Duration::ZERO,
        slide: None,
        gap: Some(Duration::from_secs(30)),
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    let sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";
    let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None, key_groups())
        .await
        .unwrap();
    assert!(result.is_some(), "Session aggregate should return Some");
}

#[tokio::test]
async fn test_detect_session_zero_gap_returns_none() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);

    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
    ctx.register_table("trades", Arc::new(mem)).unwrap();

    let window_config = WindowOperatorConfig {
        window_type: WindowType::Session,
        time_column: "ts".to_string(),
        size: Duration::ZERO,
        slide: None,
        gap: Some(Duration::ZERO),
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    let sql = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";
    let result = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None, key_groups())
        .await
        .unwrap();
    assert!(result.is_none(), "Session with gap=0 should return None");
}

#[tokio::test]
async fn sql_hop_emits_each_assigned_boundary() {
    use std::time::Duration;

    let (ctx, schema) = sql_window_context();
    let config = WindowOperatorConfig {
        window_type: WindowType::Sliding,
        time_column: "ts".to_string(),
        size: Duration::from_secs(4),
        slide: Some(Duration::from_secs(2)),
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };
    let sql = "SELECT HOP(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND) AS window_start, \
                          HOP_END(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND) AS window_end, \
                          SUM(amount) AS total \
                   FROM events \
                   GROUP BY HOP(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND), \
                            HOP_END(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND)";
    let mut state = CoreWindowState::try_from_sql(&ctx, sql, &config, None, key_groups())
        .await
        .unwrap()
        .unwrap();
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![3000])),
        ],
    )
    .unwrap();
    let projected = state
        .compiled_projection()
        .unwrap()
        .evaluate(&input)
        .unwrap();
    state.update_batch(&projected).unwrap();

    let mut actual = state
        .close_windows(6000)
        .unwrap()
        .into_iter()
        .map(|batch| {
            let starts = batch
                .column_by_name("window_start")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .unwrap();
            let ends = batch
                .column_by_name("window_end")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .unwrap();
            let totals = batch
                .column_by_name("total")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (starts.value(0), ends.value(0), totals.value(0))
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, vec![(0, 4_000_000, 7), (2_000_000, 6_000_000, 7)]);
}

#[tokio::test]
async fn sql_session_marker_does_not_split_a_session_key() {
    use std::time::Duration;

    let (ctx, schema) = sql_window_context();
    let config = WindowOperatorConfig {
        window_type: WindowType::Session,
        time_column: "ts".to_string(),
        size: Duration::ZERO,
        slide: None,
        gap: Some(Duration::from_secs(3)),
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };
    let sql = "SELECT user_id, SESSION(ts, INTERVAL '3' SECOND) AS window_start, \
                          SUM(amount) AS total \
                   FROM events \
                   GROUP BY user_id, SESSION(ts, INTERVAL '3' SECOND)";
    let mut state = CoreWindowState::try_from_sql(&ctx, sql, &config, None, key_groups())
        .await
        .unwrap()
        .unwrap();
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A"])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![1000, 5000, 3500])),
        ],
    )
    .unwrap();
    let projected = state
        .compiled_projection()
        .unwrap()
        .evaluate(&input)
        .unwrap();
    state.update_batch(&projected).unwrap();

    let batches = state.close_windows(8000).unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("window_start")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1_000_000
    );
    assert_eq!(
        batch
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        60
    );
}

#[test]
fn test_hopping_basic_sum() {
    // 4s window, 2s slide → each event in 2 windows
    let mut state = make_hopping_core_window_state(4000, 2000);

    // ts=1000 → windows [-2000,2000) and [0,4000)
    // ts=3000 → windows [0,4000) and [2000,6000)
    let batch = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 3000]);
    state.update_batch(&batch).unwrap();

    //   watermark=2000 → window [-2000, 2000): only ts=1000 → SUM=10
    //   watermark=4000 → window [0, 4000):     ts=1000+3000 → SUM=30
    //   watermark=6000 → window [2000, 6000):  only ts=3000 → SUM=20
    let read_total = |b: &arrow_array::RecordBatch| -> i64 {
        b.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };

    let b = state.close_windows(2000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(read_total(&b[0]), 10);

    let b = state.close_windows(4000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(read_total(&b[0]), 30);

    let b = state.close_windows(6000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(read_total(&b[0]), 20);
}

#[test]
fn test_hopping_multi_group() {
    let mut state = make_hopping_core_window_state(4000, 2000);

    let batch = make_pre_agg_batch(
        vec!["A", "B", "A"],
        vec![10, 100, 20],
        vec![1000, 1000, 3000],
    );
    state.update_batch(&batch).unwrap();

    let collect_sym_total = |b: &arrow_array::RecordBatch| -> Vec<(String, i64)> {
        let syms = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let totals = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        (0..b.num_rows())
            .map(|i| (syms.value(i).to_string(), totals.value(i)))
            .collect()
    };
    let sorted = |b: &arrow_array::RecordBatch| -> Vec<(String, i64)> {
        let mut v = collect_sym_total(b);
        v.sort();
        v
    };

    // window [-2000, 2000): A=10, B=100
    let b = state.close_windows(2000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(
        sorted(&b[0]),
        vec![("A".to_string(), 10), ("B".to_string(), 100)]
    );

    // window [0, 4000): A=10+20=30, B=100
    let b = state.close_windows(4000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(
        sorted(&b[0]),
        vec![("A".to_string(), 30), ("B".to_string(), 100)]
    );

    // window [2000, 6000): A=20 (B has no event in [2000,6000))
    let b = state.close_windows(6000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(sorted(&b[0]), vec![("A".to_string(), 20)]);
}

#[test]
fn test_hopping_watermark_ordering() {
    let mut state = make_hopping_core_window_state(4000, 2000);

    let batch = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 3000]);
    state.update_batch(&batch).unwrap();

    let total = |b: &arrow_array::RecordBatch| -> i64 {
        b.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };

    // Watermark at 2000 → window [-2000, 2000): only ts=1000, SUM=10.
    let batches = state.close_windows(2000).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(total(&batches[0]), 10);

    // Watermark at 4000 → window [0, 4000): ts=1000+3000, SUM=30.
    let batches = state.close_windows(4000).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(total(&batches[0]), 30);

    // Watermark at 5000 → nothing (window [2000,6000) still open)
    let batches = state.close_windows(5000).unwrap();
    assert!(batches.is_empty());
}

#[test]
fn test_session_basic_sum() {
    // gap=5000ms — events within 5s merge into one session
    let mut state = make_session_core_window_state(5000);

    // Three events within gap for group A
    let batch = make_pre_agg_batch(
        vec!["A", "A", "A"],
        vec![10, 20, 30],
        vec![1000, 3000, 4000],
    );
    state.update_batch(&batch).unwrap();
    assert_session_deadline_index(&state);
    assert!(state.close_windows(6000).unwrap().is_empty());
    assert_session_deadline_index(&state);

    // Session: start=1000, end=4000+5000=9000.
    // Watermark at 9000 → session closes.
    let batches = state.close_windows(9000).unwrap();
    assert_eq!(batches.len(), 1);
    let result = &batches[0];
    assert_eq!(result.num_rows(), 1);

    let total = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 60);
}

#[test]
fn session_window_rejects_unrepresentable_end_before_mutation() {
    let mut state = make_session_core_window_state(1);
    let baseline = state.accounted_state_bytes();
    let batch = make_pre_agg_batch(vec!["AAPL"], vec![10], vec![i64::MAX]);

    let error = state.update_batch(&batch).unwrap_err();

    assert!(matches!(&error, DbError::PipelineTerminal(_)));
    assert!(error.requires_pipeline_halt());
    assert_eq!(session_group_count(&state), 0);
    assert_eq!(state.accounted_state_bytes(), baseline);
}

#[test]
fn test_session_two_sessions() {
    // gap=2000ms
    let mut state = make_session_core_window_state(2000);

    // Two clusters: [1000] and [5000, 6000]
    let batch = make_pre_agg_batch(
        vec!["A", "A", "A"],
        vec![10, 20, 30],
        vec![1000, 5000, 6000],
    );
    state.update_batch(&batch).unwrap();

    // Session 1: [1000, 3000) → SUM=10
    // Session 2: [5000, 8000) → SUM=20+30=50
    let batches = state.close_windows(8000).unwrap();
    assert_eq!(batches.len(), 1);
    let result = &batches[0];
    assert_eq!(result.num_rows(), 2);

    let totals = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut sums: Vec<i64> = (0..result.num_rows()).map(|i| totals.value(i)).collect();
    sums.sort_unstable();
    assert_eq!(sums, vec![10, 50]);
}

#[test]
fn test_session_merge() {
    // gap=3000ms
    let mut state = make_session_core_window_state(3000);

    // First two events create separate sessions
    let batch1 = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 5000]);
    state.update_batch(&batch1).unwrap();
    // Session 1: [1000, 4000), Session 2: [5000, 8000)

    // Late event at ts=3500 bridges the two sessions
    let batch2 = make_pre_agg_batch(vec!["A"], vec![30], vec![3500]);
    state.update_batch(&batch2).unwrap();
    assert_session_deadline_index(&state);
    // Merged: [1000, 8000) with SUM = 10+20+30 = 60.

    let batches = state.close_windows(8000).unwrap();
    assert_eq!(batches.len(), 1);
    let result = &batches[0];
    assert_eq!(result.num_rows(), 1);

    let total = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 60);
}

#[test]
fn restored_watermark_prevents_closed_session_recreation() {
    let mut state = make_session_core_window_state(3000);
    state
        .update_batch(&make_pre_agg_batch(vec!["A"], vec![20], vec![5000]))
        .unwrap();
    assert!(state.close_windows(5000).unwrap().is_empty());

    state
        .update_batch(&make_pre_agg_batch(vec!["A"], vec![10], vec![2000]))
        .unwrap();
    let emitted = state.close_windows(8000).unwrap();
    assert_eq!(emitted.len(), 1);
    assert_eq!(sum_total(&emitted[0]), 30);

    let checkpoint = checkpoint_all(&mut state);
    let mut restored = make_session_core_window_state(3000);
    restore_all(&mut restored, checkpoint).unwrap();
    restored
        .update_batch(&make_pre_agg_batch(vec!["A"], vec![10], vec![2000]))
        .unwrap();
    assert_eq!(session_group_count(&restored), 0);
    assert!(restored.close_windows(10_000).unwrap().is_empty());
}

#[test]
fn test_session_multi_group_independent() {
    let mut state = make_session_core_window_state(3000);

    let batch = make_pre_agg_batch(
        vec!["A", "B", "A", "B"],
        vec![10, 100, 20, 200],
        vec![1000, 2000, 2000, 3000],
    );
    state.update_batch(&batch).unwrap();

    // A: session [1000, 5000) SUM=30
    // B: session [2000, 6000) SUM=300
    let batches = state.close_windows(6000).unwrap();
    assert_eq!(batches.len(), 1);
    let result = &batches[0];
    assert_eq!(result.num_rows(), 2);

    let syms = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let mut results: Vec<(String, i64)> = (0..result.num_rows())
        .map(|i| (syms.value(i).to_string(), totals.value(i)))
        .collect();
    results.sort();

    assert_eq!(results[0], ("A".to_string(), 30));
    assert_eq!(results[1], ("B".to_string(), 300));
}

#[test]
fn test_session_group_cardinality_cap_fails_closed() {
    let mut state = make_session_core_window_state(3000);
    state.max_groups_per_window = 2;

    let batch1 = make_pre_agg_batch(vec!["A", "B"], vec![10, 100], vec![1000, 1000]);
    state.update_batch(&batch1).unwrap();
    assert_eq!(session_group_count(&state), 2);

    let batch2 = make_pre_agg_batch(vec!["C", "A"], vec![999, 20], vec![1500, 1500]);
    let error = state.update_batch(&batch2).unwrap_err();
    assert!(error.to_string().contains("cardinality limit"));
    assert_eq!(session_group_count(&state), 2);

    let batches = state.close_windows(10_000).unwrap();
    assert_eq!(batches.len(), 1);
    let result = &batches[0];
    let syms = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut out: Vec<(String, i64)> = (0..result.num_rows())
        .map(|i| (syms.value(i).to_string(), totals.value(i)))
        .collect();
    out.sort();
    assert_eq!(out, vec![("A".to_string(), 10), ("B".to_string(), 100)]);
}

#[test]
fn test_hopping_checkpoint_roundtrip() {
    let mut state = make_hopping_core_window_state(4000, 2000);

    let batch = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 3000]);
    state.update_batch(&batch).unwrap();

    let cp = checkpoint_all(&mut state);
    assert!(cp.iter().all(|(_, checkpoint)| checkpoint.window_type == 2));
    assert!(cp
        .iter()
        .any(|(_, checkpoint)| !checkpoint.windows.is_empty()));

    let mut state2 = make_hopping_core_window_state(4000, 2000);
    restore_all(&mut state2, cp).unwrap();

    let total = |b: &arrow_array::RecordBatch| -> i64 {
        b.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    let b = state2.close_windows(2000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(total(&b[0]), 10);
    let b = state2.close_windows(4000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(total(&b[0]), 30);
    let b = state2.close_windows(6000).unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(total(&b[0]), 20);
}

#[test]
fn test_session_checkpoint_roundtrip() {
    let mut state = make_session_core_window_state(3000);

    // Create two sessions then merge them
    let batch1 = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 5000]);
    state.update_batch(&batch1).unwrap();

    let batch2 = make_pre_agg_batch(vec!["A"], vec![30], vec![3500]);
    state.update_batch(&batch2).unwrap();

    let cp = checkpoint_all(&mut state);
    assert!(cp.iter().all(|(_, checkpoint)| checkpoint.window_type == 3));
    assert!(cp
        .iter()
        .any(|(_, checkpoint)| !checkpoint.session_state.is_empty()));

    let mut state2 = make_session_core_window_state(3000);
    restore_all(&mut state2, cp).unwrap();
    assert_session_deadline_index(&state2);

    let batches = state2.close_windows(8000).unwrap();
    assert_eq!(batches.len(), 1);
    let result = &batches[0];
    assert_eq!(result.num_rows(), 1);

    let total = result
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        total.value(0),
        60,
        "SUM should be 10+20+30=60 after restore"
    );
}

#[tokio::test]
async fn test_post_aggregate_projection_detection() {
    use arrow::datatypes::Field;

    let ctx = laminar_sql::create_streaming_context_with_validator(
        laminar_sql::StreamingValidatorMode::Off,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("a", DataType::Float64, false),
        Field::new("b", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            Arc::new(arrow::array::Float64Array::from(vec![2.0])),
            Arc::new(Int64Array::from(vec![1000])),
        ],
    )
    .unwrap();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let config = laminar_sql::translator::WindowOperatorConfig {
        window_type: laminar_sql::translator::WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: std::time::Duration::from_secs(10),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: std::time::Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    // SUM(a)/SUM(b) is a post-aggregate projection — should now be accepted.
    let result = CoreWindowState::try_from_sql(
        &ctx,
        "SELECT symbol, SUM(a) / SUM(b) AS ratio \
             FROM events GROUP BY symbol, \
             TUMBLE(ts, INTERVAL '10' SECOND)",
        &config,
        Some(&laminar_sql::parser::EmitClause::OnWindowClose),
        key_groups(),
    )
    .await
    .unwrap();
    assert!(
        result.is_some(),
        "Post-aggregate projection should now be accepted"
    );
    let state = result.unwrap();
    assert!(
        state.post_projection.is_some(),
        "PostProjection should be compiled"
    );
}

#[tokio::test]
async fn test_tumbling_ratio_projection_pipeline() {
    use arrow::datatypes::{Field, TimeUnit};

    let ctx = laminar_sql::create_streaming_context_with_validator(
        laminar_sql::StreamingValidatorMode::Off,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("a", DataType::Float64, false),
        Field::new("b", DataType::Float64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["X"])),
            Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            Arc::new(arrow::array::Float64Array::from(vec![2.0])),
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![1000])),
        ],
    )
    .unwrap();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let config = laminar_sql::translator::WindowOperatorConfig {
        window_type: laminar_sql::translator::WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: std::time::Duration::from_secs(10),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: std::time::Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    let mut state = CoreWindowState::try_from_sql(
        &ctx,
        "SELECT symbol, SUM(a) / SUM(b) AS ratio \
             FROM events GROUP BY symbol, \
             TUMBLE(ts, INTERVAL '10' SECOND) \
             HAVING SUM(a) > 0 AND SUM(b) > 0",
        &config,
        Some(&laminar_sql::parser::EmitClause::OnWindowClose),
        key_groups(),
    )
    .await
    .unwrap()
    .expect("should detect as core window");

    // Execute the pre-agg SQL to get correctly shaped input batches.
    let pre_agg_sql = state.pre_agg_sql().to_string();
    let pre_agg_df = ctx.sql(&pre_agg_sql).await.unwrap();
    let pre_agg_batches = pre_agg_df.collect().await.unwrap();
    for batch in &pre_agg_batches {
        state.update_batch(batch).unwrap();
    }

    // Close the window (watermark past window end = 10_000).
    let batches = state.close_windows(11_000).unwrap();
    assert_eq!(batches.len(), 1, "should emit one batch");
    let out = &batches[0];

    // The post-aggregate projection emits [symbol, ratio].
    assert_eq!(out.num_columns(), 2, "schema: {:?}", out.schema());
    assert_eq!(out.num_rows(), 1);

    // a=1.0, b=2.0 → ratio = SUM(a)/SUM(b) = 0.5.
    let ratio_col = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("ratio should be Float64");
    let ratio = ratio_col.value(0);
    assert!(
        (ratio - 0.5).abs() < 1e-9,
        "expected ratio=0.5, got {ratio}"
    );
}

#[tokio::test]
async fn test_session_with_projection() {
    use arrow::datatypes::Field;

    let ctx = laminar_sql::create_streaming_context_with_validator(
        laminar_sql::StreamingValidatorMode::Off,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["alice"])),
            Arc::new(arrow::array::Float64Array::from(vec![100.0])),
            Arc::new(Int64Array::from(vec![1000])),
        ],
    )
    .unwrap();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("events", Arc::new(mem_table)).unwrap();

    let config = laminar_sql::translator::WindowOperatorConfig {
        window_type: laminar_sql::translator::WindowType::Session,
        time_column: "ts".to_string(),
        size: std::time::Duration::from_secs(5),
        slide: None,
        gap: Some(std::time::Duration::from_secs(5)),
        offset_ms: 0,
        allowed_lateness: std::time::Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };

    // Session window + derived column: SUM(amount) * 2 AS double_total
    let result = CoreWindowState::try_from_sql(
        &ctx,
        "SELECT user_id, SUM(amount) * 2 AS double_total \
             FROM events GROUP BY user_id, \
             SESSION(ts, INTERVAL '5' SECOND)",
        &config,
        Some(&laminar_sql::parser::EmitClause::OnWindowClose),
        key_groups(),
    )
    .await
    .unwrap();
    assert!(result.is_some(), "Session + projection should be accepted");
    let state = result.unwrap();
    assert!(state.post_projection.is_some());
}

#[test]
fn test_apply_post_projection_passthrough() {
    // Without post_projection, batches pass through unchanged.
    let state = make_core_window_state(1000);
    let batch = make_pre_agg_batch(vec!["A"], vec![10], vec![100]);
    let result = state.apply_post_projection(vec![batch.clone()]).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), batch.num_rows());
}

// ── now()/current_timestamp() in streaming predicates ─────

use laminar_core::time::now_unix_millis as now_ms;

#[tokio::test]
async fn now_in_where_builds_and_filters_per_cycle() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("v", DataType::Float64, false),
    ]));
    let mem = datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
    ctx.register_table("evt", Arc::new(mem)).unwrap();

    let window_config = WindowOperatorConfig {
        window_type: WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: Duration::from_secs(60),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::Periodic(Duration::from_secs(5)),
        late_data_side_output: None,
    };
    let sql = "SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS w, COUNT(*) AS c \
                   FROM evt \
                   WHERE ts > now() - INTERVAL '10' MINUTE \
                     AND ts < now() + INTERVAL '2' MINUTE \
                   GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)";
    let mut cw = CoreWindowState::try_from_sql(&ctx, sql, &window_config, None, key_groups())
        .await
        .expect("now() in WHERE must build, not error")
        .expect("tumbling aggregate => Some");
    assert!(cw.now_where.is_some(), "WHERE now() captured dynamically");
    assert!(
        cw.compiled_projection
            .as_ref()
            .is_some_and(|p| p.filter.is_none()),
        "static filter must be empty (now() is applied per cycle)"
    );

    // Three rows: ~now (kept), 1h old (dropped), 1h future (dropped).
    let n = now_ms();
    let ts = arrow::array::TimestampMillisecondArray::from(vec![
        n,
        n - 60 * 60 * 1000,
        n + 60 * 60 * 1000,
    ]);
    let v = arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0]);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(ts) as ArrayRef, Arc::new(v) as ArrayRef],
    )
    .unwrap();

    // now() binds to the watermark; pass the same reference time.
    let filtered = cw
        .apply_dynamic_now_filter(&ctx, std::slice::from_ref(&batch), n)
        .expect("dynamic now() filter compiles + applies")
        .expect("now_where set => Some");
    let kept: usize = filtered.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(kept, 1, "only the ~now row is within now()±bounds");
}

#[tokio::test]
async fn now_outside_where_is_rejected_at_build() {
    use laminar_sql::{create_session_context, register_streaming_functions};
    use std::time::Duration;

    let ctx = create_session_context();
    register_streaming_functions(&ctx);
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("v", DataType::Float64, false),
    ]));
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
    ctx.register_table("evt", Arc::new(mem)).unwrap();

    let window_config = WindowOperatorConfig {
        window_type: WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: Duration::from_secs(60),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };
    // now() in SELECT (not WHERE) must fail loud at CREATE, not freeze.
    let sql = "SELECT TUMBLE(ts, INTERVAL '1' MINUTE) AS w, COUNT(*) AS c, now() AS planned_at \
                   FROM evt GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)";
    let Err(err) =
        CoreWindowState::try_from_sql(&ctx, sql, &window_config, None, key_groups()).await
    else {
        panic!("now() outside WHERE must be rejected at build");
    };
    assert!(
        format!("{err}").contains(laminar_core::error_codes::SQL_UNSUPPORTED),
        "expected {} in error, got: {err}",
        laminar_core::error_codes::SQL_UNSUPPORTED
    );
}
