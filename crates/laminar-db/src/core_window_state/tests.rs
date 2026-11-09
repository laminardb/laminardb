use super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray};
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

fn concrete_templates(agg_specs: &[AggFuncSpec]) -> Vec<ConcreteAggregateState> {
    agg_specs
        .iter()
        .map(|spec| ConcreteAggregateState::try_new(spec, ConcreteInputMode::AppendOnly).unwrap())
        .collect()
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
        vnode_applied_rows: vec![0; 1].into_boxed_slice(),
        window_group_counts: FxHashMap::default(),
        session_group_count: 0,
        aggregate_templates: concrete_templates(&agg_specs),
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
        key_grouper: None,
        scratch_grouped: FxHashMap::default(),
        scratch_key_ids: BatchKeyIds::default(),
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
        vnode_applied_rows: vec![0; 1].into_boxed_slice(),
        window_group_counts: FxHashMap::default(),
        session_group_count: 0,
        aggregate_templates: concrete_templates(&agg_specs),
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
        key_grouper: None,
        scratch_grouped: FxHashMap::default(),
        scratch_key_ids: BatchKeyIds::default(),
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
        vnode_applied_rows: vec![0; 1].into_boxed_slice(),
        window_group_counts: FxHashMap::default(),
        session_group_count: 0,
        aggregate_templates: concrete_templates(&agg_specs),
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
        key_grouper: None,
        scratch_grouped: FxHashMap::default(),
        scratch_key_ids: BatchKeyIds::default(),
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
        vnode_applied_rows: vec![0; 1].into_boxed_slice(),
        window_group_counts: FxHashMap::default(),
        session_group_count: 0,
        aggregate_templates: concrete_templates(&agg_specs),
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
        key_grouper: None,
        scratch_grouped: FxHashMap::default(),
        scratch_key_ids: BatchKeyIds::default(),
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
fn applied_rows_count_each_row_once_per_window_and_skip_late_rows() {
    let applied = |state: &CoreWindowState| {
        state
            .vnode_loads()
            .iter()
            .map(|load| load.applied_rows)
            .sum::<u64>()
    };
    let batch = make_pre_agg_batch(vec!["A", "A", "B"], vec![1, 2, 3], vec![100, 500, 1500]);

    let mut tumbling = make_core_window_state(1000);
    tumbling.update_batch(&batch).unwrap();
    assert_eq!(applied(&tumbling), 3);
    // Window 1000 stays open, so the one vnode keeps state and stays in the loads.
    tumbling.close_windows(1000).unwrap();
    tumbling
        .update_batch(&make_pre_agg_batch(vec!["A"], vec![4], vec![200]))
        .unwrap();
    assert_eq!(applied(&tumbling), 3, "a late row is not applied");

    let mut hopping = make_hopping_core_window_state(1000, 500);
    hopping.update_batch(&batch).unwrap();
    assert_eq!(applied(&hopping), 6);

    let mut session = make_session_core_window_state(1000);
    session.update_batch(&batch).unwrap();
    assert_eq!(applied(&session), 3);
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
            .map(|(_, checkpoint)| checkpoint.window_starts.len())
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

    let batches = state.close_windows(6000).unwrap();
    assert_eq!(batches.len(), 1, "one close emits one batch");
    let mut actual = batches
        .iter()
        .flat_map(|batch| {
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
            (0..batch.num_rows())
                .map(|row| (starts.value(row), ends.value(row), totals.value(row)))
                .collect::<Vec<_>>()
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
    assert!(
        matches!(
            error,
            crate::DbError::GroupLimitExceeded {
                groups: 3,
                limit: 2,
                ..
            }
        ),
        "{error}"
    );
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
        .any(|(_, checkpoint)| !checkpoint.window_starts.is_empty()));

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
        .any(|(_, checkpoint)| !checkpoint.group_session_ends.is_empty()));

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

#[tokio::test]
async fn compiled_window_pre_aggregation_coerces_mixed_operand_types() {
    use arrow::array::AsArray;
    use arrow::datatypes::{Decimal128Type, Float64Type};
    use std::time::Duration;

    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("qty", DataType::Decimal128(18, 8), false),
        Field::new("px", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let qty = arrow::array::Decimal128Array::from(vec![150_000_000_i128, 200_000_000, 50_000_000])
        .with_precision_and_scale(18, 8)
        .unwrap();
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A"])),
            Arc::new(StringArray::from(vec!["F", "N", "F"])),
            Arc::new(qty),
            Arc::new(arrow::array::Float64Array::from(vec![2.5, 3.0, 1.0])),
            Arc::new(Int64Array::from(vec![1_000, 2_000, 3_000])),
        ],
    )
    .unwrap();
    let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
    ctx.register_table("fills", Arc::new(table)).unwrap();
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
    let sql = "SELECT symbol, SUM(CASE WHEN kind = 'F' THEN qty ELSE 0 END) AS volume, \
               MAX(px * 1000) AS px_milli FROM fills WHERE qty >= 1 GROUP BY symbol";
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

    let projection = state
        .compiled_projection()
        .expect("compiled pre-aggregation");
    let pre_aggregated =
        crate::operator_graph::try_evaluate_compiled(projection, &[input]).unwrap();
    for batch in &pre_aggregated {
        state.update_batch(batch).unwrap();
    }
    let emitted = state.close_windows(11_000).unwrap();
    assert_eq!(emitted.len(), 1);
    // The 0.5 fill fails WHERE; the non-fill contributes the ELSE branch.
    let volume = emitted[0].column_by_name("volume").unwrap();
    assert_eq!(volume.data_type(), &DataType::Decimal128(28, 8));
    assert_eq!(
        volume.as_primitive::<Decimal128Type>().value(0),
        150_000_000
    );
    let px_milli = emitted[0].column_by_name("px_milli").unwrap();
    assert_eq!(px_milli.as_primitive::<Float64Type>().value(0), 3000.0);
}

fn restore_one(
    mut state: CoreWindowState,
    vnode: u32,
    checkpoint: CoreWindowVnodeCheckpoint,
) -> Result<(), DbError> {
    let vnode_count = u32::from(state.key_group_count());
    state.restore_high_watermark_ms(checkpoint.frontier_floor_ms)?;
    state.restore_vnode(vnode, vnode_count, checkpoint)
}

fn only_populated_vnode(
    checkpoints: Vec<(u32, CoreWindowVnodeCheckpoint)>,
) -> (u32, CoreWindowVnodeCheckpoint) {
    let mut populated = checkpoints.into_iter().filter(|(_, checkpoint)| {
        !checkpoint.window_starts.is_empty() || !checkpoint.group_session_ends.is_empty()
    });
    let only = populated.next().unwrap();
    assert!(populated.next().is_none());
    only
}

#[test]
fn fixed_window_frame_rejects_non_canonical_rosters() {
    let mut state = make_core_window_state(1000);
    let batch = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![100, 1500]);
    state.update_batch(&batch).unwrap();
    let (vnode, checkpoint) = only_populated_vnode(checkpoint_all(&mut state));
    assert_eq!(checkpoint.window_starts, [0, 1000]);
    assert_eq!(checkpoint.window_group_ends, [1, 2]);
    assert_eq!(checkpoint.acc_state_ipc.len(), 1);

    let vnode_count = u32::from(state.key_group_count());
    let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint).unwrap();
    assert!(state
        .preflight_vnode_bytes(vnode, vnode_count, &archive)
        .is_ok());
    // Restore rejects every edit; returns whether the archive preflight also rejects it.
    let preflight_rejects = |edit: fn(&mut CoreWindowVnodeCheckpoint), expected: &str| {
        let mut corrupt = checkpoint.clone();
        edit(&mut corrupt);
        let error = restore_one(make_core_window_state(1000), vnode, corrupt.clone())
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{expected}: {error}");
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&corrupt).unwrap();
        state
            .preflight_vnode_bytes(vnode, vnode_count, &archive)
            .is_err()
    };

    for (edit, expected) in [
        (
            (|frame| frame.window_group_ends = vec![0, 2]) as fn(&mut CoreWindowVnodeCheckpoint),
            "non-canonical",
        ),
        (|frame| frame.window_starts = vec![1000, 0], "non-canonical"),
        (|frame| frame.group_session_ends = vec![1], "non-canonical"),
        (
            |frame| frame.acc_state_ipc.clear(),
            "non-canonical state payloads",
        ),
        (
            |frame| frame.keys_ipc.clear(),
            "non-canonical state payloads",
        ),
    ] {
        assert!(preflight_rejects(edit, expected), "{expected}");
    }
    // Duplicate keys and batch row counts are only visible in the decoded batches.
    assert!(!preflight_rejects(
        |frame| {
            frame.window_starts = vec![0];
            frame.window_group_ends = vec![2];
        },
        "duplicate group key",
    ));
    assert!(!preflight_rejects(
        |frame| frame.window_group_ends = vec![1, 3],
        "expected 3"
    ));
}

#[test]
fn session_frame_rejects_mismatched_interval_rosters() {
    let mut state = make_session_core_window_state(3000);
    let batch = make_pre_agg_batch(vec!["A", "A"], vec![10, 20], vec![1000, 9000]);
    state.update_batch(&batch).unwrap();
    let (vnode, checkpoint) = only_populated_vnode(checkpoint_all(&mut state));
    assert_eq!(checkpoint.group_session_ends, [2]);
    assert_eq!(checkpoint.session_starts, [1000, 9000]);
    assert_eq!(checkpoint.session_starts.capacity(), 2);
    assert_eq!(checkpoint.session_ends.capacity(), 2);
    assert!(checkpoint.window_starts.is_empty());

    for (edit, expected) in [
        (
            (|frame| {
                frame.session_ends.pop();
            }) as fn(&mut CoreWindowVnodeCheckpoint),
            "non-canonical session interval roster",
        ),
        (
            |frame| frame.session_starts[1] = 2000,
            "not strictly ordered and disjoint",
        ),
        (
            |frame| frame.group_session_ends = vec![1, 1],
            "non-canonical",
        ),
        (
            |frame| frame.window_starts = vec![0],
            "non-canonical fixed-window roster",
        ),
    ] {
        let mut corrupt = checkpoint.clone();
        edit(&mut corrupt);
        let error = restore_one(make_session_core_window_state(3000), vnode, corrupt)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{expected}: {error}");
    }
    restore_one(make_session_core_window_state(3000), vnode, checkpoint).unwrap();
}

/// A window without GROUP BY keeps no key batch, and AVG's two-column state round-trips.
#[tokio::test]
async fn global_window_frame_restores_multi_column_state() {
    use std::time::Duration;

    let (ctx, schema) = sql_window_context();
    let config = WindowOperatorConfig {
        window_type: WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: Duration::from_secs(1),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    };
    let sql = "SELECT TUMBLE(ts, INTERVAL '1' SECOND) AS window_start, \
               SUM(amount) AS total, AVG(amount) AS mean \
               FROM events GROUP BY TUMBLE(ts, INTERVAL '1' SECOND)";
    let build = || async {
        CoreWindowState::try_from_sql(&ctx, sql, &config, None, key_groups())
            .await
            .unwrap()
            .unwrap()
    };
    let mut state = build().await;
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
            Arc::new(Int64Array::from(vec![2, 4, 9])),
            Arc::new(Int64Array::from(vec![100, 200, 1500])),
        ],
    )
    .unwrap();
    let projected = state
        .compiled_projection()
        .unwrap()
        .evaluate(&input)
        .unwrap();
    state.update_batch(&projected).unwrap();

    let (vnode, checkpoint) = only_populated_vnode(checkpoint_all(&mut state));
    assert_eq!(checkpoint.window_group_ends, [1, 2]);
    assert!(checkpoint.keys_ipc.is_empty());
    assert_eq!(checkpoint.acc_state_ipc.len(), 2);

    let mut restored = build().await;
    let vnode_count = u32::from(restored.key_group_count());
    restored
        .restore_high_watermark_ms(checkpoint.frontier_floor_ms)
        .unwrap();
    restored
        .restore_vnode(vnode, vnode_count, checkpoint)
        .unwrap();
    let batches = restored.close_windows(2000).unwrap();
    assert_eq!(batches.len(), 1, "one close emits one batch");
    let column = |name: &str| Arc::clone(batches[0].column_by_name(name).unwrap());
    let totals = column("total");
    let means = column("mean");
    let totals = totals.as_any().downcast_ref::<Int64Array>().unwrap();
    let means = means
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    let windows = (0..batches[0].num_rows())
        .map(|row| (totals.value(row), means.value(row)))
        .collect::<Vec<_>>();
    assert_eq!(windows, [(6, 3.0), (9, 9.0)]);
}

type KindRow = (
    &'static str,
    Option<i64>,
    Option<f64>,
    Option<i128>,
    Option<&'static str>,
    i64,
);

fn every_kind_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, true),
        Field::new("p", DataType::Float64, true),
        Field::new("d", DataType::Decimal128(10, 2), true),
        Field::new("s", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn every_kind_batch(rows: &[KindRow], ts_divisor: i64) -> RecordBatch {
    RecordBatch::try_new(
        every_kind_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.1))),
            Arc::new(arrow::array::Float64Array::from_iter(
                rows.iter().map(|row| row.2),
            )),
            Arc::new(
                arrow::array::Decimal128Array::from_iter(rows.iter().map(|row| row.3))
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(StringArray::from_iter(rows.iter().map(|row| row.4))),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.5 / ts_divisor),
            )),
        ],
    )
    .unwrap()
}

fn sorted_by_key(batch: &RecordBatch) -> RecordBatch {
    let order =
        arrow::compute::sort_to_indices(batch.column_by_name("k").unwrap(), None, None).unwrap();
    arrow::compute::take_record_batch(batch, &order).unwrap()
}

/// Every concrete aggregate kind, updated over two batches, merged across sessions and restored
/// from a checkpoint, emits what `DataFusion` computes over the same rows.
#[tokio::test]
async fn windows_aggregate_every_kind_like_datafusion_through_merge_and_restore() {
    use std::time::Duration;

    const AGGREGATES: &str = "COUNT(*) AS n, COUNT(v) AS nv, SUM(v) AS sv, SUM(p) AS sp, \
        SUM(d) AS sd, AVG(p) AS ap, AVG(d) AS ad, MIN(s) AS lo, MAX(s) AS hi, \
        MAX(v) FILTER (WHERE p > 1) AS fv";
    // With a one-second gap, each key's first batch opens two sessions and its second-batch row
    // bridges them, so the session case merges two sessions and a row into one.
    let first: [KindRow; 5] = [
        ("a", Some(7), Some(1.25), Some(1_050), Some("pear"), 0),
        ("a", None, Some(0.5), None, Some("apple"), 100),
        ("a", Some(-3), None, Some(-275), None, 1_600),
        ("b", Some(40), Some(2.75), Some(99_999), Some("fig"), 0),
        (
            "b",
            Some(i64::from(i32::MAX)),
            Some(3.0),
            Some(1),
            Some("date"),
            1_600,
        ),
    ];
    let second: [KindRow; 2] = [
        ("a", Some(11), Some(4.5), Some(5), Some("zucchini"), 1_050),
        ("b", None, None, None, None, 900),
    ];

    let reference = {
        let ctx = SessionContext::new();
        let rows = every_kind_batch(&[first.as_slice(), &second].concat(), 1);
        let table =
            datafusion::datasource::MemTable::try_new(every_kind_schema(), vec![vec![rows]])
                .unwrap();
        ctx.register_table("rows", Arc::new(table)).unwrap();
        let batches = ctx
            .sql(&format!(
                "SELECT k, {AGGREGATES} FROM rows GROUP BY k ORDER BY k"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
    };

    let tumble = WindowOperatorConfig::tumbling("ts".into(), Duration::from_secs(1));
    let session = WindowOperatorConfig::session("ts".into(), Duration::from_secs(1));
    for (window, config, ts_divisor, close_at) in [
        ("TUMBLE(ts, INTERVAL '1' SECOND)", tumble, 10, 1_000),
        ("SESSION(ts, INTERVAL '1' SECOND)", session, 1, 2_600),
    ] {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let table =
            datafusion::datasource::MemTable::try_new(every_kind_schema(), vec![vec![]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        let sql = format!("SELECT k, {window} AS w, {AGGREGATES} FROM events GROUP BY k, {window}");
        let build = || async {
            CoreWindowState::try_from_sql(&ctx, &sql, &config, None, key_groups())
                .await
                .unwrap()
                .unwrap()
        };
        let mut state = build().await;
        for rows in [first.as_slice(), &second] {
            let projected = state
                .compiled_projection()
                .unwrap()
                .evaluate(&every_kind_batch(rows, ts_divisor))
                .unwrap();
            state.update_batch(&projected).unwrap();
        }
        assert_session_deadline_index(&state);

        let mut restored = build().await;
        restore_all(&mut restored, checkpoint_all(&mut state)).unwrap();
        for emitted in [
            state.close_windows(close_at),
            restored.close_windows(close_at),
        ] {
            let emitted = emitted.unwrap();
            assert_eq!(emitted.len(), 1, "{window}");
            let emitted = sorted_by_key(&emitted[0]);
            for field in reference.schema().fields() {
                assert_eq!(
                    emitted.column_by_name(field.name()).unwrap().as_ref(),
                    reference.column_by_name(field.name()).unwrap().as_ref(),
                    "{window} {}",
                    field.name()
                );
            }
        }
    }
}

#[test]
fn window_sum_overflow_fails_instead_of_wrapping() {
    let mut state = make_core_window_state(1000);
    state
        .update_batch(&make_pre_agg_batch(vec!["A"], vec![i64::MAX], vec![100]))
        .unwrap();

    let error = state
        .update_batch(&make_pre_agg_batch(vec!["A"], vec![1], vec![200]))
        .unwrap_err();

    assert!(error.to_string().contains("SUM state overflow"), "{error}");
    assert_eq!(sum_total(&state.close_windows(1000).unwrap()[0]), i64::MAX);
}

async fn sql_window_over_vnodes(sql: &str, config: &WindowOperatorConfig) -> CoreWindowState {
    let (ctx, _) = sql_window_context();
    CoreWindowState::try_from_sql(
        &ctx,
        sql,
        config,
        None,
        KeyGroupCount::try_from(16_u16).unwrap(),
    )
    .await
    .unwrap()
    .unwrap()
}

fn apply_events(state: &mut CoreWindowState, events: &[(String, i64, i64)]) {
    let (_, schema) = sql_window_context();
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                events
                    .iter()
                    .map(|(key, ..)| key.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                events
                    .iter()
                    .map(|&(_, amount, _)| amount)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                events.iter().map(|&(.., ts)| ts).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let projected = state
        .compiled_projection()
        .unwrap()
        .evaluate(&input)
        .unwrap();
    state.update_batch(&projected).unwrap();
}

fn final_schema(state: &CoreWindowState) -> SchemaRef {
    state.post_projection.as_ref().map_or_else(
        || Arc::clone(&state.output_schema),
        |projection| Arc::clone(&projection.final_schema),
    )
}

/// Each output row's vnode and its `(user_id, window_start, total)` values, in batch order.
fn closed_rows_by_vnode(
    state: &CoreWindowState,
    batch: &RecordBatch,
) -> Vec<(u32, (String, i64, i64))> {
    let keys = batch.column_by_name("user_id").unwrap();
    let encoded = state
        .row_converter
        .convert_columns(std::slice::from_ref(keys))
        .unwrap();
    let keys = keys.as_any().downcast_ref::<StringArray>().unwrap();
    let starts = batch
        .column_by_name("window_start")
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
    (0..batch.num_rows())
        .map(|row| {
            (
                state.vnode_for_group_key(encoded.row(row).as_ref()),
                (
                    keys.value(row).to_string(),
                    starts.value(row),
                    totals.value(row),
                ),
            )
        })
        .collect()
}

/// Rows of one vnode are contiguous and each vnode's rows satisfy `in_order`.
fn assert_vnode_major(
    rows: &[(u32, (String, i64, i64))],
    in_order: impl Fn(&(String, i64, i64), &(String, i64, i64)) -> bool,
) {
    let mut seen = BTreeSet::new();
    for pair in rows.windows(2) {
        let ((left_vnode, left), (right_vnode, right)) = (&pair[0], &pair[1]);
        if left_vnode == right_vnode {
            assert!(in_order(left, right), "{left:?} then {right:?}");
        } else {
            assert!(seen.insert(*left_vnode), "vnode {left_vnode} split");
        }
    }
    let vnodes = rows
        .iter()
        .map(|(vnode, _)| *vnode)
        .collect::<BTreeSet<_>>();
    assert!(vnodes.len() > 1, "keys must span vnodes: {vnodes:?}");
}

#[tokio::test]
async fn hop_close_emits_one_batch_across_vnodes_and_windows() {
    use std::time::Duration;

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
    let mut state = sql_window_over_vnodes(
        "SELECT user_id, HOP(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND) AS window_start, \
                SUM(amount) AS total \
         FROM events \
         GROUP BY user_id, HOP(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND) \
         HAVING SUM(amount) > 0",
        &config,
    )
    .await;
    let keys = (0..32).map(|key| format!("k{key:02}")).collect::<Vec<_>>();
    let mut events = Vec::new();
    for (index, key) in (0_i64..).zip(&keys) {
        events.push((key.clone(), index, 1000));
        events.push((key.clone(), 100 + index, 3000));
    }
    apply_events(&mut state, &events);

    let batches = state.close_windows(6000).unwrap();
    assert_eq!(batches.len(), 1, "one close emits one batch");
    assert_eq!(batches[0].schema(), final_schema(&state));
    let rows = closed_rows_by_vnode(&state, &batches[0]);
    assert_vnode_major(&rows, |left, right| left.1 <= right.1);

    let mut expected = Vec::new();
    for (index, key) in (0_i64..).zip(&keys) {
        if index > 0 {
            expected.push((key.clone(), -2_000_000, index));
        }
        expected.push((key.clone(), 0, 100 + 2 * index));
        expected.push((key.clone(), 2_000_000, 100 + index));
    }
    expected.sort_unstable();
    let mut actual = rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, expected);
    assert!(state.close_windows(8000).unwrap().is_empty());
}

#[tokio::test]
async fn session_close_emits_one_batch_across_vnodes() {
    use std::time::Duration;

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
    let mut state = sql_window_over_vnodes(
        "SELECT user_id, SESSION(ts, INTERVAL '3' SECOND) AS window_start, \
                SUM(amount) AS total \
         FROM events GROUP BY user_id, SESSION(ts, INTERVAL '3' SECOND)",
        &config,
    )
    .await;
    let keys = (0..32).map(|key| format!("k{key:02}")).collect::<Vec<_>>();
    let mut events = Vec::new();
    for (index, key) in (0_i64..).zip(&keys) {
        events.push((key.clone(), 10 + index, 10_000));
        events.push((key.clone(), index, 1000 + index));
    }
    apply_events(&mut state, &events);

    let batches = state.close_windows(20_000).unwrap();
    assert_eq!(batches.len(), 1, "one close emits one batch");
    assert_eq!(batches[0].schema(), final_schema(&state));
    let rows = closed_rows_by_vnode(&state, &batches[0]);
    assert_vnode_major(&rows, |left, right| left.1 <= right.1);

    let mut expected = Vec::new();
    for (index, key) in (0_i64..).zip(&keys) {
        expected.push((key.clone(), (1000 + index) * 1000, index));
        expected.push((key.clone(), 10_000_000, 10 + index));
    }
    expected.sort_unstable();
    let mut actual = rows.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, expected);
}
