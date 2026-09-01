use super::*;
use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use std::time::Duration;

fn execute_interval_join_cycle(
    state: &mut IntervalJoinState,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &StreamJoinConfig,
    left_watermark: i64,
    right_watermark: i64,
) -> Result<Vec<RecordBatch>, DbError> {
    let mut output_budget = IntervalJoinOutputBudget::default();
    super::execute_interval_join_cycle(
        state,
        left_batches,
        right_batches,
        config,
        left_watermark,
        right_watermark,
        left_watermark,
        right_watermark,
        usize::MAX,
        &mut output_budget,
    )
}

fn execute_weighted_cycle(
    state: &mut IntervalJoinState,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    config: &StreamJoinConfig,
    left_watermark: i64,
    right_watermark: i64,
) -> Result<Vec<RecordBatch>, DbError> {
    let mut output_budget = IntervalJoinOutputBudget::default();
    execute_interval_join_cycle_with_mode(
        state,
        left_batches,
        right_batches,
        config,
        left_watermark,
        right_watermark,
        left_watermark,
        right_watermark,
        usize::MAX,
        &mut output_budget,
        JoinExecutionMode::Weighted,
    )
}

fn weighted_batch(batch: RecordBatch, weights: &[i64]) -> RecordBatch {
    assert_eq!(batch.num_rows(), weights.len());
    let input_schema = batch.schema();
    let mut fields = input_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(
        laminar_core::changelog::WEIGHT_COLUMN,
        DataType::Int64,
        false,
    ));
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(weights.to_vec())));
    RecordBatch::try_new(schema, columns).unwrap()
}

fn emitted_weights(output: &[RecordBatch]) -> Vec<i64> {
    output
        .iter()
        .flat_map(|batch| {
            let weight_index = batch.num_columns() - 1;
            assert_eq!(
                batch.schema().field(weight_index).name(),
                laminar_core::changelog::WEIGHT_COLUMN
            );
            assert!(!batch.schema().field(weight_index).is_nullable());
            assert_eq!(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .filter(|field| { field.name() == laminar_core::changelog::WEIGHT_COLUMN })
                    .count(),
                1
            );
            batch
                .column(weight_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn make_config() -> StreamJoinConfig {
    StreamJoinConfig {
        join_type: JoinType::Inner,
        left_keys: vec!["id".to_string()],
        right_keys: vec!["id".to_string()],
        left_time_column: "ts".to_string(),
        right_time_column: "ts".to_string(),
        left_table: "left_stream".to_string(),
        right_table: "right_stream".to_string(),
        time_bound: Duration::from_millis(100),
    }
}

fn left_batch(ids: &[&str], timestamps: &[i64], values: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("price", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn right_batch(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("amount", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(amounts.to_vec())),
        ],
    )
    .unwrap()
}

fn composite_batch(
    right: bool,
    ids: &[Option<&str>],
    regions: &[Option<i64>],
    timestamps: &[i64],
) -> RecordBatch {
    let value_name = if right { "amount" } else { "price" };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("region", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(value_name, DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(Int64Array::from(regions.to_vec())),
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(vec![1.0; ids.len()])),
        ],
    )
    .unwrap()
}

#[test]
fn all_bounded_join_kinds_have_final_watermark_semantics() {
    for (join_type, expected_rows, expected_columns) in [
        (JoinType::Inner, 1, 6),
        (JoinType::Left, 2, 6),
        (JoinType::Right, 2, 6),
        (JoinType::Full, 3, 6),
        (JoinType::LeftSemi, 1, 3),
        (JoinType::RightSemi, 1, 3),
        (JoinType::LeftAnti, 1, 3),
        (JoinType::RightAnti, 1, 3),
    ] {
        let mut config = make_config();
        config.join_type = join_type;
        let left = left_batch(&["A", "B"], &[100, 100], &[1.0, 2.0]);
        let right = right_batch(&["A", "C"], &[110, 110], &[3.0, 4.0]);
        let mut state = IntervalJoinState::new();
        state
            .seed_input_schemas(left.schema(), right.schema(), &config)
            .unwrap();

        let mut output =
            execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
        output.extend(
            execute_interval_join_cycle(&mut state, &[], &[], &config, 1_000, 1_000).unwrap(),
        );

        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows,
            "{join_type:?}"
        );
        assert!(
            output
                .iter()
                .all(|batch| batch.num_columns() == expected_columns),
            "{join_type:?}"
        );
        assert_eq!(state.buffered_rows(), (0, 0), "{join_type:?}");

        let schema = build_output_schema(
            state.left_schema.as_ref().unwrap(),
            state.right_schema.as_ref().unwrap(),
            &config,
        );
        if matches!(join_type, JoinType::Left | JoinType::Full) {
            assert!(schema.fields()[3..].iter().all(|field| field.is_nullable()));
        }
        if matches!(join_type, JoinType::Right | JoinType::Full) {
            assert!(schema.fields()[..3].iter().all(|field| field.is_nullable()));
        }
    }
}

#[test]
fn current_batch_rows_are_admitted_before_its_watermark_closes_them() {
    let config = make_config();
    let mut state = IntervalJoinState::new();
    let output = super::execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["A"], &[100], &[1.0])],
        &[right_batch(&["A"], &[110], &[2.0])],
        &config,
        i64::MIN,
        i64::MIN,
        300,
        300,
        usize::MAX,
        &mut IntervalJoinOutputBudget::default(),
    )
    .unwrap();

    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(state.buffered_rows(), (0, 0));

    let error = super::execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["late"], &[100], &[1.0])],
        &[],
        &config,
        300,
        300,
        300,
        300,
        usize::MAX,
        &mut IntervalJoinOutputBudget::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("below closed cutoff 300"));
    assert_eq!(state.buffered_rows(), (0, 0));
}

#[test]
fn lagging_input_uses_its_own_admission_watermark_before_cross_side_closure() {
    let mut config = make_config();
    config.join_type = JoinType::Left;
    let mut state = IntervalJoinState::new();
    state
        .seed_input_schemas(
            left_batch(&["schema"], &[0], &[0.0]).schema(),
            right_batch(&["schema"], &[0], &[0.0]).schema(),
            &config,
        )
        .unwrap();
    execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 300).unwrap();
    assert_eq!(state.left_evicted_cutoff, 200);

    let output = super::execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["valid"], &[150], &[1.0])],
        &[],
        &config,
        0,
        300,
        0,
        300,
        usize::MAX,
        &mut IntervalJoinOutputBudget::default(),
    )
    .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(state.buffered_rows(), (0, 0));
}

#[test]
fn composite_keys_match_in_order_and_null_tuples_never_match() {
    let mut config = make_config();
    config.left_keys = vec!["id".into(), "region".into()];
    config.right_keys = config.left_keys.clone();
    let left = composite_batch(
        false,
        &[Some("A"), Some("A"), None, Some("B")],
        &[Some(1), Some(2), Some(1), None],
        &[100; 4],
    );
    let right = composite_batch(
        true,
        &[Some("A"), Some("A"), None, Some("B")],
        &[Some(1), Some(3), Some(1), None],
        &[110; 4],
    );

    let mut inner = IntervalJoinState::new();
    inner
        .seed_input_schemas(left.schema(), right.schema(), &config)
        .unwrap();
    let output = execute_interval_join_cycle(
        &mut inner,
        std::slice::from_ref(&left),
        std::slice::from_ref(&right),
        &config,
        0,
        0,
    )
    .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    config.join_type = JoinType::LeftAnti;
    let mut anti = IntervalJoinState::new();
    anti.seed_input_schemas(left.schema(), right.schema(), &config)
        .unwrap();
    assert!(
        execute_interval_join_cycle(&mut anti, &[left], &[right], &config, 0, 0,)
            .unwrap()
            .is_empty()
    );
    let output = execute_interval_join_cycle(&mut anti, &[], &[], &config, 1_000, 1_000).unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[test]
fn checkpoint_preserves_semi_first_match_emission() {
    let mut config = make_config();
    config.join_type = JoinType::LeftSemi;
    let mut state = IntervalJoinState::new();
    let first = execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["A"], &[100], &[1.0])],
        &[right_batch(&["A"], &[110], &[1.0])],
        &config,
        0,
        0,
    )
    .unwrap();
    assert_eq!(first.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let checkpoint = state
        .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    assert!(!checkpoint.weighted);
    assert!(checkpoint.left_row_weights.is_empty());
    assert_eq!(checkpoint.left_match_flags, vec![vec![1]]);
    let mut incompatible = config.clone();
    incompatible.join_type = JoinType::RightSemi;
    let error = IntervalJoinState::from_checkpoint(
        &checkpoint,
        &incompatible,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
    )
    .err()
    .expect("join-state semantics must be checkpoint-bound");
    assert!(error.to_string().contains("configuration does not match"));
    let mut restored = IntervalJoinState::from_checkpoint(
        &checkpoint,
        &config,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
    )
    .unwrap();
    let repeated = execute_interval_join_cycle(
        &mut restored,
        &[],
        &[right_batch(&["A"], &[120], &[2.0])],
        &config,
        0,
        0,
    )
    .unwrap();
    assert!(repeated.is_empty());
}

#[test]
fn test_basic_inner_join_same_cycle() {
    let config = make_config();
    let mut state = IntervalJoinState::new();

    let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
    let right = right_batch(&["A", "B"], &[110, 250], &[1.0, 2.0]);

    let result = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

    // Both right timestamps fall between the matching left timestamp and left + 100ms.
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 2);
    assert_eq!(result[0].num_columns(), 6); // 3 left + 3 right
}

#[test]
fn test_cross_cycle_matching() {
    let config = make_config();
    let mut state = IntervalJoinState::new();

    // Cycle 1: only left data
    let left = left_batch(&["A"], &[100], &[10.0]);
    let result = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0, 0).unwrap();
    assert!(result.is_empty()); // No right data yet

    // Cycle 2: right data arrives, should match the buffered left
    let right = right_batch(&["A"], &[150], &[1.0]);
    let result = execute_interval_join_cycle(&mut state, &[], &[right], &config, 0, 0).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 1);
}

#[test]
fn test_time_bound_enforcement() {
    let config = make_config(); // time_bound = 100ms
    let mut state = IntervalJoinState::new();

    let left = left_batch(&["A"], &[100], &[10.0]);
    let right = right_batch(&["A", "A"], &[50, 300], &[1.0, 2.0]);

    let result = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
    assert!(result.is_empty()); // Before the left timestamp and after left + bound.
}

#[test]
fn test_eviction_on_watermark_advance() {
    let config = make_config(); // time_bound = 100ms
    let mut state = IntervalJoinState::new();

    // Cycle 1: buffer left row at ts=100
    let left = left_batch(&["A"], &[100], &[10.0]);
    let _ = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0, 0).unwrap();
    assert_eq!(state.left.row_count, 1);

    // Cycle 2: advance watermark to 300 → cutoff = 300 - 100 = 200
    // Row at ts=100 < 200, should be evicted
    let _ = execute_interval_join_cycle(&mut state, &[], &[], &config, 300, 300).unwrap();
    assert_eq!(state.left.row_count, 0);
}

#[test]
fn test_multiple_keys() {
    let config = make_config();
    let mut state = IntervalJoinState::new();

    let left = left_batch(&["A", "B"], &[100, 100], &[10.0, 20.0]);
    let right = right_batch(&["B", "A"], &[110, 110], &[1.0, 2.0]);

    let result = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

    // A@100 matches A@110 and B@100 matches B@110.
    // A@100 does NOT match B@110 (different keys)
    // B@100 does NOT match A@110 (different keys)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 2);
}

#[test]
fn test_no_double_emit() {
    let config = make_config();
    let mut state = IntervalJoinState::new();

    // Both sides in same cycle — each match should appear exactly once
    let left = left_batch(&["A"], &[100], &[10.0]);
    let right = right_batch(&["A"], &[110], &[1.0]);

    let result = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 1); // Exactly one match, not two
}

#[test]
fn test_empty_inputs() {
    let config = make_config();
    let mut state = IntervalJoinState::new();

    let result = execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 0).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_checkpoint_roundtrip() {
    let config = make_config();
    let mut state = IntervalJoinState::new();

    let left = left_batch(&["A"], &[100], &[10.0]);
    let right = right_batch(&["A"], &[110], &[1.0]);
    let _ = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 50, 50).unwrap();

    // Snapshot and serialize the retained cut.
    let cp = state
        .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    assert!(cp.left_buffer_rows > 0);
    assert!(cp.right_buffer_rows > 0);

    // Restore
    let mut restored = IntervalJoinState::from_checkpoint(
        &cp,
        &config,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
    )
    .unwrap();

    // New right data should still match the restored left
    let right2 = right_batch(&["A"], &[120], &[2.0]);
    let result =
        execute_interval_join_cycle(&mut restored, &[], &[right2], &config, 50, 50).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 1); // Matches restored A@100
}

#[test]
fn checkpoint_restore_rejects_row_count_mismatch() {
    let config = make_config();
    let mut state = IntervalJoinState::new();
    execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["A"], &[100], &[1.0])],
        &[],
        &config,
        0,
        0,
    )
    .unwrap();
    let mut checkpoint = state
        .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    checkpoint.left_buffer_rows += 1;

    let error = IntervalJoinState::from_checkpoint(
        &checkpoint,
        &config,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
    )
    .err()
    .expect("corrupt row-count metadata must fail restore");
    assert!(error.to_string().contains("row-count mismatch"));
}

#[test]
fn restored_frontier_rejects_a_genuinely_late_outer_row() {
    let mut config = make_config();
    config.join_type = JoinType::Left;
    let mut state = IntervalJoinState::new();
    execute_interval_join_cycle(&mut state, &[], &[], &config, 300, 300).unwrap();
    let checkpoint = state
        .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    let mut restored = IntervalJoinState::from_checkpoint(
        &checkpoint,
        &config,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
    )
    .unwrap();
    restored
        .seed_input_schemas(
            left_batch(&["schema"], &[300], &[1.0]).schema(),
            right_batch(&["schema"], &[300], &[1.0]).schema(),
            &config,
        )
        .unwrap();

    let error = super::execute_interval_join_cycle(
        &mut restored,
        &[left_batch(&["late"], &[150], &[1.0])],
        &[],
        &config,
        300,
        300,
        0,
        0,
        usize::MAX,
        &mut IntervalJoinOutputBudget::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("below closed cutoff 300"));
    assert_eq!(restored.buffered_rows(), (0, 0));
}

#[test]
fn managed_accounting_charges_index_topology() {
    let mut shared_timestamp = SideState::new();
    shared_timestamp
        .add_batch(
            &left_batch(&["A", "A"], &[100, 100], &[1.0, 2.0]),
            None,
            &["id".to_string()],
            "ts",
            false,
            false,
            JoinExecutionMode::AppendOnly,
        )
        .unwrap();
    let mut distinct_topology = SideState::new();
    distinct_topology
        .add_batch(
            &left_batch(&["A", "B"], &[100, 200], &[1.0, 2.0]),
            None,
            &["id".to_string()],
            "ts",
            false,
            false,
            JoinExecutionMode::AppendOnly,
        )
        .unwrap();

    assert!(distinct_topology.accounted_state_bytes() > shared_timestamp.accounted_state_bytes());
}

#[test]
fn checkpoint_capture_is_shallow_and_does_not_compact_live_state() {
    let config = make_config();
    let mut state = IntervalJoinState::new();
    execute_interval_join_cycle(
        &mut state,
        &[left_batch_nullable(
            &[Some("A"), None],
            &[100, 200],
            &[1.0, 2.0],
        )],
        &[],
        &config,
        0,
        0,
    )
    .unwrap();
    assert!(!state.left.is_compact());
    let first_column = state.left.batches[0].column(0).clone();

    let error = state
        .capture_checkpoint(&config, usize::MAX)
        .unwrap()
        .encode(1)
        .err()
        .expect("a one-byte checkpoint budget must reject Arrow IPC");
    assert!(error.to_string().contains("checkpoint limit"));
    assert_eq!(state.left.batches.len(), 1);
    assert!(!state.left.is_compact());
    assert!(Arc::ptr_eq(&first_column, state.left.batches[0].column(0)));

    let capture = state.capture_checkpoint(&config, usize::MAX).unwrap();
    assert!(Arc::ptr_eq(
        &first_column,
        capture.left_batches[0].column(0)
    ));
    let checkpoint = capture
        .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    assert_eq!(checkpoint.left_buffer_rows, 1);
    assert!(checkpoint.left_row_weights.is_empty());
    assert!(checkpoint.left_match_flags.is_empty());
    assert!(Arc::ptr_eq(&first_column, state.left.batches[0].column(0)));
    assert!(!state.left.is_compact());

    let mut multi_batch = IntervalJoinState::new();
    execute_interval_join_cycle(
        &mut multi_batch,
        &[left_batch(&["A"], &[100], &[1.0])],
        &[],
        &config,
        0,
        0,
    )
    .unwrap();
    execute_interval_join_cycle(
        &mut multi_batch,
        &[left_batch(&["B"], &[101], &[2.0])],
        &[],
        &config,
        0,
        0,
    )
    .unwrap();
    assert_eq!(multi_batch.left.batches.len(), 2);
    let (fixed_capture_bytes, left_batch_clones, right_batch_clones) =
        multi_batch.capture_preflight_bytes(&config).unwrap();
    let expected_batch_clones = charged_allocation(
        2_usize
            .checked_mul(std::mem::size_of::<RecordBatch>())
            .unwrap(),
    )
    .checked_add(
        2_usize
            .checked_mul(charged_allocation(
                3_usize
                    .checked_mul(std::mem::size_of::<ArrayRef>())
                    .unwrap(),
            ))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(left_batch_clones, expected_batch_clones);
    assert_eq!(right_batch_clones, 0);
    let exact_preflight = fixed_capture_bytes
        .checked_add(left_batch_clones)
        .and_then(|bytes| bytes.checked_add(right_batch_clones))
        .unwrap();
    let error = multi_batch
        .capture_checkpoint(&config, exact_preflight - 1)
        .err()
        .expect("one byte below the shallow-clone preflight must reject capture");
    assert!(error.to_string().contains("capture requires"));
}

#[test]
fn post_cut_support_mutation_preserves_captured_support() {
    let mut config = make_config();
    config.join_type = JoinType::LeftSemi;
    let mut state = IntervalJoinState::new();
    execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["A"], &[100], &[1.0])],
        &[],
        &config,
        0,
        0,
    )
    .unwrap();
    let capture = state.capture_checkpoint(&config, usize::MAX).unwrap();

    execute_interval_join_cycle(
        &mut state,
        &[],
        &[right_batch(&["A"], &[110], &[2.0])],
        &config,
        0,
        0,
    )
    .unwrap();
    assert_eq!(state.left.match_flags[0][0], 1);
    assert!(!Arc::ptr_eq(
        &capture.left_match_flags[0],
        &state.left.match_flags[0]
    ));

    let checkpoint = capture
        .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    assert_eq!(checkpoint.left_match_flags, vec![vec![0]]);
}

#[test]
fn restore_cardinality_preflight_precedes_ipc_decode() {
    let checkpoint = JoinStateCheckpoint {
        weighted: false,
        join_type: join_type_tag(JoinType::Inner),
        left_keys: vec!["id".into()],
        right_keys: vec!["id".into()],
        left_time_column: "ts".into(),
        right_time_column: "ts".into(),
        left_table: "left_stream".into(),
        right_table: "right_stream".into(),
        bound_ms: 100,
        left_buffer_rows: 2,
        right_buffer_rows: 0,
        left_batches: vec![vec![0xff]],
        right_batches: Vec::new(),
        left_evicted_cutoff: i64::MIN,
        right_evicted_cutoff: i64::MIN,
        left_row_weights: Vec::new(),
        right_row_weights: Vec::new(),
        left_match_flags: Vec::new(),
        right_match_flags: Vec::new(),
        left_match_weights: Vec::new(),
        right_match_weights: Vec::new(),
    };
    let error = IntervalJoinState::from_checkpoint(
        &checkpoint,
        &make_config(),
        RESTORE_WORST_CASE_ROW_CHARGE,
    )
    .err()
    .expect("oversized restore cardinality must fail before IPC decode");
    assert!(error
        .to_string()
        .contains("worst-case decoded index charge"));
    assert!(!error.to_string().contains("deserialization"));
}

#[test]
fn input_growth_preflight_rejects_before_mutation() {
    let state = IntervalJoinState::new();
    let error = state
        .preflight_input_growth(
            &[left_batch(&["A"], &[100], &[1.0])],
            &[],
            JoinType::Inner,
            JoinExecutionMode::AppendOnly,
            state.accounted_state_bytes(),
        )
        .unwrap_err();
    assert!(matches!(error, DbError::BackpressureFail(_)));
    assert_eq!(state.buffered_rows(), (0, 0));
}

#[test]
fn watermark_only_cycle_frees_state_for_later_input_growth() {
    let config = make_config();
    let mut state = IntervalJoinState::new();
    let old_key = "old".repeat(4 * 1024);
    execute_interval_join_cycle(
        &mut state,
        &[left_batch(&[old_key.as_str()], &[100], &[1.0])],
        &[],
        &config,
        0,
        0,
    )
    .unwrap();
    let incoming = left_batch(&["new"], &[1_000], &[2.0]);
    let limit = state.accounted_state_bytes();
    let before_eviction = state
        .accounted_state_bytes()
        .checked_add(
            state
                .left
                .worst_case_input_growth(
                    std::slice::from_ref(&incoming),
                    false,
                    JoinExecutionMode::AppendOnly,
                )
                .unwrap(),
        )
        .unwrap();
    assert!(before_eviction > limit);

    let error = super::execute_interval_join_cycle(
        &mut state,
        std::slice::from_ref(&incoming),
        &[],
        &config,
        0,
        0,
        0,
        1_000,
        limit,
        &mut IntervalJoinOutputBudget::default(),
    )
    .unwrap_err();
    assert!(matches!(error, DbError::BackpressureFail(_)));
    assert_eq!(state.buffered_rows(), (1, 0));

    execute_interval_join_cycle(&mut state, &[], &[], &config, 0, 1_000).unwrap();
    super::execute_interval_join_cycle(
        &mut state,
        &[incoming],
        &[],
        &config,
        0,
        1_000,
        0,
        1_000,
        limit,
        &mut IntervalJoinOutputBudget::default(),
    )
    .unwrap();
    assert_eq!(state.buffered_rows(), (1, 0));
}

#[test]
fn hot_key_preflight_does_not_recharge_historical_hash_rows() {
    let mut state = IntervalJoinState::new();
    let keys = vec!["hot"; 1_024];
    let timestamps = vec![100; keys.len()];
    let values = vec![1.0; keys.len()];
    state
        .left
        .add_batch(
            &left_batch(&keys, &timestamps, &values),
            None,
            &["id".to_string()],
            "ts",
            false,
            false,
            JoinExecutionMode::AppendOnly,
        )
        .unwrap();

    state
        .preflight_input_growth(
            &[left_batch(&["hot"], &[100], &[1.0])],
            &[],
            JoinType::Inner,
            JoinExecutionMode::AppendOnly,
            state.accounted_state_bytes().saturating_add(64 * 1024),
        )
        .unwrap();
}

#[test]
fn compaction_failure_leaves_original_state_intact() {
    let mut side = SideState::new();
    side.add_batch(
        &left_batch(&["A"], &[100], &[1.0]),
        None,
        &["id".to_string()],
        "ts",
        false,
        false,
        JoinExecutionMode::AppendOnly,
    )
    .unwrap();
    let before_index = side.index.clone();
    let before_batch = side.batches[0].clone();

    let error = side.compact(&["missing".to_string()], "ts").unwrap_err();
    assert!(error.to_string().contains("missing"));
    assert_eq!(side.row_count, 1);
    assert_eq!(side.index, before_index);
    assert_eq!(side.batches.len(), 1);
    assert!(Arc::ptr_eq(
        side.batches[0].column(0),
        before_batch.column(0)
    ));
}

#[test]
fn schema_fault_is_rejected_before_either_side_changes() {
    let config = make_config();
    let mut state = IntervalJoinState::new();
    execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["seed"], &[100], &[1.0])],
        &[right_batch(&["seed"], &[100], &[1.0])],
        &config,
        0,
        0,
    )
    .unwrap();

    let checkpoint_bytes = |state: &mut IntervalJoinState| {
        let checkpoint = state
            .snapshot_checkpoint(&config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap();
        rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .unwrap()
            .to_vec()
    };
    let before = checkpoint_bytes(&mut state);

    let incompatible_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("price", DataType::Int64, false),
    ]));
    let incompatible = RecordBatch::try_new(
        incompatible_schema,
        vec![
            Arc::new(StringArray::from(vec!["new"])),
            Arc::new(TimestampMillisecondArray::from(vec![110])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .unwrap();

    let error = execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["new"], &[110], &[2.0]), incompatible],
        &[right_batch(&["new"], &[110], &[2.0])],
        &config,
        0,
        0,
    )
    .unwrap_err();
    assert!(matches!(error, DbError::SchemaMismatch(_)));
    assert_eq!(checkpoint_bytes(&mut state), before);
}

fn left_batch_nullable(ids: &[Option<&str>], timestamps: &[i64], values: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("price", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn right_batch_nullable(ids: &[Option<&str>], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("amount", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(amounts.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn test_null_key_no_match() {
    let config = make_config();
    let mut state = IntervalJoinState::new();

    // Left has a null key row, right has a matching timestamp
    let left = left_batch_nullable(&[Some("A"), None], &[100, 100], &[10.0, 20.0]);
    let right = right_batch_nullable(&[Some("A"), None], &[110, 110], &[1.0, 2.0]);

    let result = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

    // Only A matches A — null keys never match (SQL three-valued logic)
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 1);
}

#[test]
fn all_null_keys_are_not_retained() {
    let config = make_config();
    let mut state = IntervalJoinState::new();
    let result = execute_interval_join_cycle(
        &mut state,
        &[left_batch_nullable(&[None], &[100], &[1.0])],
        &[right_batch_nullable(&[None], &[100], &[1.0])],
        &config,
        0,
        0,
    )
    .unwrap();
    assert!(result.is_empty());
    assert!(state.left.batches.is_empty());
    assert!(state.right.batches.is_empty());
}

#[test]
fn test_compaction_frees_batches() {
    let config = make_config(); // time_bound = 100ms
    let mut state = IntervalJoinState::new();

    // Add 40+ single-row batches to left side
    for i in 0i64..40 {
        let ts = i * 10 + 1000;
        #[allow(clippy::cast_precision_loss)]
        let left = left_batch(&["A"], &[ts], &[i as f64]);
        let _ = execute_interval_join_cycle(&mut state, &[left], &[], &config, 0, 0).unwrap();
    }
    assert!(state.left.batches.len() >= 40);

    // Evict the first half (ts < 1200). Watermark = 1300 → cutoff = 1300 - 100 = 1200
    let _ = execute_interval_join_cycle(&mut state, &[], &[], &config, 1300, 1300).unwrap();

    // After compaction (triggered because batch count > COMPACTION_THRESHOLD),
    // should have exactly 1 batch with only live rows
    assert_eq!(state.left.batches.len(), 1);
    assert!(state.left.row_count > 0);

    // Verify live rows are still accessible by probing with a right-side match
    let right = right_batch(&["A"], &[1350], &[99.0]);
    let result =
        execute_interval_join_cycle(&mut state, &[], &[right], &config, 1300, 1300).unwrap();
    // A right row at 1350 matches left rows within [1250, 1350].
    assert!(!result.is_empty());
}

#[test]
fn retracting_cdc_fails_before_either_side_mutates() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("amount", DataType::Float64, false),
        Field::new("_op", DataType::Utf8, false),
    ]));
    let delete = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(TimestampMillisecondArray::from(vec![100])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(StringArray::from(vec!["D"])),
        ],
    )
    .unwrap();
    let mut state = IntervalJoinState::new();
    let error = execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["A"], &[100], &[1.0])],
        &[delete],
        &make_config(),
        0,
        0,
    )
    .unwrap_err();
    assert!(error.to_string().contains("append-only"));
    assert_eq!(state.left.row_count, 0);
    assert_eq!(state.right.row_count, 0);
    assert!(state.left.batches.is_empty());
    assert!(state.right.batches.is_empty());
}

#[test]
fn append_normalization_borrows_plain_batches_and_owns_only_weight_strips() {
    let config = make_config();
    let plain_input = [left_batch(&["A"], &[100], &[1.0])];
    let plain = normalize_join_input(
        "left",
        &plain_input,
        &config.left_keys,
        &config.left_time_column,
        0,
        JoinExecutionMode::AppendOnly,
    )
    .unwrap();
    assert!(plain.row_weights.is_empty());
    assert!(matches!(&plain.batches, Cow::Borrowed(_)));

    let weighted_input = [weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])];
    let stripped = normalize_join_input(
        "left",
        &weighted_input,
        &config.left_keys,
        &config.left_time_column,
        0,
        JoinExecutionMode::AppendOnly,
    )
    .unwrap();
    assert!(stripped.row_weights.is_empty());
    assert!(matches!(&stripped.batches, Cow::Owned(_)));
    assert_eq!(stripped.batches[0].num_columns(), 3);
    assert_eq!(
        std::mem::size_of::<JoinOutputRow>(),
        std::mem::size_of::<(Option<(usize, usize)>, Option<(usize, usize)>,)>()
    );
}

#[test]
fn side_admission_requires_mode_exact_weight_rosters() {
    let batch = left_batch(&["A"], &[100], &[1.0]);
    let keys = ["id".to_string()];

    let mut append = SideState::new();
    append
        .add_batch(
            &batch,
            None,
            &keys,
            "ts",
            false,
            false,
            JoinExecutionMode::AppendOnly,
        )
        .unwrap();
    assert!(append.row_weights.is_empty());

    let append_error = SideState::new()
        .add_batch(
            &batch,
            Some(Arc::<[i64]>::from([1_i64])),
            &keys,
            "ts",
            false,
            false,
            JoinExecutionMode::AppendOnly,
        )
        .unwrap_err();
    assert!(append_error.to_string().contains("row-weight roster"));

    let weighted_error = SideState::new()
        .add_batch(
            &batch,
            None,
            &keys,
            "ts",
            false,
            false,
            JoinExecutionMode::Weighted,
        )
        .unwrap_err();
    assert!(weighted_error.to_string().contains("row-weight roster"));
}

#[test]
fn negative_weight_fails_before_state_mutation() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("price", DataType::Float64, false),
        Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ),
    ]));
    let retraction = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(TimestampMillisecondArray::from(vec![100])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Int64Array::from(vec![-1])),
        ],
    )
    .unwrap();
    let mut state = IntervalJoinState::new();
    let error = execute_interval_join_cycle(&mut state, &[retraction], &[], &make_config(), 0, 0)
        .unwrap_err();
    assert!(error.to_string().contains("requires +1 weights"));
    assert_eq!(state.left.row_count, 0);
}

#[test]
fn row_below_prior_input_watermark_is_rejected_without_retention() {
    let config = make_config();
    let mut state = IntervalJoinState::new();
    execute_interval_join_cycle(&mut state, &[], &[], &config, 300, 300).unwrap();
    assert_eq!(state.left_evicted_cutoff, 200);

    let error = execute_interval_join_cycle(
        &mut state,
        &[left_batch(&["late"], &[199], &[1.0])],
        &[],
        &config,
        300,
        300,
    )
    .unwrap_err();
    assert!(error.to_string().contains("below closed cutoff 300"));
    assert_eq!(state.left.row_count, 0);
    assert!(state.left.batches.is_empty());
}

#[test]
#[allow(clippy::cast_possible_wrap, clippy::cast_precision_loss)]
fn test_match_pairs_bounded_partial_emit_on_cross_product() {
    // Adversarial shape: every left × every right matches (single key,
    // wide bound, all timestamps within tolerance). The candidate buffer must
    // flush into batches no larger than EMIT_THRESHOLD.
    let config = StreamJoinConfig {
        join_type: JoinType::Inner,
        left_keys: vec!["id".to_string()],
        right_keys: vec!["id".to_string()],
        left_time_column: "ts".to_string(),
        right_time_column: "ts".to_string(),
        left_table: "left_stream".to_string(),
        right_table: "right_stream".to_string(),
        time_bound: Duration::from_millis(1_000_000),
    };
    let mut state = IntervalJoinState::new();

    // 300 × 300 pairs exceed the emit threshold, so output spans multiple batches.
    let m = 300usize;
    let ids_l: Vec<&str> = (0..m).map(|_| "K").collect();
    let ts_l = vec![0; m];
    let v_l: Vec<f64> = (0..m).map(|i| i as f64).collect();
    let left = left_batch(&ids_l, &ts_l, &v_l);

    let ids_r: Vec<&str> = (0..m).map(|_| "K").collect();
    let ts_r = vec![0; m];
    let v_r: Vec<f64> = (0..m).map(|i| i as f64).collect();
    let right = right_batch(&ids_r, &ts_r, &v_r);

    let result = execute_interval_join_cycle(&mut state, &[left], &[right], &config, 0, 0).unwrap();

    let total: usize = result.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, m * m, "every pair must appear exactly once");
    assert!(
        result.len() >= 2,
        "expected partial emits across multiple batches, got {}",
        result.len()
    );
    for b in &result {
        assert!(
            b.num_rows() <= EMIT_THRESHOLD,
            "partial batch exceeded EMIT_THRESHOLD: {}",
            b.num_rows()
        );
    }
}

#[test]
fn hot_key_output_budget_halts_before_unbounded_allocation() {
    let config = StreamJoinConfig {
        join_type: JoinType::Inner,
        left_keys: vec!["id".into()],
        right_keys: vec!["id".into()],
        left_time_column: "ts".into(),
        right_time_column: "ts".into(),
        left_table: "left_stream".into(),
        right_table: "right_stream".into(),
        time_bound: Duration::from_millis(1),
    };
    let rows = 513usize;
    let ids = vec!["K"; rows];
    let timestamps = vec![0; rows];
    let values = vec![1.0; rows];
    let mut state = IntervalJoinState::new();

    let error = execute_interval_join_cycle(
        &mut state,
        &[left_batch(&ids, &timestamps, &values)],
        &[right_batch(&ids, &timestamps, &values)],
        &config,
        0,
        0,
    )
    .unwrap_err();
    assert!(matches!(error, DbError::BackpressureFail(_)));
}

#[test]
fn output_budget_is_shared_across_shards() {
    let config = make_config();
    let mut output_budget = IntervalJoinOutputBudget {
        emitted_rows: MAX_CYCLE_OUTPUT_ROWS - 1,
        emitted_bytes: 0,
    };
    let mut first = IntervalJoinState::new();
    let output = super::execute_interval_join_cycle(
        &mut first,
        &[left_batch(&["A"], &[100], &[1.0])],
        &[right_batch(&["A"], &[100], &[1.0])],
        &config,
        0,
        0,
        0,
        0,
        usize::MAX,
        &mut output_budget,
    )
    .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let mut second = IntervalJoinState::new();
    let error = super::execute_interval_join_cycle(
        &mut second,
        &[left_batch(&["B"], &[100], &[1.0])],
        &[right_batch(&["B"], &[100], &[1.0])],
        &config,
        0,
        0,
        0,
        0,
        usize::MAX,
        &mut output_budget,
    )
    .unwrap_err();
    assert!(matches!(error, DbError::BackpressureFail(_)));
}

#[test]
fn weighted_deltas_cover_all_interval_join_kinds() {
    let cases = [
        (JoinType::Inner, vec![6, -6]),
        (JoinType::Left, vec![6, -6, 2]),
        (JoinType::Right, vec![6, -6]),
        (JoinType::Full, vec![6, -6, 2]),
        (JoinType::LeftSemi, vec![2, -2]),
        (JoinType::LeftAnti, vec![2]),
        (JoinType::RightSemi, vec![3, -3]),
        (JoinType::RightAnti, Vec::new()),
    ];

    for (join_type, expected) in cases {
        let mut config = make_config();
        config.join_type = join_type;
        let mut state = IntervalJoinState::new_weighted();
        let mut output = execute_weighted_cycle(
            &mut state,
            &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[2])],
            &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[3])],
            &config,
            0,
            0,
        )
        .unwrap();
        output.extend(
            execute_weighted_cycle(
                &mut state,
                &[],
                &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-3])],
                &config,
                0,
                0,
            )
            .unwrap(),
        );
        output.extend(execute_weighted_cycle(&mut state, &[], &[], &config, 1_000, 1_000).unwrap());
        assert_eq!(emitted_weights(&output), expected, "{join_type:?}");
    }
}

#[test]
fn execution_mode_is_bound_even_while_state_is_empty() {
    let config = make_config();
    let mut append_only = IntervalJoinState::new();
    let error = execute_weighted_cycle(&mut append_only, &[], &[], &config, 0, 0)
        .expect_err("append-only state must reject weighted execution");
    assert!(error.to_string().contains("execution mode changed"));
    assert_eq!(append_only.buffered_rows(), (0, 0));

    let mut weighted = IntervalJoinState::new_weighted();
    let error = execute_interval_join_cycle(&mut weighted, &[], &[], &config, 0, 0)
        .expect_err("weighted state must reject append-only execution");
    assert!(error.to_string().contains("execution mode changed"));
    assert_eq!(weighted.buffered_rows(), (0, 0));
}

#[test]
fn weighted_semi_tracks_full_support_and_canonical_right_delta_order() {
    let mut left_semi = make_config();
    left_semi.join_type = JoinType::LeftSemi;
    let mut state = IntervalJoinState::new_weighted();
    let first = execute_weighted_cycle(
        &mut state,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[5])],
        &[weighted_batch(
            right_batch(&["A", "A"], &[110, 110], &[10.0, 20.0]),
            &[2, 3],
        )],
        &left_semi,
        0,
        0,
    )
    .unwrap();
    assert_eq!(emitted_weights(&first), vec![5]);
    let still_matched = execute_weighted_cycle(
        &mut state,
        &[],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-2])],
        &left_semi,
        0,
        0,
    )
    .unwrap();
    assert!(still_matched.is_empty());
    let becomes_unmatched = execute_weighted_cycle(
        &mut state,
        &[],
        &[weighted_batch(right_batch(&["A"], &[110], &[20.0]), &[-3])],
        &left_semi,
        0,
        0,
    )
    .unwrap();
    assert_eq!(emitted_weights(&becomes_unmatched), vec![-5]);

    let mut right_semi = make_config();
    right_semi.join_type = JoinType::RightSemi;
    let mut ordered = IntervalJoinState::new_weighted();
    execute_weighted_cycle(
        &mut ordered,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
        &[],
        &right_semi,
        0,
        0,
    )
    .unwrap();
    let output = execute_weighted_cycle(
        &mut ordered,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[-1])],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
        &right_semi,
        0,
        0,
    )
    .unwrap();
    assert_eq!(emitted_weights(&output), vec![1, -1]);
}

#[test]
fn weighted_cross_term_is_emitted_once() {
    let config = make_config();
    let mut state = IntervalJoinState::new_weighted();
    execute_weighted_cycle(
        &mut state,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[2])],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[3])],
        &config,
        0,
        0,
    )
    .unwrap();
    let output = execute_weighted_cycle(
        &mut state,
        &[weighted_batch(left_batch(&["A"], &[100], &[2.0]), &[5])],
        &[weighted_batch(right_batch(&["A"], &[110], &[20.0]), &[7])],
        &config,
        0,
        0,
    )
    .unwrap();
    assert_eq!(emitted_weights(&output), vec![14, 15, 35]);
}

#[test]
fn weighted_checkpoint_is_shallow_and_restores_exact_support() {
    let mut config = make_config();
    config.join_type = JoinType::LeftSemi;
    let mut state = IntervalJoinState::new_weighted();
    execute_weighted_cycle(
        &mut state,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[4])],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[2])],
        &config,
        0,
        0,
    )
    .unwrap();
    let row_weights = Arc::clone(&state.left.row_weights[0]);
    let match_weights = Arc::clone(&state.left.match_weights[0]);
    let capture = state.capture_checkpoint(&config, usize::MAX).unwrap();
    assert!(Arc::ptr_eq(&row_weights, &capture.left_row_weights[0]));
    assert!(Arc::ptr_eq(&match_weights, &capture.left_match_weights[0]));

    let checkpoint = capture
        .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    assert!(checkpoint.weighted);
    let mode_error = IntervalJoinState::from_checkpoint(
        &checkpoint,
        &config,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
    )
    .err()
    .expect("weighted checkpoint must not restore into append-only state");
    assert!(mode_error.to_string().contains("execution mode"));
    let mut restored = IntervalJoinState::from_checkpoint_with_mode(
        &checkpoint,
        &config,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        JoinExecutionMode::Weighted,
    )
    .unwrap();
    let output = execute_weighted_cycle(
        &mut restored,
        &[],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-2])],
        &config,
        0,
        0,
    )
    .unwrap();
    assert_eq!(emitted_weights(&output), vec![-4]);

    execute_weighted_cycle(
        &mut state,
        &[],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-2])],
        &config,
        0,
        0,
    )
    .unwrap();
    assert!(!Arc::ptr_eq(&match_weights, &state.left.match_weights[0]));
    assert!(Arc::ptr_eq(&row_weights, &state.left.row_weights[0]));
}

#[test]
fn weighted_checkpoint_compaction_keeps_payload_and_weight_rosters_aligned() {
    let mut config = make_config();
    config.join_type = JoinType::LeftSemi;
    let mut state = IntervalJoinState::new_weighted();
    execute_weighted_cycle(
        &mut state,
        &[weighted_batch(
            left_batch_nullable(&[Some("A"), None], &[100, 200], &[1.0, 2.0]),
            &[7, -2],
        )],
        &[],
        &config,
        0,
        0,
    )
    .unwrap();
    assert!(!state.left.is_compact());

    let checkpoint = state
        .capture_checkpoint(&config, usize::MAX)
        .unwrap()
        .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    assert_eq!(checkpoint.left_row_weights, vec![vec![7]]);
    assert_eq!(checkpoint.left_match_weights, vec![vec![0]]);
    let decoded =
        laminar_core::serialization::deserialize_batch_stream(&checkpoint.left_batches[0]).unwrap();
    assert_eq!(decoded.num_rows(), 1);
    assert_eq!(
        decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "A"
    );
}

#[test]
fn weighted_checkpoint_keeps_support_after_opposite_eviction() {
    let mut config = make_config();
    config.join_type = JoinType::Left;
    let mut state = IntervalJoinState::new_weighted();
    execute_weighted_cycle(
        &mut state,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
        &config,
        0,
        0,
    )
    .unwrap();
    execute_weighted_cycle(&mut state, &[], &[], &config, 200, 150).unwrap();
    assert_eq!(state.buffered_rows(), (1, 0));

    let checkpoint = state
        .capture_checkpoint(&config, usize::MAX)
        .unwrap()
        .encode(crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap();
    let mut restored = IntervalJoinState::from_checkpoint_with_mode(
        &checkpoint,
        &config,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        JoinExecutionMode::Weighted,
    )
    .unwrap();
    let output = execute_weighted_cycle(&mut restored, &[], &[], &config, 200, 300).unwrap();
    assert!(
        output.is_empty(),
        "matched left row became falsely unmatched"
    );
}

#[test]
fn weighted_arithmetic_and_late_deltas_fail_closed() {
    let config = make_config();
    let mut overflow = IntervalJoinState::new_weighted();
    let error = execute_weighted_cycle(
        &mut overflow,
        &[weighted_batch(
            left_batch(&["A"], &[100], &[1.0]),
            &[i64::MAX],
        )],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[2])],
        &config,
        0,
        0,
    )
    .unwrap_err();
    assert!(error.requires_pipeline_halt());
    assert!(error.to_string().contains("multiplication overflow"));

    let mut left_semi = make_config();
    left_semi.join_type = JoinType::LeftSemi;
    let mut underflow = IntervalJoinState::new_weighted();
    execute_weighted_cycle(
        &mut underflow,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
        &[],
        &left_semi,
        0,
        0,
    )
    .unwrap();
    let error = execute_weighted_cycle(
        &mut underflow,
        &[],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-1])],
        &left_semi,
        0,
        0,
    )
    .unwrap_err();
    assert!(error.requires_pipeline_halt());
    assert!(error.to_string().contains("became negative"));
    assert_eq!(underflow.left.match_weights[0][0], 0);

    let mut support_overflow = IntervalJoinState::new_weighted();
    execute_weighted_cycle(
        &mut support_overflow,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
        &[weighted_batch(
            right_batch(&["A"], &[110], &[10.0]),
            &[i64::MAX],
        )],
        &left_semi,
        0,
        0,
    )
    .unwrap();
    let error = execute_weighted_cycle(
        &mut support_overflow,
        &[],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
        &left_semi,
        0,
        0,
    )
    .unwrap_err();
    assert!(error.requires_pipeline_halt());
    assert!(error.to_string().contains("match-support overflow"));
    assert_eq!(support_overflow.left.match_weights[0][0], i64::MAX);

    let mut negate_overflow = IntervalJoinState::new_weighted();
    execute_weighted_cycle(
        &mut negate_overflow,
        &[weighted_batch(
            left_batch(&["A"], &[100], &[1.0]),
            &[i64::MIN],
        )],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
        &left_semi,
        0,
        0,
    )
    .unwrap();
    let error = execute_weighted_cycle(
        &mut negate_overflow,
        &[],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[-1])],
        &left_semi,
        0,
        0,
    )
    .unwrap_err();
    assert!(error.requires_pipeline_halt());
    assert!(error.to_string().contains("semi-join retraction overflow"));
    assert_eq!(negate_overflow.left.match_weights[0][0], 1);

    let mut late = IntervalJoinState::new_weighted();
    let error = execute_interval_join_cycle_with_mode(
        &mut late,
        &[weighted_batch(left_batch(&["A"], &[99], &[1.0]), &[-1])],
        &[],
        &config,
        100,
        0,
        100,
        0,
        usize::MAX,
        &mut IntervalJoinOutputBudget::default(),
        JoinExecutionMode::Weighted,
    )
    .unwrap_err();
    assert!(error.to_string().contains("late row"));
    assert_eq!(late.buffered_rows(), (0, 0));
}

#[test]
fn weighted_output_budget_accounts_for_the_trailing_weight_before_build() {
    let config = make_config();
    let initial_bytes = MAX_CYCLE_OUTPUT_BYTES - 225;
    let mut append_only = IntervalJoinState::new();
    let mut append_budget = IntervalJoinOutputBudget {
        emitted_rows: 0,
        emitted_bytes: initial_bytes,
    };
    let append_result = super::execute_interval_join_cycle(
        &mut append_only,
        &[left_batch(&["A"], &[100], &[1.0])],
        &[right_batch(&["A"], &[110], &[10.0])],
        &config,
        0,
        0,
        0,
        0,
        usize::MAX,
        &mut append_budget,
    );
    match append_result {
        Ok(output) => assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1),
        Err(error) => {
            assert!(error.to_string().contains("exceeded"));
            assert!(!error.to_string().contains("would exceed"));
        }
    }

    let mut state = IntervalJoinState::new_weighted();
    let mut output_budget = IntervalJoinOutputBudget {
        emitted_rows: 0,
        emitted_bytes: initial_bytes,
    };
    let error = execute_interval_join_cycle_with_mode(
        &mut state,
        &[weighted_batch(left_batch(&["A"], &[100], &[1.0]), &[1])],
        &[weighted_batch(right_batch(&["A"], &[110], &[10.0]), &[1])],
        &config,
        0,
        0,
        0,
        0,
        usize::MAX,
        &mut output_budget,
        JoinExecutionMode::Weighted,
    )
    .unwrap_err();
    assert!(matches!(error, DbError::BackpressureFail(_)));
    assert!(error.to_string().contains("would exceed"));
    assert_eq!(output_budget.emitted_bytes, initial_bytes);
}

#[test]
fn weighted_restore_rejects_corrupt_support_before_ipc_decode() {
    let mut config = make_config();
    config.join_type = JoinType::LeftSemi;
    let checkpoint = JoinStateCheckpoint {
        weighted: true,
        join_type: join_type_tag(config.join_type),
        left_keys: config.left_keys.clone(),
        right_keys: config.right_keys.clone(),
        left_time_column: config.left_time_column.clone(),
        right_time_column: config.right_time_column.clone(),
        left_table: config.left_table.clone(),
        right_table: config.right_table.clone(),
        bound_ms: 100,
        left_buffer_rows: 1,
        right_buffer_rows: 0,
        left_batches: vec![vec![0xff]],
        right_batches: Vec::new(),
        left_evicted_cutoff: i64::MIN,
        right_evicted_cutoff: i64::MIN,
        left_row_weights: vec![vec![1]],
        right_row_weights: Vec::new(),
        left_match_flags: Vec::new(),
        right_match_flags: Vec::new(),
        left_match_weights: vec![vec![-1]],
        right_match_weights: Vec::new(),
    };
    let error = IntervalJoinState::from_checkpoint_with_mode(
        &checkpoint,
        &config,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        JoinExecutionMode::Weighted,
    )
    .err()
    .expect("corrupt weighted support must fail before IPC decode");
    assert!(error.to_string().contains("invalid match weights"));
    assert!(!error.to_string().contains("deserialization"));
}
