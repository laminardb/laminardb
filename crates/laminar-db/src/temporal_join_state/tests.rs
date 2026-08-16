use super::*;
use arrow::array::{
    ArrayAccessor, DictionaryArray, Int32Array, StringArray, TimestampMillisecondArray, UInt8Array,
};
use arrow::array::{StringViewArray, StringViewBuilder};
use arrow::datatypes::Int32Type;
use laminar_connectors::connector::{
    schema_with_source_mutations_and_row_positions, source_mutations,
};

fn schema(prefix: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new(format!("{prefix}_value"), DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(SOURCE_PARTITION_COLUMN, DataType::Binary, false),
        Field::new(SOURCE_ORDER_COLUMN, DataType::Binary, false),
        Field::new(SOURCE_SUB_OFFSET_COLUMN, DataType::UInt32, false),
    ]))
}

fn view_schema(prefix: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new(format!("{prefix}_value"), DataType::Utf8View, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(SOURCE_PARTITION_COLUMN, DataType::Binary, false),
        Field::new(SOURCE_ORDER_COLUMN, DataType::Binary, false),
        Field::new(SOURCE_SUB_OFFSET_COLUMN, DataType::UInt32, false),
    ]))
}

fn dictionary_schema(prefix: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new(
            format!("{prefix}_value"),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View)),
            true,
        ),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(SOURCE_PARTITION_COLUMN, DataType::Binary, false),
        Field::new(SOURCE_ORDER_COLUMN, DataType::Binary, false),
        Field::new(SOURCE_SUB_OFFSET_COLUMN, DataType::UInt32, false),
    ]))
}

fn batch(
    schema: SchemaRef,
    keys: Vec<Option<&str>>,
    values: Vec<Option<i64>>,
    times: Vec<Option<i64>>,
    orders: Vec<u8>,
) -> RecordBatch {
    let rows = keys.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(TimestampMillisecondArray::from(times)),
            Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
                b"p0".as_slice(),
                rows,
            ))),
            Arc::new(BinaryArray::from_iter_values(
                orders.into_iter().map(|order| vec![order]),
            )),
            Arc::new(UInt32Array::from_iter_values(
                0..u32::try_from(rows).unwrap(),
            )),
        ],
    )
    .unwrap()
}

fn view_batch(
    schema: SchemaRef,
    keys: Vec<Option<&str>>,
    values: Vec<Option<&str>>,
    times: Vec<Option<i64>>,
    orders: Vec<u8>,
) -> RecordBatch {
    let rows = keys.len();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(StringViewArray::from(values)),
            Arc::new(TimestampMillisecondArray::from(times)),
            Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
                b"p0".as_slice(),
                rows,
            ))),
            Arc::new(BinaryArray::from_iter_values(
                orders.into_iter().map(|order| vec![order]),
            )),
            Arc::new(UInt32Array::from_iter_values(
                0..u32::try_from(rows).unwrap(),
            )),
        ],
    )
    .unwrap()
}

fn dictionary_batch(schema: SchemaRef, value: &str) -> RecordBatch {
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0)]),
        Arc::new(StringViewArray::from(vec![Some(value)])),
    )
    .unwrap();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("A")])),
            Arc::new(dictionary),
            Arc::new(TimestampMillisecondArray::from(vec![Some(100)])),
            Arc::new(BinaryArray::from_iter_values([b"p0".as_slice()])),
            Arc::new(BinaryArray::from_iter_values([b"1".as_slice()])),
            Arc::new(UInt32Array::from(vec![0])),
        ],
    )
    .unwrap()
}

fn mutation_metadata(batch: &RecordBatch, operations: &[SourceMutation]) -> RecordBatch {
    let visible_columns = batch.num_columns() - POSITION_COLUMN_COUNT;
    let visible_schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields()[..visible_columns].to_vec(),
        batch.schema().metadata().clone(),
    ));
    let schema = schema_with_source_mutations_and_row_positions(&visible_schema).unwrap();
    let mut columns = batch.columns()[..visible_columns].to_vec();
    columns.push(Arc::new(UInt8Array::from_iter_values(
        operations.iter().map(|operation| match operation {
            SourceMutation::Put => 0,
            SourceMutation::Tombstone => 1,
        }),
    )));
    columns.extend_from_slice(&batch.columns()[visible_columns..]);
    RecordBatch::try_new(schema, columns).unwrap()
}

fn with_values(batch: &RecordBatch, values: Vec<Option<i64>>) -> RecordBatch {
    let mut columns = batch.columns().to_vec();
    columns[1] = Arc::new(Int64Array::from(values));
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

fn config(kind: TemporalJoinKind, schedule: TemporalProbeSchedule) -> TemporalJoinStateConfig {
    TemporalJoinStateConfig {
        vnode: 0,
        vnode_count: NonZeroU32::new(1).unwrap(),
        left_key_indices: vec![0],
        right_key_indices: vec![0],
        key_codec: Arc::new(PartitionKeyCodecV1::try_new([DataType::Utf8]).unwrap()),
        left_time_index: 2,
        right_time_index: 2,
        left_name: "trades".into(),
        right_name: "quotes".into(),
        operator_name: "trade_quote_asof".into(),
        join_kind: kind,
        emit_probe_metadata: schedule.is_multi_horizon(),
        schedule,
        left_allowed_lateness_ms: 0,
        right_allowed_lateness_ms: 0,
        history_retention_ms: 10_000,
        limits: TemporalStateLimits {
            max_retained_bytes: 4 * 1024 * 1024,
            max_pending_probes: 100,
            max_offsets_per_row: 16,
            max_horizon_ms: 60_000,
        },
    }
}

fn state(kind: TemporalJoinKind, schedule: TemporalProbeSchedule) -> TemporalJoinVnodeState {
    TemporalJoinVnodeState::try_new(schema("left"), schema("right"), config(kind, schedule))
        .unwrap()
}

fn prices(output: &RecordBatch) -> Vec<Option<i64>> {
    let index = output.schema().index_of("right_value_quotes").unwrap();
    output
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn predecessor_equal_time_position_and_tombstone_are_deterministic() {
    let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let right = batch(
        schema("right"),
        vec![Some("A"), Some("A"), Some("A")],
        vec![Some(10), Some(11), None],
        vec![Some(100), Some(100), Some(200)],
        vec![1, 2, 3],
    );
    let operations = [
        SourceMutation::Put,
        SourceMutation::Put,
        SourceMutation::Tombstone,
    ];
    let metadata = mutation_metadata(&right, &operations);
    let operations = source_mutations(&metadata).unwrap();
    assert_eq!(
        state
            .apply_right_batch(&right, operations)
            .unwrap()
            .inserted,
        3
    );
    assert_eq!(
        state
            .apply_right_batch(&right, operations)
            .unwrap()
            .duplicates,
        3
    );
    state.advance_right_frontier(Some(1_000), false).unwrap();
    assert_eq!(
        state
            .apply_right_batch(&right, operations)
            .unwrap()
            .duplicates,
        3
    );
    let changed = batch(
        schema("right"),
        vec![Some("A"), Some("A"), Some("A")],
        vec![Some(10), Some(11), Some(99)],
        vec![Some(100), Some(100), Some(200)],
        vec![1, 2, 3],
    );
    assert!(state
        .apply_right_batch(&changed, operations)
        .unwrap_err()
        .to_string()
        .contains("replayed with different temporal data"));
    let left = batch(
        schema("left"),
        vec![Some("A"), Some("A"), None, Some("A")],
        vec![Some(1); 4],
        vec![Some(100), Some(150), Some(150), Some(250)],
        vec![10, 11, 12, 13],
    );
    let output = state.probe_left_batch(&left).unwrap();
    assert_eq!(prices(&output), vec![Some(11), Some(11), None, None]);
}

#[test]
fn compact_replay_frontiers_plateau_with_lifetime_records() {
    let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let mut plateau = None;
    for order in 1..=64_u8 {
        let right = batch(
            schema("right"),
            vec![None],
            vec![Some(1)],
            vec![Some(100)],
            vec![order],
        );
        assert_eq!(
            state.apply_right_batch(&right, None).unwrap().ignored_nulls,
            1
        );
        let left = batch(
            schema("left"),
            vec![None],
            vec![Some(1)],
            vec![Some(100)],
            vec![order],
        );
        assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 1);
        let charged = state.accounted_state_bytes();
        if let Some(expected) = plateau {
            assert_eq!(charged, expected);
        } else {
            plateau = Some(charged);
        }
    }
    assert_eq!(state.right_replay_frontiers.len(), 1);
    assert_eq!(state.left_replay_frontiers.len(), 1);
    assert_eq!(state.retained_versions(), 0);
    assert_eq!(state.pending_probes(), 0);
    assert_eq!(
        state.accounted_state_bytes(),
        calculate_charge(&state).unwrap()
    );
}

#[test]
fn ipc_compaction_does_not_multiply_shared_buffer_accounting() {
    let input = batch(
        schema("right"),
        vec![Some("A"), Some("B"), Some("C")],
        vec![Some(10), Some(20), Some(30)],
        vec![Some(100), Some(200), Some(300)],
        vec![1, 2, 3],
    );
    let encoded = serialize_batches_stream_bounded(
        input.schema().as_ref(),
        std::iter::once(&input),
        4 * 1024 * 1024,
    )
    .unwrap();
    let restored = deserialize_batch_stream(&encoded).unwrap();

    // IPC columns share one record-body allocation, so Arrow's generic memory-size helper
    // counts substantially more than the allocator actually retains for this batch.
    assert!(restored.get_array_memory_size() > input.get_array_memory_size());
    let live_charge = batch_charge(&input).unwrap();
    let restored_charge = batch_charge(&restored).unwrap();
    assert!(
            restored_charge <= live_charge,
            "IPC restore increased canonical temporal batch charge from {live_charge} to {restored_charge}"
        );
}

#[test]
fn ipc_padding_charge_is_stable_for_many_narrow_columns() {
    let fields = (0..64)
        .map(|column| Field::new(format!("c{column}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        (0..64)
            .map(|value| Arc::new(Int64Array::from(vec![value])) as ArrayRef)
            .collect(),
    )
    .unwrap();
    let encoded = serialize_batches_stream_bounded(
        input.schema().as_ref(),
        std::iter::once(&input),
        4 * 1024 * 1024,
    )
    .unwrap();
    let restored = deserialize_batch_stream(&encoded).unwrap();

    assert!(restored.get_array_memory_size() > input.get_array_memory_size());
    assert!(batch_charge(&restored).unwrap() <= batch_charge(&input).unwrap());
}

#[test]
fn detached_view_compaction_preserves_deduplicated_backing_charge() {
    let rows = 8;
    let payload = "x".repeat(128 * 1024);
    let mut values = StringViewBuilder::with_capacity(rows)
        .with_fixed_block_size(u32::try_from(payload.len()).unwrap())
        .with_deduplicate_strings();
    for _ in 0..rows {
        values.append_value(&payload);
    }
    let values = values.finish();
    assert_eq!(values.data_buffers().len(), 1);
    assert!(values.views().windows(2).all(|views| views[0] == views[1]));
    let left_schema = view_schema("left");
    let left = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(StringArray::from(vec![Some("A"); rows])),
            Arc::new(values),
            Arc::new(TimestampMillisecondArray::from(vec![Some(100); rows])),
            Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
                b"p0".as_slice(),
                rows,
            ))),
            Arc::new(BinaryArray::from_iter_values(
                (1..=u8::try_from(rows).unwrap()).map(|order| vec![order]),
            )),
            Arc::new(UInt32Array::from_iter_values(
                0..u32::try_from(rows).unwrap(),
            )),
        ],
    )
    .unwrap();
    let mut cfg = config(
        TemporalJoinKind::Left,
        TemporalProbeSchedule::list((1..=16).map(i64::from).collect()).unwrap(),
    );
    cfg.limits.max_pending_probes = 256;
    let mut state =
        TemporalJoinVnodeState::try_new(Arc::clone(&left_schema), schema("right"), cfg.clone())
            .unwrap();
    assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
    assert_eq!(state.pending_probes(), rows * 16);
    let live_limit = state.accounted_state_bytes();
    let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();

    cfg.limits.max_retained_bytes = live_limit;
    let restored =
        TemporalJoinVnodeState::restore(left_schema, schema("right"), cfg, &checkpoint).unwrap();
    assert!(restored.accounted_state_bytes() <= live_limit);
    let retained = restored.left_batches.values().next().unwrap();
    assert_eq!(retained.batch.num_rows(), rows);
    assert_eq!(retained.references, rows * 16);
    let restored_values = retained
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(restored_values.data_buffers().len(), 1);
    assert!(restored_values
        .iter()
        .all(|value| value == Some(payload.as_str())));
}

#[test]
fn checkpoint_compaction_restores_within_the_live_state_limit() {
    let mut cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let mut state =
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
    let keys = (0..200).map(|key| format!("K{key:03}")).collect::<Vec<_>>();
    let key_refs = keys
        .iter()
        .map(|key| Some(key.as_str()))
        .collect::<Vec<_>>();
    let input = batch(
        schema("right"),
        key_refs,
        (0..200).map(|value| Some(i64::from(value))).collect(),
        (0..200).map(|time| Some(1_000 + i64::from(time))).collect(),
        (0_u8..200).collect(),
    );
    state.apply_right_batch(&input, None).unwrap();
    let live_limit = state.accounted_state_bytes();
    let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();

    cfg.limits.max_retained_bytes = live_limit;
    let restored =
        TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint).unwrap();
    assert!(restored.accounted_state_bytes() <= live_limit);
    assert_eq!(restored.retained_versions(), state.retained_versions());
}

#[test]
fn temporal_checkpoint_admission_rejects_run_end_encoded_fields() {
    let mut fields = schema("left").fields().to_vec();
    fields[1] = Arc::new(Field::new(
        "left_value",
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ),
        true,
    ));
    let error = TemporalJoinVnodeState::try_new(
        Arc::new(Schema::new(fields)),
        schema("right"),
        config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of()),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("run-end encoded"));
}

#[test]
fn legacy_list_probe_checkpoint_canonicalizes_view_backing_and_one_left_row() {
    let offsets = (1..=32).map(i64::from).collect::<Vec<_>>();
    let mut cfg = config(
        TemporalJoinKind::Left,
        TemporalProbeSchedule::list(offsets).unwrap(),
    );
    cfg.limits.max_offsets_per_row = 64;
    let left_schema = view_schema("left");
    let mut state =
        TemporalJoinVnodeState::try_new(Arc::clone(&left_schema), schema("right"), cfg.clone())
            .unwrap();
    let left = view_batch(
        Arc::clone(&left_schema),
        vec![Some("A")],
        vec![Some(
            "a deliberately long payload retained by an Arrow string view",
        )],
        vec![Some(100)],
        vec![1],
    );
    assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
    assert_eq!(state.pending_probes(), 32);
    let live_limit = state.accounted_state_bytes();
    let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();

    let mut decoded =
        rkyv::from_bytes::<TemporalJoinCheckpoint, rkyv::rancor::Error>(&checkpoint).unwrap();
    assert_eq!(decoded.pending.len(), 32);
    assert_eq!(
        decoded
            .pending
            .iter()
            .map(|probe| probe.left_row)
            .collect::<Vec<_>>(),
        (0..32).collect::<Vec<_>>()
    );
    assert_eq!(
        deserialize_batch_stream(&decoded.left_rows_ipc)
            .unwrap()
            .num_rows(),
        32
    );
    decoded.pending[1].left_row = 0;
    let writer = rkyv::ser::writer::IoWriter::new(
        laminar_core::serialization::BoundedBytesWriter::new(4 * 1024 * 1024),
    );
    let corrupt = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&decoded, writer)
        .unwrap()
        .into_inner()
        .into_vec();
    assert!(TemporalJoinVnodeState::restore(
        Arc::clone(&left_schema),
        schema("right"),
        cfg.clone(),
        &corrupt,
    )
    .err()
    .unwrap()
    .to_string()
    .contains("row references are invalid"));

    cfg.limits.max_retained_bytes = live_limit;
    let mut restored = TemporalJoinVnodeState::restore(
        Arc::clone(&left_schema),
        schema("right"),
        cfg,
        &checkpoint,
    )
    .unwrap();
    assert_eq!(restored.pending_probes(), 32);
    assert!(restored.accounted_state_bytes() <= live_limit);
    let retained = restored.left_batches.values().next().unwrap();
    assert_eq!(retained.batch.num_rows(), 1);
    assert_eq!(retained.references, 32);
    let restored_view = retained
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(
        restored_view.value(0),
        "a deliberately long payload retained by an Arrow string view"
    );
    assert_eq!(restored_view.data_buffers().len(), 1);
    restored.advance_right_frontier(Some(102), false).unwrap();
    let first = restored
        .drain_ready_probes(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(first.drained_probes, 1);
    let retained = restored.left_batches.values().next().unwrap();
    assert_eq!(retained.batch.num_rows(), 1);
    assert_eq!(retained.references, 31);
    restored.advance_right_frontier(Some(1_000), false).unwrap();
    let drained = restored
        .drain_ready_probes(NonZeroUsize::new(64).unwrap())
        .unwrap();
    assert_eq!(drained.drained_probes, 31);
    assert_eq!(drained.output.num_rows(), 31);
    assert_eq!(restored.pending_probes(), 0);
    assert!(restored.left_batches.is_empty());
    assert_eq!(
        restored.accounted_state_bytes(),
        calculate_charge(&restored).unwrap()
    );
}

#[test]
fn legacy_list_probe_checkpoint_detaches_dictionary_backing() {
    let schedule = TemporalProbeSchedule::list((1..=32).map(i64::from).collect()).unwrap();
    let mut cfg = config(TemporalJoinKind::Left, schedule);
    cfg.limits.max_offsets_per_row = 64;
    let left_schema = dictionary_schema("left");
    let mut state =
        TemporalJoinVnodeState::try_new(Arc::clone(&left_schema), schema("right"), cfg.clone())
            .unwrap();
    // An inline View makes Arrow's `gc` take its zero-buffer fast path. The recursive copier
    // must still detach its views/null allocation from the expanded decoded IPC body.
    let value = "inline";
    let left = dictionary_batch(Arc::clone(&left_schema), value);
    assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
    assert_eq!(state.pending_probes(), 32);
    let live_limit = state.accounted_state_bytes();
    let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();

    cfg.limits.max_retained_bytes = live_limit;
    let restored =
        TemporalJoinVnodeState::restore(left_schema, schema("right"), cfg, &checkpoint).unwrap();
    assert!(restored.accounted_state_bytes() <= live_limit);
    let retained = restored.left_batches.values().next().unwrap();
    assert_eq!(retained.batch.num_rows(), 1);
    assert_eq!(retained.references, 32);
    let dictionary = retained
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap()
        .downcast_dict::<StringViewArray>()
        .unwrap();
    assert_eq!(dictionary.value(0), value);
}

#[test]
fn replay_frontier_validates_current_and_skips_older_cursor() {
    let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let right = batch(
        schema("right"),
        vec![Some("A"), Some("A")],
        vec![Some(1), Some(2)],
        vec![Some(10), Some(20)],
        vec![1, 2],
    );
    state.apply_right_batch(&right, None).unwrap();
    let older_changed = with_values(&right.slice(0, 1), vec![Some(99)]);
    assert_eq!(
        state
            .apply_right_batch(&older_changed, None)
            .unwrap()
            .duplicates,
        1
    );
    let current = right.slice(1, 1);
    assert_eq!(
        state.apply_right_batch(&current, None).unwrap().duplicates,
        1
    );
    let current_changed = with_values(&current, vec![Some(99)]);
    assert!(state
        .apply_right_batch(&current_changed, None)
        .unwrap_err()
        .to_string()
        .contains("replayed with different temporal data"));

    state.advance_right_frontier(Some(1_000), false).unwrap();
    let left = batch(
        schema("left"),
        vec![Some("A"), Some("A")],
        vec![Some(1), Some(2)],
        vec![Some(10), Some(20)],
        vec![3, 4],
    );
    assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 2);
    assert_eq!(
        state
            .probe_left_batch(&with_values(&left.slice(0, 1), vec![Some(99)]))
            .unwrap()
            .num_rows(),
        0
    );
    let current = left.slice(1, 1);
    assert_eq!(state.probe_left_batch(&current).unwrap().num_rows(), 0);
    assert!(state
        .probe_left_batch(&with_values(&current, vec![Some(99)]))
        .unwrap_err()
        .to_string()
        .contains("replayed with different temporal data"));
}

#[test]
fn older_left_replay_validates_retained_pending_horizons() {
    let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let left = batch(
        schema("left"),
        vec![Some("A"), Some("A")],
        vec![Some(1), Some(2)],
        vec![Some(10), Some(20)],
        vec![1, 2],
    );
    assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
    assert_eq!(state.pending_probes(), 2);
    assert_eq!(
        state
            .probe_left_batch(&left.slice(0, 1))
            .unwrap()
            .num_rows(),
        0
    );
    assert!(state
        .probe_left_batch(&with_values(&left.slice(0, 1), vec![Some(99)]))
        .unwrap_err()
        .to_string()
        .contains("disagrees with its pending temporal probes"));
    assert_eq!(state.pending_probes(), 2);
}

#[test]
fn source_order_regression_within_batch_is_atomic() {
    let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let right = batch(
        schema("right"),
        vec![Some("A"), Some("A")],
        vec![Some(1), Some(2)],
        vec![Some(10), Some(20)],
        vec![2, 1],
    );
    assert!(state
        .apply_right_batch(&right, None)
        .unwrap_err()
        .to_string()
        .contains("regressed within one input batch"));
    assert!(state.right_replay_frontiers.is_empty());
    assert_eq!(state.retained_versions(), 0);
    assert_eq!(state.accounted_state_bytes(), BASE_STATE_CHARGE);
}

#[test]
fn idle_left_uses_finite_retention_and_rejects_old_revival_probe() {
    let mut cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    cfg.history_retention_ms = 50;
    let mut state = TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg).unwrap();
    let right = batch(
        schema("right"),
        vec![Some("A"), Some("A"), Some("A")],
        vec![Some(1), Some(2), Some(3)],
        vec![Some(10), Some(20), Some(100)],
        vec![1, 2, 3],
    );
    state.apply_right_batch(&right, None).unwrap();
    state.advance_left_frontier(Some(20), true).unwrap();
    state.advance_right_frontier(Some(200), false).unwrap();
    assert_eq!(state.history_evicted_before, Some(150));
    while state.has_history_gc_work() {
        state
            .drain_history_gc(NonZeroUsize::new(16).unwrap())
            .unwrap();
    }
    assert_eq!(state.retained_versions(), 1);

    let revival = batch(
        schema("left"),
        vec![Some("A")],
        vec![Some(1)],
        vec![Some(140)],
        vec![10],
    );
    assert!(state
        .probe_left_batch(&revival)
        .unwrap_err()
        .to_string()
        .contains("older than retained history"));
    assert!(state.left_replay_frontiers.is_empty());
}

#[test]
fn inner_join_omits_nulls_and_missing_versions() {
    let mut state = state(TemporalJoinKind::Inner, TemporalProbeSchedule::as_of());
    state.advance_right_frontier(Some(1_000), false).unwrap();
    let left = batch(
        schema("left"),
        vec![Some("missing"), None],
        vec![Some(1), Some(2)],
        vec![Some(100), Some(100)],
        vec![1, 2],
    );
    assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
}

#[test]
fn multi_horizon_null_key_keeps_probe_time_without_hidden_positions() {
    let mut state = state(
        TemporalJoinKind::Left,
        TemporalProbeSchedule::list(vec![0, 5]).unwrap(),
    );
    let left = batch(
        schema("left"),
        vec![None],
        vec![Some(1)],
        vec![Some(100)],
        vec![1],
    );
    let output = state.probe_left_batch(&left).unwrap();
    let probe_times = output
        .column_by_name("probe_time")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(probe_times, vec![Some(100), Some(105)]);
    assert!(output.column_by_name(SOURCE_PARTITION_COLUMN).is_none());
    assert!(output.column_by_name(SOURCE_ORDER_COLUMN).is_none());
    assert!(output.column_by_name(SOURCE_SUB_OFFSET_COLUMN).is_none());
}

#[test]
fn sub_millisecond_timestamps_are_rejected_without_rounding() {
    let aligned = TimestampMicrosecondArray::from(vec![2_000, -2_000]);
    let view = TimestampMillisView::try_new(&aligned, "left").unwrap();
    assert_eq!(view.value(0, "left").unwrap(), 2);
    assert_eq!(view.value(1, "left").unwrap(), -2);

    for value in [1_500, -1_500] {
        let timestamps = TimestampMicrosecondArray::from(vec![value]);
        let view = TimestampMillisView::try_new(&timestamps, "left").unwrap();
        assert!(view
            .value(0, "left")
            .unwrap_err()
            .to_string()
            .contains("not exactly representable in milliseconds"));
    }
}

#[test]
fn list_and_range_share_state_and_timestamp_addition_is_checked() {
    let limits = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of()).limits;
    assert_eq!(
        expand_offsets(
            &TemporalProbeSchedule::list(vec![5, 15, -5]).unwrap(),
            limits
        )
        .unwrap(),
        vec![5, 15, -5]
    );
    assert_eq!(
        expand_offsets(&TemporalProbeSchedule::range(-5, 5, 5).unwrap(), limits).unwrap(),
        vec![-5, 0, 5]
    );
    let mut overflow_state = state(
        TemporalJoinKind::Left,
        TemporalProbeSchedule::list(vec![1]).unwrap(),
    );
    let left = batch(
        schema("left"),
        vec![Some("A")],
        vec![Some(1)],
        vec![Some(i64::MAX)],
        vec![1],
    );
    assert!(overflow_state
        .probe_left_batch(&left)
        .unwrap_err()
        .to_string()
        .contains("overflowed"));

    let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let left = batch(
        schema("left"),
        vec![Some("A")],
        vec![Some(1)],
        vec![Some(i64::MAX)],
        vec![2],
    );
    assert!(state
        .probe_left_batch(&left)
        .unwrap_err()
        .to_string()
        .contains("finite frontier"));

    let mut fields = schema("left").fields().to_vec();
    fields[2] = Arc::new(Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    ));
    assert!(TemporalJoinVnodeState::try_new(
        Arc::new(Schema::new(fields)),
        schema("right"),
        config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of()),
    )
    .err()
    .unwrap()
    .to_string()
    .contains("non-null timestamp"));

    let mut invalid = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    invalid.right_allowed_lateness_ms = 101;
    invalid.history_retention_ms = 100;
    assert!(
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), invalid)
            .err()
            .unwrap()
            .to_string()
            .contains("retention must cover")
    );

    let schedule = TemporalProbeSchedule::list(vec![0, 1]).unwrap();
    let mut fields = schema("left").fields().to_vec();
    fields[0] = Arc::new(Field::new("OFFSET_MS", DataType::Utf8, true));
    assert!(TemporalJoinVnodeState::try_new(
        Arc::new(Schema::new(fields)),
        schema("right"),
        config(TemporalJoinKind::Left, schedule),
    )
    .err()
    .unwrap()
    .to_string()
    .contains("output column name collision"));

    let mut below_base = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    below_base.limits.max_retained_bytes = BASE_STATE_CHARGE - 1;
    assert!(matches!(
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), below_base),
        Err(DbError::ManagedStateBudgetExceeded { .. })
    ));
}

#[test]
fn right_frontier_finalizes_buffered_probe() {
    let mut state = state(
        TemporalJoinKind::Left,
        TemporalProbeSchedule::list(vec![50]).unwrap(),
    );
    let right = batch(
        schema("right"),
        vec![Some("A")],
        vec![Some(7)],
        vec![Some(120)],
        vec![1],
    );
    state.apply_right_batch(&right, None).unwrap();
    let left = batch(
        schema("left"),
        vec![Some("A"), Some("A")],
        vec![Some(1), Some(2)],
        vec![Some(100), Some(100)],
        vec![2, 3],
    );
    assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
    assert_eq!(state.pending_probes(), 2);
    assert_eq!(state.pending_watermark_hold(), Some(100));
    state.advance_right_frontier(Some(149), false).unwrap();
    assert!(!state.has_ready_probes());
    state.advance_right_frontier(Some(150), false).unwrap();
    assert!(!state.has_ready_probes());
    state.advance_right_frontier(Some(151), false).unwrap();
    assert!(state.has_ready_probes());
    let drained = state
        .drain_ready_probes(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(prices(&drained.output), vec![Some(7)]);
    assert_eq!(drained.drained_probes, 1);
    assert!(drained.has_more);
    assert_eq!(
        state.accounted_state_bytes(),
        calculate_charge(&state).unwrap()
    );
    let drained = state
        .drain_ready_probes(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(prices(&drained.output), vec![Some(7)]);
    assert_eq!(drained.drained_probes, 1);
    assert!(!drained.has_more);
    assert_eq!(state.pending_probes(), 0);
    assert_eq!(state.pending_watermark_hold(), None);
    assert_eq!(
        state.accounted_state_bytes(),
        calculate_charge(&state).unwrap()
    );
}

#[test]
fn retention_preserves_one_predecessor_anchor() {
    let mut cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    cfg.history_retention_ms = 50;
    let mut state =
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
    let right = batch(
        schema("right"),
        vec![Some("A"), Some("A"), Some("A"), Some("B"), Some("B")],
        vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
        vec![Some(10), Some(20), Some(100), Some(10), Some(20)],
        vec![1, 2, 3, 4, 5],
    );
    state.apply_right_batch(&right, None).unwrap();
    state.advance_right_frontier(Some(120), false).unwrap();
    assert_eq!(state.retained_versions(), 5);
    state.advance_left_frontier(Some(120), false).unwrap();
    assert_eq!(state.retained_versions(), 5);
    assert!(state.has_history_gc_work());
    let drained = state
        .drain_history_gc(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(drained.steps, 1);
    assert_eq!(drained.removed_versions, 1);
    assert!(drained.has_more);
    assert_eq!(state.retained_versions(), 4);
    assert_eq!(
        state.accounted_state_bytes(),
        calculate_charge(&state).unwrap()
    );

    let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();
    let mut corrupted =
        rkyv::from_bytes::<TemporalJoinCheckpoint, rkyv::rancor::Error>(&checkpoint).unwrap();
    corrupted.history_gc_sweep_end = 1;
    let writer = rkyv::ser::writer::IoWriter::new(
        laminar_core::serialization::BoundedBytesWriter::new(4 * 1024 * 1024),
    );
    let corrupted = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&corrupted, writer)
        .unwrap()
        .into_inner()
        .into_vec();
    assert!(TemporalJoinVnodeState::restore(
        schema("left"),
        schema("right"),
        cfg.clone(),
        &corrupted,
    )
    .err()
    .unwrap()
    .to_string()
    .contains("appended after the active GC snapshot"));

    let mut restored =
        TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint).unwrap();
    assert!(restored.has_history_gc_work());
    let drained = restored
        .drain_history_gc(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(drained.steps, 1);
    assert_eq!(drained.removed_versions, 0);
    assert!(drained.has_more);
    let drained = restored
        .drain_history_gc(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(drained.removed_versions, 1);
    assert!(drained.has_more);
    let drained = restored
        .drain_history_gc(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(drained.removed_versions, 0);
    assert!(!drained.has_more);
    assert_eq!(restored.retained_versions(), 3);
}

#[test]
fn checkpoint_restores_compact_replay_frontier_across_history_gc() {
    let mut cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    cfg.history_retention_ms = 50;
    let mut state =
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
    let right = batch(
        schema("right"),
        vec![Some("A"), Some("A")],
        vec![Some(1), Some(2)],
        vec![Some(10), Some(100)],
        vec![1, 2],
    );
    state.apply_right_batch(&right, None).unwrap();
    state.advance_right_frontier(Some(200), false).unwrap();
    let left = batch(
        schema("left"),
        vec![Some("A")],
        vec![Some(1)],
        vec![Some(100)],
        vec![3],
    );
    assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 1);
    state.advance_left_frontier(Some(200), false).unwrap();
    assert_eq!(state.retained_versions(), 2);
    assert!(state.has_history_gc_work());

    let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();
    let mut restored =
        TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint).unwrap();
    assert_eq!(restored.probe_left_batch(&left).unwrap().num_rows(), 0);
    assert_eq!(restored.left_replay_frontiers.len(), 1);
    assert_eq!(restored.retained_versions(), 2);
    assert!(restored.has_history_gc_work());
    let first = restored
        .drain_history_gc(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(first.removed_versions, 1);
    assert!(first.has_more);
    let second = restored
        .drain_history_gc(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(second.removed_versions, 0);
    assert!(!second.has_more);
    assert_eq!(restored.retained_versions(), 1);
}

#[test]
fn pending_key_bytes_are_rejected_before_state_mutation() {
    let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let left = batch(
        schema("left"),
        vec![Some(&"x".repeat(4_096))],
        vec![Some(1)],
        vec![Some(100)],
        vec![1],
    );
    let encoded = state.encode_keys(&left, true).unwrap();
    let old_growth =
        VERSION_ENTRY_CHARGE + TIMER_ENTRY_CHARGE + left.get_array_memory_size() + BATCH_CHARGE + 6;
    state.config.limits.max_retained_bytes = state.accounted_state_bytes() + old_growth;
    let before = state.accounted_state_bytes();

    assert!(matches!(
        state.probe_left_batch(&left),
        Err(DbError::ManagedStateBudgetExceeded { .. })
    ));
    assert!(!encoded.row(0).as_ref().is_empty());
    assert_eq!(state.pending_probes(), 0);
    assert!(state.left_replay_frontiers.is_empty());
    assert!(state.timers.is_empty());
    assert_eq!(state.accounted_state_bytes(), before);
}

#[test]
fn restore_rejects_pending_probe_ahead_of_left_replay_frontier() {
    let cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    let mut state =
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
    let left = batch(
        schema("left"),
        vec![Some("A")],
        vec![Some(1)],
        vec![Some(100)],
        vec![1],
    );
    state.probe_left_batch(&left).unwrap();
    let bytes = state.checkpoint(4 * 1024 * 1024).unwrap();
    let mut decoded =
        rkyv::from_bytes::<TemporalJoinCheckpoint, rkyv::rancor::Error>(&bytes).unwrap();
    decoded.left_replay_frontiers[0].order.clear();
    let writer = rkyv::ser::writer::IoWriter::new(
        laminar_core::serialization::BoundedBytesWriter::new(4 * 1024 * 1024),
    );
    let corrupted = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&decoded, writer)
        .unwrap()
        .into_inner()
        .into_vec();

    assert!(
        TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &corrupted,)
            .err()
            .unwrap()
            .to_string()
            .contains("pending probe is ahead of its left replay frontier")
    );
}

#[test]
fn checkpoint_roundtrip_restores_history_pending_timer_and_frontiers() {
    let cfg = config(
        TemporalJoinKind::Left,
        TemporalProbeSchedule::list(vec![50]).unwrap(),
    );
    let mut state =
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
    let right = batch(
        schema("right"),
        vec![Some("A")],
        vec![Some(9)],
        vec![Some(120)],
        vec![1],
    );
    state.apply_right_batch(&right, None).unwrap();
    let left = batch(
        schema("left"),
        vec![Some("A")],
        vec![Some(1)],
        vec![Some(100)],
        vec![2],
    );
    state.probe_left_batch(&left).unwrap();
    state.advance_left_frontier(Some(90), true).unwrap();
    state.advance_right_frontier(Some(151), false).unwrap();
    assert!(state.has_ready_probes());
    assert!(state
        .capture_checkpoint(state.accounted_state_bytes())
        .is_err());
    let capture = state.capture_checkpoint(usize::MAX).unwrap();
    fn assert_send_static<T: Send + 'static>(_: &T) {}
    assert_send_static(&capture);
    assert!(capture.retained_bytes() >= state.accounted_state_bytes());
    state
        .drain_ready_probes(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(state.pending_probes(), 0);
    let post_cut = batch(
        schema("right"),
        vec![Some("A")],
        vec![Some(10)],
        vec![Some(160)],
        vec![3],
    );
    state.apply_right_batch(&post_cut, None).unwrap();
    assert_eq!(state.retained_versions(), 2);
    let checkpoint = capture.encode(4 * 1024 * 1024, None).unwrap();
    let mut restored =
        TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint).unwrap();
    assert_eq!(restored.retained_versions(), 1);
    assert_eq!(restored.right_replay_frontiers.len(), 1);
    assert_eq!(restored.left_replay_frontiers.len(), 1);
    assert_eq!(restored.pending_probes(), 1);
    assert_eq!(restored.pending_watermark_hold(), Some(100));
    assert!(restored.has_ready_probes());
    assert_eq!(
        restored.apply_right_batch(&right, None).unwrap().duplicates,
        1
    );
    assert_eq!(restored.probe_left_batch(&left).unwrap().num_rows(), 0);
    assert_eq!(restored.pending_probes(), 1);
    let drained = restored
        .drain_ready_probes(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert_eq!(prices(&drained.output), vec![Some(9)]);
}

#[test]
fn deferred_checkpoint_preserves_history_removed_by_post_cut_gc() {
    let mut cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
    cfg.history_retention_ms = 50;
    let mut state =
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
    let right = batch(
        schema("right"),
        vec![Some("A"), Some("A"), Some("A")],
        vec![Some(1), Some(2), Some(3)],
        vec![Some(10), Some(20), Some(100)],
        vec![1, 2, 3],
    );
    state.apply_right_batch(&right, None).unwrap();
    let capture = state.capture_checkpoint(usize::MAX).unwrap();

    state.advance_right_frontier(Some(200), false).unwrap();
    state.advance_left_frontier(Some(200), false).unwrap();
    let drained = state
        .drain_history_gc(NonZeroUsize::new(16).unwrap())
        .unwrap();
    assert_eq!(drained.removed_versions, 2);
    assert_eq!(state.retained_versions(), 1);

    let checkpoint = capture.encode(4 * 1024 * 1024, None).unwrap();
    let restored =
        TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint).unwrap();
    assert_eq!(restored.retained_versions(), 3);
}
