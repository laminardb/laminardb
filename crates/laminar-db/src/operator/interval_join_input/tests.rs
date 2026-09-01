use super::*;
use arrow::array::{BinaryArray, StringArray, TimestampMillisecondArray, UInt32Array};
use arrow::datatypes::{TimeUnit, UnionFields, UnionMode};
use laminar_connectors::connector::{
    schema_with_source_mutations_and_row_positions, schema_with_source_row_positions, SourceBatch,
    SourceRowPositionCapability, SourceRowPositions,
};

fn plain_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn plain_batch(ids: &[Option<&str>], times: &[i64], values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        plain_schema(),
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(TimestampMillisecondArray::from(times.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn expansion_prone_schemas_are_rejected_before_state_or_restore() {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let rejected = [
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::clone(&item),
        ),
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::List(Arc::clone(&item)),
        DataType::LargeList(Arc::clone(&item)),
        DataType::ListView(Arc::clone(&item)),
        DataType::LargeListView(Arc::clone(&item)),
        DataType::FixedSizeList(Arc::clone(&item), 2),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        DataType::Struct(vec![Field::new("item", DataType::Int64, true)].into()),
        DataType::Union(
            UnionFields::try_new([0], [Field::new("item", DataType::Int64, true)]).unwrap(),
            UnionMode::Sparse,
        ),
        DataType::Union(
            UnionFields::try_new([0], [Field::new("item", DataType::Int64, true)]).unwrap(),
            UnionMode::Dense,
        ),
        DataType::Boolean,
        DataType::Null,
        DataType::FixedSizeBinary(-1),
        DataType::FixedSizeBinary(0),
    ];
    let mode = BoundedJoinInputMode::KeyedUpsert {
        primary_key_indices: vec![0, 1],
    };
    for data_type in rejected {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", data_type, false),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));
        let config = BoundedJoinInputConfig {
            vnode: 0,
            event_time_index: 1,
            mode: mode.clone(),
            max_retained_bytes: usize::MAX,
        };
        let error = match BoundedJoinInputNormalizer::try_new(schema, config) {
            Ok(_) => panic!("expansion-prone schema must be rejected before construction"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("expansion-prone"));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));
    let config = BoundedJoinInputConfig {
        vnode: 0,
        event_time_index: 1,
        mode: mode.clone(),
        max_retained_bytes: usize::MAX,
    };
    let checkpoint = BoundedJoinInputCheckpoint {
        version: NORMALIZER_CHECKPOINT_VERSION,
        config_fingerprint: normalizer_config_fingerprint(schema.as_ref(), 1, &mode),
        closed_cutoff: i64::MIN,
        replay_frontiers: Vec::new(),
        mode: BoundedJoinInputModeCheckpoint::Keyed {
            next_batch_id: 1,
            slots: Vec::new(),
            compacted_rows_ipc: Vec::new(),
        },
    };
    let error = match BoundedJoinInputNormalizer::from_checkpoint(
        &checkpoint,
        schema,
        config,
        usize::MAX,
    ) {
        Ok(_) => panic!("dictionary schema must be rejected before checkpoint restore"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("expansion-prone"));
}

#[test]
fn duplicate_primary_key_indices_are_rejected_before_row_converter_construction() {
    let error = match BoundedJoinInputNormalizer::try_new(
        plain_schema(),
        BoundedJoinInputConfig {
            vnode: 0,
            event_time_index: 1,
            mode: BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices: vec![0, 1, 1],
            },
            max_retained_bytes: usize::MAX,
        },
    ) {
        Ok(_) => panic!("duplicate primary-key columns must be rejected before construction"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("primary-key contract is invalid"));

    BoundedJoinInputNormalizer::try_new(
        plain_schema(),
        BoundedJoinInputConfig {
            vnode: 0,
            event_time_index: 1,
            mode: BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices: vec![1, 0],
            },
            max_retained_bytes: usize::MAX,
        },
    )
    .expect("declared primary-key order is semantic and need not be sorted");
}

fn weighted_batch(
    ids: &[Option<&str>],
    times: &[i64],
    values: &[i64],
    weights: &[i64],
) -> RecordBatch {
    let mut fields = plain_schema().fields().to_vec();
    fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(TimestampMillisecondArray::from(times.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(weights.to_vec())),
        ],
    )
    .unwrap()
}

fn positioned(
    batch: RecordBatch,
    partitions: &[&[u8]],
    orders: &[u64],
    mutations: Option<Vec<SourceMutation>>,
) -> RecordBatch {
    assert_eq!(partitions.len(), batch.num_rows());
    assert_eq!(orders.len(), batch.num_rows());
    let order_bytes = orders
        .iter()
        .map(|order| order.to_be_bytes())
        .collect::<Vec<_>>();
    let positions = SourceRowPositions::try_new(
        BinaryArray::from_iter_values(partitions.iter().copied()),
        BinaryArray::from_iter_values(order_bytes.iter()),
        UInt32Array::from_iter_values(std::iter::repeat_n(0, batch.num_rows())),
    )
    .unwrap();
    let visible_schema = batch.schema();
    let positioned_schema = schema_with_source_row_positions(&visible_schema).unwrap();
    let mutation_schema = schema_with_source_mutations_and_row_positions(&visible_schema).unwrap();
    let source = SourceBatch::positioned(batch, positions).unwrap();
    let source = if let Some(mutations) = mutations {
        source.with_mutations(mutations).unwrap()
    } else {
        source
    };
    source
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &positioned_schema,
            &mutation_schema,
        )
        .unwrap()
}

fn keyed_normalizer() -> BoundedJoinInputNormalizer {
    BoundedJoinInputNormalizer::try_new(
        plain_schema(),
        BoundedJoinInputConfig {
            vnode: 7,
            event_time_index: 1,
            mode: BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices: vec![0, 1],
            },
            max_retained_bytes: 16 * 1024 * 1024,
        },
    )
    .unwrap()
}

fn full_normalizer() -> BoundedJoinInputNormalizer {
    BoundedJoinInputNormalizer::try_new(
        weighted_batch(&[], &[], &[], &[]).schema(),
        BoundedJoinInputConfig {
            vnode: 9,
            event_time_index: 1,
            mode: BoundedJoinInputMode::FullChangelog,
            max_retained_bytes: 16 * 1024 * 1024,
        },
    )
    .unwrap()
}

fn append_normalizer() -> BoundedJoinInputNormalizer {
    BoundedJoinInputNormalizer::try_new(
        plain_schema(),
        BoundedJoinInputConfig {
            vnode: 5,
            event_time_index: 1,
            mode: BoundedJoinInputMode::AppendOnly,
            max_retained_bytes: 16 * 1024 * 1024,
        },
    )
    .unwrap()
}

fn output_weights(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column_by_name(WEIGHT_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn output_values(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn assert_gc_consistent(state: &BoundedJoinInputNormalizer) {
    match &state.mode {
        ModeState::AppendOnly => return,
        ModeState::Keyed(keyed) => assert_gc_index(&keyed.gc, keyed.slots.len(), |key| {
            keyed.slots.contains_key(key)
        }),
        ModeState::Full(full) => assert_gc_index(&full.gc, full.slots.len(), |row| {
            full.slots.contains_key(row)
        }),
    }
}

fn assert_gc_index(gc: &ExactGcIndex, slot_count: usize, contains: impl Fn(&[u8]) -> bool) {
    assert_eq!(gc.heap.len(), slot_count);
    for Reverse((_, identity)) in &gc.heap {
        assert!(contains(identity.as_ref()));
    }
}

#[test]
fn construction_accounts_dynamic_schema_metadata_and_normalization_preflights_rows() {
    let baseline = append_normalizer().accounted_state_bytes();
    let metadata_value = "x".repeat(16 * 1024);
    let schema = Arc::new(Schema::new_with_metadata(
        plain_schema().fields().to_vec(),
        std::collections::HashMap::from([("large".into(), metadata_value.clone())]),
    ));
    let state = BoundedJoinInputNormalizer::try_new(
        schema,
        BoundedJoinInputConfig {
            vnode: 11,
            event_time_index: 1,
            mode: BoundedJoinInputMode::AppendOnly,
            max_retained_bytes: 16 * 1024 * 1024,
        },
    )
    .unwrap();
    assert!(state.accounted_state_bytes() >= baseline + metadata_value.len());

    let rows = MAX_CYCLE_OUTPUT_ROWS / 2 + 1;
    let oversized = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )])),
        vec![Arc::new(Int64Array::new_null(rows))],
    )
    .unwrap();
    assert!(preflight_normalization_scratch(&oversized, 2, 11)
        .unwrap_err()
        .to_string()
        .contains("could expand beyond"));
}

#[test]
fn normalization_preflight_counts_hidden_source_coordinate_buffers() {
    preflight_normalization_scratch_parts(1, 3, 3, 0, 1, 11).unwrap();
    let oversized_coordinates = MAX_CYCLE_OUTPUT_BYTES / 4 + 1;
    let error = preflight_normalization_scratch_parts(1, 6, 6, oversized_coordinates, 1, 11)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("normalization scratch would exceed"),
        "unexpected error: {error}"
    );

    let wide_flat =
        preflight_normalization_scratch_parts(16 * 1024, 512, 512, 8 * 1024 * 1024, 1, 11)
            .unwrap_err()
            .to_string();
    assert!(
        wide_flat.contains("normalization scratch would exceed"),
        "unexpected error: {wide_flat}"
    );
}

#[test]
fn output_payload_is_preflighted_before_roster_allocation_or_row_access() {
    let visible_schema = plain_schema();
    let output_schema = weighted_schema(visible_schema.as_ref());
    let batch = Arc::new(plain_batch(&[Some("A")], &[100], &[10]));
    let rows = [OutputRowRef {
        batch,
        row: u32::MAX,
        logical_bytes: MAX_CYCLE_OUTPUT_BYTES,
    }];
    let error = build_output(&visible_schema, &output_schema, &rows, &[1])
        .unwrap_err()
        .to_string();
    assert!(error.contains("would exceed"), "unexpected error: {error}");
}

#[test]
fn keyed_accepts_routed_all_put_metadata() {
    let mut state = keyed_normalizer();
    let mixed = positioned(
        plain_batch(&[Some("A"), Some("B")], &[100, 100], &[10, 20]),
        &[b"partition-0", b"partition-0"],
        &[1, 2],
        Some(vec![SourceMutation::Put, SourceMutation::Tombstone]),
    );
    let routed_put = mixed.slice(0, 1);
    let prepared = state.prepare(routed_put, i64::MIN).unwrap();
    assert_eq!(output_values(prepared.output()), vec![10]);
    assert_eq!(output_weights(prepared.output()), vec![1]);
    prepared.commit();
}

#[test]
fn keyed_prefix_and_retained_batch_references_are_exact() {
    let mut state = keyed_normalizer();
    let initial = positioned(
        plain_batch(
            &[Some("A"), Some("B"), Some("A")],
            &[100, 100, 100],
            &[10, 20, 11],
        ),
        &[b"partition-0", b"partition-0", b"partition-0"],
        &[1, 2, 3],
        None,
    );
    let prepared = state.prepare(initial, i64::MIN).unwrap();
    assert_eq!(output_values(prepared.output()), vec![10, 20, 10, 11]);
    assert_eq!(output_weights(prepared.output()), vec![1, 1, -1, 1]);
    prepared.commit();
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert_eq!(keyed.retained_batches.len(), 1);
    assert_eq!(
        keyed.retained_batches.values().next().unwrap().references,
        2
    );
    assert!(keyed
        .slots
        .values()
        .filter_map(|slot| slot.row)
        .all(|row| row.logical_bytes != 0));
    assert!(Arc::ptr_eq(
        &keyed
            .retained_batches
            .values()
            .next()
            .unwrap()
            .batch
            .schema(),
        &state.visible_schema
    ));

    state
        .prepare(
            positioned(
                plain_batch(&[Some("A")], &[100], &[12]),
                &[b"partition-0"],
                &[4],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert_eq!(keyed.retained_batches.len(), 2);
    assert!(keyed
        .retained_batches
        .values()
        .all(|retained| retained.references == 1));

    state
        .prepare(
            positioned(
                plain_batch(&[Some("B")], &[100], &[999]),
                &[b"partition-0"],
                &[5],
                Some(vec![SourceMutation::Tombstone]),
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert_eq!(keyed.retained_batches.len(), 1);
    assert_eq!(
        keyed.retained_batches.values().next().unwrap().references,
        1
    );
    assert_gc_consistent(&state);
}

#[test]
fn full_changelog_applies_same_batch_prefix_arithmetic() {
    let mut state = full_normalizer();
    let prepared = state
        .prepare(
            positioned(
                weighted_batch(&[Some("A"), Some("A")], &[100, 100], &[10, 10], &[2, -1]),
                &[b"partition-0", b"partition-0"],
                &[1, 2],
                None,
            ),
            i64::MIN,
        )
        .unwrap();
    assert_eq!(output_weights(prepared.output()), vec![2, -1]);
    prepared.commit();
    let ModeState::Full(full) = &state.mode else {
        unreachable!()
    };
    assert_eq!(full.slots.values().next().unwrap().multiplicity, 1);
    assert_gc_consistent(&state);
}

#[test]
fn keyed_replay_differential_and_partition_affinity_are_exact() {
    let mut state = keyed_normalizer();
    let first = positioned(
        plain_batch(&[Some("A")], &[100], &[10]),
        &[b"partition-0"],
        &[1],
        None,
    );
    let prepared = state.prepare(first.clone(), i64::MIN).unwrap();
    assert_eq!(output_weights(prepared.output()), vec![1]);
    prepared.commit();

    let replay = state.prepare(first, i64::MIN).unwrap();
    assert_eq!(replay.output().num_rows(), 0);
    replay.commit();

    let divergent = positioned(
        plain_batch(&[Some("A")], &[100], &[11]),
        &[b"partition-0"],
        &[1],
        None,
    );
    assert!(state
        .prepare(divergent, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("divergent row bytes"));

    let update = positioned(
        plain_batch(&[Some("A")], &[100], &[20]),
        &[b"partition-0"],
        &[2],
        None,
    );
    let prepared = state.prepare(update, i64::MIN).unwrap();
    assert_eq!(output_values(prepared.output()), vec![10, 20]);
    assert_eq!(output_weights(prepared.output()), vec![-1, 1]);
    prepared.commit();
    assert_gc_consistent(&state);

    let older = positioned(
        plain_batch(&[Some("A")], &[100], &[777]),
        &[b"partition-0"],
        &[1],
        None,
    );
    let prepared = state.prepare(older, i64::MIN).unwrap();
    assert_eq!(prepared.output().num_rows(), 0);
    prepared.commit();

    let moved = positioned(
        plain_batch(&[Some("A")], &[100], &[30]),
        &[b"partition-1"],
        &[1],
        None,
    );
    assert!(state
        .prepare(moved, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("moved between source partitions"));
}

#[test]
fn keyed_multi_batch_overlay_rejects_cross_batch_regression_and_affinity_atomically() {
    let mut state = keyed_normalizer();
    let regression = [
        positioned(
            plain_batch(&[Some("A")], &[100], &[10]),
            &[b"partition-0"],
            &[10],
            None,
        ),
        positioned(
            plain_batch(&[Some("A")], &[100], &[9]),
            &[b"partition-0"],
            &[9],
            None,
        ),
    ];
    let error = state
        .prepare_batches(&regression, i64::MIN, i64::MIN, usize::MAX)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("regressed within one partition"));
    assert!(state.replay_frontiers.is_empty());
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert!(keyed.slots.is_empty());

    let moved = [
        positioned(
            plain_batch(&[Some("A")], &[100], &[10]),
            &[b"partition-0"],
            &[1],
            None,
        ),
        positioned(
            plain_batch(&[Some("A")], &[100], &[20]),
            &[b"partition-1"],
            &[2],
            None,
        ),
    ];
    let error = state
        .prepare_batches(&moved, i64::MIN, i64::MIN, usize::MAX)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("moved between source partitions"));
    assert!(state.replay_frontiers.is_empty());
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert!(keyed.slots.is_empty());

    let valid = [
        positioned(
            plain_batch(&[Some("A")], &[100], &[10]),
            &[b"partition-0"],
            &[1],
            None,
        ),
        positioned(
            plain_batch(&[Some("A")], &[100], &[20]),
            &[b"partition-0"],
            &[2],
            Some(vec![SourceMutation::Tombstone]),
        ),
    ];
    let prepared = state
        .prepare_batches(&valid, i64::MIN, i64::MIN, usize::MAX)
        .unwrap();
    assert_eq!(output_values(prepared.output()), vec![10, 10]);
    assert_eq!(output_weights(prepared.output()), vec![1, -1]);
    prepared.commit();
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert_eq!(keyed.slots.len(), 1);
    assert!(keyed.slots.values().all(|slot| slot.row.is_none()));
    assert!(keyed.retained_batches.is_empty());
}

#[test]
fn keyed_split_cutoff_emits_old_image_before_post_cycle_gc() {
    let mut state = keyed_normalizer();
    state
        .prepare(
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-0"],
                &[1],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let update = [positioned(
        plain_batch(&[Some("A")], &[100], &[20]),
        &[b"partition-0"],
        &[2],
        None,
    )];
    let prepared = state
        .prepare_batches(&update, i64::MIN, 101, usize::MAX)
        .unwrap();
    assert_eq!(output_values(prepared.output()), vec![10, 20]);
    assert_eq!(output_weights(prepared.output()), vec![-1, 1]);
    prepared.commit();
    assert_eq!(state.closed_cutoff(), 101);
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert!(keyed.slots.is_empty());
    assert!(keyed.retained_batches.is_empty());
    assert!(keyed.gc.heap.is_empty());
    assert_eq!(state.replay_frontiers.len(), 1);
}

#[test]
fn replay_regression_is_terminal_even_when_every_row_is_older() {
    let mut state = keyed_normalizer();
    state
        .prepare(
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-0"],
                &[10],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let regressed = positioned(
        plain_batch(&[Some("A"), Some("A")], &[100, 100], &[8, 7]),
        &[b"partition-0", b"partition-0"],
        &[8, 7],
        None,
    );
    assert!(state
        .prepare(regressed, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("regressed within one partition"));
}

#[test]
fn keyed_tombstones_keep_zero_affinity_without_arrow_payload() {
    let mut state = keyed_normalizer();
    state
        .prepare(
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-0"],
                &[1],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let tombstone = positioned(
        plain_batch(&[Some("A")], &[100], &[999]),
        &[b"partition-0"],
        &[2],
        Some(vec![SourceMutation::Tombstone]),
    );
    let prepared = state.prepare(tombstone, i64::MIN).unwrap();
    assert_eq!(output_values(prepared.output()), vec![10]);
    assert_eq!(output_weights(prepared.output()), vec![-1]);
    prepared.commit();

    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert_eq!(keyed.slots.len(), 1);
    assert!(keyed.slots.values().all(|slot| slot.row.is_none()));
    assert!(keyed.retained_batches.is_empty());
    assert_eq!(keyed.gc.heap.len(), 1);
    assert_gc_consistent(&state);

    let absent = positioned(
        plain_batch(&[Some("B")], &[100], &[1]),
        &[b"partition-1"],
        &[1],
        Some(vec![SourceMutation::Tombstone]),
    );
    let prepared = state.prepare(absent, i64::MIN).unwrap();
    assert_eq!(prepared.output().num_rows(), 0);
    prepared.commit();
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert_eq!(keyed.slots.len(), 2);
    assert!(keyed.retained_batches.is_empty());
    assert_eq!(keyed.gc.heap.len(), 2);
    assert_gc_consistent(&state);
}

#[test]
fn keyed_primary_keys_are_runtime_non_null() {
    let mut state = keyed_normalizer();
    let null_key = positioned(
        plain_batch(&[None], &[100], &[10]),
        &[b"partition-0"],
        &[1],
        None,
    );
    assert!(state
        .prepare(null_key, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("primary key contains NULL"));
}

#[test]
fn full_changelog_checks_multiplicity_and_partition_affinity() {
    let mut state = full_normalizer();
    state
        .prepare(
            positioned(
                weighted_batch(&[Some("A")], &[100], &[10], &[2]),
                &[b"partition-0"],
                &[1],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let divergent_weight = positioned(
        weighted_batch(&[Some("A")], &[100], &[10], &[3]),
        &[b"partition-0"],
        &[1],
        None,
    );
    assert!(state
        .prepare(divergent_weight, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("divergent row bytes"));
    let to_zero = positioned(
        weighted_batch(&[Some("A")], &[100], &[10], &[-2]),
        &[b"partition-0"],
        &[2],
        None,
    );
    let prepared = state.prepare(to_zero, i64::MIN).unwrap();
    assert_eq!(output_weights(prepared.output()), vec![-2]);
    prepared.commit();
    let ModeState::Full(full) = &state.mode else {
        unreachable!()
    };
    assert_eq!(full.slots.values().next().unwrap().multiplicity, 0);
    assert_eq!(full.gc.heap.len(), 1);
    assert_gc_consistent(&state);

    let moved = positioned(
        weighted_batch(&[Some("A")], &[100], &[10], &[1]),
        &[b"partition-1"],
        &[1],
        None,
    );
    assert!(state
        .prepare(moved, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("moved between source partitions"));

    let underflow = positioned(
        weighted_batch(&[Some("A")], &[100], &[10], &[-1]),
        &[b"partition-0"],
        &[3],
        None,
    );
    assert!(state
        .prepare(underflow, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("multiplicity underflow"));
}

#[test]
fn full_changelog_overflow_and_zero_weight_are_terminal() {
    let mut overflow = full_normalizer();
    overflow
        .prepare(
            positioned(
                weighted_batch(&[Some("A")], &[100], &[10], &[i64::MAX]),
                &[b"partition-0"],
                &[1],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let plus_one = positioned(
        weighted_batch(&[Some("A")], &[100], &[10], &[1]),
        &[b"partition-0"],
        &[2],
        None,
    );
    assert!(overflow
        .prepare(plus_one, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("multiplicity overflow"));

    let mut zero = full_normalizer();
    let zero_weight = positioned(
        weighted_batch(&[Some("A")], &[100], &[10], &[0]),
        &[b"partition-0"],
        &[1],
        None,
    );
    assert!(zero
        .prepare(zero_weight, i64::MIN)
        .err()
        .unwrap()
        .to_string()
        .contains("zero weight"));
}

#[test]
fn cutoff_evicts_slots_but_keeps_replay_frontiers() {
    let mut state = keyed_normalizer();
    state
        .prepare(
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-0"],
                &[1],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    state
        .prepare(
            positioned(
                plain_batch(&[Some("Z")], &[300], &[30]),
                &[b"partition-0"],
                &[2],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    let capacities = (
        keyed.slots.capacity(),
        keyed.retained_batches.capacity(),
        keyed.gc.heap.capacity(),
    );

    let replacement = positioned(
        plain_batch(&[Some("B")], &[200], &[20]),
        &[b"partition-0"],
        &[3],
        None,
    );
    let prepared = state.prepare(replacement, 101).unwrap();
    assert_eq!(output_values(prepared.output()), vec![20]);
    assert_eq!(output_weights(prepared.output()), vec![1]);
    prepared.commit();
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert_eq!(keyed.slots.len(), 2);
    assert_eq!(keyed.retained_batches.len(), 2);
    assert!(keyed
        .retained_batches
        .values()
        .all(|retained| retained.references == 1));
    assert_eq!(keyed.gc.heap.len(), 2);
    assert_eq!(
        (
            keyed.slots.capacity(),
            keyed.retained_batches.capacity(),
            keyed.gc.heap.capacity(),
        ),
        capacities,
        "one cutoff eviction plus one insertion must reserve only final net growth"
    );
    assert_gc_consistent(&state);
    assert_eq!(state.replay_frontiers.len(), 1);

    let replay = state
        .prepare(
            positioned(
                plain_batch(&[Some("A"), Some("Z")], &[100, 300], &[10, 30]),
                &[b"partition-0", b"partition-0"],
                &[1, 2],
                None,
            ),
            101,
        )
        .unwrap();
    assert_eq!(replay.output().num_rows(), 0);
    replay.commit();
    let late = positioned(
        plain_batch(&[Some("A")], &[100], &[10]),
        &[b"partition-0"],
        &[4],
        None,
    );
    assert!(state
        .prepare(late, 101)
        .err()
        .unwrap()
        .to_string()
        .contains("below closed cutoff"));

    let empty = positioned(plain_batch(&[], &[], &[]), &[], &[], None);
    state.prepare(empty, 301).unwrap().commit();
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert!(keyed.slots.is_empty());
    assert!(keyed.retained_batches.is_empty());
    assert!(keyed.gc.heap.is_empty());
    assert_gc_consistent(&state);
}

#[test]
fn dropping_prepared_input_is_logically_atomic_and_keeps_capacity_charged() {
    let mut state = keyed_normalizer();
    let update = positioned(
        plain_batch(&[Some("A")], &[100], &[10]),
        &[b"partition-0"],
        &[1],
        None,
    );
    let before = state.accounted_state_bytes();
    {
        let prepared = state.prepare(update.clone(), i64::MIN).unwrap();
        assert_eq!(output_weights(prepared.output()), vec![1]);
    }
    assert!(state.accounted_state_bytes() >= before);
    assert!(state.replay_frontiers.is_empty());
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert!(keyed.slots.is_empty());
    assert!(keyed.gc.heap.is_empty());
    assert!(keyed.gc.heap.capacity() > 0);

    let prepared = state.prepare(update, i64::MIN).unwrap();
    assert_eq!(output_weights(prepared.output()), vec![1]);
    prepared.commit();
    assert_eq!(state.replay_frontiers.len(), 1);

    let empty = positioned(plain_batch(&[], &[], &[]), &[], &[], None);
    {
        let prepared = state.prepare(empty, 101).unwrap();
        assert_eq!(prepared.output().num_rows(), 0);
    }
    assert_eq!(state.closed_cutoff, i64::MIN);
    let ModeState::Keyed(keyed) = &state.mode else {
        unreachable!()
    };
    assert_eq!(keyed.slots.len(), 1);
    assert_eq!(keyed.retained_batches.len(), 1);
    assert_eq!(
        keyed.retained_batches.values().next().unwrap().references,
        1
    );
    assert_eq!(keyed.gc.heap.len(), 1);
    assert_gc_consistent(&state);
}

#[test]
fn replay_only_and_zero_affinity_checkpoint_round_trip() {
    let mut append = append_normalizer();
    let replay = positioned(
        plain_batch(&[Some("A")], &[100], &[10]),
        &[b"partition-0"],
        &[1],
        None,
    );
    append.prepare(replay.clone(), i64::MIN).unwrap().commit();
    append
        .prepare_batches(&[], i64::MIN, 101, usize::MAX)
        .unwrap()
        .commit();
    let checkpoint = append
        .capture_checkpoint(usize::MAX)
        .unwrap()
        .encode(usize::MAX)
        .unwrap();
    let mut append = BoundedJoinInputNormalizer::from_checkpoint(
        &checkpoint,
        plain_schema(),
        BoundedJoinInputConfig {
            vnode: 5,
            event_time_index: 1,
            mode: BoundedJoinInputMode::AppendOnly,
            max_retained_bytes: 16 * 1024 * 1024,
        },
        usize::MAX,
    )
    .unwrap();
    assert_eq!(append.closed_cutoff(), 101);
    assert_eq!(append.replay_frontiers.len(), 1);
    let prepared = append
        .prepare_batches(&[replay], 101, 101, usize::MAX)
        .unwrap();
    assert_eq!(prepared.output().num_rows(), 0);
    prepared.commit();

    let mut keyed = keyed_normalizer();
    keyed
        .prepare(
            positioned(
                plain_batch(&[Some("A")], &[100], &[999]),
                &[b"partition-0"],
                &[1],
                Some(vec![SourceMutation::Tombstone]),
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let checkpoint = keyed
        .capture_checkpoint(usize::MAX)
        .unwrap()
        .encode(usize::MAX)
        .unwrap();
    let mut keyed = BoundedJoinInputNormalizer::from_checkpoint(
        &checkpoint,
        plain_schema(),
        BoundedJoinInputConfig {
            vnode: 7,
            event_time_index: 1,
            mode: BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices: vec![0, 1],
            },
            max_retained_bytes: 16 * 1024 * 1024,
        },
        usize::MAX,
    )
    .unwrap();
    let ModeState::Keyed(restored) = &keyed.mode else {
        unreachable!()
    };
    assert_eq!(restored.slots.len(), 1);
    assert!(restored.slots.values().all(|slot| slot.row.is_none()));
    assert!(restored.retained_batches.is_empty());
    assert_gc_consistent(&keyed);
    let error = keyed
        .prepare(
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-1"],
                &[2],
                None,
            ),
            i64::MIN,
        )
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("moved between source partitions"));
}

#[test]
fn keyed_live_checkpoint_rejects_tight_budget_before_ipc_materialization() {
    let mut keyed = keyed_normalizer();
    keyed
        .prepare(
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-0"],
                &[1],
                None,
            ),
            i64::MIN,
        )
        .unwrap()
        .commit();
    let checkpoint = keyed
        .capture_checkpoint(usize::MAX)
        .unwrap()
        .encode(usize::MAX)
        .unwrap();
    let config = BoundedJoinInputConfig {
        vnode: 7,
        event_time_index: 1,
        mode: BoundedJoinInputMode::KeyedUpsert {
            primary_key_indices: vec![0, 1],
        },
        max_retained_bytes: usize::MAX,
    };
    let restore_peak = BoundedJoinInputNormalizer::checkpoint_restore_preflight_bytes(
        &checkpoint,
        plain_schema().as_ref(),
        &config,
    )
    .unwrap();
    let error = match BoundedJoinInputNormalizer::from_checkpoint(
        &checkpoint,
        plain_schema(),
        config.clone(),
        restore_peak - 1,
    ) {
        Ok(_) => panic!("tight restore headroom must fail before decoding compacted IPC"),
        Err(error) => error,
    };
    assert!(matches!(error, DbError::ManagedStateBudgetExceeded { .. }));

    let restored = BoundedJoinInputNormalizer::from_checkpoint(
        &checkpoint,
        plain_schema(),
        config,
        restore_peak,
    )
    .unwrap();
    let ModeState::Keyed(restored_keyed) = &restored.mode else {
        unreachable!()
    };
    assert_eq!(restored_keyed.slots.len(), 1);
    assert_eq!(restored_keyed.retained_batches.len(), 1);
    assert!(restored_keyed
        .slots
        .values()
        .all(|slot| slot.row.is_some() && slot.row_identity.is_some()));
    assert_gc_consistent(&restored);
}

#[test]
fn append_only_is_replay_safe_and_emits_explicit_unit_weights() {
    let mut state = append_normalizer();
    let input = positioned(
        plain_batch(&[Some("A"), Some("B")], &[100, 101], &[10, 20]),
        &[b"partition-0", b"partition-1"],
        &[1, 1],
        None,
    );
    let prepared = state.prepare(input.clone(), i64::MIN).unwrap();
    assert_eq!(output_weights(prepared.output()), vec![1, 1]);
    prepared.commit();
    let replay = state.prepare(input, i64::MIN).unwrap();
    assert_eq!(replay.output().num_rows(), 0);
    replay.commit();
}
