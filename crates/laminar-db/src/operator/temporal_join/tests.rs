use super::*;
use arrow::array::{
    Array, BinaryArray, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use laminar_connectors::connector::{
    schema_with_source_mutations_and_row_positions, schema_with_source_row_positions, SourceBatch,
    SourceMutation, SourceRowPositionCapability, SourceRowPositions, SOURCE_ORDER_KEY_COLUMN,
    SOURCE_PARTITION_COLUMN, SOURCE_SUB_OFFSET_COLUMN,
};
use laminar_sql::temporal::{TemporalJoinKind, TemporalProbeSchedule};

fn visible_schemas() -> (SchemaRef, SchemaRef) {
    let left = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new(
            "trade_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("trade_id", DataType::Int64, false),
    ]));
    let right = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new(
            "quote_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Utf8, false),
    ]));
    (left, right)
}

fn limits(ready_probe_budget: usize) -> TemporalJoinExecutionLimits {
    TemporalJoinExecutionLimits {
        left_allowed_lateness_ms: 0,
        right_allowed_lateness_ms: 0,
        history_retention_ms: 10_000,
        max_pending_probes: 100,
        ready_probe_budget: NonZeroUsize::new(ready_probe_budget).unwrap(),
        history_gc_budget: NonZeroUsize::new(8).unwrap(),
        maintenance_vnode_budget: NonZeroUsize::new(1).unwrap(),
    }
}

fn config() -> TemporalJoinTranslatorConfig {
    TemporalJoinTranslatorConfig {
        left_table: "trades".into(),
        right_table: "quotes".into(),
        left_key_columns: vec!["symbol".into(), "venue".into()],
        right_key_columns: vec!["symbol".into(), "venue".into()],
        left_time_column: "trade_time".into(),
        right_time_column: "quote_time".into(),
        join_kind: TemporalJoinKind::Left,
        probe_schedule: TemporalProbeSchedule::as_of(),
        probe_alias: None,
    }
}

fn operator(ready_probe_budget: usize) -> (ManagedTemporalJoinOperator, SchemaRef, SchemaRef) {
    operator_with_projection(ready_probe_budget, None)
}

fn operator_with_projection(
    ready_probe_budget: usize,
    projection_sql: Option<&str>,
) -> (ManagedTemporalJoinOperator, SchemaRef, SchemaRef) {
    let (left_visible, right_visible) = visible_schemas();
    let left = schema_with_source_row_positions(&left_visible).unwrap();
    let right = schema_with_source_row_positions(&right_visible).unwrap();
    let operator = ManagedTemporalJoinOperator::try_new(
        "temporal",
        config(),
        projection_sql.map(Arc::from),
        SessionContext::new(),
        Arc::clone(&left),
        Arc::clone(&right),
        KeyGroupCount::try_from(2_u16).unwrap(),
        limits(ready_probe_budget),
    )
    .unwrap();
    (operator, left, right)
}

#[cfg(feature = "cluster")]
#[test]
fn terminal_errors_survive_every_temporal_post_admission_wrapper() {
    fn assert_terminal(error: DbError, expected: &str) {
        let DbError::ShuffleTerminal(reason) = error else {
            panic!("expected permanent shuffle halt, got {error}");
        };
        assert_eq!(reason, expected);
    }

    let (operator, _, _) = operator(1);
    assert_terminal(
        operator.outbound_finalize_error(DbError::ShuffleTerminal("outbound".into())),
        "outbound",
    );
    assert_terminal(
        operator.remote_replay_error(DbError::ShuffleTerminal("remote replay".into())),
        "remote replay",
    );
    assert_terminal(
        operator.post_projection_error(DbError::ShuffleTerminal("post projection".into())),
        "post projection",
    );
    assert_terminal(
        operator.after_apply_error(true, 1, DbError::ShuffleTerminal("after apply".into())),
        "after apply",
    );
}

fn positions(rows: usize, first: u64) -> SourceRowPositions {
    let partitions = std::iter::repeat_n(b"p0".as_slice(), rows);
    let orders: Vec<[u8; 8]> = (first..first + rows as u64).map(u64::to_be_bytes).collect();
    SourceRowPositions::try_new(
        BinaryArray::from_iter_values(partitions),
        BinaryArray::from_iter_values(orders.iter()),
        UInt32Array::from(vec![0; rows]),
    )
    .unwrap()
}

fn left_batch(keys: &[String], venues: &[&str], times: &[i64], ids: &[i64]) -> RecordBatch {
    let (visible, _) = visible_schemas();
    let rows = RecordBatch::try_new(
        Arc::clone(&visible),
        vec![
            Arc::new(StringArray::from_iter_values(keys)),
            Arc::new(StringArray::from(venues.to_vec())),
            Arc::new(TimestampMillisecondArray::from(times.to_vec())),
            Arc::new(Int64Array::from(ids.to_vec())),
        ],
    )
    .unwrap();
    let positioned = schema_with_source_row_positions(&visible).unwrap();
    let mutations = schema_with_source_mutations_and_row_positions(&visible).unwrap();
    SourceBatch::positioned(rows, positions(keys.len(), 100))
        .unwrap()
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &positioned,
            &mutations,
        )
        .unwrap()
}

fn right_batch(
    keys: &[String],
    venues: &[&str],
    times: &[i64],
    values: &[&str],
    mutations: &[SourceMutation],
) -> RecordBatch {
    right_batch_at(keys, venues, times, values, mutations, 1)
}

fn right_batch_at(
    keys: &[String],
    venues: &[&str],
    times: &[i64],
    values: &[&str],
    mutations: &[SourceMutation],
    first_position: u64,
) -> RecordBatch {
    let (_, visible) = visible_schemas();
    let rows = RecordBatch::try_new(
        Arc::clone(&visible),
        vec![
            Arc::new(StringArray::from_iter_values(keys)),
            Arc::new(StringArray::from(venues.to_vec())),
            Arc::new(TimestampMillisecondArray::from(times.to_vec())),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .unwrap();
    let positioned = schema_with_source_row_positions(&visible).unwrap();
    let mutation_schema = schema_with_source_mutations_and_row_positions(&visible).unwrap();
    SourceBatch::positioned(rows, positions(keys.len(), first_position))
        .unwrap()
        .with_mutations(mutations.to_vec())
        .unwrap()
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &positioned,
            &mutation_schema,
        )
        .unwrap()
}

fn key_for_vnode(target: u32) -> String {
    for candidate in 0..1_000 {
        let key = format!("key-{candidate}");
        let batch = left_batch(std::slice::from_ref(&key), &["X"], &[0], &[0]);
        if laminar_core::shuffle::row_vnodes(&batch, &[0, 1], 2).unwrap() == [target] {
            return key;
        }
    }
    panic!("could not find key for vnode {target}");
}

fn materialize_capture(capture: StateFrameCapture) -> bytes::Bytes {
    let mut staged_bytes = capture.retained_bytes();
    capture.materialize(&mut staged_bytes, u64::MAX).unwrap()
}

fn unaligned_temporal_archive_transport(bytes: &[u8], archive_offset: usize) -> bytes::Bytes {
    let mut transport = vec![0_u8; bytes.len() + CHECKPOINT_ARCHIVE_ALIGNMENT];
    let base = transport.as_ptr() as usize;
    let offset = (0..CHECKPOINT_ARCHIVE_ALIGNMENT)
        .find(|offset| {
            !(base + offset + archive_offset).is_multiple_of(CHECKPOINT_ARCHIVE_ALIGNMENT)
        })
        .expect("a temporal archive transport offset must be unaligned");
    transport[offset..offset + bytes.len()].copy_from_slice(bytes);
    let bytes = bytes::Bytes::from(transport).slice(offset..offset + bytes.len());
    assert_ne!(
        bytes[archive_offset..]
            .as_ptr()
            .align_offset(CHECKPOINT_ARCHIVE_ALIGNMENT),
        0
    );
    bytes
}

fn frontier(watermark: i64) -> [InputFrontier; 2] {
    [
        InputFrontier {
            watermark: Some(watermark),
            idle: false,
        },
        InputFrontier {
            watermark: Some(watermark),
            idle: false,
        },
    ]
}

#[test]
fn whole_checkpoint_preflight_bounds_unaligned_owned_vectors() {
    let checkpoint = TemporalJoinOperatorCheckpoint {
        version: OPERATOR_CHECKPOINT_VERSION,
        frontiers: frontier(10).map(Into::into),
        maintenance_cursor: 0,
        maintenance_pending: false,
        maintenance_remaining: 0,
        maintenance_rescan: false,
        published_output_frontier: Some(frontier(10)[0].into()),
        cluster: Some(TemporalClusterCheckpoint {
            assignment_version: 7,
            owner_map_digest: [3; 32],
            self_id: 1,
            local_frontiers: frontier(10).map(Into::into),
            remote_peer_cursors: [None; 2],
            channels: [
                vec![TemporalCheckpointChannel {
                    peer: 2,
                    applied: frontier(10)[0].into(),
                    events: vec![TemporalCheckpointEvent::Frontier {
                        recovery_gen: 4,
                        frontier: frontier(10)[0].into(),
                    }],
                    positioned_ipc: Vec::new(),
                    mutation_ipc: Vec::new(),
                }],
                vec![TemporalCheckpointChannel {
                    peer: 2,
                    applied: frontier(10)[1].into(),
                    events: Vec::new(),
                    positioned_ipc: Vec::new(),
                    mutation_ipc: Vec::new(),
                }],
            ],
        }),
    };
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint).unwrap();
    let encoded = unaligned_temporal_archive_transport(&encoded, 0);
    assert_eq!(checkpoint_alignment_copy_bytes(&encoded), encoded.len());

    let (operator, _, _) = operator(8);
    let preflight = operator
        .preflight_whole_checkpoint_archive(&encoded, "test checkpoint", |_| Ok(()))
        .unwrap();
    let decoded = with_aligned_checkpoint_bytes(&encoded, |bytes| {
        rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| DbError::Checkpoint(error.to_string()))
    })
    .unwrap();
    assert!(
        ManagedTemporalJoinOperator::decoded_whole_checkpoint_bytes(&decoded).unwrap()
            <= preflight.decoded_checkpoint
    );
    assert!(operator
        .preflight_whole_checkpoint_archive(&[0xff, 0xfe, 0xfd], "malformed", |_| Ok(()))
        .is_err());
}

#[test]
fn whole_restore_rejects_preflight_peak_one_byte_over_budget() {
    let (mut donor, _, _) = operator(8);
    donor.frontiers = frontier(10);
    let checkpoint = donor.checkpoint().unwrap().unwrap();
    let (mut restored, _, _) = operator(8);
    let restore_preflight = restored
        .preflight_whole_checkpoint_archive(&checkpoint.data, "test checkpoint", |archived| {
            if archived.cluster.is_some() {
                return Err(DbError::Checkpoint("unexpected cluster checkpoint".into()));
            }
            Ok(())
        })
        .unwrap();
    let required = restored
        .checked_accounted_state_bytes()
        .unwrap()
        .checked_add(checkpoint_allocation_bytes(checkpoint.data.capacity()).unwrap())
        .and_then(|bytes| {
            bytes.checked_add(checkpoint_alignment_copy_charge(&checkpoint.data).unwrap())
        })
        .and_then(|bytes| bytes.checked_add(restore_preflight.decoded_checkpoint))
        .unwrap();
    restored.set_managed_state_budget(required - 1);

    assert!(matches!(
        restored.restore(checkpoint),
        Err(DbError::ManagedStateBudgetExceeded { .. })
    ));
    assert_eq!(restored.frontiers, [InputFrontier::default(); 2]);
}

#[tokio::test]
async fn local_vnodes_share_one_bounded_path_for_asof_and_tombstones() {
    let key0 = key_for_vnode(0);
    let key1 = key_for_vnode(1);
    let (mut operator, _, _) = operator(1);
    let right = right_batch(
        &[key0.clone(), key0.clone(), key1.clone(), key0.clone()],
        &["X", "X", "X", "Y"],
        &[90, 110, 95, 100],
        &["old", "deleted", "live", "other-venue"],
        &[
            SourceMutation::Put,
            SourceMutation::Tombstone,
            SourceMutation::Put,
            SourceMutation::Put,
        ],
    );
    let left = left_batch(
        &[key0.clone(), key0.clone(), key1, key0],
        &["X", "X", "X", "Y"],
        &[100, 120, 120, 120],
        &[1, 2, 3, 4],
    );
    let fronts = frontier(200);
    let mut output = operator
        .process_with_frontiers(&[vec![left], vec![right]], &fronts)
        .await
        .unwrap();
    assert!(operator.checkpoint_drain_pending());
    let advanced = frontier(250);
    let drained = operator
        .process_with_frontiers(&[], &advanced)
        .await
        .unwrap();
    assert!(drained.iter().map(RecordBatch::num_rows).sum::<usize>() <= 1);
    output.extend(drained);
    while !operator.wants_input() {
        let drained = operator
            .process_with_frontiers(&[], &advanced)
            .await
            .unwrap();
        assert!(drained.iter().map(RecordBatch::num_rows).sum::<usize>() <= 1);
        output.extend(drained);
    }
    assert!(!operator.checkpoint_drain_pending());
    assert_eq!(operator.frontiers, fronts);
    output.extend(
        operator
            .process_with_frontiers(&[], &advanced)
            .await
            .unwrap(),
    );
    assert!(operator.checkpoint_drain_pending());
    while !operator.wants_input() {
        output.extend(
            operator
                .process_with_frontiers(&[], &advanced)
                .await
                .unwrap(),
        );
    }
    assert!(operator.vnode_states.iter().all(Option::is_some));
    assert_eq!(operator.resident_vnodes, [0, 1]);
    assert_eq!(operator.frontiers, advanced);
    assert_eq!(
        operator.output_frontier(InputFrontier {
            watermark: Some(300),
            idle: false,
        }),
        InputFrontier {
            watermark: Some(250),
            idle: false,
        }
    );

    let mut actual = BTreeMap::new();
    for batch in output {
        let ids = batch
            .column(batch.schema().index_of("trade_id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(batch.schema().index_of("value_quotes").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            actual.insert(
                ids.value(row),
                (!values.is_null(row)).then(|| values.value(row).to_owned()),
            );
        }
    }
    assert_eq!(
        actual,
        BTreeMap::from([
            (1, Some("old".into())),
            (2, None),
            (3, Some("live".into())),
            (4, Some("other-venue".into())),
        ])
    );

    #[cfg(feature = "cluster")]
    {
        let (mut restored_cut, _, _) = self::operator(1);
        restored_cut.frontiers = [
            InputFrontier {
                watermark: Some(100),
                idle: true,
            },
            InputFrontier {
                watermark: Some(50),
                idle: true,
            },
        ];
        let restored_frontiers = restored_cut.frontiers;
        restored_cut.record_published_output_frontier(&restored_frontiers);
        let checkpoint = restored_cut.checkpoint().unwrap().unwrap();
        let (mut recovered, _, _) = self::operator(1);
        recovered.restore(checkpoint).unwrap();
        assert_eq!(
            recovered.restored_output_frontier(),
            Some(InputFrontier {
                watermark: Some(100),
                idle: true,
            })
        );
    }

    let (_, left_schema, right_schema) = self::operator(1);
    let mut negative_config = config();
    negative_config.probe_schedule = TemporalProbeSchedule::list(vec![-50, 0]).unwrap();
    negative_config.probe_alias = Some("probe".into());
    let mut negative = ManagedTemporalJoinOperator::try_new(
        "negative",
        negative_config,
        None,
        SessionContext::new(),
        left_schema,
        right_schema,
        KeyGroupCount::try_from(2_u16).unwrap(),
        limits(1),
    )
    .unwrap();
    negative.frontiers[0] = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    assert_eq!(
        negative.output_frontier(InputFrontier {
            watermark: Some(100),
            idle: false,
        }),
        InputFrontier {
            watermark: Some(50),
            idle: false,
        }
    );
}

#[tokio::test]
async fn uninitialized_idle_left_holds_output_watermark_until_revival() {
    let key = key_for_vnode(0);
    let (mut operator, _, _) = operator(8);
    let right = right_batch(
        std::slice::from_ref(&key),
        &["X"],
        &[12_000],
        &["live"],
        &[SourceMutation::Put],
    );
    let idle_left = [
        InputFrontier {
            watermark: None,
            idle: true,
        },
        InputFrontier {
            watermark: Some(20_000),
            idle: false,
        },
    ];

    assert!(operator
        .process_with_frontiers(&[Vec::new(), vec![right]], &idle_left)
        .await
        .unwrap()
        .is_empty());
    while !operator.wants_input() {
        assert!(operator
            .process_with_frontiers(&[], &idle_left)
            .await
            .unwrap()
            .is_empty());
    }
    assert_eq!(
        operator
            .published_output_frontier
            .and_then(|frontier| frontier.watermark),
        None
    );

    let left = left_batch(std::slice::from_ref(&key), &["X"], &[15_000], &[7]);
    let revived = [
        InputFrontier {
            watermark: Some(16_000),
            idle: false,
        },
        idle_left[1],
    ];
    let output = operator
        .process_with_frontiers(&[vec![left], Vec::new()], &revived)
        .await
        .unwrap();

    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        output[0]
            .column_by_name("value_quotes")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "live"
    );
    assert_eq!(
        operator
            .published_output_frontier
            .and_then(|frontier| frontier.watermark),
        Some(16_000)
    );
}

#[tokio::test]
async fn decision_bound_source_cuts_survive_temporal_checkpoint_round_trip() {
    let decision_cut = [
        InputFrontier {
            watermark: Some(900),
            idle: false,
        },
        InputFrontier {
            watermark: Some(700),
            idle: false,
        },
    ];
    let (mut operator, _, _) = operator(8);

    operator
        .process_with_frontiers(&[], &decision_cut)
        .await
        .unwrap();
    for _ in 0..32 {
        if operator.wants_input() {
            break;
        }
        operator
            .process_with_frontiers(&[], &decision_cut)
            .await
            .unwrap();
    }
    assert!(
        operator.wants_input(),
        "temporal frontier drain did not settle"
    );
    assert_eq!(
        operator.published_output_frontier,
        Some(InputFrontier {
            watermark: Some(700),
            idle: false,
        }),
        "the higher left decision must not publish beyond the lower right decision"
    );

    let checkpoint = operator
        .checkpoint()
        .unwrap()
        .expect("an initialized temporal frontier must be checkpointed");
    let (mut restored, _, _) = self::operator(8);
    restored.restore(checkpoint).unwrap();
    assert_eq!(
        restored.published_output_frontier,
        Some(InputFrontier {
            watermark: Some(700),
            idle: false,
        })
    );

    restored
        .process_with_frontiers(&[], &decision_cut)
        .await
        .unwrap();
    for _ in 0..32 {
        if restored.wants_input() {
            break;
        }
        restored
            .process_with_frontiers(&[], &decision_cut)
            .await
            .unwrap();
    }
    assert!(
        restored.wants_input(),
        "restored frontier drain did not settle"
    );
    assert_eq!(
        restored.published_output_frontier,
        Some(InputFrontier {
            watermark: Some(700),
            idle: false,
        })
    );
    assert!(restored.checkpoint().unwrap().is_some());
}

#[tokio::test]
async fn projection_sees_only_visible_join_columns() {
    let keys = [key_for_vnode(0), key_for_vnode(1)];
    let (mut operator, _, _) = operator_with_projection(
        8,
        Some("SELECT * FROM __temporal_tmp WHERE value_quotes = 'live'"),
    );
    operator.initialize_managed_state().await.unwrap();
    let left = left_batch(&keys, &["X", "X"], &[100, 100], &[7, 8]);
    let right = right_batch(
        &keys,
        &["X", "X"],
        &[90, 90],
        &["live", "stale"],
        &[SourceMutation::Put, SourceMutation::Put],
    );

    let frontiers = frontier(200);
    let mut output = operator
        .process_with_frontiers(&[vec![left], vec![right]], &frontiers)
        .await
        .unwrap();
    while !operator.wants_input() {
        output.extend(
            operator
                .process_with_frontiers(&[], &frontiers)
                .await
                .unwrap(),
        );
    }

    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0]
            .column_by_name("trade_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        output[0]
            .column_by_name("value_quotes")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "live"
    );
    for hidden in [
        SOURCE_PARTITION_COLUMN,
        SOURCE_ORDER_KEY_COLUMN,
        SOURCE_SUB_OFFSET_COLUMN,
    ] {
        assert!(output[0].column_by_name(hidden).is_none());
    }
}

#[tokio::test]
async fn invalid_post_projection_fails_initialization_before_state_admission() {
    let (mut operator, _, _) =
        operator_with_projection(8, Some("SELECT missing_column FROM __temporal_tmp"));

    let error = operator.initialize_managed_state().await.unwrap_err();

    assert!(matches!(error, DbError::Pipeline(_)));
    assert!(operator.vnode_states.iter().all(Option::is_none));
}

#[tokio::test]
async fn vnode_capture_is_full_then_sparse_and_forceable() {
    let key = key_for_vnode(1);
    let (mut donor, _, _) = operator(8);
    let right = right_batch(
        std::slice::from_ref(&key),
        &["X"],
        &[90],
        &["live"],
        &[SourceMutation::Put],
    );
    donor
        .process_with_frontiers(&[Vec::new(), vec![right]], &[InputFrontier::default(); 2])
        .await
        .unwrap();
    donor.maintenance_cursor = 1;
    let whole = donor.checkpoint().unwrap().unwrap();
    let captured = donor
        .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|frame| (frame.vnode, frame.state.map(materialize_capture)))
        .collect::<Vec<_>>();
    assert_eq!(captured.len(), 2);
    assert_eq!(
        captured.iter().map(|(vnode, _)| *vnode).collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(captured[0].1.as_deref().unwrap(), &[ABSENT_VNODE]);
    assert_eq!(captured[1].1.as_deref().unwrap()[0], PRESENT_VNODE);
    let clean = donor
        .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
        .unwrap()
        .unwrap();
    assert!(clean.is_empty());

    let update = right_batch_at(
        std::slice::from_ref(&key),
        &["X"],
        &[95],
        &["new"],
        &[SourceMutation::Put],
        2,
    );
    donor
        .process_with_frontiers(&[Vec::new(), vec![update]], &[InputFrontier::default(); 2])
        .await
        .unwrap();
    let sparse = donor
        .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(sparse.len(), 1);
    assert_eq!(sparse[0].vnode, 1);
    assert!(sparse[0].state.is_some());

    let (mut restored, _, _) = operator(8);
    restored.restore(whole).unwrap();
    assert_eq!(restored.maintenance_cursor, 1);
    restored
        .restore_vnode(0, 2, captured[0].1.as_deref().unwrap())
        .unwrap();
    let present = unaligned_temporal_archive_transport(captured[1].1.as_deref().unwrap(), 1);
    assert_eq!(
        vnode_checkpoint_alignment_copy_bytes(&present),
        present.len() - 1
    );
    let restore_transport_peak = restored
        .checked_accounted_state_bytes()
        .unwrap()
        .checked_add(present.len())
        .and_then(|bytes| {
            bytes.checked_add(vnode_checkpoint_alignment_copy_charge(&present).unwrap())
        })
        .unwrap();
    restored.set_managed_state_budget(restore_transport_peak - 1);
    assert!(matches!(
        restored.restore_vnode(1, 2, &present),
        Err(DbError::ManagedStateBudgetExceeded { .. })
    ));
    assert!(restored.vnode_states[1].is_none());
    restored.set_managed_state_budget(usize::MAX);
    restored.restore_vnode(1, 2, &present).unwrap();
    assert!(restored.vnode_states[0].is_none());
    assert!(restored.vnode_states[1].is_some());
    restored.force_full_vnode_capture();
    let recaptured = restored
        .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(recaptured.len(), 2);
    assert_eq!(
        recaptured
            .iter()
            .map(|frame| frame.vnode)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert!(recaptured.iter().all(|frame| frame.state.is_some()));

    let left = left_batch(std::slice::from_ref(&key), &["X"], &[100], &[7]);
    let output = restored
        .process_with_frontiers(&[vec![left], Vec::new()], &frontier(200))
        .await
        .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let value = output[0]
        .column(output[0].schema().index_of("value_quotes").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(value.value(0), "live");
}

#[cfg(feature = "cluster")]
async fn two_owner_scope() -> ClusterShuffleConfig {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};

    let registry = Arc::new(VnodeRegistry::new(2));
    registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
    let deadline = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(60)));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    sender.install_process_lease_deadline(deadline).unwrap();
    let fence = CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();
    sender.install_assignment_fence(&fence, &[1, 2]).unwrap();
    receiver.install_assignment_fence(&fence, &[1, 2]).unwrap();
    ClusterShuffleConfig {
        registry,
        sender,
        receiver,
        self_id: NodeId(1),
    }
}

#[cfg(feature = "cluster")]
fn install_all_local_assignment(scope: &ClusterShuffleConfig) -> CheckpointAssignmentFence {
    use laminar_core::checkpoint::CheckpointParticipant;

    let version = scope.registry.assignment_version() + 1;
    let owners = [1_u64, 1];
    let fence = CheckpointAssignmentFence::from_owner_map(
        version,
        &owners,
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(1),
        }],
    )
    .unwrap();
    scope
        .sender
        .install_assignment_fence(&fence, &owners)
        .unwrap();
    scope
        .receiver
        .install_assignment_fence(&fence, &owners)
        .unwrap();
    scope
        .registry
        .set_assignment_and_version(Arc::from(owners.map(NodeId)), version);
    fence
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn fresh_owner_installs_portable_cut_and_bootstrap_topology() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    use crate::operator_graph::{ManagedVnodeRestore, ManagedWholeRestore};

    let key = key_for_vnode(0);
    let cut = frontier(100);
    let (mut donor, _, _) = operator(8);
    donor
        .process_with_frontiers(
            &[
                Vec::new(),
                vec![right_batch(
                    std::slice::from_ref(&key),
                    &["X"],
                    &[90],
                    &["live"],
                    &[SourceMutation::Put],
                )],
            ],
            &cut,
        )
        .await
        .unwrap();
    while !donor.wants_input() {
        donor.process_with_frontiers(&[], &cut).await.unwrap();
    }
    let captured = donor.checkpoint_vnodes(&[0], 2, u64::MAX).unwrap().unwrap();
    let vnode_frame = materialize_capture(captured.into_iter().next().unwrap().state.unwrap());
    let vnode_frame = unaligned_temporal_archive_transport(&vnode_frame, 1);

    let scope = two_owner_scope().await;
    let target_version = scope.registry.assignment_version() + 1;
    let target_fence = CheckpointAssignmentFence::from_owner_map(
        target_version,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();
    scope
        .sender
        .install_assignment_fence(&target_fence, &[1, 2])
        .unwrap();
    scope
        .receiver
        .install_assignment_fence(&target_fence, &[1, 2])
        .unwrap();
    scope
        .registry
        .set_assignment_and_version(Arc::from([NodeId(1), NodeId(2)]), target_version);

    let predecessor = CheckpointAssignmentFence::from_owner_map(
        target_version - 1,
        &[2, 3],
        vec![
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
            CheckpointParticipant {
                node_id: 3,
                boot_incarnation: uuid::Uuid::from_u128(3),
            },
        ],
    )
    .unwrap();
    let predecessor_registry = VnodeRegistry::new(2);
    predecessor_registry.set_assignment_and_version(
        Arc::from([NodeId(2), NodeId(3)]),
        predecessor.assignment_version,
    );

    let (mut target, _, _) = operator(8);
    target.attach_cluster_shuffle(scope.clone());
    target.local_assignment = predecessor_registry.versioned_snapshot();
    target.cluster_peers = Arc::from([2_u64, 3]);
    for port in 0..2 {
        target.peer_channels[port].entry(3).or_default();
    }

    let encode_whole = |participant_id: u64, peer: u64, queued: bool, published_watermark: i64| {
        let channel = |side: usize| TemporalCheckpointChannel {
            peer,
            applied: cut[side].into(),
            events: if queued && side == 0 {
                vec![TemporalCheckpointEvent::Frontier {
                    recovery_gen: 0,
                    frontier: cut[side].into(),
                }]
            } else {
                Vec::new()
            },
            positioned_ipc: Vec::new(),
            mutation_ipc: Vec::new(),
        };
        let checkpoint = TemporalJoinOperatorCheckpoint {
            version: OPERATOR_CHECKPOINT_VERSION,
            frontiers: cut.map(Into::into),
            maintenance_cursor: 0,
            maintenance_pending: false,
            maintenance_remaining: 0,
            maintenance_rescan: false,
            published_output_frontier: Some(
                InputFrontier {
                    watermark: Some(published_watermark),
                    idle: participant_id == 3,
                }
                .into(),
            ),
            cluster: Some(TemporalClusterCheckpoint {
                assignment_version: predecessor.assignment_version,
                owner_map_digest: predecessor.assignment_digest,
                self_id: participant_id,
                local_frontiers: cut.map(Into::into),
                remote_peer_cursors: [None; 2],
                channels: [vec![channel(0)], vec![channel(1)]],
            }),
        };
        rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .unwrap()
            .to_vec()
    };
    let restores = [ManagedVnodeRestore {
        participant_id: 2,
        vnode: 0,
        state: vnode_frame.as_ref(),
    }];

    let absent_vnode = [ABSENT_VNODE];
    let multi_donor_restores = [
        ManagedVnodeRestore {
            participant_id: 2,
            vnode: 0,
            state: vnode_frame.as_ref(),
        },
        ManagedVnodeRestore {
            participant_id: 3,
            vnode: 1,
            state: &absent_vnode,
        },
    ];
    let donor2_frame = encode_whole(2, 3, false, 80);
    let donor3_frame = encode_whole(3, 2, false, 60);
    let multi_donor_whole = [
        ManagedWholeRestore {
            participant_id: 2,
            state: &donor2_frame,
        },
        ManagedWholeRestore {
            participant_id: 3,
            state: &donor3_frame,
        },
    ];
    let merged = target
        .portable_handoff_cut(
            &ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &multi_donor_restores,
                whole_restores: &multi_donor_whole,
                mode: ManagedVnodeTransitionMode::Live,
            },
            true,
        )
        .unwrap()
        .unwrap();
    assert_eq!(merged.frontiers, cut);
    assert_eq!(
        merged.published_output_frontier,
        Some(InputFrontier {
            watermark: Some(60),
            idle: false,
        })
    );

    let queued_frame = encode_whole(2, 3, true, 100);
    let queued_whole = [ManagedWholeRestore {
        participant_id: 2,
        state: &queued_frame,
    }];
    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &queued_whole,
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap_err();
    assert_eq!(target.frontiers, [InputFrontier::default(); 2]);
    assert!(target.vnode_states.iter().all(Option::is_none));

    let whole_frame = encode_whole(2, 3, false, 100);
    let whole_frame = unaligned_temporal_archive_transport(&whole_frame, 0);
    let whole_restores = [ManagedWholeRestore {
        participant_id: 2,
        state: &whole_frame,
    }];
    let decoded_bound = target
        .preflight_whole_checkpoint_archive(&whole_frame, "test donor", |_| Ok(()))
        .unwrap()
        .decoded_checkpoint;
    let raw_restore_bytes = vnode_frame.len().checked_add(whole_frame.len()).unwrap();
    let sequential_peak = vnode_checkpoint_alignment_copy_charge(&vnode_frame)
        .unwrap()
        .max(
            checkpoint_alignment_copy_charge(&whole_frame)
                .unwrap()
                .checked_add(decoded_bound)
                .unwrap(),
        );
    let restore_peak = target
        .checked_accounted_state_bytes()
        .unwrap()
        .checked_add(raw_restore_bytes)
        .and_then(|bytes| bytes.checked_add(sequential_peak))
        .unwrap();
    target.set_managed_state_budget(restore_peak - 1);
    assert!(matches!(
        target.prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::Live,
        }),
        Err(DbError::ManagedStateBudgetExceeded { .. })
    ));
    assert!(target.prepared_vnode_transition.is_none());
    target.set_managed_state_budget(usize::MAX);
    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    assert_eq!(target.frontiers, [InputFrontier::default(); 2]);
    target.publish_vnode_transition();
    assert_eq!(target.frontiers, cut);
    assert_eq!(target.local_frontiers, cut);
    assert_eq!(target.published_output_frontier, Some(cut[0]));
    assert_eq!(target.cluster_peers.as_ref(), &[2]);
    for channel in target.peer_channels.iter().flat_map(BTreeMap::values) {
        assert_eq!(channel.applied, cut[0]);
        assert_eq!(channel.accepted, cut[0]);
        assert!(channel.events.is_empty());
    }
    assert_eq!(target.last_broadcasts, [InputFrontier::default(); 2]);
    assert!(target.checkpoint_drain_pending());
    target.finish_vnode_transition();

    let predecessor_owners = [NodeId(2), NodeId(3)];
    let (mut bootstrap, _, _) = operator(8);
    bootstrap.attach_cluster_shuffle(scope);
    bootstrap
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                predecessor_owners: &predecessor_owners,
            },
        })
        .unwrap();
    bootstrap.publish_vnode_transition();
    assert_eq!(bootstrap.frontiers, cut);
    assert_eq!(bootstrap.local_frontiers, cut);
    assert_eq!(bootstrap.published_output_frontier, Some(cut[0]));
    assert_eq!(
        bootstrap.vnode_states[0]
            .as_ref()
            .unwrap()
            .retained_versions(),
        1
    );
    bootstrap.finish_vnode_transition();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn zero_owner_transition_publishes_remote_topology_without_state() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let scope = two_owner_scope().await;
    let predecessor_version = scope.registry.assignment_version();
    let target_version = predecessor_version + 1;
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        predecessor_version,
        &[2, 2],
        vec![CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(2),
        }],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        target_version,
        &[3, 3],
        vec![CheckpointParticipant {
            node_id: 3,
            boot_incarnation: uuid::Uuid::from_u128(3),
        }],
    )
    .unwrap();

    let (mut operator, _, _) = operator(8);
    operator.attach_cluster_shuffle(scope.clone());
    let predecessor_registry = VnodeRegistry::new_unassigned(2);
    predecessor_registry
        .set_assignment_and_version(Arc::from([NodeId(2), NodeId(2)]), predecessor_version);
    operator.local_assignment = predecessor_registry.versioned_snapshot();

    scope
        .registry
        .set_assignment_and_version(Arc::from([NodeId(3), NodeId(3)]), target_version);
    scope.sender.invalidate_assignment_fence();
    scope.receiver.invalidate_assignment_fence();

    operator
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &[],
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    assert_eq!(operator.local_assignment.version(), predecessor_version);
    assert_eq!(operator.cluster_peers.as_ref(), &[2]);
    assert!(operator.vnode_states.iter().all(Option::is_none));

    operator.publish_vnode_transition();

    assert_eq!(operator.local_assignment.version(), target_version);
    assert_eq!(operator.local_assignment.owners(), &[NodeId(3), NodeId(3)]);
    assert_eq!(operator.cluster_peers.as_ref(), &[3]);
    assert!(operator
        .peer_channels
        .iter()
        .all(|channels| { channels.len() == 1 && channels.contains_key(&3) }));
    assert!(operator.resident_vnodes.is_empty());
    assert!(operator.vnode_states.iter().all(Option::is_none));
    operator.finish_vnode_transition();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn vnode_transition_is_atomic_and_publishes_target_topology() {
    use laminar_core::checkpoint::CheckpointParticipant;
    use laminar_core::shuffle::ShuffleMessage;

    use crate::operator_graph::ManagedVnodeRestore;

    let key = key_for_vnode(1);
    let cut = frontier(100);
    let (mut donor, _, _) = operator(8);
    donor
        .process_with_frontiers(
            &[
                Vec::new(),
                vec![right_batch(
                    std::slice::from_ref(&key),
                    &["X"],
                    &[90],
                    &["live"],
                    &[SourceMutation::Put],
                )],
            ],
            &cut,
        )
        .await
        .unwrap();
    while !donor.wants_input() {
        donor.process_with_frontiers(&[], &cut).await.unwrap();
    }
    let captured = donor.checkpoint_vnodes(&[1], 2, u64::MAX).unwrap().unwrap();
    let frame = materialize_capture(captured.into_iter().next().unwrap().state.unwrap());

    let (mut target, _, _) = operator(8);
    let scope = two_owner_scope().await;
    target.attach_cluster_shuffle(scope.clone());
    target.frontiers = cut;
    target.local_frontiers = cut;
    target.last_broadcasts = cut;
    for channel in target
        .peer_channels
        .iter_mut()
        .flat_map(BTreeMap::values_mut)
    {
        channel.applied = cut[0];
        channel.accepted = cut[0];
    }
    let predecessor_version = target.local_assignment.version();
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        predecessor_version,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();
    let fence = install_all_local_assignment(&scope);
    let corrupt = [PRESENT_VNODE, 0xff];
    let corrupt_restore = [ManagedVnodeRestore {
        participant_id: 2,
        vnode: 1,
        state: &corrupt,
    }];
    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &corrupt_restore,
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap_err();
    assert_eq!(target.local_assignment.version(), predecessor_version);
    assert!(target.vnode_states.iter().all(Option::is_none));
    assert_eq!(target.cluster_peers.as_ref(), &[2]);

    let restores = [ManagedVnodeRestore {
        participant_id: 2,
        vnode: 1,
        state: frame.as_ref(),
    }];
    let send = tokio::spawn(std::future::pending::<(
        Result<(), DbError>,
        Option<Vec<(u64, ShuffleMessage)>>,
    )>());
    target.pending_cluster_input = Some(PendingTemporalClusterInput {
        routed: BTreeMap::new(),
        outbound: None,
        local_frontiers: cut,
        send: Some(send),
        accounted_bytes: 0,
    });
    let error = target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap_err();
    assert!(error.to_string().contains("drained frontier"));
    target.pending_cluster_input.take();

    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    assert!(target.managed_state_accounting().unwrap().prepared > 0);
    target.abort_vnode_transition();
    assert!(target.vnode_states.iter().all(Option::is_none));
    target.finish_vnode_transition();

    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    target.publish_vnode_transition();
    assert_eq!(target.local_assignment.version(), fence.assignment_version);
    assert_eq!(target.resident_vnodes, [1]);
    assert_eq!(
        target.vnode_states[1].as_ref().unwrap().retained_versions(),
        1
    );
    assert!(target.cluster_peers.is_empty());
    assert!(target.managed_state_accounting().unwrap().retired > 0);
    target.finish_vnode_transition();
    assert!(!target.checkpoint_drain_pending());
    assert!(target.wants_input());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_plan_is_atomic_and_orders_idle_revival_data_and_frontier() {
    use laminar_core::shuffle::ShuffleMessage;

    let key = key_for_vnode(1);
    let (mut operator, _, _) = operator(8);
    let scope = two_owner_scope().await;
    operator.attach_cluster_shuffle(scope.clone());
    let idle = InputFrontier {
        watermark: Some(100),
        idle: true,
    };
    operator.local_frontiers = [idle; 2];
    operator.frontiers = [InputFrontier {
        watermark: Some(200),
        idle: false,
    }; 2];
    for port in 0..2 {
        let channel = operator.peer_channels[port].get_mut(&2).unwrap();
        channel.applied = idle;
        channel.accepted = idle;
    }
    operator.last_broadcasts = [idle; 2];
    let left = left_batch(std::slice::from_ref(&key), &["X"], &[210], &[7]);
    let right = right_batch(
        std::slice::from_ref(&key),
        &["X"],
        &[205],
        &["live"],
        &[SourceMutation::Put],
    );
    let active = [InputFrontier {
        watermark: Some(150),
        idle: false,
    }; 2];
    let assignment = scope.registry.versioned_snapshot();
    let plan = operator
        .plan_cluster_inputs(
            &[vec![left], vec![right]],
            active,
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    assert_eq!(plan.outbound.len(), 6);
    let expected = [
        ("temporal::right", "frontier", Some(100)),
        ("temporal::right", "data", None),
        ("temporal::right", "frontier", Some(200)),
        ("temporal::left", "frontier", Some(100)),
        ("temporal::left", "data", None),
        ("temporal::left", "frontier", Some(200)),
    ];
    for ((peer, message), (stage, kind, watermark)) in plan.outbound.iter().zip(expected) {
        assert_eq!(*peer, 2);
        match (kind, message) {
            (
                "frontier",
                ShuffleMessage::Frontier {
                    stage: actual,
                    watermark: actual_watermark,
                    idle: false,
                },
            ) => {
                assert_eq!(actual, stage);
                assert_eq!(*actual_watermark, watermark);
            }
            ("data", ShuffleMessage::Data { stage: actual, .. }) => {
                assert_eq!(actual, stage);
            }
            _ => panic!("unexpected temporal shuffle order"),
        }
    }
    operator.local_frontiers = plan.local_frontiers;
    operator.last_broadcasts = [idle; 2];
    let revived_without_data = operator
        .plan_cluster_inputs(
            &[Vec::new(), Vec::new()],
            operator.local_frontiers,
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    assert_eq!(revived_without_data.outbound.len(), 2);
    assert!(revived_without_data.outbound.iter().all(|(_, message)| {
        matches!(
            message,
            ShuffleMessage::Frontier {
                watermark: Some(200),
                idle: false,
                ..
            }
        )
    }));
    operator.last_broadcasts = revived_without_data.local_frontiers;
    let unchanged = operator
        .plan_cluster_inputs(
            &[Vec::new(), Vec::new()],
            plan.local_frontiers,
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    assert!(unchanged.outbound.is_empty());

    let invalid_left = RecordBatch::new_empty(Arc::new(Schema::empty()));
    assert!(operator
        .plan_cluster_inputs(
            &[vec![invalid_left], Vec::new()],
            plan.local_frontiers,
            &scope,
            &assignment,
            &[2],
        )
        .is_err());

    operator.local_frontiers[0] = InputFrontier {
        watermark: Some(300),
        idle: false,
    };
    operator.frontiers[0] = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let late = left_batch(std::slice::from_ref(&key), &["X"], &[250], &[8]);
    assert!(operator
        .plan_cluster_inputs(
            &[vec![late], Vec::new()],
            operator.local_frontiers,
            &scope,
            &assignment,
            &[2],
        )
        .is_err());

    let nullable = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["X"])),
            Arc::new(TimestampMillisecondArray::from(vec![None])),
        ],
    )
    .unwrap();
    assert!(operator
        .validate_batch_lateness(
            TemporalInputSide::Left,
            &nullable,
            operator.local_frontiers[0],
            false,
        )
        .is_err());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn bootstrap_broadcast_holds_restored_cut_ahead_of_live_replay_frontier() {
    use laminar_core::shuffle::ShuffleMessage;

    let key = key_for_vnode(1);
    let (mut operator, _, _) = operator(8);
    let scope = two_owner_scope().await;
    operator.attach_cluster_shuffle(scope.clone());
    let cut = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let live = InputFrontier {
        watermark: Some(300),
        idle: false,
    };
    operator.frontiers = [cut; 2];
    operator.local_frontiers = [cut; 2];
    operator.last_broadcasts = [InputFrontier::default(); 2];
    for channels in &mut operator.peer_channels {
        let channel = channels.get_mut(&2).unwrap();
        channel.applied = cut;
        channel.accepted = cut;
    }
    let assignment = scope.registry.versioned_snapshot();

    assert!(!operator.wants_input());
    let bootstrap = operator
        .plan_cluster_inputs(
            &[Vec::new(), Vec::new()],
            [live; 2],
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    assert_eq!(bootstrap.local_frontiers, [cut; 2]);
    assert_eq!(bootstrap.outbound.len(), 2);
    assert!(bootstrap.outbound.iter().all(|(_, message)| {
        matches!(
            message,
            ShuffleMessage::Frontier {
                watermark: Some(100),
                idle: false,
                ..
            }
        )
    }));

    let replay = right_batch(
        std::slice::from_ref(&key),
        &["X"],
        &[150],
        &["replayed"],
        &[SourceMutation::Put],
    );
    let error = match operator.plan_cluster_inputs(
        &[Vec::new(), vec![replay.clone()]],
        [live; 2],
        &scope,
        &assignment,
        &[2],
    ) {
        Err(error) => error,
        Ok(_) => panic!("local replay must wait for the restored frontier broadcast"),
    };
    assert!(
        error
            .to_string()
            .contains("before its restored frontier was broadcast"),
        "{error}"
    );

    operator.last_broadcasts = [cut; 2];
    let replay = operator
        .plan_cluster_inputs(
            &[Vec::new(), vec![replay]],
            [live; 2],
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    assert_eq!(replay.local_frontiers, [live; 2]);
    assert_eq!(replay.outbound.len(), 3);
    assert!(matches!(
        &replay.outbound[0].1,
        ShuffleMessage::Data { stage, .. } if stage == "temporal::right"
    ));
    assert!(matches!(
        &replay.outbound[1].1,
        ShuffleMessage::Frontier {
            stage,
            watermark: Some(300),
            idle: false,
        } if stage == "temporal::right"
    ));
    assert!(matches!(
        &replay.outbound[2].1,
        ShuffleMessage::Frontier {
            stage,
            watermark: Some(300),
            idle: false,
        } if stage == "temporal::left"
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn zero_admission_send_restarts_once_without_becoming_runnable() {
    use laminar_core::shuffle::ShuffleMessage;

    let (mut operator, _, _) = operator(8);
    let scope = two_owner_scope().await;
    operator.initialize_managed_state().await.unwrap();
    operator.attach_cluster_shuffle(scope);

    let retry_plan = vec![(
        2,
        ShuffleMessage::Frontier {
            stage: "temporal::left".to_owned(),
            watermark: None,
            idle: false,
        },
    )];
    let send = tokio::spawn(async move {
        (
            Err(DbError::ShuffleNotReady("injected zero admission".into())),
            Some(retry_plan),
        )
    });
    operator.pending_cluster_input = Some(PendingTemporalClusterInput {
        routed: BTreeMap::new(),
        outbound: None,
        local_frontiers: [InputFrontier::default(); 2],
        send: Some(send),
        accounted_bytes: 0,
    });

    while !operator
        .pending_cluster_input
        .as_ref()
        .unwrap()
        .send
        .as_ref()
        .unwrap()
        .is_finished()
    {
        tokio::task::yield_now().await;
    }
    assert!(!operator.deferred_work_is_runnable());

    let output = operator
        .process_cluster(&[Vec::new(), Vec::new()], [InputFrontier::default(); 2])
        .await
        .unwrap();
    assert!(output.is_empty());
    let pending = operator.pending_cluster_input.as_ref().unwrap();
    assert!(pending.send.is_some());
    assert!(pending.outbound.is_none());
    assert!(!operator.deferred_work_is_runnable());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn pending_send_drains_remote_history_before_local_probe() {
    use laminar_core::shuffle::ShuffleMessage;

    let local_key = key_for_vnode(0);
    let outbound_key = key_for_vnode(1);
    let (mut operator, _, _) = operator(8);
    let scope = two_owner_scope().await;
    operator.initialize_managed_state().await.unwrap();
    operator.attach_cluster_shuffle(scope.clone());
    let close = frontier(300);
    let assignment = scope.registry.versioned_snapshot();
    let plan = operator
        .plan_cluster_inputs(
            &[
                vec![
                    left_batch(std::slice::from_ref(&local_key), &["X"], &[220], &[7]),
                    left_batch(std::slice::from_ref(&outbound_key), &["X"], &[220], &[8]),
                ],
                Vec::new(),
            ],
            close,
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    assert!(plan.routed.contains_key(&0));
    assert!(plan
        .outbound
        .iter()
        .any(|(_, message)| matches!(message, ShuffleMessage::Data { .. })));
    let accounted_bytes = operator.cluster_input_plan_bytes(&plan).unwrap();
    let ClusterInputPlan {
        routed,
        outbound,
        local_frontiers,
        effective_frontiers: _,
    } = plan;
    let baseline = operator.managed_state_accounting().unwrap().live;
    let (release, wait) = tokio::sync::oneshot::channel();
    let send = tokio::spawn(async move {
        let _ = wait.await;
        drop(outbound);
        (Ok(()), None)
    });
    operator.pending_cluster_input = Some(PendingTemporalClusterInput {
        routed,
        outbound: None,
        local_frontiers,
        send: Some(send),
        accounted_bytes,
    });

    let assignment_version = scope.registry.assignment_version();
    let recovery_gen = scope.receiver.recovery_gen();
    operator
        .stage_checkpointed_shuffle(
            "temporal::right",
            crate::operator::RetainedBatch::restored_channel(
                right_batch(
                    std::slice::from_ref(&local_key),
                    &["X"],
                    &[210],
                    &["live"],
                    &[SourceMutation::Put],
                ),
                2,
                assignment_version,
                recovery_gen,
                Arc::from([0_u32]),
            ),
            i64::MIN,
        )
        .unwrap();
    operator
        .stage_checkpointed_shuffle_frontier(
            "temporal::right",
            2,
            close[1],
            assignment_version,
            recovery_gen,
        )
        .unwrap();
    operator
        .stage_checkpointed_shuffle_frontier(
            "temporal::left",
            2,
            close[0],
            assignment_version,
            recovery_gen,
        )
        .unwrap();
    assert!(operator.deferred_work_is_runnable());
    assert!(operator.managed_state_accounting().unwrap().live >= baseline + accounted_bytes);

    let output = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        operator.process_cluster(&[Vec::new(), Vec::new()], [InputFrontier::default(); 2]),
    )
    .await
    .expect("pending temporal send blocked the graph task")
    .unwrap();
    assert!(output.is_empty());
    assert_eq!(operator.queued_remote_events, 0);
    let local_state = operator.vnode_states[0].as_ref().unwrap();
    assert_eq!(local_state.retained_versions(), 1);
    assert_eq!(local_state.pending_probes(), 0);
    assert_eq!(operator.frontiers, [InputFrontier::default(); 2]);
    assert_eq!(operator.local_frontiers, [InputFrontier::default(); 2]);
    assert_eq!(operator.last_broadcasts, [InputFrontier::default(); 2]);
    assert!(operator.pending_cluster_input.is_some());
    assert!(!operator.wants_input());
    assert!(!operator.deferred_work_is_runnable());
    assert!(operator.capture_operator_checkpoint(usize::MAX).is_err());
    assert!(operator.checkpoint_vnodes(&[0], 2, u64::MAX).is_err());

    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !operator
            .pending_cluster_input
            .as_ref()
            .unwrap()
            .send
            .as_ref()
            .unwrap()
            .is_finished()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending temporal send task did not finish");
    let output = operator
        .process_cluster(&[Vec::new(), Vec::new()], [InputFrontier::default(); 2])
        .await
        .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let ids = output[0]
        .column_by_name("trade_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = output[0]
        .column_by_name("value_quotes")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), 7);
    assert_eq!(values.value(0), "live");
    assert!(operator.pending_cluster_input.is_none());
    assert_eq!(operator.local_frontiers, close);
    assert_eq!(operator.last_broadcasts, close);
    assert_eq!(operator.frontiers, close);

    let output = operator
        .process_cluster(&[Vec::new(), Vec::new()], close)
        .await
        .unwrap();
    assert!(output.is_empty());
    assert!(operator.wants_input());
    assert!(!operator.checkpoint_drain_pending());
}

#[cfg(feature = "cluster")]
async fn queued_cluster_checkpoint() -> (OperatorCheckpoint, ClusterShuffleConfig) {
    let key = key_for_vnode(0);
    let (mut operator, _, _) = operator(8);
    let scope = two_owner_scope().await;
    operator.attach_cluster_shuffle(scope.clone());
    let applied = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let local = InputFrontier {
        watermark: Some(300),
        idle: false,
    };
    operator.frontiers = [applied; 2];
    operator.local_frontiers = [local; 2];
    operator.last_broadcasts = [local; 2];
    operator.published_output_frontier = Some(InputFrontier {
        watermark: Some(100),
        idle: true,
    });
    for port in 0..2 {
        let channel = operator.peer_channels[port].get_mut(&2).unwrap();
        channel.applied = applied;
        channel.accepted = applied;
    }

    let assignment = scope.registry.assignment_version();
    let recovery = scope.receiver.recovery_gen();
    let stale_right = crate::operator::RetainedBatch::restored_channel(
        right_batch(
            std::slice::from_ref(&key),
            &["X"],
            &[205],
            &["stale"],
            &[SourceMutation::Put],
        ),
        2,
        assignment,
        recovery,
        Arc::from([0_u32]),
    );
    operator
        .stage_checkpointed_shuffle("temporal::right", stale_right, 100)
        .unwrap();
    let removed_right = crate::operator::RetainedBatch::restored_channel(
        right_batch_at(
            std::slice::from_ref(&key),
            &["X"],
            &[207],
            &["deleted"],
            &[SourceMutation::Tombstone],
            2,
        ),
        2,
        assignment,
        recovery,
        Arc::from([0_u32]),
    );
    operator
        .stage_checkpointed_shuffle("temporal::right", removed_right, 100)
        .unwrap();
    let right = crate::operator::RetainedBatch::restored_channel(
        right_batch_at(
            std::slice::from_ref(&key),
            &["X"],
            &[210],
            &["live"],
            &[SourceMutation::Put],
            3,
        ),
        2,
        assignment,
        recovery,
        Arc::from([0_u32]),
    );
    operator
        .stage_checkpointed_shuffle("temporal::right", right, 100)
        .unwrap();
    operator
        .stage_checkpointed_shuffle_frontier(
            "temporal::right",
            2,
            InputFrontier {
                watermark: Some(250),
                idle: false,
            },
            assignment,
            recovery,
        )
        .unwrap();
    let left = crate::operator::RetainedBatch::restored_channel(
        left_batch(std::slice::from_ref(&key), &["X"], &[220], &[7]),
        2,
        assignment,
        recovery,
        Arc::from([0_u32]),
    );
    operator
        .stage_checkpointed_shuffle("temporal::left", left, 100)
        .unwrap();
    operator
        .stage_checkpointed_shuffle_frontier(
            "temporal::left",
            2,
            InputFrontier {
                watermark: Some(250),
                idle: false,
            },
            assignment,
            recovery,
        )
        .unwrap();
    operator.remote_peer_cursors = [Some(2); 2];
    let capture = operator
        .checkpoint_capture(u64::MAX)
        .unwrap()
        .expect("queued cluster state must capture");
    let local = operator.local_frontiers;
    operator.process_with_frontiers(&[], &local).await.unwrap();
    assert_eq!(operator.queued_remote_events, 0);
    let data = materialize_capture(capture).to_vec();
    (OperatorCheckpoint { data }, scope)
}

#[cfg(feature = "cluster")]
fn assert_cluster_restore_pristine(operator: &ManagedTemporalJoinOperator) {
    assert_eq!(operator.whole_restore, WholeRestoreState::Pending);
    assert_eq!(operator.frontiers, [InputFrontier::default(); 2]);
    assert_eq!(operator.local_frontiers, [InputFrontier::default(); 2]);
    assert_eq!(operator.last_broadcasts, [InputFrontier::default(); 2]);
    assert_eq!(operator.remote_peer_cursors, [None; 2]);
    assert!(operator.published_output_frontier.is_none());
    assert_eq!(operator.queued_shuffle_bytes, 0);
    assert_eq!(operator.queued_remote_events, 0);
    assert_eq!(operator.queued_event_capacity_bytes, 0);
    assert!(operator.peer_channels.iter().flatten().all(|(_, channel)| {
        channel.applied == InputFrontier::default()
            && channel.accepted == InputFrontier::default()
            && channel.events.is_empty()
    }));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_channel_checkpoint_round_trip_preserves_order_and_replay() {
    let (checkpoint, scope) = queued_cluster_checkpoint().await;
    let checkpoint_data = checkpoint.data.clone();
    let (mut tight, _, _) = operator(8);
    tight.attach_cluster_shuffle(scope.clone());
    let preflight = tight
        .preflight_whole_checkpoint_archive(&checkpoint_data, "tight queued checkpoint", |_| Ok(()))
        .unwrap();
    assert!(preflight.runtime_scratch > 0);
    let live_bytes = tight.checked_accounted_state_bytes().unwrap();
    let runtime_peak = live_bytes
        .checked_add(preflight.decoded_checkpoint)
        .and_then(|bytes| bytes.checked_add(preflight.runtime_scratch))
        .unwrap();
    tight.set_managed_state_budget(runtime_peak - 1);
    assert!(matches!(
        tight.restore(OperatorCheckpoint {
            data: checkpoint_data.clone(),
        }),
        Err(DbError::ManagedStateBudgetExceeded { .. })
    ));
    assert_cluster_restore_pristine(&tight);

    scope.sender.set_recovery_gen(1);
    scope.receiver.set_recovery_gen(1);
    let (mut restored, _, _) = operator(8);
    restored.attach_cluster_shuffle(scope.clone());
    restored.restore(checkpoint).unwrap();

    assert_eq!(restored.queued_remote_events, 6);
    assert_eq!(restored.remote_peer_cursors, [Some(2); 2]);
    for port in 0..2 {
        let channel = &restored.peer_channels[port][&2];
        assert_eq!(channel.applied.watermark, Some(100));
        assert_eq!(channel.accepted.watermark, Some(250));
    }
    assert!(matches!(
        &restored.peer_channels[0][&2].events[0].payload,
        TemporalRemoteEventPayload::Data(_)
    ));
    assert!(matches!(
        &restored.peer_channels[0][&2].events[1].payload,
        TemporalRemoteEventPayload::Frontier(_)
    ));
    assert!(matches!(
        &restored.peer_channels[1][&2].events[0].payload,
        TemporalRemoteEventPayload::Data(_)
    ));
    assert!(matches!(
        &restored.peer_channels[1][&2].events[1].payload,
        TemporalRemoteEventPayload::Data(_)
    ));
    assert!(matches!(
        &restored.peer_channels[1][&2].events[2].payload,
        TemporalRemoteEventPayload::Data(_)
    ));
    assert!(matches!(
        &restored.peer_channels[1][&2].events[3].payload,
        TemporalRemoteEventPayload::Frontier(_)
    ));
    let right_streams = restored.peer_channels[1][&2]
        .events
        .iter()
        .filter_map(|event| match &event.payload {
            TemporalRemoteEventPayload::Data(batch) => Some(batch.mutation_stream),
            TemporalRemoteEventPayload::Frontier(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(right_streams, [false, true, false]);
    assert_eq!(
        restored.restored_output_frontier(),
        Some(InputFrontier {
            watermark: Some(100),
            idle: false,
        })
    );
    assert!(restored.checkpoint().unwrap().is_some());

    let local = restored.local_frontiers;
    let output = restored.process_with_frontiers(&[], &local).await.unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(restored.queued_remote_events, 0);
    assert!(!restored.has_remote_events());
    let value = output[0]
        .column_by_name("value_quotes")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(value.value(0), "live");

    let idle = InputFrontier {
        watermark: Some(100),
        idle: true,
    };
    let channel = restored.peer_channels[1].get_mut(&2).unwrap();
    channel.applied = idle;
    channel.accepted = idle;
    restored.pending_frontiers = Some([
        restored.frontiers[0],
        InputFrontier {
            watermark: Some(500),
            idle: false,
        },
    ]);
    let assignment = scope.registry.assignment_version();
    let recovery = scope.receiver.recovery_gen();
    restored
        .stage_checkpointed_shuffle_frontier(
            "temporal::right",
            2,
            InputFrontier {
                watermark: Some(100),
                idle: false,
            },
            assignment,
            recovery,
        )
        .unwrap();
    assert_eq!(restored.peer_channels[1][&2].accepted.watermark, Some(500));
    let key = key_for_vnode(0);
    let late = crate::operator::RetainedBatch::restored_channel(
        right_batch(
            std::slice::from_ref(&key),
            &["X"],
            &[499],
            &["late"],
            &[SourceMutation::Put],
        ),
        2,
        assignment,
        recovery,
        Arc::from([0_u32]),
    );
    let error = restored
        .stage_checkpointed_shuffle("temporal::right", late, i64::MIN)
        .unwrap_err();
    assert!(error.to_string().contains("applied frontier 500"));
    restored
        .stage_checkpointed_shuffle_frontier(
            "temporal::right",
            2,
            InputFrontier {
                watermark: Some(150),
                idle: false,
            },
            assignment,
            recovery,
        )
        .unwrap();
    assert_eq!(restored.peer_channels[1][&2].accepted.watermark, Some(500));
    restored
        .stage_checkpointed_shuffle_frontier(
            "temporal::right",
            2,
            InputFrontier {
                watermark: Some(550),
                idle: false,
            },
            assignment,
            recovery,
        )
        .unwrap();
    assert_eq!(restored.peer_channels[1][&2].accepted.watermark, Some(550));
    assert!(restored
        .stage_checkpointed_shuffle_frontier(
            "temporal::right",
            2,
            InputFrontier {
                watermark: None,
                idle: false,
            },
            assignment,
            recovery,
        )
        .is_err());
    assert!(restored
        .stage_checkpointed_shuffle_frontier(
            "temporal::right",
            2,
            InputFrontier {
                watermark: Some(525),
                idle: false,
            },
            assignment,
            recovery,
        )
        .is_err());
    assert_eq!(restored.peer_channels[1][&2].accepted.watermark, Some(550));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_checkpoint_restore_rejects_topology_and_routes_atomically() {
    let (checkpoint, scope) = queued_cluster_checkpoint().await;
    let mut wrong_rows =
        rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
            .unwrap();
    let wrong_row_count = wrong_rows
        .cluster
        .as_mut()
        .unwrap()
        .channels
        .iter_mut()
        .flatten()
        .flat_map(|channel| channel.events.iter_mut())
        .find_map(|event| match event {
            TemporalCheckpointEvent::Data { row_count, .. } => Some(row_count),
            TemporalCheckpointEvent::Frontier { .. } => None,
        })
        .unwrap();
    *wrong_row_count = wrong_row_count.checked_add(1).unwrap();
    let wrong_rows = OperatorCheckpoint {
        data: rkyv::to_bytes::<rkyv::rancor::Error>(&wrong_rows)
            .unwrap()
            .to_vec(),
    };
    let (mut restored, _, _) = operator(8);
    restored.attach_cluster_shuffle(scope.clone());
    assert!(restored.restore(wrong_rows).is_err());
    assert_cluster_restore_pristine(&restored);

    let mut wrong_topology =
        rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
            .unwrap();
    wrong_topology.cluster.as_mut().unwrap().owner_map_digest[0] ^= 0xff;
    let wrong_topology = OperatorCheckpoint {
        data: rkyv::to_bytes::<rkyv::rancor::Error>(&wrong_topology)
            .unwrap()
            .to_vec(),
    };
    let (mut restored, _, _) = operator(8);
    restored.attach_cluster_shuffle(scope.clone());
    assert!(restored.restore(wrong_topology).is_err());
    assert_cluster_restore_pristine(&restored);

    let mut wrong_route =
        rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(&checkpoint.data)
            .unwrap();
    let mut changed = false;
    for channel in &mut wrong_route.cluster.as_mut().unwrap().channels[1] {
        for event in &mut channel.events {
            if let TemporalCheckpointEvent::Data { routed_vnodes, .. } = event {
                *routed_vnodes = vec![1];
                changed = true;
                break;
            }
        }
        if changed {
            break;
        }
    }
    assert!(changed);
    let wrong_route = OperatorCheckpoint {
        data: rkyv::to_bytes::<rkyv::rancor::Error>(&wrong_route)
            .unwrap()
            .to_vec(),
    };
    let (mut restored, _, _) = operator(8);
    restored.attach_cluster_shuffle(scope);
    assert!(restored.restore(wrong_route).is_err());
    assert_cluster_restore_pristine(&restored);
}
