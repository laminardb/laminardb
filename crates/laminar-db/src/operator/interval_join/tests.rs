use super::*;
use arrow::array::{
    BinaryArray, Float64Array, StringArray, TimestampMillisecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use laminar_connectors::connector::{
    schema_with_source_mutations_and_row_positions, schema_with_source_row_positions, SourceBatch,
    SourceMutation, SourceRowPositionCapability, SourceRowPositions,
};
use std::time::Duration;

#[cfg(feature = "cluster")]
use arrow::array::{DictionaryArray, Int64Array, Int8Array};
#[cfg(feature = "cluster")]
use arrow::datatypes::Int8Type;

fn materialize_capture(capture: StateFrameCapture) -> Result<bytes::Bytes, DbError> {
    let mut staged_bytes = capture.retained_bytes();
    capture.materialize(&mut staged_bytes, u64::MAX)
}

fn unaligned_checkpoint_transport(
    bytes: &[u8],
    archive_offset: usize,
    archive_alignment: usize,
) -> bytes::Bytes {
    let mut transport = vec![0_u8; bytes.len() + CHECKPOINT_ARCHIVE_ALIGNMENT];
    let base = transport.as_ptr() as usize;
    let offset = (0..CHECKPOINT_ARCHIVE_ALIGNMENT)
        .find(|offset| !(base + offset + archive_offset).is_multiple_of(archive_alignment))
        .expect("an unaligned checkpoint transport offset exists");
    transport[offset..offset + bytes.len()].copy_from_slice(bytes);
    let bytes = bytes::Bytes::from(transport).slice(offset..offset + bytes.len());
    assert_ne!(
        bytes[archive_offset..]
            .as_ptr()
            .align_offset(archive_alignment),
        0
    );
    let archive = &bytes[archive_offset..];
    assert_eq!(
        checkpoint_alignment_copy_bytes(archive),
        archive.len().saturating_add(HEAP_ALLOCATION_CHARGE)
    );
    bytes
}

#[cfg(feature = "cluster")]
async fn single_owner_shuffle(
    vnode_count: u32,
) -> (
    ClusterShuffleConfig,
    laminar_core::checkpoint::CheckpointAssignmentFence,
) {
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::state::{NodeId, VnodeRegistry};

    let self_id = NodeId(1);
    let incarnation = uuid::Uuid::from_u128(1);
    let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
    registry.set_assignment(vec![self_id; vnode_count as usize].into());
    let receiver = Arc::new(
        laminar_core::shuffle::ShuffleReceiver::bind(
            self_id.0,
            "127.0.0.1:0".parse().unwrap(),
            incarnation,
        )
        .await
        .unwrap(),
    );
    let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
        self_id.0,
        incarnation,
    ));
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    sender.install_process_lease_deadline(deadline).unwrap();

    let owners = vec![self_id.0; usize::try_from(vnode_count).unwrap()];
    let fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &owners,
        vec![laminar_core::checkpoint::CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: incarnation,
        }],
    )
    .unwrap();
    sender.install_assignment_fence(&fence, &owners).unwrap();
    receiver.install_assignment_fence(&fence, &owners).unwrap();

    (
        ClusterShuffleConfig {
            registry,
            sender,
            receiver,
            self_id,
        },
        fence,
    )
}

#[cfg(feature = "cluster")]
fn install_single_owner_predecessor(
    operator: &mut IntervalJoinOperator,
    target: &laminar_core::checkpoint::CheckpointAssignmentFence,
) -> laminar_core::checkpoint::CheckpointAssignmentFence {
    let owners = vec![1; target.vnode_count as usize];
    let registry = VnodeRegistry::single_owner(target.vnode_count, laminar_core::state::NodeId(1));
    let predecessor_version = target.assignment_version - 1;
    if predecessor_version > registry.assignment_version() {
        registry.set_assignment_and_version(
            vec![laminar_core::state::NodeId(1); target.vnode_count as usize].into(),
            predecessor_version,
        );
    }
    operator.local_assignment = registry.versioned_snapshot();
    laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
        target.assignment_version - 1,
        &owners,
        target.participants.clone(),
    )
    .unwrap()
}

#[cfg(feature = "cluster")]
async fn two_owner_shuffle() -> (
    ClusterShuffleConfig,
    Arc<laminar_core::shuffle::ShuffleReceiver>,
) {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let registry = Arc::new(VnodeRegistry::new(2));
    registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
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
    let local_receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
            .await
            .unwrap(),
    );
    let remote_receiver = Arc::new(
        ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
    sender.register_peer(2, remote_receiver.local_addr());
    let local_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    local_receiver
        .install_process_lease_deadline(Arc::clone(&local_deadline))
        .unwrap();
    sender
        .install_process_lease_deadline(local_deadline)
        .unwrap();
    remote_receiver
        .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    local_receiver
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();
    remote_receiver
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();
    sender.install_assignment_fence(&fence, &[1, 2]).unwrap();

    (
        ClusterShuffleConfig {
            registry,
            sender,
            receiver: local_receiver,
            self_id: NodeId(1),
        },
        remote_receiver,
    )
}

fn test_config() -> StreamJoinConfig {
    StreamJoinConfig {
        join_type: laminar_sql::parser::join_parser::JoinType::Inner,
        left_keys: vec!["id".to_string()],
        right_keys: vec!["id".to_string()],
        left_time_column: "ts".to_string(),
        right_time_column: "ts".to_string(),
        left_table: "left_stream".to_string(),
        right_table: "right_stream".to_string(),
        time_bound: Duration::from_millis(100),
    }
}

#[cfg(feature = "cluster")]
#[test]
fn interval_shuffle_wrappers_preserve_terminal_disposition() {
    fn assert_terminal(error: DbError, expected: &str) {
        let DbError::ShuffleTerminal(reason) = error else {
            panic!("expected permanent shuffle halt, got {error}");
        };
        assert_eq!(reason, expected);
    }

    let operator =
        IntervalJoinOperator::new("test_interval", test_config(), None, SessionContext::new());
    assert_terminal(
        operator.remote_replay_error(DbError::ShuffleTerminal("remote replay".into())),
        "remote replay",
    );
    assert_terminal(
        operator.outbound_finalize_error(DbError::ShuffleTerminal("outbound".into())),
        "outbound",
    );
}

#[tokio::test]
async fn invalid_post_projection_fails_before_eager_or_lazy_interval_state() {
    let mut operator = IntervalJoinOperator::new(
        "invalid-projection",
        test_config(),
        Some(Arc::from("SELECT missing_column FROM __interval_tmp")),
        SessionContext::new(),
    );
    operator.set_input_schemas(
        left_batch(&[], &[], &[]).schema(),
        right_batch(&[], &[], &[]).schema(),
    );

    let error = operator
        .initialize_managed_state()
        .await
        .expect_err("invalid projection must fail during startup initialization");

    assert!(error.to_string().contains("missing_column"), "{error}");
    assert!(operator.vnode_states.iter().all(Option::is_none));
    assert!(operator.resident_vnodes.is_empty());

    let mut live_operator = IntervalJoinOperator::new(
        "invalid-live-projection",
        test_config(),
        Some(Arc::from("SELECT missing_column FROM __interval_tmp")),
        SessionContext::new(),
    );
    live_operator.set_input_schemas(
        left_batch(&[], &[], &[]).schema(),
        right_batch(&[], &[], &[]).schema(),
    );
    let error = live_operator
        .process_with_frontiers(
            &[Vec::new(), Vec::new()],
            &[
                InputFrontier {
                    watermark: Some(100),
                    idle: true,
                },
                InputFrontier {
                    watermark: Some(200),
                    idle: true,
                },
            ],
        )
        .await
        .expect_err("live projection must initialize before an empty cycle can advance state");

    assert!(error.to_string().contains("missing_column"), "{error}");
    assert!(live_operator.vnode_states.iter().all(Option::is_none));
    assert!(live_operator.resident_vnodes.is_empty());
    assert_eq!(live_operator.applied_left_watermark, i64::MIN);
    assert_eq!(live_operator.applied_right_watermark, i64::MIN);
}

#[tokio::test]
async fn lazy_projection_initializes_before_first_interval_cycle() {
    let mut operator = IntervalJoinOperator::new(
        "live-projection",
        test_config(),
        Some(Arc::from("SELECT id AS projected_id FROM __interval_tmp")),
        laminar_sql::create_session_context(),
    );
    operator.set_input_schemas(
        left_batch(&[], &[], &[]).schema(),
        right_batch(&[], &[], &[]).schema(),
    );

    let output = operator
        .process(
            &[
                vec![left_batch(&["A"], &[100], &[10.0])],
                vec![right_batch(&["A"], &[110], &[1.0])],
            ],
            &[0, 0],
        )
        .await
        .unwrap();

    assert!(operator.projection.is_initialized());
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(output[0].schema().field(0).name(), "projected_id");
    assert!(operator.vnode_states.iter().any(Option::is_some));
}

fn unconstrained_frontier() -> InputFrontier {
    InputFrontier {
        watermark: Some(i64::MAX),
        idle: true,
    }
}

#[test]
fn output_frontier_uses_the_preserved_output_side() {
    use laminar_sql::parser::join_parser::JoinType;

    for join_type in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::RightSemi,
        JoinType::RightAnti,
    ] {
        let mut config = test_config();
        config.join_type = join_type;
        let mut operator =
            IntervalJoinOperator::new("frontier", config, None, SessionContext::new());
        operator.applied_left_watermark = 2_000;
        operator.applied_right_watermark = 1_500;
        operator.applied_left_idle = true;
        operator.applied_right_idle = true;
        let expected = if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
            1_500
        } else {
            1_400
        };
        let output = operator.output_frontier(unconstrained_frontier());
        assert_eq!(output.watermark, Some(expected), "{join_type:?}");
        assert!(output.idle, "{join_type:?}");
    }
}

#[tokio::test]
async fn all_input_idle_is_checkpointed_and_restored() {
    let mut operator =
        IntervalJoinOperator::new("idle-frontier", test_config(), None, SessionContext::new());
    let frontiers = [
        InputFrontier {
            watermark: Some(200),
            idle: true,
        },
        InputFrontier {
            watermark: Some(400),
            idle: true,
        },
    ];
    operator
        .process_with_frontiers(&[Vec::new(), Vec::new()], &frontiers)
        .await
        .unwrap();

    let checkpoint = operator.checkpoint().unwrap().unwrap();
    let mut restored =
        IntervalJoinOperator::new("idle-frontier", test_config(), None, SessionContext::new());
    restored.restore(checkpoint).unwrap();

    assert!(restored.applied_left_idle);
    assert!(restored.applied_right_idle);
    let output = restored.output_frontier(InputFrontier {
        watermark: Some(i64::MAX),
        idle: false,
    });
    assert_eq!(
        output,
        InputFrontier {
            watermark: Some(200),
            idle: true,
        }
    );
    #[cfg(feature = "cluster")]
    assert_eq!(restored.restored_output_frontier(), Some(output));
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

#[cfg(feature = "cluster")]
fn right_dictionary_batch(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
    assert_eq!(ids.len(), timestamps.len());
    assert_eq!(ids.len(), amounts.len());
    let labels = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0_i8; ids.len()]),
        Arc::new(StringArray::from(vec!["queued-dictionary"])),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("amount", DataType::Float64, false),
        Field::new(
            "label",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(amounts.to_vec())),
            Arc::new(labels),
        ],
    )
    .unwrap()
}

fn positioned_input(
    batch: RecordBatch,
    partitions: &[&[u8]],
    orders: &[u64],
    mutations: Option<Vec<SourceMutation>>,
) -> RecordBatch {
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

fn configure_keyed_ordered(operator: &mut IntervalJoinOperator) {
    operator.set_input_schemas(
        left_batch(&[], &[], &[]).schema(),
        right_batch(&[], &[], &[]).schema(),
    );
    let keyed = || BoundedJoinInputMode::KeyedUpsert {
        primary_key_indices: vec![0, 1],
    };
    operator.configure_ordered_inputs(keyed(), keyed()).unwrap();
}

fn key_for_vnode(target: u32, vnode_count: u32) -> String {
    for candidate in 0..1_000 {
        let key = format!("vnode-{candidate}");
        let batch = left_batch(&[key.as_str()], &[100], &[1.0]);
        let vnodes = laminar_core::shuffle::row_vnodes(&batch, &[0], vnode_count).unwrap();
        if vnodes == [target] {
            return key;
        }
    }
    panic!("could not find a key for vnode {target}");
}

#[cfg(feature = "cluster")]
fn composite_left_batch(regions: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("region", DataType::Int64, false),
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
            Arc::new(StringArray::from(vec!["hot"; regions.len()])),
            Arc::new(Int64Array::from(regions.to_vec())),
            Arc::new(TimestampMillisecondArray::from(vec![100; regions.len()])),
            Arc::new(Float64Array::from(vec![1.0; regions.len()])),
        ],
    )
    .unwrap()
}

#[cfg(feature = "cluster")]
fn incompatible_left_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("price", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["bad"])),
            Arc::new(TimestampMillisecondArray::from(vec![110])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_basic_interval_join() {
    let ctx = laminar_sql::create_session_context();
    let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

    let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
    let right = right_batch(&["A", "B"], &[110, 250], &[1.0, 2.0]);

    let result = op
        .process(&[vec![left], vec![right]], &[0, 0])
        .await
        .unwrap();

    // A: |100 - 110| = 10 <= 100 -> match
    // B: |200 - 250| = 50 <= 100 -> match
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 2);
}

#[tokio::test]
async fn local_join_routes_into_configured_vnodes() {
    let key_group_count = KeyGroupCount::try_from(8_u16).unwrap();
    let mut op = IntervalJoinOperator::new_with_key_groups(
        "local_vnodes",
        test_config(),
        None,
        laminar_sql::create_session_context(),
        key_group_count,
    );
    let key_zero = key_for_vnode(0, u32::from(key_group_count));
    let key_one = key_for_vnode(1, u32::from(key_group_count));
    let keys = [key_zero.as_str(), key_one.as_str()];

    let output = op
        .process(
            &[
                vec![left_batch(&keys, &[100, 200], &[10.0, 20.0])],
                vec![right_batch(&keys, &[110, 210], &[1.0, 2.0])],
            ],
            &[0, 0],
        )
        .await
        .unwrap();

    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(op.vnode_states[0].is_some());
    assert!(op.vnode_states[1].is_some());
    assert!(op.vnode_states[2..].iter().all(Option::is_none));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_admits_current_batch_before_advancing_its_watermark() {
    let (shuffle, _) = single_owner_shuffle(8).await;
    let mut op = IntervalJoinOperator::new(
        "current_batch_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    op.attach_cluster_shuffle(shuffle);

    let output = op
        .process(
            &[
                vec![left_batch(&["A"], &[100], &[1.0])],
                vec![right_batch(&["A"], &[110], &[2.0])],
            ],
            &[300, 300],
        )
        .await
        .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let error = op
        .process(
            &[vec![left_batch(&["late"], &[100], &[1.0])], vec![]],
            &[300, 300],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("below closed cutoff 300"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn bootstrap_broadcast_holds_restored_cut_ahead_of_live_replay_frontier() {
    use laminar_core::shuffle::ShuffleMessage;

    let (scope, _remote) = two_owner_shuffle().await;
    let mut operator = IntervalJoinOperator::new(
        "bootstrap_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    operator.attach_cluster_shuffle(scope.clone());
    let cut = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let live = InputFrontier {
        watermark: Some(300),
        idle: false,
    };
    operator.applied_left_watermark = 100;
    operator.applied_right_watermark = 100;
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

    let key = key_for_vnode(1, 2);
    let replay = left_batch(&[key.as_str()], &[150], &[1.0]);
    let error = match operator.plan_cluster_inputs(
        &[vec![replay.clone()], Vec::new()],
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
    for observed in [
        InputFrontier::default(),
        InputFrontier {
            watermark: Some(90),
            idle: false,
        },
    ] {
        let catch_up = operator
            .plan_cluster_inputs(
                &[Vec::new(), Vec::new()],
                [observed; 2],
                &scope,
                &assignment,
                &[2],
            )
            .unwrap();
        assert_eq!(catch_up.local_frontiers, [cut; 2]);
    }
    let replay = operator
        .plan_cluster_inputs(
            &[vec![replay], Vec::new()],
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
        ShuffleMessage::Frontier {
            stage,
            watermark: Some(300),
            idle: false,
        } if stage == "bootstrap_interval::right"
    ));
    assert!(matches!(
        &replay.outbound[1].1,
        ShuffleMessage::Data { stage, .. } if stage == "bootstrap_interval::left"
    ));
    assert!(matches!(
        &replay.outbound[2].1,
        ShuffleMessage::Frontier {
            stage,
            watermark: Some(300),
            idle: false,
        } if stage == "bootstrap_interval::left"
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn zero_admission_send_retries_without_becoming_runnable() {
    let (scope, _) = two_owner_shuffle().await;
    let mut operator = IntervalJoinOperator::new(
        "retry_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    operator.attach_cluster_shuffle(scope);
    let retry_plan = vec![(
        2,
        laminar_core::shuffle::ShuffleMessage::Frontier {
            stage: "retry_interval::left".to_string(),
            watermark: None,
            idle: false,
        },
    )];
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
    let (visible_tx, visible_rx) = tokio::sync::oneshot::channel();
    let send = tokio::spawn(async move {
        let _ = outcome_tx.send((
            Err(DbError::ShuffleNotReady("injected zero admission".into())),
            Some(retry_plan),
        ));
        let _ = visible_tx.send(());
    });
    operator.pending_cluster_input = Some(PendingIntervalClusterInput {
        routed: BTreeMap::new(),
        outbound: None,
        local_frontiers: [InputFrontier::default(); 2],
        send: Some(send),
        outcome: Some(outcome_rx),
        accounted_bytes: 0,
    });
    visible_rx.await.unwrap();

    let output = operator
        .process_cluster(
            &[Vec::new(), Vec::new()],
            InputFrontier::default(),
            InputFrontier::default(),
        )
        .await
        .unwrap();
    assert!(output.is_empty());
    assert!(operator
        .pending_cluster_input
        .as_ref()
        .unwrap()
        .send
        .is_some());
    assert!(operator
        .pending_cluster_input
        .as_ref()
        .unwrap()
        .outcome
        .is_some());
    assert!(!operator.deferred_work_is_runnable());
    assert!(operator.checkpoint_capture(u64::MAX).is_err());
    assert!(operator.checkpoint_vnodes(&[0], 2, u64::MAX).is_err());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn pending_send_applies_remote_match_before_local_finalize() {
    use laminar_sql::parser::join_parser::JoinType;

    let (scope, _) = two_owner_shuffle().await;
    let mut config = test_config();
    config.join_type = JoinType::Left;
    let mut operator = IntervalJoinOperator::new(
        "pending_interval",
        config,
        None,
        laminar_sql::create_session_context(),
    );
    operator.attach_cluster_shuffle(scope.clone());
    let local_key = key_for_vnode(0, 2);
    let remote_key = key_for_vnode(1, 2);
    let local = left_batch(&[local_key.as_str()], &[100], &[8.0]);
    let outbound = left_batch(&[remote_key.as_str()], &[100], &[1.0]);
    let close = InputFrontier {
        watermark: Some(300),
        idle: false,
    };
    let assignment = scope.registry.versioned_snapshot();
    let plan = operator
        .plan_cluster_inputs(
            &[vec![local, outbound], Vec::new()],
            [close; 2],
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    let accounted_bytes = operator.cluster_input_plan_bytes(&plan).unwrap();
    let IntervalClusterInputPlan {
        routed,
        outbound,
        local_frontiers,
        effective_frontiers: _,
    } = plan;
    let (release, wait) = tokio::sync::oneshot::channel();
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
    let (visible_tx, visible_rx) = tokio::sync::oneshot::channel();
    let send = tokio::spawn(async move {
        let _ = wait.await;
        drop(outbound);
        let _ = outcome_tx.send((Ok(()), None));
        let _ = visible_tx.send(());
    });
    operator.pending_cluster_input = Some(PendingIntervalClusterInput {
        routed,
        outbound: None,
        local_frontiers,
        send: Some(send),
        outcome: Some(outcome_rx),
        accounted_bytes,
    });
    let assignment_version = scope.registry.assignment_version();
    let recovery_gen = scope.receiver.recovery_gen();
    operator
        .stage_checkpointed_shuffle(
            "pending_interval::right",
            crate::operator::RetainedBatch::restored_channel(
                right_batch(&[local_key.as_str()], &[110], &[34.0]),
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
            "pending_interval::right",
            2,
            close,
            assignment_version,
            recovery_gen,
        )
        .unwrap();

    let output = tokio::time::timeout(
        Duration::from_millis(50),
        operator.process_cluster(
            &[Vec::new(), Vec::new()],
            InputFrontier::default(),
            InputFrontier::default(),
        ),
    )
    .await
    .expect("pending interval send blocked the graph task")
    .unwrap();
    assert!(output.is_empty());
    assert_eq!(operator.queued_remote_events, 1);
    assert!(operator.pending_cluster_input.is_some());

    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), visible_rx)
        .await
        .expect("pending interval send outcome was not published")
        .unwrap();
    let output = operator
        .process_cluster(
            &[Vec::new(), Vec::new()],
            InputFrontier::default(),
            InputFrontier::default(),
        )
        .await
        .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let amount = output[0]
        .column_by_name("amount_right_stream")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(amount.value(0), 34.0);
    assert!(operator.pending_cluster_input.is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpointed_remote_frontiers_compare_in_receiver_domain() {
    let (scope, _) = two_owner_shuffle().await;
    let mut operator = IntervalJoinOperator::new(
        "frontier_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    operator.attach_cluster_shuffle(scope.clone());
    operator.applied_right_watermark = 500;
    let idle = InputFrontier {
        watermark: Some(100),
        idle: true,
    };
    let channel = operator.peer_channels[JoinInputSide::Right.port()]
        .get_mut(&2)
        .unwrap();
    channel.applied = idle;
    channel.accepted = idle;
    let assignment = scope.registry.assignment_version();
    let recovery = scope.receiver.recovery_gen();
    let active = |watermark| InputFrontier {
        watermark: Some(watermark),
        idle: false,
    };

    operator
        .stage_checkpointed_shuffle_frontier(
            "frontier_interval::right",
            2,
            active(100),
            assignment,
            recovery,
        )
        .unwrap();
    assert_eq!(
        operator.peer_channels[JoinInputSide::Right.port()][&2]
            .accepted
            .watermark,
        Some(500)
    );
    operator
        .stage_checkpointed_shuffle_frontier(
            "frontier_interval::right",
            2,
            active(150),
            assignment,
            recovery,
        )
        .unwrap();
    assert_eq!(
        operator.peer_channels[JoinInputSide::Right.port()][&2]
            .accepted
            .watermark,
        Some(500)
    );
    operator
        .stage_checkpointed_shuffle_frontier(
            "frontier_interval::right",
            2,
            active(550),
            assignment,
            recovery,
        )
        .unwrap();
    assert_eq!(
        operator.peer_channels[JoinInputSide::Right.port()][&2]
            .accepted
            .watermark,
        Some(550)
    );
    assert!(operator
        .stage_checkpointed_shuffle_frontier(
            "frontier_interval::right",
            2,
            InputFrontier {
                watermark: None,
                idle: false,
            },
            assignment,
            recovery,
        )
        .is_err());
    assert!(operator
        .stage_checkpointed_shuffle_frontier(
            "frontier_interval::right",
            2,
            active(525),
            assignment,
            recovery,
        )
        .is_err());
    assert_eq!(
        operator.peer_channels[JoinInputSide::Right.port()][&2]
            .accepted
            .watermark,
        Some(550)
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn ordered_channel_checkpoint_restores_scope_and_rejects_idle_data() {
    let (scope, _) = two_owner_shuffle().await;
    let mut operator = IntervalJoinOperator::new(
        "channel_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    operator.attach_cluster_shuffle(scope.clone());
    let key = key_for_vnode(0, 2);
    let assignment_version = scope.registry.assignment_version();
    let recovery_gen = scope.receiver.recovery_gen();
    let close = InputFrontier {
        watermark: Some(300),
        idle: false,
    };
    operator
        .stage_checkpointed_shuffle(
            "channel_interval::right",
            crate::operator::RetainedBatch::restored_channel(
                right_dictionary_batch(&[key.as_str()], &[110], &[2.0]),
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
            "channel_interval::right",
            2,
            close,
            assignment_version,
            recovery_gen,
        )
        .unwrap();
    operator
        .stage_checkpointed_shuffle_frontier(
            "channel_interval::left",
            2,
            close,
            assignment_version,
            recovery_gen,
        )
        .unwrap();
    let IntervalRemoteEventPayload::Data(queued) = &operator.peer_channels
        [JoinInputSide::Right.port()][&2]
        .events
        .front()
        .unwrap()
        .payload
    else {
        panic!("right channel did not retain its staged data first");
    };
    assert_eq!(queued.row_vnodes.as_ref(), &[0]);
    let declared_vnodes = queued.retained.routed_vnodes().len();
    operator.remote_side_cursor = 1;
    let sized_cluster = operator
        .capture_cluster_checkpoint(usize::MAX)
        .unwrap()
        .unwrap();
    let retained_cluster_bytes = sized_cluster.retained_bytes().unwrap();
    drop(sized_cluster);
    let mut coverage_probe = Vec::<u8>::new();
    coverage_probe.try_reserve_exact(declared_vnodes).unwrap();
    let coverage_scratch_bytes = coverage_probe
        .capacity()
        .checked_mul(std::mem::size_of::<u8>())
        .and_then(|bytes| bytes.checked_add(HEAP_ALLOCATION_CHARGE))
        .unwrap();
    let exact_capture_peak = retained_cluster_bytes
        .checked_add(coverage_scratch_bytes)
        .unwrap();
    assert!(operator
        .capture_cluster_checkpoint(exact_capture_peak - 1)
        .is_err());
    assert!(operator
        .capture_cluster_checkpoint(exact_capture_peak)
        .unwrap()
        .is_some());
    let checkpoint = operator.checkpoint().unwrap().unwrap();
    let checkpoint_data = checkpoint.data.clone();
    let mut tight_restore = IntervalJoinOperator::new(
        "channel_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    tight_restore.attach_cluster_shuffle(scope.clone());
    let restore_preflight = tight_restore
        .preflight_whole_restore_archive(&checkpoint_data)
        .unwrap();
    assert!(restore_preflight.decoded_checkpoint > checkpoint_data.len());
    let exact_restore_peak = tight_restore
        .accounted_state_bytes()
        .checked_add(restore_preflight.encoded_frame)
        .and_then(|bytes| bytes.checked_add(restore_preflight.decoded_checkpoint))
        .and_then(|bytes| bytes.checked_add(restore_preflight.runtime_scratch))
        .unwrap();
    tight_restore.set_managed_state_budget(exact_restore_peak - 1);
    assert!(matches!(
        tight_restore.restore(OperatorCheckpoint {
            data: checkpoint_data.clone(),
        }),
        Err(DbError::ManagedStateBudgetExceeded { .. })
    ));
    assert_eq!(tight_restore.applied_left_watermark, i64::MIN);
    assert_eq!(tight_restore.applied_right_watermark, i64::MIN);
    assert_eq!(tight_restore.queued_remote_events, 0);
    assert!(tight_restore.vnode_states.iter().all(Option::is_none));

    let active_recovery = recovery_gen + 1;
    scope.sender.set_recovery_gen(active_recovery);
    scope.receiver.set_recovery_gen(active_recovery);

    let mut malformed =
        rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(&checkpoint_data)
            .unwrap();
    malformed.applied_right_watermark = 200;
    let right_channel =
        &mut malformed.cluster.as_mut().unwrap().channels[JoinInputSide::Right.port()][0];
    right_channel.applied = IntervalCheckpointFrontier {
        watermark: Some(100),
        idle: true,
    };
    right_channel.events.insert(
        0,
        IntervalCheckpointEvent::Frontier {
            recovery_gen,
            frontier: IntervalCheckpointFrontier {
                watermark: Some(150),
                idle: false,
            },
        },
    );
    let mut malformed_target = IntervalJoinOperator::new(
        "channel_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    malformed_target.attach_cluster_shuffle(scope.clone());
    let error = malformed_target
        .restore(OperatorCheckpoint {
            data: rkyv::to_bytes::<rkyv::rancor::Error>(&malformed)
                .unwrap()
                .to_vec(),
        })
        .unwrap_err();
    assert!(error.to_string().contains("revival frontier is below"));

    let mut rejected = IntervalJoinOperator::new(
        "channel_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    rejected.attach_cluster_shuffle(scope.clone());
    let prior_key = "p".repeat(checkpoint_data.len().saturating_mul(2).max(1));
    let mut prior_state = IntervalJoinState::new();
    execute_interval_join_cycle(
        &mut prior_state,
        &[left_batch(&[prior_key.as_str()], &[90], &[1.0])],
        &[],
        &rejected.config,
        i64::MIN,
        i64::MIN,
        i64::MIN,
        i64::MIN,
        usize::MAX,
        &mut IntervalJoinOutputBudget::default(),
    )
    .unwrap();
    rejected.vnode_states[0] = Some(Box::new(IntervalJoinVnodeState {
        core: prior_state,
        ordered: None,
    }));
    rejected.add_resident_vnode(0);
    let prior_channel_frontiers = [
        InputFrontier {
            watermark: Some(17),
            idle: false,
        },
        InputFrontier {
            watermark: Some(19),
            idle: false,
        },
    ];
    for (port, frontier) in prior_channel_frontiers.into_iter().enumerate() {
        let channel = rejected.peer_channels[port].get_mut(&2).unwrap();
        channel.applied = frontier;
        channel.accepted = frontier;
    }
    rejected.remote_side_cursor = 0;
    rejected.remote_peer_cursors = [Some(2), Some(2)];
    let prior_state_ptr = std::ptr::from_ref(rejected.vnode_states[0].as_deref().unwrap());
    let baseline = rejected.accounted_state_bytes();
    assert!(checkpoint_data.len() <= baseline);
    rejected.set_managed_state_budget(baseline);
    let error = rejected
        .restore(OperatorCheckpoint {
            data: checkpoint_data.clone(),
        })
        .unwrap_err();
    let DbError::ManagedStateBudgetExceeded {
        context,
        accounted_bytes,
        limit_bytes,
    } = error
    else {
        panic!("cluster restore did not reject its raw-plus-live state budget");
    };
    assert!(context.contains("whole checkpoint restore payload"));
    assert_eq!(limit_bytes, baseline);
    assert!(accounted_bytes > baseline);
    assert_eq!(
        std::ptr::from_ref(rejected.vnode_states[0].as_deref().unwrap()),
        prior_state_ptr
    );
    assert_eq!(
        rejected.vnode_states[0].as_deref().unwrap().buffered_rows(),
        (1, 0)
    );
    assert_eq!(rejected.resident_vnodes, [0]);
    assert_eq!(rejected.applied_left_watermark, i64::MIN);
    assert_eq!(rejected.applied_right_watermark, i64::MIN);
    assert!(!rejected.applied_left_idle);
    assert!(!rejected.applied_right_idle);
    assert_eq!(rejected.local_frontiers, [InputFrontier::default(); 2]);
    assert_eq!(rejected.last_broadcasts, [InputFrontier::default(); 2]);
    assert_eq!(rejected.remote_side_cursor, 0);
    assert_eq!(rejected.remote_peer_cursors, [Some(2), Some(2)]);
    assert_eq!(rejected.queued_remote_events, 0);
    assert_eq!(rejected.queued_shuffle_bytes, 0);
    assert_eq!(rejected.queued_event_capacity_bytes, 0);
    assert!(rejected.pending_cluster_input.is_none());
    for (port, frontier) in prior_channel_frontiers.into_iter().enumerate() {
        let channel = &rejected.peer_channels[port][&2];
        assert_eq!(channel.applied, frontier);
        assert_eq!(channel.accepted, frontier);
        assert!(channel.events.is_empty());
    }

    let mut restored = IntervalJoinOperator::new(
        "channel_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    restored.attach_cluster_shuffle(scope);
    restored.restore(checkpoint).unwrap();
    assert_eq!(restored.queued_remote_events, 3);
    let IntervalRemoteEventPayload::Data(queued) = &restored.peer_channels
        [JoinInputSide::Right.port()][&2]
        .events
        .front()
        .unwrap()
        .payload
    else {
        panic!("restored right channel lost its staged data order");
    };
    assert_eq!(queued.row_vnodes.as_ref(), &[0]);
    let dictionary = queued
        .retained
        .batch()
        .column(3)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .expect("queued dictionary column was not restored as a dictionary");
    assert_eq!(dictionary.keys().value(0), 0);
    assert_eq!(
        dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "queued-dictionary"
    );
    assert!(!restored.wants_input());
    assert!(restored
        .checkpoint_vnodes(&[0], 2, u64::MAX)
        .unwrap()
        .is_some());

    let first = restored
        .process_cluster(
            &[Vec::new(), Vec::new()],
            InputFrontier::default(),
            InputFrontier::default(),
        )
        .await
        .unwrap();
    assert!(first.is_empty());
    assert_eq!(restored.queued_remote_events, 2);
    assert_eq!(
        restored.vnode_states[0].as_ref().unwrap().buffered_rows(),
        (0, 1)
    );
    assert_eq!(
        restored.peer_channels[JoinInputSide::Right.port()][&2].applied,
        InputFrontier::default()
    );
    let second = restored
        .process_cluster(
            &[Vec::new(), Vec::new()],
            InputFrontier::default(),
            InputFrontier::default(),
        )
        .await
        .unwrap();
    assert!(second.is_empty());
    assert_eq!(restored.queued_remote_events, 1);
    assert_eq!(
        restored.peer_channels[JoinInputSide::Left.port()][&2].applied,
        close
    );
    assert_eq!(
        restored.peer_channels[JoinInputSide::Right.port()][&2].applied,
        InputFrontier::default()
    );
    assert_eq!(
        restored.vnode_states[0].as_ref().unwrap().buffered_rows(),
        (0, 1)
    );
    let third = restored
        .process_cluster(
            &[Vec::new(), Vec::new()],
            InputFrontier::default(),
            InputFrontier::default(),
        )
        .await
        .unwrap();
    assert!(third.is_empty());
    assert_eq!(restored.queued_remote_events, 0);
    assert_eq!(
        restored.peer_channels[JoinInputSide::Right.port()][&2].applied,
        close
    );
    assert_eq!(
        restored.vnode_states[0].as_ref().unwrap().buffered_rows(),
        (0, 1)
    );

    restored
        .stage_checkpointed_shuffle_frontier(
            "channel_interval::left",
            2,
            InputFrontier {
                watermark: close.watermark,
                idle: true,
            },
            assignment_version,
            active_recovery,
        )
        .unwrap();
    let error = restored
        .stage_checkpointed_shuffle(
            "channel_interval::left",
            crate::operator::RetainedBatch::restored_channel(
                left_batch(&[key.as_str()], &[100], &[1.0]),
                2,
                assignment_version,
                active_recovery,
                Arc::from([0_u32]),
            ),
            i64::MIN,
        )
        .unwrap_err();
    assert!(error.to_string().contains("behind an idle peer frontier"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn pending_plan_budget_rejects_before_shuffle_admission() {
    let (shuffle, remote_receiver) = two_owner_shuffle().await;
    let local_key = key_for_vnode(0, 2);
    let remote_key = key_for_vnode(1, 2);
    let mut op = IntervalJoinOperator::new(
        "post_shuffle_failure",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    op.attach_cluster_shuffle(shuffle);
    let baseline = op.accounted_state_bytes();
    op.max_managed_state_bytes = baseline;

    let error = op
        .process(
            &[
                vec![left_batch(
                    &[local_key.as_str(), remote_key.as_str()],
                    &[100, 100],
                    &[1.0, 2.0],
                )],
                vec![],
            ],
            &[0, 0],
        )
        .await
        .unwrap_err();

    let DbError::ManagedStateBudgetExceeded {
        context,
        accounted_bytes,
        limit_bytes,
    } = error
    else {
        panic!("pending interval plan did not fail its managed-state budget");
    };
    assert!(context.contains("pending shuffle send"));
    assert_eq!(limit_bytes, baseline);
    assert!(accounted_bytes > baseline);
    assert!(op.pending_cluster_input.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), remote_receiver.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_cross_cycle_matching() {
    let ctx = laminar_sql::create_session_context();
    let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

    // Cycle 1: only left data
    let left = left_batch(&["A"], &[100], &[10.0]);
    let result = op.process(&[vec![left], vec![]], &[0, 0]).await.unwrap();
    assert!(result.is_empty());

    // Cycle 2: right data arrives, should match the buffered left
    let right = right_batch(&["A"], &[150], &[1.0]);
    let result = op.process(&[vec![], vec![right]], &[0, 0]).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 1);
}

#[tokio::test]
async fn test_empty_inputs() {
    let ctx = laminar_sql::create_session_context();
    let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

    let result = op.process(&[], &[0]).await.unwrap();
    assert!(result.is_empty());
    assert!(op.vnode_states[0].is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn composite_keys_route_by_the_full_ordered_tuple() {
    let vnode_count = 64;
    let batch = composite_left_batch(&[1, 2]);
    let mut expected = laminar_core::shuffle::row_vnodes(&batch, &[0, 1], vnode_count).unwrap();
    expected.sort_unstable();
    expected.dedup();
    assert_eq!(expected.len(), 2, "test tuple hashes unexpectedly collided");

    let (shuffle, _) = single_owner_shuffle(vnode_count).await;
    let mut config = test_config();
    config.left_keys.push("region".into());
    config.right_keys.push("region".into());
    let mut op = IntervalJoinOperator::new(
        "composite_interval",
        config,
        None,
        laminar_sql::create_session_context(),
    );
    op.attach_cluster_shuffle(shuffle);
    op.process(&[vec![batch], vec![]], &[0, 0]).await.unwrap();

    let actual = op
        .vnode_states
        .iter()
        .zip(0_u32..)
        .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn later_vnode_failure_requires_recovery_after_prior_admission() {
    let (shuffle, _) = single_owner_shuffle(2).await;
    let mut op = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    op.attach_cluster_shuffle(shuffle);

    let mut retained = IntervalJoinState::new();
    let mut output_budget = IntervalJoinOutputBudget::default();
    execute_interval_join_cycle(
        &mut retained,
        &[left_batch(&["seed"], &[100], &[1.0])],
        &[],
        &op.config,
        0,
        0,
        0,
        0,
        usize::MAX,
        &mut output_budget,
    )
    .unwrap();
    op.vnode_states[1] = Some(Box::new(IntervalJoinVnodeState {
        core: retained,
        ordered: None,
    }));
    op.add_resident_vnode(1);

    let mut routed = BTreeMap::new();
    routed.insert(0, [vec![left_batch(&["ok"], &[100], &[1.0])], vec![]]);
    routed.insert(1, [vec![incompatible_left_batch()], vec![]]);
    let error = op.execute_routed_shards(routed, 0, 0).unwrap_err();

    assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
    assert_eq!(op.vnode_states[0].as_ref().unwrap().buffered_rows(), (1, 0));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_watermark_sweep_failure_requires_recovery() {
    let (shuffle, _) = single_owner_shuffle(2).await;
    let mut op = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    op.attach_cluster_shuffle(shuffle);

    let mut retained = IntervalJoinState::new();
    let mut output_budget = IntervalJoinOutputBudget::default();
    for timestamp in (1_000..1_400).step_by(10) {
        execute_interval_join_cycle(
            &mut retained,
            &[left_batch(&["seed"], &[timestamp], &[1.0])],
            &[],
            &op.config,
            0,
            0,
            0,
            0,
            usize::MAX,
            &mut output_budget,
        )
        .unwrap();
    }
    op.vnode_states[0] = Some(Box::new(IntervalJoinVnodeState {
        core: retained,
        ordered: None,
    }));
    op.add_resident_vnode(0);

    // Force compaction to fail after the sweep has already removed old index entries.
    op.config.left_keys = vec!["missing".to_string()];
    let error = op.process(&[], &[0, 1_300]).await.unwrap_err();

    assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
    assert!(error.requires_pipeline_recovery());
    assert_eq!(
        op.vnode_states[0].as_ref().unwrap().buffered_rows(),
        (20, 0)
    );
}

#[tokio::test]
async fn test_checkpoint_roundtrip() {
    let ctx = laminar_sql::create_session_context();
    let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx.clone());

    // Buffer some data
    let left = left_batch(&["A"], &[100], &[10.0]);
    let right = right_batch(&["A"], &[110], &[1.0]);
    let _ = op
        .process(&[vec![left], vec![right]], &[50, 50])
        .await
        .unwrap();
    assert_eq!(
        op.output_frontier(unconstrained_frontier()).watermark,
        Some(-50)
    );

    let metadata = op
        .checkpoint()
        .unwrap()
        .expect("watermarks are checkpointed");
    let captured = op
        .checkpoint_vnodes(&[0], 1, u64::MAX)
        .unwrap()
        .expect("interval join has vnode state");
    let state = captured
        .into_iter()
        .next()
        .and_then(|captured| captured.state)
        .expect("the first vnode capture is complete");
    assert!(op
        .checkpoint_vnodes(&[0], 1, u64::MAX)
        .unwrap()
        .unwrap()
        .is_empty());
    let state = materialize_capture(state).unwrap();

    let mut op2 = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
    op2.restore(metadata).unwrap();
    op2.restore_vnode(0, 1, &state).unwrap();
    assert_eq!(
        op2.output_frontier(unconstrained_frontier()).watermark,
        Some(-50)
    );

    // New right data should match the restored left
    let right2 = right_batch(&["A"], &[120], &[2.0]);
    let result = op2
        .process(&[vec![], vec![right2]], &[50, 50])
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 1);
}

#[tokio::test]
async fn vnode_capture_is_full_then_sparse_until_forced() {
    let vnode_count = 4_u32;
    let required = [0, 1, 2, 3];
    let mut op = IntervalJoinOperator::new_with_key_groups(
        "sparse_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
        KeyGroupCount::try_from(4_u16).unwrap(),
    );

    let baseline = op
        .checkpoint_vnodes(&required, vnode_count, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(
        baseline.iter().map(|frame| frame.vnode).collect::<Vec<_>>(),
        required
    );
    assert!(baseline.iter().all(|frame| frame.state.is_some()));
    drop(baseline);

    assert!(op
        .checkpoint_vnodes(&required, vnode_count, u64::MAX)
        .unwrap()
        .unwrap()
        .is_empty());

    let vnode = 2;
    let key = key_for_vnode(vnode, vnode_count);
    op.process(
        &[vec![left_batch(&[key.as_str()], &[100], &[1.0])], vec![]],
        &[0, 0],
    )
    .await
    .unwrap();
    let dirty = op
        .checkpoint_vnodes(&required, vnode_count, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].vnode, vnode);
    assert!(dirty[0].state.is_some());
    drop(dirty);

    op.force_full_vnode_capture();
    let forced = op
        .checkpoint_vnodes(&required, vnode_count, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(
        forced.iter().map(|frame| frame.vnode).collect::<Vec<_>>(),
        required
    );
    assert!(forced.iter().all(|frame| frame.state.is_some()));
}

#[tokio::test]
async fn checkpoint_respects_ipc_and_archive_peak_budget() {
    let ctx = laminar_sql::create_session_context();
    let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
    let wide_key = "x".repeat(64 * 1024);
    op.process(
        &[
            vec![left_batch(&[wide_key.as_str()], &[100], &[1.0])],
            vec![],
        ],
        &[0, 0],
    )
    .await
    .unwrap();

    let ipc_bytes = op.vnode_states[0]
        .as_mut()
        .unwrap()
        .snapshot_checkpoint(&op.config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
        .unwrap()
        .retained_ipc_bytes()
        .unwrap();
    let archive_bytes = IntervalJoinOperator::serialize_state(
        op.vnode_states[0].as_mut().unwrap(),
        &op.config,
        "interval join peak-budget sizing",
        usize::MAX,
    )
    .unwrap()
    .len();
    let limit = ipc_bytes
        .checked_add(archive_bytes)
        .and_then(|bytes| bytes.checked_add(HEAP_ALLOCATION_CHARGE))
        .unwrap();
    assert!(op.accounted_state_bytes() <= limit);
    op.set_managed_state_budget(limit);

    let state = op
        .checkpoint_vnodes(&[0], 1, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .next()
        .and_then(|captured| captured.state)
        .expect("the first vnode capture is complete");
    assert!(matches!(&state, StateFrameCapture::Deferred { .. }));
    let state = materialize_capture(state).unwrap();
    assert_eq!(state.len(), archive_bytes);
}

#[tokio::test]
async fn ordered_zero_affinity_vnode_restore_is_exact_and_combined_budget_atomic() {
    let mut donor = IntervalJoinOperator::new(
        "ordered-checkpoint",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    configure_keyed_ordered(&mut donor);
    let tombstone = positioned_input(
        left_batch(&["A"], &[100], &[999.0]),
        &[b"partition-0"],
        &[1],
        Some(vec![SourceMutation::Tombstone]),
    );
    assert!(donor
        .process(&[vec![tombstone], vec![]], &[0, 0])
        .await
        .unwrap()
        .is_empty());
    let donor_state = donor.vnode_states[0].as_ref().unwrap();
    assert_eq!(donor_state.buffered_rows(), (0, 0));
    let normalizer_evidence = |normalizer: &BoundedJoinInputNormalizer| {
        let checkpoint = normalizer
            .capture_checkpoint(usize::MAX)
            .unwrap()
            .encode(usize::MAX)
            .unwrap();
        rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .unwrap()
            .to_vec()
    };
    let ordered = donor_state.ordered.as_ref().unwrap();
    let expected_left = normalizer_evidence(&ordered.left);
    let expected_right = normalizer_evidence(&ordered.right);
    let expected_fingerprints = donor_state.ordered_fingerprints();
    let vnode_charge = donor_state.accounted_state_bytes();

    let whole = donor.checkpoint().unwrap().unwrap();
    let frame = donor
        .checkpoint_vnodes(&[0], 1, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .next()
        .and_then(|captured| captured.state)
        .map(materialize_capture)
        .unwrap()
        .unwrap();

    let mut rejected = IntervalJoinOperator::new(
        "ordered-checkpoint",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    configure_keyed_ordered(&mut rejected);
    rejected
        .restore(OperatorCheckpoint {
            data: whole.data.clone(),
        })
        .unwrap();
    let limit = rejected
        .accounted_state_bytes()
        .checked_add(vnode_charge)
        .unwrap()
        .saturating_sub(1);
    rejected.set_managed_state_budget(limit);
    let error = rejected.restore_vnode(0, 1, &frame).unwrap_err();
    assert!(
        error.to_string().contains("limit") || error.to_string().contains("remaining"),
        "unexpected restore error: {error}"
    );
    assert!(rejected.vnode_states[0].is_none());
    assert!(rejected.resident_vnodes.is_empty());

    let mut restored = IntervalJoinOperator::new(
        "ordered-checkpoint",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    configure_keyed_ordered(&mut restored);
    restored.restore(whole).unwrap();
    restored.restore_vnode(0, 1, &frame).unwrap();
    let state = restored.vnode_states[0].as_ref().unwrap();
    assert_eq!(state.buffered_rows(), (0, 0));
    assert_eq!(state.ordered_fingerprints(), expected_fingerprints);
    let ordered = state.ordered.as_ref().unwrap();
    assert_eq!(ordered.left.closed_cutoff(), 0);
    assert_eq!(ordered.right.closed_cutoff(), 0);
    assert_eq!(normalizer_evidence(&ordered.left), expected_left);
    assert_eq!(normalizer_evidence(&ordered.right), expected_right);
}

#[tokio::test]
async fn ordered_absent_vnode_bootstraps_at_applied_cut_after_frontier_and_restore() {
    let frontiers = [
        InputFrontier {
            watermark: Some(100),
            idle: false,
        },
        InputFrontier {
            watermark: Some(200),
            idle: false,
        },
    ];
    let valid = || {
        positioned_input(
            left_batch(&["A"], &[100], &[1.0]),
            &[b"partition-0"],
            &[1],
            None,
        )
    };

    let mut direct = IntervalJoinOperator::new(
        "ordered-lazy",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    configure_keyed_ordered(&mut direct);
    direct
        .process_with_frontiers(&[Vec::new(), Vec::new()], &frontiers)
        .await
        .unwrap();
    assert!(direct.vnode_states[0].is_none());
    assert!(direct
        .process_with_frontiers(&[vec![valid()], Vec::new()], &frontiers)
        .await
        .unwrap()
        .is_empty());
    let state = direct.vnode_states[0].as_ref().unwrap();
    let ordered = state.ordered.as_ref().unwrap();
    assert_eq!(ordered.left.closed_cutoff(), 100);
    assert_eq!(ordered.right.closed_cutoff(), 200);
    assert_eq!(state.evicted_cutoffs(), (100, 100));

    let mut donor = IntervalJoinOperator::new(
        "ordered-lazy",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    configure_keyed_ordered(&mut donor);
    donor
        .process_with_frontiers(&[Vec::new(), Vec::new()], &frontiers)
        .await
        .unwrap();
    let late = positioned_input(
        left_batch(&["A"], &[99], &[1.0]),
        &[b"partition-0"],
        &[1],
        None,
    );
    let error = donor
        .process_with_frontiers(&[vec![late], Vec::new()], &frontiers)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("below closed cutoff"));
    assert!(
        donor.vnode_states[0].is_none(),
        "failed lazy initialization must not publish an empty ordered vnode"
    );

    let whole = donor.checkpoint().unwrap().unwrap();
    donor.force_full_vnode_capture();
    let absent = donor
        .checkpoint_vnodes(&[0], 1, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .next()
        .and_then(|captured| captured.state)
        .map(materialize_capture)
        .unwrap()
        .unwrap();
    assert_eq!(absent.as_ref(), ABSENT_VNODE_FRAME);

    let mut restored = IntervalJoinOperator::new(
        "ordered-lazy",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    configure_keyed_ordered(&mut restored);
    restored.restore(whole).unwrap();
    restored.restore_vnode(0, 1, &absent).unwrap();
    assert!(restored.vnode_states[0].is_none());
    restored
        .process_with_frontiers(&[vec![valid()], Vec::new()], &frontiers)
        .await
        .unwrap();
    let state = restored.vnode_states[0].as_ref().unwrap();
    let ordered = state.ordered.as_ref().unwrap();
    assert_eq!(ordered.left.closed_cutoff(), 100);
    assert_eq!(ordered.right.closed_cutoff(), 200);
    assert_eq!(state.evicted_cutoffs(), (100, 100));
}

#[test]
fn deferred_vnode_frames_share_the_operator_checkpoint_budget() {
    let make_operator = || {
        let mut operator = IntervalJoinOperator::new_with_key_groups(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            KeyGroupCount::try_from(2_u16).unwrap(),
        );
        for state in &mut operator.vnode_states {
            *state = Some(Box::new(IntervalJoinVnodeState::new_append()));
        }
        operator
    };

    let mut sizing = make_operator();
    let capture = sizing
        .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .next()
        .and_then(|captured| captured.state)
        .unwrap();
    let retained_bytes = sizing.vnode_states[0]
        .as_mut()
        .unwrap()
        .snapshot_checkpoint(&sizing.config, usize::MAX)
        .unwrap()
        .retained_ipc_bytes()
        .unwrap();
    let frame_bytes = materialize_capture(capture).unwrap().len();
    let single_frame_peak = retained_bytes
        .checked_add(HEAP_ALLOCATION_CHARGE)
        .and_then(|bytes| bytes.checked_add(frame_bytes))
        .unwrap();
    let limit = single_frame_peak.checked_add(frame_bytes).unwrap() - 1;

    let mut peak_operator = make_operator();
    peak_operator.vnode_states[1] = None;
    peak_operator.set_managed_state_budget(single_frame_peak - 1);
    let peak_capture = peak_operator
        .checkpoint_vnodes(&[0], 2, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .next()
        .and_then(|captured| captured.state)
        .unwrap();
    assert!(materialize_capture(peak_capture).is_err());

    let mut operator = make_operator();
    operator.set_managed_state_budget(limit);
    let mut frames = operator
        .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|captured| captured.state.unwrap());
    materialize_capture(frames.next().unwrap()).unwrap();
    assert!(materialize_capture(frames.next().unwrap()).is_err());
}

#[tokio::test]
async fn vnode_restore_preserves_sparse_state_from_unaligned_frame_and_budgets_copy() {
    let key_group_count = KeyGroupCount::try_from(2_u16).unwrap();
    let key = key_for_vnode(1, 2);
    let mut donor = IntervalJoinOperator::new_with_key_groups(
        "sparse_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
        key_group_count,
    );
    donor
        .process(
            &[vec![left_batch(&[key.as_str()], &[100], &[1.0])], vec![]],
            &[0, 0],
        )
        .await
        .unwrap();
    let frames = donor
        .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|frame| {
            (
                frame.vnode,
                materialize_capture(frame.state.unwrap()).unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let mut restored = IntervalJoinOperator::new_with_key_groups(
        "sparse_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
        key_group_count,
    );
    let present = frames
        .iter()
        .find(|(vnode, _)| *vnode == 1)
        .map(|(_, state)| state.clone())
        .unwrap();
    for (vnode, state) in frames {
        let state = if vnode == 1 {
            unaligned_checkpoint_transport(&state, VNODE_FRAME_HEADER_LEN, VNODE_FRAME_HEADER_LEN)
        } else {
            state
        };
        restored.restore_vnode(vnode, 2, &state).unwrap();
    }

    assert!(restored.vnode_states[0].is_none() && restored.vnode_states[1].is_some());

    let present =
        unaligned_checkpoint_transport(&present, VNODE_FRAME_HEADER_LEN, VNODE_FRAME_HEADER_LEN);
    let mut tight = IntervalJoinOperator::new_with_key_groups(
        "sparse_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
        key_group_count,
    );
    let tight_limit = tight
        .accounted_state_bytes()
        .checked_add(present.len())
        .and_then(|bytes| bytes.checked_add(vnode_checkpoint_alignment_copy_bytes(&present)))
        .unwrap()
        - 1;
    tight.set_managed_state_budget(tight_limit);
    let error = tight.restore_vnode(1, 2, &present).unwrap_err();
    assert!(error.to_string().contains("alignment copy"), "{error}");
    assert!(tight.vnode_states.iter().all(Option::is_none));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn fresh_owner_stages_unaligned_portable_cut_atomically() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::state::NodeId;

    use crate::operator_graph::{ManagedVnodeRestore, ManagedWholeRestore};

    let vnode_count = 2;
    let (scope, target_fence) = single_owner_shuffle(vnode_count).await;
    let mut target = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    target.attach_cluster_shuffle(scope.clone());
    let predecessor_version = target_fence.assignment_version - 1;
    let predecessor_owners = [2_u64, 3];
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        predecessor_version,
        &predecessor_owners,
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
    let predecessor_registry = VnodeRegistry::new_unassigned(vnode_count);
    predecessor_registry.set_assignment_and_version(
        Arc::from(predecessor_owners.map(NodeId)),
        predecessor_version,
    );
    target.local_assignment = predecessor_registry.versioned_snapshot();

    let mut empty = IntervalJoinState::new();
    let vnode0 = IntervalJoinOperator::serialize_state(
        &mut empty,
        &target.config,
        "test vnode 0",
        target.max_managed_state_bytes,
    )
    .unwrap();
    let vnode1 = IntervalJoinOperator::serialize_state(
        &mut IntervalJoinState::new(),
        &target.config,
        "test vnode 1",
        target.max_managed_state_bytes,
    )
    .unwrap();
    let restores = [
        ManagedVnodeRestore {
            participant_id: 2,
            vnode: 0,
            state: &vnode0,
        },
        ManagedVnodeRestore {
            participant_id: 3,
            vnode: 1,
            state: &vnode1,
        },
    ];
    let donor_config = target.config.clone();
    let encode_whole =
        |participant_id: u64, right_watermark: i64, right_evidence: i64, left_idle: bool| {
            let peer = if participant_id == 2 { 3 } else { 2 };
            let left_frontier = IntervalCheckpointFrontier {
                watermark: Some(300),
                idle: left_idle,
            };
            let right_frontier = IntervalCheckpointFrontier {
                watermark: Some(right_evidence),
                idle: false,
            };
            let checkpoint = IntervalJoinOperatorCheckpoint {
                version: OPERATOR_CHECKPOINT_VERSION,
                ordered_input_fingerprints: None,
                join_type: join_type_tag(donor_config.join_type),
                left_keys: donor_config.left_keys.clone(),
                right_keys: donor_config.right_keys.clone(),
                left_time_column: donor_config.left_time_column.clone(),
                right_time_column: donor_config.right_time_column.clone(),
                left_table: donor_config.left_table.clone(),
                right_table: donor_config.right_table.clone(),
                bound_ms: i64::try_from(donor_config.time_bound.as_millis()).unwrap(),
                applied_left_watermark: 300,
                applied_right_watermark: right_watermark,
                applied_left_idle: left_idle,
                applied_right_idle: false,
                cluster: Some(IntervalClusterCheckpoint {
                    assignment_version: predecessor.assignment_version,
                    owner_map_digest: predecessor.assignment_digest,
                    self_id: participant_id,
                    recovery_gen: 1,
                    local_frontiers: [left_frontier, right_frontier],
                    remote_side_cursor: 0,
                    remote_peer_cursors: [None; 2],
                    channels: [
                        vec![IntervalCheckpointChannel {
                            peer,
                            applied: left_frontier,
                            events: Vec::new(),
                        }],
                        vec![IntervalCheckpointChannel {
                            peer,
                            applied: right_frontier,
                            events: Vec::new(),
                        }],
                    ],
                }),
            };
            rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
                .unwrap()
                .to_vec()
        };
    let donor2 = encode_whole(2, 250, 250, true);
    let corrupt_donor3 = encode_whole(3, 250, 249, true);
    let corrupt = [
        ManagedWholeRestore {
            participant_id: 2,
            state: &donor2,
        },
        ManagedWholeRestore {
            participant_id: 3,
            state: &corrupt_donor3,
        },
    ];
    let error = target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &corrupt,
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap_err();
    assert!(error.to_string().contains("exact drained-cut evidence"));
    assert_eq!(target.applied_left_watermark, i64::MIN);
    assert!(target.vnode_states.iter().all(Option::is_none));

    let disagreeing_donor3 = encode_whole(3, 251, 251, false);
    let disagreeing = [
        ManagedWholeRestore {
            participant_id: 2,
            state: &donor2,
        },
        ManagedWholeRestore {
            participant_id: 3,
            state: &disagreeing_donor3,
        },
    ];
    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &disagreeing,
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap_err();
    assert_eq!(target.applied_left_watermark, i64::MIN);
    assert!(target.vnode_states.iter().all(Option::is_none));

    let donor3 = encode_whole(3, 250, 250, true);
    let unaligned_vnode0 =
        unaligned_checkpoint_transport(&vnode0, VNODE_FRAME_HEADER_LEN, VNODE_FRAME_HEADER_LEN);
    let unaligned_vnode1 =
        unaligned_checkpoint_transport(&vnode1, VNODE_FRAME_HEADER_LEN, VNODE_FRAME_HEADER_LEN);
    let unaligned_restores = [
        ManagedVnodeRestore {
            participant_id: 2,
            vnode: 0,
            state: &unaligned_vnode0,
        },
        ManagedVnodeRestore {
            participant_id: 3,
            vnode: 1,
            state: &unaligned_vnode1,
        },
    ];
    let whole_archive_alignment = std::mem::align_of::<ArchivedIntervalJoinOperatorCheckpoint>();
    let unaligned_donor2 = unaligned_checkpoint_transport(&donor2, 0, whole_archive_alignment);
    let unaligned_donor3 = unaligned_checkpoint_transport(&donor3, 0, whole_archive_alignment);
    let whole_restores = [
        ManagedWholeRestore {
            participant_id: 2,
            state: &unaligned_donor2,
        },
        ManagedWholeRestore {
            participant_id: 3,
            state: &unaligned_donor3,
        },
    ];

    let mut stale = IntervalJoinState::new();
    let stale_key = key_for_vnode(0, vnode_count);
    execute_interval_join_cycle(
        &mut stale,
        &[left_batch(&[stale_key.as_str()], &[200], &[1.0])],
        &[],
        &target.config,
        i64::MIN,
        i64::MIN,
        i64::MIN,
        i64::MIN,
        target.max_managed_state_bytes,
        &mut IntervalJoinOutputBudget::default(),
    )
    .unwrap();
    let stale_vnode0 = IntervalJoinOperator::serialize_state(
        &mut stale,
        &target.config,
        "stale vnode 0",
        target.max_managed_state_bytes,
    )
    .unwrap();
    let stale_restores = [
        ManagedVnodeRestore {
            participant_id: 2,
            vnode: 0,
            state: &stale_vnode0,
        },
        ManagedVnodeRestore {
            participant_id: 3,
            vnode: 1,
            state: &vnode1,
        },
    ];
    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &stale_restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap_err();
    assert!(target.vnode_states.iter().all(Option::is_none));

    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &unaligned_restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    assert_eq!(target.applied_left_watermark, i64::MIN);
    target.publish_vnode_transition();
    assert_eq!(target.applied_left_watermark, 300);
    assert_eq!(target.applied_right_watermark, 250);
    assert!(target.applied_left_idle);
    assert!(!target.applied_right_idle);
    target.finish_vnode_transition();

    let predecessor_nodes = predecessor_owners.map(NodeId);
    let mut bootstrap = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    bootstrap.attach_cluster_shuffle(scope);
    bootstrap
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &unaligned_restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                predecessor_owners: &predecessor_nodes,
            },
        })
        .unwrap();
    bootstrap.publish_vnode_transition();
    assert_eq!(bootstrap.applied_left_watermark, 300);
    assert_eq!(bootstrap.applied_right_watermark, 250);
    assert!(bootstrap.vnode_states.iter().all(Option::is_some));
    bootstrap.finish_vnode_transition();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn zero_owner_topology_transition_is_not_an_acquisition() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let (scope, current) = single_owner_shuffle(1).await;
    let predecessor_version = current.assignment_version;
    let target_version = predecessor_version + 1;
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        predecessor_version,
        &[2],
        vec![CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(2),
        }],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        target_version,
        &[2],
        vec![CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(22),
        }],
    )
    .unwrap();
    scope
        .registry
        .set_assignment_and_version(Arc::from([NodeId(2)]), target_version);
    scope.sender.invalidate_assignment_fence();
    scope.receiver.invalidate_assignment_fence();

    let mut operator = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    operator.attach_cluster_shuffle(scope);
    let predecessor_registry = VnodeRegistry::new_unassigned(1);
    predecessor_registry.set_assignment_and_version(Arc::from([NodeId(2)]), predecessor_version);
    operator.local_assignment = predecessor_registry.versioned_snapshot();
    operator.applied_left_watermark = 100;
    operator.applied_right_watermark = 200;
    operator.local_frontiers = operator.applied_frontiers();
    operator.last_broadcasts = operator.local_frontiers;
    for port in 0..2 {
        let stale = InputFrontier {
            watermark: Some(900 + i64::try_from(port).unwrap()),
            idle: false,
        };
        let channel = operator.peer_channels[port].get_mut(&2).unwrap();
        channel.applied = stale;
        channel.accepted = stale;
    }

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
    let prepared = operator.prepared_vnode_transition.as_ref().unwrap();
    assert!(prepared.bootstrap_broadcast);
    for port in 0..2 {
        let channel = &prepared.peer_channels[port][&2];
        assert_eq!(channel.applied, operator.local_frontiers[port]);
        assert_eq!(channel.accepted, operator.local_frontiers[port]);
        assert!(channel.events.is_empty());
    }
    operator.publish_vnode_transition();

    assert_eq!(operator.local_assignment.version(), target_version);
    assert_eq!(operator.local_assignment.owners(), &[NodeId(2)]);
    assert_eq!(operator.last_broadcasts, [InputFrontier::default(); 2]);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn vnode_transition_preserves_checkpointed_admission_watermarks() {
    let vnode_count = 8;
    let (source_shuffle, _) = single_owner_shuffle(vnode_count).await;
    let mut source = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    source.attach_cluster_shuffle(source_shuffle);
    assert!(source
        .process(&[vec![], vec![]], &[300, 300])
        .await
        .unwrap()
        .is_empty());
    let checkpoint = source.checkpoint().unwrap().unwrap();

    let (restored_shuffle, fence) = single_owner_shuffle(vnode_count).await;
    let mut restored = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    restored.attach_cluster_shuffle(restored_shuffle);
    restored.restore(checkpoint).unwrap();
    let predecessor = install_single_owner_predecessor(&mut restored, &fence);
    restored
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &[],
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    restored.publish_vnode_transition();
    assert_eq!(
        restored.local_assignment.version(),
        fence.assignment_version
    );
    restored.finish_vnode_transition();

    let error = restored
        .process(
            &[vec![left_batch(&["late"], &[100], &[1.0])], vec![]],
            &[300, 300],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("below closed cutoff 300"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn vnode_capture_restore_preserves_cross_cycle_match() {
    let vnode_count = 8;
    let key_batch = left_batch(&["hot"], &[100], &[10.0]);
    let vnode = laminar_core::shuffle::row_vnodes(&key_batch, &[0], vnode_count).unwrap()[0];

    let (donor_shuffle, _) = single_owner_shuffle(vnode_count).await;
    let mut donor = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    donor.attach_cluster_shuffle(donor_shuffle);
    assert!(donor
        .process(&[vec![key_batch], vec![]], &[0, 0])
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        donor
            .vnode_states
            .iter()
            .zip(0_u32..)
            .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
            .collect::<Vec<_>>(),
        vec![vnode]
    );

    let captured = donor
        .checkpoint_vnodes(&[vnode], vnode_count, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(captured[0].vnode, vnode);
    let capture = captured
        .into_iter()
        .next()
        .and_then(|captured| captured.state)
        .expect("the first vnode capture is complete");
    let state = materialize_capture(capture).unwrap();

    let (restored_shuffle, fence) = single_owner_shuffle(vnode_count).await;
    let mut restored = IntervalJoinOperator::new(
        "test_interval",
        test_config(),
        None,
        laminar_sql::create_session_context(),
    );
    restored.attach_cluster_shuffle(restored_shuffle);
    let restores = [crate::operator_graph::ManagedVnodeRestore {
        participant_id: 1,
        vnode,
        state: &state,
    }];
    let predecessor = install_single_owner_predecessor(&mut restored, &fence);
    restored
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    let prepared = restored.managed_state_accounting().unwrap();
    assert!(prepared.prepared > 0);
    assert_eq!(prepared.retired, 0);
    restored.abort_vnode_transition();
    let aborted = restored.managed_state_accounting().unwrap();
    assert!(aborted.prepared > 0);
    assert_eq!(aborted.retired, 0);
    assert_eq!(
        restored.local_assignment.version(),
        predecessor.assignment_version
    );
    restored.finish_vnode_transition();
    restored
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    restored.publish_vnode_transition();
    assert_eq!(
        restored.local_assignment.version(),
        fence.assignment_version
    );
    assert_eq!(restored.applied_left_watermark, 0);
    assert_eq!(restored.applied_right_watermark, 0);
    restored.finish_vnode_transition();

    let output = restored
        .process(
            &[vec![], vec![right_batch(&["hot"], &[110], &[1.0])]],
            &[0, 0],
        )
        .await
        .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        restored
            .vnode_states
            .iter()
            .zip(0_u32..)
            .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
            .collect::<Vec<_>>(),
        vec![vnode]
    );

    assert!(restored
        .process(&[vec![], vec![]], &[500, 500])
        .await
        .unwrap()
        .is_empty());
    let state = restored.vnode_states[vnode as usize].as_ref().unwrap();
    assert_eq!(state.buffered_rows(), (0, 0));
}

#[test]
fn current_vnode_frame_version_rejects_legacy_present_and_absent_frames() {
    let config = test_config();
    let mut state = IntervalJoinState::new();
    let encoded = IntervalJoinOperator::serialize_state(
        &mut state,
        &config,
        "versioned vnode",
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
    )
    .unwrap();
    assert_eq!(encoded[0], PRESENT_VNODE);
    assert_eq!(encoded[1], VNODE_FRAME_VERSION);
    assert!(IntervalJoinOperator::decode_vnode_frame(
        &ABSENT_VNODE_FRAME,
        0,
        &config,
        None,
        "current absent",
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        None,
    )
    .unwrap()
    .is_none());

    let previous_version = VNODE_FRAME_VERSION - 1;
    let mut legacy_present = encoded.clone();
    legacy_present[1] = previous_version;
    let error = IntervalJoinOperator::decode_vnode_frame(
        &legacy_present,
        0,
        &config,
        None,
        "legacy present",
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        None,
    )
    .err()
    .expect("legacy present vnode frame must fail");
    assert!(error
        .to_string()
        .contains(&format!("version {previous_version} is unsupported")));

    let mut legacy_absent = ABSENT_VNODE_FRAME;
    legacy_absent[1] = previous_version;
    let error = IntervalJoinOperator::decode_vnode_frame(
        &legacy_absent,
        0,
        &config,
        None,
        "legacy absent",
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        None,
    )
    .err()
    .expect("legacy absent vnode frame must fail");
    assert!(error
        .to_string()
        .contains(&format!("version {previous_version} is unsupported")));

    let mut malformed = ABSENT_VNODE_FRAME;
    malformed[2] = 1;
    let error = IntervalJoinOperator::decode_vnode_frame(
        &malformed,
        0,
        &config,
        None,
        "malformed current",
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
        None,
    )
    .err()
    .expect("malformed current vnode frame must fail");
    assert!(error.to_string().contains("header is malformed"));
}

#[test]
fn whole_checkpoint_rejects_previous_operator_version() {
    let config = test_config();
    let mut source =
        IntervalJoinOperator::new("stale-whole", config.clone(), None, SessionContext::new());
    source.applied_left_watermark = 0;
    let mut checkpoint = source
        .capture_operator_checkpoint(u64::MAX)
        .unwrap()
        .unwrap()
        .checkpoint;
    checkpoint.version = OPERATOR_CHECKPOINT_VERSION - 1;
    let data = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
        .unwrap()
        .to_vec();
    let mut target = IntervalJoinOperator::new("stale-whole", config, None, SessionContext::new());
    let error = target.restore(OperatorCheckpoint { data }).unwrap_err();
    assert!(error
        .to_string()
        .contains("version or configuration does not match"));
}

#[test]
fn test_name() {
    let ctx = laminar_sql::create_session_context();
    let op = IntervalJoinOperator::new("my_interval_join", test_config(), None, ctx);
    assert_eq!(&*op.projection.op_name, "my_interval_join");
}
