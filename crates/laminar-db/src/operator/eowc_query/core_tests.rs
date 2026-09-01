use super::*;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use std::time::Duration;

const AGG_SQL: &str = "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol";

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn test_batch(ts_values: Vec<i64>) -> RecordBatch {
    let n = ts_values.len();
    let symbols: Vec<&str> = (0..n)
        .map(|i| if i % 2 == 0 { "AAPL" } else { "GOOG" })
        .collect();
    #[allow(clippy::cast_precision_loss)]
    let prices: Vec<f64> = (0..n).map(|i| (i as f64 + 1.0) * 100.0).collect();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(symbols)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Int64Array::from(ts_values)),
        ],
    )
    .unwrap()
}

fn aggregate_context() -> SessionContext {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let empty = MemTable::try_new(test_schema(), vec![vec![]]).unwrap();
    ctx.register_table("trades", Arc::new(empty)).unwrap();
    ctx
}

fn test_window_config() -> WindowOperatorConfig {
    WindowOperatorConfig {
        window_type: laminar_sql::translator::WindowType::Tumbling,
        time_column: "ts".to_string(),
        size: Duration::from_secs(60),
        slide: None,
        gap: None,
        offset_ms: 0,
        allowed_lateness: Duration::ZERO,
        emit_strategy: laminar_sql::parser::EmitStrategy::OnWindowClose,
        late_data_side_output: None,
    }
}

fn key_groups() -> KeyGroupCount {
    KeyGroupCount::try_from(8_u32).unwrap()
}

#[cfg(feature = "cluster")]
async fn cluster_scope(owners: [u64; 8]) -> ClusterShuffleConfig {
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::VnodeRegistry;

    let registry = Arc::new(VnodeRegistry::new(8));
    registry.set_assignment(Arc::from(owners.map(NodeId)));
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    sender.install_process_lease_deadline(deadline).unwrap();
    let fence = test_assignment_fence(registry.assignment_version(), &owners);
    sender.install_assignment_fence(&fence, &owners).unwrap();
    receiver.install_assignment_fence(&fence, &owners).unwrap();
    ClusterShuffleConfig {
        registry,
        sender,
        receiver,
        self_id: NodeId(1),
    }
}

#[cfg(feature = "cluster")]
fn test_assignment_fence(assignment_version: u64, owners: &[u64; 8]) -> CheckpointAssignmentFence {
    use laminar_core::checkpoint::CheckpointParticipant;

    let participants = owners
        .iter()
        .copied()
        .filter(|node_id| *node_id != 0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        })
        .collect();
    CheckpointAssignmentFence::from_owner_map(assignment_version, owners, participants).unwrap()
}

#[cfg(feature = "cluster")]
fn install_next_assignment(
    scope: &ClusterShuffleConfig,
    owners: [u64; 8],
) -> CheckpointAssignmentFence {
    let fence = test_assignment_fence(scope.registry.assignment_version() + 1, &owners);
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
        .set_assignment_and_version(Arc::from(owners.map(NodeId)), fence.assignment_version);
    fence
}

#[cfg(feature = "cluster")]
fn encode_handoff_whole(
    fence: &CheckpointAssignmentFence,
    participant_id: u64,
    frontier: InputFrontier,
    queued: bool,
) -> Vec<u8> {
    let channels = fence
        .participant_ids()
        .into_iter()
        .filter(|peer| *peer != participant_id)
        .enumerate()
        .map(|(index, peer)| EowcCheckpointChannel {
            peer,
            applied: frontier.into(),
            events: if queued && index == 0 {
                vec![EowcCheckpointEvent::Frontier {
                    recovery_gen: 0,
                    frontier: frontier.into(),
                }]
            } else {
                Vec::new()
            },
        })
        .collect();
    let checkpoint = EowcOperatorCheckpoint {
        version: OPERATOR_CHECKPOINT_VERSION,
        high_watermark_ms: EowcQueryOperator::frontier_watermark(frontier),
        cluster: Some(EowcClusterCheckpoint {
            assignment_version: fence.assignment_version,
            owner_map_digest: fence.assignment_digest,
            self_id: participant_id,
            local_frontier: frontier.into(),
            effective_frontier: frontier.into(),
            remote_peer_cursor: None,
            channels,
            data_ipc: Vec::new(),
        }),
    };
    rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
        .unwrap()
        .to_vec()
}

#[cfg(feature = "cluster")]
fn projected_batch_for_vnode(
    operator: &EowcQueryOperator,
    vnode: u32,
    price: f64,
) -> (String, RecordBatch) {
    let window = operator.state.as_ref().unwrap();
    let projection = window.compiled_projection().unwrap();
    for index in 0..1_000 {
        let symbol = format!("K{index}");
        let raw = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(vec![symbol.as_str()])),
                Arc::new(Float64Array::from(vec![price])),
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .unwrap();
        let projected = projection.evaluate(&raw).unwrap();
        let routed = crate::operator::sql_query::hash_rows_to_vnodes(
            &projected,
            window.num_group_cols(),
            u32::from(key_groups()),
        )
        .unwrap();
        if routed == [vnode] {
            return (symbol, projected);
        }
    }
    panic!("no test key hashes to vnode {vnode}");
}

fn materialize_capture(capture: crate::operator_graph::CapturedVnodeState) -> (u32, bytes::Bytes) {
    let state = capture.state.unwrap();
    let mut staged_bytes = state.retained_bytes();
    let bytes = state.materialize(&mut staged_bytes, u64::MAX).unwrap();
    (capture.vnode, bytes)
}

fn unaligned_archive_transport(bytes: &[u8]) -> bytes::Bytes {
    let mut transport = vec![0_u8; bytes.len() + CHECKPOINT_ARCHIVE_ALIGNMENT];
    let base = transport.as_ptr() as usize;
    let offset = (0..CHECKPOINT_ARCHIVE_ALIGNMENT)
        .find(|offset| !(base + offset).is_multiple_of(CHECKPOINT_ARCHIVE_ALIGNMENT))
        .expect("an archive transport offset must be unaligned");
    transport[offset..offset + bytes.len()].copy_from_slice(bytes);
    let bytes = bytes::Bytes::from(transport).slice(offset..offset + bytes.len());
    assert_ne!(bytes.as_ptr().align_offset(CHECKPOINT_ARCHIVE_ALIGNMENT), 0);
    bytes
}

#[tokio::test]
async fn grouped_window_restores_exact_unaligned_vnode_frames_and_frontier() {
    let mut original = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    original.initialize_managed_state().await.unwrap();
    original
        .process(&[vec![test_batch(vec![100, 200])]], &[10_000])
        .await
        .unwrap();

    let required = (0..u32::from(key_groups())).collect::<Vec<_>>();
    let captures = original
        .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(captures.len(), required.len());
    let frames = captures
        .into_iter()
        .map(materialize_capture)
        .map(|(vnode, state)| (vnode, unaligned_archive_transport(&state)))
        .collect::<Vec<_>>();
    assert!(original
        .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
        .unwrap()
        .unwrap()
        .is_empty());
    original.process(&[vec![]], &[20_000]).await.unwrap();
    assert!(original
        .checkpoint_vnodes(&required, u32::from(key_groups()), u64::MAX)
        .unwrap()
        .unwrap()
        .is_empty());
    let whole = original.checkpoint().unwrap().unwrap();

    let mut restored = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    restored.initialize_managed_state().await.unwrap();
    assert!(restored
        .restore_vnode(frames[0].0, u32::from(key_groups()), &frames[0].1)
        .unwrap_err()
        .to_string()
        .contains("whole watermark frame"));
    restored.restore(whole).unwrap();
    #[cfg(feature = "cluster")]
    assert_eq!(restored.restored_output_frontier(), None);
    assert_eq!(restored.state.as_ref().unwrap().high_watermark_ms(), 20_000);
    assert!(restored
        .restore_vnode(1, u32::from(key_groups()), &frames[0].1)
        .is_err());
    for (vnode, state) in &frames {
        restored
            .restore_vnode(*vnode, u32::from(key_groups()), state)
            .unwrap();
    }

    let expected = original.process(&[vec![]], &[60_000]).await.unwrap();
    let actual = restored.process(&[vec![]], &[60_000]).await.unwrap();
    assert_eq!(actual, expected);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_plan_orders_idle_revival_before_data_and_frontier() {
    use laminar_core::shuffle::ShuffleMessage;

    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let mut operator = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    operator.initialize_managed_state().await.unwrap();
    let (_, projected) = projected_batch_for_vnode(&operator, 1, 42.0);
    operator.attach_cluster_scope(scope.clone());
    let idle = InputFrontier {
        watermark: Some(100),
        idle: true,
    };
    operator.local_frontier = idle;
    operator.last_broadcast = idle;
    operator.effective_frontier = idle;
    let channel = operator.peer_channels.get_mut(&2).unwrap();
    channel.applied = idle;
    channel.accepted = idle;
    let active = InputFrontier {
        watermark: Some(200),
        idle: false,
    };
    let assignment = scope.registry.versioned_snapshot();
    let plan = operator
        .plan_cluster_batches(vec![projected], active, &scope, &assignment, &[2])
        .unwrap();
    assert!(plan.local_batches.is_empty());
    assert_eq!(plan.effective_frontier, active);
    assert_eq!(plan.outbound.len(), 3);
    assert!(matches!(
        &plan.outbound[0],
        (
            2,
            ShuffleMessage::Frontier {
                watermark: Some(100),
                idle: false,
                ..
            }
        )
    ));
    assert!(matches!(
        &plan.outbound[1],
        (
            2,
            ShuffleMessage::Data {
                routed_vnodes,
                ..
            }
        ) if routed_vnodes.as_ref() == [1]
    ));
    assert!(matches!(
        &plan.outbound[2],
        (
            2,
            ShuffleMessage::Frontier {
                watermark: Some(200),
                idle: false,
                ..
            }
        )
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpointed_remote_frontiers_compare_in_receiver_domain() {
    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let mut operator = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    operator.initialize_managed_state().await.unwrap();
    operator.attach_cluster_scope(scope.clone());
    operator.effective_frontier = InputFrontier {
        watermark: Some(500),
        idle: false,
    };
    let idle = InputFrontier {
        watermark: Some(100),
        idle: true,
    };
    let channel = operator.peer_channels.get_mut(&2).unwrap();
    channel.applied = idle;
    channel.accepted = idle;
    let assignment = scope.registry.assignment_version();
    let recovery = scope.receiver.recovery_gen();
    let active = |watermark| InputFrontier {
        watermark: Some(watermark),
        idle: false,
    };

    operator
        .stage_checkpointed_shuffle_frontier("managed_window", 2, active(100), assignment, recovery)
        .unwrap();
    assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(500));
    operator
        .stage_checkpointed_shuffle_frontier("managed_window", 2, active(150), assignment, recovery)
        .unwrap();
    assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(500));
    operator
        .stage_checkpointed_shuffle_frontier("managed_window", 2, active(550), assignment, recovery)
        .unwrap();
    assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(550));
    assert!(operator
        .stage_checkpointed_shuffle_frontier(
            "managed_window",
            2,
            InputFrontier {
                watermark: None,
                idle: false,
            },
            assignment,
            recovery,
        )
        .is_err());
    assert!(
        operator
            .stage_checkpointed_shuffle_frontier(
                "managed_window",
                2,
                active(525),
                assignment,
                recovery,
            )
            .is_err()
    );
    assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(550));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restored_frontier_bootstrap_precedes_live_source_frontier() {
    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let mut operator = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    operator.initialize_managed_state().await.unwrap();
    let (symbol, local) = projected_batch_for_vnode(&operator, 0, 42.0);
    let buffered = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(vec![symbol.as_str()])),
            Arc::new(Float64Array::from(vec![42.0])),
            Arc::new(Int64Array::from(vec![1_000])),
        ],
    )
    .unwrap();
    operator.attach_cluster_scope(scope.clone());
    let restored = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let live = InputFrontier {
        watermark: Some(1_000),
        idle: false,
    };
    operator.local_frontier = restored;
    operator.effective_frontier = restored;
    operator.last_broadcast = InputFrontier::default();
    operator
        .state
        .as_mut()
        .unwrap()
        .restore_high_watermark_ms(100)
        .unwrap();
    let channel = operator.peer_channels.get_mut(&2).unwrap();
    channel.applied = restored;
    channel.accepted = restored;

    assert!(!operator.wants_input());
    let assignment = scope.registry.versioned_snapshot();
    let bootstrap = operator.cluster_cycle_local_frontier(live, false).unwrap();
    assert_eq!(bootstrap, restored);
    let plan = operator
        .plan_cluster_batches(Vec::new(), bootstrap, &scope, &assignment, &[2])
        .unwrap();
    assert!(matches!(
        plan.outbound.as_slice(),
        [(
            2,
            ShuffleMessage::Frontier {
                watermark: Some(100),
                idle: false,
                ..
            }
        )]
    ));
    operator.process_cluster(&[Vec::new()], live).await.unwrap();
    let mut pending = operator.pending_cluster_input.take().unwrap();
    assert_eq!(pending.local_frontier, restored);
    assert!(pending.local_batches.is_empty());
    pending.send.take().unwrap().abort();

    // Simulate completion of the bootstrap send. The graph may now release its retained row,
    // and the ordinary node-local frontier is used without being globally frozen.
    operator.last_broadcast = restored;
    assert!(operator.wants_input());
    let admitted = operator.cluster_cycle_local_frontier(live, true).unwrap();
    assert_eq!(admitted, live);
    let plan = operator
        .plan_cluster_batches(vec![local], admitted, &scope, &assignment, &[2])
        .unwrap();
    assert_eq!(plan.local_batches.len(), 1);
    assert_eq!(plan.local_frontier, live);
    operator
        .process_cluster(&[vec![buffered]], live)
        .await
        .unwrap();
    let mut pending = operator.pending_cluster_input.take().unwrap();
    assert_eq!(pending.local_frontier, live);
    assert_eq!(pending.local_batches.len(), 1);
    pending.send.take().unwrap().abort();
}

#[cfg(feature = "cluster")]
#[test]
fn stale_source_observation_preserves_the_installed_frontier_floor() {
    let mut operator = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    let proven = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    operator.local_frontier = proven;
    operator.effective_frontier = proven;
    operator.last_broadcast = proven;

    for observed in [
        InputFrontier::default(),
        InputFrontier {
            watermark: Some(90),
            idle: false,
        },
    ] {
        for has_data in [false, true] {
            let normalized = operator
                .normalized_local_frontier(observed, has_data)
                .unwrap();
            assert_eq!(normalized.watermark, proven.watermark);
            assert_eq!(normalized.idle, observed.idle);
        }
    }

    let invalid = InputFrontier {
        watermark: Some(i64::MIN),
        idle: false,
    };
    for has_data in [false, true] {
        assert!(operator
            .normalized_local_frontier(invalid, has_data)
            .is_err());
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn zero_admission_send_restarts_once_without_becoming_runnable() {
    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let mut operator = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    operator.initialize_managed_state().await.unwrap();
    operator.attach_cluster_scope(scope);

    let retry_plan = vec![(
        2,
        ShuffleMessage::Frontier {
            stage: "managed_window".to_string(),
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
    operator.pending_cluster_input = Some(PendingEowcClusterInput {
        local_batches: Vec::new(),
        outbound: None,
        local_frontier: InputFrontier::default(),
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
        .process_cluster(&[Vec::new()], InputFrontier::default())
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
async fn pending_cluster_send_drains_remote_data_before_committing_local_cut() {
    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let mut operator = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    operator.initialize_managed_state().await.unwrap();
    let (local_symbol, local) = projected_batch_for_vnode(&operator, 0, 8.0);
    let (remote_symbol, remote) = projected_batch_for_vnode(&operator, 0, 34.0);
    let (_, outbound_batch) = projected_batch_for_vnode(&operator, 1, 1.0);
    assert_eq!(local_symbol, remote_symbol);
    operator.attach_cluster_scope(scope.clone());
    let close = InputFrontier {
        watermark: Some(60_000),
        idle: false,
    };
    let assignment = scope.registry.versioned_snapshot();
    let plan = operator
        .plan_cluster_batches(
            vec![local, outbound_batch],
            close,
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    assert_eq!(plan.local_batches.len(), 1);
    assert!(plan
        .outbound
        .iter()
        .any(|(_, message)| matches!(message, ShuffleMessage::Data { .. })));
    let accounted_bytes = operator.cluster_input_plan_bytes(&plan).unwrap();
    let EowcClusterInputPlan {
        local_batches,
        outbound,
        local_frontier,
        effective_frontier: _,
    } = plan;
    let baseline = operator.managed_state_accounting().unwrap().live;
    let (release, wait) = tokio::sync::oneshot::channel();
    let send = tokio::spawn(async move {
        let _ = wait.await;
        drop(outbound);
        (Ok(()), None)
    });
    operator.pending_cluster_input = Some(PendingEowcClusterInput {
        local_batches,
        outbound: None,
        local_frontier,
        send: Some(send),
        accounted_bytes,
    });
    let assignment_version = scope.registry.assignment_version();
    let recovery_gen = scope.receiver.recovery_gen();
    operator
        .stage_checkpointed_shuffle(
            "managed_window",
            crate::operator::RetainedBatch::restored_channel(
                remote,
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
            "managed_window",
            2,
            close,
            assignment_version,
            recovery_gen,
        )
        .unwrap();
    assert_eq!(operator.queued_remote_events, 2);
    assert_ne!(operator.queued_payload_bytes, 0);
    assert!(operator.deferred_work_is_runnable());

    let output = tokio::time::timeout(
        Duration::from_millis(50),
        operator.process_cluster(&[Vec::new()], InputFrontier::default()),
    )
    .await
    .expect("pending send blocked the graph task")
    .unwrap();
    assert!(output.is_empty());
    assert!(!operator.wants_input());
    assert!(operator.checkpoint_drain_pending());
    assert!(operator.capture_operator_checkpoint(usize::MAX).is_err());
    assert_eq!(operator.local_frontier, InputFrontier::default());
    assert_eq!(operator.queued_remote_events, 1);
    assert_eq!(operator.queued_payload_bytes, 0);
    assert!(!operator
        .pending_cluster_input
        .as_ref()
        .unwrap()
        .send
        .as_ref()
        .unwrap()
        .is_finished());
    assert!(operator.managed_state_accounting().unwrap().live >= baseline + accounted_bytes);

    let output = tokio::time::timeout(
        Duration::from_millis(50),
        operator.process_cluster(&[Vec::new()], InputFrontier::default()),
    )
    .await
    .expect("remote frontier waited for the blocked send")
    .unwrap();
    assert!(output.is_empty());
    assert_eq!(operator.queued_remote_events, 0);
    assert_eq!(operator.peer_channels[&2].applied, close);
    assert_eq!(operator.local_frontier, InputFrontier::default());
    assert!(operator.pending_cluster_input.is_some());
    assert!(!operator.deferred_work_is_runnable());

    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
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
    .expect("pending send task did not finish");
    assert!(!operator.deferred_work_is_runnable());
    let output = operator
        .process_cluster(&[Vec::new()], InputFrontier::default())
        .await
        .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let total = output[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(total.value(0), 42.0);
    assert!(operator.pending_cluster_input.is_none());
    assert_eq!(operator.local_frontier, close);
    assert_eq!(operator.effective_frontier, close);
    assert!(operator.wants_input());
    assert!(!operator.checkpoint_drain_pending());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_channel_checkpoint_replays_data_before_window_close() {
    let scope = cluster_scope([1, 2, 2, 2, 2, 2, 2, 2]).await;
    let mut operator = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    operator.initialize_managed_state().await.unwrap();
    let (symbol, projected) = projected_batch_for_vnode(&operator, 0, 42.0);
    operator.attach_cluster_scope(scope.clone());
    let idle = InputFrontier {
        watermark: Some(0),
        idle: true,
    };
    let close = InputFrontier {
        watermark: Some(60_000),
        idle: false,
    };
    operator.local_frontier = idle;
    operator.last_broadcast = idle;
    operator.effective_frontier = idle;
    operator
        .state
        .as_mut()
        .unwrap()
        .restore_high_watermark_ms(0)
        .unwrap();
    let channel = operator.peer_channels.get_mut(&2).unwrap();
    channel.applied = idle;
    channel.accepted = idle;
    let assignment_version = scope.registry.assignment_version();
    let recovery_gen = scope.receiver.recovery_gen();
    operator
        .stage_checkpointed_shuffle_frontier(
            "managed_window",
            2,
            InputFrontier {
                watermark: Some(0),
                idle: false,
            },
            assignment_version,
            recovery_gen,
        )
        .unwrap();
    let retained = crate::operator::RetainedBatch::restored_channel(
        projected,
        2,
        assignment_version,
        recovery_gen,
        Arc::from([0_u32]),
    );
    operator
        .stage_checkpointed_shuffle("managed_window", retained, i64::MIN)
        .unwrap();
    operator
        .stage_checkpointed_shuffle_frontier(
            "managed_window",
            2,
            close,
            assignment_version,
            recovery_gen,
        )
        .unwrap();
    let checkpoint = operator.checkpoint().unwrap().unwrap();

    let mut restored = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    restored.initialize_managed_state().await.unwrap();
    restored.attach_cluster_scope(scope);
    restored.restore(checkpoint).unwrap();
    let channel = &restored.peer_channels[&2];
    assert!(matches!(
        &channel.events[0].payload,
        EowcRemoteEventPayload::Frontier(_)
    ));
    assert!(matches!(
        &channel.events[1].payload,
        EowcRemoteEventPayload::Data(_)
    ));
    assert!(matches!(
        &channel.events[2].payload,
        EowcRemoteEventPayload::Frontier(_)
    ));
    assert_eq!(channel.applied, idle);
    assert_eq!(channel.accepted, close);
    assert!(!restored.wants_input());

    let first = restored
        .process_with_frontiers(&[], std::slice::from_ref(&close))
        .await
        .unwrap();
    assert!(first.is_empty());
    let second = restored
        .process_with_frontiers(&[], std::slice::from_ref(&close))
        .await
        .unwrap();
    assert!(second.is_empty());
    let third = restored
        .process_with_frontiers(&[], std::slice::from_ref(&close))
        .await
        .unwrap();
    assert_eq!(third.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let output = &third[0];
    let output_symbol = output
        .column_by_name("symbol")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let total = output
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(output_symbol.value(0), symbol);
    assert_eq!(total.value(0), 42.0);
    assert_eq!(restored.queued_remote_events, 0);
    assert_eq!(restored.effective_frontier, close);
    assert_eq!(restored.state.as_ref().unwrap().high_watermark_ms(), 60_000);
    assert!(restored.wants_input());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn fresh_owner_reconciles_portable_cut_atomically() {
    use crate::operator_graph::{ManagedVnodeRestore, ManagedWholeRestore};

    let new_operator = || {
        EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        )
    };
    let cut = InputFrontier {
        watermark: Some(20_000),
        idle: false,
    };
    let mut donor = new_operator();
    donor.initialize_managed_state().await.unwrap();
    let (_, projected) = projected_batch_for_vnode(&donor, 0, 42.0);
    let output = EowcQueryOperator::apply_routed_and_close(
        donor.state.as_mut().unwrap(),
        &[(projected, Some(0))],
        10_000,
        "managed_window",
    )
    .unwrap();
    assert!(output.is_empty());
    let frames = donor
        .checkpoint_vnodes(&[0, 1], u32::from(key_groups()), u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(materialize_capture)
        .collect::<Vec<_>>();
    assert_eq!(
        frames.iter().map(|(vnode, _)| *vnode).collect::<Vec<_>>(),
        [0, 1]
    );

    let target_owners = [1, 1, 2, 2, 2, 2, 2, 2];
    let scope = cluster_scope(target_owners).await;
    let _skipped_assignment = install_next_assignment(&scope, target_owners);
    let target_fence = install_next_assignment(&scope, target_owners);
    let predecessor_owners = [2, 3, 2, 2, 2, 2, 2, 2];
    let predecessor =
        test_assignment_fence(target_fence.assignment_version - 2, &predecessor_owners);
    let predecessor_nodes = predecessor_owners.map(NodeId);

    let mut target = new_operator();
    target.initialize_managed_state().await.unwrap();
    target.attach_cluster_scope(scope);
    let pristine_core_bytes = target.state.as_ref().unwrap().accounted_state_bytes();
    let pristine_accounting = target.managed_state_accounting().unwrap();
    let restores = [
        ManagedVnodeRestore {
            participant_id: 2,
            vnode: 0,
            state: frames[0].1.as_ref(),
        },
        ManagedVnodeRestore {
            participant_id: 3,
            vnode: 1,
            state: frames[1].1.as_ref(),
        },
    ];

    let queued_donor = encode_handoff_whole(&predecessor, 2, cut, true);
    let donor3 = encode_handoff_whole(&predecessor, 3, cut, false);
    let queued_whole = [
        ManagedWholeRestore {
            participant_id: 2,
            state: &queued_donor,
        },
        ManagedWholeRestore {
            participant_id: 3,
            state: &donor3,
        },
    ];
    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &queued_whole,
            mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                predecessor_owners: &predecessor_nodes,
            },
        })
        .unwrap_err();
    assert_eq!(
        target.managed_state_accounting().unwrap(),
        pristine_accounting
    );
    assert_eq!(target.state.as_ref().unwrap().high_watermark_ms(), i64::MIN);
    assert_eq!(
        target.cluster_assignment.as_ref().unwrap().version(),
        target_fence.assignment_version
    );

    let donor2 = encode_handoff_whole(&predecessor, 2, cut, false);
    let idle_cut = InputFrontier {
        watermark: cut.watermark,
        idle: true,
    };
    let idle_donor = encode_handoff_whole(&predecessor, 3, idle_cut, false);
    let disagreeing_whole = [
        ManagedWholeRestore {
            participant_id: 2,
            state: &donor2,
        },
        ManagedWholeRestore {
            participant_id: 3,
            state: &idle_donor,
        },
    ];
    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &disagreeing_whole,
            mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                predecessor_owners: &predecessor_nodes,
            },
        })
        .unwrap_err();
    assert_eq!(
        target.managed_state_accounting().unwrap(),
        pristine_accounting
    );
    assert_eq!(
        target.state.as_ref().unwrap().accounted_state_bytes(),
        pristine_core_bytes
    );
    assert_eq!(target.local_frontier, InputFrontier::default());
    assert_eq!(target.effective_frontier, InputFrontier::default());
    assert_eq!(target.cluster_peers.as_ref(), &[2]);

    let unaligned_donor2 = unaligned_archive_transport(&donor2);
    let unaligned_donor3 = unaligned_archive_transport(&donor3);
    let valid_whole = [
        ManagedWholeRestore {
            participant_id: 2,
            state: &unaligned_donor2,
        },
        ManagedWholeRestore {
            participant_id: 3,
            state: &unaligned_donor3,
        },
    ];
    target
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target_fence,
            revoked: &rustc_hash::FxHashSet::default(),
            restores: &restores,
            whole_restores: &valid_whole,
            mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                predecessor_owners: &predecessor_nodes,
            },
        })
        .unwrap();
    assert!(target.managed_state_accounting().unwrap().prepared > 0);
    assert_eq!(target.state.as_ref().unwrap().high_watermark_ms(), i64::MIN);
    assert_eq!(
        target.state.as_ref().unwrap().accounted_state_bytes(),
        pristine_core_bytes
    );
    assert_eq!(target.local_frontier, InputFrontier::default());
    assert_eq!(
        target.cluster_assignment.as_ref().unwrap().version(),
        target_fence.assignment_version
    );

    target.publish_vnode_transition();
    assert_eq!(
        target.cluster_assignment.as_ref().unwrap().version(),
        target_fence.assignment_version
    );
    assert_eq!(
        target.cluster_assignment.as_ref().unwrap().owners(),
        target_owners.map(NodeId)
    );
    assert_eq!(
        target.cluster_assignment_digest,
        Some(target_fence.assignment_digest)
    );
    assert_eq!(target.cluster_peers.as_ref(), &[2]);
    assert_eq!(target.peer_channels.len(), 1);
    let channel = &target.peer_channels[&2];
    assert_eq!(channel.applied, cut);
    assert_eq!(channel.accepted, cut);
    assert!(channel.events.is_empty());
    assert_eq!(target.local_frontier, cut);
    assert_eq!(target.effective_frontier, cut);
    assert_eq!(target.last_broadcast, InputFrontier::default());
    assert!(target.checkpoint_drain_pending());
    assert_eq!(target.remote_peer_cursor, None);
    assert_eq!(target.queued_payload_bytes, 0);
    assert_eq!(target.queued_event_capacity_bytes, 0);
    assert_eq!(target.queued_remote_events, 0);
    assert_eq!(target.state.as_ref().unwrap().high_watermark_ms(), 20_000);
    assert!(target.state.as_ref().unwrap().accounted_state_bytes() > pristine_core_bytes);
    assert!(target.managed_state_accounting().unwrap().retired > 0);
    target.finish_vnode_transition();
    assert_eq!(target.managed_state_accounting().unwrap().retired, 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn surviving_owner_preserves_channel_and_bootstraps_new_peer() {
    let predecessor_owners = [1, 2, 1, 4, 1, 5, 1, 5];
    let scope = cluster_scope(predecessor_owners).await;
    let predecessor =
        test_assignment_fence(scope.registry.assignment_version(), &predecessor_owners);
    let mut operator = EowcQueryOperator::new(
        "managed_window",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        aggregate_context(),
        key_groups(),
        None,
    );
    operator.initialize_managed_state().await.unwrap();
    operator.attach_cluster_scope(scope.clone());
    let effective = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let local_before = InputFrontier {
        watermark: Some(80),
        idle: true,
    };
    let local_after = InputFrontier {
        watermark: Some(100),
        idle: true,
    };
    let surviving = InputFrontier {
        watermark: Some(120),
        idle: true,
    };
    let restarted = InputFrontier {
        watermark: Some(140),
        idle: true,
    };
    operator.local_frontier = local_before;
    operator.last_broadcast = local_before;
    operator.effective_frontier = effective;
    operator
        .state
        .as_mut()
        .unwrap()
        .restore_high_watermark_ms(100)
        .unwrap();
    let channel = operator.peer_channels.get_mut(&2).unwrap();
    channel.applied = surviving;
    channel.accepted = surviving;
    let channel = operator.peer_channels.get_mut(&4).unwrap();
    channel.applied = restarted;
    channel.accepted = restarted;
    let channel = operator.peer_channels.get_mut(&5).unwrap();
    channel.applied = effective;
    channel.accepted = effective;

    let target_owners = [1, 2, 1, 4, 1, 3, 1, 3];
    let mut target = test_assignment_fence(scope.registry.assignment_version() + 1, &target_owners);
    target
        .participants
        .iter_mut()
        .find(|participant| participant.node_id == 4)
        .unwrap()
        .boot_incarnation = uuid::Uuid::from_u128(44);
    scope
        .sender
        .install_assignment_fence(&target, &target_owners)
        .unwrap();
    scope
        .receiver
        .install_assignment_fence(&target, &target_owners)
        .unwrap();
    scope.registry.set_assignment_and_version(
        Arc::from(target_owners.map(NodeId)),
        target.assignment_version,
    );
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
    assert_eq!(
        operator.cluster_assignment.as_ref().unwrap().version(),
        predecessor.assignment_version
    );
    assert_eq!(operator.cluster_peers.as_ref(), &[2, 4, 5]);
    assert!(!operator.peer_channels.contains_key(&3));
    assert_eq!(operator.peer_channels[&2].applied, surviving);
    assert_eq!(operator.peer_channels[&4].applied, restarted);
    assert_eq!(operator.peer_channels[&5].applied, effective);
    assert_eq!(operator.local_frontier, local_before);
    assert_eq!(operator.last_broadcast, local_before);
    assert_eq!(operator.effective_frontier, effective);
    assert_eq!(operator.state.as_ref().unwrap().high_watermark_ms(), 100);

    operator.publish_vnode_transition();
    assert_eq!(
        operator.cluster_assignment.as_ref().unwrap().version(),
        target.assignment_version
    );
    assert_eq!(
        operator.cluster_assignment.as_ref().unwrap().owners(),
        target_owners.map(NodeId)
    );
    assert_eq!(
        operator.cluster_assignment_digest,
        Some(target.assignment_digest)
    );
    assert_eq!(operator.cluster_peers.as_ref(), &[2, 3, 4]);
    assert_eq!(operator.peer_channels.len(), 3);
    let surviving_channel = &operator.peer_channels[&2];
    assert_eq!(surviving_channel.applied, surviving);
    assert_eq!(surviving_channel.accepted, surviving);
    assert!(surviving_channel.events.is_empty());
    let new_channel = &operator.peer_channels[&3];
    assert_eq!(new_channel.applied, effective);
    assert_eq!(new_channel.accepted, effective);
    assert!(new_channel.events.is_empty());
    let restarted_channel = &operator.peer_channels[&4];
    assert_eq!(restarted_channel.applied, effective);
    assert_eq!(restarted_channel.accepted, effective);
    assert!(restarted_channel.events.is_empty());
    assert!(!operator.peer_channels.contains_key(&5));
    assert_eq!(operator.local_frontier, local_after);
    assert_eq!(operator.effective_frontier, effective);
    assert_eq!(operator.last_broadcast, InputFrontier::default());
    assert!(operator.checkpoint_drain_pending());
    assert_eq!(
        operator
            .normalized_local_frontier(local_before, false)
            .unwrap(),
        local_after
    );
    assert_eq!(
        operator
            .normalized_local_frontier(
                InputFrontier {
                    watermark: Some(90),
                    idle: false,
                },
                false,
            )
            .unwrap(),
        effective
    );
    assert_eq!(operator.remote_peer_cursor, None);
    assert_eq!(operator.queued_payload_bytes, 0);
    assert_eq!(operator.queued_event_capacity_bytes, 0);
    assert_eq!(operator.queued_remote_events, 0);
    assert_eq!(operator.state.as_ref().unwrap().high_watermark_ms(), 100);
    operator.finish_vnode_transition();
    assert_eq!(operator.managed_state_accounting().unwrap().retired, 0);

    operator.last_broadcast = operator.local_frontier;
    let exit_owners = [2, 2, 3, 4, 2, 3, 4, 2];
    let exit = test_assignment_fence(scope.registry.assignment_version() + 1, &exit_owners);
    scope.sender.invalidate_assignment_fence();
    scope.receiver.invalidate_assignment_fence();
    scope
        .registry
        .set_assignment_and_version(Arc::from(exit_owners.map(NodeId)), exit.assignment_version);
    let revoked = [0_u32, 2, 4, 6]
        .into_iter()
        .collect::<rustc_hash::FxHashSet<_>>();
    operator
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &target,
            target: &exit,
            revoked: &revoked,
            restores: &[],
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    operator.publish_vnode_transition();
    assert_eq!(operator.cluster_peers.as_ref(), &[2, 3, 4]);
    assert!(operator.peer_channels.values().all(|channel| {
        channel.applied == effective && channel.accepted == effective && channel.events.is_empty()
    }));
    assert_eq!(operator.local_frontier, effective);
    assert_eq!(operator.effective_frontier, effective);
    assert_eq!(operator.last_broadcast, effective);
    assert!(!operator.checkpoint_drain_pending());
    operator
        .validate_drained_transition_cut(
            operator.cluster_assignment.as_ref().unwrap(),
            operator.state.as_ref().unwrap(),
            NodeId(1),
        )
        .unwrap();
    operator.finish_vnode_transition();
}

#[tokio::test]
async fn whole_restore_rejects_unwatermarked_live_state() {
    let new_operator = || {
        EowcQueryOperator::new(
            "managed_window",
            AGG_SQL,
            Some(EmitClause::OnWindowClose),
            Some(test_window_config()),
            aggregate_context(),
            key_groups(),
            None,
        )
    };
    let mut donor = new_operator();
    donor.initialize_managed_state().await.unwrap();
    let checkpoint = donor.checkpoint().unwrap().unwrap();

    let mut target = new_operator();
    target.initialize_managed_state().await.unwrap();
    target
        .process(&[vec![test_batch(vec![100])]], &[i64::MIN])
        .await
        .unwrap();
    assert!(!target.state.as_ref().unwrap().is_pristine_for_restore());
    assert!(target.restore(checkpoint).is_err());
}

#[test]
fn test_eowc_operator_creation() {
    let ctx = laminar_sql::create_session_context();
    let op = EowcQueryOperator::new(
        "test_eowc",
        "SELECT symbol, SUM(price) FROM trades GROUP BY symbol",
        Some(EmitClause::OnWindowClose),
        None,
        ctx,
        key_groups(),
        None,
    );
    assert_eq!(&*op.op_name, "test_eowc");
    assert!(op.state.is_none());
}

#[test]
fn core_window_partial_apply_wrapper_preserves_terminal_disposition() {
    let error = EowcQueryOperator::core_window_apply_error(
        "test_eowc",
        "window update",
        DbError::PipelineTerminal("invalid compiled expression".into()),
    );
    assert!(matches!(
        &error,
        DbError::PipelineTerminal(reason) if reason == "invalid compiled expression"
    ));
    assert!(error.requires_pipeline_halt());
}

#[cfg(feature = "cluster")]
#[test]
fn core_window_shuffle_wrappers_preserve_terminal_disposition() {
    fn assert_terminal(error: DbError, expected: &str) {
        let DbError::ShuffleTerminal(reason) = error else {
            panic!("expected permanent shuffle halt, got {error}");
        };
        assert_eq!(reason, expected);
    }

    let operator = EowcQueryOperator::new(
        "test_eowc",
        "SELECT symbol, SUM(price) FROM trades GROUP BY symbol",
        Some(EmitClause::OnWindowClose),
        None,
        laminar_sql::create_session_context(),
        key_groups(),
        None,
    );
    assert_terminal(
        operator.remote_replay_error(DbError::ShuffleTerminal("remote replay".into())),
        "remote replay",
    );
    assert_terminal(
        operator.outbound_finalize_error(DbError::ShuffleTerminal("outbound".into())),
        "outbound",
    );
}

#[test]
fn test_eowc_checkpoint_uninit_returns_none() {
    let ctx = laminar_sql::create_session_context();
    let mut op = EowcQueryOperator::new(
        "test_eowc",
        "SELECT * FROM trades",
        Some(EmitClause::OnWindowClose),
        None,
        ctx,
        key_groups(),
        None,
    );
    let cp = op.checkpoint().unwrap();
    assert!(cp.is_none());
}

#[tokio::test]
async fn test_eowc_process_empty_inputs() {
    let ctx = aggregate_context();
    let mut op = EowcQueryOperator::new(
        "test_eowc",
        AGG_SQL,
        Some(EmitClause::OnWindowClose),
        Some(test_window_config()),
        ctx,
        key_groups(),
        None,
    );
    op.initialize_managed_state().await.unwrap();

    let result = op.process(&[vec![]], &[0]).await.unwrap();
    assert!(result.is_empty());
}
