use super::super::callback::CycleOutcome;
#[cfg(feature = "cluster")]
use super::source_drain::{
    apply_latest_source_drain_command, publish_source_drain_ready, resolve_pending_source_drain,
};
use super::source_lifecycle::source_barrier_release_covers;
use super::source_runtime::{source_actor_terminal_guard, SourceActorTerminalState};
use super::*;
use arrow::array::{BinaryArray, Int64Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

fn manual_request(reply: crate::db::ForceCheckpointReply) -> ForceCheckpointRequest {
    ForceCheckpointRequest {
        reply,
        reservation_claim: None,
        deadline: tokio::time::Instant::now() + Duration::from_secs(60),
    }
}

fn unreserved_manual_request(
    reply: crate::db::ForceCheckpointReply,
) -> (
    ForceCheckpointRequest,
    crate::db::ForceCheckpointReservationWait,
) {
    manual_request_until(reply, tokio::time::Instant::now() + Duration::from_secs(60))
}

fn manual_request_until(
    reply: crate::db::ForceCheckpointReply,
    deadline: tokio::time::Instant,
) -> (
    ForceCheckpointRequest,
    crate::db::ForceCheckpointReservationWait,
) {
    let (reservation_claim, reservation_wait) = crate::db::ForceCheckpointReservationClaim::new();
    (
        ForceCheckpointRequest {
            reply,
            reservation_claim: Some(reservation_claim),
            deadline,
        },
        reservation_wait,
    )
}

fn replayable_append_only_source_contract() -> laminar_connectors::connector::SourceContract {
    laminar_connectors::connector::SourceContract::new(
        laminar_connectors::connector::SourceConsistency::Replayable,
        laminar_connectors::connector::SourceTopology::Singleton,
        laminar_connectors::connector::SourceInputMode::AppendOnly,
    )
}

#[test]
fn source_metadata_stays_row_aligned_and_mutations_are_route_admitted() {
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceMutation, SourceRowPositions, SOURCE_MUTATION_COLUMN, SOURCE_ORDER_KEY_COLUMN,
        SOURCE_PARTITION_COLUMN, SOURCE_SUB_OFFSET_COLUMN,
    };

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let positions = SourceRowPositions::try_new(
        BinaryArray::from(vec![&b"p0"[..], &b"p0"[..]]),
        BinaryArray::from(vec![&b"o1"[..], &b"o2"[..]]),
        UInt32Array::from(vec![0, 1]),
    )
    .unwrap();
    let positioned_schema = schema_with_source_row_positions(&schema).unwrap();
    let mutation_schema = schema_with_source_mutations_and_row_positions(&schema).unwrap();
    let output = prepare_encoded_source_batch(
        "positioned",
        &schema,
        &positioned_schema,
        &mutation_schema,
        &[],
        &[],
        SourceRowPositionCapability::OrderedDeterministic,
        SourceBatch::positioned(records.clone(), positions.clone()).unwrap(),
    )
    .unwrap();

    assert_eq!(output.schema(), positioned_schema);
    assert!(output.column_by_name(SOURCE_MUTATION_COLUMN).is_none());
    assert!(output.column_by_name(SOURCE_PARTITION_COLUMN).is_some());
    assert!(output.column_by_name(SOURCE_ORDER_KEY_COLUMN).is_some());
    assert!(output.column_by_name(SOURCE_SUB_OFFSET_COLUMN).is_some());

    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.late_filter = MockLateFilter::DropFirst;
    let mut events = 0;
    coordinator
        .stage_batch(
            0,
            &output,
            SourceBatchCursor::Complete(checkpoint_at(1)),
            &mut callback,
            &mut events,
        )
        .unwrap();
    let positioned = &coordinator.source_batches_buf["test_source"][0];
    assert_eq!(positioned.schema(), positioned_schema);
    assert_eq!(positioned.num_rows(), 1);
    assert_eq!(
        positioned
            .column_by_name(SOURCE_ORDER_KEY_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"o2"
    );
    assert_eq!(
        coordinator.pending_watermark_batches[0].batch.schema(),
        positioned_schema
    );
    assert_eq!(coordinator.pending_watermark_batches[0].batch.num_rows(), 2);
    assert_eq!(events, 2);

    let mutations = SourceBatch::positioned(records, positions)
        .unwrap()
        .with_mutations(vec![SourceMutation::Put, SourceMutation::Tombstone])
        .unwrap();
    let mutations = prepare_encoded_source_batch(
        "positioned",
        &schema,
        &positioned_schema,
        &mutation_schema,
        &[],
        &[],
        SourceRowPositionCapability::OrderedDeterministic,
        mutations,
    )
    .unwrap();
    let error = coordinator
        .stage_batch(
            0,
            &mutations,
            SourceBatchCursor::Complete(checkpoint_at(2)),
            &mut callback,
            &mut events,
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("emitted keyed mutation metadata outside a keyed-upsert route"));

    coordinator.source_input_modes[0] = Some(SourceInputMode::KeyedUpsert);
    coordinator
        .stage_batch(
            0,
            &mutations,
            SourceBatchCursor::Complete(checkpoint_at(2)),
            &mut callback,
            &mut events,
        )
        .unwrap();
    assert!(coordinator.source_batches_buf["test_source"][1]
        .column_by_name(SOURCE_MUTATION_COLUMN)
        .is_some());
    assert_eq!(
        coordinator.source_batches_buf["test_source"][1].num_rows(),
        2
    );
}

#[test]
fn full_changelog_route_bypasses_generic_late_filtering() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, -1])),
        ],
    )
    .unwrap();
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    coordinator.source_input_modes[0] = Some(SourceInputMode::FullChangelog);
    let mut callback = MockCallback::new();
    callback.late_filter = MockLateFilter::DropFirst;
    let mut events = 0;

    coordinator
        .stage_batch(
            0,
            &batch,
            SourceBatchCursor::Complete(checkpoint_at(1)),
            &mut callback,
            &mut events,
        )
        .unwrap();

    assert_eq!(
        coordinator.source_batches_buf["test_source"][0].num_rows(),
        2
    );
    assert_eq!(events, 2);
}

#[test]
fn source_preparation_failure_does_not_stage_offset_or_data() {
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.late_filter_error = Some("injected visible-schema failure".into());
    let mut events = 0;

    let error = coordinator
        .stage_batch(
            0,
            &int_batch(1),
            SourceBatchCursor::Complete(checkpoint_at(9)),
            &mut callback,
            &mut events,
        )
        .expect_err("source preparation must fail closed");

    assert!(matches!(error, CycleError::Recovery(ref reason)
        if reason.contains("injected visible-schema failure")));
    assert!(coordinator.pending_offsets[0].is_none());
    assert!(coordinator.source_batches_buf.is_empty());
    assert!(coordinator.pending_watermark_batches.is_empty());
    assert_eq!(events, 0);
}

#[tokio::test]
async fn fully_filtered_batch_executes_an_empty_progress_cycle() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.late_filter = MockLateFilter::DropAll;
    let cycle_input_rows = Arc::clone(&callback.cycle_input_rows);
    let cycle_elapsed_ns = Arc::clone(&callback.cycle_elapsed_ns);
    let cycle_phase_records = Arc::clone(&callback.cycle_phase_records);
    let run = tokio::spawn(coordinator.run(callback));

    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while cycle_input_rows.lock().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("filtered source progress did not execute");

    assert_eq!(&*cycle_input_rows.lock(), &[0]);
    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
    let elapsed = cycle_elapsed_ns.lock();
    let phases = cycle_phase_records.lock();
    assert_eq!(elapsed.len(), 1);
    assert_eq!(phases.len(), 1);
    assert!(
        phases[0].iter().copied().sum::<u64>() <= elapsed[0],
        "non-overlapping phase durations must fit inside the end-to-end cycle duration"
    );
}

#[tokio::test]
async fn watermark_extraction_failure_faults_before_cycle_publication() {
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.watermark_error = Some("invalid event-time column".into());
    let cycle_input_rows = Arc::clone(&callback.cycle_input_rows);
    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    let exit = coordinator.run(callback).await;

    assert!(matches!(exit, ExitReason::Fault(ref reason)
        if reason.contains("invalid event-time column")));
    assert!(cycle_input_rows.lock().is_empty());
}

#[test]
fn positioned_source_batches_fail_closed_on_misalignment_and_name_collision() {
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceRowPositions, SOURCE_PARTITION_COLUMN,
    };

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let two_rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let positions = SourceRowPositions::try_new(
        BinaryArray::from(vec![&b"p"[..], &b"p"[..]]),
        BinaryArray::from(vec![&b"1"[..], &b"2"[..]]),
        UInt32Array::from(vec![0, 0]),
    )
    .unwrap();
    let mut malformed = SourceBatch::positioned(two_rows, positions).unwrap();
    malformed.records = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let positioned_schema = schema_with_source_row_positions(&schema).unwrap();
    let mutation_schema = schema_with_source_mutations_and_row_positions(&schema).unwrap();
    assert!(prepare_encoded_source_batch(
        "malformed",
        &schema,
        &positioned_schema,
        &mutation_schema,
        &[],
        &[],
        SourceRowPositionCapability::OrderedDeterministic,
        malformed,
    )
    .is_err());

    let colliding = Arc::new(Schema::new(vec![Field::new(
        SOURCE_PARTITION_COLUMN.to_ascii_uppercase(),
        DataType::Binary,
        false,
    )]));
    assert!(TrackedSourceRegistration::metadata_schemas(
        "colliding",
        SourceContract::default(),
        &colliding,
    )
    .is_err());
}

#[test]
fn barrier_release_high_watermark_cannot_be_overwritten_by_stale_attempt() {
    let injector = CheckpointBarrierInjector::new();
    let (release_tx, release_rx) = tokio::sync::watch::channel(None);
    let control = SourceBarrierControl::new(injector, release_tx);
    let old = CheckpointAttempt::new(7, 7);
    let newer = CheckpointAttempt::new(8, 8);

    control.release_exact(old);
    control.release_exact(newer);
    control.release_exact(old);

    assert_eq!(
        *release_rx.borrow(),
        Some(SourceBarrierSignal::Release(newer))
    );
    assert!(source_barrier_release_covers(newer, old));
    assert!(!source_barrier_release_covers(old, newer));

    let (equivocal_tx, equivocal_rx) = tokio::sync::watch::channel(None);
    let equivocal = SourceBarrierControl::new(CheckpointBarrierInjector::new(), equivocal_tx);
    let first = CheckpointAttempt::new(9, 9);
    let conflicting = CheckpointAttempt::new(9, 10);
    equivocal.release_exact(first);
    equivocal.release_exact(conflicting);
    assert_eq!(
        *equivocal_rx.borrow(),
        Some(SourceBarrierSignal::Release(first))
    );
    assert!(!source_barrier_release_covers(first, conflicting));

    for conflicting in [
        CheckpointAttempt::new(10, 8),
        CheckpointAttempt::new(10, 9),
        CheckpointAttempt::new(8, 10),
    ] {
        equivocal.release_exact(conflicting);
        assert_eq!(
            *equivocal_rx.borrow(),
            Some(SourceBarrierSignal::Release(first)),
            "a conflicting release must not overwrite the retained high-watermark"
        );
        assert!(!source_barrier_release_covers(conflicting, first));
        assert!(!source_barrier_release_covers(first, conflicting));
    }
}

#[test]
fn stale_cancelled_barrier_does_not_fence_later_source_data() {
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::ExactlyOnce,
        None,
    );
    coordinator
        .pending_barrier
        .reset(CheckpointAttempt::new(8, 8), 1);
    let mut callback = MockCallback::new();
    let mut barriers = Vec::new();
    let mut events = 0;

    coordinator
        .process_msg(
            SourceMsg::Barrier {
                source_idx: 0,
                barrier: CheckpointBarrier::new(7, 7),
                checkpoint: checkpoint_at(7),
            },
            &mut callback,
            &mut barriers,
            &mut events,
        )
        .unwrap();
    assert!(barriers.is_empty());
    assert!(!coordinator.barrier_seen.contains(&0));

    coordinator
        .process_msg(
            SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(11),
                cursor: SourceBatchCursor::Complete(checkpoint_at(8)),
            },
            &mut callback,
            &mut barriers,
            &mut events,
        )
        .expect("data after a released stale barrier belongs to the open epoch");
    assert_eq!(
        coordinator
            .source_batches_buf
            .get("test_source")
            .map(Vec::len),
        Some(1)
    );
}

#[tokio::test]
async fn ready_completion_does_not_drop_the_parked_intake_message() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);
    let attempt = CheckpointAttempt::new(7, 7);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    )
    .with_checkpoint_complete_rx(completion_rx);
    coordinator.parked_source_msg = Some(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(42),
        cursor: SourceBatchCursor::Complete(checkpoint_at(8)),
    });
    completion_tx
        .send(CheckpointCompletion::new(
            attempt,
            FxHashMap::default(),
            false,
        ))
        .await
        .unwrap();

    let callback = MockCallback::new();
    let written_rows = Arc::clone(&callback.written_rows);
    let published = Arc::clone(&callback.published_barriers);
    let observed_rows = Arc::clone(&written_rows);
    let observed_published = Arc::clone(&published);
    let stop = tokio::spawn(async move {
        while observed_rows.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(observed_published.lock().as_slice(), &[(7, 7)]);
        shutdown.notify_one();
    });

    let exit = tokio::time::timeout(Duration::from_secs(2), coordinator.run(callback))
        .await
        .expect("parked message did not run after the higher-priority completion");
    stop.await.unwrap();
    drop(source_tx);
    drop(completion_tx);
    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(written_rows.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn external_commit_pressure_keeps_completion_live_and_source_queued() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);
    let attempt = CheckpointAttempt::new(7, 7);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    )
    .with_checkpoint_complete_rx(completion_rx);
    source_tx
        .send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(42),
            cursor: SourceBatchCursor::Complete(checkpoint_at(8)),
        })
        .await
        .unwrap();

    let callback = MockCallback::new();
    callback
        .external_commit_backpressure
        .store(true, Ordering::Release);
    let written_rows = Arc::clone(&callback.written_rows);
    let published = Arc::clone(&callback.published_barriers);
    let observed_rows = Arc::clone(&written_rows);
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            observed_rows.load(Ordering::Acquire),
            0,
            "source input ran before the durable output cut resolved"
        );
        completion_tx
            .send(CheckpointCompletion::new(
                attempt,
                FxHashMap::default(),
                false,
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while observed_rows.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source input did not resume after checkpoint completion");
        shutdown.notify_one();
    });

    let exit = tokio::time::timeout(Duration::from_secs(2), coordinator.run(callback))
        .await
        .expect("coordinator did not settle external commit backpressure");
    release.await.unwrap();
    drop(source_tx);
    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(written_rows.load(Ordering::Acquire), 1);
    assert_eq!(published.lock().as_slice(), &[(7, 7)]);
}

fn assignment_fence(
    version: u64,
    participants: &[u64],
) -> laminar_core::checkpoint::CheckpointAssignmentFence {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let participants = participants
        .iter()
        .map(|node_id| CheckpointParticipant {
            node_id: *node_id,
            boot_incarnation: format!("00000000-0000-0000-0000-{node_id:012x}")
                .parse()
                .unwrap(),
        })
        .collect::<Vec<_>>();
    let owners = participants
        .iter()
        .map(|participant| participant.node_id)
        .collect::<Vec<_>>();
    CheckpointAssignmentFence::from_owner_map(version, &owners, participants).unwrap()
}

struct MockRuntimeState {
    leader: bool,
    recovering: bool,
    assignment_ready: bool,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum ProcessAuthorityFencePoint {
    Watermark,
    AssignmentAdmission,
    CheckpointDrain,
    PreparePublication,
    SubscriptionCut,
    CheckpointControl,
}

/// Minimal mock callback for testing the coordinator loop.
#[derive(Clone, Copy, Default)]
enum MockLateFilter {
    #[default]
    Keep,
    DropAll,
    DropFirst,
}

#[allow(clippy::struct_excessive_bools)]
struct MockCallback {
    cycle_count: u32,
    attempt_to_reserve: CheckpointAttempt,
    reserve_error: Option<String>,
    reserve_calls: u64,
    reserve_call_audit: Arc<AtomicU64>,
    reserve_observed: Option<Arc<tokio::sync::Notify>>,
    reserve_release: Option<Arc<tokio::sync::Notify>>,
    checkpoint_assignment_calls: u64,
    control_checkpoint_calls: u64,
    control_checkpoint_call_audit: Arc<AtomicU64>,
    control_checkpoint_fault: Option<String>,
    control_checkpoint_fault_observed: Option<Arc<tokio::sync::Notify>>,
    control_checkpoint_fault_release: Option<Arc<tokio::sync::Notify>>,
    #[cfg(feature = "cluster")]
    checkpoint_control_enabled: bool,
    #[cfg(feature = "cluster")]
    checkpoint_control_watch: Option<
        tokio::sync::watch::Receiver<Option<laminar_core::cluster::control::BarrierAnnouncement>>,
    >,
    shuffle_work_wake: Option<Arc<tokio::sync::Notify>>,
    runnable_deferred_input: bool,
    barrier_captures: Vec<(CheckpointAttempt, usize)>,
    runtime: MockRuntimeState,
    assignment_fault: Option<String>,
    assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    assignment_flags: u64,
    assignment_admission_lock: Option<Arc<tokio::sync::Mutex<()>>>,
    capture_saw_assignment_lock_released: Arc<AtomicBool>,
    assignment_admission_observed: Option<Arc<tokio::sync::Notify>>,
    assignment_admission_release: Option<Arc<tokio::sync::Notify>>,
    prepared_attempts: Vec<(
        CheckpointAttempt,
        Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    )>,
    prepare_observed: Option<Arc<tokio::sync::Notify>>,
    prepare_release: Option<Arc<tokio::sync::Notify>>,
    prepare_error: Option<String>,
    checkpoint_order: Arc<Mutex<Vec<&'static str>>>,
    checkpoint_drain_error: Option<CycleError>,
    abandon_error: Option<String>,
    abandoned_attempts: Arc<Mutex<Vec<(CheckpointAttempt, String)>>>,
    cancelled_source_barrier_attempts: Arc<Mutex<Vec<(CheckpointAttempt, String)>>>,
    resolved_follower_aborts: Arc<Mutex<Vec<CheckpointAttempt>>>,
    resolve_follower_abort_error: Option<String>,
    abandoned_fences:
        Arc<Mutex<Vec<Option<laminar_core::cluster::control::CheckpointAssignmentFence>>>>,
    checkpoint_failures: Vec<(u64, String)>,
    checkpoint_continuation_failures: Vec<(CheckpointAttempt, String)>,
    checkpoint_admission_failures: Vec<String>,
    barrier_outcome: Option<BarrierOutcome>,
    results: Vec<FxHashMap<Arc<str>, Vec<RecordBatch>>>,
    watermark: i64,
    watermark_error: Option<String>,
    late_filter_error: Option<String>,
    late_filter: MockLateFilter,
    /// Halt cleanly on this 1-based cycle number.
    halt_at_cycle: Option<u32>,
    /// Fail on this 1-based cycle number.
    fatal_at_cycle: Option<u32>,
    /// Require recovery on this 1-based cycle number, independent of delivery guarantee.
    recovery_at_cycle: Option<u32>,
    /// Retain this cycle's batches and report a replay-preserving deferral once.
    defer_at_cycle: Option<u32>,
    retained_results: Option<FxHashMap<Arc<str>, Vec<RecordBatch>>>,
    cycle_input_rows: Arc<Mutex<Vec<usize>>>,
    source_frontier_pin_cycles: Arc<Mutex<Vec<u32>>>,
    cycle_elapsed_ns: Arc<Mutex<Vec<u64>>>,
    cycle_phase_records: Arc<Mutex<Vec<[u64; 3]>>>,
    cycle_errors: Arc<AtomicU64>,
    /// Whether a fatal cycle error requires recovery instead of a best-effort drop.
    fault_on_error: bool,
    /// Returned once by `take_pipeline_halt`; takes precedence over `pipeline_fault`.
    pipeline_halt: Option<String>,
    /// Returned once by `take_pipeline_fault`.
    pipeline_fault: Option<String>,
    /// Exact downstream checkpoint identities published by async completions.
    published_barriers: Arc<Mutex<Vec<(u64, u64)>>>,
    reserved_subscription_cuts: Arc<Mutex<Vec<CheckpointAttempt>>>,
    aborted_subscription_cuts: Arc<Mutex<Vec<CheckpointAttempt>>>,
    publish_barrier_error: Arc<Mutex<Option<String>>>,
    publication_error: Arc<Mutex<Option<String>>>,
    sink_publication_error: Arc<Mutex<Option<String>>>,
    written_rows: Arc<AtomicU64>,
    published_barriers_observed_at_close: Arc<AtomicU64>,
    invalidated_subscriptions: Arc<Mutex<Vec<String>>>,
    drop_audit: Option<Arc<AtomicBool>>,
    shutdown_sink_order: Arc<Mutex<Vec<&'static str>>>,
    settle_sink_epoch_error: Option<String>,
    close_error: Option<String>,
    barrier_control_installed: Arc<AtomicBool>,
    intake_gate: Arc<AtomicBool>,
    intake_pause_call_audit: Arc<AtomicU64>,
    external_checkpoint_pressure: Arc<AtomicBool>,
    external_commit_backpressure: Arc<AtomicBool>,
    handoff_replay_pending: bool,
    pending_vnode_transition: bool,
    vnode_transition_completions: Arc<AtomicU64>,
    #[cfg(feature = "cluster")]
    process_authority_fence:
        Arc<Mutex<Option<(ProcessAuthorityFencePoint, Arc<ClusterController>)>>>,
    control_checkpoint_outcome: Option<CheckpointControlOutcome>,
}

impl MockCallback {
    fn new() -> Self {
        Self {
            cycle_count: 0,
            attempt_to_reserve: CheckpointAttempt::new(1, 1),
            reserve_error: None,
            reserve_calls: 0,
            reserve_call_audit: Arc::new(AtomicU64::new(0)),
            reserve_observed: None,
            reserve_release: None,
            checkpoint_assignment_calls: 0,
            control_checkpoint_calls: 0,
            control_checkpoint_call_audit: Arc::new(AtomicU64::new(0)),
            control_checkpoint_fault: None,
            control_checkpoint_fault_observed: None,
            control_checkpoint_fault_release: None,
            #[cfg(feature = "cluster")]
            checkpoint_control_enabled: false,
            #[cfg(feature = "cluster")]
            checkpoint_control_watch: None,
            shuffle_work_wake: None,
            runnable_deferred_input: false,
            barrier_captures: Vec::new(),
            runtime: MockRuntimeState {
                leader: true,
                recovering: false,
                assignment_ready: true,
            },
            assignment_fault: None,
            assignment_fence: None,
            assignment_flags: laminar_core::checkpoint::flags::NONE,
            assignment_admission_lock: None,
            capture_saw_assignment_lock_released: Arc::new(AtomicBool::new(false)),
            assignment_admission_observed: None,
            assignment_admission_release: None,
            prepared_attempts: Vec::new(),
            prepare_observed: None,
            prepare_release: None,
            prepare_error: None,
            checkpoint_order: Arc::new(Mutex::new(Vec::new())),
            checkpoint_drain_error: None,
            abandon_error: None,
            abandoned_attempts: Arc::new(Mutex::new(Vec::new())),
            cancelled_source_barrier_attempts: Arc::new(Mutex::new(Vec::new())),
            resolved_follower_aborts: Arc::new(Mutex::new(Vec::new())),
            resolve_follower_abort_error: None,
            abandoned_fences: Arc::new(Mutex::new(Vec::new())),
            checkpoint_failures: Vec::new(),
            checkpoint_continuation_failures: Vec::new(),
            checkpoint_admission_failures: Vec::new(),
            barrier_outcome: None,
            results: Vec::new(),
            watermark: 0,
            watermark_error: None,
            late_filter_error: None,
            late_filter: MockLateFilter::default(),
            halt_at_cycle: None,
            fatal_at_cycle: None,
            recovery_at_cycle: None,
            defer_at_cycle: None,
            retained_results: None,
            cycle_input_rows: Arc::new(Mutex::new(Vec::new())),
            source_frontier_pin_cycles: Arc::new(Mutex::new(Vec::new())),
            cycle_elapsed_ns: Arc::new(Mutex::new(Vec::new())),
            cycle_phase_records: Arc::new(Mutex::new(Vec::new())),
            cycle_errors: Arc::new(AtomicU64::new(0)),
            fault_on_error: false,
            pipeline_halt: None,
            pipeline_fault: None,
            published_barriers: Arc::new(Mutex::new(Vec::new())),
            reserved_subscription_cuts: Arc::new(Mutex::new(Vec::new())),
            aborted_subscription_cuts: Arc::new(Mutex::new(Vec::new())),
            publish_barrier_error: Arc::new(Mutex::new(None)),
            publication_error: Arc::new(Mutex::new(None)),
            sink_publication_error: Arc::new(Mutex::new(None)),
            written_rows: Arc::new(AtomicU64::new(0)),
            published_barriers_observed_at_close: Arc::new(AtomicU64::new(0)),
            invalidated_subscriptions: Arc::new(Mutex::new(Vec::new())),
            drop_audit: None,
            shutdown_sink_order: Arc::new(Mutex::new(Vec::new())),
            settle_sink_epoch_error: None,
            close_error: None,
            barrier_control_installed: Arc::new(AtomicBool::new(false)),
            intake_gate: Arc::new(AtomicBool::new(false)),
            intake_pause_call_audit: Arc::new(AtomicU64::new(0)),
            external_checkpoint_pressure: Arc::new(AtomicBool::new(false)),
            external_commit_backpressure: Arc::new(AtomicBool::new(false)),
            handoff_replay_pending: false,
            pending_vnode_transition: false,
            vnode_transition_completions: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "cluster")]
            process_authority_fence: Arc::new(Mutex::new(None)),
            control_checkpoint_outcome: None,
        }
    }

    #[cfg(feature = "cluster")]
    fn fence_process_authority_at(&self, point: ProcessAuthorityFencePoint) {
        let controller = {
            let mut configured = self.process_authority_fence.lock();
            if configured
                .as_ref()
                .is_some_and(|(configured, _)| *configured == point)
            {
                configured.take().map(|(_, controller)| controller)
            } else {
                None
            }
        };
        if let Some(controller) = controller {
            controller.fence_process_lease();
        }
    }
}

impl Drop for MockCallback {
    fn drop(&mut self) {
        if let Some(audit) = &self.drop_audit {
            audit.store(true, Ordering::Release);
        }
    }
}

// The test double mirrors the async production contract without performing asynchronous I/O.
#[allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)]
impl PipelineCallback for MockCallback {
    fn pin_source_frontiers_for_new_cycle(&mut self) -> Result<(), String> {
        self.source_frontier_pin_cycles
            .lock()
            .push(self.cycle_count);
        Ok(())
    }

    fn checkpoint_control_wake(&self) -> Option<crate::pipeline::callback::CheckpointControlWake> {
        #[cfg(feature = "cluster")]
        {
            return self.checkpoint_control_enabled.then(|| {
                crate::pipeline::callback::CheckpointControlWake::new(
                    self.checkpoint_control_watch.clone(),
                )
            });
        }
        #[cfg(not(feature = "cluster"))]
        {
            None
        }
    }

    fn shuffle_work_wake(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.shuffle_work_wake.clone()
    }

    fn has_deferred_input(&self) -> bool {
        self.shuffle_work_wake.is_some()
    }

    fn has_runnable_deferred_input(&self) -> bool {
        self.runnable_deferred_input
    }

    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        _watermark: i64,
    ) -> Result<CycleOutcome, CycleError> {
        self.cycle_count += 1;
        let input_rows = source_batches
            .values()
            .flat_map(|batches| batches.iter())
            .map(RecordBatch::num_rows)
            .sum();
        self.cycle_input_rows.lock().push(input_rows);
        if self.halt_at_cycle == Some(self.cycle_count) {
            return Err(CycleError::Halt(format!(
                "injected halt at cycle {}",
                self.cycle_count
            )));
        }
        if self.recovery_at_cycle == Some(self.cycle_count) {
            return Err(CycleError::Recovery(format!(
                "injected recovery at cycle {}",
                self.cycle_count
            )));
        }
        if self.fatal_at_cycle == Some(self.cycle_count) {
            return Err(CycleError::Fatal(format!(
                "injected fatal at cycle {}",
                self.cycle_count
            )));
        }
        if self.defer_at_cycle == Some(self.cycle_count) {
            self.retained_results = Some(
                source_batches
                    .iter()
                    .map(|(name, batches)| (Arc::clone(name), batches.clone()))
                    .collect(),
            );
            let mut outcome = CycleOutcome::clean(FxHashMap::default());
            outcome.any_deferred = true;
            outcome.deferred_sources = source_batches.keys().cloned().collect();
            return Ok(outcome);
        }
        // Pass through source batches as results.
        let results: FxHashMap<Arc<str>, Vec<RecordBatch>> =
            self.retained_results.take().unwrap_or_else(|| {
                source_batches
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            });
        self.results.push(results.clone());
        Ok(CycleOutcome::clean(results))
    }

    async fn complete_pending_vnode_transition(&mut self) -> Result<bool, CycleError> {
        if !self.pending_vnode_transition {
            return Ok(false);
        }
        self.pending_vnode_transition = false;
        self.vnode_transition_completions
            .fetch_add(1, Ordering::Release);
        Ok(true)
    }

    async fn drain_checkpoint_edges_until(
        &mut self,
        _deadline: tokio::time::Instant,
    ) -> Result<(), CycleError> {
        self.checkpoint_order.lock().push("drain");
        #[cfg(feature = "cluster")]
        self.fence_process_authority_at(ProcessAuthorityFencePoint::CheckpointDrain);
        self.checkpoint_drain_error.take().map_or(Ok(()), Err)
    }

    fn note_cycle_error(&self) {
        self.cycle_errors.fetch_add(1, Ordering::SeqCst);
    }

    fn intake_paused(&self) -> bool {
        self.intake_pause_call_audit.fetch_add(1, Ordering::Relaxed);
        self.intake_gate.load(Ordering::Acquire)
    }

    fn external_output_pressure(&self) -> crate::pipeline::callback::ExternalOutputPressure {
        use crate::pipeline::callback::ExternalOutputPressure;

        if self.external_commit_backpressure.load(Ordering::Acquire) {
            ExternalOutputPressure::CommitBackpressured
        } else if self.external_checkpoint_pressure.load(Ordering::Acquire) {
            ExternalOutputPressure::CheckpointDue
        } else {
            ExternalOutputPressure::Normal
        }
    }

    fn fault_on_cycle_error(&self) -> bool {
        self.fault_on_error
    }

    fn take_pipeline_halt(&mut self) -> Option<String> {
        self.pipeline_halt.take()
    }

    fn take_pipeline_fault(&mut self) -> Option<String> {
        self.pipeline_fault.take()
    }

    fn is_leader(&self) -> bool {
        self.runtime.leader
    }

    fn is_recovering(&mut self) -> bool {
        self.runtime.recovering
    }

    async fn checkpoint_assignment_for_admission(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> CheckpointAssignmentAdmission {
        self.checkpoint_assignment_calls += 1;
        let assignment_guard = if let Some(lock) = self.assignment_admission_lock.as_ref() {
            match tokio::time::timeout_at(deadline, Arc::clone(lock).lock_owned()).await {
                Ok(guard) => Some(guard),
                Err(_) => {
                    return CheckpointAssignmentAdmission::Deferred(
                        "assignment serialization timed out".into(),
                    );
                }
            }
        } else {
            None
        };
        if let Some(observed) = self.assignment_admission_observed.as_ref() {
            observed.notify_one();
        }
        if let Some(release) = self.assignment_admission_release.as_ref() {
            release.notified().await;
        }
        #[cfg(feature = "cluster")]
        self.fence_process_authority_at(ProcessAuthorityFencePoint::AssignmentAdmission);
        if let Some(error) = self.assignment_fault.take() {
            CheckpointAssignmentAdmission::Fault(error)
        } else if self.runtime.assignment_ready {
            CheckpointAssignmentAdmission::Ready {
                assignment_fence: self.assignment_fence.clone(),
                flags: self.assignment_flags,
                assignment_guard,
            }
        } else {
            CheckpointAssignmentAdmission::Deferred("assignment is not checkpoint-ready".into())
        }
    }

    async fn reserve_checkpoint_attempt(
        &mut self,
        _deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, String> {
        self.reserve_calls += 1;
        self.reserve_call_audit.fetch_add(1, Ordering::SeqCst);
        if let Some(observed) = self.reserve_observed.as_ref() {
            observed.notify_one();
        }
        if let Some(release) = self.reserve_release.as_ref() {
            release.notified().await;
        }
        match self.reserve_error.take() {
            Some(error) => Err(error),
            None => Ok(self.attempt_to_reserve),
        }
    }

    async fn publish_checkpoint_prepare(
        &mut self,
        attempt: CheckpointAttempt,
        _attempt_started: Instant,
        _deadline: tokio::time::Instant,
        _flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        self.prepared_attempts.push((attempt, assignment_fence));
        if let Some(observed) = self.prepare_observed.as_ref() {
            observed.notify_one();
        }
        if let Some(release) = self.prepare_release.as_ref() {
            release.notified().await;
        }
        #[cfg(feature = "cluster")]
        self.fence_process_authority_at(ProcessAuthorityFencePoint::PreparePublication);
        match self.prepare_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn abandon_checkpoint_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: &str,
        _flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        self.checkpoint_order.lock().push("cleanup");
        self.abandoned_attempts
            .lock()
            .push((attempt, reason.to_owned()));
        self.abandoned_fences.lock().push(assignment_fence);
        match self.abandon_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn cancel_source_barrier_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: &str,
    ) -> Result<(), String> {
        self.checkpoint_order.lock().push("cleanup");
        self.cancelled_source_barrier_attempts
            .lock()
            .push((attempt, reason.to_owned()));
        Ok(())
    }

    fn resolve_authoritative_follower_abort(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<(), String> {
        self.checkpoint_order.lock().push("cleanup");
        self.resolved_follower_aborts.lock().push(attempt);
        self.resolve_follower_abort_error.take().map_or(Ok(()), Err)
    }

    fn record_checkpoint_failure(&mut self, checkpoint_id: u64, reason: &str) {
        self.checkpoint_failures
            .push((checkpoint_id, reason.to_owned()));
    }

    fn record_checkpoint_continuation_fault(&mut self, attempt: CheckpointAttempt, reason: &str) {
        self.checkpoint_continuation_failures
            .push((attempt, reason.to_owned()));
    }

    fn record_checkpoint_admission_failure(&mut self, reason: &str) {
        self.checkpoint_admission_failures.push(reason.to_owned());
    }

    fn push_to_streams(
        &self,
        _results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Result<(), CycleError> {
        match self.publication_error.lock().take() {
            Some(error) => Err(CycleError::Recovery(error)),
            None => Ok(()),
        }
    }
    async fn write_to_sinks(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        _deadline: Option<tokio::time::Instant>,
    ) -> Result<(), CycleError> {
        if let Some(error) = self.sink_publication_error.lock().take() {
            return Err(CycleError::Recovery(error));
        }
        let rows = results
            .values()
            .flat_map(|batches| batches.iter())
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        self.written_rows
            .fetch_add(u64::try_from(rows).unwrap(), Ordering::SeqCst);
        Ok(())
    }

    fn extract_watermark(
        &mut self,
        _source_name: &str,
        batch: &RecordBatch,
        _admission_floor: i64,
    ) -> Result<(), CycleError> {
        #[cfg(feature = "cluster")]
        self.fence_process_authority_at(ProcessAuthorityFencePoint::Watermark);
        if let Some(error) = &self.watermark_error {
            return Err(CycleError::Recovery(error.clone()));
        }
        // Use row count as a simple watermark proxy.
        {
            self.watermark = self
                .watermark
                .saturating_add(i64::try_from(batch.num_rows()).unwrap_or(i64::MAX));
        }
        Ok(())
    }

    fn reconcile_source_input_channels(
        &mut self,
        _source_name: &str,
        _input_channels: Option<Arc<[Vec<u8>]>>,
    ) -> Result<(), CycleError> {
        // This coordinator mock models only the logical row-count watermark above.
        Ok(())
    }

    fn filter_late_rows(
        &self,
        _source_name: &str,
        batch: &RecordBatch,
    ) -> Result<Option<RecordBatch>, CycleError> {
        if let Some(error) = &self.late_filter_error {
            return Err(CycleError::Recovery(error.clone()));
        }
        match self.late_filter {
            MockLateFilter::Keep => Ok(Some(batch.clone())),
            MockLateFilter::DropAll => Ok(None),
            MockLateFilter::DropFirst => {
                let skip = usize::from(batch.num_rows() != 0);
                Ok(Some(batch.slice(skip, batch.num_rows() - skip)))
            }
        }
    }

    fn current_watermark(&self) -> i64 {
        self.watermark
    }

    fn publish_barrier(&mut self, attempt: CheckpointAttempt) -> Result<(), String> {
        if let Some(error) = self.publish_barrier_error.lock().take() {
            return Err(error);
        }
        self.published_barriers
            .lock()
            .push((attempt.epoch, attempt.checkpoint_id));
        self.external_commit_backpressure
            .store(false, Ordering::Release);
        Ok(())
    }

    fn reserve_subscription_cut(&mut self, attempt: CheckpointAttempt) -> Result<(), String> {
        self.reserved_subscription_cuts.lock().push(attempt);
        #[cfg(feature = "cluster")]
        self.fence_process_authority_at(ProcessAuthorityFencePoint::SubscriptionCut);
        Ok(())
    }

    fn abort_subscription_cut(&mut self, attempt: CheckpointAttempt) {
        self.aborted_subscription_cuts.lock().push(attempt);
    }

    async fn service_checkpoint_control(
        &mut self,
        _source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> CheckpointControlOutcome {
        self.control_checkpoint_calls += 1;
        self.control_checkpoint_call_audit
            .fetch_add(1, Ordering::SeqCst);
        if let Some(fault) = self.control_checkpoint_fault.take() {
            self.pipeline_fault = Some(fault);
            if let Some(observed) = self.control_checkpoint_fault_observed.as_ref() {
                observed.notify_one();
            }
            if let Some(release) = self.control_checkpoint_fault_release.as_ref() {
                release.notified().await;
            }
        }
        #[cfg(feature = "cluster")]
        self.fence_process_authority_at(ProcessAuthorityFencePoint::CheckpointControl);
        self.control_checkpoint_outcome
            .take()
            .unwrap_or(CheckpointControlOutcome::Idle)
    }

    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        attempt: CheckpointAttempt,
        _attempt_started: Instant,
        _deadline: tokio::time::Instant,
        flags: u64,
        _assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> BarrierOutcome {
        if let Some(lock) = self.assignment_admission_lock.as_ref() {
            self.capture_saw_assignment_lock_released
                .store(Arc::clone(lock).try_lock_owned().is_ok(), Ordering::Release);
        }
        self.checkpoint_order.lock().push("capture");
        self.barrier_captures
            .push((attempt, source_checkpoints.len()));
        #[cfg(not(feature = "cluster"))]
        let _ = (flags, self.handoff_replay_pending);
        #[cfg(feature = "cluster")]
        crate::pipeline_callback::fence_intake_after_terminal_handoff_capture(
            &self.intake_gate,
            flags,
            self.handoff_replay_pending,
        );
        self.barrier_outcome
            .take()
            .unwrap_or(BarrierOutcome::Committed(attempt.epoch))
    }

    fn record_cycle(&self, _events: u64, _batches: u64, elapsed_ns: u64) {
        self.cycle_elapsed_ns.lock().push(elapsed_ns);
    }

    fn record_cycle_phases(&self, execute_ns: u64, output_store_ns: u64, sink_enqueue_ns: u64) {
        self.cycle_phase_records
            .lock()
            .push([execute_ns, output_store_ns, sink_enqueue_ns]);
    }
    fn apply_control(&mut self, _msg: crate::pipeline::ControlMsg) {}

    async fn settle_sink_epoch_for_shutdown(&mut self) -> Result<(), String> {
        self.shutdown_sink_order.lock().push("settle");
        match self.settle_sink_epoch_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn close_sinks(&mut self) -> Result<(), String> {
        self.shutdown_sink_order.lock().push("close");
        let published = self.published_barriers.lock().len();
        self.published_barriers_observed_at_close
            .store(u64::try_from(published).unwrap(), Ordering::SeqCst);
        match self.close_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn invalidate_subscriptions(&self, reason: &str) {
        self.invalidated_subscriptions
            .lock()
            .push(reason.to_owned());
    }

    fn set_barrier_injectors(&mut self, _injectors: Vec<SourceBarrierControl>) {
        self.barrier_control_installed
            .store(true, Ordering::Release);
    }
}

fn empty_source_fault_rx() -> tokio::sync::mpsc::UnboundedReceiver<SourceFault> {
    tokio::sync::mpsc::unbounded_channel().1
}

#[cfg(feature = "cluster")]
fn empty_connector_task_fences() -> OwnedConnectorTaskFences {
    Arc::new(parking_lot::Mutex::new(Vec::new()))
}

#[tokio::test]
async fn coordinator_exit_invalidates_provisional_subscription_delivery() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let callback = MockCallback::new();
    let invalidated = Arc::clone(&callback.invalidated_subscriptions);
    shutdown.notify_one();

    let exit = coordinator.run(callback).await;

    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(invalidated.lock().len(), 1);
    assert!(invalidated.lock()[0].contains("last committed progress frontier"));
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn checkpoint_control_watch_wakes_a_quiet_follower() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );

    let (announcement_tx, announcement_rx) = tokio::sync::watch::channel(None);
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.checkpoint_control_enabled = true;
    callback.checkpoint_control_watch = Some(announcement_rx);
    let calls = Arc::clone(&callback.control_checkpoint_call_audit);

    let task = tokio::spawn(coordinator.run(callback));
    tokio::time::timeout(Duration::from_millis(10), async {
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the initial merged-history poll must wake the quiet follower");

    announcement_tx
        .send(Some(laminar_core::cluster::control::BarrierAnnouncement {
            epoch: 7,
            checkpoint_id: 7,
            assignment_fence: None,
            leader_proof: None,
            phase: laminar_core::cluster::control::Phase::Commit,
            flags: 0,
        }))
        .unwrap();
    tokio::time::timeout(Duration::from_millis(10), async {
        while calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a direct checkpoint announcement must wake before the 250ms fallback");

    shutdown.notify_one();
    assert!(matches!(task.await.unwrap(), ExitReason::Shutdown));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn shuffle_work_wakes_a_quiet_coordinator_before_idle_fallback() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        shutdown,
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let wake = Arc::new(tokio::sync::Notify::new());
    let mut callback = MockCallback::new();
    callback.shuffle_work_wake = Some(Arc::clone(&wake));
    callback.runnable_deferred_input = false;
    callback.halt_at_cycle = Some(1);

    let task = tokio::spawn(coordinator.run(callback));
    tokio::task::yield_now().await;
    assert!(!task.is_finished());

    wake.notify_one();
    let exit = tokio::time::timeout(Duration::from_millis(10), task)
        .await
        .expect("shuffle input did not wake before the idle fallback")
        .unwrap();
    assert!(matches!(exit, ExitReason::Halt(ref reason)
        if reason == "injected halt at cycle 1"));
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn runnable_deferred_replay_does_not_wait_for_notify() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        shutdown,
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    coordinator.replay_pending = true;
    let mut callback = MockCallback::new();
    callback.shuffle_work_wake = Some(Arc::new(tokio::sync::Notify::new()));
    callback.runnable_deferred_input = true;
    callback.halt_at_cycle = Some(1);

    let exit = tokio::time::timeout(Duration::from_millis(10), coordinator.run(callback))
        .await
        .expect("runnable deferred replay waited for a shuffle notification");
    assert!(matches!(exit, ExitReason::Halt(ref reason)
        if reason == "injected halt at cycle 1"));
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn blocked_deferred_replay_waits_for_the_idle_fallback() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    coordinator.replay_pending = true;

    let mut callback = MockCallback::new();
    callback.runnable_deferred_input = false;
    let cycles = Arc::clone(&callback.cycle_input_rows);

    let task = tokio::spawn(coordinator.run(callback));
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(cycles.lock().is_empty());
    assert!(!task.is_finished());

    tokio::time::advance(IDLE_TIMEOUT - Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(cycles.lock().is_empty());

    tokio::time::advance(Duration::from_millis(1)).await;
    for _ in 0..16 {
        if cycles.lock().len() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(cycles.lock().len(), 1);
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(cycles.lock().len(), 1);

    shutdown.notify_one();
    let exit = task.await.unwrap();
    assert!(matches!(exit, ExitReason::Shutdown));
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn pending_checkpoint_control_rechecks_when_completion_precedes_claim_drop() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(1);
    let in_flight = Arc::new(AtomicU64::new(1));
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    )
    .with_checkpoint_complete_rx(completion_rx);
    coordinator.checkpoint_in_flight = Arc::clone(&in_flight);

    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.checkpoint_control_enabled = true;
    let calls = Arc::clone(&callback.control_checkpoint_call_audit);
    let published = Arc::clone(&callback.published_barriers);

    let task = tokio::spawn(coordinator.run(callback));
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let completed = CheckpointAttempt::new(6, 6);
    completion_tx
        .send(CheckpointCompletion::new(
            completed,
            FxHashMap::default(),
            false,
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(10), async {
        while published.lock().as_slice() != [(6, 6)] {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the completion must be handled before its in-flight claim is dropped");
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    in_flight.store(0, Ordering::Release);
    tokio::time::advance(Duration::from_millis(24)).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::timeout(Duration::from_millis(1), async {
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the retained control edge must retry after the completion/claim-drop race");

    shutdown.notify_one();
    assert!(matches!(task.await.unwrap(), ExitReason::Shutdown));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn pending_checkpoint_control_rechecks_eventless_follower_tail_at_25ms() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let in_flight = Arc::new(AtomicU64::new(1));
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    coordinator.checkpoint_in_flight = Arc::clone(&in_flight);

    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.checkpoint_control_enabled = true;
    let calls = Arc::clone(&callback.control_checkpoint_call_audit);

    let task = tokio::spawn(coordinator.run(callback));
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    in_flight.store(0, Ordering::Release);
    tokio::time::advance(Duration::from_millis(23)).await;
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_millis(2)).await;
    tokio::time::timeout(Duration::from_millis(1), async {
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an eventless follower tail must be rechecked at the 25ms capacity bound");

    shutdown.notify_one();
    assert!(matches!(task.await.unwrap(), ExitReason::Shutdown));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn pending_checkpoint_control_rearms_while_intake_is_paused() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );

    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.checkpoint_control_enabled = true;
    let calls = Arc::clone(&callback.control_checkpoint_call_audit);
    let intake_gate = Arc::clone(&callback.intake_gate);
    let intake_calls = Arc::clone(&callback.intake_pause_call_audit);
    intake_gate.store(true, Ordering::Release);

    let task = tokio::spawn(coordinator.run(callback));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        intake_calls.load(Ordering::Relaxed) <= 3,
        "a past-due control timer spun while the intake gate was closed"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    intake_gate.store(false, Ordering::Release);
    tokio::time::advance(Duration::from_millis(25)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(25)).await;
    tokio::time::timeout(Duration::from_millis(1), async {
        while calls.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("checkpoint control did not resume after the intake gate opened");

    shutdown.notify_one();
    assert!(matches!(task.await.unwrap(), ExitReason::Shutdown));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn checkpoint_control_fault_stops_before_post_fault_batch_executes() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.checkpoint_control_enabled = true;
    callback.control_checkpoint_fault = Some("uncertified follower Prepare".into());
    let fault_observed = Arc::new(tokio::sync::Notify::new());
    let fault_release = Arc::new(tokio::sync::Notify::new());
    callback.control_checkpoint_fault_observed = Some(Arc::clone(&fault_observed));
    callback.control_checkpoint_fault_release = Some(Arc::clone(&fault_release));
    let cycle_inputs = Arc::clone(&callback.cycle_input_rows);
    let written_rows = Arc::clone(&callback.written_rows);

    let task = tokio::spawn(coordinator.run(callback));
    fault_observed.notified().await;
    source_tx
        .send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(42),
            cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
        })
        .await
        .unwrap();
    fault_release.notify_one();

    let exit = task.await.unwrap();
    assert!(matches!(
        exit,
        ExitReason::Fault(ref error) if error == "uncertified follower Prepare"
    ));
    assert!(cycle_inputs.lock().is_empty());
    assert_eq!(written_rows.load(Ordering::Acquire), 0);
}

/// Build a source-less coordinator over a direct channel (bypasses source spawning).
fn test_coordinator(
    rx: SourceMsgRx,
    control_rx: ControlMsgRx,
    shutdown: Arc<tokio::sync::Notify>,
    delivery_guarantee: DeliveryGuarantee,
    checkpoint_interval: Option<Duration>,
) -> StreamingCoordinator {
    let checkpoint_schedule =
        checkpoint_interval.map_or(CheckpointSchedule::Disabled, CheckpointSchedule::Periodic);
    StreamingCoordinator {
        config: PipelineConfig {
            batch_window: Duration::ZERO,
            max_poll_records: 1000,
            channel_capacity: 64,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_schedule,
            delivery_guarantee,
            checkpoint_timeout: Duration::from_secs(30),
            cycle_budget_ns: 10_000_000,
            drain_budget_ns: 1_000_000,
            query_budget_ns: 8_000_000,
            max_input_buf_batches: 256,
            max_input_buf_bytes: None,
            backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            shared_source_isolation: false,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        },
        rx,
        source_fault_rx: empty_source_fault_rx(),
        source_handles: Vec::new(),
        source_names: vec![Arc::from("test_source")],
        source_input_modes: vec![None],
        shutdown,
        terminal_shutdown: tokio_util::sync::CancellationToken::new(),
        pending_barrier: PendingBarrier::new(),
        last_checkpoint: Instant::now(),
        checkpoint_retry_not_before: None,
        checkpoint_retry_backoff: Duration::ZERO,
        source_batches_buf: FxHashMap::default(),
        parked_source_msg: None,
        pending_watermark_batches: Vec::new(),
        barrier_seen: FxHashSet::default(),
        committed_offsets: vec![None],
        pending_offsets: vec![None],
        replay_pending: false,
        control_rx,
        checkpoint_complete_rx: None,
        force_ckpt_rx: None,
        manual_waiting: Vec::new(),
        manual_handoff_required: false,
        manual_active: None,
        checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
        last_published_checkpoint: None,
        public_generation: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    }
}

fn int_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
}

fn checkpoint_at(position: u64) -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_offset("test_position", position.to_string());
    checkpoint
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn terminal_handoff_capture_fences_post_cut_work_but_intermediate_cut_stays_open() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    let intake_gate = Arc::clone(&callback.intake_gate);
    let intake_checks = Arc::clone(&callback.intake_pause_call_audit);
    let written_rows = Arc::clone(&callback.written_rows);

    callback.handoff_replay_pending = true;
    let intermediate = CheckpointAttempt::new(1, 1);
    let _ = callback
        .checkpoint_with_barrier(
            FxHashMap::default(),
            intermediate,
            Instant::now(),
            tokio::time::Instant::now() + Duration::from_secs(1),
            laminar_core::checkpoint::flags::HANDOFF,
            None,
        )
        .await;
    assert!(
        !intake_gate.load(Ordering::Acquire),
        "an intermediate HANDOFF cut must stay open to drain retained replay"
    );

    callback.handoff_replay_pending = false;
    let terminal = CheckpointAttempt::new(2, 2);
    let _ = callback
        .checkpoint_with_barrier(
            FxHashMap::default(),
            terminal,
            Instant::now(),
            tokio::time::Instant::now() + Duration::from_secs(1),
            laminar_core::checkpoint::flags::HANDOFF,
            None,
        )
        .await;
    assert!(
        intake_gate.load(Ordering::Acquire),
        "the replay-free HANDOFF cut must fence intake before the capture callback returns"
    );

    source_tx
        .send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(42),
            cursor: SourceBatchCursor::Complete(checkpoint_at(2)),
        })
        .await
        .unwrap();
    let run = tokio::spawn(coordinator.run(callback));
    tokio::time::timeout(Duration::from_secs(1), async {
        while intake_checks.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the coordinator did not observe the terminal handoff fence");
    assert_eq!(
        written_rows.load(Ordering::Acquire),
        0,
        "work queued after the terminal handoff cut must not execute under its predecessor"
    );

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

fn successful_checkpoint_result(
    attempt: CheckpointAttempt,
) -> crate::checkpoint_coordinator::CheckpointResult {
    crate::checkpoint_coordinator::CheckpointResult {
        success: true,
        checkpoint_id: attempt.checkpoint_id,
        epoch: attempt.epoch,
        duration: Duration::from_millis(1),
        error: None,
        failure_disposition: None,
    }
}

#[tokio::test]
async fn runtime_ready_is_published_after_barrier_control_is_installed() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let callback = MockCallback::new();
    let installed = Arc::clone(&callback.barrier_control_installed);
    let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();

    let run = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
    ready_rx
        .await
        .expect("coordinator must retain the startup sender")
        .expect("coordinator startup must succeed");
    assert!(
        installed.load(Ordering::Acquire),
        "ready was published before barrier control was installed"
    );

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

#[tokio::test]
async fn source_less_runtime_stays_live_until_explicit_shutdown() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        Vec::new(),
        PipelineConfig::default(),
        Arc::clone(&shutdown),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("a source-less pipeline is valid");
    let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();

    let run = tokio::spawn(async move {
        coordinator
            .run_with_ready(MockCallback::new(), ready_tx)
            .await
    });
    ready_rx
        .await
        .expect("source-less coordinator retained readiness sender")
        .expect("source-less coordinator entered its control loop");
    tokio::task::yield_now().await;
    assert!(
        !run.is_finished(),
        "disconnected source channel stopped a valid source-less runtime"
    );

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

#[tokio::test]
async fn configured_source_channel_exhaustion_is_a_fault() {
    let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    drop(source_tx);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::BestEffort,
        None,
    );

    let exit = coordinator.run(MockCallback::new()).await;
    assert!(
        matches!(exit, ExitReason::Fault(ref reason)
            if reason.contains("all configured source tasks exited unexpectedly")),
        "configured-source exhaustion was reported as a clean stop: {exit:?}"
    );
}

#[tokio::test]
async fn recovery_intake_gate_blocks_compute_and_discards_shutdown_open_epoch() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let callback = MockCallback::new();
    let intake_gate = Arc::clone(&callback.intake_gate);
    let written_rows = Arc::clone(&callback.written_rows);
    intake_gate.store(true, Ordering::Release);
    let run = tokio::spawn(async move { coordinator.run(callback).await });

    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(written_rows.load(Ordering::Acquire), 0);

    intake_gate.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_millis(500), async {
        while written_rows.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("compute did not resume after the intake fence opened");

    intake_gate.store(true, Ordering::Release);
    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(2),
        cursor: SourceBatchCursor::Complete(checkpoint_at(2)),
    })
    .await
    .unwrap();
    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
    assert_eq!(
        written_rows.load(Ordering::Acquire),
        1,
        "a recovery-fenced shutdown must discard the open epoch"
    );
}

#[tokio::test]
async fn recovery_intake_reopen_processes_backlog_before_periodic_checkpoint() {
    let interval = Duration::from_millis(100);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        Some(interval),
    );
    coordinator.last_checkpoint = Instant::now() - interval;
    let callback = MockCallback::new();
    let intake_gate = Arc::clone(&callback.intake_gate);
    let written_rows = Arc::clone(&callback.written_rows);
    let reserve_calls = Arc::clone(&callback.reserve_call_audit);
    intake_gate.store(true, Ordering::Release);
    let run = tokio::spawn(coordinator.run(callback));

    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(written_rows.load(Ordering::Acquire), 0);
    assert_eq!(reserve_calls.load(Ordering::Acquire), 0);

    intake_gate.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_millis(500), async {
        while written_rows.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the queued recovery backlog did not execute after intake reopened");
    assert_eq!(
        reserve_calls.load(Ordering::Acquire),
        0,
        "an overdue periodic checkpoint outran the first post-recovery data cycle"
    );

    tokio::time::sleep(interval / 2).await;
    assert_eq!(
        reserve_calls.load(Ordering::Acquire),
        0,
        "periodic admission did not wait a full interval after intake reopened"
    );
    tokio::time::timeout(Duration::from_millis(500), async {
        while reserve_calls.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("periodic admission did not resume after the restarted interval");

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

#[tokio::test(start_paused = true)]
async fn recovery_intake_gate_expires_manual_request_without_checkpoint_admission() {
    let (source, barrier_poll) = checkpoint_source_handle("fenced-manual-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let shutdown = Arc::clone(&coordinator.shutdown);
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(1);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, mut reply_rx) = crossfire::oneshot::oneshot();
    let (request, mut reservation_wait) = manual_request_until(
        reply_tx,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    force_tx.send(request).await.unwrap();

    let callback = MockCallback::new();
    callback.intake_gate.store(true, Ordering::Release);
    let reserve_calls = Arc::clone(&callback.reserve_call_audit);
    let control_calls = Arc::clone(&callback.control_checkpoint_call_audit);
    let run = tokio::spawn(coordinator.run(callback));

    for _ in 0..20 {
        if force_tx.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        force_tx.is_empty(),
        "fenced coordinator did not accept the queued manual request"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    let std::task::Poll::Ready(reply) = futures::poll!(&mut reply_rx) else {
        panic!("fenced manual request remained pending past its pre-reservation deadline");
    };
    let error = reply.unwrap().unwrap_err();
    assert!(error
        .to_string()
        .contains("deadline expired before exact attempt reservation"));
    assert!(reservation_wait.wait().await.is_err());
    assert_eq!(reserve_calls.load(Ordering::SeqCst), 0);
    assert_eq!(control_calls.load(Ordering::SeqCst), 0);
    assert!(barrier_poll.poll().is_none());
    assert!(!run.is_finished(), "the intake fence stopped the runtime");

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

#[tokio::test(start_paused = true)]
async fn recovery_intake_gate_allows_control_only_vnode_transition_completion() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.runtime.recovering = true;
    callback.intake_gate.store(true, Ordering::Release);
    callback.pending_vnode_transition = true;
    let wake = Arc::new(tokio::sync::Notify::new());
    callback.shuffle_work_wake = Some(Arc::clone(&wake));
    let completions = Arc::clone(&callback.vnode_transition_completions);
    let written_rows = Arc::clone(&callback.written_rows);

    let run = tokio::spawn(async move { coordinator.run(callback).await });
    tokio::task::yield_now().await;
    wake.notify_one();
    tokio::time::timeout(Duration::from_millis(10), async {
        while completions.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shuffle completion did not wake the fenced transition");

    assert_eq!(
        completions.load(Ordering::Acquire),
        1,
        "a fenced idle wake must complete assignment-scoped vnode work"
    );
    assert_eq!(written_rows.load(Ordering::Acquire), 0);
    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

#[tokio::test]
async fn intake_gate_close_after_receive_parks_fifo_message_until_reopen() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    coordinator.config.batch_window = Duration::from_millis(200);

    let callback = MockCallback::new();
    let intake_gate = Arc::clone(&callback.intake_gate);
    let written_rows = Arc::clone(&callback.written_rows);

    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();
    let run = tokio::spawn(async move { coordinator.run(callback).await });

    // Capacity one makes completion of this send proof that the coordinator removed the
    // first message from the FIFO and is inside its batch window.
    tokio::time::timeout(
        Duration::from_secs(1),
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(2),
            cursor: SourceBatchCursor::Complete(checkpoint_at(2)),
        }),
    )
    .await
    .expect("coordinator did not receive the first FIFO message")
    .unwrap();
    intake_gate.store(true, Ordering::Release);

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        written_rows.load(Ordering::Acquire),
        0,
        "a message received just before gate closure must remain unexecuted"
    );

    intake_gate.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(1), async {
        while written_rows.load(Ordering::Acquire) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("parked and queued FIFO messages did not resume after gate reopen");

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

fn checkpoint_source_handle(
    name: &str,
) -> (SourceHandle, laminar_core::checkpoint::BarrierPollHandle) {
    let barrier_injector = CheckpointBarrierInjector::new();
    let barrier_handle = barrier_injector.handle();
    let (epoch_committed_tx, _epoch_committed_rx) = tokio::sync::watch::channel(None);
    let (barrier_release_tx, _barrier_release_rx) = tokio::sync::watch::channel(None);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let expected_shutdown = Arc::new(AtomicBool::new(false));
    let runtime = tokio::runtime::Handle::current();
    let (join, actor_terminal) = spawn_source_actor(&runtime, async {});
    let task = SourceTaskLease::supervise(
        Arc::from(name),
        shutdown,
        expected_shutdown,
        join,
        actor_terminal,
        None,
        &runtime,
    );
    (
        SourceHandle {
            recovery_cursor: true,
            task,
            startup_activation: None,
            barrier_injector,
            barrier_release_tx,
            epoch_committed_tx,
        },
        barrier_handle,
    )
}

fn admission_coordinator(source_handles: Vec<SourceHandle>) -> StreamingCoordinator {
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let source_names = source_handles
        .iter()
        .map(|handle| Arc::clone(&handle.task.state.name))
        .collect::<Vec<_>>();
    let source_count = source_handles.len();
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::ExactlyOnce,
        Some(Duration::ZERO),
    );
    coordinator.source_handles = source_handles;
    coordinator.source_names = source_names;
    coordinator.source_input_modes = vec![None; source_count];
    coordinator.committed_offsets = vec![None; source_count];
    coordinator.pending_offsets = vec![None; source_count];
    coordinator
}

#[cfg(feature = "cluster")]
fn install_test_process_authority(
    coordinator: &mut StreamingCoordinator,
    node: u64,
) -> Arc<ClusterController> {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let node_id = laminar_core::state::NodeId(node);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    coordinator.process_authority = Some(SourceProcessAuthority::new(Arc::clone(&controller)));
    controller
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_wakes_coordinator_delay() {
    let mut coordinator = admission_coordinator(Vec::new());
    install_test_process_authority(&mut coordinator, 40);
    let authority = coordinator.process_authority.as_ref().unwrap();
    let lost = authority.lost.clone();
    let wait = wait_coordinator_delay(Duration::from_secs(60), Some(authority));
    tokio::pin!(wait);
    tokio::task::yield_now().await;
    lost.cancel();

    assert!(tokio::time::timeout(Duration::from_millis(100), wait)
        .await
        .expect("process lease loss remained hidden behind coordinator delay"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_before_reservation_reports_admission_failure() {
    let mut coordinator = admission_coordinator(Vec::new());
    let controller = install_test_process_authority(&mut coordinator, 48);
    controller.fence_process_lease();
    let mut callback = MockCallback::new();

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.reserve_calls, 0);
    assert_eq!(callback.checkpoint_assignment_calls, 0);
    assert_eq!(callback.checkpoint_admission_failures.len(), 1);
    assert!(callback.checkpoint_admission_failures[0].contains("checkpoint admission"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_during_assignment_admission_stops_before_reservation() {
    let mut coordinator = admission_coordinator(Vec::new());
    let controller = install_test_process_authority(&mut coordinator, 49);
    let mut callback = MockCallback::new();
    *callback.process_authority_fence.lock() =
        Some((ProcessAuthorityFencePoint::AssignmentAdmission, controller));

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.checkpoint_assignment_calls, 1);
    assert_eq!(callback.reserve_calls, 0);
    assert_eq!(callback.checkpoint_admission_failures.len(), 1);
    assert!(callback.checkpoint_admission_failures[0].contains("checkpoint attempt creation"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_after_prepare_abandons_the_exact_attempt() {
    let mut coordinator = admission_coordinator(Vec::new());
    let controller = install_test_process_authority(&mut coordinator, 41);
    let attempt = CheckpointAttempt::new(41, 41);
    let admission = CheckpointAdmission {
        manual: false,
        flags: laminar_core::checkpoint::flags::NONE,
        assignment_fence: None,
        assignment_guard: None,
        deadline: tokio::time::Instant::now() + Duration::from_secs(60),
    };
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    *callback.process_authority_fence.lock() =
        Some((ProcessAuthorityFencePoint::PreparePublication, controller));
    let abandoned = Arc::clone(&callback.abandoned_attempts);

    let error = coordinator
        .reserve_prepared_checkpoint_attempt(&mut callback, &admission, Instant::now())
        .await
        .expect_err("lease loss after Prepare publication must abandon the attempt");

    assert!(error.contains("checkpoint prepare completion"), "{error}");
    assert_eq!(callback.prepared_attempts.len(), 1);
    let abandoned = abandoned.lock();
    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].0, attempt);
    assert!(abandoned[0].1.contains("checkpoint prepare completion"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_less_checkpoint_rechecks_authority_after_each_async_cut_boundary() {
    for (node, point, boundary, cut_reserved) in [
        (
            42,
            ProcessAuthorityFencePoint::CheckpointDrain,
            "source-less checkpoint capture",
            false,
        ),
        (
            43,
            ProcessAuthorityFencePoint::SubscriptionCut,
            "source-less checkpoint capture start",
            true,
        ),
    ] {
        let mut coordinator = admission_coordinator(Vec::new());
        let controller = install_test_process_authority(&mut coordinator, node);
        let attempt = CheckpointAttempt::new(node, node);
        let mut admission = CheckpointAdmission {
            manual: false,
            flags: laminar_core::checkpoint::flags::NONE,
            assignment_fence: None,
            assignment_guard: None,
            deadline: tokio::time::Instant::now() + Duration::from_secs(60),
        };
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = attempt;
        *callback.process_authority_fence.lock() = Some((point, controller));
        let abandoned = Arc::clone(&callback.abandoned_attempts);
        let reserved = Arc::clone(&callback.reserved_subscription_cuts);
        let aborted = Arc::clone(&callback.aborted_subscription_cuts);

        coordinator
            .admit_source_less_checkpoint(&mut callback, &mut admission)
            .await;

        assert!(callback.barrier_captures.is_empty());
        assert_eq!(
            reserved.lock().as_slice(),
            cut_reserved.then_some(attempt).as_slice()
        );
        assert_eq!(
            aborted.lock().as_slice(),
            cut_reserved.then_some(attempt).as_slice()
        );
        let abandoned = abandoned.lock();
        assert_eq!(abandoned.len(), 1);
        assert_eq!(abandoned[0].0, attempt);
        assert!(abandoned[0].1.contains(boundary), "{:?}", *abandoned);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn aligned_checkpoint_rechecks_authority_after_each_async_cut_boundary() {
    for (node, point, boundary, cut_reserved) in [
        (
            44,
            ProcessAuthorityFencePoint::CheckpointDrain,
            "aligned checkpoint capture",
            false,
        ),
        (
            45,
            ProcessAuthorityFencePoint::SubscriptionCut,
            "aligned checkpoint capture start",
            true,
        ),
    ] {
        let (source, _poll) = checkpoint_source_handle("source");
        let mut release_rx = source.barrier_release_tx.subscribe();
        let mut coordinator = admission_coordinator(vec![source]);
        let controller = install_test_process_authority(&mut coordinator, node);
        let attempt = CheckpointAttempt::new(node, node);
        coordinator.pending_barrier.reset(attempt, 1);
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
        let mut callback = MockCallback::new();
        *callback.process_authority_fence.lock() = Some((point, controller));
        let abandoned = Arc::clone(&callback.abandoned_attempts);
        let reserved = Arc::clone(&callback.reserved_subscription_cuts);
        let aborted = Arc::clone(&callback.aborted_subscription_cuts);

        let error = coordinator
            .handle_barrier(0, &barrier, &checkpoint_at(node), &mut callback)
            .await
            .expect_err("lease loss must stop aligned checkpoint capture");

        assert!(error.to_string().contains(boundary), "{error}");
        assert!(!coordinator.pending_barrier.active);
        assert!(callback.barrier_captures.is_empty());
        assert_eq!(
            reserved.lock().as_slice(),
            cut_reserved.then_some(attempt).as_slice()
        );
        assert_eq!(
            aborted.lock().as_slice(),
            cut_reserved.then_some(attempt).as_slice()
        );
        let abandoned = abandoned.lock();
        assert_eq!(abandoned.len(), 1);
        assert_eq!(abandoned[0].0, attempt);
        assert!(abandoned[0].1.contains(boundary), "{:?}", *abandoned);
        assert_eq!(
            *release_rx.borrow_and_update(),
            Some(SourceBarrierSignal::Release(attempt))
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_control_outcome_is_not_applied_after_process_lease_loss() {
    let mut coordinator = admission_coordinator(Vec::new());
    let controller = install_test_process_authority(&mut coordinator, 46);
    let attempt = CheckpointAttempt::new(46, 46);
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.control_checkpoint_outcome = Some(CheckpointControlOutcome::Started {
        attempt,
        captured: false,
        flags: laminar_core::checkpoint::flags::NONE,
    });
    *callback.process_authority_fence.lock() =
        Some((ProcessAuthorityFencePoint::CheckpointControl, controller));

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.control_checkpoint_calls, 1);
    assert!(!coordinator.pending_barrier.active);
    assert!(callback
        .checkpoint_failures
        .iter()
        .any(|(id, reason)| *id == attempt.checkpoint_id
            && reason.contains("follower checkpoint control application")));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_idle_control_reports_process_lease_loss_as_admission_failure() {
    let mut coordinator = admission_coordinator(Vec::new());
    let controller = install_test_process_authority(&mut coordinator, 50);
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    *callback.process_authority_fence.lock() =
        Some((ProcessAuthorityFencePoint::CheckpointControl, controller));

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.control_checkpoint_calls, 1);
    assert_eq!(callback.checkpoint_admission_failures.len(), 1);
    assert!(callback.checkpoint_admission_failures[0]
        .contains("follower checkpoint control application"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_control_admission_failure_is_not_silently_idle() {
    let mut coordinator = admission_coordinator(Vec::new());
    install_test_process_authority(&mut coordinator, 51);
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.control_checkpoint_outcome = Some(CheckpointControlOutcome::AdmissionFailed {
        error: "follower source handoff reconciliation failed".into(),
    });

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.control_checkpoint_calls, 1);
    assert_eq!(
        callback.checkpoint_admission_failures,
        ["follower source handoff reconciliation failed"]
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn authoritative_follower_control_abort_is_not_recorded_as_a_failure() {
    let mut coordinator = admission_coordinator(Vec::new());
    install_test_process_authority(&mut coordinator, 52);
    let attempt = CheckpointAttempt::new(52, 52);
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.control_checkpoint_outcome = Some(CheckpointControlOutcome::Aborted { attempt });

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.control_checkpoint_calls, 1);
    assert!(callback.checkpoint_failures.is_empty());
    assert!(callback.checkpoint_admission_failures.is_empty());
    assert!(callback.resolved_follower_aborts.lock().is_empty());
    assert!(callback.cancelled_source_barrier_attempts.lock().is_empty());
    assert!(!coordinator.pending_barrier.active);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_scope_cancellation_is_not_recorded_as_a_failure() {
    let mut coordinator = admission_coordinator(Vec::new());
    install_test_process_authority(&mut coordinator, 53);
    let attempt = CheckpointAttempt::new(53, 53);
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.control_checkpoint_outcome = Some(CheckpointControlOutcome::Cancelled { attempt });

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.control_checkpoint_calls, 1);
    assert!(callback.checkpoint_failures.is_empty());
    assert!(callback.checkpoint_admission_failures.is_empty());
    assert!(!coordinator.pending_barrier.active);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_barrier_state_is_not_installed_after_process_lease_loss() {
    let (source, poll) = checkpoint_source_handle("source");
    let mut coordinator = admission_coordinator(vec![source]);
    let controller = install_test_process_authority(&mut coordinator, 47);
    let attempt = CheckpointAttempt::new(47, 47);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    coordinator.manual_active = Some(ManualCheckpointAttempt {
        attempt,
        flags: laminar_core::checkpoint::flags::NONE,
        requests: vec![manual_request(reply_tx)],
    });
    let admission = CheckpointAdmission {
        manual: true,
        flags: laminar_core::checkpoint::flags::NONE,
        assignment_fence: None,
        assignment_guard: None,
        deadline: tokio::time::Instant::now() + Duration::from_secs(60),
    };
    let mut callback = MockCallback::new();
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    controller.fence_process_lease();

    coordinator
        .inject_prepared_source_barrier_attempt(&mut callback, &admission, attempt, Instant::now())
        .await;

    assert!(!coordinator.pending_barrier.active);
    assert!(coordinator.manual_active.is_none());
    assert!(poll.poll().is_none());
    let error = reply_rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("source barrier injection"));
    let abandoned = abandoned.lock();
    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].0, attempt);
    assert!(abandoned[0].1.contains("source barrier injection"));
}

#[test]
fn durable_completion_publication_requires_strict_identity_progress() {
    let mut coordinator = admission_coordinator(Vec::new());
    let mut callback = MockCallback::new();
    let newer = CheckpointAttempt::new(12, 12);

    let completion = CheckpointCompletion::validated(
        newer,
        successful_checkpoint_result(newer),
        FxHashMap::default(),
        false,
    )
    .unwrap();
    assert!(coordinator
        .handle_checkpoint_completion(completion, &mut callback)
        .is_none());
    let accepted_cadence = coordinator.last_checkpoint;

    for invalid in [
        CheckpointAttempt::new(13, 11),
        CheckpointAttempt::new(11, 13),
        CheckpointAttempt::new(12, 13),
        CheckpointAttempt::new(13, 12),
        CheckpointAttempt::new(12, 12),
        CheckpointAttempt::new(11, 11),
    ] {
        let completion = CheckpointCompletion::validated(
            invalid,
            successful_checkpoint_result(invalid),
            FxHashMap::default(),
            false,
        )
        .unwrap();
        let error = coordinator
            .handle_checkpoint_completion(completion, &mut callback)
            .expect("non-strict checkpoint identity progress must fault");
        assert!(error.contains("not strictly newer"), "{error}");
        assert_eq!(coordinator.last_checkpoint, accepted_cadence);
    }
    assert_eq!(*callback.published_barriers.lock(), vec![(12, 12)]);
    assert_eq!(coordinator.last_published_checkpoint, Some(newer));
}

#[tokio::test]
async fn durable_completion_acks_sources_when_subscription_publication_fails() {
    let (source, _poll) = checkpoint_source_handle("source");
    let mut committed_rx = source.epoch_committed_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    let mut callback = MockCallback::new();
    *callback.publish_barrier_error.lock() = Some("injected publication failure".into());

    let attempt = CheckpointAttempt::new(13, 13);
    let mut checkpoint = checkpoint_at(attempt.epoch);
    checkpoint.set_offset("partition-0", "offset-13");
    let mut source_checkpoints = FxHashMap::default();
    source_checkpoints.insert("source".to_owned(), checkpoint.clone());
    let completion = CheckpointCompletion::validated(
        attempt,
        successful_checkpoint_result(attempt),
        source_checkpoints,
        false,
    )
    .unwrap();

    let error = coordinator
        .handle_checkpoint_completion(completion, &mut callback)
        .expect("publication failure must fault continuation");

    assert_eq!(error, "injected publication failure");
    assert_eq!(
        committed_rx.borrow_and_update().clone(),
        Some((attempt.epoch, checkpoint))
    );
    assert!(callback.checkpoint_failures.is_empty());
    assert_eq!(
        callback.checkpoint_continuation_failures,
        [(attempt, "injected publication failure".to_string())]
    );
    assert_eq!(coordinator.last_published_checkpoint, Some(attempt));
}

#[test]
fn durable_completion_reports_successor_epoch_continuation_failure() {
    let mut coordinator = admission_coordinator(Vec::new());
    let mut callback = MockCallback::new();
    let attempt = CheckpointAttempt::new(131, 131);
    let mut result = successful_checkpoint_result(attempt);
    result.error = Some("injected successor epoch failure".into());
    let completion =
        CheckpointCompletion::validated(attempt, result, FxHashMap::default(), false).unwrap();

    let error = coordinator
        .handle_checkpoint_completion(completion, &mut callback)
        .expect("successor epoch failure must fault continuation");

    assert_eq!(error, "injected successor epoch failure");
    assert_eq!(
        callback.checkpoint_continuation_failures,
        [(attempt, "injected successor epoch failure".to_string())]
    );
    assert_eq!(
        *callback.published_barriers.lock(),
        vec![(attempt.epoch, attempt.checkpoint_id)]
    );
    assert_eq!(coordinator.last_published_checkpoint, Some(attempt));
}

#[tokio::test]
async fn committed_handoff_replay_requeues_manual_request_without_advancing_cadence() {
    let mut coordinator = admission_coordinator(Vec::new());
    let mut callback = MockCallback::new();
    let attempt = CheckpointAttempt::canonical(132);
    let (reply_tx, _reply_rx) = crossfire::oneshot::oneshot();
    coordinator.manual_active = Some(ManualCheckpointAttempt {
        attempt,
        flags: laminar_core::checkpoint::flags::HANDOFF,
        requests: vec![manual_request(reply_tx)],
    });
    coordinator.last_checkpoint = Instant::now() - Duration::from_secs(60);
    let previous_cadence = coordinator.last_checkpoint;

    let completion = CheckpointCompletion::validated(
        attempt,
        crate::checkpoint_coordinator::CheckpointResult {
            success: true,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration: Duration::ZERO,
            error: None,
            failure_disposition: None,
        },
        FxHashMap::default(),
        true,
    )
    .unwrap();

    assert!(coordinator
        .handle_checkpoint_completion(completion, &mut callback)
        .is_none());

    assert!(coordinator.manual_active.is_none());
    assert_eq!(coordinator.manual_waiting.len(), 1);
    assert!(coordinator.manual_handoff_required);
    assert!(coordinator.replay_pending);
    assert!(callback.checkpoint_failures.is_empty());
    assert_eq!(coordinator.last_checkpoint, previous_cadence);
    assert!(callback.aborted_subscription_cuts.lock().is_empty());
    assert_eq!(coordinator.last_published_checkpoint, Some(attempt));
    assert_eq!(
        callback.published_barriers.lock().as_slice(),
        &[(attempt.epoch, attempt.checkpoint_id)]
    );

    assert!(coordinator
        .checkpoint_admission(&mut callback)
        .await
        .is_none());
    assert!(coordinator.manual_waiting.is_empty());
    assert!(!coordinator.manual_handoff_required);
}

#[tokio::test(start_paused = true)]
async fn acknowledged_handoff_replay_survives_public_expiry_until_terminal_successor() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let mut callback = MockCallback::new();
    let first = CheckpointAttempt::canonical(133);
    let original_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let (original_reply, original_completion) = crossfire::oneshot::oneshot();
    coordinator.manual_active = Some(ManualCheckpointAttempt {
        attempt: first,
        flags: laminar_core::checkpoint::flags::HANDOFF,
        requests: vec![ForceCheckpointRequest {
            reply: original_reply,
            reservation_claim: None,
            deadline: original_deadline,
        }],
    });
    let completion = CheckpointCompletion::validated(
        first,
        successful_checkpoint_result(first),
        FxHashMap::default(),
        true,
    )
    .unwrap();
    assert!(coordinator
        .handle_checkpoint_completion(completion, &mut callback)
        .is_none());
    assert_eq!(coordinator.manual_waiting.len(), 1);
    assert!(coordinator.manual_handoff_required);

    drop(original_completion);
    tokio::time::advance(Duration::from_secs(2)).await;
    coordinator.prune_manual_requests();
    assert_eq!(
        coordinator.manual_waiting.len(),
        1,
        "an acknowledged replay owner was pruned by its public deadline or disconnect"
    );
    assert!(coordinator.manual_handoff_required);

    // A later public caller can coalesce, but its shorter deadline must not replace the fresh
    // coordinator budget already owed to the acknowledged replay owner.
    let fresh_attempt_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let (coalesced_reply, coalesced_completion) = crossfire::oneshot::oneshot();
    let (coalesced_request, mut coalesced_reservation) = manual_request_until(
        coalesced_reply,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    coordinator.manual_waiting.push(coalesced_request);
    assert_eq!(
        coordinator.manual_deadline(fresh_attempt_deadline),
        fresh_attempt_deadline
    );

    let second = CheckpointAttempt::canonical(134);
    callback.attempt_to_reserve = second;
    callback.assignment_flags = laminar_core::checkpoint::flags::HANDOFF;
    coordinator.maybe_checkpoint(&mut callback).await;

    coalesced_reservation
        .wait()
        .await
        .expect("the terminal HANDOFF successor must reserve an exact attempt");
    let result = coalesced_completion.await.unwrap().unwrap();
    assert_eq!(
        CheckpointAttempt::new(result.epoch, result.checkpoint_id),
        second
    );
    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(callback.prepared_attempts[0].0, second);
    assert!(coordinator.manual_waiting.is_empty());
    assert!(coordinator.manual_active.is_none());
    assert!(!coordinator.manual_handoff_required);
}

#[tokio::test(start_paused = true)]
async fn handoff_replay_does_not_preclaim_new_waiter_during_delayed_reservation() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let replay_attempt = CheckpointAttempt::canonical(135);
    let (replay_reply, _replay_completion) = crossfire::oneshot::oneshot();
    coordinator.manual_waiting.push(ForceCheckpointRequest {
        reply: replay_reply,
        reservation_claim: None,
        deadline: tokio::time::Instant::now(),
    });
    coordinator.manual_handoff_required = true;

    let (fresh_reply, fresh_completion) = crossfire::oneshot::oneshot();
    let (fresh_request, fresh_wait) = manual_request_until(
        fresh_reply,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    coordinator.manual_waiting.push(fresh_request);

    let reserve_observed = Arc::new(tokio::sync::Notify::new());
    let reserve_release = Arc::new(tokio::sync::Notify::new());
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = replay_attempt;
    callback.assignment_flags = laminar_core::checkpoint::flags::HANDOFF;
    callback.barrier_outcome = Some(BarrierOutcome::Async);
    callback.reserve_observed = Some(Arc::clone(&reserve_observed));
    callback.reserve_release = Some(Arc::clone(&reserve_release));
    let expire_new_waiter = async move {
        reserve_observed.notified().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        reserve_release.notify_one();
        fresh_wait
    };

    let (_serviced, mut fresh_wait) = tokio::join!(
        coordinator.maybe_checkpoint(&mut callback),
        expire_new_waiter
    );

    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(callback.prepared_attempts[0].0, replay_attempt);
    let active = coordinator
        .manual_active
        .as_ref()
        .expect("the owned replay must retain the exact successor");
    assert_eq!(active.attempt, replay_attempt);
    assert_eq!(
        active.requests.len(),
        1,
        "an expired newcomer attached to the owned replay attempt"
    );
    assert!(active.requests[0].reservation_claim.is_none());
    let error = fresh_completion.await.unwrap().unwrap_err();
    assert!(error
        .to_string()
        .contains("deadline expired before exact attempt reservation"));
    assert!(fresh_wait.wait().await.is_err());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn durable_completion_does_not_publish_or_ack_after_process_lease_loss() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let (source, _poll) = checkpoint_source_handle("source");
    let committed_rx = source.epoch_committed_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    let node_id = laminar_core::state::NodeId(36);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::fenced()))
        .unwrap();
    coordinator.process_authority = Some(SourceProcessAuthority::new(controller));
    let mut callback = MockCallback::new();
    let published = Arc::clone(&callback.published_barriers);
    let aborted = Arc::clone(&callback.aborted_subscription_cuts);
    let previous_cadence = coordinator.last_checkpoint;
    let attempt = CheckpointAttempt::new(14, 14);
    let mut source_checkpoints = FxHashMap::default();
    source_checkpoints.insert("source".to_owned(), checkpoint_at(attempt.epoch));
    let completion = CheckpointCompletion::validated(
        attempt,
        successful_checkpoint_result(attempt),
        source_checkpoints,
        false,
    )
    .unwrap();

    let error = coordinator
        .handle_checkpoint_completion(completion, &mut callback)
        .expect("lease loss must fault local durable-completion publication");

    assert!(error.contains("cluster process lease expired"));
    assert!(published.lock().is_empty());
    assert!(committed_rx.borrow().is_none());
    assert_eq!(aborted.lock().as_slice(), &[attempt]);
    assert_eq!(callback.checkpoint_continuation_failures.len(), 1);
    assert_eq!(callback.checkpoint_continuation_failures[0].0, attempt);
    assert!(callback.checkpoint_continuation_failures[0]
        .1
        .contains("cluster process lease expired"));
    assert_eq!(coordinator.last_published_checkpoint, None);
    assert_eq!(coordinator.last_checkpoint, previous_cadence);
}

#[test]
fn checkpoint_admission_serializes_every_durable_tail() {
    let coordinator = admission_coordinator(Vec::new());
    assert!(coordinator.checkpoint_capacity_available());
    coordinator
        .checkpoint_in_flight
        .store(1, std::sync::atomic::Ordering::Release);
    assert!(!coordinator.checkpoint_capacity_available());
}

#[tokio::test]
async fn pending_output_accelerates_only_a_configured_periodic_checkpoint() {
    let mut periodic = admission_coordinator(Vec::new());
    periodic.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::from_secs(60));
    periodic.last_checkpoint = Instant::now();
    let mut callback = MockCallback::new();
    callback
        .external_checkpoint_pressure
        .store(true, Ordering::Release);

    let admission = periodic
        .checkpoint_admission(&mut callback)
        .await
        .expect("pending output must accelerate the configured periodic cut");
    assert!(!admission.manual);

    for schedule in [CheckpointSchedule::Manual, CheckpointSchedule::Disabled] {
        let mut coordinator = admission_coordinator(Vec::new());
        coordinator.config.checkpoint_schedule = schedule;
        assert!(coordinator
            .checkpoint_admission(&mut callback)
            .await
            .is_none());
    }
}

#[tokio::test]
async fn sourced_async_capture_does_not_advance_checkpoint_cadence() {
    let (source, poll) = checkpoint_source_handle("source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::from_secs(60));
    coordinator.last_checkpoint = Instant::now() - Duration::from_secs(60);
    coordinator.checkpoint_retry_not_before = Some(Instant::now() - Duration::from_millis(1));
    coordinator.checkpoint_retry_backoff = Duration::from_millis(400);
    let previous_cadence = coordinator.last_checkpoint;
    let previous_retry = coordinator.checkpoint_retry_not_before;
    let attempt = CheckpointAttempt::canonical(150);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    callback.barrier_outcome = Some(BarrierOutcome::Async);

    coordinator.maybe_checkpoint(&mut callback).await;
    let barrier = poll
        .poll()
        .expect("periodic source barrier was not injected");
    coordinator
        .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
        .await
        .unwrap();

    assert_eq!(coordinator.last_checkpoint, previous_cadence);
    assert_eq!(coordinator.checkpoint_retry_not_before, previous_retry);
    assert_eq!(
        coordinator.checkpoint_retry_backoff,
        Duration::from_millis(400)
    );
}

#[tokio::test]
async fn source_less_async_capture_does_not_advance_checkpoint_cadence() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::from_secs(60));
    coordinator.last_checkpoint = Instant::now() - Duration::from_secs(60);
    coordinator.checkpoint_retry_not_before = Some(Instant::now() - Duration::from_millis(1));
    coordinator.checkpoint_retry_backoff = Duration::from_millis(400);
    let previous_cadence = coordinator.last_checkpoint;
    let previous_retry = coordinator.checkpoint_retry_not_before;
    let attempt = CheckpointAttempt::new(16, 16);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    callback.barrier_outcome = Some(BarrierOutcome::Async);

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(coordinator.last_checkpoint, previous_cadence);
    assert_eq!(coordinator.checkpoint_retry_not_before, previous_retry);
    assert_eq!(
        coordinator.checkpoint_retry_backoff,
        Duration::from_millis(400)
    );
}

#[tokio::test]
async fn terminal_completions_start_the_next_periodic_delay() {
    let committed = CheckpointAttempt::new(17, 17);
    let failed = CheckpointAttempt::new(18, 18);
    let completions = [
        (
            committed,
            CheckpointCompletion::validated(
                committed,
                successful_checkpoint_result(committed),
                FxHashMap::default(),
                false,
            )
            .unwrap(),
            false,
        ),
        (
            failed,
            CheckpointCompletion::failed(failed, "injected durable-tail failure"),
            true,
        ),
    ];

    for (attempt, completion, failed) in completions {
        let mut coordinator = admission_coordinator(Vec::new());
        let interval = Duration::from_secs(60);
        coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(interval);
        coordinator.last_checkpoint = Instant::now() - interval;
        coordinator.checkpoint_retry_not_before = Some(Instant::now() + interval);
        coordinator.checkpoint_retry_backoff = Duration::from_millis(800);
        let terminal_started = Instant::now();
        let mut callback = MockCallback::new();
        callback.runtime.leader = false;

        assert!(coordinator
            .handle_checkpoint_completion(completion, &mut callback)
            .is_none());

        assert!(coordinator.last_checkpoint >= terminal_started);
        assert!(coordinator.checkpoint_retry_not_before.is_none());
        assert_eq!(coordinator.checkpoint_retry_backoff, Duration::ZERO);
        assert_eq!(callback.checkpoint_failures.len(), usize::from(failed));

        callback.runtime.leader = true;
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 0);

        coordinator.last_checkpoint = Instant::now() - interval;
        let successor = CheckpointAttempt::canonical(attempt.epoch + 100);
        callback.attempt_to_reserve = successor;
        callback.barrier_outcome = Some(BarrierOutcome::Committed(successor.epoch));
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 1);
    }
}

#[tokio::test]
async fn intake_reopen_restarts_periodic_cadence_without_delaying_manual_handoff() {
    let interval = Duration::from_millis(250);
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(interval);
    coordinator.last_checkpoint = Instant::now() - interval;
    coordinator.checkpoint_retry_not_before = Some(Instant::now() - Duration::from_millis(1));
    coordinator.checkpoint_retry_backoff = Duration::from_millis(400);
    let mut state = CoordinatorRunState {
        batch_window: Duration::ZERO,
        checkpoint_control_wake: None,
        shuffle_work_wake: None,
        checkpoint_control_poll_at: tokio::time::Instant::now(),
        checkpoint_control_pending: false,
        intake_was_paused: true,
        barriers: Vec::new(),
        fault: None,
        halted: false,
        halt_reason: None,
        source_channel_expected: false,
    };
    let mut callback = MockCallback::new();

    let reopened_at = Instant::now();
    coordinator.observe_intake_gate_for_checkpoint_cadence(&mut state, false);
    let reopened_cadence = coordinator.last_checkpoint;
    assert!(reopened_cadence >= reopened_at);
    assert!(coordinator.checkpoint_retry_not_before.is_none());
    assert_eq!(coordinator.checkpoint_retry_backoff, Duration::ZERO);
    assert!(coordinator
        .checkpoint_admission(&mut callback)
        .await
        .is_none());

    coordinator.observe_intake_gate_for_checkpoint_cadence(&mut state, false);
    assert_eq!(
        coordinator.last_checkpoint, reopened_cadence,
        "repeated open observations must not keep rearming periodic cadence"
    );
    coordinator.last_checkpoint = Instant::now() - (interval - Duration::from_millis(1));
    assert!(coordinator
        .checkpoint_admission(&mut callback)
        .await
        .is_none());
    coordinator.last_checkpoint = Instant::now() - interval;
    let periodic = coordinator
        .checkpoint_admission(&mut callback)
        .await
        .expect("the ordinary checkpoint must become due one full interval after reopen");
    assert!(!periodic.manual);

    let mut manual = admission_coordinator(Vec::new());
    manual.config.checkpoint_schedule = CheckpointSchedule::Periodic(interval);
    manual.last_checkpoint = Instant::now() - interval;
    state.intake_was_paused = true;
    manual.observe_intake_gate_for_checkpoint_cadence(&mut state, false);
    let (reply_tx, _reply_rx) = crossfire::oneshot::oneshot();
    let (request, _reservation_wait) = unreserved_manual_request(reply_tx);
    manual.manual_waiting.push(request);
    callback.assignment_flags = laminar_core::checkpoint::flags::HANDOFF;

    let handoff = manual
        .checkpoint_admission(&mut callback)
        .await
        .expect("manual HANDOFF must remain immediately eligible after intake reopens");
    assert!(handoff.manual);
    assert_ne!(handoff.flags & laminar_core::checkpoint::flags::HANDOFF, 0);
}

#[tokio::test]
async fn periodic_admission_defers_handoff_to_manual_owner_arriving_during_audit() {
    let interval = Duration::from_secs(60);
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(interval);
    coordinator.last_checkpoint = Instant::now() - interval;
    let cadence_due_since = coordinator.last_checkpoint;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);

    let observed = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let attempt = CheckpointAttempt::canonical(18_001);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    callback.assignment_flags = laminar_core::checkpoint::flags::HANDOFF;
    callback.assignment_admission_observed = Some(Arc::clone(&observed));
    callback.assignment_admission_release = Some(Arc::clone(&release));

    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, mut reservation_wait) = unreserved_manual_request(reply_tx);
    let enqueue_during_periodic_audit = async move {
        observed.notified().await;
        force_tx.send(request).await.unwrap();
        release.notify_one();
    };

    tokio::join!(
        coordinator.maybe_checkpoint(&mut callback),
        enqueue_during_periodic_audit
    );

    assert_eq!(callback.checkpoint_assignment_calls, 1);
    assert_eq!(callback.reserve_calls, 0);
    assert!(callback.prepared_attempts.is_empty());
    assert!(callback.barrier_captures.is_empty());
    assert!(!callback.intake_gate.load(Ordering::Acquire));
    assert_eq!(
        coordinator.last_checkpoint, cadence_due_since,
        "a deferred periodic HANDOFF must leave its cadence due"
    );

    callback.assignment_admission_observed = None;
    callback.assignment_admission_release = None;
    coordinator.maybe_checkpoint(&mut callback).await;

    reservation_wait
        .wait()
        .await
        .expect("the explicit HANDOFF owner must reserve the exact successor attempt");
    let result = reply_rx.await.unwrap().unwrap();
    assert_eq!(
        CheckpointAttempt::new(result.epoch, result.checkpoint_id),
        attempt
    );
    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(callback.prepared_attempts.as_slice(), &[(attempt, None)]);
    assert_eq!(callback.barrier_captures.as_slice(), &[(attempt, 0)]);
    #[cfg(feature = "cluster")]
    assert!(callback.intake_gate.load(Ordering::Acquire));
}

#[tokio::test]
async fn assignment_admission_excludes_drain_until_prepare_and_source_trigger() {
    let (source, barrier_poll) = checkpoint_source_handle("assignment-boundary-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::ZERO);

    let assignment_lock = Arc::new(tokio::sync::Mutex::new(()));
    let reserve_observed = Arc::new(tokio::sync::Notify::new());
    let reserve_release = Arc::new(tokio::sync::Notify::new());
    let attempt = CheckpointAttempt::canonical(18_002);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    callback.assignment_admission_lock = Some(Arc::clone(&assignment_lock));
    callback.reserve_observed = Some(Arc::clone(&reserve_observed));
    callback.reserve_release = Some(Arc::clone(&reserve_release));

    let writer_barrier_poll = barrier_poll.clone();
    let drain_activation = async {
        reserve_observed.notified().await;
        let mut writer = Box::pin(Arc::clone(&assignment_lock).lock_owned());
        assert!(
            matches!(futures::poll!(&mut writer), std::task::Poll::Pending),
            "drain activation must wait after admission and before exact reservation"
        );
        reserve_release.notify_one();
        let guard = tokio::time::timeout(Duration::from_secs(1), &mut writer)
            .await
            .expect("drain activation remained blocked after source-barrier installation");
        let barrier = writer_barrier_poll.poll();
        (guard, barrier)
    };

    let (served, (drain_guard, observed_barrier)) = tokio::join!(
        coordinator.maybe_checkpoint(&mut callback),
        drain_activation
    );
    assert!(served);
    drop(drain_guard);
    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(callback.prepared_attempts.as_slice(), &[(attempt, None)]);
    assert!(callback.abandoned_attempts.lock().is_empty());
    assert_eq!(
        observed_barrier,
        Some(CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch)),
        "drain activation acquired before every source accepted the prepared barrier"
    );
}

#[tokio::test]
async fn source_less_capture_releases_assignment_admission_after_prepare() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::ZERO);
    let assignment_lock = Arc::new(tokio::sync::Mutex::new(()));
    let attempt = CheckpointAttempt::canonical(18_003);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    callback.assignment_admission_lock = Some(Arc::clone(&assignment_lock));

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.prepared_attempts.as_slice(), &[(attempt, None)]);
    assert_eq!(callback.barrier_captures.as_slice(), &[(attempt, 0)]);
    assert!(
        callback
            .capture_saw_assignment_lock_released
            .load(Ordering::Acquire),
        "source-less capture must not nest the admission claim with its graph rotation guard"
    );
    assert!(Arc::clone(&assignment_lock).try_lock_owned().is_ok());
}

#[tokio::test]
async fn source_less_synchronous_outcome_advances_checkpoint_cadence() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::from_secs(60));
    coordinator.last_checkpoint = Instant::now() - Duration::from_secs(60);
    coordinator.checkpoint_retry_not_before = Some(Instant::now() - Duration::from_millis(1));
    coordinator.checkpoint_retry_backoff = Duration::from_millis(400);
    let previous_cadence = coordinator.last_checkpoint;
    let attempt = CheckpointAttempt::new(19, 19);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    callback.barrier_outcome = Some(BarrierOutcome::Committed(attempt.epoch));

    coordinator.maybe_checkpoint(&mut callback).await;

    assert!(coordinator.last_checkpoint > previous_cadence);
    assert!(coordinator.checkpoint_retry_not_before.is_none());
    assert_eq!(coordinator.checkpoint_retry_backoff, Duration::ZERO);
}

#[tokio::test]
async fn sourced_pipeline_without_output_streams_has_one_periodic_barrier_path() {
    // `MockCallback` has no output-stream registrations. Admission must depend on the
    // coordinator's input handles, never on production callback stream publication.
    let (source, poll) = checkpoint_source_handle("input-only");
    let mut coordinator = admission_coordinator(vec![source]);
    let mut callback = MockCallback::new();
    let reserved = CheckpointAttempt::canonical(10_001);
    callback.attempt_to_reserve = reserved;

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.control_checkpoint_calls, 0);
    assert_eq!(callback.reserve_calls, 1);
    assert!(callback.barrier_captures.is_empty());
    assert_eq!(
        poll.poll(),
        Some(CheckpointBarrier::new(
            reserved.checkpoint_id,
            reserved.epoch
        ))
    );
}

#[tokio::test]
async fn deferred_operator_work_does_not_cancel_due_checkpoint() {
    let (source, poll) = checkpoint_source_handle("deferred-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::ZERO);
    coordinator.replay_pending = true;
    let deferred_offset = checkpoint_at(42);
    coordinator.pending_offsets[0] = Some(SourceBatchCursor::Complete(deferred_offset.clone()));
    let attempt = CheckpointAttempt::canonical(10_002);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    let mut state = CoordinatorRunState {
        batch_window: Duration::ZERO,
        checkpoint_control_wake: None,
        shuffle_work_wake: None,
        checkpoint_control_poll_at: tokio::time::Instant::now(),
        checkpoint_control_pending: false,
        intake_was_paused: false,
        barriers: Vec::new(),
        fault: None,
        halted: false,
        halt_reason: None,
        source_channel_expected: true,
    };

    assert!(
        coordinator
            .service_background_work(&mut callback, &mut state, false)
            .await
    );
    assert_eq!(
        poll.poll(),
        Some(CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch))
    );
    assert!(coordinator.pending_barrier.active);
    assert!(coordinator.committed_offsets[0].is_none());

    assert!(
        coordinator
            .service_background_work(&mut callback, &mut state, false)
            .await
    );
    assert!(coordinator.pending_barrier.active);
    assert_eq!(callback.reserve_calls, 1);
    assert!(!coordinator.replay_pending);
    assert!(coordinator.pending_offsets[0].is_none());
    assert_eq!(coordinator.committed_offsets[0], Some(deferred_offset));
    assert_eq!(callback.checkpoint_order.lock().as_slice(), &["drain"]);
    assert!(callback.abandoned_attempts.lock().is_empty());
}

#[tokio::test]
async fn periodic_checkpoint_waits_for_recovery_and_exact_assignment_fence() {
    let mut coordinator = admission_coordinator(Vec::new());
    let mut callback = MockCallback::new();

    callback.runtime.recovering = true;
    coordinator.maybe_checkpoint(&mut callback).await;
    assert_eq!(callback.reserve_calls, 0);

    callback.runtime.recovering = false;
    callback.runtime.assignment_ready = false;
    coordinator.maybe_checkpoint(&mut callback).await;
    assert_eq!(callback.reserve_calls, 0);
    assert!(callback.checkpoint_admission_failures.is_empty());
    assert!(coordinator.checkpoint_retry_not_before.is_some());

    coordinator.checkpoint_retry_not_before = Some(Instant::now() - Duration::from_millis(1));
    callback.runtime.assignment_ready = true;
    coordinator.maybe_checkpoint(&mut callback).await;
    assert_eq!(callback.reserve_calls, 1);
}

#[tokio::test]
async fn assignment_authority_fault_fails_closed_before_attempt_reservation() {
    let mut coordinator = admission_coordinator(Vec::new());
    let mut callback = MockCallback::new();
    callback.assignment_fault = Some("assignment authority is invalid".into());

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.reserve_calls, 0);
    assert_eq!(
        callback.checkpoint_admission_failures,
        ["assignment authority is invalid"]
    );
    assert!(coordinator.checkpoint_retry_not_before.is_none());
}

#[tokio::test]
async fn prepare_publication_failure_prevents_source_cut_and_retains_exact_abort_fence() {
    let (source, poll) = checkpoint_source_handle("prepare-failure");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::ZERO);
    let assignment_fence = assignment_fence(9, &[1, 2]);
    let attempt = CheckpointAttempt::new(107, 107);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    callback.assignment_fence = Some(assignment_fence.clone());
    callback.prepare_error = Some("injected Prepare publication failure".into());

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(
        poll.poll(),
        None,
        "source cut must follow certified Prepare"
    );
    assert_eq!(
        callback.prepared_attempts,
        vec![(attempt, Some(assignment_fence.clone()))]
    );
    assert_eq!(
        callback.abandoned_fences.lock().as_slice(),
        &[Some(assignment_fence)]
    );
}

#[tokio::test]
async fn terminal_shutdown_cancels_hung_checkpoint_prepare_without_false_completion() {
    let terminal_shutdown = tokio_util::sync::CancellationToken::new();
    let coordinator =
        admission_coordinator(Vec::new()).with_terminal_shutdown(terminal_shutdown.clone());
    let prepare_observed = Arc::new(tokio::sync::Notify::new());
    let prepare_release = Arc::new(tokio::sync::Notify::new());
    let mut callback = MockCallback::new();
    callback.prepare_observed = Some(Arc::clone(&prepare_observed));
    callback.prepare_release = Some(Arc::clone(&prepare_release));
    let published = Arc::clone(&callback.published_barriers);
    let reserved_cuts = Arc::clone(&callback.reserved_subscription_cuts);
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    let invalidated = Arc::clone(&callback.invalidated_subscriptions);
    let shutdown_sink_order = Arc::clone(&callback.shutdown_sink_order);

    let run = tokio::spawn(coordinator.run(callback));
    tokio::time::timeout(Duration::from_secs(1), prepare_observed.notified())
        .await
        .expect("periodic checkpoint must reach Prepare publication");
    assert!(
        !run.is_finished(),
        "the unreleased Prepare publication must hold checkpoint admission"
    );

    terminal_shutdown.cancel();
    let exit = tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("terminal shutdown must cancel pre-tail checkpoint work")
        .unwrap();

    assert!(matches!(exit, ExitReason::Shutdown));
    assert!(
        published.lock().is_empty(),
        "an ambiguously published Prepare must not become a local completion"
    );
    assert!(
        reserved_cuts.lock().is_empty(),
        "checkpoint capture must not begin after the cancelled Prepare"
    );
    assert!(
        abandoned.lock().is_empty(),
        "coordinated recovery, not local shutdown, owns ambiguous Prepare reconciliation"
    );
    assert_eq!(shutdown_sink_order.lock().as_slice(), &["settle", "close"]);
    assert_eq!(invalidated.lock().len(), 1);
}

#[tokio::test]
async fn manual_checkpoint_rejects_unready_assignment_without_burning_attempt() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, _reservation_rx) = unreserved_manual_request(reply_tx);
    force_tx.send(request).await.unwrap();
    let mut callback = MockCallback::new();
    callback.runtime.assignment_ready = false;

    coordinator.maybe_checkpoint(&mut callback).await;

    let error = reply_rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("LDB-6056"));
    assert_eq!(callback.reserve_calls, 0);
    assert!(coordinator.manual_waiting.is_empty());
    assert!(coordinator.manual_active.is_none());
}

#[tokio::test(start_paused = true)]
async fn manual_deadline_expiring_during_assignment_audit_does_not_reserve_attempt() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, _reservation_rx) = manual_request_until(
        reply_tx,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    force_tx.send(request).await.unwrap();

    let observed = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut callback = MockCallback::new();
    callback.assignment_admission_observed = Some(Arc::clone(&observed));
    callback.assignment_admission_release = Some(Arc::clone(&release));
    let expire_during_audit = async {
        observed.notified().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        release.notify_one();
    };

    tokio::join!(
        coordinator.maybe_checkpoint(&mut callback),
        expire_during_audit
    );

    let error = reply_rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("deadline expired"), "{error}");
    assert_eq!(callback.checkpoint_assignment_calls, 1);
    assert_eq!(callback.reserve_calls, 0);
    assert!(callback.prepared_attempts.is_empty());
    assert!(coordinator.manual_waiting.is_empty());
}

#[tokio::test]
async fn cancelled_manual_waiter_during_assignment_audit_does_not_reserve_attempt() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, _reservation_rx) = unreserved_manual_request(reply_tx);
    force_tx.send(request).await.unwrap();

    let observed = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut callback = MockCallback::new();
    callback.assignment_admission_observed = Some(Arc::clone(&observed));
    callback.assignment_admission_release = Some(Arc::clone(&release));
    let cancel_during_audit = async {
        observed.notified().await;
        drop(reply_rx);
        release.notify_one();
    };

    tokio::join!(
        coordinator.maybe_checkpoint(&mut callback),
        cancel_during_audit
    );

    assert_eq!(callback.checkpoint_assignment_calls, 1);
    assert_eq!(callback.reserve_calls, 0);
    assert!(callback.prepared_attempts.is_empty());
    assert!(coordinator.manual_waiting.is_empty());
}

#[tokio::test]
async fn cancelled_manual_waiter_after_reservation_does_not_abandon_exact_attempt() {
    let (source, poll) = checkpoint_source_handle("manual-cancel-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, mut reservation_rx) = manual_request_until(
        reply_tx,
        tokio::time::Instant::now() + Duration::from_secs(60),
    );
    force_tx.send(request).await.unwrap();

    let observed = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut callback = MockCallback::new();
    callback.prepare_observed = Some(Arc::clone(&observed));
    callback.prepare_release = Some(Arc::clone(&release));
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    let cancel_during_prepare = async {
        observed.notified().await;
        reservation_rx
            .wait()
            .await
            .expect("exact reservation must be acknowledged before Prepare completes");
        drop(reply_rx);
        release.notify_one();
    };

    tokio::join!(
        coordinator.maybe_checkpoint(&mut callback),
        cancel_during_prepare
    );

    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(callback.prepared_attempts.len(), 1);
    assert!(abandoned.lock().is_empty());
    assert!(poll.poll().is_some());
    assert!(coordinator.manual_waiting.is_empty());
    assert!(coordinator.manual_active.is_some());
}

#[tokio::test(start_paused = true)]
async fn reserved_manual_deadline_replies_only_after_exact_cleanup_finishes() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, mut reservation_rx) = manual_request_until(
        reply_tx,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    force_tx.send(request).await.unwrap();

    let observed = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut callback = MockCallback::new();
    callback.prepare_observed = Some(Arc::clone(&observed));
    callback.prepare_release = Some(Arc::clone(&release));
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    let hold_past_deadline = async {
        observed.notified().await;
        reservation_rx
            .wait()
            .await
            .expect("exact reservation must precede Prepare completion");
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(
            reply_rx.is_empty(),
            "reserved caller received a terminal reply before cleanup"
        );
        release.notify_one();
    };

    tokio::join!(
        coordinator.maybe_checkpoint(&mut callback),
        hold_past_deadline
    );

    let error = reply_rx.await.unwrap().unwrap_err();
    assert!(error
        .to_string()
        .contains("deadline expired after Prepare publication"));
    assert_eq!(callback.prepared_attempts.len(), 1);
    assert_eq!(abandoned.lock().len(), 1);
    assert!(coordinator.manual_active.is_none());
}

#[tokio::test]
async fn cancelled_reservation_claim_stops_before_exact_attempt_or_barrier() {
    let (source, poll) = checkpoint_source_handle("manual-reservation-cancel-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, reservation_rx) = unreserved_manual_request(reply_tx);
    force_tx.send(request).await.unwrap();
    drop(reservation_rx);

    let mut callback = MockCallback::new();
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.reserve_calls, 0);
    assert!(callback.prepared_attempts.is_empty());
    assert!(abandoned.lock().is_empty());
    assert!(poll.poll().is_none());
    assert!(coordinator.manual_waiting.is_empty());
    assert!(coordinator.manual_active.is_none());
    assert!(reply_rx.await.is_err());
}

#[tokio::test]
async fn cancelled_coalesced_waiter_does_not_abort_an_attempt_owned_by_a_live_waiter() {
    let (source, poll) = checkpoint_source_handle("manual-coalesced-cancel-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);

    let (cancelled_reply, cancelled_completion) = crossfire::oneshot::oneshot();
    let (cancelled_request, cancelled_reservation) = unreserved_manual_request(cancelled_reply);
    let (live_reply, _live_completion) = crossfire::oneshot::oneshot();
    let (live_request, mut live_reservation) = unreserved_manual_request(live_reply);
    force_tx.send(cancelled_request).await.unwrap();
    force_tx.send(live_request).await.unwrap();
    drop(cancelled_reservation);
    drop(cancelled_completion);

    let mut callback = MockCallback::new();
    callback.barrier_outcome = Some(BarrierOutcome::Async);
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    coordinator.maybe_checkpoint(&mut callback).await;

    live_reservation
        .wait()
        .await
        .expect("the live coalesced waiter must observe exact reservation");
    assert_eq!(callback.reserve_calls, 1);
    assert!(abandoned.lock().is_empty());
    assert!(poll.poll().is_some());
    assert_eq!(
        coordinator
            .manual_active
            .as_ref()
            .expect("the live waiter owns the attempt")
            .requests
            .len(),
        1
    );
}

#[tokio::test]
async fn manual_only_checkpointing_does_not_schedule_periodic_attempts() {
    let (source, poll) = checkpoint_source_handle("manual-only-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let mut callback = MockCallback::new();

    coordinator.maybe_checkpoint(&mut callback).await;
    assert_eq!(callback.reserve_calls, 0);
    assert_eq!(poll.poll(), None);

    let (reply_tx, _reply_rx) = crossfire::oneshot::oneshot();
    let (request, _reservation_rx) = unreserved_manual_request(reply_tx);
    force_tx.send(request).await.unwrap();
    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.reserve_calls, 1);
    assert!(poll.poll().is_some());
}

#[tokio::test]
async fn source_less_local_periodic_checkpoint_uses_exact_attempt_capture() {
    let mut coordinator = admission_coordinator(Vec::new());
    let mut callback = MockCallback::new();
    let reserved = CheckpointAttempt::new(102, 102);
    callback.attempt_to_reserve = reserved;

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.control_checkpoint_calls, 0);
    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(callback.barrier_captures, vec![(reserved, 0)]);
    assert!(callback.abandoned_attempts.lock().is_empty());
    assert_eq!(
        *callback.published_barriers.lock(),
        vec![(reserved.epoch, reserved.checkpoint_id)]
    );
}

#[tokio::test]
async fn source_less_cluster_follower_never_originates_checkpoint_attempt() {
    let mut coordinator = admission_coordinator(Vec::new());
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.control_checkpoint_calls, 1);
    assert_eq!(callback.reserve_calls, 0);
    assert!(callback.barrier_captures.is_empty());
}

#[tokio::test]
async fn cluster_follower_rejects_manual_checkpoint_instead_of_stranding_caller() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, _reservation_rx) = unreserved_manual_request(reply_tx);
    force_tx.send(request).await.unwrap();
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;

    coordinator.maybe_checkpoint(&mut callback).await;

    let reply = tokio::time::timeout(Duration::from_secs(1), reply_rx)
        .await
        .expect("follower must answer the manual caller")
        .unwrap();
    let error = reply.expect_err("a follower cannot originate a checkpoint");
    assert!(error.to_string().contains("only the cluster leader"));
    assert_eq!(callback.reserve_calls, 0);
    assert_eq!(callback.control_checkpoint_calls, 1);
    assert!(coordinator.manual_waiting.is_empty());
}

#[tokio::test]
async fn busy_source_injector_preflight_does_not_reserve_an_attempt() {
    let (busy_source, busy_poll) = checkpoint_source_handle("busy");
    let (idle_source, idle_poll) = checkpoint_source_handle("idle");
    let already_pending = CheckpointBarrier::new(71, 71);
    assert!(busy_source.barrier_injector.trigger(already_pending));

    let mut coordinator = admission_coordinator(vec![busy_source, idle_source]);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = CheckpointAttempt::canonical(9_001);

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(
        callback.reserve_calls, 0,
        "preflight must not burn a durable checkpoint ID while any injector is busy"
    );
    assert!(!coordinator.pending_barrier.active);
    assert_eq!(busy_poll.poll(), Some(already_pending));
    assert_eq!(idle_poll.poll(), None);
}

#[tokio::test]
async fn admitted_checkpoint_injects_the_exact_durably_reserved_attempt() {
    let (source_0, poll_0) = checkpoint_source_handle("source-0");
    let (source_1, poll_1) = checkpoint_source_handle("source-1");
    let mut coordinator = admission_coordinator(vec![source_0, source_1]);
    let mut callback = MockCallback::new();
    let reserved = CheckpointAttempt::canonical(u64::from(u32::MAX) + 8_192);
    callback.attempt_to_reserve = reserved;

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(coordinator.pending_barrier.attempt, Some(reserved));
    assert!(coordinator.pending_barrier.active);
    for injected in [poll_0.poll(), poll_1.poll()] {
        let injected = injected.expect("every preflighted source must receive the barrier");
        assert_eq!(injected.epoch, reserved.epoch);
        assert_eq!(injected.checkpoint_id, reserved.checkpoint_id);
    }
}

#[tokio::test]
async fn ephemeral_source_aligns_without_publishing_a_recovery_cursor() {
    let (mut source, _poll) = checkpoint_source_handle("local-ingress");
    source.recovery_cursor = false;
    let mut coordinator = admission_coordinator(vec![source]);
    let mut local_progress = SourceCheckpoint::new();
    local_progress.set_offset("records_polled", "41");
    coordinator.committed_offsets[0] = Some(local_progress.clone());

    assert!(
        coordinator.current_source_offsets().is_empty(),
        "follower readiness must not publish non-replayable local progress"
    );

    let attempt = CheckpointAttempt::new(42, 42);
    coordinator.pending_barrier.reset(attempt, 1);
    let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
    let mut callback = MockCallback::new();
    coordinator
        .handle_barrier(0, &barrier, &local_progress, &mut callback)
        .await
        .unwrap();

    assert_eq!(
        callback.barrier_captures,
        vec![(attempt, 0)],
        "an ephemeral source must align the state cut without entering its manifest"
    );
}

#[tokio::test]
async fn checkpoint_drain_precedes_capture_and_exact_source_release() {
    let (source, _poll) = checkpoint_source_handle("source");
    let release = source.barrier_release_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    let attempt = CheckpointAttempt::new(61, 61);
    coordinator.pending_barrier.reset(attempt, 1);
    let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
    let mut callback = MockCallback::new();
    let order = Arc::clone(&callback.checkpoint_order);

    coordinator
        .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
        .await
        .unwrap();

    assert_eq!(order.lock().as_slice(), &["drain", "capture"]);
    assert_eq!(
        *release.borrow(),
        Some(SourceBarrierSignal::Release(attempt))
    );
}

#[tokio::test]
async fn checkpoint_drain_failure_cleans_up_and_keeps_source_held() {
    let (source, _poll) = checkpoint_source_handle("source");
    let release = source.barrier_release_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    let attempt = CheckpointAttempt::new(62, 62);
    coordinator.pending_barrier.reset(attempt, 1);
    let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
    let mut callback = MockCallback::new();
    callback.checkpoint_drain_error =
        Some(CycleError::Recovery("injected graph drain failure".into()));
    let order = Arc::clone(&callback.checkpoint_order);

    let error = coordinator
        .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("injected graph drain failure"));
    assert_eq!(order.lock().as_slice(), &["drain", "cleanup"]);
    assert_eq!(*release.borrow(), None);
}

#[tokio::test]
async fn checkpoint_drain_halt_cleans_up_and_exits_without_recovery() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, source_rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        source_rx,
        control_rx,
        shutdown,
        DeliveryGuarantee::AtLeastOnce,
        Some(Duration::from_secs(60)),
    );
    let attempt = CheckpointAttempt::new(64, 64);
    coordinator.pending_barrier.reset(attempt, 1);
    let mut callback = MockCallback::new();
    callback.checkpoint_drain_error = Some(CycleError::Halt(
        "injected terminal checkpoint drain".into(),
    ));
    let order = Arc::clone(&callback.checkpoint_order);
    source_tx
        .send(SourceMsg::Barrier {
            source_idx: 0,
            barrier: CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            checkpoint: checkpoint_at(attempt.epoch),
        })
        .await
        .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("terminal checkpoint drain must stop the coordinator");

    assert!(matches!(exit, ExitReason::Halt(ref reason)
        if reason == "injected terminal checkpoint drain"));
    assert_eq!(order.lock().as_slice(), &["drain", "cleanup"]);
    drop(source_tx);
}

#[tokio::test]
async fn checkpoint_alignment_halt_beats_the_recovery_fault_side_channel() {
    let (source, _poll) = checkpoint_source_handle("source");
    let mut coordinator = admission_coordinator(vec![source]);
    let attempt = CheckpointAttempt::new(65, 65);
    coordinator.pending_barrier.reset(attempt, 1);
    let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
    let mut callback = MockCallback::new();
    callback.barrier_outcome = Some(BarrierOutcome::Failed);
    callback.pipeline_halt = Some("permanent shuffle routing failure".into());
    callback.pipeline_fault = Some("generic graph execution poison".into());

    let error = coordinator
        .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
        .await
        .expect_err("the terminal alignment disposition must stop the coordinator");

    assert!(matches!(error, CycleError::Halt(ref reason)
        if reason == "permanent shuffle routing failure"));
    assert_eq!(
        callback.pipeline_fault.as_deref(),
        Some("generic graph execution poison"),
        "halt arbitration must not consume or expose the lower-priority recovery fault"
    );
}

#[tokio::test]
async fn checkpoint_cleanup_failure_keeps_source_held() {
    let (source, _poll) = checkpoint_source_handle("source");
    let release = source.barrier_release_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    let attempt = CheckpointAttempt::new(63, 63);
    coordinator.pending_barrier.reset(attempt, 1);
    let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
    let mut callback = MockCallback::new();
    callback.barrier_outcome = Some(BarrierOutcome::Failed);
    callback.abandon_error = Some("injected rollback failure".into());
    let order = Arc::clone(&callback.checkpoint_order);

    let error = coordinator
        .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("injected rollback failure"));
    assert_eq!(order.lock().as_slice(), &["drain", "capture", "cleanup"]);
    assert_eq!(*release.borrow(), None);
}

#[tokio::test]
async fn mutable_capture_fault_keeps_source_held_for_recovery() {
    let (source, _poll) = checkpoint_source_handle("source");
    let release = source.barrier_release_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    let attempt = CheckpointAttempt::new(64, 64);
    coordinator.pending_barrier.reset(attempt, 1);
    let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);
    let mut callback = MockCallback::new();
    callback.barrier_outcome = Some(BarrierOutcome::Failed);
    callback.pipeline_fault = Some(
        "operator state checkpoint capture failed; recovery from the last committed checkpoint is required"
            .into(),
    );
    let order = Arc::clone(&callback.checkpoint_order);

    let error = coordinator
        .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("recovery from the last committed checkpoint is required"));
    assert_eq!(order.lock().as_slice(), &["drain", "capture", "cleanup"]);
    assert_eq!(*release.borrow(), None);
}

#[tokio::test]
async fn manual_requests_coalesce_onto_one_new_exact_source_barrier() {
    let (source, poll) = checkpoint_source_handle("manual-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(8);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);

    let (first_sender, first_completion) = crossfire::oneshot::oneshot();
    let (second_sender, second_completion) = crossfire::oneshot::oneshot();
    let (first_request, mut first_reservation) = unreserved_manual_request(first_sender);
    let (second_request, mut second_reservation) = unreserved_manual_request(second_sender);
    force_tx.send(first_request).await.unwrap();
    force_tx.send(second_request).await.unwrap();

    let attempt = CheckpointAttempt::canonical(8_080);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = attempt;
    callback.barrier_outcome = Some(BarrierOutcome::Async);

    coordinator.maybe_checkpoint(&mut callback).await;

    first_reservation
        .wait()
        .await
        .expect("first caller must observe exact reservation");
    second_reservation
        .wait()
        .await
        .expect("second caller must observe exact reservation");

    assert_eq!(callback.reserve_calls, 1);
    let active = coordinator
        .manual_active
        .as_ref()
        .expect("manual callers must attach at admission");
    assert_eq!(active.attempt, attempt);
    assert_eq!(active.requests.len(), 2);
    assert!(coordinator.manual_waiting.is_empty());

    let barrier = poll
        .poll()
        .expect("manual attempt must inject a source barrier");
    assert_eq!(
        barrier,
        CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch)
    );
    coordinator
        .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
        .await
        .unwrap();
    assert!(coordinator.manual_active.is_some());

    let result = successful_checkpoint_result(attempt);
    let completion =
        CheckpointCompletion::validated(attempt, result.clone(), FxHashMap::default(), false)
            .unwrap();
    assert!(coordinator
        .handle_checkpoint_completion(completion, &mut callback)
        .is_none());

    for reply in [first_completion, second_completion] {
        let completed = reply.await.unwrap().unwrap();
        assert_eq!(completed.epoch, attempt.epoch);
        assert_eq!(completed.checkpoint_id, attempt.checkpoint_id);
    }
    assert!(coordinator.manual_active.is_none());
}

#[tokio::test]
async fn manual_reservation_failure_replies_instead_of_hanging() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(2);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    let (request, _reservation_rx) = unreserved_manual_request(reply_tx);
    force_tx.send(request).await.unwrap();

    let mut callback = MockCallback::new();
    callback.reserve_error = Some("decision store unavailable".into());
    coordinator.maybe_checkpoint(&mut callback).await;

    let error = reply_rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("decision store unavailable"));
    assert!(coordinator.manual_waiting.is_empty());
    assert!(coordinator.manual_active.is_none());
}

#[tokio::test]
async fn manual_request_after_admission_waits_for_the_next_attempt() {
    let (source, poll) = checkpoint_source_handle("manual-source");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Manual;
    let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointRequest>(8);
    coordinator = coordinator.with_force_checkpoint_rx(force_rx);

    let first = CheckpointAttempt::canonical(8_081);
    let second = CheckpointAttempt::canonical(8_099);
    let (first_tx, first_rx) = crossfire::oneshot::oneshot();
    let (first_request, mut first_reservation) = unreserved_manual_request(first_tx);
    force_tx.send(first_request).await.unwrap();
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = first;
    callback.barrier_outcome = Some(BarrierOutcome::Async);
    coordinator.maybe_checkpoint(&mut callback).await;
    first_reservation
        .wait()
        .await
        .expect("first caller must observe exact reservation");
    let first_barrier = poll.poll().unwrap();

    let (second_tx, second_rx) = crossfire::oneshot::oneshot();
    let (second_request, mut second_reservation) = unreserved_manual_request(second_tx);
    force_tx.send(second_request).await.unwrap();
    coordinator.maybe_checkpoint(&mut callback).await;
    assert_eq!(coordinator.manual_waiting.len(), 1);
    assert_eq!(coordinator.manual_active.as_ref().unwrap().attempt, first);

    coordinator
        .handle_barrier(
            0,
            &first_barrier,
            &checkpoint_at(first.epoch),
            &mut callback,
        )
        .await
        .unwrap();
    assert!(coordinator
        .handle_checkpoint_completion(
            CheckpointCompletion::validated(
                first,
                successful_checkpoint_result(first),
                FxHashMap::default(),
                false,
            )
            .unwrap(),
            &mut callback,
        )
        .is_none());
    assert_eq!(
        first_rx.await.unwrap().unwrap().checkpoint_id,
        first.checkpoint_id
    );
    assert_eq!(coordinator.manual_waiting.len(), 1);

    callback.attempt_to_reserve = second;
    coordinator.maybe_checkpoint(&mut callback).await;
    second_reservation
        .wait()
        .await
        .expect("second caller must observe exact reservation");
    assert_eq!(coordinator.manual_active.as_ref().unwrap().attempt, second);
    let second_barrier = poll.poll().unwrap();
    assert_eq!(
        second_barrier,
        CheckpointBarrier::new(second.checkpoint_id, second.epoch)
    );
    callback.barrier_outcome = Some(BarrierOutcome::Async);
    coordinator
        .handle_barrier(
            0,
            &second_barrier,
            &checkpoint_at(second.epoch),
            &mut callback,
        )
        .await
        .unwrap();

    assert!(coordinator
        .handle_checkpoint_completion(
            CheckpointCompletion::failed(second, "injected durable-tail failure"),
            &mut callback,
        )
        .is_none());
    let error = second_rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("injected durable-tail failure"));
}

#[tokio::test]
async fn noncommitted_aligned_checkpoint_abandons_exact_attempt_with_correct_cadence() {
    use super::super::callback::SkipReason;

    let outcomes = [
        (
            BarrierOutcome::Skipped(SkipReason::PreservingReplayWindowAfterSinkTimeout),
            "preserving_replay_window_after_sink_timeout",
            false,
            true,
        ),
        (
            BarrierOutcome::CancelledBeforeCapture,
            "checkpoint topology closed before state capture",
            false,
            false,
        ),
        (
            BarrierOutcome::Aborted,
            "checkpoint was aborted by authoritative cluster control before state capture",
            false,
            true,
        ),
        (
            BarrierOutcome::Failed,
            "barrier-aligned checkpoint failed before durable tail",
            true,
            true,
        ),
    ];

    for (outcome, expected_reason, records_failure, advances_cadence) in outcomes {
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::new(tokio::sync::Notify::new()),
            DeliveryGuarantee::ExactlyOnce,
            None,
        );
        coordinator.last_checkpoint = Instant::now() - Duration::from_secs(60);
        let previous_cadence = coordinator.last_checkpoint;
        let attempt = CheckpointAttempt::new(53, 53);
        coordinator.pending_barrier.reset(attempt, 1);
        let mut callback = MockCallback::new();
        callback.barrier_outcome = Some(outcome);
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);

        coordinator
            .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
            .await
            .unwrap();

        let abandoned = callback.abandoned_attempts.lock();
        assert_eq!(abandoned.len(), 1);
        assert_eq!(abandoned[0].0, attempt);
        assert_eq!(abandoned[0].1, expected_reason);
        assert_eq!(
            callback.checkpoint_failures.len(),
            usize::from(records_failure)
        );
        if records_failure {
            assert_eq!(callback.checkpoint_failures[0].0, attempt.checkpoint_id);
        }
        if advances_cadence {
            assert!(coordinator.last_checkpoint > previous_cadence);
            assert!(coordinator.checkpoint_retry_not_before.is_none());
        } else {
            assert_eq!(coordinator.last_checkpoint, previous_cadence);
            assert!(coordinator.checkpoint_retry_not_before.is_some());
        }
    }
}

#[tokio::test]
async fn topology_retry_waits_for_backoff_and_ready_assignment_without_burning_an_attempt() {
    let (source, poll) = checkpoint_source_handle("topology-retry");
    let mut coordinator = admission_coordinator(vec![source]);
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::from_secs(60));
    coordinator.last_checkpoint = Instant::now() - Duration::from_secs(60);
    let cancelled = CheckpointAttempt::canonical(90_053);
    coordinator.pending_barrier.reset(cancelled, 1);
    let mut callback = MockCallback::new();
    callback.barrier_outcome = Some(BarrierOutcome::CancelledBeforeCapture);

    coordinator
        .handle_barrier(
            0,
            &CheckpointBarrier::new(cancelled.checkpoint_id, cancelled.epoch),
            &checkpoint_at(cancelled.epoch),
            &mut callback,
        )
        .await
        .unwrap();

    let assignment_calls = callback.checkpoint_assignment_calls;
    coordinator.maybe_checkpoint(&mut callback).await;
    assert_eq!(callback.checkpoint_assignment_calls, assignment_calls);
    assert_eq!(callback.reserve_calls, 0);

    coordinator.checkpoint_retry_not_before = Some(Instant::now() - Duration::from_millis(1));
    callback.runtime.assignment_ready = false;
    coordinator.maybe_checkpoint(&mut callback).await;
    assert_eq!(callback.reserve_calls, 0);
    assert!(callback.checkpoint_admission_failures.is_empty());

    let successor = CheckpointAttempt::canonical(90_054);
    coordinator.checkpoint_retry_not_before = Some(Instant::now() - Duration::from_millis(1));
    callback.runtime.assignment_ready = true;
    callback.attempt_to_reserve = successor;
    coordinator.maybe_checkpoint(&mut callback).await;
    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(
        poll.poll(),
        Some(CheckpointBarrier::new(
            successor.checkpoint_id,
            successor.epoch
        ))
    );
}

#[tokio::test]
async fn source_less_topology_cancellation_keeps_periodic_checkpoint_due() {
    let mut coordinator = admission_coordinator(Vec::new());
    coordinator.config.checkpoint_schedule = CheckpointSchedule::Periodic(Duration::from_secs(60));
    coordinator.last_checkpoint = Instant::now() - Duration::from_secs(60);
    let previous_cadence = coordinator.last_checkpoint;
    let cancelled = CheckpointAttempt::new(55, 55);
    let mut callback = MockCallback::new();
    callback.attempt_to_reserve = cancelled;
    callback.barrier_outcome = Some(BarrierOutcome::CancelledBeforeCapture);

    coordinator.maybe_checkpoint(&mut callback).await;

    assert_eq!(callback.reserve_calls, 1);
    assert_eq!(coordinator.last_checkpoint, previous_cadence);
    assert!(coordinator.checkpoint_retry_not_before.is_some());
    assert_eq!(callback.abandoned_attempts.lock()[0].0, cancelled);
}

#[test]
fn topology_retry_backoff_is_bounded_and_resets_with_checkpoint_cadence() {
    let mut coordinator = admission_coordinator(Vec::new());

    for expected in [
        Duration::from_millis(100),
        Duration::from_millis(200),
        Duration::from_millis(400),
        Duration::from_millis(800),
        Duration::from_millis(1_600),
        Duration::from_millis(3_200),
        Duration::from_secs(5),
        Duration::from_secs(5),
    ] {
        coordinator.defer_checkpoint_until_topology_ready();
        assert_eq!(coordinator.checkpoint_retry_backoff, expected);
        assert!(coordinator.checkpoint_retry_not_before.is_some());
    }

    coordinator.advance_checkpoint_cadence();
    assert_eq!(coordinator.checkpoint_retry_backoff, Duration::ZERO);
    assert!(coordinator.checkpoint_retry_not_before.is_none());
}

#[tokio::test]
async fn originator_aligned_abort_abandons_after_role_change_without_failure() {
    let (source, _poll) = checkpoint_source_handle("source");
    let mut release = source.barrier_release_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    let attempt = CheckpointAttempt::new(54, 54);
    coordinator.pending_barrier.reset(attempt, 1);
    let mut callback = MockCallback::new();
    callback.runtime.leader = false;
    callback.barrier_outcome = Some(BarrierOutcome::Aborted);
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    let aborted_cuts = Arc::clone(&callback.aborted_subscription_cuts);

    coordinator
        .handle_barrier(
            0,
            &CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &checkpoint_at(attempt.epoch),
            &mut callback,
        )
        .await
        .unwrap();

    assert!(callback.checkpoint_failures.is_empty());
    assert_eq!(aborted_cuts.lock().as_slice(), &[attempt]);
    let abandoned = abandoned.lock();
    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].0, attempt);
    assert!(abandoned[0].1.contains("authoritative cluster control"));
    assert_eq!(
        *release.borrow_and_update(),
        Some(SourceBarrierSignal::Release(attempt))
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_aligned_abort_resolves_after_role_change_without_failure() {
    let (source, _poll) = checkpoint_source_handle("source");
    let mut release = source.barrier_release_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    install_test_process_authority(&mut coordinator, 56);
    let attempt = CheckpointAttempt::new(56, 56);
    coordinator
        .pending_barrier
        .reset_follower(attempt, 1, laminar_core::checkpoint::flags::NONE);
    let mut callback = MockCallback::new();
    callback.runtime.leader = true;
    callback.barrier_outcome = Some(BarrierOutcome::Aborted);
    let cancelled = Arc::clone(&callback.cancelled_source_barrier_attempts);
    let resolved = Arc::clone(&callback.resolved_follower_aborts);

    coordinator
        .handle_barrier(
            0,
            &CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &checkpoint_at(attempt.epoch),
            &mut callback,
        )
        .await
        .unwrap();

    assert!(callback.checkpoint_failures.is_empty());
    assert!(cancelled.lock().is_empty());
    assert_eq!(resolved.lock().as_slice(), &[attempt]);
    assert_eq!(
        *release.borrow_and_update(),
        Some(SourceBarrierSignal::Release(attempt))
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_topology_cancellation_preserves_checkpoint_cadence_after_promotion() {
    let (source, _poll) = checkpoint_source_handle("source");
    let mut coordinator = admission_coordinator(vec![source]);
    install_test_process_authority(&mut coordinator, 56);
    coordinator.last_checkpoint = Instant::now() - Duration::from_secs(60);
    let previous_cadence = coordinator.last_checkpoint;
    let attempt = CheckpointAttempt::new(56, 56);
    coordinator
        .pending_barrier
        .reset_follower(attempt, 1, laminar_core::checkpoint::flags::NONE);
    let mut callback = MockCallback::new();
    callback.runtime.leader = true;
    callback.barrier_outcome = Some(BarrierOutcome::CancelledBeforeCapture);

    coordinator
        .handle_barrier(
            0,
            &CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &checkpoint_at(attempt.epoch),
            &mut callback,
        )
        .await
        .unwrap();

    assert_eq!(coordinator.last_checkpoint, previous_cadence);
    assert!(coordinator.checkpoint_retry_not_before.is_none());
    assert_eq!(
        callback.cancelled_source_barrier_attempts.lock()[0].0,
        attempt
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_aligned_abort_cleanup_failure_keeps_sources_fenced() {
    let (source, _poll) = checkpoint_source_handle("source");
    let mut release = source.barrier_release_tx.subscribe();
    let mut coordinator = admission_coordinator(vec![source]);
    install_test_process_authority(&mut coordinator, 57);
    let attempt = CheckpointAttempt::new(57, 57);
    coordinator
        .pending_barrier
        .reset_follower(attempt, 1, laminar_core::checkpoint::flags::NONE);
    let mut callback = MockCallback::new();
    callback.barrier_outcome = Some(BarrierOutcome::Aborted);
    callback.resolve_follower_abort_error = Some("injected local cleanup failure".into());

    let error = coordinator
        .handle_barrier(
            0,
            &CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &checkpoint_at(attempt.epoch),
            &mut callback,
        )
        .await
        .expect_err("failed follower Abort cleanup must require recovery");

    assert!(matches!(error, CycleError::Recovery(_)), "{error:?}");
    assert!(callback.cancelled_source_barrier_attempts.lock().is_empty());
    assert_eq!(
        callback.resolved_follower_aborts.lock().as_slice(),
        &[attempt]
    );
    assert!(callback.checkpoint_failures.is_empty());
    assert_eq!(*release.borrow_and_update(), None);
}

#[tokio::test]
async fn authoritative_source_less_abort_abandons_without_failure() {
    let mut coordinator = admission_coordinator(Vec::new());
    let admission = CheckpointAdmission {
        manual: false,
        flags: laminar_core::checkpoint::flags::NONE,
        assignment_fence: None,
        assignment_guard: None,
        deadline: tokio::time::Instant::now() + Duration::from_secs(60),
    };
    let attempt = CheckpointAttempt::new(55, 55);
    let mut callback = MockCallback::new();
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    let aborted_cuts = Arc::clone(&callback.aborted_subscription_cuts);

    coordinator
        .handle_source_less_checkpoint_outcome(
            &mut callback,
            &admission,
            attempt,
            BarrierOutcome::Aborted,
        )
        .await
        .unwrap();

    assert!(callback.checkpoint_failures.is_empty());
    assert_eq!(aborted_cuts.lock().as_slice(), &[attempt]);
    let abandoned = abandoned.lock();
    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].0, attempt);
    assert!(abandoned[0].1.contains("authoritative cluster control"));
}

#[tokio::test]
async fn alignment_timeout_abandons_the_exact_reserved_attempt() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::ExactlyOnce,
        None,
    );
    coordinator.config.checkpoint_timeout = Duration::ZERO;
    let attempt = CheckpointAttempt::new(61, 61);
    coordinator.pending_barrier.reset(attempt, 2);
    coordinator.pending_barrier.sources_aligned.insert(0);

    let callback = MockCallback::new();
    let abandoned_attempts = Arc::clone(&callback.abandoned_attempts);
    let observed_abandoned_attempts = Arc::clone(&abandoned_attempts);
    let stop = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let observed = !observed_abandoned_attempts.lock().is_empty();
                if observed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("alignment timeout was not observed");
        shutdown.notify_one();
    });

    let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
        .await
        .expect("coordinator must stop after the timeout is observed");
    stop.await.expect("timeout observer must not panic");

    assert!(matches!(exit, ExitReason::Shutdown));
    let abandoned = abandoned_attempts.lock();
    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].0, attempt);
    assert_eq!(abandoned[0].1, "source barrier alignment timeout");
}

#[derive(Default)]
struct StartupSourceState {
    open: AtomicBool,
    open_calls: AtomicU64,
    start_completions: AtomicU64,
    restore_calls: AtomicU64,
    close_calls: AtomicU64,
    poll_calls: AtomicU64,
}

struct StartupSource {
    state: Arc<StartupSourceState>,
    schema: Arc<Schema>,
    start_delay: Duration,
    close_delay: Duration,
    fail_open: bool,
    fail_restore: bool,
    cancellation_policy: ConnectorCancellationPolicy,
    contract: laminar_connectors::connector::SourceContract,
}

struct LateBoundMutationSource {
    starts: Arc<AtomicU64>,
    started: bool,
}

fn test_source_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

struct TrackedStartupSource {
    _task_owner: laminar_connectors::connector::ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
    tracker_calls: Arc<AtomicU64>,
    start_error: Option<ConnectorError>,
    close_calls: Arc<AtomicU64>,
}

#[derive(Default)]
struct BarrierRetrySourceState {
    allow_capture: AtomicBool,
    block_checkpoint_ready: AtomicBool,
    emit_batch: AtomicBool,
    assignment_version: AtomicU64,
    capture_attempts: AtomicU64,
    successful_captures: AtomicU64,
    polls: AtomicU64,
}

struct BarrierRetrySource {
    state: Arc<BarrierRetrySourceState>,
}

#[derive(Default)]
struct BarrierHoldProbeState {
    starts: AtomicU64,
    polls: AtomicU64,
    control_drives: AtomicU64,
    commit_notifications: AtomicU64,
    closes: AtomicU64,
    data_ready: Arc<tokio::sync::Notify>,
    #[cfg(feature = "cluster")]
    drain_begins: AtomicU64,
    #[cfg(feature = "cluster")]
    drain_finish_starts: AtomicU64,
    #[cfg(feature = "cluster")]
    drain_finishes: AtomicU64,
    #[cfg(feature = "cluster")]
    cancelled_drain_finishes: AtomicU64,
    #[cfg(feature = "cluster")]
    block_drain_finish: AtomicBool,
    #[cfg(feature = "cluster")]
    drain_finish_started: tokio::sync::Notify,
    #[cfg(feature = "cluster")]
    release_drain_finish: tokio::sync::Notify,
}

struct BarrierHoldProbeSource {
    state: Arc<BarrierHoldProbeState>,
}

#[cfg(feature = "cluster")]
struct DrainFinishGuard {
    state: Arc<BarrierHoldProbeState>,
    completed: bool,
}

#[cfg(feature = "cluster")]
impl Drop for DrainFinishGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.state
                .cancelled_drain_finishes
                .fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(feature = "cluster")]
fn barrier_hold_probe_source(name: &str, state: Arc<BarrierHoldProbeState>) -> SourceRegistration {
    SourceRegistration {
        name: name.into(),
        connector: Box::new(BarrierHoldProbeSource { state }),
        config: laminar_connectors::config::ConnectorConfig::new(name),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    }
}

#[async_trait::async_trait]
impl SourceConnector for BarrierHoldProbeSource {
    fn contract(
        &self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<laminar_connectors::connector::SourceContract, ConnectorError> {
        Ok(replayable_append_only_source_contract())
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        self.state.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.state.polls.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }

    fn schema(&self) -> Arc<Schema> {
        test_source_schema()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
        Ok(true)
    }

    fn drive_control_plane(&mut self) {
        self.state.control_drives.fetch_add(1, Ordering::SeqCst);
    }

    fn data_ready_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(Arc::clone(&self.state.data_ready))
    }

    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        _checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.state
            .commit_notifications
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn begin_drain(
        &mut self,
        _request: &SourceDrainRequest,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.state.drain_begins.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn poll_drain_ready(&mut self, _round: AssignmentDrainId) -> Result<bool, ConnectorError> {
        Ok(true)
    }

    #[cfg(feature = "cluster")]
    async fn finish_drain(
        &mut self,
        _resolution: SourceDrainResolution,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.state
            .drain_finish_starts
            .fetch_add(1, Ordering::SeqCst);
        if self.state.block_drain_finish.load(Ordering::Acquire) {
            let mut guard = DrainFinishGuard {
                state: Arc::clone(&self.state),
                completed: false,
            };
            self.state.drain_finish_started.notify_one();
            self.state.release_drain_finish.notified().await;
            guard.completed = true;
        }
        self.state.drain_finishes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.state.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(feature = "cluster")]
#[test]
fn source_drain_receipts_reject_stale_processes_and_duplicate_tasks() {
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let receipt = SourceDrainReceipt {
        round,
        participant,
        source_task_incarnation: uuid::Uuid::from_u128(101),
    };
    validate_source_drain_receipts(round, participant, std::slice::from_ref(&receipt)).unwrap();
    assert!(validate_source_drain_receipts(
        round,
        participant,
        &[receipt.clone(), receipt.clone()]
    )
    .is_err());

    let mut stale = receipt;
    stale.participant.boot_incarnation = uuid::Uuid::from_u128(12);
    assert!(validate_source_drain_receipts(round, participant, &[stale]).is_err());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_drain_ready_is_retained_before_coordinator_subscription() {
    let mut connector = BarrierHoldProbeSource {
        state: Arc::new(BarrierHoldProbeState::default()),
    };
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    let request = SourceDrainRequest::new(round).unwrap();
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let (command_tx, mut command_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (status_tx, status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    drop(status_rx);
    let control = SourceDrainLeaseControl {
        task_incarnation: uuid::Uuid::from_u128(101),
        command_tx,
        status_tx,
        wake: Arc::new(tokio::sync::Notify::new()),
    };
    control
        .command_tx
        .send(Some(SourceDrainCommand::Begin {
            request,
            participant,
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();

    let mut active = None;
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &control.status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();
    publish_source_drain_ready(&mut connector, &control, &mut active).unwrap();

    let status_rx = control.status_tx.subscribe();
    assert!(matches!(
        status_rx.borrow().clone(),
        SourceDrainTaskStatus::Ready(receipt)
            if receipt.round == round
                && receipt.participant == participant
                && receipt.source_task_incarnation == control.task_incarnation
                && receipt.is_canonical()
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_drain_deadlines_fail_before_provider_work() {
    let state = Arc::new(BarrierHoldProbeState::default());
    let mut connector = BarrierHoldProbeSource {
        state: Arc::clone(&state),
    };
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    let request = SourceDrainRequest::new(round).unwrap();
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let (command_tx, mut command_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    command_tx
        .send(Some(SourceDrainCommand::Begin {
            request: request.clone(),
            participant,
            deadline: tokio::time::Instant::now(),
        }))
        .unwrap();

    let error = apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &status_tx,
        &mut None,
        true,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ConnectorError::Internal(_)));
    assert_eq!(state.drain_begins.load(Ordering::SeqCst), 0);

    let mut active = Some(ActiveSourceDrain {
        request,
        participant,
        provider_drain: true,
        prepare_deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        ready: true,
        pending_resolution: Some(PendingSourceDrainResolution {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Abort,
            },
            deadline: tokio::time::Instant::now(),
        }),
    });
    let error = resolve_pending_source_drain(&mut connector, &status_tx, &mut active)
        .await
        .unwrap_err();
    assert!(matches!(error, ConnectorError::Internal(_)));
    assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 0);
    assert!(active.is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn blocking_finish_drain_retires_at_its_absolute_deadline() {
    let state = Arc::new(BarrierHoldProbeState::default());
    state.block_drain_finish.store(true, Ordering::Release);
    let mut connector = BarrierHoldProbeSource {
        state: Arc::clone(&state),
    };
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [10; 32],
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut active = Some(ActiveSourceDrain {
        request: SourceDrainRequest::new(round).unwrap(),
        participant: CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        },
        provider_drain: true,
        prepare_deadline: deadline,
        ready: true,
        pending_resolution: Some(PendingSourceDrainResolution {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Abort,
            },
            deadline,
        }),
    });
    let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    let mut lifecycle = SourceConnectorLifecycle::default();
    let started = tokio::time::Instant::now();

    let error = resolve_pending_source_drain_fenced(
        &mut connector,
        &status_tx,
        &mut active,
        "blocking-deadline-source",
        ConnectorCancellationPolicy::RetireConnector,
        &mut lifecycle,
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ConnectorError::Internal(_)));
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(2)
    );
    assert!(lifecycle.retired);
    assert_eq!(state.drain_finish_starts.load(Ordering::SeqCst), 1);
    assert_eq!(state.cancelled_drain_finishes.load(Ordering::SeqCst), 1);
    assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 0);
    if lifecycle.may_invoke_connector() {
        connector.close().await.unwrap();
    }
    assert_eq!(
        state.closes.load(Ordering::SeqCst),
        0,
        "a retired generation must not receive close after finish_drain cancellation"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_cancels_blocking_finish_drain_without_later_hooks() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let node_id = laminar_core::state::NodeId(41);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let authority = SourceProcessAuthority::new(Arc::clone(&controller));

    let state = Arc::new(BarrierHoldProbeState::default());
    state.block_drain_finish.store(true, Ordering::Release);
    let task_state = Arc::clone(&state);
    let task_authority = Arc::clone(&authority);
    let task = tokio::spawn(async move {
        let mut connector = BarrierHoldProbeSource { state: task_state };
        let round = AssignmentDrainId {
            predecessor_version: 8,
            target_version: 9,
            digest: [11; 32],
        };
        let mut active = Some(ActiveSourceDrain {
            request: SourceDrainRequest::new(round).unwrap(),
            participant: CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(12),
            },
            provider_drain: true,
            prepare_deadline: tokio::time::Instant::now() + Duration::from_secs(60),
            ready: true,
            pending_resolution: Some(PendingSourceDrainResolution {
                resolution: SourceDrainResolution {
                    round,
                    outcome: SourceDrainOutcome::Commit,
                },
                deadline: tokio::time::Instant::now() + Duration::from_secs(60),
            }),
        });
        let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
        let mut lifecycle = SourceConnectorLifecycle::default();
        let result = resolve_pending_source_drain_fenced(
            &mut connector,
            &status_tx,
            &mut active,
            "blocking-lease-source",
            ConnectorCancellationPolicy::RetireConnector,
            &mut lifecycle,
            Some(task_authority.as_ref()),
        )
        .await;
        if lifecycle.may_invoke_connector() {
            connector.close().await.unwrap();
        }
        (result, lifecycle)
    });

    tokio::time::timeout(
        Duration::from_secs(2),
        state.drain_finish_started.notified(),
    )
    .await
    .expect("source never entered finish_drain");
    controller.fence_process_lease();
    let (result, lifecycle) = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("lease loss did not cancel finish_drain")
        .unwrap();

    assert!(matches!(result, Err(ConnectorError::InvalidState { .. })));
    assert!(lifecycle.process_authority_lost);
    assert_eq!(state.cancelled_drain_finishes.load(Ordering::SeqCst), 1);
    assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.closes.load(Ordering::SeqCst),
        0,
        "authority loss must fence every later connector hook"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn ready_source_drain_outlives_prepare_deadline() {
    let state = Arc::new(BarrierHoldProbeState::default());
    let mut connector = BarrierHoldProbeSource {
        state: Arc::clone(&state),
    };
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let (command_tx, mut command_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    let control = SourceDrainLeaseControl {
        task_incarnation: uuid::Uuid::from_u128(101),
        command_tx,
        status_tx,
        wake: Arc::new(tokio::sync::Notify::new()),
    };
    control
        .command_tx
        .send(Some(SourceDrainCommand::Begin {
            request: SourceDrainRequest::new(round).unwrap(),
            participant,
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
        }))
        .unwrap();

    let mut active = None;
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &control.status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();
    publish_source_drain_ready(&mut connector, &control, &mut active).unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;

    control
        .command_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Commit,
            },
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
        }))
        .unwrap();
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &control.status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();

    assert!(active.is_none());
    assert_eq!(state.drain_begins.load(Ordering::SeqCst), 1);
    assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        control.status_tx.borrow().clone(),
        SourceDrainTaskStatus::Resolved {
            round: resolved,
            outcome: SourceDrainOutcome::Commit,
        } if resolved == round
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn source_drain_retries_cannot_extend_phase_deadlines() {
    let state = Arc::new(BarrierHoldProbeState::default());
    let mut connector = BarrierHoldProbeSource { state };
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    let request = SourceDrainRequest::new(round).unwrap();
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let (command_tx, mut command_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    let prepare_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    command_tx
        .send(Some(SourceDrainCommand::Begin {
            request: request.clone(),
            participant,
            deadline: prepare_deadline,
        }))
        .unwrap();
    let mut active = None;
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();
    command_tx
        .send(Some(SourceDrainCommand::Begin {
            request,
            participant,
            deadline: prepare_deadline + Duration::from_secs(10),
        }))
        .unwrap();
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();
    assert_eq!(active.as_ref().unwrap().prepare_deadline, prepare_deadline);

    let first_resolution_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let resolution = SourceDrainResolution {
        round,
        outcome: SourceDrainOutcome::Abort,
    };
    command_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution,
            deadline: first_resolution_deadline,
        }))
        .unwrap();
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();
    command_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution,
            deadline: first_resolution_deadline + Duration::from_secs(10),
        }))
        .unwrap();
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        active
            .as_ref()
            .unwrap()
            .pending_resolution
            .unwrap()
            .deadline,
        first_resolution_deadline
    );

    tokio::time::advance(Duration::from_secs(3)).await;
    let control = SourceDrainLeaseControl {
        task_incarnation: uuid::Uuid::from_u128(101),
        command_tx,
        status_tx: status_tx.clone(),
        wake: Arc::new(tokio::sync::Notify::new()),
    };
    publish_source_drain_ready(&mut connector, &control, &mut active).unwrap();
    let error = resolve_pending_source_drain(&mut connector, &status_tx, &mut active)
        .await
        .unwrap_err();
    assert!(matches!(error, ConnectorError::Internal(_)));
    assert!(active.is_some());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn dropping_process_lease_authority_aborts_its_watcher() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let node_id = laminar_core::state::NodeId(37);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let controller_weak = Arc::downgrade(&controller);
    let authority = SourceProcessAuthority::new(Arc::clone(&controller));
    let watcher = authority
        .watcher_abort
        .as_ref()
        .expect("a live authority must own its watcher")
        .clone();
    drop(controller);

    drop(authority);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !watcher.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping process authority did not terminate its watcher");
    assert!(controller_weak.upgrade().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_source_runtime_requires_installed_process_lease_authority() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let result = StreamingCoordinator::new_with_source_registry(
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
        None,
        Arc::new(parking_lot::Mutex::new(Vec::new())),
        empty_connector_task_fences(),
        crate::db::RuntimeMode::Cluster,
    )
    .await;
    let Err(error) = result else {
        panic!("cluster source runtime accepted a missing authority controller");
    };
    assert!(
        error.to_string().contains("process lease authority"),
        "{error}"
    );

    let node_id = laminar_core::state::NodeId(38);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let result = StreamingCoordinator::new_with_source_registry(
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
        Some(Arc::clone(&controller)),
        Arc::new(parking_lot::Mutex::new(Vec::new())),
        empty_connector_task_fences(),
        crate::db::RuntimeMode::Cluster,
    )
    .await;
    let Err(error) = result else {
        panic!("cluster source runtime accepted a controller without a lease deadline");
    };
    assert!(
        error.to_string().contains("shared process lease deadline"),
        "{error}"
    );

    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let result = StreamingCoordinator::new_with_source_registry(
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
        Some(controller),
        Arc::new(parking_lot::Mutex::new(Vec::new())),
        empty_connector_task_fences(),
        crate::db::RuntimeMode::Local,
    )
    .await;
    let Err(error) = result else {
        panic!("local source runtime accepted cluster process authority");
    };
    assert!(
        error.to_string().contains("local source runtime"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn expired_process_lease_rejects_source_start_without_connector_calls() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let state = Arc::new(BarrierHoldProbeState::default());
    let source = barrier_hold_probe_source("expired-process-lease-probe", Arc::clone(&state));
    let node_id = laminar_core::state::NodeId(31);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::fenced()))
        .unwrap();

    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let result = StreamingCoordinator::new_with_source_registry(
        vec![source],
        PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            checkpoint_schedule: CheckpointSchedule::Disabled,
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
        Some(controller),
        Arc::new(parking_lot::Mutex::new(Vec::new())),
        empty_connector_task_fences(),
        crate::db::RuntimeMode::Cluster,
    )
    .await;
    let Err(error) = result else {
        panic!("expired process authority unexpectedly started the source");
    };

    assert!(error.to_string().contains("process lease expired"));
    assert_eq!(state.starts.load(Ordering::SeqCst), 0);
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    assert_eq!(state.control_drives.load(Ordering::SeqCst), 0);
    assert_eq!(state.commit_notifications.load(Ordering::SeqCst), 0);
    assert_eq!(state.closes.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn lease_loss_before_activation_stops_without_later_connector_hooks() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let state = Arc::new(BarrierHoldProbeState::default());
    let source = barrier_hold_probe_source("fenced-source-task", Arc::clone(&state));
    let node_id = laminar_core::state::NodeId(32);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();

    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let mut coordinator = StreamingCoordinator::new_with_source_registry(
        vec![source],
        PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            checkpoint_schedule: CheckpointSchedule::Disabled,
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
        Some(Arc::clone(&controller)),
        Arc::new(parking_lot::Mutex::new(Vec::new())),
        empty_connector_task_fences(),
        crate::db::RuntimeMode::Cluster,
    )
    .await
    .unwrap();

    controller.fence_process_lease();
    let task = coordinator.source_handles[0].task.clone();
    coordinator.source_handles[0]
        .epoch_committed_tx
        .send(Some((1, SourceCheckpoint::new())))
        .unwrap();
    coordinator.source_handles[0]
        .startup_activation
        .take()
        .unwrap()
        .send(());
    assert!(
        task.wait_until(tokio::time::Instant::now() + Duration::from_secs(2))
            .await,
        "fenced process authority did not terminate the source task"
    );

    assert_eq!(state.starts.load(Ordering::SeqCst), 1);
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    assert_eq!(state.control_drives.load(Ordering::SeqCst), 0);
    assert_eq!(state.commit_notifications.load(Ordering::SeqCst), 0);
    assert_eq!(state.closes.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_wakes_a_source_blocked_on_the_bounded_fifo() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();
    let node_id = laminar_core::state::NodeId(34);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let authority = SourceProcessAuthority::new(Arc::clone(&controller));
    let shutdown = tokio::sync::Notify::new();
    let mut blocked = std::pin::pin!(send_source_msg(
        &tx,
        SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(2),
            cursor: SourceBatchCursor::Complete(checkpoint_at(2)),
        },
        &shutdown,
        Some(&authority),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut blocked)
            .await
            .is_err(),
        "source publication did not block on the full FIFO"
    );

    controller.fence_process_lease();
    assert!(!tokio::time::timeout(Duration::from_secs(1), blocked)
        .await
        .expect("process lease loss did not wake the blocked source publication"));
    assert!(rx.recv().await.is_ok());
    assert!(matches!(rx.try_recv(), Err(crossfire::TryRecvError::Empty)));
}

#[tokio::test]
async fn shutdown_wakes_a_source_blocked_on_the_bounded_fifo_without_cluster_authority() {
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();
    let shutdown = tokio::sync::Notify::new();
    let mut blocked = std::pin::pin!(send_source_msg(
        &tx,
        SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(2),
            cursor: SourceBatchCursor::Complete(checkpoint_at(2)),
        },
        &shutdown,
        #[cfg(feature = "cluster")]
        None,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut blocked)
            .await
            .is_err(),
        "source publication did not block on the full FIFO"
    );

    shutdown.notify_one();
    assert!(!tokio::time::timeout(Duration::from_secs(1), blocked)
        .await
        .expect("shutdown did not wake the blocked source publication"));
    assert!(rx.recv().await.is_ok());
    assert!(matches!(rx.try_recv(), Err(crossfire::TryRecvError::Empty)));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_between_drain_and_execute_prevents_cycle_publication() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        shutdown,
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let node_id = laminar_core::state::NodeId(35);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    coordinator.process_authority = Some(SourceProcessAuthority::new(Arc::clone(&controller)));

    let callback = MockCallback::new();
    *callback.process_authority_fence.lock() =
        Some((ProcessAuthorityFencePoint::Watermark, controller));
    let cycle_input_rows = Arc::clone(&callback.cycle_input_rows);
    let written_rows = Arc::clone(&callback.written_rows);
    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("process authority loss did not stop the drained cycle");
    assert!(matches!(exit, ExitReason::Fault(ref reason)
        if reason.contains("cluster process lease expired before operator execution")));
    assert!(cycle_input_rows.lock().is_empty());
    assert_eq!(written_rows.load(Ordering::Acquire), 0);
    drop(tx);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn ready_global_source_drain_holds_polling_but_still_emits_barriers() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let state = Arc::new(BarrierHoldProbeState::default());
    let source = barrier_hold_probe_source("global-drain-probe", Arc::clone(&state));
    let node_id = laminar_core::state::NodeId(36);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let mut coordinator = StreamingCoordinator::new_with_source_registry(
        vec![source],
        PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            checkpoint_schedule: CheckpointSchedule::Disabled,
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
        Some(controller),
        Arc::new(parking_lot::Mutex::new(Vec::new())),
        empty_connector_task_fences(),
        crate::db::RuntimeMode::Cluster,
    )
    .await
    .unwrap();

    let task = coordinator.source_handles[0].task.clone();
    let drain = task
        .drain_control()
        .expect("every cluster source has drain control");
    let barrier_control = coordinator.source_handles[0].barrier_control();
    let barrier_injector = coordinator.source_handles[0].barrier_injector.clone();
    coordinator.source_handles[0]
        .startup_activation
        .take()
        .unwrap()
        .send(());
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.polls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source was not activated");

    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let mut status_rx = drain.status_tx.subscribe();
    drain
        .command_tx
        .send(Some(SourceDrainCommand::Begin {
            request: SourceDrainRequest::new(round).unwrap(),
            participant,
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    drain.wake.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                status_rx.borrow_and_update().clone(),
                SourceDrainTaskStatus::Ready(receipt) if receipt.round == round
            ) {
                break;
            }
            status_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("source did not publish its global cut");

    let polls_at_cut = state.polls.load(Ordering::SeqCst);
    let control_at_cut = state.control_drives.load(Ordering::SeqCst);
    for _ in 0..4 {
        state.data_ready.notify_one();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(
        state.polls.load(Ordering::SeqCst),
        polls_at_cut,
        "source polled data after publishing its global cut"
    );
    assert!(
        state.control_drives.load(Ordering::SeqCst) > control_at_cut,
        "held source stopped servicing its control plane"
    );

    let barrier = CheckpointBarrier::new(8, 8);
    assert!(barrier_injector.trigger(barrier));
    drain.wake.notify_one();
    let received = tokio::time::timeout(Duration::from_secs(2), coordinator.rx.recv())
        .await
        .expect("held source did not emit the checkpoint barrier")
        .unwrap();
    assert!(matches!(received, SourceMsg::Barrier { barrier: seen, .. } if seen == barrier));
    assert_eq!(state.polls.load(Ordering::SeqCst), polls_at_cut);

    drain
        .command_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Abort,
            },
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    drain.wake.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                status_rx.borrow_and_update().clone(),
                SourceDrainTaskStatus::Resolved {
                    round: resolved,
                    outcome: SourceDrainOutcome::Abort,
                } if resolved == round
            ) {
                break;
            }
            status_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("source did not resolve the global cut");

    for _ in 0..4 {
        state.data_ready.notify_one();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(
        state.polls.load(Ordering::SeqCst),
        polls_at_cut,
        "resolving the global drain resumed polling across a held barrier"
    );

    barrier_control.release_exact(CheckpointAttempt::new(8, 8));
    state.data_ready.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.polls.load(Ordering::SeqCst) == polls_at_cut {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source did not resume polling after the held barrier was released");

    task.mark_expected_shutdown();
    barrier_control.stop_hold();
    task.notify_shutdown();
    let handle = coordinator.source_handles.pop().unwrap();
    drop(handle.epoch_committed_tx);
    drop(handle.barrier_release_tx);
    assert!(
        task.wait_until(tokio::time::Instant::now() + Duration::from_secs(2))
            .await,
        "source task did not stop"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn barrier_hold_retains_unobserved_begin_until_abort_overwrites_it() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let state = Arc::new(BarrierHoldProbeState::default());
    let source = barrier_hold_probe_source("barrier-first-drain-probe", Arc::clone(&state));
    let node_id = laminar_core::state::NodeId(37);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let mut coordinator = StreamingCoordinator::new_with_source_registry(
        vec![source],
        PipelineConfig {
            fallback_poll_interval: Duration::from_secs(60),
            checkpoint_schedule: CheckpointSchedule::Disabled,
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
        Some(controller),
        Arc::new(parking_lot::Mutex::new(Vec::new())),
        empty_connector_task_fences(),
        crate::db::RuntimeMode::Cluster,
    )
    .await
    .unwrap();

    let task = coordinator.source_handles[0].task.clone();
    let drain = task
        .drain_control()
        .expect("every cluster source has drain control");
    let barrier_control = coordinator.source_handles[0].barrier_control();
    let barrier_injector = coordinator.source_handles[0].barrier_injector.clone();
    coordinator.source_handles[0]
        .startup_activation
        .take()
        .unwrap()
        .send(());
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.polls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source was not activated");

    let barrier = CheckpointBarrier::new(9, 9);
    assert!(barrier_injector.trigger(barrier));
    drain.wake.notify_one();
    let received = tokio::time::timeout(Duration::from_secs(2), coordinator.rx.recv())
        .await
        .expect("source did not emit the checkpoint barrier")
        .unwrap();
    assert!(matches!(received, SourceMsg::Barrier { barrier: seen, .. } if seen == barrier));
    let polls_at_barrier = state.polls.load(Ordering::SeqCst);

    // Synchronize with the first turn inside the nested barrier hold before injecting Begin.
    let control_at_barrier = state.control_drives.load(Ordering::SeqCst);
    drain.wake.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.control_drives.load(Ordering::SeqCst) == control_at_barrier {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source did not enter the barrier hold");

    let round = AssignmentDrainId {
        predecessor_version: 8,
        target_version: 9,
        digest: [10; 32],
    };
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(12),
    };
    let mut status_rx = drain.status_tx.subscribe();
    let control_before_begin = state.control_drives.load(Ordering::SeqCst);
    drain
        .command_tx
        .send(Some(SourceDrainCommand::Begin {
            request: SourceDrainRequest::new(round).unwrap(),
            participant,
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    drain.wake.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.control_drives.load(Ordering::SeqCst) == control_before_begin {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("barrier-held source did not inspect the drain command");
    assert!(matches!(
        status_rx.borrow_and_update().clone(),
        SourceDrainTaskStatus::Idle
    ));

    drain
        .command_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Abort,
            },
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    drain.wake.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                status_rx.borrow_and_update().clone(),
                SourceDrainTaskStatus::Resolved {
                    round: resolved,
                    outcome: SourceDrainOutcome::Abort,
                } if resolved == round
            ) {
                break;
            }
            status_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("overwriting Abort did not resolve the unobserved drain Begin");
    assert_eq!(
        state.polls.load(Ordering::SeqCst),
        polls_at_barrier,
        "drain command handling polled across the held barrier"
    );

    barrier_control.release_exact(CheckpointAttempt::new(9, 9));
    state.data_ready.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.polls.load(Ordering::SeqCst) == polls_at_barrier {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source did not resume polling after the held barrier was released");

    task.mark_expected_shutdown();
    barrier_control.stop_hold();
    task.notify_shutdown();
    let handle = coordinator.source_handles.pop().unwrap();
    drop(handle.epoch_committed_tx);
    drop(handle.barrier_release_tx);
    assert!(
        task.wait_until(tokio::time::Instant::now() + Duration::from_secs(2))
            .await,
        "source task did not stop"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn local_runtime_rejects_assignment_scoped_sources() {
    let state = Arc::new(BarrierHoldProbeState::default());
    let source = SourceRegistration {
        name: "local-drain-probe".into(),
        connector: Box::new(BarrierHoldProbeSource {
            state: Arc::clone(&state),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("local-drain-probe"),
        assignment_scoped: true,
        position: SourcePosition::Initial,
    };
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let result = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            checkpoint_schedule: CheckpointSchedule::Disabled,
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("local runtime accepted an assignment-scoped source"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("database-owned cluster runtime"));
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    assert_eq!(state.drain_begins.load(Ordering::SeqCst), 0);
    assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn idle_source_drain_resolution_accepts_only_a_reconciled_replacement_commit() {
    let state = Arc::new(BarrierHoldProbeState::default());
    let mut connector = BarrierHoldProbeSource {
        state: Arc::clone(&state),
    };
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    let (command_tx, mut command_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    command_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Abort,
            },
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();

    let mut active = None;
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();

    assert!(active.is_none());
    assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 0);
    assert!(matches!(
        status_tx.borrow().clone(),
        SourceDrainTaskStatus::Resolved {
            round: resolved,
            outcome: SourceDrainOutcome::Abort,
        } if resolved == round
    ));

    let commit_state = Arc::new(BarrierRetrySourceState::default());
    commit_state.allow_capture.store(true, Ordering::Release);
    commit_state
        .block_checkpoint_ready
        .store(true, Ordering::Release);
    commit_state
        .assignment_version
        .store(round.target_version, Ordering::Release);
    let mut commit_connector = BarrierRetrySource {
        state: Arc::clone(&commit_state),
    };
    let (commit_tx, mut commit_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (commit_status_tx, _commit_status_rx) =
        tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    commit_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Commit,
            },
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    let mut commit_active = None;
    apply_latest_source_drain_command(
        &mut commit_connector,
        &mut commit_rx,
        &commit_status_tx,
        &mut commit_active,
        true,
    )
    .await
    .unwrap();
    assert!(matches!(
        commit_status_tx.borrow().clone(),
        SourceDrainTaskStatus::Idle
    ));
    assert!(commit_rx.has_changed().unwrap());
    assert_eq!(commit_state.capture_attempts.load(Ordering::Acquire), 0);

    commit_state
        .block_checkpoint_ready
        .store(false, Ordering::Release);
    apply_latest_source_drain_command(
        &mut commit_connector,
        &mut commit_rx,
        &commit_status_tx,
        &mut commit_active,
        true,
    )
    .await
    .unwrap();
    assert!(matches!(
        commit_status_tx.borrow().clone(),
        SourceDrainTaskStatus::Resolved {
            round: resolved,
            outcome: SourceDrainOutcome::Commit,
        } if resolved == round
    ));

    commit_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Commit,
            },
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    apply_latest_source_drain_command(
        &mut commit_connector,
        &mut commit_rx,
        &commit_status_tx,
        &mut commit_active,
        true,
    )
    .await
    .unwrap();

    let wrong_state = Arc::new(BarrierRetrySourceState::default());
    wrong_state.allow_capture.store(true, Ordering::Release);
    wrong_state
        .assignment_version
        .store(round.predecessor_version, Ordering::Release);
    let mut wrong_connector = BarrierRetrySource { state: wrong_state };
    let (wrong_tx, mut wrong_rx) = tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (wrong_status_tx, _wrong_status_rx) =
        tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    wrong_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Commit,
            },
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    let error = apply_latest_source_drain_command(
        &mut wrong_connector,
        &mut wrong_rx,
        &wrong_status_tx,
        &mut None,
        true,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ConnectorError::InvalidState { .. }));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn finished_source_retains_its_exact_resolved_drain_terminal() {
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [29; 32],
    };
    let resolution = SourceDrainResolution {
        round,
        outcome: SourceDrainOutcome::Commit,
    };
    let tasks: OwnedSourceTasks = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let task = install_replacement_source_drain_task_for_test(&tasks, "resolved-source");

    resolve_owned_source_drain(
        &tasks,
        resolution,
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(owned_source_drain_resolved(&tasks, resolution).unwrap());

    task.request_shutdown();
    assert!(
        task.wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
    assert!(task.is_finished());
    assert!(
        owned_source_drain_resolved(&tasks, resolution).unwrap(),
        "joining a task must not erase its exact retained drain terminal"
    );
    assert!(
        !owned_source_drain_resolved(
            &tasks,
            SourceDrainResolution {
                round: AssignmentDrainId {
                    predecessor_version: 8,
                    target_version: 9,
                    digest: [30; 32],
                },
                outcome: SourceDrainOutcome::Commit,
            },
        )
        .unwrap(),
        "a finished task must not satisfy a different committed drain"
    );

    let unresolved: OwnedSourceTasks = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let unresolved_task =
        install_replacement_source_drain_task_for_test(&unresolved, "unresolved-source");
    unresolved_task.request_shutdown();
    assert!(
        unresolved_task
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
    assert!(
        !owned_source_drain_resolved(&unresolved, resolution).unwrap(),
        "an exited task without the exact retained terminal must stay fail-closed"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn active_source_drain_abort_waits_for_the_fifo_cut() {
    let state = Arc::new(BarrierHoldProbeState::default());
    let mut connector = BarrierHoldProbeSource {
        state: Arc::clone(&state),
    };
    let round = AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    let request = SourceDrainRequest::new(round).unwrap();
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let (command_tx, mut command_rx) =
        tokio::sync::watch::channel::<Option<SourceDrainCommand>>(None);
    let (status_tx, _status_rx) = tokio::sync::watch::channel(SourceDrainTaskStatus::Idle);
    command_tx
        .send(Some(SourceDrainCommand::Begin {
            request,
            participant,
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    let mut active = None;
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();
    command_tx
        .send(Some(SourceDrainCommand::Resolve {
            resolution: SourceDrainResolution {
                round,
                outcome: SourceDrainOutcome::Abort,
            },
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }))
        .unwrap();
    apply_latest_source_drain_command(
        &mut connector,
        &mut command_rx,
        &status_tx,
        &mut active,
        true,
    )
    .await
    .unwrap();

    resolve_pending_source_drain(&mut connector, &status_tx, &mut active)
        .await
        .unwrap();
    assert!(active.is_some(), "abort must not resolve before Ready");
    assert!(matches!(
        status_tx.borrow().clone(),
        SourceDrainTaskStatus::Pausing(active_round) if active_round == round
    ));

    let control = SourceDrainLeaseControl {
        task_incarnation: uuid::Uuid::from_u128(101),
        command_tx,
        status_tx,
        wake: Arc::new(tokio::sync::Notify::new()),
    };
    publish_source_drain_ready(&mut connector, &control, &mut active).unwrap();
    resolve_pending_source_drain(&mut connector, &control.status_tx, &mut active)
        .await
        .unwrap();
    assert!(active.is_none());
    assert_eq!(state.drain_finishes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        control.status_tx.borrow().clone(),
        SourceDrainTaskStatus::Resolved {
            round: resolved,
            outcome: SourceDrainOutcome::Abort,
        } if resolved == round
    ));
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SourceConnector for BarrierRetrySource {
    fn contract(
        &self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<laminar_connectors::connector::SourceContract, ConnectorError> {
        Ok(replayable_append_only_source_contract())
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.state.polls.fetch_add(1, Ordering::SeqCst);
        if self.state.emit_batch.swap(false, Ordering::AcqRel) {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
                .unwrap();
            Ok(Some(SourceBatch::new(batch)))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        self.state.capture_attempts.fetch_add(1, Ordering::SeqCst);
        if !self.state.allow_capture.load(Ordering::Acquire) {
            return Ok(None);
        }
        self.state
            .successful_captures
            .fetch_add(1, Ordering::SeqCst);
        let mut checkpoint = SourceCheckpoint::new();
        if let Some(version) =
            std::num::NonZeroU64::new(self.state.assignment_version.load(Ordering::Acquire))
        {
            checkpoint.bind_assignment_version(version);
        }
        Ok(Some(checkpoint))
    }

    fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
        Ok(!self.state.block_checkpoint_ready.load(Ordering::Acquire))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SourceConnector for StartupSource {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        self.cancellation_policy
    }

    fn contract(
        &self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<laminar_connectors::connector::SourceContract, ConnectorError> {
        Ok(self.contract)
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        self.state.open_calls.fetch_add(1, Ordering::SeqCst);
        // Model a connector that acquired resources inside the atomic startup operation
        // before discovering that startup failed. The coordinator must still close it.
        self.state.open.store(true, Ordering::SeqCst);
        if !self.start_delay.is_zero() {
            tokio::time::sleep(self.start_delay).await;
        }
        if self.fail_open {
            return Err(ConnectorError::ConnectionFailed(
                "injected open failure".into(),
            ));
        }

        self.state.start_completions.fetch_add(1, Ordering::SeqCst);

        if matches!(request.into_parts().1, SourcePosition::Resume { .. }) {
            self.state.restore_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_restore {
                return Err(ConnectorError::Internal(
                    "injected resume-position failure".into(),
                ));
            }
        }

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.state.poll_calls.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.state.close_calls.fetch_add(1, Ordering::SeqCst);
        if !self.close_delay.is_zero() {
            tokio::time::sleep(self.close_delay).await;
        }
        self.state.open.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait::async_trait]
impl SourceConnector for LateBoundMutationSource {
    fn contract(
        &self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<laminar_connectors::connector::SourceContract, ConnectorError> {
        Ok(replayable_append_only_source_contract())
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.started = true;
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        unreachable!("late-bound mutation source must fail admission before polling")
    }

    fn schema(&self) -> Arc<Schema> {
        if self.started {
            Arc::new(Schema::new(vec![Field::new(
                "__weight",
                DataType::Int64,
                false,
            )]))
        } else {
            Arc::new(Schema::empty())
        }
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl SourceConnector for TrackedStartupSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        self.tracker_calls.fetch_add(1, Ordering::SeqCst);
        Some(self.task_tracker.clone())
    }

    fn contract(
        &self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<laminar_connectors::connector::SourceContract, ConnectorError> {
        Ok(replayable_append_only_source_contract())
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        self.start_error.take().map_or(Ok(()), Err)
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        unreachable!("a source whose start failed must never be polled")
    }

    fn schema(&self) -> Arc<Schema> {
        test_source_schema()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum RuntimeSourceFailure {
    TerminalPoll,
    SchemaMismatch,
    CommitNotification,
    Panic,
}

#[derive(Default)]
struct RuntimeSourceState {
    polls: AtomicU64,
    commit_notifications: AtomicU64,
    closes: AtomicU64,
}

struct RuntimeFailureSource {
    state: Arc<RuntimeSourceState>,
    failure: RuntimeSourceFailure,
}

#[derive(Default)]
struct PendingCheckpointFailureState {
    polls: AtomicU64,
    checkpoint_captures: AtomicU64,
    commit_notifications: AtomicU64,
    closes: AtomicU64,
}

struct PendingCheckpointFailureSource {
    state: Arc<PendingCheckpointFailureState>,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SourceConnector for PendingCheckpointFailureSource {
    fn contract(
        &self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<laminar_connectors::connector::SourceContract, ConnectorError> {
        Ok(replayable_append_only_source_contract())
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let poll = self.state.polls.fetch_add(1, Ordering::SeqCst);
        if poll != 0 {
            return Ok(None);
        }
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        Ok(Some(SourceBatch::new(batch)))
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        let capture = self
            .state
            .checkpoint_captures
            .fetch_add(1, Ordering::SeqCst);
        if capture == 0 {
            Ok(None)
        } else {
            Err(ConnectorError::Internal(
                "injected pending checkpoint failure".into(),
            ))
        }
    }

    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        _checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.state
            .commit_notifications
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.state.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct CancellationSafePollState {
    poll_calls: AtomicU64,
    cancelled_polls: AtomicU64,
    commit_notification_calls: AtomicU64,
    closes: AtomicU64,
    first_poll_started: tokio::sync::Notify,
    release_first_poll: tokio::sync::Notify,
}

struct PollCancellationGuard {
    state: Arc<CancellationSafePollState>,
    completed: bool,
}

impl Drop for PollCancellationGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.state.cancelled_polls.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct CancellationSafePollSource {
    state: Arc<CancellationSafePollState>,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SourceConnector for CancellationSafePollSource {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    fn contract(
        &self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<laminar_connectors::connector::SourceContract, ConnectorError> {
        Ok(replayable_append_only_source_contract())
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let call = self.state.poll_calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            let mut guard = PollCancellationGuard {
                state: Arc::clone(&self.state),
                completed: false,
            };
            self.state.first_poll_started.notify_one();
            self.state.release_first_poll.notified().await;
            guard.completed = true;
        }
        Ok(None)
    }

    fn schema(&self) -> Arc<Schema> {
        test_source_schema()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        _checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.state
            .commit_notification_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.state.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SourceConnector for RuntimeFailureSource {
    fn contract(
        &self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<laminar_connectors::connector::SourceContract, ConnectorError> {
        laminar_connectors::generator::GeneratorSource::default().contract(
            &laminar_connectors::config::ConnectorConfig::new("generator"),
        )
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.state.polls.fetch_add(1, Ordering::SeqCst);
        match self.failure {
            RuntimeSourceFailure::TerminalPoll => Err(ConnectorError::Internal(
                "injected terminal poll failure".into(),
            )),
            RuntimeSourceFailure::SchemaMismatch => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "actual",
                    DataType::Int64,
                    false,
                )]));
                let batch =
                    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
                        .unwrap();
                Ok(Some(SourceBatch::new(batch)))
            }
            RuntimeSourceFailure::CommitNotification => Ok(None),
            RuntimeSourceFailure::Panic => panic!("injected source-task panic"),
        }
    }

    fn schema(&self) -> Arc<Schema> {
        match self.failure {
            RuntimeSourceFailure::SchemaMismatch => Arc::new(Schema::new(vec![Field::new(
                "expected",
                DataType::Int64,
                false,
            )])),
            RuntimeSourceFailure::TerminalPoll
            | RuntimeSourceFailure::CommitNotification
            | RuntimeSourceFailure::Panic => test_source_schema(),
        }
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    async fn notify_epoch_committed(
        &mut self,
        _epoch: u64,
        _checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.state
            .commit_notifications
            .fetch_add(1, Ordering::SeqCst);
        match self.failure {
            RuntimeSourceFailure::CommitNotification => Err(ConnectorError::Internal(
                "injected commit notification failure".into(),
            )),
            RuntimeSourceFailure::TerminalPoll
            | RuntimeSourceFailure::SchemaMismatch
            | RuntimeSourceFailure::Panic => Ok(()),
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.state.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn runtime_failure_coordinator(
    delivery_guarantee: DeliveryGuarantee,
    failure: RuntimeSourceFailure,
    state: Arc<RuntimeSourceState>,
    shutdown: Arc<tokio::sync::Notify>,
) -> StreamingCoordinator {
    let source = SourceRegistration {
        name: "runtime-failure-source".into(),
        connector: Box::new(RuntimeFailureSource { state, failure }),
        config: laminar_connectors::config::ConnectorConfig::new("runtime-failure-test"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let config = PipelineConfig {
        delivery_guarantee,
        checkpoint_schedule: CheckpointSchedule::Periodic(Duration::from_secs(60)),
        fallback_poll_interval: Duration::from_millis(1),
        ..PipelineConfig::default()
    };

    StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        config,
        shutdown,
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("runtime failure source must start")
}

async fn shut_down_after_observed(counter: &AtomicU64, shutdown: &tokio::sync::Notify) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while counter.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("injected source failure was not observed");
    shutdown.notify_one();
}

fn startup_source(
    name: &str,
    state: Arc<StartupSourceState>,
    fail_open: bool,
    fail_restore: bool,
    position: SourcePosition,
) -> SourceRegistration {
    startup_source_with_delay(
        name,
        state,
        fail_open,
        fail_restore,
        Duration::ZERO,
        position,
    )
}

fn startup_source_with_delay(
    name: &str,
    state: Arc<StartupSourceState>,
    fail_open: bool,
    fail_restore: bool,
    start_delay: Duration,
    position: SourcePosition,
) -> SourceRegistration {
    startup_source_with_policy(
        name,
        state,
        fail_open,
        fail_restore,
        start_delay,
        position,
        ConnectorCancellationPolicy::CancelSafe,
    )
}

fn startup_source_with_policy(
    name: &str,
    state: Arc<StartupSourceState>,
    fail_open: bool,
    fail_restore: bool,
    start_delay: Duration,
    position: SourcePosition,
    cancellation_policy: ConnectorCancellationPolicy,
) -> SourceRegistration {
    startup_source_with_close_delay(
        name,
        state,
        fail_open,
        fail_restore,
        start_delay,
        Duration::ZERO,
        position,
        cancellation_policy,
    )
}

fn startup_source_with_close_delay(
    name: &str,
    state: Arc<StartupSourceState>,
    fail_open: bool,
    fail_restore: bool,
    start_delay: Duration,
    close_delay: Duration,
    position: SourcePosition,
    cancellation_policy: ConnectorCancellationPolicy,
) -> SourceRegistration {
    startup_source_with_close_delay_and_contract(
        name,
        state,
        fail_open,
        fail_restore,
        start_delay,
        close_delay,
        position,
        cancellation_policy,
        replayable_append_only_source_contract(),
    )
}

#[allow(clippy::too_many_arguments)]
fn startup_source_with_close_delay_and_contract(
    name: &str,
    state: Arc<StartupSourceState>,
    fail_open: bool,
    fail_restore: bool,
    start_delay: Duration,
    close_delay: Duration,
    position: SourcePosition,
    cancellation_policy: ConnectorCancellationPolicy,
    contract: laminar_connectors::connector::SourceContract,
) -> SourceRegistration {
    SourceRegistration {
        name: name.into(),
        connector: Box::new(StartupSource {
            state,
            schema: test_source_schema(),
            start_delay,
            close_delay,
            fail_open,
            fail_restore,
            cancellation_policy,
            contract,
        }),
        config: laminar_connectors::config::ConnectorConfig::new("startup-test"),
        assignment_scoped: false,
        position,
    }
}

async fn startup_result(sources: Vec<SourceRegistration>) -> Result<StreamingCoordinator, DbError> {
    startup_result_with_config(sources, PipelineConfig::default()).await
}

async fn startup_result_with_config(
    sources: Vec<SourceRegistration>,
    config: PipelineConfig,
) -> Result<StreamingCoordinator, DbError> {
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        sources,
        config,
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
}

#[tokio::test]
async fn uncertified_exact_source_is_rejected_before_start() {
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source(
        "uncertified-exact-source",
        Arc::clone(&state),
        false,
        false,
        SourcePosition::Initial,
    );
    let result = startup_result_with_config(
        vec![source],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
            checkpoint_schedule: CheckpointSchedule::Periodic(Duration::from_secs(60)),
            ..PipelineConfig::default()
        },
    )
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("an uncertified exact source must fail before connector startup"),
    };

    assert!(
        error
            .to_string()
            .contains(laminar_core::error_codes::EXACTLY_ONCE_SOURCE_UNCERTIFIED),
        "unexpected error: {error}"
    );
    assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn public_coordinator_uses_connector_contract_for_mutation_admission() {
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source_with_close_delay_and_contract(
        "mutation-source",
        Arc::clone(&state),
        false,
        false,
        Duration::ZERO,
        Duration::ZERO,
        SourcePosition::Initial,
        ConnectorCancellationPolicy::CancelSafe,
        laminar_connectors::connector::SourceContract::new(
            laminar_connectors::connector::SourceConsistency::Replayable,
            laminar_connectors::connector::SourceTopology::Singleton,
            laminar_connectors::connector::SourceInputMode::FullChangelog,
        ),
    );

    let error = match startup_result(vec![source]).await {
        Err(error) => error,
        Ok(_) => panic!("the public coordinator must reject the connector's mutation contract"),
    };

    assert!(
        error
            .to_string()
            .contains(laminar_core::error_codes::SOURCE_MUTATION_NOT_ADMITTED),
        "unexpected error: {error}"
    );
    assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn public_coordinator_rejects_late_bound_schema_before_start() {
    let starts = Arc::new(AtomicU64::new(0));
    let source = SourceRegistration {
        name: "late-bound-mutation-source".into(),
        connector: Box::new(LateBoundMutationSource {
            starts: Arc::clone(&starts),
            started: false,
        }),
        config: laminar_connectors::config::ConnectorConfig::new("late-bound-mutation-test"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };

    let error = match startup_result(vec![source]).await {
        Err(error) => error,
        Ok(_) => panic!("the public coordinator must reject a late-bound source schema"),
    };

    assert!(
        error.to_string().contains("non-empty schema before"),
        "{error}"
    );
    assert_eq!(starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn public_coordinator_rejects_commit_coupled_best_effort_before_start() {
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source_with_close_delay_and_contract(
        "commit-coupled-best-effort",
        Arc::clone(&state),
        false,
        false,
        Duration::ZERO,
        Duration::ZERO,
        SourcePosition::Initial,
        ConnectorCancellationPolicy::CancelSafe,
        laminar_connectors::connector::SourceContract::new(
            laminar_connectors::connector::SourceConsistency::CommitCoupled,
            laminar_connectors::connector::SourceTopology::Singleton,
            laminar_connectors::connector::SourceInputMode::AppendOnly,
        ),
    );
    let result = startup_result_with_config(
        vec![source],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::BestEffort,
            checkpoint_schedule: CheckpointSchedule::Manual,
            ..PipelineConfig::default()
        },
    )
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("best-effort must reject a commit-coupled source"),
    };

    assert!(error.to_string().contains("only at-least-once"), "{error}");
    assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn at_least_once_manual_only_checkpointing_is_admitted() {
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source_with_close_delay_and_contract(
        "manual-only-at-least-once",
        Arc::clone(&state),
        false,
        false,
        Duration::ZERO,
        Duration::ZERO,
        SourcePosition::Initial,
        ConnectorCancellationPolicy::CancelSafe,
        laminar_connectors::connector::SourceContract::new(
            laminar_connectors::connector::SourceConsistency::CommitCoupled,
            laminar_connectors::connector::SourceTopology::Singleton,
            laminar_connectors::connector::SourceInputMode::AppendOnly,
        ),
    );
    let coordinator = startup_result_with_config(
        vec![source],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_schedule: CheckpointSchedule::Manual,
            ..PipelineConfig::default()
        },
    )
    .await
    .expect("manual-only checkpointing must satisfy at-least-once admission");

    assert_eq!(
        coordinator.config.checkpoint_schedule,
        CheckpointSchedule::Manual
    );
    assert_eq!(state.open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 1);
    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
    drop(coordinator);
}

#[tokio::test]
async fn at_least_once_without_checkpointing_is_rejected_before_start() {
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source(
        "disabled-at-least-once",
        Arc::clone(&state),
        false,
        false,
        SourcePosition::Initial,
    );
    let result = startup_result_with_config(
        vec![source],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_schedule: CheckpointSchedule::Disabled,
            ..PipelineConfig::default()
        },
    )
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("at-least-once must reject disabled checkpointing"),
    };

    assert!(error.to_string().contains("[LDB-5032]"), "{error}");
    assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn zero_periodic_checkpoint_interval_is_rejected_before_start() {
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source(
        "zero-checkpoint-interval",
        Arc::clone(&state),
        false,
        false,
        SourcePosition::Initial,
    );
    let result = startup_result_with_config(
        vec![source],
        PipelineConfig {
            checkpoint_schedule: CheckpointSchedule::Periodic(Duration::ZERO),
            ..PipelineConfig::default()
        },
    )
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("a zero periodic checkpoint interval must be rejected"),
    };

    assert!(error.to_string().contains("greater than zero"), "{error}");
    assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_io_waits_for_the_runtime_ready_boundary() {
    let state = Arc::new(StartupSourceState::default());
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let source = startup_source(
        "activation-fenced",
        Arc::clone(&state),
        false,
        false,
        SourcePosition::Initial,
    );
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        PipelineConfig::default(),
        Arc::clone(&shutdown),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        state.poll_calls.load(Ordering::SeqCst),
        0,
        "source polled before the compute loop published readiness"
    );

    let callback = MockCallback::new();
    let installed = Arc::clone(&callback.barrier_control_installed);
    let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
    let run = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
    ready_rx
        .await
        .expect("coordinator retained readiness sender")
        .expect("coordinator entered its control loop");
    assert!(installed.load(Ordering::Acquire));
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.poll_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source was not activated after readiness");

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
    assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_runtime_generation_fails_readiness_without_source_activation() {
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source(
        "cancelled-before-ready",
        Arc::clone(&state),
        false,
        false,
        SourcePosition::Initial,
    );
    let coordinator = startup_result(vec![source]).await.unwrap();
    let terminal_shutdown = tokio_util::sync::CancellationToken::new();
    terminal_shutdown.cancel();
    let coordinator = coordinator.with_terminal_shutdown(terminal_shutdown);
    let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();

    let run = tokio::spawn(async move {
        coordinator
            .run_with_ready(MockCallback::new(), ready_tx)
            .await
    });
    let (readiness, exit) = tokio::time::timeout(Duration::from_secs(1), async {
        let readiness = ready_rx
            .await
            .expect("coordinator retained readiness sender")
            .expect_err("a cancelled runtime generation must not publish readiness");
        (readiness, run.await.unwrap())
    })
    .await
    .expect("pre-activation shutdown exceeded the sub-second latency bound");

    assert!(readiness.contains("cancelled before readiness"));
    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn claimed_barrier_is_retained_while_source_cursor_is_unreconciled() {
    let state = Arc::new(BarrierRetrySourceState::default());
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let source = SourceRegistration {
        name: "barrier-retry".into(),
        connector: Box::new(BarrierRetrySource {
            state: Arc::clone(&state),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("barrier-retry"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let config = PipelineConfig {
        fallback_poll_interval: Duration::from_millis(1),
        checkpoint_schedule: CheckpointSchedule::Disabled,
        ..PipelineConfig::default()
    };
    let coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        config,
        Arc::clone(&shutdown),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    let injector = coordinator.source_handles[0].barrier_injector.clone();
    let callback = MockCallback::new();
    let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
    let run = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
    ready_rx.await.unwrap().unwrap();

    assert!(injector.trigger(CheckpointBarrier::new(77, 77)));
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.capture_attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source did not claim the injected barrier");
    let polls_after_claim = state.polls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        state.polls.load(Ordering::SeqCst),
        polls_after_claim,
        "source polled data after claiming an unreconciled barrier"
    );

    state.allow_capture.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.successful_captures.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retained barrier was not retried after reconciliation");
    assert!(state.capture_attempts.load(Ordering::SeqCst) >= 2);

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

#[test]
fn source_checkpoint_scope_is_validated_before_publication() {
    let state = Arc::new(BarrierRetrySourceState::default());
    state.allow_capture.store(true, Ordering::Release);
    let source = BarrierRetrySource {
        state: Arc::clone(&state),
    };

    assert!(try_source_checkpoint(&source, false).unwrap().is_some());
    let error = try_source_checkpoint(&source, true).unwrap_err();
    assert!(error.to_string().contains("missing its assignment version"));

    state.assignment_version.store(7, Ordering::Release);
    assert_eq!(
        try_source_checkpoint(&source, true)
            .unwrap()
            .unwrap()
            .assignment_version()
            .map(std::num::NonZeroU64::get),
        Some(7)
    );
    let error = try_source_checkpoint(&source, false).unwrap_err();
    assert!(error
        .to_string()
        .contains("unexpectedly carries cluster assignment version 7"));
}

#[test]
fn assignment_scoped_batch_uses_its_bound_cursor_and_missing_fails_closed() {
    let mut batch = SourceBatch::new(RecordBatch::new_empty(test_source_schema()));
    let error = match take_assignment_bound_batch_cursor(&mut batch, true) {
        Err(error) => error,
        Ok(_) => panic!("missing assignment cursor was accepted"),
    };
    assert!(error
        .to_string()
        .contains("missing its assignment-bound checkpoint"));

    let mut expected = SourceCheckpoint::new();
    expected.bind_assignment_version(std::num::NonZeroU64::new(7).unwrap());
    expected
        .set_input_channels(vec![b"old-partition".to_vec()])
        .unwrap();
    let mut batch = SourceBatch::new(RecordBatch::new_empty(test_source_schema()))
        .with_checkpoint(expected.clone());

    match take_assignment_bound_batch_cursor(&mut batch, true).unwrap() {
        Some(SourceBatchCursor::Complete(checkpoint)) => {
            assert_eq!(checkpoint, expected);
        }
        _ => panic!("expected a complete assignment cursor"),
    }

    let channels: Arc<[Vec<u8>]> = Arc::from([b"old-partition".to_vec()]);
    let delta = SourceCheckpointDelta::new(
        std::num::NonZeroU64::new(7).unwrap(),
        channels,
        std::collections::HashMap::from([("topic:0".into(), Some("11".into()))]),
    )
    .unwrap();
    let mut batch = SourceBatch::new(RecordBatch::new_empty(test_source_schema()))
        .with_checkpoint_delta(delta.clone());
    match take_assignment_bound_batch_cursor(&mut batch, true).unwrap() {
        Some(SourceBatchCursor::Incremental(actual)) => assert_eq!(actual, delta),
        _ => panic!("expected an incremental assignment cursor"),
    }
}

#[test]
fn incremental_assignment_cursor_preserves_complete_cut_and_rotation_replaces_it() {
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::ExactlyOnce,
        None,
    );
    let channels: Arc<[Vec<u8>]> = Arc::from([b"topic:0".to_vec(), b"topic:1".to_vec()]);
    let mut committed = SourceCheckpoint::new();
    committed.set_offset("topic:0", "10");
    committed.set_offset("@laminar.kafka.next.v1:topic:0", "1");
    committed.set_offset("@laminar.kafka.next.v1:topic:1", "5");
    committed.set_metadata("owner", "node-a");
    committed.set_input_channels(Arc::clone(&channels)).unwrap();
    committed.bind_assignment_version(std::num::NonZeroU64::new(7).unwrap());
    coordinator.committed_offsets[0] = Some(committed);

    let delta = SourceCheckpointDelta::new(
        std::num::NonZeroU64::new(7).unwrap(),
        Arc::clone(&channels),
        std::collections::HashMap::from([
            ("topic:0".into(), Some("11".into())),
            ("@laminar.kafka.next.v1:topic:0".into(), None),
        ]),
    )
    .unwrap();
    let mut callback = MockCallback::new();
    let mut events = 0;
    coordinator
        .stage_batch(
            0,
            &int_batch(1),
            SourceBatchCursor::Incremental(delta),
            &mut callback,
            &mut events,
        )
        .unwrap();
    assert_eq!(
        coordinator.committed_offsets[0]
            .as_ref()
            .and_then(|checkpoint| checkpoint.get_offset("topic:0")),
        Some("10")
    );
    let deferred = FxHashSet::from_iter([Arc::from("test_source")]);
    coordinator
        .settle_pending_offsets(&FxHashSet::default(), &deferred)
        .unwrap();
    assert!(coordinator.pending_offsets[0].is_some());
    coordinator
        .settle_pending_offsets(&FxHashSet::default(), &FxHashSet::default())
        .unwrap();

    let committed = coordinator.committed_offsets[0].as_ref().unwrap();
    assert_eq!(committed.get_offset("topic:0"), Some("11"));
    assert_eq!(committed.get_offset("@laminar.kafka.next.v1:topic:0"), None);
    assert_eq!(
        committed.get_offset("@laminar.kafka.next.v1:topic:1"),
        Some("5")
    );
    assert_eq!(
        committed.metadata().get("owner").map(String::as_str),
        Some("node-a")
    );
    assert_eq!(committed.input_channels(), Some(channels.as_ref()));
    assert_eq!(
        committed
            .assignment_version()
            .map(std::num::NonZeroU64::get),
        Some(7)
    );

    let replacement_channels: Arc<[Vec<u8>]> = Arc::from([b"topic:2".to_vec()]);
    let mut replacement = SourceCheckpoint::new();
    replacement.set_offset("topic:2", "1");
    replacement
        .set_input_channels(Arc::clone(&replacement_channels))
        .unwrap();
    replacement.bind_assignment_version(std::num::NonZeroU64::new(8).unwrap());
    coordinator
        .stage_batch(
            0,
            &int_batch(2),
            SourceBatchCursor::Complete(replacement),
            &mut callback,
            &mut events,
        )
        .unwrap();
    coordinator.commit_pending_offsets().unwrap();
    let committed = coordinator.committed_offsets[0].as_ref().unwrap();
    assert_eq!(committed.get_offset("topic:2"), Some("1"));
    assert_eq!(committed.get_offset("topic:0"), None);
    assert_eq!(committed.get_offset("@laminar.kafka.next.v1:topic:0"), None);
    assert_eq!(committed.get_offset("@laminar.kafka.next.v1:topic:1"), None);
    assert_eq!(
        committed.input_channels(),
        Some(replacement_channels.as_ref())
    );

    let stale = SourceCheckpointDelta::new(
        std::num::NonZeroU64::new(7).unwrap(),
        channels,
        std::collections::HashMap::from([("topic:0".into(), Some("12".into()))]),
    )
    .unwrap();
    let error = coordinator
        .stage_batch(
            0,
            &int_batch(3),
            SourceBatchCursor::Incremental(stale),
            &mut callback,
            &mut events,
        )
        .unwrap_err();
    assert!(error.to_string().contains("invalid incremental cursor"));

    let rollback = SourceCheckpointDelta::new(
        std::num::NonZeroU64::new(8).unwrap(),
        replacement_channels,
        std::collections::HashMap::from([("topic:2".into(), Some("2".into()))]),
    )
    .unwrap();
    coordinator
        .stage_batch(
            0,
            &int_batch(4),
            SourceBatchCursor::Incremental(rollback),
            &mut callback,
            &mut events,
        )
        .unwrap();
    coordinator.discard_pending_offsets();
    assert!(coordinator.pending_offsets[0].is_none());
    assert_eq!(
        coordinator.committed_offsets[0]
            .as_ref()
            .and_then(|checkpoint| checkpoint.get_offset("topic:2")),
        Some("1")
    );
}

#[tokio::test]
async fn emitted_barrier_holds_polling_until_an_applicable_release() {
    let state = Arc::new(BarrierHoldProbeState::default());
    let source = SourceRegistration {
        name: "barrier-hold-probe".into(),
        connector: Box::new(BarrierHoldProbeSource {
            state: Arc::clone(&state),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("barrier-hold-probe"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let mut coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        PipelineConfig {
            fallback_poll_interval: Duration::from_secs(60),
            checkpoint_schedule: CheckpointSchedule::Disabled,
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    let barrier = CheckpointBarrier::new(70, 70);
    let control = coordinator.source_handles[0].barrier_control();
    assert!(coordinator.source_handles[0]
        .barrier_injector
        .trigger(barrier));
    state.data_ready.notify_one();
    coordinator.source_handles[0]
        .startup_activation
        .take()
        .unwrap()
        .send(());

    let received = tokio::time::timeout(Duration::from_secs(2), coordinator.rx.recv())
        .await
        .expect("source did not emit the injected barrier")
        .unwrap();
    assert!(matches!(received, SourceMsg::Barrier { barrier: seen, .. } if seen == barrier));
    let polls_at_barrier = state.polls.load(Ordering::SeqCst);
    let control_before_stale = state.control_drives.load(Ordering::SeqCst);

    control.release_exact(CheckpointAttempt::canonical(60));
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.control_drives.load(Ordering::SeqCst) <= control_before_stale {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("held source did not service its control plane");
    assert_eq!(
        state.polls.load(Ordering::SeqCst),
        polls_at_barrier,
        "a stale release resumed source polling"
    );

    control.release_exact(CheckpointAttempt::canonical(70));
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.polls.load(Ordering::SeqCst) == polls_at_barrier {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exact barrier release did not resume source polling");

    coordinator.source_handles[0].task.mark_expected_shutdown();
    control.stop_hold();
    coordinator.source_handles[0].task.notify_shutdown();
    let handle = coordinator.source_handles.pop().unwrap();
    drop(handle.epoch_committed_tx);
    drop(handle.barrier_release_tx);
    assert!(
        handle
            .task
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(2))
            .await,
        "source task did not stop"
    );
}

#[tokio::test]
async fn returned_batch_is_retained_while_source_cursor_is_unreconciled() {
    let state = Arc::new(BarrierRetrySourceState::default());
    state.emit_batch.store(true, Ordering::Release);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let source = SourceRegistration {
        name: "batch-retry".into(),
        connector: Box::new(BarrierRetrySource {
            state: Arc::clone(&state),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("batch-retry"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let config = PipelineConfig {
        fallback_poll_interval: Duration::from_millis(1),
        checkpoint_schedule: CheckpointSchedule::Disabled,
        ..PipelineConfig::default()
    };
    let coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        config,
        Arc::clone(&shutdown),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    let callback = MockCallback::new();
    let written_rows = Arc::clone(&callback.written_rows);
    let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
    let run = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
    ready_rx.await.unwrap().unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while state.capture_attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("returned batch did not attempt cursor capture");
    let polls_after_batch = state.polls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(written_rows.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.polls.load(Ordering::SeqCst),
        polls_after_batch,
        "source polled past a batch whose cursor was not reconciled"
    );

    state.allow_capture.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), async {
        while written_rows.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retained batch was not delivered after cursor reconciliation");

    shutdown.notify_one();
    assert!(matches!(run.await.unwrap(), ExitReason::Shutdown));
}

#[tokio::test]
async fn dropping_before_runtime_ready_closes_source_without_polling() {
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source(
        "cancelled-before-activation",
        Arc::clone(&state),
        false,
        false,
        SourcePosition::Initial,
    );
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();

    drop(coordinator);
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.close_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source was not closed when startup ownership disappeared");
    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 0);
    assert!(!state.open.load(Ordering::SeqCst));
}

#[tokio::test]
async fn cancelling_run_fences_replacement_until_source_exit() {
    let runtime = StreamingCoordinatorRuntime::new();
    let state = Arc::new(StartupSourceState::default());
    let source = startup_source_with_close_delay(
        "cancelled-run",
        Arc::clone(&state),
        false,
        false,
        Duration::ZERO,
        Duration::from_millis(250),
        SourcePosition::Initial,
        ConnectorCancellationPolicy::CancelSafe,
    );
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = StreamingCoordinator::new(
        &runtime,
        vec![source],
        PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("the source generation must start");
    let run = tokio::spawn(coordinator.run(MockCallback::new()));
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.poll_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the source actor never entered its polling loop");

    run.abort();
    assert!(run
        .await
        .expect_err("the run task must be cancelled")
        .is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.close_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling run did not request source shutdown");

    let (_overlap_control_tx, overlap_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let overlap = StreamingCoordinator::new(
        &runtime,
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        overlap_control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    assert!(matches!(overlap, Err(DbError::Pipeline(ref message))
        if message.contains("cancelled-run")
            && message.contains("prior connector generations remain unresolved")));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let terminal = runtime
                .owned_source_tasks
                .lock()
                .iter()
                .all(SourceTaskLease::is_finished);
            if terminal && state.close_calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelling run orphaned its source generation");
    assert!(!state.open.load(Ordering::SeqCst));

    let (_replacement_control_tx, replacement_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    StreamingCoordinator::new(
        &runtime,
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        replacement_control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("a cancelled terminal generation must not block replacement construction");
}

#[tokio::test]
async fn public_runtime_rejects_overlapping_source_generations() {
    let runtime = StreamingCoordinatorRuntime::new();
    let (task_owner, task_tracker) = laminar_connectors::connector::ConnectorTaskOwner::new();
    let child = task_owner.track().expect("live source child");
    let tracker_calls = Arc::new(AtomicU64::new(0));
    let close_calls = Arc::new(AtomicU64::new(0));
    let source = SourceRegistration {
        name: "runtime-owned-source".into(),
        connector: Box::new(TrackedStartupSource {
            _task_owner: task_owner,
            task_tracker,
            tracker_calls: Arc::clone(&tracker_calls),
            start_error: None,
            close_calls: Arc::clone(&close_calls),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("runtime-owned-source"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let (_first_control_tx, first_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let first = StreamingCoordinator::new(
        &runtime,
        vec![source],
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        first_control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("the first source generation must start");
    assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);

    let (_overlap_control_tx, overlap_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let overlap = StreamingCoordinator::new(
        &runtime,
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        overlap_control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    assert!(matches!(overlap, Err(DbError::Pipeline(ref message))
        if message.contains("runtime-owned-source")
            && message.contains("prior connector generations remain unresolved")));

    drop(first);
    tokio::time::timeout(Duration::from_secs(2), async {
        while close_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first source actor did not close");
    assert!(runtime
        .owned_source_tasks
        .lock()
        .iter()
        .any(|task| !task.is_finished()));

    drop(child);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime
                .owned_source_tasks
                .lock()
                .iter()
                .all(SourceTaskLease::is_finished)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first source generation did not terminate");
    assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);

    let (_replacement_control_tx, replacement_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    StreamingCoordinator::new(
        &runtime,
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        replacement_control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("a terminated generation must not block replacement construction");
}

#[tokio::test]
async fn public_runtime_fences_a_live_source_less_coordinator() {
    let runtime = StreamingCoordinatorRuntime::new();
    let (_first_control_tx, first_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let first = StreamingCoordinator::new(
        &runtime,
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        first_control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("the first coordinator generation must be admitted");

    let (_overlap_control_tx, overlap_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let overlap = StreamingCoordinator::new(
        &runtime,
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        overlap_control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    assert!(matches!(overlap, Err(DbError::Pipeline(ref message))
        if message.contains("prior coordinator generation is still active")));

    drop(first);
    let (_replacement_control_tx, replacement_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    StreamingCoordinator::new(
        &runtime,
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        replacement_control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("dropping the prior coordinator must release its generation");
}

#[test]
fn public_runtime_terminal_proof_survives_executor_shutdown() {
    let runtime = StreamingCoordinatorRuntime::new();
    let (task_owner, task_tracker) = laminar_connectors::connector::ConnectorTaskOwner::new();
    let child = task_owner.track().expect("live source child");
    let tracker_calls = Arc::new(AtomicU64::new(0));
    let source = SourceRegistration {
        name: "executor-shutdown-source".into(),
        connector: Box::new(TrackedStartupSource {
            _task_owner: task_owner,
            task_tracker,
            tracker_calls: Arc::clone(&tracker_calls),
            start_error: None,
            close_calls: Arc::new(AtomicU64::new(0)),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("executor-shutdown-source"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let first_executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_first_control_tx, first_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let first = first_executor
        .block_on(StreamingCoordinator::new(
            &runtime,
            vec![source],
            PipelineConfig::default(),
            Arc::new(tokio::sync::Notify::new()),
            first_control_rx,
            Arc::new(AtomicBool::new(false)),
        ))
        .expect("the first source generation must start");
    assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);

    // The actor and its detached outcome supervisor are both cancelled without another poll.
    // The actor wrapper must still publish exit, while the exact connector child remains fenced.
    drop(first_executor);
    drop(first);

    let second_executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_overlap_control_tx, overlap_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let overlap = second_executor.block_on(StreamingCoordinator::new(
        &runtime,
        Vec::new(),
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        overlap_control_rx,
        Arc::new(AtomicBool::new(false)),
    ));
    assert!(matches!(overlap, Err(DbError::Pipeline(ref message))
        if message.contains("executor-shutdown-source")
            && message.contains("prior connector generations remain unresolved")));

    drop(child);
    let (_replacement_control_tx, replacement_control_rx) =
        mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    second_executor
        .block_on(StreamingCoordinator::new(
            &runtime,
            Vec::new(),
            PipelineConfig::default(),
            Arc::new(tokio::sync::Notify::new()),
            replacement_control_rx,
            Arc::new(AtomicBool::new(false)),
        ))
        .expect("tracker termination must release the executor-independent source fence");
    assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn source_start_stage_uses_one_deadline_and_rolls_back_current_and_prior() {
    let prior = Arc::new(StartupSourceState::default());
    let current = Arc::new(StartupSourceState::default());
    let config = PipelineConfig {
        checkpoint_timeout: Duration::from_secs(10),
        ..PipelineConfig::default()
    };
    let result = startup_result_with_config(
        vec![
            startup_source_with_delay(
                "prior",
                Arc::clone(&prior),
                false,
                false,
                Duration::from_secs(6),
                SourcePosition::Initial,
            ),
            startup_source_with_delay(
                "current",
                Arc::clone(&current),
                false,
                false,
                Duration::from_secs(6),
                SourcePosition::Initial,
            ),
        ],
        config,
    )
    .await;

    let Err(error) = result else {
        panic!("the second source must consume the remaining shared startup budget");
    };
    assert!(
        matches!(error, DbError::Config(ref message)
            if message.contains("source 'current' start failed at initial position")
                && message.contains("shared 10s source-start stage deadline")),
        "unexpected error: {error}"
    );
    assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(current.open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(current.close_calls.load(Ordering::SeqCst), 1);
    assert!(!prior.open.load(Ordering::SeqCst));
    assert!(!current.open.load(Ordering::SeqCst));
    assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(current.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn startup_rollback_closes_stalled_sources_with_one_shared_deadline() {
    let first = Arc::new(StartupSourceState::default());
    let second = Arc::new(StartupSourceState::default());
    let failing = Arc::new(StartupSourceState::default());
    let started = tokio::time::Instant::now();

    let result = startup_result(vec![
        startup_source_with_close_delay(
            "stalled-cleanup-a",
            Arc::clone(&first),
            false,
            false,
            Duration::ZERO,
            Duration::from_secs(60),
            SourcePosition::Initial,
            ConnectorCancellationPolicy::CancelSafe,
        ),
        startup_source_with_close_delay(
            "stalled-cleanup-b",
            Arc::clone(&second),
            false,
            false,
            Duration::ZERO,
            Duration::from_secs(60),
            SourcePosition::Initial,
            ConnectorCancellationPolicy::CancelSafe,
        ),
        startup_source(
            "failing-cleanup-trigger",
            Arc::clone(&failing),
            true,
            false,
            SourcePosition::Initial,
        ),
    ])
    .await;

    assert!(matches!(result, Err(DbError::Config(_))));
    assert_eq!(
        tokio::time::Instant::now() - started,
        PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
        "independent cleanup attempts must not multiply the rollback budget"
    );
    assert_eq!(first.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(failing.close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expired_source_start_budget_never_polls_initial_start() {
    let state = Arc::new(StartupSourceState::default());
    let result = startup_result_with_config(
        vec![startup_source(
            "unattempted-initial",
            Arc::clone(&state),
            false,
            false,
            SourcePosition::Initial,
        )],
        PipelineConfig {
            checkpoint_timeout: Duration::ZERO,
            ..PipelineConfig::default()
        },
    )
    .await;

    assert!(
        matches!(result, Err(DbError::Config(ref message))
            if message.contains("source 'unattempted-initial' start was not attempted")),
        "an unattempted initial start must retain configuration-error classification"
    );
    assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
    assert_eq!(state.close_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn expired_source_start_budget_never_polls_resume_start() {
    let state = Arc::new(StartupSourceState::default());
    let attempt = CheckpointAttempt::new(9, 9);
    let result = startup_result_with_config(
        vec![startup_source(
            "unattempted-resume",
            Arc::clone(&state),
            false,
            false,
            SourcePosition::Resume {
                attempt,
                checkpoint: checkpoint_at(9),
            },
        )],
        PipelineConfig {
            checkpoint_timeout: Duration::ZERO,
            ..PipelineConfig::default()
        },
    )
    .await;

    assert!(
        matches!(result, Err(DbError::Checkpoint(ref message))
            if message.contains("[LDB-6003]")
                && message.contains("source 'unattempted-resume' start was not attempted")
                && message.contains("epoch=9 id=9")),
        "an unattempted resume must retain checkpoint-error classification"
    );
    assert_eq!(state.open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
    assert_eq!(state.restore_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.close_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn timed_out_source_start_retires_candidate_at_the_shared_deadline() {
    let state = Arc::new(StartupSourceState::default());
    let config = PipelineConfig {
        checkpoint_timeout: Duration::from_secs(10),
        ..PipelineConfig::default()
    };
    let started = tokio::time::Instant::now();
    let result = startup_result_with_config(
        vec![startup_source_with_policy(
            "retired-start",
            Arc::clone(&state),
            false,
            false,
            Duration::from_secs(12),
            SourcePosition::Initial,
            ConnectorCancellationPolicy::RetireConnector,
        )],
        config,
    )
    .await;

    assert!(matches!(
        result,
        Err(DbError::Config(ref message))
            if message.contains("shared 10s source-start stage deadline")
    ));
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(10)
    );
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.close_calls.load(Ordering::SeqCst),
        0,
        "a retired startup candidate must not receive a later connector call"
    );
    assert!(state.open.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn source_start_completion_tied_with_deadline_is_rejected() {
    let state = Arc::new(StartupSourceState::default());
    let mut connector = StartupSource {
        state: Arc::clone(&state),
        schema: Arc::new(Schema::empty()),
        start_delay: Duration::from_secs(2),
        close_delay: Duration::ZERO,
        fail_open: false,
        fail_restore: false,
        cancellation_policy: ConnectorCancellationPolicy::RetireConnector,
        contract: replayable_append_only_source_contract(),
    };
    let request = SourceStart::new(
        laminar_connectors::config::ConnectorConfig::new("deadline-tie"),
        SourcePosition::Initial,
        DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap();

    let outcome = start_source_once(
        &mut connector,
        request,
        tokio::time::Instant::now() + Duration::from_secs(2),
        #[cfg(feature = "cluster")]
        None,
    )
    .await;

    assert!(matches!(outcome, SourceStartOutcome::TimedOut));
    assert_eq!(state.start_completions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_resume_failure_is_fatal_and_closes_all_started_sources() {
    let prior = Arc::new(StartupSourceState::default());
    let failing = Arc::new(StartupSourceState::default());
    let result = startup_result(vec![
        startup_source(
            "prior",
            Arc::clone(&prior),
            false,
            false,
            SourcePosition::Resume {
                attempt: CheckpointAttempt::new(7, 7),
                checkpoint: checkpoint_at(7),
            },
        ),
        startup_source(
            "failing",
            Arc::clone(&failing),
            false,
            true,
            SourcePosition::Resume {
                attempt: CheckpointAttempt::new(7, 7),
                checkpoint: checkpoint_at(7),
            },
        ),
    ])
    .await;

    let Err(err) = result else {
        panic!("source resume-position failure must abort startup");
    };
    assert!(
        matches!(err, DbError::Checkpoint(ref msg) if msg.contains("source 'failing' start failed while resuming exact checkpoint epoch=7 id=7")),
        "unexpected error: {err}"
    );
    assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(failing.open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(prior.restore_calls.load(Ordering::SeqCst), 1);
    assert_eq!(failing.restore_calls.load(Ordering::SeqCst), 1);
    assert!(!prior.open.load(Ordering::SeqCst));
    assert!(!failing.open.load(Ordering::SeqCst));
    assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(failing.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(failing.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_initial_start_failure_closes_prior_and_partially_started_source() {
    let prior = Arc::new(StartupSourceState::default());
    let failing = Arc::new(StartupSourceState::default());
    let result = startup_result(vec![
        startup_source(
            "prior",
            Arc::clone(&prior),
            false,
            false,
            SourcePosition::Initial,
        ),
        startup_source(
            "failing",
            Arc::clone(&failing),
            true,
            false,
            SourcePosition::Initial,
        ),
    ])
    .await;

    let Err(err) = result else {
        panic!("source initial-start failure must abort startup");
    };
    assert!(
        matches!(err, DbError::Config(ref msg) if msg.contains("source 'failing' start failed at initial position")),
        "unexpected error: {err}"
    );
    assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(failing.open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(prior.restore_calls.load(Ordering::SeqCst), 0);
    assert_eq!(failing.restore_calls.load(Ordering::SeqCst), 0);
    assert!(!prior.open.load(Ordering::SeqCst));
    assert!(!failing.open.load(Ordering::SeqCst));
    assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(failing.close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(failing.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn failed_source_start_retains_connector_child_fence() {
    let (task_owner, task_tracker) = laminar_connectors::connector::ConnectorTaskOwner::new();
    let child = task_owner
        .track()
        .expect("the startup generation must still admit child tasks");
    let close_calls = Arc::new(AtomicU64::new(0));
    let tracker_calls = Arc::new(AtomicU64::new(0));
    let source = SourceRegistration {
        name: "tracked-start-failure".into(),
        connector: Box::new(TrackedStartupSource {
            _task_owner: task_owner,
            task_tracker,
            tracker_calls: Arc::clone(&tracker_calls),
            start_error: Some(ConnectorError::Internal(
                "injected tracked startup failure".into(),
            )),
            close_calls: Arc::clone(&close_calls),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("tracked-start-failure"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let runtime = StreamingCoordinatorRuntime::new();
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);

    let result = StreamingCoordinator::new(
        &runtime,
        vec![source],
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await;

    assert!(matches!(result, Err(DbError::Config(ref message))
        if message.contains("injected tracked startup failure")));
    assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);
    let fence = {
        let fences = runtime.owned_connector_task_fences.lock();
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].name(), "source:tracked-start-failure");
        fences[0].clone()
    };
    assert!(
        !fence
            .wait_until(tokio::time::Instant::now() + Duration::from_millis(1))
            .await,
        "startup returned without retaining the still-running connector child"
    );

    drop(child);
    assert!(
        fence
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        "startup fence remained live after the final connector child exited"
    );
}

#[tokio::test(start_paused = true)]
async fn outcome_unknown_source_start_retires_without_close() {
    let (task_owner, task_tracker) = laminar_connectors::connector::ConnectorTaskOwner::new();
    let child = task_owner
        .track()
        .expect("the startup generation must still admit child tasks");
    let close_calls = Arc::new(AtomicU64::new(0));
    let tracker_calls = Arc::new(AtomicU64::new(0));
    let source = SourceRegistration {
        name: "ambiguous-start".into(),
        connector: Box::new(TrackedStartupSource {
            _task_owner: task_owner,
            task_tracker,
            tracker_calls: Arc::clone(&tracker_calls),
            start_error: Some(ConnectorError::outcome_unknown(
                "injected ambiguous start result",
                true,
            )),
            close_calls: Arc::clone(&close_calls),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("ambiguous-start"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let runtime = StreamingCoordinatorRuntime::new();
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);

    let result = StreamingCoordinator::new(
        &runtime,
        vec![source],
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await;

    assert!(matches!(result, Err(DbError::Config(ref message))
        if message.contains("injected ambiguous start result")));
    assert_eq!(
        close_calls.load(Ordering::SeqCst),
        0,
        "an ambiguous start result must retire the connector without another hook"
    );
    assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);
    let fence = {
        let fences = runtime.owned_connector_task_fences.lock();
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].name(), "source:ambiguous-start");
        fences[0].clone()
    };
    assert!(!fence.is_finished());
    drop(child);
    assert!(
        fence
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
}

#[tokio::test]
async fn immediate_source_fault_is_ordered_after_runtime_ready() {
    let state = Arc::new(RuntimeSourceState::default());
    let coordinator = runtime_failure_coordinator(
        DeliveryGuarantee::AtLeastOnce,
        RuntimeSourceFailure::TerminalPoll,
        Arc::clone(&state),
        Arc::new(tokio::sync::Notify::new()),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        state.polls.load(Ordering::SeqCst),
        0,
        "terminal source fault was produced before runtime readiness"
    );

    let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
    let run = tokio::spawn(async move {
        coordinator
            .run_with_ready(MockCallback::new(), ready_tx)
            .await
    });
    ready_rx
        .await
        .expect("coordinator retained readiness sender")
        .expect("runtime readiness must precede source activation");
    let exit = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("terminal source fault was not observed")
        .unwrap();
    assert!(matches!(exit, ExitReason::Fault(ref reason)
            if reason.contains("terminal poll failure")));
}

#[tokio::test]
async fn connector_batch_schema_mismatch_faults_before_enqueue() {
    let coordinator = runtime_failure_coordinator(
        DeliveryGuarantee::BestEffort,
        RuntimeSourceFailure::SchemaMismatch,
        Arc::new(RuntimeSourceState::default()),
        Arc::new(tokio::sync::Notify::new()),
    )
    .await;

    let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
        .await
        .expect("connector schema mismatch was not observed");
    assert!(matches!(exit, ExitReason::Fault(ref reason) if reason.contains("schema mismatch")));
}

#[tokio::test]
async fn terminal_source_poll_failure_faults_all_delivery_modes() {
    for guarantee in [
        DeliveryGuarantee::BestEffort,
        DeliveryGuarantee::AtLeastOnce,
        DeliveryGuarantee::ExactlyOnce,
    ] {
        let state = Arc::new(RuntimeSourceState::default());
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let coordinator = runtime_failure_coordinator(
            guarantee,
            RuntimeSourceFailure::TerminalPoll,
            Arc::clone(&state),
            shutdown,
        )
        .await;

        let exit =
            tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
                .await
                .expect("terminal source poll failure must stop the pipeline");

        assert!(
            matches!(exit, ExitReason::Fault(ref error) if error.contains("terminal poll failure")),
            "{guarantee} must not stay live after losing a configured source, got {exit:?}"
        );
        assert!(state.polls.load(Ordering::SeqCst) > 0);
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn terminal_checkpoint_failure_does_not_tail_poll_or_ack() {
    let state = Arc::new(PendingCheckpointFailureState::default());
    let source = SourceRegistration {
        name: "pending-checkpoint-failure-source".into(),
        connector: Box::new(PendingCheckpointFailureSource {
            state: Arc::clone(&state),
        }),
        config: laminar_connectors::config::ConnectorConfig::new(
            "pending-checkpoint-failure-source",
        ),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::BestEffort,
            checkpoint_schedule: CheckpointSchedule::Periodic(Duration::from_secs(60)),
            fallback_poll_interval: Duration::from_millis(1),
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("source must start");

    let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
        .await
        .expect("checkpoint failure must stop the pipeline");
    assert!(matches!(exit, ExitReason::Fault(ref error)
        if error.contains("injected pending checkpoint failure")));
    assert_eq!(state.polls.load(Ordering::SeqCst), 1);
    assert_eq!(state.checkpoint_captures.load(Ordering::SeqCst), 2);
    assert_eq!(state.commit_notifications.load(Ordering::SeqCst), 0);
    assert_eq!(state.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn source_commit_notification_failure_faults_replay_guaranteed_modes() {
    for guarantee in [
        DeliveryGuarantee::AtLeastOnce,
        DeliveryGuarantee::ExactlyOnce,
    ] {
        let state = Arc::new(RuntimeSourceState::default());
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let coordinator = runtime_failure_coordinator(
            guarantee,
            RuntimeSourceFailure::CommitNotification,
            Arc::clone(&state),
            shutdown,
        )
        .await;
        coordinator.broadcast_epoch_committed(11, &FxHashMap::default());

        let exit =
            tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
                .await
                .expect("source commit-notification failure must stop the pipeline");

        assert!(
            matches!(exit, ExitReason::Fault(ref error) if error.contains("commit notification failed at epoch 11")),
            "{guarantee} must fault for recovery after commit notification fails, got {exit:?}"
        );
        assert_eq!(state.commit_notifications.load(Ordering::SeqCst), 1);
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_cancels_an_in_flight_cancel_safe_poll() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let state = Arc::new(CancellationSafePollState::default());
    let source = SourceRegistration {
        name: "lease-fenced-cancel-safe-source".into(),
        connector: Box::new(CancellationSafePollSource {
            state: Arc::clone(&state),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("lease-fenced-cancel-safe-source"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let node_id = laminar_core::state::NodeId(33);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let mut coordinator = StreamingCoordinator::new_with_source_registry(
        vec![source],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_schedule: CheckpointSchedule::Periodic(Duration::from_secs(60)),
            fallback_poll_interval: Duration::from_millis(1),
            ..PipelineConfig::default()
        },
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
        Some(Arc::clone(&controller)),
        Arc::new(parking_lot::Mutex::new(Vec::new())),
        empty_connector_task_fences(),
        crate::db::RuntimeMode::Cluster,
    )
    .await
    .unwrap();
    let task = coordinator.source_handles[0].task.clone();
    coordinator.source_handles[0]
        .startup_activation
        .take()
        .unwrap()
        .send(());

    tokio::time::timeout(Duration::from_secs(2), state.first_poll_started.notified())
        .await
        .expect("source never entered its first poll");
    controller.fence_process_lease();
    assert!(
        task.wait_until(tokio::time::Instant::now() + Duration::from_secs(2))
            .await,
        "lease loss did not stop the in-flight poll"
    );

    assert_eq!(state.poll_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.cancelled_polls.load(Ordering::SeqCst), 1);
    assert_eq!(state.commit_notification_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.closes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn epoch_commit_waits_for_in_flight_poll_without_cancelling_it() {
    let state = Arc::new(CancellationSafePollState::default());
    let source = SourceRegistration {
        name: "cancellation-safe-source".into(),
        connector: Box::new(CancellationSafePollSource {
            state: Arc::clone(&state),
        }),
        config: laminar_connectors::config::ConnectorConfig::new("cancellation-safe-test"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let config = PipelineConfig {
        delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
        checkpoint_schedule: CheckpointSchedule::Periodic(Duration::from_secs(60)),
        fallback_poll_interval: Duration::from_millis(1),
        ..PipelineConfig::default()
    };
    let coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![source],
        config,
        Arc::clone(&shutdown),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    let epoch_committed_tx = coordinator.source_handles[0].epoch_committed_tx.clone();
    let run = tokio::spawn(async move { coordinator.run(MockCallback::new()).await });

    tokio::time::timeout(Duration::from_secs(2), state.first_poll_started.notified())
        .await
        .expect("source never entered its first poll");
    epoch_committed_tx
        .send(Some((17, SourceCheckpoint::new())))
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(state.cancelled_polls.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.commit_notification_calls.load(Ordering::SeqCst),
        0,
        "commit notification must wait for the connector borrow to return"
    );

    state.release_first_poll.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.commit_notification_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("commit notification was not applied after poll completion");
    assert_eq!(state.cancelled_polls.load(Ordering::SeqCst), 0);

    drop(epoch_committed_tx);
    shutdown.notify_one();
    let exit = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("coordinator must stop after shutdown")
        .unwrap();
    assert!(matches!(exit, ExitReason::Shutdown));
}

#[tokio::test]
async fn best_effort_commit_notification_failure_does_not_claim_recovery() {
    let state = Arc::new(RuntimeSourceState::default());
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let coordinator = runtime_failure_coordinator(
        DeliveryGuarantee::BestEffort,
        RuntimeSourceFailure::CommitNotification,
        Arc::clone(&state),
        Arc::clone(&shutdown),
    )
    .await;
    coordinator.broadcast_epoch_committed(11, &FxHashMap::default());

    let run = coordinator.run(MockCallback::new());
    let stop = shut_down_after_observed(&state.commit_notifications, &shutdown);
    let (exit, ()) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(run, stop) })
            .await
            .expect("best-effort pipeline must stop cleanly after shutdown");

    assert!(
        matches!(exit, ExitReason::Shutdown),
        "an advisory commit failure must not claim replay in best-effort mode, got {exit:?}"
    );
    assert_eq!(state.closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fatal_cycle_error_faults_exactly_once() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::ExactlyOnce,
        Some(Duration::from_secs(60)),
    );

    let mut callback = MockCallback::new();
    callback.fatal_at_cycle = Some(1);
    callback.fault_on_error = true; // exactly-once: a fatal cycle error must fault

    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
        .await
        .expect("run() must return after a fatal cycle error");

    assert!(
        matches!(exit, ExitReason::Fault(_)),
        "exactly-once fatal cycle error must fault, got {exit:?}"
    );
    drop(tx);
}

#[tokio::test]
async fn recovery_cycle_error_faults_best_effort() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        shutdown,
        DeliveryGuarantee::BestEffort,
        Some(Duration::from_millis(1)),
    );
    coordinator.last_checkpoint = Instant::now() - Duration::from_secs(1);
    let mut callback = MockCallback::new();
    callback.recovery_at_cycle = Some(1);
    *callback.publication_error.lock() = Some("publication must not be attempted".into());
    let publication_error = Arc::clone(&callback.publication_error);
    let written_rows = Arc::clone(&callback.written_rows);
    let checkpoint_order = Arc::clone(&callback.checkpoint_order);
    let published_barriers = Arc::clone(&callback.published_barriers);
    let generation_dropped = Arc::new(AtomicBool::new(false));
    callback.drop_audit = Some(Arc::clone(&generation_dropped));

    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("recovery error must stop best-effort execution");
    assert!(matches!(exit, ExitReason::Fault(ref error)
        if error.contains("injected recovery")));
    assert_eq!(
        publication_error.lock().as_deref(),
        Some("publication must not be attempted"),
        "a recovery cycle result must not reach stream publication"
    );
    assert_eq!(written_rows.load(Ordering::SeqCst), 0);
    assert!(
        checkpoint_order.lock().is_empty(),
        "a due checkpoint must not drain or capture after an indeterminate cycle"
    );
    assert!(published_barriers.lock().is_empty());
    assert!(
        generation_dropped.load(Ordering::Acquire),
        "a recovery exit must destroy the callback/graph generation before returning"
    );
}

#[tokio::test]
async fn halt_cycle_error_exits_cleanly() {
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::AtLeastOnce,
        Some(Duration::from_secs(60)),
    );
    let mut callback = MockCallback::new();
    callback.halt_at_cycle = Some(1);
    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("halt must stop the coordinator");

    assert!(matches!(exit, ExitReason::Halt(ref reason)
        if reason == "injected halt at cycle 1"));
    drop(tx);
}

#[tokio::test]
async fn permanent_halt_skips_shutdown_graph_drain_and_beats_existing_fault() {
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::ExactlyOnce,
        None,
    );
    coordinator.parked_source_msg = Some(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(7),
        cursor: SourceBatchCursor::Complete(checkpoint_at(7)),
    });
    let mut callback = MockCallback::new();
    callback.fault_on_error = true;
    callback.pipeline_halt = Some("permanent shuffle routing failure".into());
    callback.pipeline_fault = Some("generic graph execution poison".into());
    let cycle_inputs = Arc::clone(&callback.cycle_input_rows);
    let written_rows = Arc::clone(&callback.written_rows);

    let exit = coordinator
        .finish_run(
            &mut callback,
            Some("earlier recoverable checkpoint fault".into()),
            false,
            None,
        )
        .await;

    assert!(matches!(exit, ExitReason::Halt(ref reason)
        if reason == "permanent shuffle routing failure"));
    assert!(cycle_inputs.lock().is_empty());
    assert_eq!(written_rows.load(Ordering::Acquire), 0);
    assert!(coordinator.parked_source_msg.is_none());
}

#[tokio::test]
async fn publication_failure_does_not_settle_offsets_or_write_sinks_and_faults_all_modes() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        shutdown,
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    coordinator.pending_offsets[0] = Some(SourceBatchCursor::Complete(checkpoint_at(7)));
    let mut callback = MockCallback::new();
    *callback.publication_error.lock() = Some("injected subscription admission failure".into());
    let written_rows = Arc::clone(&callback.written_rows);
    let mut results = FxHashMap::default();
    results.insert(Arc::from("test_source"), vec![int_batch(1)]);

    let error = coordinator
        .publish_cycle_outputs(&mut callback, &CycleOutcome::clean(results))
        .await
        .expect_err("publication admission must fail closed");
    assert!(matches!(error, CycleError::Recovery(ref reason)
        if reason.contains("injected subscription admission failure")));
    assert!(coordinator.pending_offsets[0].is_none());
    assert!(coordinator.committed_offsets[0].is_none());
    assert_eq!(written_rows.load(Ordering::SeqCst), 0);

    for guarantee in [
        DeliveryGuarantee::BestEffort,
        DeliveryGuarantee::AtLeastOnce,
        DeliveryGuarantee::ExactlyOnce,
    ] {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
        let coordinator = test_coordinator(rx, control_rx, shutdown, guarantee, None);
        let callback = MockCallback::new();
        *callback.publication_error.lock() = Some("injected subscription admission failure".into());
        let written_rows = Arc::clone(&callback.written_rows);
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            cursor: SourceBatchCursor::Complete(checkpoint_at(7)),
        })
        .await
        .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("publication failure must stop the pipeline");
        assert!(matches!(exit, ExitReason::Fault(ref reason)
            if reason.contains("injected subscription admission failure")));
        assert_eq!(written_rows.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn sink_publication_failure_does_not_advance_source_cursor() {
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(1);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(1);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::new(tokio::sync::Notify::new()),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    coordinator.committed_offsets[0] = Some(checkpoint_at(3));
    coordinator.pending_offsets[0] = Some(SourceBatchCursor::Complete(checkpoint_at(7)));
    let mut callback = MockCallback::new();
    *callback.sink_publication_error.lock() = Some("injected sink rejection".into());
    let mut results = FxHashMap::default();
    results.insert(Arc::from("test_source"), vec![int_batch(1)]);

    let error = coordinator
        .publish_cycle_outputs(&mut callback, &CycleOutcome::clean(results))
        .await
        .expect_err("sink publication must fail the cycle");

    assert!(matches!(error, CycleError::Recovery(ref reason)
        if reason.contains("injected sink rejection")));
    assert!(coordinator.pending_offsets[0].is_none());
    assert_eq!(
        coordinator.committed_offsets[0]
            .as_ref()
            .and_then(|checkpoint| checkpoint.get_offset("test_position")),
        Some("3")
    );
    assert_eq!(callback.written_rows.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn fatal_cycle_error_faults_at_least_once_before_publication() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );

    let mut callback = MockCallback::new();
    callback.fatal_at_cycle = Some(1);
    callback.fault_on_error = true;
    let errors = Arc::clone(&callback.cycle_errors);
    let written_rows = Arc::clone(&callback.written_rows);

    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
        .await
        .expect("run() must return after a fatal cycle error");

    assert!(
        matches!(exit, ExitReason::Fault(_)),
        "at-least-once must recover from a fatal cycle error, got {exit:?}"
    );
    assert_eq!(
        errors.load(Ordering::SeqCst),
        0,
        "a recovery fault must not be counted as a best-effort drop"
    );
    assert_eq!(
        written_rows.load(Ordering::SeqCst),
        0,
        "the failed cycle must not publish output or settle its cursor"
    );
    drop(tx);
}

#[test]
fn source_data_after_barrier_returns_invariant_fault() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::ExactlyOnce,
        Some(Duration::from_secs(60)),
    );
    coordinator.source_names = vec![Arc::from("src0")];
    coordinator.committed_offsets = vec![None];
    coordinator.pending_offsets = vec![None];
    let mut callback = MockCallback::new();
    let mut barriers = Vec::new();
    let mut events = 0u64;
    coordinator.barrier_seen.insert(0);
    let error = coordinator
        .process_msg(
            SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(99),
                cursor: SourceBatchCursor::Complete(checkpoint_at(8)),
            },
            &mut callback,
            &mut barriers,
            &mut events,
        )
        .expect_err("post-barrier data must fail closed");
    assert!(matches!(error, CycleError::Recovery(ref reason)
        if reason.contains("without an exact release")));
}

/// CP-4: an exactly-once sink failure poisons the epoch and aborts its transaction; the
/// coordinator must fault for recovery (via `take_pipeline_fault`) rather than continue and seal
/// offsets past the dropped rows on the next checkpoint.
#[tokio::test]
async fn exactly_once_sink_fault_faults_pipeline() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::ExactlyOnce,
        Some(Duration::from_secs(60)),
    );

    let mut callback = MockCallback::new();
    callback.pipeline_fault = Some("sink 's' write error at epoch 1".to_string());

    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch: int_batch(1),
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
        .await
        .expect("run() must return after a sink fault");

    assert!(
        matches!(exit, ExitReason::Fault(_)),
        "an exactly-once sink fault must fault the pipeline, got {exit:?}"
    );
    drop(tx);
}

/// Test that the coordinator processes messages via direct mpsc channel.
#[tokio::test]
async fn test_coordinator_direct_channel() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);

    // Create coordinator directly (bypassing source spawning).
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
    let coordinator = StreamingCoordinator {
        config: PipelineConfig {
            batch_window: Duration::ZERO,
            max_poll_records: 1000,
            channel_capacity: 64,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_schedule: CheckpointSchedule::Manual,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_timeout: Duration::from_secs(30),
            cycle_budget_ns: 10_000_000,
            drain_budget_ns: 1_000_000,
            query_budget_ns: 8_000_000,
            max_input_buf_batches: 256,
            max_input_buf_bytes: None,
            backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            shared_source_isolation: false,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        },
        rx,
        source_fault_rx: empty_source_fault_rx(),
        source_handles: Vec::new(),
        source_names: vec![Arc::from("test_source")],
        source_input_modes: vec![None],
        shutdown: Arc::clone(&shutdown),
        terminal_shutdown: tokio_util::sync::CancellationToken::new(),
        pending_barrier: PendingBarrier::new(),
        last_checkpoint: Instant::now(),
        checkpoint_retry_not_before: None,
        checkpoint_retry_backoff: Duration::ZERO,
        source_batches_buf: FxHashMap::default(),
        parked_source_msg: None,
        pending_watermark_batches: Vec::new(),
        barrier_seen: FxHashSet::default(),
        committed_offsets: vec![None],
        pending_offsets: vec![None],
        replay_pending: false,
        control_rx,
        checkpoint_complete_rx: None,
        force_ckpt_rx: None,
        manual_waiting: Vec::new(),
        manual_handoff_required: false,
        manual_active: None,
        checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
        last_published_checkpoint: None,
        public_generation: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    let callback = MockCallback::new();

    // Send a batch.
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch,
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    // Signal shutdown after a brief delay.
    let shutdown_clone = Arc::clone(&shutdown);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown_clone.notify_one();
    });

    // Run coordinator — it should process the batch and exit on shutdown.
    coordinator.run(callback).await;

    // The callback was consumed by run(), so we can't inspect it directly.
    // But the test proves: no panics, no deadlocks, clean shutdown.
}

#[tokio::test(start_paused = true)]
async fn source_lease_waits_for_connector_child_guard() {
    let (owner, tracker) = laminar_connectors::connector::ConnectorTaskOwner::new();
    let guard = owner.track().expect("live connector task generation");
    drop(owner);
    let runtime = tokio::runtime::Handle::current();
    let (join, actor_terminal) = spawn_source_actor(&runtime, async {});
    let lease = SourceTaskLease::supervise(
        Arc::from("child-task-source"),
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(AtomicBool::new(false)),
        join,
        actor_terminal,
        Some(tracker),
        &runtime,
    );

    assert!(
        !lease
            .wait_until(tokio::time::Instant::now() + Duration::from_millis(1))
            .await,
        "the source lease finished while a connector child guard remained live"
    );
    drop(guard);
    assert!(
        lease
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        "the source lease did not finish after its last child guard dropped"
    );
}

#[test]
fn abort_before_first_poll_drops_source_actor_before_publishing_terminal() {
    struct DropProbe {
        terminal: Arc<Mutex<Option<Arc<SourceActorTerminalState>>>>,
        dropped: Arc<AtomicBool>,
        terminal_was_finished: Arc<AtomicBool>,
    }

    impl std::future::Future for DropProbe {
        type Output = ();

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            panic!("source actor was polled before its immediate abort");
        }
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let terminal = self
                .terminal
                .lock()
                .clone()
                .expect("terminal state must be installed before abort");
            self.terminal_was_finished
                .store(terminal.is_finished(), Ordering::Release);
            self.dropped.store(true, Ordering::Release);
        }
    }

    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    executor.block_on(async {
        let terminal_slot = Arc::new(Mutex::new(None));
        let dropped = Arc::new(AtomicBool::new(false));
        let terminal_was_finished = Arc::new(AtomicBool::new(false));
        let (join, terminal) = spawn_source_actor(
            &tokio::runtime::Handle::current(),
            DropProbe {
                terminal: Arc::clone(&terminal_slot),
                dropped: Arc::clone(&dropped),
                terminal_was_finished: Arc::clone(&terminal_was_finished),
            },
        );
        *terminal_slot.lock() = Some(Arc::clone(&terminal));

        join.abort();
        assert!(join
            .await
            .expect_err("the unpolled source actor must be cancelled")
            .is_cancelled());
        assert!(dropped.load(Ordering::Acquire));
        assert!(!terminal_was_finished.load(Ordering::Acquire));
        assert!(terminal.is_finished());
    });
}

/// An already-running blocking task ignores Tokio abort. The coordinator reaper stays bounded,
/// but the lease must not report terminal completion until the blocking work actually exits.
#[tokio::test]
async fn shutdown_retains_source_lease_for_task_that_ignores_abort() {
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let task_release = Arc::clone(&release);
    let task_started = Arc::new(AtomicBool::new(false));
    let task_started_flag = Arc::clone(&task_started);
    let (actor_lifetime, actor_terminal) = source_actor_terminal_guard();
    let wedged = tokio::task::spawn_blocking(move || {
        let _actor_lifetime = actor_lifetime;
        task_started_flag.store(true, Ordering::Release);
        let (released, wake) = &*task_release;
        let mut released = released.lock();
        while !*released {
            wake.wait(&mut released);
        }
    });
    while !task_started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    let lease = SourceTaskLease::supervise(
        Arc::from("wedged"),
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(AtomicBool::new(false)),
        wedged,
        actor_terminal,
        None,
        &tokio::runtime::Handle::current(),
    );

    // Always release the blocking worker before asserting so the test runtime can tear down.
    StreamingCoordinator::reap_source_task(lease.clone());
    assert!(!lease.is_finished());
    let (released, wake) = &*release;
    *released.lock() = true;
    wake.notify_all();

    assert!(
        lease
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        "source lease did not observe the blocking task's actual exit"
    );
}

#[tokio::test]
async fn shutdown_retires_a_source_task_that_misses_its_budget() {
    let (_release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let completed = Arc::new(AtomicBool::new(false));
    let task_completed = Arc::clone(&completed);
    let runtime = tokio::runtime::Handle::current();
    let (task, actor_terminal) = spawn_source_actor(&runtime, async move {
        let _ = release_rx.await;
        task_completed.store(true, Ordering::Release);
    });
    let lease = SourceTaskLease::supervise(
        Arc::from("retired"),
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(AtomicBool::new(false)),
        task,
        actor_terminal,
        None,
        &runtime,
    );

    StreamingCoordinator::reap_source_task(lease.clone());
    tokio::time::timeout(
        Duration::from_secs(1),
        lease.wait_until(tokio::time::Instant::now() + Duration::from_secs(1)),
    )
    .await
    .expect("retired source task did not terminate");
    assert!(lease.is_finished());
    assert!(!completed.load(Ordering::Acquire));
}

#[test]
fn completion_rejects_result_for_a_different_attempt() {
    let admitted = CheckpointAttempt::new(7, 7);
    let error = CheckpointCompletion::validated(
        admitted,
        crate::checkpoint_coordinator::CheckpointResult {
            success: true,
            checkpoint_id: 43,
            epoch: admitted.epoch,
            duration: Duration::ZERO,
            error: None,
            failure_disposition: None,
        },
        FxHashMap::default(),
        false,
    )
    .expect_err("a different durable checkpoint ID must be rejected");
    assert!(error.contains("identity mismatch"));
    assert!(error.contains("id=7"));
    assert!(error.contains("id=43"));
}

#[tokio::test]
async fn async_completion_publishes_exact_attempt() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);

    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::BestEffort,
        None,
    )
    .with_checkpoint_complete_rx(completion_rx);
    let callback = MockCallback::new();
    let published = Arc::clone(&callback.published_barriers);
    let join = tokio::spawn(async move { coordinator.run(callback).await });

    let attempt = CheckpointAttempt::new(7, 7);
    completion_tx
        .send(CheckpointCompletion::new(
            attempt,
            FxHashMap::default(),
            false,
        ))
        .await
        .expect("completion receiver must be live");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let has_published = !published.lock().is_empty();
            if has_published {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("async completion was not published");

    shutdown.notify_one();
    drop(source_tx);
    drop(completion_tx);
    let _ = join.await.expect("coordinator task panicked");

    assert_eq!(
        published.lock().as_slice(),
        &[(attempt.epoch, attempt.checkpoint_id)]
    );
}

#[tokio::test]
async fn one_source_task_panic_faults_while_its_peer_remains_connected() {
    let panic_state = Arc::new(RuntimeSourceState::default());
    let peer_state = Arc::new(StartupSourceState::default());
    let panic_source = SourceRegistration {
        name: "panic-source".into(),
        connector: Box::new(RuntimeFailureSource {
            state: Arc::clone(&panic_state),
            failure: RuntimeSourceFailure::Panic,
        }),
        config: laminar_connectors::config::ConnectorConfig::new("panic-source-test"),
        assignment_scoped: false,
        position: SourcePosition::Initial,
    };
    let peer = startup_source(
        "live-peer",
        Arc::clone(&peer_state),
        false,
        false,
        SourcePosition::Initial,
    );
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = StreamingCoordinator::new(
        &StreamingCoordinatorRuntime::new(),
        vec![panic_source, peer],
        PipelineConfig::default(),
        Arc::new(tokio::sync::Notify::new()),
        control_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
        .await
        .expect("a single panicked source task was not supervised");
    assert!(
        matches!(exit, ExitReason::Fault(ref reason)
            if reason.contains("panic-source")
                && reason.contains("without coordinator shutdown")),
        "panicked source was hidden by its live peer: {exit:?}"
    );
    assert_eq!(panic_state.polls.load(Ordering::SeqCst), 1);
    assert_eq!(peer_state.close_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn committed_cut_with_successor_failure_acks_then_faults_before_next_write() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);
    let (source, _barrier_poll) = checkpoint_source_handle("test_source");
    let committed_rx = source.epoch_committed_tx.subscribe();

    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::ExactlyOnce,
        None,
    )
    .with_checkpoint_complete_rx(completion_rx);
    coordinator.source_handles = vec![source];

    let callback = MockCallback::new();
    let published = Arc::clone(&callback.published_barriers);
    let published_at_close = Arc::clone(&callback.published_barriers_observed_at_close);
    let written_rows = Arc::clone(&callback.written_rows);
    let attempt = CheckpointAttempt::new(11, 11);
    let mut result = successful_checkpoint_result(attempt);
    result.error = Some(
        "checkpoint 11 epoch 11 committed, but successor sink epoch 12 failed to begin".into(),
    );
    let mut source_checkpoints = FxHashMap::default();
    let mut source_checkpoint = checkpoint_at(attempt.epoch);
    source_checkpoint.set_offset("partition-0", "committed-11");
    source_checkpoints.insert("test_source".to_string(), source_checkpoint);

    // Make both branches ready before run starts. The completion branch is biased ahead of
    // source intake and must publish checkpoint N, then terminally fence the queued N+1 row.
    source_tx
        .send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(12),
            cursor: SourceBatchCursor::Complete(checkpoint_at(attempt.epoch + 1)),
        })
        .await
        .unwrap();
    completion_tx
        .send(
            CheckpointCompletion::validated(attempt, result, source_checkpoints, false)
                .expect("completion identity must match"),
        )
        .await
        .unwrap();

    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("successor-open failure must terminate the pipeline");

    assert!(
        matches!(exit, ExitReason::Fault(ref error) if error.contains("successor sink epoch 12 failed to begin")),
        "pipeline must report the successor-open fault, got {exit:?}"
    );
    assert_eq!(
        published.lock().as_slice(),
        &[(attempt.epoch, attempt.checkpoint_id)],
        "the durable checkpoint must be published before faulting"
    );
    assert_eq!(
        published_at_close.load(Ordering::Acquire),
        1,
        "checkpoint acknowledgement must precede lifecycle teardown"
    );
    let committed = committed_rx
        .borrow()
        .clone()
        .expect("source must receive the durable checkpoint acknowledgement");
    assert_eq!(committed.0, attempt.epoch);
    assert_eq!(committed.1.get_offset("partition-0"), Some("committed-11"));
    assert_eq!(
        written_rows.load(Ordering::Acquire),
        0,
        "no successor-epoch row may reach a sink after begin_epoch failed"
    );
    drop(source_tx);
    drop(completion_tx);
}

/// Shutdown drains the open epoch but must not synthesize an unaligned final checkpoint.
#[tokio::test]
async fn shutdown_does_not_synthesize_final_checkpoint() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

    let coordinator = StreamingCoordinator {
        config: PipelineConfig {
            batch_window: Duration::ZERO,
            max_poll_records: 1000,
            channel_capacity: 64,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_schedule: CheckpointSchedule::Periodic(Duration::from_secs(60)),
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_timeout: Duration::from_secs(30),
            cycle_budget_ns: 10_000_000,
            drain_budget_ns: 1_000_000,
            query_budget_ns: 8_000_000,
            max_input_buf_batches: 256,
            max_input_buf_bytes: None,
            backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            shared_source_isolation: false,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        },
        rx,
        source_fault_rx: empty_source_fault_rx(),
        source_handles: Vec::new(),
        source_names: vec![Arc::from("test_source")],
        source_input_modes: vec![None],
        shutdown: Arc::clone(&shutdown),
        terminal_shutdown: tokio_util::sync::CancellationToken::new(),
        pending_barrier: PendingBarrier::new(),
        last_checkpoint: Instant::now(),
        checkpoint_retry_not_before: None,
        checkpoint_retry_backoff: Duration::ZERO,
        source_batches_buf: FxHashMap::default(),
        parked_source_msg: None,
        pending_watermark_batches: Vec::new(),
        barrier_seen: FxHashSet::default(),
        committed_offsets: vec![None],
        pending_offsets: vec![None],
        replay_pending: false,
        control_rx,
        checkpoint_complete_rx: None,
        force_ckpt_rx: None,
        manual_waiting: Vec::new(),
        manual_handoff_required: false,
        manual_active: None,
        checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
        last_published_checkpoint: None,
        public_generation: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    let callback = MockCallback::new();
    let control_calls = Arc::clone(&callback.control_checkpoint_call_audit);
    let written_rows = Arc::clone(&callback.written_rows);

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    tx.send(SourceMsg::Batch {
        source_idx: 0,
        batch,
        cursor: SourceBatchCursor::Complete(checkpoint_at(1)),
    })
    .await
    .unwrap();

    shutdown.notify_one();
    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("shutdown drain must terminate");

    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(written_rows.load(Ordering::SeqCst), 1);
    assert_eq!(
        control_calls.load(Ordering::SeqCst),
        0,
        "shutdown must not invoke checkpoint control or originate a final attempt"
    );
}

#[tokio::test]
async fn shutdown_abandons_exact_pending_barrier_and_fails_manual_caller() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let mut coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        Some(Duration::from_secs(60)),
    );
    let attempt = CheckpointAttempt::new(31, 31);
    coordinator.pending_barrier.reset(attempt, 1);
    let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
    coordinator.manual_active = Some(ManualCheckpointAttempt {
        attempt,
        flags: laminar_core::checkpoint::flags::NONE,
        requests: vec![manual_request(reply_tx)],
    });

    let callback = MockCallback::new();
    let abandoned = Arc::clone(&callback.abandoned_attempts);
    shutdown.notify_one();
    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("pending alignment cancellation must not stall shutdown");

    assert!(matches!(exit, ExitReason::Shutdown));
    {
        let audit = abandoned.lock();
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].0, attempt);
        assert!(audit[0].1.contains("shutdown interrupted"));
    }
    let error = reply_rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("shutdown interrupted"));
}

#[tokio::test]
async fn shutdown_drain_ignores_barrier_and_processes_following_batch() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );

    let attempt = CheckpointAttempt::new(41, 41);
    source_tx
        .send(SourceMsg::Barrier {
            source_idx: 0,
            barrier: CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            checkpoint: checkpoint_at(attempt.epoch),
        })
        .await
        .unwrap();
    source_tx
        .send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(7),
            cursor: SourceBatchCursor::Complete(checkpoint_at(attempt.epoch + 1)),
        })
        .await
        .unwrap();

    let callback = MockCallback::new();
    let written_rows = Arc::clone(&callback.written_rows);
    shutdown.notify_one();
    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("a shutdown barrier must not requeue the following batch forever");

    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(written_rows.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_settles_async_tail_before_closing_sinks() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(8);
    let in_flight = Arc::new(AtomicU64::new(1));
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    )
    .with_checkpoint_admission(Arc::clone(&in_flight))
    .with_checkpoint_complete_rx(completion_rx);

    let callback = MockCallback::new();
    let published = Arc::clone(&callback.published_barriers);
    let published_at_close = Arc::clone(&callback.published_barriers_observed_at_close);
    let shutdown_sink_order = Arc::clone(&callback.shutdown_sink_order);
    let attempt = CheckpointAttempt::new(51, 51);
    let tail_in_flight = Arc::clone(&in_flight);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        completion_tx
            .send(CheckpointCompletion::new(
                attempt,
                FxHashMap::default(),
                false,
            ))
            .await
            .unwrap();
        tail_in_flight.fetch_sub(1, Ordering::AcqRel);
    });

    shutdown.notify_one();
    let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
        .await
        .expect("shutdown must wait for the captured durable tail");

    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(
        published.lock().as_slice(),
        &[(attempt.epoch, attempt.checkpoint_id)]
    );
    assert_eq!(
        published_at_close.load(Ordering::SeqCst),
        1,
        "sink close raced the terminal completion"
    );
    assert_eq!(shutdown_sink_order.lock().as_slice(), &["settle", "close"]);
}

#[tokio::test]
async fn shutdown_keeps_sink_actor_open_when_epoch_settlement_fails() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::ExactlyOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.settle_sink_epoch_error = Some("durable witness is unresolved".into());
    let shutdown_sink_order = Arc::clone(&callback.shutdown_sink_order);

    shutdown.notify_one();
    let exit = coordinator.run(callback).await;
    let ExitReason::Fault(reason) = exit else {
        panic!("unresolved durable sink ownership did not fault shutdown");
    };
    assert!(reason.contains("durable witness is unresolved"), "{reason}");
    assert_eq!(shutdown_sink_order.lock().as_slice(), &["settle"]);
}

#[tokio::test]
async fn replay_guarantee_faults_when_sink_shutdown_is_not_acknowledged() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.fault_on_error = true;
    callback.close_error = Some("flush acknowledgement failed".to_string());

    shutdown.notify_one();
    let exit = coordinator.run(callback).await;
    let ExitReason::Fault(reason) = exit else {
        panic!("replay guarantee accepted an unacknowledged sink close");
    };
    assert!(reason.contains("flush acknowledgement failed"));
}

#[tokio::test]
async fn best_effort_reports_sink_shutdown_failure_without_recovery_fault() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let coordinator = test_coordinator(
        rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::BestEffort,
        None,
    );
    let mut callback = MockCallback::new();
    callback.close_error = Some("close acknowledgement failed".to_string());
    let errors = Arc::clone(&callback.cycle_errors);

    shutdown.notify_one();
    let exit = coordinator.run(callback).await;
    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(errors.load(Ordering::SeqCst), 1);
}

/// Test that post-barrier batches are excluded from the current cycle's
/// `source_batches_buf` and deferred to the next cycle.
#[tokio::test]
async fn test_barrier_excludes_post_barrier_data() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));

    let (_control_tx2, control_rx2) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
    let mut coordinator = StreamingCoordinator {
        config: PipelineConfig {
            batch_window: Duration::ZERO,
            max_poll_records: 1000,
            channel_capacity: 64,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_schedule: CheckpointSchedule::Manual,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_timeout: Duration::from_secs(30),
            cycle_budget_ns: 10_000_000,
            drain_budget_ns: 1_000_000,
            query_budget_ns: 8_000_000,
            max_input_buf_batches: 256,
            max_input_buf_bytes: None,
            backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            shared_source_isolation: false,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        },
        rx: mpsc::bounded_async::<SourceMsg>(64).1, // dummy, not used
        source_fault_rx: empty_source_fault_rx(),
        source_handles: Vec::new(),
        source_names: vec![Arc::from("s0"), Arc::from("s1")],
        source_input_modes: vec![None; 2],
        shutdown: Arc::clone(&shutdown),
        terminal_shutdown: tokio_util::sync::CancellationToken::new(),
        pending_barrier: PendingBarrier::new(),
        last_checkpoint: Instant::now(),
        checkpoint_retry_not_before: None,
        checkpoint_retry_backoff: Duration::ZERO,
        source_batches_buf: FxHashMap::default(),
        parked_source_msg: None,
        pending_watermark_batches: Vec::new(),
        barrier_seen: FxHashSet::default(),
        committed_offsets: vec![None, None],
        pending_offsets: vec![None, None],
        replay_pending: false,
        control_rx: control_rx2,
        checkpoint_complete_rx: None,
        force_ckpt_rx: None,
        manual_waiting: Vec::new(),
        manual_handoff_required: false,
        manual_active: None,
        checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
        last_published_checkpoint: None,
        public_generation: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    let mut callback = MockCallback::new();
    let mut barriers = Vec::new();
    let mut cycle_events: u64 = 0;
    coordinator
        .pending_barrier
        .reset(CheckpointAttempt::new(1, 1), 2);

    // Source 0: one pre-barrier batch, then the exact barrier hold begins.
    let batch_1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let barrier = CheckpointBarrier::new(1, 1);

    coordinator
        .process_msg(
            SourceMsg::Batch {
                source_idx: 0,
                batch: batch_1,
                cursor: SourceBatchCursor::Complete(checkpoint_at(10)),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        )
        .unwrap();
    coordinator
        .process_msg(
            SourceMsg::Barrier {
                source_idx: 0,
                barrier,
                checkpoint: checkpoint_at(10),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        )
        .unwrap();
    // Source 1: batch(ts=1), barrier
    let batch_s1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    coordinator
        .process_msg(
            SourceMsg::Batch {
                source_idx: 1,
                batch: batch_s1,
                cursor: SourceBatchCursor::Complete(checkpoint_at(5)),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        )
        .unwrap();
    coordinator
        .process_msg(
            SourceMsg::Barrier {
                source_idx: 1,
                barrier,
                checkpoint: checkpoint_at(5),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        )
        .unwrap();

    // Verify that only the pre-barrier data is staged for each source.
    let s0_batches = coordinator.source_batches_buf.get("s0").unwrap();
    assert_eq!(
        s0_batches.len(),
        1,
        "s0 should have exactly 1 pre-barrier batch"
    );
    let s0_col = s0_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(s0_col.value(0), 1, "s0 batch should contain ts=1");

    let s1_batches = coordinator.source_batches_buf.get("s1").unwrap();
    assert_eq!(s1_batches.len(), 1, "s1 should have exactly 1 batch");

    // Pending offsets stop at the barrier cut.
    assert!(matches!(
        &coordinator.pending_offsets[0],
        Some(SourceBatchCursor::Complete(checkpoint))
            if checkpoint.get_offset("test_position") == Some("10")
    ));
    assert!(matches!(
        &coordinator.pending_offsets[1],
        Some(SourceBatchCursor::Complete(checkpoint))
            if checkpoint.get_offset("test_position") == Some("5")
    ));
    // committed_offsets must still be None — no execute_cycle has run.
    assert!(
        coordinator.committed_offsets[0].is_none(),
        "s0 committed offset should be None before execute_cycle"
    );
    assert!(
        coordinator.committed_offsets[1].is_none(),
        "s1 committed offset should be None before execute_cycle"
    );

    // Simulate successful cycle → commit.
    coordinator.commit_pending_offsets().unwrap();
    assert_eq!(
        coordinator.committed_offsets[0]
            .as_ref()
            .unwrap()
            .get_offset("test_position"),
        Some("10"),
        "s0 committed after cycle"
    );
    assert_eq!(
        coordinator.committed_offsets[1]
            .as_ref()
            .unwrap()
            .get_offset("test_position"),
        Some("5"),
        "s1 committed after cycle"
    );

    // Barriers should have both sources.
    assert_eq!(barriers.len(), 2, "should have barriers from both sources");
}

// A faulted domain's source offset is held back while a healthy sibling source commits.
#[tokio::test]
async fn test_settle_pending_offsets_holds_failed_source() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
    let mut coordinator = StreamingCoordinator {
        config: PipelineConfig {
            batch_window: Duration::ZERO,
            max_poll_records: 1000,
            channel_capacity: 64,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_schedule: CheckpointSchedule::Manual,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_timeout: Duration::from_secs(30),
            cycle_budget_ns: 10_000_000,
            drain_budget_ns: 1_000_000,
            query_budget_ns: 8_000_000,
            max_input_buf_batches: 256,
            max_input_buf_bytes: None,
            backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            shared_source_isolation: false,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        },
        rx: mpsc::bounded_async::<SourceMsg>(64).1,
        source_fault_rx: empty_source_fault_rx(),
        source_handles: Vec::new(),
        source_names: vec![Arc::from("s0"), Arc::from("s1")],
        source_input_modes: vec![None; 2],
        shutdown,
        terminal_shutdown: tokio_util::sync::CancellationToken::new(),
        pending_barrier: PendingBarrier::new(),
        last_checkpoint: Instant::now(),
        checkpoint_retry_not_before: None,
        checkpoint_retry_backoff: Duration::ZERO,
        source_batches_buf: FxHashMap::default(),
        parked_source_msg: None,
        pending_watermark_batches: Vec::new(),
        barrier_seen: FxHashSet::default(),
        committed_offsets: vec![None, None],
        pending_offsets: vec![
            Some(SourceBatchCursor::Complete(checkpoint_at(10))),
            Some(SourceBatchCursor::Complete(checkpoint_at(20))),
        ],
        replay_pending: false,
        control_rx,
        checkpoint_complete_rx: None,
        force_ckpt_rx: None,
        manual_waiting: Vec::new(),
        manual_handoff_required: false,
        manual_active: None,
        checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
        last_published_checkpoint: None,
        public_generation: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    let mut failed: FxHashSet<Arc<str>> = FxHashSet::default();
    failed.insert(Arc::from("s0"));
    coordinator
        .settle_pending_offsets(&failed, &FxHashSet::default())
        .unwrap();

    assert!(
        coordinator.committed_offsets[0].is_none(),
        "faulted s0 must not commit"
    );
    assert!(
        coordinator.pending_offsets[0].is_none(),
        "faulted s0 staged offset is discarded for replay"
    );
    assert_eq!(
        coordinator.committed_offsets[1]
            .as_ref()
            .unwrap()
            .get_offset("test_position"),
        Some("20"),
        "healthy s1 commits and advances"
    );

    coordinator.committed_offsets = vec![None, None];
    coordinator.pending_offsets = vec![
        Some(SourceBatchCursor::Complete(checkpoint_at(10))),
        Some(SourceBatchCursor::Complete(checkpoint_at(20))),
    ];
    let failed = FxHashSet::default();
    let deferred = FxHashSet::from_iter([Arc::from("s0")]);
    coordinator
        .settle_pending_offsets(&failed, &deferred)
        .unwrap();
    assert!(coordinator.committed_offsets[0].is_none());
    assert!(matches!(
        &coordinator.pending_offsets[0],
        Some(SourceBatchCursor::Complete(checkpoint))
            if checkpoint.get_offset("test_position") == Some("10")
    ));
    assert_eq!(
        coordinator.committed_offsets[1]
            .as_ref()
            .and_then(|cp| cp.get_offset("test_position")),
        Some("20")
    );

    coordinator
        .settle_pending_offsets(&failed, &FxHashSet::default())
        .unwrap();
    assert!(coordinator.pending_offsets[0].is_none());
    assert_eq!(
        coordinator.committed_offsets[0]
            .as_ref()
            .and_then(|cp| cp.get_offset("test_position")),
        Some("10")
    );
}

#[tokio::test]
async fn quiet_source_deferral_retries_before_reading_another_message() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, source_rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let coordinator = test_coordinator(
        source_rx,
        control_rx,
        Arc::clone(&shutdown),
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.defer_at_cycle = Some(1);
    let cycle_input_rows = Arc::clone(&callback.cycle_input_rows);
    let written_rows = Arc::clone(&callback.written_rows);

    source_tx
        .send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(7),
            cursor: SourceBatchCursor::Complete(checkpoint_at(10)),
        })
        .await
        .unwrap();

    let stop = {
        let shutdown = Arc::clone(&shutdown);
        let written_rows = Arc::clone(&written_rows);
        tokio::spawn(async move {
            while written_rows.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
            shutdown.notify_one();
        })
    };
    let exit = tokio::time::timeout(Duration::from_secs(2), coordinator.run(callback))
        .await
        .expect("deferred quiet-source input was not retried");
    stop.await.unwrap();

    assert!(matches!(exit, ExitReason::Shutdown));
    assert_eq!(written_rows.load(Ordering::Acquire), 1);
    assert_eq!(
        cycle_input_rows.lock().get(..2),
        Some(&[1, 0][..]),
        "the retry must use graph-retained input before another source drain"
    );
    drop(source_tx);
}

#[tokio::test]
async fn deferred_replay_keeps_its_source_frontier_pin_until_settlement() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (source_tx, source_rx) = mpsc::bounded_async::<SourceMsg>(4);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
    let coordinator = test_coordinator(
        source_rx,
        control_rx,
        shutdown,
        DeliveryGuarantee::AtLeastOnce,
        None,
    );
    let mut callback = MockCallback::new();
    callback.defer_at_cycle = Some(1);
    callback.halt_at_cycle = Some(3);
    let cycle_input_rows = Arc::clone(&callback.cycle_input_rows);
    let pin_cycles = Arc::clone(&callback.source_frontier_pin_cycles);

    source_tx
        .send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(7),
            cursor: SourceBatchCursor::Complete(checkpoint_at(10)),
        })
        .await
        .unwrap();
    let second_source = tokio::spawn(async move {
        while cycle_input_rows.lock().len() < 2 {
            tokio::task::yield_now().await;
        }
        source_tx
            .send(SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(8),
                cursor: SourceBatchCursor::Complete(checkpoint_at(20)),
            })
            .await
            .unwrap();
    });

    let exit = tokio::time::timeout(Duration::from_secs(2), coordinator.run(callback))
        .await
        .expect("deferred replay or successor admission did not complete");
    second_source.await.unwrap();

    assert!(matches!(exit, ExitReason::Halt(ref reason)
        if reason == "injected halt at cycle 3"));
    assert_eq!(
        pin_cycles.lock().as_slice(),
        &[0, 2],
        "replay must retain the original pin; the clean successor cycle must repin"
    );
}

struct BackpressuredCallback {
    inner: MockCallback,
    cycle_count: Arc<std::sync::atomic::AtomicU32>,
    events_per_cycle: Arc<Mutex<Vec<u64>>>,
}

impl BackpressuredCallback {
    fn new(
        cycle_count: Arc<std::sync::atomic::AtomicU32>,
        events_per_cycle: Arc<Mutex<Vec<u64>>>,
    ) -> Self {
        Self {
            inner: MockCallback::new(),
            cycle_count,
            events_per_cycle,
        }
    }
}

impl PipelineCallback for BackpressuredCallback {
    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<CycleOutcome, CycleError> {
        self.cycle_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let total: u64 = source_batches
            .values()
            .flat_map(|bs| bs.iter())
            .map(|b| b.num_rows() as u64)
            .sum();
        self.events_per_cycle.lock().push(total);
        self.inner.execute_cycle(source_batches, watermark).await
    }

    async fn complete_pending_vnode_transition(&mut self) -> Result<bool, CycleError> {
        self.inner.complete_pending_vnode_transition().await
    }

    async fn drain_checkpoint_edges_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), CycleError> {
        self.inner.drain_checkpoint_edges_until(deadline).await
    }

    fn push_to_streams(&self, r: &FxHashMap<Arc<str>, Vec<RecordBatch>>) -> Result<(), CycleError> {
        self.inner.push_to_streams(r)
    }
    async fn write_to_sinks(
        &mut self,
        r: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), CycleError> {
        self.inner.write_to_sinks(r, deadline).await
    }
    fn extract_watermark(
        &mut self,
        s: &str,
        b: &RecordBatch,
        admission_floor: i64,
    ) -> Result<(), CycleError> {
        self.inner.extract_watermark(s, b, admission_floor)
    }
    fn reconcile_source_input_channels(
        &mut self,
        source_name: &str,
        input_channels: Option<Arc<[Vec<u8>]>>,
    ) -> Result<(), CycleError> {
        self.inner
            .reconcile_source_input_channels(source_name, input_channels)
    }
    fn filter_late_rows(
        &self,
        s: &str,
        b: &RecordBatch,
    ) -> Result<Option<RecordBatch>, CycleError> {
        self.inner.filter_late_rows(s, b)
    }
    fn current_watermark(&self) -> i64 {
        self.inner.current_watermark()
    }
    fn publish_barrier(&mut self, attempt: CheckpointAttempt) -> Result<(), String> {
        self.inner.publish_barrier(attempt)
    }
    async fn service_checkpoint_control(
        &mut self,
        offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> CheckpointControlOutcome {
        self.inner.service_checkpoint_control(offsets).await
    }
    async fn checkpoint_with_barrier(
        &mut self,
        cp: FxHashMap<String, SourceCheckpoint>,
        attempt: CheckpointAttempt,
        attempt_started: Instant,
        deadline: tokio::time::Instant,
        flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> BarrierOutcome {
        self.inner
            .checkpoint_with_barrier(
                cp,
                attempt,
                attempt_started,
                deadline,
                flags,
                assignment_fence,
            )
            .await
    }
    async fn reserve_checkpoint_attempt(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, String> {
        self.inner.reserve_checkpoint_attempt(deadline).await
    }
    async fn abandon_checkpoint_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: &str,
        flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        self.inner
            .abandon_checkpoint_attempt(attempt, reason, flags, assignment_fence)
            .await
    }
    async fn cancel_source_barrier_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        reason: &str,
    ) -> Result<(), String> {
        self.inner
            .cancel_source_barrier_attempt(attempt, reason)
            .await
    }
    fn resolve_authoritative_follower_abort(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<(), String> {
        self.inner.resolve_authoritative_follower_abort(attempt)
    }
    fn record_cycle(&self, e: u64, b: u64, ns: u64) {
        self.inner.record_cycle(e, b, ns);
    }
    fn apply_control(&mut self, msg: crate::pipeline::ControlMsg) {
        self.inner.apply_control(msg);
    }

    fn is_backpressured(&self) -> bool {
        true // Always backpressured — drain loop should never fire.
    }
}

/// With `is_backpressured() == true`, the coordinator processes only
/// the first wakeup message per cycle (no drain coalescing). With 5
/// messages pre-loaded and `batch_window=0`, each cycle should see
/// exactly 1 event, spread across multiple cycles.
#[tokio::test]
async fn test_drain_skip_under_backpressure() {
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
    let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

    let coordinator = StreamingCoordinator {
        config: PipelineConfig {
            batch_window: Duration::ZERO,
            max_poll_records: 1000,
            channel_capacity: 64,
            fallback_poll_interval: Duration::from_millis(10),
            checkpoint_schedule: CheckpointSchedule::Manual,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_timeout: Duration::from_secs(30),
            cycle_budget_ns: 10_000_000,
            drain_budget_ns: 1_000_000,
            query_budget_ns: 8_000_000,
            max_input_buf_batches: 256,
            max_input_buf_bytes: None,
            backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            shared_source_isolation: false,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        },
        rx,
        source_fault_rx: empty_source_fault_rx(),
        source_handles: Vec::new(),
        source_names: vec![Arc::from("src")],
        source_input_modes: vec![None],
        shutdown: Arc::clone(&shutdown),
        terminal_shutdown: tokio_util::sync::CancellationToken::new(),
        pending_barrier: PendingBarrier::new(),
        last_checkpoint: Instant::now(),
        checkpoint_retry_not_before: None,
        checkpoint_retry_backoff: Duration::ZERO,
        source_batches_buf: FxHashMap::default(),
        parked_source_msg: None,
        pending_watermark_batches: Vec::new(),
        barrier_seen: FxHashSet::default(),
        committed_offsets: vec![None],
        pending_offsets: vec![None],
        replay_pending: false,
        control_rx,
        checkpoint_complete_rx: None,
        force_ckpt_rx: None,
        manual_waiting: Vec::new(),
        manual_handoff_required: false,
        manual_active: None,
        checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
        last_published_checkpoint: None,
        public_generation: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

    // Pre-load 5 batches (1 row each).
    for i in 0..5 {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![i]))],
        )
        .unwrap();
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch,
            cursor: SourceBatchCursor::Complete(checkpoint_at(u64::try_from(i).unwrap())),
        })
        .await
        .unwrap();
    }

    let shutdown_clone = Arc::clone(&shutdown);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        shutdown_clone.notify_one();
    });

    let cycle_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let events_per_cycle = Arc::new(Mutex::new(Vec::new()));
    let callback =
        BackpressuredCallback::new(Arc::clone(&cycle_count), Arc::clone(&events_per_cycle));
    coordinator.run(callback).await;

    let cycles = cycle_count.load(std::sync::atomic::Ordering::SeqCst);
    let epc = events_per_cycle.lock();
    let total: u64 = epc.iter().sum();

    // All 5 events must be processed (no data loss).
    assert_eq!(total, 5, "all events must be processed, got {total}");
    // Under backpressure each cycle gets only the wakeup message (1
    // event), so we need at least 5 cycles for 5 messages. Without
    // backpressure, cycle 1 would drain all 5 in one shot.
    assert!(
        cycles >= 5,
        "expected >=5 cycles (1 event each), got {cycles} cycles with events/cycle: {epc:?}"
    );
    // Each cycle sees at most 1 event (the wakeup message; drain skipped).
    for (i, &events) in epc.iter().enumerate() {
        assert!(
            events <= 1,
            "cycle {i} saw {events} events, expected <=1 under backpressure"
        );
    }
}
