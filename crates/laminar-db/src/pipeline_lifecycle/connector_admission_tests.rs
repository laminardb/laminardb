use super::LaminarDB;
use super::{
    admit_sink, admit_sink_contract, admit_source_contract, admit_temporal_source_contract,
    close_opened_sinks, has_only_temporal_right_consumers, open_prepared_sinks,
    resolve_stream_output_schemas, validate_source_recovery_assignment,
    ConnectorTaskFenceRegistration, DbError, PreparedSink, RuntimeMode, SinkAdmissionContext,
    TemporalSourceRole, TrackedSourceRegistration, CLUSTER_BEST_EFFORT, EXACT_SINK_PROTOCOL,
    KEYED_SOURCE_PRIMARY_KEY,
};
use crate::db::DbState;
use crate::pipeline::streaming_coordinator::MUTATION_SOURCE_NOT_ADMITTED;
use crate::pipeline::PipelineConfig;
use crate::sink_task::{SinkTaskConfig, DEFAULT_CHANNEL_CAPACITY, SINK_EVENT_CHANNEL_CAPACITY};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, ConnectorTaskOwner, ConnectorTaskTracker, DeliveryGuarantee,
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, SourceBatch,
    SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourceRowPositionCapability, SourceStart, SourceTopology, WriteResult,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::object_store_builder::CheckpointStorageScope;
use laminar_core::checkpoint::ConnectorCheckpoint;
use laminar_core::streaming::StreamCheckpointConfig;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn temporal_test_db() -> (
    Arc<LaminarDB>,
    crate::temporal_test_source::TemporalTestSourceControl,
) {
    let control = crate::temporal_test_source::TemporalTestSourceControl::new();
    let registration_control = control.clone();
    let db = LaminarDB::builder()
        .temporal_join_idle_history_retention(Duration::from_secs(60))
        .register_connector(move |registry| {
            crate::temporal_test_source::register(registry, &registration_control)
        })
        .build()
        .await
        .unwrap();
    (db, control)
}

const ORDERED_FULL_CHANGELOG_CONNECTOR: &str = "ordered-full-changelog-test";

fn ordered_full_changelog_schema() -> SchemaRef {
    use arrow_schema::{DataType, Field, TimeUnit};

    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
        Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ),
    ]))
}

struct OrderedFullChangelogSource;

#[async_trait]
impl SourceConnector for OrderedFullChangelogSource {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            SourceInputMode::FullChangelog,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic))
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        ordered_full_changelog_schema()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

async fn ordered_full_changelog_test_db() -> Arc<LaminarDB> {
    let control = crate::temporal_test_source::TemporalTestSourceControl::new();
    LaminarDB::builder()
        .register_connector(move |registry| {
            crate::temporal_test_source::register(registry, &control)?;
            registry.register_source(
                ORDERED_FULL_CHANGELOG_CONNECTOR,
                laminar_connectors::config::ConnectorInfo {
                    name: ORDERED_FULL_CHANGELOG_CONNECTOR.into(),
                    display_name: "Ordered full changelog test source".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: Vec::new(),
                },
                Arc::new(|_| Ok(Box::new(OrderedFullChangelogSource))),
            )
        })
        .build()
        .await
        .unwrap()
}

async fn persistent_temporal_alo_test_db(
    storage_dir: &std::path::Path,
    control: &crate::temporal_test_source::TemporalTestSourceControl,
) -> Arc<LaminarDB> {
    let registration_control = control.clone();
    LaminarDB::builder()
        .storage_dir(storage_dir)
        .checkpoint(StreamCheckpointConfig {
            interval_ms: None,
            ..StreamCheckpointConfig::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .temporal_join_idle_history_retention(Duration::from_secs(60))
        .register_connector(move |registry| {
            crate::temporal_test_source::register(registry, &registration_control)
        })
        .build()
        .await
        .unwrap()
}

async fn create_temporal_test_inputs(db: &LaminarDB) -> Result<(), DbError> {
    let connector = crate::temporal_test_source::CONNECTOR_NAME;
    db.execute(&format!(
        "CREATE SOURCE left_events (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
         FROM \"{connector}\" ('mode' = 'append')"
    ))
    .await?;
    db.execute(&format!(
        "CREATE SOURCE right_events (id BIGINT PRIMARY KEY, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
         FROM \"{connector}\" ('mode' = 'upsert')"
    ))
    .await?;
    Ok(())
}

async fn create_temporal_test_stream(db: &LaminarDB, name: &str) -> Result<(), DbError> {
    db.execute(&format!(
        "CREATE STREAM {name} AS \
         SELECT l.id AS id, l.value AS left_value, r.value AS right_value \
         FROM left_events l LEFT JOIN right_events \
         FOR SYSTEM_TIME AS OF l.ts AS r ON l.id = r.id"
    ))
    .await?;
    Ok(())
}

async fn next_temporal_output(
    subscription: &mut crate::subscription::SubscriptionPortal,
) -> RecordBatch {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match subscription.next_frame().await {
                Some(crate::subscription::PortalFrame::Batch { batch, .. })
                    if batch.num_rows() != 0 =>
                {
                    break batch;
                }
                Some(crate::subscription::PortalFrame::Batch { .. })
                | Some(crate::subscription::PortalFrame::Barrier { .. }) => {}
                Some(crate::subscription::PortalFrame::Lagged(skipped)) => {
                    panic!("temporal test subscription lagged by {skipped} entries");
                }
                Some(crate::subscription::PortalFrame::Error { message }) => {
                    panic!("temporal test subscription failed: {message}");
                }
                None => panic!("temporal test subscription closed before output"),
            }
        }
    })
    .await
    .expect("managed temporal output timed out")
}

struct RetiringQuiesceSink {
    schema: SchemaRef,
}

#[async_trait]
impl SinkConnector for RetiringQuiesceSink {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Err(ConnectorError::outcome_unknown(
            "injected unknown write outcome",
            true,
        ))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
}

#[tokio::test]
async fn sink_quiesce_waits_for_terminal_proof_after_sticky_close_error() {
    let db = LaminarDB::open().unwrap();
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().expect("live connector child");
    drop(owner);
    let schema = Arc::new(Schema::empty());
    let (event_tx, mut events) =
        laminar_core::streaming::channel::channel(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(SinkTaskConfig {
        name: "retired-quiesce".into(),
        sink_id: Arc::from("retired-quiesce"),
        connector: Box::new(RetiringQuiesceSink {
            schema: Arc::clone(&schema),
        }),
        contract: SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            laminar_connectors::connector::SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_secs(60),
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: Some(tracker),
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    handle
        .write_batch(RecordBatch::new_empty(schema))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("retiring write did not report its error")
        .expect("sink event channel closed unexpectedly");
    handle
        .sync()
        .await
        .expect_err("retired actor must reject later commands");
    db.owned_sink_handles.lock().push(handle.clone());

    let quiesce_db = Arc::clone(&db);
    let quiesce = tokio::spawn(async move {
        quiesce_db
            .quiesce_owned_sink_handles_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !handle.close_outcome_published() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired sink did not publish its sticky close error");
    assert!(
        !quiesce.is_finished(),
        "a sticky close error must not bypass the terminal connector proof"
    );

    drop(guard);
    quiesce
        .await
        .expect("sink quiesce task panicked")
        .expect("terminal sink generation should permit replacement");
    assert!(db.owned_sink_handles.lock().is_empty());
}

#[tokio::test]
async fn expired_sink_quiesce_budget_prunes_an_already_terminal_actor() {
    let db = LaminarDB::open().unwrap();
    let schema = Arc::new(Schema::empty());
    let (event_tx, _events) =
        laminar_core::streaming::channel::channel(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(SinkTaskConfig {
        name: "terminal-before-quiesce".into(),
        sink_id: Arc::from("terminal-before-quiesce"),
        connector: Box::new(RetiringQuiesceSink { schema }),
        contract: SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_secs(60),
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    handle.close().await.unwrap();
    assert!(
        handle
            .wait_terminal_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
    db.owned_sink_handles.lock().push(handle);

    db.quiesce_owned_sink_handles_until(tokio::time::Instant::now())
        .await
        .expect("terminal actors need no remaining cleanup budget");
    assert!(db.owned_sink_handles.lock().is_empty());
}

#[test]
fn recovered_source_assignment_scope_fails_closed() {
    let expected = std::num::NonZeroU64::new(7).unwrap();
    let mut checkpoint = ConnectorCheckpoint::new();

    let error =
        validate_source_recovery_assignment("events", true, Some(&checkpoint), Some(expected))
            .unwrap_err();
    assert!(error.to_string().contains("missing its assignment version"));

    checkpoint.source_assignment_version = std::num::NonZeroU64::new(6);
    let error =
        validate_source_recovery_assignment("events", true, Some(&checkpoint), Some(expected))
            .unwrap_err();
    assert!(error.to_string().contains("committed fence is 7"));

    checkpoint.source_assignment_version = Some(expected);
    validate_source_recovery_assignment("events", true, Some(&checkpoint), Some(expected)).unwrap();

    let error =
        validate_source_recovery_assignment("events", true, Some(&checkpoint), None).unwrap_err();
    assert!(error
        .to_string()
        .contains("no authoritative assignment fence"));

    let error =
        validate_source_recovery_assignment("local", false, Some(&checkpoint), None).unwrap_err();
    assert!(error.to_string().contains("non-assigned source 'local'"));

    checkpoint.source_assignment_version = None;
    validate_source_recovery_assignment("local", false, Some(&checkpoint), None).unwrap();
    validate_source_recovery_assignment("local", false, Some(&checkpoint), Some(expected)).unwrap();
}

#[test]
fn startup_transition_publishes_running_from_starting() {
    let db = LaminarDB::open().unwrap();
    DbState::Starting.store(&db.state);

    db.finish_start_transition().unwrap();

    assert_eq!(DbState::load(&db.state), DbState::Running);
}

#[test]
fn startup_transition_fails_closed_when_compute_loop_faulted() {
    let db = LaminarDB::open().unwrap();
    DbState::Faulted.store(&db.state);
    *db.last_fault.lock() = Some("injected startup fault".into());

    let error = db.finish_start_transition().unwrap_err();

    assert!(
        error.to_string().contains("injected startup fault"),
        "unexpected startup error: {error}"
    );
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
}

#[test]
fn cancelled_runtime_generation_cannot_publish_running() {
    let db = LaminarDB::open().unwrap();
    DbState::Starting.store(&db.state);
    db.runtime_shutdown.read().cancel();

    let error = db.finish_start_transition().unwrap_err();

    assert!(matches!(error, crate::error::DbError::Shutdown));
    assert_eq!(DbState::load(&db.state), DbState::Created);
}

#[cfg(feature = "cluster")]
#[test]
fn assignment_restore_cancellation_outlives_runtime_generation_but_not_close() {
    let db = LaminarDB::open().unwrap();
    let restore = db.assignment_restore_shutdown.clone();

    db.runtime_shutdown.read().cancel();
    assert!(!restore.is_cancelled());

    db.close();
    assert!(restore.is_cancelled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_and_concurrent_start_wait_for_one_owned_attempt() {
    let db = LaminarDB::open().unwrap();
    let topology = db.topology_ddl_lock.write().await;

    let first_db = Arc::clone(&db);
    let first = tokio::spawn(async move { first_db.start().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while DbState::load(&db.state) != DbState::Starting {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("startup attempt was not registered");
    let owned = db
        .startup_attempt
        .lock()
        .clone()
        .expect("Starting must publish its attempt first");

    first.abort();
    let _ = first.await;
    assert_eq!(DbState::load(&db.state), DbState::Starting);

    let second_db = Arc::clone(&db);
    let second = tokio::spawn(async move { second_db.start().await });
    tokio::task::yield_now().await;
    assert!(!second.is_finished());
    assert!(Arc::ptr_eq(
        &owned,
        db.startup_attempt.lock().as_ref().unwrap()
    ));

    drop(topology);
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("owned startup did not finish")
        .expect("concurrent start task panicked")
        .expect("owned startup failed");
    assert_eq!(DbState::load(&db.state), DbState::Running);
    db.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synchronous_close_wakes_the_running_pipeline() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE input (id BIGINT)").await.unwrap();
    db.execute("CREATE STREAM output AS SELECT id FROM input")
        .await
        .unwrap();
    db.start().await.unwrap();
    assert!(db.runtime_handle.lock().await.is_some());

    db.close();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let finished = db
                .runtime_handle
                .lock()
                .await
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished);
            if finished {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("synchronous shutdown request did not wake the pipeline runtime");
    db.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_stop_cannot_downgrade_completed_shutdown() {
    let db = LaminarDB::open().unwrap();
    db.start().await.unwrap();

    // Hold the second lifecycle fence so shutdown owns ShuttingDown and the topology lock,
    // while the trailing stop deterministically records that it observed the same teardown.
    let lifecycle = db.lifecycle_lock.lock().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *db.stop_after_claim_gate.lock() = Some((Arc::clone(&entered), Arc::clone(&release)));

    let shutdown_db = Arc::clone(&db);
    let shutdown = tokio::spawn(async move { shutdown_db.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while DbState::load(&db.state) != DbState::ShuttingDown {
            tokio::task::yield_now().await;
        }
        while db.topology_ddl_lock.try_read().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown did not acquire lifecycle ownership");

    let stop_db = Arc::clone(&db);
    let stop = tokio::spawn(async move { stop_db.stop_pipeline().await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("stop did not observe the in-progress shutdown");

    drop(lifecycle);
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("shutdown remained blocked")
        .expect("shutdown task panicked")
        .expect("shutdown failed");
    assert_eq!(DbState::load(&db.state), DbState::Stopped);

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), stop)
        .await
        .expect("trailing stop remained blocked")
        .expect("stop task panicked")
        .expect("trailing stop failed");
    assert_eq!(DbState::load(&db.state), DbState::Stopped);
    *db.stop_after_claim_gate.lock() = None;
}

#[test]
fn runtime_fault_publication_preserves_lifecycle_ownership() {
    let state = std::sync::atomic::AtomicU8::new(0);
    for initial in [DbState::Starting, DbState::Running] {
        initial.store(&state);
        assert!(super::publish_runtime_fault_state(&state));
        assert_eq!(DbState::load(&state), DbState::Faulted);
    }
    for preserved in [
        DbState::Faulted,
        DbState::Created,
        DbState::ShuttingDown,
        DbState::Stopped,
    ] {
        preserved.store(&state);
        assert!(!super::publish_runtime_fault_state(&state));
        assert_eq!(DbState::load(&state), preserved);
    }
}

#[test]
fn recoverable_runtime_fault_respects_terminal_stop_ownership() {
    let running_state = std::sync::atomic::AtomicU8::new(DbState::Running as u8);
    let live_generation = tokio_util::sync::CancellationToken::new();
    let owns_fault_state = super::publish_runtime_fault_state(&running_state);

    assert!(owns_fault_state);
    assert!(!super::runtime_exit_is_covered_by_terminal_stop(
        owns_fault_state,
        &running_state,
        &live_generation,
    ));
    assert_eq!(DbState::load(&running_state), DbState::Faulted);

    let stopping_state = std::sync::atomic::AtomicU8::new(DbState::ShuttingDown as u8);
    let cancelled_generation = tokio_util::sync::CancellationToken::new();
    cancelled_generation.cancel();
    let owns_fault_state = super::publish_runtime_fault_state(&stopping_state);

    assert!(!owns_fault_state);
    assert!(super::runtime_exit_is_covered_by_terminal_stop(
        owns_fault_state,
        &stopping_state,
        &cancelled_generation,
    ));
    assert_eq!(DbState::load(&stopping_state), DbState::ShuttingDown);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_fault_cannot_steal_a_stop_transition() {
    let db = LaminarDB::open().unwrap();
    db.start().await.unwrap();

    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *db.stop_after_claim_gate.lock() = Some((Arc::clone(&entered), Arc::clone(&release)));
    let stopping_db = Arc::clone(&db);
    let stopping = tokio::spawn(async move { stopping_db.stop_pipeline().await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("stop did not claim lifecycle ownership");

    assert_eq!(DbState::load(&db.state), DbState::ShuttingDown);
    assert!(!super::publish_runtime_fault_state(&db.state));
    assert!(!super::publish_runtime_fault_state(&db.state));
    assert_eq!(DbState::load(&db.state), DbState::ShuttingDown);

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), stopping)
        .await
        .expect("stop remained blocked")
        .expect("stop task panicked")
        .expect("stop failed after racing runtime fault");
    assert_eq!(DbState::load(&db.state), DbState::Created);
    *db.stop_after_claim_gate.lock() = None;

    db.start().await.unwrap();
    assert_eq!(DbState::load(&db.state), DbState::Running);
    db.shutdown().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_fence_linearizes_before_a_public_start_claim() {
    let db = LaminarDB::open().unwrap();
    let lifecycle_claim = db.startup_attempt.lock();
    let starting_db = Arc::clone(&db);
    let starting = tokio::spawn(async move { starting_db.start().await });

    db.set_source_gate(true);
    db.coordinated_recovery_fenced
        .store(true, Ordering::Release);
    drop(lifecycle_claim);

    let error = tokio::time::timeout(Duration::from_secs(2), starting)
        .await
        .expect("public start remained blocked behind the recovery fence")
        .expect("public start task panicked")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("pipeline start is fenced by coordinated recovery"));
    assert_eq!(DbState::load(&db.state), DbState::Created);

    db.release_coordinated_recovery_lifecycle();
    db.shutdown().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_recovery_authority_can_stop_or_start_while_fenced() {
    let db = LaminarDB::open().unwrap();
    db.start().await.unwrap();
    db.fence_coordinated_recovery_lifecycle();

    let stop_error = db.stop_pipeline().await.unwrap_err();
    assert!(stop_error
        .to_string()
        .contains("pipeline stop is fenced by coordinated recovery"));
    assert_eq!(DbState::load(&db.state), DbState::Running);

    db.stop_pipeline_for_coordinated_recovery().await.unwrap();
    assert_eq!(DbState::load(&db.state), DbState::Created);

    let start_error = db.start().await.unwrap_err();
    assert!(start_error
        .to_string()
        .contains("pipeline start is fenced by coordinated recovery"));
    db.start_for_coordinated_recovery().await.unwrap();
    assert_eq!(DbState::load(&db.state), DbState::Running);

    db.release_coordinated_recovery_lifecycle();
    db.shutdown().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn terminal_halt_stays_fenced_and_refuses_coordinated_recovery_start() {
    let db = LaminarDB::open().unwrap();
    let terminal_reason = "permanent shuffle terminal";
    DbState::Faulted.store(&db.state);
    *db.last_fault.lock() = Some(terminal_reason.into());
    db.terminal_pipeline_halt.store(true, Ordering::Release);
    db.set_source_gate(true);
    db.coordinated_recovery_fenced
        .store(true, Ordering::Release);

    db.release_coordinated_recovery_lifecycle();

    assert!(db.coordinated_recovery_fenced.load(Ordering::Acquire));
    assert!(db.source_gate.load(Ordering::Acquire));
    let error = db.start_for_coordinated_recovery().await.unwrap_err();
    match error {
        DbError::PipelineTerminal(message) => {
            assert!(message.contains(terminal_reason), "{message}");
        }
        other => panic!("expected a permanent pipeline halt, got {other}"),
    }
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
    assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), 0);

    // Restore the synthetic test-only state so ordinary database shutdown can complete cleanly.
    db.terminal_pipeline_halt.store(false, Ordering::Release);
    db.coordinated_recovery_fenced
        .store(false, Ordering::Release);
    *db.last_fault.lock() = None;
    db.shutdown().await.unwrap();
}

#[cfg(feature = "cluster")]
#[test]
fn terminal_latches_linearize_with_source_gate_openers() {
    let db = LaminarDB::open().unwrap();
    let transition = Arc::clone(&db.cluster_authority_transition);
    let opener = transition.lock();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(1);
    let terminal_db = Arc::clone(&db);
    let terminal = std::thread::spawn(move || {
        entered_tx.send(()).unwrap();
        terminal_db.latch_durable_terminal_recovery_fence();
        finished_tx.send(()).unwrap();
    });
    entered_rx.recv().unwrap();
    assert!(finished_rx.try_recv().is_err());

    // The already-authorized opener linearizes first. The waiting terminal publication must then
    // close the gate before it can return.
    db.source_gate.store(false, Ordering::SeqCst);
    drop(opener);
    finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("terminal fence must acquire the released authority transition");
    terminal.join().unwrap();
    assert!(db.cluster_intake_fenced());

    // In the reverse ordering, the opener observes the monotonic latch while holding the same
    // transition and cannot publish an open gate.
    db.set_source_gate(false);
    assert!(db.cluster_intake_fenced());

    let local_db = LaminarDB::open().unwrap();
    local_db.latch_local_terminal_pipeline_halt();
    local_db.set_source_gate(false);
    assert!(local_db.cluster_intake_fenced());
}

#[test]
fn prelatched_terminal_halt_cannot_complete_startup_successfully() {
    let db = LaminarDB::open().unwrap();
    db.terminal_pipeline_halt.store(true, Ordering::SeqCst);
    *db.last_fault.lock() = Some("halt raced startup completion".into());

    assert!(matches!(
        db.normalize_start_result_for_terminal_latch(Ok(())),
        Err(DbError::PipelineTerminal(reason)) if reason == "halt raced startup completion"
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn topology_ddl_is_rejected_while_recovery_owns_lifecycle() {
    let db = LaminarDB::open().unwrap();
    db.fence_coordinated_recovery_lifecycle();

    let error = db
        .execute("CREATE SOURCE blocked_source (id BIGINT)")
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("pipeline database mutation is fenced by coordinated recovery"));

    db.release_coordinated_recovery_lifecycle();
    db.execute("CREATE SOURCE blocked_source (id BIGINT)")
        .await
        .unwrap();
    db.shutdown().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn all_public_database_mutations_are_rejected_while_recovery_owns_lifecycle() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE retained (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    db.fence_coordinated_recovery_lifecycle();

    for sql in [
        "INSERT INTO retained VALUES (1)",
        "SET application_name = 'blocked'",
        "CHECKPOINT",
    ] {
        let error = db.execute(sql).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("pipeline database mutation is fenced by coordinated recovery"),
            "{sql}: {error}"
        );
    }
    let error = db.checkpoint().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("pipeline manual checkpoint is fenced by coordinated recovery"));

    db.release_coordinated_recovery_lifecycle();
    db.execute("INSERT INTO retained VALUES (1)").await.unwrap();
    db.shutdown().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinated_recovery_supervisor_is_owned_and_replaces_a_terminated_task() {
    let controller = sink_open_authority(1);
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .build()
        .await
        .unwrap();
    let runtime = db.control_runtime.handle().unwrap();
    let finished = runtime.spawn(async {});
    tokio::time::timeout(Duration::from_secs(2), async {
        while !finished.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("injected recovery task did not terminate");
    *db.recovery_monitor.lock() = Some(finished);

    db.enable_coordinated_recovery().unwrap();
    assert!(db
        .recovery_monitor
        .lock()
        .as_ref()
        .is_some_and(|monitor| !monitor.is_finished()));

    db.shutdown().await.unwrap();
    assert!(db.recovery_monitor.lock().is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_without_checkpoint_config_still_derives_graph_identity() {
    let controller = sink_open_authority(1);
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .build()
        .await
        .unwrap();
    assert!(db.config.checkpoint.is_none());

    let identity = db
        .initialize_checkpointing(
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            RuntimeMode::Cluster,
            db.cluster_checkpoint_object_store(),
        )
        .await
        .unwrap();

    assert!(identity.is_some());
    assert!(db.coordinator.lock().await.is_none());
    db.shutdown().await.unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn coordinated_recovery_supervisor_rejects_local_runtime() {
    let db = LaminarDB::open().unwrap();

    let error = db.enable_coordinated_recovery().unwrap_err();

    assert!(error
        .to_string()
        .contains("coordinated recovery requires a cluster runtime"));
    assert!(db.recovery_monitor.lock().is_none());
    db.shutdown().await.unwrap();
}

#[test]
fn source_contract_admission_matrix_is_fail_closed() {
    let consistencies = [
        SourceConsistency::Ephemeral,
        SourceConsistency::Replayable,
        SourceConsistency::CommitCoupled,
    ];
    let topologies = [
        SourceTopology::Singleton,
        SourceTopology::Splittable,
        SourceTopology::NodeLocalIngress,
    ];
    let deliveries = [
        DeliveryGuarantee::BestEffort,
        DeliveryGuarantee::AtLeastOnce,
        DeliveryGuarantee::ExactlyOnce,
    ];
    let runtimes = [RuntimeMode::Local, RuntimeMode::Cluster];
    let certifications = [false, true];

    for consistency in consistencies {
        for topology in topologies {
            for delivery in deliveries {
                for runtime in runtimes {
                    for checkpointing_enabled in [false, true] {
                        for certified in certifications {
                            let mut contract = if certified {
                                laminar_connectors::generator::GeneratorSource::default()
                                    .contract(&ConnectorConfig::new("generator"))
                                    .expect("static generator contract")
                            } else {
                                SourceContract::new(
                                    consistency,
                                    topology,
                                    SourceInputMode::AppendOnly,
                                )
                            };
                            contract.consistency = consistency;
                            contract.topology = topology;
                            let expected = match consistency {
                                SourceConsistency::Ephemeral => {
                                    delivery == DeliveryGuarantee::BestEffort
                                }
                                SourceConsistency::Replayable => true,
                                SourceConsistency::CommitCoupled => {
                                    delivery == DeliveryGuarantee::AtLeastOnce
                                        && checkpointing_enabled
                                }
                            };
                            let expected = expected
                                && (delivery != DeliveryGuarantee::ExactlyOnce || certified)
                                && !(runtime == RuntimeMode::Cluster
                                    && delivery == DeliveryGuarantee::BestEffort)
                                && (runtime != RuntimeMode::Cluster
                                    || topology == SourceTopology::Splittable);

                            assert_eq!(
                                admit_source_contract(
                                    contract,
                                    false,
                                    false,
                                    delivery,
                                    checkpointing_enabled,
                                    runtime,
                                )
                                .is_ok(),
                                expected,
                                "contract={contract:?}, delivery={delivery:?}, \
                                 checkpointing_enabled={checkpointing_enabled}, \
                                 runtime={runtime:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn cluster_best_effort_is_rejected_before_source_topology() {
    let contract = SourceContract::new(
        SourceConsistency::Replayable,
        SourceTopology::Singleton,
        SourceInputMode::AppendOnly,
    );
    let error = admit_source_contract(
        contract,
        false,
        false,
        DeliveryGuarantee::BestEffort,
        true,
        RuntimeMode::Cluster,
    )
    .unwrap_err();
    assert_eq!(error, CLUSTER_BEST_EFFORT);
}

#[test]
fn commit_coupled_exactly_once_requires_a_certified_barrier_cut() {
    let mut contract = laminar_connectors::generator::GeneratorSource::default()
        .contract(&ConnectorConfig::new("generator"))
        .expect("static generator contract");
    contract.consistency = SourceConsistency::CommitCoupled;
    let error = admit_source_contract(
        contract,
        false,
        false,
        DeliveryGuarantee::ExactlyOnce,
        true,
        RuntimeMode::Local,
    )
    .unwrap_err();

    assert!(error.contains("certified in-flight transaction/barrier checkpoint cut"));
}

#[test]
fn deterministic_generator_is_admitted_for_local_exact_delivery() {
    let source = laminar_connectors::generator::GeneratorSource::default();
    let contract = source
        .contract(&ConnectorConfig::new("generator"))
        .expect("static generator contract");

    assert!(contract.is_exact_delivery_certified());
    admit_source_contract(
        contract,
        false,
        false,
        DeliveryGuarantee::ExactlyOnce,
        true,
        RuntimeMode::Local,
    )
    .expect("the certified deterministic generator must remain locally admissible");
}

#[test]
fn uncertified_replayable_source_is_rejected_for_exact_delivery() {
    let contract = SourceContract::new(
        SourceConsistency::Replayable,
        SourceTopology::Splittable,
        SourceInputMode::AppendOnly,
    );
    assert!(!contract.is_exact_delivery_certified());
    let error = admit_source_contract(
        contract,
        false,
        false,
        DeliveryGuarantee::ExactlyOnce,
        true,
        RuntimeMode::Local,
    )
    .expect_err("uncertified source recovery must fail closed for exact delivery");
    assert!(error.contains(laminar_core::error_codes::EXACTLY_ONCE_SOURCE_UNCERTIFIED));
}

#[test]
fn mutation_sources_fail_before_connector_io() {
    let keyed = SourceContract::new(
        SourceConsistency::Replayable,
        SourceTopology::Splittable,
        SourceInputMode::KeyedUpsert,
    );
    assert_eq!(
        admit_source_contract(
            keyed,
            false,
            false,
            DeliveryGuarantee::AtLeastOnce,
            true,
            RuntimeMode::Local,
        )
        .unwrap_err(),
        KEYED_SOURCE_PRIMARY_KEY
    );
    assert_eq!(
        admit_source_contract(
            keyed,
            true,
            false,
            DeliveryGuarantee::AtLeastOnce,
            true,
            RuntimeMode::Local,
        )
        .unwrap_err(),
        MUTATION_SOURCE_NOT_ADMITTED
    );

    let changelog = SourceContract::new(
        SourceConsistency::Replayable,
        SourceTopology::Splittable,
        SourceInputMode::FullChangelog,
    );
    assert_eq!(
        admit_source_contract(
            changelog,
            false,
            false,
            DeliveryGuarantee::AtLeastOnce,
            true,
            RuntimeMode::Local,
        )
        .unwrap_err(),
        MUTATION_SOURCE_NOT_ADMITTED
    );

    let append_only = SourceContract::new(
        SourceConsistency::Replayable,
        SourceTopology::Splittable,
        SourceInputMode::AppendOnly,
    );
    assert_eq!(
        admit_source_contract(
            append_only,
            false,
            true,
            DeliveryGuarantee::AtLeastOnce,
            true,
            RuntimeMode::Local,
        )
        .unwrap_err(),
        MUTATION_SOURCE_NOT_ADMITTED
    );
}

#[test]
fn mutable_interval_weight_reference_scan_allows_wildcards_only() {
    assert!(!crate::sql_analysis::query_references_weight(
        "SELECT l.* FROM left_events l JOIN right_events r ON l.id = r.id"
    ));
    assert!(crate::sql_analysis::query_references_weight(
        "SELECT l.__weight AS weight_copy FROM left_events l JOIN right_events r ON l.id = r.id"
    ));
    assert!(crate::sql_analysis::query_references_weight(
        "SELECT l.id AS __WEIGHT FROM left_events l JOIN right_events r ON l.id = r.id"
    ));
    assert!(crate::sql_analysis::query_references_weight(
        "SELECT l.id AS id FROM left_events l JOIN right_events r ON l.id = r.id WHERE r.__WEIGHT > 0"
    ));
}

#[tokio::test]
async fn append_only_derived_interval_inputs_skip_mutation_admission() {
    let db = LaminarDB::open().unwrap();
    let mut config = laminar_sql::translator::StreamJoinConfig::new(
        laminar_sql::parser::join_parser::JoinType::Inner,
        vec!["id".into()],
        vec!["id".into()],
        Duration::from_secs(1),
    );
    config.left_table = "derived_left".into();
    config.right_table = "derived_right".into();
    config.left_time_column = "ts".into();
    config.right_time_column = "ts".into();
    let streams = HashMap::from([(
        "joined".into(),
        crate::connector_manager::StreamRegistration {
            name: "joined".into(),
            query_sql: "SELECT l.id AS id FROM derived_left l JOIN derived_right r \
                        ON l.id = r.id AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND"
                .into(),
            emit_clause: None,
            window_config: None,
            order_config: None,
            join_config: Some(vec![
                laminar_sql::translator::JoinOperatorConfig::StreamStream(config),
            ]),
            has_analytic: false,
            has_frame: false,
            incremental: false,
            subscription_output: None,
            subscription_retention_bytes: 0,
            catalog_generation: 1,
            subscription_certificate: None,
        },
    )]);

    let admitted = db
        .validate_persisted_interval_source_contracts(
            &HashMap::new(),
            &HashMap::new(),
            &streams,
            RuntimeMode::Local,
        )
        .await
        .unwrap();
    assert!(admitted.joins.is_empty());
    assert!(admitted.source_modes.is_empty());
}

#[tokio::test]
async fn keyed_interval_admission_requires_complete_pk_and_exclusive_consumers() {
    use crate::operator::interval_join_input::BoundedJoinInputMode;

    let (db, _) = temporal_test_db().await;
    let connector = crate::temporal_test_source::CONNECTOR_NAME;
    db.execute(&format!(
        "CREATE SOURCE interval_left (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
         FROM \"{connector}\" ('mode' = 'append')"
    ))
    .await
    .unwrap();
    db.execute(&format!(
        "CREATE SOURCE interval_right (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, PRIMARY KEY (id, ts), WATERMARK FOR ts AS ts) \
         FROM \"{connector}\" ('mode' = 'upsert')"
    ))
    .await
    .unwrap();
    let error = db
        .execute(
            "CREATE STREAM nested_interval AS \
             SELECT EXISTS (SELECT 1 FROM nested_dim d WHERE d.id = l.id) AS present \
             FROM interval_left l JOIN interval_left r ON l.id = r.id \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("subquer"), "{error}");
    assert!(!db.catalog_namespace.lock().contains_key("nested_interval"));
    db.execute(
        "CREATE STREAM mutable_join AS \
         SELECT l.id AS id, l.value AS left_value, r.value AS right_value \
         FROM interval_left l JOIN interval_right r ON l.id = r.id \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND",
    )
    .await
    .unwrap();

    let (sources, sinks, streams) = {
        let manager = db.connector_manager.lock();
        (
            manager.sources().clone(),
            manager.sinks().clone(),
            manager.streams().clone(),
        )
    };
    let admitted = db
        .validate_persisted_interval_source_contracts(
            &sources,
            &sinks,
            &streams,
            RuntimeMode::Local,
        )
        .await
        .unwrap();
    assert_eq!(
        admitted.source_modes.get("interval_right"),
        Some(&SourceInputMode::KeyedUpsert)
    );
    let modes = admitted.joins.get("mutable_join").unwrap();
    assert!(matches!(&modes[0], BoundedJoinInputMode::AppendOnly));
    assert!(matches!(
        &modes[1],
        BoundedJoinInputMode::KeyedUpsert { primary_key_indices }
            if primary_key_indices == &[0, 1]
    ));

    let error = db
        .execute("CREATE STREAM invalid_copy AS SELECT id AS id FROM interval_right")
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("consumer outside its admitted bounded interval joins"));

    let error = db
        .execute(
            "CREATE STREAM volatile_copy AS \
             SELECT id, random() AS nonce FROM mutable_join",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("not replay-immutable"),
        "{error}"
    );
    assert!(!db.catalog_namespace.lock().contains_key("volatile_copy"));

    let error = db
        .execute(
            "CREATE MATERIALIZED VIEW rejected_top_snapshot AS \
             SELECT id, SUM(left_value) AS total FROM mutable_join GROUP BY id \
             ORDER BY total DESC LIMIT 1",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("ordering or row limits")
            || error
                .to_string()
                .contains("managed streaming aggregates require"),
        "{error}"
    );
    assert!(!db
        .catalog_namespace
        .lock()
        .contains_key("rejected_top_snapshot"));

    db.execute(
        "CREATE MATERIALIZED VIEW mutable_snapshot AS \
         SELECT id, left_value, right_value FROM mutable_join",
    )
    .await
    .unwrap();
    assert!(db
        .connector_manager
        .lock()
        .streams()
        .get("mutable_snapshot")
        .is_some_and(|registration| registration.incremental));

    db.start().await.unwrap();
    db.execute(
        "CREATE STREAM late_append_interval AS \
         SELECT l.id AS id FROM interval_left l JOIN interval_left r ON l.id = r.id \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND",
    )
    .await
    .unwrap();
    assert!(db
        .catalog_namespace
        .lock()
        .contains_key("late_append_interval"));
    let error = db
        .execute("CREATE STREAM late_copy AS SELECT id FROM mutable_join")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("creates or consumes") && error.to_string().contains("stop"),
        "{error}"
    );
    assert!(!db.catalog_namespace.lock().contains_key("late_copy"));
    let error = db
        .execute("DROP STREAM mutable_join CASCADE")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("running fixed startup topology"),
        "{error}"
    );
    db.shutdown().await.unwrap();

    let (invalid, _) = temporal_test_db().await;
    invalid
        .execute(&format!(
            "CREATE SOURCE invalid_left (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
             value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
             FROM \"{connector}\" ('mode' = 'append')"
        ))
        .await
        .unwrap();
    invalid
        .execute(&format!(
            "CREATE SOURCE invalid_right (id BIGINT PRIMARY KEY, ts TIMESTAMP NOT NULL, \
             value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
             FROM \"{connector}\" ('mode' = 'upsert')"
        ))
        .await
        .unwrap();
    let error = invalid
        .execute(
            "CREATE STREAM invalid_pk_join AS \
             SELECT l.id AS id FROM invalid_left l JOIN invalid_right r ON l.id = r.id \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND",
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("PRIMARY KEY must include join/event-time column 'ts'"));
}

#[tokio::test]
async fn persisted_mutable_enrich_rejects_on_demand_lookup_provenance() {
    let (db, _) = temporal_test_db().await;
    let connector = crate::temporal_test_source::CONNECTOR_NAME;
    db.execute(&format!(
        "CREATE SOURCE lookup_left (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
         FROM \"{connector}\" ('mode' = 'append')"
    ))
    .await
    .unwrap();
    db.execute(&format!(
        "CREATE SOURCE lookup_right (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, PRIMARY KEY (id, ts), WATERMARK FOR ts AS ts) \
         FROM \"{connector}\" ('mode' = 'upsert')"
    ))
    .await
    .unwrap();
    db.execute(
        "CREATE STREAM lookup_changes AS \
         SELECT l.id AS id, l.value AS left_value, r.value AS right_value \
         FROM lookup_left l JOIN lookup_right r ON l.id = r.id \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE LOOKUP TABLE dimensions (id BIGINT NOT NULL, label VARCHAR, \
         PRIMARY KEY (id)) WITH ('connector' = 'static')",
    )
    .await
    .unwrap();
    db.connector_manager
        .lock()
        .register_table(crate::connector_manager::TableRegistration {
            name: "dimensions".into(),
            primary_key: "id".into(),
            connector_type: Some("static".into()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            on_demand: true,
            cache_max_bytes: Some(1024),
            cache_ttl: None,
        });
    let unsafe_sql = "SELECT d.label, c.* FROM lookup_changes c \
                      JOIN dimensions d ON c.id = d.id";
    let error = db
        .execute(&format!(
            "CREATE STREAM rejected_direct_enrich AS {unsafe_sql}"
        ))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot safely consume a changelog"),
        "{error}"
    );
    assert!(!db
        .catalog_namespace
        .lock()
        .contains_key("rejected_direct_enrich"));

    db.connector_manager
        .lock()
        .register_stream(crate::connector_manager::StreamRegistration {
            name: "unsafe_direct_enrich".into(),
            query_sql: unsafe_sql.into(),
            emit_clause: None,
            window_config: None,
            order_config: None,
            join_config: None,
            has_analytic: false,
            has_frame: false,
            incremental: false,
            subscription_output: None,
            subscription_retention_bytes: 0,
            catalog_generation: 1,
            subscription_certificate: None,
        });

    let error = db.start().await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot safely consume a changelog"),
        "{error}"
    );
}

#[tokio::test]
async fn full_changelog_source_and_interval_admission_are_contract_gated() {
    use crate::operator::interval_join_input::BoundedJoinInputMode;

    let db = ordered_full_changelog_test_db().await;
    db.execute(&format!(
        "CREATE SOURCE weighted_left (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, __weight BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
         FROM \"{ORDERED_FULL_CHANGELOG_CONNECTOR}\""
    ))
    .await
    .unwrap();
    let append_connector = crate::temporal_test_source::CONNECTOR_NAME;
    db.execute(&format!(
        "CREATE SOURCE weighted_right (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
         FROM \"{append_connector}\" ('mode' = 'append')"
    ))
    .await
    .unwrap();
    db.execute(
        "CREATE STREAM weighted_join AS \
         SELECT l.id AS id, l.value AS left_value, r.value AS right_value \
         FROM weighted_left l JOIN weighted_right r ON l.id = r.id \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND",
    )
    .await
    .unwrap();
    db.execute("CREATE TABLE weighted_dimensions (id BIGINT PRIMARY KEY, label VARCHAR)")
        .await
        .unwrap();
    db.execute(
        "CREATE STREAM weighted_enriched AS \
         SELECT j.id AS id, d.label AS label FROM weighted_join j \
         JOIN weighted_dimensions d ON j.id = d.id",
    )
    .await
    .unwrap();
    assert!(db
        .connector_manager
        .lock()
        .streams()
        .contains_key("weighted_enriched"));

    for (name, query) in [
        (
            "weighted_distinct",
            "SELECT DISTINCT l.id AS id, l.value AS left_value, r.value AS right_value \
             FROM weighted_left l JOIN weighted_right r ON l.id = r.id \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND",
        ),
        (
            "weighted_top_one",
            "SELECT l.id AS id, l.value AS left_value, r.value AS right_value \
             FROM weighted_left l JOIN weighted_right r ON l.id = r.id \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND \
             ORDER BY id LIMIT 1",
        ),
    ] {
        let error = db
            .execute(&format!("CREATE STREAM {name} AS {query}"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("row-set modifiers")
                || error
                    .to_string()
                    .contains("cannot contain an aggregate stage"),
            "{error}"
        );
        assert!(!db.catalog_namespace.lock().contains_key(name));
    }

    let (sources, sinks, streams) = {
        let manager = db.connector_manager.lock();
        (
            manager.sources().clone(),
            manager.sinks().clone(),
            manager.streams().clone(),
        )
    };
    let admitted = db
        .validate_persisted_interval_source_contracts(
            &sources,
            &sinks,
            &streams,
            RuntimeMode::Local,
        )
        .await
        .unwrap();
    assert_eq!(
        admitted.source_modes.get("weighted_left"),
        Some(&SourceInputMode::FullChangelog)
    );
    let modes = &admitted.joins["weighted_join"];
    assert!(matches!(
        (&modes[0], &modes[1]),
        (
            BoundedJoinInputMode::FullChangelog,
            BoundedJoinInputMode::AppendOnly
        )
    ));

    let error = db
        .execute(&format!(
            "CREATE SOURCE missing_weight (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
             value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
             FROM \"{ORDERED_FULL_CHANGELOG_CONNECTOR}\""
        ))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("requires exact trailing non-null BIGINT '__weight'"));

    let error = db
        .execute(&format!(
            "CREATE SOURCE spoofed_weight (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
             value BIGINT NOT NULL, __weight BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
             FROM \"{append_connector}\" ('mode' = 'append')"
        ))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("connector input mode is AppendOnly"));

    // Persisted metadata is revalidated independently of the DDL path. First prove that an older
    // row-limiting query is rejected by the same admission method startup invokes.
    let mut row_limited_streams = streams.clone();
    row_limited_streams
        .get_mut("weighted_join")
        .unwrap()
        .query_sql = "SELECT l.id AS id, l.value AS left_value, r.value AS right_value \
         FROM weighted_left l JOIN weighted_right r ON l.id = r.id \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND \
         ORDER BY id LIMIT 1"
        .into();
    let error = db
        .validate_persisted_interval_source_contracts(
            &sources,
            &sinks,
            &row_limited_streams,
            RuntimeMode::Local,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("row-set modifiers"), "{error}");

    let mut aggregate_streams = streams.clone();
    aggregate_streams
        .get_mut("weighted_join")
        .unwrap()
        .query_sql = "SELECT COUNT(l.id) AS row_count \
         FROM weighted_left l JOIN weighted_right r ON l.id = r.id \
         AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND"
        .into();
    let error = db
        .validate_persisted_interval_source_contracts(
            &sources,
            &sinks,
            &aggregate_streams,
            RuntimeMode::Local,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("aggregate stage"), "{error}");

    // Then inject an unaliased persisted projection and require actual startup to fail before
    // source intake rather than deferring the schema disagreement to the first data cycle.
    {
        let mut manager = db.connector_manager.lock();
        let mut registration = manager.streams()["weighted_join"].clone();
        registration.query_sql = "SELECT l.id, l.value AS left_value, r.value AS right_value \
             FROM weighted_left l JOIN weighted_right r ON l.id = r.id \
             AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND"
            .into();
        manager.register_stream(registration);
    }
    let error = db.start().await.unwrap_err();
    assert!(error.to_string().contains("explicit alias"), "{error}");
}

#[test]
fn temporal_source_contracts_enforce_recovery_positions_and_input_roles() {
    let positioned = |input_mode| {
        SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            input_mode,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
    };

    admit_temporal_source_contract(
        positioned(SourceInputMode::AppendOnly),
        TemporalSourceRole::Left,
        false,
        false,
        DeliveryGuarantee::BestEffort,
        false,
        RuntimeMode::Local,
    )
    .expect("local in-memory temporal execution may be best-effort");

    let error = admit_temporal_source_contract(
        positioned(SourceInputMode::AppendOnly),
        TemporalSourceRole::Left,
        false,
        false,
        DeliveryGuarantee::AtLeastOnce,
        false,
        RuntimeMode::Local,
    )
    .unwrap_err();
    assert!(error.contains("require checkpointing"));

    let unavailable = SourceContract::new(
        SourceConsistency::Replayable,
        SourceTopology::Splittable,
        SourceInputMode::AppendOnly,
    );
    let error = admit_temporal_source_contract(
        unavailable,
        TemporalSourceRole::Left,
        false,
        false,
        DeliveryGuarantee::AtLeastOnce,
        true,
        RuntimeMode::Local,
    )
    .unwrap_err();
    assert!(error.contains("ordered deterministic row positions"));

    let keyed = positioned(SourceInputMode::KeyedUpsert);
    admit_temporal_source_contract(
        keyed,
        TemporalSourceRole::Right,
        true,
        false,
        DeliveryGuarantee::AtLeastOnce,
        true,
        RuntimeMode::Local,
    )
    .expect("keyed-upsert history is a supported temporal-right route");
    assert_eq!(
        admit_temporal_source_contract(
            keyed,
            TemporalSourceRole::Left,
            true,
            false,
            DeliveryGuarantee::AtLeastOnce,
            true,
            RuntimeMode::Local,
        )
        .unwrap_err(),
        "temporal left inputs must be append-only"
    );
    let full_changelog = positioned(SourceInputMode::FullChangelog);
    assert!(admit_temporal_source_contract(
        full_changelog,
        TemporalSourceRole::Right,
        true,
        false,
        DeliveryGuarantee::AtLeastOnce,
        true,
        RuntimeMode::Local,
    )
    .unwrap_err()
    .contains("not full changelogs"));
}

fn temporal_stream_registration(name: &str) -> crate::connector_manager::StreamRegistration {
    crate::connector_manager::StreamRegistration {
        name: name.into(),
        query_sql: "SELECT * FROM trades JOIN prices FOR SYSTEM_TIME AS OF trades.ts_ms ON trades.seq = prices.seq".into(),
        emit_clause: None,
        window_config: None,
        order_config: None,
        join_config: Some(vec![laminar_sql::translator::JoinOperatorConfig::Temporal(
            laminar_sql::translator::TemporalJoinTranslatorConfig {
                left_table: "trades".into(),
                right_table: "prices".into(),
                left_key_columns: vec!["seq".into()],
                right_key_columns: vec!["seq".into()],
                left_time_column: "ts_ms".into(),
                right_time_column: "ts_ms".into(),
                join_kind: laminar_sql::temporal::TemporalJoinKind::Inner,
                probe_schedule: laminar_sql::temporal::TemporalProbeSchedule::as_of(),
                probe_alias: None,
            },
        )]),
        has_analytic: false,
        has_frame: false,
        incremental: false,
        subscription_output: None,
        subscription_retention_bytes: 0,
        catalog_generation: 1,
        subscription_certificate: None,
    }
}

#[test]
fn temporal_mutation_routes_cannot_share_the_source_with_other_consumers() {
    let mut streams = HashMap::from([("joined".into(), temporal_stream_registration("joined"))]);
    let sinks = HashMap::new();
    assert!(has_only_temporal_right_consumers(
        "prices", &streams, &sinks
    ));

    let mut left_consumer = temporal_stream_registration("reverse_join");
    left_consumer.query_sql = "managed temporal plan".into();
    let Some([laminar_sql::translator::JoinOperatorConfig::Temporal(config)]) =
        left_consumer.join_config.as_deref_mut()
    else {
        unreachable!()
    };
    config.left_table = "prices".into();
    config.right_table = "profiles".into();
    streams.insert("reverse_join".into(), left_consumer);
    assert!(!has_only_temporal_right_consumers(
        "prices", &streams, &sinks
    ));
    streams.remove("reverse_join");

    streams.insert(
        "copied".into(),
        crate::connector_manager::StreamRegistration {
            name: "copied".into(),
            query_sql: "SELECT * FROM prices".into(),
            emit_clause: None,
            window_config: None,
            order_config: None,
            join_config: None,
            has_analytic: false,
            has_frame: false,
            incremental: false,
            subscription_output: None,
            subscription_retention_bytes: 0,
            catalog_generation: 1,
            subscription_certificate: None,
        },
    );
    assert!(!has_only_temporal_right_consumers(
        "prices", &streams, &sinks
    ));
}

#[tokio::test]
async fn persisted_temporal_preflight_requires_direct_event_time_sources() {
    let (db, _) = temporal_test_db().await;
    let schema = crate::temporal_test_source::schema();
    for (name, primary_key) in [("trades", Vec::new()), ("prices", vec!["id".into()])] {
        db.catalog
            .register_source(
                name,
                Arc::clone(&schema),
                primary_key,
                Some("ts".into()),
                Some(Duration::ZERO),
                None,
                None,
            )
            .unwrap();
    }
    let source = |name: &str, mode: &str| crate::connector_manager::SourceRegistration {
        name: name.into(),
        connector_type: Some(crate::temporal_test_source::CONNECTOR_NAME.into()),
        connector_options: HashMap::from([("mode".into(), mode.into())]),
        format: None,
        format_options: HashMap::new(),
    };
    let mut sources = HashMap::from([
        ("trades".into(), source("trades", "append")),
        ("prices".into(), source("prices", "upsert")),
    ]);
    let mut temporal_stream = temporal_stream_registration("joined");
    let Some([laminar_sql::translator::JoinOperatorConfig::Temporal(config)]) =
        temporal_stream.join_config.as_deref_mut()
    else {
        unreachable!()
    };
    config.left_key_columns = vec!["id".into()];
    config.right_key_columns = vec!["id".into()];
    config.left_time_column = "ts".into();
    config.right_time_column = "ts".into();
    let streams = HashMap::from([("joined".into(), temporal_stream)]);
    let sinks = HashMap::new();

    let admitted = db
        .validate_persisted_temporal_source_contracts(
            &sources,
            &sinks,
            &streams,
            RuntimeMode::Local,
        )
        .unwrap();
    assert_eq!(admitted.get("trades"), Some(&TemporalSourceRole::Left));
    assert_eq!(admitted.get("prices"), Some(&TemporalSourceRole::Right));

    sources.get_mut("prices").unwrap().connector_type = None;
    let error = db
        .validate_persisted_temporal_source_contracts(
            &sources,
            &sinks,
            &streams,
            RuntimeMode::Local,
        )
        .unwrap_err();
    assert!(error.to_string().contains("direct configured source"));

    sources.get_mut("prices").unwrap().connector_type =
        Some(crate::temporal_test_source::CONNECTOR_NAME.into());
    db.catalog
        .get_source("prices")
        .unwrap()
        .is_processing_time
        .store(true, std::sync::atomic::Ordering::Release);
    let error = db
        .validate_persisted_temporal_source_contracts(
            &sources,
            &sinks,
            &streams,
            RuntimeMode::Local,
        )
        .unwrap_err();
    assert!(error.to_string().contains("must use event time"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_temporal_stream_starts_and_emits_with_positioned_microsecond_sources() {
    let (db, control) = temporal_test_db().await;
    create_temporal_test_inputs(&db).await.unwrap();
    create_temporal_test_stream(&db, "matched").await.unwrap();

    db.start().await.unwrap();
    let mut subscription = db
        .open_subscription("matched", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();
    control.release();
    let batch = next_temporal_output(&mut subscription).await;

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().field(0).name(), "id");
    assert_eq!(batch.schema().field(1).name(), "left_value");
    assert_eq!(batch.schema().field(2).name(), "right_value");
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    db.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_temporal_alo_clean_reopen_recovers_history_and_source_positions() {
    let storage = tempfile::tempdir().unwrap();
    let control = crate::temporal_test_source::TemporalTestSourceControl::new();

    let db = persistent_temporal_alo_test_db(storage.path(), &control).await;
    create_temporal_test_inputs(&db).await.unwrap();
    create_temporal_test_stream(&db, "matched").await.unwrap();
    db.start().await.unwrap();
    let mut subscription = db
        .open_subscription("matched", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();
    control.release();
    let initial = next_temporal_output(&mut subscription).await;
    assert_eq!(
        initial
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    let checkpoint = db.checkpoint().await.unwrap();
    assert!(
        checkpoint.success && checkpoint.error.is_none(),
        "temporal checkpoint did not commit cleanly: {checkpoint:?}"
    );
    db.shutdown().await.unwrap();

    let db = persistent_temporal_alo_test_db(storage.path(), &control).await;
    create_temporal_test_inputs(&db).await.unwrap();
    create_temporal_test_stream(&db, "matched").await.unwrap();
    db.start().await.unwrap();
    assert_eq!(control.start_cursor("left_events"), Some(1));
    assert_eq!(control.start_cursor("right_events"), Some(1));
    let mut subscription = db
        .open_subscription("matched", None, crate::subscription::SubscribeStart::Tail)
        .await
        .unwrap();
    control.release();
    let recovered = next_temporal_output(&mut subscription).await;
    assert_eq!(recovered.num_rows(), 1);
    assert_eq!(
        recovered
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        11
    );
    assert_eq!(
        recovered
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        9,
        "the post-restart probe must match history restored from the checkpoint"
    );
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn keyed_temporal_right_consumer_admission_is_ddl_order_independent() {
    let (db, _) = temporal_test_db().await;
    create_temporal_test_inputs(&db).await.unwrap();
    create_temporal_test_stream(&db, "matched").await.unwrap();

    for (name, sql) in [
        (
            "right_copy",
            "CREATE STREAM right_copy AS SELECT id, ts, value FROM right_events",
        ),
        ("right_sink", "CREATE SINK right_sink FROM right_events"),
        (
            "right_mv",
            "CREATE MATERIALIZED VIEW right_mv AS SELECT id, ts, value FROM right_events",
        ),
    ] {
        let error = db.execute(sql).await.unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("non-temporal-right consumer")
                || message.contains("exclusive to admitted stateful routes"),
            "{sql}: {error}"
        );
        assert!(!db.catalog_namespace.lock().contains_key(name));
    }
    for (name, sql) in [
        (
            "right_query_sink",
            "CREATE SINK right_query_sink FROM (SELECT id FROM right_events)",
        ),
        (
            "left_query_sink",
            "CREATE SINK left_query_sink FROM (SELECT id FROM left_events)",
        ),
    ] {
        let error = db.execute(sql).await.unwrap_err();
        assert!(
            error.to_string().contains("named internal stream"),
            "{error}"
        );
        assert!(!db.catalog_namespace.lock().contains_key(name));
        assert!(db.connector_manager.lock().get_sink(name).is_none());
    }
    assert!(db.catalog.get_stream_entry("right_copy").is_none());
    assert!(db.catalog.get_sink_input("right_sink").is_none());
    assert!(!db
        .connector_manager
        .lock()
        .streams()
        .contains_key("right_copy"));
    assert!(db.connector_manager.lock().get_sink("right_sink").is_none());
    assert!(db.mv_registry.lock().get("right_mv").is_none());
    assert!(!db.mv_store.read().has_mv("right_mv"));
    assert!(!db.ctx.table_exist("right_mv").unwrap());
    db.shutdown().await.unwrap();

    let (db, _) = temporal_test_db().await;
    create_temporal_test_inputs(&db).await.unwrap();
    let error = db
        .execute("CREATE SINK right_sink FROM right_events")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exclusive to admitted stateful routes"),
        "{error}"
    );
    assert!(db.connector_manager.lock().get_sink("right_sink").is_none());
    create_temporal_test_stream(&db, "matched").await.unwrap();
    assert!(db.catalog.get_stream_entry("matched").is_some());
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_source_creation_revalidates_preexisting_consumers_and_plain_copies() {
    let connector = crate::temporal_test_source::CONNECTOR_NAME;
    let (db, _) = temporal_test_db().await;
    db.execute("CREATE SINK future_sink FROM future_events")
        .await
        .unwrap();
    let error = db
        .execute(&format!(
            "CREATE SOURCE future_events (id BIGINT PRIMARY KEY, ts TIMESTAMP NOT NULL, \
             value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
             FROM \"{connector}\" ('mode' = 'upsert')"
        ))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exclusive to admitted stateful routes"),
        "{error}"
    );
    assert!(db.catalog.get_source("future_events").is_none());
    assert!(db
        .connector_manager
        .lock()
        .get_source("future_events")
        .is_none());
    db.shutdown().await.unwrap();

    let (db, _) = temporal_test_db().await;
    db.execute(&format!(
        "CREATE SOURCE keyed_events (id BIGINT PRIMARY KEY, ts TIMESTAMP NOT NULL, \
         value BIGINT NOT NULL, WATERMARK FOR ts AS ts) \
         FROM \"{connector}\" ('mode' = 'upsert')"
    ))
    .await
    .unwrap();
    let error = db
        .execute("CREATE STREAM keyed_copy AS SELECT * FROM keyed_events")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exactly one admitted temporal-right or bounded interval route"),
        "{error}"
    );
    assert!(db.catalog.get_stream_entry("keyed_copy").is_none());
    db.shutdown().await.unwrap();
}

#[test]
fn sink_contract_admission_matrix_is_fail_closed() {
    let consistencies = [
        SinkConsistency::Ephemeral,
        SinkConsistency::DurableAtLeastOnce,
        SinkConsistency::CheckpointCommittable,
    ];
    let topologies = [
        SinkTopology::Singleton,
        SinkTopology::MultiWriter,
        SinkTopology::NodeLocalEgress,
    ];
    let input_modes = [
        SinkInputMode::AppendOnly,
        SinkInputMode::KeyedUpsert,
        SinkInputMode::FullChangelog,
    ];
    let deliveries = [
        DeliveryGuarantee::BestEffort,
        DeliveryGuarantee::AtLeastOnce,
        DeliveryGuarantee::ExactlyOnce,
    ];
    let runtimes = [RuntimeMode::Local, RuntimeMode::Cluster];

    for consistency in consistencies {
        for topology in topologies {
            for input_mode in input_modes {
                for delivery in deliveries {
                    for runtime in runtimes {
                        for carries_changelog in [false, true] {
                            let contract = SinkContract::new(consistency, topology, input_mode);
                            let durable = delivery != DeliveryGuarantee::AtLeastOnce
                                || consistency != SinkConsistency::Ephemeral;
                            let placed = runtime != RuntimeMode::Cluster
                                || topology == SinkTopology::MultiWriter;
                            let input_compatible =
                                !carries_changelog || input_mode.accepts_full_changelog();
                            let protocol_compatible = if delivery == DeliveryGuarantee::ExactlyOnce
                            {
                                consistency == SinkConsistency::CheckpointCommittable
                            } else {
                                consistency != SinkConsistency::CheckpointCommittable
                            };
                            let expected = protocol_compatible
                                && durable
                                && placed
                                && input_compatible
                                && !(runtime == RuntimeMode::Cluster
                                    && delivery != DeliveryGuarantee::AtLeastOnce);

                            assert_eq!(
                                admit_sink_contract(
                                    contract,
                                    delivery,
                                    runtime,
                                    carries_changelog,
                                )
                                .is_ok(),
                                expected,
                                "contract={contract:?}, delivery={delivery:?}, \
                                 runtime={runtime:?}, carries_changelog={carries_changelog}"
                            );
                        }
                    }
                }
            }
        }
    }
}

struct OpenProbeSink {
    contract: SinkContract,
    opened: Arc<AtomicBool>,
    schema: SchemaRef,
    exact_protocol: bool,
}

struct StartupProbeSink {
    open_delay: Duration,
    open_error: Option<ConnectorError>,
    close_delay: Duration,
    open_calls: Arc<AtomicU64>,
    close_calls: Arc<AtomicU64>,
    schema: SchemaRef,
    cancellation_policy: ConnectorCancellationPolicy,
}

struct TrackedAdmissionSink {
    _owner: ConnectorTaskOwner,
    tracker: ConnectorTaskTracker,
    schema: SchemaRef,
}

struct TrackedPlanningSource {
    _owner: ConnectorTaskOwner,
    tracker: ConnectorTaskTracker,
    schema: SchemaRef,
}

#[async_trait]
impl SinkConnector for TrackedAdmissionSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.tracker.clone())
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ))
    }

    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for TrackedPlanningSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.tracker.clone())
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        ))
    }

    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait]
impl SinkConnector for StartupProbeSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        self.cancellation_policy
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ))
    }

    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.open_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.open_delay).await;
        self.open_error.take().map_or(Ok(()), Err)
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.close_delay).await;
        Ok(())
    }
}

fn prepared_startup_probe(
    name: &str,
    delay: Duration,
    open_calls: Arc<AtomicU64>,
    close_calls: Arc<AtomicU64>,
) -> PreparedSink {
    prepared_lifecycle_probe(name, delay, Duration::ZERO, open_calls, close_calls)
}

fn prepared_lifecycle_probe(
    name: &str,
    open_delay: Duration,
    close_delay: Duration,
    open_calls: Arc<AtomicU64>,
    close_calls: Arc<AtomicU64>,
) -> PreparedSink {
    prepared_lifecycle_probe_with_policy(
        name,
        open_delay,
        close_delay,
        open_calls,
        close_calls,
        ConnectorCancellationPolicy::CancelSafe,
    )
}

fn prepared_lifecycle_probe_with_policy(
    name: &str,
    open_delay: Duration,
    close_delay: Duration,
    open_calls: Arc<AtomicU64>,
    close_calls: Arc<AtomicU64>,
    cancellation_policy: ConnectorCancellationPolicy,
) -> PreparedSink {
    prepared_lifecycle_probe_with_error(
        name,
        open_delay,
        close_delay,
        open_calls,
        close_calls,
        cancellation_policy,
        None,
    )
}

fn prepared_lifecycle_probe_with_error(
    name: &str,
    open_delay: Duration,
    close_delay: Duration,
    open_calls: Arc<AtomicU64>,
    close_calls: Arc<AtomicU64>,
    cancellation_policy: ConnectorCancellationPolicy,
    open_error: Option<ConnectorError>,
) -> PreparedSink {
    PreparedSink {
        name: name.into(),
        connector: Box::new(StartupProbeSink {
            open_delay,
            open_error,
            close_delay,
            open_calls,
            close_calls,
            schema: Arc::new(Schema::empty()),
            cancellation_policy,
        }),
        config: ConnectorConfig::new("startup-probe"),
        filter_expr: None,
        input: "input".into(),
        contract: SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        expects_changelog: false,
        write_timeout: Duration::from_secs(1),
        flush_interval: Duration::from_secs(5),
        requires_recovery_on_error: true,
        task_fence: ConnectorTaskFenceRegistration::capture(
            Arc::<str>::from(format!("sink:{name}")),
            None,
        ),
    }
}

#[tokio::test]
async fn source_preplanning_failure_retains_captured_generation_fence() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().expect("live source child");
    let owned = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let source: Box<dyn SourceConnector> = Box::new(TrackedPlanningSource {
        _owner: owner,
        tracker,
        schema: Arc::new(Schema::empty()),
    });
    let task_fence = ConnectorTaskFenceRegistration::capture_registered(
        "source:planning-failure",
        source.terminal_task_tracker(),
        &owned,
    );
    let source = TrackedSourceRegistration::from_captured(
        crate::pipeline::SourceRegistration {
            name: "planning-failure".into(),
            connector: source,
            config: ConnectorConfig::new("planning-probe"),
            assignment_scoped: false,
            position: laminar_connectors::connector::SourcePosition::Initial,
        },
        task_fence,
    )
    .expect("append-only planning source contract");

    let result: Result<(), DbError> = async move {
        let _source = source;
        let context = datafusion::prelude::SessionContext::new();
        let mut streams = HashMap::new();
        streams.insert(
            "unresolved".into(),
            crate::connector_manager::StreamRegistration {
                name: "unresolved".into(),
                query_sql: "SELECT * FROM missing_source".into(),
                emit_clause: None,
                window_config: None,
                order_config: None,
                join_config: None,
                has_analytic: false,
                has_frame: false,
                incremental: false,
                subscription_output: None,
                subscription_retention_bytes: 0,
                catalog_generation: 1,
                subscription_certificate: None,
            },
        );
        resolve_stream_output_schemas(&context, &streams, &Default::default(), &Default::default())
            .await?;
        Ok(())
    }
    .await;

    assert!(matches!(result, Err(DbError::Pipeline(_))));
    assert_eq!(owned.lock().len(), 1);
    assert!(!owned.lock()[0].is_finished());
    drop(guard);
    assert!(owned.lock()[0].is_finished());
}

#[test]
fn sink_admission_failure_retains_captured_generation_fence() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().expect("live sink child");
    let owned = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let result: Result<(), DbError> = (|| {
        let sink: Box<dyn SinkConnector> = Box::new(TrackedAdmissionSink {
            _owner: owner,
            tracker,
            schema: Arc::new(Schema::empty()),
        });
        let _task_fence = ConnectorTaskFenceRegistration::capture_registered(
            "sink:admission-failure",
            sink.terminal_task_tracker(),
            &owned,
        );
        let config = ConnectorConfig::new("admission-probe");
        admit_sink(
            sink.as_ref(),
            SinkAdmissionContext {
                config: &config,
                name: "admission-failure",
                input: "input",
                delivery: DeliveryGuarantee::ExactlyOnce,
                runtime: RuntimeMode::Local,
                carries_changelog: false,
                checkpointing_enabled: true,
                checkpoint_storage_scope: CheckpointStorageScope::NodeDurable,
            },
        )?;
        Ok(())
    })();

    assert!(matches!(result, Err(DbError::Config(_))));
    assert_eq!(owned.lock().len(), 1);
    assert!(!owned.lock()[0].is_finished());
    drop(guard);
    assert!(owned.lock()[0].is_finished());
}

#[tokio::test(start_paused = true)]
async fn sink_open_stage_uses_one_deadline_and_rolls_back_current_and_prior() {
    let prior_open = Arc::new(AtomicU64::new(0));
    let prior_close = Arc::new(AtomicU64::new(0));
    let current_open = Arc::new(AtomicU64::new(0));
    let current_close = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![
        prepared_startup_probe(
            "prior",
            Duration::from_secs(6),
            Arc::clone(&prior_open),
            Arc::clone(&prior_close),
        ),
        prepared_startup_probe(
            "current",
            Duration::from_secs(6),
            Arc::clone(&current_open),
            Arc::clone(&current_close),
        ),
    ];

    let error = open_prepared_sinks(
        &mut sinks,
        Duration::from_secs(10),
        #[cfg(feature = "cluster")]
        None,
    )
    .await
    .expect_err("the second sink must consume the remaining shared startup budget");

    let message = error.to_string();
    assert!(
        message.contains("Failed to open sink 'current'")
            && message.contains("shared 10s sink-open stage deadline"),
        "unexpected error: {error}"
    );
    assert_eq!(prior_open.load(Ordering::SeqCst), 1);
    assert_eq!(current_open.load(Ordering::SeqCst), 1);
    assert_eq!(prior_close.load(Ordering::SeqCst), 1);
    assert_eq!(current_close.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn live_cluster_authority_rolls_back_timed_out_sink_startup() {
    let prior_open = Arc::new(AtomicU64::new(0));
    let prior_close = Arc::new(AtomicU64::new(0));
    let current_open = Arc::new(AtomicU64::new(0));
    let current_close = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![
        prepared_startup_probe(
            "prior",
            Duration::from_secs(6),
            Arc::clone(&prior_open),
            Arc::clone(&prior_close),
        ),
        prepared_startup_probe(
            "current",
            Duration::from_secs(6),
            Arc::clone(&current_open),
            Arc::clone(&current_close),
        ),
    ];
    let controller = sink_open_authority(40);

    let error = open_prepared_sinks(
        &mut sinks,
        Duration::from_secs(10),
        Some(controller.as_ref()),
    )
    .await
    .expect_err("the second sink must consume the remaining shared startup budget");

    assert!(
        error
            .to_string()
            .contains("shared 10s sink-open stage deadline"),
        "{error}"
    );
    assert!(controller.process_lease_is_live());
    assert_eq!(prior_open.load(Ordering::SeqCst), 1);
    assert_eq!(current_open.load(Ordering::SeqCst), 1);
    assert_eq!(prior_close.load(Ordering::SeqCst), 1);
    assert_eq!(current_close.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expired_sink_open_budget_never_polls_connector() {
    let open_calls = Arc::new(AtomicU64::new(0));
    let close_calls = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![prepared_startup_probe(
        "unattempted",
        Duration::ZERO,
        Arc::clone(&open_calls),
        Arc::clone(&close_calls),
    )];

    let error = open_prepared_sinks(
        &mut sinks,
        Duration::ZERO,
        #[cfg(feature = "cluster")]
        None,
    )
    .await
    .expect_err("an expired shared budget must reject before polling open");

    assert!(
        error
            .to_string()
            .contains("deadline was exhausted before open began"),
        "unexpected error: {error}"
    );
    assert_eq!(open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(close_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn timed_out_sink_open_retires_candidate_at_the_shared_deadline() {
    let open_calls = Arc::new(AtomicU64::new(0));
    let close_calls = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![prepared_lifecycle_probe_with_policy(
        "retired-open",
        Duration::from_secs(12),
        Duration::ZERO,
        Arc::clone(&open_calls),
        Arc::clone(&close_calls),
        ConnectorCancellationPolicy::RetireConnector,
    )];
    let started = tokio::time::Instant::now();

    let error = open_prepared_sinks(
        &mut sinks,
        Duration::from_secs(10),
        #[cfg(feature = "cluster")]
        None,
    )
    .await
    .expect_err("late open must still fail the startup stage");

    assert!(error
        .to_string()
        .contains("shared 10s sink-open stage deadline"));
    assert_eq!(
        tokio::time::Instant::now() - started,
        Duration::from_secs(10)
    );
    assert_eq!(open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        close_calls.load(Ordering::SeqCst),
        0,
        "a retired startup candidate must not receive a later connector call"
    );
}

#[cfg(feature = "cluster")]
fn sink_open_authority(node: u64) -> Arc<laminar_core::cluster::control::ClusterController> {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};
    use laminar_core::state::NodeId;

    let node = NodeId(node);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(laminar_core::cluster::control::ClusterController::new(
        node, kv, None, members_rx,
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    controller
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn cluster_process_lease_loss_cancels_cancel_safe_sink_open() {
    let open_calls = Arc::new(AtomicU64::new(0));
    let close_calls = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![prepared_startup_probe(
        "fenced-open",
        Duration::from_secs(60),
        Arc::clone(&open_calls),
        Arc::clone(&close_calls),
    )];
    let controller = sink_open_authority(41);
    let fencer = Arc::clone(&controller);
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        fencer.fence_process_lease();
    });
    let started = tokio::time::Instant::now();

    let error = open_prepared_sinks(
        &mut sinks,
        Duration::from_secs(60),
        Some(controller.as_ref()),
    )
    .await
    .expect_err("process lease loss must reject sink startup");

    assert!(
        error.to_string().contains("process lease expired"),
        "{error}"
    );
    assert!(tokio::time::Instant::now() - started < Duration::from_secs(60));
    assert_eq!(open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        close_calls.load(Ordering::SeqCst),
        0,
        "a fenced process must not invoke generic close because close may publish"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(start_paused = true)]
async fn cluster_process_lease_loss_retires_sink_open_without_cleanup() {
    let open_calls = Arc::new(AtomicU64::new(0));
    let close_calls = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![prepared_lifecycle_probe_with_policy(
        "retired-authority-open",
        Duration::from_secs(12),
        Duration::ZERO,
        Arc::clone(&open_calls),
        Arc::clone(&close_calls),
        ConnectorCancellationPolicy::RetireConnector,
    )];
    let controller = sink_open_authority(42);
    let fencer = Arc::clone(&controller);
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        fencer.fence_process_lease();
    });
    let started = tokio::time::Instant::now();

    let error = open_prepared_sinks(
        &mut sinks,
        Duration::from_secs(60),
        Some(controller.as_ref()),
    )
    .await
    .expect_err("lease loss must retire the in-flight open");

    assert!(
        error.to_string().contains("process lease expired"),
        "{error}"
    );
    assert!(tokio::time::Instant::now() - started < Duration::from_secs(12));
    assert_eq!(open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(close_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn outcome_unknown_sink_open_retires_candidate_without_close() {
    let open_calls = Arc::new(AtomicU64::new(0));
    let close_calls = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![prepared_lifecycle_probe_with_error(
        "unknown-open",
        Duration::ZERO,
        Duration::ZERO,
        Arc::clone(&open_calls),
        Arc::clone(&close_calls),
        ConnectorCancellationPolicy::CancelSafe,
        Some(ConnectorError::outcome_unknown(
            "remote admission acknowledgement was lost",
            true,
        )),
    )];

    let error = open_prepared_sinks(
        &mut sinks,
        Duration::from_secs(1),
        #[cfg(feature = "cluster")]
        None,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("outcome unknown"), "{error}");
    assert_eq!(open_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        close_calls.load(Ordering::SeqCst),
        0,
        "a generation with unknown startup outcome must not receive another connector call"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn expired_cluster_process_lease_never_polls_sink_open() {
    let open_calls = Arc::new(AtomicU64::new(0));
    let close_calls = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![prepared_startup_probe(
        "expired-authority",
        Duration::ZERO,
        Arc::clone(&open_calls),
        Arc::clone(&close_calls),
    )];
    let controller = sink_open_authority(43);
    controller.fence_process_lease();

    let error = open_prepared_sinks(
        &mut sinks,
        Duration::from_secs(10),
        Some(controller.as_ref()),
    )
    .await
    .expect_err("expired authority must reject before polling sink open");

    assert!(
        error.to_string().contains("process lease expired"),
        "{error}"
    );
    assert_eq!(open_calls.load(Ordering::SeqCst), 0);
    assert_eq!(close_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn sink_startup_cleanup_attempts_every_connector_with_one_shared_deadline() {
    let first_close = Arc::new(AtomicU64::new(0));
    let second_close = Arc::new(AtomicU64::new(0));
    let mut sinks = vec![
        prepared_lifecycle_probe(
            "first",
            Duration::ZERO,
            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT + Duration::from_secs(1),
            Arc::new(AtomicU64::new(0)),
            Arc::clone(&first_close),
        ),
        prepared_lifecycle_probe(
            "second",
            Duration::ZERO,
            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT + Duration::from_secs(1),
            Arc::new(AtomicU64::new(0)),
            Arc::clone(&second_close),
        ),
    ];
    let started = tokio::time::Instant::now();
    close_opened_sinks(
        &mut sinks,
        PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
        #[cfg(feature = "cluster")]
        None,
    )
    .await;

    assert_eq!(
        tokio::time::Instant::now().duration_since(started),
        PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
        "startup rollback must not multiply latency by the connector count"
    );
    assert_eq!(first_close.load(Ordering::SeqCst), 1);
    assert_eq!(second_close.load(Ordering::SeqCst), 1);
}

#[test]
fn coordinated_commit_is_rejected_under_at_least_once_before_open() {
    let opened = Arc::new(AtomicBool::new(false));
    let sink = OpenProbeSink {
        contract: SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        opened: Arc::clone(&opened),
        schema: Arc::new(Schema::empty()),
        exact_protocol: true,
    };
    let config = ConnectorConfig::new("checkpoint-committable-probe");

    let error = admit_sink(
        &sink,
        SinkAdmissionContext {
            config: &config,
            name: "exact_out",
            input: "input",
            delivery: DeliveryGuarantee::AtLeastOnce,
            runtime: RuntimeMode::Local,
            carries_changelog: false,
            checkpointing_enabled: true,
            checkpoint_storage_scope: CheckpointStorageScope::Volatile,
        },
    )
    .expect_err("the exact descriptor/cursor path must not activate under ALO");

    assert!(error.to_string().contains("require global exactly-once"));
    assert!(!opened.load(Ordering::SeqCst));
}

#[async_trait]
impl SinkConnector for OpenProbeSink {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(self.contract)
    }

    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.opened.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn as_coordinated_committer(
        &self,
    ) -> Option<&dyn laminar_connectors::connector::CoordinatedCommitter> {
        self.exact_protocol.then_some(self)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait]
impl laminar_connectors::connector::CoordinatedCommitter for OpenProbeSink {
    async fn commit_aggregated(
        &self,
        _batch: laminar_connectors::connector::CoordinatedCommitBatch,
        _context: laminar_connectors::connector::CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn committed_cursor(
        &self,
        _namespace: &laminar_connectors::connector::CoordinatedCommitNamespace,
    ) -> Result<Option<laminar_connectors::connector::CoordinatedCommitCursor>, ConnectorError>
    {
        Ok(None)
    }
}

#[test]
fn complete_exact_protocol_is_admitted_without_opening() {
    let opened = Arc::new(AtomicBool::new(false));
    let sink = OpenProbeSink {
        contract: SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        opened: Arc::clone(&opened),
        schema: Arc::new(Schema::empty()),
        exact_protocol: true,
    };

    let config = ConnectorConfig::new("exact-probe");
    admit_sink(
        &sink,
        SinkAdmissionContext {
            config: &config,
            name: "exact_out",
            input: "input",
            delivery: DeliveryGuarantee::ExactlyOnce,
            runtime: RuntimeMode::Local,
            carries_changelog: false,
            checkpointing_enabled: true,
            checkpoint_storage_scope: CheckpointStorageScope::NodeDurable,
        },
    )
    .unwrap();
    assert!(!opened.load(Ordering::SeqCst));
}

#[cfg(all(feature = "cluster", feature = "iceberg"))]
#[test]
fn rest_s3_iceberg_is_cluster_exact_admitted_before_io() {
    use laminar_connectors::lakehouse::iceberg::IcebergSink;
    use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;

    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.type", "rest");
    config.set("catalog.uri", "http://catalog.invalid");
    config.set("catalog.warehouse", "s3://warehouse/root");
    config.set("storage.type", "s3");
    config.set("namespace", "production");
    config.set("table.name", "events");
    config.set("delivery.guarantee", "exactly-once");
    let sink = IcebergSink::new(IcebergSinkConfig::from_config(&config).unwrap(), None);

    let (contract, _) = admit_sink(
        &sink,
        SinkAdmissionContext {
            config: &config,
            name: "iceberg_out",
            input: "events",
            delivery: DeliveryGuarantee::ExactlyOnce,
            runtime: RuntimeMode::Cluster,
            carries_changelog: false,
            checkpointing_enabled: true,
            checkpoint_storage_scope: CheckpointStorageScope::ClusterShared,
        },
    )
    .expect("certified REST/S3 Iceberg append must pass cluster admission");
    assert!(contract.is_cluster_exact_delivery_certified());
}

#[cfg(all(feature = "cluster", feature = "iceberg"))]
#[test]
fn uncertified_or_unsupported_iceberg_targets_fail_before_io() {
    use laminar_connectors::lakehouse::iceberg::IcebergSink;
    use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;

    for (key, value, expected_reason) in [
        ("catalog.type", "glue", "cluster exactly-once"),
        ("storage.type", "fs", "cluster exactly-once"),
        (
            "catalog.warehouse",
            "file:///tmp/warehouse",
            "cluster exactly-once",
        ),
        ("catalog.access_delegation", "true", "access-delegation"),
        (
            "catalog.property.s3.remote-signing-enabled",
            "true",
            "REMOTE-SIGNING",
        ),
        (
            "storage.property.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
            "access-delegation",
        ),
    ] {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://catalog.invalid");
        config.set("catalog.warehouse", "s3://warehouse/root");
        config.set("storage.type", "s3");
        config.set("namespace", "production");
        config.set("table.name", "events");
        config.set("delivery.guarantee", "exactly-once");
        config.set(key, value);
        let sink = IcebergSink::new(IcebergSinkConfig::from_config(&config).unwrap(), None);
        let error = admit_sink(
            &sink,
            SinkAdmissionContext {
                config: &config,
                name: "iceberg_out",
                input: "events",
                delivery: DeliveryGuarantee::ExactlyOnce,
                runtime: RuntimeMode::Cluster,
                carries_changelog: false,
                checkpointing_enabled: true,
                checkpoint_storage_scope: CheckpointStorageScope::ClusterShared,
            },
        )
        .expect_err("uncertified Iceberg target must fail before connector open");
        assert!(error.to_string().contains(expected_reason), "{error}");
    }
}

#[test]
fn checkpoint_committable_contract_without_committer_is_rejected_before_open() {
    let opened = Arc::new(AtomicBool::new(false));
    let sink = OpenProbeSink {
        contract: SinkContract::new(
            SinkConsistency::CheckpointCommittable,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        opened: Arc::clone(&opened),
        schema: Arc::new(Schema::empty()),
        exact_protocol: false,
    };

    let config = ConnectorConfig::new("incomplete-exact-probe");
    let error = admit_sink(
        &sink,
        SinkAdmissionContext {
            config: &config,
            name: "exact_out",
            input: "input",
            delivery: DeliveryGuarantee::ExactlyOnce,
            runtime: RuntimeMode::Local,
            carries_changelog: false,
            checkpointing_enabled: true,
            checkpoint_storage_scope: CheckpointStorageScope::NodeDurable,
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("does not implement"));
    assert!(!opened.load(Ordering::SeqCst));
}

#[test]
fn exact_state_scope_is_runtime_aware_and_checked_before_open() {
    let cases = [
        (RuntimeMode::Local, CheckpointStorageScope::Volatile, false),
        (
            RuntimeMode::Local,
            CheckpointStorageScope::NodeDurable,
            true,
        ),
        (
            RuntimeMode::Cluster,
            CheckpointStorageScope::NodeDurable,
            false,
        ),
        (
            RuntimeMode::Cluster,
            CheckpointStorageScope::ClusterShared,
            false,
        ),
    ];

    for (runtime, scope, expected_admission) in cases {
        let opened = Arc::new(AtomicBool::new(false));
        let sink = OpenProbeSink {
            contract: SinkContract::new(
                SinkConsistency::CheckpointCommittable,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ),
            opened: Arc::clone(&opened),
            schema: Arc::new(Schema::empty()),
            exact_protocol: true,
        };

        let config = ConnectorConfig::new("exact-probe");
        let result = admit_sink(
            &sink,
            SinkAdmissionContext {
                config: &config,
                name: "exact_out",
                input: "input",
                delivery: DeliveryGuarantee::ExactlyOnce,
                runtime,
                carries_changelog: false,
                checkpointing_enabled: true,
                checkpoint_storage_scope: scope,
            },
        );

        assert_eq!(
            result.is_ok(),
            expected_admission,
            "runtime={runtime:?}, scope={scope:?}, result={result:?}"
        );
        assert!(!opened.load(Ordering::SeqCst));
    }
}

#[test]
fn exact_rejection_precedes_open_for_non_committable_contract() {
    let opened = Arc::new(AtomicBool::new(false));
    let sink = OpenProbeSink {
        contract: SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::Singleton,
            SinkInputMode::AppendOnly,
        ),
        opened: Arc::clone(&opened),
        schema: Arc::new(Schema::empty()),
        exact_protocol: false,
    };
    let config = ConnectorConfig::new("durable-probe");
    let error = admit_sink(
        &sink,
        SinkAdmissionContext {
            config: &config,
            name: "durable_out",
            input: "input",
            delivery: DeliveryGuarantee::ExactlyOnce,
            runtime: RuntimeMode::Local,
            carries_changelog: false,
            checkpointing_enabled: true,
            checkpoint_storage_scope: CheckpointStorageScope::NodeDurable,
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains(EXACT_SINK_PROTOCOL));
    assert!(!opened.load(Ordering::SeqCst));
}
