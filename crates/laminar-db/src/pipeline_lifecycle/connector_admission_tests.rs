use super::LaminarDB;
use super::{
    admit_sink, admit_sink_contract, admit_source_contract, close_opened_sinks,
    open_prepared_sinks, resolve_stream_output_schemas, validate_source_recovery_assignment,
    ConnectorTaskFenceRegistration, DbError, PreparedSink, RuntimeMode, SinkAdmissionContext,
    TrackedSourceRegistration, CLUSTER_BEST_EFFORT, EXACT_SINK_PROTOCOL,
};
#[cfg(feature = "cluster")]
use super::{
    cluster_delta_chain_bound, cluster_vnode_chain_artifact_limit,
    MAX_CLUSTER_VNODE_CHAIN_ARTIFACTS,
};
use crate::db::DbState;
use crate::pipeline::PipelineConfig;
use crate::sink_task::{SinkTaskConfig, DEFAULT_CHANNEL_CAPACITY, SINK_EVENT_CHANNEL_CAPACITY};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, ConnectorTaskOwner, ConnectorTaskTracker, DeliveryGuarantee,
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, SourceBatch,
    SourceConnector, SourceConsistency, SourceContract, SourceStart, SourceTopology, WriteResult,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::state::StateBackendDurability;
use laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "cluster")]
#[test]
fn cluster_delta_chain_bound_is_derived_from_retention() {
    assert_eq!(cluster_delta_chain_bound(0), None);
    assert_eq!(cluster_delta_chain_bound(1), None);
    assert_eq!(cluster_delta_chain_bound(2), Some(1));
    assert_eq!(cluster_delta_chain_bound(3), Some(2));
    assert_eq!(cluster_delta_chain_bound(4), Some(3));
    assert_eq!(cluster_delta_chain_bound(5), Some(4));
    assert_eq!(cluster_delta_chain_bound(usize::MAX), Some(4));

    assert_eq!(cluster_vnode_chain_artifact_limit(0), 1);
    assert_eq!(cluster_vnode_chain_artifact_limit(1), 1);
    assert_eq!(cluster_vnode_chain_artifact_limit(2), 3);
    assert_eq!(cluster_vnode_chain_artifact_limit(3), 4);
    assert_eq!(cluster_vnode_chain_artifact_limit(4), 5);
    assert_eq!(cluster_vnode_chain_artifact_limit(5), 6);
    assert_eq!(cluster_vnode_chain_artifact_limit(usize::MAX), 6);
    assert_eq!(MAX_CLUSTER_VNODE_CHAIN_ARTIFACTS, 6);
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
                                SourceContract::new(consistency, topology)
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
                                    && delivery != DeliveryGuarantee::AtLeastOnce)
                                && (runtime != RuntimeMode::Cluster
                                    || topology == SourceTopology::Splittable);

                            assert_eq!(
                                admit_source_contract(
                                    contract,
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
    let contract = SourceContract::new(SourceConsistency::Replayable, SourceTopology::Singleton);
    let error = admit_source_contract(
        contract,
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
        DeliveryGuarantee::ExactlyOnce,
        true,
        RuntimeMode::Local,
    )
    .expect("the certified deterministic generator must remain locally admissible");
}

#[test]
fn uncertified_replayable_source_is_rejected_for_exact_delivery() {
    let contract = SourceContract::new(SourceConsistency::Replayable, SourceTopology::Splittable);
    assert!(!contract.is_exact_delivery_certified());
    let error = admit_source_contract(
        contract,
        DeliveryGuarantee::ExactlyOnce,
        true,
        RuntimeMode::Local,
    )
    .expect_err("uncertified source recovery must fail closed for exact delivery");
    assert!(error.contains(laminar_core::error_codes::EXACTLY_ONCE_SOURCE_UNCERTIFIED));
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
            contract: SourceContract::new(SourceConsistency::Replayable, SourceTopology::Singleton),
            assignment_scoped: false,
            position: laminar_connectors::connector::SourcePosition::Initial,
        },
        task_fence,
    );

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
            },
        );
        resolve_stream_output_schemas(&context, &streams).await?;
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
                state_backend_scope: StateBackendDurability::NodeDurable,
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
            state_backend_scope: StateBackendDurability::Volatile,
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
            state_backend_scope: StateBackendDurability::NodeDurable,
        },
    )
    .unwrap();
    assert!(!opened.load(Ordering::SeqCst));
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
            state_backend_scope: StateBackendDurability::NodeDurable,
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("does not implement"));
    assert!(!opened.load(Ordering::SeqCst));
}

#[test]
fn exact_state_scope_is_runtime_aware_and_checked_before_open() {
    let cases = [
        (RuntimeMode::Local, StateBackendDurability::Volatile, false),
        (
            RuntimeMode::Local,
            StateBackendDurability::NodeDurable,
            true,
        ),
        (
            RuntimeMode::Cluster,
            StateBackendDurability::NodeDurable,
            false,
        ),
        (
            RuntimeMode::Cluster,
            StateBackendDurability::ClusterShared,
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
                state_backend_scope: scope,
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
            state_backend_scope: StateBackendDurability::NodeDurable,
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains(EXACT_SINK_PROTOCOL));
    assert!(!opened.load(Ordering::SeqCst));
}
