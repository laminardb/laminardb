//! Exact-cut retirement reuses real task wrappers, sink actors and connector-child trackers.

use super::*;
use laminar_connectors::connector::ConnectorTaskOwner;
use tokio::sync::Notify;

#[path = "topology_target_preparation.rs"]
mod target_preparation;

#[path = "topology_commit.rs"]
mod topology_commit;

fn namespace_lock(db: &LaminarDB) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    let owner = file.reopen().unwrap();
    owner.try_lock().unwrap();
    *db.checkpoint_namespace_lock.lock() = Some(Arc::new(owner));
    file
}

fn require_namespace_owned(db: &LaminarDB, file: &tempfile::NamedTempFile) {
    assert!(db.checkpoint_namespace_lock.lock().is_some());
    assert!(file.reopen().unwrap().try_lock().is_err());
}

async fn wait_for(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(5), notify.notified())
        .await
        .expect("retirement boundary was not reached");
}

async fn blocked_runtime(db: &LaminarDB) -> (Arc<Notify>, Arc<Notify>) {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let task_entered = Arc::clone(&entered);
    let task_release = Arc::clone(&release);
    let shutdown = db.runtime_shutdown.read().clone();
    *db.runtime_handle.lock().await = Some(tokio::spawn(async move {
        shutdown.cancelled().await;
        task_entered.notify_one();
        task_release.notified().await;
    }));
    (entered, release)
}

#[tokio::test]
async fn topology_retirement_preserves_cut_catalog_namespace_and_unstarted_target() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let catalog = fixture.db.catalog_manifest_inventory().unwrap();
    let authority = fixture.authority.lease_store.load().await.unwrap();
    let objects = object_paths(fixture.authority.checkpoint_store.as_ref()).await;
    let bytes = image.managed_state_bytes();
    let root = image.root().clone();
    assert!(!image.parent_retirement_observed());
    let db = Arc::clone(&fixture.db);
    let (result, mut image) = tokio::spawn(async move {
        let result = db.retire_cluster_topology_parent(&mut image).await;
        (result, image)
    })
    .await
    .unwrap();
    result.unwrap();
    assert!(image.parent_retirement_observed());
    assert_eq!(DbState::load(&fixture.db.state), DbState::ShuttingDown);
    assert_eq!(DbState::load(&image.candidate.state), DbState::Created);
    assert_eq!(image.managed_state_bytes(), bytes);
    assert_eq!(image.root(), &root);
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), catalog);
    assert_eq!(
        fixture.authority.lease_store.load().await.unwrap(),
        authority
    );
    assert_eq!(
        object_paths(fixture.authority.checkpoint_store.as_ref()).await,
        objects
    );
    assert_eq!(
        fixture
            .db
            .coordinator
            .lock()
            .await
            .as_ref()
            .unwrap()
            .bound_pipeline_identity()
            .unwrap(),
        image.parent_checkpoint().pipeline_identity
    );
    require_namespace_owned(&fixture.db, &file);
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .unwrap();
    assert!(image.parent_retirement_observed());
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_restore(staged.operation_id)
            .await,
        Err(DbError::Topology(_))
    ));
    let status = fixture.db.cluster_topology_status().await.unwrap();
    assert_eq!(status.committed_version.unwrap().get(), 1);
    assert!(status.locally_active_version.is_none());
    assert!(fixture
        .db
        .start()
        .await
        .unwrap_err()
        .to_string()
        .contains("held topology cut"));
    assert!(fixture
        .db
        .stop_pipeline()
        .await
        .unwrap_err()
        .to_string()
        .contains("held topology cut"));
    drop(image);
    assert!(fixture.db.start().await.is_err());
    require_namespace_owned(&fixture.db, &file);
    // Terminal shutdown, unlike a migration retirement, may release the namespace.
    fixture.db.shutdown().await.unwrap();
    assert!(fixture.db.checkpoint_namespace_lock.lock().is_none());
    file.reopen().unwrap().try_lock().unwrap();
}

struct RetirementSink {
    _owner: ConnectorTaskOwner,
    entered: Arc<Notify>,
    closed: Arc<Notify>,
    finished: Arc<Notify>,
}

#[async_trait]
impl SinkConnector for RetirementSink {
    async fn open(&mut self, _: &ConnectorConfig) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn write_batch(&mut self, _: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        panic!("retirement must not publish rows");
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.entered.notify_one();
        self.closed.notified().await;
        self.finished.notify_one();
        Ok(())
    }
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::new(arrow_schema::Schema::empty())
    }
    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
}

#[tokio::test]
async fn topology_retirement_observes_compute_source_sink_and_connector_children() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (compute_entered, compute_release) = blocked_runtime(&fixture.db).await;
    let (source_owner, source_tracker) = ConnectorTaskOwner::new();
    let source_child = source_owner.track().unwrap();
    let source = crate::pipeline::streaming_coordinator::SourceTaskLease::spawn_for_test(
        "parent-source",
        async move {
            let _owner = source_owner;
            futures::future::pending::<()>().await;
        },
        Some(source_tracker),
    );
    fixture.db.owned_source_tasks.lock().push(source.clone());
    let (sink_owner, sink_tracker) = ConnectorTaskOwner::new();
    let sink_child = sink_owner.track().unwrap();
    let sink_entered = Arc::new(Notify::new());
    let sink_closed = Arc::new(Notify::new());
    let sink_finished = Arc::new(Notify::new());
    let (events, _receiver) =
        laminar_core::streaming::channel::channel(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let sink = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "parent-sink".into(),
        sink_id: Arc::from("parent-sink"),
        connector: Box::new(RetirementSink {
            _owner: sink_owner,
            entered: Arc::clone(&sink_entered),
            closed: Arc::clone(&sink_closed),
            finished: Arc::clone(&sink_finished),
        }),
        contract: SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_secs(60),
        write_timeout: Duration::from_secs(1),
        event_tx: events,
        terminal_tasks: Some(sink_tracker),
        process_authority: Some(Arc::clone(&fixture.authority.controller)),
    });
    fixture.db.owned_sink_handles.lock().push(sink.clone());
    let (startup_owner, startup_tracker) = ConnectorTaskOwner::new();
    let startup_child = startup_owner.track().unwrap();
    fixture.db.owned_connector_task_fences.lock().push(
        crate::connector_task_fence::ConnectorTaskFence::new("pre-actor", startup_tracker),
    );
    drop(startup_owner);
    let db = Arc::clone(&fixture.db);
    let retirement = tokio::spawn(async move {
        let result = db.retire_cluster_topology_parent(&mut image).await;
        (result, image)
    });
    wait_for(&compute_entered).await;
    assert!(!source.is_finished());
    assert!(sink.has_unresolved_task());
    assert!(!retirement.is_finished());
    require_namespace_owned(&fixture.db, &file);
    compute_release.notify_one();
    wait_for(&sink_entered).await;
    sink_closed.notify_one();
    wait_for(&sink_finished).await;
    // Even after connector close returns and source abort is requested, children remain live.
    assert!(sink.has_unresolved_task());
    assert!(!source.is_finished());
    assert!(!retirement.is_finished());
    drop(source_child);
    drop(sink_child);
    tokio::task::yield_now().await;
    assert!(!retirement.is_finished());
    drop(startup_child);
    let (result, image) = tokio::time::timeout(Duration::from_secs(5), retirement)
        .await
        .unwrap()
        .unwrap();
    result.unwrap();
    assert!(image.parent_retirement_observed());
    assert!(source.is_finished());
    assert!(!sink.has_unresolved_task());
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    assert!(fixture.db.owned_connector_task_fences.lock().is_empty());
    assert!(sink
        .write_batch(RecordBatch::new_empty(Arc::new(
            arrow_schema::Schema::empty()
        )))
        .await
        .is_err());
    require_namespace_owned(&fixture.db, &file);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    drop(image);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_retirement_cancellation_retains_runtime_owner_and_supports_retry() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (entered, release) = blocked_runtime(&fixture.db).await;
    let mut retirement = Box::pin(fixture.db.retire_cluster_topology_parent(&mut image));
    tokio::select! {
        () = wait_for(&entered) => {},
        result = &mut retirement => panic!("retirement skipped the runtime owner: {result:?}"),
    }
    drop(retirement);
    assert!(!image.parent_retirement_observed());
    assert_eq!(DbState::load(&fixture.db.state), DbState::ShuttingDown);
    assert!(fixture.db.runtime_handle.lock().await.is_some());
    require_namespace_owned(&fixture.db, &file);
    release.notify_one();
    fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .unwrap();
    assert!(image.parent_retirement_observed());
    assert!(fixture.db.runtime_handle.lock().await.is_none());
    drop(image);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_retirement_deadline_keeps_unobserved_work_fenced() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (entered, release) = blocked_runtime(&fixture.db).await;
    tokio::time::pause();
    let mut retirement = Box::pin(fixture.db.retire_cluster_topology_parent(&mut image));
    tokio::select! {
        () = wait_for(&entered) => {},
        result = &mut retirement => panic!("retirement skipped the runtime owner: {result:?}"),
    }
    tokio::time::advance(Duration::from_secs(45)).await;
    let error = retirement.await.unwrap_err();
    // Either the total request deadline or the stop's shared cleanup deadline can win.
    assert!(
        matches!(
            &error,
            DbError::Topology(TopologyError::Contended) | DbError::InvalidOperation(_)
        ),
        "{error}"
    );
    assert!(!image.parent_retirement_observed());
    assert!(fixture.db.runtime_handle.lock().await.is_some());
    require_namespace_owned(&fixture.db, &file);
    release.notify_one();
    tokio::time::resume();
    fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .unwrap();
    drop(image);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_retirement_rejects_foreign_image_and_lost_local_hold_before_stop() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let other = Fixture::new().await;
    assert!(matches!(
        other.db.retire_cluster_topology_parent(&mut image).await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    assert!(!other.db.runtime_shutdown.read().is_cancelled());
    fixture.db.source_gate.store(false, Ordering::Release);
    assert!(fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .is_err());
    assert!(!fixture.db.runtime_shutdown.read().is_cancelled());
    assert!(!image.parent_retirement_observed());
    assert_eq!(DbState::load(&fixture.db.state), DbState::Running);
}

#[tokio::test]
async fn topology_retirement_rejects_stale_authority_before_stop() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    fixture
        .authority
        .lease_store
        .begin_new_term(&fixture.authority.lease.owner, 1)
        .await
        .unwrap();
    assert!(fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .is_err());
    assert!(!fixture.db.runtime_shutdown.read().is_cancelled());
    assert!(!image.parent_retirement_observed());
    assert_eq!(DbState::load(&fixture.db.state), DbState::Running);
}

#[tokio::test]
async fn topology_retirement_rechecks_authority_after_observing_the_old_runtime() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (entered, release) = blocked_runtime(&fixture.db).await;
    let db = Arc::clone(&fixture.db);
    let retirement = tokio::spawn(async move {
        let result = db.retire_cluster_topology_parent(&mut image).await;
        (result, image)
    });
    wait_for(&entered).await;
    fixture
        .authority
        .lease_store
        .begin_new_term(&fixture.authority.lease.owner, 1)
        .await
        .unwrap();
    release.notify_one();
    let (result, image) = retirement.await.unwrap();
    assert!(result.is_err());
    assert!(!image.parent_retirement_observed());
    assert_eq!(DbState::load(&fixture.db.state), DbState::ShuttingDown);
    require_namespace_owned(&fixture.db, &file);
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    let aborted = fixture
        .db
        .cluster_topology_operation_status(staged.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        aborted.phase,
        laminar_core::cluster::control::TopologyAdmissionPhase::Aborted { .. }
    ));
    assert_eq!(aborted.migration_root, staged.migration_root);
    // The existing recovery authority can take over an aborted, retired parent.
    fixture.db.fence_coordinated_recovery_lifecycle();
    fixture
        .db
        .stop_pipeline_for_coordinated_recovery()
        .await
        .unwrap();
    assert_eq!(DbState::load(&fixture.db.state), DbState::Created);
    assert!(fixture.db.checkpoint_namespace_lock.lock().is_none());
    file.reopen().unwrap().try_lock().unwrap();
}

#[tokio::test]
async fn topology_retirement_runtime_panic_is_sticky_and_never_certifies_an_image() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    *fixture.db.runtime_handle.lock().await =
        Some(tokio::spawn(async { panic!("injected watcher failure") }));
    let error = fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("watcher failed"), "{error}");
    assert!(!image.parent_retirement_observed());
    require_namespace_owned(&fixture.db, &file);
    assert!(fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .is_err());
    assert!(fixture.db.last_fault().unwrap().contains("watcher failed"));
    drop(image);
    assert!(fixture.db.shutdown().await.is_err());
    assert!(fixture.db.checkpoint_namespace_lock.lock().is_none());
}

#[tokio::test]
async fn topology_retirement_process_lease_loss_during_cleanup_cannot_certify_the_image() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (entered, release) = blocked_runtime(&fixture.db).await;
    let db = Arc::clone(&fixture.db);
    let retirement = tokio::spawn(async move {
        let result = db.retire_cluster_topology_parent(&mut image).await;
        (result, image)
    });
    wait_for(&entered).await;
    fixture.authority.controller.fence_process_lease();
    release.notify_one();
    let (result, image) = retirement.await.unwrap();
    assert!(result.is_err());
    assert!(!image.parent_retirement_observed());
    assert_eq!(DbState::load(&fixture.db.state), DbState::ShuttingDown);
    assert!(fixture.db.runtime_handle.lock().await.is_none());
    require_namespace_owned(&fixture.db, &file);
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    drop(image);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_retirement_local_recovery_and_fault_fences_cannot_be_overridden() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    fixture.db.fence_coordinated_recovery_lifecycle();
    assert!(fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .is_err());
    assert!(!fixture.db.runtime_shutdown.read().is_cancelled());
    fixture.db.release_coordinated_recovery_lifecycle();
    fixture
        .db
        .pending_recovery_fault
        .store(1, Ordering::Release);
    assert!(fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .is_err());
    assert!(!fixture.db.runtime_shutdown.read().is_cancelled());
    fixture
        .db
        .pending_recovery_fault
        .store(0, Ordering::Release);
    *fixture.db.last_fault.lock() = Some("existing runtime fault".into());
    assert!(fixture
        .db
        .retire_cluster_topology_parent(&mut image)
        .await
        .is_err());
    assert!(!image.parent_retirement_observed());
}
