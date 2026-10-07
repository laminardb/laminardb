//! Public embedded validation over real durable catalog authority and the production planner.

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use futures::TryStreamExt;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, SourceBatch,
    SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourceRowPositionCapability, SourceStart, SourceTopology, WriteResult,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::cluster::control::{CheckpointDecisionStore, TopologyError, TopologyVersion};
use laminar_core::state::{NodeId, VnodeRegistry};
use object_store::{ObjectStore, ObjectStoreExt};

use super::*;
use crate::{ClusterTopologyObjectTransition, TopologyInitialization, TopologyValidationScope};

#[derive(Default)]
struct RestoreValidationControl {
    block: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    runtime: parking_lot::Mutex<Option<Arc<runtime_probe::InstallationProbe>>>,
}

#[path = "topology_installation_probe.rs"]
mod runtime_probe;

struct PlanningSource(
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<RestoreValidationControl>,
    Option<runtime_probe::RuntimeSource>,
    bool,
);

#[path = "topology_restore.rs"]
mod restore;

fn forbidden_effect(effects: &AtomicUsize) -> ConnectorError {
    effects.fetch_add(1, Ordering::SeqCst);
    ConnectorError::ConfigurationError("validation invoked a connector lifecycle effect".into())
}

#[async_trait]
impl SourceConnector for PlanningSource {
    fn supports_initialized_start(&self) -> bool {
        self.3
            .as_ref()
            .is_some_and(|runtime| !runtime.probe.reject_initialized.load(Ordering::Acquire))
    }

    fn set_vnode_assignment(
        &mut self,
        name: &str,
        registry: Arc<VnodeRegistry>,
        _: NodeId,
    ) -> Result<(), ConnectorError> {
        let runtime = self.3.as_mut().ok_or_else(|| forbidden_effect(&self.0))?;
        runtime.name = name.to_owned();
        runtime.assignment = std::num::NonZeroU64::new(registry.assignment_version());
        Ok(())
    }

    fn drive_control_plane(&mut self) {
        if let Some(runtime) = self.3.as_mut() {
            runtime.probe.controls.fetch_add(1, Ordering::AcqRel);
        }
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        _: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        if let Some(runtime) = self.3.as_ref() {
            runtime
                .probe
                .acknowledgements
                .lock()
                .push((runtime.name.clone(), epoch));
        }
        Ok(())
    }
    async fn validate_initial_position(
        &mut self,
        config: &ConnectorConfig,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        if config.get("topic") == Some("new")
            && config.get("start") == Some("latest")
            && checkpoint
                .offsets()
                .get("partition-0-next")
                .and_then(|next| next.parse::<u64>().ok())
                .is_some_and(|next| next >= 91)
            && checkpoint.assignment_version().is_none()
        {
            self.2.entered.notify_one();
            if self.2.block.load(Ordering::Acquire) {
                self.2.release.notified().await;
            }
            Ok(())
        } else {
            Err(forbidden_effect(&self.0))
        }
    }
    async fn resolve_initial_position(
        &mut self,
        config: &ConnectorConfig,
    ) -> Result<SourceCheckpoint, ConnectorError> {
        if config.get("topic") != Some("new") || config.get("start") != Some("latest") {
            return Err(forbidden_effect(&self.0));
        }
        let next = 91 + self.1.fetch_add(1, Ordering::SeqCst);
        let mut checkpoint = SourceCheckpoint::with_offsets(HashMap::from([(
            "partition-0-next".into(),
            next.to_string(),
        )]));
        checkpoint.set_metadata("connector", "planning-source");
        checkpoint.set_input_channels(vec![vec![1]])?;
        Ok(checkpoint)
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        let schema = crate::temporal_test_source::schema();
        if !self.4 {
            return schema;
        }
        let mut fields = schema.fields().to_vec();
        fields.push(Arc::new(arrow_schema::Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            arrow_schema::DataType::Int64,
            false,
        )));
        Arc::new(arrow_schema::Schema::new(fields))
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        let topology = if config.get("placement") == Some("singleton") {
            SourceTopology::Singleton
        } else {
            SourceTopology::Splittable
        };
        let consistency = if config.get("durability") == Some("ephemeral") {
            SourceConsistency::Ephemeral
        } else {
            SourceConsistency::Replayable
        };
        let input = if config.get("input") == Some("changelog") {
            SourceInputMode::FullChangelog
        } else {
            SourceInputMode::AppendOnly
        };
        Ok(SourceContract::new(consistency, topology, input)
            .with_row_positions(SourceRowPositionCapability::OrderedDeterministic))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if let Some(runtime) = self.3.as_mut() {
            return runtime.start(request).await;
        }
        Err(forbidden_effect(&self.0))
    }
    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        if let Some(runtime) = self.3.as_mut() {
            return runtime.poll();
        }
        Err(forbidden_effect(&self.0))
    }
    fn checkpoint(&self) -> SourceCheckpoint {
        if let Some(runtime) = self.3.as_ref() {
            return runtime.checkpoint.clone();
        }
        self.0.fetch_add(1, Ordering::SeqCst);
        SourceCheckpoint::new()
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(runtime) = self.3.as_ref() {
            runtime.probe.source_closes.fetch_add(1, Ordering::AcqRel);
            return Ok(());
        }
        Err(forbidden_effect(&self.0))
    }
    async fn discover_schema(&mut self, _: &HashMap<String, String>) -> Result<(), ConnectorError> {
        Err(forbidden_effect(&self.0))
    }
}

struct PlanningSink(Arc<AtomicUsize>, Option<runtime_probe::RuntimeSink>);

#[async_trait]
impl SinkConnector for PlanningSink {
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::new(arrow_schema::Schema::empty())
    }
    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let topology = if config.get("placement") == Some("singleton") {
            SinkTopology::Singleton
        } else {
            SinkTopology::MultiWriter
        };
        let consistency = if config.get("durability") == Some("ephemeral") {
            SinkConsistency::Ephemeral
        } else {
            SinkConsistency::DurableAtLeastOnce
        };
        let input = if config.get("input") == Some("append") {
            SinkInputMode::AppendOnly
        } else {
            SinkInputMode::FullChangelog
        };
        Ok(SinkContract::new(consistency, topology, input))
    }
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if let Some(runtime) = self.1.as_mut() {
            return runtime.open(config);
        }
        Err(forbidden_effect(&self.0))
    }
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if let Some(runtime) = self.1.as_ref() {
            return runtime.write(batch);
        }
        Err(forbidden_effect(&self.0))
    }
    async fn begin_epoch(&mut self, _: u64) -> Result<(), ConnectorError> {
        if let Some(runtime) = self.1.as_ref() {
            runtime.probe.sink_epochs.fetch_add(1, Ordering::AcqRel);
            return Ok(());
        }
        Err(forbidden_effect(&self.0))
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(runtime) = self.1.as_ref() {
            runtime.probe.sink_closes.fetch_add(1, Ordering::AcqRel);
            return Ok(());
        }
        Err(forbidden_effect(&self.0))
    }
    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
}

struct Fixture {
    db: Arc<LaminarDB>,
    authority: TestCatalogAuthority,
    effects: Arc<AtomicUsize>,
    resolutions: Arc<AtomicUsize>,
    restore_validation: Arc<RestoreValidationControl>,
    process_lease: Option<laminar_core::cluster::control::ProcessLease>,
}

impl Fixture {
    async fn new() -> Self {
        let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        Self::with_objects(objects).await
    }

    async fn with_objects(objects: Arc<dyn ObjectStore>) -> Self {
        let authority = test_catalog_authority_with_ttl(objects, 60_000).await;
        Self::with_authority(authority).await
    }

    async fn with_authority(authority: TestCatalogAuthority) -> Self {
        Self::with_authority_and_generation(authority, 1).await
    }

    async fn with_authority_and_generation(
        authority: TestCatalogAuthority,
        stream_generation: u64,
    ) -> Self {
        let bootstrap = vec![
            source_ddl("trades", "'topic' = 'old'"),
            "CREATE STREAM totals AS SELECT id, SUM(value) AS total FROM trades GROUP BY id EMIT CHANGES WITH ('retain_history' = '4mb')".into(),
            "CREATE SINK existing_sink FROM totals INTO \"planning-sink\" ('topic' = 'old-output')".into(),
        ];
        if stream_generation != 1 {
            use laminar_core::cluster::control::{
                CatalogManifest, CatalogManifestEntry, CatalogObjectKind,
            };
            let entries = bootstrap
                .iter()
                .cloned()
                .zip([
                    ("trades", CatalogObjectKind::Source, 1),
                    ("totals", CatalogObjectKind::Stream, stream_generation),
                    ("existing_sink", CatalogObjectKind::Sink, 1),
                ])
                .map(
                    |(ddl, (name, kind, catalog_generation))| CatalogManifestEntry {
                        schema_binding: None,
                        canonical_name: name.into(),
                        kind,
                        catalog_generation,
                        ddl,
                    },
                )
                .collect();
            authority
                .manifest_store
                .seal(
                    &CatalogManifest::new(entries).unwrap(),
                    &authority.lease.proof(),
                )
                .await
                .unwrap();
        }
        let effects = Arc::new(AtomicUsize::new(0));
        let factory_effects = Arc::clone(&effects);
        let resolutions = Arc::new(AtomicUsize::new(0));
        let factory_resolutions = Arc::clone(&resolutions);
        let restore_validation = Arc::new(RestoreValidationControl::default());
        let factory_validation = Arc::clone(&restore_validation);
        let process = authority.controller.recovery_incarnation();
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                1,
                "127.0.0.1:0".parse().unwrap(),
                process,
            )
            .await
            .unwrap(),
        );
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&authority.controller))
            .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
            .catalog_manifest_store(Arc::clone(&authority.manifest_store))
            .shuffle_sender(Arc::new(laminar_core::shuffle::ShuffleSender::new(
                1, process,
            )))
            .shuffle_receiver(receiver)
            .vnode_registry(Arc::new(VnodeRegistry::single_owner(8, NodeId(1))))
            .delivery_guarantee(laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce)
            .temporal_join_idle_history_retention(Duration::from_secs(60))
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            })
            .register_connector(move |registry| {
                let sink_validation = Arc::clone(&factory_validation);
                for (name, changelog) in [("planning-source", false), ("planning-changelog", true)]
                {
                    let source_effects = Arc::clone(&factory_effects);
                    let source_resolutions = Arc::clone(&factory_resolutions);
                    let source_validation = Arc::clone(&factory_validation);
                    registry.register_source(
                        name,
                        ConnectorInfo {
                            schema_capabilities: laminar_connectors::schema::resolution::SchemaCapabilities::declared(false),
                            name: name.into(),
                            display_name: name.into(),
                            version: "1".into(),
                            is_source: true,
                            is_sink: false,
                            config_keys: vec![],
                        },
                        Arc::new(move |_| {
                            Ok(Box::new(PlanningSource(
                                Arc::clone(&source_effects),
                                Arc::clone(&source_resolutions),
                                Arc::clone(&source_validation),
                                source_validation.runtime.lock().as_ref().map(|probe| {
                                    runtime_probe::RuntimeSource::new(Arc::clone(probe))
                                }),
                                changelog,
                            )))
                        }),
                    )?;
                }
                registry.register_sink(
                    "planning-sink",
                    ConnectorInfo {
                        schema_capabilities: laminar_connectors::schema::resolution::SchemaCapabilities::declared(true),
                        name: "planning-sink".into(),
                        display_name: "planning sink".into(),
                        version: "1".into(),
                        is_source: false,
                        is_sink: true,
                        config_keys: vec![],
                    },
                    Arc::new(move |_, _| {
                        Ok(Box::new(PlanningSink(
                            Arc::clone(&factory_effects),
                            sink_validation
                                .runtime
                                .lock()
                                .as_ref()
                                .map(|probe| runtime_probe::RuntimeSink::new(Arc::clone(probe))),
                        )))
                    }),
                )
            })
            .build()
            .await
            .unwrap();
        db.execute_cluster_bootstrap_batch(&bootstrap)
            .await
            .unwrap();
        Self {
            db,
            authority,
            effects,
            resolutions,
            restore_validation,
            process_lease: None,
        }
    }

    async fn adopt(&self) {
        let reference = self
            .authority
            .manifest_store
            .load()
            .await
            .unwrap()
            .unwrap()
            .reference()
            .unwrap();
        let deployment = CheckpointDecisionStore::new(Arc::clone(&self.authority.checkpoint_store))
            .load_or_create_deployment_id()
            .await
            .unwrap();
        self.authority
            .manifest_store
            .adopt_legacy_topology(
                &self.authority.lease.proof(),
                uuid::Uuid::from_u128(77).try_into().unwrap(),
                &reference,
                &deployment,
            )
            .await
            .unwrap();
    }

    async fn validate(
        &self,
        statements: &[String],
    ) -> Result<crate::ClusterTopologyValidation, DbError> {
        self.db
            .validate_cluster_topology_change(TopologyVersion::LEGACY_BASELINE, statements)
            .await
    }

    fn assert_no_effects(&self) {
        assert_eq!(self.effects.load(Ordering::SeqCst), 0);
        assert!(self.db.owned_source_tasks.lock().is_empty());
        assert!(self.db.owned_sink_handles.lock().is_empty());
        assert!(self.db.owned_connector_task_fences.lock().is_empty());
        assert!(self.db.coordinator.try_lock().unwrap().is_none());
        assert!(self.db.runtime_handle.try_lock().unwrap().is_none());
        assert!(self.db.force_ckpt_tx.lock().is_none());
        assert!(self.db.source_gate.load(Ordering::Acquire));
        assert!(!self.db.topology_cut_hold.load(Ordering::Acquire));
        assert_eq!(DbState::load(&self.db.state), DbState::Created);
    }
}

async fn preparation_fixture() -> (
    Fixture,
    laminar_core::cluster::control::AssignmentSnapshotStore,
) {
    preparation_fixture_with_generation(1).await
}

async fn preparation_fixture_with_generation(
    stream_generation: u64,
) -> (
    Fixture,
    laminar_core::cluster::control::AssignmentSnapshotStore,
) {
    use laminar_core::cluster::control::{
        AssignmentSnapshot, AssignmentSnapshotStore, ClusterController, ClusterKv, InMemoryKv,
        LeaseDeadline, ProcessLeaseAuthority, ProcessLeaseOutcome,
    };
    use laminar_core::cluster::discovery::NodeId as ClusterNodeId;
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut authority = test_catalog_authority_with_ttl(Arc::clone(&objects), 60_000).await;
    let owner = authority.lease.owner.clone();
    let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&objects)));
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(owner.node));
    let (_, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        owner.node,
        Arc::clone(&control),
        control,
        Some(Arc::clone(&assignments)),
        members_rx,
        owner.boot,
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    // Deliberately separate process storage: the controller must supply this configured authority.
    let processes = Arc::new(
        ProcessLeaseAuthority::new(
            Arc::new(object_store::memory::InMemory::new()),
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process) = processes
        .store_for(owner.node)
        .try_acquire(owner.boot, 0)
        .await
        .unwrap()
    else {
        panic!("fixture process must acquire its term");
    };
    controller.set_process_lease_authority(processes).unwrap();
    controller
        .publish_leased_recovery_incarnation(&process)
        .await
        .unwrap();
    let (lease_tx, lease_rx) = tokio::sync::watch::channel(Some(authority.lease.clone()));
    controller
        .set_leader_lease_watch(
            lease_rx,
            owner.clone(),
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority.lease_store));
    controller.install_local_leader_proof_provider();
    controller.set_active(true);
    let snapshot = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&[ClusterNodeId(1); 8]),
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: owner.node.0,
                boot_incarnation: owner.boot,
            }],
        )
        .unwrap();
    assignments.save_if_absent(&snapshot).await.unwrap();
    let fence = snapshot.assignment_fence().unwrap();
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    controller
        .announce_adopted_assignment(&laminar_core::checkpoint::CheckpointAssignmentAdoption {
            participant: fence.participants[0],
            assignment_version: fence.assignment_version,
            partitioning_abi_version: fence.partitioning_abi_version,
            vnode_count: fence.vnode_count,
            assignment_digest: fence.assignment_digest,
            vnode_state_ready: true,
        })
        .await
        .unwrap();
    authority.controller = controller;
    authority.lease_tx = lease_tx;
    let mut fixture = Fixture::with_authority_and_generation(authority, stream_generation).await;
    fixture.process_lease = Some(process);
    fixture.adopt().await;
    (fixture, AssignmentSnapshotStore::new(objects))
}

async fn admit_preparation_candidate(
    fixture: &Fixture,
    assignments: &laminar_core::cluster::control::AssignmentSnapshotStore,
) -> laminar_core::cluster::control::TopologyAdmissionStatus {
    admit_preparation_entries(
        fixture,
        assignments,
        vec![laminar_core::cluster::control::CatalogManifestEntry {
            schema_binding: None,
            canonical_name: "future".into(),
            kind: laminar_core::cluster::control::CatalogObjectKind::Stream,
            catalog_generation: 1,
            ddl: "CREATE STREAM future AS SELECT * FROM totals".into(),
        }],
    )
    .await
}

async fn admit_preparation_entries(
    fixture: &Fixture,
    assignments: &laminar_core::cluster::control::AssignmentSnapshotStore,
    entries: Vec<laminar_core::cluster::control::CatalogManifestEntry>,
) -> laminar_core::cluster::control::TopologyAdmissionStatus {
    let statements = entries
        .iter()
        .map(|entry| entry.ddl.clone())
        .collect::<Vec<_>>();
    admit_preparation_statements(fixture, assignments, statements).await
}

async fn admit_preparation_statements(
    fixture: &Fixture,
    assignments: &laminar_core::cluster::control::AssignmentSnapshotStore,
    statements: Vec<String>,
) -> laminar_core::cluster::control::TopologyAdmissionStatus {
    use laminar_core::cluster::control::{
        TopologyAdmissionPlan, TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
    };
    let (descriptor, target) = fixture
        .db
        .plan_cluster_topology_change(TopologyVersion::LEGACY_BASELINE, &statements)
        .await
        .unwrap();
    let compatibility = fixture
        .authority
        .lease_store
        .stage_topology_compatibility(&descriptor)
        .await
        .unwrap();
    let plan = TopologyAdmissionPlan {
        protocol_version: TOPOLOGY_PREPARATION_PROTOCOL_VERSION,
        operation_id: uuid::Uuid::from_u128(88).try_into().unwrap(),
        expected_parent: descriptor.parent_version,
        parent_manifest: descriptor.parent_manifest.clone(),
        target_manifest: descriptor.target_manifest.clone(),
        assignment: assignments
            .load()
            .await
            .unwrap()
            .unwrap()
            .assignment_fence()
            .unwrap(),
        compatibility: Some(compatibility),
    };
    let admitted = fixture
        .authority
        .lease_store
        .admit_topology_plan(
            &fixture.authority.lease.proof(),
            assignments,
            &plan,
            &target,
        )
        .await
        .unwrap();
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        test_checkpoint_store(),
    )
    .unwrap();
    coordinator
        .bind_pipeline_identity(descriptor.parent_pipeline)
        .unwrap();
    *fixture.db.coordinator.lock().await = Some(coordinator);
    DbState::Running.store(&fixture.db.state);
    admitted
}

#[tokio::test]
async fn topology_preparation_recompiles_durable_candidate_without_connector_effects() {
    let (fixture, assignments) = preparation_fixture().await;
    let admitted = admit_preparation_candidate(&fixture, &assignments).await;
    let inventory = fixture.db.catalog_manifest_inventory().unwrap();
    let prepared = fixture
        .db
        .prepare_cluster_topology_operation(admitted.operation_id)
        .await
        .unwrap();
    assert_eq!(
        prepared.phase,
        laminar_core::cluster::control::TopologyAdmissionPhase::Preparing
    );
    let preparation = prepared.preparation.as_ref().unwrap();
    assert_eq!(preparation.certificates.len(), 1);
    assert_eq!(
        preparation.complete_sequence,
        Some(prepared.status_sequence)
    );
    assert!(prepared.cut.is_none());
    assert_eq!(
        fixture
            .db
            .prepare_cluster_topology_operation(admitted.operation_id)
            .await
            .unwrap(),
        prepared
    );
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), inventory);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    assert!(!fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    let guard = fixture.db.topology_validation_lock.lock().await;
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_operation(admitted.operation_id)
            .await,
        Err(DbError::Topology(TopologyError::PlanningBusy))
    ));
    drop(guard);
    fixture.db.shutdown.store(true, Ordering::Release);
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_operation(admitted.operation_id)
            .await,
        Err(DbError::Shutdown)
    ));
}

#[tokio::test]
async fn topology_preparation_rejects_changed_local_config_and_nonrunning_parent() {
    let (fixture, assignments) = preparation_fixture().await;
    let admitted = admit_preparation_candidate(&fixture, &assignments).await;
    DbState::Created.store(&fixture.db.state);
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_operation(admitted.operation_id)
            .await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    DbState::Running.store(&fixture.db.state);
    let before = fixture.authority.lease_store.load().await.unwrap();
    let mut changed = fixture.db.connector_manager.lock().sources()["trades"].clone();
    changed
        .connector_options
        .insert("topic".into(), "divergent".into());
    fixture.db.connector_manager.lock().register_source(changed);
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_operation(admitted.operation_id)
            .await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn topology_root_staging_uses_configured_metadata_and_process_authority_without_actor_effects(
) {
    assert_root_staging(false).await;
}

#[tokio::test]
async fn topology_initialization_db_uses_configured_factory_once_without_actor_effects() {
    assert_root_staging(true).await;
}

async fn assert_root_staging(add_source: bool) {
    use laminar_core::checkpoint::{
        ByteRange, CheckpointAttempt, CheckpointManifest, CheckpointStore,
        CommittedCheckpointIndex, CommittedParticipantRef, ConnectorCheckpoint,
        ObjectStoreCheckpointStore, StateFrame, StateFrameKey,
    };
    use laminar_core::checkpoint_decision::{CheckpointArtifactInventory, CheckpointVerdict};
    use std::collections::BTreeMap;
    // New-source replay must preserve a durable parent incarnation greater than 1.
    let (fixture, assignments) =
        preparation_fixture_with_generation(if add_source { 7 } else { 1 }).await;
    let admitted = if add_source {
        let entries = independent_pipeline()
            .into_iter()
            .zip([
                (
                    "added_source",
                    laminar_core::cluster::control::CatalogObjectKind::Source,
                ),
                (
                    "added_stream",
                    laminar_core::cluster::control::CatalogObjectKind::Stream,
                ),
                (
                    "added_sink",
                    laminar_core::cluster::control::CatalogObjectKind::Sink,
                ),
            ])
            .map(
                |(ddl, (name, kind))| laminar_core::cluster::control::CatalogManifestEntry {
                    schema_binding: None,
                    canonical_name: name.into(),
                    kind,
                    catalog_generation: 1,
                    ddl,
                },
            )
            .collect();
        admit_preparation_entries(&fixture, &assignments, entries).await
    } else {
        admit_preparation_candidate(&fixture, &assignments).await
    };
    fixture
        .db
        .prepare_cluster_topology_operation(admitted.operation_id)
        .await
        .unwrap();
    let (_, plan, _, descriptor) = fixture
        .authority
        .lease_store
        .topology_preparation_input(admitted.operation_id)
        .await
        .unwrap();
    assert!(fixture
        .db
        .stage_cluster_topology_migration_root(admitted.operation_id)
        .await
        .is_err());
    let inventory = CheckpointArtifactInventory {
        deployment_id: descriptor.deployment_id.clone(),
        pipeline_identity: descriptor.parent_pipeline.clone(),
        attempt: CheckpointAttempt::canonical(1),
        assignment_fence: Some(plan.assignment.clone()),
        sink_artifact_intent_protocol: true,
    };
    fixture
        .authority
        .controller
        .begin_topology_checkpoint_cut(
            &fixture.authority.lease.proof(),
            admitted.operation_id,
            &admitted.plan,
            inventory,
        )
        .await
        .unwrap();
    // These are checksummed cut metadata fixtures. Staging must inspect their slots/references
    // without decoding, duplicating or restoring the placeholder state payload.
    let store = ObjectStoreCheckpointStore::new(fixture.authority.checkpoint_store.clone(), "")
        .with_key_group_count(fixture.db.checkpoint_key_groups());
    let mut manifest = CheckpointManifest::new_with_key_group_count(1, 1, store.key_group_count());
    manifest.bind_participant(plan.assignment.participants[0].node_id);
    manifest.assignment_fence = Some(plan.assignment.clone());
    manifest.deployment_id.clone_from(&descriptor.deployment_id);
    manifest
        .pipeline_identity
        .clone_from(&descriptor.parent_pipeline);
    manifest.reassignment_portable = true;
    manifest.source_names = vec!["trades".into()];
    manifest
        .source_offsets
        .insert("trades".into(), ConnectorCheckpoint::new());
    manifest.sink_names = vec!["existing_sink".into()];
    let data = bytes::Bytes::from_static(b"12345678");
    manifest.node_data.object_length = 8;
    manifest.node_data.sha256 = laminar_core::checkpoint::checkpoint_sha256(&data);
    manifest.state_frames = (0..8)
        .map(|vnode| StateFrame {
            key: StateFrameKey::Vnode {
                operator_id: "graph:totals".into(),
                vnode,
            },
            chunk: manifest.node_data.chunk,
            range: ByteRange {
                offset: u64::from(vnode),
                length: 1,
            },
            sha256: laminar_core::checkpoint::checkpoint_sha256(
                &data[usize::from(vnode)..=usize::from(vnode)],
            ),
        })
        .collect();
    let bytes = store.save_checkpoint(&manifest, &[data]).await.unwrap();
    let index = CommittedCheckpointIndex {
        version: laminar_core::checkpoint::COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: descriptor.deployment_id,
        pipeline_identity: descriptor.parent_pipeline,
        epoch: 1,
        checkpoint_id: 1,
        scope: laminar_core::checkpoint::CheckpointScope::Cluster,
        vnode_count: 8,
        assignment_fence: Some(plan.assignment.clone()),
        reassignment_portable: true,
        predecessor: None,
        participants: vec![CommittedParticipantRef::from_manifest(&manifest, &bytes).unwrap()],
        source_names: manifest.source_names.clone(),
        source_offsets: BTreeMap::from([("trades".into(), ConnectorCheckpoint::new())]),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    };
    let reference = CheckpointDecisionStore::new(fixture.authority.checkpoint_store.clone())
        .create_committed_checkpoint(&index)
        .await
        .unwrap();
    fixture
        .authority
        .lease_store
        .record_cluster_outcome(
            &fixture.authority.lease.proof(),
            1,
            1,
            plan.assignment.clone(),
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap();
    fixture
        .authority
        .controller
        .complete_topology_checkpoint_cut(
            &fixture.authority.lease.proof(),
            CheckpointAttempt::canonical(1),
        )
        .await
        .unwrap();
    fixture.db.topology_cut_hold.store(true, Ordering::Release);
    let inventory = fixture.db.catalog_manifest_inventory().unwrap();
    let staged = fixture
        .db
        .stage_cluster_topology_migration_root(admitted.operation_id)
        .await
        .unwrap();
    assert!(staged.migration_root.is_some());
    let root = fixture
        .authority
        .lease_store
        .topology_migration_root(admitted.operation_id)
        .await
        .unwrap()
        .unwrap();
    if add_source {
        assert_eq!(
            root.preserved_objects
                .iter()
                .find(|object| object.name == "totals")
                .unwrap()
                .catalog_generation,
            7,
        );
        assert_eq!(root.format_version, 2);
        assert_eq!(root.source_initializations.len(), 1);
        let source = &root.source_initializations[0];
        assert_eq!(source.name, "added_source");
        assert_eq!(source.checkpoint.offsets["partition-0-next"], "91");
        assert_eq!(source.checkpoint.input_channels, Some(vec![vec![1]]));
        assert_eq!(source.checkpoint.source_assignment_version, None);
    } else {
        assert!(root.source_initializations.is_empty());
    }
    assert_eq!(
        fixture
            .db
            .stage_cluster_topology_migration_root(admitted.operation_id)
            .await
            .unwrap(),
        staged
    );
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), inventory);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.resolutions.load(Ordering::SeqCst),
        usize::from(add_source)
    );
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    fixture.db.shutdown.store(true, Ordering::Release);
    assert!(matches!(
        fixture
            .db
            .stage_cluster_topology_migration_root(admitted.operation_id)
            .await,
        Err(DbError::Shutdown)
    ));
}

fn source_ddl(name: &str, options: &str) -> String {
    format!("CREATE SOURCE {name} (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, value BIGINT NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND) FROM \"planning-source\" ({options})")
}

fn independent_pipeline() -> Vec<String> {
    vec![
        source_ddl("added_source", "'topic' = 'new', 'start' = 'latest'").replacen(
            "id BIGINT NOT NULL",
            "id BIGINT PRIMARY KEY",
            1,
        ),
        "CREATE STREAM added_stream AS SELECT id, value FROM added_source WHERE value > 0".into(),
        "CREATE SINK added_sink FROM added_stream INTO \"planning-sink\" ('topic' = 'new-output')"
            .into(),
    ]
}

fn stateful_additions() -> Vec<laminar_core::cluster::control::CatalogManifestEntry> {
    [
        ("new_total", "CREATE STREAM new_total AS SELECT id, SUM(value) AS total FROM trades GROUP BY id EMIT CHANGES WITH ('retain_history' = '4mb')"),
        ("new_global", "CREATE STREAM new_global AS SELECT SUM(value) AS total FROM trades"),
        ("new_tumble", "CREATE STREAM new_tumble AS SELECT id, TUMBLE(ts, INTERVAL '1' SECOND) AS bucket, SUM(value) AS total FROM trades GROUP BY id, TUMBLE(ts, INTERVAL '1' SECOND) EMIT ON WINDOW CLOSE"),
        ("new_hop", "CREATE STREAM new_hop AS SELECT id, HOP(ts, INTERVAL '1' SECOND, INTERVAL '2' SECOND) AS bucket, SUM(value) AS total FROM trades GROUP BY id, HOP(ts, INTERVAL '1' SECOND, INTERVAL '2' SECOND) EMIT ON WINDOW CLOSE"),
        ("new_session", "CREATE STREAM new_session AS SELECT id, SESSION(ts, INTERVAL '1' SECOND) AS bucket, SUM(value) AS total FROM trades GROUP BY id, SESSION(ts, INTERVAL '1' SECOND) EMIT ON WINDOW CLOSE"),
        ("new_join", "CREATE STREAM new_join AS SELECT l.id AS id, l.value AS left_value, r.value AS right_value FROM trades l JOIN added_source r ON l.id = r.id AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND"),
        ("new_temporal", "CREATE STREAM new_temporal AS SELECT l.id AS id, r.value AS right_value FROM trades l LEFT JOIN added_source FOR SYSTEM_TIME AS OF l.ts AS r ON l.id = r.id"),
    ].into_iter().map(|(name, ddl)| laminar_core::cluster::control::CatalogManifestEntry {
        schema_binding: None,
        canonical_name: name.into(), kind: laminar_core::cluster::control::CatalogObjectKind::Stream,
        catalog_generation: 1, ddl: ddl.into(),
    }).collect()
}

#[tokio::test]
async fn topology_validation_certifies_new_managed_state_at_a_future_only_cut_without_effects() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let mut statements = independent_pipeline();
    statements.extend(stateful_additions().into_iter().map(|entry| entry.ddl));
    let report = fixture.validate(&statements).await.unwrap();
    for (name, codec) in [
        ("new_total", "sql_aggregate_v1"),
        ("new_global", "sql_aggregate_v1"),
        ("new_tumble", "core_window_v1"),
        ("new_hop", "core_window_v1"),
        ("new_session", "core_window_v1"),
        ("new_join", "bounded_interval_join_v3"),
        ("new_temporal", "temporal_join_v1"),
    ] {
        let object = report
            .objects
            .iter()
            .find(|object| object.name == name)
            .unwrap();
        assert_eq!(
            object.transition,
            ClusterTopologyObjectTransition::AddFutureOnly
        );
        assert_eq!(
            object.initialization,
            TopologyInitialization::EmptyManagedStateAtCut
        );
        assert_eq!(object.managed_state_contract.as_deref(), Some(codec));
    }
    assert_eq!(report, fixture.validate(&statements).await.unwrap());
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap().len(), 3);
    fixture.assert_no_effects();
}

async fn object_paths(objects: &dyn ObjectStore) -> Vec<String> {
    let mut paths: Vec<_> = objects
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();
    paths.sort_unstable();
    paths
}

#[tokio::test]
async fn topology_validation_additive_plan_is_deterministic_preserves_state_and_has_no_effects() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let before_paths = object_paths(fixture.authority.checkpoint_store.as_ref()).await;
    let before_authority = fixture.authority.lease_store.load().await.unwrap();
    let before_inventory = fixture.db.catalog_manifest_inventory().unwrap();
    let first = fixture.validate(&independent_pipeline()).await.unwrap();
    let second = fixture.validate(&independent_pipeline()).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(first.scope, TopologyValidationScope::LocalCandidatePlan);
    assert_eq!(first.parent_version.get(), 1);
    assert_eq!(first.target_version.get(), 2);
    assert_ne!(first.parent_pipeline, first.target_pipeline);
    assert_eq!(first.objects.len(), 6);
    assert_eq!(first.required_before_activation.len(), 6);
    assert!(first.requires_processing_pause);
    let preserved = first
        .objects
        .iter()
        .find(|object| object.name == "totals")
        .unwrap();
    assert_eq!(
        preserved.transition,
        ClusterTopologyObjectTransition::Preserve
    );
    assert_eq!(
        preserved.managed_state_contract.as_deref(),
        Some("sql_aggregate_v1")
    );
    assert_eq!(
        preserved.initialization,
        TopologyInitialization::PreserveExactCut
    );
    assert_eq!(preserved.dependencies, ["trades"]);
    let source = first
        .objects
        .iter()
        .find(|object| object.name == "added_source")
        .unwrap();
    assert_eq!(
        source.initialization,
        TopologyInitialization::ResolveSourcePositionsOnce
    );
    let stream = first
        .objects
        .iter()
        .find(|object| object.name == "added_stream")
        .unwrap();
    assert_eq!(
        stream.initialization,
        TopologyInitialization::FutureOnlyAtCut
    );
    assert_eq!(stream.managed_state_contract, None);
    assert_eq!(
        before_inventory,
        fixture.db.catalog_manifest_inventory().unwrap()
    );
    assert_eq!(
        before_paths,
        object_paths(fixture.authority.checkpoint_store.as_ref()).await
    );
    assert_eq!(
        before_authority,
        fixture.authority.lease_store.load().await.unwrap()
    );
    fixture.assert_no_effects();
    let error = fixture
        .db
        .execute(&independent_pipeline()[0])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("LDB-6043"), "{error}");
}

#[tokio::test]
async fn topology_validation_downstream_plan_binds_preserved_incarnation_and_dependency_closure() {
    let fixture = Fixture::new().await;
    // Simulate a durable preexisting stream incarnation through the same replay mechanism.
    // Seal is immutable, so use a second namespace whose original manifest carries generation 7.
    let old = fixture
        .authority
        .manifest_store
        .load()
        .await
        .unwrap()
        .unwrap();
    let mut entries = old.entries.clone();
    entries
        .iter_mut()
        .find(|entry| entry.canonical_name == "totals")
        .unwrap()
        .catalog_generation = 7;
    let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let authority = test_catalog_authority_with_ttl(Arc::clone(&objects), 60_000).await;
    let manifest = laminar_core::cluster::control::CatalogManifest::new(entries).unwrap();
    authority
        .manifest_store
        .seal(&manifest, &authority.lease.proof())
        .await
        .unwrap();
    *fixture.db.catalog_manifest_store.lock() = Some(Arc::clone(&authority.manifest_store));
    fixture
        .db
        .reconcile_catalog_manifest_inventory(&manifest)
        .unwrap();
    let deployment = CheckpointDecisionStore::new(objects)
        .load_or_create_deployment_id()
        .await
        .unwrap();
    authority
        .manifest_store
        .adopt_legacy_topology(
            &authority.lease.proof(),
            uuid::Uuid::from_u128(78).try_into().unwrap(),
            &manifest.reference().unwrap(),
            &deployment,
        )
        .await
        .unwrap();
    let a = fixture
        .validate(&["CREATE STREAM later AS SELECT id, total FROM totals WHERE total > 10".into()])
        .await
        .unwrap();
    let b = fixture
        .validate(&[
            "CREATE STREAM earlier AS SELECT id, total FROM totals WHERE total > 20".into(),
        ])
        .await
        .unwrap();
    let total_a = a
        .objects
        .iter()
        .find(|object| object.name == "totals")
        .unwrap();
    assert_eq!(total_a.catalog_generation, 7);
    assert_eq!(
        Some(total_a),
        b.objects.iter().find(|object| object.name == "totals")
    );
    let later = a
        .objects
        .iter()
        .find(|object| object.name == "later")
        .unwrap();
    assert_eq!(later.dependencies, ["totals"]);
    assert_ne!(a.target_pipeline, b.target_pipeline);
    assert_ne!(a.compatibility_sha256, b.compatibility_sha256);
    fixture.assert_no_effects();
}

#[tokio::test]
async fn topology_validation_rejects_legacy_conflicting_parent_and_changed_resolved_definition() {
    let fixture = Fixture::new().await;
    let error = fixture.validate(&independent_pipeline()).await.unwrap_err();
    assert!(matches!(
        error,
        DbError::Topology(TopologyError::Protocol(_))
    ));
    fixture.adopt().await;
    let error = fixture
        .db
        .validate_cluster_topology_change(TopologyVersion::new(2).unwrap(), &independent_pipeline())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DbError::Topology(TopologyError::Conflict(_))
    ));
    let mut changed = fixture.db.connector_manager.lock().sources()["trades"].clone();
    changed
        .connector_options
        .insert("topic".into(), "uncommitted-change".into());
    fixture.db.connector_manager.lock().register_source(changed);
    let error = fixture.validate(&independent_pipeline()).await.unwrap_err();
    assert!(matches!(
        error,
        DbError::Topology(TopologyError::Conflict(_))
    ));
    fixture.assert_no_effects();
}

#[tokio::test]
async fn topology_validation_rejects_implicit_reset_unsafe_removal_and_unsupported_state_before_live_mutation(
) {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let original = fixture.db.catalog_manifest_inventory().unwrap();
    for sql in [
        "CREATE OR REPLACE STREAM totals AS SELECT id, COUNT(*) AS total FROM trades GROUP BY id",
        "CREATE STREAM IF NOT EXISTS totals AS SELECT * FROM trades",
        "DROP STREAM totals",
        "CREATE STREAM new_state AS SELECT id, ROW_NUMBER() OVER (ORDER BY ts) AS n FROM trades",
        "CREATE STREAM unseeded_retractions AS SELECT id, SUM(total) AS total FROM totals GROUP BY id",
        "CREATE MATERIALIZED VIEW new_mv AS SELECT * FROM trades",
        "CREATE SOURCE plain_ingress (id BIGINT)",
        "CREATE STREAM bad_schema AS SELECT missing_column FROM trades",
        "CREATE STREAM bad_dependency AS SELECT * FROM not_created",
        "CREATE SOURCE a (id INT); CREATE SOURCE b (id INT)",
    ] {
        let error = fixture.validate(&[sql.into()]).await.unwrap_err();
        assert!(!error.to_string().is_empty(), "{sql}");
        assert_eq!(original, fixture.db.catalog_manifest_inventory().unwrap());
        fixture.assert_no_effects();
    }
}

#[tokio::test]
async fn topology_explicit_reset_maps_both_incarnations_and_preserves_the_live_parent() {
    let (fixture, _) = preparation_fixture_with_generation(7).await;
    DbState::Created.store(&fixture.db.state);
    let parent = fixture.db.catalog_manifest_inventory().unwrap();
    let before = fixture.authority.lease_store.load().await.unwrap();
    for query in [
        "SELECT id, SUM(value) AS total FROM trades GROUP BY id EMIT CHANGES WITH ('retain_history' = '4mb')",
        "SELECT value, SUM(id) AS total FROM trades GROUP BY value EMIT CHANGES WITH ('retain_history' = '4mb')",
        "SELECT id, SUM(value) AS total FROM trades GROUP BY id, TUMBLE(ts, INTERVAL '2' SECOND) EMIT FINAL",
    ] {
        let statements = vec!["DROP SINK existing_sink".into(), "DROP STREAM totals".into(),
            format!("CREATE STREAM totals AS {query}"),
            "CREATE SINK existing_sink FROM totals INTO \"planning-sink\" ('topic' = 'changed-output')".into()];
        let (report, target) = fixture.db.plan_cluster_topology_change(TopologyVersion::LEGACY_BASELINE, &statements).await.unwrap();
        assert_eq!(report.statements, statements);
        assert_eq!(target.entries[1].catalog_generation, 8);
        assert_eq!(target.entries[2].catalog_generation, 2);
        let mappings = report.objects.iter().filter(|object| object.name == "totals").collect::<Vec<_>>();
        assert_eq!(mappings.len(), 2);
        assert_eq!((mappings[0].catalog_generation, mappings[0].transition), (7, ClusterTopologyObjectTransition::Remove));
        assert_eq!((mappings[1].catalog_generation, mappings[1].transition), (8, ClusterTopologyObjectTransition::AddFutureOnly));
        assert_eq!(mappings[1].initialization, TopologyInitialization::EmptyManagedStateAtCut);
        assert_ne!(report.parent_pipeline, report.target_pipeline);
        report.validate_catalogs(&laminar_core::cluster::control::CatalogManifest::new(parent.clone()).unwrap(), &target).unwrap();
        let mut omitted = report.clone();
        omitted.objects.retain(|object| !(object.name == "totals" && object.transition == ClusterTopologyObjectTransition::Remove));
        omitted.compatibility_sha256 = omitted.descriptor_digest().unwrap();
        assert!(omitted.validate_catalogs(&laminar_core::cluster::control::CatalogManifest::new(parent.clone()).unwrap(), &target).is_err());
    }
    let statements = vec![
        "DROP SINK existing_sink".into(),
        "DROP STREAM totals".into(),
        "DROP SOURCE trades".into(),
        source_ddl("trades", "'topic' = 'new', 'start' = 'latest'").replace(
            "value BIGINT NOT NULL",
            "value BIGINT NOT NULL, extra BIGINT",
        ),
        parent[1].ddl.clone(),
        parent[2].ddl.clone(),
    ];
    let report = fixture.validate(&statements).await.unwrap();
    assert_eq!(
        report
            .objects
            .iter()
            .filter(|object| object.transition == ClusterTopologyObjectTransition::Remove)
            .count(),
        3
    );
    let source = report
        .objects
        .iter()
        .find(|object| {
            object.name == "trades"
                && object.transition == ClusterTopologyObjectTransition::AddFutureOnly
        })
        .unwrap();
    assert_eq!(source.catalog_generation, 2);
    assert_eq!(
        source.initialization,
        TopologyInitialization::ResolveSourcePositionsOnce
    );
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), parent);
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    fixture.assert_no_effects();
}

#[tokio::test]
async fn topology_validation_reuses_cluster_source_sink_and_changelog_admission() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    for sql in [
        source_ddl("ephemeral", "'durability' = 'ephemeral'"),
        source_ddl("singleton", "'placement' = 'singleton'"),
        "CREATE SINK weak FROM totals INTO \"planning-sink\" ('durability' = 'ephemeral')".into(),
        "CREATE SINK singleton_sink FROM totals INTO \"planning-sink\" ('placement' = 'singleton')".into(),
        "CREATE SINK retracting_to_append FROM totals INTO \"planning-sink\" ('input' = 'append')".into(),
        "CREATE SINK bad_filter FROM totals INTO \"planning-sink\" ('topic' = 'out') WHERE missing_column = 1".into(),
        "CREATE SINK volatile_filter FROM totals INTO \"planning-sink\" ('topic' = 'out') WHERE random() > 0.5".into(),
        "CREATE SINK engine_column_filter FROM totals INTO \"planning-sink\" ('topic' = 'out') WHERE __weight > 0".into(),
    ] {
        let result = fixture.validate(std::slice::from_ref(&sql)).await;
        assert!(result.is_err(), "unsafe candidate was accepted: {sql}");
        fixture.assert_no_effects();
    }
}

#[tokio::test]
async fn topology_stateful_rejects_changelog_input_that_could_retract_the_unavailable_prefix() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let statements = vec![
        source_ddl("weighted", "'input' = 'changelog'")
            .replace("\"planning-source\"", "\"planning-changelog\"")
            .replace(
                "value BIGINT NOT NULL, WATERMARK",
                "value BIGINT NOT NULL, __weight BIGINT NOT NULL, WATERMARK",
            ),
        "CREATE STREAM unseeded_join AS SELECT l.id AS id, l.value AS left_value, r.value AS right_value FROM trades l JOIN weighted r ON l.id = r.id AND r.ts BETWEEN l.ts AND l.ts + INTERVAL '1' SECOND".into(),
    ];
    let error = fixture.validate(&statements).await.unwrap_err();
    assert!(
        error.to_string().contains("without a cut baseline"),
        "{error}"
    );
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap().len(), 3);
    fixture.assert_no_effects();
}

#[tokio::test]
async fn topology_validation_is_read_only_even_when_all_authority_writes_fail() {
    use laminar_core::cluster::testing::{FaultyObjectStore, ObjectStoreFault};
    let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let faulty = Arc::new(FaultyObjectStore::new(inner));
    let fixture = Fixture::with_objects(faulty.clone()).await;
    fixture.adopt().await;
    let before = object_paths(fixture.authority.checkpoint_store.as_ref()).await;
    faulty.set_fault(ObjectStoreFault::FailWrites);
    fixture.validate(&independent_pipeline()).await.unwrap();
    assert_eq!(
        before,
        object_paths(fixture.authority.checkpoint_store.as_ref()).await
    );
    fixture.assert_no_effects();
}

#[tokio::test]
async fn topology_validation_does_not_invent_missing_live_shuffle_scope() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let original = fixture.db.catalog_manifest_inventory().unwrap();
    let receiver = fixture.db.shuffle_receiver.lock().take().unwrap();
    let result = fixture.validate(&independent_pipeline()).await;
    assert!(
        matches!(result, Err(DbError::InvalidOperation(ref reason)) if reason.contains("no complete distributed shuffle"))
    );
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), original);
    fixture.assert_no_effects();
    *fixture.db.shuffle_receiver.lock() = Some(receiver);
    fixture.validate(&independent_pipeline()).await.unwrap();
    fixture.assert_no_effects();
}

#[tokio::test]
async fn topology_validation_cancellation_and_concurrency_do_not_leak_candidate_ownership() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let catalog_lock = fixture.db.topology_ddl_lock.write().await;
    let db = Arc::clone(&fixture.db);
    let task = tokio::spawn(async move {
        db.validate_cluster_topology_change(
            TopologyVersion::LEGACY_BASELINE,
            &independent_pipeline(),
        )
        .await
    });
    tokio::task::yield_now().await;
    assert!(matches!(
        fixture.validate(&independent_pipeline()).await,
        Err(DbError::Topology(TopologyError::PlanningBusy))
    ));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(catalog_lock);
    fixture.validate(&independent_pipeline()).await.unwrap();
    fixture.assert_no_effects();
}

#[tokio::test(start_paused = true)]
async fn topology_validation_deadline_includes_waiting_for_catalog_snapshot() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let catalog_lock = fixture.db.topology_ddl_lock.write().await;
    let db = Arc::clone(&fixture.db);
    let task = tokio::spawn(async move {
        db.validate_cluster_topology_change(
            TopologyVersion::LEGACY_BASELINE,
            &independent_pipeline(),
        )
        .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(DbError::Topology(TopologyError::PlanningTimedOut))
    ));
    drop(catalog_lock);
    fixture.validate(&independent_pipeline()).await.unwrap();
    fixture.assert_no_effects();
}

#[tokio::test]
async fn topology_validation_rejects_request_bounds_comments_and_schema_discovery() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    for statements in [
        vec![], vec!["CREATE SOURCE large (id INT)".repeat(16_384)],
        vec!["CREATE SOURCE each (id INT)".into(); 65],
        vec!["CREATE SOURCE discover FROM \"planning-source\" ('topic' = 'new')".into()],
        vec!["CREATE STREAM commented AS SELECT * FROM trades -- persisted comment".into()],
        vec!["CREATE SINK secret_sink FROM totals INTO \"planning-sink\" ('password' = 'inline-secret')".into()],
    ] {
        assert!(fixture.validate(&statements).await.is_err());
        fixture.assert_no_effects();
    }
}

#[tokio::test]
async fn topology_validation_missing_parent_blob_fails_closed() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let reference = fixture
        .authority
        .manifest_store
        .load()
        .await
        .unwrap()
        .unwrap()
        .reference()
        .unwrap();
    fixture
        .authority
        .checkpoint_store
        .delete(&object_store::path::Path::from(format!(
            "control/catalog-manifest/v1/{}.json",
            reference.sha256
        )))
        .await
        .unwrap();
    assert!(fixture.validate(&independent_pipeline()).await.is_err());
    fixture.assert_no_effects();
}

mod removal {
    //! Explicit retirement preserves surviving contracts and requires the complete old cut.

    use super::*;

    #[tokio::test]
    async fn topology_pipeline_removal_requires_dependency_order_and_preserves_exact_request() {
        let fixture = Fixture::new().await;
        fixture.adopt().await;
        let inventory = fixture.db.catalog_manifest_inventory().unwrap();
        let before = fixture.authority.lease_store.load().await.unwrap();
        let paths = object_paths(fixture.authority.checkpoint_store.as_ref()).await;
        let mut statements = vec![
            "DROP SINK existing_sink".into(),
            "DROP STREAM totals".into(),
            "DROP SOURCE trades".into(),
        ];
        statements.extend(independent_pipeline());
        let report = fixture.validate(&statements).await.unwrap();
        assert_eq!(report.statements, statements);
        for entry in &inventory {
            let object = report
                .objects
                .iter()
                .find(|object| object.name == entry.canonical_name)
                .unwrap();
            assert_eq!(object.transition, ClusterTopologyObjectTransition::Remove);
            assert_eq!(object.initialization, TopologyInitialization::RetireAtCut);
            assert_eq!(object.catalog_generation, entry.catalog_generation);
        }
        assert_eq!(report.objects.iter().filter(|object| object.transition == ClusterTopologyObjectTransition::AddFutureOnly).count(), 3);
        assert!(report
            .objects
            .iter()
            .find(|object| object.name == "totals")
            .unwrap()
            .managed_state_contract
            .is_some());
        for statements in [
            vec![
                "DROP SOURCE trades".into(),
                "DROP STREAM totals".into(),
                "DROP SINK existing_sink".into(),
            ],
            vec!["DROP SOURCE IF EXISTS missing".into()],
            vec!["DROP STREAM IF EXISTS missing".into()],
            vec!["DROP SOURCE trades CASCADE".into()],
            vec!["DROP STREAM totals CASCADE".into()],
            vec![
                "DROP SINK existing_sink".into(),
                "DROP STREAM totals /* hidden */".into(),
            ],
        ] {
            assert!(
                fixture.validate(&statements).await.is_err(),
                "{statements:?}"
            );
        }
        assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), inventory);
        assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
        assert_eq!(
            object_paths(fixture.authority.checkpoint_store.as_ref()).await,
            paths
        );
        fixture.assert_no_effects();
        fixture.db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn topology_sink_removal_validation_is_effect_free_and_preserves_upstream_contracts() {
        let fixture = Fixture::new().await;
        fixture.adopt().await;
        let parent = fixture
            .authority
            .manifest_store
            .load()
            .await
            .unwrap()
            .unwrap();
        let before = fixture.authority.lease_store.load().await.unwrap();
        let paths = object_paths(fixture.authority.checkpoint_store.as_ref()).await;
        let statements = vec!["DROP SINK IF EXISTS existing_sink".into()];
        let (report, target) = fixture
            .db
            .plan_cluster_topology_change(TopologyVersion::LEGACY_BASELINE, &statements)
            .await
            .unwrap();
        assert_eq!(report.statements, statements);
        assert_eq!(report.objects.len(), 3);
        assert_eq!(target.entries, parent.entries[..2]);
        let removed = report
            .objects
            .iter()
            .find(|object| object.name == "existing_sink")
            .unwrap();
        assert_eq!(removed.transition, ClusterTopologyObjectTransition::Remove);
        assert_eq!(removed.initialization, TopologyInitialization::RetireAtCut);
        assert!(report
            .objects
            .iter()
            .filter(|object| object.name != "existing_sink")
            .all(|object| {
                object.transition == ClusterTopologyObjectTransition::Preserve
                    && object.initialization == TopologyInitialization::PreserveExactCut
            }));
        report.validate_catalogs(&parent, &target).unwrap();
        assert_eq!(fixture.validate(&statements).await.unwrap(), report);
        for statements in [
            vec!["DROP SINK IF EXISTS missing".into()],
            vec!["DROP SINK existing_sink CASCADE".into()],
            vec![
                "DROP SINK existing_sink".into(),
                "DROP SINK existing_sink".into(),
            ],
        ] {
            assert!(matches!(
                fixture.validate(&statements).await,
                Err(DbError::Topology(TopologyError::Unsupported(_)))
            ));
        }
        for sql in ["DROP SOURCE trades", "DROP STREAM totals"] {
            assert!(fixture.validate(&[sql.into()]).await.is_err());
        }
        assert_eq!(
            fixture.db.catalog_manifest_inventory().unwrap(),
            parent.entries
        );
        assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
        assert_eq!(
            object_paths(fixture.authority.checkpoint_store.as_ref()).await,
            paths
        );
        fixture.assert_no_effects();
        fixture.db.shutdown().await.unwrap();
    }
}
