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

struct PlanningSource(Arc<AtomicUsize>);

fn forbidden_effect(effects: &AtomicUsize) -> ConnectorError {
    effects.fetch_add(1, Ordering::SeqCst);
    ConnectorError::ConfigurationError("validation invoked a connector lifecycle effect".into())
}

#[async_trait]
impl SourceConnector for PlanningSource {
    fn schema(&self) -> arrow_schema::SchemaRef {
        crate::temporal_test_source::schema()
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
        Ok(
            SourceContract::new(consistency, topology, SourceInputMode::AppendOnly)
                .with_row_positions(SourceRowPositionCapability::OrderedDeterministic),
        )
    }

    async fn start(&mut self, _: SourceStart) -> Result<(), ConnectorError> {
        Err(forbidden_effect(&self.0))
    }
    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        Err(forbidden_effect(&self.0))
    }
    fn checkpoint(&self) -> SourceCheckpoint {
        self.0.fetch_add(1, Ordering::SeqCst);
        SourceCheckpoint::new()
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        Err(forbidden_effect(&self.0))
    }
    async fn discover_schema(&mut self, _: &HashMap<String, String>) -> Result<(), ConnectorError> {
        Err(forbidden_effect(&self.0))
    }
}

struct PlanningSink(Arc<AtomicUsize>);

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
    async fn open(&mut self, _: &ConnectorConfig) -> Result<(), ConnectorError> {
        Err(forbidden_effect(&self.0))
    }
    async fn write_batch(&mut self, _: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Err(forbidden_effect(&self.0))
    }
    async fn begin_epoch(&mut self, _: u64) -> Result<(), ConnectorError> {
        Err(forbidden_effect(&self.0))
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
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
}

impl Fixture {
    async fn new() -> Self {
        let objects: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        Self::with_objects(objects).await
    }

    async fn with_objects(objects: Arc<dyn ObjectStore>) -> Self {
        let authority = test_catalog_authority_with_ttl(objects, 60_000).await;
        let effects = Arc::new(AtomicUsize::new(0));
        let factory_effects = Arc::clone(&effects);
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
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            })
            .register_connector(move |registry| {
                let source_effects = Arc::clone(&factory_effects);
                registry.register_source(
                    "planning-source",
                    ConnectorInfo {
                        name: "planning-source".into(),
                        display_name: "planning source".into(),
                        version: "1".into(),
                        is_source: true,
                        is_sink: false,
                        config_keys: vec![],
                    },
                    Arc::new(move |_| Ok(Box::new(PlanningSource(Arc::clone(&source_effects))))),
                )?;
                registry.register_sink(
                    "planning-sink",
                    ConnectorInfo {
                        name: "planning-sink".into(),
                        display_name: "planning sink".into(),
                        version: "1".into(),
                        is_source: false,
                        is_sink: true,
                        config_keys: vec![],
                    },
                    Arc::new(move |_, _| Ok(Box::new(PlanningSink(Arc::clone(&factory_effects))))),
                )
            })
            .build()
            .await
            .unwrap();
        db.execute_cluster_bootstrap_batch(&[
            source_ddl("trades", "'topic' = 'old'"),
            "CREATE STREAM totals AS SELECT id, SUM(value) AS total FROM trades GROUP BY id EMIT CHANGES WITH ('retain_history' = '4mb')".into(),
            "CREATE SINK existing_sink FROM totals INTO \"planning-sink\" ('topic' = 'old-output')".into(),
        ]).await.unwrap();
        Self {
            db,
            authority,
            effects,
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

fn source_ddl(name: &str, options: &str) -> String {
    format!("CREATE SOURCE {name} (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL, value BIGINT NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND) FROM \"planning-source\" ({options})")
}

fn independent_pipeline() -> Vec<String> {
    vec![
        source_ddl("added_source", "'topic' = 'new', 'start' = 'latest'"),
        "CREATE STREAM added_stream AS SELECT id, value FROM added_source WHERE value > 0".into(),
        "CREATE SINK added_sink FROM added_stream INTO \"planning-sink\" ('topic' = 'new-output')"
            .into(),
    ]
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
    assert_eq!(preserved.managed_state_contract, Some("sql_aggregate_v1"));
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
async fn topology_validation_rejects_replacements_removals_and_new_state_before_live_mutation() {
    let fixture = Fixture::new().await;
    fixture.adopt().await;
    let original = fixture.db.catalog_manifest_inventory().unwrap();
    for sql in [
        "CREATE OR REPLACE STREAM totals AS SELECT id, COUNT(*) AS total FROM trades GROUP BY id",
        "CREATE STREAM IF NOT EXISTS totals AS SELECT * FROM trades",
        "DROP STREAM totals",
        "CREATE STREAM new_state AS SELECT id, SUM(value) AS total FROM trades GROUP BY id",
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
