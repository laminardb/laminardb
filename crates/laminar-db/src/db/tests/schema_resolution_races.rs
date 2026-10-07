//! Metadata suspension exercises publication and activation boundaries.

use super::*;
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode, SourceStart,
    SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_connectors::schema::resolution::{SchemaCapabilities, SchemaPreparation};
use laminar_connectors::testing::MockSourceConnector;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
struct MetadataGate {
    entered: Notify,
    release: Notify,
    discoveries: AtomicUsize,
    starts: AtomicUsize,
}

struct SuspendedMetadataSource {
    inner: MockSourceConnector,
    gate: Arc<MetadataGate>,
}

#[async_trait]
impl SourceConnector for SuspendedMetadataSource {
    fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            SourceInputMode::AppendOnly,
        ))
    }

    async fn discover_schema(&mut self, _: &HashMap<String, String>) -> Result<(), ConnectorError> {
        self.gate.discoveries.fetch_add(1, Ordering::SeqCst);
        self.gate.entered.notify_one();
        self.gate.release.notified().await;
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        self.gate.starts.fetch_add(1, Ordering::SeqCst);
        self.inner.start(request).await
    }

    async fn poll_batch(&mut self, limit: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        self.inner.poll_batch(limit).await
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.inner.checkpoint()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.inner.close().await
    }
}

fn builder(gate: &Arc<MetadataGate>) -> crate::LaminarDbBuilder {
    let gate = Arc::clone(gate);
    LaminarDB::builder().register_connector(move |registry| {
        registry.register_source(
            "suspended-metadata",
            ConnectorInfo {
                schema_capabilities: SchemaCapabilities::metadata(
                    &[],
                    false,
                    SchemaPreparation::None,
                ),
                name: "suspended-metadata".into(),
                display_name: "suspended-metadata".into(),
                version: "1".into(),
                is_source: true,
                is_sink: false,
                config_keys: vec![],
            },
            Arc::new(move |_| {
                Ok(Box::new(SuspendedMetadataSource {
                    inner: MockSourceConnector::new(),
                    gate: Arc::clone(&gate),
                }))
            }),
        )
    })
}

async fn entered(gate: &MetadataGate) {
    tokio::time::timeout(std::time::Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
}

fn create(db: &Arc<LaminarDB>) -> tokio::task::JoinHandle<Result<ExecuteResult, DbError>> {
    let db = Arc::clone(db);
    tokio::spawn(async move {
        db.execute("CREATE SOURCE events FROM \"suspended-metadata\"")
            .await
    })
}

#[tokio::test]
async fn concurrent_schema_creates_share_one_committed_definition_without_duplicate_discovery() {
    let gate = Arc::new(MetadataGate::default());
    let db = builder(&gate).build().await.unwrap();
    let first = create(&db);
    entered(&gate).await;
    let duplicate = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.execute("CREATE SOURCE IF NOT EXISTS events FROM \"suspended-metadata\"")
                .await
        })
    };
    assert!(db.catalog.get_source("events").is_none());
    assert!(tokio::time::timeout(
        std::time::Duration::from_secs(2),
        db.execute("SHOW SOURCES")
    )
    .await
    .unwrap()
    .is_ok());
    gate.release.notify_one();
    first.await.unwrap().unwrap();
    duplicate.await.unwrap().unwrap();
    assert_eq!(gate.discoveries.load(Ordering::SeqCst), 1);
    assert_eq!(gate.starts.load(Ordering::SeqCst), 0);
    assert_eq!(db.connector_manager.lock().sources().len(), 1);
}

#[tokio::test]
async fn changed_dependency_generation_fences_metadata_completion() {
    let gate = Arc::new(MetadataGate::default());
    let db = builder(&gate).build().await.unwrap();
    db.execute("CREATE SOURCE upstream FROM GENERATOR")
        .await
        .unwrap();
    let pending = create(&db);
    entered(&gate).await;
    // Model an authoritative generation change arriving while metadata I/O is suspended.
    let catalog_guard = db.topology_ddl_lock.write().await;
    db.connector_manager
        .lock()
        .set_local_schema_generation("upstream", 2);
    assert_eq!(
        db.connector_manager.lock().sources()["upstream"].catalog_generation,
        2
    );
    drop(catalog_guard);
    gate.release.notify_one();
    let error = pending.await.unwrap().unwrap_err();
    assert!(
        error.to_string().contains("dependencies changed"),
        "{error}"
    );
    assert!(db.catalog.get_source("events").is_none());
    assert!(db
        .connector_manager
        .lock()
        .schema_binding("events")
        .is_none());
    assert_eq!(gate.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelled_metadata_creation_releases_ownership_for_a_clean_retry() {
    let gate = Arc::new(MetadataGate::default());
    let db = builder(&gate).build().await.unwrap();
    let cancelled = create(&db);
    entered(&gate).await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert!(db.catalog.get_source("events").is_none());
    let retry = create(&db);
    entered(&gate).await;
    gate.release.notify_one();
    retry.await.unwrap().unwrap();
    assert_eq!(gate.discoveries.load(Ordering::SeqCst), 2);
    assert_eq!(
        db.catalog.get_source("events").unwrap().schema,
        laminar_connectors::testing::mock_schema()
    );
    assert_eq!(gate.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_during_metadata_cannot_publish_a_local_durable_binding() {
    let directory = tempfile::tempdir().unwrap();
    let gate = Arc::new(MetadataGate::default());
    let mut db = builder(&gate).build().await.unwrap();
    Arc::get_mut(&mut db).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        });
    let pending = create(&db);
    entered(&gate).await;
    db.shutdown.store(true, Ordering::Release);
    gate.release.notify_one();
    assert!(matches!(pending.await.unwrap(), Err(DbError::Shutdown)));
    assert!(db.catalog.get_source("events").is_none());
    assert!(!directory
        .path()
        .join("catalog/schema-contracts-v1.json")
        .exists());
    assert_eq!(gate.starts.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn lost_leader_lease_during_metadata_keeps_manifest_and_local_catalog_empty() {
    let gate = Arc::new(MetadataGate::default());
    let authority = test_catalog_authority(Arc::new(object_store::memory::InMemory::new())).await;
    let db = builder(&gate)
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&authority.manifest_store))
        .build()
        .await
        .unwrap();
    let pending = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.execute_cluster_bootstrap("CREATE SOURCE events FROM \"suspended-metadata\"")
                .await
        })
    };
    entered(&gate).await;
    authority.lease_tx.send_replace(None);
    gate.release.notify_one();
    let error = pending.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("leader lease"), "{error}");
    assert!(authority.manifest_store.load().await.unwrap().is_none());
    assert!(db.catalog.get_source("events").is_none());
    assert_eq!(gate.starts.load(Ordering::SeqCst), 0);
}

#[cfg(all(feature = "cluster", feature = "kafka"))]
#[tokio::test]
async fn cluster_restart_and_join_replay_concrete_kafka_binding_when_external_latest_changes() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let native = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"label","type":"string"}]}"#;
    Mock::given(method("GET"))
        .and(path("/subjects/events-value/versions/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id":73,"version":4,"schema":native,"schemaType":"AVRO"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let authority =
        test_catalog_authority_with_ttl(Arc::new(object_store::memory::InMemory::new()), 60_000)
            .await;
    let source = format!(
        "CREATE SOURCE events FROM KAFKA (
        'bootstrap.servers'='unused:9092', 'topic'='events', 'group.id'='schema-join',
        'schema.registry.url'='{}') FORMAT AVRO",
        server.uri()
    );
    let definitions = vec![
        source,
        "CREATE STREAM output AS SELECT label, id FROM events".into(),
    ];
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&authority.controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
        .catalog_manifest_store(Arc::clone(&authority.manifest_store))
        .build()
        .await
        .unwrap();
    db.execute_cluster_bootstrap_batch(&definitions)
        .await
        .unwrap();
    let committed = db
        .connector_manager
        .lock()
        .schema_binding("events")
        .unwrap()
        .clone();
    assert_eq!(committed.value.as_ref().unwrap().identity["id"], "73");
    drop(db);
    server.verify().await;
    server.reset().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503))
        .expect(0)
        .mount(&server)
        .await;
    for node in [1, 2] {
        let (controller, _lease) = catalog_authority_controller(
            laminar_core::cluster::discovery::NodeId(node),
            authority.lease.owner.clone(),
        );
        let restored = LaminarDB::builder()
            .cluster_controller(controller)
            .cluster_checkpoint_object_store(Arc::clone(&authority.checkpoint_store))
            .catalog_manifest_store(Arc::clone(&authority.manifest_store))
            .build()
            .await
            .unwrap();
        restored
            .execute_cluster_bootstrap_batch(&definitions)
            .await
            .unwrap();
        assert_eq!(
            restored.connector_manager.lock().schema_binding("events"),
            Some(&committed)
        );
        assert_eq!(
            restored
                .ctx
                .table_provider("output")
                .await
                .unwrap()
                .schema()
                .field(0)
                .name(),
            "label"
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}
