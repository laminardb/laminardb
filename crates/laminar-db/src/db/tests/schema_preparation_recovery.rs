//! Authorized preparation survives cancellation without activating an unpublished sink.

use super::*;
use async_trait::async_trait;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{SinkConnector, SinkContract, WriteResult};
use laminar_connectors::error::ConnectorError;
use laminar_connectors::schema::resolution::{
    NativeSchema, SchemaBinding, SchemaCapabilities, SchemaPreparation,
};
use laminar_connectors::testing::MockSinkConnector;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

const INPUT: &str = "CREATE SOURCE input (id BIGINT NOT NULL, value VARCHAR NOT NULL)";
const OUTPUT: &str =
    "CREATE SINK output FROM input INTO \"prepared-resource\" ('auto.create'='true')";

struct PreparationGate {
    entered: Notify,
    release: Notify,
    block_first: AtomicBool,
    creations: AtomicUsize,
    opens: AtomicUsize,
}

impl PreparationGate {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: Notify::new(),
            block_first: AtomicBool::new(true),
            creations: AtomicUsize::new(0),
            opens: AtomicUsize::new(0),
        }
    }
}

struct PreparedResourceSink {
    inner: MockSinkConnector,
    gate: Arc<PreparationGate>,
}

#[async_trait]
impl SinkConnector for PreparedResourceSink {
    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        self.inner.contract(config)
    }

    async fn prepare_schema(
        &mut self,
        config: &ConnectorConfig,
        binding: &mut SchemaBinding,
    ) -> Result<(), ConnectorError> {
        if binding.value.is_some() {
            return Ok(());
        }
        assert_eq!(config.get("auto.create"), Some("true"));
        // Model an idempotent remote create that can finish before catalog publication.
        let _ = self
            .gate
            .creations
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst);
        if self.gate.block_first.swap(false, Ordering::SeqCst) {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
        }
        if config.get("corrupt.contract") == Some("true") {
            binding.logical = arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Utf8, false),
            ]);
        }
        binding.value = Some(NativeSchema {
            format: "test-resource".into(),
            identity: std::collections::BTreeMap::from([("resource_id".into(), "fixed-1".into())]),
            definition: serde_json::json!({"schema": binding.logical}),
            references: Vec::new(),
        });
        Ok(())
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.gate.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.open(config).await
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        self.inner.write_batch(batch).await
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn suggested_write_timeout(&self) -> std::time::Duration {
        self.inner.suggested_write_timeout()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.inner.close().await
    }
}

async fn durable_database(
    directory: &std::path::Path,
    gate: &Arc<PreparationGate>,
) -> Arc<LaminarDB> {
    let gate = Arc::clone(gate);
    let mut db = LaminarDB::builder()
        .register_connector(move |registry| {
            registry.register_sink(
                "prepared-resource",
                ConnectorInfo {
                    schema_capabilities: SchemaCapabilities::metadata(
                        &[],
                        true,
                        SchemaPreparation::ExplicitTableCreation,
                    ),
                    name: "prepared-resource".into(),
                    display_name: "prepared-resource".into(),
                    version: "1".into(),
                    is_source: false,
                    is_sink: true,
                    config_keys: vec![],
                },
                Arc::new(move |_, _| {
                    Ok(Box::new(PreparedResourceSink {
                        inner: MockSinkConnector::new(),
                        gate: Arc::clone(&gate),
                    }))
                }),
            )
        })
        .build()
        .await
        .unwrap();
    Arc::get_mut(&mut db).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.into()),
            ..Default::default()
        });
    db.execute(INPUT).await.unwrap();
    db
}

async fn suspended_preparation(
    db: &Arc<LaminarDB>,
    gate: &PreparationGate,
) -> tokio::task::JoinHandle<Result<ExecuteResult, DbError>> {
    let db = Arc::clone(db);
    let pending = tokio::spawn(async move { db.execute(OUTPUT).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    pending
}

#[tokio::test]
async fn cancellation_after_external_prepare_retries_without_a_second_resource_or_early_activation()
{
    let directory = tempfile::tempdir().unwrap();
    let gate = Arc::new(PreparationGate::new());
    let db = durable_database(directory.path(), &gate).await;
    let pending = suspended_preparation(&db, &gate).await;
    assert_eq!(gate.creations.load(Ordering::SeqCst), 1);
    assert!(db
        .connector_manager
        .lock()
        .schema_binding("output")
        .is_none());
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    assert!(!directory
        .path()
        .join("catalog/schema-contracts-v1.json")
        .exists());
    drop(db);
    let restored = durable_database(directory.path(), &gate).await;
    restored.execute(OUTPUT).await.unwrap();
    let committed = restored
        .connector_manager
        .lock()
        .schema_binding("output")
        .unwrap()
        .clone();
    assert_eq!(
        committed.value.as_ref().unwrap().identity["resource_id"],
        "fixed-1"
    );
    assert_eq!(gate.creations.load(Ordering::SeqCst), 1);
    assert_eq!(gate.opens.load(Ordering::SeqCst), 0);
    drop(restored);
    let restarted = durable_database(directory.path(), &gate).await;
    restarted.execute(OUTPUT).await.unwrap();
    assert_eq!(
        restarted.connector_manager.lock().schema_binding("output"),
        Some(&committed)
    );
    assert_eq!(gate.creations.load(Ordering::SeqCst), 1);
    assert_eq!(gate.opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_after_external_preparation_fences_publication_and_preserves_retry_identity() {
    let directory = tempfile::tempdir().unwrap();
    let gate = Arc::new(PreparationGate::new());
    let db = durable_database(directory.path(), &gate).await;
    let pending = suspended_preparation(&db, &gate).await;
    db.shutdown.store(true, Ordering::Release);
    gate.release.notify_one();
    assert!(matches!(pending.await.unwrap(), Err(DbError::Shutdown)));
    assert!(!directory
        .path()
        .join("catalog/schema-contracts-v1.json")
        .exists());
    assert!(db
        .connector_manager
        .lock()
        .schema_binding("output")
        .is_none());
    drop(db);
    let restored = durable_database(directory.path(), &gate).await;
    restored.execute(OUTPUT).await.unwrap();
    assert_eq!(
        restored
            .connector_manager
            .lock()
            .schema_binding("output")
            .unwrap()
            .value
            .as_ref()
            .unwrap()
            .identity["resource_id"],
        "fixed-1"
    );
    assert_eq!(gate.creations.load(Ordering::SeqCst), 1);
    assert_eq!(gate.opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn external_preparation_cannot_replace_the_bound_query_schema_before_publication() {
    let directory = tempfile::tempdir().unwrap();
    let gate = Arc::new(PreparationGate::new());
    gate.block_first.store(false, Ordering::SeqCst);
    let db = durable_database(directory.path(), &gate).await;
    let sql = OUTPUT.replace(
        "'auto.create'='true'",
        "'auto.create'='true','corrupt.contract'='true'",
    );
    let error = db.execute(&sql).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sink 'output' schema preparation"),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("changed the resolved query schema"),
        "{error}"
    );
    assert!(db
        .connector_manager
        .lock()
        .schema_binding("output")
        .is_none());
    assert!(!directory
        .path()
        .join("catalog/schema-contracts-v1.json")
        .exists());
    assert_eq!(gate.creations.load(Ordering::SeqCst), 1);
    assert_eq!(gate.opens.load(Ordering::SeqCst), 0);
}
