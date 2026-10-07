//! Schema creation, durable replay, and projection regression coverage.

use super::*;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use std::sync::atomic::Ordering;

struct RetryTableMetadata(std::sync::Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl laminar_connectors::reference::ReferenceTableSource for RetryTableMetadata {
    async fn resolve_schema(
        &mut self,
        config: &laminar_connectors::config::ConnectorConfig,
        _explicit: Option<arrow::datatypes::SchemaRef>,
    ) -> Result<
        laminar_connectors::schema::resolution::SchemaBinding,
        laminar_connectors::error::ConnectorError,
    > {
        use laminar_connectors::schema::resolution::{
            logical_binding, SchemaDirection, SchemaOrigin,
        };
        if self.0.fetch_add(1, Ordering::SeqCst) != 0 {
            return Err(laminar_connectors::error::ConnectorError::ConnectionFailed(
                "metadata unavailable".into(),
            ));
        }
        logical_binding(
            config,
            SchemaDirection::Source,
            SchemaOrigin::Metadata,
            &Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int64,
                false,
            )])),
        )
    }

    async fn poll_snapshot(
        &mut self,
    ) -> Result<Option<RecordBatch>, laminar_connectors::error::ConnectorError> {
        panic!("metadata discovery cannot consume snapshot rows")
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }
}

#[tokio::test]
async fn committed_table_retry_does_not_rediscover_unavailable_metadata() {
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::schema::resolution::{SchemaCapabilities, SchemaPreparation};
    let directory = tempfile::tempdir().unwrap();
    let discoveries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = discoveries.clone();
    let mut db = LaminarDB::builder()
        .register_connector(move |registry| {
            registry.register_table_source(
                "retry-table",
                ConnectorInfo {
                    schema_capabilities: SchemaCapabilities::metadata(
                        &[],
                        false,
                        SchemaPreparation::None,
                    ),
                    name: "retry-table".into(),
                    display_name: "Retry Table".into(),
                    version: "0.1.0".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(move |_, _| Ok(Box::new(RetryTableMetadata(observed.clone())))),
            )
        })
        .build()
        .await
        .unwrap();
    Arc::get_mut(&mut db).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        });
    const DDL: &str = "CREATE TABLE dimension (PRIMARY KEY(id)) WITH (connector='retry-table')";
    db.execute(DDL).await.unwrap();
    let binding = db
        .connector_manager
        .lock()
        .schema_binding("dimension")
        .unwrap()
        .clone();
    assert!(directory
        .path()
        .join("catalog/schema-contracts-v1.json")
        .exists());
    let result = db
        .execute(&DDL.replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1))
        .await
        .unwrap();
    assert!(matches!(result, ExecuteResult::Ddl(info) if !info.applied));
    assert_eq!(discoveries.load(Ordering::SeqCst), 1);
    assert_eq!(
        db.connector_manager.lock().schema_binding("dimension"),
        Some(&binding)
    );
}

#[tokio::test]
async fn omitted_generator_fields_are_resolved_before_stream_binding() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE generated FROM GENERATOR")
        .await
        .unwrap();
    let fields = db.catalog.get_source("generated").unwrap().schema.clone();
    assert_eq!(
        fields
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["seq", "ts_ms", "value"]
    );
    let binding = db
        .connector_manager
        .lock()
        .schema_binding("generated")
        .unwrap()
        .clone();
    assert_eq!(binding.logical, *fields);
    assert_eq!(
        binding.origin,
        laminar_core::schema_binding::SchemaOrigin::BuiltIn
    );
    db.execute("CREATE STREAM reordered AS SELECT value AS label, seq AS id FROM generated")
        .await
        .unwrap();
    let schema = db.ctx.table_provider("reordered").await.unwrap().schema();
    assert_eq!(schema.field(0).name(), "label");
    assert_eq!(schema.field(1).name(), "id");
}

#[tokio::test]
async fn explicit_fixed_protocol_fields_cannot_replace_the_decoder_layout() {
    let db = LaminarDB::open().unwrap();
    let error = db
        .execute("CREATE SOURCE broken (seq VARCHAR) FROM GENERATOR")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("protocol"), "{error}");
    assert!(db.catalog.get_source("broken").is_none());
    assert!(db
        .connector_manager
        .lock()
        .schema_binding("broken")
        .is_none());
}

#[cfg(feature = "files")]
#[tokio::test]
async fn file_sink_derives_query_aliases_and_order_without_columns() {
    let directory = tempfile::tempdir().unwrap();
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE generated FROM GENERATOR")
        .await
        .unwrap();
    db.execute("CREATE STREAM renamed AS SELECT value AS label, seq AS id FROM generated")
        .await
        .unwrap();
    let ddl = format!(
        "CREATE SINK output FROM renamed INTO FILES (path = '{}') FORMAT PARQUET",
        directory.path().to_string_lossy().replace('\\', "/")
    );
    db.execute(&ddl).await.unwrap();
    let binding = db
        .connector_manager
        .lock()
        .schema_binding("output")
        .unwrap()
        .clone();
    assert_eq!(
        binding.origin,
        laminar_core::schema_binding::SchemaOrigin::Query
    );
    assert_eq!(binding.logical.field(0).name(), "label");
    assert_eq!(binding.logical.field(1).name(), "id");
    assert_eq!(
        std::fs::read_dir(directory.path()).unwrap().count(),
        0,
        "resolution must not publish files"
    );
}

#[tokio::test]
async fn local_schema_journal_replays_original_omitted_ddl_without_discovery() {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "payload",
            DataType::Struct(vec![Field::new("amount", DataType::Decimal128(19, 6), true)].into()),
            true,
        ),
    ]));
    let (mut writer, discoveries) = fake_source_db("durable-metadata", Some(schema.clone())).await;
    Arc::get_mut(&mut writer).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        });
    const DDL: &str = "CREATE SOURCE events FROM \"durable-metadata\"";
    writer.execute(DDL).await.unwrap();
    assert_eq!(discoveries.load(Ordering::SeqCst), 1);
    let first = writer
        .connector_manager
        .lock()
        .schema_binding("events")
        .unwrap()
        .clone();
    let bytes = std::fs::read(directory.path().join("catalog/schema-contracts-v1.json")).unwrap();
    assert!(String::from_utf8(bytes)
        .unwrap()
        .contains(DDL.replace('"', "\\\"").as_str()));
    drop(writer);

    let (mut restored, discoveries) = fake_source_db("durable-metadata", None).await;
    Arc::get_mut(&mut restored).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        });
    restored.execute(DDL).await.unwrap();
    restored
        .execute("CREATE SOURCE IF NOT EXISTS events FROM \"durable-metadata\"")
        .await
        .unwrap();
    assert_eq!(
        discoveries.load(Ordering::SeqCst),
        0,
        "restart cannot select metadata again"
    );
    assert_eq!(
        restored.catalog.get_source("events").unwrap().schema,
        schema
    );
    assert_eq!(
        restored.connector_manager.lock().schema_binding("events"),
        Some(&first)
    );
    let error = restored
        .execute("CREATE SOURCE other FROM \"durable-metadata\"")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("schema resolution"));
}

#[tokio::test]
async fn local_drop_create_changes_schema_generation_even_for_identical_fields() {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let (mut db, discoveries) = fake_source_db("generation-metadata", Some(schema)).await;
    Arc::get_mut(&mut db).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        });
    const DDL: &str = "CREATE SOURCE events FROM \"generation-metadata\"";
    db.execute(DDL).await.unwrap();
    assert_eq!(
        db.connector_manager.lock().sources()["events"].catalog_generation,
        1
    );
    db.execute("DROP SOURCE events").await.unwrap();
    db.execute(DDL).await.unwrap();
    assert_eq!(discoveries.load(Ordering::SeqCst), 2);
    assert_eq!(
        db.connector_manager.lock().sources()["events"].catalog_generation,
        3
    );
}

#[tokio::test]
async fn invalid_source_semantics_are_rejected_before_durable_publication() {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "_op",
        DataType::Utf8,
        false,
    )]));
    let (mut db, _) = fake_source_db("reserved-metadata", Some(schema)).await;
    Arc::get_mut(&mut db).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        });
    assert!(db
        .execute("CREATE SOURCE broken FROM \"reserved-metadata\"")
        .await
        .is_err());
    assert!(db.catalog.get_source("broken").is_none());
    assert!(!directory
        .path()
        .join("catalog/schema-contracts-v1.json")
        .exists());
}

#[tokio::test]
async fn durable_configuration_drift_cannot_rediscover_latest() {
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let (mut db, _) = fake_source_db("drift-metadata", Some(schema)).await;
    Arc::get_mut(&mut db).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        });
    db.execute("CREATE SOURCE events FROM \"drift-metadata\"")
        .await
        .unwrap();
    drop(db);
    let (mut db, discoveries) = fake_source_db("drift-metadata", None).await;
    Arc::get_mut(&mut db).unwrap().config.checkpoint =
        Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        });
    let error = db
        .execute("CREATE SOURCE events FROM \"drift-metadata\" (topic = 'changed')")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("differs from the original DDL"),
        "{error}"
    );
    assert_eq!(discoveries.load(Ordering::SeqCst), 0);
    assert!(db.catalog.get_source("events").is_none());
}

#[tokio::test]
async fn describe_exposes_frozen_contract_and_duplicate_create_has_no_resolution() {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let (db, discoveries) = fake_source_db("describe-metadata", Some(schema)).await;
    db.execute("CREATE SOURCE events FROM \"describe-metadata\"")
        .await
        .unwrap();
    let count = discoveries.load(Ordering::SeqCst);
    assert!(db
        .execute("CREATE SOURCE events FROM \"describe-metadata\"")
        .await
        .is_err());
    assert_eq!(discoveries.load(Ordering::SeqCst), count);
    let rows = db.build_describe("events").unwrap();
    assert_eq!(rows.num_rows(), 1);
    assert_eq!(rows.schema().field(0).name(), "column_name");
    let origin = rows
        .column_by_name("schema_origin")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(origin.value(0), "metadata");
    let generation = rows
        .column_by_name("contract_generation")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(generation.value(0), 1);
    assert!(!rows
        .column_by_name("schema_fingerprint")
        .unwrap()
        .is_null(0));
}
