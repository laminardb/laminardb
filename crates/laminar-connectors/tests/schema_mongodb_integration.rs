//! Real collection contracts; requires an explicitly supplied isolated MongoDB 8 replica set.

#![cfg(feature = "mongodb-cdc")]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::registry::ConnectorRegistry;
use laminar_connectors::schema::resolution::SchemaOrigin;
use mongodb::bson::doc;

async fn database() -> mongodb::Database {
    let uri = std::env::var("LAMINAR_SCHEMA_TEST_MONGO").expect("set LAMINAR_SCHEMA_TEST_MONGO");
    mongodb::Client::with_uri_str(uri)
        .await
        .unwrap()
        .database("laminar_schema_contracts")
}

fn registry() -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    laminar_connectors::mongodb::register_mongodb_cdc_source(&registry).unwrap();
    laminar_connectors::mongodb::register_mongodb_sink(&registry).unwrap();
    registry
}

fn config(kind: &str, collection: &str) -> ConnectorConfig {
    let mut config = ConnectorConfig::new(kind);
    config.set(
        "connection.uri",
        std::env::var("LAMINAR_SCHEMA_TEST_MONGO").unwrap(),
    );
    config.set("database", "laminar_schema_contracts");
    config.set("collection", collection);
    config
}

fn input() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_MONGO"]
async fn empty_closed_validator_resolves_lookup_and_collection_replacement_is_fenced() {
    let database = database().await;
    let name = "schema_lookup_uuid";
    let _ = database
        .collection::<mongodb::bson::Document>(name)
        .drop()
        .await;
    let validator = doc! {"$jsonSchema":{"bsonType":"object","additionalProperties":false,"required":["_id"],"properties":{"_id":{"bsonType":"long"},"label":{"bsonType":["string","null"]}}}};
    database
        .create_collection(name)
        .validator(validator.clone())
        .await
        .unwrap();
    let registry = registry();
    let mut config = config("mongodb", name);
    config.set("_primary_key_columns", "_id");
    let binding = registry.resolve_lookup_schema(&config, None).await.unwrap();
    assert_eq!(binding.origin, SchemaOrigin::Metadata);
    assert_eq!(
        binding.logical.field(0),
        &Field::new("_id", DataType::Int64, false)
    );
    assert!(binding.logical.field(1).is_nullable());
    config.set_schema_binding(binding.clone()).unwrap();
    let lookup = registry
        .create_lookup_source(config.clone(), None)
        .await
        .unwrap()
        .unwrap();
    database
        .collection(name)
        .insert_one(doc! {"_id":7_i64,"label":"seven"})
        .await
        .unwrap();
    let rows = RowConverter::new(vec![SortField::new(DataType::Int64)])
        .unwrap()
        .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
        .unwrap();
    let key = rows.row(0);
    let result = lookup.query_batch(&[key.as_ref()], &[], &[]).await.unwrap();
    let batch = result[0].as_ref().unwrap();
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "seven"
    );
    database
        .collection::<mongodb::bson::Document>(name)
        .drop()
        .await
        .unwrap();
    database
        .create_collection(name)
        .validator(validator)
        .await
        .unwrap();
    let changed = registry.resolve_lookup_schema(&config, None).await.unwrap();
    assert_ne!(binding.value, changed.value);
    assert!(lookup
        .query_batch(&[key.as_ref()], &[], &[])
        .await
        .unwrap_err()
        .to_string()
        .contains("identity"));
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_MONGO"]
async fn sink_maps_reordered_query_and_rejects_validator_drift_before_flush() {
    let database = database().await;
    let name = "schema_writer_validator";
    let _ = database
        .collection::<mongodb::bson::Document>(name)
        .drop()
        .await;
    database.create_collection(name).validator(doc! {"$jsonSchema":{"bsonType":"object","required":["id","label"],"properties":{"id":{"bsonType":"long"},"label":{"bsonType":"string"}}}}).await.unwrap();
    let registry = registry();
    let mut config = config("mongodb-sink", name);
    config.set("_arrow_schema", encode_arrow_schema_ipc(&input()));
    let binding = registry
        .resolve_sink_schema(&config, input())
        .await
        .unwrap();
    config.set_schema_binding(binding).unwrap();
    let mut sink = registry.create_sink(&config, None).unwrap();
    sink.open(&config).await.unwrap();
    let batch = RecordBatch::try_new(
        input(),
        vec![
            Arc::new(StringArray::from(vec!["mapped"])),
            Arc::new(Int64Array::from(vec![42])),
        ],
    )
    .unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();
    let document = database
        .collection::<mongodb::bson::Document>(name)
        .find_one(doc! {"id":42_i64})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(document.get_str("label").unwrap(), "mapped");
    sink.write_batch(&batch).await.unwrap();
    database.run_command(doc! {"collMod": name, "validator":{"$jsonSchema":{"properties":{"id":{"bsonType":"string"}}}}}).await.unwrap();
    assert!(sink
        .flush()
        .await
        .unwrap_err()
        .to_string()
        .contains("schema"));
    assert_eq!(
        database
            .collection::<mongodb::bson::Document>(name)
            .count_documents(doc! {})
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_MONGO"]
async fn creation_permission_is_separate_and_restart_never_recreates_a_bound_collection() {
    let database = database().await;
    let name = "schema_explicit_creation";
    let _ = database
        .collection::<mongodb::bson::Document>(name)
        .drop()
        .await;
    let registry = registry();
    let mut config = config("mongodb-sink", name);
    config.set("_arrow_schema", encode_arrow_schema_ipc(&input()));
    assert!(registry
        .resolve_sink_schema(&config, input())
        .await
        .unwrap_err()
        .to_string()
        .contains("auto.create"));
    config.set("auto.create", "true");
    let mut binding = registry
        .resolve_sink_schema(&config, input())
        .await
        .unwrap();
    assert!(binding.value.is_none());
    assert!(!database
        .list_collection_names()
        .await
        .unwrap()
        .iter()
        .any(|collection| collection == name));
    let mut sink = registry.create_sink(&config, None).unwrap();
    sink.prepare_schema(&config, &mut binding).await.unwrap();
    assert!(binding.value.is_some());
    config.set_schema_binding(binding).unwrap();
    sink.open(&config).await.unwrap();
    sink.close().await.unwrap();
    database
        .collection::<mongodb::bson::Document>(name)
        .drop()
        .await
        .unwrap();
    let mut recovered = registry.create_sink(&config, None).unwrap();
    assert!(recovered.open(&config).await.is_err());
    assert!(!database
        .list_collection_names()
        .await
        .unwrap()
        .iter()
        .any(|collection| collection == name));
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_MONGO"]
async fn cdc_factory_resolves_empty_collection_identity_without_opening_a_change_stream() {
    let database = database().await;
    let name = "schema_cdc_empty";
    let _ = database
        .collection::<mongodb::bson::Document>(name)
        .drop()
        .await;
    database.create_collection(name).await.unwrap();
    let registry = registry();
    let config = config("mongodb-cdc", name);
    let binding = registry.resolve_source_schema(&config, None).await.unwrap();
    assert_eq!(binding.origin, SchemaOrigin::Metadata);
    assert_eq!(
        binding.value.as_ref().unwrap().format,
        "mongodb_change_stream"
    );
    assert!(binding.logical.index_of("document_key").is_ok());
    assert!(binding.logical.index_of("_op").is_err());
    assert_eq!(
        database
            .collection::<mongodb::bson::Document>(name)
            .count_documents(doc! {})
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_MONGO"]
async fn document_projection_binds_declared_keyed_columns_and_rejects_unsafe_ones() {
    let database = database().await;
    let name = "schema_cdc_document";
    let _ = database
        .collection::<mongodb::bson::Document>(name)
        .drop()
        .await;
    database.create_collection(name).await.unwrap();
    let registry = registry();
    let mut config = config("mongodb-cdc", name);
    config.set("output.mode", "document");
    config.set("full.document.mode", "required");
    config.set("objectid.columns", "_id");
    config.set("_primary_key_columns", "_id");
    let declared = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(18, 2), true),
    ]));
    let binding = registry
        .resolve_source_schema(&config, Some(declared.clone()))
        .await
        .unwrap();
    assert_eq!(binding.logical, *declared);
    assert_eq!(
        binding.value.as_ref().unwrap().format,
        "mongodb_change_stream"
    );

    let strict = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(18, 2), false),
    ]));
    let error = registry
        .resolve_source_schema(&config, Some(strict))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must be nullable"), "{error}");

    config.set("_primary_key_columns", "amount");
    let error = registry
        .resolve_source_schema(&config, Some(declared))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("_id"), "{error}");
}
