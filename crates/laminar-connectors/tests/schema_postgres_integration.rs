//! Native PostgreSQL schema contracts against an explicitly supplied test database.
//! Run with LAMINAR_SCHEMA_TEST_PG and --ignored; the database is never auto-created.

#![cfg(all(feature = "postgres-sink", feature = "postgres-cdc"))]

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SinkConnector;
use laminar_connectors::postgres::{
    PostgresReferenceTableSource, PostgresSink, PostgresSinkConfig, SslMode,
};
use laminar_connectors::reference::ReferenceTableSource;
use laminar_connectors::schema::resolution::SchemaOrigin;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

async fn connection() -> Client {
    let uri = std::env::var("LAMINAR_SCHEMA_TEST_PG").expect("set LAMINAR_SCHEMA_TEST_PG");
    let (client, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
}

fn reference_config(table: &str) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("postgres");
    config.set(
        "connection",
        std::env::var("LAMINAR_SCHEMA_TEST_PG").unwrap(),
    );
    config.set("table", table);
    config.set("ssl.mode", "disable");
    config
}

fn sink(table: &str) -> PostgresSink {
    let pg: tokio_postgres::Config = std::env::var("LAMINAR_SCHEMA_TEST_PG")
        .unwrap()
        .parse()
        .unwrap();
    let mut config = PostgresSinkConfig::new("127.0.0.1", pg.get_dbname().unwrap(), table);
    config.port = pg.get_ports()[0];
    config.username = pg.get_user().unwrap().into();
    config.password = String::from_utf8(pg.get_password().unwrap().to_vec()).unwrap();
    config.ssl_mode = SslMode::Disable;
    PostgresSink::new(input_schema(), config, None)
}

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_PG with wal_level=logical"]
async fn registered_publication_resolution_preserves_selected_columns_and_slot_cursor() {
    use laminar_connectors::registry::ConnectorRegistry;
    let client = connection().await;
    client.batch_execute("DROP PUBLICATION IF EXISTS schema_contract_publication; DROP TABLE IF EXISTS schema_contract_cdc; CREATE TABLE schema_contract_cdc (id bigint PRIMARY KEY, label text, unpublished integer); CREATE PUBLICATION schema_contract_publication FOR TABLE schema_contract_cdc (id, label) WITH (publish='insert,update,delete')").await.unwrap();
    client.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'schema_contract_slot'", &[]).await.unwrap();
    client
        .query(
            "SELECT pg_create_logical_replication_slot('schema_contract_slot','pgoutput')",
            &[],
        )
        .await
        .unwrap();
    let before: String = client.query_one("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='schema_contract_slot'", &[]).await.unwrap().get(0);
    let pg: tokio_postgres::Config = std::env::var("LAMINAR_SCHEMA_TEST_PG")
        .unwrap()
        .parse()
        .unwrap();
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", "127.0.0.1");
    config.set("port", pg.get_ports()[0].to_string());
    config.set("database", pg.get_dbname().unwrap());
    config.set("username", pg.get_user().unwrap());
    config.set(
        "password",
        String::from_utf8(pg.get_password().unwrap().to_vec()).unwrap(),
    );
    config.set("ssl.mode", "disable");
    config.set("publication", "schema_contract_publication");
    config.set("slot.name", "schema_contract_slot");
    let registry = ConnectorRegistry::new();
    laminar_connectors::postgres::register_postgres_cdc_source(&registry).unwrap();
    let binding = registry.resolve_source_schema(&config, None).await.unwrap();
    assert_eq!(binding.origin, SchemaOrigin::Metadata);
    let relations = &binding.value.as_ref().unwrap().definition["relations"];
    assert_eq!(relations.as_array().unwrap().len(), 1);
    let columns = relations[0]["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0]["name"], "id");
    assert_eq!(columns[1]["name"], "label");
    let after = client.query_one("SELECT confirmed_flush_lsn::text, active FROM pg_replication_slots WHERE slot_name='schema_contract_slot'", &[]).await.unwrap();
    assert_eq!(after.get::<_, String>(0), before);
    assert!(!after.get::<_, bool>(1));
    let mut reference = reference_config("schema_contract_cdc");
    let table = registry
        .resolve_table_schema(&reference, None)
        .await
        .unwrap();
    assert_eq!(table.logical.fields().len(), 3);
    reference.set("_primary_key_columns", "id");
    let lookup = registry
        .resolve_lookup_schema(&reference, None)
        .await
        .unwrap();
    assert_eq!(lookup, table);
    client
        .query(
            "SELECT pg_drop_replication_slot('schema_contract_slot')",
            &[],
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "DROP PUBLICATION schema_contract_publication; DROP TABLE schema_contract_cdc",
        )
        .await
        .unwrap();
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(vec!["one", "two"])),
            Arc::new(Int64Array::from(vec![11, 22])),
        ],
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_PG"]
async fn empty_reference_uses_metadata_and_pins_native_identity() {
    let client = connection().await;
    client.batch_execute("DROP TABLE IF EXISTS schema_reference_empty; CREATE TABLE schema_reference_empty (id bigint NOT NULL PRIMARY KEY, label text)").await.unwrap();
    let config = reference_config("schema_reference_empty");
    let mut discovery =
        PostgresReferenceTableSource::new(config.clone(), Arc::new(Schema::empty()));
    let binding = discovery.resolve_schema(&config, None).await.unwrap();
    assert_eq!(binding.origin, SchemaOrigin::Metadata);
    assert_eq!(binding.logical.field(0).name(), "id");
    assert!(!binding.logical.field(0).is_nullable());
    let bytes = String::from_utf8(binding.canonical_bytes().unwrap()).unwrap();
    let pg: tokio_postgres::Config = std::env::var("LAMINAR_SCHEMA_TEST_PG")
        .unwrap()
        .parse()
        .unwrap();
    assert!(!bytes.contains(std::str::from_utf8(pg.get_password().unwrap()).unwrap()));
    let mut committed = config;
    committed.set_schema_binding(binding.clone()).unwrap();
    let mut reader = PostgresReferenceTableSource::new(committed, Arc::new(binding.logical));
    assert!(reader.poll_snapshot().await.unwrap().is_none());
    reader.close().await.unwrap();
    client
        .batch_execute("DROP TABLE schema_reference_empty")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_PG"]
async fn named_copy_applies_database_defaults_and_generated_columns() {
    let client = connection().await;
    client.batch_execute("DROP TABLE IF EXISTS schema_writer_defaults; CREATE TABLE schema_writer_defaults (id bigint NOT NULL, label text NOT NULL, supplied text NOT NULL DEFAULT 'database-default', generated bigint GENERATED ALWAYS AS (id * 2) STORED)").await.unwrap();
    let mut writer = sink("schema_writer_defaults");
    writer
        .open(&ConnectorConfig::new("postgres-sink"))
        .await
        .unwrap();
    writer.write_batch(&batch()).await.unwrap();
    writer.flush().await.unwrap();
    let rows = client
        .query(
            "SELECT id,label,supplied,generated FROM schema_writer_defaults ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    for (row, (id, label)) in rows.iter().zip([(11_i64, "one"), (22_i64, "two")]) {
        assert_eq!(row.get::<_, i64>(0), id);
        assert_eq!(row.get::<_, String>(1), label);
        assert_eq!(row.get::<_, String>(2), "database-default");
        assert_eq!(row.get::<_, i64>(3), id * 2);
    }
    writer.close().await.unwrap();
    client
        .batch_execute("DROP TABLE schema_writer_defaults")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_PG"]
async fn replacement_and_drift_fail_before_copy_or_acknowledgement() {
    let client = connection().await;
    client.batch_execute("DROP TABLE IF EXISTS schema_writer_replaced; CREATE TABLE schema_writer_replaced (id bigint NOT NULL, label text NOT NULL)").await.unwrap();
    let mut writer = sink("schema_writer_replaced");
    writer
        .open(&ConnectorConfig::new("postgres-sink"))
        .await
        .unwrap();
    writer.write_batch(&batch()).await.unwrap();
    client.batch_execute("DROP TABLE schema_writer_replaced; CREATE TABLE schema_writer_replaced (id bigint NOT NULL, label text NOT NULL)").await.unwrap();
    let error = writer.flush().await.unwrap_err();
    assert!(
        error.to_string().contains("identity") || error.to_string().contains("layout"),
        "{error}"
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM schema_writer_replaced", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert!(
        writer.close().await.is_err(),
        "failed buffered work cannot be silently acknowledged"
    );
    client
        .batch_execute("DROP TABLE schema_writer_replaced")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_PG"]
async fn explicit_snapshot_projection_preserves_names_and_values() {
    let client = connection().await;
    client.batch_execute("DROP TABLE IF EXISTS schema_reference_projection; CREATE TABLE schema_reference_projection (id bigint NOT NULL, label text NOT NULL, ignored boolean); INSERT INTO schema_reference_projection VALUES (42,'projected',true)").await.unwrap();
    let config = reference_config("schema_reference_projection");
    let mut discovery = PostgresReferenceTableSource::new(config.clone(), input_schema());
    let binding = discovery
        .resolve_schema(&config, Some(input_schema()))
        .await
        .unwrap();
    assert_eq!(binding.logical.field(0).name(), "label");
    assert_eq!(binding.logical.fields().len(), 2);
    let mut config = config;
    config.set_schema_binding(binding).unwrap();
    let mut reader = PostgresReferenceTableSource::new(config, input_schema());
    let rows = reader.poll_snapshot().await.unwrap().unwrap();
    assert_eq!(rows.num_rows(), 1);
    assert_eq!(
        rows.column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "projected"
    );
    assert_eq!(
        rows.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(rows.column(0).null_count(), 0);
    assert!(reader.poll_snapshot().await.unwrap().is_none());
    reader.close().await.unwrap();
    client
        .batch_execute("DROP TABLE schema_reference_projection")
        .await
        .unwrap();
}
