//! PostgreSQL CDC integration coverage against a real logical-replication server.
//!
//! Run with:
//! `cargo test -p laminar-connectors --no-default-features --features postgres-cdc --test postgres_cdc_integration`

#![cfg(feature = "postgres-cdc")]
#![cfg(not(target_os = "windows"))]

use std::collections::HashMap;
use std::time::Duration;

use arrow_array::{Array, StringArray, UInt64Array};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceBatch, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::postgres::{
    Lsn, PostgresCdcConfig, PostgresCdcSource, PostgresLookupSource, PostgresLookupSourceConfig,
};
use laminar_core::state::CheckpointAttempt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::time::{sleep, timeout, Instant};
use tokio_postgres::{Client, NoTls};

const SLOT: &str = "laminar_cdc_integration_slot";
const PUBLICATION: &str = "laminar_cdc_integration_pub";
const POSTGRES_MAJOR_ENV: &str = "LAMINAR_TEST_POSTGRES_MAJOR";
const DEFAULT_POSTGRES_MAJOR: &str = "18";

fn parse_postgres_major(value: Option<&str>) -> Result<&'static str, String> {
    match value {
        None => Ok(DEFAULT_POSTGRES_MAJOR),
        Some("17") => Ok("17"),
        Some("18") => Ok("18"),
        Some(value) => Err(format!(
            "{POSTGRES_MAJOR_ENV} must be exactly '17' or '18', got {value:?}"
        )),
    }
}

fn postgres_major() -> &'static str {
    match std::env::var(POSTGRES_MAJOR_ENV) {
        Ok(value) => parse_postgres_major(Some(&value)).unwrap_or_else(|error| panic!("{error}")),
        Err(std::env::VarError::NotPresent) => DEFAULT_POSTGRES_MAJOR,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{POSTGRES_MAJOR_ENV} must be valid UTF-8 and exactly '17' or '18'")
        }
    }
}

async fn start_postgres() -> (testcontainers::ContainerAsync<GenericImage>, String, u16) {
    let major = postgres_major();
    let container = GenericImage::new("postgres", major)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_exposed_port(5432.into())
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=4",
            "-c",
            "max_wal_senders=4",
        ])
        .start()
        .await
        .unwrap_or_else(|error| panic!("start PostgreSQL {major} container: {error}"));

    let host = container
        .get_host()
        .await
        .unwrap_or_else(|error| panic!("resolve PostgreSQL {major} container host: {error}"))
        .to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .unwrap_or_else(|error| panic!("resolve PostgreSQL {major} container port: {error}"));
    (container, host, port)
}

async fn connect(host: &str, port: u16) -> Client {
    let connection_string = format!(
        "host={host} port={port} user=postgres password=postgres dbname=postgres sslmode=disable"
    );
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls)
        .await
        .expect("connect to PostgreSQL");
    tokio::spawn(async move {
        connection.await.expect("PostgreSQL admin connection");
    });
    client
}

fn source_config(host: &str, port: u16) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", host);
    config.set("port", port.to_string());
    config.set("database", "postgres");
    config.set("username", "postgres");
    config.set("password", "postgres");
    config.set("slot.name", SLOT);
    config.set("publication", PUBLICATION);
    config.set("ssl.mode", "disable");
    config.set("max.buffered.bytes", "1048576");
    config
}

fn lookup_config(host: &str, port: u16, table: &str) -> PostgresLookupSourceConfig {
    PostgresLookupSourceConfig {
        table: table.into(),
        primary_key_columns: vec!["id".into()],
        properties: HashMap::from([
            ("host".into(), host.into()),
            ("port".into(), port.to_string()),
            ("database".into(), "postgres".into()),
            ("user".into(), "postgres".into()),
            ("password".into(), "postgres".into()),
            ("ssl.mode".into(), "disable".into()),
        ]),
        pool_size: 2,
    }
}

fn initial_start(config: &ConnectorConfig) -> SourceStart {
    SourceStart::new(
        config.clone(),
        SourcePosition::Initial,
        DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap()
}

fn resume_start(config: &ConnectorConfig, checkpoint: SourceCheckpoint) -> SourceStart {
    SourceStart::new(
        config.clone(),
        SourcePosition::Resume {
            attempt: CheckpointAttempt::new(1, 1),
            checkpoint,
        },
        DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap()
}

fn checkpoint_lsn(checkpoint: &SourceCheckpoint) -> Lsn {
    checkpoint
        .get_offset("lsn")
        .expect("PostgreSQL checkpoint LSN")
        .parse()
        .expect("valid PostgreSQL checkpoint LSN")
}

fn digest_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn source_config_digest(config: &PostgresCdcConfig) -> String {
    let mut digest = Sha256::new();
    digest.update(b"laminardb-postgres-cdc-source-v1\0");
    digest_field(&mut digest, b"pgoutput");
    digest_field(&mut digest, b"proto_version=1");
    digest_field(&mut digest, b"messages=false");
    for tables in [&config.table_include, &config.table_exclude] {
        let mut canonical: Vec<&str> = tables.iter().map(String::as_str).collect();
        canonical.sort_unstable();
        canonical.dedup();
        digest.update(
            u64::try_from(canonical.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for table in canonical {
            digest_field(&mut digest, table.as_bytes());
        }
    }
    format!("{:x}", digest.finalize())
}

async fn source_checkpoint(
    client: &Client,
    config: &ConnectorConfig,
    lsn: Lsn,
) -> SourceCheckpoint {
    let parsed = PostgresCdcConfig::from_config(config).expect("valid PostgreSQL CDC config");
    let cluster_row = client
        .query_one(
            "SELECT control_system.system_identifier::text, control_checkpoint.timeline_id::text, db.oid::text \
             FROM pg_catalog.pg_control_system() AS control_system \
             CROSS JOIN pg_catalog.pg_control_checkpoint() AS control_checkpoint \
             CROSS JOIN pg_catalog.pg_database AS db \
             WHERE db.datname = current_database()",
            &[],
        )
        .await
        .expect("query PostgreSQL cluster and database identity");
    let publication_row = client
        .query_opt(
            "SELECT p.oid::text, \
                    jsonb_build_object( \
                        'properties', to_jsonb(p) - ARRAY['oid', 'pubname', 'pubowner']::text[], \
                        'tables', COALESCE( \
                            (SELECT jsonb_agg( \
                                 jsonb_build_array( \
                                     c.oid::text, pt.schemaname, pt.tablename, \
                                     pt.attnames, pt.rowfilter \
                                 ) \
                                 ORDER BY pt.schemaname, pt.tablename, c.oid \
                             ) \
                             FROM pg_catalog.pg_publication_tables AS pt \
                             LEFT JOIN pg_catalog.pg_namespace AS n \
                                    ON n.nspname = pt.schemaname \
                             LEFT JOIN pg_catalog.pg_class AS c \
                                    ON c.relnamespace = n.oid AND c.relname = pt.tablename \
                             WHERE pt.pubname = p.pubname), \
                            '[]'::jsonb \
                        ) \
                    )::text \
             FROM pg_catalog.pg_publication AS p \
             WHERE p.pubname = $1",
            &[&parsed.publication],
        )
        .await
        .expect("query PostgreSQL publication identity");
    let slot_row = client
        .query_opt(
            "SELECT plugin, two_phase, failover \
             FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&parsed.slot_name],
        )
        .await
        .expect("query PostgreSQL slot properties");

    let (publication_oid, publication_definition) = publication_row.map_or_else(
        || ("0".to_string(), "missing-publication".to_string()),
        |row| (row.get::<_, String>(0), row.get::<_, String>(1)),
    );
    let mut publication_digest = Sha256::new();
    publication_digest.update(b"laminardb-postgres-publication-v1\0");
    digest_field(&mut publication_digest, publication_definition.as_bytes());
    let (slot_plugin, slot_two_phase, slot_failover) = slot_row.map_or_else(
        || ("pgoutput".to_string(), false, true),
        |row| (row.get(0), row.get(1), row.get(2)),
    );

    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_offset("lsn", lsn.to_string());
    checkpoint.set_metadata("connector", "postgres-cdc");
    checkpoint.set_metadata("checkpoint_version", "3");
    checkpoint.set_metadata("slot_name", &parsed.slot_name);
    checkpoint.set_metadata("publication", &parsed.publication);
    checkpoint.set_metadata("database", &parsed.database);
    checkpoint.set_metadata("system_identifier", cluster_row.get::<_, String>(0));
    checkpoint.set_metadata("timeline_id", cluster_row.get::<_, String>(1));
    checkpoint.set_metadata("database_oid", cluster_row.get::<_, String>(2));
    checkpoint.set_metadata("publication_oid", publication_oid);
    checkpoint.set_metadata(
        "publication_definition_sha256",
        format!("{:x}", publication_digest.finalize()),
    );
    checkpoint.set_metadata("source_config_sha256", source_config_digest(&parsed));
    checkpoint.set_metadata("slot_plugin", slot_plugin);
    checkpoint.set_metadata("slot_two_phase", slot_two_phase.to_string());
    checkpoint.set_metadata("slot_failover", slot_failover.to_string());
    checkpoint
}

async fn create_slot(client: &Client) -> Lsn {
    let row = client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)",
            &[&SLOT],
        )
        .await
        .expect("create logical replication slot");
    row.get::<_, String>(0)
        .parse()
        .expect("valid created-slot LSN")
}

async fn slot_exists(client: &Client) -> bool {
    client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&SLOT],
        )
        .await
        .expect("query replication slot existence")
        .get(0)
}

async fn slot_lsn(client: &Client) -> Lsn {
    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .await
        .expect("query replication slot");
    row.get::<_, Option<String>>(0)
        .expect("replication slot has a confirmed LSN")
        .parse()
        .expect("valid replication-slot LSN")
}

#[derive(Debug, PartialEq, Eq)]
struct SlotState {
    restart_lsn: Option<String>,
    confirmed_flush_lsn: Option<String>,
    active: bool,
    active_pid: Option<i32>,
}

async fn slot_state(client: &Client) -> SlotState {
    let row = client
        .query_one(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text, active, active_pid \
             FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .await
        .expect("query replication slot state");
    SlotState {
        restart_lsn: row.get(0),
        confirmed_flush_lsn: row.get(1),
        active: row.get(2),
        active_pid: row.get(3),
    }
}

async fn assert_source_remains_created(source: &mut PostgresCdcSource) {
    let error = source
        .poll_batch(1)
        .await
        .expect_err("failed startup must not publish a running source");
    assert!(error.to_string().contains("Created"), "{error}");
}

async fn wait_for_slot_lsn(client: &Client, expected: Lsn) {
    timeout(Duration::from_secs(30), async {
        loop {
            if slot_lsn(client).await >= expected {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("replication slot did not receive durable checkpoint feedback");
}

async fn next_batch(source: &mut PostgresCdcSource) -> SourceBatch {
    timeout(Duration::from_secs(30), async {
        loop {
            match source.poll_batch(1).await {
                Ok(Some(batch)) => return batch,
                Ok(None) => sleep(Duration::from_millis(10)).await,
                Err(error) => panic!("PostgreSQL CDC poll failed: {error}"),
            }
        }
    })
    .await
    .expect("timed out waiting for PostgreSQL CDC data")
}

fn json_field(batch: &SourceBatch, row: usize, column: usize) -> Option<Value> {
    let values = batch
        .records
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("CDC JSON StringArray");
    (!values.is_null(row)).then(|| serde_json::from_str(values.value(row)).expect("valid CDC JSON"))
}

fn event(batch: &SourceBatch, row: usize) -> (&str, Value, Option<Value>, Lsn) {
    assert!(row < batch.num_rows());
    let records = &batch.records;
    let operation = records
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("_op StringArray")
        .value(row);
    let event_lsn = Lsn::new(
        records
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("_lsn UInt64Array")
            .value(row),
    );
    let before = json_field(batch, row, 4);
    let after = json_field(batch, row, 5).expect("event has an _after image");
    (operation, after, before, event_lsn)
}

#[test]
fn postgres_major_override_is_strict() {
    assert_eq!(parse_postgres_major(None).unwrap(), "18");
    assert_eq!(parse_postgres_major(Some("17")).unwrap(), "17");
    assert_eq!(parse_postgres_major(Some("18")).unwrap(), "18");

    for value in ["", "16", "19", "latest", "17-alpine", " 17", "18 "] {
        let error = parse_postgres_major(Some(value)).unwrap_err();
        assert!(error.contains(POSTGRES_MAJOR_ENV), "{error}");
        assert!(error.contains(&format!("{value:?}")), "{error}");
    }
}

#[tokio::test]
async fn commit_boundary_feedback_and_checkpoint_resume() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY, name TEXT NOT NULL); \
             ALTER TABLE cdc_events REPLICA IDENTITY FULL; \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete');"
        ))
        .await
        .expect("create CDC table and publication");

    let config = source_config(&host, port);
    let created_lsn = create_slot(&admin).await;
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let checkpoint = source_checkpoint(&admin, &config, created_lsn).await;
    source
        .start(resume_start(&config, checkpoint))
        .await
        .expect("resume PostgreSQL CDC source from an existing durable slot");

    let initial_checkpoint = source.checkpoint();
    let initial_checkpoint_lsn = checkpoint_lsn(&initial_checkpoint);
    let initial_slot_lsn = slot_lsn(&admin).await;

    admin
        .batch_execute(
            "BEGIN; \
             INSERT INTO cdc_events (id, name) VALUES (1, 'before'); \
             UPDATE cdc_events SET name = 'after' WHERE id = 1; \
             COMMIT;",
        )
        .await
        .expect("commit insert/update transaction");

    let transaction = next_batch(&mut source).await;
    assert_eq!(
        transaction.num_rows(),
        2,
        "the engine batch target must not split a committed PostgreSQL transaction"
    );
    let (operation, after, before, insert_lsn) = event(&transaction, 0);
    assert_eq!(operation, "I");
    assert_eq!(after["id"], "1");
    assert_eq!(after["name"], "before");
    assert!(before.is_none());
    let (operation, after, before, update_lsn) = event(&transaction, 1);
    assert_eq!(operation, "U");
    assert_eq!(after["id"], "1");
    assert_eq!(after["name"], "after");
    let before = before.expect("REPLICA IDENTITY FULL update before-image");
    assert_eq!(before["id"], "1");
    assert_eq!(before["name"], "before");
    assert_eq!(update_lsn, insert_lsn);

    let committed_checkpoint = source.checkpoint();
    let committed_lsn = checkpoint_lsn(&committed_checkpoint);
    assert!(committed_lsn > initial_checkpoint_lsn);
    assert!(committed_lsn >= update_lsn);

    sleep(Duration::from_millis(500)).await;
    assert_eq!(
        slot_lsn(&admin).await,
        initial_slot_lsn,
        "polling must not acknowledge WAL before the engine durably commits its checkpoint"
    );
    assert_eq!(source.confirmed_flush_lsn(), initial_checkpoint_lsn);

    let mut ahead_checkpoint = committed_checkpoint.clone();
    ahead_checkpoint.set_offset(
        "lsn",
        Lsn::new(committed_lsn.as_u64().saturating_add(1)).to_string(),
    );
    let error = source
        .notify_epoch_committed(1, &ahead_checkpoint)
        .await
        .expect_err("feedback ahead of emitted data must fail closed");
    assert!(error.to_string().contains("ahead of"), "{error}");
    sleep(Duration::from_millis(200)).await;
    assert_eq!(source.confirmed_flush_lsn(), initial_checkpoint_lsn);
    assert_eq!(
        slot_lsn(&admin).await,
        initial_slot_lsn,
        "rejected feedback must not advance PostgreSQL"
    );

    source
        .notify_epoch_committed(1, &committed_checkpoint)
        .await
        .expect("publish durable checkpoint feedback");
    assert_eq!(source.confirmed_flush_lsn(), committed_lsn);
    wait_for_slot_lsn(&admin, committed_lsn).await;
    source.close().await.expect("close first CDC source");

    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    resumed
        .start(resume_start(&config, committed_checkpoint.clone()))
        .await
        .expect("resume PostgreSQL CDC source from durable checkpoint");
    assert_eq!(checkpoint_lsn(&resumed.checkpoint()), committed_lsn);

    admin
        .execute(
            "INSERT INTO cdc_events (id, name) VALUES ($1, $2)",
            &[&2_i64, &"resumed"],
        )
        .await
        .expect("insert after CDC restart");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(Instant::now() < deadline, "timed out after CDC restart");
        let batch = next_batch(&mut resumed).await;
        let (operation, after, _, _) = event(&batch, 0);
        if after["id"] == "2" {
            assert_eq!(operation, "I");
            assert_eq!(after["name"], "resumed");
            break;
        }
        assert_eq!(
            after["id"], "1",
            "only at-least-once replay from the checkpoint boundary is permitted"
        );
    }

    assert!(checkpoint_lsn(&resumed.checkpoint()) > committed_lsn);
    resumed.close().await.expect("close resumed CDC source");
}

#[tokio::test]
async fn default_replica_identity_omits_unavailable_old_non_key_fields() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY, name TEXT NOT NULL); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete'); \
             INSERT INTO cdc_events (id, name) VALUES (1, 'retained');"
        ))
        .await
        .expect("create default-identity CDC table");

    let config = source_config(&host, port);
    let created_lsn = create_slot(&admin).await;
    let checkpoint = source_checkpoint(&admin, &config, created_lsn).await;
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    source
        .start(resume_start(&config, checkpoint))
        .await
        .expect("start PostgreSQL CDC source");

    admin
        .batch_execute(
            "BEGIN; \
             UPDATE cdc_events SET id = 2 WHERE id = 1; \
             DELETE FROM cdc_events WHERE id = 2; \
             COMMIT;",
        )
        .await
        .expect("commit key update and delete");

    let transaction = next_batch(&mut source).await;
    assert_eq!(transaction.num_rows(), 2);
    let operations = transaction
        .records
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("_op StringArray");
    assert_eq!(operations.value(0), "U");
    assert_eq!(operations.value(1), "D");

    let update_before = json_field(&transaction, 0, 4).expect("update _before");
    assert_eq!(update_before["id"], "1");
    assert_eq!(update_before.as_object().expect("object").len(), 1);
    let update_after = json_field(&transaction, 0, 5).expect("update _after");
    assert_eq!(update_after["id"], "2");
    assert_eq!(update_after["name"], "retained");

    let delete_before = json_field(&transaction, 1, 4).expect("delete _before");
    assert_eq!(delete_before["id"], "2");
    assert_eq!(delete_before.as_object().expect("object").len(), 1);
    assert!(json_field(&transaction, 1, 5).is_none());

    source.close().await.expect("close PostgreSQL CDC source");
}

#[tokio::test]
async fn initial_start_is_rejected_without_creating_a_slot() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY, name TEXT NOT NULL); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete');"
        ))
        .await
        .expect("create CDC table and publication");

    let config = source_config(&host, port);
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = source
        .start(initial_start(&config))
        .await
        .expect_err("initial start must wait for snapshot/WAL bootstrap");
    assert!(error.to_string().contains("[LDB-5060]"), "{error}");
    assert!(
        !slot_exists(&admin).await,
        "Initial must not mutate PostgreSQL"
    );
}

#[tokio::test]
async fn resume_missing_slot_fails_without_creating_it() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY, name TEXT NOT NULL); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete');"
        ))
        .await
        .expect("create CDC table and publication");

    let config = source_config(&host, port);
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let checkpoint = source_checkpoint(&admin, &config, Lsn::new(1)).await;
    let error = source
        .start(resume_start(&config, checkpoint))
        .await
        .expect_err("Resume must require the exact existing slot");
    assert!(
        error.to_string().contains("exact durable slot is missing"),
        "{error}"
    );
    assert_source_remains_created(&mut source).await;
    assert!(!slot_exists(&admin).await, "Resume must not create a slot");
}

#[tokio::test]
async fn resume_rejects_a_missing_publication_before_replication() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    let created_lsn = create_slot(&admin).await;
    let mut config = source_config(&host, port);
    config.set("publication", "missing_publication");
    let checkpoint = source_checkpoint(&admin, &config, created_lsn).await;
    let before = slot_state(&admin).await;

    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = source
        .start(resume_start(&config, checkpoint))
        .await
        .expect_err("identity inspection must reject an unknown publication");
    assert!(error.to_string().contains("does not exist"), "{error}");
    assert_source_remains_created(&mut source).await;
    assert_eq!(slot_state(&admin).await, before);
}

#[tokio::test]
async fn resume_rejects_a_truncate_publication_without_mutating_the_slot() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events;"
        ))
        .await
        .expect("create default truncate-enabled publication");
    let config = source_config(&host, port);
    let created_lsn = create_slot(&admin).await;
    let checkpoint = source_checkpoint(&admin, &config, created_lsn).await;
    let before = slot_state(&admin).await;

    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = source
        .start(resume_start(&config, checkpoint))
        .await
        .expect_err("TRUNCATE must be rejected until it has a representable CDC operation");
    assert!(error.to_string().contains("publishes TRUNCATE"), "{error}");
    assert_source_remains_created(&mut source).await;
    assert_eq!(slot_state(&admin).await, before);
}

#[tokio::test]
async fn exact_replication_socket_rejects_a_checkpoint_ahead_of_server_wal() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events \
             WITH (publish = 'insert, update, delete');"
        ))
        .await
        .expect("create CDC fixture");
    let config = source_config(&host, port);
    create_slot(&admin).await;
    let checkpoint = source_checkpoint(&admin, &config, Lsn::MAX).await;
    let before = slot_state(&admin).await;

    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = source
        .start(resume_start(&config, checkpoint))
        .await
        .expect_err("a future recovery LSN must fail before START_REPLICATION");
    assert!(
        error
            .to_string()
            .contains("ahead of the exact server WAL flush"),
        "{error}"
    );
    assert_source_remains_created(&mut source).await;
    assert_eq!(slot_state(&admin).await, before);
}

#[tokio::test]
async fn source_start_rejects_bad_authentication_before_running() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    let created_lsn = create_slot(&admin).await;
    let mut config = source_config(&host, port);
    config.set("password", "incorrect-password");

    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let checkpoint = source_checkpoint(&admin, &config, created_lsn).await;
    let before = slot_state(&admin).await;
    let error = source
        .start(resume_start(&config, checkpoint))
        .await
        .expect_err("source start must reject bad authentication");
    assert!(error.to_string().contains("PostgreSQL connect"), "{error}");
    assert_source_remains_created(&mut source).await;
    assert_eq!(slot_state(&admin).await, before);
}

#[tokio::test]
async fn source_start_reports_the_cluster_identity_privilege_requirement() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete'); \
             CREATE ROLE cdc_reader LOGIN REPLICATION PASSWORD 'cdc-password';"
        ))
        .await
        .expect("create restricted replication role");
    let mut config = source_config(&host, port);
    config.set("username", "cdc_reader");
    config.set("password", "cdc-password");
    let created_lsn = create_slot(&admin).await;
    let checkpoint = source_checkpoint(&admin, &config, created_lsn).await;
    let before = slot_state(&admin).await;

    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = source
        .start(resume_start(&config, checkpoint))
        .await
        .expect_err("cluster identity must not be silently omitted");
    assert!(error.to_string().contains("pg_control_system"), "{error}");
    assert!(
        error.to_string().contains("pg_control_checkpoint"),
        "{error}"
    );
    assert!(error.to_string().contains("pg_monitor"), "{error}");
    assert_source_remains_created(&mut source).await;
    assert_eq!(slot_state(&admin).await, before);
    let remaining_connections: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE usename = 'cdc_reader'",
            &[],
        )
        .await
        .expect("query leaked startup connections")
        .get(0);
    assert_eq!(
        remaining_connections, 0,
        "failed startup must release its control connection"
    );
}

#[tokio::test]
async fn resume_rejects_filter_publication_membership_and_recreation_drift() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY); \
             CREATE TABLE cdc_extra (id BIGINT PRIMARY KEY); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete');"
        ))
        .await
        .expect("create publication drift fixtures");
    let config = source_config(&host, port);
    let created_lsn = create_slot(&admin).await;
    let checkpoint = source_checkpoint(&admin, &config, created_lsn).await;
    let before = slot_state(&admin).await;

    let mut filtered_config = config.clone();
    filtered_config.set("table.include", "cdc_events");
    let mut filtered_source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = filtered_source
        .start(resume_start(&filtered_config, checkpoint.clone()))
        .await
        .expect_err("changed source filters must not reuse an old checkpoint");
    assert!(
        error.to_string().contains("filter/configuration"),
        "{error}"
    );
    assert_source_remains_created(&mut filtered_source).await;
    assert_eq!(slot_state(&admin).await, before);

    admin
        .batch_execute(&format!(
            "ALTER PUBLICATION {PUBLICATION} ADD TABLE cdc_extra;"
        ))
        .await
        .expect("change publication membership");
    let mut membership_source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = membership_source
        .start(resume_start(&config, checkpoint.clone()))
        .await
        .expect_err("changed publication membership must fail before replication");
    assert!(error.to_string().contains("identity drifted"), "{error}");
    assert_source_remains_created(&mut membership_source).await;
    assert_eq!(slot_state(&admin).await, before);

    admin
        .batch_execute(&format!(
            "DROP PUBLICATION {PUBLICATION}; \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete');"
        ))
        .await
        .expect("recreate publication with the same name and definition");
    let mut recreated_source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = recreated_source
        .start(resume_start(&config, checkpoint))
        .await
        .expect_err("same-name publication recreation must fail on OID drift");
    assert!(error.to_string().contains("identity drifted"), "{error}");
    assert_source_remains_created(&mut recreated_source).await;
    assert_eq!(slot_state(&admin).await, before);
}

#[tokio::test]
async fn resume_rejects_same_names_on_a_different_postgres_cluster() {
    let (_first_container, first_host, first_port) = start_postgres().await;
    let first_admin = connect(&first_host, first_port).await;
    first_admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete');"
        ))
        .await
        .expect("create first-cluster fixtures");
    let first_config = source_config(&first_host, first_port);
    let first_lsn = create_slot(&first_admin).await;
    let first_checkpoint = source_checkpoint(&first_admin, &first_config, first_lsn).await;

    let (_second_container, second_host, second_port) = start_postgres().await;
    let second_admin = connect(&second_host, second_port).await;
    second_admin
        .batch_execute(&format!(
            "CREATE TABLE cdc_events (id BIGINT PRIMARY KEY); \
             CREATE PUBLICATION {PUBLICATION} FOR TABLE cdc_events WITH (publish = 'insert, update, delete');"
        ))
        .await
        .expect("create second-cluster fixtures");
    let second_config = source_config(&second_host, second_port);
    create_slot(&second_admin).await;
    let before = slot_state(&second_admin).await;

    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = source
        .start(resume_start(&second_config, first_checkpoint))
        .await
        .expect_err("a checkpoint must be bound to its physical PostgreSQL cluster");
    assert!(error.to_string().contains("identity drifted"), "{error}");
    assert_source_remains_created(&mut source).await;
    assert_eq!(slot_state(&second_admin).await, before);
}

#[tokio::test]
async fn lookup_open_requires_a_usable_single_key_unique_index() {
    let (_container, host, port) = start_postgres().await;
    let admin = connect(&host, port).await;
    admin
        .batch_execute(
            "CREATE TABLE lookup_unindexed (id BIGINT, payload TEXT); \
             CREATE TABLE lookup_nonunique (id BIGINT, payload TEXT); \
             CREATE INDEX lookup_nonunique_id ON lookup_nonunique (id); \
             CREATE TABLE lookup_unique (id BIGINT, payload TEXT, included TEXT); \
             CREATE UNIQUE INDEX lookup_unique_id ON lookup_unique (id) INCLUDE (included); \
             CREATE TABLE lookup_primary (id BIGINT PRIMARY KEY, payload TEXT);",
        )
        .await
        .expect("create lookup admission fixtures");

    for table in ["public.lookup_unindexed", "public.lookup_nonunique"] {
        let error = match PostgresLookupSource::open(lookup_config(&host, port, table)).await {
            Ok(_) => panic!("non-unique lookup key must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unique index"), "{error}");
    }

    let missing =
        match PostgresLookupSource::open(lookup_config(&host, port, "public.lookup_missing")).await
        {
            Ok(_) => panic!("unresolved lookup table must be rejected"),
            Err(error) => error,
        };
    assert!(missing.to_string().contains("to_regclass"), "{missing}");

    PostgresLookupSource::open(lookup_config(&host, port, "public.lookup_unique"))
        .await
        .expect("single-key unique index with INCLUDE columns must be admitted");
    PostgresLookupSource::open(lookup_config(&host, port, "public.lookup_primary"))
        .await
        .expect("primary key must be admitted");
}
