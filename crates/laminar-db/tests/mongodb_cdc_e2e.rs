//! Direct MongoDB CDC through the engine: DDL, checkpoints, restarts, and real targets.
//!
//! Needs `docker compose -f tests/docker/mongodb-cdc-compose.yml up -d --wait`. Tests skip when
//! MongoDB is unreachable unless `LAMINAR_REQUIRE_MONGODB_CDC=1`.

#![cfg(all(
    feature = "mongodb-cdc",
    feature = "postgres-sink",
    feature = "delta-lake",
    feature = "files"
))]
#![allow(clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::Array;
use futures_util::TryStreamExt;
use laminar_db::{DeliveryGuarantee, LaminarDB};
use mongodb::bson::{doc, oid::ObjectId, Bson, Document};

const REQUIRE_ENV: &str = "LAMINAR_REQUIRE_MONGODB_CDC";
const MONGO_URI: &str = "mongodb://127.0.0.1:27117/?directConnection=true&tls=false";
const MONGO_RS3_URI: &str =
    "mongodb://127.0.0.1:27201,127.0.0.1:27202,127.0.0.1:27203/?replicaSet=rs3&tls=false";
const PG_PROPS: &str = "'hostname' = '127.0.0.1', 'port' = '15433', 'database' = 'mirror', \
     'username' = 'laminar', 'password' = '${E2E_PG_PASSWORD}', 'ssl.mode' = 'disable'";
const PG_CONN: &str =
    "host=127.0.0.1 port=15433 user=laminar password=laminar-test-secret dbname=mirror";
const CONVERGE: Duration = Duration::from_secs(60);

static NEXT: AtomicU64 = AtomicU64::new(0);

fn unique(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{name}_{:x}_{}",
        nanos % 0xffff_ffff,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn mongo(uri: &str) -> Option<mongodb::Client> {
    let mut options = mongodb::options::ClientOptions::parse(uri).await.unwrap();
    options.server_selection_timeout = Some(Duration::from_secs(5));
    let client = mongodb::Client::with_options(options).unwrap();
    match client.database("admin").run_command(doc! {"ping": 1}).await {
        Ok(_) => Some(client),
        Err(error) if std::env::var(REQUIRE_ENV).is_ok_and(|value| value == "1") => {
            panic!("MongoDB fixture is required by {REQUIRE_ENV}: {error}")
        }
        Err(error) => {
            eprintln!("skipping: MongoDB fixture unreachable: {error}");
            None
        }
    }
}

async fn postgres() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(PG_CONN, tokio_postgres::NoTls)
        .await
        .expect("PostgreSQL fixture");
    tokio::spawn(connection);
    client
}

async fn open(storage: &Path) -> std::sync::Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(300),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .config_var("E2E_PG_PASSWORD", "laminar-test-secret")
        .config_var("E2E_BAD_MONGO_PASSWORD", "wrong")
        .config_var("E2E_MINIO_SECRET", "minioadmin")
        .build()
        .await
        .expect("open database")
}

async fn collection_with_images(db: &mongodb::Database, name: &str) {
    db.run_command(doc! {"create": name, "changeStreamPreAndPostImages": {"enabled": true}})
        .await
        .unwrap();
}

/// Poll until `check` holds or `timeout` elapses, returning the last observation.
async fn eventually<T, F, Fut>(timeout: Duration, mut observe: F, check: impl Fn(&T) -> bool) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let value = observe().await;
        if check(&value) || tokio::time::Instant::now() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Open a database and register `statements`, waiting for the previous process generation to
/// release the checkpoint namespace lock after shutdown.
async fn reopen(storage: &Path, statements: &[String]) -> std::sync::Arc<LaminarDB> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let db = open(storage).await;
        match first_ddl_error(&db, statements).await {
            None => return db,
            Some(error) if error.contains("LDB-0014") && tokio::time::Instant::now() < deadline => {
                drop(db);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Some(error) => panic!("{error}"),
        }
    }
}

async fn first_ddl_error(db: &LaminarDB, statements: &[String]) -> Option<String> {
    for statement in statements {
        if let Err(error) = db.execute(statement).await {
            return Some(format!("{statement}: {error}"));
        }
    }
    None
}

async fn execute_all(db: &LaminarDB, statements: &[String]) {
    for statement in statements {
        db.execute(statement)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
}

/// `_id` → canonical Extended JSON of the remaining fields, for exact comparisons.
fn canonical_rows(documents: Vec<Document>) -> BTreeMap<String, String> {
    documents
        .into_iter()
        .map(|mut document| {
            let id = document.remove("_id").expect("_id");
            let key = match id {
                Bson::ObjectId(id) => id.to_hex(),
                Bson::String(id) => id,
                other => other.into_canonical_extjson().to_string(),
            };
            (
                key,
                Bson::Document(document)
                    .into_canonical_extjson()
                    .to_string(),
            )
        })
        .collect()
}

async fn mongo_rows(collection: &mongodb::Collection<Document>) -> BTreeMap<String, String> {
    canonical_rows(
        collection
            .find(doc! {})
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap(),
    )
}

async fn pg_rows(
    client: &tokio_postgres::Client,
    table: &str,
) -> BTreeMap<String, (Option<String>, Option<i64>)> {
    client
        .query(&format!("SELECT \"_id\", name, age FROM {table}"), &[])
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.get::<_, String>(0), (row.get(1), row.get(2))))
                .collect()
        })
        .unwrap_or_default()
}

async fn delta_rows(path: &Path) -> BTreeMap<String, (Option<String>, Option<i64>)> {
    let ctx = datafusion::prelude::SessionContext::new();
    let location = path.to_string_lossy().replace('\\', "/");
    if laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "mirror",
        &location,
        std::collections::HashMap::new(),
    )
    .await
    .is_err()
    {
        return BTreeMap::new();
    }
    let batches = ctx
        .sql("SELECT \"_id\", name, age FROM mirror")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = BTreeMap::new();
    for batch in batches {
        let utf8 = |index: usize| {
            arrow::compute::cast(batch.column(index), &arrow::datatypes::DataType::Utf8).unwrap()
        };
        let (ids, names) = (utf8(0), utf8(1));
        let ids = arrow::array::cast::as_string_array(&ids);
        let names = arrow::array::cast::as_string_array(&names);
        let ages = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.insert(
                ids.value(row).to_string(),
                (
                    (!names.is_null(row)).then(|| names.value(row).to_string()),
                    (!ages.is_null(row)).then(|| ages.value(row)),
                ),
            );
        }
    }
    rows
}

fn expected_rows(documents: &[Document]) -> BTreeMap<String, (Option<String>, Option<i64>)> {
    documents
        .iter()
        .map(|document| {
            let id = match document.get("_id").unwrap() {
                Bson::ObjectId(id) => id.to_hex(),
                Bson::String(id) => id.clone(),
                other => panic!("unexpected _id {other:?}"),
            };
            (
                id,
                (
                    document.get_str("name").ok().map(str::to_string),
                    document.get_i64("age").ok(),
                ),
            )
        })
        .collect()
}

fn document_source(name: &str, uri: &str, database: &str, collection: &str, extra: &str) -> String {
    format!(
        "CREATE SOURCE {name} (_id VARCHAR NOT NULL, name VARCHAR, age BIGINT, doc VARCHAR, \
         PRIMARY KEY (_id)) FROM \"mongodb-cdc\" ('connection.uri' = '{uri}', \
         'database' = '{database}', 'collection' = '{collection}', 'output.mode' = 'document', \
         'full.document.mode' = 'required', 'objectid.columns' = '_id', \
         'document.json.column' = 'doc'{extra})"
    )
}

fn history_source(name: &str, uri: &str, database: &str, collection: &str, extra: &str) -> String {
    format!(
        "CREATE SOURCE {name} FROM \"mongodb-cdc\" ('connection.uri' = '{uri}', \
         'database' = '{database}', 'collection' = '{collection}', \
         'full.document.mode' = 'required'{extra})"
    )
}

/// Every mirror shape the change applies to: PostgreSQL upsert, Delta MERGE, MongoDB replay.
struct Mirrors {
    pg_table: String,
    delta_path: std::path::PathBuf,
    mirror_db: String,
    statements: Vec<String>,
}

fn mirrors(storage: &Path, database: &str, collection: &str) -> Mirrors {
    let pg_table = unique("users");
    let delta_path = storage.join(unique("delta_users"));
    let mirror_db = unique("mirror");
    let delta_location = delta_path.to_string_lossy().replace('\\', "/");
    let statements = vec![
        document_source("users_doc", MONGO_URI, database, collection, ""),
        history_source("users_history", MONGO_URI, database, collection, ""),
        format!(
            "CREATE SINK users_pg FROM users_doc INTO \"postgres-sink\" ({PG_PROPS}, \
             'table.name' = '{pg_table}', 'write.mode' = 'upsert', 'primary.key' = '_id', \
             'changelog.mode' = 'true', 'auto.create.table' = 'true')"
        ),
        format!(
            "CREATE SINK users_delta FROM users_doc INTO \"delta-lake\" (\
             'table.path' = '{delta_location}', 'write.mode' = 'upsert', \
             'merge.key.columns' = '_id', 'auto.create' = 'true')"
        ),
        format!(
            "CREATE SINK users_mongo FROM users_history INTO \"mongodb-sink\" (\
             'connection.uri' = '{MONGO_URI}', 'database' = '{mirror_db}', \
             'collection' = '{collection}', 'write.mode' = 'cdc_replay', \
             'replay.source.namespace' = '{database}.{collection}', 'auto.create' = 'true')"
        ),
    ];
    Mirrors {
        pg_table,
        delta_path,
        mirror_db,
        statements,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_and_history_mirrors_apply_puts_and_key_only_deletes_across_restart() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("app");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "users").await;
    let users = source_db.collection::<Document>("users");
    let storage = tempfile::tempdir().unwrap();
    let mirrors = mirrors(storage.path(), &database, "users");
    let pg = postgres().await;

    let ids: Vec<ObjectId> = (0..4).map(|_| ObjectId::new()).collect();
    {
        let db = open(storage.path()).await;
        execute_all(&db, &mirrors.statements).await;
        db.start().await.expect("start");
        for (index, id) in ids.iter().enumerate() {
            users
                .insert_one(doc! {
                    "_id": id,
                    "name": format!("user{index}"),
                    "age": 20_i64 + index as i64,
                    "tags": ["a", {"nested": 1_i32}],
                    "balance": "12.30".parse::<mongodb::bson::Decimal128>().unwrap(),
                    "big": 9_007_199_254_740_993_i64,
                    "small_long": 5_i64,
                    "at": mongodb::bson::DateTime::from_millis(1_700_000_000_123),
                    "nothing": Bson::Null,
                })
                .await
                .unwrap();
        }
        users
            .update_one(doc! {"_id": ids[0]}, doc! {"$set": {"name": "renamed"}})
            .await
            .unwrap();
        users
            .update_one(doc! {"_id": ids[0]}, doc! {"$inc": {"age": 1_i64}})
            .await
            .unwrap();
        users
            .replace_one(
                doc! {"_id": ids[1]},
                doc! {"name": "replaced", "age": 99_i64},
            )
            .await
            .unwrap();
        users.delete_one(doc! {"_id": ids[2]}).await.unwrap();
        users.delete_one(doc! {"_id": ids[3]}).await.unwrap();
        users
            .insert_one(doc! {"_id": ids[3], "name": "reborn", "age": 7_i64})
            .await
            .unwrap();

        let expected = expected_rows(
            &users
                .find(doc! {})
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
        );
        let observed = eventually(
            CONVERGE,
            || pg_rows(&pg, &mirrors.pg_table),
            |rows| rows == &expected,
        )
        .await;
        assert_eq!(observed, expected, "PostgreSQL mirror");
        assert!(db.checkpoint().await.unwrap().success);
        db.shutdown().await.expect("shutdown");
    }

    // Changes while the pipeline is down are replayed from the committed resume token.
    users
        .update_one(doc! {"_id": ids[0]}, doc! {"$set": {"name": "offline"}})
        .await
        .unwrap();
    users.delete_one(doc! {"_id": ids[1]}).await.unwrap();
    {
        let db = reopen(storage.path(), &mirrors.statements).await;
        db.start().await.expect("restart");
        users
            .insert_one(doc! {"_id": ObjectId::new(), "name": "after-restart", "age": 1_i64})
            .await
            .unwrap();
        let documents: Vec<Document> = users
            .find(doc! {})
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let expected = expected_rows(&documents);
        let observed = eventually(
            CONVERGE,
            || pg_rows(&pg, &mirrors.pg_table),
            |rows| rows == &expected,
        )
        .await;
        assert_eq!(observed, expected, "PostgreSQL mirror after restart");
        let delta = eventually(
            CONVERGE,
            || delta_rows(&mirrors.delta_path),
            |rows| rows == &expected,
        )
        .await;
        assert_eq!(delta, expected, "Delta mirror after restart");
        let mongo_mirror = client
            .database(&mirrors.mirror_db)
            .collection::<Document>("users");
        let source_rows = mongo_rows(&users).await;
        let mirror_rows = eventually(
            CONVERGE,
            || mongo_rows(&mongo_mirror),
            |rows| rows == &source_rows,
        )
        .await;
        assert_eq!(
            mirror_rows, source_rows,
            "MongoDB cdc_replay mirror keeps exact BSON types"
        );
        let kept = pg
            .query_one(
                &format!("SELECT doc FROM {} WHERE \"_id\" = $1", mirrors.pg_table),
                &[&ids[3].to_hex()],
            )
            .await
            .unwrap()
            .get::<_, Option<String>>(0);
        assert!(kept.is_some_and(|doc| doc.contains("\"_id\":{\"$oid\"")));
        db.shutdown().await.expect("shutdown after restart");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_export_keeps_every_event_including_deletes() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("hist");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "events").await;
    let events = source_db.collection::<Document>("events");
    let storage = tempfile::tempdir().unwrap();
    let files_path = storage.path().join("history_files");
    std::fs::create_dir_all(&files_path).unwrap();
    let delta_path = storage.path().join("history_delta");
    let delta_location = delta_path.to_string_lossy().replace('\\', "/");
    let files_location = files_path.to_string_lossy().replace('\\', "/");
    let statements = vec![
        history_source("changes", MONGO_URI, &database, "events", ""),
        format!(
            "CREATE SINK changes_files FROM changes INTO FILES ('path' = '{files_location}') \
             FORMAT JSON"
        ),
        format!(
            "CREATE SINK changes_delta FROM changes INTO \"delta-lake\" (\
             'table.path' = '{delta_location}', 'write.mode' = 'append', 'auto.create' = 'true')"
        ),
    ];

    let db = open(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    let id = ObjectId::new();
    events
        .insert_one(doc! {"_id": id, "v": 1_i32})
        .await
        .unwrap();
    events
        .update_one(doc! {"_id": id}, doc! {"$set": {"v": 2_i32}})
        .await
        .unwrap();
    events
        .update_one(doc! {"_id": id}, doc! {"$set": {"v": 3_i32}})
        .await
        .unwrap();
    events
        .replace_one(doc! {"_id": id}, doc! {"v": 4_i32})
        .await
        .unwrap();
    events.delete_one(doc! {"_id": id}).await.unwrap();

    let read_files = || async {
        let mut rows = Vec::new();
        for entry in std::fs::read_dir(&files_path)
            .into_iter()
            .flatten()
            .flatten()
        {
            if let Ok(text) = std::fs::read_to_string(entry.path()) {
                rows.extend(
                    text.lines()
                        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok()),
                );
            }
        }
        rows
    };
    let rows = eventually(CONVERGE, read_files, |rows| rows.len() >= 5).await;
    assert!(db.checkpoint().await.unwrap().success);
    db.shutdown().await.expect("shutdown");

    // The append lakehouse keeps the delete as a history row instead of deleting older rows.
    let ctx = datafusion::prelude::SessionContext::new();
    laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "history",
        &delta_location,
        std::collections::HashMap::new(),
    )
    .await
    .unwrap();
    let batches = ctx
        .sql("SELECT operation FROM history ORDER BY cluster_time_seconds, cluster_time_increment")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut lake_operations = Vec::new();
    for batch in &batches {
        let column =
            arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8).unwrap();
        let column = arrow::array::cast::as_string_array(&column);
        lake_operations.extend((0..column.len()).map(|row| column.value(row).to_string()));
    }
    assert_eq!(
        lake_operations,
        ["insert", "update", "update", "replace", "delete"]
    );

    let mut rows = rows;
    rows.sort_by_key(|row| {
        (
            row["cluster_time_seconds"].as_i64(),
            row["cluster_time_increment"].as_i64(),
        )
    });
    let operations: Vec<&str> = rows
        .iter()
        .map(|row| row["operation"].as_str().unwrap())
        .collect();
    assert_eq!(
        operations,
        ["insert", "update", "update", "replace", "delete"]
    );
    let identities: std::collections::BTreeSet<&str> = rows
        .iter()
        .map(|row| row["event_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        identities.len(),
        5,
        "repeated updates keep distinct identities"
    );
    let key = format!("{{\"_id\":{{\"$oid\":\"{}\"}}}}", id.to_hex());
    assert!(rows.iter().all(|row| row["document_key"] == key.as_str()));
    assert!(
        rows[4]["full_document"].is_null(),
        "a delete carries only its key"
    );
    assert_eq!(rows[0]["event_version"], 1);
}

fn snapshot_statements(database: &str, pg_table: &str) -> Vec<String> {
    vec![
        document_source(
            "seeded",
            MONGO_URI,
            database,
            "accounts",
            ", 'snapshot.mode' = 'initial', 'max.buffered.bytes' = '1048576'",
        ),
        format!(
            "CREATE SINK seeded_pg FROM seeded INTO \"postgres-sink\" ({PG_PROPS}, \
             'table.name' = '{pg_table}', 'write.mode' = 'upsert', 'primary.key' = '_id', \
             'changelog.mode' = 'true', 'auto.create.table' = 'true')"
        ),
    ]
}

/// Inserts, updates, deletes, and delete-then-reinserts while the snapshot scan runs.
async fn churn(accounts: mongodb::Collection<Document>, seeded: Vec<ObjectId>, rounds: usize) {
    for round in 0..rounds {
        let target = seeded[(round * 7919) % seeded.len()];
        match round % 4 {
            0 => {
                accounts
                    .update_one(
                        doc! {"_id": target},
                        doc! {"$set": {"name": format!("u{round}")}},
                    )
                    .await
                    .unwrap();
            }
            1 => {
                accounts.delete_one(doc! {"_id": target}).await.unwrap();
            }
            2 => {
                accounts
                    .insert_one(doc! {"_id": ObjectId::new(), "name": format!("n{round}"), "age": round as i64})
                    .await
                    .unwrap();
            }
            _ => {
                accounts.delete_one(doc! {"_id": target}).await.unwrap();
                accounts
                    .insert_one(doc! {"_id": target, "name": format!("r{round}"), "age": -1_i64})
                    .await
                    .unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_snapshot_converges_under_concurrent_writes_and_mid_scan_restarts() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("snap");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "accounts").await;
    let accounts = source_db.collection::<Document>("accounts");
    let padding = "x".repeat(1024);
    let seeded: Vec<ObjectId> = (0..20_000).map(|_| ObjectId::new()).collect();
    for chunk in seeded.chunks(1000) {
        accounts
            .insert_many(chunk.iter().enumerate().map(|(index, id)| {
                doc! {"_id": id, "name": format!("seed{index}"), "age": index as i64, "pad": &padding}
            }))
            .await
            .unwrap();
    }
    let storage = tempfile::tempdir().unwrap();
    let pg_table = unique("accounts");
    let statements = snapshot_statements(&database, &pg_table);
    let pg = postgres().await;

    let writer = tokio::spawn(churn(accounts.clone(), seeded.clone(), 400));
    // Stop twice during the copy; each restart continues the same snapshot cut.
    let mut copied_at_shutdown = Vec::new();
    for _ in 0..2 {
        let db = reopen(storage.path(), &statements).await;
        db.start().await.expect("start");
        let before = pg_rows(&pg, &pg_table).await.len();
        eventually(
            CONVERGE,
            || pg_rows(&pg, &pg_table),
            |rows| rows.len() > before,
        )
        .await;
        db.shutdown().await.expect("shutdown mid-scan");
        copied_at_shutdown.push(pg_rows(&pg, &pg_table).await.len());
    }
    assert!(
        copied_at_shutdown
            .iter()
            .any(|copied| *copied < seeded.len()),
        "no restart interrupted the snapshot copy: {copied_at_shutdown:?}"
    );
    writer.await.unwrap();

    let db = reopen(storage.path(), &statements).await;
    db.start().await.expect("final start");
    let documents: Vec<Document> = accounts
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let expected = expected_rows(&documents);
    let observed = eventually(
        CONVERGE,
        || pg_rows(&pg, &pg_table),
        |rows| rows == &expected,
    )
    .await;
    let missing = expected
        .keys()
        .filter(|key| !observed.contains_key(*key))
        .count();
    let stale = observed
        .keys()
        .filter(|key| !expected.contains_key(*key))
        .count();
    assert_eq!(
        (missing, stale),
        (0, 0),
        "snapshot handoff must neither lose rows nor resurrect deleted ones"
    );
    assert_eq!(observed, expected);
    db.shutdown().await.expect("shutdown");
}

/// The first error from executing `statements` and then starting the pipeline.
async fn first_error(db: &std::sync::Arc<LaminarDB>, statements: &[String]) -> String {
    for statement in statements {
        if let Err(error) = db.execute(statement).await {
            return error.to_string();
        }
    }
    db.start()
        .await
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default()
}

async fn expect_start_error(statements: &[String], storage: &Path, needle: &str) {
    let db = open(storage).await;
    let failure = first_error(&db, statements).await;
    assert!(
        failure.contains(needle),
        "expected a failure containing {needle:?}, got {failure:?}"
    );
    let _ = db.shutdown().await;
}

/// Process-wide log capture: an embedded pipeline reports a terminal source fault by stopping
/// its coordinator and logging the cause.
fn captured_logs() -> &'static std::sync::Mutex<Vec<u8>> {
    static LOGS: std::sync::OnceLock<&'static std::sync::Mutex<Vec<u8>>> =
        std::sync::OnceLock::new();
    LOGS.get_or_init(|| {
        let logs: &'static std::sync::Mutex<Vec<u8>> =
            Box::leak(Box::new(std::sync::Mutex::new(Vec::new())));
        let writer = move || LogWriter(logs);
        let _ = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .try_init();
        logs
    })
}

struct LogWriter(&'static std::sync::Mutex<Vec<u8>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Wait until the pipeline stops and its logged fault names `needle`.
async fn expect_pipeline_fault(db: &LaminarDB, needle: &str) {
    let logs = captured_logs();
    let stopped = eventually(
        CONVERGE,
        || async {
            let text = String::from_utf8_lossy(&logs.lock().unwrap()).into_owned();
            (text.contains(needle), db.checkpoint().await.is_err())
        },
        |(logged, stopped)| *logged && *stopped,
    )
    .await;
    assert_eq!(
        stopped,
        (true, true),
        "pipeline must stop with a fault naming {needle:?}"
    );
}

fn pg_sink_on(source: &str, table: &str, extra: &str) -> String {
    format!(
        "CREATE SINK {source}_pg FROM {source} INTO \"postgres-sink\" ({PG_PROPS}, \
         'table.name' = '{table}', 'write.mode' = 'upsert', 'primary.key' = '_id', \
         'auto.create.table' = 'true'{extra})"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_compositions_fail_before_any_data_moves() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("reject");
    collection_with_images(&client.database(&database), "users").await;
    client
        .database(&database)
        .create_collection("plain")
        .await
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let source = document_source("docs", MONGO_URI, &database, "users", "");
    let changelog = ", 'changelog.mode' = 'true'";
    let table = unique("reject");
    let bad_credentials =
        "mongodb://nobody:${E2E_BAD_MONGO_PASSWORD}@127.0.0.1:27117/?directConnection=true&tls=false";
    let cases: Vec<(Vec<String>, &str)> = vec![
        (
            vec![
                source.clone(),
                pg_sink_on("docs", &table, changelog).replacen(" INTO ", " WHERE age > 1 INTO ", 1),
            ],
            "cannot filter",
        ),
        (
            vec![
                source.clone(),
                "CREATE STREAM copy AS SELECT _id, name FROM docs".into(),
            ],
            "mutation source",
        ),
        (
            vec![source.clone(), pg_sink_on("docs", &table, "")],
            "changelog.mode=true",
        ),
        (
            vec![
                document_source("docs", MONGO_URI, &database, "plain", ""),
                pg_sink_on("docs", &table, changelog),
            ],
            "changeStreamPreAndPostImages",
        ),
        (
            vec![
                document_source("docs", bad_credentials, &database, "users", ""),
                pg_sink_on("docs", &table, changelog),
            ],
            "uthentication",
        ),
        (
            vec![format!(
                "CREATE SOURCE docs (_id VARCHAR NOT NULL, name VARCHAR NOT NULL, \
                 PRIMARY KEY (_id)) FROM \"mongodb-cdc\" ('connection.uri' = '{MONGO_URI}', \
                 'database' = '{database}', 'collection' = 'users', 'output.mode' = 'document', \
                 'full.document.mode' = 'required')"
            )],
            "must be nullable",
        ),
        (
            vec![format!(
                "CREATE SOURCE docs (_id VARCHAR NOT NULL, PRIMARY KEY (_id)) FROM \
                 \"mongodb-cdc\" ('connection.uri' = '{MONGO_URI}', 'database' = '{database}', \
                 'collection' = 'users', 'output.mode' = 'document')"
            )],
            "full.document.mode=required",
        ),
    ];
    for (index, (statements, needle)) in cases.into_iter().enumerate() {
        let case_storage = storage.path().join(format!("case{index}"));
        expect_start_error(&statements, &case_storage, needle).await;
    }

    // Exactly-once is not certified for MongoDB CDC sources.
    let exact = LaminarDB::builder()
        .storage_dir(storage.path().join("exact"))
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
        .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
        .config_var("E2E_PG_PASSWORD", "laminar-test-secret")
        .build()
        .await
        .unwrap();
    let error = first_error(
        &exact,
        &[source.clone(), pg_sink_on("docs", &table, changelog)],
    )
    .await;
    assert!(error.contains("LDB-5037"), "{error}");
    let _ = exact.shutdown().await;

    // The initial snapshot needs durable checkpoints to commit its cut before copying.
    let unchecked = LaminarDB::builder()
        .storage_dir(storage.path().join("unchecked"))
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .config_var("E2E_PG_PASSWORD", "laminar-test-secret")
        .build()
        .await
        .unwrap();
    let snapshot = document_source(
        "docs",
        MONGO_URI,
        &database,
        "users",
        ", 'snapshot.mode' = 'initial'",
    );
    let error = first_error(
        &unchecked,
        &[snapshot, pg_sink_on("docs", &table, changelog)],
    )
    .await;
    assert!(error.contains("checkpoint"), "{error}");
    let _ = unchecked.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_change_and_missing_images_stop_without_silent_reset() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("fence");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "users").await;
    let users = source_db.collection::<Document>("users");
    let storage = tempfile::tempdir().unwrap();
    let pg_table = unique("fence");
    let pg = postgres().await;
    let statements = vec![
        document_source("docs", MONGO_URI, &database, "users", ""),
        pg_sink_on("docs", &pg_table, ", 'changelog.mode' = 'true'"),
    ];
    let kept = ObjectId::new();
    {
        captured_logs();
        let db = open(storage.path()).await;
        execute_all(&db, &statements).await;
        db.start().await.unwrap();
        users
            .insert_one(doc! {"_id": kept, "name": "kept", "age": 1_i64})
            .await
            .unwrap();
        eventually(CONVERGE, || pg_rows(&pg, &pg_table), |rows| rows.len() == 1).await;
        assert!(db.checkpoint().await.unwrap().success);
        // Without post-images the next update has no exact image; it must never become a delete.
        source_db
            .run_command(
                doc! {"collMod": "users", "changeStreamPreAndPostImages": {"enabled": false}},
            )
            .await
            .unwrap();
        users
            .update_one(doc! {"_id": kept}, doc! {"$set": {"name": "lost-image"}})
            .await
            .unwrap();
        expect_pipeline_fault(&db, "post-image").await;
        let rows = pg_rows(&pg, &pg_table).await;
        assert_eq!(rows[&kept.to_hex()].0.as_deref(), Some("kept"));
        let _ = db.shutdown().await;
    }

    // Dropping and recreating the collection changes its UUID; recovery refuses to rebind.
    users.drop().await.unwrap();
    collection_with_images(&source_db, "users").await;
    expect_start_error(&statements, storage.path(), "identity changed").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unusable_resume_position_fails_without_restarting_from_now() {
    use laminar_connectors::config::ConnectorConfig;
    use laminar_connectors::connector::{SourceConnector, SourcePosition, SourceStart};

    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("lost");
    collection_with_images(&client.database(&database), "users").await;
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", MONGO_URI);
    config.set("database", &database);
    config.set("collection", "users");
    let new_source = || {
        laminar_connectors::mongodb::MongoDbCdcSource::new(
            laminar_connectors::mongodb::MongoDbSourceConfig::default(),
            None,
        )
    };

    // Capture a genuine checkpoint, then replace its position.
    let mut source = new_source();
    source
        .start(
            SourceStart::new(
                config.clone(),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let captured = source.checkpoint();
    source.close().await.unwrap();
    let mut rewound = laminar_connectors::checkpoint::SourceCheckpoint::new();
    for (key, value) in captured.metadata() {
        rewound.set_metadata(key.clone(), value.clone());
    }
    // A token the server cannot decode must fail permanently, never restart from "now".
    rewound.set_offset("resume_token", r#"{"_data":"00"}"#);
    rewound.set_offset("sequence", "0");

    let error = new_source()
        .start(
            SourceStart::new(
                config,
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(3),
                    checkpoint: rewound,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("an unusable resume position must not silently restart from now");
    let message = error.to_string();
    assert!(message.contains("cannot be resumed"), "{message}");
    assert!(!error.is_transient());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_set_election_keeps_the_mirror_exact() {
    let Some(client) = mongo(MONGO_RS3_URI).await else {
        return;
    };
    let database = unique("elect");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "users").await;
    let users = source_db.collection::<Document>("users");
    let storage = tempfile::tempdir().unwrap();
    let pg_table = unique("elect");
    let pg = postgres().await;
    let statements = vec![
        document_source("docs", MONGO_RS3_URI, &database, "users", ""),
        pg_sink_on("docs", &pg_table, ", 'changelog.mode' = 'true'"),
    ];
    let db = open(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");

    let writer = {
        let users = users.clone();
        tokio::spawn(async move {
            let ids: Vec<ObjectId> = (0..300).map(|_| ObjectId::new()).collect();
            for (index, id) in ids.iter().enumerate() {
                users
                    .insert_one(doc! {"_id": id, "name": "v1", "age": index as i64})
                    .await
                    .unwrap();
                if index % 3 == 0 {
                    users
                        .update_one(doc! {"_id": id}, doc! {"$set": {"name": "v2"}})
                        .await
                        .unwrap();
                }
                if index % 5 == 0 {
                    users.delete_one(doc! {"_id": id}).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(800)).await;
    let before = client
        .database("admin")
        .run_command(doc! {"hello": 1})
        .await
        .unwrap();
    // The driver retries elections for both the writer and the change stream.
    let _ = client
        .database("admin")
        .run_command(doc! {"replSetStepDown": 30, "secondaryCatchUpPeriodSecs": 10})
        .await;
    writer.await.unwrap();
    let after = eventually(
        CONVERGE,
        || async {
            client
                .database("admin")
                .run_command(doc! {"hello": 1})
                .await
                .ok()
                .and_then(|hello| hello.get_str("primary").ok().map(str::to_string))
        },
        |primary| primary.is_some(),
    )
    .await;
    assert_ne!(
        before.get_str("primary").ok(),
        after.as_deref(),
        "the step-down must elect a different primary"
    );

    let documents: Vec<Document> = users
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let expected = expected_rows(&documents);
    let observed = eventually(
        CONVERGE,
        || pg_rows(&pg, &pg_table),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected, "mirror after an election");
    db.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_write_outcome_on_one_target_replays_both_without_stale_overwrite() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("dual");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "users").await;
    let users = source_db.collection::<Document>("users");
    let storage = tempfile::tempdir().unwrap();
    let pg_table = unique("dual");
    let mirror_db = unique("dualmirror");
    let pg = postgres().await;
    let statements = vec![
        document_source("docs", MONGO_URI, &database, "users", ""),
        history_source("changes", MONGO_URI, &database, "users", ""),
        pg_sink_on("docs", &pg_table, ", 'changelog.mode' = 'true'"),
        format!(
            "CREATE SINK changes_mongo FROM changes INTO \"mongodb-sink\" (\
             'connection.uri' = '{MONGO_URI}', 'database' = '{mirror_db}', \
             'collection' = 'users', 'write.mode' = 'cdc_replay', \
             'replay.source.namespace' = '{database}.users', 'auto.create' = 'true', \
             'sink.write.timeout.ms' = '1000')"
        ),
    ];
    let mirror = client.database(&mirror_db).collection::<Document>("users");
    let id = ObjectId::new();
    let name_of = |collection: mongodb::Collection<Document>| async move {
        collection
            .find_one(doc! {"_id": id})
            .await
            .ok()
            .flatten()
            .and_then(|document| document.get_str("name").ok().map(str::to_string))
    };

    let db = open(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    users
        .insert_one(doc! {"_id": id, "name": "v1", "age": 1_i64})
        .await
        .unwrap();
    eventually(
        CONVERGE,
        || name_of(mirror.clone()),
        |name| name.as_deref() == Some("v1"),
    )
    .await;
    assert!(db.checkpoint().await.unwrap().success);

    // The mirror's next bulk write blocks past the sink deadline: its outcome is unknown, the
    // writer is retired, and PostgreSQL may already hold the newer value.
    let failpoint_count = |response: Document| {
        response
            .get_i64("count")
            .or_else(|_| response.get_i32("count").map(i64::from))
            .unwrap()
    };
    let entered_before = failpoint_count(
        client
            .database("admin")
            .run_command(doc! {
                "configureFailPoint": "failCommand",
                "mode": {"times": 1},
                "data": {
                    "failCommands": ["bulkWrite"],
                    "blockConnection": true,
                    "blockTimeMS": 6000,
                },
            })
            .await
            .unwrap(),
    );
    users
        .update_one(doc! {"_id": id}, doc! {"$set": {"name": "v2"}})
        .await
        .unwrap();
    eventually(
        CONVERGE,
        || pg_rows(&pg, &pg_table),
        |rows| rows.get(&id.to_hex()).and_then(|row| row.0.as_deref()) == Some("v2"),
    )
    .await;
    let _ = db.shutdown().await;
    let entered = failpoint_count(
        client
            .database("admin")
            .run_command(doc! {"configureFailPoint": "failCommand", "mode": "off"})
            .await
            .unwrap(),
    );
    assert_eq!(
        entered - entered_before,
        1,
        "the mirror write must have blocked past its deadline"
    );

    // Recovery replays from the last checkpoint; a newer write follows immediately.
    let db = eventually(
        CONVERGE,
        || async {
            let db = reopen(storage.path(), &statements).await;
            match db.start().await {
                Ok(()) => Some(db),
                Err(error) => {
                    eprintln!("restart deferred: {error}");
                    let _ = db.shutdown().await;
                    None
                }
            }
        },
        Option::is_some,
    )
    .await
    .expect("restart after the retired writer resolves");
    users
        .update_one(doc! {"_id": id}, doc! {"$set": {"name": "v3"}})
        .await
        .unwrap();
    // Outlast the blocked write: a retired writer must never land after its successor.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let mirrored = eventually(
        CONVERGE,
        || name_of(mirror.clone()),
        |name| name.as_deref() == Some("v3"),
    )
    .await;
    assert_eq!(mirrored.as_deref(), Some("v3"), "MongoDB mirror");
    let rows = pg_rows(&pg, &pg_table).await;
    assert_eq!(
        rows[&id.to_hex()].0.as_deref(),
        Some("v3"),
        "PostgreSQL mirror"
    );
    db.shutdown().await.expect("shutdown");
}

type TextRows = BTreeMap<String, Vec<Option<String>>>;

/// Rows keyed by their first column, every column rendered as text by the query.
async fn pg_text_rows(client: &tokio_postgres::Client, query: &str) -> TextRows {
    client
        .query(query, &[])
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    let values = (1..row.len()).map(|index| row.get(index)).collect();
                    (row.get::<_, String>(0), values)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Rows of a Delta table keyed by their first column, every column cast to text.
async fn delta_text_rows(path: &Path, select: &str) -> TextRows {
    let ctx = datafusion::prelude::SessionContext::new();
    let location = path.to_string_lossy().replace('\\', "/");
    if laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "mirror",
        &location,
        std::collections::HashMap::new(),
    )
    .await
    .is_err()
    {
        return BTreeMap::new();
    }
    let Ok(frame) = ctx.sql(&format!("SELECT {select} FROM mirror")).await else {
        return BTreeMap::new();
    };
    let mut rows = BTreeMap::new();
    for batch in frame.collect().await.unwrap() {
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| arrow::compute::cast(column, &arrow::datatypes::DataType::Utf8).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let mut values = columns.iter().map(|column| {
                let column = arrow::array::cast::as_string_array(column);
                (!column.is_null(row)).then(|| column.value(row).to_string())
            });
            let key = values.next().flatten().expect("key column");
            rows.insert(key, values.collect());
        }
    }
    rows
}

fn text(values: &[Option<&str>]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn typed_columns_and_history_reach_postgres_and_delta() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("typed");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "items").await;
    let items = source_db.collection::<Document>("items");
    let storage = tempfile::tempdir().unwrap();
    let pg_table = unique("typed");
    let history_table = unique("typed_history");
    let lake = storage.path().join(unique("typed_lake"));
    let location = |path: &Path| path.to_string_lossy().replace('\\', "/");
    let mode = "'output.mode' = 'document', 'full.document.mode' = 'required', \
                'objectid.columns' = '_id'";
    let statements = vec![
        format!(
            "CREATE SOURCE typed (_id VARCHAR NOT NULL, i INT, l BIGINT, d DOUBLE, b BOOLEAN, \
             amount DECIMAL(18, 2), at TIMESTAMP, bin BYTEA, doc VARCHAR, PRIMARY KEY (_id)) \
             FROM \"mongodb-cdc\" ('connection.uri' = '{MONGO_URI}', 'database' = '{database}', \
             'collection' = 'items', {mode}, 'document.json.column' = 'doc')"
        ),
        history_source("changes", MONGO_URI, &database, "items", ""),
        pg_sink_on("typed", &pg_table, ", 'changelog.mode' = 'true'"),
        format!(
            "CREATE SINK typed_delta FROM typed INTO \"delta-lake\" ('table.path' = '{}', \
             'write.mode' = 'upsert', 'merge.key.columns' = '_id', 'auto.create' = 'true')",
            location(&lake)
        ),
        format!(
            "CREATE SINK changes_pg FROM changes INTO \"postgres-sink\" ({PG_PROPS}, \
             'table.name' = '{history_table}', 'write.mode' = 'append', \
             'auto.create.table' = 'true')"
        ),
    ];
    let (kept, deleted, widened, sparse) = (
        ObjectId::new(),
        ObjectId::new(),
        ObjectId::new(),
        ObjectId::new(),
    );
    let at = mongodb::bson::DateTime::from_millis(1_700_000_000_123);
    let decimal = |value: &str| Bson::Decimal128(value.parse().unwrap());
    let binary = Bson::Binary(mongodb::bson::Binary {
        subtype: mongodb::bson::spec::BinarySubtype::Generic,
        bytes: vec![1, 2, 3],
    });

    let db = open(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    items
        .insert_one(doc! {
            "_id": kept, "i": 7_i32, "l": 9_007_199_254_740_993_i64, "d": 1.5, "b": true,
            "amount": decimal("12345.67"), "at": at, "bin": binary, "nested": {"x": [1_i32, "a"]},
        })
        .await
        .unwrap();
    items
        .insert_one(doc! {"_id": deleted, "i": 1_i32})
        .await
        .unwrap();
    // Integers widen exactly into BIGINT, DOUBLE, and DECIMAL columns.
    items
        .insert_one(doc! {"_id": widened, "l": 5_i32, "d": 2_i32, "amount": 3_i32})
        .await
        .unwrap();
    items
        .insert_one(doc! {"_id": sparse, "i": Bson::Null})
        .await
        .unwrap();
    items
        .update_one(
            doc! {"_id": kept},
            doc! {"$set": {"l": -5_i64, "amount": decimal("0.10")}},
        )
        .await
        .unwrap();
    items.delete_one(doc! {"_id": deleted}).await.unwrap();

    let pg = postgres().await;
    let pg_expected: TextRows = [
        (
            kept,
            text(&[
                Some("7"),
                Some("-5"),
                Some("1.5"),
                Some("true"),
                Some("0.10"),
                Some("2023-11-14 22:13:20.123"),
                Some("010203"),
            ]),
        ),
        (
            widened,
            text(&[None, Some("5"), Some("2"), None, Some("3.00"), None, None]),
        ),
        (sparse, text(&[None, None, None, None, None, None, None])),
    ]
    .into_iter()
    .map(|(id, values)| (id.to_hex(), values))
    .collect();
    let pg_query = format!(
        "SELECT \"_id\", i::text, l::text, d::text, b::text, amount::text, at::text, \
         encode(bin, 'hex') FROM {pg_table}"
    );
    let observed = eventually(
        CONVERGE,
        || pg_text_rows(&pg, &pg_query),
        |rows| rows == &pg_expected,
    )
    .await;
    assert_eq!(observed, pg_expected, "PostgreSQL typed mirror");

    let lake_expected: TextRows = [
        (
            kept,
            text(&[
                Some("7"),
                Some("-5"),
                Some("1.5"),
                Some("true"),
                Some("0.10"),
                Some("2023-11-14T22:13:20.123"),
                Some("010203"),
            ]),
        ),
        (
            widened,
            text(&[None, Some("5"), Some("2.0"), None, Some("3.00"), None, None]),
        ),
        (sparse, text(&[None, None, None, None, None, None, None])),
    ]
    .into_iter()
    .map(|(id, values)| (id.to_hex(), values))
    .collect();
    let observed = eventually(
        CONVERGE,
        || delta_text_rows(&lake, "\"_id\", i, l, d, b, amount, at, encode(bin, 'hex')"),
        |rows| rows == &lake_expected,
    )
    .await;
    assert_eq!(observed, lake_expected, "Delta typed mirror");

    // The JSON column keeps exact BSON types, nesting, and explicit null versus missing.
    let documents = pg_text_rows(&pg, &format!("SELECT \"_id\", doc FROM {pg_table}")).await;
    let document = |id: ObjectId| -> serde_json::Value {
        serde_json::from_str(documents[&id.to_hex()][0].as_deref().unwrap()).unwrap()
    };
    let kept_document = document(kept);
    assert_eq!(kept_document["l"], serde_json::json!({"$numberLong": "-5"}));
    assert_eq!(
        kept_document["nested"],
        serde_json::json!({"x": [{"$numberInt": "1"}, "a"]})
    );
    let sparse_document = document(sparse);
    assert!(sparse_document["i"].is_null());
    assert!(sparse_document.get("l").is_none());

    // History lands in an append-only PostgreSQL table with every event, including the delete.
    let history_query = format!(
        "SELECT event_id, operation FROM {history_table} \
         ORDER BY cluster_time_seconds, cluster_time_increment"
    );
    let history = eventually(
        CONVERGE,
        || async {
            pg.query(&history_query, &[])
                .await
                .map(|rows| {
                    rows.iter()
                        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        },
        |rows| rows.len() >= 6,
    )
    .await;
    let operations: Vec<&str> = history
        .iter()
        .map(|(_, operation)| operation.as_str())
        .collect();
    assert_eq!(
        operations,
        ["insert", "insert", "insert", "insert", "update", "delete"]
    );
    let identities: std::collections::BTreeSet<&str> =
        history.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(identities.len(), 6);
    db.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_snapshot_of_an_empty_collection_streams_later_writes() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("empty");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "accounts").await;
    let accounts = source_db.collection::<Document>("accounts");
    let storage = tempfile::tempdir().unwrap();
    let pg_table = unique("empty");
    let statements = snapshot_statements(&database, &pg_table);
    let pg = postgres().await;

    let db = open(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start()
        .await
        .expect("an empty collection still has a snapshot time");
    let documents: Vec<Document> = (0..3_i64)
        .map(|age| doc! {"_id": ObjectId::new(), "name": format!("late{age}"), "age": age})
        .collect();
    accounts.insert_many(documents.clone()).await.unwrap();
    let expected = expected_rows(&documents);
    let observed = eventually(
        CONVERGE,
        || pg_rows(&pg, &pg_table),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected);
    db.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_snapshot_with_manual_checkpoints_copies_after_the_first_checkpoint() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("manual");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "accounts").await;
    let accounts = source_db.collection::<Document>("accounts");
    let documents: Vec<Document> = (0..50_i64)
        .map(|age| doc! {"_id": ObjectId::new(), "name": format!("seed{age}"), "age": age})
        .collect();
    accounts.insert_many(documents.clone()).await.unwrap();
    let storage = tempfile::tempdir().unwrap();
    let pg_table = unique("manual");
    let pg = postgres().await;

    let db = LaminarDB::builder()
        .storage_dir(storage.path())
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .config_var("E2E_PG_PASSWORD", "laminar-test-secret")
        .build()
        .await
        .expect("open database");
    execute_all(&db, &snapshot_statements(&database, &pg_table)).await;
    db.start().await.expect("start");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        pg_rows(&pg, &pg_table).await.is_empty(),
        "the copy must wait until a checkpoint commits its cut"
    );
    assert!(db.checkpoint().await.unwrap().success);
    let expected = expected_rows(&documents);
    let observed = eventually(
        CONVERGE,
        || pg_rows(&pg, &pg_table),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected);
    db.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_resume_continues_across_id_types_in_index_order() {
    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::ConnectorConfig;
    use laminar_connectors::connector::{SourceConnector, SourcePosition, SourceStart};

    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("mixed");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "things").await;
    // `_id` index order: numbers by value across types, then strings, documents, binary,
    // ObjectIds, booleans, and dates.
    let ids = vec![
        Bson::Int32(1),
        Bson::Double(2.5),
        Bson::Int32(5),
        Bson::Int64(7),
        Bson::Int32(10),
        Bson::String("a".into()),
        Bson::String("b".into()),
        Bson::Document(doc! {"k": 1_i32}),
        Bson::Binary(mongodb::bson::Binary {
            subtype: mongodb::bson::spec::BinarySubtype::Generic,
            bytes: vec![9],
        }),
        Bson::ObjectId(ObjectId::new()),
        Bson::Boolean(true),
        Bson::DateTime(mongodb::bson::DateTime::from_millis(0)),
    ];
    source_db
        .collection::<Document>("things")
        .insert_many(ids.iter().map(|id| doc! {"_id": id.clone()}))
        .await
        .unwrap();
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", MONGO_URI);
    config.set("database", &database);
    config.set("collection", "things");
    config.set("snapshot.mode", "initial");
    let new_source = || {
        laminar_connectors::mongodb::MongoDbCdcSource::new(
            laminar_connectors::mongodb::MongoDbSourceConfig::default(),
            None,
        )
    };

    // A fresh start chooses the cut; its copy waits for a commit this test never sends.
    let mut source = new_source();
    source
        .start(
            SourceStart::new(
                config.clone(),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let captured = source.checkpoint();
    source.close().await.unwrap();
    assert!(captured.get_offset("snapshot_at").is_some(), "{captured:?}");

    // Resume the same cut after an Int32 key: every later key of every type must be copied.
    let mut resumed = SourceCheckpoint::new();
    for (key, value) in captured.metadata() {
        resumed.set_metadata(key.clone(), value.clone());
    }
    for (key, value) in captured.offsets() {
        resumed.set_offset(key.clone(), value.clone());
    }
    resumed.set_offset("snapshot_after_key", r#"{"$numberInt":"5"}"#);
    let mut source = new_source();
    source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(1),
                    checkpoint: resumed,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let expected: Vec<String> = ids[3..]
        .iter()
        .map(|id| {
            Bson::Document(doc! {"_id": id.clone()})
                .into_canonical_extjson()
                .to_string()
        })
        .collect();
    let mut copied = Vec::new();
    // Keep polling briefly after the expected rows to catch any key copied twice or out of order.
    let mut settle_until = None;
    let deadline = tokio::time::Instant::now() + CONVERGE;
    while tokio::time::Instant::now() < settle_until.unwrap_or(deadline) {
        if copied.len() >= expected.len() && settle_until.is_none() {
            settle_until = Some(tokio::time::Instant::now() + Duration::from_secs(1));
        }
        let Some(batch) = source.poll_batch(100).await.unwrap() else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let column = |name: &str| {
            batch
                .records
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .clone()
        };
        let (operations, keys) = (column("operation"), column("document_key"));
        for row in 0..batch.num_rows() {
            if operations.value(row) == "snapshot" {
                copied.push(keys.value(row).to_string());
            }
        }
    }
    source.close().await.unwrap();
    assert_eq!(copied, expected);
}

/// Measurement harness, not a pass/fail gate. Run one scenario per process so its peak memory
/// is its own, in release:
/// `cargo test --release -p laminar-db --test mongodb_cdc_e2e perf_ -- --ignored --nocapture --exact <name>`
mod perf {
    use super::*;
    use arrow::datatypes::SchemaRef;
    use laminar_connectors::config::{ConfigKeySpec, ConnectorConfig, ConnectorInfo};
    use laminar_connectors::connector::{
        SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
    };
    use laminar_connectors::error::ConnectorError;
    use std::sync::Arc;

    const RECORDING_SINK: &str = "perf-recording";

    fn now_us() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros(),
        )
        .unwrap()
    }

    /// `(p50, p95, p99, max)` of the samples.
    fn percentiles(mut samples: Vec<i64>) -> (i64, i64, i64, i64) {
        assert!(!samples.is_empty(), "no samples");
        samples.sort_unstable();
        let at = |quantile: f64| {
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let index = ((samples.len() - 1) as f64 * quantile).round() as usize;
            samples[index]
        };
        (at(0.50), at(0.95), at(0.99), samples[samples.len() - 1])
    }

    fn report(label: &str, unit: &str, samples: Vec<i64>) {
        let count = samples.len();
        let (p50, p95, p99, max) = percentiles(samples);
        println!("PERF {label}: n={count} p50={p50}{unit} p95={p95}{unit} p99={p99}{unit} max={max}{unit}");
    }

    /// Peak working set of this test process in MiB.
    fn peak_memory_mib() -> Option<u64> {
        #[cfg(windows)]
        {
            let output = std::process::Command::new("powershell")
                .args([
                    "-NoProfile",
                    "-Command",
                    &format!("(Get-Process -Id {}).PeakWorkingSet64", std::process::id()),
                ])
                .output()
                .ok()?;
            String::from_utf8(output.stdout)
                .ok()?
                .trim()
                .parse::<u64>()
                .ok()
                .map(|bytes| bytes >> 20)
        }
        #[cfg(not(windows))]
        {
            std::fs::read_to_string("/proc/self/status")
                .ok()?
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:"))
                .and_then(|value| {
                    value
                        .trim()
                        .trim_end_matches("kB")
                        .trim()
                        .parse::<u64>()
                        .ok()
                })
                .map(|kib| kib >> 10)
        }
    }

    #[derive(Clone, Default)]
    struct Recorded {
        rows: Arc<AtomicU64>,
        latencies_us: Arc<parking_lot::Mutex<Vec<i64>>>,
    }

    /// A durable-looking sink that records, per row, the delay since the writer stamped
    /// `sent_us`, then optionally sleeps to model a slow destination.
    struct RecordingSink {
        schema: SchemaRef,
        recorded: Recorded,
        delay: Duration,
        keyed: bool,
    }

    #[async_trait::async_trait]
    impl SinkConnector for RecordingSink {
        fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
            Ok(SinkContract::new(
                SinkConsistency::DurableAtLeastOnce,
                SinkTopology::Singleton,
                if self.keyed {
                    SinkInputMode::FullChangelog
                } else {
                    SinkInputMode::AppendOnly
                },
            ))
        }

        fn keyed_mutation_key(
            &self,
            _config: &ConnectorConfig,
        ) -> Result<Option<Vec<String>>, ConnectorError> {
            Ok(self.keyed.then(|| vec!["_id".to_string()]))
        }

        async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            batch: &arrow::array::RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            let received = now_us();
            let mut latencies = Vec::with_capacity(batch.num_rows());
            if let Some(column) = batch.column_by_name("sent_us") {
                let sent = column
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                latencies.extend(
                    (0..sent.len())
                        .filter(|row| !sent.is_null(*row))
                        .map(|row| received - sent.value(row)),
                );
            } else if let Some(column) = batch.column_by_name("full_document") {
                let documents = arrow::array::cast::as_string_array(column);
                for row in 0..documents.len() {
                    if documents.is_null(row) {
                        continue;
                    }
                    let document: serde_json::Value =
                        serde_json::from_str(documents.value(row)).unwrap();
                    if let Some(sent) = document["sent_us"]["$numberLong"].as_str() {
                        latencies.push(received - sent.parse::<i64>().unwrap());
                    }
                }
            }
            self.recorded.latencies_us.lock().extend(latencies);
            self.recorded
                .rows
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(WriteResult::new(batch.num_rows(), 0))
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(120)
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    async fn recording_db(
        storage: &Path,
        recorded: &Recorded,
        delay: Duration,
        keyed: bool,
    ) -> std::sync::Arc<LaminarDB> {
        let recorded = recorded.clone();
        LaminarDB::builder()
            .storage_dir(storage)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: Some(1000),
                ..Default::default()
            })
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .config_var("E2E_PG_PASSWORD", "laminar-test-secret")
            .register_connector(move |registry| {
                registry.register_sink(
                    RECORDING_SINK,
                    ConnectorInfo {
                        schema_capabilities:
                            laminar_connectors::schema::resolution::SchemaCapabilities::declared(
                                false,
                            ),
                        name: RECORDING_SINK.into(),
                        display_name: "Latency recording sink".into(),
                        version: "1".into(),
                        is_source: false,
                        is_sink: true,
                        config_keys: vec![ConfigKeySpec::optional("label", "Run label", "")],
                    },
                    Arc::new(
                        move |config: &ConnectorConfig, _: Option<&Arc<prometheus::Registry>>| {
                            let schema = config
                                .get("_arrow_schema")
                                .and_then(laminar_connectors::config::decode_arrow_schema_ipc)
                                .map_or_else(
                                    laminar_connectors::mongodb::mongodb_history_schema,
                                    Arc::new,
                                );
                            Ok(Box::new(RecordingSink {
                                schema,
                                recorded: recorded.clone(),
                                delay,
                                keyed,
                            }) as Box<dyn SinkConnector>)
                        },
                    ),
                )
            })
            .build()
            .await
            .expect("open database")
    }

    fn padded(id: i64, padding: &str) -> Document {
        doc! {"_id": id, "name": format!("user{id}"), "age": id, "pad": padding, "sent_us": now_us()}
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "performance measurement"]
    async fn perf_history_fetch_decode_and_batch() {
        use laminar_connectors::connector::{SourceConnector, SourcePosition, SourceStart};

        const DOCS: i64 = 100_000;
        let client = mongo(MONGO_URI).await.expect("MongoDB fixture");
        let database = unique("perfdecode");
        let source_db = client.database(&database);
        collection_with_images(&source_db, "events").await;
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", MONGO_URI);
        config.set("database", &database);
        config.set("collection", "events");
        config.set("max.buffered.bytes", "268435456");
        let mut source = laminar_connectors::mongodb::MongoDbCdcSource::new(
            laminar_connectors::mongodb::MongoDbSourceConfig::default(),
            None,
        );
        source
            .start(
                SourceStart::new(
                    config,
                    SourcePosition::Initial,
                    DeliveryGuarantee::AtLeastOnce,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let padding = "x".repeat(128);
        let events = source_db.collection::<Document>("events");
        for chunk in (0..DOCS).collect::<Vec<_>>().chunks(1000) {
            events
                .insert_many(chunk.iter().map(|id| padded(*id, &padding)))
                .await
                .unwrap();
        }
        // Reader prefetch is bounded, so polls measure fetch, decode and batch building together.
        tokio::time::sleep(Duration::from_secs(15)).await;

        let mut rows = 0_usize;
        let mut poll_us = Vec::new();
        let mut rows_per_poll = Vec::new();
        let started = std::time::Instant::now();
        while rows < usize::try_from(DOCS).unwrap() {
            let polled = std::time::Instant::now();
            let Some(batch) = source.poll_batch(1024).await.unwrap() else {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            };
            poll_us.push(i64::try_from(polled.elapsed().as_micros()).unwrap());
            rows_per_poll.push(i64::try_from(batch.num_rows()).unwrap());
            rows += batch.num_rows();
        }
        let elapsed = started.elapsed();
        source.close().await.unwrap();
        report(
            "history change-stream fetch+decode+batch per poll(1024)",
            "us",
            poll_us,
        );
        report("history rows per poll", "", rows_per_poll);
        println!(
            "PERF history change-stream fetch+decode+batch from a backlog: {rows} rows in {:.3}s = {:.0} rows/s",
            elapsed.as_secs_f64(),
            rows as f64 / elapsed.as_secs_f64()
        );
        println!("PERF peak memory: {:?} MiB", peak_memory_mib());
    }

    async fn source_to_receipt(keyed: bool) {
        const RATE_PER_SECOND: u64 = 1000;
        const SECONDS: u64 = 20;
        let client = mongo(MONGO_URI).await.expect("MongoDB fixture");
        let database = unique("perflatency");
        let source_db = client.database(&database);
        collection_with_images(&source_db, "events").await;
        let storage = tempfile::tempdir().unwrap();
        let recorded = Recorded::default();
        let db = recording_db(storage.path(), &recorded, Duration::ZERO, keyed).await;
        let source = if keyed {
            format!(
                "CREATE SOURCE events (_id BIGINT NOT NULL, name VARCHAR, age BIGINT, \
                 sent_us BIGINT, PRIMARY KEY (_id)) FROM \"mongodb-cdc\" (\
                 'connection.uri' = '{MONGO_URI}', 'database' = '{database}', \
                 'collection' = 'events', 'output.mode' = 'document', \
                 'full.document.mode' = 'required')"
            )
        } else {
            history_source("events", MONGO_URI, &database, "events", "")
        };
        execute_all(
            &db,
            &[
                source,
                format!(
                    "CREATE SINK recorded FROM events INTO \"{RECORDING_SINK}\" ('label' = 'perf')"
                ),
            ],
        )
        .await;
        db.start().await.expect("start");

        let events = source_db.collection::<Document>("events");
        let padding = "x".repeat(128);
        let total = RATE_PER_SECOND * SECONDS;
        let interval = Duration::from_micros(1_000_000 / RATE_PER_SECOND);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        let started = std::time::Instant::now();
        for id in 0..i64::try_from(total).unwrap() {
            ticker.tick().await;
            events.insert_one(padded(id, &padding)).await.unwrap();
        }
        let offered = started.elapsed();
        eventually(
            CONVERGE,
            || async { recorded.rows.load(Ordering::Relaxed) },
            |rows| *rows >= total,
        )
        .await;
        db.shutdown().await.expect("shutdown");
        let mode = if keyed { "document" } else { "history" };
        println!(
            "PERF {mode} offered {total} single-document inserts in {:.2}s ({:.0}/s)",
            offered.as_secs_f64(),
            total as f64 / offered.as_secs_f64()
        );
        let latencies = std::mem::take(&mut *recorded.latencies_us.lock());
        report(&format!("{mode} insert-to-sink-receipt"), "us", latencies);
        println!("PERF peak memory: {:?} MiB", peak_memory_mib());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "performance measurement"]
    async fn perf_history_source_to_receipt() {
        source_to_receipt(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "performance measurement"]
    async fn perf_document_source_to_receipt() {
        source_to_receipt(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "performance measurement"]
    async fn perf_postgres_document_mirror() {
        const PROBES: usize = 200;
        const BULK: i64 = 50_000;
        let client = mongo(MONGO_URI).await.expect("MongoDB fixture");
        let database = unique("perfpg");
        let source_db = client.database(&database);
        collection_with_images(&source_db, "users").await;
        let users = source_db.collection::<Document>("users");
        let storage = tempfile::tempdir().unwrap();
        let pg_table = unique("perfpg");
        let pg = postgres().await;
        let db = open(storage.path()).await;
        execute_all(
            &db,
            &[
                document_source("docs", MONGO_URI, &database, "users", ""),
                pg_sink_on("docs", &pg_table, ", 'changelog.mode' = 'true'"),
            ],
        )
        .await;
        db.start().await.expect("start");

        // Sequential probes: insert one document, then wait until PostgreSQL shows it.
        let mut visible_us = Vec::with_capacity(PROBES);
        let probe = format!("SELECT 1 FROM {pg_table} WHERE \"_id\" = $1");
        for _ in 0..PROBES {
            let id = ObjectId::new();
            let started = std::time::Instant::now();
            users
                .insert_one(doc! {"_id": id, "name": "probe", "age": 1_i64})
                .await
                .unwrap();
            let hex = id.to_hex();
            loop {
                if pg.query_opt(&probe, &[&hex]).await.ok().flatten().is_some() {
                    break;
                }
                assert!(started.elapsed() < CONVERGE, "probe never became visible");
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            visible_us.push(i64::try_from(started.elapsed().as_micros()).unwrap());
        }
        report("postgres insert-to-visible (sequential)", "us", visible_us);

        let padding = "x".repeat(128);
        let started = std::time::Instant::now();
        for chunk in (0..BULK).collect::<Vec<_>>().chunks(1000) {
            users
                .insert_many(chunk.iter().map(|age| {
                    doc! {"_id": ObjectId::new(), "name": format!("bulk{age}"), "age": age, "pad": &padding}
                }))
                .await
                .unwrap();
        }
        let inserted = started.elapsed();
        let expected = i64::try_from(PROBES).unwrap() + BULK;
        let count = format!("SELECT count(*) FROM {pg_table}");
        let mirrored = eventually(
            Duration::from_secs(600),
            || async { pg.query_one(&count, &[]).await.unwrap().get::<_, i64>(0) },
            |rows| *rows >= expected,
        )
        .await;
        let elapsed = started.elapsed();
        db.shutdown().await.expect("shutdown");
        assert_eq!(mirrored, expected);
        println!(
            "PERF postgres bulk: inserted {BULK} in {:.2}s; mirrored in {:.2}s = {:.0} rows/s end to end",
            inserted.as_secs_f64(),
            elapsed.as_secs_f64(),
            BULK as f64 / elapsed.as_secs_f64()
        );
        println!("PERF peak memory: {:?} MiB", peak_memory_mib());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "performance measurement"]
    async fn perf_slow_sink_backlog_stays_bounded() {
        const DOCS: i64 = 100_000;
        let client = mongo(MONGO_URI).await.expect("MongoDB fixture");
        let database = unique("perfslow");
        let source_db = client.database(&database);
        collection_with_images(&source_db, "events").await;
        let storage = tempfile::tempdir().unwrap();
        let recorded = Recorded::default();
        let db = recording_db(storage.path(), &recorded, Duration::from_millis(200), false).await;
        execute_all(
            &db,
            &[
                history_source(
                    "events",
                    MONGO_URI,
                    &database,
                    "events",
                    ", 'max.buffered.bytes' = '4194304'",
                ),
                format!(
                    "CREATE SINK recorded FROM events INTO \"{RECORDING_SINK}\" ('label' = 'perf')"
                ),
            ],
        )
        .await;
        db.start().await.expect("start");
        let padding = "x".repeat(1024);
        let events = source_db.collection::<Document>("events");
        let started = std::time::Instant::now();
        for chunk in (0..DOCS).collect::<Vec<_>>().chunks(1000) {
            events
                .insert_many(chunk.iter().map(|id| padded(*id, &padding)))
                .await
                .unwrap();
        }
        let inserted = started.elapsed();
        let delivered_at_insert_end = recorded.rows.load(Ordering::Relaxed);
        let total = u64::try_from(DOCS).unwrap();
        eventually(
            Duration::from_secs(1200),
            || async { recorded.rows.load(Ordering::Relaxed) },
            |rows| *rows >= total,
        )
        .await;
        let drained = started.elapsed();
        db.shutdown().await.expect("shutdown");
        println!(
            "PERF slow sink (200 ms per write): {DOCS} x ~1.2 KiB inserted in {:.1}s, {} delivered \
             by then, all delivered after {:.1}s",
            inserted.as_secs_f64(),
            delivered_at_insert_end,
            drained.as_secs_f64()
        );
        report(
            "slow sink insert-to-receipt",
            "us",
            std::mem::take(&mut *recorded.latencies_us.lock()),
        );
        println!("PERF peak memory: {:?} MiB", peak_memory_mib());
    }
}

/// Arm a one-shot `failCommand` failpoint that answers `command` from the client named `app`
/// with `ChangeStreamHistoryLost`, the error a server returns once the resume point has left
/// the oplog.
async fn fail_with_history_lost(client: &mongodb::Client, command: &str, app: &str) {
    client
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": {"times": 1},
            "data": {"failCommands": [command], "errorCode": 286, "appName": app},
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_oplog_history_stops_the_source_instead_of_restarting_from_now() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let database = unique("oplog");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "users").await;
    let users = source_db.collection::<Document>("users");
    // The failpoint targets only this pipeline's MongoDB client.
    let app = unique("oplog");
    let uri = format!("{MONGO_URI}&appName={app}");
    let storage = tempfile::tempdir().unwrap();
    let pg_table = unique("oplog");
    let pg = postgres().await;
    let statements = vec![
        document_source("docs", &uri, &database, "users", ""),
        pg_sink_on("docs", &pg_table, ", 'changelog.mode' = 'true'"),
    ];

    let db = open(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    let (before, after) = (ObjectId::new(), ObjectId::new());
    users
        .insert_one(doc! {"_id": before, "name": "before", "age": 1_i64})
        .await
        .unwrap();
    eventually(
        CONVERGE,
        || pg_rows(&pg, &pg_table),
        |rows| rows.contains_key(&before.to_hex()),
    )
    .await;
    assert!(db.checkpoint().await.unwrap().success);

    // History lost mid-stream: the source stops with the recovery action instead of reopening.
    // The next awaited getMore fails; a later write must never reach the target.
    fail_with_history_lost(&client, "getMore", &app).await;
    expect_pipeline_fault(&db, "no longer in the oplog").await;
    users
        .insert_one(doc! {"_id": after, "name": "after", "age": 2_i64})
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !pg_rows(&pg, &pg_table).await.contains_key(&after.to_hex()),
        "a stopped source must not keep delivering changes"
    );
    let _ = db.shutdown().await;

    // History lost at resume: the restart fails rather than opening a fresh stream at "now".
    let db = reopen(storage.path(), &statements).await;
    fail_with_history_lost(&client, "aggregate", &app).await;
    let error = db
        .start()
        .await
        .expect_err("a lost resume point must not restart from now")
        .to_string();
    assert!(error.contains("no longer in the oplog"), "{error}");
    let _ = db.shutdown().await;
}

#[cfg(feature = "iceberg")]
fn iceberg_options(table: &str, secret: &str) -> Vec<(&'static str, String)> {
    [
        ("catalog.uri", "http://localhost:8181"),
        ("warehouse", "s3://warehouse/wh"),
        ("storage.type", "s3"),
        ("namespace", "laminar_mongodb_cdc"),
        ("auto.create", "true"),
        ("storage.endpoint", "http://localhost:9000"),
        ("storage.region", "us-east-1"),
        ("storage.path_style", "true"),
        ("storage.property.s3.access-key-id", "minioadmin"),
    ]
    .into_iter()
    .map(|(key, value)| (key, value.to_string()))
    .chain([
        ("table.name", table.to_string()),
        ("storage.property.s3.secret-access-key", secret.to_string()),
    ])
    .collect()
}

/// `(cluster time, operation, event_id)` of every row in an Iceberg history table.
#[cfg(feature = "iceberg")]
async fn iceberg_history(table: &str) -> Vec<((i64, i64), String, String)> {
    use futures_util::StreamExt;
    use laminar_connectors::lakehouse::{iceberg_config::IcebergSinkConfig, iceberg_io};

    let mut config = laminar_connectors::config::ConnectorConfig::new("iceberg");
    for (key, value) in iceberg_options(table, "minioadmin") {
        config.set(key, value);
    }
    let config = IcebergSinkConfig::from_config(&config).unwrap();
    let Ok(catalog) = iceberg_io::build_catalog(&config.catalog, &config.storage).await else {
        return Vec::new();
    };
    let Ok(table) = iceberg_io::load_table(
        catalog.as_ref(),
        &config.catalog.namespace,
        &config.catalog.table_name,
    )
    .await
    else {
        return Vec::new();
    };
    let Ok(mut stream) = table.scan().build().unwrap().to_arrow().await else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let text = |name: &str| {
            arrow::compute::cast(
                batch.column_by_name(name).unwrap(),
                &arrow::datatypes::DataType::Utf8,
            )
            .unwrap()
        };
        let (seconds, increments, operations, ids) = (
            text("cluster_time_seconds"),
            text("cluster_time_increment"),
            text("operation"),
            text("event_id"),
        );
        let string = |column: &arrow::array::ArrayRef, row: usize| {
            arrow::array::cast::as_string_array(column)
                .value(row)
                .to_string()
        };
        for row in 0..batch.num_rows() {
            rows.push((
                (
                    string(&seconds, row).parse().unwrap(),
                    string(&increments, row).parse().unwrap(),
                ),
                string(&operations, row),
                string(&ids, row),
            ));
        }
        assert!(
            matches!(
                batch.column_by_name("wall_time").unwrap().data_type(),
                arrow::datatypes::DataType::Timestamp(_, _)
            ),
            "wall_time keeps a timestamp type in Iceberg"
        );
    }
    rows.sort();
    rows
}

#[cfg(feature = "iceberg")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_appends_every_event_to_iceberg() {
    let Some(client) = mongo(MONGO_URI).await else {
        return;
    };
    let catalog = std::net::SocketAddr::from(([127, 0, 0, 1], 8181));
    if std::net::TcpStream::connect_timeout(&catalog, Duration::from_secs(2)).is_err() {
        assert!(
            std::env::var(REQUIRE_ENV).is_err(),
            "the Iceberg fixture (tests/docker/iceberg-compose.yml) is required"
        );
        eprintln!("skipping: Iceberg REST catalog is not reachable on {catalog}");
        return;
    }
    let database = unique("icehist");
    let source_db = client.database(&database);
    collection_with_images(&source_db, "events").await;
    let events = source_db.collection::<Document>("events");
    let storage = tempfile::tempdir().unwrap();
    let table = unique("history");
    let options = iceberg_options(&table, "${E2E_MINIO_SECRET}")
        .into_iter()
        .map(|(key, value)| format!("'{key}' = '{value}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let statements = vec![
        history_source("changes", MONGO_URI, &database, "events", ""),
        format!("CREATE SINK changes_lake FROM changes INTO \"iceberg\" ({options})"),
    ];

    let db = open(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    let id = ObjectId::new();
    events
        .insert_one(doc! {"_id": id, "v": 1_i32})
        .await
        .unwrap();
    events
        .update_one(doc! {"_id": id}, doc! {"$set": {"v": 2_i32}})
        .await
        .unwrap();
    events
        .update_one(doc! {"_id": id}, doc! {"$set": {"v": 3_i32}})
        .await
        .unwrap();
    events
        .replace_one(doc! {"_id": id}, doc! {"v": 4_i32})
        .await
        .unwrap();
    events.delete_one(doc! {"_id": id}).await.unwrap();

    let rows = eventually(
        CONVERGE,
        || async {
            let _ = db.checkpoint().await;
            iceberg_history(&table).await
        },
        |rows| rows.len() >= 5,
    )
    .await;
    db.shutdown().await.expect("shutdown");
    let operations: Vec<&str> = rows
        .iter()
        .map(|(_, operation, _)| operation.as_str())
        .collect();
    assert_eq!(
        operations,
        ["insert", "update", "update", "replace", "delete"]
    );
    let identities: std::collections::BTreeSet<&str> =
        rows.iter().map(|(_, _, id)| id.as_str()).collect();
    assert_eq!(identities.len(), 5);
}
