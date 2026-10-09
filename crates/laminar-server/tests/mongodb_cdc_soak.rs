//! Standalone-server MongoDB CDC soak: sustained mixed writes, repeated hard kills, exact final
//! mirrors in PostgreSQL and MongoDB.
//!
//! Needs `docker compose -f tests/docker/mongodb-cdc-compose.yml up -d --wait`. Run with
//! `cargo test --release -p laminar-server --test mongodb_cdc_soak -- --ignored --nocapture`;
//! `LAMINAR_MONGODB_SOAK_SECONDS` (default 120) and `LAMINAR_MONGODB_SOAK_KILL_EVERY_SECONDS`
//! (default 20) size the run.

#![cfg(all(feature = "mongodb-cdc", feature = "postgres-sink"))]
#![allow(clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use mongodb::bson::{doc, oid::ObjectId, Bson, Document};

const MONGO_URI: &str = "mongodb://127.0.0.1:27117/?directConnection=true&tls=false";
const PG_CONN: &str =
    "host=127.0.0.1 port=15433 user=laminar password=laminar-test-secret dbname=mirror";
const KEYS: usize = 2_000;

fn seconds_from_env(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default),
    )
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn server_config(root: &Path, database: &str, pg_table: &str, mirror_db: &str) -> String {
    let checkpoint = path_string(&root.join("checkpoints"));
    let checkpoint_url = if checkpoint.starts_with('/') {
        format!("file://{checkpoint}")
    } else {
        format!("file:///{checkpoint}")
    };
    let sql = format!(
        "CREATE SOURCE accounts (_id VARCHAR NOT NULL, name VARCHAR, age BIGINT, doc VARCHAR, \
         PRIMARY KEY (_id)) FROM \"mongodb-cdc\" ('connection.uri' = '{MONGO_URI}', \
         'database' = '{database}', 'collection' = 'accounts', 'output.mode' = 'document', \
         'full.document.mode' = 'required', 'objectid.columns' = '_id', \
         'document.json.column' = 'doc', 'snapshot.mode' = 'initial');
         CREATE SOURCE account_history FROM \"mongodb-cdc\" ('connection.uri' = '{MONGO_URI}', \
         'database' = '{database}', 'collection' = 'accounts', 'full.document.mode' = 'required',          'snapshot.mode' = 'initial');
         CREATE SINK accounts_pg FROM accounts INTO \"postgres-sink\" ('hostname' = '127.0.0.1', \
         'port' = '15433', 'database' = 'mirror', 'username' = 'laminar', \
         'password' = '$${{SOAK_PG_PASSWORD}}', 'ssl.mode' = 'disable', 'table.name' = '{pg_table}', \
         'write.mode' = 'upsert', 'primary.key' = '_id', 'changelog.mode' = 'true', \
         'auto.create.table' = 'true');
         CREATE SINK accounts_mongo FROM account_history INTO \"mongodb-sink\" (\
         'connection.uri' = '{MONGO_URI}', 'database' = '{mirror_db}', 'collection' = 'accounts', \
         'write.mode' = 'cdc_replay', 'replay.source.namespace' = '{database}.accounts', \
         'auto.create' = 'true');"
    );
    // TOML basic strings accept JSON string escapes; `sql` precedes the first table header.
    format!(
        "sql = {sql}\n[server]\nbind = {bind}\ndelivery = \"at_least_once\"\n\
         [checkpoint]\nurl = {checkpoint_url}\ninterval = \"500ms\"\n",
        sql = serde_json::to_string(&sql).unwrap(),
        bind = serde_json::to_string(&format!("127.0.0.1:{}", free_port())).unwrap(),
        checkpoint_url = serde_json::to_string(&checkpoint_url).unwrap(),
    )
}

fn spawn_server(config: &Path, log: &Path) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    Command::new(env!("CARGO_BIN_EXE_laminardb"))
        .arg("--config")
        .arg(config)
        .args(["--log-level", "warn"])
        .env("SOAK_PG_PASSWORD", "laminar-test-secret")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("start laminardb")
}

/// Resident memory of `pid` in MiB.
fn resident_mib(pid: u32) -> Option<u64> {
    #[cfg(windows)]
    {
        let output = Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                &format!("(Get-Process -Id {pid}).WorkingSet64"),
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
        std::fs::read_to_string(format!("/proc/{pid}/status"))
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
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

/// Deterministic xorshift choices, so a failing run replays the same operation mix.
struct Choices(u64);

impl Choices {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        usize::try_from(self.0 % bound as u64).unwrap()
    }
}

/// Inserts, updates, replaces, deletes, and small multi-document transactions over a fixed
/// key space until `deadline`. Returns the number of acknowledged operations.
async fn write_load(client: mongodb::Client, database: String, deadline: Instant) -> u64 {
    let accounts = client
        .database(&database)
        .collection::<Document>("accounts");
    let keys: Vec<ObjectId> = (0..KEYS).map(|_| ObjectId::new()).collect();
    let mut present = vec![false; KEYS];
    let mut random = Choices(0x5eed_5eed);
    let mut operations = 0_u64;
    while Instant::now() < deadline {
        if operations % 50 == 49 {
            let mut session = client.start_session().await.unwrap();
            session.start_transaction().await.unwrap();
            for _ in 0..3 {
                let index = random.below(KEYS);
                if present[index] {
                    accounts
                        .update_one(doc! {"_id": keys[index]}, doc! {"$inc": {"age": 1_i64}})
                        .session(&mut session)
                        .await
                        .unwrap();
                }
            }
            session.commit_transaction().await.unwrap();
            operations += 1;
            continue;
        }
        let index = random.below(KEYS);
        let id = keys[index];
        let version = i64::try_from(operations).unwrap();
        if !present[index] {
            accounts
                .insert_one(doc! {"_id": id, "name": format!("n{version}"), "age": version, "tags": ["a", 1_i32]})
                .await
                .unwrap();
            present[index] = true;
        } else {
            match random.below(10) {
                0..=5 => {
                    accounts
                        .update_one(
                            doc! {"_id": id},
                            doc! {"$set": {"name": format!("u{version}")}, "$inc": {"age": 1_i64}},
                        )
                        .await
                        .unwrap();
                }
                6..=7 => {
                    accounts
                        .replace_one(
                            doc! {"_id": id},
                            doc! {"name": format!("r{version}"), "age": -version},
                        )
                        .await
                        .unwrap();
                }
                _ => {
                    accounts.delete_one(doc! {"_id": id}).await.unwrap();
                    present[index] = false;
                }
            }
        }
        operations += 1;
        if operations.is_multiple_of(200) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    operations
}

fn canonical(document: Document) -> (String, String) {
    let mut document = document;
    let id = match document.remove("_id").expect("_id") {
        Bson::ObjectId(id) => id.to_hex(),
        other => other.into_canonical_extjson().to_string(),
    };
    (
        id,
        Bson::Document(document)
            .into_canonical_extjson()
            .to_string(),
    )
}

async fn mongo_rows(collection: &mongodb::Collection<Document>) -> BTreeMap<String, String> {
    let documents: Vec<Document> = collection
        .find(doc! {})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    documents.into_iter().map(canonical).collect()
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

fn expected_pg(rows: &BTreeMap<String, String>) -> BTreeMap<String, (Option<String>, Option<i64>)> {
    rows.iter()
        .map(|(id, document)| {
            let value: serde_json::Value = serde_json::from_str(document).unwrap();
            let age = value["age"]["$numberLong"]
                .as_str()
                .map(|age| age.parse::<i64>().unwrap());
            (
                id.clone(),
                (value["name"].as_str().map(str::to_string), age),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: needs the MongoDB CDC fixture and minutes of wall time"]
#[allow(clippy::too_many_lines)]
async fn mongodb_cdc_survives_repeated_hard_kills_under_sustained_writes() {
    let run_for = seconds_from_env("LAMINAR_MONGODB_SOAK_SECONDS", 120);
    let kill_every = seconds_from_env("LAMINAR_MONGODB_SOAK_KILL_EVERY_SECONDS", 20);
    let client = mongodb::Client::with_uri_str(MONGO_URI).await.unwrap();
    let suffix = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let database = format!("soak_{suffix}");
    let mirror_db = format!("soakmirror_{suffix}");
    let pg_table = format!("soak_{suffix}");
    client
        .database(&database)
        .run_command(doc! {"create": "accounts", "changeStreamPreAndPostImages": {"enabled": true}})
        .await
        .unwrap();
    // Existing documents exercise the initial snapshot under concurrent writes.
    let accounts = client
        .database(&database)
        .collection::<Document>("accounts");
    accounts
        .insert_many(
            (0..5_000_i64).map(|age| doc! {"_id": ObjectId::new(), "name": "seed", "age": age}),
        )
        .await
        .unwrap();

    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("server.toml");
    std::fs::write(
        &config,
        server_config(root.path(), &database, &pg_table, &mirror_db),
    )
    .unwrap();
    let log = root.path().join("server.log");
    let started = Instant::now();
    let writer = tokio::spawn(write_load(
        client.clone(),
        database.clone(),
        started + run_for,
    ));

    let mut server = spawn_server(&config, &log);
    let mut kills = 0_u32;
    let mut peak_mib = 0_u64;
    let mut next_kill = started + kill_every;
    while Instant::now() < started + run_for {
        if let Some(status) = server.try_wait().unwrap() {
            panic!(
                "server exited on its own with {status}:\n{}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
        }
        if let Some(mib) = resident_mib(server.id()) {
            peak_mib = peak_mib.max(mib);
        }
        if Instant::now() >= next_kill {
            server.kill().unwrap();
            server.wait().unwrap();
            kills += 1;
            server = spawn_server(&config, &log);
            next_kill = Instant::now() + kill_every;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let operations = writer.await.unwrap();

    let (pg, connection) = tokio_postgres::connect(PG_CONN, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    let mirror = client
        .database(&mirror_db)
        .collection::<Document>("accounts");
    let source = mongo_rows(&accounts).await;
    let expected = expected_pg(&source);
    let converge_started = Instant::now();
    let deadline = converge_started + Duration::from_secs(300);
    let (mut observed_pg, mut observed_mongo) = (BTreeMap::new(), BTreeMap::new());
    while Instant::now() < deadline {
        observed_pg = pg_rows(&pg, &pg_table).await;
        observed_mongo = mongo_rows(&mirror).await;
        if observed_pg == expected && observed_mongo == source {
            break;
        }
        if let Some(mib) = resident_mib(server.id()) {
            peak_mib = peak_mib.max(mib);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let converged = converge_started.elapsed();
    server.kill().unwrap();
    server.wait().unwrap();
    let mismatched = |label: &str, missing: usize, stale: usize, different: usize| {
        println!("SOAK {label}: missing={missing} stale={stale} different={different}");
    };
    mismatched(
        "postgres",
        expected
            .keys()
            .filter(|key| !observed_pg.contains_key(*key))
            .count(),
        observed_pg
            .keys()
            .filter(|key| !expected.contains_key(*key))
            .count(),
        expected
            .iter()
            .filter(|(key, value)| observed_pg.get(*key).is_some_and(|row| row != *value))
            .count(),
    );
    mismatched(
        "mongodb",
        source
            .keys()
            .filter(|key| !observed_mongo.contains_key(*key))
            .count(),
        observed_mongo
            .keys()
            .filter(|key| !source.contains_key(*key))
            .count(),
        source
            .iter()
            .filter(|(key, value)| observed_mongo.get(*key).is_some_and(|row| row != *value))
            .count(),
    );
    println!(
        "SOAK ran {:.0}s: {operations} acknowledged write operations, {kills} hard kills, \
         {} final documents, converged {:.1}s after writes stopped, peak server memory {peak_mib} MiB",
        run_for.as_secs_f64(),
        source.len(),
        converged.as_secs_f64()
    );
    assert!(kills > 0, "the run must include hard kills");
    assert_eq!(observed_pg, expected, "PostgreSQL mirror");
    assert_eq!(observed_mongo, source, "MongoDB mirror");
}
