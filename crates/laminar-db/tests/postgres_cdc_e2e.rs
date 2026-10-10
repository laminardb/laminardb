//! PostgreSQL CDC through the engine: DDL, snapshot bootstrap, checkpoints, restarts, and SQL.
//!
//! Needs `docker compose -f tests/docker/postgres-cdc-compose.yml up -d --wait`. The source and
//! sink tables share one server, selected by `LAMINAR_TEST_POSTGRES_PORT` (default 15532).
//! Tests skip when PostgreSQL is unreachable unless `LAMINAR_REQUIRE_POSTGRES_CDC=1`.

#![cfg(all(feature = "postgres-cdc", feature = "postgres-sink"))]
#![allow(clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use laminar_db::{DeliveryGuarantee, LaminarDB};

const REQUIRE_ENV: &str = "LAMINAR_REQUIRE_POSTGRES_CDC";
const PORT_ENV: &str = "LAMINAR_TEST_POSTGRES_PORT";
const PASSWORD: &str = "laminar-test-secret";
const CONVERGE: Duration = Duration::from_secs(60);

static NEXT: AtomicU64 = AtomicU64::new(0);

fn port() -> u16 {
    std::env::var(PORT_ENV).ok().map_or(15532, |port| {
        port.parse().expect("LAMINAR_TEST_POSTGRES_PORT")
    })
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{prefix}_{:x}_{}",
        nanos % 0xffff_ffff,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn postgres() -> Option<tokio_postgres::Client> {
    let connection = format!(
        "host=127.0.0.1 port={} user=laminar password={PASSWORD} dbname=cdc",
        port()
    );
    match tokio_postgres::connect(&connection, tokio_postgres::NoTls).await {
        Ok((client, driver)) => {
            tokio::spawn(driver);
            Some(client)
        }
        Err(error) if std::env::var(REQUIRE_ENV).is_ok_and(|value| value == "1") => {
            panic!("PostgreSQL CDC fixture is required by {REQUIRE_ENV}: {error}")
        }
        Err(error) => {
            eprintln!("skipping: PostgreSQL CDC fixture unreachable: {error}");
            None
        }
    }
}

async fn open(storage: &Path) -> std::sync::Arc<LaminarDB> {
    open_with(storage, Some(300)).await
}

/// A database checkpointing every `interval_ms`, or only on `checkpoint()` when `None`.
async fn open_with(storage: &Path, interval_ms: Option<u64>) -> std::sync::Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .config_var("E2E_PG_PASSWORD", PASSWORD)
        .build()
        .await
        .expect("open database")
}

/// Open a database and register `statements`, waiting for the previous process generation to
/// release the checkpoint namespace lock after shutdown.
async fn reopen(storage: &Path, statements: &[String]) -> std::sync::Arc<LaminarDB> {
    reopen_with(storage, statements, Some(300)).await
}

async fn reopen_with(
    storage: &Path,
    statements: &[String],
    interval_ms: Option<u64>,
) -> std::sync::Arc<LaminarDB> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let db = open_with(storage, interval_ms).await;
        match first_error(&db, statements).await {
            None => return db,
            Some(error) if error.contains("LDB-0014") && tokio::time::Instant::now() < deadline => {
                drop(db);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Some(error) => panic!("{error}"),
        }
    }
}

async fn first_error(db: &LaminarDB, statements: &[String]) -> Option<String> {
    for statement in statements {
        if let Err(error) = db.execute(statement).await {
            return Some(format!("{statement}: {error}"));
        }
    }
    None
}

async fn execute_all(db: &LaminarDB, statements: &[String]) {
    if let Some(error) = first_error(db, statements).await {
        panic!("{error}");
    }
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

type Rows = BTreeMap<i64, (Option<String>, Option<i32>)>;

async fn rows(client: &tokio_postgres::Client, query: &str) -> Rows {
    client
        .query(query, &[])
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.get(0), (row.get(1), row.get(2))))
                .collect()
        })
        .unwrap_or_default()
}

const PG_SINK: &str = "'hostname' = '127.0.0.1', 'database' = 'cdc', 'username' = 'laminar', \
     'password' = '${E2E_PG_PASSWORD}', 'ssl.mode' = 'disable'";

/// One captured table with its own slot prefix and publication, plus a mirror table name.
/// Dropping it ends sessions on and drops every slot under the prefix.
struct Capture {
    table: String,
    slot: String,
    publication: String,
    mirror: String,
}

impl Capture {
    async fn new(client: &tokio_postgres::Client) -> Self {
        let capture = Self {
            table: unique("orders"),
            slot: unique("slot"),
            publication: unique("pub"),
            mirror: unique("mirror"),
        };
        client
            .batch_execute(&format!(
                "CREATE TABLE {t} (id bigint PRIMARY KEY, label text NOT NULL, qty integer); \
                 ALTER TABLE {t} REPLICA IDENTITY FULL; \
                 CREATE PUBLICATION {p} FOR TABLE {t};",
                t = capture.table,
                p = capture.publication
            ))
            .await
            .unwrap();
        capture
    }

    fn source(&self, name: &str, columns: &str, options: &str) -> String {
        format!(
            "CREATE SOURCE {name} ({columns}) FROM \"postgres-cdc\" (\
             'host' = '127.0.0.1', 'port' = '{port}', 'database' = 'cdc', \
             'username' = 'laminar', 'password' = '${{E2E_PG_PASSWORD}}', 'ssl.mode' = 'disable', \
             'slot.name' = '{slot}', 'publication' = '{publication}', \
             'table' = 'public.{table}'{options})",
            port = port(),
            slot = self.slot,
            publication = self.publication,
            table = self.table,
        )
    }

    fn upsert_sink(&self, name: &str, input: &str, key: &str) -> String {
        format!(
            "CREATE SINK {name} FROM {input} INTO \"postgres-sink\" ({PG_SINK}, \
             'port' = '{port}', 'table.name' = '{mirror}', 'write.mode' = 'upsert', \
             'primary.key' = '{key}', 'changelog.mode' = 'true', 'auto.create.table' = 'true')",
            port = port(),
            mirror = self.mirror,
        )
    }

    fn source_rows(&self) -> String {
        format!("SELECT id, label, qty FROM {} ORDER BY id", self.table)
    }

    fn mirror_rows(&self) -> String {
        format!("SELECT id, label, qty FROM {} ORDER BY id", self.mirror)
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        let prefix = format!("{}_", self.slot);
        let _ = std::thread::spawn(move || drop_test_slots(&prefix)).join();
    }
}

/// Test cleanup only: end every session on, then drop, every slot whose name starts with
/// `prefix`.
fn drop_test_slots(prefix: &str) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    runtime.block_on(async {
        let Some(client) = postgres().await else {
            return;
        };
        let deadline = tokio::time::Instant::now() + CONVERGE;
        while tokio::time::Instant::now() < deadline {
            let _ = client
                .execute(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                     WHERE starts_with(slot_name::text, $1) AND active_pid IS NOT NULL",
                    &[&prefix],
                )
                .await;
            let _ = client
                .execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                     WHERE starts_with(slot_name::text, $1) AND NOT active",
                    &[&prefix],
                )
                .await;
            let remaining: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_replication_slots \
                     WHERE starts_with(slot_name::text, $1)",
                    &[&prefix],
                )
                .await
                .map_or(1, |row| row.get(0));
            if remaining == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
}

/// Slots the source created under its `slot.name` prefix.
async fn slots_under(client: &tokio_postgres::Client, prefix: &str) -> Vec<String> {
    client
        .query(
            "SELECT slot_name::text FROM pg_replication_slots \
             WHERE starts_with(slot_name::text, $1) ORDER BY 1",
            &[&format!("{prefix}_")],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

const ORDER_COLUMNS: &str = "id BIGINT NOT NULL, label VARCHAR, qty INT, PRIMARY KEY (id)";

async fn churn(client: &tokio_postgres::Client, table: &str, round: i64) {
    client
        .batch_execute(&format!(
            "UPDATE {table} SET qty = qty + 1, label = 'round{round}' WHERE id % 7 = {m}; \
             DELETE FROM {table} WHERE id % 11 = {m}; \
             INSERT INTO {table} SELECT 100000 * {r} + g, 'new', g FROM generate_series(1, 20) g; \
             UPDATE {table} SET id = id + 1000000 WHERE id % 13 = {m} AND id < 1000000;",
            m = round % 7,
            r = round + 1,
        ))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_and_changes_mirror_into_postgres_across_restart() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'seed', g FROM generate_series(1, 5000) g",
            capture.table
        ))
        .await
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let statements = vec![
        capture.source("orders", ORDER_COLUMNS, ""),
        capture.upsert_sink("orders_mirror", "orders", "id"),
    ];
    {
        let db = open(storage.path()).await;
        execute_all(&db, &statements).await;
        db.start().await.expect("start");
        // Concurrent writes during the snapshot commit after the slot's consistent point.
        for round in 0..5 {
            churn(&client, &capture.table, round).await;
        }
        let expected = rows(&client, &capture.source_rows()).await;
        let mirror_query = capture.mirror_rows();
        let observed = eventually(
            CONVERGE,
            || rows(&client, &mirror_query),
            |rows| rows == &expected,
        )
        .await;
        assert_eq!(observed, expected, "mirror after snapshot and live changes");
        assert!(db.checkpoint().await.unwrap().success);
        db.shutdown().await.expect("shutdown");
    }

    // Changes while the pipeline is down replay from the committed cursor.
    for round in 5..8 {
        churn(&client, &capture.table, round).await;
    }
    {
        let db = reopen(storage.path(), &statements).await;
        db.start().await.expect("restart");
        churn(&client, &capture.table, 8).await;
        let expected = rows(&client, &capture.source_rows()).await;
        let mirror_query = capture.mirror_rows();
        let observed = eventually(
            CONVERGE,
            || rows(&client, &mirror_query),
            |rows| rows == &expected,
        )
        .await;
        assert_eq!(observed, expected, "mirror after restart");
        db.shutdown().await.expect("shutdown");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_upsert_compositions_fail_before_data_moves() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    for (consumer, needle) in [
        (
            "CREATE STREAM open_orders AS SELECT id, qty FROM orders WHERE label = 'OPEN'"
                .to_string(),
            "mutation",
        ),
        (
            format!(
                "CREATE SINK plain FROM orders INTO \"postgres-sink\" ({PG_SINK}, \
                 'port' = '{}', 'table.name' = '{}', 'write.mode' = 'append', \
                 'auto.create.table' = 'true')",
                port(),
                capture.mirror
            ),
            "changelog.mode=true",
        ),
    ] {
        let storage = tempfile::tempdir().unwrap();
        let db = open(storage.path()).await;
        let mut error = first_error(
            &db,
            &[capture.source("orders", ORDER_COLUMNS, ""), consumer],
        )
        .await
        .unwrap_or_default();
        if error.is_empty() {
            error = db
                .start()
                .await
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default();
        }
        assert!(error.contains(needle), "{needle}: {error}");
        let slots = slots_under(&client, &capture.slot).await.len();
        assert_eq!(slots, 0, "a rejected pipeline never creates the slot");
        let _ = db.shutdown().await;
    }
}

const CHANGELOG_COLUMNS: &str = "id BIGINT NOT NULL, label VARCHAR NOT NULL, qty INT, \
     __weight BIGINT NOT NULL, PRIMARY KEY (id)";

type Totals = BTreeMap<String, (i64, i64)>;

async fn totals(client: &tokio_postgres::Client, query: &str) -> Totals {
    client
        .query(query, &[])
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.get(0), (row.get(1), row.get(2))))
                .collect()
        })
        .unwrap_or_default()
}

/// The maintained results of the changelog pipeline and the queries that recompute them.
struct ChangelogViews {
    expected_totals: String,
    observed_totals: String,
    expected_open: String,
    observed_open: String,
}

impl ChangelogViews {
    async fn converge(&self, client: &tokio_postgres::Client) {
        let want = totals(client, &self.expected_totals).await;
        let got = eventually(
            CONVERGE,
            || totals(client, &self.observed_totals),
            |got| got == &want,
        )
        .await;
        assert_eq!(got, want, "maintained totals");
        let want = rows(client, &self.expected_open).await;
        let got = eventually(
            CONVERGE,
            || rows(client, &self.observed_open),
            |got| got == &want,
        )
        .await;
        assert_eq!(got, want, "filtered rows");
    }
}

/// Retractable SQL over a changelog source: a maintained total, a filter that rows move in and
/// out of, primary-key changes, and deletes, before and after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changelog_source_maintains_aggregates_and_filters_exactly() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    client
        .batch_execute(&format!(
            "INSERT INTO {t} VALUES (1, 'OPEN', 100), (2, 'OPEN', 5), (3, 'CLOSED', 7); \
             INSERT INTO {t} SELECT g, CASE WHEN g % 3 = 0 THEN 'OPEN' ELSE 'CLOSED' END, g \
             FROM generate_series(10, 400) g;",
            t = capture.table
        ))
        .await
        .unwrap();
    let totals_table = unique("totals");
    let open_table = unique("open");
    let statements = vec![
        capture.source("orders", CHANGELOG_COLUMNS, ", 'output.mode' = 'changelog'"),
        "CREATE STREAM status_totals AS SELECT label, SUM(qty) AS total, COUNT(*) AS n \
         FROM orders GROUP BY label EMIT CHANGES"
            .to_string(),
        "CREATE STREAM open_orders AS SELECT id, label, qty FROM orders WHERE label = 'OPEN'"
            .to_string(),
        format!(
            "CREATE SINK totals_sink FROM status_totals INTO \"postgres-sink\" ({PG_SINK}, \
             'port' = '{port}', 'table.name' = '{totals_table}', 'write.mode' = 'upsert', \
             'primary.key' = 'label', 'changelog.mode' = 'true', 'auto.create.table' = 'true')",
            port = port()
        ),
        format!(
            "CREATE SINK open_sink FROM open_orders INTO \"postgres-sink\" ({PG_SINK}, \
             'port' = '{port}', 'table.name' = '{open_table}', 'write.mode' = 'upsert', \
             'primary.key' = 'id', 'changelog.mode' = 'true', 'auto.create.table' = 'true')",
            port = port()
        ),
    ];
    let views = ChangelogViews {
        expected_totals: format!(
            "SELECT label, SUM(qty)::bigint, COUNT(*) FROM {} GROUP BY label",
            capture.table
        ),
        observed_totals: format!("SELECT label, total::bigint, n FROM {totals_table}"),
        expected_open: format!(
            "SELECT id, label, qty FROM {} WHERE label = 'OPEN' ORDER BY id",
            capture.table
        ),
        observed_open: format!("SELECT id, label, qty FROM {open_table} ORDER BY id"),
    };
    let storage = tempfile::tempdir().unwrap();
    {
        let db = open(storage.path()).await;
        execute_all(&db, &statements).await;
        db.start().await.expect("start");
        views.converge(&client).await;
        client
            .batch_execute(&format!(
                "UPDATE {t} SET qty = 120 WHERE id = 1; \
                 UPDATE {t} SET label = 'CLOSED' WHERE id = 2; \
                 UPDATE {t} SET id = 4 WHERE id = 3; \
                 DELETE FROM {t} WHERE id BETWEEN 10 AND 30;",
                t = capture.table
            ))
            .await
            .unwrap();
        views.converge(&client).await;
        let observed = totals(&client, &views.observed_totals).await;
        let expected = totals(&client, &views.expected_totals).await;
        assert_eq!(
            observed["OPEN"], expected["OPEN"],
            "100 -> 120 contributes 120, not 220"
        );
        assert!(db.checkpoint().await.unwrap().success);
        db.shutdown().await.expect("shutdown");
    }
    client
        .batch_execute(&format!(
            "UPDATE {t} SET label = 'OPEN' WHERE id BETWEEN 40 AND 60; \
             DELETE FROM {t} WHERE id = 1;",
            t = capture.table
        ))
        .await
        .unwrap();
    {
        let db = reopen(storage.path(), &statements).await;
        db.start().await.expect("restart");
        views.converge(&client).await;
        db.shutdown().await.expect("shutdown");
    }
}

/// Terminate the backends `query` selects, returning how many were terminated.
async fn terminate(client: &tokio_postgres::Client, query: &str) -> i64 {
    client
        .query_one(
            &format!("SELECT count(pg_terminate_backend(pid)) FROM ({query}) victims"),
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

/// Losing the replication connection, or the sink's and the source's control sessions, faults
/// the pipeline. The supervisor (on by default in the server) restarts it in process: the source
/// reattaches to its own slot at the last committed position and the mirror converges, replaying
/// at least once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_loss_recovers_from_the_committed_slot_position() {
    let _ = logs();
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'seed', g FROM generate_series(1, 200) g",
            capture.table
        ))
        .await
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path()).await;
    db.enable_supervision();
    execute_all(
        &db,
        &[
            capture.source("orders", ORDER_COLUMNS, ""),
            capture.upsert_sink("orders_mirror", "orders", "id"),
        ],
    )
    .await;
    db.start().await.expect("start");
    let source_rows = capture.source_rows();
    let mirror_rows = capture.mirror_rows();
    let converge = || async {
        let expected = rows(&client, &source_rows).await;
        eventually(
            CONVERGE,
            || rows(&client, &mirror_rows),
            |rows| rows == &expected,
        )
        .await
            == expected
    };
    assert!(converge().await, "mirror before any connection loss");
    // Commit a position on the slot so each restart below resumes the stream.
    assert!(db.checkpoint().await.expect("checkpoint").success);

    let walsender = walsender_of(&capture.slot);
    // The sink's sessions and the source's control session.
    let sessions = "SELECT pid FROM pg_stat_activity WHERE datname = current_database() \
                    AND backend_type = 'client backend' AND pid <> pg_backend_pid()"
        .to_string();
    for (victims, round) in [walsender, sessions].iter().zip(0_i64..) {
        churn(&client, &capture.table, 2 * round + 1).await;
        let terminated = eventually(
            CONVERGE,
            || terminate(&client, victims),
            |terminated| *terminated > 0,
        )
        .await;
        assert!(terminated > 0, "round {round}: no backend to terminate");
        churn(&client, &capture.table, 2 * round + 2).await;
        assert!(
            converge().await,
            "round {round}: mirror after the connection loss"
        );
    }
    db.shutdown().await.expect("shutdown");
}

/// The source's replication connection, once its slot exists and streams.
fn walsender_of(prefix: &str) -> String {
    format!(
        "SELECT active_pid AS pid FROM pg_replication_slots \
         WHERE starts_with(slot_name, '{prefix}_') AND active_pid IS NOT NULL"
    )
}

/// A source that faults before any checkpoint covering its rows commits restarts on the slot it
/// created and loses nothing. Its stream opens one checkpoint after the slot is created, and the
/// fault lands before the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fault_before_the_first_slot_position_commits_recovers() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    let storage = tempfile::tempdir().unwrap();
    let db = open_with(storage.path(), Some(5_000)).await;
    db.enable_supervision();
    execute_all(
        &db,
        &[
            capture.source("orders", ORDER_COLUMNS, ", 'snapshot.mode' = 'never'"),
            capture.upsert_sink("orders_mirror", "orders", "id"),
        ],
    )
    .await;
    db.start().await.expect("start");
    let walsender = walsender_of(&capture.slot);
    let count = format!("SELECT count(*) FROM ({walsender}) holders");
    let streaming = eventually(
        CONVERGE,
        || async {
            client
                .query_one(&count, &[])
                .await
                .unwrap()
                .get::<_, i64>(0)
        },
        |active| *active > 0,
    )
    .await;
    assert!(streaming > 0, "the source never started streaming");
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'before', g FROM generate_series(1, 100) g",
            capture.table
        ))
        .await
        .unwrap();
    assert!(terminate(&client, &walsender).await > 0);
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'after', g FROM generate_series(101, 200) g",
            capture.table
        ))
        .await
        .unwrap();
    let expected = rows(&client, &capture.source_rows()).await;
    let mirror_rows = capture.mirror_rows();
    let observed = eventually(
        CONVERGE,
        || rows(&client, &mirror_rows),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(
        observed.len(),
        expected.len(),
        "mirror after the early fault: {:?}",
        db.last_fault()
    );
    assert_eq!(observed, expected);
    db.shutdown().await.expect("shutdown");
}

/// With manual checkpoints the slot is created after the first `checkpoint()` commits the claim
/// and the copy starts after the next; a restart before the claim commits leaves nothing behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_checkpoints_create_the_slot_and_a_restart_before_them_leaves_nothing() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'seed', g FROM generate_series(1, 200) g",
            capture.table
        ))
        .await
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let statements = vec![
        capture.source("orders", ORDER_COLUMNS, ""),
        capture.upsert_sink("orders_mirror", "orders", "id"),
    ];
    let db = open_with(storage.path(), None).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(slots_under(&client, &capture.slot).await.is_empty());
    db.shutdown().await.expect("shutdown");

    let db = reopen_with(storage.path(), &statements, None).await;
    db.start().await.expect("restart before the claim commits");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        slots_under(&client, &capture.slot).await.is_empty(),
        "no slot before a checkpoint commits the claim"
    );
    assert!(db.checkpoint().await.expect("checkpoint").success);
    let created = eventually(
        CONVERGE,
        || slots_under(&client, &capture.slot),
        |slots| !slots.is_empty(),
    )
    .await;
    assert_eq!(created.len(), 1, "{created:?}");
    let expected = rows(&client, &capture.source_rows()).await;
    let mirror_rows = capture.mirror_rows();
    let observed = eventually(
        CONVERGE,
        || async {
            assert!(db.checkpoint().await.expect("checkpoint").success);
            rows(&client, &mirror_rows).await
        },
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected);
    assert_eq!(slots_under(&client, &capture.slot).await, created);
    db.shutdown().await.expect("shutdown");
}

/// Slot creation waits for transactions already running. The wait is reported with the
/// transaction it waits for, and a restart during the wait ends the stale creation and creates
/// the claimed slot once the transaction ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slot_creation_waits_for_running_transactions_and_survives_a_restart() {
    let logs = logs();
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    let unrelated = unique("unrelated");
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'seed', g FROM generate_series(1, 100) g; \
             CREATE TABLE {unrelated} (id integer);",
            capture.table
        ))
        .await
        .unwrap();
    let blocker = postgres().await.expect("a second session");
    blocker
        .batch_execute(&format!("BEGIN; INSERT INTO {unrelated} VALUES (1)"))
        .await
        .unwrap();
    let pid: i32 = blocker
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let storage = tempfile::tempdir().unwrap();
    let statements = vec![
        capture.source("orders", ORDER_COLUMNS, ""),
        capture.upsert_sink("orders_mirror", "orders", "id"),
    ];
    let db = open(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    let needle = format!("pid {pid} ");
    let reported = eventually(
        Duration::from_secs(90),
        || async { logged(&logs, &needle) },
        |reported| *reported,
    )
    .await;
    assert!(
        reported,
        "the creation wait names the transaction it waits for"
    );
    assert!(rows(&client, &capture.mirror_rows()).await.is_empty());
    db.shutdown()
        .await
        .expect("shutdown while the slot is being created");

    let db = reopen(storage.path(), &statements).await;
    db.start().await.expect("restart from the committed claim");
    blocker.batch_execute("COMMIT").await.unwrap();
    let expected = rows(&client, &capture.source_rows()).await;
    let mirror_rows = capture.mirror_rows();
    let observed = eventually(
        CONVERGE,
        || rows(&client, &mirror_rows),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected);
    assert_eq!(
        slots_under(&client, &capture.slot).await.len(),
        1,
        "the restart created the claimed slot and nothing else"
    );
    db.shutdown().await.expect("shutdown");
    client
        .batch_execute(&format!("DROP TABLE {unrelated}"))
        .await
        .unwrap();
}

/// A crash after the slot exists but before a cursor naming its snapshot commits emitted no row,
/// so the restart copies the table again from a new slot and reports the first one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_before_the_snapshot_cursor_commits_restarts_on_a_new_slot_exactly() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'seed', g FROM generate_series(1, 300) g",
            capture.table
        ))
        .await
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let statements = vec![
        capture.source("orders", ORDER_COLUMNS, ""),
        capture.upsert_sink("orders_mirror", "orders", "id"),
    ];
    let db = open_with(storage.path(), Some(5_000)).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    let first = eventually(
        CONVERGE,
        || slots_under(&client, &capture.slot),
        |slots| !slots.is_empty(),
    )
    .await;
    assert_eq!(first.len(), 1, "{first:?}");
    db.shutdown()
        .await
        .expect("shutdown before the next checkpoint");
    assert!(
        rows(&client, &capture.mirror_rows()).await.is_empty(),
        "no snapshot row passes the gate before its cursor commits"
    );

    churn(&client, &capture.table, 0).await;
    let db = reopen(storage.path(), &statements).await;
    db.start().await.expect("restart from the committed claim");
    churn(&client, &capture.table, 1).await;
    let expected = rows(&client, &capture.source_rows()).await;
    let mirror_rows = capture.mirror_rows();
    let observed = eventually(
        CONVERGE,
        || rows(&client, &mirror_rows),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected);
    let slots = slots_under(&client, &capture.slot).await;
    assert_eq!(slots.len(), 2, "{slots:?}");
    assert!(
        slots.contains(&first[0]),
        "the first slot is reported, never dropped"
    );
    db.shutdown().await.expect("shutdown");
}

/// One process-wide subscriber: `RUST_LOG`-filtered output for the test log, plus every
/// `laminar_connectors` warning kept for assertions.
fn logs() -> Arc<Mutex<Vec<u8>>> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;

    #[derive(Clone)]
    struct Captured(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    static LOGS: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    Arc::clone(LOGS.get_or_init(|| {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let captured = Captured(Arc::clone(&logs));
        let _ = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_test_writer()
                    .with_filter(tracing_subscriber::EnvFilter::from_default_env()),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || captured.clone())
                    .with_filter(tracing_subscriber::filter::Targets::new().with_target(
                        "laminar_connectors",
                        tracing_subscriber::filter::LevelFilter::WARN,
                    )),
            )
            .try_init();
        logs
    }))
}

fn logged(logs: &Mutex<Vec<u8>>, needle: &str) -> bool {
    String::from_utf8_lossy(
        &logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
    .contains(needle)
}

/// A changelog source read straight into a keyed changelog sink applies each retraction and
/// insertion by key, primary-key changes included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changelog_source_feeds_a_changelog_sink_directly() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'seed', g FROM generate_series(1, 500) g",
            capture.table
        ))
        .await
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path()).await;
    execute_all(
        &db,
        &[
            capture.source("orders", CHANGELOG_COLUMNS, ", 'output.mode' = 'changelog'"),
            capture.upsert_sink("orders_mirror", "orders", "id"),
        ],
    )
    .await;
    db.start().await.expect("start");
    churn(&client, &capture.table, 0).await;
    let expected = rows(&client, &capture.source_rows()).await;
    let mirror_query = capture.mirror_rows();
    let observed = eventually(
        CONVERGE,
        || rows(&client, &mirror_query),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected);
    db.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changelog_consumers_that_cannot_retract_are_rejected_before_data_moves() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    let changelog = capture.source("orders", CHANGELOG_COLUMNS, ", 'output.mode' = 'changelog'");
    let cases = [
        (
            vec![
                changelog.clone(),
                "CREATE STREAM top AS SELECT label, MAX(qty) AS m FROM orders GROUP BY label"
                    .to_string(),
            ],
            "MIN/MAX",
        ),
        (
            vec![capture.source(
                "orders",
                CHANGELOG_COLUMNS,
                ", 'output.mode' = 'changelog', 'snapshot.mode' = 'never'",
            )],
            "snapshot.mode=initial",
        ),
        (
            vec![
                changelog.clone(),
                format!(
                    "CREATE SINK raw FROM orders INTO \"postgres-sink\" ({PG_SINK}, \
                     'port' = '{}', 'table.name' = '{}', 'write.mode' = 'append', \
                     'auto.create.table' = 'true')",
                    port(),
                    capture.mirror
                ),
            ],
            "changelog",
        ),
        (
            vec![capture.source("orders", ORDER_COLUMNS, ", 'output.mode' = 'changelog'")],
            "__weight",
        ),
    ];
    for (statements, needle) in cases {
        let storage = tempfile::tempdir().unwrap();
        let db = open(storage.path()).await;
        let error = match first_error(&db, &statements).await {
            Some(error) => error,
            None => db
                .start()
                .await
                .err()
                .map(|error| error.to_string())
                .unwrap_or_default(),
        };
        assert!(error.contains(needle), "{needle}: {error}");
        let slots = slots_under(&client, &capture.slot).await.len();
        assert_eq!(
            slots, 0,
            "{needle}: a rejected pipeline never creates the slot"
        );
        let _ = db.shutdown().await;
    }
}

/// Start a mirror of `capture` into a pre-created target with `UNIQUE (label) {constraint}`.
async fn unique_mirror(
    client: &tokio_postgres::Client,
    capture: &Capture,
    storage: &Path,
    constraint: &str,
) -> Result<std::sync::Arc<LaminarDB>, String> {
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (id bigint PRIMARY KEY, label text, qty integer, \
             CONSTRAINT {}_label UNIQUE (label) {constraint})",
            capture.mirror, capture.mirror
        ))
        .await
        .unwrap();
    let db = open(storage).await;
    let statements = [
        capture.source("orders", ORDER_COLUMNS, ""),
        capture.upsert_sink("orders_mirror", "orders", "id"),
    ];
    if let Some(error) = first_error(&db, &statements).await {
        return Err(error);
    }
    db.start().await.map_err(|error| error.to_string())?;
    Ok(db)
}

/// One source transaction deletes a row and gives its unique value to another, and two rows
/// swap values: applied as final row states, these transfers are valid only when the target
/// checks its extra unique constraint at commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_value_transfers_reach_a_deferrable_target() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    client
        .batch_execute(&format!(
            "INSERT INTO {} VALUES (1, 'x', 1), (2, 'y', 2), (3, 'p', 3), (4, 'q', 4)",
            capture.table
        ))
        .await
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let db = unique_mirror(
        &client,
        &capture,
        storage.path(),
        "DEFERRABLE INITIALLY IMMEDIATE",
    )
    .await
    .expect("a deferrable extra unique constraint is admitted");
    let source_rows = capture.source_rows();
    let mirror_rows = capture.mirror_rows();
    let expected = rows(&client, &source_rows).await;
    let observed = eventually(
        CONVERGE,
        || rows(&client, &mirror_rows),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected, "snapshot");
    client
        .batch_execute(&format!(
            "BEGIN; DELETE FROM {t} WHERE id = 1; UPDATE {t} SET label = 'x' WHERE id = 2; \
             UPDATE {t} SET label = 'tmp' WHERE id = 3; UPDATE {t} SET label = 'p' WHERE id = 4; \
             UPDATE {t} SET label = 'q' WHERE id = 3; COMMIT;",
            t = capture.table
        ))
        .await
        .unwrap();
    let expected = rows(&client, &source_rows).await;
    let observed = eventually(
        CONVERGE,
        || rows(&client, &mirror_rows),
        |rows| rows == &expected,
    )
    .await;
    assert_eq!(observed, expected, "unique transfers and swaps");
    db.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_deferrable_extra_unique_constraints_are_rejected_before_data_moves() {
    let Some(client) = postgres().await else {
        return;
    };
    for constraint in ["", "NOT DEFERRABLE"] {
        let capture = Capture::new(&client).await;
        let storage = tempfile::tempdir().unwrap();
        let Err(error) = unique_mirror(&client, &capture, storage.path(), constraint).await else {
            panic!("a non-deferrable extra unique constraint is rejected");
        };
        assert!(error.contains("DEFERRABLE"), "{error}");
        let slots = slots_under(&client, &capture.slot).await.len();
        assert_eq!(slots, 0);
    }

    // A plain unique index backs no constraint and can never be deferred.
    let capture = Capture::new(&client).await;
    client
        .batch_execute(&format!(
            "CREATE TABLE {m} (id bigint PRIMARY KEY, label text, qty integer); \
             CREATE UNIQUE INDEX {m}_label ON {m} (label);",
            m = capture.mirror
        ))
        .await
        .unwrap();
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path()).await;
    let error = first_error(
        &db,
        &[
            capture.source("orders", ORDER_COLUMNS, ""),
            capture.upsert_sink("orders_mirror", "orders", "id"),
        ],
    )
    .await
    .expect("a plain unique index is rejected");
    assert!(error.contains("unique index"), "{error}");
    let _ = db.shutdown().await;
}

/// Create `mirror` with a trigger that sleeps `delay_ms` for every row written to it.
async fn create_slow_mirror(client: &tokio_postgres::Client, mirror: &str, delay_ms: f64) {
    client
        .batch_execute(&format!(
            "CREATE TABLE {mirror} (id bigint PRIMARY KEY, label text, qty integer); \
             CREATE FUNCTION {mirror}_delay() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN PERFORM pg_sleep({seconds}); RETURN NEW; END $$; \
             CREATE TRIGGER delay BEFORE INSERT OR UPDATE ON {mirror} \
             FOR EACH ROW EXECUTE FUNCTION {mirror}_delay();",
            seconds = delay_ms / 1000.0
        ))
        .await
        .unwrap();
}

/// One source transaction that the target needs about 40 s to apply, longer than the default
/// 30 s statement timeout, lands with default timeouts and without faulting the pipeline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_slower_than_the_statement_timeout_reaches_a_slow_target() {
    const ROWS: usize = 10_000;
    let Some(client) = postgres().await else {
        return;
    };
    let capture = Capture::new(&client).await;
    create_slow_mirror(&client, &capture.mirror, 4.0).await;
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path()).await;
    execute_all(
        &db,
        &[
            capture.source("orders", ORDER_COLUMNS, ""),
            capture.upsert_sink("orders_mirror", "orders", "id"),
        ],
    )
    .await;
    db.start().await.expect("start");
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, 'slow', g FROM generate_series(1, {ROWS}) g",
            capture.table
        ))
        .await
        .unwrap();
    let expected = rows(&client, &capture.source_rows()).await;
    let mirror_rows = capture.mirror_rows();
    let observed = eventually(
        Duration::from_secs(80),
        || rows(&client, &mirror_rows),
        |rows| rows == &expected || db.pipeline_state() == "Faulted",
    )
    .await;
    assert_eq!(db.last_fault(), None);
    assert!(
        observed == expected,
        "mirror holds {} of {ROWS} rows",
        observed.len()
    );
    db.shutdown().await.expect("shutdown");
}

const WIDE_ROWS: usize = 6_000;

/// A table whose initial snapshot takes seconds: 6,000 rows of 16 KiB.
async fn wide_capture(client: &tokio_postgres::Client) -> Capture {
    let capture = Capture::new(client).await;
    client
        .batch_execute(&format!(
            "INSERT INTO {} SELECT g, repeat(md5(g::text), 500), g \
             FROM generate_series(1, {WIDE_ROWS}) g",
            capture.table
        ))
        .await
        .unwrap();
    capture
}

/// A database whose checkpoints fire every 100 ms and time out well before the wide snapshot
/// finishes copying.
async fn open_with_short_checkpoints(storage: &Path) -> std::sync::Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(100),
            timeout_ms: Some(2_000),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .config_var("E2E_PG_PASSWORD", PASSWORD)
        .build()
        .await
        .expect("open database")
}

fn wide_pipeline(capture: &Capture) -> Vec<String> {
    vec![
        capture.source(
            "orders",
            ORDER_COLUMNS,
            ", 'max.buffered.bytes' = '1048576'",
        ),
        capture.upsert_sink("orders_mirror", "orders", "id"),
    ]
}

/// Checkpoints keep committing while the snapshot copies, so a copy that outlives the
/// checkpoint timeout neither stalls a barrier nor faults the pipeline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_longer_than_the_checkpoint_timeout_completes() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = wide_capture(&client).await;
    let storage = tempfile::tempdir().unwrap();
    let db = open_with_short_checkpoints(storage.path()).await;
    execute_all(&db, &wide_pipeline(&capture)).await;
    db.start().await.expect("start");
    client
        .batch_execute(&format!(
            "UPDATE {} SET qty = -qty WHERE id % 1000 = 0",
            capture.table
        ))
        .await
        .unwrap();
    let source_rows = capture.source_rows();
    let mirror_rows = capture.mirror_rows();
    let expected = rows(&client, &source_rows).await;
    let observed = eventually(
        CONVERGE,
        || rows(&client, &mirror_rows),
        |rows| rows == &expected,
    )
    .await;
    assert!(
        observed == expected,
        "mirror converged after a long snapshot"
    );
    assert!(db.checkpoint().await.expect("checkpoint").success);
    client
        .batch_execute(&format!(
            "UPDATE {} SET qty = 0 WHERE id = 1",
            capture.table
        ))
        .await
        .unwrap();
    let expected = rows(&client, &source_rows).await;
    let observed = eventually(
        CONVERGE,
        || rows(&client, &mirror_rows),
        |rows| rows == &expected,
    )
    .await;
    assert!(
        observed == expected,
        "streaming continues after the long snapshot"
    );
    db.shutdown().await.expect("shutdown");
}

/// An exported snapshot cannot be re-imported, so a restart inside the initial snapshot fails
/// closed with reset guidance instead of resuming from a partial copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_inside_the_snapshot_fails_closed() {
    let Some(client) = postgres().await else {
        return;
    };
    let capture = wide_capture(&client).await;
    let storage = tempfile::tempdir().unwrap();
    let statements = wide_pipeline(&capture);
    let db = open_with_short_checkpoints(storage.path()).await;
    execute_all(&db, &statements).await;
    db.start().await.expect("start");
    let mirror_rows = capture.mirror_rows();
    let partial = eventually(
        CONVERGE,
        || rows(&client, &mirror_rows),
        |rows| !rows.is_empty(),
    )
    .await;
    assert!(
        !partial.is_empty() && partial.len() < WIDE_ROWS,
        "the restart lands inside the snapshot ({} mirrored rows)",
        partial.len()
    );
    db.shutdown().await.expect("shutdown");
    let slots = slots_under(&client, &capture.slot).await;
    assert_eq!(slots.len(), 1, "{slots:?}");

    let db = reopen(storage.path(), &statements).await;
    let error = db
        .start()
        .await
        .expect_err("a snapshot-phase checkpoint is not resumable")
        .to_string();
    assert!(error.contains("initial snapshot"), "{error}");
    assert!(
        error.contains(&format!("SELECT pg_drop_replication_slot('{}')", slots[0])),
        "{error}"
    );
    let _ = db.shutdown().await;
}

/// Latency and resource measurements for the direct PostgreSQL CDC path. The numbers are only
/// meaningful from a release build:
///
/// ```text
/// cargo test --release -p laminar-db --no-default-features --features postgres-cdc,postgres-sink \
///     --test postgres_cdc_e2e latency -- --ignored --nocapture --test-threads=1
/// ```
///
/// Commit→sink compares the commit timestamps PostgreSQL records (`track_commit_timestamp`) for
/// each source row and for the mirror row that applied it, so both ends read one clock.
/// Commit→visible compares when the writer's `COMMIT` returned with when a stream subscription
/// delivered the row, both on this process's monotonic clock; it under-counts by the network leg
/// of the commit acknowledgement.
mod latency {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow_array::{Array, Int64Array, RecordBatch};
    use laminar_db::{DeliveryGuarantee, FromBatch, LaminarDB, TypedSubscriptionFrame};

    use super::{
        create_slow_mirror, execute_all, first_error, postgres, unique, Capture, CHANGELOG_COLUMNS,
        ORDER_COLUMNS, PASSWORD,
    };

    /// How the writer commits rows.
    #[derive(Clone, Copy)]
    enum Load {
        /// One single-row transaction every `every` for `run`.
        Paced { every: Duration, run: Duration },
        /// `bursts` groups of `txns` single-row transactions pipelined on one connection,
        /// `pause` apart; commit flushes bound the rate.
        Bursts {
            bursts: usize,
            txns: usize,
            pause: Duration,
        },
        /// `writers` connections committing single-row transactions back to back for `run`.
        Sustained { writers: usize, run: Duration },
        /// `rows` inserts of a `kib` KiB incompressible payload, then an update of each row that
        /// leaves the payload unchanged (an unchanged out-of-line TOAST value).
        Wide { rows: usize, kib: usize },
        /// One transaction inserting `rows` rows.
        Large { rows: usize },
    }

    struct Scenario {
        name: &'static str,
        load: Load,
        checkpoint_ms: u64,
        max_buffered_bytes: usize,
        /// Delay per mirror row, applied by a trigger on the target.
        sink_delay_ms: Option<f64>,
        /// Also measure commit→visible through a changelog source and a stream subscription.
        visible: bool,
    }

    impl Scenario {
        const fn new(name: &'static str, load: Load) -> Self {
            Self {
                name,
                load,
                checkpoint_ms: 1_000,
                max_buffered_bytes: 64 << 20,
                sink_delay_ms: None,
                visible: false,
            }
        }
    }

    /// One delivered `id` with its weight; retractions are not arrivals.
    struct Arrival(i64, i64);

    impl FromBatch for Arrival {
        fn from_batch(batch: &RecordBatch, row: usize) -> Self {
            let column = |name: &str| {
                batch
                    .column_by_name(name)
                    .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                    .map(|values| values.value(row))
            };
            Self(column("id").unwrap_or(-1), column("__weight").unwrap_or(1))
        }

        fn from_batch_all(batch: &RecordBatch) -> Vec<Self> {
            (0..batch.num_rows())
                .map(|row| Self::from_batch(batch, row))
                .collect()
        }
    }

    async fn connect() -> tokio_postgres::Client {
        postgres().await.expect("PostgreSQL CDC fixture")
    }

    fn percentiles(mut values: Vec<f64>) -> String {
        if values.is_empty() {
            return "n/a".into();
        }
        values.sort_by(f64::total_cmp);
        let at = |q: f64| {
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                clippy::cast_precision_loss
            )]
            let index = ((values.len() - 1) as f64 * q).round() as usize;
            values[index]
        };
        format!(
            "p50={:.1} p95={:.1} p99={:.1} max={:.1}",
            at(0.5),
            at(0.95),
            at(0.99),
            at(1.0)
        )
    }

    /// Resident memory of this process in MiB.
    fn resident_mib() -> Option<u64> {
        #[cfg(windows)]
        {
            let output = std::process::Command::new("powershell")
                .args([
                    "-NoProfile",
                    "-Command",
                    &format!("(Get-Process -Id {}).WorkingSet64", std::process::id()),
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

    /// Peak process memory and slot retention sampled until `stop` is set.
    struct Peaks {
        rss_mib: u64,
        lag_bytes: i64,
        retained_bytes: i64,
    }

    async fn sample_peaks(slot: String, stop: Arc<AtomicBool>) -> Peaks {
        let client = connect().await;
        let mut peaks = Peaks {
            rss_mib: 0,
            lag_bytes: 0,
            retained_bytes: 0,
        };
        let mut next_rss = Instant::now();
        while !stop.load(Ordering::Relaxed) {
            if let Ok(row) = client
                .query_one(
                    "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint, \
                     COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
                     FROM pg_replication_slots WHERE starts_with(slot_name::text, $1)",
                    &[&format!("{slot}_")],
                )
                .await
            {
                peaks.lag_bytes = peaks.lag_bytes.max(row.get(0));
                peaks.retained_bytes = peaks.retained_bytes.max(row.get(1));
            }
            if Instant::now() >= next_rss {
                next_rss = Instant::now() + Duration::from_secs(2);
                if let Ok(Some(mib)) = tokio::task::spawn_blocking(resident_mib).await {
                    peaks.rss_mib = peaks.rss_mib.max(mib);
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        peaks
    }

    fn insert(table: &str, id: i64) -> String {
        format!("INSERT INTO {table} VALUES ({id}, 'row', 0)")
    }

    /// Commit `load` against `table`, returning each row's id with the instant its commit
    /// returned.
    #[allow(clippy::too_many_lines)]
    async fn write(table: String, load: Load) -> Vec<(i64, Instant)> {
        let client = connect().await;
        let mut committed = Vec::new();
        match load {
            Load::Paced { every, run } => {
                let deadline = Instant::now() + run;
                let mut tick = tokio::time::interval(every);
                for id in 1_i64.. {
                    tick.tick().await;
                    if Instant::now() >= deadline {
                        break;
                    }
                    client.batch_execute(&insert(&table, id)).await.unwrap();
                    committed.push((id, Instant::now()));
                }
            }
            Load::Bursts {
                bursts,
                txns,
                pause,
            } => {
                let mut next = 1_i64;
                for _ in 0..bursts {
                    let ids = next..next + i64::try_from(txns).unwrap();
                    next = ids.end;
                    let commits = ids.map(|id| {
                        let client = &client;
                        let statement = insert(&table, id);
                        async move {
                            client.execute(statement.as_str(), &[]).await.unwrap();
                            (id, Instant::now())
                        }
                    });
                    committed.extend(futures_util::future::join_all(commits).await);
                    tokio::time::sleep(pause).await;
                }
            }
            Load::Sustained { writers, run } => {
                let deadline = Instant::now() + run;
                let tasks = (0..writers).map(|writer| {
                    let table = table.clone();
                    tokio::spawn(async move {
                        let client = connect().await;
                        let mut committed = Vec::new();
                        let base = 1_000_000_000 * i64::try_from(writer).unwrap();
                        let mut id = base;
                        while Instant::now() < deadline {
                            id += 1;
                            client.batch_execute(&insert(&table, id)).await.unwrap();
                            committed.push((id, Instant::now()));
                        }
                        committed
                    })
                });
                for task in futures_util::future::join_all(tasks).await {
                    committed.extend(task.unwrap());
                }
            }
            Load::Wide { rows, kib } => {
                let rows = i64::try_from(rows).unwrap();
                for id in 1..=rows {
                    client
                        .batch_execute(&format!(
                            "INSERT INTO {table} SELECT {id}, string_agg(md5(random()::text), ''), 0 \
                             FROM generate_series(1, {chunks})",
                            chunks = kib * 32
                        ))
                        .await
                        .unwrap();
                }
                for id in 1..=rows {
                    client
                        .batch_execute(&format!("UPDATE {table} SET qty = 1 WHERE id = {id}"))
                        .await
                        .unwrap();
                    committed.push((id, Instant::now()));
                }
            }
            Load::Large { rows } => {
                client
                    .batch_execute(&format!(
                        "INSERT INTO {table} SELECT g, 'large', g FROM generate_series(1, {rows}) g"
                    ))
                    .await
                    .unwrap();
                let at = Instant::now();
                committed.extend((1..=i64::try_from(rows).unwrap()).map(|id| (id, at)));
            }
        }
        committed
    }

    async fn digest(client: &tokio_postgres::Client, table: &str) -> (i64, i64, i64, i64) {
        client
            .query_one(
                &format!(
                    "SELECT count(*), COALESCE(sum(id), 0)::bigint, COALESCE(sum(qty), 0)::bigint, \
                     COALESCE(sum(length(label)), 0)::bigint FROM {table}"
                ),
                &[],
            )
            .await
            .map_or((-1, 0, 0, 0), |row| {
                (row.get(0), row.get(1), row.get(2), row.get(3))
            })
    }

    /// Wait until the mirror equals the source, returning the time it took.
    async fn converge(client: &tokio_postgres::Client, capture: &Capture) -> Duration {
        let started = Instant::now();
        let expected = digest(client, &capture.table).await;
        while digest(client, &capture.mirror).await != expected {
            assert!(
                started.elapsed() < Duration::from_secs(600),
                "mirror did not converge"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        started.elapsed()
    }

    /// Per-row commit→sink milliseconds, and rows per second from the first source commit to the
    /// last mirror commit.
    async fn sink_latency(client: &tokio_postgres::Client, capture: &Capture) -> (Vec<f64>, f64) {
        let rows = client
            .query(
                &format!(
                    "SELECT (extract(epoch FROM pg_xact_commit_timestamp(m.xmin) \
                     - pg_xact_commit_timestamp(s.xmin)) * 1000)::float8 \
                     FROM {} s JOIN {} m USING (id)",
                    capture.table, capture.mirror
                ),
                &[],
            )
            .await
            .unwrap();
        let span = client
            .query_one(
                &format!(
                    "SELECT count(*)::float8 / greatest(extract(epoch FROM \
                     (SELECT max(pg_xact_commit_timestamp(xmin)) FROM {}) \
                     - (SELECT min(pg_xact_commit_timestamp(xmin)) FROM {})), 0.001)::float8 \
                     FROM {}",
                    capture.mirror, capture.table, capture.table
                ),
                &[],
            )
            .await
            .unwrap();
        (
            rows.into_iter().map(|row| row.get(0)).collect(),
            span.get(0),
        )
    }

    async fn open_db(storage: &Path, checkpoint_ms: u64) -> Arc<LaminarDB> {
        let db = LaminarDB::builder()
            .storage_dir(storage)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: Some(checkpoint_ms),
                ..Default::default()
            })
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .config_var("E2E_PG_PASSWORD", PASSWORD)
            .build()
            .await
            .expect("open database");
        // As in the server, a recoverable fault restarts the pipeline instead of parking it.
        db.enable_supervision();
        db
    }

    /// Rows the writer committed, and commit→visible milliseconds for those the subscription
    /// delivered within a minute of the writer finishing.
    async fn visible_latency(
        subscription: &mut laminar_db::TypedSubscription<Arrival>,
        writer: tokio::task::JoinHandle<Vec<(i64, Instant)>>,
    ) -> (Vec<(i64, Instant)>, Vec<f64>) {
        let mut arrivals = HashMap::new();
        let mut writer = Some(writer);
        let mut committed = Vec::new();
        let mut deadline = None;
        loop {
            if writer
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
            {
                committed = writer.take().unwrap().await.unwrap();
                deadline = Some(Instant::now() + Duration::from_secs(60));
            }
            if writer.is_none() && arrivals.len() >= committed.len() {
                break;
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                break;
            }
            match tokio::time::timeout(Duration::from_millis(50), subscription.next_frame()).await {
                Ok(Ok(Some(TypedSubscriptionFrame::Rows { rows, .. }))) => {
                    let at = Instant::now();
                    for Arrival(id, weight) in rows {
                        if weight > 0 {
                            arrivals.entry(id).or_insert(at);
                        }
                    }
                }
                Ok(Ok(Some(_))) | Err(_) => {}
                Ok(Ok(None)) => break,
                Ok(Err(error)) => panic!("subscription failed: {error}"),
            }
        }
        let latencies = committed
            .iter()
            .filter_map(|(id, at)| {
                arrivals
                    .get(id)
                    .map(|seen| (*seen - *at).as_secs_f64() * 1e3)
            })
            .collect();
        (committed, latencies)
    }

    #[allow(clippy::too_many_lines)]
    async fn run(scenario: Scenario) {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let client = connect().await;
        let capture = Capture::new(&client).await;
        if let Some(delay) = scenario.sink_delay_ms {
            create_slow_mirror(&client, &capture.mirror, delay).await;
        }
        let changes = Capture {
            table: capture.table.clone(),
            slot: unique("slot"),
            publication: capture.publication.clone(),
            mirror: unique("unused"),
        };
        let storage = tempfile::tempdir().unwrap();
        let db = open_db(storage.path(), scenario.checkpoint_ms).await;
        let budget = format!(", 'max.buffered.bytes' = '{}'", scenario.max_buffered_bytes);
        let mut statements = vec![
            capture.source("orders", ORDER_COLUMNS, &budget),
            capture.upsert_sink("orders_mirror", "orders", "id"),
        ];
        if scenario.visible {
            statements.push(changes.source(
                "changes",
                CHANGELOG_COLUMNS,
                &format!("{budget}, 'output.mode' = 'changelog'"),
            ));
            statements.push("CREATE STREAM visible AS SELECT id, qty FROM changes".into());
        }
        execute_all(&db, &statements).await;
        db.start().await.expect("start");
        let mut subscription = if scenario.visible {
            Some(db.subscribe::<Arrival>("visible").await.unwrap())
        } else {
            None
        };
        let stop = Arc::new(AtomicBool::new(false));
        let sampler = tokio::spawn(sample_peaks(capture.slot.clone(), Arc::clone(&stop)));
        let started = Instant::now();
        let writer = tokio::spawn(write(capture.table.clone(), scenario.load));
        let (committed, visible) = match subscription.as_mut() {
            Some(subscription) => visible_latency(subscription, writer).await,
            None => (writer.await.unwrap(), Vec::new()),
        };
        let wrote = started.elapsed();
        let drained = converge(&client, &capture).await;
        stop.store(true, Ordering::Relaxed);
        let peaks = sampler.await.unwrap();
        let checkpoints = db.checkpoint_stats().await;
        let (sink, throughput) = sink_latency(&client, &capture).await;
        println!(
            "LATENCY {name}: checkpoint={ckpt}ms rows={rows} wrote={wrote:.1}s \
             drained_after_writes={drained:.2}s throughput={throughput:.0} rows/s\n  \
             commit->sink ms {sink}\n  commit->visible ms {visible}\n  \
             peak RSS {rss} MiB, peak slot lag {lag} KiB, peak retained WAL {retained} KiB, \
             checkpoints {checkpoints}",
            name = scenario.name,
            ckpt = scenario.checkpoint_ms,
            rows = committed.len(),
            wrote = wrote.as_secs_f64(),
            drained = drained.as_secs_f64(),
            sink = percentiles(sink),
            visible = percentiles(visible),
            rss = peaks.rss_mib,
            lag = peaks.lag_bytes >> 10,
            retained = peaks.retained_bytes >> 10,
            checkpoints = checkpoints.map_or_else(
                || "n/a".into(),
                |stats| format!(
                    "completed={} failed={} p50={}ms p95={}ms p99={}ms",
                    stats.completed,
                    stats.failed,
                    stats.duration_p50_ms,
                    stats.duration_p95_ms,
                    stats.duration_p99_ms
                )
            ),
        );
        drop(subscription);
        db.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "benchmark: run in release with --nocapture"]
    async fn latency_low_rate() {
        for checkpoint_ms in [100, 1_000] {
            run(Scenario {
                checkpoint_ms,
                visible: true,
                ..Scenario::new(
                    "low_rate",
                    Load::Paced {
                        every: Duration::from_millis(20),
                        run: Duration::from_secs(15),
                    },
                )
            })
            .await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "benchmark: run in release with --nocapture"]
    async fn latency_bursts() {
        run(Scenario {
            visible: true,
            ..Scenario::new(
                "bursts",
                Load::Bursts {
                    bursts: 10,
                    txns: 2_000,
                    pause: Duration::from_secs(1),
                },
            )
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "benchmark: run in release with --nocapture"]
    async fn latency_sustained_small_transactions() {
        for checkpoint_ms in [100, 1_000] {
            run(Scenario {
                checkpoint_ms,
                visible: true,
                ..Scenario::new(
                    "sustained",
                    Load::Sustained {
                        writers: 8,
                        run: Duration::from_secs(20),
                    },
                )
            })
            .await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "benchmark: run in release with --nocapture"]
    async fn latency_wide_toast_rows() {
        run(Scenario::new(
            "wide_toast",
            Load::Wide { rows: 300, kib: 64 },
        ))
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "benchmark: run in release with --nocapture"]
    async fn latency_large_transaction() {
        run(Scenario {
            max_buffered_bytes: 1 << 30,
            ..Scenario::new("large_txn", Load::Large { rows: 200_000 })
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "benchmark: run in release with --nocapture"]
    async fn latency_slow_sink() {
        run(Scenario {
            sink_delay_ms: Some(10.0),
            ..Scenario::new(
                "slow_sink",
                Load::Sustained {
                    writers: 2,
                    run: Duration::from_secs(20),
                },
            )
        })
        .await;
    }

    /// Changes written while the pipeline is down, replayed after a restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "benchmark: run in release with --nocapture"]
    async fn latency_recovery_catch_up() {
        const ROWS: i64 = 200_000;
        let client = connect().await;
        let capture = Capture::new(&client).await;
        let storage = tempfile::tempdir().unwrap();
        let statements = vec![
            capture.source("orders", ORDER_COLUMNS, ""),
            capture.upsert_sink("orders_mirror", "orders", "id"),
        ];
        {
            let db = open_db(storage.path(), 1_000).await;
            execute_all(&db, &statements).await;
            db.start().await.expect("start");
            client
                .batch_execute(&insert(&capture.table, 0))
                .await
                .unwrap();
            converge(&client, &capture).await;
            assert!(db.checkpoint().await.unwrap().success);
            db.shutdown().await.expect("shutdown");
        }
        for chunk in 0..ROWS / 100 {
            client
                .batch_execute(&format!(
                    "INSERT INTO {} SELECT g, 'down', 0 FROM generate_series({}, {}) g",
                    capture.table,
                    chunk * 100 + 1,
                    chunk * 100 + 100
                ))
                .await
                .unwrap();
        }
        let lag: i64 = client
            .query_one(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint \
                 FROM pg_replication_slots WHERE starts_with(slot_name::text, $1)",
                &[&format!("{}_", capture.slot)],
            )
            .await
            .unwrap()
            .get(0);
        let restarted = Instant::now();
        let db = loop {
            let db = open_db(storage.path(), 1_000).await;
            match first_error(&db, &statements).await {
                None => break db,
                Some(error) if error.contains("LDB-0014") => {
                    drop(db);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Some(error) => panic!("{error}"),
            }
        };
        db.start().await.expect("restart");
        let started = restarted.elapsed();
        converge(&client, &capture).await;
        let caught_up = restarted.elapsed();
        #[allow(clippy::cast_precision_loss)]
        let rate = ROWS as f64 / caught_up.as_secs_f64();
        println!(
            "LATENCY recovery_catch_up: {ROWS} rows in {} transactions written while down \
             ({} KiB of WAL behind the slot); restart took {:.2}s, caught up {:.2}s after \
             restart ({rate:.0} rows/s)",
            ROWS / 100,
            lag >> 10,
            started.as_secs_f64(),
            caught_up.as_secs_f64()
        );
        db.shutdown().await.expect("shutdown");
    }
}
