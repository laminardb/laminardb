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
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(300),
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
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let db = open(storage).await;
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

/// One captured table with its own slot and publication, plus a mirror table name.
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

    async fn drop_slot(&self, client: &tokio_postgres::Client) {
        let deadline = tokio::time::Instant::now() + CONVERGE;
        while tokio::time::Instant::now() < deadline {
            let dropped = client
                .execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                     WHERE slot_name = $1 AND NOT active",
                    &[&self.slot],
                )
                .await;
            let remaining = client
                .query_one(
                    "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot],
                )
                .await
                .unwrap()
                .get::<_, i64>(0);
            if dropped.is_ok() && remaining == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
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
    capture.drop_slot(&client).await;
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
        let slots = client
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&capture.slot],
            )
            .await
            .unwrap()
            .get::<_, i64>(0);
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
    capture.drop_slot(&client).await;
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
        let slots = client
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&capture.slot],
            )
            .await
            .unwrap()
            .get::<_, i64>(0);
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
    capture.drop_slot(&client).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_deferrable_extra_unique_constraints_are_rejected_before_data_moves() {
    let Some(client) = postgres().await else {
        return;
    };
    for constraint in ["", "NOT DEFERRABLE"] {
        let capture = Capture::new(&client).await;
        let storage = tempfile::tempdir().unwrap();
        let error = unique_mirror(&client, &capture, storage.path(), constraint)
            .await
            .err()
            .expect("a non-deferrable extra unique constraint is rejected");
        assert!(error.contains("DEFERRABLE"), "{error}");
        let slots = client
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&capture.slot],
            )
            .await
            .unwrap()
            .get::<_, i64>(0);
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
