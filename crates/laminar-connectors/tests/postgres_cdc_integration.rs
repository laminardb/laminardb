//! PostgreSQL CDC against a real logical-replication server, through the public connector API.
//!
//! Needs `docker compose -f tests/docker/postgres-cdc-compose.yml up -d --wait`.
//! `LAMINAR_TEST_POSTGRES_PORT` selects the server (15532 = PostgreSQL 17, 15533 = 18). Tests
//! skip when the server is unreachable unless `LAMINAR_REQUIRE_POSTGRES_CDC=1`.
//!
//! Run with:
//! `cargo test -p laminar-connectors --no-default-features --features postgres-cdc --test postgres_cdc_integration -- --test-threads=1`

#![cfg(feature = "postgres-cdc")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::Array;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceBatch, SourceBatchCursor, SourceConnector, SourceMutation,
    SourcePosition, SourceStart,
};
use laminar_connectors::postgres::{
    Lsn, PostgresCdcConfig, PostgresCdcSource, PostgresLookupSource, PostgresLookupSourceConfig,
};
use laminar_core::checkpoint::CheckpointAttempt;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, NoTls};

const REQUIRE_ENV: &str = "LAMINAR_REQUIRE_POSTGRES_CDC";
const PORT_ENV: &str = "LAMINAR_TEST_POSTGRES_PORT";
const PASSWORD: &str = "laminar-test-secret";
const WAIT: Duration = Duration::from_secs(30);

static NEXT: AtomicU64 = AtomicU64::new(0);

fn port() -> u16 {
    std::env::var(PORT_ENV).ok().map_or(15532, |port| {
        port.parse().expect("LAMINAR_TEST_POSTGRES_PORT")
    })
}

async fn admin() -> Option<Client> {
    admin_on(port()).await
}

async fn admin_on(port: u16) -> Option<Client> {
    let connection =
        format!("host=127.0.0.1 port={port} user=laminar password={PASSWORD} dbname=cdc");
    match tokio_postgres::connect(&connection, NoTls).await {
        Ok((client, driver)) => {
            tokio::spawn(async move {
                let _ = driver.await;
            });
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

/// One isolated table, publication, and slot prefix. Dropping it ends sessions on and drops every
/// slot under the prefix, so a failed test leaks none.
struct Fixture {
    admin: Client,
    port: u16,
    table: String,
    slot: String,
    publication: String,
}

/// A name suffix unique across test runs against the long-lived fixture.
fn unique_id() -> String {
    format!(
        "{:x}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            % 0xffff_ffff,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

const PREFIX_SLOTS: &str = "SELECT slot_name::text, confirmed_flush_lsn::text, active \
     FROM pg_replication_slots WHERE starts_with(slot_name::text, $1)";

impl Fixture {
    async fn new(columns: &str) -> Option<Self> {
        Self::on(port(), columns).await
    }

    async fn on(port: u16, columns: &str) -> Option<Self> {
        let admin = admin_on(port).await?;
        let id = unique_id();
        let fixture = Self {
            admin,
            port,
            table: format!("cdc_{id}"),
            slot: format!("slot_{id}"),
            publication: format!("pub_{id}"),
        };
        fixture
            .exec(&format!(
                "CREATE TABLE {table} ({columns}); \
                 ALTER TABLE {table} REPLICA IDENTITY FULL; \
                 CREATE PUBLICATION {publication} FOR TABLE {table};",
                table = fixture.table,
                publication = fixture.publication
            ))
            .await;
        Some(fixture)
    }

    async fn exec(&self, sql: &str) {
        self.admin
            .batch_execute(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }

    fn config(
        &self,
        schema: &Schema,
        primary_key: &[&str],
        options: &[(&str, &str)],
    ) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("postgres-cdc");
        for (key, value) in [
            ("host", "127.0.0.1"),
            ("database", "cdc"),
            ("username", "laminar"),
            ("password", PASSWORD),
            ("ssl.mode", "disable"),
            ("max.buffered.bytes", "4194304"),
        ] {
            config.set(key, value);
        }
        config.set("port", self.port.to_string());
        config.set("slot.name", &self.slot);
        config.set("publication", &self.publication);
        config.set("table", format!("public.{}", self.table));
        config.set("_arrow_schema", encode_arrow_schema_ipc(schema));
        config.set("_primary_key_columns", primary_key.join(","));
        for (key, value) in options {
            config.set(*key, *value);
        }
        config
    }

    /// Slots under this fixture's `slot.name` prefix: confirmed position and whether active.
    async fn slots(&self) -> BTreeMap<String, (Option<Lsn>, bool)> {
        self.admin
            .query(PREFIX_SLOTS, &[&format!("{}_", self.slot)])
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                let lsn: Option<String> = row.get(1);
                (
                    row.get(0),
                    (lsn.map(|lsn| lsn.parse().unwrap()), row.get(2)),
                )
            })
            .collect()
    }

    async fn slot(&self, name: &str) -> Option<(Option<Lsn>, bool)> {
        self.slots().await.remove(name)
    }

    async fn current_wal_lsn(&self) -> Lsn {
        let lsn: String = self
            .admin
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .unwrap()
            .get(0);
        lsn.parse().unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let (port, prefix) = (self.port, format!("{}_", self.slot));
        let _ = std::thread::spawn(move || drop_test_slots(port, &prefix)).join();
    }
}

/// Test cleanup only: end every session on, then drop, every slot whose name starts with
/// `prefix`.
fn drop_test_slots(port: u16, prefix: &str) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    runtime.block_on(async {
        let Some(admin) = admin_on(port).await else {
            return;
        };
        let _ = timeout(WAIT, async {
            loop {
                let _ = admin
                    .execute(
                        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                         WHERE starts_with(slot_name::text, $1) AND active_pid IS NOT NULL",
                        &[&prefix],
                    )
                    .await;
                let _ = admin
                    .execute(
                        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                         WHERE starts_with(slot_name::text, $1) AND NOT active",
                        &[&prefix],
                    )
                    .await;
                let remaining: i64 = admin
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
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
    });
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("qty", DataType::Int32, true),
    ]))
}

const ORDERS: &str = "id bigint PRIMARY KEY, label text, qty integer";

fn start(config: &ConnectorConfig) -> SourceStart {
    SourceStart::new(
        config.clone(),
        SourcePosition::Initial,
        DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap()
}

fn resume(config: &ConnectorConfig, checkpoint: SourceCheckpoint) -> SourceStart {
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

fn phase(cursor: &SourceCheckpoint) -> &str {
    cursor.get_metadata("phase").unwrap_or_default()
}

fn slot_of(cursor: &SourceCheckpoint) -> String {
    cursor.get_metadata("slot").expect("slot").to_string()
}

/// Commit cursors as the engine does until the source has its slot and a committed cursor
/// naming it has opened its snapshot or stream gate. No row may arrive before.
async fn commit_until_ready(source: &mut PostgresCdcSource) {
    timeout(WAIT, async {
        loop {
            let cursor = source.try_checkpoint().unwrap().expect("a cursor");
            source.notify_epoch_committed(1, &cursor).await.unwrap();
            if phase(&cursor) != "claimed" {
                return;
            }
            assert!(
                source.poll_batch(64).await.unwrap().is_none(),
                "no row before the slot exists"
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the claimed slot was not installed");
}

/// Poll until the cursor reaches `wanted` without committing it, as when the process dies first.
async fn poll_until_phase(source: &mut PostgresCdcSource, wanted: &str) -> SourceCheckpoint {
    timeout(WAIT, async {
        loop {
            assert!(source.poll_batch(64).await.unwrap().is_none());
            let cursor = source.try_checkpoint().unwrap().expect("a cursor");
            if phase(&cursor) == wanted {
                return cursor;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("phase not reached")
}

async fn started(request: SourceStart) -> PostgresCdcSource {
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    source.start(request).await.expect("source start");
    commit_until_ready(&mut source).await;
    source
}

/// Current-row state rebuilt from keyed puts and tombstones.
type Mirror = BTreeMap<i64, (Option<String>, Option<i32>)>;

fn apply(mirror: &mut Mirror, batch: &SourceBatch) {
    let ids = batch.records.column(0).as_primitive::<Int64Type>();
    let labels = batch.records.column(1).as_string::<i32>();
    let qty = batch.records.column(2).as_primitive::<Int32Type>();
    for row in 0..batch.num_rows() {
        match batch
            .mutations()
            .map_or(SourceMutation::Put, |mutations| mutations[row])
        {
            SourceMutation::Put => {
                mirror.insert(
                    ids.value(row),
                    (
                        labels.is_valid(row).then(|| labels.value(row).to_string()),
                        qty.is_valid(row).then(|| qty.value(row)),
                    ),
                );
            }
            SourceMutation::Tombstone => {
                mirror.remove(&ids.value(row));
            }
        }
    }
}

async fn table_state(fixture: &Fixture) -> Mirror {
    fixture
        .admin
        .query(
            &format!("SELECT id, label, qty FROM {} ORDER BY id", fixture.table),
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), (row.get(1), row.get(2))))
        .collect()
}

/// Poll until `done` holds over the batches seen so far, returning the last cursor.
async fn poll_until(
    source: &mut PostgresCdcSource,
    mirror: &mut Mirror,
    mut done: impl FnMut(&Mirror) -> bool,
) -> Option<SourceCheckpoint> {
    let mut last = None;
    timeout(WAIT, async {
        loop {
            match source.poll_batch(64).await.expect("poll") {
                Some(mut batch) => {
                    if let Some(SourceBatchCursor::Complete(cursor)) = batch.take_cursor() {
                        last = Some(cursor);
                    }
                    apply(mirror, &batch);
                }
                None => sleep(Duration::from_millis(10)).await,
            }
            if done(mirror) {
                return;
            }
        }
    })
    .await
    .expect("PostgreSQL CDC did not converge");
    last
}

/// The next batch the source emits.
async fn next_batch(source: &mut PostgresCdcSource) -> SourceBatch {
    timeout(WAIT, async {
        loop {
            if let Some(batch) = source.poll_batch(64).await.expect("poll") {
                return batch;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("no batch arrived")
}

/// Poll until the source has left the snapshot and emitted every row up to the server's WAL end.
async fn drain_to(source: &mut PostgresCdcSource, mirror: &mut Mirror, fixture: &Fixture) {
    let expected = table_state(fixture).await;
    poll_until(source, mirror, |mirror| *mirror == expected).await;
}

#[tokio::test]
async fn snapshot_hands_off_to_wal_without_gap_or_overlap() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, 'seed', g::int FROM generate_series(1, 2000) g",
            fixture.table
        ))
        .await;
    let config = fixture.config(&orders_schema(), &["id"], &[]);
    let mut source = started(start(&config)).await;
    let inside = source.try_checkpoint().unwrap().expect("snapshot cursor");
    assert_eq!(phase(&inside), "snapshot");
    assert_eq!(
        inside.get_offset("lsn"),
        inside.get_metadata("consistent_point"),
        "a cursor inside the copy carries the stream position, from the slot's consistent point"
    );

    // The first chunk is emitted; these changes commit after the slot's consistent point and
    // reach the target either through WAL or through later chunks, once each.
    let mut mirror = Mirror::new();
    let first = next_batch(&mut source).await;
    apply(&mut mirror, &first);
    fixture
        .exec(&format!(
            "UPDATE {t} SET qty = qty + 1000, label = 'updated' WHERE id <= 100; \
             DELETE FROM {t} WHERE id BETWEEN 101 AND 200; \
             INSERT INTO {t} SELECT g, 'late', 7 FROM generate_series(2001, 2100) g; \
             UPDATE {t} SET id = id + 10000 WHERE id BETWEEN 1901 AND 1950;",
            t = fixture.table
        ))
        .await;
    drain_to(&mut source, &mut mirror, &fixture).await;
    assert_eq!(mirror.len(), 2000);
    assert_eq!(mirror[&1], (Some("updated".into()), Some(1001)));
    assert!(!mirror.contains_key(&150));
    assert!(mirror.contains_key(&11_901) && !mirror.contains_key(&1901));
    assert!(source.try_checkpoint().unwrap().is_some());
    source.close().await.unwrap();
}

#[tokio::test]
async fn changes_from_now_skip_existing_rows() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} VALUES (1, 'old', 1)",
            fixture.table
        ))
        .await;
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    fixture
        .exec(&format!(
            "INSERT INTO {t} VALUES (2, 'new', 2); UPDATE {t} SET qty = 9 WHERE id = 1",
            t = fixture.table
        ))
        .await;
    let mut mirror = Mirror::new();
    poll_until(&mut source, &mut mirror, |mirror| mirror.len() == 2).await;
    assert_eq!(
        mirror[&1],
        (Some("old".into()), Some(9)),
        "an update carries the full row"
    );
    assert_eq!(mirror[&2], (Some("new".into()), Some(2)));
    source.close().await.unwrap();
}

#[tokio::test]
async fn committed_cursor_resumes_and_feedback_advances_the_slot() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    fixture
        .exec(&format!("INSERT INTO {} VALUES (1, 'a', 1)", fixture.table))
        .await;
    let mut mirror = Mirror::new();
    let cursor = poll_until(&mut source, &mut mirror, |mirror| mirror.len() == 1)
        .await
        .expect("cursor");
    let committed: Lsn = cursor.get_offset("lsn").unwrap().parse().unwrap();
    let slot = slot_of(&cursor);
    source.notify_epoch_committed(1, &cursor).await.unwrap();
    timeout(WAIT, async {
        while fixture.slot(&slot).await.and_then(|slot| slot.0) < Some(committed) {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("durable feedback must reach the slot");

    fixture
        .exec(&format!("INSERT INTO {} VALUES (2, 'b', 2)", fixture.table))
        .await;
    poll_until(&mut source, &mut mirror, |mirror| mirror.len() == 2).await;
    source.close().await.unwrap();

    // Row 2 was emitted but never committed, so the resumed source replays it.
    let mut resumed = started(resume(&config, cursor)).await;
    let mut replayed = Mirror::new();
    poll_until(&mut resumed, &mut replayed, |mirror| {
        mirror.contains_key(&2)
    })
    .await;
    assert!(
        !replayed.contains_key(&1),
        "committed rows are not replayed"
    );
    resumed.close().await.unwrap();
}

#[tokio::test]
async fn durable_feedback_reaches_postgres_while_intake_is_blocked() {
    let Some(fixture) = Fixture::new("id bigint PRIMARY KEY, label text, qty integer").await else {
        return;
    };
    let mut config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    config.set("max.buffered.bytes", "1048576");
    let mut source = started(start(&config)).await;
    fixture
        .exec(&format!("INSERT INTO {} VALUES (1, 'a', 1)", fixture.table))
        .await;
    let mut mirror = Mirror::new();
    let cursor = poll_until(&mut source, &mut mirror, |mirror| mirror.len() == 1)
        .await
        .expect("cursor");
    let committed: Lsn = cursor.get_offset("lsn").unwrap().parse().unwrap();
    // Far more WAL than the raw budget: with no further polls the reader blocks on its queue.
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, repeat('x', 2000), g FROM generate_series(2, 5000) g",
            fixture.table
        ))
        .await;
    sleep(Duration::from_millis(500)).await;
    let slot = slot_of(&cursor);
    source.notify_epoch_committed(1, &cursor).await.unwrap();
    timeout(WAIT, async {
        while fixture.slot(&slot).await.and_then(|slot| slot.0) < Some(committed) {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("feedback must not wait for the blocked reader");
    source.close().await.unwrap();
}

#[tokio::test]
async fn idle_table_cursor_follows_keepalives_past_other_tables_writes() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    // Outside the publication: PostgreSQL 15+ skips these transactions and sends keepalives.
    let busy = format!("{}_busy", fixture.table);
    fixture
        .exec(&format!(
            "CREATE TABLE {busy} (id bigint PRIMARY KEY, note text)"
        ))
        .await;
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    fixture
        .exec(&format!("INSERT INTO {} VALUES (1, 'a', 1)", fixture.table))
        .await;
    let mut mirror = Mirror::new();
    poll_until(&mut source, &mut mirror, |mirror| mirror.len() == 1).await;
    for id in 0..200 {
        fixture
            .exec(&format!(
                "INSERT INTO {busy} VALUES ({id}, repeat('x', 1000))"
            ))
            .await;
    }
    let busy_end = fixture.current_wal_lsn().await;
    let mut next_id = 200;
    let cursor = timeout(WAIT, async {
        loop {
            assert!(source.poll_batch(64).await.expect("poll").is_none());
            let cursor = source.try_checkpoint().unwrap().expect("streaming cursor");
            let lsn: Lsn = cursor.get_offset("lsn").unwrap().parse().unwrap();
            if lsn >= busy_end {
                return cursor;
            }
            fixture
                .exec(&format!("INSERT INTO {busy} VALUES ({next_id}, 'x')"))
                .await;
            next_id += 1;
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("an idle source's cursor must pass the other table's writes");
    let committed: Lsn = cursor.get_offset("lsn").unwrap().parse().unwrap();
    let slot = slot_of(&cursor);
    source.notify_epoch_committed(1, &cursor).await.unwrap();
    timeout(WAIT, async {
        while fixture.slot(&slot).await.and_then(|slot| slot.0) < Some(committed) {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("durable feedback must release the other table's WAL");
    source.close().await.unwrap();

    // Row 2 commits between the committed cursor and the resume; row 3 after it.
    fixture
        .exec(&format!("INSERT INTO {} VALUES (2, 'b', 2)", fixture.table))
        .await;
    let mut resumed = started(resume(&config, cursor)).await;
    fixture
        .exec(&format!("INSERT INTO {} VALUES (3, 'c', 3)", fixture.table))
        .await;
    let mut delivered = Vec::new();
    timeout(WAIT, async {
        while !delivered.contains(&3) {
            match resumed.poll_batch(64).await.expect("poll") {
                Some(batch) => delivered.extend(
                    batch
                        .records
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .values()
                        .iter()
                        .copied(),
                ),
                None => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("changes after the committed cursor must arrive");
    assert_eq!(
        delivered,
        [2, 3],
        "each change after the cursor arrives once"
    );
    resumed.close().await.unwrap();
}

fn changelog_schema() -> SchemaRef {
    let mut fields = orders_schema().fields().to_vec();
    fields.push(Arc::new(Field::new("__weight", DataType::Int64, false)));
    Arc::new(Schema::new(fields))
}

/// A changelog source cannot resume inside the copy: a target fed before the restart may keep a
/// row the failed attempt copied and that was deleted before the range was copied again.
#[tokio::test]
async fn fresh_starts_leave_existing_slots_alone_and_an_interrupted_snapshot_fails_closed() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, 'seed', 1 FROM generate_series(1, 500) g",
            fixture.table
        ))
        .await;
    let config = fixture.config(
        &changelog_schema(),
        &["id"],
        &[("output.mode", "changelog")],
    );
    let mut source = started(start(&config)).await;
    let mut batch = next_batch(&mut source).await;
    let Some(SourceBatchCursor::Complete(snapshot_cursor)) = batch.take_cursor() else {
        panic!("snapshot rows carry a cursor");
    };
    assert_eq!(phase(&snapshot_cursor), "snapshot");
    source.close().await.unwrap();
    let slot = slot_of(&snapshot_cursor);
    let slot_before = fixture.slot(&slot).await.expect("the created slot is kept");

    let registry = prometheus::Registry::new();
    let mut fresh = PostgresCdcSource::new(PostgresCdcConfig::default(), Some(&registry));
    fresh
        .start(start(&config))
        .await
        .expect("a fresh start claims a new slot name");
    assert_ne!(slot_of(&fresh.try_checkpoint().unwrap().unwrap()), slot);
    assert_eq!(orphaned_slots(&registry), "postgres_cdc_orphaned_slots 1");
    fresh.close().await.unwrap();

    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = resumed
        .start(resume(&config, snapshot_cursor))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("changelog source cannot resume"), "{error}");
    assert!(
        error.contains(&format!("SELECT pg_drop_replication_slot('{slot}')")),
        "{error}"
    );
    assert_eq!(fixture.slot(&slot).await, Some(slot_before));
}

#[tokio::test]
async fn contract_violations_fail_before_creating_a_slot() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let schema = orders_schema();
    let cases: Vec<(&str, ConnectorConfig, &str)> = vec![
        (
            "ALTER TABLE {t} REPLICA IDENTITY DEFAULT",
            fixture.config(&schema, &["id"], &[]),
            "REPLICA IDENTITY FULL",
        ),
        (
            "ALTER PUBLICATION {p} SET (publish = 'insert, update, delete')",
            fixture.config(&schema, &["id"], &[]),
            "TRUNCATE",
        ),
        (
            "CREATE TABLE {t}_other (id int PRIMARY KEY); ALTER PUBLICATION {p} ADD TABLE {t}_other",
            fixture.config(&schema, &["id"], &[]),
            "exactly the captured table",
        ),
        (
            "ALTER PUBLICATION {p} SET TABLE {t} WHERE (qty > 0)",
            fixture.config(&schema, &["id"], &[]),
            "row filter",
        ),
        ("", fixture.config(&schema, &["label"], &[]), "PRIMARY KEY"),
        (
            "",
            fixture.config(
                &Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("qty", DataType::Int64, true),
                ]),
                &["id"],
                &[],
            ),
            "qty",
        ),
        (
            "ALTER TABLE {t} DROP CONSTRAINT {t}_pkey; \
             ALTER TABLE {t} ADD PRIMARY KEY (id) DEFERRABLE",
            fixture.config(&schema, &["id"], &[]),
            "DEFERRABLE",
        ),
    ];
    for (setup, config, expected) in cases {
        let reset = format!(
            "ALTER TABLE {t} REPLICA IDENTITY FULL; DROP TABLE IF EXISTS {t}_other; \
             ALTER PUBLICATION {p} SET TABLE {t}; \
             ALTER PUBLICATION {p} SET (publish = 'insert, update, delete, truncate');",
            t = fixture.table,
            p = fixture.publication
        );
        fixture.exec(&reset).await;
        if !setup.is_empty() {
            fixture
                .exec(
                    &setup
                        .replace("{t}", &fixture.table)
                        .replace("{p}", &fixture.publication),
                )
                .await;
        }
        let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
        let error = source.start(start(&config)).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{expected}: {error}");
        assert!(
            fixture.slots().await.is_empty(),
            "{expected}: no slot may be created"
        );
    }
}

/// Columns the primary-key index only `INCLUDE`s are not key columns: the declared key is the
/// index's key columns alone, and a change to an included column keeps the row's key.
#[tokio::test]
async fn included_columns_of_the_primary_key_index_are_not_key_columns() {
    let Some(fixture) =
        Fixture::new("id bigint, label text, qty integer, PRIMARY KEY (id) INCLUDE (label)").await
    else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    fixture
        .exec(&format!(
            "INSERT INTO {t} VALUES (1, 'a', 1); UPDATE {t} SET label = 'b' WHERE id = 1;",
            t = fixture.table
        ))
        .await;
    let expected = table_state(&fixture).await;
    let mut mirror = Mirror::new();
    poll_until(&mut source, &mut mirror, |mirror| *mirror == expected).await;
    source.close().await.unwrap();
}

#[tokio::test]
async fn unchanged_toast_values_and_nulls_survive_real_wal() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    // Random text defeats compression, so the value is stored out of line in TOAST.
    fixture
        .exec(&format!(
            "INSERT INTO {t} SELECT 1, string_agg(md5(g::text), ''), NULL FROM generate_series(1, 2000) g; \
             UPDATE {t} SET qty = 5 WHERE id = 1;",
            t = fixture.table
        ))
        .await;
    let expected = table_state(&fixture).await;
    let mut mirror = Mirror::new();
    poll_until(&mut source, &mut mirror, |mirror| *mirror == expected).await;
    assert_eq!(mirror[&1].0.as_ref().map(String::len), Some(64_000));
    source.close().await.unwrap();
}

#[tokio::test]
async fn column_change_stops_intake_at_the_first_change_after_it() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    let mut mirror = Mirror::new();
    fixture
        .exec(&format!("INSERT INTO {} VALUES (1, 'a', 1)", fixture.table))
        .await;
    drain_to(&mut source, &mut mirror, &fixture).await;
    fixture
        .exec(&format!(
            "ALTER TABLE {} ADD COLUMN extra integer",
            fixture.table
        ))
        .await;
    fixture
        .exec(&format!(
            "INSERT INTO {} VALUES (2, 'b', 2, 2)",
            fixture.table
        ))
        .await;
    let error = timeout(WAIT, async {
        loop {
            match source.poll_batch(64).await {
                Ok(Some(batch)) => apply(&mut mirror, &batch),
                Ok(None) => sleep(Duration::from_millis(10)).await,
                Err(error) => return error,
            }
        }
    })
    .await
    .expect("a column change must stop the source");
    assert!(
        error
            .to_string()
            .contains("no longer matches the bound layout"),
        "{error}"
    );
    assert!(mirror.contains_key(&1) && !mirror.contains_key(&2));
    source.close().await.unwrap();
}

#[tokio::test]
async fn truncate_stops_intake_before_feedback() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    fixture
        .exec(&format!(
            "INSERT INTO {t} VALUES (1, 'a', 1); TRUNCATE {t};",
            t = fixture.table
        ))
        .await;
    let error = timeout(WAIT, async {
        loop {
            match source.poll_batch(64).await {
                Ok(_) => sleep(Duration::from_millis(10)).await,
                Err(error) => return error,
            }
        }
    })
    .await
    .expect("TRUNCATE must stop the source");
    assert!(error.to_string().contains("TRUNCATE"), "{error}");
    source.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_and_wal_decode_every_supported_type_identically() {
    let Some(fixture) = Fixture::new(
        "id bigint PRIMARY KEY, b boolean, s smallint, i integer, f4 real, f8 double precision, \
         n numeric(12,3), t text, v varchar(10), c char(3), j jsonb, u uuid, by bytea, d date, \
         tm time, ts timestamp, tz timestamptz",
    )
    .await
    else {
        return;
    };
    let values = "true, -7, 42, 1.5, 0.30000000000000004, -123456789.012, 'héllo', 'v', 'ab', \
                  '{\"k\": [1, 2]}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\x00ff', \
                  '2024-02-29', '23:59:59.999999', '2024-02-29 12:34:56.789012', \
                  '2024-02-29 12:34:56.789012+05:30'";
    fixture
        .exec(&format!(
            "INSERT INTO {} VALUES (1, {values})",
            fixture.table
        ))
        .await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("b", DataType::Boolean, true),
        Field::new("s", DataType::Int16, true),
        Field::new("i", DataType::Int32, true),
        Field::new("f4", DataType::Float32, true),
        Field::new("f8", DataType::Float64, true),
        Field::new("n", DataType::Decimal128(12, 3), true),
        Field::new("t", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
        Field::new("c", DataType::Utf8, true),
        Field::new("j", DataType::Utf8, true),
        Field::new("u", DataType::Utf8, true),
        Field::new("by", DataType::Binary, true),
        Field::new("d", DataType::Date32, true),
        Field::new(
            "tm",
            DataType::Time64(arrow_schema::TimeUnit::Microsecond),
            true,
        ),
        Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "tz",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let config = fixture.config(&schema, &["id"], &[]);
    let mut source = started(start(&config)).await;
    let mut batches = Vec::new();
    let mut inserted = false;
    timeout(WAIT, async {
        while batches.len() < 2 {
            // Inserted once the copy is complete, so the row arrives through WAL.
            let copied = phase(&source.try_checkpoint().unwrap().unwrap()) == "streaming";
            if !inserted && batches.len() == 1 && copied {
                fixture
                    .exec(&format!(
                        "INSERT INTO {} VALUES (2, {values})",
                        fixture.table
                    ))
                    .await;
                inserted = true;
            }
            match source.poll_batch(64).await.unwrap() {
                Some(batch) => batches.push(batch.records),
                None => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("snapshot row and WAL row");
    let (snapshot, wal) = (&batches[0], &batches[1]);
    for column in 1..schema.fields().len() {
        assert_eq!(
            snapshot.column(column).slice(0, 1).to_data(),
            wal.column(column).slice(0, 1).to_data(),
            "column {} must decode identically from snapshot and WAL",
            schema.field(column).name()
        );
    }
    let tz = wal
        .column(16)
        .as_primitive::<arrow_array::types::TimestampMicrosecondType>();
    assert_eq!(
        tz.value(0),
        1_709_190_296_789_012,
        "timestamptz is the UTC instant"
    );
    source.close().await.unwrap();
}

#[tokio::test]
async fn resume_rejects_publication_drift() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    let cursor = source.try_checkpoint().unwrap().expect("streaming cursor");
    source.close().await.unwrap();
    fixture
        .exec(&format!(
            "DROP PUBLICATION {p}; CREATE PUBLICATION {p} FOR TABLE {t}",
            p = fixture.publication,
            t = fixture.table
        ))
        .await;
    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = resumed.start(resume(&config, cursor)).await.unwrap_err();
    assert!(error.to_string().contains("drifted"), "{error}");
}

/// The orphan gauge's exposition line.
fn orphaned_slots(registry: &prometheus::Registry) -> String {
    use prometheus::Encoder;
    let mut text = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&registry.gather(), &mut text)
        .unwrap();
    String::from_utf8(text)
        .unwrap()
        .lines()
        .find(|line| line.starts_with("postgres_cdc_orphaned_slots "))
        .unwrap_or_default()
        .to_string()
}

/// Hold `slot` with a replication session announcing `application_name`.
async fn hold_slot(
    fixture: &Fixture,
    slot: &str,
    application_name: &str,
) -> pgwire_replication::ReplicationClient {
    let holder =
        pgwire_replication::ReplicationClient::connect(pgwire_replication::ReplicationConfig {
            host: "127.0.0.1".into(),
            port: port(),
            user: "laminar".into(),
            password: PASSWORD.into(),
            database: "cdc".into(),
            slot: slot.into(),
            publication: fixture.publication.clone(),
            application_name: application_name.into(),
            ..pgwire_replication::ReplicationConfig::default()
        })
        .await
        .expect("hold the slot");
    assert_eq!(fixture.slot(slot).await.map(|slot| slot.1), Some(true));
    holder
}

/// Ids delivered by `source` until `last` arrives, in delivery order.
async fn delivered_until(source: &mut PostgresCdcSource, last: i64) -> Vec<i64> {
    let mut delivered = Vec::new();
    timeout(WAIT, async {
        while !delivered.contains(&last) {
            match source.poll_batch(64).await.expect("poll") {
                Some(batch) => delivered.extend(
                    batch
                        .records
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .values()
                        .iter()
                        .copied(),
                ),
                None => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("the change must arrive");
    delivered
}

#[tokio::test]
async fn the_slot_follows_its_committed_claim_and_rows_follow_the_committed_snapshot_cursor() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, 'seed', g::int FROM generate_series(1, 50) g",
            fixture.table
        ))
        .await;
    let config = fixture.config(&orders_schema(), &["id"], &[]);
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    source.start(start(&config)).await.unwrap();
    let claimed = source.try_checkpoint().unwrap().expect("a claim");
    assert_eq!(phase(&claimed), "claimed");
    assert!(claimed.get_offset("lsn").is_none());
    let slot = slot_of(&claimed);
    assert!(
        slot.len() == fixture.slot.len() + 17 && slot.starts_with(&format!("{}_", fixture.slot)),
        "{slot}"
    );
    for _ in 0..20 {
        assert!(source.poll_batch(64).await.unwrap().is_none());
        sleep(Duration::from_millis(25)).await;
    }
    assert!(
        fixture.slots().await.is_empty(),
        "no slot before the claim commits"
    );

    source.notify_epoch_committed(1, &claimed).await.unwrap();
    let snapshot = poll_until_phase(&mut source, "snapshot").await;
    assert_eq!(slot_of(&snapshot), slot);
    assert_eq!(
        fixture.slots().await.into_keys().collect::<Vec<_>>(),
        std::slice::from_ref(&slot)
    );
    for _ in 0..20 {
        assert!(
            source.poll_batch(64).await.unwrap().is_none(),
            "no snapshot row before a cursor naming the slot commits"
        );
        sleep(Duration::from_millis(25)).await;
    }

    source.notify_epoch_committed(2, &snapshot).await.unwrap();
    let mut mirror = Mirror::new();
    drain_to(&mut source, &mut mirror, &fixture).await;
    assert_eq!(mirror.len(), 50);
    source.close().await.unwrap();
}

#[tokio::test]
async fn a_claim_that_committed_before_a_crash_creates_its_slot_on_restart() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut first = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    first.start(start(&config)).await.unwrap();
    let claimed = first.try_checkpoint().unwrap().expect("a claim");
    first.close().await.unwrap();
    assert!(fixture.slots().await.is_empty());

    let mut resumed = started(resume(&config, claimed.clone())).await;
    assert_eq!(
        fixture.slots().await.into_keys().collect::<Vec<_>>(),
        [slot_of(&claimed)]
    );
    fixture
        .exec(&format!("INSERT INTO {} VALUES (1, 'a', 1)", fixture.table))
        .await;
    assert_eq!(delivered_until(&mut resumed, 1).await, [1]);
    resumed.close().await.unwrap();
}

/// Rows wait for a committed cursor naming the slot, so a crash before it emitted nothing, and the
/// restart from the claim adopts the slot and streams from its consistent point exactly once.
#[tokio::test]
async fn never_mode_emits_nothing_before_a_cursor_naming_its_slot_commits() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut first = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    first.start(start(&config)).await.unwrap();
    let claimed = first.try_checkpoint().unwrap().expect("a claim");
    first.notify_epoch_committed(1, &claimed).await.unwrap();
    let held = poll_until_phase(&mut first, "streaming").await;
    let slot = slot_of(&claimed);
    assert_eq!(slot_of(&held), slot);
    fixture
        .exec(&format!(
            "INSERT INTO {t} VALUES (1, 'a', 1); INSERT INTO {t} VALUES (2, 'b', 2);",
            t = fixture.table
        ))
        .await;
    for _ in 0..20 {
        assert!(
            first.poll_batch(64).await.unwrap().is_none(),
            "no row before a streaming cursor naming the slot commits"
        );
        sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        fixture.slot(&slot).await.map(|slot| slot.1),
        Some(false),
        "the stream is not even opened"
    );
    first.close().await.unwrap();
    let (confirmed, _) = fixture.slot(&slot).await.expect("the slot is kept");

    let registry = prometheus::Registry::new();
    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), Some(&registry));
    resumed.start(resume(&config, claimed)).await.unwrap();
    let cursor = resumed.try_checkpoint().unwrap().expect("a cursor");
    assert_eq!(phase(&cursor), "streaming");
    assert_eq!(slot_of(&cursor), slot);
    assert_eq!(
        cursor.get_offset("lsn"),
        confirmed.map(|lsn| lsn.to_string()).as_deref(),
        "the adopted slot streams from its consistent point"
    );
    assert!(resumed.poll_batch(64).await.unwrap().is_none());
    resumed.notify_epoch_committed(2, &cursor).await.unwrap();
    assert_eq!(
        delivered_until(&mut resumed, 2).await,
        [1, 2],
        "each change after the consistent point arrives once"
    );
    assert_eq!(fixture.slots().await.len(), 1, "adopted, not replaced");
    assert_eq!(orphaned_slots(&registry), "postgres_cdc_orphaned_slots 0");
    resumed.close().await.unwrap();
}

/// A crash after the claimed slot exists but before a cursor naming its copy commits emitted
/// nothing. The restart adopts that slot from its consistent point and takes the copy's floor
/// from a temporary probe slot, so changes committed meanwhile reach the target exactly once.
#[tokio::test]
async fn initial_mode_adopts_its_claimed_slot_with_a_probe_floor_and_copies_exactly() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, 'seed', g::int FROM generate_series(1, 30) g",
            fixture.table
        ))
        .await;
    let config = fixture.config(&orders_schema(), &["id"], &[]);
    let mut first = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    first.start(start(&config)).await.unwrap();
    let claimed = first.try_checkpoint().unwrap().expect("a claim");
    first.notify_epoch_committed(1, &claimed).await.unwrap();
    let slot = slot_of(&poll_until_phase(&mut first, "snapshot").await);
    first.close().await.unwrap();
    fixture
        .exec(&format!(
            "UPDATE {t} SET label = 'down' WHERE id <= 3; DELETE FROM {t} WHERE id = 4; \
             INSERT INTO {t} VALUES (31, 'down', 31)",
            t = fixture.table
        ))
        .await;

    let registry = prometheus::Registry::new();
    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), Some(&registry));
    resumed.start(resume(&config, claimed)).await.unwrap();
    commit_until_ready(&mut resumed).await;
    let cursor = resumed.try_checkpoint().unwrap().expect("a cursor");
    assert_eq!(phase(&cursor), "snapshot");
    assert_eq!(slot_of(&cursor), slot, "the claimed slot is adopted");
    assert!(
        cursor.get_metadata("visibility_floor").is_some(),
        "the probe slot provides the floor"
    );
    assert_eq!(orphaned_slots(&registry), "postgres_cdc_orphaned_slots 0");
    fixture
        .exec(&format!(
            "UPDATE {} SET label = 'later' WHERE id <= 5",
            fixture.table
        ))
        .await;
    let mut mirror = Mirror::new();
    drain_to(&mut resumed, &mut mirror, &fixture).await;
    assert_eq!(mirror.len(), 30);
    assert_eq!(
        fixture.slots().await.into_keys().collect::<Vec<_>>(),
        [slot],
        "adopted, not replaced"
    );
    let probes: i64 = fixture
        .admin
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'laminar_probe_%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(probes, 0, "the probe slot ends with its session");
    resumed.close().await.unwrap();
}

#[tokio::test]
async fn a_stale_session_of_the_claim_is_ended_and_its_slot_adopted() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut first = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    first.start(start(&config)).await.unwrap();
    let claimed = first.try_checkpoint().unwrap().expect("a claim");
    commit_until_ready(&mut first).await;
    first.close().await.unwrap();
    let slot = slot_of(&claimed);
    let claim = claimed.get_metadata("claim").unwrap();
    let stale = hold_slot(&fixture, &slot, &format!("laminar:{claim}:deadbeef")).await;

    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    resumed
        .start(resume(&config, claimed))
        .await
        .expect("the stale session is ended and the slot adopted");
    assert_eq!(
        phase(&resumed.try_checkpoint().unwrap().unwrap()),
        "streaming"
    );
    commit_until_ready(&mut resumed).await;
    fixture
        .exec(&format!("INSERT INTO {} VALUES (1, 'a', 1)", fixture.table))
        .await;
    assert_eq!(delivered_until(&mut resumed, 1).await, [1]);
    drop(stale);
    resumed.close().await.unwrap();
}

#[tokio::test]
async fn a_slot_held_by_another_consumer_is_a_retryable_error_naming_it() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut first = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    first.start(start(&config)).await.unwrap();
    let claimed = first.try_checkpoint().unwrap().expect("a claim");
    commit_until_ready(&mut first).await;
    first.close().await.unwrap();
    let slot = slot_of(&claimed);
    let foreign = hold_slot(&fixture, &slot, "foreign-consumer").await;
    let pid: i32 = fixture
        .admin
        .query_one(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .get(0);

    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = resumed.start(resume(&config, claimed)).await.unwrap_err();
    assert!(error.is_transient(), "{error:?}");
    let text = error.to_string();
    for needle in [slot.as_str(), &format!("pid {pid}"), "foreign-consumer"] {
        assert!(text.contains(needle), "{needle}: {text}");
    }
    assert_eq!(
        fixture.slot(&slot).await.map(|slot| slot.1),
        Some(true),
        "another consumer's session is never ended"
    );
    drop(foreign);
}

#[tokio::test]
async fn an_unusable_claimed_slot_is_left_as_an_orphan_for_a_new_claim() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut first = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    first.start(start(&config)).await.unwrap();
    let claimed = first.try_checkpoint().unwrap().expect("a claim");
    first.close().await.unwrap();
    let slot = slot_of(&claimed);
    // LaminarDB never creates two-phase slots, so this one fails the adoption checks.
    fixture
        .exec(&format!(
            "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput', false, true)"
        ))
        .await;

    let registry = prometheus::Registry::new();
    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), Some(&registry));
    resumed.start(resume(&config, claimed)).await.unwrap();
    let reclaimed = resumed.try_checkpoint().unwrap().expect("a claim");
    assert_eq!(phase(&reclaimed), "claimed");
    assert_ne!(slot_of(&reclaimed), slot);
    assert_eq!(orphaned_slots(&registry), "postgres_cdc_orphaned_slots 1");
    commit_until_ready(&mut resumed).await;
    assert!(
        fixture.slots().await.contains_key(&slot),
        "the unusable slot is never dropped"
    );
    resumed.close().await.unwrap();
}

#[tokio::test]
async fn a_streaming_cursor_fails_closed_naming_its_exact_slot() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let config = fixture.config(&orders_schema(), &["id"], &[("snapshot.mode", "never")]);
    let mut source = started(start(&config)).await;
    let cursor = source.try_checkpoint().unwrap().expect("a cursor");
    assert_eq!(phase(&cursor), "streaming");
    source.close().await.unwrap();
    let slot = slot_of(&cursor);
    let reset = format!("SELECT pg_drop_replication_slot('{slot}')");

    fixture
        .exec(&format!(
            "SELECT pg_drop_replication_slot('{slot}'); \
             SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput', false, true);"
        ))
        .await;
    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = resumed
        .start(resume(&config, cursor.clone()))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("two_phase") && error.contains(&reset),
        "{error}"
    );

    fixture.exec(&reset).await;
    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = resumed
        .start(resume(&config, cursor))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("missing") && error.contains(&reset),
        "{error}"
    );
}

/// The initial copy must read every row and declared column that logical replication streams: a
/// role missing a column grant, or filtered by row-level security, fails before any slot exists.
#[tokio::test]
async fn a_copy_that_cannot_read_every_row_and_column_fails_before_any_slot() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let role = format!("rls_{}", unique_id());
    let table = fixture.table.as_str();
    fixture
        .exec(&format!(
            "CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}'; \
             GRANT SELECT (id, label) ON {table} TO {role};"
        ))
        .await;
    let mut config = fixture.config(&orders_schema(), &["id"], &[]);
    config.set("username", &role);
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let missing_column = source.start(start(&config)).await;
    fixture
        .exec(&format!(
            "GRANT SELECT ON {table} TO {role}; ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;"
        ))
        .await;
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let filtered = source.start(start(&config)).await;
    fixture
        .exec(&format!("DROP OWNED BY {role}; DROP ROLE {role};"))
        .await;

    let error = missing_column
        .expect_err("the copy would miss a column")
        .to_string();
    for needle in ["qty", table, "GRANT SELECT"] {
        assert!(error.contains(needle), "{needle}: {error}");
    }
    let error = filtered.expect_err("the copy would miss rows").to_string();
    for needle in ["row-level security", table, "BYPASSRLS"] {
        assert!(error.contains(needle), "{needle}: {error}");
    }
    assert!(fixture.slots().await.is_empty());
}

#[tokio::test]
async fn lookup_open_requires_a_usable_single_key_unique_index() {
    let Some(client) = admin().await else {
        return;
    };
    let suffix = unique_id();
    client
        .batch_execute(&format!(
            "CREATE TABLE lookup_unindexed_{suffix} (id BIGINT, payload TEXT); \
             CREATE TABLE lookup_nonunique_{suffix} (id BIGINT, payload TEXT); \
             CREATE INDEX ON lookup_nonunique_{suffix} (id); \
             CREATE TABLE lookup_unique_{suffix} (id BIGINT, payload TEXT, included TEXT); \
             CREATE UNIQUE INDEX ON lookup_unique_{suffix} (id) INCLUDE (included); \
             CREATE TABLE lookup_primary_{suffix} (id BIGINT PRIMARY KEY, payload TEXT);"
        ))
        .await
        .expect("create lookup admission fixtures");
    let lookup = |table: String| PostgresLookupSourceConfig {
        table,
        primary_key_columns: vec!["id".into()],
        properties: [
            ("host".to_string(), "127.0.0.1".to_string()),
            ("port".to_string(), port().to_string()),
            ("database".to_string(), "cdc".to_string()),
            ("user".to_string(), "laminar".to_string()),
            ("password".to_string(), PASSWORD.to_string()),
            ("ssl.mode".to_string(), "disable".to_string()),
        ]
        .into_iter()
        .collect(),
        pool_size: 2,
    };
    for table in ["lookup_unindexed", "lookup_nonunique"] {
        let error =
            match PostgresLookupSource::open(lookup(format!("public.{table}_{suffix}"))).await {
                Ok(_) => panic!("non-unique lookup key must be rejected"),
                Err(error) => error,
            };
        assert!(error.to_string().contains("unique index"), "{error}");
    }
    PostgresLookupSource::open(lookup(format!("public.lookup_unique_{suffix}")))
        .await
        .expect("single-key unique index with INCLUDE columns must be admitted");
    PostgresLookupSource::open(lookup(format!("public.lookup_primary_{suffix}")))
        .await
        .expect("primary key must be admitted");
}

// ── Chunked copy ──

/// Rows keyed by their primary-key texts, every column as text.
type TextRows = BTreeMap<Vec<String>, Vec<Option<String>>>;

/// The first `columns` values of `row` as text.
fn row_texts(batch: &SourceBatch, row: usize, columns: usize) -> Vec<Option<String>> {
    (0..columns)
        .map(|column| {
            let array = batch.records.column(column);
            array
                .is_valid(row)
                .then(|| arrow_cast::display::array_value_to_string(array, row).unwrap())
        })
        .collect()
}

fn key_of(values: &[Option<String>], key: &[usize]) -> Vec<String> {
    key.iter()
        .map(|&column| values[column].clone().expect("key value"))
        .collect()
}

/// Apply upsert puts and tombstones to `rows`.
fn apply_keyed(rows: &mut TextRows, batch: &SourceBatch, key: &[usize], columns: usize) {
    for row in 0..batch.num_rows() {
        let values = row_texts(batch, row, columns);
        match batch
            .mutations()
            .map_or(SourceMutation::Put, |all| all[row])
        {
            SourceMutation::Put => {
                rows.insert(key_of(&values, key), values);
            }
            SourceMutation::Tombstone => {
                rows.remove(&key_of(&values, key));
            }
        }
    }
}

/// The table's rows as `columns` read them, as text.
async fn table_texts(fixture: &Fixture, columns: &str, key: &[usize]) -> TextRows {
    let messages = fixture
        .admin
        .simple_query(&format!("SELECT {columns} FROM {}", fixture.table))
        .await
        .unwrap();
    messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let values: Vec<Option<String>> = (0..row.len())
                    .map(|index| row.get(index).map(String::from))
                    .collect();
                Some((key_of(&values, key), values))
            }
            _ => None,
        })
        .collect()
}

/// Writes that run concurrently with a copy until stopped: each round runs the next statement,
/// with `{n}` replaced by the round number. Statements may fail, as a key change onto an
/// existing key does; the table is the reference either way.
struct Churn {
    stop: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<u64>,
}

impl Churn {
    fn start(statements: Vec<String>) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let task = tokio::spawn(async move {
            let client = admin().await.expect("churn session");
            let mut round = 0_u64;
            while !stopping.load(Ordering::Acquire) {
                let statement = &statements[usize::try_from(round).unwrap() % statements.len()];
                let _ = client
                    .batch_execute(&statement.replace("{n}", &round.to_string()))
                    .await;
                round += 1;
                sleep(Duration::from_millis(2)).await;
            }
            round
        });
        Self { stop, task }
    }

    async fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.task.await.unwrap()
    }
}

/// The copy's chunk count recorded in a cursor.
fn chunks_of(cursor: &SourceCheckpoint) -> u64 {
    cursor
        .get_metadata("snapshot_chunks")
        .map_or(0, |chunks| chunks.parse().unwrap())
}

/// Copy the fixture's table while `churn` writes to it, then drain until the upsert mirror equals
/// the table. Returns how many chunks the copy took.
async fn upsert_copy_under_churn(
    fixture: &Fixture,
    schema: &Schema,
    primary_key: &[&str],
    select: &str,
    key: &[usize],
    churn: Vec<String>,
) -> u64 {
    let mut config = fixture.config(schema, primary_key, &[]);
    config.set("max.buffered.bytes", "1048576");
    let mut source = started(start(&config)).await;
    let writes = Churn::start(churn);
    let columns = schema.fields().len();
    let mut mirror = TextRows::new();
    let mut chunks = 0;
    timeout(WAIT * 4, async {
        loop {
            match source.poll_batch(256).await.expect("poll") {
                Some(mut batch) => {
                    if let Some(SourceBatchCursor::Complete(cursor)) = batch.take_cursor() {
                        chunks = chunks.max(chunks_of(&cursor));
                    }
                    apply_keyed(&mut mirror, &batch, key, columns);
                }
                None => sleep(Duration::from_millis(5)).await,
            }
            if phase(&source.try_checkpoint().unwrap().unwrap()) == "streaming" {
                return;
            }
        }
    })
    .await
    .expect("the copy did not finish");
    assert!(writes.stop().await > 10, "writes ran during the copy");
    let expected = table_texts(fixture, select, key).await;
    timeout(WAIT, async {
        while mirror != expected {
            match source.poll_batch(256).await.expect("poll") {
                Some(batch) => apply_keyed(&mut mirror, &batch, key, columns),
                None => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        let missing: Vec<_> = expected
            .keys()
            .filter(|key| !mirror.contains_key(*key))
            .take(5)
            .collect();
        let extra: Vec<_> = mirror
            .keys()
            .filter(|key| !expected.contains_key(*key))
            .take(5)
            .collect();
        let stale: Vec<_> = expected
            .iter()
            .filter(|(key, row)| mirror.get(*key).is_some_and(|got| got != *row))
            .map(|(key, _)| key)
            .take(5)
            .collect();
        panic!("mirror != table: missing {missing:?}, extra {extra:?}, stale {stale:?}")
    });
    source.close().await.unwrap();
    chunks
}

fn int_churn(table: &str) -> Vec<String> {
    [
        "UPDATE {t} SET label = 'u{n}' WHERE id = ({n} * 37) % 3000 + 1",
        "DELETE FROM {t} WHERE id = ({n} * 53) % 3000 + 1",
        "INSERT INTO {t} VALUES (({n} * 61) % 3000 + 1, 'back', {n}) \
         ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty",
        "UPDATE {t} SET id = id + 5000 WHERE id = ({n} * 71) % 3000 + 1",
        "UPDATE {t} SET id = id - 5000 WHERE id = ({n} * 79) % 3000 + 5001",
        "INSERT INTO {t} VALUES (10000 + {n}, 'new', {n})",
    ]
    .map(|statement| statement.replace("{t}", table))
    .to_vec()
}

/// A copy of many chunks equals the table while inserts, updates, deletes, and key changes
/// across chunk bounds run concurrently.
#[tokio::test]
async fn chunked_copy_equals_the_table_under_concurrent_writes_with_integer_keys() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, repeat('s', 2000), g FROM generate_series(1, 3000) g",
            fixture.table
        ))
        .await;
    let chunks = upsert_copy_under_churn(
        &fixture,
        &orders_schema(),
        &["id"],
        "id, label, qty",
        &[0],
        int_churn(&fixture.table),
    )
    .await;
    assert!(chunks > 20, "{chunks} chunks");
}

/// Composite keys whose index order differs from the attribute order, with mixed-case text that
/// `en_US.utf8` orders differently from bytes: chunk bounds and streamed keys are compared by
/// the server under the index collation.
#[tokio::test]
async fn chunked_copy_equals_the_table_with_composite_collated_keys() {
    let Some(fixture) = Fixture::new(
        "label text, name text NOT NULL, tenant integer NOT NULL, PRIMARY KEY (tenant, name)",
    )
    .await
    else {
        return;
    };
    let t = &fixture.table;
    fixture
        .exec(&format!(
            "INSERT INTO {t} SELECT repeat('s', 2000), \
                    CASE WHEN g % 3 = 0 THEN initcap(md5(g::text)) ELSE md5(g::text) END, g % 7 \
             FROM generate_series(1, 2500) g"
        ))
        .await;
    let pick = |n: u32| {
        format!("(SELECT tenant, name FROM {t} ORDER BY tenant, name OFFSET ({{n}} * {n}) % 2000 LIMIT 1)")
    };
    let churn = vec![
        format!(
            "UPDATE {t} SET label = 'u{{n}}' WHERE (tenant, name) = {}",
            pick(37)
        ),
        format!("DELETE FROM {t} WHERE (tenant, name) = {}", pick(53)),
        format!("INSERT INTO {t} VALUES ('i', initcap(md5('x{{n}}')), {{n}} % 7)"),
        format!(
            "UPDATE {t} SET name = upper(name) || 'z' WHERE (tenant, name) = {}",
            pick(71)
        ),
        format!(
            "UPDATE {t} SET tenant = (tenant + 4) % 7 WHERE (tenant, name) = {}",
            pick(79)
        ),
    ];
    let schema = Schema::new(vec![
        Field::new("label", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("tenant", DataType::Int32, false),
    ]);
    let chunks = upsert_copy_under_churn(
        &fixture,
        &schema,
        &["tenant", "name"],
        "label, name, tenant",
        &[2, 1],
        churn,
    )
    .await;
    assert!(chunks > 20, "{chunks} chunks");
}

/// A text key with its own column collation (`"C"`) inside an `en_US.utf8` database.
#[tokio::test]
async fn chunked_copy_equals_the_table_with_a_column_collation_key() {
    let Some(fixture) = Fixture::new("code text COLLATE \"C\" PRIMARY KEY, label text").await
    else {
        return;
    };
    let t = &fixture.table;
    fixture
        .exec(&format!(
            "INSERT INTO {t} SELECT CASE WHEN g % 2 = 0 THEN initcap(md5(g::text)) \
                                         ELSE md5(g::text) END, repeat('s', 2000) \
             FROM generate_series(1, 2500) g"
        ))
        .await;
    let pick = |n: u32| {
        format!("(SELECT code FROM {t} ORDER BY code OFFSET ({{n}} * {n}) % 2000 LIMIT 1)")
    };
    let churn = vec![
        format!("UPDATE {t} SET label = 'u{{n}}' WHERE code = {}", pick(37)),
        format!("DELETE FROM {t} WHERE code = {}", pick(53)),
        format!("INSERT INTO {t} VALUES (initcap(md5('x{{n}}')), 'i')"),
        format!(
            "UPDATE {t} SET code = upper(code) WHERE code = {}",
            pick(71)
        ),
        format!(
            "UPDATE {t} SET code = lower(code) || 'a' WHERE code = {}",
            pick(79)
        ),
    ];
    let schema = Schema::new(vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
    ]);
    let chunks =
        upsert_copy_under_churn(&fixture, &schema, &["code"], "code, label", &[0], churn).await;
    assert!(chunks > 20, "{chunks} chunks");
}

/// In changelog mode every row image's running weight stays 0 or 1, and the images at weight 1
/// at the end are exactly the table.
#[tokio::test]
async fn changelog_copy_weights_sum_to_the_table_under_concurrent_writes() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, repeat('s', 2000), g FROM generate_series(1, 3000) g",
            fixture.table
        ))
        .await;
    let mut config = fixture.config(
        &changelog_schema(),
        &["id"],
        &[("output.mode", "changelog")],
    );
    config.set("max.buffered.bytes", "1048576");
    let mut source = started(start(&config)).await;
    let writes = Churn::start(int_churn(&fixture.table));
    let mut weights = Weights::new();
    timeout(WAIT * 4, async {
        while phase(&source.try_checkpoint().unwrap().unwrap()) != "streaming" {
            match source.poll_batch(256).await.expect("poll") {
                Some(batch) => add_weights(&mut weights, &batch),
                None => sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .expect("the copy did not finish");
    assert!(writes.stop().await > 10);
    let expected: Vec<Vec<Option<String>>> = table_texts(&fixture, "id, label, qty", &[0])
        .await
        .into_values()
        .collect();
    let current = |weights: &Weights| {
        weights
            .iter()
            .filter(|(_, weight)| **weight == 1)
            .map(|(image, _)| image.clone())
            .collect::<Vec<_>>()
    };
    timeout(WAIT, async {
        while current(&weights) != expected {
            match source.poll_batch(256).await.expect("poll") {
                Some(batch) => add_weights(&mut weights, &batch),
                None => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("the changelog did not converge to the table");
    source.close().await.unwrap();
}

/// The running weight of every row image of a changelog.
type Weights = BTreeMap<Vec<Option<String>>, i64>;

fn add_weights(weights: &mut Weights, batch: &SourceBatch) {
    let weight = batch.records.column(3).as_primitive::<Int64Type>();
    for row in 0..batch.num_rows() {
        let image = row_texts(batch, row, 3);
        let total = weights.entry(image.clone()).or_default();
        *total += weight.value(row);
        assert!(
            (0..=1).contains(total),
            "image {image:?} reached weight {total}"
        );
    }
}

/// Copy cursors carry the stream position, so committing them advances the slot while the copy
/// is still running.
#[tokio::test]
async fn confirmed_flush_lsn_advances_during_the_copy() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, repeat('s', 2000), g FROM generate_series(1, 6000) g",
            fixture.table
        ))
        .await;
    let mut config = fixture.config(&orders_schema(), &["id"], &[]);
    config.set("max.buffered.bytes", "1048576");
    let mut source = started(start(&config)).await;
    let gate = source.try_checkpoint().unwrap().unwrap();
    let start_lsn: Lsn = gate.get_offset("lsn").unwrap().parse().unwrap();
    let slot = slot_of(&gate);
    let mut epoch = 2;
    let advanced = timeout(WAIT, async {
        loop {
            fixture
                .exec(&format!(
                    "UPDATE {} SET qty = qty + 1 WHERE id = 6000",
                    fixture.table
                ))
                .await;
            let _ = source.poll_batch(256).await.expect("poll");
            let cursor = source.try_checkpoint().unwrap().unwrap();
            assert_eq!(phase(&cursor), "snapshot", "the copy finished first");
            source.notify_epoch_committed(epoch, &cursor).await.unwrap();
            epoch += 1;
            let confirmed = fixture.slot(&slot).await.and_then(|slot| slot.0);
            if confirmed.is_some_and(|confirmed| confirmed > start_lsn) {
                return confirmed;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the slot must advance during the copy");
    assert!(advanced > Some(start_lsn));
    source.close().await.unwrap();
}

/// A table rewrite during the copy fails the source closed: a chunk snapshot taken across it
/// would read an empty table and end the copy early.
#[tokio::test]
async fn a_column_type_change_during_the_copy_fails_closed_without_ending_it() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, repeat('s', 2000), g FROM generate_series(1, 3000) g",
            fixture.table
        ))
        .await;
    let mut config = fixture.config(&orders_schema(), &["id"], &[]);
    config.set("max.buffered.bytes", "1048576");
    let mut source = started(start(&config)).await;
    let mut mirror = Mirror::new();
    while mirror.len() < 100 {
        apply(&mut mirror, &next_batch(&mut source).await);
    }
    fixture
        .exec(&format!(
            "ALTER TABLE {} ALTER COLUMN qty TYPE bigint",
            fixture.table
        ))
        .await;
    let error = timeout(WAIT, async {
        loop {
            match source.poll_batch(256).await {
                Ok(Some(batch)) => apply(&mut mirror, &batch),
                Ok(None) => sleep(Duration::from_millis(10)).await,
                Err(error) => return error,
            }
            assert_ne!(
                phase(&source.try_checkpoint().unwrap().unwrap()),
                "streaming",
                "the copy must never end early"
            );
        }
    })
    .await
    .expect("the rewrite must stop the copy")
    .to_string();
    assert!(
        error.contains("during the initial snapshot") || error.contains("bound layout"),
        "{error}"
    );
    assert!(mirror.len() < 3000);
    source.close().await.unwrap();
}

/// With no other writes on the server, each chunk's window closes as soon as the stream decodes
/// the chunk's WAL marker.
#[tokio::test]
async fn an_idle_server_closes_each_chunk_window_promptly() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    fixture
        .exec(&format!(
            "INSERT INTO {} SELECT g, 'seed', g FROM generate_series(1, 5) g",
            fixture.table
        ))
        .await;
    let config = fixture.config(&orders_schema(), &["id"], &[]);
    let mut source = started(start(&config)).await;
    let began = std::time::Instant::now();
    let mut mirror = Mirror::new();
    while phase(&source.try_checkpoint().unwrap().unwrap()) != "streaming" {
        match source.poll_batch(64).await.expect("poll") {
            Some(batch) => apply(&mut mirror, &batch),
            None => sleep(Duration::from_millis(5)).await,
        }
        assert!(
            began.elapsed() < Duration::from_secs(3),
            "the copy waited for an unrequested keepalive"
        );
    }
    let elapsed = began.elapsed();
    eprintln!("idle copy of two chunks took {elapsed:?}");
    assert!(elapsed < Duration::from_millis(1_500), "{elapsed:?}");
    assert_eq!(mirror.len(), 5);
    source.close().await.unwrap();
}

/// Keys equal under a non-deterministic collation can differ as text, so the copy could not
/// match streamed keys to copied ones; the source refuses before creating a slot.
#[tokio::test]
async fn a_non_deterministic_key_collation_is_rejected_before_any_slot() {
    let Some(probe) = admin().await else {
        return;
    };
    let collation = format!("ci_{}", unique_id());
    probe
        .batch_execute(&format!(
            "CREATE COLLATION {collation} (provider = icu, locale = 'und-u-ks-level2', \
             deterministic = false)"
        ))
        .await
        .unwrap();
    let Some(fixture) = Fixture::new(&format!(
        "code text COLLATE {collation} PRIMARY KEY, label text"
    ))
    .await
    else {
        return;
    };
    let schema = Schema::new(vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
    ]);
    let config = fixture.config(&schema, &["code"], &[]);
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = source.start(start(&config)).await.unwrap_err().to_string();
    let never = fixture.config(&schema, &["code"], &[("snapshot.mode", "never")]);
    let mut streaming = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let streams = streaming.start(start(&never)).await;
    fixture
        .exec(&format!(
            "DROP TABLE {}; DROP COLLATION {collation}",
            fixture.table
        ))
        .await;
    assert!(error.contains("non-deterministic collation"), "{error}");
    assert!(streams.is_ok(), "only the copy compares keys");
    streaming.close().await.unwrap();
}

/// An upsert source restarted from a cursor inside the copy converges on targets the failed
/// attempt already wrote: rows it copied after the cursor and that were deleted, changed, or
/// moved before the restart are deleted or rewritten, because tombstones are never suppressed.
#[tokio::test]
async fn an_upsert_restart_inside_the_copy_converges_on_rows_written_after_the_cursor() {
    let Some(fixture) = Fixture::new(ORDERS).await else {
        return;
    };
    let t = fixture.table.clone();
    fixture
        .exec(&format!(
            "INSERT INTO {t} SELECT g, repeat('s', 2000), g FROM generate_series(1, 3000) g"
        ))
        .await;
    let config = fixture.config(&orders_schema(), &["id"], &[]);
    let mut first = started(start(&config)).await;
    let mut target = Mirror::new();
    let mut committed = None;
    timeout(WAIT, async {
        loop {
            let mut batch = next_batch(&mut first).await;
            let Some(SourceBatchCursor::Complete(cursor)) = batch.take_cursor() else {
                panic!("a complete cursor");
            };
            apply(&mut target, &batch);
            let chunks = chunks_of(&cursor);
            if committed.is_none() && chunks >= 3 {
                first.notify_epoch_committed(2, &cursor).await.unwrap();
                committed = Some(cursor);
            } else if chunks >= 8 {
                return;
            }
        }
    })
    .await
    .expect("the copy emitted chunks past the committed cursor");
    let committed = committed.unwrap();
    let bound: Vec<String> =
        serde_json::from_str(committed.get_metadata("snapshot_bound").unwrap()).unwrap();
    let bound: i64 = bound[0].parse().unwrap();
    let written = target.keys().copied().filter(|&id| id > bound).count();
    assert!(
        written > 100,
        "the failed attempt wrote rows after the cursor"
    );
    first.close().await.unwrap();

    fixture
        .exec(&format!(
            "DELETE FROM {t} WHERE id BETWEEN {a} AND {b}; \
             UPDATE {t} SET label = 'changed' WHERE id BETWEEN {c} AND {d}; \
             UPDATE {t} SET id = id + 10000 WHERE id BETWEEN {e} AND {f}; \
             UPDATE {t} SET id = {a} WHERE id = 2999;",
            a = bound + 1,
            b = bound + 40,
            c = bound + 41,
            d = bound + 80,
            e = bound + 81,
            f = bound + 100,
        ))
        .await;
    let mut resumed = started(resume(&config, committed)).await;
    fixture
        .exec(&format!(
            "DELETE FROM {t} WHERE id BETWEEN {} AND {}",
            bound + 101,
            bound + 120
        ))
        .await;
    drain_to(&mut resumed, &mut target, &fixture).await;
    resumed.close().await.unwrap();
}

// ── Visibility races (tests/docker/postgres-cdc-syncrep-compose.yml) ──

const SYNCREP_PORT_ENV: &str = "LAMINAR_TEST_POSTGRES_SYNCREP_PORT";

/// A fixture on the server whose synchronous standby never confirms, or `None` when
/// `LAMINAR_TEST_POSTGRES_SYNCREP_PORT` is unset.
async fn syncrep_fixture(columns: &str) -> Option<Fixture> {
    let port = std::env::var(SYNCREP_PORT_ENV)
        .ok()?
        .parse()
        .expect(SYNCREP_PORT_ENV);
    let fixture = Fixture::on(port, columns)
        .await
        .expect("the sync-rep fixture is unreachable");
    // A racer left waiting by a failed run would hold back every slot creation.
    fixture
        .exec("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'")
        .await;
    Some(fixture)
}

async fn until_true(fixture: &Fixture, query: &str) {
    timeout(WAIT, async {
        while !fixture
            .admin
            .query_one(query, &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
        {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never true: {query}"));
}

/// Run `sql` committing with `synchronous_commit = on`: the commit record is flushed and
/// streamed, but the session waits for the standby and snapshots treat it as running. Returns
/// the session's pid once it waits.
async fn race(fixture: &Fixture, sql: String) -> i32 {
    let racer = admin_on(fixture.port).await.unwrap();
    let pid: i32 = racer
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    tokio::spawn(async move {
        let _ = racer
            .batch_execute(&format!("SET synchronous_commit = on; {sql}"))
            .await;
    });
    until_true(
        fixture,
        &format!("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = {pid} AND wait_event = 'SyncRep')"),
    )
    .await;
    pid
}

/// End the standby wait; the commit, already durable locally, becomes visible.
async fn release(fixture: &Fixture, pid: i32) {
    fixture
        .exec(&format!("SELECT pg_cancel_backend({pid})"))
        .await;
    until_true(
        fixture,
        &format!("SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = {pid} AND wait_event = 'SyncRep')"),
    )
    .await;
}

/// A transaction the stream delivered while snapshots still treat it as running is pending:
/// copying holds until it is visible, so the chunk that copies its row includes it.
#[tokio::test]
#[ignore = "needs tests/docker/postgres-cdc-syncrep-compose.yml and LAMINAR_TEST_POSTGRES_SYNCREP_PORT"]
async fn sync_rep_commits_streamed_before_they_are_visible_hold_the_copy() {
    let Some(fixture) = syncrep_fixture(ORDERS).await else {
        return;
    };
    let t = fixture.table.clone();
    fixture
        .exec(&format!(
            "INSERT INTO {t} SELECT g, repeat('s', 2000), g FROM generate_series(1, 3000) g"
        ))
        .await;
    let mut config = fixture.config(&orders_schema(), &["id"], &[]);
    config.set("max.buffered.bytes", "1048576");
    let mut source = started(start(&config)).await;
    let mut mirror = Mirror::new();
    apply(&mut mirror, &next_batch(&mut source).await);
    let racer = race(
        &fixture,
        format!("UPDATE {t} SET label = 'raced' WHERE id = 2900"),
    )
    .await;

    let poll_once = async |source: &mut PostgresCdcSource, mirror: &mut Mirror| {
        if let Some(batch) = source.poll_batch(256).await.expect("poll") {
            apply(mirror, &batch);
        }
        sleep(Duration::from_millis(10)).await;
        source.try_checkpoint().unwrap().unwrap()
    };
    timeout(WAIT, async {
        while poll_once(&mut source, &mut mirror)
            .await
            .get_metadata("pending_xids")
            .is_none_or(str::is_empty)
        {}
    })
    .await
    .expect("the streamed racer becomes pending");
    let settle = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < settle {
        poll_once(&mut source, &mut mirror).await;
    }
    let held = chunks_of(&source.try_checkpoint().unwrap().unwrap());
    let hold = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < hold {
        let cursor = poll_once(&mut source, &mut mirror).await;
        assert_eq!(chunks_of(&cursor), held, "copying waits for the racer");
        assert_eq!(phase(&cursor), "snapshot");
    }
    assert!(!mirror.contains_key(&2900));

    release(&fixture, racer).await;
    drain_to(&mut source, &mut mirror, &fixture).await;
    assert_eq!(mirror[&2900].0.as_deref(), Some("raced"));
    source.close().await.unwrap();
}

/// The pid-free condition "a session of this source waits on transaction `xid`".
fn creation_waits_on(xid: &str) -> String {
    format!(
        "SELECT EXISTS (SELECT 1 FROM pg_locks AS l JOIN pg_stat_activity AS a ON a.pid = l.pid \
         WHERE NOT l.granted AND l.locktype = 'transactionid' AND l.transactionid::text = '{xid}' \
         AND a.application_name LIKE 'laminar:%')"
    )
}

/// The slot counts a transaction as committed once it decoded its commit record, before the
/// procarray does: such a transaction committed before the consistent point is not streamed,
/// so copying holds until chunk snapshots see it.
///
/// Slot creation waits for the transactions running at each stage of its snapshot builder. The
/// racer starts after the second stage began and commits before the last, so the builder decodes
/// its commit while it still waits for the standby.
#[tokio::test]
#[ignore = "needs tests/docker/postgres-cdc-syncrep-compose.yml and LAMINAR_TEST_POSTGRES_SYNCREP_PORT"]
async fn sync_rep_commits_before_the_consistent_point_hold_the_copy() {
    let Some(fixture) = syncrep_fixture(ORDERS).await else {
        return;
    };
    let t = fixture.table.clone();
    fixture
        .exec(&format!(
            "CREATE TABLE {t}_other (id integer); \
             INSERT INTO {t} SELECT g, 'seed', g FROM generate_series(1, 3000) g"
        ))
        .await;
    let config = fixture.config(&orders_schema(), &["id"], &[]);
    let open = async |id: i32| {
        let session = admin_on(fixture.port).await.unwrap();
        session
            .batch_execute(&format!("BEGIN; INSERT INTO {t}_other VALUES ({id})"))
            .await
            .unwrap();
        let xid: String = session
            .query_one("SELECT pg_current_xact_id()::text", &[])
            .await
            .unwrap()
            .get(0);
        (session, xid)
    };
    let (first_blocker, first_xid) = open(1).await;
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    source.start(start(&config)).await.unwrap();
    let claimed = source.try_checkpoint().unwrap().expect("a claim");
    source.notify_epoch_committed(1, &claimed).await.unwrap();
    until_true(&fixture, &creation_waits_on(&first_xid)).await;
    let (second_blocker, second_xid) = open(2).await;
    first_blocker.batch_execute("COMMIT").await.unwrap();
    until_true(&fixture, &creation_waits_on(&second_xid)).await;
    let racer = race(
        &fixture,
        format!("UPDATE {t} SET label = 'raced' WHERE id = 2900"),
    )
    .await;
    second_blocker.batch_execute("COMMIT").await.unwrap();

    commit_until_ready(&mut source).await;
    let gate = source.try_checkpoint().unwrap().unwrap();
    assert!(gate.get_metadata("visibility_floor").is_some());
    let hold = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < hold {
        assert!(
            source.poll_batch(256).await.expect("poll").is_none(),
            "no chunk while the racer the slot counted is invisible"
        );
        sleep(Duration::from_millis(20)).await;
    }

    release(&fixture, racer).await;
    let mut mirror = Mirror::new();
    drain_to(&mut source, &mut mirror, &fixture).await;
    assert_eq!(mirror[&2900].0.as_deref(), Some("raced"));
    source.close().await.unwrap();
    fixture.exec(&format!("DROP TABLE {t}_other")).await;
}
