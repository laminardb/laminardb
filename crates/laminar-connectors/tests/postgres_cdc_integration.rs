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
    let connection = format!(
        "host=127.0.0.1 port={} user=laminar password={PASSWORD} dbname=cdc",
        port()
    );
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
        let admin = admin().await?;
        let id = unique_id();
        let fixture = Self {
            admin,
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
        config.set("port", port().to_string());
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
        let Some(admin) = admin().await else {
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
    assert!(
        inside.get_offset("lsn").is_none(),
        "a cursor inside the snapshot carries no slot position"
    );

    // The first fetch is in flight on the imported snapshot; these changes commit after the
    // slot's consistent point and must arrive through WAL only.
    let mut mirror = Mirror::new();
    let first = source.poll_batch(64).await.unwrap().expect("snapshot rows");
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
    let config = fixture.config(&orders_schema(), &["id"], &[]);
    let mut source = started(start(&config)).await;
    let mut batch = source.poll_batch(64).await.unwrap().expect("snapshot rows");
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
    assert!(error.contains("cannot be resumed"), "{error}");
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
            if !inserted && batches.len() == 1 && source.try_checkpoint().unwrap().is_some() {
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

#[tokio::test]
async fn initial_mode_restarts_from_its_claim_on_a_new_slot_and_reports_the_old_one() {
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
    let old = slot_of(&poll_until_phase(&mut first, "snapshot").await);
    // The process dies before a cursor naming the snapshot commits, so no row was emitted.
    first.close().await.unwrap();

    let registry = prometheus::Registry::new();
    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), Some(&registry));
    resumed.start(resume(&config, claimed)).await.unwrap();
    let reclaimed = resumed.try_checkpoint().unwrap().expect("a claim");
    assert_eq!(phase(&reclaimed), "claimed");
    let new = slot_of(&reclaimed);
    assert_ne!(new, old);
    assert_eq!(orphaned_slots(&registry), "postgres_cdc_orphaned_slots 1");

    commit_until_ready(&mut resumed).await;
    fixture
        .exec(&format!(
            "UPDATE {} SET label = 'later' WHERE id <= 5",
            fixture.table
        ))
        .await;
    let mut mirror = Mirror::new();
    drain_to(&mut resumed, &mut mirror, &fixture).await;
    assert_eq!(mirror.len(), 30);
    let slots = fixture.slots().await;
    assert!(
        slots.contains_key(&old) && slots.contains_key(&new),
        "the orphan is reported, never dropped: {slots:?}"
    );
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
