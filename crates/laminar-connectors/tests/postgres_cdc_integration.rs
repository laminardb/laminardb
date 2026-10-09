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

/// One isolated table, publication, and slot.
struct Fixture {
    admin: Client,
    table: String,
    slot: String,
    publication: String,
}

impl Fixture {
    async fn new(columns: &str) -> Option<Self> {
        let admin = admin().await?;
        let id = format!(
            "{:x}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                % 0xffff_ffff,
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
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

    async fn slot(&self) -> Option<(Option<Lsn>, bool)> {
        self.admin
            .query_opt(
                "SELECT confirmed_flush_lsn::text, active FROM pg_replication_slots \
                 WHERE slot_name = $1",
                &[&self.slot],
            )
            .await
            .unwrap()
            .map(|row| {
                let lsn: Option<String> = row.get(0);
                (lsn.map(|lsn| lsn.parse().unwrap()), row.get(1))
            })
    }

    async fn drop_slot(&self) {
        timeout(WAIT, async {
            loop {
                match self.slot().await {
                    None => return,
                    Some((_, false)) => {
                        let _ = self
                            .admin
                            .execute("SELECT pg_drop_replication_slot($1)", &[&self.slot])
                            .await;
                    }
                    Some((_, true)) => sleep(Duration::from_millis(50)).await,
                }
            }
        })
        .await
        .expect("slot must become droppable");
    }
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

async fn started(request: SourceStart) -> PostgresCdcSource {
    let mut source = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    source.start(request).await.expect("source start");
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
    assert!(
        source.try_checkpoint().unwrap().is_none(),
        "no resumable cursor exists inside the snapshot"
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
    fixture.drop_slot().await;
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
    fixture.drop_slot().await;
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
    source.notify_epoch_committed(1, &cursor).await.unwrap();
    timeout(WAIT, async {
        while fixture.slot().await.and_then(|slot| slot.0) < Some(committed) {
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
    fixture.drop_slot().await;
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
    source.notify_epoch_committed(1, &cursor).await.unwrap();
    timeout(WAIT, async {
        while fixture.slot().await.and_then(|slot| slot.0) < Some(committed) {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("feedback must not wait for the blocked reader");
    source.close().await.unwrap();
    fixture.drop_slot().await;
}

#[tokio::test]
async fn existing_slot_and_interrupted_snapshot_fail_closed() {
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
    source.close().await.unwrap();
    let slot_before = fixture.slot().await.expect("the created slot is kept");

    let mut restarted = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = restarted.start(start(&config)).await.unwrap_err();
    assert!(error.to_string().contains("already exists"), "{error}");
    assert!(
        error.to_string().contains("pg_drop_replication_slot"),
        "{error}"
    );
    let mut resumed = PostgresCdcSource::new(PostgresCdcConfig::default(), None);
    let error = resumed
        .start(resume(&config, snapshot_cursor))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot be resumed"), "{error}");
    assert_eq!(fixture.slot().await.map(|slot| slot.0), Some(slot_before.0));
    fixture.drop_slot().await;
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
            fixture.slot().await.is_none(),
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
    fixture.drop_slot().await;
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
    fixture.drop_slot().await;
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
    fixture.drop_slot().await;
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
    fixture.drop_slot().await;
}

#[tokio::test]
async fn lookup_open_requires_a_usable_single_key_unique_index() {
    let Some(client) = admin().await else {
        return;
    };
    let suffix = NEXT.fetch_add(1, Ordering::Relaxed);
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
