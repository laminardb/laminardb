//! Three-node at-least-once soak for a PostgreSQL CDC source in upsert mode with the initial
//! copy, which one node at a time reads, as an interval join's left side. The quotes table is
//! keyed by `(at, key)`, so the copy reads it in event-time order, and before any node starts it
//! holds one wide row per key and second of a historical stretch: the copy outlasts the start. Kafka
//! already holds one trade for every fourth second, each matching exactly one quote, its key's at
//! the start of the trade's second. A stream keeps a trade only when the join found that quote,
//! and a keyed materialized view folds the quote values of those trades by trade id bucket.
//!
//! The first round kills the node copying the table, and a survivor must continue the copy. Then
//! the observed leader and each follower are killed inside a checkpoint and rejoin
//! (`kill_rejoin`). Operator state and source positions commit in one cut, so every node's
//! committed read of the view must equal the buckets of every produced trade, each once: a lost,
//! repeated or wrongly copied quote changes a bucket. The quotes must never claim a second slot.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,postgres-cdc,postgres-sink --test cluster_soak \
//!   postgres_upsert_copy_source::three_node_postgres_upsert_copy_interval_kill9_soak \
//!   -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the rounds, default 30), `LAMINAR_SOAK_POSTGRES_SEED_ROWS` (1 KiB quotes
//! the copy reads, default 200000), the Kafka broker in `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`, and
//! the `LAMINAR_SOAK_POSTGRES_*` server of the PostgreSQL sink soak, which must run with
//! `wal_level=logical`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::bucket_view::{converged_reads, view_sql, Buckets, BUCKETS};
use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, steady_phase, SoakSettings, CHECKPOINTS_COMPLETED,
};
use super::multi_writer_sinks::block_on;
use super::multi_writer_sinks::postgres::Postgres;
use super::postgres_changelog_source::copier_kill_round;
use super::postgres_source::{slots_under, Capture, CaptureCleanup};
use super::temporal_pricing::TRADE_PARTITIONS;
use super::{
    env_u64, kafka_create_topic, produce_temporal_payloads, scoped_soak_storage_url, soak_run_id,
    JoinDelivery, ResolvedExecutable, NODES,
};

const BROKERS: &str = "LAMINAR_SOAK_KAFKA_SOURCE_BROKERS";
/// One key per view bucket, so a trade's key is its bucket.
const KEYS: i64 = BUCKETS;
/// The quotes' first second, 2026-01-01T00:00:00Z.
const FIRST_SECOND_MS: i64 = 1_767_225_600_000;
/// One trade for every this many seconds of quotes.
const TRADE_EVERY_SECONDS: i64 = 4;

#[test]
#[ignore = "spawns 3 real laminardb processes with S3, Kafka and a logical-replication PostgreSQL"]
fn three_node_postgres_upsert_copy_interval_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let postgres = Postgres::from_environment();
    let brokers = std::env::var(BROKERS)
        .unwrap_or_else(|_| panic!("the PostgreSQL upsert copy soak requires {BROKERS}"));
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let suffix = run_id.replace('-', "_");
    let capture = Capture {
        table: format!("quotes_{suffix}"),
        publication: format!("quotes_{suffix}"),
        slot: format!("soak_{suffix}"),
    };
    let seconds = i64::try_from(env_u64("LAMINAR_SOAK_POSTGRES_SEED_ROWS", 200_000))
        .expect("LAMINAR_SOAK_POSTGRES_SEED_ROWS fits i64")
        / KEYS;
    let topic = format!("soak-trades-{run_id}");
    // Bound before the nodes, so it runs after they stop.
    let _cleanup = CaptureCleanup {
        postgres: &postgres,
        capture: &capture,
        output: None,
    };
    create_and_seed(&postgres, &capture, seconds);
    kafka_create_topic(&brokers, &topic, TRADE_PARTITIONS);
    let (records, produced) = trades(seconds);
    produce_temporal_payloads(&brokers, &topic, TRADE_PARTITIONS, &records);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!(
        "soak: PostgreSQL upsert copy node logs in {}",
        log_dir.display()
    );
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        &view_sql("correctly_quoted"),
        &workload(&brokers, &topic, &capture, &postgres.source_properties()),
    );
    let mut live = || {};
    let mut latest = boot(&mut nodes, &mut live, &settings, CHECKPOINTS_COMPLETED);
    latest = copier_kill_round(&mut nodes, &mut live, &settings, latest);
    let mut killed_followers = BTreeSet::new();
    for round in 0..settings.kills {
        (_, latest) = kill_round(
            &mut nodes,
            &mut live,
            round,
            &mut killed_followers,
            &settings,
            CHECKPOINTS_COMPLETED,
            latest,
        );
    }
    steady_phase(
        &mut nodes,
        &mut live,
        settings.steady,
        CHECKPOINTS_COMPLETED,
    );
    let epoch = converged_reads(&mut nodes, &quoted_buckets(produced), &settings, latest);
    let slots = block_on(slots_under(&postgres, &capture.slot));
    assert_eq!(
        slots.len(),
        1,
        "the source claimed more than one slot, orphaning the others: {slots:?}"
    );
    eprintln!(
        "soak: PostgreSQL upsert copy across {NODES} nodes: {} seeded quotes, every node's view \
         held the {produced} trades each joined to its quote once at epoch {epoch}, \
         kills={} (followers {killed_followers:?}) slots={slots:?}",
        seconds * KEYS,
        settings.kills
    );
}

fn workload(brokers: &str, topic: &str, capture: &Capture, source: &str) -> String {
    let Capture {
        table,
        publication,
        slot,
    } = capture;
    format!(
        r#"
[[source]]
name = "quotes"
connector = "postgres-cdc"
primary_key = ["at", "key"]
[source.properties]
{source}"slot.name" = "{slot}"
publication = "{publication}"
table = "public.{table}"
"max.buffered.bytes" = "1048576"
[[source.schema]]
name = "at"
type = "TIMESTAMP"
nullable = false
[[source.schema]]
name = "key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "value"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "note"
type = "VARCHAR"
nullable = true
[source.watermark]
column = "at"
max_out_of_orderness = "1s"

[[source]]
name = "trades"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "{brokers}"
topic = "{topic}"
"group.id" = "{topic}"
"startup.mode" = "earliest"
"json.column.at.epoch_unit" = "millis"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "key"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "expected"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "at"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "at"
max_out_of_orderness = "1s"

[[pipeline]]
name = "quoted"
sql = """
SELECT t.id AS id, t.expected AS expected, q.value AS value
FROM quotes q JOIN trades t
  ON q.key = t.key AND t.at BETWEEN q.at AND q.at + INTERVAL '1' SECOND
"""

# A trade joined to no quote or another one is dropped here, so its bucket falls short.
[[pipeline]]
name = "correctly_quoted"
sql = "SELECT id, value AS amount FROM quoted WHERE value = expected"
"#
    )
}

/// Create the quotes table and its publication, and seed key `k`'s quote `second * 1000 + k` at
/// every second of `seconds`, with a 1 KiB note, before any node starts.
fn create_and_seed(postgres: &Postgres, capture: &Capture, seconds: i64) {
    let Capture {
        table, publication, ..
    } = capture;
    block_on(async {
        postgres
            .client()
            .await
            .batch_execute(&format!(
                "CREATE TABLE {table} (at timestamp NOT NULL, key bigint NOT NULL, \
                 value bigint NOT NULL, note text, PRIMARY KEY (at, key)); \
                 ALTER TABLE {table} REPLICA IDENTITY FULL; \
                 CREATE PUBLICATION {publication} FOR TABLE {table}; \
                 INSERT INTO {table} \
                 SELECT to_timestamp({FIRST_SECOND_MS} / 1000 + s) AT TIME ZONE 'UTC', k, \
                 s * 1000 + k, repeat('x', 1024) \
                 FROM generate_series(0, {seconds} - 1) s, generate_series(0, {KEYS} - 1) k;"
            ))
            .await
            .expect("create and seed the quotes table");
    });
}

/// Trade `id` at half past second `id * TRADE_EVERY_SECONDS` on key `id % KEYS`, expecting that
/// key's quote of that second, then one trade per partition on a key with no quotes, ten seconds
/// past the quotes, so every partition's watermark passes the last matched trade. Returns the
/// records and the number of matched trades, ids `0..`.
fn trades(seconds: i64) -> (Vec<(i32, String, String)>, i64) {
    let matched = seconds / TRADE_EVERY_SECONDS;
    let partitions = i64::from(TRADE_PARTITIONS);
    let tail = (matched..matched + partitions).map(|id| (id, KEYS, seconds + 10));
    let records = (0..matched)
        .map(|id| (id, id % KEYS, id * TRADE_EVERY_SECONDS))
        .chain(tail)
        .map(|(id, key, second)| {
            let partition = i32::try_from(id % partitions).expect("partitions fit i32");
            let payload = format!(
                r#"{{"id":{id},"key":{key},"expected":{},"at":{}}}"#,
                second * 1000 + key,
                FIRST_SECOND_MS + second * 1000 + 500
            );
            (partition, id.to_string(), payload)
        })
        .collect();
    (records, matched)
}

/// The view over the first `produced` trades, each joined once to its quote: trade `id` carries
/// its key's quote of second `id * TRADE_EVERY_SECONDS`, and its key is its bucket.
fn quoted_buckets(produced: i64) -> Buckets {
    (0..KEYS)
        .map(|bucket| {
            let values: Vec<i64> = (bucket..produced)
                .step_by(usize::try_from(KEYS).expect("KEYS fits usize"))
                .map(|id| id * TRADE_EVERY_SECONDS * 1000 + bucket)
                .collect();
            let n = i64::try_from(values.len()).expect("counts fit i64");
            [
                bucket,
                n,
                values.iter().sum(),
                values.iter().copied().max().unwrap_or(0),
            ]
        })
        .filter(|bucket| bucket[1] > 0)
        .collect()
}
