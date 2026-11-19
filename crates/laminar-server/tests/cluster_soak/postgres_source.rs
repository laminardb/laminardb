//! Three-node at-least-once soak for a PostgreSQL CDC upsert source on a temporal join's right
//! side, which one node at a time streams from its replication slot. Each key's rate row is
//! committed before any trade can use it, and a heartbeat row changes every 250 ms so the rates'
//! commit-time watermark keeps passing the trades. A Kafka writer produces trades `0..` stamped
//! with the wall time; a temporal `LEFT JOIN` prices each trade with its key's rate, a stream
//! keeps only the trades priced with their key's rate, and a PostgreSQL sink receives their ids.
//! The observed leader and each follower are killed inside a checkpoint and rejoin
//! (`kill_rejoin`).
//!
//! One more round freezes the node streaming the slot with `SIGSTOP`, its replication session
//! still attached, until a survivor takes the slot over. Resumed, the frozen node must exit
//! fenced while the survivor's session keeps the slot.
//!
//! A trade that joins no rate or another one is filtered out, so it shows as a missing id: every
//! produced id must reach the output and no other id may; duplicates are reported. More than one
//! node must have opened the rates source, the source must never claim a second slot, and every
//! node must keep committing checkpoints.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,postgres-cdc,postgres-sink --test cluster_soak \
//!   postgres_source::three_node_postgres_temporal_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the rounds, default 30), `LAMINAR_SOAK_RPS` (default 400), the Kafka broker
//! in `LAMINAR_SOAK_KAFKA_SOURCE_BROKERS`, and the `LAMINAR_SOAK_POSTGRES_*` server of the
//! PostgreSQL sink soak, which must run with `wal_level=logical`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use super::kill_rejoin::{
    boot, cluster_nodes, kill_round, rejoin, steady_phase, SoakSettings, CHECKPOINTS_COMPLETED,
};
use super::multi_writer_sinks::postgres::Postgres;
use super::multi_writer_sinks::{block_on, Destination as _};
use super::temporal_pricing::{
    priced_trades, spawn_trader, trades_and_pricing, warm_up, PricedTrades, Worker,
    HEARTBEAT_EVERY, HEARTBEAT_KEY, KEYS, RATES_SETTLE, RATE_BASE, TRADE_PARTITIONS,
};
use super::{
    kafka_create_topic, scoped_soak_storage_url, soak_run_id, wait_for,
    wait_for_converged_durable_checkpoint, DurableCheckpointStatus, JoinDelivery, Node,
    ResolvedExecutable, NODES,
};

const BROKERS: &str = "LAMINAR_SOAK_KAFKA_SOURCE_BROKERS";
/// Grows only on the node that streams the rates slot; every node reads trades.
const RATES_RECEIVED: &str = "laminardb_postgres_cdc_events_received_total";

/// The tables, publication, and slot prefix of one run.
struct Capture {
    rates: String,
    publication: String,
    slot: String,
}

#[test]
#[ignore = "spawns 3 real laminardb processes with S3, Kafka and a logical-replication PostgreSQL"]
fn three_node_postgres_temporal_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let postgres = Postgres::from_environment();
    let brokers = std::env::var(BROKERS)
        .unwrap_or_else(|_| panic!("the PostgreSQL source soak requires {BROKERS}"));
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let suffix = run_id.replace('-', "_");
    let capture = Capture {
        rates: format!("rates_{suffix}"),
        publication: format!("rates_{suffix}"),
        slot: format!("soak_{suffix}"),
    };
    let (trades, output) = (format!("soak-trades-{run_id}"), postgres.name(&run_id));
    create_tables(&postgres, &capture, &output);
    kafka_create_topic(&brokers, &trades, TRADE_PARTITIONS);
    let mut oracle = postgres.spawn_oracle(&output);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: PostgreSQL source node logs in {}", log_dir.display());
    let workload = workload(
        &brokers,
        &trades,
        &capture,
        &postgres.source_properties(),
        &postgres.sink_properties(&output),
    );
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        "",
        &workload,
    );

    let mut latest = boot(
        &mut nodes,
        &mut || oracle.assert_running(),
        &settings,
        CHECKPOINTS_COMPLETED,
    );
    let mut heartbeat = spawn_heartbeat(&postgres, &capture.rates);
    latest = warm_up(
        &mut nodes,
        &mut heartbeat,
        &settings,
        latest,
        RATES_RECEIVED,
    );
    write_rates(&postgres, &capture.rates);
    std::thread::sleep(RATES_SETTLE);
    let mut trader = spawn_trader(brokers.clone(), trades.clone(), settings.rps);
    let mut live = || {
        heartbeat.assert_running();
        trader.assert_running();
        oracle.assert_running();
    };
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
    latest = paused_owner_round(
        &mut nodes, &mut live, &settings, &postgres, &capture, latest,
    );
    steady_phase(
        &mut nodes,
        &mut live,
        settings.steady,
        CHECKPOINTS_COMPLETED,
    );
    wait_for_converged_durable_checkpoint(
        &mut nodes,
        Instant::now() + settings.recovery_ceiling,
        "steady durable progress",
        Some(latest),
    );
    let PricedTrades {
        produced,
        duplicates,
        owners,
    } = priced_trades(&nodes, trader, heartbeat, oracle, &settings);
    let slots = block_on(slots_under(&postgres, &capture.slot));
    assert_eq!(
        slots.len(),
        1,
        "the source claimed more than one slot, orphaning the others: {slots:?}"
    );
    eprintln!(
        "soak: PostgreSQL source across {NODES} nodes: produced={produced} priced correctly, \
         lost=0 duplicates={duplicates} owners={owners:?} kills={} (followers \
         {killed_followers:?}) slots={slots:?}",
        settings.kills
    );
    drop(nodes);
    drop_capture(&postgres, &capture);
    postgres.delete(&output);
}

fn workload(brokers: &str, trades: &str, capture: &Capture, source: &str, sink: &str) -> String {
    let Capture {
        rates,
        publication,
        slot,
    } = capture;
    format!(
        r#"
[[source]]
name = "rates"
connector = "postgres-cdc"
primary_key = ["id"]
[source.properties]
{source}"slot.name" = "{slot}"
publication = "{publication}"
table = "public.{rates}"
"snapshot.mode" = "never"
"change.time.column" = "changed_at"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "rate"
type = "BIGINT"
nullable = true
[[source.schema]]
name = "changed_at"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "changed_at"
max_out_of_orderness = "1s"
{pricing}
[[sink]]
name = "priced_output"
pipeline = "correctly_priced"
connector = "postgres-sink"
[sink.properties]
{sink}"#,
        pricing = trades_and_pricing(brokers, trades, "id"),
    )
}

/// Create the rates table, its publication and the output table before any node starts. Updates
/// carry the whole row only under `REPLICA IDENTITY FULL`.
fn create_tables(postgres: &Postgres, capture: &Capture, output: &str) {
    let Capture {
        rates, publication, ..
    } = capture;
    block_on(async {
        postgres
            .client()
            .await
            .batch_execute(&format!(
                "CREATE TABLE {rates} (id bigint PRIMARY KEY, rate bigint); \
                 ALTER TABLE {rates} REPLICA IDENTITY FULL; \
                 CREATE PUBLICATION {publication} FOR TABLE {rates}; \
                 CREATE TABLE {output} (id bigint NOT NULL);"
            ))
            .await
            .expect("create the rates, publication and output tables");
    });
}

/// End every session on the run's slots, then drop them, the publication, and the table.
fn drop_capture(postgres: &Postgres, capture: &Capture) {
    let Capture {
        rates,
        publication,
        slot,
    } = capture;
    block_on(async {
        let client = postgres.client().await;
        for name in slots_under(postgres, slot).await {
            let _ = client
                .batch_execute(&format!(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                     WHERE slot_name = '{name}' AND active_pid IS NOT NULL; \
                     SELECT pg_drop_replication_slot('{name}');"
                ))
                .await;
        }
        let _ = client
            .batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {rates}"
            ))
            .await;
    });
}

/// Freeze the node streaming the slot past its process lease while its session stays attached,
/// require a survivor to take the slot over, then resume the frozen node: it must exit fenced
/// while the survivor's session keeps the slot. Returns the checkpoint the cluster commits after
/// the node rejoined.
fn paused_owner_round(
    nodes: &mut [Node],
    live: &mut dyn FnMut(),
    settings: &SoakSettings,
    postgres: &Postgres,
    capture: &Capture,
    latest: DurableCheckpointStatus,
) -> DurableCheckpointStatus {
    let owner = rates_streamer(nodes, live, settings);
    let frozen = block_on(slot_session(postgres, &capture.slot))
        .expect("the node streaming the rates holds the slot");
    let paused = nodes[owner].pause();
    eprintln!("soak: SIGSTOP delivered to node {owner}, which streams the rates slot");
    let survived = wait_for_converged_durable_checkpoint(
        nodes,
        Instant::now() + settings.recovery_ceiling,
        "survivor progress with the rates owner paused",
        Some(latest),
    );
    let mut taken = None;
    wait_for(
        "a survivor to take the rates slot over",
        settings.recovery_ceiling,
        || {
            live();
            taken = block_on(slot_session(postgres, &capture.slot))
                .filter(|session| *session != frozen);
            taken.is_some()
        },
    );
    let taken = taken.expect("the takeover wait returned a session");
    eprintln!("soak: a survivor took the rates slot over from node {owner}");

    paused.resume_until_fenced_exit(settings.recovery_ceiling);
    assert_eq!(
        block_on(slot_session(postgres, &capture.slot)).as_ref(),
        Some(&taken),
        "the resumed node {owner} ended the session that took its slot over"
    );
    rejoin(
        nodes,
        live,
        owner,
        settings,
        CHECKPOINTS_COMPLETED,
        survived,
    )
}

/// The node streaming the slot: the one node whose received-event count grows while the
/// heartbeat changes the table.
fn rates_streamer(nodes: &mut [Node], live: &mut dyn FnMut(), settings: &SoakSettings) -> usize {
    let received = |nodes: &[Node]| -> Vec<f64> {
        nodes
            .iter()
            .map(|node| node.metric(RATES_RECEIVED).unwrap_or(0.0))
            .collect()
    };
    let mut streamer = None;
    wait_for(
        "one node to stream the rates slot",
        settings.recovery_ceiling,
        || {
            live();
            let before = received(nodes);
            std::thread::sleep(HEARTBEAT_EVERY * 4);
            let growing: Vec<usize> = received(nodes)
                .iter()
                .zip(&before)
                .enumerate()
                .filter(|(_, (after, before))| after > before)
                .map(|(index, _)| index)
                .collect();
            streamer = (growing.len() == 1).then(|| growing[0]);
            streamer.is_some()
        },
    );
    streamer.expect("the streamer wait returned a node")
}

/// The replication session holding the run's slot.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SlotSession {
    pid: i32,
    application_name: String,
}

async fn slot_session(postgres: &Postgres, prefix: &str) -> Option<SlotSession> {
    postgres
        .client()
        .await
        .query_opt(
            "SELECT a.pid, a.application_name FROM pg_replication_slots s \
             JOIN pg_stat_activity a ON a.pid = s.active_pid \
             WHERE starts_with(s.slot_name::text, $1)",
            &[&format!("{prefix}_")],
        )
        .await
        .expect("read the slot's session")
        .map(|row| SlotSession {
            pid: row.get(0),
            application_name: row.get(1),
        })
}

async fn slots_under(postgres: &Postgres, prefix: &str) -> Vec<String> {
    postgres
        .client()
        .await
        .query(
            "SELECT slot_name::text FROM pg_replication_slots \
             WHERE starts_with(slot_name::text, $1) ORDER BY 1",
            &[&format!("{prefix}_")],
        )
        .await
        .expect("list the run's slots")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// Set every key's one rate. A retry rewrites the same row.
fn write_rates(postgres: &Postgres, rates: &str) {
    block_on(async {
        let client = postgres.client().await;
        for key in 0..KEYS {
            upsert(&client, rates, key, RATE_BASE + key).await;
        }
    });
}

async fn upsert(client: &tokio_postgres::Client, rates: &str, key: i64, rate: i64) {
    client
        .execute(
            &format!(
                "INSERT INTO {rates} (id, rate) VALUES ($1, $2) \
                 ON CONFLICT (id) DO UPDATE SET rate = EXCLUDED.rate"
            ),
            &[&key, &rate],
        )
        .await
        .unwrap_or_else(|error| panic!("PostgreSQL never committed rate {key}: {error}"));
}

/// Changes the heartbeat row every [`HEARTBEAT_EVERY`].
fn spawn_heartbeat(postgres: &Postgres, rates: &str) -> Worker {
    let (postgres, rates) = (postgres.clone(), rates.to_owned());
    Worker::spawn("heartbeat writer", move |stop| async move {
        let client = postgres.client().await;
        let mut beats = 0;
        while !stop.load(Ordering::Acquire) {
            let beat = i64::try_from(beats).expect("beats fit i64");
            upsert(&client, &rates, HEARTBEAT_KEY, beat).await;
            beats += 1;
            tokio::time::sleep(HEARTBEAT_EVERY).await;
        }
        beats
    })
}
