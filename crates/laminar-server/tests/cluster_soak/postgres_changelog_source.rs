//! Three-node at-least-once soak for a PostgreSQL CDC source in changelog mode, which one node at
//! a time reads: it copies the table, then streams the slot. Before any node starts the table
//! holds a seed of wide rows, copied in small chunks under a small memory budget, so the copy
//! outlasts the start. No at-least-once sink that applies changes by key reads the source, so a
//! kill inside the copy resumes it on whichever node opens the source next. A writer inserts,
//! updates and deletes rows throughout, in the seeded range and above it, and a keyed
//! materialized view folds the changes.
//!
//! The first round kills the node copying the table, and a survivor must continue the copy. Then
//! the observed leader and each follower are killed inside a checkpoint and rejoin
//! (`kill_rejoin`). One more round freezes the node streaming the slot with `SIGSTOP` until a
//! survivor takes the slot over; resumed, the frozen node must exit fenced while the survivor's
//! session keeps the slot.
//!
//! Operator state and the source cursor commit in one cut, so once the writer stops, every node's
//! committed read of the view must equal the same aggregate over the table, even at-least-once. More
//! than one node must have read the source, and the source must never claim a second slot.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,postgres-cdc,postgres-sink --test cluster_soak \
//!   postgres_changelog_source::three_node_postgres_changelog_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the rounds, default 30), `LAMINAR_SOAK_RPS` (rows per second the writer
//! inserts, default 400), `LAMINAR_SOAK_POSTGRES_SEED_ROWS` (1 KiB rows the copy reads, default
//! 200000), and the `LAMINAR_SOAK_POSTGRES_*` server of the PostgreSQL sink soak, which must run
//! with `wal_level=logical`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::bucket_view::{bucket_sql, converged_reads, view_sql, Buckets};
use super::kill_rejoin::{
    boot, cluster_nodes, growing_node, kill_round, record_consumers, rejoin, steady_phase,
    SoakSettings, CHECKPOINTS_COMPLETED,
};
use super::multi_writer_sinks::block_on;
use super::multi_writer_sinks::postgres::Postgres;
use super::postgres_source::{paused_owner_round, slots_under, Capture, CaptureCleanup};
use super::temporal_pricing::Worker;
use super::{
    env_u64, scoped_soak_storage_url, soak_run_id, wait_for_converged_durable_checkpoint,
    DurableCheckpointStatus, JoinDelivery, Node, ResolvedExecutable, NODES,
};

/// Grows on a node while it copies the table.
pub(super) const SNAPSHOT_ROWS: &str = "laminardb_postgres_cdc_snapshot_rows_total";
pub(super) const SOURCE: &str = "changes";
const COMMIT_INTERVAL: Duration = Duration::from_millis(500);
/// Updates and deletes above the seed touch the ids of this many most recent inserts.
const RECENT_INSERTS: i64 = 8;

#[test]
#[ignore = "spawns 3 real laminardb processes with S3 and a logical-replication PostgreSQL"]
fn three_node_postgres_changelog_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let postgres = Postgres::from_environment();
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let suffix = run_id.replace('-', "_");
    let capture = Capture {
        table: format!("changes_{suffix}"),
        publication: format!("changes_{suffix}"),
        slot: format!("soak_{suffix}"),
    };
    let seed_rows = i64::try_from(env_u64("LAMINAR_SOAK_POSTGRES_SEED_ROWS", 200_000))
        .expect("LAMINAR_SOAK_POSTGRES_SEED_ROWS fits i64");
    // Bound before the nodes, so it runs after they stop.
    let _cleanup = CaptureCleanup {
        postgres: &postgres,
        capture: &capture,
        output: None,
    };
    create_and_seed(&postgres, &capture, seed_rows);

    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!(
        "soak: PostgreSQL changelog node logs in {}",
        log_dir.display()
    );
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        &view_sql(SOURCE),
        &workload(&capture, &postgres.source_properties()),
    );
    let mut writer = spawn_writer(&postgres, &capture.table, seed_rows, settings.rps);
    let mut live = || writer.assert_running();
    let mut consumers = BTreeSet::new();
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
        record_consumers(&nodes, &mut consumers);
    }
    latest = paused_owner_round(
        &mut nodes,
        &mut live,
        &settings,
        &postgres,
        &capture.slot,
        latest,
    );
    record_consumers(&nodes, &mut consumers);
    steady_phase(
        &mut nodes,
        &mut live,
        settings.steady,
        CHECKPOINTS_COMPLETED,
    );
    let commits = writer.stop();
    let expected = block_on(table_buckets(&postgres, &capture.table));
    let epoch = converged_reads(&mut nodes, &expected, &settings, latest);
    let slots = block_on(slots_under(&postgres, &capture.slot));
    assert_eq!(
        slots.len(),
        1,
        "the source claimed more than one slot, orphaning the others: {slots:?}"
    );
    assert!(
        consumers.len() > 1,
        "only node {consumers:?} ever read the source: it never moved"
    );
    eprintln!(
        "soak: PostgreSQL changelog across {NODES} nodes: {seed_rows} seeded rows, {commits} \
         writer rounds, every node read the table's {} buckets at epoch {epoch}, \
         consumers={consumers:?} kills={} (followers {killed_followers:?}) slots={slots:?}",
        expected.len(),
        settings.kills
    );
}

pub(super) fn workload(capture: &Capture, source: &str) -> String {
    let Capture {
        table,
        publication,
        slot,
    } = capture;
    format!(
        r#"
[[source]]
name = "{SOURCE}"
connector = "postgres-cdc"
primary_key = ["id"]
[source.properties]
{source}"slot.name" = "{slot}"
publication = "{publication}"
table = "public.{table}"
"output.mode" = "changelog"
"max.buffered.bytes" = "1048576"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "amount"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "note"
type = "VARCHAR"
nullable = true
[[source.schema]]
name = "__weight"
type = "BIGINT"
nullable = false
"#
    )
}

/// Create the table and its publication, and seed `rows` ids with 1 KiB notes, before any node
/// starts. Updates and deletes carry the whole old row only under `REPLICA IDENTITY FULL`.
pub(super) fn create_and_seed(postgres: &Postgres, capture: &Capture, rows: i64) {
    let Capture {
        table, publication, ..
    } = capture;
    block_on(async {
        postgres
            .client()
            .await
            .batch_execute(&format!(
                "CREATE TABLE {table} (id bigint PRIMARY KEY, amount bigint NOT NULL, note text); \
                 ALTER TABLE {table} REPLICA IDENTITY FULL; \
                 CREATE PUBLICATION {publication} FOR TABLE {table}; \
                 INSERT INTO {table} SELECT g, g % 100, repeat('x', 1024) \
                 FROM generate_series(0, {rows} - 1) g;"
            ))
            .await
            .expect("create and seed the captured table");
    });
}

/// The node copying the table: the one whose copied-row count grows over a second.
fn copier(nodes: &mut [Node], live: &mut dyn FnMut(), settings: &SoakSettings) -> usize {
    growing_node(
        nodes,
        live,
        settings,
        SNAPSHOT_ROWS,
        Duration::from_secs(1),
        "one node to copy the table",
    )
}

/// Kill the node copying the table, require a survivor to continue the copy from the committed
/// cut, and rejoin the killed node. Returns the checkpoint the cluster commits after it rejoined.
pub(super) fn copier_kill_round(
    nodes: &mut [Node],
    live: &mut dyn FnMut(),
    settings: &SoakSettings,
    latest: DurableCheckpointStatus,
) -> DurableCheckpointStatus {
    let killed = copier(nodes, live, settings);
    let failover_deadline = Instant::now() + settings.recovery_ceiling;
    nodes[killed].kill9();
    eprintln!("soak: kill -9 delivered to node {killed}, which copies the table");
    let survived = wait_for_converged_durable_checkpoint(
        nodes,
        failover_deadline,
        "survivor progress after the copier was killed",
        Some(latest),
    );
    let successor = copier(nodes, live, settings);
    eprintln!("soak: node {successor} continues the copy in place of node {killed}");
    rejoin(
        nodes,
        live,
        killed,
        settings,
        CHECKPOINTS_COMPLETED,
        survived,
    )
}

/// Every [`COMMIT_INTERVAL`], one transaction that inserts new ids above the seed, updates every
/// seventh recent id and deletes some even ones, and updates a few seeded ids and deletes one,
/// wherever the copy stands. Returns the transactions committed.
pub(super) fn spawn_writer(postgres: &Postgres, table: &str, seed_rows: i64, rps: u64) -> Worker {
    let (postgres, table) = (postgres.clone(), table.to_owned());
    let per_insert = i64::try_from(rps).expect("rps fits i64") / 2;
    Worker::spawn("changelog writer", move |stop| async move {
        let client = postgres.client().await;
        let mut next_id = seed_rows;
        let mut round = 0_i64;
        while !stop.load(Ordering::Acquire) {
            tokio::time::sleep(COMMIT_INTERVAL).await;
            let recent = next_id - per_insert * RECENT_INSERTS;
            let seeded = (round * 7_919).rem_euclid(seed_rows);
            client
                .batch_execute(&format!(
                    "BEGIN; \
                     INSERT INTO {table} SELECT g, g % 100 FROM \
                     generate_series({next_id}, {next_id} + {per_insert} - 1) g; \
                     UPDATE {table} SET amount = amount + 1 \
                     WHERE id >= {recent} AND id % 7 = {round} % 7; \
                     DELETE FROM {table} \
                     WHERE id >= {recent} AND id % 11 = {round} % 11 AND id % 2 = 0; \
                     UPDATE {table} SET amount = amount + 1 \
                     WHERE id BETWEEN {seeded} AND {seeded} + 20; \
                     DELETE FROM {table} WHERE id = ({round} * 104729) % {seed_rows}; \
                     COMMIT;"
                ))
                .await
                .unwrap_or_else(|error| panic!("writer round {round}: {error}"));
            next_id += per_insert;
            round += 1;
        }
        u64::try_from(round).expect("rounds are positive")
    })
}

/// The view's rows over the table. PostgreSQL sums `bigint` as `numeric`.
pub(super) async fn table_buckets(postgres: &Postgres, table: &str) -> Buckets {
    postgres
        .client()
        .await
        .query(
            &format!(
                "SELECT bucket, n, total::bigint, peak FROM ({}) buckets ORDER BY bucket",
                bucket_sql(table)
            ),
            &[],
        )
        .await
        .expect("aggregate the table")
        .into_iter()
        .map(|row| std::array::from_fn(|column| row.get::<_, i64>(column)))
        .collect()
}
