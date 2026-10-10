//! Standalone-server PostgreSQL CDC soak: an initial snapshot under load, sustained mixed
//! transactions, repeated hard kills, and exact final results for a keyed mirror and for
//! retractable aggregates over the changelog.
//!
//! Needs `docker compose -f tests/docker/postgres-cdc-compose.yml up -d --wait`. Run with
//! `cargo test --release -p laminar-server --test postgres_cdc_soak -- --ignored --nocapture`;
//! `LAMINAR_POSTGRES_SOAK_SECONDS` (default 120), `LAMINAR_POSTGRES_SOAK_KILL_EVERY_SECONDS`
//! (default 20), and `LAMINAR_TEST_POSTGRES_PORT` (default 15532) size and place the run.

#![cfg(all(feature = "postgres-cdc", feature = "postgres-sink"))]

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const PASSWORD: &str = "laminar-test-secret";
const KEYS: i64 = 4_000;
const BUCKETS: i64 = 16;

fn seconds_from_env(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default),
    )
}

fn port() -> u16 {
    std::env::var("LAMINAR_TEST_POSTGRES_PORT")
        .ok()
        .map_or(15532, |port| port.parse().unwrap())
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

async fn connect() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=laminar password={PASSWORD} dbname=cdc",
            port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("PostgreSQL CDC fixture");
    tokio::spawn(connection);
    client
}

/// Names of one soak run's objects.
struct Run {
    table: String,
    publication: String,
    keyed_slot: String,
    changelog_slot: String,
    mirror: String,
    totals: String,
}

fn server_config(root: &Path, run: &Run) -> String {
    let checkpoint = path_string(&root.join("checkpoints"));
    let checkpoint_url = if checkpoint.starts_with('/') {
        format!("file://{checkpoint}")
    } else {
        format!("file:///{checkpoint}")
    };
    let connection = format!(
        "'host' = '127.0.0.1', 'port' = '{}', 'database' = 'cdc', 'username' = 'laminar', \
         'password' = '$${{SOAK_PG_PASSWORD}}', 'ssl.mode' = 'disable', \
         'publication' = '{}', 'table' = 'public.{}'",
        port(),
        run.publication,
        run.table
    );
    let sink = format!(
        "'hostname' = '127.0.0.1', 'port' = '{}', 'database' = 'cdc', 'username' = 'laminar', \
         'password' = '$${{SOAK_PG_PASSWORD}}', 'ssl.mode' = 'disable', \
         'write.mode' = 'upsert', 'changelog.mode' = 'true', 'auto.create.table' = 'true'",
        port()
    );
    let sql = format!(
        "CREATE SOURCE accounts (id BIGINT NOT NULL, bucket VARCHAR, balance BIGINT, \
         note VARCHAR, PRIMARY KEY (id)) FROM \"postgres-cdc\" ({connection}, \
         'slot.name' = '{keyed_slot}');
         CREATE SOURCE account_changes (id BIGINT NOT NULL, bucket VARCHAR NOT NULL, \
         balance BIGINT, __weight BIGINT NOT NULL, PRIMARY KEY (id)) FROM \"postgres-cdc\" \
         ({connection}, 'slot.name' = '{changelog_slot}', 'output.mode' = 'changelog');
         CREATE STREAM bucket_totals AS SELECT bucket, SUM(balance) AS total, COUNT(*) AS n \
         FROM account_changes GROUP BY bucket EMIT CHANGES;
         CREATE SINK accounts_mirror FROM accounts INTO \"postgres-sink\" ({sink}, \
         'table.name' = '{mirror}', 'primary.key' = 'id');
         CREATE SINK totals_mirror FROM bucket_totals INTO \"postgres-sink\" ({sink}, \
         'table.name' = '{totals}', 'primary.key' = 'bucket');",
        keyed_slot = run.keyed_slot,
        changelog_slot = run.changelog_slot,
        mirror = run.mirror,
        totals = run.totals,
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
        .env("SOAK_PG_PASSWORD", PASSWORD)
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
    fn below(&mut self, bound: i64) -> i64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        i64::try_from(self.0 % u64::try_from(bound).unwrap()).unwrap()
    }
}

/// Single-row and multi-row transactions over a fixed key space until `deadline`: updates
/// that move rows between buckets, deletes, reinserts, primary-key changes, and occasional
/// large out-of-line notes. Returns the number of committed transactions.
async fn write_load(table: String, deadline: Instant) -> u64 {
    let client = connect().await;
    let mut random = Choices(0x5eed_5eed);
    let mut transactions = 0_u64;
    while Instant::now() < deadline {
        let mut statements = Vec::new();
        for _ in 0..=random.below(4) {
            let id = random.below(KEYS);
            let statement = match random.below(10) {
                0..=4 => format!(
                    "UPDATE {table} SET balance = balance + {}, bucket = 'b{}' WHERE id = {id}",
                    random.below(100) - 50,
                    random.below(BUCKETS)
                ),
                5 => format!(
                    "UPDATE {table} SET note = (SELECT string_agg(md5(g::text), '') \
                     FROM generate_series(1, 200) g) WHERE id = {id}"
                ),
                6..=7 => format!("DELETE FROM {table} WHERE id = {id}"),
                8 => format!(
                    "UPDATE {table} SET id = id + {KEYS} WHERE id = {id} \
                     AND NOT EXISTS (SELECT 1 FROM {table} WHERE id = {id} + {KEYS})"
                ),
                _ => format!(
                    "INSERT INTO {table} VALUES ({id}, 'b{}', {}, NULL) ON CONFLICT (id) DO NOTHING",
                    random.below(BUCKETS),
                    random.below(1000)
                ),
            };
            statements.push(statement);
        }
        client
            .batch_execute(&format!("BEGIN; {}; COMMIT;", statements.join("; ")))
            .await
            .unwrap();
        transactions += 1;
        if transactions.is_multiple_of(100) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    transactions
}

type Rows = BTreeMap<i64, (Option<String>, Option<i64>, Option<String>)>;
type Totals = BTreeMap<String, (i64, i64)>;

async fn rows(client: &tokio_postgres::Client, table: &str) -> Rows {
    client
        .query(
            &format!("SELECT id, bucket, balance, note FROM {table}"),
            &[],
        )
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.get(0), (row.get(1), row.get(2), row.get(3))))
                .collect()
        })
        .unwrap_or_default()
}

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

/// Bytes of WAL the slots under each `slot.name` prefix, orphans included, still retain behind
/// the server's current position; `-1` before a slot exists.
async fn retained_wal(client: &tokio_postgres::Client, slots: [&str; 2]) -> Vec<i64> {
    let mut retained = Vec::new();
    for slot in slots {
        let row = client
            .query_one(
                "SELECT COALESCE(MAX(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)), -1)::bigint \
                 FROM pg_replication_slots WHERE starts_with(slot_name::text, $1)",
                &[&format!("{slot}_")],
            )
            .await
            .unwrap();
        retained.push(row.get(0));
    }
    retained
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: needs the PostgreSQL CDC fixture and minutes of wall time"]
#[allow(clippy::too_many_lines)]
async fn postgres_cdc_survives_repeated_hard_kills_under_sustained_writes() {
    let run_for = seconds_from_env("LAMINAR_POSTGRES_SOAK_SECONDS", 120);
    let kill_every = seconds_from_env("LAMINAR_POSTGRES_SOAK_KILL_EVERY_SECONDS", 20);
    let suffix = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let run = Run {
        table: format!("soak_{suffix}"),
        publication: format!("soak_pub_{suffix}"),
        keyed_slot: format!("soak_keyed_{suffix}"),
        changelog_slot: format!("soak_changes_{suffix}"),
        mirror: format!("soak_mirror_{suffix}"),
        totals: format!("soak_totals_{suffix}"),
    };
    let client = connect().await;
    // Existing rows exercise the initial snapshot under concurrent writes.
    client
        .batch_execute(&format!(
            "CREATE TABLE {t} (id bigint PRIMARY KEY, bucket text NOT NULL, balance bigint, \
             note text); ALTER TABLE {t} REPLICA IDENTITY FULL; \
             CREATE PUBLICATION {p} FOR TABLE {t}; \
             INSERT INTO {t} SELECT g, 'b' || (g % {BUCKETS}), g, NULL \
             FROM generate_series(0, {last}) g;",
            t = run.table,
            p = run.publication,
            last = KEYS - 1
        ))
        .await
        .unwrap();

    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("server.toml");
    std::fs::write(&config, server_config(root.path(), &run)).unwrap();
    let log = root.path().join("server.log");
    let started = Instant::now();
    let writer = tokio::spawn(write_load(run.table.clone(), started + run_for));

    let mut server = spawn_server(&config, &log);
    let mut kills = 0_u32;
    let mut peak_mib = 0_u64;
    let mut peak_retained = vec![0_i64; 2];
    let mut next_kill = started + kill_every;
    let slots = [run.keyed_slot.as_str(), run.changelog_slot.as_str()];
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
        if Instant::now() > started + Duration::from_secs(5) {
            for (peak, retained) in peak_retained
                .iter_mut()
                .zip(retained_wal(&client, slots).await)
            {
                *peak = (*peak).max(retained);
            }
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
    let transactions = writer.await.unwrap();

    let expected_rows = rows(&client, &run.table).await;
    let totals_query = format!(
        "SELECT bucket, SUM(balance)::bigint, COUNT(*) FROM {} GROUP BY bucket",
        run.table
    );
    let expected_totals = totals(&client, &totals_query).await;
    let observed_totals_query = format!("SELECT bucket, total::bigint, n FROM {}", run.totals);
    let converge_started = Instant::now();
    let deadline = converge_started + Duration::from_secs(300);
    let (mut observed_rows, mut observed_totals) = (Rows::new(), Totals::new());
    while Instant::now() < deadline {
        observed_rows = rows(&client, &run.mirror).await;
        observed_totals = totals(&client, &observed_totals_query).await;
        if observed_rows == expected_rows && observed_totals == expected_totals {
            break;
        }
        if let Some(mib) = resident_mib(server.id()) {
            peak_mib = peak_mib.max(mib);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let converged = converge_started.elapsed();
    // Feedback follows checkpoint commits; give it a few intervals before measuring retention.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let final_retained = retained_wal(&client, slots).await;
    server.kill().unwrap();
    server.wait().unwrap();
    let missing = expected_rows
        .keys()
        .filter(|key| !observed_rows.contains_key(*key))
        .count();
    let stale = observed_rows
        .keys()
        .filter(|key| !expected_rows.contains_key(*key))
        .count();
    let different = expected_rows
        .iter()
        .filter(|(key, value)| observed_rows.get(*key).is_some_and(|row| row != *value))
        .count();
    println!("SOAK mirror: missing={missing} stale={stale} different={different}");
    println!(
        "SOAK ran {:.0}s: {transactions} committed transactions, {kills} hard kills, \
         {} final rows, converged {:.1}s after writes stopped, peak server memory {peak_mib} MiB, \
         peak retained WAL keyed/changelog {:?} bytes, final retained {:?} bytes",
        run_for.as_secs_f64(),
        expected_rows.len(),
        converged.as_secs_f64(),
        peak_retained,
        final_retained
    );
    assert!(kills > 0, "the run must include hard kills");
    assert_eq!(observed_rows, expected_rows, "keyed mirror");
    assert_eq!(
        observed_totals, expected_totals,
        "changelog aggregate totals"
    );
    for slot in slots {
        let _ = client
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE starts_with(slot_name::text, $1) AND NOT active",
                &[&format!("{slot}_")],
            )
            .await;
    }
}
