//! Standalone-server PostgreSQL CDC soak: a large initial copy under sustained mixed writes,
//! repeated hard kills, terminated PostgreSQL sessions, and exact final results for a keyed
//! mirror and for retractable aggregates over the changelog.
//!
//! Two servers read one table through slots of their own. `keyed` (upsert source → mirror) is
//! faulted from the start, so kills land inside its copy, which resumes, once the table is large
//! enough for the copy to outlast the first kill (20 M rows do on a workstation). `changes`
//! (changelog source → aggregates) loses its replication session or process only after its copy
//! has completed, because a changelog restart inside the copy fails closed by design; its
//! snapshot session is terminated during the copy, which only retries the chunk.
//!
//! Needs `docker compose -f tests/docker/postgres-cdc-compose.yml up -d --wait`. Run with
//! `cargo test --release -p laminar-server --test postgres_cdc_soak -- --ignored --nocapture`.
//! `LAMINAR_POSTGRES_SOAK_SECONDS` (default 120), `LAMINAR_POSTGRES_SOAK_ROWS` (initial rows,
//! default 200000), `LAMINAR_POSTGRES_SOAK_SEED` (default from the clock; printed so a run can be
//! replayed), and `LAMINAR_TEST_POSTGRES_PORT` (default 15532) size and place the run. Each server
//! is killed every 20–90 s, and a replication or snapshot session is terminated every 10–40 s.

#![cfg(all(feature = "postgres-cdc", feature = "postgres-sink"))]

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const PASSWORD: &str = "laminar-test-secret";
const BUCKETS: i64 = 16;
/// Narrow rows per chunk at the default `max.buffered.bytes`.
const CHUNK_ROWS: i64 = 65_536;
const KILL_SECONDS: (i64, i64) = (20, 90);
const TERMINATE_SECONDS: (i64, i64) = (10, 40);
/// A changelog restart before a checkpoint taken after the copy commits fails closed, so the
/// changelog server is faulted only this long after its copy completes.
const AFTER_COPY: Duration = Duration::from_secs(10);
const COPY_COMPLETE: &str = "PostgreSQL CDC initial snapshot complete";
const MAX_FLUSH_STALL: Duration = Duration::from_secs(60);
const CONVERGE: Duration = Duration::from_secs(600);

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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

/// Deterministic xorshift choices, so a seed replays the same operation mix and fault schedule.
struct Choices(u64);

impl Choices {
    fn below(&mut self, bound: i64) -> i64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        i64::try_from(self.0 % u64::try_from(bound).unwrap()).unwrap()
    }

    fn seconds(&mut self, (low, high): (i64, i64)) -> Duration {
        Duration::from_secs(u64::try_from(low + self.below(high - low + 1)).unwrap())
    }
}

/// Names of one soak run's objects.
struct Run {
    table: String,
    publication: String,
    keyed_slot: String,
    changes_slot: String,
    mirror: String,
    totals: String,
}

impl Run {
    fn new() -> Self {
        let suffix = format!(
            "{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        Self {
            table: format!("soak_{suffix}"),
            publication: format!("soak_pub_{suffix}"),
            keyed_slot: format!("soak_keyed_{suffix}"),
            changes_slot: format!("soak_changes_{suffix}"),
            mirror: format!("soak_mirror_{suffix}"),
            totals: format!("soak_totals_{suffix}"),
        }
    }

    async fn create(&self, client: &tokio_postgres::Client, rows: i64) {
        client
            .batch_execute(&format!(
                "CREATE TABLE {t} (id bigint PRIMARY KEY, bucket text NOT NULL, balance bigint, \
                 note text); ALTER TABLE {t} REPLICA IDENTITY FULL; \
                 CREATE PUBLICATION {p} FOR TABLE {t}; \
                 INSERT INTO {t} SELECT g, 'b' || (g % {BUCKETS}), g, NULL \
                 FROM generate_series(0, {last}) g;",
                t = self.table,
                p = self.publication,
                last = rows - 1
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!("VACUUM (ANALYZE) {}", self.table))
            .await
            .unwrap();
    }

    fn source(&self, slot: &str) -> String {
        format!(
            "'host' = '127.0.0.1', 'port' = '{}', 'database' = 'cdc', 'username' = 'laminar', \
             'password' = '$${{SOAK_PG_PASSWORD}}', 'ssl.mode' = 'disable', \
             'publication' = '{}', 'table' = 'public.{}', 'slot.name' = '{slot}'",
            port(),
            self.publication,
            self.table
        )
    }

    fn keyed_sql(&self) -> String {
        format!(
            "CREATE SOURCE accounts (id BIGINT NOT NULL, bucket VARCHAR, balance BIGINT, \
             note VARCHAR, PRIMARY KEY (id)) FROM \"postgres-cdc\" ({}); \
             CREATE SINK accounts_mirror FROM accounts INTO \"postgres-sink\" ({});",
            self.source(&self.keyed_slot),
            sink(&self.mirror, "id")
        )
    }

    fn changes_sql(&self) -> String {
        format!(
            "CREATE SOURCE account_changes (id BIGINT NOT NULL, bucket VARCHAR NOT NULL, \
             balance BIGINT, __weight BIGINT NOT NULL, PRIMARY KEY (id)) FROM \"postgres-cdc\" \
             ({}, 'output.mode' = 'changelog'); \
             CREATE STREAM bucket_totals AS SELECT bucket, SUM(balance) AS total, COUNT(*) AS n \
             FROM account_changes GROUP BY bucket EMIT CHANGES; \
             CREATE SINK totals_mirror FROM bucket_totals INTO \"postgres-sink\" ({});",
            self.source(&self.changes_slot),
            sink(&self.totals, "bucket")
        )
    }
}

fn sink(table: &str, key: &str) -> String {
    format!(
        "'hostname' = '127.0.0.1', 'port' = '{}', 'database' = 'cdc', 'username' = 'laminar', \
         'password' = '$${{SOAK_PG_PASSWORD}}', 'ssl.mode' = 'disable', \
         'write.mode' = 'upsert', 'changelog.mode' = 'true', 'auto.create.table' = 'true', \
         'table.name' = '{table}', 'primary.key' = '{key}'",
        port()
    )
}

fn server_config(root: &Path, sql: &str) -> String {
    let checkpoint = path_string(&root.join("checkpoints"));
    let checkpoint_url = if checkpoint.starts_with('/') {
        format!("file://{checkpoint}")
    } else {
        format!("file:///{checkpoint}")
    };
    // TOML basic strings accept JSON string escapes; `sql` precedes the first table header.
    format!(
        "sql = {sql}\n[server]\nbind = {bind}\ndelivery = \"at_least_once\"\n\
         [checkpoint]\nurl = {checkpoint_url}\ninterval = \"1s\"\n",
        sql = serde_json::to_string(sql).unwrap(),
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
        .env("RUST_LOG", "warn,laminar_connectors::postgres::cdc=info")
        .env("SOAK_PG_PASSWORD", PASSWORD)
        .stdout(log.try_clone().unwrap())
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

/// How the confirmed position of the slots under one prefix advances.
#[derive(Default)]
struct FlushWatch {
    last: Option<(i64, Instant)>,
    advances: u32,
    longest_stall: Duration,
}

impl FlushWatch {
    fn observe(&mut self, confirmed: Option<i64>, now: Instant) {
        let Some(confirmed) = confirmed else {
            return;
        };
        match self.last {
            Some((last, _)) if confirmed > last => {
                self.advances += 1;
                self.last = Some((confirmed, now));
            }
            Some((_, since)) => self.longest_stall = self.longest_stall.max(now - since),
            None => self.last = Some((confirmed, now)),
        }
    }
}

/// One server process with its own configuration, checkpoints, log and kill schedule.
struct Server {
    name: &'static str,
    slot: String,
    config: PathBuf,
    log: PathBuf,
    child: Child,
    copied_at: Option<Instant>,
    next_kill: Option<Instant>,
    kills: u32,
    kills_in_copy: u32,
    peak_mib: u64,
    next_memory_sample: Instant,
    peak_retained: i64,
    flush: FlushWatch,
}

impl Server {
    fn start(name: &'static str, root: &Path, sql: &str, slot: &str) -> Self {
        let root = root.join(name);
        std::fs::create_dir_all(&root).unwrap();
        let config = root.join("server.toml");
        std::fs::write(&config, server_config(&root, sql)).unwrap();
        let log = root.join("server.log");
        let child = spawn_server(&config, &log);
        Self {
            name,
            slot: slot.to_string(),
            config,
            log,
            child,
            copied_at: None,
            next_kill: None,
            kills: 0,
            kills_in_copy: 0,
            peak_mib: 0,
            next_memory_sample: Instant::now(),
            peak_retained: -1,
            flush: FlushWatch::default(),
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Panics when the process exited on its own or supervision stopped restarting its pipeline.
    fn check_running(&mut self) {
        if let Some(status) = self.child.try_wait().unwrap() {
            panic!(
                "{} server exited on its own with {status}:\n{}",
                self.name,
                self.log()
            );
        }
        let log = self.log();
        if log.contains("non-running") {
            panic!("{} pipeline stopped:\n{log}", self.name);
        }
    }

    /// Whether its copy is still running; notes when it completed.
    fn copying(&mut self) -> bool {
        if self.copied_at.is_none() && self.log().contains(COPY_COMPLETE) {
            self.copied_at = Some(Instant::now());
        }
        self.copied_at.is_none()
    }

    /// Whether a fault now would hit only states that resume, never a changelog restart inside
    /// the copy.
    fn faultable(&mut self, changelog: bool) -> bool {
        !changelog
            || (!self.copying() && self.copied_at.is_some_and(|at| at.elapsed() >= AFTER_COPY))
    }

    fn kill(&mut self, at: Duration, next: Duration) {
        let copying = self.copying();
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.kills += 1;
        self.kills_in_copy += u32::from(copying);
        println!(
            "SOAK {:>7.1}s kill -9 {} ({})",
            at.as_secs_f64(),
            self.name,
            if copying {
                "during the copy"
            } else {
                "after the copy"
            }
        );
        self.child = spawn_server(&self.config, &self.log);
        self.next_kill = Some(Instant::now() + next);
    }

    async fn sample(&mut self, client: &tokio_postgres::Client) {
        if Instant::now() >= self.next_memory_sample {
            self.next_memory_sample = Instant::now() + Duration::from_secs(10);
            if let Some(mib) = resident_mib(self.child.id()) {
                self.peak_mib = self.peak_mib.max(mib);
            }
        }
        let Ok(row) = client
            .query_one(
                "SELECT MAX(pg_wal_lsn_diff(confirmed_flush_lsn, '0/0'))::bigint, \
                 MAX(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))::bigint \
                 FROM pg_replication_slots WHERE starts_with(slot_name::text, $1)",
                &[&format!("{}_", self.slot)],
            )
            .await
        else {
            return;
        };
        self.flush.observe(row.get(0), Instant::now());
        if let Some(retained) = row.get::<_, Option<i64>>(1) {
            self.peak_retained = self.peak_retained.max(retained);
        }
    }

    /// Terminate this server's walsender, or its other sessions (the snapshot session while it
    /// copies), found through the claim in its slot's name; returns how many ended.
    async fn terminate(&self, client: &tokio_postgres::Client, walsender: bool) -> i64 {
        client
            .query_one(
                "SELECT count(pg_terminate_backend(a.pid)) FROM pg_stat_activity a \
                 JOIN pg_replication_slots s ON starts_with(s.slot_name::text, $1) \
                 AND a.application_name LIKE 'laminar:' || right(s.slot_name::text, 16) || ':%' \
                 WHERE (a.backend_type = 'walsender') = $2",
                &[&format!("{}_", self.slot), &walsender],
            )
            .await
            .map_or(0, |row| row.get(0))
    }
}

/// Transactions of one to five statements over the whole key space until `deadline`: updates
/// that move rows between buckets, large out-of-line notes, deletes, reinserts, and primary-key
/// changes across chunk bounds, by `rows` (between the first and the last chunks) and by up to
/// two chunks. Returns the number of committed transactions.
async fn write_load(table: String, rows: i64, seed: u64, deadline: Instant) -> u64 {
    let client = connect().await;
    let mut random = Choices(seed);
    let mut transactions = 0_u64;
    while Instant::now() < deadline {
        let mut statements = Vec::new();
        for _ in 0..=random.below(4) {
            let id = random.below(rows);
            let moved = |to: i64| {
                format!(
                    "UPDATE {table} SET id = {to} WHERE id = {id} \
                     AND NOT EXISTS (SELECT 1 FROM {table} WHERE id = {to})"
                )
            };
            let statement = match random.below(10) {
                0..=3 => format!(
                    "UPDATE {table} SET balance = balance + {}, bucket = 'b{}' WHERE id = {id}",
                    random.below(100) - 50,
                    random.below(BUCKETS)
                ),
                4 => format!(
                    "UPDATE {table} SET note = (SELECT string_agg(md5(g::text), '') \
                     FROM generate_series(1, 200) g) WHERE id = {id}"
                ),
                5..=6 => format!("DELETE FROM {table} WHERE id IN ({id}, {})", id + rows),
                7 => moved(id + rows),
                8 => moved(id + 1 + random.below(2 * CHUNK_ROWS)),
                _ => format!(
                    "INSERT INTO {table} VALUES ({}, 'b{}', {}, NULL) ON CONFLICT (id) DO NOTHING",
                    random.below(2 * rows),
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

/// Kill each server and terminate its sessions on the seeded schedule until `deadline`, while
/// sampling memory, retained WAL and slot progress. Returns the terminations made.
async fn inject_faults(
    client: &tokio_postgres::Client,
    servers: &mut [Server; 2],
    random: &mut Choices,
    deadline: Instant,
) -> u32 {
    let started = Instant::now();
    servers[0].next_kill = Some(started + random.seconds(KILL_SECONDS));
    let mut next_terminate = started + random.seconds(TERMINATE_SECONDS);
    let mut terminations = 0;
    while Instant::now() < deadline {
        for (index, server) in servers.iter_mut().enumerate() {
            server.check_running();
            server.sample(client).await;
            let changelog = index == 1;
            if changelog && server.next_kill.is_none() && !server.copying() {
                server.next_kill = Some(Instant::now() + random.seconds(KILL_SECONDS));
            }
            let due = server.next_kill.is_some_and(|at| Instant::now() >= at);
            if due && server.faultable(changelog) {
                server.kill(started.elapsed(), random.seconds(KILL_SECONDS));
            }
        }
        if Instant::now() >= next_terminate {
            next_terminate = Instant::now() + random.seconds(TERMINATE_SECONDS);
            terminations += terminate_one(client, servers, random, started.elapsed()).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    terminations
}

/// Terminate one eligible session: a walsender, or a snapshot session during its copy.
async fn terminate_one(
    client: &tokio_postgres::Client,
    servers: &mut [Server; 2],
    random: &mut Choices,
    at: Duration,
) -> u32 {
    let mut targets = Vec::new();
    for (index, server) in servers.iter_mut().enumerate() {
        if server.faultable(index == 1) {
            targets.push((index, true));
        }
        if server.copying() {
            targets.push((index, false));
        }
    }
    if targets.is_empty() {
        return 0;
    }
    let (index, walsender) =
        targets[usize::try_from(random.below(i64::try_from(targets.len()).unwrap())).unwrap()];
    let server = &servers[index];
    let ended = server.terminate(client, walsender).await;
    println!(
        "SOAK {:>7.1}s terminated {ended} {} session(s) of {}",
        at.as_secs_f64(),
        if walsender { "walsender" } else { "snapshot" },
        server.name
    );
    u32::from(ended > 0)
}

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

/// Count, key sum, balance sum and a content hash sum of `table`; `None` while it is missing.
async fn digest(client: &tokio_postgres::Client, table: &str) -> Option<String> {
    client
        .query_one(
            &format!(
                "SELECT concat_ws(':', count(*), sum(id), sum(balance), \
                 sum(hashtext(bucket || coalesce(note, '')))) FROM {table}"
            ),
            &[],
        )
        .await
        .ok()
        .map(|row| row.get(0))
}

/// Rows only in the table and rows only in the mirror.
async fn mirror_differences(client: &tokio_postgres::Client, run: &Run) -> Option<(i64, i64)> {
    let (table, mirror) = (&run.table, &run.mirror);
    client
        .query_one(
            &format!(
                "SELECT (SELECT count(*) FROM (SELECT id, bucket, balance, note FROM {table} \
                 EXCEPT SELECT id, bucket, balance, note FROM {mirror}) missing), \
                 (SELECT count(*) FROM (SELECT id, bucket, balance, note FROM {mirror} \
                 EXCEPT SELECT id, bucket, balance, note FROM {table}) extra)"
            ),
            &[],
        )
        .await
        .ok()
        .map(|row| (row.get(0), row.get(1)))
}

/// Results once the writes stopped and the pipelines caught up.
struct Outcome {
    converged: Duration,
    differences: Option<(i64, i64)>,
    expected_totals: Totals,
    observed_totals: Totals,
}

async fn converge(
    client: &tokio_postgres::Client,
    run: &Run,
    servers: &mut [Server; 2],
) -> Outcome {
    let expected = digest(client, &run.table).await;
    let expected_totals = totals(
        client,
        &format!(
            "SELECT bucket, SUM(balance)::bigint, COUNT(*) FROM {} GROUP BY bucket",
            run.table
        ),
    )
    .await;
    let observed_query = format!("SELECT bucket, total::bigint, n FROM {}", run.totals);
    let started = Instant::now();
    let mut observed_totals = Totals::new();
    while started.elapsed() < CONVERGE {
        for server in servers.iter_mut() {
            server.check_running();
        }
        observed_totals = totals(client, &observed_query).await;
        if observed_totals == expected_totals && digest(client, &run.mirror).await == expected {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Outcome {
        converged: started.elapsed(),
        differences: mirror_differences(client, run).await,
        expected_totals,
        observed_totals,
    }
}

/// Inactive slots under the server's prefix that its log never reported as orphans, unless the
/// prefix holds one slot only; and how many slots it holds.
async fn leaked_slots(client: &tokio_postgres::Client, server: &Server) -> (usize, Vec<String>) {
    let slots = client
        .query(
            "SELECT slot_name::text, active FROM pg_replication_slots \
             WHERE starts_with(slot_name::text, $1)",
            &[&format!("{}_", server.slot)],
        )
        .await
        .unwrap();
    if slots.len() <= 1 {
        return (slots.len(), Vec::new());
    }
    let log = server.log();
    let reported = |slot: &str| {
        log.lines().any(|line| {
            line.contains(slot)
                && (line.contains("orphaned PostgreSQL CDC slot")
                    || line.contains("leaving this slot as an orphan"))
        })
    };
    let leaked = slots
        .iter()
        .filter(|row| !row.get::<_, bool>(1))
        .map(|row| row.get::<_, String>(0))
        .filter(|slot| !reported(slot))
        .collect();
    (slots.len(), leaked)
}

/// Stop the servers and drop the run's slots, tables and publication.
async fn clean_up(client: &tokio_postgres::Client, run: &Run, servers: &mut [Server; 2]) {
    for server in servers.iter_mut() {
        let _ = server.child.kill();
        let _ = server.child.wait();
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    for slot in [&run.keyed_slot, &run.changes_slot] {
        let prefix = format!("{slot}_");
        while Instant::now() < deadline {
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
            let left: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_replication_slots \
                     WHERE starts_with(slot_name::text, $1)",
                    &[&prefix],
                )
                .await
                .map_or(1, |row| row.get(0));
            if left == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let _ = client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {}, {}, {}; DROP PUBLICATION IF EXISTS {}",
            run.table, run.mirror, run.totals, run.publication
        ))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: needs the PostgreSQL CDC fixture and minutes of wall time"]
#[allow(clippy::too_many_lines)]
async fn postgres_cdc_survives_repeated_hard_kills_under_sustained_writes() {
    let run_for = Duration::from_secs(env_number("LAMINAR_POSTGRES_SOAK_SECONDS", 120));
    let rows = i64::try_from(env_number("LAMINAR_POSTGRES_SOAK_ROWS", 200_000)).unwrap();
    let clock = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let seed = env_number("LAMINAR_POSTGRES_SOAK_SEED", clock) | 1;
    println!(
        "SOAK seed={seed} rows={rows} seconds={} port={}",
        run_for.as_secs(),
        port()
    );
    let run = Run::new();
    let client = connect().await;
    run.create(&client, rows).await;
    let root = tempfile::tempdir().unwrap();
    let started = Instant::now();
    let deadline = started + run_for;
    let writer = tokio::spawn(write_load(run.table.clone(), rows, seed, deadline));
    let mut servers = [
        Server::start("keyed", root.path(), &run.keyed_sql(), &run.keyed_slot),
        Server::start(
            "changes",
            root.path(),
            &run.changes_sql(),
            &run.changes_slot,
        ),
    ];
    let mut random = Choices(seed.rotate_left(32) | 1);
    let terminations = inject_faults(&client, &mut servers, &mut random, deadline).await;
    let transactions = writer.await.unwrap();
    let outcome = converge(&client, &run, &mut servers).await;
    // Feedback follows checkpoint commits; give it a few intervals before measuring retention.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut slots = Vec::new();
    for server in &mut servers {
        server.sample(&client).await;
        slots.push(leaked_slots(&client, server).await);
    }
    let final_rows: i64 = client
        .query_one(&format!("SELECT count(*) FROM {}", run.table), &[])
        .await
        .unwrap()
        .get(0);
    clean_up(&client, &run, &mut servers).await;

    println!(
        "SOAK ran {:.0}s: {transactions} committed transactions, {terminations} terminations, \
         {final_rows} final rows, converged {:.1}s after writes stopped; mirror (missing, extra) \
         {:?}",
        run_for.as_secs_f64(),
        outcome.converged.as_secs_f64(),
        outcome.differences
    );
    for (server, (count, leaked)) in servers.iter().zip(&slots) {
        println!(
            "SOAK {}: kills {} ({} during the copy), copy complete {}, peak memory {} MiB, \
             confirmed_flush advanced {} times, longest stall {:.1}s, peak retained WAL {} KiB, \
             slots {count}, leaked {leaked:?}",
            server.name,
            server.kills,
            server.kills_in_copy,
            server.copied_at.map_or_else(
                || "never".into(),
                |at| format!("{:.1}s", (at - started).as_secs_f64())
            ),
            server.peak_mib,
            server.flush.advances,
            server.flush.longest_stall.as_secs_f64(),
            server.peak_retained >> 10,
        );
    }
    assert!(servers[0].kills > 0, "the run must include hard kills");
    assert_eq!(outcome.differences, Some((0, 0)), "keyed mirror");
    assert_eq!(
        outcome.observed_totals, outcome.expected_totals,
        "changelog aggregate totals"
    );
    for (server, (_, leaked)) in servers.iter().zip(&slots) {
        assert!(leaked.is_empty(), "{} leaked slots {leaked:?}", server.name);
        assert!(server.flush.advances > 0, "{} never confirmed", server.name);
        assert!(
            server.flush.longest_stall < MAX_FLUSH_STALL,
            "{} confirmed nothing for {:?}",
            server.name,
            server.flush.longest_stall
        );
    }
}
