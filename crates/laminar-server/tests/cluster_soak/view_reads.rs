//! Materialized-view read oracle for the three-node exactly-once soak.
//!
//! Four views over `join_left`, which the producer writes as `id` on partition `id % P`:
//!
//! - `soak_view_progress` counts each partition's rows, so a read shows which input prefix its cut
//!   holds and whether that prefix has a gap or a duplicate;
//! - `soak_view_keys`, a keyed aggregate;
//! - `soak_view_counts`, its rows without the key (placed by the full row);
//! - `soak_view_rows`, an append-only view.
//!
//! A reader queries all four in one pgwire statement at the committed level, rotating across the
//! nodes through every `kill -9` and paused-leader round. Each read must equal the views computed
//! from the input prefix its own progress rows describe, the prefix must never shrink, the same
//! epoch must read the same rows on every node, and every epoch must be at least the floor the
//! session set. After each pgwire session the reader reads the same node over HTTP, checked by
//! row counts and sums. Neither may be refused during coordinated recovery. A WebSocket
//! subscriber folds `soak_view_keys`; its fold at each progress epoch must equal the reader's rows
//! at that epoch, a subscription open on a node a fault spares must outlive the round, and a
//! connection that ends resumes at its last progress epoch. After the input stops, every node must
//! read every produced row at the final cut, over pgwire and HTTP.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use sha2::{Digest as _, Sha256};

use super::{
    connect_cluster_subscription_gateway, subscription_websocket_error_is_retryable, wait_for,
    Node, ProducedPrefix, ZipfSampler, BASE_PORT, NODES, SOAK_CONSOLE_TOKEN,
};

const PGWIRE_PORT_OFFSET: u16 = 200;
const KEYED_VIEW: &str = "soak_view_keys";
const APPEND_EVERY: u64 = 16;
const READS_PER_SESSION: u32 = 3;
const READ_INTERVAL: Duration = Duration::from_millis(500);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
// A committed read with a floor waits up to 30 s for that epoch before failing with 57014.
const READ_TIMEOUT: Duration = Duration::from_secs(45);
const READ_SQL: &str = "\
SELECT 'p' AS kind, part AS a, n AS b, max_id AS c, id_sum AS d FROM soak_view_progress \
UNION ALL SELECT 'k', join_key, n, max_id, CAST(NULL AS BIGINT) AS d FROM soak_view_keys \
UNION ALL SELECT 'c', n, max_id, CAST(NULL AS BIGINT) AS c, CAST(NULL AS BIGINT) AS d \
FROM soak_view_counts \
UNION ALL SELECT 'r', id, join_key, CAST(NULL AS BIGINT) AS c, CAST(NULL AS BIGINT) AS d \
FROM soak_view_rows";
/// The same views over HTTP, within its 1,000-row cap: the progress rows and each other view's
/// row count and sums.
const HTTP_READ_SQL: &str = "\
SELECT 'p' AS kind, part AS a, n AS b, max_id AS c, id_sum AS d FROM soak_view_progress \
UNION ALL SELECT 'k', COUNT(*) AS a, SUM(n) AS b, SUM(max_id) AS c, CAST(NULL AS BIGINT) AS d \
FROM soak_view_keys \
UNION ALL SELECT 'c', COUNT(*) AS a, SUM(n) AS b, SUM(max_id) AS c, CAST(NULL AS BIGINT) AS d \
FROM soak_view_counts \
UNION ALL SELECT 'r', COUNT(*) AS a, SUM(id) AS b, SUM(join_key) AS c, CAST(NULL AS BIGINT) AS d \
FROM soak_view_rows";
/// Committed-read errors HTTP answers with 400 while a node catches up, as pgwire's retryable
/// SQLSTATEs below. A read that reaches its deadline answers 504.
const RETRYABLE_HTTP_ERRORS: [&str; 4] = [
    "no checkpoint has committed",
    "is not retained",
    "hold checkpoints",
    "has no committed state",
];
/// SQLSTATEs a committed read can return while the cluster recovers: no committed cut yet or the
/// view not yet in it, the floor not committed in time, shutdown, a retired cut, pin capacity.
const RETRYABLE_SQLSTATES: [&str; 5] = ["55000", "57014", "57P01", "22023", "53000"];

/// The `pgwire_bind` port of soak node `id`.
pub(super) fn pgwire_port(id: usize) -> u16 {
    BASE_PORT + PGWIRE_PORT_OFFSET + u16::try_from(id).expect("soak node id fits u16")
}

/// Top-level server configuration declaring the views; it must precede every TOML table.
pub(super) fn views_config(partitions: i32) -> String {
    format!(
        r#"sql = """
CREATE MATERIALIZED VIEW soak_view_progress AS
SELECT id % {partitions} AS part, COUNT(*) AS n, MAX(id) AS max_id, SUM(id) AS id_sum
FROM join_left WHERE join_key >= 0 GROUP BY id % {partitions};
CREATE MATERIALIZED VIEW {KEYED_VIEW} AS
SELECT join_key, COUNT(*) AS n, MAX(id) AS max_id
FROM join_left WHERE join_key >= 0 GROUP BY join_key;
CREATE MATERIALIZED VIEW soak_view_counts AS SELECT n, max_id FROM {KEYED_VIEW};
CREATE MATERIALIZED VIEW soak_view_rows AS
SELECT id, join_key, event_time FROM join_left WHERE join_key >= 0 AND id % {APPEND_EVERY} = 0
WITH (retention = '1 hour');
"""
"#
    )
}

/// One committed read of every view, keyed so that equal contents compare equal.
#[derive(Debug, Default, PartialEq, Eq)]
struct ViewRows {
    /// Partition → (rows, highest id, id sum).
    progress: BTreeMap<u64, (u64, u64, u128)>,
    /// Join key → (rows, highest id).
    keys: BTreeMap<u64, (u64, u64)>,
    /// (rows, highest id) → how many keys have it.
    counts: BTreeMap<(u64, u64), u64>,
    /// Id → join key.
    rows: BTreeMap<u64, u64>,
}

fn digest<T: std::fmt::Debug>(value: &T) -> [u8; 32] {
    Sha256::digest(format!("{value:?}")).into()
}

/// Computes the views from an input prefix with the producer's deterministic key sampler.
struct ViewOracle {
    partitions: u64,
    sampler: ZipfSampler,
    keys: Vec<u64>,
}

impl ViewOracle {
    fn new(partitions: i32, key_count: u64, zipf_milli: u64) -> Self {
        Self {
            partitions: u64::try_from(partitions).expect("positive Kafka partition count"),
            sampler: ZipfSampler::new(key_count, zipf_milli),
            keys: Vec::new(),
        }
    }

    fn key(&mut self, id: u64) -> u64 {
        let id = usize::try_from(id).expect("soak id fits usize");
        while self.keys.len() <= id {
            let next = self.keys.len() as u64;
            self.keys.push(self.sampler.sample(next));
        }
        self.keys[id]
    }

    /// Each partition's highest id, after checking that the partition's rows are exactly the ids
    /// up to it.
    fn prefix(&self, rows: &ViewRows) -> Result<Vec<Option<u64>>, String> {
        let mut prefix = vec![None; usize::try_from(self.partitions).expect("partitions fit")];
        for (&part, &(n, max_id, id_sum)) in &rows.progress {
            let contiguous = part < self.partitions
                && max_id % self.partitions == part
                && n == (max_id - part) / self.partitions + 1
                && id_sum == u128::from(n) * u128::from(part + max_id) / 2;
            if !contiguous {
                return Err(format!(
                    "partition {part} holds {n} rows up to id {max_id} with id sum {id_sum}, \
                     not a contiguous prefix"
                ));
            }
            prefix[usize::try_from(part).expect("partition fits")] = Some(max_id);
        }
        Ok(prefix)
    }

    fn expected(&mut self, prefix: &[Option<u64>]) -> ViewRows {
        let mut expected = ViewRows::default();
        for (part, max_id) in prefix.iter().enumerate() {
            let Some(max_id) = *max_id else { continue };
            let part = part as u64;
            let n = (max_id - part) / self.partitions + 1;
            expected.progress.insert(
                part,
                (n, max_id, u128::from(n) * u128::from(part + max_id) / 2),
            );
            for id in (part..=max_id).step_by(usize::try_from(self.partitions).expect("fits")) {
                let key = self.key(id);
                let entry = expected.keys.entry(key).or_insert((0, 0));
                entry.0 += 1;
                entry.1 = entry.1.max(id);
                if id % APPEND_EVERY == 0 {
                    expected.rows.insert(id, key);
                }
            }
        }
        for value in expected.keys.values() {
            *expected.counts.entry(*value).or_insert(0) += 1;
        }
        expected
    }

    /// The prefix a read holds, once the read equals the views over that prefix.
    fn check(&mut self, rows: &ViewRows) -> Result<Vec<Option<u64>>, String> {
        let prefix = self.prefix(rows)?;
        let expected = self.expected(&prefix);
        for (view, equal) in [
            ("soak_view_keys", rows.keys == expected.keys),
            ("soak_view_counts", rows.counts == expected.counts),
            ("soak_view_rows", rows.rows == expected.rows),
        ] {
            if !equal {
                return Err(format!(
                    "{view} differs from batch SQL over the read's input prefix ({} input rows): {}",
                    expected.progress.values().map(|row| row.0).sum::<u64>(),
                    first_difference(rows, &expected)
                ));
            }
        }
        Ok(prefix)
    }

    /// Each partition's highest id once the producer wrote ids `0..count`.
    fn produced(&self, count: u64) -> Vec<Option<u64>> {
        (0..self.partitions)
            .map(|part| {
                (count > part)
                    .then(|| part + (count - 1 - part) / self.partitions * self.partitions)
            })
            .collect()
    }
}

fn first_difference(rows: &ViewRows, expected: &ViewRows) -> String {
    fn diff<K: Ord + std::fmt::Debug, V: PartialEq + std::fmt::Debug>(
        name: &str,
        read: &BTreeMap<K, V>,
        expected: &BTreeMap<K, V>,
    ) -> Option<String> {
        let keys: BTreeSet<&K> = read.keys().chain(expected.keys()).collect();
        keys.into_iter()
            .find(|key| read.get(key) != expected.get(key))
            .map(|key| {
                format!(
                    "{name}[{key:?}] read {:?}, expected {:?} ({} read rows, {} expected)",
                    read.get(key),
                    expected.get(key),
                    read.len(),
                    expected.len()
                )
            })
    }
    diff("keys", &rows.keys, &expected.keys)
        .or_else(|| diff("counts", &rows.counts, &expected.counts))
        .or_else(|| diff("rows", &rows.rows, &expected.rows))
        .unwrap_or_default()
}

fn parse_rows(messages: &[tokio_postgres::SimpleQueryMessage]) -> Result<ViewRows, String> {
    let mut rows = ViewRows::default();
    for message in messages {
        let tokio_postgres::SimpleQueryMessage::Row(row) = message else {
            continue;
        };
        let column = |index: usize| -> Result<u64, String> {
            row.get(index)
                .ok_or_else(|| format!("view read column {index} is NULL"))?
                .parse::<u64>()
                .map_err(|error| format!("view read column {index}: {error}"))
        };
        let duplicate = match row.get(0) {
            Some("p") => rows
                .progress
                .insert(column(1)?, (column(2)?, column(3)?, u128::from(column(4)?)))
                .is_some(),
            Some("k") => rows
                .keys
                .insert(column(1)?, (column(2)?, column(3)?))
                .is_some(),
            Some("c") => {
                *rows.counts.entry((column(1)?, column(2)?)).or_insert(0) += 1;
                false
            }
            Some("r") => rows.rows.insert(column(1)?, column(2)?).is_some(),
            kind => return Err(format!("view read returned an unknown row kind {kind:?}")),
        };
        if duplicate {
            return Err(format!("view read returned a key twice: {:?}", row.get(1)));
        }
    }
    Ok(rows)
}

enum ReadFailure {
    /// The node is down, paused or recovering; the reader moves on.
    Retryable(String),
    Fatal(String),
}

impl From<tokio_postgres::Error> for ReadFailure {
    fn from(error: tokio_postgres::Error) -> Self {
        match error.code().map(tokio_postgres::error::SqlState::code) {
            Some(code) if !RETRYABLE_SQLSTATES.contains(&code) => Self::Fatal(format!(
                "committed view read failed with {code}: {}",
                error
                    .as_db_error()
                    .map_or_else(|| error.to_string(), |db| db.message().to_owned())
            )),
            Some(code) => Self::Retryable(code.to_owned()),
            None => Self::Retryable("connection".to_owned()),
        }
    }
}

async fn connect(node: usize) -> Result<tokio_postgres::Client, ReadFailure> {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(pgwire_port(node))
        .user("soak")
        .dbname("laminardb")
        .connect_timeout(CONNECT_TIMEOUT);
    let connecting = config.connect(tokio_postgres::NoTls);
    // A paused node accepts the TCP connection but never answers the startup handshake.
    let (client, connection) = tokio::time::timeout(CONNECT_TIMEOUT, connecting)
        .await
        .map_err(|_| ReadFailure::Retryable("connect timeout".to_owned()))??;
    tokio::spawn(connection);
    client
        .batch_execute("SET laminar.read_level = 'committed'")
        .await?;
    Ok(client)
}

async fn read_once(client: &tokio_postgres::Client) -> Result<(u64, ViewRows), ReadFailure> {
    let read = async {
        let rows = client.simple_query(READ_SQL).await?;
        let epoch = client.simple_query("SHOW laminar.read_epoch").await?;
        Ok::<_, ReadFailure>((rows, epoch))
    };
    let (rows, epoch) = tokio::time::timeout(READ_TIMEOUT, read)
        .await
        .map_err(|_| ReadFailure::Retryable("read timeout".to_owned()))??;
    let epoch = epoch
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        })
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| ReadFailure::Fatal("SHOW laminar.read_epoch returned no epoch".into()))?;
    let rows = parse_rows(&rows).map_err(ReadFailure::Fatal)?;
    Ok((epoch, rows))
}

/// Row count and sums of `soak_view_keys`, `soak_view_counts` and `soak_view_rows`.
type ViewSums = [(u64, u64, u64); 3];

fn view_sums(rows: &ViewRows) -> ViewSums {
    let keys = rows.keys.values();
    let counts = rows.counts.iter();
    [
        (
            rows.keys.len() as u64,
            keys.clone().map(|value| value.0).sum(),
            keys.map(|value| value.1).sum(),
        ),
        counts
            .clone()
            .fold((0, 0, 0), |sums, (&(n, max_id), &count)| {
                (sums.0 + count, sums.1 + n * count, sums.2 + max_id * count)
            }),
        (
            rows.rows.len() as u64,
            rows.rows.keys().sum(),
            rows.rows.values().sum(),
        ),
    ]
}

/// One committed read over `POST /api/v1/sql`, checked like a pgwire read but by row counts and
/// sums. Returns the epoch it read.
fn http_read(node: usize, floor: u64, oracle: &mut ViewOracle) -> Result<u64, ReadFailure> {
    let body = serde_json::json!({
        "sql": HTTP_READ_SQL,
        "read_level": "committed",
        "min_epoch": floor,
    })
    .to_string();
    let (status, response) = http_post(node, "/api/v1/sql", &body)
        .map_err(|_| ReadFailure::Retryable("http connection".to_owned()))?;
    match status {
        200 => {}
        503 if response.contains("coordinated recovery") => {
            return Err(ReadFailure::Fatal(format!(
                "node{node} refused an HTTP committed read during coordinated recovery: {response}"
            )))
        }
        503 => return Err(ReadFailure::Retryable("http 503".to_owned())),
        504 => return Err(ReadFailure::Retryable("http 504".to_owned())),
        400 if RETRYABLE_HTTP_ERRORS
            .iter()
            .any(|error| response.contains(error)) =>
        {
            return Err(ReadFailure::Retryable("http catching up".to_owned()))
        }
        _ => {
            return Err(ReadFailure::Fatal(format!(
                "node{node} HTTP committed read failed with {status}: {response}"
            )))
        }
    }
    let fatal = |error: String| ReadFailure::Fatal(format!("node{node} HTTP read: {error}"));
    let response: serde_json::Value =
        serde_json::from_str(&response).map_err(|error| fatal(error.to_string()))?;
    let epoch = json_u64(&response, "epoch").map_err(fatal)?;
    if epoch < floor {
        return Err(fatal(format!("epoch {epoch} below the floor {floor}")));
    }
    let column = |row: &serde_json::Value, field: &str| {
        if row.get(field).is_some_and(serde_json::Value::is_null) {
            Ok(0)
        } else {
            json_u64(row, field)
        }
    };
    let mut progress = ViewRows::default();
    let mut sums = [(0, 0, 0); 3];
    for row in response["data"].as_array().into_iter().flatten() {
        let values = (
            column(row, "a").map_err(fatal)?,
            column(row, "b").map_err(fatal)?,
            column(row, "c").map_err(fatal)?,
        );
        match row["kind"].as_str() {
            Some("p") => {
                let id_sum = u128::from(column(row, "d").map_err(fatal)?);
                progress
                    .progress
                    .insert(values.0, (values.1, values.2, id_sum));
            }
            Some("k") => sums[0] = values,
            Some("c") => sums[1] = values,
            Some("r") => sums[2] = values,
            kind => return Err(fatal(format!("unknown row kind {kind:?}"))),
        }
    }
    let prefix = oracle
        .prefix(&progress)
        .map_err(|error| fatal(format!("epoch {epoch}: {error}")))?;
    let expected = view_sums(&oracle.expected(&prefix));
    if sums != expected {
        return Err(fatal(format!(
            "epoch {epoch}: view sums {sums:?} differ from batch SQL over the read's input prefix \
             {expected:?}"
        )));
    }
    Ok(epoch)
}

fn http_post(node: usize, path: &str, body: &str) -> std::io::Result<(u16, String)> {
    let port = BASE_PORT + u16::try_from(node).expect("soak node id fits u16");
    let mut stream = TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        CONNECT_TIMEOUT,
    )?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    stream.set_write_timeout(Some(CONNECT_TIMEOUT))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {SOAK_CONSOLE_TOKEN}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| std::io::Error::other("HTTP response without a body"))?;
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .ok_or_else(|| std::io::Error::other("HTTP response without a status"))?;
    Ok((status, body.to_owned()))
}

/// What the reader observed, shared with the soak thread.
#[derive(Default)]
struct ReadEvidence {
    floor: u64,
    reads_by_node: [u64; NODES],
    http_reads_by_node: [u64; NODES],
    retried: BTreeMap<String, u64>,
    last_prefix: Option<(u64, Vec<Option<u64>>)>,
    read_digests: BTreeMap<u64, [u8; 32]>,
    key_digests: BTreeMap<u64, [u8; 32]>,
}

impl ReadEvidence {
    fn record(
        &mut self,
        node: usize,
        epoch: u64,
        rows: &ViewRows,
        prefix: Vec<Option<u64>>,
    ) -> Result<(), String> {
        if epoch < self.floor {
            return Err(format!(
                "node{node} read epoch {epoch} below the session floor {}",
                self.floor
            ));
        }
        let read_digest = digest(rows);
        if self
            .read_digests
            .get(&epoch)
            .is_some_and(|existing| *existing != read_digest)
        {
            return Err(format!(
                "node{node} read different rows at epoch {epoch} than an earlier read"
            ));
        }
        if let Some((previous_epoch, previous)) = &self.last_prefix {
            let shrank = previous
                .iter()
                .zip(&prefix)
                .position(|(before, now)| now < before);
            if let Some(part) = shrank {
                return Err(format!(
                    "node{node} read at epoch {epoch} holds less of partition {part} than epoch \
                     {previous_epoch}: {:?} < {:?}",
                    prefix[part], previous[part]
                ));
            }
        }
        self.read_digests.insert(epoch, read_digest);
        self.key_digests.insert(epoch, digest(&rows.keys));
        self.floor = epoch;
        self.last_prefix = Some((epoch, prefix));
        self.reads_by_node[node] += 1;
        Ok(())
    }
}

/// Reads on one node until the session's read budget or the first failure.
async fn read_session(
    node: usize,
    stop: &AtomicBool,
    evidence: &parking_lot::Mutex<ReadEvidence>,
    oracle: &mut ViewOracle,
) -> Result<(), ReadFailure> {
    let client = connect(node).await?;
    let floor = evidence.lock().floor;
    client
        .batch_execute(&format!("SET laminar.min_epoch = {floor}"))
        .await?;
    for _ in 0..READS_PER_SESSION {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let (epoch, rows) = read_once(&client).await?;
        let prefix = oracle
            .check(&rows)
            .map_err(|error| ReadFailure::Fatal(format!("node{node} epoch {epoch}: {error}")))?;
        evidence
            .lock()
            .record(node, epoch, &rows, prefix)
            .map_err(ReadFailure::Fatal)?;
        tokio::time::sleep(READ_INTERVAL).await;
    }
    Ok(())
}

fn run_reader(
    stop: &AtomicBool,
    evidence: &parking_lot::Mutex<ReadEvidence>,
    mut oracle: ViewOracle,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("view reader runtime: {error}"))?;
    let mut sessions = 0usize;
    while !stop.load(Ordering::Acquire) {
        let node = sessions % NODES;
        sessions += 1;
        let pgwire = runtime.block_on(read_session(node, stop, evidence, &mut oracle));
        let floor = evidence.lock().floor;
        let http = http_read(node, floor, &mut oracle).map(|_| {
            evidence.lock().http_reads_by_node[node] += 1;
        });
        for result in [pgwire, http] {
            match result {
                Ok(()) => {}
                Err(ReadFailure::Retryable(reason)) => {
                    *evidence.lock().retried.entry(reason).or_insert(0) += 1;
                    std::thread::sleep(Duration::from_millis(200));
                }
                Err(ReadFailure::Fatal(error)) => return Err(error),
            }
        }
    }
    Ok(())
}

/// What the subscriber observed, shared with the soak thread.
#[derive(Default)]
struct SubscriptionEvidence {
    progress: BTreeMap<u64, [u8; 32]>,
    gateways: BTreeSet<usize>,
    connections: u64,
    /// The open connection's number and gateway.
    open: Option<(u64, usize)>,
    /// Each connection's last progress epoch.
    last_progress: BTreeMap<u64, u64>,
    /// Connections that started at the previous connection's last progress epoch.
    resumed: u64,
    /// Resumes refused because that epoch was no longer retained (410), restarted at the tail.
    resume_lost: u64,
    /// Set by the soak thread to move the subscription to the next gateway.
    rotate: bool,
    retried: BTreeMap<String, u64>,
}

impl SubscriptionEvidence {
    fn retry(&mut self, reason: &str) {
        let reason = reason.split(':').next().unwrap_or(reason).to_owned();
        *self.retried.entry(reason).or_insert(0) += 1;
    }
}

/// One gateway connection's fold of `soak_view_keys`: full row → count.
#[derive(Default)]
struct Fold {
    rows: BTreeMap<(u64, u64, u64), i64>,
    epoch: Option<u64>,
    /// The epoch of the data frames since the last progress frame.
    pending: Option<u64>,
    progress: u64,
}

fn json_u64(value: &serde_json::Value, field: &str) -> Result<u64, String> {
    let value = value
        .get(field)
        .ok_or_else(|| format!("view frame omitted {field}"))?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| format!("view frame {field} is not an unsigned integer: {value}"))
}

fn json_i64(value: &serde_json::Value, field: &str) -> Result<i64, String> {
    let value = value
        .get(field)
        .ok_or_else(|| format!("view frame omitted {field}"))?;
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| format!("view frame {field} is not an integer: {value}"))
}

impl Fold {
    fn data(&mut self, frame: &serde_json::Value) -> Result<(), String> {
        let snapshot = frame
            .get("snapshot")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| "view data frame omitted snapshot".to_string())?;
        if snapshot == (self.progress > 0) {
            return Err(format!(
                "view data frame snapshot={snapshot} after {} progress frames",
                self.progress
            ));
        }
        let epoch = json_u64(frame, "committed_epoch")?;
        if self.epoch.is_some_and(|previous| epoch <= previous)
            || self.pending.is_some_and(|pending| pending != epoch)
        {
            return Err(format!(
                "view data epoch {epoch} after progress {:?} and data {:?}",
                self.epoch, self.pending
            ));
        }
        self.pending = Some(epoch);
        let rows = frame
            .get("data")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "view data frame omitted its rows".to_string())?;
        for row in rows {
            let key = (
                json_u64(row, "join_key")?,
                json_u64(row, "n")?,
                json_u64(row, "max_id")?,
            );
            *self.rows.entry(key).or_insert(0) += json_i64(row, "__weight")?;
        }
        Ok(())
    }

    /// The folded view at a progress frame's epoch.
    fn progress(&mut self, frame: &serde_json::Value) -> Result<(u64, [u8; 32]), String> {
        let epoch = json_u64(frame, "epoch")?;
        if self.epoch.is_some_and(|previous| epoch <= previous)
            || self.pending.take().is_some_and(|pending| pending != epoch)
        {
            return Err(format!(
                "view progress epoch {epoch} after progress {:?}",
                self.epoch
            ));
        }
        self.rows.retain(|_, count| *count != 0);
        let mut keys = BTreeMap::new();
        for (&(key, n, max_id), &count) in &self.rows {
            if count != 1 || keys.insert(key, (n, max_id)).is_some() {
                return Err(format!(
                    "view fold at epoch {epoch} holds join key {key} with count {count} or twice"
                ));
            }
        }
        self.epoch = Some(epoch);
        self.progress += 1;
        Ok((epoch, digest(&keys)))
    }
}

/// Folds one gateway connection until the soak rotates it, a stop, or a transport failure.
fn consume_view_gateway(
    socket: &mut tungstenite::WebSocket<TcpStream>,
    connection: u64,
    stop: &AtomicBool,
    evidence: &parking_lot::Mutex<SubscriptionEvidence>,
) -> Result<(), String> {
    let mut fold = Fold::default();
    while !stop.load(Ordering::Acquire) {
        if std::mem::take(&mut evidence.lock().rotate) {
            return Ok(());
        }
        let text = match socket.read() {
            Ok(tungstenite::Message::Text(text)) => text,
            Ok(tungstenite::Message::Ping(payload)) => {
                socket
                    .send(tungstenite::Message::Pong(payload))
                    .map_err(|error| format!("retryable view subscription pong: {error}"))?;
                continue;
            }
            Ok(tungstenite::Message::Close(_)) => {
                return Err("retryable view subscription closed".to_string())
            }
            Ok(_) => continue,
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue
            }
            Err(error) if subscription_websocket_error_is_retryable(&error) => {
                return Err("retryable view subscription transport closed".to_string());
            }
            Err(error) => return Err(format!("view subscription WebSocket failed: {error}")),
        };
        let frame: serde_json::Value = serde_json::from_str(text.as_ref())
            .map_err(|error| format!("invalid view subscription frame: {error}"))?;
        match frame.get("type").and_then(serde_json::Value::as_str) {
            Some("data") => fold.data(&frame)?,
            Some("progress") => {
                let (epoch, keys) = fold.progress(&frame)?;
                let mut evidence = evidence.lock();
                evidence.last_progress.insert(connection, epoch);
                let existing = *evidence.progress.entry(epoch).or_insert(keys);
                if existing != keys {
                    return Err(format!(
                        "two view subscriptions folded different rows at epoch {epoch}"
                    ));
                }
            }
            other => return Err(format!("view subscription sent {other:?}: {frame}")),
        }
    }
    Ok(())
}

fn run_subscriber(
    stop: &AtomicBool,
    evidence: &parking_lot::Mutex<SubscriptionEvidence>,
) -> Result<(), String> {
    let mut attempts = 0usize;
    // A connection that ends (its gateway stopped, or the soak rotated it) resumes at the last
    // progress epoch; its snapshot there must equal the fold the last one reached.
    let mut tail = false;
    while !stop.load(Ordering::Acquire) {
        let gateway = attempts % NODES;
        attempts += 1;
        let resume = if tail {
            None
        } else {
            evidence.lock().progress.keys().next_back().copied()
        };
        let socket = match connect_cluster_subscription_gateway(KEYED_VIEW, gateway, resume) {
            Ok(socket) => socket,
            Err(error) if resume.is_some() && error.contains(" 410 ") => {
                evidence.lock().resume_lost += 1;
                tail = true;
                continue;
            }
            // A node that just rejoined may not have learned of that epoch yet.
            Err(error) if resume.is_some() && error.contains(" 409 ") => {
                evidence.lock().retry("resume epoch not yet committed here");
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
            Err(error) => return Err(error),
        };
        let Some(mut socket) = socket else {
            evidence.lock().retry("gateway unavailable");
            std::thread::sleep(Duration::from_millis(200));
            continue;
        };
        tail = false;
        let connection = {
            let mut evidence = evidence.lock();
            evidence.gateways.insert(gateway);
            evidence.connections += 1;
            evidence.resumed += u64::from(resume.is_some());
            evidence.open = Some((evidence.connections, gateway));
            evidence.connections
        };
        let result = consume_view_gateway(&mut socket, connection, stop, evidence);
        let mut evidence = evidence.lock();
        evidence.open = None;
        match result {
            Ok(()) => {}
            Err(error) if error.starts_with("retryable") => evidence.retry(&error),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// A background observer thread whose failure fails the soak at its next check.
struct Observer<S> {
    name: &'static str,
    stop: Arc<AtomicBool>,
    evidence: Arc<parking_lot::Mutex<S>>,
    handle: Option<JoinHandle<Result<(), String>>>,
}

impl<S: Default + Send + 'static> Observer<S> {
    fn spawn(
        name: &'static str,
        run: impl FnOnce(&AtomicBool, &parking_lot::Mutex<S>) -> Result<(), String> + Send + 'static,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let evidence = Arc::new(parking_lot::Mutex::new(S::default()));
        let (task_stop, task_evidence) = (Arc::clone(&stop), Arc::clone(&evidence));
        let handle = std::thread::Builder::new()
            .name(name.into())
            .spawn(move || run(&task_stop, &task_evidence))
            .expect("spawn view soak observer");
        Self {
            name,
            stop,
            evidence,
            handle: Some(handle),
        }
    }

    fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("observer handle").join() {
            Ok(Ok(())) => panic!("{} stopped before the soak", self.name),
            Ok(Err(error)) => panic!("{} failed: {error}", self.name),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    fn wait_until(&mut self, what: &str, window: Duration, done: impl Fn(&S) -> bool) {
        wait_for(what, window, || {
            self.assert_running();
            done(&self.evidence.lock())
        });
    }

    fn stop(mut self) -> Arc<parking_lot::Mutex<S>> {
        self.stop.store(true, Ordering::Release);
        match self
            .handle
            .take()
            .expect("observer was already stopped")
            .join()
        {
            Ok(Ok(())) => Arc::clone(&self.evidence),
            Ok(Err(error)) => panic!("{} failed: {error}", self.name),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

impl<S> Drop for Observer<S> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// The view reader and subscriber of one soak run.
pub(super) struct ViewSoak {
    partitions: i32,
    key_count: u64,
    zipf_milli: u64,
    reader: Observer<ReadEvidence>,
    subscriber: Observer<SubscriptionEvidence>,
    /// The subscription connection open on a node the current round's fault spares.
    spared: Option<u64>,
    /// Rounds whose spared subscription delivered the recovered checkpoint.
    spanned: u64,
}

impl ViewSoak {
    /// Start reading once the cluster has committed its first checkpoint.
    pub(super) fn start(
        partitions: i32,
        key_count: u64,
        zipf_milli: u64,
        window: Duration,
    ) -> Self {
        let oracle = ViewOracle::new(partitions, key_count, zipf_milli);
        let mut soak = Self {
            partitions,
            key_count,
            zipf_milli,
            reader: Observer::spawn("view-soak-reader", move |stop, evidence| {
                run_reader(stop, evidence, oracle)
            }),
            subscriber: Observer::spawn("view-soak-subscriber", run_subscriber),
            spared: None,
            spanned: 0,
        };
        soak.reader
            .wait_until("a first committed view read", window, |evidence| {
                evidence.reads_by_node.iter().any(|reads| *reads > 0)
            });
        soak.subscriber
            .wait_until("a first view subscription progress", window, |evidence| {
                !evidence.progress.is_empty()
            });
        soak
    }

    /// Remember the subscription open on a node the fault spares; it must outlive the round.
    pub(super) fn before_fault(&mut self, victim: usize) {
        self.subscriber.assert_running();
        self.spared = self
            .subscriber
            .evidence
            .lock()
            .open
            .filter(|(_, gateway)| *gateway != victim)
            .map(|(connection, _)| connection);
    }

    /// After a fault round: both observers are healthy and have reached the recovered cut, and a
    /// subscription that was open on a spared node delivered it. The subscription then moves to
    /// the next gateway.
    pub(super) fn after_round(&mut self, epoch: u64, window: Duration) {
        self.reader.wait_until(
            "a committed view read after the round",
            window,
            |evidence| evidence.floor >= epoch,
        );
        self.subscriber.wait_until(
            "view subscription progress after the round",
            window,
            |evidence| {
                evidence
                    .progress
                    .last_key_value()
                    .is_some_and(|(e, _)| *e >= epoch)
            },
        );
        let mut subscription = self.subscriber.evidence.lock();
        if let Some(connection) = self.spared.take() {
            let delivered = subscription.last_progress.get(&connection).copied();
            assert!(
                delivered.is_some_and(|delivered| delivered >= epoch),
                "the view subscription open on a surviving node ended during the round: \
                 connection {connection} last delivered {delivered:?}, recovered epoch {epoch}"
            );
            self.spanned += 1;
        }
        subscription.rotate = true;
        let reads = self.reader.evidence.lock();
        eprintln!(
            "soak: view reads through epoch {} (pgwire {:?}, HTTP {:?} per node; retried {:?}); \
             subscription connections {} ({} kept open through a fault, {} resumed, {} resumes \
             lost; retried {:?})",
            reads.floor,
            reads.reads_by_node,
            reads.http_reads_by_node,
            reads.retried,
            subscription.connections,
            self.spanned,
            subscription.resumed,
            subscription.resume_lost,
            subscription.retried
        );
    }

    /// Once the input is frozen and durable at `epoch`: every node reads every produced row, the
    /// subscription folds to the same rows, and the observers agree at every shared epoch.
    pub(super) fn finish(
        mut self,
        nodes: &[Node],
        epoch: u64,
        produced: &ProducedPrefix,
        window: Duration,
    ) {
        self.after_round(epoch, window);
        let reads = self.reader.stop();
        let subscription = self.subscriber.stop();
        let reads = reads.lock();
        let subscription = subscription.lock();

        let mut oracle = ViewOracle::new(self.partitions, self.key_count, self.zipf_milli);
        let complete = oracle.expected(&oracle.produced(produced.count));
        assert_final_pgwire_reads(nodes, epoch, &complete);
        assert_final_http_reads(nodes, epoch, &complete);

        let shared: Vec<u64> = subscription
            .progress
            .keys()
            .filter(|epoch| reads.key_digests.contains_key(epoch))
            .copied()
            .collect();
        for shared_epoch in &shared {
            assert_eq!(
                subscription.progress[shared_epoch], reads.key_digests[shared_epoch],
                "the view subscription folded different rows than a committed read at epoch \
                 {shared_epoch}"
            );
        }
        assert_eq!(
            subscription.progress.get(&epoch),
            Some(&digest(&complete.keys)),
            "the view subscription did not fold every produced row at the final epoch {epoch}"
        );
        assert!(
            reads.reads_by_node.iter().all(|reads| *reads > 0),
            "every node must serve committed view reads: {:?}",
            reads.reads_by_node
        );
        assert!(
            subscription.resumed > 0 && self.spanned > 0,
            "the view subscription must resume at its last progress epoch and stay open through \
             a fault on another node: {} resumed, {} kept open",
            subscription.resumed,
            self.spanned
        );
        assert!(
            reads.http_reads_by_node.iter().all(|reads| *reads > 0),
            "every node must serve HTTP committed view reads: {:?}",
            reads.http_reads_by_node
        );
        assert!(
            shared.len() >= 2 && subscription.gateways.len() == NODES,
            "the view subscription must span every gateway and share epochs with the reader: \
             {} shared epochs, gateways {:?}",
            shared.len(),
            subscription.gateways
        );
        for node in nodes {
            let committed_reads = node.metric("laminardb_mv_reads_total").unwrap_or(0.0);
            assert!(
                committed_reads > 0.0,
                "node{} exported no view reads",
                node.id
            );
        }
        eprintln!(
            "soak: view oracle verified through epoch {epoch}: {} committed reads across {} \
             epochs ({:?} per node, {:?} HTTP reads, retried {:?}); subscription folded {} epochs \
             over {} connections ({} kept open through a fault, {} resumed, {} resumes lost, \
             retried {:?}), {} epochs shared with the reader",
            reads.reads_by_node.iter().sum::<u64>(),
            reads.read_digests.len(),
            reads.reads_by_node,
            reads.http_reads_by_node,
            reads.retried,
            subscription.progress.len(),
            subscription.connections,
            self.spanned,
            subscription.resumed,
            subscription.resume_lost,
            subscription.retried,
            shared.len()
        );
    }
}

fn assert_final_pgwire_reads(nodes: &[Node], epoch: u64, complete: &ViewRows) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("final view read runtime");
    for node in nodes {
        let read = runtime.block_on(async {
            let client = connect(node.id).await?;
            client
                .batch_execute(&format!("SET laminar.min_epoch = {epoch}"))
                .await?;
            read_once(&client).await
        });
        let (read_epoch, rows) = match read {
            Ok(read) => read,
            Err(ReadFailure::Retryable(reason) | ReadFailure::Fatal(reason)) => {
                panic!("node{} final committed view read failed: {reason}", node.id)
            }
        };
        assert!(
            read_epoch >= epoch,
            "node{} read epoch {read_epoch}",
            node.id
        );
        assert!(
            rows == *complete,
            "node{} final committed read differs from every produced row: {}",
            node.id,
            first_difference(&rows, complete)
        );
    }
}

fn assert_final_http_reads(nodes: &[Node], epoch: u64, complete: &ViewRows) {
    let body = serde_json::json!({
        "sql": format!(
            "SELECT COUNT(*) AS key_count, SUM(n) AS row_count, SUM(max_id) AS max_id_sum \
             FROM {KEYED_VIEW}"
        ),
        "read_level": "committed",
        "min_epoch": epoch,
    })
    .to_string();
    let expected = (
        complete.keys.len() as u64,
        complete.keys.values().map(|value| value.0).sum::<u64>(),
        complete.keys.values().map(|value| value.1).sum::<u64>(),
    );
    for node in nodes {
        let response = node
            .http_request("POST", "/api/v1/sql", Some(&body), Duration::from_secs(10))
            .unwrap_or_else(|| panic!("node{} HTTP committed view read failed", node.id));
        let response: serde_json::Value =
            serde_json::from_str(&response).expect("HTTP SQL response is JSON");
        let row = &response["data"][0];
        let read = (
            json_u64(row, "key_count").expect("key_count"),
            json_u64(row, "row_count").expect("row_count"),
            json_u64(row, "max_id_sum").expect("max_id_sum"),
        );
        assert!(
            json_u64(&response, "epoch").expect("HTTP read reports its epoch") >= epoch,
            "node{} HTTP read an epoch below {epoch}",
            node.id
        );
        assert_eq!(read, expected, "node{} HTTP committed view read", node.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_accepts_prefixes_and_rejects_gaps_duplicates_and_wrong_aggregates() {
        let mut oracle = ViewOracle::new(4, 16, 800);
        let prefix = oracle.produced(37);
        assert_eq!(prefix, vec![Some(36), Some(33), Some(34), Some(35)]);
        let complete = oracle.expected(&prefix);
        assert_eq!(complete.progress.values().map(|row| row.0).sum::<u64>(), 37);
        assert_eq!(oracle.check(&complete).unwrap(), prefix);

        let partial = oracle.expected(&[Some(8), None, Some(2), Some(35)]);
        assert_eq!(
            oracle.check(&partial).unwrap(),
            vec![Some(8), None, Some(2), Some(35)]
        );

        let mut gap = oracle.expected(&prefix);
        gap.progress.get_mut(&1).unwrap().0 -= 1;
        assert!(oracle.check(&gap).unwrap_err().contains("contiguous"));

        let mut wrong = oracle.expected(&prefix);
        let first = *wrong.keys.keys().next().unwrap();
        wrong.keys.get_mut(&first).unwrap().0 += 1;
        assert!(oracle.check(&wrong).unwrap_err().contains("soak_view_keys"));

        let mut lost = oracle.expected(&prefix);
        lost.rows.remove(&16);
        assert!(oracle.check(&lost).unwrap_err().contains("soak_view_rows"));
    }

    #[test]
    fn evidence_rejects_a_shrinking_prefix_and_a_changed_epoch() {
        let mut oracle = ViewOracle::new(2, 8, 800);
        let mut evidence = ReadEvidence::default();
        let early = oracle.expected(&oracle.produced(10));
        let late = oracle.expected(&oracle.produced(20));
        evidence.record(0, 5, &early, oracle.produced(10)).unwrap();
        evidence.record(1, 7, &late, oracle.produced(20)).unwrap();
        assert!(evidence
            .record(2, 6, &early, oracle.produced(10))
            .unwrap_err()
            .contains("floor"));
        assert!(evidence
            .record(2, 7, &early, oracle.produced(10))
            .unwrap_err()
            .contains("different rows"));
        assert!(evidence
            .record(2, 8, &early, oracle.produced(10))
            .unwrap_err()
            .contains("less of partition"));
    }

    #[test]
    fn fold_requires_snapshot_first_and_one_row_per_key() {
        let mut fold = Fold::default();
        let row = |key: u64, n: u64, weight: i64| {
            serde_json::json!({"join_key": key.to_string(), "n": n.to_string(),
                "max_id": "9", "__weight": weight.to_string()})
        };
        let data = |snapshot: bool, rows: Vec<serde_json::Value>| {
            serde_json::json!({"type": "data", "snapshot": snapshot,
                "committed_epoch": "3", "data": rows})
        };
        fold.data(&data(true, vec![row(1, 2, 1), row(2, 1, 1)]))
            .unwrap();
        let (epoch, _) = fold.progress(&serde_json::json!({"epoch": "3"})).unwrap();
        assert_eq!(epoch, 3);
        assert!(fold.data(&data(true, vec![row(1, 2, 1)])).is_err());
        let change = serde_json::json!({"type": "data", "snapshot": false,
            "committed_epoch": "4", "data": [row(1, 2, -1), row(1, 3, 1)]});
        fold.data(&change).unwrap();
        let (_, keys) = fold.progress(&serde_json::json!({"epoch": "4"})).unwrap();
        assert_eq!(
            keys,
            digest(&BTreeMap::from([(1_u64, (3_u64, 9_u64)), (2, (1, 9))]))
        );
        let doubled = serde_json::json!({"type": "data", "snapshot": false,
            "committed_epoch": "5", "data": [row(2, 4, 1)]});
        fold.data(&doubled).unwrap();
        assert!(fold.progress(&serde_json::json!({"epoch": "5"})).is_err());
    }
}
