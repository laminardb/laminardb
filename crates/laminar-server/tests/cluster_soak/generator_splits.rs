//! Three-node at-least-once soak for the split generator. The generator has one split per vnode,
//! so every node that owns a vnode reads a channel, and the rows feed a keyed view. The observed
//! leader and each follower are killed inside a checkpoint and rejoin (`kill_rejoin`): survivors
//! take the killed node's channels from the committed cut, and the rejoined node takes channels
//! back from outside the predecessor participants.
//!
//! The oracle reads the view at the committed level, rotating over the nodes through every round.
//! The view groups `seq % (2 * splits)`, so split `s` alternates between lanes `s` and
//! `s + splits`. A cut holds a prefix of each split's sequence `s, s + splits, …`, so a lane's
//! count fixes its sum and a split's two lanes differ by at most one row: a lost or doubled row
//! breaks the read. The same epoch must read the same prefixes on every node, a later epoch may not
//! hold fewer rows of any split, and every split must advance after the last rejoin.
//!
//! The exactly-once leg runs the same rounds with `delivery = exactly_once` over a bounded
//! generator whose rows also go to a Delta table on S3 through a stream. After every round the
//! table may hold no `seq` twice, and once the sequence ends it must hold each of its rows exactly
//! once, as must the view.
//!
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka --test cluster_soak \
//!   generator_splits::three_node_generator_splits_kill9_soak -- --ignored --exact --nocapture
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka,delta-lake-s3 --test cluster_soak \
//!   generator_splits::three_node_generator_splits_delta_eo_kill9_soak -- --ignored --exact \
//!   --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the kills, default 30) and `LAMINAR_SOAK_RPS` (the generator's
//! `rows.per.second`, default 400). The exactly-once leg also reads `LAMINAR_SOAK_DELTA_BUCKET`
//! and `LAMINAR_SOAK_GENERATOR_SECONDS` (default 480): the sequence holds that many seconds of
//! rows, so every kill round must end before it does.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::kill_rejoin::{boot, cluster_nodes, kill_round, steady_phase, SoakSettings};
use super::view_reads::{http_post, RETRYABLE_HTTP_ERRORS};
use super::{
    assert_running_nodes, scoped_soak_storage_url, soak_run_id, wait_for,
    wait_for_converged_durable_checkpoint, JoinDelivery, Node, ResolvedExecutable, NODES,
};

const VIEW: &str = "generator_lanes";
const INGESTED: &str = "laminardb_events_ingested_total";
const READ_INTERVAL: Duration = Duration::from_millis(250);

#[test]
#[ignore = "spawns 3 real laminardb processes with S3"]
fn three_node_generator_splits_kill9_soak() {
    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::AtLeastOnce);
    let splits = u64::from(settings.key_groups);
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!("soak: split generator node logs in {}", log_dir.display());
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        &view(splits),
        &source(splits, settings.rps, None),
    );
    let mut reader = LaneReader::spawn(splits);

    let mut latest = boot(
        &mut nodes,
        &mut || reader.assert_running(),
        &settings,
        INGESTED,
    );
    let mut killed_followers = BTreeSet::new();
    let mut rejoined = Vec::new();
    for round in 0..settings.kills {
        let (victim, committed) = kill_round(
            &mut nodes,
            &mut || reader.assert_running(),
            round,
            &mut killed_followers,
            &settings,
            INGESTED,
            latest,
        );
        latest = committed;
        rejoined.push(victim);
    }
    // A read at or after the checkpoint committed once the last node rejoined.
    let after_kills = reader.newest_after(&mut nodes, latest.epoch - 1, settings.recovery_ceiling);
    steady_phase(
        &mut nodes,
        &mut || reader.assert_running(),
        settings.steady,
        INGESTED,
    );
    wait_for_converged_durable_checkpoint(
        &mut nodes,
        Instant::now() + settings.recovery_ceiling,
        "steady durable progress",
        Some(latest),
    );
    let last = reader.newest_after(&mut nodes, after_kills.0, settings.recovery_ceiling);
    let evidence = reader.finish();

    let stalled: Vec<u64> = (0..splits)
        .filter(|split| last.1[*split as usize] <= after_kills.1[*split as usize])
        .collect();
    assert!(
        stalled.is_empty(),
        "splits {stalled:?} did not advance after the last rejoin: epoch {} read {:?}, epoch {} \
         read {:?}",
        after_kills.0,
        after_kills.1,
        last.0,
        last.1
    );
    assert!(
        evidence.reads_by_node.iter().all(|reads| *reads > 0),
        "every node must serve a committed read: {:?}",
        evidence.reads_by_node
    );
    eprintln!(
        "soak: split generator across {NODES} nodes: {splits} splits, {} rows at epoch {}, \
         {} checked epochs, reads per node {:?}, retried {:?}, kills={} (rejoined {rejoined:?})",
        last.1.iter().sum::<u64>(),
        last.0,
        evidence.prefixes.len(),
        evidence.reads_by_node,
        evidence.retried,
        settings.kills
    );
}

#[cfg(feature = "delta-lake-s3")]
#[test]
#[ignore = "spawns 3 real laminardb processes with S3 and a Delta sink"]
fn three_node_generator_splits_delta_eo_kill9_soak() {
    use super::{
        delta_append_sink_config, delta_soak_table_uri, env_u64, wait_until, DeltaSoakStorage,
    };

    let executable = Arc::new(
        ResolvedExecutable::from_environment()
            .unwrap_or_else(|error| panic!("invalid soak server executable: {error}")),
    );
    executable.describe();
    let settings = SoakSettings::from_environment(JoinDelivery::ExactlyOnce);
    let splits = u64::from(settings.key_groups);
    let rows = settings.rps * env_u64("LAMINAR_SOAK_GENERATOR_SECONDS", 480);
    let run_id = soak_run_id();
    let checkpoint_url =
        scoped_soak_storage_url("LAMINAR_SOAK_CHECKPOINT_URL", &run_id, "checkpoints");
    let storage = DeltaSoakStorage::from_environment();
    let table_uri = delta_soak_table_uri(&run_id, "generator");
    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir: PathBuf = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("soak-{run_id}"));
    std::fs::create_dir(&log_dir).expect("create exclusive soak log directory");
    eprintln!(
        "soak: exactly-once split generator node logs in {}",
        log_dir.display()
    );
    // A cluster sink may not read a source directly.
    let workload = format!(
        r#"{}
[[pipeline]]
name = "generator_forwarded"
sql = "SELECT seq, ts_ms FROM gen"
{}"#,
        source(splits, settings.rps, Some(rows)),
        delta_append_sink_config(
            "generator_delta",
            "generator_forwarded",
            &table_uri,
            &storage
        )
    );
    let mut nodes = cluster_nodes(
        &executable,
        dir.path(),
        &log_dir,
        &settings,
        &checkpoint_url,
        &view(splits),
        &workload,
    );
    let delta = delta::Rows::new(table_uri, &storage);
    let mut reader = LaneReader::spawn(splits);

    let mut latest = boot(
        &mut nodes,
        &mut || reader.assert_running(),
        &settings,
        INGESTED,
    );
    let mut killed_followers = BTreeSet::new();
    for round in 0..settings.kills {
        let (victim, committed) = kill_round(
            &mut nodes,
            &mut || reader.assert_running(),
            round,
            &mut killed_followers,
            &settings,
            INGESTED,
            latest,
        );
        latest = committed;
        let seen = delta
            .counts(rows)
            .unwrap_or_else(|error| panic!("read the Delta output after round {round}: {error}"));
        seen.assert_no_duplicates();
        eprintln!(
            "soak round {round}: node {victim} rejoined; Delta holds {} of {rows} rows once",
            seen.distinct
        );
    }

    let deadline = settings.recovery_ceiling + Duration::from_secs(rows / settings.rps + 60);
    let mut seen = None;
    let complete = wait_until(deadline, || {
        assert_running_nodes(&mut nodes);
        reader.assert_running();
        seen = delta.counts(rows).ok();
        seen.as_ref().is_some_and(|seen| {
            seen.assert_no_duplicates();
            seen.distinct == rows
        })
    });
    let seen = seen.expect("the Delta output was never read");
    assert!(
        complete,
        "Delta holds {} of the {rows} generated rows once, first missing {:?}",
        seen.distinct, seen.first_missing
    );
    // The prefix oracle already bounds every read, so a read holding every row holds each once.
    reader.newest_where(
        &mut nodes,
        &format!("a committed view read holding all {rows} rows"),
        settings.recovery_ceiling,
        |_, read| read.iter().sum::<u64>() == rows,
    );
    let evidence = reader.finish();
    assert!(
        evidence.reads_by_node.iter().all(|reads| *reads > 0),
        "every node must serve a committed read: {:?}",
        evidence.reads_by_node
    );
    eprintln!(
        "soak: exactly-once split generator across {NODES} nodes: {splits} splits, {rows} rows in \
         Delta and the view exactly once, {} checked epochs, kills={}",
        evidence.prefixes.len(),
        settings.kills
    );
}

#[cfg(feature = "delta-lake-s3")]
mod delta {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Array as _, Int64Array};
    use arrow_schema::DataType;

    use super::super::DeltaSoakStorage;

    /// Reads the generator rows committed to the Delta output.
    pub(super) struct Rows {
        table_uri: String,
        options: HashMap<String, String>,
        runtime: tokio::runtime::Runtime,
    }

    /// How often the table holds each `seq` of a sequence of `rows` rows.
    pub(super) struct Seen {
        pub distinct: u64,
        pub first_missing: Option<u64>,
        duplicates: Vec<u64>,
        outside: Vec<i64>,
    }

    impl Seen {
        pub fn assert_no_duplicates(&self) {
            assert!(
                self.duplicates.is_empty() && self.outside.is_empty(),
                "Delta holds rows twice {:?} or outside the sequence {:?}",
                &self.duplicates[..self.duplicates.len().min(8)],
                &self.outside[..self.outside.len().min(8)]
            );
        }
    }

    impl Rows {
        pub fn new(table_uri: String, storage: &DeltaSoakStorage) -> Self {
            Self {
                table_uri,
                options: storage.options(),
                runtime: tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Delta reader runtime"),
            }
        }

        pub fn counts(&self, rows: u64) -> Result<Seen, String> {
            let batches = self.runtime.block_on(async {
                let uri = deltalake::ensure_table_uri(&self.table_uri)
                    .map_err(|error| format!("invalid Delta table URI: {error}"))?;
                let table = deltalake::open_table_with_storage_options(uri, self.options.clone())
                    .await
                    .map_err(|error| format!("open Delta output: {error}"))?;
                let context = deltalake::datafusion::prelude::SessionContext::new();
                table
                    .update_datafusion_session(&context.state())
                    .map_err(|error| format!("register Delta object store: {error}"))?;
                let provider = table
                    .table_provider()
                    .build()
                    .await
                    .map_err(|error| format!("build Delta table provider: {error}"))?;
                context
                    .register_table("generator_delta", Arc::new(provider))
                    .map_err(|error| format!("register Delta output: {error}"))?;
                context
                    .sql("SELECT seq FROM generator_delta")
                    .await
                    .map_err(|error| format!("plan Delta scan: {error}"))?
                    .collect()
                    .await
                    .map_err(|error| format!("scan Delta output: {error}"))
            })?;
            let mut seqs = Vec::new();
            for batch in batches {
                let column = arrow_cast::cast(batch.column(0), &DataType::Int64)
                    .map_err(|error| format!("cast Delta seq: {error}"))?;
                let column = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("Delta seq did not cast to Int64")?;
                seqs.extend((0..column.len()).map(|row| column.value(row)));
            }
            Seen::count(rows, seqs)
        }
    }

    impl Seen {
        pub fn count(rows: u64, seqs: impl IntoIterator<Item = i64>) -> Result<Self, String> {
            let size = usize::try_from(rows).map_err(|_| "row count exceeds usize")?;
            let mut counts = vec![0_u32; size];
            let mut outside = Vec::new();
            for seq in seqs {
                match usize::try_from(seq).ok().filter(|seq| *seq < size) {
                    Some(seq) => counts[seq] += 1,
                    None => outside.push(seq),
                }
            }
            Ok(Self {
                distinct: counts.iter().filter(|count| **count > 0).count() as u64,
                first_missing: counts
                    .iter()
                    .position(|count| *count == 0)
                    .map(|seq| seq as u64),
                duplicates: (0_u64..)
                    .zip(&counts)
                    .filter(|(_, count)| **count > 1)
                    .map(|(seq, _)| seq)
                    .collect(),
                outside,
            })
        }
    }

    #[test]
    fn delta_rows_are_counted_once_each() {
        let once = Seen::count(4, [2, 0, 3, 1]).unwrap();
        assert_eq!((once.distinct, once.first_missing), (4, None));
        once.assert_no_duplicates();
        let partial = Seen::count(4, [0, 2]).unwrap();
        assert_eq!((partial.distinct, partial.first_missing), (2, Some(1)));
        let doubled = Seen::count(4, [0, 1, 1]).unwrap();
        assert!(std::panic::catch_unwind(|| doubled.assert_no_duplicates()).is_err());
        let outside = Seen::count(4, [0, 4]).unwrap();
        assert!(std::panic::catch_unwind(|| outside.assert_no_duplicates()).is_err());
    }
}

/// Top-level server configuration declaring the view; it must precede every TOML table.
fn view(splits: u64) -> String {
    let lanes = 2 * splits;
    format!(
        r#"sql = """
CREATE MATERIALIZED VIEW {VIEW} AS
SELECT seq % {lanes} AS lane, COUNT(*) AS n, SUM(seq) AS total FROM gen GROUP BY seq % {lanes};
"""
"#
    )
}

fn source(splits: u64, rps: u64, max_rows: Option<u64>) -> String {
    let max_rows = max_rows.map_or_else(String::new, |rows| format!("\"max.rows\" = \"{rows}\"\n"));
    format!(
        r#"
[[source]]
name = "gen"
connector = "generator"
[source.properties]
splits = "{splits}"
"rows.per.second" = "{rps}"
"batch.max.size" = "64"
{max_rows}"#
    )
}

/// The rows each split has read at a cut, from the view's lanes, or why they are not a prefix.
fn prefixes(splits: u64, lanes: &BTreeMap<u64, (u64, u64)>) -> Result<Vec<u64>, String> {
    if let Some(lane) = lanes.keys().find(|lane| **lane >= 2 * splits) {
        return Err(format!("lane {lane} is outside the {} lanes", 2 * splits));
    }
    // Lane `l` holds `l, l + 2 * splits, …`: `n` rows sum to `n * l + splits * n * (n - 1)`.
    let lane = |lane: u64| {
        let (n, total) = lanes.get(&lane).copied().unwrap_or_default();
        let expected = n * lane + splits * n * n.saturating_sub(1);
        if total == expected {
            Ok(n)
        } else {
            Err(format!(
                "lane {lane} holds {n} rows summing to {total}, not a prefix (sum {expected})"
            ))
        }
    };
    (0..splits)
        .map(|split| {
            let (even, odd) = (lane(split)?, lane(split + splits)?);
            if even == odd || even == odd + 1 {
                Ok(even + odd)
            } else {
                Err(format!(
                    "split {split} read {even} rows into lane {split} and {odd} into lane {}",
                    split + splits
                ))
            }
        })
        .collect()
}

#[derive(Default)]
struct Evidence {
    reads_by_node: [u64; NODES],
    retried: BTreeMap<String, u64>,
    /// Each checked epoch's per-split prefix lengths.
    prefixes: BTreeMap<u64, Vec<u64>>,
}

impl Evidence {
    /// Keep one read's prefixes: the same epoch must hold the same prefixes on every node, and a
    /// later epoch may not hold fewer rows of any split.
    fn record(&mut self, node: usize, epoch: u64, read: Vec<u64>) -> Result<(), String> {
        if let Some(seen) = self.prefixes.get(&epoch) {
            if *seen != read {
                return Err(format!(
                    "node{node} read {read:?} at epoch {epoch}, another node read {seen:?}"
                ));
            }
        }
        let shrinks = |(older, newer): (&Vec<u64>, &Vec<u64>)| {
            older.iter().zip(newer).any(|(older, newer)| older > newer)
        };
        if let Some((before, older)) = self.prefixes.range(..epoch).next_back() {
            if shrinks((older, &read)) {
                return Err(format!(
                    "node{node} read {read:?} at epoch {epoch}, fewer rows than {older:?} at \
                     epoch {before}"
                ));
            }
        }
        if let Some((after, newer)) = self.prefixes.range(epoch + 1..).next() {
            if shrinks((&read, newer)) {
                return Err(format!(
                    "node{node} read {read:?} at epoch {epoch}, more rows than {newer:?} at \
                     epoch {after}"
                ));
            }
        }
        self.reads_by_node[node] += 1;
        self.prefixes.insert(epoch, read);
        Ok(())
    }

    fn newest(&self) -> Option<(u64, Vec<u64>)> {
        self.prefixes
            .last_key_value()
            .map(|(epoch, read)| (*epoch, read.clone()))
    }
}

/// Reads the view at the committed level, rotating over the nodes, until stopped. A read that
/// fails the oracle panics the thread, which `assert_running` raises on the soak thread.
struct LaneReader {
    evidence: Arc<Mutex<Evidence>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl LaneReader {
    fn spawn(splits: u64) -> Self {
        let evidence = Arc::new(Mutex::new(Evidence::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let (evidence, stop) = (Arc::clone(&evidence), Arc::clone(&stop));
            std::thread::spawn(move || {
                for read in 0_usize.. {
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    let node = read % NODES;
                    let outcome = read_lanes(node, splits).and_then(|(epoch, prefixes)| {
                        lock(&evidence)
                            .record(node, epoch, prefixes)
                            .map_err(Failure::Fatal)
                    });
                    match outcome {
                        Ok(()) => {}
                        Err(Failure::Retryable(reason)) => {
                            *lock(&evidence).retried.entry(reason).or_default() += 1;
                        }
                        Err(Failure::Fatal(error)) => panic!("committed view read: {error}"),
                    }
                    std::thread::sleep(READ_INTERVAL);
                }
            })
        };
        Self {
            evidence,
            stop,
            handle: Some(handle),
        }
    }

    fn assert_running(&mut self) {
        if !self.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return;
        }
        match self.handle.take().expect("reader handle").join() {
            Ok(()) => panic!("view reader stopped before the soak completed"),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// The newest checked epoch once it is above `epoch`.
    fn newest_after(
        &mut self,
        nodes: &mut [Node],
        epoch: u64,
        within: Duration,
    ) -> (u64, Vec<u64>) {
        self.newest_where(
            nodes,
            &format!("a committed read above epoch {epoch}"),
            within,
            |read, _| read > epoch,
        )
    }

    /// The newest checked epoch once it satisfies `accept`.
    fn newest_where(
        &mut self,
        nodes: &mut [Node],
        what: &str,
        within: Duration,
        accept: impl Fn(u64, &[u64]) -> bool,
    ) -> (u64, Vec<u64>) {
        let mut newest = None;
        wait_for(what, within, || {
            assert_running_nodes(nodes);
            self.assert_running();
            newest = lock(&self.evidence)
                .newest()
                .filter(|(epoch, read)| accept(*epoch, read));
            newest.is_some()
        });
        newest.expect("committed read wait completed without a read")
    }

    fn finish(mut self) -> Evidence {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            if let Err(payload) = handle.join() {
                std::panic::resume_unwind(payload);
            }
        }
        std::mem::take(&mut *lock(&self.evidence))
    }
}

impl Drop for LaneReader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn lock(evidence: &Mutex<Evidence>) -> std::sync::MutexGuard<'_, Evidence> {
    evidence.lock().expect("reader evidence lock")
}

enum Failure {
    /// The node is down, paused, recovering, or has no committed cut yet.
    Retryable(String),
    Fatal(String),
}

/// One committed read of the view on `node`: the epoch it read and each split's prefix.
fn read_lanes(node: usize, splits: u64) -> Result<(u64, Vec<u64>), Failure> {
    let body = serde_json::json!({
        "sql": format!("SELECT lane, n, total FROM {VIEW}"),
        "read_level": "committed",
    })
    .to_string();
    let (status, response) = http_post(node, "/api/v1/sql", &body)
        .map_err(|_| Failure::Retryable("http connection".to_owned()))?;
    match status {
        200 => {}
        503 | 504 => return Err(Failure::Retryable(format!("http {status}"))),
        400 if RETRYABLE_HTTP_ERRORS
            .iter()
            .any(|error| response.contains(error)) =>
        {
            return Err(Failure::Retryable("http catching up".to_owned()))
        }
        _ => {
            return Err(Failure::Fatal(format!(
                "node{node} failed with {status}: {response}"
            )))
        }
    }
    let fatal = |error: String| Failure::Fatal(format!("node{node}: {error}"));
    let response: serde_json::Value =
        serde_json::from_str(&response).map_err(|error| fatal(error.to_string()))?;
    let epoch = field(&response, "epoch").map_err(fatal)?;
    let mut lanes = BTreeMap::new();
    for row in response["data"].as_array().into_iter().flatten() {
        let lane = field(row, "lane").map_err(fatal)?;
        let counted = (
            field(row, "n").map_err(fatal)?,
            field(row, "total").map_err(fatal)?,
        );
        if lanes.insert(lane, counted).is_some() {
            return Err(fatal(format!("epoch {epoch} read lane {lane} twice")));
        }
    }
    let read =
        prefixes(splits, &lanes).map_err(|error| fatal(format!("epoch {epoch}: {error}")))?;
    Ok((epoch, read))
}

fn field(value: &serde_json::Value, name: &str) -> Result<u64, String> {
    let value = value
        .get(name)
        .ok_or_else(|| format!("the response omitted {name}"))?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| format!("{name} is not an unsigned integer: {value}"))
}

#[test]
fn a_read_is_a_prefix_of_every_split_only_without_lost_or_doubled_rows() {
    let splits = 2;
    // Split 0 read 0, 2, 4 (lanes 0 and 2); split 1 read 1, 3 (lanes 1 and 3).
    let read = |rows: &[u64]| {
        let mut lanes = BTreeMap::<u64, (u64, u64)>::new();
        for row in rows {
            let (n, total) = lanes.entry(row % (2 * splits)).or_default();
            *n += 1;
            *total += row;
        }
        prefixes(splits, &lanes)
    };
    assert_eq!(read(&[0, 2, 4, 1, 3]), Ok(vec![3, 2]));
    assert_eq!(read(&[]), Ok(vec![0, 0]));
    assert!(read(&[0, 2, 4, 4, 1, 3]).is_err(), "a doubled row");
    assert!(read(&[0, 4, 1, 3]).is_err(), "a lost row");
    assert!(read(&[0, 4, 8, 2, 1]).is_err(), "lanes out of step");
}
