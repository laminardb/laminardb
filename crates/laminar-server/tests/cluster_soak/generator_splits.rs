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
//! ```text
//! cargo test --profile soak -p laminar-server --no-default-features \
//!   --features cluster,aws,kafka --test cluster_soak \
//!   generator_splits::three_node_generator_splits_kill9_soak -- --ignored --exact --nocapture
//! ```
//!
//! It reads the shared `LAMINAR_SOAK_CHECKPOINT_URL`, `LAMINAR_SOAK_S3_*`,
//! `LAMINAR_SOAK_KEY_GROUPS`, `LAMINAR_SOAK_INTERVAL_MS`, `LAMINAR_SOAK_MAX_RECOVERY_MS` and
//! `LAMINAR_SOAK_DISCOVERY` knobs, plus `LAMINAR_SOAK_KILLS` (default 4), `LAMINAR_SOAK_SECONDS`
//! (steady phase after the kills, default 30) and `LAMINAR_SOAK_RPS` (the generator's
//! `rows.per.second`, default 400).

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
    wait_for_converged_durable_checkpoint, Node, ResolvedExecutable, NODES,
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
    let settings = SoakSettings::from_environment();
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
        &source(splits, settings.rps),
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

fn source(splits: u64, rps: u64) -> String {
    format!(
        r#"
[[source]]
name = "gen"
connector = "generator"
[source.properties]
splits = "{splits}"
"rows.per.second" = "{rps}"
"batch.max.size" = "64"
"#
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
        let mut newest = None;
        wait_for(
            &format!("a committed read above epoch {epoch}"),
            within,
            || {
                assert_running_nodes(nodes);
                self.assert_running();
                newest = lock(&self.evidence)
                    .newest()
                    .filter(|(read, _)| *read > epoch);
                newest.is_some()
            },
        );
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
