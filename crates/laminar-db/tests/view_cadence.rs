#![allow(clippy::disallowed_types)]
//! Checkpoint cost and committed-read staleness of a view-heavy pipeline at several checkpoint
//! cadences. A measurement, not a check; run it in release mode on the hardware in question:
//!
//! ```text
//! cargo test --release -p laminar-db --test view_cadence -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Int64Array, RecordBatch, TimestampMicrosecondArray};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{CommittedRead, LaminarConfig, LaminarDB};

const RUN: Duration = Duration::from_secs(10);
const ROWS_PER_BATCH: usize = 500;
const BATCH_PERIOD: Duration = Duration::from_millis(10);
const KEYS: u64 = 50_000;
const CADENCES_MS: [u64; 3] = [1_000, 500, 100];

const VIEWS: [&str; 7] = [
    "CREATE MATERIALIZED VIEW by_key AS SELECT k, COUNT(*) AS n, SUM(v) AS total \
     FROM events GROUP BY k",
    "CREATE MATERIALIZED VIEW by_bucket AS SELECT b, COUNT(*) AS n FROM events GROUP BY b",
    "CREATE MATERIALIZED VIEW totals AS SELECT COUNT(*) AS n, SUM(v) AS total FROM events",
    "CREATE MATERIALIZED VIEW tail AS SELECT k, v, b FROM events \
     WITH (retention = '200000 rows')",
    "CREATE MATERIALIZED VIEW large AS SELECT k, v FROM events WHERE v > 500 \
     WITH (retention = '100000 rows')",
    "CREATE MATERIALIZED VIEW per_minute AS SELECT k, tumble(ts, INTERVAL '1' MINUTE) AS w, \
     COUNT(*) AS n, SUM(v) AS total FROM events \
     GROUP BY k, tumble(ts, INTERVAL '1' MINUTE) EMIT ON WINDOW CLOSE",
    "CREATE MATERIALIZED VIEW sessions AS SELECT k, SESSION(ts, INTERVAL '30' SECOND) AS w, \
     COUNT(*) AS n FROM events GROUP BY k, SESSION(ts, INTERVAL '30' SECOND) EMIT ON WINDOW CLOSE",
];

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// A deterministic batch of `ROWS_PER_BATCH` rows spread over `KEYS` keys. Batch `n` is stamped
/// `n` × `BATCH_PERIOD` after the epoch, so a run stays inside the first one-minute window.
fn batch(sequence: u64) -> RecordBatch {
    let rows = 0..ROWS_PER_BATCH as u64;
    let keys: Vec<i64> = rows
        .clone()
        .map(|row| i64::try_from((sequence * 7_919 + row * 104_729) % KEYS).unwrap())
        .collect();
    let values: Vec<i64> = rows
        .map(|row| i64::try_from((sequence + row) % 1_000).unwrap())
        .collect();
    let buckets: Vec<i64> = keys.iter().map(|key| key % 64).collect();
    let event_us =
        i64::try_from(sequence * u64::try_from(BATCH_PERIOD.as_micros()).unwrap()).unwrap();
    RecordBatch::try_from_iter_with_nullable(vec![
        ("k", Arc::new(Int64Array::from(keys)) as _, true),
        ("v", Arc::new(Int64Array::from(values)) as _, true),
        ("b", Arc::new(Int64Array::from(buckets)) as _, true),
        (
            "ts",
            Arc::new(TimestampMicrosecondArray::from(vec![
                event_us;
                ROWS_PER_BATCH
            ])) as _,
            true,
        ),
    ])
    .unwrap()
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() - 1) * percentile / 100]
}

async fn measure(cadence_ms: u64) {
    let dir = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(LaminarConfig {
        storage_dir: Some(dir.path().to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: Some(cadence_ms),
            ..StreamCheckpointConfig::default()
        }),
        ..LaminarConfig::default()
    })
    .unwrap();
    db.execute(
        "CREATE SOURCE events (k BIGINT, v BIGINT, b BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await
    .unwrap();
    for view in VIEWS {
        db.execute(view).await.unwrap();
    }
    db.start().await.unwrap();
    let source = db.source_untyped("events").unwrap();

    let started = Instant::now();
    let mut sequence = 0;
    let mut staleness = Vec::new();
    let mut next_read = Instant::now();
    while started.elapsed() < RUN {
        source.push_arrow(batch(sequence)).unwrap();
        sequence += 1;
        if Instant::now() >= next_read {
            if let Ok(read) = db
                .query_committed("SELECT n FROM totals", CommittedRead::Latest)
                .await
            {
                staleness.push(now_ms().saturating_sub(read.taken_at_ms));
            }
            next_read = Instant::now() + Duration::from_millis(37);
        }
        tokio::time::sleep(BATCH_PERIOD).await;
    }
    let stats = db.checkpoint_stats().await.unwrap();
    staleness.sort_unstable();
    let rows = sequence * ROWS_PER_BATCH as u64;
    println!(
        "| {cadence_ms} ms | {:.0} rows/s | {} ok / {} failed | {} / {} / {} | {:.0} KiB | {} / {} / {} |",
        rows as f64 / started.elapsed().as_secs_f64(),
        stats.completed,
        stats.failed,
        stats.duration_p50_ms,
        stats.duration_p95_ms,
        stats.duration_p99_ms,
        stats.total_bytes_written as f64 / stats.completed.max(1) as f64 / 1024.0,
        percentile(&staleness, 50),
        percentile(&staleness, 99),
        staleness.last().copied().unwrap_or(0),
    );
    db.shutdown().await.unwrap();
    println!(
        "  bytes the last checkpoint wrote: {}",
        last_checkpoint_bytes(dir.path())
    );
}

/// The newest manifest's new bytes by operator, then its view-log object, largest first.
fn last_checkpoint_bytes(dir: &std::path::Path) -> String {
    let mut manifests = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name == "manifest.json") {
                manifests.push(path);
            }
        }
    }
    manifests.sort();
    let Some(newest) = manifests.last() else {
        return "no manifest".into();
    };
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(newest).unwrap()).unwrap();
    let current = &manifest["node_data"]["chunk"];
    let mut by_operator = std::collections::BTreeMap::<String, u64>::new();
    for frame in manifest["state_frames"].as_array().unwrap() {
        if frame["chunk"] != *current {
            continue;
        }
        let key = &frame["key"];
        let operator = key["operator_id"].as_str().unwrap_or("?").to_owned();
        *by_operator.entry(operator).or_default() += frame["range"]["length"].as_u64().unwrap();
    }
    if let Some(length) = manifest["view_log"]["object_length"].as_u64() {
        by_operator.insert("view-log.bin".into(), length);
    }
    let mut sorted: Vec<_> = by_operator.into_iter().collect();
    sorted.sort_by_key(|(_, bytes)| std::cmp::Reverse(*bytes));
    sorted
        .iter()
        .take(8)
        .map(|(operator, bytes)| format!("{operator} {} KiB", bytes / 1024))
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run in release mode"]
async fn checkpoint_cost_and_committed_staleness_by_cadence() {
    println!(
        "| cadence | ingest | checkpoints | duration p50/p95/p99 ms | data per checkpoint | \
         committed staleness p50/p99/max ms |"
    );
    println!("|---|---|---|---|---|---|");
    for cadence_ms in CADENCES_MS {
        measure(cadence_ms).await;
    }
}
