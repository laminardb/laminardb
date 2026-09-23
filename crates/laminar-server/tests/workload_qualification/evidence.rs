use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{ensure, Context as _, Result};
use serde::Serialize;

use super::super::{prometheus_histogram_latency, Node};
use super::load::Counts;
use super::spec::Spec;

#[derive(Serialize)]
pub(super) struct Sample {
    pub seconds: f64,
    pub generation: u64,
    pub offered: u64,
    pub enqueued: u64,
    pub acknowledged: u64,
    pub observed: u64,
    pub backlog: u64,
    pub rss_bytes: Option<f64>,
    pub cycle_p50_p95_p99_ms: Option<[f64; 3]>,
    pub checkpoint_p99_ms: Option<f64>,
    pub checkpoint_stall_p99_ms: Option<f64>,
}

pub(super) struct Evidence {
    raw: BufWriter<File>,
    pub samples: Vec<Sample>,
}

#[derive(Serialize)]
pub(super) struct Summary {
    pub samples: usize,
    pub peak_rss_bytes: Option<f64>,
    pub rss_growth_bytes_per_second: Option<f64>,
    pub backlog_growth_rows_per_second: Option<f64>,
    pub max_backlog_rows: u64,
    pub checkpoint_p99_ms: Option<f64>,
    pub generations: Vec<GenerationResources>,
}

#[derive(Serialize)]
pub(super) struct GenerationResources {
    pub generation: u64,
    pub samples: usize,
    pub first_load_sample_seconds: Option<f64>,
    pub last_load_sample_seconds: Option<f64>,
    pub rss_growth_start_seconds: Option<f64>,
    pub rss_growth_samples: usize,
    pub missing_rss_samples_after_warmup: usize,
    pub peak_rss_bytes: Option<f64>,
    pub rss_growth_bytes_per_second: Option<f64>,
    pub checkpoint_p99_ms: Option<f64>,
}

impl Evidence {
    pub fn new(directory: &Path) -> Result<Self> {
        Ok(Self {
            raw: BufWriter::new(File::create(directory.join("metrics.jsonl"))?),
            samples: Vec::new(),
        })
    }

    pub fn event(&mut self, elapsed: Duration, event: serde_json::Value) -> Result<()> {
        writeln!(
            self.raw,
            "{}",
            serde_json::json!({
                "seconds": elapsed.as_secs_f64(), "elapsed_ns": elapsed.as_nanos(), "event": event
            })
        )?;
        self.raw.flush()?;
        Ok(())
    }

    pub fn sample(
        &mut self,
        spec: &Spec,
        node: &Node,
        counts: &Counts,
        elapsed: Duration,
    ) -> Result<()> {
        let offered = spec.offered(elapsed);
        let observed = counts.observed.load(Ordering::Acquire);
        let enqueued = counts.enqueued.load(Ordering::Acquire);
        let acknowledged = counts.acknowledged.load(Ordering::Acquire);
        let metrics = node.http_get("/metrics");
        let histogram = |name: &str| {
            metrics
                .as_ref()
                .and_then(|body| prometheus_histogram_latency(body, name).ok())
        };
        let rss_bytes = metrics.as_ref().and_then(|body| {
            body.lines().find_map(|line| {
                let rest = line.strip_prefix("laminardb_process_resident_memory_bytes")?;
                if !rest.starts_with(['{', ' ']) {
                    return None;
                }
                rest.split_whitespace()
                    .last()?
                    .parse::<f64>()
                    .ok()
                    .filter(|v| v.is_finite() && *v > 0.0)
            })
        });
        let sample = Sample {
            seconds: elapsed.as_secs_f64(),
            generation: node.process_generation,
            offered,
            enqueued,
            acknowledged,
            observed,
            backlog: offered.saturating_sub(observed),
            rss_bytes,
            cycle_p50_p95_p99_ms: histogram("laminardb_cycle_duration_seconds_bucket").map(|h| {
                [
                    h.p50_upper_seconds * 1_000.0,
                    h.p95_upper_seconds * 1_000.0,
                    h.p99_upper_seconds * 1_000.0,
                ]
            }),
            checkpoint_p99_ms: histogram("laminardb_checkpoint_duration_seconds_bucket")
                .filter(|h| spec.limits.is_none() || h.observations >= 100)
                .map(|h| h.p99_upper_seconds * 1_000.0),
            checkpoint_stall_p99_ms: histogram(
                "laminardb_checkpoint_pipeline_stall_duration_seconds_bucket",
            )
            .map(|h| h.p99_upper_seconds * 1_000.0),
        };
        writeln!(
            self.raw,
            "{}",
            serde_json::json!({"sample":sample,"prometheus":metrics})
        )?;
        self.raw.flush()?;
        self.samples.push(sample);
        Ok(())
    }

    pub fn summary(&self, spec: &Spec) -> Summary {
        // Exclude warmup and final drain. The second half tests sustained growth after settling.
        let steady: Vec<_> = self
            .samples
            .iter()
            .filter(|s| s.seconds >= (spec.seconds / 2) as f64 && s.seconds < spec.seconds as f64)
            .collect();
        let backlog: Vec<_> = steady
            .iter()
            .map(|s| (s.seconds, s.backlog as f64))
            .collect();
        let peak_rss_bytes = self
            .samples
            .iter()
            .filter_map(|s| s.rss_bytes)
            .reduce(f64::max);
        // A restart must not hide a leaking or slow predecessor behind a fresh process's counters.
        let mut generations = std::collections::BTreeMap::new();
        for sample in &self.samples {
            generations
                .entry(sample.generation)
                .or_insert_with(Vec::new)
                .push(sample);
        }
        let generations: Vec<_> = generations
            .into_iter()
            .map(|(generation, samples)| summarize_generation(generation, &samples, spec))
            .collect();
        let checkpoints: Option<Vec<_>> = generations.iter().map(|g| g.checkpoint_p99_ms).collect();
        let growth: Option<Vec<_>> = generations
            .iter()
            .map(|g| g.rss_growth_bytes_per_second)
            .collect();
        Summary {
            samples: self.samples.len(),
            peak_rss_bytes,
            rss_growth_bytes_per_second: growth
                .and_then(|values| values.into_iter().reduce(f64::max)),
            backlog_growth_rows_per_second: slope(&backlog),
            max_backlog_rows: self.samples.iter().map(|s| s.backlog).max().unwrap_or(0),
            checkpoint_p99_ms: checkpoints.and_then(|values| values.into_iter().reduce(f64::max)),
            generations,
        }
    }
}

fn summarize_generation(generation: u64, samples: &[&Sample], spec: &Spec) -> GenerationResources {
    let load: Vec<_> = samples
        .iter()
        .copied()
        .filter(|s| s.seconds < spec.seconds as f64)
        .collect();
    let first = load.first().map(|s| s.seconds);
    let last = load.last().map(|s| s.seconds);
    let warmup_end = first.map(|start| start + spec.warmup_seconds as f64);
    // Fit each process separately, over the latter half of its sampled post-warmup load interval.
    let growth_start = warmup_end
        .zip(last)
        .filter(|(start, end)| end >= start)
        .map(|(start, end)| f64::midpoint(start, end));
    let missing = load
        .iter()
        .filter(|s| warmup_end.is_some_and(|end| s.seconds >= end) && s.rss_bytes.is_none())
        .count();
    let points: Vec<_> = load
        .iter()
        .filter(|s| growth_start.is_some_and(|start| s.seconds >= start))
        .filter_map(|s| s.rss_bytes.map(|rss| (s.seconds, rss)))
        .collect();
    let sufficient = spec.limits.is_none()
        || (points.len() >= 60
            && points
                .first()
                .zip(points.last())
                .is_some_and(|(first, last)| last.0 - first.0 >= 60.0));
    GenerationResources {
        generation,
        samples: samples.len(),
        first_load_sample_seconds: first,
        last_load_sample_seconds: last,
        rss_growth_start_seconds: growth_start,
        rss_growth_samples: points.len(),
        missing_rss_samples_after_warmup: missing,
        peak_rss_bytes: samples.iter().filter_map(|s| s.rss_bytes).reduce(f64::max),
        rss_growth_bytes_per_second: (missing == 0 && sufficient)
            .then(|| slope(&points))
            .flatten(),
        checkpoint_p99_ms: samples.last().and_then(|s| s.checkpoint_p99_ms),
    }
}

pub(super) fn slope(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 3 {
        return None;
    }
    let n = points.len() as f64;
    let x = points.iter().map(|p| p.0).sum::<f64>() / n;
    let y = points.iter().map(|p| p.1).sum::<f64>() / n;
    let denominator = points.iter().map(|p| (p.0 - x).powi(2)).sum::<f64>();
    (denominator > 0.0)
        .then(|| points.iter().map(|p| (p.0 - x) * (p.1 - y)).sum::<f64>() / denominator)
}

impl Summary {
    pub fn check(&self, spec: &Spec, recovery_ms: Option<f64>) -> Result<()> {
        let Some(limits) = &spec.limits else {
            return Ok(());
        };
        ensure!(
            self.peak_rss_bytes.context("RSS unavailable")? <= limits.rss_bytes as f64,
            "RSS ceiling exceeded"
        );
        ensure!(
            self.rss_growth_bytes_per_second
                .context("RSS growth unavailable")?
                <= limits.rss_growth_bytes_per_second,
            "RSS did not plateau within the declared growth ceiling"
        );
        ensure!(
            self.backlog_growth_rows_per_second
                .context("backlog growth unavailable")?
                <= limits.backlog_growth_rows_per_second,
            "backlog did not stabilize within the declared growth ceiling"
        );
        ensure!(
            self.checkpoint_p99_ms
                .context("checkpoint latency unavailable")?
                <= limits.checkpoint_p99_ms,
            "checkpoint p99 ceiling exceeded"
        );
        if spec.fault == super::spec::Fault::ProcessKill {
            ensure!(
                recovery_ms.context("no externally observed recovery")? <= limits.recovery_ms,
                "recovery ceiling exceeded"
            );
        }
        Ok(())
    }
}
