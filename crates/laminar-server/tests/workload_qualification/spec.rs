use std::time::Duration;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Spec {
    pub hardware: String,
    pub seconds: u64,
    pub warmup_seconds: u64,
    pub drain_seconds: u64,
    pub pipelines: usize,
    pub rps_per_pipeline: u64,
    pub partitions: i32,
    pub payload_bytes: usize,
    pub keys: u64,
    pub seed: u64,
    pub distribution: Distribution,
    pub checkpoint_ms: u64,
    pub fault: Fault,
    pub limits: Option<Limits>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Distribution {
    Uniform,
    Zipf,
    HotKey,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Fault {
    None,
    ProcessKill,
    SourcePause,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Limits {
    pub visibility_ms: [f64; 4],
    pub rss_bytes: u64,
    pub rss_growth_bytes_per_second: f64,
    pub backlog_growth_rows_per_second: f64,
    pub checkpoint_p99_ms: f64,
    pub recovery_ms: f64,
}

impl Spec {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.hardware.trim().is_empty(), "name the test hardware");
        ensure!(
            (10..=86_400).contains(&self.seconds),
            "seconds must be 10..86400"
        );
        ensure!(
            self.fault != Fault::SourcePause || self.seconds >= 20,
            "source pause needs at least 20 seconds"
        );
        ensure!(
            self.warmup_seconds < self.seconds / 2,
            "warmup must be less than half the run"
        );
        ensure!(
            (10..=600).contains(&self.drain_seconds),
            "drain_seconds must be 10..600"
        );
        ensure!(matches!(self.pipelines, 1 | 4), "pipelines must be 1 or 4");
        ensure!(
            (1..=1_000_000).contains(&self.rps_per_pipeline),
            "invalid offered rate"
        );
        ensure!(
            (1..=96).contains(&self.partitions),
            "partitions must be 1..96"
        );
        ensure!(
            (1..=16_384).contains(&self.payload_bytes),
            "payload_bytes must be 1..16384"
        );
        ensure!(
            (1..=1_000_000).contains(&self.keys),
            "keys must be 1..1000000"
        );
        ensure!(
            (100..=60_000).contains(&self.checkpoint_ms),
            "checkpoint_ms must be 100..60000"
        );
        ensure!(
            self.total_rows() <= 100_000_000,
            "oracle is bounded to 100 million rows"
        );
        if let Some(limits) = &self.limits {
            let pause_seconds = if self.fault == Fault::SourcePause {
                5
            } else {
                0
            };
            ensure!(
                self.seconds - self.warmup_seconds - pause_seconds >= 3_600,
                "declared-limit runs require at least one hour of offered load after warmup"
            );
            ensure!(
                self.warmup_seconds >= 60,
                "declared-limit runs require at least 60s warmup"
            );
            ensure!(
                self.measured_rows_per_pipeline() >= 100_000,
                "p99.9 needs at least 100000 post-warmup samples per pipeline"
            );
            ensure!(limits.rss_bytes > 0, "RSS ceiling must be positive");
            ensure!(
                limits
                    .visibility_ms
                    .iter()
                    .all(|v| v.is_finite() && *v > 0.0)
                    && limits.visibility_ms.windows(2).all(|p| p[0] <= p[1]),
                "visibility limits must be finite, positive and ordered p50/p95/p99/p99.9"
            );
            for value in [
                limits.rss_growth_bytes_per_second,
                limits.backlog_growth_rows_per_second,
            ] {
                ensure!(
                    value.is_finite() && value >= 0.0,
                    "growth ceilings must be finite and nonnegative"
                );
            }
            for value in [limits.checkpoint_p99_ms, limits.recovery_ms] {
                ensure!(
                    value.is_finite() && value > 0.0,
                    "time ceilings must be finite and positive"
                );
            }
        }
        Ok(())
    }

    pub fn rows_per_pipeline(&self) -> u64 {
        let pause = if self.fault == Fault::SourcePause {
            5
        } else {
            0
        };
        (self.seconds - pause) * self.rps_per_pipeline
    }

    pub fn measured_rows_per_pipeline(&self) -> u64 {
        self.rows_per_pipeline() - self.warmup_seconds * self.rps_per_pipeline
    }

    pub fn total_rows(&self) -> u64 {
        self.rows_per_pipeline() * self.pipelines as u64
    }

    pub fn scheduled(&self, id: u64) -> Duration {
        let mut nanos = id * 1_000_000_000 / self.rps_per_pipeline;
        if self.fault == Fault::SourcePause && id >= self.seconds / 2 * self.rps_per_pipeline {
            nanos += 5_000_000_000;
        }
        Duration::from_nanos(nanos)
    }

    pub fn offered(&self, elapsed: Duration) -> u64 {
        let mut seconds = elapsed.as_secs_f64();
        if self.fault == Fault::SourcePause {
            let pause_at = (self.seconds / 2) as f64;
            if (pause_at..pause_at + 5.0).contains(&seconds) {
                return self.seconds / 2 * self.rps_per_pipeline * self.pipelines as u64;
            }
            seconds -= (seconds - (self.seconds / 2) as f64).clamp(0.0, 5.0);
        }
        ((seconds * self.rps_per_pipeline as f64).floor() as u64 + 1).min(self.rows_per_pipeline())
            * self.pipelines as u64
    }
}
