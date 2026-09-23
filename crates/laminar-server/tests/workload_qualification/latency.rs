use anyhow::{ensure, Result};
use prometheus::{core::Collector as _, exponential_buckets, Histogram, HistogramOpts};
use serde::Serialize;

pub(super) struct Latency(Histogram);

#[derive(Debug, Serialize)]
pub(super) struct Distribution {
    pub samples: u64,
    pub quantile_upper_ms: [Option<f64>; 4],
    pub buckets_seconds: Vec<(f64, u64)>,
}

impl Latency {
    pub fn new() -> Result<Self> {
        // One-percent buckets, from 1us to over 10 minutes. Overflow stays visible in +Inf.
        let buckets = exponential_buckets(0.000_001, 1.01, 2_050)?;
        Ok(Self(Histogram::with_opts(
            HistogramOpts::new(
                "qualification_visibility_seconds",
                "Scheduled arrival to first external observation",
            )
            .buckets(buckets),
        )?))
    }

    pub fn observe(&self, seconds: f64) {
        self.0.observe(seconds);
    }

    pub fn distribution(&self) -> Distribution {
        let families = self.0.collect();
        let histogram = families[0].get_metric()[0].get_histogram();
        let samples = histogram.get_sample_count();
        let buckets_seconds: Vec<_> = histogram
            .get_bucket()
            .iter()
            .map(|b| (b.upper_bound(), b.cumulative_count()))
            .collect();
        let quantile_upper_ms = [500_u64, 950, 990, 999].map(|quantile| {
            let rank = (u128::from(samples) * u128::from(quantile)).div_ceil(1_000);
            (samples > 0)
                .then(|| {
                    buckets_seconds
                        .iter()
                        .find(|(_, count)| u128::from(*count) >= rank)
                        .map(|(bound, _)| bound * 1_000.0)
                })
                .flatten()
        });
        Distribution {
            samples,
            quantile_upper_ms,
            buckets_seconds,
        }
    }
}

impl Distribution {
    pub fn check(&self, limits: &[f64; 4]) -> Result<()> {
        ensure!(
            self.samples >= 100_000,
            "insufficient p99.9 samples: {}",
            self.samples
        );
        for ((name, observed), limit) in ["p50", "p95", "p99", "p99.9"]
            .iter()
            .zip(self.quantile_upper_ms)
            .zip(limits)
        {
            ensure!(
                observed.is_some_and(|value| value <= *limit),
                "{name} visibility {observed:?}ms exceeds {limit}ms or histogram range"
            );
        }
        Ok(())
    }
}
