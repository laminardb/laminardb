//! Prometheus metrics for the streaming engine.

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};

/// Pipeline metrics registered on an explicit prometheus `Registry`.
///
/// Constructed once at startup, `Arc`-shared into `PipelineCallback`,
/// `CheckpointCoordinator`, and `OperatorGraph`.
pub struct EngineMetrics {
    /// Events ingested from sources.
    pub events_ingested: IntCounter,
    /// Events emitted to streams.
    pub events_emitted: IntCounter,
    /// Events dropped.
    pub events_dropped: IntCounter,
    /// Processing cycles completed.
    pub cycles: IntCounter,
    /// Batches processed.
    pub batches: IntCounter,
    /// Queries using compiled `PhysicalExpr`.
    pub queries_compiled: IntCounter,
    /// Queries using cached logical plan.
    pub queries_cached_plan: IntCounter,
    /// Cycles skipped by backpressure.
    pub cycles_backpressured: IntCounter,
    /// Materialized view updates.
    pub mv_updates: IntCounter,
    /// Approximate MV bytes stored.
    pub mv_bytes_stored: IntGauge,
    /// Global pipeline watermark.
    pub pipeline_watermark: IntGauge,
    /// Completed checkpoints.
    pub checkpoints_completed: IntCounter,
    /// Failed checkpoints.
    pub checkpoints_failed: IntCounter,
    /// Current checkpoint epoch.
    pub checkpoint_epoch: IntGauge,
    /// Last checkpoint size in bytes.
    pub checkpoint_size_bytes: IntGauge,
    /// Sink write errors.
    pub sink_write_failures: IntCounter,
    /// Sink write timeouts.
    pub sink_write_timeouts: IntCounter,
    /// Sink task channel closed.
    pub sink_task_channel_closed: IntCounter,
    /// Per-cycle processing duration.
    pub cycle_duration: Histogram,
    /// Checkpoint cycle duration.
    pub checkpoint_duration: Histogram,
    /// Sink pre-commit round-trip (2PC phase 1).
    pub sink_precommit_duration: Histogram,
    /// Sink commit round-trip (2PC phase 2).
    pub sink_commit_duration: Histogram,
}

impl EngineMetrics {
    /// Register all engine metrics on the given registry. Startup only.
    ///
    /// # Panics
    ///
    /// Panics if metric registration fails (duplicate names).
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn new(registry: &Registry) -> Self {
        macro_rules! reg {
            ($m:expr) => {{
                let m = $m;
                registry.register(Box::new(m.clone())).unwrap();
                m
            }};
        }

        Self {
            events_ingested: reg!(IntCounter::new(
                "events_ingested_total",
                "Events ingested from sources"
            )
            .unwrap()),
            events_emitted: reg!(IntCounter::new(
                "events_emitted_total",
                "Events emitted to streams"
            )
            .unwrap()),
            events_dropped: reg!(IntCounter::new("events_dropped_total", "Events dropped").unwrap()),
            cycles: reg!(IntCounter::new("cycles_total", "Processing cycles completed").unwrap()),
            batches: reg!(IntCounter::new("batches_total", "Batches processed").unwrap()),
            queries_compiled: reg!(IntCounter::new(
                "queries_compiled_total",
                "Queries using compiled PhysicalExpr"
            )
            .unwrap()),
            queries_cached_plan: reg!(IntCounter::new(
                "queries_cached_plan_total",
                "Queries using cached logical plan"
            )
            .unwrap()),
            cycles_backpressured: reg!(IntCounter::new(
                "cycles_backpressured_total",
                "Cycles skipped by backpressure"
            )
            .unwrap()),
            mv_updates: reg!(
                IntCounter::new("mv_updates_total", "Materialized view updates").unwrap()
            ),
            mv_bytes_stored: reg!(
                IntGauge::new("mv_bytes_stored", "Approximate MV bytes stored").unwrap()
            ),
            pipeline_watermark: reg!(IntGauge::new(
                "pipeline_watermark",
                "Global pipeline watermark"
            )
            .unwrap()),
            checkpoints_completed: reg!(IntCounter::new(
                "checkpoints_completed_total",
                "Completed checkpoints"
            )
            .unwrap()),
            checkpoints_failed: reg!(IntCounter::new(
                "checkpoints_failed_total",
                "Failed checkpoints"
            )
            .unwrap()),
            checkpoint_epoch: reg!(
                IntGauge::new("checkpoint_epoch", "Current checkpoint epoch").unwrap()
            ),
            checkpoint_size_bytes: reg!(IntGauge::new(
                "checkpoint_size_bytes",
                "Last checkpoint size"
            )
            .unwrap()),
            sink_write_failures: reg!(IntCounter::new(
                "sink_write_failures_total",
                "Sink write errors"
            )
            .unwrap()),
            sink_write_timeouts: reg!(IntCounter::new(
                "sink_write_timeouts_total",
                "Sink write timeouts"
            )
            .unwrap()),
            sink_task_channel_closed: reg!(IntCounter::new(
                "sink_task_channel_closed_total",
                "Sink task channel closed"
            )
            .unwrap()),
            cycle_duration: reg!(Histogram::with_opts(
                HistogramOpts::new("cycle_duration_seconds", "Per-cycle processing duration")
                    .buckets(vec![1e-7, 5e-7, 1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3]),
            )
            .unwrap()),
            // Checkpoint: serialization_timeout=120s, so max bucket must cover that.
            // 0.01 * 2^14 = 163.84s.
            checkpoint_duration: reg!(Histogram::with_opts(
                HistogramOpts::new("checkpoint_duration_seconds", "Checkpoint cycle duration")
                    .buckets(prometheus::exponential_buckets(0.01, 2.0, 15).unwrap()),
            )
            .unwrap()),
            // pre_commit_timeout=30s. 0.005 * 2^13 = 40.96s.
            sink_precommit_duration: reg!(Histogram::with_opts(
                HistogramOpts::new("sink_precommit_duration_seconds", "Sink pre-commit latency")
                    .buckets(prometheus::exponential_buckets(0.005, 2.0, 14).unwrap()),
            )
            .unwrap()),
            // commit_timeout=60s. 0.005 * 2^14 = 81.92s.
            sink_commit_duration: reg!(Histogram::with_opts(
                HistogramOpts::new("sink_commit_duration_seconds", "Sink commit latency")
                    .buckets(prometheus::exponential_buckets(0.005, 2.0, 15).unwrap()),
            )
            .unwrap()),
        }
    }
}
