//! Prometheus metrics for the streaming engine.

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry,
};

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
    /// Per-source watermark (epoch-ms). Label: `source`.
    pub source_watermark_ms: IntGaugeVec,
    /// Per-stream watermark (epoch-ms). Label: `stream`.
    pub stream_watermark_ms: IntGaugeVec,
    /// Per-stream input-port buffered bytes. Label: `stream`.
    pub input_buf_bytes: IntGaugeVec,
    /// Per-stream rows shed by the `ShedOldest` policy. Label: `stream`.
    pub shed_records_total: IntCounterVec,
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
    /// Time an operator spent waiting for the slowest input to deliver
    /// its checkpoint barrier. Label: `operator`. Zero for single-input.
    pub barrier_alignment_wait: HistogramVec,
    /// Delay between local watermark advancement and its cluster-wide
    /// observation. Written by the cluster watermark bus.
    pub watermark_propagation: Histogram,
    /// End-to-end event latency, source ingest to sink commit.
    /// Label: `pipeline`. The primary SLA metric.
    pub pipeline_e2e_latency: HistogramVec,
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
            // Labels are catalog-bound, so cardinality is finite.
            source_watermark_ms: reg!(IntGaugeVec::new(
                Opts::new("source_watermark_ms", "Per-source watermark (epoch-ms)"),
                &["source"],
            )
            .unwrap()),
            stream_watermark_ms: reg!(IntGaugeVec::new(
                Opts::new("stream_watermark_ms", "Per-stream watermark (epoch-ms)"),
                &["stream"],
            )
            .unwrap()),
            input_buf_bytes: reg!(IntGaugeVec::new(
                Opts::new("input_buf_bytes", "Per-stream input buffer bytes"),
                &["stream"],
            )
            .unwrap()),
            shed_records_total: reg!(IntCounterVec::new(
                Opts::new("shed_records_total", "Rows shed by ShedOldest policy"),
                &["stream"],
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
            // Alignment wait: typical <1ms, worst-case tens of seconds
            // under skew. 0.0001s * 2^16 = 6.5s base range; tail captured.
            barrier_alignment_wait: reg!(HistogramVec::new(
                HistogramOpts::new(
                    "barrier_alignment_wait_seconds",
                    "Operator wait for slowest input barrier",
                )
                .buckets(prometheus::exponential_buckets(0.0001, 2.0, 16).unwrap()),
                &["operator"],
            )
            .unwrap()),
            // Watermark propagation: gossip is ~100ms–1s; coordinator-
            // mediated could push down to 20–50ms. Bucketing covers both.
            watermark_propagation: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "watermark_propagation_seconds",
                    "Delay between local watermark advancement and peer observation",
                )
                .buckets(prometheus::exponential_buckets(0.001, 2.0, 14).unwrap()),
            )
            .unwrap()),
            // E2E latency: dominated by checkpoint cadence, so same
            // bucket shape as checkpoint_duration.
            pipeline_e2e_latency: reg!(HistogramVec::new(
                HistogramOpts::new(
                    "pipeline_e2e_latency_seconds",
                    "End-to-end event latency, source to sink commit",
                )
                .buckets(prometheus::exponential_buckets(0.01, 2.0, 15).unwrap()),
                &["pipeline"],
            )
            .unwrap()),
        }
    }
}
