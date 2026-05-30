//! Prometheus metrics for the streaming engine.

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
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
    /// `1` if a source is idle (excluded from the watermark min), else
    /// `0`. Label: `source`.
    pub source_idle: IntGaugeVec,
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
    /// Rows dropped because the sink's WHERE filter failed to compile to
    /// a `PhysicalExpr` (fail-closed). Label: `sink`.
    pub sink_filter_rejected_rows: IntCounterVec,
    /// Rows dropped at operator level past `allowed_lateness` (distinct
    /// from `events_dropped`, which is source-side).
    pub window_late_dropped: IntCounter,
    /// Source rows dropped because the event-time column was null.
    pub events_null_timestamp: IntCounter,
    /// Rows currently buffered by temporal-filter operators.
    pub temporal_filter_buffered: IntGauge,
    /// Z-set inserts (+1) emitted by temporal-filter operators.
    pub temporal_filter_inserts: IntCounter,
    /// Z-set retractions (-1) emitted by temporal-filter operators.
    pub temporal_filter_retracts: IntCounter,
    /// Late / born-expired / beyond-horizon rows dropped un-emitted.
    pub temporal_filter_dropped: IntCounter,
    /// Per-cycle processing duration.
    pub cycle_duration: Histogram,
    /// Checkpoint cycle duration.
    pub checkpoint_duration: Histogram,
    /// Sink pre-commit round-trip (2PC phase 1).
    pub sink_precommit_duration: Histogram,
    /// Sink commit round-trip (2PC phase 2).
    pub sink_commit_duration: Histogram,
    /// On-demand lookup cache hits (served without a source fetch). Label: `table`.
    pub lookup_cache_hits: IntCounterVec,
    /// On-demand lookup cache misses (not in cache). Label: `table`.
    pub lookup_cache_misses: IntCounterVec,
    /// On-demand lookup source fetch errors/timeouts. Label: `table`.
    pub lookup_source_errors: IntCounterVec,
    /// On-demand lookup rows awaiting a source fetch. Label: `table`.
    pub lookup_in_flight_rows: IntGaugeVec,
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
            source_idle: reg!(IntGaugeVec::new(
                Opts::new(
                    "source_idle",
                    "1 if source idle (excluded from watermark min)"
                ),
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
            sink_filter_rejected_rows: reg!(IntCounterVec::new(
                Opts::new(
                    "sink_filter_rejected_rows_total",
                    "Rows dropped because the sink filter failed to compile",
                ),
                &["sink"],
            )
            .unwrap()),
            window_late_dropped: reg!(IntCounter::new(
                "window_late_dropped_total",
                "Rows dropped by window operators past allowed_lateness"
            )
            .unwrap()),
            events_null_timestamp: reg!(IntCounter::new(
                "events_null_timestamp_total",
                "Source rows dropped because the event-time column was null"
            )
            .unwrap()),
            temporal_filter_buffered: reg!(IntGauge::new(
                "temporal_filter_buffered",
                "Rows buffered by retracting temporal-filter operators"
            )
            .unwrap()),
            temporal_filter_inserts: reg!(IntCounter::new(
                "temporal_filter_inserts_total",
                "Z-set inserts emitted by temporal-filter operators"
            )
            .unwrap()),
            temporal_filter_retracts: reg!(IntCounter::new(
                "temporal_filter_retracts_total",
                "Z-set retractions emitted by temporal-filter operators"
            )
            .unwrap()),
            temporal_filter_dropped: reg!(IntCounter::new(
                "temporal_filter_dropped_total",
                "Rows dropped un-emitted by temporal-filter operators"
            )
            .unwrap()),
            cycle_duration: reg!(Histogram::with_opts(
                HistogramOpts::new("cycle_duration_seconds", "Per-cycle processing duration")
                    .buckets(vec![
                        1e-7, 5e-7, 1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2,
                        1e-1, 5e-1, 1.0,
                    ]),
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
            // Labels are bound to the registered lookup tables, so cardinality is finite.
            lookup_cache_hits: reg!(IntCounterVec::new(
                Opts::new("lookup_cache_hits_total", "On-demand lookup cache hits"),
                &["table"],
            )
            .unwrap()),
            lookup_cache_misses: reg!(IntCounterVec::new(
                Opts::new("lookup_cache_misses_total", "On-demand lookup cache misses"),
                &["table"],
            )
            .unwrap()),
            lookup_source_errors: reg!(IntCounterVec::new(
                Opts::new(
                    "lookup_source_errors_total",
                    "On-demand lookup source fetch errors"
                ),
                &["table"],
            )
            .unwrap()),
            lookup_in_flight_rows: reg!(IntGaugeVec::new(
                Opts::new(
                    "lookup_in_flight_rows",
                    "On-demand lookup rows awaiting a source fetch"
                ),
                &["table"],
            )
            .unwrap()),
        }
    }
}
