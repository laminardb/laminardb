//! Prometheus metrics for the streaming engine.

use prometheus::{
    Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry,
};

/// Bounded-cardinality metrics for committed cluster subscriptions.
#[cfg(feature = "cluster")]
pub struct ClusterSubscriptionMetrics {
    /// Readers currently attached to this gateway process.
    pub active_readers: IntGauge,
    /// Cluster subscription open attempts handled by this process.
    pub open_total: IntCounter,
    /// Failed cluster subscription open attempts.
    pub open_failures_total: IntCounter,
    /// Output frames made visible by authoritative checkpoint Commit outcomes.
    pub frames_committed_total: IntCounter,
    /// Output rows made visible by authoritative checkpoint Commit outcomes.
    pub rows_committed_total: IntCounter,
    /// Encoded segment bytes made visible by authoritative checkpoint Commit outcomes.
    pub bytes_committed_total: IntCounter,
    /// Immutable output segments written by this process.
    pub segments_written_total: IntCounter,
    /// Immutable output segment writes that failed.
    pub segment_write_failures_total: IntCounter,
    /// Manifest construction, loading, or validation failures.
    pub manifest_failures_total: IntCounter,
    /// Committed manifest or segment integrity failures.
    pub integrity_failures_total: IntCounter,
    /// Fenced output writer attempts rejected by this process.
    pub stale_writer_rejections_total: IntCounter,
    /// Partition continuity failures detected by writers or readers.
    pub sequence_gaps_total: IntCounter,
    /// Encoded bytes read during committed replay.
    pub replay_bytes_total: IntCounter,
    /// Frames read during committed replay.
    pub replay_frames_total: IntCounter,
    /// Replay opens rejected because their epoch was pruned.
    pub replay_pruned_total: IntCounter,
    /// Readers disconnected after exhausting bounded gateway lag allowance.
    pub gateway_lag_disconnects_total: IntCounter,
    /// In-memory output bytes awaiting checkpoint disposition on this process.
    pub pending_bytes: IntGauge,
    /// Bytes reachable from the retained committed output-log suffix.
    pub retained_bytes: IntGauge,
    /// Grace-held unreachable output bytes found by the latest orphan scan.
    pub orphan_bytes: IntGauge,
    /// Subscription output encoding and upload duration during checkpoint preparation.
    pub checkpoint_prepare_seconds: Histogram,
    /// Authoritative Commit publication duration before local visibility.
    pub commit_visibility_seconds: Histogram,
    /// Gateway committed-index refresh duration.
    pub gateway_manifest_refresh_seconds: Histogram,
}

#[cfg(feature = "cluster")]
impl ClusterSubscriptionMetrics {
    fn new(registry: &Registry) -> Self {
        Self {
            active_readers: register_int_gauge(
                registry,
                "cluster_subscription_active_readers",
                "Committed cluster subscription readers attached to this process",
            ),
            open_total: register_int_counter(
                registry,
                "cluster_subscription_open_total",
                "Committed cluster subscription open attempts",
            ),
            open_failures_total: register_int_counter(
                registry,
                "cluster_subscription_open_failures_total",
                "Failed committed cluster subscription open attempts",
            ),
            frames_committed_total: register_int_counter(
                registry,
                "cluster_subscription_frames_committed_total",
                "Output frames made visible by authoritative checkpoint commits",
            ),
            rows_committed_total: register_int_counter(
                registry,
                "cluster_subscription_rows_committed_total",
                "Output rows made visible by authoritative checkpoint commits",
            ),
            bytes_committed_total: register_int_counter(
                registry,
                "cluster_subscription_bytes_committed_total",
                "Encoded output bytes made visible by authoritative checkpoint commits",
            ),
            segments_written_total: register_int_counter(
                registry,
                "cluster_subscription_segments_written_total",
                "Immutable committed-output segments written by this process",
            ),
            segment_write_failures_total: register_int_counter(
                registry,
                "cluster_subscription_segment_write_failures_total",
                "Immutable committed-output segment writes that failed",
            ),
            manifest_failures_total: register_int_counter(
                registry,
                "cluster_subscription_manifest_failures_total",
                "Committed-output manifest construction, loading, or validation failures",
            ),
            integrity_failures_total: register_int_counter(
                registry,
                "cluster_subscription_integrity_failures_total",
                "Committed-output manifest or segment integrity failures",
            ),
            stale_writer_rejections_total: register_int_counter(
                registry,
                "cluster_subscription_stale_writer_rejections_total",
                "Fenced committed-output writer attempts rejected by this process",
            ),
            sequence_gaps_total: register_int_counter(
                registry,
                "cluster_subscription_sequence_gaps_total",
                "Partition sequence continuity failures detected by writers or readers",
            ),
            replay_bytes_total: register_int_counter(
                registry,
                "cluster_subscription_replay_bytes_total",
                "Encoded bytes read during committed subscription replay",
            ),
            replay_frames_total: register_int_counter(
                registry,
                "cluster_subscription_replay_frames_total",
                "Frames read during committed subscription replay",
            ),
            replay_pruned_total: register_int_counter(
                registry,
                "cluster_subscription_replay_pruned_total",
                "Replay opens rejected because their epoch was pruned",
            ),
            gateway_lag_disconnects_total: register_int_counter(
                registry,
                "cluster_subscription_gateway_lag_disconnects_total",
                "Readers disconnected after exhausting bounded gateway lag allowance",
            ),
            pending_bytes: register_int_gauge(
                registry,
                "cluster_subscription_pending_bytes",
                "In-memory committed-output bytes awaiting checkpoint disposition",
            ),
            retained_bytes: register_int_gauge(
                registry,
                "cluster_subscription_retained_bytes",
                "Bytes reachable from the retained committed output-log suffix",
            ),
            orphan_bytes: register_int_gauge(
                registry,
                "cluster_subscription_orphan_bytes",
                "Grace-held unreachable output bytes found by the latest orphan scan",
            ),
            checkpoint_prepare_seconds: register_subscription_histogram(
                registry,
                "cluster_subscription_checkpoint_prepare_seconds",
                "Subscription output encoding and upload duration during checkpoint preparation",
            ),
            commit_visibility_seconds: register_subscription_histogram(
                registry,
                "cluster_subscription_commit_visibility_seconds",
                "Authoritative checkpoint Commit publication duration before local visibility",
            ),
            gateway_manifest_refresh_seconds: register_subscription_histogram(
                registry,
                "cluster_subscription_gateway_manifest_refresh_seconds",
                "Gateway committed-index refresh duration",
            ),
        }
    }
}

fn register_int_counter(registry: &Registry, name: &str, help: &str) -> IntCounter {
    let metric = IntCounter::new(name, help).unwrap();
    registry.register(Box::new(metric.clone())).unwrap();
    metric
}

fn register_event_counters(registry: &Registry) -> (IntCounter, IntCounter) {
    (
        register_int_counter(
            registry,
            "events_ingested_total",
            "Events ingested from sources",
        ),
        register_int_counter(
            registry,
            "events_emitted_total",
            "Events emitted to streams",
        ),
    )
}

#[cfg(feature = "cluster")]
fn register_int_gauge(registry: &Registry, name: &str, help: &str) -> IntGauge {
    let metric = IntGauge::new(name, help).unwrap();
    registry.register(Box::new(metric.clone())).unwrap();
    metric
}

#[cfg(feature = "cluster")]
fn register_subscription_histogram(registry: &Registry, name: &str, help: &str) -> Histogram {
    let metric = Histogram::with_opts(
        HistogramOpts::new(name, help)
            .buckets(prometheus::exponential_buckets(0.001, 2.0, 18).unwrap()),
    )
    .unwrap();
    registry.register(Box::new(metric.clone())).unwrap();
    metric
}

/// Pipeline metrics registered on an explicit prometheus `Registry`.
///
/// Constructed once at startup, `Arc`-shared into `PipelineCallback`,
/// `CheckpointCoordinator`, and `OperatorGraph`.
pub struct EngineMetrics {
    /// Committed cluster-subscription metrics without per-stream or per-reader labels.
    #[cfg(feature = "cluster")]
    pub cluster_subscription: ClusterSubscriptionMetrics,
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
    /// Operator-reported retained managed-state charge. Labels: `operator`, `phase`.
    /// `live` is current at sampling; transient `prepared`/`retired` values are the maximum
    /// observed since the prior sample. Current aggregate values are lower bounds between cold
    /// reconciliation points and exclude hash buckets, nested/shared payloads, allocator overhead,
    /// and process RSS.
    pub managed_state_accounted_bytes: IntGaugeVec,
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
    /// Window assignments dropped past `allowed_lateness`. A hopping-window row can contribute
    /// more than one assignment.
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
    /// SQL operator-graph execution within successful normal cycles.
    pub cycle_execute_duration: Histogram,
    /// Materialized-view and subscription-stream publication within successful normal cycles.
    pub cycle_output_store_duration: Histogram,
    /// Sink command admission within successful normal cycles.
    pub cycle_sink_enqueue_duration: Histogram,
    /// Per-operator processing duration. Labels: `operator`, `mode` (`normal` or
    /// `checkpoint_drain`).
    pub operator_process_duration: HistogramVec,
    /// Checkpoint cycle duration.
    pub checkpoint_duration: Histogram,
    /// Synchronous mutable checkpoint-state capture duration.
    pub checkpoint_state_capture_duration: Histogram,
    /// Pipeline stall per barrier: sink write fence, shuffle alignment, state capture, durable-tail
    /// handoff, and the Aligned resume gate. Every durable tail remains supervised in the
    /// background; committable sink writes wait on its exact successor-epoch gate.
    pub checkpoint_pipeline_stall_duration: Histogram,
    /// Local barrier work while the pipeline is paused: sink fencing, shuffle alignment, state
    /// capture, and construction of the immutable durable-tail handoff.
    pub checkpoint_barrier_local_duration: Histogram,
    /// Cluster-shuffle pause after local capture while waiting for the global Aligned release.
    /// Embedded and single-node runtimes do not observe this metric.
    pub checkpoint_aligned_resume_wait: Histogram,
    /// Sink pre-commit round-trip (2PC phase 1).
    pub sink_precommit_duration: Histogram,
    /// On-demand lookup cache hits (served without a source fetch). Label: `table`.
    pub lookup_cache_hits: IntCounterVec,
    /// On-demand lookup cache misses (not in cache). Label: `table`.
    pub lookup_cache_misses: IntCounterVec,
    /// On-demand lookup source fetch errors/timeouts. Label: `table`.
    pub lookup_source_errors: IntCounterVec,
    /// On-demand lookup rows awaiting a source fetch. Label: `table`.
    pub lookup_in_flight_rows: IntGaugeVec,
    /// Vnodes owned per failure domain (cluster mode). Label: `domain`.
    pub placement_vnodes_per_domain: IntGaugeVec,
    /// Largest single domain's share of all vnodes (`[0, 1]`) — the blast radius.
    pub placement_blast_radius_ratio: Gauge,
    /// Unexpected compute-thread exits (operator panics) that faulted the pipeline.
    pub pipeline_faults_total: IntCounter,
    /// Fatal cycle errors dropped-and-continued under at-least-once delivery.
    pub pipeline_cycle_errors_total: IntCounter,
    /// Automatic recover-from-checkpoint restarts triggered by the fault supervisor.
    pub pipeline_restarts_total: IntCounter,
    /// Leader-coordinated global restart-to-epoch rounds this node applied (cluster mode).
    pub coordinated_recoveries_total: IntCounter,
    /// Leader-coordinated recovery rounds abandoned (self-restore failed or restore quorum timed out).
    pub coordinated_recovery_failures_total: IntCounter,
    /// Shuffle delivery-loss incidents; each one fences an epoch and forces replay.
    pub shuffle_delivery_loss_incidents_total: IntCounter,
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

        let (events_ingested, events_emitted) = register_event_counters(registry);

        Self {
            #[cfg(feature = "cluster")]
            cluster_subscription: ClusterSubscriptionMetrics::new(registry),
            events_ingested,
            events_emitted,
            events_dropped: reg!(IntCounter::new("events_dropped_total", "Events dropped").unwrap()),
            cycles: reg!(IntCounter::new(
                "cycles_total",
                "Normal processing cycles completed (excludes checkpoint graph drains)"
            )
            .unwrap()),
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
            // Operator names are catalog-bound and phase is one of live/prepared/retired.
            managed_state_accounted_bytes: reg!(IntGaugeVec::new(
                Opts::new(
                    "managed_state_accounted_bytes",
                    "Operator-reported retained-state charge; current aggregate values are lower bounds between checkpoint/lifecycle reconciliation and exclude hash buckets, nested/shared payloads, allocator overhead, and RSS",
                ),
                &["operator", "phase"],
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
                "Window assignments dropped past allowed_lateness"
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
                HistogramOpts::new(
                    "cycle_duration_seconds",
                    "Normal processing-cycle duration (excludes checkpoint graph drains)",
                )
                .buckets(vec![
                        1e-7, 5e-7, 1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2,
                        1e-1, 5e-1, 1.0,
                    ]),
            )
            .unwrap()),
            cycle_execute_duration: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "cycle_execute_duration_seconds",
                    "SQL operator-graph execution within successful normal processing cycles",
                )
                .buckets(vec![
                    1e-7, 5e-7, 1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2,
                    1e-1, 5e-1, 1.0,
                ]),
            )
            .unwrap()),
            cycle_output_store_duration: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "cycle_output_store_duration_seconds",
                    "Materialized-view and subscription-stream publication within successful normal processing cycles",
                )
                .buckets(vec![
                    1e-7, 5e-7, 1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2,
                    1e-1, 5e-1, 1.0,
                ]),
            )
            .unwrap()),
            cycle_sink_enqueue_duration: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "cycle_sink_enqueue_duration_seconds",
                    "Sink publication preparation and command admission within successful normal processing cycles",
                )
                .buckets(vec![
                    1e-7, 5e-7, 1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2,
                    1e-1, 5e-1, 1.0,
                ]),
            )
            .unwrap()),
            // Operator names are catalog-bound and mode has exactly two values.
            operator_process_duration: reg!(HistogramVec::new(
                HistogramOpts::new(
                    "operator_process_duration_seconds",
                    "Operator processing duration by catalog operator and execution mode",
                )
                .buckets(prometheus::exponential_buckets(0.0001, 4.0, 10).unwrap()),
                &["operator", "mode"],
            )
            .unwrap()),
            // Checkpoint: serialization_timeout=120s, so max bucket must cover that.
            // 0.01 * 2^14 = 163.84s.
            checkpoint_duration: reg!(Histogram::with_opts(
                HistogramOpts::new("checkpoint_duration_seconds", "Checkpoint cycle duration")
                    .buckets(prometheus::exponential_buckets(0.01, 2.0, 15).unwrap()),
            )
            .unwrap()),
            checkpoint_state_capture_duration: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "checkpoint_state_capture_duration_seconds",
                    "Synchronous mutable checkpoint-state capture duration",
                )
                .buckets(prometheus::exponential_buckets(0.001, 2.0, 16).unwrap()),
            )
            .unwrap()),
            // Stall target is sub-second; the resume gate is bounded at
            // 30s. 0.001 * 2^15 = 32.77s.
            checkpoint_pipeline_stall_duration: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "checkpoint_pipeline_stall_duration_seconds",
                    "Pipeline stall per checkpoint barrier (align + capture + resume gate)",
                )
                .buckets(prometheus::exponential_buckets(0.001, 2.0, 16).unwrap()),
            )
            .unwrap()),
            checkpoint_barrier_local_duration: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "checkpoint_barrier_local_duration_seconds",
                    "Local paused barrier work (sink fence + shuffle align + state capture + tail handoff)",
                )
                .buckets(prometheus::exponential_buckets(0.001, 2.0, 16).unwrap()),
            )
            .unwrap()),
            checkpoint_aligned_resume_wait: reg!(Histogram::with_opts(
                HistogramOpts::new(
                    "checkpoint_aligned_resume_wait_seconds",
                    "Cluster-shuffle pause waiting for the global Aligned release",
                )
                .buckets(prometheus::exponential_buckets(0.001, 2.0, 16).unwrap()),
            )
            .unwrap()),
            // The one attempt deadline defaults to 120s. 0.005 * 2^15 = 163.84s.
            sink_precommit_duration: reg!(Histogram::with_opts(
                HistogramOpts::new("sink_precommit_duration_seconds", "Sink pre-commit latency")
                    .buckets(prometheus::exponential_buckets(0.005, 2.0, 16).unwrap()),
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
            placement_vnodes_per_domain: reg!(IntGaugeVec::new(
                Opts::new(
                    "placement_vnodes_per_domain",
                    "Vnodes owned per failure domain (cluster mode)"
                ),
                &["domain"],
            )
            .unwrap()),
            placement_blast_radius_ratio: reg!(Gauge::new(
                "placement_blast_radius_ratio",
                "Largest single domain's share of all vnodes (0-1); state affected if it fails"
            )
            .unwrap()),
            pipeline_faults_total: reg!(IntCounter::new(
                "pipeline_faults_total",
                "Compute-thread crashes that faulted the pipeline"
            )
            .unwrap()),
            pipeline_cycle_errors_total: reg!(IntCounter::new(
                "pipeline_cycle_errors_total",
                "Fatal cycle errors dropped-and-continued under at-least-once delivery"
            )
            .unwrap()),
            pipeline_restarts_total: reg!(IntCounter::new(
                "pipeline_restarts_total",
                "Automatic recover-from-checkpoint restarts after a pipeline fault"
            )
            .unwrap()),
            coordinated_recoveries_total: reg!(IntCounter::new(
                "coordinated_recoveries_total",
                "Leader-coordinated global restart-to-epoch rounds applied by this node"
            )
            .unwrap()),
            coordinated_recovery_failures_total: reg!(IntCounter::new(
                "coordinated_recovery_failures_total",
                "Leader-coordinated recovery rounds abandoned (self-restore failed or quorum timed out)"
            )
            .unwrap()),
            shuffle_delivery_loss_incidents_total: reg!(IntCounter::new(
                "shuffle_delivery_loss_incidents_total",
                "Cross-node shuffle delivery-loss incidents (fences the epoch, forces replay)"
            )
            .unwrap()),
        }
    }
}

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use super::*;

    #[test]
    fn cluster_subscription_metric_family_has_the_release_names() {
        let registry = Registry::new();
        let _metrics = EngineMetrics::new(&registry);
        let names = registry
            .gather()
            .into_iter()
            .map(|family| family.name().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        for expected in [
            "cluster_subscription_active_readers",
            "cluster_subscription_open_total",
            "cluster_subscription_open_failures_total",
            "cluster_subscription_frames_committed_total",
            "cluster_subscription_rows_committed_total",
            "cluster_subscription_bytes_committed_total",
            "cluster_subscription_segments_written_total",
            "cluster_subscription_segment_write_failures_total",
            "cluster_subscription_manifest_failures_total",
            "cluster_subscription_integrity_failures_total",
            "cluster_subscription_stale_writer_rejections_total",
            "cluster_subscription_sequence_gaps_total",
            "cluster_subscription_replay_bytes_total",
            "cluster_subscription_replay_frames_total",
            "cluster_subscription_replay_pruned_total",
            "cluster_subscription_gateway_lag_disconnects_total",
            "cluster_subscription_pending_bytes",
            "cluster_subscription_retained_bytes",
            "cluster_subscription_orphan_bytes",
            "cluster_subscription_checkpoint_prepare_seconds",
            "cluster_subscription_commit_visibility_seconds",
            "cluster_subscription_gateway_manifest_refresh_seconds",
        ] {
            assert!(names.contains(expected), "missing metric {expected}");
        }
    }
}
