//! Flow and skew series: where accepted input waits, how full the shared budgets are, and how
//! evenly managed state spreads over this node's vnodes.
//!
//! Labels are bounded by the catalog (source, sink, operator) or by fixed literals; no series is
//! labelled by vnode or key.

#[cfg(feature = "cluster")]
mod shuffle;

#[cfg(feature = "cluster")]
use prometheus::IntCounterVec;
use prometheus::{
    Gauge, Histogram, HistogramOpts, HistogramTimer, HistogramVec, IntGauge, IntGaugeVec, Opts,
    PullingGauge, Registry,
};

use crate::pipeline::streaming_coordinator::CoordinatorFlowMetrics;

/// Flow and skew metrics registered with the engine metrics.
pub(crate) struct FlowMetrics {
    pub(crate) coordinator: CoordinatorFlowMetrics,
    /// Time a sink write or checkpoint step waited for the sink's next writable epoch. An epoch
    /// that is already open is not observed. Label: `sink`.
    sink_epoch_gate_wait_seconds: HistogramVec,
    /// Commands queued for each sink task. Label: `sink`.
    sink_command_queue_depth: IntGaugeVec,
    /// Accounted managed state over `pipeline_max_managed_state_bytes`; 1.0 halts the pipeline.
    pub(crate) managed_state_budget_ratio: Gauge,
    /// Rows of an aggregate's input owned by another node (`input`) and the rows it put on the
    /// wire for them (`sent`). Labels: `operator`, `form`.
    #[cfg(feature = "cluster")]
    pub(crate) aggregate_shuffle_rows_total: IntCounterVec,
    #[cfg(feature = "cluster")]
    pub(crate) shuffle: shuffle::ShuffleFlowMetrics,
    /// Accounted state bytes of this node's largest vnode. Label: `operator`.
    pub(crate) vnode_state_bytes_max: IntGaugeVec,
    /// Mean accounted state bytes over the vnodes this node holds state for. Label: `operator`.
    pub(crate) vnode_state_bytes_mean: IntGaugeVec,
    /// Group count of this node's vnode with the most groups. Label: `operator`.
    pub(crate) vnode_groups_max: IntGaugeVec,
    /// Id of this node's largest vnode by state bytes (lowest id on ties). Label: `operator`.
    pub(crate) vnode_hottest: IntGaugeVec,
    /// Each keyed operator's state per vnode at the last sample, for the vnode-load endpoint.
    #[cfg(feature = "cluster")]
    pub(crate) vnode_loads: crate::vnode_load::VnodeLoadBoard,
}

/// The flow series of one sink, resolved once when its task starts.
pub(crate) struct SinkFlowMetrics {
    epoch_gate_wait: Histogram,
    command_queue_depth: IntGauge,
}

impl SinkFlowMetrics {
    /// Observes on drop, so every exit of the wait records its duration.
    pub(crate) fn time_epoch_gate_wait(&self) -> HistogramTimer {
        self.epoch_gate_wait.start_timer()
    }

    pub(crate) fn observe_command_queue(&self, depth: usize) {
        self.command_queue_depth
            .set(i64::try_from(depth).unwrap_or(i64::MAX));
    }
}

fn register<M: prometheus::core::Collector + Clone + 'static>(registry: &Registry, metric: M) -> M {
    registry.register(Box::new(metric.clone())).unwrap();
    metric
}

fn labelled_gauge(registry: &Registry, name: &str, help: &str, label: &str) -> IntGaugeVec {
    register(
        registry,
        IntGaugeVec::new(Opts::new(name, help), &[label]).unwrap(),
    )
}

impl FlowMetrics {
    pub(crate) fn sink(&self, name: &str) -> SinkFlowMetrics {
        SinkFlowMetrics {
            epoch_gate_wait: self.sink_epoch_gate_wait_seconds.with_label_values(&[name]),
            command_queue_depth: self.sink_command_queue_depth.with_label_values(&[name]),
        }
    }

    /// # Panics
    ///
    /// Panics if registration fails (duplicate names); registration happens once at startup.
    pub(crate) fn new(registry: &Registry) -> Self {
        let subscription_log_bytes = PullingGauge::new(
            "subscription_log_bytes",
            "Bytes local subscription logs hold against the shared 256 MiB process budget",
            Box::new(|| {
                let bytes = crate::subscription::process_subscription_log_bytes();
                // The process budget is 256 MiB, so every charge is exact in f64.
                f64::from(u32::try_from(bytes).unwrap_or(u32::MAX))
            }),
        )
        .unwrap();
        registry.register(Box::new(subscription_log_bytes)).unwrap();
        Self {
            coordinator: CoordinatorFlowMetrics::register(registry).unwrap(),
            sink_epoch_gate_wait_seconds: register(
                registry,
                HistogramVec::new(
                    HistogramOpts::new(
                        "sink_epoch_gate_wait_seconds",
                        "Time a sink write or checkpoint step waited for the sink's next writable epoch",
                    )
                    // 0.001 * 2^17 = 131s covers the default 120s checkpoint timeout.
                    .buckets(prometheus::exponential_buckets(0.001, 2.0, 18).unwrap()),
                    &["sink"],
                )
                .unwrap(),
            ),
            sink_command_queue_depth: labelled_gauge(
                registry,
                "sink_command_queue_depth",
                "Commands queued for each sink task",
                "sink",
            ),
            managed_state_budget_ratio: register(
                registry,
                Gauge::new(
                    "managed_state_budget_ratio",
                    "Accounted managed state over pipeline_max_managed_state_bytes; 1 halts the pipeline",
                )
                .unwrap(),
            ),
            #[cfg(feature = "cluster")]
            aggregate_shuffle_rows_total: register(
                registry,
                IntCounterVec::new(
                    Opts::new(
                        "aggregate_shuffle_rows_total",
                        "Aggregate input rows owned by another node (form=input) and rows sent for them (form=sent)",
                    ),
                    &["operator", "form"],
                )
                .unwrap(),
            ),
            #[cfg(feature = "cluster")]
            shuffle: shuffle::ShuffleFlowMetrics::register(registry).unwrap(),
            vnode_state_bytes_max: labelled_gauge(
                registry,
                "vnode_state_bytes_max",
                "Accounted state bytes of this node's largest vnode",
                "operator",
            ),
            vnode_state_bytes_mean: labelled_gauge(
                registry,
                "vnode_state_bytes_mean",
                "Mean accounted state bytes over the vnodes this node holds state for",
                "operator",
            ),
            vnode_groups_max: labelled_gauge(
                registry,
                "vnode_groups_max",
                "Group count of this node's vnode with the most groups",
                "operator",
            ),
            vnode_hottest: labelled_gauge(
                registry,
                "vnode_hottest",
                "Id of this node's largest vnode by state bytes, lowest id on ties",
                "operator",
            ),
            #[cfg(feature = "cluster")]
            vnode_loads: crate::vnode_load::VnodeLoadBoard::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn flow_series_have_their_published_names() {
        let registry = prometheus::Registry::new();
        let metrics = crate::EngineMetrics::new(&registry);
        let flow = &metrics.flow;
        let sink = flow.sink("out");
        sink.observe_command_queue(0);
        drop(sink.time_epoch_gate_wait());
        for gauge in [
            &flow.vnode_state_bytes_max,
            &flow.vnode_state_bytes_mean,
            &flow.vnode_groups_max,
            &flow.vnode_hottest,
        ] {
            gauge.with_label_values(&["agg"]).set(0);
        }
        #[cfg(feature = "cluster")]
        flow.aggregate_shuffle_rows_total
            .with_label_values(&["agg", "input"])
            .inc();
        let names = registry
            .gather()
            .into_iter()
            .map(|family| family.name().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        for expected in [
            "source_queue_reserved_bytes",
            "source_queue_messages",
            "intake_paused_seconds_total",
            "sink_epoch_gate_wait_seconds",
            "sink_command_queue_depth",
            "managed_state_budget_ratio",
            "subscription_log_bytes",
            #[cfg(feature = "cluster")]
            "aggregate_shuffle_rows_total",
            "vnode_state_bytes_max",
            "vnode_state_bytes_mean",
            "vnode_groups_max",
            "vnode_hottest",
        ] {
            assert!(names.contains(expected), "missing metric {expected}");
        }
    }
}
