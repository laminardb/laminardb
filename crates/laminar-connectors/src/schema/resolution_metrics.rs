//! Low-cardinality control-plane schema resolution telemetry.

use crate::error::ConnectorError;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};
use std::sync::Arc;

pub(crate) struct ResolutionMetrics {
    duration: HistogramVec,
    outcomes: IntCounterVec,
    active: IntGaugeVec,
}

impl ResolutionMetrics {
    pub(crate) fn register(registry: &Registry) -> Result<Arc<Self>, ConnectorError> {
        let metrics = Self {
            duration: HistogramVec::new(
                HistogramOpts::new(
                    "laminar_schema_resolution_seconds",
                    "Schema resolution queue and work latency",
                )
                .buckets(vec![0.001, 0.01, 0.1, 1.0, 5.0, 10.0, 30.0]),
                &["direction"],
            )
            .map_err(metrics_error)?,
            outcomes: IntCounterVec::new(
                Opts::new(
                    "laminar_schema_resolution_total",
                    "Schema resolution outcomes",
                ),
                &["direction", "outcome"],
            )
            .map_err(metrics_error)?,
            active: IntGaugeVec::new(
                Opts::new(
                    "laminar_schema_resolution_active",
                    "Queued and active control-plane schema resolutions",
                ),
                &["direction"],
            )
            .map_err(metrics_error)?,
        };
        registry
            .register(Box::new(metrics.duration.clone()))
            .map_err(metrics_error)?;
        registry
            .register(Box::new(metrics.outcomes.clone()))
            .map_err(metrics_error)?;
        registry
            .register(Box::new(metrics.active.clone()))
            .map_err(metrics_error)?;
        Ok(Arc::new(metrics))
    }
}

pub(crate) struct ResolutionObservation {
    metrics: Option<Arc<ResolutionMetrics>>,
    direction: &'static str,
    started: std::time::Instant,
    outcome: &'static str,
}

impl ResolutionObservation {
    pub(crate) fn new(metrics: Option<Arc<ResolutionMetrics>>, direction: &'static str) -> Self {
        if let Some(metrics) = &metrics {
            metrics.active.with_label_values(&[direction]).inc();
        }
        Self {
            metrics,
            direction,
            started: std::time::Instant::now(),
            outcome: "cancelled",
        }
    }

    pub(crate) fn finish<T>(
        mut self,
        result: Result<T, ConnectorError>,
    ) -> Result<T, ConnectorError> {
        self.outcome = if result.is_ok() { "success" } else { "failure" };
        result
    }
}

impl Drop for ResolutionObservation {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.active.with_label_values(&[self.direction]).dec();
            metrics
                .outcomes
                .with_label_values(&[self.direction, self.outcome])
                .inc();
            metrics
                .duration
                .with_label_values(&[self.direction])
                .observe(self.started.elapsed().as_secs_f64());
        }
    }
}

#[allow(clippy::needless_pass_by_value)] // Adapter for owned Result::map_err errors.
fn metrics_error(error: prometheus::Error) -> ConnectorError {
    ConnectorError::Internal(format!("schema resolution metrics registration: {error}"))
}
