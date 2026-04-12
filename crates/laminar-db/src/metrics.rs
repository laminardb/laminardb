//! Pipeline observability metrics.

use std::time::Duration;

/// The state of a streaming pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineState {
    /// Created but not started.
    Created,
    /// Starting.
    Starting,
    /// Processing events.
    Running,
    /// Gracefully shutting down.
    ShuttingDown,
    /// Stopped.
    Stopped,
}

impl std::fmt::Display for PipelineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Starting => write!(f, "Starting"),
            Self::Running => write!(f, "Running"),
            Self::ShuttingDown => write!(f, "ShuttingDown"),
            Self::Stopped => write!(f, "Stopped"),
        }
    }
}

/// Pipeline-wide metrics snapshot.
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    /// Events ingested.
    pub total_events_ingested: u64,
    /// Events emitted.
    pub total_events_emitted: u64,
    /// Events dropped.
    pub total_events_dropped: u64,
    /// Cycles.
    pub total_cycles: u64,
    /// Batches.
    pub total_batches: u64,
    /// Uptime.
    pub uptime: Duration,
    /// State.
    pub state: PipelineState,
    /// Sources.
    pub source_count: usize,
    /// Streams.
    pub stream_count: usize,
    /// Sinks.
    pub sink_count: usize,
    /// Min watermark across all sources.
    pub pipeline_watermark: i64,
    /// MV updates.
    pub mv_updates: u64,
    /// Approximate MV bytes.
    pub mv_bytes_stored: u64,
}

/// Metrics for a single registered source.
#[derive(Debug, Clone)]
pub struct SourceMetrics {
    /// Name.
    pub name: String,
    /// Total events (sequence number).
    pub total_events: u64,
    /// Buffered events.
    pub pending: usize,
    /// Capacity.
    pub capacity: usize,
    /// >80% full.
    pub is_backpressured: bool,
    /// Watermark.
    pub watermark: i64,
    /// 0.0..1.0.
    pub utilization: f64,
}

/// Metrics for a single registered stream.
#[derive(Debug, Clone)]
pub struct StreamMetrics {
    /// Name.
    pub name: String,
    /// Total events.
    pub total_events: u64,
    /// Buffered events.
    pub pending: usize,
    /// Capacity.
    pub capacity: usize,
    /// >80% full.
    pub is_backpressured: bool,
    /// Watermark.
    pub watermark: i64,
    /// Defining SQL query.
    pub sql: Option<String>,
}

const BACKPRESSURE_THRESHOLD: f64 = 0.8;

#[must_use]
#[allow(clippy::cast_precision_loss)]
pub(crate) fn is_backpressured(pending: usize, capacity: usize) -> bool {
    capacity > 0 && (pending as f64 / capacity as f64) > BACKPRESSURE_THRESHOLD
}

#[must_use]
#[allow(clippy::cast_precision_loss)]
pub(crate) fn utilization(pending: usize, capacity: usize) -> f64 {
    if capacity == 0 {
        0.0
    } else {
        pending as f64 / capacity as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_state_display() {
        assert_eq!(PipelineState::Created.to_string(), "Created");
        assert_eq!(PipelineState::Starting.to_string(), "Starting");
        assert_eq!(PipelineState::Running.to_string(), "Running");
        assert_eq!(PipelineState::ShuttingDown.to_string(), "ShuttingDown");
        assert_eq!(PipelineState::Stopped.to_string(), "Stopped");
    }

    #[test]
    fn test_pipeline_state_equality() {
        assert_eq!(PipelineState::Running, PipelineState::Running);
        assert_ne!(PipelineState::Created, PipelineState::Running);
    }

    #[test]
    fn test_backpressure_detection() {
        // Empty buffer: not backpressured
        assert!(!is_backpressured(0, 100));
        // 50% full: not backpressured
        assert!(!is_backpressured(50, 100));
        // 80% full: not backpressured (threshold is >0.8, not >=)
        assert!(!is_backpressured(80, 100));
        // 81% full: backpressured
        assert!(is_backpressured(81, 100));
        // Full: backpressured
        assert!(is_backpressured(100, 100));
        // Zero capacity: not backpressured
        assert!(!is_backpressured(0, 0));
    }

    #[test]
    fn test_utilization() {
        assert!((utilization(0, 100) - 0.0).abs() < f64::EPSILON);
        assert!((utilization(50, 100) - 0.5).abs() < f64::EPSILON);
        assert!((utilization(100, 100) - 1.0).abs() < f64::EPSILON);
        assert!((utilization(0, 0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pipeline_metrics_clone() {
        let m = PipelineMetrics {
            total_events_ingested: 100,
            total_events_emitted: 50,
            total_events_dropped: 0,
            total_cycles: 10,
            total_batches: 5,
            uptime: Duration::from_secs(60),
            state: PipelineState::Running,
            source_count: 2,
            stream_count: 1,
            sink_count: 1,
            pipeline_watermark: i64::MIN,
            mv_updates: 0,
            mv_bytes_stored: 0,
        };
        let m2 = m.clone();
        assert_eq!(m2.total_events_ingested, 100);
        assert_eq!(m2.state, PipelineState::Running);
    }

    #[test]
    fn test_source_metrics_debug() {
        let m = SourceMetrics {
            name: "trades".to_string(),
            total_events: 1000,
            pending: 50,
            capacity: 1024,
            is_backpressured: false,
            watermark: 12345,
            utilization: 0.05,
        };
        let dbg = format!("{m:?}");
        assert!(dbg.contains("trades"));
        assert!(dbg.contains("1000"));
    }

    #[test]
    fn test_stream_metrics_with_sql() {
        let m = StreamMetrics {
            name: "avg_price".to_string(),
            total_events: 500,
            pending: 0,
            capacity: 1024,
            is_backpressured: false,
            watermark: 0,
            sql: Some("SELECT symbol, AVG(price) FROM trades GROUP BY symbol".to_string()),
        };
        assert_eq!(
            m.sql.as_deref(),
            Some("SELECT symbol, AVG(price) FROM trades GROUP BY symbol")
        );
    }
}
