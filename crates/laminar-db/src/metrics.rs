//! Pipeline observability metrics types.
//!
//! Provides atomic counters for pipeline-loop aggregates and snapshot types
//! for querying source, stream, and pipeline-wide metrics from user code.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// The state of a streaming pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineState {
    /// Pipeline has been created but not started.
    Created,
    /// Pipeline is in the process of starting.
    Starting,
    /// Pipeline is actively processing events.
    Running,
    /// Pipeline is gracefully shutting down.
    ShuttingDown,
    /// Pipeline has stopped.
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

/// Shared atomic counters incremented by the pipeline processing loop.
///
/// All reads and writes use `Ordering::Relaxed` — metrics are advisory,
/// not transactional.
pub struct PipelineCounters {
    /// Total events ingested from sources.
    pub events_ingested: AtomicU64,
    /// Total events emitted to streams/sinks.
    pub events_emitted: AtomicU64,
    /// Total events dropped (e.g. backpressure).
    pub events_dropped: AtomicU64,
    /// Total processing cycles completed.
    pub cycles: AtomicU64,
    /// Duration of the last processing cycle in nanoseconds.
    pub last_cycle_duration_ns: AtomicU64,
    /// Total batches processed.
    pub total_batches: AtomicU64,
}

impl PipelineCounters {
    /// Create zeroed counters.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events_ingested: AtomicU64::new(0),
            events_emitted: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            cycles: AtomicU64::new(0),
            last_cycle_duration_ns: AtomicU64::new(0),
            total_batches: AtomicU64::new(0),
        }
    }

    /// Take a snapshot of all counters.
    #[must_use]
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            events_ingested: self.events_ingested.load(Ordering::Relaxed),
            events_emitted: self.events_emitted.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            cycles: self.cycles.load(Ordering::Relaxed),
            last_cycle_duration_ns: self.last_cycle_duration_ns.load(Ordering::Relaxed),
            total_batches: self.total_batches.load(Ordering::Relaxed),
        }
    }
}

impl Default for PipelineCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time snapshot of [`PipelineCounters`].
#[derive(Debug, Clone, Copy)]
pub struct CounterSnapshot {
    /// Total events ingested.
    pub events_ingested: u64,
    /// Total events emitted.
    pub events_emitted: u64,
    /// Total events dropped.
    pub events_dropped: u64,
    /// Total processing cycles.
    pub cycles: u64,
    /// Last cycle duration in nanoseconds.
    pub last_cycle_duration_ns: u64,
    /// Total batches processed.
    pub total_batches: u64,
}

/// Pipeline-wide metrics snapshot.
#[derive(Debug, Clone)]
pub struct PipelineMetrics {
    /// Total events ingested across all sources.
    pub total_events_ingested: u64,
    /// Total events emitted to streams/sinks.
    pub total_events_emitted: u64,
    /// Total events dropped.
    pub total_events_dropped: u64,
    /// Total processing cycles completed.
    pub total_cycles: u64,
    /// Total batches processed.
    pub total_batches: u64,
    /// Time since the pipeline was created.
    pub uptime: Duration,
    /// Current pipeline state.
    pub state: PipelineState,
    /// Duration of the last processing cycle in nanoseconds.
    pub last_cycle_duration_ns: u64,
    /// Number of registered sources.
    pub source_count: usize,
    /// Number of registered streams.
    pub stream_count: usize,
    /// Number of registered sinks.
    pub sink_count: usize,
    /// Global pipeline watermark (minimum across all source watermarks).
    pub pipeline_watermark: i64,
}

/// Metrics for a single registered source.
#[derive(Debug, Clone)]
pub struct SourceMetrics {
    /// Source name.
    pub name: String,
    /// Total events pushed to this source (sequence number).
    pub total_events: u64,
    /// Number of events currently buffered.
    pub pending: usize,
    /// Buffer capacity.
    pub capacity: usize,
    /// Whether the source is experiencing backpressure (>80% full).
    pub is_backpressured: bool,
    /// Current watermark value.
    pub watermark: i64,
    /// Buffer utilization ratio (0.0 to 1.0).
    pub utilization: f64,
}

/// Metrics for a single registered stream.
#[derive(Debug, Clone)]
pub struct StreamMetrics {
    /// Stream name.
    pub name: String,
    /// Total events pushed to this stream.
    pub total_events: u64,
    /// Number of events currently buffered.
    pub pending: usize,
    /// Buffer capacity.
    pub capacity: usize,
    /// Whether the stream is experiencing backpressure (>80% full).
    pub is_backpressured: bool,
    /// Current watermark value.
    pub watermark: i64,
    /// SQL query that defines this stream, if any.
    pub sql: Option<String>,
}

/// Backpressure threshold: a buffer is considered backpressured when
/// its utilization exceeds this fraction.
const BACKPRESSURE_THRESHOLD: f64 = 0.8;

/// Compute whether a buffer is backpressured given pending and capacity.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub(crate) fn is_backpressured(pending: usize, capacity: usize) -> bool {
    capacity > 0 && (pending as f64 / capacity as f64) > BACKPRESSURE_THRESHOLD
}

/// Compute buffer utilization as a ratio (0.0 to 1.0).
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
    fn test_pipeline_counters_default() {
        let c = PipelineCounters::new();
        let s = c.snapshot();
        assert_eq!(s.events_ingested, 0);
        assert_eq!(s.events_emitted, 0);
        assert_eq!(s.events_dropped, 0);
        assert_eq!(s.cycles, 0);
        assert_eq!(s.total_batches, 0);
        assert_eq!(s.last_cycle_duration_ns, 0);
    }

    #[test]
    fn test_pipeline_counters_increment() {
        let c = PipelineCounters::new();
        c.events_ingested.fetch_add(100, Ordering::Relaxed);
        c.events_emitted.fetch_add(50, Ordering::Relaxed);
        c.events_dropped.fetch_add(3, Ordering::Relaxed);
        c.cycles.fetch_add(10, Ordering::Relaxed);
        c.total_batches.fetch_add(5, Ordering::Relaxed);
        c.last_cycle_duration_ns.store(1234, Ordering::Relaxed);

        let s = c.snapshot();
        assert_eq!(s.events_ingested, 100);
        assert_eq!(s.events_emitted, 50);
        assert_eq!(s.events_dropped, 3);
        assert_eq!(s.cycles, 10);
        assert_eq!(s.total_batches, 5);
        assert_eq!(s.last_cycle_duration_ns, 1234);
    }

    #[test]
    fn test_pipeline_counters_concurrent_access() {
        use std::sync::Arc;
        let c = Arc::new(PipelineCounters::new());
        let c2 = Arc::clone(&c);

        let t = std::thread::spawn(move || {
            for _ in 0..1000 {
                c2.events_ingested.fetch_add(1, Ordering::Relaxed);
            }
        });

        for _ in 0..1000 {
            c.events_ingested.fetch_add(1, Ordering::Relaxed);
        }

        t.join().unwrap();
        assert_eq!(c.events_ingested.load(Ordering::Relaxed), 2000);
    }

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
            last_cycle_duration_ns: 500,
            source_count: 2,
            stream_count: 1,
            sink_count: 1,
            pipeline_watermark: i64::MIN,
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
