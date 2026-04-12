//! Pipeline observability metrics.

use std::sync::atomic::{AtomicU64, Ordering};
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

const CACHE_LINE_SIZE: usize = 64;

/// Shared atomic counters for the pipeline processing loop.
///
/// Ring 0 fields (hot path) and Ring 2 fields (checkpoint) are on separate
/// cache lines to prevent false sharing. All use `Ordering::Relaxed`.
#[repr(C)]
pub struct PipelineCounters {
    // Ring 0 (reactor thread, every cycle)
    /// Events ingested.
    pub events_ingested: AtomicU64,
    /// Events emitted.
    pub events_emitted: AtomicU64,
    /// Events dropped.
    pub events_dropped: AtomicU64,
    /// Processing cycles.
    pub cycles: AtomicU64,
    /// Last cycle duration (ns).
    pub last_cycle_duration_ns: AtomicU64,
    /// Batches processed.
    pub total_batches: AtomicU64,
    /// Queries using compiled `PhysicalExpr`.
    pub queries_compiled: AtomicU64,
    /// Queries using cached logical plan.
    pub queries_cached_plan: AtomicU64,

    // Pad to cache line boundary so Ring 2 starts on a new line.
    _pad: [u8; CACHE_LINE_SIZE - (8 * std::mem::size_of::<AtomicU64>()) % CACHE_LINE_SIZE],

    // Ring 2 (checkpoint coordinator, async)
    /// Checkpoints completed.
    pub checkpoints_completed: AtomicU64,
    /// Checkpoints failed.
    pub checkpoints_failed: AtomicU64,
    /// Last checkpoint duration (ms).
    pub last_checkpoint_duration_ms: AtomicU64,
    /// Checkpoint epoch.
    pub checkpoint_epoch: AtomicU64,
    /// Max state bytes per operator (0 = unlimited).
    pub max_state_bytes: AtomicU64,
    /// Cycle p50 (ns).
    pub cycle_p50_ns: AtomicU64,
    /// Cycle p95 (ns).
    pub cycle_p95_ns: AtomicU64,
    /// Cycle p99 (ns).
    pub cycle_p99_ns: AtomicU64,

    // Sink 2PC timing
    /// Last sink pre-commit (us).
    pub sink_precommit_duration_us: AtomicU64,
    /// Last sink commit (us).
    pub sink_commit_duration_us: AtomicU64,
    /// `write_batch` returned `Err` (broker, serialization, etc.).
    pub sink_write_failures: AtomicU64,
    /// `write_batch` exceeded its per-call I/O timeout.
    pub sink_write_timeouts: AtomicU64,
    /// Sink command channel was closed (sink task died).
    pub sink_task_channel_closed: AtomicU64,
    /// Cycles skipped due to backpressure.
    pub cycles_backpressured: AtomicU64,

    /// Last checkpoint size (bytes).
    pub last_checkpoint_size_bytes: AtomicU64,
    /// Last checkpoint wall-clock timestamp (ms since epoch).
    pub last_checkpoint_timestamp_ms: AtomicU64,

    /// MV update operations.
    pub mv_updates: AtomicU64,
    /// Approximate MV bytes stored.
    pub mv_bytes_stored: AtomicU64,
}

impl PipelineCounters {
    /// Zeroed counters.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events_ingested: AtomicU64::new(0),
            events_emitted: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            cycles: AtomicU64::new(0),
            last_cycle_duration_ns: AtomicU64::new(0),
            total_batches: AtomicU64::new(0),
            queries_compiled: AtomicU64::new(0),
            queries_cached_plan: AtomicU64::new(0),
            _pad: [0; CACHE_LINE_SIZE - (8 * std::mem::size_of::<AtomicU64>()) % CACHE_LINE_SIZE],
            checkpoints_completed: AtomicU64::new(0),
            checkpoints_failed: AtomicU64::new(0),
            last_checkpoint_duration_ms: AtomicU64::new(0),
            checkpoint_epoch: AtomicU64::new(0),
            max_state_bytes: AtomicU64::new(0),
            cycle_p50_ns: AtomicU64::new(0),
            cycle_p95_ns: AtomicU64::new(0),
            cycle_p99_ns: AtomicU64::new(0),
            sink_precommit_duration_us: AtomicU64::new(0),
            sink_commit_duration_us: AtomicU64::new(0),
            sink_write_failures: AtomicU64::new(0),
            sink_write_timeouts: AtomicU64::new(0),
            sink_task_channel_closed: AtomicU64::new(0),
            cycles_backpressured: AtomicU64::new(0),
            last_checkpoint_size_bytes: AtomicU64::new(0),
            last_checkpoint_timestamp_ms: AtomicU64::new(0),
            mv_updates: AtomicU64::new(0),
            mv_bytes_stored: AtomicU64::new(0),
        }
    }

    /// Relaxed-load snapshot of all counters.
    #[must_use]
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            events_ingested: self.events_ingested.load(Ordering::Relaxed),
            events_emitted: self.events_emitted.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            cycles: self.cycles.load(Ordering::Relaxed),
            last_cycle_duration_ns: self.last_cycle_duration_ns.load(Ordering::Relaxed),
            total_batches: self.total_batches.load(Ordering::Relaxed),
            queries_compiled: self.queries_compiled.load(Ordering::Relaxed),
            queries_cached_plan: self.queries_cached_plan.load(Ordering::Relaxed),
            checkpoints_completed: self.checkpoints_completed.load(Ordering::Relaxed),
            checkpoints_failed: self.checkpoints_failed.load(Ordering::Relaxed),
            last_checkpoint_duration_ms: self.last_checkpoint_duration_ms.load(Ordering::Relaxed),
            checkpoint_epoch: self.checkpoint_epoch.load(Ordering::Relaxed),
            max_state_bytes: self.max_state_bytes.load(Ordering::Relaxed),
            cycle_p50_ns: self.cycle_p50_ns.load(Ordering::Relaxed),
            cycle_p95_ns: self.cycle_p95_ns.load(Ordering::Relaxed),
            cycle_p99_ns: self.cycle_p99_ns.load(Ordering::Relaxed),
            sink_precommit_duration_us: self.sink_precommit_duration_us.load(Ordering::Relaxed),
            sink_commit_duration_us: self.sink_commit_duration_us.load(Ordering::Relaxed),
            sink_write_failures: self.sink_write_failures.load(Ordering::Relaxed),
            sink_write_timeouts: self.sink_write_timeouts.load(Ordering::Relaxed),
            sink_task_channel_closed: self.sink_task_channel_closed.load(Ordering::Relaxed),
            cycles_backpressured: self.cycles_backpressured.load(Ordering::Relaxed),
            last_checkpoint_size_bytes: self.last_checkpoint_size_bytes.load(Ordering::Relaxed),
            last_checkpoint_timestamp_ms: self.last_checkpoint_timestamp_ms.load(Ordering::Relaxed),
            mv_updates: self.mv_updates.load(Ordering::Relaxed),
            mv_bytes_stored: self.mv_bytes_stored.load(Ordering::Relaxed),
        }
    }
}

impl Default for PipelineCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of [`PipelineCounters`]. Fields mirror the atomic originals.
#[derive(Debug, Clone, Copy)]
pub struct CounterSnapshot {
    /// Events ingested.
    pub events_ingested: u64,
    /// Events emitted.
    pub events_emitted: u64,
    /// Events dropped.
    pub events_dropped: u64,
    /// Processing cycles.
    pub cycles: u64,
    /// Last cycle (ns).
    pub last_cycle_duration_ns: u64,
    /// Batches processed.
    pub total_batches: u64,
    /// Compiled-expr queries.
    pub queries_compiled: u64,
    /// Cached-plan queries.
    pub queries_cached_plan: u64,
    /// Checkpoints completed.
    pub checkpoints_completed: u64,
    /// Checkpoints failed.
    pub checkpoints_failed: u64,
    /// Last checkpoint (ms).
    pub last_checkpoint_duration_ms: u64,
    /// Checkpoint epoch.
    pub checkpoint_epoch: u64,
    /// Max state bytes (0 = unlimited).
    pub max_state_bytes: u64,
    /// Cycle p50 (ns).
    pub cycle_p50_ns: u64,
    /// Cycle p95 (ns).
    pub cycle_p95_ns: u64,
    /// Cycle p99 (ns).
    pub cycle_p99_ns: u64,
    /// Last pre-commit (us).
    pub sink_precommit_duration_us: u64,
    /// Last commit (us).
    pub sink_commit_duration_us: u64,
    /// `write_batch` returned `Err`.
    pub sink_write_failures: u64,
    /// `write_batch` exceeded its per-call I/O timeout.
    pub sink_write_timeouts: u64,
    /// Sink command channel closed (task died).
    pub sink_task_channel_closed: u64,
    /// Backpressure-skipped cycles.
    pub cycles_backpressured: u64,
    /// Last checkpoint size (bytes).
    pub last_checkpoint_size_bytes: u64,
    /// Last checkpoint timestamp (ms since epoch).
    pub last_checkpoint_timestamp_ms: u64,
    /// MV updates.
    pub mv_updates: u64,
    /// Approximate MV bytes.
    pub mv_bytes_stored: u64,
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
    /// Last cycle (ns).
    pub last_cycle_duration_ns: u64,
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
    fn test_cache_line_separation() {
        let c = PipelineCounters::new();
        let base = &raw const c as usize;
        let ring0_start = &raw const c.events_ingested as usize;
        let ring2_start = &raw const c.checkpoints_completed as usize;

        // Ring 0 starts at offset 0
        assert_eq!(ring0_start - base, 0);
        // Ring 2 starts at least 64 bytes from Ring 0
        assert!(
            ring2_start - ring0_start >= 64,
            "Ring 2 counters must be on a separate cache line (offset: {})",
            ring2_start - ring0_start
        );
    }

    #[test]
    fn test_checkpoint_counters() {
        let c = PipelineCounters::new();
        c.checkpoints_completed.fetch_add(5, Ordering::Relaxed);
        c.checkpoints_failed.fetch_add(1, Ordering::Relaxed);
        c.last_checkpoint_duration_ms.store(250, Ordering::Relaxed);
        c.checkpoint_epoch.store(10, Ordering::Relaxed);

        let s = c.snapshot();
        assert_eq!(s.checkpoints_completed, 5);
        assert_eq!(s.checkpoints_failed, 1);
        assert_eq!(s.last_checkpoint_duration_ms, 250);
        assert_eq!(s.checkpoint_epoch, 10);
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
