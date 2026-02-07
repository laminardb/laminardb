# F-OBS-001: Pipeline Observability API

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-OBS-001 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (2-3 days) |
| **Dependencies** | F-DAG-008 (Pipeline Introspection) |

## Summary

Add a pipeline observability API to `LaminarDB` exposing runtime metrics:
throughput counters, per-stream statistics, pipeline latency tracking, watermark
visibility, queue depth introspection, and backpressure state. These APIs enable
dashboards, alerting, and operational monitoring without external instrumentation.

## Motivation

The demo's `main.rs` documents 7 missing observability APIs:

1. No `db.metrics()` for system-level stats (CPU, memory, latency)
2. No pipeline latency tracking (push-to-poll end-to-end)
3. No event counters (`db.total_events_processed()`)
4. No per-stream metrics (`db.stream_stats("ohlc_bars")`)
5. No backpressure feedback (`source.is_backpressured()`)
6. No watermark visibility
7. No queue depth introspection

These gaps force application-level workarounds (`sysinfo` crate, manual counters)
and limit operational insight.

## Design

### PipelineMetrics Struct

```rust
/// Snapshot of pipeline-wide metrics.
pub struct PipelineMetrics {
    /// Total events ingested across all sources
    pub total_events_ingested: u64,
    /// Total events emitted across all sinks/subscriptions
    pub total_events_emitted: u64,
    /// Total events dropped (backpressure, errors)
    pub total_events_dropped: u64,
    /// Pipeline uptime
    pub uptime: Duration,
    /// Current pipeline state
    pub state: PipelineState,
    /// Last checkpoint epoch (None if never checkpointed)
    pub last_checkpoint_epoch: Option<u64>,
    /// Time since last checkpoint
    pub time_since_checkpoint: Option<Duration>,
}
```

### Per-Stream Metrics

```rust
/// Metrics for a single stream/materialized view.
pub struct StreamMetrics {
    /// Stream name
    pub name: String,
    /// Events processed in last second
    pub throughput_per_sec: f64,
    /// Total events processed since startup
    pub total_events: u64,
    /// Current input queue depth
    pub input_queue_depth: usize,
    /// Current output queue depth (subscribers)
    pub output_queue_depth: usize,
    /// Whether input is backpressured
    pub is_backpressured: bool,
    /// Current watermark position (event time ms)
    pub watermark: Option<i64>,
    /// Average processing latency per event (ns)
    pub avg_latency_ns: u64,
    /// p99 processing latency per event (ns)
    pub p99_latency_ns: u64,
}
```

### Per-Source Metrics

```rust
/// Metrics for a source connector.
pub struct SourceMetrics {
    pub name: String,
    /// Events ingested since startup
    pub total_events: u64,
    /// Current pending (buffered) events
    pub pending: usize,
    /// Whether source is backpressured
    pub is_backpressured: bool,
    /// Current watermark
    pub watermark: i64,
    /// Kafka-specific: consumer lag per partition
    pub consumer_lag: Option<HashMap<i32, i64>>,
}
```

### API Surface on LaminarDB

```rust
impl LaminarDB {
    /// Pipeline-wide metrics snapshot.
    pub fn metrics(&self) -> PipelineMetrics;

    /// Metrics for a specific stream.
    pub fn stream_metrics(&self, name: &str) -> Option<StreamMetrics>;

    /// Metrics for all streams.
    pub fn all_stream_metrics(&self) -> Vec<StreamMetrics>;

    /// Metrics for a specific source.
    pub fn source_metrics(&self, name: &str) -> Option<SourceMetrics>;

    /// Total events processed across all streams.
    pub fn total_events_processed(&self) -> u64;

    /// Pipeline end-to-end latency (push to subscribe delivery).
    pub fn pipeline_latency(&self) -> LatencyStats;
}

pub struct LatencyStats {
    pub avg_ns: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub sample_count: u64,
}
```

### Implementation: Atomic Counters

Use `AtomicU64` counters in the pipeline executor, updated on each cycle:

```rust
pub struct PipelineCounters {
    pub events_ingested: AtomicU64,
    pub events_emitted: AtomicU64,
    pub events_dropped: AtomicU64,
    pub cycles: AtomicU64,
}
```

Shared via `Arc<PipelineCounters>` between the pipeline thread and the
`LaminarDB` API methods. Reads are lock-free (`Ordering::Relaxed`).

### Implementation: Latency Tracking

Use a lightweight HDR histogram (or simple ring buffer of recent latencies)
updated per pipeline cycle:

```rust
struct LatencyTracker {
    /// Ring buffer of recent latencies (ns)
    samples: VecDeque<u64>,
    /// Maximum samples to retain
    max_samples: usize,
}
```

Latency measured as: `Instant::now() - cycle_start` for each pipeline cycle.
For per-event latency, embed a timestamp in the event metadata and compare
on emission.

### Implementation: Watermark Visibility

Expose the watermark tracker from the streaming engine:

```rust
impl LaminarDB {
    pub fn watermark(&self, source_name: &str) -> Option<i64> {
        self.catalog.get_source(source_name)?.current_watermark()
    }
}
```

### Implementation: Backpressure State

```rust
impl SourceHandle<T> {
    /// Returns true if the source's input buffer is above the high watermark.
    pub fn is_backpressured(&self) -> bool {
        let depth = self.pending();
        let capacity = self.capacity();
        depth as f64 / capacity as f64 > 0.8
    }
}
```

### Thread Safety

All metric types are `Send + Sync`. Atomic counters use `Ordering::Relaxed`
for reads (metrics are advisory, not transactional). The `metrics()` method
returns a snapshot struct (no references held).

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `pipeline_metrics` | 5 | Counters increment, snapshot reads, reset on startup |
| `stream_metrics` | 5 | Per-stream throughput, queue depth, backpressure detection |
| `source_metrics` | 3 | Source counters, watermark visibility, pending count |
| `latency_tracking` | 4 | Histogram accuracy, ring buffer rollover, percentile calculation |
| `integration` | 3 | Full pipeline: push events, verify metrics reflect state |

## Files

- `crates/laminar-db/src/metrics.rs` — NEW: PipelineMetrics, StreamMetrics, SourceMetrics, LatencyStats
- `crates/laminar-db/src/db.rs` — API methods: metrics(), stream_metrics(), etc.
- `crates/laminar-db/src/handle.rs` — SourceHandle: is_backpressured(), capacity()
- `crates/laminar-db/src/lib.rs` — Re-export metric types
- `crates/laminar-core/src/streaming/channel.rs` — Expose queue capacity
