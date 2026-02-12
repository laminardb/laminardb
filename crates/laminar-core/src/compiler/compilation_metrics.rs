//! Compilation metrics and observability for the JIT pipeline.
//!
//! [`CompilationMetrics`] tracks atomic counters for compiled, fallback, and
//! errored queries along with cumulative compilation time. [`CacheSnapshot`]
//! provides a point-in-time view of the compiler cache state.
//!
//! All counters use `Relaxed` ordering — these are advisory/observability
//! values, not synchronization primitives.

use std::sync::atomic::{AtomicU64, Ordering};

/// Global metrics for the JIT compilation pipeline.
///
/// All counters are atomic and use `Relaxed` ordering for minimal overhead
/// (< 5ns per increment). Intended for dashboards and diagnostics, not
/// for correctness-critical decisions.
#[derive(Debug)]
pub struct CompilationMetrics {
    /// Queries successfully compiled via JIT.
    queries_compiled: AtomicU64,
    /// Queries that fell back to `DataFusion` interpreted execution.
    queries_fallback: AtomicU64,
    /// Queries where compilation failed with an error.
    queries_error: AtomicU64,
    /// Total compilation time (nanoseconds).
    compile_time_total_ns: AtomicU64,
}

impl CompilationMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            queries_compiled: AtomicU64::new(0),
            queries_fallback: AtomicU64::new(0),
            queries_error: AtomicU64::new(0),
            compile_time_total_ns: AtomicU64::new(0),
        }
    }

    /// Records a successful JIT compilation.
    pub fn record_compiled(&self, compile_time_ns: u64) {
        self.queries_compiled.fetch_add(1, Ordering::Relaxed);
        self.compile_time_total_ns
            .fetch_add(compile_time_ns, Ordering::Relaxed);
    }

    /// Records a fallback to interpreted execution (plan not compilable).
    pub fn record_fallback(&self) {
        self.queries_fallback.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a compilation error.
    pub fn record_error(&self) {
        self.queries_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the number of successfully compiled queries.
    #[must_use]
    pub fn compiled_count(&self) -> u64 {
        self.queries_compiled.load(Ordering::Relaxed)
    }

    /// Returns the number of fallback queries.
    #[must_use]
    pub fn fallback_count(&self) -> u64 {
        self.queries_fallback.load(Ordering::Relaxed)
    }

    /// Returns the number of errored queries.
    #[must_use]
    pub fn error_count(&self) -> u64 {
        self.queries_error.load(Ordering::Relaxed)
    }

    /// Returns total compilation time in nanoseconds.
    #[must_use]
    pub fn compile_time_total_ns(&self) -> u64 {
        self.compile_time_total_ns.load(Ordering::Relaxed)
    }

    /// Returns total number of queries (compiled + fallback + error).
    #[must_use]
    pub fn total_queries(&self) -> u64 {
        self.compiled_count() + self.fallback_count() + self.error_count()
    }

    /// Takes a snapshot of all metrics.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            queries_compiled: self.compiled_count(),
            queries_fallback: self.fallback_count(),
            queries_error: self.error_count(),
            compile_time_total_ns: self.compile_time_total_ns(),
        }
    }
}

impl Default for CompilationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of [`CompilationMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Queries successfully compiled.
    pub queries_compiled: u64,
    /// Queries that fell back to interpreted.
    pub queries_fallback: u64,
    /// Queries where compilation errored.
    pub queries_error: u64,
    /// Total compilation time (nanoseconds).
    pub compile_time_total_ns: u64,
}

impl MetricsSnapshot {
    /// Returns total queries across all outcomes.
    #[must_use]
    pub fn total_queries(&self) -> u64 {
        self.queries_compiled + self.queries_fallback + self.queries_error
    }

    /// Returns the JIT compilation rate as a fraction (0.0–1.0).
    ///
    /// Returns 0.0 if no queries have been processed.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn compilation_rate(&self) -> f64 {
        let total = self.total_queries();
        if total == 0 {
            return 0.0;
        }
        self.queries_compiled as f64 / total as f64
    }
}

/// Point-in-time snapshot of the compiler cache state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheSnapshot {
    /// Number of cached entries.
    pub entries: usize,
    /// Maximum capacity.
    pub capacity: usize,
    /// Cache hits.
    pub hits: u64,
    /// Cache misses (triggered compilation).
    pub misses: u64,
    /// Cache evictions.
    pub evictions: u64,
}

impl CacheSnapshot {
    /// Returns the hit rate as a fraction (0.0–1.0).
    ///
    /// Returns 0.0 if no lookups have occurred.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            return 0.0;
        }
        self.hits as f64 / total as f64
    }

    /// Returns the fill ratio (entries / capacity).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn fill_ratio(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }
        self.entries as f64 / self.capacity as f64
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    // ── CompilationMetrics tests ─────────────────────────────────

    #[test]
    fn metrics_initial_zero() {
        let m = CompilationMetrics::new();
        assert_eq!(m.compiled_count(), 0);
        assert_eq!(m.fallback_count(), 0);
        assert_eq!(m.error_count(), 0);
        assert_eq!(m.compile_time_total_ns(), 0);
        assert_eq!(m.total_queries(), 0);
    }

    #[test]
    fn metrics_record_compiled() {
        let m = CompilationMetrics::new();
        m.record_compiled(1_000_000);
        m.record_compiled(2_000_000);
        assert_eq!(m.compiled_count(), 2);
        assert_eq!(m.compile_time_total_ns(), 3_000_000);
    }

    #[test]
    fn metrics_record_fallback() {
        let m = CompilationMetrics::new();
        m.record_fallback();
        m.record_fallback();
        m.record_fallback();
        assert_eq!(m.fallback_count(), 3);
    }

    #[test]
    fn metrics_record_error() {
        let m = CompilationMetrics::new();
        m.record_error();
        assert_eq!(m.error_count(), 1);
    }

    #[test]
    fn metrics_total_queries() {
        let m = CompilationMetrics::new();
        m.record_compiled(100);
        m.record_compiled(200);
        m.record_fallback();
        m.record_error();
        assert_eq!(m.total_queries(), 4);
    }

    #[test]
    fn metrics_default() {
        let m = CompilationMetrics::default();
        assert_eq!(m.compiled_count(), 0);
    }

    // ── MetricsSnapshot tests ────────────────────────────────────

    #[test]
    fn snapshot_captures_state() {
        let m = CompilationMetrics::new();
        m.record_compiled(500);
        m.record_fallback();

        let snap = m.snapshot();
        assert_eq!(snap.queries_compiled, 1);
        assert_eq!(snap.queries_fallback, 1);
        assert_eq!(snap.queries_error, 0);
        assert_eq!(snap.compile_time_total_ns, 500);
        assert_eq!(snap.total_queries(), 2);
    }

    #[test]
    fn snapshot_compilation_rate() {
        let snap = MetricsSnapshot {
            queries_compiled: 3,
            queries_fallback: 1,
            queries_error: 0,
            compile_time_total_ns: 0,
        };
        let rate = snap.compilation_rate();
        assert!((rate - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn snapshot_compilation_rate_zero() {
        let snap = MetricsSnapshot {
            queries_compiled: 0,
            queries_fallback: 0,
            queries_error: 0,
            compile_time_total_ns: 0,
        };
        assert_eq!(snap.compilation_rate(), 0.0);
    }

    // ── CacheSnapshot tests ──────────────────────────────────────

    #[test]
    fn cache_snapshot_hit_rate() {
        let snap = CacheSnapshot {
            entries: 10,
            capacity: 64,
            hits: 80,
            misses: 20,
            evictions: 5,
        };
        assert!((snap.hit_rate() - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn cache_snapshot_hit_rate_zero() {
        let snap = CacheSnapshot {
            entries: 0,
            capacity: 64,
            hits: 0,
            misses: 0,
            evictions: 0,
        };
        assert_eq!(snap.hit_rate(), 0.0);
    }

    #[test]
    fn cache_snapshot_fill_ratio() {
        let snap = CacheSnapshot {
            entries: 32,
            capacity: 64,
            hits: 0,
            misses: 0,
            evictions: 0,
        };
        assert!((snap.fill_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn cache_snapshot_fill_ratio_zero_capacity() {
        let snap = CacheSnapshot {
            entries: 0,
            capacity: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
        };
        assert_eq!(snap.fill_ratio(), 0.0);
    }

    #[test]
    fn cache_snapshot_debug() {
        let snap = CacheSnapshot {
            entries: 5,
            capacity: 64,
            hits: 10,
            misses: 2,
            evictions: 1,
        };
        let s = format!("{snap:?}");
        assert!(s.contains("CacheSnapshot"));
        assert!(s.contains("entries: 5"));
    }
}
