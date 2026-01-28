//! XDP statistics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics collected by the XDP program.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct XdpStats {
    /// Packets dropped (invalid protocol, malformed).
    pub dropped: u64,
    /// Packets passed to kernel stack.
    pub passed: u64,
    /// Packets redirected to specific CPUs.
    pub redirected: u64,
    /// Invalid packets (too short, bad header).
    pub invalid: u64,
    /// Total bytes processed.
    pub bytes_processed: u64,
}

impl XdpStats {
    /// Returns the total number of packets processed.
    #[must_use]
    pub const fn total_packets(&self) -> u64 {
        self.dropped + self.passed + self.redirected + self.invalid
    }

    /// Returns the drop rate as a percentage (0.0 to 100.0).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn drop_rate(&self) -> f64 {
        let total = self.total_packets();
        if total == 0 {
            return 0.0;
        }
        (self.dropped as f64 / total as f64) * 100.0
    }

    /// Returns the redirect rate as a percentage (0.0 to 100.0).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn redirect_rate(&self) -> f64 {
        let total = self.total_packets();
        if total == 0 {
            return 0.0;
        }
        (self.redirected as f64 / total as f64) * 100.0
    }

    /// Merges stats from another instance.
    pub fn merge(&mut self, other: &XdpStats) {
        self.dropped += other.dropped;
        self.passed += other.passed;
        self.redirected += other.redirected;
        self.invalid += other.invalid;
        self.bytes_processed += other.bytes_processed;
    }
}

impl std::fmt::Display for XdpStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "XdpStats {{ dropped: {}, passed: {}, redirected: {}, invalid: {}, bytes: {} }}",
            self.dropped, self.passed, self.redirected, self.invalid, self.bytes_processed
        )
    }
}

/// Atomic version of XDP stats for concurrent updates.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct AtomicXdpStats {
    dropped: AtomicU64,
    passed: AtomicU64,
    redirected: AtomicU64,
    invalid: AtomicU64,
    bytes_processed: AtomicU64,
}

#[allow(dead_code)]
impl AtomicXdpStats {
    /// Creates new atomic stats.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            dropped: AtomicU64::new(0),
            passed: AtomicU64::new(0),
            redirected: AtomicU64::new(0),
            invalid: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
        }
    }

    /// Increments the dropped counter.
    pub fn inc_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the passed counter.
    pub fn inc_passed(&self) {
        self.passed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the redirected counter.
    pub fn inc_redirected(&self) {
        self.redirected.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the invalid counter.
    pub fn inc_invalid(&self) {
        self.invalid.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds to the bytes processed counter.
    pub fn add_bytes(&self, bytes: u64) {
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Returns a snapshot of the current stats.
    #[must_use]
    pub fn snapshot(&self) -> XdpStats {
        XdpStats {
            dropped: self.dropped.load(Ordering::Relaxed),
            passed: self.passed.load(Ordering::Relaxed),
            redirected: self.redirected.load(Ordering::Relaxed),
            invalid: self.invalid.load(Ordering::Relaxed),
            bytes_processed: self.bytes_processed.load(Ordering::Relaxed),
        }
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        self.dropped.store(0, Ordering::Relaxed);
        self.passed.store(0, Ordering::Relaxed);
        self.redirected.store(0, Ordering::Relaxed);
        self.invalid.store(0, Ordering::Relaxed);
        self.bytes_processed.store(0, Ordering::Relaxed);
    }
}

/// Per-CPU statistics.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct PerCpuStats {
    /// Stats per CPU.
    pub cpu_stats: Vec<XdpStats>,
}

#[allow(dead_code)]
impl PerCpuStats {
    /// Creates new per-CPU stats for the given number of CPUs.
    #[must_use]
    pub fn new(num_cpus: usize) -> Self {
        Self {
            cpu_stats: vec![XdpStats::default(); num_cpus],
        }
    }

    /// Returns aggregate stats across all CPUs.
    #[must_use]
    pub fn aggregate(&self) -> XdpStats {
        let mut total = XdpStats::default();
        for stats in &self.cpu_stats {
            total.merge(stats);
        }
        total
    }

    /// Returns stats for a specific CPU.
    #[must_use]
    pub fn get(&self, cpu: usize) -> Option<&XdpStats> {
        self.cpu_stats.get(cpu)
    }

    /// Returns the number of CPUs.
    #[must_use]
    pub fn num_cpus(&self) -> usize {
        self.cpu_stats.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdp_stats_default() {
        let stats = XdpStats::default();
        assert_eq!(stats.dropped, 0);
        assert_eq!(stats.total_packets(), 0);
    }

    #[test]
    fn test_xdp_stats_total() {
        let stats = XdpStats {
            dropped: 10,
            passed: 20,
            redirected: 30,
            invalid: 5,
            bytes_processed: 1000,
        };
        assert_eq!(stats.total_packets(), 65);
    }

    #[test]
    fn test_xdp_stats_rates() {
        let stats = XdpStats {
            dropped: 25,
            passed: 50,
            redirected: 25,
            invalid: 0,
            bytes_processed: 0,
        };
        assert!((stats.drop_rate() - 25.0).abs() < 0.01);
        assert!((stats.redirect_rate() - 25.0).abs() < 0.01);
    }

    #[test]
    fn test_xdp_stats_rates_zero_total() {
        let stats = XdpStats::default();
        assert_eq!(stats.drop_rate(), 0.0);
        assert_eq!(stats.redirect_rate(), 0.0);
    }

    #[test]
    fn test_xdp_stats_merge() {
        let mut stats1 = XdpStats {
            dropped: 10,
            passed: 20,
            redirected: 30,
            invalid: 5,
            bytes_processed: 1000,
        };
        let stats2 = XdpStats {
            dropped: 5,
            passed: 10,
            redirected: 15,
            invalid: 2,
            bytes_processed: 500,
        };
        stats1.merge(&stats2);

        assert_eq!(stats1.dropped, 15);
        assert_eq!(stats1.passed, 30);
        assert_eq!(stats1.redirected, 45);
        assert_eq!(stats1.invalid, 7);
        assert_eq!(stats1.bytes_processed, 1500);
    }

    #[test]
    fn test_xdp_stats_display() {
        let stats = XdpStats {
            dropped: 1,
            passed: 2,
            redirected: 3,
            invalid: 4,
            bytes_processed: 5,
        };
        let display = format!("{stats}");
        assert!(display.contains("dropped: 1"));
        assert!(display.contains("redirected: 3"));
    }

    #[test]
    fn test_atomic_stats() {
        let stats = AtomicXdpStats::new();

        stats.inc_dropped();
        stats.inc_dropped();
        stats.inc_passed();
        stats.inc_redirected();
        stats.inc_invalid();
        stats.add_bytes(100);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.dropped, 2);
        assert_eq!(snapshot.passed, 1);
        assert_eq!(snapshot.redirected, 1);
        assert_eq!(snapshot.invalid, 1);
        assert_eq!(snapshot.bytes_processed, 100);
    }

    #[test]
    fn test_atomic_stats_reset() {
        let stats = AtomicXdpStats::new();
        stats.inc_dropped();
        stats.reset();
        assert_eq!(stats.snapshot().dropped, 0);
    }

    #[test]
    fn test_per_cpu_stats() {
        let mut per_cpu = PerCpuStats::new(4);

        per_cpu.cpu_stats[0].dropped = 10;
        per_cpu.cpu_stats[1].dropped = 20;
        per_cpu.cpu_stats[2].redirected = 30;
        per_cpu.cpu_stats[3].passed = 40;

        let aggregate = per_cpu.aggregate();
        assert_eq!(aggregate.dropped, 30);
        assert_eq!(aggregate.redirected, 30);
        assert_eq!(aggregate.passed, 40);

        assert_eq!(per_cpu.get(0).unwrap().dropped, 10);
        assert!(per_cpu.get(10).is_none());
        assert_eq!(per_cpu.num_cpus(), 4);
    }
}
