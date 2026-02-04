//! Statistics tracking for three-ring I/O.

use std::time::Duration;

/// Statistics for three-ring operation.
///
/// Tracks completions, sleeps, wake-ups, and latency for each ring.
#[derive(Debug, Default, Clone)]
pub struct ThreeRingStats {
    /// Completions from the latency ring.
    pub latency_completions: u64,
    /// Completions from the main ring.
    pub main_completions: u64,
    /// Completions from the poll ring.
    pub poll_completions: u64,

    /// Number of times the main ring blocked waiting for I/O.
    pub main_ring_sleeps: u64,
    /// Number of times main ring was woken by latency ring activity.
    pub latency_wake_ups: u64,

    /// Submissions to latency ring.
    pub latency_submissions: u64,
    /// Submissions to main ring.
    pub main_submissions: u64,
    /// Submissions to poll ring.
    pub poll_submissions: u64,

    /// Latency ring total latency in nanoseconds.
    pub latency_ring_total_ns: u64,
    /// Number of latency ring samples.
    pub latency_ring_samples: u64,
    /// Minimum latency ring latency in nanoseconds.
    pub latency_ring_min_ns: u64,
    /// Maximum latency ring latency in nanoseconds.
    pub latency_ring_max_ns: u64,

    /// Main ring total latency in nanoseconds.
    pub main_ring_total_ns: u64,
    /// Number of main ring samples.
    pub main_ring_samples: u64,
    /// Minimum main ring latency in nanoseconds.
    pub main_ring_min_ns: u64,
    /// Maximum main ring latency in nanoseconds.
    pub main_ring_max_ns: u64,

    /// Poll ring total latency in nanoseconds.
    pub poll_ring_total_ns: u64,
    /// Number of poll ring samples.
    pub poll_ring_samples: u64,
    /// Minimum poll ring latency in nanoseconds.
    pub poll_ring_min_ns: u64,
    /// Maximum poll ring latency in nanoseconds.
    pub poll_ring_max_ns: u64,

    /// Errors on latency ring.
    pub latency_errors: u64,
    /// Errors on main ring.
    pub main_errors: u64,
    /// Errors on poll ring.
    pub poll_errors: u64,

    /// Poll fallbacks (poll ring ops that went to main ring).
    pub poll_fallbacks: u64,
}

impl ThreeRingStats {
    /// Create new stats instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            latency_ring_min_ns: u64::MAX,
            main_ring_min_ns: u64::MAX,
            poll_ring_min_ns: u64::MAX,
            ..Default::default()
        }
    }

    /// Record a latency ring completion.
    #[allow(clippy::cast_possible_truncation)]
    pub fn record_latency_completion(&mut self, latency: Option<Duration>, success: bool) {
        self.latency_completions += 1;
        if !success {
            self.latency_errors += 1;
        }
        if let Some(lat) = latency {
            let ns = lat.as_nanos() as u64;
            self.latency_ring_total_ns += ns;
            self.latency_ring_samples += 1;
            self.latency_ring_min_ns = self.latency_ring_min_ns.min(ns);
            self.latency_ring_max_ns = self.latency_ring_max_ns.max(ns);
        }
    }

    /// Record a main ring completion.
    #[allow(clippy::cast_possible_truncation)]
    pub fn record_main_completion(&mut self, latency: Option<Duration>, success: bool) {
        self.main_completions += 1;
        if !success {
            self.main_errors += 1;
        }
        if let Some(lat) = latency {
            let ns = lat.as_nanos() as u64;
            self.main_ring_total_ns += ns;
            self.main_ring_samples += 1;
            self.main_ring_min_ns = self.main_ring_min_ns.min(ns);
            self.main_ring_max_ns = self.main_ring_max_ns.max(ns);
        }
    }

    /// Record a poll ring completion.
    #[allow(clippy::cast_possible_truncation)]
    pub fn record_poll_completion(&mut self, latency: Option<Duration>, success: bool) {
        self.poll_completions += 1;
        if !success {
            self.poll_errors += 1;
        }
        if let Some(lat) = latency {
            let ns = lat.as_nanos() as u64;
            self.poll_ring_total_ns += ns;
            self.poll_ring_samples += 1;
            self.poll_ring_min_ns = self.poll_ring_min_ns.min(ns);
            self.poll_ring_max_ns = self.poll_ring_max_ns.max(ns);
        }
    }

    /// Record a main ring sleep.
    pub fn record_sleep(&mut self) {
        self.main_ring_sleeps += 1;
    }

    /// Record a wake-up from latency ring.
    pub fn record_latency_wake_up(&mut self) {
        self.latency_wake_ups += 1;
    }

    /// Record a poll ring fallback to main ring.
    pub fn record_poll_fallback(&mut self) {
        self.poll_fallbacks += 1;
    }

    /// Get total completions across all rings.
    #[must_use]
    pub const fn total_completions(&self) -> u64 {
        self.latency_completions + self.main_completions + self.poll_completions
    }

    /// Get total submissions across all rings.
    #[must_use]
    pub const fn total_submissions(&self) -> u64 {
        self.latency_submissions + self.main_submissions + self.poll_submissions
    }

    /// Get total errors across all rings.
    #[must_use]
    pub const fn total_errors(&self) -> u64 {
        self.latency_errors + self.main_errors + self.poll_errors
    }

    /// Get average latency ring latency in nanoseconds.
    #[must_use]
    pub fn latency_ring_avg_ns(&self) -> u64 {
        if self.latency_ring_samples > 0 {
            self.latency_ring_total_ns / self.latency_ring_samples
        } else {
            0
        }
    }

    /// Get average main ring latency in nanoseconds.
    #[must_use]
    pub fn main_ring_avg_ns(&self) -> u64 {
        if self.main_ring_samples > 0 {
            self.main_ring_total_ns / self.main_ring_samples
        } else {
            0
        }
    }

    /// Get average poll ring latency in nanoseconds.
    #[must_use]
    pub fn poll_ring_avg_ns(&self) -> u64 {
        if self.poll_ring_samples > 0 {
            self.poll_ring_total_ns / self.poll_ring_samples
        } else {
            0
        }
    }

    /// Get wake-up efficiency (wake-ups / sleeps).
    ///
    /// Higher ratio means latency ring is effectively waking the main ring.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn wake_up_efficiency(&self) -> f64 {
        if self.main_ring_sleeps > 0 {
            self.latency_wake_ups as f64 / self.main_ring_sleeps as f64
        } else {
            0.0
        }
    }

    /// Get success rate across all rings.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn success_rate(&self) -> f64 {
        let total = self.total_completions();
        if total > 0 {
            (total - self.total_errors()) as f64 / total as f64
        } else {
            1.0
        }
    }

    /// Reset all statistics.
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Merge another stats instance into this one.
    pub fn merge(&mut self, other: &Self) {
        self.latency_completions += other.latency_completions;
        self.main_completions += other.main_completions;
        self.poll_completions += other.poll_completions;

        self.main_ring_sleeps += other.main_ring_sleeps;
        self.latency_wake_ups += other.latency_wake_ups;

        self.latency_submissions += other.latency_submissions;
        self.main_submissions += other.main_submissions;
        self.poll_submissions += other.poll_submissions;

        self.latency_ring_total_ns += other.latency_ring_total_ns;
        self.latency_ring_samples += other.latency_ring_samples;
        self.latency_ring_min_ns = self.latency_ring_min_ns.min(other.latency_ring_min_ns);
        self.latency_ring_max_ns = self.latency_ring_max_ns.max(other.latency_ring_max_ns);

        self.main_ring_total_ns += other.main_ring_total_ns;
        self.main_ring_samples += other.main_ring_samples;
        self.main_ring_min_ns = self.main_ring_min_ns.min(other.main_ring_min_ns);
        self.main_ring_max_ns = self.main_ring_max_ns.max(other.main_ring_max_ns);

        self.poll_ring_total_ns += other.poll_ring_total_ns;
        self.poll_ring_samples += other.poll_ring_samples;
        self.poll_ring_min_ns = self.poll_ring_min_ns.min(other.poll_ring_min_ns);
        self.poll_ring_max_ns = self.poll_ring_max_ns.max(other.poll_ring_max_ns);

        self.latency_errors += other.latency_errors;
        self.main_errors += other.main_errors;
        self.poll_errors += other.poll_errors;

        self.poll_fallbacks += other.poll_fallbacks;
    }
}

impl std::fmt::Display for ThreeRingStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Three-Ring I/O Statistics:")?;
        writeln!(
            f,
            "  Latency Ring: {} completions, {} submissions, avg {}ns",
            self.latency_completions,
            self.latency_submissions,
            self.latency_ring_avg_ns()
        )?;
        writeln!(
            f,
            "  Main Ring:    {} completions, {} submissions, avg {}ns",
            self.main_completions,
            self.main_submissions,
            self.main_ring_avg_ns()
        )?;
        writeln!(
            f,
            "  Poll Ring:    {} completions, {} submissions, avg {}ns",
            self.poll_completions,
            self.poll_submissions,
            self.poll_ring_avg_ns()
        )?;
        writeln!(
            f,
            "  Sleeps: {}, Wake-ups: {}, Efficiency: {:.2}",
            self.main_ring_sleeps,
            self.latency_wake_ups,
            self.wake_up_efficiency()
        )?;
        writeln!(
            f,
            "  Errors: {} ({:.2}% success rate)",
            self.total_errors(),
            self.success_rate() * 100.0
        )?;
        if self.poll_fallbacks > 0 {
            writeln!(f, "  Poll Fallbacks: {}", self.poll_fallbacks)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats() {
        let stats = ThreeRingStats::new();
        assert_eq!(stats.latency_completions, 0);
        assert_eq!(stats.main_completions, 0);
        assert_eq!(stats.poll_completions, 0);
        assert_eq!(stats.latency_ring_min_ns, u64::MAX);
    }

    #[test]
    fn test_record_latency_completion() {
        let mut stats = ThreeRingStats::new();
        stats.record_latency_completion(Some(Duration::from_nanos(1000)), true);
        stats.record_latency_completion(Some(Duration::from_nanos(2000)), true);
        stats.record_latency_completion(Some(Duration::from_nanos(500)), false);

        assert_eq!(stats.latency_completions, 3);
        assert_eq!(stats.latency_errors, 1);
        assert_eq!(stats.latency_ring_samples, 3);
        assert_eq!(stats.latency_ring_avg_ns(), 1166); // (1000 + 2000 + 500) / 3
        assert_eq!(stats.latency_ring_min_ns, 500);
        assert_eq!(stats.latency_ring_max_ns, 2000);
    }

    #[test]
    fn test_totals() {
        let mut stats = ThreeRingStats::new();
        stats.latency_completions = 10;
        stats.main_completions = 20;
        stats.poll_completions = 5;
        stats.latency_submissions = 12;
        stats.main_submissions = 22;
        stats.poll_submissions = 6;
        stats.latency_errors = 1;
        stats.main_errors = 2;

        assert_eq!(stats.total_completions(), 35);
        assert_eq!(stats.total_submissions(), 40);
        assert_eq!(stats.total_errors(), 3);
    }

    #[test]
    fn test_wake_up_efficiency() {
        let mut stats = ThreeRingStats::new();
        stats.main_ring_sleeps = 10;
        stats.latency_wake_ups = 8;

        assert!((stats.wake_up_efficiency() - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_success_rate() {
        let mut stats = ThreeRingStats::new();
        stats.latency_completions = 100;
        stats.latency_errors = 5;

        assert!((stats.success_rate() - 0.95).abs() < 0.001);
    }

    #[test]
    fn test_merge() {
        let mut stats1 = ThreeRingStats::new();
        stats1.latency_completions = 10;
        stats1.main_completions = 20;
        stats1.latency_ring_min_ns = 100;
        stats1.latency_ring_max_ns = 1000;

        let mut stats2 = ThreeRingStats::new();
        stats2.latency_completions = 5;
        stats2.main_completions = 10;
        stats2.latency_ring_min_ns = 50;
        stats2.latency_ring_max_ns = 500;

        stats1.merge(&stats2);

        assert_eq!(stats1.latency_completions, 15);
        assert_eq!(stats1.main_completions, 30);
        assert_eq!(stats1.latency_ring_min_ns, 50);
        assert_eq!(stats1.latency_ring_max_ns, 1000);
    }

    #[test]
    fn test_reset() {
        let mut stats = ThreeRingStats::new();
        stats.latency_completions = 100;
        stats.main_completions = 200;
        stats.reset();

        assert_eq!(stats.latency_completions, 0);
        assert_eq!(stats.main_completions, 0);
        assert_eq!(stats.latency_ring_min_ns, u64::MAX);
    }

    #[test]
    fn test_display() {
        let mut stats = ThreeRingStats::new();
        stats.latency_completions = 100;
        stats.main_completions = 200;
        stats.poll_completions = 50;

        let output = format!("{stats}");
        assert!(output.contains("Three-Ring"));
        assert!(output.contains("100"));
        assert!(output.contains("200"));
        assert!(output.contains("50"));
    }
}
