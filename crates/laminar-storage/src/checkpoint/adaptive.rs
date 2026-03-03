//! Adaptive checkpoint interval tuning.
//!
//! Dynamically adjusts the checkpoint interval based on observed checkpoint
//! costs and changelog throughput. The goal is to keep estimated recovery time
//! within a configurable target while avoiding excessively frequent checkpoints.
//!
//! ## Algorithm
//!
//! After each checkpoint the tuner recomputes the interval:
//!
//! ```text
//! ideal_interval = target_recovery_time − last_checkpoint_duration
//! interval       = clamp(ideal_interval, min_interval, max_interval)
//! ```
//!
//! The model assumes `recovery_time ≈ checkpoint_restore_time + interval`
//! (i.e., replay throughput roughly matches ingest throughput). An exponential
//! moving average (EMA) of the changelog rate is maintained for metrics.

use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Knobs for the adaptive checkpoint interval tuner.
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// Target upper bound for estimated recovery time.
    pub target_recovery_time: Duration,

    /// Minimum checkpoint interval (floor).
    pub min_interval: Duration,

    /// Maximum checkpoint interval (ceiling).
    pub max_interval: Duration,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            target_recovery_time: Duration::from_secs(10),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(300),
        }
    }
}

// ---------------------------------------------------------------------------
// Tuner
// ---------------------------------------------------------------------------

/// After each checkpoint completes, call [`record_checkpoint()`](Self::record_checkpoint)
/// with the checkpoint duration and the number of changelog bytes accumulated
/// since the previous checkpoint. The tuner then recomputes the recommended
/// interval via [`current_interval()`](Self::current_interval).
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use laminar_storage::checkpoint::adaptive::{AdaptiveCheckpointer, AdaptiveConfig};
///
/// let config = AdaptiveConfig {
///     target_recovery_time: Duration::from_secs(10),
///     min_interval: Duration::from_secs(1),
///     max_interval: Duration::from_secs(300),
/// };
/// let mut tuner = AdaptiveCheckpointer::new(config);
///
/// // After each checkpoint finishes:
/// tuner.record_checkpoint(Duration::from_millis(200), 1_000_000);
/// let next_interval = tuner.current_interval();
/// ```
pub struct AdaptiveCheckpointer {
    config: AdaptiveConfig,
    current_interval: Duration,
    last_checkpoint_duration: Duration,
    last_checkpoint_time: Option<Instant>,
    changelog_rate_ema: f64,
    /// EMA smoothing factor (0..1). Higher = more weight on recent samples.
    ema_alpha: f64,
}

impl AdaptiveCheckpointer {
    /// Creates a new adaptive checkpointer.
    ///
    /// The initial interval is set to half the target recovery time,
    /// clamped to `[min_interval, max_interval]`.
    #[must_use]
    pub fn new(config: AdaptiveConfig) -> Self {
        let initial = config.target_recovery_time / 2;
        let current_interval = clamp_duration(initial, config.min_interval, config.max_interval);
        Self {
            config,
            current_interval,
            last_checkpoint_duration: Duration::ZERO,
            last_checkpoint_time: None,
            changelog_rate_ema: 0.0,
            ema_alpha: 0.3,
        }
    }

    /// Record the completion of a checkpoint.
    ///
    /// Updates the changelog rate EMA and recomputes the recommended interval.
    ///
    /// # Arguments
    ///
    /// * `duration` — wall-clock time the checkpoint save took
    /// * `changelog_bytes` — bytes of state changes since the previous checkpoint
    pub fn record_checkpoint(&mut self, duration: Duration, changelog_bytes: u64) {
        let now = Instant::now();

        if let Some(last_time) = self.last_checkpoint_time {
            let elapsed_secs = now.duration_since(last_time).as_secs_f64();
            if elapsed_secs > 0.0 {
                #[allow(clippy::cast_precision_loss)]
                // changelog_bytes fits in f64 mantissa for practical sizes
                let rate = changelog_bytes as f64 / elapsed_secs;
                if self.changelog_rate_ema == 0.0 {
                    self.changelog_rate_ema = rate;
                } else {
                    self.changelog_rate_ema = self
                        .ema_alpha
                        .mul_add(rate, (1.0 - self.ema_alpha) * self.changelog_rate_ema);
                }
            }
        }

        self.last_checkpoint_duration = duration;
        self.last_checkpoint_time = Some(now);
        self.current_interval = self.compute_interval();
    }

    /// Returns the current recommended checkpoint interval.
    #[must_use]
    pub fn current_interval(&self) -> Duration {
        self.current_interval
    }

    /// Returns the estimated recovery time at the current interval.
    ///
    /// `recovery_estimate = last_checkpoint_duration + current_interval`
    #[must_use]
    pub fn recovery_estimate(&self) -> Duration {
        self.last_checkpoint_duration + self.current_interval
    }

    /// Returns the current checkpoint interval in milliseconds (for metrics).
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // millis fits u64 for any practical duration
    pub fn interval_ms(&self) -> u64 {
        self.current_interval.as_millis() as u64
    }

    /// Returns the estimated recovery time in milliseconds (for metrics).
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // millis fits u64 for any practical duration
    pub fn recovery_estimate_ms(&self) -> u64 {
        self.recovery_estimate().as_millis() as u64
    }

    /// Returns the changelog rate in bytes per second (EMA).
    #[must_use]
    pub fn changelog_rate_bytes_per_sec(&self) -> f64 {
        self.changelog_rate_ema
    }

    /// Recompute the interval from the current state.
    fn compute_interval(&self) -> Duration {
        let target = self.config.target_recovery_time;
        let overhead = self.last_checkpoint_duration;

        let ideal = target.saturating_sub(overhead);
        clamp_duration(ideal, self.config.min_interval, self.config.max_interval)
    }
}

/// Clamp a `Duration` to `[min, max]`.
fn clamp_duration(d: Duration, min: Duration, max: Duration) -> Duration {
    if d < min {
        min
    } else if d > max {
        max
    } else {
        d
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn config_10s() -> AdaptiveConfig {
        AdaptiveConfig {
            target_recovery_time: Duration::from_secs(10),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(300),
        }
    }

    #[test]
    fn initial_interval_is_half_target() {
        let tuner = AdaptiveCheckpointer::new(config_10s());
        assert_eq!(tuner.current_interval(), Duration::from_secs(5));
    }

    #[test]
    fn initial_interval_clamped_to_min() {
        let config = AdaptiveConfig {
            target_recovery_time: Duration::from_millis(500),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(300),
        };
        let tuner = AdaptiveCheckpointer::new(config);
        assert_eq!(tuner.current_interval(), Duration::from_secs(1));
    }

    #[test]
    fn initial_interval_clamped_to_max() {
        let config = AdaptiveConfig {
            target_recovery_time: Duration::from_secs(1200),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(300),
        };
        let tuner = AdaptiveCheckpointer::new(config);
        assert_eq!(tuner.current_interval(), Duration::from_secs(300));
    }

    #[test]
    fn fast_checkpoint_yields_longer_interval() {
        let mut tuner = AdaptiveCheckpointer::new(config_10s());
        // Checkpoint takes only 200ms → ideal = 10s - 0.2s = 9.8s
        tuner.record_checkpoint(Duration::from_millis(200), 1_000_000);
        assert_eq!(tuner.current_interval(), Duration::from_millis(9800));
    }

    #[test]
    fn slow_checkpoint_yields_shorter_interval() {
        let mut tuner = AdaptiveCheckpointer::new(config_10s());
        // Checkpoint takes 8s → ideal = 10s - 8s = 2s
        tuner.record_checkpoint(Duration::from_secs(8), 50_000_000);
        assert_eq!(tuner.current_interval(), Duration::from_secs(2));
    }

    #[test]
    fn overhead_exceeding_target_clamps_to_min() {
        let mut tuner = AdaptiveCheckpointer::new(config_10s());
        // Checkpoint takes 15s > target 10s → saturating_sub = 0 → clamp to min 1s
        tuner.record_checkpoint(Duration::from_secs(15), 100_000_000);
        assert_eq!(tuner.current_interval(), Duration::from_secs(1));
    }

    #[test]
    fn interval_never_below_min() {
        let config = AdaptiveConfig {
            target_recovery_time: Duration::from_secs(2),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(300),
        };
        let mut tuner = AdaptiveCheckpointer::new(config);
        tuner.record_checkpoint(Duration::from_millis(1800), 10_000);
        // ideal = 2s - 1.8s = 0.2s → clamped to min 1s
        assert_eq!(tuner.current_interval(), Duration::from_secs(1));
    }

    #[test]
    fn interval_never_above_max() {
        let config = AdaptiveConfig {
            target_recovery_time: Duration::from_secs(600),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(60),
        };
        let mut tuner = AdaptiveCheckpointer::new(config);
        tuner.record_checkpoint(Duration::from_millis(100), 1000);
        // ideal = 600s - 0.1s ≈ 600s → clamped to max 60s
        assert_eq!(tuner.current_interval(), Duration::from_secs(60));
    }

    #[test]
    fn recovery_estimate_equals_overhead_plus_interval() {
        let mut tuner = AdaptiveCheckpointer::new(config_10s());
        tuner.record_checkpoint(Duration::from_secs(2), 1_000_000);
        // interval = 10 - 2 = 8s, estimate = 2 + 8 = 10s
        assert_eq!(tuner.recovery_estimate(), Duration::from_secs(10));
    }

    #[test]
    fn metrics_accessors_return_correct_values() {
        let mut tuner = AdaptiveCheckpointer::new(config_10s());
        tuner.record_checkpoint(Duration::from_millis(500), 2_000_000);
        assert_eq!(tuner.interval_ms(), 9500);
        assert_eq!(tuner.recovery_estimate_ms(), 10_000);
    }

    #[test]
    fn changelog_rate_ema_initialized_on_second_checkpoint() {
        let mut tuner = AdaptiveCheckpointer::new(config_10s());
        // First checkpoint — no previous time, rate stays 0
        tuner.record_checkpoint(Duration::from_millis(100), 0);
        assert!(tuner.changelog_rate_bytes_per_sec().abs() < f64::EPSILON);

        // Simulate passage of time by advancing last_checkpoint_time
        tuner.last_checkpoint_time = Instant::now().checked_sub(Duration::from_secs(5));

        // Second checkpoint — now we have a previous time
        tuner.record_checkpoint(Duration::from_millis(100), 5_000_000);
        // Rate ≈ 5_000_000 / 5 = 1_000_000 bytes/sec (approximately)
        let rate = tuner.changelog_rate_bytes_per_sec();
        assert!(rate > 900_000.0 && rate < 1_100_000.0, "rate was {rate}");
    }

    #[test]
    fn changelog_rate_ema_smooths_over_multiple_checkpoints() {
        let mut tuner = AdaptiveCheckpointer::new(config_10s());

        // First checkpoint
        tuner.record_checkpoint(Duration::from_millis(100), 0);

        // High-rate checkpoint
        tuner.last_checkpoint_time = Instant::now().checked_sub(Duration::from_secs(1));
        tuner.record_checkpoint(Duration::from_millis(100), 10_000_000);
        let rate_after_high = tuner.changelog_rate_bytes_per_sec();

        // Low-rate checkpoint — EMA should decrease but not jump to the new rate
        tuner.last_checkpoint_time = Instant::now().checked_sub(Duration::from_secs(1));
        tuner.record_checkpoint(Duration::from_millis(100), 100_000);
        let rate_after_low = tuner.changelog_rate_bytes_per_sec();

        assert!(
            rate_after_low < rate_after_high,
            "EMA should decrease: {rate_after_low} < {rate_after_high}"
        );
        // With alpha=0.3, the EMA shouldn't drop all the way to the new rate
        assert!(
            rate_after_low > 100_000.0,
            "EMA should be smoothed: {rate_after_low} > 100_000"
        );
    }

    #[test]
    fn default_config_is_sensible() {
        let config = AdaptiveConfig::default();
        assert_eq!(config.target_recovery_time, Duration::from_secs(10));
        assert_eq!(config.min_interval, Duration::from_secs(1));
        assert_eq!(config.max_interval, Duration::from_secs(300));
    }

    #[test]
    fn multiple_checkpoints_converge() {
        let mut tuner = AdaptiveCheckpointer::new(config_10s());

        // Simulate several checkpoints with consistent 500ms overhead
        for _ in 0..10 {
            tuner.record_checkpoint(Duration::from_millis(500), 1_000_000);
        }

        // Should converge to 10s - 0.5s = 9.5s
        assert_eq!(tuner.current_interval(), Duration::from_millis(9500));
    }
}
