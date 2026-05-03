//! Retry/backoff helper shared by connectors.
//!
//! Centralises exponential backoff with jitter so every reconnect path
//! behaves the same way: capped exponent (no shift overflow), capped
//! delay, jittered to break thundering-herd on broker-wide outages.

use std::time::Duration;

use rand::RngExt;

/// Exponential backoff schedule with jitter and a cap.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    /// Multiplicative jitter range, expressed as a fraction in `[0.0, 1.0]`.
    /// `0.25` means the actual delay is uniform on
    /// `[delay * 0.75, delay * 1.25]`.
    jitter: f64,
}

impl Backoff {
    /// New backoff with the given bounds and jitter fraction.
    #[must_use]
    pub const fn new(initial: Duration, max: Duration, jitter: f64) -> Self {
        Self {
            initial,
            max,
            jitter,
        }
    }

    /// Default broker-reconnect schedule: 1s → 30s, ±25 % jitter.
    /// Matches what Kafka clients do; safe under simultaneous broker
    /// restarts (jitter avoids reconnect storms).
    #[must_use]
    pub const fn broker_reconnect() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(30), 0.25)
    }

    /// Compute the delay for `attempt` (0-indexed). Caps the exponent at
    /// 30 to prevent shift overflow with adversarial inputs, then caps
    /// the resulting duration at `self.max`, then applies jitter.
    #[must_use]
    pub fn delay(&self, attempt: u32) -> Duration {
        // 2^30 seconds is ~34 years; capping the exponent at 30 keeps
        // the multiplication in u64 range for any sane `initial`.
        let shift = attempt.min(30);
        let factor = 1u64 << shift;
        let raw_nanos = self
            .initial
            .as_nanos()
            .saturating_mul(u128::from(factor))
            .min(u128::from(u64::MAX));
        #[allow(clippy::cast_possible_truncation)]
        let raw = Duration::from_nanos(raw_nanos as u64).min(self.max);

        if self.jitter <= 0.0 {
            return raw;
        }
        let mut rng = rand::rng();
        let frac: f64 = rng.random_range(-self.jitter..=self.jitter);
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation
        )]
        let jittered_nanos = (raw.as_nanos() as f64 * (1.0 + frac)).max(0.0) as u64;
        Duration::from_nanos(jittered_nanos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_caps_at_max() {
        let b = Backoff::new(Duration::from_secs(1), Duration::from_secs(30), 0.0);
        assert_eq!(b.delay(0), Duration::from_secs(1));
        assert_eq!(b.delay(1), Duration::from_secs(2));
        assert_eq!(b.delay(4), Duration::from_secs(16));
        assert_eq!(b.delay(5), Duration::from_secs(30));
        // Past saturation still capped, no panic.
        assert_eq!(b.delay(100), Duration::from_secs(30));
        assert_eq!(b.delay(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn jitter_stays_inside_bounds() {
        let initial = Duration::from_secs(1);
        let max = Duration::from_secs(60);
        let b = Backoff::new(initial, max, 0.25);
        for attempt in 0..7 {
            let d = b.delay(attempt);
            let raw = (initial.saturating_mul(1u32 << attempt)).min(max);
            let lo = raw.as_secs_f64() * 0.74;
            let hi = raw.as_secs_f64() * 1.26;
            let actual = d.as_secs_f64();
            assert!(
                actual >= lo && actual <= hi,
                "attempt {attempt}: {actual} not in [{lo}, {hi}]"
            );
        }
    }

    #[test]
    fn shift_overflow_protected() {
        // The previous hand-rolled `1u64 << consecutive_failures` panicked
        // for attempt >= 64. Backoff must not.
        let b = Backoff::broker_reconnect();
        let _ = b.delay(64);
        let _ = b.delay(u32::MAX);
    }
}
