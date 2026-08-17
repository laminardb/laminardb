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
    /// Jitter prevents reconnect storms during simultaneous endpoint restarts.
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
        Duration::from_nanos(jittered_nanos).min(self.max)
    }
}

#[cfg(test)]
mod tests;
