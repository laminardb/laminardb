//! Token bucket rate limiter for DLQ writes.
//!
//! Prevents a burst of malformed records from flooding the DLQ destination.
//! Operates in Ring 2 (async DLQ writer task).

use std::time::Instant;

/// Token bucket rate limiter for DLQ writes.
///
/// Uses a classic token bucket algorithm: tokens refill at a constant
/// rate up to `max_tokens`. Each DLQ write consumes one token. When no
/// tokens are available, the write is rejected (counted as rate-limited).
#[derive(Debug)]
pub struct TokenBucket {
    /// Maximum tokens (errors allowed per second). 0 = unlimited.
    max_tokens: u32,
    /// Current available tokens.
    tokens: f64,
    /// Last refill timestamp.
    last_refill: Instant,
}

impl TokenBucket {
    /// Creates a new token bucket with the given rate (tokens per second).
    ///
    /// A rate of 0 means unlimited (every acquire succeeds).
    #[must_use]
    pub fn new(max_per_second: u32) -> Self {
        Self {
            max_tokens: max_per_second,
            tokens: f64::from(max_per_second),
            last_refill: Instant::now(),
        }
    }

    /// Tries to consume one token. Returns `true` if allowed.
    pub fn try_acquire(&mut self) -> bool {
        if self.max_tokens == 0 {
            return true; // unlimited
        }
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Refills tokens based on elapsed time since last refill.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens =
            (self.tokens + elapsed * f64::from(self.max_tokens)).min(f64::from(self.max_tokens));
        self.last_refill = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_unlimited_always_acquires() {
        let mut bucket = TokenBucket::new(0);
        for _ in 0..1000 {
            assert!(bucket.try_acquire());
        }
    }

    #[test]
    fn test_acquire_within_rate() {
        let mut bucket = TokenBucket::new(10);
        // Should be able to acquire up to 10 immediately
        for _ in 0..10 {
            assert!(bucket.try_acquire());
        }
    }

    #[test]
    fn test_deny_over_rate() {
        let mut bucket = TokenBucket::new(5);
        // Drain all 5 tokens
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }
        // Next one should be denied
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn test_refill_after_time() {
        let mut bucket = TokenBucket::new(100);
        // Drain all tokens
        for _ in 0..100 {
            bucket.try_acquire();
        }
        assert!(!bucket.try_acquire());

        // Wait for refill (100 tokens/sec, 50ms = ~5 tokens)
        thread::sleep(Duration::from_millis(60));
        assert!(bucket.try_acquire());
    }

    #[test]
    fn test_new_bucket_starts_full() {
        let bucket = TokenBucket::new(50);
        assert!((bucket.tokens - 50.0).abs() < f64::EPSILON);
    }
}
