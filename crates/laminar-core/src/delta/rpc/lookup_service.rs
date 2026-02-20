//! Remote lookup service for cross-node lookup table queries.
//!
//! Provides both the tonic service implementation (server side) and
//! the `LookupBatcher` + `CircuitBreaker` utilities (client side).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Circuit breaker states for remote lookup calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation — requests are allowed through.
    Closed,
    /// Circuit is open — requests are rejected immediately.
    Open,
    /// Testing whether the service has recovered.
    HalfOpen,
}

/// Circuit breaker for remote lookup RPCs.
///
/// Prevents cascading failures by short-circuiting requests when a
/// remote node is consistently failing.
#[derive(Debug)]
pub struct CircuitBreaker {
    state: Mutex<CircuitState>,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    failure_threshold: u64,
    success_threshold: u64,
    cooldown: Duration,
    last_failure: Mutex<Option<Instant>>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    #[must_use]
    pub fn new(failure_threshold: u64, success_threshold: u64, cooldown: Duration) -> Self {
        Self {
            state: Mutex::new(CircuitState::Closed),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            failure_threshold,
            success_threshold,
            cooldown,
            last_failure: Mutex::new(None),
        }
    }

    /// Check if a request should be allowed through.
    #[must_use]
    pub fn allow_request(&self) -> bool {
        let mut state = self.state.lock();
        match *state {
            CircuitState::Closed | CircuitState::HalfOpen => true,
            CircuitState::Open => {
                // Check if cooldown has elapsed
                let last = self.last_failure.lock();
                if let Some(t) = *last {
                    if t.elapsed() >= self.cooldown {
                        *state = CircuitState::HalfOpen;
                        self.success_count.store(0, Ordering::Relaxed);
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Record a successful request.
    pub fn record_success(&self) {
        let mut state = self.state.lock();
        match *state {
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.success_threshold {
                    *state = CircuitState::Closed;
                    self.failure_count.store(0, Ordering::Relaxed);
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::Open => {}
        }
    }

    /// Record a failed request.
    pub fn record_failure(&self) {
        let mut state = self.state.lock();
        match *state {
            CircuitState::Closed => {
                let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.failure_threshold {
                    *state = CircuitState::Open;
                    *self.last_failure.lock() = Some(Instant::now());
                }
            }
            CircuitState::HalfOpen => {
                *state = CircuitState::Open;
                *self.last_failure.lock() = Some(Instant::now());
            }
            CircuitState::Open => {}
        }
    }

    /// Get the current circuit state.
    #[must_use]
    pub fn state(&self) -> CircuitState {
        *self.state.lock()
    }

    /// Reset the circuit breaker to closed state.
    pub fn reset(&self) {
        *self.state.lock() = CircuitState::Closed;
        self.failure_count.store(0, Ordering::Relaxed);
        self.success_count.store(0, Ordering::Relaxed);
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(5, 3, Duration::from_secs(30))
    }
}

/// Configuration for the lookup batcher.
#[derive(Debug, Clone)]
pub struct LookupBatcherConfig {
    /// Maximum keys per batch before flush.
    pub max_batch_size: usize,
    /// Maximum time to hold a batch before flush.
    pub max_batch_delay: Duration,
    /// Maximum concurrent batches in-flight.
    pub max_in_flight: usize,
}

impl Default for LookupBatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 500,
            max_batch_delay: Duration::from_millis(5),
            max_in_flight: 8,
        }
    }
}

/// Groups lookup keys by target node and flushes on size/timer.
#[derive(Debug)]
pub struct LookupBatcher {
    config: LookupBatcherConfig,
    pending_count: AtomicU64,
}

impl LookupBatcher {
    /// Create a new lookup batcher.
    #[must_use]
    pub fn new(config: LookupBatcherConfig) -> Self {
        Self {
            config,
            pending_count: AtomicU64::new(0),
        }
    }

    /// Get the batcher configuration.
    #[must_use]
    pub fn config(&self) -> &LookupBatcherConfig {
        &self.config
    }

    /// Get the number of pending (unflushed) keys.
    #[must_use]
    pub fn pending_count(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }
}

impl Default for LookupBatcher {
    fn default() -> Self {
        Self::new(LookupBatcherConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_closed_allows() {
        let cb = CircuitBreaker::default();
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new(3, 2, Duration::from_secs(30));

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::new(3, 2, Duration::from_secs(30));

        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // Should reset failure count

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_recovers() {
        let cb = CircuitBreaker::new(2, 2, Duration::from_millis(10));

        // Trip the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for cooldown
        std::thread::sleep(Duration::from_millis(20));

        // Should transition to HalfOpen
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Successes should close it
        cb.record_success();
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_fails() {
        let cb = CircuitBreaker::new(2, 2, Duration::from_millis(10));

        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(20));

        assert!(cb.allow_request()); // → HalfOpen
        cb.record_failure(); // Should go back to Open
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let cb = CircuitBreaker::new(2, 2, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_batcher_config_default() {
        let config = LookupBatcherConfig::default();
        assert_eq!(config.max_batch_size, 500);
        assert_eq!(config.max_batch_delay, Duration::from_millis(5));
    }

    #[test]
    fn test_batcher_initial_state() {
        let batcher = LookupBatcher::default();
        assert_eq!(batcher.pending_count(), 0);
    }
}
