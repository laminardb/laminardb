//! Retry policies and circuit breaker for connector resilience.
//!
//! Provides utilities to handle transient failures in connector operations:
//! - [`RetryPolicy`]: Configurable retry with exponential backoff
//! - [`with_retry`]: Async helper that retries operations
//! - [`CircuitBreaker`]: Prevents cascade failures

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::error::ConnectorError;

/// Retry policy configuration.
///
/// Supports exponential backoff with optional jitter to prevent thundering herd.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (not counting the initial attempt).
    pub max_attempts: usize,
    /// Initial delay before first retry.
    pub initial_backoff: Duration,
    /// Maximum delay between retries.
    pub max_backoff: Duration,
    /// Backoff multiplier (e.g., 2.0 for doubling).
    pub multiplier: f64,
    /// Jitter factor (0.0-1.0) to randomize delays.
    pub jitter: f64,
}

impl RetryPolicy {
    /// Creates a new retry policy.
    #[must_use]
    pub fn new(
        max_attempts: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
        multiplier: f64,
        jitter: f64,
    ) -> Self {
        Self {
            max_attempts,
            initial_backoff,
            max_backoff,
            multiplier,
            jitter: jitter.clamp(0.0, 1.0),
        }
    }

    /// Creates a simple exponential backoff policy.
    ///
    /// Uses a multiplier of 2.0 and 10% jitter.
    #[must_use]
    pub fn exponential(max_attempts: usize, initial_backoff: Duration) -> Self {
        Self {
            max_attempts,
            initial_backoff,
            max_backoff: Duration::from_secs(60),
            multiplier: 2.0,
            jitter: 0.1,
        }
    }

    /// Creates a fixed-interval retry policy (no backoff).
    #[must_use]
    pub fn fixed(max_attempts: usize, interval: Duration) -> Self {
        Self {
            max_attempts,
            initial_backoff: interval,
            max_backoff: interval,
            multiplier: 1.0,
            jitter: 0.0,
        }
    }

    /// Creates a policy with no retries.
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_attempts: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            multiplier: 1.0,
            jitter: 0.0,
        }
    }

    /// Calculates the delay for a given attempt number.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // attempt count will never exceed i32::MAX
    #[allow(clippy::cast_possible_wrap)] // attempt count is always small positive
    #[allow(clippy::cast_precision_loss)] // acceptable for delay calculation
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        let exponent = (attempt as i32).saturating_sub(1);
        let base_delay = self.initial_backoff.as_secs_f64() * self.multiplier.powi(exponent);
        let clamped = base_delay.min(self.max_backoff.as_secs_f64());

        // Apply jitter
        let jitter_range = clamped * self.jitter;
        let jitter_offset = if self.jitter > 0.0 {
            // Use a simple deterministic "randomness" based on attempt number
            // In production, you'd use a proper RNG
            let pseudo_random = ((attempt as f64 * 0.618_033_988_749_895) % 1.0) * 2.0 - 1.0;
            jitter_range * pseudo_random
        } else {
            0.0
        };

        Duration::from_secs_f64((clamped + jitter_offset).max(0.0))
    }

    /// Returns `true` if we should retry after the given attempt.
    #[must_use]
    pub fn should_retry(&self, attempt: usize) -> bool {
        attempt < self.max_attempts
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential(3, Duration::from_millis(100))
    }
}

/// Executes an async operation with retry.
///
/// # Errors
///
/// Returns the last `ConnectorError` after all retry attempts are exhausted.
///
/// # Example
///
/// ```rust,ignore
/// let policy = RetryPolicy::exponential(3, Duration::from_millis(100));
/// let result = with_retry(&policy, || async {
///     perform_operation().await
/// }).await;
/// ```
pub async fn with_retry<F, T, Fut>(policy: &RetryPolicy, mut op: F) -> Result<T, ConnectorError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ConnectorError>>,
{
    let mut attempt = 0;

    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) if !policy.should_retry(attempt) => return Err(e),
            Err(_) => {
                attempt += 1;
                let delay = policy.delay_for_attempt(attempt);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

/// Executes an operation with retry, calling an error handler on each failure.
///
/// The error handler receives the attempt number and error, allowing logging
/// or metrics collection.
///
/// # Errors
///
/// Returns the last `ConnectorError` after all retry attempts are exhausted.
pub async fn with_retry_and_handler<F, T, Fut, H>(
    policy: &RetryPolicy,
    mut op: F,
    mut on_error: H,
) -> Result<T, ConnectorError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ConnectorError>>,
    H: FnMut(usize, &ConnectorError),
{
    let mut attempt = 0;

    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) if !policy.should_retry(attempt) => {
                on_error(attempt, &e);
                return Err(e);
            }
            Err(e) => {
                on_error(attempt, &e);
                attempt += 1;
                let delay = policy.delay_for_attempt(attempt);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed, operations proceed normally.
    Closed,
    /// Circuit is open, operations fail fast.
    Open,
    /// Circuit is testing if the service has recovered.
    HalfOpen,
}

/// Circuit breaker for preventing cascade failures.
///
/// Tracks failures and opens the circuit when a threshold is reached,
/// preventing further calls until a reset timeout.
#[derive(Debug)]
pub struct CircuitBreaker {
    /// Failure threshold to open the circuit.
    failure_threshold: u64,
    /// Duration to stay open before trying half-open.
    reset_timeout: Duration,
    /// Number of successful calls in half-open to close.
    half_open_successes_required: u64,
    /// Internal state.
    state: Arc<Mutex<CircuitBreakerState>>,
}

#[derive(Debug)]
struct CircuitBreakerState {
    current_state: CircuitState,
    failure_count: u64,
    success_count: u64,
    last_failure_time: Option<Instant>,
}

impl CircuitBreaker {
    /// Creates a new circuit breaker.
    #[must_use]
    pub fn new(failure_threshold: u64, reset_timeout: Duration) -> Self {
        Self {
            failure_threshold,
            reset_timeout,
            half_open_successes_required: 1,
            state: Arc::new(Mutex::new(CircuitBreakerState {
                current_state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                last_failure_time: None,
            })),
        }
    }

    /// Creates a circuit breaker with custom half-open success requirement.
    #[must_use]
    pub fn with_half_open_successes(mut self, successes: u64) -> Self {
        self.half_open_successes_required = successes.max(1);
        self
    }

    /// Returns the current circuit state.
    #[must_use]
    pub fn state(&self) -> CircuitState {
        let mut state = self.state.lock();
        self.maybe_transition_from_open(&mut state);
        state.current_state
    }

    /// Checks if the circuit allows the operation to proceed.
    ///
    /// Returns `true` if the operation should proceed, `false` if it should
    /// fail fast.
    #[must_use]
    pub fn allow(&self) -> bool {
        let mut state = self.state.lock();
        self.maybe_transition_from_open(&mut state);

        match state.current_state {
            CircuitState::Open => false,
            // Closed allows normal operation, HalfOpen allows one request to test recovery
            CircuitState::Closed | CircuitState::HalfOpen => true,
        }
    }

    /// Records a successful operation.
    pub fn record_success(&self) {
        let mut state = self.state.lock();

        match state.current_state {
            CircuitState::Closed => {
                state.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                state.success_count += 1;
                if state.success_count >= self.half_open_successes_required {
                    state.current_state = CircuitState::Closed;
                    state.failure_count = 0;
                    state.success_count = 0;
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but handle gracefully
            }
        }
    }

    /// Records a failed operation.
    pub fn record_failure(&self) {
        let mut state = self.state.lock();

        match state.current_state {
            CircuitState::Closed => {
                state.failure_count += 1;
                if state.failure_count >= self.failure_threshold {
                    state.current_state = CircuitState::Open;
                    state.last_failure_time = Some(Instant::now());
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately reopens
                state.current_state = CircuitState::Open;
                state.last_failure_time = Some(Instant::now());
                state.success_count = 0;
            }
            CircuitState::Open => {
                state.last_failure_time = Some(Instant::now());
            }
        }
    }

    /// Resets the circuit breaker to closed state.
    pub fn reset(&self) {
        let mut state = self.state.lock();
        state.current_state = CircuitState::Closed;
        state.failure_count = 0;
        state.success_count = 0;
        state.last_failure_time = None;
    }

    /// Returns the current failure count.
    #[must_use]
    pub fn failure_count(&self) -> u64 {
        self.state.lock().failure_count
    }

    fn maybe_transition_from_open(&self, state: &mut CircuitBreakerState) {
        if state.current_state == CircuitState::Open {
            if let Some(last_failure) = state.last_failure_time {
                if last_failure.elapsed() >= self.reset_timeout {
                    state.current_state = CircuitState::HalfOpen;
                    state.success_count = 0;
                }
            }
        }
    }
}

impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            failure_threshold: self.failure_threshold,
            reset_timeout: self.reset_timeout,
            half_open_successes_required: self.half_open_successes_required,
            state: self.state.clone(),
        }
    }
}

/// Executes an operation protected by a circuit breaker.
///
/// # Errors
///
/// Returns `ConnectorError::Internal` if the circuit is open, or propagates
/// the operation's error.
pub async fn with_circuit_breaker<F, T, Fut>(
    breaker: &CircuitBreaker,
    op: F,
) -> Result<T, ConnectorError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, ConnectorError>>,
{
    if !breaker.allow() {
        return Err(ConnectorError::Internal(
            "circuit breaker is open".to_string(),
        ));
    }

    match op().await {
        Ok(value) => {
            breaker.record_success();
            Ok(value)
        }
        Err(e) => {
            breaker.record_failure();
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[test]
    fn test_retry_policy_exponential() {
        let policy = RetryPolicy::exponential(3, Duration::from_millis(100));

        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.multiplier, 2.0);
    }

    #[test]
    fn test_retry_policy_fixed() {
        let policy = RetryPolicy::fixed(5, Duration::from_secs(1));

        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.multiplier, 1.0);
        assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(1));
        assert_eq!(policy.delay_for_attempt(5), Duration::from_secs(1));
    }

    #[test]
    fn test_retry_policy_none() {
        let policy = RetryPolicy::none();
        assert!(!policy.should_retry(0));
    }

    #[test]
    fn test_delay_for_attempt_exponential() {
        let policy = RetryPolicy::new(
            5,
            Duration::from_millis(100),
            Duration::from_secs(10),
            2.0,
            0.0, // No jitter for predictable testing
        );

        assert_eq!(policy.delay_for_attempt(0), Duration::ZERO);
        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(400));
        assert_eq!(policy.delay_for_attempt(4), Duration::from_millis(800));
    }

    #[test]
    fn test_delay_capped_at_max() {
        let policy = RetryPolicy::new(10, Duration::from_secs(1), Duration::from_secs(5), 2.0, 0.0);

        // After several attempts, should be capped at 5 seconds
        assert_eq!(policy.delay_for_attempt(10), Duration::from_secs(5));
    }

    #[test]
    fn test_should_retry() {
        let policy = RetryPolicy::exponential(3, Duration::from_millis(100));

        assert!(policy.should_retry(0));
        assert!(policy.should_retry(1));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
    }

    #[tokio::test]
    async fn test_with_retry_success() {
        let policy = RetryPolicy::exponential(3, Duration::from_millis(1));
        let result = with_retry(&policy, || async { Ok::<_, ConnectorError>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_with_retry_eventual_success() {
        let policy = RetryPolicy::fixed(3, Duration::from_millis(1));
        let attempt = std::sync::atomic::AtomicUsize::new(0);

        let result = with_retry(&policy, || {
            let a = attempt.fetch_add(1, Ordering::Relaxed);
            async move {
                if a < 2 {
                    Err(ConnectorError::ConnectionFailed("test".into()))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempt.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_with_retry_exhausted() {
        let policy = RetryPolicy::fixed(2, Duration::from_millis(1));
        let attempt = AtomicU64::new(0);

        let result = with_retry(&policy, || {
            attempt.fetch_add(1, Ordering::Relaxed);
            async { Err::<i32, _>(ConnectorError::ConnectionFailed("always fail".into())) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempt.load(Ordering::Relaxed), 3); // Initial + 2 retries
    }

    #[test]
    fn test_circuit_breaker_initial_state() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow());
    }

    #[test]
    fn test_circuit_breaker_opens_on_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failure_count() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.failure_count(), 2);

        cb.record_success();
        assert_eq!(cb.failure_count(), 0);
    }

    #[test]
    fn test_circuit_breaker_half_open() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(10));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for reset timeout
        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.allow());
    }

    #[test]
    fn test_circuit_breaker_half_open_to_closed() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(10)).with_half_open_successes(2);

        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_failure_reopens() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(10));

        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let cb = CircuitBreaker::new(2, Duration::from_secs(30));

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow());
    }

    #[tokio::test]
    async fn test_with_circuit_breaker_success() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));

        let result = with_circuit_breaker(&cb, || async { Ok::<_, ConnectorError>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_with_circuit_breaker_open() {
        let cb = CircuitBreaker::new(1, Duration::from_secs(30));
        cb.record_failure();

        let result = with_circuit_breaker(&cb, || async { Ok::<_, ConnectorError>(42) }).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("circuit breaker"));
    }
}
