//! # Credit-Based Flow Control
//!
//! Implements credit-based backpressure similar to Apache Flink's network stack.
//!
//! ## How It Works
//!
//! ```text
//! ┌──────────┐                      ┌──────────┐
//! │  Sender  │                      │ Receiver │
//! │          │<── Credits (N=4) ────│          │
//! │          │                      │          │
//! │          │── Data + Backlog ───>│          │
//! │          │                      │          │
//! │          │<── Credits (N=2) ────│          │
//! └──────────┘                      └──────────┘
//! ```
//!
//! 1. Receiver grants initial credits (buffer slots) to sender
//! 2. Sender decrements credits when sending, includes backlog size
//! 3. Receiver processes data and returns credits based on capacity
//! 4. If sender has no credits, it must wait or apply overflow strategy
//!
//! ## Credit Types
//!
//! - **Exclusive credits**: Fixed per-sender, always available
//! - **Floating credits**: Shared pool, allocated based on backlog priority
//!
//! This design prevents buffer overflow while maximizing throughput.

// Credits are bounded by configuration to be within safe range for casting
// Max credits: 65535 (u16::MAX) to ensure clean i64/f64 conversions

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// Configuration for backpressure handling.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Initial exclusive credits per sender (like Flink's buffers-per-channel).
    /// These credits are always reserved for a specific sender.
    pub exclusive_credits: usize,

    /// Floating credits shared across all senders (like Flink's floating-buffers-per-gate).
    /// Allocated dynamically based on backlog priority.
    pub floating_credits: usize,

    /// Strategy when credits are exhausted.
    pub overflow_strategy: OverflowStrategy,

    /// High watermark - when queue reaches this %, start throttling.
    /// Value between 0.0 and 1.0.
    pub high_watermark: f64,

    /// Low watermark - when queue drops below this %, resume normal flow.
    /// Value between 0.0 and 1.0.
    pub low_watermark: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            exclusive_credits: 4,
            floating_credits: 8,
            overflow_strategy: OverflowStrategy::Block,
            high_watermark: 0.8,
            low_watermark: 0.5,
        }
    }
}

impl BackpressureConfig {
    /// Creates a new configuration builder.
    #[must_use]
    pub fn builder() -> BackpressureConfigBuilder {
        BackpressureConfigBuilder::default()
    }

    /// Total credits available (exclusive + floating).
    #[must_use]
    pub fn total_credits(&self) -> usize {
        self.exclusive_credits + self.floating_credits
    }
}

/// Builder for `BackpressureConfig`.
#[derive(Debug, Default)]
pub struct BackpressureConfigBuilder {
    exclusive_credits: Option<usize>,
    floating_credits: Option<usize>,
    overflow_strategy: Option<OverflowStrategy>,
    high_watermark: Option<f64>,
    low_watermark: Option<f64>,
}

impl BackpressureConfigBuilder {
    /// Sets exclusive credits per sender.
    #[must_use]
    pub fn exclusive_credits(mut self, credits: usize) -> Self {
        self.exclusive_credits = Some(credits);
        self
    }

    /// Sets floating credits (shared pool).
    #[must_use]
    pub fn floating_credits(mut self, credits: usize) -> Self {
        self.floating_credits = Some(credits);
        self
    }

    /// Sets the overflow strategy.
    #[must_use]
    pub fn overflow_strategy(mut self, strategy: OverflowStrategy) -> Self {
        self.overflow_strategy = Some(strategy);
        self
    }

    /// Sets the high watermark (0.0 to 1.0).
    #[must_use]
    pub fn high_watermark(mut self, watermark: f64) -> Self {
        self.high_watermark = Some(watermark.clamp(0.0, 1.0));
        self
    }

    /// Sets the low watermark (0.0 to 1.0).
    #[must_use]
    pub fn low_watermark(mut self, watermark: f64) -> Self {
        self.low_watermark = Some(watermark.clamp(0.0, 1.0));
        self
    }

    /// Builds the configuration.
    #[must_use]
    pub fn build(self) -> BackpressureConfig {
        BackpressureConfig {
            exclusive_credits: self.exclusive_credits.unwrap_or(4).min(u16::MAX as usize),
            floating_credits: self.floating_credits.unwrap_or(8).min(u16::MAX as usize),
            overflow_strategy: self.overflow_strategy.unwrap_or(OverflowStrategy::Block),
            high_watermark: self.high_watermark.unwrap_or(0.8),
            low_watermark: self.low_watermark.unwrap_or(0.5),
        }
    }
}

/// Strategy for handling overflow when credits are exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowStrategy {
    /// Block the sender until credits become available.
    /// Best for exactly-once semantics where no data loss is acceptable.
    Block,

    /// Drop the data and record metrics.
    /// Best for best-effort streams where latency matters more than completeness.
    Drop,

    /// Return an error immediately without blocking or dropping.
    /// Caller decides what to do.
    Error,
}

/// Result of attempting to acquire credits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreditAcquireResult {
    /// Credits acquired successfully.
    Acquired,
    /// No credits available, would need to wait.
    WouldBlock,
    /// Credits exhausted and overflow strategy is Drop.
    Dropped,
}

/// Manages credits for a single receiver (core).
///
/// Thread-safe for use between sender and receiver threads.
#[derive(Debug)]
pub struct CreditGate {
    /// Available credits (can go negative temporarily during high contention).
    available: AtomicI64,
    /// Maximum credits (exclusive + floating).
    max_credits: usize,
    /// Configuration.
    config: BackpressureConfig,
    /// Metrics.
    metrics: CreditMetrics,
}

impl CreditGate {
    /// Creates a new credit gate with the given configuration.
    #[must_use]
    pub fn new(config: BackpressureConfig) -> Self {
        let max_credits = config.total_credits();
        Self {
            #[allow(clippy::cast_possible_wrap)] // Safe: bounded by u16::MAX in build()
            available: AtomicI64::new(max_credits as i64),
            max_credits,
            config,
            metrics: CreditMetrics::new(),
        }
    }

    /// Attempts to acquire one credit.
    ///
    /// Returns the result based on the overflow strategy.
    pub fn try_acquire(&self) -> CreditAcquireResult {
        self.try_acquire_n(1)
    }

    /// Attempts to acquire N credits.
    pub fn try_acquire_n(&self, n: usize) -> CreditAcquireResult {
        let n = i64::try_from(n).unwrap_or(i64::MAX);

        // Try to acquire credits atomically
        let mut current = self.available.load(Ordering::Acquire);
        loop {
            if current < n {
                // Not enough credits
                self.metrics.record_blocked();
                return match self.config.overflow_strategy {
                    OverflowStrategy::Drop => {
                        self.metrics.record_dropped(u64::try_from(n).unwrap_or(0));
                        CreditAcquireResult::Dropped
                    }
                    // Both Block and Error return WouldBlock - caller handles differently
                    OverflowStrategy::Block | OverflowStrategy::Error => {
                        CreditAcquireResult::WouldBlock
                    }
                };
            }

            match self.available.compare_exchange_weak(
                current,
                current - n,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.metrics.record_acquired(u64::try_from(n).unwrap_or(0));
                    return CreditAcquireResult::Acquired;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Acquires credits, spinning until available.
    ///
    /// Only use when `OverflowStrategy::Block` is configured.
    pub fn acquire_blocking(&self, n: usize) {
        loop {
            match self.try_acquire_n(n) {
                // Success or dropped (shouldn't happen with Block strategy)
                CreditAcquireResult::Acquired | CreditAcquireResult::Dropped => return,
                CreditAcquireResult::WouldBlock => {
                    // Spin with backoff
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Releases credits back to the pool.
    ///
    /// Called by receiver after processing data.
    pub fn release(&self, n: usize) {
        let n = i64::try_from(n).unwrap_or(i64::MAX);
        let prev = self.available.fetch_add(n, Ordering::Release);

        // Clamp to max (in case of over-release)
        let new_val = prev + n;
        if new_val > {
            #[allow(clippy::cast_possible_wrap)]
            let max = self.max_credits as i64;
            max
        } {
            // Try to correct, but don't worry if it fails (another thread may have acquired)
            let _ = self.available.compare_exchange(
                new_val,
                {
                    #[allow(clippy::cast_possible_wrap)]
                    let max = self.max_credits as i64;
                    max
                },
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }

        self.metrics.record_released(u64::try_from(n).unwrap_or(0));
    }

    /// Returns the number of available credits.
    #[must_use]
    pub fn available(&self) -> usize {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        // Safe: load().max(0) is >= 0
        let val = self.available.load(Ordering::Relaxed).max(0) as usize;
        val
    }

    /// Returns the maximum credits.
    #[must_use]
    pub fn max_credits(&self) -> usize {
        self.max_credits
    }

    /// Returns true if backpressure is active (credits below threshold).
    #[must_use]
    pub fn is_backpressured(&self) -> bool {
        #[allow(clippy::cast_precision_loss)] // Acceptable for ratio
        let available = self.available() as f64;
        #[allow(clippy::cast_precision_loss)] // Acceptable for ratio calculation
        let max = self.max_credits as f64;
        (available / max) < (1.0 - self.config.high_watermark)
    }

    /// Returns true if backpressure has cleared (credits above low watermark).
    #[must_use]
    pub fn is_recovered(&self) -> bool {
        #[allow(clippy::cast_precision_loss)] // Acceptable for ratio
        let available = self.available() as f64;
        #[allow(clippy::cast_precision_loss)] // Acceptable for ratio calculation
        let max = self.max_credits as f64;
        (available / max) >= (1.0 - self.config.low_watermark)
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &BackpressureConfig {
        &self.config
    }

    /// Returns the metrics.
    #[must_use]
    pub fn metrics(&self) -> &CreditMetrics {
        &self.metrics
    }

    /// Resets the gate to initial state.
    pub fn reset(&self) {
        #[allow(clippy::cast_possible_wrap)] // Safe: max_credits bounded to u16::MAX
        self.available
            .store(self.max_credits as i64, Ordering::Release);
        self.metrics.reset();
    }
}

/// Metrics for credit-based flow control.
#[derive(Debug)]
pub struct CreditMetrics {
    /// Total credits acquired.
    credits_acquired: AtomicU64,
    /// Total credits released.
    credits_released: AtomicU64,
    /// Times sender was blocked due to no credits.
    times_blocked: AtomicU64,
    /// Items dropped due to overflow.
    items_dropped: AtomicU64,
}

impl CreditMetrics {
    /// Creates new metrics.
    fn new() -> Self {
        Self {
            credits_acquired: AtomicU64::new(0),
            credits_released: AtomicU64::new(0),
            times_blocked: AtomicU64::new(0),
            items_dropped: AtomicU64::new(0),
        }
    }

    fn record_acquired(&self, n: u64) {
        self.credits_acquired.fetch_add(n, Ordering::Relaxed);
    }

    fn record_released(&self, n: u64) {
        self.credits_released.fetch_add(n, Ordering::Relaxed);
    }

    fn record_blocked(&self) {
        self.times_blocked.fetch_add(1, Ordering::Relaxed);
    }

    fn record_dropped(&self, n: u64) {
        self.items_dropped.fetch_add(n, Ordering::Relaxed);
    }

    /// Resets all metrics.
    fn reset(&self) {
        self.credits_acquired.store(0, Ordering::Relaxed);
        self.credits_released.store(0, Ordering::Relaxed);
        self.times_blocked.store(0, Ordering::Relaxed);
        self.items_dropped.store(0, Ordering::Relaxed);
    }

    /// Returns total credits acquired.
    #[must_use]
    pub fn credits_acquired(&self) -> u64 {
        self.credits_acquired.load(Ordering::Relaxed)
    }

    /// Returns total credits released.
    #[must_use]
    pub fn credits_released(&self) -> u64 {
        self.credits_released.load(Ordering::Relaxed)
    }

    /// Returns times sender was blocked.
    #[must_use]
    pub fn times_blocked(&self) -> u64 {
        self.times_blocked.load(Ordering::Relaxed)
    }

    /// Returns items dropped due to overflow.
    #[must_use]
    pub fn items_dropped(&self) -> u64 {
        self.items_dropped.load(Ordering::Relaxed)
    }

    /// Returns a snapshot of all metrics.
    #[must_use]
    pub fn snapshot(&self) -> CreditMetricsSnapshot {
        CreditMetricsSnapshot {
            credits_acquired: self.credits_acquired(),
            credits_released: self.credits_released(),
            times_blocked: self.times_blocked(),
            items_dropped: self.items_dropped(),
        }
    }
}

/// Snapshot of credit metrics for reporting.
#[derive(Debug, Clone, Copy)]
pub struct CreditMetricsSnapshot {
    /// Total credits acquired.
    pub credits_acquired: u64,
    /// Total credits released.
    pub credits_released: u64,
    /// Times sender was blocked.
    pub times_blocked: u64,
    /// Items dropped due to overflow.
    pub items_dropped: u64,
}

impl CreditMetricsSnapshot {
    /// Returns credits currently in flight (acquired - released).
    #[allow(clippy::cast_possible_wrap)] // Metrics are u64, difference fits in i64 unless skewed
    #[must_use]
    pub fn credits_in_flight(&self) -> i64 {
        (self.credits_acquired as i64).saturating_sub(self.credits_released as i64)
    }
}

/// Manages credit flow between a sender and receiver.
///
/// This is a convenience wrapper that combines a `CreditGate` with
/// additional sender-side state like backlog tracking.
#[derive(Debug)]
pub struct CreditChannel {
    /// The credit gate (shared with receiver).
    gate: Arc<CreditGate>,
    /// Current backlog at sender (items waiting to be sent).
    /// Wrapped in `Arc` for safe sharing with `CreditSender`.
    backlog: Arc<AtomicU64>,
}

impl CreditChannel {
    /// Creates a new credit channel.
    #[must_use]
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            gate: Arc::new(CreditGate::new(config)),
            backlog: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns a handle for the sender side.
    #[must_use]
    pub fn sender(&self) -> CreditSender {
        CreditSender {
            gate: Arc::clone(&self.gate),
            backlog: Arc::clone(&self.backlog),
        }
    }

    /// Returns a handle for the receiver side.
    #[must_use]
    pub fn receiver(&self) -> CreditReceiver {
        CreditReceiver {
            gate: Arc::clone(&self.gate),
        }
    }

    /// Returns the credit gate.
    #[must_use]
    pub fn gate(&self) -> &CreditGate {
        &self.gate
    }
}

/// Sender-side handle for credit flow control.
#[derive(Debug, Clone)]
pub struct CreditSender {
    gate: Arc<CreditGate>,
    /// Shared backlog counter with the channel.
    backlog: Arc<AtomicU64>,
}

impl CreditSender {
    /// Attempts to send (acquire credit).
    #[must_use]
    pub fn try_send(&self) -> CreditAcquireResult {
        self.gate.try_acquire()
    }

    /// Sends with blocking if needed.
    pub fn send_blocking(&self) {
        self.gate.acquire_blocking(1);
    }

    /// Reports backlog size to receiver.
    pub fn set_backlog(&self, size: u64) {
        self.backlog.store(size, Ordering::Relaxed);
    }

    /// Returns current backlog.
    #[must_use]
    pub fn backlog(&self) -> u64 {
        self.backlog.load(Ordering::Relaxed)
    }

    /// Returns available credits.
    #[must_use]
    pub fn available_credits(&self) -> usize {
        self.gate.available()
    }

    /// Returns true if backpressured.
    #[must_use]
    pub fn is_backpressured(&self) -> bool {
        self.gate.is_backpressured()
    }
}

/// Receiver-side handle for credit flow control.
#[derive(Debug, Clone)]
pub struct CreditReceiver {
    gate: Arc<CreditGate>,
}

impl CreditReceiver {
    /// Releases credits after processing.
    pub fn release(&self, n: usize) {
        self.gate.release(n);
    }

    /// Returns available credits.
    #[must_use]
    pub fn available_credits(&self) -> usize {
        self.gate.available()
    }

    /// Returns true if recovered from backpressure.
    #[must_use]
    pub fn is_recovered(&self) -> bool {
        self.gate.is_recovered()
    }

    /// Returns metrics.
    #[must_use]
    pub fn metrics(&self) -> &CreditMetrics {
        self.gate.metrics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credit_gate_basic() {
        let config = BackpressureConfig {
            exclusive_credits: 2,
            floating_credits: 2,
            overflow_strategy: OverflowStrategy::Block,
            ..Default::default()
        };
        let gate = CreditGate::new(config);

        assert_eq!(gate.available(), 4);
        assert_eq!(gate.max_credits(), 4);

        // Acquire all credits
        assert_eq!(gate.try_acquire(), CreditAcquireResult::Acquired);
        assert_eq!(gate.try_acquire(), CreditAcquireResult::Acquired);
        assert_eq!(gate.try_acquire(), CreditAcquireResult::Acquired);
        assert_eq!(gate.try_acquire(), CreditAcquireResult::Acquired);
        assert_eq!(gate.available(), 0);

        // Should block now
        assert_eq!(gate.try_acquire(), CreditAcquireResult::WouldBlock);

        // Release some
        gate.release(2);
        assert_eq!(gate.available(), 2);

        // Can acquire again
        assert_eq!(gate.try_acquire(), CreditAcquireResult::Acquired);
        assert_eq!(gate.available(), 1);
    }

    #[test]
    fn test_credit_gate_drop_strategy() {
        let config = BackpressureConfig {
            exclusive_credits: 1,
            floating_credits: 0,
            overflow_strategy: OverflowStrategy::Drop,
            ..Default::default()
        };
        let gate = CreditGate::new(config);

        assert_eq!(gate.try_acquire(), CreditAcquireResult::Acquired);
        assert_eq!(gate.try_acquire(), CreditAcquireResult::Dropped);

        assert_eq!(gate.metrics().items_dropped(), 1);
    }

    #[test]
    fn test_credit_gate_batch_acquire() {
        let config = BackpressureConfig {
            exclusive_credits: 4,
            floating_credits: 4,
            overflow_strategy: OverflowStrategy::Block,
            ..Default::default()
        };
        let gate = CreditGate::new(config);

        assert_eq!(gate.try_acquire_n(5), CreditAcquireResult::Acquired);
        assert_eq!(gate.available(), 3);

        assert_eq!(gate.try_acquire_n(4), CreditAcquireResult::WouldBlock);
        assert_eq!(gate.try_acquire_n(3), CreditAcquireResult::Acquired);
        assert_eq!(gate.available(), 0);
    }

    #[test]
    fn test_credit_channel() {
        let config = BackpressureConfig::default();
        let channel = CreditChannel::new(config);

        let sender = channel.sender();
        let receiver = channel.receiver();

        // Sender can send
        assert_eq!(sender.try_send(), CreditAcquireResult::Acquired);
        sender.set_backlog(10);
        assert_eq!(sender.backlog(), 10);

        // Receiver releases
        receiver.release(1);
        assert_eq!(receiver.available_credits(), channel.gate().max_credits());
    }

    #[test]
    fn test_backpressure_watermarks() {
        let config = BackpressureConfig {
            exclusive_credits: 10,
            floating_credits: 0,
            high_watermark: 0.8, // Backpressure when <20% available
            low_watermark: 0.5,  // Recovered when >50% available
            ..Default::default()
        };
        let gate = CreditGate::new(config);

        // Initially not backpressured
        assert!(!gate.is_backpressured());
        assert!(gate.is_recovered());

        // Use 9 credits (10% available) - should be backpressured
        for _ in 0..9 {
            gate.try_acquire();
        }
        assert!(gate.is_backpressured());
        assert!(!gate.is_recovered());

        // Release 4 (50% available) - should be recovered
        gate.release(4);
        assert!(!gate.is_backpressured());
        assert!(gate.is_recovered());
    }

    #[test]
    fn test_metrics_snapshot() {
        let config = BackpressureConfig::default();
        let gate = CreditGate::new(config);

        gate.try_acquire();
        gate.try_acquire();
        gate.release(1);

        let snapshot = gate.metrics().snapshot();
        assert_eq!(snapshot.credits_acquired, 2);
        assert_eq!(snapshot.credits_released, 1);
        assert_eq!(snapshot.credits_in_flight(), 1);
    }

    #[test]
    fn test_config_builder() {
        let config = BackpressureConfig::builder()
            .exclusive_credits(8)
            .floating_credits(16)
            .overflow_strategy(OverflowStrategy::Drop)
            .high_watermark(0.9)
            .low_watermark(0.6)
            .build();

        assert_eq!(config.exclusive_credits, 8);
        assert_eq!(config.floating_credits, 16);
        assert_eq!(config.overflow_strategy, OverflowStrategy::Drop);
        assert!((config.high_watermark - 0.9).abs() < f64::EPSILON);
        assert!((config.low_watermark - 0.6).abs() < f64::EPSILON);
        assert_eq!(config.total_credits(), 24);
    }

    #[test]
    fn test_concurrent_acquire_release() {
        use std::sync::Arc;
        use std::thread;

        let config = BackpressureConfig {
            exclusive_credits: 100,
            floating_credits: 0,
            overflow_strategy: OverflowStrategy::Block,
            ..Default::default()
        };
        let gate = Arc::new(CreditGate::new(config));

        let gate_sender = Arc::clone(&gate);
        let gate_receiver = Arc::clone(&gate);

        let sender = thread::spawn(move || {
            let mut acquired = 0;
            for _ in 0..1000 {
                if gate_sender.try_acquire() == CreditAcquireResult::Acquired {
                    acquired += 1;
                }
                // Simulate some work
                std::hint::spin_loop();
            }
            acquired
        });

        let receiver = thread::spawn(move || {
            let mut released = 0;
            for _ in 0..500 {
                gate_receiver.release(1);
                released += 1;
                std::hint::spin_loop();
            }
            released
        });

        let acquired = sender.join().unwrap();
        let released = receiver.join().unwrap();

        // Should have acquired some (up to 100 initial + 500 released)
        assert!(acquired > 0);
        assert_eq!(released, 500);
    }
}
