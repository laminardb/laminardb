//! Backpressure control for the subscription system.
//!
//! Provides per-subscription backpressure strategies that determine behavior
//! when a subscriber's channel buffer is full, plus demand-based flow control
//! for Reactive Streams integration.
//!
//! # Strategies
//!
//! - **`DropOldest`**: Uses broadcast channel's natural lagging (subscribers skip ahead)
//! - **`DropNewest`**: Silently discards new events when buffer full
//! - **`Block`**: Blocks the Ring 1 dispatcher until buffer has space (never Ring 0)
//! - **`Sample(n)`**: Delivers every Nth event, drops the rest
//!
//! # Demand-Based Flow Control
//!
//! [`DemandBackpressure`] implements the Reactive Streams `request(n)` model:
//! the subscriber must explicitly request events via [`DemandHandle::request`].
//! The dispatcher only delivers when pending demand > 0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::subscription::registry::BackpressureStrategy;

// ---------------------------------------------------------------------------
// BackpressureController
// ---------------------------------------------------------------------------

/// Per-subscription backpressure controller.
///
/// Tracks the backpressure strategy and dropped event count for a single
/// subscription. The dispatcher consults this controller before sending
/// each event to determine whether it should be delivered or dropped.
pub struct BackpressureController {
    /// The backpressure strategy.
    strategy: BackpressureStrategy,
    /// Total events dropped due to backpressure.
    dropped: u64,
    /// Counter for `Sample(n)` strategy.
    sample_counter: u64,
    /// Lag threshold for emitting a warning log.
    lag_warning_threshold: u64,
}

impl BackpressureController {
    /// Creates a new controller with the given strategy.
    #[must_use]
    pub fn new(strategy: BackpressureStrategy) -> Self {
        Self {
            strategy,
            dropped: 0,
            sample_counter: 0,
            lag_warning_threshold: 1000,
        }
    }

    /// Creates a new controller with a custom lag warning threshold.
    #[must_use]
    pub fn with_lag_threshold(strategy: BackpressureStrategy, threshold: u64) -> Self {
        Self {
            strategy,
            dropped: 0,
            sample_counter: 0,
            lag_warning_threshold: threshold,
        }
    }

    /// Determines whether the next event should be delivered.
    ///
    /// For `Sample(n)`, increments an internal counter and returns `true`
    /// every Nth call. For other strategies, always returns `true` — the
    /// actual backpressure is handled by the broadcast channel or the
    /// dispatcher's send logic.
    #[inline]
    pub fn should_deliver(&mut self) -> bool {
        match self.strategy {
            BackpressureStrategy::DropOldest
            | BackpressureStrategy::DropNewest
            | BackpressureStrategy::Block => true,
            BackpressureStrategy::Sample(n) => {
                self.sample_counter += 1;
                if self.sample_counter.is_multiple_of(n as u64) {
                    true
                } else {
                    self.dropped += 1;
                    false
                }
            }
        }
    }

    /// Records an event drop (e.g., `DropNewest` when buffer is full).
    pub fn record_drop(&mut self) {
        self.dropped += 1;
    }

    /// Returns the total number of events dropped.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Returns the configured backpressure strategy.
    #[must_use]
    pub fn strategy(&self) -> BackpressureStrategy {
        self.strategy
    }

    /// Returns the lag warning threshold.
    #[must_use]
    pub fn lag_warning_threshold(&self) -> u64 {
        self.lag_warning_threshold
    }

    /// Returns `true` if the given lag exceeds the warning threshold.
    #[must_use]
    pub fn is_lagging(&self, lag: u64) -> bool {
        lag >= self.lag_warning_threshold
    }
}

// ---------------------------------------------------------------------------
// DemandBackpressure
// ---------------------------------------------------------------------------

/// Demand-based backpressure (Reactive Streams `request(n)` model).
///
/// The subscriber calls [`DemandHandle::request`] to indicate it can accept
/// N more events. The dispatcher calls [`try_consume`](Self::try_consume)
/// before each delivery — if pending demand is 0, the event is not sent.
///
/// # Thread Safety
///
/// The pending demand counter is an [`AtomicU64`] shared between the
/// dispatcher (which decrements via `try_consume`) and the subscriber
/// (which increments via `request`). The CAS loop in `try_consume`
/// ensures correctness under concurrent access.
pub struct DemandBackpressure {
    /// Pending demand: subscriber has requested this many more events.
    pending: Arc<AtomicU64>,
}

/// Handle given to the subscriber to request more events.
///
/// Created by [`DemandBackpressure::new`]. The subscriber calls
/// [`request`](Self::request) to increase pending demand.
#[derive(Clone)]
pub struct DemandHandle {
    pending: Arc<AtomicU64>,
}

impl DemandBackpressure {
    /// Creates a new demand-based backpressure pair with initial demand of 0.
    ///
    /// Returns `(controller, handle)` where the controller is held by the
    /// dispatcher and the handle is given to the subscriber.
    #[must_use]
    pub fn new() -> (Self, DemandHandle) {
        let pending = Arc::new(AtomicU64::new(0));
        let handle = DemandHandle {
            pending: Arc::clone(&pending),
        };
        (Self { pending }, handle)
    }

    /// Attempts to consume one unit of demand.
    ///
    /// Returns `true` if demand was available (and decremented), `false`
    /// if pending demand was 0. Uses a CAS loop for lock-free correctness.
    #[inline]
    #[must_use]
    pub fn try_consume(&self) -> bool {
        loop {
            let current = self.pending.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            if self
                .pending
                .compare_exchange_weak(current, current - 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Returns the current pending demand.
    #[must_use]
    pub fn pending(&self) -> u64 {
        self.pending.load(Ordering::Acquire)
    }
}

impl DemandHandle {
    /// Requests `n` more events from the dispatcher.
    ///
    /// Atomically adds `n` to the pending demand counter.
    pub fn request(&self, n: u64) {
        self.pending.fetch_add(n, Ordering::Release);
    }

    /// Returns the current pending demand.
    #[must_use]
    pub fn pending(&self) -> u64 {
        self.pending.load(Ordering::Acquire)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- BackpressureController tests ---

    #[test]
    fn test_backpressure_drop_oldest() {
        let mut ctrl = BackpressureController::new(BackpressureStrategy::DropOldest);
        for _ in 0..100 {
            assert!(ctrl.should_deliver());
        }
        assert_eq!(ctrl.dropped(), 0);
    }

    #[test]
    fn test_backpressure_drop_newest() {
        let mut ctrl = BackpressureController::new(BackpressureStrategy::DropNewest);
        assert!(ctrl.should_deliver());
        ctrl.record_drop();
        ctrl.record_drop();
        assert_eq!(ctrl.dropped(), 2);
    }

    #[test]
    fn test_backpressure_sample() {
        let mut ctrl = BackpressureController::new(BackpressureStrategy::Sample(3));
        let mut delivered = Vec::new();
        for i in 0..12 {
            if ctrl.should_deliver() {
                delivered.push(i);
            }
        }
        assert_eq!(delivered, vec![2, 5, 8, 11]);
        assert_eq!(ctrl.dropped(), 8);
    }

    #[test]
    fn test_backpressure_controller_dropped_count() {
        let mut ctrl = BackpressureController::new(BackpressureStrategy::DropNewest);
        assert_eq!(ctrl.dropped(), 0);
        ctrl.record_drop();
        assert_eq!(ctrl.dropped(), 1);
        ctrl.record_drop();
        ctrl.record_drop();
        assert_eq!(ctrl.dropped(), 3);
    }

    #[test]
    fn test_backpressure_strategy_accessor() {
        let ctrl = BackpressureController::new(BackpressureStrategy::Block);
        assert_eq!(ctrl.strategy(), BackpressureStrategy::Block);
    }

    #[test]
    fn test_lagging_subscriber_detection() {
        let ctrl =
            BackpressureController::with_lag_threshold(BackpressureStrategy::DropOldest, 500);
        assert!(!ctrl.is_lagging(499));
        assert!(ctrl.is_lagging(500));
        assert!(ctrl.is_lagging(1000));
        assert_eq!(ctrl.lag_warning_threshold(), 500);
    }

    // --- DemandBackpressure tests ---

    #[test]
    fn test_backpressure_demand_request() {
        let (demand, handle) = DemandBackpressure::new();
        assert_eq!(demand.pending(), 0);

        handle.request(5);
        assert_eq!(demand.pending(), 5);

        for _ in 0..5 {
            assert!(demand.try_consume());
        }
        assert!(!demand.try_consume());
        assert_eq!(demand.pending(), 0);
    }

    #[test]
    fn test_backpressure_demand_zero() {
        let (demand, _handle) = DemandBackpressure::new();
        assert!(!demand.try_consume());
        assert!(!demand.try_consume());
    }

    #[test]
    fn test_backpressure_demand_concurrent() {
        let (demand, handle) = DemandBackpressure::new();
        let demand = Arc::new(demand);
        let handle = Arc::new(handle);

        let h = Arc::clone(&handle);
        let requester = std::thread::spawn(move || {
            for _ in 0..100 {
                h.request(100);
            }
        });

        let d = Arc::clone(&demand);
        let consumer = std::thread::spawn(move || {
            let mut consumed = 0u64;
            loop {
                if d.try_consume() {
                    consumed += 1;
                    if consumed == 10_000 {
                        break;
                    }
                }
                std::thread::yield_now();
            }
            consumed
        });

        requester.join().unwrap();
        let consumed = consumer.join().unwrap();
        assert_eq!(consumed, 10_000);
        assert_eq!(demand.pending(), 0);
    }

    #[test]
    fn test_demand_handle_clone() {
        let (demand, handle1) = DemandBackpressure::new();
        let handle2 = handle1.clone();
        handle1.request(3);
        handle2.request(2);
        assert_eq!(demand.pending(), 5);
    }
}
