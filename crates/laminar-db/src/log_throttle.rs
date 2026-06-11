//! Rate gate for hot-path warnings.

use std::time::{Duration, Instant};

/// Allows an event through at most once per `interval`. Monotonic
/// (`Instant`-based), so an NTP step backwards can't suppress warnings
/// indefinitely the way the old `SystemTime` throttles could.
///
/// Usable as a `static`:
///
/// ```ignore
/// static THROTTLE: LogThrottle = LogThrottle::every(Duration::from_secs(10));
/// if THROTTLE.allow() {
///     tracing::warn!("...");
/// }
/// ```
pub(crate) struct LogThrottle {
    last: parking_lot::Mutex<Option<Instant>>,
    interval: Duration,
}

impl LogThrottle {
    pub(crate) const fn every(interval: Duration) -> Self {
        Self {
            last: parking_lot::Mutex::new(None),
            interval,
        }
    }

    /// `true` if the caller should emit its event now.
    pub(crate) fn allow(&self) -> bool {
        let mut last = self.last.lock();
        match *last {
            Some(at) if at.elapsed() < self.interval => false,
            _ => {
                *last = Some(Instant::now());
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_event_passes_then_throttles() {
        let t = LogThrottle::every(Duration::from_secs(60));
        assert!(t.allow());
        assert!(!t.allow());
    }
}
