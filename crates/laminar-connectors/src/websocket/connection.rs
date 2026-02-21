//! WebSocket connection management: reconnection, failover, heartbeat.
//!
//! Provides exponential backoff with jitter for reconnection attempts
//! and multi-URL failover support.

use std::time::Duration;

use tracing::{debug, warn};

use super::source_config::ReconnectConfig;

/// Manages WebSocket reconnection with exponential backoff and URL failover.
pub struct ConnectionManager {
    /// Reconnection configuration.
    config: ReconnectConfig,
    /// Available URLs for failover (source client mode).
    urls: Vec<String>,
    /// Index of the currently active URL.
    current_url_index: usize,
    /// Current retry attempt number.
    attempt: u32,
    /// Current backoff delay.
    current_delay: Duration,
}

impl ConnectionManager {
    /// Creates a new connection manager.
    ///
    /// # Arguments
    ///
    /// * `urls` - One or more WebSocket URLs for failover. First is primary.
    /// * `config` - Reconnection settings.
    #[must_use]
    pub fn new(urls: Vec<String>, config: ReconnectConfig) -> Self {
        let initial_delay = config.initial_delay;
        Self {
            config,
            urls,
            current_url_index: 0,
            attempt: 0,
            current_delay: initial_delay,
        }
    }

    /// Returns the currently active URL.
    #[must_use]
    pub fn current_url(&self) -> &str {
        &self.urls[self.current_url_index]
    }

    /// Returns the current retry attempt count.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns whether reconnection is enabled.
    #[must_use]
    pub fn reconnect_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Returns whether the maximum retry count has been exceeded.
    #[must_use]
    pub fn max_retries_exceeded(&self) -> bool {
        self.config
            .max_retries
            .is_some_and(|max| self.attempt >= max)
    }

    /// Resets the retry state after a successful connection.
    pub fn reset(&mut self) {
        self.attempt = 0;
        self.current_delay = self.config.initial_delay;
        debug!(url = %self.current_url(), "connection established, reset retry state");
    }

    /// Computes the next backoff delay and advances the failover state.
    ///
    /// Returns `None` if reconnection is disabled or max retries exceeded.
    /// Otherwise returns the duration to wait before the next attempt.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn next_backoff(&mut self) -> Option<Duration> {
        if !self.config.enabled {
            return None;
        }

        if self.max_retries_exceeded() {
            warn!(
                attempts = self.attempt,
                max = ?self.config.max_retries,
                "max reconnection retries exceeded"
            );
            return None;
        }

        self.attempt += 1;

        // Cycle to the next URL for failover.
        if self.urls.len() > 1 {
            self.current_url_index = (self.current_url_index + 1) % self.urls.len();
        }

        let delay = self.current_delay;

        // Apply jitter: ±25% of the delay.
        let delay = if self.config.jitter {
            let jitter_range = delay.as_millis() as f64 * 0.25;
            // Simple deterministic jitter based on attempt number.
            let jitter_offset =
                (f64::from(self.attempt) * 7.0 % jitter_range) - (jitter_range / 2.0);
            let jittered_ms = (delay.as_millis() as f64 + jitter_offset).max(1.0);
            Duration::from_millis(jittered_ms as u64)
        } else {
            delay
        };

        // Increase delay for next attempt.
        let next_ms =
            (self.current_delay.as_millis() as f64 * self.config.backoff_multiplier) as u64;
        self.current_delay = Duration::from_millis(next_ms).min(self.config.max_delay);

        warn!(
            attempt = self.attempt,
            delay_ms = delay.as_millis(),
            next_url = %self.current_url(),
            "scheduling reconnection attempt"
        );

        Some(delay)
    }

    /// Returns a list of all configured URLs.
    #[must_use]
    pub fn urls(&self) -> &[String] {
        &self.urls
    }
}

impl std::fmt::Debug for ConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager")
            .field("current_url", &self.current_url())
            .field("attempt", &self.attempt)
            .field("urls", &self.urls.len())
            .field("reconnect_enabled", &self.config.enabled)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ReconnectConfig {
        ReconnectConfig {
            enabled: true,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            max_retries: None,
            jitter: false,
        }
    }

    #[test]
    fn test_current_url() {
        let mgr = ConnectionManager::new(vec!["ws://a".into(), "ws://b".into()], test_config());
        assert_eq!(mgr.current_url(), "ws://a");
    }

    #[test]
    fn test_failover_cycles_urls() {
        let mut mgr = ConnectionManager::new(
            vec!["ws://a".into(), "ws://b".into(), "ws://c".into()],
            test_config(),
        );

        mgr.next_backoff();
        assert_eq!(mgr.current_url(), "ws://b");

        mgr.next_backoff();
        assert_eq!(mgr.current_url(), "ws://c");

        mgr.next_backoff();
        assert_eq!(mgr.current_url(), "ws://a");
    }

    #[test]
    fn test_exponential_backoff() {
        let mut mgr = ConnectionManager::new(vec!["ws://a".into()], test_config());

        let d1 = mgr.next_backoff().unwrap();
        assert_eq!(d1, Duration::from_millis(100));

        let d2 = mgr.next_backoff().unwrap();
        assert_eq!(d2, Duration::from_millis(200));

        let d3 = mgr.next_backoff().unwrap();
        assert_eq!(d3, Duration::from_millis(400));
    }

    #[test]
    fn test_max_delay_cap() {
        let config = ReconnectConfig {
            enabled: true,
            initial_delay: Duration::from_secs(20),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            max_retries: None,
            jitter: false,
        };
        let mut mgr = ConnectionManager::new(vec!["ws://a".into()], config);

        mgr.next_backoff(); // 20s
        let d2 = mgr.next_backoff().unwrap(); // would be 40s, capped to 30s
        assert_eq!(d2, Duration::from_secs(30));
    }

    #[test]
    fn test_max_retries() {
        let config = ReconnectConfig {
            max_retries: Some(2),
            ..test_config()
        };
        let mut mgr = ConnectionManager::new(vec!["ws://a".into()], config);

        assert!(mgr.next_backoff().is_some()); // attempt 1
        assert!(mgr.next_backoff().is_some()); // attempt 2
        assert!(mgr.next_backoff().is_none()); // exceeded
    }

    #[test]
    fn test_reset() {
        let mut mgr = ConnectionManager::new(vec!["ws://a".into()], test_config());

        mgr.next_backoff();
        mgr.next_backoff();
        assert_eq!(mgr.attempt(), 2);

        mgr.reset();
        assert_eq!(mgr.attempt(), 0);

        let d = mgr.next_backoff().unwrap();
        assert_eq!(d, Duration::from_millis(100)); // reset to initial
    }

    #[test]
    fn test_disabled_reconnect() {
        let config = ReconnectConfig {
            enabled: false,
            ..test_config()
        };
        let mut mgr = ConnectionManager::new(vec!["ws://a".into()], config);
        assert!(mgr.next_backoff().is_none());
    }

    #[test]
    fn test_jitter_varies_delay() {
        let config = ReconnectConfig {
            jitter: true,
            ..test_config()
        };
        let mut mgr = ConnectionManager::new(vec!["ws://a".into()], config);

        let d1 = mgr.next_backoff().unwrap();
        // With jitter, delay should be approximately 100ms but not exactly
        assert!(d1.as_millis() > 0);
        assert!(d1.as_millis() <= 150); // within 25% + margin
    }
}
