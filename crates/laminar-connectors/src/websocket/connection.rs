//! `WebSocket` connection management: reconnection and failover.
//!
//! Provides exponential backoff with jitter for reconnection attempts
//! and multi-URL failover support.

use std::time::Duration;

use tracing::{debug, warn};

use super::source_config::ReconnectConfig;
use crate::retry::Backoff;

pub(super) fn redact_url(url: &str) -> String {
    crate::security::sanitize_identity_value("url", url)
}

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
    /// Shared connector backoff schedule.
    backoff: Backoff,
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
        let backoff = Backoff::new(config.initial_delay, config.max_delay, 0.25);
        Self {
            config,
            urls,
            current_url_index: 0,
            attempt: 0,
            backoff,
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

    /// Returns whether the maximum retry count has been exceeded.
    #[must_use]
    fn max_retries_exceeded(&self) -> bool {
        self.config
            .max_retries
            .is_some_and(|max| self.attempt >= max)
    }

    /// Resets the retry state after a successful connection.
    pub fn reset(&mut self) {
        self.attempt = 0;
        debug!(url = %redact_url(self.current_url()), "connection established, reset retry state");
    }

    /// Computes the next backoff delay and advances the failover state.
    ///
    /// Returns `None` if reconnection is disabled or max retries exceeded.
    /// Otherwise returns the duration to wait before the next attempt.
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

        let delay = self.backoff.delay(self.attempt);
        self.attempt += 1;

        // Cycle to the next URL for failover.
        if self.urls.len() > 1 {
            self.current_url_index = (self.current_url_index + 1) % self.urls.len();
        }

        warn!(
            attempt = self.attempt,
            delay_ms = delay.as_millis(),
            next_url = %redact_url(self.current_url()),
            "scheduling reconnection attempt"
        );

        Some(delay)
    }
}

impl std::fmt::Debug for ConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let current_url = redact_url(self.current_url());
        f.debug_struct("ConnectionManager")
            .field("current_url", &current_url)
            .field("attempt", &self.attempt)
            .field("urls", &self.urls.len())
            .field("reconnect_enabled", &self.config.enabled)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
