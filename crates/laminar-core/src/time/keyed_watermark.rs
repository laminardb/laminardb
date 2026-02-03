//! # Keyed Watermark Tracking
//!
//! Per-key watermark tracking to achieve 99%+ data accuracy compared to 63-67%
//! with traditional global watermarks. This addresses the fundamental problem of
//! fast-moving keys causing late data drops for slower keys.
//!
//! ## Problem with Global Watermarks
//!
//! ```text
//! Global Watermark Scenario (Traditional):
//! ┌─────────────────────────────────────────────────────────────────┐
//! │              Global Watermark = 10:05 (from Key C)              │
//! │                                                                 │
//! │  Key A: ████████████░░░░░░ events at 10:03 → DROPPED (late!)   │
//! │  Key B: ██████░░░░░░░░░░░░ events at 10:01 → DROPPED (late!)   │
//! │  Key C: █████████████████ events at 10:08 → OK                 │
//! │                                                                 │
//! │  Result: Fast-moving Key C advances watermark, slow keys       │
//! │          have their valid events dropped as "late"             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Keyed Watermarks Solution
//!
//! ```text
//! Keyed Watermark Scenario (This Feature):
//! ┌─────────────────────────────────────────────────────────────────┐
//! │             Per-Key Watermarks                                  │
//! │                                                                 │
//! │  Key A: watermark = 10:02 → events at 10:03 → OK               │
//! │  Key B: watermark = 10:00 → events at 10:01 → OK               │
//! │  Key C: watermark = 10:07 → events at 10:08 → OK               │
//! │                                                                 │
//! │  Global (for ordering): min(A,B,C) = 10:00                     │
//! │                                                                 │
//! │  Result: Each key tracks its own progress independently        │
//! │          99%+ accuracy vs 63-67% with global                   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust
//! use laminar_core::time::{KeyedWatermarkTracker, KeyedWatermarkConfig, Watermark};
//! use std::time::Duration;
//!
//! let config = KeyedWatermarkConfig {
//!     bounded_delay: Duration::from_secs(5),
//!     idle_timeout: Duration::from_secs(60),
//!     ..Default::default()
//! };
//!
//! let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);
//!
//! // Fast tenant advances quickly
//! tracker.update("tenant_a".to_string(), 10_000);
//! tracker.update("tenant_a".to_string(), 15_000);
//!
//! // Slow tenant at earlier time
//! tracker.update("tenant_b".to_string(), 5_000);
//!
//! // Per-key watermarks
//! assert_eq!(tracker.watermark_for_key(&"tenant_a".to_string()), Some(10_000)); // 15000 - 5000
//! assert_eq!(tracker.watermark_for_key(&"tenant_b".to_string()), Some(0));      // 5000 - 5000
//!
//! // Global watermark is minimum across active keys
//! assert_eq!(tracker.global_watermark(), Some(Watermark::new(0))); // min of 10000, 0
//!
//! // Events for tenant_b at 3000 are NOT late (their key watermark allows it)
//! assert!(!tracker.is_late(&"tenant_b".to_string(), 3000));
//!
//! // But events for tenant_a at 3000 ARE late (their key watermark is 10000)
//! assert!(tracker.is_late(&"tenant_a".to_string(), 3000));
//! ```

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use super::Watermark;

/// Per-key watermark state.
///
/// Tracks the maximum event time seen for a key and computes its watermark
/// using bounded out-of-orderness.
#[derive(Debug, Clone)]
pub struct KeyWatermarkState {
    /// Maximum event time seen for this key
    pub max_event_time: i64,
    /// Current watermark (`max_event_time - bounded_delay`)
    pub watermark: i64,
    /// Last activity time (wall clock)
    pub last_activity: Instant,
    /// Whether this key is marked idle
    pub is_idle: bool,
}

impl KeyWatermarkState {
    /// Creates a new key watermark state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            max_event_time: i64::MIN,
            watermark: i64::MIN,
            last_activity: Instant::now(),
            is_idle: false,
        }
    }

    /// Updates state with a new event timestamp.
    ///
    /// Returns `true` if the watermark advanced.
    #[inline]
    pub fn update(&mut self, event_time: i64, bounded_delay: i64) -> bool {
        self.last_activity = Instant::now();
        self.is_idle = false;

        if event_time > self.max_event_time {
            self.max_event_time = event_time;
            let new_watermark = event_time.saturating_sub(bounded_delay);
            if new_watermark > self.watermark {
                self.watermark = new_watermark;
                return true;
            }
        }
        false
    }

    /// Checks if an event is late relative to this key's watermark.
    #[inline]
    #[must_use]
    pub fn is_late(&self, event_time: i64) -> bool {
        event_time < self.watermark
    }
}

impl Default for KeyWatermarkState {
    fn default() -> Self {
        Self::new()
    }
}

/// Keyed watermark tracker configuration.
#[derive(Debug, Clone)]
pub struct KeyedWatermarkConfig {
    /// Maximum out-of-orderness for watermark calculation (in milliseconds)
    pub bounded_delay: Duration,
    /// Timeout before marking a key as idle
    pub idle_timeout: Duration,
    /// Maximum number of keys to track (for memory bounds)
    pub max_keys: Option<usize>,
    /// Eviction policy when `max_keys` reached
    pub eviction_policy: KeyEvictionPolicy,
}

impl Default for KeyedWatermarkConfig {
    fn default() -> Self {
        Self {
            bounded_delay: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            max_keys: None,
            eviction_policy: KeyEvictionPolicy::LeastRecentlyActive,
        }
    }
}

impl KeyedWatermarkConfig {
    /// Creates a new configuration with the specified bounded delay.
    #[must_use]
    pub fn with_bounded_delay(bounded_delay: Duration) -> Self {
        Self {
            bounded_delay,
            ..Default::default()
        }
    }

    /// Sets the idle timeout.
    #[must_use]
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Sets the maximum number of keys to track.
    #[must_use]
    pub fn with_max_keys(mut self, max_keys: usize) -> Self {
        self.max_keys = Some(max_keys);
        self
    }

    /// Sets the eviction policy.
    #[must_use]
    pub fn with_eviction_policy(mut self, policy: KeyEvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }
}

/// Policy for evicting keys when `max_keys` is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyEvictionPolicy {
    /// Evict key with oldest `last_activity`
    #[default]
    LeastRecentlyActive,
    /// Evict key with lowest watermark
    LowestWatermark,
    /// Reject new keys (return error)
    RejectNew,
}

/// Errors that can occur in keyed watermark operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum KeyedWatermarkError {
    /// Maximum number of keys reached with `RejectNew` policy
    #[error("Maximum keys reached ({max_keys}), cannot add new key")]
    MaxKeysReached {
        /// Maximum keys configured
        max_keys: usize,
    },
}

/// Metrics for keyed watermark tracking.
#[derive(Debug, Clone, Default)]
pub struct KeyedWatermarkMetrics {
    /// Total unique keys tracked
    pub total_keys: usize,
    /// Currently active (non-idle) keys
    pub active_keys: usize,
    /// Idle keys
    pub idle_keys: usize,
    /// Keys evicted due to `max_keys` limit
    pub evicted_keys: u64,
    /// Global watermark advancements
    pub global_advances: u64,
    /// Per-key watermark advancements
    pub key_advances: u64,
}

impl KeyedWatermarkMetrics {
    /// Creates new metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Tracks watermarks per logical key.
///
/// Provides fine-grained watermark tracking for multi-tenant workloads
/// and scenarios with significant event-time skew between keys.
///
/// # Research Background
///
/// Based on research (March 2025), keyed watermarks achieve
/// **99%+ accuracy** compared to **63-67%** with global watermarks.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{KeyedWatermarkTracker, KeyedWatermarkConfig, Watermark};
/// use std::time::Duration;
///
/// let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_secs(5));
/// let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);
///
/// // Fast tenant advances quickly
/// tracker.update("tenant_a".to_string(), 10_000);
/// tracker.update("tenant_a".to_string(), 15_000);
///
/// // Slow tenant at earlier time
/// tracker.update("tenant_b".to_string(), 5_000);
///
/// // Per-key watermarks differ
/// assert_eq!(tracker.watermark_for_key(&"tenant_a".to_string()), Some(10_000));
/// assert_eq!(tracker.watermark_for_key(&"tenant_b".to_string()), Some(0));
///
/// // Global watermark is minimum
/// assert_eq!(tracker.global_watermark(), Some(Watermark::new(0)));
/// ```
#[derive(Debug)]
pub struct KeyedWatermarkTracker<K: Hash + Eq + Clone> {
    /// Per-key watermark state
    key_states: HashMap<K, KeyWatermarkState>,

    /// Global watermark (minimum across all active keys)
    global_watermark: i64,

    /// Configuration
    config: KeyedWatermarkConfig,

    /// Bounded delay in milliseconds (cached from config)
    bounded_delay_ms: i64,

    /// Metrics
    metrics: KeyedWatermarkMetrics,
}

impl<K: Hash + Eq + Clone> KeyedWatermarkTracker<K> {
    /// Creates a new keyed watermark tracker with the given configuration.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Duration.as_millis() fits i64 for practical values
    pub fn new(config: KeyedWatermarkConfig) -> Self {
        let bounded_delay_ms = config.bounded_delay.as_millis() as i64;
        Self {
            key_states: HashMap::new(),
            global_watermark: i64::MIN,
            config,
            bounded_delay_ms,
            metrics: KeyedWatermarkMetrics::new(),
        }
    }

    /// Creates a tracker with default configuration.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(KeyedWatermarkConfig::default())
    }

    /// Updates the watermark for a specific key.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(Watermark))` if the global watermark changes
    /// - `Ok(None)` if no global change
    ///
    /// # Errors
    ///
    /// Returns `KeyedWatermarkError::MaxKeysReached` if `max_keys` is reached
    /// and the `RejectNew` eviction policy is configured.
    ///
    /// # Example
    ///
    /// ```rust
    /// use laminar_core::time::{KeyedWatermarkTracker, KeyedWatermarkConfig};
    /// use std::time::Duration;
    ///
    /// let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(100));
    /// let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);
    ///
    /// // First update creates the key
    /// let wm = tracker.update("key1".to_string(), 1000).unwrap();
    /// assert!(wm.is_some()); // Global watermark advances
    /// ```
    #[allow(clippy::missing_panics_doc)] // Internal invariant: key is always present after insert
    #[allow(clippy::needless_pass_by_value)] // Key must be owned for HashMap insertion
    pub fn update(
        &mut self,
        key: K,
        event_time: i64,
    ) -> Result<Option<Watermark>, KeyedWatermarkError> {
        // Check if we need to create a new key
        if !self.key_states.contains_key(&key) {
            // Check max_keys limit
            if let Some(max_keys) = self.config.max_keys {
                if self.key_states.len() >= max_keys {
                    match self.config.eviction_policy {
                        KeyEvictionPolicy::RejectNew => {
                            return Err(KeyedWatermarkError::MaxKeysReached { max_keys });
                        }
                        KeyEvictionPolicy::LeastRecentlyActive => {
                            self.evict_least_recently_active();
                        }
                        KeyEvictionPolicy::LowestWatermark => {
                            self.evict_lowest_watermark();
                        }
                    }
                }
            }
            self.key_states
                .insert(key.clone(), KeyWatermarkState::new());
            self.metrics.total_keys = self.key_states.len();
        }

        // Update the key's watermark
        let state = self.key_states.get_mut(&key).expect("key just inserted");
        let watermark_advanced = state.update(event_time, self.bounded_delay_ms);

        if watermark_advanced {
            self.metrics.key_advances += 1;
        }

        // Update metrics
        self.update_metrics_counts();

        // Try to advance global watermark
        Ok(self.try_advance_global())
    }

    /// Batch update for multiple events (more efficient).
    ///
    /// Returns the new global watermark if it changed.
    ///
    /// # Errors
    ///
    /// Returns `KeyedWatermarkError::MaxKeysReached` if `max_keys` is reached
    /// and the `RejectNew` eviction policy is configured.
    #[allow(clippy::missing_panics_doc)] // Internal invariant: key is always present after insert
    pub fn update_batch(
        &mut self,
        events: &[(K, i64)],
    ) -> Result<Option<Watermark>, KeyedWatermarkError> {
        for (key, event_time) in events {
            // Check if we need to create a new key
            if !self.key_states.contains_key(key) {
                if let Some(max_keys) = self.config.max_keys {
                    if self.key_states.len() >= max_keys {
                        match self.config.eviction_policy {
                            KeyEvictionPolicy::RejectNew => {
                                return Err(KeyedWatermarkError::MaxKeysReached { max_keys });
                            }
                            KeyEvictionPolicy::LeastRecentlyActive => {
                                self.evict_least_recently_active();
                            }
                            KeyEvictionPolicy::LowestWatermark => {
                                self.evict_lowest_watermark();
                            }
                        }
                    }
                }
                self.key_states
                    .insert(key.clone(), KeyWatermarkState::new());
            }

            let state = self.key_states.get_mut(key).expect("key just inserted");
            if state.update(*event_time, self.bounded_delay_ms) {
                self.metrics.key_advances += 1;
            }
        }

        self.metrics.total_keys = self.key_states.len();
        self.update_metrics_counts();

        Ok(self.try_advance_global())
    }

    /// Returns the watermark for a specific key.
    #[must_use]
    pub fn watermark_for_key(&self, key: &K) -> Option<i64> {
        self.key_states.get(key).map(|s| s.watermark)
    }

    /// Returns the global watermark (minimum across active keys).
    #[must_use]
    pub fn global_watermark(&self) -> Option<Watermark> {
        if self.global_watermark == i64::MIN {
            None
        } else {
            Some(Watermark::new(self.global_watermark))
        }
    }

    /// Checks if an event is late for its key.
    ///
    /// Uses the key's individual watermark, not the global watermark.
    /// If the key doesn't exist, returns `false` (not late).
    #[must_use]
    pub fn is_late(&self, key: &K, event_time: i64) -> bool {
        self.key_states
            .get(key)
            .is_some_and(|s| s.is_late(event_time))
    }

    /// Checks if an event is late using the global watermark.
    ///
    /// Use this for cross-key ordering guarantees.
    #[must_use]
    pub fn is_late_global(&self, event_time: i64) -> bool {
        event_time < self.global_watermark
    }

    /// Marks a key as idle, excluding it from global watermark calculation.
    ///
    /// Returns `Some(Watermark)` if the global watermark advances.
    pub fn mark_idle(&mut self, key: &K) -> Option<Watermark> {
        if let Some(state) = self.key_states.get_mut(key) {
            if !state.is_idle {
                state.is_idle = true;
                self.update_metrics_counts();
                return self.try_advance_global();
            }
        }
        None
    }

    /// Marks a key as active again.
    pub fn mark_active(&mut self, key: &K) {
        if let Some(state) = self.key_states.get_mut(key) {
            if state.is_idle {
                state.is_idle = false;
                state.last_activity = Instant::now();
                self.update_metrics_counts();
            }
        }
    }

    /// Checks for keys that have been idle longer than the timeout.
    ///
    /// Should be called periodically from Ring 1.
    ///
    /// Returns `Some(Watermark)` if marking idle keys causes the global watermark to advance.
    pub fn check_idle_keys(&mut self) -> Option<Watermark> {
        let idle_timeout = self.config.idle_timeout;
        let mut any_marked = false;

        for state in self.key_states.values_mut() {
            if !state.is_idle && state.last_activity.elapsed() >= idle_timeout {
                state.is_idle = true;
                any_marked = true;
            }
        }

        if any_marked {
            self.update_metrics_counts();
            self.try_advance_global()
        } else {
            None
        }
    }

    /// Returns the number of active (non-idle) keys.
    #[must_use]
    pub fn active_key_count(&self) -> usize {
        self.key_states.values().filter(|s| !s.is_idle).count()
    }

    /// Returns the total number of tracked keys.
    #[must_use]
    pub fn total_key_count(&self) -> usize {
        self.key_states.len()
    }

    /// Returns metrics.
    #[must_use]
    pub fn metrics(&self) -> &KeyedWatermarkMetrics {
        &self.metrics
    }

    /// Forces recalculation of global watermark.
    ///
    /// Useful after bulk operations or recovery.
    pub fn recalculate_global(&mut self) -> Option<Watermark> {
        let new_global = self.calculate_global();
        if new_global != i64::MAX && new_global != i64::MIN {
            self.global_watermark = new_global;
            Some(Watermark::new(new_global))
        } else {
            None
        }
    }

    /// Removes a key from tracking.
    ///
    /// Returns the key's watermark state if it existed.
    pub fn remove_key(&mut self, key: &K) -> Option<KeyWatermarkState> {
        let state = self.key_states.remove(key);
        if state.is_some() {
            self.metrics.total_keys = self.key_states.len();
            self.update_metrics_counts();
            // Recalculate global since we removed a key
            let new_global = self.calculate_global();
            if new_global > self.global_watermark && new_global != i64::MAX {
                self.global_watermark = new_global;
            }
        }
        state
    }

    /// Clears all tracked keys.
    pub fn clear(&mut self) {
        self.key_states.clear();
        self.global_watermark = i64::MIN;
        self.metrics = KeyedWatermarkMetrics::new();
    }

    /// Returns the state for a specific key.
    #[must_use]
    pub fn key_state(&self, key: &K) -> Option<&KeyWatermarkState> {
        self.key_states.get(key)
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &KeyedWatermarkConfig {
        &self.config
    }

    /// Checks if a key exists in the tracker.
    #[must_use]
    pub fn contains_key(&self, key: &K) -> bool {
        self.key_states.contains_key(key)
    }

    /// Returns an iterator over all keys.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.key_states.keys()
    }

    /// Returns an iterator over all key-state pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &KeyWatermarkState)> {
        self.key_states.iter()
    }

    /// Returns the bounded delay in milliseconds.
    #[must_use]
    pub fn bounded_delay_ms(&self) -> i64 {
        self.bounded_delay_ms
    }

    /// Tries to advance the global watermark.
    ///
    /// Note: Unlike partition-based tracking where global only advances,
    /// keyed watermarks can regress when new keys are added (since a new
    /// key starts with MIN watermark). This is intentional - the global
    /// watermark always reflects the minimum across active keys.
    fn try_advance_global(&mut self) -> Option<Watermark> {
        let new_global = self.calculate_global();

        if new_global != i64::MAX && new_global != i64::MIN && new_global != self.global_watermark {
            let old_global = self.global_watermark;
            self.global_watermark = new_global;
            if new_global > old_global {
                self.metrics.global_advances += 1;
            }
            Some(Watermark::new(new_global))
        } else {
            None
        }
    }

    /// Calculates the global watermark.
    fn calculate_global(&self) -> i64 {
        let mut min_watermark = i64::MAX;
        let mut has_active = false;

        for state in self.key_states.values() {
            if !state.is_idle {
                has_active = true;
                min_watermark = min_watermark.min(state.watermark);
            }
        }

        // If all keys are idle, use the max watermark to allow progress
        if !has_active {
            min_watermark = self
                .key_states
                .values()
                .map(|s| s.watermark)
                .max()
                .unwrap_or(i64::MIN);
        }

        min_watermark
    }

    /// Updates the metrics counts.
    fn update_metrics_counts(&mut self) {
        self.metrics.idle_keys = self.key_states.values().filter(|s| s.is_idle).count();
        self.metrics.active_keys = self.metrics.total_keys - self.metrics.idle_keys;
    }

    /// Evicts the least recently active key.
    fn evict_least_recently_active(&mut self) {
        if let Some(key_to_evict) = self
            .key_states
            .iter()
            .min_by_key(|(_, state)| state.last_activity)
            .map(|(k, _)| k.clone())
        {
            self.key_states.remove(&key_to_evict);
            self.metrics.evicted_keys += 1;
        }
    }

    /// Evicts the key with the lowest watermark.
    fn evict_lowest_watermark(&mut self) {
        if let Some(key_to_evict) = self
            .key_states
            .iter()
            .min_by_key(|(_, state)| state.watermark)
            .map(|(k, _)| k.clone())
        {
            self.key_states.remove(&key_to_evict);
            self.metrics.evicted_keys += 1;
        }
    }
}

/// Keyed watermark tracker with late event handling.
///
/// Wraps `KeyedWatermarkTracker` and provides utilities for handling late events,
/// including counting and optional side-output.
#[derive(Debug)]
pub struct KeyedWatermarkTrackerWithLateHandling<K: Hash + Eq + Clone> {
    /// Inner tracker
    tracker: KeyedWatermarkTracker<K>,
    /// Count of late events per key
    late_events_per_key: HashMap<K, u64>,
    /// Total late events
    total_late_events: u64,
}

impl<K: Hash + Eq + Clone> KeyedWatermarkTrackerWithLateHandling<K> {
    /// Creates a new tracker with late event handling.
    #[must_use]
    pub fn new(config: KeyedWatermarkConfig) -> Self {
        Self {
            tracker: KeyedWatermarkTracker::new(config),
            late_events_per_key: HashMap::new(),
            total_late_events: 0,
        }
    }

    /// Updates the watermark and checks for late events.
    ///
    /// Returns `(watermark_result, is_late)`.
    ///
    /// # Errors
    ///
    /// Returns `KeyedWatermarkError::MaxKeysReached` if the maximum number of keys
    /// is reached and the eviction policy is `RejectNew`.
    pub fn update_with_late_check(
        &mut self,
        key: K,
        event_time: i64,
    ) -> Result<(Option<Watermark>, bool), KeyedWatermarkError> {
        let is_late = self.tracker.is_late(&key, event_time);

        if is_late {
            *self.late_events_per_key.entry(key.clone()).or_insert(0) += 1;
            self.total_late_events += 1;
        }

        let wm = self.tracker.update(key, event_time)?;
        Ok((wm, is_late))
    }

    /// Returns late event count for a key.
    #[must_use]
    pub fn late_events_for_key(&self, key: &K) -> u64 {
        self.late_events_per_key.get(key).copied().unwrap_or(0)
    }

    /// Returns total late event count.
    #[must_use]
    pub fn total_late_events(&self) -> u64 {
        self.total_late_events
    }

    /// Returns a reference to the inner tracker.
    #[must_use]
    pub fn inner(&self) -> &KeyedWatermarkTracker<K> {
        &self.tracker
    }

    /// Returns a mutable reference to the inner tracker.
    pub fn inner_mut(&mut self) -> &mut KeyedWatermarkTracker<K> {
        &mut self.tracker
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_watermark_state_creation() {
        let state = KeyWatermarkState::new();
        assert_eq!(state.max_event_time, i64::MIN);
        assert_eq!(state.watermark, i64::MIN);
        assert!(!state.is_idle);
    }

    #[test]
    fn test_key_watermark_state_update() {
        let mut state = KeyWatermarkState::new();

        // First update
        let advanced = state.update(1000, 100);
        assert!(advanced);
        assert_eq!(state.max_event_time, 1000);
        assert_eq!(state.watermark, 900); // 1000 - 100

        // Out-of-order event
        let advanced = state.update(800, 100);
        assert!(!advanced);
        assert_eq!(state.max_event_time, 1000); // Unchanged

        // New max
        let advanced = state.update(1500, 100);
        assert!(advanced);
        assert_eq!(state.watermark, 1400);
    }

    #[test]
    fn test_key_watermark_state_is_late() {
        let mut state = KeyWatermarkState::new();
        state.update(1000, 100); // watermark = 900

        assert!(state.is_late(800)); // Before watermark
        assert!(state.is_late(899)); // Just before watermark
        assert!(!state.is_late(900)); // At watermark
        assert!(!state.is_late(1000)); // After watermark
    }

    #[test]
    fn test_config_defaults() {
        let config = KeyedWatermarkConfig::default();
        assert_eq!(config.bounded_delay, Duration::from_secs(5));
        assert_eq!(config.idle_timeout, Duration::from_secs(60));
        assert!(config.max_keys.is_none());
        assert_eq!(
            config.eviction_policy,
            KeyEvictionPolicy::LeastRecentlyActive
        );
    }

    #[test]
    fn test_config_builder() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_secs(10))
            .with_idle_timeout(Duration::from_secs(30))
            .with_max_keys(1000)
            .with_eviction_policy(KeyEvictionPolicy::LowestWatermark);

        assert_eq!(config.bounded_delay, Duration::from_secs(10));
        assert_eq!(config.idle_timeout, Duration::from_secs(30));
        assert_eq!(config.max_keys, Some(1000));
        assert_eq!(config.eviction_policy, KeyEvictionPolicy::LowestWatermark);
    }

    #[test]
    fn test_keyed_tracker_single_key_updates_watermark() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(100));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        let wm = tracker.update("key1".to_string(), 1000).unwrap();
        assert_eq!(wm, Some(Watermark::new(900)));
        assert_eq!(tracker.watermark_for_key(&"key1".to_string()), Some(900));
        assert_eq!(tracker.global_watermark(), Some(Watermark::new(900)));
    }

    #[test]
    fn test_keyed_tracker_multiple_keys_independent_watermarks() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(100));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("fast".to_string(), 5000).unwrap();
        tracker.update("slow".to_string(), 1000).unwrap();

        // Each key has its own watermark
        assert_eq!(tracker.watermark_for_key(&"fast".to_string()), Some(4900));
        assert_eq!(tracker.watermark_for_key(&"slow".to_string()), Some(900));

        // Global is minimum
        assert_eq!(tracker.global_watermark(), Some(Watermark::new(900)));
    }

    #[test]
    fn test_keyed_tracker_global_is_minimum_of_active_keys() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("a".to_string(), 5000).unwrap();
        tracker.update("b".to_string(), 3000).unwrap();
        tracker.update("c".to_string(), 7000).unwrap();

        assert_eq!(tracker.global_watermark(), Some(Watermark::new(3000)));
    }

    #[test]
    fn test_keyed_tracker_fast_key_does_not_affect_slow_key() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(100));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        // Slow key starts first
        tracker.update("slow".to_string(), 1000).unwrap();

        // Fast key advances rapidly
        tracker.update("fast".to_string(), 5000).unwrap();
        tracker.update("fast".to_string(), 10000).unwrap();

        // Slow key's watermark is independent
        assert_eq!(tracker.watermark_for_key(&"slow".to_string()), Some(900));

        // Event at 950 is not late for slow key
        assert!(!tracker.is_late(&"slow".to_string(), 950));

        // But it would be very late for fast key
        assert!(tracker.is_late(&"fast".to_string(), 950));
    }

    #[test]
    fn test_keyed_tracker_is_late_uses_key_watermark_not_global() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(100));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("fast".to_string(), 10000).unwrap(); // watermark = 9900
        tracker.update("slow".to_string(), 1000).unwrap(); // watermark = 900

        // Global is 900, but we use per-key watermarks
        assert_eq!(tracker.global_watermark(), Some(Watermark::new(900)));

        // Event at 5000 is NOT late for slow key (5000 >= 900)
        assert!(!tracker.is_late(&"slow".to_string(), 5000));

        // Event at 5000 IS late for fast key (5000 < 9900)
        assert!(tracker.is_late(&"fast".to_string(), 5000));
    }

    #[test]
    fn test_keyed_tracker_idle_key_excluded_from_global() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("fast".to_string(), 5000).unwrap();
        tracker.update("slow".to_string(), 1000).unwrap();

        assert_eq!(tracker.global_watermark(), Some(Watermark::new(1000)));

        // Mark slow key as idle
        let wm = tracker.mark_idle(&"slow".to_string());
        assert_eq!(wm, Some(Watermark::new(5000)));
        assert_eq!(tracker.global_watermark(), Some(Watermark::new(5000)));
    }

    #[test]
    fn test_keyed_tracker_all_idle_uses_max() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("a".to_string(), 5000).unwrap();
        tracker.update("b".to_string(), 3000).unwrap();

        tracker.mark_idle(&"a".to_string());
        let wm = tracker.mark_idle(&"b".to_string());

        // When all idle, use max to allow progress
        assert_eq!(wm, Some(Watermark::new(5000)));
    }

    #[test]
    fn test_keyed_tracker_key_eviction_lru() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0))
            .with_max_keys(2)
            .with_eviction_policy(KeyEvictionPolicy::LeastRecentlyActive);

        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("a".to_string(), 1000).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        tracker.update("b".to_string(), 2000).unwrap();

        assert_eq!(tracker.total_key_count(), 2);

        // Adding third key should evict "a" (least recently active)
        tracker.update("c".to_string(), 3000).unwrap();

        assert_eq!(tracker.total_key_count(), 2);
        assert!(!tracker.contains_key(&"a".to_string())); // Evicted
        assert!(tracker.contains_key(&"b".to_string()));
        assert!(tracker.contains_key(&"c".to_string()));
        assert_eq!(tracker.metrics().evicted_keys, 1);
    }

    #[test]
    fn test_keyed_tracker_key_eviction_lowest_watermark() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0))
            .with_max_keys(2)
            .with_eviction_policy(KeyEvictionPolicy::LowestWatermark);

        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("high".to_string(), 5000).unwrap();
        tracker.update("low".to_string(), 1000).unwrap();

        // Adding third key should evict "low" (lowest watermark)
        tracker.update("mid".to_string(), 3000).unwrap();

        assert!(!tracker.contains_key(&"low".to_string())); // Evicted
        assert!(tracker.contains_key(&"high".to_string()));
        assert!(tracker.contains_key(&"mid".to_string()));
    }

    #[test]
    fn test_keyed_tracker_key_eviction_reject_new() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0))
            .with_max_keys(2)
            .with_eviction_policy(KeyEvictionPolicy::RejectNew);

        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("a".to_string(), 1000).unwrap();
        tracker.update("b".to_string(), 2000).unwrap();

        // Third key should be rejected
        let result = tracker.update("c".to_string(), 3000);
        assert!(matches!(
            result,
            Err(KeyedWatermarkError::MaxKeysReached { max_keys: 2 })
        ));

        // Existing keys still work
        assert!(tracker.update("a".to_string(), 1500).is_ok());
    }

    #[test]
    fn test_keyed_tracker_batch_update_efficient() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(100));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        let events = vec![
            ("a".to_string(), 1000),
            ("b".to_string(), 2000),
            ("a".to_string(), 1500),
            ("c".to_string(), 3000),
        ];

        let wm = tracker.update_batch(&events).unwrap();
        assert!(wm.is_some());

        assert_eq!(tracker.total_key_count(), 3);
        assert_eq!(tracker.watermark_for_key(&"a".to_string()), Some(1400)); // 1500 - 100
        assert_eq!(tracker.watermark_for_key(&"b".to_string()), Some(1900));
        assert_eq!(tracker.watermark_for_key(&"c".to_string()), Some(2900));
    }

    #[test]
    fn test_keyed_tracker_remove_key_recalculates_global() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("fast".to_string(), 5000).unwrap();
        tracker.update("slow".to_string(), 1000).unwrap();

        assert_eq!(tracker.global_watermark(), Some(Watermark::new(1000)));

        // Remove slow key
        let state = tracker.remove_key(&"slow".to_string());
        assert!(state.is_some());
        assert_eq!(state.unwrap().watermark, 1000);

        // Global should now be 5000
        assert_eq!(tracker.global_watermark(), Some(Watermark::new(5000)));
    }

    #[test]
    fn test_keyed_tracker_check_idle_keys() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0))
            .with_idle_timeout(Duration::from_millis(10));

        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("fast".to_string(), 5000).unwrap();
        tracker.update("slow".to_string(), 1000).unwrap();

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(20));

        // Update only fast key
        tracker.update("fast".to_string(), 6000).unwrap();

        // Check for idle keys
        let wm = tracker.check_idle_keys();

        // slow should be marked idle
        assert!(tracker.key_state(&"slow".to_string()).unwrap().is_idle);

        // Global should advance
        assert!(wm.is_some() || tracker.global_watermark() == Some(Watermark::new(6000)));
    }

    #[test]
    fn test_keyed_tracker_metrics() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(100));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("a".to_string(), 1000).unwrap();
        tracker.update("b".to_string(), 2000).unwrap();
        tracker.update("a".to_string(), 1500).unwrap(); // Advances a's watermark

        let metrics = tracker.metrics();
        assert_eq!(metrics.total_keys, 2);
        assert_eq!(metrics.active_keys, 2);
        assert_eq!(metrics.idle_keys, 0);
        assert!(metrics.key_advances >= 3); // At least 3 watermark advances
        assert!(metrics.global_advances >= 1);

        tracker.mark_idle(&"b".to_string());

        let metrics = tracker.metrics();
        assert_eq!(metrics.active_keys, 1);
        assert_eq!(metrics.idle_keys, 1);
    }

    #[test]
    fn test_keyed_tracker_clear() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("a".to_string(), 1000).unwrap();
        tracker.update("b".to_string(), 2000).unwrap();

        tracker.clear();

        assert_eq!(tracker.total_key_count(), 0);
        assert_eq!(tracker.global_watermark(), None);
    }

    #[test]
    fn test_keyed_tracker_is_late_global() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("fast".to_string(), 5000).unwrap();
        tracker.update("slow".to_string(), 1000).unwrap();

        // Global watermark is 1000
        assert!(!tracker.is_late_global(1000));
        assert!(tracker.is_late_global(999));
    }

    #[test]
    fn test_keyed_tracker_iteration() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(0));
        let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);

        tracker.update("a".to_string(), 1000).unwrap();
        tracker.update("b".to_string(), 2000).unwrap();

        let keys: Vec<_> = tracker.keys().collect();
        assert_eq!(keys.len(), 2);

        let pairs: Vec<_> = tracker.iter().collect();
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn test_late_handling_tracker() {
        let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_millis(100));
        let mut tracker: KeyedWatermarkTrackerWithLateHandling<String> =
            KeyedWatermarkTrackerWithLateHandling::new(config);

        // First event
        let (wm, is_late) = tracker
            .update_with_late_check("key1".to_string(), 1000)
            .unwrap();
        assert!(wm.is_some());
        assert!(!is_late);

        // On-time event
        let (_, is_late) = tracker
            .update_with_late_check("key1".to_string(), 950)
            .unwrap();
        assert!(!is_late); // 950 >= 900 (watermark)

        // Late event
        let (_, is_late) = tracker
            .update_with_late_check("key1".to_string(), 800)
            .unwrap();
        assert!(is_late); // 800 < 900

        assert_eq!(tracker.late_events_for_key(&"key1".to_string()), 1);
        assert_eq!(tracker.total_late_events(), 1);
    }
}
