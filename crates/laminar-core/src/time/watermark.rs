//! # Watermark Generation and Tracking
//!
//! Watermarks indicate event-time progress through a streaming system. They are assertions
//! that no events with timestamps earlier than the watermark are expected to arrive.
//!
//! ## Watermark Generation Strategies
//!
//! - [`BoundedOutOfOrdernessGenerator`]: Allows events to be late by a fixed duration
//! - [`AscendingTimestampsGenerator`]: Assumes strictly increasing timestamps (no lateness)
//! - [`PeriodicGenerator`]: Emits watermarks at fixed wall-clock intervals
//! - [`PunctuatedGenerator`]: Emits watermarks based on special marker events
//!
//! ## Multi-Source Alignment
//!
//! When processing multiple input streams (e.g., joins), use [`WatermarkTracker`] to
//! track the minimum watermark across all sources.
//!
//! ## Idle Source Handling
//!
//! Sources that stop producing events can block watermark progress. The
//! [`IdleSourceDetector`] marks sources as idle after a configurable timeout.
//!
//! # Example
//!
//! ```rust
//! use laminar_core::time::{
//!     WatermarkGenerator, BoundedOutOfOrdernessGenerator,
//!     WatermarkTracker, Watermark,
//! };
//!
//! // Single source with bounded out-of-orderness
//! let mut generator = BoundedOutOfOrdernessGenerator::new(1000); // 1 second
//! let wm = generator.on_event(5000);
//! assert_eq!(wm, Some(Watermark::new(4000)));
//!
//! // Multi-source tracking
//! let mut tracker = WatermarkTracker::new(2);
//! tracker.update_source(0, 5000);
//! tracker.update_source(1, 3000);
//! assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));
//! ```

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::Watermark;

/// Trait for generating watermarks from event timestamps.
///
/// Implementations track observed timestamps and produce watermarks that indicate
/// event-time progress. The watermark is an assertion that no events with timestamps
/// earlier than the watermark are expected.
pub trait WatermarkGenerator: Send {
    /// Process an event timestamp and potentially emit a new watermark.
    ///
    /// Called for each event processed. Returns `Some(watermark)` if the watermark
    /// should advance, `None` otherwise.
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark>;

    /// Called periodically to emit watermarks based on wall-clock time.
    ///
    /// Useful for generating watermarks even when no events are arriving.
    fn on_periodic(&mut self) -> Option<Watermark>;

    /// Returns the current watermark value without advancing it.
    fn current_watermark(&self) -> i64;

    /// Advances the watermark to at least the given timestamp from an external source.
    ///
    /// Called when the source provides an explicit watermark (e.g., `Source::watermark()`).
    /// Returns `Some(Watermark)` if the watermark advanced, `None` if the timestamp
    /// was not higher than the current watermark.
    fn advance_watermark(&mut self, timestamp: i64) -> Option<Watermark>;
}

/// Watermark generator with bounded out-of-orderness.
///
/// Allows events to arrive out of order by up to `max_out_of_orderness` milliseconds.
/// The watermark is always `max_timestamp_seen - max_out_of_orderness`.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{BoundedOutOfOrdernessGenerator, WatermarkGenerator, Watermark};
///
/// let mut gen = BoundedOutOfOrdernessGenerator::new(100); // 100ms lateness allowed
///
/// // First event at t=1000
/// assert_eq!(gen.on_event(1000), Some(Watermark::new(900)));
///
/// // Out-of-order event at t=800 - no watermark advance
/// assert_eq!(gen.on_event(800), None);
///
/// // New max at t=1200
/// assert_eq!(gen.on_event(1200), Some(Watermark::new(1100)));
/// ```
pub struct BoundedOutOfOrdernessGenerator {
    max_out_of_orderness: i64,
    current_max_timestamp: i64,
    current_watermark: i64,
}

impl BoundedOutOfOrdernessGenerator {
    /// Creates a new generator with the specified maximum out-of-orderness.
    ///
    /// # Arguments
    ///
    /// * `max_out_of_orderness` - Maximum allowed lateness in milliseconds
    #[must_use]
    pub fn new(max_out_of_orderness: i64) -> Self {
        Self {
            max_out_of_orderness,
            current_max_timestamp: i64::MIN,
            current_watermark: i64::MIN,
        }
    }

    /// Creates a new generator from a `Duration`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Duration.as_millis() fits i64 for practical values
    pub fn from_duration(max_out_of_orderness: Duration) -> Self {
        Self::new(max_out_of_orderness.as_millis() as i64)
    }

    /// Returns the maximum out-of-orderness in milliseconds.
    #[must_use]
    pub fn max_out_of_orderness(&self) -> i64 {
        self.max_out_of_orderness
    }
}

impl WatermarkGenerator for BoundedOutOfOrdernessGenerator {
    #[inline]
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark> {
        if timestamp > self.current_max_timestamp {
            self.current_max_timestamp = timestamp;
            let new_watermark = timestamp.saturating_sub(self.max_out_of_orderness);
            if new_watermark > self.current_watermark {
                self.current_watermark = new_watermark;
                return Some(Watermark::new(new_watermark));
            }
        }
        None
    }

    #[inline]
    fn on_periodic(&mut self) -> Option<Watermark> {
        // Bounded out-of-orderness doesn't emit periodic watermarks
        None
    }

    #[inline]
    fn current_watermark(&self) -> i64 {
        self.current_watermark
    }

    #[inline]
    fn advance_watermark(&mut self, timestamp: i64) -> Option<Watermark> {
        if timestamp > self.current_watermark {
            self.current_watermark = timestamp;
            // Maintain invariant: current_max_timestamp >= current_watermark + max_out_of_orderness
            let min_max = timestamp.saturating_add(self.max_out_of_orderness);
            if min_max > self.current_max_timestamp {
                self.current_max_timestamp = min_max;
            }
            Some(Watermark::new(timestamp))
        } else {
            None
        }
    }
}

/// Watermark generator for strictly ascending timestamps.
///
/// Assumes events arrive in timestamp order with no lateness. The watermark
/// equals the current timestamp. Use this for sources that guarantee ordering.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{AscendingTimestampsGenerator, WatermarkGenerator, Watermark};
///
/// let mut gen = AscendingTimestampsGenerator::new();
/// assert_eq!(gen.on_event(1000), Some(Watermark::new(1000)));
/// assert_eq!(gen.on_event(2000), Some(Watermark::new(2000)));
/// ```
#[derive(Debug, Default)]
pub struct AscendingTimestampsGenerator {
    current_watermark: i64,
}

impl AscendingTimestampsGenerator {
    /// Creates a new ascending timestamps generator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            current_watermark: i64::MIN,
        }
    }
}

impl WatermarkGenerator for AscendingTimestampsGenerator {
    #[inline]
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark> {
        if timestamp > self.current_watermark {
            self.current_watermark = timestamp;
            Some(Watermark::new(timestamp))
        } else {
            None
        }
    }

    #[inline]
    fn on_periodic(&mut self) -> Option<Watermark> {
        None
    }

    #[inline]
    fn current_watermark(&self) -> i64 {
        self.current_watermark
    }

    #[inline]
    fn advance_watermark(&mut self, timestamp: i64) -> Option<Watermark> {
        if timestamp > self.current_watermark {
            self.current_watermark = timestamp;
            Some(Watermark::new(timestamp))
        } else {
            None
        }
    }
}

/// Periodic watermark generator that emits at fixed wall-clock intervals.
///
/// Wraps another generator and emits watermarks periodically even when no
/// events are arriving. Useful for handling idle sources and ensuring
/// time-based windows eventually trigger.
///
/// # Example
///
/// ```rust,no_run
/// use laminar_core::time::{
///     PeriodicGenerator, BoundedOutOfOrdernessGenerator, WatermarkGenerator,
/// };
/// use std::time::Duration;
///
/// let inner = BoundedOutOfOrdernessGenerator::new(100);
/// let mut gen = PeriodicGenerator::new(inner, Duration::from_millis(500));
///
/// // First event
/// gen.on_event(1000);
///
/// // Later, periodic check may emit watermark
/// // (depends on wall-clock time elapsed)
/// let wm = gen.on_periodic();
/// ```
pub struct PeriodicGenerator<G: WatermarkGenerator> {
    inner: G,
    period: Duration,
    last_emit_time: Instant,
    last_emitted_watermark: i64,
}

impl<G: WatermarkGenerator> PeriodicGenerator<G> {
    /// Creates a new periodic generator wrapping another generator.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying watermark generator
    /// * `period` - How often to emit watermarks (wall-clock time)
    #[must_use]
    pub fn new(inner: G, period: Duration) -> Self {
        Self {
            inner,
            period,
            last_emit_time: Instant::now(),
            last_emitted_watermark: i64::MIN,
        }
    }

    /// Returns a reference to the inner generator.
    #[must_use]
    pub fn inner(&self) -> &G {
        &self.inner
    }

    /// Returns a mutable reference to the inner generator.
    pub fn inner_mut(&mut self) -> &mut G {
        &mut self.inner
    }
}

impl<G: WatermarkGenerator> WatermarkGenerator for PeriodicGenerator<G> {
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark> {
        let wm = self.inner.on_event(timestamp);
        if let Some(ref w) = wm {
            self.last_emitted_watermark = w.timestamp();
            self.last_emit_time = Instant::now();
        }
        wm
    }

    fn on_periodic(&mut self) -> Option<Watermark> {
        // Check if enough wall-clock time has passed
        if self.last_emit_time.elapsed() >= self.period {
            let current = self.inner.current_watermark();
            if current > self.last_emitted_watermark {
                self.last_emitted_watermark = current;
                self.last_emit_time = Instant::now();
                return Some(Watermark::new(current));
            }
            self.last_emit_time = Instant::now();
        }
        None
    }

    fn current_watermark(&self) -> i64 {
        self.inner.current_watermark()
    }

    fn advance_watermark(&mut self, timestamp: i64) -> Option<Watermark> {
        let wm = self.inner.advance_watermark(timestamp);
        if let Some(ref w) = wm {
            self.last_emitted_watermark = w.timestamp();
            self.last_emit_time = Instant::now();
        }
        wm
    }
}

/// Punctuated watermark generator that emits based on special events.
///
/// Uses a predicate function to identify watermark-carrying events. When the
/// predicate returns `Some(watermark)`, that watermark is emitted.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{PunctuatedGenerator, WatermarkGenerator, Watermark};
///
/// // Emit watermark on every 1000ms boundary
/// let mut gen = PunctuatedGenerator::new(|ts| {
///     if ts % 1000 == 0 {
///         Some(Watermark::new(ts))
///     } else {
///         None
///     }
/// });
///
/// assert_eq!(gen.on_event(999), None);
/// assert_eq!(gen.on_event(1000), Some(Watermark::new(1000)));
/// ```
pub struct PunctuatedGenerator<F>
where
    F: Fn(i64) -> Option<Watermark> + Send,
{
    predicate: F,
    current_watermark: i64,
}

impl<F> PunctuatedGenerator<F>
where
    F: Fn(i64) -> Option<Watermark> + Send,
{
    /// Creates a new punctuated generator with the given predicate.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Function that returns `Some(Watermark)` for watermark events
    #[must_use]
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            current_watermark: i64::MIN,
        }
    }
}

impl<F> WatermarkGenerator for PunctuatedGenerator<F>
where
    F: Fn(i64) -> Option<Watermark> + Send,
{
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark> {
        if let Some(wm) = (self.predicate)(timestamp) {
            if wm.timestamp() > self.current_watermark {
                self.current_watermark = wm.timestamp();
                return Some(wm);
            }
        }
        None
    }

    fn on_periodic(&mut self) -> Option<Watermark> {
        None
    }

    fn current_watermark(&self) -> i64 {
        self.current_watermark
    }

    fn advance_watermark(&mut self, timestamp: i64) -> Option<Watermark> {
        if timestamp > self.current_watermark {
            self.current_watermark = timestamp;
            Some(Watermark::new(timestamp))
        } else {
            None
        }
    }
}

/// Tracks watermarks across multiple input sources.
///
/// For operators with multiple inputs (e.g., joins, unions), the combined
/// watermark is the minimum across all sources. This ensures no late events
/// from any source are missed.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{WatermarkTracker, Watermark};
///
/// let mut tracker = WatermarkTracker::new(3); // 3 sources
///
/// // Source 0 advances to 1000
/// let wm = tracker.update_source(0, 1000);
/// assert_eq!(wm, None); // Other sources still at MIN
///
/// // Source 1 advances to 2000
/// tracker.update_source(1, 2000);
///
/// // Source 2 advances to 500
/// let wm = tracker.update_source(2, 500);
/// assert_eq!(wm, Some(Watermark::new(500))); // Min of all sources
/// ```
#[derive(Debug)]
pub struct WatermarkTracker {
    /// Watermark for each source
    source_watermarks: Vec<i64>,
    /// Combined minimum watermark
    combined_watermark: i64,
    /// Idle status for each source
    idle_sources: Vec<bool>,
    /// Last activity time for each source
    last_activity: Vec<Instant>,
    /// Idle timeout duration
    idle_timeout: Duration,
}

impl WatermarkTracker {
    /// Creates a new tracker for the specified number of sources.
    #[must_use]
    pub fn new(num_sources: usize) -> Self {
        Self {
            source_watermarks: vec![i64::MIN; num_sources],
            combined_watermark: i64::MIN,
            idle_sources: vec![false; num_sources],
            last_activity: vec![Instant::now(); num_sources],
            idle_timeout: Duration::from_secs(30), // Default 30 second idle timeout
        }
    }

    /// Creates a new tracker with a custom idle timeout.
    #[must_use]
    pub fn with_idle_timeout(num_sources: usize, idle_timeout: Duration) -> Self {
        Self {
            source_watermarks: vec![i64::MIN; num_sources],
            combined_watermark: i64::MIN,
            idle_sources: vec![false; num_sources],
            last_activity: vec![Instant::now(); num_sources],
            idle_timeout,
        }
    }

    /// Updates the watermark for a specific source.
    ///
    /// Returns `Some(Watermark)` if the combined watermark advances.
    pub fn update_source(&mut self, source_id: usize, watermark: i64) -> Option<Watermark> {
        if source_id >= self.source_watermarks.len() {
            return None;
        }

        // Mark source as active
        self.idle_sources[source_id] = false;
        self.last_activity[source_id] = Instant::now();

        // Update source watermark
        if watermark > self.source_watermarks[source_id] {
            self.source_watermarks[source_id] = watermark;
            self.update_combined()
        } else {
            None
        }
    }

    /// Marks a source as idle, excluding it from watermark calculation.
    ///
    /// Idle sources don't hold back the combined watermark.
    pub fn mark_idle(&mut self, source_id: usize) -> Option<Watermark> {
        if source_id >= self.idle_sources.len() {
            return None;
        }

        self.idle_sources[source_id] = true;
        self.update_combined()
    }

    /// Checks for sources that have been idle longer than the timeout.
    ///
    /// Should be called periodically to detect stalled sources.
    pub fn check_idle_sources(&mut self) -> Option<Watermark> {
        let mut any_marked = false;
        for i in 0..self.idle_sources.len() {
            if !self.idle_sources[i] && self.last_activity[i].elapsed() >= self.idle_timeout {
                self.idle_sources[i] = true;
                any_marked = true;
            }
        }
        if any_marked {
            self.update_combined()
        } else {
            None
        }
    }

    /// Returns the current combined watermark.
    #[must_use]
    pub fn current_watermark(&self) -> Option<Watermark> {
        if self.combined_watermark == i64::MIN {
            None
        } else {
            Some(Watermark::new(self.combined_watermark))
        }
    }

    /// Returns the watermark for a specific source.
    #[must_use]
    pub fn source_watermark(&self, source_id: usize) -> Option<i64> {
        self.source_watermarks.get(source_id).copied()
    }

    /// Returns whether a source is marked as idle.
    #[must_use]
    pub fn is_idle(&self, source_id: usize) -> bool {
        self.idle_sources.get(source_id).copied().unwrap_or(false)
    }

    /// Returns the number of sources being tracked.
    #[must_use]
    pub fn num_sources(&self) -> usize {
        self.source_watermarks.len()
    }

    /// Returns the number of active (non-idle) sources.
    #[must_use]
    pub fn active_source_count(&self) -> usize {
        self.idle_sources.iter().filter(|&&idle| !idle).count()
    }

    /// Updates the combined watermark based on all active sources.
    fn update_combined(&mut self) -> Option<Watermark> {
        // Calculate minimum across active sources only
        let mut min_watermark = i64::MAX;
        let mut has_active = false;

        for (i, &wm) in self.source_watermarks.iter().enumerate() {
            if !self.idle_sources[i] {
                has_active = true;
                min_watermark = min_watermark.min(wm);
            }
        }

        // If all sources are idle, use the max watermark
        if !has_active {
            min_watermark = self
                .source_watermarks
                .iter()
                .copied()
                .max()
                .unwrap_or(i64::MIN);
        }

        if min_watermark > self.combined_watermark && min_watermark != i64::MAX {
            self.combined_watermark = min_watermark;
            Some(Watermark::new(min_watermark))
        } else {
            None
        }
    }
}

/// Watermark generator for sources with embedded watermarks.
///
/// Some sources (like Kafka with EOS) may provide watermarks directly.
/// This generator tracks both event timestamps and explicit watermarks.
pub struct SourceProvidedGenerator {
    /// Last watermark from the source
    source_watermark: i64,
    /// Fallback generator for when source doesn't provide watermarks
    fallback: BoundedOutOfOrdernessGenerator,
    /// Whether to use source watermarks when available
    prefer_source: bool,
}

impl SourceProvidedGenerator {
    /// Creates a new source-provided generator.
    ///
    /// # Arguments
    ///
    /// * `fallback_lateness` - Lateness for fallback bounded generator
    /// * `prefer_source` - If true, source watermarks take precedence
    #[must_use]
    pub fn new(fallback_lateness: i64, prefer_source: bool) -> Self {
        Self {
            source_watermark: i64::MIN,
            fallback: BoundedOutOfOrdernessGenerator::new(fallback_lateness),
            prefer_source,
        }
    }

    /// Updates the watermark from the source.
    ///
    /// Call this when the source provides an explicit watermark.
    pub fn on_source_watermark(&mut self, watermark: i64) -> Option<Watermark> {
        if watermark > self.source_watermark {
            self.source_watermark = watermark;
            if self.prefer_source || watermark > self.fallback.current_watermark() {
                return Some(Watermark::new(watermark));
            }
        }
        None
    }
}

impl WatermarkGenerator for SourceProvidedGenerator {
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark> {
        let fallback_wm = self.fallback.on_event(timestamp);

        if self.prefer_source {
            // Only emit if source watermark allows and it's an advancement
            if self.source_watermark > i64::MIN {
                return None; // Wait for source watermark
            }
        }

        fallback_wm
    }

    fn on_periodic(&mut self) -> Option<Watermark> {
        None
    }

    fn current_watermark(&self) -> i64 {
        if self.prefer_source && self.source_watermark > i64::MIN {
            self.source_watermark
        } else {
            self.fallback.current_watermark().max(self.source_watermark)
        }
    }

    fn advance_watermark(&mut self, timestamp: i64) -> Option<Watermark> {
        self.on_source_watermark(timestamp)
    }
}

/// Processing-time watermark generator.
///
/// Ignores event timestamps entirely and advances the watermark based on
/// wall-clock time. Use with `PROCTIME()` sources where events are processed
/// in arrival order without event-time semantics.
///
/// - [`on_event`](WatermarkGenerator::on_event) returns `None` (event timestamps are ignored)
/// - [`on_periodic`](WatermarkGenerator::on_periodic) returns `Some(Watermark::new(now_millis()))`
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{ProcessingTimeGenerator, WatermarkGenerator};
///
/// let mut gen = ProcessingTimeGenerator::new();
/// // on_event ignores the timestamp
/// assert_eq!(gen.on_event(1000), None);
/// // on_periodic returns wall-clock time
/// let wm = gen.on_periodic();
/// assert!(wm.is_some());
/// ```
pub struct ProcessingTimeGenerator {
    current_watermark: i64,
}

impl ProcessingTimeGenerator {
    /// Creates a new processing-time watermark generator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            current_watermark: i64::MIN,
        }
    }

    /// Returns the current wall-clock time in milliseconds since Unix epoch.
    #[allow(clippy::cast_possible_truncation)]
    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as i64
    }
}

impl Default for ProcessingTimeGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl WatermarkGenerator for ProcessingTimeGenerator {
    #[inline]
    fn on_event(&mut self, _timestamp: i64) -> Option<Watermark> {
        // Processing-time mode ignores event timestamps
        None
    }

    #[inline]
    fn on_periodic(&mut self) -> Option<Watermark> {
        let now = Self::now_millis();
        if now > self.current_watermark {
            self.current_watermark = now;
            Some(Watermark::new(now))
        } else {
            None
        }
    }

    #[inline]
    fn current_watermark(&self) -> i64 {
        self.current_watermark
    }

    #[inline]
    fn advance_watermark(&mut self, timestamp: i64) -> Option<Watermark> {
        if timestamp > self.current_watermark {
            self.current_watermark = timestamp;
            Some(Watermark::new(timestamp))
        } else {
            None
        }
    }
}

/// Metrics for watermark tracking.
#[derive(Debug, Clone, Default)]
pub struct WatermarkMetrics {
    /// Current watermark timestamp
    pub current_watermark: i64,
    /// Maximum observed event timestamp
    pub max_event_timestamp: i64,
    /// Number of watermark emissions
    pub watermarks_emitted: u64,
    /// Number of late events detected
    pub late_events: u64,
}

impl WatermarkMetrics {
    /// Creates new metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the watermark lag (difference between max event time and watermark).
    #[must_use]
    pub fn lag(&self) -> i64 {
        self.max_event_timestamp
            .saturating_sub(self.current_watermark)
    }
}

/// Watermark generator wrapper that collects metrics.
pub struct MeteredGenerator<G: WatermarkGenerator> {
    inner: G,
    metrics: WatermarkMetrics,
}

impl<G: WatermarkGenerator> MeteredGenerator<G> {
    /// Creates a new metered generator.
    #[must_use]
    pub fn new(inner: G) -> Self {
        Self {
            inner,
            metrics: WatermarkMetrics::new(),
        }
    }

    /// Returns the current metrics.
    #[must_use]
    pub fn metrics(&self) -> &WatermarkMetrics {
        &self.metrics
    }

    /// Returns a mutable reference to the inner generator.
    pub fn inner_mut(&mut self) -> &mut G {
        &mut self.inner
    }

    /// Records a late event.
    pub fn record_late_event(&mut self) {
        self.metrics.late_events += 1;
    }
}

impl<G: WatermarkGenerator> WatermarkGenerator for MeteredGenerator<G> {
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark> {
        // Track max event timestamp
        if timestamp > self.metrics.max_event_timestamp {
            self.metrics.max_event_timestamp = timestamp;
        }

        let wm = self.inner.on_event(timestamp);
        if let Some(ref w) = wm {
            self.metrics.current_watermark = w.timestamp();
            self.metrics.watermarks_emitted += 1;
        }
        wm
    }

    fn on_periodic(&mut self) -> Option<Watermark> {
        let wm = self.inner.on_periodic();
        if let Some(ref w) = wm {
            self.metrics.current_watermark = w.timestamp();
            self.metrics.watermarks_emitted += 1;
        }
        wm
    }

    fn current_watermark(&self) -> i64 {
        self.inner.current_watermark()
    }

    fn advance_watermark(&mut self, timestamp: i64) -> Option<Watermark> {
        let wm = self.inner.advance_watermark(timestamp);
        if let Some(ref w) = wm {
            self.metrics.current_watermark = w.timestamp();
            self.metrics.watermarks_emitted += 1;
        }
        wm
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounded_generator_first_event() {
        let mut gen = BoundedOutOfOrdernessGenerator::new(100);
        let wm = gen.on_event(1000);
        assert_eq!(wm, Some(Watermark::new(900)));
        assert_eq!(gen.current_watermark(), 900);
    }

    #[test]
    fn test_bounded_generator_out_of_order() {
        let mut gen = BoundedOutOfOrdernessGenerator::new(100);

        // First event
        gen.on_event(1000);

        // Out of order - should not emit new watermark
        let wm = gen.on_event(800);
        assert_eq!(wm, None);
        assert_eq!(gen.current_watermark(), 900); // Still at 1000 - 100
    }

    #[test]
    fn test_bounded_generator_advancement() {
        let mut gen = BoundedOutOfOrdernessGenerator::new(100);

        gen.on_event(1000);
        let wm = gen.on_event(1200);

        assert_eq!(wm, Some(Watermark::new(1100)));
    }

    #[test]
    fn test_bounded_generator_from_duration() {
        let gen = BoundedOutOfOrdernessGenerator::from_duration(Duration::from_secs(5));
        assert_eq!(gen.max_out_of_orderness(), 5000);
    }

    #[test]
    fn test_bounded_generator_no_periodic() {
        let mut gen = BoundedOutOfOrdernessGenerator::new(100);
        assert_eq!(gen.on_periodic(), None);
    }

    #[test]
    fn test_ascending_generator_advances_on_each_event() {
        let mut gen = AscendingTimestampsGenerator::new();

        let wm1 = gen.on_event(1000);
        assert_eq!(wm1, Some(Watermark::new(1000)));

        let wm2 = gen.on_event(2000);
        assert_eq!(wm2, Some(Watermark::new(2000)));
    }

    #[test]
    fn test_ascending_generator_ignores_backwards() {
        let mut gen = AscendingTimestampsGenerator::new();

        gen.on_event(2000);
        let wm = gen.on_event(1000); // Earlier timestamp

        assert_eq!(wm, None);
        assert_eq!(gen.current_watermark(), 2000);
    }

    #[test]
    fn test_periodic_generator_passes_through() {
        let inner = BoundedOutOfOrdernessGenerator::new(100);
        let mut gen = PeriodicGenerator::new(inner, Duration::from_millis(100));

        let wm = gen.on_event(1000);
        assert_eq!(wm, Some(Watermark::new(900)));
    }

    #[test]
    fn test_periodic_generator_inner_access() {
        let inner = BoundedOutOfOrdernessGenerator::new(100);
        let gen = PeriodicGenerator::new(inner, Duration::from_millis(100));

        assert_eq!(gen.inner().max_out_of_orderness(), 100);
    }

    #[test]
    fn test_punctuated_generator_predicate() {
        let mut gen = PunctuatedGenerator::new(|ts| {
            if ts % 1000 == 0 {
                Some(Watermark::new(ts))
            } else {
                None
            }
        });

        assert_eq!(gen.on_event(500), None);
        assert_eq!(gen.on_event(999), None);
        assert_eq!(gen.on_event(1000), Some(Watermark::new(1000)));
        assert_eq!(gen.on_event(1500), None);
        assert_eq!(gen.on_event(2000), Some(Watermark::new(2000)));
    }

    #[test]
    fn test_punctuated_generator_no_regression() {
        let mut gen = PunctuatedGenerator::new(|ts| Some(Watermark::new(ts)));

        gen.on_event(2000);
        let wm = gen.on_event(1000); // Lower watermark

        assert_eq!(wm, None);
        assert_eq!(gen.current_watermark(), 2000);
    }

    #[test]
    fn test_tracker_single_source() {
        let mut tracker = WatermarkTracker::new(1);

        let wm = tracker.update_source(0, 1000);
        assert_eq!(wm, Some(Watermark::new(1000)));
        assert_eq!(tracker.current_watermark(), Some(Watermark::new(1000)));
    }

    #[test]
    fn test_tracker_multiple_sources() {
        let mut tracker = WatermarkTracker::new(3);

        // All sources need to report before watermark advances
        tracker.update_source(0, 1000);
        tracker.update_source(1, 2000);
        let wm = tracker.update_source(2, 500);

        assert_eq!(wm, Some(Watermark::new(500))); // Minimum
    }

    #[test]
    fn test_tracker_min_watermark() {
        let mut tracker = WatermarkTracker::new(2);

        tracker.update_source(0, 5000);
        tracker.update_source(1, 3000);

        assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));

        // Source 1 advances
        tracker.update_source(1, 4000);
        assert_eq!(tracker.current_watermark(), Some(Watermark::new(4000)));
    }

    #[test]
    fn test_tracker_idle_source() {
        let mut tracker = WatermarkTracker::new(2);

        tracker.update_source(0, 5000);
        tracker.update_source(1, 1000);

        // Source 1 is slow, mark it idle
        let wm = tracker.mark_idle(1);

        // Now only source 0's watermark counts
        assert_eq!(wm, Some(Watermark::new(5000)));
    }

    #[test]
    fn test_tracker_all_idle() {
        let mut tracker = WatermarkTracker::new(2);

        tracker.update_source(0, 5000);
        tracker.update_source(1, 3000);

        tracker.mark_idle(0);
        let wm = tracker.mark_idle(1);

        // Use max when all idle
        assert_eq!(wm, Some(Watermark::new(5000)));
    }

    #[test]
    fn test_tracker_source_watermark() {
        let mut tracker = WatermarkTracker::new(2);

        tracker.update_source(0, 1000);
        tracker.update_source(1, 2000);

        assert_eq!(tracker.source_watermark(0), Some(1000));
        assert_eq!(tracker.source_watermark(1), Some(2000));
        assert_eq!(tracker.source_watermark(5), None); // Out of bounds
    }

    #[test]
    fn test_tracker_active_source_count() {
        let mut tracker = WatermarkTracker::new(3);

        assert_eq!(tracker.active_source_count(), 3);

        tracker.mark_idle(0);
        assert_eq!(tracker.active_source_count(), 2);

        tracker.mark_idle(2);
        assert_eq!(tracker.active_source_count(), 1);

        // Reactivate by updating
        tracker.update_source(0, 1000);
        assert_eq!(tracker.active_source_count(), 2);
    }

    #[test]
    fn test_tracker_invalid_source() {
        let mut tracker = WatermarkTracker::new(2);

        let wm = tracker.update_source(5, 1000); // Invalid source ID
        assert_eq!(wm, None);

        let wm = tracker.mark_idle(5);
        assert_eq!(wm, None);
    }

    #[test]
    fn test_source_provided_fallback() {
        let mut gen = SourceProvidedGenerator::new(100, false);

        let wm = gen.on_event(1000);
        assert_eq!(wm, Some(Watermark::new(900))); // Fallback behavior
    }

    #[test]
    fn test_source_provided_explicit_watermark() {
        let mut gen = SourceProvidedGenerator::new(100, true);

        let wm = gen.on_source_watermark(500);
        assert_eq!(wm, Some(Watermark::new(500)));
        assert_eq!(gen.current_watermark(), 500);
    }

    #[test]
    fn test_metered_generator_tracks_metrics() {
        let inner = BoundedOutOfOrdernessGenerator::new(100);
        let mut gen = MeteredGenerator::new(inner);

        gen.on_event(1000);
        gen.on_event(2000);
        gen.on_event(1500); // Out of order

        let metrics = gen.metrics();
        assert_eq!(metrics.max_event_timestamp, 2000);
        assert_eq!(metrics.watermarks_emitted, 2); // 1000 and 2000 advanced
    }

    #[test]
    fn test_metered_generator_lag() {
        let inner = BoundedOutOfOrdernessGenerator::new(100);
        let mut gen = MeteredGenerator::new(inner);

        gen.on_event(1000);

        let metrics = gen.metrics();
        assert_eq!(metrics.lag(), 100); // max_event (1000) - watermark (900)
    }

    #[test]
    fn test_metered_generator_late_events() {
        let inner = BoundedOutOfOrdernessGenerator::new(100);
        let mut gen = MeteredGenerator::new(inner);

        gen.record_late_event();
        gen.record_late_event();

        assert_eq!(gen.metrics().late_events, 2);
    }

    #[test]
    fn test_watermark_metrics_default() {
        let metrics = WatermarkMetrics::new();
        assert_eq!(metrics.current_watermark, 0);
        assert_eq!(metrics.max_event_timestamp, 0);
        assert_eq!(metrics.watermarks_emitted, 0);
        assert_eq!(metrics.late_events, 0);
    }

    // --- advance_watermark() tests ---

    #[test]
    fn test_advance_watermark_bounded_generator() {
        let mut gen = BoundedOutOfOrdernessGenerator::new(100);

        // Advance from initial state
        let wm = gen.advance_watermark(500);
        assert_eq!(wm, Some(Watermark::new(500)));
        assert_eq!(gen.current_watermark(), 500);

        // Advance further
        let wm = gen.advance_watermark(800);
        assert_eq!(wm, Some(Watermark::new(800)));
        assert_eq!(gen.current_watermark(), 800);

        // No regression
        let wm = gen.advance_watermark(600);
        assert_eq!(wm, None);
        assert_eq!(gen.current_watermark(), 800);
    }

    #[test]
    fn test_advance_watermark_maintains_invariant() {
        let mut gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process an event to set initial state
        gen.on_event(1000); // wm=900, max_ts=1000

        // Advance watermark beyond current
        gen.advance_watermark(1200);
        assert_eq!(gen.current_watermark(), 1200);

        // Now on_event at 1250 should work correctly: max_ts should be >= 1300
        // wm = 1250 - 100 = 1150 which is < 1200, so no new watermark from on_event
        let wm = gen.on_event(1250);
        assert_eq!(wm, None);
        assert_eq!(gen.current_watermark(), 1200);

        // But event at 1400: max_ts = 1400, wm = 1300 > 1200
        let wm = gen.on_event(1400);
        assert_eq!(wm, Some(Watermark::new(1300)));
    }

    #[test]
    fn test_advance_watermark_ascending_generator() {
        let mut gen = AscendingTimestampsGenerator::new();

        let wm = gen.advance_watermark(500);
        assert_eq!(wm, Some(Watermark::new(500)));
        assert_eq!(gen.current_watermark(), 500);

        // No regression
        let wm = gen.advance_watermark(300);
        assert_eq!(wm, None);
        assert_eq!(gen.current_watermark(), 500);

        // Further advance
        let wm = gen.advance_watermark(1000);
        assert_eq!(wm, Some(Watermark::new(1000)));
    }

    #[test]
    fn test_advance_watermark_periodic_generator() {
        let inner = BoundedOutOfOrdernessGenerator::new(100);
        let mut gen = PeriodicGenerator::new(inner, Duration::from_millis(100));

        let wm = gen.advance_watermark(500);
        assert_eq!(wm, Some(Watermark::new(500)));
        assert_eq!(gen.current_watermark(), 500);

        // No regression
        let wm = gen.advance_watermark(300);
        assert_eq!(wm, None);
    }

    #[test]
    fn test_advance_watermark_punctuated_generator() {
        let mut gen = PunctuatedGenerator::new(|ts| {
            if ts % 1000 == 0 {
                Some(Watermark::new(ts))
            } else {
                None
            }
        });

        // External advance (does not invoke predicate)
        let wm = gen.advance_watermark(500);
        assert_eq!(wm, Some(Watermark::new(500)));
        assert_eq!(gen.current_watermark(), 500);

        // No regression
        let wm = gen.advance_watermark(200);
        assert_eq!(wm, None);
    }

    #[test]
    fn test_advance_watermark_source_provided_generator() {
        let mut gen = SourceProvidedGenerator::new(100, true);

        let wm = gen.advance_watermark(500);
        assert_eq!(wm, Some(Watermark::new(500)));
        assert_eq!(gen.current_watermark(), 500);

        // No regression
        let wm = gen.advance_watermark(300);
        assert_eq!(wm, None);
    }

    #[test]
    fn test_advance_watermark_metered_generator() {
        let inner = BoundedOutOfOrdernessGenerator::new(100);
        let mut gen = MeteredGenerator::new(inner);

        let wm = gen.advance_watermark(500);
        assert_eq!(wm, Some(Watermark::new(500)));
        assert_eq!(gen.metrics().current_watermark, 500);
        assert_eq!(gen.metrics().watermarks_emitted, 1);

        // Advance further
        gen.advance_watermark(800);
        assert_eq!(gen.metrics().current_watermark, 800);
        assert_eq!(gen.metrics().watermarks_emitted, 2);

        // No regression doesn't bump metrics
        gen.advance_watermark(600);
        assert_eq!(gen.metrics().watermarks_emitted, 2);
    }

    // --- ProcessingTimeGenerator tests ---

    #[test]
    fn test_processing_time_generator_ignores_events() {
        let mut gen = ProcessingTimeGenerator::new();
        assert_eq!(gen.on_event(1000), None);
        assert_eq!(gen.on_event(2000), None);
        assert_eq!(gen.current_watermark(), i64::MIN);
    }

    #[test]
    fn test_processing_time_generator_periodic() {
        let mut gen = ProcessingTimeGenerator::new();
        let wm = gen.on_periodic();
        assert!(wm.is_some());
        let ts = wm.unwrap().timestamp();
        // Should be a reasonable timestamp (after 2020-01-01)
        assert!(ts > 1_577_836_800_000, "timestamp too old: {ts}");
    }

    #[test]
    fn test_processing_time_generator_advance_watermark() {
        let mut gen = ProcessingTimeGenerator::new();

        let wm = gen.advance_watermark(500);
        assert_eq!(wm, Some(Watermark::new(500)));
        assert_eq!(gen.current_watermark(), 500);

        // No regression
        let wm = gen.advance_watermark(300);
        assert_eq!(wm, None);
        assert_eq!(gen.current_watermark(), 500);

        // Further advance
        let wm = gen.advance_watermark(1000);
        assert_eq!(wm, Some(Watermark::new(1000)));
    }

    #[test]
    fn test_processing_time_generator_default() {
        let gen = ProcessingTimeGenerator::default();
        assert_eq!(gen.current_watermark(), i64::MIN);
    }
}
