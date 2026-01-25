//! # Watermark Alignment Groups
//!
//! Coordinates watermark progress across multiple sources to prevent unbounded state growth
//! when sources have different processing speeds.
//!
//! ## Problem
//!
//! When sources have different processing speeds, fast sources can cause:
//! - Excessive state growth (buffering data waiting for slow sources)
//! - Memory pressure on downstream operators
//! - Uneven resource utilization
//!
//! ## Solution
//!
//! Alignment groups enforce a maximum watermark drift between sources. When a source
//! gets too far ahead, it is paused until slower sources catch up.
//!
//! ## Example
//!
//! ```rust
//! use laminar_core::time::{
//!     WatermarkAlignmentGroup, AlignmentGroupConfig, AlignmentGroupId,
//!     EnforcementMode, AlignmentAction,
//! };
//! use std::time::Duration;
//!
//! let config = AlignmentGroupConfig {
//!     group_id: AlignmentGroupId("orders-payments".to_string()),
//!     max_drift: Duration::from_secs(300), // 5 minutes
//!     update_interval: Duration::from_secs(1),
//!     enforcement_mode: EnforcementMode::Pause,
//! };
//!
//! let mut group = WatermarkAlignmentGroup::new(config);
//!
//! // Register sources
//! group.register_source(0); // orders stream
//! group.register_source(1); // payments stream
//!
//! // Report watermarks
//! let action = group.report_watermark(0, 10_000); // orders at 10:00
//! assert_eq!(action, AlignmentAction::Continue);
//!
//! // Source 0 jumps far ahead
//! let action = group.report_watermark(0, 310_000); // orders at 10:05:10
//! assert_eq!(action, AlignmentAction::Pause); // Too far ahead of source 1!
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Identifier for an alignment group.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AlignmentGroupId(pub String);

impl AlignmentGroupId {
    /// Creates a new alignment group ID.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the group ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AlignmentGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Configuration for a watermark alignment group.
#[derive(Debug, Clone)]
pub struct AlignmentGroupConfig {
    /// Group identifier.
    pub group_id: AlignmentGroupId,
    /// Maximum allowed drift between fastest and slowest source.
    pub max_drift: Duration,
    /// How often to check alignment (wall clock).
    pub update_interval: Duration,
    /// Whether to pause sources or just emit warnings.
    pub enforcement_mode: EnforcementMode,
}

impl AlignmentGroupConfig {
    /// Creates a new configuration with defaults.
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: AlignmentGroupId::new(group_id),
            max_drift: Duration::from_secs(300), // 5 minutes default
            update_interval: Duration::from_secs(1),
            enforcement_mode: EnforcementMode::Pause,
        }
    }

    /// Sets the maximum allowed drift.
    #[must_use]
    pub fn with_max_drift(mut self, max_drift: Duration) -> Self {
        self.max_drift = max_drift;
        self
    }

    /// Sets the update interval.
    #[must_use]
    pub fn with_update_interval(mut self, interval: Duration) -> Self {
        self.update_interval = interval;
        self
    }

    /// Sets the enforcement mode.
    #[must_use]
    pub fn with_enforcement_mode(mut self, mode: EnforcementMode) -> Self {
        self.enforcement_mode = mode;
        self
    }
}

/// Enforcement mode for alignment groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnforcementMode {
    /// Pause fast sources (recommended for production).
    #[default]
    Pause,
    /// Emit warnings but don't pause (for monitoring).
    WarnOnly,
    /// Drop events from fast sources that exceed drift.
    DropExcess,
}

/// State for a source within an alignment group.
#[derive(Debug, Clone)]
pub struct AlignmentSourceState {
    /// Source identifier.
    pub source_id: usize,
    /// Current watermark (milliseconds since epoch).
    pub watermark: i64,
    /// Whether this source is currently paused.
    pub is_paused: bool,
    /// Time when pause started (for metrics).
    pub pause_start: Option<Instant>,
    /// Total time spent paused.
    pub total_pause_time: Duration,
    /// Events processed while paused (for `DropExcess` mode).
    pub events_dropped_while_paused: u64,
    /// Last activity time.
    pub last_activity: Instant,
}

impl AlignmentSourceState {
    /// Creates a new source state.
    fn new(source_id: usize) -> Self {
        Self {
            source_id,
            watermark: i64::MIN,
            is_paused: false,
            pause_start: None,
            total_pause_time: Duration::ZERO,
            events_dropped_while_paused: 0,
            last_activity: Instant::now(),
        }
    }

    /// Pauses this source.
    fn pause(&mut self) {
        if !self.is_paused {
            self.is_paused = true;
            self.pause_start = Some(Instant::now());
        }
    }

    /// Resumes this source.
    fn resume(&mut self) {
        if self.is_paused {
            self.is_paused = false;
            if let Some(start) = self.pause_start.take() {
                self.total_pause_time += start.elapsed();
            }
        }
    }
}

/// Metrics for an alignment group.
#[derive(Debug, Clone, Default)]
pub struct AlignmentGroupMetrics {
    /// Number of times sources were paused.
    pub pause_events: u64,
    /// Number of times sources were resumed.
    pub resume_events: u64,
    /// Total pause time across all sources.
    pub total_pause_time: Duration,
    /// Maximum observed drift.
    pub max_observed_drift: Duration,
    /// Current drift.
    pub current_drift: Duration,
    /// Events dropped (in `DropExcess` mode).
    pub events_dropped: u64,
    /// Number of warnings emitted (in `WarnOnly` mode).
    pub warnings_emitted: u64,
}

impl AlignmentGroupMetrics {
    /// Creates new metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all metrics.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Action to take for a source based on alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignmentAction {
    /// Continue processing normally.
    Continue,
    /// Pause this source (too far ahead).
    Pause,
    /// Resume this source (caught up).
    Resume,
    /// Drop this event (`DropExcess` mode).
    Drop,
    /// Warning only (`WarnOnly` mode).
    Warn {
        /// Current drift in milliseconds.
        drift_ms: i64,
    },
}

/// Manages watermark alignment across sources in a group.
///
/// Tracks watermarks from multiple sources and enforces that no source
/// gets too far ahead of others. When a source exceeds the maximum drift,
/// it is paused (or warned/dropped depending on enforcement mode) until
/// slower sources catch up.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{
///     WatermarkAlignmentGroup, AlignmentGroupConfig, AlignmentGroupId,
///     EnforcementMode, AlignmentAction,
/// };
/// use std::time::Duration;
///
/// let config = AlignmentGroupConfig {
///     group_id: AlignmentGroupId("join-group".to_string()),
///     max_drift: Duration::from_secs(60), // 1 minute
///     update_interval: Duration::from_millis(100),
///     enforcement_mode: EnforcementMode::Pause,
/// };
///
/// let mut group = WatermarkAlignmentGroup::new(config);
/// group.register_source(0);
/// group.register_source(1);
///
/// // Both sources start at 0
/// group.report_watermark(0, 0);
/// group.report_watermark(1, 0);
///
/// // Source 0 advances within limit
/// let action = group.report_watermark(0, 50_000); // 50 seconds
/// assert_eq!(action, AlignmentAction::Continue);
///
/// // Source 0 advances beyond limit (>60s drift)
/// let action = group.report_watermark(0, 70_000); // 70 seconds
/// assert_eq!(action, AlignmentAction::Pause);
///
/// // Source 1 catches up
/// group.report_watermark(1, 30_000); // 30 seconds
/// assert!(group.should_resume(0)); // Drift now 40s < 60s
/// ```
#[derive(Debug)]
pub struct WatermarkAlignmentGroup {
    /// Configuration.
    config: AlignmentGroupConfig,
    /// Per-source state.
    sources: HashMap<usize, AlignmentSourceState>,
    /// Current minimum watermark in the group.
    min_watermark: i64,
    /// Current maximum watermark in the group.
    max_watermark: i64,
    /// Last alignment check time.
    last_check: Instant,
    /// Metrics.
    metrics: AlignmentGroupMetrics,
}

impl WatermarkAlignmentGroup {
    /// Creates a new alignment group.
    #[must_use]
    pub fn new(config: AlignmentGroupConfig) -> Self {
        Self {
            config,
            sources: HashMap::new(),
            min_watermark: i64::MIN,
            max_watermark: i64::MIN,
            last_check: Instant::now(),
            metrics: AlignmentGroupMetrics::new(),
        }
    }

    /// Returns the group ID.
    #[must_use]
    pub fn group_id(&self) -> &AlignmentGroupId {
        &self.config.group_id
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &AlignmentGroupConfig {
        &self.config
    }

    /// Registers a source with this alignment group.
    pub fn register_source(&mut self, source_id: usize) {
        self.sources
            .entry(source_id)
            .or_insert_with(|| AlignmentSourceState::new(source_id));
    }

    /// Removes a source from this alignment group.
    pub fn unregister_source(&mut self, source_id: usize) {
        self.sources.remove(&source_id);
        self.recalculate_bounds();
    }

    /// Reports a watermark update from a source.
    ///
    /// Returns the action the source should take.
    pub fn report_watermark(&mut self, source_id: usize, watermark: i64) -> AlignmentAction {
        // Ensure source exists
        self.sources
            .entry(source_id)
            .or_insert_with(|| AlignmentSourceState::new(source_id));

        // Check if source is paused and handle accordingly
        let is_paused = self.sources.get(&source_id).is_some_and(|s| s.is_paused);

        if is_paused {
            match self.config.enforcement_mode {
                EnforcementMode::Pause => {
                    // Check if we can resume (need to check before mutating)
                    let can_resume = self.can_resume_with_watermark(source_id, watermark);
                    if can_resume {
                        if let Some(source) = self.sources.get_mut(&source_id) {
                            source.watermark = watermark;
                            source.last_activity = Instant::now();
                            source.resume();
                        }
                        self.metrics.resume_events += 1;
                        self.recalculate_bounds();
                        return AlignmentAction::Resume;
                    }
                    return AlignmentAction::Pause;
                }
                EnforcementMode::DropExcess => {
                    if let Some(source) = self.sources.get_mut(&source_id) {
                        source.events_dropped_while_paused += 1;
                    }
                    self.metrics.events_dropped += 1;
                    return AlignmentAction::Drop;
                }
                EnforcementMode::WarnOnly => {
                    // WarnOnly doesn't actually pause, so continue to update watermark
                }
            }
        }

        // Update source watermark
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.watermark = watermark;
            source.last_activity = Instant::now();
        }

        // Recalculate min/max
        self.recalculate_bounds();

        // Calculate current drift
        let current_drift = if self.min_watermark == i64::MIN || self.max_watermark == i64::MIN {
            Duration::ZERO
        } else {
            let drift_ms = self.max_watermark.saturating_sub(self.min_watermark).max(0);
            // SAFETY: drift_ms is guaranteed to be >= 0 due to max(0)
            #[allow(clippy::cast_sign_loss)]
            Duration::from_millis(drift_ms as u64)
        };

        self.metrics.current_drift = current_drift;
        if current_drift > self.metrics.max_observed_drift {
            self.metrics.max_observed_drift = current_drift;
        }

        // Check if this source is the one causing excessive drift
        if watermark == self.max_watermark && current_drift > self.config.max_drift {
            match self.config.enforcement_mode {
                EnforcementMode::Pause => {
                    if let Some(source) = self.sources.get_mut(&source_id) {
                        source.pause();
                    }
                    self.metrics.pause_events += 1;
                    AlignmentAction::Pause
                }
                EnforcementMode::WarnOnly => {
                    self.metrics.warnings_emitted += 1;
                    // Cap drift_ms to i64::MAX for extremely large drifts
                    #[allow(clippy::cast_possible_truncation)]
                    let drift_ms = current_drift.as_millis().min(i64::MAX as u128) as i64;
                    AlignmentAction::Warn { drift_ms }
                }
                EnforcementMode::DropExcess => {
                    if let Some(source) = self.sources.get_mut(&source_id) {
                        source.is_paused = true; // Mark as paused for tracking
                        source.events_dropped_while_paused += 1;
                    }
                    self.metrics.events_dropped += 1;
                    AlignmentAction::Drop
                }
            }
        } else {
            AlignmentAction::Continue
        }
    }

    /// Checks if a paused source should resume.
    #[must_use]
    pub fn should_resume(&self, source_id: usize) -> bool {
        let Some(source) = self.sources.get(&source_id) else {
            return false;
        };

        if !source.is_paused {
            return false;
        }

        self.can_resume_with_watermark(source_id, source.watermark)
    }

    /// Checks if a source can resume with a given watermark.
    fn can_resume_with_watermark(&self, source_id: usize, watermark: i64) -> bool {
        // Calculate what the min would be without this source
        let min_without_source = self
            .sources
            .iter()
            .filter(|(&id, s)| id != source_id && !s.is_paused && s.watermark != i64::MIN)
            .map(|(_, s)| s.watermark)
            .min()
            .unwrap_or(i64::MIN);

        if min_without_source == i64::MIN {
            return true; // No other active sources
        }

        let drift_if_resumed = watermark.saturating_sub(min_without_source).max(0);
        // SAFETY: drift_if_resumed is guaranteed to be >= 0 due to max(0)
        #[allow(clippy::cast_sign_loss)]
        let drift_duration = Duration::from_millis(drift_if_resumed as u64);

        drift_duration <= self.config.max_drift
    }

    /// Returns the current drift (max - min watermark).
    #[must_use]
    pub fn current_drift(&self) -> Duration {
        self.metrics.current_drift
    }

    /// Returns whether a specific source is paused.
    #[must_use]
    pub fn is_paused(&self, source_id: usize) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|s| s.is_paused)
    }

    /// Returns the minimum watermark in the group.
    #[must_use]
    pub fn min_watermark(&self) -> i64 {
        self.min_watermark
    }

    /// Returns the maximum watermark in the group.
    #[must_use]
    pub fn max_watermark(&self) -> i64 {
        self.max_watermark
    }

    /// Returns metrics for this group.
    #[must_use]
    pub fn metrics(&self) -> &AlignmentGroupMetrics {
        &self.metrics
    }

    /// Returns the number of registered sources.
    #[must_use]
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Returns the number of paused sources.
    #[must_use]
    pub fn paused_source_count(&self) -> usize {
        self.sources.values().filter(|s| s.is_paused).count()
    }

    /// Returns the number of active (non-paused) sources.
    #[must_use]
    pub fn active_source_count(&self) -> usize {
        self.sources.values().filter(|s| !s.is_paused).count()
    }

    /// Performs periodic alignment check.
    ///
    /// Should be called from Ring 1 at the configured `update_interval`.
    /// Returns list of (`source_id`, action) pairs.
    pub fn check_alignment(&mut self) -> Vec<(usize, AlignmentAction)> {
        if self.last_check.elapsed() < self.config.update_interval {
            return Vec::new();
        }

        self.last_check = Instant::now();
        let mut actions = Vec::new();

        // Check each paused source to see if it can resume
        let paused_sources: Vec<usize> = self
            .sources
            .iter()
            .filter(|(_, s)| s.is_paused)
            .map(|(&id, _)| id)
            .collect();

        for source_id in paused_sources {
            if self.should_resume(source_id) {
                if let Some(source) = self.sources.get_mut(&source_id) {
                    source.resume();
                    self.metrics.resume_events += 1;
                    actions.push((source_id, AlignmentAction::Resume));
                }
            }
        }

        // Recalculate bounds after resumptions
        if !actions.is_empty() {
            self.recalculate_bounds();
        }

        actions
    }

    /// Returns the state of a specific source.
    #[must_use]
    pub fn source_state(&self, source_id: usize) -> Option<&AlignmentSourceState> {
        self.sources.get(&source_id)
    }

    /// Recalculates min/max watermarks from active sources.
    fn recalculate_bounds(&mut self) {
        let active_watermarks: Vec<i64> = self
            .sources
            .values()
            .filter(|s| !s.is_paused && s.watermark != i64::MIN)
            .map(|s| s.watermark)
            .collect();

        if active_watermarks.is_empty() {
            self.min_watermark = i64::MIN;
            self.max_watermark = i64::MIN;
        } else {
            self.min_watermark = *active_watermarks.iter().min().unwrap();
            self.max_watermark = *active_watermarks.iter().max().unwrap();
        }
    }
}

/// Error type for alignment group operations.
#[derive(Debug, thiserror::Error)]
pub enum AlignmentError {
    /// Source not registered in any group.
    #[error("source {0} not in any group")]
    SourceNotInGroup(usize),

    /// Group not found.
    #[error("group '{0}' not found")]
    GroupNotFound(String),

    /// Source already assigned to another group.
    #[error("source {source_id} already in group '{group_id}'")]
    SourceAlreadyInGroup {
        /// The source ID.
        source_id: usize,
        /// The group ID.
        group_id: String,
    },
}

/// Manages multiple alignment groups.
///
/// Provides a single coordination point for all alignment groups in the system.
/// Routes watermark updates to the appropriate group based on source assignment.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{
///     AlignmentGroupCoordinator, AlignmentGroupConfig, AlignmentGroupId,
///     EnforcementMode, AlignmentAction,
/// };
/// use std::time::Duration;
///
/// let mut coordinator = AlignmentGroupCoordinator::new();
///
/// // Create a group for orders-payments join
/// let config = AlignmentGroupConfig::new("orders-payments")
///     .with_max_drift(Duration::from_secs(300));
/// coordinator.add_group(config);
///
/// // Assign sources to the group
/// coordinator.assign_source_to_group(0, &AlignmentGroupId::new("orders-payments")).unwrap();
/// coordinator.assign_source_to_group(1, &AlignmentGroupId::new("orders-payments")).unwrap();
///
/// // Report watermarks
/// let action = coordinator.report_watermark(0, 10_000);
/// assert_eq!(action, Some(AlignmentAction::Continue));
/// ```
#[derive(Debug, Default)]
pub struct AlignmentGroupCoordinator {
    /// Groups by ID.
    groups: HashMap<AlignmentGroupId, WatermarkAlignmentGroup>,
    /// Source to group mapping (a source can be in one group).
    source_groups: HashMap<usize, AlignmentGroupId>,
}

impl AlignmentGroupCoordinator {
    /// Creates a new coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an alignment group.
    pub fn add_group(&mut self, config: AlignmentGroupConfig) {
        let group_id = config.group_id.clone();
        self.groups
            .insert(group_id, WatermarkAlignmentGroup::new(config));
    }

    /// Removes an alignment group.
    pub fn remove_group(&mut self, group_id: &AlignmentGroupId) -> Option<WatermarkAlignmentGroup> {
        // Remove source mappings for this group
        self.source_groups.retain(|_, gid| gid != group_id);
        self.groups.remove(group_id)
    }

    /// Assigns a source to a group.
    ///
    /// # Errors
    ///
    /// Returns an error if the group doesn't exist or the source is already
    /// assigned to another group.
    pub fn assign_source_to_group(
        &mut self,
        source_id: usize,
        group_id: &AlignmentGroupId,
    ) -> Result<(), AlignmentError> {
        // Check if source is already in a group
        if let Some(existing_group) = self.source_groups.get(&source_id) {
            if existing_group != group_id {
                return Err(AlignmentError::SourceAlreadyInGroup {
                    source_id,
                    group_id: existing_group.0.clone(),
                });
            }
            // Already in this group, nothing to do
            return Ok(());
        }

        // Check if group exists
        let group = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| AlignmentError::GroupNotFound(group_id.0.clone()))?;

        // Register source with group
        group.register_source(source_id);
        self.source_groups.insert(source_id, group_id.clone());

        Ok(())
    }

    /// Removes a source from its group.
    pub fn unassign_source(&mut self, source_id: usize) {
        if let Some(group_id) = self.source_groups.remove(&source_id) {
            if let Some(group) = self.groups.get_mut(&group_id) {
                group.unregister_source(source_id);
            }
        }
    }

    /// Reports a watermark update.
    ///
    /// Returns the action for the source, or `None` if source not in any group.
    pub fn report_watermark(&mut self, source_id: usize, watermark: i64) -> Option<AlignmentAction> {
        let group_id = self.source_groups.get(&source_id)?;
        let group = self.groups.get_mut(group_id)?;
        Some(group.report_watermark(source_id, watermark))
    }

    /// Checks alignment for all groups.
    ///
    /// Returns all resume/pause actions across all groups.
    pub fn check_all_alignments(&mut self) -> Vec<(usize, AlignmentAction)> {
        let mut all_actions = Vec::new();
        for group in self.groups.values_mut() {
            all_actions.extend(group.check_alignment());
        }
        all_actions
    }

    /// Returns metrics for all groups.
    #[must_use]
    pub fn all_metrics(&self) -> HashMap<AlignmentGroupId, AlignmentGroupMetrics> {
        self.groups
            .iter()
            .map(|(id, group)| (id.clone(), group.metrics().clone()))
            .collect()
    }

    /// Returns a reference to a specific group.
    #[must_use]
    pub fn group(&self, group_id: &AlignmentGroupId) -> Option<&WatermarkAlignmentGroup> {
        self.groups.get(group_id)
    }

    /// Returns a mutable reference to a specific group.
    pub fn group_mut(
        &mut self,
        group_id: &AlignmentGroupId,
    ) -> Option<&mut WatermarkAlignmentGroup> {
        self.groups.get_mut(group_id)
    }

    /// Returns the group ID for a source.
    #[must_use]
    pub fn source_group(&self, source_id: usize) -> Option<&AlignmentGroupId> {
        self.source_groups.get(&source_id)
    }

    /// Returns the number of groups.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Returns the total number of sources across all groups.
    #[must_use]
    pub fn total_source_count(&self) -> usize {
        self.source_groups.len()
    }

    /// Checks if a source should resume.
    #[must_use]
    pub fn should_resume(&self, source_id: usize) -> bool {
        let Some(group_id) = self.source_groups.get(&source_id) else {
            return false;
        };
        let Some(group) = self.groups.get(group_id) else {
            return false;
        };
        group.should_resume(source_id)
    }

    /// Checks if a source is paused.
    #[must_use]
    pub fn is_paused(&self, source_id: usize) -> bool {
        let Some(group_id) = self.source_groups.get(&source_id) else {
            return false;
        };
        let Some(group) = self.groups.get(group_id) else {
            return false;
        };
        group.is_paused(source_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== AlignmentGroupId Tests ====================

    #[test]
    fn test_alignment_group_id() {
        let id = AlignmentGroupId::new("test-group");
        assert_eq!(id.as_str(), "test-group");
        assert_eq!(format!("{}", id), "test-group");
    }

    // ==================== AlignmentGroupConfig Tests ====================

    #[test]
    fn test_alignment_group_config_builder() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(120))
            .with_update_interval(Duration::from_millis(500))
            .with_enforcement_mode(EnforcementMode::WarnOnly);

        assert_eq!(config.group_id.as_str(), "test");
        assert_eq!(config.max_drift, Duration::from_secs(120));
        assert_eq!(config.update_interval, Duration::from_millis(500));
        assert_eq!(config.enforcement_mode, EnforcementMode::WarnOnly);
    }

    // ==================== WatermarkAlignmentGroup Tests ====================

    #[test]
    fn test_alignment_group_single_source_no_pause() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60));
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);

        // Single source should never be paused
        let action = group.report_watermark(0, 100_000);
        assert_eq!(action, AlignmentAction::Continue);
        assert!(!group.is_paused(0));
    }

    #[test]
    fn test_alignment_group_two_sources_fast_paused() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60)); // 60 second max drift
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        // Both start at 0
        group.report_watermark(0, 0);
        group.report_watermark(1, 0);

        // Source 0 advances within limit
        let action = group.report_watermark(0, 50_000); // 50 seconds
        assert_eq!(action, AlignmentAction::Continue);
        assert!(!group.is_paused(0));

        // Source 0 advances beyond limit
        let action = group.report_watermark(0, 70_000); // 70 seconds, drift > 60
        assert_eq!(action, AlignmentAction::Pause);
        assert!(group.is_paused(0));
    }

    #[test]
    fn test_alignment_group_resume_when_slow_catches_up() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60));
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        // Source 0 at 0, source 1 at 0
        group.report_watermark(0, 0);
        group.report_watermark(1, 0);

        // Source 0 jumps ahead and gets paused
        group.report_watermark(0, 100_000); // 100 seconds
        assert!(group.is_paused(0));

        // Source 1 catches up
        group.report_watermark(1, 50_000); // 50 seconds
        // Drift is now 100 - 50 = 50 seconds, within limit
        assert!(group.should_resume(0));
    }

    #[test]
    fn test_alignment_group_warn_only_mode() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60))
            .with_enforcement_mode(EnforcementMode::WarnOnly);
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        group.report_watermark(0, 0);
        group.report_watermark(1, 0);

        // Source 0 exceeds drift but only warns
        let action = group.report_watermark(0, 100_000);
        match action {
            AlignmentAction::Warn { drift_ms } => {
                assert_eq!(drift_ms, 100_000); // 100 seconds drift
            }
            _ => panic!("Expected Warn action"),
        }
        assert!(!group.is_paused(0)); // Not actually paused
        assert_eq!(group.metrics().warnings_emitted, 1);
    }

    #[test]
    fn test_alignment_group_drop_excess_mode() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60))
            .with_enforcement_mode(EnforcementMode::DropExcess);
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        group.report_watermark(0, 0);
        group.report_watermark(1, 0);

        // Source 0 exceeds drift and gets dropped
        let action = group.report_watermark(0, 100_000);
        assert_eq!(action, AlignmentAction::Drop);
        assert_eq!(group.metrics().events_dropped, 1);

        // Subsequent events from source 0 are also dropped
        let action = group.report_watermark(0, 110_000);
        assert_eq!(action, AlignmentAction::Drop);
        assert_eq!(group.metrics().events_dropped, 2);
    }

    #[test]
    fn test_alignment_group_drift_calculation() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(300)); // 5 minutes
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);
        group.register_source(2);

        group.report_watermark(0, 100_000); // 100s
        group.report_watermark(1, 200_000); // 200s
        group.report_watermark(2, 150_000); // 150s

        // Drift should be max - min = 200 - 100 = 100 seconds
        assert_eq!(group.current_drift(), Duration::from_secs(100));
        assert_eq!(group.min_watermark(), 100_000);
        assert_eq!(group.max_watermark(), 200_000);
    }

    #[test]
    fn test_alignment_group_metrics_accurate() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60));
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        group.report_watermark(0, 0);
        group.report_watermark(1, 0);

        // Pause source 0
        group.report_watermark(0, 100_000);
        assert_eq!(group.metrics().pause_events, 1);

        // Source 1 catches up
        group.report_watermark(1, 50_000);

        // Check alignment should resume source 0
        let actions = group.check_alignment();
        // Note: check_alignment only runs if update_interval has passed
        // For this test, we check should_resume directly
        assert!(group.should_resume(0));
    }

    #[test]
    fn test_alignment_group_unregister_source() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60));
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        group.report_watermark(0, 100_000);
        group.report_watermark(1, 50_000);

        assert_eq!(group.source_count(), 2);

        group.unregister_source(1);
        assert_eq!(group.source_count(), 1);

        // After removing source 1, only source 0 remains
        // so drift should be 0 (single source)
        assert_eq!(group.min_watermark(), 100_000);
        assert_eq!(group.max_watermark(), 100_000);
    }

    #[test]
    fn test_alignment_group_source_state() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60));
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.report_watermark(0, 50_000);

        let state = group.source_state(0).expect("source exists");
        assert_eq!(state.source_id, 0);
        assert_eq!(state.watermark, 50_000);
        assert!(!state.is_paused);
    }

    // ==================== AlignmentGroupCoordinator Tests ====================

    #[test]
    fn test_coordinator_multiple_groups() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let config1 = AlignmentGroupConfig::new("group1")
            .with_max_drift(Duration::from_secs(60));
        let config2 = AlignmentGroupConfig::new("group2")
            .with_max_drift(Duration::from_secs(120));

        coordinator.add_group(config1);
        coordinator.add_group(config2);

        assert_eq!(coordinator.group_count(), 2);
    }

    #[test]
    fn test_coordinator_source_assignment() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let config = AlignmentGroupConfig::new("test-group")
            .with_max_drift(Duration::from_secs(60));
        coordinator.add_group(config);

        let group_id = AlignmentGroupId::new("test-group");

        // Assign sources
        coordinator
            .assign_source_to_group(0, &group_id)
            .expect("should succeed");
        coordinator
            .assign_source_to_group(1, &group_id)
            .expect("should succeed");

        assert_eq!(coordinator.total_source_count(), 2);
        assert_eq!(coordinator.source_group(0), Some(&group_id));
    }

    #[test]
    fn test_coordinator_source_already_in_group() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let config1 = AlignmentGroupConfig::new("group1");
        let config2 = AlignmentGroupConfig::new("group2");
        coordinator.add_group(config1);
        coordinator.add_group(config2);

        let group1 = AlignmentGroupId::new("group1");
        let group2 = AlignmentGroupId::new("group2");

        coordinator
            .assign_source_to_group(0, &group1)
            .expect("should succeed");

        // Try to assign to different group
        let result = coordinator.assign_source_to_group(0, &group2);
        assert!(matches!(result, Err(AlignmentError::SourceAlreadyInGroup { .. })));

        // Assigning to same group should be fine
        let result = coordinator.assign_source_to_group(0, &group1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_coordinator_group_not_found() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let result = coordinator.assign_source_to_group(0, &AlignmentGroupId::new("nonexistent"));
        assert!(matches!(result, Err(AlignmentError::GroupNotFound(_))));
    }

    #[test]
    fn test_coordinator_report_watermark() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let config = AlignmentGroupConfig::new("test-group")
            .with_max_drift(Duration::from_secs(60));
        coordinator.add_group(config);

        let group_id = AlignmentGroupId::new("test-group");
        coordinator.assign_source_to_group(0, &group_id).unwrap();
        coordinator.assign_source_to_group(1, &group_id).unwrap();

        // Report watermarks
        let action = coordinator.report_watermark(0, 0);
        assert_eq!(action, Some(AlignmentAction::Continue));

        let action = coordinator.report_watermark(1, 0);
        assert_eq!(action, Some(AlignmentAction::Continue));

        // Source not in any group
        let action = coordinator.report_watermark(99, 0);
        assert_eq!(action, None);
    }

    #[test]
    fn test_coordinator_unassign_source() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let config = AlignmentGroupConfig::new("test-group");
        coordinator.add_group(config);

        let group_id = AlignmentGroupId::new("test-group");
        coordinator.assign_source_to_group(0, &group_id).unwrap();

        assert_eq!(coordinator.total_source_count(), 1);

        coordinator.unassign_source(0);
        assert_eq!(coordinator.total_source_count(), 0);
        assert_eq!(coordinator.source_group(0), None);
    }

    #[test]
    fn test_coordinator_remove_group() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let config = AlignmentGroupConfig::new("test-group");
        coordinator.add_group(config);

        let group_id = AlignmentGroupId::new("test-group");
        coordinator.assign_source_to_group(0, &group_id).unwrap();
        coordinator.assign_source_to_group(1, &group_id).unwrap();

        assert_eq!(coordinator.group_count(), 1);
        assert_eq!(coordinator.total_source_count(), 2);

        coordinator.remove_group(&group_id);

        assert_eq!(coordinator.group_count(), 0);
        assert_eq!(coordinator.total_source_count(), 0);
    }

    #[test]
    fn test_coordinator_is_paused() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let config = AlignmentGroupConfig::new("test-group")
            .with_max_drift(Duration::from_secs(60));
        coordinator.add_group(config);

        let group_id = AlignmentGroupId::new("test-group");
        coordinator.assign_source_to_group(0, &group_id).unwrap();
        coordinator.assign_source_to_group(1, &group_id).unwrap();

        coordinator.report_watermark(0, 0);
        coordinator.report_watermark(1, 0);

        // Source 0 exceeds drift
        coordinator.report_watermark(0, 100_000);
        assert!(coordinator.is_paused(0));
        assert!(!coordinator.is_paused(1));
    }

    #[test]
    fn test_coordinator_all_metrics() {
        let mut coordinator = AlignmentGroupCoordinator::new();

        let config1 = AlignmentGroupConfig::new("group1");
        let config2 = AlignmentGroupConfig::new("group2");
        coordinator.add_group(config1);
        coordinator.add_group(config2);

        let metrics = coordinator.all_metrics();
        assert_eq!(metrics.len(), 2);
        assert!(metrics.contains_key(&AlignmentGroupId::new("group1")));
        assert!(metrics.contains_key(&AlignmentGroupId::new("group2")));
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_alignment_group_empty() {
        let config = AlignmentGroupConfig::new("test");
        let group = WatermarkAlignmentGroup::new(config);

        assert_eq!(group.source_count(), 0);
        assert_eq!(group.min_watermark(), i64::MIN);
        assert_eq!(group.max_watermark(), i64::MIN);
    }

    #[test]
    fn test_alignment_group_all_paused() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(10));
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        group.report_watermark(0, 0);
        group.report_watermark(1, 0);

        // Pause both sources by having them exceed drift relative to each other
        // Source 0 jumps far ahead
        group.report_watermark(0, 100_000);
        assert!(group.is_paused(0));

        // Source 1 is now the min, so drift calculation only includes it
        // Source 1 can't get paused because it's the slowest
        assert!(!group.is_paused(1));
    }

    #[test]
    fn test_alignment_group_negative_watermarks() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60));
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        // Use negative timestamps (before epoch)
        group.report_watermark(0, -100_000);
        group.report_watermark(1, -50_000);

        assert_eq!(group.min_watermark(), -100_000);
        assert_eq!(group.max_watermark(), -50_000);
        assert_eq!(group.current_drift(), Duration::from_secs(50));
    }

    #[test]
    fn test_alignment_source_state_pause_resume_tracking() {
        let mut state = AlignmentSourceState::new(0);

        assert!(!state.is_paused);
        assert!(state.pause_start.is_none());

        state.pause();
        assert!(state.is_paused);
        assert!(state.pause_start.is_some());

        // Wait a tiny bit
        std::thread::sleep(Duration::from_millis(1));

        state.resume();
        assert!(!state.is_paused);
        assert!(state.pause_start.is_none());
        assert!(state.total_pause_time > Duration::ZERO);
    }

    #[test]
    fn test_alignment_group_check_alignment_interval() {
        let config = AlignmentGroupConfig::new("test")
            .with_max_drift(Duration::from_secs(60))
            .with_update_interval(Duration::from_millis(100));
        let mut group = WatermarkAlignmentGroup::new(config);

        group.register_source(0);
        group.register_source(1);

        group.report_watermark(0, 0);
        group.report_watermark(1, 0);
        group.report_watermark(0, 100_000); // Paused

        // Immediate check should return empty (interval not elapsed)
        let immediate_actions = group.check_alignment();
        assert!(immediate_actions.is_empty());

        // Wait for interval
        std::thread::sleep(Duration::from_millis(110));

        // Now check should potentially return actions
        group.report_watermark(1, 50_000); // Slow source catches up
        let actions = group.check_alignment();
        // Should have a Resume action for source 0
        assert!(actions.iter().any(|(id, action)| *id == 0 && *action == AlignmentAction::Resume));
    }
}
