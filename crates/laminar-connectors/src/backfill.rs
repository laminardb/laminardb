//! Historical backfill support for source connectors.
//!
//! Provides a wrapper that replays historical data from a source before
//! switching to live streaming. This is useful for scenarios like:
//! - "Process all existing Kafka messages, then switch to live tail"
//! - "Snapshot all MongoDB documents, then stream changes"
//! - "Read all existing CDC events, then follow the live replication stream"
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────┐
//! │         BackfillSource               │
//! │  ┌────────────┐  ┌───────────────┐   │
//! │  │  Backfill   │  │    Live       │   │
//! │  │  Source     │──│    Source      │   │
//! │  │ (historical)│  │  (streaming)  │   │
//! │  └────────────┘  └───────────────┘   │
//! │        │                │             │
//! │        ▼                ▼             │
//! │  Phase 1: Backfill  Phase 2: Live    │
//! └──────────────────────────────────────┘
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

/// Current phase of the backfill process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillPhase {
    /// Not yet started.
    NotStarted,
    /// Replaying historical data.
    Backfilling,
    /// Backfill is complete, switched to live streaming.
    Live,
}

impl std::fmt::Display for BackfillPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillPhase::NotStarted => write!(f, "not_started"),
            BackfillPhase::Backfilling => write!(f, "backfilling"),
            BackfillPhase::Live => write!(f, "live"),
        }
    }
}

/// Progress tracking for the backfill process.
#[derive(Debug)]
pub struct BackfillProgress {
    /// Current offset in the backfill (records processed so far).
    pub current_offset: AtomicU64,

    /// End offset (total records to process, 0 if unknown).
    pub end_offset: AtomicU64,

    /// Whether the backfill is complete.
    pub complete: AtomicBool,
}

impl BackfillProgress {
    /// Creates new progress tracking.
    #[must_use]
    pub fn new() -> Self {
        Self {
            current_offset: AtomicU64::new(0),
            end_offset: AtomicU64::new(0),
            complete: AtomicBool::new(false),
        }
    }

    /// Sets the end offset (total records to backfill).
    pub fn set_end_offset(&self, end: u64) {
        self.end_offset.store(end, Ordering::Relaxed);
    }

    /// Advances the current offset by `count`.
    pub fn advance(&self, count: u64) {
        self.current_offset.fetch_add(count, Ordering::Relaxed);
    }

    /// Returns the current offset.
    #[must_use]
    pub fn current(&self) -> u64 {
        self.current_offset.load(Ordering::Relaxed)
    }

    /// Returns the end offset.
    #[must_use]
    pub fn end(&self) -> u64 {
        self.end_offset.load(Ordering::Relaxed)
    }

    /// Returns the completion percentage (0.0 to 100.0).
    ///
    /// Returns 0.0 if the end offset is unknown (zero).
    #[must_use]
    pub fn percent_complete(&self) -> f64 {
        let end = self.end();
        if end == 0 {
            return 0.0;
        }
        let current = self.current();
        (current as f64 / end as f64 * 100.0).min(100.0)
    }

    /// Marks the backfill as complete.
    pub fn mark_complete(&self) {
        self.complete.store(true, Ordering::Release);
    }

    /// Returns whether the backfill is complete.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    /// Returns a snapshot of the progress.
    #[must_use]
    pub fn snapshot(&self) -> BackfillProgressSnapshot {
        BackfillProgressSnapshot {
            current_offset: self.current(),
            end_offset: self.end(),
            percent_complete: self.percent_complete(),
            complete: self.is_complete(),
        }
    }
}

impl Default for BackfillProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of backfill progress.
#[derive(Debug, Clone)]
pub struct BackfillProgressSnapshot {
    /// Current offset (records processed).
    pub current_offset: u64,
    /// End offset (total records, 0 if unknown).
    pub end_offset: u64,
    /// Completion percentage (0.0 to 100.0).
    pub percent_complete: f64,
    /// Whether the backfill is complete.
    pub complete: bool,
}

/// Configuration for the backfill behavior.
#[derive(Debug, Clone)]
pub struct BackfillConfig {
    /// Whether backfill is enabled.
    pub enabled: bool,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self { enabled: false }
    }
}

impl BackfillConfig {
    /// Creates a config with backfill enabled.
    #[must_use]
    pub fn enabled() -> Self {
        Self { enabled: true }
    }

    /// Parses backfill config from a [`ConnectorConfig`].
    #[must_use]
    pub fn from_config(config: &ConnectorConfig) -> Self {
        let enabled = config
            .get("backfill")
            .or_else(|| config.get("backfill.enabled"))
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        Self { enabled }
    }
}

/// A source connector wrapper that supports historical backfill.
///
/// Wraps an inner `SourceConnector` with backfill tracking. The connector
/// polls the inner source normally; the caller is responsible for determining
/// when the backfill phase ends (by calling `complete_backfill()`).
///
/// # Usage
///
/// ```ignore
/// let inner = KafkaSource::new(config);
/// let backfill_config = BackfillConfig::enabled();
/// let mut source = BackfillSource::new(Box::new(inner), backfill_config);
///
/// source.open(&config).await?;
///
/// // Set the end offset if known
/// source.progress().set_end_offset(total_messages);
///
/// while let Some(batch) = source.poll_batch(1000).await? {
///     // Process batch...
///     // When caught up, call:
///     // source.complete_backfill();
/// }
/// ```
pub struct BackfillSource {
    /// The underlying source connector.
    inner: Box<dyn SourceConnector>,

    /// Backfill configuration.
    config: BackfillConfig,

    /// Current backfill phase.
    phase: BackfillPhase,

    /// Progress tracking.
    progress: Arc<BackfillProgress>,
}

impl std::fmt::Debug for BackfillSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackfillSource")
            .field("config", &self.config)
            .field("phase", &self.phase)
            .field("progress", &self.progress.snapshot())
            .finish_non_exhaustive()
    }
}

impl BackfillSource {
    /// Creates a new backfill source wrapping the given inner source.
    #[must_use]
    pub fn new(inner: Box<dyn SourceConnector>, config: BackfillConfig) -> Self {
        Self {
            inner,
            config,
            phase: BackfillPhase::NotStarted,
            progress: Arc::new(BackfillProgress::new()),
        }
    }

    /// Returns the current backfill phase.
    #[must_use]
    pub fn phase(&self) -> BackfillPhase {
        self.phase
    }

    /// Returns the backfill progress tracker.
    #[must_use]
    pub fn progress(&self) -> &BackfillProgress {
        &self.progress
    }

    /// Returns a shared reference to the progress tracker.
    #[must_use]
    pub fn progress_arc(&self) -> Arc<BackfillProgress> {
        Arc::clone(&self.progress)
    }

    /// Returns whether the source is currently in backfill mode.
    #[must_use]
    pub fn is_backfilling(&self) -> bool {
        self.phase == BackfillPhase::Backfilling
    }

    /// Returns whether the backfill has completed and the source is live.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.phase == BackfillPhase::Live
    }

    /// Marks the backfill as complete, transitioning to live mode.
    ///
    /// After this call, `phase()` returns `Live` and `is_backfilling()` returns false.
    pub fn complete_backfill(&mut self) {
        if self.phase == BackfillPhase::Backfilling {
            self.phase = BackfillPhase::Live;
            self.progress.mark_complete();
            tracing::info!(
                current_offset = self.progress.current(),
                end_offset = self.progress.end(),
                "backfill complete, switching to live streaming"
            );
        }
    }

    /// Returns the backfill configuration.
    #[must_use]
    pub fn backfill_config(&self) -> &BackfillConfig {
        &self.config
    }
}

#[async_trait]
impl SourceConnector for BackfillSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.inner.open(config).await?;

        if self.config.enabled {
            self.phase = BackfillPhase::Backfilling;
            tracing::info!("backfill mode enabled, starting historical replay");
        } else {
            self.phase = BackfillPhase::Live;
            self.progress.mark_complete();
        }

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let result = self.inner.poll_batch(max_records).await?;

        if let Some(ref batch) = result {
            let row_count = batch.num_rows() as u64;
            if self.phase == BackfillPhase::Backfilling {
                self.progress.advance(row_count);
            }
        }

        Ok(result)
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = self.inner.checkpoint();
        checkpoint.set_metadata("backfill.phase", self.phase.to_string());
        checkpoint.set_metadata(
            "backfill.current_offset",
            self.progress.current().to_string(),
        );
        checkpoint.set_metadata("backfill.end_offset", self.progress.end().to_string());
        checkpoint.set_metadata("backfill.complete", self.progress.is_complete().to_string());
        checkpoint
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        self.inner.restore(checkpoint).await?;

        // Restore backfill state from checkpoint metadata
        if let Some(phase_str) = checkpoint.get_metadata("backfill.phase") {
            match phase_str {
                "backfilling" => self.phase = BackfillPhase::Backfilling,
                "live" => {
                    self.phase = BackfillPhase::Live;
                    self.progress.mark_complete();
                }
                _ => {}
            }
        }
        if let Some(current) = checkpoint.get_metadata("backfill.current_offset") {
            if let Ok(v) = current.parse::<u64>() {
                self.progress.current_offset.store(v, Ordering::Relaxed);
            }
        }
        if let Some(end) = checkpoint.get_metadata("backfill.end_offset") {
            if let Ok(v) = end.parse::<u64>() {
                self.progress.set_end_offset(v);
            }
        }

        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        self.inner.health_check()
    }

    fn metrics(&self) -> ConnectorMetrics {
        let mut metrics = self.inner.metrics();
        metrics.add_custom(
            "backfill.percent_complete",
            self.progress.percent_complete(),
        );
        metrics.add_custom("backfill.current_offset", self.progress.current() as f64);
        metrics.add_custom("backfill.end_offset", self.progress.end() as f64);
        metrics
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        self.inner.data_ready_notify()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.inner.close().await
    }

    fn supports_replay(&self) -> bool {
        self.inner.supports_replay()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockSourceConnector;

    #[test]
    fn test_backfill_config_default() {
        let config = BackfillConfig::default();
        assert!(!config.enabled);
    }

    #[test]
    fn test_backfill_config_enabled() {
        let config = BackfillConfig::enabled();
        assert!(config.enabled);
    }

    #[test]
    fn test_backfill_config_from_connector_config() {
        let mut cc = ConnectorConfig::new("test");
        assert!(!BackfillConfig::from_config(&cc).enabled);

        cc.set("backfill", "true");
        assert!(BackfillConfig::from_config(&cc).enabled);

        let mut cc2 = ConnectorConfig::new("test");
        cc2.set("backfill.enabled", "true");
        assert!(BackfillConfig::from_config(&cc2).enabled);

        let mut cc3 = ConnectorConfig::new("test");
        cc3.set("backfill", "false");
        assert!(!BackfillConfig::from_config(&cc3).enabled);
    }

    #[test]
    fn test_backfill_progress() {
        let progress = BackfillProgress::new();
        assert_eq!(progress.current(), 0);
        assert_eq!(progress.end(), 0);
        assert!(!progress.is_complete());
        assert_eq!(progress.percent_complete(), 0.0);

        progress.set_end_offset(1000);
        progress.advance(250);
        assert_eq!(progress.current(), 250);
        assert_eq!(progress.end(), 1000);
        assert!((progress.percent_complete() - 25.0).abs() < f64::EPSILON);

        progress.advance(750);
        assert_eq!(progress.current(), 1000);
        assert!((progress.percent_complete() - 100.0).abs() < f64::EPSILON);

        progress.mark_complete();
        assert!(progress.is_complete());
    }

    #[test]
    fn test_backfill_progress_snapshot() {
        let progress = BackfillProgress::new();
        progress.set_end_offset(500);
        progress.advance(100);

        let snap = progress.snapshot();
        assert_eq!(snap.current_offset, 100);
        assert_eq!(snap.end_offset, 500);
        assert!((snap.percent_complete - 20.0).abs() < f64::EPSILON);
        assert!(!snap.complete);
    }

    #[test]
    fn test_backfill_progress_unknown_end() {
        let progress = BackfillProgress::new();
        progress.advance(100);
        // End offset is 0 (unknown), so percent is 0.0
        assert_eq!(progress.percent_complete(), 0.0);
    }

    #[test]
    fn test_backfill_source_new() {
        let inner = Box::new(MockSourceConnector::new());
        let source = BackfillSource::new(inner, BackfillConfig::enabled());
        assert_eq!(source.phase(), BackfillPhase::NotStarted);
        assert!(!source.is_backfilling());
        assert!(!source.is_live());
    }

    #[tokio::test]
    async fn test_backfill_source_open_enabled() {
        let inner = Box::new(MockSourceConnector::new());
        let mut source = BackfillSource::new(inner, BackfillConfig::enabled());

        source.open(&ConnectorConfig::new("test")).await.unwrap();
        assert_eq!(source.phase(), BackfillPhase::Backfilling);
        assert!(source.is_backfilling());
        assert!(!source.is_live());
    }

    #[tokio::test]
    async fn test_backfill_source_open_disabled() {
        let inner = Box::new(MockSourceConnector::new());
        let mut source = BackfillSource::new(inner, BackfillConfig::default());

        source.open(&ConnectorConfig::new("test")).await.unwrap();
        assert_eq!(source.phase(), BackfillPhase::Live);
        assert!(!source.is_backfilling());
        assert!(source.is_live());
        assert!(source.progress().is_complete());
    }

    #[tokio::test]
    async fn test_backfill_source_complete() {
        let inner = Box::new(MockSourceConnector::new());
        let mut source = BackfillSource::new(inner, BackfillConfig::enabled());

        source.open(&ConnectorConfig::new("test")).await.unwrap();
        assert!(source.is_backfilling());

        source.progress().set_end_offset(1000);
        source.progress().advance(1000);
        source.complete_backfill();

        assert!(source.is_live());
        assert!(source.progress().is_complete());
    }

    #[tokio::test]
    async fn test_backfill_source_checkpoint_restore() {
        let inner = Box::new(MockSourceConnector::new());
        let mut source = BackfillSource::new(inner, BackfillConfig::enabled());

        source.open(&ConnectorConfig::new("test")).await.unwrap();
        source.progress().set_end_offset(1000);
        source.progress().advance(500);

        // Create checkpoint
        let checkpoint = source.checkpoint();
        assert_eq!(
            checkpoint.get_metadata("backfill.phase"),
            Some("backfilling")
        );
        assert_eq!(
            checkpoint.get_metadata("backfill.current_offset"),
            Some("500")
        );
        assert_eq!(checkpoint.get_metadata("backfill.end_offset"), Some("1000"));

        // Restore into a new source
        let inner2 = Box::new(MockSourceConnector::new());
        let mut source2 = BackfillSource::new(inner2, BackfillConfig::enabled());
        source2.restore(&checkpoint).await.unwrap();

        assert_eq!(source2.phase(), BackfillPhase::Backfilling);
        assert_eq!(source2.progress().current(), 500);
        assert_eq!(source2.progress().end(), 1000);
    }

    #[tokio::test]
    async fn test_backfill_source_checkpoint_restore_live() {
        let inner = Box::new(MockSourceConnector::new());
        let mut source = BackfillSource::new(inner, BackfillConfig::enabled());

        source.open(&ConnectorConfig::new("test")).await.unwrap();
        source.complete_backfill();

        let checkpoint = source.checkpoint();
        assert_eq!(checkpoint.get_metadata("backfill.phase"), Some("live"));
        assert_eq!(checkpoint.get_metadata("backfill.complete"), Some("true"));

        // Restore
        let inner2 = Box::new(MockSourceConnector::new());
        let mut source2 = BackfillSource::new(inner2, BackfillConfig::enabled());
        source2.restore(&checkpoint).await.unwrap();

        assert!(source2.is_live());
        assert!(source2.progress().is_complete());
    }

    #[tokio::test]
    async fn test_backfill_source_metrics() {
        let inner = Box::new(MockSourceConnector::new());
        let mut source = BackfillSource::new(inner, BackfillConfig::enabled());

        source.open(&ConnectorConfig::new("test")).await.unwrap();
        source.progress().set_end_offset(1000);
        source.progress().advance(250);

        let metrics = source.metrics();
        let custom: std::collections::HashMap<_, _> = metrics.custom.into_iter().collect();
        assert!((custom["backfill.percent_complete"] - 25.0).abs() < f64::EPSILON);
        assert!((custom["backfill.current_offset"] - 250.0).abs() < f64::EPSILON);
        assert!((custom["backfill.end_offset"] - 1000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_backfill_phase_display() {
        assert_eq!(BackfillPhase::NotStarted.to_string(), "not_started");
        assert_eq!(BackfillPhase::Backfilling.to_string(), "backfilling");
        assert_eq!(BackfillPhase::Live.to_string(), "live");
    }

    #[tokio::test]
    async fn test_backfill_source_close() {
        let inner = Box::new(MockSourceConnector::new());
        let mut source = BackfillSource::new(inner, BackfillConfig::enabled());

        source.open(&ConnectorConfig::new("test")).await.unwrap();
        assert!(source.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_backfill_source_schema() {
        let inner = Box::new(MockSourceConnector::new());
        let source = BackfillSource::new(inner, BackfillConfig::enabled());

        // Schema should delegate to inner
        let schema = source.schema();
        assert!(schema.fields().len() > 0);
    }
}
