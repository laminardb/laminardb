//! Notification batching for throughput optimization.
//!
//! Accumulates multiple [`ChangeEvent`]s before delivering them as a
//! [`ChangeEventBatch`], reducing per-event dispatch overhead for
//! high-throughput scenarios.
//!
//! # Flush Triggers
//!
//! A batch is flushed when either:
//! - The batch reaches `max_batch_size` events (size trigger)
//! - The time since the last flush exceeds `max_batch_delay` (time trigger)
//!
//! When batching is disabled (`enabled: false`), events pass through
//! immediately as single-event batches.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::subscription::event::{ChangeEvent, ChangeEventBatch};

// ---------------------------------------------------------------------------
// BatchConfig
// ---------------------------------------------------------------------------

/// Configuration for notification batching.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum events per batch before flushing.
    pub max_batch_size: usize,
    /// Maximum time to wait for a batch to fill before flushing.
    pub max_batch_delay: Duration,
    /// Whether batching is enabled. When `false`, events pass through
    /// immediately as single-event batches.
    pub enabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 64,
            max_batch_delay: Duration::from_micros(100),
            enabled: false,
        }
    }
}

// ---------------------------------------------------------------------------
// NotificationBatcher
// ---------------------------------------------------------------------------

/// Accumulates events per source and flushes as batches.
///
/// Used by the Ring 1 dispatcher to reduce per-event overhead when
/// delivering to subscribers. Each source has its own buffer, so
/// events from different sources are never mixed.
pub struct NotificationBatcher {
    /// Buffered events per `source_id`.
    buffers: HashMap<u32, Vec<ChangeEvent>>,
    /// Last flush time per `source_id`.
    last_flush: HashMap<u32, Instant>,
    /// Configuration.
    config: BatchConfig,
}

impl NotificationBatcher {
    /// Creates a new batcher with the given configuration.
    #[must_use]
    pub fn new(config: BatchConfig) -> Self {
        Self {
            buffers: HashMap::new(),
            last_flush: HashMap::new(),
            config,
        }
    }

    /// Adds an event to the batcher.
    ///
    /// Returns `Some(ChangeEventBatch)` if the batch is ready to deliver
    /// (size or time trigger), or `None` if the event was buffered.
    ///
    /// When batching is disabled, always returns a single-event batch.
    pub fn add(
        &mut self,
        source_id: u32,
        source_name: &str,
        event: ChangeEvent,
    ) -> Option<ChangeEventBatch> {
        if !self.config.enabled {
            let seq = event.sequence().unwrap_or(0);
            return Some(ChangeEventBatch::new(
                source_name.to_string(),
                vec![event],
                seq,
                seq,
            ));
        }

        let buffer = self.buffers.entry(source_id).or_default();
        buffer.push(event);

        let now = Instant::now();
        let last = self.last_flush.entry(source_id).or_insert(now);

        if buffer.len() >= self.config.max_batch_size
            || now.duration_since(*last) >= self.config.max_batch_delay
        {
            *last = now;
            let events = std::mem::take(buffer);
            let first_seq = events.first().and_then(ChangeEvent::sequence).unwrap_or(0);
            let last_seq = events.last().and_then(ChangeEvent::sequence).unwrap_or(0);
            Some(ChangeEventBatch::new(
                source_name.to_string(),
                events,
                first_seq,
                last_seq,
            ))
        } else {
            None
        }
    }

    /// Forces flush of all pending batches.
    ///
    /// Returns a vec of `(source_id, batch)` for all non-empty buffers.
    pub fn flush_all(&mut self) -> Vec<(u32, ChangeEventBatch)> {
        let mut results = Vec::new();

        for (&source_id, buffer) in &mut self.buffers {
            if !buffer.is_empty() {
                let events = std::mem::take(buffer);
                let first_seq = events.first().and_then(ChangeEvent::sequence).unwrap_or(0);
                let last_seq = events.last().and_then(ChangeEvent::sequence).unwrap_or(0);
                results.push((
                    source_id,
                    ChangeEventBatch::new(String::new(), events, first_seq, last_seq),
                ));
                self.last_flush.insert(source_id, Instant::now());
            }
        }

        results
    }

    /// Flushes batches that have exceeded the `max_batch_delay`.
    ///
    /// Returns a vec of `(source_id, batch)` for expired buffers.
    pub fn flush_expired(&mut self) -> Vec<(u32, ChangeEventBatch)> {
        let now = Instant::now();
        let mut results = Vec::new();

        let expired: Vec<u32> = self
            .buffers
            .iter()
            .filter(|(_, buf)| !buf.is_empty())
            .filter(|(id, _)| {
                let last = self.last_flush.get(id).copied().unwrap_or(now);
                now.duration_since(last) >= self.config.max_batch_delay
            })
            .map(|(&id, _)| id)
            .collect();

        for source_id in expired {
            if let Some(buffer) = self.buffers.get_mut(&source_id) {
                if !buffer.is_empty() {
                    let events = std::mem::take(buffer);
                    let first_seq = events.first().and_then(ChangeEvent::sequence).unwrap_or(0);
                    let last_seq = events.last().and_then(ChangeEvent::sequence).unwrap_or(0);
                    self.last_flush.insert(source_id, now);
                    results.push((
                        source_id,
                        ChangeEventBatch::new(String::new(), events, first_seq, last_seq),
                    ));
                }
            }
        }

        results
    }

    /// Returns the number of buffered events across all sources.
    #[must_use]
    pub fn buffered_count(&self) -> usize {
        self.buffers.values().map(Vec::len).sum()
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &BatchConfig {
        &self.config
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_event(seq: u64) -> ChangeEvent {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let array = Int64Array::from(vec![seq as i64]);
        let batch = Arc::new(RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap());
        ChangeEvent::insert(batch, 1000 + seq as i64, seq)
    }

    #[test]
    fn test_batcher_immediate_when_disabled() {
        let config = BatchConfig {
            enabled: false,
            ..Default::default()
        };
        let mut batcher = NotificationBatcher::new(config);

        let result = batcher.add(0, "mv_a", make_event(1));
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.first_sequence, 1);
        assert_eq!(batch.last_sequence, 1);
        assert_eq!(batch.source, "mv_a");
    }

    #[test]
    fn test_batcher_size_trigger() {
        let config = BatchConfig {
            max_batch_size: 3,
            max_batch_delay: Duration::from_secs(60),
            enabled: true,
        };
        let mut batcher = NotificationBatcher::new(config);

        assert!(batcher.add(0, "mv_a", make_event(1)).is_none());
        assert!(batcher.add(0, "mv_a", make_event(2)).is_none());
        assert_eq!(batcher.buffered_count(), 2);

        let batch = batcher.add(0, "mv_a", make_event(3));
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.first_sequence, 1);
        assert_eq!(batch.last_sequence, 3);
        assert_eq!(batcher.buffered_count(), 0);
    }

    #[test]
    fn test_batcher_timeout_trigger() {
        let config = BatchConfig {
            max_batch_size: 1000,
            max_batch_delay: Duration::from_millis(1),
            enabled: true,
        };
        let mut batcher = NotificationBatcher::new(config);

        assert!(batcher.add(0, "mv_a", make_event(1)).is_none());
        std::thread::sleep(Duration::from_millis(5));

        let batch = batcher.add(0, "mv_a", make_event(2));
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 2);
    }

    #[test]
    fn test_batcher_flush_all() {
        let config = BatchConfig {
            max_batch_size: 100,
            max_batch_delay: Duration::from_secs(60),
            enabled: true,
        };
        let mut batcher = NotificationBatcher::new(config);

        batcher.add(0, "mv_a", make_event(1));
        batcher.add(0, "mv_a", make_event(2));
        batcher.add(1, "mv_b", make_event(3));

        let batches = batcher.flush_all();
        assert_eq!(batches.len(), 2);

        let total_events: usize = batches.iter().map(|(_, b)| b.len()).sum();
        assert_eq!(total_events, 3);
        assert_eq!(batcher.buffered_count(), 0);
    }

    #[test]
    fn test_batcher_flush_expired() {
        let config = BatchConfig {
            max_batch_size: 100,
            max_batch_delay: Duration::from_millis(1),
            enabled: true,
        };
        let mut batcher = NotificationBatcher::new(config);

        batcher.add(0, "mv_a", make_event(1));
        batcher.add(0, "mv_a", make_event(2));

        std::thread::sleep(Duration::from_millis(5));

        let batches = batcher.flush_expired();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].1.len(), 2);
        assert_eq!(batcher.buffered_count(), 0);
    }

    #[test]
    fn test_batcher_multiple_sources() {
        let config = BatchConfig {
            max_batch_size: 2,
            max_batch_delay: Duration::from_secs(60),
            enabled: true,
        };
        let mut batcher = NotificationBatcher::new(config);

        assert!(batcher.add(0, "mv_a", make_event(1)).is_none());
        assert!(batcher.add(1, "mv_b", make_event(2)).is_none());
        let batch = batcher.add(0, "mv_a", make_event(3));
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 2);
        assert_eq!(batcher.buffered_count(), 1);
    }
}
