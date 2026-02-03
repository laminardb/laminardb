//! Kafka offset tracking for per-partition consumption progress.
//!
//! [`OffsetTracker`] maintains the latest consumed offset for each
//! topic-partition and supports checkpoint/restore roundtrips via
//! [`SourceCheckpoint`].

use std::collections::HashMap;

use rdkafka::Offset;
use rdkafka::TopicPartitionList;

use crate::checkpoint::SourceCheckpoint;

/// Tracks consumed offsets per topic-partition.
///
/// Offsets stored are the last-consumed offset (not the next offset to fetch).
/// When committing to Kafka, `to_topic_partition_list()` returns offset+1
/// (the next offset to consume) per Kafka convention.
#[derive(Debug, Clone, Default)]
pub struct OffsetTracker {
    /// Map from (topic, partition) to last-consumed offset.
    offsets: HashMap<(String, i32), i64>,
}

impl OffsetTracker {
    /// Creates a new empty offset tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Updates the offset for a topic-partition.
    pub fn update(&mut self, topic: &str, partition: i32, offset: i64) {
        self.offsets.insert((topic.to_string(), partition), offset);
    }

    /// Gets the last-consumed offset for a topic-partition.
    #[must_use]
    pub fn get(&self, topic: &str, partition: i32) -> Option<i64> {
        self.offsets.get(&(topic.to_string(), partition)).copied()
    }

    /// Returns the number of tracked partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.offsets.len()
    }

    /// Converts tracked offsets to a [`SourceCheckpoint`].
    ///
    /// Key format: `"{topic}-{partition}"`, value: offset as string.
    #[must_use]
    pub fn to_checkpoint(&self) -> SourceCheckpoint {
        let offsets: HashMap<String, String> = self
            .offsets
            .iter()
            .map(|((topic, partition), offset)| {
                (format!("{topic}-{partition}"), offset.to_string())
            })
            .collect();
        let mut cp = SourceCheckpoint::with_offsets(0, offsets);
        cp.set_metadata("connector", "kafka");
        cp
    }

    /// Restores offset state from a [`SourceCheckpoint`].
    ///
    /// Parses keys in `"{topic}-{partition}"` format.
    #[must_use]
    pub fn from_checkpoint(cp: &SourceCheckpoint) -> Self {
        let mut tracker = Self::new();
        for (key, value) in cp.offsets() {
            if let Ok(offset) = value.parse::<i64>() {
                // Find the last '-' to split topic from partition
                if let Some(dash_pos) = key.rfind('-') {
                    let topic = &key[..dash_pos];
                    if let Ok(partition) = key[dash_pos + 1..].parse::<i32>() {
                        tracker.update(topic, partition, offset);
                    }
                }
            }
        }
        tracker
    }

    /// Builds an rdkafka [`TopicPartitionList`] for committing.
    ///
    /// Per Kafka convention, committed offsets are next-to-fetch (offset+1).
    #[must_use]
    pub fn to_topic_partition_list(&self) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in &self.offsets {
            tpl.add_partition_offset(topic, *partition, Offset::Offset(offset + 1))
                .ok();
        }
        tpl
    }

    /// Clears all tracked offsets.
    pub fn clear(&mut self) {
        self.offsets.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_and_get() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 1, 200);

        assert_eq!(tracker.get("events", 0), Some(100));
        assert_eq!(tracker.get("events", 1), Some(200));
        assert_eq!(tracker.get("events", 2), None);
        assert_eq!(tracker.partition_count(), 2);
    }

    #[test]
    fn test_update_overwrites() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 0, 200);
        assert_eq!(tracker.get("events", 0), Some(200));
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 1, 200);
        tracker.update("orders", 0, 50);

        let cp = tracker.to_checkpoint();
        let restored = OffsetTracker::from_checkpoint(&cp);

        assert_eq!(restored.get("events", 0), Some(100));
        assert_eq!(restored.get("events", 1), Some(200));
        assert_eq!(restored.get("orders", 0), Some(50));
        assert_eq!(restored.partition_count(), 3);
    }

    #[test]
    fn test_empty_tracker() {
        let tracker = OffsetTracker::new();
        assert_eq!(tracker.partition_count(), 0);
        assert!(tracker.to_checkpoint().is_empty());
    }

    #[test]
    fn test_topic_partition_list() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 99);
        tracker.update("events", 1, 199);

        let tpl = tracker.to_topic_partition_list();
        // rdkafka TPL commit offset = next-to-fetch = offset+1
        let elements = tpl.elements();
        assert_eq!(elements.len(), 2);

        for elem in &elements {
            match elem.partition() {
                0 => assert_eq!(elem.offset(), Offset::Offset(100)),
                1 => assert_eq!(elem.offset(), Offset::Offset(200)),
                _ => panic!("unexpected partition"),
            }
        }
    }

    #[test]
    fn test_clear() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.clear();
        assert_eq!(tracker.partition_count(), 0);
        assert_eq!(tracker.get("events", 0), None);
    }

    #[test]
    fn test_multi_topic_checkpoint() {
        let mut tracker = OffsetTracker::new();
        tracker.update("topic-a", 0, 10);
        tracker.update("topic-b", 0, 20);

        let cp = tracker.to_checkpoint();
        let restored = OffsetTracker::from_checkpoint(&cp);

        assert_eq!(restored.get("topic-a", 0), Some(10));
        assert_eq!(restored.get("topic-b", 0), Some(20));
    }

    #[test]
    fn test_checkpoint_metadata() {
        let tracker = OffsetTracker::new();
        let cp = tracker.to_checkpoint();
        assert_eq!(cp.get_metadata("connector"), Some("kafka"));
    }
}
