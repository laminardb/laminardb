//! Kafka offset tracking for per-partition consumption progress.
//!
//! [`OffsetTracker`] maintains the latest consumed offset for each
//! topic-partition and supports checkpoint/restore roundtrips via
//! [`SourceCheckpoint`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rdkafka::Offset;
use rdkafka::TopicPartitionList;

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;

/// Reserved source-offset key prefix for a partition's durable numeric next-to-read baseline.
/// `@` and `:` are invalid in Kafka topic names, so a real topic-partition key cannot collide.
pub(super) const KAFKA_PARTITION_BASELINE_PREFIX: &str = "@laminar.kafka.next.v1:";

/// Tracks consumed offsets per topic-partition.
///
/// Offsets stored are the last-consumed offset (not the next offset to fetch).
/// When committing to Kafka, `to_topic_partition_list()` returns offset+1
/// (the next offset to consume) per Kafka convention.
#[derive(Debug, Clone, Default)]
pub struct OffsetTracker {
    /// Two-level map: topic -> (partition -> offset). Uses `Arc<str>` keys
    /// to avoid per-message String allocations on the hot path.
    topics: HashMap<Arc<str>, HashMap<i32, i64>>,
}

impl OffsetTracker {
    /// Offsets start empty; populated by `update()` / `from_checkpoint()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Updates the offset (monotonic: only advances forward).
    pub fn update(&mut self, topic: &str, partition: i32, offset: i64) {
        if let Some(partitions) = self.topics.get_mut(topic as &str) {
            partitions
                .entry(partition)
                .and_modify(|existing| {
                    if offset > *existing {
                        *existing = offset;
                    }
                })
                .or_insert(offset);
        } else {
            let mut partitions = HashMap::new();
            partitions.insert(partition, offset);
            self.topics.insert(Arc::from(topic), partitions);
        }
    }

    /// Updates the offset using a pre-interned topic Arc (avoids allocation).
    pub fn update_arc(&mut self, topic: &Arc<str>, partition: i32, offset: i64) {
        if let Some(partitions) = self.topics.get_mut(&**topic as &str) {
            partitions
                .entry(partition)
                .and_modify(|existing| {
                    if offset > *existing {
                        *existing = offset;
                    }
                })
                .or_insert(offset);
        } else {
            let mut partitions = HashMap::new();
            partitions.insert(partition, offset);
            self.topics.insert(Arc::clone(topic), partitions);
        }
    }

    /// Unconditionally sets the offset for a topic-partition (used by restore).
    pub fn update_force(&mut self, topic: &str, partition: i32, offset: i64) {
        self.topics
            .entry(Arc::from(topic))
            .or_default()
            .insert(partition, offset);
    }

    /// Gets the last-consumed offset for a topic-partition.
    #[must_use]
    pub fn get(&self, topic: &str, partition: i32) -> Option<i64> {
        self.topics
            .get(topic)
            .and_then(|p| p.get(&partition))
            .copied()
    }

    /// Removes a tracked partition position.
    ///
    /// Vnode handoff uses this to discard a stale position from an earlier
    /// ownership stint before folding records from the rehydrated handoff cut.
    pub fn remove(&mut self, topic: &str, partition: i32) {
        let remove_topic = self.topics.get_mut(topic).is_some_and(|partitions| {
            partitions.remove(&partition);
            partitions.is_empty()
        });
        if remove_topic {
            self.topics.remove(topic);
        }
    }

    /// Returns the total number of tracked partitions across all topics.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.topics.values().map(HashMap::len).sum()
    }

    /// Converts tracked offsets to a [`SourceCheckpoint`].
    ///
    /// Key format: `"{topic}:{partition}"`, value: offset as string.
    ///
    /// `:` is not valid in a Kafka topic name, so the encoding remains
    /// unambiguous for valid topics containing or ending in `-`.
    #[must_use]
    pub fn to_checkpoint(&self) -> SourceCheckpoint {
        let offsets: HashMap<String, String> = self
            .topics
            .iter()
            .flat_map(|(topic, partitions)| {
                partitions.iter().map(move |(partition, offset)| {
                    (format!("{topic}:{partition}"), offset.to_string())
                })
            })
            .collect();
        let mut cp = SourceCheckpoint::with_offsets(offsets);
        cp.set_metadata("connector", "kafka");
        cp
    }

    /// Strictly restores offset state from a durable Kafka checkpoint.
    ///
    /// A malformed entry is corruption, not a partition that can safely be
    /// omitted: skipping it would let that partition fall back to a broker or
    /// startup cursor from a different engine timeline.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the connector identity, partition
    /// key, or offset value is malformed.
    pub fn try_from_checkpoint(cp: &SourceCheckpoint) -> Result<Self, ConnectorError> {
        if let Some(connector) = cp.get_metadata("connector") {
            if connector != "kafka" {
                return Err(ConnectorError::ConfigurationError(format!(
                    "Kafka checkpoint belongs to connector '{connector}'"
                )));
            }
        }
        Self::try_from_offset_map(cp.offsets())
    }

    /// Strictly builds a tracker from a raw `"{topic}:{partition}" -> offset`
    /// map. Kafka topic names cannot contain `:`, so the partition delimiter is
    /// unambiguous even when a topic contains or ends in `-`. Keys and values
    /// must use their canonical non-negative decimal form.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when a key cannot be split into a non-empty
    /// topic and canonical partition, or an offset is not canonical and non-negative.
    pub fn try_from_offset_map(offsets: &HashMap<String, String>) -> Result<Self, ConnectorError> {
        let mut tracker = Self::new();
        for (key, value) in offsets {
            if key.starts_with(KAFKA_PARTITION_BASELINE_PREFIX) {
                continue;
            }
            let (topic, partition_text) = key.rsplit_once(':').ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "invalid Kafka offset key '{key}': expected '<topic>:<partition>'"
                ))
            })?;
            if topic.is_empty() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid Kafka offset key '{key}': topic is empty"
                )));
            }
            if topic.contains(':') {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid Kafka offset key '{key}': topic contains the ':' delimiter"
                )));
            }

            let partition = partition_text.parse::<i32>().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid Kafka partition in '{key}': '{partition_text}'"
                ))
            })?;
            if partition < 0 || partition.to_string() != partition_text {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid Kafka partition in '{key}': expected a canonical non-negative integer"
                )));
            }

            let offset = value.parse::<i64>().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid Kafka offset for '{key}': '{value}'"
                ))
            })?;
            if offset < 0 || offset == i64::MAX || offset.to_string() != value.as_str() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "invalid Kafka offset for '{key}': expected a canonical non-negative integer below i64::MAX"
                )));
            }

            tracker.update_force(topic, partition, offset);
        }
        Ok(tracker)
    }

    /// Builds an rdkafka [`TopicPartitionList`] for committing.
    ///
    /// Per Kafka convention, committed offsets are next-to-fetch (offset+1).
    #[must_use]
    pub fn to_topic_partition_list(&self) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for (topic, partitions) in &self.topics {
            for (&partition, &offset) in partitions {
                if let Err(e) =
                    tpl.add_partition_offset(topic, partition, Offset::Offset(offset + 1))
                {
                    tracing::warn!(
                        %topic, partition, offset,
                        error = %e,
                        "failed to add partition offset to commit list"
                    );
                }
            }
        }
        tpl
    }

    /// Builds a [`TopicPartitionList`] for seeking after a rebalance assign.
    ///
    /// Only includes partitions where the tracker holds a known offset,
    /// using `Offset::Offset(offset + 1)` (next-to-fetch). Partitions NOT
    /// in the tracker are omitted — callers should let `auto.offset.reset`
    /// handle those.
    #[must_use]
    pub fn to_seek_tpl(&self, assigned: &[(String, i32)]) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for (topic, partition) in assigned {
            if let Some(off) = self.get(topic, *partition) {
                if let Err(e) = tpl.add_partition_offset(topic, *partition, Offset::Offset(off + 1))
                {
                    tracing::warn!(
                        %topic, partition, offset = off,
                        error = %e,
                        "failed to add partition to rebalance seek list"
                    );
                }
            }
        }
        tpl
    }

    /// Removes partitions that are not in the `assigned` set.
    ///
    /// Called after a rebalance revoke to purge offsets for partitions this
    /// consumer no longer owns. This prevents stale offsets from leaking
    /// into checkpoints and causing incorrect partition assignment on recovery.
    pub fn retain_assigned(&mut self, assigned: &HashSet<(String, i32)>) {
        self.topics.retain(|topic, partitions| {
            partitions.retain(|&partition, _| assigned.contains(&(topic.to_string(), partition)));
            !partitions.is_empty()
        });
    }

    /// Builds a checkpoint containing only offsets for currently assigned partitions.
    ///
    /// Non-mutating alternative to `retain_assigned()` + `to_checkpoint()`.
    /// When `assigned` is empty (either before first rebalance or after full
    /// revocation), returns an empty checkpoint — this is correct because
    /// there are no partitions this consumer owns.
    #[must_use]
    pub fn to_checkpoint_filtered(&self, assigned: &HashSet<(String, i32)>) -> SourceCheckpoint {
        let offsets: HashMap<String, String> = self
            .topics
            .iter()
            .flat_map(|(topic, partitions)| {
                partitions.iter().filter_map(move |(partition, offset)| {
                    if assigned.contains(&(topic.to_string(), *partition)) {
                        Some((format!("{topic}:{partition}"), offset.to_string()))
                    } else {
                        None
                    }
                })
            })
            .collect();
        let mut cp = SourceCheckpoint::with_offsets(offsets);
        cp.set_metadata("connector", "kafka");
        cp
    }

    /// Builds an rdkafka [`TopicPartitionList`] for only assigned partitions.
    ///
    /// Like [`Self::to_topic_partition_list`] but filtered to the `assigned` set.
    /// When `assigned` is empty, returns an empty TPL.
    #[must_use]
    pub fn to_topic_partition_list_filtered(
        &self,
        assigned: &HashSet<(String, i32)>,
    ) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for (topic, partitions) in &self.topics {
            for (&partition, &offset) in partitions {
                if assigned.contains(&(topic.to_string(), partition)) {
                    if let Err(e) =
                        tpl.add_partition_offset(topic, partition, Offset::Offset(offset + 1))
                    {
                        tracing::warn!(
                            %topic, partition, offset,
                            error = %e,
                            "failed to add partition offset to filtered commit list"
                        );
                    }
                }
            }
        }
        tpl
    }

    /// Clears all tracked offsets.
    pub fn clear(&mut self) {
        self.topics.clear();
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
    fn test_update_advances_forward() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 0, 200);
        assert_eq!(tracker.get("events", 0), Some(200));
    }

    #[test]
    fn test_update_rejects_regression() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 200);
        tracker.update("events", 0, 100); // should be ignored
        assert_eq!(tracker.get("events", 0), Some(200));
    }

    #[test]
    fn test_update_rejects_equal() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 0, 100); // same offset, no change
        assert_eq!(tracker.get("events", 0), Some(100));
    }

    #[test]
    fn test_update_force_overwrites() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 200);
        tracker.update_force("events", 0, 50); // force allows regression
        assert_eq!(tracker.get("events", 0), Some(50));
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 1, 200);
        tracker.update("orders", 0, 50);

        let cp = tracker.to_checkpoint();
        let restored = OffsetTracker::try_from_checkpoint(&cp).unwrap();

        assert_eq!(restored.get("events", 0), Some(100));
        assert_eq!(restored.get("events", 1), Some(200));
        assert_eq!(restored.get("orders", 0), Some(50));
        assert_eq!(restored.partition_count(), 3);
    }

    #[test]
    fn from_offset_map_supports_hyphenated_topics() {
        let map = HashMap::from([
            ("events:5".to_string(), "100".to_string()),
            ("my-topic:2".to_string(), "7".to_string()),
            ("trailing-hyphen-:3".to_string(), "9".to_string()),
        ]);
        let tracker = OffsetTracker::try_from_offset_map(&map).unwrap();
        assert_eq!(tracker.get("events", 5), Some(100));
        assert_eq!(tracker.get("my-topic", 2), Some(7));
        assert_eq!(tracker.get("trailing-hyphen-", 3), Some(9));
        assert_eq!(tracker.partition_count(), 3);
    }

    #[test]
    fn from_offset_map_ignores_reserved_partition_baselines() {
        let map = HashMap::from([
            ("events:0".to_string(), "100".to_string()),
            (
                format!("{KAFKA_PARTITION_BASELINE_PREFIX}events:0"),
                "7".to_string(),
            ),
            (
                format!("{KAFKA_PARTITION_BASELINE_PREFIX}events:1"),
                "9".to_string(),
            ),
        ]);

        let tracker = OffsetTracker::try_from_offset_map(&map).unwrap();
        assert_eq!(tracker.get("events", 0), Some(100));
        assert_eq!(tracker.get("events", 1), None);
        assert_eq!(tracker.partition_count(), 1);
    }

    #[test]
    fn checkpoint_roundtrips_topic_ending_in_hyphen() {
        let mut tracker = OffsetTracker::new();
        tracker.update("trailing-hyphen-", 3, 9);

        let checkpoint = tracker.to_checkpoint();
        assert_eq!(checkpoint.get_offset("trailing-hyphen-:3"), Some("9"));

        let restored = OffsetTracker::try_from_checkpoint(&checkpoint).unwrap();
        assert_eq!(restored.get("trailing-hyphen-", 3), Some(9));
    }

    #[test]
    fn try_from_offset_map_rejects_malformed_entries() {
        for (key, value) in [
            ("bad-partition:x", "10"),
            ("noseparator", "10"),
            ("events-0", "10"),
            ("offset:0", "notanumber"),
            ("negative-partition:-1", "10"),
            ("negative-offset:0", "-1"),
            ("noncanonical-partition:00", "10"),
            ("noncanonical-offset:0", "010"),
            ("invalid:topic:0", "10"),
        ] {
            let map = HashMap::from([(key.to_string(), value.to_string())]);
            assert!(
                OffsetTracker::try_from_offset_map(&map).is_err(),
                "malformed handoff entry {key}={value} was accepted"
            );
        }
    }

    #[test]
    fn from_offset_map_empty_is_empty() {
        let tracker = OffsetTracker::try_from_offset_map(&HashMap::new()).unwrap();
        assert_eq!(tracker.partition_count(), 0);
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
        let restored = OffsetTracker::try_from_checkpoint(&cp).unwrap();

        assert_eq!(restored.get("topic-a", 0), Some(10));
        assert_eq!(restored.get("topic-b", 0), Some(20));
    }

    #[test]
    fn test_checkpoint_metadata() {
        let tracker = OffsetTracker::new();
        let cp = tracker.to_checkpoint();
        assert_eq!(cp.get_metadata("connector"), Some("kafka"));
    }

    #[test]
    fn test_retain_assigned() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 1, 200);
        tracker.update("events", 2, 300);
        tracker.update("orders", 0, 50);

        let mut assigned = HashSet::new();
        assigned.insert(("events".to_string(), 0));
        assigned.insert(("events".to_string(), 2));
        // orders partition 0 and events partition 1 are NOT assigned (revoked)

        tracker.retain_assigned(&assigned);

        assert_eq!(tracker.get("events", 0), Some(100));
        assert_eq!(tracker.get("events", 1), None); // removed
        assert_eq!(tracker.get("events", 2), Some(300));
        assert_eq!(tracker.get("orders", 0), None); // removed
        assert_eq!(tracker.partition_count(), 2);
    }

    #[test]
    fn test_retain_assigned_empty_set_clears_all() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);

        tracker.retain_assigned(&HashSet::new());

        assert_eq!(tracker.partition_count(), 0);
    }

    #[test]
    fn test_to_checkpoint_filtered() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 1, 200);
        tracker.update("orders", 0, 50);

        let mut assigned = HashSet::new();
        assigned.insert(("events".to_string(), 0));
        assigned.insert(("orders".to_string(), 0));

        let cp = tracker.to_checkpoint_filtered(&assigned);

        assert_eq!(cp.get_offset("events:0"), Some("100"));
        assert_eq!(cp.get_offset("events:1"), None); // filtered out
        assert_eq!(cp.get_offset("orders:0"), Some("50"));
    }

    #[test]
    fn test_to_checkpoint_filtered_empty_returns_empty() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 100);
        tracker.update("events", 1, 200);

        // Empty assigned set → no owned partitions → empty checkpoint
        let cp = tracker.to_checkpoint_filtered(&HashSet::new());

        assert!(cp.is_empty());
    }

    #[test]
    fn test_to_seek_tpl_known_and_unknown() {
        let mut tracker = OffsetTracker::new();
        tracker.update("events", 0, 99);
        tracker.update("events", 1, 199);

        let assigned = vec![
            ("events".to_string(), 0),
            ("events".to_string(), 1),
            ("events".to_string(), 2), // unknown — omitted from seek TPL
        ];
        let tpl = tracker.to_seek_tpl(&assigned);
        // Only partitions with known offsets are included.
        assert_eq!(tpl.count(), 2);

        for elem in tpl.elements() {
            match (elem.topic(), elem.partition()) {
                ("events", 0) => assert_eq!(elem.offset(), Offset::Offset(100)),
                ("events", 1) => assert_eq!(elem.offset(), Offset::Offset(200)),
                _ => panic!("unexpected partition {}-{}", elem.topic(), elem.partition()),
            }
        }
    }

    #[test]
    fn test_to_seek_tpl_empty_tracker() {
        let tracker = OffsetTracker::new();
        let assigned = vec![("events".to_string(), 0)];
        let tpl = tracker.to_seek_tpl(&assigned);
        // No known offsets → empty TPL.
        assert_eq!(tpl.count(), 0);
    }
}
