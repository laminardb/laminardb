//! WAL entry types with epoch-based ordering for per-core WAL.

use std::cmp::Ordering;
use std::collections::HashMap;

// WAL entry types with derive macros
mod entry_types {
    #![allow(missing_docs)] // Allow for derive-generated code

    use std::collections::HashMap;

    use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

    /// Operations that can be logged in the WAL.
    #[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
    pub enum WalOperation {
        /// Put a key-value pair.
        Put {
            /// The key.
            key: Vec<u8>,
            /// The value.
            value: Vec<u8>,
        },
        /// Delete a key.
        Delete {
            /// The key to delete.
            key: Vec<u8>,
        },
        /// Checkpoint marker.
        Checkpoint {
            /// Checkpoint ID.
            id: u64,
        },
        /// Commit offsets for exactly-once semantics.
        Commit {
            /// Map of topic/partition to offset.
            offsets: HashMap<String, u64>,
            /// Current watermark at commit time.
            watermark: Option<i64>,
        },
        /// Barrier for epoch boundary.
        EpochBarrier {
            /// The epoch that just completed.
            epoch: u64,
        },
    }

    /// Per-core WAL entry with epoch for cross-core ordering.
    ///
    /// Entries are ordered by (epoch, `timestamp_ns`) for deterministic recovery.
    /// The 32-byte compact layout is designed for cache efficiency.
    #[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
    pub struct PerCoreWalEntry {
        /// Global epoch (for ordering during recovery).
        pub epoch: u64,
        /// Core-local sequence number (monotonically increasing per core).
        pub sequence: u64,
        /// Core ID that wrote this entry.
        pub core_id: u16,
        /// Timestamp in nanoseconds since Unix epoch.
        pub timestamp_ns: i64,
        /// The actual operation.
        pub operation: WalOperation,
    }
}

pub use entry_types::{PerCoreWalEntry, WalOperation};

impl PerCoreWalEntry {
    /// Creates a new Put entry.
    ///
    /// # Arguments
    ///
    /// * `timestamp_ns` - Pre-cached timestamp from [`now_ns()`](Self::now_ns)
    #[must_use]
    pub fn put(
        core_id: u16,
        epoch: u64,
        sequence: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp_ns: i64,
    ) -> Self {
        Self {
            epoch,
            sequence,
            core_id,
            timestamp_ns,
            operation: WalOperation::Put { key, value },
        }
    }

    /// Creates a new Delete entry.
    ///
    /// # Arguments
    ///
    /// * `timestamp_ns` - Pre-cached timestamp from [`now_ns()`](Self::now_ns)
    #[must_use]
    pub fn delete(
        core_id: u16,
        epoch: u64,
        sequence: u64,
        key: Vec<u8>,
        timestamp_ns: i64,
    ) -> Self {
        Self {
            epoch,
            sequence,
            core_id,
            timestamp_ns,
            operation: WalOperation::Delete { key },
        }
    }

    /// Creates a new Checkpoint entry.
    ///
    /// # Arguments
    ///
    /// * `timestamp_ns` - Pre-cached timestamp from [`now_ns()`](Self::now_ns)
    #[must_use]
    pub fn checkpoint(
        core_id: u16,
        epoch: u64,
        sequence: u64,
        checkpoint_id: u64,
        timestamp_ns: i64,
    ) -> Self {
        Self {
            epoch,
            sequence,
            core_id,
            timestamp_ns,
            operation: WalOperation::Checkpoint { id: checkpoint_id },
        }
    }

    /// Creates a new Commit entry.
    ///
    /// # Arguments
    ///
    /// * `timestamp_ns` - Pre-cached timestamp from [`now_ns()`](Self::now_ns)
    #[must_use]
    pub fn commit(
        core_id: u16,
        epoch: u64,
        sequence: u64,
        offsets: HashMap<String, u64>,
        watermark: Option<i64>,
        timestamp_ns: i64,
    ) -> Self {
        Self {
            epoch,
            sequence,
            core_id,
            timestamp_ns,
            operation: WalOperation::Commit { offsets, watermark },
        }
    }

    /// Creates a new `EpochBarrier` entry.
    ///
    /// # Arguments
    ///
    /// * `timestamp_ns` - Pre-cached timestamp from [`now_ns()`](Self::now_ns)
    #[must_use]
    pub fn epoch_barrier(core_id: u16, epoch: u64, sequence: u64, timestamp_ns: i64) -> Self {
        Self {
            epoch,
            sequence,
            core_id,
            timestamp_ns,
            operation: WalOperation::EpochBarrier { epoch },
        }
    }

    /// Returns true if this is a Put operation.
    #[must_use]
    pub fn is_put(&self) -> bool {
        matches!(self.operation, WalOperation::Put { .. })
    }

    /// Returns true if this is a Delete operation.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self.operation, WalOperation::Delete { .. })
    }

    /// Returns true if this is a Checkpoint operation.
    #[must_use]
    pub fn is_checkpoint(&self) -> bool {
        matches!(self.operation, WalOperation::Checkpoint { .. })
    }

    /// Returns true if this is a state-modifying operation (Put or Delete).
    #[must_use]
    pub fn is_state_operation(&self) -> bool {
        self.is_put() || self.is_delete()
    }

    /// Gets the key if this is a Put or Delete operation.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        match &self.operation {
            WalOperation::Put { key, .. } | WalOperation::Delete { key } => Some(key),
            _ => None,
        }
    }

    /// Gets the value if this is a Put operation.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        match &self.operation {
            WalOperation::Put { value, .. } => Some(value),
            _ => None,
        }
    }

    /// Returns current timestamp in nanoseconds.
    ///
    /// Call once and pass the result to entry constructors to avoid
    /// repeated `SystemTime::now()` syscalls in tight loops.
    #[must_use]
    pub fn now_ns() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
            #[allow(clippy::cast_possible_truncation)] // i64 ns won't overflow for ~292 years
            let ns = d.as_nanos() as i64;
            ns
        })
    }
}

impl Ord for PerCoreWalEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by epoch first, then by timestamp within epoch
        match self.epoch.cmp(&other.epoch) {
            Ordering::Equal => {
                // Within same epoch, order by timestamp
                match self.timestamp_ns.cmp(&other.timestamp_ns) {
                    Ordering::Equal => {
                        // Tie-breaker: use core_id then sequence
                        match self.core_id.cmp(&other.core_id) {
                            Ordering::Equal => self.sequence.cmp(&other.sequence),
                            ord => ord,
                        }
                    }
                    ord => ord,
                }
            }
            ord => ord,
        }
    }
}

impl PartialOrd for PerCoreWalEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_ordering_by_epoch() {
        let e1 = PerCoreWalEntry {
            epoch: 1,
            sequence: 100,
            core_id: 0,
            timestamp_ns: 1000,
            operation: WalOperation::Put {
                key: vec![1],
                value: vec![1],
            },
        };
        let e2 = PerCoreWalEntry {
            epoch: 2,
            sequence: 1,
            core_id: 1,
            timestamp_ns: 500, // Earlier timestamp but later epoch
            operation: WalOperation::Put {
                key: vec![2],
                value: vec![2],
            },
        };

        assert!(e1 < e2); // Epoch takes precedence
    }

    #[test]
    fn test_entry_ordering_by_timestamp() {
        let e1 = PerCoreWalEntry {
            epoch: 1,
            sequence: 1,
            core_id: 0,
            timestamp_ns: 1000,
            operation: WalOperation::Put {
                key: vec![1],
                value: vec![1],
            },
        };
        let e2 = PerCoreWalEntry {
            epoch: 1,
            sequence: 2,
            core_id: 1,
            timestamp_ns: 2000,
            operation: WalOperation::Put {
                key: vec![2],
                value: vec![2],
            },
        };

        assert!(e1 < e2); // Same epoch, timestamp determines order
    }

    #[test]
    fn test_entry_ordering_tiebreaker() {
        let e1 = PerCoreWalEntry {
            epoch: 1,
            sequence: 1,
            core_id: 0,
            timestamp_ns: 1000,
            operation: WalOperation::Put {
                key: vec![1],
                value: vec![1],
            },
        };
        let e2 = PerCoreWalEntry {
            epoch: 1,
            sequence: 2,
            core_id: 1,
            timestamp_ns: 1000, // Same timestamp
            operation: WalOperation::Put {
                key: vec![2],
                value: vec![2],
            },
        };

        assert!(e1 < e2); // Same epoch and timestamp, core_id determines order
    }

    #[test]
    fn test_entry_constructors() {
        let ts = PerCoreWalEntry::now_ns();

        let put = PerCoreWalEntry::put(0, 1, 1, b"key".to_vec(), b"value".to_vec(), ts);
        assert!(put.is_put());
        assert!(!put.is_delete());
        assert_eq!(put.key(), Some(b"key".as_slice()));
        assert_eq!(put.value(), Some(b"value".as_slice()));

        let delete = PerCoreWalEntry::delete(1, 1, 2, b"key2".to_vec(), ts);
        assert!(delete.is_delete());
        assert!(!delete.is_put());
        assert_eq!(delete.key(), Some(b"key2".as_slice()));
        assert!(delete.value().is_none());

        let checkpoint = PerCoreWalEntry::checkpoint(0, 1, 3, 100, ts);
        assert!(checkpoint.is_checkpoint());
        assert!(!checkpoint.is_state_operation());
    }

    #[test]
    fn test_sorting_multiple_entries() {
        let mut entries = [
            PerCoreWalEntry {
                epoch: 2,
                sequence: 1,
                core_id: 0,
                timestamp_ns: 100,
                operation: WalOperation::Put {
                    key: vec![1],
                    value: vec![1],
                },
            },
            PerCoreWalEntry {
                epoch: 1,
                sequence: 2,
                core_id: 1,
                timestamp_ns: 200,
                operation: WalOperation::Put {
                    key: vec![2],
                    value: vec![2],
                },
            },
            PerCoreWalEntry {
                epoch: 1,
                sequence: 1,
                core_id: 0,
                timestamp_ns: 100,
                operation: WalOperation::Put {
                    key: vec![3],
                    value: vec![3],
                },
            },
            PerCoreWalEntry {
                epoch: 2,
                sequence: 2,
                core_id: 1,
                timestamp_ns: 200,
                operation: WalOperation::Put {
                    key: vec![4],
                    value: vec![4],
                },
            },
        ];

        entries.sort();

        // Check ordering: epoch 1 entries first (by timestamp), then epoch 2 entries
        assert_eq!(entries[0].epoch, 1);
        assert_eq!(entries[0].timestamp_ns, 100);
        assert_eq!(entries[1].epoch, 1);
        assert_eq!(entries[1].timestamp_ns, 200);
        assert_eq!(entries[2].epoch, 2);
        assert_eq!(entries[2].timestamp_ns, 100);
        assert_eq!(entries[3].epoch, 2);
        assert_eq!(entries[3].timestamp_ns, 200);
    }

    #[test]
    fn test_commit_entry() {
        let mut offsets = HashMap::new();
        offsets.insert("topic1".to_string(), 100);
        offsets.insert("topic2".to_string(), 200);

        let commit =
            PerCoreWalEntry::commit(0, 1, 1, offsets, Some(12345), PerCoreWalEntry::now_ns());

        match &commit.operation {
            WalOperation::Commit { offsets, watermark } => {
                assert_eq!(offsets.get("topic1"), Some(&100));
                assert_eq!(offsets.get("topic2"), Some(&200));
                assert_eq!(*watermark, Some(12345));
            }
            _ => panic!("Expected Commit operation"),
        }
    }

    #[test]
    fn test_epoch_barrier_entry() {
        let barrier = PerCoreWalEntry::epoch_barrier(0, 5, 100, PerCoreWalEntry::now_ns());

        match &barrier.operation {
            WalOperation::EpochBarrier { epoch } => {
                assert_eq!(*epoch, 5);
            }
            _ => panic!("Expected EpochBarrier operation"),
        }
    }
}
