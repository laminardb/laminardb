//! Source checkpoints: the minimal state needed to resume a connector
//! where it left off after a restart (Kafka offsets, CDC LSN/GTID, etc.).
#![allow(clippy::disallowed_types)] // cold path: connector checkpoint

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

enum OffsetTree {
    Leaf(Arc<str>),
    Branch { older: Arc<Self>, newer: Arc<Self> },
}

impl OffsetTree {
    fn write_to(&self, output: &mut String, separator: &str, first: &mut bool) {
        match self {
            Self::Leaf(value) => {
                if *first {
                    *first = false;
                } else {
                    output.push_str(separator);
                }
                output.push_str(value);
            }
            Self::Branch { older, newer } => {
                older.write_to(output, separator, first);
                newer.write_to(output, separator, first);
            }
        }
    }
}

/// Immutable, append-only serialized value for a large source offset.
///
/// Cloning or taking a checkpoint snapshot is `O(1)`. Appending path-copies at
/// most `O(log N)` tree roots, and materialization preserves insertion order.
#[derive(Clone)]
pub struct PersistentOffset {
    /// Binary-counter forest. Slot `n` holds an immutable tree with `2^n`
    /// fragments; roots from highest to lowest are chronological.
    trees: Arc<Vec<Option<Arc<OffsetTree>>>>,
    prefix: Arc<str>,
    separator: Arc<str>,
    suffix: Arc<str>,
    fragment_count: usize,
    fragment_bytes: usize,
}

impl PersistentOffset {
    /// Creates an empty persistent serialized value.
    ///
    /// For example, a JSON array uses `"["`, `","`, and
    /// `"]"`. Each pushed fragment must itself be a complete JSON value.
    #[must_use]
    pub fn new(
        prefix: impl Into<Arc<str>>,
        separator: impl Into<Arc<str>>,
        suffix: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            trees: Arc::new(Vec::new()),
            prefix: prefix.into(),
            separator: separator.into(),
            suffix: suffix.into(),
            fragment_count: 0,
            fragment_bytes: 0,
        }
    }

    /// Appends one already-serialized fragment in `O(log N)` without changing
    /// any previously cloned snapshot.
    pub fn push_fragment(&mut self, fragment: impl Into<Arc<str>>) {
        let value = fragment.into();
        // These counters are allocation hints/diagnostics, never correctness
        // state. Saturation keeps an impossible address-space overflow from
        // turning connector input into a process panic.
        self.fragment_bytes = self.fragment_bytes.saturating_add(value.len());
        self.fragment_count = self.fragment_count.saturating_add(1);

        let trees = Arc::make_mut(&mut self.trees);
        let mut carry = Arc::new(OffsetTree::Leaf(value));
        let mut level = 0;
        loop {
            if level == trees.len() {
                trees.push(Some(carry));
                break;
            }
            if let Some(older) = trees[level].take() {
                carry = Arc::new(OffsetTree::Branch {
                    older,
                    newer: carry,
                });
                level += 1;
            } else {
                trees[level] = Some(carry);
                break;
            }
        }
    }

    /// Number of serialized fragments in this value.
    #[must_use]
    pub const fn fragment_count(&self) -> usize {
        self.fragment_count
    }

    fn materialize(&self) -> String {
        let separator_bytes = self
            .separator
            .len()
            .saturating_mul(self.fragment_count.saturating_sub(1));
        let capacity = self
            .prefix
            .len()
            .saturating_add(self.fragment_bytes)
            .saturating_add(separator_bytes)
            .saturating_add(self.suffix.len());
        let mut output = String::with_capacity(capacity);
        output.push_str(&self.prefix);
        let mut first = true;
        for tree in self.trees.iter().rev().flatten() {
            tree.write_to(&mut output, &self.separator, &mut first);
        }
        output.push_str(&self.suffix);
        output
    }
}

impl fmt::Debug for PersistentOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistentOffset")
            .field("fragment_count", &self.fragment_count)
            .field("fragment_bytes", &self.fragment_bytes)
            .finish_non_exhaustive()
    }
}

/// Checkpoint state for a source connector.
///
/// Captures the connector's position using string key-value pairs.
/// This is flexible enough to represent:
/// - Kafka: `{"partition-0": "1234", "partition-1": "5678"}`
/// - `PostgreSQL` CDC: `{"lsn": "0/1234ABCD"}`
/// - File: `{"manifest": "[...]", "file_progress": "..."}`
///
/// This payload deliberately has no checkpoint epoch or attempt identifier.
/// The barrier protocol and top-level manifest bind it to an exact attempt.
/// Ordinary offsets remain eager; connectors with large append-only positions
/// can opt into [`PersistentOffset`] without changing the compatibility API.
pub struct SourceCheckpoint {
    /// Small connector-specific offset data.
    offsets: HashMap<String, String>,

    /// Large values represented as immutable serialized fragment logs.
    persistent_offsets: HashMap<String, PersistentOffset>,

    /// Compatibility view for callers that explicitly request all offsets.
    materialized_offsets: Arc<OnceLock<HashMap<String, String>>>,

    /// Optional metadata for the checkpoint.
    metadata: HashMap<String, String>,
}

impl Clone for SourceCheckpoint {
    fn clone(&self) -> Self {
        Self {
            offsets: self.offsets.clone(),
            persistent_offsets: self.persistent_offsets.clone(),
            materialized_offsets: Arc::clone(&self.materialized_offsets),
            metadata: self.metadata.clone(),
        }
    }
}

impl Default for SourceCheckpoint {
    fn default() -> Self {
        Self {
            offsets: HashMap::new(),
            persistent_offsets: HashMap::new(),
            materialized_offsets: Arc::new(OnceLock::new()),
            metadata: HashMap::new(),
        }
    }
}

impl fmt::Debug for SourceCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceCheckpoint")
            .field("offsets", &self.offsets)
            .field(
                "persistent_offset_keys",
                &self.persistent_offsets.keys().collect::<Vec<_>>(),
            )
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}

impl PartialEq for SourceCheckpoint {
    fn eq(&self, other: &Self) -> bool {
        self.offsets() == other.offsets() && self.metadata == other.metadata
    }
}

impl Eq for SourceCheckpoint {}

impl SourceCheckpoint {
    /// Creates an empty checkpoint.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a checkpoint with the given connector-specific offsets.
    #[must_use]
    pub fn with_offsets(offsets: HashMap<String, String>) -> Self {
        Self {
            offsets,
            persistent_offsets: HashMap::new(),
            materialized_offsets: Arc::new(OnceLock::new()),
            metadata: HashMap::new(),
        }
    }

    /// Sets an offset value.
    pub fn set_offset(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        self.persistent_offsets.remove(&key);
        self.offsets.insert(key, value.into());
        self.invalidate_materialized_offsets();
    }

    /// Sets a large offset value backed by an immutable fragment log.
    ///
    /// This replaces any eager value under the same key. The value remains
    /// lazy across checkpoint clones until [`Self::get_offset`],
    /// [`Self::offsets`], or [`Self::durable_offsets`] explicitly requests it.
    pub fn set_persistent_offset(&mut self, key: impl Into<String>, value: PersistentOffset) {
        let key = key.into();
        self.offsets.remove(&key);
        self.persistent_offsets.insert(key, value);
        self.invalidate_materialized_offsets();
    }

    /// Gets an offset value.
    #[must_use]
    pub fn get_offset(&self, key: &str) -> Option<&str> {
        if let Some(value) = self.offsets.get(key) {
            return Some(value);
        }
        if !self.persistent_offsets.contains_key(key) {
            return None;
        }
        self.offsets().get(key).map(String::as_str)
    }

    /// Returns all offsets.
    #[must_use]
    pub fn offsets(&self) -> &HashMap<String, String> {
        if self.persistent_offsets.is_empty() {
            return &self.offsets;
        }
        self.materialized_offsets.get_or_init(|| {
            let mut offsets = self.offsets.clone();
            offsets.extend(
                self.persistent_offsets
                    .iter()
                    .map(|(key, value)| (key.clone(), value.materialize())),
            );
            offsets
        })
    }

    /// Materializes all offsets into an owned map for durable checkpoint
    /// conversion.
    ///
    /// If the compatibility view has not been requested, persistent values are
    /// written directly into the returned map and are not retained as a second
    /// full copy inside this checkpoint.
    #[must_use]
    pub fn durable_offsets(&self) -> HashMap<String, String> {
        if let Some(offsets) = self.materialized_offsets.get() {
            return offsets.clone();
        }
        let mut offsets = self.offsets.clone();
        offsets.extend(
            self.persistent_offsets
                .iter()
                .map(|(key, value)| (key.clone(), value.materialize())),
        );
        offsets
    }

    /// Sets metadata on the checkpoint.
    pub fn set_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    /// Gets metadata from the checkpoint.
    #[must_use]
    pub fn get_metadata(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(String::as_str)
    }

    /// Returns all metadata.
    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Returns `true` if the checkpoint has no offsets.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty() && self.persistent_offsets.is_empty()
    }

    fn invalidate_materialized_offsets(&mut self) {
        self.materialized_offsets = Arc::new(OnceLock::new());
    }

    #[cfg(test)]
    fn offsets_are_materialized(&self) -> bool {
        self.materialized_offsets.get().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_checkpoint_basic() {
        let mut cp = SourceCheckpoint::new();
        cp.set_offset("partition-0", "1234");
        cp.set_offset("partition-1", "5678");

        assert_eq!(cp.get_offset("partition-0"), Some("1234"));
        assert_eq!(cp.get_offset("partition-1"), Some("5678"));
        assert_eq!(cp.get_offset("partition-2"), None);
        assert!(!cp.is_empty());
    }

    #[test]
    fn test_source_checkpoint_with_offsets() {
        let mut offsets = HashMap::new();
        offsets.insert("lsn".to_string(), "0/1234ABCD".to_string());

        let cp = SourceCheckpoint::with_offsets(offsets);
        assert_eq!(cp.get_offset("lsn"), Some("0/1234ABCD"));
    }

    #[test]
    fn test_source_checkpoint_metadata() {
        let mut cp = SourceCheckpoint::new();
        cp.set_metadata("connector", "kafka");
        cp.set_metadata("topic", "events");

        assert_eq!(cp.get_metadata("connector"), Some("kafka"));
        assert_eq!(cp.get_metadata("topic"), Some("events"));
    }

    #[test]
    fn test_empty_checkpoint() {
        let cp = SourceCheckpoint::new();
        assert!(cp.is_empty());
    }

    #[test]
    fn persistent_offset_is_lazy_and_durable_conversion_does_not_cache_a_copy() {
        let mut value = PersistentOffset::new("[", ",", "]");
        value.push_fragment(r#""first""#);
        value.push_fragment(r#""second""#);
        let mut cp = SourceCheckpoint::new();
        cp.set_offset("small", "7");
        cp.set_persistent_offset("large", value);

        assert!(!cp.offsets_are_materialized());
        assert_eq!(cp.get_offset("small"), Some("7"));
        assert!(!cp.offsets_are_materialized());
        let durable = cp.durable_offsets();
        assert_eq!(
            durable.get("large").map(String::as_str),
            Some(r#"["first","second"]"#)
        );
        assert!(!cp.offsets_are_materialized());

        assert_eq!(cp.get_offset("large"), Some(r#"["first","second"]"#));
        assert!(cp.offsets_are_materialized());
    }

    #[test]
    fn persistent_offset_snapshots_are_immutable() {
        let mut value = PersistentOffset::new("[", ",", "]");
        value.push_fragment("1");
        let snapshot = value.clone();
        value.push_fragment("2");

        let mut old = SourceCheckpoint::new();
        old.set_persistent_offset("values", snapshot);
        let mut new = SourceCheckpoint::new();
        new.set_persistent_offset("values", value);
        assert_eq!(old.get_offset("values"), Some("[1]"));
        assert_eq!(new.get_offset("values"), Some("[1,2]"));
    }

    #[test]
    fn persistent_offset_preserves_order_across_tree_carries() {
        let mut value = PersistentOffset::new("[", ",", "]");
        for fragment in 0..19 {
            value.push_fragment(fragment.to_string());
        }
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_persistent_offset("values", value);
        assert_eq!(
            checkpoint.get_offset("values"),
            Some("[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]")
        );
    }

    #[test]
    fn eager_offsets_do_not_allocate_a_compatibility_copy() {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("partition", "42");
        assert_eq!(
            checkpoint.offsets().get("partition").map(String::as_str),
            Some("42")
        );
        assert!(!checkpoint.offsets_are_materialized());
    }

    #[test]
    fn mutating_a_checkpoint_does_not_change_its_clone() {
        let mut original = SourceCheckpoint::new();
        original.set_offset("position", "1");
        let snapshot = original.clone();
        original.set_offset("position", "2");

        assert_eq!(snapshot.get_offset("position"), Some("1"));
        assert_eq!(original.get_offset("position"), Some("2"));
    }
}
