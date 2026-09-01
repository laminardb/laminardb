//! Source checkpoints: the minimal state needed to resume a connector
//! where it left off after a restart (Kafka offsets, CDC LSN/GTID, etc.).
#![allow(clippy::disallowed_types)] // cold path: connector checkpoint

use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};

use crate::error::ConnectorError;

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
/// - Kafka: `{"events:0": "1234", "events:1": "5678"}`
/// - `PostgreSQL` CDC: `{"lsn": "0/1234ABCD"}`
/// - File: `{"manifest": "[...]", "file_progress": "..."}`
///
/// This payload deliberately has no checkpoint epoch or attempt identifier.
/// The barrier protocol and top-level manifest bind it to an exact attempt.
/// Ordinary offsets remain eager; connectors with large append-only positions
/// can opt into [`PersistentOffset`] while retaining the same accessors.
pub struct SourceCheckpoint {
    /// Small connector-specific offset data.
    offsets: HashMap<String, String>,

    /// Large values represented as immutable serialized fragment logs.
    persistent_offsets: HashMap<String, PersistentOffset>,

    /// Materialized view for callers that explicitly request all offsets.
    materialized_offsets: Arc<OnceLock<HashMap<String, String>>>,

    /// Optional metadata for the checkpoint.
    metadata: HashMap<String, String>,

    /// Canonically ordered opaque identities of the input channels owned by this cut.
    input_channels: Option<Arc<[Vec<u8>]>>,

    /// Provider-neutral assignment version that owns this source cut.
    assignment_version: Option<NonZeroU64>,
}

/// Offset changes for one assignment-scoped source batch.
///
/// The input-channel allocation is shared with the complete checkpoint published for the
/// assignment. A runtime must merge this delta into that exact checkpoint before durability.
/// `Some(value)` upserts an offset; `None` removes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceCheckpointDelta {
    assignment_version: NonZeroU64,
    input_channels: Arc<[Vec<u8>]>,
    offset_changes: HashMap<String, Option<String>>,
}

impl SourceCheckpointDelta {
    /// Creates an incremental cursor for an already-published assignment.
    ///
    /// # Errors
    /// Returns an error when the delta has no changed offsets.
    pub fn new(
        assignment_version: NonZeroU64,
        input_channels: Arc<[Vec<u8>]>,
        offset_changes: HashMap<String, Option<String>>,
    ) -> Result<Self, ConnectorError> {
        if offset_changes.is_empty() {
            return Err(ConnectorError::InvalidState {
                expected: "at least one changed source offset".into(),
                actual: "an empty incremental source cursor".into(),
            });
        }
        Ok(Self {
            assignment_version,
            input_channels,
            offset_changes,
        })
    }

    /// Assignment publication owning this delta.
    #[must_use]
    pub const fn assignment_version(&self) -> NonZeroU64 {
        self.assignment_version
    }

    /// Shared input-channel inventory for the owning assignment.
    #[must_use]
    pub const fn input_channels_arc(&self) -> &Arc<[Vec<u8>]> {
        &self.input_channels
    }

    #[cfg(all(test, feature = "kafka"))]
    pub(crate) fn changes(&self) -> &HashMap<String, Option<String>> {
        &self.offset_changes
    }

    /// Validates that this delta extends the supplied complete cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the base cursor is unbound or belongs to a different assignment or
    /// input-channel roster.
    pub fn validate_base(&self, base: &SourceCheckpoint) -> Result<(), ConnectorError> {
        let Some(base_version) = base.assignment_version else {
            return Err(ConnectorError::InvalidState {
                expected: format!(
                    "complete source checkpoint for assignment {}",
                    self.assignment_version
                ),
                actual: "an unbound source checkpoint".into(),
            });
        };
        if base_version != self.assignment_version {
            return Err(ConnectorError::InvalidState {
                expected: format!("source assignment {}", self.assignment_version),
                actual: format!("source assignment {base_version}"),
            });
        }
        let Some(base_channels) = base.input_channels.as_ref() else {
            return Err(ConnectorError::InvalidState {
                expected: "a complete source checkpoint with input channels".into(),
                actual: "a source checkpoint without input channels".into(),
            });
        };
        if !Arc::ptr_eq(base_channels, &self.input_channels)
            && base_channels.as_ref() != self.input_channels.as_ref()
        {
            return Err(ConnectorError::InvalidState {
                expected: "the input-channel roster of the complete source checkpoint".into(),
                actual: "a different incremental source roster".into(),
            });
        }
        Ok(())
    }

    /// Coalesces a later batch delta from the same assignment.
    ///
    /// # Errors
    ///
    /// Returns an error when `newer` belongs to a different assignment or input-channel roster.
    pub fn merge(&mut self, newer: Self) -> Result<(), ConnectorError> {
        if self.assignment_version != newer.assignment_version
            || (!Arc::ptr_eq(&self.input_channels, &newer.input_channels)
                && self.input_channels.as_ref() != newer.input_channels.as_ref())
        {
            return Err(ConnectorError::InvalidState {
                expected: format!(
                    "source assignment {} and its input channels",
                    self.assignment_version
                ),
                actual: format!(
                    "source assignment {} or a different input-channel roster",
                    newer.assignment_version
                ),
            });
        }
        self.offset_changes.extend(newer.offset_changes);
        Ok(())
    }
}

impl Clone for SourceCheckpoint {
    fn clone(&self) -> Self {
        Self {
            offsets: self.offsets.clone(),
            persistent_offsets: self.persistent_offsets.clone(),
            materialized_offsets: Arc::clone(&self.materialized_offsets),
            metadata: self.metadata.clone(),
            input_channels: self.input_channels.clone(),
            assignment_version: self.assignment_version,
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
            input_channels: None,
            assignment_version: None,
        }
    }
}

impl fmt::Debug for SourceCheckpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let input_channel_count = self.input_channels.as_ref().map(|channels| channels.len());
        f.debug_struct("SourceCheckpoint")
            .field("offsets", &self.offsets)
            .field(
                "persistent_offset_keys",
                &self.persistent_offsets.keys().collect::<Vec<_>>(),
            )
            .field("metadata", &self.metadata)
            .field("input_channel_count", &input_channel_count)
            .field("assignment_version", &self.assignment_version)
            .finish_non_exhaustive()
    }
}

impl PartialEq for SourceCheckpoint {
    fn eq(&self, other: &Self) -> bool {
        self.offsets() == other.offsets()
            && self.metadata == other.metadata
            && self.input_channels == other.input_channels
            && self.assignment_version == other.assignment_version
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
            input_channels: None,
            assignment_version: None,
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
    /// If the materialized view has not been requested, persistent values are
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

    /// Replaces the complete input-channel inventory for this source cut.
    ///
    /// # Errors
    ///
    /// Returns an error when a channel is empty, reserved, duplicated, or out of canonical order.
    pub fn set_input_channels(
        &mut self,
        channels: impl Into<Arc<[Vec<u8>]>>,
    ) -> Result<(), ConnectorError> {
        let channels = channels.into();
        if channels.iter().any(Vec::is_empty) {
            return Err(ConnectorError::InvalidState {
                expected: "non-empty opaque input-channel identities".into(),
                actual: "an empty input-channel identity".into(),
            });
        }
        if channels
            .iter()
            .any(|channel| channel == laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL)
        {
            return Err(ConnectorError::InvalidState {
                expected: "physical input-channel identities".into(),
                actual: "the reserved logical watermark channel".into(),
            });
        }
        if !channels.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(ConnectorError::InvalidState {
                expected: "strictly ordered unique input-channel identities".into(),
                actual: "noncanonical input-channel inventory".into(),
            });
        }
        self.input_channels = Some(channels);
        Ok(())
    }

    /// Returns the canonical opaque input-channel inventory, when the connector declares one.
    #[must_use]
    pub fn input_channels(&self) -> Option<&[Vec<u8>]> {
        self.input_channels.as_deref()
    }

    /// Returns the shared input-channel inventory for lock-free runtime installation.
    #[must_use]
    pub const fn input_channels_arc(&self) -> Option<&Arc<[Vec<u8>]>> {
        self.input_channels.as_ref()
    }

    /// Binds this source cut to the exact partition-assignment publication that owns it.
    pub fn bind_assignment_version(&mut self, assignment_version: NonZeroU64) {
        self.assignment_version = Some(assignment_version);
    }

    /// Returns the partition-assignment publication that owns this source cut.
    #[must_use]
    pub const fn assignment_version(&self) -> Option<NonZeroU64> {
        self.assignment_version
    }

    /// Applies changed offsets from the same assignment without cloning the complete cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when the delta does not extend this checkpoint's assignment and channel
    /// roster.
    pub fn apply_delta(&mut self, delta: SourceCheckpointDelta) -> Result<(), ConnectorError> {
        delta.validate_base(self)?;
        for (key, value) in delta.offset_changes {
            self.persistent_offsets.remove(&key);
            if let Some(value) = value {
                self.offsets.insert(key, value);
            } else {
                self.offsets.remove(&key);
            }
        }
        self.invalidate_materialized_offsets();
        Ok(())
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
        cp.set_offset("events:0", "1234");
        cp.set_offset("events:1", "5678");

        assert_eq!(cp.get_offset("events:0"), Some("1234"));
        assert_eq!(cp.get_offset("events:1"), Some("5678"));
        assert_eq!(cp.get_offset("events:2"), None);
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
    fn input_channels_are_canonical_and_part_of_checkpoint_identity() {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint
            .set_input_channels(vec![b"partition-1".to_vec(), b"partition-2".to_vec()])
            .unwrap();

        assert_eq!(
            checkpoint.input_channels(),
            Some([b"partition-1".to_vec(), b"partition-2".to_vec()].as_slice())
        );
        assert_ne!(checkpoint, SourceCheckpoint::new());
        let cloned = checkpoint.clone();
        assert_eq!(cloned, checkpoint);
        assert!(Arc::ptr_eq(
            cloned.input_channels.as_ref().unwrap(),
            checkpoint.input_channels.as_ref().unwrap()
        ));

        assert!(SourceCheckpoint::new()
            .set_input_channels(vec![b"partition-2".to_vec(), b"partition-1".to_vec()])
            .is_err());
        assert!(SourceCheckpoint::new()
            .set_input_channels(vec![Vec::new()])
            .is_err());
        assert!(SourceCheckpoint::new()
            .set_input_channels(vec![
                laminar_core::checkpoint::SINGLETON_WATERMARK_CHANNEL.to_vec(),
            ])
            .is_err());

        let mut owned_empty = SourceCheckpoint::new();
        owned_empty.set_input_channels(Vec::new()).unwrap();
        assert_eq!(owned_empty.input_channels(), Some([].as_slice()));
        assert_ne!(owned_empty, SourceCheckpoint::new());
    }

    #[test]
    fn source_checkpoint_assignment_version_is_typed_and_part_of_identity() {
        let mut checkpoint = SourceCheckpoint::new();
        assert_eq!(checkpoint.assignment_version(), None);

        let version = NonZeroU64::new(7).unwrap();
        checkpoint.bind_assignment_version(version);
        assert_eq!(checkpoint.assignment_version(), Some(version));
        assert_eq!(checkpoint.clone(), checkpoint);
        assert!(format!("{checkpoint:?}").contains("assignment_version: Some(7)"));

        let unbound = SourceCheckpoint::new();
        assert_ne!(checkpoint, unbound);
        assert_eq!(SourceCheckpoint::default().assignment_version(), None);
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
    fn eager_offsets_do_not_allocate_a_materialized_copy() {
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
