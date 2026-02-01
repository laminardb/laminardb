# F-SUB-001: ChangeEvent Types

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-001 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F063 (Changelog/Retraction) |
| **Blocks** | F-SUB-002, F-SUB-003, F-SUB-004, F-SUB-005 |
| **Created** | 2026-02-01 |

## Summary

Define the `ChangeEvent` enum and supporting types that represent data changes flowing through the reactive subscription system. This is the core data model shared across all subscription features - from Ring 0 notification references to Ring 2 subscriber delivery.

Builds on the existing `ChangelogRecord<T>` and `CdcOperation` from F063, adding Arrow-native `RecordBatch` variants for zero-copy delivery and lightweight notification references for Ring 0.

**Research Reference**: [Reactive Subscriptions Research - Section 6: Incremental View Maintenance](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: `ChangeEvent` enum with variants: `Insert`, `Delete`, `Update`, `Watermark`, `Snapshot`
- **FR-2**: Arrow-native payload using `Arc<RecordBatch>` for zero-copy multicast
- **FR-3**: Lightweight `NotificationRef` for Ring 0 (no allocation, fixed-size)
- **FR-4**: Conversion from existing `ChangelogRecord<T>` and `CdcOperation` types
- **FR-5**: `ChangeEventBatch` for batched delivery (vec of events sharing a schema)
- **FR-6**: Serializable for checkpoint/recovery of subscription state

### Non-Functional Requirements

- **NFR-1**: `NotificationRef` must be `Copy` and fit in a single cache line (64 bytes)
- **NFR-2**: `ChangeEvent` clone cost must be O(1) via `Arc<RecordBatch>`
- **NFR-3**: Zero heap allocations when creating `NotificationRef`
- **NFR-4**: Ring placement: `NotificationRef` in Ring 0, `ChangeEvent` in Ring 1/2

## Technical Design

### Architecture

```
Ring 0 (Hot Path)                     Ring 1 (Background)
+----------------------------+        +----------------------------+
| NotificationRef            |        | ChangeEvent                |
| - sequence: u64            | -----> | - Insert(Arc<RecordBatch>) |
| - source_id: u32           |  SPSC  | - Delete(Arc<RecordBatch>) |
| - event_type: EventType    |  queue | - Update { old, new }      |
| - row_count: u32           |        | - Watermark(i64)           |
| - timestamp: i64           |        | - Snapshot(Arc<RecordBatch>)|
| (48 bytes, Copy, no alloc) |        +----------------------------+
+----------------------------+
```

### Data Structures

```rust
use std::sync::Arc;
use arrow::array::RecordBatch;

/// Lightweight change event type indicator.
///
/// Used in both `NotificationRef` (Ring 0) and `ChangeEvent` (Ring 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EventType {
    /// New record(s) inserted.
    Insert = 0,
    /// Record(s) deleted.
    Delete = 1,
    /// Record(s) updated (before + after pair).
    Update = 2,
    /// Watermark advancement.
    Watermark = 3,
    /// Initial snapshot delivery.
    Snapshot = 4,
}

impl EventType {
    /// Returns the Z-set weight for this event type.
    #[inline]
    pub const fn weight(&self) -> i64 {
        match self {
            Self::Insert | Self::Update | Self::Snapshot => 1,
            Self::Delete => -1,
            Self::Watermark => 0,
        }
    }

    /// Returns true if this event carries data rows.
    #[inline]
    pub const fn has_data(&self) -> bool {
        !matches!(self, Self::Watermark)
    }
}

impl From<CdcOperation> for EventType {
    fn from(op: CdcOperation) -> Self {
        match op {
            CdcOperation::Insert => Self::Insert,
            CdcOperation::Delete => Self::Delete,
            CdcOperation::UpdateBefore | CdcOperation::UpdateAfter => Self::Update,
        }
    }
}

/// Zero-allocation notification reference for Ring 0.
///
/// This is the only type that crosses from Ring 0 to Ring 1 via SPSC queue.
/// It carries enough metadata for the dispatcher to route and fetch data,
/// without carrying the actual data payload.
///
/// # Size
///
/// 48 bytes, fits in a single cache line with padding.
#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct NotificationRef {
    /// Monotonically increasing sequence number per source.
    pub sequence: u64,
    /// Source/MV identifier (index into registry).
    pub source_id: u32,
    /// Event type indicator.
    pub event_type: EventType,
    /// Number of rows affected.
    pub row_count: u32,
    /// Event timestamp (epoch millis).
    pub timestamp: i64,
    /// Batch offset in the source's ring buffer (for zero-copy fetch).
    pub batch_offset: u64,
    /// Padding to cache line.
    _pad: [u8; 11],
}

impl NotificationRef {
    /// Creates a new notification reference.
    #[inline]
    pub const fn new(
        sequence: u64,
        source_id: u32,
        event_type: EventType,
        row_count: u32,
        timestamp: i64,
        batch_offset: u64,
    ) -> Self {
        Self {
            sequence,
            source_id,
            event_type,
            row_count,
            timestamp,
            batch_offset,
            _pad: [0; 11],
        }
    }
}

/// Change event delivered to subscribers.
///
/// This is the Ring 1/Ring 2 representation that carries actual data.
/// Uses `Arc<RecordBatch>` for zero-copy multicast to multiple subscribers.
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    /// New record(s) inserted.
    Insert {
        /// The inserted rows as an Arrow RecordBatch.
        data: Arc<RecordBatch>,
        /// Event timestamp.
        timestamp: i64,
        /// Source sequence number.
        sequence: u64,
    },

    /// Record(s) deleted.
    Delete {
        /// The deleted rows as an Arrow RecordBatch.
        data: Arc<RecordBatch>,
        /// Event timestamp.
        timestamp: i64,
        /// Source sequence number.
        sequence: u64,
    },

    /// Record(s) updated (retraction pair).
    Update {
        /// Values before the update.
        old: Arc<RecordBatch>,
        /// Values after the update.
        new: Arc<RecordBatch>,
        /// Event timestamp.
        timestamp: i64,
        /// Source sequence number.
        sequence: u64,
    },

    /// Watermark advancement.
    Watermark {
        /// New watermark timestamp.
        timestamp: i64,
    },

    /// Initial state snapshot (sent on subscription start).
    Snapshot {
        /// Current state as a RecordBatch.
        data: Arc<RecordBatch>,
        /// Snapshot timestamp.
        timestamp: i64,
    },
}

impl ChangeEvent {
    /// Creates an insert event.
    pub fn insert(data: Arc<RecordBatch>, timestamp: i64, sequence: u64) -> Self {
        Self::Insert { data, timestamp, sequence }
    }

    /// Creates a delete event.
    pub fn delete(data: Arc<RecordBatch>, timestamp: i64, sequence: u64) -> Self {
        Self::Delete { data, timestamp, sequence }
    }

    /// Creates an update event from a retraction pair.
    pub fn update(
        old: Arc<RecordBatch>,
        new: Arc<RecordBatch>,
        timestamp: i64,
        sequence: u64,
    ) -> Self {
        Self::Update { old, new, timestamp, sequence }
    }

    /// Creates a watermark event.
    pub fn watermark(timestamp: i64) -> Self {
        Self::Watermark { timestamp }
    }

    /// Creates a snapshot event.
    pub fn snapshot(data: Arc<RecordBatch>, timestamp: i64) -> Self {
        Self::Snapshot { data, timestamp }
    }

    /// Returns the event type.
    #[inline]
    pub fn event_type(&self) -> EventType {
        match self {
            Self::Insert { .. } => EventType::Insert,
            Self::Delete { .. } => EventType::Delete,
            Self::Update { .. } => EventType::Update,
            Self::Watermark { .. } => EventType::Watermark,
            Self::Snapshot { .. } => EventType::Snapshot,
        }
    }

    /// Returns the timestamp of this event.
    #[inline]
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Insert { timestamp, .. }
            | Self::Delete { timestamp, .. }
            | Self::Update { timestamp, .. }
            | Self::Watermark { timestamp }
            | Self::Snapshot { timestamp, .. } => *timestamp,
        }
    }

    /// Returns the sequence number, if present.
    #[inline]
    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Insert { sequence, .. }
            | Self::Delete { sequence, .. }
            | Self::Update { sequence, .. } => Some(*sequence),
            _ => None,
        }
    }

    /// Returns the number of rows in this event.
    pub fn row_count(&self) -> usize {
        match self {
            Self::Insert { data, .. } | Self::Delete { data, .. } | Self::Snapshot { data, .. } => {
                data.num_rows()
            }
            Self::Update { new, .. } => new.num_rows(),
            Self::Watermark { .. } => 0,
        }
    }

    /// Returns true if this event carries data rows.
    #[inline]
    pub fn has_data(&self) -> bool {
        self.event_type().has_data()
    }
}

/// Batched change events sharing a common schema.
///
/// Used for efficient delivery when multiple events are buffered.
#[derive(Debug, Clone)]
pub struct ChangeEventBatch {
    /// The events in this batch.
    pub events: Vec<ChangeEvent>,
    /// Source/MV name.
    pub source: String,
    /// Starting sequence number.
    pub start_sequence: u64,
    /// Ending sequence number (exclusive).
    pub end_sequence: u64,
}

impl ChangeEventBatch {
    /// Creates a new batch from events.
    pub fn new(source: String, events: Vec<ChangeEvent>) -> Self {
        let start_sequence = events.first()
            .and_then(|e| e.sequence())
            .unwrap_or(0);
        let end_sequence = events.last()
            .and_then(|e| e.sequence())
            .map(|s| s + 1)
            .unwrap_or(start_sequence);

        Self { events, source, start_sequence, end_sequence }
    }

    /// Returns the total row count across all events.
    pub fn total_rows(&self) -> usize {
        self.events.iter().map(|e| e.row_count()).sum()
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the number of events.
    pub fn len(&self) -> usize {
        self.events.len()
    }
}
```

### Conversion from Existing Types

```rust
impl ChangeEvent {
    /// Converts from a ChangelogRecord with an Arrow RecordBatch payload.
    pub fn from_changelog_record(
        record: &ChangelogRecord<Arc<RecordBatch>>,
        sequence: u64,
    ) -> Self {
        match record.operation {
            CdcOperation::Insert => Self::insert(
                Arc::clone(&record.data),
                record.timestamp,
                sequence,
            ),
            CdcOperation::Delete => Self::delete(
                Arc::clone(&record.data),
                record.timestamp,
                sequence,
            ),
            CdcOperation::UpdateAfter => Self::insert(
                Arc::clone(&record.data),
                record.timestamp,
                sequence,
            ),
            CdcOperation::UpdateBefore => Self::delete(
                Arc::clone(&record.data),
                record.timestamp,
                sequence,
            ),
        }
    }
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| ChangelogRecord | `laminar-core/src/operator/changelog.rs` | Add `From` impl for ChangeEvent |
| CdcOperation | `laminar-core/src/operator/changelog.rs` | Add `From` impl for EventType |
| SubscriptionMessage | `laminar-core/src/streaming/subscription.rs` | Add ChangeEvent variant |

### New Files

- `crates/laminar-core/src/subscription/mod.rs` - Module root
- `crates/laminar-core/src/subscription/event.rs` - ChangeEvent, NotificationRef, EventType

## Test Plan

### Unit Tests

- [ ] `test_event_type_weights` - Z-set weight correctness
- [ ] `test_notification_ref_size` - Assert size_of == 64 (cache line)
- [ ] `test_notification_ref_is_copy` - Compile-time Copy verification
- [ ] `test_change_event_insert` - Insert variant construction
- [ ] `test_change_event_delete` - Delete variant construction
- [ ] `test_change_event_update` - Update retraction pair
- [ ] `test_change_event_watermark` - Watermark construction
- [ ] `test_change_event_snapshot` - Snapshot construction
- [ ] `test_change_event_row_count` - Row count across variants
- [ ] `test_change_event_clone_is_cheap` - Arc clone, not deep copy
- [ ] `test_from_changelog_record` - Conversion from F063 types
- [ ] `test_from_cdc_operation` - EventType from CdcOperation
- [ ] `test_change_event_batch` - Batch construction and accessors
- [ ] `test_change_event_batch_empty` - Empty batch handling

### Benchmarks

- [ ] `bench_notification_ref_create` - Target: < 5ns
- [ ] `bench_change_event_clone` - Target: < 10ns (Arc clone)
- [ ] `bench_change_event_from_changelog` - Target: < 20ns

## Completion Checklist

- [ ] `ChangeEvent` enum implemented with all variants
- [ ] `NotificationRef` implemented, verified 64-byte cache-aligned
- [ ] `EventType` enum with Z-set weights
- [ ] `ChangeEventBatch` for batched delivery
- [ ] Conversion from `ChangelogRecord` and `CdcOperation`
- [ ] Unit tests passing (14+ tests)
- [ ] Benchmarks meeting targets
- [ ] Documentation with examples
- [ ] Code reviewed

## References

- [F063: Changelog/Retraction](../../phase-2/F063-changelog-retraction.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [DBSP Incremental View Maintenance](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf)
