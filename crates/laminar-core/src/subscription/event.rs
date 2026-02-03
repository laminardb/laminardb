//! Change event types for the reactive subscription system.
//!
//! Provides three tiers of types matching the ring architecture:
//! - Ring 0: [`EventType`] + [`NotificationRef`] (zero-allocation, cache-aligned)
//! - Ring 1/2: [`ChangeEvent`] (data delivery via `Arc<RecordBatch>`)
//! - Batching: [`ChangeEventBatch`] (coalesced delivery)

use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::operator::window::{CdcOperation, ChangelogRecord};

// ---------------------------------------------------------------------------
// EventType — Ring 0 discriminant
// ---------------------------------------------------------------------------

/// Discriminant for change event kinds.
///
/// Stored as `#[repr(u8)]` for compact, zero-cost embedding in
/// [`NotificationRef`] and protocol headers.
///
/// Weight semantics match [`CdcOperation::weight()`]:
/// - Insert: +1
/// - Delete: -1
/// - Update: 0 (decomposed into -1 / +1 pair in Z-set model)
/// - Watermark / Snapshot: 0 (control events)
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    /// A new row was inserted (+1 weight).
    Insert = 0,
    /// A row was deleted (-1 weight).
    Delete = 1,
    /// A row was updated (weight 0 — decomposes to delete + insert).
    Update = 2,
    /// Watermark progress notification (no data).
    Watermark = 3,
    /// Snapshot delivery (initial state load, +1 weight per row).
    Snapshot = 4,
}

impl EventType {
    /// Returns the Z-set weight for this event type.
    ///
    /// Matches the convention in [`CdcOperation::weight()`]:
    /// - Insert / Snapshot: +1
    /// - Delete: -1
    /// - Update / Watermark: 0
    #[inline]
    #[must_use]
    pub fn weight(&self) -> i32 {
        match self {
            Self::Insert | Self::Snapshot => 1,
            Self::Delete => -1,
            Self::Update | Self::Watermark => 0,
        }
    }

    /// Returns `true` if this event type carries data rows.
    #[inline]
    #[must_use]
    pub fn has_data(&self) -> bool {
        !matches!(self, Self::Watermark)
    }
}

impl From<CdcOperation> for EventType {
    /// Converts a [`CdcOperation`] into an [`EventType`].
    ///
    /// - `Insert` / `UpdateAfter` → `Insert`
    /// - `Delete` / `UpdateBefore` → `Delete`
    fn from(op: CdcOperation) -> Self {
        match op {
            CdcOperation::Insert | CdcOperation::UpdateAfter => Self::Insert,
            CdcOperation::Delete | CdcOperation::UpdateBefore => Self::Delete,
        }
    }
}

// ---------------------------------------------------------------------------
// NotificationRef — Ring 0 cache-line-aligned slot
// ---------------------------------------------------------------------------

/// Zero-allocation notification reference for Ring 0.
///
/// Exactly 64 bytes (`#[repr(C, align(64))]`) to occupy a single cache line,
/// avoiding false sharing when used in lock-free notification slots.
///
/// Contains only metadata — actual data is fetched from Ring 1 using
/// `batch_offset` as a reference into a shared buffer.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct NotificationRef {
    /// Monotonically increasing sequence number.
    pub sequence: u64,
    /// Identifier of the source materialized view or query.
    pub source_id: u32,
    /// The type of change event.
    pub event_type: EventType,
    // 3 bytes padding (u8 enum + 3 to align next u32)
    _pad_event: [u8; 3],
    /// Number of rows affected.
    pub row_count: u32,
    /// Event timestamp (milliseconds since epoch).
    pub timestamp: i64,
    /// Offset into a shared batch buffer for data retrieval.
    pub batch_offset: u64,
    /// Padding to fill to 64 bytes.
    _pad: [u8; 24],
}

impl NotificationRef {
    /// Creates a new notification reference.
    #[inline]
    #[must_use]
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
            _pad_event: [0; 3],
            row_count,
            timestamp,
            batch_offset,
            _pad: [0; 24],
        }
    }
}

// ---------------------------------------------------------------------------
// ChangeEvent — Ring 1/2 data delivery
// ---------------------------------------------------------------------------

/// A change event carrying Arrow data for subscriber delivery.
///
/// Each variant includes the minimal data needed for that event type.
/// Data is shared via `Arc<RecordBatch>` for zero-copy fan-out to
/// multiple subscribers.
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    /// A new row batch was inserted.
    Insert {
        /// The inserted rows.
        data: Arc<RecordBatch>,
        /// Event timestamp.
        timestamp: i64,
        /// Sequence number from the notification.
        sequence: u64,
    },
    /// A row batch was deleted.
    Delete {
        /// The deleted rows.
        data: Arc<RecordBatch>,
        /// Event timestamp.
        timestamp: i64,
        /// Sequence number from the notification.
        sequence: u64,
    },
    /// A row batch was updated (before + after).
    Update {
        /// The old row values.
        old: Arc<RecordBatch>,
        /// The new row values.
        new: Arc<RecordBatch>,
        /// Event timestamp.
        timestamp: i64,
        /// Sequence number from the notification.
        sequence: u64,
    },
    /// Watermark progress (no data).
    Watermark {
        /// The new watermark timestamp.
        timestamp: i64,
    },
    /// Initial snapshot delivery.
    Snapshot {
        /// The snapshot rows.
        data: Arc<RecordBatch>,
        /// Event timestamp.
        timestamp: i64,
        /// Sequence number from the notification.
        sequence: u64,
    },
}

impl ChangeEvent {
    /// Creates an insert change event.
    #[must_use]
    pub fn insert(data: Arc<RecordBatch>, timestamp: i64, sequence: u64) -> Self {
        Self::Insert {
            data,
            timestamp,
            sequence,
        }
    }

    /// Creates a delete change event.
    #[must_use]
    pub fn delete(data: Arc<RecordBatch>, timestamp: i64, sequence: u64) -> Self {
        Self::Delete {
            data,
            timestamp,
            sequence,
        }
    }

    /// Creates an update change event with old and new values.
    #[must_use]
    pub fn update(
        old: Arc<RecordBatch>,
        new: Arc<RecordBatch>,
        timestamp: i64,
        sequence: u64,
    ) -> Self {
        Self::Update {
            old,
            new,
            timestamp,
            sequence,
        }
    }

    /// Creates a watermark change event.
    #[must_use]
    pub fn watermark(timestamp: i64) -> Self {
        Self::Watermark { timestamp }
    }

    /// Creates a snapshot change event.
    #[must_use]
    pub fn snapshot(data: Arc<RecordBatch>, timestamp: i64, sequence: u64) -> Self {
        Self::Snapshot {
            data,
            timestamp,
            sequence,
        }
    }

    /// Returns the [`EventType`] for this change event.
    #[must_use]
    pub fn event_type(&self) -> EventType {
        match self {
            Self::Insert { .. } => EventType::Insert,
            Self::Delete { .. } => EventType::Delete,
            Self::Update { .. } => EventType::Update,
            Self::Watermark { .. } => EventType::Watermark,
            Self::Snapshot { .. } => EventType::Snapshot,
        }
    }

    /// Returns the event timestamp.
    #[must_use]
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Insert { timestamp, .. }
            | Self::Delete { timestamp, .. }
            | Self::Update { timestamp, .. }
            | Self::Watermark { timestamp }
            | Self::Snapshot { timestamp, .. } => *timestamp,
        }
    }

    /// Returns the sequence number, or `None` for watermark events.
    #[must_use]
    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Insert { sequence, .. }
            | Self::Delete { sequence, .. }
            | Self::Update { sequence, .. }
            | Self::Snapshot { sequence, .. } => Some(*sequence),
            Self::Watermark { .. } => None,
        }
    }

    /// Returns the total number of rows in this event.
    ///
    /// For `Update` events, returns the row count of the new batch.
    /// For `Watermark`, returns 0.
    #[must_use]
    pub fn row_count(&self) -> usize {
        match self {
            Self::Insert { data, .. } | Self::Delete { data, .. } | Self::Snapshot { data, .. } => {
                data.num_rows()
            }
            Self::Update { new, .. } => new.num_rows(),
            Self::Watermark { .. } => 0,
        }
    }

    /// Returns `true` if this event carries data rows.
    #[must_use]
    pub fn has_data(&self) -> bool {
        self.event_type().has_data()
    }

    /// Creates a [`ChangeEvent`] from a [`ChangelogRecord`].
    ///
    /// Maps CDC operations to subscription event types:
    /// - `Insert` / `UpdateAfter` → `ChangeEvent::Insert`
    /// - `Delete` / `UpdateBefore` → `ChangeEvent::Delete`
    #[must_use]
    pub fn from_changelog_record(record: &ChangelogRecord, sequence: u64) -> Self {
        let data = Arc::clone(&record.event.data);
        let timestamp = record.emit_timestamp;
        match record.operation {
            CdcOperation::Insert | CdcOperation::UpdateAfter => Self::Insert {
                data,
                timestamp,
                sequence,
            },
            CdcOperation::Delete | CdcOperation::UpdateBefore => Self::Delete {
                data,
                timestamp,
                sequence,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// ChangeEventBatch — coalesced delivery
// ---------------------------------------------------------------------------

/// A batch of change events for coalesced delivery to subscribers.
///
/// Groups multiple [`ChangeEvent`]s from the same source, with metadata
/// about the sequence range for gap detection and resumption.
#[derive(Debug, Clone)]
pub struct ChangeEventBatch {
    /// The source materialized view or query name.
    pub source: String,
    /// The events in this batch.
    pub events: Vec<ChangeEvent>,
    /// First sequence number in this batch.
    pub first_sequence: u64,
    /// Last sequence number in this batch (inclusive).
    pub last_sequence: u64,
}

impl ChangeEventBatch {
    /// Creates a new change event batch.
    #[must_use]
    pub fn new(
        source: String,
        events: Vec<ChangeEvent>,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Self {
        Self {
            source,
            events,
            first_sequence,
            last_sequence,
        }
    }

    /// Returns the total number of data rows across all events.
    #[must_use]
    pub fn total_rows(&self) -> usize {
        self.events.iter().map(ChangeEvent::row_count).sum()
    }

    /// Returns `true` if this batch contains no events.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the number of events in this batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::mem;

    /// Helper: create a RecordBatch with `n` rows.
    fn make_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..n as i64).collect();
        let array = Int64Array::from(values);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    // --- EventType tests ---

    #[test]
    fn event_type_weights() {
        assert_eq!(EventType::Insert.weight(), 1);
        assert_eq!(EventType::Delete.weight(), -1);
        assert_eq!(EventType::Update.weight(), 0);
        assert_eq!(EventType::Watermark.weight(), 0);
        assert_eq!(EventType::Snapshot.weight(), 1);
    }

    #[test]
    fn event_type_has_data() {
        assert!(EventType::Insert.has_data());
        assert!(EventType::Delete.has_data());
        assert!(EventType::Update.has_data());
        assert!(!EventType::Watermark.has_data());
        assert!(EventType::Snapshot.has_data());
    }

    #[test]
    fn event_type_from_cdc_operation() {
        assert_eq!(EventType::from(CdcOperation::Insert), EventType::Insert);
        assert_eq!(EventType::from(CdcOperation::Delete), EventType::Delete);
        assert_eq!(
            EventType::from(CdcOperation::UpdateAfter),
            EventType::Insert
        );
        assert_eq!(
            EventType::from(CdcOperation::UpdateBefore),
            EventType::Delete
        );
    }

    #[test]
    fn event_type_repr_u8() {
        assert_eq!(EventType::Insert as u8, 0);
        assert_eq!(EventType::Delete as u8, 1);
        assert_eq!(EventType::Update as u8, 2);
        assert_eq!(EventType::Watermark as u8, 3);
        assert_eq!(EventType::Snapshot as u8, 4);
    }

    // --- NotificationRef tests ---

    #[test]
    fn notification_ref_size_and_alignment() {
        assert_eq!(mem::size_of::<NotificationRef>(), 64);
        assert_eq!(mem::align_of::<NotificationRef>(), 64);
    }

    #[test]
    fn notification_ref_fields() {
        let nr = NotificationRef::new(42, 7, EventType::Insert, 100, 1_000_000, 0xFF);
        assert_eq!(nr.sequence, 42);
        assert_eq!(nr.source_id, 7);
        assert_eq!(nr.event_type, EventType::Insert);
        assert_eq!(nr.row_count, 100);
        assert_eq!(nr.timestamp, 1_000_000);
        assert_eq!(nr.batch_offset, 0xFF);
    }

    #[test]
    fn notification_ref_copy() {
        let a = NotificationRef::new(1, 2, EventType::Delete, 10, 500, 0);
        let b = a; // Copy
        assert_eq!(a.sequence, b.sequence);
        assert_eq!(a.event_type, b.event_type);
    }

    // --- ChangeEvent tests ---

    #[test]
    fn change_event_insert() {
        let batch = Arc::new(make_batch(5));
        let ev = ChangeEvent::insert(Arc::clone(&batch), 1000, 1);
        assert_eq!(ev.event_type(), EventType::Insert);
        assert_eq!(ev.timestamp(), 1000);
        assert_eq!(ev.sequence(), Some(1));
        assert_eq!(ev.row_count(), 5);
        assert!(ev.has_data());
    }

    #[test]
    fn change_event_delete() {
        let batch = Arc::new(make_batch(3));
        let ev = ChangeEvent::delete(batch, 2000, 2);
        assert_eq!(ev.event_type(), EventType::Delete);
        assert_eq!(ev.timestamp(), 2000);
        assert_eq!(ev.sequence(), Some(2));
        assert_eq!(ev.row_count(), 3);
    }

    #[test]
    fn change_event_update() {
        let old = Arc::new(make_batch(2));
        let new = Arc::new(make_batch(2));
        let ev = ChangeEvent::update(old, new, 3000, 3);
        assert_eq!(ev.event_type(), EventType::Update);
        assert_eq!(ev.timestamp(), 3000);
        assert_eq!(ev.sequence(), Some(3));
        assert_eq!(ev.row_count(), 2);
    }

    #[test]
    fn change_event_watermark() {
        let ev = ChangeEvent::watermark(5000);
        assert_eq!(ev.event_type(), EventType::Watermark);
        assert_eq!(ev.timestamp(), 5000);
        assert_eq!(ev.sequence(), None);
        assert_eq!(ev.row_count(), 0);
        assert!(!ev.has_data());
    }

    #[test]
    fn change_event_snapshot() {
        let batch = Arc::new(make_batch(10));
        let ev = ChangeEvent::snapshot(batch, 100, 7);
        assert_eq!(ev.event_type(), EventType::Snapshot);
        assert_eq!(ev.row_count(), 10);
    }

    #[test]
    fn change_event_clone_shares_arc() {
        let batch = Arc::new(make_batch(4));
        let ev = ChangeEvent::insert(Arc::clone(&batch), 0, 0);
        let cloned = ev.clone();
        // Both point to the same underlying allocation.
        if let (ChangeEvent::Insert { data: d1, .. }, ChangeEvent::Insert { data: d2, .. }) =
            (&ev, &cloned)
        {
            assert!(Arc::ptr_eq(d1, d2));
        } else {
            panic!("expected Insert variants");
        }
    }

    #[test]
    fn change_event_from_changelog_record() {
        use crate::operator::Event;

        let batch = make_batch(3);
        let event = Event::new(1000, batch);

        let insert_rec = ChangelogRecord::insert(event.clone(), 2000);
        let ce = ChangeEvent::from_changelog_record(&insert_rec, 10);
        assert_eq!(ce.event_type(), EventType::Insert);
        assert_eq!(ce.timestamp(), 2000);
        assert_eq!(ce.sequence(), Some(10));
        assert_eq!(ce.row_count(), 3);

        let delete_rec = ChangelogRecord::delete(event, 3000);
        let ce = ChangeEvent::from_changelog_record(&delete_rec, 11);
        assert_eq!(ce.event_type(), EventType::Delete);
        assert_eq!(ce.timestamp(), 3000);
        assert_eq!(ce.sequence(), Some(11));
    }

    // --- ChangeEventBatch tests ---

    #[test]
    fn change_event_batch_operations() {
        let batch = Arc::new(make_batch(5));
        let events = vec![
            ChangeEvent::insert(Arc::clone(&batch), 100, 1),
            ChangeEvent::insert(Arc::clone(&batch), 200, 2),
            ChangeEvent::watermark(300),
        ];
        let ceb = ChangeEventBatch::new("test_mv".into(), events, 1, 2);
        assert_eq!(ceb.len(), 3);
        assert!(!ceb.is_empty());
        assert_eq!(ceb.total_rows(), 10); // 5 + 5 + 0
        assert_eq!(ceb.source, "test_mv");
        assert_eq!(ceb.first_sequence, 1);
        assert_eq!(ceb.last_sequence, 2);
    }

    #[test]
    fn change_event_batch_empty() {
        let ceb = ChangeEventBatch::new("empty".into(), vec![], 0, 0);
        assert!(ceb.is_empty());
        assert_eq!(ceb.len(), 0);
        assert_eq!(ceb.total_rows(), 0);
    }
}
