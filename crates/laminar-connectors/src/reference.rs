//! Reference table source trait and refresh modes.
//!
//! A [`ReferenceTableSource`](crate::reference::ReferenceTableSource) populates a reference/dimension table from an
//! external connector. The source produces an initial snapshot (one or more
//! `RecordBatch`es) followed by an optional stream of incremental changes.
//!
//! [`RefreshMode`](crate::reference::RefreshMode) controls how and when the table is refreshed:
//!
//! - `SnapshotOnly` — load once at startup, never update.
//! - `SnapshotPlusCdc` — load at startup, then apply CDC changes.
//! - `Periodic` — re-snapshot on a timer.
//! - `Manual` — no automatic loading; the user triggers refreshes.

use std::collections::VecDeque;
use std::time::Duration;

use arrow_array::RecordBatch;

use crate::checkpoint::SourceCheckpoint;
use crate::error::ConnectorError;

/// How a reference table is refreshed after initial population.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshMode {
    /// Load the table once at startup and never update.
    SnapshotOnly,
    /// Load at startup, then apply incremental CDC changes.
    SnapshotPlusCdc,
    /// Re-snapshot the entire table on a fixed interval.
    Periodic {
        /// Interval between full re-snapshots.
        interval: Duration,
    },
    /// No automatic loading; the user triggers refreshes explicitly.
    Manual,
}

/// A source that populates a reference/dimension table.
///
/// The lifecycle is:
/// 1. Call [`poll_snapshot`](Self::poll_snapshot) repeatedly until it returns
///    `Ok(None)` (snapshot complete).
/// 2. Optionally call [`poll_changes`](Self::poll_changes) in a loop to receive
///    incremental updates (CDC mode).
/// 3. Call [`close`](Self::close) when the table is no longer needed.
///
/// Checkpoint/restore support allows resuming from a saved position across
/// restarts.
#[async_trait::async_trait]
pub trait ReferenceTableSource: Send {
    /// Polls for the next batch of snapshot data.
    ///
    /// Returns `Ok(Some(batch))` while snapshot data is available.
    /// Returns `Ok(None)` when the snapshot is complete.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` on read failure.
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    /// Returns `true` once all snapshot batches have been delivered.
    fn is_snapshot_complete(&self) -> bool;

    /// Polls for the next batch of incremental changes (CDC).
    ///
    /// Returns `Ok(Some(batch))` when change data is available,
    /// `Ok(None)` when no changes are pending.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` on read failure.
    async fn poll_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    /// Creates a checkpoint of the current source position.
    fn checkpoint(&self) -> SourceCheckpoint;

    /// Restores the source position from a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the checkpoint is invalid or restore fails.
    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError>;

    /// Closes the source and releases resources.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if shutdown fails.
    async fn close(&mut self) -> Result<(), ConnectorError>;
}

// ── Mock Implementation ──

/// A mock [`ReferenceTableSource`] for testing.
///
/// Configurable queues of snapshot and change batches. Tracks lifecycle flags
/// (`snapshot_complete`, `restored`, `closed`) for test assertions.
pub struct MockReferenceTableSource {
    /// Snapshot batches to deliver (drained in order).
    pub snapshot_batches: VecDeque<RecordBatch>,
    /// Change batches to deliver after snapshot (drained in order).
    pub change_batches: VecDeque<RecordBatch>,
    /// Set to `true` once all snapshot batches have been delivered.
    pub snapshot_complete: bool,
    /// Set to `true` after [`restore`](ReferenceTableSource::restore) is called.
    pub restored: bool,
    /// Set to `true` after [`close`](ReferenceTableSource::close) is called.
    pub closed: bool,
    /// The checkpoint returned by [`checkpoint`](ReferenceTableSource::checkpoint).
    pub mock_checkpoint: SourceCheckpoint,
}

impl MockReferenceTableSource {
    /// Creates a new mock with the given snapshot and change batches.
    #[must_use]
    pub fn new(snapshot_batches: Vec<RecordBatch>, change_batches: Vec<RecordBatch>) -> Self {
        Self {
            snapshot_batches: VecDeque::from(snapshot_batches),
            change_batches: VecDeque::from(change_batches),
            snapshot_complete: false,
            restored: false,
            closed: false,
            mock_checkpoint: SourceCheckpoint::new(0),
        }
    }

    /// Creates a new mock with no data.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(vec![], vec![])
    }
}

#[async_trait::async_trait]
impl ReferenceTableSource for MockReferenceTableSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if let Some(batch) = self.snapshot_batches.pop_front() {
            Ok(Some(batch))
        } else {
            self.snapshot_complete = true;
            Ok(None)
        }
    }

    fn is_snapshot_complete(&self) -> bool {
        self.snapshot_complete
    }

    async fn poll_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        Ok(self.change_batches.pop_front())
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.mock_checkpoint.clone()
    }

    async fn restore(&mut self, _checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        self.restored = true;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))]).unwrap()
    }

    #[tokio::test]
    async fn test_mock_snapshot_exhaustion() {
        let mut src =
            MockReferenceTableSource::new(vec![test_batch(&[1, 2]), test_batch(&[3])], vec![]);

        assert!(!src.is_snapshot_complete());

        let b1 = src.poll_snapshot().await.unwrap().unwrap();
        assert_eq!(b1.num_rows(), 2);
        assert!(!src.is_snapshot_complete());

        let b2 = src.poll_snapshot().await.unwrap().unwrap();
        assert_eq!(b2.num_rows(), 1);
        assert!(!src.is_snapshot_complete());

        let none = src.poll_snapshot().await.unwrap();
        assert!(none.is_none());
        assert!(src.is_snapshot_complete());

        // Subsequent calls also return None
        assert!(src.poll_snapshot().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_mock_change_polling() {
        let mut src =
            MockReferenceTableSource::new(vec![], vec![test_batch(&[10]), test_batch(&[20, 30])]);

        // Exhaust snapshot first
        assert!(src.poll_snapshot().await.unwrap().is_none());

        let c1 = src.poll_changes().await.unwrap().unwrap();
        assert_eq!(c1.num_rows(), 1);

        let c2 = src.poll_changes().await.unwrap().unwrap();
        assert_eq!(c2.num_rows(), 2);

        assert!(src.poll_changes().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_mock_checkpoint_round_trip() {
        let mut cp = SourceCheckpoint::new(5);
        cp.set_offset("lsn", "0/ABCD");

        let mut src = MockReferenceTableSource::empty();
        src.mock_checkpoint = cp.clone();

        let returned = src.checkpoint();
        assert_eq!(returned.epoch(), 5);
        assert_eq!(returned.get_offset("lsn"), Some("0/ABCD"));
    }

    #[tokio::test]
    async fn test_mock_restore_sets_flag() {
        let mut src = MockReferenceTableSource::empty();
        assert!(!src.restored);

        let cp = SourceCheckpoint::new(1);
        src.restore(&cp).await.unwrap();
        assert!(src.restored);
    }

    #[tokio::test]
    async fn test_mock_close_idempotent() {
        let mut src = MockReferenceTableSource::empty();
        assert!(!src.closed);

        src.close().await.unwrap();
        assert!(src.closed);

        // Calling close again should succeed
        src.close().await.unwrap();
        assert!(src.closed);
    }

    #[tokio::test]
    async fn test_trait_compliance_with_mock() {
        // Exercise the full lifecycle through trait object
        let mut src: Box<dyn ReferenceTableSource> = Box::new(MockReferenceTableSource::new(
            vec![test_batch(&[1])],
            vec![test_batch(&[2])],
        ));

        // Snapshot
        let batch = src.poll_snapshot().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(src.poll_snapshot().await.unwrap().is_none());
        assert!(src.is_snapshot_complete());

        // Changes
        let change = src.poll_changes().await.unwrap().unwrap();
        assert_eq!(change.num_rows(), 1);
        assert!(src.poll_changes().await.unwrap().is_none());

        // Checkpoint round-trip
        let _cp = src.checkpoint();

        // Restore
        let cp = SourceCheckpoint::new(0);
        src.restore(&cp).await.unwrap();

        // Close
        src.close().await.unwrap();
    }
}
