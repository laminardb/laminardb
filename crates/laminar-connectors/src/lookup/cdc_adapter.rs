//! CDC-to-reference-table adapter.
//!
//! Wraps any `SourceConnector` (Postgres CDC, MySQL CDC) as a
//! `ReferenceTableSource` so CDC streams can populate lookup tables.
//!
//! The adapter has two phases:
//! 1. **Snapshot**: calls `poll_batch()` until the source returns `None`,
//!    treating the initial burst as a consistent snapshot.
//! 2. **Changes**: subsequent `poll_changes()` calls continue polling
//!    for incremental CDC events.

use arrow_array::RecordBatch;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{DeliveryGuarantee, SourcePosition, SourceStart};
use crate::connector::{SourceBatch, SourceConnector, SourceContract};
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;

/// Lifecycle phase of the CDC adapter.
enum Phase {
    /// Not yet opened.
    Init,
    /// Delivering initial snapshot batches via `poll_snapshot()`.
    Snapshot,
    /// Snapshot complete; delivering incremental CDC via `poll_changes()`.
    Changes,
    /// Closed.
    Closed,
}

/// Adapts a [`SourceConnector`] into a [`ReferenceTableSource`].
///
/// The underlying connector is opened lazily on the first `poll_snapshot()`
/// call. Snapshot completion is detected when `poll_batch()` returns `None`
/// for the first time after delivering at least one batch (or immediately
/// if the source has no data).
pub struct CdcTableSource {
    connector: Box<dyn SourceConnector>,
    config: ConnectorConfig,
    pending_position: Option<SourcePosition>,
    phase: Phase,
    max_batch_size: usize,
}

impl CdcTableSource {
    /// Creates a new adapter wrapping the given source connector.
    ///
    /// `max_batch_size` is the hint passed to `poll_batch()`.
    #[must_use]
    pub fn new(
        connector: Box<dyn SourceConnector>,
        config: ConnectorConfig,
        max_batch_size: usize,
    ) -> Self {
        Self {
            connector,
            config,
            pending_position: Some(SourcePosition::Initial),
            phase: Phase::Init,
            max_batch_size,
        }
    }

    /// Returns the restart and placement contract of the wrapped CDC source.
    ///
    /// # Errors
    ///
    /// Propagates an invalid concrete source configuration or contract.
    pub fn contract(&self) -> Result<SourceContract, ConnectorError> {
        self.connector.contract(&self.config)
    }

    /// Atomically starts the connector at its pending position before polling.
    async fn ensure_started(&mut self) -> Result<(), ConnectorError> {
        if matches!(self.phase, Phase::Init) {
            let position =
                self.pending_position
                    .clone()
                    .ok_or_else(|| ConnectorError::InvalidState {
                        expected: "pending source position".into(),
                        actual: "source position already consumed".into(),
                    })?;
            self.connector
                .start(SourceStart {
                    config: self.config.clone(),
                    position,
                    delivery: DeliveryGuarantee::BestEffort,
                })
                .await?;
            self.pending_position = None;
            self.phase = Phase::Snapshot;
        }
        Ok(())
    }

    /// Polls the underlying connector for the next batch.
    async fn poll_inner(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        match self.connector.poll_batch(self.max_batch_size).await? {
            Some(SourceBatch { records, .. }) if records.num_rows() > 0 => Ok(Some(records)),
            _ => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl ReferenceTableSource for CdcTableSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        self.ensure_started().await?;

        if !matches!(self.phase, Phase::Snapshot) {
            return Ok(None);
        }

        if let Some(batch) = self.poll_inner().await? {
            return Ok(Some(batch));
        }
        self.phase = Phase::Changes;
        Ok(None)
    }

    fn is_snapshot_complete(&self) -> bool {
        matches!(self.phase, Phase::Changes | Phase::Closed)
    }

    async fn poll_changes(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if !matches!(self.phase, Phase::Changes) {
            return Ok(None);
        }
        self.poll_inner().await
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.connector.checkpoint()
    }

    async fn restore(&mut self, _checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        Err(ConnectorError::ConfigurationError(
            "CDC reference-table recovery requires an exact checkpoint attempt, but \
             ReferenceTableSource::restore supplies only connector state; atomic resume is \
             unsupported until that API carries SourcePosition::Resume"
                .into(),
        ))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.phase = Phase::Closed;
        self.connector.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn test_batch(values: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    /// Minimal mock connector for testing the adapter.
    struct MockCdcConnector {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
        opened: bool,
        closed: bool,
    }

    impl MockCdcConnector {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self {
                schema: test_schema(),
                batches: VecDeque::from(batches),
                opened: false,
                closed: false,
            }
        }
    }

    #[async_trait::async_trait]
    impl SourceConnector for MockCdcConnector {
        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            if !matches!(request.position, SourcePosition::Initial) {
                return Err(ConnectorError::ConfigurationError(
                    "mock CDC source received an unexpected resume position".into(),
                ));
            }
            self.opened = true;
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            Ok(self.batches.pop_front().map(SourceBatch::new))
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closed = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn snapshot_then_changes() {
        let connector = MockCdcConnector::new(vec![
            test_batch(&[1, 2]),
            test_batch(&[3]),
            // None gap = snapshot complete
        ]);
        let mut src =
            CdcTableSource::new(Box::new(connector), ConnectorConfig::new("mock-cdc"), 1024);

        // Snapshot phase
        let b1 = src.poll_snapshot().await.unwrap().unwrap();
        assert_eq!(b1.num_rows(), 2);
        assert!(!src.is_snapshot_complete());

        let b2 = src.poll_snapshot().await.unwrap().unwrap();
        assert_eq!(b2.num_rows(), 1);

        // No more data → snapshot complete
        assert!(src.poll_snapshot().await.unwrap().is_none());
        assert!(src.is_snapshot_complete());

        // Changes phase returns None (no more batches in mock)
        assert!(src.poll_changes().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn empty_source_completes_snapshot_immediately() {
        let connector = MockCdcConnector::new(vec![]);
        let mut src =
            CdcTableSource::new(Box::new(connector), ConnectorConfig::new("mock-cdc"), 1024);

        assert!(src.poll_snapshot().await.unwrap().is_none());
        assert!(src.is_snapshot_complete());
    }

    #[tokio::test]
    async fn poll_changes_before_snapshot_returns_none() {
        let connector = MockCdcConnector::new(vec![test_batch(&[1])]);
        let mut src =
            CdcTableSource::new(Box::new(connector), ConnectorConfig::new("mock-cdc"), 1024);

        // Calling poll_changes before snapshot is complete
        assert!(src.poll_changes().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn checkpoint_delegates_to_connector() {
        let connector = MockCdcConnector::new(vec![]);
        let src = CdcTableSource::new(Box::new(connector), ConnectorConfig::new("mock-cdc"), 1024);
        let cp = src.checkpoint();
        assert!(cp.is_empty());
    }

    #[tokio::test]
    async fn restore_fails_closed_without_an_exact_attempt() {
        let connector = MockCdcConnector::new(vec![]);
        let mut src =
            CdcTableSource::new(Box::new(connector), ConnectorConfig::new("mock-cdc"), 1024);

        let error = src
            .restore(&SourceCheckpoint::new())
            .await
            .expect_err("reference restore cannot synthesize an exact checkpoint attempt");
        assert!(error.to_string().contains("exact checkpoint attempt"));
        assert!(matches!(src.phase, Phase::Init));
        assert!(matches!(
            src.pending_position,
            Some(SourcePosition::Initial)
        ));
    }

    #[tokio::test]
    async fn close_transitions_to_closed() {
        let connector = MockCdcConnector::new(vec![]);
        let mut src =
            CdcTableSource::new(Box::new(connector), ConnectorConfig::new("mock-cdc"), 1024);
        src.close().await.unwrap();
        assert!(src.is_snapshot_complete());
        assert!(src.poll_snapshot().await.unwrap().is_none());
    }
}
