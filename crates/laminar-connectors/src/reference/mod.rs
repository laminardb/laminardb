//! Startup snapshot sources for reference tables.

#[cfg(any(test, feature = "testing"))]
use std::collections::VecDeque;

use arrow_array::RecordBatch;

use crate::error::ConnectorError;

/// A finite source used to hydrate a reference table before processing starts.
#[async_trait::async_trait]
pub trait ReferenceTableSource: Send {
    /// Returns the next snapshot batch, or `None` after the complete snapshot was delivered.
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    /// Releases source resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;
}

/// In-memory finite snapshot source for tests.
#[cfg(any(test, feature = "testing"))]
pub struct MockReferenceTableSource {
    snapshot_batches: VecDeque<RecordBatch>,
    /// Whether [`ReferenceTableSource::close`] has been called.
    pub closed: bool,
}

#[cfg(any(test, feature = "testing"))]
impl MockReferenceTableSource {
    /// Creates a source that drains the supplied snapshot batches in order.
    #[must_use]
    pub fn new(snapshot_batches: Vec<RecordBatch>) -> Self {
        Self {
            snapshot_batches: VecDeque::from(snapshot_batches),
            closed: false,
        }
    }

    /// Creates a source with an empty snapshot.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }
}

#[cfg(any(test, feature = "testing"))]
#[async_trait::async_trait]
impl ReferenceTableSource for MockReferenceTableSource {
    async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Err(ConnectorError::InvalidState {
                expected: "open reference snapshot source".into(),
                actual: "closed".into(),
            });
        }
        Ok(self.snapshot_batches.pop_front())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
