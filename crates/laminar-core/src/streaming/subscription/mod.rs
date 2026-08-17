//! Subscription — receive records from a Sink.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use tokio::sync::broadcast;

use super::error::RecvError;
use super::source::{Record, SourceMessage};

/// A subscription to a streaming sink. Each subscriber independently receives
/// every message via broadcast.
pub struct Subscription<T: Record> {
    rx: broadcast::Receiver<SourceMessage<T>>,
    schema: SchemaRef,
    closed: bool,
}

impl<T: Record> Subscription<T> {
    pub(crate) fn new(rx: broadcast::Receiver<SourceMessage<T>>, schema: SchemaRef) -> Self {
        Self {
            rx,
            schema,
            closed: false,
        }
    }

    /// Non-blocking poll. Returns the next batch.
    /// Returns `None` on empty or closed channel. Check `is_disconnected()`
    /// to distinguish.
    pub fn poll(&mut self) -> Option<RecordBatch> {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => return Some(to_batch(msg)),
                Err(broadcast::error::TryRecvError::Empty) => return None,
                Err(broadcast::error::TryRecvError::Closed) => {
                    self.closed = true;
                    return None;
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {}
            }
        }
    }

    /// Async receive. Awaits the next batch.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` if the source has been dropped.
    pub async fn recv_async(&mut self) -> Result<RecordBatch, RecvError> {
        loop {
            match self.rx.recv().await {
                Ok(msg) => return Ok(to_batch(msg)),
                Err(broadcast::error::RecvError::Closed) => {
                    self.closed = true;
                    return Err(RecvError::Disconnected);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
            }
        }
    }

    /// Blocking receive. Uses tokio's waker-based `blocking_recv`.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` if the source has been dropped.
    pub fn recv(&mut self) -> Result<RecordBatch, RecvError> {
        loop {
            match self.rx.blocking_recv() {
                Ok(msg) => return Ok(to_batch(msg)),
                Err(broadcast::error::RecvError::Closed) => {
                    self.closed = true;
                    return Err(RecvError::Disconnected);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
            }
        }
    }

    /// Blocking receive with timeout. Requires a tokio runtime in the current
    /// thread context.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Timeout` or `RecvError::Disconnected`.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<RecordBatch, RecvError> {
        let handle = tokio::runtime::Handle::current();
        match handle.block_on(tokio::time::timeout(timeout, self.recv_async())) {
            Ok(Ok(batch)) => Ok(batch),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(RecvError::Timeout),
        }
    }

    /// Returns true if the channel has been observed closed.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.closed
    }

    /// Returns the schema for records in this subscription.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn to_batch<T: Record>(msg: SourceMessage<T>) -> RecordBatch {
    match msg {
        SourceMessage::Record(r) => r.to_record_batch(),
        SourceMessage::Batch(b) => b,
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Subscription<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("closed", &self.closed)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
