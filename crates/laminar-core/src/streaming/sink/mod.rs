//! Sink — the single consumer of a source's bounded queue.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::channel::AsyncConsumer;
use super::error::RecvError;
use super::source::{Record, SourceMessage};

/// The single consumer of a source's bounded queue.
///
/// Messages leave the queue only when the sink receives them, so a slow sink applies
/// backpressure to producers (`ChannelFull`) instead of losing accepted input. Dropping the
/// sink disconnects every producer.
pub struct Sink<T: Record> {
    consumer: AsyncConsumer<SourceMessage<T>>,
    schema: SchemaRef,
    closed: bool,
}

impl<T: Record> Sink<T> {
    pub(crate) fn new(consumer: AsyncConsumer<SourceMessage<T>>, schema: SchemaRef) -> Self {
        Self {
            consumer,
            schema,
            closed: false,
        }
    }

    /// Non-blocking poll. Returns the next batch.
    /// Returns `None` on empty or closed queue. Check `is_disconnected()`
    /// to distinguish.
    pub fn poll(&mut self) -> Option<RecordBatch> {
        match self.consumer.try_recv() {
            Ok(message) => Some(message.into_batch()),
            Err(crossfire::TryRecvError::Empty) => None,
            Err(crossfire::TryRecvError::Disconnected) => {
                self.closed = true;
                None
            }
        }
    }

    /// Async receive. Awaits the next batch. Cancellation never loses a message.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` once every producer has been dropped and the queue is
    /// empty.
    pub async fn recv_async(&mut self) -> Result<RecordBatch, RecvError> {
        let Ok(message) = self.consumer.recv().await else {
            self.closed = true;
            return Err(RecvError::Disconnected);
        };
        Ok(message.into_batch())
    }

    /// Returns true if the queue has been observed closed and drained.
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.closed
    }

    /// Returns the schema for this sink.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Sink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sink")
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
