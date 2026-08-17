//! Sink — consumption endpoint with broadcast fan-out to multiple subscribers.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use tokio::sync::broadcast;

use super::channel::AsyncConsumer;
use super::source::{Record, SourceMessage};
use super::subscription::Subscription;

const DEFAULT_BROADCAST_CAPACITY: usize = 2048;

/// A streaming data sink. Each `subscribe()` call returns an independent
/// receiver that gets a copy of every message via broadcast.
///
/// The Sink intentionally outlives itself for a brief window after Drop:
/// the internal drain task holds the broadcast sender and continues
/// pumping until the upstream `AsyncConsumer` returns end-of-stream.
/// This is what lets a query handle keep receiving rows after the
/// `(source, sink)` pair has been dropped from the bridge function.
pub struct Sink<T: Record> {
    broadcast_tx: broadcast::Sender<SourceMessage<T>>,
    schema: SchemaRef,
}

impl<T: Record> Sink<T> {
    pub(crate) fn new(consumer: AsyncConsumer<SourceMessage<T>>, schema: SchemaRef) -> Self {
        let (broadcast_tx, _) = broadcast::channel(DEFAULT_BROADCAST_CAPACITY);
        let tx = broadcast_tx.clone();

        // Detached on purpose. Task ends naturally when `consumer.recv()`
        // returns Err (source closed). Aborting on Sink::drop would cut
        // the tail of in-flight messages off mid-stream.
        tokio::spawn(async move {
            drain_loop(consumer, tx).await;
        });

        Self {
            broadcast_tx,
            schema,
        }
    }

    /// Subscribe to this sink. Returns an independent receiver.
    #[must_use]
    pub fn subscribe(&self) -> Subscription<T> {
        Subscription::new(self.broadcast_tx.subscribe(), Arc::clone(&self.schema))
    }

    /// Returns the schema for this sink.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Number of active broadcast subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.broadcast_tx.receiver_count()
    }
}

impl<T: Record + std::fmt::Debug> std::fmt::Debug for Sink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sink")
            .field("subscribers", &self.subscriber_count())
            .finish()
    }
}

async fn drain_loop<T: Record>(
    mut consumer: AsyncConsumer<SourceMessage<T>>,
    tx: broadcast::Sender<SourceMessage<T>>,
) {
    while let Ok(msg) = consumer.recv().await {
        let _ = tx.send(msg);
    }
}

#[cfg(test)]
mod tests;
